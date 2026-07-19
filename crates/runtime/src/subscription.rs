// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::task_manager::MailboxRuntimeHandle;
use mica_relation_kernel::{
    CatalogChange, Commit, FactChange, FactChangeKind, RelationKernel, Snapshot, Tuple,
};
use mica_var::{CapabilityId, Symbol, Value};
use mica_vm::{
    AuthorityContext, RuntimeContext, RuntimeError, SubscriptionInitialDelivery,
    SubscriptionOperation, SubscriptionRequest, SubscriptionSubject,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct SubscriptionRuntimeHandle {
    state: Arc<Mutex<SubscriptionStore>>,
    mailboxes: MailboxRuntimeHandle,
}

#[derive(Debug, Default)]
struct SubscriptionStore {
    subscriptions: HashMap<CapabilityId, ActiveSubscription>,
    dispatched_cursor: Option<u64>,
}

#[derive(Debug)]
struct ActiveSubscription {
    capability: Value,
    request: SubscriptionRequest,
    runtime_context: RuntimeContext,
    root_authority: bool,
    baseline: BTreeSet<Tuple>,
    needs_resynchronization: bool,
    revoked: bool,
}

impl SubscriptionRuntimeHandle {
    pub(crate) fn new(mailboxes: MailboxRuntimeHandle) -> Self {
        Self {
            state: Arc::new(Mutex::new(SubscriptionStore::default())),
            mailboxes,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().unwrap().subscriptions.len()
    }

    pub(crate) fn apply_boundary(
        &self,
        kernel: &RelationKernel,
        snapshot: &Arc<Snapshot>,
        operations: &mut Vec<SubscriptionOperation>,
    ) -> Result<Vec<u64>, RuntimeError> {
        let mut state = self.state.lock().unwrap();
        let mut delivered = self.dispatch_through(kernel, &mut state, snapshot)?;
        for operation in operations.drain(..) {
            match operation {
                SubscriptionOperation::Register {
                    subscription,
                    request,
                    runtime_context,
                    root_authority,
                } => {
                    let capability = subscription.as_capability().ok_or_else(|| {
                        RuntimeError::InvalidMailboxCapability {
                            operation: "subscription",
                            capability: subscription.clone(),
                        }
                    })?;
                    let mut active = ActiveSubscription {
                        capability: subscription,
                        request,
                        runtime_context,
                        root_authority,
                        baseline: BTreeSet::new(),
                        needs_resynchronization: false,
                        revoked: false,
                    };
                    let authority = refreshed_authority(kernel, &active);
                    if authority.is_none_or(|authority| {
                        active
                            .request
                            .subject
                            .relation()
                            .is_some_and(|relation| !authority.can_read_relation(relation))
                    }) {
                        let message =
                            marker_message(&active.capability, snapshot.version(), "revoked");
                        let mailbox = self
                            .mailboxes
                            .replace_subscription_message(&active.capability, message)?;
                        active.revoked = true;
                        delivered.push(mailbox);
                    } else {
                        match scan_subject(snapshot, &active.request.subject) {
                            Ok(baseline) => {
                                active.baseline = baseline;
                                delivered.extend(self.deliver_initial(
                                    kernel,
                                    snapshot,
                                    &mut active,
                                )?);
                            }
                            Err(_) => delivered
                                .extend(self.resynchronize(&mut active, snapshot.version())?),
                        }
                    }
                    state.subscriptions.insert(capability, active);
                }
                SubscriptionOperation::Cancel { subscription } => {
                    if let Some(capability) = subscription.as_capability() {
                        state.subscriptions.remove(&capability);
                    }
                    self.mailboxes.release_subscription(&subscription);
                }
            }
        }
        delivered.sort_unstable();
        delivered.dedup();
        Ok(delivered)
    }

    fn dispatch_through(
        &self,
        kernel: &RelationKernel,
        state: &mut SubscriptionStore,
        snapshot: &Arc<Snapshot>,
    ) -> Result<Vec<u64>, RuntimeError> {
        let Some(cursor) = state.dispatched_cursor else {
            state.dispatched_cursor = Some(snapshot.version());
            return Ok(Vec::new());
        };
        if cursor >= snapshot.version() {
            return Ok(Vec::new());
        }
        let commits = snapshot.commits_since(cursor);
        if commits.first().map(Commit::version) != Some(cursor + 1) {
            state.dispatched_cursor = Some(snapshot.version());
            return self.resynchronize_all(state, snapshot.version());
        }
        let multiple_catalogue_commits = commits.len() > 1
            && commits
                .iter()
                .any(|commit| !commit.catalog_changes().is_empty());
        let mut delivered = Vec::new();
        for commit in &commits {
            for subscription in state.subscriptions.values_mut() {
                if multiple_catalogue_commits
                    && matches!(
                        subscription.request.subject,
                        SubscriptionSubject::Relation { .. }
                    )
                {
                    delivered.extend(self.resynchronize(subscription, commit.version())?);
                    continue;
                }
                delivered.extend(self.deliver_commit(kernel, snapshot, subscription, commit)?);
            }
            state.dispatched_cursor = Some(commit.version());
        }
        Ok(delivered)
    }

    fn deliver_initial(
        &self,
        kernel: &RelationKernel,
        snapshot: &Arc<Snapshot>,
        subscription: &mut ActiveSubscription,
    ) -> Result<Vec<u64>, RuntimeError> {
        if let Some(cursor) = subscription.request.cursor {
            if cursor > snapshot.version() {
                return self.resynchronize(subscription, snapshot.version());
            }
            let commits = snapshot.commits_since(cursor);
            if cursor != snapshot.version()
                && commits.first().map(Commit::version) != Some(cursor + 1)
            {
                return self.resynchronize(subscription, snapshot.version());
            }
            if matches!(
                subscription.request.subject,
                SubscriptionSubject::Relation { .. }
            ) && commits.iter().any(|commit| {
                !commit.settled_relation_changes_available() || !commit.catalog_changes().is_empty()
            }) {
                return self.resynchronize(subscription, snapshot.version());
            }
            let current_baseline = subscription.baseline.clone();
            let mut delivered = Vec::new();
            for commit in &commits {
                delivered.extend(self.deliver_commit(kernel, snapshot, subscription, commit)?);
            }
            subscription.baseline = current_baseline;
            return Ok(delivered);
        }
        if subscription.request.initial_delivery == SubscriptionInitialDelivery::ChangesOnly {
            return Ok(Vec::new());
        }
        if matches!(subscription.request.subject, SubscriptionSubject::Catalogue) {
            let entries = catalogue_snapshot_values(snapshot);
            let entry_count = entries.len();
            let message = Value::map([
                (symbol("kind"), symbol("snapshot")),
                (symbol("subscription"), subscription.capability.clone()),
                (symbol("cursor"), cursor_value(snapshot.version())),
                (symbol("subject"), symbol("catalogue")),
                (symbol("entries"), Value::list(entries)),
            ]);
            return self.enqueue(subscription, message, snapshot.version(), entry_count);
        }
        let assertions = subscription.baseline.iter().cloned().collect::<Vec<_>>();
        let entry_count = assertions.len();
        let message = change_message(
            &subscription.capability,
            snapshot.version(),
            "snapshot",
            assertions,
            Vec::new(),
        );
        self.enqueue(subscription, message, snapshot.version(), entry_count)
    }

    fn deliver_commit(
        &self,
        kernel: &RelationKernel,
        snapshot: &Arc<Snapshot>,
        subscription: &mut ActiveSubscription,
        commit: &Commit,
    ) -> Result<Vec<u64>, RuntimeError> {
        if subscription.revoked {
            return Ok(Vec::new());
        }
        if subscription.needs_resynchronization {
            return self.resynchronize(subscription, commit.version());
        }
        let authority = refreshed_authority(kernel, subscription);
        if authority.is_none_or(|authority| {
            subscription
                .request
                .subject
                .relation()
                .is_some_and(|relation| !authority.can_read_relation(relation))
        }) {
            let message = marker_message(&subscription.capability, commit.version(), "revoked");
            let mailbox = self
                .mailboxes
                .replace_subscription_message(&subscription.capability, message)?;
            subscription.needs_resynchronization = true;
            subscription.revoked = true;
            return Ok(vec![mailbox]);
        }

        match &subscription.request.subject {
            SubscriptionSubject::Catalogue => {
                if commit.catalog_changes().is_empty() {
                    return Ok(Vec::new());
                }
                let entries = commit
                    .catalog_changes()
                    .iter()
                    .map(catalogue_change_value)
                    .collect::<Vec<_>>();
                let entry_count = entries.len();
                let message = Value::map([
                    (symbol("kind"), symbol("changes")),
                    (symbol("subscription"), subscription.capability.clone()),
                    (symbol("cursor"), cursor_value(commit.version())),
                    (symbol("subject"), symbol("catalogue")),
                    (symbol("entries"), Value::list(entries)),
                ]);
                self.enqueue(subscription, message, commit.version(), entry_count)
            }
            SubscriptionSubject::Facts { relation, bindings } => {
                let (assertions, retractions) =
                    matching_changes(commit.changes(), *relation, bindings);
                if assertions.is_empty() && retractions.is_empty() {
                    return Ok(Vec::new());
                }
                let entry_count = assertions.len() + retractions.len();
                let message = change_message(
                    &subscription.capability,
                    commit.version(),
                    "facts",
                    assertions,
                    retractions,
                );
                self.enqueue(subscription, message, commit.version(), entry_count)
            }
            SubscriptionSubject::Relation { relation, bindings } => {
                let (assertions, retractions) = if commit.catalog_changes().is_empty() {
                    matching_changes(commit.relation_changes(), *relation, bindings)
                } else {
                    let Ok(current) = scan_subject(snapshot, &subscription.request.subject) else {
                        return self.resynchronize(subscription, commit.version());
                    };
                    let assertions = current
                        .difference(&subscription.baseline)
                        .cloned()
                        .collect::<Vec<_>>();
                    let retractions = subscription
                        .baseline
                        .difference(&current)
                        .cloned()
                        .collect::<Vec<_>>();
                    subscription.baseline = current;
                    (assertions, retractions)
                };
                if commit.catalog_changes().is_empty() {
                    apply_baseline_changes(&mut subscription.baseline, &assertions, &retractions);
                }
                if assertions.is_empty() && retractions.is_empty() {
                    return Ok(Vec::new());
                }
                let entry_count = assertions.len() + retractions.len();
                let message = change_message(
                    &subscription.capability,
                    commit.version(),
                    "relation",
                    assertions,
                    retractions,
                );
                self.enqueue(subscription, message, commit.version(), entry_count)
            }
        }
    }

    fn enqueue(
        &self,
        subscription: &mut ActiveSubscription,
        message: Value,
        cursor: u64,
        entry_count: usize,
    ) -> Result<Vec<u64>, RuntimeError> {
        if entry_count > subscription.request.queue_budget {
            return self.resynchronize(subscription, cursor);
        }
        let resynchronization = marker_message(&subscription.capability, cursor, "resynchronize");
        let (mailbox, overflow) = self.mailboxes.deliver_subscription(
            &subscription.capability,
            message,
            resynchronization,
            subscription.request.queue_budget,
        )?;
        subscription.needs_resynchronization = overflow;
        Ok(vec![mailbox])
    }

    fn resynchronize(
        &self,
        subscription: &mut ActiveSubscription,
        cursor: u64,
    ) -> Result<Vec<u64>, RuntimeError> {
        let message = marker_message(&subscription.capability, cursor, "resynchronize");
        let mailbox = self
            .mailboxes
            .replace_subscription_message(&subscription.capability, message)?;
        subscription.needs_resynchronization = true;
        Ok(vec![mailbox])
    }

    fn resynchronize_all(
        &self,
        state: &mut SubscriptionStore,
        cursor: u64,
    ) -> Result<Vec<u64>, RuntimeError> {
        let mut delivered = Vec::new();
        for subscription in state.subscriptions.values_mut() {
            delivered.extend(self.resynchronize(subscription, cursor)?);
        }
        Ok(delivered)
    }
}

fn refreshed_authority(
    kernel: &RelationKernel,
    subscription: &ActiveSubscription,
) -> Option<AuthorityContext> {
    if subscription.root_authority {
        return Some(AuthorityContext::root());
    }
    crate::authority_for_runtime_context(kernel, subscription.runtime_context).ok()
}

fn scan_subject(
    snapshot: &Snapshot,
    subject: &SubscriptionSubject,
) -> Result<BTreeSet<Tuple>, RuntimeError> {
    let Some(relation) = subject.relation() else {
        return Ok(BTreeSet::new());
    };
    let bindings = match subject {
        SubscriptionSubject::Catalogue => unreachable!(),
        SubscriptionSubject::Facts { bindings, .. }
        | SubscriptionSubject::Relation { bindings, .. } => bindings,
    };
    let rows = match subject {
        SubscriptionSubject::Facts { .. } => snapshot.scan_facts(relation, bindings),
        SubscriptionSubject::Relation { .. } => snapshot.scan(relation, bindings),
        SubscriptionSubject::Catalogue => unreachable!(),
    };
    rows.map(|rows| rows.into_iter().collect())
        .map_err(RuntimeError::Kernel)
}

fn catalogue_snapshot_values(snapshot: &Snapshot) -> Vec<Value> {
    let mut entries = snapshot
        .relation_metadata()
        .map(|metadata| {
            Value::map([
                (symbol("kind"), symbol("relation_created")),
                (symbol("relation"), Value::identity(metadata.id())),
                (symbol("name"), Value::symbol(metadata.name())),
            ])
        })
        .collect::<Vec<_>>();
    entries.extend(snapshot.rules().iter().map(|definition| {
        Value::map([
            (
                symbol("kind"),
                symbol(if definition.active() {
                    "rule_installed"
                } else {
                    "rule_disabled"
                }),
            ),
            (symbol("rule"), Value::identity(definition.id())),
            (
                symbol("relation"),
                Value::identity(definition.rule().head_relation()),
            ),
        ])
    }));
    entries
}

fn matching_changes(
    changes: &[FactChange],
    relation: mica_relation_kernel::RelationId,
    bindings: &[Option<Value>],
) -> (Vec<Tuple>, Vec<Tuple>) {
    let mut assertions = Vec::new();
    let mut retractions = Vec::new();
    for change in changes {
        if change.relation != relation || !change.tuple.matches_bindings(bindings) {
            continue;
        }
        match change.kind {
            FactChangeKind::Assert => assertions.push(change.tuple.clone()),
            FactChangeKind::Retract => retractions.push(change.tuple.clone()),
        }
    }
    (assertions, retractions)
}

fn apply_baseline_changes(
    baseline: &mut BTreeSet<Tuple>,
    assertions: &[Tuple],
    retractions: &[Tuple],
) {
    for tuple in retractions {
        baseline.remove(tuple);
    }
    baseline.extend(assertions.iter().cloned());
}

fn change_message(
    subscription: &Value,
    cursor: u64,
    subject: &str,
    assertions: Vec<Tuple>,
    retractions: Vec<Tuple>,
) -> Value {
    Value::map([
        (symbol("kind"), symbol("changes")),
        (symbol("subscription"), subscription.clone()),
        (symbol("cursor"), cursor_value(cursor)),
        (symbol("subject"), symbol(subject)),
        (
            symbol("assertions"),
            Value::list(assertions.into_iter().map(tuple_value)),
        ),
        (
            symbol("retractions"),
            Value::list(retractions.into_iter().map(tuple_value)),
        ),
    ])
}

fn marker_message(subscription: &Value, cursor: u64, kind: &str) -> Value {
    Value::map([
        (symbol("kind"), symbol(kind)),
        (symbol("subscription"), subscription.clone()),
        (symbol("cursor"), cursor_value(cursor)),
    ])
}

fn catalogue_change_value(change: &CatalogChange) -> Value {
    match change {
        CatalogChange::RelationCreated(metadata) => Value::map([
            (symbol("kind"), symbol("relation_created")),
            (symbol("relation"), Value::identity(metadata.id())),
            (symbol("name"), Value::symbol(metadata.name())),
        ]),
        CatalogChange::RuleInstalled(definition) => Value::map([
            (symbol("kind"), symbol("rule_installed")),
            (symbol("rule"), Value::identity(definition.id())),
            (
                symbol("relation"),
                Value::identity(definition.rule().head_relation()),
            ),
        ]),
        CatalogChange::RuleDisabled(rule) => Value::map([
            (symbol("kind"), symbol("rule_disabled")),
            (symbol("rule"), Value::identity(*rule)),
        ]),
    }
}

fn tuple_value(tuple: Tuple) -> Value {
    Value::list(tuple.values().iter().cloned())
}

fn symbol(name: &str) -> Value {
    Value::symbol(Symbol::intern(name))
}

fn cursor_value(cursor: u64) -> Value {
    Value::int(i64::try_from(cursor).unwrap_or(i64::MAX)).unwrap()
}
