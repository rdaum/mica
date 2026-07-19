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

use crate::index::RelationState;
use crate::relation_states::RelationStates;
use crate::snapshot::{
    CommitHistory, active_rules, empty_derived_cache, empty_dispatch_cache, empty_maintained_cache,
    empty_method_program_cache, empty_packed_cache,
};
use crate::{
    CatalogChange, Commit, CommitProvider, ComputedRelation, ComputedRelationRegistry,
    ExecutionContext, FactChangeKind, KernelError, RelationDurability, RelationMetadata, Rule,
    RuleDefinition, RuleSet, Snapshot, Transaction,
};
use arc_swap::ArcSwap;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) const GENERATED_RULE_ID_START: u64 = 0x00d0_0000_0000_0000;

pub struct RelationKernel {
    root: ArcSwap<Snapshot>,
    provider: Arc<dyn CommitProvider>,
    commit_lock: Mutex<()>,
    execution_context: ExecutionContext,
}

impl RelationKernel {
    pub fn new() -> Self {
        Self::with_provider(Arc::new(crate::InMemoryCommitProvider::new()))
    }

    pub fn with_provider(provider: Arc<dyn CommitProvider>) -> Self {
        Self::with_provider_and_computed_relations(provider, [])
    }

    pub fn with_provider_and_computed_relations(
        provider: Arc<dyn CommitProvider>,
        computed_relations: impl IntoIterator<Item = Arc<dyn ComputedRelation>>,
    ) -> Self {
        let computed_relations = Arc::new(ComputedRelationRegistry::new(computed_relations));
        let snapshot = Arc::new(Snapshot {
            version: 0,
            relations: RelationStates::new(),
            rules: Vec::new(),
            computed_relations: computed_relations.clone(),
            derived_cache: empty_derived_cache(),
            maintained_cache: empty_maintained_cache(),
            packed_cache: empty_packed_cache(),
            dispatch_cache: empty_dispatch_cache(),
            method_program_cache: empty_method_program_cache(),
            commits: CommitHistory::empty(),
        });
        crate::metrics::metrics().record_snapshot(&snapshot);
        Self {
            root: ArcSwap::new(snapshot),
            provider,
            commit_lock: Mutex::new(()),
            execution_context: ExecutionContext::serial(),
        }
    }

    pub fn load_from_commits(
        relations: impl IntoIterator<Item = RelationMetadata>,
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
    ) -> Result<Self, KernelError> {
        Self::load_from_commits_and_computed_relations(relations, commits, provider, [])
    }

    pub fn load_from_commits_and_computed_relations(
        relations: impl IntoIterator<Item = RelationMetadata>,
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
        computed_relations: impl IntoIterator<Item = Arc<dyn ComputedRelation>>,
    ) -> Result<Self, KernelError> {
        let mut states = RelationStates::new();
        for metadata in relations {
            states.insert(metadata.id(), RelationState::empty(metadata)?);
        }

        let commits = commits.into_iter().collect::<Vec<_>>();
        let mut rules = Vec::new();
        for commit in &commits {
            for change in commit.catalog_changes() {
                if let CatalogChange::RuleInstalled(rule) = change {
                    validate_rule_definition_against_relations(&states, rule)?;
                    let mut next_rules = rules.clone();
                    next_rules.push(rule.clone());
                    RuleSet::new(active_rules(&next_rules))
                        .validate_stratified()
                        .map_err(KernelError::Rule)?;
                    rules = next_rules;
                } else if let CatalogChange::RuleDisabled(rule_id) = change {
                    disable_rule_in(&mut rules, *rule_id)?;
                }
            }
            for change in commit.changes() {
                let relation = states
                    .get_mut(&change.relation)
                    .ok_or(KernelError::UnknownRelation(change.relation))?;
                if relation.metadata().durability() == RelationDurability::Volatile {
                    continue;
                }
                if relation.metadata().arity() as usize != change.tuple.arity() {
                    return Err(KernelError::ArityMismatch {
                        relation: change.relation,
                        expected: relation.metadata().arity(),
                        actual: change.tuple.arity(),
                    });
                }
                let _ = match change.kind {
                    FactChangeKind::Assert => relation.insert(change.tuple.clone()),
                    FactChangeKind::Retract => relation.remove(&change.tuple),
                };
            }
        }

        let computed_relations = Arc::new(
            ComputedRelationRegistry::new(computed_relations)
                .bind_relations(states.values().map(RelationState::metadata)),
        );
        let version = commits.last().map_or(0, Commit::version);
        let snapshot = Arc::new(Snapshot {
            version,
            relations: states,
            rules,
            computed_relations: computed_relations.clone(),
            derived_cache: empty_derived_cache(),
            maintained_cache: empty_maintained_cache(),
            packed_cache: empty_packed_cache(),
            dispatch_cache: empty_dispatch_cache(),
            method_program_cache: empty_method_program_cache(),
            commits: CommitHistory::from_commits(commits),
        });
        crate::metrics::metrics().record_snapshot(&snapshot);
        Ok(Self {
            root: ArcSwap::new(snapshot),
            provider,
            commit_lock: Mutex::new(()),
            execution_context: ExecutionContext::serial(),
        })
    }

    pub fn load_from_commit_log(
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
    ) -> Result<Self, KernelError> {
        Self::load_from_commit_log_and_computed_relations(commits, provider, [])
    }

    pub fn load_from_commit_log_and_computed_relations(
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
        computed_relations: impl IntoIterator<Item = Arc<dyn ComputedRelation>>,
    ) -> Result<Self, KernelError> {
        let commits = commits.into_iter().collect::<Vec<_>>();
        let mut states = RelationStates::new();
        let mut rules = Vec::new();

        for commit in &commits {
            for change in commit.catalog_changes() {
                match change {
                    CatalogChange::RelationCreated(metadata) => {
                        states.insert(metadata.id(), RelationState::empty(metadata.clone())?);
                    }
                    CatalogChange::RuleInstalled(rule) => {
                        validate_rule_definition_against_relations(&states, rule)?;
                        let mut next_rules = rules.clone();
                        next_rules.push(rule.clone());
                        RuleSet::new(active_rules(&next_rules))
                            .validate_stratified()
                            .map_err(KernelError::Rule)?;
                        rules = next_rules;
                    }
                    CatalogChange::RuleDisabled(rule_id) => {
                        disable_rule_in(&mut rules, *rule_id)?;
                    }
                }
            }
            for change in commit.changes() {
                let relation = states
                    .get_mut(&change.relation)
                    .ok_or(KernelError::UnknownRelation(change.relation))?;
                if relation.metadata().durability() == RelationDurability::Volatile {
                    continue;
                }
                if relation.metadata().arity() as usize != change.tuple.arity() {
                    return Err(KernelError::ArityMismatch {
                        relation: change.relation,
                        expected: relation.metadata().arity(),
                        actual: change.tuple.arity(),
                    });
                }
                let _ = match change.kind {
                    FactChangeKind::Assert => relation.insert(change.tuple.clone()),
                    FactChangeKind::Retract => relation.remove(&change.tuple),
                };
            }
        }

        let computed_relations = Arc::new(
            ComputedRelationRegistry::new(computed_relations)
                .bind_relations(states.values().map(RelationState::metadata)),
        );
        let version = commits.last().map_or(0, Commit::version);
        let snapshot = Arc::new(Snapshot {
            version,
            relations: states,
            rules,
            computed_relations: computed_relations.clone(),
            derived_cache: empty_derived_cache(),
            maintained_cache: empty_maintained_cache(),
            packed_cache: empty_packed_cache(),
            dispatch_cache: empty_dispatch_cache(),
            method_program_cache: empty_method_program_cache(),
            commits: CommitHistory::from_commits(commits),
        });
        crate::metrics::metrics().record_snapshot(&snapshot);
        Ok(Self {
            root: ArcSwap::new(snapshot),
            provider,
            commit_lock: Mutex::new(()),
            execution_context: ExecutionContext::serial(),
        })
    }

    pub fn load_from_state(
        state: crate::PersistedKernelState,
        provider: Arc<dyn CommitProvider>,
    ) -> Result<Self, KernelError> {
        Self::load_from_state_and_computed_relations(state, provider, [])
    }

    pub fn load_from_state_and_computed_relations(
        state: crate::PersistedKernelState,
        provider: Arc<dyn CommitProvider>,
        computed_relations: impl IntoIterator<Item = Arc<dyn ComputedRelation>>,
    ) -> Result<Self, KernelError> {
        let mut states = RelationStates::new();
        for metadata in state.relations {
            states.insert(metadata.id(), RelationState::empty(metadata)?);
        }

        for rule in &state.rules {
            validate_rule_definition_against_relations(&states, rule)?;
        }
        RuleSet::new(active_rules(&state.rules))
            .validate_stratified()
            .map_err(KernelError::Rule)?;

        for (relation_id, tuple) in state.facts {
            let relation = states
                .get_mut(&relation_id)
                .ok_or(KernelError::UnknownRelation(relation_id))?;
            if relation.metadata().durability() == RelationDurability::Volatile {
                continue;
            }
            if relation.metadata().arity() as usize != tuple.arity() {
                return Err(KernelError::ArityMismatch {
                    relation: relation_id,
                    expected: relation.metadata().arity(),
                    actual: tuple.arity(),
                });
            }
            relation.insert(tuple);
        }

        let computed_relations = Arc::new(
            ComputedRelationRegistry::new(computed_relations)
                .bind_relations(states.values().map(RelationState::metadata)),
        );
        let snapshot = Arc::new(Snapshot {
            version: state.version,
            relations: states,
            rules: state.rules,
            computed_relations: computed_relations.clone(),
            derived_cache: empty_derived_cache(),
            maintained_cache: empty_maintained_cache(),
            packed_cache: empty_packed_cache(),
            dispatch_cache: empty_dispatch_cache(),
            method_program_cache: empty_method_program_cache(),
            commits: CommitHistory::empty(),
        });
        crate::metrics::metrics().record_snapshot(&snapshot);
        Ok(Self {
            root: ArcSwap::new(snapshot),
            provider,
            commit_lock: Mutex::new(()),
            execution_context: ExecutionContext::serial(),
        })
    }

    pub fn with_execution_context(mut self, execution_context: ExecutionContext) -> Self {
        self.execution_context = execution_context;
        self
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.root.load_full()
    }

    pub fn create_relation(
        &self,
        metadata: RelationMetadata,
    ) -> Result<Arc<Snapshot>, KernelError> {
        let _guard = self.commit_guard();
        let relation = RelationState::empty(metadata.clone())?;
        let current = self.snapshot();
        if current.relations.contains_key(&metadata.id()) {
            return Err(KernelError::RelationAlreadyExists(metadata.id()));
        }

        let mut next = (*current).clone();
        next.relations.insert(metadata.id(), relation);
        next.computed_relations = Arc::new(current.computed_relations.with_relation(&metadata));
        next.derived_cache = empty_derived_cache();
        next.maintained_cache = empty_maintained_cache();
        next.packed_cache = empty_packed_cache();
        next.dispatch_cache = empty_dispatch_cache();
        next.method_program_cache = empty_method_program_cache();
        next.version += 1;
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([CatalogChange::RelationCreated(metadata.clone())]),
            changes: Arc::from([]),
        };
        next.commits = current.commits.append(commit.clone());
        let next = Arc::new(next);

        self.persist_commit(&commit)?;
        if !self.try_publish(current.version(), next.clone()) {
            return Err(KernelError::Persistence(
                "commit publish failed after serialized persistence".to_owned(),
            ));
        }
        crate::metrics::metrics()
            .catalog_operations
            .inc(crate::metrics::CatalogOperation::RelationCreated);
        Ok(next)
    }

    pub fn install_rule(
        &self,
        rule: Rule,
        source: impl Into<String>,
    ) -> Result<RuleDefinition, KernelError> {
        let source = source.into();
        let _guard = self.commit_guard();
        let current = self.snapshot();
        validate_rule_against_relations(&current.relations, &rule)?;
        let definition =
            RuleDefinition::new(next_rule_id(&current.rules), rule.clone(), source.clone());
        let mut rules = current.rules.clone();
        rules.push(definition.clone());
        RuleSet::new(active_rules(&rules))
            .validate_stratified()
            .map_err(KernelError::Rule)?;

        let mut next = (*current).clone();
        next.rules = rules;
        next.derived_cache = empty_derived_cache();
        next.maintained_cache = empty_maintained_cache();
        next.packed_cache = empty_packed_cache();
        next.dispatch_cache = empty_dispatch_cache();
        next.method_program_cache = empty_method_program_cache();
        next.version += 1;
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([CatalogChange::RuleInstalled(definition.clone())]),
            changes: Arc::from([]),
        };
        next.commits = current.commits.append(commit.clone());
        let next = Arc::new(next);

        self.persist_commit(&commit)?;
        if !self.try_publish(current.version(), next) {
            return Err(KernelError::Persistence(
                "commit publish failed after serialized persistence".to_owned(),
            ));
        }
        crate::metrics::metrics()
            .catalog_operations
            .inc(crate::metrics::CatalogOperation::RuleInstalled);
        Ok(definition)
    }

    pub fn disable_rule(&self, rule_id: crate::FactId) -> Result<Arc<Snapshot>, KernelError> {
        let _guard = self.commit_guard();
        let current = self.snapshot();
        let mut rules = current.rules.clone();
        disable_rule_in(&mut rules, rule_id)?;
        RuleSet::new(active_rules(&rules))
            .validate_stratified()
            .map_err(KernelError::Rule)?;

        let mut next = (*current).clone();
        next.rules = rules;
        next.derived_cache = empty_derived_cache();
        next.maintained_cache = empty_maintained_cache();
        next.packed_cache = empty_packed_cache();
        next.dispatch_cache = empty_dispatch_cache();
        next.method_program_cache = empty_method_program_cache();
        next.version += 1;
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([CatalogChange::RuleDisabled(rule_id)]),
            changes: Arc::from([]),
        };
        next.commits = current.commits.append(commit.clone());
        let next = Arc::new(next);

        self.persist_commit(&commit)?;
        if !self.try_publish(current.version(), next.clone()) {
            return Err(KernelError::Persistence(
                "commit publish failed after serialized persistence".to_owned(),
            ));
        }
        crate::metrics::metrics()
            .catalog_operations
            .inc(crate::metrics::CatalogOperation::RuleDisabled);
        Ok(next)
    }

    pub fn begin(&self) -> Transaction<'_> {
        crate::metrics::metrics().transactions_started.inc();
        Transaction::new(self, self.snapshot(), self.execution_context.clone())
    }

    pub(crate) fn try_publish(&self, expected_version: u64, next: Arc<Snapshot>) -> bool {
        let mut success = false;
        self.root.rcu(|current| {
            if current.version == expected_version {
                success = true;
                next.clone()
            } else {
                success = false;
                Arc::clone(current)
            }
        });
        if success {
            crate::metrics::metrics().record_snapshot(&next);
        }
        success
    }

    pub(crate) fn persist_commit(&self, commit: &Commit) -> Result<(), KernelError> {
        let snapshot = self.snapshot();
        let changes = commit
            .changes()
            .iter()
            .map(|change| {
                let relation = snapshot
                    .relations
                    .get(&change.relation)
                    .ok_or(KernelError::UnknownRelation(change.relation))?;
                Ok(
                    (relation.metadata().durability() == RelationDurability::Durable)
                        .then(|| change.clone()),
                )
            })
            .collect::<Result<Vec<_>, KernelError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        if commit.catalog_changes().is_empty() && changes.is_empty() {
            return Ok(());
        }
        let persistent_commit = Commit {
            version: commit.version(),
            catalog_changes: commit.catalog_changes.clone(),
            changes: changes.into(),
        };
        self.provider
            .persist_commit(&persistent_commit)
            .map_err(KernelError::Persistence)
    }

    pub(crate) fn commit_guard(&self) -> MutexGuard<'_, ()> {
        self.commit_lock.lock().unwrap()
    }
}

fn validate_rule_definition_against_relations(
    relations: &RelationStates,
    definition: &RuleDefinition,
) -> Result<(), KernelError> {
    validate_rule_against_relations(relations, definition.rule())
}

fn validate_rule_against_relations(
    relations: &RelationStates,
    rule: &Rule,
) -> Result<(), KernelError> {
    validate_rule_atom(relations, rule.head_relation(), rule.head_terms())?;
    for atom in rule.body_atoms() {
        validate_rule_atom(relations, atom.relation(), atom.terms())?;
    }
    Ok(())
}

fn next_rule_id(rules: &[RuleDefinition]) -> crate::FactId {
    let mut raw = GENERATED_RULE_ID_START + rules.len() as u64;
    loop {
        let id = crate::FactId::new(raw & crate::FactId::MAX).unwrap();
        if !rules.iter().any(|rule| rule.id() == id) {
            return id;
        }
        raw = raw.wrapping_add(1);
    }
}

fn disable_rule_in(
    rules: &mut [RuleDefinition],
    rule_id: crate::FactId,
) -> Result<(), KernelError> {
    let Some(rule) = rules.iter_mut().find(|rule| rule.id() == rule_id) else {
        return Err(KernelError::UnknownRule(rule_id));
    };
    rule.deactivate();
    Ok(())
}

fn validate_rule_atom(
    relations: &RelationStates,
    relation: crate::RelationId,
    terms: &[crate::Term],
) -> Result<(), KernelError> {
    let metadata = relations
        .get(&relation)
        .ok_or(KernelError::UnknownRelation(relation))?
        .metadata();
    if metadata.arity() as usize != terms.len() {
        return Err(KernelError::ArityMismatch {
            relation,
            expected: metadata.arity(),
            actual: terms.len(),
        });
    }
    Ok(())
}

impl Default for RelationKernel {
    fn default() -> Self {
        Self::new()
    }
}
