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

mod overlay;

use crate::computed::ComputedRelationRead;
use crate::index::{RelationMutationKind, RelationState};
use crate::metrics::{CommitOutcome, TransactionReadOperation, TransactionWriteOperation};
use crate::snapshot::{Commit, CommitResult, FactChange, FactChangeKind};
use crate::snapshot::{
    active_rules, build_derived_relations, empty_derived_cache, empty_dispatch_cache,
    empty_method_program_cache, empty_packed_cache, relation_has_active_rule_head,
};
use crate::tuple::{difference_ordered_tuple_rows, union_ordered_tuple_rows};
use crate::{
    ApplicableMethodCall, Conflict, ConflictKind, ConflictPolicy, DispatchRead, DispatchRelations,
    KernelError, PackedRelation, RelationCapabilities, RelationId, RelationKernel,
    RelationMetadata, RelationRead, RelationSource, RelationWorkspace, RuleSet, ScanControl,
    Snapshot, Tuple, ValueDomain, Version,
};
use mica_var::{Symbol, Value};
use overlay::{FunctionalVisibleMap, LocalChange, RelationWriteOverlay};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

pub struct Transaction<'a> {
    kernel: &'a RelationKernel,
    pub(crate) base: Arc<Snapshot>,
    writes: HashMap<RelationId, RelationWriteOverlay>,
    functional_visible: HashMap<RelationId, FunctionalVisibleMap>,
    derived_cache: RefCell<Option<Result<HashMap<RelationId, RelationState>, KernelError>>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(kernel: &'a RelationKernel, base: Arc<Snapshot>) -> Self {
        Self {
            kernel,
            base,
            writes: HashMap::new(),
            functional_visible: HashMap::new(),
            derived_cache: RefCell::new(None),
        }
    }

    pub fn base_version(&self) -> Version {
        self.base.version()
    }

    pub fn kernel(&self) -> &'a RelationKernel {
        self.kernel
    }

    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty()
    }

    pub(crate) fn has_local_writes(&self, relation: RelationId) -> bool {
        self.writes.contains_key(&relation)
    }

    pub(crate) fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Vec<ApplicableMethodCall>, KernelError> {
        if self.has_dispatch_writes(relations) {
            return crate::dispatch::applicable_method_calls_uncached(
                self, relations, selector, roles,
            );
        }
        self.base
            .cached_applicable_method_calls(relations, selector, roles)
    }

    pub(crate) fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Vec<ApplicableMethodCall>, KernelError> {
        if self.has_dispatch_writes(relations) {
            return crate::dispatch::applicable_method_calls_uncached(
                self, relations, selector, roles,
            );
        }
        self.base
            .cached_applicable_method_calls_normalized(relations, selector, roles)
    }

    pub(crate) fn cached_method_program(
        &self,
        relation: RelationId,
        method: &Value,
    ) -> Result<Option<Value>, KernelError> {
        if self.has_local_writes(relation) {
            return crate::dispatch::method_program_id_uncached(self, relation, method);
        }
        self.base.cached_method_program(relation, method)
    }

    pub(crate) fn cached_applicable_positional_methods(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        args: &[Value],
    ) -> Result<Vec<Value>, KernelError> {
        if self.has_dispatch_writes(relations) {
            return crate::dispatch::applicable_positional_methods(
                self,
                relations,
                selector.clone(),
                args,
            );
        }
        self.base
            .cached_applicable_positional_methods(relations, selector, args)
    }

    fn has_dispatch_writes(&self, relations: DispatchRelations) -> bool {
        self.has_local_writes(relations.method_selector)
            || self.has_local_writes(relations.param)
            || self.has_local_writes(relations.delegates)
    }

    pub fn assert(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.apply_local_change(relation, tuple, LocalChange::Assert)
    }

    pub fn retract(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.apply_local_change(relation, tuple, LocalChange::Retract)
    }

    pub fn replace_functional(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError> {
        self.base.relation(relation)?.validate_tuple(&tuple)?;
        let ConflictPolicy::Functional { key_positions } =
            self.base.relation(relation)?.metadata().conflict_policy()
        else {
            self.assert(relation, tuple)?;
            return Ok(());
        };
        let key_positions = key_positions.clone();

        if let Some(old_tuple) = self.visible_tuple_for_key(relation, &key_positions, &tuple)? {
            self.retract(relation, old_tuple)?;
        }
        let result = self.assert(relation, tuple);
        if result.is_ok() {
            crate::metrics::metrics()
                .transaction_functional_replacements
                .inc();
        }
        result
    }

    pub fn scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        crate::metrics::metrics()
            .transaction_read_operations
            .inc(TransactionReadOperation::Scan);
        let metadata = self.base.relation(relation)?.metadata();
        if self.base.rules().is_empty() && !self.writes.contains_key(&relation) {
            if self.base.computed_relations.is_computed_relation(metadata) {
                let rows = self.scan_extensional_rows(relation, bindings)?;
                crate::metrics::metrics()
                    .transaction_read_rows
                    .record(TransactionReadOperation::Scan, rows.len() as u64);
                return Ok(rows);
            }
            let rows = self.base.scan_extensional(relation, bindings)?;
            crate::metrics::metrics()
                .transaction_read_rows
                .record(TransactionReadOperation::Scan, rows.len() as u64);
            return Ok(rows);
        }

        let mut visible = self.scan_extensional_rows(relation, bindings)?;

        if relation_has_active_rule_head(self.base.rules(), relation) {
            let derived = self.derived_relations()?;
            if let Some(rows) = derived.get(&relation) {
                visible = union_ordered_tuple_rows(visible, rows.scan(bindings)?);
            }
        }

        crate::metrics::metrics()
            .transaction_read_rows
            .record(TransactionReadOperation::Scan, visible.len() as u64);
        Ok(visible)
    }

    pub(crate) fn estimate_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        crate::metrics::metrics()
            .transaction_read_operations
            .inc(TransactionReadOperation::EstimateScan);
        let mut rows = self.estimate_extensional_scan(relation, bindings)?;
        if relation_has_active_rule_head(self.base.rules(), relation)
            && let Some(derived) = self.derived_relations()?.get(&relation)
        {
            rows = rows.saturating_add(derived.estimate_scan_count(bindings)?);
        }
        crate::metrics::metrics()
            .transaction_read_rows
            .record(TransactionReadOperation::EstimateScan, rows as u64);
        Ok(rows)
    }

    fn derived_relations(&self) -> Result<HashMap<RelationId, RelationState>, KernelError> {
        if self.derived_cache.borrow().is_none() {
            let derived = RuleSet::new(active_rules(self.base.rules()))
                .evaluate_fixpoint(&ExtensionalTransactionReader { tx: self })
                .map_err(KernelError::from)
                .and_then(|derived| build_derived_relations(&self.base.relations, derived))
                .map(|derived| derived.into_iter().collect());
            *self.derived_cache.borrow_mut() = Some(derived);
        }
        self.derived_cache.borrow().as_ref().unwrap().clone()
    }

    pub(crate) fn estimate_extensional_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        let metadata = self.base.relation(relation)?.metadata();
        if self.base.computed_relations.is_computed_relation(metadata) {
            return self
                .base
                .computed_relations
                .estimate(self, metadata, bindings)
                .expect("computed relation should have a registered handler");
        }
        if !self.writes.contains_key(&relation) {
            return self.base.estimate_extensional_scan(relation, bindings);
        }
        let base = self.base.estimate_extensional_scan(relation, bindings)?;
        let local = self.writes[&relation].len();
        Ok(base.saturating_add(local))
    }

    pub fn visit(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        crate::metrics::metrics()
            .transaction_read_operations
            .inc(TransactionReadOperation::Visit);
        let mut rows = 0usize;
        let metadata = self.base.relation(relation)?.metadata();
        if !relation_has_active_rule_head(self.base.rules(), relation)
            && !self.writes.contains_key(&relation)
        {
            if self.base.computed_relations.is_computed_relation(metadata) {
                for tuple in self.scan_extensional_rows(relation, bindings)? {
                    rows += 1;
                    if visitor(&tuple)? == ScanControl::Stop {
                        break;
                    }
                }
                crate::metrics::metrics()
                    .transaction_read_rows
                    .record(TransactionReadOperation::Visit, rows as u64);
                return Ok(());
            }
            let result = self
                .base
                .visit_extensional(relation, bindings, &mut |tuple| {
                    rows += 1;
                    visitor(tuple)
                });
            if result.is_ok() {
                crate::metrics::metrics()
                    .transaction_read_rows
                    .record(TransactionReadOperation::Visit, rows as u64);
            }
            return result;
        }

        for tuple in self.scan(relation, bindings)? {
            rows += 1;
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        crate::metrics::metrics()
            .transaction_read_rows
            .record(TransactionReadOperation::Visit, rows as u64);
        Ok(())
    }

    pub(crate) fn scan_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<BTreeSet<Tuple>, KernelError> {
        Ok(self
            .scan_extensional_rows(relation, bindings)?
            .into_iter()
            .collect())
    }

    fn scan_extensional_rows(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        crate::metrics::metrics()
            .transaction_read_operations
            .inc(TransactionReadOperation::ScanExtensional);
        let metadata = self.base.relation(relation)?.metadata();
        if self.base.computed_relations.is_computed_relation(metadata) {
            let rows = self
                .base
                .computed_relations
                .scan(self, metadata, bindings)
                .expect("computed relation should have a registered handler")?;
            crate::metrics::metrics()
                .transaction_read_rows
                .record(TransactionReadOperation::ScanExtensional, rows.len() as u64);
            return Ok(rows);
        }
        let mut visible = self.base.scan_extensional(relation, bindings)?;

        if let Some(writes) = self.writes.get(&relation) {
            let metadata = self.base.relation(relation)?.metadata();
            let mut local_asserts = Vec::new();
            let mut local_retracts = Vec::new();
            writes.visit_matches(metadata, bindings, &mut |tuple, change| {
                if !tuple.matches_bindings(bindings) {
                    return;
                }
                match change {
                    LocalChange::Assert => {
                        local_asserts.push(tuple.clone());
                    }
                    LocalChange::Retract => {
                        local_retracts.push(tuple.clone());
                    }
                }
            });
            if visible.is_empty() && local_retracts.is_empty() {
                crate::metrics::metrics().transaction_read_rows.record(
                    TransactionReadOperation::ScanExtensional,
                    local_asserts.len() as u64,
                );
                return Ok(local_asserts);
            }
            if !local_asserts.is_empty() {
                visible = union_ordered_tuple_rows(visible, local_asserts);
            }
            if !local_retracts.is_empty() {
                visible = difference_ordered_tuple_rows(visible, local_retracts);
            }
        }

        crate::metrics::metrics().transaction_read_rows.record(
            TransactionReadOperation::ScanExtensional,
            visible.len() as u64,
        );
        Ok(visible)
    }

    pub fn relation_metadata(&self, relation: RelationId) -> Option<RelationMetadata> {
        self.base
            .relations
            .get(&relation)
            .map(|relation| relation.metadata().clone())
    }

    fn extensional_facts_with_local_writes(&self) -> Result<Vec<(RelationId, Tuple)>, KernelError> {
        let mut facts = Vec::new();
        for metadata in self.base.relation_metadata() {
            if self.base.computed_relations.is_computed_relation(metadata) {
                continue;
            }
            let bindings = vec![None; metadata.arity() as usize];
            facts.extend(
                self.scan_extensional_rows(metadata.id(), &bindings)?
                    .into_iter()
                    .map(|tuple| (metadata.id(), tuple)),
            );
        }
        facts.sort();
        Ok(facts)
    }

    pub fn reconcile_relation(
        &mut self,
        relation: RelationId,
        desired: impl IntoIterator<Item = Tuple>,
    ) -> Result<(), KernelError> {
        let arity = self.base.relation(relation)?.metadata().arity();
        let desired = desired.into_iter().collect::<BTreeSet<_>>();
        for tuple in &desired {
            if tuple.arity() != arity as usize {
                return Err(KernelError::ArityMismatch {
                    relation,
                    expected: arity,
                    actual: tuple.arity(),
                });
            }
        }

        let current = self
            .scan(relation, &vec![None; arity as usize])?
            .into_iter()
            .collect::<BTreeSet<_>>();

        for tuple in current.difference(&desired) {
            self.retract(relation, tuple.clone())?;
        }
        for tuple in desired.difference(&current) {
            self.assert(relation, tuple.clone())?;
        }
        Ok(())
    }

    pub fn commit(self) -> Result<CommitResult, KernelError> {
        let start = Instant::now();
        let result = self.commit_inner();
        let elapsed = start.elapsed();
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        crate::metrics::metrics()
            .transaction_commit_duration_us
            .record(elapsed_us);
        crate::metrics::metrics()
            .transaction_commit_duration
            .record_elapsed(elapsed);
        match &result {
            Ok(result) => {
                crate::metrics::metrics()
                    .transaction_commits
                    .inc(CommitOutcome::Committed);
                crate::metrics::metrics()
                    .transaction_commit_changes
                    .record(result.commit().changes().len() as u64);
            }
            Err(KernelError::Conflict(_)) => crate::metrics::metrics()
                .transaction_commits
                .inc(CommitOutcome::Conflict),
            Err(KernelError::Persistence(_)) => crate::metrics::metrics()
                .transaction_commits
                .inc(CommitOutcome::PersistenceError),
            Err(_) => crate::metrics::metrics()
                .transaction_commits
                .inc(CommitOutcome::Error),
        }
        result
    }

    fn commit_inner(self) -> Result<CommitResult, KernelError> {
        let _guard = self.kernel.commit_guard();
        let current = self.kernel.snapshot();
        if current.version() != self.base.version() {
            self.validate_conflicts(&current)?;
        }
        let (next, commit) = self.build_next_snapshot(&current)?;
        self.kernel.persist_commit(&commit)?;
        if !self.kernel.try_publish(current.version(), next.clone()) {
            return Err(KernelError::Persistence(
                "commit publish failed after serialized persistence".to_owned(),
            ));
        }
        Ok(CommitResult {
            snapshot: next,
            commit,
        })
    }

    fn visible_tuple_for_key(
        &mut self,
        relation: RelationId,
        positions: &[u16],
        tuple: &Tuple,
    ) -> Result<Option<Tuple>, KernelError> {
        self.ensure_functional_visible(relation, positions)?;
        let visible = self
            .functional_visible
            .get(&relation)
            .expect("ensured functional visibility map should exist");
        if let Some(tuple) = visible.tracked_tuple(tuple) {
            return Ok(tuple);
        }
        self.base
            .relation(relation)
            .map(|base_relation| base_relation.tuple_for_key(positions, tuple))
    }

    fn ensure_functional_visible(
        &mut self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<(), KernelError> {
        if self.functional_visible.contains_key(&relation) {
            return Ok(());
        }

        let base_relation = self.base.relation(relation)?;
        let visible =
            FunctionalVisibleMap::from_writes(positions, self.writes.get(&relation), |tuple| {
                base_relation.tuple_for_key(positions, tuple)
            });
        self.functional_visible.insert(relation, visible);
        Ok(())
    }

    fn apply_local_change(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
        change: LocalChange,
    ) -> Result<(), KernelError> {
        let metadata = self.base.relation(relation)?.metadata();
        if self.base.computed_relations.is_computed_relation(metadata) {
            return Err(KernelError::ReadOnlyRelation(relation));
        }
        self.base.relation(relation)?.validate_tuple(&tuple)?;
        self.writes
            .entry(relation)
            .or_default()
            .insert(tuple.clone(), change);
        match change {
            LocalChange::Assert => crate::metrics::metrics()
                .transaction_write_operations
                .inc(TransactionWriteOperation::Assert),
            LocalChange::Retract => crate::metrics::metrics()
                .transaction_write_operations
                .inc(TransactionWriteOperation::Retract),
        }
        self.record_functional_change(relation, &tuple, change)?;
        *self.derived_cache.borrow_mut() = None;
        Ok(())
    }

    fn record_functional_change(
        &mut self,
        relation: RelationId,
        tuple: &Tuple,
        change: LocalChange,
    ) -> Result<(), KernelError> {
        if self.functional_visible.is_empty() {
            return Ok(());
        }

        if !self.functional_visible.contains_key(&relation) {
            return Ok(());
        }
        let base_relation = self.base.relation(relation)?;
        self.functional_visible
            .get_mut(&relation)
            .expect("checked functional visibility map should exist")
            .record_change_with_base_lookup(tuple, change, |positions| {
                base_relation.tuple_for_key(positions, tuple)
            });
        Ok(())
    }

    fn validate_conflicts(&self, current: &Snapshot) -> Result<(), KernelError> {
        for (relation_id, writes) in &self.writes {
            let base_relation = self.base.relation(*relation_id)?;
            let current_relation = current.relation(*relation_id)?;
            match base_relation.metadata().conflict_policy() {
                ConflictPolicy::Set => self.validate_set_conflicts(
                    *relation_id,
                    writes,
                    base_relation,
                    current_relation,
                )?,
                ConflictPolicy::Functional { key_positions } => {
                    self.validate_functional_conflicts(
                        *relation_id,
                        key_positions,
                        writes,
                        base_relation,
                        current_relation,
                    )?;
                }
                ConflictPolicy::EventAppend => {}
            }
        }
        Ok(())
    }

    fn validate_set_conflicts(
        &self,
        relation_id: RelationId,
        writes: &RelationWriteOverlay,
        base_relation: &RelationState,
        current_relation: &RelationState,
    ) -> Result<(), KernelError> {
        writes.try_for_each(|tuple, change| {
            if matches!(change, LocalChange::Assert)
                && base_relation.contains_tuple(tuple)
                && !current_relation.contains_tuple(tuple)
            {
                Err(KernelError::Conflict(Conflict {
                    relation: relation_id,
                    tuple: tuple.clone(),
                    kind: ConflictKind::AssertRetract,
                }))
            } else {
                Ok(())
            }
        })
    }

    fn validate_functional_conflicts(
        &self,
        relation_id: RelationId,
        key_positions: &[u16],
        writes: &RelationWriteOverlay,
        base_relation: &RelationState,
        current_relation: &RelationState,
    ) -> Result<(), KernelError> {
        if let Some(visible) = self.functional_visible.get(&relation_id) {
            for entry in visible.conflict_entries() {
                let base = base_relation.tuple_for_key(key_positions, entry.representative);
                let current = current_relation.tuple_for_key(key_positions, entry.representative);
                if base != current {
                    return Err(KernelError::Conflict(Conflict {
                        relation: relation_id,
                        tuple: entry.tuple.clone().or(base).unwrap_or_else(|| {
                            entry.representative.select(key_positions.iter().copied())
                        }),
                        kind: ConflictKind::FunctionalKeyChanged,
                    }));
                }
            }
            return Ok(());
        }

        writes.visit_touched_projected_keys(key_positions, |key, representative| {
            let base = base_relation.tuple_for_projected_key(key_positions, key);
            let current = current_relation.tuple_for_projected_key(key_positions, key);
            if base != current {
                Err(KernelError::Conflict(Conflict {
                    relation: relation_id,
                    tuple: representative.clone(),
                    kind: ConflictKind::FunctionalKeyChanged,
                }))
            } else {
                Ok(())
            }
        })
    }

    fn build_next_snapshot(
        &self,
        current: &Snapshot,
    ) -> Result<(Arc<Snapshot>, Commit), KernelError> {
        let mut next = current.clone();
        let mut changes = Vec::new();

        let mut relation_ids = self.writes.keys().copied().collect::<Vec<_>>();
        relation_ids.sort();

        for relation_id in relation_ids {
            let writes = self
                .writes
                .get(&relation_id)
                .expect("relation id should come from write set");
            let relation = next
                .relations
                .get_mut(&relation_id)
                .ok_or(KernelError::UnknownRelation(relation_id))?;
            let base_relation = self.base.relation(relation_id)?;
            writes.apply_ordered_changes(relation, base_relation, |tuple, kind| {
                changes.push(FactChange {
                    relation: relation_id,
                    tuple: tuple.clone(),
                    kind: match kind {
                        RelationMutationKind::Assert => FactChangeKind::Assert,
                        RelationMutationKind::Retract => FactChangeKind::Retract,
                    },
                });
            });
        }

        next.version = current.version() + 1;
        next.derived_cache = empty_derived_cache();
        next.packed_cache = empty_packed_cache();
        next.dispatch_cache = empty_dispatch_cache();
        next.method_program_cache = empty_method_program_cache();
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([]),
            changes: changes.into(),
        };
        next.commits = current.commits.append(commit.clone());
        Ok((Arc::new(next), commit))
    }
}

impl RelationWorkspace for Transaction<'_> {
    fn assert_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.assert(relation, tuple)
    }

    fn retract_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.retract(relation, tuple)
    }

    fn replace_functional_tuple(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError> {
        self.replace_functional(relation, tuple)
    }
}

impl DispatchRead for Transaction<'_> {
    fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls(relations, selector, roles)
            .map(Some)
    }

    fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls_normalized(relations, selector, roles)
            .map(Some)
    }

    fn cached_method_program(
        &self,
        relation: RelationId,
        method: &Value,
    ) -> Result<Option<Option<Value>>, KernelError> {
        self.cached_method_program(relation, method).map(Some)
    }

    fn cached_applicable_positional_methods(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        args: &[Value],
    ) -> Result<Option<Vec<Value>>, KernelError> {
        self.cached_applicable_positional_methods(relations, selector, args)
            .map(Some)
    }
}

impl RelationRead for Transaction<'_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.scan(relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.visit(relation, bindings, visitor)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.estimate_scan(relation, bindings).map(Some)
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        if !self.has_local_writes(relation) {
            return self.base.relation_capabilities(relation);
        }
        let mut capabilities = self.base.extensional_relation_capabilities(relation)?;
        capabilities.source = RelationSource::TransactionOverlay;
        capabilities.cardinality = capabilities
            .cardinality
            .map(|rows| rows.saturating_add(self.writes[&relation].len()));
        capabilities.exact_indexes.clear();
        capabilities.value_domains =
            vec![ValueDomain::Unknown; self.base.relation(relation)?.metadata().arity() as usize];
        capabilities.supports_batch_export = false;
        Ok(capabilities)
    }

    fn export_relation_batch(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<Arc<PackedRelation>>, KernelError> {
        if self.has_local_writes(relation) {
            return Ok(None);
        }
        self.base.export_relation_batch(relation, bindings)
    }

    fn has_exact_relation_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        self.base.relation_has_exact_index(relation, positions)
    }

    fn join_relation_scans(
        &self,
        left_relation: RelationId,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right_relation: RelationId,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        if !relation_has_active_rule_head(self.base.rules(), left_relation)
            && !relation_has_active_rule_head(self.base.rules(), right_relation)
            && !self.has_local_writes(left_relation)
            && !self.has_local_writes(right_relation)
            && let Some(rows) = self.base.join_extensional_relation_scans(
                left_relation,
                left_bindings,
                left_positions,
                right_relation,
                right_bindings,
                right_positions,
            )?
        {
            return Ok(Some(rows));
        }

        let left_rows = self.scan(left_relation, left_bindings)?;
        let right_rows = self.scan(right_relation, right_bindings)?;
        Ok(Some(crate::query::join_eq(
            left_rows,
            right_rows,
            left_positions,
            right_positions,
        )))
    }
}

impl ComputedRelationRead for Transaction<'_> {
    fn version(&self) -> Version {
        self.base.version()
    }

    fn relation_metadata_vec(&self) -> Vec<crate::RelationMetadata> {
        self.base.relation_metadata().cloned().collect()
    }

    fn relation_id(&self, name: Symbol, arity: u16) -> Option<RelationId> {
        self.base
            .relations
            .values()
            .map(|relation| relation.metadata())
            .find(|metadata| metadata.name() == name && metadata.arity() == arity)
            .map(|metadata| metadata.id())
    }

    fn rules_vec(&self) -> Vec<crate::RuleDefinition> {
        self.base.rules().to_vec()
    }

    fn extensional_facts(&self) -> Result<Vec<(RelationId, Tuple)>, KernelError> {
        self.extensional_facts_with_local_writes()
    }
}

struct ExtensionalTransactionReader<'a, 'kernel> {
    tx: &'a Transaction<'kernel>,
}

impl crate::RelationRead for ExtensionalTransactionReader<'_, '_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        Ok(self
            .tx
            .scan_extensional(relation, bindings)?
            .into_iter()
            .collect())
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.tx
            .estimate_extensional_scan(relation, bindings)
            .map(Some)
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        if !self.tx.has_local_writes(relation) {
            return self.tx.base.extensional_relation_capabilities(relation);
        }
        let mut capabilities = self.tx.base.extensional_relation_capabilities(relation)?;
        capabilities.source = RelationSource::TransactionOverlay;
        capabilities.cardinality = capabilities
            .cardinality
            .map(|rows| rows.saturating_add(self.tx.writes[&relation].len()));
        capabilities.exact_indexes.clear();
        capabilities.value_domains = vec![
            ValueDomain::Unknown;
            self.tx.base.relation(relation)?.metadata().arity()
                as usize
        ];
        capabilities.supports_batch_export = false;
        Ok(capabilities)
    }

    fn has_exact_relation_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        if self.tx.has_local_writes(relation) {
            return Ok(false);
        }
        self.tx.base.relation_has_exact_index(relation, positions)
    }

    fn join_relation_scans(
        &self,
        left_relation: RelationId,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right_relation: RelationId,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        if !self.tx.has_local_writes(left_relation)
            && !self.tx.has_local_writes(right_relation)
            && let Some(rows) = self.tx.base.join_extensional_relation_scans(
                left_relation,
                left_bindings,
                left_positions,
                right_relation,
                right_bindings,
                right_positions,
            )?
        {
            return Ok(Some(rows));
        }
        Ok(None)
    }
}

impl From<LocalChange> for RelationMutationKind {
    fn from(change: LocalChange) -> Self {
        match change {
            LocalChange::Assert => Self::Assert,
            LocalChange::Retract => Self::Retract,
        }
    }
}
