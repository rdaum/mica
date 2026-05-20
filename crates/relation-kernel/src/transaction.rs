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

use crate::commit_bloom::CommitBloom;
use crate::index::{RelationMutationKind, TupleStore};
use crate::snapshot::{Commit, CommitResult, FactChange, FactChangeKind};
use crate::snapshot::{
    active_rules, empty_derived_cache, empty_dispatch_cache, empty_method_program_cache,
};
use crate::tuple::{difference_ordered_tuple_rows, finish_tuple_rows, union_ordered_tuple_rows};
use crate::{
    ApplicableMethodCall, Conflict, ConflictKind, ConflictPolicy, DispatchRelations, KernelError,
    RelationId, RelationKernel, RelationWorkspace, RuleSet, ScanControl, Snapshot, Tuple, Version,
};
use mica_var::Value;
use rart::{AdaptiveRadixTree, Slot, SlotUpdate};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct Transaction<'a> {
    kernel: &'a RelationKernel,
    pub(crate) base: Arc<Snapshot>,
    writes: HashMap<RelationId, RelationWriteOverlay>,
    functional_visible: HashMap<RelationId, FunctionalVisibleMap>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(kernel: &'a RelationKernel, base: Arc<Snapshot>) -> Self {
        Self {
            kernel,
            base,
            writes: HashMap::new(),
            functional_visible: HashMap::new(),
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
        if !self.is_read_only() {
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
        if !self.is_read_only() {
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
        if !self.is_read_only() {
            return crate::query::method_program_id_uncached(self, relation, method);
        }
        self.base.cached_method_program(relation, method)
    }

    pub fn assert(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.writes
            .entry(relation)
            .or_default()
            .insert(tuple.clone(), LocalChange::Assert);
        self.record_functional_change(relation, &tuple, LocalChange::Assert)?;
        Ok(())
    }

    pub fn retract(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.writes
            .entry(relation)
            .or_default()
            .insert(tuple.clone(), LocalChange::Retract);
        self.record_functional_change(relation, &tuple, LocalChange::Retract)?;
        Ok(())
    }

    pub fn replace_functional(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
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
        self.assert(relation, tuple)
    }

    pub fn scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        if self.base.rules().is_empty() && !self.writes.contains_key(&relation) {
            return self.base.scan_extensional(relation, bindings);
        }

        let mut visible = self.scan_extensional_rows(relation, bindings)?;

        if !self.base.rules().is_empty() {
            let derived = RuleSet::new(active_rules(self.base.rules()))
                .evaluate_fixpoint(&ExtensionalTransactionReader { tx: self })
                .map_err(KernelError::from)?;
            if let Some(rows) = derived.get(&relation) {
                visible.extend(
                    rows.iter()
                        .filter(|tuple| tuple.matches_bindings(bindings))
                        .cloned(),
                );
                visible = finish_tuple_rows(visible);
            }
        }

        Ok(visible)
    }

    pub(crate) fn estimate_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        if self.base.rules().is_empty() && !self.writes.contains_key(&relation) {
            return self.base.estimate_extensional_scan(relation, bindings);
        }
        Ok(self.scan(relation, bindings)?.len())
    }

    pub(crate) fn estimate_extensional_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        if !self.writes.contains_key(&relation) {
            return self.base.estimate_extensional_scan(relation, bindings);
        }
        Ok(self.scan_extensional(relation, bindings)?.len())
    }

    pub fn visit(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if self.base.rules().is_empty() && !self.writes.contains_key(&relation) {
            return self.base.visit_extensional(relation, bindings, visitor);
        }

        for tuple in self.scan(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
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
                return Ok(local_asserts);
            }
            if !local_asserts.is_empty() {
                visible = union_ordered_tuple_rows(visible, local_asserts);
            }
            if !local_retracts.is_empty() {
                visible = difference_ordered_tuple_rows(visible, local_retracts);
            }
        }

        Ok(visible)
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

    fn validate_tuple(&self, relation: RelationId, tuple: &Tuple) -> Result<(), KernelError> {
        let metadata = self.base.relation(relation)?.metadata();
        if metadata.arity() as usize != tuple.arity() {
            return Err(KernelError::ArityMismatch {
                relation,
                expected: metadata.arity(),
                actual: tuple.arity(),
            });
        }
        if tuple.values().iter().any(|value| !value.is_persistable()) {
            return Err(KernelError::NonPersistentValue {
                relation,
                tuple: tuple.clone(),
            });
        }
        Ok(())
    }

    fn visible_tuple_for_key(
        &mut self,
        relation: RelationId,
        positions: &[u16],
        tuple: &Tuple,
    ) -> Result<Option<Tuple>, KernelError> {
        self.ensure_functional_visible(relation, positions)?;
        let key = projected_key(tuple, positions);
        let visible = self
            .functional_visible
            .get(&relation)
            .expect("ensured functional visibility map should exist");
        if let Some(entry) = visible.tuples.get_k(&key) {
            return Ok(entry.tuple.clone());
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
        let mut tuples = AdaptiveRadixTree::new();
        if let Some(writes) = self.writes.get(&relation) {
            writes.for_each(|tuple, change| {
                let key = projected_key(tuple, positions);
                tuples.update_k(&key, |slot| match slot {
                    Slot::Vacant => {
                        let mut current = base_relation.tuple_for_key(positions, tuple);
                        match change {
                            LocalChange::Assert => current = Some(tuple.clone()),
                            LocalChange::Retract => {
                                if current.as_ref() == Some(tuple) {
                                    current = None;
                                }
                            }
                        }
                        SlotUpdate::Insert(FunctionalVisibleEntry {
                            representative: tuple.clone(),
                            tuple: current,
                        })
                    }
                    Slot::Occupied(entry) => {
                        match change {
                            LocalChange::Assert => entry.tuple = Some(tuple.clone()),
                            LocalChange::Retract => {
                                if entry.tuple.as_ref() == Some(tuple) {
                                    entry.tuple = None;
                                }
                            }
                        }
                        SlotUpdate::Keep
                    }
                });
            });
        }
        self.functional_visible.insert(
            relation,
            FunctionalVisibleMap {
                positions: positions.to_vec(),
                tuples,
            },
        );
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

        let Some(visible) = self.functional_visible.get(&relation) else {
            return Ok(());
        };
        let positions = visible.positions.clone();
        let key = projected_key(tuple, &positions);
        match change {
            LocalChange::Assert => {
                self.functional_visible
                    .get_mut(&relation)
                    .expect("checked functional visibility map should exist")
                    .tuples
                    .update_k(&key, |slot| match slot {
                        Slot::Vacant => SlotUpdate::Insert(FunctionalVisibleEntry {
                            representative: tuple.clone(),
                            tuple: Some(tuple.clone()),
                        }),
                        Slot::Occupied(entry) => {
                            entry.tuple = Some(tuple.clone());
                            SlotUpdate::Keep
                        }
                    });
            }
            LocalChange::Retract => {
                let base_visible = if visible.tuples.get_k(&key).is_some() {
                    None
                } else {
                    self.base
                        .relation(relation)?
                        .tuple_for_key(&positions, tuple)
                };
                let visible = self
                    .functional_visible
                    .get_mut(&relation)
                    .expect("checked functional visibility map should exist");
                visible.tuples.update_k(&key, |slot| match slot {
                    Slot::Vacant => {
                        let mut current = base_visible;
                        if current.as_ref() == Some(tuple) {
                            current = None;
                        }
                        SlotUpdate::Insert(FunctionalVisibleEntry {
                            representative: tuple.clone(),
                            tuple: current,
                        })
                    }
                    Slot::Occupied(entry) => {
                        if entry.tuple.as_ref() == Some(tuple) {
                            entry.tuple = None;
                        }
                        SlotUpdate::Keep
                    }
                });
            }
        }
        Ok(())
    }

    fn validate_conflicts(&self, current: &Snapshot) -> Result<(), KernelError> {
        for (relation_id, writes) in &self.writes {
            let base_relation = self.base.relation(*relation_id)?;
            let current_relation = current.relation(*relation_id)?;
            match base_relation.metadata().conflict_policy() {
                ConflictPolicy::Set => {
                    writes.try_for_each(|tuple, change| {
                        if matches!(change, LocalChange::Assert)
                            && base_relation.tuples.contains(tuple)
                            && !current_relation.tuples.contains(tuple)
                        {
                            Err(KernelError::Conflict(Conflict {
                                relation: *relation_id,
                                tuple: tuple.clone(),
                                kind: ConflictKind::AssertRetract,
                            }))
                        } else {
                            Ok(())
                        }
                    })?;
                }
                ConflictPolicy::Functional { key_positions } => {
                    if let Some(visible) = self.functional_visible.get(relation_id) {
                        for entry in visible.tuples.values_iter() {
                            let base =
                                base_relation.tuple_for_key(key_positions, &entry.representative);
                            let current = current_relation
                                .tuple_for_key(key_positions, &entry.representative);
                            if base != current {
                                return Err(KernelError::Conflict(Conflict {
                                    relation: *relation_id,
                                    tuple: entry.tuple.clone().or(base).unwrap_or_else(|| {
                                        entry.representative.select(key_positions.iter().copied())
                                    }),
                                    kind: ConflictKind::FunctionalKeyChanged,
                                }));
                            }
                        }
                        continue;
                    }

                    let mut touched_keys = BTreeMap::new();
                    writes.for_each(|tuple, _change| {
                        touched_keys.insert(tuple.project(key_positions), tuple.clone());
                    });
                    for (key, representative) in touched_keys {
                        let base = base_relation.tuple_for_projected_key(key_positions, &key);
                        let current = current_relation.tuple_for_projected_key(key_positions, &key);
                        if base != current {
                            return Err(KernelError::Conflict(Conflict {
                                relation: *relation_id,
                                tuple: representative,
                                kind: ConflictKind::FunctionalKeyChanged,
                            }));
                        }
                    }
                }
                ConflictPolicy::EventAppend => {}
            }
        }
        Ok(())
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
            let base_tuples = self.base.relation(relation_id)?.tuples.clone();
            writes.apply_ordered_changes(relation, &base_tuples, |tuple, kind| {
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
        next.dispatch_cache = empty_dispatch_cache();
        next.method_program_cache = empty_method_program_cache();
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([]),
            changes: changes.into(),
            bloom: CommitBloom::new(),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalChange {
    Assert,
    Retract,
}

struct RelationWriteOverlay {
    changes: OverlayChanges,
    scan_indexes: RefCell<HashMap<Vec<u16>, LocalScanIndex>>,
    scan_requests: RefCell<HashMap<Vec<u16>, usize>>,
}

impl RelationWriteOverlay {
    fn insert(&mut self, tuple: Tuple, change: LocalChange) {
        self.changes.insert(tuple, change);
        self.scan_indexes.get_mut().clear();
        self.scan_requests.get_mut().clear();
    }

    fn for_each(&self, mut visitor: impl FnMut(&Tuple, LocalChange)) {
        match &self.changes {
            OverlayChanges::Small(changes) => {
                for entry in changes {
                    visitor(&entry.tuple, entry.change);
                }
            }
            OverlayChanges::Radix(changes) => {
                for entry in changes.values_iter() {
                    visitor(&entry.tuple, entry.change);
                }
            }
        }
    }

    fn try_for_each<E>(
        &self,
        mut visitor: impl FnMut(&Tuple, LocalChange) -> Result<(), E>,
    ) -> Result<(), E> {
        match &self.changes {
            OverlayChanges::Small(changes) => {
                for entry in changes {
                    visitor(&entry.tuple, entry.change)?;
                }
            }
            OverlayChanges::Radix(changes) => {
                for entry in changes.values_iter() {
                    visitor(&entry.tuple, entry.change)?;
                }
            }
        }
        Ok(())
    }

    fn apply_ordered_changes(
        &self,
        relation: &mut crate::index::RelationState,
        base_tuples: &TupleStore,
        mut on_applied: impl FnMut(&Tuple, RelationMutationKind),
    ) {
        match &self.changes {
            OverlayChanges::Small(changes) => {
                if base_tuples.is_empty()
                    && relation.cardinality() == 0
                    && changes
                        .iter()
                        .all(|entry| entry.change == LocalChange::Assert)
                {
                    relation.apply_ordered_asserts_to_empty(
                        changes.iter().map(|entry| &entry.tuple),
                        &mut on_applied,
                    );
                    return;
                }
                relation.apply_ordered_changes(
                    changes
                        .iter()
                        .map(|entry| (&entry.tuple, RelationMutationKind::from(entry.change))),
                    |tuple| base_tuples.contains(tuple),
                    &mut on_applied,
                );
            }
            OverlayChanges::Radix(changes) => {
                if base_tuples.is_empty()
                    && relation.cardinality() == 0
                    && changes
                        .values_iter()
                        .all(|entry| entry.change == LocalChange::Assert)
                {
                    relation.apply_ordered_asserts_to_empty(
                        changes.values_iter().map(|entry| &entry.tuple),
                        &mut on_applied,
                    );
                    return;
                }
                relation.apply_ordered_changes(
                    changes
                        .values_iter()
                        .map(|entry| (&entry.tuple, RelationMutationKind::from(entry.change))),
                    |tuple| base_tuples.contains(tuple),
                    &mut on_applied,
                );
            }
        }
    }

    fn visit_matches(
        &self,
        metadata: &crate::RelationMetadata,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple, LocalChange),
    ) {
        let Some(prefix_positions) = best_local_prefix_positions(metadata, bindings) else {
            self.for_each(|tuple, change| {
                visitor(tuple, change);
            });
            return;
        };

        if !self.scan_indexes.borrow().contains_key(&prefix_positions) {
            let mut requests = self.scan_requests.borrow_mut();
            let request_count = requests.entry(prefix_positions.clone()).or_default();
            *request_count += 1;
            if *request_count == 1 {
                drop(requests);
                self.for_each(|tuple, change| {
                    visitor(tuple, change);
                });
                return;
            }
        }

        self.ensure_scan_index(&prefix_positions);
        let key = crate::index::key_from_values(prefix_positions.iter().map(|position| {
            bindings[*position as usize]
                .as_ref()
                .expect("prefix positions should be bound")
        }));
        let indexes = self.scan_indexes.borrow();
        let index = indexes
            .get(&prefix_positions)
            .expect("ensured local scan index should exist");
        if let Some(rows) = index.rows.get_k(&key) {
            for (tuple, change) in rows {
                visitor(tuple, *change);
            }
        }
    }

    fn ensure_scan_index(&self, positions: &[u16]) {
        if self.scan_indexes.borrow().contains_key(positions) {
            return;
        }

        let mut rows = AdaptiveRadixTree::new();
        self.for_each(|tuple, change| {
            let key = crate::index::key_from_values(
                positions
                    .iter()
                    .map(|position| &tuple.values()[*position as usize]),
            );
            rows.update_k(&key, |slot| match slot {
                Slot::Vacant => SlotUpdate::Insert(vec![(tuple.clone(), change)]),
                Slot::Occupied(rows) => {
                    rows.push((tuple.clone(), change));
                    SlotUpdate::Keep
                }
            });
        });
        self.scan_indexes
            .borrow_mut()
            .insert(positions.to_vec(), LocalScanIndex { rows });
    }
}

impl Default for RelationWriteOverlay {
    fn default() -> Self {
        Self {
            changes: OverlayChanges::Small(Vec::new()),
            scan_indexes: RefCell::new(HashMap::new()),
            scan_requests: RefCell::new(HashMap::new()),
        }
    }
}

const LOCAL_RADIX_OVERLAY_THRESHOLD: usize = 64;

enum OverlayChanges {
    Small(Vec<OverlayEntry>),
    Radix(AdaptiveRadixTree<crate::index::RadixTupleKey, OverlayEntry>),
}

impl From<LocalChange> for RelationMutationKind {
    fn from(change: LocalChange) -> Self {
        match change {
            LocalChange::Assert => Self::Assert,
            LocalChange::Retract => Self::Retract,
        }
    }
}

impl OverlayChanges {
    fn insert(&mut self, tuple: Tuple, change: LocalChange) {
        let promote = match self {
            Self::Small(changes) => {
                match changes.binary_search_by(|entry| entry.tuple.cmp(&tuple)) {
                    Ok(index) => changes[index].change = change,
                    Err(index) => changes.insert(index, OverlayEntry { tuple, change }),
                }
                if changes.len() > LOCAL_RADIX_OVERLAY_THRESHOLD {
                    Some(std::mem::take(changes))
                } else {
                    None
                }
            }
            Self::Radix(changes) => {
                let key = crate::index::key_from_values(tuple.values());
                changes.insert_k(&key, OverlayEntry { tuple, change });
                None
            }
        };

        if let Some(changes) = promote {
            let mut radix = AdaptiveRadixTree::new();
            for entry in changes {
                let key = crate::index::key_from_values(entry.tuple.values());
                radix.insert_k(&key, entry);
            }
            *self = Self::Radix(radix);
        }
    }
}

struct OverlayEntry {
    tuple: Tuple,
    change: LocalChange,
}

struct LocalScanIndex {
    rows: AdaptiveRadixTree<crate::index::RadixTupleKey, Vec<(Tuple, LocalChange)>>,
}

struct FunctionalVisibleMap {
    positions: Vec<u16>,
    tuples: AdaptiveRadixTree<crate::index::RadixTupleKey, FunctionalVisibleEntry>,
}

struct FunctionalVisibleEntry {
    representative: Tuple,
    tuple: Option<Tuple>,
}

fn best_local_prefix_positions(
    metadata: &crate::RelationMetadata,
    bindings: &[Option<Value>],
) -> Option<Vec<u16>> {
    metadata
        .indexes
        .iter()
        .map(|index| (index, index.leading_bound_count(bindings)))
        .filter(|(_, count)| *count > 0)
        .max_by_key(|(_, count)| *count)
        .map(|(index, count)| index.positions.iter().take(count).copied().collect())
}

fn projected_key(tuple: &Tuple, positions: &[u16]) -> crate::index::RadixTupleKey {
    crate::index::key_from_values(
        positions
            .iter()
            .map(|position| &tuple.values()[*position as usize]),
    )
}
