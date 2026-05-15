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
use crate::snapshot::{Commit, CommitResult, FactChange, FactChangeKind};
use crate::snapshot::{active_rules, empty_derived_cache};
use crate::{
    Conflict, ConflictKind, ConflictPolicy, KernelError, RelationId, RelationKernel,
    RelationWorkspace, RuleSet, ScanControl, Snapshot, Tuple, Version,
};
use mica_var::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub struct Transaction<'a> {
    kernel: &'a RelationKernel,
    pub(crate) base: Arc<Snapshot>,
    writes: BTreeMap<RelationId, BTreeMap<Tuple, LocalChange>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(kernel: &'a RelationKernel, base: Arc<Snapshot>) -> Self {
        Self {
            kernel,
            base,
            writes: BTreeMap::new(),
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

    pub fn assert(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.writes
            .entry(relation)
            .or_default()
            .insert(tuple, LocalChange::Assert);
        Ok(())
    }

    pub fn retract(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.writes
            .entry(relation)
            .or_default()
            .insert(tuple, LocalChange::Retract);
        Ok(())
    }

    pub fn replace_functional(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        let base_relation = self.base.relation(relation)?;
        let ConflictPolicy::Functional { key_positions } =
            base_relation.metadata().conflict_policy()
        else {
            self.assert(relation, tuple)?;
            return Ok(());
        };

        if let Some(old_tuple) = self.visible_tuple_for_key(relation, key_positions, &tuple)? {
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

        let mut visible = self.scan_extensional(relation, bindings)?;

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
            }
        }

        Ok(visible.into_iter().collect())
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
        let mut visible = self
            .base
            .scan_extensional(relation, bindings)?
            .into_iter()
            .collect::<BTreeSet<_>>();

        if let Some(writes) = self.writes.get(&relation) {
            for (tuple, change) in writes {
                if !tuple.matches_bindings(bindings) {
                    continue;
                }
                match change {
                    LocalChange::Assert => {
                        visible.insert(tuple.clone());
                    }
                    LocalChange::Retract => {
                        visible.remove(tuple);
                    }
                }
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
        let write_bloom = self.write_bloom()?;
        let _guard = self.kernel.commit_guard();
        let current = self.kernel.snapshot();
        self.validate_conflicts(&current)?;
        let (next, commit) = self.build_next_snapshot(&current, write_bloom)?;
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
        &self,
        relation: RelationId,
        positions: &[u16],
        tuple: &Tuple,
    ) -> Result<Option<Tuple>, KernelError> {
        let mut visible = self
            .base
            .relation(relation)?
            .tuple_for_key(positions, tuple);
        if let Some(writes) = self.writes.get(&relation) {
            let key = tuple.project(positions);
            for (candidate, change) in writes {
                if candidate.project(positions) != key {
                    continue;
                }
                match change {
                    LocalChange::Assert => visible = Some(candidate.clone()),
                    LocalChange::Retract => {
                        if visible.as_ref() == Some(candidate) {
                            visible = None;
                        }
                    }
                }
            }
        }
        Ok(visible)
    }

    fn validate_conflicts(&self, current: &Snapshot) -> Result<(), KernelError> {
        for (relation_id, writes) in &self.writes {
            let base_relation = self.base.relation(*relation_id)?;
            let current_relation = current.relation(*relation_id)?;
            match base_relation.metadata().conflict_policy() {
                ConflictPolicy::Set => {
                    for (tuple, change) in writes {
                        if matches!(change, LocalChange::Assert)
                            && base_relation.tuples.contains(tuple)
                            && !current_relation.tuples.contains(tuple)
                        {
                            return Err(KernelError::Conflict(Conflict {
                                relation: *relation_id,
                                tuple: tuple.clone(),
                                kind: ConflictKind::AssertRetract,
                            }));
                        }
                    }
                }
                ConflictPolicy::Functional { key_positions } => {
                    let touched_keys = writes
                        .keys()
                        .map(|tuple| (tuple.project(key_positions), tuple.clone()))
                        .collect::<BTreeMap<_, _>>();
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
        bloom: CommitBloom,
    ) -> Result<(Arc<Snapshot>, Commit), KernelError> {
        let mut next = current.clone();
        let mut changes = Vec::new();

        for (relation_id, writes) in &self.writes {
            let relation = next
                .relations
                .get_mut(relation_id)
                .ok_or(KernelError::UnknownRelation(*relation_id))?;
            for (tuple, change) in writes {
                match change {
                    LocalChange::Assert => {
                        if !relation.tuples.contains(tuple) {
                            changes.push(FactChange {
                                relation: *relation_id,
                                tuple: tuple.clone(),
                                kind: FactChangeKind::Assert,
                            });
                        }
                        relation.insert(tuple.clone());
                    }
                    LocalChange::Retract => {
                        if self.base.relation(*relation_id)?.tuples.contains(tuple)
                            && relation.tuples.contains(tuple)
                        {
                            changes.push(FactChange {
                                relation: *relation_id,
                                tuple: tuple.clone(),
                                kind: FactChangeKind::Retract,
                            });
                            relation.remove(tuple);
                        }
                    }
                }
            }
        }

        next.version = current.version() + 1;
        next.derived_cache = empty_derived_cache();
        let commit = Commit {
            version: next.version,
            catalog_changes: Arc::from([]),
            changes: changes.into(),
            bloom,
        };
        let mut commits = Vec::with_capacity(current.commits.len() + 1);
        commits.extend(current.commits.iter().cloned());
        commits.push(commit.clone());
        next.commits = commits.into();
        Ok((Arc::new(next), commit))
    }

    fn write_bloom(&self) -> Result<CommitBloom, KernelError> {
        let mut bloom = CommitBloom::new();
        for (relation_id, writes) in &self.writes {
            let relation = self.base.relation(*relation_id)?;
            for tuple in writes.keys() {
                let key = match relation.metadata().conflict_policy() {
                    ConflictPolicy::Functional { key_positions } => tuple.project(key_positions),
                    ConflictPolicy::Set | ConflictPolicy::EventAppend => {
                        let positions = (0..tuple.arity() as u16).collect::<Vec<_>>();
                        tuple.project(&positions)
                    }
                };
                bloom.insert(&ModifiedKey {
                    relation: *relation_id,
                    key,
                });
            }
        }
        Ok(bloom)
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalChange {
    Assert,
    Retract,
}

#[derive(Hash)]
struct ModifiedKey {
    relation: RelationId,
    key: crate::tuple::TupleKey,
}
