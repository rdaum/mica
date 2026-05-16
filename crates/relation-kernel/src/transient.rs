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
use crate::snapshot::active_rules;
use crate::{
    ApplicableMethodCall, DispatchRelations, KernelError, RelationId, RelationMetadata,
    RelationRead, RuleSet, ScanControl, Transaction, Tuple,
};
use mica_var::{Identity, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default)]
pub struct TransientStore {
    scopes: BTreeMap<Identity, TransientScopeState>,
}

impl TransientStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    pub fn scope_len(&self, scope: Identity) -> usize {
        self.scopes
            .get(&scope)
            .map(TransientScopeState::len)
            .unwrap_or(0)
    }

    pub fn scopes(&self) -> impl Iterator<Item = Identity> + '_ {
        self.scopes.keys().copied()
    }

    pub fn assert(
        &mut self,
        scope: Identity,
        metadata: RelationMetadata,
        tuple: Tuple,
    ) -> Result<bool, KernelError> {
        self.scopes
            .entry(scope)
            .or_default()
            .assert(metadata, tuple)
    }

    pub fn retract(&mut self, scope: Identity, relation: RelationId, tuple: &Tuple) -> bool {
        let Some(scope_state) = self.scopes.get_mut(&scope) else {
            return false;
        };
        let removed = scope_state.retract(relation, tuple);
        if scope_state.is_empty() {
            self.scopes.remove(&scope);
        }
        removed
    }

    pub fn drop_scope(&mut self, scope: Identity) -> usize {
        self.scopes
            .remove(&scope)
            .map(|scope| scope.len())
            .unwrap_or(0)
    }

    pub fn scan(
        &self,
        scopes: &[Identity],
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut rows = BTreeSet::new();
        for scope in scopes {
            let Some(scope_state) = self.scopes.get(scope) else {
                continue;
            };
            rows.extend(scope_state.scan(relation, bindings)?);
        }
        Ok(rows.into_iter().collect())
    }

    pub fn relation_visible(&self, scopes: &[Identity], relation: RelationId) -> bool {
        scopes.iter().any(|scope| {
            self.scopes
                .get(scope)
                .is_some_and(|scope| scope.contains_relation(relation))
        })
    }
}

#[derive(Clone, Debug, Default)]
struct TransientScopeState {
    relations: BTreeMap<RelationId, RelationState>,
}

impl TransientScopeState {
    fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    fn len(&self) -> usize {
        self.relations
            .values()
            .map(|relation| relation.tuples.len())
            .sum()
    }

    fn contains_relation(&self, relation: RelationId) -> bool {
        self.relations.contains_key(&relation)
    }

    fn assert(&mut self, metadata: RelationMetadata, tuple: Tuple) -> Result<bool, KernelError> {
        let relation_id = metadata.id();
        let relation = match self.relations.get_mut(&relation_id) {
            Some(existing) => {
                if existing.metadata().arity() != metadata.arity() {
                    return Err(KernelError::ArityMismatch {
                        relation: relation_id,
                        expected: existing.metadata().arity(),
                        actual: metadata.arity() as usize,
                    });
                }
                existing
            }
            None => self
                .relations
                .entry(relation_id)
                .or_insert(RelationState::empty(metadata)?),
        };
        if tuple.arity() != relation.metadata().arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: relation_id,
                expected: relation.metadata().arity(),
                actual: tuple.arity(),
            });
        }
        let inserted = !relation.tuples.contains(&tuple);
        relation.insert(tuple);
        Ok(inserted)
    }

    fn retract(&mut self, relation: RelationId, tuple: &Tuple) -> bool {
        let Some(relation_state) = self.relations.get_mut(&relation) else {
            return false;
        };
        let removed = relation_state.tuples.contains(tuple);
        relation_state.remove(tuple);
        if relation_state.tuples.is_empty() {
            self.relations.remove(&relation);
        }
        removed
    }

    fn scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.relations
            .get(&relation)
            .map(|state| state.scan(bindings))
            .unwrap_or_else(|| Ok(Vec::new()))
    }
}

pub struct ComposedRelationRead<'a, R> {
    base: &'a R,
    transient: &'a TransientStore,
    scopes: &'a [Identity],
}

impl<'a, R> ComposedRelationRead<'a, R> {
    pub fn new(base: &'a R, transient: &'a TransientStore, scopes: &'a [Identity]) -> Self {
        Self {
            base,
            transient,
            scopes,
        }
    }
}

impl<R: RelationRead> RelationRead for ComposedRelationRead<'_, R> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        scan_composed_relation(self.base, self.transient, self.scopes, relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if !self.transient.relation_visible(self.scopes, relation) {
            return self.base.visit_relation(relation, bindings, visitor);
        }
        for tuple in self.scan_relation(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }
}

pub struct ComposedTransactionRead<'a, 'kernel> {
    tx: &'a Transaction<'kernel>,
    transient: &'a TransientStore,
    scopes: &'a [Identity],
}

impl<'a, 'kernel> ComposedTransactionRead<'a, 'kernel> {
    pub fn new(
        tx: &'a Transaction<'kernel>,
        transient: &'a TransientStore,
        scopes: &'a [Identity],
    ) -> Self {
        Self {
            tx,
            transient,
            scopes,
        }
    }
}

impl RelationRead for ComposedTransactionRead<'_, '_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut visible = scan_composed_transaction_extensional(
            self.tx,
            self.transient,
            self.scopes,
            relation,
            bindings,
        )?;

        if !self.tx.base.rules().is_empty() {
            let reader = ComposedExtensionalTransactionRead {
                tx: self.tx,
                transient: self.transient,
                scopes: self.scopes,
            };
            let derived = RuleSet::new(active_rules(self.tx.base.rules()))
                .evaluate_fixpoint(&reader)
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

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if !self.transient.relation_visible(self.scopes, relation) {
            return self.tx.visit_relation(relation, bindings, visitor);
        }
        for tuple in self.scan_relation(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }

    fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        if self.dispatch_cache_is_transient(relations) {
            return Ok(None);
        }
        self.tx
            .cached_applicable_method_calls(relations, selector, roles)
            .map(Some)
    }

    fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        if self.dispatch_cache_is_transient(relations) {
            return Ok(None);
        }
        self.tx
            .cached_applicable_method_calls_normalized(relations, selector, roles)
            .map(Some)
    }
}

struct ComposedExtensionalTransactionRead<'a, 'kernel> {
    tx: &'a Transaction<'kernel>,
    transient: &'a TransientStore,
    scopes: &'a [Identity],
}

impl RelationRead for ComposedExtensionalTransactionRead<'_, '_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        scan_composed_transaction_extensional(
            self.tx,
            self.transient,
            self.scopes,
            relation,
            bindings,
        )
        .map(|rows| rows.into_iter().collect())
    }
}

fn scan_composed_relation(
    base: &impl RelationRead,
    transient: &TransientStore,
    scopes: &[Identity],
    relation: RelationId,
    bindings: &[Option<Value>],
) -> Result<Vec<Tuple>, KernelError> {
    let mut base_unknown = false;
    let mut rows = match base.scan_relation(relation, bindings) {
        Ok(rows) => rows.into_iter().collect::<BTreeSet<_>>(),
        Err(KernelError::UnknownRelation(unknown)) if unknown == relation => {
            base_unknown = true;
            BTreeSet::new()
        }
        Err(error) => return Err(error),
    };
    let transient_visible = transient.relation_visible(scopes, relation);
    rows.extend(transient.scan(scopes, relation, bindings)?);
    if base_unknown && !transient_visible {
        return Err(KernelError::UnknownRelation(relation));
    }
    Ok(rows.into_iter().collect())
}

fn scan_composed_transaction_extensional(
    tx: &Transaction<'_>,
    transient: &TransientStore,
    scopes: &[Identity],
    relation: RelationId,
    bindings: &[Option<Value>],
) -> Result<BTreeSet<Tuple>, KernelError> {
    let mut base_unknown = false;
    let mut rows = match tx.scan_extensional(relation, bindings) {
        Ok(rows) => rows,
        Err(KernelError::UnknownRelation(unknown)) if unknown == relation => {
            base_unknown = true;
            BTreeSet::new()
        }
        Err(error) => return Err(error),
    };
    let transient_visible = transient.relation_visible(scopes, relation);
    rows.extend(transient.scan(scopes, relation, bindings)?);
    if base_unknown && !transient_visible {
        return Err(KernelError::UnknownRelation(relation));
    }
    Ok(rows)
}

fn dispatch_relation_can_be_derived(tx: &Transaction<'_>, relations: DispatchRelations) -> bool {
    let dispatch_relations = [
        relations.method_selector,
        relations.param,
        relations.delegates,
    ];
    tx.base
        .rules()
        .iter()
        .any(|rule| rule.active() && dispatch_relations.contains(&rule.rule().head_relation()))
}

impl ComposedTransactionRead<'_, '_> {
    fn dispatch_cache_is_transient(&self, relations: DispatchRelations) -> bool {
        let dispatch_relations = [
            relations.method_selector,
            relations.param,
            relations.delegates,
        ];
        dispatch_relations
            .iter()
            .any(|relation| self.transient.relation_visible(self.scopes, *relation))
            || dispatch_relation_can_be_derived(self.tx, relations)
                && self
                    .scopes
                    .iter()
                    .any(|scope| self.transient.scope_len(*scope) > 0)
    }
}
