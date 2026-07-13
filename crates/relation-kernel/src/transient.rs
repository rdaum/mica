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
use crate::snapshot::{active_rules, build_derived_relations, relation_has_active_rule_head};
use crate::{
    ApplicableMethodCall, DispatchRead, DispatchRelations, KernelError, RelationCapabilities,
    RelationId, RelationMetadata, RelationRead, RelationSource, RuleSet, ScanControl, Transaction,
    Tuple, ValueDomain,
};
use mica_var::{Identity, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct TransientStore {
    scopes: HashMap<Identity, TransientScopeState>,
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

    pub fn len(&self) -> usize {
        self.scopes.values().map(TransientScopeState::len).sum()
    }

    pub fn scope_count(&self) -> usize {
        self.scopes.len()
    }

    pub fn scopes(&self) -> impl Iterator<Item = Identity> + '_ {
        let mut scopes = self.scopes.keys().copied().collect::<Vec<_>>();
        scopes.sort();
        scopes.into_iter()
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

    pub fn assert_many(
        &mut self,
        scope: Identity,
        tuples: impl IntoIterator<Item = (RelationMetadata, Tuple)>,
    ) -> Result<usize, KernelError> {
        let mut tuples = tuples.into_iter();
        let Some((first_metadata, first_tuple)) = tuples.next() else {
            return Ok(0);
        };
        let scope_state = self.scopes.entry(scope).or_default();
        let mut inserted = 0;
        if scope_state.assert(first_metadata, first_tuple)? {
            inserted += 1;
        }
        for (metadata, tuple) in tuples {
            if scope_state.assert(metadata, tuple)? {
                inserted += 1;
            }
        }
        Ok(inserted)
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

    pub fn retract_many(
        &mut self,
        scope: Identity,
        tuples: impl IntoIterator<Item = (RelationId, Tuple)>,
    ) -> usize {
        let Some(scope_state) = self.scopes.get_mut(&scope) else {
            return 0;
        };
        let mut removed = 0;
        for (relation, tuple) in tuples {
            if scope_state.retract(relation, &tuple) {
                removed += 1;
            }
        }
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

    fn relation_summary(
        &self,
        scopes: &[Identity],
        relation: RelationId,
    ) -> Option<(usize, Vec<ValueDomain>)> {
        let mut rows = 0usize;
        let mut domains: Option<Vec<ValueDomain>> = None;
        for scope in scopes {
            let Some(relation) = self
                .scopes
                .get(scope)
                .and_then(|scope| scope.relations.get(&relation))
            else {
                continue;
            };
            let relation_rows = relation.cardinality();
            let relation_domains = relation.value_domains();
            domains = Some(match domains {
                None => relation_domains,
                Some(current) => {
                    combine_transient_domains(&current, rows, &relation_domains, relation_rows)
                }
            });
            rows = rows.saturating_add(relation_rows);
        }
        domains.map(|domains| (rows, domains))
    }
}

#[derive(Clone, Debug, Default)]
struct TransientScopeState {
    relations: HashMap<RelationId, RelationState>,
}

impl TransientScopeState {
    fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    fn len(&self) -> usize {
        self.relations
            .values()
            .map(RelationState::cardinality)
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
        let inserted = !relation.contains_tuple(&tuple);
        relation.insert(tuple);
        Ok(inserted)
    }

    fn retract(&mut self, relation: RelationId, tuple: &Tuple) -> bool {
        let Some(relation_state) = self.relations.get_mut(&relation) else {
            return false;
        };
        let removed = relation_state.contains_tuple(tuple);
        relation_state.remove(tuple);
        if relation_state.is_empty() {
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

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        let Some((transient_rows, transient_domains)) =
            self.transient.relation_summary(self.scopes, relation)
        else {
            return self.base.relation_capabilities(relation);
        };
        let base = match self.base.relation_capabilities(relation) {
            Ok(capabilities) => Some(capabilities),
            Err(KernelError::UnknownRelation(unknown)) if unknown == relation => None,
            Err(error) => return Err(error),
        };
        Ok(composed_capabilities(
            base.as_ref(),
            transient_rows,
            transient_domains,
        ))
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
        if !self.relation_depends_on_visible_transient(relation) {
            return self.tx.scan_relation(relation, bindings);
        }

        let mut visible = scan_composed_transaction_extensional(
            self.tx,
            self.transient,
            self.scopes,
            relation,
            bindings,
        )?;

        if relation_has_active_rule_head(self.tx.base.rules(), relation) {
            let reader = ComposedExtensionalTransactionRead {
                tx: self.tx,
                transient: self.transient,
                scopes: self.scopes,
            };
            let derived = RuleSet::new(active_rules(self.tx.base.rules()))
                .evaluate_fixpoint(&reader, &crate::ExecutionContext::serial())
                .map_err(KernelError::from)
                .and_then(|derived| build_derived_relations(&self.tx.base.relations, derived))?;
            if let Some(rows) = derived.get(&relation) {
                visible.extend(rows.scan(bindings)?);
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
        if !self.relation_depends_on_visible_transient(relation) {
            return self.tx.visit_relation(relation, bindings, visitor);
        }
        for tuple in self.scan_relation(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        if !self.relation_depends_on_visible_transient(relation) {
            return self.tx.relation_capabilities(relation);
        }
        let summary = self.transient.relation_summary(self.scopes, relation);
        let base = self.tx.relation_capabilities(relation).ok();
        let (transient_rows, transient_domains) = summary.unwrap_or_else(|| {
            let arity = base
                .as_ref()
                .map(|capabilities| capabilities.value_domains.len())
                .unwrap_or(0);
            (0, vec![ValueDomain::Unknown; arity])
        });
        Ok(composed_capabilities(
            base.as_ref(),
            transient_rows,
            transient_domains,
        ))
    }
}

impl DispatchRead for ComposedTransactionRead<'_, '_> {
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

    fn cached_method_program(
        &self,
        relation: RelationId,
        method: &Value,
    ) -> Result<Option<Option<Value>>, KernelError> {
        if self.method_program_cache_is_transient(relation) {
            return Ok(None);
        }
        self.tx.cached_method_program(relation, method).map(Some)
    }

    fn cached_applicable_positional_methods(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        args: &[Value],
    ) -> Result<Option<Arc<[Value]>>, KernelError> {
        if self.dispatch_cache_is_transient(relations) {
            return Ok(None);
        }
        self.tx
            .cached_applicable_positional_methods(relations, selector, args)
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

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        let summary = self.transient.relation_summary(self.scopes, relation);
        let base = self.tx.relation_capabilities(relation).ok();
        let Some((transient_rows, transient_domains)) = summary else {
            return base.ok_or(KernelError::UnknownRelation(relation));
        };
        Ok(composed_capabilities(
            base.as_ref(),
            transient_rows,
            transient_domains,
        ))
    }
}

fn composed_capabilities(
    base: Option<&RelationCapabilities>,
    transient_rows: usize,
    transient_domains: Vec<ValueDomain>,
) -> RelationCapabilities {
    let base_rows = base.and_then(|base| base.cardinality).unwrap_or(0);
    let value_domains = base.map_or(transient_domains.clone(), |base| {
        combine_transient_domains(
            &base.value_domains,
            base_rows,
            &transient_domains,
            transient_rows,
        )
    });
    RelationCapabilities {
        source: RelationSource::Transient,
        cardinality: Some(base_rows.saturating_add(transient_rows)),
        exact_indexes: Vec::new(),
        value_domains,
        supports_streaming: true,
        supports_batch_export: false,
    }
}

fn combine_transient_domains(
    left: &[ValueDomain],
    left_rows: usize,
    right: &[ValueDomain],
    right_rows: usize,
) -> Vec<ValueDomain> {
    if left_rows == 0 {
        return right.to_vec();
    }
    if right_rows == 0 {
        return left.to_vec();
    }
    left.iter()
        .zip(right)
        .map(|(left, right)| match (*left, *right) {
            (ValueDomain::Immediate, ValueDomain::Immediate) => ValueDomain::Immediate,
            (ValueDomain::Heap, ValueDomain::Heap) => ValueDomain::Heap,
            (ValueDomain::Unknown, _) | (_, ValueDomain::Unknown) => ValueDomain::Unknown,
            _ => ValueDomain::Mixed,
        })
        .collect()
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
    fn relation_depends_on_visible_transient(&self, relation: RelationId) -> bool {
        let mut seen = BTreeSet::new();
        relation_depends_on_visible_transient(
            self.tx,
            self.transient,
            self.scopes,
            relation,
            &mut seen,
        )
    }

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

    fn method_program_cache_is_transient(&self, relation: RelationId) -> bool {
        self.transient.relation_visible(self.scopes, relation)
            || self
                .tx
                .base
                .rules()
                .iter()
                .any(|rule| rule.active() && rule.rule().head_relation() == relation)
                && self
                    .scopes
                    .iter()
                    .any(|scope| self.transient.scope_len(*scope) > 0)
    }
}

fn relation_depends_on_visible_transient(
    tx: &Transaction<'_>,
    transient: &TransientStore,
    scopes: &[Identity],
    relation: RelationId,
    seen: &mut BTreeSet<RelationId>,
) -> bool {
    if transient.relation_visible(scopes, relation) {
        return true;
    }
    if !seen.insert(relation) {
        return false;
    }
    tx.base.rules().iter().any(|rule| {
        rule.active()
            && rule.rule().head_relation() == relation
            && rule.rule().body_atoms().any(|atom| {
                relation_depends_on_visible_transient(tx, transient, scopes, atom.relation(), seen)
            })
    })
}
