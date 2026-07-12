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
use crate::snapshot::{active_rules, build_derived_relations};
use crate::tuple::union_ordered_tuple_rows;
use crate::{
    CatalogChange, Commit, ConflictPolicy, FactChange, FactChangeKind, FactId, KernelError,
    RelationCapabilities, RelationId, RelationMetadata, RelationRead, RelationSource,
    RelationWorkspace, Rule, RuleDefinition, RuleSet, Tuple, ValueDomain, Version,
};
use mica_var::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

type ProjectedDerivedCache =
    RefCell<Option<Result<BTreeMap<RelationId, RelationState>, KernelError>>>;

#[derive(Clone, Debug, Default)]
pub struct ProjectedStore {
    version: Version,
    relations: HashMap<RelationId, RelationState>,
    rules: Vec<RuleDefinition>,
    derived_cache: ProjectedDerivedCache,
}

impl ProjectedStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn relation_metadata(&self) -> impl Iterator<Item = &RelationMetadata> {
        let mut metadata = self
            .relations
            .values()
            .map(RelationState::metadata)
            .collect::<Vec<_>>();
        metadata.sort_by_key(|metadata| metadata.id());
        metadata.into_iter()
    }

    pub fn rules(&self) -> &[RuleDefinition] {
        &self.rules
    }

    pub fn create_relation(&mut self, metadata: RelationMetadata) -> Result<(), KernelError> {
        if self.relations.contains_key(&metadata.id()) {
            return Err(KernelError::RelationAlreadyExists(metadata.id()));
        }
        self.relations
            .insert(metadata.id(), RelationState::empty(metadata)?);
        self.advance_version();
        Ok(())
    }

    pub fn install_rule(
        &mut self,
        rule: Rule,
        source: impl Into<String>,
    ) -> Result<RuleDefinition, KernelError> {
        validate_rule_against_relations(&self.relations, &rule)?;
        let definition = RuleDefinition::new(next_rule_id(&self.rules), rule, source.into());
        let mut rules = self.rules.clone();
        rules.push(definition.clone());
        RuleSet::new(active_rules(&rules))
            .validate_stratified()
            .map_err(KernelError::Rule)?;
        self.rules = rules;
        self.advance_version();
        Ok(definition)
    }

    pub fn disable_rule(&mut self, rule_id: FactId) -> Result<(), KernelError> {
        disable_rule_in(&mut self.rules, rule_id)?;
        RuleSet::new(active_rules(&self.rules))
            .validate_stratified()
            .map_err(KernelError::Rule)?;
        self.advance_version();
        Ok(())
    }

    pub fn apply_delta(&mut self, delta: ProjectedDelta) -> Result<(), KernelError> {
        for change in delta.catalog_changes {
            self.apply_catalog_change(change)?;
        }
        for change in delta.changes {
            self.apply_fact_change(change)?;
        }
        self.version = self.version.max(delta.version);
        self.invalidate_derived();
        Ok(())
    }

    pub fn apply_commit(&mut self, commit: &Commit) -> Result<(), KernelError> {
        self.apply_delta(ProjectedDelta {
            version: commit.version(),
            catalog_changes: commit.catalog_changes().to_vec(),
            changes: commit.changes().to_vec(),
        })
    }

    fn scan_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.relation(relation)?.scan(bindings)
    }

    fn assert_visible(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.apply_visible_change(relation, tuple, FactChangeKind::Assert)
    }

    fn retract_visible(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.apply_visible_change(relation, tuple, FactChangeKind::Retract)
    }

    fn visible_tuple_for_key(
        &self,
        relation: RelationId,
        positions: &[u16],
        tuple: &Tuple,
    ) -> Result<Option<Tuple>, KernelError> {
        Ok(self.relation(relation)?.tuple_for_key(positions, tuple))
    }

    fn apply_catalog_change(&mut self, change: CatalogChange) -> Result<(), KernelError> {
        match change {
            CatalogChange::RelationCreated(metadata) => {
                if self.relations.contains_key(&metadata.id()) {
                    return Ok(());
                }
                self.relations
                    .insert(metadata.id(), RelationState::empty(metadata)?);
            }
            CatalogChange::RuleInstalled(rule) => {
                validate_rule_definition_against_relations(&self.relations, &rule)?;
                if self.rules.iter().any(|existing| existing.id() == rule.id()) {
                    return Ok(());
                }
                let mut rules = self.rules.clone();
                rules.push(rule);
                RuleSet::new(active_rules(&rules))
                    .validate_stratified()
                    .map_err(KernelError::Rule)?;
                self.rules = rules;
            }
            CatalogChange::RuleDisabled(rule_id) => {
                disable_rule_in(&mut self.rules, rule_id)?;
            }
        }
        Ok(())
    }

    fn apply_fact_change(&mut self, change: FactChange) -> Result<(), KernelError> {
        let relation = self.relation_mut(change.relation)?;
        relation.validate_tuple(&change.tuple)?;
        let _ = match change.kind {
            FactChangeKind::Assert => relation.insert(change.tuple),
            FactChangeKind::Retract => relation.remove(&change.tuple),
        };
        Ok(())
    }

    fn apply_visible_change(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
        kind: FactChangeKind,
    ) -> Result<(), KernelError> {
        let relation = self.relation_mut(relation)?;
        relation.validate_tuple(&tuple)?;
        match kind {
            FactChangeKind::Assert => relation.insert(tuple),
            FactChangeKind::Retract => relation.remove(&tuple),
        };
        self.advance_version();
        Ok(())
    }

    fn relation(&self, relation: RelationId) -> Result<&RelationState, KernelError> {
        self.relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))
    }

    fn relation_mut(&mut self, relation: RelationId) -> Result<&mut RelationState, KernelError> {
        self.relations
            .get_mut(&relation)
            .ok_or(KernelError::UnknownRelation(relation))
    }

    fn derived_relations(&self) -> Result<BTreeMap<RelationId, RelationState>, KernelError> {
        if self.derived_cache.borrow().is_none() {
            let derived = RuleSet::new(active_rules(&self.rules))
                .evaluate_fixpoint(
                    &ExtensionalProjectedReader { store: self },
                    &crate::ExecutionContext::serial(),
                )
                .map_err(KernelError::from)
                .and_then(|derived| build_derived_relations(&self.relations, derived));
            *self.derived_cache.borrow_mut() = Some(derived);
        }
        self.derived_cache.borrow().as_ref().unwrap().clone()
    }

    fn advance_version(&mut self) {
        self.version += 1;
        self.invalidate_derived();
    }

    fn invalidate_derived(&self) {
        *self.derived_cache.borrow_mut() = None;
    }

    fn extensional_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        let relation = self.relation(relation)?;
        Ok(RelationCapabilities {
            source: RelationSource::Projected,
            cardinality: Some(relation.cardinality()),
            exact_indexes: relation
                .metadata()
                .indexes()
                .iter()
                .map(|index| index.positions().to_vec())
                .collect(),
            value_domains: relation.value_domains(),
            supports_streaming: true,
            supports_batch_export: false,
        })
    }
}

impl RelationRead for ProjectedStore {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut visible = self.scan_extensional(relation, bindings)?;
        if self.rules.is_empty() {
            return Ok(visible);
        }
        if let Some(rows) = self.derived_relations()?.get(&relation) {
            visible = union_ordered_tuple_rows(visible, rows.scan(bindings)?);
        }
        Ok(visible)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        let mut rows = self.relation(relation)?.estimate_scan_count(bindings)?;
        if let Some(derived) = self.derived_relations()?.get(&relation) {
            rows = rows.saturating_add(derived.estimate_scan_count(bindings)?);
        }
        Ok(Some(rows))
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        let mut capabilities = self.extensional_capabilities(relation)?;
        if let Some(derived) = self.derived_relations()?.get(&relation) {
            capabilities.cardinality = capabilities
                .cardinality
                .map(|rows| rows.saturating_add(derived.cardinality()));
            capabilities.value_domains =
                vec![ValueDomain::Unknown; self.relation(relation)?.metadata().arity() as usize];
        }
        Ok(capabilities)
    }

    fn has_exact_relation_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        Ok(self.relation(relation)?.has_exact_index(positions))
    }
}

impl RelationWorkspace for ProjectedStore {
    fn assert_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.assert_visible(relation, tuple)
    }

    fn retract_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.retract_visible(relation, tuple)
    }

    fn replace_functional_tuple(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError> {
        self.relation(relation)?.validate_tuple(&tuple)?;
        let key_positions = match self.relation(relation)?.metadata().conflict_policy() {
            ConflictPolicy::Functional { key_positions } => key_positions.to_vec(),
            ConflictPolicy::Set | ConflictPolicy::EventAppend => {
                return self.assert_visible(relation, tuple);
            }
        };
        if let Some(old_tuple) = self.visible_tuple_for_key(relation, &key_positions, &tuple)? {
            self.retract_visible(relation, old_tuple)?;
        }
        self.assert_visible(relation, tuple)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectedDelta {
    pub version: Version,
    pub catalog_changes: Vec<CatalogChange>,
    pub changes: Vec<FactChange>,
}

struct ExtensionalProjectedReader<'a> {
    store: &'a ProjectedStore,
}

impl RelationRead for ExtensionalProjectedReader<'_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.store.scan_extensional(relation, bindings)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.store
            .relation(relation)?
            .estimate_scan_count(bindings)
            .map(Some)
    }

    fn relation_capabilities(
        &self,
        relation: RelationId,
    ) -> Result<RelationCapabilities, KernelError> {
        self.store.extensional_capabilities(relation)
    }
}

pub(crate) fn validate_rule_definition_against_relations(
    relations: &HashMap<RelationId, RelationState>,
    definition: &RuleDefinition,
) -> Result<(), KernelError> {
    validate_rule_against_relations(relations, definition.rule())
}

pub(crate) fn validate_rule_against_relations(
    relations: &HashMap<RelationId, RelationState>,
    rule: &Rule,
) -> Result<(), KernelError> {
    validate_rule_atom(relations, rule.head_relation(), rule.head_terms())?;
    for atom in rule.body_atoms() {
        validate_rule_atom(relations, atom.relation(), atom.terms())?;
    }
    Ok(())
}

pub(crate) fn next_rule_id(rules: &[RuleDefinition]) -> FactId {
    let mut raw = crate::kernel::GENERATED_RULE_ID_START + rules.len() as u64;
    loop {
        let id = FactId::new(raw & FactId::MAX).unwrap();
        if !rules.iter().any(|rule| rule.id() == id) {
            return id;
        }
        raw = raw.wrapping_add(1);
    }
}

pub(crate) fn disable_rule_in(
    rules: &mut [RuleDefinition],
    rule_id: FactId,
) -> Result<(), KernelError> {
    let Some(rule) = rules.iter_mut().find(|rule| rule.id() == rule_id) else {
        return Err(KernelError::UnknownRule(rule_id));
    };
    rule.deactivate();
    Ok(())
}

fn validate_rule_atom(
    relations: &HashMap<RelationId, RelationState>,
    relation: RelationId,
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
