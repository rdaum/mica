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
    CatalogChange, Commit, ConflictPolicy, FactChange, FactChangeKind, FactId, KernelError,
    RelationId, RelationMetadata, RelationRead, RelationWorkspace, Rule, RuleDefinition, RuleSet,
    Tuple, Version,
};
use mica_var::Value;
use std::cell::RefCell;
use std::collections::BTreeMap;

type ProjectedDerivedCache = RefCell<Option<Result<BTreeMap<RelationId, Vec<Tuple>>, KernelError>>>;

#[derive(Clone, Debug, Default)]
pub struct ProjectedStore {
    version: Version,
    relations: BTreeMap<RelationId, RelationState>,
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
        self.relations.values().map(RelationState::metadata)
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
        self.relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .scan(bindings)
    }

    fn assert_visible(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.relations
            .get_mut(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .insert(tuple);
        self.advance_version();
        Ok(())
    }

    fn retract_visible(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError> {
        self.validate_tuple(relation, &tuple)?;
        self.relations
            .get_mut(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .remove(&tuple);
        self.advance_version();
        Ok(())
    }

    fn visible_tuple_for_key(
        &self,
        relation: RelationId,
        positions: &[u16],
        tuple: &Tuple,
    ) -> Result<Option<Tuple>, KernelError> {
        Ok(self
            .relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .tuple_for_key(positions, tuple))
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
        self.validate_tuple(change.relation, &change.tuple)?;
        let relation = self
            .relations
            .get_mut(&change.relation)
            .ok_or(KernelError::UnknownRelation(change.relation))?;
        match change.kind {
            FactChangeKind::Assert => relation.insert(change.tuple),
            FactChangeKind::Retract => relation.remove(&change.tuple),
        }
        Ok(())
    }

    fn validate_tuple(&self, relation: RelationId, tuple: &Tuple) -> Result<(), KernelError> {
        let metadata = self
            .relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .metadata();
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

    fn derived_tuples(&self) -> Result<BTreeMap<RelationId, Vec<Tuple>>, KernelError> {
        if self.derived_cache.borrow().is_none() {
            let derived = RuleSet::new(active_rules(&self.rules))
                .evaluate_fixpoint(&ExtensionalProjectedReader { store: self })
                .map_err(KernelError::from);
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
        if let Some(rows) = self.derived_tuples()?.get(&relation) {
            visible.extend(
                rows.iter()
                    .filter(|tuple| tuple.matches_bindings(bindings))
                    .cloned(),
            );
            visible.sort();
            visible.dedup();
        }
        Ok(visible)
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
        self.validate_tuple(relation, &tuple)?;
        let key_positions = match self
            .relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))?
            .metadata()
            .conflict_policy()
        {
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
}

pub(crate) fn validate_rule_definition_against_relations(
    relations: &BTreeMap<RelationId, RelationState>,
    definition: &RuleDefinition,
) -> Result<(), KernelError> {
    validate_rule_against_relations(relations, definition.rule())
}

pub(crate) fn validate_rule_against_relations(
    relations: &BTreeMap<RelationId, RelationState>,
    rule: &Rule,
) -> Result<(), KernelError> {
    validate_rule_atom(relations, rule.head_relation(), rule.head_terms())?;
    for atom in rule.body() {
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
    relations: &BTreeMap<RelationId, RelationState>,
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
