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
use crate::computed::{ComputedRelationRead, ComputedRelationRegistry};
use crate::dispatch_cache::DispatchCache;
use crate::index::RelationState;
use crate::method_program_cache::MethodProgramCache;
use crate::tuple::union_ordered_tuple_rows;
use crate::{
    ApplicableMethodCall, DispatchRead, DispatchRelations, KernelError, RelationId,
    RelationMetadata, RelationRead, RuleDefinition, RuleEvalError, RuleSet, ScanControl, Tuple,
    Version,
};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

pub(crate) type DerivedCache =
    Arc<OnceLock<Result<BTreeMap<RelationId, RelationState>, KernelError>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Commit {
    pub(crate) version: Version,
    pub(crate) catalog_changes: Arc<[CatalogChange]>,
    pub(crate) changes: Arc<[FactChange]>,
    pub(crate) bloom: CommitBloom,
}

impl Commit {
    pub fn version(&self) -> Version {
        self.version
    }

    pub fn catalog_changes(&self) -> &[CatalogChange] {
        &self.catalog_changes
    }

    pub fn changes(&self) -> &[FactChange] {
        &self.changes
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CommitHistory {
    head: Option<Arc<CommitHistoryNode>>,
    len: usize,
}

#[derive(Debug)]
struct CommitHistoryNode {
    commit: Commit,
    previous: Option<Arc<CommitHistoryNode>>,
}

impl CommitHistory {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn from_commits(commits: impl IntoIterator<Item = Commit>) -> Self {
        let mut history = Self::empty();
        for commit in commits {
            history = history.append(commit);
        }
        history
    }

    pub(crate) fn append(&self, commit: Commit) -> Self {
        Self {
            head: Some(Arc::new(CommitHistoryNode {
                commit,
                previous: self.head.clone(),
            })),
            len: self.len + 1,
        }
    }

    pub(crate) fn since(&self, version: Version) -> Vec<Commit> {
        let mut commits = Vec::new();
        let mut current = self.head.as_ref();
        while let Some(node) = current {
            if node.commit.version() <= version {
                break;
            }
            commits.push(node.commit.clone());
            current = node.previous.as_ref();
        }
        commits.reverse();
        commits
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogChange {
    RelationCreated(RelationMetadata),
    RuleInstalled(RuleDefinition),
    RuleDisabled(Identity),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FactChange {
    pub relation: RelationId,
    pub tuple: Tuple,
    pub kind: FactChangeKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FactChangeKind {
    Assert,
    Retract,
}

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub(crate) snapshot: Arc<Snapshot>,
    pub(crate) commit: Commit,
}

impl CommitResult {
    pub fn snapshot(&self) -> &Arc<Snapshot> {
        &self.snapshot
    }

    pub fn commit(&self) -> &Commit {
        &self.commit
    }

    pub fn into_snapshot(self) -> Arc<Snapshot> {
        self.snapshot
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub(crate) version: Version,
    pub(crate) relations: HashMap<RelationId, RelationState>,
    pub(crate) rules: Vec<RuleDefinition>,
    pub(crate) computed_relations: Arc<ComputedRelationRegistry>,
    pub(crate) derived_cache: DerivedCache,
    pub(crate) dispatch_cache: DispatchCache,
    pub(crate) method_program_cache: MethodProgramCache,
    pub(crate) commits: CommitHistory,
}

impl Snapshot {
    pub fn version(&self) -> Version {
        self.version
    }

    pub fn scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut visible = self.scan_extensional(relation, bindings)?;
        if !relation_has_active_rule_head(&self.rules, relation) {
            return Ok(visible);
        }

        let derived = self.derived_relations()?;
        if let Some(rows) = derived.get(&relation) {
            visible = union_ordered_tuple_rows(visible, rows.scan(bindings)?);
        }
        Ok(visible)
    }

    pub fn visit(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if !relation_has_active_rule_head(&self.rules, relation) {
            return self.visit_extensional(relation, bindings, visitor);
        }

        for tuple in self.scan(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }

    pub fn contains(&self, relation: RelationId, tuple: &Tuple) -> Result<bool, KernelError> {
        let bindings = tuple.values().iter().cloned().map(Some).collect::<Vec<_>>();
        Ok(!self.scan(relation, &bindings)?.is_empty())
    }

    pub fn commits_since(&self, version: Version) -> Vec<Commit> {
        self.commits.since(version)
    }

    pub fn relation_metadata(&self) -> impl Iterator<Item = &RelationMetadata> {
        let mut metadata = self
            .relations
            .values()
            .map(|relation| relation.metadata())
            .collect::<Vec<_>>();
        metadata.sort_by_key(|metadata| metadata.id());
        metadata.into_iter()
    }

    pub fn extensional_facts(&self) -> Result<Vec<(RelationId, Tuple)>, KernelError> {
        let mut facts = Vec::new();
        for (relation_id, relation) in &self.relations {
            let bindings = vec![None; relation.metadata().arity() as usize];
            facts.extend(
                relation
                    .scan(&bindings)?
                    .into_iter()
                    .map(|tuple| (*relation_id, tuple)),
            );
        }
        facts.sort();
        Ok(facts)
    }

    pub fn rules(&self) -> &[RuleDefinition] {
        &self.rules
    }

    pub(crate) fn scan_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        let relation = self.relation(relation)?;
        if let Some(rows) = self
            .computed_relations
            .scan(self, relation.metadata(), bindings)
        {
            return rows;
        }
        relation.scan(bindings)
    }

    pub(crate) fn join_extensional_relation_scans(
        &self,
        left_relation: RelationId,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right_relation: RelationId,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        let left = self.relation(left_relation)?;
        let right = self.relation(right_relation)?;
        if self
            .computed_relations
            .is_computed_relation(left.metadata())
            || self
                .computed_relations
                .is_computed_relation(right.metadata())
        {
            return Ok(None);
        }
        left.join_eq(
            left_bindings,
            left_positions,
            right,
            right_bindings,
            right_positions,
        )
    }

    pub(crate) fn relation_has_exact_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        let relation = self.relation(relation)?;
        if self
            .computed_relations
            .is_computed_relation(relation.metadata())
        {
            return Ok(false);
        }
        Ok(relation.has_exact_index(positions))
    }

    pub(crate) fn estimate_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        let mut estimate = self.relation(relation)?.estimate_scan_count(bindings)?;
        if relation_has_active_rule_head(&self.rules, relation)
            && let Some(rows) = self.derived_relations()?.get(&relation)
        {
            estimate += rows.estimate_scan_count(bindings)?;
        }
        Ok(estimate)
    }

    pub(crate) fn estimate_extensional_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        let relation = self.relation(relation)?;
        if let Some(estimate) =
            self.computed_relations
                .estimate(self, relation.metadata(), bindings)
        {
            return estimate;
        }
        relation.estimate_scan_count(bindings)
    }

    pub(crate) fn visit_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        let relation = self.relation(relation)?;
        if let Some(rows) = self
            .computed_relations
            .scan(self, relation.metadata(), bindings)
        {
            for tuple in rows? {
                if visitor(&tuple)? == ScanControl::Stop {
                    break;
                }
            }
            return Ok(());
        }
        relation.visit(bindings, visitor)
    }

    pub(crate) fn relation(&self, relation: RelationId) -> Result<&RelationState, KernelError> {
        self.relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))
    }

    fn derived_relations(&self) -> Result<&BTreeMap<RelationId, RelationState>, KernelError> {
        self.derived_cache
            .get_or_init(|| {
                let start = std::time::Instant::now();
                let derived = RuleSet::new(active_rules(&self.rules))
                    .evaluate_fixpoint(&ExtensionalSnapshotReader { snapshot: self })
                    .map_err(KernelError::from)?;
                let derived = build_derived_relations(&self.relations, derived)?;
                crate::metrics::record_derived_materialization(
                    start.elapsed(),
                    derived.iter().map(|(relation, state)| {
                        let name = self
                            .relation(*relation)
                            .ok()
                            .and_then(|relation| relation.metadata().name().name())
                            .unwrap_or("<unknown>")
                            .to_owned();
                        (*relation, name, state.cardinality())
                    }),
                );
                Ok(derived)
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    pub(crate) fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Vec<ApplicableMethodCall>, KernelError> {
        if let Some(methods) = self.dispatch_cache.get(relations, selector, roles) {
            return Ok(methods);
        }

        let methods =
            crate::dispatch::applicable_method_calls_uncached(self, relations, selector, roles)?;
        self.dispatch_cache
            .insert(relations, selector, roles, methods.clone());
        Ok(methods)
    }

    pub(crate) fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Vec<ApplicableMethodCall>, KernelError> {
        if let Some(methods) = self
            .dispatch_cache
            .get_normalized(relations, selector, roles)
        {
            return Ok(methods);
        }

        let methods =
            crate::dispatch::applicable_method_calls_uncached(self, relations, selector, roles)?;
        self.dispatch_cache
            .insert_normalized(relations, selector, roles, methods.clone());
        Ok(methods)
    }

    pub(crate) fn cached_applicable_positional_methods(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        args: &[Value],
    ) -> Result<Vec<Value>, KernelError> {
        if let Some(methods) = self
            .dispatch_cache
            .get_positional(relations, selector, args)
        {
            return Ok(methods);
        }

        let methods = crate::dispatch::applicable_positional_methods(
            self,
            relations,
            selector.clone(),
            args,
        )?;
        self.dispatch_cache
            .insert_positional(relations, selector, args, methods.clone());
        Ok(methods)
    }

    pub(crate) fn cached_method_program(
        &self,
        relation: RelationId,
        method: &Value,
    ) -> Result<Option<Value>, KernelError> {
        if let Some(program) = self.method_program_cache.get(relation, method) {
            return Ok(program);
        }

        let program = crate::dispatch::method_program_id_uncached(self, relation, method)?;
        self.method_program_cache
            .insert(relation, method, program.clone());
        Ok(program)
    }
}

pub(crate) fn build_derived_relations(
    relations: &HashMap<RelationId, RelationState>,
    derived: BTreeMap<RelationId, Vec<Tuple>>,
) -> Result<BTreeMap<RelationId, RelationState>, KernelError> {
    derived
        .into_iter()
        .map(|(relation_id, rows)| {
            let metadata = relations
                .get(&relation_id)
                .ok_or(KernelError::UnknownRelation(relation_id))?
                .metadata()
                .clone();
            let mut state = RelationState::empty(metadata)?;
            state.apply_ordered_asserts_to_empty(rows.iter(), |_, _| {});
            Ok((relation_id, state))
        })
        .collect()
}

pub(crate) fn empty_derived_cache() -> DerivedCache {
    Arc::new(OnceLock::new())
}

pub(crate) fn empty_dispatch_cache() -> DispatchCache {
    DispatchCache::new()
}

pub(crate) fn empty_method_program_cache() -> MethodProgramCache {
    MethodProgramCache::new()
}

pub(crate) fn active_rules(rules: &[RuleDefinition]) -> Vec<crate::Rule> {
    rules
        .iter()
        .filter(|rule| rule.active())
        .map(|rule| rule.rule().clone())
        .collect()
}

pub(crate) fn relation_has_active_rule_head(
    rules: &[RuleDefinition],
    relation: RelationId,
) -> bool {
    rules
        .iter()
        .any(|rule| rule.active() && rule.rule().head_relation() == relation)
}

impl From<RuleEvalError> for KernelError {
    fn from(value: RuleEvalError) -> Self {
        match value {
            RuleEvalError::Kernel(error) => error,
            RuleEvalError::Rule(error) => Self::Rule(error),
        }
    }
}

impl DispatchRead for Snapshot {
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

impl RelationRead for Snapshot {
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

    fn has_exact_relation_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        self.relation_has_exact_index(relation, positions)
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
        if !relation_has_active_rule_head(&self.rules, left_relation)
            && !relation_has_active_rule_head(&self.rules, right_relation)
            && let Some(rows) = self.join_extensional_relation_scans(
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

impl ComputedRelationRead for Snapshot {
    fn version(&self) -> Version {
        Snapshot::version(self)
    }

    fn relation_metadata_vec(&self) -> Vec<RelationMetadata> {
        self.relation_metadata().cloned().collect()
    }

    fn relation_id(&self, name: Symbol, arity: u16) -> Option<RelationId> {
        self.relations
            .values()
            .map(|relation| relation.metadata())
            .find(|metadata| metadata.name() == name && metadata.arity() == arity)
            .map(|metadata| metadata.id())
    }

    fn rules_vec(&self) -> Vec<RuleDefinition> {
        Snapshot::rules(self).to_vec()
    }

    fn extensional_facts(&self) -> Result<Vec<(RelationId, Tuple)>, KernelError> {
        Snapshot::extensional_facts(self)
    }
}

struct ExtensionalSnapshotReader<'a> {
    snapshot: &'a Snapshot,
}

impl crate::RelationRead for ExtensionalSnapshotReader<'_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.snapshot.scan_extensional(relation, bindings)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.snapshot
            .estimate_extensional_scan(relation, bindings)
            .map(Some)
    }

    fn has_exact_relation_index(
        &self,
        relation: RelationId,
        positions: &[u16],
    ) -> Result<bool, KernelError> {
        self.snapshot.relation_has_exact_index(relation, positions)
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
        self.snapshot.join_extensional_relation_scans(
            left_relation,
            left_bindings,
            left_positions,
            right_relation,
            right_bindings,
            right_positions,
        )
    }
}
