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
use crate::dispatch_cache::DispatchCache;
use crate::index::RelationState;
use crate::method_program_cache::MethodProgramCache;
use crate::{
    ApplicableMethodCall, DispatchRelations, KernelError, RelationId, RelationMetadata,
    RuleDefinition, RuleEvalError, RuleSet, ScanControl, Tuple, Version,
};
use mica_var::{Identity, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

pub(crate) type DerivedCache = Arc<OnceLock<Result<BTreeMap<RelationId, Vec<Tuple>>, KernelError>>>;

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
    pub(crate) relations: BTreeMap<RelationId, RelationState>,
    pub(crate) rules: Vec<RuleDefinition>,
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
        if self.rules.is_empty() {
            return Ok(visible);
        }

        let derived = self.derived_tuples()?;
        if let Some(rows) = derived.get(&relation) {
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

    pub fn visit(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if self.rules.is_empty() {
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
        self.relations.values().map(|relation| relation.metadata())
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
        self.relation(relation)?.scan(bindings)
    }

    pub(crate) fn estimate_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        let mut estimate = self.relation(relation)?.estimate_scan_count(bindings)?;
        if !self.rules.is_empty()
            && let Some(rows) = self.derived_tuples()?.get(&relation)
        {
            estimate += rows
                .iter()
                .filter(|tuple| tuple.matches_bindings(bindings))
                .count();
        }
        Ok(estimate)
    }

    pub(crate) fn estimate_extensional_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        self.relation(relation)?.estimate_scan_count(bindings)
    }

    pub(crate) fn visit_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.relation(relation)?.visit(bindings, visitor)
    }

    pub(crate) fn relation(&self, relation: RelationId) -> Result<&RelationState, KernelError> {
        self.relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))
    }

    fn derived_tuples(&self) -> Result<&BTreeMap<RelationId, Vec<Tuple>>, KernelError> {
        self.derived_cache
            .get_or_init(|| {
                RuleSet::new(active_rules(&self.rules))
                    .evaluate_fixpoint(&ExtensionalSnapshotReader { snapshot: self })
                    .map_err(KernelError::from)
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

    pub(crate) fn cached_method_program(
        &self,
        relation: RelationId,
        method: &Value,
    ) -> Result<Option<Value>, KernelError> {
        if let Some(program) = self.method_program_cache.get(relation, method) {
            return Ok(program);
        }

        let program = crate::query::method_program_id_uncached(self, relation, method)?;
        self.method_program_cache
            .insert(relation, method, program.clone());
        Ok(program)
    }
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

impl From<RuleEvalError> for KernelError {
    fn from(value: RuleEvalError) -> Self {
        match value {
            RuleEvalError::Kernel(error) => error,
            RuleEvalError::Rule(error) => Self::Rule(error),
        }
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
}
