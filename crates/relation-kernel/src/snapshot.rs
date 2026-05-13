use crate::commit_bloom::CommitBloom;
use crate::index::RelationState;
use crate::{
    KernelError, RelationId, RelationMetadata, Rule, RuleEvalError, RuleSet, Tuple, Version,
};
use mica_var::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogChange {
    RelationCreated(RelationMetadata),
    RuleInstalled(Rule),
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
    pub(crate) rules: Vec<Rule>,
    pub(crate) commits: Arc<[Commit]>,
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

        let derived = RuleSet::new(self.rules.clone())
            .evaluate_fixpoint(&ExtensionalSnapshotReader { snapshot: self })
            .map_err(KernelError::from)?;
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

    pub fn contains(&self, relation: RelationId, tuple: &Tuple) -> Result<bool, KernelError> {
        let bindings = tuple.values().iter().cloned().map(Some).collect::<Vec<_>>();
        Ok(!self.scan(relation, &bindings)?.is_empty())
    }

    pub fn commits_since(&self, version: Version) -> &[Commit] {
        let first = self
            .commits
            .iter()
            .position(|commit| commit.version() > version)
            .unwrap_or(self.commits.len());
        &self.commits[first..]
    }

    pub fn relation_metadata(&self) -> impl Iterator<Item = &RelationMetadata> {
        self.relations.values().map(|relation| relation.metadata())
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    pub(crate) fn scan_extensional(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.relation(relation)?.scan(bindings)
    }

    pub(crate) fn relation(&self, relation: RelationId) -> Result<&RelationState, KernelError> {
        self.relations
            .get(&relation)
            .ok_or(KernelError::UnknownRelation(relation))
    }

    pub(crate) fn bloom_since(&self, version: Version) -> CommitBloom {
        let mut bloom = CommitBloom::new();
        for commit in self.commits_since(version) {
            bloom.merge(&commit.bloom);
        }
        bloom
    }
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
}
