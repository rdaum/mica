use crate::index::RelationState;
use crate::snapshot::{active_rules, empty_derived_cache};
use crate::{
    CatalogChange, Commit, CommitProvider, FactChangeKind, KernelError, RelationMetadata, Rule,
    RuleDefinition, RuleSet, Snapshot, Transaction,
};
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::sync::Arc;

const GENERATED_RULE_ID_START: u64 = 0x00d0_0000_0000_0000;

pub struct RelationKernel {
    root: ArcSwap<Snapshot>,
    provider: Arc<dyn CommitProvider>,
}

impl RelationKernel {
    pub fn new() -> Self {
        Self::with_provider(Arc::new(crate::InMemoryCommitProvider::new()))
    }

    pub fn with_provider(provider: Arc<dyn CommitProvider>) -> Self {
        Self {
            root: ArcSwap::new(Arc::new(Snapshot {
                version: 0,
                relations: BTreeMap::new(),
                rules: Vec::new(),
                derived_cache: empty_derived_cache(),
                commits: Arc::from([]),
            })),
            provider,
        }
    }

    pub fn load_from_commits(
        relations: impl IntoIterator<Item = RelationMetadata>,
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
    ) -> Result<Self, KernelError> {
        let mut states = BTreeMap::new();
        for metadata in relations {
            states.insert(metadata.id(), RelationState::empty(metadata)?);
        }

        let commits = commits.into_iter().collect::<Vec<_>>();
        let mut rules = Vec::new();
        for commit in &commits {
            for change in commit.catalog_changes() {
                if let CatalogChange::RuleInstalled(rule) = change {
                    validate_rule_definition_against_relations(&states, rule)?;
                    let mut next_rules = rules.clone();
                    next_rules.push(rule.clone());
                    RuleSet::new(active_rules(&next_rules))
                        .validate_stratified()
                        .map_err(KernelError::Rule)?;
                    rules = next_rules;
                } else if let CatalogChange::RuleDisabled(rule_id) = change {
                    disable_rule_in(&mut rules, *rule_id)?;
                }
            }
            for change in commit.changes() {
                let relation = states
                    .get_mut(&change.relation)
                    .ok_or(KernelError::UnknownRelation(change.relation))?;
                if relation.metadata().arity() as usize != change.tuple.arity() {
                    return Err(KernelError::ArityMismatch {
                        relation: change.relation,
                        expected: relation.metadata().arity(),
                        actual: change.tuple.arity(),
                    });
                }
                match change.kind {
                    FactChangeKind::Assert => relation.insert(change.tuple.clone()),
                    FactChangeKind::Retract => relation.remove(&change.tuple),
                }
            }
        }

        let version = commits.last().map_or(0, Commit::version);
        Ok(Self {
            root: ArcSwap::new(Arc::new(Snapshot {
                version,
                relations: states,
                rules,
                derived_cache: empty_derived_cache(),
                commits: commits.into(),
            })),
            provider,
        })
    }

    pub fn load_from_commit_log(
        commits: impl IntoIterator<Item = Commit>,
        provider: Arc<dyn CommitProvider>,
    ) -> Result<Self, KernelError> {
        let commits = commits.into_iter().collect::<Vec<_>>();
        let mut states = BTreeMap::new();
        let mut rules = Vec::new();

        for commit in &commits {
            for change in commit.catalog_changes() {
                match change {
                    CatalogChange::RelationCreated(metadata) => {
                        states.insert(metadata.id(), RelationState::empty(metadata.clone())?);
                    }
                    CatalogChange::RuleInstalled(rule) => {
                        validate_rule_definition_against_relations(&states, rule)?;
                        let mut next_rules = rules.clone();
                        next_rules.push(rule.clone());
                        RuleSet::new(active_rules(&next_rules))
                            .validate_stratified()
                            .map_err(KernelError::Rule)?;
                        rules = next_rules;
                    }
                    CatalogChange::RuleDisabled(rule_id) => {
                        disable_rule_in(&mut rules, *rule_id)?;
                    }
                }
            }
            for change in commit.changes() {
                let relation = states
                    .get_mut(&change.relation)
                    .ok_or(KernelError::UnknownRelation(change.relation))?;
                if relation.metadata().arity() as usize != change.tuple.arity() {
                    return Err(KernelError::ArityMismatch {
                        relation: change.relation,
                        expected: relation.metadata().arity(),
                        actual: change.tuple.arity(),
                    });
                }
                match change.kind {
                    FactChangeKind::Assert => relation.insert(change.tuple.clone()),
                    FactChangeKind::Retract => relation.remove(&change.tuple),
                }
            }
        }

        let version = commits.last().map_or(0, Commit::version);
        Ok(Self {
            root: ArcSwap::new(Arc::new(Snapshot {
                version,
                relations: states,
                rules,
                derived_cache: empty_derived_cache(),
                commits: commits.into(),
            })),
            provider,
        })
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.root.load_full()
    }

    pub fn create_relation(
        &self,
        metadata: RelationMetadata,
    ) -> Result<Arc<Snapshot>, KernelError> {
        let relation = RelationState::empty(metadata.clone())?;

        let mut current = self.snapshot();
        loop {
            if current.relations.contains_key(&metadata.id()) {
                return Err(KernelError::RelationAlreadyExists(metadata.id()));
            }

            let mut next = (*current).clone();
            next.relations.insert(metadata.id(), relation.clone());
            next.derived_cache = empty_derived_cache();
            next.version += 1;
            let commit = Commit {
                version: next.version,
                catalog_changes: Arc::from([CatalogChange::RelationCreated(metadata.clone())]),
                changes: Arc::from([]),
                bloom: crate::commit_bloom::CommitBloom::new(),
            };
            let mut commits = Vec::with_capacity(current.commits.len() + 1);
            commits.extend(current.commits.iter().cloned());
            commits.push(commit.clone());
            next.commits = commits.into();
            let next = Arc::new(next);

            if self.try_publish(current.version(), next.clone()) {
                self.persist_commit(&commit)?;
                return Ok(next);
            }

            current = self.snapshot();
        }
    }

    pub fn install_rule(
        &self,
        rule: Rule,
        source: impl Into<String>,
    ) -> Result<RuleDefinition, KernelError> {
        let source = source.into();
        let mut current = self.snapshot();
        loop {
            validate_rule_against_relations(&current.relations, &rule)?;
            let definition =
                RuleDefinition::new(next_rule_id(&current.rules), rule.clone(), source.clone());
            let mut rules = current.rules.clone();
            rules.push(definition.clone());
            RuleSet::new(active_rules(&rules))
                .validate_stratified()
                .map_err(KernelError::Rule)?;

            let mut next = (*current).clone();
            next.rules = rules;
            next.derived_cache = empty_derived_cache();
            next.version += 1;
            let commit = Commit {
                version: next.version,
                catalog_changes: Arc::from([CatalogChange::RuleInstalled(definition.clone())]),
                changes: Arc::from([]),
                bloom: crate::commit_bloom::CommitBloom::new(),
            };
            let mut commits = Vec::with_capacity(current.commits.len() + 1);
            commits.extend(current.commits.iter().cloned());
            commits.push(commit.clone());
            next.commits = commits.into();
            let next = Arc::new(next);

            if self.try_publish(current.version(), next.clone()) {
                self.persist_commit(&commit)?;
                return Ok(definition);
            }

            current = self.snapshot();
        }
    }

    pub fn disable_rule(&self, rule_id: crate::FactId) -> Result<Arc<Snapshot>, KernelError> {
        let mut current = self.snapshot();
        loop {
            let mut rules = current.rules.clone();
            disable_rule_in(&mut rules, rule_id)?;
            RuleSet::new(active_rules(&rules))
                .validate_stratified()
                .map_err(KernelError::Rule)?;

            let mut next = (*current).clone();
            next.rules = rules;
            next.derived_cache = empty_derived_cache();
            next.version += 1;
            let commit = Commit {
                version: next.version,
                catalog_changes: Arc::from([CatalogChange::RuleDisabled(rule_id)]),
                changes: Arc::from([]),
                bloom: crate::commit_bloom::CommitBloom::new(),
            };
            let mut commits = Vec::with_capacity(current.commits.len() + 1);
            commits.extend(current.commits.iter().cloned());
            commits.push(commit.clone());
            next.commits = commits.into();
            let next = Arc::new(next);

            if self.try_publish(current.version(), next.clone()) {
                self.persist_commit(&commit)?;
                return Ok(next);
            }

            current = self.snapshot();
        }
    }

    pub fn begin(&self) -> Transaction<'_> {
        Transaction::new(self, self.snapshot())
    }

    pub(crate) fn try_publish(&self, expected_version: u64, next: Arc<Snapshot>) -> bool {
        let mut success = false;
        self.root.rcu(|current| {
            if current.version == expected_version {
                success = true;
                next.clone()
            } else {
                success = false;
                Arc::clone(current)
            }
        });
        success
    }

    pub(crate) fn persist_commit(&self, commit: &Commit) -> Result<(), KernelError> {
        self.provider
            .persist_commit(commit)
            .map_err(KernelError::Persistence)
    }
}

fn validate_rule_definition_against_relations(
    relations: &BTreeMap<crate::RelationId, RelationState>,
    definition: &RuleDefinition,
) -> Result<(), KernelError> {
    validate_rule_against_relations(relations, definition.rule())
}

fn validate_rule_against_relations(
    relations: &BTreeMap<crate::RelationId, RelationState>,
    rule: &Rule,
) -> Result<(), KernelError> {
    validate_rule_atom(relations, rule.head_relation(), rule.head_terms())?;
    for atom in rule.body() {
        validate_rule_atom(relations, atom.relation(), atom.terms())?;
    }
    Ok(())
}

fn next_rule_id(rules: &[RuleDefinition]) -> crate::FactId {
    let mut raw = GENERATED_RULE_ID_START + rules.len() as u64;
    loop {
        let id = crate::FactId::new(raw & crate::FactId::MAX).unwrap();
        if !rules.iter().any(|rule| rule.id() == id) {
            return id;
        }
        raw = raw.wrapping_add(1);
    }
}

fn disable_rule_in(
    rules: &mut [RuleDefinition],
    rule_id: crate::FactId,
) -> Result<(), KernelError> {
    let Some(rule) = rules.iter_mut().find(|rule| rule.id() == rule_id) else {
        return Err(KernelError::UnknownRule(rule_id));
    };
    rule.deactivate();
    Ok(())
}

fn validate_rule_atom(
    relations: &BTreeMap<crate::RelationId, RelationState>,
    relation: crate::RelationId,
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

impl Default for RelationKernel {
    fn default() -> Self {
        Self::new()
    }
}
