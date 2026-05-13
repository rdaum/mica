use crate::{
    CatalogChange, CatalogFact, CatalogPredicate, Conflict, ConflictKind, ConflictPolicy, Fact,
    FactChange, FactChangeKind, InMemoryCommitProvider, KernelError, MentionedFact, RelationId,
    RelationKernel, RelationMetadata, SubjectFact, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::sync::Arc;

fn rel(id: u64) -> RelationId {
    Identity::new(id).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

#[test]
fn fact_identity_is_separate_from_set_tuple_identity() {
    let tuple = Tuple::from([int(1), int(2)]);
    let fact = Fact::new(rel(99), rel(1), tuple.clone());

    assert_eq!(fact.id(), rel(99));
    assert_eq!(fact.relation(), rel(1));
    assert_eq!(fact.tuple(), &tuple);
}

fn kernel_with_located() -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(1), Symbol::intern("LocatedIn"), 2)
                .with_index([0])
                .with_index([1, 0]),
        )
        .unwrap();
    kernel
}

#[test]
fn catalog_facts_expose_relation_metadata_as_relations() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(77), Symbol::intern("Delegates"), 3)
                .with_argument_name(0, Symbol::intern("child"))
                .with_argument_name(1, Symbol::intern("proto"))
                .with_argument_name(2, Symbol::intern("rank"))
                .with_index([0, 2, 1])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0, 2],
                }),
        )
        .unwrap();

    let facts = kernel.snapshot().catalog_facts();
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::RelationName,
        tuple: Tuple::from([
            Value::identity(rel(77)),
            Value::symbol(Symbol::intern("Delegates"))
        ]),
    }));
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::ArgumentName,
        tuple: Tuple::from([
            Value::identity(rel(77)),
            int(1),
            Value::symbol(Symbol::intern("proto")),
        ]),
    }));
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::FunctionalKey,
        tuple: Tuple::from([Value::identity(rel(77)), int(1), int(2)]),
    }));
    assert!(facts.iter().any(|fact| {
        fact.predicate == CatalogPredicate::IndexStorageKind
            && fact.tuple.values()[1] == Value::symbol(Symbol::intern("btree"))
    }));
}

#[test]
fn creating_duplicate_relation_id_is_rejected() {
    let kernel = kernel_with_located();
    let error = kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedElsewhere"),
            2,
        ))
        .unwrap_err();

    assert_eq!(error, KernelError::RelationAlreadyExists(rel(1)));
}

#[test]
fn transaction_reads_own_asserts_and_retracts() {
    let kernel = kernel_with_located();
    let mut tx = kernel.begin();
    let tuple = Tuple::from([int(10), int(20)]);
    tx.assert(rel(1), tuple.clone()).unwrap();
    assert_eq!(
        tx.scan(rel(1), &[Some(int(10)), None]).unwrap(),
        vec![tuple.clone()]
    );
    tx.retract(rel(1), tuple).unwrap();
    assert!(tx.scan(rel(1), &[Some(int(10)), None]).unwrap().is_empty());
}

#[test]
fn committed_snapshot_is_immutable_for_existing_transactions() {
    let kernel = kernel_with_located();
    let old = kernel.begin();

    let mut tx = kernel.begin();
    tx.assert(rel(1), Tuple::from([int(1), int(2)])).unwrap();
    tx.commit().unwrap();

    assert!(old.scan(rel(1), &[None, None]).unwrap().is_empty());
    assert_eq!(
        kernel.snapshot().scan(rel(1), &[None, None]).unwrap().len(),
        1
    );
}

#[test]
fn concurrent_identical_set_asserts_merge() {
    let kernel = kernel_with_located();
    let mut left = kernel.begin();
    let mut right = kernel.begin();
    let tuple = Tuple::from([int(1), int(2)]);
    left.assert(rel(1), tuple.clone()).unwrap();
    right.assert(rel(1), tuple.clone()).unwrap();
    left.commit().unwrap();
    let right_commit = right.commit().unwrap();
    assert!(right_commit.commit().changes().is_empty());
    assert_eq!(
        kernel.snapshot().scan(rel(1), &[None, None]).unwrap(),
        vec![tuple]
    );
}

#[test]
fn stale_disjoint_set_asserts_both_commit() {
    let kernel = kernel_with_located();
    let mut left = kernel.begin();
    let mut right = kernel.begin();
    let left_tuple = Tuple::from([int(1), int(2)]);
    let right_tuple = Tuple::from([int(3), int(4)]);

    left.assert(rel(1), left_tuple.clone()).unwrap();
    right.assert(rel(1), right_tuple.clone()).unwrap();
    left.commit().unwrap();
    right.commit().unwrap();

    assert_eq!(
        kernel.snapshot().scan(rel(1), &[None, None]).unwrap(),
        vec![left_tuple, right_tuple]
    );
}

#[test]
fn stale_retract_of_absent_tuple_does_not_delete_concurrent_assert() {
    let kernel = kernel_with_located();
    let tuple = Tuple::from([int(1), int(2)]);
    let mut stale = kernel.begin();
    let mut inserter = kernel.begin();

    stale.retract(rel(1), tuple.clone()).unwrap();
    inserter.assert(rel(1), tuple.clone()).unwrap();
    inserter.commit().unwrap();
    let result = stale.commit().unwrap();

    assert!(result.commit().changes().is_empty());
    assert_eq!(
        kernel.snapshot().scan(rel(1), &[None, None]).unwrap(),
        vec![tuple]
    );
}

#[test]
fn functional_replace_conflicts_when_key_changes_concurrently() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(2), Symbol::intern("Name"), 2)
                .with_index([0])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0],
                }),
        )
        .unwrap();

    let mut seed = kernel.begin();
    seed.replace_functional(rel(2), Tuple::from([int(1), Value::string("old")]))
        .unwrap();
    seed.commit().unwrap();

    let mut left = kernel.begin();
    let mut right = kernel.begin();
    left.replace_functional(rel(2), Tuple::from([int(1), Value::string("left")]))
        .unwrap();
    right
        .replace_functional(rel(2), Tuple::from([int(1), Value::string("right")]))
        .unwrap();
    left.commit().unwrap();

    let error = right.commit().unwrap_err();
    assert!(matches!(
        error,
        KernelError::Conflict(Conflict {
            kind: ConflictKind::FunctionalKeyChanged,
            ..
        })
    ));
}

#[test]
fn functional_conflict_validation_supports_nonleading_keys() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(3), Symbol::intern("OwnedName"), 3)
                .with_index([1])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![1],
                }),
        )
        .unwrap();

    let mut seed = kernel.begin();
    seed.replace_functional(
        rel(3),
        Tuple::from([int(100), int(1), Value::string("old")]),
    )
    .unwrap();
    seed.commit().unwrap();

    let mut left = kernel.begin();
    let mut right = kernel.begin();
    left.replace_functional(
        rel(3),
        Tuple::from([int(100), int(1), Value::string("left")]),
    )
    .unwrap();
    right
        .replace_functional(
            rel(3),
            Tuple::from([int(200), int(1), Value::string("right")]),
        )
        .unwrap();
    left.commit().unwrap();

    let error = right.commit().unwrap_err();
    assert!(matches!(
        error,
        KernelError::Conflict(Conflict {
            kind: ConflictKind::FunctionalKeyChanged,
            ..
        })
    ));
}

#[test]
fn indexed_scan_respects_non_leading_bindings() {
    let kernel = kernel_with_located();
    let mut tx = kernel.begin();
    tx.assert(rel(1), Tuple::from([int(1), int(9)])).unwrap();
    tx.assert(rel(1), Tuple::from([int(2), int(9)])).unwrap();
    tx.assert(rel(1), Tuple::from([int(3), int(8)])).unwrap();
    tx.commit().unwrap();

    let found = kernel
        .snapshot()
        .scan(rel(1), &[None, Some(int(9))])
        .unwrap();
    assert_eq!(found.len(), 2);
}

#[test]
fn commit_result_records_semantic_fact_changes() {
    let kernel = kernel_with_located();
    let tuple = Tuple::from([int(1), int(2)]);

    let mut seed = kernel.begin();
    seed.assert(rel(1), tuple.clone()).unwrap();
    let seed_result = seed.commit().unwrap();
    assert_eq!(seed_result.commit().version(), 2);
    assert_eq!(
        seed_result.commit().changes(),
        &[FactChange {
            relation: rel(1),
            tuple: tuple.clone(),
            kind: FactChangeKind::Assert,
        }]
    );

    let mut tx = kernel.begin();
    tx.retract(rel(1), tuple.clone()).unwrap();
    let result = tx.commit().unwrap();
    assert_eq!(
        result.commit().changes(),
        &[FactChange {
            relation: rel(1),
            tuple,
            kind: FactChangeKind::Retract,
        }]
    );
    assert_eq!(result.snapshot().commits_since(1).len(), 2);
}

#[test]
fn successful_commits_are_persisted_as_fact_change_batches() {
    let provider = Arc::new(InMemoryCommitProvider::new());
    let kernel = RelationKernel::with_provider(provider.clone());
    kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap();

    let tuple = Tuple::from([int(1), int(2)]);
    let mut tx = kernel.begin();
    tx.assert(rel(1), tuple.clone()).unwrap();
    tx.commit().unwrap();

    let commits = provider.commits();
    assert_eq!(commits.len(), 2);
    assert_eq!(
        commits[0].catalog_changes(),
        &[CatalogChange::RelationCreated(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))]
    );
    assert_eq!(commits[1].version(), 2);
    assert_eq!(
        commits[1].changes(),
        &[FactChange {
            relation: rel(1),
            tuple,
            kind: FactChangeKind::Assert,
        }]
    );
}

#[test]
fn kernel_can_replay_persisted_commit_batches() {
    let provider = Arc::new(InMemoryCommitProvider::new());
    let metadata = RelationMetadata::new(rel(1), Symbol::intern("LocatedIn"), 2);
    let kernel = RelationKernel::with_provider(provider.clone());
    kernel.create_relation(metadata.clone()).unwrap();

    let kept = Tuple::from([int(1), int(2)]);
    let removed = Tuple::from([int(3), int(4)]);
    let mut seed = kernel.begin();
    seed.assert(rel(1), kept.clone()).unwrap();
    seed.assert(rel(1), removed.clone()).unwrap();
    seed.commit().unwrap();

    let mut tx = kernel.begin();
    tx.retract(rel(1), removed).unwrap();
    tx.commit().unwrap();

    let loaded = RelationKernel::load_from_commits(
        [metadata],
        provider.commits(),
        Arc::new(InMemoryCommitProvider::new()),
    )
    .unwrap();
    assert_eq!(loaded.snapshot().version(), 3);
    assert_eq!(
        loaded.snapshot().scan(rel(1), &[None, None]).unwrap(),
        vec![kept]
    );
}

#[test]
fn kernel_can_replay_catalog_and_fact_commit_log() {
    let provider = Arc::new(InMemoryCommitProvider::new());
    let kernel = RelationKernel::with_provider(provider.clone());
    kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap();

    let tuple = Tuple::from([int(1), int(2)]);
    let mut tx = kernel.begin();
    tx.assert(rel(1), tuple.clone()).unwrap();
    tx.commit().unwrap();

    let loaded = RelationKernel::load_from_commit_log(
        provider.commits(),
        Arc::new(InMemoryCommitProvider::new()),
    )
    .unwrap();
    assert_eq!(loaded.snapshot().version(), 2);
    assert_eq!(
        loaded.snapshot().scan(rel(1), &[None, None]).unwrap(),
        vec![tuple]
    );
}

#[test]
fn snapshot_neighborhood_views_find_subject_and_mentions() {
    let kernel = kernel_with_located();
    kernel
        .create_relation(RelationMetadata::new(rel(4), Symbol::intern("Tagged"), 3).with_index([2]))
        .unwrap();

    let mut tx = kernel.begin();
    tx.assert(rel(1), Tuple::from([int(1), int(9)])).unwrap();
    tx.assert(
        rel(4),
        Tuple::from([int(2), Value::symbol(Symbol::intern("kind")), int(9)]),
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = kernel.snapshot();

    assert_eq!(
        snapshot.subject_facts(&int(1)).unwrap(),
        vec![SubjectFact {
            subject: int(1),
            relation: rel(1),
            tuple: Tuple::from([int(1), int(9)]),
        }]
    );
    assert_eq!(
        snapshot.mentioned_facts(&int(9)).unwrap(),
        vec![
            MentionedFact {
                identity: int(9),
                relation: rel(1),
                position: 1,
                tuple: Tuple::from([int(1), int(9)]),
            },
            MentionedFact {
                identity: int(9),
                relation: rel(4),
                position: 2,
                tuple: Tuple::from([int(2), Value::symbol(Symbol::intern("kind")), int(9)]),
            },
        ]
    );
}

#[test]
fn transaction_neighborhood_views_include_local_overlay() {
    let kernel = kernel_with_located();
    let committed = Tuple::from([int(1), int(2)]);

    let mut seed = kernel.begin();
    seed.assert(rel(1), committed.clone()).unwrap();
    seed.commit().unwrap();

    let mut tx = kernel.begin();
    let local = Tuple::from([int(1), int(3)]);
    tx.retract(rel(1), committed).unwrap();
    tx.assert(rel(1), local.clone()).unwrap();

    assert_eq!(
        tx.subject_facts(&int(1)).unwrap(),
        vec![SubjectFact {
            subject: int(1),
            relation: rel(1),
            tuple: local,
        }]
    );
}
