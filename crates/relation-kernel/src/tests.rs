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

use crate::{
    Atom, CatalogChange, CatalogFact, CatalogPredicate, Commit, CommitProvider,
    ComposedRelationRead, ComposedTransactionRead, Conflict, ConflictKind, ConflictPolicy, Fact,
    FactChange, FactChangeKind, InMemoryCommitProvider, KernelError, MentionedFact, ProjectedStore,
    QueryPlan, RelationId, RelationKernel, RelationMetadata, RelationRead, RelationWorkspace, Rule,
    SubjectFact, Term, TransientStore, Tuple,
};
#[cfg(feature = "fjall-provider")]
use crate::{FjallDurabilityMode, FjallFormatStatus, FjallStateProvider};
use mica_var::{Identity, Symbol, Value};
#[cfg(feature = "fjall-provider")]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(feature = "fjall-provider")]
use std::time::{SystemTime, UNIX_EPOCH};

fn rel(id: u64) -> RelationId {
    Identity::new(id).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

fn cap(value: u64) -> Value {
    Value::capability_raw(value).unwrap()
}

fn var(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

#[cfg(feature = "fjall-provider")]
struct TempStore {
    path: PathBuf,
}

struct FailAfterCommitProvider {
    commits: Mutex<Vec<Commit>>,
    remaining_successes: Mutex<usize>,
}

impl FailAfterCommitProvider {
    fn new(remaining_successes: usize) -> Self {
        Self {
            commits: Mutex::new(Vec::new()),
            remaining_successes: Mutex::new(remaining_successes),
        }
    }
}

impl CommitProvider for FailAfterCommitProvider {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String> {
        self.commits.lock().unwrap().push(commit.clone());
        let mut remaining_successes = self.remaining_successes.lock().unwrap();
        if *remaining_successes == 0 {
            Err("intentional persistence failure".to_owned())
        } else {
            *remaining_successes -= 1;
            Ok(())
        }
    }
}

#[cfg(feature = "fjall-provider")]
impl TempStore {
    fn new(name: &str) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mica-relation-kernel-{name}-{}-{suffix}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(feature = "fjall-provider")]
impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
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
fn transaction_rejects_capability_values_in_tuples() {
    let kernel = kernel_with_located();
    let mut tx = kernel.begin();
    let tuple = Tuple::from([int(10), cap(1)]);

    assert_eq!(
        tx.assert(rel(1), tuple.clone()).unwrap_err(),
        KernelError::NonPersistentValue {
            relation: rel(1),
            tuple,
        }
    );

    let nested = Tuple::from([
        int(10),
        Value::map([(Value::symbol(Symbol::intern("cap")), cap(2))]),
    ]);
    assert_eq!(
        tx.replace_functional(rel(1), nested.clone()).unwrap_err(),
        KernelError::NonPersistentValue {
            relation: rel(1),
            tuple: nested,
        }
    );
}

#[test]
fn installed_rules_derive_tuples_as_relation_reads() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            rel(2),
            Symbol::intern("VisibleTo"),
            2,
        ))
        .unwrap();
    kernel
        .install_rule(
            Rule::new(
                rel(2),
                [var("actor"), var("obj")],
                [
                    Atom::positive(rel(1), [var("actor"), var("room")]),
                    Atom::positive(rel(1), [var("obj"), var("room")]),
                ],
            ),
            "VisibleTo(actor, obj) :- LocatedIn(actor, room), LocatedIn(obj, room)",
        )
        .unwrap();

    let mut tx = kernel.begin();
    tx.assert(rel(1), Tuple::from([int(10), int(1)])).unwrap();
    tx.assert(rel(1), Tuple::from([int(20), int(1)])).unwrap();

    assert_eq!(
        tx.scan(rel(2), &[Some(int(10)), None]).unwrap(),
        vec![
            Tuple::from([int(10), int(10)]),
            Tuple::from([int(10), int(20)])
        ]
    );
}

#[test]
fn relation_reads_union_asserted_and_rule_derived_tuples() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            rel(2),
            Symbol::intern("VisibleTo"),
            2,
        ))
        .unwrap();
    kernel
        .install_rule(
            Rule::new(
                rel(2),
                [var("actor"), var("obj")],
                [Atom::positive(rel(1), [var("actor"), var("obj")])],
            ),
            "VisibleTo(actor, obj) :- LocatedIn(actor, obj)",
        )
        .unwrap();

    let mut seed = kernel.begin();
    seed.assert(rel(1), Tuple::from([int(10), int(20)]))
        .unwrap();
    seed.assert(rel(2), Tuple::from([int(99), int(100)]))
        .unwrap();
    seed.commit().unwrap();

    assert_eq!(
        kernel.snapshot().scan(rel(2), &[None, None]).unwrap(),
        vec![
            Tuple::from([int(10), int(20)]),
            Tuple::from([int(99), int(100)])
        ]
    );
}

#[test]
fn installed_rules_have_catalog_facts_and_can_be_disabled() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            rel(2),
            Symbol::intern("VisibleTo"),
            2,
        ))
        .unwrap();
    let definition = kernel
        .install_rule(
            Rule::new(
                rel(2),
                [var("actor"), var("obj")],
                [Atom::positive(rel(1), [var("actor"), var("obj")])],
            ),
            "VisibleTo(actor, obj) :- LocatedIn(actor, obj)",
        )
        .unwrap();
    let rule_id = definition.id();

    let facts = kernel.snapshot().catalog_facts();
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::Rule,
        tuple: Tuple::from([Value::identity(rule_id)]),
    }));
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::RuleHead,
        tuple: Tuple::from([Value::identity(rule_id), Value::identity(rel(2))]),
    }));
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::RuleSource,
        tuple: Tuple::from([
            Value::identity(rule_id),
            Value::string("VisibleTo(actor, obj) :- LocatedIn(actor, obj)")
        ]),
    }));
    assert!(facts.contains(&CatalogFact {
        predicate: CatalogPredicate::ActiveRule,
        tuple: Tuple::from([Value::identity(rule_id), Value::bool(true)]),
    }));

    let mut seed = kernel.begin();
    seed.assert(rel(1), Tuple::from([int(10), int(20)]))
        .unwrap();
    seed.commit().unwrap();
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[None, None]).unwrap(),
        vec![Tuple::from([int(10), int(20)])]
    );

    kernel.disable_rule(rule_id).unwrap();
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[None, None]).unwrap(),
        vec![]
    );
    assert!(kernel.snapshot().catalog_facts().contains(&CatalogFact {
        predicate: CatalogPredicate::ActiveRule,
        tuple: Tuple::from([Value::identity(rule_id), Value::bool(false)]),
    }));
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
fn transient_store_scans_only_visible_scopes_and_drops_scope() {
    let mut transient = TransientStore::new();
    let scope_a = rel(10);
    let scope_b = rel(11);
    let metadata = RelationMetadata::new(rel(20), Symbol::intern("UiText"), 2).with_index([0]);
    let a_tuple = Tuple::from([int(1), Value::string("left")]);
    let b_tuple = Tuple::from([int(2), Value::string("right")]);

    assert!(
        transient
            .assert(scope_a, metadata.clone(), a_tuple.clone())
            .unwrap()
    );
    assert!(
        transient
            .assert(scope_b, metadata.clone(), b_tuple.clone())
            .unwrap()
    );
    assert!(
        !transient
            .assert(scope_b, metadata, b_tuple.clone())
            .unwrap()
    );

    assert_eq!(transient.scope_len(scope_a), 1);
    assert_eq!(
        transient.scan(&[scope_a], rel(20), &[None, None]).unwrap(),
        vec![a_tuple.clone()]
    );
    assert_eq!(
        transient
            .scan(&[scope_a, scope_b], rel(20), &[None, None])
            .unwrap(),
        vec![a_tuple, b_tuple.clone()]
    );
    assert_eq!(transient.drop_scope(scope_b), 1);
    assert!(
        transient
            .scan(&[scope_b], rel(20), &[None, None])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn composed_reader_joins_durable_and_transient_tuples() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(RelationMetadata::new(rel(30), Symbol::intern("Actor"), 1))
        .unwrap();
    let held = RelationMetadata::new(rel(31), Symbol::intern("Held"), 2)
        .with_index([0])
        .with_index([1, 0]);
    kernel.create_relation(held.clone()).unwrap();
    let mut tx = kernel.begin();
    tx.assert(rel(30), Tuple::from([int(1)])).unwrap();
    tx.commit().unwrap();
    let scope = rel(40);
    let mut transient = TransientStore::new();
    transient
        .assert(scope, held, Tuple::from([int(1), int(99)]))
        .unwrap();
    let snapshot = kernel.snapshot();
    let scopes = [scope];
    let reader = ComposedRelationRead::new(snapshot.as_ref(), &transient, &scopes);

    let rows = QueryPlan::join_eq(
        QueryPlan::scan(rel(30), [None]),
        QueryPlan::scan(rel(31), [None, None]),
        [0],
        [0],
    )
    .execute(&reader)
    .unwrap();

    assert_eq!(rows, vec![Tuple::from([int(1), int(1), int(99)])]);
}

#[test]
fn composed_reader_joins_transient_scopes() {
    let kernel = RelationKernel::new();
    let selected = RelationMetadata::new(rel(50), Symbol::intern("Selected"), 1);
    let name = RelationMetadata::new(rel(51), Symbol::intern("Name"), 2).with_index([0]);
    kernel.create_relation(selected.clone()).unwrap();
    kernel.create_relation(name.clone()).unwrap();
    let left_scope = rel(60);
    let right_scope = rel(61);
    let mut transient = TransientStore::new();
    transient
        .assert(left_scope, selected, Tuple::from([int(7)]))
        .unwrap();
    transient
        .assert(
            right_scope,
            name,
            Tuple::from([int(7), Value::string("lamp")]),
        )
        .unwrap();
    let scopes = [left_scope, right_scope];
    let snapshot = kernel.snapshot();
    let reader = ComposedRelationRead::new(snapshot.as_ref(), &transient, &scopes);

    let rows = QueryPlan::join_eq(
        QueryPlan::scan(rel(50), [None]),
        QueryPlan::scan(rel(51), [None, None]),
        [0],
        [0],
    )
    .execute(&reader)
    .unwrap();

    assert_eq!(
        rows,
        vec![Tuple::from([int(7), int(7), Value::string("lamp")])]
    );
}

#[test]
fn composed_transaction_reader_derives_rules_from_transient_inputs() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(RelationMetadata::new(
            rel(70),
            Symbol::intern("Selected"),
            1,
        ))
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(rel(71), Symbol::intern("Visible"), 1))
        .unwrap();
    kernel
        .install_rule(
            Rule::new(
                rel(71),
                [var("item")],
                [Atom::positive(rel(70), [var("item")])],
            ),
            "Visible(item) :- Selected(item)",
        )
        .unwrap();
    let scope = rel(72);
    let mut transient = TransientStore::new();
    transient
        .assert(
            scope,
            RelationMetadata::new(rel(70), Symbol::intern("Selected"), 1),
            Tuple::from([int(9)]),
        )
        .unwrap();
    let tx = kernel.begin();
    let scopes = [scope];
    let reader = ComposedTransactionRead::new(&tx, &transient, &scopes);

    assert_eq!(
        reader.scan_relation(rel(71), &[None]).unwrap(),
        vec![Tuple::from([int(9)])]
    );
}

#[test]
fn projected_store_applies_server_commits_without_provider() {
    let kernel = RelationKernel::new();
    let name = RelationMetadata::new(rel(73), Symbol::intern("Name"), 2)
        .with_index([0])
        .with_conflict_policy(ConflictPolicy::Functional {
            key_positions: vec![0],
        });
    let create_snapshot = kernel.create_relation(name.clone()).unwrap();
    let create_commit = create_snapshot.commits_since(0).last().unwrap().clone();
    let mut tx = kernel.begin();
    tx.replace_functional(rel(73), Tuple::from([int(1), Value::string("lamp")]))
        .unwrap();
    let write_commit = tx.commit().unwrap().commit().clone();

    let mut projected = ProjectedStore::new();
    projected.apply_commit(&create_commit).unwrap();
    projected.apply_commit(&write_commit).unwrap();

    assert_eq!(
        projected
            .scan_relation(rel(73), &[Some(int(1)), None])
            .unwrap(),
        vec![Tuple::from([int(1), Value::string("lamp")])]
    );
}

#[test]
fn projected_store_implements_workspace_for_local_mutation() {
    fn rename(
        workspace: &mut impl RelationWorkspace,
        relation: RelationId,
        object: Value,
        name: &str,
    ) {
        workspace
            .replace_functional_tuple(relation, Tuple::from([object, Value::string(name)]))
            .unwrap();
    }

    let mut projected = ProjectedStore::new();
    projected
        .create_relation(
            RelationMetadata::new(rel(74), Symbol::intern("Name"), 2)
                .with_index([0])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0],
                }),
        )
        .unwrap();

    rename(&mut projected, rel(74), int(1), "brass lamp");
    rename(&mut projected, rel(74), int(1), "golden lamp");

    assert_eq!(
        projected
            .scan_relation(rel(74), &[Some(int(1)), None])
            .unwrap(),
        vec![Tuple::from([int(1), Value::string("golden lamp")])]
    );
}

#[test]
fn projected_store_evaluates_recursive_rules() {
    let mut projected = ProjectedStore::new();
    projected
        .create_relation(RelationMetadata::new(rel(75), Symbol::intern("Edge"), 2))
        .unwrap();
    projected
        .create_relation(RelationMetadata::new(
            rel(76),
            Symbol::intern("Reachable"),
            2,
        ))
        .unwrap();
    projected
        .install_rule(
            Rule::new(
                rel(76),
                [var("from"), var("to")],
                [Atom::positive(rel(75), [var("from"), var("to")])],
            ),
            "Reachable(from, to) :- Edge(from, to)",
        )
        .unwrap();
    projected
        .install_rule(
            Rule::new(
                rel(76),
                [var("from"), var("to")],
                [
                    Atom::positive(rel(75), [var("from"), var("mid")]),
                    Atom::positive(rel(76), [var("mid"), var("to")]),
                ],
            ),
            "Reachable(from, to) :- Edge(from, mid), Reachable(mid, to)",
        )
        .unwrap();
    projected
        .assert_tuple(rel(75), Tuple::from([int(1), int(2)]))
        .unwrap();
    projected
        .assert_tuple(rel(75), Tuple::from([int(2), int(3)]))
        .unwrap();

    assert_eq!(
        projected
            .scan_relation(rel(76), &[Some(int(1)), Some(int(3))])
            .unwrap(),
        vec![Tuple::from([int(1), int(3)])]
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
fn failed_persistence_does_not_publish_live_snapshot() {
    let provider = Arc::new(FailAfterCommitProvider::new(0));
    let kernel = RelationKernel::with_provider(provider);
    let error = kernel
        .create_relation(RelationMetadata::new(
            rel(1),
            Symbol::intern("LocatedIn"),
            2,
        ))
        .unwrap_err();
    assert!(matches!(error, KernelError::Persistence(_)));
    assert_eq!(kernel.snapshot().version(), 0);
    assert!(matches!(
        kernel.snapshot().scan(rel(1), &[None, None]),
        Err(KernelError::UnknownRelation(id)) if id == rel(1)
    ));
}

#[test]
fn failed_fact_persistence_does_not_publish_live_snapshot() {
    let provider = Arc::new(FailAfterCommitProvider::new(1));
    let kernel = RelationKernel::with_provider(provider);
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
    let error = tx.commit().unwrap_err();
    assert!(matches!(error, KernelError::Persistence(_)));
    assert_eq!(kernel.snapshot().version(), 1);
    assert!(!kernel.snapshot().contains(rel(1), &tuple).unwrap());
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

#[cfg(feature = "fjall-provider")]
#[test]
fn fjall_provider_persists_and_loads_canonical_state() {
    let store = TempStore::new("canonical-state");
    let values_tuple = Tuple::from([
        Value::nothing(),
        Value::bool(true),
        int(42),
        Value::float(12.5),
        Value::identity(rel(99)),
        Value::symbol(Symbol::intern("symbolic")),
        Value::error_code(Symbol::intern("E_PERSIST")),
        Value::string("stored"),
        Value::bytes([0xde, 0xad, 0xbe, 0xef]),
        Value::list([int(1), int(2), int(3)]),
        Value::map([(Value::symbol(Symbol::intern("k")), Value::string("v"))]),
        Value::range(int(1), Some(int(4))),
        Value::error(Symbol::intern("E_RICH"), Some("rich error"), Some(int(7))),
    ]);

    {
        let provider = Arc::new(FjallStateProvider::open_strict(store.path()).unwrap());
        assert_eq!(provider.durability(), FjallDurabilityMode::Strict);
        let kernel = RelationKernel::with_provider(provider.clone());
        kernel
            .create_relation(
                RelationMetadata::new(rel(10), Symbol::intern("ValueTuple"), 13)
                    .with_argument_name(0, Symbol::intern("nothing"))
                    .with_index([2, 0])
                    .with_conflict_policy(ConflictPolicy::Functional {
                        key_positions: vec![2],
                    }),
            )
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(rel(11), Symbol::intern("Base"), 1))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(rel(12), Symbol::intern("Derived"), 1))
            .unwrap();
        kernel
            .install_rule(
                Rule::new(
                    rel(12),
                    [var("item")],
                    [Atom::positive(rel(11), [var("item")])],
                ),
                "Derived(item) :- Base(item)",
            )
            .unwrap();

        let mut tx = kernel.begin();
        tx.assert(rel(10), values_tuple.clone()).unwrap();
        tx.assert(rel(11), Tuple::from([int(77)])).unwrap();
        let result = tx.commit().unwrap();
        assert_eq!(provider.queued_version(), result.commit().version());
        assert_eq!(provider.completed_version(), result.commit().version());
    }

    assert_eq!(
        FjallStateProvider::check_format(store.path()).unwrap(),
        FjallFormatStatus::Current
    );

    let provider = FjallStateProvider::open(store.path()).unwrap();
    let persisted = provider.load_state().unwrap();
    assert_eq!(persisted.version, 5);
    assert_eq!(provider.load_commits().unwrap().len(), 5);
    let loaded =
        RelationKernel::load_from_state(persisted, Arc::new(InMemoryCommitProvider::new()))
            .unwrap();

    assert_eq!(loaded.snapshot().version(), 5);
    assert_eq!(
        loaded.snapshot().scan(rel(10), &vec![None; 13]).unwrap(),
        vec![values_tuple]
    );
    assert_eq!(
        loaded.snapshot().scan(rel(12), &[None]).unwrap(),
        vec![Tuple::from([int(77)])]
    );
}

#[cfg(feature = "fjall-provider")]
#[test]
fn fjall_provider_reopens_loads_and_continues_committing() {
    let store = TempStore::new("reopen-continue");
    let first = Tuple::from([int(1), int(10)]);
    let second = Tuple::from([int(2), int(20)]);

    {
        let provider = Arc::new(FjallStateProvider::open(store.path()).unwrap());
        assert_eq!(provider.durability(), FjallDurabilityMode::Relaxed);
        let kernel = RelationKernel::with_provider(provider);
        kernel
            .create_relation(RelationMetadata::new(
                rel(20),
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        let mut tx = kernel.begin();
        tx.assert(rel(20), first.clone()).unwrap();
        let result = tx.commit().unwrap();
        assert_eq!(result.commit().version(), 2);
    }

    {
        let provider = Arc::new(FjallStateProvider::open(store.path()).unwrap());
        let persisted = provider.load_state().unwrap();
        assert_eq!(persisted.version, 2);
        let kernel = RelationKernel::load_from_state(persisted, provider.clone()).unwrap();
        assert_eq!(
            kernel.snapshot().scan(rel(20), &[None, None]).unwrap(),
            vec![first.clone()]
        );

        let mut tx = kernel.begin();
        tx.assert(rel(20), second.clone()).unwrap();
        let result = tx.commit().unwrap();
        assert_eq!(result.commit().version(), 3);
        assert_eq!(provider.queued_version(), 3);
    }

    let provider = FjallStateProvider::open(store.path()).unwrap();
    let persisted = provider.load_state().unwrap();
    assert_eq!(persisted.version, 3);
    assert_eq!(provider.load_commits().unwrap().len(), 3);
    let loaded =
        RelationKernel::load_from_state(persisted, Arc::new(InMemoryCommitProvider::new()))
            .unwrap();
    assert_eq!(
        loaded.snapshot().scan(rel(20), &[None, None]).unwrap(),
        vec![first, second]
    );
}

#[cfg(feature = "fjall-provider")]
#[test]
fn fjall_provider_detects_shape_mismatch() {
    let store = TempStore::new("format-mismatch");
    assert_eq!(
        FjallStateProvider::check_format(store.path()).unwrap(),
        FjallFormatStatus::Fresh
    );

    {
        let database = fjall::Database::builder(store.path()).open().unwrap();
        let metadata = database
            .keyspace("metadata", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        metadata.insert(b"format_version", b"old-format").unwrap();
        metadata.insert(b"shape", b"old-shape").unwrap();
    }

    assert!(matches!(
        FjallStateProvider::check_format(store.path()).unwrap(),
        FjallFormatStatus::MigrationRequired {
            stored_version: Some(version),
            stored_shape: Some(shape),
            ..
        } if version == "old-format" && shape == "old-shape"
    ));
    assert!(FjallStateProvider::open(store.path()).is_err());
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
