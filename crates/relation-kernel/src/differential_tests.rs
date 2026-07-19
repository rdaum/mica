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
    Atom, Commit, ExecutionContext, FactChangeKind, RelationId, RelationKernel, RelationMetadata,
    Rule, RuleBodyItem, RuleSet, Term, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn rel(id: u64) -> RelationId {
    Identity::new(id).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

fn var(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OracleEpoch {
    version: u64,
    input_changes: usize,
    affected_rule_components: usize,
    candidate_changes: usize,
    consolidated_changes: usize,
    visible_output_changes: usize,
    fixpoint_iterations: usize,
    frontier_rows: Vec<usize>,
    elapsed: Duration,
    derived: BTreeMap<RelationId, Vec<Tuple>>,
    visible: BTreeMap<RelationId, Vec<Tuple>>,
}

struct CompleteOracle {
    head_arities: BTreeMap<RelationId, usize>,
    previous_visible: BTreeMap<RelationId, Vec<Tuple>>,
    epochs: Vec<OracleEpoch>,
}

impl CompleteOracle {
    fn new(head_arities: impl IntoIterator<Item = (RelationId, usize)>) -> Self {
        Self {
            head_arities: head_arities.into_iter().collect(),
            previous_visible: BTreeMap::new(),
            epochs: Vec::new(),
        }
    }

    fn capture(&mut self, kernel: &RelationKernel, commit: &Commit) {
        let snapshot = kernel.snapshot();
        assert_eq!(snapshot.version(), commit.version());
        let evaluation = snapshot
            .evaluate_complete_rules(&ExecutionContext::serial())
            .unwrap();
        let rules = RuleSet::new(
            snapshot
                .rules()
                .iter()
                .filter(|rule| rule.active())
                .map(|rule| rule.rule().clone()),
        );
        let affected_rule_components = rules
            .affected_component_count(commit.changes().iter().map(|change| change.relation))
            .unwrap();
        let visible = self
            .head_arities
            .iter()
            .map(|(relation, arity)| {
                (
                    *relation,
                    snapshot.scan(*relation, &vec![None; *arity]).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let visible_output_changes = visible_change_count(&self.previous_visible, &visible);
        self.previous_visible.clone_from(&visible);
        self.epochs.push(OracleEpoch {
            version: snapshot.version(),
            input_changes: commit.changes().len(),
            affected_rule_components,
            candidate_changes: evaluation.stats.candidate_rows,
            consolidated_changes: evaluation.stats.novel_rows,
            visible_output_changes,
            fixpoint_iterations: evaluation.stats.rounds,
            frontier_rows: evaluation.stats.frontier_rows,
            elapsed: evaluation.stats.elapsed,
            derived: evaluation.derived,
            visible,
        });
    }
}

fn visible_change_count(
    previous: &BTreeMap<RelationId, Vec<Tuple>>,
    next: &BTreeMap<RelationId, Vec<Tuple>>,
) -> usize {
    previous
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|relation| {
            let previous = previous
                .get(&relation)
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            let next = next
                .get(&relation)
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>();
            previous.symmetric_difference(&next).count()
        })
        .sum()
}

fn create_relations(kernel: &RelationKernel, relations: &[(u64, &str, u16)]) {
    for (id, name, arity) in relations {
        kernel
            .create_relation(RelationMetadata::new(
                rel(*id),
                Symbol::intern(name),
                *arity,
            ))
            .unwrap();
    }
}

fn assert_rows(kernel: &RelationKernel, relation: RelationId, rows: &[(i64, i64)]) -> Commit {
    let mut tx = kernel.begin();
    for (left, right) in rows {
        tx.assert(relation, Tuple::from([int(*left), int(*right)]))
            .unwrap();
    }
    tx.commit().unwrap().commit().clone()
}

fn retract_rows(kernel: &RelationKernel, relation: RelationId, rows: &[(i64, i64)]) -> Commit {
    let mut tx = kernel.begin();
    for (left, right) in rows {
        tx.retract(relation, Tuple::from([int(*left), int(*right)]))
            .unwrap();
    }
    tx.commit().unwrap().commit().clone()
}

#[test]
fn complete_oracle_captures_cyclic_commit_sequences_and_work() {
    let kernel = RelationKernel::new();
    create_relations(&kernel, &[(300, "Edge", 2), (301, "Reachable", 2)]);
    kernel
        .install_rule(
            Rule::new(
                rel(301),
                [var("from"), var("to")],
                [Atom::positive(rel(300), [var("from"), var("to")])],
            ),
            "Reachable(from, to) :- Edge(from, to)",
        )
        .unwrap();
    kernel
        .install_rule(
            Rule::new(
                rel(301),
                [var("from"), var("to")],
                [
                    Atom::positive(rel(301), [var("from"), var("middle")]),
                    Atom::positive(rel(300), [var("middle"), var("to")]),
                ],
            ),
            "Reachable(from, to) :- Reachable(from, middle), Edge(middle, to)",
        )
        .unwrap();

    let mut oracle = CompleteOracle::new([(rel(301), 2)]);
    let commit = assert_rows(&kernel, rel(300), &[(1, 2), (2, 3), (3, 1)]);
    oracle.capture(&kernel, &commit);
    let commit = retract_rows(&kernel, rel(300), &[(3, 1)]);
    oracle.capture(&kernel, &commit);

    assert_eq!(oracle.epochs.len(), 2);
    assert_eq!(oracle.epochs[0].input_changes, 3);
    assert_eq!(oracle.epochs[0].affected_rule_components, 1);
    assert!(oracle.epochs[0].candidate_changes >= 9);
    assert_eq!(oracle.epochs[0].consolidated_changes, 9);
    assert_eq!(oracle.epochs[0].visible_output_changes, 9);
    assert!(oracle.epochs[0].fixpoint_iterations >= 2);
    assert!(!oracle.epochs[0].frontier_rows.is_empty());
    assert_eq!(oracle.epochs[1].input_changes, 1);
    assert_eq!(oracle.epochs[1].visible_output_changes, 6);
    assert_eq!(
        oracle.epochs[1].visible[&rel(301)],
        vec![
            Tuple::from([int(1), int(2)]),
            Tuple::from([int(1), int(3)]),
            Tuple::from([int(2), int(3)]),
        ]
    );
}

#[test]
fn complete_oracle_captures_multi_proof_zero_crossings() {
    let kernel = RelationKernel::new();
    create_relations(
        &kernel,
        &[
            (310, "Left", 2),
            (311, "Right", 2),
            (312, "Joined", 2),
            (313, "Unrelated", 1),
        ],
    );
    kernel
        .install_rule(
            Rule::new(
                rel(312),
                [var("from"), var("to")],
                [
                    Atom::positive(rel(310), [var("from"), var("middle")]),
                    Atom::positive(rel(311), [var("middle"), var("to")]),
                ],
            ),
            "Joined(from, to) :- Left(from, middle), Right(middle, to)",
        )
        .unwrap();
    assert_rows(&kernel, rel(310), &[(1, 2), (1, 3)]);

    let mut oracle = CompleteOracle::new([(rel(312), 2)]);
    let commit = assert_rows(&kernel, rel(311), &[(2, 4), (3, 4)]);
    oracle.capture(&kernel, &commit);
    let commit = retract_rows(&kernel, rel(311), &[(2, 4)]);
    oracle.capture(&kernel, &commit);
    let commit = retract_rows(&kernel, rel(311), &[(3, 4)]);
    oracle.capture(&kernel, &commit);
    let mut tx = kernel.begin();
    tx.assert(rel(313), Tuple::from([int(9)])).unwrap();
    let commit = tx.commit().unwrap().commit().clone();
    oracle.capture(&kernel, &commit);

    assert_eq!(oracle.epochs[0].visible_output_changes, 1);
    assert_eq!(oracle.epochs[0].candidate_changes, 1);
    assert_eq!(oracle.epochs[1].visible_output_changes, 0);
    assert_eq!(oracle.epochs[2].visible_output_changes, 1);
    assert!(oracle.epochs[2].visible[&rel(312)].is_empty());
    assert_eq!(oracle.epochs[3].affected_rule_components, 0);
    assert_eq!(oracle.epochs[3].visible_output_changes, 0);
}

#[test]
fn complete_oracle_captures_negation_changes_from_both_sides() {
    let kernel = RelationKernel::new();
    create_relations(
        &kernel,
        &[(320, "Node", 1), (321, "Blocked", 1), (322, "Visible", 1)],
    );
    kernel
        .install_rule(
            Rule::new(
                rel(322),
                [var("node")],
                [
                    RuleBodyItem::from(Atom::positive(rel(320), [var("node")])),
                    RuleBodyItem::from(Atom::negated(rel(321), [var("node")])),
                ],
            ),
            "Visible(node) :- Node(node), !Blocked(node)",
        )
        .unwrap();

    let mut oracle = CompleteOracle::new([(rel(322), 1)]);
    let mut tx = kernel.begin();
    tx.assert(rel(320), Tuple::from([int(1)])).unwrap();
    tx.assert(rel(320), Tuple::from([int(2)])).unwrap();
    let commit = tx.commit().unwrap().commit().clone();
    oracle.capture(&kernel, &commit);
    let mut tx = kernel.begin();
    tx.assert(rel(321), Tuple::from([int(2)])).unwrap();
    let commit = tx.commit().unwrap().commit().clone();
    oracle.capture(&kernel, &commit);
    let mut tx = kernel.begin();
    tx.retract(rel(321), Tuple::from([int(2)])).unwrap();
    let commit = tx.commit().unwrap().commit().clone();
    oracle.capture(&kernel, &commit);

    assert_eq!(
        oracle
            .epochs
            .iter()
            .map(|epoch| epoch.visible_output_changes)
            .collect::<Vec<_>>(),
        vec![2, 1, 1]
    );
    assert!(
        oracle
            .epochs
            .iter()
            .all(|epoch| epoch.fixpoint_iterations == 0)
    );
}

#[test]
fn complete_oracle_distinguishes_extensional_and_derived_visibility() {
    let kernel = RelationKernel::new();
    create_relations(&kernel, &[(330, "Source", 2), (331, "Combined", 2)]);
    kernel
        .install_rule(
            Rule::new(
                rel(331),
                [var("left"), var("right")],
                [Atom::positive(rel(330), [var("left"), var("right")])],
            ),
            "Combined(left, right) :- Source(left, right)",
        )
        .unwrap();

    let mut oracle = CompleteOracle::new([(rel(331), 2)]);
    let mut tx = kernel.begin();
    tx.assert(rel(330), Tuple::from([int(1), int(2)])).unwrap();
    tx.assert(rel(331), Tuple::from([int(1), int(2)])).unwrap();
    let commit = tx.commit().unwrap().commit().clone();
    oracle.capture(&kernel, &commit);
    let commit = retract_rows(&kernel, rel(330), &[(1, 2)]);
    oracle.capture(&kernel, &commit);
    let commit = retract_rows(&kernel, rel(331), &[(1, 2)]);
    oracle.capture(&kernel, &commit);

    assert_eq!(oracle.epochs[0].visible_output_changes, 1);
    assert_eq!(oracle.epochs[1].visible_output_changes, 0);
    assert!(
        oracle.epochs[1]
            .derived
            .get(&rel(331))
            .is_none_or(Vec::is_empty)
    );
    assert_eq!(oracle.epochs[2].visible_output_changes, 1);
    assert!(oracle.epochs[2].visible[&rel(331)].is_empty());
    assert_eq!(oracle.epochs[2].input_changes, 1);
    assert_eq!(oracle.epochs[2].affected_rule_components, 1);
    assert_eq!(oracle.epochs[2].candidate_changes, 0);
    assert_eq!(oracle.epochs[2].consolidated_changes, 0);
    assert_eq!(oracle.epochs[2].fixpoint_iterations, 0);
    assert_eq!(oracle.epochs[2].frontier_rows, Vec::<usize>::new());
    let _measured_elapsed = oracle.epochs[2].elapsed;
    assert_eq!(oracle.epochs[2].version, commit.version());
    assert_eq!(commit.changes()[0].kind, FactChangeKind::Retract);
}
