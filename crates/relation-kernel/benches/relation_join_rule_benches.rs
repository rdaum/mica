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

use mica_relation_kernel::{
    Atom, KernelError, PreparedQuery, QueryPlan, RelationId, RelationKernel, RelationMetadata,
    RelationRead, Rule, RuleSet, ScanControl, Snapshot, Term, Tuple, metrics,
};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, DiagnosticError, DiagnosticResult, MetricValue, Throughput,
    benchmark_main, black_box,
};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

const ROOMS: usize = 96;
const ITEMS_PER_ROOM: usize = 64;
const ACTORS: usize = 24;
const ROOMS_PER_ACTOR: usize = 8;
const HIDDEN_EVERY: usize = 17;
const UNARY_SET_SIZE: usize = 16_384;
const UNARY_SET_OVERLAP: usize = 8_192;
const TEMP_PROJECTED_GROUPS: usize = 4096;
const TEMP_PROJECTED_ITEMS_PER_GROUP: usize = 4;
const RECURSIVE_CHAIN_NODES: usize = 48;
const RECURSIVE_BRANCHING_NODES: usize = 63;
const RECURSIVE_CYCLE_NODES: usize = 32;

struct JoinRuleContext {
    snapshot: Arc<Snapshot>,
    query: PreparedQuery,
    rules: RuleSet,
}

struct NaturalJoinContext {
    snapshot: Arc<Snapshot>,
    join_query: PreparedQuery,
    semi_query: PreparedQuery,
    union_query: PreparedQuery,
    difference_query: PreparedQuery,
}

struct TemporaryProjectedJoinContext {
    snapshot: Arc<Snapshot>,
    query: PreparedQuery,
}

struct CountingReader {
    snapshot: Arc<Snapshot>,
    scan_calls: Cell<u64>,
    estimate_calls: Cell<u64>,
    visit_calls: Cell<u64>,
    rows_returned: Cell<u64>,
}

struct RecursiveCase {
    reader: CountingReader,
    rules: RuleSet,
}

struct RecursiveRuleContext {
    linear_chain: RecursiveCase,
    branching_graph: RecursiveCase,
    cyclic_graph: RecursiveCase,
    mutual_recursion: RecursiveCase,
    multiple_recursive_atoms: RecursiveCase,
    extensional_target: RecursiveCase,
    within: RecursiveCase,
    reachable_room: RecursiveCase,
}

struct RuleMetricSnapshot {
    fixpoint_evaluations: u64,
    rounds: u64,
    rule_evaluations: u64,
    variant_evaluations: u64,
    candidate_rows: u64,
    novel_rows: u64,
    frontier_rows: u64,
}

impl RuleMetricSnapshot {
    fn capture() -> Self {
        let metrics = metrics::metrics();
        Self {
            fixpoint_evaluations: metrics.rule_fixpoint_evaluations.sum() as u64,
            rounds: metrics.rule_fixpoint_rounds.sum(),
            rule_evaluations: metrics.rule_evaluations.sum(),
            variant_evaluations: metrics.rule_variant_evaluations.sum(),
            candidate_rows: metrics.rule_candidate_rows.sum(),
            novel_rows: metrics.rule_novel_rows.sum(),
            frontier_rows: metrics.rule_frontier_rows.sum(),
        }
    }
}

impl CountingReader {
    fn new(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            scan_calls: Cell::new(0),
            estimate_calls: Cell::new(0),
            visit_calls: Cell::new(0),
            rows_returned: Cell::new(0),
        }
    }

    fn reset(&self) {
        self.scan_calls.set(0);
        self.estimate_calls.set(0);
        self.visit_calls.set(0);
        self.rows_returned.set(0);
    }
}

impl RelationRead for CountingReader {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.scan_calls.set(self.scan_calls.get() + 1);
        let rows = self.snapshot.scan_relation(relation, bindings)?;
        self.rows_returned
            .set(self.rows_returned.get() + rows.len() as u64);
        Ok(rows)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.visit_calls.set(self.visit_calls.get() + 1);
        self.snapshot
            .visit_relation(relation, bindings, &mut |tuple| {
                self.rows_returned.set(self.rows_returned.get() + 1);
                visitor(tuple)
            })
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.estimate_calls.set(self.estimate_calls.get() + 1);
        self.snapshot.estimate_relation_scan(relation, bindings)
    }
}

impl BenchContext for JoinRuleContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = build_kernel();
        let mut tx = kernel.begin();
        seed_world(&mut tx);
        tx.commit().unwrap();
        Self {
            snapshot: kernel.snapshot(),
            query: visible_items_query(actor(0)).prepare(),
            rules: visible_items_rules(),
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl BenchContext for NaturalJoinContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                active_item(),
                Symbol::intern("ActiveItem"),
                1,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                visible_item(),
                Symbol::intern("VisibleItem"),
                1,
            ))
            .unwrap();

        let mut tx = kernel.begin();
        for index in 0..UNARY_SET_SIZE {
            tx.assert(
                active_item(),
                Tuple::from([Value::identity(natural_join_item(index))]),
            )
            .unwrap();
        }
        for index in (UNARY_SET_SIZE - UNARY_SET_OVERLAP)..(UNARY_SET_SIZE * 2 - UNARY_SET_OVERLAP)
        {
            tx.assert(
                visible_item(),
                Tuple::from([Value::identity(natural_join_item(index))]),
            )
            .unwrap();
        }
        tx.commit().unwrap();

        let snapshot = kernel.snapshot();
        snapshot
            .export_relation_batch(active_item(), &[None])
            .unwrap();
        snapshot
            .export_relation_batch(visible_item(), &[None])
            .unwrap();
        Self {
            snapshot,
            join_query: QueryPlan::join_eq(
                QueryPlan::scan(active_item(), [None]),
                QueryPlan::scan(visible_item(), [None]),
                [0],
                [0],
            )
            .project([0])
            .prepare(),
            semi_query: QueryPlan::semi_join(
                QueryPlan::scan(active_item(), [None]),
                QueryPlan::scan(visible_item(), [None]),
                [0],
                [0],
            )
            .prepare(),
            union_query: QueryPlan::union(
                QueryPlan::scan(active_item(), [None]),
                QueryPlan::scan(visible_item(), [None]),
            )
            .prepare(),
            difference_query: QueryPlan::difference(
                QueryPlan::scan(active_item(), [None]),
                QueryPlan::scan(visible_item(), [None]),
            )
            .prepare(),
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl BenchContext for TemporaryProjectedJoinContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                temp_projected_left(),
                Symbol::intern("TempProjectedLeft"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                temp_projected_right(),
                Symbol::intern("TempProjectedRight"),
                2,
            ))
            .unwrap();

        let mut tx = kernel.begin();
        for group in 0..TEMP_PROJECTED_GROUPS {
            for item in 0..TEMP_PROJECTED_ITEMS_PER_GROUP {
                tx.assert(
                    temp_projected_left(),
                    Tuple::from([
                        Value::identity(temp_projected_group(group)),
                        Value::identity(temp_projected_left_item(group, item)),
                    ]),
                )
                .unwrap();
                tx.assert(
                    temp_projected_right(),
                    Tuple::from([
                        Value::identity(temp_projected_group(group)),
                        Value::identity(temp_projected_right_item(group, item)),
                    ]),
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();

        let left = QueryPlan::scan(temp_projected_left(), [None, None]).project([0, 1]);
        let right = QueryPlan::scan(temp_projected_right(), [None, None]).project([0, 1]);

        Self {
            snapshot: kernel.snapshot(),
            query: QueryPlan::join_eq(left, right, [0], [0]).prepare(),
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl BenchContext for RecursiveRuleContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            linear_chain: build_linear_chain_case(),
            branching_graph: build_branching_graph_case(),
            cyclic_graph: build_cyclic_graph_case(),
            mutual_recursion: build_mutual_recursion_case(),
            multiple_recursive_atoms: build_multiple_recursive_atoms_case(),
            extensional_target: build_extensional_target_case(),
            within: build_within_case(),
            reachable_room: build_reachable_room_case(),
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

fn query_visible_items(ctx: &mut JoinRuleContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.query.execute(&*ctx.snapshot).unwrap());
    }
}

fn query_natural_unary_intersection(
    ctx: &mut NaturalJoinContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.join_query.execute(&*ctx.snapshot).unwrap());
    }
}

fn query_natural_unary_semi_intersection(
    ctx: &mut NaturalJoinContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.semi_query.execute(&*ctx.snapshot).unwrap());
    }
}

fn query_natural_unary_union(ctx: &mut NaturalJoinContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.union_query.execute(&*ctx.snapshot).unwrap());
    }
}

fn query_natural_unary_difference(
    ctx: &mut NaturalJoinContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.difference_query.execute(&*ctx.snapshot).unwrap());
    }
}

fn query_temporary_projected_low_cardinality_join(
    ctx: &mut TemporaryProjectedJoinContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.query.execute(&*ctx.snapshot).unwrap());
    }
}

fn rule_visible_items(ctx: &mut JoinRuleContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.rules.evaluate(&*ctx.snapshot).unwrap());
    }
}

fn evaluate_recursive_case(case: &RecursiveCase, chunk_size: usize) {
    for _ in 0..chunk_size {
        black_box(case.rules.evaluate_fixpoint(&case.reader).unwrap());
    }
}

fn rule_recursive_linear_chain(
    ctx: &mut RecursiveRuleContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    evaluate_recursive_case(&ctx.linear_chain, chunk_size);
}

fn rule_recursive_branching_graph(
    ctx: &mut RecursiveRuleContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    evaluate_recursive_case(&ctx.branching_graph, chunk_size);
}

fn rule_recursive_cyclic_graph(
    ctx: &mut RecursiveRuleContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    evaluate_recursive_case(&ctx.cyclic_graph, chunk_size);
}

fn rule_mutual_recursion(ctx: &mut RecursiveRuleContext, chunk_size: usize, _chunk_num: usize) {
    evaluate_recursive_case(&ctx.mutual_recursion, chunk_size);
}

fn rule_multiple_recursive_atoms(
    ctx: &mut RecursiveRuleContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    evaluate_recursive_case(&ctx.multiple_recursive_atoms, chunk_size);
}

fn rule_extensional_recursive_target(
    ctx: &mut RecursiveRuleContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    evaluate_recursive_case(&ctx.extensional_target, chunk_size);
}

fn rule_within(ctx: &mut RecursiveRuleContext, chunk_size: usize, _chunk_num: usize) {
    evaluate_recursive_case(&ctx.within, chunk_size);
}

fn rule_reachable_room(ctx: &mut RecursiveRuleContext, chunk_size: usize, _chunk_num: usize) {
    evaluate_recursive_case(&ctx.reachable_room, chunk_size);
}

fn recursive_case_diagnostics(case: &RecursiveCase) -> Result<DiagnosticResult, DiagnosticError> {
    case.reader.reset();
    let before = RuleMetricSnapshot::capture();
    let derived = case
        .rules
        .evaluate_fixpoint(&case.reader)
        .map_err(|error| DiagnosticError::new(format!("recursive evaluation failed: {error:?}")))?;
    let after = RuleMetricSnapshot::capture();
    let output_rows = derived.values().map(Vec::len).sum::<usize>() as i64;
    Ok(DiagnosticResult::new("rule work")
        .push_metric(MetricValue::integer(
            "relation_scan_calls",
            case.reader.scan_calls.get() as i64,
            "calls",
        ))
        .push_metric(MetricValue::integer(
            "relation_estimate_calls",
            case.reader.estimate_calls.get() as i64,
            "calls",
        ))
        .push_metric(MetricValue::integer(
            "relation_visit_calls",
            case.reader.visit_calls.get() as i64,
            "calls",
        ))
        .push_metric(MetricValue::integer(
            "base_rows_returned",
            case.reader.rows_returned.get() as i64,
            "rows",
        ))
        .push_metric(MetricValue::integer(
            "derived_output_rows",
            output_rows,
            "rows",
        ))
        .push_metric(MetricValue::integer(
            "fixpoint_evaluations",
            (after.fixpoint_evaluations - before.fixpoint_evaluations) as i64,
            "evaluations",
        ))
        .push_metric(MetricValue::integer(
            "fixpoint_rounds",
            (after.rounds - before.rounds) as i64,
            "rounds",
        ))
        .push_metric(MetricValue::integer(
            "rule_evaluations",
            (after.rule_evaluations - before.rule_evaluations) as i64,
            "evaluations",
        ))
        .push_metric(MetricValue::integer(
            "variant_evaluations",
            (after.variant_evaluations - before.variant_evaluations) as i64,
            "evaluations",
        ))
        .push_metric(MetricValue::integer(
            "candidate_rows",
            (after.candidate_rows - before.candidate_rows) as i64,
            "rows",
        ))
        .push_metric(MetricValue::integer(
            "novel_rows",
            (after.novel_rows - before.novel_rows) as i64,
            "rows",
        ))
        .push_metric(MetricValue::integer(
            "frontier_rows",
            (after.frontier_rows - before.frontier_rows) as i64,
            "rows",
        )))
}

fn recursive_linear_chain_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.linear_chain)
}

fn recursive_branching_graph_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.branching_graph)
}

fn recursive_cyclic_graph_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.cyclic_graph)
}

fn mutual_recursion_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.mutual_recursion)
}

fn multiple_recursive_atoms_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.multiple_recursive_atoms)
}

fn extensional_target_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.extensional_target)
}

fn within_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.within)
}

fn reachable_room_diagnostics(
    ctx: &mut RecursiveRuleContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    recursive_case_diagnostics(&ctx.reachable_room)
}

fn build_kernel() -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(located_in(), Symbol::intern("LocatedIn"), 2).with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(can_see_room(), Symbol::intern("CanSeeRoom"), 2)
                .with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            portable(),
            Symbol::intern("Portable"),
            1,
        ))
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(hidden_from(), Symbol::intern("HiddenFrom"), 2)
                .with_index([1, 0]),
        )
        .unwrap();
    kernel
}

fn seed_world(tx: &mut mica_relation_kernel::Transaction<'_>) {
    for room in 0..ROOMS {
        for item in 0..ITEMS_PER_ROOM {
            let item_id = item_identity(room, item);
            tx.assert(
                located_in(),
                Tuple::from([
                    Value::identity(item_id),
                    Value::identity(room_identity(room)),
                ]),
            )
            .unwrap();
            tx.assert(portable(), Tuple::from([Value::identity(item_id)]))
                .unwrap();
            if item % HIDDEN_EVERY == 0 {
                tx.assert(
                    hidden_from(),
                    Tuple::from([
                        Value::identity(item_id),
                        Value::identity(actor(room % ACTORS)),
                    ]),
                )
                .unwrap();
            }
        }
    }

    for actor_index in 0..ACTORS {
        for offset in 0..ROOMS_PER_ACTOR {
            let room = (actor_index * ROOMS_PER_ACTOR + offset) % ROOMS;
            tx.assert(
                can_see_room(),
                Tuple::from([
                    Value::identity(actor(actor_index)),
                    Value::identity(room_identity(room)),
                ]),
            )
            .unwrap();
        }
    }
}

fn visible_items_query(actor_id: Identity) -> QueryPlan {
    let rooms_seen = QueryPlan::scan(can_see_room(), [Some(Value::identity(actor_id)), None]);
    let items_in_seen_rooms = QueryPlan::join_eq(
        rooms_seen,
        QueryPlan::scan(located_in(), [None, None]),
        [1],
        [1],
    );
    let portable_items = QueryPlan::join_eq(
        items_in_seen_rooms,
        QueryPlan::scan(portable(), [None]),
        [2],
        [0],
    );
    QueryPlan::anti_join(
        portable_items,
        QueryPlan::scan(hidden_from(), [None, Some(Value::identity(actor_id))]),
        [2],
        [0],
    )
    .project([2])
}

fn visible_items_rules() -> RuleSet {
    RuleSet::new([Rule::new(
        visible(),
        [var("actor"), var("item")],
        [
            Atom::positive(located_in(), [var("item"), var("room")]),
            Atom::positive(can_see_room(), [var("actor"), var("room")]),
            Atom::positive(portable(), [var("item")]),
            Atom::negated(hidden_from(), [var("item"), var("actor")]),
        ],
    )])
}

fn recursive_kernel(relations: &[(RelationId, &'static str, u16)]) -> RelationKernel {
    let kernel = RelationKernel::new();
    for &(relation, name, arity) in relations {
        kernel
            .create_relation(RelationMetadata::new(relation, Symbol::intern(name), arity))
            .unwrap();
    }
    kernel
}

fn finish_recursive_case(kernel: RelationKernel, rules: RuleSet) -> RecursiveCase {
    RecursiveCase {
        reader: CountingReader::new(kernel.snapshot()),
        rules,
    }
}

fn transitive_rules(edge: RelationId, reachable: RelationId) -> RuleSet {
    RuleSet::new([
        Rule::new(
            reachable,
            [var("from"), var("to")],
            [Atom::positive(edge, [var("from"), var("to")])],
        ),
        Rule::new(
            reachable,
            [var("from"), var("to")],
            [
                Atom::positive(reachable, [var("from"), var("mid")]),
                Atom::positive(edge, [var("mid"), var("to")]),
            ],
        ),
    ])
}

fn build_linear_chain_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[(recursive_edge(), "RecursiveEdge", 2)]);
    let mut tx = kernel.begin();
    for index in 0..RECURSIVE_CHAIN_NODES - 1 {
        tx.assert(
            recursive_edge(),
            Tuple::from([node_value(index), node_value(index + 1)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    finish_recursive_case(
        kernel,
        transitive_rules(recursive_edge(), recursive_reachable()),
    )
}

fn build_branching_graph_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[(recursive_edge(), "RecursiveEdge", 2)]);
    let mut tx = kernel.begin();
    for child in 1..RECURSIVE_BRANCHING_NODES {
        tx.assert(
            recursive_edge(),
            Tuple::from([node_value((child - 1) / 2), node_value(child)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    finish_recursive_case(
        kernel,
        transitive_rules(recursive_edge(), recursive_reachable()),
    )
}

fn build_cyclic_graph_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[(recursive_edge(), "RecursiveEdge", 2)]);
    let mut tx = kernel.begin();
    for index in 0..RECURSIVE_CYCLE_NODES {
        tx.assert(
            recursive_edge(),
            Tuple::from([
                node_value(index),
                node_value((index + 1) % RECURSIVE_CYCLE_NODES),
            ]),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    finish_recursive_case(
        kernel,
        transitive_rules(recursive_edge(), recursive_reachable()),
    )
}

fn build_mutual_recursion_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[
        (recursive_seed(), "RecursiveSeed", 1),
        (recursive_next(), "RecursiveNext", 2),
    ]);
    let mut tx = kernel.begin();
    tx.assert(recursive_seed(), Tuple::from([node_value(0)]))
        .unwrap();
    for index in 0..RECURSIVE_CHAIN_NODES - 1 {
        tx.assert(
            recursive_next(),
            Tuple::from([node_value(index), node_value(index + 1)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let rules = RuleSet::new([
        Rule::new(
            recursive_left(),
            [var("node")],
            [Atom::positive(recursive_seed(), [var("node")])],
        ),
        Rule::new(
            recursive_right(),
            [var("node")],
            [Atom::positive(recursive_left(), [var("node")])],
        ),
        Rule::new(
            recursive_left(),
            [var("next")],
            [
                Atom::positive(recursive_right(), [var("node")]),
                Atom::positive(recursive_next(), [var("node"), var("next")]),
            ],
        ),
    ]);
    finish_recursive_case(kernel, rules)
}

fn build_multiple_recursive_atoms_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[(recursive_edge(), "RecursiveEdge", 2)]);
    let mut tx = kernel.begin();
    for index in 0..20 {
        tx.assert(
            recursive_edge(),
            Tuple::from([node_value(index), node_value(index + 1)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let rules = RuleSet::new([
        Rule::new(
            recursive_reachable(),
            [var("from"), var("to")],
            [Atom::positive(recursive_edge(), [var("from"), var("to")])],
        ),
        Rule::new(
            recursive_reachable(),
            [var("from"), var("to")],
            [
                Atom::positive(recursive_reachable(), [var("from"), var("middle")]),
                Atom::positive(recursive_reachable(), [var("middle"), var("to")]),
            ],
        ),
    ]);
    finish_recursive_case(kernel, rules)
}

fn build_extensional_target_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[
        (recursive_edge(), "RecursiveEdge", 2),
        (recursive_reachable(), "RecursiveReachable", 2),
    ]);
    let mut tx = kernel.begin();
    tx.assert(
        recursive_reachable(),
        Tuple::from([node_value(0), node_value(1)]),
    )
    .unwrap();
    for index in 1..RECURSIVE_CHAIN_NODES - 1 {
        tx.assert(
            recursive_edge(),
            Tuple::from([node_value(index), node_value(index + 1)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    finish_recursive_case(
        kernel,
        RuleSet::new([Rule::new(
            recursive_reachable(),
            [var("from"), var("to")],
            [
                Atom::positive(recursive_reachable(), [var("from"), var("middle")]),
                Atom::positive(recursive_edge(), [var("middle"), var("to")]),
            ],
        )]),
    )
}

fn build_within_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[(contained_in(), "ContainedIn", 2)]);
    let mut tx = kernel.begin();
    for child in 1..RECURSIVE_BRANCHING_NODES {
        tx.assert(
            contained_in(),
            Tuple::from([node_value(child), node_value((child - 1) / 2)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let rules = RuleSet::new([
        Rule::new(
            within(),
            [var("object"), var("container")],
            [Atom::positive(
                contained_in(),
                [var("object"), var("container")],
            )],
        ),
        Rule::new(
            within(),
            [var("object"), var("ancestor")],
            [
                Atom::positive(contained_in(), [var("object"), var("parent")]),
                Atom::positive(within(), [var("parent"), var("ancestor")]),
            ],
        ),
    ]);
    finish_recursive_case(kernel, rules)
}

fn build_reachable_room_case() -> RecursiveCase {
    let kernel = recursive_kernel(&[
        (can_enter_room(), "CanEnterRoom", 2),
        (room_exit(), "RoomExit", 2),
    ]);
    let mut tx = kernel.begin();
    tx.assert(
        can_enter_room(),
        Tuple::from([actor_value(0), node_value(0)]),
    )
    .unwrap();
    for index in 0..RECURSIVE_CHAIN_NODES - 1 {
        tx.assert(
            room_exit(),
            Tuple::from([node_value(index), node_value(index + 1)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let rules = RuleSet::new([
        Rule::new(
            reachable_room(),
            [var("actor"), var("room")],
            [Atom::positive(
                can_enter_room(),
                [var("actor"), var("room")],
            )],
        ),
        Rule::new(
            reachable_room(),
            [var("actor"), var("to")],
            [
                Atom::positive(reachable_room(), [var("actor"), var("from")]),
                Atom::positive(room_exit(), [var("from"), var("to")]),
            ],
        ),
    ]);
    finish_recursive_case(kernel, rules)
}

fn var(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

fn located_in() -> RelationId {
    relation(90)
}

fn can_see_room() -> RelationId {
    relation(91)
}

fn portable() -> RelationId {
    relation(92)
}

fn hidden_from() -> RelationId {
    relation(93)
}

fn visible() -> RelationId {
    relation(94)
}

fn active_item() -> RelationId {
    relation(95)
}

fn visible_item() -> RelationId {
    relation(96)
}

fn temp_projected_left() -> RelationId {
    relation(97)
}

fn temp_projected_right() -> RelationId {
    relation(98)
}

fn recursive_edge() -> RelationId {
    relation(110)
}

fn recursive_reachable() -> RelationId {
    relation(111)
}

fn recursive_seed() -> RelationId {
    relation(112)
}

fn recursive_next() -> RelationId {
    relation(113)
}

fn recursive_left() -> RelationId {
    relation(114)
}

fn recursive_right() -> RelationId {
    relation(115)
}

fn contained_in() -> RelationId {
    relation(116)
}

fn within() -> RelationId {
    relation(117)
}

fn can_enter_room() -> RelationId {
    relation(118)
}

fn room_exit() -> RelationId {
    relation(119)
}

fn reachable_room() -> RelationId {
    relation(120)
}

fn relation(raw: u64) -> Identity {
    Identity::new(raw).unwrap()
}

fn actor(index: usize) -> Identity {
    relation(1_000 + index as u64)
}

fn room_identity(index: usize) -> Identity {
    relation(10_000 + index as u64)
}

fn item_identity(room: usize, item: usize) -> Identity {
    relation(100_000 + (room * ITEMS_PER_ROOM + item) as u64)
}

fn natural_join_item(index: usize) -> Identity {
    relation(200_000 + index as u64)
}

fn temp_projected_group(index: usize) -> Identity {
    relation(300_000 + index as u64)
}

fn temp_projected_left_item(group: usize, item: usize) -> Identity {
    relation(400_000 + (group * TEMP_PROJECTED_ITEMS_PER_GROUP + item) as u64)
}

fn temp_projected_right_item(group: usize, item: usize) -> Identity {
    relation(500_000 + (group * TEMP_PROJECTED_ITEMS_PER_GROUP + item) as u64)
}

fn node_value(index: usize) -> Value {
    Value::identity(relation(600_000 + index as u64))
}

fn actor_value(index: usize) -> Value {
    Value::identity(relation(700_000 + index as u64))
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some("all, query, rule, or any benchmark name substring".to_string()),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<JoinRuleContext>("query", |g| {
            g.throughput(Throughput::per_operation(1, "query"))
                .bench("visible_items_query_3way_anti", query_visible_items);
        });

        runner.group::<NaturalJoinContext>("query", |g| {
            g.throughput(Throughput::per_operation(1, "query")).bench(
                "natural_unary_set_intersection",
                query_natural_unary_intersection,
            );
            g.throughput(Throughput::per_operation(1, "query")).bench(
                "natural_unary_semi_intersection",
                query_natural_unary_semi_intersection,
            );
            g.throughput(Throughput::per_operation(1, "query"))
                .bench("natural_unary_union", query_natural_unary_union);
            g.throughput(Throughput::per_operation(1, "query"))
                .bench("natural_unary_difference", query_natural_unary_difference);
        });

        runner.group::<TemporaryProjectedJoinContext>("query", |g| {
            g.throughput(Throughput::per_operation(1, "query")).bench(
                "temporary_projected_low_cardinality_join",
                query_temporary_projected_low_cardinality_join,
            );
        });

        runner.group::<JoinRuleContext>("rule", |g| {
            g.throughput(Throughput::per_operation(1, "rule_eval"))
                .bench("visible_items_rule_join_order", rule_visible_items);
        });

        runner.group::<RecursiveRuleContext>("recursive", |g| {
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(recursive_linear_chain_diagnostics)
                .bench("recursive_linear_chain", rule_recursive_linear_chain);
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(recursive_branching_graph_diagnostics)
                .bench("recursive_branching_graph", rule_recursive_branching_graph);
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(recursive_cyclic_graph_diagnostics)
                .bench("recursive_cyclic_graph", rule_recursive_cyclic_graph);
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(mutual_recursion_diagnostics)
                .bench("mutual_recursion", rule_mutual_recursion);
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(multiple_recursive_atoms_diagnostics)
                .bench(
                    "multiple_recursive_body_atoms",
                    rule_multiple_recursive_atoms,
                );
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(extensional_target_diagnostics)
                .bench(
                    "extensional_recursive_target",
                    rule_extensional_recursive_target,
                );
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(within_diagnostics)
                .bench("within_containment", rule_within);
            g.throughput(Throughput::per_operation(1, "fixpoint"))
                .diagnostic_pass(reachable_room_diagnostics)
                .bench("reachable_room", rule_reachable_room);
        });
    }
);
