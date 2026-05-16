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

use clap::Parser;
use mica_relation_kernel::{
    Atom, KernelError, QueryPlan, RelationId, RelationKernel, RelationMetadata, RelationRead, Rule,
    RuleSet, ScanControl, Snapshot, Term, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 2_000)]
    left_rows: usize,
    #[arg(long, default_value_t = 2_000)]
    right_rows: usize,
    #[arg(long, default_value_t = 2_000)]
    third_rows: usize,
    #[arg(long, default_value_t = 128)]
    key_cardinality: usize,
    #[arg(long, default_value_t = 100)]
    iterations: usize,
}

fn main() -> Result<(), KernelError> {
    let args = Args::parse();
    let snapshot = build_snapshot(&args)?;
    let materialized_reader = MaterializedSnapshotReader {
        snapshot: snapshot.clone(),
    };

    let binary = QueryPlan::join_eq(
        QueryPlan::scan(rel(10), [None, None]),
        QueryPlan::scan(rel(11), [None, None]),
        [1],
        [0],
    );
    let multiway = QueryPlan::join_eq(
        binary.clone(),
        QueryPlan::scan(rel(12), [None, None]),
        [3],
        [0],
    );
    let rule_set = RuleSet::new([Rule::new(
        rel(13),
        [var("left"), var("payload")],
        [
            Atom::positive(rel(10), [var("left"), var("key")]),
            Atom::positive(rel(11), [var("key"), var("payload")]),
            Atom::positive(rel(12), [var("payload"), var("tag")]),
        ],
    )]);

    let binary_rows = binary.execute(snapshot.as_ref())?.len();
    let multiway_rows = multiway.execute(snapshot.as_ref())?.len();
    let rule_rows = rule_set
        .evaluate(snapshot.as_ref())
        .map_err(KernelError::from)?[&rel(13)]
        .len();
    println!(
        "shape: left_rows={} right_rows={} third_rows={} key_cardinality={} iterations={}",
        args.left_rows, args.right_rows, args.third_rows, args.key_cardinality, args.iterations
    );
    println!("binary_rows={binary_rows} multiway_rows={multiway_rows} rule_rows={rule_rows}");

    run_case(
        "binary/direct-index",
        &binary,
        snapshot.as_ref(),
        args.iterations,
    )?;
    run_case(
        "binary/materialized",
        &binary,
        &materialized_reader,
        args.iterations,
    )?;
    run_case(
        "multiway/direct-index",
        &multiway,
        snapshot.as_ref(),
        args.iterations,
    )?;
    run_case(
        "multiway/materialized",
        &multiway,
        &materialized_reader,
        args.iterations,
    )?;
    run_rule_case(
        "rule/current",
        &rule_set,
        snapshot.as_ref(),
        args.iterations,
    )?;

    Ok(())
}

fn run_case(
    name: &str,
    plan: &QueryPlan,
    reader: &impl RelationRead,
    iterations: usize,
) -> Result<(), KernelError> {
    let warmup_rows = plan.execute(reader)?.len();
    black_box(warmup_rows);

    let started = Instant::now();
    let mut rows = 0usize;
    for _ in 0..iterations {
        rows = rows.wrapping_add(black_box(plan.execute(reader)?.len()));
    }
    let elapsed = started.elapsed();
    println!(
        "{name}: total={} avg={} rows_sum={}",
        format_duration(elapsed),
        format_duration(elapsed / iterations as u32),
        rows
    );
    Ok(())
}

fn run_rule_case(
    name: &str,
    rule_set: &RuleSet,
    reader: &impl RelationRead,
    iterations: usize,
) -> Result<(), KernelError> {
    let warmup_rows = rule_set.evaluate(reader).map_err(KernelError::from)?[&rel(13)].len();
    black_box(warmup_rows);

    let started = Instant::now();
    let mut rows = 0usize;
    for _ in 0..iterations {
        rows = rows.wrapping_add(black_box(
            rule_set.evaluate(reader).map_err(KernelError::from)?[&rel(13)].len(),
        ));
    }
    let elapsed = started.elapsed();
    println!(
        "{name}: total={} avg={} rows_sum={}",
        format_duration(elapsed),
        format_duration(elapsed / iterations as u32),
        rows
    );
    Ok(())
}

fn build_snapshot(args: &Args) -> Result<Arc<Snapshot>, KernelError> {
    let kernel = RelationKernel::new();
    kernel.create_relation(
        RelationMetadata::new(rel(10), Symbol::intern("JoinLeft"), 2).with_index([1]),
    )?;
    kernel.create_relation(
        RelationMetadata::new(rel(11), Symbol::intern("JoinRight"), 2).with_index([0]),
    )?;
    kernel.create_relation(
        RelationMetadata::new(rel(12), Symbol::intern("JoinThird"), 2).with_index([0]),
    )?;

    let mut tx = kernel.begin();
    for row in 0..args.left_rows {
        tx.assert(
            rel(10),
            Tuple::from([int(row as i64), int((row % args.key_cardinality) as i64)]),
        )?;
    }
    for row in 0..args.right_rows {
        tx.assert(
            rel(11),
            Tuple::from([
                int((row % args.key_cardinality) as i64),
                int((row % args.third_rows.max(1)) as i64),
            ]),
        )?;
    }
    for row in 0..args.third_rows {
        tx.assert(
            rel(12),
            Tuple::from([int(row as i64), int((row % args.key_cardinality) as i64)]),
        )?;
    }
    Ok(tx.commit()?.into_snapshot())
}

struct MaterializedSnapshotReader {
    snapshot: Arc<Snapshot>,
}

impl RelationRead for MaterializedSnapshotReader {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.snapshot.scan(relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.snapshot.visit(relation, bindings, visitor)
    }

    fn estimate_relation_scan(
        &self,
        _relation: RelationId,
        _bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        Ok(None)
    }
}

fn rel(id: u64) -> RelationId {
    Identity::new(id).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

fn var(name: &str) -> Term {
    Term::Var(Symbol::intern(name))
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        return format!("{duration:.3?}");
    }
    if duration.as_micros() > 0 {
        return format!("{}us", duration.as_micros());
    }
    format!("{}ns", duration.as_nanos())
}
