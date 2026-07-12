// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

//! Measures a two-thread packed union prototype under concurrent Mica VM load.

use clap::Parser;
use mica_relation_kernel::{
    PackedRelation, QueryPlan, RelationId, RelationKernel, RelationMetadata, RelationRead,
    Snapshot, Tuple,
};
use mica_runtime::{AuthorityContext, SharedSourceRunner, SourceRunner, TaskInput, TaskRequest};
use mica_var::{Identity, Symbol, Value};
use std::cmp::Ordering;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const ENDPOINT_START: u64 = 0x00ee_3000_0000_0000;

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long, default_value_t = 2)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 8)]
    mica_workers: usize,
    #[arg(long, default_value_t = 2)]
    operator_workers: usize,
    #[arg(long, default_value_t = 200)]
    mica_loop_iterations: i64,
    #[arg(long, default_value_t = 16_384)]
    relation_rows: usize,
}

#[derive(Clone, Copy, Debug)]
enum OperatorMode {
    None,
    Serial,
    Parallel,
}

#[derive(Clone, Copy, Debug)]
struct ResultRow {
    mode: OperatorMode,
    mica_tasks: u64,
    operator_queries: u64,
    elapsed: Duration,
}

struct UnionWorkload {
    snapshot: Arc<Snapshot>,
    query: QueryPlan,
    left: Arc<PackedRelation>,
    right: Arc<PackedRelation>,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.duration_seconds == 0
        || args.mica_workers == 0
        || args.operator_workers == 0
        || args.relation_rows == 0
    {
        return Err("durations, worker counts, and relation rows must be non-zero".to_owned());
    }

    let (runner, actor) = build_runner()?;
    let union = build_union(args.relation_rows)?;
    black_box(
        union
            .query
            .execute(union.snapshot.as_ref())
            .map_err(debug_error)?,
    );
    black_box(parallel_union_rows(union.left.rows(), union.right.rows()));

    let baseline = run_mode(
        &args,
        OperatorMode::None,
        &runner,
        actor,
        union.snapshot.as_ref(),
        &union.query,
        &union.left,
        &union.right,
    )?;
    let serial = run_mode(
        &args,
        OperatorMode::Serial,
        &runner,
        actor,
        union.snapshot.as_ref(),
        &union.query,
        &union.left,
        &union.right,
    )?;
    let parallel = run_mode(
        &args,
        OperatorMode::Parallel,
        &runner,
        actor,
        union.snapshot.as_ref(),
        &union.query,
        &union.left,
        &union.right,
    )?;

    for result in [baseline, serial, parallel] {
        println!(
            "mode={:?} mica_tasks_per_second={:.2} operator_queries_per_second={:.2} mica_tasks={} operator_queries={}",
            result.mode,
            result.mica_tasks as f64 / result.elapsed.as_secs_f64(),
            result.operator_queries as f64 / result.elapsed.as_secs_f64(),
            result.mica_tasks,
            result.operator_queries,
        );
    }
    let serial_mica = serial.mica_tasks as f64 / serial.elapsed.as_secs_f64();
    let parallel_mica = parallel.mica_tasks as f64 / parallel.elapsed.as_secs_f64();
    let serial_operator = serial.operator_queries as f64 / serial.elapsed.as_secs_f64();
    let parallel_operator = parallel.operator_queries as f64 / parallel.elapsed.as_secs_f64();
    let mica_change = parallel_mica / serial_mica - 1.0;
    let operator_change = parallel_operator / serial_operator - 1.0;
    println!(
        "parallel_vs_serial_mica_throughput={:+.2}%",
        mica_change * 100.0
    );
    println!(
        "parallel_vs_serial_operator_throughput={:+.2}%",
        operator_change * 100.0
    );
    println!(
        "placement={}",
        if mica_change >= 0.0 && operator_change > 0.0 {
            "candidate"
        } else {
            "rejected"
        }
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_mode(
    args: &Args,
    mode: OperatorMode,
    runner: &Arc<SharedSourceRunner>,
    actor: Identity,
    snapshot: &Snapshot,
    union: &QueryPlan,
    left: &PackedRelation,
    right: &PackedRelation,
) -> Result<ResultRow, String> {
    let operator_workers = match mode {
        OperatorMode::None => 0,
        OperatorMode::Serial | OperatorMode::Parallel => args.operator_workers,
    };
    let barrier = Arc::new(Barrier::new(args.mica_workers + operator_workers + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let mica_tasks = Arc::new(AtomicU64::new(0));
    let operator_queries = Arc::new(AtomicU64::new(0));
    let elapsed = thread::scope(|scope| -> Result<Duration, String> {
        for worker in 0..args.mica_workers {
            let barrier = barrier.clone();
            let stop = stop.clone();
            let mica_tasks = mica_tasks.clone();
            let runner = runner.clone();
            scope.spawn(move || {
                barrier.wait();
                while !stop.load(AtomicOrdering::Relaxed) {
                    runner
                        .submit_invocation(TaskRequest {
                            principal: None,
                            actor: None,
                            endpoint: Identity::new(ENDPOINT_START + worker as u64).unwrap(),
                            authority: AuthorityContext::root(),
                            input: TaskInput::Invocation {
                                selector: Symbol::intern("count_up"),
                                roles: vec![
                                    (Symbol::intern("actor"), Value::identity(actor)),
                                    (
                                        Symbol::intern("count"),
                                        Value::int(args.mica_loop_iterations).unwrap(),
                                    ),
                                ],
                            },
                        })
                        .unwrap();
                    mica_tasks.fetch_add(1, AtomicOrdering::Relaxed);
                }
            });
        }
        for _ in 0..operator_workers {
            let barrier = barrier.clone();
            let stop = stop.clone();
            let operator_queries = operator_queries.clone();
            scope.spawn(move || {
                barrier.wait();
                while !stop.load(AtomicOrdering::Relaxed) {
                    match mode {
                        OperatorMode::None => unreachable!(),
                        OperatorMode::Serial => {
                            black_box(union.execute(snapshot).unwrap());
                        }
                        OperatorMode::Parallel => {
                            black_box(parallel_union_rows(left.rows(), right.rows()));
                        }
                    }
                    operator_queries.fetch_add(1, AtomicOrdering::Relaxed);
                }
            });
        }
        barrier.wait();
        let started = Instant::now();
        thread::sleep(Duration::from_secs(args.duration_seconds));
        stop.store(true, AtomicOrdering::Relaxed);
        Ok(started.elapsed())
    })?;
    Ok(ResultRow {
        mode,
        mica_tasks: mica_tasks.load(AtomicOrdering::Relaxed),
        operator_queries: operator_queries.load(AtomicOrdering::Relaxed),
        elapsed,
    })
}

fn build_runner() -> Result<(Arc<SharedSourceRunner>, Identity), String> {
    let mut runner = SourceRunner::new_empty();
    runner
        .run_filein(
            "make_identity(:player)\n\
             make_identity(:alice)\n\
             make_relation(:Delegates, 3)\n\
             assert Delegates(#alice, #player, 0)\n\
             verb count_up(actor @ #player, count)\n\
               let i = 0\n\
               while i < count\n\
                 i = i + 1\n\
               end\n\
               return i\n\
             end\n",
        )
        .map_err(debug_error)?;
    let actor = runner
        .named_identity(Symbol::intern("alice"))
        .map_err(debug_error)?;
    Ok((Arc::new(runner.into_shared()), actor))
}

fn build_union(rows: usize) -> Result<UnionWorkload, String> {
    let kernel = RelationKernel::new();
    for (relation, name) in [(rel(10), "Left"), (rel(11), "Right")] {
        kernel
            .create_relation(RelationMetadata::new(relation, Symbol::intern(name), 1))
            .map_err(debug_error)?;
    }
    let mut tx = kernel.begin();
    for row in 0..rows {
        tx.assert(rel(10), Tuple::from([int(row as i64)]))
            .map_err(debug_error)?;
        tx.assert(rel(11), Tuple::from([int((row + rows / 2) as i64)]))
            .map_err(debug_error)?;
    }
    let snapshot = tx.commit().map_err(debug_error)?.into_snapshot();
    let left = snapshot
        .export_relation_batch(rel(10), &[None])
        .map_err(debug_error)?
        .unwrap();
    let right = snapshot
        .export_relation_batch(rel(11), &[None])
        .map_err(debug_error)?
        .unwrap();
    let union = QueryPlan::union(
        QueryPlan::scan(rel(10), [None]),
        QueryPlan::scan(rel(11), [None]),
    );
    Ok(UnionWorkload {
        snapshot,
        query: union,
        left,
        right,
    })
}

fn merge_tuple_slices(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
    let mut output = Vec::with_capacity(left.len() + right.len());
    let (mut left_row, mut right_row) = (0usize, 0usize);
    while left_row < left.len() || right_row < right.len() {
        match (left.get(left_row), right.get(right_row)) {
            (Some(left), Some(right)) => match left.cmp(right) {
                Ordering::Less => {
                    output.push(left.clone());
                    left_row += 1;
                }
                Ordering::Equal => {
                    output.push(left.clone());
                    left_row += 1;
                    right_row += 1;
                }
                Ordering::Greater => {
                    output.push(right.clone());
                    right_row += 1;
                }
            },
            (Some(left), None) => {
                output.push(left.clone());
                left_row += 1;
            }
            (None, Some(right)) => {
                output.push(right.clone());
                right_row += 1;
            }
            (None, None) => break,
        }
    }
    output
}

fn parallel_union_rows(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
    let pivot = match (left.get(left.len() / 2), right.get(right.len() / 2)) {
        (Some(left), Some(right)) => left.max(right),
        (Some(left), None) => left,
        (None, Some(right)) => right,
        (None, None) => return Vec::new(),
    };
    let left_split = left.partition_point(|tuple| tuple < pivot);
    let right_split = right.partition_point(|tuple| tuple < pivot);
    thread::scope(|scope| {
        let lower = scope.spawn(|| merge_tuple_slices(&left[..left_split], &right[..right_split]));
        let upper = scope.spawn(|| merge_tuple_slices(&left[left_split..], &right[right_split..]));
        let mut rows = lower.join().unwrap();
        rows.extend(upper.join().unwrap());
        rows
    })
}

fn rel(raw: u64) -> RelationId {
    Identity::new(raw).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
