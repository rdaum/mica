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

use mica_relation_kernel::relation_algebra;
use mica_var::{RelationValue, Symbol, Tuple, Value};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, Throughput, benchmark_main, black_box,
};
use std::time::Duration;

const UNIQUE_ROWS: usize = 8_192;
const LOW_CARDINALITY_GROUPS: usize = 2_048;
const ROWS_PER_GROUP: usize = 4;
const CONCURRENT_THREADS: usize = 4;

#[derive(Clone, Copy)]
enum JoinShape {
    Disjoint,
    OneToOne,
    LowCardinality,
}

impl JoinShape {
    const ALL: [Self; 3] = [Self::Disjoint, Self::OneToOne, Self::LowCardinality];

    const fn name(self) -> &'static str {
        match self {
            Self::Disjoint => "disjoint",
            Self::OneToOne => "one_to_one",
            Self::LowCardinality => "low_cardinality",
        }
    }

    const fn input_rows_per_side(self) -> usize {
        match self {
            Self::Disjoint | Self::OneToOne => UNIQUE_ROWS,
            Self::LowCardinality => LOW_CARDINALITY_GROUPS * ROWS_PER_GROUP,
        }
    }

    const fn output_rows(self) -> usize {
        match self {
            Self::Disjoint => 0,
            Self::OneToOne => UNIQUE_ROWS,
            Self::LowCardinality => LOW_CARDINALITY_GROUPS * ROWS_PER_GROUP * ROWS_PER_GROUP,
        }
    }
}

#[derive(Clone, Copy)]
enum KeyKind {
    Immediate,
    Heap,
}

impl KeyKind {
    const ALL: [Self; 2] = [Self::Immediate, Self::Heap];

    const fn name(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Heap => "heap",
        }
    }
}

struct JoinContext {
    left: RelationValue,
    right: RelationValue,
    expected_output_rows: usize,
    input_rows: u64,
}

impl JoinContext {
    fn new(shape: JoinShape, key_kind: KeyKind) -> Self {
        let join_column = Symbol::intern("relation-value-bench-join");
        let left_column = Symbol::intern("relation-value-bench-left");
        let right_column = Symbol::intern("relation-value-bench-right");
        let rows_per_side = shape.input_rows_per_side();

        let mut left_rows = Vec::with_capacity(rows_per_side);
        let mut right_rows = Vec::with_capacity(rows_per_side);
        match shape {
            JoinShape::Disjoint => {
                for index in 0..rows_per_side {
                    left_rows.push(row(key(key_kind, index), index));
                    right_rows.push(row(key(key_kind, index + rows_per_side), index));
                }
            }
            JoinShape::OneToOne => {
                for index in 0..rows_per_side {
                    let key = key(key_kind, index);
                    left_rows.push(row(key.clone(), index));
                    right_rows.push(row(key, index));
                }
            }
            JoinShape::LowCardinality => {
                for group in 0..LOW_CARDINALITY_GROUPS {
                    let key = key(key_kind, group);
                    for item in 0..ROWS_PER_GROUP {
                        let payload = group * ROWS_PER_GROUP + item;
                        left_rows.push(row(key.clone(), payload));
                        right_rows.push(row(key.clone(), payload));
                    }
                }
            }
        }

        let left = RelationValue::new([join_column, left_column], left_rows).unwrap();
        let right = RelationValue::new([join_column, right_column], right_rows).unwrap();
        Self {
            left,
            right,
            expected_output_rows: shape.output_rows(),
            input_rows: (rows_per_side * 2) as u64,
        }
    }

    fn join(&self) {
        let result = relation_algebra::natural_join(&self.left, &self.right).unwrap();
        debug_assert_eq!(result.len(), self.expected_output_rows);
        black_box(result);
    }
}

impl BenchContext for JoinContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::new(JoinShape::Disjoint, KeyKind::Immediate)
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl ConcurrentBenchContext for JoinContext {
    fn prepare(_num_threads: usize) -> Self {
        Self::new(JoinShape::Disjoint, KeyKind::Immediate)
    }
}

fn key(kind: KeyKind, index: usize) -> Value {
    match kind {
        KeyKind::Immediate => Value::int(index as i64).unwrap(),
        KeyKind::Heap => Value::string(format!("relation-value-key-{index}")),
    }
}

fn row(key: Value, payload: usize) -> Tuple {
    Tuple::from([key, Value::int(payload as i64).unwrap()])
}

fn run_join(context: &mut JoinContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        context.join();
    }
}

fn run_concurrent_joins(
    context: &JoinContext,
    control: &ConcurrentBenchControl,
) -> ConcurrentWorkerResult {
    let mut joins = 0_u64;
    while !control.should_stop() {
        context.join();
        joins = joins.wrapping_add(1);
    }
    ConcurrentWorkerResult::operations(joins.wrapping_mul(context.input_rows))
        .with_counter("joins", joins)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some("all or any benchmark name substring".to_string()),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<JoinContext>("relation value join", |group| {
            for shape in JoinShape::ALL {
                for key_kind in KeyKind::ALL {
                    let context = move || JoinContext::new(shape, key_kind);
                    let name = format!("natural_join_{}_{}_keys", shape.name(), key_kind.name());
                    group
                        .throughput(Throughput::per_operation(
                            (shape.input_rows_per_side() * 2) as u64,
                            "input_rows",
                        ))
                        .factory(&context)
                        .bench(&name, run_join);
                }
            }
        });

        let one_worker = [ConcurrentWorker {
            name: "natural join",
            threads: 1,
            run: run_concurrent_joins,
        }];
        let four_workers = [ConcurrentWorker {
            name: "natural join",
            threads: CONCURRENT_THREADS,
            run: run_concurrent_joins,
        }];
        runner.concurrent_group::<JoinContext>("relation value join concurrent", |group| {
            for shape in JoinShape::ALL {
                for key_kind in KeyKind::ALL {
                    for (threads, workers) in [
                        (1, one_worker.as_slice()),
                        (CONCURRENT_THREADS, four_workers.as_slice()),
                    ] {
                        let context = move |_| JoinContext::new(shape, key_kind);
                        let name = format!(
                            "natural_join_{}_{}_keys_{}_threads",
                            shape.name(),
                            key_kind.name(),
                            threads
                        );
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(1, "input_rows"))
                            .metadata("shape", shape.name())
                            .metadata("key_kind", key_kind.name())
                            .metadata("threads", threads.to_string())
                            .factory(&context)
                            .bench(&name, workers);
                    }
                }
            }
        });
    }
);
