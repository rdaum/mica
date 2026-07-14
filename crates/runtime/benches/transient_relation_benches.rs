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

use mica_relation_kernel::Tuple;
use mica_runtime::{SharedSourceRunner, SourceRunner};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, Throughput, benchmark_main, black_box,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const CONCURRENT_THREADS: usize = 4;
const FIRST_BENCH_IDENTITY: u64 = 1_000_000_000;

#[derive(Clone, Copy)]
enum Workload {
    Tuple,
    Endpoint,
    Request,
}

impl Workload {
    const ALL: [Self; 3] = [Self::Tuple, Self::Endpoint, Self::Request];

    const fn name(self) -> &'static str {
        match self {
            Self::Tuple => "transient_tuple_lifecycle",
            Self::Endpoint => "transient_endpoint_lifecycle",
            Self::Request => "transient_request_lifecycle",
        }
    }

    const fn mutations(self) -> u64 {
        match self {
            Self::Tuple => 2,
            Self::Endpoint => 10,
            Self::Request => 28,
        }
    }
}

struct TransientBenchContext {
    runner: SharedSourceRunner,
    next_identity: AtomicU64,
    workload: Workload,
}

impl TransientBenchContext {
    fn new(workload: Workload) -> Self {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_source(
                "make_relation(:TransientProbe, 1)\n\
                 make_relation(:HttpRequest, 1)\n\
                 make_relation(:RequestMethod, 2)\n\
                 make_relation(:RequestPath, 2)\n\
                 make_relation(:RequestVersion, 2)\n\
                 make_relation(:RequestPrincipal, 2)\n\
                 make_relation(:RequestActor, 2)\n\
                 make_relation(:RequestHeader, 3)\n\
                 make_relation(:RequestBody, 2)",
            )
            .expect("benchmark relation declarations should install");
        Self {
            runner: runner.into_shared(),
            next_identity: AtomicU64::new(FIRST_BENCH_IDENTITY),
            workload,
        }
    }

    fn next_identity(&self) -> Identity {
        Identity::new(self.next_identity.fetch_add(1, Ordering::Relaxed))
            .expect("benchmark identities stay within the Mica identity range")
    }

    fn run_one(&self) {
        match self.workload {
            Workload::Tuple => self.run_tuple_lifecycle(),
            Workload::Endpoint => self.run_endpoint_lifecycle(),
            Workload::Request => self.run_request_lifecycle(),
        }
    }

    fn run_tuple_lifecycle(&self) {
        let scope = self.next_identity();
        let tuple = Tuple::from([Value::identity(scope)]);
        let relation = Symbol::intern("TransientProbe");
        let inserted = self
            .runner
            .assert_transient_tuple_named(scope, relation, tuple.clone())
            .expect("transient tuple assertion should succeed");
        debug_assert!(inserted);
        let removed = self
            .runner
            .retract_transient_tuple_named(scope, relation, &tuple)
            .expect("transient tuple retraction should succeed");
        debug_assert!(removed);
        black_box((inserted, removed));
    }

    fn run_endpoint_lifecycle(&self) {
        let endpoint = self.next_identity();
        let principal = self.next_identity();
        let actor = self.next_identity();
        self.runner
            .open_endpoint_with_context(
                endpoint,
                Some(principal),
                Some(actor),
                Symbol::intern("benchmark"),
            )
            .expect("endpoint should open");
        let request = self
            .runner
            .source_request_for_endpoint(endpoint, "return actor()")
            .expect("endpoint context should resolve");
        let removed = self.runner.close_endpoint(endpoint);
        debug_assert_eq!(removed, 5);
        black_box((request, removed));
    }

    fn run_request_lifecycle(&self) {
        let endpoint = self.next_identity();
        let request = self.next_identity();
        let principal = self.next_identity();
        let actor = self.next_identity();
        self.runner
            .open_endpoint_with_context(
                endpoint,
                Some(principal),
                Some(actor),
                Symbol::intern("http-request"),
            )
            .expect("request endpoint should open");
        let request_value = Value::identity(request);
        let rows = vec![
            (
                Symbol::intern("HttpRequest"),
                Tuple::from([request_value.clone()]),
            ),
            (
                Symbol::intern("RequestMethod"),
                Tuple::from([request_value.clone(), Value::string("POST")]),
            ),
            (
                Symbol::intern("RequestPath"),
                Tuple::from([request_value.clone(), Value::string("/benchmark")]),
            ),
            (
                Symbol::intern("RequestVersion"),
                Tuple::from([request_value.clone(), Value::int(1).unwrap()]),
            ),
            (
                Symbol::intern("RequestPrincipal"),
                Tuple::from([request_value.clone(), Value::identity(principal)]),
            ),
            (
                Symbol::intern("RequestActor"),
                Tuple::from([request_value.clone(), Value::identity(actor)]),
            ),
            (
                Symbol::intern("RequestHeader"),
                Tuple::from([
                    request_value.clone(),
                    Value::string("content-type"),
                    Value::bytes(b"application/json"),
                ]),
            ),
            (
                Symbol::intern("RequestHeader"),
                Tuple::from([
                    request_value.clone(),
                    Value::string("accept"),
                    Value::bytes(b"application/json"),
                ]),
            ),
            (
                Symbol::intern("RequestBody"),
                Tuple::from([request_value, Value::bytes(br#"{"probe":true}"#)]),
            ),
        ];
        let inserted = self
            .runner
            .assert_transient_tuples_named(endpoint, rows)
            .expect("request facts should install");
        debug_assert_eq!(inserted, 9);
        let removed = self.runner.close_endpoint(endpoint);
        debug_assert_eq!(removed, 14);
        black_box((inserted, removed));
    }
}

impl BenchContext for TransientBenchContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::new(Workload::Tuple)
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl ConcurrentBenchContext for TransientBenchContext {
    fn prepare(_num_threads: usize) -> Self {
        Self::new(Workload::Tuple)
    }
}

fn run_serial(context: &mut TransientBenchContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        context.run_one();
    }
}

fn run_concurrent(
    context: &TransientBenchContext,
    control: &ConcurrentBenchControl,
) -> ConcurrentWorkerResult {
    let mut lifecycles = 0_u64;
    while !control.should_stop() {
        context.run_one();
        lifecycles = lifecycles.wrapping_add(1);
    }
    ConcurrentWorkerResult::operations(lifecycles)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some("all or any benchmark name substring".to_owned()),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<TransientBenchContext>("transient relation lifecycle", |group| {
            for workload in Workload::ALL {
                let factory = move || TransientBenchContext::new(workload);
                group
                    .throughput(Throughput::per_operation(
                        workload.mutations(),
                        "tuple_mutations",
                    ))
                    .factory(&factory)
                    .bench(workload.name(), run_serial);
            }
        });

        let one_worker = [ConcurrentWorker {
            name: "transient lifecycle",
            threads: 1,
            run: run_concurrent,
        }];
        let four_workers = [ConcurrentWorker {
            name: "transient lifecycle",
            threads: CONCURRENT_THREADS,
            run: run_concurrent,
        }];
        runner.concurrent_group::<TransientBenchContext>(
            "transient relation lifecycle concurrent",
            |group| {
                for workload in Workload::ALL {
                    for (threads, workers) in [
                        (1, one_worker.as_slice()),
                        (CONCURRENT_THREADS, four_workers.as_slice()),
                    ] {
                        let factory = move |_| TransientBenchContext::new(workload);
                        let name = format!("{}_{}_threads", workload.name(), threads);
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(
                                workload.mutations(),
                                "tuple_mutations",
                            ))
                            .metadata("workload", workload.name())
                            .metadata("threads", threads.to_string())
                            .factory(&factory)
                            .bench(&name, workers);
                    }
                }
            },
        );
    }
);
