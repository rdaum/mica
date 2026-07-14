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
    Commit, CommitProvider, RelationDurability, RelationKernel, RelationMetadata, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, Throughput, benchmark_main, black_box,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const CONCURRENT_THREADS: usize = 4;
const FIRST_VALUE: u64 = 1_000_000;

#[derive(Clone, Copy)]
enum StoragePath {
    Durable,
    Volatile,
}

impl StoragePath {
    const ALL: [Self; 2] = [Self::Durable, Self::Volatile];

    const fn name(self) -> &'static str {
        match self {
            Self::Durable => "durable_transaction_lifecycle",
            Self::Volatile => "volatile_transaction_lifecycle",
        }
    }
}

#[derive(Default)]
struct CountingProvider {
    commits: AtomicU64,
    changes: AtomicU64,
}

impl CommitProvider for CountingProvider {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String> {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.changes
            .fetch_add(commit.changes().len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

struct DurabilityBenchContext {
    kernel: Arc<RelationKernel>,
    metadata: RelationMetadata,
    next_value: AtomicU64,
}

impl DurabilityBenchContext {
    fn new(path: StoragePath) -> Self {
        let relation = Identity::new(1).unwrap();
        let durability = match path {
            StoragePath::Volatile => RelationDurability::Volatile,
            StoragePath::Durable => RelationDurability::Durable,
        };
        let metadata =
            RelationMetadata::new(relation, Symbol::intern("Probe"), 1).with_durability(durability);
        let kernel = RelationKernel::with_provider(Arc::new(CountingProvider::default()));
        kernel.create_relation(metadata.clone()).unwrap();
        Self {
            kernel: Arc::new(kernel),
            metadata,
            next_value: AtomicU64::new(FIRST_VALUE),
        }
    }

    fn run_one(&self) {
        let value = Value::identity(
            Identity::new(self.next_value.fetch_add(1, Ordering::Relaxed))
                .expect("benchmark values stay within the Mica identity range"),
        );
        let tuple = Tuple::from([value.clone()]);
        let mut assert = self.kernel.begin();
        assert.assert(self.metadata.id(), tuple.clone()).unwrap();
        let assert_result = assert.commit().unwrap();
        debug_assert_eq!(
            assert_result
                .snapshot()
                .scan(self.metadata.id(), &[Some(value.clone())])
                .unwrap()
                .len(),
            1
        );
        let mut retract = self.kernel.begin();
        retract.retract(self.metadata.id(), tuple).unwrap();
        let retract_result = retract.commit().unwrap();
        debug_assert!(
            retract_result
                .snapshot()
                .scan(self.metadata.id(), &[Some(value)])
                .unwrap()
                .is_empty()
        );
        black_box((assert_result, retract_result));
    }
}

impl BenchContext for DurabilityBenchContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::new(StoragePath::Durable)
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl ConcurrentBenchContext for DurabilityBenchContext {
    fn prepare(_num_threads: usize) -> Self {
        Self::new(StoragePath::Durable)
    }
}

fn run_serial(context: &mut DurabilityBenchContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        context.run_one();
    }
}

fn run_concurrent(
    context: &DurabilityBenchContext,
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
        runner.group::<DurabilityBenchContext>("relation durability", |group| {
            for path in StoragePath::ALL {
                let factory = move || DurabilityBenchContext::new(path);
                group
                    .throughput(Throughput::per_operation(2, "tuple_mutations"))
                    .factory(&factory)
                    .bench(path.name(), run_serial);
            }
        });

        let one_worker = [ConcurrentWorker {
            name: "relation lifecycle",
            threads: 1,
            run: run_concurrent,
        }];
        let four_workers = [ConcurrentWorker {
            name: "relation lifecycle",
            threads: CONCURRENT_THREADS,
            run: run_concurrent,
        }];
        runner.concurrent_group::<DurabilityBenchContext>(
            "relation durability concurrent",
            |group| {
                for path in StoragePath::ALL {
                    for (threads, workers) in [
                        (1, one_worker.as_slice()),
                        (CONCURRENT_THREADS, four_workers.as_slice()),
                    ] {
                        let factory = move |_| DurabilityBenchContext::new(path);
                        let name = format!("{}_{}_threads", path.name(), threads);
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(2, "tuple_mutations"))
                            .metadata("storage", path.name())
                            .metadata("threads", threads.to_string())
                            .factory(&factory)
                            .bench(&name, workers);
                    }
                }
            },
        );
    }
);
