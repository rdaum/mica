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

use mica_var::Value;
use mica_vm_cranelift::{CompiledIntegerLoop, IntegerLoopOutcome};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, DiagnosticError, DiagnosticResult, MetricValue,
    NoContext, Throughput, benchmark_main, black_box,
};
use std::time::Duration;

const INTEGER_LOOP_ITERATIONS: u64 = 16_384;
const CONCURRENT_THREADS: usize = 4;

enum LoopBackend {
    Rust,
    Cranelift(Box<CompiledIntegerLoop>),
}

struct ValueLoopContext {
    backend: LoopBackend,
    start: Value,
    step: Value,
    limit: Value,
}

impl ValueLoopContext {
    fn rust() -> Self {
        Self {
            backend: LoopBackend::Rust,
            start: Value::int(0).unwrap(),
            step: Value::int(1).unwrap(),
            limit: Value::int(INTEGER_LOOP_ITERATIONS as i64).unwrap(),
        }
    }

    fn cranelift() -> Self {
        Self {
            backend: LoopBackend::Cranelift(Box::new(CompiledIntegerLoop::compile().unwrap())),
            start: Value::int(0).unwrap(),
            step: Value::int(1).unwrap(),
            limit: Value::int(INTEGER_LOOP_ITERATIONS as i64).unwrap(),
        }
    }

    fn run_once(&self) -> Value {
        match &self.backend {
            LoopBackend::Rust => interpreted_integer_loop(
                black_box(&self.start),
                black_box(&self.step),
                black_box(&self.limit),
            ),
            LoopBackend::Cranelift(compiled) => {
                let IntegerLoopOutcome::Complete {
                    current,
                    iterations,
                    ..
                } = compiled.run(
                    black_box(&self.start),
                    black_box(&self.step),
                    black_box(&self.limit),
                    INTEGER_LOOP_ITERATIONS,
                )
                else {
                    panic!("integer benchmark left the generated fast path");
                };
                debug_assert_eq!(iterations, INTEGER_LOOP_ITERATIONS);
                current
            }
        }
    }
}

impl BenchContext for ValueLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::rust()
    }
}

impl ConcurrentBenchContext for ValueLoopContext {
    fn prepare(_num_threads: usize) -> Self {
        Self::cranelift()
    }
}

fn interpreted_integer_loop(start: &Value, step: &Value, limit: &Value) -> Value {
    let mut current = start.clone();
    while &current < limit {
        current = current.checked_add(step).unwrap();
    }
    current
}

fn run_value_loops(context: &mut ValueLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(context.run_once());
    }
}

fn compile_integer_loops(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let compiled = CompiledIntegerLoop::compile().unwrap();
        black_box((compiled.code_size(), compiled.imported_helper_count()));
    }
}

fn generated_code_diagnostics(
    context: &mut ValueLoopContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    let LoopBackend::Cranelift(compiled) = &context.backend else {
        return Err(DiagnosticError::new(
            "generated-code diagnostics require a Cranelift context",
        ));
    };
    Ok(DiagnosticResult::new("generated code")
        .push_metric(MetricValue::integer(
            "value_abi_version",
            i64::from(compiled.value_abi_version()),
            "version",
        ))
        .push_metric(MetricValue::integer(
            "code_size",
            compiled.code_size() as i64,
            "bytes",
        ))
        .push_metric(MetricValue::integer(
            "imported_helpers",
            compiled.imported_helper_count() as i64,
            "helpers",
        )))
}

fn run_concurrent_value_loops(
    context: &ValueLoopContext,
    control: &ConcurrentBenchControl,
) -> ConcurrentWorkerResult {
    let mut loops = 0_u64;
    while !control.should_stop() {
        black_box(context.run_once());
        loops = loops.wrapping_add(1);
    }
    ConcurrentWorkerResult::operations(loops.wrapping_mul(INTEGER_LOOP_ITERATIONS))
        .with_counter("loops", loops)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, rust, cranelift, compile, concurrent, or a benchmark name substring".to_owned(),
        ),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(250),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 10,
            max_samples: 30,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<ValueLoopContext>("value codegen", |group| {
            let rust = || ValueLoopContext::rust();
            group
                .throughput(Throughput::per_operation(
                    INTEGER_LOOP_ITERATIONS,
                    "additions",
                ))
                .factory(&rust)
                .bench("rust_value_integer_loop", run_value_loops);

            let cranelift = || ValueLoopContext::cranelift();
            group
                .throughput(Throughput::per_operation(
                    INTEGER_LOOP_ITERATIONS,
                    "additions",
                ))
                .factory(&cranelift)
                .diagnostic_pass(generated_code_diagnostics)
                .bench("cranelift_value_integer_loop", run_value_loops);
        });

        runner.group::<NoContext>("value codegen compile", |group| {
            group
                .throughput(Throughput::per_operation(1, "compilation"))
                .bench("cranelift_compile_integer_loop", compile_integer_loops);
        });

        let one_thread = [ConcurrentWorker {
            name: "value loop",
            threads: 1,
            run: run_concurrent_value_loops,
        }];
        let four_threads = [ConcurrentWorker {
            name: "value loop",
            threads: CONCURRENT_THREADS,
            run: run_concurrent_value_loops,
        }];
        let rust = |_| ValueLoopContext::rust();
        let cranelift = |_| ValueLoopContext::cranelift();
        runner.concurrent_group::<ValueLoopContext>("value codegen concurrent", |group| {
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "additions"))
                .metadata("backend", "rust")
                .metadata("threads", "1")
                .factory(&rust)
                .bench("rust_value_integer_loop_1_thread", &one_thread);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "additions"))
                .metadata("backend", "rust")
                .metadata("threads", CONCURRENT_THREADS.to_string())
                .factory(&rust)
                .bench("rust_value_integer_loop_4_threads", &four_threads);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "additions"))
                .metadata("backend", "cranelift")
                .metadata("threads", "1")
                .metadata("side_exits", "0")
                .factory(&cranelift)
                .bench("cranelift_value_integer_loop_1_thread", &one_thread);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "additions"))
                .metadata("backend", "cranelift")
                .metadata("threads", CONCURRENT_THREADS.to_string())
                .metadata("side_exits", "0")
                .factory(&cranelift)
                .bench("cranelift_value_integer_loop_4_threads", &four_threads);
        });
    }
);
