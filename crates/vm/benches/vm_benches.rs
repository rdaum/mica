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

mod fixtures;

use fixtures::{
    BUILTIN_CALL_INSTRUCTIONS, BenchmarkHost, INTEGER_LOOP_INSTRUCTIONS, MAX_CALL_DEPTH,
    ProgramFixture, STATIC_CALL_INSTRUCTIONS, builtin_call_fixture, integer_loop_fixture,
    static_call_fixture,
};
use mica_vm::{RegisterVm, VmHostResponse};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, NoContext, Throughput, benchmark_main, black_box,
};
use std::time::Duration;

const CONCURRENT_THREADS: usize = 4;

struct IntegerLoopContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
}

struct NativeIntegerLoopContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
}

impl BenchContext for NativeIntegerLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: integer_loop_fixture(),
            host: BenchmarkHost::default(),
        }
    }
}

struct ConcurrentIntegerLoopContext {
    fixture: ProgramFixture,
    native: bool,
}

impl ConcurrentBenchContext for ConcurrentIntegerLoopContext {
    fn prepare(_num_threads: usize) -> Self {
        Self {
            fixture: integer_loop_fixture(),
            native: true,
        }
    }
}

impl BenchContext for IntegerLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: integer_loop_fixture(),
            host: BenchmarkHost::default(),
        }
    }
}

struct StaticCallContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
}

impl BenchContext for StaticCallContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: static_call_fixture(),
            host: BenchmarkHost::default(),
        }
    }
}

struct BuiltinCallContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
}

impl BenchContext for BuiltinCallContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: builtin_call_fixture(),
            host: BenchmarkHost::default(),
        }
    }
}

fn execute_fixture_interpreted(
    fixture: &ProgramFixture,
    host: &mut BenchmarkHost,
) -> VmHostResponse {
    let mut vm = RegisterVm::new_interpreted(fixture.program.clone());
    vm.run_until_host_response(host, fixture.instruction_count as usize, MAX_CALL_DEPTH)
        .unwrap()
}

fn execute_fixture_native(fixture: &ProgramFixture, host: &mut BenchmarkHost) -> VmHostResponse {
    let mut vm = RegisterVm::new(fixture.program.clone());
    vm.run_until_host_response(host, fixture.instruction_count as usize, MAX_CALL_DEPTH)
        .unwrap()
}

fn interpreter_integer_loop(ctx: &mut IntegerLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture_interpreted(&ctx.fixture, &mut ctx.host));
    }
}

fn native_integer_loop(ctx: &mut NativeIntegerLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture_native(&ctx.fixture, &mut ctx.host));
    }
}

fn native_integer_loop_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let mut host = BenchmarkHost::default();
    for _ in 0..chunk_size {
        let fixture = integer_loop_fixture();
        black_box(execute_fixture_native(&fixture, &mut host));
    }
}

fn interpreter_static_calls(ctx: &mut StaticCallContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture_interpreted(&ctx.fixture, &mut ctx.host));
    }
}

fn interpreter_builtin_calls(ctx: &mut BuiltinCallContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture_interpreted(&ctx.fixture, &mut ctx.host));
    }
    black_box(ctx.host.builtin_calls());
}

fn run_concurrent_integer_loops(
    context: &ConcurrentIntegerLoopContext,
    control: &ConcurrentBenchControl,
) -> ConcurrentWorkerResult {
    let mut host = BenchmarkHost::default();
    let mut loops = 0_u64;
    while !control.should_stop() {
        let response = if context.native {
            execute_fixture_native(&context.fixture, &mut host)
        } else {
            execute_fixture_interpreted(&context.fixture, &mut host)
        };
        black_box(response);
        loops = loops.wrapping_add(1);
    }
    ConcurrentWorkerResult::operations(loops.wrapping_mul(INTEGER_LOOP_INSTRUCTIONS))
        .with_counter("loops", loops)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, integer, call, builtin, or any benchmark name substring".to_string()
        ),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 10,
            max_samples: 30,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<IntegerLoopContext>("integer", |g| {
            g.throughput(Throughput::per_operation(
                INTEGER_LOOP_INSTRUCTIONS,
                "bytecode_instruction",
            ))
            .bench("interpreter_integer_loop", interpreter_integer_loop);
        });

        runner.group::<NativeIntegerLoopContext>("integer", |g| {
            g.throughput(Throughput::per_operation(
                INTEGER_LOOP_INSTRUCTIONS,
                "bytecode_instruction",
            ))
            .bench("cranelift_integer_loop", native_integer_loop);
        });

        runner.group::<NoContext>("integer cold", |g| {
            g.throughput(Throughput::per_operation(1, "task"))
                .bench("cranelift_integer_loop_cold", native_integer_loop_cold);
        });

        runner.group::<StaticCallContext>("call", |g| {
            g.throughput(Throughput::per_operation(
                STATIC_CALL_INSTRUCTIONS,
                "bytecode_instruction",
            ))
            .bench("interpreter_static_calls", interpreter_static_calls);
        });

        runner.group::<BuiltinCallContext>("builtin", |g| {
            g.throughput(Throughput::per_operation(
                BUILTIN_CALL_INSTRUCTIONS,
                "bytecode_instruction",
            ))
            .bench("interpreter_builtin_calls", interpreter_builtin_calls);
        });

        let one_thread = [ConcurrentWorker {
            name: "integer loop",
            threads: 1,
            run: run_concurrent_integer_loops,
        }];
        let four_threads = [ConcurrentWorker {
            name: "integer loop",
            threads: CONCURRENT_THREADS,
            run: run_concurrent_integer_loops,
        }];
        let interpreted = |_| ConcurrentIntegerLoopContext {
            fixture: integer_loop_fixture(),
            native: false,
        };
        let native = |_| ConcurrentIntegerLoopContext {
            fixture: integer_loop_fixture(),
            native: true,
        };
        runner.concurrent_group::<ConcurrentIntegerLoopContext>("integer concurrent", |group| {
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                .metadata("backend", "interpreter")
                .metadata("threads", "1")
                .factory(&interpreted)
                .bench("interpreter_integer_loop_1_thread", &one_thread);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                .metadata("backend", "interpreter")
                .metadata("threads", CONCURRENT_THREADS.to_string())
                .factory(&interpreted)
                .bench("interpreter_integer_loop_4_threads", &four_threads);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                .metadata("backend", "cranelift")
                .metadata("threads", "1")
                .factory(&native)
                .bench("cranelift_integer_loop_1_thread", &one_thread);
            group
                .sample_duration(Duration::from_millis(50))
                .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                .metadata("backend", "cranelift")
                .metadata("threads", CONCURRENT_THREADS.to_string())
                .factory(&native)
                .bench("cranelift_integer_loop_4_threads", &four_threads);
        });
    }
);
