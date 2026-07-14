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
    ALTERNATING_BRANCH_LOOP_INSTRUCTIONS, BUILTIN_CALL_INSTRUCTIONS, BenchmarkHost,
    INTEGER_LOOP_INSTRUCTIONS, MAX_CALL_DEPTH, NATURAL_FLOAT_SUM_INSTRUCTIONS,
    NATURAL_FLOAT_TRANSFORM_INSTRUCTIONS, NATURAL_MIXED_SCALE_INSTRUCTIONS,
    NATURAL_NUMERIC_DIV_REM_INSTRUCTIONS, PREDICTABLE_BRANCH_LOOP_INSTRUCTIONS, ProgramFixture,
    SCALAR_LOOP_INSTRUCTIONS, STATIC_CALL_INSTRUCTIONS, alternating_branch_loop_fixture,
    builtin_call_fixture, float_add_loop_fixture, float_multiply_loop_fixture,
    integer_loop_fixture, natural_exact_integer_division_fixture, natural_float_remainder_fixture,
    natural_float_sum_fixture, natural_float_transform_fixture,
    natural_fractional_integer_division_fixture, natural_mixed_division_fixture,
    natural_mixed_scale_fixture, predictable_branch_loop_fixture, scalar_symbol_loop_fixture,
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

struct FloatLoopContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
    native: bool,
}

struct MeasuredLoopContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
    native: bool,
}

impl BenchContext for FloatLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: float_add_loop_fixture(),
            host: BenchmarkHost::default(),
            native: false,
        }
    }
}

impl BenchContext for MeasuredLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: scalar_symbol_loop_fixture(),
            host: BenchmarkHost::default(),
            native: false,
        }
    }
}

impl BenchContext for NativeIntegerLoopContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self {
            fixture: integer_loop_fixture(),
            host: BenchmarkHost::default(),
        }
    }
}

struct ConcurrentLoopContext {
    fixture: ProgramFixture,
    native: bool,
    instruction_count: u64,
}

impl ConcurrentBenchContext for ConcurrentLoopContext {
    fn prepare(_num_threads: usize) -> Self {
        Self {
            fixture: integer_loop_fixture(),
            native: true,
            instruction_count: INTEGER_LOOP_INSTRUCTIONS,
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

fn native_float_add_loop_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let mut host = BenchmarkHost::default();
    for _ in 0..chunk_size {
        let fixture = float_add_loop_fixture();
        black_box(execute_fixture_native(&fixture, &mut host));
    }
}

fn native_float_multiply_loop_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let mut host = BenchmarkHost::default();
    for _ in 0..chunk_size {
        let fixture = float_multiply_loop_fixture();
        black_box(execute_fixture_native(&fixture, &mut host));
    }
}

fn interpreter_natural_float_sum_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    measured_loop_cold(chunk_size, false, natural_float_sum_fixture);
}

fn cranelift_natural_float_sum_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    measured_loop_cold(chunk_size, true, natural_float_sum_fixture);
}

fn interpreter_natural_float_transform_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, false, natural_float_transform_fixture);
}

fn cranelift_natural_float_transform_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, true, natural_float_transform_fixture);
}

fn interpreter_natural_mixed_scale_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, false, natural_mixed_scale_fixture);
}

fn cranelift_natural_mixed_scale_cold(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    measured_loop_cold(chunk_size, true, natural_mixed_scale_fixture);
}

fn interpreter_natural_exact_integer_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, false, natural_exact_integer_division_fixture);
}

fn cranelift_natural_exact_integer_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, true, natural_exact_integer_division_fixture);
}

fn interpreter_natural_fractional_integer_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(
        chunk_size,
        false,
        natural_fractional_integer_division_fixture,
    );
}

fn cranelift_natural_fractional_integer_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(
        chunk_size,
        true,
        natural_fractional_integer_division_fixture,
    );
}

fn interpreter_natural_mixed_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, false, natural_mixed_division_fixture);
}

fn cranelift_natural_mixed_division_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, true, natural_mixed_division_fixture);
}

fn interpreter_natural_float_remainder_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, false, natural_float_remainder_fixture);
}

fn cranelift_natural_float_remainder_cold(
    _ctx: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    measured_loop_cold(chunk_size, true, natural_float_remainder_fixture);
}

fn measured_loop_cold(chunk_size: usize, native: bool, fixture: fn() -> ProgramFixture) {
    let mut host = BenchmarkHost::default();
    for _ in 0..chunk_size {
        let fixture = fixture();
        let response = if native {
            execute_fixture_native(&fixture, &mut host)
        } else {
            execute_fixture_interpreted(&fixture, &mut host)
        };
        black_box(response);
    }
}

fn float_loop(ctx: &mut FloatLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let response = if ctx.native {
            execute_fixture_native(&ctx.fixture, &mut ctx.host)
        } else {
            execute_fixture_interpreted(&ctx.fixture, &mut ctx.host)
        };
        black_box(response);
    }
}

fn measured_loop(ctx: &mut MeasuredLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let response = if ctx.native {
            execute_fixture_native(&ctx.fixture, &mut ctx.host)
        } else {
            execute_fixture_interpreted(&ctx.fixture, &mut ctx.host)
        };
        black_box(response);
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
    context: &ConcurrentLoopContext,
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
    ConcurrentWorkerResult::operations(loops.wrapping_mul(context.instruction_count))
        .with_counter("loops", loops)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, integer, float, natural_numeric, scalar, branch, call, builtin, or any benchmark name substring"
                .to_string()
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

        runner.group::<FloatLoopContext>("float", |group| {
            for (interpreter_name, cranelift_name, fixture) in [
                (
                    "interpreter_float_add_loop",
                    "cranelift_float_add_loop",
                    float_add_loop_fixture as fn() -> ProgramFixture,
                ),
                (
                    "interpreter_float_multiply_loop",
                    "cranelift_float_multiply_loop",
                    float_multiply_loop_fixture as fn() -> ProgramFixture,
                ),
            ] {
                let instruction_count = fixture().instruction_count;
                let interpreted = move || FloatLoopContext {
                    fixture: fixture(),
                    host: BenchmarkHost::default(),
                    native: false,
                };
                group
                    .throughput(Throughput::per_operation(
                        instruction_count,
                        "bytecode_instruction",
                    ))
                    .factory(&interpreted)
                    .bench(interpreter_name, float_loop);

                let native = move || FloatLoopContext {
                    fixture: fixture(),
                    host: BenchmarkHost::default(),
                    native: true,
                };
                group
                    .throughput(Throughput::per_operation(
                        instruction_count,
                        "bytecode_instruction",
                    ))
                    .factory(&native)
                    .bench(cranelift_name, float_loop);
            }
        });

        runner.group::<NoContext>("integer cold", |g| {
            g.throughput(Throughput::per_operation(1, "task"))
                .bench("cranelift_integer_loop_cold", native_integer_loop_cold);
        });

        runner.group::<NoContext>("float cold", |group| {
            group
                .throughput(Throughput::per_operation(1, "task"))
                .bench("cranelift_float_add_loop_cold", native_float_add_loop_cold);
            group
                .throughput(Throughput::per_operation(1, "task"))
                .bench(
                    "cranelift_float_multiply_loop_cold",
                    native_float_multiply_loop_cold,
                );
        });

        runner.group::<MeasuredLoopContext>("natural numeric warm", |group| {
            for (fixture, instructions, workload_name) in [
                (
                    natural_float_sum_fixture as fn() -> ProgramFixture,
                    NATURAL_FLOAT_SUM_INSTRUCTIONS,
                    "float_sum",
                ),
                (
                    natural_float_transform_fixture as fn() -> ProgramFixture,
                    NATURAL_FLOAT_TRANSFORM_INSTRUCTIONS,
                    "float_transform",
                ),
                (
                    natural_mixed_scale_fixture as fn() -> ProgramFixture,
                    NATURAL_MIXED_SCALE_INSTRUCTIONS,
                    "mixed_scale",
                ),
                (
                    natural_exact_integer_division_fixture as fn() -> ProgramFixture,
                    NATURAL_NUMERIC_DIV_REM_INSTRUCTIONS,
                    "exact_integer_division",
                ),
                (
                    natural_fractional_integer_division_fixture as fn() -> ProgramFixture,
                    NATURAL_NUMERIC_DIV_REM_INSTRUCTIONS,
                    "fractional_integer_division",
                ),
                (
                    natural_mixed_division_fixture as fn() -> ProgramFixture,
                    NATURAL_NUMERIC_DIV_REM_INSTRUCTIONS,
                    "mixed_division",
                ),
                (
                    natural_float_remainder_fixture as fn() -> ProgramFixture,
                    NATURAL_NUMERIC_DIV_REM_INSTRUCTIONS,
                    "float_remainder",
                ),
            ] {
                for (backend, native) in [("interpreter", false), ("cranelift", true)] {
                    let factory = move || MeasuredLoopContext {
                        fixture: fixture(),
                        host: BenchmarkHost::default(),
                        native,
                    };
                    group
                        .throughput(Throughput::per_operation(
                            instructions,
                            "bytecode_instruction",
                        ))
                        .factory(&factory)
                        .bench(&format!("{backend}_natural_numeric_{workload_name}"), measured_loop);
                }
            }
        });

        runner.group::<NoContext>("natural numeric cold", |group| {
            for (name, bench) in [
                (
                    "interpreter_natural_numeric_float_sum_cold",
                    interpreter_natural_float_sum_cold as fn(&mut NoContext, usize, usize),
                ),
                (
                    "cranelift_natural_numeric_float_sum_cold",
                    cranelift_natural_float_sum_cold,
                ),
                (
                    "interpreter_natural_numeric_float_transform_cold",
                    interpreter_natural_float_transform_cold,
                ),
                (
                    "cranelift_natural_numeric_float_transform_cold",
                    cranelift_natural_float_transform_cold,
                ),
                (
                    "interpreter_natural_numeric_mixed_scale_cold",
                    interpreter_natural_mixed_scale_cold,
                ),
                (
                    "cranelift_natural_numeric_mixed_scale_cold",
                    cranelift_natural_mixed_scale_cold,
                ),
                (
                    "interpreter_natural_numeric_exact_integer_division_cold",
                    interpreter_natural_exact_integer_division_cold,
                ),
                (
                    "cranelift_natural_numeric_exact_integer_division_cold",
                    cranelift_natural_exact_integer_division_cold,
                ),
                (
                    "interpreter_natural_numeric_fractional_integer_division_cold",
                    interpreter_natural_fractional_integer_division_cold,
                ),
                (
                    "cranelift_natural_numeric_fractional_integer_division_cold",
                    cranelift_natural_fractional_integer_division_cold,
                ),
                (
                    "interpreter_natural_numeric_mixed_division_cold",
                    interpreter_natural_mixed_division_cold,
                ),
                (
                    "cranelift_natural_numeric_mixed_division_cold",
                    cranelift_natural_mixed_division_cold,
                ),
                (
                    "interpreter_natural_numeric_float_remainder_cold",
                    interpreter_natural_float_remainder_cold,
                ),
                (
                    "cranelift_natural_numeric_float_remainder_cold",
                    cranelift_natural_float_remainder_cold,
                ),
            ] {
                group
                    .throughput(Throughput::per_operation(1, "task"))
                    .bench(name, bench);
            }
        });

        runner.group::<MeasuredLoopContext>("scalar", |group| {
            let interpreted = || MeasuredLoopContext {
                fixture: scalar_symbol_loop_fixture(),
                host: BenchmarkHost::default(),
                native: false,
            };
            group
                .throughput(Throughput::per_operation(
                    SCALAR_LOOP_INSTRUCTIONS,
                    "bytecode_instruction",
                ))
                .factory(&interpreted)
                .bench("interpreter_symbol_bool_loop", measured_loop);

            let native = || MeasuredLoopContext {
                fixture: scalar_symbol_loop_fixture(),
                host: BenchmarkHost::default(),
                native: true,
            };
            group
                .throughput(Throughput::per_operation(
                    SCALAR_LOOP_INSTRUCTIONS,
                    "bytecode_instruction",
                ))
                .factory(&native)
                .bench("cranelift_symbol_bool_loop", measured_loop);
        });

        runner.group::<MeasuredLoopContext>("branch", |group| {
            for (fixture, instructions, interpreter_name, cranelift_name) in [
                (
                    predictable_branch_loop_fixture as fn() -> ProgramFixture,
                    PREDICTABLE_BRANCH_LOOP_INSTRUCTIONS,
                    "interpreter_predictable_branch_loop",
                    "cranelift_predictable_branch_loop",
                ),
                (
                    alternating_branch_loop_fixture as fn() -> ProgramFixture,
                    ALTERNATING_BRANCH_LOOP_INSTRUCTIONS,
                    "interpreter_alternating_branch_loop",
                    "cranelift_alternating_branch_loop",
                ),
            ] {
                let interpreted = move || MeasuredLoopContext {
                    fixture: fixture(),
                    host: BenchmarkHost::default(),
                    native: false,
                };
                group
                    .throughput(Throughput::per_operation(
                        instructions,
                        "bytecode_instruction",
                    ))
                    .factory(&interpreted)
                    .bench(interpreter_name, measured_loop);

                let native = move || MeasuredLoopContext {
                    fixture: fixture(),
                    host: BenchmarkHost::default(),
                    native: true,
                };
                group
                    .throughput(Throughput::per_operation(
                        instructions,
                        "bytecode_instruction",
                    ))
                    .factory(&native)
                    .bench(cranelift_name, measured_loop);
            }
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
        let interpreted = |_| ConcurrentLoopContext {
            fixture: integer_loop_fixture(),
            native: false,
            instruction_count: INTEGER_LOOP_INSTRUCTIONS,
        };
        let native = |_| ConcurrentLoopContext {
            fixture: integer_loop_fixture(),
            native: true,
            instruction_count: INTEGER_LOOP_INSTRUCTIONS,
        };
        runner.concurrent_group::<ConcurrentLoopContext>("integer concurrent", |group| {
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

        runner.concurrent_group::<ConcurrentLoopContext>("float concurrent", |group| {
            for (fixture, interpreter_one, interpreter_four, cranelift_one, cranelift_four) in [
                (
                    float_add_loop_fixture as fn() -> ProgramFixture,
                    "interpreter_float_add_loop_1_thread",
                    "interpreter_float_add_loop_4_threads",
                    "cranelift_float_add_loop_1_thread",
                    "cranelift_float_add_loop_4_threads",
                ),
                (
                    float_multiply_loop_fixture as fn() -> ProgramFixture,
                    "interpreter_float_multiply_loop_1_thread",
                    "interpreter_float_multiply_loop_4_threads",
                    "cranelift_float_multiply_loop_1_thread",
                    "cranelift_float_multiply_loop_4_threads",
                ),
            ] {
                for (backend, native, one_name, four_name) in [
                    ("interpreter", false, interpreter_one, interpreter_four),
                    ("cranelift", true, cranelift_one, cranelift_four),
                ] {
                    let one = move |_| {
                        let fixture = fixture();
                        ConcurrentLoopContext {
                            instruction_count: fixture.instruction_count,
                            fixture,
                            native,
                        }
                    };
                    group
                        .sample_duration(Duration::from_millis(50))
                        .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                        .metadata("backend", backend)
                        .metadata("threads", "1")
                        .factory(&one)
                        .bench(one_name, &one_thread);

                    let four = move |_| {
                        let fixture = fixture();
                        ConcurrentLoopContext {
                            instruction_count: fixture.instruction_count,
                            fixture,
                            native,
                        }
                    };
                    group
                        .sample_duration(Duration::from_millis(50))
                        .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                        .metadata("backend", backend)
                        .metadata("threads", CONCURRENT_THREADS.to_string())
                        .factory(&four)
                        .bench(four_name, &four_threads);
                }
            }
        });

        runner.concurrent_group::<ConcurrentLoopContext>("natural numeric concurrent", |group| {
            for (fixture, workload_name) in [
                (
                    natural_float_sum_fixture as fn() -> ProgramFixture,
                    "float_sum",
                ),
                (
                    natural_float_transform_fixture as fn() -> ProgramFixture,
                    "float_transform",
                ),
                (
                    natural_mixed_scale_fixture as fn() -> ProgramFixture,
                    "mixed_scale",
                ),
                (
                    natural_exact_integer_division_fixture as fn() -> ProgramFixture,
                    "exact_integer_division",
                ),
                (
                    natural_fractional_integer_division_fixture as fn() -> ProgramFixture,
                    "fractional_integer_division",
                ),
                (
                    natural_mixed_division_fixture as fn() -> ProgramFixture,
                    "mixed_division",
                ),
                (
                    natural_float_remainder_fixture as fn() -> ProgramFixture,
                    "float_remainder",
                ),
            ] {
                for (backend, native) in [("interpreter", false), ("cranelift", true)] {
                    for (threads, workers) in [
                        (1, &one_thread[..]),
                        (CONCURRENT_THREADS, &four_threads[..]),
                    ] {
                        let factory = move |_| {
                            let fixture = fixture();
                            ConcurrentLoopContext {
                                instruction_count: fixture.instruction_count,
                                fixture,
                                native,
                            }
                        };
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                            .metadata("backend", backend)
                            .metadata("threads", threads.to_string())
                            .factory(&factory)
                            .bench(
                                &format!(
                                    "{backend}_natural_numeric_{workload_name}_{threads}_threads"
                                ),
                                workers,
                            );
                    }
                }
            }
        });

        let one_scalar_thread = [ConcurrentWorker {
            name: "symbol and bool loop",
            threads: 1,
            run: run_concurrent_integer_loops,
        }];
        let four_scalar_threads = [ConcurrentWorker {
            name: "symbol and bool loop",
            threads: CONCURRENT_THREADS,
            run: run_concurrent_integer_loops,
        }];
        let interpreted_scalar = |_| ConcurrentLoopContext {
            fixture: scalar_symbol_loop_fixture(),
            native: false,
            instruction_count: SCALAR_LOOP_INSTRUCTIONS,
        };
        let native_scalar = |_| ConcurrentLoopContext {
            fixture: scalar_symbol_loop_fixture(),
            native: true,
            instruction_count: SCALAR_LOOP_INSTRUCTIONS,
        };
        runner.concurrent_group::<ConcurrentLoopContext>("scalar concurrent", |group| {
            for (backend, factory, one_name, four_name) in [
                (
                    "interpreter",
                    &interpreted_scalar as &dyn Fn(usize) -> ConcurrentLoopContext,
                    "interpreter_symbol_bool_loop_1_thread",
                    "interpreter_symbol_bool_loop_4_threads",
                ),
                (
                    "cranelift",
                    &native_scalar as &dyn Fn(usize) -> ConcurrentLoopContext,
                    "cranelift_symbol_bool_loop_1_thread",
                    "cranelift_symbol_bool_loop_4_threads",
                ),
            ] {
                group
                    .sample_duration(Duration::from_millis(50))
                    .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                    .metadata("backend", backend)
                    .metadata("threads", "1")
                    .factory(factory)
                    .bench(one_name, &one_scalar_thread);
                group
                    .sample_duration(Duration::from_millis(50))
                    .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                    .metadata("backend", backend)
                    .metadata("threads", CONCURRENT_THREADS.to_string())
                    .factory(factory)
                    .bench(four_name, &four_scalar_threads);
            }
        });

        let one_branch_thread = [ConcurrentWorker {
            name: "branch loop",
            threads: 1,
            run: run_concurrent_integer_loops,
        }];
        let four_branch_threads = [ConcurrentWorker {
            name: "branch loop",
            threads: CONCURRENT_THREADS,
            run: run_concurrent_integer_loops,
        }];
        runner.concurrent_group::<ConcurrentLoopContext>("branch concurrent", |group| {
            for (
                pattern,
                fixture,
                interpreter_one,
                interpreter_four,
                cranelift_one,
                cranelift_four,
            ) in [
                (
                    "predictable",
                    predictable_branch_loop_fixture as fn() -> ProgramFixture,
                    "interpreter_predictable_branch_loop_1_thread",
                    "interpreter_predictable_branch_loop_4_threads",
                    "cranelift_predictable_branch_loop_1_thread",
                    "cranelift_predictable_branch_loop_4_threads",
                ),
                (
                    "alternating",
                    alternating_branch_loop_fixture as fn() -> ProgramFixture,
                    "interpreter_alternating_branch_loop_1_thread",
                    "interpreter_alternating_branch_loop_4_threads",
                    "cranelift_alternating_branch_loop_1_thread",
                    "cranelift_alternating_branch_loop_4_threads",
                ),
            ] {
                for (backend, native, one_name, four_name) in [
                    ("interpreter", false, interpreter_one, interpreter_four),
                    ("cranelift", true, cranelift_one, cranelift_four),
                ] {
                    let one = move |_| {
                        let fixture = fixture();
                        ConcurrentLoopContext {
                            instruction_count: fixture.instruction_count,
                            fixture,
                            native,
                        }
                    };
                    group
                        .sample_duration(Duration::from_millis(50))
                        .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                        .metadata("backend", backend)
                        .metadata("branch_pattern", pattern)
                        .metadata("threads", "1")
                        .factory(&one)
                        .bench(one_name, &one_branch_thread);

                    let four = move |_| {
                        let fixture = fixture();
                        ConcurrentLoopContext {
                            instruction_count: fixture.instruction_count,
                            fixture,
                            native,
                        }
                    };
                    group
                        .sample_duration(Duration::from_millis(50))
                        .throughput(Throughput::per_operation(1, "bytecode_instruction"))
                        .metadata("backend", backend)
                        .metadata("branch_pattern", pattern)
                        .metadata("threads", CONCURRENT_THREADS.to_string())
                        .factory(&four)
                        .bench(four_name, &four_branch_threads);
                }
            }
        });
    }
);
