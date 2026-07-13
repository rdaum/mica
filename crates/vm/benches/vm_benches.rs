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
use micromeasure::{BenchContext, BenchmarkMainOptions, Throughput, benchmark_main, black_box};
use std::time::Duration;

struct IntegerLoopContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
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

fn execute_fixture(fixture: &ProgramFixture, host: &mut BenchmarkHost) -> VmHostResponse {
    let mut vm = RegisterVm::new(fixture.program.clone());
    vm.run_until_host_response(host, fixture.instruction_count as usize, MAX_CALL_DEPTH)
        .unwrap()
}

fn interpreter_integer_loop(ctx: &mut IntegerLoopContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture(&ctx.fixture, &mut ctx.host));
    }
}

fn interpreter_static_calls(ctx: &mut StaticCallContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture(&ctx.fixture, &mut ctx.host));
    }
}

fn interpreter_builtin_calls(ctx: &mut BuiltinCallContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(execute_fixture(&ctx.fixture, &mut ctx.host));
    }
    black_box(ctx.host.builtin_calls());
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
    }
);
