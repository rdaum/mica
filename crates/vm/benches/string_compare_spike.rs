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

//! Benchmark-only decision spike for string comparison in compiled loops.
//!
//! `StringDescriptor` is deliberately local to this benchmark. It is not a
//! proposed heap `Value` ABI.

#[allow(dead_code)]
mod fixtures;

use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Signature, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use fixtures::{BenchmarkHost, MAX_CALL_DEPTH, ProgramFixture};
use mica_var::Value;
use mica_vm::{
    Instruction, Operand, Program, Register, RegisterVm, RuntimeBinaryOp, VmHostResponse,
};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, ConcurrentBenchContext, ConcurrentBenchControl,
    ConcurrentWorker, ConcurrentWorkerResult, NoContext, Throughput, benchmark_main, black_box,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const STRING_COUNT: usize = 16_384;
const CONCURRENT_THREADS: usize = 4;
const DESCRIPTOR_SIZE: i32 = size_of::<StringDescriptor>() as i32;

#[derive(Clone, Copy)]
enum WorkloadCase {
    ShortEqual,
    MediumLateMismatch,
    MediumLengthMismatch,
}

impl WorkloadCase {
    fn name(self) -> &'static str {
        match self {
            Self::ShortEqual => "short_equal",
            Self::MediumLateMismatch => "medium_late_mismatch",
            Self::MediumLengthMismatch => "medium_length_mismatch",
        }
    }

    fn target(self) -> &'static str {
        match self {
            Self::ShortEqual => "mica-key",
            Self::MediumLateMismatch | Self::MediumLengthMismatch => {
                "mica-string-comparison-target-0123456789abcdef"
            }
        }
    }

    fn candidate(self) -> String {
        match self {
            Self::ShortEqual => self.target().to_owned(),
            Self::MediumLateMismatch => {
                let mut candidate = self.target().as_bytes().to_vec();
                *candidate.last_mut().expect("non-empty target") = b'g';
                String::from_utf8(candidate).expect("benchmark string is UTF-8")
            }
            Self::MediumLengthMismatch => self.target()[..self.target().len() - 1].to_owned(),
        }
    }

    fn expected_matches(self) -> u64 {
        match self {
            Self::ShortEqual => STRING_COUNT as u64,
            Self::MediumLateMismatch | Self::MediumLengthMismatch => 0,
        }
    }
}

const WORKLOAD_CASES: [WorkloadCase; 3] = [
    WorkloadCase::ShortEqual,
    WorkloadCase::MediumLateMismatch,
    WorkloadCase::MediumLengthMismatch,
];

#[repr(C)]
#[derive(Clone, Copy)]
struct StringDescriptor {
    pointer: *const u8,
    length: usize,
}

// The descriptors only point into the immutable strings owned by the same
// `StringWorkload`. Moving the workload does not move the strings' allocations,
// and benchmark execution never mutates them.
unsafe impl Send for StringDescriptor {}
unsafe impl Sync for StringDescriptor {}

struct StringWorkload {
    _strings: Vec<String>,
    descriptors: Vec<StringDescriptor>,
    target: String,
    expected_matches: u64,
}

impl StringWorkload {
    fn new(case: WorkloadCase) -> Self {
        let candidate = case.candidate();
        let strings = (0..STRING_COUNT)
            .map(|_| candidate.clone())
            .collect::<Vec<_>>();
        let descriptors = strings
            .iter()
            .map(|value| StringDescriptor {
                pointer: value.as_ptr(),
                length: value.len(),
            })
            .collect();
        Self {
            _strings: strings,
            descriptors,
            target: case.target().to_owned(),
            expected_matches: case.expected_matches(),
        }
    }
}

type StringLoopFunction = unsafe extern "C" fn(*const StringDescriptor, u64, *const u8, u64) -> u64;

#[derive(Clone, Copy)]
enum CompiledBackend {
    Helper,
    Native,
}

struct CompiledStringLoop {
    _module: Mutex<JITModule>,
    function: StringLoopFunction,
}

impl CompiledStringLoop {
    fn compile(backend: CompiledBackend) -> Self {
        let mut jit_builder =
            JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())
                .expect("initialize Cranelift");
        if matches!(backend, CompiledBackend::Helper) {
            jit_builder.symbol(
                "mica_benchmark_string_equal",
                benchmark_string_equal as *const u8,
            );
        }
        let mut module = JITModule::new(jit_builder);
        let pointer_type = module.target_config().pointer_type();
        let mut signature = Signature::new(module.isa().default_call_conv());
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(types::I64));
        signature.returns.push(AbiParam::new(types::I64));

        let helper_id = if matches!(backend, CompiledBackend::Helper) {
            let mut helper_signature = Signature::new(module.isa().default_call_conv());
            helper_signature.params.push(AbiParam::new(pointer_type));
            helper_signature.params.push(AbiParam::new(types::I64));
            helper_signature.params.push(AbiParam::new(pointer_type));
            helper_signature.params.push(AbiParam::new(types::I64));
            helper_signature.returns.push(AbiParam::new(types::I64));
            Some(
                module
                    .declare_function(
                        "mica_benchmark_string_equal",
                        Linkage::Import,
                        &helper_signature,
                    )
                    .expect("declare string equality helper"),
            )
        } else {
            None
        };
        let function_id = module
            .declare_function("mica_benchmark_string_loop", Linkage::Local, &signature)
            .expect("declare string loop");
        let mut context = Context::new();
        context.func.signature = signature;
        let helper = helper_id.map(|id| module.declare_func_in_func(id, &mut context.func));
        let mut builder_context = FunctionBuilderContext::new();
        build_string_loop(&mut context, &mut builder_context, backend, helper);
        module
            .define_function(function_id, &mut context)
            .expect("compile string loop");
        module.finalize_definitions().expect("finalize string loop");
        let code = module.get_finalized_function(function_id);
        let function = unsafe { std::mem::transmute::<*const u8, StringLoopFunction>(code) };
        Self {
            _module: Mutex::new(module),
            function,
        }
    }

    fn run(&self, workload: &StringWorkload) -> u64 {
        unsafe {
            (self.function)(
                workload.descriptors.as_ptr(),
                workload.descriptors.len() as u64,
                workload.target.as_ptr(),
                workload.target.len() as u64,
            )
        }
    }
}

unsafe extern "C" fn benchmark_string_equal(
    left: *const u8,
    left_length: u64,
    right: *const u8,
    right_length: u64,
) -> u64 {
    if left_length != right_length {
        return 0;
    }
    let left = unsafe { std::slice::from_raw_parts(left, left_length as usize) };
    let right = unsafe { std::slice::from_raw_parts(right, right_length as usize) };
    u64::from(left == right)
}

fn build_string_loop(
    context: &mut Context,
    builder_context: &mut FunctionBuilderContext,
    backend: CompiledBackend,
    helper: Option<cranelift_codegen::ir::FuncRef>,
) {
    let mut builder = FunctionBuilder::new(&mut context.func, builder_context);
    let entry = builder.create_block();
    let header = builder.create_block();
    let body = builder.create_block();
    let count_match = builder.create_block();
    let advance = builder.create_block();
    let complete = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    for block in [header, body, advance] {
        builder.append_block_param(block, types::I64);
        builder.append_block_param(block, types::I64);
    }
    builder.append_block_param(count_match, types::I64);
    builder.append_block_param(count_match, types::I64);
    builder.append_block_param(complete, types::I64);

    builder.switch_to_block(entry);
    let params = builder.block_params(entry).to_vec();
    let descriptors = params[0];
    let descriptor_count = params[1];
    let target = params[2];
    let target_length = params[3];
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().jump(header, &[zero.into(), zero.into()]);

    builder.switch_to_block(header);
    let index = builder.block_params(header)[0];
    let matches = builder.block_params(header)[1];
    let in_bounds = builder
        .ins()
        .icmp(IntCC::UnsignedLessThan, index, descriptor_count);
    builder.ins().brif(
        in_bounds,
        body,
        &[index.into(), matches.into()],
        complete,
        &[matches.into()],
    );

    builder.switch_to_block(body);
    let index = builder.block_params(body)[0];
    let matches = builder.block_params(body)[1];
    let descriptor_offset = builder.ins().imul_imm(index, i64::from(DESCRIPTOR_SIZE));
    let descriptor = builder.ins().iadd(descriptors, descriptor_offset);
    let flags = MemFlagsData::new().with_readonly();
    let candidate = builder.ins().load(types::I64, flags, descriptor, 0);
    let candidate_length =
        builder
            .ins()
            .load(types::I64, flags, descriptor, size_of::<*const u8>() as i32);
    match backend {
        CompiledBackend::Helper => {
            let call = builder.ins().call(
                helper.expect("helper function reference"),
                &[candidate, candidate_length, target, target_length],
            );
            let equal = builder.inst_results(call)[0];
            let equal = builder.ins().icmp_imm(IntCC::NotEqual, equal, 0);
            builder.ins().brif(
                equal,
                count_match,
                &[index.into(), matches.into()],
                advance,
                &[index.into(), matches.into()],
            );
        }
        CompiledBackend::Native => emit_native_string_equal(
            &mut builder,
            candidate,
            candidate_length,
            target,
            target_length,
            index,
            matches,
            count_match,
            advance,
        ),
    }

    builder.switch_to_block(count_match);
    let index = builder.block_params(count_match)[0];
    let matches = builder.block_params(count_match)[1];
    let matches = builder.ins().iadd_imm(matches, 1);
    builder.ins().jump(advance, &[index.into(), matches.into()]);

    builder.switch_to_block(advance);
    let index = builder.block_params(advance)[0];
    let matches = builder.block_params(advance)[1];
    let index = builder.ins().iadd_imm(index, 1);
    builder.ins().jump(header, &[index.into(), matches.into()]);

    builder.switch_to_block(complete);
    let matches = builder.block_params(complete)[0];
    builder.ins().return_(&[matches]);
    builder.seal_all_blocks();
    builder.finalize();
}

#[allow(clippy::too_many_arguments)]
fn emit_native_string_equal(
    builder: &mut FunctionBuilder<'_>,
    candidate: cranelift_codegen::ir::Value,
    candidate_length: cranelift_codegen::ir::Value,
    target: cranelift_codegen::ir::Value,
    target_length: cranelift_codegen::ir::Value,
    outer_index: cranelift_codegen::ir::Value,
    matches: cranelift_codegen::ir::Value,
    count_match: cranelift_codegen::ir::Block,
    advance: cranelift_codegen::ir::Block,
) {
    let byte_header = builder.create_block();
    let byte_body = builder.create_block();
    builder.append_block_param(byte_header, types::I64);
    builder.append_block_param(byte_body, types::I64);
    let same_length = builder
        .ins()
        .icmp(IntCC::Equal, candidate_length, target_length);
    let zero = builder.ins().iconst(types::I64, 0);
    builder.ins().brif(
        same_length,
        byte_header,
        &[zero.into()],
        advance,
        &[outer_index.into(), matches.into()],
    );

    builder.switch_to_block(byte_header);
    let byte_index = builder.block_params(byte_header)[0];
    let has_byte = builder
        .ins()
        .icmp(IntCC::UnsignedLessThan, byte_index, candidate_length);
    builder.ins().brif(
        has_byte,
        byte_body,
        &[byte_index.into()],
        count_match,
        &[outer_index.into(), matches.into()],
    );

    builder.switch_to_block(byte_body);
    let byte_index = builder.block_params(byte_body)[0];
    let next_candidate = builder.ins().iadd(candidate, byte_index);
    let next_target = builder.ins().iadd(target, byte_index);
    let candidate_byte = builder.ins().load(
        types::I8,
        MemFlagsData::new().with_readonly(),
        next_candidate,
        0,
    );
    let target_byte = builder.ins().load(
        types::I8,
        MemFlagsData::new().with_readonly(),
        next_target,
        0,
    );
    let bytes_equal = builder
        .ins()
        .icmp(IntCC::Equal, candidate_byte, target_byte);
    let next_byte = builder.ins().iadd_imm(byte_index, 1);
    builder.ins().brif(
        bytes_equal,
        byte_header,
        &[next_byte.into()],
        advance,
        &[outer_index.into(), matches.into()],
    );
}

fn register(index: u16) -> Register {
    Register(index)
}

fn string_vm_fixture(case: WorkloadCase) -> ProgramFixture {
    let candidate = case.candidate();
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| Value::string(&candidate)),
        Value::string(case.target()),
        case.expected_matches(),
        RuntimeBinaryOp::Eq,
    )
}

fn list_vm_fixture() -> ProgramFixture {
    let make_list = || Value::list((0..16).map(|value| Value::int(value).unwrap()));
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| make_list()),
        make_list(),
        STRING_COUNT as u64,
        RuntimeBinaryOp::Eq,
    )
}

fn map_vm_fixture() -> ProgramFixture {
    let make_map = |last| {
        Value::map((0..8).map(|key| {
            let value = if key == 7 { last } else { key };
            (Value::int(key).unwrap(), Value::int(value).unwrap())
        }))
    };
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| make_map(8)),
        make_map(7),
        0,
        RuntimeBinaryOp::Eq,
    )
}

fn string_ordering_vm_fixture() -> ProgramFixture {
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| Value::string("mica-order-alpha")),
        Value::string("mica-order-beta"),
        STRING_COUNT as u64,
        RuntimeBinaryOp::Lt,
    )
}

fn list_ordering_vm_fixture() -> ProgramFixture {
    let make_list = |last| {
        Value::list((0..16).map(|index| {
            Value::int(if index == 15 { last } else { index }).expect("benchmark list integer fits")
        }))
    };
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| make_list(15)),
        make_list(16),
        STRING_COUNT as u64,
        RuntimeBinaryOp::Lt,
    )
}

fn map_ordering_vm_fixture() -> ProgramFixture {
    let make_map = |last| {
        Value::map((0..8).map(|key| {
            let value = if key == 7 { last } else { key };
            (Value::int(key).unwrap(), Value::int(value).unwrap())
        }))
    };
    comparison_vm_fixture(
        (0..STRING_COUNT).map(|_| make_map(7)),
        make_map(8),
        STRING_COUNT as u64,
        RuntimeBinaryOp::Lt,
    )
}

fn comparison_vm_fixture(
    values: impl IntoIterator<Item = Value>,
    target: Value,
    expected_matches: u64,
    comparison: RuntimeBinaryOp,
) -> ProgramFixture {
    let collection = Value::list(values);
    let zero = Value::int(0).unwrap();
    let one = Value::int(1).unwrap();
    let program = Program::new(
        9,
        [
            Instruction::Load {
                dst: register(0),
                value: collection,
            },
            Instruction::CollectionLen {
                dst: register(1),
                collection: register(0),
            },
            Instruction::Load {
                dst: register(2),
                value: zero.clone(),
            },
            Instruction::Load {
                dst: register(3),
                value: zero,
            },
            Instruction::Load {
                dst: register(4),
                value: one,
            },
            Instruction::Load {
                dst: register(5),
                value: target,
            },
            Instruction::Binary {
                dst: register(6),
                op: RuntimeBinaryOp::Lt,
                left: register(2),
                right: register(1),
            },
            Instruction::Branch {
                condition: register(6),
                if_true: 8,
                if_false: 14,
            },
            Instruction::CollectionValueAt {
                dst: register(7),
                collection: register(0),
                index: register(2),
            },
            Instruction::Binary {
                dst: register(8),
                op: comparison,
                left: register(7),
                right: register(5),
            },
            Instruction::Branch {
                condition: register(8),
                if_true: 11,
                if_false: 12,
            },
            Instruction::Binary {
                dst: register(3),
                op: RuntimeBinaryOp::Add,
                left: register(3),
                right: register(4),
            },
            Instruction::Binary {
                dst: register(2),
                op: RuntimeBinaryOp::Add,
                left: register(2),
                right: register(4),
            },
            Instruction::Jump { target: 6 },
            Instruction::Return {
                value: Operand::Register(register(3)),
            },
        ],
    )
    .unwrap();
    ProgramFixture {
        program: Arc::new(program),
        instruction_count: if expected_matches == STRING_COUNT as u64 {
            (STRING_COUNT as u64 * 8) + 9
        } else {
            (STRING_COUNT as u64 * 7) + 9
        },
    }
}

fn execute_vm(fixture: &ProgramFixture, host: &mut BenchmarkHost, native: bool) -> u64 {
    let mut vm = if native {
        RegisterVm::new(Arc::clone(&fixture.program))
    } else {
        RegisterVm::new_interpreted(Arc::clone(&fixture.program))
    };
    let response = vm
        .run_until_host_response(
            host,
            fixture.instruction_count as usize + STRING_COUNT,
            MAX_CALL_DEPTH,
        )
        .unwrap();
    let VmHostResponse::Complete(value) = response else {
        panic!("string comparison fixture did not complete: {response:?}");
    };
    value.as_int().expect("string match count is an integer") as u64
}

struct VmContext {
    fixture: ProgramFixture,
    host: BenchmarkHost,
    expected_matches: u64,
    native: bool,
}

struct CompiledContext {
    workload: StringWorkload,
    compiled: CompiledStringLoop,
}

enum ConcurrentContext {
    Vm {
        fixture: ProgramFixture,
        expected_matches: u64,
        native: bool,
    },
    Compiled(Box<CompiledContext>),
}

impl BenchContext for VmContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::new(WorkloadCase::MediumLateMismatch, true)
    }
}

impl VmContext {
    fn new(case: WorkloadCase, native: bool) -> Self {
        Self {
            fixture: string_vm_fixture(case),
            host: BenchmarkHost::default(),
            expected_matches: case.expected_matches(),
            native,
        }
    }

    fn list(native: bool) -> Self {
        Self {
            fixture: list_vm_fixture(),
            host: BenchmarkHost::default(),
            expected_matches: STRING_COUNT as u64,
            native,
        }
    }

    fn map(native: bool) -> Self {
        Self {
            fixture: map_vm_fixture(),
            host: BenchmarkHost::default(),
            expected_matches: 0,
            native,
        }
    }

    fn string_ordering(native: bool) -> Self {
        Self {
            fixture: string_ordering_vm_fixture(),
            host: BenchmarkHost::default(),
            expected_matches: STRING_COUNT as u64,
            native,
        }
    }

    fn list_ordering(native: bool) -> Self {
        Self {
            fixture: list_ordering_vm_fixture(),
            host: BenchmarkHost::default(),
            expected_matches: STRING_COUNT as u64,
            native,
        }
    }

    fn map_ordering(native: bool) -> Self {
        Self {
            fixture: map_ordering_vm_fixture(),
            host: BenchmarkHost::default(),
            expected_matches: STRING_COUNT as u64,
            native,
        }
    }
}

impl BenchContext for CompiledContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::new(WorkloadCase::MediumLateMismatch, CompiledBackend::Native)
    }
}

impl CompiledContext {
    fn new(case: WorkloadCase, backend: CompiledBackend) -> Self {
        Self {
            workload: StringWorkload::new(case),
            compiled: CompiledStringLoop::compile(backend),
        }
    }

    fn run(&self) -> u64 {
        self.compiled.run(&self.workload)
    }
}

impl ConcurrentBenchContext for ConcurrentContext {
    fn prepare(_num_threads: usize) -> Self {
        Self::Compiled(Box::new(CompiledContext::new(
            WorkloadCase::MediumLateMismatch,
            CompiledBackend::Native,
        )))
    }
}

fn bench_vm(context: &mut VmContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let matches = execute_vm(&context.fixture, &mut context.host, context.native);
        assert_eq!(matches, context.expected_matches);
        black_box(matches);
    }
}

fn bench_compiled(context: &mut CompiledContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let matches = context.run();
        assert_eq!(matches, context.workload.expected_matches);
        black_box(matches);
    }
}

fn bench_interpreter_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_vm_cold(chunk_size, false);
}

fn bench_vm_helper_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_vm_cold(chunk_size, true);
}

fn bench_vm_cold(chunk_size: usize, native: bool) {
    for _ in 0..chunk_size {
        let case = WorkloadCase::MediumLateMismatch;
        let fixture = string_vm_fixture(case);
        let mut host = BenchmarkHost::default();
        assert_eq!(
            execute_vm(&fixture, &mut host, native),
            case.expected_matches()
        );
    }
}

fn bench_interpreter_string_ordering_cold(
    _context: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    bench_vm_ordering_cold(chunk_size, false, string_ordering_vm_fixture);
}

fn bench_native_string_ordering_cold(
    _context: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    bench_vm_ordering_cold(chunk_size, true, string_ordering_vm_fixture);
}

fn bench_interpreter_list_ordering_cold(
    _context: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    bench_vm_ordering_cold(chunk_size, false, list_ordering_vm_fixture);
}

fn bench_native_list_ordering_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_vm_ordering_cold(chunk_size, true, list_ordering_vm_fixture);
}

fn bench_interpreter_map_ordering_cold(
    _context: &mut NoContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    bench_vm_ordering_cold(chunk_size, false, map_ordering_vm_fixture);
}

fn bench_native_map_ordering_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_vm_ordering_cold(chunk_size, true, map_ordering_vm_fixture);
}

fn bench_vm_ordering_cold(chunk_size: usize, native: bool, workload: fn() -> ProgramFixture) {
    for _ in 0..chunk_size {
        let fixture = workload();
        let mut host = BenchmarkHost::default();
        assert_eq!(execute_vm(&fixture, &mut host, native), STRING_COUNT as u64,);
    }
}

fn bench_helper_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_compiled_cold(chunk_size, CompiledBackend::Helper);
}

fn bench_native_cold(_context: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    bench_compiled_cold(chunk_size, CompiledBackend::Native);
}

fn bench_compiled_cold(chunk_size: usize, backend: CompiledBackend) {
    for _ in 0..chunk_size {
        let context = CompiledContext::new(WorkloadCase::MediumLateMismatch, backend);
        assert_eq!(context.run(), context.workload.expected_matches);
    }
}

fn run_concurrent(
    context: &ConcurrentContext,
    control: &ConcurrentBenchControl,
) -> ConcurrentWorkerResult {
    let mut host = BenchmarkHost::default();
    let mut comparisons = 0_u64;
    while !control.should_stop() {
        let matches = match context {
            ConcurrentContext::Vm {
                fixture,
                expected_matches,
                native,
            } => {
                let matches = execute_vm(fixture, &mut host, *native);
                assert_eq!(&matches, expected_matches);
                matches
            }
            ConcurrentContext::Compiled(context) => {
                let matches = context.run();
                assert_eq!(matches, context.workload.expected_matches);
                matches
            }
        };
        black_box(matches);
        comparisons = comparisons.wrapping_add(STRING_COUNT as u64);
    }
    ConcurrentWorkerResult::operations(comparisons)
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, warm, cold, concurrent, interpreter, vm_helper, helper, native, equality, ordering, string, list, map, or a case name"
                .to_owned()
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
        runner.group::<VmContext>("string compare warm", |group| {
            for case in WORKLOAD_CASES {
                for (backend_name, native) in [("interpreter", false), ("vm_helper", true)] {
                    let factory = move || VmContext::new(case, native);
                    group
                        .throughput(Throughput::per_operation(STRING_COUNT as u64, "comparison"))
                        .factory(&factory)
                        .bench(&format!("{backend_name}_{}", case.name()), bench_vm);
                }
            }
        });

        runner.group::<VmContext>("collection equality warm", |group| {
            for (workload_name, workload) in [
                ("list_equal", VmContext::list as fn(bool) -> VmContext),
                ("map_late_mismatch", VmContext::map as fn(bool) -> VmContext),
            ] {
                for (backend_name, native) in [("interpreter", false), ("vm_helper", true)] {
                    let factory = move || workload(native);
                    group
                        .throughput(Throughput::per_operation(STRING_COUNT as u64, "comparison"))
                        .factory(&factory)
                        .bench(&format!("{backend_name}_{workload_name}"), bench_vm);
                }
            }
        });

        runner.group::<VmContext>("collection ordering warm", |group| {
            for (workload_name, workload) in [
                (
                    "string_less_than",
                    VmContext::string_ordering as fn(bool) -> VmContext,
                ),
                ("list_less_than", VmContext::list_ordering),
                ("map_less_than", VmContext::map_ordering),
            ] {
                for (backend_name, native) in [("interpreter", false), ("vm_helper", true)] {
                    let factory = move || workload(native);
                    group
                        .throughput(Throughput::per_operation(STRING_COUNT as u64, "comparison"))
                        .factory(&factory)
                        .bench(&format!("{backend_name}_{workload_name}"), bench_vm);
                }
            }
        });

        runner.group::<CompiledContext>("string compare warm", |group| {
            for case in WORKLOAD_CASES {
                for (backend, backend_name) in [
                    (CompiledBackend::Helper, "helper"),
                    (CompiledBackend::Native, "native"),
                ] {
                    let factory = move || CompiledContext::new(case, backend);
                    group
                        .throughput(Throughput::per_operation(STRING_COUNT as u64, "comparison"))
                        .factory(&factory)
                        .bench(&format!("{backend_name}_{}", case.name()), bench_compiled);
                }
            }
        });

        runner.group::<NoContext>("string compare cold", |group| {
            group
                .throughput(Throughput::per_operation(1, "setup_and_run"))
                .bench(
                    "interpreter_medium_late_mismatch_cold",
                    bench_interpreter_cold,
                );
            group
                .throughput(Throughput::per_operation(1, "setup_and_run"))
                .bench("vm_helper_medium_late_mismatch_cold", bench_vm_helper_cold);
            group
                .throughput(Throughput::per_operation(1, "setup_and_run"))
                .bench("helper_medium_late_mismatch_cold", bench_helper_cold);
            group
                .throughput(Throughput::per_operation(1, "setup_and_run"))
                .bench("native_medium_late_mismatch_cold", bench_native_cold);
        });

        runner.group::<NoContext>("collection ordering cold", |group| {
            for (name, bench) in [
                (
                    "interpreter_string_less_than_cold",
                    bench_interpreter_string_ordering_cold
                        as fn(&mut NoContext, usize, usize),
                ),
                (
                    "vm_helper_string_less_than_cold",
                    bench_native_string_ordering_cold,
                ),
                (
                    "interpreter_list_less_than_cold",
                    bench_interpreter_list_ordering_cold,
                ),
                (
                    "vm_helper_list_less_than_cold",
                    bench_native_list_ordering_cold,
                ),
                (
                    "interpreter_map_less_than_cold",
                    bench_interpreter_map_ordering_cold,
                ),
                (
                    "vm_helper_map_less_than_cold",
                    bench_native_map_ordering_cold,
                ),
            ] {
                group
                    .throughput(Throughput::per_operation(1, "setup_and_run"))
                    .bench(name, bench);
            }
        });

        let one_thread = [ConcurrentWorker {
            name: "string compare",
            threads: 1,
            run: run_concurrent,
        }];
        let four_threads = [ConcurrentWorker {
            name: "string compare",
            threads: CONCURRENT_THREADS,
            run: run_concurrent,
        }];
        runner.concurrent_group::<ConcurrentContext>("string compare concurrent", |group| {
            for (backend_name, backend, native) in [
                ("interpreter", None, false),
                ("vm_helper", None, true),
                ("helper", Some(CompiledBackend::Helper), true),
                ("native", Some(CompiledBackend::Native), true),
            ] {
                for (threads, workers) in [(1, &one_thread[..]), (4, &four_threads[..])] {
                    let factory = move |_| match backend {
                        Some(backend) => ConcurrentContext::Compiled(Box::new(
                            CompiledContext::new(WorkloadCase::MediumLateMismatch, backend),
                        )),
                        None => ConcurrentContext::Vm {
                            fixture: string_vm_fixture(WorkloadCase::MediumLateMismatch),
                            expected_matches: 0,
                            native,
                        },
                    };
                    group
                        .sample_duration(Duration::from_millis(50))
                        .throughput(Throughput::per_operation(1, "comparison"))
                        .metadata("backend", backend_name)
                        .metadata("threads", threads.to_string())
                        .factory(&factory)
                        .bench(
                            &format!("{backend_name}_medium_late_mismatch_{threads}_threads"),
                            workers,
                        );
                }
            }
        });

        runner.concurrent_group::<ConcurrentContext>("collection equality concurrent", |group| {
            for (workload_name, workload) in [
                ("list_equal", VmContext::list as fn(bool) -> VmContext),
                ("map_late_mismatch", VmContext::map as fn(bool) -> VmContext),
            ] {
                for (backend_name, native) in [("interpreter", false), ("vm_helper", true)] {
                    for (threads, workers) in [(1, &one_thread[..]), (4, &four_threads[..])] {
                        let factory = move |_| {
                            let context = workload(native);
                            ConcurrentContext::Vm {
                                fixture: context.fixture,
                                expected_matches: context.expected_matches,
                                native,
                            }
                        };
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(1, "comparison"))
                            .metadata("backend", backend_name)
                            .metadata("threads", threads.to_string())
                            .factory(&factory)
                            .bench(
                                &format!("{backend_name}_{workload_name}_{threads}_threads"),
                                workers,
                            );
                    }
                }
            }
        });

        runner.concurrent_group::<ConcurrentContext>("collection ordering concurrent", |group| {
            for (workload_name, workload) in [
                (
                    "string_less_than",
                    VmContext::string_ordering as fn(bool) -> VmContext,
                ),
                ("list_less_than", VmContext::list_ordering),
                ("map_less_than", VmContext::map_ordering),
            ] {
                for (backend_name, native) in [("interpreter", false), ("vm_helper", true)] {
                    for (threads, workers) in [(1, &one_thread[..]), (4, &four_threads[..])] {
                        let factory = move |_| {
                            let context = workload(native);
                            ConcurrentContext::Vm {
                                fixture: context.fixture,
                                expected_matches: context.expected_matches,
                                native,
                            }
                        };
                        group
                            .sample_duration(Duration::from_millis(50))
                            .throughput(Throughput::per_operation(1, "comparison"))
                            .metadata("backend", backend_name)
                            .metadata("threads", threads.to_string())
                            .factory(&factory)
                            .bench(
                                &format!("{backend_name}_{workload_name}_{threads}_threads"),
                                workers,
                            );
                    }
                }
            }
        });
    }
);
