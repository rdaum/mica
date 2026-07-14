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

use crate::ValueEmitter;
use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Signature, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use mica_var::Value;
use mica_var::abi::{
    VALUE_ABI_VERSION, VALUE_EMPTY_RELATION_TAG, borrowed_value_bits, from_owned_value_bits,
    pack_value,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

const STATUS_COMPLETE: u32 = 0;
const STATUS_BUDGET_EXHAUSTED: u32 = 1;
const STATUS_SIDE_EXIT: u32 = 2;

#[repr(C)]
struct RawIntegerLoopOutcome {
    current: u64,
    condition: u64,
    iterations: u64,
}

type IntegerLoopFunction =
    unsafe extern "C" fn(u64, u64, u64, u64, *mut RawIntegerLoopOutcome) -> u32;

#[derive(Debug)]
pub struct IntegerLoopError(String);

impl Display for IntegerLoopError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for IntegerLoopError {}

#[derive(Debug, Eq, PartialEq)]
pub enum IntegerLoopOutcome {
    Complete {
        current: Value,
        condition: Value,
        iterations: u64,
    },
    BudgetExhausted {
        current: Value,
        condition: Value,
        iterations: u64,
    },
    SideExit,
}

/// A generated `integer add; integer less-than; branch` loop and the executable
/// allocation that owns its code.
///
/// The loop starts immediately after a taken branch, so `current < limit` is a
/// caller precondition. It returns complete register values without mutating VM
/// state, allowing the caller to commit them atomically or discard a side exit.
///
/// The module is held behind a mutex because Cranelift's module container is
/// not `Sync`. Generated code is immutable after finalization, so execution
/// does not acquire the mutex.
pub struct CompiledIntegerLoop {
    _module: Mutex<JITModule>,
    function: IntegerLoopFunction,
    code_size: usize,
    imported_helper_count: usize,
    value_abi_version: u32,
}

impl CompiledIntegerLoop {
    pub fn compile() -> Result<Self, IntegerLoopError> {
        let builder = JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())
            .map_err(|error| {
                IntegerLoopError(format!("could not initialize Cranelift: {error}"))
            })?;
        let mut module = JITModule::new(builder);
        let pointer_type = module.target_config().pointer_type();

        let mut signature = Signature::new(module.isa().default_call_conv());
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(pointer_type));
        signature.returns.push(AbiParam::new(types::I32));

        let function_id = module
            .declare_function("mica_integer_loop", Linkage::Local, &signature)
            .map_err(|error| {
                IntegerLoopError(format!("could not declare integer loop: {error}"))
            })?;
        let mut context = Context::new();
        context.func.signature = signature;
        let mut builder_context = FunctionBuilderContext::new();
        Self::build_function(&mut context, &mut builder_context);
        let imported_helper_count = context.func.dfg.ext_funcs.len();

        module
            .define_function(function_id, &mut context)
            .map_err(|error| {
                IntegerLoopError(format!("could not compile integer loop: {error}"))
            })?;
        let code_size = context
            .compiled_code()
            .map(|code| code.code_info().total_size as usize)
            .ok_or_else(|| IntegerLoopError("Cranelift did not retain compiled code".to_owned()))?;
        module.finalize_definitions().map_err(|error| {
            IntegerLoopError(format!("could not finalize integer loop: {error}"))
        })?;
        let code = module.get_finalized_function(function_id);
        let function = unsafe { std::mem::transmute::<*const u8, IntegerLoopFunction>(code) };

        Ok(Self {
            _module: Mutex::new(module),
            function,
            code_size,
            imported_helper_count,
            value_abi_version: VALUE_ABI_VERSION,
        })
    }

    fn build_function(context: &mut Context, builder_context: &mut FunctionBuilderContext) {
        let mut builder = FunctionBuilder::new(&mut context.func, builder_context);
        let entry = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let after_add = builder.create_block();
        let continue_loop = builder.create_block();
        let complete = builder.create_block();
        let budget_exhausted = builder.create_block();
        let side_exit = builder.create_block();

        builder.append_block_params_for_function_params(entry);
        builder.append_block_param(loop_header, types::I64);
        builder.append_block_param(loop_header, types::I64);
        builder.append_block_param(loop_body, types::I64);
        builder.append_block_param(loop_body, types::I64);
        builder.append_block_param(after_add, types::I64);
        builder.append_block_param(after_add, types::I64);
        builder.append_block_param(continue_loop, types::I64);
        builder.append_block_param(continue_loop, types::I64);
        builder.append_block_param(continue_loop, types::I64);
        for block in [complete, budget_exhausted] {
            builder.append_block_param(block, types::I64);
            builder.append_block_param(block, types::I64);
            builder.append_block_param(block, types::I64);
        }

        builder.switch_to_block(entry);
        let params = builder.block_params(entry);
        let start = params[0];
        let step = params[1];
        let limit = params[2];
        let max_iterations = params[3];
        let output = params[4];
        let start_is_int = ValueEmitter::emit_is_int(&mut builder, start);
        let step_is_int = ValueEmitter::emit_is_int(&mut builder, step);
        let limit_is_int = ValueEmitter::emit_is_int(&mut builder, limit);
        let operands_are_ints = builder.ins().band(start_is_int, step_is_int);
        let operands_are_ints = builder.ins().band(operands_are_ints, limit_is_int);
        let zero = builder.ins().iconst(types::I64, 0);
        builder.ins().brif(
            operands_are_ints,
            loop_header,
            &[start.into(), zero.into()],
            side_exit,
            &[],
        );

        builder.switch_to_block(loop_header);
        let current = builder.block_params(loop_header)[0];
        let iterations = builder.block_params(loop_header)[1];
        let has_budget = builder
            .ins()
            .icmp(IntCC::UnsignedLessThan, iterations, max_iterations);
        let true_word = builder.ins().iconst(types::I8, 1);
        let true_word = ValueEmitter::emit_bool(&mut builder, true_word);
        builder.ins().brif(
            has_budget,
            loop_body,
            &[current.into(), iterations.into()],
            budget_exhausted,
            &[current.into(), true_word.into(), iterations.into()],
        );

        builder.switch_to_block(loop_body);
        let current = builder.block_params(loop_body)[0];
        let iterations = builder.block_params(loop_body)[1];
        let next = ValueEmitter::emit_checked_int_add(&mut builder, current, step);
        let one = builder.ins().iconst(types::I64, 1);
        let next_iterations = builder.ins().iadd(iterations, one);
        builder.ins().brif(
            next.is_fast(),
            after_add,
            &[next.word().into(), next_iterations.into()],
            side_exit,
            &[],
        );

        builder.switch_to_block(after_add);
        let current = builder.block_params(after_add)[0];
        let iterations = builder.block_params(after_add)[1];
        let comparison = ValueEmitter::emit_checked_int_lt(&mut builder, current, limit);
        builder.ins().brif(
            comparison.is_fast(),
            continue_loop,
            &[current.into(), iterations.into(), comparison.word().into()],
            side_exit,
            &[],
        );

        builder.switch_to_block(continue_loop);
        let current = builder.block_params(continue_loop)[0];
        let iterations = builder.block_params(continue_loop)[1];
        let condition = builder.block_params(continue_loop)[2];
        let condition_payload = ValueEmitter::emit_payload(&mut builder, condition);
        let keep_looping = builder
            .ins()
            .icmp_imm(IntCC::NotEqual, condition_payload, 0);
        builder.ins().brif(
            keep_looping,
            loop_header,
            &[current.into(), iterations.into()],
            complete,
            &[current.into(), condition.into(), iterations.into()],
        );

        builder.switch_to_block(complete);
        Self::emit_outcome(&mut builder, complete, output, STATUS_COMPLETE);

        builder.switch_to_block(budget_exhausted);
        Self::emit_outcome(
            &mut builder,
            budget_exhausted,
            output,
            STATUS_BUDGET_EXHAUSTED,
        );

        builder.switch_to_block(side_exit);
        let status = builder
            .ins()
            .iconst(types::I32, i64::from(STATUS_SIDE_EXIT));
        builder.ins().return_(&[status]);

        builder.seal_all_blocks();
        builder.finalize();
    }

    fn emit_outcome(
        builder: &mut FunctionBuilder<'_>,
        block: cranelift_codegen::ir::Block,
        output: cranelift_codegen::ir::Value,
        status: u32,
    ) {
        let values = builder.block_params(block);
        let current = values[0];
        let condition = values[1];
        let iterations = values[2];
        builder.ins().store(MemFlagsData::new(), current, output, 0);
        builder
            .ins()
            .store(MemFlagsData::new(), condition, output, 8);
        builder
            .ins()
            .store(MemFlagsData::new(), iterations, output, 16);
        let status = builder.ins().iconst(types::I32, i64::from(status));
        builder.ins().return_(&[status]);
    }

    pub const fn value_abi_version(&self) -> u32 {
        self.value_abi_version
    }

    pub const fn code_size(&self) -> usize {
        self.code_size
    }

    pub const fn imported_helper_count(&self) -> usize {
        self.imported_helper_count
    }

    pub fn run(
        &self,
        start: &Value,
        step: &Value,
        limit: &Value,
        max_iterations: u64,
    ) -> IntegerLoopOutcome {
        if self.value_abi_version != VALUE_ABI_VERSION {
            return IntegerLoopOutcome::SideExit;
        }
        let empty_relation = pack_value(VALUE_EMPTY_RELATION_TAG, 0);
        let mut output = RawIntegerLoopOutcome {
            current: empty_relation,
            condition: empty_relation,
            iterations: 0,
        };
        let status = unsafe {
            (self.function)(
                borrowed_value_bits(start),
                borrowed_value_bits(step),
                borrowed_value_bits(limit),
                max_iterations,
                &mut output,
            )
        };

        match status {
            STATUS_COMPLETE => IntegerLoopOutcome::Complete {
                current: unsafe { from_owned_value_bits(output.current) },
                condition: unsafe { from_owned_value_bits(output.condition) },
                iterations: output.iterations,
            },
            STATUS_BUDGET_EXHAUSTED => IntegerLoopOutcome::BudgetExhausted {
                current: unsafe { from_owned_value_bits(output.current) },
                condition: unsafe { from_owned_value_bits(output.condition) },
                iterations: output.iterations,
            },
            STATUS_SIDE_EXIT => IntegerLoopOutcome::SideExit,
            status => panic!("generated integer loop returned unknown status {status}"),
        }
    }
}
