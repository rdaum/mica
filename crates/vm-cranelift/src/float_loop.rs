// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.

//! A compact native binary32 loop used to validate the Mica float value ABI.

use crate::{FloatComparison, ValueEmitter};
use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Signature, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use mica_var::Value;
use mica_var::abi::{
    VALUE_ABI_VERSION, VALUE_NOTHING_TAG, borrowed_value_bits, from_owned_value_bits, pack_value,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

const STATUS_COMPLETE: u32 = 0;
const STATUS_BUDGET_EXHAUSTED: u32 = 1;
const STATUS_SIDE_EXIT: u32 = 2;

#[repr(C)]
struct RawFloatLoopOutcome {
    current: u64,
    condition: u64,
    iterations: u64,
}

type FloatLoopFunction = unsafe extern "C" fn(u64, u64, u64, u64, *mut RawFloatLoopOutcome) -> u32;

#[derive(Debug)]
pub struct FloatLoopError(String);

impl Display for FloatLoopError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for FloatLoopError {}

#[derive(Debug, Eq, PartialEq)]
pub enum FloatLoopOutcome {
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

/// A generated `float add; float less-than; branch` loop.
///
/// It owns its executable allocation and touches only the immediate Mica value
/// words supplied at its boundary. Mixed numeric operands, non-finite results,
/// and invalid input values side exit to the interpreter.
pub struct CompiledFloatLoop {
    _module: Mutex<JITModule>,
    function: FloatLoopFunction,
    code_size: usize,
    imported_helper_count: usize,
    value_abi_version: u32,
}

impl CompiledFloatLoop {
    pub fn compile() -> Result<Self, FloatLoopError> {
        let builder = JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())
            .map_err(|error| FloatLoopError(format!("could not initialize Cranelift: {error}")))?;
        let mut module = JITModule::new(builder);
        let pointer_type = module.target_config().pointer_type();
        let mut signature = Signature::new(module.isa().default_call_conv());
        for _ in 0..4 {
            signature.params.push(AbiParam::new(types::I64));
        }
        signature.params.push(AbiParam::new(pointer_type));
        signature.returns.push(AbiParam::new(types::I32));

        let function_id = module
            .declare_function("mica_float_loop", Linkage::Local, &signature)
            .map_err(|error| FloatLoopError(format!("could not declare float loop: {error}")))?;
        let mut context = Context::new();
        context.func.signature = signature;
        let mut builder_context = FunctionBuilderContext::new();
        Self::build_function(&mut context, &mut builder_context);
        let imported_helper_count = context.func.dfg.ext_funcs.len();
        module
            .define_function(function_id, &mut context)
            .map_err(|error| FloatLoopError(format!("could not compile float loop: {error}")))?;
        let code_size = context
            .compiled_code()
            .map(|code| code.code_info().total_size as usize)
            .ok_or_else(|| FloatLoopError("Cranelift did not retain compiled code".to_owned()))?;
        module
            .finalize_definitions()
            .map_err(|error| FloatLoopError(format!("could not finalize float loop: {error}")))?;
        let code = module.get_finalized_function(function_id);
        let function = unsafe { std::mem::transmute::<*const u8, FloatLoopFunction>(code) };

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
        let dispatch = builder.create_block();
        let complete = builder.create_block();
        let budget_exhausted = builder.create_block();
        let side_exit = builder.create_block();
        builder.append_block_params_for_function_params(entry);
        for block in [loop_header, loop_body, after_add] {
            builder.append_block_param(block, types::I64);
            builder.append_block_param(block, types::I64);
        }
        for _ in 0..3 {
            builder.append_block_param(dispatch, types::I64);
        }
        for block in [complete, budget_exhausted] {
            builder.append_block_param(block, types::I64);
            builder.append_block_param(block, types::I64);
            builder.append_block_param(block, types::I64);
        }

        builder.switch_to_block(entry);
        let params = builder.block_params(entry).to_vec();
        let start = params[0];
        let step = params[1];
        let limit = params[2];
        let max_iterations = params[3];
        let output = params[4];
        let start_is_float = ValueEmitter::emit_is_float(&mut builder, start);
        let step_is_float = ValueEmitter::emit_is_float(&mut builder, step);
        let limit_is_float = ValueEmitter::emit_is_float(&mut builder, limit);
        let inputs_are_float = builder.ins().band(start_is_float, step_is_float);
        let inputs_are_float = builder.ins().band(inputs_are_float, limit_is_float);
        let zero = builder.ins().iconst(types::I64, 0);
        builder.ins().brif(
            inputs_are_float,
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
        let true_value = builder.ins().iconst(types::I8, 1);
        let true_word = ValueEmitter::emit_bool(&mut builder, true_value);
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
        let next = ValueEmitter::emit_checked_float_add(&mut builder, current, step);
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
        let comparison = ValueEmitter::emit_checked_float_compare(
            &mut builder,
            current,
            limit,
            FloatComparison::LessThan,
        );
        builder.ins().brif(
            comparison.is_fast(),
            dispatch,
            &[current.into(), comparison.word().into(), iterations.into()],
            side_exit,
            &[],
        );

        builder.switch_to_block(dispatch);
        let current = builder.block_params(dispatch)[0];
        let condition = builder.block_params(dispatch)[1];
        let iterations = builder.block_params(dispatch)[2];
        let keep_looping = ValueEmitter::emit_payload(&mut builder, condition);
        let keep_looping = builder.ins().icmp_imm(IntCC::NotEqual, keep_looping, 0);
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
    ) -> FloatLoopOutcome {
        if self.value_abi_version != VALUE_ABI_VERSION {
            return FloatLoopOutcome::SideExit;
        }
        let nothing = pack_value(VALUE_NOTHING_TAG, 0);
        let mut output = RawFloatLoopOutcome {
            current: nothing,
            condition: nothing,
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
            STATUS_COMPLETE => FloatLoopOutcome::Complete {
                current: unsafe { from_owned_value_bits(output.current) },
                condition: unsafe { from_owned_value_bits(output.condition) },
                iterations: output.iterations,
            },
            STATUS_BUDGET_EXHAUSTED => FloatLoopOutcome::BudgetExhausted {
                current: unsafe { from_owned_value_bits(output.current) },
                condition: unsafe { from_owned_value_bits(output.condition) },
                iterations: output.iterations,
            },
            STATUS_SIDE_EXIT => FloatLoopOutcome::SideExit,
            status => panic!("generated float loop returned unknown status {status}"),
        }
    }
}
