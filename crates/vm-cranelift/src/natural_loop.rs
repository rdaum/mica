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

use crate::{IntegerComparison, ValueEmitter};
use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Signature, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use mica_var::abi::{VALUE_ABI_VERSION, value_is_immediate};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

const STATUS_COMPLETE: u32 = 0;
const STATUS_BUDGET_EXHAUSTED: u32 = 1;
const STATUS_SIDE_EXIT: u32 = 2;

type NaturalLoopFunction = unsafe extern "C" fn(*mut u64, u64, *mut u64) -> u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NaturalLoopInstruction {
    Load {
        dst: u16,
        value: u64,
    },
    Move {
        dst: u16,
        src: u16,
    },
    Add {
        dst: u16,
        left: u16,
        right: u16,
    },
    Subtract {
        dst: u16,
        left: u16,
        right: u16,
    },
    Multiply {
        dst: u16,
        left: u16,
        right: u16,
    },
    Compare {
        dst: u16,
        comparison: IntegerComparison,
        left: u16,
        right: u16,
    },
}

impl NaturalLoopInstruction {
    fn dst(self) -> u16 {
        match self {
            Self::Load { dst, .. }
            | Self::Move { dst, .. }
            | Self::Add { dst, .. }
            | Self::Subtract { dst, .. }
            | Self::Multiply { dst, .. }
            | Self::Compare { dst, .. } => dst,
        }
    }

    fn slots(self) -> [Option<u16>; 3] {
        match self {
            Self::Load { dst, .. } => [Some(dst), None, None],
            Self::Move { dst, src } => [Some(dst), Some(src), None],
            Self::Add { dst, left, right }
            | Self::Subtract { dst, left, right }
            | Self::Multiply { dst, left, right }
            | Self::Compare {
                dst, left, right, ..
            } => [Some(dst), Some(left), Some(right)],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NaturalLoopPlan {
    slot_count: u16,
    condition: u16,
    body: Box<[NaturalLoopInstruction]>,
    header: Box<[NaturalLoopInstruction]>,
    modified_slots: Box<[u16]>,
}

impl NaturalLoopPlan {
    pub fn new(
        slot_count: u16,
        condition: u16,
        body: impl Into<Box<[NaturalLoopInstruction]>>,
        header: impl Into<Box<[NaturalLoopInstruction]>>,
    ) -> Result<Self, NaturalLoopError> {
        let body = body.into();
        let header = header.into();
        if slot_count == 0 || condition >= slot_count {
            return Err(NaturalLoopError(
                "natural loop has an invalid slot layout".to_owned(),
            ));
        }
        if body.is_empty() || header.is_empty() {
            return Err(NaturalLoopError(
                "natural loop body and header must not be empty".to_owned(),
            ));
        }

        let mut immediate = vec![false; usize::from(slot_count)];
        let mut modified = vec![false; usize::from(slot_count)];
        let mut condition_is_comparison = false;
        for instruction in body.iter().chain(header.iter()) {
            for slot in instruction.slots().into_iter().flatten() {
                if slot >= slot_count {
                    return Err(NaturalLoopError(
                        "natural loop instruction references an invalid slot".to_owned(),
                    ));
                }
            }
            match *instruction {
                NaturalLoopInstruction::Load { dst, value } => {
                    if !value_is_immediate(value) {
                        return Err(NaturalLoopError(
                            "natural loop constants must use immediate values".to_owned(),
                        ));
                    }
                    immediate[usize::from(dst)] = true;
                }
                NaturalLoopInstruction::Move { dst, src } => {
                    if !immediate[usize::from(src)] {
                        return Err(NaturalLoopError(
                            "natural loop moves must consume a checked immediate result".to_owned(),
                        ));
                    }
                    immediate[usize::from(dst)] = true;
                }
                NaturalLoopInstruction::Add { dst, .. }
                | NaturalLoopInstruction::Subtract { dst, .. }
                | NaturalLoopInstruction::Multiply { dst, .. }
                | NaturalLoopInstruction::Compare { dst, .. } => {
                    immediate[usize::from(dst)] = true;
                }
            }
            let dst = instruction.dst();
            modified[usize::from(dst)] = true;
            if dst == condition {
                condition_is_comparison =
                    matches!(instruction, NaturalLoopInstruction::Compare { .. });
            }
        }
        if !condition_is_comparison {
            return Err(NaturalLoopError(
                "natural loop condition must be produced by an integer comparison".to_owned(),
            ));
        }
        let modified_slots = modified
            .into_iter()
            .enumerate()
            .filter_map(|(slot, modified)| modified.then_some(slot as u16))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            slot_count,
            condition,
            body,
            header,
            modified_slots,
        })
    }

    pub const fn slot_count(&self) -> u16 {
        self.slot_count
    }

    pub fn modified_slots(&self) -> &[u16] {
        &self.modified_slots
    }
}

#[derive(Debug)]
pub struct NaturalLoopError(String);

impl Display for NaturalLoopError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for NaturalLoopError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NaturalLoopOutcome {
    Complete { iterations: u64 },
    BudgetExhausted { iterations: u64 },
    SideExit,
}

/// Generated execution for a compiler-shaped integer loop.
///
/// The caller supplies scratch value words. Generated code may mutate that
/// scratch before a side exit, but never touches VM-owned values directly.
pub struct CompiledNaturalLoop {
    _module: Mutex<JITModule>,
    function: NaturalLoopFunction,
    slot_count: usize,
    code_size: usize,
    imported_helper_count: usize,
    value_abi_version: u32,
}

impl CompiledNaturalLoop {
    pub fn compile(plan: &NaturalLoopPlan) -> Result<Self, NaturalLoopError> {
        let builder = JITBuilder::with_flags(&[("opt_level", "speed")], default_libcall_names())
            .map_err(|error| {
                NaturalLoopError(format!("could not initialize Cranelift: {error}"))
            })?;
        let mut module = JITModule::new(builder);
        let pointer_type = module.target_config().pointer_type();
        let mut signature = Signature::new(module.isa().default_call_conv());
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(pointer_type));
        signature.returns.push(AbiParam::new(types::I32));

        let function_id = module
            .declare_function("mica_natural_integer_loop", Linkage::Local, &signature)
            .map_err(|error| {
                NaturalLoopError(format!("could not declare natural loop: {error}"))
            })?;
        let mut context = Context::new();
        context.func.signature = signature;
        let mut builder_context = FunctionBuilderContext::new();
        Self::build_function(plan, &mut context, &mut builder_context);
        let imported_helper_count = context.func.dfg.ext_funcs.len();
        module
            .define_function(function_id, &mut context)
            .map_err(|error| {
                NaturalLoopError(format!("could not compile natural loop: {error}"))
            })?;
        let code_size = context
            .compiled_code()
            .map(|code| code.code_info().total_size as usize)
            .ok_or_else(|| NaturalLoopError("Cranelift did not retain compiled code".to_owned()))?;
        module.finalize_definitions().map_err(|error| {
            NaturalLoopError(format!("could not finalize natural loop: {error}"))
        })?;
        let code = module.get_finalized_function(function_id);
        let function = unsafe { std::mem::transmute::<*const u8, NaturalLoopFunction>(code) };
        Ok(Self {
            _module: Mutex::new(module),
            function,
            slot_count: usize::from(plan.slot_count),
            code_size,
            imported_helper_count,
            value_abi_version: VALUE_ABI_VERSION,
        })
    }

    fn build_function(
        plan: &NaturalLoopPlan,
        context: &mut Context,
        builder_context: &mut FunctionBuilderContext,
    ) {
        let mut builder = FunctionBuilder::new(&mut context.func, builder_context);
        let entry = builder.create_block();
        let loop_header = builder.create_block();
        let cycle = builder.create_block();
        let dispatch = builder.create_block();
        let complete = builder.create_block();
        let budget_exhausted = builder.create_block();
        let side_exit = builder.create_block();

        builder.append_block_params_for_function_params(entry);
        builder.append_block_param(loop_header, types::I64);
        builder.append_block_param(cycle, types::I64);
        builder.append_block_param(dispatch, types::I64);
        builder.append_block_param(dispatch, types::I8);
        builder.append_block_param(complete, types::I64);
        builder.append_block_param(budget_exhausted, types::I64);

        builder.switch_to_block(entry);
        let params = builder.block_params(entry).to_vec();
        let scratch = params[0];
        let max_iterations = params[1];
        let iterations_out = params[2];
        let zero = builder.ins().iconst(types::I64, 0);
        builder.ins().jump(loop_header, &[zero.into()]);

        builder.switch_to_block(loop_header);
        let iterations = builder.block_params(loop_header)[0];
        let has_budget = builder
            .ins()
            .icmp(IntCC::UnsignedLessThan, iterations, max_iterations);
        builder.ins().brif(
            has_budget,
            cycle,
            &[iterations.into()],
            budget_exhausted,
            &[iterations.into()],
        );

        builder.switch_to_block(cycle);
        let iterations = builder.block_params(cycle)[0];
        let mut is_fast = builder.ins().iconst(types::I8, 1);
        for instruction in plan.body.iter().chain(plan.header.iter()) {
            is_fast = Self::emit_instruction(&mut builder, scratch, *instruction, is_fast);
        }
        let one = builder.ins().iconst(types::I64, 1);
        let iterations = builder.ins().iadd(iterations, one);
        let condition = Self::load_slot(&mut builder, scratch, plan.condition);
        let condition = ValueEmitter::emit_payload(&mut builder, condition);
        let keep_looping = builder.ins().icmp_imm(IntCC::NotEqual, condition, 0);
        builder.ins().brif(
            is_fast,
            dispatch,
            &[iterations.into(), keep_looping.into()],
            side_exit,
            &[],
        );

        builder.switch_to_block(dispatch);
        let iterations = builder.block_params(dispatch)[0];
        let keep_looping = builder.block_params(dispatch)[1];
        builder.ins().brif(
            keep_looping,
            loop_header,
            &[iterations.into()],
            complete,
            &[iterations.into()],
        );

        builder.switch_to_block(complete);
        let iterations = builder.block_params(complete)[0];
        builder
            .ins()
            .store(MemFlagsData::new(), iterations, iterations_out, 0);
        let status = builder.ins().iconst(types::I32, i64::from(STATUS_COMPLETE));
        builder.ins().return_(&[status]);

        builder.switch_to_block(budget_exhausted);
        let iterations = builder.block_params(budget_exhausted)[0];
        builder
            .ins()
            .store(MemFlagsData::new(), iterations, iterations_out, 0);
        let status = builder
            .ins()
            .iconst(types::I32, i64::from(STATUS_BUDGET_EXHAUSTED));
        builder.ins().return_(&[status]);

        builder.switch_to_block(side_exit);
        let status = builder
            .ins()
            .iconst(types::I32, i64::from(STATUS_SIDE_EXIT));
        builder.ins().return_(&[status]);

        builder.seal_all_blocks();
        builder.finalize();
    }

    fn emit_instruction(
        builder: &mut FunctionBuilder<'_>,
        scratch: cranelift_codegen::ir::Value,
        instruction: NaturalLoopInstruction,
        is_fast: cranelift_codegen::ir::Value,
    ) -> cranelift_codegen::ir::Value {
        match instruction {
            NaturalLoopInstruction::Load { dst, value } => {
                let value = builder.ins().iconst(types::I64, value as i64);
                Self::store_slot(builder, scratch, dst, value);
                is_fast
            }
            NaturalLoopInstruction::Move { dst, src } => {
                let value = Self::load_slot(builder, scratch, src);
                Self::store_slot(builder, scratch, dst, value);
                is_fast
            }
            NaturalLoopInstruction::Add { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_add(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                builder.ins().band(is_fast, result.is_fast())
            }
            NaturalLoopInstruction::Subtract { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_sub(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                builder.ins().band(is_fast, result.is_fast())
            }
            NaturalLoopInstruction::Multiply { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_mul(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                builder.ins().band(is_fast, result.is_fast())
            }
            NaturalLoopInstruction::Compare {
                dst,
                comparison,
                left,
                right,
            } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result =
                    ValueEmitter::emit_checked_int_compare(builder, left, right, comparison);
                Self::store_slot(builder, scratch, dst, result.word());
                builder.ins().band(is_fast, result.is_fast())
            }
        }
    }

    fn load_slot(
        builder: &mut FunctionBuilder<'_>,
        scratch: cranelift_codegen::ir::Value,
        slot: u16,
    ) -> cranelift_codegen::ir::Value {
        builder.ins().load(
            types::I64,
            MemFlagsData::new(),
            scratch,
            i32::from(slot) * 8,
        )
    }

    fn store_slot(
        builder: &mut FunctionBuilder<'_>,
        scratch: cranelift_codegen::ir::Value,
        slot: u16,
        value: cranelift_codegen::ir::Value,
    ) {
        builder
            .ins()
            .store(MemFlagsData::new(), value, scratch, i32::from(slot) * 8);
    }

    pub const fn code_size(&self) -> usize {
        self.code_size
    }

    pub const fn imported_helper_count(&self) -> usize {
        self.imported_helper_count
    }

    pub fn run(&self, scratch: &mut [u64], max_iterations: u64) -> NaturalLoopOutcome {
        if self.value_abi_version != VALUE_ABI_VERSION || scratch.len() < self.slot_count {
            return NaturalLoopOutcome::SideExit;
        }
        let mut iterations = 0;
        let status =
            unsafe { (self.function)(scratch.as_mut_ptr(), max_iterations, &mut iterations) };
        match status {
            STATUS_COMPLETE => NaturalLoopOutcome::Complete { iterations },
            STATUS_BUDGET_EXHAUSTED => NaturalLoopOutcome::BudgetExhausted { iterations },
            STATUS_SIDE_EXIT => NaturalLoopOutcome::SideExit,
            status => panic!("generated natural loop returned unknown status {status}"),
        }
    }
}
