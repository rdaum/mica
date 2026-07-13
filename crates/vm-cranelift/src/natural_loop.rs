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

use crate::{ScalarComparison, ValueEmitter};
use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, InstBuilder, MemFlagsData, Signature, condcodes::IntCC, types,
};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};
use mica_var::abi::{
    VALUE_ABI_VERSION, VALUE_INT_MAX, VALUE_INT_MIN, VALUE_INT_TAG, value_is_immediate,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

const STATUS_COMPLETE: u32 = 0;
const STATUS_BUDGET_EXHAUSTED: u32 = 1;
const STATUS_SIDE_EXIT: u32 = 2;

type NaturalLoopFunction = unsafe extern "C" fn(
    *mut u64,
    *const NaturalLoopRangeView,
    u64,
    *mut u64,
    *mut u32,
    *mut u32,
) -> u32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NaturalLoopRangeView {
    start: i64,
    end: i64,
}

impl NaturalLoopRangeView {
    pub fn new(start: i64, end: i64) -> Option<Self> {
        if !(VALUE_INT_MIN..=VALUE_INT_MAX).contains(&start)
            || !(VALUE_INT_MIN..=VALUE_INT_MAX).contains(&end)
        {
            return None;
        }
        Some(Self { start, end })
    }
}

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
    Negate {
        dst: u16,
        src: u16,
    },
    Not {
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
    Divide {
        dst: u16,
        left: u16,
        right: u16,
    },
    Remainder {
        dst: u16,
        left: u16,
        right: u16,
    },
    Compare {
        dst: u16,
        comparison: ScalarComparison,
        left: u16,
        right: u16,
    },
    RangeValueAt {
        dst: u16,
        view: u16,
        index: u16,
    },
    Branch {
        condition: u16,
        if_true: u16,
        if_false: u16,
    },
    Jump {
        target: u16,
    },
}

impl NaturalLoopInstruction {
    fn slots(self) -> [Option<u16>; 3] {
        match self {
            Self::Load { dst, .. } => [Some(dst), None, None],
            Self::Move { dst, src } | Self::Negate { dst, src } | Self::Not { dst, src } => {
                [Some(dst), Some(src), None]
            }
            Self::Add { dst, left, right }
            | Self::Subtract { dst, left, right }
            | Self::Multiply { dst, left, right }
            | Self::Divide { dst, left, right }
            | Self::Remainder { dst, left, right }
            | Self::Compare {
                dst, left, right, ..
            } => [Some(dst), Some(left), Some(right)],
            Self::RangeValueAt { dst, index, .. } => [Some(dst), Some(index), None],
            Self::Branch { condition, .. } => [Some(condition), None, None],
            Self::Jump { .. } => [None, None, None],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NaturalLoopPlan {
    slot_count: u16,
    range_view_count: u16,
    entry: u16,
    instructions: Box<[NaturalLoopInstruction]>,
}

impl NaturalLoopPlan {
    pub fn new(
        slot_count: u16,
        range_view_count: u16,
        entry: u16,
        instructions: impl Into<Box<[NaturalLoopInstruction]>>,
    ) -> Result<Self, NaturalLoopError> {
        let instructions = instructions.into();
        if slot_count == 0 || slot_count > 32 || usize::from(entry) >= instructions.len() {
            return Err(NaturalLoopError(
                "natural loop has an invalid slot layout".to_owned(),
            ));
        }
        if instructions.is_empty() || instructions.len() > usize::from(u16::MAX) {
            return Err(NaturalLoopError(
                "natural loop instruction layout is invalid".to_owned(),
            ));
        }

        let exit = u16::try_from(instructions.len()).expect("instruction length checked above");
        for instruction in &instructions {
            for slot in instruction.slots().into_iter().flatten() {
                if slot >= slot_count {
                    return Err(NaturalLoopError(
                        "natural loop instruction references an invalid slot".to_owned(),
                    ));
                }
            }
            match *instruction {
                NaturalLoopInstruction::Load { value, .. } if !value_is_immediate(value) => {
                    return Err(NaturalLoopError(
                        "natural loop constants must use immediate values".to_owned(),
                    ));
                }
                NaturalLoopInstruction::Branch {
                    if_true, if_false, ..
                } if if_true > exit || if_false > exit => {
                    return Err(NaturalLoopError(
                        "natural loop branch target is outside the compiled region".to_owned(),
                    ));
                }
                NaturalLoopInstruction::Jump { target } if target > exit => {
                    return Err(NaturalLoopError(
                        "natural loop jump target is outside the compiled region".to_owned(),
                    ));
                }
                NaturalLoopInstruction::RangeValueAt { view, .. } if view >= range_view_count => {
                    return Err(NaturalLoopError(
                        "natural loop range instruction references an invalid view".to_owned(),
                    ));
                }
                _ => {}
            }
        }
        Ok(Self {
            slot_count,
            range_view_count,
            entry,
            instructions,
        })
    }

    pub const fn slot_count(&self) -> u16 {
        self.slot_count
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
    Complete {
        instructions: u64,
        modified_slots: u32,
    },
    BudgetExhausted {
        instructions: u64,
        resume: u16,
        modified_slots: u32,
    },
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
    range_view_count: usize,
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
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(types::I64));
        signature.params.push(AbiParam::new(pointer_type));
        signature.params.push(AbiParam::new(pointer_type));
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
                NaturalLoopError(format!("could not compile natural loop: {error:?}"))
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
            range_view_count: usize::from(plan.range_view_count),
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
        let complete = builder.create_block();
        let budget_exhausted = builder.create_block();
        let side_exit = builder.create_block();
        let instruction_blocks = plan
            .instructions
            .iter()
            .map(|_| builder.create_block())
            .collect::<Vec<_>>();
        let execute_blocks = plan
            .instructions
            .iter()
            .map(|_| builder.create_block())
            .collect::<Vec<_>>();
        let branch_dispatch = plan
            .instructions
            .iter()
            .map(|instruction| {
                matches!(instruction, NaturalLoopInstruction::Branch { .. })
                    .then(|| builder.create_block())
            })
            .collect::<Vec<_>>();

        builder.append_block_params_for_function_params(entry);
        builder.append_block_param(complete, types::I64);
        builder.append_block_param(complete, types::I32);
        builder.append_block_param(budget_exhausted, types::I64);
        builder.append_block_param(budget_exhausted, types::I32);
        builder.append_block_param(budget_exhausted, types::I32);
        for block in instruction_blocks.iter().chain(execute_blocks.iter()) {
            builder.append_block_param(*block, types::I64);
            builder.append_block_param(*block, types::I32);
        }
        for block in branch_dispatch.iter().flatten() {
            builder.append_block_param(*block, types::I64);
            builder.append_block_param(*block, types::I32);
            builder.append_block_param(*block, types::I8);
        }

        builder.switch_to_block(entry);
        let params = builder.block_params(entry).to_vec();
        let scratch = params[0];
        let range_views = params[1];
        let instruction_budget = params[2];
        let instructions_out = params[3];
        let resume_out = params[4];
        let modified_slots_out = params[5];
        let zero_instructions = builder.ins().iconst(types::I64, 0);
        let zero_slots = builder.ins().iconst(types::I32, 0);
        builder.ins().jump(
            instruction_blocks[usize::from(plan.entry)],
            &[zero_instructions.into(), zero_slots.into()],
        );

        for (index, instruction) in plan.instructions.iter().copied().enumerate() {
            let instruction_block = instruction_blocks[index];
            let execute_block = execute_blocks[index];
            builder.switch_to_block(instruction_block);
            let instructions = builder.block_params(instruction_block)[0];
            let modified_slots = builder.block_params(instruction_block)[1];
            let has_budget =
                builder
                    .ins()
                    .icmp(IntCC::UnsignedLessThan, instructions, instruction_budget);
            let resume = builder.ins().iconst(types::I32, index as i64);
            builder.ins().brif(
                has_budget,
                execute_block,
                &[instructions.into(), modified_slots.into()],
                budget_exhausted,
                &[instructions.into(), resume.into(), modified_slots.into()],
            );

            builder.switch_to_block(execute_block);
            let instructions = builder.block_params(execute_block)[0];
            let modified_slots = builder.block_params(execute_block)[1];
            let one = builder.ins().iconst(types::I64, 1);
            let instructions = builder.ins().iadd(instructions, one);

            match instruction {
                NaturalLoopInstruction::Branch {
                    condition,
                    if_true,
                    if_false,
                } => {
                    let condition = Self::load_slot(&mut builder, scratch, condition);
                    let truth = ValueEmitter::emit_truthy(&mut builder, condition);
                    let truth_payload = ValueEmitter::emit_payload(&mut builder, truth.word());
                    let truth_value = builder.ins().icmp_imm(IntCC::NotEqual, truth_payload, 0);
                    let dispatch = branch_dispatch[index].expect("branch dispatch block");
                    builder.ins().brif(
                        truth.is_fast(),
                        dispatch,
                        &[
                            instructions.into(),
                            modified_slots.into(),
                            truth_value.into(),
                        ],
                        side_exit,
                        &[],
                    );

                    builder.switch_to_block(dispatch);
                    let instructions = builder.block_params(dispatch)[0];
                    let modified_slots = builder.block_params(dispatch)[1];
                    let truth_value = builder.block_params(dispatch)[2];
                    let if_true = Self::target_block(&instruction_blocks, complete, if_true);
                    let if_false = Self::target_block(&instruction_blocks, complete, if_false);
                    builder.ins().brif(
                        truth_value,
                        if_true,
                        &[instructions.into(), modified_slots.into()],
                        if_false,
                        &[instructions.into(), modified_slots.into()],
                    );
                }
                NaturalLoopInstruction::Jump { target } => {
                    let target = Self::target_block(&instruction_blocks, complete, target);
                    builder
                        .ins()
                        .jump(target, &[instructions.into(), modified_slots.into()]);
                }
                instruction => {
                    let (is_fast, dst) =
                        Self::emit_instruction(&mut builder, scratch, range_views, instruction);
                    let slot_bit = 1_u32 << dst;
                    let slot_bit = builder.ins().iconst(types::I32, i64::from(slot_bit));
                    let modified_slots = builder.ins().bor(modified_slots, slot_bit);
                    let next = Self::target_block(
                        &instruction_blocks,
                        complete,
                        u16::try_from(index + 1).expect("plan length validated"),
                    );
                    builder.ins().brif(
                        is_fast,
                        next,
                        &[instructions.into(), modified_slots.into()],
                        side_exit,
                        &[],
                    );
                }
            }
        }

        builder.switch_to_block(complete);
        let instructions = builder.block_params(complete)[0];
        let modified_slots = builder.block_params(complete)[1];
        builder
            .ins()
            .store(MemFlagsData::new(), instructions, instructions_out, 0);
        builder
            .ins()
            .store(MemFlagsData::new(), modified_slots, modified_slots_out, 0);
        let status = builder.ins().iconst(types::I32, i64::from(STATUS_COMPLETE));
        builder.ins().return_(&[status]);

        builder.switch_to_block(budget_exhausted);
        let instructions = builder.block_params(budget_exhausted)[0];
        let resume = builder.block_params(budget_exhausted)[1];
        let modified_slots = builder.block_params(budget_exhausted)[2];
        builder
            .ins()
            .store(MemFlagsData::new(), instructions, instructions_out, 0);
        builder
            .ins()
            .store(MemFlagsData::new(), resume, resume_out, 0);
        builder
            .ins()
            .store(MemFlagsData::new(), modified_slots, modified_slots_out, 0);
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

    fn target_block(
        instruction_blocks: &[cranelift_codegen::ir::Block],
        complete: cranelift_codegen::ir::Block,
        target: u16,
    ) -> cranelift_codegen::ir::Block {
        instruction_blocks
            .get(usize::from(target))
            .copied()
            .unwrap_or(complete)
    }

    fn emit_instruction(
        builder: &mut FunctionBuilder<'_>,
        scratch: cranelift_codegen::ir::Value,
        range_views: cranelift_codegen::ir::Value,
        instruction: NaturalLoopInstruction,
    ) -> (cranelift_codegen::ir::Value, u16) {
        match instruction {
            NaturalLoopInstruction::Load { dst, value } => {
                let value = builder.ins().iconst(types::I64, value as i64);
                Self::store_slot(builder, scratch, dst, value);
                let is_fast = builder.ins().iconst(types::I8, 1);
                (is_fast, dst)
            }
            NaturalLoopInstruction::Move { dst, src } => {
                let value = Self::load_slot(builder, scratch, src);
                Self::store_slot(builder, scratch, dst, value);
                (ValueEmitter::emit_is_immediate(builder, value), dst)
            }
            NaturalLoopInstruction::Negate { dst, src } => {
                let value = Self::load_slot(builder, scratch, src);
                let result = ValueEmitter::emit_checked_int_neg(builder, value);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Not { dst, src } => {
                let value = Self::load_slot(builder, scratch, src);
                let result = ValueEmitter::emit_not(builder, value);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Add { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_add(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Subtract { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_sub(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Multiply { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_mul(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Divide { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_div(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Remainder { dst, left, right } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_checked_int_rem(builder, left, right);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::Compare {
                dst,
                comparison,
                left,
                right,
            } => {
                let left = Self::load_slot(builder, scratch, left);
                let right = Self::load_slot(builder, scratch, right);
                let result = ValueEmitter::emit_scalar_compare(builder, left, right, comparison);
                Self::store_slot(builder, scratch, dst, result.word());
                (result.is_fast(), dst)
            }
            NaturalLoopInstruction::RangeValueAt { dst, view, index } => {
                let index = Self::load_slot(builder, scratch, index);
                let index_is_int = ValueEmitter::emit_is_int(builder, index);
                let index = ValueEmitter::emit_unbox_int(builder, index);
                let index_is_nonnegative =
                    builder
                        .ins()
                        .icmp_imm(IntCC::SignedGreaterThanOrEqual, index, 0);
                let view_offset = i32::from(view) * 16;
                let start =
                    builder
                        .ins()
                        .load(types::I64, MemFlagsData::new(), range_views, view_offset);
                let end = builder.ins().load(
                    types::I64,
                    MemFlagsData::new(),
                    range_views,
                    view_offset + 8,
                );
                let range_is_nonempty =
                    builder.ins().icmp(IntCC::SignedLessThanOrEqual, start, end);
                let value = builder.ins().iadd(start, index);
                let value_fits =
                    builder
                        .ins()
                        .icmp_imm(IntCC::SignedLessThanOrEqual, value, VALUE_INT_MAX);
                let value_in_range = builder.ins().icmp(IntCC::SignedLessThanOrEqual, value, end);
                let is_fast = builder.ins().band(index_is_int, index_is_nonnegative);
                let is_fast = builder.ins().band(is_fast, range_is_nonempty);
                let is_fast = builder.ins().band(is_fast, value_fits);
                let is_fast = builder.ins().band(is_fast, value_in_range);
                let value = ValueEmitter::emit_pack(builder, VALUE_INT_TAG, value);
                Self::store_slot(builder, scratch, dst, value);
                (is_fast, dst)
            }
            NaturalLoopInstruction::Branch { .. } | NaturalLoopInstruction::Jump { .. } => {
                unreachable!("control-flow instructions are emitted by build_function")
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

    pub fn run(
        &self,
        scratch: &mut [u64],
        range_views: &[NaturalLoopRangeView],
        instruction_budget: u64,
    ) -> NaturalLoopOutcome {
        if self.value_abi_version != VALUE_ABI_VERSION
            || scratch.len() < self.slot_count
            || range_views.len() < self.range_view_count
        {
            return NaturalLoopOutcome::SideExit;
        }
        let mut instructions = 0;
        let mut resume = 0;
        let mut modified_slots = 0;
        let status = unsafe {
            (self.function)(
                scratch.as_mut_ptr(),
                range_views.as_ptr(),
                instruction_budget,
                &mut instructions,
                &mut resume,
                &mut modified_slots,
            )
        };
        match status {
            STATUS_COMPLETE => NaturalLoopOutcome::Complete {
                instructions,
                modified_slots,
            },
            STATUS_BUDGET_EXHAUSTED => NaturalLoopOutcome::BudgetExhausted {
                instructions,
                resume: u16::try_from(resume).expect("compiled resume index fits u16"),
                modified_slots,
            },
            STATUS_SIDE_EXIT => NaturalLoopOutcome::SideExit,
            status => panic!("generated natural loop returned unknown status {status}"),
        }
    }
}
