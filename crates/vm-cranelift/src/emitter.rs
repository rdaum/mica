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

use cranelift_codegen::ir::{InstBuilder, Value as CraneliftValue, condcodes::IntCC, types};
use cranelift_frontend::FunctionBuilder;
use mica_var::abi::{
    VALUE_BOOL_TAG, VALUE_CAPABILITY_TAG, VALUE_FUNCTION_TAG, VALUE_INT_MAX, VALUE_INT_MIN,
    VALUE_INT_TAG, VALUE_LIST_TAG, VALUE_NOTHING_TAG, VALUE_PAYLOAD_MASK, VALUE_STRING_TAG,
    VALUE_TAG_SHIFT,
};

/// A generated operation result and a predicate indicating whether it completed
/// without leaving the native immediate-value path.
#[derive(Clone, Copy, Debug)]
pub struct EmittedValue {
    word: CraneliftValue,
    is_fast: CraneliftValue,
}

impl EmittedValue {
    pub const fn word(self) -> CraneliftValue {
        self.word
    }

    pub const fn is_fast(self) -> CraneliftValue {
        self.is_fast
    }
}

/// Emits operations over the process-local [`mica_var::Value`] word format.
pub struct ValueEmitter;

impl ValueEmitter {
    pub fn emit_tag(builder: &mut FunctionBuilder<'_>, word: CraneliftValue) -> CraneliftValue {
        builder.ins().ushr_imm(word, VALUE_TAG_SHIFT as i64)
    }

    pub fn emit_payload(builder: &mut FunctionBuilder<'_>, word: CraneliftValue) -> CraneliftValue {
        let mask = builder.ins().iconst(types::I64, VALUE_PAYLOAD_MASK as i64);
        builder.ins().band(word, mask)
    }

    pub fn emit_is_int(builder: &mut FunctionBuilder<'_>, word: CraneliftValue) -> CraneliftValue {
        let tag = Self::emit_tag(builder, word);
        builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_INT_TAG))
    }

    pub fn emit_is_immediate(
        builder: &mut FunctionBuilder<'_>,
        word: CraneliftValue,
    ) -> CraneliftValue {
        let tag = Self::emit_tag(builder, word);
        let before_heap =
            builder
                .ins()
                .icmp_imm(IntCC::UnsignedLessThan, tag, i64::from(VALUE_STRING_TAG));
        let capability = builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_CAPABILITY_TAG));
        let function = builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_FUNCTION_TAG));
        let before_heap_or_capability = builder.ins().bor(before_heap, capability);
        builder.ins().bor(before_heap_or_capability, function)
    }

    pub fn emit_pack(
        builder: &mut FunctionBuilder<'_>,
        tag: u8,
        payload: CraneliftValue,
    ) -> CraneliftValue {
        let masked = Self::emit_payload(builder, payload);
        let tag = builder
            .ins()
            .iconst(types::I64, i64::from(tag) << VALUE_TAG_SHIFT);
        builder.ins().bor(tag, masked)
    }

    pub fn emit_bool(
        builder: &mut FunctionBuilder<'_>,
        predicate: CraneliftValue,
    ) -> CraneliftValue {
        let payload = builder.ins().uextend(types::I64, predicate);
        Self::emit_pack(builder, VALUE_BOOL_TAG, payload)
    }

    pub fn emit_unbox_int(
        builder: &mut FunctionBuilder<'_>,
        word: CraneliftValue,
    ) -> CraneliftValue {
        let shifted = builder.ins().ishl_imm(word, 64 - VALUE_TAG_SHIFT as i64);
        builder.ins().sshr_imm(shifted, 64 - VALUE_TAG_SHIFT as i64)
    }

    pub fn emit_checked_int_add(
        builder: &mut FunctionBuilder<'_>,
        left: CraneliftValue,
        right: CraneliftValue,
    ) -> EmittedValue {
        let left_is_int = Self::emit_is_int(builder, left);
        let right_is_int = Self::emit_is_int(builder, right);
        let operands_are_ints = builder.ins().band(left_is_int, right_is_int);
        let left = Self::emit_unbox_int(builder, left);
        let right = Self::emit_unbox_int(builder, right);
        let sum = builder.ins().iadd(left, right);
        let above_min = builder
            .ins()
            .icmp_imm(IntCC::SignedGreaterThanOrEqual, sum, VALUE_INT_MIN);
        let below_max = builder
            .ins()
            .icmp_imm(IntCC::SignedLessThanOrEqual, sum, VALUE_INT_MAX);
        let in_range = builder.ins().band(above_min, below_max);
        let is_fast = builder.ins().band(operands_are_ints, in_range);

        EmittedValue {
            word: Self::emit_pack(builder, VALUE_INT_TAG, sum),
            is_fast,
        }
    }

    pub fn emit_checked_int_lt(
        builder: &mut FunctionBuilder<'_>,
        left: CraneliftValue,
        right: CraneliftValue,
    ) -> EmittedValue {
        let left_is_int = Self::emit_is_int(builder, left);
        let right_is_int = Self::emit_is_int(builder, right);
        let is_fast = builder.ins().band(left_is_int, right_is_int);
        let left = Self::emit_unbox_int(builder, left);
        let right = Self::emit_unbox_int(builder, right);
        let less = builder.ins().icmp(IntCC::SignedLessThan, left, right);

        EmittedValue {
            word: Self::emit_bool(builder, less),
            is_fast,
        }
    }

    pub fn emit_immediate_eq(
        builder: &mut FunctionBuilder<'_>,
        left: CraneliftValue,
        right: CraneliftValue,
    ) -> EmittedValue {
        let left_is_immediate = Self::emit_is_immediate(builder, left);
        let right_is_immediate = Self::emit_is_immediate(builder, right);
        let is_fast = builder.ins().band(left_is_immediate, right_is_immediate);
        let equal = builder.ins().icmp(IntCC::Equal, left, right);

        EmittedValue {
            word: Self::emit_bool(builder, equal),
            is_fast,
        }
    }

    pub fn emit_truthy(builder: &mut FunctionBuilder<'_>, word: CraneliftValue) -> EmittedValue {
        let tag = Self::emit_tag(builder, word);
        let payload = Self::emit_payload(builder, word);
        let is_nothing = builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_NOTHING_TAG));
        let is_bool = builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_BOOL_TAG));
        let is_list = builder
            .ins()
            .icmp_imm(IntCC::Equal, tag, i64::from(VALUE_LIST_TAG));
        let bool_payload = builder.ins().icmp_imm(IntCC::NotEqual, payload, 0);
        let truth = builder.ins().iconst(types::I8, 1);
        let false_value = builder.ins().iconst(types::I8, 0);
        let truth = builder.ins().select(is_bool, bool_payload, truth);
        let truth = builder.ins().select(is_nothing, false_value, truth);
        let is_fast = builder.ins().icmp_imm(IntCC::Equal, is_list, 0);

        EmittedValue {
            word: Self::emit_bool(builder, truth),
            is_fast,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EmittedValue, ValueEmitter};
    use cranelift_codegen::Context;
    use cranelift_codegen::ir::{
        AbiParam, InstBuilder, MemFlagsData, Signature, Value as CraneliftValue, types,
    };
    use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
    use cranelift_jit::{JITBuilder, JITModule};
    use cranelift_module::{Linkage, Module, default_libcall_names};
    use mica_var::Value;
    use mica_var::abi::{
        VALUE_NOTHING_TAG, borrowed_value_bits, from_owned_value_bits, pack_value,
    };

    const STATUS_COMPLETE: u32 = 0;
    const STATUS_SIDE_EXIT: u32 = 1;

    type ProbeFunction = unsafe extern "C" fn(u64, u64, *mut u64) -> u32;

    struct Probe {
        _module: JITModule,
        function: ProbeFunction,
    }

    impl Probe {
        fn compile(
            operation: impl FnOnce(
                &mut FunctionBuilder<'_>,
                CraneliftValue,
                CraneliftValue,
            ) -> EmittedValue,
        ) -> Self {
            let builder = JITBuilder::new(default_libcall_names()).unwrap();
            let mut module = JITModule::new(builder);
            let pointer_type = module.target_config().pointer_type();
            let mut signature = Signature::new(module.isa().default_call_conv());
            signature.params.push(AbiParam::new(types::I64));
            signature.params.push(AbiParam::new(types::I64));
            signature.params.push(AbiParam::new(pointer_type));
            signature.returns.push(AbiParam::new(types::I32));

            let function_id = module
                .declare_function("value_probe", Linkage::Local, &signature)
                .unwrap();
            let mut context = Context::new();
            context.func.signature = signature;
            let mut builder_context = FunctionBuilderContext::new();
            let mut function_builder =
                FunctionBuilder::new(&mut context.func, &mut builder_context);
            let entry = function_builder.create_block();
            let complete = function_builder.create_block();
            let side_exit = function_builder.create_block();
            function_builder.append_block_params_for_function_params(entry);
            function_builder.append_block_param(complete, types::I64);

            function_builder.switch_to_block(entry);
            let params = function_builder.block_params(entry).to_vec();
            let result = operation(&mut function_builder, params[0], params[1]);
            function_builder.ins().brif(
                result.is_fast(),
                complete,
                &[result.word().into()],
                side_exit,
                &[],
            );

            function_builder.switch_to_block(complete);
            let result = function_builder.block_params(complete)[0];
            function_builder
                .ins()
                .store(MemFlagsData::new(), result, params[2], 0);
            let status = function_builder
                .ins()
                .iconst(types::I32, i64::from(STATUS_COMPLETE));
            function_builder.ins().return_(&[status]);

            function_builder.switch_to_block(side_exit);
            let status = function_builder
                .ins()
                .iconst(types::I32, i64::from(STATUS_SIDE_EXIT));
            function_builder.ins().return_(&[status]);
            function_builder.seal_all_blocks();
            function_builder.finalize();

            module.define_function(function_id, &mut context).unwrap();
            module.finalize_definitions().unwrap();
            let code = module.get_finalized_function(function_id);
            let function = unsafe { std::mem::transmute::<*const u8, ProbeFunction>(code) };
            Self {
                _module: module,
                function,
            }
        }

        fn run(&self, left: &Value, right: &Value) -> Option<Value> {
            let mut output = pack_value(VALUE_NOTHING_TAG, 0);
            let status = unsafe {
                (self.function)(
                    borrowed_value_bits(left),
                    borrowed_value_bits(right),
                    &mut output,
                )
            };
            match status {
                STATUS_COMPLETE => Some(unsafe { from_owned_value_bits(output) }),
                STATUS_SIDE_EXIT => None,
                status => panic!("value probe returned unknown status {status}"),
            }
        }
    }

    #[test]
    fn emitted_checked_integer_add_matches_value_arithmetic() {
        let probe = Probe::compile(ValueEmitter::emit_checked_int_add);
        for left in [-1_000, -1, 0, 1, 1_000] {
            for right in [-31, -1, 0, 1, 31] {
                let left = Value::int(left).unwrap();
                let right = Value::int(right).unwrap();
                assert_eq!(probe.run(&left, &right), left.checked_add(&right));
            }
        }
        assert_eq!(probe.run(&Value::float(1.0), &Value::int(1).unwrap()), None);
    }

    #[test]
    fn emitted_immediate_equality_matches_value_equality() {
        let probe = Probe::compile(ValueEmitter::emit_immediate_eq);
        let values = [
            Value::nothing(),
            Value::bool(false),
            Value::bool(true),
            Value::int(-1).unwrap(),
            Value::int(0).unwrap(),
            Value::float(1.5),
            Value::identity_raw(1).unwrap(),
            Value::function_raw(1).unwrap(),
        ];
        for left in &values {
            for right in &values {
                assert_eq!(probe.run(left, right), Some(Value::bool(left == right)));
            }
        }
        assert_eq!(probe.run(&Value::string("x"), &Value::string("x")), None);
    }

    #[test]
    fn emitted_integer_comparison_matches_value_ordering() {
        let probe = Probe::compile(ValueEmitter::emit_checked_int_lt);
        for left in [-1_000, -1, 0, 1, 1_000] {
            for right in [-31, -1, 0, 1, 31] {
                let left = Value::int(left).unwrap();
                let right = Value::int(right).unwrap();
                assert_eq!(probe.run(&left, &right), Some(Value::bool(left < right)));
            }
        }
        assert_eq!(probe.run(&Value::float(1.0), &Value::int(1).unwrap()), None);
    }

    #[test]
    fn emitted_truthiness_matches_vm_fast_cases() {
        let probe = Probe::compile(|builder, value, _| ValueEmitter::emit_truthy(builder, value));
        let cases = [
            (Value::nothing(), false),
            (Value::bool(false), false),
            (Value::bool(true), true),
            (Value::int(0).unwrap(), true),
            (Value::float(0.0), true),
            (Value::string(""), true),
        ];
        for (value, expected) in cases {
            assert_eq!(
                probe.run(&value, &Value::nothing()),
                Some(Value::bool(expected)),
            );
        }
        assert_eq!(probe.run(&Value::list([]), &Value::nothing()), None,);
    }
}
