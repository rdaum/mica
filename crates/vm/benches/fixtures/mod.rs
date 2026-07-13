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

use mica_relation_kernel::{
    DispatchRead, KernelError, RelationId, RelationRead, RelationWorkspace, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use mica_vm::{
    AuthorityContext, Instruction, Operand, Program, Register, RuntimeBinaryOp, RuntimeError,
    VmHost,
};
use std::sync::Arc;

pub const INTEGER_LOOP_ITERATIONS: usize = 16_384;
pub const INTEGER_LOOP_INSTRUCTIONS: u64 = (INTEGER_LOOP_ITERATIONS as u64 * 3) + 4;
pub const STATIC_CALL_COUNT: usize = 8_192;
pub const STATIC_CALL_INSTRUCTIONS: u64 = (STATIC_CALL_COUNT as u64 * 2) + 1;
pub const BUILTIN_CALL_COUNT: usize = 16_384;
pub const BUILTIN_CALL_INSTRUCTIONS: u64 = BUILTIN_CALL_COUNT as u64 + 1;
pub const MAX_CALL_DEPTH: usize = 64;

pub struct ProgramFixture {
    pub program: Arc<Program>,
    pub instruction_count: u64,
}

pub fn integer_loop_fixture() -> ProgramFixture {
    let program = Program::new(
        4,
        [
            Instruction::Load {
                dst: reg(0),
                value: int(0),
            },
            Instruction::Load {
                dst: reg(1),
                value: int(1),
            },
            Instruction::Load {
                dst: reg(2),
                value: int(INTEGER_LOOP_ITERATIONS as i64),
            },
            Instruction::Binary {
                dst: reg(0),
                op: RuntimeBinaryOp::Add,
                left: reg(0),
                right: reg(1),
            },
            Instruction::Binary {
                dst: reg(3),
                op: RuntimeBinaryOp::Lt,
                left: reg(0),
                right: reg(2),
            },
            Instruction::Branch {
                condition: reg(3),
                if_true: 3,
                if_false: 6,
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();
    ProgramFixture {
        program: Arc::new(program),
        instruction_count: INTEGER_LOOP_INSTRUCTIONS,
    }
}

pub fn static_call_fixture() -> ProgramFixture {
    let callee = Arc::new(Program::new(1, [Instruction::Return { value: r(0) }]).unwrap());
    let mut instructions = Vec::with_capacity(STATIC_CALL_COUNT + 1);
    for index in 0..STATIC_CALL_COUNT {
        instructions.push(Instruction::Call {
            dst: reg(0),
            program: Arc::clone(&callee),
            args: vec![Operand::Value(int(index as i64))],
        });
    }
    instructions.push(Instruction::Return { value: r(0) });
    let program = Program::new(1, instructions).unwrap();
    ProgramFixture {
        program: Arc::new(program),
        instruction_count: STATIC_CALL_INSTRUCTIONS,
    }
}

pub fn builtin_call_fixture() -> ProgramFixture {
    let name = Symbol::intern("benchmark_noop");
    let mut instructions = Vec::with_capacity(BUILTIN_CALL_COUNT + 1);
    for _ in 0..BUILTIN_CALL_COUNT {
        instructions.push(Instruction::BuiltinCall {
            dst: reg(0),
            name,
            args: Vec::new(),
        });
    }
    instructions.push(Instruction::Return { value: r(0) });
    let program = Program::new(1, instructions).unwrap();
    ProgramFixture {
        program: Arc::new(program),
        instruction_count: BUILTIN_CALL_INSTRUCTIONS,
    }
}

#[derive(Default)]
pub struct BenchmarkHost {
    authority: AuthorityContext,
    builtin_calls: u64,
}

impl BenchmarkHost {
    pub fn builtin_calls(&self) -> u64 {
        self.builtin_calls
    }
}

impl RelationRead for BenchmarkHost {
    fn scan_relation(
        &self,
        _relation: RelationId,
        _bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        panic!("VM core benchmark unexpectedly scanned a relation")
    }
}

impl RelationWorkspace for BenchmarkHost {
    fn assert_tuple(&mut self, _relation: RelationId, _tuple: Tuple) -> Result<(), KernelError> {
        panic!("VM core benchmark unexpectedly asserted a tuple")
    }

    fn retract_tuple(&mut self, _relation: RelationId, _tuple: Tuple) -> Result<(), KernelError> {
        panic!("VM core benchmark unexpectedly retracted a tuple")
    }

    fn replace_functional_tuple(
        &mut self,
        _relation: RelationId,
        _tuple: Tuple,
    ) -> Result<(), KernelError> {
        panic!("VM core benchmark unexpectedly replaced a tuple")
    }
}

impl DispatchRead for BenchmarkHost {}

impl VmHost for BenchmarkHost {
    fn authority(&self) -> &AuthorityContext {
        &self.authority
    }

    fn authority_mut(&mut self) -> &mut AuthorityContext {
        &mut self.authority
    }

    fn emit(&mut self, _target: Identity, _value: Value) -> Result<(), RuntimeError> {
        panic!("VM core benchmark unexpectedly emitted a value")
    }

    fn validate_mailbox_receiver(&mut self, _receiver: &Value) -> Result<(), RuntimeError> {
        panic!("VM core benchmark unexpectedly validated a mailbox receiver")
    }

    fn resolve_program(
        &mut self,
        _program_bytes_relation: RelationId,
        _program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError> {
        panic!("VM core benchmark unexpectedly resolved a program")
    }

    fn call_builtin(&mut self, name: Symbol, args: &[Value]) -> Result<Value, RuntimeError> {
        debug_assert_eq!(name, Symbol::intern("benchmark_noop"));
        debug_assert!(args.is_empty());
        self.builtin_calls += 1;
        Ok(Value::nothing())
    }
}

fn reg(index: u16) -> Register {
    Register(index)
}

fn r(index: u16) -> Operand {
    Operand::Register(reg(index))
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}
