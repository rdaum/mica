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

use crate::{
    AuthorityContext, Instruction, Operand, Program, Register, RegisterVm, RuntimeBinaryOp,
    RuntimeError, VmHost, VmHostResponse,
};
use mica_relation_kernel::{
    DispatchRead, KernelError, RelationId, RelationRead, RelationWorkspace, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::sync::{Arc, Barrier};

const ITERATIONS: usize = 16_384;
const INSTRUCTION_COUNT: usize = (ITERATIONS * 3) + 4;
const NATURAL_INSTRUCTION_COUNT: usize = (ITERATIONS * 9) + 7;
const MAX_CALL_DEPTH: usize = 8;

#[derive(Default)]
struct TestHost {
    authority: AuthorityContext,
}

impl RelationRead for TestHost {
    fn scan_relation(
        &self,
        _relation: RelationId,
        _bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        panic!("integer loop test unexpectedly scanned a relation")
    }
}

impl RelationWorkspace for TestHost {
    fn assert_tuple(&mut self, _relation: RelationId, _tuple: Tuple) -> Result<(), KernelError> {
        panic!("integer loop test unexpectedly asserted a tuple")
    }

    fn retract_tuple(&mut self, _relation: RelationId, _tuple: Tuple) -> Result<(), KernelError> {
        panic!("integer loop test unexpectedly retracted a tuple")
    }

    fn replace_functional_tuple(
        &mut self,
        _relation: RelationId,
        _tuple: Tuple,
    ) -> Result<(), KernelError> {
        panic!("integer loop test unexpectedly replaced a tuple")
    }
}

impl DispatchRead for TestHost {}

impl VmHost for TestHost {
    fn authority(&self) -> &AuthorityContext {
        &self.authority
    }

    fn authority_mut(&mut self) -> &mut AuthorityContext {
        &mut self.authority
    }

    fn emit(&mut self, _target: Identity, _value: Value) -> Result<(), RuntimeError> {
        panic!("integer loop test unexpectedly emitted a value")
    }

    fn validate_mailbox_receiver(&mut self, _receiver: &Value) -> Result<(), RuntimeError> {
        panic!("integer loop test unexpectedly validated a mailbox receiver")
    }

    fn resolve_program(
        &mut self,
        _program_bytes_relation: RelationId,
        _program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError> {
        panic!("integer loop test unexpectedly resolved a program")
    }

    fn call_builtin(&mut self, _name: Symbol, _args: &[Value]) -> Result<Value, RuntimeError> {
        panic!("integer loop test unexpectedly called a builtin")
    }
}

fn integer_loop_program(start: Value, step: Value, limit: Value) -> Arc<Program> {
    Arc::new(
        Program::new(
            4,
            [
                Instruction::Load {
                    dst: register(0),
                    value: start,
                },
                Instruction::Load {
                    dst: register(1),
                    value: step,
                },
                Instruction::Load {
                    dst: register(2),
                    value: limit,
                },
                Instruction::Binary {
                    dst: register(0),
                    op: RuntimeBinaryOp::Add,
                    left: register(0),
                    right: register(1),
                },
                Instruction::Binary {
                    dst: register(3),
                    op: RuntimeBinaryOp::Lt,
                    left: register(0),
                    right: register(2),
                },
                Instruction::Branch {
                    condition: register(3),
                    if_true: 3,
                    if_false: 6,
                },
                Instruction::Return {
                    value: Operand::Register(register(0)),
                },
            ],
        )
        .unwrap(),
    )
}

fn canonical_program() -> Arc<Program> {
    integer_loop_program(
        Value::int(0).unwrap(),
        Value::int(1).unwrap(),
        Value::int(ITERATIONS as i64).unwrap(),
    )
}

fn natural_accumulator_program(total: Value) -> Arc<Program> {
    natural_accumulator_program_with_limit(total, ITERATIONS)
}

fn natural_accumulator_program_with_limit(total: Value, limit: usize) -> Arc<Program> {
    Arc::new(
        Program::new(
            8,
            [
                Instruction::Load {
                    dst: register(0),
                    value: Value::int(0).unwrap(),
                },
                Instruction::Load {
                    dst: register(1),
                    value: total,
                },
                Instruction::Load {
                    dst: register(2),
                    value: Value::nothing(),
                },
                Instruction::Load {
                    dst: register(3),
                    value: Value::int(limit as i64).unwrap(),
                },
                Instruction::Binary {
                    dst: register(4),
                    op: RuntimeBinaryOp::Lt,
                    left: register(0),
                    right: register(3),
                },
                Instruction::Branch {
                    condition: register(4),
                    if_true: 6,
                    if_false: 12,
                },
                Instruction::Load {
                    dst: register(5),
                    value: Value::int(1).unwrap(),
                },
                Instruction::Binary {
                    dst: register(6),
                    op: RuntimeBinaryOp::Add,
                    left: register(0),
                    right: register(5),
                },
                Instruction::Move {
                    dst: register(0),
                    src: register(6),
                },
                Instruction::Binary {
                    dst: register(7),
                    op: RuntimeBinaryOp::Add,
                    left: register(1),
                    right: register(0),
                },
                Instruction::Move {
                    dst: register(1),
                    src: register(7),
                },
                Instruction::Jump { target: 3 },
                Instruction::Return {
                    value: Operand::Register(register(1)),
                },
            ],
        )
        .unwrap(),
    )
}

fn run(vm: &mut RegisterVm, budget: usize) -> Result<VmHostResponse, RuntimeError> {
    vm.run_until_host_response(&mut TestHost::default(), budget, MAX_CALL_DEPTH)
}

fn register(index: u16) -> Register {
    Register(index)
}

#[test]
fn native_integer_loop_matches_interpreter_completion() {
    let program = canonical_program();
    let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
    let mut native = RegisterVm::new(Arc::clone(&program));

    let interpreted_outcome = run(&mut interpreted, INSTRUCTION_COUNT).unwrap();
    let native_outcome = run(&mut native, INSTRUCTION_COUNT).unwrap();
    assert_eq!(native_outcome, interpreted_outcome);
    assert_eq!(
        native_outcome,
        VmHostResponse::Complete(Value::int(ITERATIONS as i64).unwrap()),
    );
    assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    assert_eq!(program.native_compile_attempts(), 1);
}

#[test]
fn native_integer_loop_preserves_every_budget_boundary() {
    for budget in [
        1,
        3,
        5,
        6,
        7,
        1_024,
        INSTRUCTION_COUNT - 2,
        INSTRUCTION_COUNT - 1,
    ] {
        let program = canonical_program();
        let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
        let mut native = RegisterVm::new(program);

        assert!(matches!(
            run(&mut interpreted, budget),
            Err(RuntimeError::InstructionBudgetExceeded { .. })
        ));
        assert!(matches!(
            run(&mut native, budget),
            Err(RuntimeError::InstructionBudgetExceeded { .. })
        ));
        assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    }
}

#[test]
fn native_integer_loop_falls_back_before_mixed_value_mutation() {
    let program = integer_loop_program(
        Value::int(0).unwrap(),
        Value::int(1).unwrap(),
        Value::float(10.0),
    );
    let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
    let mut native = RegisterVm::new(Arc::clone(&program));

    assert!(run(&mut interpreted, 100).is_err());
    assert!(run(&mut native, 100).is_err());
    assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    assert_eq!(program.native_compile_attempts(), 0);
}

#[test]
fn native_integer_loop_does_not_compile_short_cold_loops() {
    let program = integer_loop_program(
        Value::int(0).unwrap(),
        Value::int(1).unwrap(),
        Value::int(32).unwrap(),
    );
    let mut vm = RegisterVm::new(Arc::clone(&program));
    assert_eq!(
        run(&mut vm, 100).unwrap(),
        VmHostResponse::Complete(Value::int(32).unwrap()),
    );
    assert_eq!(program.native_compile_attempts(), 0);
}

#[test]
fn native_integer_loop_keeps_short_overflow_path_interpreted() {
    let max = mica_var::abi::VALUE_INT_MAX;
    let program = integer_loop_program(
        Value::int(max - 5).unwrap(),
        Value::int(2).unwrap(),
        Value::int(max).unwrap(),
    );
    let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
    let mut native = RegisterVm::new(Arc::clone(&program));

    let interpreted_outcome = run(&mut interpreted, 100).unwrap();
    let native_outcome = run(&mut native, 100).unwrap();
    assert_eq!(native_outcome, interpreted_outcome);
    assert!(matches!(native_outcome, VmHostResponse::Abort(_)));
    assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    assert_eq!(program.native_compile_attempts(), 0);
}

#[test]
fn native_integer_loop_cache_is_shared_across_threads() {
    let program = canonical_program();
    let barrier = Arc::new(Barrier::new(4));
    let mut threads = Vec::new();
    for _ in 0..4 {
        let program = Arc::clone(&program);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let mut vm = RegisterVm::new(program);
            barrier.wait();
            run(&mut vm, INSTRUCTION_COUNT).unwrap()
        }));
    }
    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            VmHostResponse::Complete(Value::int(ITERATIONS as i64).unwrap()),
        );
    }
    assert_eq!(program.native_compile_attempts(), 1);
}

#[test]
fn native_integer_loop_resumes_from_snapshotted_budget_exit() {
    let program = canonical_program();
    let mut vm = RegisterVm::new(Arc::clone(&program));
    assert!(run(&mut vm, INSTRUCTION_COUNT - 1).is_err());
    let state = vm.snapshot_state();
    let mut resumed = RegisterVm::from_state(state.clone());
    let mut interpreted = RegisterVm::new_interpreted(program);
    interpreted.restore_state(&state);

    assert_eq!(
        run(&mut resumed, 1).unwrap(),
        run(&mut interpreted, 1).unwrap()
    );
    assert_eq!(resumed.snapshot_state(), interpreted.snapshot_state());
}

#[test]
fn native_natural_loop_matches_interpreter_completion() {
    let program = natural_accumulator_program(Value::int(0).unwrap());
    let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
    let mut native = RegisterVm::new(Arc::clone(&program));

    let interpreted_outcome = run(&mut interpreted, NATURAL_INSTRUCTION_COUNT).unwrap();
    let native_outcome = run(&mut native, NATURAL_INSTRUCTION_COUNT).unwrap();
    assert_eq!(native_outcome, interpreted_outcome);
    assert_eq!(
        native_outcome,
        VmHostResponse::Complete(Value::int(134_225_920).unwrap()),
    );
    assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    assert_eq!(program.native_compile_attempts(), 1);
}

#[test]
fn native_natural_loop_preserves_budget_remainders() {
    for budget in (NATURAL_INSTRUCTION_COUNT - 12)..=NATURAL_INSTRUCTION_COUNT {
        let program = natural_accumulator_program(Value::int(0).unwrap());
        let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
        let mut native = RegisterVm::new(program);

        let interpreted_outcome = run(&mut interpreted, budget);
        let native_outcome = run(&mut native, budget);
        match (native_outcome, interpreted_outcome) {
            (Ok(native), Ok(interpreted)) => assert_eq!(native, interpreted, "budget {budget}"),
            (
                Err(RuntimeError::InstructionBudgetExceeded { .. }),
                Err(RuntimeError::InstructionBudgetExceeded { .. }),
            ) => {}
            (native, interpreted) => panic!(
                "budget {budget} produced different outcomes: native={native:?} interpreted={interpreted:?}",
            ),
        }
        assert_eq!(
            native.snapshot_state(),
            interpreted.snapshot_state(),
            "budget {budget}",
        );
    }
}

#[test]
fn native_natural_loop_side_exit_is_atomic_and_sticky() {
    let program = natural_accumulator_program(Value::float(0.0));
    let mut interpreted = RegisterVm::new_interpreted(Arc::clone(&program));
    let mut native = RegisterVm::new(Arc::clone(&program));

    let interpreted_outcome = run(&mut interpreted, NATURAL_INSTRUCTION_COUNT).unwrap();
    let native_outcome = run(&mut native, NATURAL_INSTRUCTION_COUNT).unwrap();
    assert_eq!(native_outcome, interpreted_outcome);
    assert_eq!(native.snapshot_state(), interpreted.snapshot_state());
    assert_eq!(program.native_compile_attempts(), 1);
    assert_eq!(native.native_side_exit_count(), 1);
}

#[test]
fn native_natural_loop_does_not_compile_short_cold_loops() {
    let program = natural_accumulator_program_with_limit(Value::int(0).unwrap(), 32);
    let mut vm = RegisterVm::new(Arc::clone(&program));
    assert_eq!(
        run(&mut vm, (32 * 9) + 7).unwrap(),
        VmHostResponse::Complete(Value::int(528).unwrap()),
    );
    assert_eq!(program.native_compile_attempts(), 0);
}

#[test]
fn native_natural_loop_cache_is_shared_across_threads() {
    let program = natural_accumulator_program(Value::int(0).unwrap());
    let barrier = Arc::new(Barrier::new(4));
    let mut threads = Vec::new();
    for _ in 0..4 {
        let program = Arc::clone(&program);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let mut vm = RegisterVm::new(program);
            barrier.wait();
            run(&mut vm, NATURAL_INSTRUCTION_COUNT).unwrap()
        }));
    }
    for thread in threads {
        assert_eq!(
            thread.join().unwrap(),
            VmHostResponse::Complete(Value::int(134_225_920).unwrap()),
        );
    }
    assert_eq!(program.native_compile_attempts(), 1);
}
