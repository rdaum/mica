use crate::{Instruction, Operand, Program, Register, RuntimeError, SuspendKind};
use mica_relation_kernel::{Transaction, Tuple};
use mica_var::{Value, ValueKind};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VmState {
    ip: usize,
    registers: Vec<Value>,
}

impl VmState {
    pub fn ip(&self) -> usize {
        self.ip
    }

    pub fn registers(&self) -> &[Value] {
        &self.registers
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VmHostResponse {
    Continue,
    Commit,
    Suspend(SuspendKind),
    Complete(Value),
    Abort(Value),
    RollbackRetry,
}

#[derive(Clone, Debug)]
pub struct RegisterVm {
    program: Arc<Program>,
    state: VmState,
}

impl RegisterVm {
    pub fn new(program: Arc<Program>) -> Self {
        let registers = vec![Value::nothing(); program.register_count()];
        Self {
            program,
            state: VmState { ip: 0, registers },
        }
    }

    pub fn from_state(program: Arc<Program>, state: VmState) -> Self {
        Self { program, state }
    }

    pub fn snapshot_state(&self) -> VmState {
        self.state.clone()
    }

    pub fn restore_state(&mut self, state: &VmState) {
        self.state = state.clone();
    }

    pub fn register(&self, register: Register) -> Option<&Value> {
        self.state.registers.get(register.0 as usize)
    }

    pub fn set_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        let register_count = self.state.registers.len();
        let slot = self.state.registers.get_mut(register.0 as usize).ok_or(
            RuntimeError::RegisterOutOfBounds {
                register: register.0,
                register_count,
            },
        )?;
        *slot = value;
        Ok(())
    }

    pub fn run_until_host_response(
        &mut self,
        tx: &mut Transaction<'_>,
        pending_effects: &mut Vec<Value>,
        instruction_budget: usize,
    ) -> Result<VmHostResponse, RuntimeError> {
        for _ in 0..instruction_budget {
            let response = self.step(tx, pending_effects)?;
            if response != VmHostResponse::Continue {
                return Ok(response);
            }
        }
        Err(RuntimeError::InstructionBudgetExceeded {
            budget: instruction_budget,
        })
    }

    fn step(
        &mut self,
        tx: &mut Transaction<'_>,
        pending_effects: &mut Vec<Value>,
    ) -> Result<VmHostResponse, RuntimeError> {
        let instruction = self
            .program
            .instructions()
            .get(self.state.ip)
            .cloned()
            .ok_or(RuntimeError::ProgramCounterOutOfBounds { ip: self.state.ip })?;

        match instruction {
            Instruction::Load { dst, value } => {
                self.write_register(dst, value)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Move { dst, src } => {
                let value = self.read_register(src)?.clone();
                self.write_register(dst, value)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::ScanExists {
                dst,
                relation,
                bindings,
            } => {
                let bindings = self.resolve_bindings(&bindings)?;
                let exists = !tx.scan(relation, &bindings)?.is_empty();
                self.write_register(dst, Value::bool(exists))?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Assert { relation, values } => {
                tx.assert(relation, self.resolve_tuple(values)?)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Retract { relation, values } => {
                tx.retract(relation, self.resolve_tuple(values)?)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::RetractWhere { relation, bindings } => {
                let bindings = self.resolve_bindings(&bindings)?;
                let tuples = tx.scan(relation, &bindings)?;
                for tuple in tuples {
                    tx.retract(relation, tuple)?;
                }
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::ReplaceFunctional { relation, values } => {
                tx.replace_functional(relation, self.resolve_tuple(values)?)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Branch {
                condition,
                if_true,
                if_false,
            } => {
                self.state.ip = if truthy(self.read_register(condition)?) {
                    if_true
                } else {
                    if_false
                };
                Ok(VmHostResponse::Continue)
            }
            Instruction::Jump { target } => {
                self.state.ip = target;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Emit { value } => {
                pending_effects.push(self.resolve_operand(&value)?);
                self.state.ip += 1;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Commit => {
                self.state.ip += 1;
                Ok(VmHostResponse::Commit)
            }
            Instruction::Suspend { kind } => {
                self.state.ip += 1;
                Ok(VmHostResponse::Suspend(kind))
            }
            Instruction::RollbackRetry => {
                self.state.ip += 1;
                Ok(VmHostResponse::RollbackRetry)
            }
            Instruction::Return { value } => {
                let value = self.resolve_operand(&value)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Complete(value))
            }
            Instruction::Abort { error } => {
                let error = self.resolve_operand(&error)?;
                self.state.ip += 1;
                Ok(VmHostResponse::Abort(error))
            }
        }
    }

    fn read_register(&self, register: Register) -> Result<&Value, RuntimeError> {
        self.state
            .registers
            .get(register.0 as usize)
            .ok_or(RuntimeError::RegisterOutOfBounds {
                register: register.0,
                register_count: self.state.registers.len(),
            })
    }

    fn write_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        let register_count = self.state.registers.len();
        let slot = self.state.registers.get_mut(register.0 as usize).ok_or(
            RuntimeError::RegisterOutOfBounds {
                register: register.0,
                register_count,
            },
        )?;
        *slot = value;
        Ok(())
    }

    fn resolve_operand(&self, operand: &Operand) -> Result<Value, RuntimeError> {
        match operand {
            Operand::Register(register) => Ok(self.read_register(*register)?.clone()),
            Operand::Value(value) => Ok(value.clone()),
        }
    }

    fn resolve_tuple(&self, values: Vec<Operand>) -> Result<Tuple, RuntimeError> {
        Ok(Tuple::new(
            values
                .iter()
                .map(|value| self.resolve_operand(value))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    fn resolve_bindings(
        &self,
        bindings: &[Option<Operand>],
    ) -> Result<Vec<Option<Value>>, RuntimeError> {
        bindings
            .iter()
            .map(|binding| {
                binding
                    .as_ref()
                    .map(|operand| self.resolve_operand(operand))
                    .transpose()
            })
            .collect()
    }
}

fn truthy(value: &Value) -> bool {
    match value.kind() {
        ValueKind::Nothing => false,
        ValueKind::Bool => value.as_bool().unwrap_or(false),
        _ => true,
    }
}
