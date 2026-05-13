use crate::{Instruction, Operand, Program, ProgramResolver, Register, RuntimeError, SuspendKind};
use mica_relation_kernel::{Transaction, Tuple, applicable_methods};
use mica_var::{Value, ValueKind};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    program: Arc<Program>,
    ip: usize,
    registers: Vec<Value>,
    return_register: Option<Register>,
}

impl Frame {
    fn root(program: Arc<Program>) -> Self {
        Self::new(program, None, Vec::new()).expect("root frame has no arguments")
    }

    fn new(
        program: Arc<Program>,
        return_register: Option<Register>,
        args: Vec<Value>,
    ) -> Result<Self, RuntimeError> {
        if args.len() > program.register_count() {
            return Err(RuntimeError::InvalidCallArity {
                expected_at_most: program.register_count(),
                actual: args.len(),
            });
        }

        let mut registers = vec![Value::nothing(); program.register_count()];
        for (slot, arg) in registers.iter_mut().zip(args) {
            *slot = arg;
        }
        Ok(Self {
            program,
            ip: 0,
            registers,
            return_register,
        })
    }

    pub fn program(&self) -> &Arc<Program> {
        &self.program
    }

    pub fn ip(&self) -> usize {
        self.ip
    }

    pub fn registers(&self) -> &[Value] {
        &self.registers
    }

    pub fn return_register(&self) -> Option<Register> {
        self.return_register
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VmState {
    frames: Vec<Frame>,
}

impl VmState {
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    pub fn current_frame(&self) -> Option<&Frame> {
        self.frames.last()
    }

    pub fn ip(&self) -> usize {
        self.current_frame().map_or(0, Frame::ip)
    }

    pub fn registers(&self) -> &[Value] {
        self.current_frame().map_or(&[], Frame::registers)
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
    state: VmState,
}

impl RegisterVm {
    pub fn new(program: Arc<Program>) -> Self {
        Self {
            state: VmState {
                frames: vec![Frame::root(program)],
            },
        }
    }

    pub fn from_state(state: VmState) -> Self {
        Self { state }
    }

    pub fn snapshot_state(&self) -> VmState {
        self.state.clone()
    }

    pub fn restore_state(&mut self, state: &VmState) {
        self.state = state.clone();
    }

    pub fn frame_count(&self) -> usize {
        self.state.frames.len()
    }

    pub fn register(&self, register: Register) -> Option<&Value> {
        self.current_frame()
            .ok()
            .and_then(|frame| frame.registers.get(register.0 as usize))
    }

    pub fn set_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        self.write_register(register, value)
    }

    pub fn run_until_host_response(
        &mut self,
        tx: &mut Transaction<'_>,
        resolver: &ProgramResolver,
        pending_effects: &mut Vec<Value>,
        instruction_budget: usize,
        max_call_depth: usize,
    ) -> Result<VmHostResponse, RuntimeError> {
        for _ in 0..instruction_budget {
            let response = self.step(tx, resolver, pending_effects, max_call_depth)?;
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
        resolver: &ProgramResolver,
        pending_effects: &mut Vec<Value>,
        max_call_depth: usize,
    ) -> Result<VmHostResponse, RuntimeError> {
        let instruction = {
            let frame = self.current_frame()?;
            frame
                .program
                .instructions()
                .get(frame.ip)
                .cloned()
                .ok_or(RuntimeError::ProgramCounterOutOfBounds { ip: frame.ip })?
        };

        match instruction {
            Instruction::Load { dst, value } => {
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Move { dst, src } => {
                let value = self.read_register(src)?.clone();
                self.write_register(dst, value)?;
                self.advance_ip()?;
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
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Assert { relation, values } => {
                tx.assert(relation, self.resolve_tuple(values)?)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Retract { relation, values } => {
                tx.retract(relation, self.resolve_tuple(values)?)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::RetractWhere { relation, bindings } => {
                let bindings = self.resolve_bindings(&bindings)?;
                let tuples = tx.scan(relation, &bindings)?;
                for tuple in tuples {
                    tx.retract(relation, tuple)?;
                }
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::ReplaceFunctional { relation, values } => {
                tx.replace_functional(relation, self.resolve_tuple(values)?)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Branch {
                condition,
                if_true,
                if_false,
            } => {
                let target = if truthy(self.read_register(condition)?) {
                    if_true
                } else {
                    if_false
                };
                self.current_frame_mut()?.ip = target;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Jump { target } => {
                self.current_frame_mut()?.ip = target;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Emit { value } => {
                pending_effects.push(self.resolve_operand(&value)?);
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Call { dst, program, args } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let args = args
                    .iter()
                    .map(|arg| self.resolve_operand(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                self.advance_ip()?;
                self.state
                    .frames
                    .push(Frame::new(program, Some(dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Instruction::Dispatch {
                dst,
                relations,
                program_relation,
                program_bytes,
                selector,
                roles,
            } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let selector = self.resolve_operand(&selector)?;
                let roles = roles
                    .iter()
                    .map(|(role, value)| Ok((role.clone(), self.resolve_operand(value)?)))
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                let methods = applicable_methods(tx, relations, selector.clone(), roles.clone())?;
                let method = match methods.as_slice() {
                    [] => return Err(RuntimeError::NoApplicableMethod { selector }),
                    [method] => method.clone(),
                    _ => {
                        return Err(RuntimeError::AmbiguousDispatch { selector, methods });
                    }
                };
                let program_rows = tx.scan(program_relation, &[Some(method.clone()), None])?;
                let program_id = program_rows
                    .first()
                    .map(|row| row.values()[1].clone())
                    .ok_or_else(|| RuntimeError::MissingMethodProgram {
                        method: method.clone(),
                    })?;
                let program = resolver.resolve(tx, program_bytes, &program_id)?;
                let args = roles.into_iter().map(|(_, value)| value).collect();
                self.advance_ip()?;
                self.state
                    .frames
                    .push(Frame::new(program, Some(dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Instruction::Commit => {
                self.advance_ip()?;
                Ok(VmHostResponse::Commit)
            }
            Instruction::Suspend { kind } => {
                self.advance_ip()?;
                Ok(VmHostResponse::Suspend(kind))
            }
            Instruction::RollbackRetry => {
                self.advance_ip()?;
                Ok(VmHostResponse::RollbackRetry)
            }
            Instruction::Return { value } => {
                let value = self.resolve_operand(&value)?;
                self.return_from_frame(value)
            }
            Instruction::Abort { error } => {
                let error = self.resolve_operand(&error)?;
                Ok(VmHostResponse::Abort(error))
            }
        }
    }

    fn return_from_frame(&mut self, value: Value) -> Result<VmHostResponse, RuntimeError> {
        let frame = self
            .state
            .frames
            .pop()
            .ok_or(RuntimeError::EmptyCallStack)?;
        let Some(return_register) = frame.return_register else {
            return Ok(VmHostResponse::Complete(value));
        };
        self.write_register(return_register, value)?;
        Ok(VmHostResponse::Continue)
    }

    fn advance_ip(&mut self) -> Result<(), RuntimeError> {
        self.current_frame_mut()?.ip += 1;
        Ok(())
    }

    fn current_frame(&self) -> Result<&Frame, RuntimeError> {
        self.state.frames.last().ok_or(RuntimeError::EmptyCallStack)
    }

    fn current_frame_mut(&mut self) -> Result<&mut Frame, RuntimeError> {
        self.state
            .frames
            .last_mut()
            .ok_or(RuntimeError::EmptyCallStack)
    }

    fn read_register(&self, register: Register) -> Result<&Value, RuntimeError> {
        let frame = self.current_frame()?;
        frame
            .registers
            .get(register.0 as usize)
            .ok_or(RuntimeError::RegisterOutOfBounds {
                register: register.0,
                register_count: frame.registers.len(),
            })
    }

    fn write_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        let frame = self.current_frame_mut()?;
        let register_count = frame.registers.len();
        let slot = frame.registers.get_mut(register.0 as usize).ok_or(
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
