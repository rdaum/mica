use crate::{
    CatchHandler, ErrorField, Instruction, ListItem, Operand, Program, ProgramResolver, Register,
    RuntimeBinaryOp, RuntimeError, RuntimeUnaryOp, SuspendKind,
};
use mica_relation_kernel::{Transaction, Tuple, applicable_methods};
use mica_var::{Value, ValueKind};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    program: Arc<Program>,
    ip: usize,
    registers: Vec<Value>,
    return_register: Option<Register>,
    try_stack: Vec<TryRegion>,
    pending_finally: Vec<FinallyContinuation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TryRegion {
    catches: Vec<CatchHandler>,
    finally: Option<usize>,
    end: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FinallyContinuation {
    Normal(usize),
    Raise(Value),
    Return(Value),
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
            try_stack: Vec::new(),
            pending_finally: Vec::new(),
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
            Instruction::Unary { dst, op, src } => {
                let value = self.read_register(src)?;
                let value = eval_unary(op, value);
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Binary {
                dst,
                op,
                left,
                right,
            } => {
                let value = eval_binary(op, self.read_register(left)?, self.read_register(right)?);
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::BuildList { dst, items } => {
                let value = self.build_list(&items)?;
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::BuildMap { dst, entries } => {
                let entries = entries
                    .iter()
                    .map(|(key, value)| {
                        Ok((self.resolve_operand(key)?, self.resolve_operand(value)?))
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                self.write_register(dst, Value::map(entries))?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::BuildRange { dst, start, end } => {
                let start = self.resolve_operand(&start)?;
                let end = end
                    .as_ref()
                    .map(|end| self.resolve_operand(end))
                    .transpose()?;
                self.write_register(dst, Value::range(start, end))?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::Index {
                dst,
                collection,
                index,
            } => {
                let value = index_value(
                    self.read_register(collection)?,
                    &self.resolve_operand(&index)?,
                );
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::SetIndex {
                dst,
                collection,
                index,
                value,
            } => {
                let value = set_index_value(
                    self.read_register(collection)?,
                    &self.resolve_operand(&index)?,
                    self.resolve_operand(&value)?,
                );
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::ErrorField { dst, error, field } => {
                let value = error_field_value(self.read_register(error)?, field);
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::CollectionLen { dst, collection } => {
                let value = collection_len(self.read_register(collection)?);
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::CollectionKeyAt {
                dst,
                collection,
                index,
            } => {
                let value =
                    collection_key_at(self.read_register(collection)?, self.read_register(index)?);
                self.write_register(dst, value)?;
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::CollectionValueAt {
                dst,
                collection,
                index,
            } => {
                let value = collection_value_at(
                    self.read_register(collection)?,
                    self.read_register(index)?,
                );
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
            Instruction::ScanValue { dst, relation, key } => {
                let key = self.resolve_operand(&key)?;
                let value = tx
                    .scan(relation, &[Some(key), None])?
                    .first()
                    .map(|row| row.values()[1].clone())
                    .unwrap_or_else(Value::nothing);
                self.write_register(dst, value)?;
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
            Instruction::EnterTry {
                catches,
                finally,
                end,
            } => {
                self.current_frame_mut()?.try_stack.push(TryRegion {
                    catches,
                    finally,
                    end,
                });
                self.advance_ip()?;
                Ok(VmHostResponse::Continue)
            }
            Instruction::ExitTry => self.exit_try_region(),
            Instruction::EndFinally => self.end_finally(),
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
            Instruction::Raise {
                error,
                message,
                value,
            } => {
                let error = self.resolve_operand(&error)?;
                let message = message
                    .as_ref()
                    .map(|message| self.resolve_operand(message))
                    .transpose()?;
                let value = value
                    .as_ref()
                    .map(|value| self.resolve_operand(value))
                    .transpose()?;
                let error = normalize_raised_error(error, message, value)?;
                self.begin_raise(error)
            }
        }
    }

    fn return_from_frame(&mut self, value: Value) -> Result<VmHostResponse, RuntimeError> {
        {
            let frame = self.current_frame_mut()?;
            if frame.pending_finally.pop().is_some() {
                // A return from inside a finally body replaces the control flow
                // that originally entered the finally.
            }
            while let Some(region) = frame.try_stack.pop() {
                if let Some(finally) = region.finally {
                    frame
                        .pending_finally
                        .push(FinallyContinuation::Return(value));
                    frame.ip = finally;
                    return Ok(VmHostResponse::Continue);
                }
            }
        }

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

    fn exit_try_region(&mut self) -> Result<VmHostResponse, RuntimeError> {
        let frame = self.current_frame_mut()?;
        let region = frame.try_stack.pop().ok_or(RuntimeError::EmptyTryStack)?;
        if let Some(finally) = region.finally {
            frame
                .pending_finally
                .push(FinallyContinuation::Normal(region.end));
            frame.ip = finally;
        } else {
            frame.ip = region.end;
        }
        Ok(VmHostResponse::Continue)
    }

    fn end_finally(&mut self) -> Result<VmHostResponse, RuntimeError> {
        let continuation = self
            .current_frame_mut()?
            .pending_finally
            .pop()
            .ok_or(RuntimeError::EmptyTryStack)?;
        match continuation {
            FinallyContinuation::Normal(target) => {
                self.current_frame_mut()?.ip = target;
                Ok(VmHostResponse::Continue)
            }
            FinallyContinuation::Raise(error) => self.begin_raise(error),
            FinallyContinuation::Return(value) => self.return_from_frame(value),
        }
    }

    fn begin_raise(&mut self, error: Value) -> Result<VmHostResponse, RuntimeError> {
        loop {
            let Some(frame) = self.state.frames.last_mut() else {
                return Ok(VmHostResponse::Abort(error));
            };

            if frame.pending_finally.pop().is_some() {
                // A raise from inside a finally body replaces the control flow
                // that originally entered the finally.
            }

            while let Some(region) = frame.try_stack.pop() {
                if let Some(handler) = matching_handler(&region.catches, &error) {
                    if let Some(binding) = handler.binding {
                        let register_count = frame.registers.len();
                        let slot = frame.registers.get_mut(binding.0 as usize).ok_or(
                            RuntimeError::RegisterOutOfBounds {
                                register: binding.0,
                                register_count,
                            },
                        )?;
                        *slot = error;
                    }
                    if let Some(finally) = region.finally {
                        frame.try_stack.push(TryRegion {
                            catches: Vec::new(),
                            finally: Some(finally),
                            end: region.end,
                        });
                    }
                    frame.ip = handler.target;
                    return Ok(VmHostResponse::Continue);
                }

                if let Some(finally) = region.finally {
                    frame
                        .pending_finally
                        .push(FinallyContinuation::Raise(error));
                    frame.ip = finally;
                    return Ok(VmHostResponse::Continue);
                }
            }

            self.state.frames.pop();
        }
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

    fn build_list(&self, items: &[ListItem]) -> Result<Value, RuntimeError> {
        let mut values = Vec::new();
        for item in items {
            match item {
                ListItem::Value(operand) => values.push(self.resolve_operand(operand)?),
                ListItem::Splice(operand) => {
                    let splice = self.resolve_operand(operand)?;
                    let Some(()) = splice.with_list(|items| {
                        values.extend(items.iter().cloned());
                    }) else {
                        return Ok(Value::nothing());
                    };
                }
            }
        }
        Ok(Value::list(values))
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

fn eval_unary(op: RuntimeUnaryOp, value: &Value) -> Value {
    match op {
        RuntimeUnaryOp::Not => Value::bool(!truthy(value)),
        RuntimeUnaryOp::Neg => value.checked_neg().unwrap_or_else(Value::nothing),
    }
}

fn eval_binary(op: RuntimeBinaryOp, left: &Value, right: &Value) -> Value {
    match op {
        RuntimeBinaryOp::Eq => Value::bool(left == right),
        RuntimeBinaryOp::Ne => Value::bool(left != right),
        RuntimeBinaryOp::Lt => Value::bool(left < right),
        RuntimeBinaryOp::Le => Value::bool(left <= right),
        RuntimeBinaryOp::Gt => Value::bool(left > right),
        RuntimeBinaryOp::Ge => Value::bool(left >= right),
        RuntimeBinaryOp::Add => left.checked_add(right).unwrap_or_else(Value::nothing),
        RuntimeBinaryOp::Sub => left.checked_sub(right).unwrap_or_else(Value::nothing),
        RuntimeBinaryOp::Mul => left.checked_mul(right).unwrap_or_else(Value::nothing),
        RuntimeBinaryOp::Div => left.checked_div(right).unwrap_or_else(Value::nothing),
        RuntimeBinaryOp::Rem => left.checked_rem(right).unwrap_or_else(Value::nothing),
    }
}

fn index_value(collection: &Value, index: &Value) -> Value {
    if let Some((start, end)) = index.with_range(|start, end| (start.clone(), end.cloned()))
        && let Some(len) = collection.list_len()
    {
        return list_range_slice(collection, len, &start, end.as_ref())
            .unwrap_or_else(Value::nothing);
    }
    if let Some(index) = index.as_int()
        && index >= 0
        && let Some(value) = collection.list_get(index as usize)
    {
        return value;
    }
    collection.map_get(index).unwrap_or_else(Value::nothing)
}

fn list_range_slice(
    collection: &Value,
    len: usize,
    start: &Value,
    end: Option<&Value>,
) -> Option<Value> {
    let start = ordinal_index(start)?;
    let end_exclusive = match end {
        Some(end) => {
            let end = ordinal_index(end)?;
            if end < start {
                return None;
            }
            end.checked_add(1)?
        }
        None => len,
    };
    collection.list_slice(start, end_exclusive)
}

fn set_index_value(collection: &Value, index: &Value, value: Value) -> Value {
    collection
        .index_set(index, value)
        .unwrap_or_else(Value::nothing)
}

fn error_field_value(error: &Value, field: ErrorField) -> Value {
    if let Some(code) = error.as_error_code() {
        return match field {
            ErrorField::Code => Value::error_code(code),
            ErrorField::Message | ErrorField::Value => Value::nothing(),
        };
    }
    error
        .with_error(|error| match field {
            ErrorField::Code => Value::error_code(error.code()),
            ErrorField::Message => error.message().map_or_else(Value::nothing, Value::string),
            ErrorField::Value => error.value().cloned().unwrap_or_else(Value::nothing),
        })
        .unwrap_or_else(Value::nothing)
}

fn collection_len(collection: &Value) -> Value {
    let len = collection
        .list_len()
        .or_else(|| collection.map_len())
        .unwrap_or(0);
    i64::try_from(len)
        .ok()
        .and_then(|len| Value::int(len).ok())
        .unwrap_or_else(Value::nothing)
}

fn collection_key_at(collection: &Value, index: &Value) -> Value {
    let Some(index) = ordinal_index(index) else {
        return Value::nothing();
    };
    if collection.list_len().is_some() {
        return i64::try_from(index)
            .ok()
            .and_then(|index| Value::int(index).ok())
            .unwrap_or_else(Value::nothing);
    }
    collection
        .with_map(|entries| entries.get(index).map(|(key, _)| key.clone()))
        .flatten()
        .unwrap_or_else(Value::nothing)
}

fn collection_value_at(collection: &Value, index: &Value) -> Value {
    let Some(index) = ordinal_index(index) else {
        return Value::nothing();
    };
    collection
        .list_get(index)
        .or_else(|| {
            collection
                .with_map(|entries| entries.get(index).map(|(_, value)| value.clone()))
                .flatten()
        })
        .unwrap_or_else(Value::nothing)
}

fn ordinal_index(index: &Value) -> Option<usize> {
    let index = index.as_int()?;
    usize::try_from(index).ok()
}

fn matching_handler<'a>(catches: &'a [CatchHandler], error: &Value) -> Option<&'a CatchHandler> {
    let error_code = error.error_code_symbol();
    catches.iter().find(|catch| match &catch.code {
        Some(code) => code.error_code_symbol().is_some() && code.error_code_symbol() == error_code,
        None => true,
    })
}

fn normalize_raised_error(
    error: Value,
    message: Option<Value>,
    value: Option<Value>,
) -> Result<Value, RuntimeError> {
    let message = message
        .and_then(|message| {
            if message.kind() == ValueKind::Nothing {
                None
            } else {
                Some(error_message_text(message))
            }
        })
        .transpose()?;
    if let Some(code) = error.as_error_code() {
        return Ok(Value::error(code, message, value));
    }
    if let Some(result) = error.with_error(|existing| {
        let message = message.or_else(|| existing.message().map(str::to_owned));
        let value = value.or_else(|| existing.value().cloned());
        Value::error(existing.code(), message, value)
    }) {
        return Ok(result);
    }
    Err(RuntimeError::InvalidRaisedValue(error))
}

fn error_message_text(value: Value) -> Result<String, RuntimeError> {
    value
        .with_str(str::to_owned)
        .ok_or(RuntimeError::InvalidErrorMessage(value))
}
