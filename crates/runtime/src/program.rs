use crate::RuntimeError;
use mica_relation_kernel::{DispatchRelations, RelationId};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Register(pub u16);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operand {
    Register(Register),
    Value(Value),
}

impl From<Register> for Operand {
    fn from(value: Register) -> Self {
        Self::Register(value)
    }
}

impl From<Value> for Operand {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuspendKind {
    Commit,
    TimedMillis(u64),
    WaitingForInput(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Instruction {
    Load {
        dst: Register,
        value: Value,
    },
    Move {
        dst: Register,
        src: Register,
    },
    ScanExists {
        dst: Register,
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
    },
    Assert {
        relation: RelationId,
        values: Vec<Operand>,
    },
    Retract {
        relation: RelationId,
        values: Vec<Operand>,
    },
    RetractWhere {
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
    },
    ReplaceFunctional {
        relation: RelationId,
        values: Vec<Operand>,
    },
    Branch {
        condition: Register,
        if_true: usize,
        if_false: usize,
    },
    Jump {
        target: usize,
    },
    Emit {
        value: Operand,
    },
    Call {
        dst: Register,
        program: Arc<Program>,
        args: Vec<Operand>,
    },
    Dispatch {
        dst: Register,
        relations: DispatchRelations,
        program_relation: RelationId,
        programs: Arc<ProgramStore>,
        selector: Operand,
        roles: Vec<(Value, Operand)>,
    },
    Commit,
    Suspend {
        kind: SuspendKind,
    },
    RollbackRetry,
    Return {
        value: Operand,
    },
    Abort {
        error: Operand,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Program {
    register_count: usize,
    instructions: Arc<[Instruction]>,
}

impl Program {
    pub fn new(
        register_count: usize,
        instructions: impl IntoIterator<Item = Instruction>,
    ) -> Result<Self, RuntimeError> {
        let instructions = instructions.into_iter().collect::<Vec<_>>();
        for instruction in &instructions {
            validate_instruction(register_count, instructions.len(), instruction)?;
        }
        Ok(Self {
            register_count,
            instructions: instructions.into(),
        })
    }

    pub fn register_count(&self) -> usize {
        self.register_count
    }

    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, RuntimeError> {
        let mut out = Vec::new();
        out.extend_from_slice(b"MICAPRG1");
        write_u32(&mut out, self.register_count as u32);
        write_u32(&mut out, self.instructions.len() as u32);
        for instruction in self.instructions.iter() {
            write_instruction(&mut out, instruction)?;
        }
        Ok(out)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RuntimeError> {
        let mut input = ByteReader::new(bytes);
        input.expect_magic(b"MICAPRG1")?;
        let register_count = input.read_u32()? as usize;
        let instruction_count = input.read_u32()? as usize;
        let mut instructions = Vec::with_capacity(instruction_count);
        for _ in 0..instruction_count {
            instructions.push(input.read_instruction()?);
        }
        if !input.is_empty() {
            return Err(artifact_error("trailing program artifact bytes"));
        }
        Self::new(register_count, instructions)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProgramStore {
    programs: BTreeMap<Value, Arc<Program>>,
}

impl ProgramStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_program(mut self, method: Value, program: Program) -> Self {
        self.insert(method, program);
        self
    }

    pub fn insert(&mut self, method: Value, program: Program) -> Option<Arc<Program>> {
        self.programs.insert(method, Arc::new(program))
    }

    pub fn get(&self, method: &Value) -> Option<Arc<Program>> {
        self.programs.get(method).cloned()
    }

    pub fn contains(&self, method: &Value) -> bool {
        self.programs.contains_key(method)
    }

    pub fn len(&self) -> usize {
        self.programs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.programs.is_empty()
    }
}

fn validate_instruction(
    register_count: usize,
    instruction_count: usize,
    instruction: &Instruction,
) -> Result<(), RuntimeError> {
    match instruction {
        Instruction::Load { dst, .. } => validate_register(register_count, *dst),
        Instruction::Move { dst, src } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *src)
        }
        Instruction::ScanExists { dst, bindings, .. } => {
            validate_register(register_count, *dst)?;
            validate_bindings(register_count, bindings)
        }
        Instruction::Assert { values, .. }
        | Instruction::Retract { values, .. }
        | Instruction::ReplaceFunctional { values, .. } => {
            validate_operands(register_count, values.iter())
        }
        Instruction::RetractWhere { bindings, .. } => validate_bindings(register_count, bindings),
        Instruction::Branch {
            condition,
            if_true,
            if_false,
        } => {
            validate_register(register_count, *condition)?;
            validate_target(instruction_count, *if_true)?;
            validate_target(instruction_count, *if_false)
        }
        Instruction::Jump { target } => validate_target(instruction_count, *target),
        Instruction::Emit { value }
        | Instruction::Return { value }
        | Instruction::Abort { error: value } => validate_operand(register_count, value),
        Instruction::Call { dst, program, args } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, args.iter())?;
            if args.len() > program.register_count() {
                return Err(RuntimeError::InvalidCallArity {
                    expected_at_most: program.register_count(),
                    actual: args.len(),
                });
            }
            Ok(())
        }
        Instruction::Dispatch {
            dst,
            selector,
            roles,
            ..
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operands(register_count, roles.iter().map(|(_, operand)| operand))
        }
        Instruction::Commit | Instruction::Suspend { .. } | Instruction::RollbackRetry => Ok(()),
    }
}

fn validate_bindings(
    register_count: usize,
    bindings: &[Option<Operand>],
) -> Result<(), RuntimeError> {
    validate_operands(register_count, bindings.iter().filter_map(Option::as_ref))
}

fn validate_operands<'a>(
    register_count: usize,
    operands: impl IntoIterator<Item = &'a Operand>,
) -> Result<(), RuntimeError> {
    for operand in operands {
        validate_operand(register_count, operand)?;
    }
    Ok(())
}

fn validate_operand(register_count: usize, operand: &Operand) -> Result<(), RuntimeError> {
    match operand {
        Operand::Register(register) => validate_register(register_count, *register),
        Operand::Value(_) => Ok(()),
    }
}

fn validate_register(register_count: usize, register: Register) -> Result<(), RuntimeError> {
    if register.0 as usize >= register_count {
        return Err(RuntimeError::RegisterOutOfBounds {
            register: register.0,
            register_count,
        });
    }
    Ok(())
}

fn validate_target(instruction_count: usize, target: usize) -> Result<(), RuntimeError> {
    if target >= instruction_count {
        return Err(RuntimeError::InvalidBranchTarget {
            target,
            instruction_count,
        });
    }
    Ok(())
}

const INST_LOAD: u8 = 0;
const INST_MOVE: u8 = 1;
const INST_SCAN_EXISTS: u8 = 2;
const INST_ASSERT: u8 = 3;
const INST_RETRACT: u8 = 4;
const INST_RETRACT_WHERE: u8 = 5;
const INST_REPLACE_FUNCTIONAL: u8 = 6;
const INST_BRANCH: u8 = 7;
const INST_JUMP: u8 = 8;
const INST_EMIT: u8 = 9;
const INST_COMMIT: u8 = 10;
const INST_SUSPEND_COMMIT: u8 = 11;
const INST_ROLLBACK_RETRY: u8 = 12;
const INST_RETURN: u8 = 13;
const INST_ABORT: u8 = 14;

const OPERAND_REGISTER: u8 = 0;
const OPERAND_VALUE: u8 = 1;

const VALUE_NOTHING: u8 = 0;
const VALUE_BOOL: u8 = 1;
const VALUE_INT: u8 = 2;
const VALUE_FLOAT: u8 = 3;
const VALUE_IDENTITY: u8 = 4;
const VALUE_SYMBOL: u8 = 5;
const VALUE_STRING: u8 = 6;
const VALUE_BYTES: u8 = 7;

fn write_instruction(out: &mut Vec<u8>, instruction: &Instruction) -> Result<(), RuntimeError> {
    match instruction {
        Instruction::Load { dst, value } => {
            out.push(INST_LOAD);
            write_register(out, *dst);
            write_value(out, value)
        }
        Instruction::Move { dst, src } => {
            out.push(INST_MOVE);
            write_register(out, *dst);
            write_register(out, *src);
            Ok(())
        }
        Instruction::ScanExists {
            dst,
            relation,
            bindings,
        } => {
            out.push(INST_SCAN_EXISTS);
            write_register(out, *dst);
            write_identity(out, *relation);
            write_optional_operands(out, bindings)
        }
        Instruction::Assert { relation, values } => {
            out.push(INST_ASSERT);
            write_identity(out, *relation);
            write_operands(out, values)
        }
        Instruction::Retract { relation, values } => {
            out.push(INST_RETRACT);
            write_identity(out, *relation);
            write_operands(out, values)
        }
        Instruction::RetractWhere { relation, bindings } => {
            out.push(INST_RETRACT_WHERE);
            write_identity(out, *relation);
            write_optional_operands(out, bindings)
        }
        Instruction::ReplaceFunctional { relation, values } => {
            out.push(INST_REPLACE_FUNCTIONAL);
            write_identity(out, *relation);
            write_operands(out, values)
        }
        Instruction::Branch {
            condition,
            if_true,
            if_false,
        } => {
            out.push(INST_BRANCH);
            write_register(out, *condition);
            write_u32(out, *if_true as u32);
            write_u32(out, *if_false as u32);
            Ok(())
        }
        Instruction::Jump { target } => {
            out.push(INST_JUMP);
            write_u32(out, *target as u32);
            Ok(())
        }
        Instruction::Emit { value } => {
            out.push(INST_EMIT);
            write_operand(out, value)
        }
        Instruction::Commit => {
            out.push(INST_COMMIT);
            Ok(())
        }
        Instruction::Suspend { kind } => match kind {
            SuspendKind::Commit => {
                out.push(INST_SUSPEND_COMMIT);
                Ok(())
            }
            SuspendKind::TimedMillis(_) | SuspendKind::WaitingForInput(_) => Err(artifact_error(
                "only commit suspension is serializable in program artifacts",
            )),
        },
        Instruction::RollbackRetry => {
            out.push(INST_ROLLBACK_RETRY);
            Ok(())
        }
        Instruction::Return { value } => {
            out.push(INST_RETURN);
            write_operand(out, value)
        }
        Instruction::Abort { error } => {
            out.push(INST_ABORT);
            write_operand(out, error)
        }
        Instruction::Call { .. } | Instruction::Dispatch { .. } => Err(artifact_error(
            "nested call and dispatch instructions are not serializable in program artifacts yet",
        )),
    }
}

fn write_register(out: &mut Vec<u8>, register: Register) {
    write_u16(out, register.0);
}

fn write_operands(out: &mut Vec<u8>, operands: &[Operand]) -> Result<(), RuntimeError> {
    write_u32(out, operands.len() as u32);
    for operand in operands {
        write_operand(out, operand)?;
    }
    Ok(())
}

fn write_optional_operands(
    out: &mut Vec<u8>,
    operands: &[Option<Operand>],
) -> Result<(), RuntimeError> {
    write_u32(out, operands.len() as u32);
    for operand in operands {
        match operand {
            Some(operand) => {
                out.push(1);
                write_operand(out, operand)?;
            }
            None => out.push(0),
        }
    }
    Ok(())
}

fn write_operand(out: &mut Vec<u8>, operand: &Operand) -> Result<(), RuntimeError> {
    match operand {
        Operand::Register(register) => {
            out.push(OPERAND_REGISTER);
            write_register(out, *register);
            Ok(())
        }
        Operand::Value(value) => {
            out.push(OPERAND_VALUE);
            write_value(out, value)
        }
    }
}

fn write_value(out: &mut Vec<u8>, value: &Value) -> Result<(), RuntimeError> {
    if let Some(value) = value.as_bool() {
        out.push(VALUE_BOOL);
        out.push(value as u8);
    } else if let Some(value) = value.as_int() {
        out.push(VALUE_INT);
        write_i64(out, value);
    } else if let Some(value) = value.as_float() {
        out.push(VALUE_FLOAT);
        write_f64(out, value);
    } else if let Some(value) = value.as_identity() {
        out.push(VALUE_IDENTITY);
        write_identity(out, value);
    } else if let Some(value) = value.as_symbol() {
        let Some(name) = value.name() else {
            return Err(artifact_error("cannot serialize unnamed symbol"));
        };
        out.push(VALUE_SYMBOL);
        write_str(out, name);
    } else if value.kind() == mica_var::ValueKind::Nothing {
        out.push(VALUE_NOTHING);
    } else if let Some(()) = value.with_str(|text| {
        out.push(VALUE_STRING);
        write_str(out, text);
    }) {
    } else if let Some(()) = value.with_bytes(|bytes| {
        out.push(VALUE_BYTES);
        write_bytes(out, bytes);
    }) {
    } else {
        return Err(artifact_error(
            "list and map values are not serializable in program artifacts yet",
        ));
    }
    Ok(())
}

fn write_identity(out: &mut Vec<u8>, identity: Identity) {
    write_u64(out, identity.raw());
}

fn write_str(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_f64(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8]) -> Result<(), RuntimeError> {
        let bytes = self.read_exact(magic.len())?;
        if bytes != magic {
            return Err(artifact_error("invalid program artifact magic"));
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_instruction(&mut self) -> Result<Instruction, RuntimeError> {
        Ok(match self.read_u8()? {
            INST_LOAD => Instruction::Load {
                dst: self.read_register()?,
                value: self.read_value()?,
            },
            INST_MOVE => Instruction::Move {
                dst: self.read_register()?,
                src: self.read_register()?,
            },
            INST_SCAN_EXISTS => Instruction::ScanExists {
                dst: self.read_register()?,
                relation: self.read_identity()?,
                bindings: self.read_optional_operands()?,
            },
            INST_ASSERT => Instruction::Assert {
                relation: self.read_identity()?,
                values: self.read_operands()?,
            },
            INST_RETRACT => Instruction::Retract {
                relation: self.read_identity()?,
                values: self.read_operands()?,
            },
            INST_RETRACT_WHERE => Instruction::RetractWhere {
                relation: self.read_identity()?,
                bindings: self.read_optional_operands()?,
            },
            INST_REPLACE_FUNCTIONAL => Instruction::ReplaceFunctional {
                relation: self.read_identity()?,
                values: self.read_operands()?,
            },
            INST_BRANCH => Instruction::Branch {
                condition: self.read_register()?,
                if_true: self.read_u32()? as usize,
                if_false: self.read_u32()? as usize,
            },
            INST_JUMP => Instruction::Jump {
                target: self.read_u32()? as usize,
            },
            INST_EMIT => Instruction::Emit {
                value: self.read_operand()?,
            },
            INST_COMMIT => Instruction::Commit,
            INST_SUSPEND_COMMIT => Instruction::Suspend {
                kind: SuspendKind::Commit,
            },
            INST_ROLLBACK_RETRY => Instruction::RollbackRetry,
            INST_RETURN => Instruction::Return {
                value: self.read_operand()?,
            },
            INST_ABORT => Instruction::Abort {
                error: self.read_operand()?,
            },
            _ => return Err(artifact_error("unknown program artifact instruction tag")),
        })
    }

    fn read_operands(&mut self) -> Result<Vec<Operand>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count).map(|_| self.read_operand()).collect()
    }

    fn read_optional_operands(&mut self) -> Result<Vec<Option<Operand>>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| match self.read_u8()? {
                0 => Ok(None),
                1 => self.read_operand().map(Some),
                _ => Err(artifact_error("invalid optional operand tag")),
            })
            .collect()
    }

    fn read_operand(&mut self) -> Result<Operand, RuntimeError> {
        match self.read_u8()? {
            OPERAND_REGISTER => self.read_register().map(Operand::Register),
            OPERAND_VALUE => self.read_value().map(Operand::Value),
            _ => Err(artifact_error("unknown operand tag")),
        }
    }

    fn read_value(&mut self) -> Result<Value, RuntimeError> {
        Ok(match self.read_u8()? {
            VALUE_NOTHING => Value::nothing(),
            VALUE_BOOL => Value::bool(self.read_u8()? != 0),
            VALUE_INT => Value::int(self.read_i64()?).map_err(|error| {
                artifact_error(format!("invalid serialized integer value: {error:?}"))
            })?,
            VALUE_FLOAT => Value::float(self.read_f64()?),
            VALUE_IDENTITY => Value::identity(self.read_identity()?),
            VALUE_SYMBOL => Value::symbol(Symbol::intern(&self.read_string()?)),
            VALUE_STRING => Value::string(self.read_string()?),
            VALUE_BYTES => Value::bytes(self.read_bytes()?),
            _ => return Err(artifact_error("unknown value tag")),
        })
    }

    fn read_register(&mut self) -> Result<Register, RuntimeError> {
        self.read_u16().map(Register)
    }

    fn read_identity(&mut self) -> Result<Identity, RuntimeError> {
        let raw = self.read_u64()?;
        Identity::new(raw).ok_or_else(|| artifact_error("identity payload out of range"))
    }

    fn read_string(&mut self) -> Result<String, RuntimeError> {
        String::from_utf8(self.read_bytes()?)
            .map_err(|error| artifact_error(format!("invalid utf8 in program artifact: {error}")))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, RuntimeError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len).map(<[u8]>::to_vec)
    }

    fn read_u8(&mut self) -> Result<u8, RuntimeError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, RuntimeError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, RuntimeError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, RuntimeError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i64(&mut self) -> Result<i64, RuntimeError> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_f64(&mut self) -> Result<f64, RuntimeError> {
        let bytes = self.read_exact(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], RuntimeError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| artifact_error("program artifact offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| artifact_error("truncated program artifact"))?;
        self.offset = end;
        Ok(bytes)
    }
}

fn artifact_error(message: impl Into<String>) -> RuntimeError {
    RuntimeError::ProgramArtifact(message.into())
}
