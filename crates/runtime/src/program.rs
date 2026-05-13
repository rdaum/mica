use crate::RuntimeError;
use mica_relation_kernel::{DispatchRelations, RelationId, RelationRead};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

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
pub enum ListItem {
    Value(Operand),
    Splice(Operand),
}

impl ListItem {
    fn operand(&self) -> &Operand {
        match self {
            Self::Value(operand) | Self::Splice(operand) => operand,
        }
    }
}

impl From<Operand> for ListItem {
    fn from(value: Operand) -> Self {
        Self::Value(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchHandler {
    pub code: Option<Value>,
    pub binding: Option<Register>,
    pub target: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorField {
    Code,
    Message,
    Value,
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
    Unary {
        dst: Register,
        op: RuntimeUnaryOp,
        src: Register,
    },
    Binary {
        dst: Register,
        op: RuntimeBinaryOp,
        left: Register,
        right: Register,
    },
    BuildList {
        dst: Register,
        items: Vec<ListItem>,
    },
    BuildMap {
        dst: Register,
        entries: Vec<(Operand, Operand)>,
    },
    BuildRange {
        dst: Register,
        start: Operand,
        end: Option<Operand>,
    },
    Index {
        dst: Register,
        collection: Register,
        index: Operand,
    },
    SetIndex {
        dst: Register,
        collection: Register,
        index: Operand,
        value: Operand,
    },
    ErrorField {
        dst: Register,
        error: Register,
        field: ErrorField,
    },
    CollectionLen {
        dst: Register,
        collection: Register,
    },
    CollectionKeyAt {
        dst: Register,
        collection: Register,
        index: Register,
    },
    CollectionValueAt {
        dst: Register,
        collection: Register,
        index: Register,
    },
    ScanExists {
        dst: Register,
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
    },
    ScanValue {
        dst: Register,
        relation: RelationId,
        key: Operand,
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
    EnterTry {
        catches: Vec<CatchHandler>,
        finally: Option<usize>,
        end: usize,
    },
    ExitTry,
    EndFinally,
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
        program_bytes: RelationId,
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
    Raise {
        error: Operand,
        message: Option<Operand>,
        value: Option<Operand>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeUnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeBinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Rem,
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

#[derive(Debug, Default)]
pub struct ProgramResolver {
    cache: RwLock<BTreeMap<Value, Arc<Program>>>,
}

impl ProgramResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_program(mut self, method: Value, program: Program) -> Self {
        self.insert(method, program);
        self
    }

    pub fn insert(&mut self, method: Value, program: Program) -> Option<Arc<Program>> {
        self.cache
            .write()
            .unwrap()
            .insert(method, Arc::new(program))
    }

    pub fn get(&self, method: &Value) -> Option<Arc<Program>> {
        self.cache.read().unwrap().get(method).cloned()
    }

    pub fn contains(&self, method: &Value) -> bool {
        self.cache.read().unwrap().contains_key(method)
    }

    pub fn len(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.read().unwrap().is_empty()
    }

    pub fn resolve(
        &self,
        reader: &impl RelationRead,
        program_bytes_relation: RelationId,
        program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError> {
        if let Some(program) = self.get(program_id) {
            return Ok(program);
        }

        let rows =
            reader.scan_relation(program_bytes_relation, &[Some(program_id.clone()), None])?;
        let bytes = rows
            .first()
            .and_then(|row| row.values()[1].with_bytes(<[u8]>::to_vec))
            .ok_or_else(|| RuntimeError::MissingProgramArtifact {
                program: program_id.clone(),
            })?;
        let program = Arc::new(Program::from_bytes(&bytes)?);
        self.cache
            .write()
            .unwrap()
            .insert(program_id.clone(), program.clone());
        Ok(program)
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
        Instruction::Unary { dst, src, .. } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *src)
        }
        Instruction::Binary {
            dst, left, right, ..
        } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *left)?;
            validate_register(register_count, *right)
        }
        Instruction::BuildList { dst, items } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, items.iter().map(ListItem::operand))
        }
        Instruction::BuildMap { dst, entries } => {
            validate_register(register_count, *dst)?;
            validate_operands(
                register_count,
                entries
                    .iter()
                    .flat_map(|(key, value)| [key, value].into_iter()),
            )
        }
        Instruction::BuildRange { dst, start, end } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, start)?;
            validate_operands(register_count, end.iter())
        }
        Instruction::Index {
            dst,
            collection,
            index,
        } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *collection)?;
            validate_operand(register_count, index)
        }
        Instruction::SetIndex {
            dst,
            collection,
            index,
            value,
        } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *collection)?;
            validate_operand(register_count, index)?;
            validate_operand(register_count, value)
        }
        Instruction::ErrorField { dst, error, .. } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *error)
        }
        Instruction::CollectionLen { dst, collection }
        | Instruction::CollectionKeyAt {
            dst, collection, ..
        }
        | Instruction::CollectionValueAt {
            dst, collection, ..
        } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *collection)?;
            match instruction {
                Instruction::CollectionKeyAt { index, .. }
                | Instruction::CollectionValueAt { index, .. } => {
                    validate_register(register_count, *index)
                }
                _ => Ok(()),
            }
        }
        Instruction::ScanExists { dst, bindings, .. } => {
            validate_register(register_count, *dst)?;
            validate_bindings(register_count, bindings)
        }
        Instruction::ScanValue { dst, key, .. } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, key)
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
        Instruction::EnterTry {
            catches,
            finally,
            end,
        } => {
            validate_target(instruction_count, *end)?;
            if let Some(finally) = finally {
                validate_target(instruction_count, *finally)?;
            }
            for catch in catches {
                validate_target(instruction_count, catch.target)?;
                if let Some(binding) = catch.binding {
                    validate_register(register_count, binding)?;
                }
            }
            Ok(())
        }
        Instruction::ExitTry | Instruction::EndFinally => Ok(()),
        Instruction::Emit { value }
        | Instruction::Return { value }
        | Instruction::Abort { error: value } => validate_operand(register_count, value),
        Instruction::Raise {
            error,
            message,
            value,
        } => {
            validate_operand(register_count, error)?;
            validate_operands(register_count, message.iter())?;
            validate_operands(register_count, value.iter())
        }
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
const INST_UNARY: u8 = 2;
const INST_BINARY: u8 = 3;
const INST_SCAN_EXISTS: u8 = 4;
const INST_ASSERT: u8 = 5;
const INST_RETRACT: u8 = 6;
const INST_RETRACT_WHERE: u8 = 7;
const INST_REPLACE_FUNCTIONAL: u8 = 8;
const INST_BRANCH: u8 = 9;
const INST_JUMP: u8 = 10;
const INST_EMIT: u8 = 11;
const INST_COMMIT: u8 = 12;
const INST_SUSPEND_COMMIT: u8 = 13;
const INST_ROLLBACK_RETRY: u8 = 14;
const INST_RETURN: u8 = 15;
const INST_ABORT: u8 = 16;
const INST_DISPATCH: u8 = 17;
const INST_BUILD_LIST: u8 = 18;
const INST_BUILD_MAP: u8 = 19;
const INST_INDEX: u8 = 20;
const INST_COLLECTION_LEN: u8 = 21;
const INST_COLLECTION_KEY_AT: u8 = 22;
const INST_COLLECTION_VALUE_AT: u8 = 23;
const INST_SET_INDEX: u8 = 24;
const INST_SCAN_VALUE: u8 = 25;
const INST_CALL: u8 = 26;
const INST_BUILD_RANGE: u8 = 27;
const INST_ENTER_TRY: u8 = 28;
const INST_EXIT_TRY: u8 = 29;
const INST_END_FINALLY: u8 = 30;
const INST_RAISE: u8 = 31;
const INST_ERROR_FIELD: u8 = 32;

const UNARY_NOT: u8 = 0;
const UNARY_NEG: u8 = 1;

const ERROR_FIELD_CODE: u8 = 0;
const ERROR_FIELD_MESSAGE: u8 = 1;
const ERROR_FIELD_VALUE: u8 = 2;

const BINARY_EQ: u8 = 0;
const BINARY_NE: u8 = 1;
const BINARY_LT: u8 = 2;
const BINARY_LE: u8 = 3;
const BINARY_GT: u8 = 4;
const BINARY_GE: u8 = 5;
const BINARY_ADD: u8 = 6;
const BINARY_SUB: u8 = 7;
const BINARY_MUL: u8 = 8;
const BINARY_DIV: u8 = 9;
const BINARY_REM: u8 = 10;

const OPERAND_REGISTER: u8 = 0;
const OPERAND_VALUE: u8 = 1;

const LIST_ITEM_VALUE: u8 = 0;
const LIST_ITEM_SPLICE: u8 = 1;

const VALUE_NOTHING: u8 = 0;
const VALUE_BOOL: u8 = 1;
const VALUE_INT: u8 = 2;
const VALUE_FLOAT: u8 = 3;
const VALUE_IDENTITY: u8 = 4;
const VALUE_SYMBOL: u8 = 5;
const VALUE_ERROR_CODE: u8 = 6;
const VALUE_STRING: u8 = 7;
const VALUE_BYTES: u8 = 8;
const VALUE_ERROR: u8 = 9;

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
        Instruction::Unary { dst, op, src } => {
            out.push(INST_UNARY);
            write_register(out, *dst);
            write_unary_op(out, *op);
            write_register(out, *src);
            Ok(())
        }
        Instruction::Binary {
            dst,
            op,
            left,
            right,
        } => {
            out.push(INST_BINARY);
            write_register(out, *dst);
            write_binary_op(out, *op);
            write_register(out, *left);
            write_register(out, *right);
            Ok(())
        }
        Instruction::BuildList { dst, items } => {
            out.push(INST_BUILD_LIST);
            write_register(out, *dst);
            write_list_items(out, items)
        }
        Instruction::BuildMap { dst, entries } => {
            out.push(INST_BUILD_MAP);
            write_register(out, *dst);
            write_u32(out, entries.len() as u32);
            for (key, value) in entries {
                write_operand(out, key)?;
                write_operand(out, value)?;
            }
            Ok(())
        }
        Instruction::BuildRange { dst, start, end } => {
            out.push(INST_BUILD_RANGE);
            write_register(out, *dst);
            write_operand(out, start)?;
            write_optional_operand(out, end.as_ref())
        }
        Instruction::Index {
            dst,
            collection,
            index,
        } => {
            out.push(INST_INDEX);
            write_register(out, *dst);
            write_register(out, *collection);
            write_operand(out, index)
        }
        Instruction::SetIndex {
            dst,
            collection,
            index,
            value,
        } => {
            out.push(INST_SET_INDEX);
            write_register(out, *dst);
            write_register(out, *collection);
            write_operand(out, index)?;
            write_operand(out, value)
        }
        Instruction::ErrorField { dst, error, field } => {
            out.push(INST_ERROR_FIELD);
            write_register(out, *dst);
            write_register(out, *error);
            write_error_field(out, *field);
            Ok(())
        }
        Instruction::CollectionLen { dst, collection } => {
            out.push(INST_COLLECTION_LEN);
            write_register(out, *dst);
            write_register(out, *collection);
            Ok(())
        }
        Instruction::CollectionKeyAt {
            dst,
            collection,
            index,
        } => {
            out.push(INST_COLLECTION_KEY_AT);
            write_register(out, *dst);
            write_register(out, *collection);
            write_register(out, *index);
            Ok(())
        }
        Instruction::CollectionValueAt {
            dst,
            collection,
            index,
        } => {
            out.push(INST_COLLECTION_VALUE_AT);
            write_register(out, *dst);
            write_register(out, *collection);
            write_register(out, *index);
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
        Instruction::ScanValue { dst, relation, key } => {
            out.push(INST_SCAN_VALUE);
            write_register(out, *dst);
            write_identity(out, *relation);
            write_operand(out, key)
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
        Instruction::EnterTry {
            catches,
            finally,
            end,
        } => {
            out.push(INST_ENTER_TRY);
            write_u32(out, *end as u32);
            write_optional_target(out, *finally);
            write_catch_handlers(out, catches)
        }
        Instruction::ExitTry => {
            out.push(INST_EXIT_TRY);
            Ok(())
        }
        Instruction::EndFinally => {
            out.push(INST_END_FINALLY);
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
        Instruction::Raise {
            error,
            message,
            value,
        } => {
            out.push(INST_RAISE);
            write_operand(out, error)?;
            write_optional_operand(out, message.as_ref())?;
            write_optional_operand(out, value.as_ref())
        }
        Instruction::Dispatch {
            dst,
            relations,
            program_relation,
            program_bytes,
            selector,
            roles,
        } => {
            out.push(INST_DISPATCH);
            write_register(out, *dst);
            write_identity(out, relations.method_selector);
            write_identity(out, relations.param);
            write_identity(out, relations.delegates);
            write_identity(out, *program_relation);
            write_identity(out, *program_bytes);
            write_operand(out, selector)?;
            write_u32(out, roles.len() as u32);
            for (role, operand) in roles {
                write_value(out, role)?;
                write_operand(out, operand)?;
            }
            Ok(())
        }
        Instruction::Call { dst, program, args } => {
            out.push(INST_CALL);
            write_register(out, *dst);
            write_bytes(out, &program.to_bytes()?);
            write_operands(out, args)
        }
    }
}

fn write_register(out: &mut Vec<u8>, register: Register) {
    write_u16(out, register.0);
}

fn write_unary_op(out: &mut Vec<u8>, op: RuntimeUnaryOp) {
    out.push(match op {
        RuntimeUnaryOp::Not => UNARY_NOT,
        RuntimeUnaryOp::Neg => UNARY_NEG,
    });
}

fn write_binary_op(out: &mut Vec<u8>, op: RuntimeBinaryOp) {
    out.push(match op {
        RuntimeBinaryOp::Eq => BINARY_EQ,
        RuntimeBinaryOp::Ne => BINARY_NE,
        RuntimeBinaryOp::Lt => BINARY_LT,
        RuntimeBinaryOp::Le => BINARY_LE,
        RuntimeBinaryOp::Gt => BINARY_GT,
        RuntimeBinaryOp::Ge => BINARY_GE,
        RuntimeBinaryOp::Add => BINARY_ADD,
        RuntimeBinaryOp::Sub => BINARY_SUB,
        RuntimeBinaryOp::Mul => BINARY_MUL,
        RuntimeBinaryOp::Div => BINARY_DIV,
        RuntimeBinaryOp::Rem => BINARY_REM,
    });
}

fn write_error_field(out: &mut Vec<u8>, field: ErrorField) {
    out.push(match field {
        ErrorField::Code => ERROR_FIELD_CODE,
        ErrorField::Message => ERROR_FIELD_MESSAGE,
        ErrorField::Value => ERROR_FIELD_VALUE,
    });
}

fn write_operands(out: &mut Vec<u8>, operands: &[Operand]) -> Result<(), RuntimeError> {
    write_u32(out, operands.len() as u32);
    for operand in operands {
        write_operand(out, operand)?;
    }
    Ok(())
}

fn write_list_items(out: &mut Vec<u8>, items: &[ListItem]) -> Result<(), RuntimeError> {
    write_u32(out, items.len() as u32);
    for item in items {
        match item {
            ListItem::Value(operand) => {
                out.push(LIST_ITEM_VALUE);
                write_operand(out, operand)?;
            }
            ListItem::Splice(operand) => {
                out.push(LIST_ITEM_SPLICE);
                write_operand(out, operand)?;
            }
        }
    }
    Ok(())
}

fn write_catch_handlers(out: &mut Vec<u8>, catches: &[CatchHandler]) -> Result<(), RuntimeError> {
    write_u32(out, catches.len() as u32);
    for catch in catches {
        write_optional_value(out, catch.code.as_ref())?;
        match catch.binding {
            Some(binding) => {
                out.push(1);
                write_register(out, binding);
            }
            None => out.push(0),
        }
        write_u32(out, catch.target as u32);
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

fn write_optional_operand(
    out: &mut Vec<u8>,
    operand: Option<&Operand>,
) -> Result<(), RuntimeError> {
    match operand {
        Some(operand) => {
            out.push(1);
            write_operand(out, operand)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn write_optional_target(out: &mut Vec<u8>, target: Option<usize>) {
    match target {
        Some(target) => {
            out.push(1);
            write_u32(out, target as u32);
        }
        None => out.push(0),
    }
}

fn write_optional_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            write_str(out, value);
        }
        None => out.push(0),
    }
}

fn write_optional_value(out: &mut Vec<u8>, value: Option<&Value>) -> Result<(), RuntimeError> {
    match value {
        Some(value) => {
            out.push(1);
            write_value(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
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
    } else if let Some(value) = value.as_error_code() {
        let Some(name) = value.name() else {
            return Err(artifact_error("cannot serialize unnamed error code"));
        };
        out.push(VALUE_ERROR_CODE);
        write_str(out, name);
    } else if let Some(result) = value.with_error(|error| {
        let Some(name) = error.code().name() else {
            return Err(artifact_error("cannot serialize unnamed error code"));
        };
        out.push(VALUE_ERROR);
        write_str(out, name);
        write_optional_str(out, error.message());
        write_optional_value(out, error.value())
    }) {
        result?;
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
            "collection values are not serializable in program artifacts yet",
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
            INST_UNARY => Instruction::Unary {
                dst: self.read_register()?,
                op: self.read_unary_op()?,
                src: self.read_register()?,
            },
            INST_BINARY => Instruction::Binary {
                dst: self.read_register()?,
                op: self.read_binary_op()?,
                left: self.read_register()?,
                right: self.read_register()?,
            },
            INST_BUILD_LIST => Instruction::BuildList {
                dst: self.read_register()?,
                items: self.read_list_items()?,
            },
            INST_BUILD_MAP => Instruction::BuildMap {
                dst: self.read_register()?,
                entries: self.read_map_entries()?,
            },
            INST_BUILD_RANGE => Instruction::BuildRange {
                dst: self.read_register()?,
                start: self.read_operand()?,
                end: self.read_optional_operand()?,
            },
            INST_INDEX => Instruction::Index {
                dst: self.read_register()?,
                collection: self.read_register()?,
                index: self.read_operand()?,
            },
            INST_SET_INDEX => Instruction::SetIndex {
                dst: self.read_register()?,
                collection: self.read_register()?,
                index: self.read_operand()?,
                value: self.read_operand()?,
            },
            INST_ERROR_FIELD => Instruction::ErrorField {
                dst: self.read_register()?,
                error: self.read_register()?,
                field: self.read_error_field()?,
            },
            INST_COLLECTION_LEN => Instruction::CollectionLen {
                dst: self.read_register()?,
                collection: self.read_register()?,
            },
            INST_COLLECTION_KEY_AT => Instruction::CollectionKeyAt {
                dst: self.read_register()?,
                collection: self.read_register()?,
                index: self.read_register()?,
            },
            INST_COLLECTION_VALUE_AT => Instruction::CollectionValueAt {
                dst: self.read_register()?,
                collection: self.read_register()?,
                index: self.read_register()?,
            },
            INST_SCAN_EXISTS => Instruction::ScanExists {
                dst: self.read_register()?,
                relation: self.read_identity()?,
                bindings: self.read_optional_operands()?,
            },
            INST_SCAN_VALUE => Instruction::ScanValue {
                dst: self.read_register()?,
                relation: self.read_identity()?,
                key: self.read_operand()?,
            },
            INST_CALL => Instruction::Call {
                dst: self.read_register()?,
                program: Arc::new(Program::from_bytes(&self.read_bytes()?)?),
                args: self.read_operands()?,
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
            INST_ENTER_TRY => Instruction::EnterTry {
                end: self.read_u32()? as usize,
                finally: self.read_optional_target()?,
                catches: self.read_catch_handlers()?,
            },
            INST_EXIT_TRY => Instruction::ExitTry,
            INST_END_FINALLY => Instruction::EndFinally,
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
            INST_RAISE => Instruction::Raise {
                error: self.read_operand()?,
                message: self.read_optional_operand()?,
                value: self.read_optional_operand()?,
            },
            INST_DISPATCH => Instruction::Dispatch {
                dst: self.read_register()?,
                relations: DispatchRelations {
                    method_selector: self.read_identity()?,
                    param: self.read_identity()?,
                    delegates: self.read_identity()?,
                },
                program_relation: self.read_identity()?,
                program_bytes: self.read_identity()?,
                selector: self.read_operand()?,
                roles: self.read_dispatch_roles()?,
            },
            _ => return Err(artifact_error("unknown program artifact instruction tag")),
        })
    }

    fn read_operands(&mut self) -> Result<Vec<Operand>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count).map(|_| self.read_operand()).collect()
    }

    fn read_list_items(&mut self) -> Result<Vec<ListItem>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| match self.read_u8()? {
                LIST_ITEM_VALUE => self.read_operand().map(ListItem::Value),
                LIST_ITEM_SPLICE => self.read_operand().map(ListItem::Splice),
                _ => Err(artifact_error("unknown list item tag")),
            })
            .collect()
    }

    fn read_catch_handlers(&mut self) -> Result<Vec<CatchHandler>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| {
                Ok(CatchHandler {
                    code: self.read_optional_value()?,
                    binding: match self.read_u8()? {
                        0 => None,
                        1 => Some(self.read_register()?),
                        _ => return Err(artifact_error("invalid optional catch binding tag")),
                    },
                    target: self.read_u32()? as usize,
                })
            })
            .collect()
    }

    fn read_optional_operands(&mut self) -> Result<Vec<Option<Operand>>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count).map(|_| self.read_optional_operand()).collect()
    }

    fn read_optional_operand(&mut self) -> Result<Option<Operand>, RuntimeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_operand().map(Some),
            _ => Err(artifact_error("invalid optional operand tag")),
        }
    }

    fn read_optional_target(&mut self) -> Result<Option<usize>, RuntimeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u32().map(|target| Some(target as usize)),
            _ => Err(artifact_error("invalid optional target tag")),
        }
    }

    fn read_operand(&mut self) -> Result<Operand, RuntimeError> {
        match self.read_u8()? {
            OPERAND_REGISTER => self.read_register().map(Operand::Register),
            OPERAND_VALUE => self.read_value().map(Operand::Value),
            _ => Err(artifact_error("unknown operand tag")),
        }
    }

    fn read_dispatch_roles(&mut self) -> Result<Vec<(Value, Operand)>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| Ok((self.read_value()?, self.read_operand()?)))
            .collect()
    }

    fn read_map_entries(&mut self) -> Result<Vec<(Operand, Operand)>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| Ok((self.read_operand()?, self.read_operand()?)))
            .collect()
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
            VALUE_ERROR_CODE => Value::error_code(Symbol::intern(&self.read_string()?)),
            VALUE_STRING => Value::string(self.read_string()?),
            VALUE_BYTES => Value::bytes(self.read_bytes()?),
            VALUE_ERROR => {
                let code = Symbol::intern(&self.read_string()?);
                let message = self.read_optional_string()?;
                let value = self.read_optional_value()?;
                Value::error(code, message, value)
            }
            _ => return Err(artifact_error("unknown value tag")),
        })
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, RuntimeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string().map(Some),
            _ => Err(artifact_error("invalid optional string tag")),
        }
    }

    fn read_optional_value(&mut self) -> Result<Option<Value>, RuntimeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_value().map(Some),
            _ => Err(artifact_error("invalid optional value tag")),
        }
    }

    fn read_register(&mut self) -> Result<Register, RuntimeError> {
        self.read_u16().map(Register)
    }

    fn read_unary_op(&mut self) -> Result<RuntimeUnaryOp, RuntimeError> {
        match self.read_u8()? {
            UNARY_NOT => Ok(RuntimeUnaryOp::Not),
            UNARY_NEG => Ok(RuntimeUnaryOp::Neg),
            _ => Err(artifact_error("unknown unary operator tag")),
        }
    }

    fn read_binary_op(&mut self) -> Result<RuntimeBinaryOp, RuntimeError> {
        match self.read_u8()? {
            BINARY_EQ => Ok(RuntimeBinaryOp::Eq),
            BINARY_NE => Ok(RuntimeBinaryOp::Ne),
            BINARY_LT => Ok(RuntimeBinaryOp::Lt),
            BINARY_LE => Ok(RuntimeBinaryOp::Le),
            BINARY_GT => Ok(RuntimeBinaryOp::Gt),
            BINARY_GE => Ok(RuntimeBinaryOp::Ge),
            BINARY_ADD => Ok(RuntimeBinaryOp::Add),
            BINARY_SUB => Ok(RuntimeBinaryOp::Sub),
            BINARY_MUL => Ok(RuntimeBinaryOp::Mul),
            BINARY_DIV => Ok(RuntimeBinaryOp::Div),
            BINARY_REM => Ok(RuntimeBinaryOp::Rem),
            _ => Err(artifact_error("unknown binary operator tag")),
        }
    }

    fn read_error_field(&mut self) -> Result<ErrorField, RuntimeError> {
        match self.read_u8()? {
            ERROR_FIELD_CODE => Ok(ErrorField::Code),
            ERROR_FIELD_MESSAGE => Ok(ErrorField::Message),
            ERROR_FIELD_VALUE => Ok(ErrorField::Value),
            _ => Err(artifact_error("unknown error field tag")),
        }
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
