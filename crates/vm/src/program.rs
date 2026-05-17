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

use crate::RuntimeError;
use arc_swap::ArcSwap;
use mica_relation_kernel::{DispatchRelations, RelationId, RelationRead};
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
pub struct QueryBinding {
    pub name: Symbol,
    pub position: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationArg {
    Value(Operand),
    Splice(Operand),
    Query(Symbol),
    Hole,
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
pub struct SpawnRequest {
    pub selector: Symbol,
    pub target: SpawnTarget,
    pub delay_millis: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpawnTarget {
    NamedRoles(Vec<(Symbol, Value)>),
    PositionalArgs(Vec<Value>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxRecvRequest {
    pub receivers: Vec<Value>,
    pub timeout_millis: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxSend {
    pub sender: Value,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuspendKind {
    Commit,
    Never,
    TimedMillis(u64),
    WaitingForInput(Value),
    MailboxRecv(MailboxRecvRequest),
    Spawn(SpawnRequest),
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
    One {
        dst: Register,
        src: Register,
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
    ScanBindings {
        dst: Register,
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
        outputs: Vec<QueryBinding>,
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
    ScanDynamic {
        dst: Register,
        relation: RelationId,
        args: Vec<RelationArg>,
    },
    AssertDynamic {
        relation: RelationId,
        args: Vec<RelationArg>,
    },
    RetractDynamic {
        relation: RelationId,
        args: Vec<RelationArg>,
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
        target: Operand,
        value: Operand,
    },
    LoadFunction {
        dst: Register,
        program: Arc<Program>,
        captures: Vec<Operand>,
        min_arity: u16,
        max_arity: u16,
    },
    CallValue {
        dst: Register,
        callee: Operand,
        args: Vec<Operand>,
    },
    CallValueDynamic {
        dst: Register,
        callee: Operand,
        args: Vec<ListItem>,
    },
    Call {
        dst: Register,
        program: Arc<Program>,
        args: Vec<Operand>,
    },
    BuiltinCall {
        dst: Register,
        name: Symbol,
        args: Vec<Operand>,
    },
    BuiltinCallDynamic {
        dst: Register,
        name: Symbol,
        args: Vec<ListItem>,
    },
    Dispatch {
        dst: Register,
        relations: DispatchRelations,
        program_relation: RelationId,
        program_bytes: RelationId,
        selector: Operand,
        roles: Vec<(Value, Operand)>,
    },
    DynamicDispatch {
        dst: Register,
        relations: DispatchRelations,
        program_relation: RelationId,
        program_bytes: RelationId,
        selector: Operand,
        roles: Operand,
    },
    PositionalDispatch {
        dst: Register,
        relations: DispatchRelations,
        program_relation: RelationId,
        program_bytes: RelationId,
        selector: Operand,
        args: Vec<Operand>,
    },
    PositionalDispatchDynamic {
        dst: Register,
        relations: DispatchRelations,
        program_relation: RelationId,
        program_bytes: RelationId,
        selector: Operand,
        args: Vec<ListItem>,
    },
    SpawnDispatch {
        dst: Register,
        selector: Operand,
        roles: Vec<(Value, Operand)>,
        delay: Option<Operand>,
    },
    SpawnPositionalDispatch {
        dst: Register,
        selector: Operand,
        args: Vec<Operand>,
        delay: Option<Operand>,
    },
    Commit,
    Suspend {
        kind: SuspendKind,
    },
    SuspendValue {
        dst: Register,
        duration: Option<Operand>,
    },
    CommitValue {
        dst: Register,
    },
    Read {
        dst: Register,
        metadata: Option<Operand>,
    },
    MailboxRecv {
        dst: Register,
        receivers: Operand,
        timeout: Option<Operand>,
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
pub(crate) struct ConstId(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProgramId(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelationRef(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DispatchSpecId(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SuspendKindId(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Target(pub(crate) u16);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableRange {
    start: u16,
    len: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperandRef {
    Register(Register),
    Constant(ConstId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactListItem {
    Value(OperandRef),
    Splice(OperandRef),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactRelationArg {
    Value(OperandRef),
    Splice(OperandRef),
    Query(Symbol),
    Hole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompactCatchHandler {
    pub(crate) code: Option<ConstId>,
    pub(crate) binding: Option<Register>,
    pub(crate) target: Target,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DispatchSpec {
    pub(crate) relations: DispatchRelations,
    pub(crate) program_relation: RelationId,
    pub(crate) program_bytes: RelationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Opcode {
    Load {
        dst: Register,
        value: ConstId,
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
        items: TableRange,
    },
    BuildMap {
        dst: Register,
        entries: TableRange,
    },
    BuildRange {
        dst: Register,
        start: OperandRef,
        end: Option<OperandRef>,
    },
    Index {
        dst: Register,
        collection: Register,
        index: OperandRef,
    },
    SetIndex {
        dst: Register,
        collection: Register,
        index: OperandRef,
        value: OperandRef,
    },
    ErrorField {
        dst: Register,
        error: Register,
        field: ErrorField,
    },
    One {
        dst: Register,
        src: Register,
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
        relation: RelationRef,
        bindings: TableRange,
    },
    ScanBindings {
        dst: Register,
        relation: RelationRef,
        bindings: TableRange,
        outputs: TableRange,
    },
    ScanValue {
        dst: Register,
        relation: RelationRef,
        key: OperandRef,
    },
    Assert {
        relation: RelationRef,
        values: TableRange,
    },
    Retract {
        relation: RelationRef,
        values: TableRange,
    },
    RetractWhere {
        relation: RelationRef,
        bindings: TableRange,
    },
    ScanDynamic {
        dst: Register,
        relation: RelationRef,
        args: TableRange,
    },
    AssertDynamic {
        relation: RelationRef,
        args: TableRange,
    },
    RetractDynamic {
        relation: RelationRef,
        args: TableRange,
    },
    ReplaceFunctional {
        relation: RelationRef,
        values: TableRange,
    },
    Branch {
        condition: Register,
        if_true: Target,
        if_false: Target,
    },
    Jump {
        target: Target,
    },
    EnterTry {
        catches: TableRange,
        finally: Option<Target>,
        end: Target,
    },
    ExitTry,
    EndFinally,
    Emit {
        target: OperandRef,
        value: OperandRef,
    },
    LoadFunction {
        dst: Register,
        program: ProgramId,
        captures: TableRange,
        min_arity: u16,
        max_arity: u16,
    },
    CallValue {
        dst: Register,
        callee: OperandRef,
        args: TableRange,
    },
    CallValueDynamic {
        dst: Register,
        callee: OperandRef,
        args: TableRange,
    },
    Call {
        dst: Register,
        program: ProgramId,
        args: TableRange,
    },
    BuiltinCall {
        dst: Register,
        name: Symbol,
        args: TableRange,
    },
    BuiltinCallDynamic {
        dst: Register,
        name: Symbol,
        args: TableRange,
    },
    Dispatch {
        dst: Register,
        spec: DispatchSpecId,
        selector: OperandRef,
        roles: TableRange,
    },
    DynamicDispatch {
        dst: Register,
        spec: DispatchSpecId,
        selector: OperandRef,
        roles: OperandRef,
    },
    PositionalDispatch {
        dst: Register,
        spec: DispatchSpecId,
        selector: OperandRef,
        args: TableRange,
    },
    PositionalDispatchDynamic {
        dst: Register,
        spec: DispatchSpecId,
        selector: OperandRef,
        args: TableRange,
    },
    SpawnDispatch {
        dst: Register,
        selector: OperandRef,
        roles: TableRange,
        delay: Option<OperandRef>,
    },
    SpawnPositionalDispatch {
        dst: Register,
        selector: OperandRef,
        args: TableRange,
        delay: Option<OperandRef>,
    },
    Commit,
    Suspend {
        kind: SuspendKindId,
    },
    SuspendValue {
        dst: Register,
        duration: Option<OperandRef>,
    },
    CommitValue {
        dst: Register,
    },
    Read {
        dst: Register,
        metadata: Option<OperandRef>,
    },
    MailboxRecv {
        dst: Register,
        receivers: OperandRef,
        timeout: Option<OperandRef>,
    },
    RollbackRetry,
    Return {
        value: OperandRef,
    },
    Abort {
        error: OperandRef,
    },
    Raise {
        error: OperandRef,
        message: Option<OperandRef>,
        value: Option<OperandRef>,
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
    opcodes: Arc<[Opcode]>,
    constants: Arc<[Value]>,
    list_items: Arc<[CompactListItem]>,
    relation_args: Arc<[CompactRelationArg]>,
    map_entries: Arc<[(OperandRef, OperandRef)]>,
    operands: Arc<[OperandRef]>,
    bindings: Arc<[Option<OperandRef>]>,
    query_bindings: Arc<[QueryBinding]>,
    catches: Arc<[CompactCatchHandler]>,
    roles: Arc<[(ConstId, OperandRef)]>,
    programs: Arc<[Arc<Program>]>,
    relations: Arc<[RelationId]>,
    dispatch_specs: Arc<[DispatchSpec]>,
    suspend_kinds: Arc<[SuspendKind]>,
}

impl Program {
    pub fn new(
        register_count: usize,
        instructions: impl IntoIterator<Item = Instruction>,
    ) -> Result<Self, RuntimeError> {
        let mut builder = ProgramBuilder::new();
        for instruction in instructions {
            builder.emit(instruction)?;
        }
        builder.finish(register_count)
    }

    pub fn register_count(&self) -> usize {
        self.register_count
    }

    #[inline]
    pub fn instructions(&self) -> Vec<Instruction> {
        self.opcodes
            .iter()
            .map(|opcode| self.decode_instruction(opcode))
            .collect()
    }

    #[inline]
    pub(crate) fn opcodes(&self) -> &[Opcode] {
        &self.opcodes
    }

    #[inline]
    pub(crate) fn constant(&self, id: ConstId) -> &Value {
        &self.constants[id.0 as usize]
    }

    #[inline]
    pub(crate) fn list_items(&self, range: TableRange) -> &[CompactListItem] {
        table_range(&self.list_items, range)
    }

    #[inline]
    pub(crate) fn relation_args(&self, range: TableRange) -> &[CompactRelationArg] {
        table_range(&self.relation_args, range)
    }

    #[inline]
    pub(crate) fn map_entries(&self, range: TableRange) -> &[(OperandRef, OperandRef)] {
        table_range(&self.map_entries, range)
    }

    #[inline]
    pub(crate) fn operands(&self, range: TableRange) -> &[OperandRef] {
        table_range(&self.operands, range)
    }

    #[inline]
    pub(crate) fn bindings(&self, range: TableRange) -> &[Option<OperandRef>] {
        table_range(&self.bindings, range)
    }

    #[inline]
    pub(crate) fn query_bindings(&self, range: TableRange) -> &[QueryBinding] {
        table_range(&self.query_bindings, range)
    }

    #[inline]
    pub(crate) fn catches(&self, range: TableRange) -> &[CompactCatchHandler] {
        table_range(&self.catches, range)
    }

    #[inline]
    pub(crate) fn roles(&self, range: TableRange) -> &[(ConstId, OperandRef)] {
        table_range(&self.roles, range)
    }

    #[inline]
    pub(crate) fn program(&self, id: ProgramId) -> &Arc<Program> {
        &self.programs[id.0 as usize]
    }

    #[inline]
    pub(crate) fn relation(&self, id: RelationRef) -> RelationId {
        self.relations[id.0 as usize]
    }

    #[inline]
    pub(crate) fn dispatch_spec(&self, id: DispatchSpecId) -> DispatchSpec {
        self.dispatch_specs[id.0 as usize]
    }

    #[inline]
    pub(crate) fn suspend_kind(&self, id: SuspendKindId) -> &SuspendKind {
        &self.suspend_kinds[id.0 as usize]
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, RuntimeError> {
        let mut out = Vec::new();
        out.extend_from_slice(b"MICAPRG1");
        write_u32(&mut out, self.register_count as u32);
        write_u32(&mut out, self.opcodes.len() as u32);
        for instruction in self.instructions() {
            write_instruction(&mut out, &instruction)?;
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

    fn decode_operand(&self, operand: OperandRef) -> Operand {
        match operand {
            OperandRef::Register(register) => Operand::Register(register),
            OperandRef::Constant(id) => Operand::Value(self.constant(id).clone()),
        }
    }

    fn decode_relation_args(&self, range: TableRange) -> Vec<RelationArg> {
        self.relation_args(range)
            .iter()
            .map(|arg| match arg {
                CompactRelationArg::Value(operand) => {
                    RelationArg::Value(self.decode_operand(*operand))
                }
                CompactRelationArg::Splice(operand) => {
                    RelationArg::Splice(self.decode_operand(*operand))
                }
                CompactRelationArg::Query(name) => RelationArg::Query(*name),
                CompactRelationArg::Hole => RelationArg::Hole,
            })
            .collect()
    }

    fn decode_list_items(&self, range: TableRange) -> Vec<ListItem> {
        self.list_items(range)
            .iter()
            .map(|item| match item {
                CompactListItem::Value(operand) => ListItem::Value(self.decode_operand(*operand)),
                CompactListItem::Splice(operand) => ListItem::Splice(self.decode_operand(*operand)),
            })
            .collect()
    }

    fn decode_instruction(&self, opcode: &Opcode) -> Instruction {
        match opcode {
            Opcode::Load { dst, value } => Instruction::Load {
                dst: *dst,
                value: self.constant(*value).clone(),
            },
            Opcode::Move { dst, src } => Instruction::Move {
                dst: *dst,
                src: *src,
            },
            Opcode::Unary { dst, op, src } => Instruction::Unary {
                dst: *dst,
                op: *op,
                src: *src,
            },
            Opcode::Binary {
                dst,
                op,
                left,
                right,
            } => Instruction::Binary {
                dst: *dst,
                op: *op,
                left: *left,
                right: *right,
            },
            Opcode::BuildList { dst, items } => Instruction::BuildList {
                dst: *dst,
                items: self.decode_list_items(*items),
            },
            Opcode::BuildMap { dst, entries } => Instruction::BuildMap {
                dst: *dst,
                entries: self
                    .map_entries(*entries)
                    .iter()
                    .map(|(key, value)| (self.decode_operand(*key), self.decode_operand(*value)))
                    .collect(),
            },
            Opcode::BuildRange { dst, start, end } => Instruction::BuildRange {
                dst: *dst,
                start: self.decode_operand(*start),
                end: end.map(|operand| self.decode_operand(operand)),
            },
            Opcode::Index {
                dst,
                collection,
                index,
            } => Instruction::Index {
                dst: *dst,
                collection: *collection,
                index: self.decode_operand(*index),
            },
            Opcode::SetIndex {
                dst,
                collection,
                index,
                value,
            } => Instruction::SetIndex {
                dst: *dst,
                collection: *collection,
                index: self.decode_operand(*index),
                value: self.decode_operand(*value),
            },
            Opcode::ErrorField { dst, error, field } => Instruction::ErrorField {
                dst: *dst,
                error: *error,
                field: *field,
            },
            Opcode::One { dst, src } => Instruction::One {
                dst: *dst,
                src: *src,
            },
            Opcode::CollectionLen { dst, collection } => Instruction::CollectionLen {
                dst: *dst,
                collection: *collection,
            },
            Opcode::CollectionKeyAt {
                dst,
                collection,
                index,
            } => Instruction::CollectionKeyAt {
                dst: *dst,
                collection: *collection,
                index: *index,
            },
            Opcode::CollectionValueAt {
                dst,
                collection,
                index,
            } => Instruction::CollectionValueAt {
                dst: *dst,
                collection: *collection,
                index: *index,
            },
            Opcode::ScanExists {
                dst,
                relation,
                bindings,
            } => Instruction::ScanExists {
                dst: *dst,
                relation: self.relation(*relation),
                bindings: self
                    .bindings(*bindings)
                    .iter()
                    .map(|binding| binding.map(|operand| self.decode_operand(operand)))
                    .collect(),
            },
            Opcode::ScanBindings {
                dst,
                relation,
                bindings,
                outputs,
            } => Instruction::ScanBindings {
                dst: *dst,
                relation: self.relation(*relation),
                bindings: self
                    .bindings(*bindings)
                    .iter()
                    .map(|binding| binding.map(|operand| self.decode_operand(operand)))
                    .collect(),
                outputs: self.query_bindings(*outputs).to_vec(),
            },
            Opcode::ScanValue { dst, relation, key } => Instruction::ScanValue {
                dst: *dst,
                relation: self.relation(*relation),
                key: self.decode_operand(*key),
            },
            Opcode::Assert { relation, values } => Instruction::Assert {
                relation: self.relation(*relation),
                values: self
                    .operands(*values)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::Retract { relation, values } => Instruction::Retract {
                relation: self.relation(*relation),
                values: self
                    .operands(*values)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::RetractWhere { relation, bindings } => Instruction::RetractWhere {
                relation: self.relation(*relation),
                bindings: self
                    .bindings(*bindings)
                    .iter()
                    .map(|binding| binding.map(|operand| self.decode_operand(operand)))
                    .collect(),
            },
            Opcode::ScanDynamic {
                dst,
                relation,
                args,
            } => Instruction::ScanDynamic {
                dst: *dst,
                relation: self.relation(*relation),
                args: self.decode_relation_args(*args),
            },
            Opcode::AssertDynamic { relation, args } => Instruction::AssertDynamic {
                relation: self.relation(*relation),
                args: self.decode_relation_args(*args),
            },
            Opcode::RetractDynamic { relation, args } => Instruction::RetractDynamic {
                relation: self.relation(*relation),
                args: self.decode_relation_args(*args),
            },
            Opcode::ReplaceFunctional { relation, values } => Instruction::ReplaceFunctional {
                relation: self.relation(*relation),
                values: self
                    .operands(*values)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::Branch {
                condition,
                if_true,
                if_false,
            } => Instruction::Branch {
                condition: *condition,
                if_true: if_true.0 as usize,
                if_false: if_false.0 as usize,
            },
            Opcode::Jump { target } => Instruction::Jump {
                target: target.0 as usize,
            },
            Opcode::EnterTry {
                catches,
                finally,
                end,
            } => Instruction::EnterTry {
                catches: self
                    .catches(*catches)
                    .iter()
                    .map(|catch| CatchHandler {
                        code: catch.code.map(|id| self.constant(id).clone()),
                        binding: catch.binding,
                        target: catch.target.0 as usize,
                    })
                    .collect(),
                finally: finally.map(|target| target.0 as usize),
                end: end.0 as usize,
            },
            Opcode::ExitTry => Instruction::ExitTry,
            Opcode::EndFinally => Instruction::EndFinally,
            Opcode::Emit { target, value } => Instruction::Emit {
                target: self.decode_operand(*target),
                value: self.decode_operand(*value),
            },
            Opcode::LoadFunction {
                dst,
                program,
                captures,
                min_arity,
                max_arity,
            } => Instruction::LoadFunction {
                dst: *dst,
                program: Arc::clone(self.program(*program)),
                captures: self
                    .operands(*captures)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
                min_arity: *min_arity,
                max_arity: *max_arity,
            },
            Opcode::CallValue { dst, callee, args } => Instruction::CallValue {
                dst: *dst,
                callee: self.decode_operand(*callee),
                args: self
                    .operands(*args)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::CallValueDynamic { dst, callee, args } => Instruction::CallValueDynamic {
                dst: *dst,
                callee: self.decode_operand(*callee),
                args: self.decode_list_items(*args),
            },
            Opcode::Call { dst, program, args } => Instruction::Call {
                dst: *dst,
                program: Arc::clone(self.program(*program)),
                args: self
                    .operands(*args)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::BuiltinCall { dst, name, args } => Instruction::BuiltinCall {
                dst: *dst,
                name: *name,
                args: self
                    .operands(*args)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
            },
            Opcode::BuiltinCallDynamic { dst, name, args } => Instruction::BuiltinCallDynamic {
                dst: *dst,
                name: *name,
                args: self.decode_list_items(*args),
            },
            Opcode::Dispatch {
                dst,
                spec,
                selector,
                roles,
            } => {
                let spec = self.dispatch_spec(*spec);
                Instruction::Dispatch {
                    dst: *dst,
                    relations: spec.relations,
                    program_relation: spec.program_relation,
                    program_bytes: spec.program_bytes,
                    selector: self.decode_operand(*selector),
                    roles: self
                        .roles(*roles)
                        .iter()
                        .map(|(role, operand)| {
                            (self.constant(*role).clone(), self.decode_operand(*operand))
                        })
                        .collect(),
                }
            }
            Opcode::DynamicDispatch {
                dst,
                spec,
                selector,
                roles,
            } => {
                let spec = self.dispatch_spec(*spec);
                Instruction::DynamicDispatch {
                    dst: *dst,
                    relations: spec.relations,
                    program_relation: spec.program_relation,
                    program_bytes: spec.program_bytes,
                    selector: self.decode_operand(*selector),
                    roles: self.decode_operand(*roles),
                }
            }
            Opcode::PositionalDispatch {
                dst,
                spec,
                selector,
                args,
            } => {
                let spec = self.dispatch_spec(*spec);
                Instruction::PositionalDispatch {
                    dst: *dst,
                    relations: spec.relations,
                    program_relation: spec.program_relation,
                    program_bytes: spec.program_bytes,
                    selector: self.decode_operand(*selector),
                    args: self
                        .operands(*args)
                        .iter()
                        .map(|operand| self.decode_operand(*operand))
                        .collect(),
                }
            }
            Opcode::PositionalDispatchDynamic {
                dst,
                spec,
                selector,
                args,
            } => {
                let spec = self.dispatch_spec(*spec);
                Instruction::PositionalDispatchDynamic {
                    dst: *dst,
                    relations: spec.relations,
                    program_relation: spec.program_relation,
                    program_bytes: spec.program_bytes,
                    selector: self.decode_operand(*selector),
                    args: self.decode_list_items(*args),
                }
            }
            Opcode::SpawnDispatch {
                dst,
                selector,
                roles,
                delay,
            } => Instruction::SpawnDispatch {
                dst: *dst,
                selector: self.decode_operand(*selector),
                roles: self
                    .roles(*roles)
                    .iter()
                    .map(|(role, operand)| {
                        (self.constant(*role).clone(), self.decode_operand(*operand))
                    })
                    .collect(),
                delay: delay.map(|operand| self.decode_operand(operand)),
            },
            Opcode::SpawnPositionalDispatch {
                dst,
                selector,
                args,
                delay,
            } => Instruction::SpawnPositionalDispatch {
                dst: *dst,
                selector: self.decode_operand(*selector),
                args: self
                    .operands(*args)
                    .iter()
                    .map(|operand| self.decode_operand(*operand))
                    .collect(),
                delay: delay.map(|operand| self.decode_operand(operand)),
            },
            Opcode::Commit => Instruction::Commit,
            Opcode::Suspend { kind } => Instruction::Suspend {
                kind: self.suspend_kind(*kind).clone(),
            },
            Opcode::SuspendValue { dst, duration } => Instruction::SuspendValue {
                dst: *dst,
                duration: duration.map(|operand| self.decode_operand(operand)),
            },
            Opcode::CommitValue { dst } => Instruction::CommitValue { dst: *dst },
            Opcode::Read { dst, metadata } => Instruction::Read {
                dst: *dst,
                metadata: metadata.map(|operand| self.decode_operand(operand)),
            },
            Opcode::MailboxRecv {
                dst,
                receivers,
                timeout,
            } => Instruction::MailboxRecv {
                dst: *dst,
                receivers: self.decode_operand(*receivers),
                timeout: timeout.map(|operand| self.decode_operand(operand)),
            },
            Opcode::RollbackRetry => Instruction::RollbackRetry,
            Opcode::Return { value } => Instruction::Return {
                value: self.decode_operand(*value),
            },
            Opcode::Abort { error } => Instruction::Abort {
                error: self.decode_operand(*error),
            },
            Opcode::Raise {
                error,
                message,
                value,
            } => Instruction::Raise {
                error: self.decode_operand(*error),
                message: message.map(|operand| self.decode_operand(operand)),
                value: value.map(|operand| self.decode_operand(operand)),
            },
        }
    }
}

#[inline]
fn table_range<T>(table: &[T], range: TableRange) -> &[T] {
    let start = range.start as usize;
    let end = start + range.len as usize;
    &table[start..end]
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProgramBuilder {
    opcodes: Vec<Opcode>,
    constants: Vec<Value>,
    constant_ids: BTreeMap<Value, ConstId>,
    list_items: Vec<CompactListItem>,
    relation_args: Vec<CompactRelationArg>,
    map_entries: Vec<(OperandRef, OperandRef)>,
    operands: Vec<OperandRef>,
    bindings: Vec<Option<OperandRef>>,
    query_bindings: Vec<QueryBinding>,
    catches: Vec<CompactCatchHandler>,
    roles: Vec<(ConstId, OperandRef)>,
    programs: Vec<Arc<Program>>,
    relations: Vec<RelationId>,
    relation_ids: BTreeMap<RelationId, RelationRef>,
    dispatch_specs: Vec<DispatchSpec>,
    suspend_kinds: Vec<SuspendKind>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.opcodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.opcodes.is_empty()
    }

    pub fn emit(&mut self, instruction: Instruction) -> Result<usize, RuntimeError> {
        let index = self.opcodes.len();
        if index > u16::MAX as usize {
            return Err(artifact_error("program has too many instructions"));
        }
        let opcode = self.encode_instruction(instruction)?;
        self.opcodes.push(opcode);
        Ok(index)
    }

    pub fn emit_branch(
        &mut self,
        condition: Register,
        if_true: usize,
        if_false: usize,
    ) -> Result<usize, RuntimeError> {
        self.emit(Instruction::Branch {
            condition,
            if_true,
            if_false,
        })
    }

    pub fn emit_jump(&mut self, target: usize) -> Result<usize, RuntimeError> {
        self.emit(Instruction::Jump { target })
    }

    pub fn patch_branch(
        &mut self,
        index: usize,
        if_true: usize,
        if_false: usize,
    ) -> Result<(), RuntimeError> {
        let true_target = self.target(if_true)?;
        let false_target = self.target(if_false)?;
        let Some(Opcode::Branch {
            if_true, if_false, ..
        }) = self.opcodes.get_mut(index)
        else {
            return Err(artifact_error("expected branch opcode"));
        };
        *if_true = true_target;
        *if_false = false_target;
        Ok(())
    }

    pub fn patch_true_target(&mut self, index: usize, target: usize) -> Result<(), RuntimeError> {
        let target = self.target(target)?;
        let Some(Opcode::Branch { if_true, .. }) = self.opcodes.get_mut(index) else {
            return Err(artifact_error("expected branch opcode"));
        };
        *if_true = target;
        Ok(())
    }

    pub fn patch_false_target(&mut self, index: usize, target: usize) -> Result<(), RuntimeError> {
        let target = self.target(target)?;
        let Some(Opcode::Branch { if_false, .. }) = self.opcodes.get_mut(index) else {
            return Err(artifact_error("expected branch opcode"));
        };
        *if_false = target;
        Ok(())
    }

    pub fn patch_jump(&mut self, index: usize, target: usize) -> Result<(), RuntimeError> {
        let target = self.target(target)?;
        let Some(Opcode::Jump { target: slot }) = self.opcodes.get_mut(index) else {
            return Err(artifact_error("expected jump opcode"));
        };
        *slot = target;
        Ok(())
    }

    pub fn patch_enter_try(
        &mut self,
        index: usize,
        new_catches: Vec<CatchHandler>,
        new_finally: Option<usize>,
        new_end: usize,
    ) -> Result<(), RuntimeError> {
        let catches = self.catches(new_catches)?;
        let finally = new_finally.map(|target| self.target(target)).transpose()?;
        let end = self.target(new_end)?;
        let Some(Opcode::EnterTry {
            catches: catch_slot,
            finally: finally_slot,
            end: end_slot,
        }) = self.opcodes.get_mut(index)
        else {
            return Err(artifact_error("expected enter-try opcode"));
        };
        *catch_slot = catches;
        *finally_slot = finally;
        *end_slot = end;
        Ok(())
    }

    pub fn finish(self, register_count: usize) -> Result<Program, RuntimeError> {
        let program = Program {
            register_count,
            opcodes: self.opcodes.into(),
            constants: self.constants.into(),
            list_items: self.list_items.into(),
            relation_args: self.relation_args.into(),
            map_entries: self.map_entries.into(),
            operands: self.operands.into(),
            bindings: self.bindings.into(),
            query_bindings: self.query_bindings.into(),
            catches: self.catches.into(),
            roles: self.roles.into(),
            programs: self.programs.into(),
            relations: self.relations.into(),
            dispatch_specs: self.dispatch_specs.into(),
            suspend_kinds: self.suspend_kinds.into(),
        };
        let instructions = program.instructions();
        for instruction in &instructions {
            validate_instruction(register_count, instructions.len(), instruction)?;
        }
        Ok(program)
    }

    fn encode_instruction(&mut self, instruction: Instruction) -> Result<Opcode, RuntimeError> {
        Ok(match instruction {
            Instruction::Load { dst, value } => Opcode::Load {
                dst,
                value: self.constant(value)?,
            },
            Instruction::Move { dst, src } => Opcode::Move { dst, src },
            Instruction::Unary { dst, op, src } => Opcode::Unary { dst, op, src },
            Instruction::Binary {
                dst,
                op,
                left,
                right,
            } => Opcode::Binary {
                dst,
                op,
                left,
                right,
            },
            Instruction::BuildList { dst, items } => Opcode::BuildList {
                dst,
                items: self.list_items(items)?,
            },
            Instruction::BuildMap { dst, entries } => Opcode::BuildMap {
                dst,
                entries: self.map_entries(entries)?,
            },
            Instruction::BuildRange { dst, start, end } => Opcode::BuildRange {
                dst,
                start: self.operand(start)?,
                end: end.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::Index {
                dst,
                collection,
                index,
            } => Opcode::Index {
                dst,
                collection,
                index: self.operand(index)?,
            },
            Instruction::SetIndex {
                dst,
                collection,
                index,
                value,
            } => Opcode::SetIndex {
                dst,
                collection,
                index: self.operand(index)?,
                value: self.operand(value)?,
            },
            Instruction::ErrorField { dst, error, field } => {
                Opcode::ErrorField { dst, error, field }
            }
            Instruction::One { dst, src } => Opcode::One { dst, src },
            Instruction::CollectionLen { dst, collection } => {
                Opcode::CollectionLen { dst, collection }
            }
            Instruction::CollectionKeyAt {
                dst,
                collection,
                index,
            } => Opcode::CollectionKeyAt {
                dst,
                collection,
                index,
            },
            Instruction::CollectionValueAt {
                dst,
                collection,
                index,
            } => Opcode::CollectionValueAt {
                dst,
                collection,
                index,
            },
            Instruction::ScanExists {
                dst,
                relation,
                bindings,
            } => Opcode::ScanExists {
                dst,
                relation: self.relation(relation)?,
                bindings: self.bindings(bindings)?,
            },
            Instruction::ScanBindings {
                dst,
                relation,
                bindings,
                outputs,
            } => Opcode::ScanBindings {
                dst,
                relation: self.relation(relation)?,
                bindings: self.bindings(bindings)?,
                outputs: self.query_bindings(outputs)?,
            },
            Instruction::ScanValue { dst, relation, key } => Opcode::ScanValue {
                dst,
                relation: self.relation(relation)?,
                key: self.operand(key)?,
            },
            Instruction::Assert { relation, values } => Opcode::Assert {
                relation: self.relation(relation)?,
                values: self.operands(values)?,
            },
            Instruction::Retract { relation, values } => Opcode::Retract {
                relation: self.relation(relation)?,
                values: self.operands(values)?,
            },
            Instruction::RetractWhere { relation, bindings } => Opcode::RetractWhere {
                relation: self.relation(relation)?,
                bindings: self.bindings(bindings)?,
            },
            Instruction::ScanDynamic {
                dst,
                relation,
                args,
            } => Opcode::ScanDynamic {
                dst,
                relation: self.relation(relation)?,
                args: self.relation_args(args)?,
            },
            Instruction::AssertDynamic { relation, args } => Opcode::AssertDynamic {
                relation: self.relation(relation)?,
                args: self.relation_args(args)?,
            },
            Instruction::RetractDynamic { relation, args } => Opcode::RetractDynamic {
                relation: self.relation(relation)?,
                args: self.relation_args(args)?,
            },
            Instruction::ReplaceFunctional { relation, values } => Opcode::ReplaceFunctional {
                relation: self.relation(relation)?,
                values: self.operands(values)?,
            },
            Instruction::Branch {
                condition,
                if_true,
                if_false,
            } => Opcode::Branch {
                condition,
                if_true: self.target(if_true)?,
                if_false: self.target(if_false)?,
            },
            Instruction::Jump { target } => Opcode::Jump {
                target: self.target(target)?,
            },
            Instruction::EnterTry {
                catches,
                finally,
                end,
            } => Opcode::EnterTry {
                catches: self.catches(catches)?,
                finally: finally.map(|target| self.target(target)).transpose()?,
                end: self.target(end)?,
            },
            Instruction::ExitTry => Opcode::ExitTry,
            Instruction::EndFinally => Opcode::EndFinally,
            Instruction::Emit { target, value } => Opcode::Emit {
                target: self.operand(target)?,
                value: self.operand(value)?,
            },
            Instruction::LoadFunction {
                dst,
                program,
                captures,
                min_arity,
                max_arity,
            } => Opcode::LoadFunction {
                dst,
                program: self.program(program)?,
                captures: self.operands(captures)?,
                min_arity,
                max_arity,
            },
            Instruction::CallValue { dst, callee, args } => Opcode::CallValue {
                dst,
                callee: self.operand(callee)?,
                args: self.operands(args)?,
            },
            Instruction::CallValueDynamic { dst, callee, args } => Opcode::CallValueDynamic {
                dst,
                callee: self.operand(callee)?,
                args: self.list_items(args)?,
            },
            Instruction::Call { dst, program, args } => Opcode::Call {
                dst,
                program: self.program(program)?,
                args: self.operands(args)?,
            },
            Instruction::BuiltinCall { dst, name, args } => Opcode::BuiltinCall {
                dst,
                name,
                args: self.operands(args)?,
            },
            Instruction::BuiltinCallDynamic { dst, name, args } => Opcode::BuiltinCallDynamic {
                dst,
                name,
                args: self.list_items(args)?,
            },
            Instruction::Dispatch {
                dst,
                relations,
                program_relation,
                program_bytes,
                selector,
                roles,
            } => Opcode::Dispatch {
                dst,
                spec: self.dispatch_spec(DispatchSpec {
                    relations,
                    program_relation,
                    program_bytes,
                })?,
                selector: self.operand(selector)?,
                roles: self.roles(roles)?,
            },
            Instruction::DynamicDispatch {
                dst,
                relations,
                program_relation,
                program_bytes,
                selector,
                roles,
            } => Opcode::DynamicDispatch {
                dst,
                spec: self.dispatch_spec(DispatchSpec {
                    relations,
                    program_relation,
                    program_bytes,
                })?,
                selector: self.operand(selector)?,
                roles: self.operand(roles)?,
            },
            Instruction::PositionalDispatch {
                dst,
                relations,
                program_relation,
                program_bytes,
                selector,
                args,
            } => Opcode::PositionalDispatch {
                dst,
                spec: self.dispatch_spec(DispatchSpec {
                    relations,
                    program_relation,
                    program_bytes,
                })?,
                selector: self.operand(selector)?,
                args: self.operands(args)?,
            },
            Instruction::PositionalDispatchDynamic {
                dst,
                relations,
                program_relation,
                program_bytes,
                selector,
                args,
            } => Opcode::PositionalDispatchDynamic {
                dst,
                spec: self.dispatch_spec(DispatchSpec {
                    relations,
                    program_relation,
                    program_bytes,
                })?,
                selector: self.operand(selector)?,
                args: self.list_items(args)?,
            },
            Instruction::SpawnDispatch {
                dst,
                selector,
                roles,
                delay,
            } => Opcode::SpawnDispatch {
                dst,
                selector: self.operand(selector)?,
                roles: self.roles(roles)?,
                delay: delay.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::SpawnPositionalDispatch {
                dst,
                selector,
                args,
                delay,
            } => Opcode::SpawnPositionalDispatch {
                dst,
                selector: self.operand(selector)?,
                args: self.operands(args)?,
                delay: delay.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::Commit => Opcode::Commit,
            Instruction::Suspend { kind } => Opcode::Suspend {
                kind: self.suspend_kind(kind)?,
            },
            Instruction::SuspendValue { dst, duration } => Opcode::SuspendValue {
                dst,
                duration: duration.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::CommitValue { dst } => Opcode::CommitValue { dst },
            Instruction::Read { dst, metadata } => Opcode::Read {
                dst,
                metadata: metadata.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::MailboxRecv {
                dst,
                receivers,
                timeout,
            } => Opcode::MailboxRecv {
                dst,
                receivers: self.operand(receivers)?,
                timeout: timeout.map(|operand| self.operand(operand)).transpose()?,
            },
            Instruction::RollbackRetry => Opcode::RollbackRetry,
            Instruction::Return { value } => Opcode::Return {
                value: self.operand(value)?,
            },
            Instruction::Abort { error } => Opcode::Abort {
                error: self.operand(error)?,
            },
            Instruction::Raise {
                error,
                message,
                value,
            } => Opcode::Raise {
                error: self.operand(error)?,
                message: message.map(|operand| self.operand(operand)).transpose()?,
                value: value.map(|operand| self.operand(operand)).transpose()?,
            },
        })
    }

    fn constant(&mut self, value: Value) -> Result<ConstId, RuntimeError> {
        if let Some(id) = self.constant_ids.get(&value).copied() {
            return Ok(id);
        }
        let id = ConstId(narrow_index(self.constants.len(), "constant table")?);
        self.constants.push(value.clone());
        self.constant_ids.insert(value, id);
        Ok(id)
    }

    fn operand(&mut self, operand: Operand) -> Result<OperandRef, RuntimeError> {
        match operand {
            Operand::Register(register) => Ok(OperandRef::Register(register)),
            Operand::Value(value) => Ok(OperandRef::Constant(self.constant(value)?)),
        }
    }

    fn target(&self, target: usize) -> Result<Target, RuntimeError> {
        Ok(Target(narrow_index(target, "instruction target")?))
    }

    fn relation(&mut self, relation: RelationId) -> Result<RelationRef, RuntimeError> {
        if let Some(id) = self.relation_ids.get(&relation).copied() {
            return Ok(id);
        }
        let id = RelationRef(narrow_index(self.relations.len(), "relation table")?);
        self.relations.push(relation);
        self.relation_ids.insert(relation, id);
        Ok(id)
    }

    fn program(&mut self, program: Arc<Program>) -> Result<ProgramId, RuntimeError> {
        let id = ProgramId(narrow_index(self.programs.len(), "program table")?);
        self.programs.push(program);
        Ok(id)
    }

    fn dispatch_spec(&mut self, spec: DispatchSpec) -> Result<DispatchSpecId, RuntimeError> {
        let id = DispatchSpecId(narrow_index(
            self.dispatch_specs.len(),
            "dispatch spec table",
        )?);
        self.dispatch_specs.push(spec);
        Ok(id)
    }

    fn suspend_kind(&mut self, kind: SuspendKind) -> Result<SuspendKindId, RuntimeError> {
        let id = SuspendKindId(narrow_index(
            self.suspend_kinds.len(),
            "suspend kind table",
        )?);
        self.suspend_kinds.push(kind);
        Ok(id)
    }

    fn operands(&mut self, operands: Vec<Operand>) -> Result<TableRange, RuntimeError> {
        let start = self.operands.len();
        for operand in operands {
            let operand = self.operand(operand)?;
            self.operands.push(operand);
        }
        table_range_for(start, self.operands.len(), "operand table")
    }

    fn list_items(&mut self, items: Vec<ListItem>) -> Result<TableRange, RuntimeError> {
        let start = self.list_items.len();
        for item in items {
            let item = match item {
                ListItem::Value(operand) => CompactListItem::Value(self.operand(operand)?),
                ListItem::Splice(operand) => CompactListItem::Splice(self.operand(operand)?),
            };
            self.list_items.push(item);
        }
        table_range_for(start, self.list_items.len(), "list item table")
    }

    fn relation_args(&mut self, args: Vec<RelationArg>) -> Result<TableRange, RuntimeError> {
        let start = self.relation_args.len();
        for arg in args {
            let arg = match arg {
                RelationArg::Value(operand) => CompactRelationArg::Value(self.operand(operand)?),
                RelationArg::Splice(operand) => CompactRelationArg::Splice(self.operand(operand)?),
                RelationArg::Query(name) => CompactRelationArg::Query(name),
                RelationArg::Hole => CompactRelationArg::Hole,
            };
            self.relation_args.push(arg);
        }
        table_range_for(start, self.relation_args.len(), "relation arg table")
    }

    fn map_entries(
        &mut self,
        entries: Vec<(Operand, Operand)>,
    ) -> Result<TableRange, RuntimeError> {
        let start = self.map_entries.len();
        for (key, value) in entries {
            let key = self.operand(key)?;
            let value = self.operand(value)?;
            self.map_entries.push((key, value));
        }
        table_range_for(start, self.map_entries.len(), "map entry table")
    }

    fn bindings(&mut self, bindings: Vec<Option<Operand>>) -> Result<TableRange, RuntimeError> {
        let start = self.bindings.len();
        for binding in bindings {
            let binding = binding.map(|operand| self.operand(operand)).transpose()?;
            self.bindings.push(binding);
        }
        table_range_for(start, self.bindings.len(), "binding table")
    }

    fn query_bindings(&mut self, outputs: Vec<QueryBinding>) -> Result<TableRange, RuntimeError> {
        let start = self.query_bindings.len();
        self.query_bindings.extend(outputs);
        table_range_for(start, self.query_bindings.len(), "query binding table")
    }

    fn catches(&mut self, catches: Vec<CatchHandler>) -> Result<TableRange, RuntimeError> {
        let start = self.catches.len();
        for catch in catches {
            let code = catch.code.map(|code| self.constant(code)).transpose()?;
            let target = self.target(catch.target)?;
            self.catches.push(CompactCatchHandler {
                code,
                binding: catch.binding,
                target,
            });
        }
        table_range_for(start, self.catches.len(), "catch table")
    }

    fn roles(&mut self, roles: Vec<(Value, Operand)>) -> Result<TableRange, RuntimeError> {
        let start = self.roles.len();
        for (role, operand) in roles {
            let role = self.constant(role)?;
            let operand = self.operand(operand)?;
            self.roles.push((role, operand));
        }
        table_range_for(start, self.roles.len(), "role table")
    }
}

fn narrow_index(index: usize, table: &'static str) -> Result<u16, RuntimeError> {
    u16::try_from(index).map_err(|_| artifact_error(format!("{table} exceeds u16 capacity")))
}

fn table_range_for(
    start: usize,
    end: usize,
    table: &'static str,
) -> Result<TableRange, RuntimeError> {
    let len = end - start;
    Ok(TableRange {
        start: narrow_index(start, table)?,
        len: narrow_index(len, table)?,
    })
}

#[derive(Debug)]
pub struct ProgramResolver {
    cache: ArcSwap<BTreeMap<Value, Arc<Program>>>,
}

impl Default for ProgramResolver {
    fn default() -> Self {
        Self {
            cache: ArcSwap::new(Arc::new(BTreeMap::new())),
        }
    }
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
        let mut next = self.cache.load_full().as_ref().clone();
        let previous = next.insert(method, Arc::new(program));
        self.cache.store(Arc::new(next));
        previous
    }

    pub fn get(&self, method: &Value) -> Option<Arc<Program>> {
        self.cache.load().get(method).cloned()
    }

    pub fn contains(&self, method: &Value) -> bool {
        self.cache.load().contains_key(method)
    }

    pub fn len(&self) -> usize {
        self.cache.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.load().is_empty()
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
        self.cache.rcu(|current| {
            if current.contains_key(program_id) {
                return Arc::clone(current);
            }
            let mut next = current.as_ref().clone();
            next.insert(program_id.clone(), Arc::clone(&program));
            Arc::new(next)
        });
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
        Instruction::One { dst, src } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *src)
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
        Instruction::ScanExists { dst, bindings, .. }
        | Instruction::ScanBindings { dst, bindings, .. } => {
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
        Instruction::ScanDynamic { args, .. }
        | Instruction::AssertDynamic { args, .. }
        | Instruction::RetractDynamic { args, .. } => validate_relation_args(register_count, args),
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
        Instruction::Emit { target, value } => {
            validate_operand(register_count, target)?;
            validate_operand(register_count, value)
        }
        Instruction::Return { value } | Instruction::Abort { error: value } => {
            validate_operand(register_count, value)
        }
        Instruction::Raise {
            error,
            message,
            value,
        } => {
            validate_operand(register_count, error)?;
            validate_operands(register_count, message.iter())?;
            validate_operands(register_count, value.iter())
        }
        Instruction::LoadFunction { dst, captures, .. } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, captures.iter())
        }
        Instruction::CallValue { dst, callee, args } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, callee)?;
            validate_operands(register_count, args.iter())
        }
        Instruction::CallValueDynamic { dst, callee, args } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, callee)?;
            validate_operands(register_count, args.iter().map(ListItem::operand))
        }
        Instruction::Call { dst, program, args } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, args.iter())?;
            if args.len() > program.register_count() {
                return Err(RuntimeError::InvalidCallArity {
                    expected_min: 0,
                    expected_max: program.register_count(),
                    actual: args.len(),
                });
            }
            Ok(())
        }
        Instruction::BuiltinCall { dst, args, .. } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, args.iter())
        }
        Instruction::BuiltinCallDynamic { dst, args, .. } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, args.iter().map(ListItem::operand))
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
        Instruction::DynamicDispatch {
            dst,
            selector,
            roles,
            ..
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operand(register_count, roles)
        }
        Instruction::PositionalDispatch {
            dst,
            selector,
            args,
            ..
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operands(register_count, args.iter())
        }
        Instruction::PositionalDispatchDynamic {
            dst,
            selector,
            args,
            ..
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operands(register_count, args.iter().map(ListItem::operand))
        }
        Instruction::SpawnDispatch {
            dst,
            selector,
            roles,
            delay,
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operands(register_count, roles.iter().map(|(_, operand)| operand))?;
            validate_operands(register_count, delay.iter())
        }
        Instruction::SpawnPositionalDispatch {
            dst,
            selector,
            args,
            delay,
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, selector)?;
            validate_operands(register_count, args.iter())?;
            validate_operands(register_count, delay.iter())
        }
        Instruction::Commit | Instruction::Suspend { .. } | Instruction::RollbackRetry => Ok(()),
        Instruction::SuspendValue { dst, duration }
        | Instruction::Read {
            dst,
            metadata: duration,
        } => {
            validate_register(register_count, *dst)?;
            validate_operands(register_count, duration.iter())
        }
        Instruction::MailboxRecv {
            dst,
            receivers,
            timeout,
        } => {
            validate_register(register_count, *dst)?;
            validate_operand(register_count, receivers)?;
            validate_operands(register_count, timeout.iter())
        }
        Instruction::CommitValue { dst } => validate_register(register_count, *dst),
    }
}

fn validate_bindings(
    register_count: usize,
    bindings: &[Option<Operand>],
) -> Result<(), RuntimeError> {
    validate_operands(register_count, bindings.iter().filter_map(Option::as_ref))
}

fn validate_relation_args(register_count: usize, args: &[RelationArg]) -> Result<(), RuntimeError> {
    validate_operands(
        register_count,
        args.iter().filter_map(|arg| match arg {
            RelationArg::Value(operand) | RelationArg::Splice(operand) => Some(operand),
            RelationArg::Query(_) | RelationArg::Hole => None,
        }),
    )
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
const INST_BUILTIN_CALL: u8 = 33;
const INST_SCAN_BINDINGS: u8 = 34;
const INST_ONE: u8 = 35;
const INST_SUSPEND_VALUE: u8 = 36;
const INST_READ: u8 = 37;
const INST_COMMIT_VALUE: u8 = 38;
const INST_POSITIONAL_DISPATCH: u8 = 39;
const INST_DYNAMIC_DISPATCH: u8 = 40;
const INST_SPAWN_DISPATCH: u8 = 41;
const INST_MAILBOX_RECV: u8 = 42;
const INST_SCAN_DYNAMIC: u8 = 43;
const INST_ASSERT_DYNAMIC: u8 = 44;
const INST_RETRACT_DYNAMIC: u8 = 45;
const INST_BUILTIN_CALL_DYNAMIC: u8 = 46;
const INST_SPAWN_POSITIONAL_DISPATCH: u8 = 47;
const INST_LOAD_FUNCTION: u8 = 48;
const INST_CALL_VALUE: u8 = 49;
const INST_CALL_VALUE_DYNAMIC: u8 = 50;
const INST_POSITIONAL_DISPATCH_DYNAMIC: u8 = 51;

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
        Instruction::One { dst, src } => {
            out.push(INST_ONE);
            write_register(out, *dst);
            write_register(out, *src);
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
        Instruction::ScanBindings {
            dst,
            relation,
            bindings,
            outputs,
        } => {
            out.push(INST_SCAN_BINDINGS);
            write_register(out, *dst);
            write_identity(out, *relation);
            write_optional_operands(out, bindings)?;
            write_query_bindings(out, outputs)
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
        Instruction::ScanDynamic {
            dst,
            relation,
            args,
        } => {
            out.push(INST_SCAN_DYNAMIC);
            write_register(out, *dst);
            write_identity(out, *relation);
            write_relation_args(out, args)
        }
        Instruction::AssertDynamic { relation, args } => {
            out.push(INST_ASSERT_DYNAMIC);
            write_identity(out, *relation);
            write_relation_args(out, args)
        }
        Instruction::RetractDynamic { relation, args } => {
            out.push(INST_RETRACT_DYNAMIC);
            write_identity(out, *relation);
            write_relation_args(out, args)
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
        Instruction::Emit { target, value } => {
            out.push(INST_EMIT);
            write_operand(out, target)?;
            write_operand(out, value)
        }
        Instruction::LoadFunction {
            dst,
            program,
            captures,
            min_arity,
            max_arity,
        } => {
            out.push(INST_LOAD_FUNCTION);
            write_register(out, *dst);
            write_u16(out, *min_arity);
            write_u16(out, *max_arity);
            write_bytes(out, &program.to_bytes()?);
            write_operands(out, captures)?;
            Ok(())
        }
        Instruction::CallValue { dst, callee, args } => {
            out.push(INST_CALL_VALUE);
            write_register(out, *dst);
            write_operand(out, callee)?;
            write_operands(out, args)
        }
        Instruction::CallValueDynamic { dst, callee, args } => {
            out.push(INST_CALL_VALUE_DYNAMIC);
            write_register(out, *dst);
            write_operand(out, callee)?;
            write_list_items(out, args)
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
            SuspendKind::Never
            | SuspendKind::TimedMillis(_)
            | SuspendKind::WaitingForInput(_)
            | SuspendKind::MailboxRecv(_)
            | SuspendKind::Spawn(_) => Err(artifact_error(
                "only commit suspension is serializable in program artifacts",
            )),
        },
        Instruction::SuspendValue { dst, duration } => {
            out.push(INST_SUSPEND_VALUE);
            write_register(out, *dst);
            write_optional_operand(out, duration.as_ref())
        }
        Instruction::CommitValue { dst } => {
            out.push(INST_COMMIT_VALUE);
            write_register(out, *dst);
            Ok(())
        }
        Instruction::Read { dst, metadata } => {
            out.push(INST_READ);
            write_register(out, *dst);
            write_optional_operand(out, metadata.as_ref())
        }
        Instruction::MailboxRecv {
            dst,
            receivers,
            timeout,
        } => {
            out.push(INST_MAILBOX_RECV);
            write_register(out, *dst);
            write_operand(out, receivers)?;
            write_optional_operand(out, timeout.as_ref())
        }
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
        Instruction::DynamicDispatch {
            dst,
            relations,
            program_relation,
            program_bytes,
            selector,
            roles,
        } => {
            out.push(INST_DYNAMIC_DISPATCH);
            write_register(out, *dst);
            write_identity(out, relations.method_selector);
            write_identity(out, relations.param);
            write_identity(out, relations.delegates);
            write_identity(out, *program_relation);
            write_identity(out, *program_bytes);
            write_operand(out, selector)?;
            write_operand(out, roles)
        }
        Instruction::PositionalDispatch {
            dst,
            relations,
            program_relation,
            program_bytes,
            selector,
            args,
        } => {
            out.push(INST_POSITIONAL_DISPATCH);
            write_register(out, *dst);
            write_identity(out, relations.method_selector);
            write_identity(out, relations.param);
            write_identity(out, relations.delegates);
            write_identity(out, *program_relation);
            write_identity(out, *program_bytes);
            write_operand(out, selector)?;
            write_operands(out, args)
        }
        Instruction::PositionalDispatchDynamic {
            dst,
            relations,
            program_relation,
            program_bytes,
            selector,
            args,
        } => {
            out.push(INST_POSITIONAL_DISPATCH_DYNAMIC);
            write_register(out, *dst);
            write_identity(out, relations.method_selector);
            write_identity(out, relations.param);
            write_identity(out, relations.delegates);
            write_identity(out, *program_relation);
            write_identity(out, *program_bytes);
            write_operand(out, selector)?;
            write_list_items(out, args)
        }
        Instruction::SpawnDispatch {
            dst,
            selector,
            roles,
            delay,
        } => {
            out.push(INST_SPAWN_DISPATCH);
            write_register(out, *dst);
            write_operand(out, selector)?;
            write_u32(out, roles.len() as u32);
            for (role, operand) in roles {
                write_value(out, role)?;
                write_operand(out, operand)?;
            }
            write_optional_operand(out, delay.as_ref())
        }
        Instruction::SpawnPositionalDispatch {
            dst,
            selector,
            args,
            delay,
        } => {
            out.push(INST_SPAWN_POSITIONAL_DISPATCH);
            write_register(out, *dst);
            write_operand(out, selector)?;
            write_operands(out, args)?;
            write_optional_operand(out, delay.as_ref())
        }
        Instruction::Call { dst, program, args } => {
            out.push(INST_CALL);
            write_register(out, *dst);
            write_bytes(out, &program.to_bytes()?);
            write_operands(out, args)
        }
        Instruction::BuiltinCall { dst, name, args } => {
            let Some(name) = name.name() else {
                return Err(artifact_error("cannot serialize unnamed builtin symbol"));
            };
            out.push(INST_BUILTIN_CALL);
            write_register(out, *dst);
            write_str(out, name);
            write_operands(out, args)
        }
        Instruction::BuiltinCallDynamic { dst, name, args } => {
            let Some(name) = name.name() else {
                return Err(artifact_error("cannot serialize unnamed builtin symbol"));
            };
            out.push(INST_BUILTIN_CALL_DYNAMIC);
            write_register(out, *dst);
            write_str(out, name);
            write_list_items(out, args)
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

fn write_relation_args(out: &mut Vec<u8>, args: &[RelationArg]) -> Result<(), RuntimeError> {
    write_u32(out, args.len() as u32);
    for arg in args {
        match arg {
            RelationArg::Value(operand) => {
                out.push(0);
                write_operand(out, operand)?;
            }
            RelationArg::Splice(operand) => {
                out.push(1);
                write_operand(out, operand)?;
            }
            RelationArg::Query(name) => {
                out.push(2);
                let Some(name) = name.name() else {
                    return Err(artifact_error("cannot serialize unnamed query symbol"));
                };
                write_str(out, name);
            }
            RelationArg::Hole => out.push(3),
        }
    }
    Ok(())
}

fn write_query_bindings(out: &mut Vec<u8>, bindings: &[QueryBinding]) -> Result<(), RuntimeError> {
    write_u32(out, bindings.len() as u32);
    for binding in bindings {
        let Some(name) = binding.name.name() else {
            return Err(artifact_error(
                "cannot serialize unnamed query binding symbol",
            ));
        };
        write_str(out, name);
        write_u16(out, binding.position);
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
    } else if value.as_capability().is_some() {
        return Err(artifact_error("capability values are not serializable"));
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
            INST_ONE => Instruction::One {
                dst: self.read_register()?,
                src: self.read_register()?,
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
            INST_SCAN_BINDINGS => Instruction::ScanBindings {
                dst: self.read_register()?,
                relation: self.read_identity()?,
                bindings: self.read_optional_operands()?,
                outputs: self.read_query_bindings()?,
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
            INST_LOAD_FUNCTION => Instruction::LoadFunction {
                dst: self.read_register()?,
                min_arity: self.read_u16()?,
                max_arity: self.read_u16()?,
                program: Arc::new(Program::from_bytes(&self.read_bytes()?)?),
                captures: self.read_operands()?,
            },
            INST_CALL_VALUE => Instruction::CallValue {
                dst: self.read_register()?,
                callee: self.read_operand()?,
                args: self.read_operands()?,
            },
            INST_CALL_VALUE_DYNAMIC => Instruction::CallValueDynamic {
                dst: self.read_register()?,
                callee: self.read_operand()?,
                args: self.read_list_items()?,
            },
            INST_BUILTIN_CALL => Instruction::BuiltinCall {
                dst: self.read_register()?,
                name: Symbol::intern(&self.read_string()?),
                args: self.read_operands()?,
            },
            INST_BUILTIN_CALL_DYNAMIC => Instruction::BuiltinCallDynamic {
                dst: self.read_register()?,
                name: Symbol::intern(&self.read_string()?),
                args: self.read_list_items()?,
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
            INST_SCAN_DYNAMIC => Instruction::ScanDynamic {
                dst: self.read_register()?,
                relation: self.read_identity()?,
                args: self.read_relation_args()?,
            },
            INST_ASSERT_DYNAMIC => Instruction::AssertDynamic {
                relation: self.read_identity()?,
                args: self.read_relation_args()?,
            },
            INST_RETRACT_DYNAMIC => Instruction::RetractDynamic {
                relation: self.read_identity()?,
                args: self.read_relation_args()?,
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
                target: self.read_operand()?,
                value: self.read_operand()?,
            },
            INST_COMMIT => Instruction::Commit,
            INST_SUSPEND_COMMIT => Instruction::Suspend {
                kind: SuspendKind::Commit,
            },
            INST_SUSPEND_VALUE => Instruction::SuspendValue {
                dst: self.read_register()?,
                duration: self.read_optional_operand()?,
            },
            INST_COMMIT_VALUE => Instruction::CommitValue {
                dst: self.read_register()?,
            },
            INST_READ => Instruction::Read {
                dst: self.read_register()?,
                metadata: self.read_optional_operand()?,
            },
            INST_MAILBOX_RECV => Instruction::MailboxRecv {
                dst: self.read_register()?,
                receivers: self.read_operand()?,
                timeout: self.read_optional_operand()?,
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
            INST_DYNAMIC_DISPATCH => Instruction::DynamicDispatch {
                dst: self.read_register()?,
                relations: DispatchRelations {
                    method_selector: self.read_identity()?,
                    param: self.read_identity()?,
                    delegates: self.read_identity()?,
                },
                program_relation: self.read_identity()?,
                program_bytes: self.read_identity()?,
                selector: self.read_operand()?,
                roles: self.read_operand()?,
            },
            INST_POSITIONAL_DISPATCH => Instruction::PositionalDispatch {
                dst: self.read_register()?,
                relations: DispatchRelations {
                    method_selector: self.read_identity()?,
                    param: self.read_identity()?,
                    delegates: self.read_identity()?,
                },
                program_relation: self.read_identity()?,
                program_bytes: self.read_identity()?,
                selector: self.read_operand()?,
                args: self.read_operands()?,
            },
            INST_POSITIONAL_DISPATCH_DYNAMIC => Instruction::PositionalDispatchDynamic {
                dst: self.read_register()?,
                relations: DispatchRelations {
                    method_selector: self.read_identity()?,
                    param: self.read_identity()?,
                    delegates: self.read_identity()?,
                },
                program_relation: self.read_identity()?,
                program_bytes: self.read_identity()?,
                selector: self.read_operand()?,
                args: self.read_list_items()?,
            },
            INST_SPAWN_DISPATCH => Instruction::SpawnDispatch {
                dst: self.read_register()?,
                selector: self.read_operand()?,
                roles: self.read_dispatch_roles()?,
                delay: self.read_optional_operand()?,
            },
            INST_SPAWN_POSITIONAL_DISPATCH => Instruction::SpawnPositionalDispatch {
                dst: self.read_register()?,
                selector: self.read_operand()?,
                args: self.read_operands()?,
                delay: self.read_optional_operand()?,
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

    fn read_relation_args(&mut self) -> Result<Vec<RelationArg>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| {
                Ok(match self.read_u8()? {
                    0 => RelationArg::Value(self.read_operand()?),
                    1 => RelationArg::Splice(self.read_operand()?),
                    2 => RelationArg::Query(Symbol::intern(&self.read_string()?)),
                    3 => RelationArg::Hole,
                    _ => return Err(artifact_error("invalid relation argument tag")),
                })
            })
            .collect()
    }

    fn read_optional_operand(&mut self) -> Result<Option<Operand>, RuntimeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_operand().map(Some),
            _ => Err(artifact_error("invalid optional operand tag")),
        }
    }

    fn read_query_bindings(&mut self) -> Result<Vec<QueryBinding>, RuntimeError> {
        let count = self.read_u32()? as usize;
        (0..count)
            .map(|_| {
                Ok(QueryBinding {
                    name: Symbol::intern(&self.read_string()?),
                    position: self.read_u16()?,
                })
            })
            .collect()
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
