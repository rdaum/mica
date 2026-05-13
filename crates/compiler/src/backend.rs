use crate::{
    BinaryOp, BindingId, Diagnostic, EffectKind, HirArg, HirCatch, HirCollectionItem, HirExpr,
    HirFunctionBody, HirItem, HirPlace, HirProgram, HirRecovery, HirRelationAtom,
    HirScatterBinding, Literal, LocalKind, NodeId, ParamMode, SemanticProgram, Span, UnaryOp,
    parse_semantic,
};
use mica_relation_kernel::{DispatchRelations, RelationId, Transaction, Tuple};
use mica_runtime::{
    CatchHandler, ErrorField, Instruction, ListItem, Operand, Program, Register, RuntimeBinaryOp,
    RuntimeUnaryOp, Scheduler, SchedulerError, TaskId, TaskOutcome,
};
use mica_var::{Identity, Symbol, Value, ValueError};
use std::collections::HashMap;
use std::sync::Arc;

pub fn compile_source(
    source: &str,
    context: &CompileContext,
) -> Result<CompiledProgram, CompileError> {
    let semantic = parse_semantic(source);
    compile_semantic(semantic, context)
}

pub fn compile_semantic(
    semantic: SemanticProgram,
    context: &CompileContext,
) -> Result<CompiledProgram, CompileError> {
    if !semantic.parse_errors.is_empty() {
        return Err(CompileError::ParseErrors {
            count: semantic.parse_errors.len(),
        });
    }
    if let Some(diagnostic) = semantic.diagnostics.first() {
        return Err(CompileError::SemanticDiagnostic {
            diagnostic: diagnostic.clone(),
        });
    }

    let compiler = ProgramCompiler::new(&semantic, context);
    let program = compiler.compile_program(&semantic.hir)?;
    Ok(CompiledProgram { semantic, program })
}

pub fn submit_source_task(
    source: &str,
    context: &CompileContext,
    scheduler: &mut Scheduler,
) -> Result<SubmittedSourceTask, SourceTaskError> {
    let compiled = compile_source(source, context)?;
    let (task_id, outcome) = scheduler.submit(Arc::new(compiled.program.clone()))?;
    Ok(SubmittedSourceTask {
        compiled,
        task_id,
        outcome,
    })
}

pub fn install_methods_from_source(
    source: &str,
    context: &CompileContext,
    tx: &mut Transaction<'_>,
) -> Result<MethodInstallation, CompileError> {
    let semantic = parse_semantic(source);
    install_methods(semantic, context, tx)
}

pub fn install_methods(
    semantic: SemanticProgram,
    context: &CompileContext,
    tx: &mut Transaction<'_>,
) -> Result<MethodInstallation, CompileError> {
    if !semantic.parse_errors.is_empty() {
        return Err(CompileError::ParseErrors {
            count: semantic.parse_errors.len(),
        });
    }
    if let Some(diagnostic) = semantic.diagnostics.first() {
        return Err(CompileError::SemanticDiagnostic {
            diagnostic: diagnostic.clone(),
        });
    }

    let method_relations = context
        .method_relations
        .ok_or_else(|| CompileError::Unsupported {
            node: NodeId(0),
            span: None,
            message: "method relation ids are not configured".to_owned(),
        })?;
    let mut methods = Vec::new();

    for item in &semantic.hir.items {
        if let HirItem::Method { .. } = item {
            let method = compile_installed_method(&semantic, context, item)?;
            tx.assert(
                method_relations.dispatch.method_selector,
                Tuple::from([method.method.clone(), method.selector.clone()]),
            )?;
            tx.assert(
                method_relations.method_program,
                Tuple::from([method.method.clone(), method.program.clone()]),
            )?;
            tx.assert(
                method_relations.program_bytes,
                Tuple::from([
                    method.program.clone(),
                    Value::bytes(method.compiled.program.to_bytes()?),
                ]),
            )?;
            for role in &method.roles {
                tx.assert(
                    method_relations.dispatch.param,
                    Tuple::from([
                        method.method.clone(),
                        role.role.clone(),
                        role.restriction.clone(),
                    ]),
                )?;
            }
            methods.push(method);
        }
    }

    Ok(MethodInstallation { semantic, methods })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledProgram {
    pub semantic: SemanticProgram,
    pub program: Program,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmittedSourceTask {
    pub compiled: CompiledProgram,
    pub task_id: TaskId,
    pub outcome: TaskOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MethodInstallation {
    pub semantic: SemanticProgram,
    pub methods: Vec<InstalledMethod>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledMethod {
    pub method: Value,
    pub program: Value,
    pub selector: Value,
    pub roles: Vec<InstalledRole>,
    pub compiled: CompiledProgram,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledRole {
    pub name: String,
    pub role: Value,
    pub restriction: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MethodRelations {
    pub dispatch: DispatchRelations,
    pub method_program: RelationId,
    pub program_bytes: RelationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DotRelation {
    pub relation: RelationId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompileContext {
    relations: HashMap<String, RelationId>,
    dot_relations: HashMap<String, DotRelation>,
    identities: HashMap<String, Identity>,
    program_identities: HashMap<String, Identity>,
    method_relations: Option<MethodRelations>,
}

impl CompileContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_relation(mut self, name: impl Into<String>, id: RelationId) -> Self {
        self.define_relation(name, id);
        self
    }

    pub fn with_dot_relation(mut self, name: impl Into<String>, relation: RelationId) -> Self {
        self.define_dot_relation(name, relation);
        self
    }

    pub fn with_identity(mut self, name: impl Into<String>, id: Identity) -> Self {
        self.define_identity(name, id);
        self
    }

    pub fn with_program_identity(mut self, method: impl Into<String>, id: Identity) -> Self {
        self.define_program_identity(method, id);
        self
    }

    pub fn with_method_relations(mut self, method_relations: MethodRelations) -> Self {
        self.method_relations = Some(method_relations);
        self
    }

    pub fn define_relation(&mut self, name: impl Into<String>, id: RelationId) {
        self.relations.insert(name.into(), id);
    }

    pub fn define_dot_relation(&mut self, name: impl Into<String>, relation: RelationId) {
        self.dot_relations
            .insert(name.into(), DotRelation { relation });
    }

    pub fn define_identity(&mut self, name: impl Into<String>, id: Identity) {
        self.identities.insert(name.into(), id);
    }

    pub fn define_program_identity(&mut self, method: impl Into<String>, id: Identity) {
        self.program_identities.insert(method.into(), id);
    }

    pub fn relation(&self, name: &str) -> Option<RelationId> {
        self.relations.get(name).copied()
    }

    pub fn dot_relation(&self, name: &str) -> Option<DotRelation> {
        self.dot_relations.get(name).copied()
    }

    pub fn identity(&self, name: &str) -> Option<Identity> {
        self.identities.get(name).copied()
    }

    pub fn program_identity(&self, method: &str) -> Option<Identity> {
        self.program_identities.get(method).copied()
    }

    pub fn method_relations(&self) -> Option<MethodRelations> {
        self.method_relations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompileError {
    ParseErrors {
        count: usize,
    },
    SemanticDiagnostic {
        diagnostic: Diagnostic,
    },
    Unsupported {
        node: NodeId,
        span: Option<Span>,
        message: String,
    },
    UnknownRelation {
        node: NodeId,
        span: Option<Span>,
        name: String,
    },
    UnknownIdentity {
        node: NodeId,
        span: Option<Span>,
        name: String,
    },
    InvalidLiteral {
        node: NodeId,
        span: Option<Span>,
        message: String,
    },
    UnboundLocal {
        node: NodeId,
        span: Option<Span>,
        binding: BindingId,
    },
    Runtime(mica_runtime::RuntimeError),
    Kernel(mica_relation_kernel::KernelError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceTaskError {
    Compile(CompileError),
    Scheduler(SchedulerError),
}

impl From<CompileError> for SourceTaskError {
    fn from(value: CompileError) -> Self {
        Self::Compile(value)
    }
}

impl From<SchedulerError> for SourceTaskError {
    fn from(value: SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}

impl From<mica_runtime::RuntimeError> for CompileError {
    fn from(value: mica_runtime::RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<mica_relation_kernel::KernelError> for CompileError {
    fn from(value: mica_relation_kernel::KernelError) -> Self {
        Self::Kernel(value)
    }
}

fn compile_installed_method(
    semantic: &SemanticProgram,
    context: &CompileContext,
    item: &HirItem,
) -> Result<InstalledMethod, CompileError> {
    let HirItem::Method {
        id,
        identity,
        selector,
        clauses,
        roles,
        body,
        ..
    } = item
    else {
        return Err(CompileError::Unsupported {
            node: item_id(item),
            span: semantic.span(item_id(item)).cloned(),
            message: "only method items can be installed as methods".to_owned(),
        });
    };
    let identity_name = identity.as_ref().ok_or_else(|| CompileError::Unsupported {
        node: *id,
        span: semantic.span(*id).cloned(),
        message: "method installation requires an explicit method identity".to_owned(),
    })?;
    let selector = selector.as_ref().ok_or_else(|| CompileError::Unsupported {
        node: *id,
        span: semantic.span(*id).cloned(),
        message: "method installation requires an explicit selector".to_owned(),
    })?;
    let method = context
        .identity(identity_name)
        .ok_or_else(|| CompileError::UnknownIdentity {
            node: *id,
            span: semantic.span(*id).cloned(),
            name: identity_name.clone(),
        })
        .map(Value::identity)?;
    let program_id = context
        .program_identity(identity_name)
        .ok_or_else(|| CompileError::UnknownIdentity {
            node: *id,
            span: semantic.span(*id).cloned(),
            name: format!("{identity_name} program"),
        })
        .map(Value::identity)?;
    let roles = lower_installed_roles(*id, semantic, context, roles, clauses)?;

    let mut compiler = ProgramCompiler::new(semantic, context);
    compiler.next_register = roles.len() as u16;
    for (idx, role) in roles.iter().enumerate() {
        compiler
            .external_locals
            .insert(role.name.clone(), Register(idx as u16));
    }
    let compiled_program = compiler.compile_items(body)?;
    Ok(InstalledMethod {
        method: method.clone(),
        program: program_id,
        selector: Value::symbol(Symbol::intern(selector)),
        roles,
        compiled: CompiledProgram {
            semantic: semantic.clone(),
            program: compiled_program,
        },
    })
}

fn lower_installed_roles(
    id: NodeId,
    semantic: &SemanticProgram,
    context: &CompileContext,
    roles: &[crate::MethodRole],
    clauses: &[String],
) -> Result<Vec<InstalledRole>, CompileError> {
    if !roles.is_empty() {
        return roles
            .iter()
            .map(|role| {
                let restriction = context.identity(&role.restriction).ok_or_else(|| {
                    CompileError::UnknownIdentity {
                        node: id,
                        span: semantic.span(id).cloned(),
                        name: role.restriction.clone(),
                    }
                })?;
                Ok(InstalledRole {
                    name: role.name.clone(),
                    role: Value::symbol(Symbol::intern(&role.name)),
                    restriction: Value::identity(restriction),
                })
            })
            .collect();
    }

    let mut installed = Vec::new();
    for clause in clauses {
        let clause = clause.trim();
        let clause = clause.strip_prefix("roles").unwrap_or(clause).trim();
        if !clause.contains(':') {
            continue;
        }
        for part in clause
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            let Some((name, restriction)) = part.split_once(':') else {
                continue;
            };
            let name = name.trim();
            let restriction = restriction.trim();
            if name.is_empty() || !restriction.starts_with('$') {
                continue;
            }
            let restriction_name = restriction.trim_start_matches('$').trim();
            let restriction = context.identity(restriction_name).ok_or_else(|| {
                CompileError::UnknownIdentity {
                    node: id,
                    span: semantic.span(id).cloned(),
                    name: restriction_name.to_owned(),
                }
            })?;
            installed.push(InstalledRole {
                name: name.to_owned(),
                role: Value::symbol(Symbol::intern(name)),
                restriction: Value::identity(restriction),
            });
        }
    }
    Ok(installed)
}

fn item_id(item: &HirItem) -> NodeId {
    match item {
        HirItem::Expr { id, .. }
        | HirItem::RelationRule { id, .. }
        | HirItem::Object { id, .. }
        | HirItem::Method { id, .. } => *id,
    }
}

fn runtime_binary_op(op: BinaryOp) -> Option<RuntimeBinaryOp> {
    Some(match op {
        BinaryOp::Eq => RuntimeBinaryOp::Eq,
        BinaryOp::Ne => RuntimeBinaryOp::Ne,
        BinaryOp::Lt => RuntimeBinaryOp::Lt,
        BinaryOp::Le => RuntimeBinaryOp::Le,
        BinaryOp::Gt => RuntimeBinaryOp::Gt,
        BinaryOp::Ge => RuntimeBinaryOp::Ge,
        BinaryOp::Add => RuntimeBinaryOp::Add,
        BinaryOp::Sub => RuntimeBinaryOp::Sub,
        BinaryOp::Mul => RuntimeBinaryOp::Mul,
        BinaryOp::Div => RuntimeBinaryOp::Div,
        BinaryOp::Rem => RuntimeBinaryOp::Rem,
        BinaryOp::And | BinaryOp::Or | BinaryOp::Range => return None,
    })
}

fn builtin_error_field(name: &str) -> Option<ErrorField> {
    Some(match name {
        "code" => ErrorField::Code,
        "message" => ErrorField::Message,
        "value" => ErrorField::Value,
        _ => return None,
    })
}

struct ProgramCompiler<'a> {
    semantic: &'a SemanticProgram,
    context: &'a CompileContext,
    instructions: Vec<Instruction>,
    next_register: u16,
    locals: HashMap<BindingId, Register>,
    external_locals: HashMap<String, Register>,
    functions: HashMap<BindingId, FunctionInfo>,
    loops: Vec<LoopContext>,
    returned: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FunctionInfo {
    program: Arc<Program>,
    params: Vec<FunctionParamInfo>,
    min_arity: usize,
    max_arity: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FunctionParamInfo {
    id: NodeId,
    kind: LocalKind,
    default: Option<HirExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LoopContext {
    continue_target: usize,
    break_jumps: Vec<usize>,
    continue_jumps: Vec<usize>,
}

impl<'a> ProgramCompiler<'a> {
    fn new(semantic: &'a SemanticProgram, context: &'a CompileContext) -> Self {
        Self {
            semantic,
            context,
            instructions: Vec::new(),
            next_register: 0,
            locals: HashMap::new(),
            external_locals: HashMap::new(),
            functions: HashMap::new(),
            loops: Vec::new(),
            returned: false,
        }
    }

    fn compile_program(self, program: &HirProgram) -> Result<Program, CompileError> {
        self.compile_items(&program.items)
    }

    fn compile_items(mut self, items: &[HirItem]) -> Result<Program, CompileError> {
        let mut last_value = None;
        for item in items {
            last_value = self.compile_item(item)?;
        }
        if !self.returned {
            let value = last_value
                .map(Operand::Register)
                .unwrap_or_else(|| Operand::Value(Value::nothing()));
            self.emit(Instruction::Return { value });
        }
        Program::new(self.next_register as usize, self.instructions).map_err(Into::into)
    }

    fn compile_item(&mut self, item: &HirItem) -> Result<Option<Register>, CompileError> {
        if self.returned {
            return Ok(None);
        }

        match item {
            HirItem::Expr { expr, .. } => self.compile_expr_for_value(expr).map(Some),
            HirItem::RelationRule { id, .. } => Err(self.unsupported(
                *id,
                "relation rules are compile-time database definitions, not executable task code yet",
            )),
            HirItem::Object { id, .. } => Err(self.unsupported(
                *id,
                "object fileout declarations are not executable task code yet",
            )),
            HirItem::Method { id, .. } => Err(self.unsupported(
                *id,
                "method fileout declarations are not executable task code yet",
            )),
        }
    }

    fn compile_expr_for_value(&mut self, expr: &HirExpr) -> Result<Register, CompileError> {
        match expr {
            HirExpr::Literal { id, value } => {
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: self.literal_value(*id, value)?,
                });
                Ok(dst)
            }
            HirExpr::Identity { id, name } => {
                let identity =
                    self.context
                        .identity(name)
                        .ok_or_else(|| CompileError::UnknownIdentity {
                            node: *id,
                            span: self.span(*id),
                            name: name.clone(),
                        })?;
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::identity(identity),
                });
                Ok(dst)
            }
            HirExpr::Symbol { name, .. } => {
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::symbol(Symbol::intern(name)),
                });
                Ok(dst)
            }
            HirExpr::LocalRef { id, binding } => {
                self.locals
                    .get(binding)
                    .copied()
                    .ok_or_else(|| CompileError::UnboundLocal {
                        node: *id,
                        span: self.span(*id),
                        binding: *binding,
                    })
            }
            HirExpr::Binding {
                binding,
                scatter,
                value,
                id,
                ..
            } => {
                if !scatter.is_empty() {
                    return self.compile_scatter_binding(*id, scatter, value.as_deref());
                }
                if let (Some(binding), Some(value)) = (*binding, value.as_deref())
                    && let HirExpr::Function { name: None, .. } = value
                {
                    let function = self.compile_function(value, Some(binding))?;
                    self.functions.insert(binding, function);
                    let dst = self.alloc_register();
                    self.emit(Instruction::Load {
                        dst,
                        value: Value::nothing(),
                    });
                    return Ok(dst);
                }
                let dst = match value {
                    Some(value) => self.compile_expr_for_value(value)?,
                    None => {
                        let dst = self.alloc_register();
                        self.emit(Instruction::Load {
                            dst,
                            value: Value::nothing(),
                        });
                        dst
                    }
                };
                if let Some(binding) = binding {
                    self.locals.insert(*binding, dst);
                } else {
                    return Err(self.unsupported(
                        *id,
                        "scatter assignment lowering is not implemented in the task compiler yet",
                    ));
                }
                Ok(dst)
            }
            HirExpr::Assign { id, target, value } => {
                let value = self.compile_expr_for_value(value)?;
                match target {
                    HirPlace::Local { binding, .. } => {
                        let dst = self.locals.get(binding).copied().ok_or_else(|| {
                            CompileError::UnboundLocal {
                                node: *id,
                                span: self.span(*id),
                                binding: *binding,
                            }
                        })?;
                        self.emit(Instruction::Move { dst, src: value });
                        Ok(dst)
                    }
                    HirPlace::Index {
                        collection, index, ..
                    } => self.compile_index_assignment(*id, collection, index.as_deref(), value),
                    HirPlace::Dot { base, name, .. } => {
                        self.compile_dot_assignment(*id, base, name, value)
                    }
                    _ => Err(self.unsupported(
                        *id,
                        "only local, indexed local, and declared dot assignment are implemented in the task compiler yet",
                    )),
                }
            }
            HirExpr::List { id, items } => self.compile_list(*id, items),
            HirExpr::Map { entries, .. } => self.compile_map(entries),
            HirExpr::Index {
                id,
                collection,
                index,
            } => self.compile_index(*id, collection, index.as_deref()),
            HirExpr::Field { id, base, name } => self.compile_dot_read(*id, base, name),
            HirExpr::Unary { id, op, expr } => self.compile_unary(*id, *op, expr),
            HirExpr::Binary {
                id,
                op,
                left,
                right,
            } => self.compile_binary(*id, *op, left, right),
            HirExpr::RelationAtom(atom) => self.compile_relation_exists(atom),
            HirExpr::FactChange { kind, atom, .. } => {
                self.compile_fact_change(kind, atom)?;
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::nothing(),
                });
                Ok(dst)
            }
            HirExpr::Require { condition, id } => {
                let condition = self.compile_expr_for_value(condition)?;
                let branch = self.instructions.len();
                self.emit(Instruction::Branch {
                    condition,
                    if_true: branch + 2,
                    if_false: branch + 1,
                });
                self.emit(Instruction::Abort {
                    error: Operand::Value(Value::string("require failed")),
                });
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::bool(true),
                });
                if self.instructions.len() <= branch + 2 {
                    return Err(self.unsupported(*id, "invalid require branch layout"));
                }
                Ok(dst)
            }
            HirExpr::Return { value, .. } => {
                let value = match value {
                    Some(value) => Operand::Register(self.compile_expr_for_value(value)?),
                    None => Operand::Value(Value::nothing()),
                };
                self.emit(Instruction::Return { value });
                self.returned = true;
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::nothing(),
                });
                Ok(dst)
            }
            HirExpr::Raise {
                error,
                message,
                value,
                ..
            } => self.compile_raise(error, message.as_deref(), value.as_deref()),
            HirExpr::Recover { id, expr, catches } => self.compile_recover(*id, expr, catches),
            HirExpr::Block { items, .. } => {
                let saved = self.locals.clone();
                let mut last_value = None;
                for item in items {
                    last_value = self.compile_item(item)?;
                }
                self.locals = saved;
                Ok(last_value.unwrap_or_else(|| {
                    let dst = self.alloc_register();
                    self.emit(Instruction::Load {
                        dst,
                        value: Value::nothing(),
                    });
                    dst
                }))
            }
            HirExpr::If {
                id,
                condition,
                then_items,
                elseif,
                else_items,
            } => self.compile_if(*id, condition, then_items, elseif, else_items),
            HirExpr::Call { id, callee, args } => self.compile_call(*id, callee, args),
            HirExpr::While {
                id,
                condition,
                body,
            } => self.compile_while(*id, condition, body),
            HirExpr::For {
                id,
                key,
                value,
                iter,
                body,
                ..
            } => self.compile_for(*id, *key, *value, iter, body),
            HirExpr::Break { id } => self.compile_break(*id),
            HirExpr::Continue { id } => self.compile_continue(*id),
            HirExpr::Try {
                id,
                body,
                catches,
                finally,
            } => self.compile_try(*id, body, catches, finally),
            HirExpr::Function { id, .. } => {
                let function = self.compile_function(expr, None)?;
                let HirExpr::Function { name, .. } = expr else {
                    unreachable!();
                };
                let Some(binding) = name else {
                    return Err(self.unsupported(
                        *id,
                        "anonymous function values are not implemented in the task compiler yet",
                    ));
                };
                self.functions.insert(*binding, function);
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::nothing(),
                });
                Ok(dst)
            }
            HirExpr::RoleDispatch { id, selector, args } => {
                self.compile_dispatch(*id, selector, args, None)
            }
            HirExpr::ReceiverDispatch {
                id,
                receiver,
                selector,
                args,
            } => self.compile_dispatch(*id, selector, args, Some(receiver)),
            HirExpr::ExternalRef { id, name } => {
                if let Some(register) = self.external_locals.get(name).copied() {
                    Ok(register)
                } else {
                    Err(CompileError::Unsupported {
                        node: *id,
                        span: self.span(*id),
                        message: format!(
                            "runtime function `{name}` is not callable from compiled tasks yet"
                        ),
                    })
                }
            }
            HirExpr::Error { id } => {
                Err(self.unsupported(*id, "cannot compile erroneous HIR node"))
            }
            _ => Err(self.unsupported(
                expr_id(expr),
                "HIR form is not implemented in the task compiler yet",
            )),
        }
    }

    fn compile_unary(
        &mut self,
        _id: NodeId,
        op: UnaryOp,
        expr: &HirExpr,
    ) -> Result<Register, CompileError> {
        let op = match op {
            UnaryOp::Not => RuntimeUnaryOp::Not,
            UnaryOp::Neg => RuntimeUnaryOp::Neg,
        };
        let src = self.compile_expr_for_value(expr)?;
        let dst = self.alloc_register();
        self.emit(Instruction::Unary { dst, op, src });
        Ok(dst)
    }

    fn compile_binary(
        &mut self,
        id: NodeId,
        op: BinaryOp,
        left: &HirExpr,
        right: &HirExpr,
    ) -> Result<Register, CompileError> {
        match op {
            BinaryOp::And => self.compile_and(left, right),
            BinaryOp::Or => self.compile_or(left, right),
            BinaryOp::Range => self.compile_range(left, right),
            _ => {
                let Some(op) = runtime_binary_op(op) else {
                    return Err(self.unsupported(
                        id,
                        "binary operator is not implemented in the task compiler yet",
                    ));
                };
                let left = self.compile_expr_for_value(left)?;
                let right = self.compile_expr_for_value(right)?;
                let dst = self.alloc_register();
                self.emit(Instruction::Binary {
                    dst,
                    op,
                    left,
                    right,
                });
                Ok(dst)
            }
        }
    }

    fn compile_range(&mut self, left: &HirExpr, right: &HirExpr) -> Result<Register, CompileError> {
        let start = self.compile_expr_for_operand(left)?;
        let end = match right {
            HirExpr::Hole { .. } => None,
            _ => Some(self.compile_expr_for_operand(right)?),
        };
        let dst = self.alloc_register();
        self.emit(Instruction::BuildRange { dst, start, end });
        Ok(dst)
    }

    fn compile_list(
        &mut self,
        _id: NodeId,
        items: &[HirCollectionItem],
    ) -> Result<Register, CompileError> {
        let mut operands = Vec::with_capacity(items.len());
        for item in items {
            match item {
                HirCollectionItem::Expr(expr) => {
                    operands.push(ListItem::Value(self.compile_expr_for_operand(expr)?));
                }
                HirCollectionItem::Splice(expr) => {
                    operands.push(ListItem::Splice(self.compile_expr_for_operand(expr)?));
                }
            }
        }
        let dst = self.alloc_register();
        self.emit(Instruction::BuildList {
            dst,
            items: operands,
        });
        Ok(dst)
    }

    fn compile_empty_list(&mut self) -> Register {
        let dst = self.alloc_register();
        self.emit(Instruction::BuildList {
            dst,
            items: Vec::new(),
        });
        dst
    }

    fn compile_scatter_binding(
        &mut self,
        _id: NodeId,
        scatter: &[HirScatterBinding],
        value: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let source = match value {
            Some(value) => self.compile_expr_for_value(value)?,
            None => self.compile_empty_list(),
        };
        let len = self.alloc_register();
        self.emit(Instruction::CollectionLen {
            dst: len,
            collection: source,
        });

        let mut position = 0usize;
        let mut last = None;
        let mut saw_rest = false;
        for binding in scatter {
            if matches!(binding.mode, ParamMode::Rest) {
                if saw_rest {
                    return Err(self.unsupported(
                        binding.id,
                        "scatter assignment supports only one rest binding",
                    ));
                }
                saw_rest = true;
                let dst = self.compile_collection_rest(source, len, position, binding.id)?;
                self.locals.insert(binding.binding, dst);
                last = Some(dst);
            } else {
                let dst = if matches!(binding.mode, ParamMode::Optional) {
                    self.compile_collection_slot_with_optional_default(
                        source,
                        len,
                        position,
                        binding.id,
                        binding.default.as_ref(),
                    )?
                } else {
                    self.compile_collection_slot(source, position, binding.id)?
                };
                self.locals.insert(binding.binding, dst);
                last = Some(dst);
                position += 1;
            }
        }

        Ok(last.unwrap_or_else(|| {
            let dst = self.alloc_register();
            self.emit(Instruction::Load {
                dst,
                value: Value::nothing(),
            });
            dst
        }))
    }

    fn compile_collection_slot(
        &mut self,
        collection: Register,
        position: usize,
        id: NodeId,
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        self.emit(Instruction::Index {
            dst,
            collection,
            index: self.usize_operand(position, id)?,
        });
        Ok(dst)
    }

    fn compile_collection_slot_with_optional_default(
        &mut self,
        collection: Register,
        len: Register,
        position: usize,
        id: NodeId,
        default: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let dst = self.compile_collection_slot(collection, position, id)?;
        let Some(default) = default else {
            return Ok(dst);
        };

        let pos = self.load_usize(position, id)?;
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Lt,
            left: pos,
            right: len,
        });
        let branch = self.emit_branch(condition, 0, 0);
        let true_target = self.instructions.len();
        self.emit_jump(0);
        let false_target = self.instructions.len();
        let default = self.compile_expr_for_value(default)?;
        self.emit(Instruction::Move { dst, src: default });
        let end = self.instructions.len();
        self.patch_branch(branch, true_target, false_target)?;
        self.patch_jump(true_target, end)?;
        Ok(dst)
    }

    fn compile_collection_rest(
        &mut self,
        collection: Register,
        len: Register,
        position: usize,
        id: NodeId,
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        let start = self.load_usize(position, id)?;
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Le,
            left: start,
            right: len,
        });
        let branch = self.emit_branch(condition, 0, 0);

        let slice_target = self.instructions.len();
        let range = self.alloc_register();
        self.emit(Instruction::BuildRange {
            dst: range,
            start: Operand::Register(start),
            end: None,
        });
        self.emit(Instruction::Index {
            dst,
            collection,
            index: Operand::Register(range),
        });
        let jump = self.emit_jump(0);

        let empty_target = self.instructions.len();
        let empty = self.compile_empty_list();
        self.emit(Instruction::Move { dst, src: empty });

        let end = self.instructions.len();
        self.patch_branch(branch, slice_target, empty_target)?;
        self.patch_jump(jump, end)?;
        Ok(dst)
    }

    fn compile_map(&mut self, entries: &[(HirExpr, HirExpr)]) -> Result<Register, CompileError> {
        let entries = entries
            .iter()
            .map(|(key, value)| {
                Ok((
                    self.compile_expr_for_operand(key)?,
                    self.compile_expr_for_operand(value)?,
                ))
            })
            .collect::<Result<Vec<_>, CompileError>>()?;
        let dst = self.alloc_register();
        self.emit(Instruction::BuildMap { dst, entries });
        Ok(dst)
    }

    fn compile_index(
        &mut self,
        id: NodeId,
        collection: &HirExpr,
        index: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let Some(index) = index else {
            return Err(self.unsupported(id, "index expressions require an explicit index"));
        };
        let collection = self.compile_expr_for_value(collection)?;
        let index = self.compile_expr_for_operand(index)?;
        let dst = self.alloc_register();
        self.emit(Instruction::Index {
            dst,
            collection,
            index,
        });
        Ok(dst)
    }

    fn compile_index_assignment(
        &mut self,
        id: NodeId,
        collection: &HirExpr,
        index: Option<&HirExpr>,
        value: Register,
    ) -> Result<Register, CompileError> {
        let Some(index) = index else {
            return Err(self.unsupported(id, "indexed assignment requires an explicit index"));
        };
        let HirExpr::LocalRef { binding, .. } = collection else {
            return Err(self.unsupported(
                id,
                "indexed assignment currently requires a local collection target",
            ));
        };
        let binding_info = self
            .semantic
            .bindings
            .get(binding.0 as usize)
            .ok_or_else(|| CompileError::UnboundLocal {
                node: id,
                span: self.span(id),
                binding: *binding,
            })?;
        if !binding_info.mutable {
            return Err(self.unsupported(
                id,
                format!(
                    "cannot assign through immutable indexed binding `{}`",
                    binding_info.name
                ),
            ));
        }
        let collection =
            self.locals
                .get(binding)
                .copied()
                .ok_or_else(|| CompileError::UnboundLocal {
                    node: id,
                    span: self.span(id),
                    binding: *binding,
                })?;
        let index = self.compile_expr_for_operand(index)?;
        let updated = self.alloc_register();
        self.emit(Instruction::SetIndex {
            dst: updated,
            collection,
            index,
            value: Operand::Register(value),
        });
        self.emit(Instruction::Move {
            dst: collection,
            src: updated,
        });
        Ok(collection)
    }

    fn compile_dot_read(
        &mut self,
        id: NodeId,
        base: &HirExpr,
        name: &str,
    ) -> Result<Register, CompileError> {
        if let Some(field) = builtin_error_field(name)
            && self.context.dot_relation(name).is_none()
        {
            let error = self.compile_expr_for_value(base)?;
            let dst = self.alloc_register();
            self.emit(Instruction::ErrorField { dst, error, field });
            return Ok(dst);
        }
        let dot = self
            .context
            .dot_relation(name)
            .ok_or_else(|| self.unsupported(id, format!("dot name `{name}` is not declared")))?;
        let key = self.compile_expr_for_operand(base)?;
        let dst = self.alloc_register();
        self.emit(Instruction::ScanValue {
            dst,
            relation: dot.relation,
            key,
        });
        Ok(dst)
    }

    fn compile_dot_assignment(
        &mut self,
        id: NodeId,
        base: &HirExpr,
        name: &str,
        value: Register,
    ) -> Result<Register, CompileError> {
        let dot = self
            .context
            .dot_relation(name)
            .ok_or_else(|| self.unsupported(id, format!("dot name `{name}` is not declared")))?;
        let key = self.compile_expr_for_operand(base)?;
        self.emit(Instruction::ReplaceFunctional {
            relation: dot.relation,
            values: vec![key, Operand::Register(value)],
        });
        Ok(value)
    }

    fn compile_function(
        &self,
        expr: &HirExpr,
        binding_override: Option<BindingId>,
    ) -> Result<FunctionInfo, CompileError> {
        let HirExpr::Function {
            id,
            name,
            params,
            captures,
            body,
            ..
        } = expr
        else {
            return Err(self.unsupported(expr_id(expr), "expected function expression"));
        };
        if !captures.is_empty() {
            return Err(
                self.unsupported(*id, "closures are not implemented in the task compiler yet")
            );
        }
        if name.is_none() && binding_override.is_none() {
            return Err(self.unsupported(
                *id,
                "anonymous function values are not implemented in the task compiler yet",
            ));
        }
        let mut saw_optional = false;
        let mut saw_rest = false;
        for param in params {
            match param.kind {
                LocalKind::Param => {
                    if saw_optional || saw_rest {
                        return Err(self.unsupported(
                            param.id,
                            "required function parameters must precede optional and rest parameters",
                        ));
                    }
                }
                LocalKind::OptionalParam => {
                    if saw_rest {
                        return Err(self.unsupported(
                            param.id,
                            "optional function parameters must precede rest parameters",
                        ));
                    }
                    saw_optional = true;
                }
                LocalKind::RestParam => {
                    if saw_rest {
                        return Err(self.unsupported(
                            param.id,
                            "function signatures support only one rest parameter",
                        ));
                    }
                    saw_rest = true;
                }
                _ => {
                    return Err(self.unsupported(
                        param.id,
                        "unsupported function parameter kind in compiled function",
                    ));
                }
            }
        }

        let mut compiler = ProgramCompiler::new(self.semantic, self.context);
        compiler.next_register = params.len() as u16;
        for (idx, param) in params.iter().enumerate() {
            compiler.locals.insert(param.binding, Register(idx as u16));
        }
        let program = match body {
            HirFunctionBody::Expr(expr) => compiler.compile_function_expr_body(expr)?,
            HirFunctionBody::Block(items) => compiler.compile_items(items)?,
        };
        let param_info = params
            .iter()
            .map(|param| FunctionParamInfo {
                id: param.id,
                kind: param.kind.clone(),
                default: param.default.clone(),
            })
            .collect::<Vec<_>>();
        let min_arity = param_info
            .iter()
            .filter(|param| param.kind == LocalKind::Param)
            .count();
        let max_arity = if param_info
            .iter()
            .any(|param| param.kind == LocalKind::RestParam)
        {
            None
        } else {
            Some(param_info.len())
        };
        Ok(FunctionInfo {
            program: Arc::new(program),
            params: param_info,
            min_arity,
            max_arity,
        })
    }

    fn compile_function_expr_body(mut self, expr: &HirExpr) -> Result<Program, CompileError> {
        let value = self.compile_expr_for_value(expr)?;
        if !self.returned {
            self.emit(Instruction::Return {
                value: Operand::Register(value),
            });
        }
        Program::new(self.next_register as usize, self.instructions).map_err(Into::into)
    }

    fn compile_call(
        &mut self,
        id: NodeId,
        callee: &HirExpr,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        let HirExpr::LocalRef { binding, .. } = callee else {
            return Err(self.unsupported(
                id,
                "only direct calls to local functions are implemented in the task compiler yet",
            ));
        };
        let function = self.functions.get(binding).cloned().ok_or_else(|| {
            self.unsupported(
                id,
                "only direct calls to local functions are implemented in the task compiler yet",
            )
        })?;
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(
                id,
                "direct function calls only support positional arguments",
            ));
        }
        let has_splice = args.iter().any(|arg| arg.splice);
        if !has_splice {
            self.validate_static_function_arity(id, &function, args.len())?;
        }
        let call_args = if function
            .params
            .iter()
            .all(|param| param.kind == LocalKind::Param)
            && !has_splice
        {
            args.iter()
                .map(|arg| self.compile_arg_operand(arg))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            self.compile_bound_function_args(id, &function, args)?
        };
        let dst = self.alloc_register();
        self.emit(Instruction::Call {
            dst,
            program: function.program,
            args: call_args,
        });
        Ok(dst)
    }

    fn validate_static_function_arity(
        &self,
        id: NodeId,
        function: &FunctionInfo,
        actual: usize,
    ) -> Result<(), CompileError> {
        if actual < function.min_arity {
            return Err(self.unsupported(
                id,
                format!(
                    "function call expected at least {} arguments but got {}",
                    function.min_arity, actual
                ),
            ));
        }
        if let Some(max_arity) = function.max_arity
            && actual > max_arity
        {
            return Err(self.unsupported(
                id,
                format!(
                    "function call expected at most {} arguments but got {}",
                    max_arity, actual
                ),
            ));
        }
        Ok(())
    }

    fn compile_bound_function_args(
        &mut self,
        id: NodeId,
        function: &FunctionInfo,
        args: &[HirArg],
    ) -> Result<Vec<Operand>, CompileError> {
        let items = args
            .iter()
            .map(|arg| {
                let operand = self.compile_arg_operand(arg)?;
                Ok(if arg.splice {
                    ListItem::Splice(operand)
                } else {
                    ListItem::Value(operand)
                })
            })
            .collect::<Result<Vec<_>, CompileError>>()?;
        let actuals = self.alloc_register();
        self.emit(Instruction::BuildList {
            dst: actuals,
            items,
        });
        let len = self.alloc_register();
        self.emit(Instruction::CollectionLen {
            dst: len,
            collection: actuals,
        });

        let mut operands = Vec::with_capacity(function.params.len());
        let mut position = 0usize;
        for param in &function.params {
            match param.kind {
                LocalKind::Param => {
                    operands.push(Operand::Register(
                        self.compile_collection_slot(actuals, position, param.id)?,
                    ));
                    position += 1;
                }
                LocalKind::OptionalParam => {
                    operands.push(Operand::Register(
                        self.compile_collection_slot_with_optional_default(
                            actuals,
                            len,
                            position,
                            param.id,
                            param.default.as_ref(),
                        )?,
                    ));
                    position += 1;
                }
                LocalKind::RestParam => {
                    operands.push(Operand::Register(
                        self.compile_collection_rest(actuals, len, position, param.id)?,
                    ));
                }
                _ => {
                    return Err(
                        self.unsupported(id, "unsupported function parameter kind in call binding")
                    );
                }
            }
        }
        Ok(operands)
    }

    fn compile_and(&mut self, left: &HirExpr, right: &HirExpr) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::bool(false),
        });
        let left = self.compile_expr_for_value(left)?;
        let branch = self.emit_branch(left, 0, 0);
        let false_target = self.instructions.len();
        self.emit(Instruction::Jump { target: 0 });
        let true_target = self.instructions.len();
        let right = self.compile_expr_for_value(right)?;
        self.emit(Instruction::Move { dst, src: right });
        let end = self.instructions.len();
        self.patch_branch(branch, true_target, false_target)?;
        self.patch_jump(false_target, end)?;
        Ok(dst)
    }

    fn compile_or(&mut self, left: &HirExpr, right: &HirExpr) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::bool(true),
        });
        let left = self.compile_expr_for_value(left)?;
        let branch = self.emit_branch(left, 0, 0);
        let true_target = self.instructions.len();
        self.emit(Instruction::Jump { target: 0 });
        let false_target = self.instructions.len();
        let right = self.compile_expr_for_value(right)?;
        self.emit(Instruction::Move { dst, src: right });
        let end = self.instructions.len();
        self.patch_branch(branch, true_target, false_target)?;
        self.patch_jump(true_target, end)?;
        Ok(dst)
    }

    fn compile_if(
        &mut self,
        _id: NodeId,
        condition: &HirExpr,
        then_items: &[HirItem],
        elseif: &[(HirExpr, Vec<HirItem>)],
        else_items: &[HirItem],
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        let saved_returned = self.returned;
        self.returned = false;
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });
        let condition = self.compile_expr_for_value(condition)?;
        let first_branch = self.emit_branch(condition, 0, 0);
        let mut false_jumps = Vec::new();
        let mut end_jumps = Vec::new();
        let mut branch_returns = Vec::new();
        let then_target = self.instructions.len();
        let (value, returned) = self.compile_branch_items(then_items)?;
        branch_returns.push(returned);
        if let Some(value) = value {
            self.emit(Instruction::Move { dst, src: value });
        }
        if !returned {
            end_jumps.push(self.emit_jump(0));
        }
        false_jumps.push(first_branch);

        for (condition, items) in elseif {
            let else_if_test = self.instructions.len();
            let condition = self.compile_expr_for_value(condition)?;
            let branch = self.emit_branch(condition, 0, 0);
            let body_target = self.instructions.len();
            let (value, returned) = self.compile_branch_items(items)?;
            branch_returns.push(returned);
            if let Some(value) = value {
                self.emit(Instruction::Move { dst, src: value });
            }
            if !returned {
                end_jumps.push(self.emit_jump(0));
            }
            let previous = false_jumps.pop().unwrap();
            self.patch_false_target(previous, else_if_test)?;
            self.patch_true_target(branch, body_target)?;
            false_jumps.push(branch);
        }

        let else_target = self.instructions.len();
        let (value, else_returned) = self.compile_branch_items(else_items)?;
        if !else_items.is_empty() {
            branch_returns.push(else_returned);
        }
        if let Some(value) = value {
            self.emit(Instruction::Move { dst, src: value });
        }
        let end = self.instructions.len();
        if let Some(last_false) = false_jumps.pop() {
            self.patch_false_target(last_false, else_target)?;
        }
        self.patch_true_target(first_branch, then_target)?;
        for jump in end_jumps {
            self.patch_jump(jump, end)?;
        }
        self.returned = saved_returned
            || (!else_items.is_empty()
                && !branch_returns.is_empty()
                && branch_returns.iter().all(|returned| *returned));
        Ok(dst)
    }

    fn compile_while(
        &mut self,
        _id: NodeId,
        condition: &HirExpr,
        body: &[HirItem],
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });

        let loop_start = self.instructions.len();
        let condition = self.compile_expr_for_value(condition)?;
        let branch = self.emit_branch(condition, 0, 0);
        let body_target = self.instructions.len();

        self.loops.push(LoopContext {
            continue_target: loop_start,
            break_jumps: Vec::new(),
            continue_jumps: Vec::new(),
        });
        let body_returned = self.compile_loop_body(body)?;
        let loop_context = self.loops.pop().ok_or_else(|| CompileError::Unsupported {
            node: NodeId(0),
            span: None,
            message: "internal compiler error: missing loop context".to_owned(),
        })?;

        if !body_returned {
            self.emit_jump(loop_start);
        }
        let end = self.instructions.len();
        self.patch_branch(branch, body_target, end)?;
        for jump in loop_context.break_jumps {
            self.patch_jump(jump, end)?;
        }
        for jump in loop_context.continue_jumps {
            self.patch_jump(jump, loop_context.continue_target)?;
        }
        Ok(dst)
    }

    fn compile_for(
        &mut self,
        _id: NodeId,
        key: BindingId,
        value: Option<BindingId>,
        iter: &HirExpr,
        body: &[HirItem],
    ) -> Result<Register, CompileError> {
        let saved_locals = self.locals.clone();
        let result = self.alloc_register();
        self.emit(Instruction::Load {
            dst: result,
            value: Value::nothing(),
        });

        let collection = self.compile_expr_for_value(iter)?;
        let len = self.alloc_register();
        self.emit(Instruction::CollectionLen {
            dst: len,
            collection,
        });
        let index = self.alloc_register();
        self.emit(Instruction::Load {
            dst: index,
            value: Value::int(0).unwrap(),
        });
        let one = self.alloc_register();
        self.emit(Instruction::Load {
            dst: one,
            value: Value::int(1).unwrap(),
        });

        let key_register = self.alloc_register();
        self.emit(Instruction::Load {
            dst: key_register,
            value: Value::nothing(),
        });
        self.locals.insert(key, key_register);
        let value_register = if let Some(value) = value {
            let register = self.alloc_register();
            self.emit(Instruction::Load {
                dst: register,
                value: Value::nothing(),
            });
            self.locals.insert(value, register);
            Some(register)
        } else {
            None
        };

        let loop_start = self.instructions.len();
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Lt,
            left: index,
            right: len,
        });
        let branch = self.emit_branch(condition, 0, 0);
        let body_target = self.instructions.len();

        if let Some(value_register) = value_register {
            self.emit(Instruction::CollectionKeyAt {
                dst: key_register,
                collection,
                index,
            });
            self.emit(Instruction::CollectionValueAt {
                dst: value_register,
                collection,
                index,
            });
        } else {
            self.emit(Instruction::CollectionValueAt {
                dst: key_register,
                collection,
                index,
            });
        }

        self.loops.push(LoopContext {
            continue_target: 0,
            break_jumps: Vec::new(),
            continue_jumps: Vec::new(),
        });
        let body_returned = self.compile_loop_body(body)?;
        let loop_context = self.loops.pop().ok_or_else(|| CompileError::Unsupported {
            node: NodeId(0),
            span: None,
            message: "internal compiler error: missing loop context".to_owned(),
        })?;

        let increment_target = self.instructions.len();
        if !body_returned {
            let next_index = self.alloc_register();
            self.emit(Instruction::Binary {
                dst: next_index,
                op: RuntimeBinaryOp::Add,
                left: index,
                right: one,
            });
            self.emit(Instruction::Move {
                dst: index,
                src: next_index,
            });
            self.emit_jump(loop_start);
        }
        let end = self.instructions.len();
        self.patch_branch(branch, body_target, end)?;
        for jump in loop_context.break_jumps {
            self.patch_jump(jump, end)?;
        }
        for jump in loop_context.continue_jumps {
            self.patch_jump(jump, increment_target)?;
        }

        self.locals = saved_locals;
        Ok(result)
    }

    fn compile_loop_body(&mut self, body: &[HirItem]) -> Result<bool, CompileError> {
        let saved_returned = self.returned;
        self.returned = false;
        for item in body {
            self.compile_item(item)?;
        }
        let body_returned = self.returned;
        self.returned = saved_returned;
        Ok(body_returned)
    }

    fn compile_raise(
        &mut self,
        error: &HirExpr,
        message: Option<&HirExpr>,
        value: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let error = self.compile_expr_for_operand(error)?;
        let message = message
            .map(|message| self.compile_expr_for_operand(message))
            .transpose()?;
        let value = value
            .map(|value| self.compile_expr_for_operand(value))
            .transpose()?;
        self.emit(Instruction::Raise {
            error,
            message,
            value,
        });
        self.returned = true;
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });
        Ok(dst)
    }

    fn compile_try(
        &mut self,
        id: NodeId,
        body: &[HirItem],
        catches: &[HirCatch],
        finally: &[HirItem],
    ) -> Result<Register, CompileError> {
        let saved_locals = self.locals.clone();
        let saved_returned = self.returned;
        self.returned = false;

        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });

        let mut handlers = catches
            .iter()
            .map(|catch| {
                let binding = catch.binding.map(|binding| {
                    let register = self.alloc_register();
                    self.emit(Instruction::Load {
                        dst: register,
                        value: Value::nothing(),
                    });
                    self.locals.insert(binding, register);
                    register
                });
                let code = self.catch_code(id, catch.condition.as_ref())?;
                Ok(CatchHandler {
                    code,
                    binding,
                    target: 0,
                })
            })
            .collect::<Result<Vec<_>, CompileError>>()?;

        let enter = self.instructions.len();
        self.emit(Instruction::EnterTry {
            catches: handlers.clone(),
            finally: None,
            end: 0,
        });

        let (body_value, body_returned) = self.compile_branch_items(body)?;
        if let Some(value) = body_value {
            self.emit(Instruction::Move { dst, src: value });
        }
        if !body_returned {
            self.emit(Instruction::ExitTry);
        }

        let mut end_jumps = Vec::new();
        for (idx, catch) in catches.iter().enumerate() {
            handlers[idx].target = self.instructions.len();
            let (value, returned) = self.compile_branch_items(&catch.body)?;
            if let Some(value) = value {
                self.emit(Instruction::Move { dst, src: value });
            }
            if !returned {
                if finally.is_empty() {
                    end_jumps.push(self.emit_jump(0));
                } else {
                    self.emit(Instruction::ExitTry);
                }
            }
        }

        let finally_target = if finally.is_empty() {
            None
        } else {
            let target = self.instructions.len();
            let _ = self.compile_branch_items(finally)?;
            self.emit(Instruction::EndFinally);
            Some(target)
        };

        let end = self.instructions.len();
        for jump in end_jumps {
            self.patch_jump(jump, end)?;
        }
        self.patch_enter_try(enter, handlers, finally_target, end)?;
        self.locals = saved_locals;
        self.returned = saved_returned;
        Ok(dst)
    }

    fn compile_recover(
        &mut self,
        id: NodeId,
        expr: &HirExpr,
        catches: &[HirRecovery],
    ) -> Result<Register, CompileError> {
        let saved_locals = self.locals.clone();
        let saved_returned = self.returned;
        self.returned = false;

        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });

        let mut handlers = catches
            .iter()
            .map(|catch| {
                let binding = catch.binding.map(|binding| {
                    let register = self.alloc_register();
                    self.emit(Instruction::Load {
                        dst: register,
                        value: Value::nothing(),
                    });
                    self.locals.insert(binding, register);
                    register
                });
                let code = self.catch_code(id, catch.condition.as_ref())?;
                Ok(CatchHandler {
                    code,
                    binding,
                    target: 0,
                })
            })
            .collect::<Result<Vec<_>, CompileError>>()?;

        let enter = self.instructions.len();
        self.emit(Instruction::EnterTry {
            catches: handlers.clone(),
            finally: None,
            end: 0,
        });

        let value = self.compile_expr_for_value(expr)?;
        if !self.returned {
            self.emit(Instruction::Move { dst, src: value });
            self.emit(Instruction::ExitTry);
        }

        let mut end_jumps = Vec::new();
        for (idx, catch) in catches.iter().enumerate() {
            handlers[idx].target = self.instructions.len();
            let saved_branch_returned = self.returned;
            self.returned = false;
            let value = self.compile_expr_for_value(&catch.value)?;
            if !self.returned {
                self.emit(Instruction::Move { dst, src: value });
                end_jumps.push(self.emit_jump(0));
            }
            self.returned = saved_branch_returned;
        }

        let end = self.instructions.len();
        for jump in end_jumps {
            self.patch_jump(jump, end)?;
        }
        self.patch_enter_try(enter, handlers, None, end)?;
        self.locals = saved_locals;
        self.returned = saved_returned;
        Ok(dst)
    }

    fn catch_code(
        &self,
        id: NodeId,
        condition: Option<&HirExpr>,
    ) -> Result<Option<Value>, CompileError> {
        let Some(condition) = condition else {
            return Ok(None);
        };
        match condition {
            HirExpr::Literal {
                value: Literal::ErrorCode(code),
                ..
            } => Ok(Some(Value::error_code(Symbol::intern(code)))),
            _ => Err(self.unsupported(
                id,
                "compiled catch clauses currently match an error code literal or catch all",
            )),
        }
    }

    fn compile_break(&mut self, id: NodeId) -> Result<Register, CompileError> {
        if self.loops.is_empty() {
            return Err(self.unsupported(id, "break is only valid inside a loop"));
        }
        let jump = self.emit_jump(0);
        self.loops
            .last_mut()
            .expect("loop stack was checked above")
            .break_jumps
            .push(jump);
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });
        Ok(dst)
    }

    fn compile_continue(&mut self, id: NodeId) -> Result<Register, CompileError> {
        if self.loops.is_empty() {
            return Err(self.unsupported(id, "continue is only valid inside a loop"));
        }
        let jump = self.emit_jump(0);
        self.loops
            .last_mut()
            .expect("loop stack was checked above")
            .continue_jumps
            .push(jump);
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: Value::nothing(),
        });
        Ok(dst)
    }

    fn compile_branch_items(
        &mut self,
        items: &[HirItem],
    ) -> Result<(Option<Register>, bool), CompileError> {
        let saved = self.locals.clone();
        let saved_returned = self.returned;
        self.returned = false;
        let mut value = None;
        for item in items {
            value = self.compile_item(item)?;
        }
        let branch_returned = self.returned;
        self.locals = saved;
        self.returned = saved_returned;
        Ok((value, branch_returned))
    }

    fn compile_dispatch(
        &mut self,
        id: NodeId,
        selector: &HirExpr,
        args: &[HirArg],
        receiver: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let method_relations = self
            .context
            .method_relations
            .ok_or_else(|| self.unsupported(id, "method relation ids are not configured"))?;
        self.ensure_no_arg_splices(args, "dispatch argument splices are not implemented yet")?;
        let selector = self.compile_expr_for_operand(selector)?;
        let mut roles = Vec::new();
        if let Some(receiver) = receiver {
            roles.push((
                Value::symbol(Symbol::intern("receiver")),
                Operand::Register(self.compile_expr_for_value(receiver)?),
            ));
        }
        for arg in args {
            let Some(role) = &arg.role else {
                return Err(
                    self.unsupported(arg.id, "dispatch arguments must use explicit role names")
                );
            };
            roles.push((
                Value::symbol(Symbol::intern(role)),
                self.compile_arg_operand(arg)?,
            ));
        }
        let dst = self.alloc_register();
        self.emit(Instruction::Dispatch {
            dst,
            relations: method_relations.dispatch,
            program_relation: method_relations.method_program,
            program_bytes: method_relations.program_bytes,
            selector,
            roles,
        });
        Ok(dst)
    }

    fn compile_expr_for_operand(&mut self, expr: &HirExpr) -> Result<Operand, CompileError> {
        match expr {
            HirExpr::Symbol { name, .. } => Ok(Operand::Value(Value::symbol(Symbol::intern(name)))),
            HirExpr::Identity { id, name } => {
                let identity =
                    self.context
                        .identity(name)
                        .ok_or_else(|| CompileError::UnknownIdentity {
                            node: *id,
                            span: self.span(*id),
                            name: name.clone(),
                        })?;
                Ok(Operand::Value(Value::identity(identity)))
            }
            HirExpr::Literal { id, value } => Ok(Operand::Value(self.literal_value(*id, value)?)),
            _ => Ok(Operand::Register(self.compile_expr_for_value(expr)?)),
        }
    }

    fn compile_relation_exists(
        &mut self,
        atom: &HirRelationAtom,
    ) -> Result<Register, CompileError> {
        let relation = self.relation_id(atom)?;
        self.ensure_no_arg_splices(
            &atom.args,
            "relation argument splices are not implemented yet",
        )?;
        let dst = self.alloc_register();
        let bindings = atom
            .args
            .iter()
            .map(|arg| self.compile_arg_operand(arg).map(Some))
            .collect::<Result<Vec<_>, _>>()?;
        self.emit(Instruction::ScanExists {
            dst,
            relation,
            bindings,
        });
        Ok(dst)
    }

    fn compile_fact_change(
        &mut self,
        kind: &EffectKind,
        atom: &HirRelationAtom,
    ) -> Result<(), CompileError> {
        let relation = self.relation_id(atom)?;
        self.ensure_no_arg_splices(
            &atom.args,
            "relation argument splices are not implemented yet",
        )?;
        let values = atom
            .args
            .iter()
            .map(|arg| self.compile_arg_operand(arg))
            .collect::<Result<Vec<_>, _>>()?;
        match kind {
            EffectKind::Assert => self.emit(Instruction::Assert { relation, values }),
            EffectKind::Retract => self.emit(Instruction::Retract { relation, values }),
            EffectKind::Require => {
                return Err(self.unsupported(atom.id, "require is not a fact change instruction"));
            }
        }
        Ok(())
    }

    fn compile_arg_operand(&mut self, arg: &HirArg) -> Result<Operand, CompileError> {
        Ok(Operand::Register(self.compile_expr_for_value(&arg.value)?))
    }

    fn ensure_no_arg_splices(&self, args: &[HirArg], message: &str) -> Result<(), CompileError> {
        if let Some(arg) = args.iter().find(|arg| arg.splice) {
            return Err(self.unsupported(arg.id, message));
        }
        Ok(())
    }

    fn relation_id(&self, atom: &HirRelationAtom) -> Result<RelationId, CompileError> {
        self.context
            .relation(&atom.name)
            .ok_or_else(|| CompileError::UnknownRelation {
                node: atom.id,
                span: self.span(atom.id),
                name: atom.name.clone(),
            })
    }

    fn load_usize(&mut self, value: usize, id: NodeId) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        self.emit(Instruction::Load {
            dst,
            value: self.usize_value(value, id)?,
        });
        Ok(dst)
    }

    fn usize_operand(&self, value: usize, id: NodeId) -> Result<Operand, CompileError> {
        Ok(Operand::Value(self.usize_value(value, id)?))
    }

    fn usize_value(&self, value: usize, id: NodeId) -> Result<Value, CompileError> {
        let value = i64::try_from(value).map_err(|error| CompileError::InvalidLiteral {
            node: id,
            span: self.span(id),
            message: format!("scatter index is too large: {error}"),
        })?;
        Value::int(value).map_err(|error| self.value_error(id, error))
    }

    fn literal_value(&self, id: NodeId, literal: &Literal) -> Result<Value, CompileError> {
        match literal {
            Literal::Int(value) => {
                let value = value
                    .parse::<i64>()
                    .map_err(|error| CompileError::InvalidLiteral {
                        node: id,
                        span: self.span(id),
                        message: format!("invalid integer literal: {error}"),
                    })?;
                Value::int(value).map_err(|error| self.value_error(id, error))
            }
            Literal::Float(value) => {
                let value = value
                    .parse::<f64>()
                    .map_err(|error| CompileError::InvalidLiteral {
                        node: id,
                        span: self.span(id),
                        message: format!("invalid float literal: {error}"),
                    })?;
                Ok(Value::float(value))
            }
            Literal::String(value) => Ok(Value::string(value)),
            Literal::Bool(value) => Ok(Value::bool(*value)),
            Literal::ErrorCode(value) => Ok(Value::error_code(Symbol::intern(value))),
            Literal::Nothing => Ok(Value::nothing()),
        }
    }

    fn value_error(&self, id: NodeId, error: ValueError) -> CompileError {
        CompileError::InvalidLiteral {
            node: id,
            span: self.span(id),
            message: format!("{error:?}"),
        }
    }

    fn alloc_register(&mut self) -> Register {
        let register = Register(self.next_register);
        self.next_register += 1;
        register
    }

    fn emit(&mut self, instruction: Instruction) {
        self.instructions.push(instruction);
    }

    fn emit_branch(&mut self, condition: Register, if_true: usize, if_false: usize) -> usize {
        let index = self.instructions.len();
        self.emit(Instruction::Branch {
            condition,
            if_true,
            if_false,
        });
        index
    }

    fn emit_jump(&mut self, target: usize) -> usize {
        let index = self.instructions.len();
        self.emit(Instruction::Jump { target });
        index
    }

    fn patch_branch(
        &mut self,
        index: usize,
        if_true: usize,
        if_false: usize,
    ) -> Result<(), CompileError> {
        let Some(Instruction::Branch {
            if_true: true_slot,
            if_false: false_slot,
            ..
        }) = self.instructions.get_mut(index)
        else {
            return Err(CompileError::Unsupported {
                node: NodeId(0),
                span: None,
                message: "internal compiler error: expected branch instruction".to_owned(),
            });
        };
        *true_slot = if_true;
        *false_slot = if_false;
        Ok(())
    }

    fn patch_true_target(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        let Some(Instruction::Branch { if_true, .. }) = self.instructions.get_mut(index) else {
            return Err(CompileError::Unsupported {
                node: NodeId(0),
                span: None,
                message: "internal compiler error: expected branch instruction".to_owned(),
            });
        };
        *if_true = target;
        Ok(())
    }

    fn patch_false_target(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        let Some(Instruction::Branch { if_false, .. }) = self.instructions.get_mut(index) else {
            return Err(CompileError::Unsupported {
                node: NodeId(0),
                span: None,
                message: "internal compiler error: expected branch instruction".to_owned(),
            });
        };
        *if_false = target;
        Ok(())
    }

    fn patch_jump(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        let Some(Instruction::Jump { target: slot }) = self.instructions.get_mut(index) else {
            return Err(CompileError::Unsupported {
                node: NodeId(0),
                span: None,
                message: "internal compiler error: expected jump instruction".to_owned(),
            });
        };
        *slot = target;
        Ok(())
    }

    fn patch_enter_try(
        &mut self,
        index: usize,
        new_catches: Vec<CatchHandler>,
        new_finally: Option<usize>,
        new_end: usize,
    ) -> Result<(), CompileError> {
        let Some(Instruction::EnterTry {
            catches,
            finally,
            end,
        }) = self.instructions.get_mut(index)
        else {
            return Err(CompileError::Unsupported {
                node: NodeId(0),
                span: None,
                message: "internal compiler error: expected enter-try instruction".to_owned(),
            });
        };
        *catches = new_catches;
        *finally = new_finally;
        *end = new_end;
        Ok(())
    }

    fn unsupported(&self, node: NodeId, message: impl Into<String>) -> CompileError {
        CompileError::Unsupported {
            node,
            span: self.span(node),
            message: message.into(),
        }
    }

    fn span(&self, node: NodeId) -> Option<Span> {
        self.semantic.span(node).cloned()
    }
}

fn expr_id(expr: &HirExpr) -> NodeId {
    match expr {
        HirExpr::Literal { id, .. }
        | HirExpr::LocalRef { id, .. }
        | HirExpr::ExternalRef { id, .. }
        | HirExpr::Identity { id, .. }
        | HirExpr::Symbol { id, .. }
        | HirExpr::Hole { id }
        | HirExpr::List { id, .. }
        | HirExpr::Map { id, .. }
        | HirExpr::Unary { id, .. }
        | HirExpr::Binary { id, .. }
        | HirExpr::Assign { id, .. }
        | HirExpr::Call { id, .. }
        | HirExpr::RoleDispatch { id, .. }
        | HirExpr::ReceiverDispatch { id, .. }
        | HirExpr::FactChange { id, .. }
        | HirExpr::Require { id, .. }
        | HirExpr::Index { id, .. }
        | HirExpr::Field { id, .. }
        | HirExpr::Binding { id, .. }
        | HirExpr::If { id, .. }
        | HirExpr::Block { id, .. }
        | HirExpr::For { id, .. }
        | HirExpr::While { id, .. }
        | HirExpr::Return { id, .. }
        | HirExpr::Raise { id, .. }
        | HirExpr::Recover { id, .. }
        | HirExpr::Break { id }
        | HirExpr::Continue { id }
        | HirExpr::Try { id, .. }
        | HirExpr::Function { id, .. }
        | HirExpr::Error { id } => *id,
        HirExpr::RelationAtom(atom) => atom.id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_relation_kernel::{ConflictPolicy, RelationKernel, RelationMetadata, Tuple};
    use mica_runtime::{Scheduler, TaskOutcome};

    fn id(raw: u64) -> Identity {
        Identity::new(raw).unwrap()
    }

    fn dispatch_relations() -> MethodRelations {
        MethodRelations {
            dispatch: DispatchRelations {
                method_selector: id(40),
                param: id(41),
                delegates: id(42),
            },
            method_program: id(43),
            program_bytes: id(44),
        }
    }

    fn create_method_relations(kernel: &RelationKernel) {
        let relations = dispatch_relations();
        kernel
            .create_relation(
                RelationMetadata::new(
                    relations.dispatch.method_selector,
                    Symbol::intern("MethodSelector"),
                    2,
                )
                .with_index([1, 0]),
            )
            .unwrap();
        kernel
            .create_relation(
                RelationMetadata::new(relations.dispatch.param, Symbol::intern("Param"), 3)
                    .with_index([0, 1]),
            )
            .unwrap();
        kernel
            .create_relation(
                RelationMetadata::new(relations.dispatch.delegates, Symbol::intern("Delegates"), 3)
                    .with_index([0, 2, 1]),
            )
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                relations.method_program,
                Symbol::intern("MethodProgram"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                relations.program_bytes,
                Symbol::intern("ProgramBytes"),
                2,
            ))
            .unwrap();
    }

    #[test]
    fn compiles_open_error_code_literals() {
        let context = CompileContext::new();
        let compiled = compile_source("return E_NOT_PORTABLE", &context).unwrap();
        assert_eq!(
            compiled.program.instructions(),
            &[
                Instruction::Load {
                    dst: Register(0),
                    value: Value::error_code(Symbol::intern("E_NOT_PORTABLE")),
                },
                Instruction::Return {
                    value: Operand::Register(Register(0)),
                },
                Instruction::Load {
                    dst: Register(1),
                    value: Value::nothing(),
                },
            ]
        );
    }

    #[test]
    fn compiled_task_catches_raised_error_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               raise E_NOT_PORTABLE, \"That cannot be taken.\", :lamp\n\
             catch err if E_NOT_PORTABLE\n\
               return err\n\
             end",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::error(
                    Symbol::intern("E_NOT_PORTABLE"),
                    Some("That cannot be taken."),
                    Some(Value::symbol(Symbol::intern("lamp"))),
                ),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_supports_code_first_catch_binding_and_error_fields() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               raise E_NOT_PORTABLE, \"That cannot be taken.\", :lamp\n\
             catch E_NOT_PORTABLE as err\n\
               return (err.code == E_NOT_PORTABLE) and (err.message == \"That cannot be taken.\") and (err.value == :lamp)\n\
             end",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_finally_during_return_unwind() {
        let cleaned = id(1);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(cleaned, Symbol::intern("Cleaned"), 1))
            .unwrap();
        let context = CompileContext::new().with_relation("Cleaned", cleaned);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               return 7\n\
             finally\n\
               assert Cleaned(:done)\n\
             end",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(7).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(cleaned, &[Some(Value::symbol(Symbol::intern("done")))])
                .unwrap(),
            vec![Tuple::from([Value::symbol(Symbol::intern("done"))])]
        );
    }

    #[test]
    fn compiled_task_recovers_expression_errors_inline() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let fallback = recover raise E_NOT_PORTABLE\n\
             catch E_NOT_PORTABLE => 10\n\
             end\n\
             let untouched = recover 1\n\
             catch E_NOT_PORTABLE => 99\n\
             end\n\
             return fallback + untouched",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(11).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_supports_code_first_recover_binding() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let code = recover raise E_NO_EXIT, \"No exit.\"\n\
             catch E_NO_EXIT as err => err.code\n\
             end\n\
             return code == E_NO_EXIT",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiles_source_to_transactional_scheduler_task() {
        let located_in = id(1);
        let alice = id(10);
        let room = id(11);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        let context = CompileContext::new()
            .with_relation("LocatedIn", located_in)
            .with_identity("alice", alice)
            .with_identity("room", room);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let actor = $alice\n\
             assert LocatedIn(actor, $room)\n\
             require LocatedIn(actor, $room)\n\
             return true",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
        let tuples = scheduler
            .kernel()
            .snapshot()
            .scan(
                located_in,
                &[Some(Value::identity(alice)), Some(Value::identity(room))],
            )
            .unwrap();
        assert_eq!(
            tuples,
            vec![Tuple::from(
                [Value::identity(alice), Value::identity(room),]
            )]
        );
    }

    #[test]
    fn require_aborts_and_rolls_back_pending_asserts() {
        let located_in = id(1);
        let visible = id(2);
        let alice = id(10);
        let room = id(11);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(visible, Symbol::intern("Visible"), 2))
            .unwrap();
        let context = CompileContext::new()
            .with_relation("LocatedIn", located_in)
            .with_relation("Visible", visible)
            .with_identity("alice", alice)
            .with_identity("room", room);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "assert LocatedIn($alice, $room)\n\
             require Visible($alice, $room)\n\
             return true",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Aborted { .. }));
        let tuples = scheduler
            .kernel()
            .snapshot()
            .scan(located_in, &[None, None])
            .unwrap();
        assert_eq!(tuples, vec![]);
    }

    #[test]
    fn compiled_task_builds_and_indexes_collections() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let values = [10, 20, 30]\n\
             let labels = {:answer -> values[1]}\n\
             return labels[:answer]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(20).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_slices_lists_with_ranges() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let values = [0, 1, 2, 3, 4]\n\
             let mid = values[1..3]\n\
             let tail = values[2..$]\n\
             return mid[0] + mid[1] + mid[2] + tail[0] + tail[1] + tail[2]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(15).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_list_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let rest = [2, 3]\n\
             let values = [1, @rest, 4]\n\
             return values[0] + values[1] + values[2] + values[3]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_scatter_assignment_with_required_optional_and_rest_parts() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let [head, ?middle = 10, @tail] = [1, 2, 3, 4]\n\
             return head + middle + tail[0] + tail[1]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_uses_scatter_optional_defaults_and_empty_rest() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let [head, ?middle = 10, @tail] = [1]\n\
             return (head == 1) and (middle == 10) and (tail[0] == nothing)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_list_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let values = [10, 20, 30]\n\
             values[1] = 99\n\
             return values[1]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(99).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_map_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let counts = {:a -> 1}\n\
             counts[:a] = 2\n\
             counts[:b] = 3\n\
             return counts[:a] + counts[:b]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(5).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_values_inside_loop() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let values = [1, 2, 3]\n\
             for index, item in values\n\
               values[index] = item * 10\n\
             end\n\
             return values[0] + values[1] + values[2]",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(60).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_reads_and_writes_declared_dot_relations() {
        let name = id(1);
        let lamp = id(10);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(
                RelationMetadata::new(name, Symbol::intern("Name"), 2).with_conflict_policy(
                    ConflictPolicy::Functional {
                        key_positions: vec![0],
                    },
                ),
            )
            .unwrap();
        let context = CompileContext::new()
            .with_dot_relation("name", name)
            .with_identity("lamp", lamp);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "$lamp.name = \"brass lamp\"\n\
             return $lamp.name",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("brass lamp"),
                effects: vec![],
                retries: 0,
            }
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(name, &[Some(Value::identity(lamp)), None])
                .unwrap(),
            vec![Tuple::from([
                Value::identity(lamp),
                Value::string("brass lamp"),
            ])]
        );
    }

    #[test]
    fn undeclared_dot_names_are_rejected() {
        let lamp = id(10);
        let context = CompileContext::new().with_identity("lamp", lamp);
        let error = compile_source("$lamp.color", &context).unwrap_err();

        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. } if message == "dot name `color` is not declared"
        ));
    }

    #[test]
    fn compiled_task_runs_scalar_arithmetic() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let a = 20 - 3 * 4\n\
             let b = a / 2\n\
             let c = b % 3\n\
             return -c",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(-1).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_named_local_functions() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "fn double(x)\n\
               return x * 2\n\
             end\n\
             fn add(left, right)\n\
               return left + right\n\
             end\n\
             return add(double(10), 1)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(21).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_let_bound_function_literals() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let triple = fn(x) => x * 3\n\
             return triple(7)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(21).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_functions_with_optional_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "fn pick(value, ?fallback = 10)\n\
               return value + fallback\n\
             end\n\
             return pick(1) + pick(1, 2)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(14).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_functions_with_rest_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "fn sum(first, @rest)\n\
               return first + rest[0] + rest[1]\n\
             end\n\
             fn empty(first, @rest)\n\
               return (first == 1) and (rest[0] == nothing)\n\
             end\n\
             return sum(1, 2, 3) + if empty(1)\n\
               10\n\
             else\n\
               0\n\
             end",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(16).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_direct_call_argument_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "fn sum3(a, b, c)\n\
               return a + b + c\n\
             end\n\
             let rest = [2, 3]\n\
             return sum3(1, @rest)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_combines_optional_rest_and_call_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "fn total(first, ?second = 10, @rest)\n\
               return first + second + rest[0] + rest[1]\n\
             end\n\
             let extra = [3, 4]\n\
             return total(1, 2, @extra)",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn direct_function_calls_validate_arity() {
        let context = CompileContext::new();
        let error = compile_source(
            "fn add(left, right)\n\
               return left + right\n\
             end\n\
             return add(1)",
            &context,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. }
                if message == "function call expected at least 2 arguments but got 1"
        ));
    }

    #[test]
    fn captured_function_locals_are_rejected() {
        let context = CompileContext::new();
        let error = compile_source(
            "let factor = 2\n\
             fn scale(x)\n\
               return x * factor\n\
             end\n\
             return scale(10)",
            &context,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. }
                if message == "closures are not implemented in the task compiler yet"
        ));
    }

    #[test]
    fn compiled_task_runs_while_loops() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let i = 0\n\
             let total = 0\n\
             while i < 5\n\
               i = i + 1\n\
               total = total + i\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(15).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_break_and_continue() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let i = 0\n\
             let total = 0\n\
             while i < 10\n\
               i = i + 1\n\
               if i == 2\n\
                 continue\n\
               end\n\
               if i == 5\n\
                 break\n\
               end\n\
               total = total + i\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(8).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_list_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for item in [1, 2, 3]\n\
               total = total + item\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_list_indexes_and_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for index, item in [4, 5]\n\
               total = total + index + item\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_map_keys_and_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for key, value in {:a -> 10, :b -> 20}\n\
               if key == :b\n\
                 total = total + value\n\
               end\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(20).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_break_and_continue() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for item in [1, 2, 3, 4, 5]\n\
               if item == 2\n\
                 continue\n\
               end\n\
               if item == 5\n\
                 break\n\
               end\n\
               total = total + item\n\
             end\n\
             return total",
            &context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(8).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
    }

    #[test]
    fn installs_method_facts_and_invokes_method_through_dispatch() {
        let located_in = id(1);
        let get_method = id(100);
        let get_program = id(101);
        let player = id(200);
        let thing = id(201);
        let alice = id(300);
        let coin = id(301);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_relation("LocatedIn", located_in)
            .with_method_relations(method_relations)
            .with_identity("get_thing", get_method)
            .with_program_identity("get_thing", get_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        let installation = install_methods_from_source(
            "method $get_thing :get\n\
               roles actor: $player, item: $thing\n\
             do\n\
               assert LocatedIn(item, actor)\n\
               return true\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(coin),
                    Value::identity(thing),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        assert_eq!(installation.methods.len(), 1);
        assert_eq!(
            kernel
                .snapshot()
                .scan(
                    method_relations.dispatch.method_selector,
                    &[
                        Some(Value::identity(get_method)),
                        Some(Value::symbol(Symbol::intern("get"))),
                    ],
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            kernel
                .snapshot()
                .scan(
                    method_relations.method_program,
                    &[
                        Some(Value::identity(get_method)),
                        Some(Value::identity(get_program))
                    ],
                )
                .unwrap()
                .len(),
            1
        );

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice)
            .with_identity("coin", coin);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            ":get(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();
        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(
                    located_in,
                    &[Some(Value::identity(coin)), Some(Value::identity(alice))],
                )
                .unwrap(),
            vec![Tuple::from(
                [Value::identity(coin), Value::identity(alice),]
            )]
        );
    }

    #[test]
    fn persisted_method_can_return_indexed_collection_values() {
        let inspect_method = id(100);
        let inspect_program = id(101);
        let player = id(200);
        let thing = id(201);
        let alice = id(300);
        let coin = id(301);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("inspect_thing", inspect_method)
            .with_program_identity("inspect_thing", inspect_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $inspect_thing :inspect\n\
               roles actor: $player, item: $thing\n\
             do\n\
               let values = [actor, item]\n\
               let result = {:target -> values[1]}\n\
               return result[:target]\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(coin),
                    Value::identity(thing),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice)
            .with_identity("coin", coin);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            ":inspect(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::identity(coin),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(inspect_program))
        );
    }

    #[test]
    fn persisted_method_can_run_while_loop() {
        let count_method = id(100);
        let count_program = id(101);
        let player = id(200);
        let alice = id(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("count_loop", count_method)
            .with_program_identity("count_loop", count_program)
            .with_identity("player", player);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $count_loop :count\n\
               roles actor: $player\n\
             do\n\
               let i = 0\n\
               while i < 3\n\
                 i = i + 1\n\
               end\n\
               return i\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice);
        let mut scheduler = Scheduler::new(kernel);
        let submitted =
            submit_source_task(":count(actor: $alice)", &invoke_context, &mut scheduler).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(3).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(count_program))
        );
    }

    #[test]
    fn persisted_method_can_run_for_loop() {
        let count_method = id(100);
        let count_program = id(101);
        let player = id(200);
        let alice = id(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("sum_loop", count_method)
            .with_program_identity("sum_loop", count_program)
            .with_identity("player", player);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $sum_loop :sum\n\
               roles actor: $player\n\
             do\n\
               let total = 0\n\
               for item in [2, 3, 4]\n\
                 total = total + item\n\
               end\n\
               return total\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice);
        let mut scheduler = Scheduler::new(kernel);
        let submitted =
            submit_source_task(":sum(actor: $alice)", &invoke_context, &mut scheduler).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(9).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(count_program))
        );
    }

    #[test]
    fn persisted_method_can_run_scalar_arithmetic() {
        let calc_method = id(100);
        let calc_program = id(101);
        let player = id(200);
        let alice = id(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("calc", calc_method)
            .with_program_identity("calc", calc_program)
            .with_identity("player", player);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $calc :calc\n\
               roles actor: $player\n\
             do\n\
               return (10 * 3 - 4) / 2\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice);
        let mut scheduler = Scheduler::new(kernel);
        let submitted =
            submit_source_task(":calc(actor: $alice)", &invoke_context, &mut scheduler).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(13).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(calc_program))
        );
    }

    #[test]
    fn persisted_method_can_assign_indexed_values() {
        let update_method = id(100);
        let update_program = id(101);
        let player = id(200);
        let alice = id(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("update", update_method)
            .with_program_identity("update", update_program)
            .with_identity("player", player);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $update :update\n\
               roles actor: $player\n\
             do\n\
               let counts = {:seen -> 1}\n\
               counts[:seen] = counts[:seen] + 1\n\
               return counts[:seen]\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice);
        let mut scheduler = Scheduler::new(kernel);
        let submitted =
            submit_source_task(":update(actor: $alice)", &invoke_context, &mut scheduler).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(2).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(update_program))
        );
    }

    #[test]
    fn persisted_method_can_read_and_write_declared_dot_relations() {
        let name = id(1);
        let rename_method = id(100);
        let rename_program = id(101);
        let player = id(200);
        let thing = id(201);
        let alice = id(300);
        let coin = id(301);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);
        kernel
            .create_relation(
                RelationMetadata::new(name, Symbol::intern("Name"), 2).with_conflict_policy(
                    ConflictPolicy::Functional {
                        key_positions: vec![0],
                    },
                ),
            )
            .unwrap();

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_dot_relation("name", name)
            .with_method_relations(method_relations)
            .with_identity("rename_thing", rename_method)
            .with_program_identity("rename_thing", rename_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $rename_thing :rename\n\
               roles actor: $player, item: $thing\n\
             do\n\
               item.name = \"bright coin\"\n\
               return item.name\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(coin),
                    Value::identity(thing),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice)
            .with_identity("coin", coin);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            ":rename(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("bright coin"),
                effects: vec![],
                retries: 0,
            }
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(name, &[Some(Value::identity(coin)), None])
                .unwrap(),
            vec![Tuple::from([
                Value::identity(coin),
                Value::string("bright coin"),
            ])]
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(rename_program))
        );
    }

    #[test]
    fn persisted_method_can_call_local_function() {
        let calc_method = id(100);
        let calc_program = id(101);
        let player = id(200);
        let alice = id(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("calc", calc_method)
            .with_program_identity("calc", calc_program)
            .with_identity("player", player);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $calc :calc\n\
               roles actor: $player\n\
             do\n\
               fn double(x)\n\
                 return x * 2\n\
               end\n\
               return double(21)\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice);
        let mut scheduler = Scheduler::new(kernel);
        let submitted =
            submit_source_task(":calc(actor: $alice)", &invoke_context, &mut scheduler).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(calc_program))
        );
    }

    #[test]
    fn persisted_method_can_dispatch_to_another_persisted_method() {
        let located_in = id(1);
        let get_method = id(100);
        let get_program = id(101);
        let mark_method = id(102);
        let mark_program = id(103);
        let player = id(200);
        let thing = id(201);
        let alice = id(300);
        let coin = id(301);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_relation("LocatedIn", located_in)
            .with_method_relations(method_relations)
            .with_identity("get_thing", get_method)
            .with_program_identity("get_thing", get_program)
            .with_identity("mark_thing", mark_method)
            .with_program_identity("mark_thing", mark_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        let installation = install_methods_from_source(
            "method $mark_thing :mark\n\
               roles actor: $player, item: $thing\n\
             do\n\
               assert LocatedIn(item, actor)\n\
               return true\n\
             end\n\
             method $get_thing :get\n\
               roles actor: $player, item: $thing\n\
             do\n\
               :mark(actor: actor, item: item)\n\
               return true\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(coin),
                    Value::identity(thing),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        assert_eq!(installation.methods.len(), 2);
        assert_eq!(
            kernel
                .snapshot()
                .scan(method_relations.program_bytes, &[None, None])
                .unwrap()
                .len(),
            2
        );

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice)
            .with_identity("coin", coin);
        let mut scheduler = Scheduler::new(kernel);
        let submitted = submit_source_task(
            ":get(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
        assert!(scheduler.resolver().contains(&Value::identity(get_program)));
        assert!(
            scheduler
                .resolver()
                .contains(&Value::identity(mark_program))
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(
                    located_in,
                    &[Some(Value::identity(coin)), Some(Value::identity(alice))],
                )
                .unwrap(),
            vec![Tuple::from(
                [Value::identity(coin), Value::identity(alice),]
            )]
        );
    }

    #[test]
    fn dispatched_method_can_branch_on_relation_predicates() {
        let portable = id(1);
        let located_in = id(2);
        let take_method = id(100);
        let take_program = id(101);
        let player = id(200);
        let thing = id(201);
        let alice = id(300);
        let coin = id(301);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel);
        kernel
            .create_relation(RelationMetadata::new(
                portable,
                Symbol::intern("Portable"),
                2,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_relation("Portable", portable)
            .with_relation("LocatedIn", located_in)
            .with_method_relations(method_relations)
            .with_identity("take_thing", take_method)
            .with_program_identity("take_thing", take_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method $take_thing :take\n\
               roles actor: $player, item: $thing\n\
             do\n\
               if Portable(item, true) && !LocatedIn(item, actor)\n\
                 assert LocatedIn(item, actor)\n\
                 return true\n\
               else\n\
                 return false\n\
               end\n\
             end",
            &install_context,
            &mut install_tx,
        )
        .unwrap();
        install_tx
            .assert(
                portable,
                Tuple::from([Value::identity(coin), Value::bool(true)]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(alice),
                    Value::identity(player),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx
            .assert(
                method_relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(coin),
                    Value::identity(thing),
                    Value::int(0).unwrap(),
                ]),
            )
            .unwrap();
        install_tx.commit().unwrap();

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_identity("alice", alice)
            .with_identity("coin", coin);
        let mut scheduler = Scheduler::new(kernel);
        let first = submit_source_task(
            ":take(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();
        assert_eq!(
            first.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                retries: 0,
            }
        );
        assert_eq!(
            scheduler
                .kernel()
                .snapshot()
                .scan(
                    located_in,
                    &[Some(Value::identity(coin)), Some(Value::identity(alice))],
                )
                .unwrap()
                .len(),
            1
        );

        let second = submit_source_task(
            ":take(actor: $alice, item: $coin)",
            &invoke_context,
            &mut scheduler,
        )
        .unwrap();
        assert_eq!(
            second.outcome,
            TaskOutcome::Complete {
                value: Value::bool(false),
                effects: vec![],
                retries: 0,
            }
        );
    }
}
