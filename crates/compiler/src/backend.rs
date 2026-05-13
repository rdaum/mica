use crate::{
    BindingId, Diagnostic, EffectKind, HirArg, HirExpr, HirItem, HirPlace, HirProgram,
    HirRelationAtom, Literal, NodeId, SemanticProgram, Span, parse_semantic,
};
use mica_relation_kernel::{DispatchRelations, RelationId, RelationRead, Transaction, Tuple};
use mica_runtime::{
    Instruction, Operand, Program, ProgramStore, Register, Scheduler, SchedulerError, TaskId,
    TaskOutcome,
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
    let mut store = ProgramStore::new();
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
            store.insert(method.program.clone(), method.compiled.program.clone());
            methods.push(method);
        }
    }

    Ok(MethodInstallation {
        semantic,
        program_store: Arc::new(store),
        methods,
    })
}

pub fn load_program_store(
    reader: &impl RelationRead,
    method_relations: MethodRelations,
) -> Result<Arc<ProgramStore>, CompileError> {
    let rows = reader.scan_relation(method_relations.program_bytes, &[None, None])?;
    let mut store = ProgramStore::new();
    for row in rows {
        let program_id = row.values()[0].clone();
        let bytes = row.values()[1].with_bytes(<[u8]>::to_vec).ok_or_else(|| {
            CompileError::InvalidProgramArtifact {
                program: program_id.clone(),
                message: "ProgramBytes value is not bytes".to_owned(),
            }
        })?;
        let program = Program::from_bytes(&bytes).map_err(CompileError::Runtime)?;
        store.insert(program_id, program);
    }
    Ok(Arc::new(store))
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
    pub program_store: Arc<ProgramStore>,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompileContext {
    relations: HashMap<String, RelationId>,
    identities: HashMap<String, Identity>,
    program_identities: HashMap<String, Identity>,
    method_relations: Option<MethodRelations>,
    program_store: Option<Arc<ProgramStore>>,
}

impl CompileContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_relation(mut self, name: impl Into<String>, id: RelationId) -> Self {
        self.define_relation(name, id);
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

    pub fn with_program_store(mut self, program_store: Arc<ProgramStore>) -> Self {
        self.program_store = Some(program_store);
        self
    }

    pub fn define_relation(&mut self, name: impl Into<String>, id: RelationId) {
        self.relations.insert(name.into(), id);
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

    pub fn identity(&self, name: &str) -> Option<Identity> {
        self.identities.get(name).copied()
    }

    pub fn program_identity(&self, method: &str) -> Option<Identity> {
        self.program_identities.get(method).copied()
    }

    pub fn method_relations(&self) -> Option<MethodRelations> {
        self.method_relations
    }

    pub fn program_store(&self) -> Option<Arc<ProgramStore>> {
        self.program_store.clone()
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
    InvalidProgramArtifact {
        program: Value,
        message: String,
    },
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

struct ProgramCompiler<'a> {
    semantic: &'a SemanticProgram,
    context: &'a CompileContext,
    instructions: Vec<Instruction>,
    next_register: u16,
    locals: HashMap<BindingId, Register>,
    external_locals: HashMap<String, Register>,
    returned: bool,
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
                binding, value, id, ..
            } => {
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
                        self.locals.insert(*binding, value);
                        Ok(value)
                    }
                    _ => Err(self.unsupported(
                        *id,
                        "only local assignment is implemented in the task compiler yet",
                    )),
                }
            }
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
        let programs = self
            .context
            .program_store
            .clone()
            .ok_or_else(|| self.unsupported(id, "method program store is not configured"))?;
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
            programs,
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

    fn relation_id(&self, atom: &HirRelationAtom) -> Result<RelationId, CompileError> {
        self.context
            .relation(&atom.name)
            .ok_or_else(|| CompileError::UnknownRelation {
                node: atom.id,
                span: self.span(atom.id),
                name: atom.name.clone(),
            })
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
    use mica_relation_kernel::{RelationKernel, RelationMetadata, Tuple};
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
        assert!(
            installation
                .program_store
                .contains(&Value::identity(get_program))
        );
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
        let reloaded_store = load_program_store(&*kernel.snapshot(), method_relations).unwrap();
        assert!(reloaded_store.contains(&Value::identity(get_program)));

        let invoke_context = CompileContext::new()
            .with_method_relations(method_relations)
            .with_program_store(reloaded_store)
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
}
