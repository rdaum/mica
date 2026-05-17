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
    BinaryOp, BindingId, Diagnostic, EffectKind, HirArg, HirCatch, HirCollectionItem, HirExpr,
    HirFunctionBody, HirItem, HirPlace, HirProgram, HirRecovery, HirRelationAtom,
    HirScatterBinding, Literal, LocalKind, NodeId, ParamMode, SemanticProgram, Span, UnaryOp,
    parse_semantic,
};
use mica_relation_kernel::{
    Atom, ConflictPolicy, DispatchRelations, RelationId, RelationKernel, RelationMetadata, Rule,
    RuleDefinition, Term, Transaction, Tuple,
};
use mica_var::{Identity, Symbol, Value, ValueError};
use mica_vm::{
    CatchHandler, ErrorField, Instruction, ListItem, MapItem, Operand, Program, ProgramBuilder,
    QueryBinding, Register, RelationArg, RuntimeBinaryOp, RuntimeError, RuntimeUnaryOp,
};
use std::collections::{HashMap, HashSet};
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

pub fn install_rules_from_source(
    source: &str,
    context: &CompileContext,
    kernel: &RelationKernel,
) -> Result<Option<RuleInstallation>, CompileError> {
    let semantic = parse_semantic(source);
    install_rules(semantic, context, kernel, source)
}

pub fn install_rules(
    semantic: SemanticProgram,
    context: &CompileContext,
    kernel: &RelationKernel,
    source: &str,
) -> Result<Option<RuleInstallation>, CompileError> {
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

    if !semantic
        .hir
        .items
        .iter()
        .any(|item| matches!(item, HirItem::RelationRule { .. }))
    {
        return Ok(None);
    }

    if let Some(item) = semantic
        .hir
        .items
        .iter()
        .find(|item| !matches!(item, HirItem::RelationRule { .. }))
    {
        return Err(CompileError::Unsupported {
            node: item_id(item),
            span: semantic.span(item_id(item)).cloned(),
            message: "relation rule definitions cannot be mixed with executable task code yet"
                .to_owned(),
        });
    }

    let rules = semantic
        .hir
        .items
        .iter()
        .map(|item| compile_rule_item(&semantic, context, item))
        .collect::<Result<Vec<_>, _>>()?;
    let definitions = rules
        .into_iter()
        .map(|rule| {
            kernel
                .install_rule(rule, source.to_owned())
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    Ok(Some(RuleInstallation {
        semantic,
        rules: definitions,
    }))
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
            for param in &method.params {
                tx.assert(
                    method_relations.dispatch.param,
                    Tuple::from([
                        method.method.clone(),
                        param.role.clone(),
                        param.restriction.clone(),
                        Value::int(i64::from(param.position)).unwrap(),
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
pub struct MethodInstallation {
    pub semantic: SemanticProgram,
    pub methods: Vec<InstalledMethod>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleInstallation {
    pub semantic: SemanticProgram,
    pub rules: Vec<RuleDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledMethod {
    pub method: Value,
    pub program: Value,
    pub selector: Value,
    pub params: Vec<InstalledParam>,
    pub compiled: CompiledProgram,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledParam {
    pub name: String,
    pub role: Value,
    pub restriction: Value,
    pub position: u16,
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
    relation_metadata: HashMap<RelationId, RelationMetadata>,
    dot_relations: HashMap<String, DotRelation>,
    identities: HashMap<String, Identity>,
    program_identities: HashMap<String, Identity>,
    runtime_functions: HashSet<String>,
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

    pub fn with_relation_metadata(mut self, metadata: RelationMetadata) -> Self {
        self.define_relation_metadata(metadata);
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

    pub fn with_runtime_function(mut self, name: impl Into<String>) -> Self {
        self.define_runtime_function(name);
        self
    }

    pub fn with_method_relations(mut self, method_relations: MethodRelations) -> Self {
        self.method_relations = Some(method_relations);
        self
    }

    pub fn define_relation(&mut self, name: impl Into<String>, id: RelationId) {
        self.relations.insert(name.into(), id);
    }

    pub fn define_relation_metadata(&mut self, metadata: RelationMetadata) {
        if let Some(name) = metadata.name().name() {
            self.define_relation(name, metadata.id());
        }
        self.relation_metadata.insert(metadata.id(), metadata);
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

    pub fn define_runtime_function(&mut self, name: impl Into<String>) {
        self.runtime_functions.insert(name.into());
    }

    pub fn relation(&self, name: &str) -> Option<RelationId> {
        self.relations.get(name).copied()
    }

    pub fn relation_metadata(&self, relation: RelationId) -> Option<&RelationMetadata> {
        self.relation_metadata.get(&relation)
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

    pub fn is_runtime_function(&self, name: &str) -> bool {
        self.runtime_functions.contains(name)
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
    Runtime(RuntimeError),
    Kernel(mica_relation_kernel::KernelError),
}

impl From<RuntimeError> for CompileError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<mica_relation_kernel::KernelError> for CompileError {
    fn from(value: mica_relation_kernel::KernelError) -> Self {
        Self::Kernel(value)
    }
}

fn compile_rule_item(
    semantic: &SemanticProgram,
    context: &CompileContext,
    item: &HirItem,
) -> Result<Rule, CompileError> {
    let HirItem::RelationRule { id, head, body } = item else {
        return Err(CompileError::Unsupported {
            node: item_id(item),
            span: semantic.span(item_id(item)).cloned(),
            message: "only relation rules can be installed as rules".to_owned(),
        });
    };
    let head_relation =
        context
            .relation(&head.name)
            .ok_or_else(|| CompileError::UnknownRelation {
                node: head.id,
                span: semantic.span(head.id).cloned(),
                name: head.name.clone(),
            })?;
    let head_terms = compile_rule_terms(semantic, context, &head.args)?;
    let body = body
        .iter()
        .map(|atom| {
            let relation =
                context
                    .relation(&atom.name)
                    .ok_or_else(|| CompileError::UnknownRelation {
                        node: atom.id,
                        span: semantic.span(atom.id).cloned(),
                        name: atom.name.clone(),
                    })?;
            let terms = compile_rule_terms(semantic, context, &atom.args)?;
            Ok(if atom.negated {
                Atom::negated(relation, terms)
            } else {
                Atom::positive(relation, terms)
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    if body.is_empty() {
        return Err(CompileError::Unsupported {
            node: *id,
            span: semantic.span(*id).cloned(),
            message: "relation rules require at least one body atom".to_owned(),
        });
    }
    Ok(Rule::new(head_relation, head_terms, body))
}

fn compile_rule_terms(
    semantic: &SemanticProgram,
    context: &CompileContext,
    args: &[HirArg],
) -> Result<Vec<Term>, CompileError> {
    args.iter()
        .map(|arg| {
            if arg.role.is_some() || arg.splice {
                return Err(CompileError::Unsupported {
                    node: arg.id,
                    span: semantic.span(arg.id).cloned(),
                    message: "relation rule atoms do not support named or spliced arguments"
                        .to_owned(),
                });
            }
            compile_rule_term(semantic, context, &arg.value)
        })
        .collect()
}

fn compile_rule_term(
    semantic: &SemanticProgram,
    context: &CompileContext,
    expr: &HirExpr,
) -> Result<Term, CompileError> {
    match expr {
        HirExpr::ExternalRef { name, .. } | HirExpr::QueryVar { name, .. } => {
            Ok(Term::Var(Symbol::intern(name)))
        }
        HirExpr::Identity { id, name } => context
            .identity(name)
            .ok_or_else(|| CompileError::UnknownIdentity {
                node: *id,
                span: semantic.span(*id).cloned(),
                name: name.clone(),
            })
            .map(|identity| Term::Value(Value::identity(identity))),
        HirExpr::Symbol { name, .. } => Ok(Term::Value(Value::symbol(Symbol::intern(name)))),
        HirExpr::Literal { id, value } => {
            literal_value_for_rule(semantic, *id, value).map(Term::Value)
        }
        _ => Err(CompileError::Unsupported {
            node: expr_id(expr),
            span: semantic.span(expr_id(expr)).cloned(),
            message: "relation rule terms must be variables or literal values".to_owned(),
        }),
    }
}

fn literal_value_for_rule(
    semantic: &SemanticProgram,
    id: NodeId,
    literal: &Literal,
) -> Result<Value, CompileError> {
    match literal {
        Literal::Int(value) => {
            let value = value
                .parse::<i64>()
                .map_err(|error| CompileError::InvalidLiteral {
                    node: id,
                    span: semantic.span(id).cloned(),
                    message: format!("invalid integer literal: {error}"),
                })?;
            Value::int(value).map_err(|error| CompileError::InvalidLiteral {
                node: id,
                span: semantic.span(id).cloned(),
                message: format!("{error:?}"),
            })
        }
        Literal::Float(value) => {
            let value = value
                .parse::<f64>()
                .map_err(|error| CompileError::InvalidLiteral {
                    node: id,
                    span: semantic.span(id).cloned(),
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
        params,
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
    let params = lower_installed_params(*id, semantic, context, params, clauses)?;

    let mut compiler = ProgramCompiler::new(semantic, context);
    compiler.next_register = params.len() as u16;
    for (idx, param) in params.iter().enumerate() {
        compiler
            .external_locals
            .insert(param.name.clone(), Register(idx as u16));
    }
    let compiled_program = compiler.compile_items(body)?;
    Ok(InstalledMethod {
        method: method.clone(),
        program: program_id,
        selector: Value::symbol(Symbol::intern(selector)),
        params,
        compiled: CompiledProgram {
            semantic: semantic.clone(),
            program: compiled_program,
        },
    })
}

fn lower_installed_params(
    id: NodeId,
    semantic: &SemanticProgram,
    context: &CompileContext,
    params: &[crate::MethodParam],
    clauses: &[String],
) -> Result<Vec<InstalledParam>, CompileError> {
    if !params.is_empty() {
        return params
            .iter()
            .enumerate()
            .map(|(position, param)| {
                let restriction = match &param.restriction {
                    Some(restriction) => {
                        compile_param_restriction(id, semantic, context, restriction)?
                    }
                    None => Value::nothing(),
                };
                Ok(InstalledParam {
                    name: param.name.clone(),
                    role: Value::symbol(Symbol::intern(&param.name)),
                    restriction,
                    position: u16::try_from(position).map_err(|_| CompileError::Unsupported {
                        node: id,
                        span: semantic.span(id).cloned(),
                        message: "method parameter count exceeds supported limit".to_owned(),
                    })?,
                })
            })
            .collect();
    }

    let mut installed = Vec::new();
    for clause in clauses {
        let clause = clause.trim();
        let clause = clause.strip_prefix("roles").unwrap_or(clause).trim();
        if clause.is_empty() {
            continue;
        }
        for part in clause
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            if part.contains(':') {
                continue;
            }
            let (name, restriction) = match part.split_once('@') {
                Some((name, restriction)) => {
                    let restriction =
                        compile_param_restriction(id, semantic, context, restriction)?;
                    (name.trim(), restriction)
                }
                None => (part, Value::nothing()),
            };
            if name.is_empty() {
                continue;
            }
            installed.push(InstalledParam {
                name: name.to_owned(),
                role: Value::symbol(Symbol::intern(name)),
                restriction,
                position: u16::try_from(installed.len()).map_err(|_| {
                    CompileError::Unsupported {
                        node: id,
                        span: semantic.span(id).cloned(),
                        message: "method parameter count exceeds supported limit".to_owned(),
                    }
                })?,
            });
        }
    }
    Ok(installed)
}

fn compile_param_restriction(
    id: NodeId,
    semantic: &SemanticProgram,
    context: &CompileContext,
    restriction: &str,
) -> Result<Value, CompileError> {
    let restriction_name = restriction.trim().trim_start_matches('#').trim();
    let (name, frob_only) = match restriction_name.strip_suffix("<_>") {
        Some(name) => (name.trim(), true),
        None => (restriction_name, false),
    };
    let identity = context
        .identity(name)
        .ok_or_else(|| CompileError::UnknownIdentity {
            node: id,
            span: semantic.span(id).cloned(),
            name: name.to_owned(),
        })?;
    if frob_only {
        Ok(Value::frob(identity, Value::nothing()))
    } else {
        Ok(Value::identity(identity))
    }
}

fn item_id(item: &HirItem) -> NodeId {
    match item {
        HirItem::Expr { id, .. }
        | HirItem::RelationRule { id, .. }
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

fn query_outputs(args: &[HirArg]) -> Vec<QueryBinding> {
    args.iter()
        .enumerate()
        .filter_map(|(position, arg)| match &arg.value {
            HirExpr::QueryVar { name, .. } => Some(QueryBinding {
                name: Symbol::intern(name),
                position: position as u16,
            }),
            _ => None,
        })
        .collect()
}

fn relation_name_for_dot(name: &str) -> String {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_ascii_uppercase().to_string() + chars.as_str()
}

fn internal_bytecode_error(error: RuntimeError) -> CompileError {
    CompileError::Unsupported {
        node: NodeId(0),
        span: None,
        message: format!("internal compiler error: {error:?}"),
    }
}

fn is_static_catch_condition(condition: Option<&HirExpr>) -> bool {
    matches!(
        condition,
        None | Some(HirExpr::Literal {
            value: Literal::ErrorCode(_),
            ..
        })
    )
}

struct ProgramCompiler<'a> {
    semantic: &'a SemanticProgram,
    context: &'a CompileContext,
    instructions: ProgramBuilder,
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
    captures: Vec<BindingId>,
    min_arity: usize,
    max_arity: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FunctionParamInfo {
    id: NodeId,
    binding: BindingId,
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
            instructions: ProgramBuilder::new(),
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
        self.instructions
            .finish(self.next_register as usize)
            .map_err(Into::into)
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
            HirExpr::Frob {
                id,
                delegate,
                value,
            } => self.compile_frob(*id, delegate, value),
            HirExpr::Symbol { name, .. } => {
                let dst = self.alloc_register();
                self.emit(Instruction::Load {
                    dst,
                    value: Value::symbol(Symbol::intern(name)),
                });
                Ok(dst)
            }
            HirExpr::QueryVar { id, .. } => Err(self.unsupported(
                *id,
                "query variables are only valid inside relation queries",
            )),
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
                    let function = self.compile_function(value)?;
                    if function.captures.is_empty() {
                        self.functions.insert(binding, function.clone());
                    }
                    let dst = self.emit_function_value(*id, function)?;
                    self.locals.insert(binding, dst);
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
            HirExpr::One { expr, .. } => self.compile_one(expr),
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
                let function = self.compile_function(expr)?;
                let HirExpr::Function { name, .. } = expr else {
                    unreachable!();
                };
                if let Some(binding) = name {
                    if function.captures.is_empty() {
                        self.functions.insert(*binding, function);
                        let dst = self.alloc_register();
                        self.emit(Instruction::Load {
                            dst,
                            value: Value::nothing(),
                        });
                        Ok(dst)
                    } else {
                        let dst = self.emit_function_value(*id, function)?;
                        self.locals.insert(*binding, dst);
                        Ok(dst)
                    }
                } else {
                    self.emit_function_value(*id, function)
                }
            }
            HirExpr::RoleDispatch { id, selector, args } => {
                self.compile_dispatch(*id, selector, args, None)
            }
            HirExpr::ReceiverDispatch {
                id,
                receiver,
                selector,
                args,
            } => {
                if args.iter().all(|arg| arg.role.is_none()) {
                    self.compile_receiver_positional_dispatch(*id, receiver, selector, args)
                } else {
                    self.compile_dispatch(*id, selector, args, Some(receiver))
                }
            }
            HirExpr::Spawn { id, target, delay } => {
                self.compile_spawn(*id, target, delay.as_deref())
            }
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

    fn compile_frob(
        &mut self,
        id: NodeId,
        delegate: &str,
        value: &HirExpr,
    ) -> Result<Register, CompileError> {
        let delegate =
            self.context
                .identity(delegate)
                .ok_or_else(|| CompileError::UnknownIdentity {
                    node: id,
                    span: self.span(id),
                    name: delegate.to_owned(),
                })?;
        let value = self.compile_expr_for_operand(value)?;
        let dst = self.alloc_register();
        self.emit(Instruction::BuiltinCall {
            dst,
            name: Symbol::intern("frob"),
            args: vec![Operand::Value(Value::identity(delegate)), value],
        });
        Ok(dst)
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
        if let Some(dot) = self.context.dot_relation(name) {
            self.ensure_dot_relation_metadata(id, name, dot.relation)?;
            let key = self.compile_expr_for_operand(base)?;
            let dst = self.alloc_register();
            self.emit(Instruction::ScanValue {
                dst,
                relation: dot.relation,
                key,
            });
            return Ok(dst);
        }
        let Some(relation) = self.conventional_dot_relation(id, name)? else {
            return Err(self.unsupported(id, format!("dot name `{name}` is not declared")));
        };
        let key = self.compile_expr_for_operand(base)?;
        let query = self.alloc_register();
        self.emit(Instruction::ScanBindings {
            dst: query,
            relation,
            bindings: vec![Some(key), None],
            outputs: vec![QueryBinding {
                name: Symbol::intern(name),
                position: 1,
            }],
        });
        let dst = self.alloc_register();
        self.emit(Instruction::One { dst, src: query });
        Ok(dst)
    }

    fn compile_dot_assignment(
        &mut self,
        id: NodeId,
        base: &HirExpr,
        name: &str,
        value: Register,
    ) -> Result<Register, CompileError> {
        let dot = match self.context.dot_relation(name) {
            Some(dot) => dot,
            None => {
                let Some(relation) = self.conventional_dot_relation(id, name)? else {
                    return Err(self.unsupported(id, format!("dot name `{name}` is not declared")));
                };
                DotRelation { relation }
            }
        };
        self.ensure_dot_relation_metadata(id, name, dot.relation)?;
        let key = self.compile_expr_for_operand(base)?;
        self.emit(Instruction::ReplaceFunctional {
            relation: dot.relation,
            values: vec![key, Operand::Register(value)],
        });
        Ok(value)
    }

    fn conventional_dot_relation(
        &self,
        id: NodeId,
        name: &str,
    ) -> Result<Option<RelationId>, CompileError> {
        let Some(relation) = self.context.relation(&relation_name_for_dot(name)) else {
            return Ok(None);
        };
        self.ensure_dot_relation_metadata(id, name, relation)?;
        Ok(Some(relation))
    }

    fn ensure_dot_relation_metadata(
        &self,
        id: NodeId,
        dot_name: &str,
        relation: RelationId,
    ) -> Result<(), CompileError> {
        let Some(metadata) = self.context.relation_metadata(relation) else {
            return Err(self.unsupported(
                id,
                format!("dot name `{dot_name}` has no relation metadata"),
            ));
        };
        let relation_name = metadata.name().name().unwrap_or("<unnamed relation>");
        if metadata.arity() != 2 {
            return Err(self.unsupported(
                id,
                format!(
                    "dot name `{dot_name}` requires a binary relation, but `{}` has arity {}",
                    relation_name,
                    metadata.arity()
                ),
            ));
        }
        if !matches!(
            metadata.conflict_policy(),
            ConflictPolicy::Functional { key_positions } if key_positions.as_slice() == [0]
        ) {
            return Err(self.unsupported(
                id,
                format!(
                    "dot name `{dot_name}` requires `{}` to be functional on position 0",
                    relation_name
                ),
            ));
        }
        Ok(())
    }

    fn compile_function(&self, expr: &HirExpr) -> Result<FunctionInfo, CompileError> {
        let HirExpr::Function {
            id: _,
            name: _,
            params,
            captures,
            body,
            ..
        } = expr
        else {
            return Err(self.unsupported(expr_id(expr), "expected function expression"));
        };
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
        compiler.next_register = (captures.len() + params.len()) as u16;
        for (idx, capture) in captures.iter().enumerate() {
            compiler.locals.insert(*capture, Register(idx as u16));
        }
        for (idx, param) in params.iter().enumerate() {
            compiler
                .locals
                .insert(param.binding, Register((captures.len() + idx) as u16));
        }
        let program = match body {
            HirFunctionBody::Expr(expr) => compiler.compile_function_expr_body(expr)?,
            HirFunctionBody::Block(items) => compiler.compile_items(items)?,
        };
        let param_info = params
            .iter()
            .map(|param| FunctionParamInfo {
                id: param.id,
                binding: param.binding,
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
            captures: captures.clone(),
            min_arity,
            max_arity,
        })
    }

    fn emit_function_value(
        &mut self,
        id: NodeId,
        function: FunctionInfo,
    ) -> Result<Register, CompileError> {
        let min_arity = u16::try_from(function.min_arity)
            .map_err(|_| self.unsupported(id, "function value has too many parameters"))?;
        let max_arity = match function.max_arity {
            Some(max_arity) => {
                let max_arity = u16::try_from(max_arity)
                    .map_err(|_| self.unsupported(id, "function value has too many parameters"))?;
                if max_arity == u16::MAX {
                    return Err(self.unsupported(id, "function value has too many parameters"));
                }
                max_arity
            }
            None => u16::MAX,
        };
        let captures = function
            .captures
            .iter()
            .map(|capture| {
                self.locals
                    .get(capture)
                    .copied()
                    .map(Operand::Register)
                    .ok_or_else(|| CompileError::UnboundLocal {
                        node: id,
                        span: self.span(id),
                        binding: *capture,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let wrapper = self.compile_function_value_wrapper(id, &function)?;
        let dst = self.alloc_register();
        self.emit(Instruction::LoadFunction {
            dst,
            program: Arc::new(wrapper),
            captures,
            min_arity,
            max_arity,
        });
        Ok(dst)
    }

    fn compile_function_value_wrapper(
        &self,
        id: NodeId,
        function: &FunctionInfo,
    ) -> Result<Program, CompileError> {
        let mut compiler = ProgramCompiler::new(self.semantic, self.context);
        compiler.next_register = (function.captures.len() + 1) as u16;
        let actuals = Register(function.captures.len() as u16);
        for (idx, capture) in function.captures.iter().enumerate() {
            compiler.locals.insert(*capture, Register(idx as u16));
        }

        let len = compiler.alloc_register();
        compiler.emit(Instruction::CollectionLen {
            dst: len,
            collection: actuals,
        });

        let mut operands = Vec::with_capacity(function.captures.len() + function.params.len());
        operands.extend(
            (0..function.captures.len()).map(|idx| Operand::Register(Register(idx as u16))),
        );
        let mut position = 0usize;
        for param in &function.params {
            let value = match param.kind {
                LocalKind::Param => {
                    let value = compiler.compile_collection_slot(actuals, position, param.id)?;
                    position += 1;
                    value
                }
                LocalKind::OptionalParam => {
                    let value = compiler.compile_collection_slot_with_optional_default(
                        actuals,
                        len,
                        position,
                        param.id,
                        param.default.as_ref(),
                    )?;
                    position += 1;
                    value
                }
                LocalKind::RestParam => {
                    compiler.compile_collection_rest(actuals, len, position, param.id)?
                }
                _ => {
                    return Err(self
                        .unsupported(id, "unsupported function parameter kind in function value"));
                }
            };
            compiler.locals.insert(param.binding, value);
            operands.push(Operand::Register(value));
        }

        let dst = compiler.alloc_register();
        compiler.emit(Instruction::Call {
            dst,
            program: Arc::clone(&function.program),
            args: operands,
        });
        compiler.emit(Instruction::Return {
            value: Operand::Register(dst),
        });
        let register_count = compiler.next_register as usize;
        compiler
            .instructions
            .finish(register_count)
            .map_err(Into::into)
    }

    fn compile_function_expr_body(mut self, expr: &HirExpr) -> Result<Program, CompileError> {
        let value = self.compile_expr_for_value(expr)?;
        if !self.returned {
            self.emit(Instruction::Return {
                value: Operand::Register(value),
            });
        }
        self.instructions
            .finish(self.next_register as usize)
            .map_err(Into::into)
    }

    fn compile_call(
        &mut self,
        id: NodeId,
        callee: &HirExpr,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        if let HirExpr::ExternalRef { name, .. } = callee {
            return if self.is_compiler_builtin(name) || self.context.is_runtime_function(name) {
                self.compile_builtin_call(id, name, args)
            } else {
                self.compile_positional_dispatch(id, name, args)
            };
        }
        if let HirExpr::LocalRef { binding, .. } = callee
            && let Some(function) = self.functions.get(binding).cloned()
        {
            return self.compile_direct_function_call(id, &function, args);
        }
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(
                self.unsupported(id, "function value calls only support positional arguments")
            );
        }
        let callee = self.compile_expr_for_value(callee)?;
        let dst = self.alloc_register();
        if args.iter().any(|arg| arg.splice) {
            let call_args = self.compile_arg_items(args)?;
            self.emit(Instruction::CallValueDynamic {
                dst,
                callee: Operand::Register(callee),
                args: call_args,
            });
        } else {
            let call_args = args
                .iter()
                .map(|arg| self.compile_arg_operand(arg))
                .collect::<Result<Vec<_>, _>>()?;
            self.emit(Instruction::CallValue {
                dst,
                callee: Operand::Register(callee),
                args: call_args,
            });
        }
        Ok(dst)
    }

    fn is_compiler_builtin(&self, name: &str) -> bool {
        matches!(
            name,
            "commit" | "suspend" | "read" | "mailbox_recv" | "invoke"
        )
    }

    fn compile_direct_function_call(
        &mut self,
        id: NodeId,
        function: &FunctionInfo,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(
                id,
                "direct function calls only support positional arguments",
            ));
        }
        let has_splice = args.iter().any(|arg| arg.splice);
        if !has_splice {
            self.validate_static_function_arity(id, function, args.len())?;
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
            self.compile_bound_function_args(id, function, args)?
        };
        let dst = self.alloc_register();
        self.emit(Instruction::Call {
            dst,
            program: Arc::clone(&function.program),
            args: call_args,
        });
        Ok(dst)
    }

    fn compile_builtin_call(
        &mut self,
        id: NodeId,
        name: &str,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        match name {
            "commit" => return self.compile_commit_call(id, args),
            "suspend" => return self.compile_suspend_call(id, args),
            "read" => return self.compile_read_call(id, args),
            "mailbox_recv" => return self.compile_mailbox_recv_call(id, args),
            "invoke" => return self.compile_dynamic_invoke_call(id, args),
            _ => {}
        }
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "builtin calls only support positional arguments"));
        }
        let dst = self.alloc_register();
        let name = Symbol::intern(name);
        if args.iter().any(|arg| arg.splice) {
            let args = self.compile_arg_items(args)?;
            self.emit(Instruction::BuiltinCallDynamic { dst, name, args });
        } else {
            let args = args
                .iter()
                .map(|arg| self.compile_arg_operand(arg))
                .collect::<Result<Vec<_>, _>>()?;
            self.emit(Instruction::BuiltinCall { dst, name, args });
        }
        Ok(dst)
    }

    fn compile_dynamic_invoke_call(
        &mut self,
        id: NodeId,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        let method_relations = self
            .context
            .method_relations
            .ok_or_else(|| self.unsupported(id, "method relation ids are not configured"))?;
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "invoke does not accept named arguments"));
        }
        if args.iter().any(|arg| arg.splice) {
            let (actuals, len) = self.compile_dynamic_arg_list(args)?;
            self.compile_abort_if_len_lt(id, len, 2, "invoke expects selector and role map")?;
            self.compile_abort_if_len_gt(id, len, 2, "invoke expects selector and role map")?;
            let selector = Operand::Register(self.compile_collection_slot(actuals, 0, id)?);
            let roles = Operand::Register(self.compile_collection_slot(actuals, 1, id)?);
            let dst = self.alloc_register();
            self.emit(Instruction::DynamicDispatch {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                roles,
            });
            return Ok(dst);
        }
        if args.len() != 2 {
            return Err(self.unsupported(id, "invoke expects selector and role map"));
        }
        let selector = self.compile_arg_operand(&args[0])?;
        let roles = self.compile_arg_operand(&args[1])?;
        let dst = self.alloc_register();
        self.emit(Instruction::DynamicDispatch {
            dst,
            relations: method_relations.dispatch,
            program_relation: method_relations.method_program,
            program_bytes: method_relations.program_bytes,
            selector,
            roles,
        });
        Ok(dst)
    }

    fn compile_positional_dispatch(
        &mut self,
        id: NodeId,
        name: &str,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        let method_relations = self
            .context
            .method_relations
            .ok_or_else(|| self.unsupported(id, "method relation ids are not configured"))?;
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(
                id,
                "positional dispatch calls do not accept named arguments",
            ));
        }
        let has_splice = args.iter().any(|arg| arg.splice);
        let dst = self.alloc_register();
        let selector = Operand::Value(Value::symbol(Symbol::intern(name)));
        if has_splice {
            let args = self.compile_arg_items(args)?;
            self.emit(Instruction::PositionalDispatchDynamic {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                args,
            });
        } else {
            let args = args
                .iter()
                .map(|arg| self.compile_arg_operand(arg))
                .collect::<Result<Vec<_>, _>>()?;
            self.emit(Instruction::PositionalDispatch {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                args,
            });
        }
        Ok(dst)
    }

    fn compile_receiver_positional_dispatch(
        &mut self,
        id: NodeId,
        receiver: &HirExpr,
        selector: &HirExpr,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        let method_relations = self
            .context
            .method_relations
            .ok_or_else(|| self.unsupported(id, "method relation ids are not configured"))?;
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(
                id,
                "receiver positional dispatch calls do not accept named arguments",
            ));
        }
        let has_splice = args.iter().any(|arg| arg.splice);
        let selector = self.compile_expr_for_operand(selector)?;
        let receiver = self.compile_expr_for_value(receiver)?;
        let dst = self.alloc_register();
        if has_splice {
            let mut items = Vec::with_capacity(args.len() + 1);
            items.push(ListItem::Value(Operand::Register(receiver)));
            items.extend(self.compile_arg_items(args)?);
            self.emit(Instruction::PositionalDispatchDynamic {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                args: items,
            });
        } else {
            let mut operands = Vec::with_capacity(args.len() + 1);
            operands.push(Operand::Register(receiver));
            for arg in args {
                operands.push(self.compile_arg_operand(arg)?);
            }
            self.emit(Instruction::PositionalDispatch {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                args: operands,
            });
        }
        Ok(dst)
    }

    fn compile_commit_call(
        &mut self,
        id: NodeId,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "commit does not accept named arguments"));
        }
        if args.iter().any(|arg| arg.splice) {
            let (_, len) = self.compile_dynamic_arg_list(args)?;
            self.compile_abort_if_len_gt(id, len, 0, "commit expects no arguments")?;
            let dst = self.alloc_register();
            self.emit(Instruction::CommitValue { dst });
            return Ok(dst);
        }
        if !args.is_empty() {
            return Err(self.unsupported(id, "commit expects no arguments"));
        }
        let dst = self.alloc_register();
        self.emit(Instruction::CommitValue { dst });
        Ok(dst)
    }

    fn compile_suspend_call(
        &mut self,
        id: NodeId,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "suspend only supports positional arguments"));
        }
        if args.iter().any(|arg| arg.splice) {
            let (actuals, len) = self.compile_dynamic_arg_list(args)?;
            self.compile_abort_if_len_gt(id, len, 1, "suspend expects 0 or 1 arguments")?;
            return self.compile_suspend_from_dynamic_args(id, actuals, len);
        }
        if args.len() > 1 {
            return Err(self.unsupported(id, "suspend expects 0 or 1 arguments"));
        }
        let duration = args
            .first()
            .map(|arg| self.compile_arg_operand(arg))
            .transpose()?;
        let dst = self.alloc_register();
        self.emit(Instruction::SuspendValue { dst, duration });
        Ok(dst)
    }

    fn compile_read_call(&mut self, id: NodeId, args: &[HirArg]) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "read only supports positional arguments"));
        }
        if args.iter().any(|arg| arg.splice) {
            let (actuals, len) = self.compile_dynamic_arg_list(args)?;
            self.compile_abort_if_len_gt(id, len, 1, "read expects 0 or 1 arguments")?;
            return self.compile_read_from_dynamic_args(id, actuals, len);
        }
        if args.len() > 1 {
            return Err(self.unsupported(id, "read expects 0 or 1 arguments"));
        }
        let metadata = args
            .first()
            .map(|arg| self.compile_arg_operand(arg))
            .transpose()?;
        let dst = self.alloc_register();
        self.emit(Instruction::Read { dst, metadata });
        Ok(dst)
    }

    fn compile_mailbox_recv_call(
        &mut self,
        id: NodeId,
        args: &[HirArg],
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(id, "mailbox_recv only supports positional arguments"));
        }
        if args.iter().any(|arg| arg.splice) {
            let (actuals, len) = self.compile_dynamic_arg_list(args)?;
            self.compile_abort_if_len_lt(
                id,
                len,
                1,
                "mailbox_recv expects a receive-cap list and optional timeout",
            )?;
            self.compile_abort_if_len_gt(
                id,
                len,
                2,
                "mailbox_recv expects a receive-cap list and optional timeout",
            )?;
            return self.compile_mailbox_recv_from_dynamic_args(id, actuals, len);
        }
        if args.is_empty() || args.len() > 2 {
            return Err(self.unsupported(
                id,
                "mailbox_recv expects a receive-cap list and optional timeout",
            ));
        }
        let receivers = self.compile_arg_operand(&args[0])?;
        let timeout = args
            .get(1)
            .map(|arg| self.compile_arg_operand(arg))
            .transpose()?;
        let dst = self.alloc_register();
        self.emit(Instruction::MailboxRecv {
            dst,
            receivers,
            timeout,
        });
        Ok(dst)
    }

    fn compile_dynamic_arg_list(
        &mut self,
        args: &[HirArg],
    ) -> Result<(Register, Register), CompileError> {
        let items = self.compile_arg_items(args)?;
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
        Ok((actuals, len))
    }

    fn compile_abort_if_len_lt(
        &mut self,
        id: NodeId,
        len: Register,
        min: usize,
        message: &str,
    ) -> Result<(), CompileError> {
        let min = self.load_usize(min, id)?;
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Lt,
            left: len,
            right: min,
        });
        self.compile_abort_if(condition, message)
    }

    fn compile_abort_if_len_gt(
        &mut self,
        id: NodeId,
        len: Register,
        max: usize,
        message: &str,
    ) -> Result<(), CompileError> {
        let max = self.load_usize(max, id)?;
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Gt,
            left: len,
            right: max,
        });
        self.compile_abort_if(condition, message)
    }

    fn compile_abort_if(&mut self, condition: Register, message: &str) -> Result<(), CompileError> {
        let branch = self.emit_branch(condition, 0, 0);
        let abort_target = self.instructions.len();
        self.emit(Instruction::Abort {
            error: Operand::Value(Value::string(message)),
        });
        let continue_target = self.instructions.len();
        self.patch_branch(branch, abort_target, continue_target)
    }

    fn compile_len_gt_condition(
        &mut self,
        id: NodeId,
        len: Register,
        position: usize,
    ) -> Result<Register, CompileError> {
        let position = self.load_usize(position, id)?;
        let condition = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: condition,
            op: RuntimeBinaryOp::Gt,
            left: len,
            right: position,
        });
        Ok(condition)
    }

    fn compile_suspend_from_dynamic_args(
        &mut self,
        id: NodeId,
        actuals: Register,
        len: Register,
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        let condition = self.compile_len_gt_condition(id, len, 0)?;
        let branch = self.emit_branch(condition, 0, 0);

        let duration_target = self.instructions.len();
        let duration = self.compile_collection_slot(actuals, 0, id)?;
        self.emit(Instruction::SuspendValue {
            dst,
            duration: Some(Operand::Register(duration)),
        });
        let duration_jump = self.emit_jump(0);

        let no_duration_target = self.instructions.len();
        self.emit(Instruction::SuspendValue {
            dst,
            duration: None,
        });

        let end = self.instructions.len();
        self.patch_branch(branch, duration_target, no_duration_target)?;
        self.patch_jump(duration_jump, end)?;
        Ok(dst)
    }

    fn compile_read_from_dynamic_args(
        &mut self,
        id: NodeId,
        actuals: Register,
        len: Register,
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        let condition = self.compile_len_gt_condition(id, len, 0)?;
        let branch = self.emit_branch(condition, 0, 0);

        let metadata_target = self.instructions.len();
        let metadata = self.compile_collection_slot(actuals, 0, id)?;
        self.emit(Instruction::Read {
            dst,
            metadata: Some(Operand::Register(metadata)),
        });
        let metadata_jump = self.emit_jump(0);

        let no_metadata_target = self.instructions.len();
        self.emit(Instruction::Read {
            dst,
            metadata: None,
        });

        let end = self.instructions.len();
        self.patch_branch(branch, metadata_target, no_metadata_target)?;
        self.patch_jump(metadata_jump, end)?;
        Ok(dst)
    }

    fn compile_mailbox_recv_from_dynamic_args(
        &mut self,
        id: NodeId,
        actuals: Register,
        len: Register,
    ) -> Result<Register, CompileError> {
        let receivers = self.compile_collection_slot(actuals, 0, id)?;
        let dst = self.alloc_register();
        let condition = self.compile_len_gt_condition(id, len, 1)?;
        let branch = self.emit_branch(condition, 0, 0);

        let timeout_target = self.instructions.len();
        let timeout = self.compile_collection_slot(actuals, 1, id)?;
        self.emit(Instruction::MailboxRecv {
            dst,
            receivers: Operand::Register(receivers),
            timeout: Some(Operand::Register(timeout)),
        });
        let timeout_jump = self.emit_jump(0);

        let no_timeout_target = self.instructions.len();
        self.emit(Instruction::MailboxRecv {
            dst,
            receivers: Operand::Register(receivers),
            timeout: None,
        });

        let end = self.instructions.len();
        self.patch_branch(branch, timeout_target, no_timeout_target)?;
        self.patch_jump(timeout_jump, end)?;
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
        let items = self.compile_arg_items(args)?;
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

    fn compile_arg_items(&mut self, args: &[HirArg]) -> Result<Vec<ListItem>, CompileError> {
        args.iter()
            .map(|arg| {
                let operand = self.compile_arg_operand(arg)?;
                Ok(if arg.splice {
                    ListItem::Splice(operand)
                } else {
                    ListItem::Value(operand)
                })
            })
            .collect()
    }

    fn compile_role_map_items(
        &mut self,
        receiver: Option<Operand>,
        args: &[HirArg],
    ) -> Result<Vec<MapItem>, CompileError> {
        let mut items = Vec::with_capacity(args.len() + usize::from(receiver.is_some()));
        if let Some(receiver) = receiver {
            items.push(MapItem::Entry(
                Operand::Value(Value::symbol(Symbol::intern("receiver"))),
                receiver,
            ));
        }
        for arg in args {
            if let Some(role) = &arg.role {
                if arg.splice {
                    return Err(self.unsupported(
                        arg.id,
                        "role-named argument values do not support splices; splice a role map",
                    ));
                }
                items.push(MapItem::Entry(
                    Operand::Value(Value::symbol(Symbol::intern(role))),
                    self.compile_arg_operand(arg)?,
                ));
            } else if arg.splice {
                items.push(MapItem::Splice(self.compile_arg_operand(arg)?));
            } else {
                return Err(self.unsupported(
                    arg.id,
                    "role-named dispatch arguments must use explicit role names",
                ));
            }
        }
        Ok(items)
    }

    fn alloc_map(&mut self, items: Vec<MapItem>) -> Register {
        let dst = self.alloc_register();
        self.emit(Instruction::BuildMapDynamic { dst, items });
        dst
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
        let saved_returned = self.returned;
        let right = self.compile_expr_for_value(right)?;
        self.returned = saved_returned;
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
        let saved_returned = self.returned;
        let right = self.compile_expr_for_value(right)?;
        self.returned = saved_returned;
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

    fn compile_one(&mut self, expr: &HirExpr) -> Result<Register, CompileError> {
        let src = self.compile_expr_for_value(expr)?;
        let dst = self.alloc_register();
        self.emit(Instruction::One { dst, src });
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

        let has_dynamic_catch = catches
            .iter()
            .any(|catch| !is_static_catch_condition(catch.condition.as_ref()));
        let mut handlers = if has_dynamic_catch {
            let error = self.alloc_register();
            self.emit(Instruction::Load {
                dst: error,
                value: Value::nothing(),
            });
            vec![CatchHandler {
                code: None,
                binding: Some(error),
                target: 0,
            }]
        } else {
            catches
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
                .collect::<Result<Vec<_>, CompileError>>()?
        };

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
        if has_dynamic_catch {
            let error = handlers[0]
                .binding
                .expect("dynamic catch handler binds error");
            handlers[0].target = self.instructions.len();
            self.compile_try_catch_dispatcher(dst, error, catches, finally, &mut end_jumps)?;
        } else {
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

    fn compile_try_catch_dispatcher(
        &mut self,
        dst: Register,
        error: Register,
        catches: &[HirCatch],
        finally: &[HirItem],
        end_jumps: &mut Vec<usize>,
    ) -> Result<(), CompileError> {
        for catch in catches {
            if let Some(binding) = catch.binding {
                self.locals.insert(binding, error);
            }
            let branch = self
                .compile_catch_match(error, catch.condition.as_ref())?
                .map(|condition| self.emit_branch(condition, 0, 0));
            let body_target = self.instructions.len();
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
            let next_target = self.instructions.len();
            if let Some(branch) = branch {
                self.patch_branch(branch, body_target, next_target)?;
            } else {
                return Ok(());
            }
        }
        self.emit(Instruction::Raise {
            error: Operand::Register(error),
            message: None,
            value: None,
        });
        Ok(())
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

        let has_dynamic_catch = catches
            .iter()
            .any(|catch| !is_static_catch_condition(catch.condition.as_ref()));
        let mut handlers = if has_dynamic_catch {
            let error = self.alloc_register();
            self.emit(Instruction::Load {
                dst: error,
                value: Value::nothing(),
            });
            vec![CatchHandler {
                code: None,
                binding: Some(error),
                target: 0,
            }]
        } else {
            catches
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
                .collect::<Result<Vec<_>, CompileError>>()?
        };

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
        if has_dynamic_catch {
            let error = handlers[0]
                .binding
                .expect("dynamic recover handler binds error");
            handlers[0].target = self.instructions.len();
            self.compile_recover_catch_dispatcher(dst, error, catches, &mut end_jumps)?;
        } else {
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

    fn compile_recover_catch_dispatcher(
        &mut self,
        dst: Register,
        error: Register,
        catches: &[HirRecovery],
        end_jumps: &mut Vec<usize>,
    ) -> Result<(), CompileError> {
        for catch in catches {
            if let Some(binding) = catch.binding {
                self.locals.insert(binding, error);
            }
            let branch = self
                .compile_catch_match(error, catch.condition.as_ref())?
                .map(|condition| self.emit_branch(condition, 0, 0));
            let body_target = self.instructions.len();
            let saved_branch_returned = self.returned;
            self.returned = false;
            let value = self.compile_expr_for_value(&catch.value)?;
            if !self.returned {
                self.emit(Instruction::Move { dst, src: value });
                end_jumps.push(self.emit_jump(0));
            }
            self.returned = saved_branch_returned;
            let next_target = self.instructions.len();
            if let Some(branch) = branch {
                self.patch_branch(branch, body_target, next_target)?;
            } else {
                return Ok(());
            }
        }
        self.emit(Instruction::Raise {
            error: Operand::Register(error),
            message: None,
            value: None,
        });
        Ok(())
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

    fn compile_catch_match(
        &mut self,
        error: Register,
        condition: Option<&HirExpr>,
    ) -> Result<Option<Register>, CompileError> {
        let Some(condition) = condition else {
            return Ok(None);
        };
        match condition {
            HirExpr::Literal {
                value: Literal::ErrorCode(code),
                ..
            } => self
                .compile_error_code_match(error, Symbol::intern(code))
                .map(Some),
            _ => self.compile_expr_for_value(condition).map(Some),
        }
    }

    fn compile_error_code_match(
        &mut self,
        error: Register,
        code: Symbol,
    ) -> Result<Register, CompileError> {
        let actual = self.alloc_register();
        self.emit(Instruction::ErrorField {
            dst: actual,
            error,
            field: ErrorField::Code,
        });
        let expected = self.alloc_register();
        self.emit(Instruction::Load {
            dst: expected,
            value: Value::error_code(code),
        });
        let matched = self.alloc_register();
        self.emit(Instruction::Binary {
            dst: matched,
            op: RuntimeBinaryOp::Eq,
            left: actual,
            right: expected,
        });
        Ok(matched)
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
        let selector = self.compile_expr_for_operand(selector)?;
        if args.iter().any(|arg| arg.splice) {
            let receiver = receiver
                .map(|receiver| self.compile_expr_for_value(receiver).map(Operand::Register))
                .transpose()?;
            let roles = self.compile_role_map_items(receiver, args)?;
            let roles = self.alloc_map(roles);
            let dst = self.alloc_register();
            self.emit(Instruction::DynamicDispatch {
                dst,
                relations: method_relations.dispatch,
                program_relation: method_relations.method_program,
                program_bytes: method_relations.program_bytes,
                selector,
                roles: Operand::Register(roles),
            });
            return Ok(dst);
        }
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

    fn compile_spawn(
        &mut self,
        id: NodeId,
        target: &HirExpr,
        delay: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        let (selector, args, receiver) = match target {
            HirExpr::RoleDispatch { selector, args, .. } => (selector.as_ref(), args, None),
            HirExpr::ReceiverDispatch {
                receiver,
                selector,
                args,
                ..
            } => (selector.as_ref(), args, Some(receiver.as_ref())),
            _ => {
                return Err(
                    self.unsupported(id, "spawn expects a role or receiver dispatch target")
                );
            }
        };
        if let Some(receiver) = receiver
            && args.iter().all(|arg| arg.role.is_none())
        {
            return self.compile_receiver_positional_spawn(id, receiver, selector, args, delay);
        }
        if receiver.is_none() && args.iter().all(|arg| arg.role.is_none()) {
            return self.compile_positional_spawn(id, selector, args, delay);
        }
        let selector = self.compile_expr_for_operand(selector)?;
        if args.iter().any(|arg| arg.splice) {
            let receiver = receiver
                .map(|receiver| self.compile_expr_for_value(receiver).map(Operand::Register))
                .transpose()?;
            let roles = self.compile_role_map_items(receiver, args)?;
            let roles = self.alloc_map(roles);
            let delay = delay
                .map(|delay| self.compile_expr_for_operand(delay))
                .transpose()?;
            let dst = self.alloc_register();
            self.emit(Instruction::SpawnDispatchDynamic {
                dst,
                selector,
                roles: Operand::Register(roles),
                delay,
            });
            return Ok(dst);
        }
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
                    self.unsupported(arg.id, "spawn arguments must use explicit role names")
                );
            };
            roles.push((
                Value::symbol(Symbol::intern(role)),
                self.compile_arg_operand(arg)?,
            ));
        }
        let delay = delay
            .map(|delay| self.compile_expr_for_operand(delay))
            .transpose()?;
        let dst = self.alloc_register();
        self.emit(Instruction::SpawnDispatch {
            dst,
            selector,
            roles,
            delay,
        });
        Ok(dst)
    }

    fn compile_positional_spawn(
        &mut self,
        id: NodeId,
        selector: &HirExpr,
        args: &[HirArg],
        delay: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(
                self.unsupported(id, "positional spawn calls do not accept named arguments")
            );
        }
        let selector = self.compile_expr_for_operand(selector)?;
        let delay = delay
            .map(|delay| self.compile_expr_for_operand(delay))
            .transpose()?;
        let dst = self.alloc_register();
        if args.iter().any(|arg| arg.splice) {
            let args = self.compile_arg_items(args)?;
            self.emit(Instruction::SpawnPositionalDispatchDynamic {
                dst,
                selector,
                args,
                delay,
            });
        } else {
            let args = args
                .iter()
                .map(|arg| self.compile_arg_operand(arg))
                .collect::<Result<Vec<_>, _>>()?;
            self.emit(Instruction::SpawnPositionalDispatch {
                dst,
                selector,
                args,
                delay,
            });
        }
        Ok(dst)
    }

    fn compile_receiver_positional_spawn(
        &mut self,
        id: NodeId,
        receiver: &HirExpr,
        selector: &HirExpr,
        args: &[HirArg],
        delay: Option<&HirExpr>,
    ) -> Result<Register, CompileError> {
        if args.iter().any(|arg| arg.role.is_some()) {
            return Err(self.unsupported(
                id,
                "receiver positional spawn calls do not accept named arguments",
            ));
        }
        let selector = self.compile_expr_for_operand(selector)?;
        let receiver = self.compile_expr_for_value(receiver)?;
        let delay = delay
            .map(|delay| self.compile_expr_for_operand(delay))
            .transpose()?;
        let dst = self.alloc_register();
        if args.iter().any(|arg| arg.splice) {
            let mut items = Vec::with_capacity(args.len() + 1);
            items.push(ListItem::Value(Operand::Register(receiver)));
            items.extend(self.compile_arg_items(args)?);
            self.emit(Instruction::SpawnPositionalDispatchDynamic {
                dst,
                selector,
                args: items,
                delay,
            });
        } else {
            let mut operands = Vec::with_capacity(args.len() + 1);
            operands.push(Operand::Register(receiver));
            for arg in args {
                operands.push(self.compile_arg_operand(arg)?);
            }
            self.emit(Instruction::SpawnPositionalDispatch {
                dst,
                selector,
                args: operands,
                delay,
            });
        }
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
        if atom.args.iter().any(|arg| arg.splice) {
            let dst = self.alloc_register();
            let args = self.compile_relation_arg_items(&atom.args)?;
            self.emit(Instruction::ScanDynamic {
                dst,
                relation,
                args,
            });
            return Ok(dst);
        }
        let outputs = query_outputs(&atom.args);
        if !outputs.is_empty() {
            return self.compile_relation_query(relation, atom, outputs);
        }
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

    fn compile_relation_query(
        &mut self,
        relation: RelationId,
        atom: &HirRelationAtom,
        outputs: Vec<QueryBinding>,
    ) -> Result<Register, CompileError> {
        let dst = self.alloc_register();
        let bindings = atom
            .args
            .iter()
            .map(|arg| match &arg.value {
                HirExpr::QueryVar { .. } | HirExpr::Hole { .. } => Ok(None),
                _ => self.compile_arg_operand(arg).map(Some),
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.emit(Instruction::ScanBindings {
            dst,
            relation,
            bindings,
            outputs,
        });
        Ok(dst)
    }

    fn compile_fact_change(
        &mut self,
        kind: &EffectKind,
        atom: &HirRelationAtom,
    ) -> Result<(), CompileError> {
        let relation = self.relation_id(atom)?;
        if atom.args.iter().any(|arg| arg.splice) {
            let args = self.compile_relation_arg_items(&atom.args)?;
            match kind {
                EffectKind::Assert => self.emit(Instruction::AssertDynamic { relation, args }),
                EffectKind::Retract => self.emit(Instruction::RetractDynamic { relation, args }),
                EffectKind::Require => {
                    return Err(
                        self.unsupported(atom.id, "require is not a fact change instruction")
                    );
                }
            }
            return Ok(());
        }
        match kind {
            EffectKind::Assert => {
                let values = atom
                    .args
                    .iter()
                    .map(|arg| self.compile_arg_operand(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                self.emit(Instruction::Assert { relation, values });
            }
            EffectKind::Retract => {
                if atom
                    .args
                    .iter()
                    .any(|arg| matches!(arg.value, HirExpr::QueryVar { .. } | HirExpr::Hole { .. }))
                {
                    let bindings = atom
                        .args
                        .iter()
                        .map(|arg| match &arg.value {
                            HirExpr::QueryVar { .. } | HirExpr::Hole { .. } => Ok(None),
                            _ => self.compile_arg_operand(arg).map(Some),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    self.emit(Instruction::RetractWhere { relation, bindings });
                } else {
                    let values = atom
                        .args
                        .iter()
                        .map(|arg| self.compile_arg_operand(arg))
                        .collect::<Result<Vec<_>, _>>()?;
                    self.emit(Instruction::Retract { relation, values });
                }
            }
            EffectKind::Require => {
                return Err(self.unsupported(atom.id, "require is not a fact change instruction"));
            }
        }
        Ok(())
    }

    fn compile_arg_operand(&mut self, arg: &HirArg) -> Result<Operand, CompileError> {
        Ok(Operand::Register(self.compile_expr_for_value(&arg.value)?))
    }

    fn compile_relation_arg_items(
        &mut self,
        args: &[HirArg],
    ) -> Result<Vec<RelationArg>, CompileError> {
        args.iter()
            .map(|arg| {
                if arg.splice {
                    return self.compile_arg_operand(arg).map(RelationArg::Splice);
                }
                Ok(match &arg.value {
                    HirExpr::QueryVar { name, .. } => RelationArg::Query(Symbol::intern(name)),
                    HirExpr::Hole { .. } => RelationArg::Hole,
                    _ => RelationArg::Value(self.compile_arg_operand(arg)?),
                })
            })
            .collect()
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
        self.instructions
            .emit(instruction)
            .expect("compiler emitted bytecode within compact table limits");
    }

    fn emit_branch(&mut self, condition: Register, if_true: usize, if_false: usize) -> usize {
        self.instructions
            .emit_branch(condition, if_true, if_false)
            .expect("compiler emitted bytecode within compact table limits")
    }

    fn emit_jump(&mut self, target: usize) -> usize {
        self.instructions
            .emit_jump(target)
            .expect("compiler emitted bytecode within compact table limits")
    }

    fn patch_branch(
        &mut self,
        index: usize,
        if_true: usize,
        if_false: usize,
    ) -> Result<(), CompileError> {
        self.instructions
            .patch_branch(index, if_true, if_false)
            .map_err(internal_bytecode_error)
    }

    fn patch_true_target(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        self.instructions
            .patch_true_target(index, target)
            .map_err(internal_bytecode_error)
    }

    fn patch_false_target(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        self.instructions
            .patch_false_target(index, target)
            .map_err(internal_bytecode_error)
    }

    fn patch_jump(&mut self, index: usize, target: usize) -> Result<(), CompileError> {
        self.instructions
            .patch_jump(index, target)
            .map_err(internal_bytecode_error)
    }

    fn patch_enter_try(
        &mut self,
        index: usize,
        new_catches: Vec<CatchHandler>,
        new_finally: Option<usize>,
        new_end: usize,
    ) -> Result<(), CompileError> {
        self.instructions
            .patch_enter_try(index, new_catches, new_finally, new_end)
            .map_err(internal_bytecode_error)
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
        | HirExpr::Frob { id, .. }
        | HirExpr::Symbol { id, .. }
        | HirExpr::QueryVar { id, .. }
        | HirExpr::Hole { id }
        | HirExpr::List { id, .. }
        | HirExpr::Map { id, .. }
        | HirExpr::Unary { id, .. }
        | HirExpr::Binary { id, .. }
        | HirExpr::Assign { id, .. }
        | HirExpr::Call { id, .. }
        | HirExpr::RoleDispatch { id, .. }
        | HirExpr::ReceiverDispatch { id, .. }
        | HirExpr::Spawn { id, .. }
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
        | HirExpr::One { id, .. }
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
    use mica_runtime::{
        BuiltinContext, BuiltinRegistry, Emission, RuntimeError, SuspendKind, TaskManager,
        TaskOutcome,
    };
    use std::sync::Arc;

    fn id(raw: u64) -> Identity {
        Identity::new(raw).unwrap()
    }

    fn emitted(value: Value) -> Emission {
        Emission::new(id(99), value)
    }

    #[derive(Debug)]
    struct SubmittedSourceTask {
        outcome: TaskOutcome,
    }

    fn submit_source_task(
        source: &str,
        context: &CompileContext,
        task_manager: &mut TaskManager,
    ) -> Result<SubmittedSourceTask, mica_runtime::TaskManagerError> {
        let compiled = compile_source(source, context).unwrap();
        let (_, outcome) = task_manager.submit(Arc::new(compiled.program.clone()))?;
        Ok(SubmittedSourceTask { outcome })
    }

    fn emit_first_arg(
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        let target = args[0]
            .as_identity()
            .ok_or_else(|| RuntimeError::InvalidEffectTarget(args[0].clone()))?;
        let value = args[1].clone();
        context.emit(target, value.clone())?;
        Ok(value)
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
                RelationMetadata::new(relations.dispatch.param, Symbol::intern("Param"), 4)
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               raise E_NOT_PORTABLE, \"That cannot be taken.\", :lamp\n\
             catch err if E_NOT_PORTABLE\n\
               return err\n\
             end",
            &context,
            &mut task_manager,
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
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_supports_code_first_catch_binding_and_error_fields() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               raise E_NOT_PORTABLE, \"That cannot be taken.\", :lamp\n\
             catch E_NOT_PORTABLE as err\n\
               return (err.code == E_NOT_PORTABLE) and (err.message == \"That cannot be taken.\") and (err.value == :lamp)\n\
             end",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_conditional_try_catches_in_order() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let handled = try\n\
               raise E_NO_EXIT, \"No exit.\"\n\
             catch err if err.code == E_PERMISSION\n\
               false\n\
             catch err if err.code == E_NO_EXIT\n\
               err.message == \"No exit.\"\n\
             end\n\
             return handled",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_recover_reraises_when_no_conditional_catch_matches() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let handled = recover raise E_NO_EXIT\n\
             catch err if err.code == E_PERMISSION => 1\n\
             end\n\
             return handled",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Aborted { .. }));
    }

    #[test]
    fn compiled_task_runs_finally_during_return_unwind() {
        let cleaned = id(1);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(cleaned, Symbol::intern("Cleaned"), 1))
            .unwrap();
        let context = CompileContext::new().with_relation("Cleaned", cleaned);
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "try\n\
               return 7\n\
             finally\n\
               assert Cleaned(:done)\n\
             end",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(7).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert_eq!(
            task_manager
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let fallback = recover raise E_NOT_PORTABLE\n\
             catch E_NOT_PORTABLE => 10\n\
             end\n\
             let untouched = recover 1\n\
             catch E_NOT_PORTABLE => 99\n\
             end\n\
             return fallback + untouched",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(11).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_supports_code_first_recover_binding() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let code = recover raise E_NO_EXIT, \"No exit.\"\n\
             catch E_NO_EXIT as err => err.code\n\
             end\n\
             return code == E_NO_EXIT",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiles_source_to_transactional_task_manager() {
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let actor = #alice\n\
             assert LocatedIn(actor, #room)\n\
             require LocatedIn(actor, #room)\n\
             return true",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        let tuples = task_manager
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
    fn retract_with_hole_removes_matching_tuples() {
        let located_in = id(1);
        let alice = id(10);
        let room = id(11);
        let hallway = id(12);
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                located_in,
                Symbol::intern("LocatedIn"),
                2,
            ))
            .unwrap();
        let mut seed = kernel.begin();
        seed.assert(
            located_in,
            Tuple::from([Value::identity(alice), Value::identity(room)]),
        )
        .unwrap();
        seed.assert(
            located_in,
            Tuple::from([Value::identity(hallway), Value::identity(room)]),
        )
        .unwrap();
        seed.commit().unwrap();
        let context = CompileContext::new()
            .with_relation("LocatedIn", located_in)
            .with_identity("alice", alice)
            .with_identity("room", room)
            .with_identity("hallway", hallway);
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "retract LocatedIn(#alice, _)\n\
             return true",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
                .kernel()
                .snapshot()
                .scan(located_in, &[Some(Value::identity(alice)), None])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            task_manager
                .kernel()
                .snapshot()
                .scan(located_in, &[Some(Value::identity(hallway)), None])
                .unwrap(),
            vec![Tuple::from([
                Value::identity(hallway),
                Value::identity(room)
            ])]
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "assert LocatedIn(#alice, #room)\n\
             require Visible(#alice, #room)\n\
             return true",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Aborted { .. }));
        let tuples = task_manager
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let values = [10, 20, 30]\n\
             let labels = {:answer -> values[1]}\n\
             return labels[:answer]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(20).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_slices_lists_with_ranges() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let values = [0, 1, 2, 3, 4]\n\
             let mid = values[1..3]\n\
             let tail = values[2.._]\n\
             return mid[0] + mid[1] + mid[2] + tail[0] + tail[1] + tail[2]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(15).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_list_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let rest = [2, 3]\n\
             let values = [1, @rest, 4]\n\
             return values[0] + values[1] + values[2] + values[3]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_scatter_assignment_with_required_optional_and_rest_parts() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let [head, ?middle = 10, @tail] = [1, 2, 3, 4]\n\
             return head + middle + tail[0] + tail[1]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_uses_scatter_optional_defaults_and_empty_rest() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let [head, ?middle = 10, @tail] = [1]\n\
             return (head == 1) and (middle == 10) and (tail[0] == nothing)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_list_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let values = [10, 20, 30]\n\
             values[1] = 99\n\
             return values[1]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(99).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_map_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let counts = {:a -> 1}\n\
             counts[:a] = 2\n\
             counts[:b] = 3\n\
             return counts[:a] + counts[:b]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(5).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_assigns_indexed_values_inside_loop() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let values = [1, 2, 3]\n\
             for index, item in values\n\
               values[index] = item * 10\n\
             end\n\
             return values[0] + values[1] + values[2]",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(60).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_reads_and_writes_declared_dot_relations() {
        let name = id(1);
        let lamp = id(10);
        let kernel = RelationKernel::new();
        let name_metadata = RelationMetadata::new(name, Symbol::intern("Name"), 2)
            .with_conflict_policy(ConflictPolicy::Functional {
                key_positions: vec![0],
            });
        kernel.create_relation(name_metadata.clone()).unwrap();
        let context = CompileContext::new()
            .with_relation_metadata(name_metadata)
            .with_dot_relation("name", name)
            .with_identity("lamp", lamp);
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "#lamp.name = \"brass lamp\"\n\
             return #lamp.name",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("brass lamp"),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert_eq!(
            task_manager
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
        let error = compile_source("#lamp.color", &context).unwrap_err();

        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. } if message == "dot name `color` is not declared"
        ));
    }

    #[test]
    fn compiled_task_expands_relation_argument_splices() {
        let located_in = id(1);
        let coin = id(10);
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
            .with_identity("coin", coin)
            .with_identity("room", room);
        let mut task_manager = TaskManager::new(kernel);

        let submitted = submit_source_task(
            "let pair = [#coin, #room]\n\
             assert LocatedIn(@pair)\n\
             let prefix = [#coin]\n\
             let place = one LocatedIn(@prefix, ?place)\n\
             retract LocatedIn(@prefix, ?old_place)\n\
             return place == #room && not LocatedIn(@pair)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn dot_relations_require_binary_functional_metadata() {
        let tag = id(1);
        let name = id(2);
        let lamp = id(10);
        let context = CompileContext::new()
            .with_relation_metadata(RelationMetadata::new(tag, Symbol::intern("Tag"), 2))
            .with_relation_metadata(
                RelationMetadata::new(name, Symbol::intern("Name"), 2).with_conflict_policy(
                    ConflictPolicy::Functional {
                        key_positions: vec![1],
                    },
                ),
            )
            .with_identity("lamp", lamp);

        let error = compile_source("return #lamp.tag", &context).unwrap_err();
        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. }
                if message == "dot name `tag` requires `Tag` to be functional on position 0"
        ));

        let error = compile_source("return #lamp.name", &context).unwrap_err();
        assert!(matches!(
            error,
            CompileError::Unsupported { message, .. }
                if message == "dot name `name` requires `Name` to be functional on position 0"
        ));
    }

    #[test]
    fn compiled_task_runs_scalar_arithmetic() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let a = 20 - 3 * 4\n\
             let b = a / 2\n\
             let c = b % 3\n\
             return -c",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(-1).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_short_circuit_guards_can_return_from_one_path() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);

        let passes = submit_source_task(
            "true || return false\n\
             return true",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            passes.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));

        let returns = submit_source_task(
            "false || return false\n\
             return true",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            returns.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(false)
        ));
    }

    #[test]
    fn compiled_task_calls_registered_runtime_builtin() {
        let context = CompileContext::new()
            .with_identity("target", id(99))
            .with_runtime_function("emit_first_arg");
        let kernel = RelationKernel::new();
        let builtins = BuiltinRegistry::new().with_builtin("emit_first_arg", emit_first_arg);
        let mut task_manager = TaskManager::new(kernel).with_builtins(Arc::new(builtins));
        let submitted = submit_source_task(
            "let value = emit_first_arg(#target, \"hello\")\n\
             return value",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("hello"),
                effects: vec![emitted(Value::string("hello"))],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_runtime_builtin_argument_splices() {
        let context = CompileContext::new()
            .with_identity("target", id(99))
            .with_runtime_function("emit_first_arg");
        let kernel = RelationKernel::new();
        let builtins = BuiltinRegistry::new().with_builtin("emit_first_arg", emit_first_arg);
        let mut task_manager = TaskManager::new(kernel).with_builtins(Arc::new(builtins));
        let submitted = submit_source_task(
            "let args = [#target, \"hello\"]\n\
             let value = emit_first_arg(@args)\n\
             return value",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("hello"),
                effects: vec![emitted(Value::string("hello"))],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_task_control_argument_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);

        let commit = submit_source_task(
            "let args = []\n\
             return commit(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            commit.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Commit,
                ..
            }
        ));

        let suspend = submit_source_task(
            "let args = [0.5]\n\
             return suspend(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            suspend.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::TimedMillis(500),
                ..
            }
        ));

        let suspend_without_duration = submit_source_task(
            "let args = []\n\
             return suspend(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            suspend_without_duration.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Never,
                ..
            }
        ));

        let read = submit_source_task(
            "let args = [:line]\n\
             return read(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            read.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::WaitingForInput(value),
                ..
            } if value == Value::symbol(Symbol::intern("line"))
        ));

        let read_without_metadata = submit_source_task(
            "let args = []\n\
             return read(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            read_without_metadata.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::WaitingForInput(value),
                ..
            } if value == Value::nothing()
        ));
    }

    #[test]
    fn task_control_argument_splices_validate_dynamic_arity() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);

        let commit = submit_source_task(
            "let args = [1]\n\
             return commit(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            commit.outcome,
            TaskOutcome::Aborted { error, .. } if error == Value::string("commit expects no arguments")
        ));

        let read = submit_source_task(
            "let args = [1, 2]\n\
             return read(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            read.outcome,
            TaskOutcome::Aborted { error, .. } if error == Value::string("read expects 0 or 1 arguments")
        ));

        let mailbox_recv = submit_source_task(
            "let args = []\n\
             return mailbox_recv(@args)",
            &context,
            &mut task_manager,
        )
        .unwrap();
        assert!(matches!(
            mailbox_recv.outcome,
            TaskOutcome::Aborted { error, .. }
                if error == Value::string("mailbox_recv expects a receive-cap list and optional timeout")
        ));
    }

    #[test]
    fn compiled_builtin_call_fails_at_runtime_when_unregistered() {
        let context = CompileContext::new().with_runtime_function("missing_builtin");
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let error = submit_source_task("return missing_builtin()", &context, &mut task_manager)
            .unwrap_err();

        assert!(matches!(
            error,
            mica_runtime::TaskManagerError::Task(
                mica_runtime::TaskError::Runtime(RuntimeError::UnknownBuiltin { name })
            ) if name == Symbol::intern("missing_builtin")
        ));
    }

    #[test]
    fn compiled_task_calls_named_local_functions() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "fn double(x)\n\
               return x * 2\n\
             end\n\
             fn add(left, right)\n\
               return left + right\n\
             end\n\
             return add(double(10), 1)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(21).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_let_bound_function_literals() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let triple = fn(x) => x * 3\n\
             return triple(7)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(21).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_passes_function_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let apply = fn(f, x) => f(x)\n\
             return apply(fn(x) => x + 1, 41)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_returns_function_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let make = fn() => fn(x) => x + 1\n\
             let inc = make()\n\
             return inc(41)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_function_values_through_aliases() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let inc = fn(x) => x + 1\n\
             let alias = inc\n\
             return alias(41)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_function_value_call_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let sum = fn(a, b, c) => a + b + c\n\
             let alias = sum\n\
             let rest = [2, 3]\n\
             return alias(1, @rest)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_closure_call_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let base = 10\n\
             let sum = fn(a, b, c) => base + a + b + c\n\
             let rest = [2, 3]\n\
             return sum(1, @rest)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(16).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_function_value_optional_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let pick = fn(value, ?fallback = 10) => value + fallback\n\
             let alias = pick\n\
             return alias(1) + alias(1, 2)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(14).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_function_value_rest_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let sum = fn(first, @rest) => first + rest[0] + rest[1]\n\
             let alias = sum\n\
             return alias(1, 2, 3)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_splices_function_value_rest_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let sum = fn(first, @rest) => first + rest[0] + rest[1]\n\
             let alias = sum\n\
             let rest = [2, 3]\n\
             return alias(1, @rest)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn function_value_default_params_capture_values_when_created() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let fallback = 10\n\
             let add = fn(value, ?extra = fallback) => value + extra\n\
             fallback = 20\n\
             return add(1)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(11).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn function_value_call_splices_require_lists() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let error = submit_source_task(
            "let id = fn(value) => value\n\
             let alias = id\n\
             return alias(@1)",
            &context,
            &mut task_manager,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            mica_runtime::TaskManagerError::Task(mica_runtime::TaskError::Runtime(
                RuntimeError::InvalidArgumentSplice(value)
            )) if value == Value::int(1).unwrap()
        ));
    }

    #[test]
    fn function_value_calls_validate_arity_at_runtime() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let error = submit_source_task(
            "let inc = fn(x) => x + 1\n\
             let alias = inc\n\
             return alias()",
            &context,
            &mut task_manager,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            mica_runtime::TaskManagerError::Task(mica_runtime::TaskError::Runtime(
                RuntimeError::InvalidCallArity {
                    expected_min: 1,
                    expected_max: 1,
                    actual: 0,
                }
            ))
        ));
    }

    #[test]
    fn compiled_task_calls_functions_with_optional_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "fn pick(value, ?fallback = 10)\n\
               return value + fallback\n\
             end\n\
             return pick(1) + pick(1, 2)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(14).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_calls_functions_with_rest_params() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
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
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(16).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_direct_call_argument_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "fn sum3(a, b, c)\n\
               return a + b + c\n\
             end\n\
             let rest = [2, 3]\n\
             return sum3(1, @rest)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_combines_optional_rest_and_call_splices() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "fn total(first, ?second = 10, @rest)\n\
               return first + second + rest[0] + rest[1]\n\
             end\n\
             let extra = [3, 4]\n\
             return total(1, 2, @extra)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
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
    fn compiled_task_calls_closures() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let factor = 2\n\
             fn scale(x)\n\
               return x * factor\n\
             end\n\
             return scale(10)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(20).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_passes_returned_closures() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let make_adder = fn(base) => fn(value) => base + value\n\
             let add10 = make_adder(10)\n\
             return add10(32)",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn closures_capture_values_when_created() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let value = 1\n\
             let read = fn() => value\n\
             value = 2\n\
             return read()",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(1).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_while_loops() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let i = 0\n\
             let total = 0\n\
             while i < 5\n\
               i = i + 1\n\
               total = total + i\n\
             end\n\
             return total",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(15).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_break_and_continue() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
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
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(8).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_list_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for item in [1, 2, 3]\n\
               total = total + item\n\
             end\n\
             return total",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(6).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_list_indexes_and_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for index, item in [4, 5]\n\
               total = total + index + item\n\
             end\n\
             return total",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(10).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_over_map_keys_and_values() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            "let total = 0\n\
             for key, value in {:a -> 10, :b -> 20}\n\
               if key == :b\n\
                 total = total + value\n\
               end\n\
             end\n\
             return total",
            &context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(20).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_runs_for_loop_break_and_continue() {
        let context = CompileContext::new();
        let kernel = RelationKernel::new();
        let mut task_manager = TaskManager::new(kernel);
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
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(8).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
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
            "method #get_thing :get\n\
               roles actor @ #player, item @ #thing\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            ":get(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert_eq!(
            task_manager
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
    fn receiver_dispatch_accepts_positional_arguments() {
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
            "method #inspect_thing :inspect\n\
               roles receiver @ #thing, actor @ #player\n\
             do\n\
               return [receiver, actor]\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task("#coin:inspect(#alice)", &invoke_context, &mut task_manager)
                .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(coin), Value::identity(alice)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }

    #[test]
    fn compiled_task_expands_dispatch_argument_splices() {
        let inspect_method = id(100);
        let inspect_program = id(101);
        let look_method = id(102);
        let look_program = id(103);
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
            .with_identity("look_thing", look_method)
            .with_program_identity("look_thing", look_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method #inspect_thing :inspect\n\
               roles actor @ #player, item @ #thing\n\
             do\n\
               return [actor, item]\n\
             end\n\
             method #look_thing :look\n\
               roles receiver @ #thing, actor @ #player\n\
             do\n\
               return [receiver, actor]\n\
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
        let mut task_manager = TaskManager::new(kernel);

        let positional = submit_source_task(
            "let args = [#alice, #coin]\n\
             return inspect(@args)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            positional.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(alice), Value::identity(coin)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );

        let receiver_positional = submit_source_task(
            "let args = [#alice]\n\
             return #coin:look(@args)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            receiver_positional.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(coin), Value::identity(alice)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );

        let dynamic_invoke = submit_source_task(
            "let args = [:inspect, {:actor -> #alice, :item -> #coin}]\n\
             return invoke(@args)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            dynamic_invoke.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(alice), Value::identity(coin)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );

        let role_named = submit_source_task(
            "let roles = {:item -> #coin}\n\
             return :inspect(actor: #alice, @roles)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            role_named.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(alice), Value::identity(coin)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );

        let receiver_role_named = submit_source_task(
            "let roles = {}\n\
             return #coin:look(actor: #alice, @roles)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            receiver_role_named.outcome,
            TaskOutcome::Complete {
                value: Value::list([Value::identity(coin), Value::identity(alice)]),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
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
            "method #inspect_thing :inspect\n\
               roles actor @ #player, item @ #thing\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            ":inspect(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::identity(coin),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
            "method #count_loop :count\n\
               roles actor @ #player\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task(":count(actor: #alice)", &invoke_context, &mut task_manager)
                .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(3).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
            "method #sum_loop :sum\n\
               roles actor @ #player\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task(":sum(actor: #alice)", &invoke_context, &mut task_manager).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(9).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
            "method #calc :calc\n\
               roles actor @ #player\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task(":calc(actor: #alice)", &invoke_context, &mut task_manager).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(13).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
            "method #update :update\n\
               roles actor @ #player\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task(":update(actor: #alice)", &invoke_context, &mut task_manager)
                .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(2).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
        let name_metadata = RelationMetadata::new(name, Symbol::intern("Name"), 2)
            .with_conflict_policy(ConflictPolicy::Functional {
                key_positions: vec![0],
            });
        kernel.create_relation(name_metadata.clone()).unwrap();

        let method_relations = dispatch_relations();
        let install_context = CompileContext::new()
            .with_relation_metadata(name_metadata)
            .with_dot_relation("name", name)
            .with_method_relations(method_relations)
            .with_identity("rename_thing", rename_method)
            .with_program_identity("rename_thing", rename_program)
            .with_identity("player", player)
            .with_identity("thing", thing);
        let mut install_tx = kernel.begin();
        install_methods_from_source(
            "method #rename_thing :rename\n\
               roles actor @ #player, item @ #thing\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            ":rename(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::string("bright coin"),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert_eq!(
            task_manager
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
            task_manager
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
            "method #calc :calc\n\
               roles actor @ #player\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted =
            submit_source_task(":calc(actor: #alice)", &invoke_context, &mut task_manager).unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::int(42).unwrap(),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
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
            "method #mark_thing :mark\n\
               roles actor @ #player, item @ #thing\n\
             do\n\
               assert LocatedIn(item, actor)\n\
               return true\n\
             end\n\
             method #get_thing :get\n\
               roles actor @ #player, item @ #thing\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let submitted = submit_source_task(
            ":get(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();

        assert_eq!(
            submitted.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert!(
            task_manager
                .resolver()
                .contains(&Value::identity(get_program))
        );
        assert!(
            task_manager
                .resolver()
                .contains(&Value::identity(mark_program))
        );
        assert_eq!(
            task_manager
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
            "method #take_thing :take\n\
               roles actor @ #player, item @ #thing\n\
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
        let mut task_manager = TaskManager::new(kernel);
        let first = submit_source_task(
            ":take(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            first.outcome,
            TaskOutcome::Complete {
                value: Value::bool(true),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
        assert_eq!(
            task_manager
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
            ":take(actor: #alice, item: #coin)",
            &invoke_context,
            &mut task_manager,
        )
        .unwrap();
        assert_eq!(
            second.outcome,
            TaskOutcome::Complete {
                value: Value::bool(false),
                effects: vec![],
                mailbox_sends: Vec::new(),
                retries: 0,
            }
        );
    }
}
