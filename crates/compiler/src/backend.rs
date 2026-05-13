use crate::{
    BindingId, Diagnostic, EffectKind, HirArg, HirExpr, HirItem, HirPlace, HirProgram,
    HirRelationAtom, Literal, NodeId, SemanticProgram, Span, parse_semantic,
};
use mica_relation_kernel::RelationId;
use mica_runtime::{
    Instruction, Operand, Program, Register, Scheduler, SchedulerError, TaskId, TaskOutcome,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompileContext {
    relations: HashMap<String, RelationId>,
    identities: HashMap<String, Identity>,
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

    pub fn define_relation(&mut self, name: impl Into<String>, id: RelationId) {
        self.relations.insert(name.into(), id);
    }

    pub fn define_identity(&mut self, name: impl Into<String>, id: Identity) {
        self.identities.insert(name.into(), id);
    }

    pub fn relation(&self, name: &str) -> Option<RelationId> {
        self.relations.get(name).copied()
    }

    pub fn identity(&self, name: &str) -> Option<Identity> {
        self.identities.get(name).copied()
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

struct ProgramCompiler<'a> {
    semantic: &'a SemanticProgram,
    context: &'a CompileContext,
    instructions: Vec<Instruction>,
    next_register: u16,
    locals: HashMap<BindingId, Register>,
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
            returned: false,
        }
    }

    fn compile_program(mut self, program: &HirProgram) -> Result<Program, CompileError> {
        let mut last_value = None;
        for item in &program.items {
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
            HirExpr::ExternalRef { id, name } => Err(CompileError::Unsupported {
                node: *id,
                span: self.span(*id),
                message: format!(
                    "runtime function `{name}` is not callable from compiled tasks yet"
                ),
            }),
            HirExpr::Error { id } => {
                Err(self.unsupported(*id, "cannot compile erroneous HIR node"))
            }
            _ => Err(self.unsupported(
                expr_id(expr),
                "HIR form is not implemented in the task compiler yet",
            )),
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
}
