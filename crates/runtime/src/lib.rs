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

mod task;
mod task_manager;

#[cfg(test)]
mod vm_tests;

pub use mica_vm::{
    AuthorityContext, Builtin, BuiltinContext, BuiltinRegistry, CapabilityGrant, CapabilityOp,
    CapabilityScope, CatchHandler, Emission, ErrorField, Frame, Instruction, ListItem, Operand,
    Program, ProgramResolver, QueryBinding, Register, RegisterVm, RuntimeBinaryOp, RuntimeContext,
    RuntimeError, RuntimeUnaryOp, SYSTEM_ENDPOINT, SuspendKind, VmHostContext, VmHostResponse,
    VmState,
};
pub use task::{Task, TaskError, TaskId, TaskLimits, TaskOutcome};
pub use task_manager::{
    Effect, EffectLog, SharedTaskManager, SuspendedTask, TaskManager, TaskManagerError,
};

use mica_compiler::{
    CompileContext, CompileError, HirExpr, HirItem, Literal, MethodInstallation, MethodKind,
    MethodRelations, NodeId, compile_semantic, compile_source, install_methods,
    install_rules_from_source, parse, parse_semantic,
};
use mica_relation_kernel::{
    ConflictPolicy, DispatchRelations, FjallDurabilityMode, FjallStateProvider, KernelError,
    RelationKernel, RelationMetadata, RelationRead, Tuple,
};
use mica_var::{Identity, PRIMITIVE_PROTOTYPES, Symbol, Value, ValueKind};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const GENERATED_RELATION_ID_START: u64 = 0x00f0_0000_0000_0000;
const GENERATED_IDENTITY_ID_START: u64 = 0x00e0_0000_0000_0000;
const GENERATED_METHOD_ID_START: u64 = 0x00d1_0000_0000_0000;
const NAMED_IDENTITY_RELATION_ID: u64 = 0x00df_ffff_ffff_ffff;
const METHOD_SELECTOR_RELATION_ID: u64 = 0x00df_ffff_ffff_fffe;
const PARAM_RELATION_ID: u64 = 0x00df_ffff_ffff_fffd;
const DELEGATES_RELATION_ID: u64 = 0x00df_ffff_ffff_fffc;
const METHOD_PROGRAM_RELATION_ID: u64 = 0x00df_ffff_ffff_fffb;
const PROGRAM_BYTES_RELATION_ID: u64 = 0x00df_ffff_ffff_fffa;
const METHOD_SOURCE_RELATION_ID: u64 = 0x00df_ffff_ffff_fff9;
const SOURCE_OWNS_FACT_RELATION_ID: u64 = 0x00df_ffff_ffff_fff8;
const SOURCE_OWNS_RULE_RELATION_ID: u64 = 0x00df_ffff_ffff_fff7;
const SOURCE_OWNS_RELATION_RELATION_ID: u64 = 0x00df_ffff_ffff_fff6;
const ENDPOINT_RELATION_ID: u64 = 0x00df_ffff_ffff_fff5;
const ENDPOINT_ACTOR_RELATION_ID: u64 = 0x00df_ffff_ffff_fff4;
const ENDPOINT_PRINCIPAL_RELATION_ID: u64 = 0x00df_ffff_ffff_fff3;
const ENDPOINT_PROTOCOL_RELATION_ID: u64 = 0x00df_ffff_ffff_fff2;
const ENDPOINT_OPEN_RELATION_ID: u64 = 0x00df_ffff_ffff_fff1;

const DEFAULT_BUILTIN_NAMES: &[&str] = &[
    "emit",
    "commit",
    "suspend",
    "read",
    "make_relation",
    "make_functional_relation",
    "make_identity",
    "rules",
    "describe_rule",
    "disable_rule",
    "fileout",
    "fileout_rules",
    "tasks",
    "actor",
    "principal",
    "endpoint",
    "assume_actor",
    "destroy_identity",
    "assert_transient",
    "retract_transient",
    "drop_transient_scope",
    "string_len",
    "string_chars",
    "string_slice",
    "string_from_chars",
    "lower",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileinMode {
    Add,
    Replace,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileinReport {
    pub reports: Vec<RunReport>,
    pub owned_facts: usize,
    pub owned_rules: usize,
    pub owned_relations: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SourceProjection {
    facts: BTreeSet<(Identity, Tuple)>,
    rules: BTreeSet<Identity>,
    relations: BTreeMap<Identity, RelationMetadata>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SourceDeclarations {
    identities: BTreeSet<String>,
    relations: BTreeSet<(String, u16)>,
}

pub struct SourceRunner {
    context: CompileContext,
    task_manager: TaskManager,
    next_method_identity_id: u64,
}

pub struct SharedSourceRunner {
    context: CompileContext,
    task_manager: SharedTaskManager,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRequest {
    pub principal: Option<Identity>,
    pub actor: Option<Identity>,
    pub endpoint: Identity,
    pub authority: AuthorityContext,
    pub input: TaskInput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskInput {
    Source(String),
    Invocation {
        selector: Symbol,
        roles: Vec<(Symbol, Value)>,
    },
    Continuation {
        task_id: TaskId,
        value: Value,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmittedTask {
    pub task_id: TaskId,
    pub outcome: TaskOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceTaskError {
    Compile(CompileError),
    TaskManager(TaskManagerError),
}

impl From<CompileError> for SourceTaskError {
    fn from(value: CompileError) -> Self {
        Self::Compile(value)
    }
}

impl From<TaskManagerError> for SourceTaskError {
    fn from(value: TaskManagerError) -> Self {
        Self::TaskManager(value)
    }
}

impl SourceRunner {
    pub fn new_empty() -> Self {
        Self::with_kernel(bootstrap_kernel())
    }

    pub fn open_fjall(
        path: impl AsRef<Path>,
        durability: FjallDurabilityMode,
    ) -> Result<Self, String> {
        let provider = Arc::new(FjallStateProvider::open_with_durability(path, durability)?);
        let persisted = provider.load_state()?;
        let kernel = if persisted.version == 0 && persisted.relations.is_empty() {
            bootstrap_kernel_with_provider(provider)
        } else {
            RelationKernel::load_from_state(persisted, provider)
                .map_err(|error| format!("failed to load relation kernel state: {error:?}"))?
        };
        Ok(Self::with_kernel(kernel))
    }

    pub fn with_kernel(kernel: RelationKernel) -> Self {
        let next_method_identity_id = next_generated_method_identity_id(&kernel);
        let mut runner = Self {
            context: CompileContext::new().with_method_relations(method_relations()),
            task_manager: TaskManager::new(kernel).with_builtins(Arc::new(default_builtins())),
            next_method_identity_id,
        };
        runner.refresh_context_from_catalog();
        runner
    }

    pub fn with_task_limits(mut self, limits: TaskLimits) -> Self {
        self.task_manager = self.task_manager.with_limits(limits);
        self
    }

    pub fn into_shared(self) -> SharedSourceRunner {
        SharedSourceRunner {
            context: self.context,
            task_manager: self.task_manager.into_shared(),
        }
    }

    pub fn run_source(&mut self, source: &str) -> Result<RunReport, SourceTaskError> {
        let submitted = self.submit_source(Self::root_source_request(source))?;
        Ok(self.report(submitted.task_id, submitted.outcome))
    }

    pub fn root_source_request(source: impl Into<String>) -> TaskRequest {
        TaskRequest {
            principal: None,
            actor: None,
            endpoint: SYSTEM_ENDPOINT,
            authority: AuthorityContext::root(),
            input: TaskInput::Source(source.into()),
        }
    }

    pub fn source_request_as(
        &self,
        actor: Symbol,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let actor_id = self.actor_identity(actor)?;
        self.source_request_as_identity(actor_id, source)
    }

    pub fn source_request_as_identity(
        &self,
        actor: Identity,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let authority = authority_for_actor(self.task_manager.kernel(), actor)?;
        Ok(TaskRequest {
            principal: None,
            actor: Some(actor),
            endpoint: SYSTEM_ENDPOINT,
            authority,
            input: TaskInput::Source(source.into()),
        })
    }

    pub fn source_request_for_endpoint(
        &self,
        endpoint: Identity,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let runtime_context = self.endpoint_runtime_context(endpoint)?;
        Ok(TaskRequest {
            principal: runtime_context.principal(),
            actor: runtime_context.actor(),
            endpoint,
            authority: authority_for_runtime_context(self.task_manager.kernel(), runtime_context)?,
            input: TaskInput::Source(source.into()),
        })
    }

    pub fn named_identity(&self, name: Symbol) -> Result<Identity, SourceTaskError> {
        identity_named_in_kernel(self.task_manager.kernel(), name)?.ok_or_else(|| {
            unsupported_runner_error(
                NodeId(0),
                None,
                format!("unknown identity :{}", name.name().unwrap_or("<unnamed>")),
            )
        })
    }

    pub fn submit_source(
        &mut self,
        request: TaskRequest,
    ) -> Result<SubmittedTask, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Source(source) = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "submit_source requires source input",
            ));
        };
        let contextual = principal.is_some() || actor.is_some();
        if contextual {
            let semantic = parse_semantic(&source);
            if let Some(item) =
                semantic.hir.items.iter().find(|item| {
                    matches!(item, HirItem::Method { .. } | HirItem::RelationRule { .. })
                })
            {
                return Err(unsupported_runner_error(
                    item_id(item),
                    semantic.span(item_id(item)).cloned(),
                    "contextual source submission cannot install methods or rules",
                ));
            }
            let context = self.context_for_execution(principal, actor, endpoint);
            let compiled = compile_semantic(semantic, &context)?;
            let runtime_context = runtime_context(principal, actor, endpoint);
            let (task_id, outcome) = self.task_manager.submit_with_context(
                Arc::new(compiled.program),
                authority,
                runtime_context,
            )?;
            self.refresh_context_from_catalog();
            return Ok(SubmittedTask { task_id, outcome });
        }

        if let Some(installation) = self.install_methods_from_source(&source)? {
            let value = installed_method_value(&installation);
            let (task_id, outcome) = self.task_manager.complete_immediate(value);
            self.refresh_context_from_catalog();
            return Ok(SubmittedTask { task_id, outcome });
        }

        if let Some(installation) =
            install_rules_from_source(&source, &self.context, self.task_manager.kernel())?
        {
            let value = installed_rule_value(&installation.rules);
            let (task_id, outcome) = self.task_manager.complete_immediate(value);
            self.refresh_context_from_catalog();
            return Ok(SubmittedTask { task_id, outcome });
        }

        let context = self.context_for_execution(principal, actor, endpoint);
        let compiled = compile_source(&source, &context)?;
        let runtime_context = runtime_context(principal, actor, endpoint);
        let (task_id, outcome) = self.task_manager.submit_with_context(
            Arc::new(compiled.program),
            authority,
            runtime_context,
        )?;
        self.refresh_context_from_catalog();
        Ok(SubmittedTask { task_id, outcome })
    }

    pub fn submit_invocation(
        &mut self,
        request: TaskRequest,
    ) -> Result<SubmittedTask, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Invocation { selector, roles } = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "submit_invocation requires invocation input",
            ));
        };
        let program = invocation_program(
            selector,
            invocation_roles(principal, actor, endpoint, roles),
        )
        .map_err(CompileError::from)?;
        let runtime_context = runtime_context(principal, actor, endpoint);
        let (task_id, outcome) =
            self.task_manager
                .submit_with_context(Arc::new(program), authority, runtime_context)?;
        self.refresh_context_from_catalog();
        Ok(SubmittedTask { task_id, outcome })
    }

    pub fn resume_task(&mut self, request: TaskRequest) -> Result<TaskOutcome, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Continuation { task_id, value } = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "resume_task requires continuation input",
            ));
        };
        let runtime_context = runtime_context(principal, actor, endpoint);
        let outcome = self
            .task_manager
            .resume_with_context(task_id, authority, value, runtime_context)
            .map_err(SourceTaskError::from)?;
        self.refresh_context_from_catalog();
        Ok(outcome)
    }

    pub fn submit_source_as(
        &mut self,
        actor: Identity,
        endpoint: Identity,
        source: impl Into<String>,
    ) -> Result<SubmittedTask, SourceTaskError> {
        let authority = authority_for_actor(self.task_manager.kernel(), actor)?;
        self.submit_source(TaskRequest {
            principal: None,
            actor: Some(actor),
            endpoint,
            authority,
            input: TaskInput::Source(source.into()),
        })
    }

    pub fn submit_invocation_as(
        &mut self,
        actor: Identity,
        endpoint: Identity,
        selector: Symbol,
        roles: Vec<(Symbol, Value)>,
    ) -> Result<SubmittedTask, SourceTaskError> {
        let authority = authority_for_actor(self.task_manager.kernel(), actor)?;
        self.submit_invocation(TaskRequest {
            principal: None,
            actor: Some(actor),
            endpoint,
            authority,
            input: TaskInput::Invocation { selector, roles },
        })
    }

    pub fn drain_emissions(&mut self) -> Vec<Effect> {
        self.task_manager.drain_emissions()
    }

    pub fn open_endpoint(
        &mut self,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), SourceTaskError> {
        self.task_manager
            .open_endpoint(endpoint, actor, protocol)
            .map_err(SourceTaskError::from)
    }

    pub fn open_endpoint_with_context(
        &mut self,
        endpoint: Identity,
        principal: Option<Identity>,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), SourceTaskError> {
        self.task_manager
            .open_endpoint_with_context(endpoint, principal, actor, protocol)
            .map_err(SourceTaskError::from)
    }

    pub fn close_endpoint(&mut self, endpoint: Identity) -> usize {
        self.task_manager.close_endpoint(endpoint)
    }

    pub fn route_effect_targets(&self, target: Identity) -> Vec<Identity> {
        self.task_manager.route_effect_targets(target)
    }

    pub fn drain_routed_emissions(&mut self) -> Vec<Effect> {
        self.task_manager.drain_routed_emissions()
    }

    fn endpoint_runtime_context(
        &self,
        endpoint: Identity,
    ) -> Result<RuntimeContext, SourceTaskError> {
        self.task_manager
            .endpoint_runtime_context(endpoint)
            .map_err(SourceTaskError::from)
    }

    pub fn report_outcome(&self, task_id: TaskId, outcome: TaskOutcome) -> RunReport {
        self.report(task_id, outcome)
    }

    fn report(&self, task_id: TaskId, outcome: TaskOutcome) -> RunReport {
        RunReport {
            task_id,
            outcome,
            identity_names: self.identity_names(),
            relation_names: self.relation_names(),
        }
    }

    fn context_for_execution(
        &self,
        principal: Option<Identity>,
        actor: Option<Identity>,
        endpoint: Identity,
    ) -> CompileContext {
        let mut context = self.context.clone();
        if let Some(principal) = principal {
            context.define_identity("principal", principal);
        }
        if let Some(actor) = actor {
            context.define_identity("actor", actor);
        }
        context.define_identity("endpoint", endpoint);
        context
    }

    pub fn run_source_as(
        &mut self,
        actor: Symbol,
        source: &str,
    ) -> Result<RunReport, SourceTaskError> {
        let request = self.source_request_as(actor, source)?;
        let submitted = self.submit_source(request)?;
        Ok(self.report(submitted.task_id, submitted.outcome))
    }

    pub fn resume_as(&mut self, actor: Symbol, task_id: u64) -> Result<RunReport, SourceTaskError> {
        let actor_id = self.actor_identity(actor)?;
        let authority = authority_for_actor(self.task_manager.kernel(), actor_id)?;
        let outcome = self.resume_task(TaskRequest {
            principal: None,
            actor: Some(actor_id),
            endpoint: SYSTEM_ENDPOINT,
            authority,
            input: TaskInput::Continuation {
                task_id,
                value: Value::nothing(),
            },
        })?;
        Ok(self.report(task_id, outcome))
    }

    pub fn run_filein(&mut self, source: &str) -> Result<Vec<RunReport>, SourceTaskError> {
        let mut reports = Vec::new();
        for chunk in source_chunks(source) {
            reports.push(self.run_source(&chunk)?);
        }
        Ok(reports)
    }

    pub fn run_filein_with_unit(
        &mut self,
        unit: Symbol,
        source: &str,
        mode: FileinMode,
    ) -> Result<FileinReport, SourceTaskError> {
        if mode == FileinMode::Replace {
            self.retract_source_unit(unit)?;
        }

        let declarations = collect_source_declarations(source)?;
        let before = self.source_projection()?;
        let reports = self.run_filein(source)?;
        let after = self.source_projection()?;

        let owned_facts = after
            .facts
            .difference(&before.facts)
            .filter(|(relation, _)| is_ownable_fact_relation(*relation))
            .filter(|(relation, _)| *relation != named_identity_relation())
            .cloned()
            .collect::<BTreeSet<_>>();
        let owned_rules = after
            .rules
            .difference(&before.rules)
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut owned_relations = after
            .relations
            .keys()
            .filter(|relation| !before.relations.contains_key(relation))
            .copied()
            .collect::<BTreeSet<_>>();

        for (name, arity) in declarations.relations {
            if let Some((relation, existing_arity)) =
                relation_named(self.task_manager.kernel(), Symbol::intern(&name))
                && existing_arity == arity
            {
                owned_relations.insert(relation);
            }
        }

        let mut tx = self.task_manager.kernel().begin();
        for identity_name in declarations.identities {
            if let Some(identity) = identity_named_in_tx(&tx, Symbol::intern(&identity_name))
                .map_err(CompileError::from)?
            {
                tx.assert(
                    source_owns_fact_relation(),
                    ownership_fact_tuple(
                        unit,
                        named_identity_relation(),
                        Tuple::from([
                            Value::symbol(Symbol::intern(&identity_name)),
                            Value::identity(identity),
                        ]),
                    ),
                )
                .map_err(CompileError::from)?;
            }
        }
        for relation in &owned_relations {
            tx.assert(
                source_owns_relation_relation(),
                Tuple::from([Value::symbol(unit), Value::identity(*relation)]),
            )
            .map_err(CompileError::from)?;
        }
        for (relation, tuple) in &owned_facts {
            tx.assert(
                source_owns_fact_relation(),
                ownership_fact_tuple(unit, *relation, tuple.clone()),
            )
            .map_err(CompileError::from)?;
        }
        for rule in &owned_rules {
            tx.assert(
                source_owns_rule_relation(),
                Tuple::from([Value::symbol(unit), Value::identity(*rule)]),
            )
            .map_err(CompileError::from)?;
        }
        tx.commit().map_err(CompileError::from)?;
        self.refresh_context_from_catalog();

        Ok(FileinReport {
            reports,
            owned_facts: owned_facts.len(),
            owned_rules: owned_rules.len(),
            owned_relations: owned_relations.len(),
        })
    }

    pub fn fileout_unit(&self, unit: Symbol) -> Result<String, SourceTaskError> {
        Ok(fileout_unit_source(self.task_manager.kernel(), unit).map_err(CompileError::from)?)
    }

    fn actor_identity(&self, actor: Symbol) -> Result<Identity, SourceTaskError> {
        self.named_identity(actor).map_err(|_| {
            unsupported_runner_error(
                NodeId(0),
                None,
                format!(
                    "unknown actor identity :{}",
                    actor.name().unwrap_or("<unnamed>")
                ),
            )
        })
    }

    fn install_methods_from_source(
        &mut self,
        source: &str,
    ) -> Result<Option<MethodInstallation>, SourceTaskError> {
        let mut semantic = parse_semantic(source);
        if !semantic
            .hir
            .items
            .iter()
            .any(|item| matches!(item, HirItem::Method { .. }))
        {
            return Ok(None);
        }

        if !semantic.parse_errors.is_empty() {
            return Err(CompileError::ParseErrors {
                count: semantic.parse_errors.len(),
            }
            .into());
        }
        if let Some(diagnostic) = semantic.diagnostics.first() {
            return Err(CompileError::SemanticDiagnostic {
                diagnostic: diagnostic.clone(),
            }
            .into());
        }

        if let Some(item) = semantic
            .hir
            .items
            .iter()
            .find(|item| !matches!(item, HirItem::Method { .. }))
        {
            return Err(CompileError::Unsupported {
                node: item_id(item),
                span: semantic.span(item_id(item)).cloned(),
                message: "method definitions cannot be mixed with executable task code yet"
                    .to_owned(),
            }
            .into());
        }

        let mut install_context = self.context.clone();
        let mut next_method_identity_id = self.next_method_identity_id;
        assign_generated_verb_identities(&mut semantic, next_method_identity_id)?;
        let mut install_tx = self.task_manager.kernel().begin();
        for item in &semantic.hir.items {
            let HirItem::Method { identity, .. } = item else {
                continue;
            };
            let identity_name = identity.as_ref().ok_or_else(|| CompileError::Unsupported {
                node: item_id(item),
                span: semantic.span(item_id(item)).cloned(),
                message: "method installation requires an explicit identity".to_owned(),
            })?;
            let method_id = ensure_named_identity(
                &mut install_tx,
                identity_name,
                &mut next_method_identity_id,
            )?;
            let program_name = format!("{identity_name}_program");
            let program_id = ensure_named_identity(
                &mut install_tx,
                &program_name,
                &mut next_method_identity_id,
            )?;
            install_context.define_identity(identity_name, method_id);
            install_context.define_identity(&program_name, program_id);
            install_context.define_program_identity(identity_name, program_id);
        }

        let installation = install_methods(semantic, &install_context, &mut install_tx)?;
        for method in &installation.methods {
            install_tx
                .assert(
                    method_source_relation(),
                    Tuple::from([method.method.clone(), Value::string(source)]),
                )
                .map_err(CompileError::from)?;
        }
        install_tx.commit().map_err(CompileError::from)?;
        self.context = install_context;
        self.next_method_identity_id = next_method_identity_id;
        Ok(Some(installation))
    }

    fn refresh_context_from_catalog(&mut self) {
        let snapshot = self.task_manager.kernel().snapshot();
        self.context = CompileContext::new().with_method_relations(method_relations());
        for name in DEFAULT_BUILTIN_NAMES {
            self.context.define_runtime_function(*name);
        }
        for metadata in snapshot.relation_metadata() {
            if let Some(name) = metadata.name().name() {
                self.context.define_relation(name, metadata.id());
            }
        }
        for (identity, name) in self.identity_names() {
            self.context.define_identity(name, identity);
        }
        for (index, rule) in snapshot.rules().iter().enumerate() {
            self.context
                .define_identity(format!("rule{}", index + 1), rule.id());
        }
    }

    fn retract_source_unit(&mut self, unit: Symbol) -> Result<(), SourceTaskError> {
        let snapshot = self.task_manager.kernel().snapshot();
        let owned_rules = snapshot
            .scan(
                source_owns_rule_relation(),
                &[Some(Value::symbol(unit)), None],
            )
            .map_err(CompileError::from)?
            .into_iter()
            .filter_map(|tuple| tuple.values().get(1).and_then(Value::as_identity))
            .collect::<Vec<_>>();
        for rule in owned_rules {
            self.task_manager
                .kernel()
                .disable_rule(rule)
                .map_err(CompileError::from)?;
        }

        let mut tx = self.task_manager.kernel().begin();
        for ownership in snapshot
            .scan(
                source_owns_fact_relation(),
                &[Some(Value::symbol(unit)), None, None],
            )
            .map_err(CompileError::from)?
        {
            if let Some((relation, tuple)) = owned_fact_tuple(&ownership)
                && relation != named_identity_relation()
            {
                tx.retract(relation, tuple).map_err(CompileError::from)?;
            }
            tx.retract(source_owns_fact_relation(), ownership)
                .map_err(CompileError::from)?;
        }
        for ownership in snapshot
            .scan(
                source_owns_rule_relation(),
                &[Some(Value::symbol(unit)), None],
            )
            .map_err(CompileError::from)?
        {
            tx.retract(source_owns_rule_relation(), ownership)
                .map_err(CompileError::from)?;
        }
        for ownership in snapshot
            .scan(
                source_owns_relation_relation(),
                &[Some(Value::symbol(unit)), None],
            )
            .map_err(CompileError::from)?
        {
            tx.retract(source_owns_relation_relation(), ownership)
                .map_err(CompileError::from)?;
        }
        tx.commit().map_err(CompileError::from)?;
        self.refresh_context_from_catalog();
        Ok(())
    }

    fn source_projection(&self) -> Result<SourceProjection, SourceTaskError> {
        let snapshot = self.task_manager.kernel().snapshot();
        let facts = snapshot
            .extensional_facts()
            .map_err(CompileError::from)?
            .into_iter()
            .collect();
        let rules = snapshot
            .rules()
            .iter()
            .filter(|rule| rule.active())
            .map(|rule| rule.id())
            .collect();
        let relations = snapshot
            .relation_metadata()
            .map(|metadata| (metadata.id(), metadata.clone()))
            .collect();
        Ok(SourceProjection {
            facts,
            rules,
            relations,
        })
    }

    fn identity_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.task_manager.kernel().snapshot();
        snapshot
            .scan(named_identity_relation(), &[None, None])
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tuple| {
                let [name, identity] = tuple.values() else {
                    return None;
                };
                let name = name.as_symbol()?.name()?.to_owned();
                let identity = identity.as_identity()?;
                Some((identity, name))
            })
            .chain(
                snapshot
                    .rules()
                    .iter()
                    .enumerate()
                    .map(|(index, rule)| (rule.id(), format!("rule{}", index + 1))),
            )
            .collect()
    }

    fn relation_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.task_manager.kernel().snapshot();
        snapshot
            .relation_metadata()
            .filter_map(|metadata| Some((metadata.id(), metadata.name().name()?.to_owned())))
            .collect()
    }
}

impl SharedSourceRunner {
    pub fn named_identity(&self, name: Symbol) -> Result<Identity, SourceTaskError> {
        identity_named_in_kernel(self.task_manager.kernel(), name)?.ok_or_else(|| {
            unsupported_runner_error(
                NodeId(0),
                None,
                format!("unknown identity :{}", name.name().unwrap_or("<unnamed>")),
            )
        })
    }

    pub fn source_request_for_endpoint(
        &self,
        endpoint: Identity,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let runtime_context = self.endpoint_runtime_context(endpoint)?;
        Ok(TaskRequest {
            principal: runtime_context.principal(),
            actor: runtime_context.actor(),
            endpoint,
            authority: authority_for_runtime_context(self.task_manager.kernel(), runtime_context)?,
            input: TaskInput::Source(source.into()),
        })
    }

    pub fn source_request_as(
        &self,
        actor: Symbol,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let actor_id =
            identity_named_in_kernel(self.task_manager.kernel(), actor)?.ok_or_else(|| {
                unsupported_runner_error(
                    NodeId(0),
                    None,
                    format!("unknown actor :{}", actor.name().unwrap_or("<unnamed>")),
                )
            })?;
        self.source_request_as_identity(actor_id, source)
    }

    pub fn source_request_as_identity(
        &self,
        actor: Identity,
        source: impl Into<String>,
    ) -> Result<TaskRequest, SourceTaskError> {
        let authority = authority_for_actor(self.task_manager.kernel(), actor)?;
        Ok(TaskRequest {
            principal: None,
            actor: Some(actor),
            endpoint: SYSTEM_ENDPOINT,
            authority,
            input: TaskInput::Source(source.into()),
        })
    }

    pub fn submit_source(&self, request: TaskRequest) -> Result<SubmittedTask, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Source(source) = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "submit_source requires source input",
            ));
        };
        let contextual = principal.is_some() || actor.is_some() || endpoint != SYSTEM_ENDPOINT;
        if !contextual {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "shared source submission requires endpoint, actor, or principal context",
            ));
        }
        let semantic = parse_semantic(&source);
        if let Some(item) = semantic
            .hir
            .items
            .iter()
            .find(|item| matches!(item, HirItem::Method { .. } | HirItem::RelationRule { .. }))
        {
            return Err(unsupported_runner_error(
                item_id(item),
                semantic.span(item_id(item)).cloned(),
                "contextual source submission cannot install methods or rules",
            ));
        }
        let context = self.context_for_execution(principal, actor, endpoint);
        let compiled = compile_semantic(semantic, &context)?;
        let runtime_context = runtime_context(principal, actor, endpoint);
        let (task_id, outcome) = self.task_manager.submit_with_context(
            Arc::new(compiled.program),
            authority,
            runtime_context,
        )?;
        Ok(SubmittedTask { task_id, outcome })
    }

    pub fn submit_source_report(
        &self,
        endpoint: Identity,
        actor: Option<Symbol>,
        source: String,
    ) -> Result<RunReport, SourceTaskError> {
        let request = match actor {
            Some(actor) => self.source_request_as(actor, source)?,
            None => self.source_request_for_endpoint(endpoint, source)?,
        };
        let submitted = self.submit_source(request)?;
        Ok(self.report(submitted.task_id, submitted.outcome))
    }

    pub fn submit_invocation(
        &self,
        request: TaskRequest,
    ) -> Result<SubmittedTask, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Invocation { selector, roles } = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "submit_invocation requires invocation input",
            ));
        };
        let program = invocation_program(
            selector,
            invocation_roles(principal, actor, endpoint, roles),
        )
        .map_err(CompileError::from)?;
        let runtime_context = runtime_context(principal, actor, endpoint);
        let (task_id, outcome) =
            self.task_manager
                .submit_with_context(Arc::new(program), authority, runtime_context)?;
        Ok(SubmittedTask { task_id, outcome })
    }

    pub fn resume_task(&self, request: TaskRequest) -> Result<TaskOutcome, SourceTaskError> {
        let TaskRequest {
            principal,
            actor,
            endpoint,
            authority,
            input,
        } = request;
        let TaskInput::Continuation { task_id, value } = input else {
            return Err(unsupported_runner_error(
                NodeId(0),
                None,
                "resume_task requires continuation input",
            ));
        };
        let runtime_context = runtime_context(principal, actor, endpoint);
        self.task_manager
            .resume_with_context(task_id, authority, value, runtime_context)
            .map_err(SourceTaskError::from)
    }

    pub fn open_endpoint(
        &self,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), SourceTaskError> {
        self.task_manager
            .open_endpoint(endpoint, actor, protocol)
            .map_err(SourceTaskError::from)
    }

    pub fn open_endpoint_with_context(
        &self,
        endpoint: Identity,
        principal: Option<Identity>,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), SourceTaskError> {
        self.task_manager
            .open_endpoint_with_context(endpoint, principal, actor, protocol)
            .map_err(SourceTaskError::from)
    }

    pub fn close_endpoint(&self, endpoint: Identity) -> usize {
        self.task_manager.close_endpoint(endpoint)
    }

    pub fn drain_emissions(&self) -> Vec<Effect> {
        self.task_manager.drain_emissions()
    }

    pub fn drain_routed_emissions(&self) -> Vec<Effect> {
        self.task_manager.drain_routed_emissions()
    }

    fn endpoint_runtime_context(
        &self,
        endpoint: Identity,
    ) -> Result<RuntimeContext, SourceTaskError> {
        self.task_manager
            .endpoint_runtime_context(endpoint)
            .map_err(SourceTaskError::from)
    }

    pub fn report_outcome(&self, task_id: TaskId, outcome: TaskOutcome) -> RunReport {
        self.report(task_id, outcome)
    }

    pub fn completed_len(&self) -> usize {
        self.task_manager.completed_len()
    }

    pub fn suspended_len(&self) -> usize {
        self.task_manager.suspended_len()
    }

    fn report(&self, task_id: TaskId, outcome: TaskOutcome) -> RunReport {
        RunReport {
            task_id,
            outcome,
            identity_names: self.identity_names(),
            relation_names: self.relation_names(),
        }
    }

    fn context_for_execution(
        &self,
        principal: Option<Identity>,
        actor: Option<Identity>,
        endpoint: Identity,
    ) -> CompileContext {
        let mut context = self.context.clone();
        if let Some(principal) = principal {
            context.define_identity("principal", principal);
        }
        if let Some(actor) = actor {
            context.define_identity("actor", actor);
        }
        context.define_identity("endpoint", endpoint);
        context
    }

    fn identity_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.task_manager.kernel().snapshot();
        snapshot
            .scan_relation(named_identity_relation(), &[None, None])
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tuple| {
                let [name, identity] = tuple.values() else {
                    return None;
                };
                let name = name.as_symbol()?.name()?.to_owned();
                let identity = identity.as_identity()?;
                Some((identity, name))
            })
            .chain(
                snapshot
                    .rules()
                    .iter()
                    .enumerate()
                    .map(|(index, rule)| (rule.id(), format!("rule{}", index + 1))),
            )
            .collect()
    }

    fn relation_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.task_manager.kernel().snapshot();
        snapshot
            .relation_metadata()
            .filter_map(|metadata| Some((metadata.id(), metadata.name().name()?.to_owned())))
            .collect()
    }
}

fn source_chunks(source: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut buffer = String::new();

    for line in source.lines() {
        if line.trim().is_empty() && buffer.trim().is_empty() {
            continue;
        }
        buffer.push_str(line);
        buffer.push('\n');
        if parse(&buffer).errors.is_empty() && source_has_items(&buffer) {
            chunks.push(std::mem::take(&mut buffer));
        }
    }

    if !buffer.trim().is_empty() && source_has_items(&buffer) {
        chunks.push(buffer);
    }
    chunks
}

fn source_has_items(source: &str) -> bool {
    !parse_semantic(source).hir.items.is_empty()
}

fn collect_source_declarations(source: &str) -> Result<SourceDeclarations, SourceTaskError> {
    let mut declarations = SourceDeclarations::default();
    for chunk in source_chunks(source) {
        let semantic = parse_semantic(&chunk);
        if !semantic.parse_errors.is_empty() {
            return Err(CompileError::ParseErrors {
                count: semantic.parse_errors.len(),
            }
            .into());
        }
        if let Some(diagnostic) = semantic.diagnostics.first() {
            return Err(CompileError::SemanticDiagnostic {
                diagnostic: diagnostic.clone(),
            }
            .into());
        }
        for item in semantic.hir.items {
            let HirItem::Expr { expr, .. } = item else {
                continue;
            };
            collect_declaration_expr(&expr, &mut declarations);
        }
    }
    Ok(declarations)
}

fn collect_declaration_expr(expr: &HirExpr, declarations: &mut SourceDeclarations) {
    let HirExpr::Call { callee, args, .. } = expr else {
        return;
    };
    let HirExpr::ExternalRef { name, .. } = callee.as_ref() else {
        return;
    };
    match (name.as_str(), args.as_slice()) {
        ("make_identity", [arg]) => {
            if let HirExpr::Symbol { name, .. } = &arg.value {
                declarations.identities.insert(name.clone());
            }
        }
        ("make_relation", [name_arg, arity_arg])
        | ("make_functional_relation", [name_arg, arity_arg, _]) => {
            let (HirExpr::Symbol { name, .. }, HirExpr::Literal { value, .. }) =
                (&name_arg.value, &arity_arg.value)
            else {
                return;
            };
            let Literal::Int(arity) = value else {
                return;
            };
            if let Ok(arity) = arity.parse::<u16>() {
                declarations.relations.insert((name.clone(), arity));
            }
        }
        _ => {}
    }
}

fn is_exported_fact_relation(relation: Identity) -> bool {
    !matches!(
        relation.raw(),
        NAMED_IDENTITY_RELATION_ID
            | METHOD_SELECTOR_RELATION_ID
            | PARAM_RELATION_ID
            | METHOD_PROGRAM_RELATION_ID
            | PROGRAM_BYTES_RELATION_ID
            | METHOD_SOURCE_RELATION_ID
            | SOURCE_OWNS_FACT_RELATION_ID
            | SOURCE_OWNS_RULE_RELATION_ID
            | SOURCE_OWNS_RELATION_RELATION_ID
    )
}

fn is_ownable_fact_relation(relation: Identity) -> bool {
    !matches!(
        relation.raw(),
        SOURCE_OWNS_FACT_RELATION_ID
            | SOURCE_OWNS_RULE_RELATION_ID
            | SOURCE_OWNS_RELATION_RELATION_ID
    )
}

fn ownership_fact_tuple(unit: Symbol, relation: Identity, tuple: Tuple) -> Tuple {
    Tuple::from([
        Value::symbol(unit),
        Value::identity(relation),
        Value::list(tuple.values().to_vec()),
    ])
}

fn owned_fact_tuple(ownership: &Tuple) -> Option<(Identity, Tuple)> {
    let [_, relation, values] = ownership.values() else {
        return None;
    };
    let relation = relation.as_identity()?;
    let tuple = values.with_list(|values| Tuple::new(values.iter().cloned()))?;
    Some((relation, tuple))
}

fn fileout_unit_source(kernel: &RelationKernel, unit: Symbol) -> Result<String, KernelError> {
    let snapshot = kernel.snapshot();
    let identity_names = identity_name_map(&snapshot);
    let relation_names = relation_name_map(&snapshot);
    let mut relation_declarations = BTreeSet::new();
    let mut identity_declarations = BTreeSet::new();
    let mut facts = BTreeSet::new();
    let mut rule_sources = BTreeSet::new();
    let mut method_sources = BTreeSet::new();

    for row in snapshot.scan(
        source_owns_relation_relation(),
        &[Some(Value::symbol(unit)), None],
    )? {
        let Some(relation) = row.values().get(1).and_then(Value::as_identity) else {
            continue;
        };
        if let Some(metadata) = snapshot
            .relation_metadata()
            .find(|metadata| metadata.id() == relation)
            && let Some(declaration) = relation_declaration_source(metadata)
        {
            relation_declarations.insert(declaration);
        }
    }

    for row in snapshot.scan(
        source_owns_fact_relation(),
        &[Some(Value::symbol(unit)), None, None],
    )? {
        let Some((relation, tuple)) = owned_fact_tuple(&row) else {
            continue;
        };
        if relation == named_identity_relation() {
            if let [name, _identity] = tuple.values()
                && let Some(symbol) = name.as_symbol()
                && let Some(name) = symbol.name()
            {
                identity_declarations.insert(name.to_owned());
            }
            continue;
        }
        if relation == method_source_relation() {
            if let Some(source) = tuple
                .values()
                .get(1)
                .and_then(|value| value.with_str(str::to_owned))
            {
                method_sources.insert(source.trim().to_owned());
            }
            continue;
        }
        if is_exported_fact_relation(relation) {
            facts.insert((relation, tuple));
        }
    }

    for row in snapshot.scan(
        source_owns_rule_relation(),
        &[Some(Value::symbol(unit)), None],
    )? {
        let Some(rule_id) = row.values().get(1).and_then(Value::as_identity) else {
            continue;
        };
        if let Some(rule) = snapshot
            .rules()
            .iter()
            .find(|rule| rule.id() == rule_id && rule.active())
        {
            rule_sources.insert(rule.source().trim().to_owned());
        }
    }

    let mut sections = Vec::new();
    if !identity_declarations.is_empty() {
        sections.push(
            identity_declarations
                .into_iter()
                .map(|name| format!("make_identity(:{name})"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
    if !relation_declarations.is_empty() {
        sections.push(
            relation_declarations
                .into_iter()
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
    if !facts.is_empty() {
        sections.push(
            facts
                .into_iter()
                .filter_map(|(relation, tuple)| {
                    let relation_name = relation_names.get(&relation)?;
                    Some(format!(
                        "assert {relation_name}({})",
                        tuple
                            .values()
                            .iter()
                            .map(|value| source_literal(value, &identity_names, &relation_names))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
    if !rule_sources.is_empty() {
        sections.push(rule_sources.into_iter().collect::<Vec<_>>().join("\n\n"));
    }
    if !method_sources.is_empty() {
        sections.push(method_sources.into_iter().collect::<Vec<_>>().join("\n\n"));
    }

    Ok(sections.join("\n\n"))
}

fn relation_declaration_source(metadata: &RelationMetadata) -> Option<String> {
    let name = metadata.name().name()?;
    Some(match metadata.conflict_policy() {
        ConflictPolicy::Set => format!("make_relation(:{name}, {})", metadata.arity()),
        ConflictPolicy::Functional { key_positions } => format!(
            "make_functional_relation(:{name}, {}, [{}])",
            metadata.arity(),
            key_positions
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ConflictPolicy::EventAppend => format!("make_relation(:{name}, {})", metadata.arity()),
    })
}

fn identity_name_map(snapshot: &mica_relation_kernel::Snapshot) -> BTreeMap<Identity, String> {
    snapshot
        .scan(named_identity_relation(), &[None, None])
        .unwrap_or_default()
        .into_iter()
        .filter_map(|tuple| {
            let [name, identity] = tuple.values() else {
                return None;
            };
            Some((
                identity.as_identity()?,
                name.as_symbol()?.name()?.to_owned(),
            ))
        })
        .collect()
}

fn relation_name_map(snapshot: &mica_relation_kernel::Snapshot) -> BTreeMap<Identity, String> {
    snapshot
        .relation_metadata()
        .filter_map(|metadata| Some((metadata.id(), metadata.name().name()?.to_owned())))
        .collect()
}

fn source_literal(
    value: &Value,
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) -> String {
    match value.kind() {
        ValueKind::Nothing => "nothing".to_owned(),
        ValueKind::Bool => value.as_bool().unwrap().to_string(),
        ValueKind::Int => value.as_int().unwrap().to_string(),
        ValueKind::Float => format!("{:?}", value.as_float().unwrap()),
        ValueKind::Identity => {
            let identity = value.as_identity().unwrap();
            match identity_names.get(&identity) {
                Some(name) => format!("#{name}"),
                None if relation_names.contains_key(&identity) => format!("#{}", identity.raw()),
                None => format!("#{}", identity.raw()),
            }
        }
        ValueKind::Symbol => render_symbol(value.as_symbol().unwrap(), ":"),
        ValueKind::ErrorCode => render_symbol(value.as_error_code().unwrap(), ""),
        ValueKind::String => value.with_str(|value| format!("{value:?}")).unwrap(),
        ValueKind::Bytes => format!("{value:?}"),
        ValueKind::List => value
            .with_list(|values| {
                render_sequence(
                    "[",
                    "]",
                    values
                        .iter()
                        .map(|value| source_literal(value, identity_names, relation_names)),
                )
            })
            .unwrap(),
        ValueKind::Map => value
            .with_map(|entries| {
                render_sequence(
                    "[",
                    "]",
                    entries.iter().map(|(key, value)| {
                        format!(
                            "{}: {}",
                            source_literal(key, identity_names, relation_names),
                            source_literal(value, identity_names, relation_names)
                        )
                    }),
                )
            })
            .unwrap(),
        ValueKind::Range => value
            .with_range(|start, end| match end {
                Some(end) => format!(
                    "{}..{}",
                    source_literal(start, identity_names, relation_names),
                    source_literal(end, identity_names, relation_names)
                ),
                None => format!(
                    "{}.._",
                    source_literal(start, identity_names, relation_names)
                ),
            })
            .unwrap(),
        ValueKind::Error => value
            .with_error(|error| {
                let mut out = format!("error({}", render_symbol(error.code(), ""));
                if let Some(message) = error.message() {
                    out.push_str(", ");
                    out.push_str(&format!("{message:?}"));
                }
                if let Some(payload) = error.value() {
                    if error.message().is_none() {
                        out.push_str(", nothing");
                    }
                    out.push_str(", ");
                    out.push_str(&source_literal(payload, identity_names, relation_names));
                }
                out.push(')');
                out
            })
            .unwrap(),
        ValueKind::Capability => "<cap>".to_owned(),
    }
}

fn installed_rule_value(rules: &[mica_relation_kernel::RuleDefinition]) -> Value {
    match rules {
        [rule] => Value::identity(rule.id()),
        _ => Value::list(
            rules
                .iter()
                .map(|rule| Value::identity(rule.id()))
                .collect::<Vec<_>>(),
        ),
    }
}

fn installed_method_value(installation: &MethodInstallation) -> Value {
    match installation.methods.as_slice() {
        [method] => method.method.clone(),
        methods => Value::list(
            methods
                .iter()
                .map(|method| method.method.clone())
                .collect::<Vec<_>>(),
        ),
    }
}

fn assign_generated_verb_identities(
    semantic: &mut mica_compiler::SemanticProgram,
    next_identity_id: u64,
) -> Result<(), CompileError> {
    let mut generated = 0;
    for item in &mut semantic.hir.items {
        let HirItem::Method {
            id,
            kind,
            identity,
            selector,
            ..
        } = item
        else {
            continue;
        };
        if !matches!(kind, MethodKind::Verb) || identity.is_some() {
            continue;
        }
        let selector = selector.as_ref().ok_or_else(|| CompileError::Unsupported {
            node: *id,
            span: None,
            message: "verb installation requires a selector name".to_owned(),
        })?;
        let ordinal = next_identity_id
            .checked_sub(GENERATED_METHOD_ID_START)
            .map(|offset| offset / 2 + 1 + generated)
            .unwrap_or(1 + generated);
        generated += 1;
        *identity = Some(format!("verb_{selector}_{ordinal}"));
    }
    Ok(())
}

fn ensure_named_identity(
    tx: &mut mica_relation_kernel::Transaction<'_>,
    name: &str,
    next_identity_id: &mut u64,
) -> Result<Identity, CompileError> {
    let symbol = Symbol::intern(name);
    let tuples = tx.scan(
        named_identity_relation(),
        &[Some(Value::symbol(symbol)), None],
    )?;
    if let Some(identity) = tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity)
    {
        return Ok(identity);
    }

    let identity = loop {
        let Some(identity) = Identity::new(*next_identity_id) else {
            return Err(CompileError::Unsupported {
                node: mica_compiler::NodeId(0),
                span: None,
                message: "generated method identity exhausted".to_owned(),
            });
        };
        *next_identity_id += 1;
        if tx
            .scan(
                named_identity_relation(),
                &[None, Some(Value::identity(identity))],
            )?
            .is_empty()
        {
            break identity;
        }
    };
    tx.assert(
        named_identity_relation(),
        Tuple::from([Value::symbol(symbol), Value::identity(identity)]),
    )?;
    Ok(identity)
}

fn bootstrap_kernel() -> RelationKernel {
    bootstrap_kernel_with_provider(Arc::new(mica_relation_kernel::InMemoryCommitProvider::new()))
}

fn bootstrap_kernel_with_provider(
    provider: Arc<dyn mica_relation_kernel::CommitProvider>,
) -> RelationKernel {
    let kernel = RelationKernel::with_provider(provider);
    kernel
        .create_relation(
            RelationMetadata::new(
                named_identity_relation(),
                Symbol::intern("NamedIdentity"),
                2,
            )
            .with_conflict_policy(ConflictPolicy::Functional {
                key_positions: vec![0],
            }),
        )
        .unwrap();
    for metadata in method_relation_metadata() {
        kernel.create_relation(metadata).unwrap();
    }
    for metadata in endpoint_relation_metadata() {
        kernel.create_relation(metadata).unwrap();
    }
    seed_primitive_prototype_identities(&kernel);
    kernel
}

fn seed_primitive_prototype_identities(kernel: &RelationKernel) {
    let mut tx = kernel.begin();
    for (name, identity) in PRIMITIVE_PROTOTYPES {
        tx.assert(
            named_identity_relation(),
            Tuple::from([
                Value::symbol(Symbol::intern(name)),
                Value::identity(*identity),
            ]),
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn next_generated_method_identity_id(kernel: &RelationKernel) -> u64 {
    kernel
        .snapshot()
        .scan(named_identity_relation(), &[None, None])
        .unwrap_or_default()
        .into_iter()
        .filter_map(|tuple| tuple.values().get(1).and_then(Value::as_identity))
        .map(|identity| identity.raw())
        .filter(|raw| *raw >= GENERATED_METHOD_ID_START)
        .max()
        .and_then(|raw| raw.checked_add(1))
        .unwrap_or(GENERATED_METHOD_ID_START)
}

fn method_relations() -> MethodRelations {
    MethodRelations {
        dispatch: DispatchRelations {
            method_selector: method_selector_relation(),
            param: param_relation(),
            delegates: delegates_relation(),
        },
        method_program: method_program_relation(),
        program_bytes: program_bytes_relation(),
    }
}

fn invocation_program(
    selector: Symbol,
    roles: Vec<(Symbol, Value)>,
) -> Result<Program, RuntimeError> {
    let relations = method_relations();
    Program::new(
        1,
        [
            Instruction::Dispatch {
                dst: Register(0),
                relations: relations.dispatch,
                program_relation: relations.method_program,
                program_bytes: relations.program_bytes,
                selector: Operand::Value(Value::symbol(selector)),
                roles: roles
                    .into_iter()
                    .map(|(role, value)| (Value::symbol(role), Operand::Value(value)))
                    .collect(),
            },
            Instruction::Return {
                value: Operand::Register(Register(0)),
            },
        ],
    )
}

fn invocation_roles(
    principal: Option<Identity>,
    actor: Option<Identity>,
    endpoint: Identity,
    mut roles: Vec<(Symbol, Value)>,
) -> Vec<(Symbol, Value)> {
    push_context_role(&mut roles, "principal", principal);
    push_context_role(&mut roles, "actor", actor);
    push_required_context_role(&mut roles, "endpoint", endpoint);
    roles
}

fn runtime_context(
    principal: Option<Identity>,
    actor: Option<Identity>,
    endpoint: Identity,
) -> RuntimeContext {
    RuntimeContext::new(principal, actor, endpoint)
}

fn push_context_role(roles: &mut Vec<(Symbol, Value)>, name: &str, identity: Option<Identity>) {
    let Some(identity) = identity else {
        return;
    };
    let role = Symbol::intern(name);
    if roles.iter().any(|(existing, _)| *existing == role) {
        return;
    }
    roles.push((role, Value::identity(identity)));
}

fn push_required_context_role(roles: &mut Vec<(Symbol, Value)>, name: &str, identity: Identity) {
    let role = Symbol::intern(name);
    if roles.iter().any(|(existing, _)| *existing == role) {
        return;
    }
    roles.push((role, Value::identity(identity)));
}

fn method_relation_metadata() -> Vec<RelationMetadata> {
    vec![
        RelationMetadata::new(
            method_selector_relation(),
            Symbol::intern("MethodSelector"),
            2,
        )
        .with_index([1, 0]),
        RelationMetadata::new(param_relation(), Symbol::intern("Param"), 4).with_index([0, 1]),
        RelationMetadata::new(delegates_relation(), Symbol::intern("Delegates"), 3)
            .with_index([0, 2, 1]),
        RelationMetadata::new(
            method_program_relation(),
            Symbol::intern("MethodProgram"),
            2,
        )
        .with_index([0]),
        RelationMetadata::new(program_bytes_relation(), Symbol::intern("ProgramBytes"), 2)
            .with_index([0]),
        RelationMetadata::new(method_source_relation(), Symbol::intern("MethodSource"), 2)
            .with_conflict_policy(ConflictPolicy::Functional {
                key_positions: vec![0],
            }),
        RelationMetadata::new(
            source_owns_fact_relation(),
            Symbol::intern("SourceOwnsFact"),
            3,
        ),
        RelationMetadata::new(
            source_owns_rule_relation(),
            Symbol::intern("SourceOwnsRule"),
            2,
        ),
        RelationMetadata::new(
            source_owns_relation_relation(),
            Symbol::intern("SourceOwnsRelation"),
            2,
        ),
    ]
}

fn endpoint_relation_metadata() -> Vec<RelationMetadata> {
    vec![
        RelationMetadata::new(endpoint_relation(), Symbol::intern("Endpoint"), 1),
        RelationMetadata::new(
            endpoint_actor_relation(),
            Symbol::intern("EndpointActor"),
            2,
        )
        .with_index([1, 0])
        .with_index([0]),
        RelationMetadata::new(
            endpoint_principal_relation(),
            Symbol::intern("EndpointPrincipal"),
            2,
        )
        .with_index([1, 0])
        .with_index([0]),
        RelationMetadata::new(
            endpoint_protocol_relation(),
            Symbol::intern("EndpointProtocol"),
            2,
        )
        .with_index([0]),
        RelationMetadata::new(endpoint_open_relation(), Symbol::intern("EndpointOpen"), 1),
    ]
}

fn endpoint_metadata(relation: Identity) -> Option<RelationMetadata> {
    endpoint_relation_metadata()
        .into_iter()
        .find(|metadata| metadata.id() == relation)
}

fn default_builtins() -> BuiltinRegistry {
    BuiltinRegistry::new()
        .with_builtin("emit", emit_builtin)
        .with_builtin("make_relation", MakeRelationBuiltin::new())
        .with_builtin(
            "make_functional_relation",
            MakeFunctionalRelationBuiltin::new(),
        )
        .with_builtin("make_identity", MakeIdentityBuiltin::new())
        .with_builtin("rules", rules_builtin)
        .with_builtin("describe_rule", describe_rule_builtin)
        .with_builtin("disable_rule", disable_rule_builtin)
        .with_builtin("fileout", fileout_builtin)
        .with_builtin("fileout_rules", fileout_rules_builtin)
        .with_builtin("tasks", tasks_builtin)
        .with_builtin("actor", actor_builtin)
        .with_builtin("principal", principal_builtin)
        .with_builtin("endpoint", endpoint_builtin)
        .with_builtin("assume_actor", assume_actor_builtin)
        .with_builtin("destroy_identity", destroy_identity_builtin)
        .with_builtin("assert_transient", assert_transient_builtin)
        .with_builtin("retract_transient", retract_transient_builtin)
        .with_builtin("drop_transient_scope", drop_transient_scope_builtin)
        .with_builtin("string_len", string_len_builtin)
        .with_builtin("string_chars", string_chars_builtin)
        .with_builtin("string_slice", string_slice_builtin)
        .with_builtin("string_from_chars", string_from_chars_builtin)
        .with_builtin("lower", lower_builtin)
}

fn emit_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 2 {
        return Err(RuntimeError::InvalidBuiltinCall {
            name: Symbol::intern("emit"),
            message: "emit expects target identity and value".to_owned(),
        });
    }
    let target_value = args[0].clone();
    let target = target_value
        .as_identity()
        .ok_or(RuntimeError::InvalidEffectTarget(target_value))?;
    let value = args[1].clone();
    context.emit(target, value.clone())?;
    Ok(value)
}

fn tasks_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if !args.is_empty() {
        return Err(invalid_builtin_call("tasks", "expected tasks()"));
    }
    Ok(Value::list(context.task_snapshot().iter().cloned()))
}

fn string_len_builtin(
    _context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "string_len",
            "expected string_len(text)",
        ));
    }
    let value = builtin_string_arg("string_len", args, 0)?;
    Value::int(value.chars().count() as i64)
        .map_err(|_| invalid_builtin_call("string_len", "string length is out of range"))
}

fn string_chars_builtin(
    _context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "string_chars",
            "expected string_chars(text)",
        ));
    }
    let value = builtin_string_arg("string_chars", args, 0)?;
    Ok(Value::list(
        value.chars().map(|ch| Value::string(ch.to_string())),
    ))
}

fn string_slice_builtin(
    _context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 3 {
        return Err(invalid_builtin_call(
            "string_slice",
            "expected string_slice(text, start, end)",
        ));
    }
    let value = builtin_string_arg("string_slice", args, 0)?;
    let start = builtin_usize_arg("string_slice", args, 1)?;
    let end = builtin_usize_arg("string_slice", args, 2)?;
    Ok(string_slice_chars(&value, start, end)
        .map(Value::string)
        .unwrap_or_else(Value::nothing))
}

fn string_from_chars_builtin(
    _context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "string_from_chars",
            "expected string_from_chars(chars)",
        ));
    }
    let chars = builtin_char_list_arg("string_from_chars", args, 0)?;
    Ok(Value::string(chars.into_iter().collect::<String>()))
}

fn lower_builtin(
    _context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call("lower", "expected lower(text)"));
    }
    Ok(Value::string(
        builtin_string_arg("lower", args, 0)?.to_lowercase(),
    ))
}

fn string_slice_chars(value: &str, start: usize, end: usize) -> Option<&str> {
    if start > end {
        return None;
    }
    let char_len = value.chars().count();
    if end > char_len {
        return None;
    }
    let start_byte = value
        .char_indices()
        .nth(start)
        .map(|(index, _)| index)
        .unwrap_or(value.len());
    let end_byte = value
        .char_indices()
        .nth(end)
        .map(|(index, _)| index)
        .unwrap_or(value.len());
    Some(&value[start_byte..end_byte])
}

fn actor_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    runtime_identity_builtin("actor", args, context.runtime_context().actor())
}

fn principal_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    runtime_identity_builtin("principal", args, context.runtime_context().principal())
}

fn endpoint_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if !args.is_empty() {
        return Err(invalid_builtin_call("endpoint", "expected endpoint()"));
    }
    Ok(Value::identity(context.runtime_context().endpoint()))
}

fn assume_actor_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "assume_actor",
            "expected assume_actor(#actor)",
        ));
    }
    let actor = builtin_identity_arg("assume_actor", args, 0)?;
    require_endpoint_open(context, "assume_actor")?;
    require_actor_assumption(context, actor)?;
    replace_endpoint_identity_binding(context, endpoint_actor_relation(), actor)?;
    Ok(Value::identity(actor))
}

fn require_endpoint_open(
    context: &mut BuiltinContext<'_, '_>,
    operation: &'static str,
) -> Result<(), RuntimeError> {
    let endpoint = context.runtime_context().endpoint();
    let rows = context.scan_transient(
        &[endpoint],
        endpoint_open_relation(),
        &[Some(Value::identity(endpoint))],
    )?;
    if rows.is_empty() {
        return Err(RuntimeError::PermissionDenied {
            operation,
            target: Value::identity(endpoint),
        });
    }
    Ok(())
}

fn require_actor_assumption(
    context: &mut BuiltinContext<'_, '_>,
    actor: Identity,
) -> Result<(), RuntimeError> {
    if context.authority().can_grant() {
        return Ok(());
    }
    let Some(principal) = context.runtime_context().principal() else {
        return Err(RuntimeError::PermissionDenied {
            operation: "assume_actor",
            target: Value::identity(actor),
        });
    };
    let Some(policy_relation) = runtime_policy_relation(context.kernel(), "CanAssumeActor", 2)?
    else {
        return Err(RuntimeError::PermissionDenied {
            operation: "assume_actor",
            target: Value::identity(actor),
        });
    };
    let rows = context.tx().scan(
        policy_relation,
        &[
            Some(Value::identity(principal)),
            Some(Value::identity(actor)),
        ],
    )?;
    if rows.is_empty() {
        return Err(RuntimeError::PermissionDenied {
            operation: "assume_actor",
            target: Value::identity(actor),
        });
    }
    Ok(())
}

fn replace_endpoint_identity_binding(
    context: &mut BuiltinContext<'_, '_>,
    relation: Identity,
    identity: Identity,
) -> Result<(), RuntimeError> {
    let endpoint = context.runtime_context().endpoint();
    let rows = context.scan_transient(
        &[endpoint],
        relation,
        &[Some(Value::identity(endpoint)), None],
    )?;
    for row in rows {
        context.retract_transient(endpoint, relation, &row)?;
    }
    let metadata = endpoint_metadata(relation)
        .ok_or(RuntimeError::Kernel(KernelError::UnknownRelation(relation)))?;
    context.assert_transient(
        endpoint,
        metadata,
        Tuple::from([Value::identity(endpoint), Value::identity(identity)]),
    )?;
    Ok(())
}

fn destroy_identity_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "destroy_identity",
            "expected destroy_identity(#identity)",
        ));
    }
    require_admin_builtin(context, "destroy_identity")?;
    let identity = builtin_identity_arg("destroy_identity", args, 0)?;
    let value = Value::identity(identity);
    let facts = context.tx().subject_facts(&value)?;
    let mut retracted = BTreeSet::new();
    for fact in facts {
        if retracted.insert((fact.relation, fact.tuple.clone())) {
            context.tx().retract(fact.relation, fact.tuple)?;
        }
    }
    let names = context
        .tx()
        .scan(named_identity_relation(), &[None, Some(value)])?;
    for name in names {
        if retracted.insert((named_identity_relation(), name.clone())) {
            context.tx().retract(named_identity_relation(), name)?;
        }
    }
    Value::int(retracted.len() as i64).map_err(|_| {
        invalid_builtin_call("destroy_identity", "destroyed fact count is out of range")
    })
}

fn assert_transient_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 3 {
        return Err(invalid_builtin_call(
            "assert_transient",
            "expected assert_transient(#scope, :Relation, [values])",
        ));
    }
    let scope = builtin_identity_arg("assert_transient", args, 0)?;
    let metadata = builtin_relation_metadata_arg(context, "assert_transient", args, 1)?;
    require_relation_write(context.authority(), metadata.id())?;
    let tuple = builtin_tuple_arg("assert_transient", args, 2, metadata.arity())?;
    Ok(Value::bool(
        context.assert_transient(scope, metadata, tuple)?,
    ))
}

fn retract_transient_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 3 {
        return Err(invalid_builtin_call(
            "retract_transient",
            "expected retract_transient(#scope, :Relation, [values])",
        ));
    }
    let scope = builtin_identity_arg("retract_transient", args, 0)?;
    let metadata = builtin_relation_metadata_arg(context, "retract_transient", args, 1)?;
    require_relation_write(context.authority(), metadata.id())?;
    let tuple = builtin_tuple_arg("retract_transient", args, 2, metadata.arity())?;
    Ok(Value::bool(context.retract_transient(
        scope,
        metadata.id(),
        &tuple,
    )?))
}

fn drop_transient_scope_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "drop_transient_scope",
            "expected drop_transient_scope(#scope)",
        ));
    }
    let scope = builtin_identity_arg("drop_transient_scope", args, 0)?;
    require_transient_scope_drop(context, scope)?;
    let dropped = context.drop_transient_scope(scope)?;
    Value::int(dropped as i64).map_err(|_| {
        invalid_builtin_call(
            "drop_transient_scope",
            "dropped tuple count is out of range",
        )
    })
}

fn require_transient_scope_drop(
    context: &BuiltinContext<'_, '_>,
    scope: Identity,
) -> Result<(), RuntimeError> {
    if context.authority().can_grant() {
        return Ok(());
    }
    let runtime_context = context.runtime_context();
    if runtime_context.endpoint() == scope
        || [runtime_context.principal(), runtime_context.actor()]
            .into_iter()
            .flatten()
            .any(|visible_scope| visible_scope == scope)
    {
        return Ok(());
    }
    Err(RuntimeError::PermissionDenied {
        operation: "drop_transient_scope",
        target: Value::identity(scope),
    })
}

fn runtime_identity_builtin(
    name: &'static str,
    args: &[Value],
    identity: Option<Identity>,
) -> Result<Value, RuntimeError> {
    if !args.is_empty() {
        return Err(invalid_builtin_call(name, &format!("expected {name}()")));
    }
    Ok(identity.map(Value::identity).unwrap_or_else(Value::nothing))
}

fn unsupported_runner_error(
    node: NodeId,
    span: Option<mica_compiler::Span>,
    message: impl Into<String>,
) -> SourceTaskError {
    CompileError::Unsupported {
        node,
        span,
        message: message.into(),
    }
    .into()
}

fn rules_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call("rules", "expected rules(:Relation)"));
    }
    let name = builtin_symbol_arg("rules", args, 0)?;
    let Some((relation, _)) = relation_named(context.kernel(), name) else {
        return Ok(Value::list([]));
    };
    let rules = context
        .kernel()
        .snapshot()
        .rules()
        .iter()
        .filter(|rule| rule.active() && rule.rule().head_relation() == relation)
        .map(|rule| Value::identity(rule.id()))
        .collect::<Vec<_>>();
    Ok(Value::list(rules))
}

fn describe_rule_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "describe_rule",
            "expected describe_rule(#rule)",
        ));
    }
    let rule_id = builtin_identity_arg("describe_rule", args, 0)?;
    let source = context
        .kernel()
        .snapshot()
        .rules()
        .iter()
        .find(|rule| rule.id() == rule_id)
        .map(|rule| Value::string(rule.source()))
        .unwrap_or_else(Value::nothing);
    Ok(source)
}

fn fileout_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call("fileout", "expected fileout(:unit)"));
    }
    let unit = builtin_symbol_arg("fileout", args, 0)?;
    let source = fileout_unit_source(context.kernel(), unit)?;
    Ok(Value::string(source))
}

fn fileout_rules_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() > 1 {
        return Err(invalid_builtin_call(
            "fileout_rules",
            "expected fileout_rules() or fileout_rules(:Relation)",
        ));
    }
    let relation = if args.is_empty() {
        None
    } else {
        let name = builtin_symbol_arg("fileout_rules", args, 0)?;
        relation_named(context.kernel(), name).map(|(relation, _)| relation)
    };
    let snapshot = context.kernel().snapshot();
    let sources = snapshot
        .rules()
        .iter()
        .filter(|rule| rule.active())
        .filter(|rule| {
            relation
                .map(|relation| relation == rule.rule().head_relation())
                .unwrap_or(true)
        })
        .map(|rule| rule.source().trim().to_owned())
        .collect::<Vec<_>>();
    Ok(Value::string(sources.join("\n\n")))
}

fn disable_rule_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "disable_rule",
            "expected disable_rule(#rule)",
        ));
    }
    let rule_id = builtin_identity_arg("disable_rule", args, 0)?;
    require_admin_builtin(context, "disable_rule")?;
    context.kernel().disable_rule(rule_id)?;
    Ok(Value::nothing())
}

fn require_admin_builtin(
    context: &BuiltinContext<'_, '_>,
    name: &'static str,
) -> Result<(), RuntimeError> {
    if context.authority().can_grant() {
        return Ok(());
    }
    Err(RuntimeError::PermissionDenied {
        operation: "grant",
        target: Value::symbol(Symbol::intern(name)),
    })
}

struct MakeRelationBuiltin {
    next_relation_id: AtomicU64,
}

struct MakeFunctionalRelationBuiltin {
    next_relation_id: AtomicU64,
}

struct MakeIdentityBuiltin {
    next_identity_id: AtomicU64,
}

impl MakeRelationBuiltin {
    fn new() -> Self {
        Self {
            next_relation_id: AtomicU64::new(GENERATED_RELATION_ID_START),
        }
    }
}

impl MakeFunctionalRelationBuiltin {
    fn new() -> Self {
        Self {
            next_relation_id: AtomicU64::new(GENERATED_RELATION_ID_START),
        }
    }
}

impl MakeIdentityBuiltin {
    fn new() -> Self {
        Self {
            next_identity_id: AtomicU64::new(GENERATED_IDENTITY_ID_START),
        }
    }
}

impl Builtin for MakeRelationBuiltin {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        let name = builtin_symbol_arg("make_relation", args, 0)?;
        let arity = builtin_arity_arg("make_relation", args, 1)?;
        if args.len() != 2 {
            return Err(invalid_builtin_call(
                "make_relation",
                "expected make_relation(:Name, arity)",
            ));
        }
        require_admin_builtin(context, "make_relation")?;

        if let Some((relation, existing_arity)) = relation_named(context.kernel(), name) {
            if existing_arity == arity {
                return Ok(Value::identity(relation));
            }
            return Err(invalid_builtin_call(
                "make_relation",
                "relation name already exists with different arity",
            ));
        }

        loop {
            let Some(relation) =
                Identity::new(self.next_relation_id.fetch_add(1, Ordering::Relaxed))
            else {
                return Err(invalid_builtin_call(
                    "make_relation",
                    "generated relation identity exhausted",
                ));
            };
            let metadata = RelationMetadata::new(relation, name, arity);
            match context.kernel().create_relation(metadata) {
                Ok(_) => return Ok(Value::identity(relation)),
                Err(KernelError::RelationAlreadyExists(_)) => continue,
                Err(error) => return Err(RuntimeError::Kernel(error)),
            }
        }
    }
}

impl Builtin for MakeFunctionalRelationBuiltin {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        let name = builtin_symbol_arg("make_functional_relation", args, 0)?;
        let arity = builtin_arity_arg("make_functional_relation", args, 1)?;
        let key_positions = builtin_key_positions_arg("make_functional_relation", args, 2, arity)?;
        if args.len() != 3 {
            return Err(invalid_builtin_call(
                "make_functional_relation",
                "expected make_functional_relation(:Name, arity, [key_positions])",
            ));
        }
        require_admin_builtin(context, "make_functional_relation")?;

        if let Some(metadata) = relation_metadata_named(context.kernel(), name) {
            if metadata.arity() == arity
                && metadata.conflict_policy()
                    == &(ConflictPolicy::Functional {
                        key_positions: key_positions.clone(),
                    })
            {
                return Ok(Value::identity(metadata.id()));
            }
            return Err(invalid_builtin_call(
                "make_functional_relation",
                "relation name already exists with different metadata",
            ));
        }

        loop {
            let Some(relation) =
                Identity::new(self.next_relation_id.fetch_add(1, Ordering::Relaxed))
            else {
                return Err(invalid_builtin_call(
                    "make_functional_relation",
                    "generated relation identity exhausted",
                ));
            };
            let metadata = RelationMetadata::new(relation, name, arity).with_conflict_policy(
                ConflictPolicy::Functional {
                    key_positions: key_positions.clone(),
                },
            );
            match context.kernel().create_relation(metadata) {
                Ok(_) => return Ok(Value::identity(relation)),
                Err(KernelError::RelationAlreadyExists(_)) => continue,
                Err(error) => return Err(RuntimeError::Kernel(error)),
            }
        }
    }
}

impl Builtin for MakeIdentityBuiltin {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        if args.len() != 1 {
            return Err(invalid_builtin_call(
                "make_identity",
                "expected make_identity(:name)",
            ));
        }
        let name = builtin_symbol_arg("make_identity", args, 0)?;
        require_admin_builtin(context, "make_identity")?;

        if let Some(identity) = identity_named(context, name)? {
            return Ok(Value::identity(identity));
        }

        let Some(identity) = Identity::new(self.next_identity_id.fetch_add(1, Ordering::Relaxed))
        else {
            return Err(invalid_builtin_call(
                "make_identity",
                "generated identity exhausted",
            ));
        };
        context.tx().replace_functional(
            named_identity_relation(),
            Tuple::from([Value::symbol(name), Value::identity(identity)]),
        )?;
        Ok(Value::identity(identity))
    }
}

fn relation_named(kernel: &RelationKernel, name: Symbol) -> Option<(Identity, u16)> {
    let snapshot = kernel.snapshot();
    snapshot
        .relation_metadata()
        .find(|metadata| metadata.name() == name)
        .map(|metadata| (metadata.id(), metadata.arity()))
}

fn relation_metadata_named(kernel: &RelationKernel, name: Symbol) -> Option<RelationMetadata> {
    let snapshot = kernel.snapshot();
    snapshot
        .relation_metadata()
        .find(|metadata| metadata.name() == name)
        .cloned()
}

fn authority_for_actor(
    kernel: &RelationKernel,
    actor: Identity,
) -> Result<AuthorityContext, SourceTaskError> {
    let mut authority = AuthorityContext::empty();
    for policy_name in ["CanRead", "GrantRead"] {
        mint_relation_grants(
            kernel,
            actor,
            policy_name,
            CapabilityOp::Read,
            &mut authority,
        )?;
    }
    for policy_name in ["CanWrite", "GrantWrite"] {
        mint_relation_grants(
            kernel,
            actor,
            policy_name,
            CapabilityOp::Write,
            &mut authority,
        )?;
    }
    for policy_name in ["CanInvoke", "GrantInvoke"] {
        mint_invoke_grants(kernel, actor, policy_name, &mut authority)?;
    }
    for policy_name in ["CanEffect", "GrantEffect"] {
        mint_effect_grants(kernel, actor, policy_name, &mut authority)?;
    }
    Ok(authority)
}

fn authority_for_runtime_context(
    kernel: &RelationKernel,
    runtime_context: RuntimeContext,
) -> Result<AuthorityContext, SourceTaskError> {
    match runtime_context.actor() {
        Some(actor) => authority_for_actor(kernel, actor),
        None => Ok(AuthorityContext::empty()),
    }
}

fn mint_relation_grants(
    kernel: &RelationKernel,
    actor: Identity,
    policy_name: &str,
    op: CapabilityOp,
    authority: &mut AuthorityContext,
) -> Result<(), SourceTaskError> {
    let Some(policy_relation) = policy_relation(kernel, policy_name, 2)? else {
        return Ok(());
    };
    let snapshot = kernel.snapshot();
    let tuples = snapshot
        .scan(policy_relation, &[Some(Value::identity(actor)), None])
        .map_err(CompileError::from)?;
    for tuple in tuples {
        let Some(relation_name) = tuple.values().get(1).and_then(Value::as_symbol) else {
            return Err(invalid_policy_fact(
                policy_name,
                "expected relation name symbol",
            ));
        };
        if let Some((relation, _)) = relation_named(kernel, relation_name) {
            authority.mint(CapabilityGrant::relation(op, relation));
        }
    }
    Ok(())
}

fn mint_invoke_grants(
    kernel: &RelationKernel,
    actor: Identity,
    policy_name: &str,
    authority: &mut AuthorityContext,
) -> Result<(), SourceTaskError> {
    let Some(policy_relation) = policy_relation(kernel, policy_name, 2)? else {
        return Ok(());
    };
    let snapshot = kernel.snapshot();
    let tuples = snapshot
        .scan(policy_relation, &[Some(Value::identity(actor)), None])
        .map_err(CompileError::from)?;
    for tuple in tuples {
        let Some(selector) = tuple.values().get(1).and_then(Value::as_symbol) else {
            return Err(invalid_policy_fact(
                policy_name,
                "expected selector name symbol",
            ));
        };
        for method in snapshot
            .scan(
                method_selector_relation(),
                &[None, Some(Value::symbol(selector))],
            )
            .map_err(CompileError::from)?
        {
            if let Some(method) = method.values().first() {
                authority.mint(CapabilityGrant::method(method.clone()));
            }
        }
    }
    Ok(())
}

fn mint_effect_grants(
    kernel: &RelationKernel,
    actor: Identity,
    policy_name: &str,
    authority: &mut AuthorityContext,
) -> Result<(), SourceTaskError> {
    let Some(policy_relation) = policy_relation(kernel, policy_name, 1)? else {
        return Ok(());
    };
    let snapshot = kernel.snapshot();
    if !snapshot
        .scan(policy_relation, &[Some(Value::identity(actor))])
        .map_err(CompileError::from)?
        .is_empty()
    {
        authority.mint(CapabilityGrant::new(
            [CapabilityOp::Effect],
            CapabilityScope::All,
        ));
    }
    Ok(())
}

fn policy_relation(
    kernel: &RelationKernel,
    name: &str,
    expected_arity: u16,
) -> Result<Option<Identity>, SourceTaskError> {
    let Some((relation, arity)) = relation_named(kernel, Symbol::intern(name)) else {
        return Ok(None);
    };
    if arity != expected_arity {
        return Err(unsupported_runner_error(
            NodeId(0),
            None,
            format!("policy relation {name} has arity {arity}, expected {expected_arity}"),
        ));
    }
    Ok(Some(relation))
}

fn runtime_policy_relation(
    kernel: &RelationKernel,
    name: &str,
    expected_arity: u16,
) -> Result<Option<Identity>, RuntimeError> {
    let Some((relation, arity)) = relation_named(kernel, Symbol::intern(name)) else {
        return Ok(None);
    };
    if arity != expected_arity {
        return Err(RuntimeError::InvalidBuiltinCall {
            name: Symbol::intern(name),
            message: format!("policy relation {name} has arity {arity}, expected {expected_arity}"),
        });
    }
    Ok(Some(relation))
}

fn invalid_policy_fact(relation: &str, message: &str) -> SourceTaskError {
    unsupported_runner_error(
        NodeId(0),
        None,
        format!("{relation} policy fact is invalid: {message}"),
    )
}

fn identity_named_in_kernel(
    kernel: &RelationKernel,
    name: Symbol,
) -> Result<Option<Identity>, SourceTaskError> {
    let snapshot = kernel.snapshot();
    let tuples = snapshot
        .scan(
            named_identity_relation(),
            &[Some(Value::symbol(name)), None],
        )
        .map_err(CompileError::from)?;
    Ok(tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity))
}

fn identity_named(
    context: &mut BuiltinContext<'_, '_>,
    name: Symbol,
) -> Result<Option<Identity>, RuntimeError> {
    let tuples = context.tx().scan(
        named_identity_relation(),
        &[Some(Value::symbol(name)), None],
    )?;
    Ok(tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity))
}

fn identity_named_in_tx(
    tx: &mica_relation_kernel::Transaction<'_>,
    name: Symbol,
) -> Result<Option<Identity>, KernelError> {
    let tuples = tx.scan(
        named_identity_relation(),
        &[Some(Value::symbol(name)), None],
    )?;
    Ok(tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity))
}

fn named_identity_relation() -> Identity {
    Identity::new(NAMED_IDENTITY_RELATION_ID).unwrap()
}

fn method_selector_relation() -> Identity {
    Identity::new(METHOD_SELECTOR_RELATION_ID).unwrap()
}

fn param_relation() -> Identity {
    Identity::new(PARAM_RELATION_ID).unwrap()
}

fn delegates_relation() -> Identity {
    Identity::new(DELEGATES_RELATION_ID).unwrap()
}

fn method_program_relation() -> Identity {
    Identity::new(METHOD_PROGRAM_RELATION_ID).unwrap()
}

fn program_bytes_relation() -> Identity {
    Identity::new(PROGRAM_BYTES_RELATION_ID).unwrap()
}

fn method_source_relation() -> Identity {
    Identity::new(METHOD_SOURCE_RELATION_ID).unwrap()
}

fn source_owns_fact_relation() -> Identity {
    Identity::new(SOURCE_OWNS_FACT_RELATION_ID).unwrap()
}

fn source_owns_rule_relation() -> Identity {
    Identity::new(SOURCE_OWNS_RULE_RELATION_ID).unwrap()
}

fn source_owns_relation_relation() -> Identity {
    Identity::new(SOURCE_OWNS_RELATION_RELATION_ID).unwrap()
}

fn endpoint_relation() -> Identity {
    Identity::new(ENDPOINT_RELATION_ID).unwrap()
}

fn endpoint_actor_relation() -> Identity {
    Identity::new(ENDPOINT_ACTOR_RELATION_ID).unwrap()
}

fn endpoint_principal_relation() -> Identity {
    Identity::new(ENDPOINT_PRINCIPAL_RELATION_ID).unwrap()
}

fn endpoint_protocol_relation() -> Identity {
    Identity::new(ENDPOINT_PROTOCOL_RELATION_ID).unwrap()
}

fn endpoint_open_relation() -> Identity {
    Identity::new(ENDPOINT_OPEN_RELATION_ID).unwrap()
}

fn item_id(item: &HirItem) -> mica_compiler::NodeId {
    match item {
        HirItem::Expr { id, .. }
        | HirItem::RelationRule { id, .. }
        | HirItem::Object { id, .. }
        | HirItem::Method { id, .. } => *id,
    }
}

fn builtin_symbol_arg(name: &str, args: &[Value], index: usize) -> Result<Symbol, RuntimeError> {
    args.get(index)
        .and_then(Value::as_symbol)
        .ok_or_else(|| invalid_builtin_call(name, "expected symbol argument"))
}

fn builtin_identity_arg(
    name: &str,
    args: &[Value],
    index: usize,
) -> Result<Identity, RuntimeError> {
    args.get(index)
        .and_then(Value::as_identity)
        .ok_or_else(|| invalid_builtin_call(name, "expected identity argument"))
}

fn builtin_string_arg(name: &str, args: &[Value], index: usize) -> Result<String, RuntimeError> {
    args.get(index)
        .and_then(|value| value.with_str(str::to_owned))
        .ok_or_else(|| invalid_builtin_call(name, "expected string argument"))
}

fn builtin_char_list_arg(
    name: &str,
    args: &[Value],
    index: usize,
) -> Result<Vec<char>, RuntimeError> {
    args.get(index)
        .and_then(|value| {
            value.with_list(|values| {
                values
                    .iter()
                    .map(|value| {
                        value.with_str(|text| {
                            let mut chars = text.chars();
                            let ch = chars.next()?;
                            chars.next().is_none().then_some(ch)
                        })?
                    })
                    .collect::<Option<Vec<_>>>()
            })
        })
        .flatten()
        .ok_or_else(|| invalid_builtin_call(name, "expected single-character string list argument"))
}

fn builtin_usize_arg(name: &str, args: &[Value], index: usize) -> Result<usize, RuntimeError> {
    let Some(value) = args.get(index).and_then(Value::as_int) else {
        return Err(invalid_builtin_call(name, "expected integer argument"));
    };
    usize::try_from(value)
        .map_err(|_| invalid_builtin_call(name, "integer argument is out of range"))
}

fn builtin_arity_arg(name: &str, args: &[Value], index: usize) -> Result<u16, RuntimeError> {
    let Some(arity) = args.get(index).and_then(Value::as_int) else {
        return Err(invalid_builtin_call(name, "expected integer arity"));
    };
    u16::try_from(arity).map_err(|_| invalid_builtin_call(name, "arity must fit in u16"))
}

fn builtin_key_positions_arg(
    name: &str,
    args: &[Value],
    index: usize,
    arity: u16,
) -> Result<Vec<u16>, RuntimeError> {
    let Some(value) = args.get(index) else {
        return Err(invalid_builtin_call(name, "expected key position list"));
    };
    let Some(positions) = value.with_list(|values| {
        values
            .iter()
            .map(|value| {
                let Some(position) = value.as_int() else {
                    return Err(invalid_builtin_call(name, "key positions must be integers"));
                };
                let position = u16::try_from(position)
                    .map_err(|_| invalid_builtin_call(name, "key position must fit in u16"))?;
                if position >= arity {
                    return Err(invalid_builtin_call(
                        name,
                        "key position is outside relation arity",
                    ));
                }
                Ok(position)
            })
            .collect::<Result<Vec<_>, _>>()
    }) else {
        return Err(invalid_builtin_call(name, "expected key position list"));
    };
    positions
}

fn builtin_relation_metadata_arg(
    context: &BuiltinContext<'_, '_>,
    name: &str,
    args: &[Value],
    index: usize,
) -> Result<RelationMetadata, RuntimeError> {
    let relation_name = builtin_symbol_arg(name, args, index)?;
    relation_metadata_named(context.kernel(), relation_name)
        .ok_or_else(|| invalid_builtin_call(name, "unknown relation"))
}

fn builtin_tuple_arg(
    name: &str,
    args: &[Value],
    index: usize,
    arity: u16,
) -> Result<Tuple, RuntimeError> {
    let Some(value) = args.get(index) else {
        return Err(invalid_builtin_call(name, "expected tuple value list"));
    };
    let Some(tuple) = value.with_list(|values| {
        if values.len() != arity as usize {
            return Err(invalid_builtin_call(
                name,
                "tuple arity does not match relation",
            ));
        }
        Ok(Tuple::new(values.iter().cloned()))
    }) else {
        return Err(invalid_builtin_call(name, "expected tuple value list"));
    };
    tuple
}

fn require_relation_write(
    authority: &AuthorityContext,
    relation: Identity,
) -> Result<(), RuntimeError> {
    if authority.can_write_relation(relation) {
        Ok(())
    } else {
        Err(RuntimeError::PermissionDenied {
            operation: "write",
            target: Value::identity(relation),
        })
    }
}

fn invalid_builtin_call(name: &str, message: &str) -> RuntimeError {
    RuntimeError::InvalidBuiltinCall {
        name: Symbol::intern(name),
        message: message.to_owned(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunReport {
    pub task_id: u64,
    pub outcome: TaskOutcome,
    identity_names: BTreeMap<Identity, String>,
    relation_names: BTreeMap<Identity, String>,
}

impl RunReport {
    pub fn render(&self) -> String {
        match &self.outcome {
            TaskOutcome::Complete {
                value,
                effects,
                retries,
            } => render_finished(
                "complete",
                self.task_id,
                value,
                effects,
                *retries,
                &self.identity_names,
                &self.relation_names,
            ),
            TaskOutcome::Aborted {
                error,
                effects,
                retries,
            } => render_finished(
                "aborted",
                self.task_id,
                error,
                effects,
                *retries,
                &self.identity_names,
                &self.relation_names,
            ),
            TaskOutcome::Suspended {
                kind,
                effects,
                retries,
            } => {
                let mut out = format!(
                    "task {} suspended: {:?} (retries: {})",
                    self.task_id, kind, retries
                );
                render_effects(
                    &mut out,
                    effects,
                    &self.identity_names,
                    &self.relation_names,
                );
                out
            }
        }
    }
}

fn render_finished(
    label: &str,
    task_id: u64,
    value: &Value,
    effects: &[Emission],
    retries: u8,
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) -> String {
    let mut out = format!(
        "task {task_id} {label}: {} (retries: {retries})",
        render_value(value, identity_names, relation_names)
    );
    render_effects(&mut out, effects, identity_names, relation_names);
    out
}

fn render_effects(
    out: &mut String,
    effects: &[Emission],
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) {
    for effect in effects {
        out.push_str("\neffect ");
        out.push_str(&render_value(
            &Value::identity(effect.target()),
            identity_names,
            relation_names,
        ));
        out.push_str(": ");
        out.push_str(&render_value(
            effect.value(),
            identity_names,
            relation_names,
        ));
    }
}

fn render_value(
    value: &Value,
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) -> String {
    match value.kind() {
        ValueKind::Nothing => "nothing".to_owned(),
        ValueKind::Bool => value.as_bool().unwrap().to_string(),
        ValueKind::Int => value.as_int().unwrap().to_string(),
        ValueKind::Float => format!("{:?}", value.as_float().unwrap()),
        ValueKind::Identity => {
            let identity = value.as_identity().unwrap();
            match identity_names.get(&identity) {
                Some(name) => format!("#{name}"),
                None => match relation_names.get(&identity) {
                    Some(name) => format!("relation(:{name})"),
                    None => format!("#{}", identity.raw()),
                },
            }
        }
        ValueKind::Symbol => render_symbol(value.as_symbol().unwrap(), ":"),
        ValueKind::ErrorCode => render_symbol(value.as_error_code().unwrap(), ""),
        ValueKind::String => value.with_str(|value| format!("{value:?}")).unwrap(),
        ValueKind::Bytes => format!("{value:?}"),
        ValueKind::List => value
            .with_list(|values| {
                render_sequence(
                    "[",
                    "]",
                    values
                        .iter()
                        .map(|value| render_value(value, identity_names, relation_names)),
                )
            })
            .unwrap(),
        ValueKind::Map => value
            .with_map(|entries| {
                render_sequence(
                    "[",
                    "]",
                    entries.iter().map(|(key, value)| {
                        format!(
                            "{}: {}",
                            render_value(key, identity_names, relation_names),
                            render_value(value, identity_names, relation_names)
                        )
                    }),
                )
            })
            .unwrap(),
        ValueKind::Range => value
            .with_range(|start, end| match end {
                Some(end) => format!(
                    "{}..{}",
                    render_value(start, identity_names, relation_names),
                    render_value(end, identity_names, relation_names)
                ),
                None => format!("{}.._", render_value(start, identity_names, relation_names)),
            })
            .unwrap(),
        ValueKind::Error => value
            .with_error(|error| {
                let mut out = format!("error({}", render_symbol(error.code(), ""));
                if let Some(message) = error.message() {
                    out.push_str(", ");
                    out.push_str(&format!("{message:?}"));
                }
                if let Some(payload) = error.value() {
                    if error.message().is_none() {
                        out.push_str(", nothing");
                    }
                    out.push_str(", ");
                    out.push_str(&render_value(payload, identity_names, relation_names));
                }
                out.push(')');
                out
            })
            .unwrap(),
        ValueKind::Capability => "<cap>".to_owned(),
    }
}

fn render_symbol(symbol: Symbol, prefix: &str) -> String {
    match symbol.name() {
        Some(name) => format!("{prefix}{name}"),
        None => format!("{prefix}#{}", symbol.id()),
    }
}

fn render_sequence(open: &str, close: &str, items: impl IntoIterator<Item = String>) -> String {
    let mut out = open.to_owned();
    for (index, item) in items.into_iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        out.push_str(&item);
    }
    out.push_str(close);
    out
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorityContext, Emission, Instruction, Operand, Program, SYSTEM_ENDPOINT, SuspendKind,
        TaskOutcome,
    };
    use super::{FileinMode, SourceRunner, TaskInput, TaskRequest};
    use mica_var::{Identity, Symbol, Value};
    use std::sync::Arc;

    #[test]
    fn runner_executes_source_against_empty_kernel() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return 1 + 2").unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(3).unwrap()
        ));
    }

    #[test]
    fn runner_installs_default_emit_builtin() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:target)").unwrap();
        let report = runner
            .run_source("return emit(#target, \"hello\")")
            .unwrap();
        let target = Identity::new(0x00e0_0000_0000_0000).unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, effects, .. }
                if value == Value::string("hello")
                    && effects == vec![Emission::new(target, Value::string("hello"))]
        ));
    }

    #[test]
    fn runner_emit_requires_target_identity() {
        let mut runner = SourceRunner::new_empty();

        let missing_target = runner.run_source("return emit(\"hello\")").unwrap_err();
        assert!(format!("{missing_target:?}").contains("emit expects target identity and value"));

        let non_identity = runner
            .run_source("return emit(:target, \"hello\")")
            .unwrap_err();
        assert!(format!("{non_identity:?}").contains("InvalidEffectTarget"));
    }

    #[test]
    fn runner_string_primitives_support_character_level_munging() {
        let mut runner = SourceRunner::new_empty();

        assert!(matches!(
            runner.run_source("return string_len(\"hé\")").unwrap().outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(2).unwrap()
        ));
        assert!(matches!(
            runner.run_source("return string_chars(\"ab\")").unwrap().outcome,
            TaskOutcome::Complete { value, .. }
                if value == Value::list([Value::string("a"), Value::string("b")])
        ));
        assert!(matches!(
            runner
                .run_source("return string_slice(\"héllo\", 1, 4)")
                .unwrap()
                .outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("éll")
        ));
        assert!(matches!(
            runner
                .run_source("return string_from_chars([\"h\", \"é\"])")
                .unwrap()
                .outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("hé")
        ));
        assert!(matches!(
            runner.run_source("return lower(\"North\")").unwrap().outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("north")
        ));
    }

    #[test]
    fn runner_string_filein_installs_primitive_prototype_verbs() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/string.mica"))
            .unwrap();

        assert!(matches!(
            runner.run_source("return trim(\"  hello  \")").unwrap().outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("hello")
        ));
        assert!(matches!(
            runner.run_source("return split(\"a  b\")").unwrap().outcome,
            TaskOutcome::Complete { value, .. }
                if value == Value::list([Value::string("a"), Value::string("b")])
        ));
        assert!(matches!(
            runner
                .run_source("return join([\"a\", \"b\"], \"-\")")
                .unwrap()
                .outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("a-b")
        ));
        assert!(matches!(
            runner
                .run_source("return strip_prefix(\"north\", \"no\")")
                .unwrap()
                .outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("rth")
        ));
    }

    #[test]
    fn runner_submit_source_as_exposes_context_and_drains_emissions() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:GrantEffect, 1)").unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("assert GrantEffect(#alice)").unwrap();
        let actor = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0001).unwrap();

        let submitted = runner
            .submit_source_as(actor, endpoint, "emit(#endpoint, \"hello\")")
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("hello")
        ));
        let emissions = runner.drain_emissions();
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].task_id, submitted.task_id);
        assert_eq!(emissions[0].target, endpoint);
        assert_eq!(emissions[0].value, Value::string("hello"));
        assert!(runner.drain_emissions().is_empty());
    }

    #[test]
    fn runner_submit_invocation_as_adds_actor_and_endpoint_roles() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/capabilities.mica"))
            .unwrap();
        let actor = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0002).unwrap();
        let lamp = runner.actor_identity(Symbol::intern("lamp")).unwrap();

        let submitted = runner
            .submit_invocation_as(
                actor,
                endpoint,
                Symbol::intern("polish"),
                vec![(Symbol::intern("item"), Value::identity(lamp))],
            )
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("polished brass lamp")
        ));
        let emissions = runner.drain_emissions();
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].task_id, submitted.task_id);
        assert_eq!(emissions[0].target, actor);
    }

    #[test]
    fn shared_runner_executes_invocations_from_multiple_threads() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_identity(:player)\n\
                 make_identity(:alice)\n\
                 make_relation(:Delegates, 3)\n\
                 assert Delegates(#alice, #player, 0)\n\
                 verb count_up(actor @ #player, count)\n\
                   let i = 0\n\
                   while i < count\n\
                     i = i + 1\n\
                   end\n\
                   return i\n\
                 end\n",
            )
            .unwrap();
        let actor = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let completed_before = runner.task_manager.completed_len();
        let runner = Arc::new(runner.into_shared());

        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for worker in 0..4 {
                let runner = Arc::clone(&runner);
                handles.push(scope.spawn(move || {
                    for _ in 0..10 {
                        let submitted = runner
                            .submit_invocation(TaskRequest {
                                principal: None,
                                actor: None,
                                endpoint: Identity::new(0x00ee_2000_0000_0000 + worker).unwrap(),
                                authority: AuthorityContext::root(),
                                input: TaskInput::Invocation {
                                    selector: Symbol::intern("count_up"),
                                    roles: vec![
                                        (Symbol::intern("actor"), Value::identity(actor)),
                                        (Symbol::intern("count"), Value::int(100).unwrap()),
                                    ],
                                },
                            })
                            .unwrap();
                        assert!(matches!(
                            submitted.outcome,
                            TaskOutcome::Complete { value, .. } if value == Value::int(100).unwrap()
                        ));
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
        });

        assert_eq!(runner.completed_len(), completed_before + 40);
    }

    #[test]
    fn shared_runner_reads_endpoint_transient_state_from_multiple_threads() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        for worker in 0..4 {
            runner
                .open_endpoint(
                    Identity::new(0x00ee_2100_0000_0000 + worker).unwrap(),
                    Some(alice),
                    Symbol::intern("telnet"),
                )
                .unwrap();
        }
        let runner = Arc::new(runner.into_shared());

        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for worker in 0..4 {
                let runner = Arc::clone(&runner);
                handles.push(scope.spawn(move || {
                    let endpoint = Identity::new(0x00ee_2100_0000_0000 + worker).unwrap();
                    for _ in 0..10 {
                        let request = runner
                            .source_request_for_endpoint(
                                endpoint,
                                "return EndpointActor(endpoint(), #alice)",
                            )
                            .unwrap();
                        let request = TaskRequest {
                            authority: AuthorityContext::root(),
                            ..request
                        };
                        let submitted = runner.submit_source(request).unwrap();
                        assert!(matches!(
                            submitted.outcome,
                            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
                        ));
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
        });
    }

    #[test]
    fn runner_dispatch_binds_unrestricted_method_params() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/mud-core.mica"))
            .unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();

        let report = runner
            .run_source("return :say(actor: #alice, message: \"hello\")")
            .unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        let emissions = runner.drain_emissions();
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].target, alice);
        assert_eq!(emissions[0].value, Value::string("hello"));
    }

    #[test]
    fn runner_mud_command_parser_runs_in_mica() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/mud-core.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../examples/string.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../examples/mud-command-parser.mica"))
            .unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = SYSTEM_ENDPOINT;

        let report = runner
            .run_source("return :command(actor: #alice, endpoint: #endpoint, line: \"say hello\")")
            .unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        let emissions = runner.drain_emissions();
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].target, alice);
        assert_eq!(emissions[0].value, Value::string("hello"));

        let report = runner
            .run_source("return :command(actor: #alice, endpoint: #endpoint, line: \"dance\")")
            .unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(false)
        ));
        let emissions = runner.drain_emissions();
        assert_eq!(emissions.len(), 1);
        assert_eq!(emissions[0].target, endpoint);
        assert_eq!(
            emissions[0].value,
            Value::string("I do not understand that.")
        );
    }

    #[test]
    fn runner_resume_task_uses_continuation_request_authority() {
        let mut runner = SourceRunner::new_empty();
        let program = Arc::new(
            Program::new(
                0,
                [
                    Instruction::Suspend {
                        kind: SuspendKind::TimedMillis(1),
                    },
                    Instruction::Return {
                        value: Operand::Value(Value::bool(true)),
                    },
                ],
            )
            .unwrap(),
        );
        let (task_id, first) = runner.task_manager.submit(program).unwrap();
        assert!(matches!(first, TaskOutcome::Suspended { .. }));

        let outcome = runner
            .resume_task(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Continuation {
                    task_id,
                    value: Value::nothing(),
                },
            })
            .unwrap();

        assert!(matches!(
            outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
    }

    #[test]
    fn runner_suspend_returns_continuation_value() {
        let mut runner = SourceRunner::new_empty();
        let submitted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return suspend()".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Never,
                ..
            }
        ));

        let outcome = runner
            .resume_task(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Continuation {
                    task_id: submitted.task_id,
                    value: Value::string("awake"),
                },
            })
            .unwrap();

        assert!(matches!(
            outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("awake")
        ));
    }

    #[test]
    fn runner_commit_yields_and_resumes_with_nothing() {
        let mut runner = SourceRunner::new_empty();
        let submitted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return commit()".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Commit,
                ..
            }
        ));

        let outcome = runner
            .resume_task(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Continuation {
                    task_id: submitted.task_id,
                    value: Value::nothing(),
                },
            })
            .unwrap();

        assert!(matches!(
            outcome,
            TaskOutcome::Complete { value, .. } if value == Value::nothing()
        ));
    }

    #[test]
    fn runner_tasks_builtin_lists_running_and_suspended_tasks() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("suspend(10)").unwrap();
        let report = runner.run_source("return tasks()").unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("tasks() did not complete");
        };
        let tasks = value.with_list(<[Value]>::to_vec).unwrap();

        assert!(
            tasks
                .iter()
                .any(|task| task_status(task) == Some((1, Symbol::intern("suspended"))))
        );
        assert!(
            tasks
                .iter()
                .any(|task| task_status(task) == Some((2, Symbol::intern("running"))))
        );
    }

    #[test]
    fn runner_context_builtins_return_runtime_identities() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0003).unwrap();

        let submitted = runner
            .submit_source(TaskRequest {
                principal: Some(alice),
                actor: Some(alice),
                endpoint,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return [principal(), actor(), endpoint()]".to_owned()),
            })
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![
                        Value::identity(alice),
                        Value::identity(alice),
                        Value::identity(endpoint),
                    ]
        ));
    }

    #[test]
    fn runner_context_builtins_return_system_endpoint_without_actor_context() {
        let mut runner = SourceRunner::new_empty();
        let report = runner
            .run_source("return [principal(), actor(), endpoint()]")
            .unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![
                        Value::nothing(),
                        Value::nothing(),
                        Value::identity(SYSTEM_ENDPOINT),
                    ]
        ));
    }

    #[test]
    fn runner_transient_facts_are_visible_to_runtime_scopes() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_relation(:Selected, 1)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let lamp = runner.actor_identity(Symbol::intern("lamp")).unwrap();

        let inserted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source(
                    "assert_transient(#alice, :Selected, [#lamp])\n\
                     return Selected(?item)"
                        .to_owned(),
                ),
            })
            .unwrap();

        assert!(matches!(
            inserted.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![Value::map([(Value::symbol(Symbol::intern("item")), Value::identity(lamp))])]
        ));

        let visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return Selected(?item)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            visible.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![Value::map([(Value::symbol(Symbol::intern("item")), Value::identity(lamp))])]
        ));

        let root = runner.run_source("return Selected(?item)").unwrap();
        assert!(matches!(
            root.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));
    }

    #[test]
    fn runner_transient_retract_and_scope_drop_update_visibility() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:bob)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_relation(:Selected, 1)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let bob = runner.actor_identity(Symbol::intern("bob")).unwrap();

        for scope in [alice, bob] {
            runner
                .submit_source(TaskRequest {
                    principal: None,
                    actor: Some(scope),
                    endpoint: SYSTEM_ENDPOINT,
                    authority: AuthorityContext::root(),
                    input: TaskInput::Source(
                        "return assert_transient(actor(), :Selected, [#lamp])".to_owned(),
                    ),
                })
                .unwrap();
        }

        let retracted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source(
                    "return retract_transient(#alice, :Selected, [#lamp])".to_owned(),
                ),
            })
            .unwrap();
        assert!(matches!(
            retracted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));

        let alice_visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return Selected(?item)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            alice_visible.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));

        let dropped = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(bob),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return drop_transient_scope(#bob)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            dropped.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(1).unwrap()
        ));

        let bob_visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(bob),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return Selected(?item)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            bob_visible.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));
    }

    #[test]
    fn runner_can_drop_own_transient_scope_without_admin_authority() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_relation(:Selected, 1)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();

        runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source(
                    "return assert_transient(#alice, :Selected, [#lamp])".to_owned(),
                ),
            })
            .unwrap();

        let dropped = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::empty(),
                input: TaskInput::Source("return drop_transient_scope(#alice)".to_owned()),
            })
            .unwrap();

        assert!(matches!(
            dropped.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(1).unwrap()
        ));

        let visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return Selected(?item)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            visible.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));
    }

    #[test]
    fn runner_cannot_drop_another_actor_transient_scope_without_admin_authority() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:bob)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_relation(:Selected, 1)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let bob = runner.actor_identity(Symbol::intern("bob")).unwrap();
        let lamp = runner.actor_identity(Symbol::intern("lamp")).unwrap();

        runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(bob),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source(
                    "return assert_transient(#bob, :Selected, [#lamp])".to_owned(),
                ),
            })
            .unwrap();

        let denied = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::empty(),
                input: TaskInput::Source("return drop_transient_scope(#bob)".to_owned()),
            })
            .unwrap_err();
        assert!(format!("{denied:?}").contains("PermissionDenied"));

        let visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(bob),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return Selected(?item)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            visible.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![Value::map([(Value::symbol(Symbol::intern("item")), Value::identity(lamp))])]
        ));
    }

    #[test]
    fn runner_derived_relations_can_read_transient_facts() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_relation(:Selected, 1)").unwrap();
        runner.run_source("make_relation(:Visible, 1)").unwrap();
        runner
            .run_source("Visible(item) :- Selected(item)")
            .unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let lamp = runner.actor_identity(Symbol::intern("lamp")).unwrap();

        let visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source(
                    "assert_transient(#alice, :Selected, [#lamp])\n\
                     return Visible(?item)"
                        .to_owned(),
                ),
            })
            .unwrap();

        assert!(matches!(
            visible.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![Value::map([(Value::symbol(Symbol::intern("item")), Value::identity(lamp))])]
        ));
    }

    #[test]
    fn runner_endpoint_facts_are_transient_and_endpoint_scoped() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0010).unwrap();
        runner
            .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
            .unwrap();

        let visible = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return EndpointActor(?endpoint, #alice)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            visible.outcome,
            TaskOutcome::Complete { value, .. }
                if value.with_list(<[Value]>::to_vec).unwrap()
                    == vec![Value::map([(Value::symbol(Symbol::intern("endpoint")), Value::identity(endpoint))])]
        ));

        let root = runner
            .run_source("return EndpointActor(?endpoint, #alice)")
            .unwrap();
        assert!(matches!(
            root.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));

        assert_eq!(runner.close_endpoint(endpoint), 4);
        let closed = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: Some(alice),
                endpoint,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return EndpointActor(?endpoint, #alice)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            closed.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::list([])
        ));
    }

    #[test]
    fn runner_assume_actor_requires_principal_specific_policy() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:account)").unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:bob)").unwrap();
        runner
            .run_source("make_relation(:CanAssumeActor, 2)")
            .unwrap();
        let account = runner.actor_identity(Symbol::intern("account")).unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let bob = runner.actor_identity(Symbol::intern("bob")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0012).unwrap();
        runner
            .open_endpoint_with_context(
                endpoint,
                Some(account),
                Some(alice),
                Symbol::intern("telnet"),
            )
            .unwrap();

        let denied_request = runner
            .source_request_for_endpoint(endpoint, "return assume_actor(#bob)")
            .unwrap();
        let denied = runner.submit_source(denied_request).unwrap_err();
        assert!(format!("{denied:?}").contains("PermissionDenied"));

        runner
            .run_source("assert CanAssumeActor(#account, #bob)")
            .unwrap();
        let allowed_request = runner
            .source_request_for_endpoint(endpoint, "return assume_actor(#bob)")
            .unwrap();
        let switched = runner.submit_source(allowed_request).unwrap();
        assert!(matches!(
            switched.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(bob)
        ));

        let actor_request = runner
            .source_request_for_endpoint(endpoint, "return actor()")
            .unwrap();
        let actor = runner.submit_source(actor_request).unwrap();
        assert!(matches!(
            actor.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(bob)
        ));
    }

    #[test]
    fn runner_endpoint_actor_cannot_be_rebound_by_raw_transient_write() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:account)").unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:bob)").unwrap();
        let account = runner.actor_identity(Symbol::intern("account")).unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0013).unwrap();
        runner
            .open_endpoint_with_context(
                endpoint,
                Some(account),
                Some(alice),
                Symbol::intern("telnet"),
            )
            .unwrap();

        let request = runner
            .source_request_for_endpoint(
                endpoint,
                "return assert_transient(endpoint(), :EndpointActor, [endpoint(), #bob])",
            )
            .unwrap();
        let denied = runner.submit_source(request).unwrap_err();
        assert!(format!("{denied:?}").contains("PermissionDenied"));

        let actor_request = runner
            .source_request_for_endpoint(endpoint, "return actor()")
            .unwrap();
        let actor = runner.submit_source(actor_request).unwrap();
        assert!(matches!(
            actor.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(alice)
        ));
    }

    #[test]
    fn runner_routes_actor_effect_targets_to_open_endpoints() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = runner.actor_identity(Symbol::intern("alice")).unwrap();
        let endpoint = Identity::new(0x00ee_0000_0000_0011).unwrap();

        assert_eq!(runner.route_effect_targets(alice), vec![alice]);
        runner
            .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
            .unwrap();
        assert_eq!(runner.route_effect_targets(alice), vec![endpoint]);
        assert_eq!(runner.route_effect_targets(endpoint), vec![endpoint]);

        runner.close_endpoint(endpoint);
        assert_eq!(runner.route_effect_targets(alice), vec![alice]);
    }

    #[test]
    fn runner_destroy_identity_retracts_subject_facts_and_name_binding() {
        let mut runner = SourceRunner::new_empty();
        let thing = runner.run_source("return make_identity(:thing)").unwrap();
        let TaskOutcome::Complete {
            value: thing_value, ..
        } = thing.outcome
        else {
            panic!("make_identity did not complete");
        };
        let thing = thing_value.as_identity().unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Object, 1)").unwrap();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("assert Object(#thing)").unwrap();
        runner.run_source("assert Object(#room)").unwrap();
        runner
            .run_source("assert LocatedIn(#thing, #room)")
            .unwrap();
        runner
            .run_source("assert LocatedIn(#room, #thing)")
            .unwrap();

        let destroyed = runner
            .run_source("return destroy_identity(#thing)")
            .unwrap();

        assert!(matches!(
            destroyed.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(3).unwrap()
        ));
        let snapshot = runner.task_manager.kernel().snapshot();
        assert!(
            snapshot
                .subject_facts(&Value::identity(thing))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            snapshot
                .mentioned_facts(&Value::identity(thing))
                .unwrap()
                .len(),
            1
        );
        assert!(
            format!("{:?}", runner.run_source("return #thing").unwrap_err())
                .contains("UnknownIdentity")
        );
        assert!(runner.run_source("return #room").is_ok());
    }

    #[test]
    fn runner_read_waits_for_input_and_returns_continuation_value() {
        let mut runner = SourceRunner::new_empty();
        let submitted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return read(:line)".to_owned()),
            })
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::WaitingForInput(value),
                ..
            } if value == Value::symbol(Symbol::intern("line"))
        ));

        let outcome = runner
            .resume_task(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Continuation {
                    task_id: submitted.task_id,
                    value: Value::string("look"),
                },
            })
            .unwrap();

        assert!(matches!(
            outcome,
            TaskOutcome::Complete { value, .. } if value == Value::string("look")
        ));
    }

    #[test]
    fn runner_suspend_seconds_becomes_timed_suspend() {
        let mut runner = SourceRunner::new_empty();
        let submitted = runner
            .submit_source(TaskRequest {
                principal: None,
                actor: None,
                endpoint: SYSTEM_ENDPOINT,
                authority: AuthorityContext::root(),
                input: TaskInput::Source("return suspend(0.5)".to_owned()),
            })
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::TimedMillis(500),
                ..
            }
        ));
    }

    #[test]
    fn runner_aborts_on_divide_by_zero_before_builtin_effect() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:target)").unwrap();
        let report = runner.run_source("return emit(#target, 1 / 0)").unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Aborted { error, effects, .. }
                if error.error_code_symbol() == Some(Symbol::intern("E_DIV"))
                    && effects.is_empty()
        ));
    }

    fn task_status(value: &Value) -> Option<(i64, Symbol)> {
        let id = value
            .map_get(&Value::symbol(Symbol::intern("id")))?
            .as_int()?;
        let state = value
            .map_get(&Value::symbol(Symbol::intern("state")))?
            .as_symbol()?;
        Some((id, state))
    }

    #[test]
    fn runner_make_relation_refreshes_compile_context() {
        let mut runner = SourceRunner::new_empty();
        let made = runner.run_source("return make_relation(:Hog, 1)").unwrap();
        assert_eq!(
            made.render(),
            "task 1 complete: relation(:Hog) (retries: 0)"
        );
        let relation = match made.outcome {
            TaskOutcome::Complete { value, .. } => value.as_identity().unwrap(),
            other => panic!("unexpected make_relation outcome: {other:?}"),
        };

        let asserted = runner.run_source("assert Hog(1)\nreturn true").unwrap();

        assert!(matches!(
            asserted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        assert_eq!(
            runner
                .task_manager
                .kernel()
                .snapshot()
                .scan(relation, &[Some(Value::int(1).unwrap())])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn runner_make_relation_is_idempotent_for_matching_arity() {
        let mut runner = SourceRunner::new_empty();
        let first = runner.run_source("return make_relation(:Hog, 1)").unwrap();
        let second = runner.run_source("return make_relation(:Hog, 1)").unwrap();

        assert!(matches!(
            (first.outcome, second.outcome),
            (
                TaskOutcome::Complete { value: first, .. },
                TaskOutcome::Complete { value: second, .. }
            ) if first == second
        ));
    }

    #[test]
    fn runner_make_identity_refreshes_compile_context() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:Object, 1)").unwrap();
        let made = runner.run_source("return make_identity(:root)").unwrap();
        let root = match made.outcome {
            TaskOutcome::Complete { value, .. } => value.as_identity().unwrap(),
            other => panic!("unexpected make_identity outcome: {other:?}"),
        };

        let asserted = runner
            .run_source("assert Object(#root)\nreturn true")
            .unwrap();

        assert!(matches!(
            asserted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        assert_eq!(
            runner
                .task_manager
                .kernel()
                .snapshot()
                .scan(
                    runner.context.relation("Object").unwrap(),
                    &[Some(Value::identity(root))]
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn report_renders_named_identities_in_values_and_effects() {
        let mut runner = SourceRunner::new_empty();
        let made = runner.run_source("return make_identity(:thing)").unwrap();
        let report = runner
            .run_source("return emit(#thing, [#thing, {:owner -> #thing}])")
            .unwrap();

        assert_eq!(made.render(), "task 1 complete: #thing (retries: 0)");
        assert_eq!(
            report.render(),
            "task 2 complete: [#thing, [:owner: #thing]] (retries: 0)\neffect #thing: [#thing, [:owner: #thing]]"
        );
    }

    #[test]
    fn runner_relation_calls_with_query_vars_return_binding_maps() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location(#thing, #room)").unwrap();

        let report = runner.run_source("return Location(#thing, ?room)").unwrap();

        assert_eq!(
            report.render(),
            "task 5 complete: [[:room: #room]] (retries: 0)"
        );
    }

    #[test]
    fn runner_relation_queries_allow_all_positions_free() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location(#thing, #room)").unwrap();

        let report = runner.run_source("return Location(?what, ?where)").unwrap();

        assert_eq!(
            report.render(),
            "task 5 complete: [[:what: #thing, :where: #room]] (retries: 0)"
        );
    }

    #[test]
    fn runner_one_and_dot_read_project_binary_relations() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location(#thing, #room)").unwrap();

        let one = runner
            .run_source("return one Location(#thing, ?room)")
            .unwrap();
        let dot = runner.run_source("return #thing.location").unwrap();

        assert_eq!(one.render(), "task 5 complete: #room (retries: 0)");
        assert_eq!(dot.render(), "task 6 complete: #room (retries: 0)");
    }

    #[test]
    fn runner_installs_relation_rules_and_queries_derived_tuples() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        let rule = runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn(#alice, #room)")
            .unwrap();
        runner.run_source("assert LocatedIn(#lamp, #room)").unwrap();

        let query = runner.run_source("return VisibleTo(#alice, ?obj)").unwrap();

        assert_eq!(rule.render(), "task 3 complete: #rule1 (retries: 0)");
        assert_eq!(
            query.render(),
            "task 9 complete: [[:obj: #alice], [:obj: #lamp]] (retries: 0)"
        );
    }

    #[test]
    fn runner_inspects_and_disables_rules() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn(#alice, #room)")
            .unwrap();
        runner.run_source("assert LocatedIn(#lamp, #room)").unwrap();

        let rules = runner.run_source("return rules(:VisibleTo)").unwrap();
        let source = runner
            .run_source("return describe_rule(one rules(:VisibleTo))")
            .unwrap();
        let disabled = runner
            .run_source("disable_rule(one rules(:VisibleTo))")
            .unwrap();
        let query = runner.run_source("return VisibleTo(#alice, ?obj)").unwrap();

        assert_eq!(rules.render(), "task 9 complete: [#rule1] (retries: 0)");
        assert_eq!(
            source.render(),
            "task 10 complete: \"VisibleTo(actor, obj) :-\\n  LocatedIn(actor, room),\\n  LocatedIn(obj, room)\" (retries: 0)"
        );
        assert_eq!(disabled.render(), "task 11 complete: nothing (retries: 0)");
        assert_eq!(query.render(), "task 12 complete: [] (retries: 0)");
    }

    #[test]
    fn runner_fileouts_active_rules() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source("VisibleTo(actor, obj) :- LocatedIn(actor, obj)")
            .unwrap();

        let fileout = runner
            .run_source("return fileout_rules(:VisibleTo)")
            .unwrap();

        assert_eq!(
            fileout.render(),
            "task 4 complete: \"VisibleTo(actor, obj) :- LocatedIn(actor, obj)\" (retries: 0)"
        );

        let TaskOutcome::Complete { value, .. } = fileout.outcome else {
            panic!("expected fileout to complete");
        };
        let source = value.with_str(str::to_owned).unwrap();
        let mut imported = SourceRunner::new_empty();
        imported.run_source("make_relation(:LocatedIn, 2)").unwrap();
        imported.run_source("make_relation(:VisibleTo, 2)").unwrap();
        let installed = imported.run_source(&source).unwrap();
        assert_eq!(installed.render(), "task 3 complete: #rule1 (retries: 0)");
    }

    #[test]
    fn runner_filein_unit_fileout_round_trips_readable_source() {
        let mut runner = SourceRunner::new_empty();
        let unit = Symbol::intern("mud_core");
        runner
            .run_filein_with_unit(
                unit,
                "make_identity(:lamp)\n\
                 make_identity(:room)\n\
                 make_relation(:Name, 2)\n\
                 make_relation(:LocatedIn, 2)\n\
                 make_relation(:VisibleTo, 2)\n\
                 assert Name(#lamp, \"brass lamp\")\n\
                 assert LocatedIn(#lamp, #room)\n\
                 VisibleTo(actor, obj) :- LocatedIn(obj, actor)\n\
                 verb look(actor @ #room)\n\
                   return \"ok\"\n\
                 end\n",
                FileinMode::Add,
            )
            .unwrap();

        let source = runner.fileout_unit(unit).unwrap();

        assert!(source.contains("make_identity(:lamp)"));
        assert!(source.contains("make_relation(:Name, 2)"));
        assert!(source.contains("assert Name(#lamp, \"brass lamp\")"));
        assert!(source.contains("VisibleTo(actor, obj) :- LocatedIn(obj, actor)"));
        assert!(source.contains("verb look(actor @ #room)"));

        let mut imported = SourceRunner::new_empty();
        imported
            .run_filein_with_unit(unit, &source, FileinMode::Add)
            .unwrap();
        let query = imported.run_source("return Name(#lamp, ?name)").unwrap();
        let dispatch = imported.run_source("return :look(actor: #room)").unwrap();
        assert!(query.render().contains("[[:name: \"brass lamp\"]]"));
        assert!(dispatch.render().contains("\"ok\""));
    }

    #[test]
    fn runner_filein_replace_removes_facts_no_longer_in_source_unit() {
        let mut runner = SourceRunner::new_empty();
        let unit = Symbol::intern("mud_core");
        runner
            .run_filein_with_unit(
                unit,
                "make_identity(:lamp)\n\
                 make_relation(:Name, 2)\n\
                 assert Name(#lamp, \"brass lamp\")\n",
                FileinMode::Add,
            )
            .unwrap();
        runner
            .run_filein_with_unit(
                unit,
                "make_identity(:lamp)\n\
                 make_relation(:Name, 2)\n\
                 assert Name(#lamp, \"golden lamp\")\n",
                FileinMode::Replace,
            )
            .unwrap();

        let query = runner.run_source("return Name(#lamp, ?name)").unwrap();
        let source = runner.fileout_unit(unit).unwrap();

        assert!(query.render().contains("[[:name: \"golden lamp\"]]"));
        assert!(source.contains("assert Name(#lamp, \"golden lamp\")"));
        assert!(!source.contains("brass lamp"));
    }

    #[test]
    fn runner_fjall_store_reopens_state() {
        let path = std::env::temp_dir().join(format!(
            "mica-runtime-fjall-{}-{}",
            std::process::id(),
            Symbol::intern("runner_fjall_store_reopens_state").id()
        ));
        let _ = std::fs::remove_dir_all(&path);

        {
            let mut runner =
                SourceRunner::open_fjall(&path, mica_relation_kernel::FjallDurabilityMode::Strict)
                    .unwrap();
            runner.run_source("make_identity(:lamp)").unwrap();
            runner.run_source("make_relation(:Name, 2)").unwrap();
            runner
                .run_source("assert Name(#lamp, \"brass lamp\")")
                .unwrap();
        }

        {
            let mut runner =
                SourceRunner::open_fjall(&path, mica_relation_kernel::FjallDurabilityMode::Strict)
                    .unwrap();
            let query = runner.run_source("return Name(#lamp, ?name)").unwrap();
            assert!(query.render().contains("[[:name: \"brass lamp\"]]"));
        }

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn runner_installs_rules_with_surface_negation() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:HiddenFrom, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room),\n  not HiddenFrom(obj, actor)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn(#alice, #room)")
            .unwrap();
        runner.run_source("assert LocatedIn(#lamp, #room)").unwrap();
        runner
            .run_source("assert HiddenFrom(#lamp, #alice)")
            .unwrap();

        let query = runner.run_source("return VisibleTo(#alice, ?obj)").unwrap();

        assert_eq!(
            query.render(),
            "task 11 complete: [[:obj: #alice]] (retries: 0)"
        );
    }

    #[test]
    fn runner_filein_installs_mud_verbs_and_invokes_dispatch() {
        let mut runner = SourceRunner::new_empty();
        let reports = runner
            .run_filein(
                "make_identity(:player)\n\
                 make_identity(:thing)\n\
                 make_identity(:portable)\n\
                 make_identity(:container)\n\
                 make_identity(:alice)\n\
                 make_identity(:coin)\n\
                 make_identity(:box)\n\
                 make_relation(:Delegates, 3)\n\
                 make_relation(:HeldBy, 2)\n\
                 make_relation(:In, 2)\n\
                 make_relation(:Portable, 1)\n\
                 make_relation(:CanSee, 2)\n\
                 assert Delegates(#portable, #thing, 0)\n\
                 assert Delegates(#coin, #portable, 0)\n\
                 assert Delegates(#alice, #player, 0)\n\
                 assert Delegates(#box, #container, 0)\n\
                 assert Portable(#coin)\n\
                 CanSee(actor, item) :-\n\
                   HeldBy(actor, item)\n\
                 CanSee(actor, item) :-\n\
                   HeldBy(actor, container),\n\
                   In(item, container)\n\
                 verb get(actor @ #player, item @ #thing)\n\
                   if Portable(item)\n\
                     assert HeldBy(actor, item)\n\
                     return true\n\
                   else\n\
                     return false\n\
                   end\n\
                 end\n\
                 verb put(actor @ #player, item @ #thing, container @ #container)\n\
                   if HeldBy(actor, item)\n\
                     assert In(item, container)\n\
                     return true\n\
                   else\n\
                     return false\n\
                   end\n\
                 end\n\
                 :get(item: #coin, actor: #alice)\n\
                 :put(container: #box, item: #coin, actor: #alice)\n\
                 return In(#coin, ?container)\n\
                 return CanSee(#alice, ?item)\n",
            )
            .unwrap();

        assert_eq!(
            reports[17].render(),
            "task 18 complete: #rule1 (retries: 0)"
        );
        assert_eq!(
            reports[18].render(),
            "task 19 complete: #rule2 (retries: 0)"
        );
        assert_eq!(
            reports[19].render(),
            "task 20 complete: #verb_get_1 (retries: 0)"
        );
        assert_eq!(
            reports[20].render(),
            "task 21 complete: #verb_put_2 (retries: 0)"
        );
        assert_eq!(reports[21].render(), "task 22 complete: true (retries: 0)");
        assert_eq!(reports[22].render(), "task 23 complete: true (retries: 0)");
        assert_eq!(
            reports[23].render(),
            "task 24 complete: [[:container: #box]] (retries: 0)"
        );
        assert_eq!(
            reports[24].render(),
            "task 25 complete: [[:item: #coin]] (retries: 0)"
        );
    }

    #[test]
    fn runner_make_identity_is_idempotent_for_matching_name() {
        let mut runner = SourceRunner::new_empty();
        let first = runner.run_source("return make_identity(:root)").unwrap();
        let second = runner.run_source("return make_identity(:root)").unwrap();

        assert!(matches!(
            (first.outcome, second.outcome),
            (
                TaskOutcome::Complete { value: first, .. },
                TaskOutcome::Complete { value: second, .. }
            ) if first == second
        ));
    }

    #[test]
    fn runner_mints_actor_authority_from_policy_facts() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/capabilities.mica"))
            .unwrap();

        let alice = runner
            .run_source_as(
                Symbol::intern("alice"),
                ":polish(actor: #alice, item: #lamp)",
            )
            .unwrap();
        assert!(alice.render().contains("complete: \"polished brass lamp\""));
        assert!(
            alice
                .render()
                .contains("effect #alice: [\"polished\", #alice, #lamp]")
        );

        let bob_read = runner
            .run_source_as(Symbol::intern("bob"), "return #lamp.name")
            .unwrap();
        assert!(
            bob_read
                .render()
                .contains("complete: \"polished brass lamp\"")
        );

        let bob_write = runner
            .run_source_as(Symbol::intern("bob"), "#lamp.name = \"stolen\"")
            .unwrap_err();
        assert!(format!("{bob_write:?}").contains("PermissionDenied"));
        assert!(format!("{bob_write:?}").contains("operation: \"write\""));

        let bob_dispatch = runner
            .run_source_as(Symbol::intern("bob"), ":polish(actor: #bob, item: #lamp)")
            .unwrap_err();
        assert!(format!("{bob_dispatch:?}").contains("NoApplicableMethod"));

        let bob_catalog = runner
            .run_source_as(Symbol::intern("bob"), "make_relation(:Escape, 1)")
            .unwrap_err();
        assert!(format!("{bob_catalog:?}").contains("operation: \"grant\""));

        runner
            .run_source("retract HasRole(#alice, #builder)")
            .unwrap();
        runner
            .run_source("assert HasRole(#alice, #visitor)")
            .unwrap();

        let alice_read_after_role_change = runner
            .run_source_as(Symbol::intern("alice"), "return #lamp.name")
            .unwrap();
        assert!(
            alice_read_after_role_change
                .render()
                .contains("complete: \"polished brass lamp\"")
        );

        let alice_dispatch_after_role_change = runner
            .run_source_as(
                Symbol::intern("alice"),
                ":polish(actor: #alice, item: #lamp)",
            )
            .unwrap_err();
        assert!(format!("{alice_dispatch_after_role_change:?}").contains("NoApplicableMethod"));
    }

    #[test]
    fn runner_keeps_direct_grant_facts_as_policy_fallback() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_identity(:bob)\n\
                 make_relation(:Name, 2)\n\
                 make_relation(:GrantRead, 2)\n\
                 assert Name(#bob, \"Bob\")\n\
                 assert GrantRead(#bob, :Name)\n",
            )
            .unwrap();

        let bob_read = runner
            .run_source_as(Symbol::intern("bob"), "return #bob.name")
            .unwrap();
        assert!(bob_read.render().contains("complete: \"Bob\""));

        let bob_write = runner
            .run_source_as(Symbol::intern("bob"), "#bob.name = \"Robert\"")
            .unwrap_err();
        assert!(format!("{bob_write:?}").contains("PermissionDenied"));
        assert!(format!("{bob_write:?}").contains("operation: \"write\""));
    }

    #[test]
    fn runner_filein_ignores_comment_only_chunks() {
        let mut runner = SourceRunner::new_empty();
        let reports = runner
            .run_filein(
                "// one comment\n\
                 // another comment\n\
                 make_identity(:root)\n\
                 // trailing comment\n",
            )
            .unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].render(), "task 1 complete: #root (retries: 0)");
    }

    #[test]
    fn runner_fileout_preserves_functional_relation_declarations() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein_with_unit(
                Symbol::intern("schema"),
                "make_functional_relation(:Name, 2, [0])",
                FileinMode::Add,
            )
            .unwrap();

        let source = runner.fileout_unit(Symbol::intern("schema")).unwrap();

        assert!(source.contains("make_functional_relation(:Name, 2, [0])"));
    }

    #[test]
    fn report_renders_task_outcome() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return true").unwrap();

        assert_eq!(report.render(), "task 1 complete: true (retries: 0)");
    }
}
