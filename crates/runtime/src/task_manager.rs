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
    Task, TaskError, TaskId, TaskLimits, TaskOutcome, endpoint_actor_relation,
    endpoint_open_relation, endpoint_protocol_relation, endpoint_relation,
    endpoint_relation_metadata,
};
use mica_relation_kernel::{
    KernelError, RelationId, RelationKernel, RelationMetadata, TransientStore, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use mica_vm::{
    AuthorityContext, BuiltinRegistry, Emission, Program, ProgramResolver, RuntimeContext,
    SuspendKind,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskManagerError {
    UnknownTask(TaskId),
    TaskAlreadyCompleted(TaskId),
    Task(TaskError),
}

impl From<TaskError> for TaskManagerError {
    fn from(value: TaskError) -> Self {
        Self::Task(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Effect {
    pub task_id: TaskId,
    pub target: Identity,
    pub value: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EffectLog {
    effects: Vec<Effect>,
}

impl EffectLog {
    pub fn emit(&mut self, task_id: TaskId, effects: Vec<Emission>) {
        self.effects
            .extend(effects.into_iter().map(|effect| Effect {
                task_id,
                target: effect.target(),
                value: effect.value().clone(),
            }));
    }

    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    pub fn drain(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }
}

pub struct TaskManager {
    kernel: RelationKernel,
    next_task_id: TaskId,
    suspended: HashMap<TaskId, SuspendedTask>,
    completed: HashMap<TaskId, TaskOutcome>,
    effects: EffectLog,
    transient: TransientStore,
    limits: TaskLimits,
    resolver: Arc<ProgramResolver>,
    builtins: Arc<BuiltinRegistry>,
}

impl TaskManager {
    pub fn new(kernel: RelationKernel) -> Self {
        Self {
            kernel,
            next_task_id: 1,
            suspended: HashMap::new(),
            completed: HashMap::new(),
            effects: EffectLog::default(),
            transient: TransientStore::new(),
            limits: TaskLimits::default(),
            resolver: Arc::new(ProgramResolver::new()),
            builtins: Arc::new(BuiltinRegistry::new()),
        }
    }

    pub fn with_limits(mut self, limits: TaskLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_resolver(mut self, resolver: Arc<ProgramResolver>) -> Self {
        self.resolver = resolver;
        self
    }

    pub fn with_builtins(mut self, builtins: Arc<BuiltinRegistry>) -> Self {
        self.builtins = builtins;
        self
    }

    pub fn kernel(&self) -> &RelationKernel {
        &self.kernel
    }

    pub fn effects(&self) -> &EffectLog {
        &self.effects
    }

    pub fn effects_mut(&mut self) -> &mut EffectLog {
        &mut self.effects
    }

    pub fn transient(&self) -> &TransientStore {
        &self.transient
    }

    pub fn transient_mut(&mut self) -> &mut TransientStore {
        &mut self.transient
    }

    pub fn drain_emissions(&mut self) -> Vec<Effect> {
        self.effects.drain()
    }

    pub fn drain_routed_emissions(&mut self) -> Vec<Effect> {
        let effects = self.effects.drain();
        effects
            .into_iter()
            .flat_map(|effect| {
                self.route_effect_targets(effect.target)
                    .into_iter()
                    .map(move |target| Effect {
                        task_id: effect.task_id,
                        target,
                        value: effect.value.clone(),
                    })
            })
            .collect()
    }

    pub fn open_endpoint(
        &mut self,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), TaskManagerError> {
        self.assert_endpoint_fact(
            endpoint,
            endpoint_relation(),
            Tuple::from([Value::identity(endpoint)]),
        )?;
        if let Some(actor) = actor {
            self.assert_endpoint_fact(
                endpoint,
                endpoint_actor_relation(),
                Tuple::from([Value::identity(endpoint), Value::identity(actor)]),
            )?;
        }
        self.assert_endpoint_fact(
            endpoint,
            endpoint_protocol_relation(),
            Tuple::from([Value::identity(endpoint), Value::symbol(protocol)]),
        )?;
        self.assert_endpoint_fact(
            endpoint,
            endpoint_open_relation(),
            Tuple::from([Value::identity(endpoint)]),
        )?;
        Ok(())
    }

    pub fn close_endpoint(&mut self, endpoint: Identity) -> usize {
        self.transient.drop_scope(endpoint)
    }

    pub fn route_effect_targets(&self, target: Identity) -> Vec<Identity> {
        self.route_effect_targets_result(target)
            .unwrap_or_else(|_| vec![target])
    }

    pub fn resolver(&self) -> &Arc<ProgramResolver> {
        &self.resolver
    }

    pub fn builtins(&self) -> &Arc<BuiltinRegistry> {
        &self.builtins
    }

    pub fn submit(
        &mut self,
        program: Arc<Program>,
    ) -> Result<(TaskId, TaskOutcome), TaskManagerError> {
        self.submit_with_authority(program, AuthorityContext::root())
    }

    pub fn submit_with_authority(
        &mut self,
        program: Arc<Program>,
        authority: AuthorityContext,
    ) -> Result<(TaskId, TaskOutcome), TaskManagerError> {
        self.submit_with_context(program, authority, RuntimeContext::default())
    }

    pub fn submit_with_context(
        &mut self,
        program: Arc<Program>,
        authority: AuthorityContext,
        runtime_context: RuntimeContext,
    ) -> Result<(TaskId, TaskOutcome), TaskManagerError> {
        let task_id = self.allocate_task_id();
        let task_snapshot = self.task_snapshot_values(Some(task_id));
        let mut task = Task::new_with_authority(
            task_id,
            &self.kernel,
            program,
            self.resolver.clone(),
            self.builtins.clone(),
            authority,
            self.limits,
        );
        task.set_task_snapshot(task_snapshot);
        task.set_runtime_context(runtime_context);
        let transient_scopes = transient_scopes(runtime_context);
        let outcome = task.run_with_transient(Some(&mut self.transient), &transient_scopes)?;
        let suspended_state = suspended_state(&outcome, &task);
        drop(task);
        self.record_outcome(task_id, outcome.clone(), suspended_state);
        Ok((task_id, outcome))
    }

    pub fn complete_immediate(&mut self, value: Value) -> (TaskId, TaskOutcome) {
        let task_id = self.allocate_task_id();
        let outcome = TaskOutcome::Complete {
            value,
            effects: Vec::new(),
            retries: 0,
        };
        self.record_outcome(task_id, outcome.clone(), None);
        (task_id, outcome)
    }

    pub fn resume_with_authority(
        &mut self,
        task_id: TaskId,
        authority: AuthorityContext,
    ) -> Result<TaskOutcome, TaskManagerError> {
        self.resume_with_value(task_id, authority, Value::nothing())
    }

    pub fn resume_with_value(
        &mut self,
        task_id: TaskId,
        authority: AuthorityContext,
        value: Value,
    ) -> Result<TaskOutcome, TaskManagerError> {
        self.resume_with_context(task_id, authority, value, RuntimeContext::default())
    }

    pub fn resume_with_context(
        &mut self,
        task_id: TaskId,
        authority: AuthorityContext,
        value: Value,
        runtime_context: RuntimeContext,
    ) -> Result<TaskOutcome, TaskManagerError> {
        if self.completed.contains_key(&task_id) {
            return Err(TaskManagerError::TaskAlreadyCompleted(task_id));
        }
        let suspended = self
            .suspended
            .remove(&task_id)
            .ok_or(TaskManagerError::UnknownTask(task_id))?;
        let task_snapshot = self.task_snapshot_values(Some(task_id));
        let mut task = Task::from_state_with_authority(
            task_id,
            &self.kernel,
            self.resolver.clone(),
            self.builtins.clone(),
            suspended.state,
            authority,
        );
        task.set_task_snapshot(task_snapshot);
        task.set_runtime_context(runtime_context);
        task.resume_with(value)?;
        let transient_scopes = transient_scopes(runtime_context);
        let outcome = task.run_with_transient(Some(&mut self.transient), &transient_scopes)?;
        let suspended_state = suspended_state(&outcome, &task);
        drop(task);
        self.record_outcome(task_id, outcome.clone(), suspended_state);
        Ok(outcome)
    }

    pub fn suspended(&self, task_id: TaskId) -> Option<&SuspendedTask> {
        self.suspended.get(&task_id)
    }

    pub fn completed(&self, task_id: TaskId) -> Option<&TaskOutcome> {
        self.completed.get(&task_id)
    }

    pub fn suspended_len(&self) -> usize {
        self.suspended.len()
    }

    pub fn completed_len(&self) -> usize {
        self.completed.len()
    }

    fn allocate_task_id(&mut self) -> TaskId {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        task_id
    }

    fn task_snapshot_values(&self, running: Option<TaskId>) -> Vec<Value> {
        let mut tasks = self
            .suspended
            .values()
            .map(|task| task_status_value(task.task_id, Symbol::intern("suspended")))
            .collect::<Vec<_>>();
        if let Some(task_id) = running {
            tasks.push(task_status_value(task_id, Symbol::intern("running")));
        }
        tasks.sort();
        tasks
    }

    fn record_outcome(
        &mut self,
        task_id: TaskId,
        outcome: TaskOutcome,
        suspended_state: Option<crate::task::TaskState>,
    ) {
        match &outcome {
            TaskOutcome::Complete { effects, .. } | TaskOutcome::Aborted { effects, .. } => {
                self.effects.emit(task_id, effects.clone());
                self.completed.insert(task_id, outcome);
            }
            TaskOutcome::Suspended { kind, effects, .. } => {
                self.effects.emit(task_id, effects.clone());
                self.suspended.insert(
                    task_id,
                    SuspendedTask {
                        task_id,
                        kind: kind.clone(),
                        state: suspended_state.expect("suspended task state is present"),
                    },
                );
            }
        }
    }

    fn assert_endpoint_fact(
        &mut self,
        scope: Identity,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), TaskManagerError> {
        let metadata = endpoint_metadata(relation).ok_or_else(|| {
            TaskManagerError::Task(TaskError::Runtime(mica_vm::RuntimeError::Kernel(
                KernelError::UnknownRelation(relation),
            )))
        })?;
        self.transient
            .assert(scope, metadata, tuple)
            .map(|_| ())
            .map_err(TaskError::from)
            .map_err(TaskManagerError::from)
    }

    fn route_effect_targets_result(&self, target: Identity) -> Result<Vec<Identity>, KernelError> {
        let scopes = self.transient.scopes().collect::<Vec<_>>();
        let mut endpoints = BTreeSet::new();
        for row in self.transient.scan(
            &scopes,
            endpoint_actor_relation(),
            &[None, Some(Value::identity(target))],
        )? {
            let Some(endpoint) = row.values().first().and_then(Value::as_identity) else {
                continue;
            };
            if self.endpoint_is_open(endpoint)? {
                endpoints.insert(endpoint);
            }
        }
        if self.endpoint_is_open(target)? {
            endpoints.insert(target);
        }
        if endpoints.is_empty() {
            Ok(vec![target])
        } else {
            Ok(endpoints.into_iter().collect())
        }
    }

    fn endpoint_is_open(&self, endpoint: Identity) -> Result<bool, KernelError> {
        Ok(!self
            .transient
            .scan(
                &[endpoint],
                endpoint_open_relation(),
                &[Some(Value::identity(endpoint))],
            )?
            .is_empty())
    }
}

fn endpoint_metadata(relation: RelationId) -> Option<RelationMetadata> {
    endpoint_relation_metadata()
        .into_iter()
        .find(|metadata| metadata.id() == relation)
}

fn suspended_state(outcome: &TaskOutcome, task: &Task<'_>) -> Option<crate::task::TaskState> {
    if matches!(outcome, TaskOutcome::Suspended { .. }) {
        Some(task.checkpoint())
    } else {
        None
    }
}

fn task_status_value(task_id: TaskId, state: Symbol) -> Value {
    let task_id = i64::try_from(task_id)
        .ok()
        .and_then(|task_id| Value::int(task_id).ok())
        .unwrap_or_else(|| Value::string(task_id.to_string()));
    Value::map([
        (Value::symbol(Symbol::intern("id")), task_id),
        (Value::symbol(Symbol::intern("state")), Value::symbol(state)),
    ])
}

fn transient_scopes(runtime_context: RuntimeContext) -> Vec<Identity> {
    let mut scopes = Vec::with_capacity(3);
    for scope in [runtime_context.principal(), runtime_context.actor()]
        .into_iter()
        .flatten()
        .chain(std::iter::once(runtime_context.endpoint()))
    {
        if !scopes.contains(&scope) {
            scopes.push(scope);
        }
    }
    scopes
}

pub struct SuspendedTask {
    task_id: TaskId,
    kind: SuspendKind,
    state: crate::task::TaskState,
}

impl SuspendedTask {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn kind(&self) -> &SuspendKind {
        &self.kind
    }

    pub fn frame_count(&self) -> usize {
        self.state.frame_count()
    }
}
