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

use crate::{DriverError, DriverEvent, TaskContext};
use compio::runtime::{JoinHandle as CompioJoinHandle, spawn, time::sleep};
use mica_runtime::{RunReport, SourceRunner, SubmittedTask, TaskInput, TaskRequest};
use mica_runtime::{SuspendKind, TaskId, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

#[derive(Clone)]
pub struct CompioTaskDriver {
    state: Rc<RefCell<DriverState>>,
}

struct DriverState {
    runner: SourceRunner,
    contexts: BTreeMap<TaskId, TaskContext>,
    input_waiters: BTreeMap<Identity, Vec<TaskId>>,
    events: Vec<DriverEvent>,
}

impl CompioTaskDriver {
    pub fn new(runner: SourceRunner) -> Self {
        Self {
            state: Rc::new(RefCell::new(DriverState {
                runner,
                contexts: BTreeMap::new(),
                input_waiters: BTreeMap::new(),
                events: Vec::new(),
            })),
        }
    }

    pub fn empty() -> Self {
        Self::new(SourceRunner::new_empty())
    }

    pub fn spawn_source(
        &self,
        endpoint: Identity,
        request: TaskRequest,
    ) -> CompioJoinHandle<Result<SubmittedTask, DriverError>> {
        let driver = self.clone();
        spawn(async move { driver.run_source_request(endpoint, request) })
    }

    pub async fn submit_source(
        &self,
        endpoint: Identity,
        request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        self.spawn_source(endpoint, request)
            .await
            .map_err(|error| DriverError::Join(error.to_string()))?
    }

    pub fn spawn_source_report(
        &self,
        endpoint: Identity,
        actor: Option<Symbol>,
        source: String,
    ) -> CompioJoinHandle<Result<RunReport, DriverError>> {
        let driver = self.clone();
        spawn(async move { driver.run_source_report(endpoint, actor, source) })
    }

    pub async fn submit_source_report(
        &self,
        endpoint: Identity,
        actor: Option<Symbol>,
        source: String,
    ) -> Result<RunReport, DriverError> {
        self.spawn_source_report(endpoint, actor, source)
            .await
            .map_err(|error| DriverError::Join(error.to_string()))?
    }

    pub fn spawn_invocation(
        &self,
        endpoint: Identity,
        request: TaskRequest,
    ) -> CompioJoinHandle<Result<SubmittedTask, DriverError>> {
        let driver = self.clone();
        spawn(async move { driver.run_invocation_request(endpoint, request) })
    }

    pub async fn submit_invocation(
        &self,
        endpoint: Identity,
        request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        self.spawn_invocation(endpoint, request)
            .await
            .map_err(|error| DriverError::Join(error.to_string()))?
    }

    pub fn spawn_resume(
        &self,
        task_id: TaskId,
        value: Value,
    ) -> CompioJoinHandle<Result<TaskOutcome, DriverError>> {
        let driver = self.clone();
        spawn(async move { driver.run_resume(task_id, value) })
    }

    pub async fn resume(&self, task_id: TaskId, value: Value) -> Result<TaskOutcome, DriverError> {
        self.spawn_resume(task_id, value)
            .await
            .map_err(|error| DriverError::Join(error.to_string()))?
    }

    pub fn spawn_input(
        &self,
        endpoint: Identity,
        value: Value,
    ) -> CompioJoinHandle<Result<Vec<TaskOutcome>, DriverError>> {
        let driver = self.clone();
        spawn(async move { driver.run_input(endpoint, value) })
    }

    pub async fn input(
        &self,
        endpoint: Identity,
        value: Value,
    ) -> Result<Vec<TaskOutcome>, DriverError> {
        self.spawn_input(endpoint, value)
            .await
            .map_err(|error| DriverError::Join(error.to_string()))?
    }

    pub fn drain_events(&self) -> Vec<DriverEvent> {
        std::mem::take(&mut self.state.borrow_mut().events)
    }

    pub fn open_endpoint(
        &self,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), DriverError> {
        self.state
            .borrow_mut()
            .runner
            .open_endpoint(endpoint, actor, protocol)
            .map_err(DriverError::Source)
    }

    pub fn open_endpoint_with_context(
        &self,
        endpoint: Identity,
        principal: Option<Identity>,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), DriverError> {
        self.state
            .borrow_mut()
            .runner
            .open_endpoint_with_context(endpoint, principal, actor, protocol)
            .map_err(DriverError::Source)
    }

    pub fn close_endpoint(&self, endpoint: Identity) -> usize {
        self.state.borrow_mut().runner.close_endpoint(endpoint)
    }

    pub(crate) fn run_source_request(
        &self,
        endpoint: Identity,
        mut request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let submitted = self
            .state
            .borrow_mut()
            .runner
            .submit_source(request)
            .map_err(DriverError::Source)?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(submitted)
    }

    pub(crate) fn run_source_report(
        &self,
        endpoint: Identity,
        actor: Option<Symbol>,
        source: String,
    ) -> Result<RunReport, DriverError> {
        let request = {
            let state = self.state.borrow();
            match actor {
                Some(actor) => state
                    .runner
                    .source_request_as(actor, source)
                    .map_err(DriverError::Source)?,
                None => state
                    .runner
                    .source_request_for_endpoint(endpoint, source)
                    .map_err(DriverError::Source)?,
            }
        };
        let submitted = self.run_source_request(endpoint, request)?;
        Ok(self
            .state
            .borrow()
            .runner
            .report_outcome(submitted.task_id, submitted.outcome))
    }

    pub(crate) fn run_invocation_request(
        &self,
        endpoint: Identity,
        mut request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let submitted = self
            .state
            .borrow_mut()
            .runner
            .submit_invocation(request)
            .map_err(DriverError::Source)?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(submitted)
    }

    pub(crate) fn run_resume(
        &self,
        task_id: TaskId,
        value: Value,
    ) -> Result<TaskOutcome, DriverError> {
        let context = self
            .state
            .borrow_mut()
            .contexts
            .remove(&task_id)
            .ok_or(DriverError::MissingTaskContext(task_id))?;
        let outcome = self
            .state
            .borrow_mut()
            .runner
            .resume_task(TaskRequest {
                principal: context.principal,
                actor: context.actor,
                endpoint: context.endpoint,
                authority: context.authority.clone(),
                input: TaskInput::Continuation { task_id, value },
            })
            .map_err(DriverError::Source)?;
        self.handle_outcome(task_id, context, outcome.clone())?;
        Ok(outcome)
    }

    pub(crate) fn run_input(
        &self,
        endpoint: Identity,
        value: Value,
    ) -> Result<Vec<TaskOutcome>, DriverError> {
        let task_ids = self
            .state
            .borrow_mut()
            .input_waiters
            .remove(&endpoint)
            .unwrap_or_default();
        let mut outcomes = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            outcomes.push(self.run_resume(task_id, value.clone())?);
        }
        Ok(outcomes)
    }

    fn handle_submitted(
        &self,
        context: TaskContext,
        submitted: SubmittedTask,
    ) -> Result<(), DriverError> {
        self.handle_outcome(submitted.task_id, context, submitted.outcome)
    }

    fn handle_outcome(
        &self,
        task_id: TaskId,
        context: TaskContext,
        outcome: TaskOutcome,
    ) -> Result<(), DriverError> {
        let mut timer = None;
        {
            let mut state = self.state.borrow_mut();
            state.drain_effects_into_events();
            match &outcome {
                TaskOutcome::Complete { value, .. } => {
                    state.events.push(DriverEvent::TaskCompleted {
                        task_id,
                        value: value.clone(),
                    });
                }
                TaskOutcome::Aborted { error, .. } => {
                    state.events.push(DriverEvent::TaskAborted {
                        task_id,
                        error: error.clone(),
                    });
                }
                TaskOutcome::Suspended { kind, .. } => {
                    state.contexts.insert(task_id, context.clone());
                    state.events.push(DriverEvent::TaskSuspended {
                        task_id,
                        kind: kind.clone(),
                    });
                    match kind {
                        SuspendKind::Commit => timer = Some(Duration::ZERO),
                        SuspendKind::Never => {}
                        SuspendKind::TimedMillis(millis) => {
                            timer = Some(Duration::from_millis(*millis));
                        }
                        SuspendKind::WaitingForInput(_) => {
                            state
                                .input_waiters
                                .entry(context.endpoint)
                                .or_default()
                                .push(task_id);
                        }
                    }
                }
            }
        }
        if let Some(duration) = timer {
            self.spawn_timer_resume(task_id, duration);
        }
        Ok(())
    }

    fn spawn_timer_resume(&self, task_id: TaskId, duration: Duration) {
        let driver = self.clone();
        spawn(async move {
            sleep(duration).await;
            if let Err(error) = driver.resume(task_id, Value::nothing()).await {
                driver.record_task_failure(task_id, error);
            }
        })
        .detach();
    }

    fn record_task_failure(&self, task_id: TaskId, error: DriverError) {
        self.state
            .borrow_mut()
            .events
            .push(DriverEvent::TaskFailed {
                task_id,
                error: error.to_string(),
            });
    }
}

impl DriverState {
    fn drain_effects_into_events(&mut self) {
        self.events.extend(
            self.runner
                .drain_routed_emissions()
                .into_iter()
                .map(DriverEvent::Effect),
        );
    }
}
