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

use crate::{DispatcherConfig, DriverError, DriverEvent, TaskContext, configure_dispatcher};
use compio::dispatcher::Dispatcher;
use compio::runtime::Runtime;
use mica_runtime::{
    RunReport, SharedSourceRunner, SourceRunner, SubmittedTask, SuspendKind, TaskId, TaskInput,
    TaskOutcome, TaskRequest, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct CompioTaskDriver {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    runner: Arc<SharedSourceRunner>,
    dispatcher: Dispatcher,
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    contexts: BTreeMap<TaskId, TaskContext>,
    input_waiters: BTreeMap<Identity, Vec<TaskId>>,
    events: Vec<DriverEvent>,
    event_waker: Option<Waker>,
}

pub struct DriverEvents<'a> {
    driver: &'a CompioTaskDriver,
}

impl CompioTaskDriver {
    pub fn spawn(runner: SourceRunner) -> Result<Self, DriverError> {
        Self::spawn_with_workers(runner, None)
    }

    pub fn spawn_empty() -> Result<Self, DriverError> {
        Self::spawn(SourceRunner::new_empty())
    }

    pub fn spawn_with_workers(
        runner: SourceRunner,
        workers: Option<NonZeroUsize>,
    ) -> Result<Self, DriverError> {
        Self::spawn_with_config(
            runner,
            DispatcherConfig {
                workers,
                ..DispatcherConfig::default()
            },
        )
    }

    pub fn spawn_with_config(
        runner: SourceRunner,
        config: DispatcherConfig,
    ) -> Result<Self, DriverError> {
        let (builder, _) = configure_dispatcher(Dispatcher::builder(), config);
        let dispatcher = builder
            .thread_names(|index| format!("mica-driver-pool-{index}"))
            .build()
            .map_err(|error| DriverError::Join(format!("failed to start dispatcher: {error}")))?;
        Ok(Self {
            inner: Arc::new(PoolInner {
                runner: Arc::new(runner.into_shared()),
                dispatcher,
                state: Mutex::new(PoolState::default()),
            }),
        })
    }

    pub fn named_identity(&self, name: Symbol) -> Result<Identity, DriverError> {
        self.inner
            .runner
            .named_identity(name)
            .map_err(DriverError::Source)
    }

    pub fn submit_source(
        &self,
        endpoint: Identity,
        mut request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self.dispatch(move || async move { runner.submit_source(request) })?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(submitted)
    }

    pub fn submit_source_report(
        &self,
        endpoint: Identity,
        actor: Option<Symbol>,
        source: String,
    ) -> Result<RunReport, DriverError> {
        let mut request = match actor {
            Some(actor) => self
                .inner
                .runner
                .source_request_as(actor, source)
                .map_err(DriverError::Source)?,
            None => self
                .inner
                .runner
                .source_request_for_endpoint(endpoint, source)
                .map_err(DriverError::Source)?,
        };
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self.dispatch(move || async move { runner.submit_source(request) })?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(self
            .inner
            .runner
            .report_outcome(submitted.task_id, submitted.outcome))
    }

    pub fn submit_source_as_actor(
        &self,
        endpoint: Identity,
        actor: Identity,
        source: String,
    ) -> Result<SubmittedTask, DriverError> {
        let mut request = self
            .inner
            .runner
            .source_request_as_identity(actor, source)
            .map_err(DriverError::Source)?;
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self.dispatch(move || async move { runner.submit_source(request) })?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(submitted)
    }

    pub fn submit_invocation(
        &self,
        endpoint: Identity,
        mut request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self.dispatch(move || async move { runner.submit_invocation(request) })?;
        self.handle_submitted(context, submitted.clone())?;
        Ok(submitted)
    }

    pub fn resume(&self, task_id: TaskId, value: Value) -> Result<TaskOutcome, DriverError> {
        let context = self
            .inner
            .state
            .lock()
            .unwrap()
            .contexts
            .remove(&task_id)
            .ok_or(DriverError::MissingTaskContext(task_id))?;
        let runner = Arc::clone(&self.inner.runner);
        let request = TaskRequest {
            principal: context.principal,
            actor: context.actor,
            endpoint: context.endpoint,
            authority: context.authority.clone(),
            input: TaskInput::Continuation { task_id, value },
        };
        let outcome = self.dispatch(move || async move { runner.resume_task(request) })?;
        self.handle_outcome(task_id, context, outcome.clone())?;
        Ok(outcome)
    }

    pub fn input(&self, endpoint: Identity, value: Value) -> Result<Vec<TaskOutcome>, DriverError> {
        let task_ids = self
            .inner
            .state
            .lock()
            .unwrap()
            .input_waiters
            .remove(&endpoint)
            .unwrap_or_default();
        let mut outcomes = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            outcomes.push(self.resume(task_id, value.clone())?);
        }
        Ok(outcomes)
    }

    pub fn open_endpoint(
        &self,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: Symbol,
    ) -> Result<(), DriverError> {
        self.inner
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
        self.inner
            .runner
            .open_endpoint_with_context(endpoint, principal, actor, protocol)
            .map_err(DriverError::Source)
    }

    pub fn close_endpoint(&self, endpoint: Identity) -> usize {
        self.inner.runner.close_endpoint(endpoint)
    }

    pub fn assert_transient_named(
        &self,
        scope: Identity,
        relation: Symbol,
        values: Vec<Value>,
    ) -> Result<bool, DriverError> {
        self.inner
            .runner
            .assert_transient_named(scope, relation, values)
            .map_err(DriverError::Source)
    }

    pub fn assert_transient_tuple_named(
        &self,
        scope: Identity,
        relation: Symbol,
        tuple: Tuple,
    ) -> Result<bool, DriverError> {
        self.inner
            .runner
            .assert_transient_tuple_named(scope, relation, tuple)
            .map_err(DriverError::Source)
    }

    pub fn retract_transient_named(
        &self,
        scope: Identity,
        relation: Symbol,
        values: Vec<Value>,
    ) -> Result<bool, DriverError> {
        self.inner
            .runner
            .retract_transient_named(scope, relation, values)
            .map_err(DriverError::Source)
    }

    pub fn retract_transient_tuple_named(
        &self,
        scope: Identity,
        relation: Symbol,
        tuple: &Tuple,
    ) -> Result<bool, DriverError> {
        self.inner
            .runner
            .retract_transient_tuple_named(scope, relation, tuple)
            .map_err(DriverError::Source)
    }

    pub fn drain_events(&self) -> Vec<DriverEvent> {
        let mut state = self.inner.state.lock().unwrap();
        state.drain_effects_into_events(&self.inner.runner);
        std::mem::take(&mut state.events)
    }

    pub fn wait_events(&self) -> DriverEvents<'_> {
        DriverEvents { driver: self }
    }

    fn dispatch<F, Fut, T>(&self, f: F) -> Result<T, DriverError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, mica_runtime::SourceTaskError>> + 'static,
        T: Send + 'static,
    {
        let receiver = self
            .inner
            .dispatcher
            .dispatch(f)
            .map_err(|_| DriverError::Join("dispatcher is stopped".to_owned()))?;
        Runtime::new()
            .map_err(|error| DriverError::Join(format!("failed to create wait runtime: {error}")))?
            .block_on(receiver)
            .map_err(|_| DriverError::Join("dispatched task was cancelled".to_owned()))?
            .map_err(DriverError::Source)
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
        let event_waker;
        {
            let mut state = self.inner.state.lock().unwrap();
            state.drain_effects_into_events(&self.inner.runner);
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
            event_waker = state.event_waker.take();
        }
        if let Some(waker) = event_waker {
            waker.wake();
        }
        if let Some(duration) = timer {
            self.spawn_timer_resume(task_id, duration);
        }
        Ok(())
    }

    fn spawn_timer_resume(&self, task_id: TaskId, duration: Duration) {
        let driver = self.clone();
        thread::spawn(move || {
            thread::sleep(duration);
            if let Err(error) = driver.resume(task_id, Value::nothing()) {
                driver.record_task_failure(task_id, error);
            }
        });
    }

    fn record_task_failure(&self, task_id: TaskId, error: DriverError) {
        let event_waker = {
            let mut state = self.inner.state.lock().unwrap();
            state.events.push(DriverEvent::TaskFailed {
                task_id,
                error: error.to_string(),
            });
            state.event_waker.take()
        };
        if let Some(waker) = event_waker {
            waker.wake();
        }
    }
}

impl Future for DriverEvents<'_> {
    type Output = Vec<DriverEvent>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.driver.inner.state.lock().unwrap();
        state.drain_effects_into_events(&self.driver.inner.runner);
        if !state.events.is_empty() {
            return Poll::Ready(std::mem::take(&mut state.events));
        }
        state.event_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl PoolState {
    fn drain_effects_into_events(&mut self, runner: &SharedSourceRunner) {
        self.events.extend(
            runner
                .drain_routed_emissions()
                .into_iter()
                .map(DriverEvent::Effect),
        );
    }
}
