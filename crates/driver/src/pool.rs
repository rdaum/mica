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
use mica_runtime::{
    MailboxRecvRequest, RunReport, RuntimeError, SharedSourceRunner, SourceRunner, SourceTaskError,
    SpawnRequest, SubmittedTask, SuspendKind, TaskError, TaskId, TaskInput, TaskManagerError,
    TaskOutcome, TaskRequest, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

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
    mailbox_waiters: BTreeMap<u64, VecDeque<MailboxWaiter>>,
    events: Vec<DriverEvent>,
    event_waker: Option<Waker>,
}

#[derive(Clone, Debug)]
struct MailboxWaiter {
    task_id: TaskId,
    receivers: Vec<(u64, Value)>,
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

    pub fn format_error(&self, error: &DriverError) -> String {
        match error {
            DriverError::Source(error) => self.inner.runner.render_source_task_error(error),
            DriverError::Join(error) => format!("driver task failed: {error}"),
            DriverError::MissingTaskContext(task_id) => {
                format!("missing task context for task {task_id}")
            }
        }
    }

    pub async fn submit_source(
        &self,
        endpoint: Identity,
        mut request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        request.endpoint = endpoint;
        let context = TaskContext::from_request(&request, endpoint);
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self
            .dispatch(move || async move { runner.submit_source(request) })
            .await?;
        self.handle_submitted(context, submitted.clone()).await?;
        Ok(submitted)
    }

    pub async fn submit_source_report(
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
        let submitted = self
            .dispatch(move || async move { runner.submit_source(request) })
            .await?;
        self.handle_submitted(context, submitted.clone()).await?;
        Ok(self
            .inner
            .runner
            .report_outcome(submitted.task_id, submitted.outcome))
    }

    pub async fn submit_source_as_actor(
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
        let submitted = self
            .dispatch(move || async move { runner.submit_source(request) })
            .await?;
        self.handle_submitted(context, submitted.clone()).await?;
        Ok(submitted)
    }

    pub async fn submit_invocation(
        &self,
        endpoint: Identity,
        request: TaskRequest,
    ) -> Result<SubmittedTask, DriverError> {
        let mut request = request;
        request.endpoint = endpoint;
        let runner = Arc::clone(&self.inner.runner);
        let context = TaskContext::from_request(&request, endpoint);
        let submitted = self
            .dispatch(move || async move { runner.submit_invocation(request) })
            .await?;
        self.handle_submitted(context, submitted.clone()).await?;
        Ok(submitted)
    }

    pub async fn submit_invocation_for_endpoint(
        &self,
        endpoint: Identity,
        selector: Symbol,
        roles: Vec<(Symbol, Value)>,
    ) -> Result<SubmittedTask, DriverError> {
        let trace = driver_trace_enabled();
        let trace_selector = selector;
        let dispatch_start = Instant::now();
        let runner = Arc::clone(&self.inner.runner);
        let (context, submitted) = self
            .dispatch(move || async move {
                let request = runner.invocation_request_for_endpoint(endpoint, selector, roles)?;
                let context = TaskContext::from_request(&request, endpoint);
                let submitted = runner.submit_invocation(request)?;
                Ok((context, submitted))
            })
            .await?;
        if trace {
            eprintln!(
                "driver-trace invocation selector={} task={} dispatch +{:?}",
                trace_selector.name().unwrap_or("<unnamed>"),
                submitted.task_id,
                dispatch_start.elapsed()
            );
        }
        let handle_start = Instant::now();
        self.handle_submitted(context, submitted.clone()).await?;
        if trace {
            eprintln!(
                "driver-trace invocation selector={} task={} handle_submitted +{:?}",
                trace_selector.name().unwrap_or("<unnamed>"),
                submitted.task_id,
                handle_start.elapsed()
            );
        }
        Ok(submitted)
    }

    pub async fn resume(&self, task_id: TaskId, value: Value) -> Result<TaskOutcome, DriverError> {
        let context = {
            let mut state = self.inner.state.lock().unwrap();
            state.remove_mailbox_waiter(task_id);
            state
                .contexts
                .remove(&task_id)
                .ok_or(DriverError::MissingTaskContext(task_id))?
        };
        let runner = Arc::clone(&self.inner.runner);
        let request = TaskRequest {
            principal: context.principal,
            actor: context.actor,
            endpoint: context.endpoint,
            authority: context.authority.clone(),
            input: TaskInput::Continuation { task_id, value },
        };
        let outcome = self
            .dispatch(move || async move { runner.resume_task(request) })
            .await?;
        self.handle_submitted(
            context,
            SubmittedTask {
                task_id,
                outcome: outcome.clone(),
            },
        )
        .await?;
        Ok(outcome)
    }

    pub async fn input(
        &self,
        endpoint: Identity,
        value: Value,
    ) -> Result<Vec<TaskOutcome>, DriverError> {
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
            outcomes.push(self.resume(task_id, value.clone()).await?);
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

    pub fn assert_transient_tuples_named(
        &self,
        scope: Identity,
        tuples: Vec<(Symbol, Tuple)>,
    ) -> Result<usize, DriverError> {
        self.inner
            .runner
            .assert_transient_tuples_named(scope, tuples)
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

    pub fn retract_transient_tuples_named(
        &self,
        scope: Identity,
        tuples: Vec<(Symbol, Tuple)>,
    ) -> Result<usize, DriverError> {
        self.inner
            .runner
            .retract_transient_tuples_named(scope, tuples)
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

    async fn dispatch<F, Fut, T>(&self, f: F) -> Result<T, DriverError>
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
        receiver
            .await
            .map_err(|_| DriverError::Join("dispatched task was cancelled".to_owned()))?
            .map_err(DriverError::Source)
    }

    async fn handle_submitted(
        &self,
        context: TaskContext,
        submitted: SubmittedTask,
    ) -> Result<(), DriverError> {
        let mut queue = VecDeque::new();
        queue.push_back((submitted.task_id, context, submitted.outcome));
        self.process_outcome_queue(&mut queue).await
    }

    async fn process_outcome_queue(
        &self,
        queue: &mut VecDeque<(TaskId, TaskContext, TaskOutcome)>,
    ) -> Result<(), DriverError> {
        while let Some((task_id, context, outcome)) = queue.pop_front() {
            let delivered_mailboxes = self.delivered_mailboxes(&outcome);
            let mut timer = None;
            let mut spawn = None;
            let mut mailbox_recv = None;
            let event_waker;
            {
                let mut state = self.inner.state.lock().unwrap();
                state.drain_effects_into_events(&self.inner.runner);
                match outcome {
                    TaskOutcome::Complete { value, .. } => {
                        state
                            .events
                            .push(DriverEvent::TaskCompleted { task_id, value });
                    }
                    TaskOutcome::Aborted { error, .. } => {
                        state
                            .events
                            .push(DriverEvent::TaskAborted { task_id, error });
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
                                timer = Some(Duration::from_millis(millis));
                            }
                            SuspendKind::WaitingForInput(_) => {
                                state
                                    .input_waiters
                                    .entry(context.endpoint)
                                    .or_default()
                                    .push(task_id);
                            }
                            SuspendKind::MailboxRecv(request) => {
                                mailbox_recv = Some(request);
                            }
                            SuspendKind::Spawn(request) => {
                                spawn = Some(request);
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
            if let Some(request) = mailbox_recv {
                self.handle_mailbox_recv(task_id, request, queue).await?;
            }
            self.wake_mailbox_waiters(delivered_mailboxes, queue)
                .await?;
            if let Some(request) = spawn {
                self.spawn_child_and_resume(task_id, context, request, queue)
                    .await?;
            }
        }
        Ok(())
    }

    fn delivered_mailboxes(&self, outcome: &TaskOutcome) -> Vec<u64> {
        let mailbox_sends = match outcome {
            TaskOutcome::Complete { mailbox_sends, .. }
            | TaskOutcome::Suspended { mailbox_sends, .. }
            | TaskOutcome::Aborted { mailbox_sends, .. } => mailbox_sends,
        };
        mailbox_sends
            .iter()
            .filter_map(|send| self.inner.runner.mailbox_for_sender(&send.sender).ok())
            .fold(Vec::new(), |mut mailboxes, mailbox| {
                if !mailboxes.contains(&mailbox) {
                    mailboxes.push(mailbox);
                }
                mailboxes
            })
    }

    async fn handle_mailbox_recv(
        &self,
        task_id: TaskId,
        request: MailboxRecvRequest,
        queue: &mut VecDeque<(TaskId, TaskContext, TaskOutcome)>,
    ) -> Result<(), DriverError> {
        let mut receivers = Vec::with_capacity(request.receivers.len());
        for receiver in request.receivers {
            let mailbox = self
                .inner
                .runner
                .mailbox_for_receiver(&receiver)
                .map_err(runtime_driver_error)?;
            if receivers
                .iter()
                .any(|(existing_mailbox, _)| *existing_mailbox == mailbox)
            {
                continue;
            }
            receivers.push((mailbox, receiver));
        }
        let mut timeout = None;
        let ready = {
            let mut state = self.inner.state.lock().unwrap();
            for (mailbox, _) in &receivers {
                state
                    .mailbox_waiters
                    .entry(*mailbox)
                    .or_default()
                    .push_back(MailboxWaiter {
                        task_id,
                        receivers: receivers.clone(),
                    });
            }
            let ready = self.drain_ready_mailbox_groups(&receivers)?;
            let should_wait = ready.is_empty() && request.timeout_millis != Some(0);
            if should_wait {
                timeout = request
                    .timeout_millis
                    .map(|millis| Duration::from_millis(millis).max(Duration::from_millis(1)));
                Vec::new()
            } else {
                state.remove_mailbox_waiter(task_id);
                ready
            }
        };
        if !ready.is_empty() || request.timeout_millis == Some(0) {
            let (ctx, submitted) = self.resume_raw(task_id, Value::list(ready)).await?;
            queue.push_back((submitted.task_id, ctx, submitted.outcome));
            return Ok(());
        }
        if let Some(timeout) = timeout {
            self.spawn_mailbox_timeout(task_id, timeout);
        }
        Ok(())
    }

    fn drain_ready_mailbox_groups(
        &self,
        receivers: &[(u64, Value)],
    ) -> Result<Vec<Value>, DriverError> {
        let mut ready = Vec::new();
        for (_, receiver) in receivers {
            let messages = self
                .inner
                .runner
                .drain_mailbox(receiver.clone())
                .map_err(runtime_driver_error)?;
            if messages.is_empty() {
                continue;
            }
            ready.push(Value::list([receiver.clone(), Value::list(messages)]));
        }
        Ok(ready)
    }

    fn spawn_mailbox_timeout(&self, task_id: TaskId, duration: Duration) {
        let driver = self.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(duration).await;
            let still_waiting = {
                let state = driver.inner.state.lock().unwrap();
                state
                    .mailbox_waiters
                    .values()
                    .any(|waiters| waiters.iter().any(|waiter| waiter.task_id == task_id))
            };
            if !still_waiting {
                return;
            }
            if let Err(error) = driver.resume(task_id, Value::list([])).await {
                driver.record_task_failure(task_id, error);
            }
        })
        .detach();
    }

    async fn wake_mailbox_waiters(
        &self,
        mailboxes: Vec<u64>,
        queue: &mut VecDeque<(TaskId, TaskContext, TaskOutcome)>,
    ) -> Result<(), DriverError> {
        for mailbox in mailboxes {
            let waiter = {
                let mut state = self.inner.state.lock().unwrap();
                let waiter = state
                    .mailbox_waiters
                    .get_mut(&mailbox)
                    .and_then(VecDeque::pop_front);
                if let Some(waiter) = &waiter {
                    state.remove_mailbox_waiter(waiter.task_id);
                }
                waiter
            };
            let Some(waiter) = waiter else {
                continue;
            };
            let ready = self.drain_ready_mailbox_groups(&waiter.receivers)?;
            if ready.is_empty() {
                continue;
            }
            let (ctx, submitted) = self.resume_raw(waiter.task_id, Value::list(ready)).await?;
            queue.push_back((submitted.task_id, ctx, submitted.outcome));
        }
        Ok(())
    }

    async fn spawn_child_and_resume(
        &self,
        parent_task_id: TaskId,
        context: TaskContext,
        request: SpawnRequest,
        queue: &mut VecDeque<(TaskId, TaskContext, TaskOutcome)>,
    ) -> Result<(), DriverError> {
        let (child_ctx, child_submitted) = self.submit_spawn_raw(context, request).await?;
        queue.push_back((child_submitted.task_id, child_ctx, child_submitted.outcome));

        let child_id =
            Value::int(child_submitted.task_id as i64).expect("allocated task id fits in Value");
        let (parent_ctx, parent_submitted) = self.resume_raw(parent_task_id, child_id).await?;
        queue.push_back((
            parent_submitted.task_id,
            parent_ctx,
            parent_submitted.outcome,
        ));
        Ok(())
    }

    async fn submit_spawn_raw(
        &self,
        context: TaskContext,
        request: SpawnRequest,
    ) -> Result<(TaskContext, SubmittedTask), DriverError> {
        let runner = Arc::clone(&self.inner.runner);
        let context_authority = context.authority.clone();
        let submit_context = context.clone();
        let submitted = self
            .dispatch(move || async move {
                runner.submit_spawn(
                    context.principal,
                    context.actor,
                    context.endpoint,
                    context_authority,
                    request,
                )
            })
            .await?;
        Ok((submit_context, submitted))
    }

    async fn resume_raw(
        &self,
        task_id: TaskId,
        value: Value,
    ) -> Result<(TaskContext, SubmittedTask), DriverError> {
        let context = {
            let mut state = self.inner.state.lock().unwrap();
            state
                .contexts
                .remove(&task_id)
                .ok_or(DriverError::MissingTaskContext(task_id))?
        };
        let request = TaskRequest {
            principal: context.principal,
            actor: context.actor,
            endpoint: context.endpoint,
            authority: context.authority.clone(),
            input: TaskInput::Continuation { task_id, value },
        };
        let runner = Arc::clone(&self.inner.runner);
        let submitted = self
            .dispatch(move || async move { runner.resume_task(request) })
            .await
            .map(|outcome| SubmittedTask { task_id, outcome })?;
        Ok((context, submitted))
    }

    fn spawn_timer_resume(&self, task_id: TaskId, duration: Duration) {
        let driver = self.clone();
        compio::runtime::spawn(async move {
            compio::time::sleep(duration).await;
            if let Err(error) = driver.resume(task_id, Value::nothing()).await {
                driver.record_task_failure(task_id, error);
            }
        })
        .detach();
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

    fn remove_mailbox_waiter(&mut self, task_id: TaskId) {
        for waiters in self.mailbox_waiters.values_mut() {
            waiters.retain(|waiter| waiter.task_id != task_id);
        }
        self.mailbox_waiters
            .retain(|_, waiters| !waiters.is_empty());
    }
}

fn driver_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("MICA_DRIVER_TRACE").is_some())
}

fn runtime_driver_error(error: RuntimeError) -> DriverError {
    DriverError::Source(SourceTaskError::TaskManager(TaskManagerError::Task(
        TaskError::Runtime(error),
    )))
}
