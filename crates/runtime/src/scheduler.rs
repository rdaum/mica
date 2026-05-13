use crate::{Program, SuspendKind, Task, TaskError, TaskId, TaskLimits, TaskOutcome};
use mica_relation_kernel::RelationKernel;
use mica_var::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerError {
    UnknownTask(TaskId),
    TaskAlreadyCompleted(TaskId),
    Task(TaskError),
}

impl From<TaskError> for SchedulerError {
    fn from(value: TaskError) -> Self {
        Self::Task(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Effect {
    pub task_id: TaskId,
    pub value: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EffectLog {
    effects: Vec<Effect>,
}

impl EffectLog {
    pub fn emit(&mut self, task_id: TaskId, effects: Vec<Value>) {
        self.effects
            .extend(effects.into_iter().map(|value| Effect { task_id, value }));
    }

    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    pub fn drain(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }
}

pub struct Scheduler {
    kernel: RelationKernel,
    next_task_id: TaskId,
    suspended: HashMap<TaskId, SuspendedTask>,
    completed: HashMap<TaskId, TaskOutcome>,
    effects: EffectLog,
    limits: TaskLimits,
}

impl Scheduler {
    pub fn new(kernel: RelationKernel) -> Self {
        Self {
            kernel,
            next_task_id: 1,
            suspended: HashMap::new(),
            completed: HashMap::new(),
            effects: EffectLog::default(),
            limits: TaskLimits::default(),
        }
    }

    pub fn with_limits(mut self, limits: TaskLimits) -> Self {
        self.limits = limits;
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

    pub fn submit(
        &mut self,
        program: Arc<Program>,
    ) -> Result<(TaskId, TaskOutcome), SchedulerError> {
        let task_id = self.allocate_task_id();
        let mut task = Task::new(task_id, &self.kernel, program, self.limits);
        let outcome = task.run()?;
        let suspended_state = suspended_state(&outcome, &task);
        drop(task);
        self.record_outcome(task_id, outcome.clone(), suspended_state);
        Ok((task_id, outcome))
    }

    pub fn resume(&mut self, task_id: TaskId) -> Result<TaskOutcome, SchedulerError> {
        if self.completed.contains_key(&task_id) {
            return Err(SchedulerError::TaskAlreadyCompleted(task_id));
        }
        let suspended = self
            .suspended
            .remove(&task_id)
            .ok_or(SchedulerError::UnknownTask(task_id))?;
        let mut task = Task::from_state(task_id, &self.kernel, suspended.state);
        let outcome = task.run()?;
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
}

fn suspended_state(outcome: &TaskOutcome, task: &Task<'_>) -> Option<crate::task::TaskState> {
    if matches!(outcome, TaskOutcome::Suspended { .. }) {
        Some(task.checkpoint())
    } else {
        None
    }
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
