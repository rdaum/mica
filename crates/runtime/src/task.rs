use crate::{ProgramResolver, RegisterVm, SuspendKind, TaskError, VmHostResponse, VmState};
use mica_relation_kernel::{Conflict, KernelError, RelationKernel, Transaction};
use mica_var::Value;
use std::sync::Arc;

pub type TaskId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskLimits {
    pub instruction_budget: usize,
    pub max_retries: u8,
    pub max_call_depth: usize,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            instruction_budget: 60_000,
            max_retries: 10,
            max_call_depth: 50,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskOutcome {
    Complete {
        value: Value,
        effects: Vec<Value>,
        retries: u8,
    },
    Suspended {
        kind: SuspendKind,
        effects: Vec<Value>,
        retries: u8,
    },
    Aborted {
        error: Value,
        effects: Vec<Value>,
        retries: u8,
    },
}

pub struct Task<'a> {
    task_id: TaskId,
    kernel: &'a RelationKernel,
    program: Arc<crate::Program>,
    resolver: Arc<ProgramResolver>,
    vm: RegisterVm,
    tx: Option<Transaction<'a>>,
    retry_state: VmState,
    pending_effects: Vec<Value>,
    committed_effects: Vec<Value>,
    retries: u8,
    limits: TaskLimits,
}

impl<'a> Task<'a> {
    pub fn new(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        program: Arc<crate::Program>,
        resolver: Arc<ProgramResolver>,
        limits: TaskLimits,
    ) -> Self {
        let vm = RegisterVm::new(program.clone());
        let retry_state = vm.snapshot_state();
        Self {
            task_id,
            kernel,
            program,
            resolver,
            vm,
            tx: Some(kernel.begin()),
            retry_state,
            pending_effects: Vec::new(),
            committed_effects: Vec::new(),
            retries: 0,
            limits,
        }
    }

    pub(crate) fn from_state(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        resolver: Arc<ProgramResolver>,
        state: TaskState,
    ) -> Self {
        Self {
            task_id,
            kernel,
            vm: RegisterVm::from_state(state.vm_state),
            tx: Some(kernel.begin()),
            program: state.program,
            resolver,
            retry_state: state.retry_state,
            pending_effects: Vec::new(),
            committed_effects: Vec::new(),
            retries: state.retries,
            limits: state.limits,
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn retries(&self) -> u8 {
        self.retries
    }

    pub fn vm(&self) -> &RegisterVm {
        &self.vm
    }

    pub fn vm_mut(&mut self) -> &mut RegisterVm {
        &mut self.vm
    }

    pub(crate) fn checkpoint(&self) -> TaskState {
        TaskState {
            program: self.program.clone(),
            vm_state: self.vm.snapshot_state(),
            retry_state: self.retry_state.clone(),
            retries: self.retries,
            limits: self.limits,
        }
    }

    pub fn run(&mut self) -> Result<TaskOutcome, TaskError> {
        loop {
            let response = {
                let tx = self.tx.as_mut().ok_or(TaskError::MissingTransaction)?;
                self.vm.run_until_host_response(
                    tx,
                    &self.resolver,
                    &mut self.pending_effects,
                    self.limits.instruction_budget,
                    self.limits.max_call_depth,
                )?
            };

            match response {
                VmHostResponse::Continue => {}
                VmHostResponse::Commit => {
                    if self.commit_boundary()? == BoundaryResult::Retried {
                        continue;
                    }
                }
                VmHostResponse::Suspend(kind) => {
                    if self.commit_boundary()? == BoundaryResult::Retried {
                        continue;
                    }
                    return Ok(TaskOutcome::Suspended {
                        kind,
                        effects: self.take_committed_effects(),
                        retries: self.retries,
                    });
                }
                VmHostResponse::Complete(value) => {
                    if self.commit_boundary()? == BoundaryResult::Retried {
                        continue;
                    }
                    return Ok(TaskOutcome::Complete {
                        value,
                        effects: self.take_committed_effects(),
                        retries: self.retries,
                    });
                }
                VmHostResponse::Abort(error) => {
                    self.pending_effects.clear();
                    self.tx = Some(self.kernel.begin());
                    return Ok(TaskOutcome::Aborted {
                        error,
                        effects: self.take_committed_effects(),
                        retries: self.retries,
                    });
                }
                VmHostResponse::RollbackRetry => self.retry_from_boundary()?,
            }
        }
    }

    fn commit_boundary(&mut self) -> Result<BoundaryResult, TaskError> {
        let tx = self.tx.take().ok_or(TaskError::MissingTransaction)?;
        match tx.commit() {
            Ok(_) => {
                self.committed_effects.append(&mut self.pending_effects);
                self.retry_state = self.vm.snapshot_state();
                self.tx = Some(self.kernel.begin());
                Ok(BoundaryResult::Committed)
            }
            Err(error) if is_retryable_conflict(&error) => {
                self.tx = Some(self.kernel.begin());
                self.retry_from_boundary()?;
                Ok(BoundaryResult::Retried)
            }
            Err(error) => {
                self.tx = Some(self.kernel.begin());
                Err(error.into())
            }
        }
    }

    fn retry_from_boundary(&mut self) -> Result<(), TaskError> {
        if self.retries >= self.limits.max_retries {
            return Err(TaskError::ConflictRetriesExceeded {
                retries: self.retries,
            });
        }
        self.pending_effects.clear();
        self.vm.restore_state(&self.retry_state);
        self.tx = Some(self.kernel.begin());
        self.retries += 1;
        Ok(())
    }

    fn take_committed_effects(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.committed_effects)
    }
}

pub(crate) struct TaskState {
    program: Arc<crate::Program>,
    vm_state: VmState,
    retry_state: VmState,
    retries: u8,
    limits: TaskLimits,
}

impl TaskState {
    pub(crate) fn frame_count(&self) -> usize {
        self.vm_state.frames().len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundaryResult {
    Committed,
    Retried,
}

fn is_retryable_conflict(error: &KernelError) -> bool {
    matches!(
        error,
        KernelError::Conflict(Conflict {
            relation: _,
            tuple: _,
            kind: _
        })
    )
}
