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

use mica_relation_kernel::{Conflict, KernelError, RelationKernel, Transaction, TransientStore};
use mica_var::{Identity, Value};
use mica_vm::{
    AuthorityContext, BuiltinRegistry, Emission, Program, ProgramResolver, RegisterVm,
    RuntimeContext, RuntimeError, SuspendKind, VmHostContext, VmHostResponse, VmState,
};
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
        effects: Vec<Emission>,
        retries: u8,
    },
    Suspended {
        kind: SuspendKind,
        effects: Vec<Emission>,
        retries: u8,
    },
    Aborted {
        error: Value,
        effects: Vec<Emission>,
        retries: u8,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskError {
    Runtime(RuntimeError),
    ConflictRetriesExceeded { retries: u8 },
    MissingTransaction,
    UnknownRelation(mica_relation_kernel::RelationId),
}

impl From<RuntimeError> for TaskError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<KernelError> for TaskError {
    fn from(value: KernelError) -> Self {
        Self::Runtime(RuntimeError::Kernel(value))
    }
}

pub struct Task<'a> {
    task_id: TaskId,
    kernel: &'a RelationKernel,
    program: Arc<Program>,
    resolver: Arc<ProgramResolver>,
    builtins: Arc<BuiltinRegistry>,
    authority: AuthorityContext,
    vm: RegisterVm,
    tx: Option<Transaction<'a>>,
    retry_state: VmState,
    pending_effects: Vec<Emission>,
    committed_effects: Vec<Emission>,
    task_snapshot: Vec<Value>,
    runtime_context: RuntimeContext,
    retries: u8,
    limits: TaskLimits,
}

impl<'a> Task<'a> {
    pub fn new(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        program: Arc<Program>,
        resolver: Arc<ProgramResolver>,
        limits: TaskLimits,
    ) -> Self {
        Self::new_with_builtins(
            task_id,
            kernel,
            program,
            resolver,
            Arc::new(BuiltinRegistry::new()),
            limits,
        )
    }

    pub fn new_with_builtins(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        program: Arc<Program>,
        resolver: Arc<ProgramResolver>,
        builtins: Arc<BuiltinRegistry>,
        limits: TaskLimits,
    ) -> Self {
        Self::new_with_authority(
            task_id,
            kernel,
            program,
            resolver,
            builtins,
            AuthorityContext::root(),
            limits,
        )
    }

    pub fn new_with_authority(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        program: Arc<Program>,
        resolver: Arc<ProgramResolver>,
        builtins: Arc<BuiltinRegistry>,
        authority: AuthorityContext,
        limits: TaskLimits,
    ) -> Self {
        let vm = RegisterVm::new(program.clone());
        let retry_state = vm.snapshot_state();
        Self {
            task_id,
            kernel,
            program,
            resolver,
            builtins,
            authority,
            vm,
            tx: Some(kernel.begin()),
            retry_state,
            pending_effects: Vec::new(),
            committed_effects: Vec::new(),
            task_snapshot: Vec::new(),
            runtime_context: RuntimeContext::default(),
            retries: 0,
            limits,
        }
    }

    pub(crate) fn from_state_with_authority(
        task_id: TaskId,
        kernel: &'a RelationKernel,
        resolver: Arc<ProgramResolver>,
        builtins: Arc<BuiltinRegistry>,
        state: TaskState,
        authority: AuthorityContext,
    ) -> Self {
        Self {
            task_id,
            kernel,
            vm: RegisterVm::from_state(state.vm_state),
            tx: Some(kernel.begin()),
            program: state.program,
            resolver,
            builtins,
            authority,
            retry_state: state.retry_state,
            pending_effects: Vec::new(),
            committed_effects: Vec::new(),
            task_snapshot: Vec::new(),
            runtime_context: RuntimeContext::default(),
            retries: state.retries,
            limits: state.limits,
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn set_task_snapshot(&mut self, task_snapshot: Vec<Value>) {
        self.task_snapshot = task_snapshot;
    }

    pub(crate) fn set_runtime_context(&mut self, runtime_context: RuntimeContext) {
        self.runtime_context = runtime_context;
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

    pub fn resume_with(&mut self, value: Value) -> Result<(), TaskError> {
        self.vm.resume_with(value)?;
        self.retry_state = self.vm.snapshot_state();
        Ok(())
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
        self.run_with_transient(None, &[])
    }

    pub(crate) fn run_with_transient(
        &mut self,
        mut transient: Option<&mut TransientStore>,
        transient_scopes: &[Identity],
    ) -> Result<TaskOutcome, TaskError> {
        loop {
            let response = {
                let tx = self.tx.as_mut().ok_or(TaskError::MissingTransaction)?;
                let mut host = VmHostContext::new(
                    tx,
                    &mut self.authority,
                    &self.resolver,
                    &self.builtins,
                    &mut self.pending_effects,
                    &self.task_snapshot,
                    self.runtime_context,
                );
                if let Some(transient) = transient.as_deref_mut() {
                    host = host.with_transient(transient, transient_scopes);
                }
                self.vm.run_until_host_response(
                    &mut host,
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

    fn take_committed_effects(&mut self) -> Vec<Emission> {
        std::mem::take(&mut self.committed_effects)
    }
}

pub(crate) struct TaskState {
    program: Arc<Program>,
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
