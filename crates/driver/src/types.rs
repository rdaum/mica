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

use mica_runtime::SourceTaskError;
use mica_runtime::TaskRequest;
use mica_runtime::{AuthorityContext, Effect, SuspendKind, TaskId};
use mica_var::{Identity, Value};
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskContext {
    pub principal: Option<Identity>,
    pub actor: Option<Identity>,
    pub endpoint: Option<Identity>,
    pub authority: AuthorityContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriverEvent {
    TaskCompleted { task_id: TaskId, value: Value },
    TaskAborted { task_id: TaskId, error: Value },
    TaskFailed { task_id: TaskId, error: String },
    TaskSuspended { task_id: TaskId, kind: SuspendKind },
    Effect(Effect),
}

#[derive(Debug)]
pub enum DriverError {
    Source(SourceTaskError),
    Join(String),
    MissingTaskContext(TaskId),
    MissingEndpoint(TaskId),
}

#[derive(Debug)]
pub enum DriverThreadError {
    Start(String),
    Closed,
    Driver(DriverError),
}

impl TaskContext {
    pub(crate) fn from_request(request: &TaskRequest) -> Self {
        Self {
            principal: request.principal,
            actor: request.actor,
            endpoint: request.endpoint,
            authority: request.authority.clone(),
        }
    }
}

impl DriverError {
    pub fn source(&self) -> Option<&SourceTaskError> {
        match self {
            Self::Source(error) => Some(error),
            Self::Join(_) | Self::MissingTaskContext(_) | Self::MissingEndpoint(_) => None,
        }
    }
}

impl Display for DriverError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => write!(f, "{error:?}"),
            Self::Join(error) => write!(f, "driver task failed: {error}"),
            Self::MissingTaskContext(task_id) => {
                write!(f, "missing task context for task {task_id}")
            }
            Self::MissingEndpoint(task_id) => {
                write!(f, "task {task_id} is waiting for input without an endpoint")
            }
        }
    }
}

impl std::error::Error for DriverError {}

impl Display for DriverThreadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(error) => write!(f, "failed to start driver thread: {error}"),
            Self::Closed => f.write_str("driver thread is closed"),
            Self::Driver(error) => Display::fmt(error, f),
        }
    }
}

impl std::error::Error for DriverThreadError {}
