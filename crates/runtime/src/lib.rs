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

//! Transactional task runtime for Mica.
//!
//! This crate is the first executable runtime slice: a register-based
//! interpreter that runs over `mica-relation-kernel` transactions and reports
//! host responses at commit, suspend, retry, and completion boundaries.

mod authority;
mod builtin;
mod effect;
mod error;
mod program;
mod task;
mod task_manager;
mod vm;

#[cfg(test)]
mod tests;

pub use authority::{AuthorityContext, CapabilityGrant, CapabilityOp, CapabilityScope};
pub use builtin::{Builtin, BuiltinContext, BuiltinRegistry};
pub use effect::Emission;
pub use error::{RuntimeError, TaskError};
pub use program::{
    CatchHandler, ErrorField, Instruction, ListItem, Operand, Program, ProgramResolver,
    QueryBinding, Register, RuntimeBinaryOp, RuntimeUnaryOp, SuspendKind,
};
pub use task::{Task, TaskId, TaskLimits, TaskOutcome};
pub use task_manager::{Effect, EffectLog, SuspendedTask, TaskManager, TaskManagerError};
pub use vm::{Frame, RegisterVm, VmHostResponse, VmState};
