//! Transactional task runtime for Mica.
//!
//! This crate is the first executable runtime slice: a register-based
//! interpreter that runs over `mica-relation-kernel` transactions and reports
//! host responses at commit, suspend, retry, and completion boundaries.

mod error;
mod program;
mod task;
mod vm;

#[cfg(test)]
mod tests;

pub use error::{RuntimeError, TaskError};
pub use program::{Instruction, Operand, Program, Register, SuspendKind};
pub use task::{Task, TaskId, TaskLimits, TaskOutcome};
pub use vm::{RegisterVm, VmHostResponse, VmState};
