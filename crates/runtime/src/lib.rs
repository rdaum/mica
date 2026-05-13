//! Transactional task runtime for Mica.
//!
//! This crate is the first executable runtime slice: a register-based
//! interpreter that runs over `mica-relation-kernel` transactions and reports
//! host responses at commit, suspend, retry, and completion boundaries.

mod builtin;
mod error;
mod program;
mod scheduler;
mod task;
mod vm;

#[cfg(test)]
mod tests;

pub use builtin::{Builtin, BuiltinContext, BuiltinRegistry};
pub use error::{RuntimeError, TaskError};
pub use program::{
    CatchHandler, ErrorField, Instruction, ListItem, Operand, Program, ProgramResolver,
    QueryBinding, Register, RuntimeBinaryOp, RuntimeUnaryOp, SuspendKind,
};
pub use scheduler::{Effect, EffectLog, Scheduler, SchedulerError, SuspendedTask};
pub use task::{Task, TaskId, TaskLimits, TaskOutcome};
pub use vm::{Frame, RegisterVm, VmHostResponse, VmState};
