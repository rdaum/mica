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

//! Register-based bytecode VM for Mica.
//!
//! This crate defines Mica bytecode programs and the register interpreter that
//! executes them until a host boundary is reached. Task scheduling, source
//! execution, filein/fileout, and live-environment concerns live in
//! `mica-runtime`.

mod authority;
mod builtin;
mod effect;
mod error;
mod program;
mod vm;

pub use authority::{AuthorityContext, CapabilityGrant, CapabilityOp, CapabilityScope};
pub use builtin::{Builtin, BuiltinContext, BuiltinRegistry, RuntimeContext};
pub use effect::Emission;
pub use error::RuntimeError;
pub use program::{
    CatchHandler, ErrorField, Instruction, ListItem, Operand, Program, ProgramResolver,
    QueryBinding, Register, RuntimeBinaryOp, RuntimeUnaryOp, SuspendKind,
};
pub use vm::{Frame, RegisterVm, VmHostContext, VmHostResponse, VmState};
