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

//! Cranelift code generation for Mica's process-local value representation.

mod emitter;
mod float_loop;
mod integer_loop;
mod natural_loop;

pub use emitter::{
    EmittedValue, FloatComparison, IntegerComparison, ScalarComparison, ValueEmitter,
};
pub use float_loop::{
    CompiledFloatLoop, FloatArithmetic, FloatLoopError, FloatLoopOutcome, FloatLoopPlan,
};
pub use integer_loop::{CompiledIntegerLoop, IntegerLoopError, IntegerLoopOutcome};
pub use natural_loop::{
    CompiledNaturalLoop, NaturalLoopError, NaturalLoopInstruction, NaturalLoopOutcome,
    NaturalLoopPlan,
};
