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

use mica_relation_kernel::{KernelError, RelationId};
use mica_var::{Symbol, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    ProgramCounterOutOfBounds {
        ip: usize,
    },
    RegisterOutOfBounds {
        register: u16,
        register_count: usize,
    },
    InvalidBranchTarget {
        target: usize,
        instruction_count: usize,
    },
    InstructionBudgetExceeded {
        budget: usize,
    },
    MaxCallDepthExceeded {
        max_depth: usize,
    },
    InvalidCallArity {
        expected_at_most: usize,
        actual: usize,
    },
    NoApplicableMethod {
        selector: Value,
    },
    AmbiguousDispatch {
        selector: Value,
        methods: Vec<Value>,
    },
    UnknownBuiltin {
        name: Symbol,
    },
    InvalidBuiltinCall {
        name: Symbol,
        message: String,
    },
    PermissionDenied {
        operation: &'static str,
        target: Value,
    },
    MissingMethodProgram {
        method: Value,
    },
    MissingProgramArtifact {
        program: Value,
    },
    ProgramArtifact(String),
    EmptyCallStack,
    EmptyTryStack,
    InvalidRaisedValue(Value),
    InvalidErrorMessage(Value),
    Kernel(KernelError),
    Aborted(Value),
}

impl From<KernelError> for RuntimeError {
    fn from(value: KernelError) -> Self {
        Self::Kernel(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskError {
    Runtime(RuntimeError),
    ConflictRetriesExceeded { retries: u8 },
    MissingTransaction,
    UnknownRelation(RelationId),
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
