use mica_relation_kernel::{KernelError, RelationId};
use mica_var::Value;

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
    MissingMethodProgram {
        method: Value,
    },
    MissingProgramArtifact {
        program: Value,
    },
    ProgramArtifact(String),
    EmptyCallStack,
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
