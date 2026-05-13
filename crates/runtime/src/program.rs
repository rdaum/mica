use crate::RuntimeError;
use mica_relation_kernel::RelationId;
use mica_var::Value;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Register(pub u16);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operand {
    Register(Register),
    Value(Value),
}

impl From<Register> for Operand {
    fn from(value: Register) -> Self {
        Self::Register(value)
    }
}

impl From<Value> for Operand {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuspendKind {
    Commit,
    TimedMillis(u64),
    WaitingForInput(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Instruction {
    Load {
        dst: Register,
        value: Value,
    },
    Move {
        dst: Register,
        src: Register,
    },
    ScanExists {
        dst: Register,
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
    },
    Assert {
        relation: RelationId,
        values: Vec<Operand>,
    },
    Retract {
        relation: RelationId,
        values: Vec<Operand>,
    },
    RetractWhere {
        relation: RelationId,
        bindings: Vec<Option<Operand>>,
    },
    ReplaceFunctional {
        relation: RelationId,
        values: Vec<Operand>,
    },
    Branch {
        condition: Register,
        if_true: usize,
        if_false: usize,
    },
    Jump {
        target: usize,
    },
    Emit {
        value: Operand,
    },
    Commit,
    Suspend {
        kind: SuspendKind,
    },
    RollbackRetry,
    Return {
        value: Operand,
    },
    Abort {
        error: Operand,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Program {
    register_count: usize,
    instructions: Arc<[Instruction]>,
}

impl Program {
    pub fn new(
        register_count: usize,
        instructions: impl IntoIterator<Item = Instruction>,
    ) -> Result<Self, RuntimeError> {
        let instructions = instructions.into_iter().collect::<Vec<_>>();
        for instruction in &instructions {
            validate_instruction(register_count, instructions.len(), instruction)?;
        }
        Ok(Self {
            register_count,
            instructions: instructions.into(),
        })
    }

    pub fn register_count(&self) -> usize {
        self.register_count
    }

    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }
}

fn validate_instruction(
    register_count: usize,
    instruction_count: usize,
    instruction: &Instruction,
) -> Result<(), RuntimeError> {
    match instruction {
        Instruction::Load { dst, .. } => validate_register(register_count, *dst),
        Instruction::Move { dst, src } => {
            validate_register(register_count, *dst)?;
            validate_register(register_count, *src)
        }
        Instruction::ScanExists { dst, bindings, .. } => {
            validate_register(register_count, *dst)?;
            validate_bindings(register_count, bindings)
        }
        Instruction::Assert { values, .. }
        | Instruction::Retract { values, .. }
        | Instruction::ReplaceFunctional { values, .. } => {
            validate_operands(register_count, values.iter())
        }
        Instruction::RetractWhere { bindings, .. } => validate_bindings(register_count, bindings),
        Instruction::Branch {
            condition,
            if_true,
            if_false,
        } => {
            validate_register(register_count, *condition)?;
            validate_target(instruction_count, *if_true)?;
            validate_target(instruction_count, *if_false)
        }
        Instruction::Jump { target } => validate_target(instruction_count, *target),
        Instruction::Emit { value }
        | Instruction::Return { value }
        | Instruction::Abort { error: value } => validate_operand(register_count, value),
        Instruction::Commit | Instruction::Suspend { .. } | Instruction::RollbackRetry => Ok(()),
    }
}

fn validate_bindings(
    register_count: usize,
    bindings: &[Option<Operand>],
) -> Result<(), RuntimeError> {
    validate_operands(register_count, bindings.iter().filter_map(Option::as_ref))
}

fn validate_operands<'a>(
    register_count: usize,
    operands: impl IntoIterator<Item = &'a Operand>,
) -> Result<(), RuntimeError> {
    for operand in operands {
        validate_operand(register_count, operand)?;
    }
    Ok(())
}

fn validate_operand(register_count: usize, operand: &Operand) -> Result<(), RuntimeError> {
    match operand {
        Operand::Register(register) => validate_register(register_count, *register),
        Operand::Value(_) => Ok(()),
    }
}

fn validate_register(register_count: usize, register: Register) -> Result<(), RuntimeError> {
    if register.0 as usize >= register_count {
        return Err(RuntimeError::RegisterOutOfBounds {
            register: register.0,
            register_count,
        });
    }
    Ok(())
}

fn validate_target(instruction_count: usize, target: usize) -> Result<(), RuntimeError> {
    if target >= instruction_count {
        return Err(RuntimeError::InvalidBranchTarget {
            target,
            instruction_count,
        });
    }
    Ok(())
}
