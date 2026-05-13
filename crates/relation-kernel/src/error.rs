use crate::{RelationId, RuleError, Tuple};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KernelError {
    UnknownRelation(RelationId),
    RelationAlreadyExists(RelationId),
    ArityMismatch {
        relation: RelationId,
        expected: u16,
        actual: usize,
    },
    InvalidIndex {
        relation: RelationId,
        position: u16,
        arity: u16,
    },
    Persistence(String),
    Rule(RuleError),
    Conflict(Conflict),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Conflict {
    pub relation: RelationId,
    pub tuple: Tuple,
    pub kind: ConflictKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictKind {
    AssertRetract,
    FunctionalKeyChanged,
}
