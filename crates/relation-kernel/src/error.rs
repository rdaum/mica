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

use crate::{RelationId, RuleError, Tuple};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KernelError {
    UnknownRelation(RelationId),
    UnknownRule(crate::FactId),
    RelationAlreadyExists(RelationId),
    ArityMismatch {
        relation: RelationId,
        expected: u16,
        actual: usize,
    },
    NonPersistentValue {
        relation: RelationId,
        tuple: Tuple,
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
