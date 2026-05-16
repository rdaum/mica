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

use crate::RelationId;
use mica_var::{Symbol, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationSchema {
    arity: u16,
    argument_names: Vec<Option<Symbol>>,
}

impl RelationSchema {
    pub fn new(arity: u16) -> Self {
        Self {
            arity,
            argument_names: vec![None; arity as usize],
        }
    }

    pub fn arity(&self) -> u16 {
        self.arity
    }

    pub fn argument_name(&self, position: u16) -> Option<Symbol> {
        self.argument_names
            .get(position as usize)
            .copied()
            .flatten()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationMetadata {
    id: RelationId,
    name: Symbol,
    schema: RelationSchema,
    pub(crate) indexes: Vec<TupleIndexSpec>,
    conflict_policy: ConflictPolicy,
}

impl RelationMetadata {
    pub fn new(id: RelationId, name: Symbol, arity: u16) -> Self {
        Self {
            id,
            name,
            schema: RelationSchema::new(arity),
            indexes: vec![TupleIndexSpec::all_positions(arity)],
            conflict_policy: ConflictPolicy::Set,
        }
    }

    pub fn with_index(mut self, positions: impl IntoIterator<Item = u16>) -> Self {
        self.indexes.push(TupleIndexSpec::new(positions));
        self
    }

    pub fn with_argument_name(mut self, position: u16, name: Symbol) -> Self {
        if let Some(slot) = self.schema.argument_names.get_mut(position as usize) {
            *slot = Some(name);
        }
        self
    }

    pub fn with_conflict_policy(mut self, conflict_policy: ConflictPolicy) -> Self {
        self.conflict_policy = conflict_policy;
        self
    }

    pub fn id(&self) -> RelationId {
        self.id
    }

    pub fn arity(&self) -> u16 {
        self.schema.arity()
    }

    pub fn name(&self) -> Symbol {
        self.name
    }

    pub fn argument_name(&self, position: u16) -> Option<Symbol> {
        self.schema.argument_name(position)
    }

    pub fn indexes(&self) -> &[TupleIndexSpec] {
        &self.indexes
    }

    pub fn conflict_policy(&self) -> &ConflictPolicy {
        &self.conflict_policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleIndexSpec {
    pub(crate) positions: Vec<u16>,
}

impl TupleIndexSpec {
    pub fn new(positions: impl IntoIterator<Item = u16>) -> Self {
        Self {
            positions: positions.into_iter().collect(),
        }
    }

    pub fn positions(&self) -> &[u16] {
        &self.positions
    }

    pub(crate) fn all_positions(arity: u16) -> Self {
        Self::new(0..arity)
    }

    pub(crate) fn leading_bound_count(&self, bindings: &[Option<Value>]) -> usize {
        self.positions
            .iter()
            .take_while(|position| bindings[**position as usize].is_some())
            .count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictPolicy {
    Set,
    Functional { key_positions: Vec<u16> },
    EventAppend,
}
