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
use crate::index::RelationState;
use mica_var::Identity;
use rart::{ArrayKey, VersionedAdaptiveRadixTree};
use std::fmt;

type RelationStateKey = ArrayKey<8>;

#[derive(Clone)]
pub(crate) struct RelationStates {
    entries: VersionedAdaptiveRadixTree<RelationStateKey, RelationState>,
    len: usize,
}

impl fmt::Debug for RelationStates {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationStates")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl RelationStates {
    pub(crate) fn new() -> Self {
        Self {
            entries: VersionedAdaptiveRadixTree::new(),
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn contains_key(&self, relation: &RelationId) -> bool {
        self.get(relation).is_some()
    }

    pub(crate) fn get(&self, relation: &RelationId) -> Option<&RelationState> {
        self.entries.get(relation.raw())
    }

    pub(crate) fn get_mut(&mut self, relation: &RelationId) -> Option<&mut RelationState> {
        self.entries.get_mut(relation.raw())
    }

    pub(crate) fn insert(&mut self, relation: RelationId, state: RelationState) {
        if !self.entries.insert(relation.raw(), state) {
            self.len += 1;
        }
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &RelationState> {
        self.entries.values_iter()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (RelationId, &RelationState)> {
        self.entries.iter().map(|(key, state)| {
            let relation = Identity::new(key.to_be_u64())
                .expect("relation-state keys must contain valid identity words");
            (relation, state)
        })
    }
}

impl Default for RelationStates {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationMetadata, Tuple};
    use mica_var::{Symbol, Value};

    fn relation(id: u64, name: &str) -> (RelationId, RelationState) {
        let id = Identity::new(id).expect("test relation identities must be valid");
        let metadata = RelationMetadata::new(id, Symbol::intern(name), 1);
        let state = RelationState::empty(metadata).expect("test relation metadata must be valid");
        (id, state)
    }

    #[test]
    fn clone_isolated_from_relation_mutation() {
        let (id, state) = relation(1, "Example");
        let mut original = RelationStates::new();
        original.insert(id, state);
        let mut changed = original.clone();

        changed
            .get_mut(&id)
            .expect("cloned relation must exist")
            .insert(Tuple::from([
                Value::int(7).expect("test integer must fit in a value")
            ]));

        assert_eq!(original.get(&id).unwrap().cardinality(), 0);
        assert_eq!(changed.get(&id).unwrap().cardinality(), 1);
    }

    #[test]
    fn replacement_preserves_length_and_clone_contents() {
        let (id, state) = relation(2, "Before");
        let mut original = RelationStates::new();
        original.insert(id, state);
        let mut changed = original.clone();
        let (_, replacement) = relation(2, "After");

        changed.insert(id, replacement);

        assert_eq!(original.len(), 1);
        assert_eq!(changed.len(), 1);
        assert_eq!(
            original.get(&id).unwrap().metadata().name(),
            Symbol::intern("Before")
        );
        assert_eq!(
            changed.get(&id).unwrap().metadata().name(),
            Symbol::intern("After")
        );
    }
}
