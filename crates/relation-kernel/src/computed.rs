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

use crate::query::RelationRead;
use crate::{KernelError, RelationId, RelationMetadata, RuleDefinition, Tuple, Version};
use mica_var::Value;
use std::fmt;
use std::sync::Arc;

pub trait ComputedRelationRead: RelationRead {
    fn version(&self) -> Version;

    fn relation_metadata_vec(&self) -> Vec<RelationMetadata>;

    fn rules_vec(&self) -> Vec<RuleDefinition>;

    fn extensional_facts(&self) -> Result<Vec<(RelationId, Tuple)>, KernelError>;
}

pub trait ComputedRelation: Send + Sync {
    fn name(&self) -> &'static str;

    fn matches(&self, metadata: &RelationMetadata) -> bool;

    fn required_bound_positions(&self, _metadata: &RelationMetadata) -> &[u16] {
        &[]
    }

    fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError>;

    fn estimate(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        Ok(self.scan(reader, metadata, bindings)?.len())
    }
}

#[derive(Clone, Default)]
pub struct ComputedRelationRegistry {
    relations: Vec<Arc<dyn ComputedRelation>>,
}

impl ComputedRelationRegistry {
    pub fn new(relations: impl IntoIterator<Item = Arc<dyn ComputedRelation>>) -> Self {
        Self {
            relations: relations.into_iter().collect(),
        }
    }

    pub fn is_computed_relation(&self, metadata: &RelationMetadata) -> bool {
        self.find(metadata).is_some()
    }

    pub fn scan(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Option<Result<Vec<Tuple>, KernelError>> {
        let relation = self.find(metadata)?;
        Some(scan_checked(relation.as_ref(), reader, metadata, bindings))
    }

    pub fn estimate(
        &self,
        reader: &dyn ComputedRelationRead,
        metadata: &RelationMetadata,
        bindings: &[Option<Value>],
    ) -> Option<Result<usize, KernelError>> {
        let relation = self.find(metadata)?;
        Some(estimate_checked(
            relation.as_ref(),
            reader,
            metadata,
            bindings,
        ))
    }

    fn find(&self, metadata: &RelationMetadata) -> Option<&Arc<dyn ComputedRelation>> {
        self.relations
            .iter()
            .find(|relation| relation.matches(metadata))
    }
}

impl fmt::Debug for ComputedRelationRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self
            .relations
            .iter()
            .map(|relation| relation.name())
            .collect::<Vec<_>>();
        f.debug_struct("ComputedRelationRegistry")
            .field("relations", &names)
            .finish()
    }
}

fn scan_checked(
    relation: &dyn ComputedRelation,
    reader: &dyn ComputedRelationRead,
    metadata: &RelationMetadata,
    bindings: &[Option<Value>],
) -> Result<Vec<Tuple>, KernelError> {
    validate_bindings(relation, metadata, bindings)?;
    relation.scan(reader, metadata, bindings)
}

fn estimate_checked(
    relation: &dyn ComputedRelation,
    reader: &dyn ComputedRelationRead,
    metadata: &RelationMetadata,
    bindings: &[Option<Value>],
) -> Result<usize, KernelError> {
    validate_bindings(relation, metadata, bindings)?;
    relation.estimate(reader, metadata, bindings)
}

fn validate_bindings(
    relation: &dyn ComputedRelation,
    metadata: &RelationMetadata,
    bindings: &[Option<Value>],
) -> Result<(), KernelError> {
    if bindings.len() != metadata.arity() as usize {
        return Err(KernelError::ArityMismatch {
            relation: metadata.id(),
            expected: metadata.arity(),
            actual: bindings.len(),
        });
    }

    let missing = relation
        .required_bound_positions(metadata)
        .iter()
        .copied()
        .filter(|position| bindings.get(*position as usize).is_none_or(Option::is_none))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(KernelError::MissingRequiredBindings {
        relation: metadata.id(),
        positions: missing,
    })
}
