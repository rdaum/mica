use crate::error::KernelError;
use crate::metadata::RelationMetadata;
use crate::tuple::{Tuple, TupleKey};
use mica_var::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub(crate) struct RelationState {
    metadata: RelationMetadata,
    pub(crate) tuples: BTreeSet<Tuple>,
    indexes: Vec<TupleIndex>,
}

impl RelationState {
    pub(crate) fn empty(metadata: RelationMetadata) -> Result<Self, KernelError> {
        validate_metadata(&metadata)?;
        let indexes = metadata
            .indexes
            .iter()
            .cloned()
            .map(TupleIndex::empty)
            .collect();
        Ok(Self {
            metadata,
            tuples: BTreeSet::new(),
            indexes,
        })
    }

    pub(crate) fn metadata(&self) -> &RelationMetadata {
        &self.metadata
    }

    pub(crate) fn scan(&self, bindings: &[Option<Value>]) -> Result<Vec<Tuple>, KernelError> {
        if bindings.len() != self.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: self.metadata.id(),
                expected: self.metadata.arity(),
                actual: bindings.len(),
            });
        }

        let Some((index, bound_count)) = self.best_index(bindings) else {
            return Ok(self
                .tuples
                .iter()
                .filter(|tuple| tuple.matches_bindings(bindings))
                .cloned()
                .collect());
        };

        let prefix = index.spec.prefix(bindings, bound_count);
        Ok(index
            .scan_prefix(&prefix, bound_count)
            .into_iter()
            .filter(|tuple| tuple.matches_bindings(bindings))
            .collect())
    }

    pub(crate) fn insert(&mut self, tuple: Tuple) {
        if self.tuples.insert(tuple.clone()) {
            for index in &mut self.indexes {
                index.insert(tuple.clone());
            }
        }
    }

    pub(crate) fn remove(&mut self, tuple: &Tuple) {
        if self.tuples.remove(tuple) {
            for index in &mut self.indexes {
                index.remove(tuple);
            }
        }
    }

    pub(crate) fn tuple_for_key(&self, positions: &[u16], key_tuple: &Tuple) -> Option<Tuple> {
        let key = key_tuple.project(positions);
        self.tuple_for_projected_key(positions, &key)
    }

    pub(crate) fn tuple_for_projected_key(
        &self,
        positions: &[u16],
        key: &TupleKey,
    ) -> Option<Tuple> {
        self.tuples
            .iter()
            .find(|tuple| tuple.project(positions) == *key)
            .cloned()
    }

    fn best_index(&self, bindings: &[Option<Value>]) -> Option<(&TupleIndex, usize)> {
        self.indexes
            .iter()
            .map(|index| (index, index.spec.leading_bound_count(bindings)))
            .filter(|(_, count)| *count > 0)
            .max_by_key(|(_, count)| *count)
    }
}

#[derive(Clone, Debug)]
struct TupleIndex {
    spec: crate::TupleIndexSpec,
    entries: BTreeMap<TupleKey, BTreeSet<Tuple>>,
}

impl TupleIndex {
    fn empty(spec: crate::TupleIndexSpec) -> Self {
        Self {
            spec,
            entries: BTreeMap::new(),
        }
    }

    fn insert(&mut self, tuple: Tuple) {
        self.entries
            .entry(tuple.project(&self.spec.positions))
            .or_default()
            .insert(tuple);
    }

    fn remove(&mut self, tuple: &Tuple) {
        let key = tuple.project(&self.spec.positions);
        let Some(bucket) = self.entries.get_mut(&key) else {
            return;
        };
        bucket.remove(tuple);
        if bucket.is_empty() {
            self.entries.remove(&key);
        }
    }

    fn scan_prefix(&self, prefix: &TupleKey, bound_count: usize) -> Vec<Tuple> {
        self.entries
            .iter()
            .filter(|(key, _)| key.0.len() >= bound_count && key.0[..bound_count] == prefix.0)
            .flat_map(|(_, tuples)| tuples.iter().cloned())
            .collect()
    }
}

fn validate_metadata(metadata: &RelationMetadata) -> Result<(), KernelError> {
    for index in &metadata.indexes {
        for position in &index.positions {
            if *position >= metadata.arity() {
                return Err(KernelError::InvalidIndex {
                    relation: metadata.id(),
                    position: *position,
                    arity: metadata.arity(),
                });
            }
        }
    }
    if let crate::ConflictPolicy::Functional { key_positions } = metadata.conflict_policy() {
        for position in key_positions {
            if *position >= metadata.arity() {
                return Err(KernelError::InvalidIndex {
                    relation: metadata.id(),
                    position: *position,
                    arity: metadata.arity(),
                });
            }
        }
    }
    Ok(())
}
