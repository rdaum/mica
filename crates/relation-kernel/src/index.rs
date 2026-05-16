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

use crate::ScanControl;
use crate::error::KernelError;
use crate::metadata::RelationMetadata;
use crate::tuple::{Tuple, TupleKey};
use mica_var::{OrderedKeySink, Value};
use rart::{OverflowKey, OverflowKeyBuilder, VersionedAdaptiveRadixTree, VisitControl};
use std::collections::BTreeSet;
use std::fmt;

pub(crate) type RadixTupleKey = OverflowKey<64, 16>;

pub(crate) struct RadixTupleKeyBuilder(OverflowKeyBuilder<64, 16>);

impl RadixTupleKeyBuilder {
    pub(crate) fn new() -> Self {
        Self(RadixTupleKey::builder())
    }

    pub(crate) fn finish(self) -> RadixTupleKey {
        self.0.finish()
    }
}

impl OrderedKeySink for RadixTupleKeyBuilder {
    fn push_byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

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

    pub(crate) fn cardinality(&self) -> usize {
        self.tuples.len()
    }

    pub(crate) fn estimate_scan_count(
        &self,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError> {
        if bindings.len() != self.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: self.metadata.id(),
                expected: self.metadata.arity(),
                actual: bindings.len(),
            });
        }

        let Some((index, bound_count)) = self.best_index(bindings) else {
            if bindings.iter().any(Option::is_some) {
                return Ok(self
                    .tuples
                    .iter()
                    .filter(|tuple| tuple.matches_bindings(bindings))
                    .count());
            }
            return Ok(self.cardinality());
        };

        index.estimate_prefix_count(bindings, bound_count)
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

        index.scan_prefix(bindings, bound_count)
    }

    pub(crate) fn visit(
        &self,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        if bindings.len() != self.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: self.metadata.id(),
                expected: self.metadata.arity(),
                actual: bindings.len(),
            });
        }

        let Some((index, bound_count)) = self.best_index(bindings) else {
            for tuple in self
                .tuples
                .iter()
                .filter(|tuple| tuple.matches_bindings(bindings))
            {
                if visitor(tuple)? == ScanControl::Stop {
                    return Ok(());
                }
            }
            return Ok(());
        };

        index.visit_prefix(bindings, bound_count, &mut |tuple| {
            if tuple.matches_bindings(bindings) {
                visitor(tuple)
            } else {
                Ok(ScanControl::Continue)
            }
        })
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

struct TupleIndex {
    spec: crate::TupleIndexSpec,
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, BTreeSet<Tuple>>,
}

impl fmt::Debug for TupleIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tuple_count = self.entries.values_iter().map(BTreeSet::len).sum::<usize>();
        f.debug_struct("TupleIndex")
            .field("spec", &self.spec)
            .field("tuple_count", &tuple_count)
            .finish_non_exhaustive()
    }
}

impl Clone for TupleIndex {
    fn clone(&self) -> Self {
        Self {
            spec: self.spec.clone(),
            entries: self.entries.clone(),
        }
    }
}

impl TupleIndex {
    fn empty(spec: crate::TupleIndexSpec) -> Self {
        Self {
            spec,
            entries: VersionedAdaptiveRadixTree::new(),
        }
    }

    fn insert(&mut self, tuple: Tuple) {
        let key = self.tuple_key(&tuple);
        if let Some(bucket) = self.entries.get_mut_k(&key) {
            bucket.insert(tuple);
            return;
        }
        let mut bucket = BTreeSet::new();
        bucket.insert(tuple);
        self.entries.insert_k(&key, bucket);
    }

    fn remove(&mut self, tuple: &Tuple) {
        let key = self.tuple_key(tuple);
        let Some(bucket) = self.entries.get_mut_k(&key) else {
            return;
        };
        bucket.remove(tuple);
        if bucket.is_empty() {
            self.entries.remove_k(&key);
        }
    }

    fn scan_prefix(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
    ) -> Result<Vec<Tuple>, KernelError> {
        let mut out = Vec::new();
        self.visit_prefix(bindings, bound_count, &mut |tuple| {
            out.push(tuple.clone());
            Ok(ScanControl::Continue)
        })?;
        Ok(out)
    }

    fn estimate_prefix_count(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
    ) -> Result<usize, KernelError> {
        let mut count = 0usize;
        self.visit_prefix(bindings, bound_count, &mut |tuple| {
            if tuple.matches_bindings(bindings) {
                count += 1;
            }
            Ok(ScanControl::Continue)
        })?;
        Ok(count)
    }

    fn visit_prefix(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        let prefix = self.binding_prefix_key(bindings, bound_count);
        self.entries
            .try_prefix_values_for_each_k(&prefix, |tuples| {
                for tuple in tuples {
                    if !tuple.matches_bindings(bindings) {
                        continue;
                    }
                    match visitor(tuple) {
                        Ok(ScanControl::Continue) => {}
                        Ok(ScanControl::Stop) => return Ok(VisitControl::Stop),
                        Err(error) => return Err(error),
                    }
                }
                Ok(VisitControl::Continue)
            })
    }

    fn tuple_key(&self, tuple: &Tuple) -> RadixTupleKey {
        self.key_from_values(
            self.spec
                .positions
                .iter()
                .map(|position| &tuple.values()[*position as usize]),
        )
    }

    fn binding_prefix_key(&self, bindings: &[Option<Value>], bound_count: usize) -> RadixTupleKey {
        self.key_from_values(
            self.spec
                .positions
                .iter()
                .take(bound_count)
                .filter_map(|position| bindings[*position as usize].as_ref()),
        )
    }

    fn key_from_values<'a>(&self, values: impl IntoIterator<Item = &'a Value>) -> RadixTupleKey {
        key_from_values(values)
    }
}

#[derive(Clone)]
pub(crate) struct ProjectedTupleIndex {
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, BTreeSet<Tuple>>,
}

impl ProjectedTupleIndex {
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = Tuple>, positions: &[u16]) -> Self {
        let mut index = Self {
            entries: VersionedAdaptiveRadixTree::new(),
        };
        for row in rows {
            index.insert(row, positions);
        }
        index
    }

    pub(crate) fn intersect_values_with(
        &self,
        other: &Self,
        visit: impl FnMut(&BTreeSet<Tuple>, &BTreeSet<Tuple>),
    ) {
        self.entries.intersect_values_with(&other.entries, visit);
    }

    pub(crate) fn matching_left_rows(&self, other: &Self, mut visit: impl FnMut(&Tuple)) {
        self.intersect_values_with(other, |left, _right| {
            for tuple in left {
                visit(tuple);
            }
        });
    }

    fn insert(&mut self, tuple: Tuple, positions: &[u16]) {
        let key = key_from_values(
            positions
                .iter()
                .map(|position| &tuple.values()[*position as usize]),
        );
        if let Some(bucket) = self.entries.get_mut_k(&key) {
            bucket.insert(tuple);
            return;
        }
        let mut bucket = BTreeSet::new();
        bucket.insert(tuple);
        self.entries.insert_k(&key, bucket);
    }
}

pub(crate) fn key_from_values<'a>(values: impl IntoIterator<Item = &'a Value>) -> RadixTupleKey {
    let mut key = RadixTupleKeyBuilder::new();
    for value in values {
        value.encode_ordered_into(&mut key);
    }
    key.finish()
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
