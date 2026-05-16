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
use rart::{
    OverflowKey, OverflowKeyBuilder, Slot, SlotUpdate, VersionedAdaptiveRadixTree, VisitControl,
};
use std::collections::BTreeSet;
use std::fmt;
use std::slice;
use std::sync::Arc;

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
    pub(crate) tuples: Arc<BTreeSet<Tuple>>,
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
            tuples: Arc::new(BTreeSet::new()),
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

    pub(crate) fn join_eq(
        &self,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right: &Self,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        if left_bindings.len() != self.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: self.metadata.id(),
                expected: self.metadata.arity(),
                actual: left_bindings.len(),
            });
        }
        if right_bindings.len() != right.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: right.metadata.id(),
                expected: right.metadata.arity(),
                actual: right_bindings.len(),
            });
        }

        let Some(left_index) = self.index_for_positions(left_positions) else {
            return Ok(None);
        };
        let Some(right_index) = right.index_for_positions(right_positions) else {
            return Ok(None);
        };

        let mut out = BTreeSet::new();
        left_index.intersect_values_with(right_index, |left_bucket, right_bucket| {
            for left_tuple in left_bucket {
                if !left_tuple.matches_bindings(left_bindings) {
                    continue;
                }
                for right_tuple in right_bucket {
                    if right_tuple.matches_bindings(right_bindings) {
                        out.insert(left_tuple.concat(right_tuple));
                    }
                }
            }
        });
        Ok(Some(out.into_iter().collect()))
    }

    pub(crate) fn has_exact_index(&self, positions: &[u16]) -> bool {
        self.index_for_positions(positions).is_some()
    }

    pub(crate) fn insert(&mut self, tuple: Tuple) {
        if Arc::make_mut(&mut self.tuples).insert(tuple.clone()) {
            for index in &mut self.indexes {
                index.insert(tuple.clone());
            }
        }
    }

    pub(crate) fn remove(&mut self, tuple: &Tuple) {
        if Arc::make_mut(&mut self.tuples).remove(tuple) {
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
        let mut bindings = vec![None; self.metadata.arity() as usize];
        for (position, value) in positions.iter().zip(&key.0) {
            bindings[*position as usize] = Some(value.clone());
        }

        let mut found = None;
        self.visit(&bindings, &mut |tuple| {
            found = Some(tuple.clone());
            Ok(ScanControl::Stop)
        })
        .ok()?;
        found
    }

    fn best_index(&self, bindings: &[Option<Value>]) -> Option<(&TupleIndex, usize)> {
        self.indexes
            .iter()
            .map(|index| (index, index.spec.leading_bound_count(bindings)))
            .filter(|(_, count)| *count > 0)
            .max_by_key(|(_, count)| *count)
    }

    fn index_for_positions(&self, positions: &[u16]) -> Option<&TupleIndex> {
        self.indexes
            .iter()
            .find(|index| index.spec.positions() == positions)
    }
}

struct TupleIndex {
    spec: crate::TupleIndexSpec,
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, TupleBucket>,
}

impl fmt::Debug for TupleIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tuple_count = self
            .entries
            .values_iter()
            .map(TupleBucket::len)
            .sum::<usize>();
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
        self.entries.update_k(&key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(TupleBucket::one(tuple)),
            Slot::Occupied(bucket) => {
                bucket.insert(tuple);
                SlotUpdate::Keep
            }
        });
    }

    fn remove(&mut self, tuple: &Tuple) {
        let key = self.tuple_key(tuple);
        self.entries.update_k(&key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Keep,
            Slot::Occupied(bucket) => {
                bucket.remove(tuple);
                if bucket.is_empty() {
                    SlotUpdate::Remove
                } else {
                    SlotUpdate::Keep
                }
            }
        });
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
            .try_prefix_values_for_each_k(&prefix, |bucket| {
                for tuple in bucket {
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

    fn intersect_values_with(&self, other: &Self, visit: impl FnMut(&TupleBucket, &TupleBucket)) {
        self.entries.intersect_values_with(&other.entries, visit);
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
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, TupleBucket>,
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
        visit: impl FnMut(&TupleBucket, &TupleBucket),
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
        self.entries.update_k(&key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(TupleBucket::one(tuple)),
            Slot::Occupied(bucket) => {
                bucket.insert(tuple);
                SlotUpdate::Keep
            }
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TupleBucket {
    Empty,
    One(Tuple),
    Many(Vec<Tuple>),
}

impl TupleBucket {
    fn one(tuple: Tuple) -> Self {
        Self::One(tuple)
    }

    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Many(tuples) => tuples.len(),
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn insert(&mut self, tuple: Tuple) -> bool {
        match self {
            Self::Empty => {
                *self = Self::One(tuple);
                true
            }
            Self::One(existing) if *existing == tuple => false,
            Self::One(existing) => {
                let tuples = if tuple < *existing {
                    vec![tuple, existing.clone()]
                } else {
                    vec![existing.clone(), tuple]
                };
                *self = Self::Many(tuples);
                true
            }
            Self::Many(tuples) => match tuples.binary_search(&tuple) {
                Ok(_) => false,
                Err(index) => {
                    tuples.insert(index, tuple);
                    true
                }
            },
        }
    }

    fn remove(&mut self, tuple: &Tuple) -> bool {
        match self {
            Self::Empty => false,
            Self::One(existing) if existing == tuple => {
                *self = Self::Empty;
                true
            }
            Self::One(_) => false,
            Self::Many(tuples) => {
                let Ok(index) = tuples.binary_search(tuple) else {
                    return false;
                };
                tuples.remove(index);
                match tuples.len() {
                    0 => *self = Self::Empty,
                    1 => *self = Self::One(tuples.pop().expect("one tuple remains")),
                    _ => {}
                }
                true
            }
        }
    }

    fn iter(&self) -> TupleBucketIter<'_> {
        match self {
            Self::Empty => TupleBucketIter::Empty,
            Self::One(tuple) => TupleBucketIter::One(Some(tuple)),
            Self::Many(tuples) => TupleBucketIter::Many(tuples.iter()),
        }
    }
}

impl<'a> IntoIterator for &'a TupleBucket {
    type Item = &'a Tuple;
    type IntoIter = TupleBucketIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) enum TupleBucketIter<'a> {
    Empty,
    One(Option<&'a Tuple>),
    Many(slice::Iter<'a, Tuple>),
}

impl<'a> Iterator for TupleBucketIter<'a> {
    type Item = &'a Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::One(tuple) => tuple.take(),
            Self::Many(iter) => iter.next(),
        }
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
