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
use crate::tuple::{Tuple, TupleKey, finish_tuple_rows};
use mica_var::{OrderedKeySink, Value};
use rart::{
    AdaptiveRadixTree, OverflowKey, OverflowKeyBuilder, Slot, SlotUpdate,
    VersionedAdaptiveRadixTree, VisitControl,
};
use std::collections::BTreeSet;
use std::fmt;
use std::slice;
use std::sync::Arc;

pub(crate) type RadixTupleKey = OverflowKey<64, 16>;

#[derive(Clone, Copy)]
pub(crate) enum RelationMutationKind {
    Assert,
    Retract,
}

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
    tuples: TupleStore,
    indexes: Vec<TupleIndex>,
}

impl RelationState {
    pub(crate) fn empty(metadata: RelationMetadata) -> Result<Self, KernelError> {
        validate_metadata(&metadata)?;
        let arity = metadata.arity();
        let indexes = metadata
            .indexes
            .iter()
            .filter(|spec| !is_natural_full_tuple_index(spec.positions(), arity))
            .cloned()
            .map(|spec| TupleIndex::empty(spec, arity))
            .collect();
        Ok(Self {
            metadata,
            tuples: TupleStore::empty(),
            indexes,
        })
    }

    pub(crate) fn metadata(&self) -> &RelationMetadata {
        &self.metadata
    }

    pub(crate) fn cardinality(&self) -> usize {
        self.tuples.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tuples.is_empty()
    }

    pub(crate) fn contains_tuple(&self, tuple: &Tuple) -> bool {
        self.tuples.contains(tuple)
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

        match self.best_access(bindings) {
            Some(ScanAccess::TupleStore(bound_count)) => {
                self.tuples.estimate_prefix_count(bindings, bound_count)
            }
            Some(ScanAccess::Index(index, bound_count)) => {
                index.estimate_prefix_count(bindings, bound_count)
            }
            None if bindings.iter().any(Option::is_some) => {
                Ok(self.tuples.matching_count(bindings))
            }
            None => Ok(self.cardinality()),
        }
    }

    pub(crate) fn scan(&self, bindings: &[Option<Value>]) -> Result<Vec<Tuple>, KernelError> {
        if bindings.len() != self.metadata.arity() as usize {
            return Err(KernelError::ArityMismatch {
                relation: self.metadata.id(),
                expected: self.metadata.arity(),
                actual: bindings.len(),
            });
        }

        match self.best_access(bindings) {
            Some(ScanAccess::TupleStore(bound_count)) => {
                self.tuples.scan_prefix(bindings, bound_count)
            }
            Some(ScanAccess::Index(index, bound_count)) => index.scan_prefix(bindings, bound_count),
            None => Ok(self.tuples.matching(bindings)),
        }
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

        match self.best_access(bindings) {
            Some(ScanAccess::TupleStore(bound_count)) => {
                self.tuples.visit_prefix(bindings, bound_count, visitor)
            }
            Some(ScanAccess::Index(index, bound_count)) => {
                index.visit_prefix(bindings, bound_count, visitor)
            }
            None => self.visit_matching(bindings, visitor),
        }
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

        if is_natural_full_tuple_positions(left_positions, self.metadata.arity())
            && is_natural_full_tuple_positions(right_positions, right.metadata.arity())
        {
            return Ok(Some(self.natural_full_tuple_join(
                left_bindings,
                right,
                right_bindings,
            )));
        }

        let Some(left_index) = self.index_for_positions(left_positions) else {
            return Ok(None);
        };
        let Some(right_index) = right.index_for_positions(right_positions) else {
            return Ok(None);
        };

        let mut out = Vec::new();
        left_index.intersect_values_with(right_index, |left_bucket, right_bucket| {
            for left_tuple in left_bucket {
                if !left_tuple.matches_bindings(left_bindings) {
                    continue;
                }
                for right_tuple in right_bucket {
                    if right_tuple.matches_bindings(right_bindings) {
                        out.push(left_tuple.concat(right_tuple));
                    }
                }
            }
        });
        Ok(Some(finish_tuple_rows(out)))
    }

    fn natural_full_tuple_join(
        &self,
        left_bindings: &[Option<Value>],
        right: &Self,
        right_bindings: &[Option<Value>],
    ) -> Vec<Tuple> {
        let mut out = Vec::new();
        self.tuples
            .intersect_values_with(&right.tuples, |left, right| {
                if left.matches_bindings(left_bindings) && right.matches_bindings(right_bindings) {
                    out.push(left.concat(right));
                }
            });
        finish_tuple_rows(out)
    }

    pub(crate) fn has_exact_index(&self, positions: &[u16]) -> bool {
        is_natural_full_tuple_positions(positions, self.metadata.arity())
            || self.index_for_positions(positions).is_some()
    }

    pub(crate) fn insert(&mut self, tuple: Tuple) -> bool {
        if self.tuples.insert(tuple.clone()) {
            for index in &mut self.indexes {
                index.insert(tuple.clone());
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn remove(&mut self, tuple: &Tuple) -> bool {
        if self.tuples.remove(tuple) {
            for index in &mut self.indexes {
                index.remove(tuple);
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn apply_ordered_changes<'a>(
        &mut self,
        changes: impl IntoIterator<Item = (&'a Tuple, RelationMutationKind)>,
        mut base_contains: impl FnMut(&Tuple) -> bool,
        mut on_applied: impl FnMut(&Tuple, RelationMutationKind),
    ) {
        for (tuple, kind) in changes {
            match kind {
                RelationMutationKind::Assert => {
                    if self.tuples.insert(tuple.clone()) {
                        for index in &mut self.indexes {
                            index.insert(tuple.clone());
                        }
                        on_applied(tuple, kind);
                    }
                }
                RelationMutationKind::Retract => {
                    if base_contains(tuple) && self.tuples.remove(tuple) {
                        for index in &mut self.indexes {
                            index.remove(tuple);
                        }
                        on_applied(tuple, kind);
                    }
                }
            }
        }
    }

    pub(crate) fn apply_ordered_asserts_to_empty<'a>(
        &mut self,
        tuples: impl IntoIterator<Item = &'a Tuple>,
        mut on_applied: impl FnMut(&Tuple, RelationMutationKind),
    ) {
        debug_assert!(self.tuples.is_empty());

        let mut rows = Vec::new();
        for tuple in tuples {
            if rows.last().is_some_and(|existing| existing == tuple) {
                continue;
            }
            rows.push(tuple.clone());
        }

        if rows.is_empty() {
            return;
        }

        self.tuples = TupleStore::from_sorted_unique(&rows);
        let arity = self.metadata.arity();
        for index in &mut self.indexes {
            *index = TupleIndex::from_sorted_unique_rows(index.spec.clone(), arity, &rows);
        }
        for tuple in &rows {
            on_applied(tuple, RelationMutationKind::Assert);
        }
    }

    pub(crate) fn tuple_for_key(&self, positions: &[u16], key_tuple: &Tuple) -> Option<Tuple> {
        if is_natural_full_tuple_positions(positions, self.metadata.arity()) {
            return self.tuples.tuple_for_tuple(key_tuple);
        }
        if let Some(index) = self.index_for_positions(positions) {
            return index.tuple_for_key_tuple(key_tuple);
        }

        let mut bindings = vec![None; self.metadata.arity() as usize];
        for position in positions {
            bindings[*position as usize] = Some(key_tuple.values()[*position as usize].clone());
        }

        let mut found = None;
        self.visit(&bindings, &mut |tuple| {
            found = Some(tuple.clone());
            Ok(ScanControl::Stop)
        })
        .ok()?;
        found
    }

    pub(crate) fn tuple_for_projected_key(
        &self,
        positions: &[u16],
        key: &TupleKey,
    ) -> Option<Tuple> {
        self.tuple_for_projected_values(positions, &key.0)
    }

    pub(crate) fn tuple_for_projected_values(
        &self,
        positions: &[u16],
        key_values: &[Value],
    ) -> Option<Tuple> {
        if is_natural_full_tuple_positions(positions, self.metadata.arity()) {
            return self.tuples.tuple_for_values(key_values);
        }
        if let Some(index) = self.index_for_positions(positions) {
            return index.tuple_for_key_values(key_values);
        }

        let mut bindings = vec![None; self.metadata.arity() as usize];
        for (position, value) in positions.iter().zip(key_values) {
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

    fn best_access(&self, bindings: &[Option<Value>]) -> Option<ScanAccess<'_>> {
        let tuple_store_bound_count = natural_leading_bound_count(bindings);
        let best_index = self
            .indexes
            .iter()
            .map(|index| (index, index.spec.leading_bound_count(bindings)))
            .filter(|(_, count)| *count > 0)
            .max_by_key(|(_, count)| *count);

        match (tuple_store_bound_count, best_index) {
            (0, None) => None,
            (count, None) => Some(ScanAccess::TupleStore(count)),
            (0, Some((index, count))) => Some(ScanAccess::Index(index, count)),
            (tuple_count, Some((_index, index_count))) if tuple_count > index_count => {
                Some(ScanAccess::TupleStore(tuple_count))
            }
            (_, Some((index, count))) => Some(ScanAccess::Index(index, count)),
        }
    }

    fn visit_matching(
        &self,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        let mut stopped = false;
        self.tuples.try_for_each_matching(bindings, &mut |tuple| {
            if visitor(tuple)? == ScanControl::Stop {
                stopped = true;
            }
            Ok(stopped)
        })?;
        Ok(())
    }

    fn index_for_positions(&self, positions: &[u16]) -> Option<&TupleIndex> {
        self.indexes
            .iter()
            .find(|index| index.spec.positions() == positions)
    }
}

enum ScanAccess<'a> {
    TupleStore(usize),
    Index(&'a TupleIndex, usize),
}

#[derive(Clone)]
enum TupleStore {
    Small(Arc<BTreeSet<Tuple>>),
    Radix {
        entries: VersionedAdaptiveRadixTree<RadixTupleKey, Tuple>,
        len: usize,
    },
}

impl fmt::Debug for TupleStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TupleStore")
            .field("len", &self.len())
            .field(
                "kind",
                match self {
                    Self::Small(_) => &"small",
                    Self::Radix { .. } => &"radix",
                },
            )
            .finish_non_exhaustive()
    }
}

const TUPLE_STORE_RADIX_THRESHOLD: usize = 4096;

impl TupleStore {
    fn empty() -> Self {
        Self::Small(Arc::new(BTreeSet::new()))
    }

    fn from_sorted_unique(tuples: &[Tuple]) -> Self {
        if tuples.len() <= TUPLE_STORE_RADIX_THRESHOLD {
            return Self::Small(Arc::new(tuples.iter().cloned().collect()));
        }

        let mut entries = VersionedAdaptiveRadixTree::new();
        for tuple in tuples {
            let key = key_from_values(tuple.values());
            entries.insert_k(&key, tuple.clone());
        }
        Self::Radix {
            entries,
            len: tuples.len(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Small(tuples) => tuples.len(),
            Self::Radix { len, .. } => *len,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains(&self, tuple: &Tuple) -> bool {
        match self {
            Self::Small(tuples) => tuples.contains(tuple),
            Self::Radix { entries, .. } => {
                let key = key_from_values(tuple.values());
                entries.get_k(&key).is_some()
            }
        }
    }

    fn tuple_for_values(&self, values: &[Value]) -> Option<Tuple> {
        match self {
            Self::Small(tuples) => {
                let tuple = Tuple::new(values.iter().cloned());
                tuples.get(&tuple).cloned()
            }
            Self::Radix { entries, .. } => {
                let key = key_from_values(values);
                entries.get_k(&key).cloned()
            }
        }
    }

    fn tuple_for_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        match self {
            Self::Small(tuples) => tuples.get(tuple).cloned(),
            Self::Radix { entries, .. } => {
                let key = key_from_values(tuple.values());
                entries.get_k(&key).cloned()
            }
        }
    }

    fn insert(&mut self, tuple: Tuple) -> bool {
        match self {
            Self::Small(tuples) => {
                let tuples = Arc::make_mut(tuples);
                let inserted = tuples.insert(tuple);
                if inserted && tuples.len() > TUPLE_STORE_RADIX_THRESHOLD {
                    self.promote_to_radix();
                }
                inserted
            }
            Self::Radix { entries, len } => {
                let key = key_from_values(tuple.values());
                if entries.insert_k(&key, tuple) {
                    return false;
                }
                *len += 1;
                true
            }
        }
    }

    fn remove(&mut self, tuple: &Tuple) -> bool {
        match self {
            Self::Small(tuples) => Arc::make_mut(tuples).remove(tuple),
            Self::Radix { entries, len } => {
                let key = key_from_values(tuple.values());
                if !entries.delete_k(&key) {
                    return false;
                }
                *len -= 1;
                true
            }
        }
    }

    fn matching_count(&self, bindings: &[Option<Value>]) -> usize {
        let mut count = 0usize;
        self.for_each_matching(bindings, |_| count += 1);
        count
    }

    fn estimate_prefix_count(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
    ) -> Result<usize, KernelError> {
        let mut count = 0usize;
        self.visit_prefix(bindings, bound_count, &mut |_| {
            count += 1;
            Ok(ScanControl::Continue)
        })?;
        Ok(count)
    }

    fn matching(&self, bindings: &[Option<Value>]) -> Vec<Tuple> {
        if bindings.iter().all(Option::is_none) {
            return self.all_tuples();
        }

        let mut out = Vec::new();
        self.for_each_matching(bindings, |tuple| out.push(tuple.clone()));
        out
    }

    fn all_tuples(&self) -> Vec<Tuple> {
        let mut out = Vec::with_capacity(self.len());
        match self {
            Self::Small(tuples) => out.extend(tuples.iter().cloned()),
            Self::Radix { entries, .. } => out.extend(entries.values_iter().cloned()),
        }
        out
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

    fn visit_prefix(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        match self {
            Self::Small(_) => self.try_for_each_matching(bindings, &mut |tuple| {
                Ok(visitor(tuple)? == ScanControl::Stop)
            }),
            Self::Radix { entries, .. } => {
                let prefix_covers_all_bindings =
                    natural_prefix_covers_all_bindings(bindings, bound_count);
                let prefix = key_from_values(
                    bindings
                        .iter()
                        .take(bound_count)
                        .map(|binding| binding.as_ref().expect("natural prefix should be bound")),
                );
                entries.try_prefix_values_for_each_k(&prefix, |tuple| {
                    if !prefix_covers_all_bindings && !tuple.matches_bindings(bindings) {
                        return Ok(VisitControl::Continue);
                    }
                    match visitor(tuple) {
                        Ok(ScanControl::Continue) => Ok(VisitControl::Continue),
                        Ok(ScanControl::Stop) => Ok(VisitControl::Stop),
                        Err(error) => Err(error),
                    }
                })
            }
        }
    }

    fn for_each_matching(&self, bindings: &[Option<Value>], mut visitor: impl FnMut(&Tuple)) {
        match self {
            Self::Small(tuples) => {
                for tuple in tuples.iter() {
                    if tuple.matches_bindings(bindings) {
                        visitor(tuple);
                    }
                }
            }
            Self::Radix { entries, .. } => {
                for tuple in entries.values_iter() {
                    if tuple.matches_bindings(bindings) {
                        visitor(tuple);
                    }
                }
            }
        }
    }

    fn try_for_each_matching(
        &self,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<bool, KernelError>,
    ) -> Result<(), KernelError> {
        if bindings.iter().all(Option::is_none) {
            return self.try_for_each_tuple(visitor);
        }

        match self {
            Self::Small(tuples) => {
                for tuple in tuples.iter() {
                    if tuple.matches_bindings(bindings) && visitor(tuple)? {
                        return Ok(());
                    }
                }
            }
            Self::Radix { entries, .. } => {
                for tuple in entries.values_iter() {
                    if tuple.matches_bindings(bindings) && visitor(tuple)? {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn try_for_each_tuple(
        &self,
        visitor: &mut dyn FnMut(&Tuple) -> Result<bool, KernelError>,
    ) -> Result<(), KernelError> {
        match self {
            Self::Small(tuples) => {
                for tuple in tuples.iter() {
                    if visitor(tuple)? {
                        return Ok(());
                    }
                }
            }
            Self::Radix { entries, .. } => {
                for tuple in entries.values_iter() {
                    if visitor(tuple)? {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn promote_to_radix(&mut self) {
        let Self::Small(tuples) = std::mem::replace(self, Self::empty()) else {
            return;
        };
        let tuples = match Arc::try_unwrap(tuples) {
            Ok(tuples) => tuples,
            Err(tuples) => (*tuples).clone(),
        };
        let len = tuples.len();
        let mut entries = VersionedAdaptiveRadixTree::new();
        for tuple in tuples {
            let key = key_from_values(tuple.values());
            entries.insert_k(&key, tuple);
        }
        *self = Self::Radix { entries, len };
    }

    fn intersect_values_with(&self, other: &Self, mut visitor: impl FnMut(&Tuple, &Tuple)) {
        match (self, other) {
            (Self::Small(left), Self::Small(right)) => {
                for tuple in left.intersection(right) {
                    visitor(tuple, tuple);
                }
            }
            (Self::Small(left), Self::Radix { entries, .. }) => {
                for left_tuple in left.iter() {
                    let key = key_from_values(left_tuple.values());
                    if let Some(right_tuple) = entries.get_k(&key) {
                        visitor(left_tuple, right_tuple);
                    }
                }
            }
            (Self::Radix { entries, .. }, Self::Small(right)) => {
                for right_tuple in right.iter() {
                    let key = key_from_values(right_tuple.values());
                    if let Some(left_tuple) = entries.get_k(&key) {
                        visitor(left_tuple, right_tuple);
                    }
                }
            }
            (
                Self::Radix {
                    entries: left_entries,
                    ..
                },
                Self::Radix {
                    entries: right_entries,
                    ..
                },
            ) => {
                left_entries.intersect_values_with(right_entries, visitor);
            }
        }
    }
}

struct TupleIndex {
    spec: crate::TupleIndexSpec,
    unique_keys: bool,
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
            unique_keys: self.unique_keys,
            entries: self.entries.clone(),
        }
    }
}

impl TupleIndex {
    fn empty(spec: crate::TupleIndexSpec, arity: u16) -> Self {
        let unique_keys = spec.positions.len() == arity as usize;
        Self {
            spec,
            unique_keys,
            entries: VersionedAdaptiveRadixTree::new(),
        }
    }

    fn from_sorted_unique_rows(spec: crate::TupleIndexSpec, arity: u16, rows: &[Tuple]) -> Self {
        let mut index = Self::empty(spec, arity);
        if rows.is_empty() {
            return index;
        }

        if index.unique_keys {
            for tuple in rows {
                let key = index.tuple_key(tuple);
                index
                    .entries
                    .insert_k(&key, TupleBucket::one(tuple.clone()));
            }
            return index;
        }

        let mut keyed_rows = Vec::with_capacity(rows.len());
        let mut ordered = true;
        for tuple in rows {
            let key = index.tuple_key(tuple);
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple.clone()));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_tuple), (right_key, right_tuple)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_tuple.cmp(right_tuple))
            });
        }

        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            let bucket = TupleBucket::from_sorted_unique(
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple)| tuple.clone()),
            );
            index.entries.insert_k(&keyed_rows[start].0, bucket);
            start = end;
        }

        index
    }

    fn insert(&mut self, tuple: Tuple) {
        let key = self.tuple_key(&tuple);
        if self.unique_keys {
            self.entries.insert_k(&key, TupleBucket::one(tuple));
            return;
        }

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
        if self.unique_keys {
            self.entries.delete_k(&key);
            return;
        }

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
        self.visit_prefix(bindings, bound_count, &mut |_| {
            count += 1;
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
        let prefix_covers_all_bindings = self.prefix_covers_all_bindings(bindings, bound_count);
        let prefix = self.binding_prefix_key(bindings, bound_count);
        self.entries
            .try_prefix_values_for_each_k(&prefix, |bucket| {
                for tuple in bucket {
                    if !prefix_covers_all_bindings && !tuple.matches_bindings(bindings) {
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

    fn tuple_for_key_values(&self, values: &[Value]) -> Option<Tuple> {
        let key = self.key_from_values(values);
        self.entries.get_k(&key)?.first().cloned()
    }

    fn tuple_for_key_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let key = self.tuple_key(tuple);
        self.entries.get_k(&key)?.first().cloned()
    }

    fn prefix_covers_all_bindings(&self, bindings: &[Option<Value>], bound_count: usize) -> bool {
        bindings.iter().enumerate().all(|(position, binding)| {
            binding.is_none()
                || self
                    .spec
                    .positions
                    .iter()
                    .take(bound_count)
                    .any(|prefix_position| *prefix_position as usize == position)
        })
    }
}

pub(crate) struct ProjectedTupleIndex {
    entries: AdaptiveRadixTree<RadixTupleKey, TupleBucket>,
}

impl ProjectedTupleIndex {
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = Tuple>, positions: &[u16]) -> Self {
        let mut keyed_rows = Vec::new();
        let mut ordered = true;

        for row in rows {
            let key = key_from_values(
                positions
                    .iter()
                    .map(|position| &row.values()[*position as usize]),
            );
            if ordered
                && keyed_rows.last().is_some_and(|(last_key, last_row)| {
                    last_key > &key || (last_key == &key && last_row >= &row)
                })
            {
                ordered = false;
            }
            keyed_rows.push((key, row));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_tuple), (right_key, right_tuple)| {
                left_key
                    .cmp(right_key)
                    .then_with(|| left_tuple.cmp(right_tuple))
            });
        }

        let mut index = Self {
            entries: AdaptiveRadixTree::new(),
        };

        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }

            let mut group_rows = Vec::with_capacity(end - start);
            for (_, row) in &keyed_rows[start..end] {
                if group_rows.last() != Some(row) {
                    group_rows.push(row.clone());
                }
            }
            index.entries.insert_k(
                &keyed_rows[start].0,
                TupleBucket::from_sorted_unique(group_rows),
            );
            start = end;
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

    fn from_sorted_unique(tuples: impl IntoIterator<Item = Tuple>) -> Self {
        let mut tuples = tuples.into_iter();
        let Some(first) = tuples.next() else {
            return Self::Empty;
        };
        let Some(second) = tuples.next() else {
            return Self::One(first);
        };

        let mut rows = Vec::with_capacity(2 + tuples.size_hint().0);
        rows.push(first);
        rows.push(second);
        rows.extend(tuples);
        Self::Many(rows)
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
            Self::Many(tuples) => {
                if let Some(last) = tuples.last() {
                    if *last == tuple {
                        return false;
                    }
                    if *last < tuple {
                        tuples.push(tuple);
                        return true;
                    }
                }

                match tuples.binary_search(&tuple) {
                    Ok(_) => false,
                    Err(index) => {
                        tuples.insert(index, tuple);
                        true
                    }
                }
            }
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

    fn first(&self) -> Option<&Tuple> {
        match self {
            Self::Empty => None,
            Self::One(tuple) => Some(tuple),
            Self::Many(tuples) => tuples.first(),
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

fn is_natural_full_tuple_index(positions: &[u16], arity: u16) -> bool {
    positions.len() == arity as usize && is_natural_full_tuple_positions(positions, arity)
}

fn is_natural_full_tuple_positions(positions: &[u16], arity: u16) -> bool {
    positions.iter().copied().eq(0..arity)
}

fn natural_leading_bound_count(bindings: &[Option<Value>]) -> usize {
    bindings
        .iter()
        .take_while(|binding| binding.is_some())
        .count()
}

fn natural_prefix_covers_all_bindings(bindings: &[Option<Value>], bound_count: usize) -> bool {
    bindings.iter().skip(bound_count).all(Option::is_none)
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
