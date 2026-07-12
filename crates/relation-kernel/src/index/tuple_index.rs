use crate::ScanControl;
use crate::error::KernelError;
use crate::tuple::Tuple;
use mica_var::Value;
use rart::{AdaptiveRadixTree, Slot, SlotUpdate, VersionedAdaptiveRadixTree, VisitControl};
use std::fmt;

use crate::radix_key::{RadixTupleKey, key_from_values};

use super::tuple_bucket::TupleBucket;

pub(super) struct TupleIndex {
    spec: crate::TupleIndexSpec,
    unique_keys: bool,
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, TupleBucket>,
    tuple_count: usize,
    distinct_key_count: usize,
    max_bucket_size: usize,
}

impl fmt::Debug for TupleIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TupleIndex")
            .field("spec", &self.spec)
            .field("tuple_count", &self.tuple_count)
            .field("distinct_key_count", &self.distinct_key_count)
            .field("max_bucket_size", &self.max_bucket_size)
            .finish_non_exhaustive()
    }
}

impl Clone for TupleIndex {
    fn clone(&self) -> Self {
        Self {
            spec: self.spec.clone(),
            unique_keys: self.unique_keys,
            entries: self.entries.clone(),
            tuple_count: self.tuple_count,
            distinct_key_count: self.distinct_key_count,
            max_bucket_size: self.max_bucket_size,
        }
    }
}

impl TupleIndex {
    pub(super) fn empty(spec: crate::TupleIndexSpec, arity: u16) -> Self {
        let unique_keys = spec.positions.len() == arity as usize;
        Self {
            spec,
            unique_keys,
            entries: VersionedAdaptiveRadixTree::new(),
            tuple_count: 0,
            distinct_key_count: 0,
            max_bucket_size: 0,
        }
    }

    pub(super) fn positions(&self) -> &[u16] {
        self.spec.positions()
    }

    pub(super) fn leading_bound_count(&self, bindings: &[Option<Value>]) -> usize {
        self.spec.leading_bound_count(bindings)
    }

    pub(super) fn rebuild_from_sorted_unique_rows(&mut self, arity: u16, rows: &[Tuple]) {
        *self = Self::from_sorted_unique_rows(self.spec.clone(), arity, rows);
    }

    pub(super) fn from_sorted_unique_rows(
        spec: crate::TupleIndexSpec,
        arity: u16,
        rows: &[Tuple],
    ) -> Self {
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
            index.tuple_count = rows.len();
            index.distinct_key_count = rows.len();
            index.max_bucket_size = usize::from(!rows.is_empty());
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
            index.distinct_key_count += 1;
            index.max_bucket_size = index.max_bucket_size.max(end - start);
            start = end;
        }

        index.tuple_count = rows.len();

        index
    }

    pub(super) fn insert(&mut self, tuple: Tuple) {
        let key = self.tuple_key(&tuple);
        let previous_bucket_size = self.entries.get_k(&key).map(TupleBucket::len).unwrap_or(0);
        if self.unique_keys {
            self.entries.insert_k(&key, TupleBucket::one(tuple));
            self.record_insert(previous_bucket_size);
            return;
        }

        self.entries.update_k(&key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(TupleBucket::one(tuple)),
            Slot::Occupied(bucket) => {
                bucket.insert(tuple);
                SlotUpdate::Keep
            }
        });
        self.record_insert(previous_bucket_size);
    }

    pub(super) fn remove(&mut self, tuple: &Tuple) {
        let key = self.tuple_key(tuple);
        let previous_bucket_size = self.entries.get_k(&key).map(TupleBucket::len).unwrap_or(0);
        if self.unique_keys {
            self.entries.delete_k(&key);
            self.record_remove(previous_bucket_size);
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
        self.record_remove(previous_bucket_size);
    }

    fn record_insert(&mut self, previous_bucket_size: usize) {
        self.tuple_count += 1;
        if previous_bucket_size == 0 {
            self.distinct_key_count += 1;
        }
        self.max_bucket_size = self.max_bucket_size.max(previous_bucket_size + 1);
    }

    fn record_remove(&mut self, previous_bucket_size: usize) {
        debug_assert!(previous_bucket_size > 0);
        self.tuple_count = self.tuple_count.saturating_sub(1);
        if previous_bucket_size == 1 {
            self.distinct_key_count = self.distinct_key_count.saturating_sub(1);
        }
        if self.tuple_count == 0 {
            self.max_bucket_size = 0;
        }
    }

    pub(super) fn scan_prefix(
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

    pub(super) fn estimate_prefix_count(
        &self,
        bindings: &[Option<Value>],
        bound_count: usize,
    ) -> Result<usize, KernelError> {
        if bound_count == self.spec.positions.len() {
            let key = self.binding_prefix_key(bindings, bound_count);
            return Ok(self.entries.get_k(&key).map(TupleBucket::len).unwrap_or(0));
        }
        if self.tuple_count == 0 || bound_count == 0 {
            return Ok(self.tuple_count);
        }

        let key_fraction = bound_count as f64 / self.spec.positions.len() as f64;
        let estimated_distinct_prefixes = (self.distinct_key_count as f64)
            .powf(key_fraction)
            .round()
            .max(1.0) as usize;
        Ok(self
            .tuple_count
            .div_ceil(estimated_distinct_prefixes)
            .max(self.max_bucket_size.min(self.tuple_count)))
    }

    pub(super) fn visit_prefix(
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

    pub(super) fn matching_row_pairs(
        &self,
        other: &Self,
        left_bindings: &[Option<Value>],
        right_bindings: &[Option<Value>],
        mut visit: impl FnMut(&Tuple, &Tuple),
    ) {
        self.intersect_values_with(other, |left_bucket, right_bucket| {
            for left_tuple in left_bucket {
                if !left_tuple.matches_bindings(left_bindings) {
                    continue;
                }
                for right_tuple in right_bucket {
                    if right_tuple.matches_bindings(right_bindings) {
                        visit(left_tuple, right_tuple);
                    }
                }
            }
        });
    }

    pub(super) fn tuple_for_key_values(&self, values: &[Value]) -> Option<Tuple> {
        let key = self.key_from_values(values);
        self.entries.get_k(&key)?.first().cloned()
    }

    pub(super) fn tuple_for_key_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let key = self.tuple_key(tuple);
        self.entries.get_k(&key)?.first().cloned()
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

    fn intersect_values_with(&self, other: &Self, visit: impl FnMut(&TupleBucket, &TupleBucket)) {
        self.entries.intersect_values_with(&other.entries, visit);
    }

    pub(crate) fn matching_row_pairs(&self, other: &Self, mut visit: impl FnMut(&Tuple, &Tuple)) {
        self.intersect_values_with(other, |left, right| {
            for left_tuple in left {
                for right_tuple in right {
                    visit(left_tuple, right_tuple);
                }
            }
        });
    }

    pub(crate) fn matching_left_rows(&self, other: &Self, mut visit: impl FnMut(&Tuple)) {
        self.intersect_values_with(other, |left, _right| {
            for tuple in left {
                visit(tuple);
            }
        });
    }
}
