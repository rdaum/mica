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

use mica_relation_kernel::{
    RelationId, RelationKernel, RelationMetadata, ScanControl, Snapshot, Tuple,
};
use mica_var::{Identity, OrderedKeySink, Symbol, Value};
use micromeasure::{BenchContext, BenchmarkMainOptions, Throughput, benchmark_main, black_box};
use rart::{
    OverflowKey, OverflowKeyBuilder, Slot, SlotUpdate, VersionedAdaptiveRadixTree, VisitControl,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

const GROUPS: usize = 128;
const ITEMS_PER_GROUP: usize = 128;
const TUPLE_COUNT: usize = GROUPS * ITEMS_PER_GROUP;
const SECONDARY_UPDATE_COUNT: usize = 1024;
const WIDE_TUPLE_ARITY: usize = 16;

type RelationIndexKey = OverflowKey<64, 16>;

struct RelationIndexKeyBuilder(OverflowKeyBuilder<64, 16>);

impl RelationIndexKeyBuilder {
    fn new() -> Self {
        Self(RelationIndexKey::builder())
    }

    fn finish(self) -> RelationIndexKey {
        self.0.finish()
    }
}

impl OrderedKeySink for RelationIndexKeyBuilder {
    fn push_byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BenchTupleKey(Vec<Value>);

struct IndexContext {
    tuples: Vec<Tuple>,
    old: OldTupleIndex,
    radix: RadixTupleIndex,
    radix_secondary: RadixTupleIndex,
    logical_secondary: LogicalTupleIndex,
    payload_relation: PayloadRelationIndexes,
    arena_relation: ArenaRelationIndexes,
    shared_arena_relation: SharedArenaRelationIndexes,
    keyed_shared_arena_relation: KeyedSharedArenaRelationIndexes,
    append_version_arena_relation: AppendVersionArenaRelationIndexes,
    dirty_append_version_arena_relation: AppendVersionArenaRelationIndexes,
    dirty_current_arena_relation: DirtyCurrentArenaRelationIndexes,
    dirty_dirty_current_arena_relation: DirtyCurrentArenaRelationIndexes,
    prefix_values: Vec<Value>,
    dirty_prefix_values: Vec<Value>,
    update_ids: Vec<usize>,
    update_epoch: i64,
}

struct ProductionIndexContext {
    tuples: Vec<Tuple>,
    indexed_snapshot: Arc<Snapshot>,
    unindexed_snapshot: Arc<Snapshot>,
    indexed_relation: RelationId,
    unindexed_relation: RelationId,
    bindings: Vec<Option<Value>>,
    full_scan_bindings: Vec<Option<Value>>,
}

struct WideIndexContext {
    tuples: Vec<Tuple>,
    shared_arena_relation: SharedArenaRelationIndexes,
    keyed_shared_arena_relation: KeyedSharedArenaRelationIndexes,
    dirty_current_arena_relation: DirtyCurrentArenaRelationIndexes,
    keyed_dirty_current_arena_relation: KeyedDirtyCurrentArenaRelationIndexes,
}

#[derive(Clone)]
struct OldTupleIndex {
    positions: Vec<u16>,
    entries: BTreeMap<BenchTupleKey, BTreeSet<Tuple>>,
}

#[derive(Clone)]
struct RadixTupleIndex {
    positions: Vec<u16>,
    entries: VersionedAdaptiveRadixTree<RelationIndexKey, BTreeSet<Tuple>>,
}

#[derive(Clone)]
struct LogicalTupleIndex {
    positions: Vec<u16>,
    tuples: Vec<Tuple>,
    entries: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct PayloadRelationIndexes {
    tuples: Vec<Tuple>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, Tuple>,
    secondary: RadixTupleIndex,
}

#[derive(Clone)]
struct ArenaRelationIndexes {
    rows: Vec<Tuple>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary_positions: Vec<u16>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct SharedArenaRelationIndexes {
    rows: Arc<[Tuple]>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary_positions: Vec<u16>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct KeyedSharedArenaRelationIndexes {
    rows: Arc<[Tuple]>,
    full_keys: Arc<[RelationIndexKey]>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct AppendVersionArenaRelationIndexes {
    base_rows: Arc<[Tuple]>,
    appended_rows: Vec<Tuple>,
    current_physical: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct DirtyCurrentArenaRelationIndexes {
    base_rows: Arc<[Tuple]>,
    appended_rows: Vec<Tuple>,
    dirty_current: HashMap<usize, usize>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

#[derive(Clone)]
struct KeyedDirtyCurrentArenaRelationIndexes {
    base_rows: Arc<[Tuple]>,
    base_full_keys: Arc<[RelationIndexKey]>,
    appended_rows: Vec<Tuple>,
    appended_full_keys: Vec<RelationIndexKey>,
    dirty_current: HashMap<usize, usize>,
    primary: VersionedAdaptiveRadixTree<RelationIndexKey, usize>,
    secondary: VersionedAdaptiveRadixTree<RelationIndexKey, Vec<usize>>,
}

impl BenchContext for IndexContext {
    fn prepare(_num_chunks: usize) -> Self {
        let tuples = build_tuples();
        let old = OldTupleIndex::from_tuples(&[0, 1, 2], &tuples);
        let radix = RadixTupleIndex::from_tuples(&[0, 1, 2], &tuples);
        let radix_secondary = RadixTupleIndex::from_tuples(&[0], &tuples);
        let logical_secondary = LogicalTupleIndex::from_tuples(&[0], &tuples);
        let payload_relation = PayloadRelationIndexes::from_tuples(&tuples);
        let arena_relation = ArenaRelationIndexes::from_tuples(&tuples);
        let shared_arena_relation = SharedArenaRelationIndexes::from_tuples_bulk(&tuples);
        let keyed_shared_arena_relation =
            KeyedSharedArenaRelationIndexes::from_tuples_bulk(&tuples);
        let append_version_arena_relation =
            AppendVersionArenaRelationIndexes::from_tuples_bulk(&tuples);
        let dirty_append_version_arena_relation =
            dirty_append_version_arena_relation(&tuples, &update_ids());
        let dirty_current_arena_relation =
            DirtyCurrentArenaRelationIndexes::from_tuples_bulk(&tuples);
        let dirty_dirty_current_arena_relation =
            seeded_dirty_current_arena_relation(&tuples, &update_ids());
        Self {
            tuples,
            old,
            radix,
            radix_secondary,
            logical_secondary,
            payload_relation,
            arena_relation,
            shared_arena_relation,
            keyed_shared_arena_relation,
            append_version_arena_relation,
            dirty_append_version_arena_relation,
            dirty_current_arena_relation,
            dirty_dirty_current_arena_relation,
            prefix_values: vec![Value::identity(identity(42))],
            dirty_prefix_values: vec![Value::identity(identity(0))],
            update_ids: update_ids(),
            update_epoch: 0,
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(10)
    }
}

impl BenchContext for WideIndexContext {
    fn prepare(_num_chunks: usize) -> Self {
        let tuples = build_wide_tuples();
        Self {
            shared_arena_relation: SharedArenaRelationIndexes::from_tuples_bulk(&tuples),
            keyed_shared_arena_relation: KeyedSharedArenaRelationIndexes::from_tuples_bulk(&tuples),
            dirty_current_arena_relation: seeded_wide_dirty_current_arena_relation(
                &tuples,
                &update_ids(),
            ),
            keyed_dirty_current_arena_relation: seeded_wide_keyed_dirty_current_arena_relation(
                &tuples,
                &update_ids(),
            ),
            tuples,
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(10)
    }
}

impl BenchContext for ProductionIndexContext {
    fn prepare(_num_chunks: usize) -> Self {
        let tuples = build_tuples();
        let indexed_relation = relation(700);
        let unindexed_relation = relation(701);

        Self {
            tuples: tuples.clone(),
            indexed_snapshot: build_production_snapshot(
                indexed_relation,
                RelationMetadata::new(indexed_relation, Symbol::intern("IndexedBench"), 3)
                    .with_index([0]),
                &tuples,
            ),
            unindexed_snapshot: build_production_snapshot(
                unindexed_relation,
                RelationMetadata::new(unindexed_relation, Symbol::intern("UnindexedBench"), 3)
                    .without_indexes(),
                &tuples,
            ),
            indexed_relation,
            unindexed_relation,
            bindings: vec![Some(Value::identity(identity(42))), None, None],
            full_scan_bindings: vec![None, None, None],
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(10)
    }
}

impl OldTupleIndex {
    fn from_tuples(positions: &[u16], tuples: &[Tuple]) -> Self {
        let mut index = Self {
            positions: positions.to_vec(),
            entries: BTreeMap::new(),
        };
        for tuple in tuples {
            index.insert(tuple.clone());
        }
        index
    }

    fn insert(&mut self, tuple: Tuple) {
        self.entries
            .entry(self.tuple_key(&tuple))
            .or_default()
            .insert(tuple);
    }

    fn visit_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        for (key, tuples) in &self.entries {
            if key.0.len() < prefix.len() || key.0[..prefix.len()] != *prefix {
                continue;
            }
            for tuple in tuples {
                visit(tuple);
            }
        }
    }

    fn tuple_key(&self, tuple: &Tuple) -> BenchTupleKey {
        BenchTupleKey(
            self.positions
                .iter()
                .map(|position| tuple.values()[*position as usize].clone())
                .collect(),
        )
    }
}

impl RadixTupleIndex {
    fn from_tuples(positions: &[u16], tuples: &[Tuple]) -> Self {
        let mut index = Self {
            positions: positions.to_vec(),
            entries: VersionedAdaptiveRadixTree::new(),
        };
        for tuple in tuples {
            index.insert(tuple.clone());
        }
        index
    }

    fn from_sorted_tuples(positions: &[u16], tuples: &[Tuple]) -> Self {
        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for tuple in tuples {
            let key = key_from_values(
                positions
                    .iter()
                    .map(|position| &tuple.values()[*position as usize]),
            );
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

        let mut index = Self {
            positions: positions.to_vec(),
            entries: VersionedAdaptiveRadixTree::new(),
        };
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            index.entries.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple)| tuple.clone())
                    .collect(),
            );
            start = end;
        }
        index
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

    fn replace_same_key(&mut self, old_tuple: &Tuple, new_tuple: Tuple) {
        let key = self.tuple_key(old_tuple);
        let bucket = self
            .entries
            .get_mut_k(&key)
            .expect("secondary key should exist");
        bucket.remove(old_tuple);
        bucket.insert(new_tuple);
    }

    fn replace_key_changed(&mut self, old_tuple: &Tuple, new_tuple: Tuple) {
        let old_key = self.tuple_key(old_tuple);
        self.entries.update_k(&old_key, |slot| match slot {
            Slot::Vacant => panic!("old secondary key should exist"),
            Slot::Occupied(bucket) => {
                bucket.remove(old_tuple);
                if bucket.is_empty() {
                    SlotUpdate::Remove
                } else {
                    SlotUpdate::Keep
                }
            }
        });

        let new_key = self.tuple_key(&new_tuple);
        self.entries.update_k(&new_key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(BTreeSet::from([new_tuple])),
            Slot::Occupied(bucket) => {
                bucket.insert(new_tuple);
                SlotUpdate::Keep
            }
        });
    }

    fn visit_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        self.entries
            .try_prefix_values_for_each_k(&prefix, |tuples| -> Result<VisitControl, ()> {
                for tuple in tuples {
                    visit(tuple);
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }

    fn tuple_key(&self, tuple: &Tuple) -> RelationIndexKey {
        key_from_values(
            self.positions
                .iter()
                .map(|position| &tuple.values()[*position as usize]),
        )
    }
}

impl LogicalTupleIndex {
    fn from_tuples(positions: &[u16], tuples: &[Tuple]) -> Self {
        let mut index = Self {
            positions: positions.to_vec(),
            tuples: tuples.to_vec(),
            entries: VersionedAdaptiveRadixTree::new(),
        };
        for tuple_id in 0..index.tuples.len() {
            index.insert(tuple_id);
        }
        index
    }

    fn insert(&mut self, tuple_id: usize) {
        let key = self.tuple_key(&self.tuples[tuple_id]);
        if let Some(bucket) = self.entries.get_mut_k(&key) {
            bucket.push(tuple_id);
            return;
        }
        self.entries.insert_k(&key, vec![tuple_id]);
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        self.tuples[tuple_id] = new_tuple;
    }

    fn replace_key_changed(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_key = self.tuple_key(&self.tuples[tuple_id]);
        self.entries.update_k(&old_key, |slot| match slot {
            Slot::Vacant => panic!("old secondary key should exist"),
            Slot::Occupied(bucket) => {
                let old_index = bucket
                    .iter()
                    .position(|old_tuple_id| *old_tuple_id == tuple_id)
                    .expect("tuple id should exist in old secondary bucket");
                bucket.swap_remove(old_index);
                if bucket.is_empty() {
                    SlotUpdate::Remove
                } else {
                    SlotUpdate::Keep
                }
            }
        });

        let new_key = self.tuple_key(&new_tuple);
        self.entries.update_k(&new_key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(vec![tuple_id]),
            Slot::Occupied(bucket) => {
                bucket.push(tuple_id);
                SlotUpdate::Keep
            }
        });
        self.tuples[tuple_id] = new_tuple;
    }

    fn visit_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        self.entries
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(&self.tuples[*tuple_id]);
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }

    fn tuple_key(&self, tuple: &Tuple) -> RelationIndexKey {
        key_from_values(
            self.positions
                .iter()
                .map(|position| &tuple.values()[*position as usize]),
        )
    }
}

impl PayloadRelationIndexes {
    fn from_tuples(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        for tuple in tuples {
            let key = full_tuple_key(tuple);
            primary.insert_k(&key, tuple.clone());
        }
        Self {
            tuples: tuples.to_vec(),
            primary,
            secondary: RadixTupleIndex::from_tuples(&[0], tuples),
        }
    }

    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        for tuple in tuples {
            let key = full_tuple_key(tuple);
            primary.insert_k(&key, tuple.clone());
        }
        Self {
            tuples: tuples.to_vec(),
            primary,
            secondary: RadixTupleIndex::from_sorted_tuples(&[0], tuples),
        }
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_tuple = self.tuples[tuple_id].clone();
        let old_key = full_tuple_key(&old_tuple);
        self.primary.delete_k(&old_key);

        let new_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_key, new_tuple.clone());
        self.secondary
            .replace_same_key(&old_tuple, new_tuple.clone());
        self.tuples[tuple_id] = new_tuple;
    }

    fn replace_key_changed(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_tuple = self.tuples[tuple_id].clone();
        let old_key = full_tuple_key(&old_tuple);
        self.primary.delete_k(&old_key);

        let new_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_key, new_tuple.clone());
        self.secondary
            .replace_key_changed(&old_tuple, new_tuple.clone());
        self.tuples[tuple_id] = new_tuple;
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], visit: impl FnMut(&Tuple)) {
        self.secondary.visit_prefix(prefix, visit);
    }
}

impl ArenaRelationIndexes {
    fn from_tuples(tuples: &[Tuple]) -> Self {
        let mut index = Self {
            rows: tuples.to_vec(),
            primary: VersionedAdaptiveRadixTree::new(),
            secondary_positions: vec![0],
            secondary: VersionedAdaptiveRadixTree::new(),
        };
        for tuple_id in 0..index.rows.len() {
            index.insert_indexes(tuple_id);
        }
        index
    }

    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = full_tuple_key(tuple);
            primary.insert_k(&key, tuple_id);
        }

        let secondary_positions = vec![0];
        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = key_from_values(
                secondary_positions
                    .iter()
                    .map(|position| &tuple.values()[*position as usize]),
            );
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            rows: tuples.to_vec(),
            primary,
            secondary_positions,
            secondary,
        }
    }

    fn insert_indexes(&mut self, tuple_id: usize) {
        let primary_key = full_tuple_key(&self.rows[tuple_id]);
        self.primary.insert_k(&primary_key, tuple_id);

        let secondary_key = self.secondary_key(&self.rows[tuple_id]);
        self.secondary.update_k(&secondary_key, |slot| match slot {
            Slot::Vacant => SlotUpdate::Insert(vec![tuple_id]),
            Slot::Occupied(bucket) => {
                bucket.push(tuple_id);
                SlotUpdate::Keep
            }
        });
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_key = full_tuple_key(&self.rows[tuple_id]);
        self.primary.delete_k(&old_key);

        let new_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_key, tuple_id);
        self.rows[tuple_id] = new_tuple;
    }

    fn replace_key_changed(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_primary_key = full_tuple_key(&self.rows[tuple_id]);
        self.primary.delete_k(&old_primary_key);

        let old_secondary_key = self.secondary_key(&self.rows[tuple_id]);
        self.secondary
            .update_k(&old_secondary_key, |slot| match slot {
                Slot::Vacant => panic!("old secondary key should exist"),
                Slot::Occupied(bucket) => {
                    let old_index = bucket
                        .iter()
                        .position(|old_tuple_id| *old_tuple_id == tuple_id)
                        .expect("tuple id should exist in old secondary bucket");
                    bucket.swap_remove(old_index);
                    if bucket.is_empty() {
                        SlotUpdate::Remove
                    } else {
                        SlotUpdate::Keep
                    }
                }
            });

        let new_primary_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_primary_key, tuple_id);

        let new_secondary_key = self.secondary_key(&new_tuple);
        self.secondary
            .update_k(&new_secondary_key, |slot| match slot {
                Slot::Vacant => SlotUpdate::Insert(vec![tuple_id]),
                Slot::Occupied(bucket) => {
                    bucket.push(tuple_id);
                    SlotUpdate::Keep
                }
            });
        self.rows[tuple_id] = new_tuple;
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        self.secondary
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(&self.rows[*tuple_id]);
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }

    fn secondary_key(&self, tuple: &Tuple) -> RelationIndexKey {
        key_from_values(
            self.secondary_positions
                .iter()
                .map(|position| &tuple.values()[*position as usize]),
        )
    }
}

impl SharedArenaRelationIndexes {
    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = full_tuple_key(tuple);
            primary.insert_k(&key, tuple_id);
        }

        let secondary_positions = vec![0];
        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = key_from_values(
                secondary_positions
                    .iter()
                    .map(|position| &tuple.values()[*position as usize]),
            );
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            rows: tuples.to_vec().into(),
            primary,
            secondary_positions,
            secondary,
        }
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        self.secondary
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(&self.rows[*tuple_id]);
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }
}

impl KeyedSharedArenaRelationIndexes {
    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let rows = tuples.to_vec();
        let full_keys = rows.iter().map(full_tuple_key).collect();
        Self::from_rows_and_full_keys(rows, full_keys)
    }

    fn from_rows_and_full_keys(rows: Vec<Tuple>, full_keys: Vec<RelationIndexKey>) -> Self {
        assert_eq!(rows.len(), full_keys.len());

        let mut primary = VersionedAdaptiveRadixTree::new();
        for (tuple_id, key) in full_keys.iter().enumerate() {
            primary.insert_k(key, tuple_id);
        }

        let mut keyed_rows = Vec::with_capacity(rows.len());
        let mut ordered = true;
        for (tuple_id, tuple) in rows.iter().enumerate() {
            let key = key_from_values([&tuple.values()[0]]);
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            rows: rows.into(),
            full_keys: full_keys.into(),
            primary,
            secondary,
        }
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        self.secondary
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(&self.rows[*tuple_id]);
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }
}

impl AppendVersionArenaRelationIndexes {
    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut current_physical = VersionedAdaptiveRadixTree::new();
        let mut primary = VersionedAdaptiveRadixTree::new();
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let tuple_id_key = tuple_id_key(tuple_id);
            current_physical.insert_k(&tuple_id_key, tuple_id);

            let full_key = full_tuple_key(tuple);
            primary.insert_k(&full_key, tuple_id);
        }

        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = key_from_values([&tuple.values()[0]]);
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            base_rows: tuples.to_vec().into(),
            appended_rows: Vec::new(),
            current_physical,
            primary,
            secondary,
        }
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_tuple = self.current_tuple(tuple_id).clone();
        let old_key = full_tuple_key(&old_tuple);
        self.primary.delete_k(&old_key);

        let physical_id = self.base_rows.len() + self.appended_rows.len();
        self.appended_rows.push(new_tuple.clone());

        let tuple_id_key = tuple_id_key(tuple_id);
        self.current_physical.insert_k(&tuple_id_key, physical_id);

        let new_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_key, tuple_id);
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        if self.appended_rows.is_empty() {
            self.secondary
                .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                    for tuple_id in tuple_ids {
                        visit(&self.base_rows[*tuple_id]);
                    }
                    Ok(VisitControl::Continue)
                })
                .unwrap();
            return;
        }

        self.secondary
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(self.current_tuple(*tuple_id));
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }

    fn current_tuple(&self, tuple_id: usize) -> &Tuple {
        let tuple_id_key = tuple_id_key(tuple_id);
        let physical_id = *self
            .current_physical
            .get_k(&tuple_id_key)
            .expect("tuple id should have a current physical row");
        if physical_id < self.base_rows.len() {
            &self.base_rows[physical_id]
        } else {
            &self.appended_rows[physical_id - self.base_rows.len()]
        }
    }
}

impl DirtyCurrentArenaRelationIndexes {
    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let full_key = full_tuple_key(tuple);
            primary.insert_k(&full_key, tuple_id);
        }

        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = key_from_values([&tuple.values()[0]]);
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            base_rows: tuples.to_vec().into(),
            appended_rows: Vec::new(),
            dirty_current: HashMap::new(),
            primary,
            secondary,
        }
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_tuple = self.current_tuple(tuple_id).clone();
        let old_key = full_tuple_key(&old_tuple);
        self.primary.delete_k(&old_key);

        let physical_id = self.base_rows.len() + self.appended_rows.len();
        self.appended_rows.push(new_tuple.clone());
        self.dirty_current.insert(tuple_id, physical_id);

        let new_key = full_tuple_key(&new_tuple);
        self.primary.insert_k(&new_key, tuple_id);
    }

    fn visit_secondary_prefix(&self, prefix: &[Value], mut visit: impl FnMut(&Tuple)) {
        let prefix = key_from_values(prefix);
        if self.dirty_current.is_empty() {
            self.secondary
                .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                    for tuple_id in tuple_ids {
                        visit(&self.base_rows[*tuple_id]);
                    }
                    Ok(VisitControl::Continue)
                })
                .unwrap();
            return;
        }

        self.secondary
            .try_prefix_values_for_each_k(&prefix, |tuple_ids| -> Result<VisitControl, ()> {
                for tuple_id in tuple_ids {
                    visit(self.current_tuple(*tuple_id));
                }
                Ok(VisitControl::Continue)
            })
            .unwrap();
    }

    fn current_tuple(&self, tuple_id: usize) -> &Tuple {
        let physical_id = self
            .dirty_current
            .get(&tuple_id)
            .copied()
            .unwrap_or(tuple_id);
        if physical_id < self.base_rows.len() {
            &self.base_rows[physical_id]
        } else {
            &self.appended_rows[physical_id - self.base_rows.len()]
        }
    }

    fn compact_to_shared(&self) -> SharedArenaRelationIndexes {
        let mut rows = Vec::with_capacity(self.base_rows.len());
        for tuple_id in 0..self.base_rows.len() {
            rows.push(self.current_tuple(tuple_id).clone());
        }
        SharedArenaRelationIndexes::from_tuples_bulk(&rows)
    }

    fn compact_to_keyed_shared(&self) -> KeyedSharedArenaRelationIndexes {
        let mut rows = Vec::with_capacity(self.base_rows.len());
        let mut full_keys = Vec::with_capacity(self.base_rows.len());
        for tuple_id in 0..self.base_rows.len() {
            let tuple = self.current_tuple(tuple_id).clone();
            full_keys.push(full_tuple_key(&tuple));
            rows.push(tuple);
        }
        KeyedSharedArenaRelationIndexes::from_rows_and_full_keys(rows, full_keys)
    }
}

impl KeyedDirtyCurrentArenaRelationIndexes {
    fn from_tuples_bulk(tuples: &[Tuple]) -> Self {
        let mut primary = VersionedAdaptiveRadixTree::new();
        let mut full_keys = Vec::with_capacity(tuples.len());
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let full_key = full_tuple_key(tuple);
            primary.insert_k(&full_key, tuple_id);
            full_keys.push(full_key);
        }

        let mut keyed_rows = Vec::with_capacity(tuples.len());
        let mut ordered = true;
        for (tuple_id, tuple) in tuples.iter().enumerate() {
            let key = key_from_values([&tuple.values()[0]]);
            if keyed_rows
                .last()
                .is_some_and(|(last_key, _)| last_key > &key)
            {
                ordered = false;
            }
            keyed_rows.push((key, tuple_id));
        }

        if !ordered {
            keyed_rows.sort_by(|(left_key, left_id), (right_key, right_id)| {
                left_key.cmp(right_key).then_with(|| left_id.cmp(right_id))
            });
        }

        let mut secondary = VersionedAdaptiveRadixTree::new();
        let mut start = 0usize;
        while start < keyed_rows.len() {
            let mut end = start + 1;
            while end < keyed_rows.len() && keyed_rows[end].0 == keyed_rows[start].0 {
                end += 1;
            }
            secondary.insert_k(
                &keyed_rows[start].0,
                keyed_rows[start..end]
                    .iter()
                    .map(|(_, tuple_id)| *tuple_id)
                    .collect(),
            );
            start = end;
        }

        Self {
            base_rows: tuples.to_vec().into(),
            base_full_keys: full_keys.into(),
            appended_rows: Vec::new(),
            appended_full_keys: Vec::new(),
            dirty_current: HashMap::new(),
            primary,
            secondary,
        }
    }

    fn replace_same_key(&mut self, tuple_id: usize, new_tuple: Tuple) {
        let old_key = self.current_full_key(tuple_id).clone();
        self.primary.delete_k(&old_key);

        let new_key = full_tuple_key(&new_tuple);
        let physical_id = self.base_rows.len() + self.appended_rows.len();
        self.appended_rows.push(new_tuple);
        self.appended_full_keys.push(new_key.clone());
        self.dirty_current.insert(tuple_id, physical_id);
        self.primary.insert_k(&new_key, tuple_id);
    }

    fn current_tuple(&self, tuple_id: usize) -> &Tuple {
        let physical_id = self
            .dirty_current
            .get(&tuple_id)
            .copied()
            .unwrap_or(tuple_id);
        if physical_id < self.base_rows.len() {
            &self.base_rows[physical_id]
        } else {
            &self.appended_rows[physical_id - self.base_rows.len()]
        }
    }

    fn current_full_key(&self, tuple_id: usize) -> &RelationIndexKey {
        let physical_id = self
            .dirty_current
            .get(&tuple_id)
            .copied()
            .unwrap_or(tuple_id);
        if physical_id < self.base_rows.len() {
            &self.base_full_keys[physical_id]
        } else {
            &self.appended_full_keys[physical_id - self.base_rows.len()]
        }
    }

    fn compact_to_keyed_shared(&self) -> KeyedSharedArenaRelationIndexes {
        let mut rows = Vec::with_capacity(self.base_rows.len());
        let mut full_keys = Vec::with_capacity(self.base_rows.len());
        for tuple_id in 0..self.base_rows.len() {
            rows.push(self.current_tuple(tuple_id).clone());
            full_keys.push(self.current_full_key(tuple_id).clone());
        }
        KeyedSharedArenaRelationIndexes::from_rows_and_full_keys(rows, full_keys)
    }
}

fn old_prefix_scan(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.old.visit_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn radix_prefix_scan(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.radix.visit_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn radix_secondary_prefix_visit(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.radix_secondary
            .visit_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn logical_secondary_prefix_visit(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.logical_secondary
            .visit_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn radix_secondary_prefix_scan(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut rows = Vec::new();
        ctx.radix_secondary
            .visit_prefix(&ctx.prefix_values, |tuple| rows.push(tuple.clone()));
        black_box(rows);
    }
}

fn logical_secondary_prefix_scan(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut rows = Vec::new();
        ctx.logical_secondary
            .visit_prefix(&ctx.prefix_values, |tuple| rows.push(tuple.clone()));
        black_box(rows);
    }
}

fn payload_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.payload_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn shared_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.shared_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn keyed_shared_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.keyed_shared_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn append_version_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.append_version_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn dirty_append_version_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.dirty_append_version_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn dirty_current_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.dirty_current_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn dirty_dirty_current_arena_relation_secondary_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.dirty_dirty_current_arena_relation
            .visit_secondary_prefix(&ctx.prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn dirty_append_version_arena_relation_dirty_hit_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.dirty_append_version_arena_relation
            .visit_secondary_prefix(&ctx.dirty_prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn dirty_current_arena_relation_dirty_hit_prefix_visit(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.dirty_dirty_current_arena_relation
            .visit_secondary_prefix(&ctx.dirty_prefix_values, |_| count += 1);
        black_box(count);
    }
}

fn old_rebuild_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(OldTupleIndex::from_tuples(&[0, 1, 2], &ctx.tuples));
    }
}

fn radix_rebuild_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(RadixTupleIndex::from_tuples(&[0, 1, 2], &ctx.tuples));
    }
}

fn radix_secondary_rebuild_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(RadixTupleIndex::from_tuples(&[0], &ctx.tuples));
    }
}

fn logical_secondary_rebuild_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(LogicalTupleIndex::from_tuples(&[0], &ctx.tuples));
    }
}

fn payload_relation_rebuild_indexes(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(PayloadRelationIndexes::from_tuples(&ctx.tuples));
    }
}

fn arena_relation_rebuild_indexes(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ArenaRelationIndexes::from_tuples(&ctx.tuples));
    }
}

fn payload_relation_bulk_rebuild_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(PayloadRelationIndexes::from_tuples_bulk(&ctx.tuples));
    }
}

fn arena_relation_bulk_rebuild_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ArenaRelationIndexes::from_tuples_bulk(&ctx.tuples));
    }
}

fn shared_arena_relation_bulk_rebuild_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(SharedArenaRelationIndexes::from_tuples_bulk(&ctx.tuples));
    }
}

fn keyed_shared_arena_relation_bulk_rebuild_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(KeyedSharedArenaRelationIndexes::from_tuples_bulk(
            &ctx.tuples,
        ));
    }
}

fn dirty_current_arena_relation_compact_to_shared(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.dirty_dirty_current_arena_relation.compact_to_shared());
    }
}

fn dirty_current_arena_relation_compact_to_keyed_shared(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let compacted = ctx
            .dirty_dirty_current_arena_relation
            .compact_to_keyed_shared();
        black_box(compacted.full_keys.len());
        black_box(&compacted.primary);
        black_box(compacted);
    }
}

fn wide_shared_arena_relation_bulk_rebuild_indexes(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(SharedArenaRelationIndexes::from_tuples_bulk(&ctx.tuples));
    }
}

fn wide_keyed_shared_arena_relation_bulk_rebuild_indexes(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(KeyedSharedArenaRelationIndexes::from_tuples_bulk(
            &ctx.tuples,
        ));
    }
}

fn wide_dirty_current_arena_relation_compact_to_shared(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(ctx.dirty_current_arena_relation.compact_to_shared());
    }
}

fn wide_dirty_current_arena_relation_compact_to_keyed_shared(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(&ctx.keyed_dirty_current_arena_relation.secondary);
        black_box(
            ctx.keyed_dirty_current_arena_relation
                .compact_to_keyed_shared(),
        );
    }
}

fn wide_shared_arena_relation_clone_indexes(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.shared_arena_relation.clone();
        black_box(cloned.secondary_positions.len());
        black_box(cloned);
    }
}

fn wide_keyed_shared_arena_relation_clone_indexes(
    ctx: &mut WideIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.keyed_shared_arena_relation.clone();
        black_box(cloned.full_keys.len());
        black_box(cloned);
    }
}

fn radix_secondary_same_key_updates(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.radix_secondary
                .replace_same_key(&old_tuple, new_tuple.clone());
            ctx.tuples[*tuple_id] = new_tuple;
        }
        black_box(ctx.update_epoch);
    }
}

fn logical_secondary_same_key_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.logical_secondary
                .replace_same_key(*tuple_id, new_tuple.clone());
            ctx.tuples[*tuple_id] = new_tuple;
        }
        black_box(ctx.update_epoch);
    }
}

fn payload_relation_same_key_updates(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.payload_relation.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.payload_relation.replace_same_key(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn arena_relation_same_key_updates(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.arena_relation.rows[*tuple_id].clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.arena_relation.replace_same_key(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn append_version_arena_relation_same_key_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx
                .append_version_arena_relation
                .current_tuple(*tuple_id)
                .clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.append_version_arena_relation
                .replace_same_key(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn dirty_current_arena_relation_same_key_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx
                .dirty_current_arena_relation
                .current_tuple(*tuple_id)
                .clone();
            let new_tuple = Tuple::new([
                old_tuple.values()[0].clone(),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.dirty_current_arena_relation
                .replace_same_key(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn radix_secondary_key_changed_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                Value::identity(rotated_update_group(*tuple_id, ctx.update_epoch)),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.radix_secondary
                .replace_key_changed(&old_tuple, new_tuple.clone());
            ctx.tuples[*tuple_id] = new_tuple;
        }
        black_box(ctx.update_epoch);
    }
}

fn logical_secondary_key_changed_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                Value::identity(rotated_update_group(*tuple_id, ctx.update_epoch)),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.logical_secondary
                .replace_key_changed(*tuple_id, new_tuple.clone());
            ctx.tuples[*tuple_id] = new_tuple;
        }
        black_box(ctx.update_epoch);
    }
}

fn payload_relation_key_changed_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.payload_relation.tuples[*tuple_id].clone();
            let new_tuple = Tuple::new([
                Value::identity(rotated_update_group(*tuple_id, ctx.update_epoch)),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.payload_relation
                .replace_key_changed(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn arena_relation_key_changed_updates(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        ctx.update_epoch += 1;
        for tuple_id in &ctx.update_ids {
            let old_tuple = ctx.arena_relation.rows[*tuple_id].clone();
            let new_tuple = Tuple::new([
                Value::identity(rotated_update_group(*tuple_id, ctx.update_epoch)),
                old_tuple.values()[1].clone(),
                Value::int(ctx.update_epoch).unwrap(),
            ]);
            ctx.arena_relation.replace_key_changed(*tuple_id, new_tuple);
        }
        black_box(ctx.update_epoch);
    }
}

fn old_clone_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.old.clone());
    }
}

fn radix_clone_index(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.radix.clone());
    }
}

fn payload_relation_clone_indexes(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.payload_relation.clone());
    }
}

fn arena_relation_clone_indexes(ctx: &mut IndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(ctx.arena_relation.clone());
    }
}

fn shared_arena_relation_clone_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.shared_arena_relation.clone();
        black_box(cloned.secondary_positions.len());
        black_box(&cloned.primary);
        black_box(cloned);
    }
}

fn keyed_shared_arena_relation_clone_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.keyed_shared_arena_relation.clone();
        black_box(cloned.full_keys.len());
        black_box(&cloned.primary);
        black_box(cloned);
    }
}

fn append_version_arena_relation_clone_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.append_version_arena_relation.clone();
        black_box(cloned.appended_rows.len());
        black_box(cloned);
    }
}

fn dirty_current_arena_relation_clone_indexes(
    ctx: &mut IndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let cloned = ctx.dirty_current_arena_relation.clone();
        black_box(cloned.dirty_current.len());
        black_box(cloned);
    }
}

fn encode_radix_keys(ctx: &mut IndexContext, chunk_size: usize, chunk_num: usize) {
    for i in 0..chunk_size {
        let index = chunk_num.wrapping_mul(chunk_size).wrapping_add(i) % ctx.tuples.len();
        black_box(ctx.radix.tuple_key(&ctx.tuples[index]));
    }
}

fn project_btree_keys(ctx: &mut IndexContext, chunk_size: usize, chunk_num: usize) {
    for i in 0..chunk_size {
        let index = chunk_num.wrapping_mul(chunk_size).wrapping_add(i) % ctx.tuples.len();
        black_box(ctx.old.tuple_key(&ctx.tuples[index]));
    }
}

fn production_indexed_scan(ctx: &mut ProductionIndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(
            ctx.indexed_snapshot
                .scan(ctx.indexed_relation, &ctx.bindings)
                .unwrap(),
        );
    }
}

fn production_unindexed_scan(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(
            ctx.unindexed_snapshot
                .scan(ctx.unindexed_relation, &ctx.bindings)
                .unwrap(),
        );
    }
}

fn production_full_scan(ctx: &mut ProductionIndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        black_box(
            ctx.indexed_snapshot
                .scan(ctx.indexed_relation, &ctx.full_scan_bindings)
                .unwrap(),
        );
    }
}

fn production_unindexed_full_scan(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(
            ctx.unindexed_snapshot
                .scan(ctx.unindexed_relation, &ctx.full_scan_bindings)
                .unwrap(),
        );
    }
}

fn production_indexed_visit(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.indexed_snapshot
            .visit(ctx.indexed_relation, &ctx.bindings, &mut |_| {
                count += 1;
                Ok(ScanControl::Continue)
            })
            .unwrap();
        black_box(count);
    }
}

fn production_unindexed_visit(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.unindexed_snapshot
            .visit(ctx.unindexed_relation, &ctx.bindings, &mut |_| {
                count += 1;
                Ok(ScanControl::Continue)
            })
            .unwrap();
        black_box(count);
    }
}

fn production_full_visit(ctx: &mut ProductionIndexContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.indexed_snapshot
            .visit(ctx.indexed_relation, &ctx.full_scan_bindings, &mut |_| {
                count += 1;
                Ok(ScanControl::Continue)
            })
            .unwrap();
        black_box(count);
    }
}

fn production_unindexed_full_visit(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut count = 0usize;
        ctx.unindexed_snapshot
            .visit(ctx.unindexed_relation, &ctx.full_scan_bindings, &mut |_| {
                count += 1;
                Ok(ScanControl::Continue)
            })
            .unwrap();
        black_box(count);
    }
}

fn production_indexed_build(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(build_production_snapshot(
            ctx.indexed_relation,
            RelationMetadata::new(ctx.indexed_relation, Symbol::intern("IndexedBench"), 3)
                .with_index([0]),
            &ctx.tuples,
        ));
    }
}

fn production_unindexed_build(
    ctx: &mut ProductionIndexContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        black_box(build_production_snapshot(
            ctx.unindexed_relation,
            RelationMetadata::new(ctx.unindexed_relation, Symbol::intern("UnindexedBench"), 3)
                .without_indexes(),
            &ctx.tuples,
        ));
    }
}

fn build_tuples() -> Vec<Tuple> {
    let kind = Symbol::intern("bench_kind");
    let mut tuples = Vec::with_capacity(TUPLE_COUNT);
    for group in 0..GROUPS {
        for item in 0..ITEMS_PER_GROUP {
            tuples.push(Tuple::new([
                Value::identity(identity(group as u64)),
                Value::identity(identity(item as u64)),
                Value::symbol(kind),
            ]));
        }
    }
    tuples
}

fn build_wide_tuples() -> Vec<Tuple> {
    let kind = Symbol::intern("wide_bench_kind");
    let flavour = Symbol::intern("wide_flavour");
    let mut tuples = Vec::with_capacity(TUPLE_COUNT);
    for group in 0..GROUPS {
        for item in 0..ITEMS_PER_GROUP {
            let mut values = Vec::with_capacity(WIDE_TUPLE_ARITY);
            values.push(Value::identity(identity(group as u64)));
            values.push(Value::identity(identity(item as u64)));
            values.push(Value::symbol(kind));
            values.push(Value::symbol(flavour));
            for field in 4..WIDE_TUPLE_ARITY {
                values.push(Value::string(format!("g{group:03}-i{item:03}-f{field:02}")));
            }
            tuples.push(Tuple::new(values));
        }
    }
    tuples
}

fn update_ids() -> Vec<usize> {
    (0..SECONDARY_UPDATE_COUNT).collect()
}

fn dirty_append_version_arena_relation(
    tuples: &[Tuple],
    update_ids: &[usize],
) -> AppendVersionArenaRelationIndexes {
    let mut relation = AppendVersionArenaRelationIndexes::from_tuples_bulk(tuples);
    seed_append_version_updates(&mut relation, update_ids);
    relation
}

fn seeded_dirty_current_arena_relation(
    tuples: &[Tuple],
    update_ids: &[usize],
) -> DirtyCurrentArenaRelationIndexes {
    let mut relation = DirtyCurrentArenaRelationIndexes::from_tuples_bulk(tuples);
    seed_dirty_current_updates(&mut relation, update_ids);
    relation
}

fn seeded_wide_dirty_current_arena_relation(
    tuples: &[Tuple],
    update_ids: &[usize],
) -> DirtyCurrentArenaRelationIndexes {
    let mut relation = DirtyCurrentArenaRelationIndexes::from_tuples_bulk(tuples);
    seed_wide_dirty_current_updates(&mut relation, update_ids);
    relation
}

fn seeded_wide_keyed_dirty_current_arena_relation(
    tuples: &[Tuple],
    update_ids: &[usize],
) -> KeyedDirtyCurrentArenaRelationIndexes {
    let mut relation = KeyedDirtyCurrentArenaRelationIndexes::from_tuples_bulk(tuples);
    seed_wide_keyed_dirty_current_updates(&mut relation, update_ids);
    relation
}

fn seed_append_version_updates(
    relation: &mut AppendVersionArenaRelationIndexes,
    update_ids: &[usize],
) {
    for tuple_id in update_ids {
        let old_tuple = relation.current_tuple(*tuple_id).clone();
        relation.replace_same_key(*tuple_id, seeded_update_tuple(&old_tuple));
    }
}

fn seed_dirty_current_updates(
    relation: &mut DirtyCurrentArenaRelationIndexes,
    update_ids: &[usize],
) {
    for tuple_id in update_ids {
        let old_tuple = relation.current_tuple(*tuple_id).clone();
        relation.replace_same_key(*tuple_id, seeded_update_tuple(&old_tuple));
    }
}

fn seed_wide_dirty_current_updates(
    relation: &mut DirtyCurrentArenaRelationIndexes,
    update_ids: &[usize],
) {
    for tuple_id in update_ids {
        let old_tuple = relation.current_tuple(*tuple_id).clone();
        relation.replace_same_key(*tuple_id, seeded_wide_update_tuple(&old_tuple));
    }
}

fn seed_wide_keyed_dirty_current_updates(
    relation: &mut KeyedDirtyCurrentArenaRelationIndexes,
    update_ids: &[usize],
) {
    for tuple_id in update_ids {
        let old_tuple = relation.current_tuple(*tuple_id).clone();
        relation.replace_same_key(*tuple_id, seeded_wide_update_tuple(&old_tuple));
    }
}

fn seeded_update_tuple(old_tuple: &Tuple) -> Tuple {
    Tuple::new([
        old_tuple.values()[0].clone(),
        old_tuple.values()[1].clone(),
        Value::int(1).unwrap(),
    ])
}

fn seeded_wide_update_tuple(old_tuple: &Tuple) -> Tuple {
    let mut values = old_tuple.values().to_vec();
    let last = values
        .last_mut()
        .expect("wide benchmark tuple should not be empty");
    *last = Value::string("updated-wide-tail-value");
    Tuple::new(values)
}

fn build_production_snapshot(
    relation: RelationId,
    metadata: RelationMetadata,
    tuples: &[Tuple],
) -> Arc<Snapshot> {
    let kernel = RelationKernel::new();
    kernel.create_relation(metadata).unwrap();
    let mut tx = kernel.begin();
    for tuple in tuples {
        tx.assert(relation, tuple.clone()).unwrap();
    }
    tx.commit().unwrap().into_snapshot()
}

fn key_from_values<'a>(values: impl IntoIterator<Item = &'a Value>) -> RelationIndexKey {
    let mut key = RelationIndexKeyBuilder::new();
    for value in values {
        value.encode_ordered_into(&mut key);
    }
    key.finish()
}

fn full_tuple_key(tuple: &Tuple) -> RelationIndexKey {
    key_from_values(tuple.values())
}

fn tuple_id_key(tuple_id: usize) -> RelationIndexKey {
    let value = Value::int(tuple_id as i64).expect("benchmark tuple id should fit in int");
    key_from_values([&value])
}

fn identity(raw: u64) -> Identity {
    Identity::new(raw).unwrap()
}

fn rotated_update_group(tuple_id: usize, epoch: i64) -> Identity {
    let group = (tuple_id + epoch as usize) % GROUPS;
    identity(1_000_000 + group as u64)
}

fn relation(raw: u64) -> RelationId {
    Identity::new(raw).unwrap()
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, prefix, rebuild, clone, key, or any benchmark name substring".to_string()
        ),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<IndexContext>("prefix", |g| {
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench("old_btree_prefix_scan", old_prefix_scan);
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench("radix_prefix_scan", radix_prefix_scan);
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench("radix_secondary_prefix_visit", radix_secondary_prefix_visit);
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "logical_secondary_prefix_visit",
                    logical_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench("radix_secondary_prefix_scan", radix_secondary_prefix_scan);
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "logical_secondary_prefix_scan",
                    logical_secondary_prefix_scan,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "payload_relation_secondary_prefix_visit",
                    payload_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "arena_relation_secondary_prefix_visit",
                    arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "shared_arena_relation_secondary_prefix_visit",
                    shared_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "keyed_shared_arena_relation_secondary_prefix_visit",
                    keyed_shared_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "append_version_arena_relation_secondary_prefix_visit",
                    append_version_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "dirty_append_version_arena_relation_secondary_prefix_visit",
                    dirty_append_version_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "dirty_current_arena_relation_secondary_prefix_visit",
                    dirty_current_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "dirty_dirty_current_arena_relation_secondary_prefix_visit",
                    dirty_dirty_current_arena_relation_secondary_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "dirty_append_version_arena_relation_dirty_hit_prefix_visit",
                    dirty_append_version_arena_relation_dirty_hit_prefix_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "dirty_current_arena_relation_dirty_hit_prefix_visit",
                    dirty_current_arena_relation_dirty_hit_prefix_visit,
                );
        });

        runner.group::<IndexContext>("rebuild", |g| {
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("old_btree_rebuild_index", old_rebuild_index);
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("radix_rebuild_index", radix_rebuild_index);
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "radix_secondary_rebuild_index",
                    radix_secondary_rebuild_index,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "logical_secondary_rebuild_index",
                    logical_secondary_rebuild_index,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "payload_relation_rebuild_indexes",
                    payload_relation_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "arena_relation_rebuild_indexes",
                    arena_relation_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "payload_relation_bulk_rebuild_indexes",
                    payload_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "arena_relation_bulk_rebuild_indexes",
                    arena_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "shared_arena_relation_bulk_rebuild_indexes",
                    shared_arena_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "keyed_shared_arena_relation_bulk_rebuild_indexes",
                    keyed_shared_arena_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "dirty_current_arena_relation_compact_to_shared",
                    dirty_current_arena_relation_compact_to_shared,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "dirty_current_arena_relation_compact_to_keyed_shared",
                    dirty_current_arena_relation_compact_to_keyed_shared,
                );
        });

        runner.group::<IndexContext>("update", |g| {
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "radix_secondary_same_key_updates",
                radix_secondary_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "logical_secondary_same_key_updates",
                logical_secondary_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "payload_relation_same_key_updates",
                payload_relation_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "arena_relation_same_key_updates",
                arena_relation_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "append_version_arena_relation_same_key_updates",
                append_version_arena_relation_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "same_key_update",
            ))
            .bench(
                "dirty_current_arena_relation_same_key_updates",
                dirty_current_arena_relation_same_key_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "key_changed_update",
            ))
            .bench(
                "radix_secondary_key_changed_updates",
                radix_secondary_key_changed_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "key_changed_update",
            ))
            .bench(
                "logical_secondary_key_changed_updates",
                logical_secondary_key_changed_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "key_changed_update",
            ))
            .bench(
                "payload_relation_key_changed_updates",
                payload_relation_key_changed_updates,
            );
            g.throughput(Throughput::per_operation(
                SECONDARY_UPDATE_COUNT as u64,
                "key_changed_update",
            ))
            .bench(
                "arena_relation_key_changed_updates",
                arena_relation_key_changed_updates,
            );
        });

        runner.group::<IndexContext>("clone", |g| {
            g.throughput(Throughput::per_operation(1, "clone"))
                .bench("old_btree_clone_index", old_clone_index);
            g.throughput(Throughput::per_operation(1, "clone"))
                .bench("radix_clone_index", radix_clone_index);
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "payload_relation_clone_indexes",
                payload_relation_clone_indexes,
            );
            g.throughput(Throughput::per_operation(1, "clone"))
                .bench("arena_relation_clone_indexes", arena_relation_clone_indexes);
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "shared_arena_relation_clone_indexes",
                shared_arena_relation_clone_indexes,
            );
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "keyed_shared_arena_relation_clone_indexes",
                keyed_shared_arena_relation_clone_indexes,
            );
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "append_version_arena_relation_clone_indexes",
                append_version_arena_relation_clone_indexes,
            );
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "dirty_current_arena_relation_clone_indexes",
                dirty_current_arena_relation_clone_indexes,
            );
        });

        runner.group::<IndexContext>("key", |g| {
            g.throughput(Throughput::per_operation(1, "key"))
                .bench("old_btree_project_key", project_btree_keys);
            g.throughput(Throughput::per_operation(1, "key"))
                .bench("radix_encode_key", encode_radix_keys);
        });

        runner.group::<WideIndexContext>("wide", |g| {
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "wide_shared_arena_relation_bulk_rebuild_indexes",
                    wide_shared_arena_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "wide_keyed_shared_arena_relation_bulk_rebuild_indexes",
                    wide_keyed_shared_arena_relation_bulk_rebuild_indexes,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "wide_dirty_current_arena_relation_compact_to_shared",
                    wide_dirty_current_arena_relation_compact_to_shared,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "wide_dirty_current_arena_relation_compact_to_keyed_shared",
                    wide_dirty_current_arena_relation_compact_to_keyed_shared,
                );
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "wide_shared_arena_relation_clone_indexes",
                wide_shared_arena_relation_clone_indexes,
            );
            g.throughput(Throughput::per_operation(1, "clone")).bench(
                "wide_keyed_shared_arena_relation_clone_indexes",
                wide_keyed_shared_arena_relation_clone_indexes,
            );
        });

        runner.group::<ProductionIndexContext>("production", |g| {
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "production_low_cardinality_indexed_scan",
                    production_indexed_scan,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "production_low_cardinality_unindexed_scan",
                    production_unindexed_scan,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("production_full_scan", production_full_scan);
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "production_unindexed_full_scan",
                    production_unindexed_full_scan,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "production_low_cardinality_indexed_visit",
                    production_indexed_visit,
                );
            g.throughput(Throughput::per_operation(ITEMS_PER_GROUP as u64, "tuple"))
                .bench(
                    "production_low_cardinality_unindexed_visit",
                    production_unindexed_visit,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("production_full_visit", production_full_visit);
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "production_unindexed_full_visit",
                    production_unindexed_full_visit,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "production_low_cardinality_indexed_build",
                    production_indexed_build,
                );
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench(
                    "production_low_cardinality_unindexed_build",
                    production_unindexed_build,
                );
        });
    }
);
