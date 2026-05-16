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

use mica_relation_kernel::Tuple;
use mica_var::{Identity, OrderedKeySink, Symbol, Value};
use micromeasure::{BenchContext, BenchmarkMainOptions, Throughput, benchmark_main, black_box};
use rart::{OverflowKey, OverflowKeyBuilder, VersionedAdaptiveRadixTree, VisitControl};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const GROUPS: usize = 128;
const ITEMS_PER_GROUP: usize = 128;
const TUPLE_COUNT: usize = GROUPS * ITEMS_PER_GROUP;

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
    prefix_values: Vec<Value>,
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

impl BenchContext for IndexContext {
    fn prepare(_num_chunks: usize) -> Self {
        let tuples = build_tuples();
        let old = OldTupleIndex::from_tuples(&[0, 1, 2], &tuples);
        let radix = RadixTupleIndex::from_tuples(&[0, 1, 2], &tuples);
        Self {
            tuples,
            old,
            radix,
            prefix_values: vec![Value::identity(identity(42))],
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

fn key_from_values<'a>(values: impl IntoIterator<Item = &'a Value>) -> RelationIndexKey {
    let mut key = RelationIndexKeyBuilder::new();
    for value in values {
        value.encode_ordered_into(&mut key);
    }
    key.finish()
}

fn identity(raw: u64) -> Identity {
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
        });

        runner.group::<IndexContext>("rebuild", |g| {
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("old_btree_rebuild_index", old_rebuild_index);
            g.throughput(Throughput::per_operation(TUPLE_COUNT as u64, "tuple"))
                .bench("radix_rebuild_index", radix_rebuild_index);
        });

        runner.group::<IndexContext>("clone", |g| {
            g.throughput(Throughput::per_operation(1, "clone"))
                .bench("old_btree_clone_index", old_clone_index);
            g.throughput(Throughput::per_operation(1, "clone"))
                .bench("radix_clone_index", radix_clone_index);
        });

        runner.group::<IndexContext>("key", |g| {
            g.throughput(Throughput::per_operation(1, "key"))
                .bench("old_btree_project_key", project_btree_keys);
            g.throughput(Throughput::per_operation(1, "key"))
                .bench("radix_encode_key", encode_radix_keys);
        });
    }
);
