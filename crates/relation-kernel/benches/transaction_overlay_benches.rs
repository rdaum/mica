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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use mica_relation_kernel::{RelationId, RelationKernel, RelationMetadata, Tuple};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{BenchContext, BenchmarkMainOptions, Throughput, benchmark_main, black_box};
use std::time::Duration;

const GROUPS: usize = 64;
const ITEMS_PER_GROUP: usize = 64;
const LOCAL_WRITE_COUNT: usize = GROUPS * ITEMS_PER_GROUP;
const REPEATED_SCANS: usize = 64;
const FUNCTIONAL_UPDATE_COUNT: usize = 1024;
const MULTI_RELATION_COUNT: usize = 64;
const MULTI_RELATION_TUPLES: usize = 32;
const LARGE_GROUPS: usize = 1024;
const LARGE_ITEMS_PER_GROUP: usize = 64;
const LARGE_RELATION_TUPLES: usize = LARGE_GROUPS * LARGE_ITEMS_PER_GROUP;
const LARGE_UPDATE_COUNT: usize = 1024;

struct OverlayContext {
    kernel: RelationKernel,
    tuples: Vec<Tuple>,
    scan_bindings: [Option<Value>; 3],
    next_epoch: i64,
}

struct LargeRelationContext {
    kernel: RelationKernel,
    next_epoch: i64,
    next_large_set_epoch: i64,
}

impl BenchContext for OverlayContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(RelationMetadata::new(
                bench_relation(),
                Symbol::intern("BenchOverlay"),
                3,
            ))
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                committed_overlay_relation(),
                Symbol::intern("BenchCommittedOverlay"),
                3,
            ))
            .unwrap();
        kernel
            .create_relation(
                RelationMetadata::new(
                    functional_relation(),
                    Symbol::intern("BenchFunctionalOverlay"),
                    3,
                )
                .with_index([0])
                .with_conflict_policy(
                    mica_relation_kernel::ConflictPolicy::Functional {
                        key_positions: vec![0],
                    },
                ),
            )
            .unwrap();
        for relation in 0..MULTI_RELATION_COUNT {
            kernel
                .create_relation(RelationMetadata::new(
                    multi_relation(relation as u64),
                    Symbol::intern("BenchMultiRelationCommit"),
                    3,
                ))
                .unwrap();
            kernel
                .create_relation(
                    RelationMetadata::new(
                        multi_relation_unindexed(relation as u64),
                        Symbol::intern("BenchMultiRelationCommitUnindexed"),
                        3,
                    )
                    .without_indexes(),
                )
                .unwrap();
        }

        let tuples = build_tuples();
        let mut seed = kernel.begin();
        for tuple in &tuples {
            seed.assert(committed_overlay_relation(), tuple.clone())
                .unwrap();
        }
        for key in 0..FUNCTIONAL_UPDATE_COUNT {
            seed.replace_functional(
                functional_relation(),
                functional_tuple(key as u64, 0, Symbol::intern("bench_kind")),
            )
            .unwrap();
        }
        seed.commit().unwrap();

        Self {
            kernel,
            tuples,
            scan_bindings: [Some(Value::identity(identity(42))), None, None],
            next_epoch: 1,
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

impl BenchContext for LargeRelationContext {
    fn prepare(_num_chunks: usize) -> Self {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(
                RelationMetadata::new(
                    large_functional_relation(),
                    Symbol::intern("BenchLargeFunctionalRelation"),
                    3,
                )
                .with_index([0, 1])
                .with_conflict_policy(
                    mica_relation_kernel::ConflictPolicy::Functional {
                        key_positions: vec![0, 1],
                    },
                ),
            )
            .unwrap();
        kernel
            .create_relation(RelationMetadata::new(
                large_set_relation(),
                Symbol::intern("BenchLargeSetRelation"),
                3,
            ))
            .unwrap();

        let mut seed = kernel.begin();
        for tuple in build_large_functional_tuples(0) {
            seed.replace_functional(large_functional_relation(), tuple)
                .unwrap();
        }
        for tuple in build_large_set_tuples(0) {
            seed.assert(large_set_relation(), tuple).unwrap();
        }
        seed.commit().unwrap();

        Self {
            kernel,
            next_epoch: 1,
            next_large_set_epoch: 1,
        }
    }

    fn chunk_size() -> Option<usize> {
        Some(1)
    }
}

fn assert_local_writes(ctx: &mut OverlayContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for tuple in &ctx.tuples {
            tx.assert(bench_relation(), tuple.clone()).unwrap();
        }
        black_box(tx.is_read_only());
    }
}

fn assert_sized_local_writes<const N: usize>(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for tuple in ctx.tuples.iter().take(N) {
            tx.assert(bench_relation(), tuple.clone()).unwrap();
        }
        black_box(tx.is_read_only());
    }
}

fn scan_local_writes_with_binding(ctx: &mut OverlayContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for tuple in &ctx.tuples {
            tx.assert(bench_relation(), tuple.clone()).unwrap();
        }

        let mut total = 0usize;
        for _ in 0..REPEATED_SCANS {
            total += tx.scan(bench_relation(), &ctx.scan_bindings).unwrap().len();
        }
        black_box(total);
    }
}

fn scan_sized_local_writes_with_binding<const N: usize>(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    let scan_bindings = [Some(Value::identity(identity(0))), None, None];
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for tuple in ctx.tuples.iter().take(N) {
            tx.assert(bench_relation(), tuple.clone()).unwrap();
        }

        let mut total = 0usize;
        for _ in 0..REPEATED_SCANS {
            total += tx.scan(bench_relation(), &scan_bindings).unwrap().len();
        }
        black_box(total);
    }
}

fn scan_local_writes_once_with_binding(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for tuple in &ctx.tuples {
            tx.assert(bench_relation(), tuple.clone()).unwrap();
        }

        black_box(tx.scan(bench_relation(), &ctx.scan_bindings).unwrap().len());
    }
}

fn scan_committed_rows_with_local_retractions(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for item in 0..ITEMS_PER_GROUP {
            if item % 2 == 0 {
                tx.retract(
                    committed_overlay_relation(),
                    Tuple::new([
                        Value::identity(identity(42)),
                        Value::identity(identity(item as u64)),
                        Value::symbol(Symbol::intern("bench_kind")),
                    ]),
                )
                .unwrap();
            }
        }

        let mut total = 0usize;
        for _ in 0..REPEATED_SCANS {
            total += tx
                .scan(committed_overlay_relation(), &ctx.scan_bindings)
                .unwrap()
                .len();
        }
        black_box(total);
    }
}

fn scan_committed_rows_with_local_assertions(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let mut tx = ctx.kernel.begin();
        for item in ITEMS_PER_GROUP..(ITEMS_PER_GROUP * 2) {
            tx.assert(
                committed_overlay_relation(),
                Tuple::new([
                    Value::identity(identity(42)),
                    Value::identity(identity(item as u64)),
                    Value::symbol(Symbol::intern("bench_kind")),
                ]),
            )
            .unwrap();
        }

        let mut total = 0usize;
        for _ in 0..REPEATED_SCANS {
            total += tx
                .scan(committed_overlay_relation(), &ctx.scan_bindings)
                .unwrap()
                .len();
        }
        black_box(total);
    }
}

fn commit_functional_updates(ctx: &mut OverlayContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let epoch = ctx.next_epoch;
        ctx.next_epoch += 1;

        let mut tx = ctx.kernel.begin();
        let kind = Symbol::intern("bench_kind");
        for key in 0..FUNCTIONAL_UPDATE_COUNT {
            tx.replace_functional(
                functional_relation(),
                functional_tuple(key as u64, epoch, kind),
            )
            .unwrap();
        }
        let result = tx.commit().unwrap();
        black_box(result.snapshot().version());
    }
}

fn prepare_functional_updates(ctx: &mut OverlayContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        let epoch = ctx.next_epoch;
        ctx.next_epoch += 1;

        let mut tx = ctx.kernel.begin();
        let kind = Symbol::intern("bench_kind");
        for key in 0..FUNCTIONAL_UPDATE_COUNT {
            tx.replace_functional(
                functional_relation(),
                functional_tuple(key as u64, epoch, kind),
            )
            .unwrap();
        }
        black_box(tx.is_read_only());
    }
}

fn commit_multi_relation_set_writes(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    commit_multi_relation_set_writes_for(ctx, chunk_size, multi_relation)
}

fn commit_multi_relation_unindexed_set_writes(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    commit_multi_relation_set_writes_for(ctx, chunk_size, multi_relation_unindexed)
}

fn commit_stale_disjoint_set_writes(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let stale_epoch = ctx.next_epoch;
        let concurrent_epoch = ctx.next_epoch + 1;
        ctx.next_epoch += 2;

        let mut stale = ctx.kernel.begin();
        let mut concurrent = ctx.kernel.begin();
        let kind = Symbol::intern("bench_kind");

        for group in 0..GROUPS {
            for item in 0..ITEMS_PER_GROUP {
                stale
                    .assert(
                        bench_relation(),
                        Tuple::new([
                            Value::int(stale_epoch).unwrap(),
                            Value::identity(identity((group * ITEMS_PER_GROUP + item) as u64)),
                            Value::symbol(kind),
                        ]),
                    )
                    .unwrap();
            }
        }

        concurrent
            .assert(
                bench_relation(),
                Tuple::new([
                    Value::int(concurrent_epoch).unwrap(),
                    Value::identity(identity(1_000_000_000 + concurrent_epoch as u64)),
                    Value::symbol(kind),
                ]),
            )
            .unwrap();
        concurrent.commit().unwrap();

        let result = stale.commit().unwrap();
        black_box(result.snapshot().version());
    }
}

fn commit_multi_relation_set_writes_for(
    ctx: &mut OverlayContext,
    chunk_size: usize,
    relation_id: fn(u64) -> RelationId,
) {
    for _ in 0..chunk_size {
        let epoch = ctx.next_epoch;
        ctx.next_epoch += 1;

        let mut tx = ctx.kernel.begin();
        let kind = Symbol::intern("bench_kind");
        for relation in 0..MULTI_RELATION_COUNT {
            for item in 0..MULTI_RELATION_TUPLES {
                tx.assert(
                    relation_id(relation as u64),
                    Tuple::new([
                        Value::int(epoch).unwrap(),
                        Value::identity(identity(item as u64)),
                        Value::symbol(kind),
                    ]),
                )
                .unwrap();
            }
        }
        let result = tx.commit().unwrap();
        black_box(result.snapshot().version());
    }
}

fn commit_large_functional_updates(
    ctx: &mut LargeRelationContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let epoch = ctx.next_epoch;
        ctx.next_epoch += 1;

        let mut tx = ctx.kernel.begin();
        for index in 0..LARGE_UPDATE_COUNT {
            let group = (index / LARGE_ITEMS_PER_GROUP) as u64;
            let item = (index % LARGE_ITEMS_PER_GROUP) as u64;
            tx.replace_functional(
                large_functional_relation(),
                Tuple::new([
                    Value::identity(identity(group)),
                    Value::identity(identity(item)),
                    Value::int(epoch).unwrap(),
                ]),
            )
            .unwrap();
        }
        let result = tx.commit().unwrap();
        black_box(result.snapshot().version());
    }
}

fn commit_large_set_replacements(
    ctx: &mut LargeRelationContext,
    chunk_size: usize,
    _chunk_num: usize,
) {
    for _ in 0..chunk_size {
        let old_epoch = ctx.next_large_set_epoch - 1;
        let new_epoch = ctx.next_large_set_epoch;
        ctx.next_large_set_epoch += 1;

        let mut tx = ctx.kernel.begin();
        let kind = Symbol::intern("bench_kind");
        for index in 0..LARGE_UPDATE_COUNT {
            let id = index as u64;
            tx.retract(
                large_set_relation(),
                Tuple::new([
                    Value::identity(identity(id)),
                    Value::int(old_epoch).unwrap(),
                    Value::symbol(kind),
                ]),
            )
            .unwrap();
            tx.assert(
                large_set_relation(),
                Tuple::new([
                    Value::identity(identity(id)),
                    Value::int(new_epoch).unwrap(),
                    Value::symbol(kind),
                ]),
            )
            .unwrap();
        }
        let result = tx.commit().unwrap();
        black_box(result.snapshot().version());
    }
}

fn build_tuples() -> Vec<Tuple> {
    let kind = Symbol::intern("bench_kind");
    let mut tuples = Vec::with_capacity(LOCAL_WRITE_COUNT);
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

fn build_large_functional_tuples(epoch: i64) -> Vec<Tuple> {
    let mut tuples = Vec::with_capacity(LARGE_RELATION_TUPLES);
    for group in 0..LARGE_GROUPS {
        for item in 0..LARGE_ITEMS_PER_GROUP {
            tuples.push(Tuple::new([
                Value::identity(identity(group as u64)),
                Value::identity(identity(item as u64)),
                Value::int(epoch).unwrap(),
            ]));
        }
    }
    tuples
}

fn build_large_set_tuples(epoch: i64) -> Vec<Tuple> {
    let kind = Symbol::intern("bench_kind");
    let mut tuples = Vec::with_capacity(LARGE_RELATION_TUPLES);
    for id in 0..LARGE_RELATION_TUPLES {
        tuples.push(Tuple::new([
            Value::identity(identity(id as u64)),
            Value::int(epoch).unwrap(),
            Value::symbol(kind),
        ]));
    }
    tuples
}

fn functional_tuple(key: u64, epoch: i64, kind: Symbol) -> Tuple {
    Tuple::new([
        Value::identity(identity(key)),
        Value::int(epoch).unwrap(),
        Value::symbol(kind),
    ])
}

fn bench_relation() -> RelationId {
    identity(700)
}

fn committed_overlay_relation() -> RelationId {
    identity(702)
}

fn functional_relation() -> RelationId {
    identity(701)
}

fn multi_relation(index: u64) -> RelationId {
    identity(800 + index)
}

fn multi_relation_unindexed(index: u64) -> RelationId {
    identity(900 + index)
}

fn large_functional_relation() -> RelationId {
    identity(1000)
}

fn large_set_relation() -> RelationId {
    identity(1001)
}

fn identity(raw: u64) -> Identity {
    Identity::new(raw).unwrap()
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some("all, assert, scan, or any benchmark name substring".to_string()),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 5,
            max_samples: 10,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<OverlayContext>("assert", |g| {
            g.throughput(Throughput::per_operation(LOCAL_WRITE_COUNT as u64, "write"))
                .bench("tx_overlay_assert_local_writes", assert_local_writes);
            g.throughput(Throughput::per_operation(32, "write")).bench(
                "tx_overlay_sized_assert_local_writes_32",
                assert_sized_local_writes::<32>,
            );
            g.throughput(Throughput::per_operation(64, "write")).bench(
                "tx_overlay_sized_assert_local_writes_64",
                assert_sized_local_writes::<64>,
            );
            g.throughput(Throughput::per_operation(128, "write")).bench(
                "tx_overlay_sized_assert_local_writes_128",
                assert_sized_local_writes::<128>,
            );
            g.throughput(Throughput::per_operation(256, "write")).bench(
                "tx_overlay_sized_assert_local_writes_256",
                assert_sized_local_writes::<256>,
            );
            g.throughput(Throughput::per_operation(512, "write")).bench(
                "tx_overlay_sized_assert_local_writes_512",
                assert_sized_local_writes::<512>,
            );
        });

        runner.group::<OverlayContext>("scan", |g| {
            g.throughput(Throughput::per_operation(
                (ITEMS_PER_GROUP * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_bound_scan_local_writes",
                scan_local_writes_with_binding,
            );
            g.throughput(Throughput::per_operation(
                ITEMS_PER_GROUP as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_single_bound_scan_local_writes",
                scan_local_writes_once_with_binding,
            );
            g.throughput(Throughput::per_operation(
                (32 * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_sized_bound_scan_local_writes_32",
                scan_sized_local_writes_with_binding::<32>,
            );
            g.throughput(Throughput::per_operation(
                (64 * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_sized_bound_scan_local_writes_64",
                scan_sized_local_writes_with_binding::<64>,
            );
            g.throughput(Throughput::per_operation(
                (64 * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_sized_bound_scan_local_writes_128",
                scan_sized_local_writes_with_binding::<128>,
            );
            g.throughput(Throughput::per_operation(
                (64 * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_sized_bound_scan_local_writes_256",
                scan_sized_local_writes_with_binding::<256>,
            );
            g.throughput(Throughput::per_operation(
                (64 * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_sized_bound_scan_local_writes_512",
                scan_sized_local_writes_with_binding::<512>,
            );
            g.throughput(Throughput::per_operation(
                ((ITEMS_PER_GROUP / 2) * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_committed_scan_local_retractions",
                scan_committed_rows_with_local_retractions,
            );
            g.throughput(Throughput::per_operation(
                ((ITEMS_PER_GROUP * 2) * REPEATED_SCANS) as u64,
                "matched_tuple",
            ))
            .bench(
                "tx_overlay_committed_scan_local_assertions",
                scan_committed_rows_with_local_assertions,
            );
        });

        runner.group::<OverlayContext>("commit", |g| {
            g.throughput(Throughput::per_operation(
                FUNCTIONAL_UPDATE_COUNT as u64,
                "functional_update",
            ))
            .bench(
                "tx_prepare_functional_local_updates",
                prepare_functional_updates,
            );
            g.throughput(Throughput::per_operation(
                FUNCTIONAL_UPDATE_COUNT as u64,
                "functional_update",
            ))
            .bench(
                "tx_commit_functional_local_updates",
                commit_functional_updates,
            );
            g.throughput(Throughput::per_operation(
                (MULTI_RELATION_COUNT * MULTI_RELATION_TUPLES) as u64,
                "set_write",
            ))
            .bench(
                "tx_commit_multi_relation_set_writes",
                commit_multi_relation_set_writes,
            );
            g.throughput(Throughput::per_operation(
                (MULTI_RELATION_COUNT * MULTI_RELATION_TUPLES) as u64,
                "set_write",
            ))
            .bench(
                "tx_commit_multi_relation_unindexed_set_writes",
                commit_multi_relation_unindexed_set_writes,
            );
            g.throughput(Throughput::per_operation(
                LOCAL_WRITE_COUNT as u64,
                "stale_set_write",
            ))
            .bench(
                "tx_commit_stale_disjoint_set_writes",
                commit_stale_disjoint_set_writes,
            );
        });

        runner.group::<LargeRelationContext>("large_commit", |g| {
            g.throughput(Throughput::per_operation(
                LARGE_UPDATE_COUNT as u64,
                "large_functional_update",
            ))
            .bench(
                "tx_commit_large_functional_updates",
                commit_large_functional_updates,
            );
            g.throughput(Throughput::per_operation(
                (LARGE_UPDATE_COUNT * 2) as u64,
                "large_set_write",
            ))
            .bench(
                "tx_commit_large_set_replacements",
                commit_large_set_replacements,
            );
        });
    }
);
