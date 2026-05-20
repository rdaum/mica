# MVCC Storage And Indexing Notes

This is a running evidence log for Mica relation-kernel storage and indexing
work. The goal is to keep experiments grounded in before/after measurements
rather than intuition.

## Reference: Wu et al.

`p781-Wu.pdf`, *An Empirical Evaluation of In-Memory Multi-Version
Concurrency Control*, is the main reference for this track.

Relevant takeaways for Mica:

- MVCC performance is strongly shaped by version storage and index maintenance,
  not only by the concurrency-control protocol.
- Current-version access matters. Newest-to-oldest version chains perform better
  than oldest-to-newest chains in the paper because traversing stale versions
  pollutes caches and adds pointer chasing.
- Logical index pointers trade one level of indirection for less secondary-index
  churn on write-heavy workloads.
- Physical index pointers can help read-heavy paths, but they make updates more
  expensive because more index entries must be maintained.
- Batch-oriented garbage collection reduces synchronization overhead compared
  with eager per-tuple cleanup in update-heavy workloads.

Mica is not a conventional tuple-version-chain DBMS today. Its live snapshot is
an immutable relation map plus per-transaction write overlays. The closest
current analogues are:

- `Snapshot.relations`: current committed relation state.
- `Transaction.writes`: transaction-local new version intent.
- `RelationState.indexes`: secondary access paths for committed relation state.
- derived-relation caches and dispatch caches: snapshot-scoped read acceleration.

## Current Baseline

`Transaction.writes` currently uses:

```rust
HashMap<RelationId, RelationWriteOverlay>

RelationWriteOverlay:
    small: sorted Vec<OverlayEntry>
    promoted: AdaptiveRadixTree<RadixTupleKey, OverlayEntry>
```

This keeps relation lookup cheap while avoiding whole-tuple hashing. Small
per-relation write sets stay in a compact vector ordered by tuple, which avoids
tree-node overhead for tiny overlays. Larger per-relation write sets promote to
an ART full-tuple key map, which makes repeated dirty scans and large local
overlays cheaper.

The focused overlay benchmark added for this track is:

```sh
cargo bench -p mica-relation-kernel --bench transaction_overlay_benches -- all
```

Baseline from commit `4e3a96b`:

| Benchmark | Median |
| --- | ---: |
| `tx_overlay_assert_local_writes` | 545,264 ns/op |
| `tx_overlay_bound_scan_local_writes` | 1,416,208 ns/op |

The benchmark writes 4096 transaction-local tuples. The scan case then performs
64 bound scans over the same transaction overlay, matching one 64-tuple group
per scan.

## Rejected Experiment: Eager ART Overlay

An eager ART-backed overlay was tried and reverted.

Shape:

```rust
HashMap<RelationId, RelationWriteOverlay>

RelationWriteOverlay:
    full ART: full tuple key -> OverlayEntry
    projected ART indexes matching relation metadata
```

Result:

| Benchmark | Baseline | Eager ART | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_assert_local_writes` | 545,264 ns/op | 7,547,967 ns/op | -92.8% throughput |
| `tx_overlay_bound_scan_local_writes` | 1,416,208 ns/op | 7,883,855 ns/op | -82.0% throughput |

Conclusion: maintaining ART indexes eagerly in the transaction overlay is too
expensive for this workload. The write path paid key-encoding and index-update
costs for every local tuple, and the scan path did not recover enough time to
justify it.

This supports the same general lesson as Wu et al.'s logical-pointer discussion:
extra index maintenance on write-heavy paths can dominate even when it promises
better reads.

## Superseded Experiment: Full-Key ART Primary Overlay

A narrower ART overlay was tried earlier and reverted.

Shape:

```rust
HashMap<RelationId, RelationWriteOverlay>

RelationWriteOverlay:
    full ART: full tuple key -> { tuple, local change }
```

Unlike the eager overlay, this did not maintain projected secondary indexes.
Its purpose was to test whether a precomputed radix tuple key was enough to make
ART a better primary transaction overlay than `BTreeMap<Tuple, LocalChange>`.

Result:

| Benchmark | Baseline | Full-key ART | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_assert_local_writes` | 545,264 ns/op | 946,416 ns/op | -42.4% throughput |
| `tx_overlay_bound_scan_local_writes` | 1,416,208 ns/op | 2,760,592 ns/op | -48.7% throughput |

Conclusion at that point: precomputing a radix-friendly tuple key did not, by
itself, make ART a good primary map for transaction-local writes. The write path
still paid ordered value encoding, ART traversal, and extra stored-entry
overhead, while the scan path still performed an all-values traversal because it
had no projected prefix index to exploit.

That old result has been superseded by the current adaptive primary overlay
experiment below. The durable lesson remains: ART is not justified as a blind
replacement for every small transaction overlay, but it is useful after a local
write set grows large enough to amortize key encoding and tree setup.

## Candidate Directions

### Accepted: Transaction-Local Functional Visibility

The focused overlay benchmark now also includes:

```text
tx_commit_functional_local_updates
```

This benchmark seeds a functional relation with 1024 keys, then commits a
transaction that replaces all 1024 values.

Baseline:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 24,208,429 ns/op |

The hot path was `replace_functional`. Before the change, each replacement
looked for the current visible tuple by scanning the growing transaction write
overlay. A transaction that replaced many distinct functional keys in one batch
therefore paid avoidable repeated projection and comparison work.

Change:

```rust
Transaction:
    functional visible map:
        (relation, functional key positions) -> projected key -> visible tuple
```

The map is created on demand for functional replacement and maintained as local
assertions/retractions are recorded. Transactions that never ask for functional
visibility do not build it.

Result:

| Benchmark | Baseline | Functional visibility map | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_functional_local_updates` | 24,208,429 ns/op | 10,839,710 ns/op | +121.5% throughput |

The broad overlay benchmarks remain in the same range after the early return for
transactions without functional visibility:

| Benchmark | Median |
| --- | ---: |
| `tx_overlay_assert_local_writes` | 560,592 ns/op |
| `tx_overlay_bound_scan_local_writes` | 1,448,383 ns/op |
| `tx_commit_functional_local_updates` | 10,839,710 ns/op |

Conclusion: relation-aware write-set summaries are a better fit here than
replacing the primary local-write map. This matches the general Wu et al.
lesson that MVCC systems need to be careful about index maintenance: the useful
index is the one that avoids repeated transaction-local work on a path that
actually needs it.

### Lazy Transaction Overlay Indexes

At this point in the experiment log, keep the then-current write path:

```rust
BTreeMap<Tuple, LocalChange>
```

A lazy local overlay scan index was added after the functional visibility map.
It is deliberately transaction-local and relation-metadata driven. For a dirty
relation scan with bound leading index positions, the overlay tracks how often a
prefix shape has been requested. The first request keeps the old full-overlay
scan path. The second request builds a projected local index and subsequent
scans reuse it until the transaction writes that relation again.

Shape:

```rust
RelationWriteOverlay:
    changes: BTreeMap<Tuple, LocalChange>
    scan indexes:
        bound prefix positions -> radix projected key -> local changes
```

This preserves the cheap write path for write-only transactions and avoids
building an index for one-off scans.

Result against the post-functional-visibility baseline:

| Benchmark | Before lazy scan index | Lazy scan index | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 1,448,383 ns/op | 1,008,016 ns/op | +44.9% throughput |
| `tx_overlay_assert_local_writes` | 560,592 ns/op | 569,248 ns/op | ~0% |
| `tx_commit_functional_local_updates` | 10,839,710 ns/op | 10,809,630 ns/op | ~0% |

A one-off scan guardrail benchmark was added after this change:

| Benchmark | Median |
| --- | ---: |
| `tx_overlay_single_bound_scan_local_writes` | 572,096 ns/op |

Expected trade-off:

- no index maintenance cost for write-only or commit-only transactions;
- first bound scan keeps the old full-overlay path;
- repeated scans in one transaction can reuse the index;
- useful for rule evaluation or dispatch if they repeatedly scan a dirty
  transaction overlay.

The next benchmark refinement should compare the one-off scan to an explicit
pre-index baseline if this path becomes contentious.

Conclusion: lazy local scan indexes are useful for repeated dirty scans, but
should remain adaptive. Eager overlay indexing was already rejected; the value
comes from waiting until the transaction demonstrates repeated local scan
demand.

### Accepted: ART-Backed Lazy Scan Index

The first lazy scan index used:

```rust
BTreeMap<TupleKey, Vec<(Tuple, LocalChange)>>
```

where `TupleKey` is a cloned `Vec<Value>`. That worked because the index is
only built after repeated scan demand, but each lookup and insertion still paid
value-vector comparison costs.

Because the lazy scan index is already precomputing projected keys, it is a
better ART candidate than the primary transaction write map. Two variants were
measured:

- `VersionedAdaptiveRadixTree<RadixTupleKey, Vec<_>>`
- `AdaptiveRadixTree<RadixTupleKey, Vec<_>>`

The versioned tree is a poor fit for this scratch structure. It regressed the
repeated scan benchmark badly:

| Benchmark | BTree lazy index | Versioned ART lazy index |
| --- | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 1,008,016 ns/op | 2,689,039 ns/op |

The non-versioned tree avoids copy-on-write machinery that the transaction-local
cache does not need:

| Benchmark | BTree lazy index | Non-versioned ART lazy index | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 1,008,016 ns/op | 946,704 ns/op | +6.5% throughput |
| `tx_overlay_single_bound_scan_local_writes` | 572,096 ns/op | 577,248 ns/op | -0.9% throughput |
| `tx_overlay_assert_local_writes` | 569,248 ns/op | 565,920 ns/op | ~0% |
| `tx_commit_functional_local_updates` | 10,771,230 ns/op | 10,650,382 ns/op | ~0% |

Conclusion: ART helps here when used as an adaptive, non-versioned,
transaction-local projected index. This was the first place where the "why not
ART if the radix key is already precomputed?" question produced a clear yes.

### Accepted: Adaptive Primary Overlay Promotion

The earlier full-key ART primary overlay was rejected because it replaced every
per-relation local write set with ART:

```rust
full ART: full tuple key -> { tuple, local change }
```

That result was too broad. Re-testing the idea against the current tree showed
that the all-ART overlay has a real strength and a real weakness:

| Benchmark | BTree primary overlay | All-ART primary overlay |
| --- | ---: | ---: |
| `tx_overlay_assert_local_writes` | 573,312 ns/op | 490,847 ns/op |
| `tx_overlay_bound_scan_local_writes` | 949,487 ns/op | 758,832 ns/op |
| `tx_overlay_single_bound_scan_local_writes` | 581,936 ns/op | 383,264 ns/op |
| `tx_commit_multi_relation_set_writes` | 738,896 ns/op | 781,504 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 506,304 ns/op | 551,040 ns/op |

The large single-relation overlay benefits from the encoded full-tuple key and
ART value iteration. The many-relation commit benchmark regresses because it
creates many small per-relation overlays, where ART setup and traversal are not
recovered.

The accepted version is adaptive:

```rust
RelationWriteOverlay:
    Small(sorted Vec<OverlayEntry>)
    Radix(AdaptiveRadixTree<RadixTupleKey, OverlayEntry>)
```

Each relation starts with a small non-ART representation and promotes to ART
after enough local write entries. The first accepted threshold was 128. Results
against the same BTree baseline:

| Benchmark | BTree primary overlay | Adaptive overlay |
| --- | ---: | ---: |
| `tx_overlay_assert_local_writes` | 573,312 ns/op | 565,952 ns/op |
| `tx_overlay_bound_scan_local_writes` | 949,487 ns/op | 767,024 ns/op |
| `tx_overlay_single_bound_scan_local_writes` | 581,936 ns/op | 388,080 ns/op |
| `tx_prepare_functional_local_updates` | 764,448 ns/op | 727,951 ns/op |
| `tx_commit_functional_local_updates` | 6,218,510 ns/op | 6,225,791 ns/op |
| `tx_commit_multi_relation_set_writes` | 738,896 ns/op | 740,560 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 506,304 ns/op | 507,696 ns/op |

Conclusion: the primary overlay should not be "ART everywhere", but the
precomputed full-tuple radix key is useful once a per-relation write set is large
enough. This is the same shape as the lazy scan index result: avoid index work
for small or one-off local state, then promote when the transaction has proven
there is enough local data to amortize the encoded-key structure.

### Adaptive Overlay Threshold Sweep

The initial adaptive threshold was a guess. A size-specific benchmark surface
was added to make the transition visible:

```text
tx_overlay_sized_assert_local_writes_{32,64,128,256,512}
tx_overlay_sized_bound_scan_local_writes_{32,64,128,256,512}
```

The benchmark asserts N local writes into one relation. The scan variant then
performs 64 bound scans over the same dirty relation. Three thresholds were
measured: 64, 128, and 256. Promotion occurs once the per-relation local write
set grows past the threshold.

Assert-only median latency:

| Local writes | Threshold 64 | Threshold 128 | Threshold 256 |
| ---: | ---: | ---: | ---: |
| 32 | 3,120 ns | 3,136 ns | 3,152 ns |
| 64 | 6,576 ns | 6,832 ns | 6,816 ns |
| 128 | 15,472 ns | 14,160 ns | 14,560 ns |
| 256 | 26,832 ns | 36,736 ns | 30,032 ns |
| 512 | 58,480 ns | 65,744 ns | 74,608 ns |

Repeated bound-scan median latency:

| Local writes | Threshold 64 | Threshold 128 | Threshold 256 |
| ---: | ---: | ---: | ---: |
| 32 | 105,920 ns | 108,848 ns | 108,432 ns |
| 64 | 249,984 ns | 253,104 ns | 253,088 ns |
| 128 | 263,168 ns | 262,800 ns | 263,824 ns |
| 256 | 281,952 ns | 288,656 ns | 282,000 ns |
| 512 | 310,128 ns | 322,496 ns | 337,456 ns |

Threshold 64 is the best broad compromise in this sweep. It keeps the smallest
write sets in the `BTreeMap` path, promotes before the 256/512 write cases, and
wins most repeated-scan cases. The only clear loss is the 128-entry assert-only
case, where threshold 128 is about 9% faster. That is acceptable because the
larger write sets benefit more, and repeated dirty scans do not show the same
128-entry loss.

The full benchmark suite with threshold 64 and batched relation mutation gave:

| Benchmark | Median |
| --- | ---: |
| `tx_overlay_assert_local_writes` | 562,096 ns/op |
| `tx_overlay_bound_scan_local_writes` | 769,040 ns/op |
| `tx_overlay_single_bound_scan_local_writes` | 399,504 ns/op |
| `tx_commit_functional_local_updates` | 6,235,215 ns/op |
| `tx_commit_multi_relation_set_writes` | 704,944 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 485,008 ns/op |

Conclusion: keep the adaptive primary overlay threshold at 64 for now. Future
tuning should use representative transaction traces rather than only synthetic
sizes, but 64 is the best measured point in the current harness.

### Accepted: Small Primary Overlay Uses A Sorted Vector

The adaptive primary overlay still used `BTreeMap<Tuple, LocalChange>` for
small per-relation write sets. That is reasonable for deterministic ordering and
deduplication, but it pays tree-node overhead before the write set is large
enough to promote to ART.

Change:

```rust
OverlayChanges:
    Small(Vec<OverlayEntry>) // kept sorted by tuple
    Radix(AdaptiveRadixTree<RadixTupleKey, OverlayEntry>)
```

Small inserts use binary search to update an existing tuple or insert a new
entry in tuple order. This preserves deterministic commit emission without
building an ordered tree for tiny overlays. The ART path remains the promoted
representation, so this change does not precompute radix keys where they are not
used by an ART access path.

Size-specific result against the `BTreeMap` small-overlay baseline:

| Benchmark | `BTreeMap` small overlay | Sorted `Vec` small overlay | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_sized_assert_local_writes_32` | 3,072 ns/op | 2,592 ns/op | +18.5% throughput |
| `tx_overlay_sized_assert_local_writes_64` | 6,512 ns/op | 5,392 ns/op | +20.8% throughput |
| `tx_overlay_sized_assert_local_writes_128` | 15,280 ns/op | 14,464 ns/op | +5.6% throughput |
| `tx_overlay_sized_assert_local_writes_256` | 26,384 ns/op | 28,656 ns/op | -7.9% throughput |
| `tx_overlay_sized_assert_local_writes_512` | 51,024 ns/op | 55,136 ns/op | -7.5% throughput |

Repeated dirty scans moved slightly against the vector in the size-specific
surface:

| Benchmark | `BTreeMap` small overlay | Sorted `Vec` small overlay | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_sized_bound_scan_local_writes_32` | 104,640 ns/op | 106,784 ns/op | -2.0% throughput |
| `tx_overlay_sized_bound_scan_local_writes_64` | 249,024 ns/op | 251,567 ns/op | -1.0% throughput |
| `tx_overlay_sized_bound_scan_local_writes_128` | 260,144 ns/op | 264,640 ns/op | -1.7% throughput |
| `tx_overlay_sized_bound_scan_local_writes_256` | 277,904 ns/op | 280,800 ns/op | -1.0% throughput |
| `tx_overlay_sized_bound_scan_local_writes_512` | 306,288 ns/op | 311,680 ns/op | -1.7% throughput |

The broader 4096-tuple overlay benchmarks still improved:

| Benchmark | Before | After |
| --- | ---: | ---: |
| `tx_overlay_assert_local_writes` | 562,096 ns/op | 507,600 ns/op |
| `tx_overlay_bound_scan_local_writes` | 769,040 ns/op | 759,520 ns/op |

Conclusion: keep the sorted-vector small overlay for now. It makes the common
tiny overlay path cheaper and improves the broad write benchmark. The 256/512
assert-only regression is a signal to revisit the promotion boundary or try an
unsorted vector plus commit-time ordering later, but the current representative
benchmarks still favour the vector.

Rejected follow-up: an unsorted small vector was tried next. The intent was to
avoid maintaining tuple order on every insert and sort only when commit
application needed deterministic tuple order. In practice, replacing binary
search with a linear tuple search made the small write path worse:

| Benchmark | Sorted vector | Unsorted vector | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_sized_assert_local_writes_32` | 2,560 ns/op | 3,120 ns/op | -17.9% throughput |
| `tx_overlay_sized_assert_local_writes_64` | 5,424 ns/op | 9,408 ns/op | -42.3% throughput |
| `tx_overlay_sized_assert_local_writes_128` | 14,464 ns/op | 18,304 ns/op | -21.0% throughput |
| `tx_overlay_sized_assert_local_writes_256` | 25,616 ns/op | 30,752 ns/op | -16.7% throughput |
| `tx_overlay_sized_assert_local_writes_512` | 57,008 ns/op | 55,168 ns/op | ~0% |

The result is a useful negative: for small tuple overlays, avoiding eager order
does not help if deduplication still needs tuple equality scans. Keep the small
vector sorted until a different representation can avoid both insertion shifts
and linear tuple comparisons.

### Accepted: Batched Relation-State Commit Application

Commit application used to call:

```rust
RelationState::insert(tuple)
RelationState::remove(tuple)
```

for every local write. Each call entered `Arc::make_mut(&mut self.tuples)` for
the canonical tuple set. That is correct, but it puts copy-on-write access and
relation-state mutation behind a per-tuple method boundary even when one commit
is applying many ordered changes to the same relation.

Change:

```rust
RelationState::apply_ordered_changes(...)
```

This batches commit application for one relation. It borrows the canonical tuple
set once, applies ordered local changes, updates secondary indexes for tuples
that actually changed, and reports applied semantic changes back to the commit
builder. Stale retract semantics remain the same: a retract only applies if the
tuple existed in the transaction's base snapshot.

Result against the adaptive primary overlay:

| Benchmark | Per-tuple relation mutation | Batched relation mutation |
| --- | ---: | ---: |
| `tx_commit_functional_local_updates` | 6,225,791 ns/op | 6,214,527 ns/op |
| `tx_commit_multi_relation_set_writes` | 740,560 ns/op | 699,696 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 507,696 ns/op | 480,256 ns/op |

The scan-only benchmarks moved within normal run noise, as expected:

| Benchmark | Adaptive overlay | Batched relation mutation |
| --- | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 767,024 ns/op | 785,232 ns/op |
| `tx_overlay_single_bound_scan_local_writes` | 388,080 ns/op | 395,056 ns/op |

Conclusion: batching relation-state mutation is worth keeping. It is a small
neutral change for large single-relation functional replacement, and a measured
5-6% commit-path improvement for the many-relation write shape. It also makes
the commit path line up better with future tuple-id or logical-pointer storage,
where relation-local mutation should be an explicit batch operation rather than
an accidental loop of scalar inserts/removes.

### Write-Set Key Summaries

Conflict validation and commit bloom construction repeatedly project tuple keys.
For functional relations, the write overlay could memoize the projected conflict
key per entry at write time or lazily during validation.

The first accepted version reuses the transaction-local functional visibility
map as the touched-key summary during conflict validation and commit bloom
construction. This avoids projecting both the retract and assert tuple for every
functional replacement when the conflict granularity is already the functional
key.

Result after lazy local scan indexes:

| Benchmark | Before key-summary reuse | Key-summary reuse |
| --- | ---: | ---: |
| `tx_commit_functional_local_updates` | 10,809,630 ns/op | 10,771,230 ns/op |

This is a small improvement and within normal benchmark noise, but it also
removes duplicate functional bloom keys from the write-set summary. It is kept
as a low-risk cleanup because it follows the relation's conflict granularity.

Rejected follow-up: applying functional commits directly from the final
visibility map instead of replaying local writes was tried and reverted. It
changed the commit application path and regressed the commit benchmark to
roughly 11,004,926 ns/op. The likely cost is extra indexed lookup and mutation
work while applying the final per-key state.

### Accepted: Commit Membership Result Reuse

`build_next_snapshot` was doing an explicit membership check before calling
`RelationState::insert` or `RelationState::remove`. Those relation-state
methods then did their own membership check internally while mutating the
canonical set and indexes.

Change:

```rust
RelationState::insert(tuple) -> bool
RelationState::remove(tuple) -> bool
```

The commit builder now uses that boolean to decide whether a semantic
`FactChange` should be emitted. Stale retraction semantics are preserved:
retracting a tuple that was absent in the transaction's base snapshot must not
delete a concurrent assertion in the current snapshot.

Result:

| Benchmark | Before membership result reuse | After membership result reuse |
| --- | ---: | ---: |
| `tx_commit_functional_local_updates` | 10,738,334 ns/op | 10,650,382 ns/op |

Conclusion: this is a modest commit-path improvement, but it removes duplicate
canonical set probes and makes relation-state mutation report the information
the commit builder actually needs.

### Accepted: Ordered Commit Change Emission

`build_next_snapshot` used to collect semantic fact changes in hash-map
iteration order, then sort the whole `Vec<FactChange>` by relation, tuple, and
change kind. That makes deterministic commits, but the sort compares full tuple
values after the changes have already been emitted from tuple-ordered per-
relation overlays.

Change:

```rust
relation ids: sorted once
per relation writes: canonical tuple-key order
```

This emits fact changes in canonical relation/tuple order directly and removes
the whole-vector `FactChange` sort. A single-relation functional update
benchmark did not prove a win; its result stayed in normal noise around
10.65-10.68 ms. A new many-relation commit benchmark was added to exercise the
shape where the removed sort has more work to do.

Result:

| Benchmark | Ordered emission | Whole-vector fact-change sort |
| --- | ---: | ---: |
| `tx_commit_multi_relation_set_writes` | 736,256 ns/op | 757,344 ns/op |

Conclusion: ordered emission is a small but measurable improvement for commits
that touch many relations, and neutral on the current single-relation functional
benchmark. It also makes commit construction express the desired ordering
directly instead of repairing order afterwards.

### Accepted: Full-Tuple Bloom Keys Avoid Position Vectors

`write_bloom` builds the modified-key summary used by commit conflict checks.
For functional relations, it projects the relation's declared conflict key. For
set and event-append relations, the conflict key is the whole tuple. The old
set/event path still built a fresh vector of every tuple position for every
tuple:

```rust
let positions = (0..tuple.arity() as u16).collect::<Vec<_>>();
tuple.project(&positions)
```

That was unnecessary allocation before cloning the tuple values into the bloom
key. `Tuple::full_key()` now clones the tuple values directly into a `TupleKey`
without allocating a separate positions vector.

Result:

| Benchmark | Per-tuple positions vector | Direct full tuple key | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_multi_relation_set_writes` | 496,880 ns/op | 468,671 ns/op | +6.0% throughput |
| `tx_commit_multi_relation_unindexed_set_writes` | 485,408 ns/op | 471,215 ns/op | +3.0% throughput |
| `tx_commit_large_set_replacements` | 1,217,807 ns/op | 1,186,127 ns/op | +2.7% throughput |

Conclusion: keep this. It is a simple write-path cleanup that follows the
relation conflict model: set and event-append relations use the whole tuple as
their modified key, so constructing a projection descriptor first is wasted
work.

### Accepted: Selective Non-Stale Conflict Validation Skip

`Transaction::commit` historically ran conflict validation even when the
transaction's base snapshot was still the current snapshot. For set and
event-append relations, a non-stale transaction has no concurrent committed
change to validate against, so that work is unnecessary. The commit path now
skips conflict validation when both conditions hold:

```text
current snapshot version == transaction base version
transaction does not touch functional relations
```

Functional relations deliberately still validate even when non-stale. A blanket
non-stale skip looked attractive, but it regressed functional commit benchmarks,
likely because the validation pass warms relation/key paths used by snapshot
construction. The selective form keeps the obvious set/event fast path without
changing functional behaviour.

Clean runs after the selective skip:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_multi_relation_set_writes` | 461,807 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 476,160 ns/op |
| `tx_commit_large_set_replacements` | 1,111,007 ns/op |
| `tx_commit_large_functional_updates` | 8,112,314 ns/op |

The many-relation set numbers are within the usual variance around the direct
full-key bloom-key optimization. The large set result is a clearer improvement
over the direct full-key baseline of 1,186,127 ns/op. The important design
point is that validation can be skipped only when the transaction is non-stale
and its touched relation policies make that skip semantically trivial.

### Rejected: Carry Full Radix Keys In Promoted Overlay Entries

Once transaction overlays promote to ART, each write already has a full
`RadixTupleKey` for the ART key. I tried also storing a copy of that full key
inside the overlay value and passing it through commit application, so
`TupleStore` could reuse it for base-membership checks and canonical
insert/remove instead of re-encoding the tuple.

Shape:

```rust
OverlayEntry {
    full_key: Option<RadixTupleKey>,
    tuple: Tuple,
    change: LocalChange,
}
```

Only promoted ART overlays carried the key; small sorted-vector overlays still
kept no key.

Result:

| Benchmark | Before carried key | Carried key | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_multi_relation_set_writes` | 461,807 ns/op | 473,572 ns/op | -2.5% throughput |
| `tx_commit_multi_relation_unindexed_set_writes` | 476,160 ns/op | 471,364 ns/op | +1.0% throughput |
| `tx_commit_large_set_replacements` | 1,111,007 ns/op | 1,112,249 ns/op | ~0% |
| `tx_commit_large_functional_updates` | 8,112,314 ns/op | 8,114,463 ns/op | ~0% |

The PMU output also showed more cache pressure in the many-relation set case:
cache misses/op rose from roughly 13.5k to 20.9k for the indexed benchmark.

Conclusion: reject this. Reusing a full radix key is conceptually appealing, but
duplicating an 80-byte-ish `OverflowKey` in every promoted overlay value adds
memory traffic and does not reliably beat re-encoding the small tuples used in
these commit paths. If rart grows a cheap key-view commit traversal that can
reuse the tree key without storing another copy, this should be revisited.

### Indexed Versus Unindexed Commit Cost

A paired many-relation benchmark was added to quantify secondary index
maintenance cost. This corrected an earlier benchmark mistake: `RelationMetadata::new`
installs a default full-tuple index, so the first "unindexed" measurement was not
actually unindexed, and one indexed case accidentally installed a duplicate
full-tuple index.

```text
tx_commit_multi_relation_set_writes
tx_commit_multi_relation_unindexed_set_writes
```

Both benchmarks assert 2048 set tuples across 64 relations. The indexed variant
uses the default full-tuple ART index per relation. The unindexed variant uses
the same relation and tuple shape with the relation metadata index list cleared.

Corrected result:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_multi_relation_set_writes` | 737,680 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 509,280 ns/op |

Conclusion: for this write-heavy shape, maintaining one secondary ART index adds
roughly 230 us over 2048 writes, about 45% of the unindexed commit cost.
That does not mean the index is wrong; it means index maintenance is one of the
remaining costs worth treating as first-class in the storage design.

### Rejected: Commit Change Vector Preallocation

After ordered emission, `build_next_snapshot` still built the semantic
`FactChange` vector with `Vec::new()`. The write overlays already know their
sizes, so commit construction can reserve enough capacity before emitting
changes.

The first measurement, before the benchmark correction above, looked like a
small win. Re-running under the corrected harness did not support keeping it:

| Benchmark | `Vec::new()` | `Vec::with_capacity(...)` |
| --- | ---: | ---: |
| `tx_commit_functional_local_updates` | 6,202,078 ns/op | 6,209,087 ns/op |
| `tx_commit_multi_relation_set_writes` | 736,256 ns/op | 737,680 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 503,104 ns/op | 509,280 ns/op |

Conclusion: explicit preallocation is not worth keeping. The corrected numbers
are all neutral-to-worse, so commit construction stays with `Vec::new()`.

### Accepted: Full-Tuple Index Fast Path

The default relation index covers every tuple position. For such indexes, a
projected index key uniquely identifies one relation tuple because relation
state is already a set of full tuples. The generic `TupleIndex` maintenance path
still used `update_k` and bucket mutation for every insert/remove, because
non-unique projected indexes need a bucket per key.

Change:

```rust
TupleIndex:
    unique_keys = spec positions cover all relation positions
```

When `unique_keys` is true, `TupleIndex::insert` uses `insert_k` with a one-tuple
bucket and `TupleIndex::remove` uses `delete_k`. Non-unique indexes keep the
generic bucket path.

Result on the corrected benchmark harness:

| Benchmark | Generic bucket path | Full-tuple fast path |
| --- | ---: | ---: |
| `tx_commit_functional_local_updates` | 10,574,846 ns/op | 6,209,087 ns/op |
| `tx_commit_multi_relation_set_writes` | 737,536 ns/op | 737,680 ns/op |

Conclusion: the fast path is a major win for functional replacements, where each
logical update retracts and asserts through the default full-tuple index. It is
neutral on the many-relation append-like set-write benchmark. This is worth
keeping because the precondition is structural and cheap to detect.

### Accepted: Adaptive Canonical Tuple Store

The committed relation state still kept canonical membership in:

```rust
Arc<BTreeSet<Tuple>>
```

Secondary indexes had already moved to ART, but every committed mutation still
called `Arc::make_mut` on the canonical tuple set after cloning the snapshot.
For large relations, this made the canonical set a likely MVCC bottleneck: the
secondary indexes were copy-on-write, while the truth set copied and mutated a
tree of full tuples.

I added two large-relation commit benchmarks before changing the storage layout.
Both seed a relation with 65,536 committed tuples and then commit 1,024 logical
updates per sample:

- `tx_commit_large_functional_updates`: replaces 1,024 rows in a functional
  relation keyed by the first two positions.
- `tx_commit_large_set_replacements`: retracts 1,024 old set tuples and asserts
  1,024 replacement tuples.

Baseline with `Arc<BTreeSet<Tuple>>`:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_large_functional_updates` | 10,848,974 ns/op |
| `tx_commit_large_set_replacements` | 2,759,083 ns/op |

The first replacement used ART for every relation. That improved the new
large-relation benchmarks, but the full benchmark suite exposed a regression in
small/many-relation commits: small relations lost the old cheap snapshot clone
because their `BTreeSet` was no longer behind an `Arc`.

Final change:

```rust
TupleStore:
    Small(Arc<BTreeSet<Tuple>>)
    Radix {
        entries: VersionedAdaptiveRadixTree<RadixTupleKey, Tuple>,
        len: usize,
    }
```

`RelationState` now uses `TupleStore` as canonical committed membership.
Small relations retain the old cheap snapshot clone and `Arc::make_mut`
mutation path. Larger relations promote to a versioned ART canonical store,
where membership, insertion, and removal use the same full-tuple radix key as
the default full-tuple index. Secondary indexes remain separate projected access
paths.

Clean before/after result with promotion disabled for the baseline and enabled
for the final run:

| Benchmark | `Arc<BTreeSet<Tuple>>` | `TupleStore` | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_large_functional_updates` | 10,848,974 ns/op | 8,759,570 ns/op | +23.2% throughput |
| `tx_commit_large_set_replacements` | 2,759,083 ns/op | 1,717,933 ns/op | +62.6% throughput |

PMU data moved in the expected direction: large set replacement cache misses
dropped from about 214k/op to 41k/op, and large functional updates dropped
cache misses from about 485k/op to 252k/op. Backend stalls were still high,
which says this is not the final storage design, but the canonical tuple store
is no longer cloning a full `BTreeSet` for large relation updates.

Conclusion: this answers the "if we already compute a radix key, why not use
ART?" question for canonical committed state. ART is worth using for the
canonical tuple store, not just secondary indexes, once the relation is large
enough. Small relations still want the `Arc<BTreeSet<_>>` path.

### Accepted: TupleStore Owns The Natural Full-Tuple Access Path

After `TupleStore` gained a radix representation, the default full-tuple
`TupleIndex` became duplicate work for large relations:

```text
TupleStore:
    full tuple key -> tuple

default TupleIndex:
    full tuple key -> tuple bucket
```

That meant every committed tuple mutation could maintain two ART structures with
the same natural `[0, 1, ...]` ordering. The fix is to make the canonical
`TupleStore` responsible for natural full-tuple membership and natural leading
prefix scans. Explicit secondary indexes are still used for projected or
permuted access paths, such as `[1, 0]`.

Commit-path result against the adaptive `TupleStore` run that still maintained
the duplicate default full-tuple index:

| Benchmark | Duplicate default index | TupleStore natural path | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_functional_local_updates` | 6,236,645 ns/op | 5,853,993 ns/op | +6.5% throughput |
| `tx_commit_multi_relation_set_writes` | 722,751 ns/op | 498,527 ns/op | +45.0% throughput |
| `tx_commit_multi_relation_unindexed_set_writes` | 500,127 ns/op | 497,520 ns/op | ~0% |
| `tx_commit_large_functional_updates` | 8,737,121 ns/op | 8,132,774 ns/op | +7.4% throughput |
| `tx_commit_large_set_replacements` | 1,682,909 ns/op | 1,192,847 ns/op | +41.1% throughput |

The indexed and unindexed many-relation set-write cases now converge, which is
the desired result: the default natural access path no longer acts like an extra
secondary index on the write path.

Scan benchmarks stayed flat after moving natural prefix scans to `TupleStore`.
The benchmark session reported one small improvement and one small regression:

| Benchmark family | Result |
| --- | ---: |
| `tx_overlay_bound_scan_local_writes` | ~0% |
| `tx_overlay_single_bound_scan_local_writes` | -1.2% |
| `tx_overlay_sized_bound_scan_local_writes_32` | +1.7% |
| remaining sized bound scans | ~0% |

Conclusion: this is the clean answer to the radix-key question. If the natural
full-tuple key is already the canonical storage key, use ART there and stop
maintaining a second full-tuple ART index. Add secondary indexes only when they
provide a different key order or projection.

### Accepted: Natural TupleStore Equality Joins

Filtering out the duplicate default full-tuple `TupleIndex` exposed one missing
piece: equality joins over natural full-tuple keys should still be able to use
the canonical `TupleStore` access path directly.

The affected shape is common in relational object modelling:

```text
ActiveItem(?x) join VisibleItem(?x)
Portable(?x) join Selected(?x)
```

After the default index was removed, this kind of promoted unary-set join could
fall back to scanning both relations and building projected join indexes for the
query. A benchmark was added for two promoted unary relations with 16,384 tuples
each and 8,192 overlapping identities:

```text
natural_unary_set_intersection
```

Baseline with the natural join path temporarily disabled:

| Benchmark | Median |
| --- | ---: |
| `natural_unary_set_intersection` | 46,313,716 ns/op |

Change:

```rust
RelationState::join_eq:
    if both join position lists are the natural full tuple:
        intersect TupleStore values directly
```

`TupleStore` now handles:

- small/small intersection with `BTreeSet::intersection`;
- small/radix and radix/small by probing the radix store with the small side;
- radix/radix with ART `intersect_values_with`.

Result:

| Benchmark | Scan and projected join indexes | Natural TupleStore join | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_set_intersection` | 46,313,716 ns/op | 1,639,870 ns/op | +2720.0% throughput |

PMU moved in the expected direction: backend stalls dropped from about 63% to
27%, and cache misses dropped from roughly 255k/op to 68k/op.

The existing mixed projected-index query benchmark stayed effectively flat:

| Benchmark | Before | After |
| --- | ---: | ---: |
| `visible_items_query_3way_anti` | 3,693,100 ns/op | 3,708,013 ns/op |

Conclusion: natural full-tuple equality joins belong on `TupleStore`, not on a
duplicate default `TupleIndex`. This preserves the write-path win from removing
the duplicate index while recovering the direct ART intersection path for
identity-set joins.

### Accepted: Vector Accumulation For Join Results

Several join paths accumulated output rows incrementally in `BTreeSet<Tuple>`:

```rust
let mut out = BTreeSet::new();
for match in matches {
    out.insert(joined_tuple);
}
out.into_iter().collect()
```

That preserves set semantics and deterministic ordering, but it pays tree
navigation and full tuple comparisons on every produced row. The changed shape
is:

```rust
let mut out = Vec::new();
for match in matches {
    out.push(joined_tuple);
}
out.sort();
out.dedup();
out
```

This still returns canonical ordered set rows, but it performs sequential writes
during join production and does the ordering in one batch.

Result on the natural full-tuple equality join benchmark:

| Benchmark | `BTreeSet` accumulation | `Vec` + sort/dedup | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_set_intersection` | 1,607,336 ns/op | 879,316 ns/op | +82.8% throughput |

The broader join/rule suite stayed in the same range or improved slightly:

| Benchmark | Median after change |
| --- | ---: |
| `visible_items_query_3way_anti` | 3,117,406 ns/op |
| `natural_unary_semi_intersection` | 1,032,260 ns/op |
| `visible_items_rule_join_order` | 8,802,998 ns/op |

The PMU direction matches the intended trade-off on
`natural_unary_set_intersection`: cache misses/op dropped from roughly 66.8k to
24.9k, branch misses/op dropped from roughly 26k to 820, and backend stalls/op
dropped from roughly 1.96M cycles to 1.35M cycles.

Conclusion: keep vector accumulation for join outputs. It is a better fit for
the common case where joins produce many unique rows and then need deterministic
set output. If a future workload produces huge duplicate join fanout, that
should be measured separately; the current relation inputs are already sets, so
duplicate join rows should not be common.

Rejected follow-up: I tried the same vector sort/dedup shape for the semi/anti
join membership set:

```rust
matching left rows -> Vec<Tuple> -> sort/dedup -> binary_search
```

That did not improve the representative semi-join benchmark:

| Benchmark | `BTreeSet` membership | `Vec` + binary search | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_semi_intersection` | 1,038,580 ns/op | 1,043,811 ns/op | ~0% |

Conclusion: keep the existing `BTreeSet<Tuple>` membership set for this path.
The output-vector win applies to materializing joined rows, not necessarily to
building a side membership structure that is then probed once per left row.

### Accepted: Bulk Temporary Projected Index Construction

Generic query joins build temporary projected indexes when the planner cannot
use a direct relation index or an indexed probe. The old builder inserted rows
one at a time:

```rust
for row in rows {
    projected key = encode(row positions)
    ART update projected key bucket
    bucket insert row
}
```

That is correct, but it pays one ART update per row and one bucket insertion
per row. A benchmark was added to isolate this fallback path:

```text
temporary_projected_low_cardinality_join
```

It joins two 16,384-row inputs through non-scan wrapper plans, forcing
`ProjectedTupleIndex::from_rows` on both sides. The join key has 4096 groups
with 4 rows per group on each side, so the query still has meaningful bucket
fanout without making the output dominate the whole measurement.

Change:

```rust
rows -> Vec<(RadixTupleKey, Tuple)>
if not already ordered:
    sort by projected radix key, then tuple
group adjacent equal keys
insert one TupleBucket per projected key into the ART
```

The builder preserves the fast path for already ordered inputs, which matters
because many relation scans already return canonical tuple order and common
projected keys are monotonic in that order. A follow-up removed an unnecessary
tuple clone from that ordered-input check by comparing against the previous
collected row in the keyed batch.

Focused result:

| Benchmark | Scalar inserts | Bulk build | Change |
| --- | ---: | ---: | ---: |
| `temporary_projected_low_cardinality_join` | 53,033,550 ns/op | 10,745,401 ns/op | +393.5% throughput |

The first accepted bulk builder still used
`VersionedAdaptiveRadixTree<RadixTupleKey, TupleBucket>`. That tree is right
for committed snapshot indexes, but this projected index is a query-local
scratch structure and never needs copy-on-write snapshot semantics. Switching
only `ProjectedTupleIndex` to the non-versioned ART improved the same focused
benchmark again:

| Benchmark | Versioned ART bulk build | Non-versioned ART bulk build | Change |
| --- | ---: | ---: | ---: |
| `temporary_projected_low_cardinality_join` | 10,745,401 ns/op | 9,202,839 ns/op | +16.8% throughput |

The broader query/rule run stayed healthy:

| Benchmark | Median after change |
| --- | ---: |
| `visible_items_query_3way_anti` | 597,326 ns/op |
| `natural_unary_set_intersection` | 896,350 ns/op |
| `natural_unary_semi_intersection` | 1,042,430 ns/op |
| `natural_unary_union` | 671,854 ns/op |
| `natural_unary_difference` | 619,023 ns/op |
| `temporary_projected_low_cardinality_join` | 9,304,167 ns/op |
| `visible_items_rule_join_order` | 8,771,112 ns/op |

Conclusion: temporary projected indexes should be built in batches. This is
another case where the right answer to "if the radix key is already computed,
why not use ART?" is "yes, but feed the ART in an index-shaped way." Construct
one key per row, arrange rows by key once, and insert one bucket per key instead
of treating index construction as a stream of independent row mutations. Also
use non-versioned ART for scratch query indexes; save versioned ART for
snapshot-owned structures that actually need cheap clones.

Rejected follow-up: reserving join output capacity per intersecting bucket pair
looked plausible after the index builder got faster:

```rust
out.reserve(left_bucket.len() * right_bucket.len())
```

That regressed the focused projected-join benchmark:

| Benchmark | Before reserve | Per-bucket reserve | Change |
| --- | ---: | ---: | ---: |
| `temporary_projected_low_cardinality_join` | 9,183,931 ns/op | 9,304,939 ns/op | -1.4% throughput |

Conclusion: do not reserve per bucket. The extra reserve calls and bucket
length reads cost more than they save for this fanout shape. If output
preallocation is revisited, it should compute a single capacity in a separate
planner-visible estimate, not as a side effect inside the hot intersection
loop.

### Accepted: Batch Set Operators

`QueryPlan::Union` and `QueryPlan::Difference` also used incremental
`BTreeSet<Tuple>` structures:

```rust
Union:
    left rows -> BTreeSet
    extend with right rows

Difference:
    left rows -> BTreeSet
    remove each right row
```

Focused benchmarks were added for large unary set union and difference using
the same promoted relation shape as the natural join benchmarks:

```text
natural_unary_union
natural_unary_difference
```

The accepted shape is:

```rust
Union:
    append both row vectors
    sort/dedup once

Difference:
    sort/dedup both row vectors
    linear merge difference
```

Result:

| Benchmark | Incremental `BTreeSet` | Batch vector path | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_union` | 1,828,997 ns/op | 821,874 ns/op | +122.5% throughput |
| `natural_unary_difference` | 1,396,084 ns/op | 633,297 ns/op | +120.4% throughput |

The broader join/rule run after the change:

| Benchmark | Median |
| --- | ---: |
| `visible_items_query_3way_anti` | 3,048,998 ns/op |
| `natural_unary_set_intersection` | 885,730 ns/op |
| `natural_unary_union` | 814,706 ns/op |
| `natural_unary_difference` | 633,297 ns/op |
| `visible_items_rule_join_order` | 8,722,242 ns/op |

Rejected intermediate: for difference, I first tried sorting only the right side
and using binary search for each left row, then sorting/deduplicating the
output. That regressed:

| Benchmark | Incremental `BTreeSet` | Right-sort + binary search | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_difference` | 1,396,084 ns/op | 1,669,107 ns/op | -16.4% throughput |

Conclusion: set operators should also favour batch vector work, but only when
the algorithm is actually set-shaped. Union is append plus one canonicalization;
difference wants a linear merge over canonicalized inputs, not per-row binary
search plus a second output sort.

### Rejected: Compact Local Prefix Shapes

Transaction-local dirty scans maintain lazy projected scan indexes keyed by the
bound prefix positions. The existing cache key is `Vec<u16>`. I tried replacing
that with a compact copyable shape:

```rust
LocalPrefixShape {
    index: u16, // relation metadata index ordinal
    count: u16, // bound leading positions
}
```

The intent was to avoid allocating and copying a small `Vec<u16>` on repeated
dirty overlay scans.

Result:

| Benchmark | `Vec<u16>` key | Compact shape key | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 763,121 ns/op | 768,289 ns/op | ~0% |

Conclusion: reject this. The allocation is not the bottleneck in the current
dirty-overlay repeated scan workload; the remaining time is dominated by tuple
visitation, radix prefix probing, and transaction overlay mechanics. Keep the
clearer `Vec<u16>` key until a profile shows this cache key matters.

### Accepted: Small-Right Semi/Anti Join Membership Path

The `visible_items_query_3way_anti` benchmark exposed a different set-oriented
access pattern. After joining visible rooms to located items and intersecting
with portable items, it anti-joins against `HiddenFrom(item, actor)`. For one
actor, the right side is tiny, while the left side contains many candidate
items.

The fallback semi/anti join path used to build projected indexes for both
sides:

```rust
left rows -> ProjectedTupleIndex
right rows -> ProjectedTupleIndex
intersection -> matching left rows
```

That is good for two comparable row sets, but wasteful for a small right side.
For membership-style semi/anti joins, a small projected right-key set is enough:

```rust
right rows -> BTreeSet<TupleKey>
left rows -> filter by projected-key membership
```

The accepted change uses this path when:

```text
right_rows.len() * 4 < left_rows.len()
```

Result:

| Benchmark | Projected indexes on both sides | Small-right key set | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,714,381 ns/op | 3,199,261 ns/op | +16.1% throughput |

A full relation join/rule run after the change stayed healthy:

| Benchmark | Median |
| --- | ---: |
| `visible_items_query_3way_anti` | 3,226,718 ns/op |
| `natural_unary_set_intersection` | 1,615,070 ns/op |
| `visible_items_rule_join_order` | 8,671,209 ns/op |

Conclusion: not every join needs a full projected index on both sides. For
semi/anti membership tests with a small right side, a small right-key set avoids
building an unnecessary left-side projected index and cuts instruction count on
the benchmarked query.

Rejected follow-up: replacing the temporary `TupleKey(Vec<Value>)` set with a
`RadixTupleKey` set was measured and reverted. It was effectively neutral:

| Benchmark | `TupleKey` set | `RadixTupleKey` set | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,210,061 ns/op | 3,201,550 ns/op | ~0% |

Rejected follow-up: replacing `BTreeSet<TupleKey>` with `HashSet<TupleKey>` was
also measured and reverted:

| Benchmark | `BTreeSet<TupleKey>` | `HashSet<TupleKey>` | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,181,070 ns/op | 3,198,525 ns/op | ~0% |

Rejected follow-up: replacing `BTreeSet<TupleKey>` with a sorted
`Vec<TupleKey>` plus binary search was also measured and reverted:

| Benchmark | `BTreeSet<TupleKey>` | Sorted `Vec<TupleKey>` | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,046,165 ns/op | 3,074,596 ns/op | -1.0% throughput |

These results are too small to justify swapping the simpler deterministic
projection set out here. The radix representation remains useful where it
avoids full tuple comparison or feeds ART prefix/intersection paths, but this
small temporary membership set does not show enough benefit.

Rejected follow-up: a tiny-right linear membership path was also measured and
reverted. It avoided projecting each left row into a `TupleKey`, but replaced
that with repeated positional comparisons against every right row. On the
benchmarked anti-join, it was neutral-to-worse:

| Benchmark | `TupleKey` set | Linear tiny-right scan | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,234,397 ns/op | 3,236,270 ns/op | ~0% |

The key-set path remains the clearer implementation and the better measured
choice.

### Rejected: Direct Join Reuse For Scan-Scan Semi Joins

`JoinEq` has a direct scan-scan relation hook that can use relation indexes or
the natural `TupleStore` join path without materializing both scans first. I
tried reusing that hook for scan-scan `SemiJoin`: run the direct join, project
the joined rows back down to the left relation arity, and deduplicate.

A benchmark was added for two promoted unary relations with 16,384 tuples each
and 8,192 overlapping identities:

```text
natural_unary_semi_intersection
```

The existing path scans the left relation and probes the right relation using
the exact natural index for each left row. That beat the direct-join reuse
variant:

| Benchmark | Per-left exact probe | Direct join then project left | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_semi_intersection` | 1,235,295 ns/op | 1,676,767 ns/op | -26.1% throughput |

Conclusion: keep the probe path for scan-scan semi joins. Direct relation joins
are excellent when the caller needs joined rows, but semi joins only need
left-row membership. Producing joined rows and then projecting them away is
wasted work in this shape.

### Accepted: Semi/Anti Existence Probes Use Visits

The scan-scan semi join path was already probing the right relation one left
row at a time. However, each probe used:

```rust
reader.scan_relation(right_relation, &probe_bindings)?.is_empty()
```

That materialized a `Vec<Tuple>` for a question that only needs one bit:
whether at least one matching right tuple exists. The change is to route this
through `visit_relation` and stop after the first tuple:

```rust
reader.visit_relation(relation, bindings, &mut |_| Stop)
```

Result:

| Benchmark | Scan and test empty | Visit and stop | Change |
| --- | ---: | ---: | ---: |
| `natural_unary_semi_intersection` | 1,247,583 ns/op | 1,023,407 ns/op | +21.9% throughput |

A related mixed query remained in the same range as the previous accepted
small-right semi/anti path:

| Benchmark | Median |
| --- | ---: |
| `visible_items_query_3way_anti` | 3,192,653 ns/op |

Conclusion: semi/anti joins should not materialize right-side probe rows when
they only need existence. This keeps the per-left exact-probe strategy that beat
direct join reuse, but removes avoidable allocation and row collection from the
inner loop.

### Rejected: Fully-Bound TupleStore Lookup Fast Path

After adding natural `TupleStore` joins, I tried a narrower exact-scan shortcut:
when every position in a scan binding was bound, `RelationState` would directly
probe the canonical `TupleStore` instead of going through the prefix scan path.
The intuition was that exact membership checks should be cheaper than prefix
iteration, especially for negated atoms and repeated rule probes.

The measurement did not support keeping it:

| Benchmark | Before exact lookup shortcut | Exact lookup shortcut | Change |
| --- | ---: | ---: | ---: |
| `visible_items_query_3way_anti` | 3,699,629 ns/op | 3,684,861 ns/op | ~0% |
| `natural_unary_set_intersection` | 1,637,183 ns/op | 1,640,462 ns/op | ~0% |
| `visible_items_rule_join_order` | 8,716,456 ns/op | 9,063,608 ns/op | -3.8% throughput |

The likely cause is that the shortcut added another branch and, for small
relations, had to construct a temporary full tuple key for the `BTreeSet` probe.
The existing prefix path was already good enough for these exact probes.

Conclusion: reject this for now. Exact lookup may still be useful in a later
tuple-id storage design, but the current `TupleStore` implementation should not
special-case fully bound scans.

### Accepted: Production Low-Cardinality Index Benchmark And Bucket Append Fast Path

The synthetic index benchmark compares old B-tree-shaped indexes to ART-shaped
indexes, but it does not exercise the production `RelationState` path. I added
a production-facing benchmark that builds two 16,384-tuple relations:

- one relation with a secondary index on position `0`;
- one relation with only the canonical tuple store.

The query binds position `0`, returning one 128-tuple group. This compares a
secondary projected-key ART that lands on one `TupleBucket::Many` against the
canonical tuple-store ART prefix path.

Baseline:

| Benchmark | Median |
| --- | ---: |
| `production_low_cardinality_indexed_scan` | 1,876.80 ns/op |
| `production_low_cardinality_unindexed_scan` | 2,302.40 ns/op |
| `production_low_cardinality_indexed_visit` | 896.00 ns/op |
| `production_low_cardinality_unindexed_visit` | 963.20 ns/op |
| `production_low_cardinality_indexed_build` | 19,799,785 ns/op |
| `production_low_cardinality_unindexed_build` | 6,511,784 ns/op |

The read-side index helps, especially for materializing `scan`, but the
write-side cost of maintaining a low-cardinality secondary index is large.

The first accepted write-path improvement is a narrow append fast path for
`TupleBucket::Many`. Buckets remain sorted vectors, but if the new tuple is
greater than the current last tuple, insertion pushes directly instead of doing
a binary search and indexed insert.

Result:

| Benchmark | Before append fast path | Append fast path | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_build` | 19,799,785 ns/op | 19,194,864 ns/op | +3.2% throughput |

I also tried preallocating `TupleBucket::Many` with capacity 8 when a bucket
first grows from one tuple to many. That did not produce a meaningful
improvement:

| Benchmark | Append fast path | Initial capacity 8 | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_build` | 19,194,864 ns/op | 19,091,175 ns/op | ~0% |

Conclusion: keep the append fast path, but reject the fixed bucket
preallocation. It is not enough faster to justify wasting memory on buckets
that only ever contain two tuples.

### Accepted: Empty-Relation Bulk Assert Application

The indexed build benchmark also showed that the canonical tuple store was
paying avoidable setup cost on initial load. Before this change, committing a
transaction that filled an empty relation still applied every tuple through the
ordinary per-tuple insert path:

```text
empty TupleStore -> Small(BTreeSet) inserts -> promote to ART -> continue inserts
```

For an ordered all-assert transaction into an empty relation, the transaction
can instead build the committed tuple store directly from the sorted overlay
batch. Secondary indexes are still maintained, but the canonical tuple store no
longer churns through the small-relation B-tree state first.

Result:

| Benchmark | Append fast path | Empty-relation bulk assert | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_build` | 19,194,864 ns/op | 18,045,560 ns/op | +6.4% throughput |
| `production_low_cardinality_unindexed_build` | 6,511,784 ns/op | 6,466,426 ns/op | ~0% |

This is most useful when an initial filein or import fills a relation above the
tuple-store ART threshold in one transaction. It preserves the normal path for
mixed assert/retract transactions and for non-empty relations.

### Accepted: Bulk Secondary Index Construction

After the empty-relation canonical tuple-store fast path, low-cardinality
secondary index build was still dominated by per-tuple index maintenance:

```text
for each tuple:
    encode projected key
    update ART slot
    binary-search or append inside TupleBucket
```

For the same empty-relation all-assert path, secondary indexes can be built from
the ordered committed batch instead. The index builder computes projected keys
for all rows, keeps the existing order when projected keys are already
monotonic, sorts only when needed, then inserts one `TupleBucket` per projected
key into the ART.

Result:

| Benchmark | Empty-relation bulk assert | Bulk secondary index build | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_build` | 18,045,560 ns/op | 6,757,256 ns/op | +167.0% throughput |
| `production_low_cardinality_unindexed_build` | 6,466,426 ns/op | 6,045,154 ns/op | +7.0% throughput |

The first post-change run measured indexed build at 6,870,562 ns/op; the rerun
measured 6,757,256 ns/op. Read-side scan and visit measurements stayed in the
same broad range on rerun, so the useful conclusion is the write-side one:
bulk secondary index construction removes most of the low-cardinality index
maintenance penalty for initial relation load.

Follow-up: the bulk secondary-index builder kept an owned copy of the previous
projected radix key only to detect whether keys were already monotonic. Reusing
the last collected keyed row avoids that key clone. The effect is small, but the
benchmark moved in the expected direction:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_build` | 5,664,171.8 ns/op | 5,606,074.8 ns/op | +1.1% throughput |

### Rejected: Commit Bloom Conflict-Validation Skip

Each commit already stores a small bloom filter over modified logical keys. I
tested using that bloom to skip conflict validation for stale transactions whose
write set cannot intersect commits since the transaction's base snapshot.

The focused benchmark creates a stale transaction with 4096 disjoint set writes,
commits one concurrent disjoint set write first, then commits the stale
transaction:

```text
tx_commit_stale_disjoint_set_writes
```

Baseline with the existing stale-validation path:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_stale_disjoint_set_writes` | 2,025,996 ns/op |

Using the existing 2048-bit commit bloom to skip validation did not help:

| Benchmark | Existing validation | Bloom skip | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_stale_disjoint_set_writes` | 2,025,996 ns/op | 2,024,684 ns/op | ~0% |

The likely cause is that the 4096-key transaction bloom saturates the small
2048-bit filter, so disjoint concurrent writes still look like possible
intersections. Increasing the bloom to 16,384 bits also did not move enough to
justify the larger commit footprint:

| Benchmark | Existing validation | 16K-bit bloom skip | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_stale_disjoint_set_writes` | 2,025,996 ns/op | 2,006,812 ns/op | +1.0% throughput |

Conclusion: reject this for now. Bloom-filter conflict skips may become useful
with a different commit-history representation or adaptive filter size, but the
current benchmark says the added runtime path and larger bloom footprint are not
worth keeping.

### Accepted: Remove Unused Commit Bloom Construction

The rejected bloom-skip experiment exposed a simpler issue: the runtime was
still computing a write bloom for every transaction commit even though no
runtime path used it. Persisted commit decoding already reconstructs commits
with an empty bloom, and the bloom is not part of the public relation-kernel
API. I replaced per-commit bloom construction with an empty reserved
`CommitBloom` marker.

Baseline with per-commit write-bloom construction:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,777,335 ns/op |
| `tx_commit_multi_relation_set_writes` | 416,688 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 417,392 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,997,437 ns/op |
| `tx_commit_large_functional_updates` | 8,051,508 ns/op |
| `tx_commit_large_set_replacements` | 1,042,910 ns/op |

After removing bloom construction:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_functional_local_updates` | 5,777,335 ns/op | 5,729,480 ns/op | ~0% |
| `tx_commit_multi_relation_set_writes` | 416,688 ns/op | 278,367 ns/op | +49.7% throughput |
| `tx_commit_multi_relation_unindexed_set_writes` | 417,392 ns/op | 278,976 ns/op | +49.6% throughput |
| `tx_commit_stale_disjoint_set_writes` | 1,997,437 ns/op | 1,720,654 ns/op | +16.1% throughput |
| `tx_commit_large_functional_updates` | 8,051,508 ns/op | 7,998,630 ns/op | ~0% |
| `tx_commit_large_set_replacements` | 1,042,910 ns/op | 895,551 ns/op | +16.5% throughput |

Conclusion: keep this cut. The earlier validation-skip shape was not useful,
but computing a bloom that is not consumed was a clear write-path cost,
especially for set-heavy commits where every tuple had to be projected and
hashed into the filter.

### Accepted: Skip Conflict Validation For Fresh Snapshots

Conflict validation compares the transaction's base snapshot with the current
snapshot. If both versions are the same, there is no concurrent committed state
to validate against. Functional replacement semantics are already handled by
the transaction-local visibility map while writes are recorded, so running
functional conflict validation on a fresh snapshot cannot find a conflict.

The previous commit condition still ran validation for any transaction that
touched a functional relation:

```rust
if current.version() != self.base.version() || self.touches_functional_relations()? {
    self.validate_conflicts(&current)?;
}
```

This was simplified to only validate stale transactions:

```rust
if current.version() != self.base.version() {
    self.validate_conflicts(&current)?;
}
```

Focused measurement was essentially flat:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tx_commit_functional_local_updates` | 5,729,480 ns/op | 5,599,276 ns/op | ~0% |

The full commit filter stayed in the same range:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,599,276 ns/op |
| `tx_commit_multi_relation_set_writes` | 273,952 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 277,424 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,726,415 ns/op |
| `tx_commit_large_functional_updates` | 7,845,785 ns/op |
| `tx_commit_large_set_replacements` | 866,255 ns/op |

Conclusion: keep this as a semantic cleanup rather than a performance win. The
important correctness property is that stale functional conflicts are still
validated. The focused conflict tests covering concurrent key changes and
non-leading functional keys continue to pass.

### Accepted: Vector-Based Dirty Transaction Scans

Dirty transaction scans used to materialize the base extensional result into a
`BTreeSet<Tuple>`, then apply local assertions and retractions to that set:

```rust
base scan rows -> BTreeSet<Tuple>
local assert -> set insert
local retract -> set remove
```

That is general, but expensive for the common dirty-overlay shape where the base
scan is empty and all visible rows come from local assertions. It also repeats
tree-node work after the relation and overlay access paths have already
produced ordered rows.

The accepted path now keeps extensional dirty scans as vectors:

```rust
base scan rows -> Vec<Tuple>
append local assertions
sort + dedup
subtract local retractions if any
```

The public helper that needs a set still wraps the vector result in a
`BTreeSet`, but `Transaction::scan` and scan estimates avoid that set
construction on their direct hot path.

Result:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 771,519 ns/op | 606,160 ns/op | +27.3% throughput |
| `tx_overlay_single_bound_scan_local_writes` | 399,504 ns/op | 381,904 ns/op | +4.6% throughput |

Commit benchmarks after the scan change remained in the same range and in some
cases moved slightly favourably:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,599,276 ns/op |
| `tx_commit_multi_relation_set_writes` | 273,952 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 277,424 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,726,415 ns/op |
| `tx_commit_large_functional_updates` | 7,845,785 ns/op |
| `tx_commit_large_set_replacements` | 866,255 ns/op |

Conclusion: keep the vector dirty-scan path. It is a direct reduction in
temporary tree allocation and tuple-ordering work for local-overlay scans,
without changing the set semantics of the visible result.

### Accepted: Pure-Local Dirty Scan Fast Path

The first vector-based dirty-scan path still sorted and deduplicated rows even
when all visible rows came from transaction-local assertions:

```rust
if !local_asserts.is_empty() {
    visible.extend(local_asserts);
    visible = finish_tuple_rows(visible);
}
```

That is unnecessary when the committed/base scan is empty and the matching
local write set contains no local retractions. The transaction overlay already
deduplicates tuples and visits them in canonical tuple order for both the small
sorted-vector representation and the promoted ART representation.

Change:

```rust
if visible.is_empty() && local_retracts.is_empty() {
    return Ok(local_asserts);
}
```

Result against the vector dirty-scan path:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_bound_scan_local_writes` | 606,160 ns/op | 577,503 ns/op | +5.0% throughput |
| `tx_overlay_single_bound_scan_local_writes` | 381,904 ns/op | 388,144 ns/op | ~0% |

The size-specific repeated dirty scans benefited more because they are almost
entirely pure-local rows:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_sized_bound_scan_local_writes_32` | 51,440 ns/op | 34,272 ns/op | +50.1% throughput |
| `tx_overlay_sized_bound_scan_local_writes_64` | 91,792 ns/op | 59,184 ns/op | +55.1% throughput |
| `tx_overlay_sized_bound_scan_local_writes_128` | 104,192 ns/op | 70,624 ns/op | +47.5% throughput |
| `tx_overlay_sized_bound_scan_local_writes_256` | 120,304 ns/op | 87,088 ns/op | +38.1% throughput |
| `tx_overlay_sized_bound_scan_local_writes_512` | 152,976 ns/op | 121,232 ns/op | +26.2% throughput |

Commit benchmarks remained in the same range:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,618,652 ns/op |
| `tx_commit_multi_relation_set_writes` | 275,776 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 276,736 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,725,327 ns/op |
| `tx_commit_large_functional_updates` | 7,874,395 ns/op |
| `tx_commit_large_set_replacements` | 870,143 ns/op |

Conclusion: keep this fast path. It is narrow, it only applies when there is no
committed/base result to merge, and a regression test now checks that a pure
local scan still returns canonical tuple order after reverse-order assertions
and overlay promotion.

### Accepted: Ordered Difference For Local Retractions

The vector dirty-scan path still had one avoidable cost when a scan combined
committed/base rows with transaction-local retractions. The base relation scan
already returns canonical rows, and the local write overlay also visits matching
retractions in canonical tuple order. The generic difference helper nevertheless
canonicalized both inputs:

```rust
left = sort + dedup
right = sort + dedup
linear difference
```

For this path, the left side is already an ordered set and the right side is
already an ordered set. The new helper makes that precondition explicit:

```rust
difference_ordered_tuple_rows(left, right)
```

The existing `difference_tuple_rows` remains available for callers with
unordered inputs.

A benchmark was added for a committed relation scan with transaction-local
retractions:

```text
tx_overlay_committed_scan_local_retractions
```

It seeds committed rows, starts a transaction, retracts half the rows in one
bound group, then repeatedly scans the same bound group.

Result:

| Benchmark | Before | Focused after | Full scan-suite after |
| --- | ---: | ---: | ---: |
| `tx_overlay_committed_scan_local_retractions` | 780,560 ns/op | 728,607 ns/op | 736,208 ns/op |

The focused run shows about 7.2% better throughput. The broader scan run stayed
in the same improved range. Commit-path benchmarks remained effectively flat:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,633,069 ns/op |
| `tx_commit_multi_relation_set_writes` | 285,360 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 286,096 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,766,112 ns/op |
| `tx_commit_large_functional_updates` | 7,801,660 ns/op |
| `tx_commit_large_set_replacements` | 855,856 ns/op |

Conclusion: keep the ordered-difference helper. It is a small but clean scan
improvement, and it documents the ordering contract between `TupleStore`,
transaction overlays, and vector-based dirty scans. It also reinforces the
general pattern in this work: once access paths produce canonical row vectors,
avoid sending them back through incremental set structures or redundant sorts.

### Accepted: Ordered Union For Local Assertions

The complementary dirty-scan case is a committed/base result plus
transaction-local assertions. The old vector path appended the local assertions
and sorted/deduplicated the combined rows:

```rust
visible.extend(local_asserts)
visible = sort + dedup
```

For ordinary relation scans, both sides are already canonical:

- `TupleStore` scans produce ordered, unique committed rows;
- transaction overlays visit matching local assertions in canonical tuple
  order.

The new helper merges those two ordered sets directly:

```rust
union_ordered_tuple_rows(visible, local_asserts)
```

A benchmark was added for a committed relation scan with transaction-local
assertions:

```text
tx_overlay_committed_scan_local_assertions
```

It seeds committed rows, starts a transaction, asserts another ordered range
for the same bound group, then repeatedly scans the group.

Result:

| Benchmark | Before | Focused after | Full scan-suite after |
| --- | ---: | ---: | ---: |
| `tx_overlay_committed_scan_local_assertions` | 769,902 ns/op | 719,295 ns/op | 750,383 ns/op |

The focused run shows about 7.2% better throughput. The broader scan suite kept
the assertion case in the improved range, though the exact number was noisier.
A regression test checks that committed rows plus reverse-order local
assertions return canonical order and suppress duplicate assertions.

Commit-path guardrails stayed in the same range:

| Benchmark | Median |
| --- | ---: |
| `tx_commit_functional_local_updates` | 5,624,422 ns/op |
| `tx_commit_multi_relation_set_writes` | 287,807 ns/op |
| `tx_commit_multi_relation_unindexed_set_writes` | 288,127 ns/op |
| `tx_commit_stale_disjoint_set_writes` | 1,771,965 ns/op |
| `tx_commit_large_functional_updates` | 7,894,977 ns/op |
| `tx_commit_large_set_replacements` | 860,206 ns/op |

Conclusion: keep ordered union beside ordered difference. Dirty scans now treat
the committed result and transaction overlay as ordered set streams instead of
falling back to batch sorting whenever local assertions are present.

Rejected follow-up: replacing the owned-iterator/`Peekable` merge with an
index-based merge was slower. The index-based version removed iterator peeking,
but had to clone every output tuple handle instead of moving tuples out of the
input vectors.

| Benchmark | Owned iterator merge | Index-based clone merge | Change |
| --- | ---: | ---: | ---: |
| `tx_overlay_committed_scan_local_assertions` | 718,607 ns/op | 775,678 ns/op | -7.3% throughput |

Conclusion: keep the owned iterator merge. Avoiding `Tuple` handle clones
matters more here than avoiding `Peekable`.

### Accepted: Preallocate Ordered Difference Output

The ordered local-retraction difference helper originally built its output with
an empty vector:

```rust
let mut out = Vec::new();
```

This path is used when a scan starts with canonical committed/base rows and
then subtracts canonical transaction-local retractions. The output can never be
larger than the committed side, so reserving `left.len()` is a safe upper bound:

```rust
let mut out = Vec::with_capacity(left.len());
```

Result:

| Benchmark | Before | Focused after | Full scan-suite after |
| --- | ---: | ---: | ---: |
| `tx_overlay_committed_scan_local_retractions` | 722,751 ns/op | 716,463 ns/op | 717,151 ns/op |

The focused result is only about 0.9% faster, but the broad scan suite did not
show a regression. Keep it because it is a direct expression of the ordered-set
contract and removes avoidable growth checks on this path.

### Accepted: Direct Full TupleStore Scans

Committed full scans used the same `TupleStore::matching` path as arbitrary
non-leading binding scans:

```rust
let mut out = Vec::new();
self.for_each_matching(bindings, |tuple| out.push(tuple.clone()));
```

For arbitrary bindings, reserving full relation cardinality can be wasteful
because a non-leading filter may match only a few rows. For an all-unbound scan,
however, the output size is exactly the tuple-store cardinality. The retained
change first specialized only that case:

```rust
let mut out = if bindings.iter().all(Option::is_none) {
    Vec::with_capacity(self.len())
} else {
    Vec::new()
};
```

A production full-scan benchmark was added to the relation-index benchmark
surface, then expanded to cover both the indexed and unindexed relation shapes:

```text
production_full_scan
production_unindexed_full_scan
```

Result for a 16,384-row committed relation:

| Benchmark | Before | Focused after | Production-suite after |
| --- | ---: | ---: | ---: |
| `production_full_scan` | 186,315 ns/op | 182,776 ns/op | 182,971 ns/op |

A later paired run, with the reserve temporarily removed and then restored,
showed the indexed shape as effectively flat but the unindexed shape improving:

| Benchmark | No reserve | Reserve | Change |
| --- | ---: | ---: | ---: |
| `production_full_scan` | 184,408 ns/op | 184,888 ns/op | ~0% |
| `production_unindexed_full_scan` | 183,149 ns/op | 180,077 ns/op | +1.7% throughput |

That reserve-only change was weak on its own. The larger win came from taking
the all-unbound branch seriously: an all-unbound full scan also does not need to
call `matches_bindings` on every tuple. The final shape routes all-unbound
scans to a direct tuple-store copy:

```rust
if bindings.iter().all(Option::is_none) {
    return self.all_tuples();
}
```

Result against the reserve-only baseline:

| Benchmark | Reserve only | Direct all-tuples | Change |
| --- | ---: | ---: | ---: |
| `production_full_scan` | 183,581 ns/op | 172,512 ns/op | +6.1% throughput |
| `production_unindexed_full_scan` | 182,249 ns/op | 170,641 ns/op | +6.8% throughput |

The production benchmark suite also kept the bound scan and visit paths in
their expected ranges:

| Benchmark | Median after |
| --- | ---: |
| `production_low_cardinality_indexed_scan` | 1,744 ns/op |
| `production_low_cardinality_unindexed_scan` | 2,067 ns/op |
| `production_low_cardinality_indexed_visit` | 928 ns/op |
| `production_low_cardinality_unindexed_visit` | 928 ns/op |

Conclusion: keep the direct all-tuples full-scan path. It is intentionally
narrow, preserves canonical tuple order from the underlying tuple store, and
avoids turning selective non-leading scans into full-cardinality allocations.

### Accepted: Direct Full TupleStore Visits

The direct full-scan path above only helped materializing scans. Full visits
still went through the generic `try_for_each_matching` path:

```rust
if tuple.matches_bindings(bindings) && visitor(tuple)? {
    return Ok(());
}
```

For all-unbound visits the predicate is always true, so this was wasted
branching on every tuple. I added production full-visit benchmarks first, then
specialized the all-unbound `TupleStore::try_for_each_matching` path to visit
tuples directly:

```rust
if bindings.iter().all(Option::is_none) {
    return self.try_for_each_tuple(visitor);
}
```

Result:

| Benchmark | Generic matching visit | Direct tuple visit | Change |
| --- | ---: | ---: | ---: |
| `production_full_visit` | 100,536 ns/op | 90,437 ns/op | +11.1% throughput |
| `production_unindexed_full_visit` | 98,357 ns/op | 88,946 ns/op | +10.6% throughput |

Conclusion: keep the all-unbound visit specialization. It is as narrow as the
direct full-scan path and avoids paying predicate overhead when the query has
no predicates.

### Accepted: Prefix-Covered Scans Skip Residual Binding Checks

Prefix scans and visits had the same kind of redundant predicate work. If an
index prefix accounts for every bound value in the query, the ART prefix walk
has already selected every matching tuple. Running `matches_bindings` again is
unnecessary.

The accepted implementation distinguishes:

- prefix-covered predicates, where every bound position is part of the chosen
  prefix;
- prefix-plus-residual predicates, where the prefix narrows the search but other
  bound positions still need tuple-level filtering.

For tuple-store natural prefixes this means every binding after `bound_count`
is `None`. For explicit secondary indexes, the first `bound_count` index
positions must cover every bound position in the query. Only those
prefix-covered cases skip residual checks.

Result against the previous low-cardinality production run:

| Benchmark | Residual binding checks | Prefix-covered fast path | Change |
| --- | ---: | ---: | ---: |
| `production_low_cardinality_indexed_scan` | 1,806 ns/op | 1,520 ns/op | +17.1% throughput |
| `production_low_cardinality_unindexed_scan` | 2,099 ns/op | 1,829 ns/op | +15.3% throughput |
| `production_low_cardinality_indexed_visit` | 946 ns/op | 325 ns/op | +195.4% throughput |
| `production_low_cardinality_unindexed_visit` | 965 ns/op | 573 ns/op | +69.5% throughput |

Conclusion: keep the prefix-covered fast path. It is a clean semantic
distinction, preserves residual filtering when needed, and removes a hot
per-tuple branch from common indexed scans.

### Clarification: Precomputed Radix Keys Belong In ART Paths

If a path computes a canonical `RadixTupleKey`, the natural destination is an
ART-backed index, not a hash table keyed by the same bytes. The key format is
ordered specifically to support:

- exact lookup;
- prefix scan;
- ordered merge;
- ART-native intersection/join.

Using the same byte key as a `HashMap` key gives up most of that structure while
still paying key-construction cost and hash cost.

The important caveat is ownership. A previous experiment duplicated full radix
keys inside promoted overlay values so commit application could reuse them.
That was rejected because copying the `OverflowKey` into every value increased
memory traffic and did not improve commit throughput. The better long-term
shape is to use keys already owned by ART nodes through rart's lending key-view
APIs, or add a borrowed-key probe API where needed, rather than storing another
copy beside each tuple.

### Accepted: Functional Visibility Uses Radix Keys

The transaction-local functional visibility cache still used:

```rust
HashMap<TupleKey, Option<Tuple>>
```

where `TupleKey` is a projected `Vec<Value>`. That made functional replacement
pay for projection allocation and tuple-key hashing even after the rest of the
overlay had moved toward radix keys.

The accepted path is now:

```rust
AdaptiveRadixTree<RadixTupleKey, FunctionalVisibleEntry>
```

The first version stored the projected values beside each entry so conflict
validation could later look up the same functional key. That was already a win,
but it still allocated the projected value vector per touched key:

| Benchmark | `HashMap<TupleKey, _>` | ART with projected values | Change |
| --- | ---: | ---: | ---: |
| `tx_prepare_functional_local_updates` | 734,737 ns/op | 650,961 ns/op | +12.6% throughput |
| `tx_commit_functional_local_updates` | 5,636,983 ns/op | 5,526,499 ns/op | +2.1% throughput |
| `tx_commit_large_functional_updates` | 7,815,800 ns/op | 7,852,308 ns/op | ~0% |

The follow-up keeps a representative tuple in the entry instead. Normal lookup
encodes the `RadixTupleKey` directly from borrowed tuple values, and conflict
validation can reconstruct the projected key from the representative only when
needed.

| Benchmark | `HashMap<TupleKey, _>` | ART with representative tuple | Change |
| --- | ---: | ---: | ---: |
| `tx_prepare_functional_local_updates` | 734,737 ns/op | 600,496 ns/op | +22.4% throughput |
| `tx_commit_functional_local_updates` | 5,636,983 ns/op | 5,490,818 ns/op | +2.7% throughput |
| `tx_commit_large_functional_updates` | 7,815,800 ns/op | 7,769,138 ns/op | ~0% |

Conclusion: this is a small but coherent win. It also removes another
`TupleKey` hash path from normal MVCC writes and reinforces the rule that
precomputed radix keys should feed ART-backed structures.

### Relation-State Logical Indirection

For larger MVCC work, represent logical tuples with stable tuple ids and keep
indexes pointing to tuple ids rather than physical tuple values. This aligns
with Wu et al.'s logical pointer finding for write-heavy workloads.

Possible Mica shape:

```text
RelationState:
    tuple id -> current tuple payload
    full tuple key -> tuple id
    secondary key -> set of tuple ids
```

This would make updates less tuple-copy-heavy and creates a natural place for
future version chains.

Wu et al. separate index management into logical pointers and physical
pointers. Their evaluation finds that logical pointers are better for
write-intensive workloads because indexes can remain pointed at a stable logical
object while version storage changes underneath. Physical pointers can favour
read-heavy workloads because they avoid an indirection step, but updates have to
keep more index state synchronized.

Mica's current committed secondary indexes are closer to physical payload
pointers than logical pointers:

```rust
VersionedAdaptiveRadixTree<RadixTupleKey, TupleBucket>

TupleBucket:
    One(Tuple)
    Many(Vec<Tuple>)
```

That means a secondary index stores tuple payloads, not stable tuple ids. I
added a benchmark-only logical secondary index prototype:

```rust
LogicalTupleIndex:
    tuples: Vec<Tuple>
    entries: VersionedAdaptiveRadixTree<RadixTupleKey, Vec<usize>>
```

The benchmark compares a low-cardinality secondary index over position `[0]`
where each matching bucket contains 128 rows. It is intentionally not a full
production design: it does not implement MVCC version chains, deletes, fileout
ordering, or relation-local id reuse. It only tests whether replacing payload
buckets with tuple-id buckets is a direction worth pursuing.

Result:

| Benchmark | Tuple payload buckets | Tuple-id buckets | Change |
| --- | ---: | ---: | ---: |
| `radix_secondary_prefix_visit` | 282 ns/op | 267 ns/op | +5.4% throughput |
| `radix_secondary_prefix_scan` | 1,202 ns/op | 1,178 ns/op | +5.4% throughput |
| `radix_secondary_rebuild_index` | 3,655,614 ns/op | 2,610,767 ns/op | +40.1% throughput |

The read-side result is modest. The write-side result is the important one. I
added a same-secondary-key update benchmark where the index is over position
`[0]`, but each update changes only position `2`. The payload-bucket index has
to remove and reinsert the full tuple in the secondary bucket because tuple
ordering changed. The tuple-id index only updates the tuple arena; the secondary
index bucket does not change.

| Benchmark | Tuple payload buckets | Tuple-id buckets | Change |
| --- | ---: | ---: | ---: |
| `radix_secondary_same_key_updates` | 312,373 ns/op | 54,115 ns/op | 5.8x faster |

This is the clearest evidence so far for Wu et al.'s logical-pointer direction:
logical ids avoid secondary-index churn for updates that do not change the
secondary key.

I also added the paired key-changing update benchmark. It keeps the same
secondary index over position `[0]`, but each update changes position `0` and
therefore must move the row between secondary buckets. The workload updates
1024 rows and rotates them across 128 new groups so it measures key movement
without collapsing everything into one synthetic bucket.

| Benchmark | Tuple payload buckets | Tuple-id buckets | Change |
| --- | ---: | ---: | ---: |
| `radix_secondary_key_changed_updates` | 2,913,294 ns/op | 2,820,053 ns/op | +3.3% throughput |

This result is much smaller, as expected. When the secondary key changes, both
representations have to update the secondary ART. Tuple ids still avoid moving
full tuple payloads through the secondary bucket, but they do not eliminate the
index-maintenance work itself. The strong case for logical indirection is
therefore stable logical rows whose payloads change more often than their
indexed keys.

The logical-secondary benchmark above is still too generous because it ignores
the canonical full-tuple membership index. A production relation cannot update
only a secondary index; it also needs to maintain the full-tuple ART used for
set membership and natural scans. I added a benchmark-only relation-shaped
pair:

```rust
PayloadRelationIndexes:
    full tuple key -> Tuple
    secondary key -> BTreeSet<Tuple>

ArenaRelationIndexes:
    rows: Vec<Tuple>
    full tuple key -> row id
    secondary key -> Vec<row id>
```

This still is not a full production design. It has no version chains, free-list
policy, deletion tombstones, persisted row ids, or canonical fileout ordering.
It does measure the important missing cost: updating the primary full-tuple
membership ART alongside the secondary index.

Result:

| Benchmark | Payload indexes | Row-id arena indexes | Change |
| --- | ---: | ---: | ---: |
| `payload_relation_secondary_prefix_visit` | 358 ns/op | 293 ns/op | +22.3% throughput |
| `payload_relation_rebuild_indexes` | 6,988,041 ns/op | 7,319,106 ns/op | -4.5% throughput |
| `payload_relation_same_key_updates` | 657,107 ns/op | 383,348 ns/op | 1.7x faster |
| `payload_relation_key_changed_updates` | 3,298,923 ns/op | 3,263,737 ns/op | +1.1% throughput |

This is a more realistic result than the pure logical-secondary benchmark.
Maintaining the primary full-tuple ART takes back a large part of the same-key
win, but row ids are still materially faster because the secondary index does
not remove and reinsert full tuple payloads. Key-changing updates remain flat
because both designs must move the secondary index entry. Rebuild is slightly
worse in this prototype, which means a production arena should not land without
bulk-build work and a clear row-ordering story.

I then added bulk relation-shaped constructors for both variants. These build
the secondary index by first collecting projected keys, preserving existing
order when possible, and inserting one grouped bucket per key. This is closer to
the production bulk secondary-index path than repeated `update_k` calls.

| Benchmark | Incremental build | Bulk build | Change |
| --- | ---: | ---: | ---: |
| `payload_relation_rebuild_indexes` | 6,988,041 ns/op | 4,174,066 ns/op | +67.4% throughput |
| `arena_relation_rebuild_indexes` | 7,319,106 ns/op | 3,621,962 ns/op | +102.1% throughput |

With bulk secondary construction, the row-id arena becomes faster than the
payload relation on rebuild too:

| Benchmark | Payload bulk build | Arena bulk build | Change |
| --- | ---: | ---: | ---: |
| `*_relation_bulk_rebuild_indexes` | 4,174,066 ns/op | 3,621,962 ns/op | +15.2% throughput |

That changes the production implication. The row-id arena is not just an
update-side idea, but it depends on building indexes in batches. Incremental
construction is the wrong benchmark shape for initial load.

Snapshot cloning is the other constraint. The first arena prototype stored rows
as `Vec<Tuple>`, which means cloning a relation-shaped value still copies one
tuple handle per row even though the ART indexes themselves are cheap
copy-on-write clones. I added clone benchmarks for the relation-shaped variants
and a shared-row arena prototype:

```rust
SharedArenaRelationIndexes:
    rows: Arc<[Tuple]>
    full tuple key -> row id
    secondary key -> Vec<row id>
```

Result:

| Benchmark | Median |
| --- | ---: |
| `radix_clone_index` | 45 ns/op |
| `payload_relation_clone_indexes` | 134,707 ns/op |
| `arena_relation_clone_indexes` | 134,987 ns/op |
| `shared_arena_relation_clone_indexes` | 72 ns/op |

The plain arena has the same clone problem as the payload relation because both
copy the 16,384-row `Vec<Tuple>`. Sharing the immutable row storage restores the
clone cost to the same broad nanosecond range as the COW ART. A focused
shared-row prefix visit measured `302 ns/op`, close to the non-shared arena's
`293 ns/op`, so sharing the row slice does not appear to harm this read path.

This sharpens the production shape: a row-id arena must separate immutable
snapshot row storage from mutable/versioned row state. A plain `Vec<Tuple>` row
arena would make snapshot publication too expensive.

I then tested a benchmark-only append-version arena:

```rust
AppendVersionArenaRelationIndexes:
    base rows: Arc<[Tuple]>
    appended rows: Vec<Tuple>
    current logical row id -> physical row id
    full tuple key -> logical row id
    secondary key -> Vec<logical row id>
```

The intent was to model the Wu et al. logical-pointer shape more directly:
secondary indexes point at stable logical row ids, while updates append new
physical row versions and only update the current-version pointer plus the full
tuple membership index.

The first version made every secondary read resolve `logical id -> current
physical row` through an ART lookup. That was much too expensive for clean
snapshots:

| Benchmark | Median |
| --- | ---: |
| `append_version_arena_relation_secondary_prefix_visit` | 3,416 ns/op |
| `append_version_arena_relation_same_key_updates` | 630,596 ns/op |
| `append_version_arena_relation_clone_indexes` | 70 ns/op |

Adding the obvious clean-snapshot fast path changes the read result: if there
are no appended versions, secondary scans can use the base row slice directly by
logical id and skip current-version lookup.

| Benchmark | Per-row current lookup | Clean-snapshot fast path | Change |
| --- | ---: | ---: | ---: |
| `append_version_arena_relation_secondary_prefix_visit` | 3,416 ns/op | 304 ns/op | +1044.3% throughput |
| `append_version_arena_relation_same_key_updates` | 630,596 ns/op | 637,175 ns/op | ~0% |
| `append_version_arena_relation_clone_indexes` | 70 ns/op | 58 ns/op | ~0% |

The resulting read and clone costs are good, but same-key updates are slower
than the mutable row-id arena's `383,348 ns/op`. The extra cost is the
current-version ART lookup/update on every logical row update. That does not
reject versioned storage, but it rejects this naive "current pointer is an ART
probe per row" shape for hot update paths. A production version-chain design
needs either a cheaper current-row map, a dirty/current overlay that only
applies to changed rows, or a planner-visible distinction between clean
snapshot reads and dirty/versioned reads.

I tested that next with a dirty-current overlay:

```rust
DirtyCurrentArenaRelationIndexes:
    base rows: Arc<[Tuple]>
    appended rows: Vec<Tuple>
    dirty current logical row id -> physical row id
    full tuple key -> logical row id
    secondary key -> Vec<logical row id>
```

Clean secondary reads still use the base row slice directly. Dirty reads check
only a small `HashMap<usize, usize>` overlay for changed rows. This avoids
encoding a row-id radix key and probing an ART for every current-row lookup.

The first dirty-read comparison scanned group `42`, while seeded updates touched
groups `0` through `7`. That is still useful, but it measures a dirty relation
where the scanned rows are unchanged. The changed-row case is below it.

| Benchmark | ART current pointer | Dirty-current overlay | Change |
| --- | ---: | ---: | ---: |
| clean secondary prefix visit | 304 ns/op | 325 ns/op | ~0% |
| dirty relation, unchanged prefix visit | 3,293 ns/op | 834 ns/op | +294.9% throughput |
| dirty relation, changed prefix visit | 3,424 ns/op | 883 ns/op | +287.8% throughput |
| same-key updates | 637,175 ns/op | 401,094 ns/op | +58.9% throughput |
| clean clone | 58 ns/op | 54 ns/op | ~0% |

The dirty-current overlay is much closer to the mutable row-id arena update
cost while keeping cheap clean-snapshot cloning and reads. Dirty scans are
still slower than clean scans because every candidate row needs a dirty-current
lookup. The changed-row and unchanged-row dirty scans are close enough to make
the key point clear: the dominant cost is the current-row lookup strategy, not
whether the matched rows actually live in the appended segment. A hash overlay
is far cheaper here than an ART keyed by encoded logical row id.

This suggests a production shape closer to:

```text
base snapshot rows: immutable/shared
secondary indexes: logical row ids
transaction or next-snapshot delta:
    appended row payloads
    changed logical row id -> appended physical row id
```

The open question is how to publish or compact that delta without eventually
turning clean reads into dirty reads forever. That likely needs either batch
compaction into a new clean row segment or a small stack of row segments with a
cheap changed-row overlay.

I added one blunt publication baseline: walk every logical row, resolve the
current tuple through the dirty overlay, materialize a fresh shared row slice,
and rebuild the full-tuple and secondary ART indexes in bulk. This is not a
clever compactor, but it answers whether the simplest possible publication step
is already obviously too expensive.

Same benchmark session:

| Benchmark | Median |
| --- | ---: |
| `payload_relation_bulk_rebuild_indexes` | 4,155,582 ns/op |
| `arena_relation_bulk_rebuild_indexes` | 3,575,281 ns/op |
| `shared_arena_relation_bulk_rebuild_indexes` | 3,443,500 ns/op |
| `dirty_current_arena_relation_compact_to_shared` | 3,610,069 ns/op |

The dirty-current compaction path is about 4.8% slower than clean shared-arena
bulk rebuild in this run. That makes the simplest batch publication strategy
plausible: keep dirty-current overlays cheap while a transaction or next
snapshot is dirty, then periodically compact back into an immutable shared row
segment. It does not prove this scales to larger relations or heavier dirty
sets, but it weakens the case for starting with a more complicated segmented
publication design.

I also tested whether carrying precomputed full-tuple radix keys beside the row
arena helps this publication path:

```rust
KeyedSharedArenaRelationIndexes:
    rows: Arc<[Tuple]>
    full tuple keys: Arc<[RadixTupleKey]>
    full tuple key -> logical row id
    secondary key -> Vec<logical row id>
```

The expected benefit was avoiding repeated full-tuple key encoding when
publishing a dirty-current arena. The result was mostly negative:

| Benchmark | Shared arena | Keyed shared arena | Notes |
| --- | ---: | ---: | --- |
| secondary prefix visit | 385.6 ns/op | 376.0 ns/op | noise-level read difference |
| bulk rebuild from tuples | 7,094,910 ns/op | 7,400,505 ns/op | keyed variant slower |
| clone | 88.0 ns/op | 76.8 ns/op | both are nanosecond COW clones |
| dirty-current compaction | 3,689,895 ns/op | 3,656,201 ns/op | effectively flat; keyed run had weak stability |

This argues against making stored full-tuple keys a default row-arena payload
just to speed publication. The extra key array adds memory traffic during bulk
builds. The current benchmark's tuple keys are small, so a relation with wide
tuples or expensive value encodings could change this, but the default design
should not assume cached full keys are a win. Keep the ART indexes keyed by
encoded tuples; do not also persist a parallel full-key array unless a wider-key
benchmark proves it.

I then added that wider-key benchmark explicitly: 16-value tuples, with several
string payload fields, and a keyed dirty-current relation whose base and
appended physical rows carry precomputed full-tuple radix keys. This measures
the design where the key is computed when the row version is created, not during
publication.

| Wide benchmark | Unkeyed shared | Keyed shared | Notes |
| --- | ---: | ---: | --- |
| bulk rebuild from tuples | 13,900,303 ns/op | 15,111,854 ns/op | keyed still slower for raw build |
| dirty-current compaction | 14,238,218 ns/op | 5,443,663 ns/op | 2.6x faster when row versions already carry keys |
| clone | 124.8 ns/op | 67.2 ns/op | both are still nanosecond COW clones |

This changes the conclusion. A parallel full-key array is not useful when
building clean snapshots from raw tuples, because it adds memory traffic while
still computing every key. It is useful when MVCC row versions naturally compute
and retain their full-tuple keys at creation time. In that shape, publication
can rebuild the primary ART from existing radix keys instead of re-encoding
wide tuples.

Production implication: do not add a separate key cache to the current
tuple-owned store as a blind optimization. In a row-version arena design,
however, make the row version carry or reference its canonical full-tuple key,
especially for wider tuples and heap-heavy values.

Conclusion: tuple-id secondary indexes are worth a production experiment, but
not as a piecemeal replacement for one index structure. The meaningful version
is a relation-state layout where tuple ids are the common currency for:

- canonical full-tuple membership;
- secondary indexes;
- future version chains;
- conflict validation;
- fileout ordering.

The likely next production slice is therefore a relation-local tuple arena
prototype, not just changing `TupleBucket` to hold ids while keeping every other
path tuple-owned.

### Sparse Bitset Identity Indexes

For identity-heavy unary/binary relations, maintain optional sparse bitset
indexes over dense relation-local ordinals or identity ordinals. This is not a
primary storage replacement. It is a candidate for fast set algebra:

```text
Portable(?x) ∩ Location(?x, #room) ∩ VisibleTo(?x, #actor)
```

This should be introduced only behind a planner-visible index path and measured
against the current ART join path.

### Snapshot State Layout

The canonical tuple set is now:

```rust
TupleStore:
    Small(Arc<BTreeSet<Tuple>>)
    Radix(VersionedAdaptiveRadixTree<RadixTupleKey, Tuple>)
```

This preserves cheap snapshot cloning for small relations and copy-on-write
snapshot mutation for large committed tuple membership. It is now a measured
improvement for large relation updates. Candidate next alternatives:

- tuple-id arena plus full-key index;
- append-only tuple versions plus current tuple-id map;
- relation-local dense ids plus sparse bitset side indexes.

Any replacement needs to preserve:

- set semantics;
- deterministic fileout/public ordering;
- efficient prefix scans;
- cheap snapshot cloning;
- commit validation against stale snapshots.
