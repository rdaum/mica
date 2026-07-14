# Transient Relation Inventory

This inventory captures the manager-scoped transient relation semantics before their replacement by
transactional volatile named relations. It is an implementation checkpoint, not a compatibility
contract.

## Store And Visibility Model

`TransientStore` owns a map from scope identity to per-relation indexed tuple state. A task reads
the union of the principal, actor, and endpoint scopes supplied by its current `RuntimeContext`.
Writes select a scope explicitly and mutate the shared store immediately, outside the task's MVCC
transaction. `ComposedRelationRead` and `ComposedTransactionRead` merge visible transient rows with
ordinary snapshot, transaction-overlay, computed, and rule-derived rows.

A suspended task receives a new runtime context when resumed, so its visible transient scopes may
change. Any visible transient row can force dispatch and method-program lookup to bypass snapshot
caches. `SharedTaskManager` serializes all transient mutation behind one `RwLock<TransientStore>`.

## Producers And Consumers

| Producer or surface | Owner and cleanup | Tuple ownership | Readers and effects | Migration decision |
| --- | --- | --- | --- | --- |
| `TaskManager::open_endpoint*` | Endpoint; `close_endpoint` drops the complete endpoint scope | Every endpoint tuple already contains the endpoint identity | Runtime-context reconstruction, `assume_actor`, effect routing, endpoint-open checks, ordinary queries and rules | Volatile endpoint relations with explicit endpoint cells |
| Web-host request installation | HTTP request invocation; closing its temporary endpoint drops request and endpoint rows together | Every request tuple already contains the request identity | `http_request` dispatch and any rule or query reachable from it | Volatile request relations with lifecycle retraction; visibility requires an explicit decision |
| Driver and runner transient APIs | Caller-selected scope; caller must retract tuples or drop the scope | Arbitrary; the API does not require the selected scope to appear in the tuple | Any named relation query, rule, computed provider, or dispatch dependency | Remove after host producers migrate |
| `assert_transient`, `retract_transient`, `drop_transient_scope` | Caller-selected identity; no intrinsic lifetime owner | Arbitrary | Same general relation namespace as durable facts | Remove; no application file currently uses these builtins |
| Runtime tests using `Selected` | Actor or principal scope; tests explicitly retract or drop it | Unary tuple omits the owner | Validates ambient scope union, authority, rules, retries, and cache bypass | Replace with explicit owner column or delete with obsolete semantics |

## Relations Used By Real Host Paths

| Relation | Arity | Existing explicit owner | Current catalogue declaration | Required lifetime |
| --- | ---: | --- | --- | --- |
| `Endpoint` | 1 | Endpoint at position 0 | Runtime bootstrap metadata | Endpoint open to close; empty after restart |
| `EndpointPrincipal` | 2 | Endpoint at position 0 | Runtime bootstrap metadata | Endpoint open to close; empty after restart |
| `EndpointActor` | 2 | Endpoint at position 0 | Runtime bootstrap metadata | Endpoint open to close; empty after restart |
| `EndpointProtocol` | 2 | Endpoint at position 0 | Runtime bootstrap metadata | Endpoint open to close; empty after restart |
| `EndpointOpen` | 1 | Endpoint at position 0 | Runtime bootstrap metadata | Endpoint open to close; empty after restart |
| `HttpRequest` | 1 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestMethod` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestPath` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestVersion` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestPrincipal` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestActor` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestHeader` | 3 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |
| `RequestBody` | 2 | Request at position 0 | Application-declared ordinary relation | One request invocation; empty after restart |

The current API permits a named relation to have durable rows and transient rows simultaneously.
The host relations above must instead be declared volatile. Any other mixed extension found during
migration must be classified explicitly; volatile named relations do not preserve mixed durability
inside one relation.

## Isolation Contract

Volatility controls storage lifetime only. Authority remains relation-wide: an authorized unbound
query may see all live rows, regardless of its actor, principal, or endpoint context. Endpoint and
request relations therefore keep their explicit owner identity at position 0, and application code
must bind that identity when it needs one lifecycle's rows. Mica does not add implicit row-level
security or a task-context filter to volatile scans.

The runtime has an adversarial two-actor test for this contract: both actors have `CanRead` for one
volatile relation, both write owners are present, and each actor's unbound query sees both rows.
Binding the owner narrows the result. Endpoint and request migration must preserve that explicit
shape rather than depending on the previous ambient scope overlay.

## Baseline Harness

`transient_relation_benches` measures three real layers:

- one transient tuple assertion and retraction;
- endpoint open, effect-route lookup, and close;
- request endpoint open, nine request-fact assertions, and close.

Each workload has a serial measurement plus one- and four-worker concurrent measurements. The
concurrent cases share one `SharedSourceRunner`, matching the production `RwLock<TransientStore>`
contention boundary. Micromeasure runs the benchmark cases serially; worker concurrency exists only
inside the selected case.

Record the baseline results here before relation durability changes, then compare the final volatile
path using the same logical workloads.

### 2026-07-14 Baseline

Command:

```sh
cargo bench -p mica-runtime --bench transient_relation_benches -- all
```

Environment: Linux 6.17 aarch64, Cortex-X925, fixed 3.9 GHz maximum frequency, Rust 1.95.0.
Throughput counts tuple mutations; latency is for one complete lifecycle. Concurrent results are
combined across the workers in the selected case.

| Workload | Execution | Median lifecycle | Throughput |
| --- | --- | ---: | ---: |
| Tuple lifecycle | Serial | 1,256 ns | 1.58 M tuple mutations/s |
| Tuple lifecycle | 1 worker | 1,091 ns | 1.85 M tuple mutations/s |
| Tuple lifecycle | 4 workers | 1,033 ns | 1.94 M tuple mutations/s |
| Endpoint lifecycle | Serial | 6,824 ns | 1.46 M tuple mutations/s |
| Endpoint lifecycle | 1 worker | 5,928 ns | 1.69 M tuple mutations/s |
| Endpoint lifecycle | 4 workers | 7,182 ns | 1.39 M tuple mutations/s |
| Request lifecycle | Serial | 11,120 ns | 2.51 M tuple mutations/s |
| Request lifecycle | 1 worker | 11,259 ns | 2.48 M tuple mutations/s |
| Request lifecycle | 4 workers | 15,386 ns | 1.82 M tuple mutations/s |

The four-worker endpoint and request cases already regress relative to one worker despite performing
independent lifecycles. That is the contention signature the volatile path must eliminate or
materially improve; the final comparison must use this same harness rather than a separately shaped
microbenchmark.

## Transactional Storage Checkpoint

`relation_durability_benches` isolates one assertion commit followed by one retraction commit. It
compares durable and volatile ordinary transactions against the current `TransientStore` mutation
path. The durable case uses a non-accumulating commit provider so the difference from volatile
storage is the persistence projection and provider call, not an ever-growing in-memory log.

Command, run once per storage path so benchmark processes do not overlap:

```sh
cargo bench -p mica-relation-kernel --bench relation_durability_benches -- durable_transaction_lifecycle
cargo bench -p mica-relation-kernel --bench relation_durability_benches -- volatile_transaction_lifecycle
cargo bench -p mica-relation-kernel --bench relation_durability_benches -- transient_store_lifecycle
```

| Storage path | Execution | Median lifecycle | Throughput |
| --- | --- | ---: | ---: |
| Durable transaction | Serial | 2,680 ns | 743.22 k tuple mutations/s |
| Durable transaction | 1 worker | 3,357 ns | 596.02 k tuple mutations/s |
| Durable transaction | 4 workers | 7,284 ns | 275.52 k tuple mutations/s |
| Volatile transaction | Serial | 2,584 ns | 771.60 k tuple mutations/s |
| Volatile transaction | 1 worker | 3,265 ns | 610.45 k tuple mutations/s |
| Volatile transaction | 4 workers | 6,729 ns | 297.50 k tuple mutations/s |
| `TransientStore` | Serial | 368 ns | 4.99 M tuple mutations/s |
| `TransientStore` | 1 worker | 335 ns | 5.96 M tuple mutations/s |
| `TransientStore` | 4 workers | 505 ns | 3.96 M tuple mutations/s |

Volatile transactions avoid persistence work but retain the ordinary MVCC commit cost. In this
deliberately minimal two-commit lifecycle they are about eight times slower than `TransientStore`
with one worker and thirteen times slower with four workers. Endpoint and request migration must
therefore batch each lifecycle's tuple changes into transactions and rerun the production-shaped
benchmarks; volatility alone is not a performance win.
