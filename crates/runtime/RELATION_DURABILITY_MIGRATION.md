# Relation Durability Migration

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

`relation_lifecycle_benches` measures three real layers:

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
cargo bench -p mica-runtime --bench relation_lifecycle_benches -- all
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

## Endpoint Migration Checkpoint

Endpoint relations are now volatile ordinary relations. Opening publishes `Endpoint`, optional
`EndpointPrincipal`, optional `EndpointActor`, `EndpointProtocol`, and `EndpointOpen` together in
one transaction. Closing retracts the endpoint's rows together in a second transaction. Context
reconstruction and effect routing read ordinary snapshots, while `assume_actor` replaces the
functional `EndpointActor` tuple in the task transaction and rolls back if the task aborts.

Request facts still use an endpoint-keyed `TransientStore` scope at this checkpoint, so endpoint
close also drops any remaining rows in that scope. That part disappears with request migration.

Command:

```sh
cargo bench -p mica-runtime --bench relation_lifecycle_benches -- endpoint_lifecycle
```

| Execution | Direct-store baseline | Volatile endpoint transactions | Change |
| --- | ---: | ---: | ---: |
| Serial | 6,824 ns | 30,480 ns | 4.47x slower |
| 1 worker | 5,928 ns | 32,894 ns | 5.55x slower |
| 4 workers | 7,182 ns | 43,986 ns | 6.12x slower |

The serial checkpoint had 17.01% coefficient of variation; the one- and four-worker measurements
were stable at 0.61% and 1.13%, respectively.

The four-worker volatile path sustains 226.71 k tuple mutations/s, compared with 1.39 M/s for the
direct store. The endpoint result is a semantic migration checkpoint, not a performance win: the
runtime-sized kernel pays two MVCC snapshot publications per lifecycle. Final measurement must keep
this regression visible while evaluating transaction publication and endpoint-state layout costs.

## Request Migration Checkpoint

The web host declares all eight request relations volatile. It validates and asserts the complete
request fact set in one ordinary transaction, then retracts the same fact set in one transaction
before closing the request endpoint. A host-side ownership guard performs the retraction and close
when the async request future is cancelled as well as on explicit success and error paths. The
ordinary host API rejects durable relation metadata, preventing lifecycle cleanup code from
silently treating durable application facts as request state.

The agent and MUD `session/*` relations are also volatile. They are keyed by endpoint identities
allocated by the web host and must not survive a process restart, where the host's endpoint identity
sequence starts again. Durable world and conversation facts remain durable; only endpoint-owned UI
state is classified as volatile.

Command:

```sh
cargo bench -p mica-runtime --bench relation_lifecycle_benches -- request_lifecycle
```

| Execution | Direct-store baseline | Volatile request transactions | Change |
| --- | ---: | ---: | ---: |
| Serial | 11,120 ns | 61,944 ns | 5.57x slower |
| 1 worker | 11,259 ns | 69,187 ns | 6.15x slower |
| 4 workers | 15,386 ns | 82,962 ns | 5.39x slower |

The checkpoint is stable at 0.55%, 1.04%, and 1.32% coefficient of variation. Four workers sustain
337.07 k tuple mutations/s, compared with the 1.82 M/s direct-store baseline. Request facts are
batched, but the complete lifecycle still publishes four MVCC snapshots: endpoint open, request
assertion, request retraction, and endpoint close. That publication count and the number of relation
states copied per request are the next performance targets after the transient overlay is removed.

## Public Surface Removal Checkpoint

The `assert_transient`, `retract_transient`, and `drop_transient_scope` builtins are no longer part
of the language, and the runner, driver, and VM builtin context no longer expose transient mutation
APIs. Volatile facts use ordinary `assert` and `retract` statements inside tasks; trusted hosts use
the checked batched volatile APIs for lifecycle-owned state. The remaining transient code is now an
unreachable internal overlay, ready to be deleted without another producer migration.

## Overlay Removal Checkpoint

`TransientStore`, the composed snapshot and transaction readers, scope construction, cache-bypass
branches, mutation metrics, and VM/task wiring have been deleted. Runtime relation reads now have a
single transaction path: durable and volatile rows share the same MVCC snapshot and transaction
semantics, while durability controls only commit-provider projection and restart recovery.

## Final Measurement And Recovery

Each workload below ran in a separate benchmark process. The one- and four-worker rows contain
intentional concurrency within that one selected case; no benchmark cases overlapped.

| Workload | Execution | Median lifecycle | Throughput |
| --- | --- | ---: | ---: |
| Volatile tuple | Serial | 14,896 ns | 130.95 k tuple mutations/s |
| Volatile tuple | 1 worker | 18,697 ns | 107.09 k tuple mutations/s |
| Volatile tuple | 4 workers | 30,617 ns | 65.09 k tuple mutations/s |
| Volatile endpoint | Serial | 30,888 ns | 295.05 k tuple mutations/s |
| Volatile endpoint | 1 worker | 33,035 ns | 306.15 k tuple mutations/s |
| Volatile endpoint | 4 workers | 45,034 ns | 221.94 k tuple mutations/s |
| Volatile request | Serial | 62,192 ns | 450.72 k tuple mutations/s |
| Volatile request | 1 worker | 69,757 ns | 402.76 k tuple mutations/s |
| Volatile request | 4 workers | 82,647 ns | 339.23 k tuple mutations/s |

Removing the overlay is performance-neutral relative to the volatile migration checkpoints. The
endpoint medians changed by +1.3%, +0.4%, and +2.4%; request medians changed by +0.4%, +0.8%, and
-0.4% for serial, one-worker, and four-worker execution. These small mixed changes do not establish
a performance effect. At this checkpoint, the material cost remained MVCC publication: two commits
for the tuple and endpoint lifecycles, and four for the request lifecycle.

The final isolated transaction medians are 2,752 ns, 3,392 ns, and 7,856 ns for durable rows and
2,648 ns, 3,266 ns, and 7,057 ns for volatile rows under serial, one-worker, and four-worker
execution. Volatility consistently avoids the commit-provider projection, but both paths retain the
same publication and shared-snapshot contention shape.

The persistent recovery test reopens a Fjall store after mixed durable and volatile commits. It
confirms that durable rows remain, volatile rows are absent, volatile relation metadata remains,
and a version gap caused by a volatile-only live commit does not prevent later durable recovery:

```sh
cargo test -p mica-relation-kernel --features fjall-provider \
  fjall_provider_recovers_volatile_relations_without_their_rows
```

## Fused Request Lifecycle Optimization

The web host now opens the request endpoint and asserts its request facts in one transaction. Its
normal and cancellation cleanup paths likewise retract the request facts and close the endpoint in
one transaction. Validation of every supplied relation as volatile happens before either
transaction begins, so a rejected request-fact batch cannot leave an open endpoint. The request
lifecycle therefore publishes two MVCC snapshots instead of four while retaining the same 28 tuple
mutations and the endpoint's visibility during invocation.

Command:

```sh
cargo bench -p mica-runtime --bench relation_lifecycle_benches -- volatile_request_lifecycle
```

| Execution | Four-publication median | Two-publication median | Latency change | Throughput |
| --- | ---: | ---: | ---: | ---: |
| Serial | 62,192 ns | 48,880 ns | -21.4% | 572.83 k tuple mutations/s |
| 1 worker | 69,757 ns | 53,694 ns | -23.0% | 521.47 k tuple mutations/s |
| 4 workers | 82,647 ns | 60,025 ns | -27.4% | 466.76 k tuple mutations/s |

Two consecutive optimized runs produced medians of 49,121/53,107/62,022 ns and
48,880/53,694/60,025 ns for serial, one-worker, and four-worker execution. The four-worker case
remains noisy at 7.7--11.2% coefficient of variation, but both runs show a material improvement over
the 82,647 ns checkpoint. Publication count is therefore a demonstrated request-path cost. The
remaining four-worker backend-stall rate and variability point to shared snapshot publication and
per-relation state copying as the next relation-kernel optimization surface.
