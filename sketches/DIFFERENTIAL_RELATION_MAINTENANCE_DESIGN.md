# Differential Relation Maintenance In Mica

Date: 2026-07-18

Status: implemented staged architecture. Stages 0 through 7 are complete; Stage 8 records the
measured resident-GPU work and the explicit deferral criteria for its independent optional items.

This document is based on Mica commit `4e5e796f227fb05fe08f1f42c4c80b2fcceee1cd`.

## Why Maintain Derived Relations Incrementally?

Mica models a live world in which authoritative facts change while rules continuously define what
else is true. A rule-derived relation is therefore not only the result of an occasional analytical
query. It can be part of the current operational state used by dispatch, authority, UI composition,
agents, simulations, and application decisions.

The current complete evaluator gives each snapshot the right answer, but it discards all previous
derived work. After any committed change, the first read of a derived relation can repeat every
relevant scan, join, and recursive fixpoint round. This is reasonable for a small or rarely queried
world. It becomes a poor fit when:

- authoritative relations are large;
- commits are frequent and usually small;
- the rule graph is stable;
- derived relations are read repeatedly or drive subscriptions; and
- a change affects only a small part of the dependency graph.

Differential maintenance changes the unit of work. Instead of asking, "What is the complete answer
for this new snapshot?", it asks, "What changed in the answer because of these assertions and
retractions?" The desired cost is proportional to the changed input and its affected dependency cone
rather than the size of the whole world. This is not a complexity guarantee: a one-tuple input
change can legitimately affect millions of results. It is an opportunity to avoid work on the
unaffected majority.

This matters for more than query latency:

- **Predictable live reads:** warm derived relations do not impose a complete materialization pause
  on the first task that reads them after every commit.
- **Reactive output:** the maintenance engine naturally knows which derived tuples became visible or
  disappeared. The runtime can publish settled changes to mailbox subscribers without rescanning and
  diffing a full answer.
- **Efficient recursion:** reachability, containment, delegation, and dependency closures can reuse
  settled work from prior versions.
- **Correct retractions:** removing a fact propagates through the same dependency graph as adding
  one, including cases with multiple supporting derivations.
- **Shared computation:** several consumers can read one maintained result and its arrangements
  instead of building equivalent indexes and closures independently.
- **Backend placement:** accumulated work remains available in native packed batches and, for large
  operators, reusable `wgpu` inputs.

The capability also fits Mica's snapshot model. A settled derived epoch can be published with the
same immutable version as its authoritative inputs. Applications still reason about ordinary
snapshot truth; differential changes remain an internal way to produce that truth.

## Application Shapes

### Observability and operational reasoning

Observability systems combine relatively stable topology and policy with continually changing
status:

- services, instances, hosts, regions, and their dependencies;
- deployments, versions, ownership, and configuration;
- current alerts, incidents, maintenance state, and health observations;
- routes, queues, databases, certificates, and external providers; and
- suppression, escalation, and service-level policy.

Rules can derive affected services, customer-facing blast radius, responsible teams, correlated
incidents, invalid configurations, and currently actionable alerts. For example:

```mica
Affected(change, resource) :-
  Changed(change, resource)

Affected(change, dependent) :-
  Affected(change, dependency),
  DependsOn(dependent, dependency)

ServiceAtRisk(service) :-
  RunsOn(service, host),
  HostUnhealthy(host)
```

When one host changes health, incremental maintenance can traverse only the affected dependency
region and emit changes to `ServiceAtRisk` or `Affected`. A UI, incident router, or agent can react
to those result changes without recomputing the complete service graph.

This design does not by itself make Mica a raw time-series engine. High-rate metric aggregation,
event-time windows, lateness, histograms, and percentile calculations need explicit aggregate and
temporal operators that the current rule language does not provide. The immediate fit is the live
relational state around telemetry: topology, current observations, alert state, policy, ownership,
and derived operational consequences. A telemetry provider can summarize raw streams into changing
facts at a rate suitable for the relation kernel.

### Authorization, policy, and compliance

Mica already derives effective authority from durable policy relations. Changes to role membership,
delegation, relation surfaces, selector surfaces, or grants can affect many `Can*` facts.

Incremental maintenance can update only the affected principals, roles, resources, and permissions,
then publish a complete authority-consistent snapshot. The same pattern applies to:

- configuration compliance;
- segregation-of-duty rules;
- network segmentation policy;
- data residency and retention policy;
- feature and entitlement eligibility; and
- explanations of which policy facts make an action allowed or denied.

Authority construction must still occur at the task or session boundary described by Mica's security
model. Incremental relations make the effective policy inputs cheaper to keep current; they do not
turn durable authority policy into checkpointed capability values.

### Live worlds, simulations, and games

Mica's existing MUD rules derive containment, visibility, carrying, exits, and room reachability.
Similar applications derive:

- spatial containment and adjacency;
- visibility and audibility;
- valid movement or interaction targets;
- connected areas and transport networks;
- ownership, faction, reputation, or quest eligibility;
- propagated environmental state; and
- which actors or systems are affected by an event.

A door opening, item movement, or actor relocation usually changes a small part of the world.
Maintaining the affected closures is preferable to rebuilding all reachability and visibility on the
next command or rendered view. Large simulation ticks can still be batched, allowing native or GPU
operators to process a frontier together.

### Reactive user interfaces and collaborative applications

Many Mica interfaces are views over relations. Maintained results can identify exactly which view
facts changed after a commit:

- visible objects and available actions;
- filtered search and navigation results;
- unread, assigned, blocked, or actionable work items;
- document backlinks and dependency warnings;
- presence and collaboration state; and
- DOM or component facts derived from application state.

This can support change-oriented host synchronization: update the affected view region rather than
re-run and compare every query for every connected client. A mailbox subscription can receive one
settled batch for each relevant commit and schedule the affected rendering or synchronization work
in a later task.

### Knowledge graphs and impact analysis

Knowledge bases frequently derive transitive or multi-hop relationships:

- ancestor, containment, and classification closure;
- dependency and reverse-dependency closure;
- inferred tags and categories;
- affected documents, models, or decisions;
- consistency violations; and
- reachable evidence or source references.

This is useful for source workbenches, digital engineering models, and operational knowledge bases.
Editing one relationship can update affected conclusions and invalidations without re-evaluating an
entire graph.

### Build systems, program analysis, and configuration analysis

Code and configuration can be represented as relations describing definitions, calls, imports,
artifacts, ownership, types, effects, and dependencies. Rules can derive:

- which artifacts need rebuilding;
- which tests are affected by a change;
- call and dependency reachability;
- policy or invariant violations;
- dead or unreachable definitions;
- deployment impact; and
- review or ownership routing.

The rule graph usually stays stable while source facts change incrementally, which is the favourable
shape for differential maintenance. This does not replace specialized parsers or compilers; they
produce the changing base facts consumed by the maintained relation program.

### Digital twins, orchestration, and control planes

Operational models combine desired state, observed state, topology, constraints, and active work.
Rules can maintain:

- desired-versus-observed drift;
- eligible placement targets;
- blocked operations and their dependencies;
- failure or maintenance blast radius;
- route and resource availability;
- reconciliation candidates; and
- violated invariants.

Network controllers and orchestrators are a particularly natural fit because a small topology or
policy change can affect recursive reachability while most of the model remains unchanged. External
effects still occur through Mica tasks after transactional decisions; the rule engine only maintains
the relational conclusions.

### Agent workspaces and live planning state

An agent workspace can relate goals, tasks, resources, dependencies, observations, permissions, and
tool results. Maintained rules can derive:

- tasks unblocked by a new result;
- resources affected by a proposed change;
- currently applicable tools or methods;
- missing evidence and unmet preconditions;
- conflicts between plans; and
- which agents or users should be notified.

This keeps the declarative working model current as tasks commit observations. It does not make
language-model inference differential; it incrementally maintains the structured state around
inference and action.

### Application summary

| Application                  | Changing base facts                                  | Useful maintained results                                 |
| ---------------------------- | ---------------------------------------------------- | --------------------------------------------------------- |
| Observability                | health, alerts, deployments, topology                | blast radius, correlated incidents, actionable alerts     |
| Authorization and compliance | roles, grants, policy, resource membership           | effective permissions, violations, eligibility            |
| Worlds and simulations       | location, state, connections, actor activity         | reachability, visibility, interaction scope               |
| Reactive UI                  | application and session state                        | visible components, actions, filtered work                |
| Knowledge graphs             | assertions, classifications, references              | closure, affected conclusions, consistency failures       |
| Build and program analysis   | syntax, definitions, calls, dependencies             | rebuild sets, affected tests, analysis findings           |
| Control planes               | desired state, observed state, topology, constraints | drift, placement, blocked work, reconciliation candidates |
| Agent workspaces             | goals, tasks, results, resources, permissions        | ready work, unmet prerequisites, affected collaborators   |

## When It Is And Is Not Worthwhile

Differential maintenance is most likely to win when:

- the rule program and relation metadata remain stable across many fact commits;
- the maintained input and output relations are large;
- individual commits change a small fraction of those relations;
- derived relations are expensive because of joins, negation, or recursion;
- consumers repeatedly read the same derived state or need output changes; and
- affected result fanout is smaller than complete result cardinality.

Complete evaluation remains preferable when:

- the relations and rule results are small;
- derived relations are rarely read;
- rule installation or catalogue changes are more common than fact changes;
- one input routinely invalidates most of the derived world;
- an opaque computed relation cannot report changes;
- raw high-rate events require unsupported windows or aggregates; or
- arrangement and trace memory cost exceeds the work saved.

The warm-on-demand and backend-decline policies in this design preserve that choice. Differential
maintenance is a physical strategy for suitable live workloads, not a new semantic obligation for
every relation.

## Summary

Mica should incrementally maintain rule-derived relations across committed snapshots by adopting the
core model used by differential Datalog and differential dataflow: versioned signed changes, indexed
traces, consolidation, and iterative feedback for recursive rules.

This is an extension of the current relation kernel, not a replacement for it. The authoritative
state remains Mica's MVCC relation store. Differential state is a reconstructable execution cache
whose published output is attached to the same immutable snapshot versions as base relations.

Mica already has the first important prerequisite. `RuleSet::evaluate_fixpoint` compiles positive
dependency strongly connected components and uses `FULL`, `DELTA`, and novel frontiers to evaluate
recursive rules semi-naively within one snapshot. The missing capability is reuse across snapshots:
each new snapshot currently discards its derived cache and recomputes all active rules on the next
derived read.

The division of responsibility is:

- the relation kernel owns versions, signed changes, arrangements, fixpoint progress, consolidation,
  snapshot publication, and CPU fallback;
- the existing packed executor implements native bulk relational operators;
- the existing `wgpu` backend accelerates eligible large membership and equality-join operators;
- Fjall persists durable authoritative facts and catalogue changes, not derived execution traces;
- the compiler continues to expose ordinary relation rules without differential-specific syntax; and
- the runtime may expose settled relation and catalogue changes through existing mailbox
  capabilities without making the relation kernel depend on runtime values.

The first production slice should incrementally maintain non-recursive positive rules. Recursive
deletions are the principal correctness milestone and should not be approximated with simple tuple
reference counts.

## Goals

- Reuse prior derived work when committed base facts change.
- Propagate both assertions and retractions through positive rules, stratified negation, and
  recursive rule components.
- Preserve Mica's set semantics, snapshot isolation, transaction-local visibility, and deterministic
  canonical answers.
- Keep the committed MVCC relation store authoritative.
- Reuse the existing query plans, packed operators, CPU parallelism, and `wgpu` accelerator where
  their placement thresholds justify them.
- Keep all accelerated paths optional and semantically identical to the serial tuple evaluator.
- Allow a cold or unsupported rule program to fall back to complete fixpoint evaluation.
- Make incremental state reconstructable from the rule catalogue and authoritative facts.
- Produce settled, versioned visible-change batches that an optional runtime subscription service
  can publish without recomputing relation results.

## Non-Goals

- Replacing Mica's relation store with Timely Dataflow, Differential Dataflow, DDlog, or a column
  store.
- Introducing a distributed scheduler or general partially ordered timestamp API.
- Making differential weights or fixpoint iterations visible as Mica language values. A mailbox
  change cursor may identify a committed version without exposing differential logical time.
- Adding event-time windows, out-of-order stream processing, or user-facing temporal syntax.
- Persisting differential traces in the first implementation.
- Adding mailbox subscription builtins in the first kernel implementation stage.
- Incrementally executing opaque computed relation callbacks without an explicit change interface.
- Sending every relation operation to the GPU.
- Adding a `differential`, `materialized`, or `maintained` rule keyword before measured workloads
  show that authors need to control maintenance policy.
- Weakening rule, value, authority, or transaction semantics to make a physical backend eligible.

## Terminology

This design uses the following terms:

- **base relation**: authoritative extensional facts stored in a Mica relation;
- **derived relation**: facts contributed by active relation rules;
- **epoch**: one committed Mica snapshot version;
- **change**: a tuple, logical time, and signed difference;
- **weight**: the consolidated signed multiplicity of a tuple inside differential execution;
- **visible tuple**: a tuple whose settled set-presence value is true;
- **trace**: the indexed history or consolidated state of changes for one collection;
- **arrangement**: a trace indexed by columns needed by an operator;
- **frontier**: a statement that no more changes can arrive before a logical time;
- **SCC**: a strongly connected component of positive recursive relation dependencies;
- **settled epoch**: an epoch for which all strata and recursive iterations have completed;
- **visible change**: a transition in public set membership after extensional and derived
  contributions have been consolidated; and
- **subscription cursor**: an opaque committed-version token used to order or resume mailbox change
  delivery, not event time in the modelled world.

Mica remains a set-oriented language. Weights are internal execution state, not observable bag
semantics.

## Current Mica Behaviour

### Rule language and compilation

The compiler accepts relation rules of the form:

```mica
ReachableRoom(area, from, to) :-
  RoomInArea(from, area),
  RoomInArea(to, area),
  Exit(from, command, middle),
  ReachableRoom(area, middle, to)
```

Rule bodies may contain positive atoms, safe negated atoms, and comparison guards. Rules are
validated for stratified negation. Rule terms are variables or literal values. This subset is a good
fit for differential maintenance because rule bodies do not execute arbitrary verbs or effects.

`crates/relation-kernel/src/rules.rs` currently:

- compiles variables to binding slots;
- assigns rules to negation strata;
- finds positive dependency SCCs;
- orders SCCs by dependency;
- emits one semi-naive variant for each recursive atom occurrence;
- evaluates recursive variants with one atom reading `DELTA` and other atoms reading `FULL`; and
- combines candidate output in ordered sets before accepting novel tuples.

This is semi-naive evaluation within one snapshot. It does not maintain signed changes between
snapshots.

### Snapshot-derived state

`Snapshot::derived_relations` lazily evaluates every active rule to a fixpoint and builds ordinary
`RelationState` values for the derived output. The result is cached in a snapshot-local `OnceLock`.

Every committed fact change, relation creation, rule installation, or rule disable operation creates
a new empty derived cache. The next derived read therefore starts from the complete extensional
state even when a commit changed only one tuple.

Transactions similarly cache a complete derived result after their first derived read. Their reader
includes transaction-local assertions and retractions, so the result has correct read-your-own-write
semantics.

### Commit changes

Each commit already contains the information needed to seed an incremental engine:

```text
Commit
  version
  catalogue_changes
  changes: [FactChange]

FactChange
  relation
  tuple
  kind: Assert | Retract
```

Changes record effective set mutations produced while constructing the next snapshot. They can be
translated directly to input differences:

```text
Assert  -> +1
Retract -> -1
```

Catalogue changes are different. Installing or disabling a rule changes the dataflow program and
should initially invalidate and rebuild maintained derived state.

### Mailboxes and commit effects

Mica already has transactional mailbox delivery. `mailbox_send` records a pending task send. A
successful transaction boundary promotes it to a committed task effect; a retry or abort discards
it. The task manager later delivers committed sends and the driver wakes mailbox receivers.

Mailboxes are ephemeral runtime queues rather than authoritative relation facts. The current runtime
has no subscription registry and does not route `Commit::changes` or catalogue changes to mailboxes.
`Transaction::commit` returns a `CommitResult`, but the task commit boundary currently uses only
success or failure. Subscription support must carry the settled commit batch across that boundary
without making `mica-relation-kernel` depend on mailbox capabilities.

### Packed and accelerated execution

`QueryPlan` and its physical form represent scans, inputs, projection, equality join, semi-join,
anti-join, union, and difference. Eligible immediate-valued inputs can be exported as immutable
packed columns.

The `RelationAccelerator` boundary currently exposes:

- membership selection over one left and one right column; and
- equality join over one or two key columns.

The `mica-relation-wgpu` implementation uses Vulkan compute through `wgpu`, returns selected row
indexes or matching row pairs, and can decline work when inputs are too small, unsupported, busy, or
the device is unavailable. The native executor then falls back to serial or Rayon-backed operators.

Rule evaluation already routes eligible two-positive-atom rules through the packed equality-join
path, including semi-naive variants whose selected recursive atom reads a delta frontier. Snapshot
materialization itself currently supplies a serial `ExecutionContext`, while transaction-derived
evaluation receives the runtime-configured context.

## Required Semantics

### Public set semantics

An authoritative base tuple has presence zero or one. Differential operators may create multiple
supporting derivations for a derived tuple, so internal weights can have larger magnitude.

For a settled epoch, a public derived tuple is visible exactly when its set-presence value is true.
The engine must emit a public assertion only on a transition from absent to present, and a public
retraction only on a transition from present to absent.

```text
set_presence(weight) = if weight > 0 then 1 else 0
```

The implementation must maintain the invariant that settled support for a set-valued collection is
not negative. Negative settled support indicates an invalid input delta, a consolidation bug, or an
incorrect recursive maintenance algorithm.

### Multiple derivations

Consider two paths supporting the same result:

```text
Edge(a, b) + Edge(b, d) -> Reachable(a, d)
Edge(a, c) + Edge(c, d) -> Reachable(a, d)
```

Removing `Edge(b, d)` must remove one contribution without retracting `Reachable(a, d)`. A later
removal of `Edge(c, d)` retracts the last contribution and therefore retracts the visible tuple.

All joins, unions, projections, and rule-head contributions must preserve enough multiplicity to
make this distinction before applying set `distinct` semantics.

### Extensional facts in rule-head relations

Mica permits a relation to contain authoritative facts and also appear as a rule head. The visible
relation is the set union of extensional and derived contributions.

Incremental execution must keep those sources distinguishable internally. Removing a derived
contribution must not remove an extensional fact with the same tuple, and removing the extensional
fact must not hide a tuple still derived by a rule.

Inside a recursive SCC, extensional target tuples enter the initial recursive collection as input
changes. Rule output feeds back through set `distinct` before the next iteration.

### Stratified negation

Negation continues to read only completed lower strata. An anti-join must respond to changes on both
sides:

- a left assertion is emitted when the matching right-key count is zero;
- a left retraction removes its current output;
- a right-key transition from zero to positive retracts all matching visible left rows; and
- a right-key transition from positive to zero asserts all matching visible left rows.

Only zero-crossings of the consolidated right-key count affect set-valued anti-join output. The
operator therefore needs a right key-count arrangement and a left arrangement capable of finding
rows by the same key.

### Comparison guards

Comparison guards are stateless filters. Each signed input change either passes through unchanged or
is discarded. Comparison must use Mica `Value` equality and total ordering rather than raw pointer
or payload ordering for heap-backed values.

### Snapshot consistency

A published snapshot must never combine base facts from version `V` with derived facts from another
version. A derived read either:

- observes the settled derived state for the snapshot's exact version; or
- computes that version through the complete evaluator before returning.

Readers must not observe partially propagated strata or recursive iterations.

### Transaction-local visibility

Dirty transactions must retain read-your-own-write behaviour. The first incremental implementation
may continue to run the existing complete fixpoint evaluator over transaction overlays. This is a
correct fallback and isolates cross-commit maintenance from transaction retry and conflict logic.

Incremental transaction overlays are a later optimization. They must derive a private result from
the transaction's signed write set without mutating the committed maintained state.

## Logical Time And Progress

### Commit epochs

Mica commit versions provide a total order and are sufficient as the outer differential timestamp:

```text
epoch = snapshot version
```

The initial database load is treated as a batch of assertions at its recovered version or at a
synthetic initialization epoch before the first live commit.

### Recursive iterations

Positive recursion needs a nested iteration coordinate:

```text
time = (epoch, iteration)
```

Stratum and SCC order are compiled scheduling structure rather than user-visible time dimensions.
Within one recursive SCC:

1. input changes for epoch `V` enter iteration zero;
2. non-feedback operators propagate signed changes;
3. rule-head changes pass through set `distinct`;
4. feedback advances changes to the next iteration;
5. all mutually recursive rules observe the same iteration frontier; and
6. the SCC settles when no changes remain at a later iteration.

The engine may publish epoch `V` only after all strata have advanced beyond that epoch.

Mica does not initially need Timely Dataflow's general partially ordered timestamp or distributed
progress protocol. Commits are totally ordered, commit construction is serialized, and SCC
iterations are local. The implementation still needs an explicit frontier concept so that output is
not published early.

## Why Recursive Deletion Is The Hard Boundary

Simple support counting is insufficient for recursive cycles. For example:

```text
P(x) :- Seed(x)
P(x) :- P(x)
```

After removing `Seed(x)`, the second rule must not allow `P(x)` to support itself forever. Similar
cycles occur naturally in reachability over cyclic graphs.

The implementation uses a proven incremental fixpoint formulation. The formulation is differential
iteration:

- retain signed changes by epoch and iteration;
- feed only changes around the recursive loop;
- consolidate contributions at each logical time;
- apply set `distinct` to accumulated relation state rather than treating each proof as an
  independently permanent reference; and
- advance the output frontier only after feedback is quiescent.

The complete evaluator remains the correctness oracle until randomized cyclic insertion and deletion
sequences demonstrate equivalence. If a native differential iteration proves too complex or
memory-heavy, a deletion algorithm such as DRed is a valid alternative, but the two approaches must
not be mixed informally.

## Kernel Architecture

### Compiled maintained program

Compile the active rule set into one maintained program containing:

- strata and ordered positive dependency SCCs;
- rule variants and binding slots already produced by the current compiler;
- logical operators for scans, filters, projection, join, anti-join, union, and distinct;
- recursive feedback edges;
- required arrangements and their key columns;
- value-domain and arity information;
- physical eligibility information for tuple, packed, parallel, and accelerator execution; and
- a stable identity derived from active rule definitions and relevant relation metadata.

The current rule compiler and physical query plan are inputs to this work. The design should
converge on one internal operator vocabulary instead of adding a parallel public rule API.

### Weighted changes

The maintained executor needs a private representation conceptually equivalent to:

```text
WeightedChange
  relation
  tuple or packed row reference
  time
  difference: signed integer
```

The exact Rust layout should follow measurement. Requirements are:

- checked arithmetic or explicit overflow failure;
- consolidation of equal `(tuple, time)` entries;
- efficient conversion from `FactChange`;
- preservation of Mica `Value` ownership and equality semantics; and
- a packed form that keeps tuple columns separate from differences.

Weights should not be represented as ordinary Mica relation columns. Doing so would confuse logical
arity, projection, canonical ordering, and persistence.

### Traces and arrangements

A trace stores immutable batches of consolidated changes. It should support:

- lookup by the full tuple;
- lookup by compiled join keys;
- iteration over changes at or before a version;
- appending a new epoch batch;
- compaction after old snapshots and iteration times are no longer observable; and
- materializing a canonical `RelationState` for a published snapshot.

An arrangement is created only for a key required by the compiled program. The same arrangement
should be shared by all operators using that relation and key. Mica's authoritative ART and
secondary indexes remain the preferred access path for ordinary point and prefix queries; a
differential arrangement is justified by repeated incremental operator use.

Immutable batches are important for both MVCC sharing and `wgpu` buffer reuse. A trace should evolve
as:

```text
base batch + epoch batch + epoch batch + ... + occasional compaction
```

It should not rebuild one monolithic packed `FULL` relation after every commit.

### Versioned derived output

Each settled maintained epoch produces immutable derived relation states and a list of visible
derived changes. A snapshot that retains the epoch can answer ordinary `scan`, `visit`, estimate,
and batch-export requests through the existing relation interfaces.

Derived state remains a cache:

- it is not written to the authoritative commit log;
- restart rebuilds it from active rules and recovered base facts;
- evicting it does not change database meaning; and
- a snapshot without retained incremental state can use complete evaluation.

### Warm-on-demand policy

Maintaining every active rule after every commit can cost more than recomputation when derived
relations are rarely read. The initial policy should be warm-on-demand:

1. A cold snapshot computes derived state with the complete evaluator on first use.
2. That use installs a maintained program state for subsequent versions.
3. While the state is warm, commits advance it from the current snapshot to the next version.
4. The next snapshot publishes base and derived state atomically.
5. An idle maintained state may be evicted under an explicit memory policy.

If the current snapshot is cold, commits remain cheap and the next derived read can initialize from
the latest authoritative snapshot. If the current snapshot is warm, the first implementation may
perform maintenance synchronously under the serialized commit path. This makes atomicity simple but
must be benchmarked for commit latency.

A later worker may compute derived state outside the commit lock, but publication must still make
the exact version boundary explicit and derived reads may need to wait for catch-up. Asynchronous
maintenance is not part of the first slice.

### Catalogue changes

The initial invalidation policy is:

- rule installed or disabled: rebuild the maintained program and derived state;
- relation created: rebuild if it changes compilation or a referenced computed-relation binding;
- relation metadata changed, if later supported: rebuild;
- ordinary fact changes: incrementally advance affected operators only; and
- unrelated relation changes: produce no maintained work.

Incrementally changing the dataflow graph is unnecessary for the first implementation. Rule changes
are expected to be less frequent than fact changes, and Mica values one coherent current API over
compatibility machinery.

### Computed relations

`ComputedRelation::scan` is an opaque callback. The kernel cannot infer its changes from
`FactChange`, and some providers may perform external or dynamically calculated work.

A compiled rule dependency on a computed relation is therefore a maintenance barrier. Initially, the
affected rule component uses complete evaluation. A future provider may opt into incremental
maintenance by exposing:

- a stable collection identity;
- signed changes by snapshot version;
- value-domain and arrangement capabilities; and
- deterministic replay or initialization behaviour.

No such API should be introduced until a real computed provider needs it.

## Integration With The Existing `wgpu` Backend

### Responsibility boundary

The GPU remains a stateless physical operator backend. It does not own:

- commit versions;
- recursive iteration progress;
- tuple weights;
- set `distinct` semantics;
- trace compaction;
- snapshot publication; or
- recovery.

This keeps differential correctness in one backend-independent kernel implementation.

### Equality joins

A differential equality join evaluates contributions such as:

```text
DELTA(left) join FULL(right)
FULL(left)  join DELTA(right)
```

The existing `RelationAccelerator::join_equality` can receive the key columns and return matching
row pairs unchanged. For each pair, native materialization reads the corresponding left and right
weights and emits:

```text
output_difference = left_difference * right_difference
```

The output is then projected and consolidated by the kernel. This requires a weighted packed batch
beside the current `NativeBatch`; it does not require a weighted GPU shader in the first version.

The current packed join canonicalizes output as a set. Differential execution must not deduplicate
matching rows before their differences are added. Equal output tuples from different row pairs are
distinct contributions until consolidation.

### Membership and anti-join

The existing membership accelerator can select left rows whose keys are present or absent on the
right. It can accelerate an initial anti-join materialization or a large zero-crossing batch.

The kernel still owns right-key counts and transition detection. A positive-to-zero transition can
require enumerating all arranged left rows for a key; a zero-to-positive transition retracts them.
The GPU should be selected only when a batch is large enough to amortize encoding, submission, and
readback.

### Immutable batch reuse

`mica-relation-wgpu` caches encoded columns by immutable source `Arc` identity. Differential traces
should preserve immutable batch objects across epochs so unchanged inputs retain their GPU encoding.
Repacking all accumulated rows into a fresh `Arc` each commit would defeat this cache and turn a
small logical delta into a complete upload.

Trace compaction creates new batches and therefore new GPU encodings. Compaction placement must
include this invalidation cost.

### Placement

Most commit deltas will be small. CPU index probes and native merge operations should remain the
default. GPU execution is most plausible for:

- initial materialization;
- large batched commits;
- large recursive frontiers;
- joins against very large arrangements;
- bulk anti-join transitions; and
- later whole-pipeline sort, consolidate, and distinct experiments.

The existing accelerator decline and CPU fallback contract should remain unchanged. Differential
placement must additionally consider:

- delta and full input cardinality separately;
- output fanout;
- whether input batches are already GPU-encoded;
- number of immutable trace batches;
- consolidation cost after readback;
- current GPU and CPU admission state; and
- whether a transaction needs latency or throughput.

Current thresholds were tuned for full packed operators and must be remeasured for delta/full
workloads.

### Snapshot execution context

Snapshot-derived evaluation currently constructs a serial execution context, while transactions use
the runtime-configured context. A maintained executor must receive the configured context from the
kernel or runtime so eligible committed maintenance can reach CPU parallel and `wgpu` operators.

This is an internal execution-policy change. Snapshots should not own devices or initialize GPU
state.

### Later GPU work

A later experiment may add GPU buffers for signed differences and operators for sort, consolidate,
and distinct. That should happen only after the CPU-owned weighted path demonstrates representative
workloads where row-pair readback or CPU consolidation dominates.

GPU-side weights are not necessary to establish correct incremental maintenance and should not delay
the first implementation.

## Persistence And Recovery

Authoritative commits continue to persist:

- relation catalogue changes;
- rule catalogue changes; and
- effective durable base fact assertions and retractions.

Derived tuples, arrangements, encoded GPU columns, progress frontiers, and trace compaction state
are not durable in the first implementation.

Recovery proceeds as follows:

1. load relations, active rules, and base facts through the existing provider;
2. compile the maintained rule program when a derived relation becomes warm;
3. perform complete fixpoint evaluation to establish its initial settled state;
4. build required arrangements from that state; and
5. maintain later live commits incrementally.

Persisted trace checkpoints may be considered only if measured restart time is unacceptable. A
checkpoint would need a format version, rule-program identity, catalogue identity, base commit
version, value codec compatibility, and complete validation before use.

## Concurrency And Lifecycle

### Commit publication

The first implementation should advance warm derived state while commit construction remains
serialized. The next snapshot is published only after:

- authoritative relation changes are applied;
- all affected maintained strata settle;
- visible derived changes are known; and
- immutable derived relation states for the next version are complete.

If maintenance fails, the commit must not publish a mismatched partial snapshot. The implementation
may either fail the commit with a precise kernel error or discard the incremental attempt and use a
complete evaluation before publication. Silent stale output is not allowed.

### Old snapshots

An old snapshot may outlive later commits. It must retain either:

- immutable derived relation states for its settled epoch; or
- enough authoritative state to perform complete evaluation if first accessed later.

Trace compaction may advance only beyond times no retained snapshot or active transaction needs.
Initial implementation can retain more state than necessary and add compaction after correctness is
established.

### Transactions and retry

Committed maintained state must be immutable from a transaction's perspective. A transaction that
retries against a newer base snapshot obtains the maintained state for that new version and
re-evaluates its private writes. No incremental side effect may escape a transaction before commit.

## Language And User-Facing Behaviour

Ordinary rules remain unchanged:

```mica
Within(item, place) :-
  LocatedIn(item, place)

Within(item, place) :-
  LocatedIn(item, container),
  Within(container, place)
```

Incremental maintenance is an execution strategy. It does not change rule truth, query results,
ordering, cardinality, or error behaviour.

Commit version is processing time for maintenance, not event time in the modelled world. An
application that needs historical or temporal reasoning should represent time explicitly in facts.
Windows require ordinary committed assertions and retractions or a later explicit temporal language
feature.

The first implementation adds no compatibility path and no parallel old/new rule API. The complete
evaluator remains an internal semantic fallback and test oracle.

Differential maintenance itself adds no language syntax. Mailbox subscriptions are a separate,
opt-in user-facing runtime feature enabled by the settled change output. Adding that feature will
require a builtin or equivalent host API, but it does not require annotations on relation
definitions or rule syntax.

## Mailbox Change Subscriptions

Differential maintenance makes efficient subscriptions to rule-derived results practical. It is not
required for every kind of change feed, however, and the public semantics must distinguish three
sources:

| Subscription subject           | Source                             | Meaning                                                                                  |
| ------------------------------ | ---------------------------------- | ---------------------------------------------------------------------------------------- |
| Authoritative fact changes     | Existing `Commit::changes`         | Effective committed assertions and retractions of stored facts                           |
| Catalogue changes              | Existing `Commit::catalog_changes` | Relation creation, rule installation, and rule disabling                                 |
| Public relation-result changes | Settled visible-change batch       | Tuples whose public set membership became true or false after all affected rules settled |

"Changes to a rule" can therefore mean two different things. A consumer interested in rule
definitions subscribes to catalogue changes. A consumer interested in conclusions produced by rules
subscribes to the affected head relation. The latter observes the consolidated relation result, not
proof provenance or the contribution of one particular rule. Mica should not expose rule-specific
proof changes until it has an explicit provenance model.

### Activation and scope

A relation-result subscription is a maintained consumer. Registering it warms and pins the rule
dependency component needed to produce the subscribed relation. It does not cause unrelated rule
components or all relations to receive differential treatment. Removing the last consumer permits
that component to become cold under the normal eviction policy.

Subscriptions to authoritative fact changes or catalogue changes can be implemented from existing
commit data and do not themselves require differential execution. Subscriptions to derived results
require either differential visible changes or a complete recompute-and-diff fallback. The latter is
correct but should be used only for cold, unsupported, or rebuilding components.

The first relation-result filter should match the kernel's existing scan shape: one relation and
zero or more bound columns. This has predictable index requirements and authority scope. Arbitrary
multi-relation query subscriptions would introduce separately compiled maintained programs and are
outside the initial subscription design.

### Registration and initial state

The runtime subscription service should accept:

- the subject and optional relation pattern;
- a mailbox sender capability;
- the principal or session context needed for authority refresh;
- an initial-delivery mode; and
- an optional subscription cursor for resumption.

The initial Mica API is:

```mica
subscribe_changes(sender, subject, relation, bindings, initial [, cursor [, queue_budget]])
cancel_subscription(subscription)
```

`subject` is `:catalogue`, `:facts`, or `:relation`; `initial` is `:changes` or `:snapshot`.
Catalogue subscriptions use `nothing` for the relation and an empty binding list. Fact and relation
subscriptions accept a relation identity or name and one binding entry per column, with `nothing`
for an unbound column. The returned subscription capability is ephemeral runtime state, not a
durable fact or an annotation on a relation or rule. Equivalent host registration and cancellation
APIs use the same request model and an explicit publication boundary.

Registration and cancellation requested by a Mica task must be transactional task effects. They
become active only after the requesting boundary commits and are discarded on retry or abort, just
like pending mailbox sends. If registration commits as version `V`, snapshot-then-changes includes
the subscribed result at `V` and begins incremental delivery at `V + 1`; it must not also deliver
the transaction's changes as a separate incremental batch. A host registering outside a task needs
an equivalent explicit snapshot boundary.

Two initial-delivery modes are useful:

- **changes only:** register at settled version `V` and deliver matching changes from `V + 1`; and
- **snapshot then changes:** deliver the complete matching result at `V`, followed by matching
  changes from `V + 1`.

Registration must make the snapshot-to-change handoff atomic. It may register while holding the
publication boundary or record `V` and replay retained commits after enumerating the snapshot. It
must not miss a commit between reading the initial result and activating delivery.

### Settled change batches

The relation kernel should make a conceptual batch like the following available after maintenance:

```text
SettledCommitChanges
  cursor
  catalogue_changes
  fact_changes
  relation_changes
```

`relation_changes` contains public zero-crossings after extensional and derived contributions have
been combined. For example, retracting an authoritative fact does not emit a public relation
retraction if an active rule still derives the same tuple. Internal weights, proof counts, fixpoint
iterations, and operator changes are never placed in mailbox messages.

The runtime filters this kernel batch for each subscription and enqueues one message containing all
matching changes for the committed version. It should not send one mailbox message per tuple. A
message identifies its subscription and cursor and contains ordered assertion and retraction
entries. The public encoding can be chosen with the builtin API; the semantic batch boundary must
not depend on that representation.

Rule installation or disabling may rebuild a component and produce a large relation-result change.
If the result exceeds delivery limits, the runtime should send a resynchronization marker rather
than enqueue an unbounded number of entries. The subscriber can then read a fresh snapshot and
resume after its cursor.

### Transactional publication and ordering

For committed version `V`, the required order is:

```text
apply authoritative changes
  -> settle every affected rule stratum and recursive component
  -> publish snapshot V and its settled change batch
  -> match authorized subscriptions
  -> enqueue versioned mailbox batches
  -> wake receivers
```

No subscriber may observe intermediate recursive iterations, a partially rebuilt rule component, or
changes from a transaction that later aborts or retries. A task with more than one successful commit
boundary must publish each boundary's batch; publication cannot wait for a later terminal task
outcome and combine distinct committed versions.

Mailbox FIFO order should preserve increasing cursors for each subscription. A receiver reacts in a
later task and transaction. Its writes may create another committed version and another change
batch, but mailbox handling must never execute re-entrantly inside the originating commit or
differential fixpoint.

### Authority

Possession of a mailbox sender capability authorizes delivery to that mailbox; it does not grant
permission to read a relation. Subscription registration must check relation read authority for the
requested pattern. Delivery must use refreshed session or principal authority rather than retaining
an `AuthorityContext` indefinitely across policy changes.

Filtering occurs before values enter the mailbox queue. If refreshed authority no longer permits the
subscription, the runtime must stop data delivery and either close the subscription or enqueue a
non-sensitive revocation marker. It must not reveal the existence, cardinality, or values of
unauthorized changes.

### Backpressure, durability, and recovery

Current Mica mailboxes are in-memory runtime queues. Subscriptions therefore cannot promise durable
or exactly-once delivery across process failure. Recovery-critical commands, audit history, and
external-effect outboxes must remain committed relation facts or use another durable log.

Each subscription has an explicit queue budget. The initial implementation applies it both to the
number of undrained version batches and to the number of entries in one batch. When a subscriber
falls behind, the runtime should replace undelivered incremental batches with a single
resynchronization marker containing the latest safe cursor. Silently dropping a change or allowing
unbounded mailbox growth is not acceptable. Coalescing across versions is valid only for a
separately specified latest-state mode because it removes observable intermediate transitions.

Subscriptions are ephemeral. After restart or mailbox loss, a client recreates its mailbox,
re-authorizes, reads a fresh snapshot or supplies a retained cursor, and resumes. Cursor-based
replay is available only while the required commit and settled-change history remains retained.

### Crate boundary

`mica-relation-kernel` should produce the settled batch and remain unaware of mailboxes, principals,
sessions, and runtime capabilities. `mica-runtime` should own the subscription registry, authority
refresh, filtering, queue budgets, and conversion to mailbox values. `mica-driver` should retain its
existing role of waking receivers after delivery.

The task commit path currently discards the successful `CommitResult`. Runtime integration should
carry each successful settled batch to a post-publication dispatcher, including commit paths not
originating in an ordinary task. Introducing a relation-kernel callback that directly sends mailbox
values would invert the existing crate boundaries and is not recommended.

## Staged Implementation

### Stage 0: establish the oracle and work metrics

- Keep the current complete semi-naive evaluator unchanged.
- Add a harness that applies commit sequences and captures complete derived output after every
  version.
- Record input changes, affected rule components, candidate changes, consolidated changes, visible
  output changes, fixpoint iterations, and elapsed time.
- Add cyclic, multi-proof, negation, and extensional-plus-derived workloads.

Exit criterion: deterministic current-evaluator results and work measurements for every planned
incremental test shape.

### Stage 1: non-recursive positive maintenance

- Compile relation-to-rule dependencies for non-recursive positive components.
- Translate committed `FactChange`s to signed input changes.
- Implement weighted filter, project, join, union, head contribution, consolidate, and set-presence
  transitions.
- Publish immutable derived output with the next snapshot.
- Fall back for recursion, negation, computed relations, and dirty transactions.

Exit criterion: randomized assertion and retraction sequences match complete recomputation after
every commit.

### Stage 2: arrangements and warm-on-demand state

- Share arrangements by relation and key columns.
- Retain immutable epoch batches and compact them conservatively.
- Initialize maintained state on first derived use.
- Preserve old snapshot answers while later epochs advance.
- Add affected-component scheduling so unrelated commits do no rule work.

Exit criterion: repeated small commits reuse prior indexes and derived state with lower total work
than complete recomputation.

### Stage 3: stratified negation

- Add right-key counts and left lookup arrangements for anti-joins.
- Propagate zero-to-positive and positive-to-zero transitions.
- Settle each lower stratum before its negated consumers advance.
- Test overlapping right proofs and left duplicate derivations.

Exit criterion: randomized positive and negative input changes match complete stratified evaluation.

### Stage 4: recursive differential maintenance

- Add `(epoch, iteration)` logical times and feedback edges.
- Implement differential set `distinct` for recursive collections.
- Preserve same-round mutual recursion.
- Handle extensional inputs to recursive targets.
- Advance frontiers only after recursive quiescence.

Exit criterion: randomized cyclic graph updates, including deletions and multiple paths, match
complete recomputation after every epoch with no negative settled support.

### Stage 5: weighted packed execution and `wgpu`

- Add packed differences beside immutable value columns.
- Prevent pre-consolidation set canonicalization.
- Route eligible delta/full equality joins through `RelationAccelerator`.
- Reuse encoded immutable trace batches.
- Retune placement thresholds and retain CPU fallback.
- Pass the configured execution context into committed maintenance.

Exit criterion: accelerated and native weighted execution produce identical consolidated changes,
and at least one representative large workload improves end-to-end time including encoding,
submission, readback, materialization, and consolidation.

**Implementation decision, 2026-07-19:** The first hardware implementation proved identical
consolidated changes and showed a 5.8--7.9x improvement for the raw packed equality join on the
GB10. It did not improve committed differential maintenance by the required 20 percent. After
unchanged maintained collections were changed to share immutable roots, a 4,096-row delta against
258,048 full rows took 9.2 ms through native arrangements and 10.8 ms through warm `wgpu`. One- and
two-column workloads with 4,096- and 65,536-row deltas were either slower through `wgpu` or
effectively tied. The current delivery therefore keeps arranged differential probing native by
default. Weighted packed acceleration remains an explicit execution-context opt-in so its
correctness and hardware path stay testable. The end-to-end performance part of this exit criterion
was deferred to Stage 8, where resident device state could change the cost boundary rather than
merely moving an already indexed probe to the GPU. The Stage 8 measurements below retain native
placement after testing that resident path.

### Stage 6: transaction-local incremental overlays

- Derive private transaction changes from committed maintained state.
- Discard all private changes on retry or abort.
- Preserve computed-relation and unsupported-rule fallback.
- Compare against the current transaction complete evaluator.

Exit criterion: transaction-local derived reads perform less work on representative overlays without
changing conflict, retry, or visibility semantics.

### Stage 7: optional mailbox change subscriptions

Add only after visible-change correctness is established:

- carry each successful settled commit batch to the runtime;
- add runtime registration for catalogue, authoritative-fact, and relation-result subjects;
- support scan-shaped relation filters and mailbox sender capabilities;
- make task registration and cancellation transactional effects;
- implement atomic snapshot-then-changes registration;
- refresh authority before delivery;
- batch delivery by subscription and committed version;
- bound queues and issue explicit resynchronization markers on overflow; and
- wake receivers only after the corresponding snapshot is published.

Exit criterion: mailbox receivers observe complete, authorized, ordered committed-version batches,
never see retry or intermediate-fixpoint output, and can recover from overflow without a
snapshot-to-change gap.

### Stage 8: optional background maintenance and checkpoints

Consider only after synchronous maintenance is correct and measured:

- background catch-up with exact-version waits;
- trace memory budgets and eviction;
- persisted trace checkpoints;
- GPU-side consolidation; and
- resident GPU arrangements with end-to-end weighted maintenance placement measurements; and
- incremental computed relation providers.

Each is an independent optimization, not a requirement for the core design.

**Implementation decision, 2026-07-19:** Stage 8 added an independently keyed resident right-side
GPU arrangement for supported immediate-value equality joins. Changing delta columns no longer
forces the immutable full side to be encoded and sorted again. Cache hit and miss counters, GPU
operator time, and cache evidence are included in the committed-maintenance benchmark.

The resident arrangement removed the previous warm-cache penalty but did not justify GPU placement
for committed maintenance on the GB10. For a 4,096-row delta joining 258,048 full rows with one key
column and one result per delta row, native assertion commits had a 9.653 ms median and resident
`wgpu` assertion commits had a 10.350 ms median, or 0.933x native performance. The measured GPU
operator averaged 0.529 ms with 18 right-side cache hits and no misses during the sampled warm
commits. Increasing the full side to 2,097,152 rows reduced warm `wgpu` performance to 0.830x
native. A two-column, zero-match case reached 0.920x. These measurements include commit publication,
weighted materialization, and consolidation.

The same benchmark found and corrected a quadratic settled-change merge introduced with Stage 7.
Sorting and deduplicating the authoritative and derived change streams restored the approximately 9
ms native baseline without changing public zero-crossing semantics.

Native arrangements therefore remain the default, and weighted GPU maintenance remains an explicit
execution-context opt-in. GPU-side weight arithmetic and consolidation are not added: the resident
GPU operator is already a small fraction of the end-to-end time, so moving checked `i64` arithmetic
to a shader cannot provide the required 20 percent improvement on the measured shapes.

The other Stage 8 options remain deferred independently:

- synchronous maintenance has correct publication semantics and no measured catch-up requirement;
  background work first needs a concrete latency workload and exact-version wait contract;
- traces already compact after eight delta batches or when delta bytes reach one quarter of the base
  and export retained-byte metrics; a global eviction budget first needs consumer pin and unpin
  accounting plus a measured memory limit;
- authoritative recovery and complete recomputation remain correct, with no restart measurement
  justifying the validation and compatibility surface of persisted trace checkpoints; and
- computed relation providers have no committed change-stream contract from which incremental
  maintenance could be driven, so they continue to use the complete evaluator fallback.

## Correctness Verification

### Differential-versus-complete oracle

For every settled epoch:

```text
incremental active-rule output == complete evaluate_fixpoint output
```

Compare canonical tuples relation by relation. Run the same sequence through serial tuple, native
packed, parallel, and `wgpu`-eligible physical paths.

### Required deterministic cases

- one base assertion and one base retraction;
- two independent proofs for one output tuple;
- projection collapsing distinct joined rows to one tuple;
- one rule target with overlapping clauses;
- rule-order independence;
- extensional and derived contributions to the same tuple;
- anti-join changes from both sides;
- lower-stratum changes affecting negated consumers;
- recursive chain growth and shrinkage;
- cycles with the last external support removed;
- multiple recursive atoms;
- same-round mutual recursion;
- rule installation and disable rebuilds;
- unrelated commits producing zero maintained work;
- old snapshots read after later commits;
- dirty transaction assertions and retractions;
- restart followed by reconstruction; and
- accelerator decline, busy, failure, and CPU fallback.

Subscription tests must additionally cover:

- changes-only and atomic snapshot-then-changes registration;
- registration and cancellation commit, retry, and abort behaviour;
- authoritative facts, catalogue changes, and derived relation-result changes;
- no delivery from a retry or abort;
- no delivery before recursive quiescence and snapshot publication;
- no public retraction while another extensional or derived contribution remains;
- increasing cursor order across multiple commit boundaries in one task;
- relation-pattern filtering;
- authority removal between registration and delivery;
- bounded-queue overflow followed by resynchronization; and
- mailbox loss or restart without an exactly-once claim.

### Randomized testing

Generate small relation schemas, safe rule programs from supported shapes, and sequences of valid
set assertions and retractions. After each commit:

1. settle incremental output;
2. recompute from the authoritative snapshot;
3. compare all derived relations;
4. verify no negative settled set support;
5. verify visible changes transform the previous answer into the next answer; and
6. retain selected old snapshots and verify their answers remain unchanged.

Recursive generators must deliberately create cycles and multiple paths. Acyclic graph tests alone
cannot validate recursive deletion.

## Performance Measurement

Measure complete and incremental execution on:

- `Within` containment chains and trees;
- `ReachableRoom` sparse, dense, cyclic, and multi-area graphs;
- capability and authority joins;
- relational router negation;
- high-churn volatile request relations;
- many tiny commits;
- batched commits;
- cold initialization;
- warm steady-state maintenance; and
- large recursive frontiers eligible for `wgpu`.

Record:

- base changes per epoch;
- affected and skipped rule components;
- changes entering and leaving each operator;
- arrangement lookups and rows visited;
- consolidation input and output rows;
- visible zero-crossings;
- recursive iterations and frontier sizes;
- trace batches, retained bytes, and compaction work;
- complete fallback counts and reasons;
- commit latency and derived-read latency;
- subscription matching time, subscriber fanout, message count, and message bytes;
- subscription queue depth, overflow count, and resynchronization count;
- accelerator placement, encoding, execution, readback, and materialization time; and
- CPU/GPU admission declines.

Performance claims must include maintenance overhead on commits that never read a derived relation.
This is why warm-on-demand state is part of the initial policy.

## Risks And Mitigations

### Memory exceeds recomputation savings

Arrangements can duplicate authoritative indexes and retain historical batches.

Mitigations:

- arrange only compiled keys;
- warm only used rule programs;
- share arrangements across operators;
- compact behind retained snapshot frontiers;
- expose trace memory metrics; and
- evict maintained state back to complete evaluation.

### Commit latency becomes unpredictable

Synchronous propagation can move expensive derived work into commit.

Mitigations:

- maintain only warm programs;
- use affected-component scheduling;
- retain physical placement and admission control;
- measure worst-case recursive changes; and
- consider background catch-up only after exact-version blocking semantics are defined.

### Recursive deletion is subtly incorrect

Self-supporting cycles and multiple proofs defeat informal reference counting.

Mitigations:

- use explicit logical time and frontiers;
- keep differential `distinct` semantics centralized;
- compare every stage with complete recomputation;
- generate cyclic deletion workloads; and
- do not ship recursive maintenance before the deletion oracle passes.

### GPU work increases end-to-end latency

Small deltas, encoding, readback, and output fanout can erase kernel wins.

Mitigations:

- keep CPU fallback;
- preserve immutable encoded batches;
- place by delta/full cardinality and provenance;
- include consolidation in measured time; and
- require a whole-operation win before lowering placement thresholds.

### Dynamic rules retain obsolete state

Live rule installation can invalidate compiled operators and traces.

Mitigations:

- identity maintained state by the active rule program and relevant catalogue metadata;
- rebuild on catalogue changes initially;
- retain old state only through snapshots that still reference it; and
- avoid incremental graph mutation until rule-change workloads justify it.

### Weight overflow

High-fanout joins or pathological recursive proof multiplicity can overflow a fixed signed integer.

Mitigations:

- use checked arithmetic;
- fail with a precise kernel error rather than wrap;
- consolidate early where semantics allow; and
- measure proof multiplicity on representative rule sets before choosing the stored width.

### Slow subscribers retain memory or stall commits

One change may match many subscriptions, and one receiver may stop draining its mailbox. Synchronous
delivery inside the commit lock would turn consumer behaviour into commit latency and memory risk.

Mitigations:

- publish one immutable settled batch and match it after snapshot publication;
- bound per-subscription queued bytes and entries;
- enqueue a resynchronization marker on overflow instead of retaining an unlimited backlog;
- expose subscriber fanout, queue depth, and overflow metrics; and
- never wait for a receiver while holding relation-kernel commit state.

## Open Decisions And Recommended Defaults

These are physical and operational decisions rather than changes to Datalog truth. The design needs
initial answers so that the first implementation has one coherent shape, but measurements may change
thresholds and retention limits later.

### 1. Scope and eviction of warm maintained state

**What is at stake:** A single global maintained state is simpler to compile, advance, and compact.
It also means that reading one derived relation can make every active rule consume memory and add
work to every later commit. Independent relation-level state goes too far in the other direction:
relations in a recursive SCC must advance together, and a downstream result cannot advance without
the upstream components on which it depends.

The current compiler already identifies positive dependency SCCs and orders them within negation
strata. Those structures are the natural starting point, although stratified negative dependencies
mean that the runnable maintenance unit may be a dependency-closed group of SCCs rather than one
isolated SCC.

**Recommendation:** Compile the active rule program once per catalogue identity, but own traces,
arrangements, output, memory accounting, and eviction independently per dependency component. A
consumer of relation `R` acquires a lease on the component containing `R` and the transitive
upstream components required to produce it. Multiple queries and subscriptions share those leases.
An SCC is never partially warm or partially evicted.

An ordinary read may warm a component for reuse. A live subscription pins it. When the final lease
is released, the state becomes evictable under the memory budget; it need not be destroyed
immediately. Catalogue changes invalidate only components whose compiled identity or dependencies
changed, plus their dependants.

**Initial default:** Use component-scoped state with reference-counted consumers and memory-pressure
eviction. Do not expose per-relation annotations or make a first read warm the complete active rule
set. Reconsider coarser global state only if measurements show that component bookkeeping and
duplicated upstream state cost more than the unrelated maintenance work they avoid.

### 2. Synchronous maintenance on commit

**What is at stake:** Synchronous maintenance gives the simplest exact publication rule: version `V`
is not visible until its authoritative and derived states agree. It also adds work to the serialized
commit section. A large recursive deletion, high-fanout join, GPU submission, or complete fallback
can therefore increase tail latency for every writer waiting on the current kernel commit lock.

Asynchronous maintenance shortens the authoritative commit path but introduces a lagging derived
version, wait-or-fallback behaviour on reads, subscription progress tracking, shutdown handling, and
failure recovery. It is not merely moving the same function to another thread.

**Recommendation:** Implement synchronous maintenance first. Run it after conflict validation and
before persistence and snapshot publication, advance only affected warm components, and publish
nothing if maintenance fails. Keep the accelerator's decline-without-waiting admission behaviour; an
occupied device must fall back to native execution rather than stall a commit waiting for GPU
capacity.

Record maintenance duration separately from persistence and base snapshot construction, including
the component, input-change count, output-change count, recursion rounds, and physical placement.
There should be no universal semantic timeout: an absolute limit suitable for an interactive world
may be wrong for a batch control-plane update.

**Initial default:** Synchronous maintenance is acceptable for the correctness stages. It remains
acceptable for a representative application only while its measured commit p95 and p99 stay within
that application's latency objective. Treat either a material regression over the no-maintenance
baseline or repeated maintenance pauses above the application's latency budget as the trigger for
background catch-up. If that happens, move a whole component behind an exact-version fence; do not
publish partially settled results or dynamically weaken snapshot semantics.

### 3. Immutable trace batch representation

**What is at stake:** Reusing `RelationState` for every trace batch would obtain ART and secondary
indexes, but it would encode weighted history as repeated set states and allocate persistent index
nodes for small deltas. Reusing `PackedRelation` unchanged would share its immediate-value columns
with native and `wgpu` operators, but it has set-canonical rows and no differences or logical time.
Always storing both tuple rows and columns also duplicates every immediate `Value` word.

Mica still needs ordinary `Tuple` rows for heap-backed values and existing relation APIs. Packed
columns are useful only for immediate-value operators large enough to justify them. ART remains a
good authoritative point and prefix index but is not automatically the best immutable differential
trace spine.

**Recommendation:** Introduce one canonical immutable weighted batch with the conceptual shape:

```text
TraceBatch
  time interval or settled epoch
  rows: sorted Arc<[Tuple]>
  differences: Arc<[i64]>
  optional lazily built packed columns
  optional arrangements keyed by required column positions
```

Rows with the same tuple and time are consolidated, zero differences are removed, and the
differences array remains aligned with rows. Cloning a `Tuple` already shares its value slice, so
the batch can reuse tuple identity cheaply. Immediate-only packed columns should be created lazily
and retained only when native packed or GPU reuse pays for the extra words. Refactor shared packed
column storage out of `PackedRelation` if necessary rather than keeping two independently encoded
copies.

An arrangement over an immutable batch should map a compiled key to row ranges or row offsets. It
may reuse Mica's radix-key encoding, but it should not require one `VersionedAdaptiveRadixTree` per
small epoch batch. The trace owns a spine of immutable batches and compacts them occasionally. The
settled public output remains a separate `RelationState`, updated only by zero-crossings, so normal
snapshot scans continue to use the existing store and indexes.

**Initial default:** Store tuple rows plus checked `i64` differences. Build only arrangements named
by the compiled program. Build packed columns lazily for immediate-only batches and share them with
`wgpu`. Do not put differences into `Tuple`, add weights to the authoritative ART, or require every
trace batch to carry row and column copies.

### 4. Historical time retained by traces

**What is at stake:** Retaining every epoch and recursive iteration makes debugging and replay easy
but grows without bound. Compacting everything immediately minimizes memory but can repeat merge
work for every small commit and can invalidate state still required by an old transaction.

Once an epoch has settled, recursive iteration numbers have no public meaning. Old snapshots already
own immutable derived `RelationState`s, so they do not need the current trace to reconstruct their
ordinary answers. A later transaction-local overlay may need arrangements for its base epoch; that
snapshot can retain an `Arc` to the exact maintained epoch root it needs.

**Recommendation:** Retain no historical logical time solely because an old snapshot exists. Drop
iteration-level history as soon as its epoch settles. Keep a compacted state through version `V`
plus recent immutable delta batches needed to advance it. Each published maintained epoch is an
immutable reference-counted root; an old snapshot or transaction retains an old root without
preventing the live root from being compacted copy-on-write.

Compaction is a physical decision, not a time-retention guarantee. As starting heuristics, compact
when a trace has accumulated eight uncompacted epoch batches or when their bytes exceed 25 percent
of the compacted base, whichever occurs first. Also compact under the component memory budget.
Measure write amplification before changing these values.

**Initial default:** Correctness requires the current settled root, the root being constructed, and
any older roots still referenced by snapshots or transactions—not a fixed number of historical
epochs. Cursor replay has a separate bounded retention policy and must not keep operator iteration
history alive.

### 5. Cursor replay retention

**What is at stake:** A longer replay window lets a temporarily disconnected subscriber resume
without reading a complete snapshot. Every retained commit and visible-change batch consumes memory,
and derived batches are reconstructable caches rather than durable log records. Treating cursors as
permanent would quietly turn the in-memory subscription facility into a durable change-data-capture
system.

The current `CommitHistory` is an unbounded linked history reachable from the newest snapshot. It is
useful current machinery but should not define the subscription service-level guarantee. In
particular, authoritative commits may be recoverable from a provider while the corresponding settled
derived-change batches are not persisted.

**Recommendation:** Give the runtime a separate, shared, bounded replay buffer of settled commit
batches. Starting defaults should retain at most:

- 10,000 committed versions;
- 15 minutes of wall-clock publication time; and
- 64 MiB of encoded fact, catalogue, and visible-change payloads.

Evict the oldest batch when any limit is crossed. These are host configuration limits, not language
semantics. A cursor includes a runtime generation and committed version. An expired cursor, a cursor
from a previous process generation, or a cursor whose required batch was evicted produces an
explicit `ResyncRequired` result.

**Initial default:** Replay is a bounded convenience within one runtime process and has no
exactly-once or restart guarantee. Consumers needing longer or durable replay must commit an outbox,
event, or audit relation and manage acknowledgement there. Do not retain differential traces merely
to extend mailbox replay.

### 6. Default subscription registration mode

**What is at stake:** Changes-only registration is cheap, but a new subscriber cannot interpret
later retractions or construct a current view unless it already has a matching snapshot and knows
the exact handoff version. Snapshot-then-changes costs more initially but provides a complete,
race-free starting state.

Requiring every caller to choose makes the cost visible but makes the unsafe or incomplete choice
easy to repeat. Hiding the choice entirely makes it hard for a renderer that already holds version
`V` to resume efficiently.

**Recommendation:** Make snapshot-then-changes the user-facing default when no valid cursor is
supplied. A caller with a valid cursor resumes after that cursor. Changes-only without a cursor must
be an explicit advanced mode. The internal Rust API should still use an explicit enum so no runtime
call site inherits a default accidentally.

Large initial results may be split into bounded `SnapshotStart`, chunk, and `SnapshotEnd` messages
that all carry the same cursor. Incremental batches are held behind the end marker. If the initial
snapshot cannot fit within the subscription's delivery budget, registration fails or returns
`ResyncRequired`; it must not silently deliver a partial initial state and continue with changes.

**Initial default:** Snapshot at committed version `V`, then changes beginning at `V + 1`. Supplying
a valid cursor selects resume. Explicit changes-only mode is reserved for consumers that establish
their own atomic snapshot handoff.

### 7. Mailbox subscription queue budgets

**What is at stake:** The current mailbox store uses unbounded `VecDeque<Value>` queues. Sending one
batch per version without accounting lets a stalled subscriber consume unbounded memory. Blocking a
commit until the receiver drains would couple database progress to arbitrary application code. Very
small limits, however, turn ordinary short pauses into repeated full resynchronization.

**Recommendation:** Add subscription-aware accounting before placing a change message into the
mailbox queue. A queued subscription entry needs internal subscription identity and an approximate
encoded byte size so the runtime can remove that subscription's undelivered entries without
disturbing ordinary messages in the same mailbox. Share the immutable settled batch across matching
subscribers; charge per-subscriber filter or envelope state and logical delivered bytes rather than
deep-copying the tuples.

Use these starting limits:

- 256 undrained version batches per subscription;
- 4 MiB of logical queued payload per subscription; and
- 64 MiB of uniquely retained subscription backlog for the runtime.

Crossing either per-subscription limit removes that subscription's undelivered incremental entries
and leaves one `ResyncRequired` marker at the newest safe cursor. A single batch larger than the
byte limit immediately produces the same marker. Crossing the global limit applies this policy first
to the largest or oldest stalled backlog. Commits never wait for queue capacity.

**Initial default:** The limits are runtime configuration with the values above as guardrails, not
language-level knobs. Export queue entries, logical bytes, shared retained bytes, age of oldest
batch, and resynchronization counts. Revisit the defaults using observed pause duration and fanout;
do not raise them merely to hide consumers that never drain.

### 8. Differential weight width

**What is at stake:** Differential weights count internal derivations and can grow much larger than
visible set cardinality. A 32-bit difference saves memory and maps easily to many accelerators but
can overflow under ordinary fanout or recursive proof multiplicity. Arbitrary precision makes
overflow impossible but increases trace size, complicates packed execution, and is not supported by
the current GPU boundary.

**Recommendation:** Define one internal `Diff = i64` and use checked addition, subtraction, and
multiplication everywhere. Never wrap or saturate. Overflow fails the maintenance attempt with a
precise kernel error before persistence or publication. Record the relation, operator, epoch, and
operand magnitudes needed to diagnose it without exposing weights as public Datalog values.

The first GPU stage should keep weight arithmetic and consolidation on the CPU. The existing
accelerator can return matching row pairs; checked `i64` multiplication then occurs while
materializing weighted output. GPU-side weight arithmetic should be added only with an explicit
overflow flag or another verifiable checked scheme.

**Initial default:** Checked signed 64-bit weights. If representative workloads approach the limit,
measure the memory cost of `i128` traces or a targeted wide CPU fallback before changing the stored
format. Do not choose `i128` pre-emptively and do not trade correctness for a smaller GPU-friendly
type.

### 9. Native versus `wgpu` placement for delta/full work

**What is at stake:** Differential joins are often highly unbalanced: a small delta probes a large,
already indexed full relation. Native arrangement lookup then touches only relevant keys. GPU
execution pays encoding, ordering, submission, count readback, pair readback, materialization, and
consolidation costs even when very few delta rows changed. Large resident frontiers can reverse that
trade-off.

The current packed equality-join policy considers acceleration at 262,144 combined rows, normally
requires at least 4,096 rows on the smaller side, and admits extremely unbalanced input at 2,097,152
combined rows. Membership similarly uses a 262,144 combined-row threshold. Those are reasonable
conservative evidence for complete packed queries, but combined rows alone are a poor predictor for
`DELTA x FULL` work. The current equality join cache is also keyed by both inputs, so changing the
delta can repeat full-side ordering or encoding work unless a one-sided resident arrangement is
introduced.

**Recommendation:** Default to native indexed probing for small deltas. For the first differential
GPU policy, require all of the following for an equality join:

- both key inputs are supported immediate-value packed columns;
- the full-side encoding and key order are already cached or reusable;
- delta plus full cardinality is at least 262,144 rows;
- the delta side has at least 4,096 rows; and
- estimated output and readback fit the operator memory budget.

Do not apply the current 2,097,152-row unbalanced exception to differential joins while the delta
has fewer than 4,096 rows. A one-row delta probing two million indexed full rows is a native lookup,
not a GPU job. For differential membership and anti-join, apply the threshold to the changing probe
side as well as total input size; a huge stable right side alone does not justify submission.

Before lowering these floors, add a full-side GPU cache keyed independently by immutable batch
identity and arrangement columns. Benchmark a matrix rather than one crossover point:

- delta rows: 1, 16, 256, 4,096, and 65,536;
- full rows: 4,096, 65,536, 262,144, 2,097,152, and 16,777,216;
- zero, one, moderate, and high matches per delta row;
- one- and two-column keys;
- cold encoding versus resident encoded columns; and
- output materialization and consolidation included.

**Measured initial default:** Retain native arranged probing for committed differential work. Keep
the conservative 262,144/4,096 floor and removal of the extreme-unbalance shortcut in the explicit
weighted-acceleration path used by tests and benchmarks. Do not enable that path by default until
Stage 8 measurements show at least a 20 percent median advantage over native execution without a p95
regression. Keep admission adaptive to device occupancy: a busy or unavailable accelerator declines
immediately and does not affect correctness.

## Expected Code Boundaries

Implementation should preserve the existing crate responsibilities:

- `crates/compiler`: parse and validate ordinary relation rules; no differential syntax initially;
- `crates/relation-kernel/src/rules.rs`: logical rule compilation and complete-evaluator oracle;
- `crates/relation-kernel`: maintained program, weighted changes, traces, arrangements, progress,
  snapshot integration, and native fallback;
- `crates/relation-kernel/src/query.rs` and `batch.rs`: shared physical operators and weighted
  packed execution;
- `crates/relation-wgpu`: optional large-operator acceleration only;
- `crates/runtime`: supply execution context and admission policy and, in the optional subscription
  stage, own registration, authority refresh, filtering, queue budgets, and mailbox conversion;
- `crates/driver`: deliver subscription mailbox batches and wake receivers without participating in
  relation maintenance; and
- commit providers: persist authoritative commits without depending on maintained traces.

The exact module split should follow the first compilable slice. Do not create a broad abstraction
hierarchy before non-recursive maintenance reveals the stable boundary.

## References

- Frank McSherry, Derek Murray, Rebecca Isaacs, and Michael Isard, _Differential Dataflow_, CIDR
  2013: <https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf>
- Differential Dataflow book, introduction:
  <https://timelydataflow.github.io/differential-dataflow/>
- Differential Dataflow book, input changes and logical time:
  <https://timelydataflow.github.io/differential-dataflow/chapter_3/chapter_3_3.html>
- Differential Dataflow book, arrangements:
  <https://timelydataflow.github.io/differential-dataflow/chapter_5/chapter_5.html>
- DDlog project overview and language/runtime boundary:
  <https://github.com/vmware-archive/differential-datalog>
- Current Mica rule evaluator:
  [`crates/relation-kernel/src/rules.rs`](../crates/relation-kernel/src/rules.rs)
- Current snapshot-derived cache:
  [`crates/relation-kernel/src/snapshot.rs`](../crates/relation-kernel/src/snapshot.rs)
- Current transaction commit changes:
  [`crates/relation-kernel/src/transaction.rs`](../crates/relation-kernel/src/transaction.rs)
- Current transactional mailbox sends: [`crates/runtime/src/task.rs`](../crates/runtime/src/task.rs)
- Current mailbox delivery:
  [`crates/runtime/src/task_manager.rs`](../crates/runtime/src/task_manager.rs)
- Current packed operators:
  [`crates/relation-kernel/src/batch.rs`](../crates/relation-kernel/src/batch.rs)
- Current accelerator contract:
  [`crates/relation-kernel/src/execution.rs`](../crates/relation-kernel/src/execution.rs)
- Current Vulkan-backed accelerator:
  [`crates/relation-wgpu/src/lib.rs`](../crates/relation-wgpu/src/lib.rs)
