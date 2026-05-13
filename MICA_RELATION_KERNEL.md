# Mica Relation Kernel

This document sketches the storage and query substrate Mica wants underneath the
language. The target is a live relation layer with enough database-style
seriousness to support durable, transactional worlds without inheriting a
SQL/table/row mental model.

Mica should not start by pretending that objects are rows, or that the whole
system is a SQL database with a different parser. It should also not inherit
mooR's `Domain -> Codomain` relation shape unchanged. Mica needs a live,
transactional, set-theoretic relation kernel with real n-ary relations, tuple
indexes, joins, and rule evaluation.

The kernel's job is to make facts fast, durable enough, and semantically clean:

```mica
Object(#lamp)
Name(#lamp, "brass lamp")
LocatedIn(#lamp, #room)
Delegates(#lamp, #thing, 0)
Method(#m)
MethodSelector(#m, :take)
Param(#m, :actor, #player)
Param(#m, :item, #thing)
```

An object is still just an identity value that appears in facts. The kernel does
not store object records. It stores relations.

## Influences

mooR's database layer has the right live-world feel:

- typed relations
- fast in-memory indexes
- transaction-local working sets
- immutable-ish snapshot roots
- compare-and-swap commit publication
- relation-specific conflict handling
- semantic caches for ancestry, properties, and verbs

But mooR's relation abstraction is essentially:

```rust
Relation<Domain, Codomain>
```

That is a good fit for object attributes, but not enough for Mica. Mica needs
relations that are naturally many-to-many and n-ary:

```text
Delegates(child, proto, rank)
Access(holder, resource, operation, mode)
Param(method, role, restriction)
Edge(graph, from, label, to)
FactSource(fact, authority, time, reason)
```

Mica still needs physical seriousness:

- byte-key indexes
- buffer pool
- swizzled pointers
- B-tree range scans
- WAL/checkpoint/recovery machinery
- query planning and execution

But a full SQL-style planner/executor is probably not the center of Mica. Mica's
hot queries are often small, repeated, role-aware, recursive, and cacheable:

```text
What methods apply to this invocation?
What facts mention this identity?
What is the effective value inherited through delegation?
What objects can this actor see?
What changed in this transaction?
```

RART is the leading candidate for the live index layer because Mica tuple
indexes can be encoded as ordered byte keys with strong prefix structure:

```text
relation_id / arg0 / arg1 / arg2 / fact_id
arg0 / relation_id / arg1 / arg2 / fact_id
selector / role_count / role0 / restriction0 / method
```

Adaptive radix trees give exact lookup, ordered traversal, prefix scans, range
scans, and prefix-aware intersection behavior. RART is especially relevant
because it already has efficient low-cardinality join/intersection behavior, a
copy-on-write versioned tree, and is under our control if Mica needs new index
APIs. Those are natural operations for relations and Datalog-like evaluation, as
long as Mica's index keys are designed to be radix-friendly.

## Design Goals

The kernel should provide:

- base relations as named sets of n-ary tuples
- explicit arity and relation metadata
- functional constraints as constraints, not as the default relation model
- tuple indexes over arbitrary position orders
- transaction-local assert/retract working sets
- snapshot reads with read-your-own-writes
- relation-specific conflict policy
- efficient joins over indexed relations
- materialized and cached derived relations
- enough introspection to build object outliners and live authoring tools
- an eventual path to durable storage using page, log, and checkpoint machinery

The kernel should not require:

- SQL as the primary internal interface
- a row/table mental model for objects
- a general cost-based planner before the semantics are clear
- every query to be lowered through a heavyweight relational algebra tree
- object identity to imply record storage

## Core Model

At the lowest semantic level:

```rust
type RelationId = Identity64;
type FactId = Identity64;

struct Relation {
    id: RelationId,
    name: Symbol,
    arity: u16,
    schema: RelationSchema,
    constraints: Vec<Constraint>,
    indexes: Vec<TupleIndex>,
    conflict_policy: ConflictPolicy,
}

struct Tuple {
    values: Vec<Value>,
}

struct Fact {
    id: FactId,
    relation: RelationId,
    tuple: Tuple,
}
```

A base relation is a set of tuples. A fact id is useful for provenance,
retractions, explanations, and storage identity, but set semantics are defined
by `(relation, tuple)` unless a relation explicitly opts into bag/event
semantics.

This does not require one global physical table of all facts. The logical model
is uniform, but the physical layout can be relation-specific. A hot binary
functional relation, a sparse four-argument policy relation, and an append-heavy
event relation may all use different index layouts.

Relation metadata is itself represented relationally:

```mica
Relation(#LocatedIn)
RelationName(#LocatedIn, :LocatedIn)
Arity(#LocatedIn, 2)
ArgumentName(#LocatedIn, 0, :item)
ArgumentName(#LocatedIn, 1, :container)
Functional(#LocatedIn, {0})
Index(#LocatedIn, {:item})
Index(#LocatedIn, {:container, :item})
```

The kernel may keep a compiled catalog for speed, but the author-facing model is
still relational.

## Values

The relation kernel needs Mica values, not just SQL values:

```rust
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(...),
    String(...),
    Bytes(...),
    Symbol(SymbolId),
    Identity(Identity64),
    List(Arc<[Value]>),
    Map(Arc<[(Value, Value)]>),
}
```

`Identity` should be compact. mooR's object identity model suggests that a
64-bit identity value is a better hot-path representation than a full 128-bit
UUID. External UUIDs can still exist as ordinary values or as facts:

```mica
ExternalId(#lamp, :uuid, uuid("..."))
```

Symbols and identities must be distinct from strings and integers. The kernel
needs those distinctions for indexing, equality, display, authority, and object
neighborhood inspection.

### Runtime Value Representation

The value representation is not an implementation detail to defer. Mica's
relation kernel will store and compare enormous numbers of tuple values. If each
value is too wide, every relation scan, join, index traversal, and materialized
view pays for it in memory bandwidth.

mooR's `Var` is a useful reference point: it uses a 128-bit representation with
an 8-byte tag/meta header and an 8-byte data word. That buys simple full-width
integers, cached metadata for strings/lists/maps, and straightforward pointer
storage, but it doubles the bandwidth of dense tuple operations.

For Mica, the live relation kernel should aim for a compact 64-bit value word:

```rust
#[repr(transparent)]
struct Value(u64);
```

The exact encoding is open, but the Conatus prototype suggests the right family
of design: NaN boxing or another tagged-word representation with immediate
values for the hot scalar cases.

The likely immediate value set:

```text
none/null
false/true
small integer
f64, except reserved NaN patterns
symbol id
identity id
heap reference
```

The important point is that identities and symbols must fit inline. Mica facts
will be full of identities and symbols:

```mica
LocatedIn(#lamp, #room)
MethodSelector(#m, :take)
Param(#m, :item, #thing)
```

If those are heap objects, the relation kernel loses before query planning even
starts.

Heap references should be reserved for values that are naturally indirect:

```text
string
bytes
list
map
large integer, if needed
decimal, if needed
compiled method body
opaque host value
```

This creates a three-layer value story:

```text
runtime Value word      compact 64-bit in-memory representation
heap value arena        storage for strings, lists, maps, large objects
index key encoding      canonical ordered bytes for RART/B-tree indexes
```

The runtime representation and index encoding should not be conflated. A NaN-box
layout is a CPU-local representation. Index keys need a stable, ordered,
portable encoding. For many immediate values the index encoder can be almost
mechanical, but it must still define type order, numeric order, string order,
and treatment of NaN.

Open representation questions:

- Is the small integer range `i32`, tagged `i47`, or something else?
- Is `Identity64` a full 62-bit/63-bit payload, or does it reserve bits for
  value tags?
- Can symbols be 32-bit ids forever, or do they need a wider path?
- How are heap references managed: `Arc`, arena handles, generational handles,
  or GC-managed pointers?
- Are heap references allowed directly in relation indexes, or must compound
  values be interned/canonicalized first?
- What is the equality story for floats, especially NaN and `-0.0`?
- Does portable persistence re-encode every value rather than storing raw
  runtime words?

The first prototype should include microbenchmarks for tuple arrays and index
keys before the rest of the query engine gets too elaborate. A beautiful Datalog
engine built over bloated tuple values will lose to memory bandwidth.

## Relations Are Sets

Mica's default relation semantics are set semantics:

```mica
assert LocatedIn(#lamp, #room)
assert LocatedIn(#lamp, #room)   // no duplicate fact by default
```

Functional relations are constrained set relations:

```mica
functional LocatedIn(item) -> container
functional Name(subject) -> text
```

This means:

```mica
#lamp.name = "brass lamp"
```

can be sugar for replacing the single tuple in a declared functional relation.
It is not generic slot assignment, and it is not record mutation.

Some relations may opt into event or bag semantics:

```mica
event Said(actor, room, text, time)
```

But that should be explicit. Most world state relations should be sets.

## Tuple Indexes

Each relation can have multiple physical indexes. An index is an ordered mapping
from an encoded key to one or more fact ids or tuple payloads:

```rust
struct TupleIndex {
    relation: RelationId,
    positions: Vec<u16>,
    includes_fact_id: bool,
    uniqueness: Uniqueness,
    storage: IndexStorage,
}
```

For a relation:

```mica
Delegates(child, proto, rank)
```

useful indexes include:

```text
Delegates_by_child: child / rank / proto
Delegates_by_proto: proto / child
Delegates_by_tuple: child / proto / rank
```

For object outliners:

```text
SubjectFact: subject / relation / rest...
MentionedIdentity: identity / relation / position / rest...
```

For dispatch:

```text
MethodsBySelector: selector / method
ParamsByMethod: method / role / restriction
MethodsByRoleRestriction: selector / role / restriction / method
```

The kernel should be willing to maintain redundant indexes. Mica's performance
will come from making the common access paths explicit, not from hoping a general
planner rediscovers them every time.

## Radix-Friendly Keys

If the live index uses RART, index keys should be encoded for prefix locality.

Good key properties:

- leading bytes correspond to the most commonly bound query positions
- relation ids and argument tags have fixed-width encodings
- identities and symbols have compact fixed-width encodings
- integers use order-preserving big-endian encodings
- variable-length values use escaping or length schemes that cannot collide with
  tuple separators
- strings preserve lexical grouping when the index needs string range behavior
- type tags are single-byte and participate in ordering
- fact ids appear last when used only to disambiguate duplicates

Example key encodings:

```text
RelTuple:
  rel_id / arg0 / arg1 / arg2 / fact_id

Subject:
  arg0_identity / rel_id / arg1 / arg2 / fact_id

Mention:
  identity / rel_id / arg_position / arg0 / arg1 / ... / fact_id

Selector:
  selector_symbol / method_id
```

RART is especially attractive for:

- prefix scans over bound leading columns
- object-neighborhood scans
- selector-method scans
- delegation child/proto scans
- trie-style intersection joins
- low-cardinality joins where index intersection can avoid broad scans
- versioned snapshots with structural sharing
- experiments with ordered-index join algorithms

RART is less obviously ideal for:

- random exact lookup with no prefix reuse
- tiny relations
- very wide heterogeneous values with expensive key encoding
- full scans where key reconstruction dominates

The kernel should not hard-code RART as the only possible index implementation.
It should define an `Index` trait and allow multiple storage strategies:

```rust
enum IndexKind {
    Hash,
    Rart,
    BTree,
    DenseSmallVec,
}
```

But RART should be the default implementation for the first serious live
relation-store prototype. Other index kinds should be added in response to
measured workloads, not because the design needs symmetry.

## Transaction Model

The live model should be image-oriented and snapshot-published:

```text
current root snapshot
  relation indexes
  derived caches
  catalog snapshot

transaction
  base snapshot pointer
  asserted facts
  retracted facts
  local index overlays
  derived cache writes
  effect outbox

commit
  validate conflicts
  build next relation indexes
  publish root by CAS
  persist commit batch
```

Reads inside a transaction see:

```text
base snapshot - local retractions + local assertions
```

This preserves the authoring feel:

```mica
assert LocatedIn(#lamp, #player)
say("Taken.")
```

The mutation is direct in source code, but the runtime still executes it in a
transaction workspace.

Conflict policies should be relation-specific:

```text
set_insert              concurrent identical assert is fine
set_remove              retract missing fact may be fine or error by policy
functional_replace      conflict if another writer changed the same key
ordered_union           merge independent inserts
counter_add             commutative update
event_append            no conflict except storage failure
custom                  relation-defined merge/check hook
```

This is important because Mica relations will range from ordinary state to event
logs to derived materializations.

## Persistence

The first kernel can be live-memory-first, but it should not paint itself into
an in-memory corner.

A practical sequence:

1. Use immutable in-memory relation indexes and CAS-published snapshots.
2. Persist commit batches to a simple key-value provider.
3. Add checkpoint/snapshot loading.
4. Replace or supplement provider storage with page, B-tree, or WAL machinery
   where needed.

The key principle is that the semantic unit of persistence is a fact change:

```text
commit_id
assert rel tuple fact_id
retract rel tuple fact_id
catalog changes
materialized derived changes
```

The physical provider may store this as key-value entries, log records, pages,
or B-trees. The semantic layer should not depend on SQL rows.

## Query Model

The query kernel should understand relation expressions directly:

```text
scan relation with bound positions
join streams on equality conditions
project tuple positions
filter scalar predicates
union
difference
semi-join
anti-join
recursive fixpoint
aggregate, eventually
```

This is not necessarily a full SQL planner. It is a small relational execution
layer oriented around Datalog/rule evaluation and live object queries.

The central primitive is an indexed relation scan:

```rust
scan(RelationId, bindings: &[Option<Value>]) -> TupleStream
```

The scanner chooses an index whose leading positions are bound. For:

```mica
Delegates(#lamp, proto, rank)
```

the scanner should use `Delegates_by_child`. For:

```mica
Delegates(child, #thing, rank)
```

it should use `Delegates_by_proto`.

## Joins

Mica needs several join strategies. They do not all need to exist immediately,
but the kernel design should leave room for them.

### Index Nested Loop Join

Use when one side is small or already streaming, and the other side has a useful
index.

Example:

```mica
Delegates(o, proto, _)
MethodRestriction(m, proto)
```

For each `proto` from `Delegates`, probe `MethodRestriction_by_proto`.

This will probably be the first and most common join implementation.

### Sort-Merge Join

Use when both sides can produce tuples ordered by the join key.

If RART or B-tree indexes can stream both relations in key order, merge join is
useful for larger derived relations and materialization jobs.

### Hash Join

Use when neither side has the right index and one side is reasonably small.

Hash join is not philosophically special; it is just a fallback strategy. It is
still useful for ad hoc queries and rule bodies whose access pattern was not
anticipated.

### Prefix Intersection Join

Use when two indexes are radix-friendly and the join can be expressed as
intersection over encoded key prefixes.

This is where RART may be better than `imbl::HashMap` or a generic B-tree. A
radix tree can skip whole subtrees when prefixes diverge.

Example shape:

```text
left index:  selector / restriction / method
right index: selector / restriction / invocation_role
```

A prefix-aware intersection can walk both trees and emit only matching prefixes.

This is promising for dispatch, authorization, and Datalog semi-naive joins.

### Ordered-Index Joins

Because RART indexes are ordered, Mica can potentially support more advanced
join algorithms over ordered tuple streams. This includes leapfrog-style and
other worst-case-optimal join families, where the evaluator advances multiple
indexes together over shared variables instead of choosing a single binary join
tree.

This is not a requirement for the first implementation. The important design
constraint is to preserve enough ordered cursor functionality that Mica can grow
in this direction:

```text
seek(key)
next()
next_prefix(prefix)
lower_bound(key)
intersect cursors on variable bindings
```

Worst-case-optimal joins are most relevant for rule bodies with several atoms
sharing variables:

```mica
Triangle(a, b, c) :-
  Edge(a, b),
  Edge(b, c),
  Edge(a, c).
```

Mica's dispatch and permission rules may not look like graph analytics, but they
can still produce multiway joins over low-cardinality role, selector, prototype,
and authority dimensions. Ordered indexes keep that door open.

### Semi-Join

Return rows from the left side that have at least one match on the right.

Useful for Datalog, permissions, and existence checks:

```mica
Visible(actor, obj) :-
  LocatedIn(obj, room),
  CanSeeRoom(actor, room).
```

### Anti-Join

Return rows from the left side that have no match on the right.

This is the execution form of stratified negation:

```mica
EffectiveSlot(o, key, value) :-
  Slot(proto, key, value),
  DelegatesStar(o, proto),
  not HasCloserSlot(o, proto, key).
```

Anti-join must be stratified. Mica should not allow arbitrary recursive negation
without a clear semantics.

### Outer Joins

Outer joins are useful for author queries and inspection, but they are not a
core Datalog primitive. They can come later.

## Datalog And Rules

Rules should compile to a Mica-specific relational plan:

```mica
EffectiveName(o, name) :-
  Name(o, name).

EffectiveName(o, name) :-
  Delegates(o, proto, _),
  EffectiveName(proto, name),
  not HasCloserName(o).
```

The evaluator should support:

- non-recursive rule evaluation
- recursive positive rules through semi-naive evaluation
- stratified negation
- materialized derived relations
- transaction-local derived reads
- incremental invalidation or recomputation

Recursive relations like `DelegatesStar` are central enough that they may
deserve specialized implementations:

```mica
DelegatesStar(o, p) :-
  Delegates(o, p, _).

DelegatesStar(o, p) :-
  Delegates(o, q, _),
  DelegatesStar(q, p).
```

The rule system should expose this as ordinary relational semantics, even if the
runtime uses a cached transitive closure.

## Datalog Implementation Strategies

Mica should not choose one Datalog execution strategy for every use case. The
kernel needs a small set of strategies with a shared logical semantics.

### Bottom-Up Evaluation

Bottom-up evaluation starts from known facts and derives all reachable facts for
a rule or stratum until a fixpoint is reached.

This is the natural fit for:

- recursive derived relations
- materialized views
- global closure relations like `DelegatesStar`
- authorization summaries
- indexes that should be reused by many invocations

For recursive rules, Mica should use semi-naive evaluation rather than repeatedly
joining all known facts:

```text
new_delta = rule(previous_delta, all_facts)
derived = derived union new_delta
repeat until new_delta is empty
```

For example:

```mica
DelegatesStar(o, p) :-
  Delegates(o, p, _).

DelegatesStar(o, p) :-
  Delegates(o, q, _),
  DelegatesStar(q, p).
```

The evaluator should derive only paths affected by the latest delta, not
recompute the whole closure every iteration.

Bottom-up evaluation can be expensive if the query only needs one answer. Its
strength is amortization: once a derived relation is materialized, many queries
can reuse it.

### Top-Down Evaluation

Top-down evaluation starts from a goal and recursively asks what facts or rules
could prove it. This is closer to Prolog-style execution.

This is the natural fit for:

- one-off author queries
- interactive inspection
- narrow dispatch checks
- permission questions about a specific actor/object/action
- derived predicates where full materialization would be wasteful

Example:

```mica
CanSee(#alice, #lamp)?
```

The evaluator should start with the bound values and push them into indexed
scans, rather than deriving all `CanSee(actor, obj)` pairs for the world.

Naive top-down recursion can loop. If Mica uses top-down evaluation for
recursive rules, it needs tabling or memoization:

```text
goal + bindings -> answer table
```

This is essentially demand-driven bottom-up evaluation for the part of the rule
graph reachable from the query.

### Hybrid Evaluation

Mica probably wants a hybrid strategy:

- bottom-up for durable/materialized derived relations
- top-down with tabling for narrow demand queries
- specialized kernels for central closures like delegation
- incremental maintenance for hot derived relations affected by transactions

For example, dispatch may use all of these:

```text
DelegatesStar        materialized or cached bottom-up closure
MethodSelector       indexed base relation scan
Param matching       indexed nested-loop or prefix intersection
Applicable(...)      demand query for one invocation
```

The logical rule should remain the same regardless of execution strategy.

### Stratified Negation

Negation should be stratified. A rule may depend negatively only on relations
that are already fully defined in an earlier stratum.

Allowed:

```mica
VisibleObject(actor, obj) :-
  LocatedIn(obj, room),
  CanSeeRoom(actor, room),
  not HiddenFrom(obj, actor).
```

This is valid if `HiddenFrom` does not depend, directly or indirectly, on
`VisibleObject`.

Rejected or requiring a different explicit semantics:

```mica
Reachable(x) :-
  Node(x),
  not BlockedByReachability(x).

BlockedByReachability(x) :-
  Reachable(x),
  SomeOtherCondition(x).
```

The dependency graph has a cycle through negation, so there is no simple
stratified meaning.

The compiler should assign each derived relation to a stratum:

```text
stratum 0: base relations and positive-only derived relations
stratum 1: rules with negation over stratum 0
stratum 2: rules with negation over strata 0..1
```

Within a stratum, positive recursion is allowed. Negative edges must point to
lower strata only.

### Transaction-Local Rules

Rule evaluation inside a transaction must see the transaction workspace:

```text
base snapshot - local retractions + local assertions
```

This is easy for top-down scans because every scan can consult the local overlay.
It is harder for materialized bottom-up views because the committed
materialization may be stale relative to the transaction's local writes.

The kernel should support at least two modes:

- committed materialization plus transaction-local delta correction
- full demand evaluation against the transaction view

For hot closures like `DelegatesStar`, the first mode matters. A transaction
that adds one delegation edge should not have to rebuild the entire world
closure merely to answer one dispatch query.

### Incremental Maintenance

Eventually, materialized derived relations should be maintained incrementally
from base relation changes:

```text
assert/retract base facts
compute affected derived deltas
update materialized relation indexes
commit base and derived changes together
```

This is not required for the first prototype, but the transaction log should
record changes in a way that makes it possible. Retractions are especially
important: deleting a base fact may invalidate derived facts that still have
other proofs, so provenance or support counts may be needed for materialized
recursive relations.

### Aggregation

Aggregation should be stratified like negation. An aggregate depends on a fully
defined input relation and produces a higher-stratum relation.

Example:

```mica
InventoryCount(container, count(items)) :-
  LocatedIn(item, container).
```

Recursive aggregation should be deferred until there is a clear semantics.

## Object Neighborhoods

The relation kernel should make object inspection cheap. An outliner is not
looking up a record; it is querying the fact neighborhood of an identity.

Useful system views:

```mica
SubjectFact(subject, relation, rest...)
MentionedFact(identity, relation, position, tuple...)
IncomingFact(target, relation, subject, rest...)
RelatedMethod(identity, selector, method, reason)
EffectiveFact(subject, relation, tuple...)
```

These should be backed by indexes, not by scanning all facts.

For a MOO/Self-style developer, this is the replacement mental model for "open
the object." Opening `#lamp` means asking the kernel for the relevant facts in
which `#lamp` participates, grouped and rendered as an object neighborhood.

## Dispatch As A Query

Method dispatch should be formulated as relation evaluation over invocation
roles and method signatures.

An invocation supplies a selector and a role environment:

```mica
#lamp:take(actor: #alice)
```

Desugared shape:

```mica
Invoke(selector: :take, item: #lamp, actor: #alice)
```

Candidate methods are facts:

```mica
Method(#m)
MethodSelector(#m, :take)
Param(#m, :actor, #player)
Param(#m, :item, #thing)
DispatchMode(#m, :best)
```

Applicability is a derived relation:

```mica
Applicable(invocation, method) :-
  InvocationSelector(invocation, selector),
  MethodSelector(method, selector),
  AllParamsMatch(invocation, method).
```

This query will be hot. It should not depend on a generic SQL planner. It wants
specific indexes over selector, method params, role restrictions, and delegation
closure.

## Relation Catalog And Physical Design

Every relation should carry logical and physical metadata:

```mica
Relation(#Delegates)
RelationName(#Delegates, :Delegates)
Arity(#Delegates, 3)
ArgumentName(#Delegates, 0, :child)
ArgumentName(#Delegates, 1, :proto)
ArgumentName(#Delegates, 2, :rank)

Index(#Delegates, #idx_delegates_child)
IndexPositions(#idx_delegates_child, {0, 2, 1})
IndexKind(#idx_delegates_child, :rart)

Index(#Delegates, #idx_delegates_proto)
IndexPositions(#idx_delegates_proto, {1, 0})
IndexKind(#idx_delegates_proto, :rart)
```

Some indexes are author-visible tuning knobs. Others are kernel-required.

The catalog should distinguish:

- logical relation declarations
- constraints
- indexes
- materialized derived relations
- cached views
- private kernel relations

## Relationship To mooR

mooR remains the closest conceptual ancestor for the live image model.

Reusable ideas:

- compact object identity
- relation-specific conflict resolution
- transaction working sets
- CAS-published root snapshots
- read-only cache publication
- specialized ancestry/property/verb caches
- batched persistence

Needed changes:

- n-ary set relations instead of `Domain -> Codomain`
- general tuple indexes
- joins and rule evaluation
- relation metadata as first-class world data
- object neighborhoods instead of object records
- dispatch as a relational query rather than parent-slot lookup

## Open Questions

Important unsettled design questions:

- Should the live index use RART's versioned tree directly, or should Mica use
  a custom snapshot wrapper around unversioned indexes?
- How much should tuple indexes store: fact ids only, full tuple payloads, or
  relation-specific compact payloads?
- Are retractions by tuple enough, or do authoring/debugging tools require
  stable fact ids everywhere?
- Should materialized derived relations be stored as ordinary base relations
  with provenance, or as a separate cache tier?
- How much of query planning should be rule-compiler driven versus adaptive at
  runtime?
- What is the first persistence provider: keyspaces, pages, or a simple append
  log plus snapshot?
- Which relations are private kernel state and which are author-visible facts?

## Suggested Prototype

The first prototype should be small and explicitly not a SQL database:

1. Define `Value`, `Identity64`, `RelationId`, `Tuple`, and `FactId`.
2. Implement an in-memory `RelationStore` with n-ary set semantics.
3. Add tuple indexes over selected position orders.
4. Build the initial live indexes on RART with radix-friendly encoded tuple keys.
5. Implement transaction working sets with assert/retract and read-own-writes.
6. Implement indexed scans and index nested loop joins.
7. Implement `SubjectFact` and `MentionedFact` views.
8. Implement `DelegatesStar` as a materialized or cached derived relation.
9. Implement one dispatch query over selector, params, and delegation closure.
10. Add persistence only after the access patterns are visible.

This keeps the first implementation honest. If Mica's real workload demands a
heavier storage layer, that will become clear from concrete indexes and commit
patterns rather than speculation.
