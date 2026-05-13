# Mica Standard Library Notes

These notes sketch the first layer above the kernel: conventions and library
relations that make Mica usable as a live authoring system without hardcoding a
traditional object model back into the runtime.

The standard library should provide:

- object neighborhood and outliner views;
- relation visibility tiers for runtime-private state;
- reusable effective-property policies.

These are not all kernel primitives. They are standard shapes that authors,
tools, and implementations can agree on.

## 1. Object Neighborhood Views

An object identity is not a record, but authors still need a way to inspect
"the object." Mica should define a family of standard views rather than one
overloaded object dump.

### 1.1 Subject Facts

`SubjectFact(subject, atom)` is the narrow object view: facts where the identity
is the first, key-like argument.

```mica
SubjectFact(subject, Atom(relation, args)) :-
  BaseFact(Atom(relation, args)),
  args[0] = subject,
  Readable(CurrentAuthority(), relation, args).
```

For `#lamp42`, this might include:

```mica
Object(#lamp42)
Name(#lamp42, "brass lamp")
LocatedIn(#lamp42, #room17)
Delegates(#lamp42, #thing, 0)
Portable(#lamp42, true)
```

This is the closest equivalent to opening an object in a MOO browser. It is a
computed view, not a storage boundary.

### 1.2 Effective Facts

`EffectiveFact(subject, atom)` shows values after declared inheritance or
composition policies have been applied.

```mica
EffectiveFact(subject, Atom(Name, [subject, value])) :-
  EffectiveName(subject, value).

EffectiveFact(subject, Atom(Lit, [subject, value])) :-
  EffectiveLit(subject, value).
```

The standard outliner should distinguish local facts from effective facts:

```text
#lamp42
  local
    Name: "brass lamp"
    LocatedIn: #room17
  effective
    Portable: true          inherited from #thing
    Description: "A thing." inherited from #thing
```

This avoids pretending inherited state is physically stored on the object.

### 1.3 Incoming Facts

`IncomingFact(target, atom)` shows facts where the identity appears somewhere
other than the first argument.

```mica
IncomingFact(target, Atom(relation, args)) :-
  BaseFact(Atom(relation, args)),
  Contains(args[1..], target),
  Readable(CurrentAuthority(), relation, args).
```

For a room, this could show contained objects:

```mica
LocatedIn(#coin, #room17)
LocatedIn(#alice, #room17)
```

Incoming facts are often essential for world authoring, but they should not be
confused with subject facts. They answer a different question: "Who refers to
this identity?"

### 1.4 Related Methods

Methods are independent identities, so an object browser needs a behavior view
defined by query, not containment.

```mica
RelatedMethod(subject, method) :-
  Method(method),
  Param(method, _, subject).

RelatedMethod(subject, method) :-
  Method(method),
  Param(method, _, proto),
  DelegatesStar(subject, proto).
```

For a prototype, this shows methods that mention it directly. For an instance,
it shows methods currently applicable through delegation.

The browser should label why a method is related:

```text
#brass_key behavior
  get       #thing_get        item matches #thing
  unlock    #unlock_with_key  instrument matches #key
```

### 1.5 Outliner Composition

The standard outliner can combine these views:

```text
Outliner(subject) =
  SubjectFacts(subject)
  EffectiveFacts(subject)
  IncomingFacts(subject)
  RelatedMethods(subject)
  Versions(subject)
  Permissions(subject)
```

But tools should keep the sections distinct. A relation tuple shown in
`IncomingFacts` does not become a field on the object. A method shown in
`RelatedMethods` is not stored inside the object.

All views are authority-filtered. Two users may inspect the same identity and
see different neighborhoods.

## 2. Relation Visibility Tiers

Mica is relational, but not every relation can be ordinary enumerable world
state. Some facts are part of runtime execution and must be supplied by the
kernel as protected views.

### 2.1 Public World Relations

Public world relations are ordinary persisted relations, subject to normal read
and write authority.

Examples:

```mica
Object(#lamp42)
Name(#lamp42, "brass lamp")
LocatedIn(#lamp42, #room17)
Delegates(#lamp42, #thing, 0)
```

Authors can query these relations when authorized, define rules over them, and
mutate them with `assert` and `retract` when authorized.

### 2.2 Protected System Relations

Protected system relations are persisted or derived facts whose existence is
part of the world model, but whose mutation is restricted to trusted library or
kernel code.

Examples:

```mica
ActiveVersion(#method, version)
CompiledPlan(#method, version, plan)
RelationSchema(Name, schema)
Functional(Name, [object])
DotName(:name, Name)
```

Authors may be able to read some of these through normal authority checks, but
they should not casually write them as ordinary world facts.

### 2.3 Invocation-Local Relations

Invocation-local relations exist only for the duration of a command evaluation.
They are supplied by the runtime and are not globally enumerable.

Examples:

```mica
CurrentInvocation(inv)
CurrentActor(actor)
Authority(inv, cap)
CurrentTransaction(tx)
```

These relations let ordinary Mica rules talk about the current command without
turning runtime state into persistent world state.

### 2.4 Kernel-Private Relations

Kernel-private relations are not directly visible to user code. They may be
used internally to implement transactions, scheduling, connection handling, or
capability possession.

Examples:

```text
TransactionWriteSet(tx, atom)
TransactionSnapshot(tx, timestamp)
ConnectionSecret(connection, token)
HeldCapability(inv, cap)
```

The last example is intentionally not public. Capability possession is
designation plus authority, not a fact ordinary code can enumerate. User code
may see an invocation-local `Authority(inv, cap)` view, but it must not be able
to ask for all held capabilities in the system.

### 2.5 Outbox Relations

External side effects should be represented as committed outbox facts, not
performed during speculative transaction attempts.

```mica
assert Effect(:notify, connection, payload)
```

After commit, trusted runtime code drains committed effects. Failed or retried
attempts do not leak output or duplicate external actions.

The standard library should distinguish durable event facts from external
effect requests:

```mica
Event(:lit, actor, target)
Effect(:notify, connection, payload)
```

`Event` is world history or dispatch input. `Effect` is a request for trusted
runtime action after commit.

## 3. Effective Property Policies

Delegation is not a universal fallback. Each property-like relation needs an
explicit policy. The standard library should provide reusable policy builders so
authors do not hand-write `EffectiveName`, `EffectiveLit`, and similar rules for
every property.

### 3.1 Local First

Local-first is the common prototype property policy:

```text
Use the local value if present; otherwise search delegates in order.
```

Declaration:

```mica
EffectivePolicy(Lit, local_first).
```

Generated relation shape:

```mica
EffectiveLit(obj, val) :-
  Lit(obj, val).

EffectiveLit(obj, val) :-
  not HasLocalLit(obj),
  FirstDelegateWithEffectiveLit(obj, proto),
  EffectiveLit(proto, val).
```

This should be generated or expanded by library tooling, not manually repeated.

### 3.2 Ordered Union

Some relations should accumulate values through delegation instead of selecting
one.

Examples:

```mica
EffectiveAliases(obj, alias)
EffectiveFeatures(obj, feature)
```

Declaration:

```mica
EffectivePolicy(Alias, ordered_union).
```

The outliner should display this as a collection and preserve provenance:

```text
aliases
  "lamp"      local
  "light"     inherited from #thing
```

### 3.3 Error on Conflict

Some properties should be singular, but conflicting inherited values should be
reported rather than arbitrarily resolved.

Declaration:

```mica
EffectivePolicy(Material, error_on_conflict).
```

The effective relation is valid only when at most one visible value is present.
Otherwise the outliner and constraint system can report a conflict.

### 3.4 Required Local

Some relations should never inherit. A missing local value is an error or
absence.

Declaration:

```mica
EffectivePolicy(PasswordHash, required_local).
```

This prevents accidental inheritance of sensitive or identity-specific state.

### 3.5 No Effective Policy

Most relations should not be property-like at all. They are queried directly:

```mica
LocatedIn(item, place)
Permission(cap, op, relation, tuple)
AcousticNeighbor(room, neighbor, attenuation)
```

The standard library should make direct relation use feel normal. Effective
properties are a convenience for object-like authoring, not the foundation of
the language.

## 4. Maps as Values, Not World Shape

Mica can have map values without making maps part of the durable world model.
Maps are appropriate when the structure is local to a computation or belongs to
an external boundary:

```mica
let render_options = [:style -> :brief, :depth -> 2]
assert Effect(:notify, connection, [:body -> text, :format -> :djot])
```

Maps are a poor fit when the system needs to see inside the structure. If state
should participate in dispatch, permissions, constraints, indexing, history,
queries, or outliner views, it should be modeled relationally:

```mica
Lit(#lamp, true)
Color(#lamp, "brass")
```

instead of:

```mica
Slot(#lamp, :state, [:lit -> true, :color -> "brass"])
```

The rule of thumb:

```text
If Mica needs to reason about it, make it a relation.
If only this computation or an external payload needs it, a map is fine.
```

## 5. Standard Library Shape

A first useful standard library might define:

- core identity relations: `Object`, `Relation`, `Method`, `User`;
- structural relations: `Delegates`, `Name`, `Description`, `LocatedIn`,
  `Owner`;
- schema metadata: `Functional`, `DotName`, `RelationSchema`,
  `EffectivePolicy`;
- browser views: `SubjectFact`, `EffectiveFact`, `IncomingFact`,
  `RelatedMethod`;
- authority predicates: `Readable`, `Writable`, `Invokable`, `Grantable`;
- transaction-visible relations: `CurrentInvocation`, `CurrentActor`,
  `Authority`;
- event/effect relations: `Event`, `Effect`.

This library is not the whole system. It is the shared vocabulary that lets
tools, authors, and kernel implementations agree on what a live relational
object world looks like.
