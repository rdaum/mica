# Mica Semantics Notes

This document provides technical notes on the logic, dispatch, and execution model of the Mica kernel.

## 1. Relational Data Model

Mica's world state consists of indexed **Ground Facts** and **Derived Rules**.

### 1.1 Ground Facts vs. Executable Change Forms
- **Ground Fact:** A tuple present in a relation, such as `Name(#lamp,
  "gold")`.
- **Condition:** An atom in a rule or query: `Name(obj, "gold")`.
- **Executable Change Form:** Code that writes to the current transaction:
  `assert Name(obj, "gold")` or `retract Name(obj, _)`.

### 1.2 Atoms and Values
- **Identity Values:** Stable, unique keys (`#lamp42`).
- **Primitive Values:** Immutable data (Int, Bool, String, Symbol, open
  `E_...` ErrorCode literals, Time).
- **Collection Values:** Immutable lists and maps used inside computations and
  at external boundaries.
- **Reified Atoms:** For auditing, transaction workspaces, and history, an atom
  `R(v1, v2)` can be treated as a value: `Atom(R, [v1, v2])`.

Maps are value-level structure, not durable world structure. They are appropriate
for options, temporary computed data, and external payloads. If a piece of state
needs relational query, dispatch, constraints, authority, indexing, history, or
outliner visibility, model it as relations instead of hiding it inside a map.

---

## 2. Dispatch Engine

Dispatch derives applicable behaviour identities from a role-bound invocation.

### 2.1 Open Signatures
An invocation `i` provides a set of role-value bindings. Mica uses **Open Signatures**:
- **Missing Roles:** If a method requires a role not present in the invocation, the method is **not applicable**.
- **Extra Roles:** If the invocation contains roles not required by the method, they are **ignored** during matching.

### 2.2 Applicability Logic
A method `m` is applicable to invocation `i` with selector `s` if:
1. `Selector(m, s)` matches `Selector(i, s)`.
2. For every `Param(m, role, matcher)`, there exists a corresponding `Arg(i, role, val)` such that `Matches(val, matcher)`.

```mica
Applicable(i, m) :-
  Selector(i, s), 
  Selector(m, s),
  not MissingParam(i, m),
  not MismatchedParam(i, m).

MissingParam(i, m) :-
  Param(m, role, _),
  not Arg(i, role, _).

MismatchedParam(i, m) :-
  Param(m, role, matcher),
  Arg(i, role, val),
  not Matches(val, matcher).
```

### 2.3 Matchers and Rank Distances
Specificities are used in `best` mode to rank applicable methods. The table
below is a candidate default policy, not settled kernel semantics.

| Matcher Type | Logic | Rank Distance |
| :--- | :--- | :--- |
| **Identity** | `val == matcher` | 0 |
| **Prototype** | `DelegatesStar(val, matcher)` | N (steps in `Delegates` chain) |
| **Domain** | `PrimitiveType(val)` | T (fixed large constant) |
| **Predicate** | `matcher(val)` | P (fixed larger constant) |

### 2.4 Dispatch Result Modes
A selector policy defines how the set of applicable methods is reduced:
- **`one`**: Requires exactly one applicable method; otherwise, an error.
- **`best`**: Selects the method with the lexicographically smallest rank vector.
- **`all`**: Evaluates all applicable methods and collects all results.
- **`emit`**: Event-style dispatch; all matching methods run in the transaction.
- **`fold F`**: Evaluates all methods and combines results using reducer `F`.

---

## 3. Transactions and Authority

Mica's transaction model should be closer to mooR than to a separate
"proposal" language. A command or REPL evaluation runs inside a transaction
over a consistent snapshot of the world. Method bodies execute normally against
that transaction view.

### 3.1 Command Transactions

For an interactive command or REPL/filein chunk:

1. The system parses, lowers, and compiles ordinary Mica source.
2. The runtime starts a transaction with a stable snapshot.
3. Direct code, relation queries, dispatch, and method evaluation read from
   that snapshot plus the transaction's own writes.
4. `assert Relation(args...)` records a fact assertion in the transaction
   workspace.
5. `retract Relation(args...)` records a fact retraction in the transaction
   workspace.
6. On success, the runtime checks authority, constraints, and write conflicts.
7. If commit succeeds, the transaction becomes durable atomically.
8. If commit conflicts, the command may be retried from the beginning.

So `assert` and `retract` are executable change forms. They are not a separate
author-facing proposal calculus. An implementation may reify pending writes for
planning, auditing, or debugging, but that is an internal representation of the
transaction workspace.

### 3.2 Snapshot Isolation

The expected baseline is snapshot isolation with write-write conflict
detection, following mooR's model:

- every command sees a consistent snapshot from the moment its transaction
  starts;
- uncommitted writes are invisible to other transactions;
- commit is atomic;
- if another transaction committed a conflicting write after this transaction's
  snapshot, this transaction fails or retries;
- identical writes may be treated as non-conflicting when the resulting fact set
  is the same.

This prevents dirty reads and lost updates without forcing every command in the
world to run serially.

The trade-off is the usual snapshot-isolation trade-off: write skew is possible
when two transactions read overlapping facts but write different facts. When a
world invariant depends on that pattern, Mica should express it as a constraint
or require an explicit write to a shared coordination fact so conflicts are
detected.

### 3.3 Output, Events, and Effects

User-visible output should be buffered until the transaction commits. If a
command retries, output from failed attempts is discarded. The user sees only
the output from the committed run.

Events can be modelled as facts asserted inside the transaction:

```mica
assert Event(:lit, actor, target)
```

External effects require more care. A method body should not perform an
irreversible external effect before commit if retry would duplicate it. The
usual shape is an outbox relation:

```mica
assert Effect(:notify, connection, payload)
```

After commit, the runtime drains committed effects. Effects are therefore tied
to the successful transaction, not to speculative attempts.

### 3.4 Authority and Object Capabilities

Mica should avoid a MOO-style global ACL table as the foundation of security.
ACLs are still useful as world policy facts, but they should not be the
primitive source of authority. The primitive source of authority is possession
of unforgeable runtime designations: capabilities supplied by the connection,
the current invocation, trusted libraries, or explicit attenuation.

This gives Mica a hybrid shape:

- **Capabilities are operational authority.** A capability is an unforgeable
  runtime value or kernel-private handle. Possessing it lets code attempt a
  class of operation.
- **Policy is relational.** Public or protected relations can describe owners,
  stewardship, groups, zones, object-local policies, grant rules, and audit
  labels.
- **Possession is not a public relation.** Ordinary code must not be able to
  enumerate all capabilities, all holders, or all invocations with a given
  capability.
- **Authority decisions are derived.** Read, write, invoke, grant, and effect
  checks combine the invocation's private capabilities with relational policy.

The crucial distinction is between a capability and a description of a
capability. A world may persist facts such as:

```mica
Owner(#lamp, #alice)
Steward(#room, #bob)
GrantPolicy(#alice, :rename, #lamp)
```

These facts can help derive authority, but none of them is the authority by
itself. The actual ability to act comes from an invocation-local authority
context supplied by the runtime.

Authority is checked in several places:

- **Read filtering:** relation scans, queries, dot reads, outliner views, and
  rule evaluation only see tuples for which the current authority context can
  read.
- **Dispatch filtering:** methods that are not invokable under the current
  authority context are not applicable, even if their selector and role
  restrictions match.
- **Write validation:** at commit, each asserted or retracted tuple is checked
  against the current authority context and the relation's write policy.
- **Grant validation:** capability transfer or attenuation checks that the
  current authority context may create the attenuated authority being passed.
- **Effect validation:** committed outbox effects are checked before the
  runtime performs them.
- **Constraint validation:** the resulting state must satisfy system and world
  constraints, including binary functional arity for dot-sugar relations.

A possible internal shape is:

```text
InvocationCaps(inv, cap)        -- kernel-private
CapKind(cap, kind)              -- kernel-private or protected
CapTarget(cap, target)          -- kernel-private or protected
CapAttenuation(cap, policy)     -- kernel-private or protected

CanRead(inv, relation, tuple)   -- derived authority predicate
CanWrite(inv, op, relation, tuple)
CanInvoke(inv, method, args)
CanGrant(inv, cap_descriptor)
CanEffect(inv, effect)
```

Only the `Can...` predicates should be ordinary author-facing concepts, and even
they may need to be exposed as protected views rather than raw enumerable
relations. The private capability set is runtime state, not world state.

This avoids the main failure mode of modelling capabilities relationally:

```mica
HeldCapability(#alice, cap)
```

If that were an ordinary relation, any sufficiently privileged query path could
turn "who can do what?" into enumerable data, and accidental leakage of `cap`
values would become authority leakage. Capability possession must instead be
designation plus invocation context. User code may be able to ask "may this
invocation do X?" but not "show me every capability in the system."

#### 3.4.1 No Ambient Authority

Method bodies should not gain authority merely because they are code attached
to a trusted object, prototype, relation, or source file. Authority should flow
from the invocation and from explicitly passed or attenuated capabilities.

This is the object-capability answer to the confused-deputy problem. A method
that can rename an object because the caller designated that object and supplied
rename authority is different from a method that can rename any object because
it runs as a privileged owner.

Trusted kernel builtins may need ambient implementation authority internally,
but the surface language should treat that as a kernel boundary. Builtins should
check the caller's authority context before mutating relations, invoking
methods, performing effects, or exposing protected data.

#### 3.4.2 Attenuation and Delegation

Capability transfer should be attenuating by default. A broad capability can
create a narrower capability, but a narrower capability cannot amplify itself.

Examples of attenuation dimensions:

- operation: read, write, invoke, grant, effect;
- relation or method;
- target handle or tuple pattern;
- time or transaction boundary;
- source connection or actor;
- maximum grant depth;
- revocation cell or version.

An attenuated capability is still not just a fact. The world may contain a
grant record or revocation record, but the live ability to use the grant is an
invocation-local designation checked by the runtime.

#### 3.4.3 Authority and Transactions

Authority checks use the same snapshot discipline as ordinary reads, with one
extra rule: a transaction must not use authority that appears only because of
its own speculative writes unless the operation is explicitly designed that
way.

For example, a transaction should not normally be able to:

1. assert `Owner(#lamp, #alice)`;
2. use that uncommitted ownership to rewrite protected state;
3. commit both changes as one operation.

When bootstrapping or administrative transitions need that behaviour, they
should use explicit trusted operations whose authority checks are visible in the
kernel or standard library design.

---

## 4. Property Semantics

### 4.1 Functional Dot Sugar
The current compiler supports `obj.prop` when `prop` maps to a binary relation.
There are two paths:

1. explicit compile-context metadata can map a dot name to a relation;
2. the runner also recognizes the conventional mapping from a lower-case dot
   name to an UpperCamelCase relation, such as `location` to `Location`.

Reads project the second column and require a single result:

```mica
#thing.location
one Location(#thing, ?location)
```

Assignments replace the tuple for the first argument:

```mica
#thing.location = #room
```

The intended schema rule is stricter than the current convenience path: dot
names should be backed by binary relations that are functional for their first
argument. There is no automatic fallback from a dot name to `Slot`.

### 4.2 Effective State (Delegation)
Delegation is not a universal fallback. It is a per-relation policy.
An "Effective Property" is a derived relation that explicitly traverses `Delegates`:

```mica
EffectiveLit(obj, val) :- Lit(obj, val).
EffectiveLit(obj, val) :-
  Delegates(obj, proto, _),
  EffectiveLit(proto, val),
  not HasLocalLit(obj).
```

---

## 5. Execution Model

- **Transactional Consistency:** Interactive REPL commands and user commands run
  in auto-committing snapshot transactions.
- **Atomicity:** A transaction succeeds entirely or not at all.
- **Retry Semantics:** A conflicting command may be retried from the beginning
  against a newer snapshot.
- **Buffered Output:** user-visible output and external effects are emitted only
  from the successful committed attempt.
- **Reified Logic:** the live world state includes method bodies, selector
  policies, and authority rules as facts that can be queried and edited live.
