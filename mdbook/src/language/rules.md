# Rules

Rules let the world compute facts from other facts.

Stored facts are useful for direct observations:

```mica
LocatedIn(#alice, #room)
LocatedIn(#lamp, #room)
HiddenFrom(#lamp, #alice)
```

But many useful facts are derived. If Alice and the lamp are in the same room,
and the lamp is not hidden from Alice, Alice can see the lamp. That is not a
fact you want to update by hand every time something moves. It is a rule:

```mica
VisibleTo(actor, item) :-
  LocatedIn(actor, room),
  LocatedIn(item, room),
  not HiddenFrom(item, actor)
```

The part before `:-` is the fact being derived. The indented lines after `:-`
are the conditions that must hold. The variables in the rule are logical
variables, not local imperative bindings. Mica finds bindings that make the
body true and then produces matching `VisibleTo(actor, item)` results.

Rule variables are conventionally written as bare names. This is different
from ad hoc relation queries in task code, where free variables are normally
written with `?`:

```mica
// task-code query
VisibleTo(#alice, ?item)

// rule variables
VisibleTo(actor, item) :-
  LocatedIn(actor, room),
  LocatedIn(item, room)
```

In a rule, a variable is introduced by appearing in the rule body. Mica tries
to find values for the variables that make every body predicate true.

The current compiler also accepts `?name` terms in rule atoms and treats them
as rule variables. Prefer bare names in rules; use `?name` in task-code
queries where the result comes back as a binding map.

Rules are read through the same relation interface as stored facts:

```mica
return VisibleTo(#alice, ?item)
```

If a relation has both asserted facts and rules, reads see the union of stored
and derived facts.

## Rule Safety

Rules must be range-restricted. Every variable in the head must be bound by the
body. Variables used inside a negated predicate must already be bound by
positive predicates.

This is valid:

```mica
VisibleTo(actor, item) :-
  LocatedIn(actor, room),
  LocatedIn(item, room),
  not HiddenFrom(item, actor)
```

`actor` and `item` appear in positive predicates before they are used in
`not HiddenFrom(item, actor)`.

This shape is not valid:

```mica
VisibleTo(actor, item) :-
  not HiddenFrom(item, actor)
```

There is no positive predicate limiting which actors and items should be
considered.

Recursive rules can express transitive relationships:

```mica
Contains(container, item) :-
  In(item, container)

Contains(container, item) :-
  In(inner, container),
  Contains(inner, item)
```

This says that a container contains its direct contents, and also contains
anything inside those contents. The recursive rule gives callers a single
relation to ask "what is inside this?" without manually walking every nested
level.

Positive recursion is evaluated as a set-based least fixpoint: start with the
facts known directly, repeatedly derive new facts, and stop when another pass
would add nothing new. This is the usual finite active-domain Datalog model.

Negation is stratified. `not` means "not derivable in the current snapshot",
not SQL three-valued `NOT` and not absolute truth. A rule may use `not`, but
the rule system must be able to evaluate negative dependencies after the
positive relations they depend on are known.

This kind of mutual negation is not allowed:

```mica
A(x) :-
  not B(x)

B(x) :-
  not A(x)
```

There is no stable order in which to compute `A` and `B`.

Agent-workspace rules use the same machinery. For example, an observation can
be relevant to an agent when it is about a task assigned to that agent:

```mica
RelevantTo(agent, observation) :-
  AssignedTo(task, agent),
  AboutTask(observation, task)
```

Rules are world changes, not offline declarations. They can be installed,
inspected, disabled, and filed out.
