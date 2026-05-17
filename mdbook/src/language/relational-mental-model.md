# Relational Mental Model

Mica uses relational ideas, but you do not need to start with database theory.
The practical model is small:

| Term | Working meaning |
| --- | --- |
| identity | a durable key-like value, such as `#alice` or `#task42` |
| fact | one thing the world currently says is true |
| relation | a named collection of same-shaped facts |
| query | a pattern matched against facts |
| query variable | a marked hole, such as `?item`, that asks Mica to return bindings |
| binding map | one answer to a query, such as `{:item -> #lamp}` |
| rule | a named computed query that derives new facts from other facts |
| transaction | a private draft of world changes that commits atomically |

For example:

```mica
LocatedIn(#alice, #room)
LocatedIn(#lamp, #room)
```

These are two facts in the `LocatedIn` relation. They have the same shape:
something is located in some place.

A query can ask Mica to fill one position:

```mica
LocatedIn(?thing, #room)
```

The result is a list of binding maps:

```mica
[{:thing -> #alice}, {:thing -> #lamp}]
```

`?thing` binds a value and returns it in the answer. `_` is different: it is a
wildcard. It matches a value but does not bind or return it.

```mica
LocatedIn(_, #room)
```

That asks whether anything is in `#room`, but it does not name the thing.

Rules use bare variable names instead of `?` variables:

```mica
SameRoom(left, right) :-
  LocatedIn(left, room),
  LocatedIn(right, room)
```

Inside a rule, `left`, `right`, and `room` are logical variables. They are not
local variables assigned step by step. Mica searches for values that make the
body true and then derives matching `SameRoom(left, right)` facts.

Transactions are the runtime side of this model. A task runs against a private
draft of the world. `assert` and `retract` change that draft. Commit publishes
the draft; abort or retry throws it away.
