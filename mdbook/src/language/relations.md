# Relations

A relation is how Mica records that something is true in the world.

If an ordinary object system would write:

```text
lamp.location = room
lamp.name = "brass lamp"
lamp.portable = true
```

Mica usually writes facts:

```mica
LocatedIn(#lamp, #room)
Name(#lamp, "brass lamp")
Portable(#lamp)
```

This is the core shift. State is not packed into one hidden record behind
`#lamp`. State is a collection of named relationships that can be queried,
derived, indexed, authorized, filed out, and extended independently.

Each relation has a fixed number of positions. `Portable` has one position:
the thing that is portable. `LocatedIn` has two positions: the thing and the
place. `Name` has two positions: the thing and the string used as its name. In
database language, those fixed-position facts are tuples and the number of
positions is the relation's arity, but the practical rule is simpler: every
fact in the same relation has the same shape.

## Relation Semantics

Relations have set semantics. A fact is either present or absent; asserting the
same fact twice does not create two logical copies. There is no meaningful
tuple order inside the relation. When a query returns a list, that list is the
language-level representation of the answer set, not a promise that the logical
relation is ordered.

The positions in a relation are ordinal. `LocatedIn(#coin, #room)` means
position 0 is `#coin` and position 1 is `#room`. The positions do not have
stored column names. Names come from the relation and from how queries bind
variables.

Mica values are ordinary values when stored in relations. `nothing` is a value;
it is not SQL `NULL`, and Mica does not use SQL's three-valued logic.

Create a relation with a builtin:

```mica
make_relation(:LocatedIn, 2)
```

Assert facts into it:

```mica
assert LocatedIn(#coin, #room)
```

That fact says that `#coin` is located in `#room`. Mica does not care whether
`#coin` is a game object, an agent task, a document, or an operational entity.
The meaning comes from the relation and from the rules and verbs that use it.

Query with free variables:

```mica
return LocatedIn(?thing, #room)
```

The `?thing` part is a query variable. The result is a list of binding maps:

```mica
[{:thing -> #coin}, {:thing -> #lamp}]
```

Logically, the query result is a set of answers. In task code, Mica returns
those answers as a list of maps so ordinary code can iterate over them. Do not
write code that depends on result ordering unless the language surface
explicitly promises an order for that operation.

A relation call with no free variables is a predicate test:

```mica
if LocatedIn(#coin, #room)
  return "the coin is here"
end
```

You can also leave multiple positions open:

```mica
return LocatedIn(?thing, ?place)
```

That returns one binding map per matching fact.

Repeated query variables require equality:

```mica
SameRoom(?who, ?who)
```

This only matches facts where both positions contain the same value.

`_` is a wildcard, not a binding:

```mica
LocatedIn(_, #room)
```

It matches any first-position value but does not include that value in the
result.

Functional relations declare key positions and support single-value projection:

```mica
make_functional_relation(:Name, 2, [0])
assert Name(#lamp, "brass lamp")
return one Name(#lamp, ?name)
```

The key-position list is zero-based. In `make_functional_relation(:Name, 2,
[0])`, position 0 is the key. That means the key value `#lamp` determines the
remaining position. The relation behaves like a map from the key tuple to the
non-key values.

Functional relation metadata is a real constraint used by replacement and dot
sugar. It is not just documentation. Code that assigns through a functional
relation replaces the tuple for that key rather than adding another competing
fact.

`one` projects at most one result. It is useful for relations such as `Name`,
where the program expects a single value and should fail loudly if the data is
ambiguous.

If the query produces zero results, `one` returns `nothing`. If it produces
more than one result, `one` raises `E_AMBIGUOUS`. If the single result has
exactly one free variable, `one` returns that variable's value. If the single
result has multiple free variables, the result shape is a binding map.

Dot sugar is only valid for declared functional binary relations:

```mica
return #lamp.name
#lamp.name = "golden lamp"
```

This is convenient, but it is still relation access. The assignment replaces
the `Name(#lamp, value)` fact for the key `#lamp`; it does not write a field
inside a record.

The dot name maps to a declared binary relation. A read such as `#lamp.name`
is equivalent to a single-result projection such as `one Name(#lamp, ?name)`.
If there is no matching tuple, the result is `nothing`. If more than one tuple
matches a non-functional backing relation, the read raises `E_AMBIGUOUS`.
There is no fallback to hidden object storage.

Mica relation calls are closer to Datalog predicates than SQL `SELECT`
statements. Relations have no implicit row ids, no implicit column names, and
no SQL `NULL`. Query variables and binding maps are the bridge between the
logical relation and ordinary Mica values.
