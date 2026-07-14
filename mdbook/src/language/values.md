# Values

Mica values are the things code can compute with, pass to verbs, put in lists and maps, and store in
relation tuples. Some values are ordinary data, some are durable references into the world, and some
are ephemeral runtime authority.

Current value families include:

- `nothing`;
- booleans: `true`, `false`;
- integers;
- floats;
- strings;
- symbols such as `:look`;
- error codes such as `E_FAIL`;
- identity values such as `#alice`;
- lists such as `[1, 2, 3]`;
- maps such as `{:name -> "lamp"}`;
- frobs such as `#event<{:actor -> #alice}>`;
- bytes;
- ephemeral capability values.

The value layer is intentionally small and regular. Relations can store any persistable value, and
verbs can accept ordinary values, identities, or frobs through the same role-binding mechanism. The
language should not force authors to turn every structured value into a durable object just so it
can be passed around.

Not every value family has a source literal. Bytes and capability values are created by builtins,
host interfaces, or runtime operations. Capability values are deliberately not persistable source
values.

Primitive values behave like values in most dynamic languages:

```mica
42
true
"brass lamp"
:look
E_PERMISSION
[1, 2, 3]
{:name -> "lamp"}
```

`nothing` is the absence/sentinel value used by the language. It is still an ordinary value when
placed in a list, map, frob payload, or relation tuple. It does not mean SQL `NULL`, and comparisons
involving `nothing` do not use three-valued logic.

Symbols are interned names used for selectors, relation names, policy surfaces, message tags, and
other program-facing labels:

```mica
:look
:tool_call
:inspection
```

Error codes are also values. By convention, error-code literals begin with `E_`:

```mica
E_PERMISSION
E_NOT_FOUND
```

Errors can be raised and recovered by the error-handling surface, but the code itself is still a
value. Mica does not require a closed universe of built-in error names.

Lists are ordered sequences:

```mica
["coin", "box", "lamp"]
```

Maps are associative values:

```mica
{:actor -> #alice, :item -> #coin}
```

Maps remain useful even in a relation-first language. Relations are for world state and queryable
facts. Maps are for local structured values: role maps, options, decoded messages, frob payloads,
and temporary results. A map can be stored in a relation tuple, but doing so usually means the
relation cannot query inside that map without additional derived facts or host support.

Identity values are different. `#alice` is not the contents of Alice, and it is not a pointer to a
hidden Alice structure. It is a stable key-like value that can appear in relations:

```mica
Actor(#alice)
Name(#alice, "Alice")
LocatedIn(#alice, #first_room)
```

An identity is also not the primary key of one privileged object table. It can appear in many
relations, sometimes in key-like positions and sometimes as an ordinary referenced value.

This is why identity and equality are separate concerns. Two values can be equal because they are
the same integer or string. Two identities are equal when they are the same identity value. Whether
two identities describe equivalent domain entities is a modelled relationship, not something baked
into the value representation.

For example:

```mica
EquivalentAgent(#planner_v1, #planner_v2)
SamePerson(#alice_account, #alice_profile)
```

Those are domain claims. They do not merge identity values at the runtime level.

Frobs are delegated values. They carry a delegate identity plus a payload and can participate in
dispatch without becoming durable world objects.

Frob delegation is value-level interpretation. It is separate from prototype delegation between
durable identities, which is used by role matching and dispatch.

Capability values are runtime authority tokens. They may appear while a task is running, but they
are not persistable source literals and should not be treated as durable policy.

## Numeric Values

Mica has two numeric value families: integers and floats.

- Mica `Int` is a 56-bit signed integer (`-2^55 ..= 2^55 - 1`).
- Mica `Float` is a finite IEEE-754 binary32 (single-precision) value. Infinity and NaN are not
  valid Mica values; operations that would produce them return `E_ARITH` or `E_DIV` instead.
  Negative zero canonicalizes to positive zero.

Mica `Float` has less integer precision than Mica `Int`. Binary32 can represent every integer up to
`2^24` exactly, but above that some integers round to the nearest representable float. A 56-bit Mica
integer always carries more precision than a binary32 float.

### Numeric Equality And Key Identity

Mica has two distinct comparison concepts:

1. **Language numeric comparison** is used by `==`, `!=`, `<`, `<=`, `>`, `>=` in expressions and by
   relation rule guards. When both operands are numeric, integers and floats compare by numeric
   value rather than by value kind. Therefore `1 == 1.0` is true.

2. **Canonical value order** is the total, type-sensitive order used by maps, relation tuples,
   indexes, hashing, and persistence. An integer and a float remain distinct stored values even when
   numerically equal, and may be distinct keys.

```mica
1 == 1.0          -- true: language numeric equality
{:1 -> "int", 1.0 -> "float"}  -- two entries: canonical key identity
```

### Division Result Kinds

Division follows a result-kind rule:

- `4 / 2` produces `Int(2)` because integer division is exact.
- `4 / 2.0` produces `Float(2.0)` because a float operand produces a float result.
- `5 / 2` produces `Float(2.5)` because integer division is not exact.

The numeric values `4 / 2` and `4 / 2.0` are numerically equal (`2 == 2.0` is true), but the
resulting values have different kinds and occupy different map or relation keys.

### Structural Sorting

Structural sorting uses canonical value order, so it can distinguish and order numeric values that
language equality considers numerically equal. A sorted list may place `1` before `1.0` even though
`1 == 1.0`.
