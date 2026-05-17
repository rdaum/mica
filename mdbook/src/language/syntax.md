# Syntax Quick Reference

This chapter is a compact map of the current surface syntax. Details live in
the topical reference chapters.

## Comments

```mica
// line comment
```

## Bindings

```mica
let value = expression
const fixed = expression
value = value + 1
let [first, ?middle = nothing, @rest] = values

fn add(left, right) => left + right

fn describe(item)
  return item
end
```

## Collections

```mica
[1, 2, 3]
[@prefix, last]
{:name -> "lamp", :portable -> true}
items[2]
items[2.._]
```

`[@prefix, last]` splices the list in `prefix` into a new list before `last`.
`2.._` is an open-ended range used by index operations.

## Relations

```mica
make_relation(:LocatedIn, 2)
make_functional_relation(:Name, 2, [0])

assert LocatedIn(#coin, #room)
retract LocatedIn(#coin, _)

LocatedIn(#coin, #room)
LocatedIn(?thing, #room)
one Name(#lamp, ?name)
```

`[0]` is a zero-based key-position list for a functional relation. `?thing` and
`?name` are query variables that bind returned values. `_` is a wildcard that
matches without binding.

`assert` and `retract` require relation atoms. `require` accepts any boolean
condition:

```mica
require CanMove(actor, item)
```

## Rules

```mica
VisibleTo(actor, item) :-
  LocatedIn(actor, room),
  LocatedIn(item, room),
  not HiddenFrom(item, actor)
```

Rule variables are conventionally bare names such as `actor`, `item`, and
`room`. The current compiler also accepts `?name` in rule atoms, but bare names
are the preferred rule style.

## Control

```mica
if condition
  expression
elseif other_condition
  expression
else
  expression
end

while condition
  expression
end

break
continue

begin
  expression
end

for value in values
  expression
end

for key, value in map
  expression
end
```

## Errors

```mica
raise E_PERMISSION, "denied", item

try
  expression
catch E_PERMISSION as err
  err.message
catch
  "fallback"
finally
  cleanup()
end

recover risky()
catch E_FAIL => nothing
catch => "fallback"
end
```

## Verbs and Dispatch

```mica
verb get(actor @ #player, item @ #thing)
  return true
end

:get(actor: #alice, item: #coin)
#coin:look(actor: #alice)
```

`actor @ #player` is a role restriction. `#coin:look(actor: #alice)` is
receiver-call sugar for a named-role dispatch with `receiver: #coin`; it is not
classic method-table lookup.

## Task Control

```mica
commit()
suspend(1)
read(:line)
let child = spawn :tick(actor: actor()) after 5

let [rx, tx] = mailbox()
mailbox_send(tx, "ready")
let ready = mailbox_recv([rx], 0)
```

## Filein Definitions

```mica
make_identity(:lamp)
make_relation(:Object, 1)
assert Object(#lamp)

verb look(actor, item)
  return item
end
```
