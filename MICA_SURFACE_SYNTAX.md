# Mica Surface Syntax Proposal

This document sketches Mica's human-facing programming syntax. It is not yet a
complete grammar. The intent is to make the language feel familiar to MOO/mooR
authors while supporting Mica's relational dispatch and transactional execution
model.

The guiding shape:

- Algol/Wirth-style keyword blocks: `if ... elseif ... else ... end`;
- MOO/mooR-like expressions, lists, maps, splicing, scatter assignment, and
  receiver-call syntax;
- Dylan/Julia-inspired multimethod signatures and role dispatch;
- no hard semantic split between statements and expressions.

## 1. Design Commitments

### 1.1 Expression-Oriented

MOO separates expressions from statements. Mica should not.

In Mica, every executable form produces a value:

```mica
x = 10
y = if x > 5
      "large"
    else
      "small"
    end
```

Blocks evaluate to the value of their final expression unless control exits
early.

```mica
result = begin
  let x = compute_x()
  let y = compute_y()
  x + y
end
```

Forms used only for effects return a conventional unit value, written here as
`nothing` until the value model is finalized.

```mica
assert Lit(#lamp, true)   // returns nothing
retract Lit(#lamp, _)     // returns nothing
```

### 1.2 Semicolons Are Separators

Mica can preserve MOO's semicolon familiarity without making semicolons the
essence of the language.

```mica
x = 1;
y = 2;
x + y
```

Newlines may separate expressions where unambiguous. Semicolons remain useful
for compact one-line code and fileout stability.

### 1.3 Case Sensitivity

This should be decided explicitly. MOO is case-insensitive for variable names.
Mica likely wants case-sensitive identifiers because relation names, symbols,
and authoring tools benefit from stable spelling.

Proposed convention:

```text
UpperCamelCase  relation names: LocatedIn, EffectiveName
lower_snake     variables and roles: actor, target, destination
#lower_snake    stable identities: #brass_lamp, #room17
:lower_snake    symbols: :into, :text_html
```

## 2. Values

Mica values include primitive values, identities, collections, functions, and
possibly reified atoms.

```mica
17
3.14
true
false
"brass lamp"
:into
E_NOT_PORTABLE
#lamp42
```

### 2.1 Error Codes

Any identifier beginning with `E_` is an error-code literal:

```mica
E_PERM
E_NOT_PORTABLE
E_no_such_exit
```

An error-code literal is not a variable lookup and not a symbol. It is a
distinct value whose payload is the spelling of the code. Mica borrows MOO's
readable `E_` convention without borrowing MOO's fixed predefined error set:
new codes are valid as soon as authors write them. Tooling may warn about
unregistered or non-conventionally-cased codes, but the runtime should treat
the code space as open.

### 2.2 Lists

Use bracketed list literals. This keeps lists in the familiar modern shape
instead of reusing MOO's `{...}` list spelling, which conflicts visually with
blocks, patterns, and dictionary-like data.

```mica
[1, 2, 3]
["red", "green", "blue"]
[]
```

Splice with `@`, following MOO:

```mica
items = ["coin", "key"]
more = [@items, "lamp"]     // ["coin", "key", "lamp"]
```

This is a strong MOO convenience and worth preserving.

### 2.3 Maps

Use dictionary-style map literals with braces and `->` pairs:

```mica
{"name" -> "lamp", "lit" -> false}
{:name -> "lamp", :lit -> false}
{}
```

Maps are immutable value data. They are useful for local computation, options,
external payloads, and small lookup tables:

```mica
let opts = {:style -> :brief, :depth -> 2}
assert Effect(:notify, connection, {:body -> text, :format -> :djot})
```

Maps should not become a shadow object model. If the world needs to query,
dispatch on, constrain, authorize, index, or show a piece of state in an
outliner, that state should usually be a relation:

```mica
Lit(#lamp, true)
Color(#lamp, "brass")
```

not a map hidden inside one fact:

```mica
Slot(#lamp, :state, {:lit -> true, :color -> "brass"})
```

Map update syntax can mirror indexed assignment:

```mica
m[:lit] = true
```

Because primitive collections are immutable values, this assignment updates the
variable or place with a modified copy.

### 2.4 Ranges and Indexing

Use MOO/mooR-style ranges:

```mica
items[1]
items[2..4]
items[2.._]       // `_` as end marker inside ranges
```

Whether Mica remains 1-indexed like MOO/mooR should be a deliberate choice. If
the goal is MOO continuity, use 1-based indexing.

## 3. Variables and Binding

Mica should keep mooR's block-scoped `let` and `const`.

```mica
let count = 0
const max_inventory = 20
```

Assignment is an expression and returns the assigned value:

```mica
x = 17
y = (x = x + 1)
```

Implicit verb-wide variables are useful for MOO compatibility but are probably
the wrong default for Mica. Proposed default:

- `let` introduces mutable lexical bindings;
- `const` introduces immutable lexical bindings;
- assignment requires an existing binding or an assignable place;
- a compatibility mode may allow MOO-style implicit locals.

## 4. Places and Assignment

Assignment targets are places:

```mica
variable = expr
list[index] = expr
map[key] = expr
object.dot_name = expr
```

Dot assignment is available only for declared dot names backed by binary
functional relations:

```mica
#lamp.name = "golden lamp"
```

expands to transaction writes:

```mica
retract Name(#lamp, _)
assert Name(#lamp, "golden lamp")
```

There is no silent fallback from dot assignment to `Slot`.

## 5. Scatter Binding and Assignment

Preserve MOO/mooR scatter assignment, including required, optional, and rest
parts.

```mica
let [first, second] = pair
let [head, @tail] = items
let [item, ?prep = :none, ?destination = nothing] = parsed
```

For function and method parameter lists, the same shape should be available:

```mica
fn summarize(first, ?style = :short, @rest)
  ...
end

summarize("lamp")
summarize("lamp", :long, @extra)
```

Omitted optional parameters use their declared default. If no default is
declared, the omitted value is `nothing`. Rest parameters receive a list, empty
when there are no remaining arguments. `@` splices list values into list
construction and direct function calls.

## 6. Control Forms

Control forms are expressions.

### 6.1 Blocks

```mica
begin
  let x = 1
  let y = 2
  x + y
end
```

### 6.2 Conditionals

```mica
if Lit(#lamp, true)
  "lit"
elseif Portable(#lamp, true)
  "portable"
else
  "ordinary"
end
```

Parentheses around conditions are optional unless needed for parsing. This is a
departure from MOO, but fits a readable Algol-family syntax.

### 6.3 Loops

Loops are expressions returning the last body value or `nothing` if they do not
produce one.

```mica
for item in inventory
  inspect(item)
end

for key, value in properties
  render_property(key, value)
end

while condition
  step()
end
```

Loop labels can follow mooR if needed:

```mica
while search_loop more()
  ...
end
```

Unlike MOO/mooR, indexed or keyed iteration should use key/index first and value
second:

```mica
for index, value in items
  ...
end

for key, value in properties
  ...
end
```

Mica should not preserve MOO's backwards `value, key` iteration order except
possibly in a compatibility mode.

### 6.4 Early Exit

Mica can keep familiar forms:

```mica
return value
break
continue
```

Because the language is expression-oriented, `return` may appear inside
expressions, as mooR allows:

```mica
Valid(actor) || return false
```

## 7. Errors

Error codes are immediate values, but a raised error is a rich heap value:
it carries an error code, an optional human message, and an optional payload
value. Raising an error unwinds activations until a matching handler is found.

```mica
raise E_NOT_PORTABLE, "That cannot be taken.", item

try
  risky()
catch E_PERM as err
  "permission denied"
finally
  cleanup()
end
```

Catch conditions currently match error-code literals. A catch without a
condition catches any error. If a catch names a binding, the binding receives
the rich raised error value, not just the error code.

The preferred binding syntax puts the matched code first:

```mica
catch E_NOT_PORTABLE as err
  err.message
end
```

The older binding-first form is also accepted when a conditional spelling reads
better:

```mica
catch err if E_NOT_PORTABLE
  err.value
end
```

Rich errors expose three builtin fields:

```mica
err.code       // E_NOT_PORTABLE
err.message    // message string, or nothing
err.value      // payload value, or nothing
```

MOO's backtick error-catching expression is powerful but visually strange.
Mica uses a keyword expression form for compact local recovery:

```mica
description = recover item:description()
catch E_PERM => "You cannot see that."
catch E_NOT_PORTABLE as err => err.value
end
```

This is sugar over the same exception machinery as `try`; it is not a separate
result type protocol.

## 8. Functions

Mica should keep mooR's two convenient function forms.

Arrow functions:

```mica
{x, y} => x + y
{item} => item.name
{} => now()
```

Block functions:

```mica
fn calculate_damage(attacker, defender)
  let base = attacker.strength * 2
  let defense = defender.armor
  max(1, base - defense)
end
```

Anonymous block functions:

```mica
fn(x, y)
  if x > y
    x
  else
    y
  end
end
```

Functions are values. They close over lexical variables.

## 9. Calls

### 9.1 Ordinary Function Calls

```mica
length(items)
format_name(actor)
```

### 9.2 Role Calls

Mica's direct dispatch form should expose named roles:

```mica
:move(actor: #alice, item: #coin, destination: #box)
:look(actor: #alice, target: #lamp)
```

This is the canonical surface for invoking a selector with explicit role
bindings.

### 9.3 Receiver Sugar

For MOO familiarity, receiver syntax fills a declared receiver role:

```mica
#box:put(#coin, :into, actor: #alice)
```

If selector metadata declares:

```mica
selector :put
  receiver destination
  positional item, prep
end
```

then the call desugars to:

```mica
:put(destination: #box, item: #coin, prep: :into, actor: #alice)
```

The receiver is syntactic sugar, not a privileged dispatch axis.

### 9.4 Dynamic Calls

MOO allows dynamic verb names with `obj:(expr)(args)`. Mica can preserve the
shape:

```mica
selector = :open
: (selector)(actor: #alice, target: #door)
#door:(selector)(actor: #alice)
```

The exact spacing and parsing of `:(selector)` needs grammar work.

## 10. Relations, Rules, and Queries

Relation atoms:

```mica
LocatedIn(#coin, #room)
Name(#lamp, "brass lamp")
```

Rules use Horn-clause syntax:

```mica
VisibleTo(actor, obj) :-
  LocatedIn(actor, room),
  LocatedIn(obj, room).
```

Inside executable code, bare atoms are query conditions when used in boolean
positions:

```mica
if LocatedIn(#coin, #room)
  ...
end
```

Fact changes are explicit:

```mica
assert LocatedIn(#coin, #box)
retract LocatedIn(#coin, _)
```

## 11. Methods and Verbs

`method` is the explicit behavior definition form. It names the method identity.

```mica
method #move_into :move
  names [:move]
  grammar "{item} into {destination}"
  receiver item
  positional destination
  roles actor: #player,
        item: #portable,
        destination: #container
  mode one
do
  require CanMove(actor, item)
  require CanContain(destination, item)

  retract LocatedIn(item, _)
  assert LocatedIn(item, destination)
end
```

`verb` is authoring sugar for command-facing methods when the author does not
care to name the method identity explicitly.

```mica
verb move(self item: #portable, actor: #player, destination: #container)
  require CanMove(actor, item)
  require CanContain(destination, item)

  retract LocatedIn(item, _)
  assert LocatedIn(item, destination)
end
```

`self` is not magical. It is a lexical alias for the declared receiver role.

Open question: whether `verb` should be limited to filein/fileout and authoring
commands, or whether it is also valid as ordinary top-level program text.

## 12. Requirements and Assertions

`require` checks a condition and aborts the current transaction if it fails.

```mica
require CanMove(actor, item)
require Lit(target, false)
```

It is expression-like and returns `nothing` on success.

Open question: whether failed requirements produce structured errors by default:

```mica
require CanMove(actor, item) else E_PERM("cannot move item")
```

## 13. Transactions

Interactive commands auto-commit. Explicit transaction blocks may be useful for
admin tools and scripts, but the surface form is not settled. Candidate forms:

```mica
transaction
  ...
end

atomic
  ...
end
```

## 14. Filein/Fileout Envelope

Filein/fileout may use outer sugar:

```mica
object #lamp extends #thing
  name = "brass lamp"
  lit = false
end

method #lamp_light :light
  roles actor: #player,
        target: #lamp
do
  target.lit = true
  assert Event(:lit, actor, target)
end
```

The body inside `do ... end` is ordinary executable Mica. The `object` and
outer `method` forms are import/export and authoring syntax that expand to
facts about identities, relations, methods, versions, and source.

## 15. Syntax Still Missing

This proposal does not yet define:

- exact lexical grammar;
- operator precedence table;
- newline versus semicolon insertion;
- comments;
- string interpolation;
- relation schema declaration syntax;
- constraint declaration syntax;
- policy declaration syntax;
- aggregation syntax;
- query result syntax;
- module/package/import syntax, if any;
- exact filein/fileout grammar;
- command grammar definition syntax.

Those should be specified before implementation, but this document establishes
the intended surface feel: MOO/mooR-shaped, Algol-readable,
expression-oriented, and role-dispatch aware.
