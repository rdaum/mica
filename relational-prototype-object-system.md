# Mica

Mica is a live, multiuser programming system where the persistent world is a
relational database, and "objects" are stable identities described by relations.
Prototype inheritance, slots, methods, verbs, dispatch, permissions, source
text, and history are all ordinary facts or derived relations.

The goal is to combine:

- the live authoring model of LambdaMOO/MOO;
- the prototype extension model of Self/Slate;
- the multimethod style of Julia and Dylan;
- Datalog inspired relational / declarative semantics
- the rigour of relations as the foundation.

## Core Thesis

An object is not a record. An object is an identity, and its attributes are
*projections* from the facts that describe it.

For readers coming from object-oriented implementation models, this identity is
not a pointer to a heap allocation, object header, vtable, hash table, slot
dictionary, or any other single physical structure that "contains" the object.
There may be many indexes and storage structures underneath, but none of them
is the object in the semantic model.

In relational terms, an object identity is a durable key-like value in the
domain of values. It is closest to a surrogate entity key: a stable identifier
that can appear in many relations. It is not itself a row, and it is not the
primary key of one privileged object table that contains all object state.

## Goals

- Support MOO-like live authoring: objects evolve in the world, not primarily in
  source files.
- Use relations / facts / predicates as the semantic foundation.
- Support prototype extension and dynamic delegation.
- Treat dispatch as relational restriction over named invocation roles.
- Allow multimethod behavior without privileged receiver semantics.
- Make ambiguity explicit: dispatch can return all matches, one match, the best
  match, or a reduced result.
- Make permissions, history, rollback, and live editing first-class.

## Non-Goals

- No fixed class hierarchy.
- No assumption that a verb belongs to exactly one receiver.
- No hidden object storage separate from relational state.
- No requirement that the canonical program be stored in source files.

## Identity

Again, identities are simple keys:

```mica
#lamp42
#room17
#user3
#open_method
```

But all meaning is relational:

```mica
Object(#lamp42)
Name(#lamp42, "brass lamp")
LocatedIn(#lamp42, #room17)
Delegates(#lamp42, #portable_thing, 0)
Owner(#lamp42, #user3)
Portable(#lamp42, true)
```

`#lamp42` is the identity. It is a reusable key component in facts about the
lamp. Different relations may have different keys involving that identity:

```text
Name(object, text)              key: object
LocatedIn(object, place)        key: object
OwnedAt(object, owner, time)    key: (object, time)
Delegates(object, proto, rank)  key: (object, proto)
```

The point is to avoid this record-shaped model:

```
Object { 
  id, 
  name, 
  lit, 
  location, 
  owner, 
  prototype 
}
```

and instead use a normalized, factored model:

```mica
Object(id)
Name(id, value)
LocatedIn(id, place)
Owner(id, user)
Delegates(id, proto, order)
```

Objecthood is therefore not a *storage* layout. It is a set of facts about some objects.

## Fact Neighborhoods and Outliners

A MOO or Self programmer expects to inspect an object and see "what it has."
Mica should preserve that experience, but the view is computed. The developer
does not inspect a hidden object record; the developer inspects a fact
neighborhood.

The narrow neighborhood of an identity is the set of facts where the identity is
the first, key-like argument:

```mica
Object(#lamp42)
Name(#lamp42, "brass lamp")
Description(#lamp42, "A polished brass lamp.")
LocatedIn(#lamp42, #room17)
Owner(#lamp42, #alice)
Delegates(#lamp42, #thing, 0)
Portable(#lamp42, true)
```

This is the closest Mica equivalent of opening an object in a MOO browser. The
standard outliner can present it as if it were object-shaped:

```text
#lamp42
  name: "brass lamp"              Name(#lamp42, ...)
  description: "A polished..."    Description(#lamp42, ...)
  location: #room17               LocatedIn(#lamp42, ...)
  owner: #alice                   Owner(#lamp42, ...)
  delegates: #thing               Delegates(#lamp42, #thing, 0)
  portable: true                  Portable(#lamp42, true)
```

That outliner is a view, not the storage model. A broader neighborhood may also
show incoming and non-key references:

```mica
LocatedIn(#coin, #lamp42)
Wants(#alice, #lamp42)
Permission(#cap9, write, Name, (#lamp42, _))
```

The distinction matters. The primary outliner view answers "what facts describe
this identity as their subject?" The broader graph view answers "where does this
identity participate anywhere in the world?"

## Notation Summary

Mica's relational notation is Datalog-inspired.

```mica
Object(#lamp)
```

is a relation atom. It denotes the tuple `(#lamp)` in the `Object` relation.

```mica
LocatedIn(#lamp, #room17)
```

denotes the tuple `(#lamp, #room17)` in the `LocatedIn` relation.

```mica
VisibleTo(user, obj) :-
  LocatedIn(user, room),
  LocatedIn(obj, room).
```

defines a derived relation. The head is true when all body predicates are true.
The period terminates a serialized fact or rule. Commas are conjunction.
Variables are unquoted names like `user` and `obj`; identity values are written
with `#`, and symbols with `:`.

## Program Text Contexts

Mica uses the same predicate-shaped notation in several places, but the context
is not optional. The reader should always be able to tell whether a piece of
text is:

- a serialized world fact;
- a rule or query condition;
- executable Mica code that writes fact changes to the current transaction.

Those are different program contexts. They all target the same relational world,
but they are not interchangeable syntax.

A relation atom is the basic predicate-shaped expression:

```mica
Object(#lamp)
Name(#lamp, "brass lamp")
LocatedIn(obj, room)
```

By itself, this is just an atom: a relation name applied to values or variables.
It is the common notation out of which facts, rule conditions, queries, and
effects are written.

A ground fact is a relation atom with no variables, written as a terminated fact
in fileout/fixture text:

```mica
Object(#lamp).
Name(#lamp, "brass lamp").
```

`Object(#lamp).` says `(#lamp) in Object`. It is useful in exported world
snapshots and test fixtures. It is not, by itself, a live mutation command.

In rules and queries, relation atoms are conditions to match:

```mica
Portable(item) :-
  EffectivePortable(item, true).
```

Here `EffectivePortable(item, true)` is matched; it does not mutate anything.

In a filein/fileout document, ground facts and sugared `object`/`method` forms
serialize desired world state. Filing them in runs an import transaction.

In executable Mica code, including method bodies, REPL commands, admin scripts,
and live authoring commands, fact changes are written one way:

```mica
assert LocatedIn(#coin, #box)
retract LocatedIn(#coin, _)
#lamp.name = "brass lamp"
```

`assert Relation(...)` records a fact assertion in the current transaction.
`retract Relation(...)` records a fact retraction in the current transaction.
Assignment to a declared binary functional relation is sugar for a `retract`
plus an `assert`. Bare relation atoms in executable code are tests or queries,
not mutation.

`retract` is intentionally not called `negate`. Negation is logical:
`not LocatedIn(x, y)` asks whether a fact cannot be proven. Retraction is a
state change: it records removal of currently stored matching facts in the
current transaction.

Object-level operations such as `create` and `recycle` can still exist, but
they are ordinary verbs written in Mica. They are not a second mutation syntax.
A `create` verb might allocate a fresh identity and then `assert Object(child)`;
a `recycle` verb might `assert Recycled(o)` and retract selected reachable
facts according to policy.

So the same visual form has a disciplined interpretation:

```mica
Object(#lamp).        // fileout/fixture: serialized fact
Object(#lamp)         // rule/query/body: condition
assert Object(#lamp)  // executable Mica: transaction write
retract Object(#lamp) // executable Mica: transaction write
```

## Data Model

Mica has primitive values, object identifiers, tuples, and relations.

```mica
Object(o)
Relation(r)
Method(m)
User(u)
```

Base relations are directly stored facts. Derived relations are live definition
objects whose structure is also stored as facts.

```mica
DerivedRelation(#portable_def)
Defines(#portable_def, Portable)
Head(#portable_def, [o])
Clause(#portable_def,
  Portable(o) :-
    DelegatesStar(o, #portable))
ActiveDefinition(Portable, #portable_def)

DerivedRelation(#contains_def)
Defines(#contains_def, Contains)
Head(#contains_def, [container, item])
Clause(#contains_def,
  Contains(container, item) :-
    LocatedIn(item, container))
ActiveDefinition(Contains, #contains_def)
```

The database is the program. Source files and `def` forms are optional
import/export or editing syntax for creating these facts.

## Values, Identities, and Types

Mica distinguishes object identities from primitive values.

```text
identity values:   #lamp42, #room17, #method9, #user3
primitive values:  3, true, "brass lamp", :into, E_NOT_PORTABLE, 2026-05-12T14:00:00
```

Both may appear in relations:

```mica
Name(#lamp42, "brass lamp")
Lit(#lamp42, true)
Weight(#lamp42, 3)
LocatedIn(#lamp42, #room17)
```

Primitive values have value equality:

```text
3 = 3
"foo" = "foo"
```

Object identifiers have identity equality:

```text
#room17 = #room17
#room17 != #room18
```

Only object identities participate in object mutation and prototype delegation
by default. Primitive values are immutable data. The integer `3` is not a
mutable world object unless it is explicitly reified by some object.

```mica
Object(#three_concept)
Represents(#three_concept, 3)
```

Primitive domains are exposed as relations:

```mica
Int(3)
String("brass lamp")
Bool(true)
Symbol(:into)
Time(2026-05-12T14:00:00)
```

These are useful for dispatch and validation, but they are not static object
types. A method signature is a dynamic matcher over the current relational
state.

```mica
verb +(x: Int, y: Int) -> Int
verb describe(x: String) -> Text
verb parse(prep: :into)
```

The boolean value `true` is data. Logical truth is still relational: a query is
true when it yields the empty tuple, and false when it yields the empty relation.
This keeps ordinary data booleans separate from the truth of formulas.

Symbols are distinct from strings. Strings are user-facing text; symbols are
interned values useful for command structure and dispatch.

```mica
Arg(i, prep, :into)
Param(m, prep, :into)
```

The design center is therefore:

```text
stable identities
immutable primitive values
dynamic relational facets
optional static tooling
```

Mica can have static linting or capability analysis, but static types are not the
semantic foundation. Dispatch is late-bound against relations.

## Definitions as Live Objects

A derived relation is not primarily a source-file declaration. It is an object in
the live world.

```mica
Object(#can_move_def)
DerivedRelation(#can_move_def)
Defines(#can_move_def, CanMove)
Head(#can_move_def, [actor, item])
Clause(#can_move_def,
  CanMove(actor, item) :-
    VisibleTo(actor, item),
    Portable(item))
Owner(#can_move_def, #user3)
ActiveDefinition(CanMove, #can_move_def)
```

An authoring surface may let someone type:

```mica
CanMove(actor, item) :-
  VisibleTo(actor, item),
  Portable(item).
```

but that is a command that mutates the world. It creates or updates a definition
object, validates it, compiles it if needed, and changes the active-definition
facts.

```mica
add_rule(name: CanMove,
         head: [actor, item],
         body: { VisibleTo(actor, item), Portable(item) })
activate_definition(CanMove, #can_move_def)
```

This is the same status as editing a MOO verb. The typed text is not canonical;
the live database state is canonical.

## Relational Rule Syntax

The primary notation for facts and derived relations is Datalog-inspired
Horn-clause style.

In fileout or fixture context, a ground fact has the form:

```mica
RelationName(value1, value2, ...).
```

As serialized world state, it records that the tuple is a member of the
relation:

```text
(value1, value2, ...) in RelationName
```

For unary relations, this often looks like a type or class predicate:

```mica
Object(#thing).
Room(#kitchen).
Player(#alice).
```

Semantically, these are just set-membership facts:

```text
#thing in Object
#kitchen in Room
#alice in Player
```

For n-ary relations, the fact relates several values:

```mica
LocatedIn(#lamp, #room17).
Owner(#lamp, #alice).
Portable(#lamp, true).
```

A derived rule has the form:

```mica
Head(args...) :- BodyPredicate1(...), BodyPredicate2(...).
```

The head is derived when the body succeeds. Variables are scoped to the rule.
This is intentionally close to Datalog: facts are relation tuples, rules derive
new relation tuples, and recursive definitions are allowed subject to safety and
stratification rules.

For example:

```mica
CanMove(actor, item) :-
  VisibleTo(actor, item),
  Portable(item).
```

Multiple clauses define the union of their results:

```mica
Reach(o, o, 0).

Reach(o, proto, distance + 1) :-
  Delegates(o, parent, _),
  Reach(parent, proto, distance).
```

Negation is explicit and subject to the language's safety and stratification
rules:

```mica
EffectiveName(o, name) :-
  Delegates(o, proto, _),
  EffectiveName(proto, name),
  not HasCloserName(o).
```

This notation is still an authoring command in a live system. Entering a clause
creates or updates a definition object and associated clause facts. It is not a
return to source files as the canonical program.

Property access is surface sugar over relational application:

```mica
#lamp.portable
```

should normally target a named binary relation:

```mica
Portable[#lamp]
```

or, when used as a predicate:

```mica
Portable(#lamp, true)
```

For ad-hoc extension slots, the same surface form can fall back to `Slot`:

```mica
#lamp.glow_color
```

means:

```mica
EffectiveSlot[#lamp, :glow_color]
```

## Objects

Objects are stable identifiers. They do not contain fields. Fields are facts.
An object identifier behaves like an entity key that can be referenced by many
relations.

```mica
Object(#lamp)
Name(#lamp, "brass lamp")
Lit(#lamp, false)
```

`Object(#lamp)` is not the object record. It is an existence or facet fact about
the identity `#lamp`. The rest of the world describes `#lamp` by using it as a
key-like value in other relations.

Prototype extension is an ordered relation.

```mica
Delegates(#red_lamp, #lamp, 0)
Color(#red_lamp, "red")
```

Changing an object's behavior is a transaction over facts.

```mica
retract Delegates(#shark, #healthy_shark, _)
assert Delegates(#shark, #dying_shark, 0)
```

Two objects may have exactly the same relational facts and still be distinct if
they have different identifiers. This is essential for a multiuser world:
permissions, provenance, history, containment, and references all need stable
endpoints.

For example:

```mica
Object(#room17)
Object(#room18)
Name(#room17, "Empty Room")
Name(#room18, "Empty Room")
```

`#room17 != #room18` even if their current fact neighborhoods are otherwise
identical. Identity is not structural equality.

## Effective State

Local facts and prototype-derived facts are kept distinct.

```mica
Color(#red_lamp, "red")
Delegates(#red_lamp, #lamp, 0)
Lit(#lamp, false)
```

The visible state of an object is derived relation-by-relation:

```mica
EffectiveName(o, name) :-
  Name(o, name).

EffectiveName(o, name) :-
  Delegates(o, proto, _),
  EffectiveName(proto, name),
  not HasCloserName(o).

EffectiveLit(o, lit) :-
  Lit(o, lit).

EffectiveLit(o, lit) :-
  Delegates(o, proto, _),
  EffectiveLit(proto, lit),
  not HasCloserLit(o).
```

Generic slots are reserved for ad-hoc extension data that is not important
enough to deserve a named relation:

```mica
Slot(#lamp, :glow_color, "amber").

EffectiveSlot(o, key, value) :-
  Slot(o, key, value).
```

The exact override policy is not fundamental. The standard library can provide
common policies such as local-first, ordered-delegation, union, or error on
conflict.

## Delegation and Behavior

For MOO and Self programmers, this is the main inversion.

In MOO or Self, behavior lookup starts at a receiver object:

```text
receiver -> lookup selector in receiver/parents -> method body
```

In Mica, behavior lookup starts at the invocation:

```text
invocation roles -> find methods whose params match those roles -> method bodies
```

Prototype delegation still matters, but it is not where behavior physically
lives. A prototype is a stable identity that methods and rules can match
against. A method that says `item: #portable` is applicable to an item when that
item delegates to `#portable`:

```mica
Matches(obj, proto) :-
  DelegatesStar(obj, proto).
```

So the prototype does not "hold" the method in the MOO sense. The method is its
own identity, and the prototype is part of the method's applicability filter.

For example:

```mica
Method(#move_portable_into_container).
Selector(#move_portable_into_container, :move).
Param(#move_portable_into_container, item, #portable).
Param(#move_portable_into_container, destination, #container).

Delegates(#coin, #portable, 0).
Delegates(#box, #container, 0).
```

An invocation with `item: #coin` and `destination: #box` matches that method
because `#coin` is in the `#portable` prototype lineage and `#box` is in the
`#container` lineage.

A dispatch table can make this visible:

```text
selector  method                         role          matcher
move      #move_portable_into_container  item          #portable
move      #move_portable_into_container  destination   #container
move      #wizard_move_anything          actor         #wizard
open      #open_container                target        #container
open      #open_locked_with_key          target        #locked_container
open      #open_locked_with_key          instrument    #key
```

The object outliner may show methods related to `#portable`, but that is a
discovery view. It does not mean the method is stored inside `#portable`.

## Named Relations and Dot Sugar

Core system and domain semantics should use named relations, not generic slots.

Prefer:

```mica
Name(#lamp42, "golden lamp").
Description(#lamp42, "A polished golden lamp.").
Portable(#lamp42, true).
LocatedIn(#lamp42, #room17).
Owner(#lamp42, #alice).
```

over:

```mica
Slot(#lamp42, :name, "golden lamp").
Slot(#lamp42, :description, "A polished golden lamp.").
Slot(#lamp42, :portable, true).
```

Named relations give the system clearer keys, constraints, indexes, authority
checks, and dispatch predicates. `Slot(object, key, value)` remains useful, but
only as an open-ended escape hatch for ad-hoc author data.

For user ergonomics, binary functional relations can have familiar dot
assignment syntax:

```mica
#lamp42.name = "golden lamp"
#lamp42.description = "A polished golden lamp."
#lamp42.portable = true
```

This is sugar for relation updates:

```mica
retract Name(#lamp42, _)
assert Name(#lamp42, "golden lamp")

retract Description(#lamp42, _)
assert Description(#lamp42, "A polished golden lamp.")

retract Portable(#lamp42, _)
assert Portable(#lamp42, true)
```

This should be limited to declared binary relations where the first argument is
the identity and the relation is functional for that identity. It is not record
assignment. There is no hidden object row being updated.

The relation schema must make that promise explicit. For example:

```mica
Relation(Name).
Arguments(Name, [object, value]).
Functional(Name, [object]).
DotName(:name, Name).
```

`Functional(Name, [object])` says there may be at most one `Name(object, value)`
fact for a given object. The storage engine or transaction validator must
enforce that constraint. Without it, `#lamp.name` is not valid dot syntax; the
author must use relation syntax and decide how to handle multiple results.

Dot reads therefore have predictable cardinality:

```mica
#lamp.name
```

means "read the one visible `Name(#lamp, value)`." If the dot name is declared
to read through an effective relation such as `EffectiveName`, that effective
relation must also have a single-value policy. If no value exists, the
relation-specific policy decides whether this is `missing`, inherited, or an
error. If more than one visible value exists, that is a constraint violation,
not a set-valued property read.

Assignment is likewise constrained:

```mica
#lamp.name = "new name"
```

means:

```mica
retract Name(#lamp, _)
assert Name(#lamp, "new name")
```

or the equivalent update against the declared backing relation for that dot
name. It does not update an arbitrary slot dictionary.

Higher-arity relations should usually be written in relation form:

```mica
AcousticNeighbor(#room17, #room18, attenuation: 2, max_depth: 3).
Permission(#cap17, write, LocatedIn, (#lamp42, #room17)).
```

Trying to force all arities through dot syntax would recreate record-shaped
thinking. Dot syntax is an ergonomic surface for the common binary functional
case; relation syntax remains the general form.

## Invocation Syntax

A command or function call creates a role-bound call frame.

For example:

```mica
move #coin into #box
```

may parse to a compact role call:

```mica
:move(actor: #current_user,
      item: #coin,
      prep: :into,
      destination: #box,
      context: #current_room)
```

That call frame is the input to dispatch semantics. It does not need to be
stored as five separate facts just to call a method.

When the system needs persistence, auditing, debugging, or transaction
planning, the call frame can be reified into normalized facts:

```mica
Invocation(i)
Selector(i, move)
Arg(i, actor, #current_user)
Arg(i, item, #coin)
Arg(i, prep, :into)
Arg(i, destination, #box)
Arg(i, context, #current_room)
```

Nothing about `item`, `prep`, `destination`, `actor`, or `context` is built into
the runtime. They are role labels and values. The normalized form is an internal
representation, not the normal authoring syntax.

### Receiver-Call Sugar

For MOO familiarity, Mica can also support receiver-call syntax:

```mica
#bucket:pour_into(500, :ml, #other_bucket)
```

This is sugar for a role call:

```mica
:pour_into(from: #bucket,
           quantity: 500,
           unit: :ml,
           to: #other_bucket)
```

The selector declares which role the syntactic receiver fills and how positional
arguments map to the remaining roles:

```mica
selector :pour_into
  receiver from
  positional quantity, unit, to
end
```

or in a method signature:

```mica
method #pour_liquid :pour_into
  receiver from
  positional quantity, unit, to
  roles from: LiquidContainer,
        quantity: Int,
        unit: VolumeUnit,
        to: LiquidContainer
do
  require ContainsVolume(self, quantity, unit)
  ...
end
```

Inside the method body, `self` is a lexical alias for the declared receiver
role. In the example above, `self` means `from`.

The receiver is syntactically special but not semantically privileged. Dispatch
still matches all roles:

```mica
from: LiquidContainer
quantity: Int
unit: VolumeUnit
to: LiquidContainer
```

So these are equivalent at dispatch time:

```mica
#bucket:pour_into(500, :ml, #other_bucket)
:pour_into(from: #bucket, quantity: 500, unit: :ml, to: #other_bucket)
```

The parser is allowed to be user-extensible. Command grammars can themselves be
represented as facts:

```mica
SyntaxPattern(move, [object, preposition, object])
Preposition(:into)
Preposition(:onto)
Preposition(:through)
```

## Methods

Methods are also objects.

This means a method has its own stable identity, just like a room, lamp, player,
or relation definition. The identity is not the source text and not a function
pointer. It is a key-like value described by relations:

```mica
Object(#move_into).
Method(#move_into).
Selector(#move_into, :move).
Param(#move_into, actor, #actor).
Param(#move_into, item, #portable).
Owner(#move_into, #arch_wizard).
ActiveVersion(#move_into, version_3).
```

That method identity is useful because methods need ownership, permissions,
history, active versions, documentation, dispatch metadata, and references from
other relations. Editing a method changes facts about `#move_into`; it does not
replace an invisible function stored inside a receiver object.

The full creation syntax is `method`. It names the method identity and describes
the behavior object explicitly:

```mica
method #move_into :move
  names [:move]
  grammar "{item} into {destination}"
  receiver item
  positional destination
  roles actor: #actor,
        item: #portable,
        prep: :into,
        destination: #container,
        context: #room
  mode one
  owner #arch_wizard
do
  require CanMove(actor, item)
  require CanContain(destination, item)

  retract LocatedIn(item, _)
  assert LocatedIn(item, destination)
end
```

The signature part creates the method object and its dispatch metadata. The body
creates a method version that compiles to a transaction plan.

```mica
Method(#m1)
Selector(#m1, move)
Param(#m1, actor, #player)
Param(#m1, item, #portable)
Param(#m1, prep, :into)
Param(#m1, destination, #container)
Source(#m1, "...author text...")
```

The shorter `verb` form is only authoring sugar for command-facing methods. It
does not add another semantic category. It is useful when the author does not
care to choose the method identity, owner, command names, receiver role, or
version metadata explicitly.

```mica
verb move(self item: #portable, actor: #player, prep: :into, destination: #container)
  require CanMove(actor, item)
  require CanContain(destination, item)

  retract LocatedIn(item, _)
  assert LocatedIn(item, destination)
end
```

This is sugar for creating a fresh method identity, for example
`#method_7f3a`, with selector `:move`, inferred names and default policy, plus
an active body version.

```mica
Object(#method_7f3a).
Method(#method_7f3a).
Selector(#method_7f3a, :move).
Param(#method_7f3a, item, #portable).
Param(#method_7f3a, actor, #player).
Param(#method_7f3a, prep, :into).
Param(#method_7f3a, destination, #container).
ActiveVersion(#method_7f3a, version_1).
```

A method body runs with role bindings. The surface syntax is MOO-like in
authoring style, and Julia/Dylan-like in its multimethod signature.

This does not mean the method belongs to `item`, `destination`, or `actor`.
The method is applicable to an invocation when its parameter requirements match
the invocation's role bindings.

The parameters are late-bound matchers. They may name object identities,
prototype identities, primitive-domain relations, derived predicates, or literal
primitive values.

```mica
actor: #wizard        // prototype or identity match
item: Portable        // derived relation match
count: Int            // primitive-domain match
prep: :into           // literal symbol match
target: #door17       // identity-specific match
```

## Behavior Discovery

Because methods are independent identities, editing behavior is a search and
outliner workflow, not "open the receiver object and edit the verb stored
there."

The standard browser should provide a behavior view for an identity. For
`#thing`, that view is a query over method metadata:

```mica
RelatedMethod(object, method) :-
  Method(method),
  Param(method, role, object).

RelatedMethod(object, method) :-
  Method(method),
  Param(method, role, proto),
  DelegatesStar(object, proto).
```

For a prototype such as `#thing`, the outliner can show methods that mention it
directly:

```text
#thing behavior
  get        #thing_get        item: #thing
  drop       #thing_drop       item: #thing
  look       #thing_look       target: #thing
  describe   #thing_describe   target: #thing
```

For an instance such as `#brass_key`, the outliner can show methods that would
currently apply through delegation:

```text
#brass_key applicable behavior
  get        #thing_get        item matches #thing
  unlock     #unlock_with_key  instrument matches #key
  describe   #thing_describe   target matches #thing
```

This is the Mica replacement for a MOO verb list. It is not a containment claim;
it is a query over `Method`, `Selector`, `Param`, `Delegates`, and related
relations. Since it is a query, the browser can also pivot by selector, owner,
grammar, role, permission, version, or recent edit.

## Dispatch Semantics

This section is not invocation syntax. It defines the dispatch rules: given a
role-bound call frame, derive which method identities are applicable.

The flow is:

```text
surface call
-> role-bound call frame
-> Applicable(call, method)
-> mode: all / one / best / fold / emit
-> method body evaluation
-> transaction writes and effects
```

Dispatch is a derived relation over the call frame and live method metadata.
The Horn-clause form below is a semantic definition, not the way users call
methods:

```mica
Applicable(inv, method) :-
  Selector(inv, selector),
  Selector(method, selector),
  AllParamsSatisfied(inv, method).

ParamSatisfied(inv, method, role) :-
  Param(method, role, expected),
  Arg(inv, role, actual),
  Matches(actual, expected).

AllParamsSatisfied(inv, method) :-
  not UnsatisfiedParam(inv, method).

UnsatisfiedParam(inv, method) :-
  Param(method, role, _),
  not ParamSatisfied(inv, method, role).
```

The default matching policy is:

- every required `Param(method, role, expected)` must be satisfied by an
  `Arg(inv, role, actual)`;
- if the invocation does not provide a required role, the method is not
  applicable;
- extra invocation roles do not disqualify the method by default;
- a method may opt into a closed signature if extra roles should be rejected;
- optional and defaulted roles must be explicit metadata, not an accidental
  consequence of a missing argument.

For example, this method requires both `item` and `destination`:

```mica
Param(#move_into, item, #portable).
Param(#move_into, destination, #container).
```

An invocation with only `item: #coin` does not match. An invocation with
`item: #coin`, `destination: #box`, and `actor: #alice` can still match; the
extra `actor` role remains available to the body or to other applicable methods.

Closed signatures are explicit:

```mica
ClosedSignature(#exact_open).
```

with the semantic rule:

```mica
ExtraRole(inv, method, role) :-
  Arg(inv, role, _),
  not Param(method, role, _).

UnsatisfiedParam(inv, method) :-
  ClosedSignature(method),
  ExtraRole(inv, method, _).
```

Optional roles are also explicit:

```mica
OptionalParam(#look, instrument).
DefaultArg(#look, instrument, #none).
```

This keeps N-ary dispatch predictable. Adding a new role to an invocation can
enable more specific methods, but it does not silently break older methods
unless those methods ask for closed matching.

`Matches` handles literals, prototypes, predicates, and relation-valued
parameters.

```mica
Matches(x, x).

Matches(obj, proto) :-
  DelegatesStar(obj, proto).

Matches(x, pred) :-
  pred(x).
```

For primitive values, matching is value-based or domain-based:

```mica
Matches(3, 3)
Matches(3, Int)
Matches("lamp", String)
Matches(:into, :into)
```

The primitive result of dispatch semantics is a set:

```mica
Applicable(i) -> {#m1, #m2, #m3}
```

Single-method dispatch is a derived mode, not the foundation.

## Dispatch Result Modes

A selector or method policy declares how applicable methods are interpreted.

```mica
policy move:
  mode one
  order actor, item, prep, destination, context
end
```

Common modes:

```mica
all selector(args)        // return or run all applicable behaviors
best selector(args)       // choose most specific by rank
one selector(args)        // require exactly one applicable behavior
fold selector(args) by F  // combine results explicitly
emit selector(args)       // event-style handlers writing to the transaction
```

Ambiguity is only an error in modes that require uniqueness. In other modes,
multiple applicable behaviors are normal.

For example, describing a locked container may naturally combine several
contributions:

```mica
verb describe(target: #thing) -> Text
verb describe(target: #container) -> Text
verb describe(target: #locked) -> Text
```

If `#chest` is both a container and locked, `all describe(target: #chest)` may
produce multiple facts or text fragments. A separate reducer decides how to
combine them.

## Specificity

Specificity is a relation, not hidden virtual machine magic.

```mica
Rank(inv, method, role, distance)
MethodRank(inv, method, vector)
```

A best method can be derived:

```mica
Best(inv, method) :-
  Applicable(inv, method),
  MethodRank(inv, method, rank),
  rank = min { r | MethodRank(inv, _, r) }.
```

The standard ordering can follow Julia and Dylan by treating earlier roles as
more significant by default. A verb may declare a different role precedence.

```mica
policy open:
  mode best
  order actor, target, instrument, context
end
```

This preserves multimethod specialization while keeping the selection policy
visible and queryable.

## Effects and Transactions

Methods do not make their writes durable immediately. They execute inside the
current transaction. The runtime checks permissions, constraints, and write
conflicts, then commits the transaction atomically.

In executable Mica code, the author writes:

```mica
assert LocatedIn(#coin, #box)
retract LocatedIn(#coin, #room)
```

If the runtime reifies pending transaction writes for planning, auditing, or
authorization, they can be represented internally as:

```mica
Proposal(tx, assert, LocatedIn(#coin, #box))
Proposal(tx, retract, LocatedIn(#coin, #room))
```

This keeps `all` and `emit` dispatch coherent. Multiple behaviors can write to
the same transaction workspace. Conflicts are detected before commit.

For an interactive user, this does not mean "dry run by default." A REPL command
or method invocation normally runs in an implicit transaction that commits before
the prompt returns:

```mica
> #lamp.lit = true
committed tx: 4817
```

Mechanically, the assignment is still planned, checked, and committed:

```text
parse command
record assertions/retractions in transaction workspace
check permissions and constraints
commit
show the new visible state
```

But the authoring loop feels immediate, as in an image-based system. The
transaction workspace is the runtime's way to combine multimethod effects,
enforce constraints, and report conflicts. It is not a separate user-visible
staging area unless the author explicitly asks for one.

Explicit multi-step transaction syntax is still unsettled, but the semantic
intent is:

```mica
transaction
  #lamp.lit = true
  assert Event(:lit, #alice, #lamp)
end
```

The same machinery can support `preview`, `explain`, or `rollback`, but the
default interactive mode is auto-commit.

The command:

```mica
move #coin into #box
```

has this mechanical pipeline:

```text
parse text into Invocation/Arg facts
derive Applicable methods
apply verb policy: one, best, all, fold, or emit
evaluate selected bodies with role bindings
collect transaction writes
check constraints and permissions
commit transaction
record history
```

## Constraints

Constraints are relations that must remain true after commit, or relations that
must remain empty.

```mica
constraint SingleLocation(o):
  count[{place : LocatedIn(o, place)}] <= 1

constraint NoContainmentCycles(o):
  not ContainsPlus(o, o)
```

Constraints are part of the world model, not external validation code.

## Live Evolution

Objects, methods, and derived relations evolve over time. Editing creates facts.

```mica
Source(#m1, version_7, text)
Compiled(#m1, version_7, bytecode)
ActiveVersion(#m1, version_7)
EditedBy(#m1, version_7, #user3)
EditedAt(#m1, version_7, now)
```

Relation definitions use the same versioning shape:

```mica
Source(#can_move_def, version_4, text)
DefinitionAst(#can_move_def, version_4, ast)
CompiledDefinition(#can_move_def, version_4, plan)
ActiveVersion(#can_move_def, version_4)
EditedBy(#can_move_def, version_4, #user3)
EditedAt(#can_move_def, version_4, now)
```

Rollback is changing active facts, not restoring source files.

```mica
activate_version(#m1, version_6)
```

The live database is canonical. A source file is a serialization of some subset
of facts.

## Filein/Fileout

Mica can have a filein/fileout format, but it is only an outer
serialization/import syntax around ordinary Mica code and relational state.

The file format is for archival, review, import/export, fixtures, and bulk
editing. It may contain ground facts and pleasant sugared forms:

```mica
Object(#lamp).
Name(#lamp, "brass lamp").
Lit(#lamp, false).
Delegates(#lamp, #thing, 0).
```

or:

```mica
object #lamp extends #thing
  name = "brass lamp"
  lit = false
end

method #lamp_light :light
  names [:light]
  grammar "{target}"
  roles actor: #actor,
        target: #lamp
  mode one
do
  target.lit = true
  assert Event(:lit, actor, target)
end
```

The body inside `do ... end` is not fileout syntax. It is ordinary Mica program
code, the same syntax a live method body or REPL command would use.

Filing this in runs an import transaction. The importer interprets outer
serialization sugar as ordinary fact changes:

```mica
assert Object(#lamp)
assert Delegates(#lamp, #thing, 0)
assert Name(#lamp, "brass lamp")
assert Lit(#lamp, false)

assert Object(#lamp_light)
assert Method(#lamp_light)
assert Selector(#lamp_light, :light)
assert Param(#lamp_light, actor, #actor)
assert Param(#lamp_light, target, #lamp)
assert Source(#lamp_light, version, source_text)
assert ActiveVersion(#lamp_light, version)
```

The fileout envelope is therefore not a second mutation language. The executable
language has one way to propose fact changes: `assert` and `retract`.

```mica
method #lamp_light :light
  roles actor: #actor,
        target: #lamp
do
  require CanLight(actor, target)
  target.lit = true
  assert Event(:lit, actor, target)
end
```

Here `target.lit = true` is assignment sugar for replacing the declared
functional relation `Lit(target, value)`. `assert Event(...)` records a fact in
the current transaction. Neither line is a serialized ground fact.

## Permissions and Authority

Mica should not use object-local Unix-style ACLs as its core authority model.
Those ask the wrong question:

```text
Can user U read/write/execute object O?
```

Mica operations are relational. The useful questions are:

```text
Can authority A read this relation tuple?
Can authority A propose this assertion or retraction?
Can authority A invoke this behavior with these role bindings?
Can authority A activate this definition version?
```

Authority is carried by invocation-local capabilities. A capability is an
unforgeable identity. Possessing the identity grants designation and authority
according to relations derived from that capability and the current world state.

```mica
Capability(#cap17)
Arg(inv, authority, #cap17)
Arg(inv, actor, #user3)
Arg(inv, subject, #current_subject)
```

Knowing an object id is not authority:

```text
#door17 designates a door
#cap17 authorizes some operation involving it
```

This preserves the useful object-capability distinction:

```text
designation is naming a thing
authority is possessing a capability to act
```

Capabilities are still values in the relational system, but possession is
protected. Arbitrary user code must not be able to enumerate all capabilities or
assert that it holds one. The possession relation is supplied by the runtime as
invocation-local state.

```mica
Authority(inv, #cap17)
```

The kernel enforces authority at three boundaries:

- read: which facts a computation can observe;
- invoke: which behaviors a computation can call or emit;
- commit: which transaction writes can become durable facts.

These checks are themselves relational predicates:

```mica
Readable(auth, relation, tuple)
Writable(auth, change, relation, tuple)
Invokable(auth, selector, invocation)
Grantable(auth, capability)
```

A method does not see the whole database. It sees authority-filtered views of
relations:

```mica
VisibleName(auth, o, value) :-
  Name(o, value),
  Readable(auth, Name, (o, value)).

VisibleLocatedIn(auth, o, place) :-
  LocatedIn(o, place),
  Readable(auth, LocatedIn, (o, place)).
```

Effects are transaction writes checked before commit:

```mica
AllowedProposal(auth, assert, LocatedIn(item, dest)) :-
  Writable(auth, assert, LocatedIn, (item, dest)).

AllowedProposal(auth, retract, LocatedIn(item, old)) :-
  Writable(auth, retract, LocatedIn, (item, old)).
```

ACLs can still exist, but only as one possible way to derive the authority
relations:

```mica
Readable(auth, relation, tuple) :-
  HeldBy(auth, user),
  CanRead(user, relation, tuple).

Writable(auth, change, relation, tuple) :-
  HeldBy(auth, user),
  CanWrite(user, change, relation, tuple).
```

Likewise, object-capability style permissions are another way to derive them:

```mica
Readable(auth, relation, tuple) :-
  Allows(auth, read, relation, tuple).

Writable(auth, change, relation, tuple) :-
  Allows(auth, change, relation, tuple).
```

The trusted kernel should enforce `Readable`, `Writable`, `Invokable`, and
`Grantable`. Everything above those predicates can be ordinary Mica policy.

Subjective dispatch is just another role. A subject can contribute to method
selection and authority derivation without being a special runtime construct.

```mica
verb inspect(actor: #player, target: #thing, subject: #debug_subject)
```

## Syntax Sketch

Object authoring:

```mica
object #lamp extends #thing
  name = "brass lamp"
  lit = false
end
```

Multiple delegation:

```mica
object #magic_lamp
  extends #lamp
  extends #magic_item
  charges = 3
end
```

Relational definitions:

```mica
VisibleTo(user, obj) :-
  LocatedIn(user, room),
  LocatedIn(obj, room).

CanMove(actor, item) :-
  VisibleTo(actor, item),
  Portable(item).
```

This syntax is an editing command. It creates or updates live definition
objects; it is not the underlying storage model.

Verb definitions:

```mica
verb look(actor: #player, target: #thing) -> Text
  return Description[target]
end

verb open(actor: #player, target: #container)
  require CanOpen(actor, target)
  target.closed = false
end
```

Event-style contributions:

```mica
verb after_move(actor: #player, item: #thing, destination: #room)
  emit Message(actor, "Moved.")
end
```

Policy declaration:

```mica
policy after_move:
  mode all
end
```

## Relation to MOO

MOO-style systems already behave more like databases than conventional
programs:

```text
objects exist first
authors mutate them over time
behavior accumulates in the world
the live database is the program
```

Mica makes that explicit. Objects, verbs, source code, permissions, history, and
containment are all queryable and transactionally editable.

The familiar MOO command:

```mica
move #coin into #box
```

is not "call `move` on `#coin`." It is:

```text
assert an invocation relation
derive applicable behavior from that relation
produce a state transition
validate it
commit it
```

## Relation to PMD

PMD internalizes multimethod roles into prototype objects. Mica keeps the same
core insight but makes the applicable-method relation explicit.

PMD:

```text
selector + ordered arguments -> best method
```

Mica:

```text
selector + named role bindings -> relation of applicable behaviors
```

`best method` is one dispatch mode among several.

## Relation to Rel

Rel treats relations as the main programming abstraction. Mica adopts that as
the semantic substrate for object systems.

Field access is relational application:

```mica
object.property
```

desugars through the declared property mapping:

```mica
PropertyRelation(property, relation)
relation[object]
```

Verb invocation is relational restriction:

```mica
move(actor: #user, item: #coin, prep: :into, destination: #box)
```

desugars to an invocation relation plus dispatch over `Applicable`.

## Implementation Notes

- Store base facts in indexed relations.
- Compile derived relations to incremental queries where possible.
- Cache dispatch by selector and significant roles.
- Invalidate dispatch caches when `Delegates`, `Param`, `Selector`, prototype
  facts, or relevant method versions change.
- Compile method bodies to transactional plans.
- Keep parser extensibility relational: command grammars are facts.
- Treat source import/export as serialization of facts, not the canonical
  program.

## Summary

Mica is not "objects stored in a database." It is objecthood as a relational
theory over a live database.

Prototype inheritance is derived relation traversal. Method dispatch is
relational application plus an explicit selection or aggregation policy. A
MOO-like world becomes a persistent, queryable, authorable program whose
semantics are relational all the way down.
