# Cowbell Core Objects in Mica

This is a translation exercise, not a proposed source format. The source
examples are from `../moor/cores/cowbell/src`, especially:

- `root.moo`
- `items/thing.moo`
- `items/container.moo`
- `world/room.moo`
- `actor.moo`
- `events/event_receiver.moo`
- `player.moo`

These files are Moor's objdef import/export format. They are not the live MOO
authoring syntax; live mutation in MOO/Moor happens through builtins such as
`create`, `add_property`, `add_verb`, `set_property_info`, and verb/property
assignment. The purpose here is to use the objdef format as a convenient
snapshot of Cowbell's core object graph and test how that snapshot looks when
represented as Mica's relational object model.

## Notation Used Here

The examples use Mica's Datalog-inspired relational notation.

```mica
Object($thing).
```

means the unary tuple `($thing)` is in the `Object` relation.

```mica
LocatedIn($thing, $prototype_box).
```

means the binary tuple `($thing, $prototype_box)` is in the `LocatedIn`
relation.

Rules use Horn-clause syntax:

```mica
Portable(item) :-
  EffectivePortable(item, true).
```

The period terminates a fact or rule. Commas mean conjunction. Values like
`$thing` are object identities; values like `:get` are symbols; unquoted names
like `item` are variables inside rules.

Executable Mica code proposes fact changes with `assert` and `retract`:

```mica
assert LocatedIn($coin, $alice)
retract LocatedIn($coin, $room)
```

The more relational-looking `Proposal(tx, assert, LocatedIn(...))` examples
below are semantic sketches of what a transaction body produces. They are not a
second authoring syntax.

## Reading the Cowbell Shape

Cowbell objdef files use a familiar object-shaped serialization:

```moo
object THING
  name: "Generic Thing"
  parent: ROOT
  location: PROTOTYPE_BOX
  owner: HACKER
  fertile: true
  readable: true

  property portable = true;
  property get_rule = 0;

  verb get (this none none)
    ...
  endverb
endobject
```

In Mica, this is not one record. It becomes facts about stable identities:

```mica
Object($thing).
DisplayName($thing, "Generic Thing").
Delegates($thing, $root, 0).
LocatedIn($thing, $prototype_box).
Owner($thing, $hacker).
Fertile($thing, true).
ReadableObject($thing, true).

Portable($thing, true).
GetRule($thing, $none).
```

The object identity is `$thing`. It is a key-like value reused across many
relations.

## Prototype Graph

The Cowbell prototype graph sampled here becomes ordinary delegation facts.

```mica
Object($root).
Object($thing).
Object($container).
Object($actor).
Object($event_receiver).
Object($player).
Object($room).

Delegates($thing, $root, 0).
Delegates($container, $thing, 0).
Delegates($actor, $root, 0).
Delegates($event_receiver, $actor, 0).
Delegates($player, $event_receiver, 0).
Delegates($room, $root, 0).
```

`Delegates` is not class inheritance in the static-language sense. It is a live
relation. Moving an object to another prototype or adding a mixin is a
transaction over `Delegates`.

```mica
Prototype(o) :-
  Object(o),
  LocatedIn(o, $prototype_box).

Inherits(o, proto) :-
  DelegatesStar(o, proto).
```

## Common Object Metadata

MOO's built-in object fields can be represented as normal relations when they
are semantically important across the system.

```mica
DisplayName($root, "Root Prototype").
DisplayName($thing, "Generic Thing").
DisplayName($container, "Generic Container").
DisplayName($room, "Generic Room").
DisplayName($actor, "Generic Actor").
DisplayName($event_receiver, "Generic Event Receiver").
DisplayName($player, "Generic Player").

ImportExportId($root, "root").
ImportExportId($thing, "thing").
ImportExportId($container, "container").
ImportExportId($room, "room").
ImportExportId($actor, "actor").
ImportExportId($event_receiver, "event_receiver").
ImportExportId($player, "player").

ImportExportHierarchy($thing, ["items"]).
ImportExportHierarchy($container, ["items"]).
ImportExportHierarchy($room, ["world"]).
ImportExportHierarchy($event_receiver, ["events"]).
```

Core Cowbell properties should usually become named relations. Generic `Slot`
facts remain available for truly ad-hoc author-defined state, but they should
not be the first choice for system concepts like name, description, openness, or
portability.

```mica
Aliases($root, []).
Description($root,
  "Root prototype object from which all other objects inherit.").
Thumbnail($root, false).

Description($thing,
  "Generic thing prototype that is the basis for most items in the world.").
Portable($thing, true).
IntegratedDescription($thing, "").
CountableNoun($thing, true).
PluralNoun($thing, false).
ProperNounName($thing, false).

Description($container,
  "Generic container prototype for objects that can hold other items.").
Open($container, true).
Locked($container, false).
PutPreposition($container, :in).
PutPreposition($container, :inside).
PutPreposition($container, :into).
PutPrepDisplay($container, "in").

Description($room,
  "Parent prototype for all rooms in the system, defining room behavior and event broadcasting.").

Performing($actor, []).
Wearing($actor, []).
HasFeature($player, $social_features).
HasFeature($player, $mail_features).
Home($player, $none).
```

## Property Definitions

Cowbell properties carry metadata: owner and flags. In Mica, property definition
is separate from property value.

```mica
Property($thing, portable).
PropertyOwner($thing, portable, $hacker).
PropertyFlags($thing, portable, [:readable, :chown_on_owner_change]).
DefaultPortable($thing, true).

Property($container, locked).
PropertyOwner($container, locked, $hacker).
PropertyFlags($container, locked, [:readable]).
DefaultLocked($container, false).
```

Then effective values are derived relation-by-relation:

```mica
EffectivePortable(o, value) :-
  Portable(o, value).

EffectivePortable(o, value) :-
  Delegates(o, proto, _),
  EffectivePortable(proto, value),
  not HasCloserPortable(o).
```

Cowbell's `override description = ...` becomes a local `Slot` fact on the
prototype object that shadows the inherited slot.

## Method Objects

Cowbell verbs in objdef become method identities with selector, command
grammar, owner, flags, and role requirements.

Authors should not have to create those facts one by one. In a Mica fileout
format, a Cowbell-style verb like `get (this none none)` could be serialized
through a compact method signature:

```mica
method $thing_get :get
  names [:get, :take]
  grammar "{item}"
  receiver item
  roles actor: $actor,
        item: $thing,
        prep: :none,
        context: Any
  mode one
  owner $arch_wizard
  flags [:readable, :executable, :debug]
do
  require CanGet(actor, item)
  move item to actor
  emit Moved(actor, item, actor)
end
```

Filing in that signature would mutate the live world by creating normalized
method facts:

```mica
Method($thing_get).
Selector($thing_get, :get).
VerbNames($thing_get, [:get, :take]).
MethodOwner($thing_get, $arch_wizard).
MethodFlags($thing_get, [:readable, :executable, :debug]).

Param($thing_get, actor, $player).
Param($thing_get, item, $thing).
Param($thing_get, prep, :none).
Param($thing_get, context, Any).
ReceiverRole($thing_get, item).

CommandPattern($thing_get, [verb, object]).
```

The Cowbell form:

```moo
verb get (this none none)
```

is not preserved as a privileged receiver call. It becomes a command pattern
whose parsed invocation binds a role named `item`.

The dispatch surface should be compact:

```mica
:get(actor: $alice,
     item: $brass_key,
     prep: :none,
     context: $first_room)
```

The normalized relation form below is an internal representation for auditing,
debugging, transaction planning, or durable event history. It is not the syntax
authors should have to write for ordinary dispatch.

```mica
Invocation(i).
Selector(i, :get).
Arg(i, actor, $alice).
Arg(i, item, $brass_key).
Arg(i, prep, :none).
Arg(i, context, $first_room).
```

The method applies when the role bindings match the method parameters.

Receiver-call sugar is also possible when a selector declares a receiver role:

```mica
$brass_key:get(actor: $alice)
```

desugars to:

```mica
:get(item: $brass_key, actor: $alice)
```

This gives MOO-like `object:verb(...)` ergonomics while keeping dispatch over
all named roles.

## Root Behavior

`ROOT` supplies foundational behavior: creation, destruction, movement,
presentation, command introspection, and permission checking.

### Creation

Cowbell's `ROOT:create()` checks fertility or capability, creates a child, and
sets the new object's owner to `caller_perms()`.

In Mica this is a transaction-producing behavior:

```mica
Method($root_create).
Selector($root_create, :create).
Param($root_create, actor, $actor).
Param($root_create, prototype, $root).

MayCreateChild(auth, actor, prototype) :-
  EffectiveFertile(prototype, true).

MayCreateChild(auth, actor, prototype) :-
  Writable(auth, assert, Delegates, (fresh_object, prototype, 0)).

CreateChildProposal(tx, actor, prototype, child) :-
  FreshObject(tx, child),
  MayCreateChild(Auth(tx), actor, prototype).

Proposal(tx, assert, Object(child)) :-
  CreateChildProposal(tx, actor, prototype, child).

Proposal(tx, assert, Delegates(child, prototype, 0)) :-
  CreateChildProposal(tx, actor, prototype, child).

Proposal(tx, assert, Owner(child, actor)) :-
  CreateChildProposal(tx, actor, prototype, child).
```

The old MOO permission rule becomes a relation over authority, actor, and
prototype rather than an object-local ACL check.

### Movement

Cowbell's `moveto` delegates to the server `move(this, destination)` primitive.
In Mica, movement is a checked transition over `LocatedIn`.

```mica
Method($root_moveto).
Selector($root_moveto, :move_to).
Param($root_moveto, actor, $actor).
Param($root_moveto, item, Object).
Param($root_moveto, destination, Object).

CanMoveTo(auth, actor, item, destination) :-
  Writable(auth, retract, LocatedIn, (item, _)),
  Writable(auth, assert, LocatedIn, (item, destination)),
  Acceptable(destination, item).

Proposal(tx, retract, LocatedIn(item, old)) :-
  Invoke(tx, :move_to),
  Arg(tx, item, item),
  LocatedIn(item, old),
  CanMoveTo(Auth(tx), Actor(tx), item, Destination(tx)).

Proposal(tx, assert, LocatedIn(item, destination)) :-
  Invoke(tx, :move_to),
  Arg(tx, item, item),
  Arg(tx, destination, destination),
  CanMoveTo(Auth(tx), Actor(tx), item, destination).
```

`Acceptable` is no longer a receiver method. It is a relation with defaults and
specializations.

```mica
Acceptable(container, item) :-
  Inherits(container, $room).

Acceptable(container, item) :-
  Inherits(container, $actor).

Acceptable(container, item) :-
  Inherits(container, $container),
  EffectiveOpen(container, true).
```

## Thing: Portable World Items

Cowbell `THING` provides get/drop, grammatical presentation, integrated
descriptions, and rule-based portability.

### Pronouns and Presentation

The pronoun flyweight can be normalized into facts:

```mica
PronounSet($pronouns_it).
PronounSubject($pronouns_it, "it").
PronounObject($pronouns_it, "it").
PronounPossessiveAdj($pronouns_it, "its").
PronounPossessiveNoun($pronouns_it, "its").
PronounReflexive($pronouns_it, "itself").
PronounDisplay($pronouns_it, "it/its").
PronounPlural($pronouns_it, false).

Pronouns($thing, $pronouns_it).

IsPlural(o) :-
  EffectivePluralNoun(o, true).

IsCountable(o) :-
  EffectiveCountableNoun(o, true).

IsProperNoun(o) :-
  EffectiveProperNounName(o, true).
```

### Get and Drop

Cowbell's `THING:get` checks:

- the item is not already held by the actor;
- the item permits getting;
- the actor accepts it;
- then moves the item to the actor and emits an event.

As Mica rules:

```mica
CanGet(actor, item) :-
  Inherits(item, $thing),
  not LocatedIn(item, actor),
  EffectivePortable(item, true),
  ThingGetRuleAllows(actor, item),
  Acceptable(actor, item).

ThingGetRuleAllows(actor, item) :-
  EffectiveGetRule(item, $none).

ThingGetRuleAllows(actor, item) :-
  EffectiveGetRule(item, rule),
  RuleAllows(rule, [Actor -> actor, This -> item]).

Method($thing_get).
Selector($thing_get, :get).
Param($thing_get, actor, $actor).
Param($thing_get, item, $thing).
Policy($thing_get, :one).

Proposal(tx, retract, LocatedIn(item, old)) :-
  Invoke(tx, :get),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  LocatedIn(item, old),
  CanGet(actor, item).

Proposal(tx, assert, LocatedIn(item, actor)) :-
  Invoke(tx, :get),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  CanGet(actor, item).

Proposal(tx, emit, Event(:moved, actor, item, actor)) :-
  Invoke(tx, :get),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  CanGet(actor, item).
```

Drop is the mirror image:

```mica
CanDrop(actor, item, room) :-
  LocatedIn(item, actor),
  LocatedIn(actor, room),
  ThingDropRuleAllows(actor, item),
  Acceptable(room, item).

ThingDropRuleAllows(actor, item) :-
  EffectiveDropRule(item, $none).

ThingDropRuleAllows(actor, item) :-
  EffectiveDropRule(item, rule),
  RuleAllows(rule, [Actor -> actor, This -> item]).

Proposal(tx, retract, LocatedIn(item, actor)) :-
  Invoke(tx, :drop),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  LocatedIn(actor, room),
  CanDrop(actor, item, room).

Proposal(tx, assert, LocatedIn(item, room)) :-
  Invoke(tx, :drop),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  LocatedIn(actor, room),
  CanDrop(actor, item, room).
```

The imperative Cowbell command handler becomes a set of checks and proposed
facts. User-facing error events would be separate fallback methods selected when
one of the positive preconditions fails.

## Container

Cowbell `CONTAINER` inherits from `THING` and adds open/close, lock/unlock, and
put/take rules.

### State and Rule Relations

```mica
Open($container, true).
Locked($container, false).
OpenRule($container, $none).
CloseRule($container, $none).
LockRule($container, $none).
UnlockRule($container, $none).
TakeRule($container, $none).
PutRule($container, $none).
PutPreposition($container, :in).
PutPreposition($container, :inside).
PutPreposition($container, :into).
```

### Container Predicates

```mica
ContainerOpen(c) :-
  Inherits(c, $container),
  EffectiveOpen(c, true).

ContainerClosed(c) :-
  Inherits(c, $container),
  EffectiveOpen(c, false).

ContainerLocked(c) :-
  Inherits(c, $container),
  EffectiveLocked(c, true).
```

### Put Into

Cowbell `put (any any this)` parses a preposition, matches the direct object
from the player's inventory, checks the container is open and rule-allowed, and
moves the item.

```mica
ValidPutPrep(container, prep) :-
  EffectivePutPreposition(container, prep).

CanPutInto(actor, item, container, prep) :-
  Inherits(container, $container),
  ValidPutPrep(container, prep),
  LocatedIn(item, actor),
  item != container,
  ContainerOpen(container),
  ContainerPutRuleAllows(actor, item, container),
  Acceptable(container, item).

ContainerPutRuleAllows(actor, item, container) :-
  EffectivePutRule(container, $none).

ContainerPutRuleAllows(actor, item, container) :-
  EffectivePutRule(container, rule),
  RuleAllows(rule, [This -> container, Accessor -> actor, Dobj -> item]).

Method($container_put).
Selector($container_put, :put).
Param($container_put, actor, $actor).
Param($container_put, item, $thing).
Param($container_put, prep, Preposition).
Param($container_put, destination, $container).

Proposal(tx, retract, LocatedIn(item, actor)) :-
  Invoke(tx, :put),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  Arg(tx, prep, prep),
  Arg(tx, destination, container),
  CanPutInto(actor, item, container, prep).

Proposal(tx, assert, LocatedIn(item, container)) :-
  Invoke(tx, :put),
  Arg(tx, actor, actor),
  Arg(tx, item, item),
  Arg(tx, prep, prep),
  Arg(tx, destination, container),
  CanPutInto(actor, item, container, prep).
```

### Take From

```mica
CanTakeFrom(actor, item, container) :-
  Inherits(container, $container),
  LocatedIn(item, container),
  ContainerOpen(container),
  ContainerTakeRuleAllows(actor, item, container),
  Acceptable(actor, item).

ContainerTakeRuleAllows(actor, item, container) :-
  EffectiveTakeRule(container, $none).

ContainerTakeRuleAllows(actor, item, container) :-
  EffectiveTakeRule(container, rule),
  RuleAllows(rule, [This -> container, Accessor -> actor, Dobj -> item]).

Method($container_take).
Selector($container_take, :get).
VerbNames($container_take, [:get, :take, :steal, :grab]).
Param($container_take, actor, $actor).
Param($container_take, item, $thing).
Param($container_take, prep, :from).
Param($container_take, source, $container).
```

This shows why named roles are better than MOO's `dobj/prep/iobj`: the same
selector `:get` can dispatch on either `item` alone or on `item + source`.

### Open and Lock

```mica
CanOpen(actor, container) :-
  Inherits(container, $container),
  not ContainerLocked(container),
  ContainerOpenRuleAllows(actor, container).

ContainerOpenRuleAllows(actor, container) :-
  EffectiveOpenRule(container, $none).

ContainerOpenRuleAllows(actor, container) :-
  EffectiveOpenRule(container, rule),
  RuleAllows(rule, [This -> container, Accessor -> actor]).

Proposal(tx, retract, Open(container, false)) :-
  Invoke(tx, :open),
  Arg(tx, actor, actor),
  Arg(tx, target, container),
  CanOpen(actor, container).

Proposal(tx, assert, Open(container, true)) :-
  Invoke(tx, :open),
  Arg(tx, actor, actor),
  Arg(tx, target, container),
  CanOpen(actor, container).
```

Locking and unlocking follow the same shape, with a key role:

```mica
CanUnlock(actor, container, key) :-
  Inherits(container, $container),
  ContainerLocked(container),
  ContainerUnlockRuleAllows(actor, container, key).

ContainerUnlockRuleAllows(actor, container, key) :-
  EffectiveUnlockRule(container, rule),
  RuleAllows(rule, [This -> container, Accessor -> actor, Key -> key]).

Method($container_unlock).
Selector($container_unlock, :unlock).
Param($container_unlock, actor, $actor).
Param($container_unlock, target, $container).
Param($container_unlock, prep, :with).
Param($container_unlock, key, $thing).
```

Cowbell treats `lock_rule == 0` as "not lockable" for lock/unlock but "public"
for open/close/take/put. In Mica that distinction should not be overloaded into
one sentinel. Use explicit capability facts:

```mica
Openable(container).
Closeable(container).
Lockable(container).
Unlockable(container).
```

or explicit rule defaults:

```mica
RuleDefault($container, open_rule, :public).
RuleDefault($container, lock_rule, :disabled).
```

## Room

Cowbell `ROOM` is a container-like environment but not a `THING`; it inherits
directly from `ROOT`. Its main behaviors are accepting objects, exposing command
scope, announcing events, and handling speech/emotes.

```mica
Acceptable(room, item) :-
  Inherits(room, $room).

ScopeEntry(actor, room, obj) :-
  LocatedIn(actor, room),
  LocatedIn(obj, room),
  VisibleTo(actor, obj).

ScopeEntry(actor, room, room) :-
  LocatedIn(actor, room).
```

Room events become emitted proposals:

```mica
Method($room_announce).
Selector($room_announce, :announce).
Param($room_announce, room, $room).
Param($room_announce, event, $event).

Proposal(tx, emit, Deliver(viewer, event)) :-
  Invoke(tx, :announce),
  Arg(tx, room, room),
  Arg(tx, event, event),
  LocatedIn(viewer, room),
  CanReceiveEvent(viewer, event).
```

Acoustic propagation is naturally relational. Cowbell stores
`acoustic_neighbors` as a list of maps; Mica can normalize it:

```mica
AcousticNeighbor(room, neighbor, attenuation, max_depth).

Proposal(tx, emit, Deliver(viewer, event2)) :-
  Invoke(tx, :announce),
  Arg(tx, room, room),
  Arg(tx, event, event),
  EventLoudness(event, loudness),
  loudness > 0,
  AcousticNeighbor(room, neighbor, attenuation, max_depth),
  PropagatedEvent(event, room, neighbor, attenuation, event2),
  LocatedIn(viewer, neighbor),
  CanReceiveEvent(viewer, event2).
```

This is a good example of where Mica should improve on the MOO shape: the
relation-oriented representation makes propagation queryable and avoids
packing graph edges into ad hoc property maps.

## Actor and Player

`ACTOR` gives participants inventory acceptance, speech events, pronouns,
activity state, and facts used by the rule engine.

```mica
Acceptable(actor, item) :-
  Inherits(actor, $actor).

Actor(actor) :-
  Inherits(actor, $actor).

IsWizard(actor) :-
  Wizard(actor, true).

IsProgrammer(actor) :-
  Programmer(actor, true).

IsBuilder(actor) :-
  EffectiveBuilder(actor, true).

HasInInventory(actor, thing) :-
  LocatedIn(thing, actor).

Owns(actor, thing) :-
  Owner(thing, actor).
```

`PLAYER` specializes event receiving and command handling.

```mica
Delegates($player, $event_receiver, 0).

Player(p) :-
  Inherits(p, $player).

Connected(p) :-
  Connection(p, _).

InventoryItem(player, item) :-
  LocatedIn(item, player).
```

Cowbell's `look` command combines parsing, matching, ambiguity reporting,
passage fallback, and presentation. In Mica this should split into relations:

```mica
CommandParse(text, inv).
CandidateMatch(inv, role, obj).
AmbiguousMatch(inv, role) :-
  Count { obj | CandidateMatch(inv, role, obj) } > 1.

LookTarget(actor, target) :-
  Arg(inv, actor, actor),
  Selector(inv, :look),
  Arg(inv, target, target).

LookTarget(actor, room) :-
  Selector(inv, :look),
  Arg(inv, actor, actor),
  not Arg(inv, target, _),
  LocatedIn(actor, room).
```

This avoids baking one parser's `dobjstr`/`iobjstr` model into dispatch.

## Event Receiver

Cowbell `EVENT_RECEIVER:tell` renders events for each connection, logs the
event, and notifies clients. In Mica the pure parts and effectful parts should
be separated.

```mica
EventReceiver(o) :-
  Inherits(o, $event_receiver).

CanReceiveEvent(receiver, event) :-
  EventReceiver(receiver),
  not GaggedEvent(receiver, event).

RenderedEvent(receiver, connection, event, content_type, output) :-
  Connection(receiver, connection),
  PreferredContentType(event, content_type),
  ConnectionSupports(connection, content_type),
  Render(event, receiver, content_type, output).

Proposal(tx, assert, EventLog(receiver, event, :text_djot, output)) :-
  Invoke(tx, :tell),
  Arg(tx, receiver, receiver),
  Arg(tx, event, event),
  Render(event, receiver, :text_djot, output).

Proposal(tx, effect, Notify(connection, output, content_type)) :-
  Invoke(tx, :tell),
  Arg(tx, receiver, receiver),
  Arg(tx, event, event),
  RenderedEvent(receiver, connection, event, content_type, output).
```

This is a recurring pattern: Cowbell verbs often intermix query, validation,
state mutation, and IO. Mica should factor those into derived relations and
transaction/effect proposals.

## What This Exercise Suggests

### 1. MOO Properties Become Several Relation Kinds

Cowbell has fields, properties, overrides, property owners, and flags. Mica
probably needs at least:

```mica
Name(object, value)
Description(object, value)
Portable(object, value)
Open(object, value)
Locked(object, value)
Slot(object, key, value)        # ad-hoc extension only
Property(proto, key)
PropertyOwner(proto, key, owner)
PropertyFlags(proto, key, flags)
EffectivePortable(object, value)
```

### 2. MOO Verbs Become Methods Plus Command Patterns

Cowbell conflates callable method, command grammar, owner, flags, and
implementation. Mica should split them:

```mica
Method(m)
Selector(m, selector)
VerbNames(m, names)
CommandPattern(m, pattern)
Param(m, role, matcher)
MethodOwner(m, owner)
MethodFlags(m, flags)
ActiveVersion(m, version)
```

### 3. `this` Becomes a Named Role

Cowbell's:

```moo
verb put (any any this)
```

becomes:

```mica
Param($container_put, item, $thing).
Param($container_put, prep, Preposition).
Param($container_put, destination, $container).
```

The receiver-ish object is not privileged; it is the role `destination`.

For compatibility with MOO's `object:verb(args...)` habit, Mica can allow a
method signature to declare a receiver role:

```mica
method $container_put :put
  receiver destination
  positional item, prep
  roles actor: $actor,
        item: $thing,
        prep: Preposition,
        destination: $container
do ...
end
```

Then:

```mica
$box:put($coin, :into, actor: $alice)
```

desugars to:

```mica
:put(destination: $box, item: $coin, prep: :into, actor: $alice)
```

`self` in the method body is just a lexical alias for `destination`.

### 4. Rule Engine Predicates Can Collapse into Native Relations

Cowbell has methods like `fact_is_portable`, `fact_is_locked`,
`fact_is_wizard`, and `fact_has_in_inventory`. In Mica these should be ordinary
relations:

```mica
Portable(item) :-
  EffectivePortable(item, true).

ContainerLocked(c) :-
  EffectiveLocked(c, true).

HasInInventory(actor, item) :-
  LocatedIn(item, actor).
```

### 5. Imperative Command Handlers Should Become Transaction Plans

MOO command handlers return early on errors and mutate state directly. Mica
should instead derive:

- applicable method set;
- successful transition proposals;
- failed precondition explanations;
- emitted narrative events;
- external effects.

The commit boundary can then check permissions, constraints, and conflicts.

### 6. Sentinels Should Become Relations

Cowbell often uses `0`, `#-1`, or empty strings as sentinels. Mica should avoid
overloading primitive values where a relation is clearer.

```mica
NoRule(container, open_rule).
RuleDefault(container, open_rule, :public).
RuleDefault(container, lock_rule, :disabled).
Nothing($none).
```

### 7. Lists of Maps Want Normalization

Properties like `acoustic_neighbors`, feature lists, event metadata, and
connection entries are often better as relations:

```mica
AcousticNeighbor(room, neighbor, attenuation, max_depth)
HasFeature(player, feature)
EventMetadata(event, key, value)
Connection(player, connection)
ConnectionContentType(connection, content_type)
```

This keeps the system queryable and dispatchable instead of burying structure
inside lists and maps.

## Open Questions

- Which Cowbell properties deserve first-class named relations, and which should
  remain ad-hoc `Slot` facts?
- How should failed preconditions be represented: fallback methods, explicit
  `Failure(tx, reason)` proposals, or a validation report relation?
- Does method dispatch run before or after command matching has resolved object
  references? Cowbell does matching inside verbs; Mica likely wants matching to
  produce invocation facts first.
- Should property flags survive as compatibility metadata, or be replaced by
  `Readable`/`Writable` authority predicates?
- How much of the MOO rule engine syntax should be preserved if Horn clauses
  are native?
