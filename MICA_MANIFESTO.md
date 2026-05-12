# The Mica Manifesto

Mica is a programming system for live, multiuser worlds: social spaces,
simulations, authoring environments, programmable games, and long-lived shared
systems that need to evolve while people are using them.

It starts from a simple claim:

**An object is not a box. An object is a view on a set of facts.**

## Facts, Not Boxes

Most programming systems teach us to imagine objects as containers. A lamp has a
color because the color is inside the lamp. It has a location because the lamp
points to a room. It has behavior because methods are stored on the lamp, its
class, or its prototype.

That model is convenient, but it creates pressure to decide too early where
every piece of meaning belongs. Once the system grows, those choices become
paths that code depends on, hierarchies that authors must work around, and
storage shapes that are hard to change.

Mica keeps the identity and lets the representation move.

```mica
Object($lamp42)
Name($lamp42, "brass lamp")
LocatedIn($lamp42, $room17)
Owner($room17, $alice)
Color($lamp42, "brass")
```

`$lamp42` is not a record that contains those fields. It is a durable identity
that appears in facts. The world is the collection of those facts, the rules
that derive more facts from them, and the transactions that change them.

This does not mean objects disappear from the programmer's experience. A Mica
browser can still show an object-shaped view:

```text
$lamp42
  name: "brass lamp"
  location: $room17
  color: "brass"
```

But that view is an outliner over a fact neighborhood, not evidence of a hidden
object record. You can inspect from the lamp, the room, the owner, the relation,
or the history, because none of those perspectives owns the truth.

## Open Relations

Traditional object systems often bury important relationships inside privileged
runtime structures: parent pointers, locations, ownership, containment,
visibility, permissions, event routing, and method tables.

Mica makes those relationships explicit.

Instead of a hardcoded `location` field, there is a `LocatedIn` relation.
Instead of a hardcoded parent slot, there is a `Delegates` relation. Instead of
special-case visibility logic hidden in the server, visibility can be a derived
relation that authors can inspect, extend, and replace.

```mica
VisibleTo(actor, obj) :-
  LocatedIn(actor, room),
  LocatedIn(obj, room).
```

The point is not that every relation is equally casual. Some relations have
constraints, indexes, authority checks, and standard library meaning. The point
is that they are still part of the same world model, not a sealed layer beneath
it.

## Behavior Through Matching

In many object systems, behavior lookup starts with a receiver:

```text
receiver -> selector -> method
```

Mica starts with an invocation:

```text
roles -> matching methods -> transaction
```

A method does not have to live inside one receiver. It describes the role
bindings it can handle.

```mica
method $move_into :move
  roles actor: $player,
        item: $portable,
        destination: $container
do
  require CanMove(actor, item)
  require CanContain(destination, item)

  retract LocatedIn(item, _)
  assert LocatedIn(item, destination)
end
```

Prototype delegation still matters, but it becomes part of matching rather than
a physical place where behavior is stored. If `$coin` delegates to `$portable`,
then a method requiring `item: $portable` can apply to `$coin`.

This makes behavior additive. Different authors can contribute methods that
match different roles, relations, situations, or subjects without having to
agree that one object owns the entire behavior surface.

## Live, But Transactional

Mica is live in the sense that the world is the program. Objects, methods,
relations, rules, permissions, parser entries, and source versions are all
world state. They can be inspected and changed while the system is running.

Mica is transactional in the sense that change is checked before it becomes
durable. A command may assert facts, retract facts, record events, and request
external effects inside its transaction. The runtime checks constraints,
authority, and write conflicts, then commits the transaction atomically.

For an interactive author, the normal loop should still feel immediate:

```mica
> $lamp42.color = "gold"
committed
```

The transaction is not a bureaucratic staging area. It is how a live multiuser
system stays coherent while many behaviors and authors are active at once.

## Extensible Shape

Mica is built for systems whose shape cannot be known in advance.

Today a world may need containment and ownership. Tomorrow it may need acoustic
propagation, provenance, weather exposure, faction reputation, ritual state, or
legal responsibility. In Mica, these are not all forced into one object layout.
They can become relations, rules, constraints, and methods.

```mica
AcousticNeighbor($hall, $atrium, attenuation: 2)
OwnedAt($lamp42, $alice, t1)
OwnedAt($lamp42, $bob, t2)
WeatherExposed($garden, true)
```

This is not an argument against structure. It is an argument for structure that
can be named, queried, constrained, delegated, versioned, and revised.

## The Aim

Mica is not a database with objects pasted on top, and it is not an object
system with a database hidden underneath.

It is a live relational object system:

- objects are stable identities;
- state is facts;
- inheritance is delegation over identities;
- behavior is dispatch over role bindings;
- change is asserted and retracted transactionally;
- authoring is part of the running world.

The goal is a system with the immediacy of an image, the extensibility of a
shared world, and the rigor of relations.
