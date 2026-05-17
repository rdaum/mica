# Language Overview

Mica source is executable code for a live world. You can type it at the REPL,
put it in a filein, or install it as the body of a verb. These are different
entry points into the same language, not separate sublanguages.

The unusual part of Mica is the object model. An object is not a record with
fields and methods stored inside it. An object is a durable identity value, such
as `#alice` or `#lamp`, that can participate in many facts:

```mica
Object(#lamp)
Name(#lamp, "brass lamp")
LocatedIn(#lamp, #first_room)
Portable(#lamp)
```

When you inspect `#lamp`, the useful view is its fact neighbourhood: the facts
and derived facts that mention it. This lets different authors build different
ontologies over the same identities. A world might have `LocatedIn`, `OwnedBy`,
`ObservedBy`, `DependsOn`, `RenderedAs`, and `TrustedBy` without making any one
of those relationships the privileged object layout.

The main language pieces are:

- values: primitives, symbols, identities, collections, frobs, bytes, and
  runtime capabilities;
- relations: named facts about values;
- rules: derived facts, including recursive relationships;
- verbs: installed behaviour selected by named-role dispatch;
- authority: policy facts compiled into cheap runtime checks;
- effects: committed outputs to hosts and endpoints;
- tasks: transactional executions that can commit, suspend, resume, and spawn.

The syntax is Algol-family and expression-oriented. There is no separate
statement-only layer. Assignment, conditionals, queries, dispatch, and builtin
calls all produce values, even when those values are often ignored.
