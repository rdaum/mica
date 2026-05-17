# Mica

<p align="center">
  <img src="assets/mica-logo.png" alt="Mica logo" width="320">
</p>

Mica is a language runtime and application server for building live,
programmable systems: collaborative worlds, simulations, knowledge bases,
agent workspaces, games, and operational tools.

Its object model is relation-first and rule-aware. Long-lived identities,
facts, derived relations, verbs, and authority can all change while the system
is running. The result is closer to an object system with a built-in relational
logic layer than to a conventional application framework.

You build a Mica world by creating persistent objects and teaching them new
facts, rules, and verbs. The code that defines behaviour is part of the world:
verbs live alongside the identities they operate on, rather than only in an
external source tree. Mica is designed for many human authors and software
agents to extend a running world from inside that world, with authority checks
on reads, writes, invocations, and effects.

> **NOTE**: If you're looking at this on GitHub be aware this is just a mirror
> from the canonical Codeberg repository at https://codeberg.org/timbran/mica

Try the current telnet demo:

```sh
cargo run --bin mica-daemon -- --telnet-bind 127.0.0.1:7777
telnet 127.0.0.1 7777
```

Then try commands such as `look`, `get coin`, `put coin box`, `north`, and
`say hello`.

## Core Idea

Mica represents objects as durable identity values described by relation facts:

```mica
Object(#lamp)
Name(#lamp, "brass lamp")
LocatedIn(#lamp, #room)
Delegates(#lamp, #thing, 0)
Portable(#lamp)
```

`#lamp` is a durable identity value. The "object" is the fact neighbourhood
around that identity: the facts, derived facts, rules, methods, and authority
policy that mention it.

An object is still something authors can name, browse, extend, invoke
behaviour on, and keep alive across restarts. Its slots, parent links, methods,
permissions, and derived state are represented as relations around a durable
identity rather than fields in one fixed record.

In these examples, `#lamp` is an identity value, `:get(...)` invokes a verb,
and relation names such as `HeldBy`, `LocatedIn`, and `Name` name stored or
derived facts.

Many object systems build a few relationships directly into the runtime:
parent/child, location/contents, ownership, visibility, or method lookup. Mica
tries to make those relationships authorable. They can be ordinary relations,
rules, functional relation metadata, and behaviours that the world can inspect
and extend.

That means object structure is not fixed by one privileged storage layout. A
world can add new relations when it needs new concepts:

```mica
AcousticNeighbour(#hall, #atrium, 2)
OwnedAt(#lamp, #alice, t1)
WeatherExposed(#garden)
Believes(#agent7, #door, :locked)
```

Relations can be indexed, queried, derived, and checked through actor-derived
authority. Functional relations declare key positions, so assignments such as
`#lamp.name = "brass lamp"` replace the one matching tuple instead of adding a
duplicate fact.

For object-system readers:

| Object-system idea | Mica shape |
| --- | --- |
| object pointer or object number | durable identity value |
| slot or property | relation fact mentioning the identity |
| parent or prototype link | `Delegates(child, parent, order)` |
| method dictionary | verb/method facts matched by role restrictions |
| inspector or browser | object-neighbourhood query or outliner |

For database readers: at the storage layer, Mica relations are arity-fixed sets
of tuples over Mica values. Base relations are mutated transactionally. Derived
relations are defined by rules and read through the same relation interface.
Identities such as `#lamp` are values, not rows; relation tuples are the
durable facts that describe them.

## Behaviour

Behaviour is also relational. Instead of finding a method by starting from one
special receiver object, Mica dispatches over named roles:

```mica
verb get(actor @ #player, item @ #thing)
  if Portable(item)
    assert HeldBy(actor, item)
    return true
  else
    return false
  end
end
```

An invocation supplies role bindings:

```mica
:get(actor: #alice, item: #coin)
```

The dispatch engine finds methods whose role restrictions match the invocation.
The restriction `item @ #thing` says that the `item` role must be an identity
that matches `#thing`, either directly or through delegation. Methods are not
looked up inside a receiver's private method dictionary; they are installed
into the live world and selected by matching the roles in the invocation.

Delegation participates in dispatch matching:

```mica
Delegates(#coin, #thing, 0)
Delegates(#alice, #player, 0)
```

The third argument is the delegation order. It lets an identity have multiple
delegates while keeping dispatch and inherited relation lookup deterministic
where order matters.

Role dispatch gives Mica multimethod-like selection while still allowing verbs
to be installed and edited in the running world.

## Rules

Mica also has Datalog-inspired derived relations:

```mica
CanSee(actor, item) :-
  HeldBy(actor, item)

CanSee(actor, item) :-
  HeldBy(actor, container),
  In(item, container)
```

Rules are installed into the live world and become part of ordinary relation
reads. They are meant to make world logic inspectable and authorable instead of
burying it in server internals.

Rules can also express positive recursive relationships, such as transitive
reachability:

```mica
Reachable(from, to) :-
  Exit(from, to)

Reachable(from, to) :-
  Exit(from, mid),
  Reachable(mid, to)
```

This lets authors define concepts like ancestry, containment, visibility,
dependency, graph reachability, or delegation closure in the same relational
language as the rest of the world. Negation is more restricted: Mica supports
stratified negation, not arbitrary recursion through `not`.

The current implementation evaluates rules in the relation kernel and exposes
derived tuples through ordinary relation reads. More complete rule planning and
incremental evaluation remain active areas of work.

## Language Shape

Mica's surface language is intended to feel familiar to people who know MOO or
mooR, while borrowing from Dylan, Julia, Datalog, and Algol-family languages.

Current syntax includes:

```mica
make_identity(:alice)
make_identity(:coin)
make_relation(:HeldBy, 2)

assert HeldBy(#alice, #coin)

for key, value in properties
  render_property(key, value)
end
```

The language is expression-oriented: control forms, assignments, assertions,
queries, and calls produce values.

## Isn't this just a Database?

Mica uses transactional relation storage as part of its programming model. A
Mica task runs against a transaction snapshot and commits its relation changes
as one unit.

The runtime needs:

- stable identities as first-class values;
- live mutation of relations, methods, and rules;
- transactional command execution;
- role-based dispatch;
- prototype delegation;
- derived relations;
- object-neighbourhood inspection;
- author-facing syntax;
- durable relation state and restart recovery.

Those concerns overlap with databases, object systems, logic languages, and
interactive programming environments, but none of those models alone is quite
the intended shape.

In Mica, relations are the author-visible representation of objects, dispatch,
world rules, and authority. The runtime executes behaviours inside that same
transactional relation system.

## For Agents and Tools

Mica gives agents and tools a queryable model of identities, relations, rules,
verbs, and authority.

Compared with message logs or vector-memory stores, Mica keeps shared state in
typed relations with transactions, derived facts, executable behaviours, and
authority checks. Message records, embeddings, tool calls, and task rows can
still exist, but they become facts about durable identities.

```mica
Agent(#planner)
Task(#t42)
Goal(#t42, "prepare release notes")
AssignedTo(#t42, #planner)
Observation(#obs9)
ObservedBy(#obs9, #planner)
AboutTask(#obs9, #t42)
Mentions(#obs9, #crate_runtime)
ToolResult(#obs9, :git_diff, "...")

RelevantTo(agent, item) :-
  AssignedTo(task, agent),
  AboutTask(obs, task),
  Mentions(obs, item)
```

In this style, an agent workspace is not only a transcript. Tasks,
observations, tool results, claims, artefacts, and authority policy can be
facts around durable identities. Rules can derive working context such as
relevance, readiness, visibility, ownership, or blocked-by relationships, and
behaviours can operate over those relations transactionally.

This can support:

- shared task and artefact state across humans and agents;
- blackboard-style coordination with transactional updates;
- provenance-aware observations and tool results;
- derived context views for each actor;
- policy-derived authority for actions and effects.

Agent integrations can query the same relations, rules, methods, and identities
that authors edit. Explanation APIs for derived facts, applicable behaviours,
and authority failures are planned tooling work.

That makes Mica useful for systems where human and software authors collaborate:
knowledge bases, simulations, planning environments, design tools, operational
models, and long-lived shared workspaces.

Vector indexes, embedding stores, and external tools can be attached as
providers or tool-facing facts. Mica's core model is the identity / relation /
rule layer that says what those memories are about and how they may be used.

## MOO-Like Worlds

The current examples show how Mica can model
[MOO-like](https://en.wikipedia.org/wiki/MOO) pieces: rooms, containers,
players, verbs, live source loading, telnet interaction, and shared
programmable space.

MOO showed that a running multiuser world can also be its own authoring
environment. Mica keeps that immediacy while changing the foundation. Object
state is facts, inheritance is delegation over identities, verbs dispatch over
roles, and policies like visibility or containment can be relations and rules
instead of privileged server internals.

MUD-like examples are useful because rooms, exits, containers, and inventory are
easy to understand. They are examples of the model, not the boundary of the
project.

## Background

Mica grows out of lessons from [mooR](https://codeberg.org/timbran/moor), my
modern rewrite of LambdaMOO: a compatibility-focused MOO server with modern
conveniences, transactional command execution, durable storage, and a modern
Rust runtime. Through that lineage, it inherits the model of image-based
authoring, multiuser worlds, long-lived shared state, and online extension.

Mica also draws from Datalog-style rules, Self-style prototype delegation,
multimethod dispatch, and tuple-space-like ideas about shared facts that
independent processes can read, write, and react to.

Mica is less of a nostalgia project than mooR. Without compatibility
constraints, it can use relations, rules, dispatch, and authority as the
primary object model.

The name also reaches back to an earlier abandoned project I worked on between
2001 and 2004: an incomplete prototype-oriented, image-based object system in
the same broad family of ideas as MOO, ColdMUD, and similar systems. That
earlier Mica was written in C++, and the last version of its sources appears to
be lost to time. This project is not a continuation of that code, but it is a
return to some of the same questions with different tools and a more relational
foundation.

## Current Status

Mica's implementation is still early. The current tree has:

- a compact value layer;
- a relation kernel with base facts, transactions, indexes, catalogue metadata,
  and derived rules;
- a register-based bytecode VM;
- a runtime environment with task management, builtins, filein/fileout, and
  transaction lifecycle;
- compiler for a growing Mica language surface;
- a compio-driven task driver for timed wakeups, input resumes, and emissions;
- a telnet host that maps one endpoint identity to each connection and can run
  in process or over the host RPC/IPC protocol;
- a minimal compio HTTP/1.1 host that can route request/response traffic into
  Mica verbs;
- a host protocol console for exercising daemon RPC over ZeroMQ;
- a browser-oriented WASM package that links the compiler, VM, and projected
  relation store without durable providers;
- role-based method dispatch;
- a "filein" syntax for bringing in state-as-initial-blueprint;
- first-cut fileout for revision-controllable units;
- actor-derived authority contexts and runtime capability checks;
- Fjall-backed durable relation state with strict and relaxed commit modes;
- a simple runner and REPL;
- small filein examples, including a Mica-authored command parser.

Relaxed durability accepts commits into the provider's ordered writer queue.
Strict durability waits for the Fjall batch to be applied before the commit
returns.

Run the example:

```sh
cargo run --bin mica -- filein examples/capabilities.mica
```

Run the telnet MUD demo:

```sh
cargo run --bin mica-daemon -- --telnet-bind 127.0.0.1:7777
```

Run the HTTP demo:

```sh
cargo run --bin mica-daemon -- --web-bind 127.0.0.1:8080
curl -i http://127.0.0.1:8080/hello
```

Start the REPL:

```sh
cargo run --bin mica
```

Run the test suite:

```sh
cargo test --workspace
```

## Reference

The checked-in mdBook source lives under [`mdbook/src`](mdbook/src/SUMMARY.md).
It is an in-progress language and runtime reference; the generated HTML output
is not committed yet.

## Repository Map

- [`crates/var`](crates/var/README.md): Mica value representation.
- [`crates/relation-kernel`](crates/relation-kernel/README.md): relation
  storage, transactions, rules, dispatch matching, and catalogue facts.
- [`crates/browser`](crates/browser/README.md): browser-oriented compiler, VM,
  and projected relation package.
- [`crates/vm`](crates/vm/README.md): bytecode format and register VM execution
  core.
- [`crates/compiler`](crates/compiler/README.md): lexer, parser, lowering,
  semantic analysis, and bytecode compilation.
- [`crates/runtime`](crates/runtime/README.md): live environment, task manager,
  builtins, filein/fileout, and rendered reports.
- [`crates/driver`](crates/driver/README.md): compio task driver, wakeups,
  input, and emissions.
- [`crates/runner`](crates/runner/README.md): CLI and REPL binary.
- [`crates/daemon`](crates/daemon/README.md): runtime daemon that can link the
  telnet host in process and expose host RPC over IPC.
- [`crates/host-console`](crates/host-console/README.md): interactive console
  for testing the host protocol over ZeroMQ.
- [`crates/web-host`](crates/web-host/README.md): minimal compio HTTP/1.1
  host and `httparse`-based codec.
- [`crates/telnet-host`](crates/telnet-host/README.md): telnet listener,
  telnet codec, and host-side endpoint session handling.
- `examples/mud-core.mica`: small ontology proving relations, rules, filein,
  verbs, and dispatch.
- `examples/event-substitutions.mica` and `examples/events.mica`: compiled
  narrative substitutions and structured event values.
- `examples/mud-command-parser.mica`: command parsing authored in Mica, using
  low-level string primitives rather than Rust command matching.
- `sketches/MICA_*.md`: design notes for syntax, semantics, standard library,
  and the relation kernel.
- [`CODING-STYLE.md`](CODING-STYLE.md): project coding guidelines, including
  dependency policy.
- [`CONTRIBUTING.md`](CONTRIBUTING.md): contribution expectations, checks, and
  licence terms.

## Direction

The practical target is to build a complete live multiuser server and language
runtime with the range of capability that mooR has today, without mooR's
LambdaMOO compatibility constraint. That means Mica needs to support real users
building, extending, serving, inspecting, exporting, securing, and operating a
running world.

Near-term work includes:

- a fuller standard library for common world, agent, and application relations;
- richer method and verb cataloguing so behaviour can be found, inspected, and
  edited from inside the running system;
- object neighbourhood and outliner queries for browsing identities as live
  objects;
- import/export flows that make live state readable, editable, and
  revision-controllable outside the running store;
- authority and capability hardening beyond the current `Can*`/`Grant*`
  minting path;
- daemon, host, and client surfaces for telnet, web, agents, and tools;
- durable storage hardening, compaction, recovery, and operational testing;
- more complete rule evaluation and query planning.

The long-term aim is a production-quality relational object system for durable
multiuser worlds and inspectable agent/tool integrations.

## Licence

Mica is free software licensed under the GNU Affero General Public License v3.0,
as set out in [LICENSE](LICENSE).
