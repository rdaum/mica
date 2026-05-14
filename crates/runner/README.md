# mica-runner

`mica-runner` wires the compiler, runtime, relation kernel, and builtins into a
small executable environment. It provides the `mica` binary, the REPL, source
evaluation, and filein support.

This crate is not intended to be the final server. It is the current integration
surface for exercising the language and live relational model.

## What's Here

- `src/main.rs`: command-line entry point for `mica run`, `mica filein`,
  `mica eval`, and `mica repl`.
- `src/lib.rs`: `SourceRunner`, builtin registration, bootstrap relations,
  context refresh, method/rule installation, filein chunking, report rendering,
  and display name lookup for identities and relations.

## Commands

Run one source file as a single source chunk:

```sh
cargo run --bin mica -- run path/to/file.mica
```

File in a source file as a sequence of chunks:

```sh
cargo run --bin mica -- filein examples/mud-core.mica
```

Evaluate a one-line source string:

```sh
cargo run --bin mica -- eval "1 + 1"
```

Start the REPL:

```sh
cargo run --bin mica
```

## Role In Mica

The runner is where live authoring currently becomes visible. It creates an
empty kernel, installs builtins like `make_identity` and `make_relation`, keeps
the compiler context in sync with catalogue facts, installs rules and methods,
and submits ordinary source chunks to the task manager.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
