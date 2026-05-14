# mica-runner

`mica-runner` is the command-line executable crate for Mica. It provides the
`mica` binary, the REPL, source evaluation, filein, and fileout commands.

This crate is not the live runtime environment. That code lives in
`mica-runtime`; this crate is a thin command-line consumer of it.

## What's Here

- `src/main.rs`: command-line entry point for `mica run`, `mica filein`,
  `mica eval`, and `mica repl`.

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

The runner opens a `mica-runtime` `SourceRunner`, feeds it source from files,
command-line arguments, or the REPL, and renders reports for humans.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
