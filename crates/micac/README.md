# mica-micac

`mica-micac` is the Mica filein compiler. It reads Mica source files, compiles
them into a fresh durable relation store, and optionally runs in check-only
mode with no persistent output.

This is a standalone binary crate, not a library. It wraps `mica-runtime` and
`mica-relation-kernel` to provide the `micac` command-line tool.

## Commands

Compile fileins into a fresh database:

```sh
micac --filein apps/shared/capabilities.mica --filein apps/mud/core.mica --store path/to/db
```

Check fileins without writing a database:

```sh
micac --filein apps/shared/capabilities.mica --check
```

Force overwrite of an existing store:

```sh
micac --filein apps/shared/capabilities.mica --store path/to/db --force
```

Output diagnostics as JSON instead of human-readable text:

```sh
micac --filein apps/broken.mica --check --format json
```

## Options

- `--filein FILE` (required, repeatable): Mica source files to compile.
- `--store DIR`: target database directory (required unless `--check`).
- `--check`: compile only; do not write a database.
- `--force`: overwrite an existing store directory.
- `--format human|json`: output format for success and error reporting.
- `--durability relaxed|strict`: Fjall commit durability mode.
- `--embedding-provider deterministic|disabled|vllm`: embedding provider for
  compile-time operations.

## Role In Mica

`micac` is a development tool for compiling Mica fileins into a fresh database.
It is not the runtime, not a REPL, and not a daemon. It is useful in CI for
validating that a set of fileins compiles cleanly (`--check`) and for
pre-building databases from source fileins before deployment.
