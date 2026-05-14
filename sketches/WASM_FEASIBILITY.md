# WASM Feasibility Notes

Mica can be split into two relation-kernel shapes:

- server/runtime Mica: `RelationKernel`, MVCC transactions, commit providers,
  Fjall durability, task management, network drivers, and authority refresh;
- browser/projected Mica: value layer, tuples, indexes, relation metadata,
  query plans, rules, dispatch helpers, and `ProjectedStore`.

The workspace dependency for `mica-relation-kernel` disables durable providers
by default. Server-facing crates opt into `fjall-provider`; compiler, VM, and
browser-facing crates do not.

## Interfaces

`RelationRead` is the shared read-side interface used by query plans, rule
evaluation, dispatch matching, program resolution, snapshots, transactions,
transient composition, and projected stores.

`RelationWorkspace` is the shared mutation interface for code that needs to
assert, retract, replace functional facts, or retract a query result without
knowing whether it is operating over a server transaction or a projected
single-user store.

`ProjectedStore` is not a miniature persistent server. It is a non-durable
relation slice:

- applies commit-shaped deltas from a host/server;
- supports local immediate mutation through `RelationWorkspace`;
- evaluates derived relations over projected facts;
- avoids commit providers, durable acknowledgement, conflict retry, and
  restart recovery.

That shape fits browser-hoisted UI state, agent workspaces, and other clients
that receive selected relation subsets rather than complete copies of the
world.

## Current WASM Check

The `mica-browser` crate links:

- `mica-var`;
- `mica-relation-kernel` without durable providers;
- `mica-vm`;
- `mica-compiler`.

It exports smoke functions that deliberately retain:

- projected relation store creation and mutation;
- Mica source compilation;
- register VM execution against `ProjectedStore`.

Commands run:

```sh
rustup target add wasm32-unknown-unknown
cargo check -p mica-var --target wasm32-unknown-unknown
cargo check -p mica-relation-kernel --no-default-features --target wasm32-unknown-unknown
cargo check -p mica-vm --target wasm32-unknown-unknown
cargo check -p mica-compiler --target wasm32-unknown-unknown
cargo check -p mica-browser --target wasm32-unknown-unknown
cargo build --release -p mica-browser --target wasm32-unknown-unknown
```

Release artefact size:

```text
target/wasm32-unknown-unknown/release/mica_browser.wasm: 831312 bytes
```

This is an unstripped, un-`wasm-opt`ed release build. No WASM size tooling was
installed in the local environment during this check.

## Node/WebAssembly Smoke Timing

Node v25.2.1 can instantiate and run the release WASM artefact directly.

Measured exports:

```text
mica_browser_abi_version() -> 1
mica_browser_projected_store_smoke() -> 1
mica_browser_compile_vm_smoke() -> 10
```

One run of 1,000 calls each:

```text
sum 11000
projected_ms_per_call 0.0013937589999999994
compile_vm_ms_per_call 0.010615288999999997
```

These numbers are only smoke-level evidence. The compile/VM function compiles a
tiny source fragment that asserts `Name(#lamp, "brass lamp")` and queries it
back through `one Name(#lamp, ?name)`, all against `ProjectedStore`. The
projected-store function creates one relation and does one functional replace.
They show that the package is viable in a browser-class WebAssembly engine, not
that realistic UI or rule workloads are fast enough yet.

## Browser Smoke Timing

Firefox was installed for Playwright during this check, and the release WASM
artefact was fetched from a local static server into a real browser page.

Browser results:

```text
abi 1
projected 1
compileVm 10
byteLength 831312
projected_ms_per_call 0.005
compile_vm_ms_per_call 0.047
```

The browser result is slower than the Node result, but still comfortably small
for this tiny smoke. The important current finding is not the exact timing; it
is that the browser can instantiate the package, execute projected relation
logic, compile source, and run VM bytecode over `ProjectedStore` from the same
WASM artefact.

## Remaining Design Work

The VM can now execute bytecode over either a server transaction host or a
projected workspace host. The next browser/runtime split is builtin policy:
browser-hosted code should get a deliberately small client builtin surface
rather than the server runtime builtin registry.
