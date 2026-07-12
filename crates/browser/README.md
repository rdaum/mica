# mica-browser

`mica-browser` is the crate for running Mica in WASM and browser-facing client environments.

It exists for cases where you want to compile and run Mica code in a client environment, against
projected or client-owned state, without pulling in the full server runtime.

It bundles the parts of Mica that fit that job:

- the compiler;
- the register VM;
- the value layer;
- the projected relation store.

It does **not** include the server runtime, task driver, or durable storage providers. This crate is
for running compiled Mica code over client-owned or client-projected state, not for hosting the full
server.

Today the exported C ABI is still very small:

- `mica_browser_abi_version()`
- `mica_browser_projected_store_smoke()`
- `mica_browser_compile_vm_smoke()`

Those smoke entry points are there to verify that the WASM artefact still links the compiler, VM,
client builtin surface, and projected store. They make sure size checks and integration work are
measuring a real client-side slice of Mica rather than an almost-empty module.

The important boundary is that browser execution runs over `ProjectedStore` and the client builtin
context. Server-only pieces such as the runtime host environment and durable relation kernel
providers stay out of this crate.
