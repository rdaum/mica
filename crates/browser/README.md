# mica-browser

`mica-browser` is the browser-oriented Mica package. It is intentionally small:
it links the compiler, register VM, value layer, and non-durable relation
kernel core without the server runtime, driver, or Fjall persistence provider.

The crate currently exists to keep the browser build honest. Its exported smoke
functions force the WASM artefact to retain the compiler, VM execution path, and
projected relation store so size checks are not measuring an empty module. The
VM smoke runs over `ProjectedStore` and uses the client builtin surface, not the
server `RelationKernel` or server builtin context.
