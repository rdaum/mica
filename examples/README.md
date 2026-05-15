# Mica Examples

`mud-core.mica` is a small room/object exercise for relations, rules, and verb
dispatch. `string.mica` documents the low-level string utility surface, and
`mud-command-parser.mica` parses the demo command language in Mica source. These
are the default fileins for `mica-daemon`, so commands such as `look`,
`get coin`, `put coin box`, `north`, and `say hello` can exercise telnet endpoint
input and routed effects.

Try the MUD example over telnet:

```sh
cargo run --bin mica-daemon -- --telnet-bind 127.0.0.1:7777
```

`capabilities.mica` shows the intended bootstrap shape for capabilities. It
declares `Name` as a functional binary relation, then describes policy through
roles and surfaces. Derived `CanRead`, `CanWrite`, `CanInvoke`, and `CanEffect`
relations are the effective policy consumed by the runner. Those facts are not
live capability values. When source is run with `--actor`, the runner resolves
the actor identity, reads the effective policy, and mints ephemeral task
capabilities for that run.

Try the capability example with a persistent store:

```sh
cargo run --bin mica -- --storage fjall --store demo-db filein --unit caps --replace examples/capabilities.mica
cargo run --bin mica -- --storage fjall --store demo-db --actor alice eval ':polish(actor: #alice, item: #lamp)'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval 'return #lamp.name'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval '#lamp.name = "stolen"'
```

The first actor invocation succeeds and emits an effect because Alice has the
`#builder` role, which can inspect and edit `Name`, invoke the `:maintenance`
surface, and emit effects. Bob has the `#visitor` role, so he can read the lamp
name, but the write attempt is denied.
