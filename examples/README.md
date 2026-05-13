# Mica Examples

`mud-core.mica` is a small room/object exercise for relations, recursive rules,
and verb dispatch.

`capabilities.mica` shows the intended bootstrap shape for capabilities. It
declares `Name` as a functional binary relation, then stores durable policy
facts such as `GrantRead(#bob, :Name)` and `GrantInvoke(#alice, :polish)`.
Those facts are not live capability values. When source is run with `--actor`,
the runner resolves the actor identity, reads those policy facts, and mints
ephemeral task capabilities for that run.

Try the capability example with a persistent store:

```sh
cargo run --bin mica -- --storage fjall --store demo-db filein --unit caps --replace examples/capabilities.mica
cargo run --bin mica -- --storage fjall --store demo-db --actor alice eval ':polish(actor: #alice, item: #lamp)'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval 'return #lamp.name'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval '#lamp.name = "stolen"'
```

The first actor invocation succeeds and emits an effect because Alice has read,
write, invoke, and effect grants. Bob can read the lamp name, but the write
attempt is denied because he has no `GrantWrite(#bob, :Name)` fact.
