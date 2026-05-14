# mica-daemon

`mica-daemon` is the first concrete network transport for Mica.

It is intentionally small: one blocking TCP listener, one endpoint identity per
connection, line input delivered through the driver `read()` path, and routed
effects written back to the matching socket. Its purpose is to pressure-test
the endpoint/session model before committing to a larger daemon, IPC, or
multi-protocol architecture.

The daemon currently files in a Mica source file at startup, defaults to
`examples/mud-core.mica`, and maps a small line command surface onto Mica verb
invocations.

Try it with:

```sh
cargo run --bin mica-daemon -- --bind 127.0.0.1:7777
```

Then connect with a line-oriented TCP client and try commands such as `look`,
`get coin`, `put coin box`, `north`, and `say hello`.
