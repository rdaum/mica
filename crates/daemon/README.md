# mica-daemon

`mica-daemon` is the first concrete network transport for Mica.

It is intentionally small: one compio TCP listener, one endpoint identity per
connection, line input delivered through the driver `read()` path, and routed
effects written back to the matching socket. Its purpose is to pressure-test the
endpoint/session model before committing to a larger daemon, IPC, or
multi-protocol architecture.

The daemon currently files in Mica source files at startup. By default it loads
`examples/mud-core.mica`, `examples/string.mica`, and
`examples/mud-command-parser.mica`. Line input is submitted to the in-core
`:command(...)` verb; the Rust transport only keeps connection-control commands
such as `quit`.

Try it with:

```sh
cargo run --bin mica-daemon -- --bind 127.0.0.1:7777
```

Then connect with a line-oriented TCP client and try commands such as `look`,
`get coin`, `put coin box`, `north`, and `say hello`.
