# mica-daemon

`mica-daemon` starts a Mica runtime and, for now, links the line-oriented TCP
host in process.

The TCP listener itself lives in `mica-host-tcp`. Keeping it outside the daemon
lets the same host shape run either linked in process or, later, as an
out-of-process host over the host RPC/IPC protocol.

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
