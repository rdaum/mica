# mica-daemon

`mica-daemon` starts a Mica runtime and links the line-oriented TCP host in
process by default.

The TCP listener itself lives in `mica-host-tcp`. Keeping it outside the daemon
lets the same host shape run either linked in process or as an out-of-process
host over the host RPC/IPC protocol.

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

To expose the host RPC socket for an out-of-process host:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock
cargo run --bin mica-tcp-host -- --rpc ipc:///tmp/mica-rpc.sock --bind 127.0.0.1:7778
```
