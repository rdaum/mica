# mica-daemon

`mica-daemon` starts a Mica runtime and exposes host endpoints.

The telnet listener itself lives in `mica-telnet-host`, and the HTTP listener
lives in `mica-web-host`. Keeping hosts outside the daemon lets the same host
shape run either linked in process or as an out-of-process host over the host
RPC/IPC protocol.

The daemon currently files in Mica source files at startup. By default it loads
`examples/string.mica`, `examples/events.mica`, `examples/mud-core.mica`,
`examples/event-substitutions.mica`,
`examples/mud-command-parser.mica`, and `examples/http-core.mica`. Line input
is submitted to the in-core `:command(...)` verb; HTTP requests are submitted
to the in-core `:http_request(...)` verb. The Rust transports only own protocol
parsing and connection control.

Run the daemon with an in-process telnet listener:

```sh
cargo run --bin mica-daemon -- --telnet-bind 127.0.0.1:7777
```

Then connect with a telnet client and try commands such as `look`,
`get coin`, `put coin box`, `north`, and `say hello`.

Run the daemon with a ZeroMQ RPC listener and a separate telnet host:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock
cargo run --bin mica-telnet-host -- --rpc ipc:///tmp/mica-rpc.sock --bind 127.0.0.1:7778
```

The daemon may also expose both surfaces in one process:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock --telnet-bind 127.0.0.1:7777
```

Run the daemon with an in-process HTTP listener:

```sh
cargo run --bin mica-daemon -- --web-bind 127.0.0.1:8080
curl -i http://127.0.0.1:8080/hello
```
