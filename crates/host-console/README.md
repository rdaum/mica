# mica-host-console

`mica-host-console` is an interactive testing console for the Mica host
protocol.

It connects to a daemon ZeroMQ endpoint, opens one host endpoint, and then
treats ordinary input lines like telnet input: the console starts a
`read(:line)` task, submits the line as endpoint input, and then invokes the
demo `:command(...)` verb with the selected actor.

Slash commands expose the lower-level protocol surface:

```text
/help
/status
/actor alice
/open
/close
/source 'emit(actor(), "hi")'
/drain
/quit
```

Run a daemon RPC listener, then connect the console:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock
cargo run --bin mica-host-console -- --rpc ipc:///tmp/mica-rpc.sock --actor alice
```
