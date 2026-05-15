# mica-host-tcp

`mica-host-tcp` is the line-oriented TCP host transport for Mica.

The crate owns socket handling, line framing, endpoint output buffering, and the
small amount of host-side command submission needed by the current MUD-style
example. It is deliberately outside `mica-daemon` so the same listener shape can
run in two deployment modes:

- in process, linked directly to a `CompioTaskDriver`;
- out of process, talking to a daemon over the host RPC/IPC protocol.

The in-process mode is used by `mica-daemon` by default. The ZeroMQ-backed mode
is exposed by the `mica-tcp-host` binary:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock
cargo run --bin mica-tcp-host -- --rpc ipc:///tmp/mica-rpc.sock --bind 127.0.0.1:7778
```

The RPC-backed host resolves the configured actor name through the daemon with a
`ResolveIdentity` request before opening each endpoint. This is a development
path for local IPC; real remote admission should use grant validation rather
than trusting a command-line actor name.
