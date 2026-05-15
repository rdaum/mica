# mica-telnet-host

`mica-telnet-host` is the telnet host transport for Mica.

The crate owns socket handling, telnet line framing, endpoint output buffering,
and the small amount of host-side command submission needed by the current
MUD-style example. It is deliberately outside `mica-daemon` so the same listener
shape can run in two deployment modes:

- in process, linked directly to a `CompioTaskDriver`;
- out of process, talking to a daemon over the host RPC/IPC protocol.

The in-process mode is exposed by `mica-daemon --telnet-bind`. The
ZeroMQ-backed mode is exposed by the `mica-telnet-host` binary:

```sh
cargo run --bin mica-daemon -- --rpc-bind ipc:///tmp/mica-rpc.sock
cargo run --bin mica-telnet-host -- --rpc ipc:///tmp/mica-rpc.sock --bind 127.0.0.1:7778
```

The RPC-backed host resolves the configured actor name through the daemon with a
`ResolveIdentity` request before opening each endpoint. This is a development
path for local IPC; real remote admission should use grant validation rather
than trusting a command-line actor name.

Input is decoded with a compio-facing telnet codec: CR, LF, and CRLF all end
text lines; output lines are written as CRLF; telnet IAC command sequences are
parsed out of the text stream; and binary mode is represented separately for
future host/runtime effects.
