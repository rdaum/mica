# mica-host-tcp

`mica-host-tcp` is the line-oriented TCP host transport for Mica.

The crate owns socket handling, line framing, endpoint output buffering, and the
small amount of host-side command submission needed by the current MUD-style
example. It is deliberately outside `mica-daemon` so the same listener shape can
run in two deployment modes:

- in process, linked directly to a `CompioTaskDriver`;
- out of process, talking to a daemon over the host RPC/IPC protocol.

The in-process mode is implemented first. The ZeroMQ-backed host mode should use
the same listener/session shape, but get endpoint creation, task submission, and
output draining through `mica-host-protocol` and `mica-host-zmq`.
