# mica-webtransport-host

`mica-webtransport-host` is the browser WebTransport host surface for Mica.

The listener runs on `compio-quic` and Hyperium `h3` directly. It does not use
Tokio as a runtime, `quinn`, `wtransport`, Hyper, or an adapter thread. The
current `h3` stack does pull in the Tokio crate for trait types, but compio owns
the socket, QUIC endpoint, task spawning, and HTTP/3 connection polling.

When linked in process by `mica-daemon --webtransport-bind`, accepted
WebTransport sessions open Mica endpoints with protocol `#webtransport`.
Incoming WebTransport datagrams resume tasks waiting on endpoint input as
`#bytes(...)` values. Mica emissions targeted at the endpoint are sent back to
the client as WebTransport datagrams; byte values are preserved and other values
are encoded as text.

This first host slice is intentionally datagram-only. It accepts one
WebTransport session per HTTP/3 connection; unidirectional and bidirectional
WebTransport streams are reserved for a later endpoint contract.

Run through the daemon with a certificate and private key:

```sh
cargo run --bin mica-daemon -- \
  --webtransport-bind 127.0.0.1:4433 \
  --webtransport-cert cert.pem \
  --webtransport-key key.pem
```

The default WebTransport principal is `#web`; use
`mica-daemon --webtransport-principal NAME` to select another identity with
suitable endpoint authority.
