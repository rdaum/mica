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
  --filein examples/sync-host.mica \
  --filein examples/chat-sync.mica \
  --filein examples/sync-dom.mica \
  --filein examples/chat-http.mica \
  --web-bind 127.0.0.1:8008 \
  --webtransport-bind 127.0.0.1:4433 \
  --webtransport-cert cert.pem \
  --webtransport-key key.pem
```

The default WebTransport principal is `#web`; use
`mica-daemon --webtransport-principal NAME` to select another identity with
suitable endpoint authority.

`examples/chat-http.mica` serves `/chat` through the daemon HTTP host after
`examples/sync-host.mica`, `examples/chat-sync.mica`, and
`examples/sync-dom.mica` have been loaded.
The initial response is a Mica-rendered HTML document with the chat DOM already
mounted, `data-view`, `data-revision`, `data-signature`, a WebTransport endpoint
URL, and a small bootstrap script. The bootstrap loads `/sync-client.js`, sends
`HaveView` over WebTransport, and then applies server DOM patch deltas into the
page. `browser-smoke.html` remains available as a protocol inspection page when
this directory is served separately.

For an untrusted local certificate, use a short-lived ECDSA certificate and put
the hex SHA-256 hash of the DER certificate in the `Certificate SHA-256` field.
Browsers use the `serverCertificateHashes` option for that path, so the daemon
can still run with a temporary local certificate rather than a locally trusted
CA. The chat page accepts the certificate hash as a `certHash` query parameter.
The smoke page accepts the same values as query parameters, including
`auto=need` for a one-shot protocol run.
