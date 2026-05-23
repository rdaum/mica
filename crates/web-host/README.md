# mica-web-host

`mica-web-host` is the first HTTP host surface for Mica.

It is deliberately small: HTTP/1.1 parsing uses [`httparse`](https://docs.rs/httparse/)
and socket I/O uses `compio` directly. This crate does not use a web framework,
Hyper, Axum, or Tokio. HTTP/2, HTTP/3, TLS, and richer host protocol
integration are deferred here; browser WebTransport lives in
`crates/webtransport-host`.

The standalone binary serves a tiny built-in health/root surface. When linked
in process by `mica-daemon --web-bind`, parsed requests are installed as
transient request facts and submitted to the Mica `:http_request(...)` verb.
The daemon opens a request-scoped endpoint, derives handler authority from the
configured web principal, and encodes the task return value as the HTTP
response.

The same host now also exposes a browser DOM-sync SSE surface under `/sync`:

- `GET /sync/events?session=<u64>` opens a chunked `text/event-stream`.
- `POST /sync/input` accepts binary sync envelopes from the browser bootstrap.

Each browser sync session gets a durable Mica endpoint, so MUD-style UI session
facts can stay keyed by `endpoint()` across multiple HTTP requests without
introducing Tokio or a web framework.

Run the standalone host:

```sh
cargo run --bin mica-web-host -- --bind 127.0.0.1:8080
```

Smoke-test it:

```sh
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/
```

Run through the daemon and Mica route:

```sh
cargo run --bin mica-daemon -- \
  --filein apps/web/http-core.mica \
  --web-bind 127.0.0.1:8080
curl -i http://127.0.0.1:8080/hello
```

The default web principal is `#web`; use `mica-daemon --web-principal NAME` to
select another identity with suitable `CanRead` and `CanInvoke` policy facts.

The codec is factored separately from the listener so request parsing can be
tested and evolved without involving sockets.
