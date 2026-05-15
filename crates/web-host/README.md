# mica-web-host

`mica-web-host` is the first HTTP host surface for Mica.

It is deliberately small: HTTP/1.1 parsing uses [`httparse`](https://docs.rs/httparse/)
and socket I/O uses `compio` directly. This crate does not use a web framework,
Hyper, Axum, or Tokio. HTTP/2, HTTP/3, TLS, WebTransport, and richer host
protocol integration are deferred.

The standalone binary serves a tiny built-in health/root surface. When linked
in process by `mica-daemon --web-bind`, parsed requests are installed as
transient request facts and submitted to the Mica `:http_request(...)` verb.
The task return value is encoded as the HTTP response. Emissions are reserved
for later streaming endpoints such as SSE or WebSocket.

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
cargo run --bin mica-daemon -- --web-bind 127.0.0.1:8080
curl -i http://127.0.0.1:8080/hello
```

The codec is factored separately from the listener so request parsing can be
tested and evolved without involving sockets.
