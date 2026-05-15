# mica-web-host

`mica-web-host` is the first HTTP host surface for Mica.

It is deliberately small: HTTP/1.1 parsing uses [`httparse`](https://docs.rs/httparse/)
and socket I/O uses `compio` directly. This crate does not use a web framework,
Hyper, Axum, or Tokio. HTTP/2, HTTP/3, TLS, WebTransport, routing into Mica
tasks, and richer host protocol integration are deferred.

Run the standalone host:

```sh
cargo run --bin mica-web-host -- --bind 127.0.0.1:8080
```

Smoke-test it:

```sh
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/
```

The codec is factored separately from the listener so request parsing can be
tested and evolved without involving sockets.
