# Web Host and Driver Refinement Plan

## Current Status

- [x] `CompioTaskDriver` submission, invocation, resume, and input paths are async.
- [x] The driver no longer creates a fresh compio runtime for each dispatch.
- [x] Recursive task follow-up handling now uses a loop-based outcome queue.
- [x] `mica-runner`, `mica-telnet-host`, `mica-daemon`, and `mica-web-host` call sites have been updated for the async driver surface.
- [x] `RequestActor(req, actor)` is asserted as a transient request fact.
- [x] `examples/relational-router.mica` demonstrates relation-driven route
  selection and default-deny access policy.
- [x] The router demo was verified through `mica-daemon --web-bind` with
  matched, denied, and missing routes.

## Phase 1 Acceptance Checks

Required before starting the router demo:

```sh
cargo fmt --all
cargo test -p mica-driver
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
bash scratch/measure_latency.sh
```

The latency script measures HTTP requests through `mica-daemon --web-bind`.
Treat it as a rough threshold check, not a rigorous benchmark. Record the exact
observed value when handing off.

## Relational Router Demo

`examples/relational-router.mica` is a complete HTTP router filein instead of a
silent extension of `examples/http-core.mica`.

Suggested relation shape:

```mica
make_relation(:HttpRoute, 3)      // method, path, handler
make_relation(:CanAccess, 2)      // actor, path
make_relation(:RouteMatch, 2)     // request, handler
make_relation(:HasRouteMatch, 1)
make_relation(:AuthorizedRoute, 2)
make_relation(:DeniedRequest, 1)
make_relation(:SelectedRoute, 2)
```

Suggested rule shape:

```mica
RouteMatch(req, handler) :-
  RequestMethod(req, http_method),
  RequestPath(req, path),
  HttpRoute(http_method, path, handler)

HasRouteMatch(req) :-
  RouteMatch(req, handler)

DeniedRequest(req) :-
  RouteMatch(req, handler),
  RequestActor(req, actor),
  RequestPath(req, path),
  not CanAccess(actor, path)

AuthorizedRoute(req, handler) :-
  RouteMatch(req, handler),
  RequestActor(req, actor),
  RequestPath(req, path),
  CanAccess(actor, path)

SelectedRoute(req, :denied) :-
  DeniedRequest(req)

SelectedRoute(req, handler) :-
  AuthorizedRoute(req, handler)

SelectedRoute(req, :not_found) :-
  HttpRequest(req),
  not HasRouteMatch(req)
```

Keep variables in negated atoms range-restricted by positive atoms in the same
rule body.

## Manual Router Verification

Use an explicit filein list so the active HTTP implementation is unambiguous:

```sh
cargo run --bin mica-daemon -- \
  --filein examples/string.mica \
  --filein examples/events.mica \
  --filein examples/mud-core.mica \
  --filein examples/event-substitutions.mica \
  --filein examples/mud-command-parser.mica \
  --filein examples/relational-router.mica \
  --web-bind 127.0.0.1:8080
```

Then verify matched, missing, and denied routes with `curl`.

Observed local checks:

```sh
curl -i http://127.0.0.1:18080/hello    # 200 hello from the relational router
curl -i http://127.0.0.1:18080/admin    # 403 forbidden
curl -i http://127.0.0.1:18080/missing  # 404 not found
```
