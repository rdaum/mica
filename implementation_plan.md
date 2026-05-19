# Web Host and Driver Refinement Plan

## Current Status

- [x] `CompioTaskDriver` submission, invocation, resume, and input paths are async.
- [x] The driver no longer creates a fresh compio runtime for each dispatch.
- [x] Recursive task follow-up handling now uses a loop-based outcome queue.
- [x] `mica-runner`, `mica-telnet-host`, `mica-daemon`, and `mica-web-host` call sites have been updated for the async driver surface.
- [ ] The relational HTTP router demo has not started.

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

## Next Slice: `RequestActor`

The next implementation step is deliberately small:

- add `RequestActor(req, actor)` as a transient request fact in
  `crates/web-host/src/request.rs`;
- use the actor identity already passed to `handle_in_process_request`;
- clean up `RequestActor` with the rest of the request facts;
- add a focused request-fact test for `RequestActor`.

Do not add HTTP authentication parsing in this slice. The in-process web host
already receives an `ActorBinding` from the daemon. Header-based authentication
can be a later host concern if there is a concrete design for it.

## Router Demo Follow-Up

After `RequestActor` is in place, add a complete HTTP router filein instead of
silently depending on `examples/http-core.mica`.

Suggested relation shape:

```mica
make_relation(:RequestActor, 2)
make_relation(:HttpRoute, 3)      // method, path, handler
make_relation(:CanAccess, 2)      // actor, path
make_relation(:RouteMatch, 2)     // request, handler
make_relation(:HasRouteMatch, 1)
make_relation(:DeniedRequest, 1)
make_relation(:SelectedRoute, 2)
```

Suggested rule shape:

```mica
RouteMatch(req, handler) :-
  RequestMethod(req, method),
  RequestPath(req, path),
  HttpRoute(method, path, handler)

HasRouteMatch(req) :-
  RouteMatch(req, handler)

DeniedRequest(req) :-
  RequestActor(req, actor),
  RequestPath(req, path),
  not CanAccess(actor, path)

SelectedRoute(req, :denied) :-
  DeniedRequest(req)

SelectedRoute(req, handler) :-
  RouteMatch(req, handler),
  not DeniedRequest(req)

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
  --filein examples/http-router.mica \
  --web-bind 127.0.0.1:8080
```

Then verify matched, missing, and denied routes with `curl`.
