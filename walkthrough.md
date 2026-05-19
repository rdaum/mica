# Async Driver Refactor Walkthrough

`CompioTaskDriver` no longer blocks each dispatched task by creating a fresh
compio runtime. The driver submission, invocation, resume, and input paths now
await the dispatcher receiver directly on the caller's compio runtime.

## Changes Made

### `mica-driver`

- `submit_source`, `submit_source_report`, `submit_source_as_actor`,
  `submit_invocation`, `resume`, and `input` are async.
- `dispatch` awaits the `compio::dispatcher` receiver directly.
- Recursive follow-up work has been refactored into a loop-based
  `process_outcome_queue`, avoiding `Box::pin` recursion on the task outcome
  path.
- Shared transient relation visits materialize matching rows before invoking
  visitor callbacks, so concurrent web requests do not deadlock on a pending
  transient-store writer and a nested dispatch read.
- `open_endpoint`, `open_endpoint_with_context`, and `close_endpoint` remain
  synchronous because they are direct runtime state operations rather than
  dispatched task execution.

### `mica-runner`

- The runner uses a top-level compio runtime and awaits driver submission.
- REPL follow-up polling uses compio sleeps instead of thread sleeps.

### `mica-telnet-host` & `mica-daemon`

- Both hosts await driver calls that submit source or deliver input.
- Tests have been updated to run async driver operations inside a compio
  runtime.

## Verification Results

### Automated Tests

Verified commands:

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

### Performance Results

`scratch/measure_latency.sh` measures HTTP latency through
`mica-daemon --web-bind`. It is a rough correctness and regression check, not a
formal benchmark. The previous sub-0.1 ms result was not reliable; see
[latency_comparison.md](./latency_comparison.md) for the current measurements
and caveats.

## Future Work

- Add `RequestActor(req, actor)` from the existing in-process web actor binding.
- Implement the relational HTTP router demo after that fact is covered by tests.
