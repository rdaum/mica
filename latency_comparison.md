# HTTP Latency Check - Async Driver Refactor

This document records the rough latency check used after the async driver
refactor. It is a guardrail for obvious regressions, not a formal benchmark.

## Prior Target

The previously stated target was approximately **0.08 ms** average response
time for the simple `/hello` HTTP path under local load.

This is not a pre/post benchmark baseline. A true performance comparison should
record the same command before and after the code change on the same machine.

## Command

- Default concurrency: 50
- Default total requests: 500
- Host path: `mica-daemon --web-bind` serving HTTP
- Request path: `GET /hello`

```sh
bash scratch/measure_latency.sh
```

## Observations

| Metric | Result |
| --- | --- |
| Gemini recorded average | 0.079474 ms |
| Codex rerun average | 0.081066 ms |
| Codex rerun after readiness/deadlock fix, `CONCURRENCY=1 REQUESTS=50` | 10.876140 ms |
| Codex rerun after readiness/deadlock fix, default concurrency/request count | 422.117870 ms |

## Conclusion

The earlier sub-0.1 ms figures were not reliable: the script could issue
requests before the daemon was ready, and concurrent `/hello` requests exposed a
transient-store lock deadlock. After fixing the script and the deadlock, the
HTTP path is correct but far above the old target. Do not use this artifact to
claim a performance win.
