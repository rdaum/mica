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

## Pre/Post Baseline

This release-mode comparison uses `scratch/compare_http_latency.sh`, which
builds and runs isolated worktrees for the before and after refs on the same
machine.

| Field | Value |
| --- | --- |
| Before ref | `335e13a93be6ec17611d88f38066cfd172b02d5b` |
| After ref | `bc6c435796b42cf4f44f7f15b9a4d8cec60cd178` |
| Profile | `release` |
| Sequential run | 50 requests, concurrency 1 |
| Loaded run | 200 requests, concurrency 20 |
| Generated raw output | `scratch/http_baseline/summary.txt` |

| Case | Requests | Concurrency | Average | Min | Max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before `http-core.mica` `/hello` sequential | 50 | 1 | 1.145 ms | 0.929 ms | 1.507 ms |
| After `http-core.mica` `/hello` sequential | 50 | 1 | 1.269 ms | 1.095 ms | 1.657 ms |
| Before `http-core.mica` `/hello` loaded | 200 | 20 | 17.839 ms | 1.373 ms | 24.067 ms |
| After `http-core.mica` `/hello` loaded | 200 | 20 | 17.522 ms | 1.424 ms | 25.625 ms |
| After `relational-router.mica` `/hello` sequential | 50 | 1 | 1.113 ms | 0.941 ms | 1.786 ms |
| After `relational-router.mica` `/hello` loaded | 200 | 20 | 16.042 ms | 1.543 ms | 21.547 ms |
| After `relational-router.mica` `/admin` sequential | 50 | 1 | 1.082 ms | 0.882 ms | 1.329 ms |
| After `relational-router.mica` `/missing` sequential | 50 | 1 | 1.204 ms | 1.073 ms | 1.363 ms |

The core route is effectively flat across the async driver refactor in this
local run: sequential latency is slightly higher after the refactor, while the
loaded average is slightly lower. The router demo is a current-only measurement
because `examples/relational-router.mica` did not exist at the before ref.

## Batched Transient Request Facts

Profiling the current HTTP path with `perf record` showed request fact
assertion and cleanup spending substantial time taking the shared transient
store write lock once per fact. The follow-up change batches request fact
assertion and retraction so each request takes one write lock to install facts
and one write lock to clean them up.

Comparison shape:

| Field | Value |
| --- | --- |
| Before ref | `99af284` |
| After state | working tree with batched transient request facts |
| Profile | `release` |
| Sequential run | 50 requests, concurrency 1 |
| Loaded run | 200 requests, concurrency 20 |
| Route | `http-core.mica` `/hello` |

| Case | Average | Min | Max |
| --- | ---: | ---: | ---: |
| Before sequential | 1.100 ms | 0.893 ms | 1.596 ms |
| After sequential | 1.081 ms | 0.878 ms | 1.558 ms |
| Before loaded | 11.087 ms | 1.349 ms | 16.176 ms |
| After loaded | 6.259 ms | 1.510 ms | 14.884 ms |

The loaded average improved by roughly 44% in this run. A post-change profile
still shows transient index insert/remove work as the dominant Mica-side cost,
especially cleanup, but futex/write-lock overhead is much lower than in the
pre-batching profile.

Because batching makes each write-lock hold larger while reducing the number of
lock handoffs, a follow-up sweep checked the same route under increasing
concurrency:

| Concurrency | Requests | Before average | After average |
| ---: | ---: | ---: | ---: |
| 1 | 500 | 1.233 ms | 1.258 ms |
| 10 | 500 | 4.203 ms | 3.145 ms |
| 20 | 500 | 11.118 ms | 7.382 ms |
| 50 | 500 | 33.765 ms | 26.807 ms |
| 100 | 500 | 65.737 ms | 46.359 ms |

The sequential case is effectively flat; the concurrent cases improve because
the shared transient-store lock is acquired fewer times per HTTP request.
