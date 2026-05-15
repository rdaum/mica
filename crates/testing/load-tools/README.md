# mica-load-tools

`mica-load-tools` contains standalone load-test binaries for Mica.

`direct-driver-load-test` is the Mica equivalent of a direct scheduler load
test: it bypasses TCP transport, seeds a small world in-process, and submits
role-dispatch invocations directly through compio dispatcher workers. The
measured path is dispatcher scheduling, runtime task execution, VM dispatch,
and relation kernel method lookup.

The text output reports `per_dispatch` from cumulative worker execution time,
matching Moor's `Per-Verb` column. `amort_dispatch` is the wall-clock
amortized value across all workers.

By default the tool uses an in-memory runner. Pass `--store <path>` to use a
fresh Fjall store for setup and commits.

Run it in release mode for representative numbers:

```sh
cargo run --release --bin direct-driver-load-test -- \
  --min-concurrency 1 \
  --max-concurrency 32 \
  --num-objects 1 \
  --num-dispatch-iterations 7000 \
  --num-invocations 200 \
  --instruction-budget 1000000000 \
  --dispatcher-threads 8
```
