# mica-runtime

`mica-runtime` is Mica's register-based task runtime. It executes compiled
programs against `mica-relation-kernel` transactions and reports completion,
commit, retry, suspension, effects, and runtime errors.

The runtime is intentionally register-based rather than stack-based. Programs
operate over explicit registers, relation instructions, control-flow
instructions, exception handling, function calls, dispatch, and builtin calls.

## What's Here

- `src/program.rs`: bytecode format, `Program`, instructions, operands,
  registers, serialisation, and validation.
- `src/vm.rs`: register VM state, frames, instruction execution, and host
  response boundaries.
- `src/task.rs`: task lifecycle, transaction boundaries, retries, and limits.
- `src/task_manager.rs`: task submission, immediate completion, effect logging,
  suspended tasks, and kernel ownership.
- `src/builtin.rs`: builtin function registry and builtin call context.
- `src/error.rs`: runtime, task manager, and task error types.
- `src/tests.rs`: runtime and task manager tests.

## Role In Mica

The runtime sits between compiled Mica programs and the relation kernel. It
turns executable source into transactional world changes: relation assertions,
retractions, queries, dispatch, builtin calls, effects, and return values.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
