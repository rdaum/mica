# mica-vm

`mica-vm` is Mica's register-based bytecode execution core. It defines compiled
program artifacts and runs them until they reach a host boundary such as
completion, commit, suspension, builtin call handling, dispatch, or error.

The VM is intentionally register-based rather than stack-based. Programs
operate over explicit registers, relation instructions, control-flow
instructions, exception handling, function calls, dispatch, and builtin calls.

## What's Here

- `src/program.rs`: bytecode format, `Program`, instructions, operands,
  registers, serialisation, and validation.
- `src/vm.rs`: register VM state, frames, instruction execution, and host
  response boundaries.
- `src/builtin.rs`: builtin function registry and builtin call context.
- `src/error.rs`: VM runtime error types.

## Role In Mica

The VM is the executable substrate used by `mica-runtime`. It does not parse
source, own task scheduling, filein/fileout, or the command-line interface.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
