# mica-relation-kernel

`mica-relation-kernel` is the live relational storage layer for Mica. It owns
relations, facts, snapshots, transactions, derived rules, dispatch matching,
and the first cut of query execution.

This crate is deliberately not a SQL engine. Its job is to support Mica's
object-as-identity model: n-ary set relations, transaction-local
`assert`/`retract`, read-your-own-writes, relation metadata, rules, and
inspection views.

## What's Here

- `src/kernel.rs`: `RelationKernel`, the snapshot-published entry point.
- `src/transaction.rs`: transaction overlays, assertions, retractions,
  conflict checks, and commit.
- `src/snapshot.rs`: immutable snapshot state, commit records, and fact changes.
- `src/metadata.rs`: relation schemas, conflict policies, and index specs.
- `src/index.rs`: in-memory relation indexes.
- `src/tuple.rs` and `src/fact.rs`: tuple and fact representations.
- `src/catalog.rs`: relational representation of relation metadata.
- `src/rules.rs`: Datalog-style rule definitions and evaluation.
- `src/materialized.rs`: materialisation of derived rule results.
- `src/query.rs`: simple query plans and join execution.
- `src/dispatch.rs`: role-based method applicability matching.
- `src/closure.rs`: delegation closure helpers.
- `src/neighborhood.rs`: object-neighbourhood inspection views.
- `src/provider.rs`: commit provider boundary, including the in-memory
  provider.
- `src/commit_bloom.rs`: compact write-set tracking for conflict checks.
- `src/tests.rs`: integration-style kernel tests.

## Role In Mica

The kernel is the authoritative world state for the current prototype. The
runtime opens transactions against it, the compiler installs methods and rules
into it, and the runner refreshes its compile context from its catalogue facts.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
