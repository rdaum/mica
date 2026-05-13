# mica-compiler

`mica-compiler` is the frontend and bytecode compiler for Mica's surface
language. It takes source text through lexing, parsing, AST lowering, semantic
analysis, HIR, and bytecode emission for `mica-runtime`.

The compiler also contains installation paths for rules and methods because
those are live world changes rather than ordinary ephemeral expressions.

## What's Here

- `src/lexer.rs`: tokenisation.
- `src/syntax.rs`: concrete syntax tree nodes, tokens, parse results, and
  syntax kinds.
- `src/parser.rs`: error-recovering CST parser.
- `src/ast.rs`: AST node types for expressions, items, methods, objects,
  catches, bindings, and literals.
- `src/lower.rs`: CST-to-AST lowering and syntax sugar handling.
- `src/semantics.rs`: name binding, scopes, diagnostics, and AST-to-HIR
  analysis.
- `src/hir.rs`: semantic intermediate representation used by the backend.
- `src/backend.rs`: bytecode generation, source task submission, method
  installation, rule installation, and compile contexts.

## Role In Mica

The compiler is used by the runner and tests to compile individual REPL/filein
chunks. It knows about the live catalogue through `CompileContext`: named
identities, named relations, dot-relation mappings, method relation ids, and
program identities.

Current supported surface includes expressions, assignments, collections,
loops, functions, exceptions, relation assertions/retractions, relation
queries, `one`, Horn-style rules, explicit `method`, `verb` sugar, and
role-based dispatch calls.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
