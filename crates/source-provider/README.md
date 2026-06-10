# mica-source-provider

`mica-source-provider` exposes source code browsing as Mica computed relations.
It provides repository entry listings, file text, syntax highlighting, semantic
symbol search, definition-at and references-of lookups, text search, and VCS
history through the same relation interface that Mica code uses for all other
data.

## What's Here

- `src/index.rs`: persistent semantic index format, loading, and source index
  file building. Indexes symbols, references, and text chunks from a set of
  source root directories.
- `src/syntax.rs`: `SyntaxDocument` parsing with tree-sitter (Rust, JavaScript,
  Markdown) and Mica's native lexer for syntax highlighting and outline
  extraction.
- `src/rust_analyzer.rs`: `RustAnalyzerProvider`, a managed rust-analyzer LSP
  process pool for definition and references queries with session reuse and
  automatic document synchronization.
- `src/navigation.rs`: semantic location resolution, symbol identity encoding,
  and byte-offset-to-LSP-position conversion.
- `src/relations.rs`: computed relations that bind together local file system
  access, syntax parsing, semantic index queries, rust-analyzer LSP results,
  and VCS history into the Mica relation model. Includes `RepositoryEntry(6)`,
  `FileText(5)`, `FileLines(7)`, `SyntaxLine(8)`, `SyntaxOutline(10)`,
  `SyntaxNodeAt(11)`, `DefinitionAt(13)`, `ReferencesOf(10)`,
  `SymbolSearch(11)`, `IndexedTextUnit(9)`, `TextSearch(11)`, index metadata,
  and VCS relations for commits, diffs, logs, blame, and file history.
- `src/vcs.rs`: `VcsProvider`, a git-backed version control reader using
  `jj-lib` (GitBackend). Supports commit metadata, tree traversal, file
  content, diffs with unified diff output, blame, commit log walking, and
  commit text search.
- `src/util.rs`: shared helpers for relation binding extraction, path
  validation, content hashing, row filtering, and error construction.

## Role In Mica

This crate produces computed relations that are installed into a Mica runtime
through `default_computed_relations()`. Those relations make source code
browsing and repository history available to Mica verbs and rules, so Mica
applications can query the local file system and git history through the same
relation interface they use for the rest of the world.

Access is restricted to files under configured source roots
(`MICA_SOURCE_ROOTS` or `MICA_SOURCE_ROOT`). A persistent semantic index can be
pre-built with `build_source_index_file` and loaded from the path given by
`MICA_SOURCE_INDEX`.

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
