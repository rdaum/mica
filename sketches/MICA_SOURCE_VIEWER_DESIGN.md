# Mica Source Viewer Design

This document sketches a source viewer hosted by Mica and aimed first at
browsing Mica's own repository. The intended end state is a web application
written mostly in Mica source: it serves a browser UI through the existing HTTP
and DOM sync hosts, indexes the repository, shows files and symbols with
hyperlinks, and lets humans and agents inspect relationships between code,
documentation, issues, reviews, and runtime facts.

The product goal is not a prettier file tree. It is a source workspace where
code can be viewed through multiple relation-shaped surfaces:

- files, directories, commits, spans, symbols, definitions, references, tests,
  docs, and review notes as durable identities;
- syntax and semantic indexes exposed as provider-backed computed relations;
- UI state, annotations, tasks, retrieval traces, and authority as ordinary
  Mica facts;
- browser navigation driven by Mica verbs and DOM sync, not a separate client
  application model.

The first target language can be Rust because Mica is Rust, rust-analyzer is
available, and it gives useful semantic navigation early. The architecture
should not make Rust the product boundary. Tree-sitter, LSP, SCIP/LSIF, text
search, and embeddings should be providers behind relation-shaped queries.

## Current Grounding

Several pieces already exist in the repository:

- `crates/web-host` serves HTTP routes and the SSE DOM sync surface.
- `crates/webtransport-host` serves the same browser sync protocol over
  WebTransport.
- `apps/shared/sync-host.mica` defines request facts and basic HTTP helpers.
- `apps/shared/sync-dom.mica` defines snapshot and mount helpers.
- `apps/mud` demonstrates a server-owned browser UI written mostly in Mica:
  `sync_view_revision(view)` and `sync_view_tree(view, revision)` return the
  DOM state that the host diffs and sends to the browser.
- `apps/mud/ui-mica-inspect.mica` demonstrates relation-driven inspection of
  live Mica facts, rules, methods, prototypes, and authority.
- `apps/shared/retrieval.mica` and `NearestEmbedding` demonstrate the right
  retrieval boundary: providers propose candidates, while ordinary Mica
  relations handle provenance, authority, freshness, and workflow state.
- `crates/relation-kernel/src/computed.rs` already has read-only computed
  relations with required bound positions.

The source viewer should extend those shapes rather than inventing a separate
web framework, database, index model, or agent memory layer.

## Design Thesis

Mica should own the workspace semantics, not every byte of every index.

Heavy data structures should live in sidecars:

- file contents and line maps;
- tree-sitter parse trees and syntax spans;
- rust-analyzer or LSP processes;
- SCIP/LSIF-style durable semantic indexes;
- full-text indexes;
- embedding vectors and approximate nearest-neighbour indexes.

Mica should own the identities and workflow:

- which repositories, revisions, files, symbols, spans, and index builds exist;
- which source spans are currently selected or visible in a user session;
- which symbol links, references, comments, review notes, and agent findings
  have been accepted as workspace facts;
- which index build or provider produced a candidate;
- which actor may read a repository, file, symbol, note, or retrieved context;
- which tasks need refresh after source changes;
- which view state should be rendered to the browser.

This keeps the relation kernel from becoming a general code-index storage
engine, while still making code intelligence programmable from Mica.

## Boundary Between Stored Facts And Providers

Use ordinary durable relations for stable facts that are worth inspecting,
reviewing, editing, authorizing, or joining with other workspace state.

Examples:

```mica
Repository(#repo_mica)
RepositoryRoot(#repo_mica, "/home/ryan/src/mica")

Revision(#rev_worktree)
RevisionOf(#rev_worktree, #repo_mica)
RevisionKind(#rev_worktree, "worktree")

SourceFile(#file_runtime_lib)
FilePath(#file_runtime_lib, "crates/runtime/src/lib.rs")
FileRevision(#file_runtime_lib, #rev_worktree)

Symbol(#symbol_source_runner)
SymbolName(#symbol_source_runner, "SourceRunner")
SymbolKind(#symbol_source_runner, "struct")
SymbolDefinedIn(#symbol_source_runner, #file_runtime_lib)

ReviewNote(#note_1)
NoteSubject(#note_1, #symbol_source_runner)
NoteText(#note_1, "Split host setup from source submission.")
```

Use computed relations for large, volatile, or provider-owned surfaces.

Examples:

```mica
FileText(#repo_mica, #rev_worktree, "crates/runtime/src/lib.rs", ?text, ?content_hash)
FileLines(#repo_mica, #rev_worktree, "crates/runtime/src/lib.rs", 120, 180, ?lines)

SyntaxOutline(#repo_mica, #rev_worktree, "crates/runtime/src/lib.rs", ?node, ?kind, ?name, ?span)
SyntaxNodeAt(#repo_mica, #rev_worktree, "crates/runtime/src/lib.rs", 15234, ?node, ?kind, ?span)

DefinitionAt(#repo_mica, #rev_worktree, "crates/runtime/src/lib.rs", 15234, ?symbol, ?target_path, ?target_span, ?provider)
ReferencesOf(#repo_mica, #rev_worktree, #symbol_source_runner, ?path, ?span, ?provider)

TextSearch(#repo_mica, #rev_worktree, "ComputedRelation", 50, ?path, ?span, ?snippet, ?score)
RelatedCode(#repo_mica, #rev_worktree, #symbol_source_runner, 20, ?subject, ?score, ?reason)
```

The bound arguments matter. A source viewer should not ask Mica to enumerate
every AST node or every reference in the repository as base tuples. It should
ask constrained questions for a repository, revision, path, span, symbol,
query, and limit.

## Product Shape

The hosted app should eventually provide:

- repository and revision selection, starting with the local Mica checkout;
- directory and file browsing;
- fast source rendering with line numbers, anchors, and selected spans;
- syntax highlighting and outline navigation;
- symbol search by name and text search by content;
- go-to-definition and find-references links;
- symbol neighbourhood views showing definitions, references, callers, callees,
  impls, tests, docs, comments, and related retrieval context;
- relation inspection that ties source symbols back to live Mica facts when the
  viewed code is Mica source;
- review notes, TODOs, tickets, and agent findings anchored to spans or symbols;
- retrieval-backed questions over source, docs, reviews, tickets, and runtime
  facts;
- session-aware UI state through the existing DOM sync contract.

The first useful version does not need every item. It needs one coherent path
from repository identity to file view to clickable symbol links.

## Provider Stack

The provider model should be layered rather than choosing one index technology
as the universal answer.

### File Provider

Reads repository snapshots and exposes constrained file surfaces:

- list files under a path;
- read a file by path and revision;
- return a line window;
- map byte offsets to line/column spans;
- compute content hashes.

Early implementation can target a local worktree. Later versions can support
bare Git objects, committed revisions, remote mirrors, and archived snapshots.

### Text Search Provider

Provides filename, symbol-name, and content search. Early versions can wrap
`ripgrep` or an in-process index. Later versions can use a persistent search
index.

The output should be path/span/snippet/score rows, not opaque HTML.

### Tree-sitter Provider

Provides syntax without requiring full language semantics:

- syntax highlighting spans;
- file outline;
- local declarations;
- local references;
- bracket/fold ranges;
- nearest enclosing item at cursor.

Tree-sitter is useful as the baseline because it is fast and language-general.
It does not solve cross-module semantic navigation by itself.

### LSP Provider

Provides interactive semantic queries for a checked-out workspace:

- go to definition;
- find references;
- hover text;
- diagnostics;
- rename preview later;
- code actions later.

For Rust, this provider should use rust-analyzer. For other languages, it can
use the appropriate language server. The provider must report which tool,
version, workspace root, revision, and configuration produced the result.

LSP is not the durable index architecture. It is an interactive provider that
works well for the active revision and active workspace.

### SCIP/LSIF Provider

Provides precomputed semantic indexes when available:

- durable symbol IDs;
- definitions and references across a revision;
- cross-repository references later;
- language-server-independent navigation in hosted mode.

This provider is better for hosted browsing across many revisions because it
does not require one hot language-server process per user and revision.

### Retrieval Provider

Embeddings and retrieval should sit beside, not replace, syntax and semantic
indexes. Retrieval is useful for:

- "show code related to transaction authority";
- "find the tests that explain computed relation binding errors";
- "what files are involved in browser sync?";
- "which review notes mention this symbol?";
- "what design doc led to this implementation?"

The retrieval provider should return candidate subjects. Mica should then join
those subjects to files, spans, symbols, docs, tickets, permissions, and
recorded answer traces.

## Core Relation Vocabulary

These names are provisional, but they describe the shape of the application.

Repository and revision:

```mica
make_relation(:Repository, 1)
make_functional_relation(:RepositoryRoot, 2, [0])
make_relation(:Revision, 1)
make_functional_relation(:RevisionOf, 2, [0])
make_functional_relation(:RevisionKind, 2, [0])
make_functional_relation(:RevisionLabel, 2, [0])
make_functional_relation(:RevisionCommit, 2, [0])
```

Files and spans:

```mica
make_relation(:SourceFile, 1)
make_functional_relation(:FileRevision, 2, [0])
make_functional_relation(:FilePath, 2, [0])
make_functional_relation(:FileContentHash, 2, [0])

make_relation(:SourceSpan, 1)
make_functional_relation(:SpanFile, 2, [0])
make_functional_relation(:SpanStartByte, 2, [0])
make_functional_relation(:SpanEndByte, 2, [0])
make_functional_relation(:SpanStartLine, 2, [0])
make_functional_relation(:SpanEndLine, 2, [0])
```

Symbols:

```mica
make_relation(:Symbol, 1)
make_functional_relation(:SymbolName, 2, [0])
make_functional_relation(:SymbolKind, 2, [0])
make_functional_relation(:SymbolProvider, 2, [0])
make_relation(:SymbolDefinedAt, 2)
make_relation(:SymbolReferenceAt, 2)
make_relation(:SymbolParent, 2)
```

Index builds:

```mica
make_relation(:SourceIndex, 1)
make_functional_relation(:IndexRepository, 2, [0])
make_functional_relation(:IndexRevision, 2, [0])
make_functional_relation(:IndexProvider, 2, [0])
make_functional_relation(:IndexStatus, 2, [0])
make_functional_relation(:IndexVersion, 2, [0])
make_functional_relation(:IndexStartedAt, 2, [0])
make_functional_relation(:IndexFinishedAt, 2, [0])
```

Session state:

```mica
make_functional_relation(:source/View, 2, [0])
make_functional_relation(:source/SelectedRevision, 2, [0])
make_functional_relation(:source/SelectedPath, 2, [0])
make_functional_relation(:source/SelectedSymbol, 2, [0])
make_functional_relation(:source/SelectedSpan, 2, [0])
make_functional_relation(:source/SearchQuery, 2, [0])
make_functional_relation(:source/LayoutMode, 2, [0])
```

Authority:

```mica
make_relation(:CanBrowseRepository, 2)
make_relation(:CanReadSourceFile, 2)
make_relation(:CanReadSourceSpan, 2)
make_relation(:CanInspectSymbol, 2)
make_relation(:CanAnnotateSource, 2)
make_relation(:CanRunSourceProvider, 2)
```

The exact names can change once implementation starts. The important point is
that the app state is relation-shaped and provider outputs can be joined with
that state.

## UI Model

The UI should reuse the MUD sync pattern:

```mica
verb sync_view_revision(view)
  return source/view_revision(view)
end

verb sync_view_tree(view, revision)
  return source/app_node(view, revision)
end
```

The browser receives a server-rendered document with a sync mount. Mica owns
the current selected repository, path, symbol, search query, and panel layout.
Browser events come back as declared sync events:

- select repository;
- select revision;
- open directory;
- open file;
- jump to line or span;
- select symbol;
- search;
- go to definition;
- find references;
- add note;
- ask related-code question.

Source text rendering needs care. Large files should not be represented as one
huge static DOM subtree on every revision. The first version can render small
files directly, but the design should move toward line windows and stable
line-level sync keys:

```text
code pane query:
  repo, revision, path, start_line, line_count

code pane render:
  one row per visible line, stable key = path + line number + content hash
```

Syntax highlighting should be represented as spans over line text, not by
letting the provider return prebuilt HTML. Mica can decide how those spans are
rendered and authorized.

## Implementation Phases

Each phase should leave behind something runnable and inspectable. The early
phases can use local-only providers and generated fileins. The later phases
should converge on a hosted app that can run from `scripts/source.sh` and open
`/source`.

### Phase 0: Design Lock And Seed Fixture

Deliverables:

- this design document;
- a decision on app location, probably `apps/source/`;
- a decision on the first repository identity for the local checkout;
- a tiny source fixture in Mica facts that represents a handful of files and
  spans without any provider process.

Acceptance check:

- the fixture can be loaded by `mica-daemon`;
- a Mica expression can ask for repository, file, and span facts;
- no source-index concepts are hardcoded into the relation kernel.

### Phase 1: Static Source Browser

Build a minimal server-owned source browser over seeded facts.

Deliverables:

- `apps/source/core.mica` with repository, revision, file, span, session, and
  authority relations;
- `apps/source/http.mica` serving `/source`;
- `apps/source/ui-session.mica` and `apps/source/ui-compose.mica` following the
  current MUD sync shape;
- `scripts/source.sh` that starts the daemon with the explicit filein set;
- a page that lists Mica files and opens a selected file in a code pane.

Acceptance check:

- `scripts/source.sh` prints an HTTP URL;
- opening `/source` shows a Mica-authored UI;
- selecting a file changes durable session facts and updates through SSE sync;
- WebTransport can be enabled later without changing app semantics.

### Phase 2: Local File Provider

Replace hand-seeded file text with a constrained local worktree provider.

Deliverables:

- a host-side file provider that can list paths and read line windows for an
  allowed repository root;
- computed relations such as `RepositoryEntry`, `FileText`, `FileLines`, and
  `FileContentHash`;
- path allow-listing so Mica cannot read arbitrary files by passing strings;
- Mica code that records selected files and spans while reading file content
  from the provider on demand.

Acceptance check:

- the app can browse the current Mica worktree without regenerating a filein
  for every source edit;
- provider reads require bound repository, revision, path, and range arguments;
- attempts to read outside the configured repository root fail.

### Phase 3: Syntax Provider

Add tree-sitter-backed syntax and outline data.

Deliverables:

- syntax highlighting spans for Rust, Mica source, Markdown, and JavaScript
  if practical;
- `SyntaxOutline` for top-level declarations in a file;
- `SyntaxNodeAt` for nearest item at a selected byte offset;
- code pane rendering with line-level keys and syntax span rendering;
- outline panel that links to source spans.

Acceptance check:

- opening `crates/runtime/src/lib.rs` shows a useful outline;
- clicking an outline item scrolls or jumps to the matching span;
- syntax data remains provider-backed and constrained by file/path/span
  arguments.

### Phase 4: Rust Semantic Navigation

Use rust-analyzer for semantic navigation over the Mica workspace.

Deliverables:

- rust-analyzer provider lifecycle for the configured workspace root;
- `DefinitionAt` and `ReferencesOf` computed relations;
- symbol identities minted or resolved from provider results;
- source links for definitions and references;
- a symbol panel with name, kind, definition span, reference count, and provider
  metadata.

Acceptance check:

- clicking a Rust identifier in `crates/runtime/src/lib.rs` can jump to its
  definition when rust-analyzer knows it;
- find-references returns linked source spans for at least one local symbol;
- provider results include enough provenance to debug stale or missing links;
- Mica app code does not call rust-analyzer directly except through the
  provider relation surface.

### Phase 5: Persistent Semantic Index

Add a durable semantic index path for hosted browsing.

Deliverables:

- an index build command or host effect that produces SCIP/LSIF-like data for
  the Mica repository;
- relation facts for index status, version, source revision, provider, and
  build errors;
- computed relations backed by that index for definitions, references, and
  symbol search;
- refresh rules that mark symbols, files, and retrieval artefacts stale when
  the revision or index version changes.

Acceptance check:

- the source viewer can answer basic symbol navigation without a hot
  rust-analyzer session;
- index build failures are visible as Mica facts;
- the UI can show which provider answered a link.

### Phase 6: Retrieval And Agent Workspace

Connect source navigation to retrieval and agent workflows.

Deliverables:

- text units for files, symbols, doc comments, Markdown sections, review notes,
  and design docs;
- embeddings for source subjects through the existing embedding provider path;
- retrieval plans for code questions and symbol neighbourhood queries;
- answer/context/citation artefacts stored as ordinary Mica relations;
- agent findings anchored to files, spans, symbols, commits, or tickets;
- review UI for accepting, correcting, or rejecting generated notes.

Acceptance check:

- asking "where is DOM sync rendered?" returns linked source spans, docs, and
  relevant design notes;
- retrieved context is filtered through source authority before it is recorded;
- generated answers cite source subjects that the UI can open;
- accepted agent findings become durable workspace facts, not prompt history.

### Phase 7: Hosted Product Hardening

Turn the prototype into a real hosted source workspace.

Deliverables:

- authentication and per-repository authority;
- multiple repositories and revisions;
- background indexing jobs with progress and cancellation;
- cache eviction for provider outputs;
- stable URL shapes for files, symbols, revisions, notes, and searches;
- multi-user annotations and review workflows;
- observability for provider latency and failures;
- deployment scripts and documented operating model.

Acceptance check:

- a remote browser can use the source viewer without direct filesystem access;
- repository access is controlled by Mica policy facts;
- long-running provider work does not block ordinary UI sync;
- the app can be restarted without losing accepted notes, index metadata,
  tickets, or review state.

## Missing Builtins, Datatypes, And Functionality

This section lists functionality that is likely needed or worth tightening
before the source viewer can become more than a demo.

### Builtin And Effect Surface

The preferred shape is provider-backed computed relations, not broad builtins
that let Mica source read arbitrary host state. Some small builtins or host
effects are still likely needed.

Likely candidates:

- path helpers such as normalize, join, dirname, basename, extension, and
  relative-to-root;
- content hashing for text or bytes, so facts can track file and line-window
  freshness;
- time or monotonic job markers for index status facts;
- a host effect to start a source index build for a repository, revision, and
  provider;
- a host effect to cancel or restart an index job;
- a host effect or provider callback path for recording completed index
  metadata;
- line/column conversion helpers if this does not stay entirely inside the file
  provider.

Avoid:

- `read_file(path)` over unconstrained host paths;
- returning provider-rendered HTML for source text;
- making rust-analyzer a builtin callable from arbitrary Mica verbs;
- storing provider process handles or live capabilities as durable facts.

### General Provider Registration

Computed relations are currently registered from Rust code. Retrieval already
adds `NearestEmbedding` that way. A source viewer needs several providers, and
hardcoding each one into `mica-runtime` will become messy.

Needed:

- host configuration for provider sets;
- relation metadata declarations that can bind a relation name/arity to a
  provider;
- required-bound-position declarations exposed to Mica authors;
- provider names and versions visible as facts;
- precise error reporting for provider failures;
- tests showing providers cannot be asserted/retracted as base relations.

### Safe Repository Roots

The app needs controlled access to source files. A generic `read_file(path)`
builtin would be too broad.

Needed:

- configured repository roots or repository identities;
- canonical path validation;
- symlink handling policy;
- path normalization utilities;
- authority checks before reading file content or directory listings.

### Path, Span, And Range Values

Strings and maps can represent paths and spans initially, but source navigation
will constantly pass structured locations around.

Needed, or at least worth considering:

- canonical span records with path, start byte, end byte, start line, start
  column, end line, and end column;
- stable conversion between byte offsets and line/column locations;
- compact representation for line windows;
- comparison and ordering helpers for spans.

This does not necessarily require new core value types. It may be better to use
ordinary identities plus functional relations until the shape settles.

### Large Text And DOM Windowing

Rendering large source files through server-owned DOM sync can become expensive
if every file view is a full tree.

Needed:

- line-window queries;
- stable sync keys for line rows and token spans;
- viewport events from the browser for lazy loading;
- bounded DOM diff work for large files;
- tests or fixtures that exercise files larger than the small MUD UI pages.

### Host Effects And Background Jobs

Indexing with rust-analyzer, tree-sitter, embeddings, or SCIP should not run as
one blocking UI task.

Needed:

- a clear host-effect pattern for starting provider jobs;
- job status facts such as queued, running, complete, failed, cancelled, stale;
- a way to correlate provider callbacks with repository, revision, and index
  identities;
- cancellation and restart behaviour;
- task/session boundary rules for refreshing authority before long-running
  operations record facts.

### Provider Output Caching

Source providers will repeat the same queries.

Needed:

- cache keys based on repository, revision, path, provider version, build
  configuration, and query arguments;
- invalidation when the worktree, commit, provider version, or index version
  changes;
- explicit choice of which results become durable facts and which remain cache;
- observability for cache hit/miss and provider latency.

### Symbol Identity Resolution

LSP results do not naturally provide stable durable symbol IDs. rust-analyzer
can identify many things, but the source viewer needs Mica identities that can
survive UI sessions and provider refreshes.

Needed:

- a symbol identity scheme for provider results;
- conflict handling when two providers disagree;
- tombstones or supersession facts when a symbol disappears;
- mapping from provider-local IDs to Mica `Symbol` identities;
- review path for promoted symbols that become durable workspace facts.

### Syntax Highlighting Data

The DOM builtins can render elements, text, raw HTML, and XML. A source viewer
should not trust provider-produced HTML for code rendering.

Needed:

- token/span data from providers;
- escaping handled by Mica DOM helpers;
- CSS class mapping owned by the app;
- rendering helpers for overlapping or nested syntax spans.

### Search And Retrieval Integration

The retrieval vocabulary exists, but code-specific subjects need more shape.

Needed:

- text-unit generation for files, symbols, doc comments, notes, and design
  docs;
- source-specific `CanRetrieveSubject` rules;
- citation rendering that can open a file at a span;
- stale embedding detection when file content changes;
- retrieval plans that combine exact symbol navigation with semantic search.

### URL And Deep-Link Support

The current sync app model can serve a mounted view, but source browsing needs
stable links that can be shared.

Needed:

- route parsing for `/source/...` paths;
- mapping URL path/query state into session facts;
- canonical URLs for repository, revision, file, line, symbol, and note;
- redirect or error behaviour for stale revisions and missing symbols.

### Mica Language And Runtime Ergonomics

Building a substantial UI in Mica will stress the current authoring model.

Likely pressure points:

- repeated boilerplate for DOM construction;
- list manipulation for line/token rendering;
- string and path manipulation;
- lack of module-level namespacing beyond naming convention;
- lack of typed records for common shapes such as spans and search hits;
- test harnesses for app-level Mica source.

The source viewer should be treated as a forcing function for these ergonomics,
but changes should stay grounded in concrete app pain rather than speculative
language design.

## Risks

### Overloading The Relation Store

The largest risk is storing every AST node, token, and reference as durable
facts. That would stress storage, queries, sync rendering, and authority checks
for data that is better served from sidecars.

Mitigation: keep heavy index surfaces provider-backed and require bound
arguments for scans.

### Rust-Specific Architecture

Starting with rust-analyzer could accidentally bake Rust assumptions into the
source viewer.

Mitigation: name the relations around source concepts, not Rust concepts, and
put provider-specific fields in provenance facts.

### LSP Fragility

LSP results depend on workspace configuration, generated files, feature flags,
toolchain state, and provider process health.

Mitigation: record provider provenance and add SCIP/LSIF-like durable indexes
before treating hosted semantic navigation as reliable.

### UI Diff Cost

Source files are much larger than the current MUD panels.

Mitigation: design the code pane around line windows and stable row keys from
the start, even if Phase 1 renders only small files.

### Blurry Agent Boundaries

It will be tempting to hide retrieval, model calls, and code analysis inside an
agent pipeline.

Mitigation: record retrieval plans, citations, provider results, and accepted
findings as ordinary facts so humans can inspect and correct them.

## Near-Term Implementation Slice

The next concrete slice should be Phase 1 plus a very small part of Phase 2:

1. Add `apps/source/` with a static fixture for the Mica repository.
2. Serve `/source` through the existing HTTP and sync stack.
3. Render a two-pane UI: file list and code pane.
4. Store selected path and revision in endpoint/session facts.
5. Add `scripts/source.sh` to launch it.
6. Keep provider-backed file reads out of the first slice unless the static
   fixture proves too awkward.

That slice proves the important product loop: Mica can host a source browser UI
for its own repository. Semantic navigation can then be added behind computed
relations without changing the app model.
