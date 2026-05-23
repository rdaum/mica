# Mica GraphRAG Design

This document sketches how Mica could support GraphRAG-style retrieval and
answering while staying centred on Mica's relation-first object model.

The intended outcome is not "Mica as a vector database" or "Mica as a clone of
Microsoft GraphRAG." The intended outcome is a Mica-native memory system where
documents, extracted entities, claims, relationships, communities, summaries,
provenance, authority, and behaviours are live relation facts. Vector search is
one specialized access path into that memory, not the source of truth.

## References

Microsoft GraphRAG is the main external reference for this design:

- <https://github.com/microsoft/graphrag/blob/main/RAI_TRANSPARENCY.md#what-can-graphrag-do>
- <https://microsoft.github.io/graphrag/index/overview/>
- <https://microsoft.github.io/graphrag/index/outputs/>
- <https://microsoft.github.io/graphrag/query/overview/>
- <https://microsoft.github.io/graphrag/query/drift_search/>

GraphRAG's standard indexing pipeline extracts entities, relationships, claims,
communities, community reports, and embeddings from unstructured text. Its
default outputs are stored as tables, with embeddings written to a configured
vector store.

Mica should absorb the useful shape of that system, but change the execution
model: extracted knowledge becomes live relational world state; retrieval is
composed through rules, authority, provenance, and behaviours; and vector
indexes provide constrained nearest-neighbour access paths.

## Executive Summary

GraphRAG is useful because it does not rely only on top-k vector chunks. It
builds a structured memory from documents: entities, relationships, claims,
communities, summaries, embeddings, and query modes that combine local detail
with global themes.

Mica can support the same class of work by making those structures ordinary
live relations:

```text
Document -> TextUnit -> Entity / Relationship / Claim -> Community -> Report
```

Embeddings attach to those identities:

```text
EmbeddingOf(embedding, subject)
```

Vector indexes then provide a fast way to propose candidates:

```text
NearestEmbedding(index, query_embedding, limit, ?subject, ?score, ?version)
```

Mica rules and verbs decide what those candidates mean. They join vector
results to graph facts, provenance, authority, review status, task state, and
answer-generation workflows.

The result is a GraphRAG-like capability with a different centre of gravity:

```text
GraphRAG-style pipeline:
  build static graph/vector outputs, then query them

Mica-style memory:
  keep graph/vector/search artefacts as live programmable state
```

The first implementation should not start with an ANN index in the relation
kernel. It should start with the relation vocabulary, retrieval records, and an
exact in-process computed relation for nearest-neighbour search. The current
system reflection relations already prove that Mica can expose read-only,
computed rows through the ordinary relation query path. ANN can sit behind the
same computed-relation shape once the semantics are clear.

## GraphRAG To Mica Mapping

| GraphRAG concept        | Mica representation                                                        |
|-------------------------|----------------------------------------------------------------------------|
| source documents        | `Document`, `DocumentRevision`, `TextUnit`                                 |
| text chunks             | `TextUnit` identities with span, hash, and source revision facts           |
| extracted entities      | `Entity`, `EntityName`, `Mention`, `EntityResolution`                      |
| extracted relationships | `Relationship`, `RelationshipSource`, `RelationshipTarget`, evidence facts |
| covariates or claims    | `Claim` facts with evidence, confidence, and review state                  |
| communities             | `Community`, `CommunityMember`, `CommunityRelationship`                    |
| community reports       | `CommunityReport`, `ReportFinding`, `ReportEvidence`                       |
| embeddings              | `Embedding`, `EmbeddingOf`, `EmbeddingVector`, `EmbeddingModel`            |
| vector store            | computed search relations backed by in-process or external indexes         |
| local search            | Mica verb/rules expanding from candidate chunks/entities                   |
| global search           | Mica verb/rules over community reports and report evidence                 |
| DRIFT search            | Mica retrieval plan combining report-level and local graph expansion       |
| query trace             | `RetrievalPlan`, `RetrievedContext`, `Answer`, `AnswerCitation`            |
| human review            | `Review`, `ApprovedFact`, `RejectedFact`, `Correction`                     |

## Problem Statement

Plain vector RAG is good at finding semantically similar chunks. It is weak at
representing durable structure:

- what an item is about;
- which entities, claims, and relationships it mentions;
- how facts were extracted and by which run or model;
- whether a fact was reviewed, corrected, rejected, superseded, or trusted;
- who is allowed to use a document, chunk, claim, report, or answer;
- how retrieval results should participate in a live workflow;
- how multiple users or agents can inspect and improve the memory itself.

GraphRAG addresses part of this by building a graph over the corpus and by
generating community summaries. It is still usually used as an indexing and
query pipeline around static outputs.

Mica can take a different role. It can be the live programmable memory layer
underneath RAG:

```text
documents and tool results
  -> ingestion and extraction tasks
  -> durable relation facts
  -> derived relations and authority
  -> vector and graph access paths
  -> retrieval verbs
  -> grounded model calls and answer artefacts
```

The core design question is how to expose vector and graph retrieval in Mica
without reducing Mica to a vector store with extra metadata.

## Goals

Mica should be able to:

1. Ingest documents, tool results, notes, transcripts, and generated artefacts
   into durable identities and relation facts.
2. Chunk source material into text units with provenance and revision identity.
3. Extract entities, claims, relationships, and references from text units.
4. Represent graph structure as ordinary Mica relations.
5. Represent GraphRAG-style communities and community reports as ordinary Mica
   identities and relations.
6. Attach embeddings to documents, chunks, entities, claims, reports, and other
   subjects.
7. Maintain one or more vector indexes over those embeddings.
8. Expose nearest-neighbour search as relation-shaped queries.
9. Compose vector candidates with relation queries, rules, authority, and
   provenance.
10. Record retrieval plans, retrieved context, citations, model calls, answers,
    and review state as ordinary world facts.
11. Let humans and agents inspect, correct, reject, annotate, and rerun parts of
    the memory.

## Non-Goals

This design does not require:

- making approximate nearest-neighbour indexes part of the existing tuple index
  machinery;
- making every vector search result into a stored base tuple;
- making Mica depend on a particular vector database service;
- hiding knowledge extraction inside opaque pipeline output files;
- treating model-generated facts as trusted merely because they are present;
- defining a full query optimizer before useful retrieval can exist.

It is acceptable for early versions to use simple in-process indexes, exact
linear scans, or sidecar files while the relation shape settles.

## Core Principle

Vectors find candidates. Relations explain, constrain, authorize, connect, and
update them.

That principle should drive the API boundary:

```text
Base relations:
  durable facts Mica owns.

Derived relations:
  rule-computed facts Mica can explain.

Computed search relations:
  read-only relation-shaped access to specialized indexes or services.
```

A computed search relation is not a base relation that can be freely
enumerated. It is a constrained access path. For vector search, it requires a
bound query embedding and a limit. It produces candidates and scores that Mica
code can join with ordinary facts.

Mica now has a concrete precedent for this shape: the queryable system
reflection relations. They are declared like ordinary relations, but their rows
are computed at scan time and they are read-only. Search should grow from that
mechanism, generalized beyond the current hardcoded reflection catalogue.

## Conceptual Architecture

```text
                +-------------------+
                | source artefacts  |
                | docs, notes, logs |
                +---------+---------+
                          |
                          v
                +-------------------+
                | ingestion tasks   |
                | chunk, extract,   |
                | embed, summarize  |
                +---------+---------+
                          |
                          v
  +-----------------------+-----------------------+
  |             Mica relation store               |
  | documents, chunks, entities, claims, graph,   |
  | communities, reports, provenance, authority   |
  +-----------+-----------------------+-----------+
              |                       |
              v                       v
  +-----------------------+   +-------------------+
  | derived relations     |   | search providers  |
  | relevance, trust,     |   | vector, keyword,  |
  | visibility, workflow  |   | graph algorithms  |
  +-----------+-----------+   +---------+---------+
              |                         |
              +-----------+-------------+
                          v
                +-------------------+
                | retrieval verbs   |
                | local, global,    |
                | hybrid, drift     |
                +---------+---------+
                          |
                          v
                +-------------------+
                | answer artefacts  |
                | citations, trace, |
                | review state      |
                +-------------------+
```

## Detailed Example Scenarios

The abstract shape becomes more useful when expressed as concrete Mica
applications. Below are the kinds of systems this facility would enable.

### 1. Repository Or Design Workspace Memory

One obvious use is a long-lived technical workspace shared by humans and
software agents.

Source material:

- design notes;
- issue threads;
- architecture decisions;
- code comments and symbols;
- benchmark results;
- prior agent runs;
- tool outputs such as compiler errors or test failures.

Stored structure:

- `Document` and `TextUnit` facts for notes, diffs, issue comments, and logs;
- `Entity` identities for modules, crates, functions, types, subsystems, and
  projects;
- `Claim` facts such as "crate X owns boundary Y" or "feature Z is disabled in
  browser builds";
- `Relationship` facts such as "depends on", "supersedes", "conflicts with", or
  "fixes";
- `Run`, `DerivedBy`, and `Confidence` facts for extraction and summarization
  passes;
- `Answer`, `RetrievedContext`, and `AnswerCitation` facts for responses given
  to users or agents.

Useful retrieval behaviours:

- answer "where in the codebase is this policy enforced?";
- find all prior discussions related to a subsystem before changing it;
- retrieve benchmark evidence when someone asks whether a performance claim is
  real;
- surface contradictory design notes and mark them as needing review;
- expand from a symbol or issue to adjacent design claims and source passages.

Agentic tool integration:

- a code agent can query `NearestTextUnit` over design notes, then join that to
  extracted `Entity` and `Relationship` facts before choosing which files to
  inspect;
- a review agent can write `NeedsReview` or `Correction` facts when it finds a
  stale summary or a contradicted claim;
- a benchmark tool can ingest fresh run logs, attach them to existing entities,
  and mark older summaries stale;
- a documentation agent can answer with citations tied to Mica identities
  instead of raw blob snippets.

This is RAG as shared technical memory, not just question-answering over
documents.

### 2. Agent Workspace And Tool Trace Memory

Another use is an agent environment where tools, plans, and observations are
part of the same memory as the domain model.

Source material:

- prompts and instructions;
- tool calls and outputs;
- external fetched documents;
- user corrections;
- intermediate plans;
- generated artefacts.

Stored structure:

- `Run`, `RunInput`, `RunOutput`, `ModelCall`, and `RetrievedContext` facts;
- `ToolInvocation`, `ToolResult`, and `Observation` identities;
- `Claim` or `Hypothesis` facts extracted from tool output;
- `Correction` and `RejectedFact` rows when a user says an answer was wrong;
- `TaskState`, `OpenQuestion`, or similar workflow facts defined by the app.

Useful retrieval behaviours:

- retrieve prior observations relevant to the current step instead of replaying
  every tool call;
- answer "why did the agent choose this action?" from stored retrieval traces;
- reuse prior grounding context when a task resumes later;
- separate trusted tool observations from speculative model-generated claims;
- find earlier work on a similar incident, bug, or user request.

Agentic tool integration:

- embedding tools generate query vectors and subject embeddings, then assert
  `Embedding...` facts;
- extraction tools turn logs, documents, or tool output into `Entity`,
  `Relationship`, and `Claim` rows;
- planning agents write `RetrievalPlan` and `Answer` artefacts back into Mica;
- review agents compare new answers to prior `AnswerCitation` and `Correction`
  facts before replying.

This makes the agent's memory inspectable, correctable, and durable across
sessions.

### 3. Operations, Incident, Or Procedure Systems

GraphRAG-style retrieval is also useful for operational models where procedures,
events, assets, and current state interact.

Source material:

- runbooks;
- incident timelines;
- service catalogues;
- alerts;
- chat transcripts;
- postmortems;
- deployment records.

Stored structure:

- `Service`, `Dependency`, `Owner`, and `Runbook` entities;
- claims extracted from runbooks such as required checks or rollback steps;
- incident event streams and derived summaries;
- authority facts describing which actor may see which service or incident;
- review state over extracted procedure claims.

Useful retrieval behaviours:

- answer "what runbook steps apply to this service and symptom?";
- start from an alert, retrieve nearby incidents, then expand through shared
  dependencies and owners;
- assemble a response packet containing current alerts, prior incidents,
  relevant runbook steps, and cited evidence;
- warn when retrieved procedure text is stale relative to newer postmortems.

Agentic tool integration:

- monitoring tools ingest alerts as source artefacts;
- summarization tools build or refresh incident reports;
- procedural agents propose next actions but cite the exact runbook and event
  evidence they relied on;
- authority filtering prevents an agent from retrieving material from services
  or incidents it should not see.

Here the value is not only semantic search. It is the join between semantic
search, explicit topology, temporal state, provenance, and access control.

### 4. MUD And Simulated World Memory

The current `apps/mud/` example suggests a particularly Mica-native use: world
memory that is part of the live simulation rather than an external lore search
service.

Source material:

- room descriptions and object prose;
- scripted lore documents;
- prior narrative events;
- player journals or notes;
- quest text;
- builder design notes;
- conversation transcripts with NPCs or players.

Stored structure:

- `Document` and `TextUnit` facts for lore books, room backstory, quest notes,
  and transcripts;
- `Entity` identities aligned with world identities such as rooms, actors,
  factions, artefacts, and locations;
- `Relationship` facts such as allegiance, history, ownership, or geographic
  adjacency;
- `Claim` facts such as unresolved rumours, canon facts, or player-discovered
  clues;
- provenance and trust facts distinguishing builder-authored canon from
  generated summaries or player notes.

Useful retrieval behaviours:

- `look` or `examine` can pull in relevant lore context for a room or object;
- NPC dialogue can retrieve nearby facts about a place, faction, or player
  history instead of relying only on hand-authored branching text;
- a builder tool can answer "what existing rooms mention this faction?" or
  "where else has this artefact appeared?";
- quest systems can retrieve supporting clues and cited prior events before
  generating a hint;
- narrative review tools can find contradictions between new world text and
  existing canon claims.

Agentic tool integration:

- a "scribe" agent can summarize recent world events into persistent
  `CommunityReport` or journal-style records;
- a builder assistant can ingest new lore notes, extract entities and
  relationships, and flag conflicts with existing canon;
- an NPC dialogue tool can retrieve only facts the character is allowed to
  know, based on world-local authority rules;
- an event-analysis tool can cluster narrative events and propose new quest or
  lore connections for a human builder to approve.

This is especially interesting in Mica because world state, retrieval state,
and authority policy already live in the same relation system.

### 5. Browser Apps Similar To The Current MUD UI

The browser examples in `apps/mud/` and `apps/chat/` point to another class of
use: retrieval-backed server-owned interfaces.

Possible UI behaviours:

- an inspector panel can show "related lore", "similar prior incidents", or
  "recently cited facts" beside the currently selected entity;
- a narrative pane can request "older related events" rather than only older
  chronological events;
- a composer can suggest actions, references, or citations based on retrieved
  world context;
- a programmer-facing inspect view can jump from an entity to documents,
  claims, or extraction runs that mention it.

For the MUD specifically:

- the `Examine` panel could include cited background facts for the selected
  object or room;
- the command-strip could surface context-aware suggestions based on retrieved
  nearby lore or prior player interactions;
- the Mica inspect panel could show retrieval provenance for generated
  descriptions, rumours, or hints;
- scrollback could be summarized into higher-level narrative reports that then
  become queryable world artefacts.

The important point is that the browser does not need a separate retrieval
model. Mica can render retrieval-backed views through the same sync mechanism it
already uses for ordinary world state.

### 6. Human Review And Correction Loops

A major use of this facility is not answering end-user questions at all. It is
maintaining the memory itself.

Examples:

- a reviewer inspects extracted claims and marks them `ApprovedFact` or
  `RejectedFact`;
- a builder corrects an entity resolution and automatically marks dependent
  reports or embeddings stale;
- an agent notices that two summaries disagree and opens a review task instead
  of answering confidently;
- a UI panel shows all low-confidence facts affecting the current answer.

This is where Mica has more to offer than a conventional vector store. The
memory can track what it knows, why it thinks that, who may use it, what became
stale, and what needs human judgement.

## Base Knowledge Relations

The standard library should define a knowledge-memory vocabulary. Names below
are illustrative; the exact naming should follow whatever relation naming
conventions settle elsewhere in Mica.

### Documents And Text Units

Documents are durable artefacts. Text units are chunks or spans derived from a
document revision.

```mica
Document(document)
DocumentTitle(document, title)
DocumentSource(document, source)
DocumentRevision(revision)
RevisionOf(revision, document)
RevisionContentHash(revision, hash)
RevisionImportedAt(revision, time)

TextUnit(unit)
TextUnitOf(unit, revision)
TextUnitOrdinal(unit, ordinal)
TextUnitSpan(unit, start_offset, end_offset)
TextUnitText(unit, text)
TextUnitTokenCount(unit, count)
TextUnitHash(unit, hash)
```

For large corpora, `TextUnitText` may eventually store a content reference
rather than inline text. Mica should still represent the identity and provenance
of the unit as relation facts.

### Entities

Entities are durable identity values. Mentions connect text units to entities.

```mica
Entity(entity)
EntityName(entity, name)
EntityKind(entity, kind)
EntityDescription(entity, description)
EntityAlias(entity, alias)

Mention(mention)
MentionEntity(mention, entity)
MentionTextUnit(mention, unit)
MentionSpan(mention, start_offset, end_offset)
MentionSurface(mention, text)
MentionConfidence(mention, score)
```

Entity resolution should be explicit. A model may propose that two mentions
refer to the same entity, but Mica should record the proposal and its review
state rather than silently merging identities.

```mica
EntityResolution(resolution)
ResolutionMention(resolution, mention)
ResolutionEntity(resolution, entity)
ResolutionConfidence(resolution, score)
ResolutionStatus(resolution, status)
```

### Relationships

Relationships are facts about entities with evidence in source text.

```mica
Relationship(relationship)
RelationshipSource(relationship, source_entity)
RelationshipTarget(relationship, target_entity)
RelationshipKind(relationship, kind)
RelationshipDescription(relationship, description)
RelationshipWeight(relationship, weight)
RelationshipEvidence(relationship, unit)
RelationshipConfidence(relationship, score)
```

Many graph algorithms want a simpler edge view. That should be derived from the
relationship facts, not a replacement for them:

```mica
KnowledgeEdge(source, kind, target) :-
  Relationship(rel),
  RelationshipSource(rel, source),
  RelationshipTarget(rel, target),
  RelationshipKind(rel, kind),
  TrustedRelationship(rel)
```

### Claims

Claims are model- or user-extracted propositions. They need provenance, status,
and review state.

```mica
Claim(claim)
ClaimSubject(claim, entity)
ClaimObject(claim, entity)
ClaimKind(claim, kind)
ClaimText(claim, text)
ClaimEvidence(claim, unit)
ClaimStatus(claim, status)
ClaimConfidence(claim, score)
```

GraphRAG's covariates are often claim-like. Mica should generalize them as
claims rather than bake in one domain-specific claim schema.

Review and correction are first-class:

```mica
Review(review)
ReviewedFact(review, fact)
ReviewedBy(review, actor)
ReviewDecision(review, decision)
ReviewComment(review, comment)
ReviewTime(review, time)

RejectedFact(fact)
Supersedes(new_fact, old_fact)
```

### Communities And Reports

Communities group entities and relationships. Reports summarize communities.

```mica
Community(community)
CommunityLevel(community, level)
CommunityParent(community, parent)
CommunityTitle(community, title)
CommunityMember(community, entity)
CommunityRelationship(community, relationship)
CommunityTextUnit(community, unit)
CommunityPeriod(community, period)

CommunityReport(report)
ReportCommunity(report, community)
ReportTitle(report, title)
ReportSummary(report, summary)
ReportContent(report, content)
ReportRank(report, rank)
ReportFinding(report, finding)
ReportEvidence(report, unit)
```

Community detection may be implemented outside the relation kernel at first,
but its outputs should become ordinary Mica facts. That lets users inspect and
repair communities, rerun reports, or define domain-specific community rules.

### Embeddings

Embeddings attach vector representations to subjects.

```mica
Embedding(embedding)
EmbeddingOf(embedding, subject)
EmbeddingModel(embedding, model)
EmbeddingDimension(embedding, dimension)
EmbeddingMetric(embedding, metric)
EmbeddingVector(embedding, payload)
EmbeddingContentHash(embedding, source_hash)
EmbeddingCreatedAt(embedding, time)
```

`payload` can initially be bytes, a list of floats, or a sidecar reference. The
semantic relation should not require a first-class `Vector` value on day one.
The system can add a vector value kind later if in-language vector operations,
codec guarantees, or planner integration justify it.

## Provenance Relations

Generated knowledge needs explicit lineage.

```mica
Run(run)
RunKind(run, kind)
RunStartedAt(run, time)
RunCompletedAt(run, time)
RunInput(run, input)
RunOutput(run, output)
RunModel(run, model)
RunPrompt(run, prompt)
RunParameter(run, name, value)

DerivedFact(fact)
DerivedBy(fact, run)
DerivedFrom(fact, source)
Confidence(fact, score)
NeedsReview(fact)
TrustedFact(fact)
```

This is the surface that makes the memory inspectable. If an answer used a
claim, Mica should be able to explain which text unit, extraction run, model,
prompt, and review decision produced it.

## Authority Relations

RAG systems often fail by retrieving text that should not be available to the
actor or by letting untrusted text act as instruction. Mica should treat
retrieval as an authorized read.

Durable policy remains relation facts:

```mica
CanRead(actor, relation)
CanUseSource(actor, source)
CanUseDocument(actor, document)
CanUseTextUnit(actor, unit)
CanUseClaim(actor, claim)
CanUseReport(actor, report)
TrustedForExtraction(source)
TrustedForAnswering(source)
QuarantinedSource(source)
PromptInjectionFlag(unit)
```

Effective authority should be compiled into the task/session authority context,
not recomputed from policy relations on every tuple or vector candidate.

Retrieval rules should include authority:

```mica
UsableTextUnit(actor, unit) :-
  TextUnit(unit),
  CanUseTextUnit(actor, unit),
  not PromptInjectionFlag(unit)

UsableClaim(actor, claim) :-
  Claim(claim),
  CanUseClaim(actor, claim),
  TrustedFact(claim)
```

The vector index may return unauthorized candidates. Mica must filter them
before exposing text or answer context.

## Computed Search Relations

A computed search relation presents a specialized access path through the same
relation-shaped query model.

Example vector search surface:

```mica
NearestEmbedding(index, query_embedding, limit, ?subject, ?score, ?index_version)
NearestTextUnit(index, query_embedding, limit, ?unit, ?score, ?index_version)
NearestEntity(index, query_embedding, limit, ?entity, ?score, ?index_version)
NearestReport(index, query_embedding, limit, ?report, ?score, ?index_version)
```

These relations are access-pattern constrained:

- `index` must be bound;
- `query_embedding` must be bound;
- `limit` must be bound;
- output positions may be unbound;
- arbitrary enumeration is not supported.

The relation is computed because Mica does not store all possible nearest
neighbour answers as base facts. It computes candidate rows from an index or
other specialized search structure.

### Existing Precedent

Mica already has one form of computed read-only relation: the system reflection
relations such as `RelationName`, `SubjectFact`, and `MentionedFact`. They are
declared in the runtime catalogue, scanned through the normal relation read
path, materialized on demand, and rejected on write.

That means the first search implementation does not need a completely new
concept. It needs a generalization of the current computed-relation path so
that read-only non-stored relations are not limited to the hardcoded system
catalogue.

### Generalized Computed-Relation Boundary

The next abstraction should be a registry-backed computed relation mechanism,
not a second persistence-style provider interface. The exact trait can evolve,
but the semantics should be roughly:

```rust
trait ComputedRelation {
    fn relation(&self) -> RelationId;
    fn required_bound_positions(&self) -> &[u16];
    fn estimate(
        &self,
        snapshot: &Snapshot,
        bindings: &[Option<Value>],
    ) -> Result<usize, KernelError>;
    fn scan(
        &self,
        snapshot: &Snapshot,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError>;
}
```

The important semantic boundary is:

- computed results are relation-shaped tuples;
- computed relations may require bound inputs;
- computed relations are read-only;
- computed results are not durable truth;
- computed relations must report enough score/rank/version metadata for Mica to
  validate and explain results;
- computed relations must not bypass authority.

### In-Process First

The first vector-backed computed relation should be in-process unless there is a
strong reason not to. It can own:

- an HNSW or exact vector index;
- sidecar index files;
- a mapping from embedding identity to subject identity;
- an index version or source snapshot version;
- rebuild and refresh logic.

External services such as pgvector, Qdrant, LanceDB, or a search engine can be
supported later through the same computed-relation boundary. That should be an
implementation detail, not the Mica-facing model.

### Freshness And Snapshot Rules

Approximate indexes are usually slightly stale or rebuilt asynchronously. Mica
should make that visible.

Recommended behaviour:

1. The computed relation returns `index_version` with each candidate.
2. Mica validates that the candidate subject and embedding still exist in the
   current transaction snapshot.
3. Mica filters candidates through authority and trust rules.
4. If the index is too stale for the retrieval policy, the retrieval verb can
   fail, warn, or fall back to exact scan.

Useful relations:

```mica
VectorIndex(index)
VectorIndexKind(index, kind)
VectorIndexMetric(index, metric)
VectorIndexBuiltFrom(index, snapshot_version)
VectorIndexContains(index, embedding)
VectorIndexStale(index)
```

## Retrieval Modes

Mica should support the same broad classes of retrieval as GraphRAG, expressed
as Mica verbs and rules.

### Local Search

Local search starts from entities or text units close to the query, then expands
through nearby graph facts.

```text
query
  -> query embedding
  -> nearest text units or entities
  -> mentioned entities
  -> adjacent relationships and claims
  -> source text and citations
  -> answer
```

Example rules:

```mica
CandidateTextUnit(actor, query, unit, score) :-
  QueryEmbedding(query, embedding),
  NearestTextUnit(#text_index, embedding, 50, unit, score, ?index_version),
  UsableTextUnit(actor, unit)

CandidateEntity(actor, query, entity) :-
  CandidateTextUnit(actor, query, unit, ?score),
  MentionEntity(mention, entity),
  MentionTextUnit(mention, unit)

CandidateRelationship(actor, query, relationship) :-
  CandidateEntity(actor, query, entity),
  RelationshipSource(relationship, entity),
  UsableRelationship(actor, relationship)

CandidateRelationship(actor, query, relationship) :-
  CandidateEntity(actor, query, entity),
  RelationshipTarget(relationship, entity),
  UsableRelationship(actor, relationship)
```

### Global Search

Global search starts from community reports and high-level summaries.

```text
query
  -> query embedding
  -> nearest community reports
  -> report findings and cited sources
  -> relevant communities
  -> supporting text units
  -> answer
```

Example:

```mica
CandidateReport(actor, query, report, score) :-
  QueryEmbedding(query, embedding),
  NearestReport(#report_index, embedding, 20, report, score, ?index_version),
  CanUseReport(actor, report)

CandidateCommunity(actor, query, community) :-
  CandidateReport(actor, query, report, ?score),
  ReportCommunity(report, community)
```

### Hybrid Search

Hybrid search combines vector candidates, keyword matches, graph expansion, and
community reports.

Search relations can include keyword and graph providers:

```mica
KeywordMatch(index, query_text, limit, ?subject, ?score, ?index_version)
GraphNeighbour(seed, relation_kind, limit, ?subject, ?distance)
```

Mica rules can union and rerank candidates:

```mica
CandidateSubject(actor, query, subject, score, :vector) :-
  QueryEmbedding(query, embedding),
  NearestEmbedding(#main_index, embedding, 100, subject, score, ?version),
  CanUseSubject(actor, subject)

CandidateSubject(actor, query, subject, score, :keyword) :-
  QueryText(query, text),
  KeywordMatch(#keyword_index, text, 100, subject, score, ?version),
  CanUseSubject(actor, subject)
```

The first version can use simple score normalization in runtime code. Later,
Mica can support explicit reranking relations or model-based reranking tasks.

### DRIFT-Style Search

GraphRAG has a DRIFT search mode that mixes local and global evidence. Mica can
model this as a retrieval plan that recursively expands from both:

- query-near text units;
- query-near reports;
- entities shared between them;
- contradictions, missing context, and high-rank findings.

This should be a retrieval verb, not a hardcoded kernel path.

## Retrieval Artefacts

Queries and answers should themselves become memory.

```mica
Question(question)
QuestionText(question, text)
AskedBy(question, actor)
AskedAt(question, time)

RetrievalPlan(plan)
PlanForQuestion(plan, question)
PlanKind(plan, kind)
PlanParameter(plan, name, value)

RetrievedContext(context)
ContextForPlan(context, plan)
ContextSubject(context, subject)
ContextScore(context, score)
ContextReason(context, reason)
ContextSource(context, source)

Answer(answer)
AnswerForQuestion(answer, question)
AnswerText(answer, text)
AnswerGeneratedBy(answer, run)
AnswerUsesContext(answer, context)
AnswerCitation(answer, source)
AnswerStatus(answer, status)
```

This gives Mica a durable audit trail:

- what was asked;
- what retrieval plan was used;
- which index versions were consulted;
- which facts and text units were considered;
- which context was sent to the model;
- which answer was produced;
- who reviewed or corrected it.

## Ingestion And Maintenance Tasks

The pipeline should be expressed as tasks/verbs rather than a one-shot external
importer.

Potential verbs:

```mica
:import_document(actor: actor, source: source)
:chunk_document(actor: actor, revision: revision)
:extract_entities(actor: actor, unit: unit)
:resolve_entities(actor: actor, document: document)
:extract_relationships(actor: actor, unit: unit)
:extract_claims(actor: actor, unit: unit)
:detect_communities(actor: actor, graph: graph)
:generate_community_report(actor: actor, community: community)
:embed_subject(actor: actor, subject: subject, model: model)
:publish_vector_index(actor: actor, index: index)
:answer_question(actor: actor, question: question, mode: mode)
```

The implementations can call host effects or runtime builtins. The state
changes should be ordinary relation assertions and retractions.

Each task should record:

- actor;
- input identities;
- source snapshot or revision;
- model and prompt;
- produced facts;
- warnings and failures;
- review requirements.

## Query Semantics

Computed search relations need clearer semantics than ordinary base relations.

### Binding Requirements

Each computed relation declares required bound positions. A query that violates
the access pattern should fail at compile time when possible, or at runtime with
a precise error.

For `NearestEmbedding(index, query_embedding, limit, ?subject, ?score,
?index_version)`, positions 0, 1, and 2 are required.

### Cardinality And Ordering

Search providers return ordered candidates. Ordinary relations are sets; search
results have rank. The output should include explicit score and possibly rank:

```mica
NearestEmbedding(index, query, limit, ?subject, ?score, ?rank, ?version)
```

Whether rank is a separate output should be decided before implementation. Rank
is useful because two candidates can share a score, and approximate indexes
often return order as part of their observable behaviour.

### Approximation

Approximate nearest-neighbour results are not pure mathematical facts. The
relation should make approximation policy inspectable:

```mica
VectorIndexApproximate(index, true)
VectorIndexSearchParameter(index, :ef_search, 100)
VectorIndexRecallTarget(index, 0.95)
```

Retrieval verbs should not silently treat approximate scores as proof. They are
candidate-generation evidence.

### Transaction Boundaries

Retrieval should run against a transaction snapshot. Computed search rows must
be validated against that snapshot before use.

If a task writes new embeddings and then queries an index, there are three
possible policies:

1. Query only the committed index.
2. Query the committed index plus transaction-local exact candidates.
3. Require index refresh before querying.

The first implementation can choose policy 1 for simplicity and make the
limitation explicit. Policy 2 is likely useful later.

## Answer Generation Boundary

Mica should not treat an LLM call as magical. It is an effect with inputs and
outputs.

```mica
Model(model)
PromptTemplate(prompt)
ModelCall(call)
ModelCallModel(call, model)
ModelCallPrompt(call, prompt)
ModelCallInput(call, input)
ModelCallOutput(call, output)
ModelCallStatus(call, status)
```

Answer generation should receive structured context:

- text excerpts;
- entity summaries;
- relationship descriptions;
- claims and statuses;
- community report findings;
- citations;
- warnings about unreviewed or low-confidence facts.

Generated answers should cite Mica identities, not only raw strings. Rendering
can turn those identities into human-readable citations.

## Review And Correction Workflow

Human review should be a normal workflow, not a separate annotation system.

Examples:

```mica
NeedsReview(fact)
ApprovedFact(fact)
RejectedFact(fact)
Correction(correction)
CorrectionOf(correction, fact)
CorrectionText(correction, text)
CorrectionBy(correction, actor)
CorrectionAppliedAt(correction, time)
```

When a correction changes an entity resolution, claim, or relationship, Mica can
derive affected reports, embeddings, and answers:

```mica
StaleReport(report) :-
  ReportEvidence(report, unit),
  CorrectedTextUnit(unit)

StaleEmbedding(embedding) :-
  EmbeddingOf(embedding, subject),
  SubjectContentChanged(subject)
```

This is one of the main advantages over static pipeline output. The memory can
know what became stale.

## Implementation Phases

### Phase 1: Relation Vocabulary And Import Shape

Define the core knowledge relations for documents, text units, entities,
relationships, claims, embeddings, provenance, and answer artefacts.

Deliverables:

- Mica filein example defining the relations.
- Small importer or manual filein for a tiny corpus.
- Tests that query entities, mentions, claims, and citations.

No ANN index is required in this phase.

### Phase 2: Generalize Computed Relations For Search

Generalize the current system-reflection mechanism into a registry-backed
computed relation path, then implement exact vector scan on top of it.

Deliverables:

- generalized computed-relation registry or equivalent dispatch hook;
- `NearestEmbedding` over embeddings stored as bytes or float lists;
- Required-bound-position enforcement.
- Snapshot validation of returned subjects.
- Authority filtering in retrieval rules.

This proves the Mica-facing semantics before committing to an ANN library or a
more elaborate search backend.

### Phase 3: In-Process ANN Index

Add an in-process approximate vector index behind the same computed relation.

Deliverables:

- index build from committed embedding facts;
- index version relations;
- refresh/rebuild command;
- recall and latency benchmark on a representative corpus;
- fallback or validation path for stale indexes.

The ANN implementation should live outside the hot tuple index code unless
measurements later justify deeper integration.

### Phase 4: GraphRAG-Style Ingestion Tasks

Add task verbs for chunking, extraction, entity resolution, relationship
extraction, claim extraction, community detection, report generation, and
embedding.

Deliverables:

- host effect boundary for model calls;
- provenance records for every generated fact;
- review status relations;
- rerun/staleness behaviour for changed documents.

### Phase 5: Retrieval Verbs And Answer Artefacts

Implement local, global, and hybrid retrieval verbs.

Deliverables:

- retrieval plan records;
- retrieved context records;
- answer records with citations;
- tests for authority filtering and provenance;
- examples that answer questions over a small corpus.

### Phase 6: External Provider Adapters

Only after the Mica-facing model is stable, add optional adapters for external
indexes or databases.

Candidates:

- pgvector;
- Qdrant;
- LanceDB;
- Tantivy or another keyword engine;
- domain-specific graph algorithms.

These adapters should preserve the same computed-relation semantics.

## Risks

### Treating Computed Search Relations Like Normal Relations

If the language lets authors write impossible queries such as enumerating all
nearest-neighbour tuples, behaviour will be confusing. The compiler/runtime
needs explicit access-pattern errors.

### Letting ANN Freshness Leak Into Semantics

Approximate indexes are operational structures. They should propose candidates,
not establish truth. Mica should validate candidate existence, source revision,
authority, and trust through ordinary relations.

### Over-Centralizing Embeddings

If the first implementation makes embeddings the main memory model, Mica loses
the distinction that makes it valuable. Entities, claims, relationships,
communities, policies, and behaviours need to remain first-class.

### Under-Specifying Authority

RAG retrieval is a read path. It must honour source permissions before context
is exposed to a model or user. Filtering after answer generation is too late.

### Model-Generated Fact Drift

Extraction models change, prompts change, and source documents change. Mica
needs provenance and staleness relations from the beginning.

## Open Questions

1. Should embeddings be represented as `Bytes`, `List<Float>`, a sidecar
   reference, or a first-class `Vector` value?
2. Should computed-relation access patterns be declared in relation metadata,
   computed-relation metadata, or a separate catalogue relation?
3. Should search result rank be a mandatory output, or is score enough?
4. How should transaction-local new embeddings participate in search before an
   index refresh?
5. Should community detection be an external host effect first, or a runtime
   library over relation facts?
6. What is the minimal model-call effect boundary needed for ingestion and
   answer generation?
7. How much of the knowledge vocabulary belongs in a standard library versus an
   example application?
8. Should retrieval plans be authored in Mica source, relation facts, or both?

## Proposed First Cut

The smallest useful version is:

1. Define `Document`, `TextUnit`, `Entity`, `Mention`, `Relationship`, `Claim`,
   `Embedding`, and provenance relations in an example filein.
2. Store embeddings as opaque bytes or list values.
3. Implement `NearestEmbedding` as an exact in-process computed relation with
   required bound inputs.
4. Write a `hybrid_answer` example that:
    - embeds the query;
    - gets candidate text units;
    - expands to mentioned entities and relationships;
    - filters through authority;
    - records retrieved context and citations.
5. Add ANN indexing only after the computed-relation semantics and retrieval
   records feel right.

That first cut would be enough to show the core thesis:

```text
Mica can do RAG work because retrieval candidates become live relational memory,
not because Mica outsources memory to a vector store.
```
