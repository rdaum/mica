# Retrieval and Embeddings

Mica worlds can accumulate more facts, source text, logs, notes, dialogue, and domain objects than
an author or agent can hold in working memory. Exact relation queries are the ground truth, but they
require the caller to know what relation to ask and which identity to start from.

Retrieval is the bridge from "I have a question or a piece of text" to "which parts of this world
might matter?" It is useful for:

- showing related objects, notes, or events in a live application;
- helping an agent choose relevant context before acting;
- finding lore, tickets, incident notes, or design decisions by meaning rather than by exact string
  match;
- recording what context was used to produce an answer or recommendation.

Embeddings are one way to implement that bridge. An embedding is a vector representation of some
text or object description. Similar vectors are treated as candidate matches. Mica then uses
ordinary relations to decide what those candidates mean, whether the actor may use them, whether
they are fresh, and how they should be cited.

This division is important. Vector search proposes candidates; it does not establish truth,
authority, or provenance. Those remain relation-level concerns.

When retrieved context is passed to a model, this is the same general family of techniques usually
called retrieval-augmented generation. Mica's version keeps the retrieval trace inside the world
instead of treating it as temporary prompt assembly outside the runtime.

## Retrieval In Mica

Mica represents retrieval as ordinary world state plus one computed search relation.

The ordinary state records:

- what text or object description was indexed;
- which embedding belongs to which subject;
- which vector index includes that embedding;
- what question was asked;
- which context was retrieved;
- what answer or summary was produced;
- which sources were cited;
- whether an answer or embedding needs review or refresh.

The computed relation answers the nearest-neighbour question:

```mica
NearestEmbedding(index, query_embedding, limit, ?subject, ?score, ?snapshot_version)
```

That relation returns candidate subjects for a query vector. The rest of the retrieval workflow is
ordinary Mica code.

The shared vocabulary lives in `apps/shared/retrieval.mica`.

## Text Units

A text unit is a retrievable piece of text. It might be a paragraph, a room description, a ticket
comment, a chat excerpt, a code-review note, or a summary of a domain object.

```mica
TextUnit(#lamp_note)
TextUnitText(#lamp_note, "A brass lamp sits on the workbench.")
```

The text unit is the thing retrieval returns. In a document-heavy system, a text unit will usually
be separate from the domain object it describes. For example, `#lamp` might be the world object and
`#lamp_note_3` might be a text unit describing it.

Small demos can collapse those identities when the object and its description are effectively the
same retrieval subject:

```mica
TextUnit(#coin)
TextUnitText(#coin, "coin: A tarnished brass coin catches the light.")
```

The MUD browser example uses that shortcut so related-context search can return rooms, people, and
objects directly. Larger applications should keep the described subject and the text span separate
when they need provenance, multiple descriptions, document revisions, or citation precision.

## Embeddings And Index Membership

An embedding attaches a vector to a subject:

```mica
Embedding(#emb_lamp_note)
EmbeddingOf(#emb_lamp_note, #lamp_note)
EmbeddingModel(#emb_lamp_note, "mud-world")
EmbeddingVector(#emb_lamp_note, [0.12, 0.70, 0.03])
```

An index decides which embeddings participate in a search surface:

```mica
VectorIndex(#world_index)
VectorIndexMetric(#world_index, "cosine")
VectorIndexContains(#world_index, #emb_lamp_note)
```

The shared filein provides helper verbs for maintaining these facts:

```mica
index_text_unit(actor, #world_index, #lamp_note, "mud-world")
```

That helper reads `TextUnitText`, calls `embed_text(model, text)`, records the embedding facts, and
marks the index entry ready. It also tracks stale or missing embeddings when the text changes.

## Searching

To search, embed the question or selected text, then query `NearestEmbedding`:

```mica
let query_embedding = embed_text("mud-world", "brass light source")

return NearestEmbedding(
  #world_index,
  query_embedding,
  5,
  ?subject,
  ?score,
  ?snapshot_version
)
```

`index`, `query_embedding`, and `limit` must be bound. This is not a relation that can be enumerated
freely; nearest-neighbour results only make sense for a specific query vector and limit.

The result rows are candidates. A high score means "similar according to this embedding model and
metric", not "visible", "correct", "trusted", or "the best answer".

## Snapshot Version

`NearestEmbedding` returns `snapshot_version` because exact search scans the relation facts visible
to the reader. There is no separate approximate nearest-neighbour index build with its own version.

That distinction matters for freshness. An approximate index should expose an index or build version
so retrieval code can decide whether the index is fresh enough. The exact relation reports the
snapshot it read from.

## Retrieval Authority

Retrieval is a read path. It must not leak hidden context just because vector search found a similar
subject.

The shared retrieval verb records context only when the actor may retrieve the candidate:

```mica
CanRetrieveSubject(actor, subject)
```

Applications define this relation using their own policy. In the MUD browser example:

```mica
CanRetrieveSubject(actor, subject) :-
  CanSee(actor, subject)
```

This check happens before a `RetrievedContext` fact is recorded. Filtering only at display time
would be too late: the forbidden subject would already be part of the retrieval trace and could be
passed to an answer-generation tool.

## Retrieval Artefacts

`retrieve_context` records an auditable trace of the search:

```mica
let result = retrieve_context(
  #alice,
  #world_index,
  "brass light source",
  5,
  "mud-world"
)
```

The verb records a question, a retrieval plan, and one `RetrievedContext` per authorized candidate:

```mica
Question(question)
QuestionText(question, "brass light source")

RetrievalPlan(plan)
PlanForQuestion(plan, question)
PlanKind(plan, "nearest_embedding")
PlanModel(plan, "mud-world")

RetrievedContext(context)
ContextForPlan(context, plan)
ContextSubject(context, #lamp_note)
ContextScore(context, 0.92)
ContextReason(context, "nearest_embedding")
ContextSnapshotVersion(context, 123)
```

Because this is ordinary relation state, later code can ask:

- what was retrieved for this question?
- which model produced the query embedding?
- which actor was the question for?
- which subjects were excluded by authority?
- which snapshot did the search read?

## Answers And Citations

`answer_question` builds on `retrieve_context`. It records the question, the assembled context text,
the answer text, and citations:

```mica
Answer(answer)
AnswerForQuestion(answer, question)
AnswerPromptText(answer, "brass light source")
AnswerContextText(answer, "\n- A brass lamp sits on the workbench.")
AnswerText(answer, "Relevant context for: brass light source\n- ...")
AnswerCitation(answer, #lamp_note)
AnswerCitationText(answer, #lamp_note, "A brass lamp sits on the workbench.")
AnswerStatus(answer, "fresh")
```

`answer_question` produces an extractive summary. It is not an LLM-backed answer generator. The
relation model for retrieved context, citations, freshness, and review is visible before any host
model call is introduced.

A model-backed answer verb should reuse these artefacts and add model-call facts such as the
selected model, prompt, input context, output, status, and host effect metadata. It should not hide
retrieval and context assembly inside an opaque host call.

## Review And Refresh

Retrieval state is only useful if it can become stale in visible ways.

The shared vocabulary tracks embedding status and answer freshness:

```mica
EmbeddingStatus(embedding, "ready")
EmbeddingRefreshNeeded(index, subject, model)

AnswerStatus(answer, "stale")
AnswerNeedsReview(answer)
AnswerRefreshNeeded(answer)
```

If a text unit changes after an answer cites it, `answer_refresh_status` compares the current text
with the cited text and marks the answer stale. That lets Mica treat retrieval as live workspace
memory rather than as a throwaway prompt-building step.

## What This Is Not

The retrieval layer is not a vector database bolted onto Mica. It is not a promise that semantic
similarity is truth. It is not a replacement for relations, rules, authority, or provenance.

The intended shape is:

```text
embeddings/search -> candidate subjects
ordinary relations -> authority, freshness, provenance, explanation
retrieval artefacts -> auditable context and citations
```

That is why retrieval results are recorded back into the world. Humans and agents should be able to
inspect not only an answer, but the path by which Mica found and authorized the context behind it.
