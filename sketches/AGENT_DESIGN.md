# Mica Agent: Relation-First LLM Coding Agent Design

## Purpose

Map the ontology and agent-loop structures of two reference coding agents — pi-mono
(`packages/agent`, `packages/coding-agent`) and OpenAI Codex (`codex-rs/core`) — onto Mica's
relation-first object system, and lay out how the `apps/agent` shell evolves from its current UI
skeleton into a durable, multi-user, web-driven coding agent.

This document is design, not spec. It records the mapping, the tensions, and the next increments. It
assumes familiarity with the Mica architecture (AGENTS.md, apps/mud) and with the pi-mono and
codex-rs sources.

---

## 1. pi-mono's Ontology

pi-mono separates three layers:

### 1.1 LLM message model (`packages/ai/src/types.ts`)

A fixed, provider-neutral message algebra:

- `UserMessage` — `role: "user"`, content is string or `(Text | Image)[]`.
- `AssistantMessage` — `role: "assistant"`, content is `(Text | Thinking | ToolCall)[]`, carries
  `usage`, `stopReason`, `model`, `provider`, `api`.
- `ToolResultMessage` — `role: "toolResult"`, `toolCallId`, `toolName`, content `(Text | Image)[]`,
  `isError`, opaque `details`.
- `Message = User | Assistant | ToolResult`.
- `Tool` — `name`, `description`, `parameters` (TypeBox schema). No execute.
- `Context` — `systemPrompt`, `messages: Message[]`, `tools: Tool[]`.

This is the **boundary algebra**: everything the LLM sees must reduce to this.
`AssistantMessageEvent` is the streaming delta algebra (`text_delta`, `toolcall_delta`,
`thinking_delta`, `done`, `error`).

### 1.2 Agent message model (`packages/agent/src/types.ts`)

`AgentMessage = Message | CustomAgentMessages[keyof CustomAgentMessages]`.

Apps extend the union via declaration merging. The coding agent adds `BashExecutionMessage`,
`CompactionSummaryMessage`, `BranchSummaryMessage`, `CustomMessage`. These are **UI/session
messages** that never reach the LLM.

Two transforms bridge the algebras:

- `transformContext(AgentMessage[]) -> AgentMessage[]` — prune, inject context (compaction
  summaries, file lists, external context). Runs at AgentMessage level before each LLM call.
- `convertToLlm(AgentMessage[]) -> Message[]` — filter UI-only messages, convert custom types to
  user/assistant/toolResult. Required.

### 1.3 Agent state and loop (`packages/agent/src/agent.ts`, `agent-loop.ts`)

`AgentState`: `systemPrompt`, `model`, `thinkingLevel`, `tools`, `messages`, `isStreaming`,
`streamMessage`, `pendingToolCalls`, `error`.

The loop (`runLoop`):

```
outer while:                          # follow-up loop
  inner while:                        # tool + steering loop
    inject pending messages
    stream assistant response         # transformContext -> convertToLlm -> LLM
    if error/aborted: end
    execute tool calls (sequentially)
      after each tool: check steering -> skip remaining, queue steering
    check steering after turn
  check follow-up queue -> if any, continue outer
agent_end
```

Key control structures:

- **Steering** — `getSteeringMessages()` polled after each tool and after each turn. If present,
  remaining tools are skipped with error results, steering messages injected, loop continues. Modes:
  `one-at-a-time` | `all`.
- **Follow-up** — `getFollowUpMessages()` polled when the agent would stop. If present, injected as
  a new turn.
- **Tools** — `AgentTool<TParams, TDetails>` extends `Tool` with `label` and
  `execute(toolCallId, params, signal, onUpdate) -> AgentToolResult<TDetails>`. Execute throws on
  error; the loop catches and reports `isError: true` to the LLM. `onUpdate` streams partial
  results.

Event algebra (`AgentEvent`): `agent_start/end`, `turn_start/end`, `message_start/update/end`,
`tool_execution_start/update/end`. The `Agent` class subscribes, updates `AgentState`, re-emits to
UI listeners.

### 1.4 Coding-agent session layer (`packages/coding-agent/src/core/agent-session.ts`)

`AgentSession` wraps `Agent` and adds:

- **Session persistence** — `SessionManager` appends every `message_end` to an append-only session
  file (JSONL). Entry types: `message`, `thinking_level_change`, `model_change`, `compaction`,
  `branch_summary`, `custom`, `label`, `session_info`. `buildSessionContext()` reconstructs
  `AgentMessage[]` from entries, inserting compaction/branch-summary messages.
- **Compaction** — when context tokens exceed a threshold, older messages are summarized into a
  `CompactionSummaryMessage`, a `CompactionEntry` is persisted, and the session is reloaded from the
  compaction boundary. File operation tracking (`readFiles`, `modifiedFiles`) carries forward so the
  agent remembers what it touched.
- **Branch summarization** — fork a session, summarize the forked tail into a
  `BranchSummaryMessage`, persist `BranchSummaryEntry`.
- **Auto-retry** — on retryable errors (overloaded, rate limit), retry with backoff, emit
  `auto_retry_start/end`.
- **Extensions** — `ExtensionRunner` emits typed events (`before_agent_start`, `turn_start/end`,
  `input`, `model_select`, `session_before_switch`, etc.), can transform input, inject messages,
  modify system prompt, register commands and tools.
- **System prompt assembly** —
  `buildSystemPrompt({ cwd, skills, contextFiles, selectedTools, toolSnippets, promptGuidelines, appendSystemPrompt })`.
  Rebuilt when tools change.
- **Bash execution** — out-of-band `BashExecutionMessage` entries, separate from LLM tool results.
- **Slash commands and skills** — `/skill:name` expands skill file content into a `<skill>` block;
  prompt templates expand `/template` macros.

### 1.5 Tools (`packages/coding-agent/src/core/tools/`)

`read`, `bash`, `edit`, `write`, `grep`, `find`, `ls`. Each is
`createXTool(cwd, options) -> AgentTool`. Tools are cwd-scoped factory functions, not singletons.
`bash` streams output; `edit` does find-and-replace with diff; `read` truncates with head/tail
windows.

---

## 2. Mapping Onto Mica

### 2.1 Guiding principle

pi-mono's ontology is **imperative + in-memory**: state is a mutable `AgentState` object, messages
are an array, session is a JSONL file. Mica's ontology is **relational + durable**: state is
relation facts, messages are durable tuples, session is a relation slice. The mapping is:

| pi-mono concept                | Mica analogue                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `AgentState` (mutable object)  | Per-session functional relations (`session/CommandDraft`, `session/Revision`, etc.)                         |
| `AgentMessage` (union type)    | `Message` identity + `MessageRole`/`MessageContent`/`MessageSeq` facts; role dispatch via `Delegates` frobs |
| `Message[]` (ordered array)    | `MessageTranscript(message, transcript)` + `MessageSeq(message, n)`                                         |
| `Agent` (class, event emitter) | Mica verbs + sync view revision/tree; events become revision bumps                                          |
| `AgentEvent` stream            | `sync_view_revision` / `sync_view_tree` deltas (the host diffs and patches)                                 |
| `Tool` (interface + execute)   | `Tool` identity + `ToolName`/`ToolDescription`/`ToolParameters` facts + `tool/invoke` verb                  |
| `ToolResult`                   | `ToolResult` identity + `ToolResultCall`/`ToolResultContent`/`ToolResultIsError` facts                      |
| `transformContext`             | Derived relation or verb over `Message` facts producing a `ContextMessage` view                             |
| `convertToLlm`                 | Verb `agent/llm_messages(transcript)` returning `Message[]`-shaped values                                   |
| `SessionManager` (JSONL)       | `Transcript`/`Message` durable relations — persistence is automatic                                         |
| `CompactionEntry`              | `Compaction` identity + `CompactionTranscript`/`CompactionSummary`/`CompactionFirstKept` facts              |
| `BranchSummaryEntry`           | `Branch` identity + `BranchParent`/`BranchSummary` facts                                                    |
| `ExtensionRunner`              | `Delegates`-dispatched frobs + `Extension` identities with `ExtensionHook` relations                        |
| `buildSystemPrompt`            | Verb `agent/system_prompt(agent, workspace)` assembling from `Skill`/`ContextFile`/`Tool` relations         |
| Steering queue                 | `SteeringMessage` relation keyed by session                                                                 |
| Follow-up queue                | `FollowUpMessage` relation keyed by session                                                                 |

### 2.2 What Mica gives you for free

- **Durability** — transcript and messages are relation facts in the store. No JSONL append logic,
  no session file format, no reload parse path. A session is a `Transcript` identity; its messages
  are the `MessageTranscript` slice. Branching is a new `Transcript` with `BranchParent`.
- **Multi-user** — `session/Actor` scopes per-user view state; transcripts can be shared or private
  via `CanRead`/`CanWrite` policy. pi-mono is single-process, single-user; Mica is
  session-multi-tenant by construction.
- **Derived views** — `agent/llm_messages(transcript)` can be a derived relation, so compaction
  summaries and branch summaries are naturally interposed without rewriting the message log.
- **Authority** — tool execution is gated by `CanInvoke`/`CanEffect` policy per actor, not by a
  global `cwd`. Different users can have different tool surfaces over the same workspace.
- **Source navigation** — `source/RepositoryEntry`, `source/FileText`, `source/SyntaxLine`,
  `source/SyntaxOutline`, `source/DefinitionAt`, `source/ReferencesOf`, `source/SymbolSearch`,
  `source/TextSearch`, `source/CommitLog`, `source/FileDiff`, `source/FileBlame`,
  `source/FileHistory` are already computed relations. Tools become thin verbs over these relations
  rather than subprocess wrappers.

### 2.3 What Mica does not give you for free

- **Streaming** — the LLM token stream has no native Mica representation. The loop must bridge the
  async stream into relation writes (append partial `MessageContent` facts, bump revision) or into a
  side-channel that the sync host surfaces. The MUD's event/narrative model is a precedent: events
  are durable facts, but the _narrative_ is a bounded recent-window view over them.
- **The loop itself** — Mica has no built-in agent loop. The loop is a verb (`agent/run`) that
  submits to the LLM, processes the response, executes tools, and recurses. It must be
  suspended/resumed across async boundaries (the runtime's task suspension model).
- **LLM provider abstraction** — pi-mono's `packages/ai` is a substantial multi-provider streaming
  client. Mica's `apps/shared/openai.mica` is a starting point but covers only one provider and no
  streaming.

---

## 3. Proposed Relation Schema

### 3.1 Existing (apps/agent/core.mica)

Already defined: `Workspace`, `Agent`, `Transcript`, `Message`, `InspectorTarget`,
`MessageRole`/`MessageContent`/`MessageSeq`, policy relations. This is the skeleton.

### 3.2 Additions for the loop

```mica
// Tool definitions — durable, authorable, per-workspace
make_identity(:tool)
make_relation(:Tool, 1)
make_functional_relation(:ToolName, 2, [0])
make_functional_relation(:ToolDescription, 2, [0])
make_functional_relation(:ToolParameters, 2, [0])   // JSON schema as string
make_relation(:ToolEnabled, 2)                        // (workspace, tool)

// Tool calls and results — durable records of what the agent did
make_identity(:tool_call)
make_identity(:tool_result)
make_relation(:ToolCall, 1)
make_functional_relation(:ToolCallMessage, 2, [0])    // (call, assistant_message)
make_functional_relation(:ToolCallId, 2, [0])
make_functional_relation(:ToolCallName, 2, [0])
make_functional_relation(:ToolCallArguments, 2, [0])  // JSON string
make_functional_relation(:ToolCallStatus, 2, [0])     // "pending" | "executing" | "done" | "error" | "skipped"
make_relation(:ToolResult, 1)
make_functional_relation(:ToolResultCall, 2, [0])
make_functional_relation(:ToolResultContent, 2, [0])
make_functional_relation(:ToolResultIsError, 2, [0])
make_functional_relation(:ToolResultDetails, 2, [0])

// Loop state — per-session, functional
make_functional_relation(:session/IsStreaming, 2, [0])
make_functional_relation(:session/PendingToolCalls, 2, [0])  // count or set
make_functional_relation(:session/SteeringQueue, 2, [0])     // list of messages
make_functional_relation(:session/FollowUpQueue, 2, [0])

// Compaction and branching
make_identity(:compaction)
make_relation(:Compaction, 1)
make_functional_relation(:CompactionTranscript, 2, [0])
make_functional_relation(:CompactionSummary, 2, [0])
make_functional_relation(:CompactionFirstKept, 2, [0])
make_functional_relation(:CompactionTokensBefore, 2, [0])

make_identity(:branch)
make_relation(:Branch, 1)
make_functional_relation(:BranchTranscript, 2, [0])
make_functional_relation(:BranchParent, 2, [0])
make_functional_relation(:BranchSummary, 2, [0])

// System prompt assembly
make_functional_relation(:SystemPrompt, 2, [0])       // (agent, prompt_text)
make_relation(:Skill, 1)
make_functional_relation(:SkillName, 2, [0])
make_functional_relation(:SkillPath, 2, [0])
make_functional_relation(:SkillContent, 2, [0])
make_relation(:ContextFile, 2)                          // (workspace, path)
make_functional_relation(:ContextFileContent, 3, [0, 1])
```

### 3.3 Role dispatch for messages

pi-mono uses a discriminated union. Mica uses `Delegates` frobs:

```mica
make_identity(:message/user)
make_identity(:message/assistant)
make_identity(:message/tool_result)
make_identity(:message/system)
make_identity(:message/compaction_summary)
make_identity(:message/branch_summary)
make_identity(:message/bash_execution)

assert Delegates(#message/user, #message, 0)
assert Delegates(#message/assistant, #message, 0)
assert Delegates(#message/tool_result, #message, 0)
// ...
```

A message identity is `frob(#message/assistant, [transcript, seq])`. Its role is
`frob_delegate(message)`. Role-specific rendering and conversion dispatch through `Delegates`,
mirroring the MUD's `event/movement` / `event/say` pattern.

### 3.4 Tool calls as content

pi-mono's `AssistantMessage.content` is an array of `Text | Thinking | ToolCall`. In Mica, an
assistant message's _text_ lives in `MessageContent`. Its _tool calls_ live as separate `ToolCall`
identities linked back via `ToolCallMessage(call, message)`. This normalises the content array into
relation facts, at the cost of needing a `agent/message_content_nodes(message)` verb to reassemble
for rendering. The win: tool calls are first-class durable objects that can be inspected, replayed,
and audited independently.

---

## 4. Agent Loop Design

### 4.1 The loop verb

```mica
verb agent/run(agent @ #agent, transcript, user_message)
  // 1. Append user message
  agent/append_message(agent, "user", user_message)
  ui/bump_revision(endpoint())

  // 2. Outer follow-up loop
  agent/run_loop(agent, transcript)
end

verb agent/run_loop(agent @ #agent, transcript)
  // 3. Inner tool+steering loop
  let has_more = true
  while has_more
    // Check steering queue
    let steering = session/steering_queue(endpoint())
    if steering != []
      agent/inject_messages(agent, transcript, steering)
      session/steering_queue(endpoint()) = []
    end

    // Stream assistant response
    let assistant_msg = agent/stream_response(agent, transcript)
    if assistant_msg == nothing || assistant_msg.stop_reason == "error"
      return
    end

    // Execute tool calls
    let tool_calls = agent/message_tool_calls(assistant_msg)
    has_more = tool_calls != []
    if has_more
      agent/execute_tools(agent, transcript, assistant_msg, tool_calls)
    end
  end

  // 4. Check follow-up queue
  let follow_up = session/follow_up_queue(endpoint())
  if follow_up != []
    session/follow_up_queue(endpoint()) = []
    agent/inject_messages(agent, transcript, follow_up)
    agent/run_loop(agent, transcript)
  end
end
```

### 4.2 Streaming response

`agent/stream_response` is the LLM boundary:

```mica
verb agent/stream_response(agent @ #agent, transcript)
  // transformContext: derive compaction summaries, inject context files
  let context_messages = agent/transform_context(agent, transcript)
  // convertToLlm: filter UI-only messages, map custom types
  let llm_messages = agent/convert_to_llm(context_messages)
  let system_prompt = agent/system_prompt(agent, workspace/active(agent))
  let tools = agent/active_tools(agent)

  // Submit to LLM provider (apps/shared/openai.mica or equivalent)
  // This is the async suspension point.
  let response = llm/stream(system_prompt, llm_messages, tools)

  // Accumulate streamed content into a durable assistant message
  let msg = agent/append_message(agent, "assistant", "")
  let content = ""
  for chunk in response
    content = string_concat(content, chunk.text)
    msg.messageContent = content   // update in place; revision bumps
    ui/bump_revision(endpoint())
  end

  // Record tool calls from the response
  for call in response.tool_calls
    agent/record_tool_call(msg, call)
  end

  return msg
end
```

The streaming chunk loop is the tension point. Two options:

**Option A: Fact-per-chunk.** Each chunk is a `MessageChunk` fact. The transcript view renders by
aggregating chunks. Pros: durable streaming history, replayable. Cons: high write volume, many
relation tuples per message.

**Option B: In-place content update + revision bump.** `MessageContent` is functional; each chunk
updates it and bumps the view revision. The sync host diffs the rendered tree and patches the
browser. Pros: low write volume, matches the MUD's narrative render model. Cons: streaming history
is lost (on crash, only the last-committed content survives).

**Recommendation: Option B.** Streaming history is low-value; the final message is what matters. The
revision-bump pattern is already proven in the MUD. If replay is needed later, a `MessageChunk`
relation can be added without changing the render path.

### 4.3 Tool execution

```mica
verb agent/execute_tools(agent @ #agent, transcript, assistant_msg, tool_calls)
  for call in tool_calls
    agent/record_tool_call_status(call, "executing")
    ui/bump_revision(endpoint())

    let result = nothing
    let is_error = false
    try
      result = tool/invoke(agent, call.name, call.arguments)
    catch err
      result = err
      is_error = true
    end

    agent/record_tool_result(call, result, is_error)
    agent/append_tool_result_message(transcript, call, result, is_error)
    ui/bump_revision(endpoint())

    // Check steering after each tool (pi-mono pattern)
    let steering = session/steering_queue(endpoint())
    if steering != []
      agent/skip_remaining_tools(tool_calls, call)
      return
    end
  end
end
```

`tool/invoke` dispatches by tool name to the implementing verb. Built-in tools map to
source-provider relations:

| Tool         | Implementation                                                |
| ------------ | ------------------------------------------------------------- |
| `read`       | `source/FileText(repo, rev, path)` + `source/FileLines`       |
| `grep`       | `source/TextSearch(repo, rev, pattern, ...)`                  |
| `find`       | `source/RepositoryEntry(repo, rev, path, ...)` filtered       |
| `ls`         | `source/RepositoryEntry(repo, rev, path, ...)`                |
| `edit`       | Read `source/FileText`, apply replacement, `write`            |
| `write`      | Host effect verb writing to the workspace root                |
| `bash`       | Host effect verb spawning a process (needs a new host effect) |
| `symbol`     | `source/SymbolSearch(repo, rev, query, ...)`                  |
| `definition` | `source/DefinitionAt(repo, rev, path, line, col, ...)`        |
| `references` | `source/ReferencesOf(repo, rev, symbol, ...)`                 |
| `blame`      | `source/FileBlame(repo, rev, path, ...)`                      |

The first seven mirror pi-mono's tool set; the last four are new and free because source-provider
already exposes them as relations.

### 4.4 Compaction

pi-mono compacts by summarizing older messages into a `CompactionSummaryMessage` and persisting a
`CompactionEntry`. In Mica:

```mica
verb agent/compact(agent @ #agent, transcript)
  let messages = agent/messages(transcript)
  let token_count = agent/estimate_tokens(messages)
  if token_count < threshold
    return false
  end

  let first_kept = agent/compaction_cutoff(messages)
  let old_messages = agent/messages_before(transcript, first_kept)
  let summary = llm/summarize(system_prompt, old_messages)

  let compaction = frob(#compaction, [transcript, first_kept])
  assert Compaction(compaction)
  assert CompactionTranscript(compaction, transcript)
  assert CompactionSummary(compaction, summary)
  assert CompactionFirstKept(compaction, first_kept)
  assert CompactionTokensBefore(compaction, token_count)

  // Insert a compaction_summary message at the boundary
  agent/append_compaction_message(transcript, summary)
  return true
end
```

`agent/transform_context` checks for the latest `Compaction` fact on the transcript and replaces all
messages before `CompactionFirstKept` with the `CompactionSummary` message. This is a derived view,
not a destructive rewrite — the original messages remain in the store.

### 4.5 Branching

```mica
verb agent/branch(transcript, from_seq)
  let branch = frob(#transcript, [transcript, from_seq])
  assert Transcript(branch)
  assert Branch(branch)
  assert BranchTranscript(branch, branch)
  assert BranchParent(branch, transcript)
  // Copy messages up to from_seq into the branch transcript
  for message in agent/messages_before(transcript, from_seq)
    agent/copy_message_to(branch, message)
  end
  // Optionally summarize the forked tail
  let summary = llm/summarize(...)
  assert BranchSummary(branch, summary)
  return branch
end
```

Branching is cheap: a new `Transcript` identity with `BranchParent` pointing at the source. Messages
are either copied (durable fork) or referenced (lazy fork via a `MessageBranchedFrom` relation).

---

## 5. UI Integration

### 5.1 Transcript rendering

The existing `transcript.mica` renders messages as DOM nodes. The additions:

- **Tool call rendering** — a `ui/tool_call_node(call)` verb rendering the tool name, arguments
  (collapsed by default), and status glyph. Dispatches by tool name for tool-specific rendering
  (bash output, edit diff, file content preview).
- **Thinking rendering** — if `MessageThinking` facts are recorded, render in a collapsible block
  (like pi-mono's `thinking_start/delta/end`).
- **Streaming cursor** — the `streamMessage` concept becomes a `session/StreamMessage` relation; the
  transcript view shows it as an in-progress assistant message with a pulsing cursor.
- **Compaction/branch summary markers** — render as distinct message kinds in the transcript, with
  expand-to-detail.

### 5.2 Inspector targets

The inspector already has `InspectorTarget` with `target/file`, `target/symbol`, `target/diff`,
`target/task`. These map to:

- **File** — `source/FileText` + `source/SyntaxOutline` rendered as a file view with line numbers
  and syntax highlighting.
- **Symbol** — `source/SymbolSearch` result with `source/DefinitionAt`/`source/ReferencesOf`
  navigation.
- **Diff** — `source/FileDiff` for a commit or working-tree change.
- **Task** — a `ToolCall` identity with its arguments, status, and result.

Clicking a tool call in the transcript sets `session/inspect` to the `ToolCall` identity; the
inspector renders its arguments, result, and any diff/ file it touched.

### 5.3 Command bar

The command bar currently sends `agent_command` sync events. Extensions:

- **Slash commands** — `agent/handle_slash_command(agent, text)` dispatches `/compact`, `/branch`,
  `/model`, `/clear`, `/skill:name`, etc. These are Mica verbs, not a separate command registry.
- **Skill expansion** — `/skill:name` reads the `Skill` relation, expands into a `<skill>` block in
  the user message.
- **Steering vs follow-up** — the sync action handler checks `session/IsStreaming`; if true, the
  message is routed to `session/SteeringQueue` or `session/FollowUpQueue` based on a form field or a
  modifier (shift+enter = follow-up, enter = steer).

### 5.4 System prompt panel

A new panel showing the current `SystemPrompt` fact, the active tools (`ToolEnabled`), loaded
skills, and context files. Editable for authorised users; changes update the relations and take
effect on the next turn.

---

## 6. Authority Model

pi-mono has no authority model — the agent runs as the user with full filesystem access. Mica's
policy model is the differentiator:

- **Tool execution** — `CanInvoke(#operator, :tool/bash)` gates whether an actor can run bash. A
  read-only role might have `:tool/read`, `:tool/grep`, `:tool/find`, `:tool/ls` but not
  `:tool/bash`, `:tool/edit`, `:tool/write`.
- **Workspace scope** — `WorkspaceRoot` bounds file access; tools check `source/RepositoryEntry`
  which already enforces `MICA_SOURCE_ROOTS`. A workspace can be shared with some actors and not
  others.
- **Effect authority** — `CanEffect` gates whether the agent can emit host effects (file writes,
  process spawns). A review mode could allow reads and analysis but block effects.
- **Per-session authority** — `session/Actor` binds the web task to an actor; the agent loop runs
  with that actor's authority. Steering and follow-up queues are session-scoped, so only the session
  owner can interrupt.

This is the architectural reason to build on Mica rather than forking pi-mono: multi-user,
multi-workspace, policy-gated coding agents are a native fit, not a retrofit.

---

## 7. Implementation Increments

Ordered by dependency; each is independently shippable. Increments marked with codex lessons
incorporate production hardening from codex-rs/core.

### 7.1 Bind workspaces from MICA_SOURCE_ROOTS (implemented)

Assert `Workspace`/`WorkspaceRoot` facts from the daemon startup source based on
`MICA_SOURCE_ROOTS`. The workspace panel lists real repositories. The inspector can open files via
`source/FileText`.

### 7.2 Tool relation schema + read-only tools (implemented)

`Tool`/`ToolCall`/`ToolResult` relations describe durable tool activity. The `read`, `grep`, `glob`,
and `ls` verbs query source-provider relations without adding host effects, and tool calls and
results are carried through the transcript for the next complete-state request.

### 7.3 LLM streaming bridge (implemented)

`apps/shared/llm.mica` now exposes Responses and Chat Completions stream adapters with one typed
mailbox event vocabulary. The driver can deliver external stream events to a Mica mailbox, and the
agent updates a durable provisional `MessageContent` in batches. Responses requests use
`store: false`, resend the complete Mica-owned context, and preserve response output items needed
for stateless reasoning and tool-call continuations. Browser updates remain ordinary
subscription-driven DOM deltas.

### 7.4 Agent loop (implemented)

`agent/run_loop` submits the current transcript, receives typed stream events, executes requested
tools, appends their results, and continues until the model stops requesting tools or reaches its
round limit. The sync event handler starts this loop for ordinary commands.

### 7.5 Steering and follow-up (implemented)

`SteeringQueue` and `FollowUpQueue` are session-scoped volatile relations. Differential changes to
the steering queue wake a suspended loop immediately, cancel its provisional response, close the
stream mailbox, and resubmit the complete updated context. Follow-up input extends a loop after a
normal response boundary.

### 7.6 Write tools + bash + sandboxing (later)

Implement `edit`, `write`, `bash`. These need host effect builtins (file write, process spawn).
Authority: `CanEffect` + tool-specific `CanInvoke`. Bash output streams into `ToolResultContent` via
revision bumps. Add a sandbox host builtin (landlock on Linux) gated by `PermissionProfile`
relations, and an `Approval` relation for interactive confirmation. Codex's `ToolOrchestrator`
(approval → sandbox → attempt → escalate) is the reference pattern. Add `ExecPolicyRule` relations
for auto-approve/deny pattern matching.

### 7.7 Compaction + branching + window chain (later)

Implement `agent/compact` with `CompactionPhase` (pre-turn, mid-turn), `agent/branch`,
`agent/transform_context`. `agent/convert_to_llm` filters non-LLM message types. The system prompt
panel surfaces compaction state. Add `CompactionWindowId`/`CompactionPreviousWindow`/
`CompactionFirstWindow` facts for codex-style context-window chaining so resume can reconstruct
which window a thread is in. Mid-turn compaction (codex pattern) is essential for long tool chains
that grow context.

### 7.8 System prompt assembly + skills + hooks (later)

Implement `agent/system_prompt` assembling from `Skill`/`ContextFile`/ `ToolEnabled` relations.
Slash commands for skill invocation. The system prompt panel becomes editable. Add `Hook` relations
with `HookEvent` (`session_start`, `turn_start`, `turn_stop`, `pre_compact`, `post_compact`,
`permission_request`) + `hook/run` verb. Codex's stop-hook pattern (blocking the agent from stopping
and forcing another iteration) is a powerful extension point to include.

### 7.9 World-state diffing + retry (later)

Add `WorldState` derived relation over `source/RepositoryEntry`/
`source/FileDiff`/`WorkspaceRevision` + `world_state/diff` verb sending patches to the model (codex
pattern). Add `agent/retry` verb with `RetryAttempt`/`RateLimit` relations wrapping the sampling
request with backoff (codex's `handle_retryable_response_stream_error` pattern).

### 7.10 Multi-agent threading (later)

Implement `agent/spawn` creating a new `Agent` + `Transcript` with `BranchParent`. `AgentStatus`
functional relation (`pending_init`, `working`, `awaiting_input`, `awaiting_approval`, `completed`,
`failed`). `InterAgentCommunication` as `message/agent_message` frobs with
`AgentMessageFrom`/`AgentMessageTo` facts. Parent awaits child's `AgentStatus = :completed` via task
suspension. Codex's `AgentControl`/`AgentResolver`/built-in roles (`explorer.toml`, `awaiter.toml`)
are the reference pattern.

### 7.11 MCP integration (later)

Expose MCP server connections as `McpServer`/`McpTool` relations + host MCP builtin. MCP tools route
through the same `tool/invoke` dispatch as built-in tools. MCP resources and elicitation requests
are first-class sync events. Codex's `McpConnectionManager` is the reference.

### 7.12 Multi-user session scoping (later)

Per-actor transcripts, workspace sharing, role-gated tool surfaces. The policy relations already
exist; this is wiring them into the loop and UI. Codex's `PermissionProfile` + `Guardian` reviewer
pattern maps to Mica's `RoleCan*` + a second `Agent` with review authority.

---

## 8. Open Questions

- **Streaming suspension** — can the Mica task model suspend a verb mid-loop across an async LLM
  stream and resume on each chunk? The runtime supports `TaskSuspended` on commit; the LLM stream is
  a different suspension source. May need a new suspend kind or a host-level streaming builtin that
  drives the verb with `resume_with_value` per chunk. Codex uses tokio streams natively; Mica's
  compio driver has the same async capability but the verb/task boundary needs a bridge.
- **Concurrent tool execution** — codex runs tool calls concurrently via `FuturesOrdered` with a
  parallel lock. Can the Mica runtime spawn multiple tool-invocation tasks concurrently and await
  them as a batch? The `submit_with_authority` + `resume_with_value` task model should support this,
  but the `session/PendingToolCalls` relation needs to track multiple in-flight calls and the sync
  view needs to render them.
- **Tool parameter validation** — pi-mono uses TypeBox schemas; codex uses JSON Schema (`ToolSpec`
  with `schemars`). Mica has no schema type. Options: store the schema as a JSON string and validate
  in the `tool/invoke` verb; or use Mica's relation types to model parameter shapes. The former is
  simpler and matches how LLM tool definitions work.
- **Context window estimation** — pi-mono and codex both have token estimators
  (`approx_token_count`). Mica needs one for compaction triggers. Options: a host builtin for token
  counting, or a heuristic based on message content length. The former is more accurate but adds a
  dependency.
- **Mid-turn compaction** — codex compacts _inside_ the turn loop when the token limit is hit, not
  just between turns. The Mica loop needs a `CompactionPhase` parameter and a token-budget check
  after each sampling request. The `agent/compact` verb must work mid-turn without losing the
  in-flight tool results.
- **World-state diffing** — codex diffs `WorldState` per turn and sends patches to the model. Should
  Mica maintain a `WorldState` relation (git status, file changes, environment) and diff it, or rely
  on the agent re-reading source-provider relations each turn? The former is leaner; the latter is
  simpler. Probably start with the latter and add diffing when context pressure demands it.
- **Shared vs private transcripts** — in a multi-user workspace, is the transcript shared (all users
  see the same conversation) or private (each user has their own agent session)? Probably both,
  gated by policy. The `TranscriptAgent` relation already supports per-agent transcripts; a
  `TranscriptWorkspace` relation enables shared transcripts. Codex's multi-agent threading
  (`InterAgentCommunication`) suggests a third mode: agents communicating across transcripts.
- **Bash sandboxing** — pi-mono runs bash with full process access; codex uses landlock + permission
  profiles + Guardian review. Mica's authority model should constrain bash to the workspace root and
  gate dangerous operations. This requires a sandbox-aware host effect builtin (landlock on Linux,
  equivalent on other platforms) plus an `Approval` relation for interactive confirmation.
- **MCP integration** — codex's MCP support is substantial (`McpConnectionManager`,
  `McpResourceClient`, elicitation, tool discovery). Should Mica expose MCP as host-level relations
  (`McpServer`/`McpTool`) or as a Mica-authored verb layer? The former is more consistent with
  source-provider; the latter is more flexible.
- **Multi-agent threading** — codex's sub-agents are full sessions. In Mica, `agent/spawn` creates a
  new `Agent` + `Transcript` with `BranchParent`. But can the parent await the child's completion
  within the same verb, or does it need a separate task + mailbox? The runtime's task suspension +
  `resume_with_value` should support await, but the `AgentStatus` polling needs a wakeup mechanism.

---

## 9. Codex-rs/core Ontology

Codex (`codex-rs/core`) is a substantially more mature and complex agent than pi-mono. It shares the
same broad shape — submission queue, turn loop, streaming response, tool dispatch, compaction,
rollout persistence — but adds sandboxing, approvals, MCP connectors, multi-agent threading, hooks,
extensions, world-state diffing, and a formal protocol boundary. The core crate is the right place
to study production-grade answers to questions pi-mono defers.

### 9.1 Protocol boundary (`codex-rs/protocol`)

Codex separates a wire protocol from the core implementation. The protocol crate defines the entire
ontology that crosses the submission/event boundary:

- **`Op`** — the submission algebra: `UserInput`, `Interrupt`, `ExecApproval`, `PatchApproval`,
  `Compact`, `ThreadRollback`, `InterAgentCommunication`, `ResolveElicitation`,
  `DynamicToolResponse`, `RefreshMcpServers`, `RunUserShellCommand`, `ThreadSettings`,
  `RealtimeConversation*`, `SetThreadMemoryMode`, `ReloadUserConfig`, `Shutdown`. This is the
  client→agent direction.
- **`EventMsg`** — the event algebra (agent→client): `TurnStarted`, `TurnAborted`, `TurnComplete`,
  `AgentMessageDelta`, `ReasoningDelta`, `PlanDelta`, `CommandExecution`, `McpToolCall`,
  `ExecApprovalRequest`, `PatchApprovalRequest`, `TokenCount`, `ErrorEvent`, `CompactedItem`,
  `ThreadRolledBack`, `GuardianAssessment`, `BackgroundTerminalStarted`, `SubAgentActivity`,
  `InterAgentCommunication`, `WarningEvent`, etc.
- **`ResponseItem`** — the model-history algebra (what the LLM sees and produces): `Message`,
  `Reasoning`, `LocalShellCall`, `FunctionCall`, `CustomToolCall`, `McpToolCall`,
  `FunctionCallOutput`, `CustomToolCallOutput`, `McpToolCallOutput`, `ToolSearchCall`,
  `ToolSearchOutput`, `WebSearchCall`, `ImageGenerationCall`, `AgentMessage` (inter-agent),
  `AdditionalTools`, `Compaction`, `ContextCompaction`, `CompactionTrigger`.
- **`ResponseInputItem`** — the request-side subset sent to the model: `Message`,
  `FunctionCallOutput`, `McpToolCallOutput`, `CustomToolCallOutput`, `ToolSearchOutput`.
- **`TurnItem`** — the turn-stream algebra (what a turn emits to the UI before becoming
  `ResponseItem` history): `UserMessage`, `HookPrompt`, `AgentMessage`, `Plan`, `Reasoning`,
  `CommandExecution`, `DynamicToolCall`, `CollabAgentToolCall`, `SubAgentActivity`, `WebSearch`,
  `ImageView`, `Sleep`, `ImageGeneration`, `FileChange`, `McpToolCall`, `ContextCompaction`.
- **`RolloutItem`** — the persistence algebra: `SessionMeta`, `ResponseItem`,
  `InterAgentCommunication`, `Compacted`, `TurnContext`, `WorldState`, `EventMsg`. This is the
  JSONL/log record unit.
- **`UserInput`** — the input algebra: `Text`, `ContextFile`, `EnvironmentSelection`,
  `InterAgentMessage`, `PlanReviewDecision`, `ImageBase64`, `SessionMeta`. A single user turn can
  carry multiple `UserInput` items.

This is the key lesson: **the protocol crate is the ontology**. Core is one implementation; the TUI,
app-server, and external clients are other implementations of the same protocol. The ontology is
stable and versioned independent of the engine.

### 9.2 Session and submission loop (`session/mod.rs`, `session/handlers.rs`)

`Codex` is a queue pair: `Sender<Submission>` in, `Receiver<Event>` out. The `submission_loop`
drains the submission channel and dispatches each `Op` to a handler. Most ops either mutate session
settings or start/steer a turn.

`Session` holds:

- `SessionState` (locked) — `SessionConfiguration`, `ContextManager` (the history),
  `TokenUsageInfo`, `AutoCompactWindow`, `PreviousTurnSettings`, `AdditionalContextStore`,
  `CurrentTimeReminderState`, `active_connector_selection`.
- `SessionServices` — `ModelClient`, `McpManager`, `PluginsManager`, `SkillsService`,
  `ExecPolicyManager`, `ThreadStore` (rollout persistence), `StateDb`, `NetworkProxy`,
  `UnifiedExecProcessManager`, `GuardianReviewSessionManager`.
- `active_turn` — the current `ActiveTurn` (turn id, task kind, running task handle, mailbox state).

The submission loop is single-threaded per session: one turn at a time, ops queue behind it.
Steering (`steer_input`) injects into the active turn's input queue rather than starting a new turn.

### 9.3 Turn loop (`session/turn.rs`)

`run_turn` is the inner loop. Its structure:

```
run_turn(session, turn_context, input)
  pre-sampling compact (if token budget demands)
  record context updates + world state
  build skills + plugins injection items
  run session-start hooks
  record pending input
  loop:
    drain pending input (steering / mailbox)
    run hooks + record inputs
    capture step context (tools, mcp, connectors)
    run_sampling_request(session, step_context, ...)
      build_prompt(history, router, turn_context, base_instructions)
      stream from model client
      for each ResponseEvent:
        OutputItemAdded -> start streaming turn item to UI
        OutputItemDone -> handle_output_item_done:
          Message -> record, emit AgentMessage turn item
          Reasoning -> record, emit Reasoning turn item
          FunctionCall/CustomToolCall/McpToolCall -> spawn tool future
          LocalShellCall -> spawn shell tool future
        (tool futures run concurrently via FuturesOrdered)
      on stream error: retry with backoff (handle_retryable_response_stream_error)
    if model_needs_follow_up || has_pending_input:
      if token_limit_reached: run_auto_compact (mid-turn); continue
      continue
    else:
      run turn_stop_hooks
      if stop_hook blocks: record hook prompt; continue
      break
  emit turn complete
```

Key differences from pi-mono:

1. **Concurrent tool execution.** Tool calls are spawned as futures into a `FuturesOrdered` and
   drained as they complete, not executed sequentially. The `ToolCallRuntime` holds a
   `parallel_execution` RwLock — tools that don't support parallelism take the write lock; others
   run concurrently.

2. **Mid-turn auto-compaction.** When a sampling request returns `needs_follow_up` and the token
   limit is reached, `run_auto_compact` runs _inside_ the turn loop, summarizing older history and
   continuing without ending the turn. `InitialContextInjection::BeforeLastUserMessage` controls
   whether the compaction summary includes a fresh context reinjection.

3. **Pre-sampling compaction.** Before the first sampling request, `run_pre_sampling_compact` checks
   the token budget and compacts preemptively.

4. **Steering via input queue.** `sess.input_queue.get_pending_input()` drains steering messages
   into the turn at the top of each loop iteration. `has_pending_input` forces `needs_follow_up` so
   the loop continues to the next sampling request with the steering injected.

5. **Stop hooks.** After the model stops (no follow-up needed), `run_turn_stop_hooks` can block the
   stop and force another iteration by injecting a hook prompt. This is the extension point for
   "don't stop yet, do X more."

6. **Retry with backoff.** `handle_retryable_response_stream_error` retries the sampling request on
   retryable errors (overloaded, rate limit) with exponential backoff, reusing the
   `ModelClientSession` across retries.

### 9.4 Tool system (`tools/`)

Codex's tool system is substantially richer than pi-mono's:

- **`ToolRegistry`** — maps `ToolName` (namespace + name) to `ToolHandler` implementations. Handlers
  are in `tools/handlers/`: `shell`, `apply_patch`, `mcp`, `multi_agents`, `unified_exec`, `plan`,
  `view_image`, `request_user_input`, `request_permissions`, `get_context_remaining`,
  `new_context_window`, `sleep`, `wait_for_environment`, `current_time`, `tool_search`,
  `list_available_plugins_to_install`, `request_plugin_install`, `dynamic`, `extension_tools`.
- **`ToolRouter`** — built per turn from the registry + MCP tools + dynamic tools + extension
  tools + plugin tools. Exposes `model_visible_specs` (the tool definitions sent to the LLM) and
  dispatches `ToolCall`s to the right handler.
- **`ToolOrchestrator`** — central approval + sandbox selection + retry. For each tool call:
  approval → select sandbox → attempt → retry with escalated sandbox on denial. Network approvals
  are immediate or deferred.
- **`ToolCallRuntime`** — per-turn runtime holding router, session, step context, diff tracker.
  `handle_tool_call` dispatches a `ToolCall` to the router, managing parallel execution locking and
  cancellation.
- **`ToolRuntime` trait** — the sandbox-facing runtime interface
  (`run(req, sandbox_attempt, ctx) -> Result<Out, ToolError>`). Shell, apply_patch, and unified_exec
  each have their own runtime implementation.
- **`ToolSpec` / `ToolName` / `DiscoverableTool`** — the tool definition surface. `ToolName` is
  namespaced (`namespace::name`) so MCP tools, built-in tools, and extension tools don't collide.

### 9.5 Context manager and history (`context_manager/`)

`ContextManager` is the in-memory transcript:

- `items: Vec<ResponseItem>` — oldest to newest.
- `history_version: u64` — bumped on compaction or rollback.
- `token_info: Option<TokenUsageInfo>` — running token accounting.
- `reference_context_item: Option<TurnContextItem>` — baseline for settings diffing (so the next
  turn sends a diff, not a full reinjection, of context state).
- `world_state_baseline: Option<WorldStateSnapshot>` — baseline for world-state diffing.

`for_prompt(modalities)` produces the `Vec<ResponseItem>` sent to the model. `record_items` appends
with truncation policy. History is _rewritten_ on compaction (items replaced by summary) and
rollback (items truncated). The `history_version` bump signals consumers that the transcript changed
structurally, not just appended.

### 9.6 Compaction (`compact.rs`, `compact_remote.rs`, `compact_remote_v2.rs`)

Three compaction paths:

1. **Inline local** (`run_inline_auto_compact_task`) — summarize older messages locally with the
   configured model, replace history with the summary + kept messages.
2. **Inline remote** (`run_inline_remote_auto_compact_task`) — use a server-side compaction endpoint
   (`supports_remote_compaction`).
3. **Remote v2** — newer server-side compaction with structured replacement history.

Compaction records a `CompactedItem` in the rollout with `replacement_history`, `window_number`,
`first_window_id`, `previous_window_id`, `window_id` — a monotonic chain of context windows so
resume can reconstruct which window a thread is in. Pre- and post-compact hooks run via
`hook_runtime`.

### 9.7 Rollout persistence (`rollout.rs`, `thread-store/`)

`ThreadStore` is the persistence trait: `create_thread`, `read_thread`, `resume_thread`,
`persist_thread`, `fork_thread`, `archive_thread`. Implementations: `LocalThreadStore` (JSONL
files), `InMemoryThreadStore` (tests). `LiveThread` wraps a thread with an in-memory write buffer
and flush semantics.

The rollout is an append-only log of `RolloutItem`s. On resume, the `rollout_reconstruction` module
replays items to rebuild `ContextManager` state, token usage, and world-state baseline. This is the
same pattern as pi-mono's session JSONL, but with a formal trait boundary and structured
reconstruction.

### 9.8 Sandboxing and approvals (`sandboxing/`, `guardian/`, `exec_policy.rs`)

Codex runs tools under Linux landlock / Windows sandboxes with permission profiles (`Disabled`,
`ReadOnly`, `WorkspaceWrite`, `DangerFullAccess`). The `ToolOrchestrator` selects a sandbox per tool
call based on the profile and the tool's requirements. On denial, it escalates to a stricter sandbox
or requests approval.

The **Guardian** (`guardian/`) is a separate reviewer agent that can audit tool calls before
execution. `routes_approval_to_guardian` decides whether an approval routes to the user or to the
Guardian. This is a second agent evaluating the first agent's actions — a safety layer pi-mono lacks
entirely.

`ExecPolicyManager` (`exec_policy.rs`) is a rule engine that auto-approves or auto-denies commands
based on pattern rules, reducing approval fatigue. Rules can be added at runtime via
`RequestPermissionsResponse`.

### 9.9 Multi-agent (`agent/`, `session/multi_agents.rs`, `tools/handlers/multi_agents.rs`)

Codex supports spawning sub-agent threads:

- `AgentControl` — spawn, await, send input, steer sub-agents.
- `AgentStatus` — `PendingInit`, `Working`, `AwaitingInput`, `AwaitingApproval`, `Completed`,
  `Failed`.
- `AgentResolver` — resolves agent role to a configuration (model, instructions, tools, sandbox
  policy).
- `agent/builtins/` — `explorer.toml`, `awaiter.toml` (built-in agent roles as TOML config).
- `InterAgentCommunication` — messages between agents, recorded in history as
  `ResponseItem::AgentMessage`.
- `SubAgentActivityItem` — turn item streaming sub-agent status to the UI.

A sub-agent thread is a full `Codex` session with its own submission queue, history, and rollout.
The parent thread's tool call (`multi_agents` handler) spawns the child and awaits its completion.

### 9.10 Hooks and extensions (`hook_runtime.rs`, `plugins/`, `extensions/`)

**Hooks** (`codex-hooks`) — TOML-configured shell commands that run at lifecycle points:
`session_start`, `turn_start`, `turn_stop`, `pre_compact`, `post_compact`, `permission_request`.
Hooks can inject prompt fragments, block stops, and add context.

**Extensions** (`codex-extension-api`) — a richer plugin model with: `PromptFragment`s (injected
into system prompt by slot: `DeveloperPolicy`, `DeveloperCapabilities`, `ContextualUser`,
`SeparateDeveloper`), `TurnContextContribution`s, `ToolExecutor`s, command handlers, input transform
handlers, session lifecycle handlers. Extensions run in-process (WASM or native) and have a typed
event API.

**MCP** (`mcp/`) — Model Context Protocol server connections. MCP tools are discovered at runtime
and routed through the same `ToolRouter` as built-in tools. MCP resources and elicitation requests
are first-class. `McpConnectionManager` handles connection lifecycle.

**Plugins** (`plugins/`) — a higher-level abstraction over MCP and extensions for discoverable,
installable tool bundles.

---

## 10. Codex → Mica Mapping

### 10.1 What codex adds over pi-mono

| Concept           | pi-mono              | Codex                                                                     |
| ----------------- | -------------------- | ------------------------------------------------------------------------- |
| Protocol boundary | None (in-process TS) | `codex-protocol` crate: Op, EventMsg, ResponseItem, TurnItem, RolloutItem |
| Tool execution    | Sequential           | Concurrent (`FuturesOrdered`), with parallel locking                      |
| Compaction        | Pre-turn             | Pre-turn + mid-turn + remote + window chain                               |
| Sandboxing        | None                 | Landlock / Windows sandbox + permission profiles                          |
| Approvals         | None                 | ExecPolicy + Guardian reviewer + user approval                            |
| Multi-agent       | None                 | Sub-agent threads with inter-agent communication                          |
| Hooks             | Extensions (TS)      | Shell hooks (TOML) + extensions (WASM/native) + MCP                       |
| World state       | None                 | `WorldState` with diff/patch history items                                |
| Retry             | Auto-retry on error  | Retry with backoff + rate-limit tracking                                  |
| Turn context      | None                 | `TurnContext` with per-turn config, skills, environment                   |
| Rollout           | JSONL session file   | `ThreadStore` trait + `RolloutItem` log + reconstruction                  |

### 10.2 Mapping codex concepts to Mica

| Codex concept                             | Mica analogue                                                                                       |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Op` (submission algebra)                 | Sync event actions (`agent_command`, `agent_inspect`, `agent_compact`, `agent_steer`, ...)          |
| `EventMsg` (event algebra)                | Sync view revision/tree deltas (the host diffs and patches)                                         |
| `ResponseItem` (model history)            | `Message` identity + role-dispatched frobs + `MessageContent`/`ToolCall`/`ToolResult` facts         |
| `TurnItem` (turn stream)                  | Transient relation facts + revision bumps (not durable until turn completes)                        |
| `RolloutItem` (persistence)               | Durable relation facts — persistence is automatic, no log replay needed                             |
| `ContextManager` (in-memory history)      | `Transcript`/`Message` relations + `agent/llm_messages(transcript)` derived view                    |
| `SessionState` (locked mutable)           | Per-session functional relations (`session/Revision`, `session/CommandDraft`, ...)                  |
| `SessionServices`                         | Host builtins (`ModelClient`, `McpManager`, `SandboxManager`) + relation-configured services        |
| `ThreadStore`                             | The relation store itself — transcripts are durable by construction                                 |
| `ToolRegistry`/`ToolRouter`               | `Tool` relations + `tool/invoke` verb dispatching by `ToolName`                                     |
| `ToolOrchestrator` (approval + sandbox)   | `CanInvoke`/`CanEffect` policy + `Approval` relation + sandbox host builtin                         |
| `PermissionProfile`                       | `Role` identities + `RoleCanRead`/`RoleCanWrite`/`RoleCanInvoke`/`RoleCanEffect`                    |
| `Guardian` (reviewer agent)               | A second `Agent` identity with `Delegates(#guardian, #agent, 0)` and review-tool grants             |
| `ExecPolicyManager` (auto-approve rules)  | `ExecPolicyRule` relation (pattern, decision) + `exec_policy/check` verb                            |
| `WorldState` + diff/patch                 | `WorldState` relation + `WorldStateBaseline` functional relation + `world_state/diff` verb          |
| `CompactedItem` + window chain            | `Compaction` identity + `CompactionFirstKept`/`CompactionWindowId`/`CompactionPreviousWindow` facts |
| `InterAgentCommunication`                 | `AgentMessage` frob delegate + `AgentMessageFrom`/`AgentMessageTo` facts                            |
| `AgentControl` (sub-agent spawn)          | `agent/spawn` verb creating a new `Agent` + `Transcript` with `BranchParent`                        |
| `AgentStatus`                             | `AgentStatus` functional relation                                                                   |
| Hooks (shell, TOML)                       | `Hook` identity + `HookEvent`/`HookCommand` relations + `hook/run` verb                             |
| Extensions (PromptFragment, ToolExecutor) | `Extension` identity + `ExtensionHook`/`ExtensionPrompt`/`ExtensionTool` relations                  |
| MCP connections                           | `McpServer` identity + `McpServerConfig`/`McpTool` relations + host MCP builtin                     |
| `TurnContext` (per-turn config)           | Derived from `Agent`/`Workspace`/`ToolEnabled`/`Skill` relations at turn start                      |
| `PreviousTurnSettings`                    | `PreviousTurnModel`/`PreviousTurnCompHash` functional relations                                     |
| `AutoCompactWindow`                       | `AutoCompactWindow` relation + `AutoCompactWindowId` functional relation                            |
| Token usage / estimation                  | `TokenUsage` relation + host token-counting builtin                                                 |

### 10.3 What codex teaches that changes the design

1. **Protocol-first.** Define the ontology as a stable, versioned surface before building the
   engine. In Mica, this means defining the relation schema (identities, relation arities,
   functional positions) as the protocol, and treating the agent loop as one implementation of it.
   The sync view contract (`sync_view_revision`, `sync_view_tree`, `sync_event`) is already this
   kind of boundary; extend it with agent-specific actions.

2. **Concurrent tool execution.** pi-mono's sequential tool execution is a simplification, not a
   design. Codex proves that concurrent execution with a parallel lock is the production answer. In
   Mica, tool calls can be spawned as concurrent tasks (the runtime's task model supports this); the
   `session/PendingToolCalls` relation tracks in-flight calls.

3. **Mid-turn compaction.** Compaction inside the turn loop (not just before turns) is essential for
   long-running tool chains that grow the context. The Mica loop should check token budget after
   each sampling request and compact mid-turn if needed, using the same `agent/compact` verb with a
   `CompactionPhase` parameter.

4. **Window chain.** Codex's monotonic context-window chain (`first_window_id`,
   `previous_window_id`, `window_id`) lets resume reconstruct which compaction window a thread is
   in. In Mica, this is `CompactionWindowId`/`CompactionPreviousWindow`/`CompactionFirstWindow`
   facts on the `Compaction` identity. Resume is a relation query, not log replay.

5. **World-state diffing.** Codex maintains a `WorldState` (git status, environment, file changes)
   and diffs it per turn, sending only the patch to the model. This keeps context lean without
   losing state. In Mica, `WorldState` can be a derived relation over `source/RepositoryEntry`,
   `source/FileDiff`, and `WorkspaceRevision`; the diff is a verb
   `world_state/diff(baseline, current)`.

6. **Approval + sandbox as policy.** Codex's `PermissionProfile` + `ExecPolicy` + `Guardian` is the
   production answer to safe tool execution. Mica's `CanInvoke`/`CanEffect` + `RoleCan*` relations
   are the policy layer; sandboxing is a host builtin; the Guardian is a second agent with review
   authority. This is a native fit, not a retrofit.

7. **Steering via input queue, not just turn boundaries.** Codex's `input_queue` drains steering
   messages at the top of each loop iteration, and `has_pending_input` forces `needs_follow_up` so
   the loop continues. This is cleaner than pi-mono's `getSteeringMessages()` poll after each tool.
   In Mica, a `PendingInput` relation keyed by session/turn serves the same role.

8. **Stop hooks.** The ability for hooks to block the agent from stopping and force another
   iteration is a powerful extension point. In Mica, `Hook` relations with
   `HookEvent = :turn_stop` + a `hook/should_stop` verb returning a continuation prompt enables
   this.

9. **Retry with backoff.** Codex's `handle_retryable_response_stream_error` with rate-limit tracking
   is essential for production. In Mica, an `agent/retry` verb with `RetryAttempt`/`RateLimit`
   relations wraps the sampling request.

10. **Multi-agent as threaded sessions.** Codex's sub-agents are full sessions with their own
    history and rollout. In Mica, `agent/spawn` creates a new `Agent` + `Transcript` with
    `BranchParent` pointing at the parent transcript. Inter-agent communication is
    `message/agent_message` frobs with `AgentMessageFrom`/`AgentMessageTo` facts. The parent awaits
    the child's `AgentStatus = :completed`.

---

## 11. What To Keep From Each, What To Discard

### From pi-mono

**Keep:**

- The loop structure (outer follow-up, inner tool+steering, streaming response, sequential tool
  execution with steering checks).
- The message algebra (user/assistant/toolResult + custom types via delegation).
- The tool interface (name, description, parameters, execute, streaming updates).
- Compaction and branching as first-class concepts.
- System prompt assembly from skills, context files, and tool snippets.
- Steering and follow-up as the interruption model.

**Discard / replace:**

- The `Agent` class and event emitter — replaced by verbs and sync view revisions.
- `SessionManager` JSONL persistence — replaced by durable relation facts.
- `ExtensionRunner` — replaced by `Delegates`-dispatched frobs and relation-configured hooks.
- The in-memory `AgentState` — replaced by per-session functional relations.
- The tool factory pattern (`createReadTool(cwd)`) — replaced by workspace-scoped verbs over
  source-provider relations.
- The TypeBox parameter schema — replaced by JSON-string schemas validated in `tool/invoke`.

### From codex-rs/core

**Keep:**

- The protocol-first design: define the relation schema as a stable, versioned ontology before
  building the engine.
- Concurrent tool execution with a parallel lock (not pi-mono's sequential model).
- Mid-turn compaction with `CompactionPhase` (pre-turn, mid-turn).
- The context-window chain (`CompactionFirstWindow`/
  `CompactionPreviousWindow`/`CompactionWindowId`) for resume.
- World-state diffing per turn (send patches, not full state, to the model).
- The `TurnContext` pattern: capture per-turn config (model, tools, skills, environment) once and
  share across the turn.
- Stop hooks (`HookEvent = :turn_stop` blocking the agent from stopping).
- Retry with backoff + rate-limit tracking.
- Multi-agent as threaded sessions with inter-agent communication.
- The `PreviousTurnSettings` pattern for turn-to-turn model/reasoning continuity.
- `ExecPolicy` auto-approve rules as a relation (`ExecPolicyRule` + `exec_policy/check`).
- The Guardian reviewer pattern (a second agent with review authority).
- MCP as a runtime tool source (`McpServer`/`McpTool` relations + host MCP builtin).

**Discard / replace:**

- `codex-protocol` crate as a separate wire protocol — the relation schema _is_ the protocol; sync
  events are the wire format.
- `ContextManager` (in-memory `Vec<ResponseItem>`) — replaced by `Transcript`/`Message` durable
  relations + `agent/llm_messages` derived view.
- `ThreadStore` trait + JSONL rollout — replaced by the relation store; resume is a relation query,
  not log replay.
- `RolloutItem` log + `rollout_reconstruction` — replaced by durable facts; no replay needed.
- `SessionState` (locked mutable struct) — replaced by per-session functional relations.
- `SessionServices` (Arc'd service bag) — replaced by host builtins + relation-configured services.
- `ToolOrchestrator` (approval + sandbox + retry struct) — replaced by `CanInvoke`/`CanEffect`
  policy + `Approval` relation + sandbox host builtin + `agent/retry` verb.
- `ToolRegistry`/`ToolRouter` (Rust trait objects) — replaced by `Tool` relations + `tool/invoke`
  verb dispatch.
- `ToolRuntime` trait + per-tool runtime impls — replaced by workspace-scoped verbs over
  source-provider relations + host effect builtins.
- Hooks as TOML-configured shell commands — replaced by `Hook` relations with
  `HookEvent`/`HookCommand` + `hook/run` verb (can delegate to shell or Mica verbs).
- Extensions as WASM/native plugin modules — replaced by `Extension` identities +
  `Delegates`-dispatched frobs + `ExtensionPrompt`/ `ExtensionTool` relations.

**Keep as host builtins:**

- LLM provider streaming (both pi-mono's `packages/ai` and codex's `ModelClient` surface) — Mica
  needs an equivalent host builtin or a Mica-authored streaming HTTP client. The `openai.mica`
  filein is a start but needs streaming.
- Token counting — a host builtin (codex uses `codex_utils_output_truncation::approx_token_count`;
  Mica needs equivalent).
- Process spawning (for bash) — a host effect builtin with policy hooks (codex uses
  `SandboxManager` + landlock; Mica needs sandbox-aware spawning).
- Sandboxing (landlock / Windows) — a host builtin gated by `CanEffect`/`CanInvoke` policy.
- MCP connection management — a host builtin exposing `McpServer`/ `McpTool` relations.

### Net design

Keep pi-mono's proven loop semantics and codex's production hardening (concurrent tools, mid-turn
compaction, world-state diffing, approvals, multi-agent) while replacing both imperative substrates
with Mica's durable, relational, multi-user model. The relation schema is the protocol; the agent
loop is a verb; persistence is automatic; authority is policy-derived; tools are workspace-scoped
verbs over source-provider relations; streaming is revision-bump diffs through the sync host.

---

## 12. Staged Project Plan

> Implementation note (July 2026): the early streaming stages below record the original plan and are
> no longer authoritative. The implementation chose mailbox-based chunk delivery rather than
> host-side accumulation, uses Responses by default, retains Chat Completions as an explicit
> provider adapter, and does not rely on `previous_response_id`. Continue current work from the
> later agent-capability stages rather than reimplementing Stage 0.

The plan is concrete for the first few stages and gets progressively vaguer. The goal of the early
stages is to build and test the streaming LLM client as a standalone piece, then wire it into the
existing agent shell, then build the loop, then layer on tools, compaction, and the rest.

### Stage 0: Streaming LLM client — host builtin

**Goal:** a `llm/chat_stream` host request function that streams chat-completion chunks from an
OpenAI-compatible endpoint (OpenRouter by default) and returns the assembled response to the verb.

**Why this first:** it is the hardest runtime piece and the one with the most unknowns. It is
testable in isolation with `OPENROUTER_API_KEY` and a cheap model. Everything else (the loop, tools,
compaction) is Mica verbs over relations, which is the easy part once the LLM boundary works.

**Current state:** `openai_chat_completion` is a non-streaming host request
(`crates/runtime/src/openai.rs`). The daemon's `external_http.rs` handler does a blocking POST via
`cyper` and returns the full JSON response. The `ExternalRequestHandler` is
`Arc<dyn Fn(ExternalRequest) -> Future<Value>>` — it returns a single `Value`, not a stream. The
driver's `spawn_external_request_resume` resumes the task once with that value.

**Design decision — streaming strategy:**

The single-resume `ExternalRequest` model cannot deliver chunks to a verb incrementally. Two
options:

1. **Accumulate in the host, resume once.** The host handler reads the SSE stream, accumulates
   text + tool calls, and resumes the task with the final assembled `Value` (a map with `:text`,
   `:tool_calls`, `:usage`, `:stop_reason`). The verb sees only the final result. Streaming-to-UI is
   handled separately (see Stage 1).

2. **Mailbox-based chunk delivery.** The verb creates a mailbox, passes the receiver to the host
   request, and suspends on `MailboxRecv`. The host handler reads the SSE stream and pushes chunk
   values to the mailbox. The verb loops on `mailbox_recv`, appending chunks to `MessageContent` and
   bumping revision, until the host sends a terminal chunk.

Option 1 is simpler and sufficient for a first cut. Option 2 enables true token-by-token UI
streaming but requires the host handler to access the mailbox send capability (currently host
handlers only return a single `Value`, they don't have access to the runtime's mailbox system).

**Recommendation: Option 1 for Stage 0, Option 2 as a later enhancement.** The host handler
accumulates the stream and returns the assembled response. The verb writes `MessageContent` once and
bumps revision. The UI re-renders. This is not token-by-token streaming, but it is a working LLM
round-trip. Token-by-token streaming can be added later via mailbox or a new
`StreamingExternalRequest` suspend kind.

**Tasks:**

1. Add `llm_chat_stream` to `crates/runtime/src/openai.rs` (or a new `crates/runtime/src/llm.rs`),
   alongside the existing `openai_chat_completion`. Payload fields: `model`, `messages`, `tools`,
   `options` (temperature, max_tokens, etc.). Service: `Symbol::intern("openai")` (reuse the
   existing service so the daemon's `external_http.rs` handler picks it up).

2. In `crates/daemon/src/external_http.rs`, handle the streaming case: when
   `options[:stream] == true` (or the request payload sets a `stream` field), set `"stream": true`
   in the OpenAI request body, read the SSE response with `cyper`, parse `data:` lines, accumulate
   `choices[0].delta.content` and `choices[0].delta.tool_calls`, and return the assembled result as
   a `Value` map:
   `{:text -> "...", :tool_calls -> [...], :usage -> {...}, :stop_reason -> "stop"|"tool_use"|"length", :model -> "...", :provider -> "openrouter"}`.

3. SSE parsing: split on `\n\n`, parse `data: {...}` lines, handle `data: [DONE]`. Accumulate
   content deltas. Parse `usage` from the final chunk if present (OpenRouter includes it).

4. Tool call accumulation: OpenAI streaming delivers tool calls as `delta.tool_calls[i]` with `id`,
   `function.name` (first chunk only), `function.arguments` (incremental). Accumulate per-index into
   a list of `{:id -> ..., :name -> ..., :arguments -> "..."}` maps.

5. Tests: unit test the SSE parser with fixture data (no API key needed). Integration test with
   `OPENROUTER_API_KEY` env and a cheap model (e.g. `deepseek/deepseek-chat-v3.1:free` or
   `google/gemini-2.5-flash-preview-05-20`). Test both a plain text completion and a tool-call
   completion. Gate integration tests behind the env var (skip if unset).

6. Mica wrapper: add `apps/shared/llm.mica` with verbs `llm/system_message`, `llm/user_message`,
   `llm/assistant_message`, `llm/tool_message`, `llm/chat(model, messages, tools)` returning the
   assembled response map. This replaces `apps/shared/openai.mica` for the agent app (keep
   openai.mica for backward compat).

**Testing:**

```sh
# Unit tests (SSE parser, no API key needed)
cargo test -p mica-daemon -- external_http

# Integration test (requires OPENROUTER_API_KEY)
OPENROUTER_API_KEY=sk-or-... cargo test -p mica-daemon -- llm_stream -- --ignored

# Manual smoke test via mica eval
OPENROUTER_API_KEY=sk-or-... cargo run --bin mica -- eval \
  --filein apps/shared/llm.mica \
  'return llm/chat("deepseek/deepseek-chat-v3.1:free", \
    [llm/user_message("Say hello in one word.")], [])'
```

**Done when:** `llm/chat` returns an assembled response with text and tool calls from a real
OpenRouter model, unit tests pass, and the Mica wrapper verbs work via `mica eval`.

---

### Stage 1: Wire the LLM into the agent shell

**Goal:** replace the placeholder echo in `agent_command` with a real LLM call. The user sends a
message, the agent appends it to the transcript, calls `llm/chat`, appends the assistant response,
and the UI re-renders.

**Why this stage:** it exercises the full round-trip (sync event → verb → LLM host request →
relation writes → revision bump → sync view delta) with a real model, but without tools or a loop.
It proves the LLM boundary works end-to-end through the web UI.

**Tasks:**

1. Add `apps/shared/llm.mica` to `scripts/agent.sh` filein list.

2. In `apps/agent/core.mica`, add `AgentModel` functional relation (already defined) and assert a
   default model (`#agent/default.agentModel = "deepseek/deepseek-chat-v3.1:free"`).

3. In `apps/agent/ui-actions.mica`, replace the `agent_command` handler's placeholder echo with:
   - Append user message (`agent/append_message(agent, "user", line)`).
   - Build LLM messages from the transcript (`agent/llm_messages(transcript)` — a verb that maps
     `MessageRole`/`MessageContent` to `llm/user_message`/ `llm/assistant_message`).
   - Call `llm/chat(model, messages, [])`.
   - Append assistant message (`agent/append_message(agent, "assistant", response[:text])`).
   - `ui/bump_revision(endpoint())`.

4. `agent/llm_messages(transcript)` verb: iterate `agent/messages(transcript)`, map each `Message`
   to an LLM message map. For now, only `user` and `assistant` roles (no tool results yet). Prepend
   a system message from `agent/system_prompt(agent)` (a placeholder for now: "You are a helpful
   coding assistant.").

5. Grant `#operator` (and cascade to `#web`) `invoke` on `llm/chat`, `llm/user_message`,
   `llm/assistant_message`, `llm/system_message`.

6. Test: run `scripts/agent.sh`, open `/agent`, type a message, see a real LLM response appear in
   the transcript.

**Done when:** a user message in the web UI produces a real LLM response in the transcript, served
by OpenRouter.

---

### Stage 2: Tool relation schema + read-only tools

**Goal:** define the `Tool`/`ToolCall`/`ToolResult` relation schema and implement `read`, `grep`,
`find`, `ls` as Mica verbs over source-provider relations. The LLM can call tools, see results in
the transcript, and respond to them.

**Tasks:**

1. In `apps/agent/core.mica`, add the tool relations (as sketched in §3.2): `Tool`, `ToolName`,
   `ToolDescription`, `ToolParameters`, `ToolEnabled`, `ToolCall`, `ToolCallMessage`, `ToolCallId`,
   `ToolCallName`, `ToolCallArguments`, `ToolCallStatus`, `ToolResult`, `ToolResultCall`,
   `ToolResultContent`, `ToolResultIsError`.

2. Define tool specs as relation facts (or a verb that returns the tool list as JSON-schema maps).
   For `read`: `ToolName(#tool/read, "read")`, `ToolDescription(#tool/read, "Read file contents")`,
   `ToolParameters(#tool/read, '{"type":"object", "properties":{"path":{"type":"string"}}...}')`.

3. Implement `tool/invoke(agent, tool_name, arguments)` verb dispatching by name to
   `tool/read(agent, arguments)`, `tool/grep(agent, arguments)`, etc. Each reads from
   source-provider relations:
   - `tool/read` → `source/FileText(workspace, revision, path)`.
   - `tool/grep` → `source/TextSearch(workspace, revision, pattern, ...)`.
   - `tool/find` → `source/RepositoryEntry(workspace, revision, path, ...)`.
   - `tool/ls` → `source/RepositoryEntry(workspace, revision, path, ...)`.

4. Wire tools into `llm/chat`: build the `tools` parameter from `ToolEnabled` +
   `ToolName`/`ToolDescription`/`ToolParameters`. Parse tool calls from the LLM response, record
   them as `ToolCall` facts, execute via `tool/invoke`, record `ToolResult` facts, append
   `message/tool_result` entries to the transcript.

5. Transcript rendering: `transcript.mica` renders tool calls and results as DOM nodes (collapsed by
   default, expandable).

6. Bind `Workspace`/`WorkspaceRoot` from `MICA_SOURCE_ROOTS` in the daemon startup source so tools
   have a real repository to read.

7. Grant `#operator` `read` on source-provider relations + `invoke` on `tool/invoke` and individual
   tool verbs.

**Testing:**

```sh
# Manual: ask the agent to read a file
scripts/agent.sh
# In the UI: "Read the file crates/var/src/lib.rs and summarize it"
```

**Done when:** the LLM can call `read`/`grep`/`find`/`ls` tools, see results, and respond to them.
Tool calls and results appear in the transcript.

---

### Stage 3: Agent loop

**Goal:** implement the agent loop so the LLM can make multiple tool calls across multiple turns
until it stops.

**Tasks:**

1. Implement `agent/run_loop(agent, transcript)` (as sketched in §4.1):
   - Call `llm/chat` with current transcript + tools.
   - If response has tool calls: execute them, append results, loop.
   - If response has no tool calls: append assistant message, stop.
   - Check `session/IsStreaming` and steering/follow-up queues (stub for now — empty queues, loop
     just runs to completion).

2. The `agent_command` sync handler calls `agent/run_loop` instead of doing a single LLM call.

3. `session/IsStreaming` relation: set true at loop start, false at end. The command bar shows a
   streaming indicator.

4. Revision bumps: after each tool execution and after the final assistant message,
   `ui/bump_revision(endpoint())` so the UI re-renders incrementally.

5. The loop runs as a single task submission. Since the LLM call is a single `ExternalRequest`
   suspend+resume (Stage 0 design), the loop naturally suspends during the LLM call and resumes to
   continue. Tool execution is synchronous within the task (no suspension needed for read-only
   source-provider relations).

**Done when:** the agent can make multiple tool calls in sequence and produce a final answer. The
transcript shows the full conversation including intermediate tool calls.

---

### Stage 4: Steering and follow-up

**Goal:** the user can interrupt the agent mid-loop (steer) or queue a message for after
(follow-up).

**Tasks:**

1. `PendingInput` relation keyed by session. `SteeringQueue` and `FollowUpQueue` as list-valued
   functional relations.

2. The command bar sync handler: if `session/IsStreaming`, route the message to `SteeringQueue`
   (shift+enter) or `FollowUpQueue` (enter).

3. The loop: at the top of each iteration, drain `SteeringQueue`. If non-empty, inject as user
   messages, skip remaining tools (mark as skipped), continue loop. After the loop would stop, drain
   `FollowUpQueue`; if non-empty, inject and continue.

4. UI: show pending steering/follow-up messages in the command bar area.

**Done when:** the user can type "stop, do X instead" while the agent is running tools and the agent
responds to the interruption.

---

### Stage 5: Write tools + bash + sandboxing

**Goal:** `edit`, `write`, `bash` tools with sandboxing and approvals.

This stage requires host effect builtins (file write, process spawn) that don't exist yet. It also
requires the `Approval` relation and sandbox-aware process spawning. This is the stage where codex's
`ToolOrchestrator` pattern becomes relevant.

**Vague outline:**

- Host effect builtins for file write and process spawn, gated by `CanEffect` + `CanInvoke` policy.
- `PermissionProfile` relations (`read_only`, `workspace_write`, `full_access`).
- `Approval` relation for interactive confirmation (sync event round trip).
- `ExecPolicyRule` relations for auto-approve/deny pattern matching.
- Sandbox host builtin (landlock on Linux) — may require a new `SandboxManager` in the driver.
- `edit` tool: read `source/FileText`, apply find-and-replace, write via host effect.
- `bash` tool: spawn process via host effect, stream stdout/stderr into `ToolResultContent` via
  revision bumps.

---

### Stage 6: Compaction + branching

**Goal:** context-window management for long sessions.

**Vague outline:**

- `agent/compact(transcript, phase)` verb: summarize older messages via `llm/chat`, record
  `Compaction` facts, insert compaction-summary message.
- `CompactionWindowId`/`CompactionPreviousWindow`/`CompactionFirstWindow` for the context-window
  chain.
- `agent/transform_context(transcript)` derived view: replace pre-compaction messages with the
  summary.
- Mid-turn compaction (codex pattern): check token budget after each sampling request, compact if
  needed.
- `agent/branch(transcript, from_seq)` for forking.
- Token-counting host builtin for compaction triggers.

---

### Stage 7: System prompt assembly + skills + hooks

**Goal:** configurable system prompts, skills, and lifecycle hooks.

**Vague outline:**

- `agent/system_prompt(agent, workspace)` verb assembling from
  `Skill`/`ContextFile`/`ToolEnabled`/`ToolSnippet` relations.
- `Skill` relations loaded from `.mica` skill files or AGENTS.md.
- Slash commands (`/compact`, `/branch`, `/model`, `/clear`, `/skill:name`).
- `Hook` relations with `HookEvent` + `hook/run` verb.
- Stop hooks (codex pattern): block the agent from stopping, force another iteration.

---

### Stage 8: Multi-agent threading

**Goal:** spawn sub-agents for exploration, review, etc.

**Vague outline:**

- `agent/spawn(parent_agent, role)` creating a new `Agent` + `Transcript` with `BranchParent`.
- `AgentStatus` functional relation.
- `InterAgentCommunication` as `message/agent_message` frobs.
- Parent awaits child completion via task suspension + `AgentStatus` polling or mailbox.
- Built-in roles: `explorer`, `reviewer` (codex's Guardian pattern).

---

### Stage 9: MCP integration

**Goal:** MCP server connections as a runtime tool source.

**Vague outline:**

- `McpServer`/`McpTool` relations.
- Host MCP connection manager builtin.
- MCP tools routed through `tool/invoke` dispatch.
- MCP resources and elicitation as sync events.

---

### Stage 10: Multi-user session scoping

**Goal:** per-actor transcripts, workspace sharing, role-gated tools.

**Vague outline:**

- Per-actor `Transcript` via `TranscriptAgent`.
- Shared transcripts via `TranscriptWorkspace` + `CanRead` policy.
- Role-gated tool surfaces (`RoleCanInvoke(#reader, :tool/bash)` = false).
- Per-session authority from `session/Actor` at task boundaries.

---

## 13. Stage 0 Detail: Streaming SSE Parser

Since Stage 0 is the immediate focus, here is the detailed design for the SSE parser and host
handler.

### 13.1 SSE format

OpenRouter (and OpenAI) streaming responses are SSE: lines prefixed with `data:`, separated by
`\n\n`. Each `data:` line contains a JSON object with `choices[0].delta`. The stream ends with
`data: [DONE]`.

Delta shapes:

```json
{"choices":[{"delta":{"content":"Hello"}}]}
{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1",
  "function":{"name":"read","arguments":""}}]}}]}
{"choices":[{"delta":{"tool_calls":[{"index":0,
  "function":{"arguments":"{\"path\":"}}]}}]}
{"choices":[{"delta":{},"finish_reason":"stop"}]}
{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}
{"usage":{"prompt_tokens":10,"completion_tokens":5}}
data: [DONE]
```

### 13.2 Accumulator struct

```rust
struct StreamAccumulator {
    text: String,
    tool_calls: Vec<ToolCallAccumulator>,
    finish_reason: Option<String>,
    usage: Option<serde_json::Value>,
    model: Option<String>,
}

struct ToolCallAccumulator {
    index: usize,
    id: String,
    name: String,
    arguments: String,
}
```

### 13.3 Handler flow

```
perform_llm_stream_request(spec) -> Result<Value, String>
  set stream: true in request body
  POST via cyper
  read response body as stream (cyper supports streaming)
  for each chunk in SSE stream:
    parse data: line as JSON
    if [DONE]: break
    extract delta
    if delta.content: accumulator.text.push_str(content)
    if delta.tool_calls: for each, accumulate by index
    if finish_reason: accumulator.finish_reason = reason
    if usage: accumulator.usage = usage
  return Value::map({
    :text -> accumulator.text,
    :tool_calls -> [map(:id, :name, :arguments) for each],
    :usage -> accumulator.usage,
    :stop_reason -> accumulator.finish_reason,
    :model -> spec.model,
    :provider -> "openrouter"
  })
```

### 13.4 Cyper streaming

`cyper::Client` supports streaming response bodies via `Response::body()` which returns an `async`
reader. Read in a loop, split on `\n\n`, parse each `data:` line. This is the same pattern as
codex's `client.rs` stream handling, adapted to compio/cyper.

### 13.5 Error handling

- HTTP non-2xx: read body, return error string.
- Stream parse error: return partial accumulator + error.
- Timeout: the existing `ExternalRequest` timeout mechanism applies.
- Rate limit (429): return error with rate-limit info; the agent loop (Stage 3) handles retry.

### 13.6 Test fixtures

Unit-test the SSE parser with:

- A plain text completion stream (multi-chunk content).
- A tool-call completion stream (multi-chunk tool-call arguments).
- A stream with `usage` in the final chunk.
- A stream that ends immediately with `[DONE]` (empty response).
- A stream with an error mid-stream.

### 13.7 OpenRouter model selection

For development, use cheap/free models:

- `deepseek/deepseek-chat-v3.1:free` — free tier, good for testing.
- `google/gemini-2.5-flash-preview-05-20` — cheap, fast.
- `meta-llama/llama-3.3-70b-instruct` — cheap, capable.

Set via `AgentModel` relation or `MICA_AGENT_MODEL` env var. The `scripts/agent.sh` wrapper can
default `MICA_AGENT_MODEL` to a cheap model if unset.
