# Mica Agent

`apps/agent/` is a relation-first LLM coding agent shell. It is the application skeleton for a
web-driven, shareable coding agent in the spirit of OpenCode, Claude Code, and Junie: a command
input, a transcript, an object inspector, and source fragments, all authored as Mica relations and
verbs rather than a separate client application.

The agent is wired to an OpenRouter LLM. Submitting a command appends a user message, runs the agent
loop (LLM call, tool execution, repeat), and appends assistant and tool-result messages to the
transcript. Read-only tools (`read`, `grep`, `find`, `ls`) query the source-provider crate's
computed relations. The source-provider also exposes syntax, symbol, definition, references, and VCS
history as computed relations for future tools.

## What It Demonstrates

- Durable identities described by relation facts: workspaces, agents, transcripts, messages, tool
  calls, tool results, and inspector targets.
- Prototype/delegation dispatch through `Delegates`, including UI sync action frobs and
  role-dispatched message rendering.
- Recursive and derived relations for transcript membership and message order.
- Server-owned DOM rendering through `sync_view_dependencies`, `sync_view_tree`, and `sync_event`,
  reusing the same sync contract as the MUD app.
- Browser UI composition written mostly in Mica, with a small JavaScript bootstrap handling the
  column splitter and tool-window close affordances.
- Browser-originated actions routed through generic sync events, then dispatched inside Mica through
  action frobs.
- Authority derived from relation policy into per-task runtime checks.
- Tool calls and results as first-class durable facts (`ToolCall`, `ToolResult` relations) that can
  be inspected, replayed, and audited.
- Agent loop as a Mica verb (`agent/run_loop`) that suspends and resumes across async LLM calls via
  the runtime's task suspension model.

## Fileins

- `core.mica`: workspace, agent, transcript, message, and inspector target identities and relations,
  plus the policy relation declarations and accessor verbs.
- `workspaces.mica`: binds `Workspace`/`WorkspaceRoot`/`source/Repository` from `MICA_SOURCE_ROOTS`
  so tools have a real repository to read.
- `tools.mica`: `Tool`/`ToolCall`/`ToolResult` relations, read-only tool verbs (`read`, `grep`,
  `find`, `ls`), `agent/run_loop`, and LLM message assembly.
- `transcript.mica`: transcript and message DOM composition, including a bounded recent window,
  opt-in scrollback loading, tool-call and tool-result rendering, and a typing indicator while the
  agent loop is running.
- `ui-session.mica`: sync view selection, session facts (including `session/IsStreaming`), agent
  sync action declarations, authority grants, and `sync_view_dependencies` / `sync_view_tree`.
- `ui-compose.mica`: workspace panel, object browser, inspector, command strip with streaming
  indicator, and shell DOM composition.
- `ui-actions.mica`: browser sync event routing and delegated sync action handlers. The
  `agent_command` handler calls `agent/run_loop`.
- `http.mica`: `/agent` HTTP document route and transport-neutral sync mount.
- `style.css`: text asset loaded by `http.mica` with `include_text(...)`.
- `bootstrap.js`: browser boot script for the server-rendered sync client.

## Run The Browser Fixture

```sh
scripts/agent.sh
```

The wrapper starts `mica-daemon` with the agent filein set and points `MICA_SOURCE_ROOTS` at the
repository root so the source-provider computed relations can see the workspace. Open the printed
`/agent` URL in a browser.

Set `OPENROUTER_API_KEY` in the environment for LLM access. The default model is
`deepseek/deepseek-v4-pro`; override with `MICA_AGENT_MODEL`.

Without auth enabled, the host renders the workspace view directly. Set `MICA_AUTH_LOCAL_PASSWORD=1`
or `MICA_AUTH_GITHUB_CLIENT_ID` to require sign-in first; the shell currently renders a sign-in link
rather than a login form, so leave auth off for the shell demo.

## UI Shape

The current browser UI separates transcript state from available tools:

- The left column holds the transcript panel (message log with role glyphs, tool-call blocks,
  tool-result blocks, and a typing indicator while the agent loop is running).
- The right column holds the workspace panel, the object browser tool window, and the inspector.
- A command strip near the input exposes context actions derived from the current selection or the
  bound workspace.
- The command input sends `agent_command` sync events; the agent appends a user message and runs the
  agent loop, which appends assistant and tool-result messages as it proceeds.

## Design Boundaries

Keep app semantics in Mica source. Host/client support stays generic: browser attributes declare
sync behaviours, while agent-specific meanings such as message roles and inspector targets are
implemented by Mica verbs and relations. The agent app reuses the shared sync-host and sync-dom
fileins and does not duplicate the sync contract.

## Next Steps

- Steering and follow-up queues so the user can interrupt or queue messages during the agent loop.
- Write tools (`edit`, `write`, `bash`) with sandboxing and approvals.
- Compaction and branching for context-window management.
- System prompt assembly from skills, context files, and tool snippets.
- Multi-agent threading with sub-agent spawn and inter-agent communication.
