# Mica Agent

`apps/agent/` is a relation-first LLM coding agent shell. It is the application
skeleton for a web-driven, shareable coding agent in the spirit of OpenCode,
Claude Code, and Junie: a command input, a transcript, an object inspector, and
source fragments, all authored as Mica relations and verbs rather than a
separate client application.

The current state is the shell. The transcript records user and assistant
messages durably, the inspector surfaces workspace and target objects, and the
command bar routes sync events back into Mica verbs. There is no LLM driver yet:
submitting a command echoes a placeholder assistant message so the round trip is
exercisable end to end. The source-provider crate already exposes repository
entry, file text, syntax, symbol, definition, references, and VCS history as
computed relations, which the agent will query once it drives an LLM.

## What It Demonstrates

- Durable identities described by relation facts: workspaces, agents,
  transcripts, messages, and inspector targets.
- Prototype/delegation dispatch through `Delegates`, including UI sync action
  frobs.
- Recursive and derived relations for transcript membership and message order.
- Server-owned DOM rendering through `sync_view_revision`, `sync_view_tree`,
  and `sync_event`, reusing the same sync contract as the MUD app.
- Browser UI composition written mostly in Mica, with a small JavaScript
  bootstrap handling the column splitter and tool-window close affordances.
- Browser-originated actions routed through generic sync events, then
  dispatched inside Mica through action frobs.
- Authority derived from relation policy into per-task runtime checks.

## Fileins

- `core.mica`: workspace, agent, transcript, message, and inspector target
  identities and relations, plus the policy relation declarations and accessor
  verbs.
- `transcript.mica`: transcript and message DOM composition, including a
  bounded recent window and opt-in scrollback loading.
- `ui-session.mica`: sync view selection, session facts, agent sync action
  declarations, authority grants, and `sync_view_revision` / `sync_view_tree`.
- `ui-compose.mica`: workspace panel, object browser, inspector, command strip,
  and shell DOM composition.
- `ui-actions.mica`: browser sync event routing and delegated sync action
  handlers.
- `http.mica`: `/agent` HTTP document route and transport-neutral sync mount.
- `style.css`: text asset loaded by `http.mica` with `include_text(...)`.
- `bootstrap.js`: browser boot script for the server-rendered sync client.

## Run The Browser Fixture

```sh
scripts/agent.sh
```

The wrapper starts `mica-daemon` with the agent filein set and points
`MICA_SOURCE_ROOTS` at the repository root so the source-provider computed
relations can see the workspace. Open the printed `/agent` URL in a browser.

Without auth enabled, the host renders the workspace view directly. Set
`MICA_AUTH_LOCAL_PASSWORD=1` or `MICA_AUTH_GITHUB_CLIENT_ID` to require sign-in
first; the shell currently renders a sign-in link rather than a login form, so
leave auth off for the shell demo.

## UI Shape

The current browser UI separates transcript state from available tools:

- The left column holds the transcript panel (message log with role glyphs and
  a bounded recent window).
- The right column holds the workspace panel, the object browser tool window,
  and the inspector.
- A command strip near the input exposes context actions derived from the
  current selection or the bound workspace.
- The command input sends `agent_command` sync events; the agent appends a user
  message and a placeholder assistant acknowledgement to the transcript.

## Design Boundaries

Keep app semantics in Mica source. Host/client support stays generic: browser
attributes declare sync behaviours, while agent-specific meanings such as
message roles and inspector targets are implemented by Mica verbs and
relations. The agent app reuses the shared sync-host and sync-dom fileins and
does not duplicate the sync contract.

## Next Steps

- Bind `Workspace`/`WorkspaceRoot` facts from `MICA_SOURCE_ROOTS` at startup so
  the workspace panel lists real repositories.
- Replace the placeholder assistant acknowledgement with an LLM call routed
  through the OpenAI shared filein.
- Render source fragments, diffs, and tasks as inspector targets backed by
  source-provider computed relations.
- Add tool-call and tool-result message roles with structured payloads.