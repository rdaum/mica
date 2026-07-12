# Mica MUD

`apps/mud/` contains a small multi-user room/object world. It is useful as a playable example, but
its purpose is broader than being a game demo: it exercises the current relation-first application
model across the runtime, driver, HTTP host, WebTransport host, and browser DOM sync client. One of
its central goals is to show browser UI authored mostly in Mica source, with live DOM sync driving
the page and only sidecar CSS plus a small JavaScript bootstrap outside Mica.

The world model is ordinary Mica source. Rooms, exits, containment, visibility, names, descriptions,
command grammar, event delivery, and UI session state are relations and verbs, not privileged
runtime records.

Likewise, the UI itself is ordinary Mica source rather than a separate client application. UI verbs
query world, session, and event relations, then return DOM node values. Mica exposes a view revision
and tree to the host; the host diffs that tree and sends patches to the browser. Browser
interactions come back as declared sync events, which Mica handlers turn into relation updates and
the next rendered revision.

Together, the world model, command surface, event log, authority policy, and browser UI demonstrate
a unified declarative, data-oriented application design.

## What It Demonstrates

- Durable identities described by relation facts: rooms, players, things, containers, exits, names,
  and descriptions.
- Prototype/delegation dispatch through `Delegates`, including event and UI action frobs.
- Recursive and derived relations for containment, visibility, contents, and present actors.
- A parser-backed command surface for text commands such as `look`, `north`, `get coin`,
  `drop coin`, `put coin box`, `push button`, and `say hello`.
- Transactional command execution and routed effects over telnet endpoints.
- Server-owned DOM rendering through `sync_view_revision`, `sync_view_tree`, and `sync_event`.
- Browser UI composition written mostly in Mica, with live DOM sync applying server-rendered updates
  in the browser.
- Browser-originated actions routed through generic sync events, then dispatched inside Mica through
  action frobs.
- Narrative/event rendering from durable event source frobs, including a bounded recent window and
  opt-in scrollback loading.
- Authority derived from relation policy into per-task runtime checks.

## Fileins

- `core.mica`: world identities, relations, parser grammar, role policy, and command verbs.
- `auth.mica`: MUD auth/session relations, pre-auth login actor policy, and default local-user
  mappings for Alice and Bob.
- `command-parser.mica`: parser support for turning command text into command invocations.
- `event-substitutions.mica`: event text/template rendering support.
- `ui-session.mica`: sync view selection, session facts, MUD sync action declarations, and
  web/player authority grants.
- `ui-mica-inspect.mica`: programmer-facing Mica reflection inspector, including reflection grants,
  inspect navigation, layout controls, and method catalogue rendering.
- `ui-compose.mica`: room, inventory, presence, examine panel, command strip, and login DOM
  composition.
- `ui-narrative.mica`: event/narrative DOM composition and event-source-specific rendering.
- `ui-actions.mica`: browser sync event routing and delegated sync action handlers.
- `http.mica`: `/mud` HTTP document route and transport-neutral sync mount.
- `style.css`, `login.css`, `presence.css`, `narrative.css`: text assets loaded by `http.mica` with
  `include_text(...)`.
- `bootstrap.js`: browser boot script for the server-rendered sync client.

## Telnet Fixture

Load the shared string/event support, the MUD core, event substitutions, and the command parser for
a telnet-oriented MUD session:

```sh
cargo run --bin mica-daemon -- \
  --filein apps/shared/string.mica \
  --filein apps/shared/events.mica \
  --filein apps/mud/core.mica \
  --filein apps/mud/auth.mica \
  --filein apps/mud/event-substitutions.mica \
  --filein apps/mud/command-parser.mica \
  --telnet-bind 127.0.0.1:7777
```

Then connect with a telnet client and try commands such as:

```text
look
get coin
north
say hello
put coin box
push button
```

## SSE DOM Sync Fixture

Run the browser fixture with a plain HTTP host:

```sh
cargo run --bin mica-daemon -- \
  --filein apps/shared/sync-host.mica \
  --filein apps/shared/string.mica \
  --filein apps/shared/events.mica \
  --filein apps/mud/core.mica \
  --filein apps/mud/event-substitutions.mica \
  --filein apps/mud/command-parser.mica \
  --filein apps/shared/sync-dom.mica \
  --filein apps/mud/ui-session.mica \
  --filein apps/mud/ui-mica-inspect.mica \
  --filein apps/mud/ui-compose.mica \
  --filein apps/mud/ui-narrative.mica \
  --filein apps/mud/ui-actions.mica \
  --filein apps/mud/http.mica \
  --web-bind 127.0.0.1:8080
```

Open `http://127.0.0.1:8080/mud`. When auth is enabled, the host redirects to `/auth/login` first.
After username/password or GitHub OAuth sign-in, the authenticated actor drives the server-owned
world view without app code caring whether the transport is SSE or WebTransport.

## WebTransport DOM Sync Fixture

Run the browser fixture with:

```sh
scripts/mud.sh
```

The wrapper starts `mica-daemon` with the explicit sync filein set needed by the browser app: sync
host support, shared string/event libraries, the MUD world and parser, auth/session relations, DOM
sync support, MUD UI/session/action fileins, and the `/mud` HTTP document route.

Open the printed `/mud` URL. Local password auth is enabled by default with seeded users:

```text
alice / alice-pass
bob / bob-pass
```

To force the shared bootstrap onto WebTransport, open the printed URL with `transport=webtransport`,
the WebTransport `url`, and the certificate hash:

```text
/mud?transport=webtransport&url=https://127.0.0.1:4433/view&certHash=...
```

After sign-in, the browser view is driven by the same DOM sync contract over WebTransport: Mica
renders the DOM tree, the host diffs it, and the browser applies patches.

For GitHub OAuth, set the current Mica auth variables and use:

```sh
export MICA_AUTH_GITHUB_CLIENT_ID=...
export MICA_AUTH_GITHUB_CLIENT_SECRET=...
export MICA_AUTH_ADMIN_GITHUB_LOGIN=your-login
export MICA_AUTH_GITHUB_ALLOWED_LOGINS=your-login
scripts/mud-github-auth.sh
```

The smoke wrapper is quiet by default. Set `MICA_MUD_SMOKE_TRACE=1` to enable sync, driver, task,
and VM host tracing. Set `MICA_WT_POLL_MS=0` to disable the browser polling loop during manual
inspection.

## UI Shape

The current browser UI separates room state from available actions:

- The room panel shows room title, description, dynamic exits, and compact room contents.
- Clicking a room entity selects it for the `Examine` panel.
- The `Examine` panel owns detail and object-specific actions.
- A thumb-friendly command strip near the input exposes context actions derived from the current
  room or examined entity.
- The narrative panel is scrollable and can request older events through a declared viewport sync
  event.

## Design Boundaries

Keep app semantics in Mica source. Host/client support should stay generic: browser attributes
declare sync behaviours such as viewport-top events or scroll stability, while MUD-specific meanings
such as loading older narrative events are implemented by Mica verbs and relations.
