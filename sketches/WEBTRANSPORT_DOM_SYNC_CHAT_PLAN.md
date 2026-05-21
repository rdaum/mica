# WebTransport DOM Sync And Chat Plan

This plan starts from the current WebTransport sync foundation and moves toward
a simple multiuser chat. The near-term goal is to stop treating the snapshot
payload as an opaque Mica display string and give the browser a stable DOM sync
target. Deltas, richer identity, and recovery come after the full snapshot path
is real.

## Phase 1: Stable Snapshot Payload

Define a canonical snapshot payload format.

- Use JSON first, not Mica display strings.
- The shape should represent a tiny DOM tree plus metadata.

```json
{
  "view": 11,
  "revision": 20,
  "root": {
    "id": "chat-root",
    "tag": "main",
    "children": []
  }
}
```

Add host and browser tests for payload expectations.

- Mica emits `[:view_snapshot, ... payload]`.
- The host sends the payload unchanged.
- The browser decodes it and validates revision/signature.

Commit: `Define canonical sync snapshot payload`.

## Phase 2: Browser Sync Client

Move protocol JavaScript out of `browser-smoke.html` into a small module.

The module should own:

- encoding and decoding sync envelopes
- WebTransport connection setup
- sending `NeedView`
- sending `HaveView`
- receiving `ViewSnapshot` and `ViewDelta`

Keep `browser-smoke.html` as a fixture using the module.

Add a Playwright smoke check that asserts DOM state, not just datagram receipt.

Commit: `Add browser sync client fixture`.

## Phase 3: DOM Apply

Implement full snapshot DOM replacement.

Given a snapshot root node, render it into a mount point. The first supported
surface should be deliberately small:

- element tag
- text node
- attributes, limited to `id` and `class` if attributes are needed
- children

Add a browser smoke expectation:

- after `NeedView`, the page contains rendered server state.

Commit: `Apply sync snapshots to browser DOM`.

## Phase 4: Chat Data Model

Replace the generic view example with chat relations.

```text
ChatRoom(room)
ChatMessage(room, message)
MessageSeq(message, seq)
MessageAuthor(message, actor)
MessageText(message, text)
ChatView(view, room)
```

`sync_need_view` should build a chat DOM snapshot:

- message list
- each message row
- author/text
- optionally a composer placeholder

The snapshot signature should be based on latest message sequence plus payload
length/hash.

Commit: `Model chat room view snapshots`.

## Phase 5: Browser Chat Input

Define a client-to-server action path.

This can become a new sync envelope kind later. Initially, ordinary endpoint
input is enough if it keeps the first chat slice small and direct.

Minimal first shape:

```json
{"type": "chat_post", "room": 1, "text": "hello"}
```

Add a Mica verb for posting:

```mica
verb chat_post(endpoint, room, text)
```

The verb creates message identity/facts and advances room revision.

The browser form sends chat posts over WebTransport.

Commit: `Submit chat messages over WebTransport`.

## Phase 6: Push Updates

Use the host's active session/view registry.

- `endpoint -> session -> active views` already exists.
- Add a path for Mica/server code to emit fresh snapshots to subscribed views.

The first version can be crude:

- after `chat_post`, emit a full `ViewSnapshot` to every active session for that
  view.
- do not add deltas yet.

Multiuser proof:

- two browser sessions connect to the same room.
- session A posts.
- session B receives an updated snapshot.

Commit: `Push chat snapshots to active views`.

## Phase 7: Multiuser Identity

Assign each WebTransport session an actor/principal.

- Start with configured/demo actor names.
- Later wire in real auth/session claims.

Render author names from Mica state.

Add a smoke check with two actors posting distinct messages.

Commit: `Track chat authors across sessions`.

## Phase 8: Deltas

Add `ViewDelta` for append-only chat messages.

- If client revision matches the previous room revision, send an append delta.
- Otherwise send a full snapshot.

The browser applies deltas by:

- appending the message
- updating revision/signature
- requesting a full snapshot if signature validation fails

Commit: `Add append deltas for chat views`.

## Phase 9: Hardening

Handle backpressure and loss.

- Datagrams may drop, so the client periodically sends `HaveView`.
- The server answers with the latest snapshot/delta depending on revision.
- Missing deltas fall back to a full snapshot.

Add disconnect cleanup.

- Remove endpoint session state.
- Remove active view subscriptions.

Add tests.

- two browser tabs
- dropped/late `HaveView`
- stale revision gets a snapshot
- current revision gets a no-op or nothing

Commit: `Harden chat sync recovery`.

## Next Step

Start with Phase 1: canonical JSON snapshot payload. That removes the current
Mica display string shortcut and gives both DOM sync and chat a stable target.
