# Mica Apps

`apps/` holds runnable application fileins plus shared support fileins:

- `shared/`: reusable libraries and host policy used by more than one app.
- `mud/`: room/object, event rendering, and command parser fileins.
- `web/`: HTTP host handlers and relational route fileins.
- `chat/`: WebTransport DOM sync chat fileins.

The daemon's default fileins load the shared string and event libraries, the
MUD room/object model, the MUD command parser, and the minimal HTTP handler.
Commands such as `look`, `get coin`, `put coin box`, `north`, and `say hello`
can exercise telnet endpoint input and routed effects.

Try the MUD example over telnet:

```sh
cargo run --bin mica-daemon -- --telnet-bind 127.0.0.1:7777
```

`shared/capabilities.mica` shows the intended bootstrap shape for capabilities. It
declares `Name` as a functional binary relation, then describes policy through
roles and surfaces. Derived `CanRead`, `CanWrite`, `CanInvoke`, and `CanEffect`
relations are the effective policy consumed by the runner. Those facts are not
live capability values. When source is run with `--actor`, the runner resolves
the actor identity, reads the effective policy, and mints ephemeral task
capabilities for that run.

Try the capability example with a persistent store:

```sh
cargo run --bin mica -- --storage fjall --store demo-db filein --unit caps --replace apps/shared/capabilities.mica
cargo run --bin mica -- --storage fjall --store demo-db --actor alice eval ':polish(actor: #alice, item: #lamp)'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval 'return #lamp.name'
cargo run --bin mica -- --storage fjall --store demo-db --actor bob eval '#lamp.name = "stolen"'
```

The first actor invocation succeeds and emits an effect because Alice has the
`#builder` role, which can inspect and edit `Name`, invoke the `:maintenance`
surface, and emit effects. Bob has the `#visitor` role, so he can read the lamp
name, but the write attempt is denied.

`web/http-core.mica` is the minimal default HTTP filein. `web/relational-router.mica`
shows the same web-host request facts routed through relations and stratified
negation. It keeps route matching, access policy, forbidden responses, and
not-found fallback in Mica source. Both fileins define `#web` as the default
HTTP principal and grant it only the read/invoke authority needed to handle
requests.

Run the router demo with an explicit filein list so it replaces the default
HTTP filein:

```sh
cargo run --bin mica-daemon -- \
  --filein apps/shared/string.mica \
  --filein apps/shared/events.mica \
  --filein apps/mud/core.mica \
  --filein apps/mud/event-substitutions.mica \
  --filein apps/mud/command-parser.mica \
  --filein apps/web/relational-router.mica \
  --web-bind 127.0.0.1:8080
curl -i http://127.0.0.1:8080/hello
curl -i http://127.0.0.1:8080/admin
curl -i http://127.0.0.1:8080/missing
```

Run the first DOM-synced MUD web fixture:

```sh
scripts/mud-webtransport-smoke.sh
```

Open the printed `/mud` URL in a browser. The initial document is a
server-rendered login view; after entering the demo actor, WebTransport DOM sync
replaces it with the server-owned room view and command input.
