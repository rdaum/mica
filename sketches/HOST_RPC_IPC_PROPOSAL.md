# Host RPC And IPC Handler Proposal

This note proposes the first real Mica host/daemon RPC shape. The intent is to
keep the transport small and stable while avoiding the large schema surface and
value-conversion burden that grew around mooR's daemon protocol.

The short version:

- use `mica-host-protocol` frames as the language-neutral payload;
- use ZeroMQ as the first carrier for IPC and TCP;
- keep IPC and TCP semantically identical above the carrier;
- use CURVE/ZAP only for TCP peer authentication;
- treat IPC as locally trusted by OS boundary and socket path permissions;
- keep Mica authority separate from transport authentication;
- avoid baking player, connection, channel, or presence semantics into the
  daemon.

## Goals

The handler should support:

- out-of-process hosts for telnet, web, tools, agents, and future protocols;
- same-host IPC without requiring a TCP listener;
- remote TCP hosts when explicitly configured;
- non-Rust clients and hosts;
- bounded output delivery without assuming every consumer wants FIFO-only
  semantics;
- Mica-level authorization through endpoint authority contexts;
- fast value transport using Mica's own value codec and borrowed frame payloads
  where possible.

The handler should not become:

- a general object editing API;
- a world-state CRUD schema;
- a MOO connection compatibility layer;
- a second command parser;
- a place where durable capabilities or live authority tokens are stored.

## Layering

The proposed stack is:

```text
host process or tool
  ZeroMQ carrier: ipc:// or tcp://
  Mica Host Protocol frames
  daemon RPC handler
  driver/runtime task manager
  relation policy -> AuthorityContext
  Mica VM and relation kernel
```

The transport moves frames. The RPC handler validates framing, endpoint state,
request ordering, and authority. The runtime executes Mica tasks. The relation
store remains the source of durable policy.

## Transport Shape

Mica should use the same protocol messages over IPC and TCP.

### IPC

IPC endpoints use a local transport such as:

```text
ipc:///run/mica/<world>/rpc
ipc:///run/mica/<world>/events
```

Under IPC there is no CURVE/ZAP authentication layer. IPC access is controlled
by local operating-system boundaries: socket path ownership, file permissions,
service manager configuration, containers, jails, user ids, and group ids.

That means an IPC peer is not cryptographically authenticated by Mica's
transport. If a process can connect to the IPC socket, the daemon treats it as a
local peer admitted by the host environment. This is acceptable for same-machine
host processes, but it is not a substitute for Mica-level authorization.

IPC bypasses only transport peer authentication. It does not bypass endpoint
authority, capability validation, relation policy, or runtime permission checks.

### TCP

TCP endpoints use:

```text
tcp://<addr>:<port>
```

TCP should enable ZeroMQ CURVE for peer authentication and use ZAP to decide
whether a remote peer key is allowed to connect. TCP peers need an enrolment
story: generated host keys, allowed peer keys, and operator-controlled key
rotation.

TCP transport authentication answers "is this process allowed to connect to the
daemon transport?" It still does not answer "what can this endpoint do inside
the Mica world?"

## Socket Topology

The first topology should have two channels:

```text
RPC channel:
  host DEALER -> daemon ROUTER

event channel:
  daemon PUB -> host SUB
```

The RPC channel handles immediate requests and replies:

- hello and feature negotiation;
- opening and closing endpoints;
- submitting source or input;
- draining output;
- heartbeat and shutdown messages.

The event channel notifies hosts that something has changed:

- output is ready for an endpoint;
- a task completed or failed;
- an endpoint was closed;
- the daemon is draining or shutting down.

Events should usually be hints, not payload dumps. A host can receive an
`OutputReady` event and then use `DrainOutput` to request a bounded batch. This
keeps receivers in control of backlog, batching, coalescing, and overload
policy.

Strict REQ/REP is too narrow for this shape because task output and completion
are asynchronous. ROUTER/DEALER also gives the daemon an explicit peer identity
without forcing a thread per peer.

## rust-zmq And compio Integration

The current `rdaum/rust-zmq` fork is still a binding over `libzmq`, not a
native compio transport. That is useful because it preserves real ZeroMQ
transport behaviour, IPC, TCP, CURVE, and ZAP, but it means Mica should not try
to pretend a ZeroMQ socket is the same kind of object as a `compio::net`
socket.

The integration should be a small adapter layer with an explicit contract:

```rust
trait HostTransport {
    async fn recv(&self) -> Result<InboundFrame, TransportError>;
    async fn send(&self, peer: PeerId, frame: OutboundFrame<'_>) -> Result<(), TransportError>;
}
```

The ZeroMQ implementation can then be one transport implementation, not a
runtime dependency leaked through the daemon and host protocol crates.

### Readiness Path

The fork exposes `AsFd`/`AsSocket` for `zmq::Socket`, and `libzmq` exposes
`ZMQ_FD` plus `ZMQ_EVENTS`. Compio exposes readiness waiting through
`compio::runtime::fd::PollFd`.

That suggests a workable Linux/Unix path:

```text
zmq::Socket
  -> AsFd / ZMQ_FD
  -> compio::runtime::fd::PollFd
  -> read_ready().await
  -> socket.get_events()
  -> recv_multipart(DONTWAIT) until EAGAIN
```

The important detail is that `ZMQ_FD` is not the underlying network socket.
It is a ZeroMQ signalling descriptor. It should only be used to wait for
readiness. After wakeup, code must ask ZeroMQ for `ZMQ_EVENTS` and then drain
with `DONTWAIT` until `EAGAIN`.

Writes should use the same pattern when a send would block:

```text
send_multipart(DONTWAIT)
  ok       -> done
  EAGAIN   -> wait for writable readiness, then retry
  other    -> transport error
```

This keeps the compio event loop from blocking on `recv` or `send`, while still
letting `libzmq` own its internal I/O threads and socket state.

### Thread Ownership

ZeroMQ sockets are not ordinary shared async resources. Even if the Rust wrapper
allows moving a socket between threads, each socket should have one logical
owner task. The adapter should run one compio task per ZeroMQ socket or socket
pair and communicate with the rest of the daemon through bounded internal
queues or explicit async calls.

For the daemon side:

```text
ZmqRpcEndpoint task
  owns ROUTER socket
  receives multipart messages
  decodes MHP frames
  forwards requests to RpcHandler
  serializes replies back to peers

ZmqEventEndpoint task
  owns PUB socket
  receives daemon events over an internal queue
  publishes MHP event frames
```

This avoids exposing `zmq::Socket` to request handlers and keeps shutdown,
linger, peer identity, and error handling in one place.

### Multipart Framing

Mica should not nest arbitrary ZeroMQ message structure into the host protocol.
Use ZeroMQ multipart only for routing envelopes and payload separation.

For ROUTER/DEALER RPC:

```text
ROUTER receives:
  peer routing id
  empty delimiter, if used by selected socket pattern
  MHP frame bytes

ROUTER sends:
  peer routing id
  empty delimiter, if used by selected socket pattern
  MHP frame bytes
```

For PUB/SUB events:

```text
topic frame:
  endpoint id, peer id, or event class

payload frame:
  MHP frame bytes
```

MHP remains the semantic payload. ZeroMQ frames are carrier structure.

### Borrowed Output

`mica-host-protocol` already has segmented frame encoding for borrowed payloads.
The `zmq` crate's send path is still a `libzmq` message send, so the first
adapter does not need to force perfect zero-copy. The practical target is:

- avoid decoding Mica values only to re-encode them at the transport boundary;
- preserve borrowed MHP frame segments until the final ZeroMQ message build;
- use multipart sends to avoid flattening routing envelope and payload
  together;
- measure before adding custom `zmq::Message` constructors or unsafe borrowed
  message paths.

If `libzmq` copies each message into its own queue, that may still be acceptable
for the first carrier. The larger design win is keeping Mica values already in
wire-ready form.

### Blocking Fallback

If readiness integration proves fragile, the fallback should be isolated:

```text
one OS thread per ZeroMQ endpoint group
  blocking zmq_poll()
  bounded channel into the compio daemon
  compio event handle for wakeups
```

That is less elegant, but still better than letting arbitrary daemon code call
blocking `zmq::recv` or `zmq::send`. It also keeps the fallback replaceable if a
future pure-Rust or sans-I/O ZMTP implementation becomes attractive.

### Shutdown Rules

Every ZeroMQ socket owned by Mica should be configured deliberately:

- set finite or zero linger for daemon shutdown paths;
- set high-water marks explicitly;
- set handshake and TCP reconnect timeouts for TCP;
- set CURVE and ZAP options only on TCP endpoints;
- leave IPC without CURVE/ZAP and rely on socket path permissions;
- avoid mutating process environment after the first ZeroMQ context is created.

The adapter should provide one shutdown path that stops receiving new requests,
flushes or drops pending replies according to configured mode, closes sockets,
and terminates the context without indefinite linger.

## Endpoint Model

The daemon should not model players, sessions, channels, presence, or protocol
commands directly.

Instead, the transport layer works with endpoints:

```text
Endpoint id:       Mica identity
Endpoint protocol: :telnet, :web, :agent, :tool, ...
Endpoint peer:     transport peer id
Endpoint actor:    optional Mica actor identity after authentication
Endpoint state:    open, closing, closed
```

Endpoint ids should be first-class Mica identity values so that emitted output
and transient runtime facts can refer to them naturally. The daemon may keep a
fast in-memory endpoint table, but author-facing state should be representable
as transient relations where useful:

```mica
Endpoint(#endpoint42)
EndpointProtocol(#endpoint42, :telnet)
EndpointActor(#endpoint42, #alice)
EndpointPeer(#endpoint42, #peer17)
```

These facts are runtime state, not necessarily durable world facts. They are
useful for authorization, introspection, routing, and tests.

## Authority Model

There are three separate authority layers.

### Transport Admission

Transport admission decides whether a process can connect to the daemon.

- IPC: admitted by OS socket access. There is no CURVE/ZAP layer.
- TCP: admitted by CURVE key plus ZAP policy.

This layer should be small and mechanical.

### Grant Validation

Grant validation decides whether a connected peer can assume a Mica authority.
This is where PASETO tokens can be useful.

A PASETO token should represent a portable grant or bootstrap claim, not a live
runtime capability. Example claims:

```text
world id
issuer
subject actor
allowed endpoint purpose
issued-at time
expiry time
policy epoch
grant id
audience
```

The daemon validates the token, checks current durable policy facts and
revocation epoch, then builds an `AuthorityContext`.

### Runtime Authority

Runtime authority is what tasks actually use:

```text
durable policy relations -> effective Can* relations -> AuthorityContext
```

Runtime checks should be cheap checks against `AuthorityContext`, not fresh
policy queries on every relation read, write, invoke, or effect. Policy changes
take effect at task, endpoint, or session boundaries when authority is rebuilt
from a current snapshot.

Ephemeral capabilities may be represented as Mica values inside the runtime,
but live capabilities are not durable tokens and should not be serialized into
world state as bearer secrets.

## Current Host Protocol Gaps

`mica-host-protocol` already has frames and core message types:

- `Hello`, `HelloAck`;
- `OpenEndpoint`, `CloseEndpoint`;
- `SubmitSource`, `SubmitInput`;
- `OutputReady`, `DrainOutput`, `OutputBatch`;
- `TaskCompleted`, `TaskFailed`.

For a real RPC handler, it still needs request correlation and explicit
accepted/rejected replies.

The next protocol revision should add one of these shapes:

```text
Common request header:
  request_id u64
```

or:

```text
RequestAccepted:
  request_id u64
  optional task_id u64

RequestRejected:
  request_id u64
  code symbol
  message string
```

The first is cleaner: every request that expects a reply carries a request id,
and every reply echoes it. Task events may separately carry task ids and
endpoint ids.

Without correlation, a ROUTER/DEALER handler can still work for narrow demos,
but it becomes awkward once a host has multiple outstanding requests.

## Proposed Message Flow

### Startup

```text
host -> daemon: Hello
daemon -> host: HelloAck
```

The daemon checks protocol version and feature flags. On TCP, this happens only
after CURVE/ZAP transport admission. On IPC, this happens after the OS has
allowed the local socket connection.

### Endpoint Open

```text
host -> daemon: OpenEndpoint(endpoint, protocol, optional grant token)
daemon -> host: RequestAccepted(request, endpoint)
daemon:        builds endpoint table entry and initial AuthorityContext
```

If a grant token is provided, the daemon validates it against current policy.
If no grant token is provided, the endpoint starts with the minimal local-peer
authority configured for that transport.

For IPC, this minimal authority may be stronger than anonymous remote TCP
authority, but it should still be explicit configuration, not an implicit "root"
grant.

### Source Submission

```text
host -> daemon: SubmitSource(endpoint, actor, source)
daemon -> host: RequestAccepted(request, task)
daemon:        submits task with endpoint authority
daemon -> host: OutputReady(endpoint, buffered)
daemon -> host: TaskCompleted(task, value)
```

The submitted task's effects can append output to the endpoint buffer. The host
drains output when it is ready:

```text
host -> daemon: DrainOutput(endpoint, limit)
daemon -> host: OutputBatch(endpoint, values)
```

### Input Submission

```text
host -> daemon: SubmitInput(endpoint, value)
daemon -> host: RequestAccepted(request)
daemon:        resumes task or invokes configured input handler
```

Input is just a value. A telnet host can submit strings. A web host can submit
structured values. Parsing should be in Mica code where practical, not in the
transport.

## Output Buffers And Backpressure

The daemon should store output per endpoint as bounded vectors or chunks, not
as an unbounded FIFO that every consumer must drain one message at a time.

Each endpoint needs:

- a maximum buffered value count;
- a maximum buffered byte estimate;
- an overflow policy;
- a way to coalesce or drop low-priority output later if needed.

Initial overflow policy can be simple:

```text
if endpoint output exceeds limit:
  close endpoint or reject further output with E_OUTPUT_OVERFLOW
```

The important part is that `OutputReady` tells the host there is buffered
output, while `DrainOutput` lets the host decide how much to take.

## Error Handling

The handler should distinguish:

- malformed frame;
- unknown message type;
- unsupported protocol version;
- unauthorized transport peer;
- unauthorized endpoint action;
- stale endpoint;
- oversized request;
- runtime task failure;
- daemon overload.

Malformed frames and oversized frames can close the peer connection. Ordinary
request failures should return a rejected reply with a symbolic code and a
human-readable message.

Runtime task failures are not transport failures. They should be reported as
task events and, where useful, endpoint output.

## PASETO Use

PASETO is a reasonable substrate for portable grant tokens, especially for:

- reconnect tokens;
- web login continuation;
- short-lived host bootstrap;
- delegated invitations;
- agent credentials.

PASETO should not become the internal representation of Mica capabilities. A
validated token should produce an `AuthorityContext` and, where useful,
ephemeral capability values. Durable policy remains relational.

Tokens should include enough context to prevent replay across worlds or
purposes:

```text
world id
audience
issuer
subject actor
endpoint purpose
expiry
policy epoch or grant version
grant id
```

Revocation should be relational: update a grant relation or policy epoch, then
future authority rebuilds reject old tokens. Already-running tasks do not gain
or lose rights mid-instruction; they observe policy changes at defined
boundaries.

## Implementation Phases

### Phase 1: Protocol Completion

Update `mica-host-protocol` with:

- request ids;
- accepted and rejected replies;
- optional grant-token payload on `OpenEndpoint`;
- endpoint-closed event;
- heartbeat or ping/pong if needed by hosts.

Keep the frame format stable unless a real problem is found.

### Phase 2: Transport Crate

Add a transport crate for ZeroMQ, probably named `mica-host-zmq` or
`mica-transport-zmq`.

It should provide:

- IPC RPC bind/connect;
- IPC event bind/connect;
- TCP RPC bind/connect with CURVE/ZAP;
- TCP event bind/connect with CURVE/ZAP if supported by the chosen topology;
- MHP frame send/receive;
- compio readiness integration around ZeroMQ signalling FDs;
- a blocking `zmq_poll` fallback kept behind the same transport interface;
- tests using IPC first.

The crate should not depend on the Mica runtime.

### Phase 3: RPC Handler

Add a daemon-side handler that:

- negotiates protocol features;
- owns peer and endpoint tables;
- validates request correlation;
- submits tasks through the driver/runtime;
- appends emitted output to endpoint buffers;
- publishes output-ready and task events;
- drains bounded output batches.

The handler should depend on runtime interfaces, not on protocol-host-specific
command parsing.

### Phase 4: Authority Integration

Add:

- PASETO grant validation;
- endpoint authority construction;
- authority refresh on endpoint/task boundaries;
- policy epoch checks;
- tests for rejected grant, revoked grant, expired grant, and IPC local-peer
  baseline authority.

### Phase 5: First Host

Build a small telnet-like host over IPC:

- accept TCP clients itself;
- create one Mica endpoint per network connection;
- submit line input as string values;
- drain endpoint output and write it to the socket.

This keeps protocol-specific I/O outside the daemon while letting Mica code
control parsing and world behaviour.

## Open Questions

- Should endpoint ids always be Mica identities, or should transport peers have
  separate UUIDs with identity mapping only after `OpenEndpoint`?
- Should event delivery use PUB/SUB, XPUB/XSUB, or a ROUTER-based pull model?
- How much endpoint state should be exposed as transient relations from the
  start?
- Do we need IPC peer credential inspection, or are socket path permissions
  enough for the first version?
- Should `OpenEndpoint` always require a grant token for TCP but allow
  configured local authority for IPC?
- Should task completion events be endpoint-scoped, peer-scoped, or globally
  drainable by authorized tools?
- Should output overflow close the endpoint, drop output, or deliver a special
  overflow event?

## Recommended Next Step

Complete the missing request/reply pieces in `mica-host-protocol` before
building the ZeroMQ crate. The current frame codec is a good payload substrate,
but a robust RPC handler needs correlation ids and explicit accepted/rejected
replies before the transport can safely support multiple outstanding requests.
