# mica-host-protocol

`mica-host-protocol` defines Mica's language-neutral host/daemon wire format.
This crate is the Rust implementation of that contract, but the wire format is
not Rust-specific and should be implementable by other runtimes.

## Frame Format

All integers are little-endian. Strings are length-prefixed UTF-8. Mica values
use the `mica-var` value wire format.

```text
magic[4]      = "MHP1"
frame_len u32 = byte length after this field, including message_type and flags
message_type u16
flags u16      = reserved, currently zero
payload bytes
```

Unknown message types can be skipped by generic transports using `frame_len`.
The typed decoder in this crate rejects unknown message types and non-zero
reserved flags.

## Streaming

Transport code should treat frames as a byte stream, not as one read per
message. The Rust implementation provides a `FrameDecoder` that buffers partial
input and exposes a borrowed `FrameRef` when a complete frame is available.
Callers decode or route that borrowed frame, then explicitly consume it.

For writes, `encode_frame_segments` builds a scatter/gather-friendly frame. It
keeps fixed-width headers in small scratch segments and borrows string and heap
value payload bytes where possible, so transports that support vectored writes
can avoid flattening the frame into one contiguous buffer.

## Message IDs

```text
0x0001 Hello
0x0002 HelloAck
0x0003 RequestAccepted
0x0004 RequestRejected

0x0100 OpenEndpoint
0x0101 CloseEndpoint
0x0102 ResolveIdentity
0x0103 IdentityResolved

0x0200 SubmitSource
0x0201 SubmitInput

0x0300 OutputReady
0x0301 DrainOutput
0x0302 OutputBatch
0x0303 EndpointClosed

0x0400 TaskCompleted
0x0401 TaskFailed
```

## Payloads

```text
Hello:
  protocol_version u16
  min_protocol_version u16
  feature_bits u64
  host_name string

HelloAck:
  protocol_version u16
  feature_bits u64

RequestAccepted:
  request_id u64
  has_task_id u8
  task_id u64 if has_task_id == 1

RequestRejected:
  request_id u64
  code_symbol_name string
  message string

OpenEndpoint:
  request_id u64
  endpoint_id u64
  has_actor_id u8
  actor_id u64 if has_actor_id == 1
  protocol string
  has_grant_token u8
  grant_token string if has_grant_token == 1

CloseEndpoint:
  request_id u64
  endpoint_id u64

ResolveIdentity:
  request_id u64
  name_symbol_name string

IdentityResolved:
  request_id u64
  name_symbol_name string
  identity_id u64

SubmitSource:
  request_id u64
  endpoint_id u64
  actor_id u64
  source string

SubmitInput:
  request_id u64
  endpoint_id u64
  value mica-value

OutputReady:
  endpoint_id u64
  buffered u32

DrainOutput:
  request_id u64
  endpoint_id u64
  limit u32

OutputBatch:
  endpoint_id u64
  count u32
  value[count] mica-value

EndpointClosed:
  endpoint_id u64
  reason string

TaskCompleted:
  task_id u64
  value mica-value

TaskFailed:
  task_id u64
  error mica-value
```

## Licence

Mica is licensed under the GNU Affero General Public License v3.0. See the
repository root `LICENSE`.
