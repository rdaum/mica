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

## Message IDs

```text
0x0001 Hello
0x0002 HelloAck

0x0100 OpenEndpoint
0x0101 CloseEndpoint

0x0200 SubmitSource
0x0201 SubmitInput

0x0300 OutputReady
0x0301 DrainOutput
0x0302 OutputBatch

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

OpenEndpoint:
  endpoint_id u64
  protocol string

CloseEndpoint:
  endpoint_id u64

SubmitSource:
  endpoint_id u64
  actor_id u64
  source string

SubmitInput:
  endpoint_id u64
  value mica-value

OutputReady:
  endpoint_id u64
  buffered u32

DrainOutput:
  endpoint_id u64
  limit u32

OutputBatch:
  endpoint_id u64
  count u32
  value[count] mica-value

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
