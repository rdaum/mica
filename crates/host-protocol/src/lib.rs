// Copyright (C) 2026 Ryan Daum <ryan.daum@gmail.com> This program is free
// software: you can redistribute it and/or modify it under the terms of the GNU
// Affero General Public License as published by the Free Software Foundation,
// version 3.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! Language-neutral host protocol framing for Mica.

use mica_var::{Identity, Value, ValueCodecError, decode_value, encode_value};
use std::fmt;

pub const MAGIC: [u8; 4] = *b"MHP1";
pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum MessageType {
    Hello = 0x0001,
    HelloAck = 0x0002,
    OpenEndpoint = 0x0100,
    CloseEndpoint = 0x0101,
    SubmitSource = 0x0200,
    SubmitInput = 0x0201,
    OutputReady = 0x0300,
    DrainOutput = 0x0301,
    OutputBatch = 0x0302,
    TaskCompleted = 0x0400,
    TaskFailed = 0x0401,
}

impl MessageType {
    pub const fn raw(self) -> u16 {
        self as u16
    }

    pub const fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            0x0001 => Some(Self::Hello),
            0x0002 => Some(Self::HelloAck),
            0x0100 => Some(Self::OpenEndpoint),
            0x0101 => Some(Self::CloseEndpoint),
            0x0200 => Some(Self::SubmitSource),
            0x0201 => Some(Self::SubmitInput),
            0x0300 => Some(Self::OutputReady),
            0x0301 => Some(Self::DrainOutput),
            0x0302 => Some(Self::OutputBatch),
            0x0400 => Some(Self::TaskCompleted),
            0x0401 => Some(Self::TaskFailed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum HostMessage {
    Hello {
        protocol_version: u16,
        min_protocol_version: u16,
        feature_bits: u64,
        host_name: String,
    },
    HelloAck {
        protocol_version: u16,
        feature_bits: u64,
    },
    OpenEndpoint {
        endpoint: Identity,
        protocol: String,
    },
    CloseEndpoint {
        endpoint: Identity,
    },
    SubmitSource {
        endpoint: Identity,
        actor: Identity,
        source: String,
    },
    SubmitInput {
        endpoint: Identity,
        value: Value,
    },
    OutputReady {
        endpoint: Identity,
        buffered: u32,
    },
    DrainOutput {
        endpoint: Identity,
        limit: u32,
    },
    OutputBatch {
        endpoint: Identity,
        values: Vec<Value>,
    },
    TaskCompleted {
        task_id: u64,
        value: Value,
    },
    TaskFailed {
        task_id: u64,
        error: Value,
    },
}

impl HostMessage {
    pub const fn message_type(&self) -> MessageType {
        match self {
            Self::Hello { .. } => MessageType::Hello,
            Self::HelloAck { .. } => MessageType::HelloAck,
            Self::OpenEndpoint { .. } => MessageType::OpenEndpoint,
            Self::CloseEndpoint { .. } => MessageType::CloseEndpoint,
            Self::SubmitSource { .. } => MessageType::SubmitSource,
            Self::SubmitInput { .. } => MessageType::SubmitInput,
            Self::OutputReady { .. } => MessageType::OutputReady,
            Self::DrainOutput { .. } => MessageType::DrainOutput,
            Self::OutputBatch { .. } => MessageType::OutputBatch,
            Self::TaskCompleted { .. } => MessageType::TaskCompleted,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostProtocolError {
    FrameTooLarge(usize),
    InvalidMagic([u8; 4]),
    InvalidFrameLength {
        declared: usize,
        actual: usize,
    },
    UnexpectedEnd {
        needed: usize,
        offset: usize,
        len: usize,
    },
    TrailingFrameBytes(usize),
    TrailingPayload(usize),
    UnsupportedFlags(u16),
    InvalidUtf8(String),
    InvalidIdentity(u64),
    UnknownMessageType(u16),
    Value(ValueCodecError),
    OffsetOverflow,
}

impl fmt::Display for HostProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge(len) => write!(f, "host frame length {len} exceeds u32"),
            Self::InvalidMagic(magic) => write!(f, "invalid host frame magic {magic:?}"),
            Self::InvalidFrameLength { declared, actual } => {
                write!(
                    f,
                    "invalid host frame length: declared {declared}, actual {actual}"
                )
            }
            Self::UnexpectedEnd {
                needed,
                offset,
                len,
            } => write!(
                f,
                "host frame ended early: need {needed} bytes at offset {offset}, len {len}"
            ),
            Self::TrailingFrameBytes(count) => {
                write!(f, "trailing bytes after host frame: {count}")
            }
            Self::TrailingPayload(count) => {
                write!(f, "trailing bytes in host message payload: {count}")
            }
            Self::UnsupportedFlags(flags) => {
                write!(f, "unsupported host frame flags 0x{flags:04x}")
            }
            Self::InvalidUtf8(error) => write!(f, "invalid utf-8: {error}"),
            Self::InvalidIdentity(raw) => write!(f, "identity {raw} is out of range"),
            Self::UnknownMessageType(message_type) => {
                write!(f, "unknown host message type 0x{message_type:04x}")
            }
            Self::Value(error) => write!(f, "invalid host value payload: {error}"),
            Self::OffsetOverflow => f.write_str("host frame offset overflow"),
        }
    }
}

impl std::error::Error for HostProtocolError {}

impl From<ValueCodecError> for HostProtocolError {
    fn from(error: ValueCodecError) -> Self {
        Self::Value(error)
    }
}

pub fn encode_frame(message: &HostMessage, out: &mut Vec<u8>) -> Result<(), HostProtocolError> {
    let mut payload = Vec::new();
    encode_payload(message, &mut payload)?;

    let frame_len = payload
        .len()
        .checked_add(4)
        .ok_or(HostProtocolError::OffsetOverflow)?;
    let frame_len =
        u32::try_from(frame_len).map_err(|_| HostProtocolError::FrameTooLarge(frame_len))?;

    out.extend_from_slice(&MAGIC);
    write_u32(out, frame_len);
    write_u16(out, message.message_type().raw());
    write_u16(out, 0);
    out.extend_from_slice(&payload);
    Ok(())
}

pub fn encoded_frame(message: &HostMessage) -> Result<Vec<u8>, HostProtocolError> {
    let mut out = Vec::new();
    encode_frame(message, &mut out)?;
    Ok(out)
}

pub fn decode_frame(frame: &[u8]) -> Result<HostMessage, HostProtocolError> {
    let mut reader = Reader::new(frame);
    reader.expect_magic()?;
    let frame_len = reader.read_u32()? as usize;
    let available = frame.len().saturating_sub(8);
    if frame_len > available {
        return Err(HostProtocolError::UnexpectedEnd {
            needed: frame_len,
            offset: 8,
            len: frame.len(),
        });
    }
    if frame_len < available {
        return Err(HostProtocolError::TrailingFrameBytes(available - frame_len));
    }
    if frame_len < 4 {
        return Err(HostProtocolError::InvalidFrameLength {
            declared: frame_len,
            actual: available,
        });
    }
    let message_type = reader.read_message_type()?;
    let flags = reader.read_u16()?;
    if flags != 0 {
        return Err(HostProtocolError::UnsupportedFlags(flags));
    }
    let message = reader.read_message(message_type)?;
    reader.expect_end()?;
    Ok(message)
}

fn encode_payload(message: &HostMessage, out: &mut Vec<u8>) -> Result<(), HostProtocolError> {
    match message {
        HostMessage::Hello {
            protocol_version,
            min_protocol_version,
            feature_bits,
            host_name,
        } => {
            write_u16(out, *protocol_version);
            write_u16(out, *min_protocol_version);
            write_u64(out, *feature_bits);
            write_string(out, host_name)?;
        }
        HostMessage::HelloAck {
            protocol_version,
            feature_bits,
        } => {
            write_u16(out, *protocol_version);
            write_u64(out, *feature_bits);
        }
        HostMessage::OpenEndpoint { endpoint, protocol } => {
            write_identity(out, *endpoint);
            write_string(out, protocol)?;
        }
        HostMessage::CloseEndpoint { endpoint } => {
            write_identity(out, *endpoint);
        }
        HostMessage::SubmitSource {
            endpoint,
            actor,
            source,
        } => {
            write_identity(out, *endpoint);
            write_identity(out, *actor);
            write_string(out, source)?;
        }
        HostMessage::SubmitInput { endpoint, value } => {
            write_identity(out, *endpoint);
            encode_value(value, out)?;
        }
        HostMessage::OutputReady { endpoint, buffered } => {
            write_identity(out, *endpoint);
            write_u32(out, *buffered);
        }
        HostMessage::DrainOutput { endpoint, limit } => {
            write_identity(out, *endpoint);
            write_u32(out, *limit);
        }
        HostMessage::OutputBatch { endpoint, values } => {
            write_identity(out, *endpoint);
            write_len(out, values.len())?;
            for value in values {
                encode_value(value, out)?;
            }
        }
        HostMessage::TaskCompleted { task_id, value } => {
            write_u64(out, *task_id);
            encode_value(value, out)?;
        }
        HostMessage::TaskFailed { task_id, error } => {
            write_u64(out, *task_id);
            encode_value(error, out)?;
        }
    }
    Ok(())
}

fn write_identity(out: &mut Vec<u8>, identity: Identity) {
    write_u64(out, identity.raw());
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), HostProtocolError> {
    write_len(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_len(out: &mut Vec<u8>, len: usize) -> Result<(), HostProtocolError> {
    let len = u32::try_from(len).map_err(|_| HostProtocolError::FrameTooLarge(len))?;
    write_u32(out, len);
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self) -> Result<(), HostProtocolError> {
        let magic: [u8; 4] = self.read_exact(4)?.try_into().unwrap();
        if magic == MAGIC {
            Ok(())
        } else {
            Err(HostProtocolError::InvalidMagic(magic))
        }
    }

    fn expect_end(&self) -> Result<(), HostProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(HostProtocolError::TrailingPayload(
                self.bytes.len() - self.offset,
            ))
        }
    }

    fn read_message(
        &mut self,
        message_type: MessageType,
    ) -> Result<HostMessage, HostProtocolError> {
        Ok(match message_type {
            MessageType::Hello => HostMessage::Hello {
                protocol_version: self.read_u16()?,
                min_protocol_version: self.read_u16()?,
                feature_bits: self.read_u64()?,
                host_name: self.read_string()?,
            },
            MessageType::HelloAck => HostMessage::HelloAck {
                protocol_version: self.read_u16()?,
                feature_bits: self.read_u64()?,
            },
            MessageType::OpenEndpoint => HostMessage::OpenEndpoint {
                endpoint: self.read_identity()?,
                protocol: self.read_string()?,
            },
            MessageType::CloseEndpoint => HostMessage::CloseEndpoint {
                endpoint: self.read_identity()?,
            },
            MessageType::SubmitSource => HostMessage::SubmitSource {
                endpoint: self.read_identity()?,
                actor: self.read_identity()?,
                source: self.read_string()?,
            },
            MessageType::SubmitInput => HostMessage::SubmitInput {
                endpoint: self.read_identity()?,
                value: self.read_value()?,
            },
            MessageType::OutputReady => HostMessage::OutputReady {
                endpoint: self.read_identity()?,
                buffered: self.read_u32()?,
            },
            MessageType::DrainOutput => HostMessage::DrainOutput {
                endpoint: self.read_identity()?,
                limit: self.read_u32()?,
            },
            MessageType::OutputBatch => {
                let endpoint = self.read_identity()?;
                let count = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value()?);
                }
                HostMessage::OutputBatch { endpoint, values }
            }
            MessageType::TaskCompleted => HostMessage::TaskCompleted {
                task_id: self.read_u64()?,
                value: self.read_value()?,
            },
            MessageType::TaskFailed => HostMessage::TaskFailed {
                task_id: self.read_u64()?,
                error: self.read_value()?,
            },
        })
    }

    fn read_message_type(&mut self) -> Result<MessageType, HostProtocolError> {
        let raw = self.read_u16()?;
        MessageType::from_raw(raw).ok_or(HostProtocolError::UnknownMessageType(raw))
    }

    fn read_identity(&mut self) -> Result<Identity, HostProtocolError> {
        let raw = self.read_u64()?;
        Identity::new(raw).ok_or(HostProtocolError::InvalidIdentity(raw))
    }

    fn read_value(&mut self) -> Result<Value, HostProtocolError> {
        let (value, consumed) = decode_value(&self.bytes[self.offset..])?;
        self.offset = self
            .offset
            .checked_add(consumed)
            .ok_or(HostProtocolError::OffsetOverflow)?;
        Ok(value)
    }

    fn read_string(&mut self) -> Result<String, HostProtocolError> {
        let len = self.read_u32()? as usize;
        String::from_utf8(self.read_exact(len)?.to_vec())
            .map_err(|error| HostProtocolError::InvalidUtf8(error.to_string()))
    }

    fn read_u16(&mut self) -> Result<u16, HostProtocolError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32, HostProtocolError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64, HostProtocolError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], HostProtocolError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(HostProtocolError::OffsetOverflow)?;
        if end > self.bytes.len() {
            return Err(HostProtocolError::UnexpectedEnd {
                needed: len,
                offset: self.offset,
                len: self.bytes.len(),
            });
        }
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_var::Symbol;

    fn id(raw: u64) -> Identity {
        Identity::new(raw).unwrap()
    }

    #[test]
    fn hello_frame_matches_golden_bytes() {
        let message = HostMessage::Hello {
            protocol_version: 1,
            min_protocol_version: 1,
            feature_bits: 0,
            host_name: "h".to_owned(),
        };
        let frame = encoded_frame(&message).unwrap();
        assert_eq!(
            frame,
            vec![
                b'M', b'H', b'P', b'1', 21, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 1, 0, 0, 0, b'h',
            ]
        );
        assert_eq!(decode_frame(&frame).unwrap(), message);
    }

    #[test]
    fn submit_input_frame_matches_golden_bytes() {
        let message = HostMessage::SubmitInput {
            endpoint: id(42),
            value: Value::int(7).unwrap(),
        };
        let frame = encoded_frame(&message).unwrap();
        assert_eq!(
            frame,
            vec![
                b'M', b'H', b'P', b'1', 20, 0, 0, 0, 1, 2, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0,
                0, 0, 0, 0, 2,
            ]
        );
        assert_eq!(decode_frame(&frame).unwrap(), message);
    }

    #[test]
    fn round_trips_endpoint_task_and_output_messages() {
        let messages = [
            HostMessage::HelloAck {
                protocol_version: 1,
                feature_bits: 7,
            },
            HostMessage::OpenEndpoint {
                endpoint: id(1),
                protocol: "telnet".to_owned(),
            },
            HostMessage::CloseEndpoint { endpoint: id(1) },
            HostMessage::SubmitSource {
                endpoint: id(1),
                actor: id(2),
                source: "emit(#1, \"hi\")".to_owned(),
            },
            HostMessage::OutputReady {
                endpoint: id(1),
                buffered: 3,
            },
            HostMessage::DrainOutput {
                endpoint: id(1),
                limit: 64,
            },
            HostMessage::OutputBatch {
                endpoint: id(1),
                values: vec![
                    Value::string("hello"),
                    Value::symbol(Symbol::intern("ready")),
                ],
            },
            HostMessage::TaskCompleted {
                task_id: 10,
                value: Value::bool(true),
            },
            HostMessage::TaskFailed {
                task_id: 11,
                error: Value::error(
                    Symbol::intern("E_TEST"),
                    Some("failed"),
                    Some(Value::int(5).unwrap()),
                ),
            },
        ];

        for message in messages {
            let frame = encoded_frame(&message).unwrap();
            assert_eq!(decode_frame(&frame).unwrap(), message);
        }
    }

    #[test]
    fn rejects_unknown_message_type() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&MAGIC);
        frame.extend_from_slice(&4u32.to_le_bytes());
        frame.extend_from_slice(&0xffffu16.to_le_bytes());
        frame.extend_from_slice(&0u16.to_le_bytes());
        assert_eq!(
            decode_frame(&frame),
            Err(HostProtocolError::UnknownMessageType(0xffff))
        );
    }

    #[test]
    fn rejects_bad_magic_and_bad_frame_length() {
        let message = HostMessage::CloseEndpoint { endpoint: id(1) };
        let mut frame = encoded_frame(&message).unwrap();
        frame[0] = b'X';
        assert_eq!(
            decode_frame(&frame),
            Err(HostProtocolError::InvalidMagic(*b"XHP1"))
        );

        let mut frame = encoded_frame(&message).unwrap();
        frame[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(matches!(
            decode_frame(&frame),
            Err(HostProtocolError::UnexpectedEnd { .. })
        ));
    }

    #[test]
    fn rejects_reserved_flags_and_trailing_frame_bytes() {
        let message = HostMessage::CloseEndpoint { endpoint: id(1) };
        let mut frame = encoded_frame(&message).unwrap();
        frame[10..12].copy_from_slice(&1u16.to_le_bytes());
        assert_eq!(
            decode_frame(&frame),
            Err(HostProtocolError::UnsupportedFlags(1))
        );

        let mut frame = encoded_frame(&message).unwrap();
        frame.extend_from_slice(&[0]);
        assert_eq!(
            decode_frame(&frame),
            Err(HostProtocolError::TrailingFrameBytes(1))
        );
    }
}
