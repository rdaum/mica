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

use crate::MAGIC;
use crate::error::HostProtocolError;
use crate::frame::{
    EncodedFrameSegments, push_identity, push_len, push_optional_identity, push_optional_string,
    push_optional_u64, push_string, push_symbol_name, push_u16, push_u32, push_u64,
};
use crate::message::{HostMessage, MessageType};
use mica_var::{Identity, Symbol, Value, decode_value, encode_value};

pub(crate) fn encode_payload(
    message: &HostMessage,
    out: &mut Vec<u8>,
) -> Result<(), HostProtocolError> {
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
        HostMessage::RequestAccepted {
            request_id,
            task_id,
        } => {
            write_u64(out, *request_id);
            write_optional_u64(out, *task_id);
        }
        HostMessage::RequestRejected {
            request_id,
            code,
            message,
        } => {
            write_u64(out, *request_id);
            write_symbol_name(out, *code)?;
            write_string(out, message)?;
        }
        HostMessage::OpenEndpoint {
            request_id,
            endpoint,
            actor,
            protocol,
            grant_token,
        } => {
            write_u64(out, *request_id);
            write_identity(out, *endpoint);
            write_optional_identity(out, *actor);
            write_string(out, protocol)?;
            write_optional_string(out, grant_token.as_deref())?;
        }
        HostMessage::CloseEndpoint {
            request_id,
            endpoint,
        } => {
            write_u64(out, *request_id);
            write_identity(out, *endpoint);
        }
        HostMessage::ResolveIdentity { request_id, name } => {
            write_u64(out, *request_id);
            write_symbol_name(out, *name)?;
        }
        HostMessage::IdentityResolved {
            request_id,
            name,
            identity,
        } => {
            write_u64(out, *request_id);
            write_symbol_name(out, *name)?;
            write_identity(out, *identity);
        }
        HostMessage::SubmitSource {
            request_id,
            endpoint,
            actor,
            source,
        } => {
            write_u64(out, *request_id);
            write_identity(out, *endpoint);
            write_identity(out, *actor);
            write_string(out, source)?;
        }
        HostMessage::SubmitInput {
            request_id,
            endpoint,
            value,
        } => {
            write_u64(out, *request_id);
            write_identity(out, *endpoint);
            encode_value(value, out)?;
        }
        HostMessage::OutputReady { endpoint, buffered } => {
            write_identity(out, *endpoint);
            write_u32(out, *buffered);
        }
        HostMessage::DrainOutput {
            request_id,
            endpoint,
            limit,
        } => {
            write_u64(out, *request_id);
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
        HostMessage::EndpointClosed { endpoint, reason } => {
            write_identity(out, *endpoint);
            write_string(out, reason)?;
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

pub(crate) fn encode_payload_segments<'a>(
    message: &'a HostMessage,
    out: &mut EncodedFrameSegments<'a>,
) -> Result<(), HostProtocolError> {
    match message {
        HostMessage::Hello {
            protocol_version,
            min_protocol_version,
            feature_bits,
            host_name,
        } => {
            push_u16(out, *protocol_version);
            push_u16(out, *min_protocol_version);
            push_u64(out, *feature_bits);
            push_string(out, host_name)?;
        }
        HostMessage::HelloAck {
            protocol_version,
            feature_bits,
        } => {
            push_u16(out, *protocol_version);
            push_u64(out, *feature_bits);
        }
        HostMessage::RequestAccepted {
            request_id,
            task_id,
        } => {
            push_u64(out, *request_id);
            push_optional_u64(out, *task_id);
        }
        HostMessage::RequestRejected {
            request_id,
            code,
            message,
        } => {
            push_u64(out, *request_id);
            push_symbol_name(out, *code)?;
            push_string(out, message)?;
        }
        HostMessage::OpenEndpoint {
            request_id,
            endpoint,
            actor,
            protocol,
            grant_token,
        } => {
            push_u64(out, *request_id);
            push_identity(out, *endpoint);
            push_optional_identity(out, *actor);
            push_string(out, protocol)?;
            push_optional_string(out, grant_token.as_deref())?;
        }
        HostMessage::CloseEndpoint {
            request_id,
            endpoint,
        } => {
            push_u64(out, *request_id);
            push_identity(out, *endpoint);
        }
        HostMessage::ResolveIdentity { request_id, name } => {
            push_u64(out, *request_id);
            push_symbol_name(out, *name)?;
        }
        HostMessage::IdentityResolved {
            request_id,
            name,
            identity,
        } => {
            push_u64(out, *request_id);
            push_symbol_name(out, *name)?;
            push_identity(out, *identity);
        }
        HostMessage::SubmitSource {
            request_id,
            endpoint,
            actor,
            source,
        } => {
            push_u64(out, *request_id);
            push_identity(out, *endpoint);
            push_identity(out, *actor);
            push_string(out, source)?;
        }
        HostMessage::SubmitInput {
            request_id,
            endpoint,
            value,
        } => {
            push_u64(out, *request_id);
            push_identity(out, *endpoint);
            out.push_value(value)?;
        }
        HostMessage::OutputReady { endpoint, buffered } => {
            push_identity(out, *endpoint);
            push_u32(out, *buffered);
        }
        HostMessage::DrainOutput {
            request_id,
            endpoint,
            limit,
        } => {
            push_u64(out, *request_id);
            push_identity(out, *endpoint);
            push_u32(out, *limit);
        }
        HostMessage::OutputBatch { endpoint, values } => {
            push_identity(out, *endpoint);
            push_len(out, values.len())?;
            for value in values {
                out.push_value(value)?;
            }
        }
        HostMessage::EndpointClosed { endpoint, reason } => {
            push_identity(out, *endpoint);
            push_string(out, reason)?;
        }
        HostMessage::TaskCompleted { task_id, value } => {
            push_u64(out, *task_id);
            out.push_value(value)?;
        }
        HostMessage::TaskFailed { task_id, error } => {
            push_u64(out, *task_id);
            out.push_value(error)?;
        }
    }
    Ok(())
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn expect_magic(&mut self) -> Result<(), HostProtocolError> {
        let magic: [u8; 4] = self.read_exact(4)?.try_into().unwrap();
        if magic == MAGIC {
            Ok(())
        } else {
            Err(HostProtocolError::InvalidMagic(magic))
        }
    }

    pub(crate) fn expect_end(&self) -> Result<(), HostProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(HostProtocolError::TrailingPayload(
                self.bytes.len() - self.offset,
            ))
        }
    }

    pub(crate) fn read_message(
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
            MessageType::RequestAccepted => HostMessage::RequestAccepted {
                request_id: self.read_u64()?,
                task_id: self.read_optional_u64()?,
            },
            MessageType::RequestRejected => HostMessage::RequestRejected {
                request_id: self.read_u64()?,
                code: self.read_symbol_name()?,
                message: self.read_string()?,
            },
            MessageType::OpenEndpoint => HostMessage::OpenEndpoint {
                request_id: self.read_u64()?,
                endpoint: self.read_identity()?,
                actor: self.read_optional_identity()?,
                protocol: self.read_string()?,
                grant_token: self.read_optional_string()?,
            },
            MessageType::CloseEndpoint => HostMessage::CloseEndpoint {
                request_id: self.read_u64()?,
                endpoint: self.read_identity()?,
            },
            MessageType::ResolveIdentity => HostMessage::ResolveIdentity {
                request_id: self.read_u64()?,
                name: self.read_symbol_name()?,
            },
            MessageType::IdentityResolved => HostMessage::IdentityResolved {
                request_id: self.read_u64()?,
                name: self.read_symbol_name()?,
                identity: self.read_identity()?,
            },
            MessageType::SubmitSource => HostMessage::SubmitSource {
                request_id: self.read_u64()?,
                endpoint: self.read_identity()?,
                actor: self.read_identity()?,
                source: self.read_string()?,
            },
            MessageType::SubmitInput => HostMessage::SubmitInput {
                request_id: self.read_u64()?,
                endpoint: self.read_identity()?,
                value: self.read_value()?,
            },
            MessageType::OutputReady => HostMessage::OutputReady {
                endpoint: self.read_identity()?,
                buffered: self.read_u32()?,
            },
            MessageType::DrainOutput => HostMessage::DrainOutput {
                request_id: self.read_u64()?,
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
            MessageType::EndpointClosed => HostMessage::EndpointClosed {
                endpoint: self.read_identity()?,
                reason: self.read_string()?,
            },
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

    pub(crate) fn read_message_type(&mut self) -> Result<MessageType, HostProtocolError> {
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

    fn read_symbol_name(&mut self) -> Result<Symbol, HostProtocolError> {
        self.read_string().map(|name| Symbol::intern(&name))
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, HostProtocolError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string().map(Some),
            tag => Err(HostProtocolError::InvalidOptionTag(tag)),
        }
    }

    fn read_optional_identity(&mut self) -> Result<Option<Identity>, HostProtocolError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_identity().map(Some),
            tag => Err(HostProtocolError::InvalidOptionTag(tag)),
        }
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, HostProtocolError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u64().map(Some),
            tag => Err(HostProtocolError::InvalidOptionTag(tag)),
        }
    }

    fn read_u8(&mut self) -> Result<u8, HostProtocolError> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    pub(crate) fn read_u16(&mut self) -> Result<u16, HostProtocolError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32, HostProtocolError> {
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

fn write_identity(out: &mut Vec<u8>, identity: Identity) {
    write_u64(out, identity.raw());
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), HostProtocolError> {
    write_len(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_symbol_name(out: &mut Vec<u8>, value: Symbol) -> Result<(), HostProtocolError> {
    let Some(name) = value.name() else {
        return Err(HostProtocolError::SymbolNameUnavailable(value.id()));
    };
    write_string(out, name)
}

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), HostProtocolError> {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_string(out, value)?;
        }
        None => write_u8(out, 0),
    }
    Ok(())
}

fn write_optional_identity(out: &mut Vec<u8>, value: Option<Identity>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_identity(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_len(out: &mut Vec<u8>, len: usize) -> Result<(), HostProtocolError> {
    let len = u32::try_from(len).map_err(|_| HostProtocolError::FrameTooLarge(len))?;
    write_u32(out, len);
    Ok(())
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

pub(crate) fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
