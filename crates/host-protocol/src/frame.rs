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

use crate::error::HostProtocolError;
use crate::message::{HostMessage, MessageType};
use crate::payload::{Reader, encode_payload, encode_payload_segments, write_u16, write_u32};
use crate::{DEFAULT_MAX_FRAME_LEN, MAGIC};
use mica_var::{Identity, Symbol, Value, ValueSegment, ValueSegments, encode_value_segments};
use std::io::IoSlice;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameRef<'a> {
    bytes: &'a [u8],
}

impl<'a> FrameRef<'a> {
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }

    pub fn message_type(self) -> Result<MessageType, HostProtocolError> {
        read_frame_message_type(self.bytes)
    }

    pub fn decode(self) -> Result<HostMessage, HostProtocolError> {
        decode_frame(self.bytes)
    }
}

#[derive(Clone, Debug)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
    start: usize,
    max_frame_len: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_LEN)
    }
}

impl FrameDecoder {
    pub fn new(max_frame_len: usize) -> Self {
        Self {
            buffer: Vec::new(),
            start: 0,
            max_frame_len,
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.compact_if_sparse();
        self.buffer.extend_from_slice(bytes);
    }

    pub fn peek_frame(&self) -> Result<Option<FrameRef<'_>>, HostProtocolError> {
        let bytes = self.available_bytes();
        let Some(total_len) = complete_frame_len(bytes, self.max_frame_len)? else {
            return Ok(None);
        };
        Ok(Some(FrameRef {
            bytes: &bytes[..total_len],
        }))
    }

    pub fn consume_frame(&mut self) -> Result<bool, HostProtocolError> {
        let Some(total_len) = complete_frame_len(self.available_bytes(), self.max_frame_len)?
        else {
            return Ok(false);
        };
        self.start = self
            .start
            .checked_add(total_len)
            .ok_or(HostProtocolError::OffsetOverflow)?;
        self.compact_if_sparse();
        Ok(true)
    }

    pub fn buffered_len(&self) -> usize {
        self.available_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffered_len() == 0
    }

    fn available_bytes(&self) -> &[u8] {
        &self.buffer[self.start..]
    }

    fn compact_if_sparse(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start == self.buffer.len() {
            self.buffer.clear();
            self.start = 0;
            return;
        }
        if self.start >= 4096 || self.start * 2 >= self.buffer.len() {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameSegment<'a> {
    Borrowed(&'a [u8]),
    Scratch(usize),
    ValueScratch {
        value_index: usize,
        scratch_index: usize,
    },
    ValueBorrowed {
        value_index: usize,
        segment_index: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScratchSegment {
    bytes: [u8; 8],
    len: u8,
}

#[derive(Debug)]
pub struct EncodedFrameSegments<'a> {
    scratch: Vec<ScratchSegment>,
    values: Vec<ValueSegments<'a>>,
    segments: Vec<FrameSegment<'a>>,
    len: usize,
}

impl<'a> EncodedFrameSegments<'a> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn segment_bytes(&self, segment: FrameSegment<'a>) -> &[u8] {
        match segment {
            FrameSegment::Borrowed(bytes) => bytes,
            FrameSegment::Scratch(index) => self.scratch[index].bytes(),
            FrameSegment::ValueScratch {
                value_index,
                scratch_index,
            } => self.values[value_index].scratch(scratch_index),
            FrameSegment::ValueBorrowed {
                value_index,
                segment_index,
            } => match self.values[value_index].segments()[segment_index] {
                ValueSegment::Borrowed(bytes) => bytes,
                ValueSegment::Scratch(_) => unreachable!("value segment is not borrowed"),
            },
        }
    }

    pub fn for_each_segment(&self, mut visit: impl FnMut(&[u8])) {
        for &segment in &self.segments {
            visit(self.segment_bytes(segment));
        }
    }

    pub fn io_slices(&self) -> Vec<IoSlice<'_>> {
        self.segments
            .iter()
            .map(|&segment| IoSlice::new(self.segment_bytes(segment)))
            .collect()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len);
        self.for_each_segment(|bytes| out.extend_from_slice(bytes));
        out
    }

    fn push_scratch(&mut self, bytes: &[u8]) {
        debug_assert!(bytes.len() <= 8);
        let index = self.scratch.len();
        let mut scratch = ScratchSegment {
            bytes: [0; 8],
            len: bytes.len() as u8,
        };
        scratch.bytes[..bytes.len()].copy_from_slice(bytes);
        self.scratch.push(scratch);
        self.segments.push(FrameSegment::Scratch(index));
        self.len += bytes.len();
    }

    fn push_borrowed(&mut self, bytes: &'a [u8]) {
        if bytes.is_empty() {
            return;
        }
        self.segments.push(FrameSegment::Borrowed(bytes));
        self.len += bytes.len();
    }

    pub(crate) fn push_value(&mut self, value: &'a Value) -> Result<(), HostProtocolError> {
        let value_index = self.values.len();
        let segments = encode_value_segments(value)?;
        self.len += segments.len();
        for (segment_index, segment) in segments.segments().iter().enumerate() {
            match *segment {
                ValueSegment::Scratch(scratch_index) => {
                    self.segments.push(FrameSegment::ValueScratch {
                        value_index,
                        scratch_index,
                    });
                }
                ValueSegment::Borrowed(_) => {
                    self.segments.push(FrameSegment::ValueBorrowed {
                        value_index,
                        segment_index,
                    });
                }
            }
        }
        self.values.push(segments);
        Ok(())
    }
}

impl ScratchSegment {
    fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

pub fn encode_frame_segments(
    message: &HostMessage,
) -> Result<EncodedFrameSegments<'_>, HostProtocolError> {
    let mut payload = EncodedFrameSegments {
        scratch: Vec::new(),
        values: Vec::new(),
        segments: Vec::new(),
        len: 0,
    };
    encode_payload_segments(message, &mut payload)?;

    let frame_len = payload
        .len
        .checked_add(4)
        .ok_or(HostProtocolError::OffsetOverflow)?;
    let frame_len =
        u32::try_from(frame_len).map_err(|_| HostProtocolError::FrameTooLarge(frame_len))?;

    let mut out = EncodedFrameSegments {
        scratch: Vec::with_capacity(payload.scratch.len() + 2),
        values: payload.values,
        segments: Vec::with_capacity(payload.segments.len() + 2),
        len: 0,
    };
    let mut header = [0; 8];
    header[..4].copy_from_slice(&MAGIC);
    header[4..].copy_from_slice(&frame_len.to_le_bytes());
    out.push_scratch(&header);

    let mut envelope = [0; 4];
    envelope[..2].copy_from_slice(&message.message_type().raw().to_le_bytes());
    out.push_scratch(&envelope);

    let scratch_offset = out.scratch.len();
    out.len += payload.len;
    out.scratch.extend(payload.scratch);
    for segment in payload.segments {
        out.segments.push(match segment {
            FrameSegment::Scratch(index) => FrameSegment::Scratch(index + scratch_offset),
            other => other,
        });
    }
    Ok(out)
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
    validate_complete_frame(frame, usize::MAX)?;
    let mut reader = Reader::new(frame);
    reader.expect_magic()?;
    let _ = reader.read_u32()?;
    let message_type = reader.read_message_type()?;
    let flags = reader.read_u16()?;
    if flags != 0 {
        return Err(HostProtocolError::UnsupportedFlags(flags));
    }
    let message = reader.read_message(message_type)?;
    reader.expect_end()?;
    Ok(message)
}

fn read_frame_message_type(frame: &[u8]) -> Result<MessageType, HostProtocolError> {
    validate_complete_frame(frame, usize::MAX)?;
    let raw = u16::from_le_bytes(frame[8..10].try_into().unwrap());
    MessageType::from_raw(raw).ok_or(HostProtocolError::UnknownMessageType(raw))
}

pub(crate) fn push_identity(out: &mut EncodedFrameSegments<'_>, identity: Identity) {
    push_u64(out, identity.raw());
}

pub(crate) fn push_string<'a>(
    out: &mut EncodedFrameSegments<'a>,
    value: &'a str,
) -> Result<(), HostProtocolError> {
    push_len(out, value.len())?;
    out.push_borrowed(value.as_bytes());
    Ok(())
}

pub(crate) fn push_symbol_name(
    out: &mut EncodedFrameSegments<'_>,
    value: Symbol,
) -> Result<(), HostProtocolError> {
    let Some(name) = value.name() else {
        return Err(HostProtocolError::SymbolNameUnavailable(value.id()));
    };
    push_len(out, name.len())?;
    out.push_borrowed(name.as_bytes());
    Ok(())
}

pub(crate) fn push_optional_string<'a>(
    out: &mut EncodedFrameSegments<'a>,
    value: Option<&'a str>,
) -> Result<(), HostProtocolError> {
    match value {
        Some(value) => {
            push_u8(out, 1);
            push_string(out, value)?;
        }
        None => push_u8(out, 0),
    }
    Ok(())
}

pub(crate) fn push_optional_identity(out: &mut EncodedFrameSegments<'_>, value: Option<Identity>) {
    match value {
        Some(value) => {
            push_u8(out, 1);
            push_identity(out, value);
        }
        None => push_u8(out, 0),
    }
}

pub(crate) fn push_optional_u64(out: &mut EncodedFrameSegments<'_>, value: Option<u64>) {
    match value {
        Some(value) => {
            push_u8(out, 1);
            push_u64(out, value);
        }
        None => push_u8(out, 0),
    }
}

pub(crate) fn push_len(
    out: &mut EncodedFrameSegments<'_>,
    len: usize,
) -> Result<(), HostProtocolError> {
    let len = u32::try_from(len).map_err(|_| HostProtocolError::FrameTooLarge(len))?;
    push_u32(out, len);
    Ok(())
}

fn push_u8(out: &mut EncodedFrameSegments<'_>, value: u8) {
    out.push_scratch(&[value]);
}

pub(crate) fn push_u16(out: &mut EncodedFrameSegments<'_>, value: u16) {
    out.push_scratch(&value.to_le_bytes());
}

pub(crate) fn push_u32(out: &mut EncodedFrameSegments<'_>, value: u32) {
    out.push_scratch(&value.to_le_bytes());
}

pub(crate) fn push_u64(out: &mut EncodedFrameSegments<'_>, value: u64) {
    out.push_scratch(&value.to_le_bytes());
}

fn complete_frame_len(
    bytes: &[u8],
    max_frame_len: usize,
) -> Result<Option<usize>, HostProtocolError> {
    if bytes.len() < 8 {
        return Ok(None);
    }
    if bytes[..4] != MAGIC {
        return Err(HostProtocolError::InvalidMagic(
            bytes[..4].try_into().unwrap(),
        ));
    }
    let frame_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    validate_frame_len(frame_len, bytes.len().saturating_sub(8), max_frame_len)?;
    let total_len = frame_len
        .checked_add(8)
        .ok_or(HostProtocolError::OffsetOverflow)?;
    if bytes.len() < total_len {
        return Ok(None);
    }
    Ok(Some(total_len))
}

fn validate_complete_frame(frame: &[u8], max_frame_len: usize) -> Result<(), HostProtocolError> {
    if frame.len() < 8 {
        return Err(HostProtocolError::UnexpectedEnd {
            needed: 8,
            offset: 0,
            len: frame.len(),
        });
    }
    if frame[..4] != MAGIC {
        return Err(HostProtocolError::InvalidMagic(
            frame[..4].try_into().unwrap(),
        ));
    }
    let frame_len = u32::from_le_bytes(frame[4..8].try_into().unwrap()) as usize;
    validate_frame_len(frame_len, frame.len().saturating_sub(8), max_frame_len)?;
    let available = frame.len() - 8;
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
    Ok(())
}

fn validate_frame_len(
    frame_len: usize,
    available: usize,
    max_frame_len: usize,
) -> Result<(), HostProtocolError> {
    if frame_len > max_frame_len {
        return Err(HostProtocolError::FrameTooLarge(frame_len));
    }
    if frame_len < 4 {
        return Err(HostProtocolError::InvalidFrameLength {
            declared: frame_len,
            actual: available,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
