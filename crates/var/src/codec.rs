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

use crate::value::{
    PAYLOAD_MASK, TAG_BOOL, TAG_BYTES, TAG_CAPABILITY, TAG_ERROR, TAG_ERROR_CODE, TAG_FLOAT,
    TAG_FROB, TAG_IDENTITY, TAG_INT, TAG_LIST, TAG_MAP, TAG_NOTHING, TAG_RANGE, TAG_SHIFT,
    TAG_STRING, TAG_SYMBOL,
};
use crate::{CapabilityId, Identity, Symbol, Value, ValueRef};
use std::fmt;

const EXTENDED_TAG: u8 = 0xff;
const EXT_KIND_SHIFT: u64 = 48;
const EXT_AUX_MASK: u64 = 0x0000_ffff_ffff_ffff;
const RANGE_HAS_END: u64 = 1;
const ERROR_HAS_MESSAGE: u64 = 1;
const ERROR_HAS_VALUE: u64 = 1 << 1;

/// Controls how symbol-like values are encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SymbolEncoding {
    /// Encode symbol names. This is stable across processes and restarts.
    Name,
    /// Encode symbol ids. This is compact, but only safe when the symbol table
    /// is already known to be shared.
    Id,
}

/// Options for the owned value codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueCodecOptions {
    pub symbol_encoding: SymbolEncoding,
    pub allow_capabilities: bool,
}

impl Default for ValueCodecOptions {
    fn default() -> Self {
        Self {
            symbol_encoding: SymbolEncoding::Name,
            allow_capabilities: false,
        }
    }
}

/// Error returned by the owned value codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueCodecError {
    LengthTooLarge(usize),
    InlineHeapValue(u8),
    UnnamedSymbol(u32),
    CapabilityNotEncodable,
    CapabilityNotDecodable,
    UnexpectedEnd {
        needed: usize,
        offset: usize,
        len: usize,
    },
    TrailingBytes(usize),
    InvalidBoolPayload(u64),
    InvalidFloatPayload(u64),
    InvalidCapabilityPayload(u64),
    InvalidSymbolPayload(u64),
    InvalidUtf8(String),
    InvalidIdentity(u64),
    InvalidValue(String),
    InvalidExtendedAux {
        kind: u8,
        aux: u64,
    },
    UnknownValueTag(u8),
    OffsetOverflow,
}

impl fmt::Display for ValueCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthTooLarge(len) => write!(f, "length {len} exceeds 48-bit codec limit"),
            Self::InlineHeapValue(tag) => write!(f, "inline heap value tag {tag} is not decodable"),
            Self::UnnamedSymbol(id) => write!(f, "cannot encode unnamed symbol id {id}"),
            Self::CapabilityNotEncodable => f.write_str("capability values cannot be encoded"),
            Self::CapabilityNotDecodable => f.write_str("capability values cannot be decoded"),
            Self::UnexpectedEnd {
                needed,
                offset,
                len,
            } => write!(
                f,
                "value record ended early: need {needed} bytes at offset {offset}, len {len}"
            ),
            Self::TrailingBytes(count) => {
                write!(f, "trailing bytes in value record: {count}")
            }
            Self::InvalidBoolPayload(payload) => {
                write!(f, "invalid boolean payload {payload}")
            }
            Self::InvalidFloatPayload(payload) => {
                write!(f, "invalid float payload {payload}")
            }
            Self::InvalidCapabilityPayload(payload) => {
                write!(f, "invalid capability payload {payload}")
            }
            Self::InvalidSymbolPayload(payload) => {
                write!(f, "invalid symbol payload {payload}")
            }
            Self::InvalidUtf8(error) => write!(f, "invalid utf-8: {error}"),
            Self::InvalidIdentity(raw) => write!(f, "identity {raw} is out of range"),
            Self::InvalidValue(error) => write!(f, "invalid value: {error}"),
            Self::InvalidExtendedAux { kind, aux } => {
                write!(f, "invalid extended aux {aux} for value kind tag {kind}")
            }
            Self::UnknownValueTag(tag) => write!(f, "unknown value kind tag {tag}"),
            Self::OffsetOverflow => f.write_str("value record offset overflow"),
        }
    }
}

impl std::error::Error for ValueCodecError {}

/// Output target for value encoding.
pub trait ValueSink {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ValueCodecError>;
}

impl ValueSink for Vec<u8> {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ValueCodecError> {
        self.extend_from_slice(bytes);
        Ok(())
    }
}

/// Borrowed and owned byte segments for an encoded value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueSegment<'a> {
    Borrowed(&'a [u8]),
    Scratch(usize),
}

/// Segmented encoding of a value.
///
/// Headers and length words live in `scratch`; string and bytes payloads can be
/// borrowed from the source value.
#[derive(Debug, Default)]
pub struct ValueSegments<'a> {
    scratch: Vec<[u8; 8]>,
    segments: Vec<ValueSegment<'a>>,
}

impl<'a> ValueSegments<'a> {
    pub fn segments(&self) -> &[ValueSegment<'a>] {
        &self.segments
    }

    pub fn scratch(&self, index: usize) -> &[u8; 8] {
        &self.scratch[index]
    }

    pub fn len(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| match segment {
                ValueSegment::Borrowed(bytes) => bytes.len(),
                ValueSegment::Scratch(_) => 8,
            })
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        for segment in &self.segments {
            match segment {
                ValueSegment::Borrowed(bytes) => out.extend_from_slice(bytes),
                ValueSegment::Scratch(index) => out.extend_from_slice(&self.scratch[*index]),
            }
        }
        out
    }

    fn push_scratch_word(&mut self, word: u64) {
        let index = self.scratch.len();
        self.scratch.push(word.to_le_bytes());
        self.segments.push(ValueSegment::Scratch(index));
    }

    fn push_borrowed(&mut self, bytes: &'a [u8]) {
        if !bytes.is_empty() {
            self.segments.push(ValueSegment::Borrowed(bytes));
        }
    }
}

/// Encodes a single owned `Value` with the default persistence-safe options.
pub fn encode_value(value: &Value, out: &mut Vec<u8>) -> Result<(), ValueCodecError> {
    encode_value_with_options(value, out, ValueCodecOptions::default())
}

/// Encodes a single owned `Value`.
///
/// Immediate values are emitted as a little-endian `Value` word when they are
/// valid for the selected options. Heap values and named symbols are emitted as
/// structural extended records.
pub fn encode_value_with_options(
    value: &Value,
    out: &mut Vec<u8>,
    options: ValueCodecOptions,
) -> Result<(), ValueCodecError> {
    encode_value_to_sink(value, out, options)
}

/// Encodes a single owned `Value` into a caller-provided sink.
pub fn encode_value_to_sink<S: ValueSink>(
    value: &Value,
    sink: &mut S,
    options: ValueCodecOptions,
) -> Result<(), ValueCodecError> {
    match value.as_value_ref() {
        ValueRef::Nothing
        | ValueRef::Bool(_)
        | ValueRef::Int(_)
        | ValueRef::Float(_)
        | ValueRef::Identity(_) => {
            write_word(sink, value.raw_bits())?;
        }
        ValueRef::Symbol(symbol) => encode_symbol_value(TAG_SYMBOL, symbol, sink, options)?,
        ValueRef::ErrorCode(symbol) => encode_symbol_value(TAG_ERROR_CODE, symbol, sink, options)?,
        ValueRef::String(value) => {
            write_extended_header(sink, TAG_STRING, len_aux(value.len())?)?;
            sink.write_bytes(value.as_bytes())?;
        }
        ValueRef::Bytes(value) => {
            write_extended_header(sink, TAG_BYTES, len_aux(value.len())?)?;
            sink.write_bytes(value)?;
        }
        ValueRef::List(values) => {
            write_extended_header(sink, TAG_LIST, len_aux(values.len())?)?;
            for value in values {
                encode_value_to_sink(value, sink, options)?;
            }
        }
        ValueRef::Map(entries) => {
            write_extended_header(sink, TAG_MAP, len_aux(entries.len())?)?;
            for (key, value) in entries {
                encode_value_to_sink(key, sink, options)?;
                encode_value_to_sink(value, sink, options)?;
            }
        }
        ValueRef::Range { start, end } => {
            let flags = if end.is_some() { RANGE_HAS_END } else { 0 };
            write_extended_header(sink, TAG_RANGE, flags)?;
            encode_value_to_sink(start, sink, options)?;
            if let Some(end) = end {
                encode_value_to_sink(end, sink, options)?;
            }
        }
        ValueRef::Error {
            code,
            message,
            value,
        } => {
            let mut flags = 0;
            if message.is_some() {
                flags |= ERROR_HAS_MESSAGE;
            }
            if value.is_some() {
                flags |= ERROR_HAS_VALUE;
            }
            write_extended_header(sink, TAG_ERROR, flags)?;
            encode_symbol_value(TAG_ERROR_CODE, code, sink, options)?;
            if let Some(message) = message {
                write_blob(sink, message.as_bytes())?;
            }
            if let Some(value) = value {
                encode_value_to_sink(value, sink, options)?;
            }
        }
        ValueRef::Frob { delegate, value } => {
            write_extended_header(sink, TAG_FROB, 0)?;
            encode_value_to_sink(&Value::identity(delegate), sink, options)?;
            encode_value_to_sink(value, sink, options)?;
        }
        ValueRef::Capability(_) if options.allow_capabilities => {
            write_word(sink, value.raw_bits())?;
        }
        ValueRef::Capability(_) => return Err(ValueCodecError::CapabilityNotEncodable),
    }
    Ok(())
}

/// Encodes a value as borrowed byte segments with default persistence-safe
/// options.
pub fn encode_value_segments(value: &Value) -> Result<ValueSegments<'_>, ValueCodecError> {
    encode_value_segments_with_options(value, ValueCodecOptions::default())
}

/// Encodes a value as borrowed byte segments.
pub fn encode_value_segments_with_options<'a>(
    value: &'a Value,
    options: ValueCodecOptions,
) -> Result<ValueSegments<'a>, ValueCodecError> {
    let mut segments = ValueSegments::default();
    encode_value_to_segments(value, &mut segments, options)?;
    Ok(segments)
}

/// Decodes one value with the default persistence-safe options, returning the
/// value and the number of bytes consumed.
pub fn decode_value(bytes: &[u8]) -> Result<(Value, usize), ValueCodecError> {
    decode_value_with_options(bytes, ValueCodecOptions::default())
}

/// Decodes one value and rejects trailing bytes.
pub fn decode_value_exact(bytes: &[u8]) -> Result<Value, ValueCodecError> {
    decode_value_exact_with_options(bytes, ValueCodecOptions::default())
}

/// Decodes one value with explicit options, returning the value and the number
/// of bytes consumed.
pub fn decode_value_with_options(
    bytes: &[u8],
    options: ValueCodecOptions,
) -> Result<(Value, usize), ValueCodecError> {
    let mut reader = ValueReader::new(bytes, options);
    let value = reader.read_value()?;
    Ok((value, reader.offset()))
}

/// Decodes one value with explicit options and rejects trailing bytes.
pub fn decode_value_exact_with_options(
    bytes: &[u8],
    options: ValueCodecOptions,
) -> Result<Value, ValueCodecError> {
    let mut reader = ValueReader::new(bytes, options);
    let value = reader.read_value()?;
    reader.expect_end()?;
    Ok(value)
}

fn encode_symbol_value(
    tag: u8,
    symbol: Symbol,
    sink: &mut impl ValueSink,
    options: ValueCodecOptions,
) -> Result<(), ValueCodecError> {
    match options.symbol_encoding {
        SymbolEncoding::Id => write_word(sink, Value::pack(tag, symbol.id() as u64).raw_bits())?,
        SymbolEncoding::Name => {
            let name = symbol
                .name()
                .ok_or(ValueCodecError::UnnamedSymbol(symbol.id()))?;
            write_extended_header(sink, tag, len_aux(name.len())?)?;
            sink.write_bytes(name.as_bytes())?;
        }
    }
    Ok(())
}

fn encode_value_to_segments<'a>(
    value: &'a Value,
    segments: &mut ValueSegments<'a>,
    options: ValueCodecOptions,
) -> Result<(), ValueCodecError> {
    match value.as_value_ref() {
        ValueRef::Nothing
        | ValueRef::Bool(_)
        | ValueRef::Int(_)
        | ValueRef::Float(_)
        | ValueRef::Identity(_) => {
            segments.push_scratch_word(value.raw_bits());
        }
        ValueRef::Symbol(symbol) => {
            encode_symbol_to_segments(TAG_SYMBOL, symbol, segments, options)?;
        }
        ValueRef::ErrorCode(symbol) => {
            encode_symbol_to_segments(TAG_ERROR_CODE, symbol, segments, options)?;
        }
        ValueRef::String(value) => {
            segments.push_scratch_word(extended_header_word(TAG_STRING, len_aux(value.len())?));
            segments.push_borrowed(value.as_bytes());
        }
        ValueRef::Bytes(value) => {
            segments.push_scratch_word(extended_header_word(TAG_BYTES, len_aux(value.len())?));
            segments.push_borrowed(value);
        }
        ValueRef::List(values) => {
            segments.push_scratch_word(extended_header_word(TAG_LIST, len_aux(values.len())?));
            for value in values {
                encode_value_to_segments(value, segments, options)?;
            }
        }
        ValueRef::Map(entries) => {
            segments.push_scratch_word(extended_header_word(TAG_MAP, len_aux(entries.len())?));
            for (key, value) in entries {
                encode_value_to_segments(key, segments, options)?;
                encode_value_to_segments(value, segments, options)?;
            }
        }
        ValueRef::Range { start, end } => {
            let flags = if end.is_some() { RANGE_HAS_END } else { 0 };
            segments.push_scratch_word(extended_header_word(TAG_RANGE, flags));
            encode_value_to_segments(start, segments, options)?;
            if let Some(end) = end {
                encode_value_to_segments(end, segments, options)?;
            }
        }
        ValueRef::Error {
            code,
            message,
            value,
        } => {
            let mut flags = 0;
            if message.is_some() {
                flags |= ERROR_HAS_MESSAGE;
            }
            if value.is_some() {
                flags |= ERROR_HAS_VALUE;
            }
            segments.push_scratch_word(extended_header_word(TAG_ERROR, flags));
            encode_symbol_to_segments(TAG_ERROR_CODE, code, segments, options)?;
            if let Some(message) = message {
                let len = u64::try_from(message.len())
                    .map_err(|_| ValueCodecError::LengthTooLarge(message.len()))?;
                segments.push_scratch_word(len);
                segments.push_borrowed(message.as_bytes());
            }
            if let Some(value) = value {
                encode_value_to_segments(value, segments, options)?;
            }
        }
        ValueRef::Frob { delegate, value } => {
            segments.push_scratch_word(extended_header_word(TAG_FROB, 0));
            segments.push_scratch_word(Value::identity(delegate).raw_bits());
            encode_value_to_segments(value, segments, options)?;
        }
        ValueRef::Capability(_) if options.allow_capabilities => {
            segments.push_scratch_word(value.raw_bits());
        }
        ValueRef::Capability(_) => return Err(ValueCodecError::CapabilityNotEncodable),
    }
    Ok(())
}

fn encode_symbol_to_segments<'a>(
    tag: u8,
    symbol: Symbol,
    segments: &mut ValueSegments<'a>,
    options: ValueCodecOptions,
) -> Result<(), ValueCodecError> {
    match options.symbol_encoding {
        SymbolEncoding::Id => {
            segments.push_scratch_word(Value::pack(tag, symbol.id() as u64).raw_bits());
        }
        SymbolEncoding::Name => {
            let name = symbol
                .name()
                .ok_or(ValueCodecError::UnnamedSymbol(symbol.id()))?;
            segments.push_scratch_word(extended_header_word(tag, len_aux(name.len())?));
            segments.push_borrowed(name.as_bytes());
        }
    }
    Ok(())
}

fn write_blob(sink: &mut impl ValueSink, bytes: &[u8]) -> Result<(), ValueCodecError> {
    let len =
        u64::try_from(bytes.len()).map_err(|_| ValueCodecError::LengthTooLarge(bytes.len()))?;
    sink.write_bytes(&len.to_le_bytes())?;
    sink.write_bytes(bytes)?;
    Ok(())
}

fn write_extended_header(
    sink: &mut impl ValueSink,
    kind: u8,
    aux: u64,
) -> Result<(), ValueCodecError> {
    debug_assert!(aux <= EXT_AUX_MASK);
    write_word(sink, extended_header_word(kind, aux))
}

fn write_word(sink: &mut impl ValueSink, value: u64) -> Result<(), ValueCodecError> {
    sink.write_bytes(&value.to_le_bytes())
}

fn extended_header_word(kind: u8, aux: u64) -> u64 {
    debug_assert!(aux <= EXT_AUX_MASK);
    ((EXTENDED_TAG as u64) << TAG_SHIFT) | ((kind as u64) << EXT_KIND_SHIFT) | aux
}

fn len_aux(len: usize) -> Result<u64, ValueCodecError> {
    let len = u64::try_from(len).map_err(|_| ValueCodecError::LengthTooLarge(len))?;
    if len <= EXT_AUX_MASK {
        Ok(len)
    } else {
        Err(ValueCodecError::LengthTooLarge(usize::MAX))
    }
}

struct ValueReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    options: ValueCodecOptions,
}

impl<'a> ValueReader<'a> {
    fn new(bytes: &'a [u8], options: ValueCodecOptions) -> Self {
        Self {
            bytes,
            offset: 0,
            options,
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn expect_end(&self) -> Result<(), ValueCodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ValueCodecError::TrailingBytes(
                self.bytes.len() - self.offset,
            ))
        }
    }

    fn read_value(&mut self) -> Result<Value, ValueCodecError> {
        let word = self.read_word()?;
        let tag = word_tag(word);
        if tag != EXTENDED_TAG {
            return decode_inline_word(word, self.options);
        }

        let kind = ((word >> EXT_KIND_SHIFT) & 0xff) as u8;
        let aux = word & EXT_AUX_MASK;
        match kind {
            TAG_SYMBOL => self.read_named_symbol(TAG_SYMBOL, aux, Value::symbol),
            TAG_ERROR_CODE => self.read_named_symbol(TAG_ERROR_CODE, aux, Value::error_code),
            TAG_STRING => Ok(Value::string(self.read_string_payload(aux)?)),
            TAG_BYTES => Ok(Value::bytes(self.read_blob_payload(aux)?)),
            TAG_LIST => {
                let count = self.read_count(aux)?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value()?);
                }
                Ok(Value::list(values))
            }
            TAG_MAP => {
                let count = self.read_count(aux)?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push((self.read_value()?, self.read_value()?));
                }
                Ok(Value::map(entries))
            }
            TAG_RANGE => {
                if aux & !RANGE_HAS_END != 0 {
                    return Err(ValueCodecError::InvalidExtendedAux { kind, aux });
                }
                let start = self.read_value()?;
                let end = if aux & RANGE_HAS_END != 0 {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Ok(Value::range(start, end))
            }
            TAG_ERROR => {
                if aux & !(ERROR_HAS_MESSAGE | ERROR_HAS_VALUE) != 0 {
                    return Err(ValueCodecError::InvalidExtendedAux { kind, aux });
                }
                let code = self.read_value()?.as_error_code().ok_or_else(|| {
                    ValueCodecError::InvalidValue("rich error code is not an error code".to_owned())
                })?;
                let message = if aux & ERROR_HAS_MESSAGE != 0 {
                    Some(self.read_len_prefixed_string()?)
                } else {
                    None
                };
                let value = if aux & ERROR_HAS_VALUE != 0 {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Ok(Value::error(code, message, value))
            }
            TAG_FROB => {
                if aux != 0 {
                    return Err(ValueCodecError::InvalidExtendedAux { kind, aux });
                }
                let delegate = self.read_value()?.as_identity().ok_or_else(|| {
                    ValueCodecError::InvalidValue("frob delegate is not an identity".to_owned())
                })?;
                let value = self.read_value()?;
                Ok(Value::frob(delegate, value))
            }
            TAG_CAPABILITY => Err(ValueCodecError::CapabilityNotDecodable),
            TAG_NOTHING | TAG_BOOL | TAG_INT | TAG_FLOAT | TAG_IDENTITY => {
                Err(ValueCodecError::InvalidExtendedAux { kind, aux })
            }
            tag => Err(ValueCodecError::UnknownValueTag(tag)),
        }
    }

    fn read_named_symbol(
        &mut self,
        kind: u8,
        len: u64,
        constructor: fn(Symbol) -> Value,
    ) -> Result<Value, ValueCodecError> {
        if self.options.symbol_encoding != SymbolEncoding::Name {
            return Err(ValueCodecError::InvalidExtendedAux { kind, aux: len });
        }
        Ok(constructor(Symbol::intern(&self.read_string_payload(len)?)))
    }

    fn read_string_payload(&mut self, len: u64) -> Result<String, ValueCodecError> {
        String::from_utf8(self.read_blob_payload(len)?)
            .map_err(|error| ValueCodecError::InvalidUtf8(error.to_string()))
    }

    fn read_blob_payload(&mut self, len: u64) -> Result<Vec<u8>, ValueCodecError> {
        let len = self.len_to_usize(len)?;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_len_prefixed_string(&mut self) -> Result<String, ValueCodecError> {
        let len = self.read_word()?;
        String::from_utf8(self.read_blob_payload(len)?)
            .map_err(|error| ValueCodecError::InvalidUtf8(error.to_string()))
    }

    fn read_count(&self, count: u64) -> Result<usize, ValueCodecError> {
        let count = self.len_to_usize(count)?;
        let minimum_bytes = count
            .checked_mul(8)
            .ok_or(ValueCodecError::OffsetOverflow)?;
        if self.bytes.len().saturating_sub(self.offset) < minimum_bytes {
            return Err(ValueCodecError::UnexpectedEnd {
                needed: minimum_bytes,
                offset: self.offset,
                len: self.bytes.len(),
            });
        }
        Ok(count)
    }

    fn len_to_usize(&self, len: u64) -> Result<usize, ValueCodecError> {
        usize::try_from(len).map_err(|_| ValueCodecError::LengthTooLarge(usize::MAX))
    }

    fn read_word(&mut self) -> Result<u64, ValueCodecError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ValueCodecError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ValueCodecError::OffsetOverflow)?;
        if end > self.bytes.len() {
            return Err(ValueCodecError::UnexpectedEnd {
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

fn decode_inline_word(word: u64, options: ValueCodecOptions) -> Result<Value, ValueCodecError> {
    let tag = word_tag(word);
    let payload = word & PAYLOAD_MASK;
    match tag {
        TAG_NOTHING => {
            if payload == 0 {
                Ok(Value(word))
            } else {
                Err(ValueCodecError::InvalidValue(
                    "nothing value has non-zero payload".to_owned(),
                ))
            }
        }
        TAG_BOOL => match payload {
            0 | 1 => Ok(Value(word)),
            payload => Err(ValueCodecError::InvalidBoolPayload(payload)),
        },
        TAG_INT => Ok(Value(word)),
        TAG_FLOAT => {
            if payload <= u32::MAX as u64 {
                Ok(Value(word))
            } else {
                Err(ValueCodecError::InvalidFloatPayload(payload))
            }
        }
        TAG_IDENTITY => Identity::new(payload)
            .map(Value::identity)
            .ok_or(ValueCodecError::InvalidIdentity(payload)),
        TAG_SYMBOL => {
            if options.symbol_encoding != SymbolEncoding::Id {
                return Err(ValueCodecError::InvalidValue(
                    "inline symbol id is disabled by codec options".to_owned(),
                ));
            }
            if payload <= u32::MAX as u64 {
                Ok(Value::symbol(Symbol::from_id(payload as u32)))
            } else {
                Err(ValueCodecError::InvalidSymbolPayload(payload))
            }
        }
        TAG_ERROR_CODE => {
            if options.symbol_encoding != SymbolEncoding::Id {
                return Err(ValueCodecError::InvalidValue(
                    "inline error code id is disabled by codec options".to_owned(),
                ));
            }
            if payload <= u32::MAX as u64 {
                Ok(Value::error_code(Symbol::from_id(payload as u32)))
            } else {
                Err(ValueCodecError::InvalidSymbolPayload(payload))
            }
        }
        TAG_STRING | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_RANGE | TAG_ERROR | TAG_FROB => {
            Err(ValueCodecError::InlineHeapValue(tag))
        }
        TAG_CAPABILITY => {
            if !options.allow_capabilities {
                return Err(ValueCodecError::CapabilityNotDecodable);
            }
            CapabilityId::new(payload)
                .map(Value::capability)
                .ok_or(ValueCodecError::InvalidCapabilityPayload(payload))
        }
        tag => Err(ValueCodecError::UnknownValueTag(tag)),
    }
}

#[inline(always)]
fn word_tag(word: u64) -> u8 {
    (word >> TAG_SHIFT) as u8
}
