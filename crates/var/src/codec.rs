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

use crate::{Identity, Symbol, Value, ValueKind, ValueRef};
use std::fmt;

/// Error returned by the owned value codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueCodecError {
    LengthTooLarge(usize),
    UnnamedSymbol(u32),
    CapabilityNotEncodable,
    CapabilityNotDecodable,
    UnexpectedEnd {
        needed: usize,
        offset: usize,
        len: usize,
    },
    TrailingBytes(usize),
    InvalidBool(u8),
    InvalidUtf8(String),
    InvalidIdentity(u64),
    InvalidValue(String),
    UnknownValueTag(u8),
    OffsetOverflow,
}

impl fmt::Display for ValueCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthTooLarge(len) => write!(f, "length {len} exceeds u32"),
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
            Self::InvalidBool(value) => write!(f, "invalid boolean byte {value}"),
            Self::InvalidUtf8(error) => write!(f, "invalid utf-8: {error}"),
            Self::InvalidIdentity(raw) => write!(f, "identity {raw} is out of range"),
            Self::InvalidValue(error) => write!(f, "invalid value: {error}"),
            Self::UnknownValueTag(tag) => write!(f, "unknown value kind tag {tag}"),
            Self::OffsetOverflow => f.write_str("value record offset overflow"),
        }
    }
}

impl std::error::Error for ValueCodecError {}

/// Encodes a single owned `Value` into the canonical persistence format.
///
/// The format is structural: heap values are encoded by content rather than by
/// process-local pointer bits.
pub fn encode_value(value: &Value, out: &mut Vec<u8>) -> Result<(), ValueCodecError> {
    let value_ref = value.as_value_ref();
    out.push(value_ref.kind() as u8);
    match value_ref {
        ValueRef::Nothing => {}
        ValueRef::Bool(value) => out.push(value as u8),
        ValueRef::Int(value) => write_i64(out, value),
        ValueRef::Float(value) => write_u64(out, value.to_bits()),
        ValueRef::Identity(identity) => write_identity(out, identity),
        ValueRef::Symbol(symbol) | ValueRef::ErrorCode(symbol) => write_symbol(out, symbol)?,
        ValueRef::String(value) => write_string(out, value)?,
        ValueRef::Bytes(value) => write_bytes(out, value)?,
        ValueRef::List(values) => {
            write_u32(out, values.len())?;
            for value in values {
                encode_value(value, out)?;
            }
        }
        ValueRef::Map(entries) => {
            write_u32(out, entries.len())?;
            for (key, value) in entries {
                encode_value(key, out)?;
                encode_value(value, out)?;
            }
        }
        ValueRef::Range { start, end } => {
            encode_value(start, out)?;
            match end {
                Some(end) => {
                    out.push(1);
                    encode_value(end, out)?;
                }
                None => out.push(0),
            }
        }
        ValueRef::Error {
            code,
            message,
            value,
        } => {
            write_symbol(out, code)?;
            write_optional_string(out, message)?;
            match value {
                Some(value) => {
                    out.push(1);
                    encode_value(value, out)?;
                }
                None => out.push(0),
            }
        }
        ValueRef::Capability(_) => return Err(ValueCodecError::CapabilityNotEncodable),
    }
    Ok(())
}

/// Decodes one value from the beginning of `bytes`, returning the value and the
/// number of bytes consumed.
pub fn decode_value(bytes: &[u8]) -> Result<(Value, usize), ValueCodecError> {
    let mut reader = ValueReader::new(bytes);
    let value = reader.read_value()?;
    Ok((value, reader.offset()))
}

/// Decodes one value and rejects trailing bytes.
pub fn decode_value_exact(bytes: &[u8]) -> Result<Value, ValueCodecError> {
    let mut reader = ValueReader::new(bytes);
    let value = reader.read_value()?;
    reader.expect_end()?;
    Ok(value)
}

fn write_identity(out: &mut Vec<u8>, identity: Identity) {
    write_u64(out, identity.raw());
}

fn write_symbol(out: &mut Vec<u8>, symbol: Symbol) -> Result<(), ValueCodecError> {
    let name = symbol
        .name()
        .ok_or(ValueCodecError::UnnamedSymbol(symbol.id()))?;
    write_string(out, name)
}

fn write_optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), ValueCodecError> {
    match value {
        Some(value) => {
            out.push(1);
            write_string(out, value)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ValueCodecError> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ValueCodecError> {
    write_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn write_u32(out: &mut Vec<u8>, value: usize) -> Result<(), ValueCodecError> {
    let value = u32::try_from(value).map_err(|_| ValueCodecError::LengthTooLarge(value))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct ValueReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ValueReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
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
        let kind = self.read_u8()?;
        Ok(match kind {
            tag if tag == ValueKind::Nothing as u8 => Value::nothing(),
            tag if tag == ValueKind::Bool as u8 => Value::bool(self.read_bool()?),
            tag if tag == ValueKind::Int as u8 => Value::int(self.read_i64()?)
                .map_err(|error| ValueCodecError::InvalidValue(format!("{error:?}")))?,
            tag if tag == ValueKind::Float as u8 => Value::float(f64::from_bits(self.read_u64()?)),
            tag if tag == ValueKind::Identity as u8 => Value::identity(self.read_identity()?),
            tag if tag == ValueKind::Symbol as u8 => Value::symbol(self.read_symbol()?),
            tag if tag == ValueKind::ErrorCode as u8 => Value::error_code(self.read_symbol()?),
            tag if tag == ValueKind::String as u8 => Value::string(self.read_string()?),
            tag if tag == ValueKind::Bytes as u8 => Value::bytes(self.read_bytes()?),
            tag if tag == ValueKind::List as u8 => {
                let count = self.read_len()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_value()?);
                }
                Value::list(values)
            }
            tag if tag == ValueKind::Map as u8 => {
                let count = self.read_len()?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push((self.read_value()?, self.read_value()?));
                }
                Value::map(entries)
            }
            tag if tag == ValueKind::Range as u8 => {
                let start = self.read_value()?;
                let end = if self.read_bool()? {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Value::range(start, end)
            }
            tag if tag == ValueKind::Error as u8 => {
                let code = self.read_symbol()?;
                let message = self.read_optional_string()?;
                let value = if self.read_bool()? {
                    Some(self.read_value()?)
                } else {
                    None
                };
                Value::error(code, message, value)
            }
            tag if tag == ValueKind::Capability as u8 => {
                return Err(ValueCodecError::CapabilityNotDecodable);
            }
            tag => return Err(ValueCodecError::UnknownValueTag(tag)),
        })
    }

    fn read_identity(&mut self) -> Result<Identity, ValueCodecError> {
        let raw = self.read_u64()?;
        Identity::new(raw).ok_or(ValueCodecError::InvalidIdentity(raw))
    }

    fn read_symbol(&mut self) -> Result<Symbol, ValueCodecError> {
        Ok(Symbol::intern(&self.read_string()?))
    }

    fn read_optional_string(&mut self) -> Result<Option<String>, ValueCodecError> {
        if self.read_bool()? {
            Ok(Some(self.read_string()?))
        } else {
            Ok(None)
        }
    }

    fn read_string(&mut self) -> Result<String, ValueCodecError> {
        String::from_utf8(self.read_bytes()?)
            .map_err(|error| ValueCodecError::InvalidUtf8(error.to_string()))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, ValueCodecError> {
        let len = self.read_len()?;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_bool(&mut self) -> Result<bool, ValueCodecError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ValueCodecError::InvalidBool(value)),
        }
    }

    fn read_len(&mut self) -> Result<usize, ValueCodecError> {
        Ok(self.read_u32()? as usize)
    }

    fn read_u8(&mut self) -> Result<u8, ValueCodecError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, ValueCodecError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64, ValueCodecError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> Result<i64, ValueCodecError> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
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
