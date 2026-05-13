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
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::heap::HeapValue;
use crate::value::{
    TAG_BYTES, TAG_ERROR, TAG_LIST, TAG_MAP, TAG_RANGE, TAG_STRING, Value, ValueKind, normalize_f32,
};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

impl Value {
    pub fn ordered_key_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_ordered(&mut out);
        out
    }

    pub fn encode_ordered(&self, out: &mut Vec<u8>) {
        out.push(self.tag());
        match self.kind() {
            ValueKind::Nothing => {}
            ValueKind::Bool => out.push(self.payload() as u8),
            ValueKind::Int => {
                let normalized = self.as_int().unwrap() ^ i64::MIN;
                out.extend_from_slice(&normalized.to_be_bytes());
            }
            ValueKind::Float => {
                out.extend_from_slice(
                    &ordered_f32_bits(f32::from_bits(self.payload() as u32)).to_be_bytes(),
                );
            }
            ValueKind::Identity | ValueKind::Symbol | ValueKind::ErrorCode => {
                out.extend_from_slice(&self.payload().to_be_bytes());
            }
            ValueKind::String => {
                let _ = self.with_str(|value| encode_bytes_terminated(value.as_bytes(), out));
            }
            ValueKind::Bytes => {
                let _ = self.with_bytes(|value| encode_bytes_terminated(value, out));
            }
            ValueKind::List => {
                let _ = self.with_list(|values| {
                    for value in values {
                        out.push(1);
                        value.encode_ordered(out);
                    }
                    out.push(0);
                });
            }
            ValueKind::Map => {
                let _ = self.with_map(|entries| {
                    for (key, value) in entries {
                        out.push(1);
                        key.encode_ordered(out);
                        value.encode_ordered(out);
                    }
                    out.push(0);
                });
            }
            ValueKind::Range => {
                let _ = self.with_range(|start, end| {
                    start.encode_ordered(out);
                    match end {
                        Some(end) => {
                            out.push(1);
                            end.encode_ordered(out);
                        }
                        None => out.push(0),
                    }
                });
            }
            ValueKind::Error => {
                let _ = self.with_error(|error| {
                    out.extend_from_slice(&(error.code().id() as u64).to_be_bytes());
                    match error.message() {
                        Some(message) => {
                            out.push(1);
                            encode_bytes_terminated(message.as_bytes(), out);
                        }
                        None => out.push(0),
                    }
                    match error.value() {
                        Some(value) => {
                            out.push(1);
                            value.encode_ordered(out);
                        }
                        None => out.push(0),
                    }
                });
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self.kind(), other.kind()) {
            (ValueKind::Nothing, ValueKind::Nothing) => true,
            (ValueKind::Bool, ValueKind::Bool)
            | (ValueKind::Int, ValueKind::Int)
            | (ValueKind::Float, ValueKind::Float)
            | (ValueKind::Identity, ValueKind::Identity)
            | (ValueKind::Symbol, ValueKind::Symbol)
            | (ValueKind::ErrorCode, ValueKind::ErrorCode) => self.payload() == other.payload(),
            (ValueKind::String, ValueKind::String) => self
                .with_str(|left| other.with_str(|right| left == right).unwrap())
                .unwrap(),
            (ValueKind::Bytes, ValueKind::Bytes) => self
                .with_bytes(|left| other.with_bytes(|right| left == right).unwrap())
                .unwrap(),
            (ValueKind::List, ValueKind::List) => self
                .with_list(|left| other.with_list(|right| left == right).unwrap())
                .unwrap(),
            (ValueKind::Map, ValueKind::Map) => self
                .with_map(|left| other.with_map(|right| left == right).unwrap())
                .unwrap(),
            (ValueKind::Range, ValueKind::Range) => self
                .with_range(|left_start, left_end| {
                    other
                        .with_range(|right_start, right_end| {
                            left_start == right_start && left_end == right_end
                        })
                        .unwrap()
                })
                .unwrap(),
            (ValueKind::Error, ValueKind::Error) => self
                .with_error(|left| other.with_error(|right| left == right).unwrap())
                .unwrap(),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let left_kind = self.kind();
        let right_kind = other.kind();
        if left_kind != right_kind {
            return left_kind.cmp(&right_kind);
        }

        match left_kind {
            ValueKind::Nothing => Ordering::Equal,
            ValueKind::Bool => self.as_bool().cmp(&other.as_bool()),
            ValueKind::Int => self.as_int().cmp(&other.as_int()),
            ValueKind::Float => {
                let left = f32::from_bits(self.payload() as u32);
                let right = f32::from_bits(other.payload() as u32);
                left.total_cmp(&right)
            }
            ValueKind::Identity | ValueKind::Symbol | ValueKind::ErrorCode => {
                self.payload().cmp(&other.payload())
            }
            ValueKind::String => self
                .with_str(|left| other.with_str(|right| left.cmp(right)).unwrap())
                .unwrap(),
            ValueKind::Bytes => self
                .with_bytes(|left| other.with_bytes(|right| left.cmp(right)).unwrap())
                .unwrap(),
            ValueKind::List => self
                .with_list(|left| other.with_list(|right| left.cmp(right)).unwrap())
                .unwrap(),
            ValueKind::Map => self
                .with_map(|left| other.with_map(|right| left.cmp(right)).unwrap())
                .unwrap(),
            ValueKind::Range => self
                .with_range(|left_start, left_end| {
                    other
                        .with_range(|right_start, right_end| {
                            left_start
                                .cmp(right_start)
                                .then_with(|| left_end.cmp(&right_end))
                        })
                        .unwrap()
                })
                .unwrap(),
            ValueKind::Error => self
                .with_error(|left| other.with_error(|right| left.cmp(right)).unwrap())
                .unwrap(),
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind().hash(state);
        match self.kind() {
            ValueKind::Nothing => {}
            ValueKind::Bool
            | ValueKind::Int
            | ValueKind::Float
            | ValueKind::Identity
            | ValueKind::Symbol
            | ValueKind::ErrorCode => {
                self.payload().hash(state);
            }
            ValueKind::String => {
                let _ = self.with_str(|value| value.hash(state));
            }
            ValueKind::Bytes => {
                let _ = self.with_bytes(|value| value.hash(state));
            }
            ValueKind::List => {
                let _ = self.with_list(|values| values.hash(state));
            }
            ValueKind::Map => {
                let _ = self.with_map(|entries| entries.hash(state));
            }
            ValueKind::Range => {
                let _ = self.with_range(|start, end| {
                    start.hash(state);
                    end.hash(state);
                });
            }
            ValueKind::Error => {
                let _ = self.with_error(|error| error.hash(state));
            }
        };
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            ValueKind::Nothing => f.write_str("nothing"),
            ValueKind::Bool => write!(f, "{:?}", self.as_bool().unwrap()),
            ValueKind::Int => write!(f, "{:?}", self.as_int().unwrap()),
            ValueKind::Float => write!(f, "{:?}", self.as_float().unwrap()),
            ValueKind::Identity => write!(f, "#{}", self.as_identity().unwrap().raw()),
            ValueKind::Symbol => match self.as_symbol().unwrap().name() {
                Some(name) => write!(f, ":{name}"),
                None => write!(f, ":#{}", self.as_symbol().unwrap().id()),
            },
            ValueKind::ErrorCode => match self.as_error_code().unwrap().name() {
                Some(name) => f.write_str(name),
                None => write!(f, "E_#{}", self.as_error_code().unwrap().id()),
            },
            ValueKind::String => self.with_str(|value| write!(f, "{value:?}")).unwrap(),
            ValueKind::Bytes => self
                .with_bytes(|value| {
                    f.write_str("#bytes(\"")?;
                    write_hex_bytes(value, f)?;
                    f.write_str("\")")
                })
                .unwrap(),
            ValueKind::List => self
                .with_list(|values| f.debug_list().entries(values).finish())
                .unwrap(),
            ValueKind::Map => self
                .with_map(|entries| {
                    let mut map = f.debug_map();
                    for (key, value) in entries {
                        map.entry(key, value);
                    }
                    map.finish()
                })
                .unwrap(),
            ValueKind::Range => self
                .with_range(|start, end| write_range(start, end, f))
                .unwrap(),
            ValueKind::Error => self
                .with_error(|error| write_error_value(error, f))
                .unwrap(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            ValueKind::Nothing => f.write_str("nothing"),
            ValueKind::Bool => write!(f, "{}", self.as_bool().unwrap()),
            ValueKind::Int => write!(f, "{}", self.as_int().unwrap()),
            ValueKind::Float => write!(f, "{}", self.as_float().unwrap()),
            ValueKind::Identity => write!(f, "#{}", self.as_identity().unwrap().raw()),
            ValueKind::Symbol => match self.as_symbol().unwrap().name() {
                Some(name) => write!(f, ":{name}"),
                None => write!(f, ":#{}", self.as_symbol().unwrap().id()),
            },
            ValueKind::ErrorCode => match self.as_error_code().unwrap().name() {
                Some(name) => f.write_str(name),
                None => write!(f, "E_#{}", self.as_error_code().unwrap().id()),
            },
            ValueKind::String => self.with_str(|value| f.write_str(value)).unwrap(),
            ValueKind::Bytes => self
                .with_bytes(|value| {
                    f.write_str("#bytes(\"")?;
                    write_hex_bytes(value, f)?;
                    f.write_str("\")")
                })
                .unwrap(),
            ValueKind::List => self
                .with_list(|values| {
                    f.write_str("{")?;
                    for (index, value) in values.iter().enumerate() {
                        if index != 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{value}")?;
                    }
                    f.write_str("}")
                })
                .unwrap(),
            ValueKind::Map => self
                .with_map(|entries| {
                    f.write_str("[")?;
                    for (index, (key, value)) in entries.iter().enumerate() {
                        if index != 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{key}: {value}")?;
                    }
                    f.write_str("]")
                })
                .unwrap(),
            ValueKind::Range => self
                .with_range(|start, end| write_range(start, end, f))
                .unwrap(),
            ValueKind::Error => self
                .with_error(|error| write_error_value(error, f))
                .unwrap(),
        }
    }
}

#[inline(always)]
fn ordered_f32_bits(value: f32) -> u32 {
    let bits = normalize_f32(value).to_bits();
    if (bits & 0x8000_0000) != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

fn write_hex_bytes(bytes: &[u8], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        f.write_str("\\x")?;
        f.write_str(std::str::from_utf8(&[HEX[(byte >> 4) as usize]]).unwrap())?;
        f.write_str(std::str::from_utf8(&[HEX[(byte & 0x0f) as usize]]).unwrap())?;
    }
    Ok(())
}

fn write_range(start: &Value, end: Option<&Value>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{start}..")?;
    match end {
        Some(end) => write!(f, "{end}"),
        None => f.write_str("_"),
    }
}

fn write_error_value(error: &crate::ErrorValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("error(")?;
    match error.code().name() {
        Some(name) => f.write_str(name)?,
        None => write!(f, "E_#{}", error.code().id())?,
    }
    if let Some(message) = error.message() {
        write!(f, ", {message:?}")?;
    }
    if let Some(value) = error.value() {
        if error.message().is_none() {
            f.write_str(", nothing")?;
        }
        write!(f, ", {value:?}")?;
    }
    f.write_str(")")
}

fn encode_bytes_terminated(bytes: &[u8], out: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            out.extend_from_slice(&[0, 0xff]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

const _: () = {
    fn _heap_value_is_used(heap: &HeapValue) -> u8 {
        heap.tag()
    }
    let _ = _heap_value_is_used;
    let _ = (
        TAG_STRING, TAG_BYTES, TAG_LIST, TAG_MAP, TAG_RANGE, TAG_ERROR,
    );
};
