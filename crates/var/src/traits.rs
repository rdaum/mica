use crate::heap::HeapValue;
use crate::value::{TAG_BYTES, TAG_LIST, TAG_MAP, TAG_STRING, Value, ValueKind, normalize_f32};
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
            ValueKind::Identity | ValueKind::Symbol => {
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
            | (ValueKind::Symbol, ValueKind::Symbol) => self.payload() == other.payload(),
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
            ValueKind::Identity | ValueKind::Symbol => self.payload().cmp(&other.payload()),
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
            | ValueKind::Symbol => {
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
            ValueKind::Identity => write!(f, "${}", self.as_identity().unwrap().raw()),
            ValueKind::Symbol => match self.as_symbol().unwrap().name() {
                Some(name) => write!(f, ":{name}"),
                None => write!(f, ":#{}", self.as_symbol().unwrap().id()),
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
            ValueKind::Identity => write!(f, "${}", self.as_identity().unwrap().raw()),
            ValueKind::Symbol => match self.as_symbol().unwrap().name() {
                Some(name) => write!(f, ":{name}"),
                None => write!(f, ":#{}", self.as_symbol().unwrap().id()),
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
    let _ = (TAG_STRING, TAG_BYTES, TAG_LIST, TAG_MAP);
};
