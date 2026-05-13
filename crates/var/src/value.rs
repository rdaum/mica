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
use crate::symbol::Symbol;
use std::sync::Arc;

pub(crate) const TAG_SHIFT: u64 = 56;
pub(crate) const PAYLOAD_MASK: u64 = 0x00ff_ffff_ffff_ffff;

pub(crate) const TAG_NOTHING: u8 = 0;
pub(crate) const TAG_BOOL: u8 = 1;
pub(crate) const TAG_INT: u8 = 2;
pub(crate) const TAG_FLOAT: u8 = 3;
pub(crate) const TAG_IDENTITY: u8 = 4;
pub(crate) const TAG_SYMBOL: u8 = 5;
pub(crate) const TAG_ERROR_CODE: u8 = 6;
pub(crate) const TAG_STRING: u8 = 7;
pub(crate) const TAG_BYTES: u8 = 8;
pub(crate) const TAG_LIST: u8 = 9;
pub(crate) const TAG_MAP: u8 = 10;
pub(crate) const TAG_RANGE: u8 = 11;
pub(crate) const TAG_ERROR: u8 = 12;

pub(crate) const INT_BITS: u32 = 56;
pub(crate) const INT_MIN: i64 = -(1i64 << (INT_BITS - 1));
pub(crate) const INT_MAX: i64 = (1i64 << (INT_BITS - 1)) - 1;
pub(crate) const MAX_PAYLOAD: u64 = PAYLOAD_MASK;

/// A compact Mica value.
///
/// The layout is private. Use constructors and accessors rather than relying on
/// raw bits. The current representation is a pragmatic tagged word, not a final
/// commitment to this exact bit layout.
#[repr(transparent)]
pub struct Value(pub(crate) u64);

const _: () = assert!(std::mem::size_of::<Value>() == 8);
const _: () = assert!(std::mem::align_of::<Value>() == 8);

/// Stable entity identity payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Identity(u64);

impl Identity {
    pub const MAX: u64 = MAX_PAYLOAD;

    pub const fn new(raw: u64) -> Option<Self> {
        if raw <= Self::MAX {
            Some(Self(raw))
        } else {
            None
        }
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ErrorValue {
    code: Symbol,
    message: Option<Box<str>>,
    value: Option<Value>,
}

impl ErrorValue {
    pub fn new(code: Symbol, message: Option<impl Into<Box<str>>>, value: Option<Value>) -> Self {
        Self {
            code,
            message: message.map(Into::into),
            value,
        }
    }

    pub const fn code(&self) -> Symbol {
        self.code
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum ValueKind {
    Nothing = TAG_NOTHING,
    Bool = TAG_BOOL,
    Int = TAG_INT,
    Float = TAG_FLOAT,
    Identity = TAG_IDENTITY,
    Symbol = TAG_SYMBOL,
    ErrorCode = TAG_ERROR_CODE,
    String = TAG_STRING,
    Bytes = TAG_BYTES,
    List = TAG_LIST,
    Map = TAG_MAP,
    Range = TAG_RANGE,
    Error = TAG_ERROR,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueError {
    IntegerOutOfRange(i64),
    IdentityOutOfRange(u64),
    HeapPointerOutOfRange(usize),
}

impl Value {
    #[inline(always)]
    pub(crate) const fn pack(tag: u8, payload: u64) -> Self {
        Self(((tag as u64) << TAG_SHIFT) | (payload & PAYLOAD_MASK))
    }

    #[inline(always)]
    pub const fn nothing() -> Self {
        Self::pack(TAG_NOTHING, 0)
    }

    #[inline(always)]
    pub const fn bool(value: bool) -> Self {
        Self::pack(TAG_BOOL, value as u64)
    }

    #[inline(always)]
    pub const fn int(value: i64) -> Result<Self, ValueError> {
        if value < INT_MIN || value > INT_MAX {
            return Err(ValueError::IntegerOutOfRange(value));
        }
        Ok(Self::pack(TAG_INT, value as u64))
    }

    #[inline(always)]
    pub fn float(value: f64) -> Self {
        let value = normalize_f32(value as f32);
        Self::pack(TAG_FLOAT, value.to_bits() as u64)
    }

    #[inline(always)]
    pub const fn identity(identity: Identity) -> Self {
        Self::pack(TAG_IDENTITY, identity.raw())
    }

    #[inline(always)]
    pub const fn identity_raw(raw: u64) -> Result<Self, ValueError> {
        match Identity::new(raw) {
            Some(identity) => Ok(Self::identity(identity)),
            None => Err(ValueError::IdentityOutOfRange(raw)),
        }
    }

    #[inline(always)]
    pub const fn symbol(symbol: Symbol) -> Self {
        Self::pack(TAG_SYMBOL, symbol.id() as u64)
    }

    #[inline(always)]
    pub const fn error_code(symbol: Symbol) -> Self {
        Self::pack(TAG_ERROR_CODE, symbol.id() as u64)
    }

    pub fn error(code: Symbol, message: Option<impl Into<Box<str>>>, value: Option<Value>) -> Self {
        Self::heap(HeapValue::Error(ErrorValue::new(code, message, value)))
    }

    pub fn string(value: impl AsRef<str>) -> Self {
        Self::heap(HeapValue::String(value.as_ref().into()))
    }

    pub fn bytes(value: impl AsRef<[u8]>) -> Self {
        Self::heap(HeapValue::Bytes(value.as_ref().into()))
    }

    pub fn list(values: impl IntoIterator<Item = Value>) -> Self {
        Self::heap(HeapValue::List(
            values.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        ))
    }

    pub fn map(entries: impl IntoIterator<Item = (Value, Value)>) -> Self {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut canonical = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if let Some((last_key, last_value)) = canonical.last_mut()
                && last_key == &key
            {
                *last_value = value;
                continue;
            }
            canonical.push((key, value));
        }
        Self::heap(HeapValue::Map(canonical.into_boxed_slice()))
    }

    pub fn range(start: Value, end: Option<Value>) -> Self {
        Self::heap(HeapValue::Range { start, end })
    }

    #[inline(always)]
    pub const fn raw_bits(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    pub const fn kind(&self) -> ValueKind {
        match self.tag() {
            TAG_NOTHING => ValueKind::Nothing,
            TAG_BOOL => ValueKind::Bool,
            TAG_INT => ValueKind::Int,
            TAG_FLOAT => ValueKind::Float,
            TAG_IDENTITY => ValueKind::Identity,
            TAG_SYMBOL => ValueKind::Symbol,
            TAG_ERROR_CODE => ValueKind::ErrorCode,
            TAG_STRING => ValueKind::String,
            TAG_BYTES => ValueKind::Bytes,
            TAG_LIST => ValueKind::List,
            TAG_MAP => ValueKind::Map,
            TAG_RANGE => ValueKind::Range,
            TAG_ERROR => ValueKind::Error,
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub const fn is_immediate(&self) -> bool {
        !matches!(
            self.tag(),
            TAG_STRING | TAG_BYTES | TAG_LIST | TAG_MAP | TAG_RANGE | TAG_ERROR
        )
    }

    #[inline(always)]
    pub const fn as_bool(&self) -> Option<bool> {
        if self.tag() == TAG_BOOL {
            Some(self.payload() != 0)
        } else {
            None
        }
    }

    #[inline(always)]
    pub const fn as_int(&self) -> Option<i64> {
        if self.tag() == TAG_INT {
            let shifted = ((self.payload() << 8) as i64) >> 8;
            Some(shifted)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn as_float(&self) -> Option<f64> {
        if self.tag() == TAG_FLOAT {
            Some(f32::from_bits(self.payload() as u32) as f64)
        } else {
            None
        }
    }

    #[inline(always)]
    pub const fn as_identity(&self) -> Option<Identity> {
        if self.tag() == TAG_IDENTITY {
            Some(Identity(self.payload()))
        } else {
            None
        }
    }

    #[inline(always)]
    pub const fn as_symbol(&self) -> Option<Symbol> {
        if self.tag() == TAG_SYMBOL {
            Some(Symbol(self.payload() as u32))
        } else {
            None
        }
    }

    #[inline(always)]
    pub const fn as_error_code(&self) -> Option<Symbol> {
        if self.tag() == TAG_ERROR_CODE {
            Some(Symbol(self.payload() as u32))
        } else {
            None
        }
    }

    pub fn with_str<R>(&self, f: impl FnOnce(&str) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::String(value) => Some(f(value)),
            _ => None,
        })?
    }

    pub fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::Bytes(value) => Some(f(value)),
            _ => None,
        })?
    }

    pub fn with_list<R>(&self, f: impl FnOnce(&[Value]) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::List(values) => Some(f(values)),
            _ => None,
        })?
    }

    pub fn with_map<R>(&self, f: impl FnOnce(&[(Value, Value)]) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::Map(entries) => Some(f(entries)),
            _ => None,
        })?
    }

    pub fn with_range<R>(&self, f: impl FnOnce(&Value, Option<&Value>) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::Range { start, end } => Some(f(start, end.as_ref())),
            _ => None,
        })?
    }

    pub fn with_error<R>(&self, f: impl FnOnce(&ErrorValue) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::Error(error) => Some(f(error)),
            _ => None,
        })?
    }

    pub fn error_code_symbol(&self) -> Option<Symbol> {
        self.as_error_code()
            .or_else(|| self.with_error(ErrorValue::code))
    }

    pub fn list_len(&self) -> Option<usize> {
        self.with_list(<[Value]>::len)
    }

    pub fn list_get(&self, index: usize) -> Option<Value> {
        self.with_list(|values| values.get(index).cloned())?
    }

    pub fn list_slice(&self, start: usize, end_exclusive: usize) -> Option<Self> {
        self.with_list(|values| {
            if start > end_exclusive || end_exclusive > values.len() {
                return None;
            }
            Some(Self::list(values[start..end_exclusive].iter().cloned()))
        })?
    }

    pub fn list_set(&self, index: usize, value: Value) -> Option<Self> {
        self.with_list(|values| {
            let mut values = values.to_vec();
            let slot = values.get_mut(index)?;
            *slot = value;
            Some(Self::list(values))
        })?
    }

    pub fn map_len(&self) -> Option<usize> {
        self.with_map(<[(Value, Value)]>::len)
    }

    pub fn map_get(&self, key: &Value) -> Option<Value> {
        self.with_map(|entries| {
            entries
                .binary_search_by(|(entry_key, _)| entry_key.cmp(key))
                .ok()
                .map(|index| entries[index].1.clone())
        })?
    }

    pub fn map_set(&self, key: Value, value: Value) -> Option<Self> {
        self.with_map(|entries| {
            let mut entries = entries.to_vec();
            entries.push((key, value));
            Self::map(entries)
        })
    }

    pub fn index_set(&self, index: &Value, value: Value) -> Option<Self> {
        if self.list_len().is_some() {
            let index = usize::try_from(index.as_int()?).ok()?;
            return self.list_set(index, value);
        }
        self.map_set(index.clone(), value)
    }

    pub fn checked_add(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (Some(left), Some(right)) => {
                left.checked_add(right).and_then(|sum| Self::int(sum).ok())
            }
            _ => Some(Self::float(self.numeric_as_f64()? + rhs.numeric_as_f64()?)),
        }
    }

    pub fn checked_sub(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (Some(left), Some(right)) => left
                .checked_sub(right)
                .and_then(|diff| Self::int(diff).ok()),
            _ => Some(Self::float(self.numeric_as_f64()? - rhs.numeric_as_f64()?)),
        }
    }

    pub fn checked_mul(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (Some(left), Some(right)) => left
                .checked_mul(right)
                .and_then(|product| Self::int(product).ok()),
            _ => Some(Self::float(self.numeric_as_f64()? * rhs.numeric_as_f64()?)),
        }
    }

    pub fn checked_div(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (_, Some(0)) => None,
            (Some(left), Some(right)) if left % right == 0 => Self::int(left / right).ok(),
            _ => {
                let rhs = rhs.numeric_as_f64()?;
                if rhs == 0.0 {
                    None
                } else {
                    Some(Self::float(self.numeric_as_f64()? / rhs))
                }
            }
        }
    }

    pub fn checked_rem(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (_, Some(0)) => None,
            (Some(left), Some(right)) => {
                left.checked_rem(right).and_then(|rem| Self::int(rem).ok())
            }
            _ => {
                let rhs = rhs.numeric_as_f64()?;
                if rhs == 0.0 {
                    None
                } else {
                    Some(Self::float(self.numeric_as_f64()? % rhs))
                }
            }
        }
    }

    pub fn checked_neg(&self) -> Option<Self> {
        if let Some(value) = self.as_int() {
            value.checked_neg().and_then(|value| Self::int(value).ok())
        } else {
            Some(Self::float(-self.numeric_as_f64()?))
        }
    }

    #[inline(always)]
    pub(crate) const fn tag(&self) -> u8 {
        (self.0 >> TAG_SHIFT) as u8
    }

    #[inline(always)]
    pub(crate) const fn payload(&self) -> u64 {
        self.0 & PAYLOAD_MASK
    }

    #[inline(always)]
    fn ptr_payload(&self) -> usize {
        self.payload() as usize
    }

    fn heap(value: HeapValue) -> Self {
        let tag = value.tag();
        let ptr = Arc::into_raw(Arc::new(value)) as usize;
        assert!(
            ptr as u64 <= MAX_PAYLOAD,
            "heap pointer exceeded value payload"
        );
        Self::pack(tag, ptr as u64)
    }

    pub(crate) fn with_heap<R>(&self, f: impl FnOnce(&HeapValue) -> R) -> Option<R> {
        if self.is_immediate() {
            return None;
        }
        let ptr = self.ptr_payload() as *const HeapValue;
        Some(unsafe { f(&*ptr) })
    }

    pub(crate) fn numeric_as_f64(&self) -> Option<f64> {
        if let Some(value) = self.as_int() {
            Some(value as f64)
        } else {
            self.as_float()
        }
    }

    pub(crate) fn heap_ptr(&self) -> *const HeapValue {
        self.ptr_payload() as *const HeapValue
    }

    #[cfg(test)]
    pub(crate) fn heap_strong_count(&self) -> Option<usize> {
        if self.is_immediate() {
            return None;
        }
        let arc = unsafe { Arc::from_raw(self.heap_ptr()) };
        let count = Arc::strong_count(&arc);
        let _ = Arc::into_raw(arc);
        Some(count)
    }
}

impl Clone for Value {
    fn clone(&self) -> Self {
        if !self.is_immediate() {
            unsafe { Arc::increment_strong_count(self.heap_ptr()) };
        }
        Self(self.0)
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        if !self.is_immediate() {
            unsafe { Arc::decrement_strong_count(self.heap_ptr()) };
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Self::nothing()
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::int(value as i64).unwrap()
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::float(value)
    }
}

impl From<Symbol> for Value {
    fn from(value: Symbol) -> Self {
        Self::symbol(value)
    }
}

impl From<Identity> for Value {
    fn from(value: Identity) -> Self {
        Self::identity(value)
    }
}

#[inline(always)]
pub(crate) fn normalize_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}
