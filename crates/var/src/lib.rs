//! Compact values for Mica's relation kernel.
//!
//! `Value` is intentionally one machine word wide. Immediate identities,
//! symbols, booleans, small integers, and reduced-precision floats stay inline;
//! strings, lists, and maps are represented by refcounted heap handles.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock, RwLock};

const TAG_SHIFT: u64 = 56;
const PAYLOAD_MASK: u64 = 0x00ff_ffff_ffff_ffff;

const TAG_NOTHING: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_IDENTITY: u8 = 4;
const TAG_SYMBOL: u8 = 5;
const TAG_STRING: u8 = 6;
const TAG_LIST: u8 = 7;
const TAG_MAP: u8 = 8;

const INT_BITS: u32 = 56;
const INT_MIN: i64 = -(1i64 << (INT_BITS - 1));
const INT_MAX: i64 = (1i64 << (INT_BITS - 1)) - 1;
const MAX_PAYLOAD: u64 = PAYLOAD_MASK;

/// A compact Mica value.
///
/// The layout is private. Use constructors and accessors rather than relying on
/// raw bits. The current representation is a pragmatic tagged word, not a final
/// commitment to this exact bit layout.
#[repr(transparent)]
pub struct Value(u64);

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

/// Interned symbol id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Symbol(u32);

impl Symbol {
    pub const fn from_id(id: u32) -> Self {
        Self(id)
    }

    pub fn intern(name: &str) -> Self {
        symbol_table().intern(name)
    }

    pub const fn id(self) -> u32 {
        self.0
    }

    pub fn name(self) -> Option<String> {
        symbol_table().name(self)
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
    String = TAG_STRING,
    List = TAG_LIST,
    Map = TAG_MAP,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueError {
    IntegerOutOfRange(i64),
    IdentityOutOfRange(u64),
    HeapHandleOutOfRange(usize),
}

impl Value {
    #[inline(always)]
    const fn pack(tag: u8, payload: u64) -> Self {
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

    pub fn string(value: impl AsRef<str>) -> Self {
        Self::heap(HeapValue::String(value.as_ref().into()))
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
                && *last_key == key
            {
                *last_value = value;
                continue;
            }
            canonical.push((key, value));
        }
        Self::heap(HeapValue::Map(canonical.into_boxed_slice()))
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
            TAG_STRING => ValueKind::String,
            TAG_LIST => ValueKind::List,
            TAG_MAP => ValueKind::Map,
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub const fn is_immediate(&self) -> bool {
        !matches!(self.tag(), TAG_STRING | TAG_LIST | TAG_MAP)
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

    pub fn with_str<R>(&self, f: impl FnOnce(&str) -> R) -> Option<R> {
        self.with_heap(|heap| match heap {
            HeapValue::String(value) => Some(f(value)),
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

    pub fn checked_add(&self, rhs: &Self) -> Option<Self> {
        match (self.as_int(), rhs.as_int()) {
            (Some(left), Some(right)) => {
                left.checked_add(right).and_then(|sum| Self::int(sum).ok())
            }
            _ => Some(Self::float(self.numeric_as_f64()? + rhs.numeric_as_f64()?)),
        }
    }

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

    #[inline(always)]
    const fn tag(&self) -> u8 {
        (self.0 >> TAG_SHIFT) as u8
    }

    #[inline(always)]
    const fn payload(&self) -> u64 {
        self.0 & PAYLOAD_MASK
    }

    #[inline(always)]
    fn handle(&self) -> usize {
        self.payload() as usize
    }

    fn heap(value: HeapValue) -> Self {
        let handle = heap_table().alloc(value);
        assert!(
            handle as u64 <= MAX_PAYLOAD,
            "heap handle exceeded value payload"
        );
        let tag = match heap_table().kind(handle) {
            HeapKind::String => TAG_STRING,
            HeapKind::List => TAG_LIST,
            HeapKind::Map => TAG_MAP,
        };
        Self::pack(tag, handle as u64)
    }

    fn retain_heap(&self) {
        if !self.is_immediate() {
            heap_table().retain(self.handle());
        }
    }

    fn release_heap(&self) {
        if !self.is_immediate() {
            heap_table().release(self.handle());
        }
    }

    fn with_heap<R>(&self, f: impl FnOnce(&HeapValue) -> R) -> Option<R> {
        if self.is_immediate() {
            return None;
        }
        Some(heap_table().with(self.handle(), f))
    }

    fn numeric_as_f64(&self) -> Option<f64> {
        if let Some(value) = self.as_int() {
            Some(value as f64)
        } else {
            self.as_float()
        }
    }
}

impl Clone for Value {
    #[inline(always)]
    fn clone(&self) -> Self {
        self.retain_heap();
        Self(self.0)
    }
}

impl Drop for Value {
    #[inline(always)]
    fn drop(&mut self) {
        self.release_heap();
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

#[derive(Clone, Copy)]
enum HeapKind {
    String,
    List,
    Map,
}

enum HeapValue {
    String(Box<str>),
    List(Box<[Value]>),
    Map(Box<[(Value, Value)]>),
}

impl HeapValue {
    fn kind(&self) -> HeapKind {
        match self {
            Self::String(_) => HeapKind::String,
            Self::List(_) => HeapKind::List,
            Self::Map(_) => HeapKind::Map,
        }
    }
}

struct HeapEntry {
    rc: AtomicUsize,
    value: HeapValue,
}

struct HeapTable {
    slots: Mutex<HeapSlots>,
}

struct HeapSlots {
    entries: Vec<Option<*mut HeapEntry>>,
    free: Vec<usize>,
}

unsafe impl Send for HeapSlots {}
unsafe impl Sync for HeapSlots {}

impl HeapTable {
    fn alloc(&self, value: HeapValue) -> usize {
        let entry = Box::into_raw(Box::new(HeapEntry {
            rc: AtomicUsize::new(1),
            value,
        }));
        let mut slots = self.slots.lock().unwrap();
        if let Some(index) = slots.free.pop() {
            slots.entries[index] = Some(entry);
            index + 1
        } else {
            slots.entries.push(Some(entry));
            slots.entries.len()
        }
    }

    fn retain(&self, handle: usize) {
        let ptr = self.ptr(handle);
        unsafe { (*ptr).rc.fetch_add(1, AtomicOrdering::Relaxed) };
    }

    fn release(&self, handle: usize) {
        let ptr = self.ptr(handle);
        if unsafe { (*ptr).rc.fetch_sub(1, AtomicOrdering::Release) } != 1 {
            return;
        }
        std::sync::atomic::fence(AtomicOrdering::Acquire);
        let mut slots = self.slots.lock().unwrap();
        let index = handle - 1;
        let removed = slots.entries[index].take().unwrap();
        debug_assert_eq!(removed, ptr);
        slots.free.push(index);
        drop(slots);
        unsafe { drop(Box::from_raw(ptr)) };
    }

    fn kind(&self, handle: usize) -> HeapKind {
        let ptr = self.ptr(handle);
        unsafe { (*ptr).value.kind() }
    }

    fn with<R>(&self, handle: usize, f: impl FnOnce(&HeapValue) -> R) -> R {
        let ptr = self.ptr(handle);
        unsafe { f(&(*ptr).value) }
    }

    fn ptr(&self, handle: usize) -> *mut HeapEntry {
        assert!(handle != 0, "heap handles are one-based");
        let slots = self.slots.lock().unwrap();
        slots.entries[handle - 1].expect("invalid value heap handle")
    }
}

fn heap_table() -> &'static HeapTable {
    static HEAP: OnceLock<HeapTable> = OnceLock::new();
    HEAP.get_or_init(|| HeapTable {
        slots: Mutex::new(HeapSlots {
            entries: Vec::new(),
            free: Vec::new(),
        }),
    })
}

struct SymbolTable {
    inner: RwLock<SymbolTableInner>,
}

#[derive(Default)]
struct SymbolTableInner {
    by_name: HashMap<Box<str>, u32>,
    by_id: Vec<Box<str>>,
}

impl SymbolTable {
    fn intern(&self, name: &str) -> Symbol {
        if let Some(id) = self.inner.read().unwrap().by_name.get(name).copied() {
            return Symbol(id);
        }
        let mut inner = self.inner.write().unwrap();
        if let Some(id) = inner.by_name.get(name).copied() {
            return Symbol(id);
        }
        let id = inner.by_id.len() as u32;
        let owned: Box<str> = name.into();
        inner.by_name.insert(owned.clone(), id);
        inner.by_id.push(owned);
        Symbol(id)
    }

    fn name(&self, symbol: Symbol) -> Option<String> {
        self.inner
            .read()
            .unwrap()
            .by_id
            .get(symbol.id() as usize)
            .map(|name| name.to_string())
    }
}

fn symbol_table() -> &'static SymbolTable {
    static SYMBOLS: OnceLock<SymbolTable> = OnceLock::new();
    SYMBOLS.get_or_init(|| SymbolTable {
        inner: RwLock::new(SymbolTableInner::default()),
    })
}

#[inline(always)]
fn normalize_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};

    #[test]
    fn value_is_one_word() {
        assert_eq!(size_of::<Value>(), 8);
        assert_eq!(align_of::<Value>(), 8);
    }

    #[test]
    fn immediate_constructors_round_trip() {
        assert_eq!(Value::nothing().kind(), ValueKind::Nothing);
        assert_eq!(Value::bool(true).as_bool(), Some(true));
        assert_eq!(Value::bool(false).as_bool(), Some(false));
        assert_eq!(Value::int(INT_MIN).unwrap().as_int(), Some(INT_MIN));
        assert_eq!(Value::int(INT_MAX).unwrap().as_int(), Some(INT_MAX));
        assert!(Value::int(INT_MIN - 1).is_err());
        assert!(Value::int(INT_MAX + 1).is_err());

        let id = Identity::new(0x00ab_cdef).unwrap();
        assert_eq!(Value::identity(id).as_identity(), Some(id));

        let symbol = Symbol::intern("take");
        assert_eq!(Value::symbol(symbol).as_symbol(), Some(symbol));
        assert_eq!(symbol.name().as_deref(), Some("take"));
    }

    #[test]
    fn float_is_reduced_precision_and_canonicalizes_zero() {
        let value = Value::float(1.25);
        assert_eq!(value.as_float(), Some(1.25));
        assert_eq!(Value::float(-0.0), Value::float(0.0));
    }

    #[test]
    fn string_list_and_map_are_values() {
        let string = Value::string("brass lamp");
        assert_eq!(
            string.with_str(|s| s.to_string()),
            Some("brass lamp".to_string())
        );

        let list = Value::list([Value::int(1).unwrap(), Value::int(2).unwrap()]);
        assert_eq!(list.with_list(|values| values.len()), Some(2));

        let k = Value::symbol(Symbol::intern("color"));
        let red = Value::string("red");
        let blue = Value::string("blue");
        let map = Value::map([(k.clone(), red), (k.clone(), blue.clone())]);
        assert_eq!(map.with_map(|entries| entries.len()), Some(1));
        assert_eq!(
            map.with_map(|entries| entries[0].1.with_str(|s| s.to_string()).unwrap()),
            Some("blue".to_string())
        );
    }

    #[test]
    fn clone_and_drop_heap_values() {
        let value = Value::list([
            Value::string("alpha"),
            Value::symbol(Symbol::intern("beta")),
            Value::identity_raw(42).unwrap(),
        ]);
        let cloned = value.clone();
        assert_eq!(value, cloned);
        drop(value);
        assert_eq!(cloned.with_list(|values| values.len()), Some(3));
    }

    #[test]
    fn total_order_is_stable() {
        let values = vec![
            Value::map([]),
            Value::list([]),
            Value::string("x"),
            Value::symbol(Symbol::intern("x")),
            Value::identity_raw(1).unwrap(),
            Value::float(1.0),
            Value::int(1).unwrap(),
            Value::bool(true),
            Value::nothing(),
        ];
        let mut sorted = values.clone();
        sorted.sort();
        for pair in sorted.windows(2) {
            assert!(pair[0] <= pair[1]);
        }
        assert_eq!(sorted.first(), Some(&Value::nothing()));
        assert_eq!(sorted.last().unwrap().kind(), ValueKind::Map);
    }

    #[test]
    fn ordered_encoding_preserves_string_order() {
        let a = Value::string("a").ordered_key_bytes();
        let aa = Value::string("aa").ordered_key_bytes();
        let b = Value::string("b").ordered_key_bytes();
        assert!(a < aa);
        assert!(aa < b);
    }

    #[test]
    fn arithmetic_fast_path() {
        assert_eq!(
            Value::int(41).unwrap().checked_add(&Value::int(1).unwrap()),
            Some(Value::int(42).unwrap())
        );
        assert_eq!(
            Value::float(1.5)
                .checked_add(&Value::int(2).unwrap())
                .unwrap()
                .as_float(),
            Some(3.5)
        );
    }
}
