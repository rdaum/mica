//! Compact values for Mica's relation kernel.
//!
//! `Value` is intentionally one machine word wide. Immediate identities,
//! symbols, error codes, booleans, small integers, and reduced-precision floats
//! stay inline; strings, bytes, lists, and maps are immutable heap values shared
//! with `Arc`.

mod heap;
mod symbol;
mod traits;
mod value;

#[cfg(test)]
mod tests;

pub use symbol::{Symbol, SymbolMetadata};
pub use value::{Identity, Value, ValueError, ValueKind};
