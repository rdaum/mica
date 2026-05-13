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
pub use value::{ErrorValue, Identity, Value, ValueError, ValueKind};
