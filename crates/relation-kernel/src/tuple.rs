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

use mica_var::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Tuple(pub(crate) Arc<[Value]>);

impl Tuple {
    pub fn new(values: impl IntoIterator<Item = Value>) -> Self {
        Self(values.into_iter().collect::<Vec<_>>().into())
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn arity(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn project(&self, positions: &[u16]) -> TupleKey {
        TupleKey(
            positions
                .iter()
                .map(|position| self.0[*position as usize].clone())
                .collect(),
        )
    }

    pub fn select(&self, positions: impl IntoIterator<Item = u16>) -> Self {
        Self::new(
            positions
                .into_iter()
                .map(|position| self.0[position as usize].clone()),
        )
    }

    pub fn concat(&self, other: &Tuple) -> Self {
        Self::new(self.0.iter().cloned().chain(other.0.iter().cloned()))
    }

    pub(crate) fn matches_bindings(&self, bindings: &[Option<Value>]) -> bool {
        bindings
            .iter()
            .enumerate()
            .all(|(index, binding)| binding.as_ref().is_none_or(|value| &self.0[index] == value))
    }
}

impl<const N: usize> From<[Value; N]> for Tuple {
    fn from(value: [Value; N]) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct TupleKey(pub(crate) Vec<Value>);
