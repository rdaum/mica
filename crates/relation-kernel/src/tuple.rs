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

pub use mica_var::Tuple;
use mica_var::Value;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct TupleKey(pub(crate) Vec<Value>);

impl TupleKey {
    pub(crate) fn project(tuple: &Tuple, positions: &[u16]) -> Self {
        Self(
            positions
                .iter()
                .map(|position| tuple.values()[*position as usize].clone())
                .collect(),
        )
    }
}
