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

pub(crate) fn finish_tuple_rows(mut rows: Vec<Tuple>) -> Vec<Tuple> {
    rows.sort();
    rows.dedup();
    rows
}

pub(crate) fn extend_matching_tuple_rows(
    target: &mut impl Extend<Tuple>,
    rows: &[Tuple],
    bindings: &[Option<Value>],
) {
    target.extend(
        rows.iter()
            .filter(|tuple| tuple.matches_bindings(bindings))
            .cloned(),
    );
}

pub(crate) fn finish_with_matching_tuple_rows(
    mut visible: Vec<Tuple>,
    rows: &[Tuple],
    bindings: &[Option<Value>],
) -> Vec<Tuple> {
    extend_matching_tuple_rows(&mut visible, rows, bindings);
    finish_tuple_rows(visible)
}

pub(crate) fn union_ordered_tuple_rows(left: Vec<Tuple>, right: Vec<Tuple>) -> Vec<Tuple> {
    debug_assert!(left.windows(2).all(|window| window[0] < window[1]));
    debug_assert!(right.windows(2).all(|window| window[0] < window[1]));

    let mut out = Vec::with_capacity(left.len() + right.len());
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();

    loop {
        match (left.peek(), right.peek()) {
            (Some(left_tuple), Some(right_tuple)) if left_tuple < right_tuple => {
                out.push(left.next().expect("left tuple should exist"));
            }
            (Some(left_tuple), Some(right_tuple)) if left_tuple > right_tuple => {
                out.push(right.next().expect("right tuple should exist"));
            }
            (Some(_), Some(_)) => {
                out.push(left.next().expect("left tuple should exist"));
                right.next();
            }
            (Some(_), None) => {
                out.extend(left);
                break;
            }
            (None, Some(_)) => {
                out.extend(right);
                break;
            }
            (None, None) => break,
        }
    }

    out
}

pub(crate) fn difference_tuple_rows(left: Vec<Tuple>, right: Vec<Tuple>) -> Vec<Tuple> {
    let left = finish_tuple_rows(left);
    let right = finish_tuple_rows(right);
    difference_ordered_tuple_rows(left, right)
}

pub(crate) fn difference_ordered_tuple_rows(left: Vec<Tuple>, right: Vec<Tuple>) -> Vec<Tuple> {
    debug_assert!(left.windows(2).all(|window| window[0] < window[1]));
    debug_assert!(right.windows(2).all(|window| window[0] < window[1]));

    let mut out = Vec::with_capacity(left.len());
    let mut right_index = 0usize;

    for row in left {
        while right_index < right.len() && right[right_index] < row {
            right_index += 1;
        }
        if right_index == right.len() || right[right_index] != row {
            out.push(row);
        }
    }

    out
}
