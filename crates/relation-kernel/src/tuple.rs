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

pub(crate) fn finish_tuple_rows(mut rows: Vec<Tuple>) -> Vec<Tuple> {
    rows.sort();
    rows.dedup();
    rows
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
