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

//! Relational operations over immutable first-class relation values.

use crate::index::ProjectedTupleIndex;
use mica_var::{RelationValue, RelationValueError, Symbol, Tuple};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationAlgebraError {
    UnknownColumn(Symbol),
    HeadingMismatch {
        left: Box<[Symbol]>,
        right: Box<[Symbol]>,
    },
    InvalidRelation(RelationValueError),
}

impl fmt::Display for RelationAlgebraError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownColumn(column) => {
                f.write_str("relation has no column ")?;
                write_column(f, *column)
            }
            Self::HeadingMismatch { left, right } => {
                f.write_str("relation headings are incompatible: ")?;
                write_heading(f, left)?;
                f.write_str(" and ")?;
                write_heading(f, right)
            }
            Self::InvalidRelation(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for RelationAlgebraError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidRelation(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RelationValueError> for RelationAlgebraError {
    fn from(error: RelationValueError) -> Self {
        Self::InvalidRelation(error)
    }
}

/// Projects a relation onto the named columns.
///
/// Column order is immaterial because relation headings are sets. Projection
/// eliminates rows that become duplicates.
pub fn project(
    relation: &RelationValue,
    columns: impl AsRef<[Symbol]>,
) -> Result<RelationValue, RelationAlgebraError> {
    let columns = columns.as_ref();
    let positions = columns
        .iter()
        .map(|column| {
            relation
                .column_position(*column)
                .map(position_u16)
                .ok_or(RelationAlgebraError::UnknownColumn(*column))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = project_tuple_rows(relation.rows().iter().cloned(), &positions);
    RelationValue::new(columns.iter().copied(), rows).map_err(Into::into)
}

/// Returns the union of two relations with identical headings.
pub fn union(
    left: &RelationValue,
    right: &RelationValue,
) -> Result<RelationValue, RelationAlgebraError> {
    require_same_heading(left, right)?;
    let rows = union_ordered_tuple_rows(left.rows().to_vec(), right.rows().to_vec());
    rebuild_with_heading(left, rows)
}

/// Removes the right relation's rows from the left relation.
pub fn difference(
    left: &RelationValue,
    right: &RelationValue,
) -> Result<RelationValue, RelationAlgebraError> {
    require_same_heading(left, right)?;
    let rows = difference_ordered_tuple_rows(left.rows().to_vec(), right.rows().to_vec());
    rebuild_with_heading(left, rows)
}

/// Naturally joins two relations on all shared column names.
///
/// Shared columns occur once in the result. With no shared columns this is a
/// Cartesian product. Join keys use canonical `Value` equality, so numerically
/// equal values of different kinds do not match.
pub fn natural_join(
    left: &RelationValue,
    right: &RelationValue,
) -> Result<RelationValue, RelationAlgebraError> {
    let mut left_positions = Vec::new();
    let mut right_positions = Vec::new();
    for (left_position, column) in left.heading().iter().enumerate() {
        if let Some(right_position) = right.column_position(*column) {
            left_positions.push(position_u16(left_position));
            right_positions.push(position_u16(right_position));
        }
    }

    let right_only_positions = right
        .heading()
        .iter()
        .enumerate()
        .filter(|(_, column)| left.column_position(**column).is_none())
        .map(|(position, _)| position_u16(position))
        .collect::<Vec<_>>();
    let heading = left.heading().iter().copied().chain(
        right_only_positions
            .iter()
            .map(|position| right.heading()[*position as usize]),
    );
    let rows = join_tuple_rows_with(
        left.rows(),
        right.rows(),
        &left_positions,
        &right_positions,
        |left, right| {
            Tuple::new(
                left.values().iter().cloned().chain(
                    right_only_positions
                        .iter()
                        .map(|position| right.values()[*position as usize].clone()),
                ),
            )
        },
    );
    RelationValue::new(heading, rows).map_err(Into::into)
}

pub(crate) fn finish_tuple_rows(mut rows: Vec<Tuple>) -> Vec<Tuple> {
    rows.sort();
    rows.dedup();
    rows
}

pub(crate) fn project_tuple_rows(
    rows: impl IntoIterator<Item = Tuple>,
    positions: &[u16],
) -> Vec<Tuple> {
    finish_tuple_rows(
        rows.into_iter()
            .map(|row| row.select(positions.iter().copied()))
            .collect(),
    )
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

pub(crate) fn union_tuple_rows(left: Vec<Tuple>, right: Vec<Tuple>) -> Vec<Tuple> {
    let left = finish_tuple_rows(left);
    let right = finish_tuple_rows(right);
    union_ordered_tuple_rows(left, right)
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

pub(crate) fn equality_join_tuple_rows(
    left_rows: Vec<Tuple>,
    right_rows: Vec<Tuple>,
    left_positions: &[u16],
    right_positions: &[u16],
) -> Vec<Tuple> {
    join_tuple_rows_with(
        &left_rows,
        &right_rows,
        left_positions,
        right_positions,
        Tuple::concat,
    )
}

fn join_tuple_rows_with(
    left_rows: &[Tuple],
    right_rows: &[Tuple],
    left_positions: &[u16],
    right_positions: &[u16],
    mut combine: impl FnMut(&Tuple, &Tuple) -> Tuple,
) -> Vec<Tuple> {
    debug_assert_eq!(left_positions.len(), right_positions.len());
    let mut out = Vec::new();
    if left_positions.is_empty() {
        for left in left_rows {
            for right in right_rows {
                out.push(combine(left, right));
            }
        }
        return finish_tuple_rows(out);
    }

    let left_index = ProjectedTupleIndex::from_rows(left_rows, left_positions);
    let right_index = ProjectedTupleIndex::from_rows(right_rows, right_positions);
    left_index.matching_row_pairs(&right_index, |left, right| {
        out.push(combine(left, right));
    });
    finish_tuple_rows(out)
}

fn require_same_heading(
    left: &RelationValue,
    right: &RelationValue,
) -> Result<(), RelationAlgebraError> {
    if left.heading() == right.heading() {
        return Ok(());
    }
    Err(RelationAlgebraError::HeadingMismatch {
        left: left.heading().into(),
        right: right.heading().into(),
    })
}

fn rebuild_with_heading(
    relation: &RelationValue,
    rows: Vec<Tuple>,
) -> Result<RelationValue, RelationAlgebraError> {
    RelationValue::new(relation.heading().iter().copied(), rows).map_err(Into::into)
}

fn position_u16(position: usize) -> u16 {
    u16::try_from(position).expect("relation value positions fit in u16")
}

fn write_heading(f: &mut fmt::Formatter<'_>, heading: &[Symbol]) -> fmt::Result {
    f.write_str("{")?;
    for (index, column) in heading.iter().enumerate() {
        if index != 0 {
            f.write_str(", ")?;
        }
        write_column(f, *column)?;
    }
    f.write_str("}")
}

fn write_column(f: &mut fmt::Formatter<'_>, column: Symbol) -> fmt::Result {
    match column.name() {
        Some(name) => write!(f, ":{name}"),
        None => write!(f, ":#{}", column.id()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_var::Value;

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn relation(heading: &[Symbol], rows: &[&[i64]]) -> RelationValue {
        RelationValue::new(
            heading.iter().copied(),
            rows.iter()
                .map(|row| Tuple::new(row.iter().copied().map(int))),
        )
        .unwrap()
    }

    #[test]
    fn projection_uses_named_columns_and_set_semantics() {
        let a = Symbol::intern("algebra-project-a");
        let b = Symbol::intern("algebra-project-b");
        let input = relation(&[a, b], &[&[1, 10], &[1, 20], &[2, 30]]);

        let projected = project(&input, [a]).unwrap();

        assert_eq!(projected, relation(&[a], &[&[1], &[2]]));
        assert_eq!(
            project(&input, [Symbol::intern("algebra-project-missing")]),
            Err(RelationAlgebraError::UnknownColumn(Symbol::intern(
                "algebra-project-missing"
            )))
        );
        assert_eq!(
            project(&input, [a, a]),
            Err(RelationAlgebraError::InvalidRelation(
                RelationValueError::DuplicateColumn(a)
            ))
        );
    }

    #[test]
    fn union_and_difference_require_identical_headings() {
        let value = Symbol::intern("algebra-set-value");
        let other = Symbol::intern("algebra-set-other");
        let left = relation(&[value], &[&[1], &[2]]);
        let right = relation(&[value], &[&[2], &[3]]);

        assert_eq!(
            union(&left, &right).unwrap(),
            relation(&[value], &[&[1], &[2], &[3]])
        );
        assert_eq!(
            difference(&left, &right).unwrap(),
            relation(&[value], &[&[1]])
        );
        assert!(matches!(
            union(&left, &relation(&[other], &[&[1]])),
            Err(RelationAlgebraError::HeadingMismatch { .. })
        ));
    }

    #[test]
    fn natural_join_matches_shared_columns_once() {
        let person = Symbol::intern("algebra-join-person");
        let team = Symbol::intern("algebra-join-team");
        let colour = Symbol::intern("algebra-join-colour");
        let people = relation(&[person, team], &[&[1, 10], &[2, 20], &[3, 10]]);
        let teams = relation(&[team, colour], &[&[10, 100], &[20, 200], &[30, 300]]);

        let joined = natural_join(&people, &teams).unwrap();

        assert_eq!(
            joined,
            relation(
                &[person, team, colour],
                &[&[1, 10, 100], &[2, 20, 200], &[3, 10, 100]]
            )
        );
    }

    #[test]
    fn natural_join_without_shared_columns_is_product() {
        let left_column = Symbol::intern("algebra-product-left");
        let right_column = Symbol::intern("algebra-product-right");
        let left = relation(&[left_column], &[&[1], &[2]]);
        let right = relation(&[right_column], &[&[10], &[20]]);

        assert_eq!(
            natural_join(&left, &right).unwrap(),
            relation(
                &[left_column, right_column],
                &[&[1, 10], &[1, 20], &[2, 10], &[2, 20]]
            )
        );
    }

    #[test]
    fn zero_arity_empty_and_unit_obey_join_identities() {
        let empty = relation(&[], &[]);
        let unit = RelationValue::new([], [Tuple::new([])]).unwrap();
        let value = Symbol::intern("algebra-unit-value");
        let values = relation(&[value], &[&[1], &[2]]);

        assert_eq!(
            natural_join(&empty, &values).unwrap(),
            relation(&[value], &[])
        );
        assert_eq!(natural_join(&unit, &values).unwrap(), values);
    }

    #[test]
    fn natural_join_uses_canonical_not_numeric_equality() {
        let key = Symbol::intern("algebra-canonical-key");
        let left = RelationValue::new([key], [Tuple::from([int(1)])]).unwrap();
        let right = RelationValue::new([key], [Tuple::from([Value::float(1.0).unwrap()])]).unwrap();

        assert!(natural_join(&left, &right).unwrap().is_empty());
    }
}
