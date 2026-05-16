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

use crate::index::ProjectedTupleIndex;
use crate::{ApplicableMethodCall, DispatchRelations, KernelError, RelationId, Transaction, Tuple};
use mica_var::Value;
use std::collections::BTreeSet;

const PROBE_JOIN_LEFT_ROW_LIMIT: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanControl {
    Continue,
    Stop,
}

pub trait RelationRead {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError>;

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        for tuple in self.scan_relation(relation, bindings)? {
            if visitor(&tuple)? == ScanControl::Stop {
                break;
            }
        }
        Ok(())
    }

    fn estimate_relation_scan(
        &self,
        _relation: RelationId,
        _bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        Ok(None)
    }

    fn join_relation_scans(
        &self,
        _left_relation: RelationId,
        _left_bindings: &[Option<Value>],
        _left_positions: &[u16],
        _right_relation: RelationId,
        _right_bindings: &[Option<Value>],
        _right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        Ok(None)
    }

    fn cached_applicable_method_calls(
        &self,
        _relations: DispatchRelations,
        _selector: &Value,
        _roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        Ok(None)
    }

    fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls(relations, selector, roles)
    }
}

impl RelationRead for crate::Snapshot {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.scan(relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.visit(relation, bindings, visitor)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.estimate_scan(relation, bindings).map(Some)
    }

    fn join_relation_scans(
        &self,
        left_relation: RelationId,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right_relation: RelationId,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        let left_rows = self.scan(left_relation, left_bindings)?;
        let right_rows = self.scan(right_relation, right_bindings)?;
        Ok(Some(join_eq(
            left_rows,
            right_rows,
            left_positions,
            right_positions,
        )))
    }

    fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls(relations, selector, roles)
            .map(Some)
    }

    fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls_normalized(relations, selector, roles)
            .map(Some)
    }
}

impl RelationRead for Transaction<'_> {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.scan(relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, KernelError>,
    ) -> Result<(), KernelError> {
        self.visit(relation, bindings, visitor)
    }

    fn estimate_relation_scan(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Option<usize>, KernelError> {
        self.estimate_scan(relation, bindings).map(Some)
    }

    fn join_relation_scans(
        &self,
        left_relation: RelationId,
        left_bindings: &[Option<Value>],
        left_positions: &[u16],
        right_relation: RelationId,
        right_bindings: &[Option<Value>],
        right_positions: &[u16],
    ) -> Result<Option<Vec<Tuple>>, KernelError> {
        let left_rows = self.scan(left_relation, left_bindings)?;
        let right_rows = self.scan(right_relation, right_bindings)?;
        Ok(Some(join_eq(
            left_rows,
            right_rows,
            left_positions,
            right_positions,
        )))
    }

    fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls(relations, selector, roles)
            .map(Some)
    }

    fn cached_applicable_method_calls_normalized(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, KernelError> {
        self.cached_applicable_method_calls_normalized(relations, selector, roles)
            .map(Some)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryPlan {
    Scan {
        relation: RelationId,
        bindings: Vec<Option<Value>>,
    },
    Project {
        input: Box<QueryPlan>,
        positions: Vec<u16>,
    },
    JoinEq {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        left_positions: Vec<u16>,
        right_positions: Vec<u16>,
    },
    SemiJoin {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        left_positions: Vec<u16>,
        right_positions: Vec<u16>,
    },
    AntiJoin {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        left_positions: Vec<u16>,
        right_positions: Vec<u16>,
    },
    Union {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
    },
    Difference {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
    },
}

impl QueryPlan {
    pub fn scan(relation: RelationId, bindings: impl IntoIterator<Item = Option<Value>>) -> Self {
        Self::Scan {
            relation,
            bindings: bindings.into_iter().collect(),
        }
    }

    pub fn project(self, positions: impl IntoIterator<Item = u16>) -> Self {
        Self::Project {
            input: Box::new(self),
            positions: positions.into_iter().collect(),
        }
    }

    pub fn join_eq(
        left: Self,
        right: Self,
        left_positions: impl IntoIterator<Item = u16>,
        right_positions: impl IntoIterator<Item = u16>,
    ) -> Self {
        Self::JoinEq {
            left: Box::new(left),
            right: Box::new(right),
            left_positions: left_positions.into_iter().collect(),
            right_positions: right_positions.into_iter().collect(),
        }
    }

    pub fn semi_join(
        left: Self,
        right: Self,
        left_positions: impl IntoIterator<Item = u16>,
        right_positions: impl IntoIterator<Item = u16>,
    ) -> Self {
        Self::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_positions: left_positions.into_iter().collect(),
            right_positions: right_positions.into_iter().collect(),
        }
    }

    pub fn anti_join(
        left: Self,
        right: Self,
        left_positions: impl IntoIterator<Item = u16>,
        right_positions: impl IntoIterator<Item = u16>,
    ) -> Self {
        Self::AntiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_positions: left_positions.into_iter().collect(),
            right_positions: right_positions.into_iter().collect(),
        }
    }

    pub fn union(left: Self, right: Self) -> Self {
        Self::Union {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn difference(left: Self, right: Self) -> Self {
        Self::Difference {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn execute(&self, reader: &impl RelationRead) -> Result<Vec<Tuple>, KernelError> {
        match self {
            Self::Scan { relation, bindings } => reader.scan_relation(*relation, bindings),
            Self::Project { input, positions } => {
                let rows = input.execute(reader)?;
                Ok(rows
                    .into_iter()
                    .map(|tuple| tuple.select(positions.iter().copied()))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect())
            }
            Self::JoinEq {
                left,
                right,
                left_positions,
                right_positions,
            } => {
                validate_join_positions(left_positions, right_positions);
                if let (
                    QueryPlan::Scan {
                        relation: left_relation,
                        bindings: left_bindings,
                    },
                    QueryPlan::Scan {
                        relation: right_relation,
                        bindings: right_bindings,
                    },
                ) = (left.as_ref(), right.as_ref())
                    && let Some(rows) = reader.join_relation_scans(
                        *left_relation,
                        left_bindings,
                        left_positions,
                        *right_relation,
                        right_bindings,
                        right_positions,
                    )?
                {
                    return Ok(rows);
                }
                let left_rows = left.execute(reader)?;
                if let Some((relation, bindings)) = scan_parts(right)
                    && should_probe_join(left_rows.len(), reader, relation, bindings)?
                {
                    return indexed_nested_loop_join_rows(
                        left_rows,
                        relation,
                        bindings,
                        left_positions,
                        right_positions,
                        reader,
                    );
                }
                let right_rows = right.execute(reader)?;
                Ok(join_eq(
                    left_rows,
                    right_rows,
                    left_positions,
                    right_positions,
                ))
            }
            Self::SemiJoin {
                left,
                right,
                left_positions,
                right_positions,
            } => {
                validate_join_positions(left_positions, right_positions);
                let left_rows = left.execute(reader)?;
                if let Some((relation, bindings)) = scan_parts(right)
                    && should_probe_join(left_rows.len(), reader, relation, bindings)?
                {
                    return indexed_nested_loop_semi_join_rows(
                        left_rows,
                        relation,
                        bindings,
                        left_positions,
                        right_positions,
                        reader,
                        true,
                    );
                }
                let right_rows = right.execute(reader)?;
                Ok(semi_join(
                    left_rows,
                    right_rows,
                    left_positions,
                    right_positions,
                    true,
                ))
            }
            Self::AntiJoin {
                left,
                right,
                left_positions,
                right_positions,
            } => {
                validate_join_positions(left_positions, right_positions);
                let left_rows = left.execute(reader)?;
                if let Some((relation, bindings)) = scan_parts(right)
                    && should_probe_join(left_rows.len(), reader, relation, bindings)?
                {
                    return indexed_nested_loop_semi_join_rows(
                        left_rows,
                        relation,
                        bindings,
                        left_positions,
                        right_positions,
                        reader,
                        false,
                    );
                }
                let right_rows = right.execute(reader)?;
                Ok(semi_join(
                    left_rows,
                    right_rows,
                    left_positions,
                    right_positions,
                    false,
                ))
            }
            Self::Union { left, right } => {
                let mut rows = left.execute(reader)?.into_iter().collect::<BTreeSet<_>>();
                rows.extend(right.execute(reader)?);
                Ok(rows.into_iter().collect())
            }
            Self::Difference { left, right } => {
                let mut rows = left.execute(reader)?.into_iter().collect::<BTreeSet<_>>();
                for row in right.execute(reader)? {
                    rows.remove(&row);
                }
                Ok(rows.into_iter().collect())
            }
        }
    }
}

fn scan_parts(plan: &QueryPlan) -> Option<(RelationId, &[Option<Value>])> {
    let QueryPlan::Scan { relation, bindings } = plan else {
        return None;
    };
    Some((*relation, bindings))
}

fn should_probe_join(
    left_len: usize,
    reader: &impl RelationRead,
    right_relation: RelationId,
    right_bindings: &[Option<Value>],
) -> Result<bool, KernelError> {
    if left_len <= PROBE_JOIN_LEFT_ROW_LIMIT {
        return Ok(true);
    }
    let Some(right_estimate) = reader.estimate_relation_scan(right_relation, right_bindings)?
    else {
        return Ok(false);
    };
    Ok(left_len.saturating_mul(4) < right_estimate)
}

fn indexed_nested_loop_join_rows(
    left_rows: Vec<Tuple>,
    right_relation: RelationId,
    right_bindings: &[Option<Value>],
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
) -> Result<Vec<Tuple>, KernelError> {
    let mut out = BTreeSet::new();
    for left_row in left_rows {
        let Some(probe_bindings) =
            probe_bindings(right_bindings, &left_row, left_positions, right_positions)
        else {
            continue;
        };
        for right_row in reader.scan_relation(right_relation, &probe_bindings)? {
            out.insert(left_row.concat(&right_row));
        }
    }
    Ok(out.into_iter().collect())
}

fn indexed_nested_loop_semi_join_rows(
    left_rows: Vec<Tuple>,
    right_relation: RelationId,
    right_bindings: &[Option<Value>],
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
    keep_matches: bool,
) -> Result<Vec<Tuple>, KernelError> {
    let mut out = Vec::new();
    for left_row in left_rows {
        let matched = if let Some(probe_bindings) =
            probe_bindings(right_bindings, &left_row, left_positions, right_positions)
        {
            !reader
                .scan_relation(right_relation, &probe_bindings)?
                .is_empty()
        } else {
            false
        };

        if matched == keep_matches {
            out.push(left_row);
        }
    }
    Ok(out)
}

fn probe_bindings(
    bindings: &[Option<Value>],
    left_row: &Tuple,
    left_positions: &[u16],
    right_positions: &[u16],
) -> Option<Vec<Option<Value>>> {
    let mut probe_bindings = bindings.to_vec();
    for (left_position, right_position) in left_positions.iter().zip(right_positions.iter()) {
        let value = left_row.values()[*left_position as usize].clone();
        let binding = &mut probe_bindings[*right_position as usize];
        if binding.as_ref().is_some_and(|existing| existing != &value) {
            return None;
        }
        *binding = Some(value);
    }
    Some(probe_bindings)
}

fn join_eq(
    left_rows: Vec<Tuple>,
    right_rows: Vec<Tuple>,
    left_positions: &[u16],
    right_positions: &[u16],
) -> Vec<Tuple> {
    let left_index = ProjectedTupleIndex::from_rows(left_rows, left_positions);
    let right_index = ProjectedTupleIndex::from_rows(right_rows, right_positions);
    let mut out = BTreeSet::new();
    left_index.intersect_values_with(&right_index, |left_bucket, right_bucket| {
        for left in left_bucket {
            for right in right_bucket {
                out.insert(left.concat(right));
            }
        }
    });
    out.into_iter().collect()
}

fn semi_join(
    left_rows: Vec<Tuple>,
    right_rows: Vec<Tuple>,
    left_positions: &[u16],
    right_positions: &[u16],
    keep_matches: bool,
) -> Vec<Tuple> {
    let left_index = ProjectedTupleIndex::from_rows(left_rows.iter().cloned(), left_positions);
    let right_index = ProjectedTupleIndex::from_rows(right_rows, right_positions);
    let mut matches = BTreeSet::new();
    left_index.matching_left_rows(&right_index, |tuple| {
        matches.insert(tuple.clone());
    });
    left_rows
        .into_iter()
        .filter(|row| matches.contains(row) == keep_matches)
        .collect()
}

fn validate_join_positions(left_positions: &[u16], right_positions: &[u16]) {
    assert_eq!(
        left_positions.len(),
        right_positions.len(),
        "join position lists must have the same length"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelationKernel, RelationMetadata};
    use mica_var::{Identity, Symbol, Value};
    use std::cell::Cell;

    fn rel(id: u64) -> RelationId {
        Identity::new(id).unwrap()
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn kernel_with_edges() -> RelationKernel {
        let kernel = RelationKernel::new();
        kernel
            .create_relation(
                RelationMetadata::new(rel(10), Symbol::intern("Edge"), 2)
                    .with_index([0, 1])
                    .with_index([1, 0]),
            )
            .unwrap();
        kernel
    }

    struct ProbeOnlyReader;

    impl RelationRead for ProbeOnlyReader {
        fn scan_relation(
            &self,
            relation: RelationId,
            bindings: &[Option<Value>],
        ) -> Result<Vec<Tuple>, KernelError> {
            if relation == rel(20) {
                assert_eq!(bindings, &[None, None]);
                return Ok(vec![
                    Tuple::from([int(1), int(10)]),
                    Tuple::from([int(2), int(20)]),
                ]);
            }

            assert_eq!(relation, rel(21));
            match bindings {
                [Some(value), None] if value == &int(10) => {
                    Ok(vec![Tuple::from([int(10), int(100)])])
                }
                [Some(value), None] if value == &int(20) => {
                    Ok(vec![Tuple::from([int(20), int(200)])])
                }
                other => panic!("right relation should be probed with a bound join key: {other:?}"),
            }
        }
    }

    struct DirectJoinReader {
        called: Cell<bool>,
    }

    impl RelationRead for DirectJoinReader {
        fn scan_relation(
            &self,
            _relation: RelationId,
            _bindings: &[Option<Value>],
        ) -> Result<Vec<Tuple>, KernelError> {
            panic!("direct relation join should avoid ordinary scans")
        }

        fn join_relation_scans(
            &self,
            left_relation: RelationId,
            left_bindings: &[Option<Value>],
            left_positions: &[u16],
            right_relation: RelationId,
            right_bindings: &[Option<Value>],
            right_positions: &[u16],
        ) -> Result<Option<Vec<Tuple>>, KernelError> {
            assert_eq!(left_relation, rel(30));
            assert_eq!(left_bindings, &[None]);
            assert_eq!(left_positions, &[0]);
            assert_eq!(right_relation, rel(31));
            assert_eq!(right_bindings, &[None]);
            assert_eq!(right_positions, &[0]);
            self.called.set(true);
            Ok(Some(vec![Tuple::from([int(1), int(1)])]))
        }
    }

    #[test]
    fn query_plan_executes_projected_join_against_transaction_overlay() {
        let kernel = kernel_with_edges();
        let mut tx = kernel.begin();
        tx.assert(rel(10), Tuple::from([int(1), int(2)])).unwrap();
        tx.assert(rel(10), Tuple::from([int(2), int(3)])).unwrap();

        let path2 = QueryPlan::join_eq(
            QueryPlan::scan(rel(10), [None, None]),
            QueryPlan::scan(rel(10), [None, None]),
            [1],
            [0],
        )
        .project([0, 3]);

        assert_eq!(
            path2.execute(&tx).unwrap(),
            vec![Tuple::from([int(1), int(3)])]
        );
    }

    #[test]
    fn query_plan_uses_indexed_probe_when_right_side_is_scan() {
        let path = QueryPlan::join_eq(
            QueryPlan::scan(rel(20), [None, None]),
            QueryPlan::scan(rel(21), [None, None]),
            [1],
            [0],
        )
        .project([0, 3]);

        assert_eq!(
            path.execute(&ProbeOnlyReader).unwrap(),
            vec![
                Tuple::from([int(1), int(100)]),
                Tuple::from([int(2), int(200)])
            ]
        );
    }

    #[test]
    fn query_plan_uses_relation_join_hook_for_scan_equality_join() {
        let reader = DirectJoinReader {
            called: Cell::new(false),
        };
        let path = QueryPlan::join_eq(
            QueryPlan::scan(rel(30), [None]),
            QueryPlan::scan(rel(31), [None]),
            [0],
            [0],
        );

        assert_eq!(
            path.execute(&reader).unwrap(),
            vec![Tuple::from([int(1), int(1)])]
        );
        assert!(reader.called.get());
    }

    #[test]
    fn query_plan_executes_semi_and_anti_join() {
        let kernel = kernel_with_edges();
        let mut tx = kernel.begin();
        tx.assert(rel(10), Tuple::from([int(1), int(2)])).unwrap();
        tx.assert(rel(10), Tuple::from([int(2), int(3)])).unwrap();
        tx.assert(rel(10), Tuple::from([int(4), int(5)])).unwrap();
        tx.commit().unwrap();

        let left = QueryPlan::scan(rel(10), [None, None]);
        let right = QueryPlan::scan(rel(10), [None, None]);
        let has_outgoing_from_target = QueryPlan::semi_join(left.clone(), right.clone(), [1], [0]);
        let terminal_edges = QueryPlan::anti_join(left, right, [1], [0]);

        assert_eq!(
            has_outgoing_from_target
                .execute(&*kernel.snapshot())
                .unwrap(),
            vec![Tuple::from([int(1), int(2)])]
        );
        assert_eq!(
            terminal_edges.execute(&*kernel.snapshot()).unwrap(),
            vec![Tuple::from([int(2), int(3)]), Tuple::from([int(4), int(5)])]
        );
    }
}
