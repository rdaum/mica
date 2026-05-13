use crate::tuple::TupleKey;
use crate::{KernelError, RelationId, Transaction, Tuple};
use mica_var::Value;
use std::collections::{BTreeMap, BTreeSet};

pub trait RelationRead {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError>;
}

impl RelationRead for crate::Snapshot {
    fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, KernelError> {
        self.scan(relation, bindings)
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
                if let Some(rows) =
                    indexed_nested_loop_join(left, right, left_positions, right_positions, reader)?
                {
                    return Ok(rows);
                }
                let left_rows = left.execute(reader)?;
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
                if let Some(rows) = indexed_nested_loop_semi_join(
                    left,
                    right,
                    left_positions,
                    right_positions,
                    reader,
                    true,
                )? {
                    return Ok(rows);
                }
                let left_rows = left.execute(reader)?;
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
                if let Some(rows) = indexed_nested_loop_semi_join(
                    left,
                    right,
                    left_positions,
                    right_positions,
                    reader,
                    false,
                )? {
                    return Ok(rows);
                }
                let left_rows = left.execute(reader)?;
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

fn indexed_nested_loop_join(
    left: &QueryPlan,
    right: &QueryPlan,
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    let QueryPlan::Scan { relation, bindings } = right else {
        return Ok(None);
    };

    let mut out = BTreeSet::new();
    for left_row in left.execute(reader)? {
        let Some(probe_bindings) =
            probe_bindings(bindings, &left_row, left_positions, right_positions)
        else {
            continue;
        };
        for right_row in reader.scan_relation(*relation, &probe_bindings)? {
            out.insert(left_row.concat(&right_row));
        }
    }
    Ok(Some(out.into_iter().collect()))
}

fn indexed_nested_loop_semi_join(
    left: &QueryPlan,
    right: &QueryPlan,
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
    keep_matches: bool,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    let QueryPlan::Scan { relation, bindings } = right else {
        return Ok(None);
    };

    let mut out = Vec::new();
    for left_row in left.execute(reader)? {
        let matched = if let Some(probe_bindings) =
            probe_bindings(bindings, &left_row, left_positions, right_positions)
        {
            !reader.scan_relation(*relation, &probe_bindings)?.is_empty()
        } else {
            false
        };

        if matched == keep_matches {
            out.push(left_row);
        }
    }
    Ok(Some(out))
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
    let mut right_index: BTreeMap<TupleKey, Vec<Tuple>> = BTreeMap::new();
    for row in right_rows {
        right_index
            .entry(row.project(right_positions))
            .or_default()
            .push(row);
    }

    let mut out = BTreeSet::new();
    for left in left_rows {
        if let Some(matches) = right_index.get(&left.project(left_positions)) {
            for right in matches {
                out.insert(left.concat(right));
            }
        }
    }
    out.into_iter().collect()
}

fn semi_join(
    left_rows: Vec<Tuple>,
    right_rows: Vec<Tuple>,
    left_positions: &[u16],
    right_positions: &[u16],
    keep_matches: bool,
) -> Vec<Tuple> {
    let right_keys = right_rows
        .iter()
        .map(|row| row.project(right_positions))
        .collect::<BTreeSet<_>>();
    left_rows
        .into_iter()
        .filter(|row| right_keys.contains(&row.project(left_positions)) == keep_matches)
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
