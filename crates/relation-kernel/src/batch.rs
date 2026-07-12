// Copyright (C) 2026 Ryan Daum <ryan@timbran.org>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

use crate::query::PhysicalQueryPlan;
use crate::{KernelError, RelationRead, Tuple};
use mica_var::Value;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const PACKED_ROW_THRESHOLD: usize = 256;
type PackedPair = (Arc<crate::PackedRelation>, Arc<crate::PackedRelation>);

pub(crate) struct PackedJoinInput<'a> {
    pub reader: &'a dyn RelationRead,
    pub relation: crate::RelationId,
    pub bindings: &'a [Option<Value>],
}

pub(crate) struct PackedRows<'a> {
    batch: &'a NativeBatch,
}

impl PackedRows<'_> {
    pub fn row_count(&self) -> usize {
        self.batch.row_count
    }

    pub fn value(&self, row: usize, column: usize) -> &Value {
        &self.batch.columns[column].as_slice()[row]
    }
}

#[derive(Default)]
struct BatchWorkspace {
    columns: Vec<Vec<Value>>,
    row_indexes: Vec<Vec<usize>>,
    tuple_buffers: Vec<Vec<Tuple>>,
}

thread_local! {
    static BATCH_WORKSPACE: RefCell<BatchWorkspace> = RefCell::new(BatchWorkspace::default());
}

impl BatchWorkspace {
    fn column(&mut self, capacity: usize) -> Vec<Value> {
        let mut column = self.columns.pop().unwrap_or_default();
        column.clear();
        column.reserve(capacity);
        column
    }

    fn row_indexes(&mut self, capacity: usize) -> Vec<usize> {
        let mut rows = self.row_indexes.pop().unwrap_or_default();
        rows.clear();
        rows.reserve(capacity);
        rows
    }

    fn tuples(&mut self, capacity: usize) -> Vec<Tuple> {
        let mut tuples = self.tuple_buffers.pop().unwrap_or_default();
        tuples.clear();
        tuples.reserve(capacity);
        tuples
    }

    fn recycle_batch(&mut self, batch: NativeBatch) {
        for column in batch.columns {
            if let NativeColumn::Owned(mut values) = column {
                values.clear();
                self.columns.push(values);
            }
        }
        if let Some(mut tuples) = batch.tuples {
            tuples.clear();
            self.tuple_buffers.push(tuples);
        }
    }

    fn recycle_row_indexes(&mut self, mut rows: Vec<usize>) {
        rows.clear();
        self.row_indexes.push(rows);
    }
}

fn with_batch_workspace<T>(execute: impl FnOnce(&mut BatchWorkspace) -> T) -> T {
    BATCH_WORKSPACE.with(|workspace| {
        let Ok(mut workspace) = workspace.try_borrow_mut() else {
            return execute(&mut BatchWorkspace::default());
        };
        execute(&mut workspace)
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeBatch {
    columns: Vec<NativeColumn>,
    tuples: Option<Vec<Tuple>>,
    row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NativeColumn {
    Shared(Arc<[Value]>),
    Owned(Vec<Value>),
}

impl NativeColumn {
    fn as_slice(&self) -> &[Value] {
        match self {
            Self::Shared(values) => values,
            Self::Owned(values) => values,
        }
    }

    fn push(&mut self, value: Value) {
        let Self::Owned(values) = self else {
            panic!("output columns must be owned");
        };
        values.push(value);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum PackedKey {
    One(Value),
    Two(Value, Value),
}

impl NativeBatch {
    fn from_tuples(rows: Vec<Tuple>, arity: usize) -> Option<Self> {
        if arity == 0 || arity > 2 {
            return None;
        }
        let mut columns = (0..arity)
            .map(|_| Vec::with_capacity(rows.len()))
            .collect::<Vec<_>>();
        for tuple in &rows {
            if tuple.arity() != arity || tuple.values().iter().any(|value| !value.is_immediate()) {
                return None;
            }
            for (column, value) in columns.iter_mut().zip(tuple.values()) {
                column.push(value.clone());
            }
        }
        Some(Self {
            row_count: columns.first().map(Vec::len).unwrap_or(0),
            columns: columns.into_iter().map(NativeColumn::Owned).collect(),
            tuples: Some(rows),
        })
    }

    fn empty(arity: usize, capacity: usize, workspace: &mut BatchWorkspace) -> Self {
        Self {
            columns: (0..arity)
                .map(|_| NativeColumn::Owned(workspace.column(capacity)))
                .collect(),
            tuples: None,
            row_count: 0,
        }
    }

    fn from_packed(batch: &crate::PackedRelation, workspace: &mut BatchWorkspace) -> Self {
        let mut tuples = workspace.tuples(batch.row_count());
        tuples.extend_from_slice(batch.rows());
        Self {
            columns: batch
                .columns()
                .iter()
                .cloned()
                .map(NativeColumn::Shared)
                .collect(),
            tuples: Some(tuples),
            row_count: batch.row_count(),
        }
    }

    fn arity(&self) -> usize {
        self.columns.len()
    }

    fn project(&self, positions: &[u16], workspace: &mut BatchWorkspace) -> Option<Self> {
        let columns = positions
            .iter()
            .map(|position| self.columns.get(*position as usize).cloned())
            .collect::<Option<Vec<_>>>()?;
        let mut projected = Self {
            columns,
            tuples: positions
                .iter()
                .copied()
                .eq(0..self.arity() as u16)
                .then(|| self.tuples.clone())
                .flatten(),
            row_count: self.row_count,
        };
        projected.canonicalize(workspace);
        Some(projected)
    }

    fn key(&self, row: usize, positions: &[u16]) -> Option<PackedKey> {
        match positions {
            [one] => Some(PackedKey::One(
                self.columns
                    .get(*one as usize)?
                    .as_slice()
                    .get(row)?
                    .clone(),
            )),
            [one, two] => Some(PackedKey::Two(
                self.columns
                    .get(*one as usize)?
                    .as_slice()
                    .get(row)?
                    .clone(),
                self.columns
                    .get(*two as usize)?
                    .as_slice()
                    .get(row)?
                    .clone(),
            )),
            _ => None,
        }
    }

    fn concat_rows(&self, left_row: usize, right: &Self, right_row: usize, out: &mut Self) {
        for (output, column) in out.columns.iter_mut().zip(&self.columns) {
            output.push(column.as_slice()[left_row].clone());
        }
        for (output, column) in out.columns[self.arity()..].iter_mut().zip(&right.columns) {
            output.push(column.as_slice()[right_row].clone());
        }
        out.row_count += 1;
    }

    fn push_row_to(&self, row: usize, out: &mut Self) {
        for (output, column) in out.columns.iter_mut().zip(&self.columns) {
            output.push(column.as_slice()[row].clone());
        }
        if let (Some(input), Some(output)) = (&self.tuples, &mut out.tuples) {
            output.push(input[row].clone());
        }
        out.row_count += 1;
    }

    fn select_rows(&self, rows: &[usize], workspace: &mut BatchWorkspace) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .map(|column| {
                    let mut selected = workspace.column(rows.len());
                    selected.extend(rows.iter().map(|row| column.as_slice()[*row].clone()));
                    NativeColumn::Owned(selected)
                })
                .collect(),
            tuples: self.tuples.as_ref().map(|tuples| {
                let mut selected = workspace.tuples(rows.len());
                selected.extend(rows.iter().map(|row| tuples[*row].clone()));
                selected
            }),
            row_count: rows.len(),
        }
    }

    fn compare_rows(&self, left: usize, right: usize) -> Ordering {
        self.columns
            .iter()
            .map(|column| column.as_slice()[left].cmp(&column.as_slice()[right]))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    }

    fn compare_row_with(&self, row: usize, other: &Self, other_row: usize) -> Ordering {
        self.columns
            .iter()
            .zip(&other.columns)
            .map(|(left, right)| left.as_slice()[row].cmp(&right.as_slice()[other_row]))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    }

    fn canonicalize(&mut self, workspace: &mut BatchWorkspace) {
        if self.row_count <= 1 {
            return;
        }
        let mut order = workspace.row_indexes(self.row_count);
        order.extend(0..self.row_count);
        order.sort_unstable_by(|left, right| self.compare_rows(*left, *right));
        order.dedup_by(|right, left| self.compare_rows(*left, *right) == Ordering::Equal);
        let selected = self.select_rows(&order, workspace);
        workspace.recycle_row_indexes(order);
        let previous = std::mem::replace(self, selected);
        workspace.recycle_batch(previous);
    }

    fn into_tuples(self, workspace: &mut BatchWorkspace) -> Vec<Tuple> {
        let Self {
            columns,
            tuples,
            row_count,
        } = self;
        if let Some(tuples) = tuples {
            for column in columns {
                if let NativeColumn::Owned(mut values) = column {
                    values.clear();
                    workspace.columns.push(values);
                }
            }
            return tuples;
        }
        let mut output = workspace.tuples(row_count);
        output
            .extend((0..row_count).map(|row| {
                Tuple::new(columns.iter().map(|column| column.as_slice()[row].clone()))
            }));
        for column in columns {
            if let NativeColumn::Owned(mut values) = column {
                values.clear();
                workspace.columns.push(values);
            }
        }
        output
    }
}

pub(crate) fn execute_packed_query(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    if !packed_plan_eligible(plan, reader)? {
        return Ok(None);
    }
    if let Some(rows) = execute_cached_terminal_set(plan, reader)? {
        return Ok(Some(rows));
    }
    with_batch_workspace(|workspace| {
        Ok(execute_batch(plan, reader, workspace)?.map(|batch| batch.into_tuples(workspace)))
    })
}

pub(crate) fn execute_packed_relation_join<T>(
    left: PackedJoinInput<'_>,
    right: PackedJoinInput<'_>,
    left_positions: &[u16],
    right_positions: &[u16],
    consume: impl FnOnce(PackedRows<'_>) -> T,
) -> Result<Option<T>, KernelError> {
    let left_capabilities = match left.reader.relation_capabilities(left.relation) {
        Ok(capabilities) => capabilities,
        Err(KernelError::UnknownRelation(relation)) if relation == left.relation => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let right_capabilities = match right.reader.relation_capabilities(right.relation) {
        Ok(capabilities) => capabilities,
        Err(KernelError::UnknownRelation(relation)) if relation == right.relation => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if !batch_join_capabilities_eligible(&left_capabilities)
        || !batch_join_capabilities_eligible(&right_capabilities)
    {
        return Ok(None);
    }
    let Some(left) = left
        .reader
        .export_relation_batch(left.relation, left.bindings)?
    else {
        return Ok(None);
    };
    let Some(right) = right
        .reader
        .export_relation_batch(right.relation, right.bindings)?
    else {
        return Ok(None);
    };
    with_batch_workspace(|workspace| {
        let left = NativeBatch::from_packed(&left, workspace);
        let right = NativeBatch::from_packed(&right, workspace);
        let output = join(&left, &right, left_positions, right_positions, workspace);
        workspace.recycle_batch(left);
        workspace.recycle_batch(right);
        let result = output.as_ref().map(|batch| consume(PackedRows { batch }));
        if let Some(output) = output {
            workspace.recycle_batch(output);
        }
        Ok(result)
    })
}

fn batch_join_capabilities_eligible(capabilities: &crate::RelationCapabilities) -> bool {
    capabilities.supports_batch_export
        && capabilities.immediate_only()
        && matches!(capabilities.value_domains.len(), 1 | 2)
        && capabilities
            .cardinality
            .is_some_and(|rows| rows >= PACKED_ROW_THRESHOLD)
}

fn execute_cached_terminal_set(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    match plan {
        PhysicalQueryPlan::Project { input, positions } => {
            let PhysicalQueryPlan::JoinEq {
                left,
                right,
                left_positions,
                right_positions,
            } = input.as_ref()
            else {
                return Ok(None);
            };
            if positions.as_slice() != [0]
                || left_positions.as_slice() != [0]
                || right_positions.as_slice() != [0]
            {
                return Ok(None);
            }
            let Some((left, right)) = cached_scan_pair(left, right, reader)? else {
                return Ok(None);
            };
            if left.columns().len() != 1 || right.columns().len() != 1 {
                return Ok(None);
            }
            Ok(Some(merge_cached_intersection(left.rows(), right.rows())))
        }
        PhysicalQueryPlan::Union { left, right } => {
            let Some((left, right)) = cached_scan_pair(left, right, reader)? else {
                return Ok(None);
            };
            Ok(Some(merge_cached_union(left.rows(), right.rows())))
        }
        PhysicalQueryPlan::Difference { left, right } => {
            let Some((left, right)) = cached_scan_pair(left, right, reader)? else {
                return Ok(None);
            };
            Ok(Some(merge_cached_difference(left.rows(), right.rows())))
        }
        PhysicalQueryPlan::SemiJoin {
            left,
            right,
            left_positions,
            right_positions,
        } => cached_terminal_semi_join(left, right, left_positions, right_positions, reader, true),
        PhysicalQueryPlan::AntiJoin {
            left,
            right,
            left_positions,
            right_positions,
        } => cached_terminal_semi_join(left, right, left_positions, right_positions, reader, false),
        _ => Ok(None),
    }
}

fn cached_scan_pair(
    left: &PhysicalQueryPlan,
    right: &PhysicalQueryPlan,
    reader: &impl RelationRead,
) -> Result<Option<PackedPair>, KernelError> {
    let PhysicalQueryPlan::Scan {
        relation: left_relation,
        bindings: left_bindings,
    } = left
    else {
        return Ok(None);
    };
    let PhysicalQueryPlan::Scan {
        relation: right_relation,
        bindings: right_bindings,
    } = right
    else {
        return Ok(None);
    };
    let Some(left) = reader.export_relation_batch(*left_relation, left_bindings)? else {
        return Ok(None);
    };
    let Some(right) = reader.export_relation_batch(*right_relation, right_bindings)? else {
        return Ok(None);
    };
    Ok(Some((left, right)))
}

fn merge_cached_intersection(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
    let mut output = Vec::with_capacity(left.len().min(right.len()));
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.len() && right_row < right.len() {
        match left[left_row].cmp(&right[right_row]) {
            Ordering::Less => left_row += 1,
            Ordering::Equal => {
                output.push(left[left_row].clone());
                left_row += 1;
                right_row += 1;
            }
            Ordering::Greater => right_row += 1,
        }
    }
    output
}

fn merge_cached_union(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
    let mut output = Vec::with_capacity(left.len() + right.len());
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.len() || right_row < right.len() {
        match (left.get(left_row), right.get(right_row)) {
            (Some(left), Some(right)) => match left.cmp(right) {
                Ordering::Less => {
                    output.push(left.clone());
                    left_row += 1;
                }
                Ordering::Equal => {
                    output.push(left.clone());
                    left_row += 1;
                    right_row += 1;
                }
                Ordering::Greater => {
                    output.push(right.clone());
                    right_row += 1;
                }
            },
            (Some(left), None) => {
                output.push(left.clone());
                left_row += 1;
            }
            (None, Some(right)) => {
                output.push(right.clone());
                right_row += 1;
            }
            (None, None) => break,
        }
    }
    output
}

fn merge_cached_difference(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
    let mut output = Vec::with_capacity(left.len());
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.len() {
        if right_row == right.len() {
            output.extend(left[left_row..].iter().cloned());
            break;
        }
        match left[left_row].cmp(&right[right_row]) {
            Ordering::Less => {
                output.push(left[left_row].clone());
                left_row += 1;
            }
            Ordering::Equal => {
                left_row += 1;
                right_row += 1;
            }
            Ordering::Greater => right_row += 1,
        }
    }
    output
}

fn cached_terminal_semi_join(
    left: &PhysicalQueryPlan,
    right: &PhysicalQueryPlan,
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
    keep_matches: bool,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    if !positions_are_natural_prefix(left_positions)
        || !positions_are_natural_prefix(right_positions)
        || left_positions.len() != right_positions.len()
        || !matches!(left_positions.len(), 1 | 2)
    {
        return Ok(None);
    }
    let Some((left, right)) = cached_scan_pair(left, right, reader)? else {
        return Ok(None);
    };
    with_batch_workspace(|workspace| {
        let left_batch = NativeBatch::from_packed(&left, workspace);
        let right_batch = NativeBatch::from_packed(&right, workspace);
        let selected = merge_semi_join(
            &left_batch,
            &right_batch,
            left_positions,
            right_positions,
            keep_matches,
            workspace,
        );
        workspace.recycle_batch(left_batch);
        workspace.recycle_batch(right_batch);
        Ok(selected.map(|batch| batch.into_tuples(workspace)))
    })
}

fn packed_plan_eligible(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
) -> Result<bool, KernelError> {
    let mut has_large_input = false;
    let eligible = visit_scans(plan, &mut |relation| {
        let capabilities = reader.relation_capabilities(relation)?;
        has_large_input |= capabilities
            .cardinality
            .is_some_and(|rows| rows >= PACKED_ROW_THRESHOLD);
        Ok(capabilities.supports_batch_export
            && capabilities.immediate_only()
            && matches!(capabilities.value_domains.len(), 1 | 2))
    })?;
    Ok(eligible && has_large_input)
}

fn visit_scans(
    plan: &PhysicalQueryPlan,
    visit: &mut impl FnMut(crate::RelationId) -> Result<bool, KernelError>,
) -> Result<bool, KernelError> {
    match plan {
        PhysicalQueryPlan::Scan { relation, .. } => visit(*relation),
        PhysicalQueryPlan::Project { input, .. } => visit_scans(input, visit),
        PhysicalQueryPlan::JoinEq { left, right, .. }
        | PhysicalQueryPlan::SemiJoin { left, right, .. }
        | PhysicalQueryPlan::AntiJoin { left, right, .. }
        | PhysicalQueryPlan::Union { left, right }
        | PhysicalQueryPlan::Difference { left, right } => {
            Ok(visit_scans(left, visit)? && visit_scans(right, visit)?)
        }
    }
}

fn execute_batch(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
    workspace: &mut BatchWorkspace,
) -> Result<Option<NativeBatch>, KernelError> {
    match plan {
        PhysicalQueryPlan::Scan { relation, bindings } => {
            if let Some(batch) = reader.export_relation_batch(*relation, bindings)? {
                return Ok(Some(NativeBatch::from_packed(&batch, workspace)));
            }
            let rows = reader.scan_relation(*relation, bindings)?;
            Ok(NativeBatch::from_tuples(rows, bindings.len()))
        }
        PhysicalQueryPlan::Project { input, positions } => {
            let Some(input) = execute_batch(input, reader, workspace)? else {
                return Ok(None);
            };
            let output = input.project(positions, workspace);
            workspace.recycle_batch(input);
            Ok(output)
        }
        PhysicalQueryPlan::JoinEq {
            left,
            right,
            left_positions,
            right_positions,
        } => {
            let Some(left) = execute_batch(left, reader, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, workspace)? else {
                workspace.recycle_batch(left);
                return Ok(None);
            };
            let output = join(&left, &right, left_positions, right_positions, workspace);
            workspace.recycle_batch(left);
            workspace.recycle_batch(right);
            Ok(output)
        }
        PhysicalQueryPlan::SemiJoin {
            left,
            right,
            left_positions,
            right_positions,
        } => execute_batch_semi_join(
            left,
            right,
            left_positions,
            right_positions,
            reader,
            true,
            workspace,
        ),
        PhysicalQueryPlan::AntiJoin {
            left,
            right,
            left_positions,
            right_positions,
        } => execute_batch_semi_join(
            left,
            right,
            left_positions,
            right_positions,
            reader,
            false,
            workspace,
        ),
        PhysicalQueryPlan::Union { left, right } => {
            let Some(left) = execute_batch(left, reader, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, workspace)? else {
                workspace.recycle_batch(left);
                return Ok(None);
            };
            let output = union(&left, &right, workspace);
            workspace.recycle_batch(left);
            workspace.recycle_batch(right);
            Ok(output)
        }
        PhysicalQueryPlan::Difference { left, right } => {
            let Some(left) = execute_batch(left, reader, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, workspace)? else {
                workspace.recycle_batch(left);
                return Ok(None);
            };
            let output = difference(&left, &right, workspace);
            workspace.recycle_batch(left);
            workspace.recycle_batch(right);
            Ok(output)
        }
    }
}

fn join(
    left: &NativeBatch,
    right: &NativeBatch,
    left_positions: &[u16],
    right_positions: &[u16],
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    if left_positions.len() != right_positions.len() || !matches!(left_positions.len(), 1 | 2) {
        return None;
    }
    if positions_are_natural_prefix(left_positions) && positions_are_natural_prefix(right_positions)
    {
        return merge_join(left, right, left_positions, right_positions, workspace);
    }
    let mut right_index = BTreeMap::<PackedKey, Vec<usize>>::new();
    for row in 0..right.row_count {
        right_index
            .entry(right.key(row, right_positions)?)
            .or_default()
            .push(row);
    }
    let mut output = NativeBatch::empty(
        left.arity() + right.arity(),
        left.row_count.max(right.row_count),
        workspace,
    );
    for left_row in 0..left.row_count {
        let key = left.key(left_row, left_positions)?;
        let Some(right_rows) = right_index.get(&key) else {
            continue;
        };
        for right_row in right_rows {
            left.concat_rows(left_row, right, *right_row, &mut output);
        }
    }
    output.canonicalize(workspace);
    Some(output)
}

fn positions_are_natural_prefix(positions: &[u16]) -> bool {
    positions.iter().copied().eq(0..positions.len() as u16)
}

fn merge_join(
    left: &NativeBatch,
    right: &NativeBatch,
    left_positions: &[u16],
    right_positions: &[u16],
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    let mut output = NativeBatch::empty(
        left.arity() + right.arity(),
        left.row_count.max(right.row_count),
        workspace,
    );
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.row_count && right_row < right.row_count {
        let left_key = left.key(left_row, left_positions)?;
        let right_key = right.key(right_row, right_positions)?;
        match left_key.cmp(&right_key) {
            Ordering::Less => left_row = key_run_end(left, left_row, left_positions, &left_key)?,
            Ordering::Greater => {
                right_row = key_run_end(right, right_row, right_positions, &right_key)?
            }
            Ordering::Equal => {
                let left_end = key_run_end(left, left_row, left_positions, &left_key)?;
                let right_end = key_run_end(right, right_row, right_positions, &right_key)?;
                for matching_left in left_row..left_end {
                    for matching_right in right_row..right_end {
                        left.concat_rows(matching_left, right, matching_right, &mut output);
                    }
                }
                left_row = left_end;
                right_row = right_end;
            }
        }
    }
    Some(output)
}

fn key_run_end(
    batch: &NativeBatch,
    start: usize,
    positions: &[u16],
    key: &PackedKey,
) -> Option<usize> {
    let mut end = start + 1;
    while end < batch.row_count && batch.key(end, positions)?.eq(key) {
        end += 1;
    }
    Some(end)
}

fn execute_batch_semi_join(
    left: &PhysicalQueryPlan,
    right: &PhysicalQueryPlan,
    left_positions: &[u16],
    right_positions: &[u16],
    reader: &impl RelationRead,
    keep_matches: bool,
    workspace: &mut BatchWorkspace,
) -> Result<Option<NativeBatch>, KernelError> {
    let Some(left) = execute_batch(left, reader, workspace)? else {
        return Ok(None);
    };
    let Some(right) = execute_batch(right, reader, workspace)? else {
        workspace.recycle_batch(left);
        return Ok(None);
    };
    if left_positions.len() != right_positions.len() || !matches!(left_positions.len(), 1 | 2) {
        workspace.recycle_batch(left);
        workspace.recycle_batch(right);
        return Ok(None);
    }
    let output = if positions_are_natural_prefix(left_positions)
        && positions_are_natural_prefix(right_positions)
    {
        merge_semi_join(
            &left,
            &right,
            left_positions,
            right_positions,
            keep_matches,
            workspace,
        )
    } else {
        let mut right_keys = BTreeSet::new();
        for row in 0..right.row_count {
            let Some(key) = right.key(row, right_positions) else {
                workspace.recycle_batch(left);
                workspace.recycle_batch(right);
                return Ok(None);
            };
            right_keys.insert(key);
        }
        let mut selected = workspace.row_indexes(left.row_count);
        for row in 0..left.row_count {
            let Some(key) = left.key(row, left_positions) else {
                workspace.recycle_row_indexes(selected);
                workspace.recycle_batch(left);
                workspace.recycle_batch(right);
                return Ok(None);
            };
            if right_keys.contains(&key) == keep_matches {
                selected.push(row);
            }
        }
        let output = left.select_rows(&selected, workspace);
        workspace.recycle_row_indexes(selected);
        Some(output)
    };
    workspace.recycle_batch(left);
    workspace.recycle_batch(right);
    Ok(output)
}

fn merge_semi_join(
    left: &NativeBatch,
    right: &NativeBatch,
    left_positions: &[u16],
    right_positions: &[u16],
    keep_matches: bool,
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    let mut selected = workspace.row_indexes(left.row_count);
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.row_count {
        let left_key = left.key(left_row, left_positions)?;
        let left_end = key_run_end(left, left_row, left_positions, &left_key)?;
        while right_row < right.row_count {
            let right_key = right.key(right_row, right_positions)?;
            if right_key >= left_key {
                break;
            }
            right_row = key_run_end(right, right_row, right_positions, &right_key)?;
        }
        let matched =
            right_row < right.row_count && right.key(right_row, right_positions)?.eq(&left_key);
        if matched == keep_matches {
            selected.extend(left_row..left_end);
        }
        left_row = left_end;
    }
    let output = left.select_rows(&selected, workspace);
    workspace.recycle_row_indexes(selected);
    Some(output)
}

fn union(
    left: &NativeBatch,
    right: &NativeBatch,
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    if left.arity() != right.arity() {
        return None;
    }
    let preserve_tuples = left.tuples.is_some() && right.tuples.is_some();
    let mut output = NativeBatch::empty(left.arity(), left.row_count + right.row_count, workspace);
    if preserve_tuples {
        output.tuples = Some(workspace.tuples(left.row_count + right.row_count));
    }
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.row_count || right_row < right.row_count {
        if left_row == left.row_count {
            right.push_row_to(right_row, &mut output);
            right_row += 1;
            continue;
        }
        if right_row == right.row_count {
            left.push_row_to(left_row, &mut output);
            left_row += 1;
            continue;
        }
        match left.compare_row_with(left_row, right, right_row) {
            Ordering::Less => {
                left.push_row_to(left_row, &mut output);
                left_row += 1;
            }
            Ordering::Equal => {
                left.push_row_to(left_row, &mut output);
                left_row += 1;
                right_row += 1;
            }
            Ordering::Greater => {
                right.push_row_to(right_row, &mut output);
                right_row += 1;
            }
        }
    }
    Some(output)
}

fn difference(
    left: &NativeBatch,
    right: &NativeBatch,
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    if left.arity() != right.arity() {
        return None;
    }
    let mut selected = workspace.row_indexes(left.row_count);
    let mut left_row = 0usize;
    let mut right_row = 0usize;
    while left_row < left.row_count {
        if right_row == right.row_count {
            selected.extend(left_row..left.row_count);
            break;
        }
        match left.compare_row_with(left_row, right, right_row) {
            Ordering::Less => {
                selected.push(left_row);
                left_row += 1;
            }
            Ordering::Equal => {
                left_row += 1;
                right_row += 1;
            }
            Ordering::Greater => right_row += 1,
        }
    }
    let output = left.select_rows(&selected, workspace);
    workspace.recycle_row_indexes(selected);
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    #[test]
    fn native_batch_projects_and_canonicalizes_without_tuple_intermediates() {
        let mut workspace = BatchWorkspace::default();
        let rows = vec![
            Tuple::from([int(2), int(20)]),
            Tuple::from([int(1), int(10)]),
            Tuple::from([int(1), int(10)]),
        ];
        let projected = NativeBatch::from_tuples(rows, 2)
            .unwrap()
            .project(&[0], &mut workspace)
            .unwrap();

        assert_eq!(
            projected.into_tuples(&mut workspace),
            vec![Tuple::from([int(1)]), Tuple::from([int(2)])]
        );
    }

    #[test]
    fn native_batch_join_and_difference_match_set_semantics() {
        let mut workspace = BatchWorkspace::default();
        let left = NativeBatch::from_tuples(
            vec![
                Tuple::from([int(1), int(10)]),
                Tuple::from([int(2), int(20)]),
            ],
            2,
        )
        .unwrap();
        let right = NativeBatch::from_tuples(
            vec![
                Tuple::from([int(10), int(100)]),
                Tuple::from([int(30), int(300)]),
            ],
            2,
        )
        .unwrap();
        let joined = join(&left, &right, &[1], &[0], &mut workspace).unwrap();
        assert_eq!(
            joined.into_tuples(&mut workspace),
            vec![Tuple::from([int(1), int(10), int(10), int(100)])]
        );

        let removed = NativeBatch::from_tuples(vec![Tuple::from([int(1), int(10)])], 2).unwrap();
        let difference = difference(&left, &removed, &mut workspace).unwrap();
        assert_eq!(
            difference.into_tuples(&mut workspace),
            vec![Tuple::from([int(2), int(20)])]
        );
    }

    #[test]
    fn batch_workspace_reuses_owned_intermediate_columns() {
        let left_rows = vec![
            Tuple::from([int(1), int(10)]),
            Tuple::from([int(2), int(20)]),
        ];
        let right_rows = vec![
            Tuple::from([int(10), int(100)]),
            Tuple::from([int(20), int(200)]),
        ];
        let mut workspace = BatchWorkspace::default();

        let mut retained_columns = None;
        for _ in 0..2 {
            let left = NativeBatch::from_tuples(left_rows.clone(), 2).unwrap();
            let right = NativeBatch::from_tuples(right_rows.clone(), 2).unwrap();
            let joined = join(&left, &right, &[1], &[0], &mut workspace).unwrap();
            assert_eq!(joined.into_tuples(&mut workspace).len(), 2);
            if let Some(retained_columns) = retained_columns {
                assert_eq!(
                    workspace.columns.len(),
                    retained_columns,
                    "the second execution should return the same column buffers",
                );
            } else {
                retained_columns = Some(workspace.columns.len());
                assert!(workspace.columns.len() >= 4);
            }
        }
    }
}
