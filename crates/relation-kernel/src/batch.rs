// Copyright (C) 2026 Ryan Daum <ryan@timbran.org>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, version 3.

use crate::execution::ParallelUnavailable;
use crate::metrics::{
    EqualityJoinAccelerationPlacement, record_equality_join_acceleration_duration,
    record_equality_join_acceleration_placement, record_equality_join_input_rows,
    record_equality_join_materialization_duration, record_equality_join_output_rows,
};
use crate::metrics::{
    MembershipAccelerationPlacement, ParallelMembershipPlacement, ParallelUnionPlacement,
    record_membership_acceleration_duration, record_membership_acceleration_placement,
    record_membership_input_rows, record_membership_materialization_duration,
    record_membership_selected_rows, record_parallel_membership_placement,
    record_parallel_union_duration, record_parallel_union_placement,
};
use crate::query::PhysicalQueryPlan;
use crate::{
    AccelerationDecline, AccelerationOutcome, EqualityJoin, EqualityJoinMatch, ExecutionContext,
    KernelError, MembershipSelection, RelationRead, Tuple,
};
use mica_var::Value;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

const PACKED_ROW_THRESHOLD: usize = 256;
const ACCELERATOR_EQUALITY_JOIN_ROW_THRESHOLD: usize = 262_144;
const ACCELERATOR_EQUALITY_JOIN_UNBALANCED_ROW_THRESHOLD: usize = 2_097_152;
const ACCELERATOR_EQUALITY_JOIN_MIN_SIDE_ROWS: usize = 4_096;
const DIFFERENTIAL_EQUALITY_JOIN_ROW_THRESHOLD: usize = 262_144;
const DIFFERENTIAL_EQUALITY_JOIN_MIN_DELTA_ROWS: usize = 4_096;
const ACCELERATOR_MEMBERSHIP_ROW_THRESHOLD: usize = 262_144;
const PARALLEL_MEMBERSHIP_ROW_THRESHOLD: usize = 1_048_576;
const PARALLEL_MEMBERSHIP_WORKERS: NonZeroUsize = NonZeroUsize::new(1).unwrap();
const PARALLEL_UNION_ROW_THRESHOLD: usize = 2_097_152;
const PARALLEL_UNION_WORKERS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
type PackedPair = (Arc<crate::PackedRelation>, Arc<crate::PackedRelation>);

pub(crate) struct PackedJoinInput<'a> {
    pub reader: &'a dyn RelationRead,
    pub relation: crate::RelationId,
    pub bindings: &'a [Option<Value>],
}

pub(crate) struct PackedRows<'a> {
    batch: &'a NativeBatch,
}

#[derive(Clone, Copy)]
struct MembershipPlacement {
    left_position: usize,
    right_position: usize,
    keep_matches: bool,
    accelerator_row_threshold: usize,
    row_threshold: usize,
}

#[derive(Clone, Copy)]
struct EqualityJoinPlacement {
    row_threshold: usize,
    unbalanced_row_threshold: usize,
    min_side_rows: usize,
}

struct SemiJoinSpec<'a> {
    left_positions: &'a [u16],
    right_positions: &'a [u16],
    keep_matches: bool,
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
        if let Some(NativeRows::Owned(mut rows)) = batch.rows {
            rows.clear();
            self.tuple_buffers.push(rows);
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
    rows: Option<NativeRows>,
    row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NativeRows {
    Shared(Arc<[Tuple]>),
    Owned(Vec<Tuple>),
}

impl NativeRows {
    fn as_slice(&self) -> &[Tuple] {
        match self {
            Self::Shared(rows) => rows,
            Self::Owned(rows) => rows,
        }
    }
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

    fn shared(&self) -> Option<&Arc<[Value]>> {
        match self {
            Self::Shared(values) => Some(values),
            Self::Owned(_) => None,
        }
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
            rows: Some(NativeRows::Owned(rows)),
        })
    }

    fn empty(arity: usize, capacity: usize, workspace: &mut BatchWorkspace) -> Self {
        Self {
            columns: (0..arity)
                .map(|_| NativeColumn::Owned(workspace.column(capacity)))
                .collect(),
            rows: None,
            row_count: 0,
        }
    }

    fn from_packed(batch: &crate::PackedRelation) -> Self {
        Self {
            columns: batch
                .columns()
                .iter()
                .cloned()
                .map(NativeColumn::Shared)
                .collect(),
            rows: Some(NativeRows::Shared(batch.shared_rows())),
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
            rows: positions
                .iter()
                .copied()
                .eq(0..self.arity() as u16)
                .then(|| self.rows.clone())
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
        if let (Some(input), Some(NativeRows::Owned(output))) = (&self.rows, &mut out.rows) {
            output.push(input.as_slice()[row].clone());
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
            rows: self.rows.as_ref().map(|source| {
                let mut selected = workspace.tuples(rows.len());
                selected.extend(rows.iter().map(|row| source.as_slice()[*row].clone()));
                NativeRows::Owned(selected)
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
            rows,
            row_count,
        } = self;
        if let Some(rows) = rows {
            for column in columns {
                if let NativeColumn::Owned(mut values) = column {
                    values.clear();
                    workspace.columns.push(values);
                }
            }
            return match rows {
                NativeRows::Shared(rows) => rows.as_ref().to_vec(),
                NativeRows::Owned(rows) => rows,
            };
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
    execution_context: &ExecutionContext,
) -> Result<Option<Vec<Tuple>>, KernelError> {
    if !packed_plan_eligible(plan, reader)? {
        return Ok(None);
    }
    if let Some(rows) = execute_cached_terminal_set(plan, reader, execution_context)? {
        return Ok(Some(rows));
    }
    with_batch_workspace(|workspace| {
        Ok(execute_batch(plan, reader, execution_context, workspace)?
            .map(|batch| batch.into_tuples(workspace)))
    })
}

pub(crate) fn execute_packed_relation_join<T>(
    left: PackedJoinInput<'_>,
    right: PackedJoinInput<'_>,
    left_positions: &[u16],
    right_positions: &[u16],
    execution_context: &ExecutionContext,
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
        let left = NativeBatch::from_packed(&left);
        let right = NativeBatch::from_packed(&right);
        let output = join(
            &left,
            &right,
            left_positions,
            right_positions,
            execution_context,
            workspace,
        );
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
    execution_context: &ExecutionContext,
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
            Ok(Some(place_cached_union(
                left.rows(),
                right.rows(),
                execution_context,
            )))
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

fn place_cached_union(
    left: &[Tuple],
    right: &[Tuple],
    execution_context: &ExecutionContext,
) -> Vec<Tuple> {
    place_cached_union_with_threshold(left, right, execution_context, PARALLEL_UNION_ROW_THRESHOLD)
}

fn place_cached_union_with_threshold(
    left: &[Tuple],
    right: &[Tuple],
    execution_context: &ExecutionContext,
    row_threshold: usize,
) -> Vec<Tuple> {
    let input_rows = left.len() + right.len();
    if input_rows < row_threshold {
        record_parallel_union_placement(ParallelUnionPlacement::BelowThreshold, input_rows);
        return merge_cached_union(left, right);
    }

    let Some((left_split, right_split)) = balanced_union_split(left, right) else {
        record_parallel_union_placement(ParallelUnionPlacement::Unbalanced, input_rows);
        return merge_cached_union(left, right);
    };
    let started = Instant::now();
    let result = execution_context.try_join(
        PARALLEL_UNION_WORKERS,
        || merge_cached_union(&left[..left_split], &right[..right_split]),
        || merge_cached_union(&left[left_split..], &right[right_split..]),
    );
    let (mut lower, upper) = match result {
        Ok(output) => output,
        Err(ParallelUnavailable::NoExecutor) => {
            record_parallel_union_placement(ParallelUnionPlacement::NoExecutor, input_rows);
            return merge_cached_union(left, right);
        }
        Err(ParallelUnavailable::Capacity) => {
            record_parallel_union_placement(ParallelUnionPlacement::Capacity, input_rows);
            return merge_cached_union(left, right);
        }
    };
    lower.extend(upper);
    record_parallel_union_placement(ParallelUnionPlacement::Parallel, input_rows);
    record_parallel_union_duration(started.elapsed());
    lower
}

fn balanced_union_split(left: &[Tuple], right: &[Tuple]) -> Option<(usize, usize)> {
    let pivot = match (left.get(left.len() / 2), right.get(right.len() / 2)) {
        (Some(left), Some(right)) => left.max(right),
        (Some(left), None) => left,
        (None, Some(right)) => right,
        (None, None) => return None,
    };
    let left_split = left.partition_point(|tuple| tuple < pivot);
    let right_split = right.partition_point(|tuple| tuple < pivot);
    let total = left.len() + right.len();
    let lower = left_split + right_split;
    let upper = total - lower;
    (lower.min(upper) >= total.div_ceil(4)).then_some((left_split, right_split))
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
        let left_batch = NativeBatch::from_packed(&left);
        let right_batch = NativeBatch::from_packed(&right);
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
    let eligible = packed_sources_eligible(plan, reader, &mut has_large_input)?;
    Ok(eligible && has_large_input)
}

fn packed_sources_eligible(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
    has_large_input: &mut bool,
) -> Result<bool, KernelError> {
    match plan {
        PhysicalQueryPlan::Scan { relation, bindings } => {
            if bindings.iter().any(Option::is_some) {
                return Ok(false);
            }
            let capabilities = reader.relation_capabilities(*relation)?;
            *has_large_input |= capabilities
                .cardinality
                .is_some_and(|rows| rows >= PACKED_ROW_THRESHOLD);
            Ok(capabilities.supports_batch_export
                && capabilities.immediate_only()
                && matches!(capabilities.value_domains.len(), 1 | 2))
        }
        PhysicalQueryPlan::Input { relation, packed } => {
            *has_large_input |= relation.len() >= PACKED_ROW_THRESHOLD;
            Ok(packed.is_some())
        }
        PhysicalQueryPlan::Project { input, .. } => {
            packed_sources_eligible(input, reader, has_large_input)
        }
        PhysicalQueryPlan::JoinEq { left, right, .. }
        | PhysicalQueryPlan::SemiJoin { left, right, .. }
        | PhysicalQueryPlan::AntiJoin { left, right, .. }
        | PhysicalQueryPlan::Union { left, right }
        | PhysicalQueryPlan::Difference { left, right } => {
            Ok(packed_sources_eligible(left, reader, has_large_input)?
                && packed_sources_eligible(right, reader, has_large_input)?)
        }
    }
}

fn execute_batch(
    plan: &PhysicalQueryPlan,
    reader: &impl RelationRead,
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
) -> Result<Option<NativeBatch>, KernelError> {
    match plan {
        PhysicalQueryPlan::Scan { relation, bindings } => {
            if let Some(batch) = reader.export_relation_batch(*relation, bindings)? {
                return Ok(Some(NativeBatch::from_packed(&batch)));
            }
            let rows = reader.scan_relation(*relation, bindings)?;
            Ok(NativeBatch::from_tuples(rows, bindings.len()))
        }
        PhysicalQueryPlan::Input { packed, .. } => {
            Ok(packed.as_deref().map(NativeBatch::from_packed))
        }
        PhysicalQueryPlan::Project { input, positions } => {
            let Some(input) = execute_batch(input, reader, execution_context, workspace)? else {
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
            let Some(left) = execute_batch(left, reader, execution_context, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, execution_context, workspace)? else {
                workspace.recycle_batch(left);
                return Ok(None);
            };
            let output = join(
                &left,
                &right,
                left_positions,
                right_positions,
                execution_context,
                workspace,
            );
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
            reader,
            execution_context,
            workspace,
            SemiJoinSpec {
                left_positions,
                right_positions,
                keep_matches: true,
            },
        ),
        PhysicalQueryPlan::AntiJoin {
            left,
            right,
            left_positions,
            right_positions,
        } => execute_batch_semi_join(
            left,
            right,
            reader,
            execution_context,
            workspace,
            SemiJoinSpec {
                left_positions,
                right_positions,
                keep_matches: false,
            },
        ),
        PhysicalQueryPlan::Union { left, right } => {
            let Some(left) = execute_batch(left, reader, execution_context, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, execution_context, workspace)? else {
                workspace.recycle_batch(left);
                return Ok(None);
            };
            let output = union(&left, &right, workspace);
            workspace.recycle_batch(left);
            workspace.recycle_batch(right);
            Ok(output)
        }
        PhysicalQueryPlan::Difference { left, right } => {
            let Some(left) = execute_batch(left, reader, execution_context, workspace)? else {
                return Ok(None);
            };
            let Some(right) = execute_batch(right, reader, execution_context, workspace)? else {
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
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
) -> Option<NativeBatch> {
    if left_positions.len() != right_positions.len() || !matches!(left_positions.len(), 1 | 2) {
        return None;
    }
    if let Some(output) = place_accelerated_equality_join(
        left,
        right,
        left_positions,
        right_positions,
        execution_context,
        workspace,
        EqualityJoinPlacement {
            row_threshold: ACCELERATOR_EQUALITY_JOIN_ROW_THRESHOLD,
            unbalanced_row_threshold: ACCELERATOR_EQUALITY_JOIN_UNBALANCED_ROW_THRESHOLD,
            min_side_rows: ACCELERATOR_EQUALITY_JOIN_MIN_SIDE_ROWS,
        },
    ) {
        return Some(output);
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

pub(crate) fn execute_differential_equality_join(
    delta: &crate::PackedRelation,
    full: &crate::PackedRelation,
    delta_positions: &[u16],
    full_positions: &[u16],
    execution_context: &ExecutionContext,
) -> Option<Vec<EqualityJoinMatch>> {
    execute_differential_equality_join_with_thresholds(
        delta,
        full,
        delta_positions,
        full_positions,
        execution_context,
        DIFFERENTIAL_EQUALITY_JOIN_ROW_THRESHOLD,
        DIFFERENTIAL_EQUALITY_JOIN_MIN_DELTA_ROWS,
    )
}

fn execute_differential_equality_join_with_thresholds(
    delta: &crate::PackedRelation,
    full: &crate::PackedRelation,
    delta_positions: &[u16],
    full_positions: &[u16],
    execution_context: &ExecutionContext,
    row_threshold: usize,
    min_delta_rows: usize,
) -> Option<Vec<EqualityJoinMatch>> {
    if delta_positions.len() != full_positions.len() || !matches!(delta_positions.len(), 1 | 2) {
        return None;
    }
    let input_rows = delta.row_count().saturating_add(full.row_count());
    record_equality_join_input_rows(input_rows);
    if delta.row_count().saturating_add(full.row_count()) < row_threshold
        || delta.row_count() < min_delta_rows
    {
        record_equality_join_acceleration_placement(
            EqualityJoinAccelerationPlacement::BelowThreshold,
        );
        return None;
    }
    if !execution_context.has_accelerator() {
        record_equality_join_acceleration_placement(EqualityJoinAccelerationPlacement::Unavailable);
        return None;
    }
    let delta_columns = delta_positions
        .iter()
        .map(|position| delta.columns().get(*position as usize).cloned())
        .collect::<Option<Vec<_>>>()?;
    let full_columns = full_positions
        .iter()
        .map(|position| full.columns().get(*position as usize).cloned())
        .collect::<Option<Vec<_>>>()?;
    let started = Instant::now();
    match execution_context.join_equality(EqualityJoin {
        left: &delta_columns,
        right: &full_columns,
    }) {
        AccelerationOutcome::Completed(matches)
            if equality_join_matches_are_valid(&matches, delta.row_count(), full.row_count()) =>
        {
            record_equality_join_acceleration_placement(
                EqualityJoinAccelerationPlacement::Accelerated,
            );
            record_equality_join_acceleration_duration(started.elapsed());
            record_equality_join_output_rows(matches.len());
            Some(matches)
        }
        AccelerationOutcome::Completed(_) => {
            record_equality_join_acceleration_placement(
                EqualityJoinAccelerationPlacement::InvalidResult,
            );
            None
        }
        AccelerationOutcome::Declined(decline) => {
            record_equality_join_acceleration_placement(match decline {
                AccelerationDecline::Busy => EqualityJoinAccelerationPlacement::Busy,
                AccelerationDecline::UnsupportedInput => {
                    EqualityJoinAccelerationPlacement::UnsupportedInput
                }
                AccelerationDecline::UnsupportedDomain => {
                    EqualityJoinAccelerationPlacement::UnsupportedDomain
                }
                AccelerationDecline::Unavailable => EqualityJoinAccelerationPlacement::Unavailable,
                AccelerationDecline::Failed => EqualityJoinAccelerationPlacement::Failed,
            });
            None
        }
    }
}

pub(crate) fn differential_equality_join_eligible(delta_rows: usize, full_rows: usize) -> bool {
    delta_rows >= DIFFERENTIAL_EQUALITY_JOIN_MIN_DELTA_ROWS
        && delta_rows.saturating_add(full_rows) >= DIFFERENTIAL_EQUALITY_JOIN_ROW_THRESHOLD
}

pub(crate) fn execute_native_equality_join_matches(
    left: &crate::PackedRelation,
    right: &crate::PackedRelation,
    left_positions: &[u16],
    right_positions: &[u16],
) -> Option<Vec<EqualityJoinMatch>> {
    if left_positions.len() != right_positions.len() || !matches!(left_positions.len(), 1 | 2) {
        return None;
    }
    let mut right_index = BTreeMap::<PackedKey, Vec<usize>>::new();
    for right_row in 0..right.row_count() {
        right_index
            .entry(packed_relation_key(right, right_row, right_positions)?)
            .or_default()
            .push(right_row);
    }
    let mut matches = Vec::new();
    for left_row in 0..left.row_count() {
        let key = packed_relation_key(left, left_row, left_positions)?;
        if let Some(right_rows) = right_index.get(&key) {
            matches.extend(right_rows.iter().map(|right_row| EqualityJoinMatch {
                left_row,
                right_row: *right_row,
            }));
        }
    }
    Some(matches)
}

fn packed_relation_key(
    relation: &crate::PackedRelation,
    row: usize,
    positions: &[u16],
) -> Option<PackedKey> {
    match positions {
        [one] => Some(PackedKey::One(
            relation.columns().get(*one as usize)?.get(row)?.clone(),
        )),
        [one, two] => Some(PackedKey::Two(
            relation.columns().get(*one as usize)?.get(row)?.clone(),
            relation.columns().get(*two as usize)?.get(row)?.clone(),
        )),
        _ => None,
    }
}

fn place_accelerated_equality_join(
    left: &NativeBatch,
    right: &NativeBatch,
    left_positions: &[u16],
    right_positions: &[u16],
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
    placement: EqualityJoinPlacement,
) -> Option<NativeBatch> {
    let input_rows = left.row_count.saturating_add(right.row_count);
    record_equality_join_input_rows(input_rows);
    if !equality_join_acceleration_eligible(left.row_count, right.row_count, placement) {
        record_equality_join_acceleration_placement(
            EqualityJoinAccelerationPlacement::BelowThreshold,
        );
        return None;
    }
    if !execution_context.has_accelerator() {
        record_equality_join_acceleration_placement(EqualityJoinAccelerationPlacement::Unavailable);
        return None;
    }
    let Some(left_columns) = shared_columns(left, left_positions) else {
        record_equality_join_acceleration_placement(
            EqualityJoinAccelerationPlacement::OwnedColumns,
        );
        return None;
    };
    let Some(right_columns) = shared_columns(right, right_positions) else {
        record_equality_join_acceleration_placement(
            EqualityJoinAccelerationPlacement::OwnedColumns,
        );
        return None;
    };

    let started = Instant::now();
    let matches = match execution_context.join_equality(EqualityJoin {
        left: &left_columns,
        right: &right_columns,
    }) {
        AccelerationOutcome::Completed(matches)
            if equality_join_matches_are_valid(&matches, left.row_count, right.row_count) =>
        {
            record_equality_join_acceleration_placement(
                EqualityJoinAccelerationPlacement::Accelerated,
            );
            record_equality_join_acceleration_duration(started.elapsed());
            matches
        }
        AccelerationOutcome::Completed(_) => {
            record_equality_join_acceleration_placement(
                EqualityJoinAccelerationPlacement::InvalidResult,
            );
            return None;
        }
        AccelerationOutcome::Declined(decline) => {
            record_equality_join_acceleration_placement(match decline {
                AccelerationDecline::Busy => EqualityJoinAccelerationPlacement::Busy,
                AccelerationDecline::UnsupportedInput => {
                    EqualityJoinAccelerationPlacement::UnsupportedInput
                }
                AccelerationDecline::UnsupportedDomain => {
                    EqualityJoinAccelerationPlacement::UnsupportedDomain
                }
                AccelerationDecline::Unavailable => EqualityJoinAccelerationPlacement::Unavailable,
                AccelerationDecline::Failed => EqualityJoinAccelerationPlacement::Failed,
            });
            return None;
        }
    };
    record_equality_join_output_rows(matches.len());
    let materialization_started = Instant::now();
    let output = materialize_equality_join(left, right, &matches, workspace);
    record_equality_join_materialization_duration(materialization_started.elapsed());
    Some(output)
}

fn equality_join_acceleration_eligible(
    left_rows: usize,
    right_rows: usize,
    placement: EqualityJoinPlacement,
) -> bool {
    let input_rows = left_rows.saturating_add(right_rows);
    if input_rows < placement.row_threshold {
        return false;
    }
    left_rows.min(right_rows) >= placement.min_side_rows
        || input_rows >= placement.unbalanced_row_threshold
}

fn shared_columns(batch: &NativeBatch, positions: &[u16]) -> Option<Vec<Arc<[Value]>>> {
    positions
        .iter()
        .map(|position| batch.columns.get(*position as usize)?.shared().cloned())
        .collect()
}

fn equality_join_matches_are_valid(
    matches: &[EqualityJoinMatch],
    left_rows: usize,
    right_rows: usize,
) -> bool {
    matches
        .iter()
        .all(|pair| pair.left_row < left_rows && pair.right_row < right_rows)
        && matches.windows(2).all(|window| {
            (window[0].left_row, window[0].right_row) < (window[1].left_row, window[1].right_row)
        })
}

fn materialize_equality_join(
    left: &NativeBatch,
    right: &NativeBatch,
    matches: &[EqualityJoinMatch],
    workspace: &mut BatchWorkspace,
) -> NativeBatch {
    let mut output = NativeBatch::empty(left.arity() + right.arity(), matches.len(), workspace);
    for pair in matches {
        left.concat_rows(pair.left_row, right, pair.right_row, &mut output);
    }
    output
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
    reader: &impl RelationRead,
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
    spec: SemiJoinSpec<'_>,
) -> Result<Option<NativeBatch>, KernelError> {
    let Some(left) = execute_batch(left, reader, execution_context, workspace)? else {
        return Ok(None);
    };
    let Some(right) = execute_batch(right, reader, execution_context, workspace)? else {
        workspace.recycle_batch(left);
        return Ok(None);
    };
    if spec.left_positions.len() != spec.right_positions.len()
        || !matches!(spec.left_positions.len(), 1 | 2)
    {
        workspace.recycle_batch(left);
        workspace.recycle_batch(right);
        return Ok(None);
    }
    let output = if positions_are_natural_prefix(spec.left_positions)
        && positions_are_natural_prefix(spec.right_positions)
    {
        merge_semi_join(
            &left,
            &right,
            spec.left_positions,
            spec.right_positions,
            spec.keep_matches,
            workspace,
        )
    } else if matches!((spec.left_positions, spec.right_positions), ([_], [_])) {
        let Some(selected) = select_membership_rows(
            &left,
            &right,
            spec.left_positions[0] as usize,
            spec.right_positions[0] as usize,
            spec.keep_matches,
            execution_context,
            workspace,
        ) else {
            workspace.recycle_batch(left);
            workspace.recycle_batch(right);
            return Ok(None);
        };
        record_membership_selected_rows(selected.len());
        let materialize_started = Instant::now();
        let output = left.select_rows(&selected, workspace);
        record_membership_materialization_duration(materialize_started.elapsed());
        workspace.recycle_row_indexes(selected);
        Some(output)
    } else {
        let mut right_keys = BTreeSet::new();
        for row in 0..right.row_count {
            let Some(key) = right.key(row, spec.right_positions) else {
                workspace.recycle_batch(left);
                workspace.recycle_batch(right);
                return Ok(None);
            };
            right_keys.insert(key);
        }
        let mut selected = workspace.row_indexes(left.row_count);
        for row in 0..left.row_count {
            let Some(key) = left.key(row, spec.left_positions) else {
                workspace.recycle_row_indexes(selected);
                workspace.recycle_batch(left);
                workspace.recycle_batch(right);
                return Ok(None);
            };
            if right_keys.contains(&key) == spec.keep_matches {
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

fn select_membership_rows(
    left: &NativeBatch,
    right: &NativeBatch,
    left_position: usize,
    right_position: usize,
    keep_matches: bool,
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
) -> Option<Vec<usize>> {
    select_membership_rows_with_threshold(
        left,
        right,
        MembershipPlacement {
            left_position,
            right_position,
            keep_matches,
            accelerator_row_threshold: ACCELERATOR_MEMBERSHIP_ROW_THRESHOLD,
            row_threshold: PARALLEL_MEMBERSHIP_ROW_THRESHOLD,
        },
        execution_context,
        workspace,
    )
}

fn select_membership_rows_with_threshold(
    left: &NativeBatch,
    right: &NativeBatch,
    placement: MembershipPlacement,
    execution_context: &ExecutionContext,
    workspace: &mut BatchWorkspace,
) -> Option<Vec<usize>> {
    let left_column = left.columns.get(placement.left_position)?;
    let right_column = right.columns.get(placement.right_position)?;
    let left_values = left_column.as_slice();
    let right_values = right_column.as_slice();
    let input_rows = left_values.len() + right_values.len();
    record_membership_input_rows(input_rows);
    if input_rows < placement.accelerator_row_threshold {
        record_membership_acceleration_placement(MembershipAccelerationPlacement::BelowThreshold);
    } else if let (Some(left), Some(right)) = (left_column.shared(), right_column.shared()) {
        let started = Instant::now();
        match execution_context.select_membership(MembershipSelection {
            left,
            right,
            keep_matches: placement.keep_matches,
        }) {
            AccelerationOutcome::Completed(selected)
                if selected_rows_are_valid(&selected, left_values.len()) =>
            {
                record_membership_acceleration_placement(
                    MembershipAccelerationPlacement::Accelerated,
                );
                record_membership_acceleration_duration(started.elapsed());
                return Some(selected);
            }
            AccelerationOutcome::Completed(_) => record_membership_acceleration_placement(
                MembershipAccelerationPlacement::InvalidResult,
            ),
            AccelerationOutcome::Declined(decline) => {
                record_membership_acceleration_placement(match decline {
                    AccelerationDecline::Busy => MembershipAccelerationPlacement::Busy,
                    AccelerationDecline::UnsupportedInput => {
                        MembershipAccelerationPlacement::UnsupportedInput
                    }
                    AccelerationDecline::UnsupportedDomain => {
                        MembershipAccelerationPlacement::UnsupportedDomain
                    }
                    AccelerationDecline::Unavailable => {
                        MembershipAccelerationPlacement::Unavailable
                    }
                    AccelerationDecline::Failed => MembershipAccelerationPlacement::Failed,
                });
            }
        }
    } else {
        record_membership_acceleration_placement(MembershipAccelerationPlacement::OwnedColumns);
    }
    let mut right_keys = workspace.column(right_values.len());
    right_keys.extend_from_slice(right_values);
    right_keys.sort_unstable();
    right_keys.dedup();

    let selected = if input_rows >= placement.row_threshold {
        let split = left_values.len() / 2;
        match execution_context.try_join(
            PARALLEL_MEMBERSHIP_WORKERS,
            || {
                select_membership_range(
                    &left_values[..split],
                    &right_keys,
                    0,
                    placement.keep_matches,
                )
            },
            || {
                select_membership_range(
                    &left_values[split..],
                    &right_keys,
                    split,
                    placement.keep_matches,
                )
            },
        ) {
            Ok((mut lower, upper)) => {
                record_parallel_membership_placement(ParallelMembershipPlacement::Parallel);
                lower.extend(upper);
                lower
            }
            Err(ParallelUnavailable::NoExecutor) => {
                record_parallel_membership_placement(ParallelMembershipPlacement::NoExecutor);
                select_membership_range(left_values, &right_keys, 0, placement.keep_matches)
            }
            Err(ParallelUnavailable::Capacity) => {
                record_parallel_membership_placement(ParallelMembershipPlacement::Capacity);
                select_membership_range(left_values, &right_keys, 0, placement.keep_matches)
            }
        }
    } else {
        record_parallel_membership_placement(ParallelMembershipPlacement::BelowThreshold);
        select_membership_range(left_values, &right_keys, 0, placement.keep_matches)
    };
    right_keys.clear();
    workspace.columns.push(right_keys);
    Some(selected)
}

fn selected_rows_are_valid(rows: &[usize], input_rows: usize) -> bool {
    rows.last().is_none_or(|row| *row < input_rows)
        && rows.windows(2).all(|window| window[0] < window[1])
}

fn select_membership_range(
    left: &[Value],
    right: &[Value],
    row_offset: usize,
    keep_matches: bool,
) -> Vec<usize> {
    left.iter()
        .enumerate()
        .filter_map(|(row, value)| {
            (right.binary_search(value).is_ok() == keep_matches).then_some(row_offset + row)
        })
        .collect()
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
    let preserve_rows = left.rows.is_some() && right.rows.is_some();
    let mut output = NativeBatch::empty(left.arity(), left.row_count + right.row_count, workspace);
    if preserve_rows {
        output.rows = Some(NativeRows::Owned(
            workspace.tuples(left.row_count + right.row_count),
        ));
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
    use crate::{ExecutionAdmission, RelationAccelerator};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct TestAdmission {
        available: Mutex<usize>,
        attempts: Mutex<usize>,
    }

    struct TestAccelerator {
        membership_calls: AtomicUsize,
        selected: Vec<usize>,
        join_calls: AtomicUsize,
        join_matches: Vec<EqualityJoinMatch>,
    }

    impl RelationAccelerator for TestAccelerator {
        fn select_membership(
            &self,
            _selection: MembershipSelection<'_>,
        ) -> AccelerationOutcome<Vec<usize>> {
            self.membership_calls.fetch_add(1, AtomicOrdering::Relaxed);
            AccelerationOutcome::Completed(self.selected.clone())
        }

        fn join_equality(
            &self,
            _join: EqualityJoin<'_>,
        ) -> AccelerationOutcome<Vec<EqualityJoinMatch>> {
            self.join_calls.fetch_add(1, AtomicOrdering::Relaxed);
            AccelerationOutcome::Completed(self.join_matches.clone())
        }
    }

    impl ExecutionAdmission for TestAdmission {
        fn capacity(&self) -> NonZeroUsize {
            NonZeroUsize::new(3).unwrap()
        }

        fn try_reserve_parallel(&self, additional_workers: NonZeroUsize) -> bool {
            *self.attempts.lock().unwrap() += 1;
            let mut available = self.available.lock().unwrap();
            let Some(remaining) = available.checked_sub(additional_workers.get()) else {
                return false;
            };
            *available = remaining;
            true
        }

        fn release_parallel(&self, additional_workers: NonZeroUsize) {
            *self.available.lock().unwrap() += additional_workers.get();
        }
    }

    fn parallel_context(available: usize) -> (ExecutionContext, Arc<TestAdmission>) {
        let admission = Arc::new(TestAdmission {
            available: Mutex::new(available),
            attempts: Mutex::new(0),
        });
        let context = ExecutionContext::parallel(admission.clone());
        (context, admission)
    }

    fn int(value: i64) -> Value {
        Value::int(value).unwrap()
    }

    fn unary_rows(values: impl IntoIterator<Item = i64>) -> Vec<Tuple> {
        values
            .into_iter()
            .map(|value| Tuple::from([int(value)]))
            .collect()
    }

    #[test]
    fn parallel_cached_union_matches_serial_and_releases_capacity() {
        let left = unary_rows(0..16_384);
        let right = unary_rows(8_192..24_576);
        let expected = merge_cached_union(&left, &right);
        let (context, admission) = parallel_context(2);

        let actual = place_cached_union_with_threshold(&left, &right, &context, 32_768);

        assert_eq!(actual, expected);
        assert_eq!(*admission.attempts.lock().unwrap(), 1);
        assert_eq!(*admission.available.lock().unwrap(), 2);
    }

    #[test]
    fn cached_union_falls_back_when_parallel_capacity_is_unavailable() {
        let left = unary_rows(0..16_384);
        let right = unary_rows(8_192..24_576);
        let expected = merge_cached_union(&left, &right);
        let (context, admission) = parallel_context(1);

        assert_eq!(
            place_cached_union_with_threshold(&left, &right, &context, 32_768),
            expected
        );
        assert_eq!(*admission.attempts.lock().unwrap(), 1);
    }

    #[test]
    fn cached_union_does_not_request_workers_for_unbalanced_partitions() {
        let left = unary_rows(0..32_767);
        let right = unary_rows([1_000_000]);
        let expected = merge_cached_union(&left, &right);
        let (context, admission) = parallel_context(2);

        assert_eq!(
            place_cached_union_with_threshold(&left, &right, &context, 32_768),
            expected
        );
        assert_eq!(*admission.attempts.lock().unwrap(), 0);
    }

    #[test]
    fn cached_union_does_not_request_workers_below_threshold() {
        let left = unary_rows(0..100);
        let right = unary_rows(50..150);
        let expected = merge_cached_union(&left, &right);
        let (context, admission) = parallel_context(2);

        assert_eq!(
            place_cached_union_with_threshold(&left, &right, &context, 1_000),
            expected
        );
        assert_eq!(*admission.attempts.lock().unwrap(), 0);
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
    fn packed_batch_shares_source_rows_until_materialization() {
        let packed = crate::PackedRelation::from_canonical_tuples(unary_rows(0..4), 1).unwrap();
        let shared_rows = packed.shared_rows();

        let batch = NativeBatch::from_packed(&packed);

        let Some(NativeRows::Shared(batch_rows)) = &batch.rows else {
            panic!("packed batch should retain shared rows");
        };
        assert!(Arc::ptr_eq(batch_rows, &shared_rows));
    }

    #[test]
    fn one_column_membership_uses_parallel_admission_and_preserves_row_order() {
        let left = NativeBatch::from_tuples(
            vec![
                Tuple::from([int(0), int(30)]),
                Tuple::from([int(1), int(10)]),
                Tuple::from([int(2), int(40)]),
                Tuple::from([int(3), int(10)]),
            ],
            2,
        )
        .unwrap();
        let right = NativeBatch::from_tuples(unary_rows([10, 20]), 1).unwrap();
        let (context, admission) = parallel_context(2);
        let mut workspace = BatchWorkspace::default();

        let selected = select_membership_rows_with_threshold(
            &left,
            &right,
            MembershipPlacement {
                left_position: 1,
                right_position: 0,
                keep_matches: true,
                accelerator_row_threshold: usize::MAX,
                row_threshold: 0,
            },
            &context,
            &mut workspace,
        )
        .unwrap();

        assert_eq!(selected, vec![1, 3]);
        assert_eq!(*admission.attempts.lock().unwrap(), 1);
        assert_eq!(*admission.available.lock().unwrap(), 2);
    }

    #[test]
    fn one_column_membership_supports_anti_join_selection() {
        let left = NativeBatch::from_tuples(
            vec![
                Tuple::from([int(0), int(30)]),
                Tuple::from([int(1), int(10)]),
                Tuple::from([int(2), int(40)]),
                Tuple::from([int(3), int(10)]),
            ],
            2,
        )
        .unwrap();
        let right = NativeBatch::from_tuples(unary_rows([10, 20]), 1).unwrap();
        let mut workspace = BatchWorkspace::default();

        let selected = select_membership_rows(
            &left,
            &right,
            1,
            0,
            false,
            &ExecutionContext::serial(),
            &mut workspace,
        )
        .unwrap();

        assert_eq!(selected, vec![0, 2]);
    }

    #[test]
    fn one_column_membership_uses_accelerator_for_shared_columns() {
        let left = crate::PackedRelation::from_canonical_tuples(
            vec![
                Tuple::from([int(0), int(30)]),
                Tuple::from([int(1), int(10)]),
                Tuple::from([int(2), int(40)]),
                Tuple::from([int(3), int(10)]),
            ],
            2,
        )
        .unwrap();
        let right = crate::PackedRelation::from_canonical_tuples(unary_rows([10, 20]), 1).unwrap();
        let left = NativeBatch::from_packed(&left);
        let right = NativeBatch::from_packed(&right);
        let accelerator = Arc::new(TestAccelerator {
            membership_calls: AtomicUsize::new(0),
            selected: vec![1, 3],
            join_calls: AtomicUsize::new(0),
            join_matches: Vec::new(),
        });
        let context = ExecutionContext::serial().with_accelerator(accelerator.clone());
        let mut workspace = BatchWorkspace::default();

        let selected = select_membership_rows_with_threshold(
            &left,
            &right,
            MembershipPlacement {
                left_position: 1,
                right_position: 0,
                keep_matches: true,
                accelerator_row_threshold: 0,
                row_threshold: usize::MAX,
            },
            &context,
            &mut workspace,
        )
        .unwrap();

        assert_eq!(selected, vec![1, 3]);
        assert_eq!(
            accelerator.membership_calls.load(AtomicOrdering::Relaxed),
            1
        );
    }

    #[test]
    fn one_column_membership_does_not_accelerate_owned_columns() {
        let left = NativeBatch::from_tuples(
            vec![
                Tuple::from([int(0), int(30)]),
                Tuple::from([int(1), int(10)]),
            ],
            2,
        )
        .unwrap();
        let right = NativeBatch::from_tuples(unary_rows([10, 20]), 1).unwrap();
        let accelerator = Arc::new(TestAccelerator {
            membership_calls: AtomicUsize::new(0),
            selected: vec![1],
            join_calls: AtomicUsize::new(0),
            join_matches: Vec::new(),
        });
        let context = ExecutionContext::serial().with_accelerator(accelerator.clone());
        let mut workspace = BatchWorkspace::default();

        let selected = select_membership_rows_with_threshold(
            &left,
            &right,
            MembershipPlacement {
                left_position: 1,
                right_position: 0,
                keep_matches: true,
                accelerator_row_threshold: 0,
                row_threshold: usize::MAX,
            },
            &context,
            &mut workspace,
        )
        .unwrap();

        assert_eq!(selected, vec![1]);
        assert_eq!(
            accelerator.membership_calls.load(AtomicOrdering::Relaxed),
            0
        );
    }

    #[test]
    fn equality_join_accelerator_materializes_duplicate_key_matches() {
        let left = crate::PackedRelation::from_canonical_tuples(
            vec![
                Tuple::from([int(1), int(10)]),
                Tuple::from([int(2), int(10)]),
                Tuple::from([int(3), int(20)]),
            ],
            2,
        )
        .unwrap();
        let right = crate::PackedRelation::from_canonical_tuples(
            vec![
                Tuple::from([int(10), int(100)]),
                Tuple::from([int(10), int(200)]),
                Tuple::from([int(20), int(300)]),
            ],
            2,
        )
        .unwrap();
        let left = NativeBatch::from_packed(&left);
        let right = NativeBatch::from_packed(&right);
        let accelerator = Arc::new(TestAccelerator {
            membership_calls: AtomicUsize::new(0),
            selected: Vec::new(),
            join_calls: AtomicUsize::new(0),
            join_matches: vec![
                EqualityJoinMatch {
                    left_row: 0,
                    right_row: 0,
                },
                EqualityJoinMatch {
                    left_row: 0,
                    right_row: 1,
                },
                EqualityJoinMatch {
                    left_row: 1,
                    right_row: 0,
                },
                EqualityJoinMatch {
                    left_row: 1,
                    right_row: 1,
                },
                EqualityJoinMatch {
                    left_row: 2,
                    right_row: 2,
                },
            ],
        });
        let context = ExecutionContext::serial().with_accelerator(accelerator.clone());
        let mut workspace = BatchWorkspace::default();

        let joined = place_accelerated_equality_join(
            &left,
            &right,
            &[1],
            &[0],
            &context,
            &mut workspace,
            EqualityJoinPlacement {
                row_threshold: 0,
                unbalanced_row_threshold: 0,
                min_side_rows: 0,
            },
        )
        .unwrap();

        assert_eq!(
            joined.into_tuples(&mut workspace),
            vec![
                Tuple::from([int(1), int(10), int(10), int(100)]),
                Tuple::from([int(1), int(10), int(10), int(200)]),
                Tuple::from([int(2), int(10), int(10), int(100)]),
                Tuple::from([int(2), int(10), int(10), int(200)]),
                Tuple::from([int(3), int(20), int(20), int(300)]),
            ]
        );
        assert_eq!(accelerator.join_calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn differential_join_preserves_duplicate_contributions_before_consolidation() {
        let delta = crate::PackedRelation::from_canonical_tuples(
            vec![Tuple::from([int(1)]), Tuple::from([int(1)])],
            1,
        )
        .unwrap();
        let full = crate::PackedRelation::from_canonical_tuples(
            vec![Tuple::from([int(1)]), Tuple::from([int(1)])],
            1,
        )
        .unwrap();
        let expected = execute_native_equality_join_matches(&delta, &full, &[0], &[0]).unwrap();
        assert_eq!(expected.len(), 4);

        let accelerator = Arc::new(TestAccelerator {
            membership_calls: AtomicUsize::new(0),
            selected: Vec::new(),
            join_calls: AtomicUsize::new(0),
            join_matches: expected.clone(),
        });
        let context = ExecutionContext::serial().with_accelerator(accelerator.clone());
        let accelerated = execute_differential_equality_join_with_thresholds(
            &delta,
            &full,
            &[0],
            &[0],
            &context,
            0,
            0,
        )
        .unwrap();
        assert_eq!(accelerated, expected);
        assert_eq!(accelerator.join_calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn differential_join_requires_a_large_changing_side() {
        assert!(!differential_equality_join_eligible(1, 2_097_152));
        assert!(!differential_equality_join_eligible(4_095, 258_049));
        assert!(differential_equality_join_eligible(4_096, 258_048));
    }

    #[test]
    fn equality_join_placement_requires_scale_for_unbalanced_inputs() {
        let placement = EqualityJoinPlacement {
            row_threshold: 262_144,
            unbalanced_row_threshold: 2_097_152,
            min_side_rows: 4_096,
        };

        assert!(!equality_join_acceleration_eligible(131_072, 1, placement));
        assert!(equality_join_acceleration_eligible(
            131_072, 131_072, placement
        ));
        assert!(!equality_join_acceleration_eligible(524_288, 1, placement));
        assert!(equality_join_acceleration_eligible(2_097_152, 1, placement));
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
        let joined = join(
            &left,
            &right,
            &[1],
            &[0],
            &ExecutionContext::serial(),
            &mut workspace,
        )
        .unwrap();
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
            let joined = join(
                &left,
                &right,
                &[1],
                &[0],
                &ExecutionContext::serial(),
                &mut workspace,
            )
            .unwrap();
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
