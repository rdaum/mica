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

use fast_telemetry::{Counter, DeriveLabel, ExportMetrics, Histogram, LabeledCounter};
use mica_relation_kernel::RelationId;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

const DEFAULT_SHARDS: usize = 64;
const ROW_BUCKETS: &[u64] = &[
    0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 50_000,
];

static METRICS: LazyLock<VmMetrics> = LazyLock::new(|| VmMetrics::new(DEFAULT_SHARDS));
static RELATION_OPERATION_STATS: LazyLock<
    Mutex<BTreeMap<RelationOperationKey, RelationOperationStats>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Copy, Clone, Debug, DeriveLabel, Eq, PartialEq, Ord, PartialOrd)]
#[label_name = "operation"]
pub enum RelationOperation {
    Scan,
    Visit,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "shape"]
pub enum RelationOperationShape {
    ScanUnbound,
    ScanPrefix1,
    ScanPrefix2,
    ScanPrefix3Plus,
    ScanFull,
    ScanMixed,
    VisitUnbound,
    VisitPrefix1,
    VisitPrefix2,
    VisitPrefix3Plus,
    VisitFull,
    VisitMixed,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_vm"]
pub struct VmMetrics {
    #[help = "VM host relation operations"]
    pub relation_operations: LabeledCounter<RelationOperation>,

    #[help = "VM host relation operations by operation and bound argument shape"]
    pub relation_operation_shapes: LabeledCounter<RelationOperationShape>,

    #[help = "Rows returned by VM host relation operations"]
    pub relation_operation_rows: Histogram,

    #[help = "Total VM host relation operation time in microseconds"]
    pub relation_operation_elapsed_us: Counter,
}

impl VmMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            relation_operations: LabeledCounter::new(shard_count),
            relation_operation_shapes: LabeledCounter::new(shard_count),
            relation_operation_rows: Histogram::new(ROW_BUCKETS, shard_count),
            relation_operation_elapsed_us: Counter::new(shard_count),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RelationOperationSummary {
    pub relation_id: u64,
    pub relation_name: String,
    pub operation: &'static str,
    pub bound_mask: String,
    pub calls: u64,
    pub rows: u64,
    pub elapsed_us: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RelationOperationKey {
    relation_id: u64,
    relation_name: String,
    operation: RelationOperation,
    bound_mask: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct RelationOperationStats {
    calls: u64,
    rows: u64,
    elapsed_us: u64,
}

pub fn metrics() -> &'static VmMetrics {
    &METRICS
}

pub fn record_relation_operation(
    operation: RelationOperation,
    relation: RelationId,
    relation_name: Option<&str>,
    bindings: &[Option<mica_var::Value>],
    rows: usize,
    elapsed: Duration,
) {
    let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    let row_count = rows.min(u64::MAX as usize) as u64;
    let metrics = metrics();
    metrics.relation_operations.inc(operation);
    metrics
        .relation_operation_shapes
        .inc(operation_shape(operation, bindings));
    metrics.relation_operation_rows.record(row_count);
    metrics
        .relation_operation_elapsed_us
        .add(elapsed_us.min(isize::MAX as u64) as isize);

    let key = RelationOperationKey {
        relation_id: relation.raw(),
        relation_name: relation_name
            .filter(|name| !name.is_empty())
            .unwrap_or("<unknown>")
            .to_owned(),
        operation,
        bound_mask: bound_mask(bindings),
    };
    let mut stats = RELATION_OPERATION_STATS.lock().unwrap();
    let entry = stats.entry(key).or_default();
    entry.calls = entry.calls.saturating_add(1);
    entry.rows = entry.rows.saturating_add(row_count);
    entry.elapsed_us = entry.elapsed_us.saturating_add(elapsed_us);
}

pub fn relation_operation_summaries(limit: usize) -> Vec<RelationOperationSummary> {
    let stats = RELATION_OPERATION_STATS.lock().unwrap();
    let mut summaries = stats
        .iter()
        .map(|(key, stats)| RelationOperationSummary {
            relation_id: key.relation_id,
            relation_name: key.relation_name.clone(),
            operation: match key.operation {
                RelationOperation::Scan => "scan",
                RelationOperation::Visit => "visit",
            },
            bound_mask: key.bound_mask.clone(),
            calls: stats.calls,
            rows: stats.rows,
            elapsed_us: stats.elapsed_us,
        })
        .collect::<Vec<_>>();
    summaries.sort_by_key(|summary| {
        (
            Reverse(summary.elapsed_us),
            Reverse(summary.rows),
            Reverse(summary.calls),
        )
    });
    summaries.truncate(limit);
    summaries
}

fn operation_shape(
    operation: RelationOperation,
    bindings: &[Option<mica_var::Value>],
) -> RelationOperationShape {
    let bound = bindings.iter().filter(|binding| binding.is_some()).count();
    let prefix = bindings
        .iter()
        .take_while(|binding| binding.is_some())
        .count();
    let is_full = bound == bindings.len();
    let is_prefix = bound == prefix;
    match (operation, bound, is_full, is_prefix) {
        (RelationOperation::Scan, 0, _, _) => RelationOperationShape::ScanUnbound,
        (RelationOperation::Scan, _, true, _) => RelationOperationShape::ScanFull,
        (RelationOperation::Scan, 1, _, true) => RelationOperationShape::ScanPrefix1,
        (RelationOperation::Scan, 2, _, true) => RelationOperationShape::ScanPrefix2,
        (RelationOperation::Scan, _, _, true) => RelationOperationShape::ScanPrefix3Plus,
        (RelationOperation::Scan, _, _, false) => RelationOperationShape::ScanMixed,
        (RelationOperation::Visit, 0, _, _) => RelationOperationShape::VisitUnbound,
        (RelationOperation::Visit, _, true, _) => RelationOperationShape::VisitFull,
        (RelationOperation::Visit, 1, _, true) => RelationOperationShape::VisitPrefix1,
        (RelationOperation::Visit, 2, _, true) => RelationOperationShape::VisitPrefix2,
        (RelationOperation::Visit, _, _, true) => RelationOperationShape::VisitPrefix3Plus,
        (RelationOperation::Visit, _, _, false) => RelationOperationShape::VisitMixed,
    }
}

fn bound_mask(bindings: &[Option<mica_var::Value>]) -> String {
    bindings
        .iter()
        .map(|binding| if binding.is_some() { '1' } else { '0' })
        .collect()
}
