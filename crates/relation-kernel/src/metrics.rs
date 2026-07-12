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

use crate::RelationId;
use crate::Snapshot;
use fast_telemetry::{
    Counter, DeriveLabel, ExportMetrics, Gauge, Histogram, LabeledCounter, LabeledHistogram,
    SampledTimer,
};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

const DEFAULT_SHARDS: usize = 64;
const TIMER_SAMPLE_STRIDE: u64 = 64;
const COUNT_BUCKETS: &[u64] = &[
    0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 50_000,
];

static METRICS: LazyLock<RelationKernelMetrics> =
    LazyLock::new(|| RelationKernelMetrics::new(DEFAULT_SHARDS));
static DERIVED_RELATION_STATS: LazyLock<Mutex<BTreeMap<RelationId, DerivedRelationStats>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "operation"]
pub enum TransactionWriteOperation {
    Assert,
    Retract,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "operation"]
pub enum TransactionReadOperation {
    Scan,
    EstimateScan,
    Visit,
    ScanExtensional,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "outcome"]
pub enum CommitOutcome {
    Committed,
    Conflict,
    PersistenceError,
    Error,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "operation"]
pub enum CatalogOperation {
    RelationCreated,
    RuleInstalled,
    RuleDisabled,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_relation_kernel"]
pub struct RelationKernelMetrics {
    #[help = "Transactions opened"]
    pub transactions_started: Counter,

    #[help = "Transaction local write operations by operation"]
    pub transaction_write_operations: LabeledCounter<TransactionWriteOperation>,

    #[help = "Functional replacement operations"]
    pub transaction_functional_replacements: Counter,

    #[help = "Transaction read operations by operation"]
    pub transaction_read_operations: LabeledCounter<TransactionReadOperation>,

    #[help = "Transaction commits by outcome"]
    pub transaction_commits: LabeledCounter<CommitOutcome>,

    #[help = "Catalog mutations by operation"]
    pub catalog_operations: LabeledCounter<CatalogOperation>,

    #[help = "Transaction commit duration in microseconds"]
    pub transaction_commit_duration_us: Histogram,

    #[help = "Transaction commit duration"]
    pub transaction_commit_duration: SampledTimer,

    #[help = "Tuples changed per committed transaction"]
    pub transaction_commit_changes: Histogram,

    #[help = "Rows returned by relation reads"]
    pub transaction_read_rows: LabeledHistogram<TransactionReadOperation>,

    #[help = "Derived relation materialization passes"]
    pub derived_materializations: Counter,

    #[help = "Derived relation materialization duration in microseconds"]
    pub derived_materialization_duration_us: Histogram,

    #[help = "Rows materialized by derived relation materialization"]
    pub derived_materialization_rows: Histogram,

    #[help = "Successful rule fixpoint evaluations"]
    pub rule_fixpoint_evaluations: Counter,

    #[help = "Recursive rounds per rule fixpoint evaluation"]
    pub rule_fixpoint_rounds: Histogram,

    #[help = "Non-recursive rule evaluations per rule fixpoint evaluation"]
    pub rule_evaluations: Histogram,

    #[help = "Recursive rule variant evaluations per rule fixpoint evaluation"]
    pub rule_variant_evaluations: Histogram,

    #[help = "Candidate tuples produced per rule fixpoint evaluation"]
    pub rule_candidate_rows: Histogram,

    #[help = "Novel tuples accepted per rule fixpoint evaluation"]
    pub rule_novel_rows: Histogram,

    #[help = "Rows in each recursive frontier"]
    pub rule_frontier_rows: Histogram,

    #[help = "Relations in the current snapshot"]
    pub snapshot_relations: Gauge,

    #[help = "Rules in the current snapshot"]
    pub snapshot_rules: Gauge,

    #[help = "Current snapshot version"]
    pub snapshot_version: Gauge,

    #[help = "Commits retained in the current snapshot history"]
    pub snapshot_commits: Gauge,
}

impl RelationKernelMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            transactions_started: Counter::new(shard_count),
            transaction_write_operations: LabeledCounter::new(shard_count),
            transaction_functional_replacements: Counter::new(shard_count),
            transaction_read_operations: LabeledCounter::new(shard_count),
            transaction_commits: LabeledCounter::new(shard_count),
            catalog_operations: LabeledCounter::new(shard_count),
            transaction_commit_duration_us: Histogram::with_latency_buckets(shard_count),
            transaction_commit_duration: SampledTimer::with_latency_buckets(
                shard_count,
                TIMER_SAMPLE_STRIDE,
            ),
            transaction_commit_changes: Histogram::new(COUNT_BUCKETS, shard_count),
            transaction_read_rows: LabeledHistogram::new(COUNT_BUCKETS, shard_count),
            derived_materializations: Counter::new(shard_count),
            derived_materialization_duration_us: Histogram::with_latency_buckets(shard_count),
            derived_materialization_rows: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_fixpoint_evaluations: Counter::new(shard_count),
            rule_fixpoint_rounds: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_evaluations: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_variant_evaluations: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_candidate_rows: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_novel_rows: Histogram::new(COUNT_BUCKETS, shard_count),
            rule_frontier_rows: Histogram::new(COUNT_BUCKETS, shard_count),
            snapshot_relations: Gauge::new(),
            snapshot_rules: Gauge::new(),
            snapshot_version: Gauge::new(),
            snapshot_commits: Gauge::new(),
        }
    }

    pub(crate) fn record_snapshot(&self, snapshot: &Snapshot) {
        self.snapshot_relations.set(snapshot.relations.len() as i64);
        self.snapshot_rules.set(snapshot.rules.len() as i64);
        self.snapshot_version.set(snapshot.version() as i64);
        self.snapshot_commits.set(snapshot.commits.len() as i64);
    }
}

pub fn metrics() -> &'static RelationKernelMetrics {
    &METRICS
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DerivedRelationSummary {
    pub relation_id: u64,
    pub relation_name: String,
    pub materializations: u64,
    pub rows: u64,
}

#[derive(Clone, Debug, Default)]
struct DerivedRelationStats {
    name: String,
    materializations: u64,
    rows: u64,
}

pub(crate) fn record_derived_materialization(
    elapsed: Duration,
    rows_by_relation: impl IntoIterator<Item = (RelationId, String, usize)>,
) {
    let metrics = metrics();
    metrics.derived_materializations.inc();
    metrics
        .derived_materialization_duration_us
        .record(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);

    let mut total_rows = 0usize;
    let mut stats = DERIVED_RELATION_STATS.lock().unwrap();
    for (relation, name, rows) in rows_by_relation {
        total_rows = total_rows.saturating_add(rows);
        let entry = stats.entry(relation).or_default();
        if entry.name.is_empty() {
            entry.name = name;
        }
        entry.materializations = entry.materializations.saturating_add(1);
        entry.rows = entry
            .rows
            .saturating_add(rows.min(u64::MAX as usize) as u64);
    }
    metrics
        .derived_materialization_rows
        .record(total_rows.min(u64::MAX as usize) as u64);
}

pub(crate) fn record_rule_fixpoint(
    rounds: usize,
    rule_evaluations: usize,
    variant_evaluations: usize,
    candidate_rows: usize,
    novel_rows: usize,
    frontier_rows: &[usize],
) {
    let metrics = metrics();
    metrics.rule_fixpoint_evaluations.inc();
    metrics.rule_fixpoint_rounds.record(rounds as u64);
    metrics.rule_evaluations.record(rule_evaluations as u64);
    metrics
        .rule_variant_evaluations
        .record(variant_evaluations as u64);
    metrics.rule_candidate_rows.record(candidate_rows as u64);
    metrics.rule_novel_rows.record(novel_rows as u64);
    for rows in frontier_rows {
        metrics.rule_frontier_rows.record(*rows as u64);
    }
}

pub fn derived_relation_summaries(limit: usize) -> Vec<DerivedRelationSummary> {
    let stats = DERIVED_RELATION_STATS.lock().unwrap();
    let mut summaries = stats
        .iter()
        .map(|(relation, stats)| DerivedRelationSummary {
            relation_id: relation.raw(),
            relation_name: if stats.name.is_empty() {
                format!("#{}", relation.raw())
            } else {
                stats.name.clone()
            },
            materializations: stats.materializations,
            rows: stats.rows,
        })
        .collect::<Vec<_>>();
    summaries.sort_by_key(|summary| (Reverse(summary.rows), Reverse(summary.materializations)));
    summaries.truncate(limit);
    summaries
}
