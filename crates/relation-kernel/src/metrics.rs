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

use crate::Snapshot;
use fast_telemetry::{
    Counter, DeriveLabel, ExportMetrics, Gauge, Histogram, LabeledCounter, LabeledHistogram,
    SampledTimer,
};
use std::sync::LazyLock;

const DEFAULT_SHARDS: usize = 64;
const TIMER_SAMPLE_STRIDE: u64 = 64;
const COUNT_BUCKETS: &[u64] = &[
    0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 50_000,
];

static METRICS: LazyLock<RelationKernelMetrics> =
    LazyLock::new(|| RelationKernelMetrics::new(DEFAULT_SHARDS));

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
