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

use fast_telemetry::{
    Counter, DeriveLabel, ExportMetrics, Gauge, Histogram, LabeledCounter, LabeledHistogram,
    LabeledSampledTimer, SampledTimer,
};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

const DEFAULT_SHARDS: usize = 16;
const TIMER_SAMPLE_STRIDE: u64 = 64;
const LATENCY_BUCKETS_US: &[u64] = &[
    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
    10_000_000,
];

static METRICS: LazyLock<DaemonMetrics> = LazyLock::new(|| DaemonMetrics::new(DEFAULT_SHARDS));
static ACTIVE_SOURCE_RETRIEVAL_INDEXING: AtomicI64 = AtomicI64::new(0);

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "endpoint"]
pub enum DaemonEndpoint {
    Rpc,
    Telnet,
    Web,
    WebTransport,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "service"]
pub enum ExternalService {
    Http,
    Openai,
    Embedding,
    Unknown,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_daemon"]
pub struct DaemonMetrics {
    #[help = "DogStatsD exporters started"]
    pub dogstatsd_exporters_started: Counter,

    #[help = "Whether DogStatsD export is configured"]
    pub dogstatsd_configured: Gauge,

    #[help = "DogStatsD export ticks completed"]
    pub dogstatsd_export_ticks: Counter,

    #[help = "Fileins loaded successfully at daemon startup"]
    pub fileins_loaded: Counter,

    #[help = "Daemon driver instances started"]
    pub drivers_started: Counter,

    #[help = "Daemon endpoints configured at startup"]
    pub endpoints_configured: Gauge,

    #[help = "Daemon endpoints started by endpoint type"]
    pub endpoints_started: LabeledCounter<DaemonEndpoint>,

    #[help = "External requests by service"]
    pub external_requests: LabeledCounter<ExternalService>,

    #[help = "External request errors by service"]
    pub external_request_errors: LabeledCounter<ExternalService>,

    #[help = "External request duration in microseconds by service"]
    pub external_request_duration_us: LabeledHistogram<ExternalService>,

    #[help = "External request duration by service"]
    pub external_request_duration: LabeledSampledTimer<ExternalService>,

    #[help = "Source retrieval indexing runs started"]
    pub source_retrieval_indexing_started: Counter,

    #[help = "Source retrieval indexing runs completed"]
    pub source_retrieval_indexing_completed: Counter,

    #[help = "Source retrieval indexing runs failed"]
    pub source_retrieval_indexing_errors: Counter,

    #[help = "Currently active source retrieval indexing runs"]
    pub source_retrieval_indexing_active: Gauge,

    #[help = "Subjects processed by source retrieval indexing"]
    pub source_retrieval_indexing_subjects: Counter,

    #[help = "Source retrieval indexing run duration in microseconds"]
    pub source_retrieval_indexing_duration_us: Histogram,

    #[help = "Source retrieval indexing run duration"]
    pub source_retrieval_indexing_duration: SampledTimer,
}

impl DaemonMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            dogstatsd_exporters_started: Counter::new(shard_count),
            dogstatsd_configured: Gauge::new(),
            dogstatsd_export_ticks: Counter::new(shard_count),
            fileins_loaded: Counter::new(shard_count),
            drivers_started: Counter::new(shard_count),
            endpoints_configured: Gauge::new(),
            endpoints_started: LabeledCounter::new(shard_count),
            external_requests: LabeledCounter::new(shard_count),
            external_request_errors: LabeledCounter::new(shard_count),
            external_request_duration_us: LabeledHistogram::new(LATENCY_BUCKETS_US, shard_count),
            external_request_duration: LabeledSampledTimer::with_latency_buckets(
                shard_count,
                TIMER_SAMPLE_STRIDE,
            ),
            source_retrieval_indexing_started: Counter::new(shard_count),
            source_retrieval_indexing_completed: Counter::new(shard_count),
            source_retrieval_indexing_errors: Counter::new(shard_count),
            source_retrieval_indexing_active: Gauge::new(),
            source_retrieval_indexing_subjects: Counter::new(shard_count),
            source_retrieval_indexing_duration_us: Histogram::with_latency_buckets(shard_count),
            source_retrieval_indexing_duration: SampledTimer::with_latency_buckets(
                shard_count,
                TIMER_SAMPLE_STRIDE,
            ),
        }
    }
}

pub fn metrics() -> &'static DaemonMetrics {
    &METRICS
}

pub(crate) fn duration_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn source_retrieval_indexing_started() {
    let active = ACTIVE_SOURCE_RETRIEVAL_INDEXING.fetch_add(1, Ordering::Relaxed) + 1;
    let metrics = metrics();
    metrics.source_retrieval_indexing_started.inc();
    metrics.source_retrieval_indexing_active.set(active);
}

pub(crate) fn source_retrieval_indexing_completed(elapsed: Duration, subjects: Option<i64>) {
    record_source_retrieval_indexing_duration(elapsed);
    if let Some(subjects) = subjects
        && subjects > 0
    {
        metrics()
            .source_retrieval_indexing_subjects
            .add(subjects as isize);
    }
    metrics().source_retrieval_indexing_completed.inc();
    source_retrieval_indexing_finished();
}

pub(crate) fn source_retrieval_indexing_failed(elapsed: Duration) {
    record_source_retrieval_indexing_duration(elapsed);
    metrics().source_retrieval_indexing_errors.inc();
    source_retrieval_indexing_finished();
}

fn record_source_retrieval_indexing_duration(elapsed: Duration) {
    let metrics = metrics();
    metrics
        .source_retrieval_indexing_duration_us
        .record(duration_us(elapsed));
    metrics
        .source_retrieval_indexing_duration
        .record_elapsed(elapsed);
}

fn source_retrieval_indexing_finished() {
    let active = ACTIVE_SOURCE_RETRIEVAL_INDEXING
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some((current - 1).max(0))
        })
        .unwrap_or(0)
        .saturating_sub(1)
        .max(0);
    metrics().source_retrieval_indexing_active.set(active);
}
