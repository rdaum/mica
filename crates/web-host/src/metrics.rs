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
    Counter, DeriveLabel, ExportMetrics, Gauge, Histogram, LabelEnum, LabeledCounter,
    LabeledHistogram, LabeledSampledTimer, SampledTimerGuard,
};
use serde_json::{Map, Value as JsonValue, json};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

const DEFAULT_SHARDS: usize = 64;
const TIMER_SAMPLE_STRIDE: u64 = 64;
const LATENCY_BUCKETS_US: &[u64] = &[
    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
];

static METRICS: LazyLock<WebHostMetrics> = LazyLock::new(|| WebHostMetrics::new(DEFAULT_SHARDS));
static ACTIVE_CONNECTIONS: AtomicI64 = AtomicI64::new(0);

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "kind"]
pub enum HttpRequestKind {
    Static,
    InProcess,
    SyncEvents,
    SyncInput,
    DecodeError,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "status"]
pub enum HttpStatusClass {
    Success2xx,
    Redirect3xx,
    ClientError4xx,
    ServerError5xx,
    Other,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "phase"]
pub enum SyncRenderPhase {
    Revision,
    Tree,
    DecodeTree,
    SnapshotPayload,
    Diff,
    DeltaPayload,
    StoreRendered,
    SendEnvelope,
    Refresh,
    DomEvent,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "kind"]
pub enum SyncEnvelopeKind {
    Snapshot,
    Delta,
    Ack,
    RecoverySnapshot,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_web_host"]
pub struct WebHostMetrics {
    #[help = "HTTP connections accepted"]
    pub connections_accepted: Counter,

    #[help = "Currently active HTTP connections"]
    pub active_connections: Gauge,

    #[help = "HTTP requests by kind"]
    pub requests: LabeledCounter<HttpRequestKind>,

    #[help = "HTTP responses by status class"]
    pub responses: LabeledCounter<HttpStatusClass>,

    #[help = "HTTP request handling duration in microseconds by kind"]
    pub request_duration_us: LabeledHistogram<HttpRequestKind>,

    #[help = "HTTP request handling duration by kind"]
    pub request_duration: LabeledSampledTimer<HttpRequestKind>,

    #[help = "HTTP request body bytes"]
    pub request_body_bytes: Counter,

    #[help = "HTTP response body bytes"]
    pub response_body_bytes: Counter,

    #[help = "HTTP connection read errors"]
    pub connection_read_errors: Counter,

    #[help = "HTTP response write errors"]
    pub response_write_errors: Counter,

    #[help = "Sync render phase duration by phase"]
    pub sync_phase_duration: LabeledSampledTimer<SyncRenderPhase>,

    #[help = "Sync envelopes by kind"]
    pub sync_envelopes: LabeledCounter<SyncEnvelopeKind>,

    #[help = "Sync payload bytes by envelope kind"]
    pub sync_payload_bytes: LabeledHistogram<SyncEnvelopeKind>,

    #[help = "DOM nodes in rendered sync trees"]
    pub sync_dom_nodes: Histogram,

    #[help = "DOM patches in sync deltas"]
    pub sync_patch_count: Histogram,
}

impl WebHostMetrics {
    pub fn new(shard_count: usize) -> Self {
        let sync_timer_stride = sync_timer_sample_stride();
        Self {
            connections_accepted: Counter::new(shard_count),
            active_connections: Gauge::new(),
            requests: LabeledCounter::new(shard_count),
            responses: LabeledCounter::new(shard_count),
            request_duration_us: LabeledHistogram::new(LATENCY_BUCKETS_US, shard_count),
            request_duration: LabeledSampledTimer::with_latency_buckets(
                shard_count,
                TIMER_SAMPLE_STRIDE,
            ),
            request_body_bytes: Counter::new(shard_count),
            response_body_bytes: Counter::new(shard_count),
            connection_read_errors: Counter::new(shard_count),
            response_write_errors: Counter::new(shard_count),
            sync_phase_duration: LabeledSampledTimer::with_latency_buckets(
                shard_count,
                sync_timer_stride,
            ),
            sync_envelopes: LabeledCounter::new(shard_count),
            sync_payload_bytes: LabeledHistogram::new(
                &[
                    512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
                    524_288, 1_048_576,
                ],
                shard_count,
            ),
            sync_dom_nodes: Histogram::new(
                &[25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000],
                shard_count,
            ),
            sync_patch_count: Histogram::new(
                &[0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000],
                shard_count,
            ),
        }
    }
}

fn sync_timer_sample_stride() -> u64 {
    static STRIDE: LazyLock<u64> = LazyLock::new(|| {
        std::env::var("MICA_SYNC_TIMER_SAMPLE_STRIDE")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(TIMER_SAMPLE_STRIDE)
    });
    *STRIDE
}

pub fn metrics() -> &'static WebHostMetrics {
    &METRICS
}

pub(crate) fn status_class(status: u16) -> HttpStatusClass {
    match status {
        200..=299 => HttpStatusClass::Success2xx,
        300..=399 => HttpStatusClass::Redirect3xx,
        400..=499 => HttpStatusClass::ClientError4xx,
        500..=599 => HttpStatusClass::ServerError5xx,
        _ => HttpStatusClass::Other,
    }
}

pub(crate) fn duration_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn start_sync_phase(phase: SyncRenderPhase) -> SampledTimerGuard<'static> {
    metrics().sync_phase_duration.start(phase)
}

pub(crate) fn record_sync_envelope(kind: SyncEnvelopeKind, payload_bytes: usize) {
    let metrics = metrics();
    metrics.sync_envelopes.inc(kind);
    metrics
        .sync_payload_bytes
        .record(kind, payload_bytes.min(u64::MAX as usize) as u64);
}

pub(crate) fn record_sync_dom_nodes(node_count: usize) {
    metrics()
        .sync_dom_nodes
        .record(node_count.min(u64::MAX as usize) as u64);
}

pub(crate) fn record_sync_patch_count(patch_count: usize) {
    metrics()
        .sync_patch_count
        .record(patch_count.min(u64::MAX as usize) as u64);
}

pub fn metrics_snapshot_json() -> JsonValue {
    let metrics = metrics();
    json!({
        "mica_web_host": {
            "http": {
                "requests": labeled_counter_snapshot(&metrics.requests),
                "responses": labeled_counter_snapshot(&metrics.responses),
                "request_duration": labeled_timer_snapshot(&metrics.request_duration),
                "request_body_bytes": metrics.request_body_bytes.sum(),
                "response_body_bytes": metrics.response_body_bytes.sum(),
                "active_connections": metrics.active_connections.get(),
            },
            "sync": {
                "phase_duration": labeled_timer_snapshot(&metrics.sync_phase_duration),
                "envelopes": labeled_counter_snapshot(&metrics.sync_envelopes),
                "payload_bytes": labeled_histogram_snapshot(&metrics.sync_payload_bytes),
                "dom_nodes": histogram_snapshot(&metrics.sync_dom_nodes),
                "patch_count": histogram_snapshot(&metrics.sync_patch_count),
            },
        },
        "mica_driver": {
            "dispatch_jobs": labeled_counter_snapshot(&mica_driver::metrics::metrics().dispatch_jobs),
            "dispatch_duration": labeled_timer_snapshot(&mica_driver::metrics::metrics().dispatch_duration),
            "active_async_workers_total": mica_driver::metrics::metrics().active_async_workers_total.get(),
            "buffered_events": mica_driver::metrics::metrics().buffered_events.get(),
        },
        "mica_runtime": {
            "task_operations": labeled_counter_snapshot(&mica_runtime::metrics::metrics().task_operations),
            "task_run_duration": labeled_timer_snapshot(&mica_runtime::metrics::metrics().task_run_duration),
            "task_outcomes": labeled_counter_snapshot(&mica_runtime::metrics::metrics().task_outcomes),
            "active_endpoints": mica_runtime::metrics::metrics().active_endpoints.get(),
            "suspended_tasks": mica_runtime::metrics::metrics().suspended_tasks.get(),
            "transient_tuples": mica_runtime::metrics::metrics().transient_tuples.get(),
        },
    })
}

fn labeled_counter_snapshot<L: LabelEnum>(counter: &LabeledCounter<L>) -> JsonValue {
    let mut out = Map::new();
    for index in 0..L::CARDINALITY {
        let label = L::from_index(index);
        out.insert(label.variant_name().to_owned(), json!(counter.get(label)));
    }
    JsonValue::Object(out)
}

fn labeled_timer_snapshot<L: LabelEnum>(timer: &LabeledSampledTimer<L>) -> JsonValue {
    let mut out = Map::new();
    for index in 0..L::CARDINALITY {
        let label = L::from_index(index);
        out.insert(
            label.variant_name().to_owned(),
            json!({
                "calls": timer.calls(label),
                "samples": timer.sample_count(label),
                "sample_sum_nanos": timer.sample_sum_nanos(label),
                "avg_sample_nanos": timer.avg_sample_nanos(label),
            }),
        );
    }
    JsonValue::Object(out)
}

fn labeled_histogram_snapshot<L: LabelEnum>(histogram: &LabeledHistogram<L>) -> JsonValue {
    let mut out = Map::new();
    for (label, histogram) in histogram.iter() {
        out.insert(
            label.variant_name().to_owned(),
            histogram_snapshot(histogram),
        );
    }
    JsonValue::Object(out)
}

fn histogram_snapshot(histogram: &Histogram) -> JsonValue {
    let buckets = histogram
        .buckets_cumulative_iter()
        .map(|(upper_bound, count)| {
            json!({
                "le": if upper_bound == u64::MAX { JsonValue::String("+Inf".to_owned()) } else { json!(upper_bound) },
                "count": count,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "count": histogram.count(),
        "sum": histogram.sum(),
        "buckets": buckets,
    })
}

pub(crate) fn connection_started() {
    let active = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics().connections_accepted.inc();
    metrics().active_connections.set(active);
}

pub(crate) fn connection_ended() {
    let active = ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed) - 1;
    metrics().active_connections.set(active.max(0));
}
