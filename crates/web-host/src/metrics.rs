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
    Counter, DeriveLabel, ExportMetrics, Gauge, LabeledCounter, LabeledHistogram,
};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, Ordering};

const DEFAULT_SHARDS: usize = 64;
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

    #[help = "HTTP request body bytes"]
    pub request_body_bytes: Counter,

    #[help = "HTTP response body bytes"]
    pub response_body_bytes: Counter,
}

impl WebHostMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            connections_accepted: Counter::new(shard_count),
            active_connections: Gauge::new(),
            requests: LabeledCounter::new(shard_count),
            responses: LabeledCounter::new(shard_count),
            request_duration_us: LabeledHistogram::new(LATENCY_BUCKETS_US, shard_count),
            request_body_bytes: Counter::new(shard_count),
            response_body_bytes: Counter::new(shard_count),
        }
    }
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

pub(crate) fn elapsed_us(start: std::time::Instant) -> u64 {
    start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
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
