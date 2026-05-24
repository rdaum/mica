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

const DEFAULT_SHARDS: usize = 64;
const LATENCY_BUCKETS_US: &[u64] = &[
    10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
];

static METRICS: LazyLock<WebTransportMetrics> =
    LazyLock::new(|| WebTransportMetrics::new(DEFAULT_SHARDS));

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "kind"]
pub enum IncomingDatagramKind {
    SyncEnvelope,
    DomEvent,
    Plain,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "kind"]
pub enum SyncEnvelopeKind {
    NeedView,
    HaveView,
    ViewSnapshot,
    ViewDelta,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "operation"]
pub enum RenderOperation {
    Revision,
    View,
    Refresh,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_webtransport_host"]
pub struct WebTransportMetrics {
    #[help = "WebTransport connections accepted"]
    pub connections_accepted: Counter,

    #[help = "WebTransport sessions accepted"]
    pub sessions_accepted: Counter,

    #[help = "Currently active WebTransport sessions"]
    pub active_sessions: Gauge,

    #[help = "Incoming datagrams by kind"]
    pub incoming_datagrams: LabeledCounter<IncomingDatagramKind>,

    #[help = "Incoming bytes"]
    pub incoming_bytes: Counter,

    #[help = "Outgoing datagrams"]
    pub outgoing_datagrams: Counter,

    #[help = "Outgoing bytes"]
    pub outgoing_bytes: Counter,

    #[help = "Sync envelopes by kind"]
    pub sync_envelopes: LabeledCounter<SyncEnvelopeKind>,

    #[help = "Sync render duration in microseconds by operation"]
    pub sync_render_duration_us: LabeledHistogram<RenderOperation>,

    #[help = "Queued outgoing datagrams waiting for a session writer"]
    pub queued_outgoing_datagrams: Gauge,
}

impl WebTransportMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            connections_accepted: Counter::new(shard_count),
            sessions_accepted: Counter::new(shard_count),
            active_sessions: Gauge::new(),
            incoming_datagrams: LabeledCounter::new(shard_count),
            incoming_bytes: Counter::new(shard_count),
            outgoing_datagrams: Counter::new(shard_count),
            outgoing_bytes: Counter::new(shard_count),
            sync_envelopes: LabeledCounter::new(shard_count),
            sync_render_duration_us: LabeledHistogram::new(LATENCY_BUCKETS_US, shard_count),
            queued_outgoing_datagrams: Gauge::new(),
        }
    }
}

pub fn metrics() -> &'static WebTransportMetrics {
    &METRICS
}

pub(crate) fn elapsed_us(start: std::time::Instant) -> u64 {
    start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}
