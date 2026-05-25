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
    LabeledSampledTimer,
};
use std::sync::LazyLock;
use std::time::Duration;

const DEFAULT_SHARDS: usize = 64;
const TIMER_SAMPLE_STRIDE: u64 = 64;
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

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "kind"]
pub enum ConnectionErrorKind {
    Handshake,
    Http3,
    Request,
    Session,
    DatagramRead,
    DatagramWrite,
    UniStreamRead,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_webtransport_host"]
pub struct WebTransportMetrics {
    #[help = "WebTransport connections accepted"]
    pub connections_accepted: Counter,

    #[help = "WebTransport sessions accepted"]
    pub sessions_accepted: Counter,

    #[help = "WebTransport connection errors by kind"]
    pub connection_errors: LabeledCounter<ConnectionErrorKind>,

    #[help = "Currently active WebTransport sessions"]
    pub active_sessions: Gauge,

    #[help = "Currently active WebTransport sync views"]
    pub active_sync_views: Gauge,

    #[help = "Incoming datagrams by kind"]
    pub incoming_datagrams: LabeledCounter<IncomingDatagramKind>,

    #[help = "Incoming bytes"]
    pub incoming_bytes: Counter,

    #[help = "Incoming unidirectional streams"]
    pub incoming_uni_streams: Counter,

    #[help = "Incoming unidirectional stream bytes"]
    pub incoming_uni_stream_bytes: Counter,

    #[help = "Outgoing datagrams"]
    pub outgoing_datagrams: Counter,

    #[help = "Outgoing bytes"]
    pub outgoing_bytes: Counter,

    #[help = "Datagrams produced from sync envelopes before send attempts"]
    pub sync_envelope_datagrams: Counter,

    #[help = "Chunked datagrams produced from oversized sync envelopes"]
    pub sync_envelope_chunks: Counter,

    #[help = "Sync envelopes by kind"]
    pub sync_envelopes: LabeledCounter<SyncEnvelopeKind>,

    #[help = "Recovery snapshots sent after stale or unknown client sync state"]
    pub recovery_snapshots: Counter,

    #[help = "Session output high-water events"]
    pub output_high_water_events: Counter,

    #[help = "Attempts to enqueue output after the session writer closed"]
    pub output_send_after_close: Counter,

    #[help = "Driver events routed to WebTransport sessions"]
    pub routed_driver_events: Counter,

    #[help = "Sync render duration in microseconds by operation"]
    pub sync_render_duration_us: LabeledHistogram<RenderOperation>,

    #[help = "Sync render duration by operation"]
    pub sync_render_duration: LabeledSampledTimer<RenderOperation>,

    #[help = "Queued outgoing datagrams waiting for a session writer"]
    pub queued_outgoing_datagrams: Gauge,
}

impl WebTransportMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            connections_accepted: Counter::new(shard_count),
            sessions_accepted: Counter::new(shard_count),
            connection_errors: LabeledCounter::new(shard_count),
            active_sessions: Gauge::new(),
            active_sync_views: Gauge::new(),
            incoming_datagrams: LabeledCounter::new(shard_count),
            incoming_bytes: Counter::new(shard_count),
            incoming_uni_streams: Counter::new(shard_count),
            incoming_uni_stream_bytes: Counter::new(shard_count),
            outgoing_datagrams: Counter::new(shard_count),
            outgoing_bytes: Counter::new(shard_count),
            sync_envelope_datagrams: Counter::new(shard_count),
            sync_envelope_chunks: Counter::new(shard_count),
            sync_envelopes: LabeledCounter::new(shard_count),
            recovery_snapshots: Counter::new(shard_count),
            output_high_water_events: Counter::new(shard_count),
            output_send_after_close: Counter::new(shard_count),
            routed_driver_events: Counter::new(shard_count),
            sync_render_duration_us: LabeledHistogram::new(LATENCY_BUCKETS_US, shard_count),
            sync_render_duration: LabeledSampledTimer::with_latency_buckets(
                shard_count,
                TIMER_SAMPLE_STRIDE,
            ),
            queued_outgoing_datagrams: Gauge::new(),
        }
    }
}

pub fn metrics() -> &'static WebTransportMetrics {
    &METRICS
}

pub(crate) fn duration_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u128::from(u64::MAX)) as u64
}
