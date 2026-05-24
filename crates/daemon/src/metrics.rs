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

use fast_telemetry::{Counter, DeriveLabel, ExportMetrics, Gauge, LabeledCounter};
use std::sync::LazyLock;

const DEFAULT_SHARDS: usize = 16;

static METRICS: LazyLock<DaemonMetrics> = LazyLock::new(|| DaemonMetrics::new(DEFAULT_SHARDS));

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "endpoint"]
pub enum DaemonEndpoint {
    Rpc,
    Telnet,
    Web,
    WebTransport,
}

#[derive(ExportMetrics)]
#[metric_prefix = "mica_daemon"]
pub struct DaemonMetrics {
    #[help = "DogStatsD exporters started"]
    pub dogstatsd_exporters_started: Counter,

    #[help = "Whether DogStatsD export is configured"]
    pub dogstatsd_configured: Gauge,

    #[help = "Daemon endpoints started by endpoint type"]
    pub endpoints_started: LabeledCounter<DaemonEndpoint>,
}

impl DaemonMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            dogstatsd_exporters_started: Counter::new(shard_count),
            dogstatsd_configured: Gauge::new(),
            endpoints_started: LabeledCounter::new(shard_count),
        }
    }
}

pub fn metrics() -> &'static DaemonMetrics {
    &METRICS
}
