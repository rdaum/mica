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

use mica_driver::CompioTaskDriver;
use mica_var::Identity;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod codec;

mod request;
mod response;
mod server;

pub use server::{serve, serve_in_process};

pub const DEFAULT_BIND: &str = "127.0.0.1:8080";
pub const DAEMON_ENDPOINT_ID_START: u64 = 0x00ec_0000_0000_0000;
pub const DAEMON_REQUEST_ID_START: u64 = 0x00eb_0000_0000_0000;

#[derive(Clone, Debug)]
pub struct ActorBinding {
    pub name: String,
    pub identity: Identity,
}

pub struct InProcessWebHost {
    pub(crate) driver: Arc<CompioTaskDriver>,
    next_endpoint: AtomicU64,
    next_request: AtomicU64,
}

impl InProcessWebHost {
    pub fn new(driver: CompioTaskDriver) -> Self {
        Self {
            driver: Arc::new(driver),
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
            next_request: AtomicU64::new(DAEMON_REQUEST_ID_START),
        }
    }

    pub(crate) fn allocate_endpoint(&self) -> Result<Identity, String> {
        let raw = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        Identity::new(raw).ok_or_else(|| "endpoint identity space is exhausted".to_owned())
    }

    pub(crate) fn allocate_request(&self) -> Result<Identity, String> {
        let raw = self.next_request.fetch_add(1, Ordering::Relaxed);
        Identity::new(raw).ok_or_else(|| "request identity space is exhausted".to_owned())
    }
}

pub(crate) fn format_driver_error(error: mica_driver::DriverError) -> String {
    format!("error: {error}")
}
