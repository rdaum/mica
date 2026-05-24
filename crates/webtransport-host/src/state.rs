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

use crate::sync::{send_sync_envelope_to, start_event_pump, store_rendered_sync_view_in};
use crate::{DAEMON_ENDPOINT_ID_START, ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS};
use bytes::Bytes;
use mica_driver::CompioTaskDriver;
use mica_host_protocol::{DomNode, SyncEnvelope, SyncMessageKind};
use mica_var::Identity;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::future::Future;
use std::io::BufReader;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub struct SessionBinding {
    pub principal: Identity,
    pub actor: Option<Identity>,
}

pub struct WebTransportTlsConfig {
    pub(crate) cert_chain: Vec<CertificateDer<'static>>,
    pub(crate) key_der: PrivateKeyDer<'static>,
}

pub struct InProcessWebTransportHost {
    pub(crate) driver: Arc<CompioTaskDriver>,
    pub(crate) sessions: Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    pub(crate) stop_events: Arc<AtomicBool>,
    pub(crate) next_endpoint: AtomicU64,
}

#[derive(Default)]
pub(crate) struct SessionState {
    pub(crate) output: Arc<SessionOutput>,
    pub(crate) sync: Mutex<SessionSyncState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SessionSyncState {
    pub(crate) sessions: HashMap<u64, HashMap<u64, ActiveViewState>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ActiveViewState {
    pub(crate) client_revision: u64,
    pub(crate) client_signature: u64,
    pub(crate) server_revision: u64,
    pub(crate) server_signature: u64,
    pub(crate) last_tree: Option<DomNode>,
}

#[derive(Default)]
pub(crate) struct SessionOutput {
    pub(crate) state: Mutex<SessionOutputState>,
}

#[derive(Default)]
pub(crate) struct SessionOutputState {
    pub(crate) messages: VecDeque<SessionOutputMessage>,
    pub(crate) closed: bool,
    pub(crate) waker: Option<Waker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionOutputMessage {
    Datagram(Bytes),
}

pub(crate) struct SessionOutputRecv<'a> {
    pub(crate) output: &'a SessionOutput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveSyncView {
    pub(crate) endpoint: Identity,
    pub(crate) session_id: u64,
    pub(crate) view_id: u64,
    pub(crate) client_revision: u64,
    pub(crate) client_signature: u64,
    pub(crate) server_revision: u64,
    pub(crate) server_signature: u64,
    pub(crate) last_tree: Option<DomNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RenderedSyncView {
    pub(crate) revision: u64,
    pub(crate) signature: u64,
    pub(crate) tree: DomNode,
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionOutputReady {
    Ready { buffered: usize },
    HighWater { buffered: usize },
    Closed,
}

impl SessionState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            output: SessionOutput::new(),
            sync: Mutex::new(SessionSyncState::default()),
        })
    }
}

impl SessionSyncState {
    pub(crate) fn record_incoming_view(&mut self, envelope: &SyncEnvelope) {
        if !matches!(
            envelope.kind,
            SyncMessageKind::NeedView | SyncMessageKind::HaveView
        ) {
            return;
        }
        let view = self
            .sessions
            .entry(envelope.session_id)
            .or_default()
            .entry(envelope.view_id)
            .or_default();
        view.client_revision = envelope.client_revision;
        view.client_signature = envelope.client_signature;
    }

    pub(crate) fn store_rendered_view(
        &mut self,
        session_id: u64,
        view_id: u64,
        revision: u64,
        signature: u64,
        tree: DomNode,
    ) {
        let view = self
            .sessions
            .entry(session_id)
            .or_default()
            .entry(view_id)
            .or_default();
        view.client_revision = revision;
        view.client_signature = signature;
        view.server_revision = revision;
        view.server_signature = signature;
        view.last_tree = Some(tree);
    }

    #[cfg(test)]
    pub(crate) fn has_active_view(&self, session_id: u64, view_id: u64) -> bool {
        self.sessions
            .get(&session_id)
            .is_some_and(|views| views.contains_key(&view_id))
    }
}

impl WebTransportTlsConfig {
    pub fn from_pem_files(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, String> {
        let cert_path = cert_path.as_ref();
        let key_path = key_path.as_ref();
        let cert_file = File::open(cert_path).map_err(|error| {
            format!(
                "failed to open certificate {}: {error}",
                cert_path.display()
            )
        })?;
        let mut cert_reader = BufReader::new(cert_file);
        let cert_chain = rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                format!(
                    "failed to read certificate {}: {error}",
                    cert_path.display()
                )
            })?;
        if cert_chain.is_empty() {
            return Err(format!(
                "certificate file {} did not contain any certificates",
                cert_path.display()
            ));
        }

        let key_file = File::open(key_path).map_err(|error| {
            format!("failed to open private key {}: {error}", key_path.display())
        })?;
        let mut key_reader = BufReader::new(key_file);
        let key_der = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|error| format!("failed to read private key {}: {error}", key_path.display()))?
            .ok_or_else(|| {
                format!(
                    "private key file {} did not contain a supported key",
                    key_path.display()
                )
            })?;

        Ok(Self {
            cert_chain,
            key_der,
        })
    }
}

impl InProcessWebTransportHost {
    pub fn new(driver: CompioTaskDriver) -> Self {
        let driver = Arc::new(driver);
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let stop_events = Arc::new(AtomicBool::new(false));
        start_event_pump(driver.clone(), sessions.clone(), stop_events.clone());
        Self {
            driver,
            sessions,
            stop_events,
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_without_event_pump(driver: CompioTaskDriver) -> Self {
        Self {
            driver: Arc::new(driver),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            stop_events: Arc::new(AtomicBool::new(false)),
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
        }
    }

    pub(crate) fn allocate_endpoint(&self) -> Result<Identity, String> {
        let raw = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        Identity::new(raw).ok_or_else(|| "endpoint identity space is exhausted".to_owned())
    }

    #[cfg(test)]
    pub(crate) fn active_sync_views(&self) -> Vec<ActiveSyncView> {
        crate::sync::active_sync_views(&self.sessions)
    }

    pub(crate) fn store_rendered_sync_view(
        &self,
        endpoint: Identity,
        session_id: u64,
        view_id: u64,
        rendered: &RenderedSyncView,
    ) {
        store_rendered_sync_view_in(&self.sessions, endpoint, session_id, view_id, rendered);
    }

    pub(crate) fn send_sync_envelope(
        &self,
        endpoint: Identity,
        envelope: SyncEnvelope,
    ) -> Result<(), String> {
        send_sync_envelope_to(&self.sessions, endpoint, envelope)
    }

    pub(crate) fn active_rendered_sync_view(
        &self,
        endpoint: Identity,
        session_id: u64,
        view_id: u64,
    ) -> Option<ActiveViewState> {
        let state = self.sessions.lock().unwrap().get(&endpoint).cloned()?;
        state
            .sync
            .lock()
            .unwrap()
            .sessions
            .get(&session_id)?
            .get(&view_id)
            .cloned()
    }
}

impl Drop for InProcessWebTransportHost {
    fn drop(&mut self) {
        self.stop_events.store(true, Ordering::Relaxed);
    }
}

impl SessionOutput {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn send_datagram(&self, datagram: Bytes) -> Result<(), String> {
        self.send_message(SessionOutputMessage::Datagram(datagram))
    }

    pub(crate) fn send_message(&self, message: SessionOutputMessage) -> Result<(), String> {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                crate::metrics::metrics().output_send_after_close.inc();
                return Err("session writer is closed".to_owned());
            }
            state.messages.push_back(message);
            crate::metrics::metrics()
                .queued_outgoing_datagrams
                .set(state.messages.len() as i64);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub(crate) fn close(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn recv(&self) -> SessionOutputRecv<'_> {
        SessionOutputRecv { output: self }
    }

    pub(crate) fn drain_batch(&self, max_messages: usize) -> Vec<SessionOutputMessage> {
        let mut state = self.state.lock().unwrap();
        let count = max_messages.min(state.messages.len());
        let mut messages = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(message) = state.messages.pop_front() else {
                break;
            };
            messages.push(message);
        }
        crate::metrics::metrics()
            .queued_outgoing_datagrams
            .set(state.messages.len() as i64);
        messages
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&self) -> Option<Bytes> {
        match self.state.lock().unwrap().messages.pop_front()? {
            SessionOutputMessage::Datagram(datagram) => Some(datagram),
        }
    }
}

impl Future for SessionOutputRecv<'_> {
    type Output = SessionOutputReady;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.output.state.lock().unwrap();
        if state.messages.len() >= ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS {
            crate::metrics::metrics().output_high_water_events.inc();
            return Poll::Ready(SessionOutputReady::HighWater {
                buffered: state.messages.len(),
            });
        }
        if !state.messages.is_empty() {
            return Poll::Ready(SessionOutputReady::Ready {
                buffered: state.messages.len(),
            });
        }
        if state.closed {
            return Poll::Ready(SessionOutputReady::Closed);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub(crate) fn format_driver_error(
    driver: &CompioTaskDriver,
    error: mica_driver::DriverError,
) -> String {
    format!("error: {}", driver.format_error(&error))
}
