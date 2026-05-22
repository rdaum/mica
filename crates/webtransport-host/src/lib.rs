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

use bytes::{Buf, Bytes};
use compio::runtime::ResumeUnwind;
use compio_quic::h3::quic::RecvStream as H3RecvStream;
use compio_quic::{Endpoint, ServerBuilder};
use h3_webtransport::server::WebTransportSession;
use mica_driver::{CompioTaskDriver, DriverEvent};
#[cfg(test)]
use mica_host_protocol::SUPPORTED_DOM_TAGS;
#[cfg(test)]
use mica_host_protocol::dom_event_payload_json;
use mica_host_protocol::{
    DomEventPayload, DomNode, SyncEnvelope, SyncMessageKind, decode_dom_event_payload,
    decode_sync_envelope, diff_dom_nodes, dom_patch_payload_json, encoded_sync_envelope,
    snapshot_payload_json, sync_envelope_from_value, sync_payload_signature, sync_u64_value,
};
use mica_runtime::TaskOutcome;
use mica_var::{Identity, Symbol, Value};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::future::{Future, poll_fn};
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

pub const DEFAULT_BIND: &str = "127.0.0.1:4433";
pub const DAEMON_ENDPOINT_ID_START: u64 = 0x00ea_0000_0000_0000;

const ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS: usize = 128;
const ENDPOINT_OUTPUT_DRAIN_DATAGRAMS: usize = 64;
const SYNC_DATAGRAM_MAX_LEN: usize = 1024;
const SYNC_CHUNK_HEADER_LEN: usize = 24;
const SYNC_CHUNK_PAYLOAD_LEN: usize = SYNC_DATAGRAM_MAX_LEN - SYNC_CHUNK_HEADER_LEN;
const SYNC_CHUNK_MAGIC: &[u8; 4] = b"MSC1";
const SYNC_ENVELOPE_SEND_ATTEMPTS: usize = 3;
static NEXT_SYNC_CHUNK_ID: AtomicU32 = AtomicU32::new(1);

type H3RequestStream =
    compio_quic::h3::server::RequestStream<compio_quic::h3::BidiStream<Bytes>, Bytes>;
type WtSession = WebTransportSession<compio_quic::Connection, Bytes>;

#[derive(Clone, Debug)]
pub struct SessionBinding {
    pub principal: Identity,
    pub actor: Option<Identity>,
}

pub struct WebTransportTlsConfig {
    cert_chain: Vec<CertificateDer<'static>>,
    key_der: PrivateKeyDer<'static>,
}

pub struct InProcessWebTransportHost {
    driver: Arc<CompioTaskDriver>,
    sessions: Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    stop_events: Arc<AtomicBool>,
    next_endpoint: AtomicU64,
}

#[derive(Default)]
struct SessionState {
    output: Arc<SessionOutput>,
    sync: Mutex<SessionSyncState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SessionSyncState {
    sessions: HashMap<u64, HashMap<u64, ActiveViewState>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ActiveViewState {
    client_revision: u64,
    client_signature: u64,
    server_revision: u64,
    server_signature: u64,
    last_tree: Option<DomNode>,
}

#[derive(Default)]
struct SessionOutput {
    state: Mutex<SessionOutputState>,
}

#[derive(Default)]
struct SessionOutputState {
    messages: VecDeque<SessionOutputMessage>,
    closed: bool,
    waker: Option<Waker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionOutputMessage {
    Datagram(Bytes),
}

struct SessionOutputRecv<'a> {
    output: &'a SessionOutput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveSyncView {
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
    client_revision: u64,
    client_signature: u64,
    server_revision: u64,
    server_signature: u64,
    last_tree: Option<DomNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RenderedSyncView {
    revision: u64,
    signature: u64,
    tree: DomNode,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionOutputReady {
    Ready { buffered: usize },
    HighWater { buffered: usize },
    Closed,
}

impl SessionState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            output: SessionOutput::new(),
            sync: Mutex::new(SessionSyncState::default()),
        })
    }
}

impl SessionSyncState {
    fn record_incoming_view(&mut self, envelope: &SyncEnvelope) {
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

    fn store_rendered_view(
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
    fn has_active_view(&self, session_id: u64, view_id: u64) -> bool {
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
    fn new_without_event_pump(driver: CompioTaskDriver) -> Self {
        Self {
            driver: Arc::new(driver),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            stop_events: Arc::new(AtomicBool::new(false)),
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
        }
    }

    fn allocate_endpoint(&self) -> Result<Identity, String> {
        let raw = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        Identity::new(raw).ok_or_else(|| "endpoint identity space is exhausted".to_owned())
    }

    #[cfg(test)]
    fn active_sync_views(&self) -> Vec<ActiveSyncView> {
        active_sync_views(&self.sessions)
    }

    fn store_rendered_sync_view(
        &self,
        endpoint: Identity,
        session_id: u64,
        view_id: u64,
        rendered: &RenderedSyncView,
    ) {
        store_rendered_sync_view_in(&self.sessions, endpoint, session_id, view_id, rendered);
    }

    fn send_sync_envelope(&self, endpoint: Identity, envelope: SyncEnvelope) -> Result<(), String> {
        send_sync_envelope_to(&self.sessions, endpoint, envelope)
    }

    fn active_rendered_sync_view(
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

pub async fn bind_server_endpoint(
    bind: SocketAddr,
    tls: WebTransportTlsConfig,
) -> Result<Endpoint, String> {
    ServerBuilder::new_with_single_cert(tls.cert_chain, tls.key_der)
        .map_err(|error| format!("failed to configure WebTransport TLS: {error}"))?
        .with_alpn_protocols(&["h3"])
        .bind(bind)
        .await
        .map_err(|error| format!("failed to bind WebTransport listener {bind}: {error}"))
}

pub async fn serve_in_process(
    endpoint: Endpoint,
    host: InProcessWebTransportHost,
    binding: SessionBinding,
    max_connections: Option<usize>,
) -> Result<(), String> {
    let host = Arc::new(host);
    let mut accepted = 0usize;
    while let Some(incoming) = endpoint.wait_incoming().await {
        let host = host.clone();
        let binding = binding.clone();
        compio::runtime::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    if let Err(error) = handle_quic_connection(connection, host, binding).await {
                        eprintln!("WebTransport connection failed: {error}");
                    }
                }
                Err(error) => eprintln!("WebTransport handshake failed: {error}"),
            }
        })
        .detach();
        accepted += 1;
        if max_connections.is_some_and(|max| accepted >= max) {
            break;
        }
    }
    Ok(())
}

impl SessionOutput {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn send_datagram(&self, datagram: Bytes) -> Result<(), String> {
        self.send_message(SessionOutputMessage::Datagram(datagram))
    }

    fn send_message(&self, message: SessionOutputMessage) -> Result<(), String> {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err("session writer is closed".to_owned());
            }
            state.messages.push_back(message);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn close(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn recv(&self) -> SessionOutputRecv<'_> {
        SessionOutputRecv { output: self }
    }

    fn drain_batch(&self, max_messages: usize) -> Vec<SessionOutputMessage> {
        let mut state = self.state.lock().unwrap();
        let count = max_messages.min(state.messages.len());
        let mut messages = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(message) = state.messages.pop_front() else {
                break;
            };
            messages.push(message);
        }
        messages
    }

    #[cfg(test)]
    fn try_recv(&self) -> Option<Bytes> {
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

async fn handle_quic_connection(
    connection: compio_quic::Connection,
    host: Arc<InProcessWebTransportHost>,
    binding: SessionBinding,
) -> Result<(), String> {
    let mut builder = compio_quic::h3::server::builder();
    builder
        .enable_extended_connect(true)
        .enable_datagram(true)
        .enable_webtransport(true)
        .max_webtransport_sessions(1);
    let mut connection = builder
        .build::<_, Bytes>(connection)
        .await
        .map_err(|error| format!("failed to start HTTP/3 connection: {error}"))?;

    loop {
        let Some(resolver) = connection
            .accept()
            .await
            .map_err(|error| format!("failed to accept HTTP/3 request: {error}"))?
        else {
            return Ok(());
        };
        let (request, stream) = resolver
            .resolve_request()
            .await
            .map_err(|error| format!("failed to resolve HTTP/3 request: {error}"))?;
        if is_webtransport_connect(&request) {
            let session = WebTransportSession::accept(request, stream, connection)
                .await
                .map_err(|error| format!("failed to accept WebTransport session: {error}"))?;
            return handle_session(Rc::new(session), host, binding).await;
        }
        reject_non_webtransport_request(stream).await?;
    }
}

fn is_webtransport_connect(request: &http::Request<()>) -> bool {
    let protocol = request.extensions().get::<compio_quic::h3::ext::Protocol>();
    matches!(
        (request.method(), protocol),
        (&http::Method::CONNECT, Some(protocol))
            if protocol == &compio_quic::h3::ext::Protocol::WEB_TRANSPORT
    )
}

async fn reject_non_webtransport_request(mut stream: H3RequestStream) -> Result<(), String> {
    let response = http::Response::builder()
        .status(http::StatusCode::NOT_FOUND)
        .body(())
        .map_err(|error| format!("failed to build HTTP/3 response: {error}"))?;
    stream
        .send_response(response)
        .await
        .map_err(|error| format!("failed to reject HTTP/3 request: {error}"))
}

async fn handle_session(
    session: Rc<WtSession>,
    host: Arc<InProcessWebTransportHost>,
    binding: SessionBinding,
) -> Result<(), String> {
    let endpoint = host.allocate_endpoint()?;
    let state = SessionState::new();
    let output = state.output.clone();
    host.sessions.lock().unwrap().insert(endpoint, state);
    if let Err(error) = host.driver.open_endpoint_with_context(
        endpoint,
        Some(binding.principal),
        binding.actor,
        Symbol::intern("webtransport"),
    ) {
        drop_session_writer(&host, endpoint);
        return Err(format_driver_error(&host.driver, error));
    }

    let writer = compio::runtime::spawn(write_datagram_loop(session.clone(), output));
    let stream_reader = compio::runtime::spawn(read_uni_stream_loop(
        session.clone(),
        host.clone(),
        endpoint,
    ));
    let result = read_datagram_loop(session, &host, endpoint).await;
    let _ = host.driver.close_endpoint(endpoint);
    drop_session_writer(&host, endpoint);
    let _ = writer.await.resume_unwind();
    let _ = stream_reader.await.resume_unwind();
    result
}

async fn read_datagram_loop(
    session: Rc<WtSession>,
    host: &InProcessWebTransportHost,
    endpoint: Identity,
) -> Result<(), String> {
    let mut reader = session.datagram_reader();
    loop {
        let datagram = match reader.read_datagram().await {
            Ok(datagram) => datagram,
            Err(error) => {
                let message = error.to_string();
                if message.contains("closed") {
                    return Ok(());
                }
                return Err(format!("failed to read WebTransport datagram: {message}"));
            }
        };
        route_incoming_datagram(host, endpoint, datagram.into_payload()).await?;
    }
}

async fn read_uni_stream_loop(
    session: Rc<WtSession>,
    host: Arc<InProcessWebTransportHost>,
    endpoint: Identity,
) -> Result<(), String> {
    loop {
        let stream = match session.accept_uni().await {
            Ok(Some((_session_id, stream))) => stream,
            Ok(None) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if message.contains("closed") {
                    return Ok(());
                }
                return Err(format!("failed to accept WebTransport stream: {message}"));
            }
        };
        let payload = read_uni_stream_payload(stream).await?;
        if let Err(error) = route_incoming_datagram(&host, endpoint, payload).await {
            eprintln!("failed to route WebTransport stream payload: {error}");
        }
    }
}

async fn read_uni_stream_payload<S>(mut stream: S) -> Result<Bytes, String>
where
    S: H3RecvStream<Buf = Bytes>,
{
    let mut payload = Vec::new();
    loop {
        let chunk = poll_fn(|cx| stream.poll_data(cx))
            .await
            .map_err(|error| format!("failed to read WebTransport stream: {error}"))?;
        let Some(mut chunk) = chunk else {
            return Ok(Bytes::from(payload));
        };
        while chunk.has_remaining() {
            let bytes = chunk.copy_to_bytes(chunk.remaining());
            payload.extend_from_slice(&bytes);
        }
    }
}

async fn route_incoming_datagram(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    datagram: Bytes,
) -> Result<(), String> {
    match decode_sync_envelope(&datagram) {
        Ok(envelope) => route_sync_envelope(host, endpoint, envelope).await,
        Err(_) => route_plain_datagram(host, endpoint, datagram).await,
    }
}

async fn route_plain_datagram(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    datagram: Bytes,
) -> Result<(), String> {
    if let Some(event) = decode_dom_event_payload(&datagram)? {
        return route_dom_event(host, endpoint, event).await;
    }

    host.driver
        .input(endpoint, Value::bytes(datagram))
        .await
        .map(|_| ())
        .map_err(|error| format_driver_error(&host.driver, error))
}

async fn route_dom_event(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    event: DomEventPayload,
) -> Result<(), String> {
    let trace = SyncTrace::new("dom_event");
    let Some(active) = host.active_rendered_sync_view(endpoint, event.session_id, event.view_id)
    else {
        return send_recovery_snapshot(host, endpoint, &event).await;
    };
    trace.mark("active_view");
    if active.server_revision != event.revision || active.server_signature != event.signature {
        let rendered = render_sync_view(host, endpoint, event.view_id).await?;
        trace.mark("stale_render");
        if event.revision > rendered.revision {
            return send_recovery_snapshot_from_rendered(host, endpoint, &event, rendered).await;
        }
        host.store_rendered_sync_view(endpoint, event.session_id, event.view_id, &rendered);
    }

    let submitted = host
        .driver
        .submit_invocation_for_endpoint(
            endpoint,
            Symbol::intern("sync_event"),
            vec![
                (Symbol::intern("session"), sync_u64_value(event.session_id)),
                (Symbol::intern("view"), sync_u64_value(event.view_id)),
                (Symbol::intern("event"), Value::string(event.event)),
                (Symbol::intern("target"), Value::string(event.target)),
                (Symbol::intern("action"), Value::string(event.action)),
                (Symbol::intern("fields"), sync_event_fields(event.fields)),
            ],
        )
        .await
        .map_err(|error| format_driver_error(&host.driver, error))?;
    trace.mark("sync_event");
    match submitted.outcome {
        TaskOutcome::Complete { value, .. } => {
            if std::env::var_os("MICA_WT_TRACE_SYNC").is_some() {
                eprintln!("sync-trace dom_event sync_event_value {value}");
            }
        }
        TaskOutcome::Suspended { .. } => {}
        TaskOutcome::Aborted { error, .. } => {
            return Err(format!("sync_event aborted: {error}"));
        }
    }
    let result = refresh_active_sync_view(host, endpoint, event.session_id, event.view_id).await;
    trace.mark("refresh");
    result
}

async fn send_recovery_snapshot(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    event: &DomEventPayload,
) -> Result<(), String> {
    let rendered = render_sync_view(host, endpoint, event.view_id).await?;
    send_recovery_snapshot_from_rendered(host, endpoint, event, rendered).await
}

async fn send_recovery_snapshot_from_rendered(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    event: &DomEventPayload,
    rendered: RenderedSyncView,
) -> Result<(), String> {
    let envelope = snapshot_envelope(
        event.session_id,
        event.view_id,
        event.revision,
        event.signature,
        &rendered,
    );
    host.send_sync_envelope(endpoint, envelope)?;
    host.store_rendered_sync_view(endpoint, event.session_id, event.view_id, &rendered);
    Ok(())
}

async fn refresh_active_sync_view(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
) -> Result<(), String> {
    let Some(view_state) = host.active_rendered_sync_view(endpoint, session_id, view_id) else {
        return Ok(());
    };
    let active = ActiveSyncView {
        endpoint,
        session_id,
        view_id,
        client_revision: view_state.client_revision,
        client_signature: view_state.client_signature,
        server_revision: view_state.server_revision,
        server_signature: view_state.server_signature,
        last_tree: view_state.last_tree,
    };
    refresh_active_sync_view_for(&host.driver, &host.sessions, active).await
}

async fn refresh_active_sync_views_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
) -> Result<(), String> {
    for active in active_sync_views(sessions) {
        refresh_active_sync_view_for(driver, sessions, active).await?;
    }
    Ok(())
}

async fn refresh_active_sync_view_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    active: ActiveSyncView,
) -> Result<(), String> {
    let revision = render_sync_revision_for(driver, active.endpoint, active.view_id).await?;
    if revision == active.server_revision && active.last_tree.is_some() {
        return Ok(());
    }

    let rendered = render_sync_view_for(driver, active.endpoint, active.view_id).await?;
    let envelope = if let Some(last_tree) = &active.last_tree {
        let patches = diff_dom_nodes(last_tree, &rendered.tree);
        if patches.is_empty() {
            store_rendered_sync_view_in(
                sessions,
                active.endpoint,
                active.session_id,
                active.view_id,
                &rendered,
            );
            return Ok(());
        }
        let payload = dom_patch_payload_json(active.view_id, rendered.revision, &patches);
        SyncEnvelope {
            kind: SyncMessageKind::ViewDelta,
            session_id: active.session_id,
            view_id: active.view_id,
            client_revision: active.server_revision,
            client_signature: active.server_signature,
            server_revision: rendered.revision,
            server_signature: rendered.signature,
            payload,
        }
    } else {
        snapshot_envelope(
            active.session_id,
            active.view_id,
            active.client_revision,
            active.client_signature,
            &rendered,
        )
    };
    send_sync_envelope_to(sessions, active.endpoint, envelope)?;
    store_rendered_sync_view_in(
        sessions,
        active.endpoint,
        active.session_id,
        active.view_id,
        &rendered,
    );
    Ok(())
}

async fn render_sync_view(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    view_id: u64,
) -> Result<RenderedSyncView, String> {
    render_sync_view_for(&host.driver, endpoint, view_id).await
}

async fn render_sync_revision(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    view_id: u64,
) -> Result<u64, String> {
    render_sync_revision_for(&host.driver, endpoint, view_id).await
}

async fn render_sync_revision_for(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    view_id: u64,
) -> Result<u64, String> {
    sync_u64_from_task_value(
        "sync_view_revision",
        submit_sync_invocation_for(
            driver,
            endpoint,
            "sync_view_revision",
            vec![(Symbol::intern("view"), sync_u64_value(view_id))],
        )
        .await?,
    )
}

async fn render_sync_view_for(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    view_id: u64,
) -> Result<RenderedSyncView, String> {
    let trace = SyncTrace::new("render");
    let revision = render_sync_revision_for(driver, endpoint, view_id).await?;
    trace.mark("revision");
    let tree_value = submit_sync_invocation_for(
        driver,
        endpoint,
        "sync_view_tree",
        vec![
            (Symbol::intern("view"), sync_u64_value(view_id)),
            (Symbol::intern("revision"), sync_u64_value(revision)),
        ],
    )
    .await?;
    trace.mark("tree");
    let tree = DomNode::from_mica_value(&tree_value)
        .map_err(|error| format!("sync_view_tree returned invalid DOM tree: {error}"))?;
    trace.mark("decode_tree");
    let payload = snapshot_payload_json(view_id, revision, &tree);
    let signature = sync_payload_signature(revision, &payload);
    trace.mark("payload");

    Ok(RenderedSyncView {
        revision,
        signature,
        tree,
        payload,
    })
}

struct SyncTrace {
    enabled: bool,
    label: &'static str,
    start: Instant,
    last: Mutex<Instant>,
}

impl SyncTrace {
    fn new(label: &'static str) -> Self {
        let now = Instant::now();
        Self {
            enabled: std::env::var_os("MICA_WT_TRACE_SYNC").is_some(),
            label,
            start: now,
            last: Mutex::new(now),
        }
    }

    fn mark(&self, phase: &'static str) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut last = self.last.lock().unwrap();
        eprintln!(
            "sync-trace {} {} +{:?} total {:?}",
            self.label,
            phase,
            now.duration_since(*last),
            now.duration_since(self.start)
        );
        *last = now;
    }
}

async fn submit_sync_invocation_for(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    selector: &'static str,
    roles: Vec<(Symbol, Value)>,
) -> Result<Value, String> {
    let submitted = driver
        .submit_invocation_for_endpoint(endpoint, Symbol::intern(selector), roles)
        .await
        .map_err(|error| format_driver_error(driver, error))?;
    match submitted.outcome {
        TaskOutcome::Complete { value, .. } => Ok(value),
        TaskOutcome::Aborted { error, .. } => Err(format!(
            "sync render invocation {selector} aborted: {error}"
        )),
        TaskOutcome::Suspended { .. } => {
            Err(format!("sync render invocation {selector} suspended"))
        }
    }
}

fn store_rendered_sync_view_in(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
    rendered: &RenderedSyncView,
) {
    if let Some(state) = sessions.lock().unwrap().get(&endpoint).cloned() {
        state.sync.lock().unwrap().store_rendered_view(
            session_id,
            view_id,
            rendered.revision,
            rendered.signature,
            rendered.tree.clone(),
        );
    }
}

fn send_sync_envelope_to(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    endpoint: Identity,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    let Some(state) = sessions.lock().unwrap().get(&endpoint).cloned() else {
        return Ok(());
    };
    let datagrams = sync_envelope_datagrams(envelope.as_ref());
    for _ in 0..SYNC_ENVELOPE_SEND_ATTEMPTS {
        for datagram in &datagrams {
            state.output.send_datagram(datagram.clone())?;
        }
    }
    Ok(())
}

fn sync_u64_from_task_value(selector: &str, value: Value) -> Result<u64, String> {
    mica_host_protocol::sync_u64_from_value(&value)
        .ok_or_else(|| format!("{selector} returned non-u64 value: {value}"))
}

fn snapshot_envelope(
    session_id: u64,
    view_id: u64,
    client_revision: u64,
    client_signature: u64,
    rendered: &RenderedSyncView,
) -> SyncEnvelope {
    SyncEnvelope {
        kind: SyncMessageKind::ViewSnapshot,
        session_id,
        view_id,
        client_revision,
        client_signature,
        server_revision: rendered.revision,
        server_signature: rendered.signature,
        payload: rendered.payload.clone(),
    }
}

fn sync_event_fields(fields: BTreeMap<String, String>) -> Value {
    Value::map(
        fields
            .into_iter()
            .map(|(key, value)| (Value::symbol(Symbol::intern(&key)), Value::string(value))),
    )
}

async fn route_sync_envelope(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    if let Some(state) = host.sessions.lock().unwrap().get(&endpoint).cloned() {
        state.sync.lock().unwrap().record_incoming_view(&envelope);
    }
    if let Some(event) = decode_dom_event_payload(&envelope.payload)? {
        return route_dom_event(host, endpoint, event).await;
    }
    match envelope.kind {
        SyncMessageKind::HaveView => {
            let revision = render_sync_revision(host, endpoint, envelope.view_id).await?;
            if envelope.client_revision == revision
                && let Some(active) =
                    host.active_rendered_sync_view(endpoint, envelope.session_id, envelope.view_id)
                && active.server_revision == revision
                && active.server_signature == envelope.client_signature
                && active.last_tree.is_some()
            {
                return Ok(());
            }
            let rendered = render_sync_view(host, endpoint, envelope.view_id).await?;
            let response = host
                .active_rendered_sync_view(endpoint, envelope.session_id, envelope.view_id)
                .and_then(|active| {
                    if active.server_revision != envelope.client_revision
                        || active.server_signature != envelope.client_signature
                    {
                        return None;
                    }
                    let last_tree = active.last_tree.as_ref()?;
                    let patches = diff_dom_nodes(last_tree, &rendered.tree);
                    if patches.is_empty() {
                        return None;
                    }
                    Some(SyncEnvelope {
                        kind: SyncMessageKind::ViewDelta,
                        session_id: envelope.session_id,
                        view_id: envelope.view_id,
                        client_revision: active.server_revision,
                        client_signature: active.server_signature,
                        server_revision: rendered.revision,
                        server_signature: rendered.signature,
                        payload: dom_patch_payload_json(
                            envelope.view_id,
                            rendered.revision,
                            &patches,
                        ),
                    })
                })
                .unwrap_or_else(|| {
                    snapshot_envelope(
                        envelope.session_id,
                        envelope.view_id,
                        envelope.client_revision,
                        envelope.client_signature,
                        &rendered,
                    )
                });
            host.send_sync_envelope(endpoint, response)?;
            host.store_rendered_sync_view(
                endpoint,
                envelope.session_id,
                envelope.view_id,
                &rendered,
            );
            Ok(())
        }
        SyncMessageKind::NeedView => {
            let rendered = render_sync_view(host, endpoint, envelope.view_id).await?;
            let response = snapshot_envelope(
                envelope.session_id,
                envelope.view_id,
                envelope.client_revision,
                envelope.client_signature,
                &rendered,
            );
            host.send_sync_envelope(endpoint, response)?;
            host.store_rendered_sync_view(
                endpoint,
                envelope.session_id,
                envelope.view_id,
                &rendered,
            );
            Ok(())
        }
        SyncMessageKind::ViewSnapshot | SyncMessageKind::ViewDelta => host
            .driver
            .input(
                endpoint,
                Value::bytes(encoded_sync_envelope(envelope.as_ref())),
            )
            .await
            .map(|_| ())
            .map_err(|error| format_driver_error(&host.driver, error)),
    }
}

async fn write_datagram_loop(
    session: Rc<WtSession>,
    output: Arc<SessionOutput>,
) -> Result<(), String> {
    let mut sender = session.datagram_sender();
    while let SessionOutputReady::Ready { .. } | SessionOutputReady::HighWater { .. } =
        output.recv().await
    {
        for message in output.drain_batch(ENDPOINT_OUTPUT_DRAIN_DATAGRAMS) {
            match message {
                SessionOutputMessage::Datagram(datagram) => {
                    sender.send_datagram(datagram).map_err(|error| {
                        format!("failed to send WebTransport datagram: {error}")
                    })?;
                    compio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
    }
    Ok(())
}

fn drop_session_writer(host: &InProcessWebTransportHost, endpoint: Identity) {
    if let Some(state) = host.sessions.lock().unwrap().remove(&endpoint) {
        state.output.close();
    }
}

fn start_event_pump(
    driver: Arc<CompioTaskDriver>,
    sessions: Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    stop_events: Arc<AtomicBool>,
) {
    compio::runtime::spawn(async move {
        while !stop_events.load(Ordering::Relaxed) {
            let events = driver.wait_events().await;
            let mut refresh_views = false;
            for event in events {
                refresh_views |= route_driver_event(&sessions, event);
            }
            if refresh_views
                && let Err(error) = refresh_active_sync_views_for(&driver, &sessions).await
            {
                eprintln!("failed to refresh active WebTransport sync views: {error}");
            }
        }
    })
    .detach();
}

fn active_sync_views(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
) -> Vec<ActiveSyncView> {
    let states = sessions
        .lock()
        .unwrap()
        .iter()
        .map(|(endpoint, state)| (*endpoint, state.clone()))
        .collect::<Vec<_>>();
    let mut active = Vec::new();
    for (endpoint, state) in states {
        let sync = state.sync.lock().unwrap();
        for (session_id, views) in &sync.sessions {
            for (view_id, view_state) in views {
                active.push(ActiveSyncView {
                    endpoint,
                    session_id: *session_id,
                    view_id: *view_id,
                    client_revision: view_state.client_revision,
                    client_signature: view_state.client_signature,
                    server_revision: view_state.server_revision,
                    server_signature: view_state.server_signature,
                    last_tree: view_state.last_tree.clone(),
                });
            }
        }
    }
    active
}

fn route_driver_event(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    event: DriverEvent,
) -> bool {
    let DriverEvent::Effect(effect) = event else {
        return false;
    };
    if let Some(state) = sessions.lock().unwrap().get(&effect.target).cloned() {
        for datagram in effect_datagrams(effect.target, &effect.value) {
            let _ = state.output.send_datagram(datagram);
        }
        return true;
    }
    false
}

fn effect_datagrams(target: Identity, value: &Value) -> Vec<Bytes> {
    if let Some(envelope) = sync_envelope_from_value(target.raw(), value) {
        return sync_envelope_datagrams(envelope.as_ref());
    }
    vec![effect_datagram(target, value)]
}

fn effect_datagram(target: Identity, value: &Value) -> Bytes {
    if let Some(envelope) = sync_envelope_from_value(target.raw(), value) {
        return Bytes::from(encoded_sync_envelope(envelope.as_ref()));
    }
    if let Some(bytes) = value.with_bytes(Bytes::copy_from_slice) {
        return bytes;
    }
    if let Some(text) = value.with_str(|value| Bytes::copy_from_slice(value.as_bytes())) {
        return text;
    }
    Bytes::from(value.to_string())
}

fn sync_envelope_datagrams(envelope: mica_host_protocol::SyncEnvelopeRef<'_>) -> Vec<Bytes> {
    let encoded = encoded_sync_envelope(envelope);
    if encoded.len() <= SYNC_DATAGRAM_MAX_LEN {
        return vec![Bytes::from(encoded)];
    }

    let count = encoded.len().div_ceil(SYNC_CHUNK_PAYLOAD_LEN);
    let message_id = NEXT_SYNC_CHUNK_ID.fetch_add(1, Ordering::Relaxed);
    encoded
        .chunks(SYNC_CHUNK_PAYLOAD_LEN)
        .enumerate()
        .map(|(index, chunk)| {
            let mut datagram = Vec::with_capacity(SYNC_CHUNK_HEADER_LEN + chunk.len());
            datagram.extend_from_slice(SYNC_CHUNK_MAGIC);
            datagram.extend_from_slice(&message_id.to_le_bytes());
            datagram.extend_from_slice(&(index as u32).to_le_bytes());
            datagram.extend_from_slice(&(count as u32).to_le_bytes());
            datagram.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            datagram.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            datagram.extend_from_slice(chunk);
            Bytes::from(datagram)
        })
        .collect()
}

fn format_driver_error(driver: &CompioTaskDriver, error: mica_driver::DriverError) -> String {
    format!("error: {}", driver.format_error(&error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_runtime::SourceRunner;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::io::AsyncReadExt;

    type TestChunkMap = HashMap<u32, (u32, u32, Vec<Option<Vec<u8>>>)>;
    type MudLoginResult = Result<(SyncEnvelope, SyncEnvelope), String>;
    type MudClientResult = Result<
        (
            SyncEnvelope,
            SyncEnvelope,
            SyncEnvelope,
            SyncEnvelope,
            SyncEnvelope,
        ),
        String,
    >;
    type MudDelayedEventResult =
        Result<(SyncEnvelope, SyncEnvelope, SyncEnvelope, SyncEnvelope), String>;
    type MudTwoSessionResult = Result<(SyncEnvelope, SyncEnvelope), String>;
    static MUD_WEBTRANSPORT_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn sync_client_accepts_protocol_tags() {
        let client = include_str!("../sync-client.js");

        for tag in SUPPORTED_DOM_TAGS {
            assert!(
                client.contains(&format!("\"{tag}\"")),
                "sync-client.js is missing supported DOM tag {tag}"
            );
        }
    }

    #[test]
    fn effect_datagrams_preserve_bytes_and_strings() {
        let endpoint = Identity::new(DAEMON_ENDPOINT_ID_START).unwrap();
        assert_eq!(
            effect_datagram(endpoint, &Value::bytes([0xde, 0xad, 0xbe, 0xef])).as_ref(),
            &[0xde, 0xad, 0xbe, 0xef]
        );
        assert_eq!(
            effect_datagram(endpoint, &Value::string("hello")).as_ref(),
            b"hello"
        );
    }

    #[test]
    fn sync_effect_values_encode_view_datagrams() {
        let endpoint = Identity::new(DAEMON_ENDPOINT_ID_START).unwrap();
        let datagram = effect_datagram(
            endpoint,
            &Value::list([
                Value::symbol(Symbol::intern("view_delta")),
                Value::identity(endpoint),
                Value::int(9).unwrap(),
                Value::int(1).unwrap(),
                Value::int(2).unwrap(),
                Value::int(3).unwrap(),
                Value::int(4).unwrap(),
                Value::string("patch"),
            ]),
        );
        let envelope = decode_sync_envelope(&datagram).unwrap();

        assert_eq!(envelope.kind, SyncMessageKind::ViewDelta);
        assert_eq!(envelope.session_id, endpoint.raw());
        assert_eq!(envelope.view_id, 9);
        assert_eq!(envelope.client_revision, 1);
        assert_eq!(envelope.client_signature, 2);
        assert_eq!(envelope.server_revision, 3);
        assert_eq!(envelope.server_signature, 4);
        assert_eq!(envelope.payload, b"patch");
    }

    #[test]
    fn session_sync_state_tracks_client_active_views() {
        let mut state = SessionSyncState::default();
        state.record_incoming_view(&SyncEnvelope {
            kind: SyncMessageKind::NeedView,
            session_id: 7,
            view_id: 11,
            client_revision: 0,
            client_signature: 0,
            server_revision: 0,
            server_signature: 0,
            payload: Vec::new(),
        });
        state.record_incoming_view(&SyncEnvelope {
            kind: SyncMessageKind::ViewSnapshot,
            session_id: 7,
            view_id: 12,
            client_revision: 0,
            client_signature: 0,
            server_revision: 0,
            server_signature: 0,
            payload: Vec::new(),
        });

        assert!(state.has_active_view(7, 11));
        assert!(!state.has_active_view(7, 12));
    }

    #[test]
    fn active_sync_views_snapshot_does_not_hold_session_lock() {
        let endpoint = Identity::new(DAEMON_ENDPOINT_ID_START).unwrap();
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let state = SessionState::new();
        state
            .sync
            .lock()
            .unwrap()
            .record_incoming_view(&SyncEnvelope {
                kind: SyncMessageKind::NeedView,
                session_id: 7,
                view_id: 11,
                client_revision: 0,
                client_signature: 0,
                server_revision: 0,
                server_signature: 0,
                payload: Vec::new(),
            });
        sessions.lock().unwrap().insert(endpoint, state.clone());

        assert_eq!(
            active_sync_views(&sessions),
            vec![ActiveSyncView {
                endpoint,
                session_id: 7,
                view_id: 11,
                client_revision: 0,
                client_signature: 0,
                server_revision: 0,
                server_signature: 0,
                last_tree: None,
            }]
        );
        assert!(state.sync.try_lock().is_ok());
    }

    #[test]
    fn drop_session_writer_removes_active_views() {
        let endpoint = Identity::new(DAEMON_ENDPOINT_ID_START).unwrap();
        let host = InProcessWebTransportHost::new_without_event_pump(
            CompioTaskDriver::spawn_empty().unwrap(),
        );
        let state = SessionState::new();
        state
            .sync
            .lock()
            .unwrap()
            .record_incoming_view(&SyncEnvelope {
                kind: SyncMessageKind::HaveView,
                session_id: 7,
                view_id: 11,
                client_revision: 1,
                client_signature: 305,
                server_revision: 1,
                server_signature: 305,
                payload: Vec::new(),
            });
        host.sessions.lock().unwrap().insert(endpoint, state);

        assert_eq!(host.active_sync_views().len(), 1);
        drop_session_writer(&host, endpoint);
        assert!(host.active_sync_views().is_empty());
    }

    #[test]
    fn endpoint_allocation_uses_webtransport_identity_space() {
        let host = InProcessWebTransportHost::new_without_event_pump(
            CompioTaskDriver::spawn_empty().unwrap(),
        );
        assert_eq!(
            host.allocate_endpoint().unwrap(),
            Identity::new(DAEMON_ENDPOINT_ID_START).unwrap()
        );
    }

    #[test]
    fn routed_effect_reaches_session_output() {
        let endpoint = Identity::new(DAEMON_ENDPOINT_ID_START).unwrap();
        let output = SessionOutput::new();
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        sessions.lock().unwrap().insert(
            endpoint,
            Arc::new(SessionState {
                output: output.clone(),
                sync: Mutex::new(SessionSyncState::default()),
            }),
        );

        route_driver_event(
            &sessions,
            DriverEvent::Effect(mica_runtime::Effect {
                task_id: 1,
                target: endpoint,
                value: Value::string("hello"),
            }),
        );

        assert_eq!(output.try_recv().unwrap().as_ref(), b"hello");
    }

    #[test]
    fn webtransport_sync_need_view_renders_snapshot() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_chat_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let request = encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::NeedView,
                    session_id: 7,
                    view_id: 11,
                    client_revision: 13,
                    client_signature: 17,
                    server_revision: 19,
                    server_signature: 23,
                    payload: b"need".to_vec(),
                }
                .as_ref(),
            );
            let client = spawn_wtransport_smoke_client(
                server_addr,
                request,
                connected_tx,
                send_rx,
                result_tx,
            );

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let received = wait_for_client_result(&result_rx).await.unwrap();
            let envelope = decode_sync_envelope(&received).unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(envelope.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(envelope.session_id, 7);
            assert_eq!(envelope.view_id, 11);
            assert_eq!(envelope.client_revision, 13);
            assert_eq!(envelope.client_signature, 17);
            assert_eq!(envelope.server_revision, 1);
            assert_eq!(
                envelope.server_signature,
                sync_payload_signature(envelope.server_revision, &envelope.payload)
            );
            let payload: serde_json::Value = serde_json::from_slice(&envelope.payload).unwrap();
            assert_eq!(payload["view"], 11);
            assert_eq!(payload["revision"], 1);
            assert_eq!(payload["root"]["tag"], "main");
            assert_eq!(payload["root"]["attrs"]["id"], "chat-root");
            assert_eq!(
                payload["root"]["children"][0]["children"][0]["children"][0]["children"][0]["text"],
                "alice"
            );
            assert_eq!(payload["root"]["children"][1]["tag"], "form");
            assert_eq!(
                payload["root"]["children"][1]["attrs"]["data-sync-action"],
                "chat_post"
            );
        });
    }

    #[test]
    fn webtransport_sync_event_pushes_chat_delta() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_chat_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client =
                spawn_wtransport_dom_event_client(server_addr, connected_tx, send_rx, result_tx);

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (snapshot, delta) = wait_for_ack_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(snapshot.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delta.session_id, 7);
            assert_eq!(delta.view_id, 11);
            assert_eq!(delta.client_revision, 1);
            assert_eq!(delta.server_revision, 2);
            assert_ne!(delta.server_signature, 0);
            let payload: serde_json::Value = serde_json::from_slice(&delta.payload).unwrap();
            assert_eq!(payload["type"], "dom_patch");
            assert_eq!(payload["view"], 11);
            assert_eq!(payload["revision"], 2);
            assert_eq!(payload["patches"][0]["op"], "append_child");
            assert_eq!(payload["patches"][0]["path"], serde_json::json!([0]));
            assert_eq!(payload["patches"][0]["node"]["tag"], "li");
            assert_eq!(
                payload["patches"][0]["node"]["children"][0]["children"][0]["text"],
                "bob"
            );
            assert_eq!(
                payload["patches"][0]["node"]["children"][2]["text"],
                "hello from sync event"
            );
        });
    }

    #[test]
    fn webtransport_mud_login_pushes_world_delta() {
        let _guard = MUD_WEBTRANSPORT_TEST_LOCK.lock().unwrap();
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_mud_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client =
                spawn_wtransport_mud_login_client(server_addr, connected_tx, send_rx, result_tx);

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (snapshot, delta, command_delta, inspect_delta, mica_inspect_delta) =
                wait_for_mud_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(snapshot.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(snapshot.view_id, 21);
            assert_eq!(snapshot.server_revision, 0);
            let snapshot_payload: serde_json::Value =
                serde_json::from_slice(&snapshot.payload).unwrap();
            assert_eq!(snapshot_payload["root"]["attrs"]["class"], "mud-login");
            assert_eq!(
                snapshot_payload["root"]["children"][0]["attrs"]["class"],
                "login-card"
            );
            let snapshot_text = serde_json::to_string(&snapshot_payload).unwrap();
            assert!(snapshot_text.contains("actor-card"));
            assert!(snapshot_text.contains("Server-owned session view"));
            assert!(snapshot_text.contains("Enter as Alice"));
            assert!(snapshot_text.contains("Enter as Bob"));

            assert_eq!(delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delta.view_id, 21);
            assert_eq!(delta.client_revision, 0);
            assert_eq!(delta.server_revision, 1);
            let payload: serde_json::Value = serde_json::from_slice(&delta.payload).unwrap();
            assert_eq!(payload["type"], "dom_patch");
            assert_eq!(payload["view"], 21);
            assert_eq!(payload["revision"], 1);
            let payload_text = serde_json::to_string(&payload).unwrap();
            assert!(payload_text.contains("mud-shell"));
            assert!(payload_text.contains("The Mica Rooms"));
            assert!(payload_text.contains("First Room"));
            assert!(payload_text.contains("Available actions"));
            assert!(payload_text.contains("exit-chip"));
            assert!(payload_text.contains("room-panel"));
            assert!(payload_text.contains("mud-sidebar"));
            assert!(payload_text.contains("People here"));
            assert!(payload_text.contains("presence-card"));
            assert!(payload_text.contains("object-list"));
            assert!(payload_text.contains("object-row"));
            assert!(payload_text.contains("coin"));
            assert!(payload_text.contains("Inventory"));
            assert!(payload_text.contains("Examine"));
            assert!(!payload_text.contains("Mica Inspect"));
            assert!(!payload_text.contains("has-mica-inspect"));
            assert!(payload_text.contains("Narrative"));

            assert_eq!(command_delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(command_delta.view_id, 21);
            assert_eq!(command_delta.client_revision, 1);
            assert_eq!(command_delta.server_revision, 3);
            let command_payload: serde_json::Value =
                serde_json::from_slice(&command_delta.payload).unwrap();
            let command_payload_text = serde_json::to_string(&command_payload).unwrap();
            assert!(command_payload_text.contains("event-line transfer"));
            assert!(command_payload_text.contains("event-line-main"));
            assert!(command_payload_text.contains("event-kind"));
            assert!(command_payload_text.contains("event-entity"));
            assert!(command_payload_text.contains("data-entity"));
            assert!(command_payload_text.contains("entity-action"));
            assert!(command_payload_text.contains("entity-button"));
            assert!(command_payload_text.contains("you"));
            assert!(command_payload_text.contains("coin"));
            assert!(command_payload_text.contains("drop"));

            assert_eq!(inspect_delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(inspect_delta.view_id, 21);
            assert_eq!(inspect_delta.client_revision, 3);
            assert_eq!(inspect_delta.server_revision, 5);
            let inspect_payload: serde_json::Value =
                serde_json::from_slice(&inspect_delta.payload).unwrap();
            let inspect_payload_text = serde_json::to_string(&inspect_payload).unwrap();
            assert!(inspect_payload_text.contains("inspector"));
            assert!(inspect_payload_text.contains("entity-facts"));
            assert!(inspect_payload_text.contains("entity-kind"));
            assert!(inspect_payload_text.contains("entity-location"));
            assert!(inspect_payload_text.contains("entity-avatar"));
            assert!(inspect_payload_text.contains("look coin"));
            assert!(inspect_payload_text.contains("Mica inspect"));
            assert!(inspect_payload_text.contains("data-entity"));
            assert!(inspect_payload_text.contains("tarnished brass coin"));

            assert_eq!(mica_inspect_delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(mica_inspect_delta.view_id, 21);
            let mica_inspect_payload: serde_json::Value =
                serde_json::from_slice(&mica_inspect_delta.payload).unwrap();
            let mica_inspect_payload_text = serde_json::to_string(&mica_inspect_payload).unwrap();
            assert!(mica_inspect_payload_text.contains("Mica Inspect"));
            assert!(mica_inspect_payload_text.contains("Subject facts"));
            assert!(mica_inspect_payload_text.contains("Relation mentions"));
            assert!(mica_inspect_payload_text.contains("Method catalogue"));
            assert!(mica_inspect_payload_text.contains("method-filter active"));
            assert!(mica_inspect_payload_text.contains("mud_mica_inspect_close"));
            assert!(mica_inspect_payload_text.contains("tarnished brass coin"));
        });
    }

    #[test]
    fn webtransport_mud_login_as_bob_renders_bob_perspective() {
        let _guard = MUD_WEBTRANSPORT_TEST_LOCK.lock().unwrap();
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_mud_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client = spawn_wtransport_mud_login_only_client(
                server_addr,
                "bob",
                connected_tx,
                send_rx,
                result_tx,
            );

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (snapshot, delta) = wait_for_mud_login_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(snapshot.kind, SyncMessageKind::ViewSnapshot);
            let snapshot_payload: serde_json::Value =
                serde_json::from_slice(&snapshot.payload).unwrap();
            let snapshot_text = serde_json::to_string(&snapshot_payload).unwrap();
            assert!(snapshot_text.contains("Enter as Alice"));
            assert!(snapshot_text.contains("Enter as Bob"));

            assert_eq!(delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delta.view_id, 21);
            assert_eq!(delta.server_revision, 1);
            let payload: serde_json::Value = serde_json::from_slice(&delta.payload).unwrap();
            let payload_text = serde_json::to_string(&payload).unwrap();
            assert!(payload_text.contains("Bob"));
            assert!(payload_text.contains("mud-sidebar"));
            assert!(payload_text.contains("People here"));
            assert!(!payload_text.contains("look Bob"));
            assert!(!payload_text.contains("Mica Inspect"));
            assert!(!payload_text.contains("mica-inspect"));
        });
    }

    #[test]
    fn webtransport_mud_pushes_alice_command_to_bob_view() {
        let _guard = MUD_WEBTRANSPORT_TEST_LOCK.lock().unwrap();
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_mud_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(2))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client = spawn_wtransport_mud_two_session_client(
                server_addr,
                connected_tx,
                send_rx,
                result_tx,
            );

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (bob_delta, bob_inspect_delta) =
                wait_for_mud_two_session_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();

            assert!(matches!(
                bob_delta.kind,
                SyncMessageKind::ViewDelta | SyncMessageKind::ViewSnapshot
            ));
            assert_eq!(bob_delta.view_id, 21);
            if bob_delta.kind == SyncMessageKind::ViewDelta {
                assert_eq!(bob_delta.client_revision, 1);
            }
            assert_eq!(bob_delta.server_revision, 2);
            let bob_payload: serde_json::Value =
                serde_json::from_slice(&bob_delta.payload).unwrap();
            let bob_payload_text = serde_json::to_string(&bob_payload).unwrap();
            assert!(bob_payload_text.contains("Alice"));
            assert!(bob_payload_text.contains("actor-entity"));
            assert!(bob_payload_text.contains("takes"));
            assert!(bob_payload_text.contains("coin"));
            assert!(bob_payload_text.contains("event-line transfer"));
            assert!(bob_payload_text.contains("event-line-main"));
            assert!(bob_payload_text.contains("event-kind"));
            assert!(bob_payload_text.contains("entity-action"));

            assert!(matches!(
                bob_inspect_delta.kind,
                SyncMessageKind::ViewDelta | SyncMessageKind::ViewSnapshot
            ));
            assert_eq!(bob_inspect_delta.view_id, 21);
            if bob_inspect_delta.kind == SyncMessageKind::ViewDelta {
                assert_eq!(bob_inspect_delta.client_revision, bob_delta.server_revision);
            }
            let bob_inspect_payload: serde_json::Value =
                serde_json::from_slice(&bob_inspect_delta.payload).unwrap();
            let bob_inspect_payload_text = serde_json::to_string(&bob_inspect_payload).unwrap();
            assert!(bob_inspect_payload_text.contains("Alice"));
            assert!(bob_inspect_payload_text.contains("entity-facts"));
            assert!(bob_inspect_payload_text.contains("entity-kind"));
            assert!(bob_inspect_payload_text.contains("entity-location"));
            assert!(bob_inspect_payload_text.contains("object-actions"));
            assert!(
                bob_inspect_payload_text
                    .contains("Alice is alert and ready to test the room's stranger affordances.")
            );
        });
    }

    #[test]
    fn webtransport_mud_suspended_command_pushes_delayed_event() {
        let _guard = MUD_WEBTRANSPORT_TEST_LOCK.lock().unwrap();
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_mud_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client = spawn_wtransport_mud_suspended_command_client(
                server_addr,
                connected_tx,
                send_rx,
                result_tx,
            );

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (snapshot, login_delta, push_delta, delayed_delta) =
                wait_for_mud_delayed_event_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();

            assert_eq!(snapshot.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(login_delta.kind, SyncMessageKind::ViewDelta);

            assert_eq!(push_delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(push_delta.view_id, 21);
            assert_eq!(push_delta.client_revision, 1);
            assert!(push_delta.server_revision > login_delta.server_revision);
            let push_payload: serde_json::Value =
                serde_json::from_slice(&push_delta.payload).unwrap();
            let push_payload_text = serde_json::to_string(&push_payload).unwrap();
            assert!(push_payload_text.contains("begins to hum"));

            assert_eq!(delayed_delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delayed_delta.view_id, 21);
            assert_eq!(delayed_delta.client_revision, push_delta.server_revision);
            assert!(delayed_delta.server_revision > push_delta.server_revision);
            let poll_payload: serde_json::Value =
                serde_json::from_slice(&delayed_delta.payload).unwrap();
            let poll_payload_text = serde_json::to_string(&poll_payload).unwrap();
            assert!(poll_payload_text.contains("cheerful ding"));
            assert!(poll_payload_text.contains("event-line alert"));
            assert!(poll_payload_text.contains("event-line-main"));
            assert!(poll_payload_text.contains("event-kind"));
            assert!(poll_payload_text.contains("data-entity"));
            assert!(poll_payload_text.contains("entity-button"));
        });
    }

    #[test]
    fn webtransport_stale_dom_event_is_still_processed() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_chat_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client =
                spawn_wtransport_stale_event_client(server_addr, connected_tx, send_rx, result_tx);

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let delta = wait_for_snapshot_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delta.client_revision, 1);
            assert!(delta.server_revision > 1);
            let payload: serde_json::Value = serde_json::from_slice(&delta.payload).unwrap();
            let payload_text = serde_json::to_string(&payload).unwrap();
            assert!(payload_text.contains("bob"));
            assert!(payload_text.contains("stale"));
        });
    }

    #[test]
    fn webtransport_have_view_ack_does_not_snapshot_current_state() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let runner = sync_chat_runner();
            let principal = runner.named_identity(Symbol::intern("web")).unwrap();
            let driver = CompioTaskDriver::spawn(runner).unwrap();
            let host = InProcessWebTransportHost::new(driver.clone());
            let binding = SessionBinding {
                principal,
                actor: None,
            };
            compio::runtime::spawn(serve_in_process(endpoint, host, binding, Some(1))).detach();

            let (connected_tx, connected_rx) = mpsc::channel();
            let (send_tx, send_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let client = spawn_wtransport_ack_client(server_addr, connected_tx, send_rx, result_tx);

            wait_for_client_connected(&connected_rx).await;
            send_tx.send(()).unwrap();
            let (snapshot, delta) = wait_for_ack_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(snapshot.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(delta.kind, SyncMessageKind::ViewDelta);
            assert_eq!(delta.server_revision, 2);
            assert_ne!(delta.server_signature, 0);
        });
    }

    fn test_tls_config() -> WebTransportTlsConfig {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        WebTransportTlsConfig {
            cert_chain: vec![cert.der().clone()],
            key_der: signing_key.serialize_der().try_into().unwrap(),
        }
    }

    fn sync_chat_runner() -> SourceRunner {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/sync-host.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/chat/sync.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/shared/sync-dom.mica"))
            .unwrap();
        runner
            .run_filein_with_include_loader(
                include_str!("../../../apps/chat/http.mica"),
                chat_http_include,
            )
            .unwrap();
        runner
    }

    fn sync_mud_runner() -> SourceRunner {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../apps/shared/sync-host.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/shared/string.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/shared/events.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/core.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/event-substitutions.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/command-parser.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/shared/sync-dom.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-session.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-compose.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-narrative.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-actions.mica"))
            .unwrap();
        runner
            .run_filein_with_include_loader(
                include_str!("../../../apps/mud/http.mica"),
                mud_http_include,
            )
            .unwrap();
        runner
    }

    fn chat_http_include(path: &str) -> Result<String, String> {
        match path {
            "style.css" => Ok(include_str!("../../../apps/chat/style.css").to_owned()),
            "bootstrap.js" => Ok(include_str!("../../../apps/chat/bootstrap.js").to_owned()),
            other => Err(format!("unknown chat HTTP include {other}")),
        }
    }

    fn mud_http_include(path: &str) -> Result<String, String> {
        match path {
            "style.css" => Ok(include_str!("../../../apps/mud/style.css").to_owned()),
            "login.css" => Ok(include_str!("../../../apps/mud/login.css").to_owned()),
            "presence.css" => Ok(include_str!("../../../apps/mud/presence.css").to_owned()),
            "narrative.css" => Ok(include_str!("../../../apps/mud/narrative.css").to_owned()),
            "bootstrap.js" => Ok(include_str!("../../../apps/mud/bootstrap.js").to_owned()),
            other => Err(format!("unknown MUD HTTP include {other}")),
        }
    }

    fn spawn_wtransport_smoke_client(
        server_addr: SocketAddr,
        request: Vec<u8>,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<Result<Vec<u8>, String>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;
                        connection
                            .send_datagram(request)
                            .map_err(|error| error.to_string())?;
                        let datagram = tokio::time::timeout(
                            Duration::from_secs(3),
                            connection.receive_datagram(),
                        )
                        .await
                        .map_err(|_| "timed out waiting for WebTransport datagram".to_owned())?
                        .map_err(|error| error.to_string())?;
                        Ok(datagram.payload().to_vec())
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_dom_event_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<Result<(SyncEnvelope, SyncEnvelope), String>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 11,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection)
                            .await
                            .map_err(|error| format!("initial MUD snapshot: {error}"))?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 11,
                                revision: snapshot.server_revision,
                                signature: snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "chat-composer".to_owned(),
                                action: "chat_post".to_owned(),
                                fields: BTreeMap::from([
                                    ("actor".to_owned(), "bob".to_owned()),
                                    ("text".to_owned(), "hello from sync event".to_owned()),
                                ]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let delta =
                            receive_newer_sync_envelope(&connection, snapshot.server_revision)
                                .await
                                .map_err(|error| format!("MUD login delta: {error}"))?;

                        Ok((snapshot, delta))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_mud_login_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<MudClientResult>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 21,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection).await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: snapshot.server_revision,
                                signature: snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-login".to_owned(),
                                action: "mud_login".to_owned(),
                                fields: BTreeMap::from([("text".to_owned(), "alice".to_owned())]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let delta =
                            receive_newer_sync_envelope(&connection, snapshot.server_revision)
                                .await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: delta.server_revision,
                                signature: delta.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-command".to_owned(),
                                action: "mud_command".to_owned(),
                                fields: BTreeMap::from([(
                                    "text".to_owned(),
                                    "get coin".to_owned(),
                                )]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let command_delta =
                            receive_newer_sync_envelope(&connection, delta.server_revision)
                                .await
                                .map_err(|error| format!("MUD command delta: {error}"))?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: command_delta.server_revision,
                                signature: command_delta.server_signature,
                                event: "submit".to_owned(),
                                target: "event-inspect-coin".to_owned(),
                                action: "mud_command".to_owned(),
                                fields: BTreeMap::from([
                                    ("text".to_owned(), "look coin".to_owned()),
                                    ("entity".to_owned(), "coin".to_owned()),
                                ]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let mut inspect_delta =
                            receive_newer_sync_envelope(&connection, command_delta.server_revision)
                                .await
                                .map_err(|error| format!("MUD inspect delta: {error}"))?;
                        for _ in 0..6 {
                            let payload: serde_json::Value =
                                serde_json::from_slice(&inspect_delta.payload)
                                    .map_err(|error| error.to_string())?;
                            if serde_json::to_string(&payload)
                                .map_err(|error| error.to_string())?
                                .contains("tarnished brass coin")
                            {
                                break;
                            }
                            inspect_delta = receive_newer_sync_envelope(
                                &connection,
                                command_delta.server_revision,
                            )
                            .await
                            .map_err(|error| format!("MUD inspect retry delta: {error}"))?;
                        }

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: inspect_delta.server_revision,
                                signature: inspect_delta.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-command".to_owned(),
                                action: "mud_command".to_owned(),
                                fields: BTreeMap::from([(
                                    "text".to_owned(),
                                    "mica inspect coin".to_owned(),
                                )]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let mut mica_inspect_delta =
                            receive_newer_sync_envelope(&connection, inspect_delta.server_revision)
                                .await
                                .map_err(|error| format!("MUD Mica inspect delta: {error}"))?;
                        for _ in 0..6 {
                            let payload: serde_json::Value =
                                serde_json::from_slice(&mica_inspect_delta.payload)
                                    .map_err(|error| error.to_string())?;
                            if serde_json::to_string(&payload)
                                .map_err(|error| error.to_string())?
                                .contains("Relation mentions")
                            {
                                break;
                            }
                            mica_inspect_delta = receive_newer_sync_envelope(
                                &connection,
                                inspect_delta.server_revision,
                            )
                            .await
                            .map_err(|error| format!("MUD Mica inspect retry delta: {error}"))?;
                        }

                        Ok((
                            snapshot,
                            delta,
                            command_delta,
                            inspect_delta,
                            mica_inspect_delta,
                        ))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_mud_login_only_client(
        server_addr: SocketAddr,
        actor: &'static str,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<MudLoginResult>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 21,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection).await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: snapshot.server_revision,
                                signature: snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: format!("mud-login-{actor}"),
                                action: "mud_login".to_owned(),
                                fields: BTreeMap::from([("text".to_owned(), actor.to_owned())]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let delta =
                            receive_newer_sync_envelope(&connection, snapshot.server_revision)
                                .await?;

                        Ok((snapshot, delta))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_mud_two_session_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<MudTwoSessionResult>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let endpoint = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?;
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let alice = endpoint
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        let bob = endpoint
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let alice_need = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 101,
                            view_id: 21,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        alice
                            .send_datagram(encoded_sync_envelope(alice_need.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let alice_snapshot = receive_sync_envelope(&alice)
                            .await
                            .map_err(|error| format!("alice initial snapshot: {error}"))?;

                        let bob_need = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 202,
                            view_id: 21,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        bob.send_datagram(encoded_sync_envelope(bob_need.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let bob_snapshot = receive_sync_envelope(&bob)
                            .await
                            .map_err(|error| format!("bob initial snapshot: {error}"))?;

                        send_dom_event_stream(
                            &alice,
                            DomEventPayload {
                                session_id: 101,
                                view_id: 21,
                                revision: alice_snapshot.server_revision,
                                signature: alice_snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-login-alice".to_owned(),
                                action: "mud_login".to_owned(),
                                fields: BTreeMap::from([("text".to_owned(), "alice".to_owned())]),
                            },
                        )
                        .await?;
                        let alice_login =
                            receive_newer_sync_envelope(&alice, alice_snapshot.server_revision)
                                .await
                                .map_err(|error| format!("alice login delta: {error}"))?;

                        send_dom_event_stream(
                            &bob,
                            DomEventPayload {
                                session_id: 202,
                                view_id: 21,
                                revision: bob_snapshot.server_revision,
                                signature: bob_snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-login-bob".to_owned(),
                                action: "mud_login".to_owned(),
                                fields: BTreeMap::from([("text".to_owned(), "bob".to_owned())]),
                            },
                        )
                        .await?;
                        let bob_login =
                            receive_newer_sync_envelope(&bob, bob_snapshot.server_revision)
                                .await
                                .map_err(|error| format!("bob login delta: {error}"))?;

                        alice
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 101,
                                view_id: 21,
                                revision: alice_login.server_revision,
                                signature: alice_login.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-command".to_owned(),
                                action: "mud_command".to_owned(),
                                fields: BTreeMap::from([(
                                    "text".to_owned(),
                                    "get coin".to_owned(),
                                )]),
                            }))
                            .map_err(|error| error.to_string())?;

                        tokio::time::sleep(Duration::from_millis(250)).await;
                        let alice_have_view = SyncEnvelope {
                            kind: SyncMessageKind::HaveView,
                            session_id: 101,
                            view_id: 21,
                            client_revision: alice_login.server_revision,
                            client_signature: alice_login.server_signature,
                            server_revision: alice_login.server_revision,
                            server_signature: alice_login.server_signature,
                            payload: b"recover-alice".to_vec(),
                        };
                        alice
                            .send_datagram(encoded_sync_envelope(alice_have_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let mut bob_delta = None;
                        let mut bob_client_revision = bob_login.server_revision;
                        let mut bob_client_signature = bob_login.server_signature;
                        let mut last_bob_error = None;
                        for _ in 0..3 {
                            let bob_have_view = SyncEnvelope {
                                kind: SyncMessageKind::HaveView,
                                session_id: 202,
                                view_id: 21,
                                client_revision: bob_client_revision,
                                client_signature: bob_client_signature,
                                server_revision: bob_client_revision,
                                server_signature: bob_client_signature,
                                payload: b"recover-bob".to_vec(),
                            };
                            bob.send_datagram(encoded_sync_envelope(bob_have_view.as_ref()))
                                .map_err(|error| error.to_string())?;

                            match receive_newer_sync_envelope(&bob, bob_client_revision).await {
                                Ok(envelope) => {
                                    bob_client_revision = envelope.server_revision;
                                    bob_client_signature = envelope.server_signature;
                                    let payload: serde_json::Value =
                                        serde_json::from_slice(&envelope.payload)
                                            .map_err(|error| error.to_string())?;
                                    let payload_text = serde_json::to_string(&payload)
                                        .map_err(|error| error.to_string())?;
                                    if payload_text.contains("takes")
                                        && payload_text.contains("coin")
                                    {
                                        bob_delta = Some(envelope);
                                        break;
                                    }
                                }
                                Err(error) => last_bob_error = Some(error),
                            }
                        }
                        let bob_delta = bob_delta.ok_or_else(|| {
                            format!(
                                "bob command delta did not include transfer event{}",
                                last_bob_error
                                    .map(|error| format!(": {error}"))
                                    .unwrap_or_default()
                            )
                        })?;
                        assert_eq!(bob_login.server_revision, 1);
                        bob.send_datagram(dom_event_payload_json(&DomEventPayload {
                            session_id: 202,
                            view_id: 21,
                            revision: bob_delta.server_revision,
                            signature: bob_delta.server_signature,
                            event: "submit".to_owned(),
                            target: "event-inspect-alice".to_owned(),
                            action: "mud_command".to_owned(),
                            fields: BTreeMap::from([
                                ("text".to_owned(), "look Alice".to_owned()),
                                ("entity".to_owned(), "Alice".to_owned()),
                            ]),
                        }))
                        .map_err(|error| error.to_string())?;
                        let mut bob_inspect_delta =
                            receive_newer_sync_envelope(&bob, bob_delta.server_revision)
                                .await
                                .map_err(|error| format!("bob inspect delta: {error}"))?;
                        for _ in 0..6 {
                            let payload: serde_json::Value =
                                serde_json::from_slice(&bob_inspect_delta.payload)
                                    .map_err(|error| error.to_string())?;
                            let payload_text = serde_json::to_string(&payload)
                                .map_err(|error| error.to_string())?;
                            if payload_text.contains("Alice is alert and ready")
                                && payload_text.contains("entity-facts")
                            {
                                break;
                            }
                            bob_inspect_delta =
                                receive_newer_sync_envelope(&bob, bob_delta.server_revision)
                                    .await
                                    .map_err(|error| format!("bob inspect delta: {error}"))?;
                        }
                        alice.close(0u32.into(), b"test complete");
                        bob.close(0u32.into(), b"test complete");
                        Ok((bob_delta, bob_inspect_delta))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_mud_suspended_command_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<MudDelayedEventResult>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 21,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection).await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: snapshot.server_revision,
                                signature: snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "mud-login-alice".to_owned(),
                                action: "mud_login".to_owned(),
                                fields: BTreeMap::from([("text".to_owned(), "alice".to_owned())]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let login_delta =
                            receive_newer_sync_envelope(&connection, snapshot.server_revision)
                                .await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 21,
                                revision: login_delta.server_revision,
                                signature: login_delta.server_signature,
                                event: "submit".to_owned(),
                                target: "push-red-button".to_owned(),
                                action: "mud_command".to_owned(),
                                fields: BTreeMap::from([(
                                    "text".to_owned(),
                                    "push button".to_owned(),
                                )]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let push_delta =
                            receive_newer_sync_envelope(&connection, login_delta.server_revision)
                                .await?;

                        let mut delayed_delta =
                            receive_newer_sync_envelope(&connection, push_delta.server_revision)
                                .await?;
                        for _ in 0..6 {
                            let payload: serde_json::Value =
                                serde_json::from_slice(&delayed_delta.payload)
                                    .map_err(|error| error.to_string())?;
                            if serde_json::to_string(&payload)
                                .map_err(|error| error.to_string())?
                                .contains("cheerful ding")
                            {
                                break;
                            }
                            delayed_delta = receive_newer_sync_envelope(
                                &connection,
                                push_delta.server_revision,
                            )
                            .await?;
                        }
                        connection.close(0u32.into(), b"test complete");

                        Ok((snapshot, login_delta, push_delta, delayed_delta))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_stale_event_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<Result<SyncEnvelope, String>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 11,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection).await?;
                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 11,
                                revision: snapshot.server_revision,
                                signature: 999,
                                event: "submit".to_owned(),
                                target: "chat-composer".to_owned(),
                                action: "chat_post".to_owned(),
                                fields: BTreeMap::from([
                                    ("actor".to_owned(), "bob".to_owned()),
                                    ("text".to_owned(), "stale".to_owned()),
                                ]),
                            }))
                            .map_err(|error| error.to_string())?;
                        receive_newer_sync_envelope(&connection, snapshot.server_revision).await
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    fn spawn_wtransport_ack_client(
        server_addr: SocketAddr,
        connected_tx: mpsc::Sender<()>,
        send_rx: mpsc::Receiver<()>,
        result_tx: mpsc::Sender<Result<(SyncEnvelope, SyncEnvelope), String>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    runtime.block_on(async move {
                        let config = wtransport::ClientConfig::builder()
                            .with_bind_default()
                            .with_no_cert_validation()
                            .build();
                        let url = format!("https://127.0.0.1:{}/view", server_addr.port());
                        let connection = wtransport::Endpoint::client(config)
                            .map_err(|error| error.to_string())?
                            .connect(&url)
                            .await
                            .map_err(|error| error.to_string())?;
                        connected_tx.send(()).map_err(|error| error.to_string())?;
                        send_rx.recv().map_err(|error| error.to_string())?;

                        let need_view = SyncEnvelope {
                            kind: SyncMessageKind::NeedView,
                            session_id: 7,
                            view_id: 11,
                            client_revision: 0,
                            client_signature: 0,
                            server_revision: 0,
                            server_signature: 0,
                            payload: b"need".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(need_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        let snapshot = receive_sync_envelope(&connection).await?;

                        connection
                            .send_datagram(dom_event_payload_json(&DomEventPayload {
                                session_id: 7,
                                view_id: 11,
                                revision: snapshot.server_revision,
                                signature: snapshot.server_signature,
                                event: "submit".to_owned(),
                                target: "chat-composer".to_owned(),
                                action: "chat_post".to_owned(),
                                fields: BTreeMap::from([
                                    ("actor".to_owned(), "bob".to_owned()),
                                    ("text".to_owned(), "ack check".to_owned()),
                                ]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let delta =
                            receive_newer_sync_envelope(&connection, snapshot.server_revision)
                                .await?;

                        let have_view = SyncEnvelope {
                            kind: SyncMessageKind::HaveView,
                            session_id: 7,
                            view_id: 11,
                            client_revision: delta.server_revision,
                            client_signature: delta.server_signature,
                            server_revision: delta.server_revision,
                            server_signature: delta.server_signature,
                            payload: b"have".to_vec(),
                        };
                        connection
                            .send_datagram(encoded_sync_envelope(have_view.as_ref()))
                            .map_err(|error| error.to_string())?;
                        expect_no_newer_sync_envelope(
                            &connection,
                            delta.server_revision,
                            Duration::from_millis(200),
                        )
                        .await?;
                        Ok((snapshot, delta))
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    async fn receive_sync_envelope(
        connection: &wtransport::Connection,
    ) -> Result<SyncEnvelope, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        receive_sync_envelope_until(connection, deadline).await
    }

    async fn receive_newer_sync_envelope(
        connection: &wtransport::Connection,
        current_revision: u64,
    ) -> Result<SyncEnvelope, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let envelope = receive_sync_envelope_until(connection, deadline).await?;
            if envelope.server_revision > current_revision {
                return Ok(envelope);
            }
        }
    }

    async fn expect_no_newer_sync_envelope(
        connection: &wtransport::Connection,
        current_revision: u64,
        duration: Duration,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            match receive_sync_envelope_until(connection, deadline).await {
                Ok(envelope) if envelope.server_revision > current_revision => {
                    return Err(format!("unexpected newer sync envelope: {envelope:?}"));
                }
                Ok(_) => {}
                Err(error) if error.starts_with("timed out waiting") => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    async fn receive_sync_envelope_until(
        connection: &wtransport::Connection,
        deadline: tokio::time::Instant,
    ) -> Result<SyncEnvelope, String> {
        let mut chunks: TestChunkMap = HashMap::new();
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err("timed out waiting for WebTransport datagram".to_owned());
            }
            let payload = tokio::select! {
                datagram = tokio::time::timeout_at(deadline, connection.receive_datagram()) => {
                    datagram
                        .map_err(|_| "timed out waiting for WebTransport datagram".to_owned())?
                        .map_err(|error| error.to_string())?
                        .payload()
                }
                stream = tokio::time::timeout_at(deadline, connection.accept_uni()) => {
                    let mut stream = stream
                        .map_err(|_| "timed out waiting for WebTransport stream".to_owned())?
                        .map_err(|error| error.to_string())?;
                    let mut payload = Vec::new();
                    stream
                        .read_to_end(&mut payload)
                        .await
                        .map_err(|error| error.to_string())?;
                    Bytes::from(payload)
                }
            };
            if !payload.starts_with(SYNC_CHUNK_MAGIC) {
                match decode_sync_envelope(&payload) {
                    Ok(envelope) => return Ok(envelope),
                    Err(_) => continue,
                }
            }
            if payload.len() < SYNC_CHUNK_HEADER_LEN {
                return Err("short sync chunk datagram".to_owned());
            }
            let message_id =
                u32::from_le_bytes(payload[4..8].try_into().map_err(|_| "bad chunk id")?);
            let index =
                u32::from_le_bytes(payload[8..12].try_into().map_err(|_| "bad chunk index")?);
            let count =
                u32::from_le_bytes(payload[12..16].try_into().map_err(|_| "bad chunk count")?);
            let total_len =
                u32::from_le_bytes(payload[16..20].try_into().map_err(|_| "bad chunk len")?);
            let chunk_len =
                u32::from_le_bytes(payload[20..24].try_into().map_err(|_| "bad chunk size")?);
            if count == 0
                || index >= count
                || chunk_len as usize > payload.len() - SYNC_CHUNK_HEADER_LEN
            {
                return Err("invalid sync chunk datagram".to_owned());
            }
            let entry = chunks.entry(message_id).or_insert_with(|| {
                (
                    count,
                    total_len,
                    vec![None; usize::try_from(count).unwrap_or(0)],
                )
            });
            if entry.0 != count || entry.1 != total_len {
                return Err("inconsistent sync chunk datagram".to_owned());
            }
            entry.2[index as usize] = Some(
                payload[SYNC_CHUNK_HEADER_LEN..SYNC_CHUNK_HEADER_LEN + chunk_len as usize].to_vec(),
            );
            if entry.2.iter().all(Option::is_some) {
                let mut encoded = Vec::with_capacity(total_len as usize);
                for part in &entry.2 {
                    encoded.extend_from_slice(part.as_ref().unwrap());
                }
                if encoded.len() != total_len as usize {
                    return Err("sync chunk length mismatch".to_owned());
                }
                return decode_sync_envelope(&encoded).map_err(|error| error.to_string());
            }
        }
    }

    async fn send_dom_event_stream(
        connection: &wtransport::Connection,
        event: DomEventPayload,
    ) -> Result<(), String> {
        let envelope = SyncEnvelope {
            kind: SyncMessageKind::HaveView,
            session_id: event.session_id,
            view_id: event.view_id,
            client_revision: event.revision,
            client_signature: event.signature,
            server_revision: event.revision,
            server_signature: event.signature,
            payload: dom_event_payload_json(&event),
        };
        let mut stream = connection
            .open_uni()
            .await
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())?;
        stream
            .write_all(&encoded_sync_envelope(envelope.as_ref()))
            .await
            .map_err(|error| error.to_string())?;
        stream.finish().await.map_err(|error| error.to_string())
    }

    async fn wait_for_client_connected(receiver: &mpsc::Receiver<()>) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match receiver.try_recv() {
                Ok(()) => return,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => panic!("timed out waiting for client connect"),
                Err(mpsc::TryRecvError::Disconnected) => {
                    panic!("client disconnected before connect")
                }
            }
        }
    }

    async fn wait_for_client_result(
        receiver: &mpsc::Receiver<Result<Vec<u8>, String>>,
    ) -> Result<Vec<u8>, String> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_snapshot_client_result(
        receiver: &mpsc::Receiver<Result<SyncEnvelope, String>>,
    ) -> Result<SyncEnvelope, String> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_ack_client_result(
        receiver: &mpsc::Receiver<Result<(SyncEnvelope, SyncEnvelope), String>>,
    ) -> Result<(SyncEnvelope, SyncEnvelope), String> {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_mud_client_result(
        receiver: &mpsc::Receiver<MudClientResult>,
    ) -> MudClientResult {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_mud_login_client_result(
        receiver: &mpsc::Receiver<MudLoginResult>,
    ) -> Result<(SyncEnvelope, SyncEnvelope), String> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_mud_delayed_event_result(
        receiver: &mpsc::Receiver<MudDelayedEventResult>,
    ) -> Result<(SyncEnvelope, SyncEnvelope, SyncEnvelope, SyncEnvelope), String> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }

    async fn wait_for_mud_two_session_result(
        receiver: &mpsc::Receiver<MudTwoSessionResult>,
    ) -> Result<(SyncEnvelope, SyncEnvelope), String> {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            match receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return Err("timed out waiting for client result".to_owned());
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("client result channel disconnected".to_owned());
                }
            }
        }
    }
}
