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

use bytes::Bytes;
use compio::runtime::ResumeUnwind;
use compio_quic::{Endpoint, ServerBuilder};
use h3_webtransport::server::WebTransportSession;
use mica_driver::{CompioTaskDriver, DriverEvent};
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
use std::future::Future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub const DEFAULT_BIND: &str = "127.0.0.1:4433";
pub const DAEMON_ENDPOINT_ID_START: u64 = 0x00ea_0000_0000_0000;

const ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS: usize = 128;
const ENDPOINT_OUTPUT_DRAIN_DATAGRAMS: usize = 64;
const SYNC_DATAGRAM_MAX_LEN: usize = 1024;
const SYNC_CHUNK_HEADER_LEN: usize = 24;
const SYNC_CHUNK_PAYLOAD_LEN: usize = SYNC_DATAGRAM_MAX_LEN - SYNC_CHUNK_HEADER_LEN;
const SYNC_CHUNK_MAGIC: &[u8; 4] = b"MSC1";
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
    datagrams: VecDeque<Bytes>,
    closed: bool,
    waker: Option<Waker>,
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
        if let Some(state) = self.sessions.lock().unwrap().get(&endpoint).cloned() {
            state.sync.lock().unwrap().store_rendered_view(
                session_id,
                view_id,
                rendered.revision,
                rendered.signature,
                rendered.tree.clone(),
            );
        }
    }

    fn send_sync_envelope(&self, endpoint: Identity, envelope: SyncEnvelope) -> Result<(), String> {
        let Some(state) = self.sessions.lock().unwrap().get(&endpoint).cloned() else {
            return Ok(());
        };
        for datagram in sync_envelope_datagrams(envelope.as_ref()) {
            state.output.send(datagram)?;
        }
        Ok(())
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

    fn send(&self, datagram: Bytes) -> Result<(), String> {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err("session writer is closed".to_owned());
            }
            state.datagrams.push_back(datagram);
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

    fn drain_batch(&self, max_datagrams: usize) -> Vec<Bytes> {
        let mut state = self.state.lock().unwrap();
        let count = max_datagrams.min(state.datagrams.len());
        let mut datagrams = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(datagram) = state.datagrams.pop_front() else {
                break;
            };
            datagrams.push(datagram);
        }
        datagrams
    }

    #[cfg(test)]
    fn try_recv(&self) -> Option<Bytes> {
        self.state.lock().unwrap().datagrams.pop_front()
    }
}

impl Future for SessionOutputRecv<'_> {
    type Output = SessionOutputReady;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.output.state.lock().unwrap();
        if state.datagrams.len() >= ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS {
            return Poll::Ready(SessionOutputReady::HighWater {
                buffered: state.datagrams.len(),
            });
        }
        if !state.datagrams.is_empty() {
            return Poll::Ready(SessionOutputReady::Ready {
                buffered: state.datagrams.len(),
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
        return Err(format_driver_error(error));
    }

    let writer = compio::runtime::spawn(write_datagram_loop(session.clone(), output));
    let result = read_datagram_loop(session, &host, endpoint).await;
    let _ = host.driver.close_endpoint(endpoint);
    drop_session_writer(&host, endpoint);
    let _ = writer.await.resume_unwind();
    result
}

async fn read_datagram_loop(
    session: Rc<WtSession>,
    host: &InProcessWebTransportHost,
    endpoint: Identity,
) -> Result<(), String> {
    let mut reader = session.datagram_reader();
    loop {
        let datagram = reader
            .read_datagram()
            .await
            .map_err(|error| format!("failed to read WebTransport datagram: {error}"))?;
        route_incoming_datagram(host, endpoint, datagram.into_payload()).await?;
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
        .map_err(format_driver_error)
}

async fn route_dom_event(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    event: DomEventPayload,
) -> Result<(), String> {
    let Some(active) = host.active_rendered_sync_view(endpoint, event.session_id, event.view_id)
    else {
        return send_recovery_snapshot(host, endpoint, &event).await;
    };
    if active.server_revision != event.revision || active.server_signature != event.signature {
        return send_recovery_snapshot(host, endpoint, &event).await;
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
        .map_err(format_driver_error)?;
    match submitted.outcome {
        TaskOutcome::Complete { .. } => {}
        TaskOutcome::Aborted { error, .. } => {
            return Err(format!("sync_event aborted: {error}"));
        }
        TaskOutcome::Suspended { .. } => {
            return Err("sync_event suspended".to_owned());
        }
    }
    refresh_active_sync_views(host).await
}

async fn send_recovery_snapshot(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    event: &DomEventPayload,
) -> Result<(), String> {
    let rendered = render_sync_view(host, endpoint, event.view_id).await?;
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

async fn refresh_active_sync_views(host: &InProcessWebTransportHost) -> Result<(), String> {
    for active in host.active_sync_views() {
        let rendered = render_sync_view(host, active.endpoint, active.view_id).await?;
        if rendered.revision == active.server_revision && active.last_tree.is_some() {
            continue;
        }
        let envelope = if let Some(last_tree) = &active.last_tree {
            let patches = diff_dom_nodes(last_tree, &rendered.tree);
            if patches.is_empty() {
                host.store_rendered_sync_view(
                    active.endpoint,
                    active.session_id,
                    active.view_id,
                    &rendered,
                );
                continue;
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
        host.send_sync_envelope(active.endpoint, envelope)?;
        host.store_rendered_sync_view(
            active.endpoint,
            active.session_id,
            active.view_id,
            &rendered,
        );
    }
    Ok(())
}

async fn render_sync_view(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    view_id: u64,
) -> Result<RenderedSyncView, String> {
    let revision = sync_u64_from_task_value(
        "sync_view_revision",
        submit_sync_invocation(
            host,
            endpoint,
            "sync_view_revision",
            vec![(Symbol::intern("view"), sync_u64_value(view_id))],
        )
        .await?,
    )?;
    let tree_value = submit_sync_invocation(
        host,
        endpoint,
        "sync_view_tree",
        vec![
            (Symbol::intern("view"), sync_u64_value(view_id)),
            (Symbol::intern("revision"), sync_u64_value(revision)),
        ],
    )
    .await?;
    let tree = DomNode::from_mica_value(&tree_value)
        .map_err(|error| format!("sync_view_tree returned invalid DOM tree: {error}"))?;
    let payload = snapshot_payload_json(view_id, revision, &tree);
    let signature = sync_payload_signature(revision, &payload);

    Ok(RenderedSyncView {
        revision,
        signature,
        tree,
        payload,
    })
}

async fn submit_sync_invocation(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    selector: &'static str,
    roles: Vec<(Symbol, Value)>,
) -> Result<Value, String> {
    let submitted = host
        .driver
        .submit_invocation_for_endpoint(endpoint, Symbol::intern(selector), roles)
        .await
        .map_err(format_driver_error)?;
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
    match envelope.kind {
        SyncMessageKind::HaveView => {
            let rendered = render_sync_view(host, endpoint, envelope.view_id).await?;
            if envelope.client_revision == rendered.revision
                && envelope.client_signature == rendered.signature
            {
                host.store_rendered_sync_view(
                    endpoint,
                    envelope.session_id,
                    envelope.view_id,
                    &rendered,
                );
                return Ok(());
            }
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
            .map_err(format_driver_error),
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
        for datagram in output.drain_batch(ENDPOINT_OUTPUT_DRAIN_DATAGRAMS) {
            sender
                .send_datagram(datagram)
                .map_err(|error| format!("failed to send WebTransport datagram: {error}"))?;
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
            for event in events {
                route_driver_event(&sessions, event);
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
) {
    if let DriverEvent::Effect(effect) = event {
        let Some(state) = sessions.lock().unwrap().get(&effect.target).cloned() else {
            return;
        };
        for datagram in effect_datagrams(effect.target, &effect.value) {
            let _ = state.output.send(datagram);
        }
    }
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

fn format_driver_error(error: mica_driver::DriverError) -> String {
    format!("error: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_runtime::SourceRunner;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    type TestChunkMap = HashMap<u32, (u32, u32, Vec<Option<Vec<u8>>>)>;

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
            let (snapshot, delta) = wait_for_ack_client_result(&result_rx).await.unwrap();

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
            assert!(payload_text.contains("object-card"));
            assert!(payload_text.contains("brass coin"));
            assert!(payload_text.contains("Inventory"));
            assert!(payload_text.contains("Narrative"));
        });
    }

    #[test]
    fn webtransport_stale_dom_event_returns_snapshot() {
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
            let recovery = wait_for_snapshot_client_result(&result_rx).await.unwrap();

            server_endpoint.close(0u32.into(), b"test complete");
            client.join().unwrap();
            assert_eq!(recovery.kind, SyncMessageKind::ViewSnapshot);
            assert_eq!(recovery.client_revision, 1);
            assert_eq!(recovery.client_signature, 999);
            assert_eq!(recovery.server_revision, 1);
            let payload: serde_json::Value = serde_json::from_slice(&recovery.payload).unwrap();
            assert_eq!(
                payload["root"]["children"][0]["children"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
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
            .run_filein(include_str!("../../../apps/chat/http.mica"))
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
            .run_filein(include_str!("../../../apps/mud/sync.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/http.mica"))
            .unwrap();
        runner
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
                                    ("text".to_owned(), "hello from sync event".to_owned()),
                                ]),
                            }))
                            .map_err(|error| error.to_string())?;
                        let delta = receive_sync_envelope(&connection).await?;

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
                        let delta = receive_sync_envelope(&connection).await?;

                        Ok((snapshot, delta))
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
                        receive_sync_envelope(&connection).await
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
                        let delta = receive_sync_envelope(&connection).await?;

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
                        match tokio::time::timeout(
                            Duration::from_millis(200),
                            connection.receive_datagram(),
                        )
                        .await
                        {
                            Err(_) => Ok((snapshot, delta)),
                            Ok(Ok(datagram)) => Err(format!(
                                "unexpected HaveView response: {:?}",
                                decode_sync_envelope(&datagram.payload())
                            )),
                            Ok(Err(error)) => Err(error.to_string()),
                        }
                    })
                });
            let _ = result_tx.send(result);
        })
    }

    async fn receive_sync_envelope(
        connection: &wtransport::Connection,
    ) -> Result<SyncEnvelope, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut chunks: TestChunkMap = HashMap::new();
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err("timed out waiting for WebTransport datagram".to_owned());
            }
            let datagram = tokio::time::timeout_at(deadline, connection.receive_datagram())
                .await
                .map_err(|_| "timed out waiting for WebTransport datagram".to_owned())?
                .map_err(|error| error.to_string())?;
            let payload = datagram.payload();
            if !payload.starts_with(SYNC_CHUNK_MAGIC) {
                return decode_sync_envelope(&payload).map_err(|error| error.to_string());
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
}
