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
use mica_host_protocol::{
    SyncEnvelope, SyncMessageKind, decode_sync_envelope, encoded_sync_envelope,
    sync_envelope_from_value, sync_invocation_roles, sync_invocation_selector, sync_u64_value,
};
use mica_var::{Identity, Symbol, Value};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::future::Future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub const DEFAULT_BIND: &str = "127.0.0.1:4433";
pub const DAEMON_ENDPOINT_ID_START: u64 = 0x00ea_0000_0000_0000;

const ENDPOINT_OUTPUT_HIGH_WATER_DATAGRAMS: usize = 128;
const ENDPOINT_OUTPUT_DRAIN_DATAGRAMS: usize = 64;

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
    sessions: HashMap<u64, HashSet<u64>>,
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
struct ChatPostAction {
    room: u64,
    text: String,
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
        self.sessions
            .entry(envelope.session_id)
            .or_default()
            .insert(envelope.view_id);
    }

    #[cfg(test)]
    fn has_active_view(&self, session_id: u64, view_id: u64) -> bool {
        self.sessions
            .get(&session_id)
            .is_some_and(|views| views.contains(&view_id))
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
    if let Some(action) = decode_chat_post_action(&datagram)? {
        return host
            .driver
            .submit_invocation_for_endpoint(
                endpoint,
                Symbol::intern("chat_post"),
                vec![
                    (Symbol::intern("room"), sync_u64_value(action.room)),
                    (Symbol::intern("text"), Value::string(action.text)),
                ],
            )
            .await
            .map(|_| ())
            .map_err(format_driver_error);
    }

    host.driver
        .input(endpoint, Value::bytes(datagram))
        .await
        .map(|_| ())
        .map_err(format_driver_error)
}

fn decode_chat_post_action(datagram: &[u8]) -> Result<Option<ChatPostAction>, String> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(datagram) else {
        return Ok(None);
    };
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(|value| value.as_str()) != Some("chat_post") {
        return Ok(None);
    }

    let room = object
        .get("room")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| "chat_post action requires numeric room".to_owned())?;
    let text = object
        .get("text")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "chat_post action requires text".to_owned())?
        .to_owned();
    Ok(Some(ChatPostAction { room, text }))
}

async fn route_sync_envelope(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    if let Some(state) = host.sessions.lock().unwrap().get(&endpoint).cloned() {
        state.sync.lock().unwrap().record_incoming_view(&envelope);
    }
    let Some(selector) = sync_invocation_selector(envelope.kind) else {
        return host
            .driver
            .input(
                endpoint,
                Value::bytes(encoded_sync_envelope(envelope.as_ref())),
            )
            .await
            .map(|_| ())
            .map_err(format_driver_error);
    };
    host.driver
        .submit_invocation_for_endpoint(endpoint, selector, sync_invocation_roles(&envelope))
        .await
        .map(|_| ())
        .map_err(format_driver_error)
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

fn route_driver_event(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    event: DriverEvent,
) {
    if let DriverEvent::Effect(effect) = event {
        let Some(state) = sessions.lock().unwrap().get(&effect.target).cloned() else {
            return;
        };
        let _ = state
            .output
            .send(effect_datagram(effect.target, &effect.value));
    }
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
    fn decodes_chat_post_actions() {
        assert_eq!(
            decode_chat_post_action(br#"{"type":"chat_post","room":1,"text":"hello"}"#).unwrap(),
            Some(ChatPostAction {
                room: 1,
                text: "hello".to_owned(),
            })
        );
        assert_eq!(
            decode_chat_post_action(br#"{"type":"other","room":1,"text":"hello"}"#).unwrap(),
            None
        );
        assert!(decode_chat_post_action(br#"{"type":"chat_post"}"#).is_err());
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
    fn webtransport_sync_need_view_invokes_mica_and_returns_snapshot() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let tls = test_tls_config();
            let endpoint = bind_server_endpoint("127.0.0.1:0".parse().unwrap(), tls)
                .await
                .unwrap();
            let server_addr = endpoint.local_addr().unwrap();
            let server_endpoint = endpoint.clone();

            let mut runner = SourceRunner::new_empty();
            runner
                .run_filein(include_str!("../../../examples/sync-view-provider.mica"))
                .unwrap();
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
                1 + envelope.payload.len() as u64
            );
            assert_eq!(
                std::str::from_utf8(&envelope.payload).unwrap(),
                "{\"view\":11,\"revision\":1,\"root\":{\"id\":\"chat-root\",\"tag\":\"main\",\"children\":[{\"tag\":\"ul\",\"id\":\"messages\",\"children\":[{\"tag\":\"li\",\"children\":[{\"tag\":\"span\",\"class\":\"author\",\"children\":[{\"text\":\"#alice\"}]},{\"text\":\": \"},{\"text\":\"hello\"}]}]},{\"tag\":\"section\",\"id\":\"composer\",\"children\":[{\"text\":\"composer\"}]}]}}"
            );
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
}
