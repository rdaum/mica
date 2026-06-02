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

use crate::codec::{HttpRequest, HttpResponse};
use crate::metrics::{SyncEnvelopeKind, SyncRenderPhase};
use crate::response::internal_error_response;
use crate::{InProcessWebHost, RequestBinding, format_driver_error};
use compio::io::AsyncWriteExt;
use compio::net::TcpStream;
use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_host_protocol::{
    DomEventPayload, DomNode, SyncEnvelope, SyncMessageKind, decode_dom_event_payload,
    decode_sync_envelope, diff_dom_nodes, dom_patch_payload_json, sync_envelope_from_value,
    sync_payload_signature, sync_u64_from_value, sync_u64_value,
};
use mica_runtime::{TaskId, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use tracing;

const ENDPOINT_OUTPUT_HIGH_WATER_MESSAGES: usize = 128;
const ENDPOINT_OUTPUT_DRAIN_MESSAGES: usize = 64;
const SYNC_EVENTS_PATH: &str = "/sync/events";
const SYNC_INPUT_PATH: &str = "/sync/input";

pub(crate) struct InProcessSyncHost {
    sessions: Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    stop_events: Arc<AtomicBool>,
}

#[derive(Debug)]
struct SyncSession {
    session_id: u64,
    endpoint: Identity,
    actor: Option<Identity>,
    output: Arc<SessionOutput>,
    sync: Mutex<SessionSyncState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SessionSyncState {
    views: HashMap<u64, ActiveViewState>,
    pending_tasks: HashSet<TaskId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ActiveViewState {
    client_revision: u64,
    client_signature: u64,
    server_revision: u64,
    server_signature: u64,
    last_tree: Option<DomNode>,
}

#[derive(Default, Debug)]
struct SessionOutput {
    state: Mutex<SessionOutputState>,
}

#[derive(Default, Debug)]
struct SessionOutputState {
    messages: VecDeque<SessionOutputMessage>,
    closed: bool,
    writer_generation: u64,
    waker: Option<Waker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionOutputMessage {
    SyncEnvelope(SyncEnvelope),
}

struct SessionOutputRecv<'a> {
    output: &'a SessionOutput,
    writer_generation: u64,
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
    Replaced,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncRequestKind {
    EventStream,
    Input,
}

impl InProcessSyncHost {
    pub(crate) fn new(driver: Arc<CompioTaskDriver>) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));
        let stop_events = Arc::new(AtomicBool::new(false));
        start_event_pump(driver, sessions.clone(), stop_events.clone());
        Self {
            sessions,
            stop_events,
        }
    }
}

impl Drop for InProcessSyncHost {
    fn drop(&mut self) {
        self.stop_events.store(true, Ordering::Relaxed);
        for session in self.sessions.lock().unwrap().values() {
            session.output.close();
        }
    }
}

impl SyncSession {
    fn new(session_id: u64, endpoint: Identity, actor: Option<Identity>) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            endpoint,
            actor,
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
        let view = self.views.entry(envelope.view_id).or_default();
        view.client_revision = envelope.client_revision;
        view.client_signature = envelope.client_signature;
    }

    fn store_rendered_view(&mut self, view_id: u64, revision: u64, signature: u64, tree: DomNode) {
        let view = self.views.entry(view_id).or_default();
        view.client_revision = revision;
        view.client_signature = signature;
        view.server_revision = revision;
        view.server_signature = signature;
        view.last_tree = Some(tree);
    }
}

impl SessionOutput {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn claim_writer(&self) -> u64 {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.writer_generation = state.writer_generation.saturating_add(1).max(1);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.state.lock().unwrap().writer_generation
    }

    fn send_sync_envelope(&self, envelope: SyncEnvelope) -> Result<(), String> {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err("session writer is closed".to_owned());
            }
            state
                .messages
                .push_back(SessionOutputMessage::SyncEnvelope(envelope));
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

    fn recv(&self, writer_generation: u64) -> SessionOutputRecv<'_> {
        SessionOutputRecv {
            output: self,
            writer_generation,
        }
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
}

impl Future for SessionOutputRecv<'_> {
    type Output = SessionOutputReady;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.output.state.lock().unwrap();
        if state.writer_generation != self.writer_generation {
            return Poll::Ready(SessionOutputReady::Replaced);
        }
        if state.messages.len() >= ENDPOINT_OUTPUT_HIGH_WATER_MESSAGES {
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

pub(crate) fn request_kind(request: &HttpRequest) -> Option<SyncRequestKind> {
    if request.method == "GET" && request_path_without_query(&request.path) == SYNC_EVENTS_PATH {
        return Some(SyncRequestKind::EventStream);
    }
    if request.method == "POST" && request_path_without_query(&request.path) == SYNC_INPUT_PATH {
        return Some(SyncRequestKind::Input);
    }
    None
}

pub(crate) async fn handle_sync_input_request(
    host: &InProcessWebHost,
    binding: &RequestBinding,
    request: &HttpRequest,
    close: bool,
) -> HttpResponse {
    let actor_override = if let Some(auth) = &host.auth {
        let cookie_header = request
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("cookie"))
            .map(|h| std::str::from_utf8(&h.value).unwrap_or(""));

        match auth.resolve_auth_context(cookie_header).await {
            Ok(Some(ctx)) => match host.driver.named_identity(Symbol::intern(&ctx.actor_name)) {
                Ok(actor) => Some(actor),
                Err(error) => {
                    tracing::warn!(
                        actor_name = %ctx.actor_name,
                        error = %error,
                        "failed to resolve authenticated actor for sync"
                    );
                    return internal_error_response("Failed to resolve user identity", close);
                }
            },
            Ok(None) => {
                return HttpResponse::new(401, "Unauthorized", b"Authentication required".to_vec());
            }
            Err(error) => {
                tracing::warn!(error = %error, "sync authentication failed");
                return HttpResponse::new(
                    401,
                    "Unauthorized",
                    b"Invalid or expired session".to_vec(),
                );
            }
        }
    } else {
        binding.actor
    };

    match sync_envelope_from_request(&request.body) {
        Ok(SyncRequestEnvelope::Sync(envelope)) => {
            let session_id = envelope.session_id;
            let view_id = envelope.view_id;
            let kind = envelope.kind;
            let session = match ensure_session(host, binding, envelope.session_id, actor_override) {
                Ok(session) => session,
                Err(error) => {
                    tracing::error!(
                        target: "mica_web_host::sync",
                        session_id,
                        view_id,
                        kind = ?kind,
                        error = %error,
                        "sync input failed"
                    );
                    return internal_error_response(error, close);
                }
            };
            if let Err(error) = route_sync_envelope(host, &session, envelope).await {
                tracing::error!(
                    target: "mica_web_host::sync",
                    session_id,
                    view_id,
                    kind = ?kind,
                    error = %error,
                    "sync input failed"
                );
                return internal_error_response(error, close);
            }
        }
        Ok(SyncRequestEnvelope::DomEvent(event)) => {
            let session_id = event.session_id;
            let view_id = event.view_id;
            let event_name = event.event.clone();
            let action = event.action.clone();
            let target = event.target.clone();
            let session = match ensure_session(host, binding, event.session_id, actor_override) {
                Ok(session) => session,
                Err(error) => {
                    tracing::error!(
                        target: "mica_web_host::sync",
                        session_id,
                        view_id,
                        event = %event_name,
                        action = %action,
                        target = %target,
                        error = %error,
                        "sync DOM input failed"
                    );
                    return internal_error_response(error, close);
                }
            };
            if let Err(error) = route_dom_event(host, &session, event).await {
                tracing::error!(
                    target: "mica_web_host::sync",
                    session_id,
                    view_id,
                    event = %event_name,
                    action = %action,
                    target = %target,
                    error = %error,
                    "sync DOM input failed"
                );
                return internal_error_response(error, close);
            }
        }
        Err(error) => {
            return with_connection_header(
                HttpResponse::text(400, "Bad Request", format!("invalid sync input: {error}\n")),
                close,
            );
        }
    }
    with_connection_header(HttpResponse::new(202, "Accepted", Vec::new()), close)
}

pub(crate) async fn serve_event_stream(
    mut stream: TcpStream,
    host: Arc<InProcessWebHost>,
    binding: RequestBinding,
    request: &HttpRequest,
) -> Result<(), String> {
    let actor_override = if let Some(auth) = &host.auth {
        let cookie_header = request
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("cookie"))
            .map(|h| std::str::from_utf8(&h.value).unwrap_or(""));

        match auth.resolve_auth_context(cookie_header).await {
            Ok(Some(ctx)) => match host.driver.named_identity(Symbol::intern(&ctx.actor_name)) {
                Ok(actor) => Some(actor),
                Err(error) => {
                    tracing::warn!(
                        actor_name = %ctx.actor_name,
                        error = %error,
                        "failed to resolve authenticated actor for event stream"
                    );
                    return Err("Failed to resolve user identity".to_string());
                }
            },
            Ok(None) => return Err("Authentication required".to_string()),
            Err(error) => {
                tracing::warn!(error = %error, "event stream authentication failed");
                return Err("Invalid or expired session".to_string());
            }
        }
    } else {
        binding.actor
    };

    let session_id = session_id_from_stream_request(request)?;
    let session = ensure_session(&host, &binding, session_id, actor_override)?;
    write_event_stream_headers(&mut stream).await?;
    write_event_chunk(&mut stream, b": connected\n\n").await?;
    write_event_stream_loop(&mut stream, session.output.clone()).await
}

fn session_id_from_stream_request(request: &HttpRequest) -> Result<u64, String> {
    query_u64(&request.path, "session")
        .ok_or_else(|| "sync event stream requires ?session=<u64>".to_owned())
}

fn sync_envelope_from_request(body: &[u8]) -> Result<SyncRequestEnvelope, String> {
    if let Ok(envelope) = decode_sync_envelope(body) {
        return Ok(SyncRequestEnvelope::Sync(envelope));
    }
    if let Some(event) = decode_dom_event_payload(body)? {
        return Ok(SyncRequestEnvelope::DomEvent(event));
    }
    Err("body was neither a sync envelope nor a DOM event payload".to_owned())
}

enum SyncRequestEnvelope {
    Sync(SyncEnvelope),
    DomEvent(DomEventPayload),
}

fn ensure_session(
    host: &InProcessWebHost,
    binding: &RequestBinding,
    session_id: u64,
    actor_override: Option<Identity>,
) -> Result<Arc<SyncSession>, String> {
    let effective_actor = actor_override.or(binding.actor);
    if let Some(session) = host.sync.sessions.lock().unwrap().get(&session_id).cloned() {
        if effective_actor.is_some() && session.actor != effective_actor {
            return Err("session belongs to a different actor".to_owned());
        }
        return Ok(session);
    }

    let endpoint = host.allocate_endpoint()?;
    host.driver
        .open_endpoint_with_context(
            endpoint,
            Some(binding.principal),
            effective_actor,
            Symbol::intern("http-sync"),
        )
        .map_err(format_driver_error)?;

    let session = SyncSession::new(session_id, endpoint, effective_actor);
    let mut sessions = host.sync.sessions.lock().unwrap();
    if let Some(existing) = sessions.get(&session_id).cloned() {
        host.driver.close_endpoint(endpoint);
        if effective_actor.is_some() && existing.actor != effective_actor {
            return Err("session belongs to a different actor".to_owned());
        }
        return Ok(existing);
    }
    sessions.insert(session_id, session.clone());
    Ok(session)
}

async fn route_dom_event(
    host: &InProcessWebHost,
    session: &Arc<SyncSession>,
    event: DomEventPayload,
) -> Result<(), String> {
    let _dom_event_timer = crate::metrics::start_sync_phase(SyncRenderPhase::DomEvent);
    let trace = SyncTrace::new("dom_event");
    let Some(active) = active_rendered_sync_view(session, event.view_id) else {
        return send_recovery_snapshot(host, session, &event).await;
    };
    trace.mark("active_view");
    if active.server_revision != event.revision || active.server_signature != event.signature {
        let rendered = render_sync_view(host, session.endpoint, event.view_id).await?;
        trace.mark("stale_render");
        if event.revision > rendered.revision {
            return send_recovery_snapshot_from_rendered(session, &event, rendered).await;
        }
        store_rendered_sync_view(session, event.view_id, &rendered);
    }

    let event_name = event.event.clone();
    let action = event.action.clone();
    let target = event.target.clone();
    let submitted = host
        .driver
        .submit_invocation_for_endpoint(
            session.endpoint,
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
    trace.mark("sync_event");
    match submitted.outcome {
        TaskOutcome::Complete { .. } => {}
        TaskOutcome::Suspended { .. } => {
            session
                .sync
                .lock()
                .unwrap()
                .pending_tasks
                .insert(submitted.task_id);
        }
        TaskOutcome::Aborted { error, .. } => {
            let message = format!("sync_event aborted: {error}");
            tracing::error!(
                target: "mica_web_host::sync",
                task_id = submitted.task_id,
                session_id = event.session_id,
                view_id = event.view_id,
                event = %event_name,
                action = %action,
                target = %target,
                error = %message,
                "sync event task aborted"
            );
            return Err(message);
        }
    }
    let result =
        refresh_active_sync_views_after_dom_event(host, session.session_id, event.view_id).await;
    trace.mark("refresh");
    result
}

async fn send_recovery_snapshot(
    host: &InProcessWebHost,
    session: &Arc<SyncSession>,
    event: &DomEventPayload,
) -> Result<(), String> {
    let rendered = render_sync_view(host, session.endpoint, event.view_id).await?;
    send_recovery_snapshot_from_rendered(session, event, rendered).await
}

async fn send_recovery_snapshot_from_rendered(
    session: &Arc<SyncSession>,
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
    session.output.send_sync_envelope(envelope)?;
    store_rendered_sync_view(session, event.view_id, &rendered);
    Ok(())
}

async fn refresh_active_sync_views_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
) -> Result<(), String> {
    for active in active_sync_views(sessions) {
        refresh_active_sync_view_for(driver, sessions, active, false).await?;
    }
    Ok(())
}

async fn refresh_active_sync_views_after_dom_event(
    host: &InProcessWebHost,
    source_session_id: u64,
    source_view_id: u64,
) -> Result<(), String> {
    for active in active_sync_views(&host.sync.sessions) {
        let force_ack = active.session_id == source_session_id && active.view_id == source_view_id;
        refresh_active_sync_view_for(&host.driver, &host.sync.sessions, active, force_ack).await?;
    }
    Ok(())
}

async fn refresh_active_sync_view_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    active: ActiveSyncView,
    force_ack: bool,
) -> Result<(), String> {
    let _refresh_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Refresh);
    let revision = render_sync_revision(driver, active.endpoint, active.view_id).await?;
    if revision == active.server_revision && active.last_tree.is_some() {
        if force_ack {
            let payload = {
                let _payload_timer =
                    crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
                dom_patch_payload_json(active.view_id, active.server_revision, &[])
            };
            crate::metrics::record_sync_patch_count(0);
            crate::metrics::record_sync_envelope(SyncEnvelopeKind::Ack, payload.len());
            send_sync_envelope_to(
                sessions,
                active.session_id,
                SyncEnvelope {
                    kind: SyncMessageKind::ViewDelta,
                    session_id: active.session_id,
                    view_id: active.view_id,
                    client_revision: active.server_revision,
                    client_signature: active.server_signature,
                    server_revision: active.server_revision,
                    server_signature: active.server_signature,
                    payload,
                },
            )?;
        }
        return Ok(());
    }

    let rendered = render_sync_view_for(driver, active.endpoint, active.view_id).await?;
    let envelope = if let Some(last_tree) = &active.last_tree {
        let patches = {
            let _diff_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Diff);
            diff_dom_nodes(last_tree, &rendered.tree)
        };
        crate::metrics::record_sync_patch_count(patches.len());
        if patches.is_empty() {
            if force_ack {
                let payload = {
                    let _payload_timer =
                        crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
                    dom_patch_payload_json(active.view_id, rendered.revision, &[])
                };
                crate::metrics::record_sync_envelope(SyncEnvelopeKind::Ack, payload.len());
                send_sync_envelope_to(
                    sessions,
                    active.session_id,
                    SyncEnvelope {
                        kind: SyncMessageKind::ViewDelta,
                        session_id: active.session_id,
                        view_id: active.view_id,
                        client_revision: active.server_revision,
                        client_signature: active.server_signature,
                        server_revision: rendered.revision,
                        server_signature: rendered.signature,
                        payload,
                    },
                )?;
            }
            store_rendered_sync_view_in(sessions, active.session_id, active.view_id, &rendered);
            return Ok(());
        }
        let payload = {
            let _payload_timer = crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
            dom_patch_payload_json(active.view_id, rendered.revision, &patches)
        };
        crate::metrics::record_sync_envelope(SyncEnvelopeKind::Delta, payload.len());
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
    send_sync_envelope_to(sessions, active.session_id, envelope)?;
    store_rendered_sync_view_in(sessions, active.session_id, active.view_id, &rendered);
    Ok(())
}

async fn render_sync_view(
    host: &InProcessWebHost,
    endpoint: Identity,
    view_id: u64,
) -> Result<RenderedSyncView, String> {
    render_sync_view_for(&host.driver, endpoint, view_id).await
}

async fn render_sync_revision(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    view_id: u64,
) -> Result<u64, String> {
    let _revision_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Revision);
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
    let revision = render_sync_revision(driver, endpoint, view_id).await?;
    trace.mark("revision");
    let tree_value = {
        let _tree_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Tree);
        submit_sync_invocation_for(
            driver,
            endpoint,
            "sync_view_tree",
            vec![
                (Symbol::intern("view"), sync_u64_value(view_id)),
                (Symbol::intern("revision"), sync_u64_value(revision)),
            ],
        )
        .await?
    };
    trace.mark("tree");
    let tree = {
        let _decode_timer = crate::metrics::start_sync_phase(SyncRenderPhase::DecodeTree);
        DomNode::from_mica_value(&tree_value)
            .map_err(|error| format!("sync_view_tree returned invalid DOM tree: {error}"))?
    };
    crate::metrics::record_sync_dom_nodes(tree.node_count());
    trace.mark("decode_tree");
    let payload = {
        let _payload_timer = crate::metrics::start_sync_phase(SyncRenderPhase::SnapshotPayload);
        mica_host_protocol::snapshot_payload_json(view_id, revision, &tree)
    };
    let signature = sync_payload_signature(revision, &payload);
    crate::metrics::record_sync_envelope(SyncEnvelopeKind::Snapshot, payload.len());
    trace.mark("payload");

    Ok(RenderedSyncView {
        revision,
        signature,
        tree,
        payload,
    })
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

fn active_rendered_sync_view(session: &Arc<SyncSession>, view_id: u64) -> Option<ActiveViewState> {
    session.sync.lock().unwrap().views.get(&view_id).cloned()
}

fn store_rendered_sync_view(session: &Arc<SyncSession>, view_id: u64, rendered: &RenderedSyncView) {
    let _store_timer = crate::metrics::start_sync_phase(SyncRenderPhase::StoreRendered);
    session.sync.lock().unwrap().store_rendered_view(
        view_id,
        rendered.revision,
        rendered.signature,
        rendered.tree.clone(),
    );
}

fn store_rendered_sync_view_in(
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    session_id: u64,
    view_id: u64,
    rendered: &RenderedSyncView,
) {
    if let Some(session) = sessions.lock().unwrap().get(&session_id).cloned() {
        store_rendered_sync_view(&session, view_id, rendered);
    }
}

fn send_sync_envelope_to(
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    session_id: u64,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    let Some(session) = sessions.lock().unwrap().get(&session_id).cloned() else {
        return Ok(());
    };
    let _send_timer = crate::metrics::start_sync_phase(SyncRenderPhase::SendEnvelope);
    let result = session.output.send_sync_envelope(envelope);
    result
}

fn sync_u64_from_task_value(selector: &str, value: Value) -> Result<u64, String> {
    sync_u64_from_value(&value).ok_or_else(|| format!("{selector} returned non-u64 value: {value}"))
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
    host: &InProcessWebHost,
    session: &Arc<SyncSession>,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    session.sync.lock().unwrap().record_incoming_view(&envelope);
    if let Some(event) = decode_dom_event_payload(&envelope.payload)? {
        return route_dom_event(host, session, event).await;
    }
    match envelope.kind {
        SyncMessageKind::HaveView => {
            let revision =
                render_sync_revision(&host.driver, session.endpoint, envelope.view_id).await?;
            if envelope.client_revision == revision
                && let Some(active) = active_rendered_sync_view(session, envelope.view_id)
                && active.server_revision == revision
                && active.server_signature == envelope.client_signature
                && active.last_tree.is_some()
            {
                return Ok(());
            }
            let rendered = render_sync_view(host, session.endpoint, envelope.view_id).await?;
            let response = active_rendered_sync_view(session, envelope.view_id)
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
            session.output.send_sync_envelope(response)?;
            store_rendered_sync_view(session, envelope.view_id, &rendered);
            Ok(())
        }
        SyncMessageKind::NeedView => {
            let rendered = render_sync_view(host, session.endpoint, envelope.view_id).await?;
            let response = snapshot_envelope(
                envelope.session_id,
                envelope.view_id,
                envelope.client_revision,
                envelope.client_signature,
                &rendered,
            );
            session.output.send_sync_envelope(response)?;
            store_rendered_sync_view(session, envelope.view_id, &rendered);
            Ok(())
        }
        SyncMessageKind::ViewSnapshot | SyncMessageKind::ViewDelta => host
            .driver
            .input(
                session.endpoint,
                Value::bytes(mica_host_protocol::encoded_sync_envelope(envelope.as_ref())),
            )
            .await
            .map(|_| ())
            .map_err(format_driver_error),
    }
}

async fn write_event_stream_headers(stream: &mut TcpStream) -> Result<(), String> {
    let response = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/event-stream; charset=utf-8\r\n",
        "Cache-Control: no-store\r\n",
        "Connection: keep-alive\r\n",
        "Transfer-Encoding: chunked\r\n",
        "X-Accel-Buffering: no\r\n",
        "\r\n"
    );
    let (result, _) = stream.write_all(response.as_bytes()).await.into();
    result.map_err(|error| format!("failed to write sync event stream headers: {error}"))
}

async fn write_event_stream_loop(
    stream: &mut TcpStream,
    output: Arc<SessionOutput>,
) -> Result<(), String> {
    let writer_generation = output.claim_writer();
    while let SessionOutputReady::Ready { .. } | SessionOutputReady::HighWater { .. } =
        output.recv(writer_generation).await
    {
        for message in output.drain_batch(ENDPOINT_OUTPUT_DRAIN_MESSAGES) {
            let payload = match message {
                SessionOutputMessage::SyncEnvelope(envelope) => sync_sse_payload(&envelope),
            };
            write_event_chunk(stream, payload.as_bytes()).await?;
        }
    }
    Ok(())
}

async fn write_event_chunk(stream: &mut TcpStream, payload: &[u8]) -> Result<(), String> {
    let prefix = format!("{:X}\r\n", payload.len());
    let mut chunk = Vec::with_capacity(prefix.len() + payload.len() + 2);
    chunk.extend_from_slice(prefix.as_bytes());
    chunk.extend_from_slice(payload);
    chunk.extend_from_slice(b"\r\n");
    let (result, _) = stream.write_all(chunk).await.into();
    result.map_err(|error| format!("failed to write sync event chunk: {error}"))
}

fn sync_sse_payload(envelope: &SyncEnvelope) -> String {
    let data = serde_json::json!({
        "kind": sync_kind_name(envelope.kind),
        "session": envelope.session_id.to_string(),
        "view": envelope.view_id.to_string(),
        "clientRevision": envelope.client_revision.to_string(),
        "clientSignature": envelope.client_signature.to_string(),
        "serverRevision": envelope.server_revision.to_string(),
        "serverSignature": envelope.server_signature.to_string(),
        "payload": String::from_utf8_lossy(&envelope.payload),
    });
    format!("event: sync\ndata: {data}\n\n")
}

fn sync_kind_name(kind: SyncMessageKind) -> &'static str {
    match kind {
        SyncMessageKind::HaveView => "HaveView",
        SyncMessageKind::NeedView => "NeedView",
        SyncMessageKind::ViewSnapshot => "ViewSnapshot",
        SyncMessageKind::ViewDelta => "ViewDelta",
    }
}

fn start_event_pump(
    driver: Arc<CompioTaskDriver>,
    sessions: Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
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
                tracing::warn!(error = %error, "failed to refresh active HTTP sync views");
            }
        }
    })
    .detach();
}

fn active_sync_views(sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>) -> Vec<ActiveSyncView> {
    let sessions = sessions
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut active = Vec::new();
    for session in sessions {
        let sync = session.sync.lock().unwrap();
        for (view_id, view_state) in &sync.views {
            active.push(ActiveSyncView {
                endpoint: session.endpoint,
                session_id: session.session_id,
                view_id: *view_id,
                client_revision: view_state.client_revision,
                client_signature: view_state.client_signature,
                server_revision: view_state.server_revision,
                server_signature: view_state.server_signature,
                last_tree: view_state.last_tree.clone(),
            });
        }
    }
    active
}

fn route_driver_event(
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    event: DriverEvent,
) -> bool {
    match event {
        DriverEvent::Effect(effect) => {
            let session = sessions
                .lock()
                .unwrap()
                .values()
                .find(|session| session.endpoint == effect.target)
                .cloned();
            let Some(session) = session else {
                return false;
            };
            if let Some(envelope) = sync_envelope_from_value(session.session_id, &effect.value) {
                let _ = session.output.send_sync_envelope(envelope);
            }
            true
        }
        DriverEvent::TaskCompleted { task_id, .. } => complete_pending_sync_task(sessions, task_id),
        DriverEvent::TaskAborted { task_id, .. } | DriverEvent::TaskFailed { task_id, .. } => {
            complete_pending_sync_task(sessions, task_id)
        }
        DriverEvent::TaskSuspended { .. } => false,
    }
}

fn complete_pending_sync_task(
    sessions: &Arc<Mutex<HashMap<u64, Arc<SyncSession>>>>,
    task_id: TaskId,
) -> bool {
    let sessions = sessions
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for session in sessions {
        let mut sync = session.sync.lock().unwrap();
        if sync.pending_tasks.remove(&task_id) {
            return true;
        }
    }
    false
}

fn request_path_without_query(path: &str) -> &str {
    path.split_once('?').map(|(path, _)| path).unwrap_or(path)
}

fn query_u64(path: &str, name: &str) -> Option<u64> {
    let (_, query) = path.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        if key != name {
            return None;
        }
        value.parse::<u64>().ok()
    })
}

fn with_connection_header(response: HttpResponse, close: bool) -> HttpResponse {
    if close {
        response.with_header("Connection", b"close".as_slice())
    } else {
        response.with_header("Connection", b"keep-alive".as_slice())
    }
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
            enabled: tracing::enabled!(target: "mica_web_host::sync", tracing::Level::TRACE),
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
        tracing::trace!(
            target: "mica_web_host::sync",
            label = self.label,
            phase,
            elapsed_us = now.duration_since(*last).as_micros(),
            total_us = now.duration_since(self.start).as_micros(),
            "HTTP sync phase completed"
        );
        *last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::serve_in_process;
    use compio::net::TcpListener;
    use compio::runtime::Runtime;
    use mica_driver::CompioTaskDriver;
    use mica_host_protocol::{dom_event_payload_json, encoded_sync_envelope};
    use mica_runtime::SourceRunner;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{SocketAddr, TcpStream as StdTcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn sse_sync_session_persists_mud_login_and_command() {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            Runtime::new().unwrap().block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let runner = sync_mud_runner();
                let principal = runner.named_identity(Symbol::intern("web")).unwrap();
                let driver = CompioTaskDriver::spawn(runner).unwrap();
                let host = InProcessWebHost::new(driver);
                let binding = RequestBinding {
                    principal,
                    actor: None,
                };
                addr_tx.send(addr).unwrap();
                compio::runtime::spawn(async move {
                    if let Err(error) = serve_in_process(listener, host, binding, None).await {
                        tracing::warn!(error = %error, "test web host stopped");
                    }
                })
                .detach();
                while stop_rx.try_recv().is_err() {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });

        let addr = addr_rx.recv().unwrap();
        let result = run_mud_sse_client(addr);
        stop_tx.send(()).unwrap();
        server.join().unwrap();
        result.unwrap();
    }

    #[test]
    fn sse_noop_dom_event_sends_same_revision_ack() {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            Runtime::new().unwrap().block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let runner = sync_mud_runner();
                let principal = runner.named_identity(Symbol::intern("web")).unwrap();
                let driver = CompioTaskDriver::spawn(runner).unwrap();
                let host = InProcessWebHost::new(driver);
                let binding = RequestBinding {
                    principal,
                    actor: None,
                };
                addr_tx.send(addr).unwrap();
                compio::runtime::spawn(async move {
                    if let Err(error) = serve_in_process(listener, host, binding, None).await {
                        tracing::warn!(error = %error, "test web host stopped");
                    }
                })
                .detach();
                while stop_rx.try_recv().is_err() {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });

        let addr = addr_rx.recv().unwrap();
        let result = run_noop_sse_client(addr);
        stop_tx.send(()).unwrap();
        server.join().unwrap();
        result.unwrap();
    }

    #[test]
    fn sse_pending_dom_event_finishes_when_spawn_parent_completes() {
        let endpoint = Identity::new(0x00ee_0000_0000_0100).unwrap();
        let session = SyncSession::new(7, endpoint, None);
        session.sync.lock().unwrap().pending_tasks.insert(11);
        let sessions = Arc::new(Mutex::new(HashMap::from([(7u64, session.clone())])));

        let routed = route_driver_event(
            &sessions,
            DriverEvent::TaskCompleted {
                task_id: 11,
                value: Value::int(22).unwrap(),
            },
        );

        assert!(routed);
        assert!(session.sync.lock().unwrap().pending_tasks.is_empty());
    }

    #[test]
    fn sse_mud_pushes_alice_command_to_bob_view() {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            Runtime::new().unwrap().block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let runner = sync_mud_runner();
                let principal = runner.named_identity(Symbol::intern("web")).unwrap();
                let driver = CompioTaskDriver::spawn(runner).unwrap();
                let host = InProcessWebHost::new(driver);
                let binding = RequestBinding {
                    principal,
                    actor: None,
                };
                addr_tx.send(addr).unwrap();
                compio::runtime::spawn(async move {
                    if let Err(error) = serve_in_process(listener, host, binding, None).await {
                        tracing::warn!(error = %error, "test web host stopped");
                    }
                })
                .detach();
                while stop_rx.try_recv().is_err() {
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        });

        let addr = addr_rx.recv().unwrap();
        let result = run_mud_sse_two_session_client(addr);
        stop_tx.send(()).unwrap();
        server.join().unwrap();
        result.unwrap();
    }

    fn run_mud_sse_client(addr: SocketAddr) -> Result<(), String> {
        let session_id = 7u64;
        let mut stream = open_event_stream(addr, session_id)?;

        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::NeedView,
                    session_id,
                    view_id: 21,
                    client_revision: 0,
                    client_signature: 0,
                    server_revision: 0,
                    server_signature: 0,
                    payload: b"need".to_vec(),
                }
                .as_ref(),
            ),
        )?;
        let snapshot = read_next_sync_envelope(&mut stream)?;
        if snapshot.kind != SyncMessageKind::ViewSnapshot {
            return Err(format!("expected snapshot, got {:?}", snapshot.kind));
        }
        let snapshot_payload: serde_json::Value = serde_json::from_slice(&snapshot.payload)
            .map_err(|error| format!("failed to parse snapshot payload: {error}"))?;
        let snapshot_text = serde_json::to_string(&snapshot_payload).unwrap();
        if !snapshot_text.contains("Enter as Alice") || !snapshot_text.contains("Enter as Bob") {
            return Err("snapshot did not contain the MUD login view".to_owned());
        }

        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::HaveView,
                    session_id,
                    view_id: 21,
                    client_revision: snapshot.server_revision,
                    client_signature: snapshot.server_signature,
                    server_revision: snapshot.server_revision,
                    server_signature: snapshot.server_signature,
                    payload: dom_event_payload_json(&DomEventPayload {
                        session_id,
                        view_id: 21,
                        revision: snapshot.server_revision,
                        signature: snapshot.server_signature,
                        event: "submit".to_owned(),
                        target: "mud-login-alice".to_owned(),
                        action: "mud_login".to_owned(),
                        fields: BTreeMap::from([("text".to_owned(), "alice".to_owned())]),
                    }),
                }
                .as_ref(),
            ),
        )?;
        let login_delta = read_next_sync_envelope(&mut stream)?;
        if login_delta.kind != SyncMessageKind::ViewDelta {
            return Err(format!("expected login delta, got {:?}", login_delta.kind));
        }
        let login_payload: serde_json::Value = serde_json::from_slice(&login_delta.payload)
            .map_err(|error| format!("failed to parse login delta payload: {error}"))?;
        let login_text = serde_json::to_string(&login_payload).unwrap();
        if !login_text.contains("mud-shell") || !login_text.contains("The Mica Rooms") {
            return Err("login delta did not render the world view".to_owned());
        }

        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::HaveView,
                    session_id,
                    view_id: 21,
                    client_revision: login_delta.server_revision,
                    client_signature: login_delta.server_signature,
                    server_revision: login_delta.server_revision,
                    server_signature: login_delta.server_signature,
                    payload: dom_event_payload_json(&DomEventPayload {
                        session_id,
                        view_id: 21,
                        revision: login_delta.server_revision,
                        signature: login_delta.server_signature,
                        event: "submit".to_owned(),
                        target: "mud-command".to_owned(),
                        action: "mud_command".to_owned(),
                        fields: BTreeMap::from([("text".to_owned(), "look coin".to_owned())]),
                    }),
                }
                .as_ref(),
            ),
        )?;
        let command_delta = read_next_sync_envelope(&mut stream)?;
        if command_delta.kind != SyncMessageKind::ViewDelta {
            return Err(format!(
                "expected command delta, got {:?}",
                command_delta.kind
            ));
        }
        if command_delta.server_revision <= login_delta.server_revision {
            return Err("command delta did not advance the server revision".to_owned());
        }
        let command_payload: serde_json::Value = serde_json::from_slice(&command_delta.payload)
            .map_err(|error| format!("failed to parse command delta payload: {error}"))?;
        let command_text = serde_json::to_string(&command_payload).unwrap();
        if !command_text.contains("coin") || !command_text.contains("event-line") {
            return Err("command delta did not include the narrative update".to_owned());
        }
        Ok(())
    }

    fn run_noop_sse_client(addr: SocketAddr) -> Result<(), String> {
        let session_id = 7u64;
        let mut stream = open_event_stream(addr, session_id)?;

        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::NeedView,
                    session_id,
                    view_id: 21,
                    client_revision: 0,
                    client_signature: 0,
                    server_revision: 0,
                    server_signature: 0,
                    payload: b"need".to_vec(),
                }
                .as_ref(),
            ),
        )?;
        let snapshot = read_next_sync_envelope(&mut stream)?;
        if snapshot.kind != SyncMessageKind::ViewSnapshot {
            return Err(format!("expected snapshot, got {:?}", snapshot.kind));
        }

        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::HaveView,
                    session_id,
                    view_id: 21,
                    client_revision: snapshot.server_revision,
                    client_signature: snapshot.server_signature,
                    server_revision: snapshot.server_revision,
                    server_signature: snapshot.server_signature,
                    payload: dom_event_payload_json(&DomEventPayload {
                        session_id,
                        view_id: 21,
                        revision: snapshot.server_revision,
                        signature: snapshot.server_signature,
                        event: "submit".to_owned(),
                        target: "noop".to_owned(),
                        action: "does_not_exist".to_owned(),
                        fields: BTreeMap::new(),
                    }),
                }
                .as_ref(),
            ),
        )?;
        let ack = read_next_sync_envelope(&mut stream)?;
        if ack.kind != SyncMessageKind::ViewDelta {
            return Err(format!("expected no-op delta ack, got {:?}", ack.kind));
        }
        if ack.server_revision != snapshot.server_revision
            || ack.server_signature != snapshot.server_signature
        {
            return Err("no-op delta ack did not preserve rendered revision".to_owned());
        }
        let payload: serde_json::Value = serde_json::from_slice(&ack.payload)
            .map_err(|error| format!("failed to parse no-op delta payload: {error}"))?;
        if payload["patches"] != serde_json::json!([]) {
            return Err(format!(
                "expected empty patches, got {}",
                payload["patches"]
            ));
        }
        Ok(())
    }

    fn run_mud_sse_two_session_client(addr: SocketAddr) -> Result<(), String> {
        let alice_session_id = 101u64;
        let bob_session_id = 202u64;
        let mut alice_stream = open_event_stream(addr, alice_session_id)?;
        let mut bob_stream = open_event_stream(addr, bob_session_id)?;

        let alice_snapshot = request_initial_snapshot(addr, &mut alice_stream, alice_session_id)?;
        let bob_snapshot = request_initial_snapshot(addr, &mut bob_stream, bob_session_id)?;

        post_dom_event(
            addr,
            DomEventPayload {
                session_id: alice_session_id,
                view_id: 21,
                revision: alice_snapshot.server_revision,
                signature: alice_snapshot.server_signature,
                event: "submit".to_owned(),
                target: "mud-login-alice".to_owned(),
                action: "mud_login".to_owned(),
                fields: BTreeMap::from([("text".to_owned(), "alice".to_owned())]),
            },
        )?;
        let alice_login =
            read_newer_sync_envelope(&mut alice_stream, alice_snapshot.server_revision)?;

        post_dom_event(
            addr,
            DomEventPayload {
                session_id: bob_session_id,
                view_id: 21,
                revision: bob_snapshot.server_revision,
                signature: bob_snapshot.server_signature,
                event: "submit".to_owned(),
                target: "mud-login-bob".to_owned(),
                action: "mud_login".to_owned(),
                fields: BTreeMap::from([("text".to_owned(), "bob".to_owned())]),
            },
        )?;
        let bob_login = read_envelope_containing(
            &mut bob_stream,
            bob_snapshot.server_revision,
            &["The Mica Rooms"],
        )?;

        post_dom_event(
            addr,
            DomEventPayload {
                session_id: alice_session_id,
                view_id: 21,
                revision: alice_login.server_revision,
                signature: alice_login.server_signature,
                event: "submit".to_owned(),
                target: "mud-command".to_owned(),
                action: "mud_command".to_owned(),
                fields: BTreeMap::from([("text".to_owned(), "get coin".to_owned())]),
            },
        )?;

        let bob_delta = read_envelope_containing(
            &mut bob_stream,
            bob_login.server_revision,
            &["takes", "coin"],
        )?;
        if bob_delta.view_id != 21 {
            return Err(format!(
                "expected Bob view 21 update, got {}",
                bob_delta.view_id
            ));
        }
        if bob_delta.server_revision <= bob_login.server_revision {
            return Err("Bob update did not advance past Bob's login revision".to_owned());
        }
        Ok(())
    }

    fn open_event_stream(
        addr: SocketAddr,
        session_id: u64,
    ) -> Result<BufReader<StdTcpStream>, String> {
        let mut stream = StdTcpStream::connect(addr)
            .map_err(|error| format!("failed to connect event stream: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|error| format!("failed to set stream timeout: {error}"))?;
        stream
            .write_all(
                format!("GET /sync/events?session={session_id} HTTP/1.1\r\nHost: {addr}\r\n\r\n")
                    .as_bytes(),
            )
            .map_err(|error| format!("failed to write event stream request: {error}"))?;
        let mut reader = BufReader::new(stream);
        let status = read_http_status(&mut reader)?;
        if !status.contains("200 OK") {
            return Err(format!("unexpected stream status line: {status}"));
        }
        read_http_headers(&mut reader)?;
        Ok(reader)
    }

    fn post_sync_input(addr: SocketAddr, body: Vec<u8>) -> Result<(), String> {
        let mut stream = StdTcpStream::connect(addr)
            .map_err(|error| format!("failed to connect POST: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|error| format!("failed to set POST timeout: {error}"))?;
        let request = format!(
            "POST /sync/input HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len(),
        );
        stream
            .write_all(request.as_bytes())
            .and_then(|()| stream.write_all(&body))
            .map_err(|error| format!("failed to write sync POST: {error}"))?;
        let mut reader = BufReader::new(stream);
        let status = read_http_status(&mut reader)?;
        if !status.contains("202 Accepted") {
            return Err(format!("unexpected sync POST status line: {status}"));
        }
        read_http_headers(&mut reader)?;
        let mut body = Vec::new();
        reader
            .read_to_end(&mut body)
            .map_err(|error| format!("failed to read sync POST response: {error}"))?;
        Ok(())
    }

    fn request_initial_snapshot(
        addr: SocketAddr,
        stream: &mut BufReader<StdTcpStream>,
        session_id: u64,
    ) -> Result<SyncEnvelope, String> {
        post_sync_input(
            addr,
            encoded_sync_envelope(
                SyncEnvelope {
                    kind: SyncMessageKind::NeedView,
                    session_id,
                    view_id: 21,
                    client_revision: 0,
                    client_signature: 0,
                    server_revision: 0,
                    server_signature: 0,
                    payload: b"need".to_vec(),
                }
                .as_ref(),
            ),
        )?;
        let snapshot = read_next_sync_envelope(stream)?;
        if snapshot.kind != SyncMessageKind::ViewSnapshot {
            return Err(format!(
                "expected initial snapshot, got {:?}",
                snapshot.kind
            ));
        }
        Ok(snapshot)
    }

    fn post_dom_event(addr: SocketAddr, event: DomEventPayload) -> Result<(), String> {
        post_sync_input(addr, dom_event_payload_json(&event))
    }

    fn read_http_status(reader: &mut BufReader<StdTcpStream>) -> Result<String, String> {
        let mut status = String::new();
        reader
            .read_line(&mut status)
            .map_err(|error| format!("failed to read HTTP status line: {error}"))?;
        Ok(status)
    }

    fn read_http_headers(reader: &mut BufReader<StdTcpStream>) -> Result<Vec<String>, String> {
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .map_err(|error| format!("failed to read HTTP header: {error}"))?;
            if line == "\r\n" {
                break;
            }
            headers.push(line);
        }
        Ok(headers)
    }

    fn read_next_sync_envelope(
        reader: &mut BufReader<StdTcpStream>,
    ) -> Result<SyncEnvelope, String> {
        loop {
            let mut size = String::new();
            reader
                .read_line(&mut size)
                .map_err(|error| format!("failed to read chunk size: {error}"))?;
            let size = size.trim();
            if size.is_empty() {
                continue;
            }
            let size = usize::from_str_radix(size, 16)
                .map_err(|error| format!("invalid chunk size {size:?}: {error}"))?;
            let mut payload = vec![0u8; size];
            reader
                .read_exact(&mut payload)
                .map_err(|error| format!("failed to read chunk payload: {error}"))?;
            let mut suffix = [0u8; 2];
            reader
                .read_exact(&mut suffix)
                .map_err(|error| format!("failed to read chunk suffix: {error}"))?;
            let payload = String::from_utf8(payload)
                .map_err(|error| format!("sync chunk was not UTF-8: {error}"))?;
            if !payload.starts_with("event: sync\n") {
                continue;
            }
            return parse_sync_sse_payload(&payload);
        }
    }

    fn read_newer_sync_envelope(
        reader: &mut BufReader<StdTcpStream>,
        previous_revision: u64,
    ) -> Result<SyncEnvelope, String> {
        loop {
            let envelope = read_next_sync_envelope(reader)?;
            if envelope.server_revision > previous_revision {
                return Ok(envelope);
            }
        }
    }

    fn read_envelope_containing(
        reader: &mut BufReader<StdTcpStream>,
        previous_revision: u64,
        needles: &[&str],
    ) -> Result<SyncEnvelope, String> {
        let mut last_seen = None;
        for _ in 0..8 {
            let envelope = read_newer_sync_envelope(reader, previous_revision)?;
            let payload: serde_json::Value = serde_json::from_slice(&envelope.payload)
                .map_err(|error| format!("failed to parse sync payload: {error}"))?;
            let payload_text = serde_json::to_string(&payload).unwrap();
            if needles.iter().all(|needle| payload_text.contains(needle)) {
                return Ok(envelope);
            }
            last_seen = Some(payload_text);
        }
        Err(format!(
            "did not receive sync payload containing {:?}; last seen: {}",
            needles,
            last_seen.unwrap_or_else(|| "<none>".to_owned())
        ))
    }

    fn parse_sync_sse_payload(payload: &str) -> Result<SyncEnvelope, String> {
        let data = payload
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .ok_or_else(|| format!("missing data line in SSE payload: {payload}"))?;
        let value: serde_json::Value = serde_json::from_str(data)
            .map_err(|error| format!("failed to parse SSE payload JSON: {error}"))?;
        Ok(SyncEnvelope {
            kind: match value["kind"].as_str() {
                Some("HaveView") => SyncMessageKind::HaveView,
                Some("NeedView") => SyncMessageKind::NeedView,
                Some("ViewSnapshot") => SyncMessageKind::ViewSnapshot,
                Some("ViewDelta") => SyncMessageKind::ViewDelta,
                other => return Err(format!("unknown sync kind in SSE payload: {other:?}")),
            },
            session_id: value["session"]
                .as_str()
                .ok_or_else(|| "missing SSE session".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE session id: {error}"))?,
            view_id: value["view"]
                .as_str()
                .ok_or_else(|| "missing SSE view".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE view id: {error}"))?,
            client_revision: value["clientRevision"]
                .as_str()
                .ok_or_else(|| "missing SSE client revision".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE client revision: {error}"))?,
            client_signature: value["clientSignature"]
                .as_str()
                .ok_or_else(|| "missing SSE client signature".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE client signature: {error}"))?,
            server_revision: value["serverRevision"]
                .as_str()
                .ok_or_else(|| "missing SSE server revision".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE server revision: {error}"))?,
            server_signature: value["serverSignature"]
                .as_str()
                .ok_or_else(|| "missing SSE server signature".to_owned())?
                .parse()
                .map_err(|error| format!("invalid SSE server signature: {error}"))?,
            payload: value["payload"]
                .as_str()
                .ok_or_else(|| "missing SSE payload".to_owned())?
                .as_bytes()
                .to_vec(),
        })
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
            .run_filein(include_str!("../../../apps/shared/retrieval.mica"))
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
            .run_filein(include_str!("../../../apps/mud/ui-mica-inspect.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-compose.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-retrieval.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-narrative.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../apps/mud/ui-actions.mica"))
            .unwrap();
        runner
    }
}
