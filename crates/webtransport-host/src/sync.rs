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

use crate::metrics::{IncomingDatagramKind, RenderOperation, SyncEnvelopeKind, SyncRenderPhase};
use crate::state::{
    ActiveSyncView, InProcessWebTransportHost, RenderedSyncView, SessionState, format_driver_error,
};
use crate::{
    NEXT_SYNC_CHUNK_ID, SYNC_CHUNK_HEADER_LEN, SYNC_CHUNK_MAGIC, SYNC_CHUNK_PAYLOAD_LEN,
    SYNC_DATAGRAM_MAX_LEN,
};
use bytes::Bytes;
use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_host_protocol::{
    DomEventPayload, DomNode, SyncEnvelope, SyncMessageKind, decode_dom_event_payload,
    decode_sync_envelope, diff_dom_nodes, dom_patch_payload_json, encoded_sync_envelope,
    snapshot_payload_json, sync_envelope_from_value, sync_payload_signature, sync_u64_value,
};
use mica_runtime::{SuspendKind, TaskId, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) async fn route_incoming_datagram(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    datagram: Bytes,
) -> Result<(), String> {
    crate::metrics::metrics()
        .incoming_bytes
        .add(datagram.len() as isize);
    match decode_sync_envelope(&datagram) {
        Ok(envelope) => {
            crate::metrics::metrics()
                .incoming_datagrams
                .inc(IncomingDatagramKind::SyncEnvelope);
            crate::metrics::metrics()
                .sync_envelopes
                .inc(sync_envelope_kind(envelope.kind));
            route_sync_envelope(host, endpoint, envelope).await
        }
        Err(_) => route_plain_datagram(host, endpoint, datagram).await,
    }
}

async fn route_plain_datagram(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    datagram: Bytes,
) -> Result<(), String> {
    if let Some(event) = decode_dom_event_payload(&datagram)? {
        crate::metrics::metrics()
            .incoming_datagrams
            .inc(IncomingDatagramKind::DomEvent);
        return route_dom_event(host, endpoint, event).await;
    }

    crate::metrics::metrics()
        .incoming_datagrams
        .inc(IncomingDatagramKind::Plain);
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
    let _dom_event_timer = crate::metrics::start_sync_phase(SyncRenderPhase::DomEvent);
    let route_start = Instant::now();
    let trace = SyncTrace::new("dom_event");
    let event_name = event.event.clone();
    let action = event.action.clone();
    let Some(active) = host.active_rendered_sync_view(endpoint, event.session_id, event.view_id)
    else {
        return send_recovery_snapshot(host, endpoint, &event).await;
    };
    trace.mark("active_view");
    if event.refresh
        && (active.server_revision != event.revision || active.server_signature != event.signature)
    {
        let rendered = render_sync_view(host, endpoint, event.view_id).await?;
        trace.mark("stale_render");
        if event.revision > rendered.revision {
            return send_recovery_snapshot_from_rendered(host, endpoint, &event, rendered).await;
        }
        host.store_rendered_sync_view(endpoint, event.session_id, event.view_id, &rendered);
    }

    let sync_event_start = Instant::now();
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
    let sync_event_us = sync_event_start.elapsed().as_micros();
    trace.mark("sync_event");
    match submitted.outcome {
        TaskOutcome::Complete { value, .. } => {
            tracing::trace!(
                target: "mica_webtransport_host::sync",
                value = %value,
                "sync event completed"
            );
        }
        TaskOutcome::Suspended { .. } => {
            if let Some(state) = host.sessions.lock().unwrap().get(&endpoint).cloned() {
                state
                    .sync
                    .lock()
                    .unwrap()
                    .pending_tasks
                    .insert(submitted.task_id);
            }
        }
        TaskOutcome::Aborted { error, .. } => {
            return Err(format!("sync_event aborted: {error}"));
        }
    }
    if !event.refresh {
        tracing::debug!(
            target: "mica_webtransport_host::sync",
            endpoint = ?endpoint,
            session_id = event.session_id,
            view_id = event.view_id,
            event = %event_name,
            action = %action,
            sync_event_us,
            total_us = route_start.elapsed().as_micros(),
            "sync DOM event routed without refresh"
        );
        return Ok(());
    }
    let refresh_start = Instant::now();
    let result = refresh_active_sync_view(
        host,
        endpoint,
        event.session_id,
        event.view_id,
        true,
        Some(action.as_str()),
    )
    .await;
    let refresh_us = refresh_start.elapsed().as_micros();
    tracing::debug!(
        target: "mica_webtransport_host::sync",
        endpoint = ?endpoint,
        session_id = event.session_id,
        view_id = event.view_id,
        event = %event_name,
        action = %action,
        sync_event_us,
        refresh_us,
        total_us = route_start.elapsed().as_micros(),
        "sync DOM event routed"
    );
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
    crate::metrics::metrics().recovery_snapshots.inc();
    host.send_sync_envelope(endpoint, envelope)?;
    host.store_rendered_sync_view(endpoint, event.session_id, event.view_id, &rendered);
    Ok(())
}

async fn refresh_active_sync_view(
    host: &InProcessWebTransportHost,
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
    force_ack: bool,
    action: Option<&str>,
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
    refresh_active_sync_view_for(&host.driver, &host.sessions, active, force_ack, action).await
}

pub(crate) async fn refresh_active_sync_views_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
) -> Result<(), String> {
    for active in active_sync_views(sessions) {
        refresh_active_sync_view_for(driver, sessions, active, false, None).await?;
    }
    Ok(())
}

async fn refresh_active_sync_view_for(
    driver: &CompioTaskDriver,
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    active: ActiveSyncView,
    force_ack: bool,
    action: Option<&str>,
) -> Result<(), String> {
    let _refresh_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Refresh);
    let refresh_start = Instant::now();
    let revision_start = Instant::now();
    let revision = render_sync_revision_for(driver, active.endpoint, active.view_id).await?;
    let revision_elapsed = revision_start.elapsed();
    let revision_us = revision_elapsed.as_micros();
    crate::metrics::metrics().sync_render_duration_us.record(
        RenderOperation::Refresh,
        crate::metrics::duration_us(revision_elapsed),
    );
    crate::metrics::metrics()
        .sync_render_duration
        .record_elapsed(RenderOperation::Refresh, revision_elapsed);
    if revision == active.server_revision && active.last_tree.is_some() {
        if force_ack {
            let payload_start = Instant::now();
            let payload = {
                let _payload_timer =
                    crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
                dom_patch_payload_json(active.view_id, active.server_revision, &[])
            };
            let payload_us = payload_start.elapsed().as_micros();
            crate::metrics::record_sync_payload(SyncEnvelopeKind::ViewDelta, payload.len());
            crate::metrics::record_sync_patch_count(0);
            let payload_len = payload.len();
            let send_start = Instant::now();
            send_sync_envelope_to(
                sessions,
                active.endpoint,
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
            tracing::debug!(
                target: "mica_webtransport_host::sync",
                endpoint = ?active.endpoint,
                session_id = active.session_id,
                view_id = active.view_id,
                action = ?action,
                force_ack,
                changed = false,
                revision_us,
                payload_us,
                send_us = send_start.elapsed().as_micros(),
                total_us = refresh_start.elapsed().as_micros(),
                payload_bytes = payload_len,
                patches = 0usize,
                "sync refresh view"
            );
        }
        return Ok(());
    }

    let render_start = Instant::now();
    let rendered = render_sync_view_for_revision(
        driver,
        active.endpoint,
        active.view_id,
        revision,
        render_start,
    )
    .await?;
    let render_us = render_start.elapsed().as_micros();
    let has_queued_view_update =
        has_pending_sync_view(sessions, active.endpoint, active.session_id, active.view_id);
    let envelope = if has_queued_view_update {
        crate::metrics::record_sync_payload(SyncEnvelopeKind::ViewSnapshot, rendered.payload.len());
        tracing::debug!(
            target: "mica_webtransport_host::sync",
            endpoint = ?active.endpoint,
            session_id = active.session_id,
            view_id = active.view_id,
            action = ?action,
            force_ack,
            changed = true,
            coalesced = true,
            revision_us,
            render_us,
            total_us = refresh_start.elapsed().as_micros(),
            payload_bytes = rendered.payload.len(),
            "sync refresh view"
        );
        snapshot_envelope(
            active.session_id,
            active.view_id,
            active.client_revision,
            active.client_signature,
            &rendered,
        )
    } else if let Some(last_tree) = &active.last_tree {
        let diff_start = Instant::now();
        let patches = {
            let _diff_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Diff);
            diff_dom_nodes(last_tree, &rendered.tree)
        };
        let diff_us = diff_start.elapsed().as_micros();
        crate::metrics::record_sync_patch_count(patches.len());
        if patches.is_empty() {
            if force_ack {
                let payload_start = Instant::now();
                let payload = {
                    let _payload_timer =
                        crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
                    dom_patch_payload_json(active.view_id, rendered.revision, &[])
                };
                let payload_us = payload_start.elapsed().as_micros();
                crate::metrics::record_sync_payload(SyncEnvelopeKind::ViewDelta, payload.len());
                let payload_len = payload.len();
                let send_start = Instant::now();
                send_sync_envelope_to(
                    sessions,
                    active.endpoint,
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
                tracing::debug!(
                    target: "mica_webtransport_host::sync",
                    endpoint = ?active.endpoint,
                    session_id = active.session_id,
                    view_id = active.view_id,
                    action = ?action,
                    force_ack,
                    changed = true,
                    revision_us,
                    render_us,
                    diff_us,
                    payload_us,
                    send_us = send_start.elapsed().as_micros(),
                    total_us = refresh_start.elapsed().as_micros(),
                    payload_bytes = payload_len,
                    patches = 0usize,
                    "sync refresh view"
                );
            }
            store_rendered_sync_view_in(
                sessions,
                active.endpoint,
                active.session_id,
                active.view_id,
                &rendered,
            );
            return Ok(());
        }
        let patch_count = patches.len();
        let payload_start = Instant::now();
        let payload = {
            let _payload_timer = crate::metrics::start_sync_phase(SyncRenderPhase::DeltaPayload);
            dom_patch_payload_json(active.view_id, rendered.revision, &patches)
        };
        let payload_us = payload_start.elapsed().as_micros();
        crate::metrics::record_sync_payload(SyncEnvelopeKind::ViewDelta, payload.len());
        tracing::debug!(
            target: "mica_webtransport_host::sync",
            endpoint = ?active.endpoint,
            session_id = active.session_id,
            view_id = active.view_id,
            action = ?action,
            force_ack,
            changed = true,
            revision_us,
            render_us,
            diff_us,
            payload_us,
            total_us = refresh_start.elapsed().as_micros(),
            payload_bytes = payload.len(),
            patches = patch_count,
            "sync refresh view"
        );
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
    let send_start = Instant::now();
    send_sync_envelope_to(sessions, active.endpoint, envelope)?;
    tracing::debug!(
        target: "mica_webtransport_host::sync",
        endpoint = ?active.endpoint,
        session_id = active.session_id,
        view_id = active.view_id,
        action = ?action,
        force_ack,
        send_us = send_start.elapsed().as_micros(),
        total_us = refresh_start.elapsed().as_micros(),
        "sync send view"
    );
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
    let start = Instant::now();
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
    .inspect(|_| {
        let elapsed = start.elapsed();
        crate::metrics::metrics().sync_render_duration_us.record(
            RenderOperation::Revision,
            crate::metrics::duration_us(elapsed),
        );
        crate::metrics::metrics()
            .sync_render_duration
            .record_elapsed(RenderOperation::Revision, elapsed);
    })
}

async fn render_sync_view_for(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    view_id: u64,
) -> Result<RenderedSyncView, String> {
    let render_start = Instant::now();
    let trace = SyncTrace::new("render");
    let revision = {
        let _revision_timer = crate::metrics::start_sync_phase(SyncRenderPhase::Revision);
        render_sync_revision_for(driver, endpoint, view_id).await?
    };
    trace.mark("revision");
    render_sync_view_for_revision(driver, endpoint, view_id, revision, render_start).await
}

async fn render_sync_view_for_revision(
    driver: &CompioTaskDriver,
    endpoint: Identity,
    view_id: u64,
    revision: u64,
    render_start: Instant,
) -> Result<RenderedSyncView, String> {
    let trace = SyncTrace::new("render");
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
        snapshot_payload_json(view_id, revision, &tree)
    };
    let signature = sync_payload_signature(revision, &payload);
    crate::metrics::record_sync_payload(SyncEnvelopeKind::ViewSnapshot, payload.len());
    trace.mark("payload");

    let rendered = RenderedSyncView {
        revision,
        signature,
        tree,
        payload,
    };
    let elapsed = render_start.elapsed();
    crate::metrics::metrics()
        .sync_render_duration_us
        .record(RenderOperation::View, crate::metrics::duration_us(elapsed));
    crate::metrics::metrics()
        .sync_render_duration
        .record_elapsed(RenderOperation::View, elapsed);
    Ok(rendered)
}

pub(crate) struct SyncTrace {
    enabled: bool,
    label: &'static str,
    start: Instant,
    last: Mutex<Instant>,
}

impl SyncTrace {
    pub(crate) fn new(label: &'static str) -> Self {
        let now = Instant::now();
        Self {
            enabled: tracing::enabled!(
                target: "mica_webtransport_host::sync",
                tracing::Level::TRACE
            ),
            label,
            start: now,
            last: Mutex::new(now),
        }
    }

    pub(crate) fn mark(&self, phase: &'static str) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut last = self.last.lock().unwrap();
        tracing::trace!(
            target: "mica_webtransport_host::sync",
            label = self.label,
            phase,
            elapsed_us = now.duration_since(*last).as_micros(),
            total_us = now.duration_since(self.start).as_micros(),
            "WebTransport sync phase completed"
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

pub(crate) fn store_rendered_sync_view_in(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
    rendered: &RenderedSyncView,
) {
    if let Some(state) = sessions.lock().unwrap().get(&endpoint).cloned() {
        let _store_timer = crate::metrics::start_sync_phase(SyncRenderPhase::StoreRendered);
        state.sync.lock().unwrap().store_rendered_view(
            session_id,
            view_id,
            rendered.revision,
            rendered.signature,
            rendered.tree.clone(),
        );
    }
}

pub(crate) fn send_sync_envelope_to(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    endpoint: Identity,
    envelope: SyncEnvelope,
) -> Result<(), String> {
    let Some(state) = sessions.lock().unwrap().get(&endpoint).cloned() else {
        return Ok(());
    };
    crate::metrics::metrics()
        .sync_envelopes
        .inc(sync_envelope_kind(envelope.kind));
    crate::metrics::record_sync_payload(sync_envelope_kind(envelope.kind), envelope.payload.len());
    let _send_timer = crate::metrics::start_sync_phase(SyncRenderPhase::SendEnvelope);
    state.output.send_sync_envelope(envelope)?;
    Ok(())
}

fn has_pending_sync_view(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    endpoint: Identity,
    session_id: u64,
    view_id: u64,
) -> bool {
    sessions
        .lock()
        .unwrap()
        .get(&endpoint)
        .is_some_and(|state| state.output.has_pending_view_sync(session_id, view_id))
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

pub(crate) fn start_event_pump(
    driver: Arc<CompioTaskDriver>,
    sessions: Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    stop_events: Arc<AtomicBool>,
) {
    compio::runtime::spawn(async move {
        while !stop_events.load(Ordering::Relaxed) {
            let events = driver.wait_events().await;
            if route_driver_events(&sessions, events) {
                loop {
                    if let Err(error) = refresh_active_sync_views_for(&driver, &sessions).await {
                        tracing::warn!(
                            error = %error,
                            "failed to refresh active WebTransport sync views"
                        );
                    }
                    let pending = driver.drain_events();
                    let refresh_again = route_driver_events(&sessions, pending);
                    if !refresh_again {
                        break;
                    }
                }
            }
        }
    })
    .detach();
}

pub(crate) fn active_sync_views(
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
    crate::metrics::metrics()
        .active_sync_views
        .set(active.len() as i64);
    active
}

pub(crate) fn route_driver_event(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    event: DriverEvent,
) -> bool {
    match event {
        DriverEvent::Effect(effect) => {
            if let Some(state) = sessions.lock().unwrap().get(&effect.target).cloned() {
                crate::metrics::metrics().routed_driver_events.inc();
                for datagram in effect_datagrams(effect.target, &effect.value) {
                    let _ = state.output.send_datagram(datagram);
                }
            }
            true
        }
        DriverEvent::TaskCompleted { task_id, .. } => complete_pending_sync_task(sessions, task_id),
        DriverEvent::TaskAborted { task_id, .. } | DriverEvent::TaskFailed { task_id, .. } => {
            complete_pending_sync_task(sessions, task_id)
        }
        DriverEvent::TaskSuspended { kind, .. } => matches!(kind, SuspendKind::Commit),
    }
}

fn route_driver_events(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    events: Vec<DriverEvent>,
) -> bool {
    let mut refresh = false;
    for event in events {
        refresh = route_driver_event(sessions, event) || refresh;
    }
    refresh
}

fn complete_pending_sync_task(
    sessions: &Arc<Mutex<HashMap<Identity, Arc<SessionState>>>>,
    task_id: TaskId,
) -> bool {
    let sessions = sessions
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for state in sessions {
        let mut sync = state.sync.lock().unwrap();
        if sync.pending_tasks.remove(&task_id) {
            return true;
        }
    }
    false
}

fn effect_datagrams(target: Identity, value: &Value) -> Vec<Bytes> {
    if let Some(envelope) = sync_envelope_from_value(target.raw(), value) {
        crate::metrics::metrics()
            .sync_envelopes
            .inc(sync_envelope_kind(envelope.kind));
        return sync_envelope_datagrams(envelope.as_ref());
    }
    vec![effect_datagram(target, value)]
}

fn sync_envelope_kind(kind: SyncMessageKind) -> SyncEnvelopeKind {
    match kind {
        SyncMessageKind::NeedView => SyncEnvelopeKind::NeedView,
        SyncMessageKind::HaveView => SyncEnvelopeKind::HaveView,
        SyncMessageKind::ViewSnapshot => SyncEnvelopeKind::ViewSnapshot,
        SyncMessageKind::ViewDelta => SyncEnvelopeKind::ViewDelta,
    }
}

pub(crate) fn effect_datagram(target: Identity, value: &Value) -> Bytes {
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

pub(crate) fn sync_envelope_datagrams(
    envelope: mica_host_protocol::SyncEnvelopeRef<'_>,
) -> Vec<Bytes> {
    let encoded = encoded_sync_envelope(envelope);
    if encoded.len() <= SYNC_DATAGRAM_MAX_LEN {
        crate::metrics::metrics().sync_envelope_datagrams.inc();
        return vec![Bytes::from(encoded)];
    }

    let count = encoded.len().div_ceil(SYNC_CHUNK_PAYLOAD_LEN);
    crate::metrics::metrics()
        .sync_envelope_datagrams
        .add(count as isize);
    crate::metrics::metrics()
        .sync_envelope_chunks
        .add(count as isize);
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
