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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_host_protocol::{HostMessage, PROTOCOL_VERSION};
use mica_host_zmq::{ZmqHostSocket, ZmqTransportError};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;

const DEFAULT_DRAIN_LIMIT: u32 = 64;
const MAX_DRAIN_LIMIT: u32 = 1024;

pub(crate) struct RpcHandler {
    driver: CompioTaskDriver,
    endpoints: BTreeMap<Identity, EndpointState>,
}

#[derive(Debug)]
pub(crate) enum RpcServerError {
    Transport(ZmqTransportError),
}

#[derive(Default)]
struct EndpointState {
    actor: Option<Identity>,
    output: VecDeque<Value>,
}

impl fmt::Display for RpcServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RpcServerError {}

impl From<ZmqTransportError> for RpcServerError {
    fn from(error: ZmqTransportError) -> Self {
        Self::Transport(error)
    }
}

pub(crate) async fn serve_zmq_rpc_once(
    socket: &ZmqHostSocket,
    handler: &mut RpcHandler,
) -> Result<(), RpcServerError> {
    let request = socket.recv_routed_message().await?;
    let replies = handler.handle_message(request.message);
    for reply in replies {
        socket.send_routed_message(&request.peer, &reply).await?;
    }
    Ok(())
}

pub(crate) async fn serve_zmq_rpc_n(
    socket: &ZmqHostSocket,
    handler: &mut RpcHandler,
    requests: usize,
) -> Result<(), RpcServerError> {
    for _ in 0..requests {
        serve_zmq_rpc_once(socket, handler).await?;
    }
    Ok(())
}

pub(crate) async fn serve_zmq_rpc_forever(
    socket: &ZmqHostSocket,
    handler: &mut RpcHandler,
) -> Result<(), RpcServerError> {
    loop {
        serve_zmq_rpc_once(socket, handler).await?;
    }
}

impl RpcHandler {
    pub(crate) fn new(driver: CompioTaskDriver) -> Self {
        Self {
            driver,
            endpoints: BTreeMap::new(),
        }
    }

    pub(crate) fn handle_message(&mut self, message: HostMessage) -> Vec<HostMessage> {
        let is_request = is_request_message(&message);
        let mut replies = match message {
            HostMessage::Hello { .. } => vec![HostMessage::HelloAck {
                protocol_version: PROTOCOL_VERSION,
                feature_bits: 0,
            }],
            HostMessage::HelloAck { .. }
            | HostMessage::RequestAccepted { .. }
            | HostMessage::RequestRejected { .. }
            | HostMessage::IdentityResolved { .. }
            | HostMessage::OutputReady { .. }
            | HostMessage::OutputBatch { .. }
            | HostMessage::EndpointClosed { .. }
            | HostMessage::TaskCompleted { .. }
            | HostMessage::TaskFailed { .. } => vec![rejected(
                0,
                "E_UNEXPECTED_MESSAGE",
                "message is not a daemon request",
            )],
            HostMessage::OpenEndpoint {
                request_id,
                endpoint,
                actor,
                protocol,
                grant_token,
            } => self.open_endpoint(request_id, endpoint, actor, protocol, grant_token),
            HostMessage::CloseEndpoint {
                request_id,
                endpoint,
            } => self.close_endpoint(request_id, endpoint),
            HostMessage::ResolveIdentity { request_id, name } => {
                self.resolve_identity(request_id, name)
            }
            HostMessage::SubmitSource {
                request_id,
                endpoint,
                actor,
                source,
            } => self.submit_source(request_id, endpoint, actor, source),
            HostMessage::SubmitInput {
                request_id,
                endpoint,
                value,
            } => self.submit_input(request_id, endpoint, value),
            HostMessage::DrainOutput {
                request_id,
                endpoint,
                limit,
            } => self.drain_output(request_id, endpoint, limit),
        };
        if is_request {
            let mut events = self.drain_driver_messages();
            events.append(&mut replies);
            return events;
        }
        replies
    }

    fn open_endpoint(
        &mut self,
        request_id: u64,
        endpoint: Identity,
        actor: Option<Identity>,
        protocol: String,
        grant_token: Option<String>,
    ) -> Vec<HostMessage> {
        if grant_token.is_some() {
            return vec![rejected(
                request_id,
                "E_GRANT_UNSUPPORTED",
                "grant tokens are not implemented yet",
            )];
        }
        match self
            .driver
            .open_endpoint(endpoint, actor, Symbol::intern(&protocol))
        {
            Ok(()) => {
                self.endpoints
                    .entry(endpoint)
                    .and_modify(|state| state.actor = actor)
                    .or_insert_with(|| EndpointState {
                        actor,
                        output: VecDeque::new(),
                    });
                vec![accepted(request_id, None)]
            }
            Err(error) => vec![rejected(request_id, "E_OPEN_ENDPOINT", error.to_string())],
        }
    }

    fn close_endpoint(&mut self, request_id: u64, endpoint: Identity) -> Vec<HostMessage> {
        let closed = self.driver.close_endpoint(endpoint);
        self.endpoints.remove(&endpoint);
        vec![
            HostMessage::EndpointClosed {
                endpoint,
                reason: format!("closed {closed} task endpoint bindings"),
            },
            accepted(request_id, None),
        ]
    }

    fn resolve_identity(&self, request_id: u64, name: Symbol) -> Vec<HostMessage> {
        match self.driver.named_identity(name) {
            Ok(identity) => vec![HostMessage::IdentityResolved {
                request_id,
                name,
                identity,
            }],
            Err(error) => vec![rejected(
                request_id,
                "E_UNKNOWN_IDENTITY",
                error.to_string(),
            )],
        }
    }

    fn submit_source(
        &mut self,
        request_id: u64,
        endpoint: Identity,
        actor: Identity,
        source: String,
    ) -> Vec<HostMessage> {
        if !self.endpoints.contains_key(&endpoint) {
            return vec![rejected(
                request_id,
                "E_NO_ENDPOINT",
                "endpoint is not open",
            )];
        }
        match self.driver.submit_source_as_actor(endpoint, actor, source) {
            Ok(submitted) => vec![accepted(request_id, Some(submitted.task_id))],
            Err(error) => vec![rejected(request_id, "E_SUBMIT_SOURCE", error.to_string())],
        }
    }

    fn submit_input(
        &mut self,
        request_id: u64,
        endpoint: Identity,
        value: Value,
    ) -> Vec<HostMessage> {
        if !self.endpoints.contains_key(&endpoint) {
            return vec![rejected(
                request_id,
                "E_NO_ENDPOINT",
                "endpoint is not open",
            )];
        }
        match self.driver.input(endpoint, value) {
            Ok(_) => vec![accepted(request_id, None)],
            Err(error) => vec![rejected(request_id, "E_SUBMIT_INPUT", error.to_string())],
        }
    }

    fn drain_output(
        &mut self,
        request_id: u64,
        endpoint: Identity,
        limit: u32,
    ) -> Vec<HostMessage> {
        let Some(state) = self.endpoints.get_mut(&endpoint) else {
            return vec![rejected(
                request_id,
                "E_NO_ENDPOINT",
                "endpoint is not open",
            )];
        };
        let limit = normalized_drain_limit(limit) as usize;
        let count = limit.min(state.output.len());
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(value) = state.output.pop_front() else {
                break;
            };
            values.push(value);
        }
        vec![
            HostMessage::OutputBatch { endpoint, values },
            accepted(request_id, None),
        ]
    }

    fn drain_driver_messages(&mut self) -> Vec<HostMessage> {
        self.driver
            .drain_events()
            .into_iter()
            .flat_map(|event| self.route_driver_event(event))
            .collect()
    }

    fn route_driver_event(&mut self, event: DriverEvent) -> Vec<HostMessage> {
        match event {
            DriverEvent::TaskCompleted { task_id, value } => {
                vec![HostMessage::TaskCompleted { task_id, value }]
            }
            DriverEvent::TaskAborted { task_id, error } => {
                vec![HostMessage::TaskFailed { task_id, error }]
            }
            DriverEvent::TaskFailed { task_id, error } => vec![HostMessage::TaskFailed {
                task_id,
                error: Value::error(Symbol::intern("E_DRIVER"), Some(error), None),
            }],
            DriverEvent::TaskSuspended { .. } => Vec::new(),
            DriverEvent::Effect(effect) => {
                let targets = self.effect_targets(effect.target);
                let mut messages = Vec::with_capacity(targets.len());
                for target in targets {
                    let state = self.endpoints.entry(target).or_default();
                    state.output.push_back(effect.value.clone());
                    messages.push(HostMessage::OutputReady {
                        endpoint: target,
                        buffered: state.output.len().try_into().unwrap_or(u32::MAX),
                    });
                }
                messages
            }
        }
    }

    fn effect_targets(&self, target: Identity) -> Vec<Identity> {
        let mut targets = Vec::new();
        if self.endpoints.contains_key(&target) {
            targets.push(target);
        }
        for (endpoint, state) in &self.endpoints {
            if state.actor == Some(target) && !targets.contains(endpoint) {
                targets.push(*endpoint);
            }
        }
        targets
    }
}

fn accepted(request_id: u64, task_id: Option<u64>) -> HostMessage {
    HostMessage::RequestAccepted {
        request_id,
        task_id,
    }
}

fn is_request_message(message: &HostMessage) -> bool {
    matches!(
        message,
        HostMessage::OpenEndpoint { .. }
            | HostMessage::CloseEndpoint { .. }
            | HostMessage::ResolveIdentity { .. }
            | HostMessage::SubmitSource { .. }
            | HostMessage::SubmitInput { .. }
            | HostMessage::DrainOutput { .. }
    )
}

fn rejected(request_id: u64, code: &str, message: impl Into<String>) -> HostMessage {
    HostMessage::RequestRejected {
        request_id,
        code: Symbol::intern(code),
        message: message.into(),
    }
}

fn normalized_drain_limit(limit: u32) -> u32 {
    if limit == 0 {
        return DEFAULT_DRAIN_LIMIT;
    }
    limit.min(MAX_DRAIN_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_host_zmq::ZmqSocketOptions;
    use mica_runtime::{SourceRunner, TaskOutcome};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_IPC_ENDPOINT: AtomicU64 = AtomicU64::new(1);

    fn endpoint(raw: u64) -> Identity {
        Identity::new(raw).unwrap()
    }

    fn seeded_driver() -> (CompioTaskDriver, Identity) {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:GrantEffect, 1)").unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("assert GrantEffect(#alice)").unwrap();
        let alice = runner.named_identity(Symbol::intern("alice")).unwrap();
        (CompioTaskDriver::spawn(runner).unwrap(), alice)
    }

    #[test]
    fn rpc_handler_accepts_endpoint_and_submits_source() {
        let (driver, actor) = seeded_driver();
        let endpoint = endpoint(0x00ef_0000_0000_0001);
        let mut handler = RpcHandler::new(driver);

        assert_eq!(
            handler.handle_message(HostMessage::OpenEndpoint {
                request_id: 1,
                endpoint,
                actor: None,
                protocol: "test".to_owned(),
                grant_token: None,
            }),
            vec![accepted(1, None)]
        );

        let replies = handler.handle_message(HostMessage::SubmitSource {
            request_id: 2,
            endpoint,
            actor,
            source: "emit(#endpoint, \"hello\")\nreturn actor()".to_owned(),
        });
        assert!(matches!(
            &replies[..],
            [
                HostMessage::OutputReady { endpoint: target, buffered: 1 },
                HostMessage::TaskCompleted { value, .. },
                HostMessage::RequestAccepted {
                    request_id: 2,
                    task_id: Some(_),
                },
            ] if *target == endpoint && *value == Value::identity(actor)
        ));

        let replies = handler.handle_message(HostMessage::DrainOutput {
            request_id: 3,
            endpoint,
            limit: 10,
        });
        assert_eq!(
            replies,
            vec![
                HostMessage::OutputBatch {
                    endpoint,
                    values: vec![Value::string("hello")],
                },
                accepted(3, None),
            ]
        );
    }

    #[test]
    fn rpc_handler_rejects_grant_tokens_until_validation_exists() {
        let (driver, _) = seeded_driver();
        let mut handler = RpcHandler::new(driver);
        assert_eq!(
            handler.handle_message(HostMessage::OpenEndpoint {
                request_id: 1,
                endpoint: endpoint(0x00ef_0000_0000_0002),
                actor: None,
                protocol: "test".to_owned(),
                grant_token: Some("token".to_owned()),
            }),
            vec![rejected(
                1,
                "E_GRANT_UNSUPPORTED",
                "grant tokens are not implemented yet"
            )]
        );
    }

    #[test]
    fn rpc_handler_routes_failed_source_as_rejection() {
        let (driver, actor) = seeded_driver();
        let endpoint = endpoint(0x00ef_0000_0000_0003);
        let mut handler = RpcHandler::new(driver);
        handler.handle_message(HostMessage::OpenEndpoint {
            request_id: 1,
            endpoint,
            actor: None,
            protocol: "test".to_owned(),
            grant_token: None,
        });

        let replies = handler.handle_message(HostMessage::SubmitSource {
            request_id: 2,
            endpoint,
            actor,
            source: "return 1 / 0".to_owned(),
        });
        assert!(matches!(
            &replies[..],
            [
                HostMessage::TaskFailed { error, .. },
                HostMessage::RequestAccepted {
                    request_id: 2,
                    task_id: Some(_),
                },
            ] if error.error_code_symbol() == Some(Symbol::intern("E_DIV"))
        ));
    }

    #[test]
    fn rpc_handler_routes_actor_effects_to_actor_endpoint() {
        let (driver, actor) = seeded_driver();
        let endpoint = endpoint(0x00ef_0000_0000_0007);
        let mut handler = RpcHandler::new(driver);
        handler.handle_message(HostMessage::OpenEndpoint {
            request_id: 1,
            endpoint,
            actor: Some(actor),
            protocol: "test".to_owned(),
            grant_token: None,
        });

        let replies = handler.handle_message(HostMessage::SubmitSource {
            request_id: 2,
            endpoint,
            actor,
            source: "emit(#alice, \"hello actor\")".to_owned(),
        });
        assert!(matches!(
            &replies[..],
            [
                HostMessage::OutputReady { endpoint: target, buffered: 1 },
                HostMessage::TaskCompleted { .. },
                HostMessage::RequestAccepted { request_id: 2, .. },
            ] if *target == endpoint
        ));
        let replies = handler.handle_message(HostMessage::DrainOutput {
            request_id: 3,
            endpoint,
            limit: 1,
        });
        assert_eq!(
            replies,
            vec![
                HostMessage::OutputBatch {
                    endpoint,
                    values: vec![Value::string("hello actor")],
                },
                accepted(3, None),
            ]
        );
    }

    #[test]
    fn rpc_handler_resolves_named_identity() {
        let (driver, actor) = seeded_driver();
        let mut handler = RpcHandler::new(driver);

        assert_eq!(
            handler.handle_message(HostMessage::ResolveIdentity {
                request_id: 12,
                name: Symbol::intern("alice"),
            }),
            vec![HostMessage::IdentityResolved {
                request_id: 12,
                name: Symbol::intern("alice"),
                identity: actor,
            }]
        );

        assert!(matches!(
            handler.handle_message(HostMessage::ResolveIdentity {
                request_id: 13,
                name: Symbol::intern("missing"),
            }).as_slice(),
            [HostMessage::RequestRejected { request_id: 13, code, .. }]
                if *code == Symbol::intern("E_UNKNOWN_IDENTITY")
        ));
    }

    #[test]
    fn normalized_drain_limit_defaults_and_clamps() {
        assert_eq!(normalized_drain_limit(0), DEFAULT_DRAIN_LIMIT);
        assert_eq!(normalized_drain_limit(1), 1);
        assert_eq!(normalized_drain_limit(MAX_DRAIN_LIMIT + 1), MAX_DRAIN_LIMIT);
    }

    #[test]
    fn source_runner_identity_request_uses_actor_context() {
        let (driver, actor) = seeded_driver();
        let submitted = driver
            .submit_source_as_actor(
                endpoint(0x00ef_0000_0000_0004),
                actor,
                "return actor()".to_owned(),
            )
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(actor)
        ));
    }

    #[test]
    fn zmq_rpc_service_handles_hello_over_ipc() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ipc = ipc_endpoint();
            let _cleanup = IpcCleanup::new(ipc.path.clone());
            let context = zmq::Context::new();
            let router =
                ZmqHostSocket::bind(&context, zmq::ROUTER, &ipc.uri, ZmqSocketOptions::default())
                    .unwrap();
            let dealer = ZmqHostSocket::connect(
                &context,
                zmq::DEALER,
                &ipc.uri,
                ZmqSocketOptions::default(),
            )
            .unwrap();
            let (driver, _) = seeded_driver();
            let mut handler = RpcHandler::new(driver);

            let server = compio::runtime::spawn(async move {
                serve_zmq_rpc_once(&router, &mut handler).await.unwrap();
            });
            dealer
                .send_message(&HostMessage::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    min_protocol_version: PROTOCOL_VERSION,
                    feature_bits: 0,
                    host_name: "client".to_owned(),
                })
                .await
                .unwrap();
            assert_eq!(
                dealer.recv_message().await.unwrap(),
                HostMessage::HelloAck {
                    protocol_version: PROTOCOL_VERSION,
                    feature_bits: 0,
                }
            );
            server.await.unwrap();
        });
    }

    #[test]
    fn zmq_rpc_service_handles_multiple_requests_over_ipc() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let ipc = ipc_endpoint();
            let _cleanup = IpcCleanup::new(ipc.path.clone());
            let context = zmq::Context::new();
            let router =
                ZmqHostSocket::bind(&context, zmq::ROUTER, &ipc.uri, ZmqSocketOptions::default())
                    .unwrap();
            let dealer = ZmqHostSocket::connect(
                &context,
                zmq::DEALER,
                &ipc.uri,
                ZmqSocketOptions::default(),
            )
            .unwrap();
            let (driver, _) = seeded_driver();
            let mut handler = RpcHandler::new(driver);
            let endpoint = endpoint(0x00ef_0000_0000_0005);

            let server = compio::runtime::spawn(async move {
                serve_zmq_rpc_n(&router, &mut handler, 2).await.unwrap();
            });
            dealer
                .send_message(&HostMessage::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    min_protocol_version: PROTOCOL_VERSION,
                    feature_bits: 0,
                    host_name: "client".to_owned(),
                })
                .await
                .unwrap();
            assert!(matches!(
                dealer.recv_message().await.unwrap(),
                HostMessage::HelloAck { .. }
            ));

            dealer
                .send_message(&HostMessage::OpenEndpoint {
                    request_id: 42,
                    endpoint,
                    actor: None,
                    protocol: "test".to_owned(),
                    grant_token: None,
                })
                .await
                .unwrap();
            assert_eq!(
                dealer.recv_message().await.unwrap(),
                HostMessage::RequestAccepted {
                    request_id: 42,
                    task_id: None,
                }
            );
            server.await.unwrap();
        });
    }

    #[test]
    fn rpc_handler_can_drain_delayed_driver_events() {
        let (driver, actor) = seeded_driver();
        let endpoint = endpoint(0x00ef_0000_0000_0006);
        let mut handler = RpcHandler::new(driver);
        handler.handle_message(HostMessage::OpenEndpoint {
            request_id: 1,
            endpoint,
            actor: None,
            protocol: "test".to_owned(),
            grant_token: None,
        });
        let replies = handler.handle_message(HostMessage::SubmitSource {
            request_id: 2,
            endpoint,
            actor,
            source: "suspend(0.001)\nemit(#endpoint, \"awake\")".to_owned(),
        });
        assert!(matches!(
            &replies[..],
            [HostMessage::RequestAccepted {
                request_id: 2,
                task_id: Some(_)
            }]
        ));

        std::thread::sleep(std::time::Duration::from_millis(20));
        let events = handler.drain_driver_messages();
        assert!(matches!(
            &events[..],
            [
                HostMessage::OutputReady { endpoint: target, buffered: 1 },
                HostMessage::TaskCompleted { .. },
            ] if *target == endpoint
        ));
    }

    struct IpcEndpoint {
        uri: String,
        path: PathBuf,
    }

    struct IpcCleanup {
        path: PathBuf,
    }

    impl IpcCleanup {
        fn new(path: PathBuf) -> Self {
            let _ = std::fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for IpcCleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn ipc_endpoint() -> IpcEndpoint {
        let index = NEXT_IPC_ENDPOINT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mica-daemon-rpc-{}-{index}.sock",
            std::process::id()
        ));
        IpcEndpoint {
            uri: format!("ipc://{}", path.display()),
            path,
        }
    }
}
