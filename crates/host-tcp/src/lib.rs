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

use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
use compio::runtime::ResumeUnwind;
use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_runtime::{SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub const DEFAULT_BIND: &str = "127.0.0.1:7777";
pub const DAEMON_ENDPOINT_ID_START: u64 = 0x00ed_0000_0000_0000;

const ENDPOINT_OUTPUT_HIGH_WATER_LINES: usize = 128;
const ENDPOINT_OUTPUT_DRAIN_LINES: usize = 64;

#[derive(Clone, Debug)]
pub struct ActorBinding {
    pub name: String,
    pub identity: Identity,
}

pub struct InProcessTcpHost {
    driver: Arc<CompioTaskDriver>,
    endpoints: Arc<Mutex<BTreeMap<Identity, Arc<EndpointOutput>>>>,
    stop_events: Arc<AtomicBool>,
    next_endpoint: AtomicU64,
}

#[derive(Default)]
struct EndpointOutput {
    state: Mutex<EndpointOutputState>,
}

#[derive(Default)]
struct EndpointOutputState {
    lines: VecDeque<String>,
    closed: bool,
    waker: Option<Waker>,
}

struct EndpointOutputRecv<'a> {
    output: &'a EndpointOutput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EndpointOutputReady {
    Ready { buffered: usize },
    HighWater { buffered: usize },
    Closed,
}

impl InProcessTcpHost {
    pub fn new(driver: CompioTaskDriver) -> Self {
        let driver = Arc::new(driver);
        let endpoints = Arc::new(Mutex::new(BTreeMap::new()));
        let stop_events = Arc::new(AtomicBool::new(false));
        start_event_pump(driver.clone(), endpoints.clone(), stop_events.clone());
        Self {
            driver,
            endpoints,
            stop_events,
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
        }
    }

    #[cfg(test)]
    fn new_without_event_pump(driver: CompioTaskDriver) -> Self {
        Self {
            driver: Arc::new(driver),
            endpoints: Arc::new(Mutex::new(BTreeMap::new())),
            stop_events: Arc::new(AtomicBool::new(false)),
            next_endpoint: AtomicU64::new(DAEMON_ENDPOINT_ID_START),
        }
    }

    fn allocate_endpoint(&self) -> Result<Identity, String> {
        let raw = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        Identity::new(raw).ok_or_else(|| "endpoint identity space is exhausted".to_owned())
    }
}

impl Drop for InProcessTcpHost {
    fn drop(&mut self) {
        self.stop_events.store(true, Ordering::Relaxed);
    }
}

pub async fn serve_in_process(
    listener: TcpListener,
    host: InProcessTcpHost,
    actor: ActorBinding,
    max_connections: Option<usize>,
) -> Result<(), String> {
    let host = Arc::new(host);
    let mut accepted = 0usize;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("failed to accept connection: {error}"))?;
        let host = host.clone();
        let actor = actor.clone();
        compio::runtime::spawn(async move {
            if let Err(error) = handle_connection(stream, host, actor).await {
                eprintln!("connection failed: {error}");
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

impl EndpointOutput {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn send(&self, line: String) -> Result<(), String> {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err("endpoint writer is closed".to_owned());
            }
            state.lines.push_back(line);
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

    fn recv(&self) -> EndpointOutputRecv<'_> {
        EndpointOutputRecv { output: self }
    }

    fn drain_batch(&self, max_lines: usize) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        let count = max_lines.min(state.lines.len());
        let mut lines = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(line) = state.lines.pop_front() else {
                break;
            };
            lines.push(line);
        }
        lines
    }

    #[cfg(test)]
    fn try_recv(&self) -> Option<String> {
        self.state.lock().unwrap().lines.pop_front()
    }
}

impl Future for EndpointOutputRecv<'_> {
    type Output = EndpointOutputReady;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.output.state.lock().unwrap();
        if state.lines.len() >= ENDPOINT_OUTPUT_HIGH_WATER_LINES {
            return Poll::Ready(EndpointOutputReady::HighWater {
                buffered: state.lines.len(),
            });
        }
        if !state.lines.is_empty() {
            return Poll::Ready(EndpointOutputReady::Ready {
                buffered: state.lines.len(),
            });
        }
        if state.closed {
            return Poll::Ready(EndpointOutputReady::Closed);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

async fn handle_connection(
    stream: TcpStream,
    host: Arc<InProcessTcpHost>,
    actor: ActorBinding,
) -> Result<(), String> {
    let endpoint = host.allocate_endpoint()?;
    let output = EndpointOutput::new();
    host.endpoints
        .lock()
        .unwrap()
        .insert(endpoint, output.clone());
    open_endpoint(&host, endpoint, actor.identity)?;

    let (read_half, write_half) = stream.into_split();
    let writer = compio::runtime::spawn(write_socket_loop(write_half, output));
    send_line(&host, endpoint, "Connected to Mica.")?;
    send_line(
        &host,
        endpoint,
        "Try: look, get coin, put coin box, north, say hello, quit.",
    )?;

    let result = read_socket_loop(read_half, &host, endpoint, &actor.name).await;
    let _ = host.driver.close_endpoint(endpoint);
    drop_socket_writer(&host, endpoint);
    let _ = writer.await.resume_unwind();
    result
}

async fn read_socket_loop(
    mut stream: OwnedReadHalf<TcpStream>,
    host: &InProcessTcpHost,
    endpoint: Identity,
    actor_name: &str,
) -> Result<(), String> {
    let mut pending = Vec::new();
    loop {
        start_read_task(host, endpoint)?;
        let line = read_line(&mut stream, &mut pending).await?;
        let Some(line) = line else {
            return Ok(());
        };
        let outcomes = host
            .driver
            .input(endpoint, Value::string(line.clone()))
            .map_err(format_driver_error)?;
        for outcome in outcomes {
            if let TaskOutcome::Complete { value, .. } = outcome {
                let command = value.with_str(str::to_owned).unwrap_or(line.clone());
                if handle_command(host, endpoint, actor_name, &command)? {
                    return Ok(());
                }
            }
        }
    }
}

async fn read_line(
    stream: &mut OwnedReadHalf<TcpStream>,
    pending: &mut Vec<u8>,
) -> Result<Option<String>, String> {
    loop {
        if let Some(index) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=index).collect::<Vec<_>>();
            return String::from_utf8(trim_line_end(&line).to_vec())
                .map(Some)
                .map_err(|error| format!("connection sent invalid UTF-8: {error}"));
        }
        let (result, buffer) = stream.read([0u8; 4096]).await.into();
        let bytes = result.map_err(|error| format!("failed to read from connection: {error}"))?;
        if bytes == 0 {
            if pending.is_empty() {
                return Ok(None);
            }
            let line = String::from_utf8(trim_line_end(pending).to_vec())
                .map_err(|error| format!("connection sent invalid UTF-8: {error}"))?;
            pending.clear();
            return Ok(Some(line));
        }
        pending.extend_from_slice(&buffer[..bytes]);
    }
}

fn trim_line_end(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn start_read_task(host: &InProcessTcpHost, endpoint: Identity) -> Result<(), String> {
    let report = host
        .driver
        .submit_source_report(endpoint, None, "return read(:line)".to_owned())
        .map_err(format_driver_error)?;
    match report.outcome {
        TaskOutcome::Suspended {
            kind: SuspendKind::WaitingForInput(_),
            ..
        } => Ok(()),
        other => Err(format!("read task did not wait for input: {other:?}")),
    }
}

fn handle_command(
    host: &InProcessTcpHost,
    endpoint: Identity,
    actor_name: &str,
    command: &str,
) -> Result<bool, String> {
    if is_quit_command(command) {
        send_line(host, endpoint, "Goodbye.")?;
        return Ok(true);
    }
    let source = command_invocation_source(actor_name, command);
    host.driver
        .submit_source_report(endpoint, None, source)
        .map_err(format_driver_error)?;
    flush_routed_effects(host);
    Ok(false)
}

fn command_invocation_source(actor_name: &str, command: &str) -> String {
    format!(
        ":command(actor: #{actor_name}, endpoint: #endpoint, line: {})",
        mica_string(command)
    )
}

fn is_quit_command(command: &str) -> bool {
    let command = command.trim();
    command.eq_ignore_ascii_case("quit") || command.eq_ignore_ascii_case("exit")
}

fn open_endpoint(
    host: &InProcessTcpHost,
    endpoint: Identity,
    actor: Identity,
) -> Result<(), String> {
    host.driver
        .open_endpoint(endpoint, Some(actor), Symbol::intern("tcp"))
        .map_err(format_driver_error)
}

fn send_line(host: &InProcessTcpHost, endpoint: Identity, line: &str) -> Result<(), String> {
    let sender = host
        .endpoints
        .lock()
        .unwrap()
        .get(&endpoint)
        .cloned()
        .ok_or_else(|| "endpoint is not connected".to_owned())?;
    sender.send(line.to_owned())
}

fn drop_socket_writer(host: &InProcessTcpHost, endpoint: Identity) {
    if let Some(output) = host.endpoints.lock().unwrap().remove(&endpoint) {
        output.close();
    }
}

async fn write_socket_loop(
    mut stream: OwnedWriteHalf<TcpStream>,
    output: Arc<EndpointOutput>,
) -> Result<(), String> {
    while let EndpointOutputReady::Ready { .. } | EndpointOutputReady::HighWater { .. } =
        output.recv().await
    {
        for line in output.drain_batch(ENDPOINT_OUTPUT_DRAIN_LINES) {
            let mut bytes = line.into_bytes();
            bytes.push(b'\n');
            let (result, _) = stream.write_all(bytes).await.into();
            if result.is_err() {
                return shutdown_socket_writer(stream).await;
            }
        }
    }
    shutdown_socket_writer(stream).await
}

async fn shutdown_socket_writer(mut stream: OwnedWriteHalf<TcpStream>) -> Result<(), String> {
    match stream.shutdown().await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotConnected => Ok(()),
        Err(error) if error.kind() == ErrorKind::BrokenPipe => Ok(()),
        Err(error) if error.kind() == ErrorKind::ConnectionReset => Ok(()),
        Err(error) => Err(format!("failed to shut down connection writer: {error}")),
    }
}

fn start_event_pump(
    driver: Arc<CompioTaskDriver>,
    endpoints: Arc<Mutex<BTreeMap<Identity, Arc<EndpointOutput>>>>,
    stop_events: Arc<AtomicBool>,
) {
    compio::runtime::spawn(async move {
        while !stop_events.load(Ordering::Relaxed) {
            let events = driver.wait_events().await;
            for event in events {
                route_driver_event(&endpoints, event);
            }
        }
    })
    .detach();
}

fn flush_routed_effects(host: &InProcessTcpHost) {
    let events = host.driver.drain_events();
    for event in events {
        route_driver_event(&host.endpoints, event);
    }
}

fn route_driver_event(
    endpoints: &Arc<Mutex<BTreeMap<Identity, Arc<EndpointOutput>>>>,
    event: DriverEvent,
) {
    if let DriverEvent::Effect(effect) = event {
        let Some(sender) = endpoints.lock().unwrap().get(&effect.target).cloned() else {
            return;
        };
        let _ = sender.send(effect_text(&effect.value));
    }
}

fn effect_text(value: &Value) -> String {
    value
        .with_str(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn mica_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push(' '),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn format_driver_error(error: mica_driver::DriverError) -> String {
    format!("error: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mica_runtime::SourceRunner;

    #[test]
    fn builds_command_invocation_source() {
        assert_eq!(
            command_invocation_source("alice", "say hello"),
            ":command(actor: #alice, endpoint: #endpoint, line: \"say hello\")"
        );
        assert!(is_quit_command("quit"));
        assert!(is_quit_command("EXIT"));
        assert!(!is_quit_command("look"));
    }

    #[test]
    fn escapes_mica_string_literals() {
        assert_eq!(mica_string("hello"), "\"hello\"");
        assert_eq!(
            mica_string("a \"quoted\" path"),
            "\"a \\\"quoted\\\" path\""
        );
        assert_eq!(mica_string("line\nbreak"), "\"line\\nbreak\"");
    }

    #[test]
    fn endpoint_output_wait_reports_buffer_state_without_dequeueing() {
        let output = EndpointOutput::new();
        output.send("first".to_owned()).unwrap();
        output.send("second".to_owned()).unwrap();

        let ready = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(output.recv());

        assert_eq!(ready, EndpointOutputReady::Ready { buffered: 2 });
        assert_eq!(
            output.drain_batch(ENDPOINT_OUTPUT_DRAIN_LINES),
            vec!["first".to_owned(), "second".to_owned()]
        );

        for index in 0..ENDPOINT_OUTPUT_HIGH_WATER_LINES {
            output.send(format!("line {index}")).unwrap();
        }
        let ready = compio::runtime::Runtime::new()
            .unwrap()
            .block_on(output.recv());

        assert_eq!(
            ready,
            EndpointOutputReady::HighWater {
                buffered: ENDPOINT_OUTPUT_HIGH_WATER_LINES
            }
        );
    }

    #[test]
    fn routed_command_effect_reaches_endpoint_sender() {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(include_str!("../../../examples/mud-core.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../examples/string.mica"))
            .unwrap();
        runner
            .run_filein(include_str!("../../../examples/mud-command-parser.mica"))
            .unwrap();
        let alice = runner.named_identity(Symbol::intern("alice")).unwrap();
        let host =
            InProcessTcpHost::new_without_event_pump(CompioTaskDriver::spawn(runner).unwrap());
        let endpoint = host.allocate_endpoint().unwrap();
        let output = EndpointOutput::new();
        host.endpoints
            .lock()
            .unwrap()
            .insert(endpoint, output.clone());
        open_endpoint(&host, endpoint, alice).unwrap();

        assert!(!handle_command(&host, endpoint, "alice", "look").unwrap());

        let line = output.try_recv().unwrap();
        assert_eq!(
            line,
            "First Room. A coin and a box are here. The only exit is north."
        );
        assert!(!handle_command(&host, endpoint, "alice", "say hello").unwrap());

        let line = output.try_recv().unwrap();
        assert_eq!(line, "hello");
        assert!(!handle_command(&host, endpoint, "alice", "dance").unwrap());

        let line = output.try_recv().unwrap();
        assert_eq!(line, "I do not understand that.");
        let _ = host.driver.close_endpoint(endpoint);
    }

    #[test]
    fn endpoint_read_task_accepts_driver_input() {
        let runner = SourceRunner::new_empty();
        let host =
            InProcessTcpHost::new_without_event_pump(CompioTaskDriver::spawn(runner).unwrap());
        let endpoint = host.allocate_endpoint().unwrap();
        host.endpoints
            .lock()
            .unwrap()
            .insert(endpoint, EndpointOutput::new());
        open_endpoint(&host, endpoint, endpoint).unwrap();

        start_read_task(&host, endpoint).unwrap();
        let outcomes = host.driver.input(endpoint, Value::string("look")).unwrap();

        assert!(matches!(
            outcomes.as_slice(),
            [TaskOutcome::Complete { value, .. }] if *value == Value::string("look")
        ));
        let _ = host.driver.close_endpoint(endpoint);
    }
}
