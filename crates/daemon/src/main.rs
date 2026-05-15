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

use clap::Parser;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
use compio::runtime::{ResumeUnwind, Runtime};
use compio::time::sleep;
use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_runtime::{SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DEFAULT_BIND: &str = "127.0.0.1:7777";
const DEFAULT_FILEINS: &[&str] = &[
    "examples/mud-core.mica",
    "examples/string.mica",
    "examples/mud-command-parser.mica",
];
const DAEMON_ENDPOINT_ID_START: u64 = 0x00ed_0000_0000_0000;
const EVENT_POLL_DELAY: Duration = Duration::from_millis(10);

#[derive(Parser)]
#[command(
    name = "mica-daemon",
    about = "Run a minimal TCP endpoint transport for Mica"
)]
struct Cli {
    #[arg(long, default_value = DEFAULT_BIND)]
    bind: SocketAddr,
    #[arg(long = "filein", value_name = "FILE")]
    fileins: Vec<PathBuf>,
    #[arg(long, default_value = "alice", value_name = "IDENTITY")]
    actor: String,
    #[arg(long, value_name = "THREADS")]
    driver_threads: Option<NonZeroUsize>,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    Runtime::new()
        .map_err(|error| format!("failed to start compio runtime: {error}"))?
        .block_on(run_async(cli))
}

async fn run_async(cli: Cli) -> Result<(), String> {
    let mut runner = SourceRunner::new_empty();
    for filein in fileins_or_defaults(&cli.fileins) {
        let source = fs::read_to_string(&filein)
            .map_err(|error| format!("failed to read {}: {error}", filein.display()))?;
        runner.run_filein(&source).map_err(format_source_error)?;
    }
    let actor_name = actor_name(&cli.actor)?;
    let actor_symbol = Symbol::intern(&actor_name);
    let actor = runner
        .named_identity(actor_symbol)
        .map_err(format_source_error)?;
    let listener = TcpListener::bind(cli.bind)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", cli.bind))?;
    println!(
        "mica-daemon listening on {}",
        listener.local_addr().unwrap()
    );
    let state = ServerState::new(
        CompioTaskDriver::spawn_with_workers(runner, cli.driver_threads)
            .map_err(format_driver_error)?,
    );
    serve(
        listener,
        state,
        ActorBinding {
            name: actor_name,
            identity: actor,
        },
        None,
    )
    .await
}

fn fileins_or_defaults(fileins: &[PathBuf]) -> Vec<PathBuf> {
    if fileins.is_empty() {
        return DEFAULT_FILEINS.iter().map(PathBuf::from).collect();
    }
    fileins.to_vec()
}

#[derive(Clone, Debug)]
struct ActorBinding {
    name: String,
    identity: Identity,
}

struct ServerState {
    driver: Arc<CompioTaskDriver>,
    endpoints: Arc<Mutex<BTreeMap<Identity, mpsc::Sender<String>>>>,
    stop_events: Arc<AtomicBool>,
    next_endpoint: AtomicU64,
}

impl ServerState {
    fn new(driver: CompioTaskDriver) -> Self {
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

impl Drop for ServerState {
    fn drop(&mut self) {
        self.stop_events.store(true, Ordering::Relaxed);
    }
}

async fn serve(
    listener: TcpListener,
    state: ServerState,
    actor: ActorBinding,
    max_connections: Option<usize>,
) -> Result<(), String> {
    let state = Arc::new(state);
    let mut accepted = 0usize;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("failed to accept connection: {error}"))?;
        let state = state.clone();
        let actor = actor.clone();
        compio::runtime::spawn(async move {
            if let Err(error) = handle_connection(stream, state, actor).await {
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

async fn handle_connection(
    stream: TcpStream,
    state: Arc<ServerState>,
    actor: ActorBinding,
) -> Result<(), String> {
    let endpoint = state.allocate_endpoint()?;
    let (out_tx, out_rx) = mpsc::channel();
    state.endpoints.lock().unwrap().insert(endpoint, out_tx);
    open_endpoint(&state, endpoint, actor.identity)?;

    let (read_half, write_half) = stream.into_split();
    let writer = compio::runtime::spawn(write_socket_loop(write_half, out_rx));
    send_line(&state, endpoint, "Connected to Mica.")?;
    send_line(
        &state,
        endpoint,
        "Try: look, get coin, put coin box, north, say hello, quit.",
    )?;

    let result = read_socket_loop(read_half, &state, endpoint, &actor.name).await;
    state.endpoints.lock().unwrap().remove(&endpoint);
    let _ = state.driver.close_endpoint(endpoint);
    drop_socket_writer(&state, endpoint);
    let _ = writer.await.resume_unwind();
    result
}

async fn read_socket_loop(
    mut stream: OwnedReadHalf<TcpStream>,
    state: &ServerState,
    endpoint: Identity,
    actor_name: &str,
) -> Result<(), String> {
    let mut pending = Vec::new();
    loop {
        start_read_task(state, endpoint)?;
        let line = read_line(&mut stream, &mut pending).await?;
        let Some(line) = line else {
            return Ok(());
        };
        let outcomes = state
            .driver
            .input(endpoint, Value::string(line.clone()))
            .map_err(format_driver_error)?;
        for outcome in outcomes {
            if let TaskOutcome::Complete { value, .. } = outcome {
                let command = value.with_str(str::to_owned).unwrap_or(line.clone());
                if handle_command(state, endpoint, actor_name, &command)? {
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

fn start_read_task(state: &ServerState, endpoint: Identity) -> Result<(), String> {
    let report = state
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
    state: &ServerState,
    endpoint: Identity,
    actor_name: &str,
    command: &str,
) -> Result<bool, String> {
    if is_quit_command(command) {
        send_line(state, endpoint, "Goodbye.")?;
        return Ok(true);
    }
    let source = command_invocation_source(actor_name, command);
    state
        .driver
        .submit_source_report(endpoint, None, source)
        .map_err(format_driver_error)?;
    flush_routed_effects(state)?;
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

fn open_endpoint(state: &ServerState, endpoint: Identity, actor: Identity) -> Result<(), String> {
    state
        .driver
        .open_endpoint(endpoint, Some(actor), Symbol::intern("tcp"))
        .map_err(format_driver_error)
}

fn send_line(state: &ServerState, endpoint: Identity, line: &str) -> Result<(), String> {
    let sender = state
        .endpoints
        .lock()
        .unwrap()
        .get(&endpoint)
        .cloned()
        .ok_or_else(|| "endpoint is not connected".to_owned())?;
    sender
        .send(line.to_owned())
        .map_err(|_| "endpoint writer is closed".to_owned())
}

fn drop_socket_writer(state: &ServerState, endpoint: Identity) {
    state.endpoints.lock().unwrap().remove(&endpoint);
}

async fn write_socket_loop(
    mut stream: OwnedWriteHalf<TcpStream>,
    rx: mpsc::Receiver<String>,
) -> Result<(), String> {
    loop {
        match rx.try_recv() {
            Ok(line) => {
                let mut bytes = line.into_bytes();
                bytes.push(b'\n');
                let (result, _) = stream.write_all(bytes).await.into();
                if result.is_err() {
                    break;
                }
            }
            Err(mpsc::TryRecvError::Empty) => sleep(EVENT_POLL_DELAY).await,
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
    }
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
    endpoints: Arc<Mutex<BTreeMap<Identity, mpsc::Sender<String>>>>,
    stop_events: Arc<AtomicBool>,
) {
    compio::runtime::spawn(async move {
        while !stop_events.load(Ordering::Relaxed) {
            let events = driver.drain_events();
            for event in events {
                route_driver_event(&endpoints, event);
            }
            sleep(EVENT_POLL_DELAY).await;
        }
    })
    .detach();
}

fn flush_routed_effects(state: &ServerState) -> Result<(), String> {
    let events = state.driver.drain_events();
    for event in events {
        route_driver_event(&state.endpoints, event);
    }
    Ok(())
}

fn route_driver_event(
    endpoints: &Arc<Mutex<BTreeMap<Identity, mpsc::Sender<String>>>>,
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

fn actor_name(actor: &str) -> Result<String, String> {
    let actor = actor.trim().trim_start_matches('#').trim_start_matches(':');
    if actor.is_empty()
        || !actor
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || actor.chars().next().is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err("actor must be a named identity such as alice or #alice".to_owned());
    }
    Ok(actor.to_owned())
}

fn format_source_error(error: mica_runtime::SourceTaskError) -> String {
    format!("error: {error:?}")
}

fn format_driver_error(error: mica_driver::DriverError) -> String {
    format!("error: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let state = ServerState::new_without_event_pump(CompioTaskDriver::spawn(runner).unwrap());
        let endpoint = state.allocate_endpoint().unwrap();
        let (tx, rx) = mpsc::channel();
        state.endpoints.lock().unwrap().insert(endpoint, tx);
        open_endpoint(&state, endpoint, alice).unwrap();

        assert!(!handle_command(&state, endpoint, "alice", "look").unwrap());

        let line = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            line,
            "First Room. A coin and a box are here. The only exit is north."
        );
        assert!(!handle_command(&state, endpoint, "alice", "say hello").unwrap());

        let line = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(line, "hello");
        assert!(!handle_command(&state, endpoint, "alice", "dance").unwrap());

        let line = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(line, "I do not understand that.");
        let _ = state.driver.close_endpoint(endpoint);
    }

    #[test]
    fn endpoint_read_task_accepts_driver_input() {
        let runner = SourceRunner::new_empty();
        let state = ServerState::new_without_event_pump(CompioTaskDriver::spawn(runner).unwrap());
        let endpoint = state.allocate_endpoint().unwrap();
        let (tx, _rx) = mpsc::channel();
        state.endpoints.lock().unwrap().insert(endpoint, tx);
        open_endpoint(&state, endpoint, endpoint).unwrap();

        start_read_task(&state, endpoint).unwrap();
        let outcomes = state.driver.input(endpoint, Value::string("look")).unwrap();

        assert!(matches!(
            outcomes.as_slice(),
            [TaskOutcome::Complete { value, .. }] if *value == Value::string("look")
        ));
        let _ = state.driver.close_endpoint(endpoint);
    }
}
