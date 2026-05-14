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
use mica_driver::{CompioTaskDriverThread, DriverEvent};
use mica_runtime::{SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_BIND: &str = "127.0.0.1:7777";
const DEFAULT_FILEIN: &str = "examples/mud-core.mica";
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
    #[arg(long, default_value = DEFAULT_FILEIN)]
    filein: PathBuf,
    #[arg(long, default_value = "alice", value_name = "IDENTITY")]
    actor: String,
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
    let source = fs::read_to_string(&cli.filein)
        .map_err(|error| format!("failed to read {}: {error}", cli.filein.display()))?;
    let mut runner = SourceRunner::new_empty();
    runner.run_filein(&source).map_err(format_source_error)?;
    let actor_name = actor_name(&cli.actor)?;
    let actor_symbol = Symbol::intern(&actor_name);
    let actor = runner
        .named_identity(actor_symbol)
        .map_err(format_source_error)?;
    let listener = TcpListener::bind(cli.bind)
        .map_err(|error| format!("failed to bind {}: {error}", cli.bind))?;
    println!(
        "mica-daemon listening on {}",
        listener.local_addr().unwrap()
    );
    let state =
        ServerState::new(CompioTaskDriverThread::spawn(runner).map_err(format_driver_error)?);
    serve(
        listener,
        state,
        ActorBinding {
            name: actor_name,
            identity: actor,
        },
        None,
    )
}

#[derive(Clone, Debug)]
struct ActorBinding {
    name: String,
    identity: Identity,
}

struct ServerState {
    driver: Arc<Mutex<CompioTaskDriverThread>>,
    endpoints: Arc<Mutex<BTreeMap<Identity, mpsc::Sender<String>>>>,
    stop_events: mpsc::Sender<()>,
    next_endpoint: AtomicU64,
}

impl ServerState {
    fn new(driver: CompioTaskDriverThread) -> Self {
        let driver = Arc::new(Mutex::new(driver));
        let endpoints = Arc::new(Mutex::new(BTreeMap::new()));
        let (stop_events, stop_rx) = mpsc::channel();
        start_event_pump(driver.clone(), endpoints.clone(), stop_rx);
        Self {
            driver,
            endpoints,
            stop_events,
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
        let _ = self.stop_events.send(());
    }
}

fn serve(
    listener: TcpListener,
    state: ServerState,
    actor: ActorBinding,
    max_connections: Option<usize>,
) -> Result<(), String> {
    let state = Arc::new(state);
    for (accepted, stream) in listener.incoming().enumerate() {
        let stream = stream.map_err(|error| format!("failed to accept connection: {error}"))?;
        let state = state.clone();
        let actor = actor.clone();
        thread::spawn(move || {
            if let Err(error) = handle_connection(stream, state, actor) {
                eprintln!("connection failed: {error}");
            }
        });
        if max_connections.is_some_and(|max| accepted + 1 >= max) {
            break;
        }
    }
    Ok(())
}

fn handle_connection(
    stream: TcpStream,
    state: Arc<ServerState>,
    actor: ActorBinding,
) -> Result<(), String> {
    let endpoint = state.allocate_endpoint()?;
    let (out_tx, out_rx) = mpsc::channel();
    state.endpoints.lock().unwrap().insert(endpoint, out_tx);
    open_endpoint(&state, endpoint, actor.identity)?;

    let writer = stream
        .try_clone()
        .map_err(|error| format!("failed to clone connection stream: {error}"))?;
    let writer = thread::spawn(move || write_socket_loop(writer, out_rx));
    send_line(&state, endpoint, "Connected to Mica.")?;
    send_line(
        &state,
        endpoint,
        "Try: look, get coin, put coin box, north, say hello, quit.",
    )?;

    let result = read_socket_loop(stream, &state, endpoint, &actor.name);
    state.endpoints.lock().unwrap().remove(&endpoint);
    let _ = state.driver.lock().unwrap().close_endpoint(endpoint);
    drop_socket_writer(&state, endpoint);
    let _ = writer.join();
    result
}

fn read_socket_loop(
    stream: TcpStream,
    state: &ServerState,
    endpoint: Identity,
    actor_name: &str,
) -> Result<(), String> {
    let mut reader = BufReader::new(stream);
    loop {
        start_read_task(state, endpoint)?;
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| format!("failed to read from connection: {error}"))?;
        if bytes == 0 {
            return Ok(());
        }
        let line = line.trim_end_matches(['\r', '\n']).trim().to_owned();
        let outcomes = state
            .driver
            .lock()
            .unwrap()
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

fn start_read_task(state: &ServerState, endpoint: Identity) -> Result<(), String> {
    let report = state
        .driver
        .lock()
        .unwrap()
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
    let Some(source) = command_source(command) else {
        send_line(state, endpoint, "I do not understand that.")?;
        return Ok(false);
    };
    if source == CommandSource::Quit {
        send_line(state, endpoint, "Goodbye.")?;
        return Ok(true);
    }
    let source = source.into_mica_source(actor_name);
    state
        .driver
        .lock()
        .unwrap()
        .submit_source_report(endpoint, None, source)
        .map_err(format_driver_error)?;
    flush_routed_effects(state)?;
    Ok(false)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommandSource {
    Look,
    North,
    GetCoin,
    PutCoinBox,
    Say(String),
    Quit,
}

impl CommandSource {
    fn into_mica_source(self, actor_name: &str) -> String {
        let actor = format!("#{actor_name}");
        match self {
            Self::Look => format!(":look(actor: {actor})"),
            Self::North => format!(":north(actor: {actor})"),
            Self::GetCoin => format!(":get(actor: {actor}, item: #coin)"),
            Self::PutCoinBox => format!(":put(actor: {actor}, item: #coin, container: #box)"),
            Self::Say(message) => {
                format!(":say(actor: {actor}, message: {})", mica_string(&message))
            }
            Self::Quit => unreachable!("quit is handled before source generation"),
        }
    }
}

fn command_source(command: &str) -> Option<CommandSource> {
    let command = command.trim();
    if command.is_empty() {
        return None;
    }
    let lower = command.to_ascii_lowercase();
    match lower.as_str() {
        "look" | "l" => Some(CommandSource::Look),
        "north" | "n" => Some(CommandSource::North),
        "get coin" | "take coin" => Some(CommandSource::GetCoin),
        "put coin box" | "put coin in box" => Some(CommandSource::PutCoinBox),
        "quit" | "exit" => Some(CommandSource::Quit),
        _ => lower
            .strip_prefix("say ")
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .map(|message| CommandSource::Say(message.to_owned())),
    }
}

fn open_endpoint(state: &ServerState, endpoint: Identity, actor: Identity) -> Result<(), String> {
    state
        .driver
        .lock()
        .unwrap()
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

fn write_socket_loop(mut stream: TcpStream, rx: mpsc::Receiver<String>) {
    for line in rx {
        if writeln!(stream, "{line}").is_err() {
            break;
        }
        let _ = stream.flush();
    }
}

fn start_event_pump(
    driver: Arc<Mutex<CompioTaskDriverThread>>,
    endpoints: Arc<Mutex<BTreeMap<Identity, mpsc::Sender<String>>>>,
    stop_rx: mpsc::Receiver<()>,
) {
    thread::spawn(move || {
        while stop_rx.try_recv().is_err() {
            let events = match driver.lock().unwrap().drain_events() {
                Ok(events) => events,
                Err(error) => {
                    eprintln!("{}", format_driver_error(error));
                    Vec::new()
                }
            };
            for event in events {
                route_driver_event(&endpoints, event);
            }
            thread::sleep(EVENT_POLL_DELAY);
        }
    });
}

fn flush_routed_effects(state: &ServerState) -> Result<(), String> {
    let events = state
        .driver
        .lock()
        .unwrap()
        .drain_events()
        .map_err(format_driver_error)?;
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

fn format_driver_error(error: mica_driver::DriverThreadError) -> String {
    format!("error: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_command_sources() {
        assert_eq!(command_source("look"), Some(CommandSource::Look));
        assert_eq!(command_source("n"), Some(CommandSource::North));
        assert_eq!(command_source("get coin"), Some(CommandSource::GetCoin));
        assert_eq!(
            command_source("put coin in box"),
            Some(CommandSource::PutCoinBox)
        );
        assert_eq!(
            command_source("say hello"),
            Some(CommandSource::Say("hello".to_owned()))
        );
        assert_eq!(command_source("quit"), Some(CommandSource::Quit));
        assert_eq!(command_source(""), None);
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
        let alice = runner.named_identity(Symbol::intern("alice")).unwrap();
        let state = ServerState::new(CompioTaskDriverThread::spawn(runner).unwrap());
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
        let _ = state.driver.lock().unwrap().close_endpoint(endpoint);
    }

    #[test]
    fn endpoint_read_task_accepts_driver_input() {
        let runner = SourceRunner::new_empty();
        let state = ServerState::new(CompioTaskDriverThread::spawn(runner).unwrap());
        let endpoint = state.allocate_endpoint().unwrap();
        let (tx, _rx) = mpsc::channel();
        state.endpoints.lock().unwrap().insert(endpoint, tx);
        open_endpoint(&state, endpoint, endpoint).unwrap();

        start_read_task(&state, endpoint).unwrap();
        let outcomes = state
            .driver
            .lock()
            .unwrap()
            .input(endpoint, Value::string("look"))
            .unwrap();

        assert!(matches!(
            outcomes.as_slice(),
            [TaskOutcome::Complete { value, .. }] if *value == Value::string("look")
        ));
        let _ = state.driver.lock().unwrap().close_endpoint(endpoint);
    }
}
