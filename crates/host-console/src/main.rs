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
use compio::runtime::Runtime;
use mica_host_protocol::{HostMessage, PROTOCOL_VERSION};
use mica_host_zmq::{ZmqHostSocket, ZmqSocketOptions, ZmqTransportError};
use mica_var::{Identity, Symbol, Value};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const CONSOLE_ENDPOINT_PREFIX: u64 = 0x00ef_0000_0000_0000;
const OUTPUT_DRAIN_LIMIT: u32 = 64;

#[derive(Parser)]
#[command(
    name = "mica-host-console",
    about = "Interactive console for the Mica host protocol"
)]
struct Cli {
    #[arg(long, value_name = "URI")]
    rpc: String,
    #[arg(long, default_value = "alice", value_name = "IDENTITY")]
    actor: String,
    #[arg(long, value_name = "IDENTITY")]
    endpoint: Option<String>,
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
    let mut console = HostConsole::connect(&cli.rpc, endpoint_from_cli(cli.endpoint.as_deref())?)
        .await
        .map_err(|error| format!("failed to connect to {}: {error}", cli.rpc))?;
    console.set_actor(actor_name(&cli.actor)?).await?;
    console.open_endpoint().await?;
    repl(console).await
}

async fn repl(mut console: HostConsole) -> Result<(), String> {
    let mut editor = DefaultEditor::new()
        .map_err(|error| format!("failed to initialize host console: {error}"))?;
    println!("Mica host console. Enter /help for commands.");
    console.print_status();
    loop {
        match editor.readline("host> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line.as_str());
                let keep_running = handle_console_line(&mut console, trimmed).await?;
                if !keep_running {
                    return Ok(());
                }
            }
            Err(ReadlineError::Interrupted) => {}
            Err(ReadlineError::Eof) => return Ok(()),
            Err(error) => return Err(format!("readline failed: {error}")),
        }
    }
}

async fn handle_console_line(console: &mut HostConsole, line: &str) -> Result<bool, String> {
    if let Some(command) = line.strip_prefix('/') {
        return handle_slash_command(console, command).await;
    }
    console.submit_command_line(line).await?;
    Ok(true)
}

async fn handle_slash_command(console: &mut HostConsole, command: &str) -> Result<bool, String> {
    let (name, rest) = split_command(command);
    match name {
        "help" | "h" => {
            print_help();
            Ok(true)
        }
        "quit" | "q" | "exit" => {
            let _ = console.close_endpoint().await;
            Ok(false)
        }
        "status" => {
            console.print_status();
            Ok(true)
        }
        "actor" => {
            let actor = required_arg(name, rest)?;
            console.set_actor(actor_name(actor)?).await?;
            println!("actor: #{}", console.actor_name);
            Ok(true)
        }
        "open" => {
            console.open_endpoint().await?;
            println!("opened endpoint {}", display_identity(console.endpoint));
            Ok(true)
        }
        "close" => {
            console.close_endpoint().await?;
            println!("closed endpoint {}", display_identity(console.endpoint));
            Ok(true)
        }
        "source" | "eval" => {
            let source = required_arg(name, rest)?;
            console.submit_source(source.to_owned()).await?;
            console.drain_output().await?;
            Ok(true)
        }
        "drain" => {
            console.drain_output().await?;
            Ok(true)
        }
        "" => Ok(true),
        _ => Err(format!("unknown slash command /{name}; use /help")),
    }
}

struct HostConsole {
    _context: Arc<zmq::Context>,
    socket: ZmqHostSocket,
    rpc: String,
    endpoint: Identity,
    actor: Identity,
    actor_name: String,
    next_request: u64,
    open: bool,
}

impl HostConsole {
    async fn connect(rpc: &str, endpoint: Identity) -> Result<Self, String> {
        let context = Arc::new(zmq::Context::new());
        let socket =
            ZmqHostSocket::connect(&context, zmq::DEALER, rpc, ZmqSocketOptions::default())
                .map_err(format_zmq_error)?;
        let mut console = Self {
            _context: context,
            socket,
            rpc: rpc.to_owned(),
            endpoint,
            actor: endpoint,
            actor_name: "<unresolved>".to_owned(),
            next_request: 1,
            open: false,
        };
        console.hello().await?;
        Ok(console)
    }

    async fn hello(&mut self) -> Result<(), String> {
        self.socket
            .send_message(&HostMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                min_protocol_version: PROTOCOL_VERSION,
                feature_bits: 0,
                host_name: "mica-host-console".to_owned(),
            })
            .await
            .map_err(format_zmq_error)?;
        match self.socket.recv_message().await.map_err(format_zmq_error)? {
            HostMessage::HelloAck { .. } => Ok(()),
            other => Err(format!("unexpected hello response: {other:?}")),
        }
    }

    async fn set_actor(&mut self, actor_name: String) -> Result<(), String> {
        let request_id = self.next_request_id();
        let messages = self
            .request(HostMessage::ResolveIdentity {
                request_id,
                name: Symbol::intern(&actor_name),
            })
            .await?;
        for message in messages {
            match message {
                HostMessage::IdentityResolved {
                    request_id: actual,
                    identity,
                    ..
                } if actual == request_id => {
                    self.actor = identity;
                    self.actor_name = actor_name;
                    return Ok(());
                }
                HostMessage::RequestRejected {
                    request_id: actual,
                    code,
                    message,
                } if actual == request_id => {
                    return Err(format!(
                        "actor resolution rejected with {}: {message}",
                        symbol_name(code)
                    ));
                }
                other => self.route_message(other).await?,
            }
        }
        Err(format!(
            "actor resolution request {request_id} had no reply"
        ))
    }

    async fn open_endpoint(&mut self) -> Result<(), String> {
        if self.open {
            return Ok(());
        }
        let request_id = self.next_request_id();
        let messages = self
            .request(HostMessage::OpenEndpoint {
                request_id,
                endpoint: self.endpoint,
                actor: Some(self.actor),
                protocol: "console".to_owned(),
                grant_token: None,
            })
            .await?;
        self.expect_accepted(request_id, messages).await?;
        self.open = true;
        Ok(())
    }

    async fn close_endpoint(&mut self) -> Result<(), String> {
        if !self.open {
            return Ok(());
        }
        let request_id = self.next_request_id();
        let messages = self
            .request(HostMessage::CloseEndpoint {
                request_id,
                endpoint: self.endpoint,
            })
            .await?;
        self.expect_accepted(request_id, messages).await?;
        self.open = false;
        Ok(())
    }

    async fn submit_command_line(&mut self, line: &str) -> Result<(), String> {
        self.open_endpoint().await?;
        self.submit_source("return read(:line)".to_owned()).await?;
        let values = self.submit_input(line.to_owned()).await?;
        for value in values {
            let command = value
                .with_str(str::to_owned)
                .unwrap_or_else(|| line.to_owned());
            if is_quit_command(&command) {
                println!("Goodbye.");
                let _ = self.close_endpoint().await;
                continue;
            }
            self.submit_source(command_invocation_source(&self.actor_name, &command))
                .await?;
        }
        self.drain_output().await?;
        Ok(())
    }

    async fn submit_input(&mut self, input: String) -> Result<Vec<Value>, String> {
        self.open_endpoint().await?;
        let request_id = self.next_request_id();
        let messages = self
            .request(HostMessage::SubmitInput {
                request_id,
                endpoint: self.endpoint,
                value: Value::string(input),
            })
            .await?;
        self.expect_accepted_collecting_completions(request_id, messages)
            .await
    }

    async fn submit_source(&mut self, source: String) -> Result<Vec<Value>, String> {
        self.open_endpoint().await?;
        let request_id = self.next_request_id();
        let messages = self
            .request(HostMessage::SubmitSource {
                request_id,
                endpoint: self.endpoint,
                actor: self.actor,
                source,
            })
            .await?;
        self.expect_accepted_collecting_completions(request_id, messages)
            .await
    }

    async fn drain_output(&mut self) -> Result<(), String> {
        if !self.open {
            return Ok(());
        }
        for _ in 0..4 {
            let request_id = self.next_request_id();
            let messages = self
                .request(HostMessage::DrainOutput {
                    request_id,
                    endpoint: self.endpoint,
                    limit: OUTPUT_DRAIN_LIMIT,
                })
                .await?;
            let mut saw_output_ready = false;
            for message in messages {
                match message {
                    HostMessage::RequestAccepted { .. } => {}
                    HostMessage::RequestRejected {
                        request_id: actual,
                        code,
                        message,
                    } if actual == request_id => {
                        return Err(format!(
                            "output drain rejected with {}: {message}",
                            symbol_name(code)
                        ));
                    }
                    HostMessage::OutputReady { endpoint, .. } if endpoint == self.endpoint => {
                        saw_output_ready = true;
                    }
                    other => self.route_message(other).await?,
                }
            }
            if !saw_output_ready {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn request(&mut self, message: HostMessage) -> Result<Vec<HostMessage>, String> {
        let request_id = request_id_for(&message)
            .ok_or_else(|| format!("message is not a request: {message:?}"))?;
        self.socket
            .send_message(&message)
            .await
            .map_err(format_zmq_error)?;
        let mut messages = Vec::new();
        loop {
            let message = self.socket.recv_message().await.map_err(format_zmq_error)?;
            let terminal = is_terminal_response(request_id, &message);
            messages.push(message);
            if terminal {
                break;
            }
        }
        while let Some(message) = self.socket.try_recv_message().map_err(format_zmq_error)? {
            messages.push(message);
        }
        Ok(messages)
    }

    async fn expect_accepted(
        &mut self,
        request_id: u64,
        messages: Vec<HostMessage>,
    ) -> Result<(), String> {
        self.expect_accepted_collecting_completions(request_id, messages)
            .await
            .map(|_| ())
    }

    async fn expect_accepted_collecting_completions(
        &mut self,
        request_id: u64,
        messages: Vec<HostMessage>,
    ) -> Result<Vec<Value>, String> {
        let mut accepted = false;
        let mut completed = Vec::new();
        for message in messages {
            match message {
                HostMessage::RequestAccepted {
                    request_id: actual, ..
                } if actual == request_id => accepted = true,
                HostMessage::RequestRejected {
                    request_id: actual,
                    code,
                    message,
                } if actual == request_id => {
                    return Err(format!(
                        "request rejected with {}: {message}",
                        symbol_name(code)
                    ));
                }
                HostMessage::TaskCompleted { value, .. } => completed.push(value),
                other => self.route_message(other).await?,
            }
        }
        if accepted {
            return Ok(completed);
        }
        Err(format!(
            "request {request_id} did not receive an accepted reply"
        ))
    }

    async fn route_message(&mut self, message: HostMessage) -> Result<(), String> {
        match message {
            HostMessage::OutputReady { .. } => Ok(()),
            HostMessage::OutputBatch { endpoint, values } if endpoint == self.endpoint => {
                for value in values {
                    println!("{value}");
                }
                Ok(())
            }
            HostMessage::TaskCompleted { value, .. } => {
                println!("task complete: {value}");
                Ok(())
            }
            HostMessage::TaskFailed { error, .. } => {
                eprintln!("task failed: {error}");
                Ok(())
            }
            HostMessage::EndpointClosed { endpoint, reason } if endpoint == self.endpoint => {
                self.open = false;
                println!("endpoint closed: {reason}");
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn print_status(&self) {
        println!(
            "rpc: {}; endpoint: {}; actor: #{}; open: {}",
            self.rpc,
            display_identity(self.endpoint),
            self.actor_name,
            self.open
        );
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_request;
        self.next_request = self.next_request.saturating_add(1);
        request_id
    }
}

fn request_id_for(message: &HostMessage) -> Option<u64> {
    match message {
        HostMessage::OpenEndpoint { request_id, .. }
        | HostMessage::CloseEndpoint { request_id, .. }
        | HostMessage::ResolveIdentity { request_id, .. }
        | HostMessage::SubmitSource { request_id, .. }
        | HostMessage::SubmitInput { request_id, .. }
        | HostMessage::DrainOutput { request_id, .. } => Some(*request_id),
        _ => None,
    }
}

fn is_terminal_response(request_id: u64, message: &HostMessage) -> bool {
    match message {
        HostMessage::RequestAccepted {
            request_id: actual, ..
        }
        | HostMessage::RequestRejected {
            request_id: actual, ..
        }
        | HostMessage::IdentityResolved {
            request_id: actual, ..
        } => *actual == request_id,
        _ => false,
    }
}

fn command_invocation_source(actor_name: &str, command: &str) -> String {
    format!(
        ":command(actor: #{actor_name}, endpoint: endpoint(), line: {})",
        mica_string(command)
    )
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
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn is_quit_command(command: &str) -> bool {
    let command = command.trim();
    command.eq_ignore_ascii_case("quit") || command.eq_ignore_ascii_case("exit")
}

fn split_command(command: &str) -> (&str, &str) {
    let command = command.trim();
    let Some((name, rest)) = command.split_once(char::is_whitespace) else {
        return (command, "");
    };
    (name, rest.trim())
}

fn required_arg<'a>(command: &str, arg: &'a str) -> Result<&'a str, String> {
    if arg.is_empty() {
        return Err(format!("/{command} requires an argument"));
    }
    Ok(arg)
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

fn endpoint_from_cli(endpoint: Option<&str>) -> Result<Identity, String> {
    let Some(endpoint) = endpoint else {
        return generated_endpoint();
    };
    let endpoint = endpoint.trim().trim_start_matches('#');
    let raw = endpoint
        .parse::<u64>()
        .map_err(|error| format!("endpoint must be a numeric identity: {error}"))?;
    Identity::new(raw).ok_or_else(|| "endpoint identity is out of range".to_owned())
}

fn generated_endpoint() -> Result<Identity, String> {
    let pid = u64::from(std::process::id()) & 0xffff;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before unix epoch: {error}"))?
        .subsec_nanos();
    let raw = CONSOLE_ENDPOINT_PREFIX | (pid << 32) | u64::from(nanos);
    Identity::new(raw).ok_or_else(|| "generated endpoint identity is out of range".to_owned())
}

fn display_identity(identity: Identity) -> String {
    format!("#{}", identity.raw())
}

fn symbol_name(symbol: Symbol) -> String {
    symbol.name().unwrap_or("<unnamed>").to_owned()
}

fn format_zmq_error(error: ZmqTransportError) -> String {
    error.to_string()
}

fn print_help() {
    println!(
        "\
/help              show this help
/status            show current RPC endpoint, actor, and host endpoint
/actor NAME        resolve and select an actor identity
/open              open the current host endpoint
/close             close the current host endpoint
/source SOURCE     submit raw Mica source as the selected actor
/drain             drain queued output for the current endpoint
/quit              close the endpoint and exit

Plain text lines run through the same read(:line) and :command(...) path as telnet input."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_command_separates_name_and_trimmed_rest() {
        assert_eq!(split_command("source  1 + 1"), ("source", "1 + 1"));
        assert_eq!(split_command("help"), ("help", ""));
        assert_eq!(split_command("  actor   alice  "), ("actor", "alice"));
    }

    #[test]
    fn mica_string_escapes_source_literal_contents() {
        assert_eq!(mica_string("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
    }

    #[test]
    fn actor_names_reject_source_like_values() {
        assert_eq!(actor_name("#alice").unwrap(), "alice");
        assert!(actor_name("1alice").is_err());
        assert!(actor_name("alice-bob").is_err());
    }
}
