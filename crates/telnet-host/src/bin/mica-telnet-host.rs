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
use compio::net::TcpListener;
use compio::runtime::Runtime;
use mica_telnet_host::{DEFAULT_BIND, ZmqTelnetHost, serve_zmq_telnet};
use std::net::SocketAddr;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "mica-telnet-host",
    about = "Run a telnet host connected to a Mica daemon RPC socket"
)]
struct Cli {
    #[arg(long, default_value = DEFAULT_BIND)]
    bind: SocketAddr,
    #[arg(long, value_name = "URI")]
    rpc: String,
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
    Runtime::new()
        .map_err(|error| format!("failed to start compio runtime: {error}"))?
        .block_on(run_async(cli))
}

async fn run_async(cli: Cli) -> Result<(), String> {
    let actor = actor_name(&cli.actor)?;
    let listener = TcpListener::bind(cli.bind)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", cli.bind))?;
    println!(
        "mica-telnet-host listening on {}, RPC {}",
        listener.local_addr().unwrap(),
        cli.rpc
    );
    serve_zmq_telnet(listener, ZmqTelnetHost::new(cli.rpc), actor, None).await
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
