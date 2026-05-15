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
use mica_driver::CompioTaskDriver;
use mica_host_zmq::{ZmqHostSocket, ZmqSocketOptions};
use mica_runtime::SourceRunner;
use mica_telnet_host::{ActorBinding, InProcessTelnetHost, serve_in_process};
use mica_var::Symbol;
use std::fs;
use std::future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;

#[allow(dead_code)]
mod rpc;

const DEFAULT_FILEINS: &[&str] = &[
    "examples/mud-core.mica",
    "examples/string.mica",
    "examples/mud-command-parser.mica",
];

#[derive(Parser)]
#[command(
    name = "mica-daemon",
    about = "Run a Mica daemon with optional host endpoints"
)]
struct Cli {
    #[arg(long = "filein", value_name = "FILE")]
    fileins: Vec<PathBuf>,
    #[arg(long, default_value = "alice", value_name = "IDENTITY")]
    actor: String,
    #[arg(long, value_name = "THREADS")]
    driver_threads: Option<NonZeroUsize>,
    #[arg(long, value_name = "URI")]
    rpc_bind: Option<String>,
    #[arg(long, value_name = "ADDR")]
    telnet_bind: Option<SocketAddr>,
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
    if cli.rpc_bind.is_none() && cli.telnet_bind.is_none() {
        return Err(
            "daemon needs at least one endpoint: use --rpc-bind or --telnet-bind".to_owned(),
        );
    }
    let mut runner = SourceRunner::new_empty();
    for filein in fileins_or_defaults(&cli.fileins) {
        let source = fs::read_to_string(&filein)
            .map_err(|error| format!("failed to read {}: {error}", filein.display()))?;
        runner.run_filein(&source).map_err(format_source_error)?;
    }
    let in_process_actor = if cli.telnet_bind.is_some() {
        let actor_name = actor_name(&cli.actor)?;
        let actor = runner
            .named_identity(Symbol::intern(&actor_name))
            .map_err(format_source_error)?;
        Some(ActorBinding {
            name: actor_name,
            identity: actor,
        })
    } else {
        None
    };
    let driver = CompioTaskDriver::spawn_with_workers(runner, cli.driver_threads)
        .map_err(format_driver_error)?;
    if let Some(rpc_bind) = cli.rpc_bind {
        start_rpc_server(driver.clone(), rpc_bind)?;
    }
    if let Some(telnet_bind) = cli.telnet_bind {
        let actor = in_process_actor.expect("telnet actor should be resolved before driver spawn");
        let listener = TcpListener::bind(telnet_bind)
            .await
            .map_err(|error| format!("failed to bind telnet listener {telnet_bind}: {error}"))?;
        println!(
            "mica-daemon telnet listening on {}",
            listener.local_addr().unwrap()
        );
        return serve_in_process(listener, InProcessTelnetHost::new(driver), actor, None).await;
    }
    future::pending::<()>().await;
    Ok(())
}

fn start_rpc_server(driver: CompioTaskDriver, endpoint: String) -> Result<(), String> {
    let context = zmq::Context::new();
    let socket = ZmqHostSocket::bind(
        &context,
        zmq::ROUTER,
        &endpoint,
        ZmqSocketOptions::default(),
    )
    .map_err(|error| format!("failed to bind RPC socket {endpoint}: {error}"))?;
    println!("mica-daemon RPC listening on {endpoint}");
    compio::runtime::spawn(async move {
        let _context = context;
        let mut handler = rpc::RpcHandler::new(driver);
        if let Err(error) = rpc::serve_zmq_rpc_forever(&socket, &mut handler).await {
            eprintln!("RPC server failed: {error}");
        }
    })
    .detach();
    Ok(())
}

fn fileins_or_defaults(fileins: &[PathBuf]) -> Vec<PathBuf> {
    if fileins.is_empty() {
        return DEFAULT_FILEINS.iter().map(PathBuf::from).collect();
    }
    fileins.to_vec()
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
