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
use mica_web_host::{DEFAULT_BIND, serve};
use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "mica-web-host", about = "Run a minimal compio HTTP/1.1 host")]
struct Cli {
    #[arg(long, default_value = DEFAULT_BIND)]
    bind: SocketAddr,
    #[arg(long, value_name = "FILTER")]
    log_filter: Option<String>,
    #[arg(long)]
    no_log_ansi: bool,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "mica-web-host stopped");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    init_tracing(&cli);
    Runtime::new()
        .map_err(|error| format!("failed to start compio runtime: {error}"))?
        .block_on(run_async(cli))
}

fn init_tracing(cli: &Cli) {
    let filter = cli
        .log_filter
        .clone()
        .or_else(|| env::var("MICA_LOG_FILTER").ok())
        .unwrap_or_else(|| "info".to_owned());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_log::LogTracer::init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(!cli.no_log_ansi)
        .try_init();
}

async fn run_async(cli: Cli) -> Result<(), String> {
    let listener = TcpListener::bind(cli.bind)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", cli.bind))?;
    let local_addr = listener.local_addr().unwrap();
    tracing::info!(bind = %cli.bind, local_addr = %local_addr, "HTTP listener started");
    serve(listener, None).await
}
