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
use std::net::SocketAddr;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "mica-web-host", about = "Run a minimal compio HTTP/1.1 host")]
struct Cli {
    #[arg(long, default_value = DEFAULT_BIND)]
    bind: SocketAddr,
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
    let listener = TcpListener::bind(cli.bind)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", cli.bind))?;
    println!(
        "mica-web-host listening on {}",
        listener.local_addr().unwrap()
    );
    serve(listener, None).await
}
