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

use crate::codec::{HttpCodec, HttpResponse, encode_response};
use crate::request::handle_in_process_request;
use crate::response::{error_response, route_request};
use crate::{ActorBinding, InProcessWebHost, format_driver_error};
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};
use mica_var::Symbol;
use std::sync::Arc;

pub async fn serve(listener: TcpListener, max_connections: Option<usize>) -> Result<(), String> {
    let mut accepted = 0usize;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|error| format!("failed to accept connection: {error}"))?;
        compio::runtime::spawn(async move {
            if let Err(error) = handle_connection(stream).await {
                eprintln!("HTTP connection failed: {error}");
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

pub async fn serve_in_process(
    listener: TcpListener,
    host: InProcessWebHost,
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
            if let Err(error) = handle_in_process_connection(stream, host, actor).await {
                eprintln!("HTTP connection failed: {error}");
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

async fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    let mut codec = HttpCodec::new();
    loop {
        let (result, buffer) = stream.read([0u8; 8192]).await.into();
        let bytes = result.map_err(|error| format!("failed to read from connection: {error}"))?;
        if bytes == 0 {
            return Ok(());
        }
        match codec.decode(&buffer[..bytes]) {
            Ok(requests) => {
                for request in requests {
                    let close = request.connection_should_close();
                    write_response(&mut stream, route_request(&request, close)).await?;
                    if close {
                        return Ok(());
                    }
                }
            }
            Err(error) => {
                write_response(&mut stream, error_response(error, true)).await?;
                return Ok(());
            }
        }
    }
}

async fn handle_in_process_connection(
    mut stream: TcpStream,
    host: Arc<InProcessWebHost>,
    actor: ActorBinding,
) -> Result<(), String> {
    let endpoint = host.allocate_endpoint()?;
    host.driver
        .open_endpoint(endpoint, Some(actor.identity), Symbol::intern("http"))
        .map_err(format_driver_error)?;
    let mut codec = HttpCodec::new();
    loop {
        let (result, buffer) = stream.read([0u8; 8192]).await.into();
        let bytes = result.map_err(|error| format!("failed to read from connection: {error}"))?;
        if bytes == 0 {
            host.driver.close_endpoint(endpoint);
            return Ok(());
        }
        match codec.decode(&buffer[..bytes]) {
            Ok(requests) => {
                for request in requests {
                    let close = request.connection_should_close();
                    let response =
                        handle_in_process_request(&host, endpoint, actor.identity, &request, close);
                    write_response(&mut stream, response).await?;
                    if close {
                        host.driver.close_endpoint(endpoint);
                        return Ok(());
                    }
                }
            }
            Err(error) => {
                write_response(&mut stream, error_response(error, true)).await?;
                host.driver.close_endpoint(endpoint);
                return Ok(());
            }
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> Result<(), String> {
    let mut out = Vec::new();
    encode_response(&response, &mut out);
    let (result, _) = stream.write_all(out).await.into();
    result.map_err(|error| format!("failed to write to connection: {error}"))
}
