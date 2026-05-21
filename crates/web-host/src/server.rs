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
use crate::{InProcessWebHost, RequestBinding, format_driver_error};
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
    binding: RequestBinding,
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
        let binding = binding.clone();
        compio::runtime::spawn(async move {
            if let Err(error) = handle_in_process_connection(stream, host, binding).await {
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
    binding: RequestBinding,
) -> Result<(), String> {
    let connection_endpoint = host.allocate_endpoint()?;
    if let Err(error) = host.driver.open_endpoint_with_context(
        connection_endpoint,
        Some(binding.principal),
        binding.actor,
        Symbol::intern("http"),
    ) {
        host.driver.close_endpoint(connection_endpoint);
        return Err(format_driver_error(error));
    }
    let _connection_scope = EndpointScope::new(host.clone(), connection_endpoint);
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
                    let response =
                        handle_in_process_request(&host, &binding, &request, close).await;
                    write_response(&mut stream, response).await?;
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

struct EndpointScope {
    host: Arc<InProcessWebHost>,
    endpoint: mica_var::Identity,
}

impl EndpointScope {
    fn new(host: Arc<InProcessWebHost>, endpoint: mica_var::Identity) -> Self {
        Self { host, endpoint }
    }
}

impl Drop for EndpointScope {
    fn drop(&mut self) {
        self.host.driver.close_endpoint(self.endpoint);
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> Result<(), String> {
    let mut out = Vec::new();
    encode_response(&response, &mut out);
    let (result, _) = stream.write_all(out).await.into();
    result.map_err(|error| format!("failed to write to connection: {error}"))
}
