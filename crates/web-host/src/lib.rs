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

use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};

pub mod codec;

use crate::codec::{HttpCodec, HttpCodecError, HttpRequest, HttpResponse, encode_response};

pub const DEFAULT_BIND: &str = "127.0.0.1:8080";

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
                let close = true;
                write_response(&mut stream, error_response(error, close)).await?;
                return Ok(());
            }
        }
    }
}

fn route_request(request: &HttpRequest, close: bool) -> HttpResponse {
    let response = match request.method.as_str() {
        "GET" if request.path == "/healthz" => HttpResponse::text(200, "OK", "ok\n"),
        "GET" if request.path == "/" => HttpResponse::html(
            200,
            "OK",
            concat!(
                "<!doctype html><html><head><meta charset=\"utf-8\">",
                "<title>Mica</title></head><body><main>",
                "<h1>Mica</h1><p>HTTP/1.1 host is running.</p>",
                "</main></body></html>\n"
            ),
        ),
        "GET" => HttpResponse::text(404, "Not Found", "not found\n"),
        _ => HttpResponse::text(405, "Method Not Allowed", "method not allowed\n")
            .with_header("Allow", b"GET".as_slice()),
    };
    with_connection_header(response, close)
}

fn error_response(error: HttpCodecError, close: bool) -> HttpResponse {
    let response = match error {
        HttpCodecError::UnsupportedTransferEncoding => HttpResponse::text(
            501,
            "Not Implemented",
            "transfer encoding is not supported\n",
        ),
        HttpCodecError::HeaderTooLarge | HttpCodecError::BodyTooLarge => {
            HttpResponse::text(413, "Content Too Large", "request is too large\n")
        }
        HttpCodecError::TooManyHeaders => {
            HttpResponse::text(431, "Request Header Fields Too Large", "too many headers\n")
        }
        _ => HttpResponse::text(400, "Bad Request", "bad request\n"),
    };
    with_connection_header(response, close)
}

fn with_connection_header(response: HttpResponse, close: bool) -> HttpResponse {
    if close {
        response.with_header("Connection", b"close".as_slice())
    } else {
        response.with_header("Connection", b"keep-alive".as_slice())
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> Result<(), String> {
    let mut out = Vec::new();
    encode_response(&response, &mut out);
    let (result, _) = stream.write_all(out).await.into();
    result.map_err(|error| format!("failed to write to connection: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_health_check() {
        let response = route_request(
            &HttpRequest {
                method: "GET".to_owned(),
                path: "/healthz".to_owned(),
                version: 1,
                headers: Vec::new(),
                body: Vec::new(),
            },
            false,
        );

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok\n");
        assert_eq!(
            response
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("connection"))
                .map(|header| header.value.as_slice()),
            Some(b"keep-alive".as_slice())
        );
    }

    #[test]
    fn rejects_unsupported_methods() {
        let response = route_request(
            &HttpRequest {
                method: "POST".to_owned(),
                path: "/".to_owned(),
                version: 1,
                headers: Vec::new(),
                body: Vec::new(),
            },
            true,
        );

        assert_eq!(response.status, 405);
        assert_eq!(
            response
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("connection"))
                .map(|header| header.value.as_slice()),
            Some(b"close".as_slice())
        );
    }
}
