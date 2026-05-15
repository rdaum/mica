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

use crate::codec::{HttpCodecError, HttpRequest, HttpResponse};
use mica_runtime::{SubmittedTask, TaskOutcome};
use mica_var::{Symbol, Value};

pub(crate) fn route_request(request: &HttpRequest, close: bool) -> HttpResponse {
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

pub(crate) fn error_response(error: HttpCodecError, close: bool) -> HttpResponse {
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

pub(crate) fn internal_error_response(message: impl Into<String>, close: bool) -> HttpResponse {
    with_connection_header(
        HttpResponse::text(500, "Internal Server Error", message.into()),
        close,
    )
}

pub(crate) fn response_from_submitted(submitted: SubmittedTask, close: bool) -> HttpResponse {
    match submitted.outcome {
        TaskOutcome::Complete { value, .. } => {
            decode_response_value(value, close).unwrap_or_else(|error| {
                internal_error_response(format!("invalid HTTP response value: {error}"), close)
            })
        }
        TaskOutcome::Aborted { error, .. } => {
            internal_error_response(format!("HTTP handler aborted with error: {error}"), close)
        }
        TaskOutcome::Suspended { .. } => {
            internal_error_response("HTTP handler suspended before returning a response", close)
        }
    }
}

pub(crate) fn decode_response_value(value: Value, close: bool) -> Result<HttpResponse, String> {
    if let Some(text) = value.with_str(str::to_owned) {
        return Ok(with_connection_header(
            HttpResponse::text(200, "OK", text),
            close,
        ));
    }
    if value == Value::nothing() {
        return Ok(with_connection_header(
            HttpResponse::new(204, "No Content", Vec::new()),
            close,
        ));
    }
    if value.map_len().is_none() {
        return Err("response must be a string, nothing, or response map".to_owned());
    }
    let status = map_int(&value, "status")?.unwrap_or(200);
    if !(100..=999).contains(&status) {
        return Err("status must be between 100 and 999".to_owned());
    }
    let reason =
        map_string(&value, "reason")?.unwrap_or_else(|| standard_reason(status as u16).to_owned());
    let body = map_body(&value)?.unwrap_or_default();
    let mut response = HttpResponse::new(status as u16, reason, body);
    for (name, value) in map_headers(&value)? {
        response = response.with_header(name, value);
    }
    Ok(with_connection_header(response, close))
}

fn map_int(value: &Value, key: &str) -> Result<Option<i64>, String> {
    let Some(value) = value.map_get(&Value::symbol(Symbol::intern(key))) else {
        return Ok(None);
    };
    value
        .as_int()
        .map(Some)
        .ok_or_else(|| format!(":{key} must be an integer"))
}

fn map_string(value: &Value, key: &str) -> Result<Option<String>, String> {
    let Some(value) = value.map_get(&Value::symbol(Symbol::intern(key))) else {
        return Ok(None);
    };
    value
        .with_str(str::to_owned)
        .map(Some)
        .ok_or_else(|| format!(":{key} must be a string"))
}

fn map_body(value: &Value) -> Result<Option<Vec<u8>>, String> {
    let Some(body) = value.map_get(&Value::symbol(Symbol::intern("body"))) else {
        return Ok(None);
    };
    if let Some(text) = body.with_str(str::to_owned) {
        return Ok(Some(text.into_bytes()));
    }
    body.with_bytes(<[u8]>::to_vec)
        .map(Some)
        .ok_or_else(|| ":body must be a string or bytes".to_owned())
}

fn map_headers(value: &Value) -> Result<Vec<(String, Vec<u8>)>, String> {
    let Some(headers) = value.map_get(&Value::symbol(Symbol::intern("headers"))) else {
        return Ok(Vec::new());
    };
    headers
        .with_list(|headers| {
            headers
                .iter()
                .map(header_pair)
                .collect::<Result<Vec<_>, _>>()
        })
        .ok_or(":headers must be a list")?
}

fn header_pair(value: &Value) -> Result<(String, Vec<u8>), String> {
    value
        .with_list(|parts| {
            let [name, value] = parts else {
                return Err("header entries must be [name, value]".to_owned());
            };
            let name = name
                .with_str(str::to_owned)
                .ok_or_else(|| "header name must be a string".to_owned())?;
            let value = if let Some(text) = value.with_str(str::to_owned) {
                text.into_bytes()
            } else {
                value
                    .with_bytes(<[u8]>::to_vec)
                    .ok_or_else(|| "header value must be a string or bytes".to_owned())?
            };
            Ok((name, value))
        })
        .ok_or_else(|| "header entries must be lists".to_owned())?
}

fn standard_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Content",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

fn with_connection_header(response: HttpResponse, close: bool) -> HttpResponse {
    if close {
        response.with_header("Connection", b"close".as_slice())
    } else {
        response.with_header("Connection", b"keep-alive".as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::HttpRequest;

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

    #[test]
    fn decodes_mica_response_map() {
        let response = decode_response_value(
            Value::map([
                (
                    Value::symbol(Symbol::intern("status")),
                    Value::int(201).unwrap(),
                ),
                (
                    Value::symbol(Symbol::intern("headers")),
                    Value::list([Value::list([
                        Value::string("content-type"),
                        Value::string("text/plain"),
                    ])]),
                ),
                (
                    Value::symbol(Symbol::intern("body")),
                    Value::string("created"),
                ),
            ]),
            false,
        )
        .unwrap();

        assert_eq!(response.status, 201);
        assert_eq!(response.reason, "Created");
        assert_eq!(response.body, b"created");
        assert_eq!(
            response
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("content-type"))
                .map(|header| header.value.as_slice()),
            Some(b"text/plain".as_slice())
        );
    }
}
