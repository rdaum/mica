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

use crate::metrics::{self, ExternalService};
use cyper::Client;
use http::Method;
use mica_driver::ExternalRequestHandler;
use mica_runtime::ExternalRequest;
use mica_var::{Symbol, Value};
use std::sync::Arc;
use std::time::Instant;

pub fn handler() -> ExternalRequestHandler {
    handler_with_config(ExternalHttpConfig::default())
}

#[cfg(test)]
fn handler_with_embedding_base_url(base_url: String) -> ExternalRequestHandler {
    handler_with_config(ExternalHttpConfig {
        embedding_base_url: Some(base_url),
    })
}

fn handler_with_config(config: ExternalHttpConfig) -> ExternalRequestHandler {
    let config = Arc::new(config);
    Arc::new(move |request| {
        let config = Arc::clone(&config);
        Box::pin(async move { handle_external_request(request, &config).await })
    })
}

#[derive(Default)]
struct ExternalHttpConfig {
    embedding_base_url: Option<String>,
}

async fn handle_external_request(request: ExternalRequest, config: &ExternalHttpConfig) -> Value {
    let service = external_service_label(request.service);
    let start = Instant::now();
    metrics::metrics().external_requests.inc(service);
    let result = perform_external_request(request, config).await;
    let elapsed = start.elapsed();
    metrics::metrics()
        .external_request_duration_us
        .record(service, metrics::duration_us(elapsed));
    metrics::metrics()
        .external_request_duration
        .record_elapsed(service, elapsed);
    match result {
        Ok(value) => {
            tracing::debug!(
                service = ?service,
                elapsed_us = elapsed.as_micros(),
                "external request completed"
            );
            value
        }
        Err(message) => {
            metrics::metrics().external_request_errors.inc(service);
            tracing::warn!(
                service = ?service,
                elapsed_us = elapsed.as_micros(),
                error = %message,
                "external request failed"
            );
            external_error(message)
        }
    }
}

fn external_service_label(service: Symbol) -> ExternalService {
    match service.name() {
        Some("http") => ExternalService::Http,
        Some("openai") => ExternalService::Openai,
        Some("embedding") => ExternalService::Embedding,
        _ => ExternalService::Unknown,
    }
}

async fn perform_external_request(
    request: ExternalRequest,
    config: &ExternalHttpConfig,
) -> Result<Value, String> {
    match request.service.name() {
        Some("http") => {
            let spec = HttpRequestSpec::from_http_payload(&request.payload)?;
            perform_http_request(spec).await
        }
        Some("openai") => {
            let spec = HttpRequestSpec::from_openai_payload(&request.payload)?;
            perform_http_request(spec).await
        }
        Some("embedding") => {
            let spec = HttpRequestSpec::from_embedding_payload(
                &request.payload,
                config.embedding_base_url.as_deref(),
            )?;
            perform_embedding_request(spec).await
        }
        _ => Err(format!("unknown external service {:?}", request.service)),
    }
}

struct HttpRequestSpec {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

struct HttpResponseData {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpRequestSpec {
    fn from_http_payload(payload: &Value) -> Result<Self, String> {
        let url = map_string(payload, "url")?;
        let method = optional_map_text(payload, "method")?.unwrap_or_else(|| "GET".to_owned());
        let headers = optional_map_headers(payload, "headers")?.unwrap_or_default();
        let body = request_body(payload)?;
        Ok(Self {
            method,
            url,
            headers,
            body,
        })
    }

    fn from_openai_payload(payload: &Value) -> Result<Self, String> {
        let base_url = optional_map_string(payload, "base_url")?
            .or_else(|| std::env::var("MICA_OPENAI_BASE_URL").ok())
            .or_else(|| std::env::var("MICA_VLLM_BASE_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8000".to_owned());
        let path = optional_map_string(payload, "path")?
            .unwrap_or_else(|| "/v1/chat/completions".to_owned());
        let url = join_url_path(&base_url, &path);
        let mut headers = optional_map_headers(payload, "headers")?.unwrap_or_default();
        headers.push(("Content-Type".to_owned(), "application/json".to_owned()));
        if let Some(api_key) = optional_map_string(payload, "api_key")?
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        {
            headers.push(("Authorization".to_owned(), format!("Bearer {api_key}")));
        }
        Ok(Self {
            method: "POST".to_owned(),
            url,
            headers,
            body: request_body(payload)?,
        })
    }

    fn from_embedding_payload(
        payload: &Value,
        configured_base_url: Option<&str>,
    ) -> Result<Self, String> {
        let model = map_string(payload, "model")?;
        let text = map_string(payload, "text")?;
        let base_url = optional_map_string(payload, "base_url")?
            .or_else(|| configured_base_url.map(str::to_owned))
            .or_else(|| std::env::var("MICA_VLLM_BASE_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8000/v1".to_owned());
        let path =
            optional_map_string(payload, "path")?.unwrap_or_else(|| "/embeddings".to_owned());
        let url = join_url_path(&base_url, &path);
        let mut body = serde_json::json!({
            "input": text,
            "model": model,
        });
        if let Some(truncate_prompt_tokens) = truncate_prompt_tokens()? {
            body["truncate_prompt_tokens"] = serde_json::json!(truncate_prompt_tokens);
        }
        let mut headers = optional_map_headers(payload, "headers")?.unwrap_or_default();
        headers.push(("Content-Type".to_owned(), "application/json".to_owned()));
        if let Some(api_key) = optional_map_string(payload, "api_key")?
            .or_else(|| std::env::var("MICA_VLLM_API_KEY").ok())
        {
            headers.push(("Authorization".to_owned(), format!("Bearer {api_key}")));
        }
        Ok(Self {
            method: "POST".to_owned(),
            url,
            headers,
            body: serde_json::to_vec(&body)
                .map_err(|error| format!("failed to encode embedding request: {error}"))?,
        })
    }
}

async fn perform_http_request(spec: HttpRequestSpec) -> Result<Value, String> {
    let response = perform_http_bytes(spec).await?;
    let status = i64::from(response.status);
    let headers = response
        .headers
        .into_iter()
        .map(|(name, value)| Value::list([Value::string(name), Value::string(value)]))
        .collect::<Vec<_>>();
    let body = String::from_utf8_lossy(&response.body).into_owned();
    Ok(Value::map([
        (
            Value::symbol(Symbol::intern("status")),
            Value::int(status).unwrap(),
        ),
        (
            Value::symbol(Symbol::intern("headers")),
            Value::list(headers),
        ),
        (Value::symbol(Symbol::intern("body")), Value::string(body)),
    ]))
}

async fn perform_embedding_request(spec: HttpRequestSpec) -> Result<Value, String> {
    let response = perform_http_bytes(spec).await?;
    if !(200..300).contains(&response.status) {
        let message = String::from_utf8_lossy(&response.body);
        return Err(format!(
            "embedding request failed with HTTP {}: {message}",
            response.status
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|error| format!("invalid embedding response: {error}"))?;
    let Some(embedding) = value
        .get("data")
        .and_then(|data| data.as_array())
        .and_then(|data| data.first())
        .and_then(|item| item.get("embedding"))
        .and_then(|embedding| embedding.as_array())
    else {
        return Err("embedding response did not contain data[0].embedding".to_owned());
    };
    let values = embedding
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_f64()
                .map(Value::float)
                .ok_or_else(|| format!("embedding value at index {index} was not a float"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::list(values))
}

async fn perform_http_bytes(spec: HttpRequestSpec) -> Result<HttpResponseData, String> {
    let client = Client::new().map_err(|error| format!("failed to create HTTP client: {error}"))?;
    let method = spec
        .method
        .parse::<Method>()
        .map_err(|error| format!("invalid HTTP method {:?}: {error}", spec.method))?;
    let mut request = client
        .request(method, &spec.url)
        .map_err(|error| format!("invalid HTTP request URL {:?}: {error}", spec.url))?;
    for (name, value) in spec.headers {
        request = request
            .header(name.as_str(), value.as_str())
            .map_err(|error| format!("invalid HTTP header {name:?}: {error}"))?;
    }
    let response = request
        .body(spec.body)
        .send()
        .await
        .map_err(|error| format!("HTTP request failed: {error}"))?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let body = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read HTTP response body: {error}"))?;
    Ok(HttpResponseData {
        status,
        headers,
        body: body.as_ref().to_vec(),
    })
}

fn request_body(payload: &Value) -> Result<Vec<u8>, String> {
    if let Some(body) = optional_map_string(payload, "body")? {
        return Ok(body.into_bytes());
    }
    if let Some(json) = map_get(payload, "json") {
        return serde_json::to_vec(&json_from_value(&json)?)
            .map_err(|error| format!("failed to encode JSON body: {error}"));
    }
    Ok(Vec::new())
}

fn json_from_value(value: &Value) -> Result<serde_json::Value, String> {
    if *value == Value::nothing() {
        return Ok(serde_json::Value::Null);
    }
    if let Some(value) = value.as_bool() {
        return Ok(serde_json::Value::Bool(value));
    }
    if let Some(value) = value.as_int() {
        return Ok(serde_json::Value::Number(value.into()));
    }
    if let Some(value) = value.as_float() {
        return serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| "non-finite float cannot be encoded as JSON".to_owned());
    }
    if let Some(text) = value.with_str(str::to_owned) {
        return Ok(serde_json::Value::String(text));
    }
    if let Some(values) = value.with_list(<[Value]>::to_vec) {
        return values
            .iter()
            .map(json_from_value)
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array);
    }
    if let Some(entries) = value.with_map(<[(Value, Value)]>::to_vec) {
        let mut object = serde_json::Map::new();
        for (key, value) in entries {
            object.insert(json_key(&key)?, json_from_value(&value)?);
        }
        return Ok(serde_json::Value::Object(object));
    }
    Err(format!("unsupported JSON value kind {:?}", value.kind()))
}

fn json_key(value: &Value) -> Result<String, String> {
    if let Some(text) = value.with_str(str::to_owned) {
        return Ok(text);
    }
    if let Some(symbol) = value.as_symbol() {
        return symbol
            .name()
            .map(str::to_owned)
            .ok_or_else(|| format!("symbol {symbol:?} has no interned name"));
    }
    Err("JSON object keys must be strings or symbols".to_owned())
}

fn optional_map_headers(
    payload: &Value,
    key: &str,
) -> Result<Option<Vec<(String, String)>>, String> {
    let Some(value) = map_get(payload, key) else {
        return Ok(None);
    };
    let headers = value
        .with_list(|headers| {
            headers
                .iter()
                .map(|header| {
                    header
                        .with_list(|pair| {
                            if pair.len() != 2 {
                                return Err("header pairs must contain name and value".to_owned());
                            }
                            Ok((value_text(&pair[0])?, value_text(&pair[1])?))
                        })
                        .ok_or_else(|| "headers must be a list of pairs".to_owned())?
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .ok_or_else(|| "headers must be a list".to_owned())??;
    Ok(Some(headers))
}

fn map_string(payload: &Value, key: &str) -> Result<String, String> {
    optional_map_string(payload, key)?.ok_or_else(|| format!("missing {key:?}"))
}

fn optional_map_string(payload: &Value, key: &str) -> Result<Option<String>, String> {
    let Some(value) = map_get(payload, key) else {
        return Ok(None);
    };
    Ok(Some(
        value
            .with_str(str::to_owned)
            .ok_or_else(|| format!("{key:?} must be a string"))?,
    ))
}

fn optional_map_text(payload: &Value, key: &str) -> Result<Option<String>, String> {
    let Some(value) = map_get(payload, key) else {
        return Ok(None);
    };
    Ok(Some(value_text(&value)?))
}

fn value_text(value: &Value) -> Result<String, String> {
    if let Some(text) = value.with_str(str::to_owned) {
        return Ok(text);
    }
    if let Some(symbol) = value.as_symbol() {
        return symbol
            .name()
            .map(str::to_owned)
            .ok_or_else(|| format!("symbol {symbol:?} has no interned name"));
    }
    Err("expected string or symbol".to_owned())
}

fn map_get(payload: &Value, key: &str) -> Option<Value> {
    payload.map_get(&Value::symbol(Symbol::intern(key)))
}

fn join_url_path(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn truncate_prompt_tokens() -> Result<Option<usize>, String> {
    match std::env::var("MICA_VLLM_TRUNCATE_PROMPT_TOKENS") {
        Ok(value) if value == "0" => Ok(None),
        Ok(value) => value.parse::<usize>().map(Some).map_err(|error| {
            format!("invalid MICA_VLLM_TRUNCATE_PROMPT_TOKENS value {value:?}: {error}")
        }),
        Err(_) => Ok(Some(512)),
    }
}

fn external_error(message: String) -> Value {
    Value::error(Symbol::intern("ExternalError"), Some(message), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::io::{AsyncRead, AsyncWriteExt};
    use compio::net::TcpListener;

    #[test]
    fn http_external_request_returns_status_headers_and_body() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            compio::runtime::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (result, _) = stream.read([0u8; 1024]).await.into();
                assert!(result.unwrap() > 0);
                let response =
                    b"HTTP/1.1 200 OK\r\nX-Test: yes\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong";
                let (result, _) = stream.write_all(response.to_vec()).await.into();
                result.unwrap();
            })
            .detach();

            let request = ExternalRequest {
                service: Symbol::intern("http"),
                payload: Value::map([
                    (
                        Value::symbol(Symbol::intern("url")),
                        Value::string(format!("http://{addr}/ping")),
                    ),
                    (
                        Value::symbol(Symbol::intern("method")),
                        Value::symbol(Symbol::intern("GET")),
                    ),
                ]),
                timeout_millis: None,
            };
            let response = handle_external_request(request, &ExternalHttpConfig::default()).await;
            assert_eq!(
                response.map_get(&Value::symbol(Symbol::intern("status"))),
                Some(Value::int(200).unwrap())
            );
            assert_eq!(
                response.map_get(&Value::symbol(Symbol::intern("body"))),
                Some(Value::string("pong"))
            );
        });
    }

    #[test]
    fn openai_external_request_posts_json_to_configured_base_url() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            compio::runtime::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request_bytes = Vec::new();
                loop {
                    let (result, buffer) = stream.read([0u8; 2048]).await.into();
                    let count = result.unwrap();
                    if count == 0 {
                        break;
                    }
                    request_bytes.extend_from_slice(&buffer[..count]);
                    if complete_http_request(&request_bytes) {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request_bytes);
                assert!(request.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("Content-Type: application/json"))
                );
                assert!(request.contains(r#""input":"hello""#));
                assert!(request.contains(r#""model":"source-workspace""#));
                let response =
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                let (result, _) = stream.write_all(response.to_vec()).await.into();
                result.unwrap();
            })
            .detach();

            let request = ExternalRequest {
                service: Symbol::intern("openai"),
                payload: Value::map([
                    (
                        Value::symbol(Symbol::intern("base_url")),
                        Value::string(format!("http://{addr}")),
                    ),
                    (
                        Value::symbol(Symbol::intern("path")),
                        Value::string("/v1/embeddings"),
                    ),
                    (
                        Value::symbol(Symbol::intern("json")),
                        Value::map([
                            (
                                Value::symbol(Symbol::intern("model")),
                                Value::string("source-workspace"),
                            ),
                            (
                                Value::symbol(Symbol::intern("input")),
                                Value::string("hello"),
                            ),
                        ]),
                    ),
                ]),
                timeout_millis: None,
            };
            let response = handle_external_request(request, &ExternalHttpConfig::default()).await;
            assert_eq!(
                response.map_get(&Value::symbol(Symbol::intern("status"))),
                Some(Value::int(200).unwrap())
            );
        });
    }

    #[test]
    fn embedding_external_request_returns_vector_from_vllm_response() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            compio::runtime::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request_bytes = Vec::new();
                loop {
                    let (result, buffer) = stream.read([0u8; 2048]).await.into();
                    let count = result.unwrap();
                    if count == 0 {
                        break;
                    }
                    request_bytes.extend_from_slice(&buffer[..count]);
                    if complete_http_request(&request_bytes) {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request_bytes);
                assert!(request.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
                assert!(request.contains(r#""input":"red brass lamp""#));
                assert!(request.contains(r#""model":"source-workspace""#));
                let response_body =
                    r#"{"data":[{"embedding":[0.25,0.5,0.75],"index":0}],"object":"list"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                let (result, _) = stream.write_all(response.into_bytes()).await.into();
                result.unwrap();
            })
            .detach();

            let request = ExternalRequest {
                service: Symbol::intern("embedding"),
                payload: Value::map([
                    (
                        Value::symbol(Symbol::intern("base_url")),
                        Value::string(format!("http://{addr}/v1")),
                    ),
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("source-workspace"),
                    ),
                    (
                        Value::symbol(Symbol::intern("text")),
                        Value::string("red brass lamp"),
                    ),
                ]),
                timeout_millis: None,
            };
            let response = handle_external_request(request, &ExternalHttpConfig::default()).await;
            assert_eq!(
                response,
                Value::list([Value::float(0.25), Value::float(0.5), Value::float(0.75)])
            );
        });
    }

    #[test]
    fn vllm_embed_text_runs_through_daemon_external_handler() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            compio::runtime::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request_bytes = Vec::new();
                loop {
                    let (result, buffer) = stream.read([0u8; 2048]).await.into();
                    let count = result.unwrap();
                    if count == 0 {
                        break;
                    }
                    request_bytes.extend_from_slice(&buffer[..count]);
                    if complete_http_request(&request_bytes) {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request_bytes);
                assert!(request.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
                assert!(request.contains(r#""input":"red brass lamp""#));
                assert!(request.contains(r#""model":"source-workspace""#));
                let response_body =
                    r#"{"data":[{"embedding":[0.25,0.5,0.75],"index":0}],"object":"list"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                let (result, _) = stream.write_all(response.into_bytes()).await.into();
                result.unwrap();
            })
            .detach();

            let runner = mica_runtime::SourceRunner::new_empty_with_embedding_provider(
                mica_runtime::EmbeddingProviderKind::Vllm,
            );
            let driver = mica_driver::CompioTaskDriver::spawn_with_external_handler(
                runner,
                handler_with_embedding_base_url(format!("http://{addr}/v1")),
            )
            .unwrap();
            let source = "return embed_text(\"source-workspace\", \"red brass lamp\")".to_owned();
            let report = driver.submit_root_source_report(source).await.unwrap();
            assert!(matches!(
                report.outcome,
                mica_runtime::TaskOutcome::Suspended { .. }
            ));

            let mut events = Vec::new();
            for _ in 0..50 {
                events.extend(driver.drain_events());
                if events.iter().any(|event| {
                    matches!(
                        event,
                        mica_driver::DriverEvent::TaskCompleted { task_id, value }
                            if *task_id == report.task_id
                                && *value == Value::list([
                                    Value::float(0.25),
                                    Value::float(0.5),
                                    Value::float(0.75),
                                ])
                    )
                }) {
                    return;
                }
                compio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("missing completed embed_text task event: {events:?}");
        });
    }

    fn complete_http_request(bytes: &[u8]) -> bool {
        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .split("\r\n")
            .find_map(|line| {
                let (name, value) = line.split_once(": ")?;
                name.eq_ignore_ascii_case("Content-Length").then_some(value)
            })
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_default();
        bytes.len() >= header_end + 4 + content_length
    }
}
