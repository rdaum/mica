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
            let spec = OpenaiRequestSpec::from_payload(&request.payload)?;
            perform_openai_request(spec).await
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

struct OpenaiRequestSpec {
    request: HttpRequestSpec,
    response_mode: OpenaiResponseMode,
}

enum OpenaiResponseMode {
    Http,
    Json,
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
}

impl OpenaiRequestSpec {
    fn from_payload(payload: &Value) -> Result<Self, String> {
        let base_url = optional_map_string(payload, "base_url")?
            .or_else(|| std::env::var("MICA_OPENAI_BASE_URL").ok())
            .or_else(|| std::env::var("OPENROUTER_BASE_URL").ok())
            .unwrap_or_else(|| "https://openrouter.ai/api/v1".to_owned());
        let path =
            optional_map_string(payload, "path")?.unwrap_or_else(|| "/chat/completions".to_owned());
        let url = join_url_path(&base_url, &path);
        let mut headers = optional_map_headers(payload, "headers")?.unwrap_or_default();
        headers.push(("Content-Type".to_owned(), "application/json".to_owned()));
        if let Some(api_key) = optional_map_string(payload, "api_key")?
            .or_else(|| std::env::var("OPENROUTER_API_KEY").ok())
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        {
            headers.push(("Authorization".to_owned(), format!("Bearer {api_key}")));
        }
        if let Some(referer) = optional_map_string(payload, "referer")?
            .or_else(|| std::env::var("MICA_OPENROUTER_REFERER").ok())
            .or_else(|| std::env::var("OPENROUTER_HTTP_REFERER").ok())
        {
            headers.push(("HTTP-Referer".to_owned(), referer));
        }
        if let Some(title) = optional_map_string(payload, "title")?
            .or_else(|| std::env::var("MICA_OPENROUTER_TITLE").ok())
            .or_else(|| std::env::var("OPENROUTER_TITLE").ok())
        {
            headers.push(("X-OpenRouter-Title".to_owned(), title));
        }
        let (body, response_mode) =
            if map_get(payload, "body").is_some() || map_get(payload, "json").is_some() {
                (request_body(payload)?, OpenaiResponseMode::Http)
            } else {
                (
                    serde_json::to_vec(&openai_chat_completion_json(payload)?).map_err(
                        |error| format!("failed to encode OpenAI chat completion request: {error}"),
                    )?,
                    OpenaiResponseMode::Json,
                )
            };
        Ok(Self {
            request: HttpRequestSpec {
                method: "POST".to_owned(),
                url,
                headers,
                body,
            },
            response_mode,
        })
    }
}

impl HttpRequestSpec {
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

async fn perform_openai_request(spec: OpenaiRequestSpec) -> Result<Value, String> {
    match spec.response_mode {
        OpenaiResponseMode::Http => perform_http_request(spec.request).await,
        OpenaiResponseMode::Json => {
            let response = perform_http_bytes(spec.request).await?;
            if !(200..300).contains(&response.status) {
                let message = String::from_utf8_lossy(&response.body);
                return Err(format!(
                    "OpenAI chat completion failed with HTTP {}: {message}",
                    response.status
                ));
            }
            let mut json: serde_json::Value = serde_json::from_slice(&response.body)
                .map_err(|error| format!("invalid OpenAI chat completion response: {error}"))?;
            normalize_openai_tool_call_response(&mut json);
            value_from_json(&json)
        }
    }
}

fn normalize_openai_tool_call_response(response: &mut serde_json::Value) {
    let Some(choices) = response
        .get_mut("choices")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    for choice in choices {
        let Some(message) = choice
            .get_mut("message")
            .and_then(|value| value.as_object_mut())
        else {
            continue;
        };
        if message.contains_key("tool_calls") {
            continue;
        }
        let Some(content) = message.get("content").and_then(|value| value.as_str()) else {
            continue;
        };
        let tool_calls = parse_dsml_tool_calls(content);
        if tool_calls.is_empty() {
            continue;
        }
        tracing::warn!(
            tool_call_count = tool_calls.len(),
            "normalized leaked DSML tool calls from OpenAI response content"
        );
        message.insert(
            "tool_calls".to_owned(),
            serde_json::Value::Array(tool_calls),
        );
        message.insert("content".to_owned(), serde_json::Value::Null);
    }
}

fn parse_dsml_tool_calls(content: &str) -> Vec<serde_json::Value> {
    if !content.contains("DSML") || !content.contains("invoke name=\"") {
        return Vec::new();
    }
    let mut calls = Vec::new();
    let mut cursor = 0;
    let marker = "invoke name=\"";
    while let Some(relative_start) = content[cursor..].find(marker) {
        let invoke_start = cursor + relative_start;
        let name_start = invoke_start + marker.len();
        let Some(relative_name_end) = content[name_start..].find('"') else {
            break;
        };
        let name_end = name_start + relative_name_end;
        let name = &content[name_start..name_end];
        let Some(relative_tag_end) = content[name_end..].find('>') else {
            break;
        };
        let tag_end = name_end + relative_tag_end;
        let next_invoke = content[tag_end + 1..]
            .find(marker)
            .map(|relative| tag_end + 1 + relative)
            .unwrap_or(content.len());
        let block = &content[tag_end + 1..next_invoke];
        let args = parse_dsml_parameters(block);
        let arguments = serde_json::to_string(&serde_json::Value::Object(args))
            .unwrap_or_else(|_| "{}".to_owned());
        calls.push(serde_json::json!({
            "id": format!("dsml_tool_{}", calls.len() + 1),
            "type": "function",
            "function": {
                "name": decode_dsml_text(name),
                "arguments": arguments,
            },
        }));
        cursor = next_invoke;
    }
    calls
}

fn parse_dsml_parameters(block: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut args = serde_json::Map::new();
    let mut cursor = 0;
    let marker = "parameter name=\"";
    while let Some(relative_start) = block[cursor..].find(marker) {
        let parameter_start = cursor + relative_start;
        let name_start = parameter_start + marker.len();
        let Some(relative_name_end) = block[name_start..].find('"') else {
            break;
        };
        let name_end = name_start + relative_name_end;
        let name = decode_dsml_text(&block[name_start..name_end]);
        let Some(relative_tag_end) = block[name_end..].find('>') else {
            break;
        };
        let tag_end = name_end + relative_tag_end;
        let tag = &block[parameter_start..=tag_end];
        let value_start = tag_end + 1;
        let Some(relative_value_end) = block[value_start..].find("</") else {
            break;
        };
        let value_end = value_start + relative_value_end;
        let raw_value = decode_dsml_text(&block[value_start..value_end]);
        let is_string = quoted_attr_value(tag, "string").as_deref() != Some("false");
        args.insert(name, dsml_parameter_json_value(&raw_value, is_string));
        cursor = value_end + 2;
    }
    args
}

fn quoted_attr_value(tag: &str, attr: &str) -> Option<String> {
    let marker = format!("{attr}=\"");
    let value_start = tag.find(&marker)? + marker.len();
    let value_end = tag[value_start..].find('"')? + value_start;
    Some(tag[value_start..value_end].to_owned())
}

fn dsml_parameter_json_value(raw_value: &str, is_string: bool) -> serde_json::Value {
    if is_string {
        return serde_json::Value::String(raw_value.to_owned());
    }
    let trimmed = raw_value.trim();
    match trimmed {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        "null" => return serde_json::Value::Null,
        _ => {}
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return serde_json::Value::Number(value.into());
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(value) {
            return serde_json::Value::Number(number);
        }
    }
    serde_json::Value::String(raw_value.to_owned())
}

fn decode_dsml_text(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
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

fn openai_chat_completion_json(payload: &Value) -> Result<serde_json::Value, String> {
    let model = map_string(payload, "model")?;
    let messages = map_get(payload, "messages").ok_or_else(|| "missing \"messages\"".to_owned())?;
    let mut object = match map_get(payload, "options") {
        Some(options) => match json_from_value(&options)? {
            serde_json::Value::Object(object) => object,
            _ => return Err("\"options\" must be a map".to_owned()),
        },
        None => serde_json::Map::new(),
    };
    object.insert("model".to_owned(), serde_json::Value::String(model));
    object.insert("messages".to_owned(), json_from_value(&messages)?);
    object
        .entry("stream")
        .or_insert(serde_json::Value::Bool(false));
    Ok(serde_json::Value::Object(object))
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

fn value_from_json(value: &serde_json::Value) -> Result<Value, String> {
    match value {
        serde_json::Value::Null => Ok(Value::nothing()),
        serde_json::Value::Bool(value) => Ok(Value::bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                return Ok(Value::int(value).unwrap_or_else(|_| Value::float(value as f64)));
            }
            if let Some(value) = value.as_u64() {
                return Ok(i64::try_from(value)
                    .ok()
                    .and_then(|value| Value::int(value).ok())
                    .unwrap_or_else(|| Value::float(value as f64)));
            }
            value
                .as_f64()
                .map(Value::float)
                .ok_or_else(|| "unsupported JSON number".to_owned())
        }
        serde_json::Value::String(value) => Ok(Value::string(value)),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::list),
        serde_json::Value::Object(entries) => entries
            .iter()
            .map(|(key, value)| {
                Ok((
                    Value::symbol(Symbol::intern(key.as_str())),
                    value_from_json(value)?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()
            .map(Value::map),
    }
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
    fn openai_chat_completion_posts_openrouter_request_and_returns_json() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            compio::runtime::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request_bytes = Vec::new();
                loop {
                    let (result, buffer) = stream.read([0u8; 4096]).await.into();
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
                assert!(request.starts_with("POST /api/v1/chat/completions HTTP/1.1\r\n"));
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("Authorization: Bearer test-key"))
                );
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("HTTP-Referer: https://mica.local"))
                );
                assert!(
                    request
                        .lines()
                        .any(|line| line.eq_ignore_ascii_case("X-OpenRouter-Title: Mica"))
                );
                assert!(request.contains(r#""model":"~openai/gpt-latest""#));
                assert!(request.contains(r#""role":"user""#));
                assert!(request.contains(r#""content":"ping""#));
                assert!(request.contains(r#""temperature":0.2"#));
                let response_body = r#"{"id":"chatcmpl-test","choices":[{"finish_reason":"stop","index":0,"message":{"role":"assistant","content":"pong"}}],"model":"openai/test","usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}"#;
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
                service: Symbol::intern("openai"),
                payload: Value::map([
                    (
                        Value::symbol(Symbol::intern("base_url")),
                        Value::string(format!("http://{addr}/api/v1")),
                    ),
                    (
                        Value::symbol(Symbol::intern("api_key")),
                        Value::string("test-key"),
                    ),
                    (
                        Value::symbol(Symbol::intern("referer")),
                        Value::string("https://mica.local"),
                    ),
                    (
                        Value::symbol(Symbol::intern("title")),
                        Value::string("Mica"),
                    ),
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("~openai/gpt-latest"),
                    ),
                    (
                        Value::symbol(Symbol::intern("messages")),
                        Value::list([Value::map([
                            (
                                Value::symbol(Symbol::intern("role")),
                                Value::string("user"),
                            ),
                            (
                                Value::symbol(Symbol::intern("content")),
                                Value::string("ping"),
                            ),
                        ])]),
                    ),
                    (
                        Value::symbol(Symbol::intern("options")),
                        Value::map([(
                            Value::symbol(Symbol::intern("temperature")),
                            Value::float(0.2),
                        )]),
                    ),
                ]),
                timeout_millis: None,
            };
            let response = handle_external_request(request, &ExternalHttpConfig::default()).await;
            let content = response
                .map_get(&Value::symbol(Symbol::intern("choices")))
                .and_then(|choices| choices.with_list(|choices| choices.first().cloned()).flatten())
                .and_then(|choice| choice.map_get(&Value::symbol(Symbol::intern("message"))))
                .and_then(|message| message.map_get(&Value::symbol(Symbol::intern("content"))));
            assert_eq!(content, Some(Value::string("pong")));
            let usage = response
                .map_get(&Value::symbol(Symbol::intern("usage")))
                .and_then(|usage| {
                    usage.map_get(&Value::symbol(Symbol::intern("total_tokens")))
                });
            assert_eq!(usage, Some(Value::int(6).unwrap()));
        });
    }

    #[test]
    fn normalizes_leaked_dsml_tool_calls_in_openai_response() {
        let mut response = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "< | DSML | tool_calls>< | DSML | invoke name=\"source_file_window\">< | DSML | parameter name=\"path\" string=\"true\">crates/relation-kernel/src/computed.rs</ | DSML | parameter>< | DSML | parameter name=\"start_line\" string=\"false\">150</ | DSML | parameter>< | DSML | parameter name=\"line_count\" string=\"false\">100</ | DSML | parameter></ | DSML | invoke></ | DSML | tool_calls>"
                }
            }]
        });

        normalize_openai_tool_call_response(&mut response);

        let message = response["choices"][0]["message"]
            .as_object()
            .expect("message should stay an object");
        assert!(message["content"].is_null());
        let tool_calls = message["tool_calls"]
            .as_array()
            .expect("tool_calls should be normalized");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["type"], "function");
        assert_eq!(tool_calls[0]["function"]["name"], "source_file_window");
        let arguments: serde_json::Value =
            serde_json::from_str(tool_calls[0]["function"]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(arguments["path"], "crates/relation-kernel/src/computed.rs");
        assert_eq!(arguments["start_line"], 150);
        assert_eq!(arguments["line_count"], 100);
    }

    #[test]
    fn preserves_openai_responses_with_native_tool_calls() {
        let mut response = serde_json::json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_native",
                        "type": "function",
                        "function": {
                            "name": "source_search",
                            "arguments": "{\"query\":\"computed\"}"
                        }
                    }]
                }
            }]
        });

        normalize_openai_tool_call_response(&mut response);

        assert_eq!(
            response["choices"][0]["message"]["tool_calls"][0]["id"],
            "call_native"
        );
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
