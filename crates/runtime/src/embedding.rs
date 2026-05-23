use mica_var::{Symbol, Value};
use mica_vm::{Builtin, BuiltinContext, RuntimeError};
use serde_json::json;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub trait EmbeddingProvider: Send + Sync {
    fn embed_text(&self, model: &str, text: &str) -> Result<Vec<f64>, String>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmbeddingProviderKind {
    Deterministic,
    Disabled,
    Vllm,
}

#[derive(Default)]
pub struct DeterministicEmbeddingProvider;

impl EmbeddingProvider for DeterministicEmbeddingProvider {
    fn embed_text(&self, _model: &str, text: &str) -> Result<Vec<f64>, String> {
        const DIMENSIONS: usize = 8;
        let mut buckets = [0.0f64; DIMENSIONS];
        for (index, ch) in text.chars().enumerate() {
            let slot = ((ch as usize).wrapping_add(index)) % DIMENSIONS;
            buckets[slot] += 1.0;
        }
        let norm = buckets
            .iter()
            .map(|value| value * value)
            .sum::<f64>()
            .sqrt();
        let values = if norm == 0.0 {
            buckets.into_iter().collect::<Vec<_>>()
        } else {
            buckets
                .into_iter()
                .map(|value| value / norm)
                .collect::<Vec<_>>()
        };
        Ok(values)
    }
}

pub struct DisabledEmbeddingProvider;

impl EmbeddingProvider for DisabledEmbeddingProvider {
    fn embed_text(&self, model: &str, _text: &str) -> Result<Vec<f64>, String> {
        Err(format!(
            "embedding provider is disabled; cannot embed with model {model:?}"
        ))
    }
}

pub struct VllmEmbeddingProvider {
    embeddings_url: String,
    api_key: Option<String>,
}

impl VllmEmbeddingProvider {
    pub fn from_env() -> Result<Self, String> {
        let base_url = env::var("MICA_VLLM_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8000/v1".to_owned());
        let api_key = env::var("MICA_VLLM_API_KEY").ok();
        Self::new(base_url, api_key)
    }

    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Result<Self, String> {
        let base_url = base_url.into();
        let embeddings_url = if base_url.ends_with("/embeddings") {
            base_url
        } else {
            format!("{}/embeddings", base_url.trim_end_matches('/'))
        };
        if !embeddings_url.starts_with("http://") {
            return Err(format!(
                "vllm embedding URL must start with http://, got {embeddings_url:?}"
            ));
        }
        Ok(Self {
            embeddings_url,
            api_key,
        })
    }
}

impl EmbeddingProvider for VllmEmbeddingProvider {
    fn embed_text(&self, model: &str, text: &str) -> Result<Vec<f64>, String> {
        let request = json!({
            "input": text,
            "model": model,
        })
        .to_string();
        let response = http_post_json(&self.embeddings_url, &request, self.api_key.as_deref())?;
        let value: serde_json::Value = serde_json::from_slice(&response)
            .map_err(|error| format!("invalid vllm embeddings response: {error}"))?;
        let Some(embedding) = value
            .get("data")
            .and_then(|data| data.as_array())
            .and_then(|data| data.first())
            .and_then(|item| item.get("embedding"))
            .and_then(|embedding| embedding.as_array())
        else {
            return Err("vllm embeddings response did not contain data[0].embedding".to_owned());
        };
        embedding
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value
                    .as_f64()
                    .ok_or_else(|| format!("vllm embedding value at index {index} was not a float"))
            })
            .collect()
    }
}

pub fn embedding_provider(kind: EmbeddingProviderKind) -> Arc<dyn EmbeddingProvider> {
    match kind {
        EmbeddingProviderKind::Deterministic => Arc::new(DeterministicEmbeddingProvider),
        EmbeddingProviderKind::Disabled => Arc::new(DisabledEmbeddingProvider),
        EmbeddingProviderKind::Vllm => {
            Arc::new(VllmEmbeddingProvider::from_env().unwrap_or_else(|error| {
                panic!("failed to initialize vllm embedding provider: {error}")
            }))
        }
    }
}

pub fn default_embedding_provider() -> Arc<dyn EmbeddingProvider> {
    embedding_provider(EmbeddingProviderKind::Deterministic)
}

pub struct EmbedTextBuiltin {
    provider: Arc<dyn EmbeddingProvider>,
}

impl EmbedTextBuiltin {
    pub fn new(provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self { provider }
    }
}

impl Builtin for EmbedTextBuiltin {
    fn call(
        &self,
        _context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        if args.len() != 2 {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("embed_text"),
                message: "expected embed_text(model, text)".to_owned(),
            });
        }
        let Some(model) = args[0].with_str(str::to_owned) else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("embed_text"),
                message: "embedding model must be a string".to_owned(),
            });
        };
        let Some(text) = args[1].with_str(str::to_owned) else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("embed_text"),
                message: "embedding text must be a string".to_owned(),
            });
        };
        let values = self.provider.embed_text(&model, &text).map_err(|message| {
            RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("embed_text"),
                message,
            }
        })?;
        Ok(Value::list(values.into_iter().map(Value::float)))
    }
}

fn http_post_json(url: &str, body: &str, bearer_token: Option<&str>) -> Result<Vec<u8>, String> {
    let (authority, path) = split_http_url(url)?;
    let mut stream = TcpStream::connect(&authority)
        .map_err(|error| format!("failed to connect to vllm at {authority}: {error}"))?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(token) = bearer_token {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.write_all(body.as_bytes()))
        .map_err(|error| format!("failed to send vllm embeddings request: {error}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| format!("failed to read vllm embeddings response: {error}"))?;
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err("invalid vllm response: missing header terminator".to_owned());
    };
    let status_line_end = response
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| "invalid vllm response: missing status line".to_owned())?;
    let status_line = std::str::from_utf8(&response[..status_line_end])
        .map_err(|error| format!("invalid vllm status line: {error}"))?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "invalid vllm response: missing status code".to_owned())?
        .parse::<u16>()
        .map_err(|error| format!("invalid vllm status code: {error}"))?;
    let body = response[(header_end + 4)..].to_vec();
    if !(200..300).contains(&status_code) {
        let message = String::from_utf8_lossy(&body);
        return Err(format!(
            "vllm embeddings request failed with HTTP {status_code}: {message}"
        ));
    }
    Ok(body)
}

fn split_http_url(url: &str) -> Result<(String, String), String> {
    let Some(rest) = url.strip_prefix("http://") else {
        return Err(format!(
            "only http:// URLs are supported for vllm, got {url:?}"
        ));
    };
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_owned()),
    };
    if authority.is_empty() {
        return Err(format!("invalid vllm URL, missing host: {url:?}"));
    }
    Ok((authority.to_owned(), path))
}

#[cfg(test)]
mod tests {
    use super::{EmbeddingProvider, VllmEmbeddingProvider};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn vllm_provider_reads_openai_compatible_embeddings() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            let mut expected_len = None;
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if expected_len.is_none() {
                    expected_len = expected_request_len(&request);
                }
                if expected_len.is_some_and(|expected| request.len() >= expected) {
                    break;
                }
            }
            let request_text = String::from_utf8(request).unwrap();
            assert!(request_text.starts_with("POST /v1/embeddings HTTP/1.1\r\n"));
            assert!(request_text.contains("\"model\":\"mud-world\""));
            assert!(request_text.contains("\"input\":\"red brass lamp\""));
            let response_body =
                r#"{"data":[{"embedding":[0.25,0.5,0.75],"index":0}],"object":"list"}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .unwrap();
        });
        let provider = VllmEmbeddingProvider::new(format!("http://{addr}/v1"), None).unwrap();
        let values = provider.embed_text("mud-world", "red brass lamp").unwrap();
        assert_eq!(values, vec![0.25, 0.5, 0.75]);
        server.join().unwrap();
    }

    fn expected_request_len(request: &[u8]) -> Option<usize> {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")?;
        let headers = std::str::from_utf8(&request[..header_end]).ok()?;
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    return value.trim().parse::<usize>().ok();
                }
                None
            })
            .unwrap_or(0);
        Some(header_end + 4 + content_length)
    }
}
