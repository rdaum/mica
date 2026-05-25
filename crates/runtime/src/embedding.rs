use mica_compiler::HostRequestFunction;
use mica_var::{Symbol, Value};
use mica_vm::{Builtin, BuiltinContext, RuntimeError};
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

pub struct RemoteEmbeddingProvider;

impl EmbeddingProvider for RemoteEmbeddingProvider {
    fn embed_text(&self, model: &str, text: &str) -> Result<Vec<f64>, String> {
        Err(format!(
            "remote embedding provider requires host request execution; cannot synchronously embed text with model {model:?} and text length {}",
            text.len()
        ))
    }
}

pub fn embedding_provider(kind: EmbeddingProviderKind) -> Arc<dyn EmbeddingProvider> {
    match kind {
        EmbeddingProviderKind::Deterministic => Arc::new(DeterministicEmbeddingProvider),
        EmbeddingProviderKind::Disabled => Arc::new(DisabledEmbeddingProvider),
        EmbeddingProviderKind::Vllm => Arc::new(RemoteEmbeddingProvider),
    }
}

pub fn default_embedding_provider() -> Arc<dyn EmbeddingProvider> {
    embedding_provider(EmbeddingProviderKind::Deterministic)
}

pub fn host_request_functions(kind: EmbeddingProviderKind) -> Vec<(String, HostRequestFunction)> {
    match kind {
        EmbeddingProviderKind::Deterministic | EmbeddingProviderKind::Disabled => Vec::new(),
        EmbeddingProviderKind::Vllm => vec![(
            "embed_text".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("embedding"),
                payload_fields: vec![Symbol::intern("model"), Symbol::intern("text")],
                timeout: None,
            },
        )],
    }
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

#[cfg(test)]
mod tests {
    use super::{EmbeddingProviderKind, host_request_functions};

    #[test]
    fn vllm_provider_registers_embed_text_host_request() {
        let functions = host_request_functions(EmbeddingProviderKind::Vllm);
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].0, "embed_text");
        assert_eq!(functions[0].1.service.name(), Some("embedding"));
    }
}
