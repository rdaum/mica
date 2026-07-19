use mica_compiler::HostRequestFunction;
use mica_var::{Symbol, Value};

fn openai_timeout() -> Option<Value> {
    let timeout = std::env::var("MICA_OPENAI_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(60);
    if timeout <= 0 {
        return None;
    }
    Some(Value::int(timeout).expect("timeout should fit in mica int"))
}

pub fn host_request_functions() -> Vec<(String, HostRequestFunction)> {
    let timeout = openai_timeout();
    vec![
        (
            "openai_chat_completion".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("openai"),
                payload_fields: vec![Symbol::intern("model"), Symbol::intern("messages")],
                timeout: timeout.clone(),
            },
        ),
        (
            "openai_chat_completion_with_options".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("openai"),
                payload_fields: vec![
                    Symbol::intern("model"),
                    Symbol::intern("messages"),
                    Symbol::intern("options"),
                ],
                timeout: timeout.clone(),
            },
        ),
        (
            "llm_chat_stream_to".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("openai"),
                payload_fields: vec![
                    Symbol::intern("model"),
                    Symbol::intern("messages"),
                    Symbol::intern("options"),
                    Symbol::intern("tools"),
                    Symbol::intern("stream_to"),
                ],
                timeout: timeout.clone(),
            },
        ),
        (
            "llm_responses_stream".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("openai_responses"),
                payload_fields: vec![
                    Symbol::intern("model"),
                    Symbol::intern("input"),
                    Symbol::intern("instructions"),
                    Symbol::intern("options"),
                    Symbol::intern("tools"),
                    Symbol::intern("stream_to"),
                ],
                timeout,
            },
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::host_request_functions;

    #[test]
    fn registers_openai_chat_completion_host_requests() {
        let functions = host_request_functions();
        assert_eq!(functions.len(), 4);
        assert_eq!(functions[0].0, "openai_chat_completion");
        assert_eq!(functions[0].1.service.name(), Some("openai"));
        assert_eq!(functions[0].1.payload_fields[0].name(), Some("model"));
        assert_eq!(functions[0].1.payload_fields[1].name(), Some("messages"));
        assert_eq!(functions[1].0, "openai_chat_completion_with_options");
        assert_eq!(functions[1].1.payload_fields[2].name(), Some("options"));
        assert_eq!(functions[2].0, "llm_chat_stream_to");
        assert_eq!(functions[2].1.payload_fields[4].name(), Some("stream_to"));
        assert_eq!(functions[3].0, "llm_responses_stream");
        assert_eq!(functions[3].1.service.name(), Some("openai_responses"));
        assert_eq!(functions[3].1.payload_fields[1].name(), Some("input"));
        assert_eq!(functions[3].1.payload_fields[5].name(), Some("stream_to"));
    }
}
