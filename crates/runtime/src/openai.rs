use mica_compiler::HostRequestFunction;
use mica_var::{Symbol, Value};

pub fn host_request_functions() -> Vec<(String, HostRequestFunction)> {
    vec![
        (
            "openai_chat_completion".to_owned(),
            HostRequestFunction {
                service: Symbol::intern("openai"),
                payload_fields: vec![Symbol::intern("model"), Symbol::intern("messages")],
                timeout: Some(Value::int(60).expect("static timeout should fit in mica int")),
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
                timeout: Some(Value::int(60).expect("static timeout should fit in mica int")),
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
        assert_eq!(functions.len(), 2);
        assert_eq!(functions[0].0, "openai_chat_completion");
        assert_eq!(functions[0].1.service.name(), Some("openai"));
        assert_eq!(functions[0].1.payload_fields[0].name(), Some("model"));
        assert_eq!(functions[0].1.payload_fields[1].name(), Some("messages"));
        assert_eq!(functions[1].0, "openai_chat_completion_with_options");
        assert_eq!(functions[1].1.payload_fields[2].name(), Some("options"));
    }
}
