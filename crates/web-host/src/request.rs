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

use crate::codec::{HttpRequest, HttpResponse};
use crate::response::{internal_error_response, response_from_submitted, route_request};
use crate::{InProcessWebHost, format_driver_error};
use mica_runtime::{TaskInput, TaskRequest};
use mica_var::{Identity, Symbol, Value};

#[derive(Clone, Debug)]
pub(crate) struct RequestFact {
    relation: Symbol,
    values: Vec<Value>,
}

pub(crate) fn handle_in_process_request(
    host: &InProcessWebHost,
    endpoint: Identity,
    actor: Identity,
    request: &HttpRequest,
    close: bool,
) -> HttpResponse {
    if request.method == "GET" && request.path == "/healthz" {
        return route_request(request, close);
    }
    let request_id = match host.allocate_request() {
        Ok(request_id) => request_id,
        Err(error) => return internal_error_response(error, close),
    };
    let facts = request_facts(request_id, request);
    for fact in &facts {
        if let Err(error) =
            host.driver
                .assert_transient_named(endpoint, fact.relation, fact.values.clone())
        {
            cleanup_request_facts(host, endpoint, &facts);
            return internal_error_response(format_driver_error(error), close);
        }
    }

    let submitted = host.driver.submit_invocation(
        endpoint,
        TaskRequest {
            principal: None,
            actor: Some(actor),
            endpoint,
            authority: mica_runtime::AuthorityContext::root(),
            input: TaskInput::Invocation {
                selector: Symbol::intern("http_request"),
                roles: vec![(Symbol::intern("request"), Value::identity(request_id))],
            },
        },
    );
    cleanup_request_facts(host, endpoint, &facts);

    match submitted {
        Ok(submitted) => response_from_submitted(submitted, close),
        Err(error) => internal_error_response(format_driver_error(error), close),
    }
}

pub(crate) fn request_facts(request_id: Identity, request: &HttpRequest) -> Vec<RequestFact> {
    let request_value = Value::identity(request_id);
    let mut facts = vec![
        RequestFact {
            relation: Symbol::intern("HttpRequest"),
            values: vec![request_value.clone()],
        },
        RequestFact {
            relation: Symbol::intern("RequestMethod"),
            values: vec![request_value.clone(), Value::string(&request.method)],
        },
        RequestFact {
            relation: Symbol::intern("RequestPath"),
            values: vec![request_value.clone(), Value::string(&request.path)],
        },
        RequestFact {
            relation: Symbol::intern("RequestVersion"),
            values: vec![
                request_value.clone(),
                Value::int(i64::from(request.version)).unwrap(),
            ],
        },
    ];
    for header in &request.headers {
        facts.push(RequestFact {
            relation: Symbol::intern("RequestHeader"),
            values: vec![
                request_value.clone(),
                Value::string(header.name.to_ascii_lowercase()),
                Value::bytes(&header.value),
            ],
        });
    }
    if !request.body.is_empty() {
        facts.push(RequestFact {
            relation: Symbol::intern("RequestBody"),
            values: vec![request_value, Value::bytes(&request.body)],
        });
    }
    facts
}

fn cleanup_request_facts(host: &InProcessWebHost, endpoint: Identity, facts: &[RequestFact]) {
    for fact in facts.iter().rev() {
        let _ = host
            .driver
            .retract_transient_named(endpoint, fact.relation, fact.values.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_facts_include_core_request_neighbourhood() {
        let request_id = Identity::new(0x00eb_0000_0000_0001).unwrap();
        let facts = request_facts(
            request_id,
            &HttpRequest {
                method: "GET".to_owned(),
                path: "/hello".to_owned(),
                version: 1,
                headers: vec![crate::codec::HttpHeader::new("Accept", b"text/plain")],
                body: Vec::new(),
            },
        );

        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("HttpRequest")
                    && fact.values == vec![Value::identity(request_id)])
        );
        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("RequestPath")
                    && fact.values == vec![Value::identity(request_id), Value::string("/hello")])
        );
        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("RequestHeader")
                    && fact.values
                        == vec![
                            Value::identity(request_id),
                            Value::string("accept"),
                            Value::bytes(b"text/plain")
                        ])
        );
    }
}
