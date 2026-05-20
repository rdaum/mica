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
use mica_runtime::{TaskInput, TaskRequest, Tuple};
use mica_var::{Identity, Symbol, Value};

#[derive(Clone, Debug)]
pub(crate) struct RequestFact {
    relation: Symbol,
    tuple: Tuple,
}

impl RequestFact {
    fn new(relation: Symbol, values: impl IntoIterator<Item = Value>) -> Self {
        Self {
            relation,
            tuple: Tuple::new(values),
        }
    }
}

pub(crate) async fn handle_in_process_request(
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
    let request_facts = request_facts(request_id, actor, request);
    let transient_tuples = request_facts
        .iter()
        .map(|fact| (fact.relation, fact.tuple.clone()))
        .collect();
    if let Err(error) = host
        .driver
        .assert_transient_tuples_named(endpoint, transient_tuples)
    {
        return internal_error_response(format_driver_error(error), close);
    }

    let submitted = host
        .driver
        .submit_invocation(
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
        )
        .await;
    cleanup_request_facts(host, endpoint, &request_facts);

    match submitted {
        Ok(submitted) => response_from_submitted(submitted, close),
        Err(error) => internal_error_response(format_driver_error(error), close),
    }
}

fn visit_request_facts<E>(
    request_id: Identity,
    actor: Identity,
    request: &HttpRequest,
    mut visit: impl FnMut(RequestFact) -> Result<(), E>,
) -> Result<(), E> {
    let request_value = Value::identity(request_id);
    visit(RequestFact::new(
        Symbol::intern("HttpRequest"),
        [request_value.clone()],
    ))?;
    visit(RequestFact::new(
        Symbol::intern("RequestMethod"),
        [request_value.clone(), Value::string(&request.method)],
    ))?;
    visit(RequestFact::new(
        Symbol::intern("RequestPath"),
        [request_value.clone(), Value::string(&request.path)],
    ))?;
    visit(RequestFact::new(
        Symbol::intern("RequestVersion"),
        [
            request_value.clone(),
            Value::int(i64::from(request.version)).unwrap(),
        ],
    ))?;
    visit(RequestFact::new(
        Symbol::intern("RequestActor"),
        [request_value.clone(), Value::identity(actor)],
    ))?;
    for header in &request.headers {
        visit(RequestFact::new(
            Symbol::intern("RequestHeader"),
            [
                request_value.clone(),
                Value::string(header.name.to_ascii_lowercase()),
                Value::bytes(&header.value),
            ],
        ))?;
    }
    if !request.body.is_empty() {
        visit(RequestFact::new(
            Symbol::intern("RequestBody"),
            [request_value, Value::bytes(&request.body)],
        ))?;
    }
    Ok(())
}

fn request_facts(request_id: Identity, actor: Identity, request: &HttpRequest) -> Vec<RequestFact> {
    let mut facts = Vec::new();
    visit_request_facts(request_id, actor, request, |fact| {
        facts.push(fact);
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    facts
}

fn cleanup_request_facts(host: &InProcessWebHost, endpoint: Identity, facts: &[RequestFact]) {
    let transient_tuples = facts
        .iter()
        .rev()
        .map(|fact| (fact.relation, fact.tuple.clone()))
        .collect();
    let _ = host
        .driver
        .retract_transient_tuples_named(endpoint, transient_tuples);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_facts_include_core_request_neighbourhood() {
        let request_id = Identity::new(0x00eb_0000_0000_0001).unwrap();
        let actor = Identity::new(0x00e0_0000_0000_0001).unwrap();
        let facts = request_facts(
            request_id,
            actor,
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
                    && fact.tuple.values() == [Value::identity(request_id)])
        );
        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("RequestPath")
                    && fact.tuple.values()
                        == [Value::identity(request_id), Value::string("/hello")])
        );
        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("RequestHeader")
                    && fact.tuple.values()
                        == [
                            Value::identity(request_id),
                            Value::string("accept"),
                            Value::bytes(b"text/plain")
                        ])
        );
    }

    #[test]
    fn request_facts_include_actor_binding() {
        let request_id = Identity::new(0x00eb_0000_0000_0002).unwrap();
        let actor = Identity::new(0x00e0_0000_0000_0002).unwrap();
        let facts = request_facts(
            request_id,
            actor,
            &HttpRequest {
                method: "GET".to_owned(),
                path: "/secure".to_owned(),
                version: 1,
                headers: Vec::new(),
                body: Vec::new(),
            },
        );

        assert!(
            facts
                .iter()
                .any(|fact| fact.relation == Symbol::intern("RequestActor")
                    && fact.tuple.values()
                        == [Value::identity(request_id), Value::identity(actor)])
        );
    }
}
