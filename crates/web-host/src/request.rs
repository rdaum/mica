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
use crate::response::{
    internal_error_response, is_sync_client_path, response_from_submitted, route_request,
};
use crate::{InProcessWebHost, RequestBinding, format_driver_error};
use mica_runtime::Tuple;
use mica_var::{Identity, Symbol, Value};
use std::sync::OnceLock;

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
    binding: &RequestBinding,
    request: &HttpRequest,
    close: bool,
) -> HttpResponse {
    if request.method == "GET"
        && request.path == "/_mica/metrics/snapshot"
        && metrics_snapshot_enabled()
    {
        let body = serde_json::to_vec_pretty(&crate::metrics::metrics_snapshot_json())
            .unwrap_or_else(|_| b"{\"error\":\"failed to serialize metrics\"}".to_vec());
        return HttpResponse::new(200, "OK", body)
            .with_header("Content-Type", b"application/json; charset=utf-8")
            .with_header("Cache-Control", b"no-store");
    }

    if request.method == "GET" && (request.path == "/healthz" || is_sync_client_path(&request.path))
    {
        return route_request(request, close);
    }

    if let Some(auth) = &host.auth {
        if let Some(response) = auth.handle_auth_start_github(request).await {
            return response;
        }
        if let Some(response) = auth.handle_auth_callback(request).await {
            return response;
        }
        if let Some(response) = auth.handle_auth_logout(request).await {
            return response;
        }
    }

    let effective_actor = if let Some(auth) = &host.auth {
        if crate::auth::is_pre_auth_login_path(&request.path) {
            match host
                .driver
                .named_identity(Symbol::intern("source/auth_guest"))
            {
                Ok(actor) => Some(actor),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "failed to resolve pre-auth login actor"
                    );
                    return HttpResponse::new(
                        500,
                        "Internal Server Error",
                        b"Failed to resolve login identity".to_vec(),
                    );
                }
            }
        } else if !crate::auth::is_unauthenticated_path(&request.path) {
            let cookie_header = request
                .headers
                .iter()
                .find(|h| h.name.eq_ignore_ascii_case("cookie"))
                .map(|h| std::str::from_utf8(&h.value).unwrap_or(""));

            match auth.resolve_auth_context(cookie_header).await {
                Ok(Some(ctx)) => {
                    match host.driver.named_identity(Symbol::intern(&ctx.actor_name)) {
                        Ok(actor) => Some(actor),
                        Err(error) => {
                            tracing::warn!(
                                actor_name = %ctx.actor_name,
                                error = %error,
                                "failed to resolve authenticated actor"
                            );
                            return HttpResponse::new(
                                500,
                                "Internal Server Error",
                                b"Failed to resolve user identity".to_vec(),
                            );
                        }
                    }
                }
                Ok(None) => {
                    if request.method == "GET" {
                        return crate::auth::login_redirect_response(&request.path);
                    }
                    return HttpResponse::new(
                        401,
                        "Unauthorized",
                        b"Authentication required".to_vec(),
                    );
                }
                Err(error) => {
                    tracing::warn!(error = %error, "authentication failed");
                    if request.method == "GET" {
                        tracing::info!(
                            path = %request.path,
                            "clearing invalid session cookie and redirecting to login"
                        );
                        return crate::auth::clear_session_login_redirect_response(&request.path);
                    }
                    return HttpResponse::new(
                        401,
                        "Unauthorized",
                        b"Invalid or expired session".to_vec(),
                    );
                }
            }
        } else {
            binding.actor
        }
    } else {
        binding.actor
    };

    let request_id = match host.allocate_request() {
        Ok(request_id) => request_id,
        Err(error) => return internal_error_response(error, close),
    };
    let request_endpoint = match host.allocate_endpoint() {
        Ok(endpoint) => endpoint,
        Err(error) => return internal_error_response(error, close),
    };
    if let Err(error) = host.driver.open_endpoint_with_context(
        request_endpoint,
        Some(binding.principal),
        effective_actor,
        Symbol::intern("http-request"),
    ) {
        return internal_error_response(format_driver_error(error), close);
    }

    let request_facts = request_facts(request_id, binding.principal, effective_actor, request);
    let transient_tuples = request_facts
        .iter()
        .map(|fact| (fact.relation, fact.tuple.clone()))
        .collect();
    if let Err(error) = host
        .driver
        .assert_transient_tuples_named(request_endpoint, transient_tuples)
    {
        host.driver.close_endpoint(request_endpoint);
        return internal_error_response(format_driver_error(error), close);
    }

    let submitted = host
        .driver
        .submit_invocation_for_endpoint(
            request_endpoint,
            Symbol::intern("http_request"),
            vec![(Symbol::intern("request"), Value::identity(request_id))],
        )
        .await;
    host.driver.close_endpoint(request_endpoint);

    match submitted {
        Ok(submitted) => response_from_submitted(submitted, close),
        Err(error) => internal_error_response(format_driver_error(error), close),
    }
}

fn metrics_snapshot_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MICA_METRICS_SNAPSHOT")
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

fn visit_request_facts<E>(
    request_id: Identity,
    principal: Identity,
    actor: Option<Identity>,
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
        Symbol::intern("RequestPrincipal"),
        [request_value.clone(), Value::identity(principal)],
    ))?;
    if let Some(actor) = actor {
        visit(RequestFact::new(
            Symbol::intern("RequestActor"),
            [request_value.clone(), Value::identity(actor)],
        ))?;
    }
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

fn request_facts(
    request_id: Identity,
    principal: Identity,
    actor: Option<Identity>,
    request: &HttpRequest,
) -> Vec<RequestFact> {
    let mut facts = Vec::new();
    visit_request_facts(request_id, principal, actor, request, |fact| {
        facts.push(fact);
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    facts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_facts_include_core_request_neighbourhood() {
        let request_id = Identity::new(0x00eb_0000_0000_0001).unwrap();
        let principal = Identity::new(0x00e0_0000_0000_0001).unwrap();
        let facts = request_facts(
            request_id,
            principal,
            None,
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
    fn request_facts_include_principal_and_optional_actor_binding() {
        let request_id = Identity::new(0x00eb_0000_0000_0002).unwrap();
        let principal = Identity::new(0x00e0_0000_0000_0002).unwrap();
        let actor = Identity::new(0x00e0_0000_0000_0003).unwrap();
        let facts = request_facts(
            request_id,
            principal,
            Some(actor),
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
                .any(|fact| fact.relation == Symbol::intern("RequestPrincipal")
                    && fact.tuple.values()
                        == [Value::identity(request_id), Value::identity(principal)])
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
