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

use crate::{CompioTaskDriver, DriverEvent};
use mica_runtime::{
    AuthorityContext, EmbeddingProviderKind, ReadOnlySourceQueryOptions, ReadOnlySourceQueryStatus,
    RuntimeError, SourceTaskError, TaskError, TaskInput, TaskManagerError, TaskRequest,
};
use mica_runtime::{SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SOURCE_APP_FILEINS: &[&str] = &[
    include_str!("../../../apps/shared/sync-host.mica"),
    include_str!("../../../apps/shared/sync-dom.mica"),
    include_str!("../../../apps/shared/retrieval.mica"),
    include_str!("../../../apps/shared/openai.mica"),
    include_str!("../../../apps/source/core.mica"),
    include_str!("../../../apps/source/retrieval.mica"),
    include_str!("../../../apps/source/ui-session.mica"),
    include_str!("../../../apps/source/ui-policy.mica"),
    include_str!("../../../apps/source/ui-state.mica"),
    include_str!("../../../apps/source/ui-actions.mica"),
    include_str!("../../../apps/source/ui-sync.mica"),
    include_str!("../../../apps/source/ui-compose.mica"),
    include_str!("../../../apps/source/ui-navigator.mica"),
    include_str!("../../../apps/source/ui-retrieval-panel.mica"),
    include_str!("../../../apps/source/ui-agent-panel.mica"),
    include_str!("../../../apps/source/ui-code-panel.mica"),
    include_str!("../../../apps/source/http.mica"),
];

fn endpoint(offset: u64) -> Identity {
    Identity::new(0x00ee_0000_0000_0000 + offset).unwrap()
}

fn root_source(source: &str) -> TaskRequest {
    SourceRunner::root_source_request(source)
}

fn load_source_app(runner: &mut SourceRunner) {
    for filein in SOURCE_APP_FILEINS {
        runner.run_filein(filein).unwrap();
    }
}

#[test]
fn driver_runs_source_on_compio_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(endpoint(1), root_source("return 1 + 1"))
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(2).unwrap()
        ));
        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::int(2).unwrap()
        )));
    });
}

#[test]
fn driver_events_can_be_awaited() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(endpoint(29), root_source("return 3 + 4"))
            .await
            .unwrap();

        let events = driver.wait_events().await;

        assert!(events.iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::int(7).unwrap()
        )));
    });
}

#[test]
fn timed_suspend_wakes_and_resumes_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(endpoint(2), root_source("suspend(0.001)\nreturn \"awake\""))
            .await
            .unwrap();
        assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::string("awake")
        )));
    });
}

#[test]
fn external_request_suspends_and_resumes_from_handler() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                Value::list([
                    Value::symbol(request.service),
                    request.payload,
                    Value::int(request.timeout_millis.unwrap_or_default() as i64).unwrap(),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let driver =
            CompioTaskDriver::spawn_with_external_handler(SourceRunner::new_empty(), handler)
                .unwrap();
        let submitted = driver
            .submit_source(
                endpoint(30),
                root_source("return external_request(:echo, \"hello\", 0.005)"),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && *value == Value::list([
                        Value::symbol(Symbol::intern("echo")),
                        Value::string("hello"),
                        Value::int(5).unwrap(),
                    ])
        )));
    });
}

#[test]
fn vllm_embed_text_suspends_as_embedding_external_request() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("embedding"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("source-workspace"))
                );
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("text"))),
                    Some(Value::string("red brass lamp"))
                );
                Value::list([Value::float(0.25), Value::float(0.5), Value::float(0.75)])
            }) as crate::types::ExternalRequestFuture
        });
        let runner = SourceRunner::new_empty_with_embedding_provider(EmbeddingProviderKind::Vllm);
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(33),
                root_source("return embed_text(\"source-workspace\", \"red brass lamp\")"),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && *value == Value::list([
                        Value::float(0.25),
                        Value::float(0.5),
                        Value::float(0.75),
                    ])
        )));
    });
}

#[test]
fn openai_chat_completion_suspends_as_openai_external_request() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("~openai/gpt-latest"))
                );
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                assert_eq!(messages.list_len(), Some(1));
                assert_eq!(request.timeout_millis, Some(60_000));
                Value::map([(
                    Value::symbol(Symbol::intern("choices")),
                    Value::list([Value::map([(
                        Value::symbol(Symbol::intern("message")),
                        Value::map([(
                            Value::symbol(Symbol::intern("content")),
                            Value::string("pong"),
                        )]),
                    )])]),
                )])
            }) as crate::types::ExternalRequestFuture
        });
        let runner = SourceRunner::new_empty();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(33),
                root_source(
                    "return openai_chat_completion(\"~openai/gpt-latest\", [{:role -> \"user\", :content -> \"ping\"}])",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && value
                        .map_get(&Value::symbol(Symbol::intern("choices")))
                        .and_then(|choices| choices.with_list(|choices| choices.first().cloned()).flatten())
                        .and_then(|choice| choice.map_get(&Value::symbol(Symbol::intern("message"))))
                        .and_then(|message| message.map_get(&Value::symbol(Symbol::intern("content"))))
                        == Some(Value::string("pong"))
        )));
    });
}

#[test]
fn mica_query_host_request_runs_read_only_query_and_resumes_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_identity(:web)\n\
                 make_identity(:lamp)\n\
                 make_relation(:CanRead, 2)\n\
                 make_relation(:CanWrite, 2)\n\
                 make_relation(:ThingName, 2)\n\
                 assert CanRead(#web, :ThingName)\n\
                 assert CanWrite(#web, :ThingName)\n\
                 assert ThingName(#lamp, \"Lamp\")\n",
            )
            .unwrap();
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(64);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "return mica_query(\"return one ThingName(#lamp, ?name)\", {:max_output_chars -> 100})",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(request),
                ..
            } if request.timeout_millis == Some(5_000)
        ));

        let mut completed = None;
        for _ in 0..50 {
            for event in driver.drain_events() {
                if let DriverEvent::TaskCompleted { task_id, value } = event
                    && task_id == submitted.task_id
                {
                    completed = Some(value);
                    break;
                }
            }
            if completed.is_some() {
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        let value = completed.expect("mica_query task did not complete");
        assert_eq!(
            value.map_get(&Value::symbol(Symbol::intern("status"))),
            Some(Value::string("complete"))
        );
        assert_eq!(
            value.map_get(&Value::symbol(Symbol::intern("value"))),
            Some(Value::string("Lamp"))
        );
        assert_eq!(
            value.map_get(&Value::symbol(Symbol::intern("rendered"))),
            Some(Value::string("\"Lamp\""))
        );
    });
}

#[test]
fn source_generated_answer_records_reviewable_facts() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("deepseek/deepseek-v4-flash"))
                );
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                let prompt = messages
                    .with_list(|messages| {
                        messages.iter().find_map(|message| {
                            let role = message.map_get(&Value::symbol(Symbol::intern("role")))?;
                            if role != Value::string("user") {
                                return None;
                            }
                            message.map_get(&Value::symbol(Symbol::intern("content")))
                        })
                    })
                    .flatten()
                    .expect("messages should include user content");
                assert!(prompt
                    .with_str(|prompt| prompt.contains("sync_view_tree"))
                    .unwrap_or(false));
                assert!(!prompt
                    .with_str(|prompt| prompt.contains("SECRET_UNAUTHORIZED_CONTEXT"))
                    .unwrap_or(false));
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("sync_view_tree is rendered by the source UI."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(33),
                root_source(
                    "make_identity(:source/secret_subject)\n\
                     let question = retrieval/question_value(#web, \"where is DOM sync rendered?\", #source/retrieval_index, 2, \"source-workspace\")\n\
                     assert Question(question)\n\
                     assert QuestionText(question, \"where is DOM sync rendered?\")\n\
                     assert AskedBy(question, #web)\n\
                     let plan = retrieval/plan_value(question, #source/retrieval_index, 2, \"source-workspace\")\n\
                     assert RetrievalPlan(plan)\n\
                     assert PlanForQuestion(plan, question)\n\
                     assert PlanKind(plan, \"text_search\")\n\
                     assert PlanModel(plan, \"source-workspace\")\n\
                     let allowed_context = retrieval/context_value(plan, #source/text_symbol_sync_view_tree)\n\
                     assert RetrievedContext(allowed_context)\n\
                     assert ContextForPlan(allowed_context, plan)\n\
                     assert ContextOrdinal(allowed_context, 1)\n\
                     assert ContextSubject(allowed_context, #source/text_symbol_sync_view_tree)\n\
                     assert ContextScore(allowed_context, 1.0)\n\
                     assert ContextReason(allowed_context, \"test\")\n\
                     assert ContextSnapshotVersion(allowed_context, \"test\")\n\
                     assert source/RetrievalCitation(plan, #source/text_symbol_sync_view_tree)\n\
                     assert source/RetrievalCitationText(plan, #source/text_symbol_sync_view_tree, \"sync_view_tree renders DOM sync\")\n\
                     assert source/RetrievalCitationLine(plan, #source/text_symbol_sync_view_tree, 8)\n\
                     let secret_context = retrieval/context_value(plan, #source/secret_subject)\n\
                     assert RetrievedContext(secret_context)\n\
                     assert ContextForPlan(secret_context, plan)\n\
                     assert ContextOrdinal(secret_context, 2)\n\
                     assert ContextSubject(secret_context, #source/secret_subject)\n\
                     assert ContextScore(secret_context, 0.9)\n\
                     assert ContextReason(secret_context, \"test\")\n\
                     assert ContextSnapshotVersion(secret_context, \"test\")\n\
                     assert TextUnit(#source/secret_subject)\n\
                     assert TextUnitText(#source/secret_subject, \"SECRET_UNAUTHORIZED_CONTEXT\")\n\
                     assert source/SelectedRetrievalPlan(endpoint(), plan)\n\
                     return source/generate_answer_for_selected_plan(endpoint(), #web)",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..50 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("sync_view_tree is rendered by the source UI."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "generated answer task did not complete");

        let report = driver
            .submit_root_source_report(
                "let selected = one source/SelectedGeneratedAnswer(?endpoint, ?answer)\n\
                 let selected_answer = selected[:answer]\n\
                 let answer = one Answer(?answer)\n\
                 if answer == nothing\n\
                   answer = selected_answer\n\
                 end\n\
                 let review = one source/AnswerReviewStatus(answer, ?status)\n\
                 let answer_status = one AnswerStatus(answer, ?status)\n\
                 let provider = one source/AnswerProvider(answer, ?provider)\n\
                 let model = one source/AnswerResolvedModel(answer, ?model)\n\
                 let answer_text = one AnswerText(answer, ?text)\n\
                 let context_text = one AnswerContextText(answer, ?text)\n\
                 let allowed_citation = AnswerCitation(answer, #source/text_symbol_sync_view_tree)\n\
                 let secret_citation = AnswerCitation(answer, #source/secret_subject)\n\
                 let finding = one source/FindingAnswer(?finding, answer)\n\
                 let finding_status = one source/FindingStatus(finding, ?status)\n\
                 let finding_subject = one source/FindingSubject(finding, ?subject)\n\
                 let context_has_sync = false\n\
                 let context_has_no_secret = false\n\
                 if context_text != nothing\n\
                   context_has_sync = string_contains(context_text, \"sync_view_tree\")\n\
                   context_has_no_secret = string_contains(context_text, \"SECRET_UNAUTHORIZED_CONTEXT\") == false\n\
                 end\n\
                 let corrected_finding = source/correct_finding(finding, \"corrected finding\")\n\
                 let corrected_finding_status = one source/FindingStatus(finding, ?status)\n\
                 let corrected_finding_text = one source/FindingText(finding, ?text)\n\
                 let accepted = source/accept_finding(finding)\n\
                 let accepted_status = one source/FindingStatus(finding, ?status)\n\
                 let accepted_fact = source/AcceptedAgentFinding(finding)\n\
                 let corrected_answer = source/correct_answer(answer, \"corrected answer\")\n\
                 let corrected_answer_status = one source/AnswerReviewStatus(answer, ?status)\n\
                 let corrected_answer_text = one AnswerText(answer, ?text)\n\
                 let rejected = source/reject_answer(answer)\n\
                 let rejected_review = one source/AnswerReviewStatus(answer, ?status)\n\
                 return [selected_answer == answer, review, answer_status, provider, model, answer_text, context_has_sync, context_has_no_secret, allowed_citation, secret_citation == false, finding_status, finding_subject == #source/text_symbol_sync_view_tree, corrected_finding, corrected_finding_status, corrected_finding_text, accepted, accepted_status, accepted_fact, corrected_answer, corrected_answer_status, corrected_answer_text, rejected, rejected_review]".to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!("expected fact inspection to complete, got {:?}", report.outcome);
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("pending"));
                assert_eq!(values[2], Value::string("generated"));
                assert_eq!(values[3], Value::string("openrouter"));
                assert_eq!(
                    values[4],
                    Value::string("openrouter/test-model")
                );
                assert_eq!(
                    values[5],
                    Value::string("sync_view_tree is rendered by the source UI.")
                );
                assert_eq!(values[6], Value::bool(true));
                assert_eq!(values[7], Value::bool(true));
                assert_eq!(values[8], Value::bool(true));
                assert_eq!(values[9], Value::bool(true));
                assert_eq!(values[10], Value::string("pending"));
                assert_eq!(values[11], Value::bool(true));
                assert_eq!(values[12], Value::bool(true));
                assert_eq!(values[13], Value::string("corrected"));
                assert_eq!(values[14], Value::string("corrected finding"));
                assert_eq!(values[15], Value::bool(true));
                assert_eq!(values[16], Value::string("accepted"));
                assert_eq!(values[17], Value::bool(true));
                assert_eq!(values[18], Value::bool(true));
                assert_eq!(values[19], Value::string("corrected"));
                assert_eq!(values[20], Value::string("corrected answer"));
                assert_eq!(values[21], Value::bool(true));
                assert_eq!(values[22], Value::string("rejected"));
            })
            .expect("expected generated answer fact tuple");
    });
}

#[test]
fn source_runtime_config_can_override_retrieval_and_generation_defaults() {
    let mut runner = SourceRunner::new_empty();
    load_source_app(&mut runner);

    let report = runner
        .run_source(
            "let default_model = source/generation_model()\n\
             let default_provider = source/generation_provider()\n\
             let default_retrieval_model = source/retrieval_model()\n\
             let default_limit = source/retrieval_limit()\n\
             let default_file_context_limit = source/agent_file_context_line_limit()\n\
             let default_tool_call_limit = source/agent_tool_call_limit()\n\
             assert source/RuntimeConfig(#source/config_agent_model, \"openai/gpt-4.1\")\n\
             assert source/RuntimeConfig(#source/config_agent_file_context_line_limit, 17)\n\
             assert source/RuntimeConfig(#source/config_agent_tool_call_limit, 19)\n\
             assert source/RuntimeConfig(#source/config_generation_provider, \"test-provider\")\n\
             assert source/RuntimeConfig(#source/config_retrieval_model, \"test-retrieval-model\")\n\
             assert source/RuntimeConfig(#source/config_retrieval_limit, 13)\n\
             return [default_model, default_provider, default_retrieval_model, default_limit, default_file_context_limit, default_tool_call_limit, source/generation_model(), source/agent_file_context_line_limit(), source/agent_tool_call_limit(), source/generation_provider(), source/retrieval_model(), source/retrieval_limit()]",
        )
        .unwrap();
    let TaskOutcome::Complete { value, .. } = report.outcome else {
        panic!(
            "expected source generation model inspection to complete, got {:?}",
            report.outcome
        );
    };
    value
        .with_list(|values| {
            assert_eq!(values[0], Value::string("deepseek/deepseek-v4-flash"));
            assert_eq!(values[1], Value::string("openrouter"));
            assert_eq!(values[2], Value::string("source-workspace"));
            assert_eq!(values[3], Value::int(8).unwrap());
            assert_eq!(values[4], Value::int(20000).unwrap());
            assert_eq!(values[5], Value::int(24).unwrap());
            assert_eq!(values[6], Value::string("openai/gpt-4.1"));
            assert_eq!(values[7], Value::int(17).unwrap());
            assert_eq!(values[8], Value::int(19).unwrap());
            assert_eq!(values[9], Value::string("test-provider"));
            assert_eq!(values[10], Value::string("test-retrieval-model"));
            assert_eq!(values[11], Value::int(13).unwrap());
        })
        .expect("expected source runtime config tuple");
}

#[test]
fn source_agent_tool_facts_record_requests_results_and_transcript() {
    let mut runner = SourceRunner::new_empty();
    load_source_app(&mut runner);

    let report = runner
        .run_source(
            "let turn = source/record_agent_user_turn(endpoint(), \"inspect workspace\")\n\
             let args = {:source -> \"return 1\"}\n\
             let request = source/record_agent_tool_request(turn, 1, \"mica_query\", args, \"requested\")\n\
             source/record_agent_tool_result(request, {:rows -> [1]}, \"complete\", \"\")\n\
             source/record_agent_tool_transcript_text(request, \"mica_query complete\")\n\
             let request_row = one source/AgentToolRequest(request, ?ordinal, ?tool, ?args, ?status)\n\
             let result_row = one source/AgentToolResult(request, ?result, ?result_status, ?error)\n\
             let call_turn = one source/AgentToolCallForTurn(request, ?turn)\n\
             let transcript = one source/AgentToolTranscriptText(request, ?text)\n\
             return [request != nothing, call_turn == turn, request_row[:ordinal], request_row[:tool], request_row[:args][:source], request_row[:status], result_row[:result][:rows][0], result_row[:result_status], result_row[:error], transcript]",
        )
        .unwrap();
    let TaskOutcome::Complete { value, .. } = report.outcome else {
        panic!(
            "expected source agent tool fact inspection to complete, got {:?}",
            report.outcome
        );
    };
    value
        .with_list(|values| {
            assert_eq!(values[0], Value::bool(true));
            assert_eq!(values[1], Value::bool(true));
            assert_eq!(values[2], Value::int(1).unwrap());
            assert_eq!(values[3], Value::string("mica_query"));
            assert_eq!(values[4], Value::string("return 1"));
            assert_eq!(values[5], Value::string("complete"));
            assert_eq!(values[6], Value::int(1).unwrap());
            assert_eq!(values[7], Value::string("complete"));
            assert_eq!(values[8], Value::string(""));
            assert_eq!(values[9], Value::string("mica_query complete"));
        })
        .expect("expected source agent tool facts tuple");
}

#[test]
fn source_agent_proposal_tools_record_explicit_pending_kinds() {
    let mut runner = SourceRunner::new_empty();
    load_source_app(&mut runner);

    let report = runner
        .run_source(
            "let turn = source/record_agent_user_turn(endpoint(), \"proposal tools\")\n\
             let note_call = {:id -> \"call_note\", :function -> {:name -> \"source_create_pending_note\", :arguments -> json_encode({:title -> \"Note\", :body -> \"note body\", :target_path -> \"src/lib.rs\"})}}\n\
             let finding_call = {:id -> \"call_finding\", :function -> {:name -> \"source_create_pending_finding\", :arguments -> json_encode({:title -> \"Finding\", :body -> \"finding body\"})}}\n\
             let patch_call = {:id -> \"call_patch\", :function -> {:name -> \"source_propose_patch\", :arguments -> json_encode({:title -> \"Patch\", :body -> \"diff --git a/src/lib.rs b/src/lib.rs\"})}}\n\
             let action_call = {:id -> \"call_action\", :function -> {:name -> \"source_propose_action\", :arguments -> json_encode({:title -> \"Action\", :body -> \"run focused tests\"})}}\n\
             source/run_agent_tool(turn, 1, note_call, #web)\n\
             source/run_agent_tool(turn, 2, finding_call, #web)\n\
             source/run_agent_tool(turn, 3, patch_call, #web)\n\
             source/run_agent_tool(turn, 4, action_call, #web)\n\
             let note_request = one source/AgentToolRequest(source/agent_tool_request_value(turn, 1), ?ordinal, ?tool, ?args, ?status)\n\
             let finding_request = one source/AgentToolRequest(source/agent_tool_request_value(turn, 2), ?ordinal, ?tool, ?args, ?status)\n\
             let patch_request = one source/AgentToolRequest(source/agent_tool_request_value(turn, 3), ?ordinal, ?tool, ?args, ?status)\n\
             let action_request = one source/AgentToolRequest(source/agent_tool_request_value(turn, 4), ?ordinal, ?tool, ?args, ?status)\n\
             let kinds = []\n\
             for row in source/AgentProposalKind(?proposal, ?kind)\n\
               kinds = [@kinds, row[:kind]]\n\
             end\n\
             let statuses = []\n\
             for row in source/AgentProposalStatus(?proposal, ?status)\n\
               statuses = [@statuses, row[:status]]\n\
             end\n\
             return [note_request[:tool], finding_request[:tool], patch_request[:tool], action_request[:tool], kinds, statuses]",
        )
        .unwrap();
    let TaskOutcome::Complete { value, .. } = report.outcome else {
        panic!(
            "expected source agent explicit proposal tool inspection to complete, got {:?}",
            report.outcome
        );
    };
    value
        .with_list(|values| {
            assert_eq!(values[0], Value::string("source_create_pending_note"));
            assert_eq!(values[1], Value::string("source_create_pending_finding"));
            assert_eq!(values[2], Value::string("source_propose_patch"));
            assert_eq!(values[3], Value::string("source_propose_action"));
            values[4]
                .with_list(|kinds| {
                    assert_eq!(kinds[0], Value::string("note"));
                    assert_eq!(kinds[1], Value::string("finding"));
                    assert_eq!(kinds[2], Value::string("patch"));
                    assert_eq!(kinds[3], Value::string("action"));
                })
                .expect("expected proposal kind list");
            values[5]
                .with_list(|statuses| {
                    assert_eq!(statuses[0], Value::string("pending"));
                    assert_eq!(statuses[1], Value::string("pending"));
                    assert_eq!(statuses[2], Value::string("pending"));
                    assert_eq!(statuses[3], Value::string("pending"));
                })
                .expect("expected proposal status list");
        })
        .expect("expected explicit proposal tool tuple");
}

#[test]
fn source_agent_tool_activity_renders_for_web_endpoint() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(66);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let workspace_root = std::env::current_dir().unwrap().display().to_string();

        let report = driver
            .submit_source(
                endpoint,
                root_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {workspace_root:?})\n\
                     let turn = frob(#source/agent_turn, {{:endpoint -> endpoint(), :ordinal -> 1, :role -> \"assistant\", :text -> \"Tool test\"}})\n\
                     assert source/AgentTurn(turn)\n\
                     assert source/AgentTurnEndpoint(turn, endpoint())\n\
                     assert source/AgentTurnOrdinal(turn, 1)\n\
                     assert source/AgentTurnRole(turn, \"assistant\")\n\
                     assert source/AgentTurnText(turn, \"Tool test\")\n\
                     assert source/AgentTurnStatus(turn, \"working\")\n\
                     let request = source/start_agent_tool_request(turn, 1, \"source_search\", {{:query -> \"relation index\"}})\n\
                     source/finish_agent_tool_request(request, {{:status -> \"complete\", :rendered -> \"source_search returned relation index\", :value -> {{:count -> 1}}}}, \"call_1\")\n\
                     let payload = dom_snapshot_payload(31, 1, source/agent_panel_node())\n\
                     return [string_contains(payload, \"Tool activity\"), string_contains(payload, \"source-agent-tools\"), string_contains(payload, \"source_search\"), string_contains(payload, \"query: relation index\"), string_contains(payload, \"source_search returned relation index\")]"
                )),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent tool activity render to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::bool(true));
                assert_eq!(values[2], Value::bool(true));
                assert_eq!(values[3], Value::bool(true));
                assert_eq!(values[4], Value::bool(true));
            })
            .expect("expected tool activity render tuple");
    });
}

#[test]
fn source_retrieval_context_list_uses_context_ordinals() {
    let mut runner = SourceRunner::new_empty();
    load_source_app(&mut runner);

    let report = runner
        .run_source(
            "make_identity(:source/retrieval_test_unit)\n\
             make_identity(:source/retrieval_test_file)\n\
             let plan = frob(#retrieval/plan, {:test -> \"ordinal\"})\n\
             let first = frob(#source/retrieval_test_unit, {:name -> \"first\"})\n\
             let second = frob(#source/retrieval_test_unit, {:name -> \"second\"})\n\
             let first_file = frob(#source/retrieval_test_file, {:name -> \"first\"})\n\
             let second_file = frob(#source/retrieval_test_file, {:name -> \"second\"})\n\
             let first_context = retrieval/context_value(plan, first)\n\
             let second_context = retrieval/context_value(plan, second)\n\
             assert source/FilePath(first_file, \"first.rs\")\n\
             assert source/FilePath(second_file, \"second.rs\")\n\
             assert TextUnit(first)\n\
             assert TextUnit(second)\n\
             assert TextUnitText(first, \"first ordered result\")\n\
             assert TextUnitText(second, \"second ordered result\")\n\
             assert source/RetrievalTextUnitKind(first, \"rust\")\n\
             assert source/RetrievalTextUnitKind(second, \"rust\")\n\
             assert source/RetrievalTextUnitTitle(first, \"first result\")\n\
             assert source/RetrievalTextUnitTitle(second, \"second result\")\n\
             assert source/RetrievalTextUnitFile(first, first_file)\n\
             assert source/RetrievalTextUnitFile(second, second_file)\n\
             assert source/RetrievalTextUnitStartLine(first, 1)\n\
             assert source/RetrievalTextUnitStartLine(second, 1)\n\
             assert source/SelectedRetrievalPlan(endpoint(), plan)\n\
             assert ContextForPlan(first_context, plan)\n\
             assert ContextSubject(first_context, first)\n\
             assert ContextScore(first_context, 1)\n\
             assert ContextReason(first_context, \"expected-first\")\n\
             assert ContextOrdinal(first_context, 1)\n\
             assert ContextForPlan(second_context, plan)\n\
             assert ContextSubject(second_context, second)\n\
             assert ContextScore(second_context, 2)\n\
             assert ContextReason(second_context, \"expected-second\")\n\
             assert ContextOrdinal(second_context, 2)\n\
             return dom_snapshot_payload(31, 1, source/retrieval_context_list_node())",
        )
        .unwrap();
    let TaskOutcome::Complete { value, .. } = report.outcome else {
        panic!(
            "expected retrieval context list render to complete, got {:?}",
            report.outcome
        );
    };
    let payload = value
        .with_str(str::to_owned)
        .expect("expected rendered payload string");
    let first_index = payload
        .find("first ordered result")
        .expect("expected first context snippet");
    let second_index = payload
        .find("second ordered result")
        .expect("expected second context snippet");
    assert!(
        first_index < second_index,
        "retrieval context UI should render ContextOrdinal order"
    );
}

#[test]
fn source_retrieval_search_button_is_disabled_while_prewarming() {
    let mut runner = SourceRunner::new_empty();
    load_source_app(&mut runner);

    let report = runner
        .run_source(
            "let model = source/retrieval_model()\n\
             let corpus_version = source/retrieval_corpus_version()\n\
             retract source/RetrievalCorpusIndexVersion(#source/retrieval_index, model, _)\n\
             retract source/RetrievalPrewarmStatus(model, _)\n\
             assert source/RetrievalPrewarmStatus(model, \"running\")\n\
             let running = dom_snapshot_payload(31, 1, source/retrieval_search_form_node())\n\
             assert source/RetrievalCorpusIndexVersion(#source/retrieval_index, model, corpus_version)\n\
             retract source/RetrievalPrewarmStatus(model, _)\n\
             assert source/RetrievalPrewarmStatus(model, \"complete\")\n\
             let complete = dom_snapshot_payload(31, 2, source/retrieval_search_form_node())\n\
             return [string_contains(running, \"Indexing\"), string_contains(running, \"disabled\"), string_contains(running, \"aria-disabled\"), string_contains(complete, \"Search\"), string_contains(complete, \"disabled\") == false, string_contains(complete, \"aria-disabled\") == false]",
        )
        .unwrap();
    let TaskOutcome::Complete { value, .. } = report.outcome else {
        panic!(
            "expected retrieval search form render to complete, got {:?}",
            report.outcome
        );
    };
    value
        .with_list(|values| {
            assert_eq!(values[0], Value::bool(true));
            assert_eq!(values[1], Value::bool(true));
            assert_eq!(values[2], Value::bool(true));
            assert_eq!(values[3], Value::bool(true));
            assert_eq!(values[4], Value::bool(true));
            assert_eq!(values[5], Value::bool(true));
        })
        .expect("expected search button state tuple");
}

#[test]
fn source_agent_prompt_records_turns_and_grounded_prompt() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("deepseek/deepseek-v4-flash"))
                );
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                let prompt = messages
                    .with_list(|messages| {
                        messages.iter().find_map(|message| {
                            let role = message.map_get(&Value::symbol(Symbol::intern("role")))?;
                            if role != Value::string("user") {
                                return None;
                            }
                            message.map_get(&Value::symbol(Symbol::intern("content")))
                        })
                    })
                    .flatten()
                    .expect("messages should include user content");
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("User request:"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("Current source focus:"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("Value::int(5).unwrap()"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("sync_view_tree"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("source/FileLines"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("current file context is truncated"))
                        .unwrap_or(false)
                );
                assert!(
                    prompt
                        .with_str(|prompt| prompt.contains("call source_file_window"))
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([
                                (
                                    Value::symbol(Symbol::intern("content")),
                                    Value::string("**sync_view_tree** is the source sync render hook."),
                                ),
                                (
                                    Value::symbol(Symbol::intern("reasoning")),
                                    Value::string(
                                        "The prompt contains source focus and retrieved context.",
                                    ),
                                ),
                            ]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let workspace_root = env!("CARGO_MANIFEST_DIR").to_owned();
        let submitted = driver
            .submit_source(
                endpoint(34),
                root_source(&format!(
                    "retract source/RepositoryRoot(#source/repo_mica, _)\n\
                     assert source/RepositoryRoot(#source/repo_mica, {workspace_root:?})\n\
                     assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"where is sync_view_tree rendered?\")"
                )),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..50 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("**sync_view_tree** is the source sync render hook."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent prompt task did not complete");

        let report = driver
            .submit_source(
                endpoint(34),
                root_source(
                "let user = one source/AgentTurnRole(?turn, \"user\")\n\
                 let assistant = one source/AgentTurnRole(?turn, \"assistant\")\n\
                 let assistant_text = one source/AgentTurnText(assistant, ?text)\n\
                 let assistant_reasoning = one source/AgentTurnReasoning(assistant, ?reasoning)\n\
                 let model = one source/AgentTurnResolvedModel(assistant, ?model)\n\
                 let plan = one source/AgentTurnPlan(assistant, ?plan)\n\
                 let context_text = one source/AgentTurnContextText(assistant, ?text)\n\
                 let prompt = one source/AgentTurnPromptText(assistant, ?text)\n\
                 let payload = dom_snapshot_payload(31, 1, source/agent_panel_node())\n\
                 return [user != nothing, assistant != nothing, assistant_text, assistant_reasoning, model, plan == nothing, string_contains(context_text, \"Current file:\"), string_contains(context_text, \"Value::int(5).unwrap()\"), string_contains(prompt, \"No source search results are preloaded.\"), string_contains(prompt, \"Current source focus:\"), string_contains(prompt, \"source/FileLines\"), string_contains(prompt, \"source_search\"), string_contains(prompt, \"mica_query tool result\"), string_contains(payload, \"source-agent-turn-text\"), string_contains(payload, \"source-agent-reasoning\"), string_contains(payload, \"Thinking\")]",
                ),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::bool(true));
                assert_eq!(
                    values[2],
                    Value::string("**sync_view_tree** is the source sync render hook.")
                );
                assert_eq!(
                    values[3],
                    Value::string("The prompt contains source focus and retrieved context.")
                );
                assert_eq!(values[4], Value::string("openrouter/test-model"));
                assert_eq!(values[5], Value::bool(true));
                assert_eq!(values[6], Value::bool(true));
                assert_eq!(values[7], Value::bool(true));
                assert_eq!(values[8], Value::bool(true));
                assert_eq!(values[9], Value::bool(true));
                assert_eq!(values[10], Value::bool(true));
                assert_eq!(values[11], Value::bool(true));
                assert_eq!(values[12], Value::bool(true));
                assert_eq!(values[13], Value::bool(true));
                assert_eq!(values[14], Value::bool(true));
                assert_eq!(values[15], Value::bool(true));
            })
            .expect("expected source agent facts tuple");

        let revision = driver
            .submit_source(endpoint(34), root_source("return source/view_revision()"))
            .await
            .unwrap();
        assert!(matches!(
            revision.outcome,
            TaskOutcome::Complete { value, .. } if value.as_int().is_some_and(|revision| revision > 1)
        ));
    });
}

#[test]
fn root_startup_source_can_resume_vllm_embed_text() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("embedding"));
                Value::list([Value::float(1.0), Value::float(0.0)])
            }) as crate::types::ExternalRequestFuture
        });
        let runner = SourceRunner::new_empty_with_embedding_provider(EmbeddingProviderKind::Vllm);
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let report = driver
            .submit_root_source_report(
                "return embed_text(\"source-workspace\", \"lamp\")".to_owned(),
            )
            .await
            .unwrap();

        assert!(matches!(report.outcome, TaskOutcome::Suspended { .. }));
        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == report.task_id
                    && *value == Value::list([Value::float(1.0), Value::float(0.0)])
        )));
    });
}

#[test]
fn external_request_requires_effect_authority() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let request = TaskRequest {
            principal: None,
            actor: None,
            endpoint: endpoint(31),
            authority: AuthorityContext::empty(),
            input: TaskInput::Source("return external_request(:echo, \"hello\")".to_owned()),
        };

        let denied = driver
            .submit_source(endpoint(31), request)
            .await
            .unwrap_err();
        assert!(driver.format_error(&denied).contains("permission denied"));
    });
}

#[test]
fn log_requires_effect_authority() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let request = TaskRequest {
            principal: None,
            actor: None,
            endpoint: endpoint(33),
            authority: AuthorityContext::empty(),
            input: TaskInput::Source("return log(:info, \"hello\")".to_owned()),
        };

        let denied = driver
            .submit_source(endpoint(33), request)
            .await
            .unwrap_err();
        assert!(driver.format_error(&denied).contains("permission denied"));
    });
}

#[test]
fn log_returns_nothing_with_effect_authority() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(endpoint(34), root_source("return log(:debug, \"hello\")"))
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::nothing()
        ));
    });
}

#[test]
fn external_request_timeout_resumes_with_error_value() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|_request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                compio::time::sleep(Duration::from_millis(50)).await;
                Value::string("late")
            }) as crate::types::ExternalRequestFuture
        });
        let driver =
            CompioTaskDriver::spawn_with_external_handler(SourceRunner::new_empty(), handler)
                .unwrap();
        let submitted = driver
            .submit_source(
                endpoint(32),
                root_source("return external_request(:slow, \"hello\", 0.001)"),
            )
            .await
            .unwrap();
        assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && value.error_code_symbol() == Some(Symbol::intern("ExternalTimeout"))
        )));
    });
}

#[test]
fn commit_yields_and_immediately_resumes_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(endpoint(3), root_source("commit()\nreturn \"committed\""))
            .await
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Commit,
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::string("committed")
        )));
    });
}

#[test]
fn spawn_commits_parent_and_runs_child_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_relation(:Seen, 1)\n\
                 verb child(endpoint)\n\
                   if Seen(:parent)\n\
                     emit(endpoint, \"saw parent\")\n\
                   else\n\
                     emit(endpoint, \"missed parent\")\n\
                   end\n\
                   return nothing\n\
                 end\n",
            )
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(31),
                root_source(
                    "assert Seen(:parent)\n\
                     let child = spawn :child(endpoint: endpoint()) after 0.001\n\
                     return child",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Spawn(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        let events = driver.drain_events();
        let child_task_id = events.iter().find_map(|event| match event {
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && value.as_int().is_some() =>
            {
                Some(value.as_int().unwrap() as u64)
            }
            _ => None,
        });
        let child_task_id = child_task_id.expect("parent completed with spawned child task id");
        assert!(events.iter().any(|event| matches!(
            event,
            DriverEvent::Effect(effect)
                if effect.task_id == child_task_id && effect.value == Value::string("saw parent")
        )));
    });
}

#[test]
fn spawn_runs_receiver_positional_child_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        let coin = runner.run_source("return make_identity(:coin)").unwrap();
        let TaskOutcome::Complete { value: coin, .. } = coin.outcome else {
            panic!("expected coin identity creation to complete");
        };
        let alice = runner.run_source("return make_identity(:alice)").unwrap();
        let TaskOutcome::Complete { value: alice, .. } = alice.outcome else {
            panic!("expected alice identity creation to complete");
        };
        runner
            .run_filein(
                "verb parent()\n\
                 let child = spawn #coin:inspect(#alice) after 0\n\
                 return child\n\
               end\n\
               verb inspect(receiver, actor)\n\
                 emit(endpoint(), [receiver, actor])\n\
                 return nothing\n\
               end\n",
            )
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let submitted = driver
            .submit_source(endpoint(32), root_source("return :parent()"))
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Spawn(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::Effect(effect)
                if effect.value == Value::list([coin.clone(), alice.clone()])
        )));
    });
}

#[test]
fn endpoint_input_resumes_reading_task() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let endpoint = endpoint(4);
        let submitted = driver
            .submit_source(endpoint, root_source("return read(:line)"))
            .await
            .unwrap();
        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::WaitingForInput(_),
                ..
            }
        ));

        let outcomes = driver.input(endpoint, Value::string("look")).await.unwrap();

        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            &outcomes[0],
            TaskOutcome::Complete { value, .. } if *value == Value::string("look")
        ));
    });
}

#[test]
fn mailbox_recv_drains_messages_sent_before_wait() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "verb send_reply(reply)\n\
                 mailbox_send(reply, \"done\")\n\
                 return nothing\n\
               end\n",
            )
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(32),
                root_source(
                    "let caps = mailbox()\n\
                     let rx = caps[0]\n\
                     let tx = caps[1]\n\
                     let child = spawn :send_reply(reply: tx) after 0\n\
                     return mailbox_recv([rx], 1)",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::Spawn(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && value.with_list(|groups| groups.len()) == Some(1)
                    && value.with_list(|groups| groups[0].with_list(|group| {
                        group.len() == 2 && group[1] == Value::list([Value::string("done")])
                    })) == Some(Some(true))
        )));
    });
}

#[test]
fn mailbox_recv_waits_until_sender_commits() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "verb delayed_send(reply)\n\
                 suspend(0.001)\n\
                 mailbox_send(reply, \"late\")\n\
                 return nothing\n\
               end\n",
            )
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let submitted = driver
            .submit_source(
                endpoint(33),
                root_source(
                    "let caps = mailbox()\n\
                     let rx = caps[0]\n\
                     let tx = caps[1]\n\
                     let child = spawn :delayed_send(reply: tx) after 0\n\
                     return mailbox_recv([rx], 1)",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

        compio::time::sleep(Duration::from_millis(30)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id
                    && value.with_list(|groups| groups.len()) == Some(1)
                    && value.with_list(|groups| groups[0].with_list(|group| {
                        group.len() == 2 && group[1] == Value::list([Value::string("late")])
                    })) == Some(Some(true))
        )));
    });
}

#[test]
fn mailbox_recv_zero_timeout_returns_empty_list() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(
                endpoint(34),
                root_source(
                    "let caps = mailbox()\n\
                     return mailbox_recv([caps[0]], 0)",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::MailboxRecv(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(5)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::list([])
        )));
    });
}

#[test]
fn mailbox_recv_reports_which_mailbox_is_ready() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let submitted = driver
            .submit_source(
                endpoint(36),
                root_source(
                    "let first = mailbox()\n\
                     let second = mailbox()\n\
                     mailbox_send(second[1], \"second\")\n\
                     let ready = mailbox_recv([first[0], second[0]], 0)\n\
                     return ready[0][0] == second[0] && ready[0][1][0] == \"second\"",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::MailboxRecv(_),
                ..
            }
        ));

        compio::time::sleep(Duration::from_millis(5)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::bool(true)
        )));
    });
}

#[test]
fn mailbox_caps_are_directional() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let error = driver
            .submit_source(
                endpoint(35),
                root_source(
                    "let caps = mailbox()\n\
                     return mailbox_recv([caps[1]], 0)",
                ),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error.source(),
            Some(SourceTaskError::TaskManager(TaskManagerError::Task(
                TaskError::Runtime(RuntimeError::InvalidMailboxCapability {
                    operation: "recv",
                    ..
                })
            )))
        ));
    });
}

#[test]
fn driver_submit_source_sets_endpoint_context() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let endpoint = endpoint(5);
        let submitted = driver
            .submit_source(endpoint, root_source("return endpoint()"))
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(endpoint)
        ));
    });
}

#[test]
fn source_agent_prompt_runs_mica_query_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    assert!(
                        options
                            .map_get(&Value::symbol(Symbol::intern("tools")))
                            .and_then(|tools| tools.with_list(|tools| Some(!tools.is_empty())).flatten())
                            .unwrap_or(false)
                    );
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_1"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("mica_query"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string(
                                                            "{\"query\":\"return one source/SelectedPath(endpoint(), ?path)\"}",
                                                        ),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("src/tests.rs")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The selected path is src/tests.rs."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(35);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"what path is selected?\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("The selected path is src/tests.rs."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent tool prompt task did not complete");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"mica_query\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 let tool_status = nothing\n\
                 let error = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   tool_status = found[:stored_status]\n\
                   error = found[:stored_error]\n\
                   break\n\
                 end\n\
                 let transcript = one source/AgentToolTranscriptText(request, ?text)\n\
                 return [request != nothing, result[:status], result[:value], tool_status, error, transcript]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent tool fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("src/tests.rs"));
                assert_eq!(values[3], Value::string("complete"));
                assert_eq!(values[4], Value::string(""));
                assert!(
                    values[5]
                        .with_str(|transcript| transcript.contains("src/tests.rs"))
                        .unwrap_or(false)
                );
            })
            .expect("expected source agent tool inspection tuple");
    });
}

#[test]
fn source_agent_prompt_runs_source_file_window_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    let has_window_tool = options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .and_then(|tools| {
                            tools
                                .with_list(|tools| {
                                    tools.iter().any(|tool| {
                                        tool.map_get(&Value::symbol(Symbol::intern("function")))
                                            .and_then(|function| {
                                                function.map_get(&Value::symbol(Symbol::intern("name")))
                                            })
                                            == Some(Value::string("source_file_window"))
                                    })
                                })
                        })
                        .unwrap_or(false);
                    assert!(has_window_tool);
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_window"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_file_window"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string(
                                                            "{\"path\":\"src/tests.rs\",\"start_line\":14,\"line_count\":2}",
                                                        ),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("src/tests.rs")
                                                    && content.contains("use crate")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The file window includes the test imports."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(36);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"show me the local test import window\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("The file window includes the test imports."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent file-window prompt task did not complete");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"source_file_window\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   break\n\
                 end\n\
                 return [request != nothing, result[:status], result[:value][:path], result[:value][:start_line], result[:value][:end_line], result[:value][:lines][0][:line], result[:value][:lines][1][:line], string_contains(result[:rendered], \"use crate\")]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent file-window fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("src/tests.rs"));
                assert_eq!(values[3], Value::int(14).unwrap());
                assert_eq!(values[4], Value::int(15).unwrap());
                assert_eq!(values[5], Value::int(14).unwrap());
                assert_eq!(values[6], Value::int(15).unwrap());
                assert_eq!(values[7], Value::bool(true));
            })
            .expect("expected source agent file-window inspection tuple");
    });
}

#[test]
fn source_agent_prompt_synthesizes_after_tool_limit() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    assert!(
                        options
                            .map_get(&Value::symbol(Symbol::intern("tools")))
                            .and_then(|tools| tools.with_list(|tools| Some(!tools.is_empty())).flatten())
                            .unwrap_or(false)
                    );
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_window"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_file_window"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string(
                                                            "{\"path\":\"src/tests.rs\",\"start_line\":14,\"line_count\":2}",
                                                        ),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                let options = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("options")))
                    .expect("final request should include options");
                assert!(
                    options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .is_none(),
                    "final synthesis request should not expose tools"
                );
                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("src/tests.rs")
                                                    && content.contains("use crate")
                                            })
                                        })
                                        .unwrap_or(false)
                            }) && messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("user"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("Stop using tools now")
                                                    && content.contains("Do not dump raw tool output")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The relevant file is src/tests.rs."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(42);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/RuntimeConfig(#source/config_agent_tool_call_limit, 1)\n\
                     assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"find the file\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("The relevant file is src/tests.rs."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            completed,
            "source agent synthesis-after-limit task did not complete"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let count = 0\n\
                 for request_row in source/AgentToolRequest(?tool_request, ?ordinal, \"source_file_window\", ?args, ?status)\n\
                   count = count + 1\n\
                 end\n\
                 return count"
                    .to_owned(),
            )
            .await
            .unwrap();
        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(1).unwrap()
        ));
    });
}

#[test]
fn source_agent_prompt_runs_multiple_file_window_tool_calls() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let tool_calls = (0..4)
                        .map(|index| {
                            Value::map([
                                (
                                    Value::symbol(Symbol::intern("id")),
                                    Value::string(format!("call_window_{index}")),
                                ),
                                (
                                    Value::symbol(Symbol::intern("type")),
                                    Value::string("function"),
                                ),
                                (
                                    Value::symbol(Symbol::intern("function")),
                                    Value::map([
                                        (
                                            Value::symbol(Symbol::intern("name")),
                                            Value::string("source_file_window"),
                                        ),
                                        (
                                            Value::symbol(Symbol::intern("arguments")),
                                            Value::string(format!(
                                                "{{\"path\":\"src/tests.rs\",\"start_line\":{},\"line_count\":1}}",
                                                14 + index
                                            )),
                                        ),
                                    ]),
                                ),
                            ])
                        })
                        .collect::<Vec<_>>();
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list(tool_calls),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                let tool_message_count = messages
                    .with_list(|messages| {
                        messages
                            .iter()
                            .filter(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                            })
                            .count()
                    })
                    .unwrap_or(0);
                assert_eq!(tool_message_count, 4);
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("I inspected four file windows."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(41);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"inspect several file windows\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("I inspected four file windows."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            completed,
            "source agent multi-window prompt task did not complete"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let count = 0\n\
                 let fourth_complete = false\n\
                 for request_row in source/AgentToolRequest(?tool_request, ?ordinal, \"source_file_window\", ?args, ?status)\n\
                   count = count + 1\n\
                   if request_row[:ordinal] == 4 && request_row[:status] == \"complete\"\n\
                     fourth_complete = true\n\
                   end\n\
                 end\n\
                 return [count, fourth_complete]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent multi-window fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::int(4).unwrap());
                assert_eq!(values[1], Value::bool(true));
            })
            .expect("expected source agent multi-window inspection tuple");
    });
}

#[test]
fn source_agent_prompt_runs_source_search_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    let has_search_tool = options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .and_then(|tools| {
                            tools.with_list(|tools| {
                                tools.iter().any(|tool| {
                                    tool.map_get(&Value::symbol(Symbol::intern("function")))
                                        .and_then(|function| {
                                            function.map_get(&Value::symbol(Symbol::intern("name")))
                                        })
                                        == Some(Value::string("source_search"))
                                })
                            })
                        })
                        .unwrap_or(false);
                    assert!(has_search_tool);
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_search"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_search"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string(
                                                            "{\"query\":\"sync_view_tree\",\"scope\":\"all\",\"limit\":4}",
                                                        ),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("sync_view_tree")
                                                    && content.contains("semantic_status")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The search tool returned source leads."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(37);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"search for sync_view_tree\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("The search tool returned source leads."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent search prompt task did not complete");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"source_search\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   break\n\
                 end\n\
                 return [request != nothing, result[:status], result[:value][:query], result[:value][:scope], result[:value][:limit], result[:value][:semantic_status], string_contains(result[:rendered], \"source_search\")]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent search fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("sync_view_tree"));
                assert_eq!(values[3], Value::string("all"));
                assert_eq!(values[4], Value::int(4).unwrap());
                assert_eq!(values[5], Value::string("vector_not_ready"));
                assert_eq!(values[6], Value::bool(true));
            })
            .expect("expected source agent search inspection tuple");
    });
}

#[test]
fn source_agent_prompt_runs_source_symbol_context_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    let has_symbol_tool = options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .and_then(|tools| {
                            tools.with_list(|tools| {
                                tools.iter().any(|tool| {
                                    tool.map_get(&Value::symbol(Symbol::intern("function")))
                                        .and_then(|function| {
                                            function.map_get(&Value::symbol(Symbol::intern("name")))
                                        })
                                        == Some(Value::string("source_symbol_context"))
                                })
                            })
                        })
                        .unwrap_or(false);
                    assert!(has_symbol_tool);
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_symbol"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_symbol_context"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string("{}"),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("sync_view_tree")
                                                    && content.contains("src/tests.rs")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The symbol context includes the selected definition."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(38);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     assert source/SelectedSymbol(endpoint(), \"sync_view_tree\")\n\
                     assert source/SelectedSymbolName(endpoint(), \"sync_view_tree\")\n\
                     assert source/SelectedSymbolKind(endpoint(), \"verb\")\n\
                     assert source/SelectedDefinitionPath(endpoint(), \"src/tests.rs\")\n\
                     assert source/SelectedDefinitionStartLine(endpoint(), 14)\n\
                     assert source/SelectedDefinitionEndLine(endpoint(), 15)\n\
                     assert source/SelectedDefinitionStartByte(endpoint(), 0)\n\
                     assert source/SelectedDefinitionEndByte(endpoint(), 0)\n\
                     assert source/SelectedSymbolProvider(endpoint(), \"test\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"explain the selected symbol\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string(
                                    "The symbol context includes the selected definition."
                                ))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent symbol prompt task did not complete");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"source_symbol_context\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   break\n\
                 end\n\
                 return [request != nothing, result[:status], result[:value][:name], result[:value][:kind], result[:value][:definition][:path], result[:value][:definition][:start_line], result[:value][:source], string_contains(result[:rendered], \"use crate\")]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent symbol fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("sync_view_tree"));
                assert_eq!(values[3], Value::string("verb"));
                assert_eq!(values[4], Value::string("src/tests.rs"));
                assert_eq!(values[5], Value::int(14).unwrap());
                assert_eq!(values[6], Value::string("selected_symbol"));
                assert_eq!(values[7], Value::bool(true));
            })
            .expect("expected source agent symbol inspection tuple");
    });
}

#[test]
fn source_agent_prompt_runs_source_find_references_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    let has_references_tool = options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .and_then(|tools| {
                            tools.with_list(|tools| {
                                tools.iter().any(|tool| {
                                    tool.map_get(&Value::symbol(Symbol::intern("function")))
                                        .and_then(|function| {
                                            function.map_get(&Value::symbol(Symbol::intern("name")))
                                        })
                                        == Some(Value::string("source_find_references"))
                                })
                            })
                        })
                        .unwrap_or(false);
                    assert!(has_references_tool);
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_references"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_find_references"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string("{\"limit\":5}"),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("source_find_references")
                                                    && content.contains("sync_view_tree")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("The references tool checked the selected symbol."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(39);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     assert source/SelectedSymbol(endpoint(), \"sync_view_tree\")\n\
                     assert source/SelectedSymbolName(endpoint(), \"sync_view_tree\")\n\
                     assert source/SelectedSymbolKind(endpoint(), \"verb\")\n\
                     assert source/SelectedDefinitionPath(endpoint(), \"src/tests.rs\")\n\
                     assert source/SelectedDefinitionStartLine(endpoint(), 14)\n\
                     assert source/SelectedDefinitionEndLine(endpoint(), 15)\n\
                     assert source/SelectedDefinitionStartByte(endpoint(), 0)\n\
                     assert source/SelectedDefinitionEndByte(endpoint(), 0)\n\
                     assert source/SelectedSymbolProvider(endpoint(), \"test\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"find references for the selected symbol\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string(
                                    "The references tool checked the selected symbol."
                                ))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            completed,
            "source agent references prompt task did not complete"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"source_find_references\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   break\n\
                 end\n\
                 return [request != nothing, result[:status], result[:value][:name], result[:value][:kind], result[:value][:limit], result[:value][:count], result[:value][:source], string_contains(result[:rendered], \"source_find_references\")]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent references fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("sync_view_tree"));
                assert_eq!(values[3], Value::string("verb"));
                assert_eq!(values[4], Value::int(5).unwrap());
                assert_eq!(values[5], Value::int(0).unwrap());
                assert_eq!(values[6], Value::string("selected_symbol"));
                assert_eq!(values[7], Value::bool(true));
            })
            .expect("expected source agent references inspection tuple");
    });
}

#[test]
fn source_agent_prompt_records_pending_proposal_tool_call() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let handler = Arc::new(move |request: mica_runtime::ExternalRequest| {
            let call_index = handler_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                let messages = request
                    .payload
                    .map_get(&Value::symbol(Symbol::intern("messages")))
                    .expect("host request should include messages");
                if call_index == 0 {
                    let options = request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("options")))
                        .expect("tool request should include options");
                    let has_proposal_tool = options
                        .map_get(&Value::symbol(Symbol::intern("tools")))
                        .and_then(|tools| {
                            tools.with_list(|tools| {
                                tools.iter().any(|tool| {
                                    tool.map_get(&Value::symbol(Symbol::intern("function")))
                                        .and_then(|function| {
                                            function.map_get(&Value::symbol(Symbol::intern("name")))
                                        })
                                        == Some(Value::string("source_create_pending_note"))
                                })
                            })
                        })
                        .unwrap_or(false);
                    assert!(has_proposal_tool);
                    return Value::map([
                        (
                            Value::symbol(Symbol::intern("model")),
                            Value::string("openrouter/test-model"),
                        ),
                        (
                            Value::symbol(Symbol::intern("choices")),
                            Value::list([Value::map([(
                                Value::symbol(Symbol::intern("message")),
                                Value::map([
                                    (
                                        Value::symbol(Symbol::intern("role")),
                                        Value::string("assistant"),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("content")),
                                        Value::nothing(),
                                    ),
                                    (
                                        Value::symbol(Symbol::intern("tool_calls")),
                                        Value::list([Value::map([
                                            (
                                                Value::symbol(Symbol::intern("id")),
                                                Value::string("call_proposal"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("type")),
                                                Value::string("function"),
                                            ),
                                            (
                                                Value::symbol(Symbol::intern("function")),
                                                Value::map([
                                                    (
                                                        Value::symbol(Symbol::intern("name")),
                                                        Value::string("source_create_pending_note"),
                                                    ),
                                                    (
                                                        Value::symbol(Symbol::intern("arguments")),
                                                        Value::string(
                                                            "{\"title\":\"Document selected symbol\",\"body\":\"Add a note explaining sync_view_tree.\"}",
                                                        ),
                                                    ),
                                                ]),
                                            ),
                                        ])]),
                                    ),
                                ]),
                            )])]),
                        ),
                    ]);
                }

                assert!(
                    messages
                        .with_list(|messages| {
                            messages.iter().any(|message| {
                                message.map_get(&Value::symbol(Symbol::intern("role")))
                                    == Some(Value::string("tool"))
                                    && message
                                        .map_get(&Value::symbol(Symbol::intern("content")))
                                        .and_then(|content| {
                                            content.with_str(|content| {
                                                content.contains("pending")
                                                    && content.contains("Document selected symbol")
                                            })
                                        })
                                        .unwrap_or(false)
                            })
                        })
                        .unwrap_or(false)
                );
                Value::map([
                    (
                        Value::symbol(Symbol::intern("model")),
                        Value::string("openrouter/test-model"),
                    ),
                    (
                        Value::symbol(Symbol::intern("choices")),
                        Value::list([Value::map([(
                            Value::symbol(Symbol::intern("message")),
                            Value::map([(
                                Value::symbol(Symbol::intern("content")),
                                Value::string("I recorded a pending note proposal."),
                            )]),
                        )])]),
                    ),
                ])
            }) as crate::types::ExternalRequestFuture
        });
        let mut runner = SourceRunner::new_empty();
        load_source_app(&mut runner);
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(40);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn_with_external_handler(runner, handler).unwrap();
        let submitted = driver
            .submit_source(
                endpoint,
                root_source(
                    "assert source/SelectedPath(endpoint(), \"src/tests.rs\")\n\
                     return source/run_agent_prompt(endpoint(), #web, \"record a note proposal about this symbol\")",
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Suspended {
                kind: SuspendKind::ExternalRequest(_),
                ..
            }
        ));

        let mut completed = false;
        for _ in 0..80 {
            if driver.drain_events().iter().any(|event| {
                matches!(
                    event,
                    DriverEvent::TaskCompleted { task_id, value }
                        if *task_id == submitted.task_id
                            && value
                                .map_get(&Value::symbol(Symbol::intern("text")))
                                == Some(Value::string("I recorded a pending note proposal."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            completed,
            "source agent proposal prompt task did not complete"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let report = driver
            .submit_root_source_report(
                "let request = nothing\n\
                 for request_row in source/AgentToolRequest(?tool_request, 1, \"source_create_pending_note\", ?args, \"complete\")\n\
                   request = request_row[:tool_request]\n\
                   break\n\
                 end\n\
                 let result = nothing\n\
                 for found in source/AgentToolResult(request, ?stored_result, ?stored_status, ?stored_error)\n\
                   result = found[:stored_result]\n\
                   break\n\
                 end\n\
                 let proposal = from_literal(result[:value][:proposal])\n\
                 let accepted_before = one source/AgentProposalStatus(proposal, ?status)\n\
                 let accepted = source/accept_agent_proposal(proposal)\n\
                 let accepted_after = one source/AgentProposalStatus(proposal, ?status)\n\
                 return [request != nothing, result[:status], result[:value][:kind], result[:value][:title], accepted_before, accepted, accepted_after, source/AgentProposal(proposal), result[:value][:status]]"
                    .to_owned(),
            )
            .await
            .unwrap();
        let TaskOutcome::Complete { value, .. } = report.outcome else {
            panic!(
                "expected source agent proposal fact inspection to complete, got {:?}",
                report.outcome
            );
        };
        value
            .with_list(|values| {
                assert_eq!(values[0], Value::bool(true));
                assert_eq!(values[1], Value::string("complete"));
                assert_eq!(values[2], Value::string("note"));
                assert_eq!(values[3], Value::string("Document selected symbol"));
                assert_eq!(values[4], Value::string("pending"));
                assert_eq!(values[5], Value::bool(true));
                assert_eq!(values[6], Value::string("accepted"));
                assert_eq!(values[7], Value::bool(true));
                assert_eq!(values[8], Value::string("pending"));
            })
            .expect("expected source agent proposal inspection tuple");
    });
}

#[test]
fn driver_runs_bounded_read_only_source_query_as_endpoint_actor() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_identity(:web)\n\
                 make_identity(:lamp)\n\
                 make_relation(:CanRead, 2)\n\
                 make_relation(:CanWrite, 2)\n\
                 make_relation(:ThingName, 2)\n\
                 assert CanRead(#web, :ThingName)\n\
                 assert CanWrite(#web, :ThingName)\n\
                 assert ThingName(#lamp, \"Lamp\")\n",
            )
            .unwrap();
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(61);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();

        let report = driver
            .run_read_only_source_query(
                endpoint,
                "let names = []\n\
                 for found in ThingName(?thing, ?name)\n\
                   names = [@names, found[:name]]\n\
                 end\n\
                 return names"
                    .to_owned(),
                ReadOnlySourceQueryOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(report.status, ReadOnlySourceQueryStatus::Complete);
        assert_eq!(report.value, Some(Value::list([Value::string("Lamp")])));
        assert_eq!(report.rendered, "[\"Lamp\"]");
        assert!(!report.rendered_truncated);
        assert_eq!(report.diagnostics, Vec::<String>::new());
    });
}

#[test]
fn driver_read_only_source_query_rejects_mutation_and_effects() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "make_identity(:web)\n\
                 make_identity(:lamp)\n\
                 make_relation(:CanRead, 2)\n\
                 make_relation(:CanWrite, 2)\n\
                 make_relation(:ThingName, 2)\n\
                 assert CanRead(#web, :ThingName)\n\
                 assert CanWrite(#web, :ThingName)\n\
                 assert ThingName(#lamp, \"Lamp\")\n",
            )
            .unwrap();
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(62);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();

        let mutation = driver
            .run_read_only_source_query(
                endpoint,
                "assert ThingName(#lamp, \"Desk\")\nreturn 1".to_owned(),
                ReadOnlySourceQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(mutation.status, ReadOnlySourceQueryStatus::Rejected);
        assert!(mutation.task_id.is_none());
        assert!(
            mutation
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("cannot assert or retract facts"))
        );

        let effect = driver
            .run_read_only_source_query(
                endpoint,
                "return log(:info, \"hello\")".to_owned(),
                ReadOnlySourceQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(effect.status, ReadOnlySourceQueryStatus::Rejected);
        assert!(
            effect
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("cannot call `log`"))
        );

        let spawn = driver
            .run_read_only_source_query(
                endpoint,
                "let child = spawn :inspect(#lamp) after 0\nreturn child".to_owned(),
                ReadOnlySourceQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(spawn.status, ReadOnlySourceQueryStatus::Rejected);
        assert!(
            spawn
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("cannot spawn tasks"))
        );

        let dispatch = driver
            .run_read_only_source_query(
                endpoint,
                "return #lamp:inspect(#web)".to_owned(),
                ReadOnlySourceQueryOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(dispatch.status, ReadOnlySourceQueryStatus::Rejected);
        assert!(
            dispatch
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("cannot invoke methods"))
        );
    });
}

#[test]
fn driver_read_only_source_query_bounds_rendered_output() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein("make_identity(:web)\nmake_relation(:CanRead, 2)\n")
            .unwrap();
        let web = runner.named_identity(Symbol::intern("web")).unwrap();
        let endpoint = endpoint(63);
        runner
            .open_endpoint(endpoint, Some(web), Symbol::intern("web"))
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();

        let report = driver
            .run_read_only_source_query(
                endpoint,
                "return \"abcdef\"".to_owned(),
                ReadOnlySourceQueryOptions {
                    max_output_chars: 3,
                    ..ReadOnlySourceQueryOptions::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(report.status, ReadOnlySourceQueryStatus::Complete);
        assert_eq!(report.value, Some(Value::string("abcdef")));
        assert!(report.rendered_truncated);
        assert!(report.rendered.ends_with("... truncated"));
    });
}

#[test]
fn driver_submit_invocation_overrides_request_endpoint_context() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner
            .run_filein(
                "verb report_endpoint(endpoint)\n\
                   return endpoint\n\
                 end\n",
            )
            .unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let actual_endpoint = endpoint(6);
        let stale_endpoint = endpoint(7);

        let submitted = driver
            .submit_invocation(
                actual_endpoint,
                TaskRequest {
                    principal: None,
                    actor: None,
                    endpoint: stale_endpoint,
                    authority: AuthorityContext::root(),
                    input: TaskInput::Invocation {
                        selector: Symbol::intern("report_endpoint"),
                        roles: Vec::new(),
                    },
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            submitted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::identity(actual_endpoint)
        ));
    });
}

#[test]
fn driver_routes_actor_effects_to_open_endpoints() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let endpoint = endpoint(10);
        driver
            .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
            .unwrap();

        let submitted = driver
            .submit_source(endpoint, root_source("emit(#alice, \"hello\")"))
            .await
            .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::Effect(effect)
                if effect.task_id == submitted.task_id
                    && effect.target == endpoint
                    && effect.value == Value::string("hello")
        )));
    });
}

#[test]
fn driver_stops_routing_after_endpoint_close() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let endpoint = endpoint(11);
        driver
            .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
            .unwrap();
        assert_eq!(driver.close_endpoint(endpoint), 4);

        let submitted = driver
            .submit_source(endpoint, root_source("emit(#alice, \"hello\")"))
            .await
            .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::Effect(effect)
                if effect.task_id == submitted.task_id
                    && effect.target == alice
                    && effect.value == Value::string("hello")
        )));
    });
}

#[test]
fn driver_routes_endpoint_input() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::spawn_empty().unwrap();
        let endpoint = endpoint(27);
        let submitted = driver
            .submit_source(endpoint, root_source("return read(:line)"))
            .await
            .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));
        let outcomes = driver
            .input(endpoint, Value::string("north"))
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        assert!(matches!(
            &outcomes[0],
            TaskOutcome::Complete { value, .. } if *value == Value::string("north")
        ));
    });
}

#[test]
fn driver_routes_actor_effects_to_open_endpoints_after_setup() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
        let driver = CompioTaskDriver::spawn(runner).unwrap();
        let endpoint = endpoint(28);
        driver
            .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
            .unwrap();

        let submitted = driver
            .submit_source(endpoint, root_source("emit(#alice, \"hello\")"))
            .await
            .unwrap();

        assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
        let events = driver.drain_events();
        assert!(events.iter().any(|event| matches!(
            event,
            DriverEvent::Effect(effect)
                if effect.task_id == submitted.task_id
                    && effect.target == endpoint
                    && effect.value == Value::string("hello")
        )));
        assert_eq!(driver.close_endpoint(endpoint), 4);
    });
}
