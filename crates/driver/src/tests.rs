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
fn source_generated_answer_records_reviewable_facts() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("deepseek/deepseek-v4-pro"))
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
             assert source/RuntimeConfig(#source/config_agent_model, \"openai/gpt-4.1\")\n\
             assert source/RuntimeConfig(#source/config_agent_file_context_line_limit, 17)\n\
             assert source/RuntimeConfig(#source/config_generation_provider, \"test-provider\")\n\
             assert source/RuntimeConfig(#source/config_retrieval_model, \"test-retrieval-model\")\n\
             assert source/RuntimeConfig(#source/config_retrieval_limit, 13)\n\
             return [default_model, default_provider, default_retrieval_model, default_limit, default_file_context_limit, source/generation_model(), source/agent_file_context_line_limit(), source/generation_provider(), source/retrieval_model(), source/retrieval_limit()]",
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
            assert_eq!(values[0], Value::string("deepseek/deepseek-v4-pro"));
            assert_eq!(values[1], Value::string("openrouter"));
            assert_eq!(values[2], Value::string("source-workspace"));
            assert_eq!(values[3], Value::int(8).unwrap());
            assert_eq!(values[4], Value::int(2000).unwrap());
            assert_eq!(values[5], Value::string("openai/gpt-4.1"));
            assert_eq!(values[6], Value::int(17).unwrap());
            assert_eq!(values[7], Value::string("test-provider"));
            assert_eq!(values[8], Value::string("test-retrieval-model"));
            assert_eq!(values[9], Value::int(13).unwrap());
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
fn source_agent_prompt_records_turns_and_grounded_prompt() {
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let handler = Arc::new(|request: mica_runtime::ExternalRequest| {
            Box::pin(async move {
                assert_eq!(request.service, Symbol::intern("openai"));
                assert_eq!(
                    request
                        .payload
                        .map_get(&Value::symbol(Symbol::intern("model"))),
                    Some(Value::string("deepseek/deepseek-v4-pro"))
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
                                Value::string("sync_view_tree is the source sync render hook."),
                            )]),
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
                                == Some(Value::string("sync_view_tree is the source sync render hook."))
                )
            }) {
                completed = true;
                break;
            }
            compio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(completed, "source agent prompt task did not complete");

        let report = driver
            .submit_root_source_report(
                "let user = one source/AgentTurnRole(?turn, \"user\")\n\
                 let assistant = one source/AgentTurnRole(?turn, \"assistant\")\n\
                 let assistant_text = one source/AgentTurnText(assistant, ?text)\n\
                 let model = one source/AgentTurnResolvedModel(assistant, ?model)\n\
                 let plan = one source/AgentTurnPlan(assistant, ?plan)\n\
                 let context_text = one source/AgentTurnContextText(assistant, ?text)\n\
                 let prompt = one source/AgentTurnPromptText(assistant, ?text)\n\
                 return [user != nothing, assistant != nothing, assistant_text, model, plan != nothing, string_contains(context_text, \"Current file:\"), string_contains(context_text, \"Value::int(5).unwrap()\"), string_contains(prompt, \"Current source focus:\")]".to_owned(),
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
                    Value::string("sync_view_tree is the source sync render hook.")
                );
                assert_eq!(values[3], Value::string("openrouter/test-model"));
                assert_eq!(values[4], Value::bool(true));
                assert_eq!(values[5], Value::bool(true));
                assert_eq!(values[6], Value::bool(true));
                assert_eq!(values[7], Value::bool(true));
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
