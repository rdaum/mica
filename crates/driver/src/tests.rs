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
    AuthorityContext, EmbeddingProviderKind, RuntimeError, SourceTaskError, TaskError, TaskInput,
    TaskManagerError, TaskRequest,
};
use mica_runtime::{SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::sync::Arc;
use std::time::Duration;

fn endpoint(offset: u64) -> Identity {
    Identity::new(0x00ee_0000_0000_0000 + offset).unwrap()
}

fn root_source(source: &str) -> TaskRequest {
    SourceRunner::root_source_request(source)
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
