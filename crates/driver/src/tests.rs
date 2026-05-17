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
use mica_runtime::{RuntimeError, SourceTaskError, TaskError, TaskManagerError, TaskRequest};
use mica_runtime::{SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::time::Duration;

fn endpoint(offset: u64) -> Identity {
    Identity::new(0x00ee_0000_0000_0000 + offset).unwrap()
}

fn root_source(source: &str) -> TaskRequest {
    SourceRunner::root_source_request(source)
}

#[test]
fn driver_runs_source_on_compio_task() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(1), root_source("return 1 + 1"))
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
}

#[test]
fn driver_events_can_be_awaited() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(29), root_source("return 3 + 4"))
        .unwrap();

    let events = compio::runtime::Runtime::new()
        .unwrap()
        .block_on(driver.wait_events());

    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::int(7).unwrap()
    )));
}

#[test]
fn timed_suspend_wakes_and_resumes_task() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(2), root_source("suspend(0.001)\nreturn \"awake\""))
        .unwrap();
    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

    std::thread::sleep(Duration::from_millis(20));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("awake")
    )));
}

#[test]
fn commit_yields_and_immediately_resumes_task() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(3), root_source("commit()\nreturn \"committed\""))
        .unwrap();
    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::Commit,
            ..
        }
    ));

    std::thread::sleep(Duration::from_millis(20));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("committed")
    )));
}

#[test]
fn spawn_commits_parent_and_runs_child_task() {
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
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::Spawn(_),
            ..
        }
    ));

    std::thread::sleep(Duration::from_millis(20));

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
}

#[test]
fn endpoint_input_resumes_reading_task() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let endpoint = endpoint(4);
    let submitted = driver
        .submit_source(endpoint, root_source("return read(:line)"))
        .unwrap();
    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::WaitingForInput(_),
            ..
        }
    ));

    let outcomes = driver.input(endpoint, Value::string("look")).unwrap();

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        &outcomes[0],
        TaskOutcome::Complete { value, .. } if *value == Value::string("look")
    ));
}

#[test]
fn mailbox_recv_drains_messages_sent_before_wait() {
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
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::Spawn(_),
            ..
        }
    ));

    std::thread::sleep(Duration::from_millis(20));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id
                && value.with_list(|groups| groups.len()) == Some(1)
                && value.with_list(|groups| groups[0].with_list(|group| {
                    group.len() == 2 && group[1] == Value::list([Value::string("done")])
                })) == Some(Some(true))
    )));
}

#[test]
fn mailbox_recv_waits_until_sender_commits() {
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
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

    std::thread::sleep(Duration::from_millis(30));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id
                && value.with_list(|groups| groups.len()) == Some(1)
                && value.with_list(|groups| groups[0].with_list(|group| {
                    group.len() == 2 && group[1] == Value::list([Value::string("late")])
                })) == Some(Some(true))
    )));
}

#[test]
fn mailbox_recv_zero_timeout_returns_empty_list() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(
            endpoint(34),
            root_source(
                "let caps = mailbox()\n\
                 return mailbox_recv([caps[0]], 0)",
            ),
        )
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::MailboxRecv(_),
            ..
        }
    ));

    std::thread::sleep(Duration::from_millis(5));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::list([])
    )));
}

#[test]
fn mailbox_recv_reports_which_mailbox_is_ready() {
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
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::MailboxRecv(_),
            ..
        }
    ));

    std::thread::sleep(Duration::from_millis(5));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::bool(true)
    )));
}

#[test]
fn mailbox_caps_are_directional() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let error = driver
        .submit_source(
            endpoint(35),
            root_source(
                "let caps = mailbox()\n\
                 return mailbox_recv([caps[1]], 0)",
            ),
        )
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
}

#[test]
fn driver_submit_source_sets_endpoint_context() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let endpoint = endpoint(5);
    let submitted = driver
        .submit_source(endpoint, root_source("return endpoint()"))
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Complete { value, .. } if value == Value::identity(endpoint)
    ));
}

#[test]
fn driver_routes_actor_effects_to_open_endpoints() {
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
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::Effect(effect)
            if effect.task_id == submitted.task_id
                && effect.target == endpoint
                && effect.value == Value::string("hello")
    )));
}

#[test]
fn driver_stops_routing_after_endpoint_close() {
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
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::Effect(effect)
            if effect.task_id == submitted.task_id
                && effect.target == alice
                && effect.value == Value::string("hello")
    )));
}

#[test]
fn driver_routes_endpoint_input() {
    let driver = CompioTaskDriver::spawn_empty().unwrap();
    let endpoint = endpoint(27);
    let submitted = driver
        .submit_source(endpoint, root_source("return read(:line)"))
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));
    let outcomes = driver.input(endpoint, Value::string("north")).unwrap();

    assert_eq!(outcomes.len(), 1);
    assert!(matches!(
        &outcomes[0],
        TaskOutcome::Complete { value, .. } if *value == Value::string("north")
    ));
}

#[test]
fn driver_routes_actor_effects_to_open_endpoints_after_setup() {
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
}
