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

use crate::{CompioTaskDriver, CompioTaskDriverPool, CompioTaskDriverThread, DriverEvent};
use compio::runtime::{Runtime, time::sleep};
use mica_runtime::TaskRequest;
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
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
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
fn timed_suspend_wakes_and_resumes_task() {
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
        let submitted = driver
            .submit_source(endpoint(2), root_source("suspend(0.001)\nreturn \"awake\""))
            .await
            .unwrap();
        assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));

        sleep(Duration::from_millis(20)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::string("awake")
        )));
    });
}

#[test]
fn commit_yields_and_immediately_resumes_task() {
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
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

        sleep(Duration::from_millis(5)).await;

        assert!(driver.drain_events().iter().any(|event| matches!(
            event,
            DriverEvent::TaskCompleted { task_id, value }
                if *task_id == submitted.task_id && *value == Value::string("committed")
        )));
    });
}

#[test]
fn endpoint_input_resumes_reading_task() {
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
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
fn driver_submit_source_sets_endpoint_context() {
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
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
fn driver_routes_actor_effects_to_open_endpoints() {
    Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
        let driver = CompioTaskDriver::new(runner);
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
    Runtime::new().unwrap().block_on(async {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:alice)").unwrap();
        let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
        let driver = CompioTaskDriver::new(runner);
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
fn thread_driver_runs_mica_work_on_compio_runtime() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(20), root_source("return 40 + 2"))
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Complete { value, .. } if value == Value::int(42).unwrap()
    ));
    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::int(42).unwrap()
    )));
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_wakes_timed_suspension() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(
            endpoint(21),
            root_source("suspend(0.001)\nreturn \"awake\""),
        )
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));
    std::thread::sleep(Duration::from_millis(20));

    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("awake")
    )));
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_drain_events_pumps_ready_timer() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(22), root_source("suspend(0)\nreturn \"awake\""))
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));
    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("awake")
    )));
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_reports_timer_resume_failure() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(
            endpoint(23),
            root_source("suspend(0)\nemit(\"missing target\")"),
        )
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Suspended { .. }));
    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskFailed { task_id, error }
            if *task_id == submitted.task_id
                && error.contains("emit expects target identity and value")
    )));
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_immediately_resumes_commit() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(endpoint(24), root_source("commit()\nreturn \"committed\""))
        .unwrap();

    assert!(matches!(
        submitted.outcome,
        TaskOutcome::Suspended {
            kind: SuspendKind::Commit,
            ..
        }
    ));
    std::thread::sleep(Duration::from_millis(20));

    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("committed")
    )));
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_routes_endpoint_input() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let endpoint = endpoint(25);
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
    driver.shutdown().unwrap();
}

#[test]
fn thread_driver_routes_actor_effects_to_open_endpoints() {
    let mut runner = SourceRunner::new_empty();
    runner.run_source("make_identity(:alice)").unwrap();
    let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
    let driver = CompioTaskDriverThread::spawn(runner).unwrap();
    let endpoint = endpoint(26);
    driver
        .open_endpoint(endpoint, Some(alice), Symbol::intern("telnet"))
        .unwrap();

    let submitted = driver
        .submit_source(endpoint, root_source("emit(#alice, \"hello\")"))
        .unwrap();

    assert!(matches!(submitted.outcome, TaskOutcome::Complete { .. }));
    let events = driver.drain_events().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        DriverEvent::Effect(effect)
            if effect.task_id == submitted.task_id
                && effect.target == endpoint
                && effect.value == Value::string("hello")
    )));
    assert_eq!(driver.close_endpoint(endpoint).unwrap(), 4);
    driver.shutdown().unwrap();
}

#[test]
fn dispatcher_pool_routes_endpoint_input() {
    let driver = CompioTaskDriverPool::spawn_empty().unwrap();
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
fn dispatcher_pool_routes_actor_effects_to_open_endpoints() {
    let mut runner = SourceRunner::new_empty();
    runner.run_source("make_identity(:alice)").unwrap();
    let alice = Identity::new(0x00e0_0000_0000_0000).unwrap();
    let driver = CompioTaskDriverPool::spawn(runner).unwrap();
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
