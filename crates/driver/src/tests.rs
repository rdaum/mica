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

    std::thread::sleep(Duration::from_millis(5));

    assert!(driver.drain_events().iter().any(|event| matches!(
        event,
        DriverEvent::TaskCompleted { task_id, value }
            if *task_id == submitted.task_id && *value == Value::string("committed")
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
