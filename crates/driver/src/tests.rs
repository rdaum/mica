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

use crate::{CompioTaskDriver, CompioTaskDriverThread, DriverEvent};
use compio::runtime::{Runtime, time::sleep};
use mica_runtime::{AuthorityContext, SuspendKind, TaskOutcome};
use mica_runtime::{TaskInput, TaskRequest};
use mica_var::{Identity, Value};
use std::time::Duration;

fn root_source(source: &str, endpoint: Option<Identity>) -> TaskRequest {
    TaskRequest {
        principal: None,
        actor: None,
        endpoint,
        authority: AuthorityContext::root(),
        input: TaskInput::Source(source.to_owned()),
    }
}

#[test]
fn driver_runs_source_on_compio_task() {
    Runtime::new().unwrap().block_on(async {
        let driver = CompioTaskDriver::empty();
        let submitted = driver
            .submit_source(root_source("return 1 + 1", None))
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
            .submit_source(root_source("suspend(0.001)\nreturn \"awake\"", None))
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
            .submit_source(root_source("commit()\nreturn \"committed\"", None))
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
        let endpoint = Identity::new(0x00ee_0000_0000_0001).unwrap();
        let submitted = driver
            .submit_source(root_source("return read(:line)", Some(endpoint)))
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
fn thread_driver_runs_mica_work_on_compio_runtime() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(root_source("return 40 + 2", None))
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
        .submit_source(root_source("suspend(0.001)\nreturn \"awake\"", None))
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
fn thread_driver_immediately_resumes_commit() {
    let driver = CompioTaskDriverThread::spawn_empty().unwrap();
    let submitted = driver
        .submit_source(root_source("commit()\nreturn \"committed\"", None))
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
    let endpoint = Identity::new(0x00ee_0000_0000_0002).unwrap();
    let submitted = driver
        .submit_source(root_source("return read(:line)", Some(endpoint)))
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
