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

use crate::{CompioTaskDriver, DriverError, DriverEvent, DriverThreadError};
use compio::runtime::{Runtime, spawn};
use mica_runtime::{RunReport, SourceRunner, SubmittedTask, TaskRequest};
use mica_runtime::{TaskId, TaskOutcome};
use mica_var::{Identity, Symbol, Value};
use std::sync::mpsc;
use std::task::Waker;
use std::thread::{self, JoinHandle as ThreadJoinHandle};

pub struct CompioTaskDriverThread {
    commands: mpsc::Sender<DriverCommand>,
    waker: Waker,
    thread: Option<ThreadJoinHandle<()>>,
}

enum DriverCommand {
    SubmitSource {
        request: TaskRequest,
        reply: mpsc::Sender<Result<SubmittedTask, DriverError>>,
    },
    SubmitSourceReport {
        actor: Option<Symbol>,
        source: String,
        reply: mpsc::Sender<Result<RunReport, DriverError>>,
    },
    SubmitInvocation {
        request: TaskRequest,
        reply: mpsc::Sender<Result<SubmittedTask, DriverError>>,
    },
    Resume {
        task_id: TaskId,
        value: Value,
        reply: mpsc::Sender<Result<TaskOutcome, DriverError>>,
    },
    Input {
        endpoint: Identity,
        value: Value,
        reply: mpsc::Sender<Result<Vec<TaskOutcome>, DriverError>>,
    },
    DrainEvents {
        reply: mpsc::Sender<Vec<DriverEvent>>,
    },
    Shutdown,
}

impl CompioTaskDriverThread {
    pub fn spawn(runner: SourceRunner) -> Result<Self, DriverThreadError> {
        let (commands_tx, commands_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("mica-compio-driver".to_owned())
            .spawn(move || {
                let runtime = Runtime::new().expect("failed to create compio runtime");
                let waker = runtime.waker();
                ready_tx
                    .send(waker.clone())
                    .expect("driver handle was dropped before startup");
                let driver_runtime = runtime.clone();
                runtime.enter(|| run_thread_driver(driver_runtime, runner, commands_rx));
            })
            .map_err(|error| DriverThreadError::Start(error.to_string()))?;
        let waker = ready_rx
            .recv()
            .map_err(|error| DriverThreadError::Start(error.to_string()))?;
        Ok(Self {
            commands: commands_tx,
            waker,
            thread: Some(thread),
        })
    }

    pub fn spawn_empty() -> Result<Self, DriverThreadError> {
        Self::spawn(SourceRunner::new_empty())
    }

    pub fn submit_source(&self, request: TaskRequest) -> Result<SubmittedTask, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::SubmitSource { request, reply })?;
        response
            .recv()
            .map_err(|_| DriverThreadError::Closed)?
            .map_err(DriverThreadError::Driver)
    }

    pub fn submit_source_report(
        &self,
        actor: Option<Symbol>,
        source: String,
    ) -> Result<RunReport, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::SubmitSourceReport {
            actor,
            source,
            reply,
        })?;
        response
            .recv()
            .map_err(|_| DriverThreadError::Closed)?
            .map_err(DriverThreadError::Driver)
    }

    pub fn submit_invocation(
        &self,
        request: TaskRequest,
    ) -> Result<SubmittedTask, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::SubmitInvocation { request, reply })?;
        response
            .recv()
            .map_err(|_| DriverThreadError::Closed)?
            .map_err(DriverThreadError::Driver)
    }

    pub fn resume(&self, task_id: TaskId, value: Value) -> Result<TaskOutcome, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::Resume {
            task_id,
            value,
            reply,
        })?;
        response
            .recv()
            .map_err(|_| DriverThreadError::Closed)?
            .map_err(DriverThreadError::Driver)
    }

    pub fn input(
        &self,
        endpoint: Identity,
        value: Value,
    ) -> Result<Vec<TaskOutcome>, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::Input {
            endpoint,
            value,
            reply,
        })?;
        response
            .recv()
            .map_err(|_| DriverThreadError::Closed)?
            .map_err(DriverThreadError::Driver)
    }

    pub fn drain_events(&self) -> Result<Vec<DriverEvent>, DriverThreadError> {
        let (reply, response) = mpsc::channel();
        self.send(DriverCommand::DrainEvents { reply })?;
        response.recv().map_err(|_| DriverThreadError::Closed)
    }

    pub fn shutdown(mut self) -> Result<(), DriverThreadError> {
        self.send(DriverCommand::Shutdown)?;
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| DriverThreadError::Closed)?;
        }
        Ok(())
    }

    fn send(&self, command: DriverCommand) -> Result<(), DriverThreadError> {
        self.commands
            .send(command)
            .map_err(|_| DriverThreadError::Closed)?;
        self.waker.wake_by_ref();
        Ok(())
    }
}

impl Drop for CompioTaskDriverThread {
    fn drop(&mut self) {
        let _ = self.commands.send(DriverCommand::Shutdown);
        self.waker.wake_by_ref();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_thread_driver(
    runtime: Runtime,
    runner: SourceRunner,
    commands: mpsc::Receiver<DriverCommand>,
) {
    let driver = CompioTaskDriver::new(runner);
    let mut running = true;
    while running {
        while let Ok(command) = commands.try_recv() {
            match command {
                DriverCommand::SubmitSource { request, reply } => {
                    let driver = driver.clone();
                    spawn(async move {
                        let _ = reply.send(driver.run_source_request(request));
                    })
                    .detach();
                }
                DriverCommand::SubmitSourceReport {
                    actor,
                    source,
                    reply,
                } => {
                    let driver = driver.clone();
                    spawn(async move {
                        let _ = reply.send(driver.run_source_report(actor, source));
                    })
                    .detach();
                }
                DriverCommand::SubmitInvocation { request, reply } => {
                    let driver = driver.clone();
                    spawn(async move {
                        let _ = reply.send(driver.run_invocation_request(request));
                    })
                    .detach();
                }
                DriverCommand::Resume {
                    task_id,
                    value,
                    reply,
                } => {
                    let driver = driver.clone();
                    spawn(async move {
                        let _ = reply.send(driver.run_resume(task_id, value));
                    })
                    .detach();
                }
                DriverCommand::Input {
                    endpoint,
                    value,
                    reply,
                } => {
                    let driver = driver.clone();
                    spawn(async move {
                        let _ = reply.send(driver.run_input(endpoint, value));
                    })
                    .detach();
                }
                DriverCommand::DrainEvents { reply } => {
                    while runtime.run() {}
                    let _ = reply.send(driver.drain_events());
                }
                DriverCommand::Shutdown => running = false,
            }
        }
        while runtime.run() {}
        if running {
            runtime.poll();
        }
    }
}
