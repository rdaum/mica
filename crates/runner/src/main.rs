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

use clap::{Parser, Subcommand, ValueEnum};
use mica_compiler::parse;
use mica_driver::{CompioTaskDriver, DriverError, DriverEvent};
use mica_relation_kernel::FjallDurabilityMode;
use mica_runtime::{FileinMode, SourceRunner, SuspendKind, TaskOutcome};
use mica_var::{Identity, Symbol};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

const REPL_SETTLE_LIMIT: Duration = Duration::from_millis(50);
const CLI_ENDPOINT_ID: u64 = 0x00ee_0000_0000_0000;

#[derive(Parser)]
#[command(name = "mica", about = "Run Mica source, fileins, fileouts, and REPLs")]
struct Cli {
    #[arg(long, global = true, value_enum, default_value_t = StorageMode::Memory)]
    storage: StorageMode,
    #[arg(long, global = true)]
    store: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = DurabilityMode::Relaxed)]
    durability: DurabilityMode,
    #[arg(long, global = true, value_name = "IDENTITY")]
    actor: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum StorageMode {
    Memory,
    Fjall,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum DurabilityMode {
    Relaxed,
    Strict,
}

impl From<DurabilityMode> for FjallDurabilityMode {
    fn from(value: DurabilityMode) -> Self {
        match value {
            DurabilityMode::Relaxed => Self::Relaxed,
            DurabilityMode::Strict => Self::Strict,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    Run {
        file: PathBuf,
    },
    Filein {
        #[arg(long)]
        unit: Option<String>,
        #[arg(long)]
        replace: bool,
        file: PathBuf,
    },
    Fileout {
        unit: String,
        output: Option<PathBuf>,
    },
    Eval {
        #[arg(required = true, trailing_var_arg = true)]
        source: Vec<String>,
    },
    Repl,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command.as_ref().unwrap_or(&Command::Repl) {
        Command::Run { file } => {
            let source = fs::read_to_string(file)
                .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
            let session = open_cli_session(&cli, Symbol::intern("cli"))?;
            let report = submit_cli_source(&session, &cli, source)?;
            print_report_and_follow(&session.driver, report);
            let _ = session.driver.close_endpoint(session.endpoint);
            Ok(())
        }
        Command::Filein {
            unit,
            replace,
            file,
        } => {
            reject_actor(&cli)?;
            let source = fs::read_to_string(file)
                .map_err(|error| format!("failed to read {}: {error}", file.display()))?;
            let mut runner = open_runner(&cli)?;
            if let Some(unit) = unit {
                let mode = if *replace {
                    FileinMode::Replace
                } else {
                    FileinMode::Add
                };
                let report = runner
                    .run_filein_with_unit(
                        Symbol::intern(unit.trim_start_matches(':')),
                        &source,
                        mode,
                    )
                    .map_err(format_source_error)?;
                for report in report.reports {
                    print_report(report);
                }
            } else {
                for report in runner.run_filein(&source).map_err(format_source_error)? {
                    print_report(report);
                }
            }
            Ok(())
        }
        Command::Fileout { unit, output } => {
            reject_actor(&cli)?;
            let runner = open_runner(&cli)?;
            let source = runner
                .fileout_unit(Symbol::intern(unit.trim_start_matches(':')))
                .map_err(format_source_error)?;
            if let Some(output) = output {
                fs::write(output, source)
                    .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            } else {
                println!("{source}");
            }
            Ok(())
        }
        Command::Eval { source } => {
            let source = source.join(" ");
            let session = open_cli_session(&cli, Symbol::intern("cli"))?;
            let report = submit_cli_source(&session, &cli, source)?;
            print_report_and_follow(&session.driver, report);
            let _ = session.driver.close_endpoint(session.endpoint);
            Ok(())
        }
        Command::Repl => repl(&cli),
    }
}

struct CliSession {
    driver: CompioTaskDriver,
    endpoint: Identity,
}

fn submit_cli_source(
    session: &CliSession,
    cli: &Cli,
    source: String,
) -> Result<mica_runtime::RunReport, String> {
    session
        .driver
        .submit_source_report(
            session.endpoint,
            cli.actor.as_deref().map(actor_symbol),
            source,
        )
        .map_err(format_driver_error)
}

fn actor_symbol(actor: &str) -> Symbol {
    Symbol::intern(actor.trim().trim_start_matches('#').trim_start_matches(':'))
}

fn reject_actor(cli: &Cli) -> Result<(), String> {
    if cli.actor.is_some() {
        return Err("--actor is only supported for run, eval, and repl".to_owned());
    }
    Ok(())
}

fn open_runner(cli: &Cli) -> Result<SourceRunner, String> {
    let use_fjall = cli.storage == StorageMode::Fjall || cli.store.is_some();
    if !use_fjall {
        return Ok(SourceRunner::new_empty());
    }
    let store = cli
        .store
        .as_ref()
        .ok_or_else(|| "--store is required with --storage fjall".to_owned())?;
    SourceRunner::open_fjall(store, cli.durability.into())
}

fn open_cli_session(cli: &Cli, protocol: Symbol) -> Result<CliSession, String> {
    let runner = open_runner(cli)?;
    let actor = cli
        .actor
        .as_deref()
        .map(actor_symbol)
        .map(|actor| runner.named_identity(actor).map_err(format_source_error))
        .transpose()?;
    let driver = CompioTaskDriver::spawn(runner).map_err(format_driver_error)?;
    let endpoint = cli_endpoint();
    driver
        .open_endpoint(endpoint, actor, protocol)
        .map_err(format_driver_error)?;
    Ok(CliSession { driver, endpoint })
}

fn cli_endpoint() -> Identity {
    Identity::new(CLI_ENDPOINT_ID).unwrap()
}

fn repl(cli: &Cli) -> Result<(), String> {
    let mut editor =
        DefaultEditor::new().map_err(|error| format!("failed to initialize repl: {error}"))?;
    let session = open_cli_session(cli, Symbol::intern("repl"))?;
    let result = repl_loop(cli, &session, &mut editor);
    let _ = session.driver.close_endpoint(session.endpoint);
    result
}

fn repl_loop(cli: &Cli, session: &CliSession, editor: &mut DefaultEditor) -> Result<(), String> {
    let mut buffer = String::new();

    println!("Mica REPL. Enter :quit to exit. Blank line forces evaluation.");
    loop {
        print_driver_events(session.driver.drain_events());
        let prompt = if buffer.is_empty() {
            "mica> "
        } else {
            "....> "
        };
        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if buffer.is_empty() && matches!(trimmed, ":quit" | ":q") {
                    return Ok(());
                }
                if buffer.is_empty() && matches!(trimmed, ":help" | ":h") {
                    print_repl_help();
                    continue;
                }
                if buffer.is_empty() && matches!(trimmed, ":poll" | ":p") {
                    print_driver_events(session.driver.drain_events());
                    continue;
                }
                if trimmed.is_empty() {
                    if !buffer.trim().is_empty() {
                        evaluate_buffer(session, cli, &mut buffer);
                    }
                    continue;
                }

                let _ = editor.add_history_entry(line.as_str());
                buffer.push_str(&line);
                buffer.push('\n');
                if parse(&buffer).errors.is_empty() {
                    evaluate_buffer(session, cli, &mut buffer);
                }
            }
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => return Ok(()),
            Err(error) => return Err(format!("repl error: {error}")),
        }
    }
}

fn evaluate_buffer(session: &CliSession, cli: &Cli, buffer: &mut String) {
    match submit_cli_source(session, cli, buffer.clone()) {
        Ok(report) => {
            let task_id = report.task_id;
            let outcome = report.outcome.clone();
            print_report(report);
            print_driver_events_without_initial_report(
                task_id,
                &outcome,
                session.driver.drain_events(),
            );
            settle_repl_task(&session.driver, task_id, &outcome);
        }
        Err(error) => eprintln!("{error}"),
    }
    buffer.clear();
}

fn print_report(report: mica_runtime::RunReport) {
    println!("{}", report.render());
}

fn format_source_error(error: mica_runtime::SourceTaskError) -> String {
    format!("error: {error:?}")
}

fn format_driver_error(error: DriverError) -> String {
    format!("error: {error}")
}

fn print_report_and_follow(driver: &CompioTaskDriver, report: mica_runtime::RunReport) {
    let task_id = report.task_id;
    let outcome = report.outcome.clone();
    let mut suspended = suspended_kind(&outcome);
    print_report(report);

    while let Some(kind) = suspended {
        let Some(duration) = follow_delay(&kind) else {
            break;
        };
        thread::sleep(duration);
        suspended = None;
        for event in driver.drain_events() {
            match &event {
                DriverEvent::TaskSuspended {
                    task_id: event_task,
                    kind,
                } if *event_task == task_id => {
                    suspended = Some(kind.clone());
                }
                DriverEvent::TaskCompleted {
                    task_id: event_task,
                    ..
                }
                | DriverEvent::TaskAborted {
                    task_id: event_task,
                    ..
                }
                | DriverEvent::TaskFailed {
                    task_id: event_task,
                    ..
                } if *event_task == task_id => {
                    print_driver_event(event);
                    return;
                }
                DriverEvent::TaskSuspended {
                    task_id: event_task,
                    ..
                } if *event_task == task_id => {}
                _ => print_driver_event(event),
            }
        }
    }

    print_driver_events_without_initial_report(task_id, &outcome, driver.drain_events());
}

fn settle_repl_task(driver: &CompioTaskDriver, task_id: u64, outcome: &TaskOutcome) {
    let mut suspended = suspended_kind(outcome);
    for _ in 0..8 {
        let Some(kind) = suspended else {
            return;
        };
        let Some(duration) = repl_settle_delay(&kind) else {
            return;
        };
        thread::sleep(duration);
        suspended = None;
        let events = driver.drain_events();
        for event in events {
            match &event {
                DriverEvent::TaskSuspended {
                    task_id: event_task,
                    kind,
                } if *event_task == task_id => {
                    suspended = Some(kind.clone());
                }
                _ => print_driver_event(event),
            }
        }
    }
}

fn suspended_kind(outcome: &TaskOutcome) -> Option<SuspendKind> {
    match outcome {
        TaskOutcome::Suspended { kind, .. } => Some(kind.clone()),
        TaskOutcome::Complete { .. } | TaskOutcome::Aborted { .. } => None,
    }
}

fn follow_delay(kind: &SuspendKind) -> Option<Duration> {
    match kind {
        SuspendKind::Commit => Some(Duration::from_millis(1)),
        SuspendKind::TimedMillis(millis) => {
            Some(Duration::from_millis(*millis).max(Duration::from_millis(1)))
        }
        SuspendKind::MailboxRecv(request) => request
            .timeout_millis
            .map(|millis| Duration::from_millis(millis).max(Duration::from_millis(1))),
        SuspendKind::Spawn(_) => Some(Duration::from_millis(1)),
        SuspendKind::Never | SuspendKind::WaitingForInput(_) => None,
    }
}

fn repl_settle_delay(kind: &SuspendKind) -> Option<Duration> {
    follow_delay(kind).filter(|duration| *duration <= REPL_SETTLE_LIMIT)
}

fn print_driver_events(events: Vec<DriverEvent>) {
    for event in events {
        print_driver_event(event);
    }
}

fn print_driver_events_without_initial_report(
    task_id: u64,
    outcome: &TaskOutcome,
    events: Vec<DriverEvent>,
) {
    for event in events {
        if event_matches_initial_report(task_id, outcome, &event) {
            continue;
        }
        print_driver_event(event);
    }
}

fn event_matches_initial_report(task_id: u64, outcome: &TaskOutcome, event: &DriverEvent) -> bool {
    match (outcome, event) {
        (
            TaskOutcome::Complete { .. },
            DriverEvent::TaskCompleted {
                task_id: event_task,
                ..
            },
        )
        | (
            TaskOutcome::Aborted { .. },
            DriverEvent::TaskAborted {
                task_id: event_task,
                ..
            },
        )
        | (
            TaskOutcome::Aborted { .. },
            DriverEvent::TaskFailed {
                task_id: event_task,
                ..
            },
        )
        | (
            TaskOutcome::Suspended { .. },
            DriverEvent::TaskSuspended {
                task_id: event_task,
                ..
            },
        ) => *event_task == task_id,
        (_, DriverEvent::Effect(effect)) => effect.task_id == task_id,
        _ => false,
    }
}

fn print_driver_event(event: DriverEvent) {
    println!("event: {event:?}");
}

fn print_repl_help() {
    println!(
        ":quit exits. :poll drains pending events. Blank line forces evaluation of an incomplete buffer."
    );
}
