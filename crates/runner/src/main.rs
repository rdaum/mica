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
use mica_relation_kernel::FjallDurabilityMode;
use mica_runner::{FileinMode, SourceRunner};
use mica_var::Symbol;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "mica", about = "Run Mica source, fileins, fileouts, and REPLs")]
struct Cli {
    #[arg(long, global = true, value_enum, default_value_t = StorageMode::Memory)]
    storage: StorageMode,
    #[arg(long, global = true)]
    store: Option<PathBuf>,
    #[arg(long, global = true, value_enum, default_value_t = DurabilityMode::Relaxed)]
    durability: DurabilityMode,
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
            let mut runner = open_runner(&cli)?;
            print_report(runner.run_source(&source).map_err(format_source_error)?);
            Ok(())
        }
        Command::Filein {
            unit,
            replace,
            file,
        } => {
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
            let mut runner = open_runner(&cli)?;
            print_report(runner.run_source(&source).map_err(format_source_error)?);
            Ok(())
        }
        Command::Repl => repl(&cli),
    }
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

fn repl(cli: &Cli) -> Result<(), String> {
    let mut editor =
        DefaultEditor::new().map_err(|error| format!("failed to initialize repl: {error}"))?;
    let mut runner = open_runner(cli)?;
    let mut buffer = String::new();

    println!("Mica REPL. Enter :quit to exit. Blank line forces evaluation.");
    loop {
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
                if trimmed.is_empty() {
                    if !buffer.trim().is_empty() {
                        evaluate_buffer(&mut runner, &mut buffer);
                    }
                    continue;
                }

                let _ = editor.add_history_entry(line.as_str());
                buffer.push_str(&line);
                buffer.push('\n');
                if parse(&buffer).errors.is_empty() {
                    evaluate_buffer(&mut runner, &mut buffer);
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

fn evaluate_buffer(runner: &mut SourceRunner, buffer: &mut String) {
    match runner.run_source(buffer) {
        Ok(report) => print_report(report),
        Err(error) => eprintln!("{}", format_source_error(error)),
    }
    buffer.clear();
}

fn print_report(report: mica_runner::RunReport) {
    println!("{}", report.render());
}

fn format_source_error(error: mica_compiler::SourceTaskError) -> String {
    format!("error: {error:?}")
}

fn print_repl_help() {
    println!(":quit exits. Blank line forces evaluation of an incomplete buffer.");
}
