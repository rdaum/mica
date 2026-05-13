use mica_compiler::parse;
use mica_runner::SourceRunner;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::env;
use std::fs;
use std::process::ExitCode;

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
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = args.first().map(String::as_str) else {
        return repl();
    };

    match command {
        "run" => {
            args.remove(0);
            let path = args
                .first()
                .ok_or_else(|| "usage: mica run <file.mica>".to_owned())?;
            let source = fs::read_to_string(path)
                .map_err(|error| format!("failed to read {path}: {error}"))?;
            let mut runner = SourceRunner::new_empty();
            print_report(runner.run_source(&source).map_err(format_source_error)?);
            Ok(())
        }
        "eval" => {
            args.remove(0);
            if args.is_empty() {
                return Err("usage: mica eval <source>".to_owned());
            }
            let source = args.join(" ");
            let mut runner = SourceRunner::new_empty();
            print_report(runner.run_source(&source).map_err(format_source_error)?);
            Ok(())
        }
        "repl" => repl(),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => Err(format!("unknown command `{command}`\n\n{}", help_text())),
    }
}

fn repl() -> Result<(), String> {
    let mut editor =
        DefaultEditor::new().map_err(|error| format!("failed to initialize repl: {error}"))?;
    let mut runner = SourceRunner::new_empty();
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

fn print_help() {
    println!("{}", help_text());
}

fn help_text() -> &'static str {
    "usage:\n  mica run <file.mica>\n  mica eval <source>\n  mica repl"
}

fn print_repl_help() {
    println!(":quit exits. Blank line forces evaluation of an incomplete buffer.");
}
