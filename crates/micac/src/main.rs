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

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use mica_compiler::{CompileError, DiagnosticRenderOptions, compile_error_diagnostics};
use mica_relation_kernel::FjallDurabilityMode;
use mica_runtime::{EmbeddingProviderKind, SourceRunner, SourceTaskError};
use serde::Serialize;

#[derive(Parser)]
#[command(name = "micac", about = "Compile Mica fileins into a fresh database")]
struct Cli {
    #[arg(long = "filein", value_name = "FILE", required = true)]
    fileins: Vec<PathBuf>,
    #[arg(
        long,
        value_name = "DIR",
        required_unless_present = "check",
        conflicts_with = "check"
    )]
    store: Option<PathBuf>,
    #[arg(long)]
    check: bool,
    #[arg(long)]
    force: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
    #[arg(long, value_enum, default_value_t = DurabilityMode::Relaxed)]
    durability: DurabilityMode,
    #[arg(long, value_enum, default_value_t = EmbeddingProviderMode::Deterministic)]
    embedding_provider: EmbeddingProviderMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum DurabilityMode {
    Relaxed,
    Strict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum EmbeddingProviderMode {
    Deterministic,
    Disabled,
    Vllm,
}

impl From<DurabilityMode> for FjallDurabilityMode {
    fn from(value: DurabilityMode) -> Self {
        match value {
            DurabilityMode::Relaxed => Self::Relaxed,
            DurabilityMode::Strict => Self::Strict,
        }
    }
}

impl From<EmbeddingProviderMode> for EmbeddingProviderKind {
    fn from(value: EmbeddingProviderMode) -> Self {
        match value {
            EmbeddingProviderMode::Deterministic => Self::Deterministic,
            EmbeddingProviderMode::Disabled => Self::Disabled,
            EmbeddingProviderMode::Vllm => Self::Vllm,
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(summary) => {
            write_success(&cli, &summary);
            ExitCode::SUCCESS
        }
        Err(error) => {
            write_error(&cli, &error);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Result<CompileSummary, CompileFailure> {
    let mut runner = open_runner(cli)?;
    let mut loaded = Vec::new();

    for filein in &cli.fileins {
        let source = fs::read_to_string(filein)
            .map_err(|error| CompileFailure::read(filein, error.to_string()))?;
        let include_base = filein.parent().unwrap_or_else(|| Path::new("."));
        runner
            .run_filein_with_include_loader(&source, |path| read_filein_include(include_base, path))
            .map_err(|error| CompileFailure::source(&runner, filein, &source, error))?;
        loaded.push(filein.display().to_string());
    }

    Ok(CompileSummary {
        mode: if cli.check {
            CompileMode::Check
        } else {
            CompileMode::Compile
        },
        store: cli.store.as_ref().map(|path| path.display().to_string()),
        fileins: loaded,
    })
}

fn open_runner(cli: &Cli) -> Result<SourceRunner, CompileFailure> {
    if cli.check {
        return Ok(SourceRunner::new_empty_with_embedding_provider(
            cli.embedding_provider.into(),
        ));
    }

    let store = cli
        .store
        .as_ref()
        .expect("clap requires --store unless --check is present");
    prepare_fresh_store(store, cli.force)?;
    SourceRunner::open_fjall_with_embedding_provider(
        store,
        cli.durability.into(),
        cli.embedding_provider.into(),
    )
    .map_err(CompileFailure::configuration)
}

fn prepare_fresh_store(store: &Path, force: bool) -> Result<(), CompileFailure> {
    if !store.exists() {
        return Ok(());
    }
    if !force {
        return Err(CompileFailure::configuration(format!(
            "store {} already exists; remove it or pass --force",
            store.display()
        )));
    }
    fs::remove_dir_all(store).map_err(|error| {
        CompileFailure::configuration(format!("failed to remove {}: {error}", store.display()))
    })
}

fn read_filein_include(base: &Path, path: &str) -> Result<String, String> {
    let include_path = base.join(path);
    fs::read_to_string(&include_path)
        .map_err(|error| format!("failed to read {}: {error}", include_path.display()))
}

fn write_success(cli: &Cli, summary: &CompileSummary) {
    match cli.format {
        OutputFormat::Human => match summary.mode {
            CompileMode::Check => {
                println!("checked {} filein(s)", summary.fileins.len());
            }
            CompileMode::Compile => {
                println!(
                    "compiled {} filein(s) to {}",
                    summary.fileins.len(),
                    summary.store.as_deref().unwrap_or("<memory>")
                );
            }
        },
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&SuccessOutput::from(summary))
                    .expect("success output is serializable")
            );
        }
    }
}

fn write_error(cli: &Cli, error: &CompileFailure) {
    match cli.format {
        OutputFormat::Human => eprintln!("{}", error.rendered),
        OutputFormat::Json => {
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&ErrorOutput::from(error))
                    .expect("error output is serializable")
            );
        }
    }
}

#[derive(Clone, Debug)]
struct CompileSummary {
    mode: CompileMode,
    store: Option<String>,
    fileins: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum CompileMode {
    Check,
    Compile,
}

impl CompileMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Compile => "compile",
        }
    }
}

#[derive(Clone, Debug)]
struct CompileFailure {
    kind: FailureKind,
    file: Option<String>,
    rendered: String,
    diagnostics: Vec<JsonDiagnostic>,
}

impl CompileFailure {
    fn read(path: &Path, message: String) -> Self {
        Self {
            kind: FailureKind::Read,
            file: Some(path.display().to_string()),
            rendered: format!("failed to read {}: {message}", path.display()),
            diagnostics: Vec::new(),
        }
    }

    fn configuration(message: String) -> Self {
        Self {
            kind: FailureKind::Configuration,
            file: None,
            rendered: message,
            diagnostics: Vec::new(),
        }
    }

    fn source(runner: &SourceRunner, path: &Path, source: &str, error: SourceTaskError) -> Self {
        let file = path.display().to_string();
        let rendered = runner.render_source_task_error_with_source_options(
            &error,
            Some(&file),
            source,
            DiagnosticRenderOptions::source_context(),
        );
        let diagnostics = match &error {
            SourceTaskError::Compile(error) => json_diagnostics(error),
            SourceTaskError::TaskManager(_) => Vec::new(),
        };
        Self {
            kind: FailureKind::Source,
            file: Some(file),
            rendered,
            diagnostics,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FailureKind {
    Configuration,
    Read,
    Source,
}

impl FailureKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Read => "read",
            Self::Source => "source",
        }
    }
}

fn json_diagnostics(error: &CompileError) -> Vec<JsonDiagnostic> {
    compile_error_diagnostics(error)
        .into_iter()
        .map(|diagnostic| JsonDiagnostic {
            title: diagnostic.title,
            message: diagnostic.message,
            span: diagnostic.span.map(|span| JsonSpan {
                start: span.start,
                end: span.end,
            }),
        })
        .collect()
}

#[derive(Serialize)]
struct SuccessOutput<'a> {
    status: &'static str,
    mode: &'static str,
    store: Option<&'a str>,
    fileins: &'a [String],
}

impl<'a> From<&'a CompileSummary> for SuccessOutput<'a> {
    fn from(summary: &'a CompileSummary) -> Self {
        Self {
            status: "ok",
            mode: summary.mode.as_str(),
            store: summary.store.as_deref(),
            fileins: &summary.fileins,
        }
    }
}

#[derive(Serialize)]
struct ErrorOutput<'a> {
    status: &'static str,
    kind: &'static str,
    file: Option<&'a str>,
    message: &'a str,
    diagnostics: &'a [JsonDiagnostic],
}

impl<'a> From<&'a CompileFailure> for ErrorOutput<'a> {
    fn from(error: &'a CompileFailure) -> Self {
        Self {
            status: "error",
            kind: error.kind.as_str(),
            file: error.file.as_deref(),
            message: &error.rendered,
            diagnostics: &error.diagnostics,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct JsonDiagnostic {
    title: String,
    message: String,
    span: Option<JsonSpan>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct JsonSpan {
    start: usize,
    end: usize,
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn check_mode_compiles_without_store() {
        let root = temp_root("check");
        fs::create_dir_all(&root).unwrap();
        let filein = root.join("world.mica");
        let other_filein = root.join("more.mica");
        fs::write(&filein, "make_identity(:alice)\n").unwrap();
        fs::write(&other_filein, "make_identity(:bob)\n").unwrap();

        let cli = test_cli(vec![filein, other_filein], None, true);
        let summary = run(&cli).unwrap();

        assert!(matches!(summary.mode, CompileMode::Check));
        assert_eq!(summary.store, None);
        assert_eq!(summary.fileins.len(), 2);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compile_mode_writes_fresh_store() {
        let root = temp_root("store");
        fs::create_dir_all(&root).unwrap();
        let filein = root.join("world.mica");
        let store = root.join("db");
        fs::write(&filein, "make_identity(:alice)\n").unwrap();

        let cli = test_cli(vec![filein], Some(store.clone()), false);
        let summary = run(&cli).unwrap();

        assert!(matches!(summary.mode, CompileMode::Compile));
        assert_eq!(summary.store, Some(store.display().to_string()));
        assert!(store.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compile_mode_rejects_existing_store_without_force() {
        let root = temp_root("existing");
        fs::create_dir_all(&root).unwrap();
        let filein = root.join("world.mica");
        let store = root.join("db");
        fs::write(&filein, "make_identity(:alice)\n").unwrap();
        fs::create_dir_all(&store).unwrap();

        let cli = test_cli(vec![filein], Some(store), false);
        let error = run(&cli).unwrap_err();

        assert!(matches!(error.kind, FailureKind::Configuration));
        assert!(error.rendered.contains("already exists"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_failure_carries_json_diagnostics() {
        let root = temp_root("diagnostics");
        fs::create_dir_all(&root).unwrap();
        let filein = root.join("bad.mica");
        fs::write(&filein, "verb\n").unwrap();

        let cli = test_cli(vec![filein], None, true);
        let error = run(&cli).unwrap_err();

        assert!(matches!(error.kind, FailureKind::Source));
        assert_eq!(error.diagnostics.len(), 1);
        assert_eq!(error.diagnostics[0].title, "parse error");
        assert!(error.diagnostics[0].span.is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_failure_span_is_shifted_to_file_offset() {
        let root = temp_root("diagnostic-offset");
        fs::create_dir_all(&root).unwrap();
        let filein = root.join("bad.mica");
        let source = "// leading comment\nmake_identity(:player)\nverb test(x @ #missing)\nend\n";
        fs::write(&filein, source).unwrap();

        let cli = test_cli(vec![filein], None, true);
        let error = run(&cli).unwrap_err();

        let span = error.diagnostics[0].span.as_ref().unwrap();
        assert_eq!(span.start, source.find("#missing").unwrap());
        assert_eq!(span.end, span.start + "#missing".len());

        fs::remove_dir_all(root).unwrap();
    }

    fn test_cli(fileins: Vec<PathBuf>, store: Option<PathBuf>, check: bool) -> Cli {
        Cli {
            fileins,
            store,
            check,
            force: false,
            format: OutputFormat::Human,
            durability: DurabilityMode::Relaxed,
            embedding_provider: EmbeddingProviderMode::Deterministic,
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("mica-micac-{name}-{}-{nanos}", std::process::id()))
    }
}
