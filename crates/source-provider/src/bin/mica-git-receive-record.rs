use clap::{Parser, Subcommand};
use mica_source_provider::receive::{GitReceiveRecorder, default_git_dir};
use std::fs;
use std::io::{self, BufReader};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(about = "Record Git refs/for/* post-receive commands for Mica")]
struct Args {
    #[arg(long)]
    git_dir: Option<PathBuf>,

    #[arg(long)]
    quiet: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Read post-receive command lines from stdin and append JSON records.
    PostReceive,
    /// Install this executable as a Git post-receive hook for a bare repository.
    InstallPostReceiveHook {
        #[arg(long)]
        binary: Option<PathBuf>,
    },
    /// Print all stored receive records as a JSON array.
    ListUpdates,
}

fn main() {
    let args = Args::parse();
    let git_dir = args.git_dir.unwrap_or_else(default_git_dir);
    let recorder = GitReceiveRecorder::new(git_dir);
    let result = match args.command {
        Command::PostReceive => {
            let stdin = io::stdin();
            let updates = recorder.receive_post_receive_lines(BufReader::new(stdin.lock()));
            updates.and_then(|updates| {
                if !args.quiet {
                    serde_json::to_writer_pretty(io::stdout(), &updates)
                        .map_err(|error| format!("failed to write JSON output: {error}"))?;
                    println!();
                }
                Ok(())
            })
        }
        Command::InstallPostReceiveHook { binary } => {
            let binary = match binary {
                Some(binary) => Ok(binary),
                None => std::env::current_exe()
                    .map_err(|error| format!("failed to find current executable: {error}")),
            };
            binary.and_then(|binary| {
                install_post_receive_hook(recorder.git_dir(), &binary)?;
                println!(
                    "installed post-receive hook: {}",
                    recorder
                        .git_dir()
                        .join("hooks")
                        .join("post-receive")
                        .display()
                );
                Ok(())
            })
        }
        Command::ListUpdates => recorder.read_updates().and_then(|updates| {
            serde_json::to_writer_pretty(io::stdout(), &updates)
                .map_err(|error| format!("failed to write JSON output: {error}"))?;
            println!();
            Ok(())
        }),
    };
    if let Err(error) = result {
        eprintln!("mica-git-receive-record: {error}");
        std::process::exit(1);
    }
}

fn install_post_receive_hook(
    git_dir: &std::path::Path,
    binary: &std::path::Path,
) -> Result<(), String> {
    let hooks_dir = git_dir.join("hooks");
    fs::create_dir_all(&hooks_dir)
        .map_err(|error| format!("failed to create {}: {error}", hooks_dir.display()))?;
    let hook = hooks_dir.join("post-receive");
    let script = format!(
        "#!/bin/sh\nexec \"{}\" --git-dir \"$GIT_DIR\" --quiet post-receive\n",
        binary.display()
    );
    fs::write(&hook, script)
        .map_err(|error| format!("failed to write {}: {error}", hook.display()))?;
    make_executable(&hook)
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .map_err(|error| format!("failed to stat {}: {error}", path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .map_err(|error| format!("failed to chmod {}: {error}", path.display()))
}

#[cfg(not(unix))]
fn make_executable(_path: &std::path::Path) -> Result<(), String> {
    Ok(())
}
