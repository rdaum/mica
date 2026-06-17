use clap::{Parser, Subcommand};
use mica_source_provider::receive::{GitReceiveRecorder, GitReceivedRefUpdate, default_git_dir};
use std::fs::{self, OpenOptions};
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};

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
        /// Base URL for review links printed during push, for example http://localhost:8008/source/review.
        #[arg(long)]
        review_base_url: Option<String>,
        /// Repository name to include in printed review links.
        #[arg(long)]
        repository_name: Option<String>,
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
                log_received_updates(recorder.git_dir(), &updates)?;
                if !args.quiet {
                    serde_json::to_writer_pretty(io::stdout(), &updates)
                        .map_err(|error| format!("failed to write JSON output: {error}"))?;
                    println!();
                }
                Ok(())
            })
        }
        Command::InstallPostReceiveHook {
            binary,
            review_base_url,
            repository_name,
        } => {
            let binary = match binary {
                Some(binary) => Ok(binary),
                None => std::env::current_exe()
                    .map_err(|error| format!("failed to find current executable: {error}")),
            };
            binary.and_then(|binary| {
                install_post_receive_hook(
                    recorder.git_dir(),
                    &binary,
                    review_base_url.as_deref(),
                    repository_name.as_deref(),
                )?;
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

fn log_received_updates(git_dir: &Path, updates: &[GitReceivedRefUpdate]) -> Result<(), String> {
    if updates.is_empty() {
        return Ok(());
    }
    let log_path = git_dir.join("mica-receive").join("receive.log");
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| format!("failed to open {}: {error}", log_path.display()))?;
    let review_base_url = std::env::var("CONATUS_REVIEW_BASE_URL").ok();
    let repository_name = std::env::var("CONATUS_REVIEW_REPOSITORY").ok();
    for update in updates {
        let change_id = update.change_id_footer.as_deref().unwrap_or("<none>");
        let review_url = review_base_url
            .as_deref()
            .and_then(|base| review_url(base, repository_name.as_deref(), update));
        let line = format!(
            "received update={} target={} commit={} change_id={} subject={}\n",
            update.update_id, update.target_ref, update.commit_id, change_id, update.subject
        );
        eprint!("remote: conatus {line}");
        if let Some(review_url) = &review_url {
            eprintln!("remote: conatus review_url={review_url}");
        }
        log.write_all(line.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", log_path.display()))?;
        if let Some(review_url) = review_url {
            let review_line = format!("review_url={review_url}\n");
            log.write_all(review_line.as_bytes())
                .map_err(|error| format!("failed to write {}: {error}", log_path.display()))?;
        }
    }
    Ok(())
}

fn install_post_receive_hook(
    git_dir: &Path,
    binary: &Path,
    review_base_url: Option<&str>,
    repository_name: Option<&str>,
) -> Result<(), String> {
    let hooks_dir = git_dir.join("hooks");
    fs::create_dir_all(&hooks_dir)
        .map_err(|error| format!("failed to create {}: {error}", hooks_dir.display()))?;
    let hook = hooks_dir.join("post-receive");
    let mut script = "#!/bin/sh\n".to_owned();
    if let Some(review_base_url) = review_base_url {
        script.push_str(&format!(
            "CONATUS_REVIEW_BASE_URL='{}'\nexport CONATUS_REVIEW_BASE_URL\n",
            shell_single_quote(review_base_url)
        ));
    }
    if let Some(repository_name) = repository_name {
        script.push_str(&format!(
            "CONATUS_REVIEW_REPOSITORY='{}'\nexport CONATUS_REVIEW_REPOSITORY\n",
            shell_single_quote(repository_name)
        ));
    }
    script.push_str(&format!(
        "exec \"{}\" --git-dir \"$GIT_DIR\" --quiet post-receive\n",
        binary.display()
    ));
    fs::write(&hook, script)
        .map_err(|error| format!("failed to write {}: {error}", hook.display()))?;
    make_executable(&hook)
}

fn review_url(
    base_url: &str,
    repository_name: Option<&str>,
    update: &GitReceivedRefUpdate,
) -> Option<String> {
    let repository_name = repository_name?;
    if repository_name.is_empty() {
        return None;
    }
    let change_key = update
        .change_id_footer
        .as_deref()
        .filter(|change_id| !change_id.is_empty())
        .unwrap_or(&update.commit_id);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    Some(format!(
        "{}{}repo={}&target={}&change={}",
        base_url,
        separator,
        url_encode_component(repository_name),
        url_encode_component(&update.target_ref),
        url_encode_component(change_key)
    ))
}

fn url_encode_component(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(hex_digit(byte >> 4));
            out.push(hex_digit(byte & 0x0f));
        }
    }
    out
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + (nibble - 10)) as char,
        _ => unreachable!("hex nibble is always below 16"),
    }
}

fn shell_single_quote(input: &str) -> String {
    input.replace('\'', "'\"'\"'")
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn update() -> GitReceivedRefUpdate {
        GitReceivedRefUpdate {
            update_id: "1781704493-1-b4fac2877e0d".to_owned(),
            repository_git_dir: "/tmp/remote.git".to_owned(),
            target_ref: "refs/heads/main".to_owned(),
            ref_name: "refs/for/main".to_owned(),
            commit_id: "b4fac2877e0d6c0ede672e6f00cdd20d8b85a7d7".to_owned(),
            parent_ids: vec!["44fdcbca921e67d76b218a8c969db5f878fbaf7b".to_owned()],
            subject: "Doing a little renovations".to_owned(),
            message: "Doing a little renovations\n\nChange-Id: Iabc123\n".to_owned(),
            author_name: "Conatus Reviewer".to_owned(),
            author_email: "reviewer@example.test".to_owned(),
            author_time: 1_781_704_493,
            change_id_footer: Some("Iabc123".to_owned()),
            options: BTreeMap::new(),
            received_at: 1_781_704_493,
        }
    }

    #[test]
    fn review_url_encodes_query_components() {
        assert_eq!(
            review_url("http://localhost:8008/source/review", Some("review"), &update()),
            Some(
                "http://localhost:8008/source/review?repo=review&target=refs%2Fheads%2Fmain&change=Iabc123"
                    .to_owned()
            )
        );
    }

    #[test]
    fn installed_hook_bakes_review_url_environment() {
        let dir = std::env::temp_dir().join(format!(
            "mica-receive-hook-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let git_dir = dir.join("remote.git");
        fs::create_dir_all(&git_dir).unwrap();

        install_post_receive_hook(
            &git_dir,
            Path::new("/bin/mica-git-receive-record"),
            Some("http://localhost:8008/source/review"),
            Some("review"),
        )
        .unwrap();

        let hook = fs::read_to_string(git_dir.join("hooks").join("post-receive")).unwrap();
        assert!(hook.contains("CONATUS_REVIEW_BASE_URL='http://localhost:8008/source/review'"));
        assert!(hook.contains("CONATUS_REVIEW_REPOSITORY='review'"));
        assert!(hook.contains(
            "exec \"/bin/mica-git-receive-record\" --git-dir \"$GIT_DIR\" --quiet post-receive"
        ));

        fs::remove_dir_all(dir).unwrap();
    }
}
