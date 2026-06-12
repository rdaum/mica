use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const ZERO_ID: &str = "0000000000000000000000000000000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveCommandLine {
    pub old_id: String,
    pub new_id: String,
    pub ref_name: String,
}

impl ReceiveCommandLine {
    pub fn parse(line: &str) -> Result<Self, String> {
        let mut parts = line.split_whitespace();
        let old_id = parts
            .next()
            .ok_or_else(|| "receive command is missing old object id".to_string())?;
        let new_id = parts
            .next()
            .ok_or_else(|| "receive command is missing new object id".to_string())?;
        let ref_name = parts
            .next()
            .ok_or_else(|| "receive command is missing ref name".to_string())?;
        if parts.next().is_some() {
            return Err("receive command has unexpected trailing fields".to_string());
        }
        Ok(Self {
            old_id: old_id.to_string(),
            new_id: new_id.to_string(),
            ref_name: ref_name.to_string(),
        })
    }

    pub fn is_delete(&self) -> bool {
        self.new_id == ZERO_ID
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitMagicRef {
    pub target_ref: String,
    pub options: BTreeMap<String, Vec<String>>,
}

impl GitMagicRef {
    pub fn parse(ref_name: &str) -> Option<Self> {
        let rest = ref_name.strip_prefix("refs/for/")?;
        let (target, raw_options) = rest.split_once('%').unwrap_or((rest, ""));
        if target.is_empty() {
            return None;
        }
        let target_ref = if target.starts_with("refs/") {
            target.to_string()
        } else {
            format!("refs/heads/{target}")
        };
        let mut options = BTreeMap::<String, Vec<String>>::new();
        for raw_option in raw_options.split(',').filter(|value| !value.is_empty()) {
            let (key, value) = raw_option.split_once('=').unwrap_or((raw_option, "true"));
            if key.is_empty() {
                continue;
            }
            options
                .entry(key.to_string())
                .or_default()
                .push(value.to_string());
        }
        Some(Self {
            target_ref,
            options,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitReceivedRefUpdate {
    pub update_id: String,
    pub repository_git_dir: String,
    pub target_ref: String,
    pub ref_name: String,
    pub commit_id: String,
    pub parent_ids: Vec<String>,
    pub subject: String,
    pub message: String,
    pub author_name: String,
    pub author_email: String,
    pub author_time: i64,
    pub change_id_footer: Option<String>,
    pub options: BTreeMap<String, Vec<String>>,
    pub received_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRefUpdate {
    update: GitReceivedRefUpdate,
}

#[derive(Debug, Clone)]
pub struct GitReceiveRecorder {
    git_dir: PathBuf,
}

impl GitReceiveRecorder {
    pub fn new(git_dir: impl Into<PathBuf>) -> Self {
        let git_dir = git_dir.into();
        let git_dir = git_dir.canonicalize().unwrap_or(git_dir);
        Self { git_dir }
    }

    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    pub fn receive_post_receive_lines<R: BufRead>(
        &self,
        reader: R,
    ) -> Result<Vec<GitReceivedRefUpdate>, String> {
        let mut updates = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|error| format!("failed to read receive command: {error}"))?;
            if line.trim().is_empty() {
                continue;
            }
            let command = ReceiveCommandLine::parse(&line)?;
            updates.extend(self.receive_command(&command)?);
        }
        Ok(updates)
    }

    pub fn receive_command(
        &self,
        command: &ReceiveCommandLine,
    ) -> Result<Vec<GitReceivedRefUpdate>, String> {
        if command.is_delete() {
            return Ok(Vec::new());
        }
        let Some(magic_ref) = GitMagicRef::parse(&command.ref_name) else {
            return Ok(Vec::new());
        };
        let commits = self.commits_for_magic_ref(command, &magic_ref)?;
        let received_at = unix_time_now()?;
        let mut updates = Vec::new();
        for (index, commit_id) in commits.into_iter().enumerate() {
            let metadata = self.commit_metadata(&commit_id)?;
            let change_id_footer = extract_change_id(&metadata.message);
            let commit_prefix = commit_id.get(..12).unwrap_or(&commit_id);
            updates.push(GitReceivedRefUpdate {
                update_id: format!("{received_at}-{}-{commit_prefix}", index + 1),
                repository_git_dir: self.git_dir.display().to_string(),
                target_ref: magic_ref.target_ref.clone(),
                ref_name: command.ref_name.clone(),
                commit_id,
                parent_ids: metadata.parent_ids,
                subject: metadata.subject,
                message: metadata.message,
                author_name: metadata.author_name,
                author_email: metadata.author_email,
                author_time: metadata.author_time,
                change_id_footer,
                options: magic_ref.options.clone(),
                received_at,
            });
        }
        self.append_updates(&updates)?;
        self.consume_magic_ref(command)?;
        Ok(updates)
    }

    fn consume_magic_ref(&self, command: &ReceiveCommandLine) -> Result<(), String> {
        let status = Command::new("git")
            .arg("--git-dir")
            .arg(&self.git_dir)
            .arg("update-ref")
            .arg("-d")
            .arg(&command.ref_name)
            .arg(&command.new_id)
            .status()
            .map_err(|error| format!("failed to delete magic ref: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "failed to delete consumed magic ref {}",
                command.ref_name
            ))
        }
    }

    fn commits_for_magic_ref(
        &self,
        command: &ReceiveCommandLine,
        magic_ref: &GitMagicRef,
    ) -> Result<Vec<String>, String> {
        let mut args = vec!["rev-list".to_string(), "--reverse".to_string()];
        if self.ref_exists(&magic_ref.target_ref)? {
            args.push(command.new_id.clone());
            args.push(format!("^{}", magic_ref.target_ref));
        } else if command.old_id != ZERO_ID {
            args.push(format!("{}..{}", command.old_id, command.new_id));
        } else {
            args.push(command.new_id.clone());
        }
        let output = self.git(args)?;
        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }

    fn ref_exists(&self, ref_name: &str) -> Result<bool, String> {
        let status = Command::new("git")
            .arg("--git-dir")
            .arg(&self.git_dir)
            .arg("rev-parse")
            .arg("--verify")
            .arg("--quiet")
            .arg(ref_name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("failed to run git rev-parse: {error}"))?;
        Ok(status.success())
    }

    fn commit_metadata(&self, commit_id: &str) -> Result<CommitMetadata, String> {
        let format = "%H%x00%P%x00%an%x00%ae%x00%at%x00%s%x00%B";
        let output = self.git([
            "show".to_string(),
            "-s".to_string(),
            format!("--format={format}"),
            commit_id.to_string(),
        ])?;
        let mut parts = output.splitn(7, '\0');
        let id = parts
            .next()
            .ok_or_else(|| "git show did not return a commit id".to_string())?;
        if id.trim() != commit_id {
            return Err(format!("git show returned unexpected commit id {id}"));
        }
        let parent_ids = parts
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_string)
            .collect();
        let author_name = parts.next().unwrap_or_default().to_string();
        let author_email = parts.next().unwrap_or_default().to_string();
        let author_time = parts
            .next()
            .unwrap_or_default()
            .trim()
            .parse::<i64>()
            .map_err(|error| format!("invalid author timestamp: {error}"))?;
        let subject = parts.next().unwrap_or_default().trim_end().to_string();
        let message = parts.next().unwrap_or_default().trim_end().to_string();
        Ok(CommitMetadata {
            parent_ids,
            subject,
            message,
            author_name,
            author_email,
            author_time,
        })
    }

    pub fn read_updates(&self) -> Result<Vec<GitReceivedRefUpdate>, String> {
        let path = self.update_log_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&path)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
        let reader = BufReader::new(file);
        let mut updates = Vec::new();
        for (index, line) in reader.lines().enumerate() {
            let line = line.map_err(|error| {
                format!(
                    "failed to read {} line {}: {error}",
                    path.display(),
                    index + 1
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let stored = serde_json::from_str::<StoredRefUpdate>(&line).map_err(|error| {
                format!(
                    "failed to parse {} line {} as Git receive JSON: {error}",
                    path.display(),
                    index + 1
                )
            })?;
            updates.push(stored.update);
        }
        Ok(updates)
    }

    fn append_updates(&self, updates: &[GitReceivedRefUpdate]) -> Result<(), String> {
        if updates.is_empty() {
            return Ok(());
        }
        let path = self.update_log_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
        for update in updates {
            serde_json::to_writer(
                &mut file,
                &StoredRefUpdate {
                    update: update.clone(),
                },
            )
            .map_err(|error| format!("failed to write Git receive JSON: {error}"))?;
            file.write_all(b"\n")
                .map_err(|error| format!("failed to write Git receive newline: {error}"))?;
        }
        Ok(())
    }

    fn update_log_path(&self) -> PathBuf {
        self.git_dir.join("mica-receive").join("ref-updates.jsonl")
    }

    fn git<I, S>(&self, args: I) -> Result<String, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(&self.git_dir)
            .args(args.into_iter().map(|arg| arg.as_ref().to_string()))
            .output()
            .map_err(|error| format!("failed to run git: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "git command failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        String::from_utf8(output.stdout)
            .map_err(|error| format!("git output was not valid UTF-8: {error}"))
    }
}

#[derive(Debug)]
struct CommitMetadata {
    parent_ids: Vec<String>,
    subject: String,
    message: String,
    author_name: String,
    author_email: String,
    author_time: i64,
}

pub fn extract_change_id(message: &str) -> Option<String> {
    message
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("Change-Id:").map(str::trim))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn unix_time_now() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?;
    i64::try_from(duration.as_secs()).map_err(|_| "current time exceeds i64".to_string())
}

pub fn default_git_dir() -> PathBuf {
    std::env::var_os("GIT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".git").to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn parses_magic_ref_with_options() {
        let parsed = GitMagicRef::parse("refs/for/main%topic=demo,r=a@example.com,wip")
            .expect("magic ref should parse");
        assert_eq!(parsed.target_ref, "refs/heads/main");
        assert_eq!(parsed.options.get("topic"), Some(&vec!["demo".to_string()]));
        assert_eq!(
            parsed.options.get("r"),
            Some(&vec!["a@example.com".to_string()])
        );
        assert_eq!(parsed.options.get("wip"), Some(&vec!["true".to_string()]));
    }

    #[test]
    fn extracts_last_change_id_footer() {
        let message = "subject\n\nBody\n\nChange-Id: Ideadbeef\nChange-Id: Inew";
        assert_eq!(extract_change_id(message), Some("Inew".to_string()));
    }

    #[test]
    fn parses_receive_command_line() {
        let line = "0000000000000000000000000000000000000000 1111111111111111111111111111111111111111 refs/for/main";
        let parsed = ReceiveCommandLine::parse(line).expect("receive command should parse");
        assert_eq!(parsed.old_id, ZERO_ID);
        assert_eq!(parsed.new_id, "1111111111111111111111111111111111111111");
        assert_eq!(parsed.ref_name, "refs/for/main");
    }

    #[test]
    fn records_refs_for_push_as_received_ref_update() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let remote = tmp.path().join("remote.git");
        let work = tmp.path().join("work");
        git(["init", "--bare", path(&remote)]);
        git(["clone", path(&remote), path(&work)]);
        git_in(&work, ["config", "user.name", "Mica Tester"]);
        git_in(&work, ["config", "user.email", "mica@example.test"]);
        std::fs::write(work.join("README.md"), "base\n").expect("base file should be written");
        git_in(&work, ["add", "README.md"]);
        git_in(&work, ["commit", "-m", "Initial base"]);
        git_in(&work, ["push", "origin", "HEAD:refs/heads/main"]);
        git_in(&work, ["checkout", "-b", "review"]);
        std::fs::write(work.join("README.md"), "base\nchange\n")
            .expect("review file should be written");
        git_in(
            &work,
            [
                "commit",
                "-am",
                "Review change\n\nChange-Id: I1234567890abcdef1234567890abcdef12345678",
            ],
        );
        let new_id = git_output_in(&work, ["rev-parse", "HEAD"]);
        git_in(&work, ["push", "origin", "HEAD:refs/for/main"]);

        let recorder = GitReceiveRecorder::new(&remote);
        let updates = recorder
            .receive_command(&ReceiveCommandLine {
                old_id: ZERO_ID.to_string(),
                new_id: new_id.trim().to_string(),
                ref_name: "refs/for/main".to_string(),
            })
            .expect("receive command should be processed");

        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.target_ref, "refs/heads/main");
        assert_eq!(
            update.change_id_footer.as_deref(),
            Some("I1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(update.ref_name, "refs/for/main");
        assert_eq!(update.subject, "Review change");
        assert_eq!(
            recorder.read_updates().expect("updates should read"),
            updates
        );
        assert!(
            !git_ref_exists(&remote, "refs/for/main"),
            "magic ref should be consumed after recording"
        );

        std::fs::write(work.join("README.md"), "base\nchange again\n")
            .expect("amended review file should be written");
        git_in(&work, ["commit", "-a", "--amend", "--no-edit"]);
        let replacement_id = git_output_in(&work, ["rev-parse", "HEAD"]);
        git_in(&work, ["push", "origin", "HEAD:refs/for/main"]);

        let replacement_updates = recorder
            .receive_command(&ReceiveCommandLine {
                old_id: ZERO_ID.to_string(),
                new_id: replacement_id.trim().to_string(),
                ref_name: "refs/for/main".to_string(),
            })
            .expect("replacement receive command should be processed");
        assert_eq!(replacement_updates.len(), 1);
        assert_ne!(replacement_updates[0].commit_id, update.commit_id);
        assert_eq!(
            replacement_updates[0].change_id_footer.as_deref(),
            Some("I1234567890abcdef1234567890abcdef12345678")
        );
        assert!(
            !git_ref_exists(&remote, "refs/for/main"),
            "replacement magic ref should also be consumed"
        );
    }

    fn git<const N: usize>(args: [&str; N]) {
        run_git(args, None);
    }

    fn git_in<const N: usize>(work_dir: &Path, args: [&str; N]) {
        run_git(args, Some(work_dir));
    }

    fn git_output_in<const N: usize>(work_dir: &Path, args: [&str; N]) -> String {
        let output = Command::new("git")
            .current_dir(work_dir)
            .args(args)
            .output()
            .expect("git should run");
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("git output should be utf-8")
    }

    fn run_git<const N: usize>(args: [&str; N], work_dir: Option<&Path>) {
        let mut command = Command::new("git");
        if let Some(work_dir) = work_dir {
            command.current_dir(work_dir);
        }
        let output = command.args(args).output().expect("git should run");
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn path(path: &Path) -> &str {
        path.to_str().expect("test path should be utf-8")
    }

    fn git_ref_exists(git_dir: &Path, ref_name: &str) -> bool {
        Command::new("git")
            .arg("--git-dir")
            .arg(git_dir)
            .arg("rev-parse")
            .arg("--verify")
            .arg("--quiet")
            .arg(ref_name)
            .status()
            .expect("git should run")
            .success()
    }
}
