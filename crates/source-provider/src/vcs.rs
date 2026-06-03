use jj_lib::backend::{Backend, CommitId, TreeId};
use jj_lib::git_backend::GitBackend;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub(crate) struct VcsProvider {
    backend: GitBackend,
    git_dir: PathBuf,
    _store_dir: tempfile::TempDir,
    tree_cache: Mutex<HashMap<CommitId, TreeId>>,
    author_cache: Mutex<HashMap<CommitId, (String, String, i64)>>,
    message_cache: Mutex<HashMap<CommitId, String>>,
    parents_cache: Mutex<HashMap<CommitId, Vec<CommitId>>>,
}

impl fmt::Debug for VcsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VcsProvider")
            .field("git_dir", &self.git_dir)
            .finish_non_exhaustive()
    }
}

impl VcsProvider {
    pub(crate) fn open(git_dir: &Path) -> Result<Self, String> {
        let resolved = resolve_git_dir(git_dir)?;
        let config = jj_lib::config::StackedConfig::with_defaults();
        let settings = jj_lib::settings::UserSettings::from_config(config)
            .map_err(|e| format!("failed to create settings: {e}"))?;
        let store_dir =
            tempfile::TempDir::new().map_err(|e| format!("failed to create temp dir: {e}"))?;
        let backend = GitBackend::init_external(&settings, store_dir.path(), &resolved)
            .map_err(|e| format!("failed to open git repo at {}: {e}", resolved.display()))?;
        Ok(Self {
            backend,
            git_dir: resolved,
            _store_dir: store_dir,
            tree_cache: Mutex::new(HashMap::new()),
            author_cache: Mutex::new(HashMap::new()),
            message_cache: Mutex::new(HashMap::new()),
            parents_cache: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn resolve_commit(&self, commit_hex: &str) -> Result<CommitId, String> {
        CommitId::try_from_hex(commit_hex)
            .ok_or_else(|| format!("invalid commit hex: '{commit_hex}'"))
    }

    pub(crate) fn commit_tree(&self, commit_id: &CommitId) -> Result<TreeId, String> {
        {
            let cache = self.tree_cache.lock().unwrap();
            if let Some(tree_id) = cache.get(commit_id) {
                return Ok(tree_id.clone());
            }
        }
        let commit = pollster::block_on(self.backend.read_commit(commit_id))
            .map_err(|e| format!("failed to read commit: {e}"))?;
        let tree_id = commit
            .root_tree
            .into_resolved()
            .map_err(|_conflict| "commit has conflicted tree".to_string())?;
        self.tree_cache
            .lock()
            .unwrap()
            .insert(commit_id.clone(), tree_id.clone());
        Ok(tree_id)
    }

    pub(crate) fn commit_author(
        &self,
        commit_id: &CommitId,
    ) -> Result<(String, String, i64), String> {
        {
            let cache = self.author_cache.lock().unwrap();
            if let Some(author) = cache.get(commit_id) {
                return Ok(author.clone());
            }
        }
        let commit = pollster::block_on(self.backend.read_commit(commit_id))
            .map_err(|e| format!("failed to read commit: {e}"))?;
        let author = (
            commit.author.name.clone(),
            commit.author.email.clone(),
            commit.author.timestamp.timestamp.0,
        );
        self.author_cache
            .lock()
            .unwrap()
            .insert(commit_id.clone(), author.clone());
        Ok(author)
    }

    pub(crate) fn commit_message(&self, commit_id: &CommitId) -> Result<String, String> {
        {
            let cache = self.message_cache.lock().unwrap();
            if let Some(msg) = cache.get(commit_id) {
                return Ok(msg.clone());
            }
        }
        let commit = pollster::block_on(self.backend.read_commit(commit_id))
            .map_err(|e| format!("failed to read commit: {e}"))?;
        self.message_cache
            .lock()
            .unwrap()
            .insert(commit_id.clone(), commit.description.clone());
        Ok(commit.description.clone())
    }

    pub(crate) fn commit_parents(&self, commit_id: &CommitId) -> Result<Vec<CommitId>, String> {
        {
            let cache = self.parents_cache.lock().unwrap();
            if let Some(parents) = cache.get(commit_id) {
                return Ok(parents.clone());
            }
        }
        let commit = pollster::block_on(self.backend.read_commit(commit_id))
            .map_err(|e| format!("failed to read commit: {e}"))?;
        self.parents_cache
            .lock()
            .unwrap()
            .insert(commit_id.clone(), commit.parents.clone());
        Ok(commit.parents.clone())
    }

    pub(crate) fn commit_exists(&self, commit_id: &CommitId) -> Result<bool, String> {
        match pollster::block_on(self.backend.read_commit(commit_id)) {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") {
                    Ok(false)
                } else {
                    Err(format!("commit exists check failed: {e}"))
                }
            }
        }
    }
}

fn resolve_git_dir(path: &Path) -> Result<PathBuf, String> {
    if path.is_dir() {
        return Ok(path.to_path_buf());
    }
    if !path.is_file() {
        return Err(format!(
            ".git is neither a directory nor a file: {}",
            path.display()
        ));
    }
    let mut contents = String::new();
    fs::File::open(path)
        .map_err(|e| format!("failed to open {}: {e}", path.display()))?
        .read_to_string(&mut contents)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let trimmed = contents.trim();
    if let Some(gitdir) = trimmed.strip_prefix("gitdir: ") {
        let target = PathBuf::from(gitdir);
        if target.is_absolute() {
            Ok(target)
        } else {
            let parent = path
                .parent()
                .ok_or_else(|| format!("no parent directory for {}", path.display()))?;
            Ok(parent.join(target))
        }
    } else {
        Err(format!(
            "unrecognized .git file format in {}",
            path.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::object_id::ObjectId;
    use std::path::PathBuf;

    fn mica_git_dir() -> PathBuf {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir.join("../../.git")
    }

    #[test]
    fn open_mica_repo() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open mica repo");
        assert!(!vcs.backend.name().is_empty());
    }

    #[test]
    fn read_root_commit_metadata() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open mica repo");
        vcs.backend.import_head_commits(&[]).expect("import HEAD");
        let root = vcs.backend.root_commit_id().clone();
        let tree = vcs.commit_tree(&root).expect("tree");
        assert!(!tree.hex().is_empty());

        let parents = vcs.commit_parents(&root).expect("parents");
        assert!(parents.is_empty(), "root commit has no parents");

        let _msg = vcs.commit_message(&root).expect("message");
    }

    #[test]
    fn resolve_hex_commit() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open");
        let root = vcs.backend.root_commit_id().clone();
        let hex = root.hex();
        let resolved = vcs.resolve_commit(&hex).expect("resolve");
        assert_eq!(root, resolved);

        let bad = vcs.resolve_commit("nothex");
        assert!(bad.is_err());
    }

    #[test]
    fn commit_exists_true_for_real() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open");
        let root = vcs.backend.root_commit_id().clone();
        assert!(vcs.commit_exists(&root).expect("commit_exists"));
    }

    #[test]
    fn commit_exists_false_for_bogus() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open");
        let bogus = CommitId::from_hex("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        assert!(!vcs.commit_exists(&bogus).expect("commit_exists"));
    }

    #[test]
    fn resolve_gitfile() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let git_dir = temp.path().join("actual.git");
        fs::create_dir(&git_dir).expect("mkdir");
        let gitfile = temp.path().join("link.git");
        fs::write(&gitfile, format!("gitdir: {}\n", git_dir.display())).expect("write");
        let resolved = resolve_git_dir(&gitfile).expect("resolve");
        assert_eq!(resolved, git_dir);
    }

    #[test]
    fn resolve_gitfile_relative() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let target = temp.path().join("sub").join("actual.git");
        fs::create_dir_all(&target).expect("mkdir");
        let gitfile = temp.path().join("link.git");
        fs::write(&gitfile, "gitdir: sub/actual.git\n").expect("write");
        let resolved = resolve_git_dir(&gitfile).expect("resolve");
        assert_eq!(resolved, target);
    }
}
