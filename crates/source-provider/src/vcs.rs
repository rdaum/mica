use gix_imara_diff::{
    Algorithm, Diff, InternedInput, Interner, Token, UnifiedDiffConfig, UnifiedDiffPrinter,
};
use jj_lib::backend::{Backend, CommitId, FileId, Tree, TreeId, TreeValue};
use jj_lib::git_backend::GitBackend;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPathBuf, RepoPathComponentBuf};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::io::AsyncReadExt;

const MAX_COMMIT_WALK: usize = 512;
const MAX_FILE_SIZE: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeKind {
    Added,
    Modified,
    Removed,
}

impl ChangeKind {
    pub(crate) fn symbol_name(&self) -> &'static str {
        match self {
            ChangeKind::Added => "source/vcs_added",
            ChangeKind::Modified => "source/vcs_modified",
            ChangeKind::Removed => "source/vcs_removed",
        }
    }
}

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

    // -- commit identity ---------------------------------------------------

    pub(crate) fn resolve_commit(&self, commit_hex: &str) -> Result<CommitId, String> {
        if commit_hex.is_empty() {
            return Err("commit hex must not be empty".to_string());
        }
        CommitId::try_from_hex(commit_hex)
            .ok_or_else(|| format!("invalid commit hex: '{commit_hex}'"))
    }

    pub(crate) fn commit_exists(&self, commit_id: &CommitId) -> Result<bool, String> {
        let git_id = gix::ObjectId::from_hex(commit_id.hex().as_bytes())
            .map_err(|e| format!("invalid commit id {}: {e}", commit_id.hex()))?;
        match self.backend.git_repo().find_commit(git_id) {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("object not found")
                    || msg.contains("did not find")
                    || msg.contains("could not be found")
                    || msg.contains("is not a commit")
                {
                    Ok(false)
                } else {
                    Err(format!("commit exists check failed: {e}"))
                }
            }
        }
    }

    // -- commit metadata (cached) ------------------------------------------

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

    // -- head resolution ---------------------------------------------------

    fn resolve_head(&self) -> Result<CommitId, String> {
        let git_repo = self.backend.git_repo();
        let mut head_ref = git_repo
            .find_reference("HEAD")
            .map_err(|e| format!("failed to find HEAD: {e}"))?;
        let peeled = head_ref
            .peel_to_id()
            .map_err(|e| format!("failed to peel HEAD: {e}"))?;
        let hex = peeled.to_string();
        self.resolve_commit(&hex)
    }

    pub(crate) fn resolve_ref(&self, ref_name: &str) -> Result<Option<CommitId>, String> {
        if ref_name.is_empty() {
            return Err("ref name must not be empty".to_string());
        }
        let git_repo = self.backend.git_repo();
        let mut reference = match git_repo.find_reference(ref_name) {
            Ok(reference) => reference,
            Err(_) => return Ok(None),
        };
        let peeled = reference
            .peel_to_id()
            .map_err(|e| format!("failed to peel ref {ref_name}: {e}"))?;
        let hex = peeled.to_string();
        self.resolve_commit(&hex).map(Some)
    }

    // -- tree / blob access ------------------------------------------------

    fn read_tree(&self, tree_id: &TreeId) -> Result<Tree, String> {
        pollster::block_on(self.backend.read_tree(&RepoPathBuf::root(), tree_id))
            .map_err(|e| format!("failed to read tree: {e}"))
    }

    fn read_file_bytes(&self, file_id: &FileId) -> Result<Vec<u8>, String> {
        pollster::block_on(async {
            let reader = self
                .backend
                .read_file(&RepoPathBuf::root(), file_id)
                .await
                .map_err(|e| format!("failed to read file: {e}"))?;
            let limit = MAX_FILE_SIZE + 1;
            let mut taken = reader.take(limit);
            let mut buf = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut taken, &mut buf)
                .await
                .map_err(|e| format!("failed to read file content: {e}"))?;
            if buf.len() as u64 >= limit {
                return Err(format!(
                    "file too large (>= {:.1} MB)",
                    MAX_FILE_SIZE as f64 / 1_000_000.0
                ));
            }
            Ok(buf)
        })
    }

    fn read_file_text(&self, file_id: &FileId) -> Result<String, String> {
        let bytes = self.read_file_bytes(file_id)?;
        String::from_utf8(bytes).map_err(|e| format!("file is not valid UTF-8: {e}"))
    }

    fn tree_file_id_at_path(&self, tree_id: &TreeId, path: &str) -> Result<Option<FileId>, String> {
        if path.is_empty() {
            return Err("empty path".to_string());
        }
        let components: Vec<&str> = path.split('/').collect();
        let mut current_tree_id = tree_id.clone();
        for (i, component) in components.iter().enumerate() {
            let tree = self.read_tree(&current_tree_id)?;
            let name = RepoPathComponentBuf::new(component.to_string())
                .map_err(|e| format!("invalid path component '{component}': {e}"))?;
            let value = tree.value(&name);
            if i == components.len() - 1 {
                return match value {
                    Some(TreeValue::File { id, .. }) => Ok(Some(id.clone())),
                    Some(_) => Ok(None),
                    None => Ok(None),
                };
            }
            match value {
                Some(TreeValue::Tree(subtree_id)) => {
                    current_tree_id = subtree_id.clone();
                }
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    fn read_file_at_tree(&self, tree_id: &TreeId, path: &str) -> Result<Option<String>, String> {
        match self.tree_file_id_at_path(tree_id, path)? {
            Some(file_id) => {
                let text = self.read_file_text(&file_id)?;
                Ok(Some(text))
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn read_file_at_commit(
        &self,
        commit_id: &CommitId,
        path: &str,
    ) -> Result<Option<String>, String> {
        let tree_id = self.commit_tree(commit_id)?;
        self.read_file_at_tree(&tree_id, path)
    }

    fn collect_file_ids(
        &self,
        tree_id: &TreeId,
        prefix: &str,
    ) -> Result<HashMap<String, FileId>, String> {
        let tree = self.read_tree(tree_id)?;
        let mut files = HashMap::new();
        for entry in tree.entries() {
            let name_str = entry
                .name()
                .to_fs_name()
                .map_err(|_| {
                    "tree contains a path component that cannot be represented as a source path"
                        .to_string()
                })?
                .to_string();
            let full_path = if prefix.is_empty() {
                name_str
            } else {
                format!("{}/{}", prefix, name_str)
            };
            match entry.value() {
                TreeValue::File { id, .. } => {
                    files.insert(full_path, id.clone());
                }
                TreeValue::Tree(subtree_id) => {
                    let sub = self.collect_file_ids(subtree_id, &full_path)?;
                    files.extend(sub);
                }
                _ => {}
            }
        }
        Ok(files)
    }

    fn diff_trees(
        &self,
        from_tree_id: &TreeId,
        to_tree_id: &TreeId,
    ) -> Result<Vec<(String, ChangeKind)>, String> {
        let from_files = self.collect_file_ids(from_tree_id, "")?;
        let to_files = self.collect_file_ids(to_tree_id, "")?;
        let mut all_names: Vec<&String> = from_files.keys().chain(to_files.keys()).collect();
        all_names.sort();
        all_names.dedup();
        let mut result = Vec::new();
        for name in all_names {
            match (from_files.get(name), to_files.get(name)) {
                (None, Some(_)) => result.push((name.clone(), ChangeKind::Added)),
                (Some(_), None) => result.push((name.clone(), ChangeKind::Removed)),
                (Some(from_id), Some(to_id))
                    if from_id != to_id => {
                        result.push((name.clone(), ChangeKind::Modified));
                    }
                _ => {}
            }
        }
        Ok(result)
    }

    // -- changed files -----------------------------------------------------

    pub(crate) fn changed_files(
        &self,
        from: &CommitId,
        to: &CommitId,
    ) -> Result<Vec<(String, ChangeKind)>, String> {
        let from_tree_id = self.commit_tree(from)?;
        let to_tree_id = self.commit_tree(to)?;
        self.diff_trees(&from_tree_id, &to_tree_id)
    }

    // -- file diff ---------------------------------------------------------

    pub(crate) fn file_diff(
        &self,
        from: &CommitId,
        to: &CommitId,
        path: &str,
    ) -> Result<Option<(ChangeKind, String)>, String> {
        let from_tree_id = self.commit_tree(from)?;
        let to_tree_id = self.commit_tree(to)?;
        let from_text = self.read_file_at_tree(&from_tree_id, path)?;
        let to_text = self.read_file_at_tree(&to_tree_id, path)?;

        match diff_kind(from_text.as_deref(), to_text.as_deref()) {
            None => Ok(None),
            Some(kind) => {
                let diff = unified_diff(
                    from_text.as_deref().unwrap_or(""),
                    to_text.as_deref().unwrap_or(""),
                    path,
                    from,
                    to,
                );
                Ok(Some((kind, diff)))
            }
        }
    }

    // -- commit log --------------------------------------------------------

    pub(crate) fn commit_log(
        &self,
        limit: usize,
    ) -> Result<Vec<(CommitId, Vec<CommitId>, String, String, i64, String)>, String> {
        let effective_limit = limit.min(MAX_COMMIT_WALK);
        let head = self.resolve_head()?;
        let mut results = Vec::new();
        let mut visited: HashSet<CommitId> = HashSet::new();
        let mut queue: Vec<CommitId> = vec![head];

        while let Some(current) = queue.pop() {
            if results.len() >= effective_limit {
                break;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            let parents = self.commit_parents(&current)?;
            let (name, email, ts) = self.commit_author(&current)?;
            let msg = self.commit_message(&current)?;
            results.push((current.clone(), parents.clone(), name, email, ts, msg));
            for p in parents.iter().rev() {
                queue.push(p.clone());
            }
        }

        Ok(results)
    }

    // -- commit search -----------------------------------------------------

    pub(crate) fn commit_search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(CommitId, Vec<CommitId>, String, String, i64, String)>, String> {
        let effective_limit = limit.min(MAX_COMMIT_WALK);
        let query_lower = query.to_lowercase();
        let head = self.resolve_head()?;
        let mut results = Vec::new();
        let mut visited: HashSet<CommitId> = HashSet::new();
        let mut queue: Vec<CommitId> = vec![head];

        while let Some(current) = queue.pop() {
            if results.len() >= effective_limit || visited.len() >= MAX_COMMIT_WALK {
                break;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            let parents = self.commit_parents(&current)?;
            let (name, email, ts) = self.commit_author(&current)?;
            let msg = self.commit_message(&current)?;

            let name_lower = name.to_lowercase();
            let email_lower = email.to_lowercase();
            let msg_lower = msg.to_lowercase();
            if name_lower.contains(&query_lower)
                || email_lower.contains(&query_lower)
                || msg_lower.contains(&query_lower)
            {
                results.push((current.clone(), parents.clone(), name, email, ts, msg));
            }

            for p in parents.iter().rev() {
                queue.push(p.clone());
            }
        }

        Ok(results)
    }

    // -- file history ------------------------------------------------------

    pub(crate) fn file_history(
        &self,
        path: &str,
        limit: usize,
    ) -> Result<Vec<(CommitId, Vec<CommitId>, String, String, i64, String)>, String> {
        let effective_limit = limit.min(MAX_COMMIT_WALK);
        let head = self.resolve_head()?;
        let mut results = Vec::new();
        let mut visited: HashSet<CommitId> = HashSet::new();
        let mut queue: Vec<CommitId> = vec![head];

        while let Some(current) = queue.pop() {
            if results.len() >= effective_limit || visited.len() >= MAX_COMMIT_WALK {
                break;
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            let tree_id = self.commit_tree(&current)?;
            let current_fid = self.tree_file_id_at_path(&tree_id, path)?;

            let parents = self.commit_parents(&current)?;
            let mut file_changed = parents.is_empty();

            for parent in &parents {
                if file_changed {
                    break;
                }
                let parent_tree_id = self.commit_tree(parent)?;
                let parent_fid = self.tree_file_id_at_path(&parent_tree_id, path)?;
                if current_fid != parent_fid {
                    file_changed = true;
                }
            }

            if file_changed {
                let (name, email, ts) = self.commit_author(&current)?;
                let msg = self.commit_message(&current)?;
                results.push((current.clone(), parents.clone(), name, email, ts, msg));
            }

            for p in parents.iter().rev() {
                queue.push(p.clone());
            }
        }

        Ok(results)
    }

    // -- blame -------------------------------------------------------------

    pub(crate) fn blame(
        &self,
        commit_id: &CommitId,
        path: &str,
    ) -> Result<Vec<(u64, CommitId, String, String, i64, String)>, String> {
        let tree_id = self.commit_tree(commit_id)?;
        let file_id = match self.tree_file_id_at_path(&tree_id, path)? {
            Some(fid) => fid,
            None => return Ok(Vec::new()),
        };

        let text = self.read_file_text(&file_id)?;
        if text.is_empty() {
            return Ok(Vec::new());
        }

        let lines: Vec<String> = text.lines().map(|s| s.to_string()).collect();
        let mut result: Vec<(u64, CommitId, String, String, i64, String)> = Vec::new();
        let mut visited: HashSet<CommitId> = HashSet::new();
        let mut queue: Vec<CommitId> = vec![commit_id.clone()];
        let mut remaining: Vec<(usize, usize)> = vec![(0, lines.len())];

        while let Some(current) = queue.pop() {
            if visited.len() >= MAX_COMMIT_WALK || remaining.is_empty() {
                break;
            }
            if !visited.insert(current.clone()) {
                continue;
            }

            let current_tree_id = self.commit_tree(&current)?;
            let current_fid = match self.tree_file_id_at_path(&current_tree_id, path)? {
                Some(fid) => fid,
                None => {
                    let (name, email, ts) = self.commit_author(&current)?;
                    let msg = self.commit_message(&current)?;
                    for (start, end) in remaining.drain(..) {
                        for line in start..end {
                            result.push((
                                (line + 1) as u64,
                                current.clone(),
                                name.clone(),
                                email.clone(),
                                ts,
                                msg.clone(),
                            ));
                        }
                    }
                    break;
                }
            };

            let current_text = self.read_file_text(&current_fid)?;
            let current_lines: Vec<&str> = current_text.lines().collect();
            let parents = self.commit_parents(&current)?;

            if parents.is_empty() {
                let (name, email, ts) = self.commit_author(&current)?;
                let msg = self.commit_message(&current)?;
                for (start, end) in remaining.drain(..) {
                    for line in start..end {
                        result.push((
                            (line + 1) as u64,
                            current.clone(),
                            name.clone(),
                            email.clone(),
                            ts,
                            msg.clone(),
                        ));
                    }
                }
                break;
            }

            let mut new_remaining = Vec::new();
            for (start, end) in remaining.drain(..) {
                let mut resolved = false;
                for parent in &parents {
                    let parent_tree_id = self.commit_tree(parent)?;
                    if let Some(parent_fid) = self.tree_file_id_at_path(&parent_tree_id, path)? {
                        let parent_text = self.read_file_text(&parent_fid)?;
                        let parent_lines: Vec<&str> = parent_text.lines().collect();
                        let mut range_start = start;
                        for line in start..end {
                            let cur = current_lines.get(line).copied().unwrap_or("");
                            let par = parent_lines.get(line).copied().unwrap_or("\0");
                            if cur == par {
                                if range_start < line {
                                    let (name, email, ts) = self.commit_author(&current)?;
                                    let msg = self.commit_message(&current)?;
                                    for l in range_start..line {
                                        result.push((
                                            (l + 1) as u64,
                                            current.clone(),
                                            name.clone(),
                                            email.clone(),
                                            ts,
                                            msg.clone(),
                                        ));
                                    }
                                }
                                range_start = line + 1;
                                resolved = true;
                            }
                        }
                        if range_start < end {
                            new_remaining.push((range_start, end));
                        }
                        break;
                    }
                }
                if !resolved {
                    let (name, email, ts) = self.commit_author(&current)?;
                    let msg = self.commit_message(&current)?;
                    for line in start..end {
                        result.push((
                            (line + 1) as u64,
                            current.clone(),
                            name.clone(),
                            email.clone(),
                            ts,
                            msg.clone(),
                        ));
                    }
                }
            }
            remaining = new_remaining;
            if !remaining.is_empty() {
                for p in parents.iter().rev() {
                    queue.push(p.clone());
                }
            }
        }

        result.sort_by_key(|(line, _, _, _, _, _)| *line);
        Ok(result)
    }
}

// -- unified diff helper ---------------------------------------------------

fn diff_kind(old: Option<&str>, new: Option<&str>) -> Option<ChangeKind> {
    match (old, new) {
        (None, None) => None,
        (None, Some(_)) => Some(ChangeKind::Added),
        (Some(_), None) => Some(ChangeKind::Removed),
        (Some(a), Some(b)) if a == b => None,
        (Some(_), Some(_)) => Some(ChangeKind::Modified),
    }
}

struct SourceLineDiffPrinter<'a>(&'a Interner<&'a str>);

impl UnifiedDiffPrinter for SourceLineDiffPrinter<'_> {
    fn display_header(
        &self,
        mut f: impl fmt::Write,
        start_before: u32,
        start_after: u32,
        len_before: u32,
        len_after: u32,
    ) -> fmt::Result {
        writeln!(
            f,
            "@@ -{},{} +{},{} @@",
            start_before + 1,
            len_before,
            start_after + 1,
            len_after
        )
    }

    fn display_context_token(&self, mut f: impl fmt::Write, token: Token) -> fmt::Result {
        self.display_line(&mut f, ' ', token)
    }

    fn display_hunk(
        &self,
        mut f: impl fmt::Write,
        before: &[Token],
        after: &[Token],
    ) -> fmt::Result {
        for &token in before {
            self.display_line(&mut f, '-', token)?;
        }
        for &token in after {
            self.display_line(&mut f, '+', token)?;
        }
        Ok(())
    }
}

impl SourceLineDiffPrinter<'_> {
    fn display_line(&self, mut f: impl fmt::Write, prefix: char, token: Token) -> fmt::Result {
        let line = self.0[token];
        write!(f, "{prefix}{line}")?;
        if !line.ends_with('\n') {
            writeln!(f)?;
            writeln!(f, "\\ No newline at end of file")?;
        }
        Ok(())
    }
}

fn unified_diff(
    old_text: &str,
    new_text: &str,
    path: &str,
    from: &CommitId,
    to: &CommitId,
) -> String {
    let from_hash = &from.hex()[..8.min(from.hex().len())];
    let to_hash = &to.hex()[..8.min(to.hex().len())];
    let mut out = String::new();
    out.push_str(&format!("--- a/{}\t{}\n", path, from_hash));
    out.push_str(&format!("+++ b/{}\t{}\n", path, to_hash));

    let input = InternedInput::new(old_text, new_text);
    let mut diff = Diff::compute(Algorithm::Histogram, &input);
    diff.postprocess_lines(&input);
    out.push_str(
        &diff
            .unified_diff(
                &SourceLineDiffPrinter(&input.interner),
                UnifiedDiffConfig::default(),
                &input,
            )
            .to_string(),
    );

    out
}

// -- gitfile resolution ----------------------------------------------------

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
        let head = vcs.resolve_head().expect("resolve HEAD");
        assert!(vcs.commit_exists(&head).expect("commit_exists"));
    }

    #[test]
    fn commit_exists_true_for_review_fixture_history() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open mica repo");
        let base = vcs
            .resolve_commit("696dbc78cc394c7882c3199d2bac62b38a2ed2bd")
            .expect("resolve fixture base");
        let patch_set = vcs
            .resolve_commit("fea67143608204247917088611d51f1f828f4cc3")
            .expect("resolve fixture patch set");

        assert!(vcs.commit_exists(&base).expect("base exists"));
        assert!(vcs.commit_exists(&patch_set).expect("patch set exists"));
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

    fn dummy_commit() -> CommitId {
        CommitId::from_hex("0000000000000000000000000000000000000000")
    }

    #[test]
    fn diff_kind_tracks_file_presence() {
        assert_eq!(diff_kind(None, None), None);
        assert_eq!(diff_kind(None, Some("new")), Some(ChangeKind::Added));
        assert_eq!(diff_kind(Some("old"), None), Some(ChangeKind::Removed));
        assert_eq!(diff_kind(Some("same"), Some("same")), None);
        assert_eq!(
            diff_kind(Some("old"), Some("new")),
            Some(ChangeKind::Modified)
        );
    }

    #[test]
    fn diff_add_only() {
        let from = dummy_commit();
        let to = dummy_commit();
        let diff = unified_diff("", "added line\n", "test.txt", &from, &to);
        assert!(diff.contains("@@ -1,0 +1,1 @@"));
        assert!(diff.contains("+added line"));
    }

    #[test]
    fn diff_delete_only() {
        let from = dummy_commit();
        let to = dummy_commit();
        let diff = unified_diff("removed line\n", "", "test.txt", &from, &to);
        assert!(diff.contains("@@ -1,1 +1,0 @@"));
        assert!(diff.contains("-removed line"));
    }

    #[test]
    fn diff_replace() {
        let from = dummy_commit();
        let to = dummy_commit();
        let diff = unified_diff("old line\n", "new line\n", "test.txt", &from, &to);
        assert!(diff.contains("@@ -1,1 +1,1 @@"));
        assert!(diff.contains("-old line"));
        assert!(diff.contains("+new line"));
    }

    #[test]
    fn diff_modified_block_keeps_context_and_pairs_changes() {
        let from = dummy_commit();
        let to = dummy_commit();
        let old = "\
fn main() {
    let value = 1;
    println!(\"{value}\");
}
";
        let new = "\
fn main() {
    let value = 2;
    println!(\"value={value}\");
}
";
        let diff = unified_diff(old, new, "test.rs", &from, &to);
        assert!(diff.contains("@@ -1,4 +1,4 @@"));
        assert!(diff.contains(" fn main() {"));
        assert!(diff.contains("-    let value = 1;"));
        assert!(diff.contains("+    let value = 2;"));
        assert!(diff.contains("-    println!(\"{value}\");"));
        assert!(diff.contains("+    println!(\"value={value}\");"));
        assert!(diff.contains(" }"));
        assert_eq!(
            diff.lines().filter(|line| line.starts_with("@@ ")).count(),
            1,
            "nearby replacements should render as one understandable hunk:\n{diff}"
        );
    }

    #[test]
    fn file_diff_for_review_fixture_is_not_whole_file_replacement() {
        let vcs = VcsProvider::open(&mica_git_dir()).expect("open mica repo");
        let from = vcs
            .resolve_commit("696dbc78cc394c7882c3199d2bac62b38a2ed2bd")
            .expect("resolve fixture base");
        let to = vcs
            .resolve_commit("fea67143608204247917088611d51f1f828f4cc3")
            .expect("resolve fixture patch set");
        let (kind, diff) = vcs
            .file_diff(&from, &to, "crates/relation-kernel/src/snapshot.rs")
            .expect("diff fixture file")
            .expect("fixture file changed");

        assert_eq!(kind, ChangeKind::Modified);
        assert!(diff.contains("-use crate::commit_bloom::CommitBloom;"));
        assert!(diff.contains("-    pub(crate) bloom: CommitBloom,"));
        assert_eq!(
            diff.lines().filter(|line| line.starts_with("@@ ")).count(),
            2,
            "fixture diff should be two small hunks, not a full-file replacement:\n{diff}"
        );
        assert!(
            diff.lines()
                .filter(|line| line.starts_with('+') && !line.starts_with("+++ "))
                .count()
                <= 1,
            "fixture is mostly removals and should not re-add the file body:\n{diff}"
        );
    }

    #[test]
    fn diff_unchanged() {
        let from = dummy_commit();
        let to = dummy_commit();
        let diff = unified_diff("same\n", "same\n", "test.txt", &from, &to);
        assert!(!diff.contains("@@"), "unchanged should have no hunks");
    }

    #[test]
    fn diff_final_newline_change() {
        let from = dummy_commit();
        let to = dummy_commit();
        let diff = unified_diff("same", "same\n", "test.txt", &from, &to);
        assert!(
            diff.contains("@@"),
            "newline-only change should emit a hunk"
        );
        assert!(diff.contains("-same"));
        assert!(diff.contains("+same"));
        assert!(diff.contains("\\ No newline at end of file"));
    }
}
