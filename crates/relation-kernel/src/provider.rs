use crate::Commit;
use std::sync::Mutex;

pub trait CommitProvider: Send + Sync {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String>;
}

#[derive(Default)]
pub struct InMemoryCommitProvider {
    commits: Mutex<Vec<Commit>>,
}

impl InMemoryCommitProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn commits(&self) -> Vec<Commit> {
        self.commits.lock().unwrap().clone()
    }
}

impl CommitProvider for InMemoryCommitProvider {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String> {
        self.commits.lock().unwrap().push(commit.clone());
        Ok(())
    }
}
