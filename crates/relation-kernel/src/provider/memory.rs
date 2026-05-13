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

use super::CommitProvider;
use crate::Commit;
use std::sync::Mutex;

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
