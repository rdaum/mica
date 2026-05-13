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

mod fjall;
mod memory;

use crate::Commit;

pub use fjall::{FjallDurabilityMode, FjallFormatStatus, FjallStateProvider, PersistedKernelState};
pub use memory::InMemoryCommitProvider;

pub trait CommitProvider: Send + Sync {
    fn persist_commit(&self, commit: &Commit) -> Result<(), String>;
}
