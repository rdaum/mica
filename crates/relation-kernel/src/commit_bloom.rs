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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitBloom;

impl CommitBloom {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Default for CommitBloom {
    fn default() -> Self {
        Self::new()
    }
}
