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

use mica_var::{Identity, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Emission {
    target: Identity,
    value: Value,
}

impl Emission {
    pub fn new(target: Identity, value: Value) -> Self {
        Self { target, value }
    }

    pub fn target(&self) -> Identity {
        self.target
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}
