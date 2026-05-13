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

use crate::{FactId, RelationId, Tuple};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Fact {
    id: FactId,
    relation: RelationId,
    tuple: Tuple,
}

impl Fact {
    pub fn new(id: FactId, relation: RelationId, tuple: Tuple) -> Self {
        Self {
            id,
            relation,
            tuple,
        }
    }

    pub fn id(&self) -> FactId {
        self.id
    }

    pub fn relation(&self) -> RelationId {
        self.relation
    }

    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }

    pub fn into_tuple(self) -> Tuple {
        self.tuple
    }
}
