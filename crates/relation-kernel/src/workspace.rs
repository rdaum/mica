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

use crate::{KernelError, RelationId, RelationRead, Tuple};

pub trait RelationWorkspace: RelationRead {
    fn assert_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError>;

    fn retract_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), KernelError>;

    fn replace_functional_tuple(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), KernelError>;

    fn retract_matching(
        &mut self,
        relation: RelationId,
        bindings: &[Option<mica_var::Value>],
    ) -> Result<(), KernelError> {
        let tuples = self.scan_relation(relation, bindings)?;
        for tuple in tuples {
            self.retract_tuple(relation, tuple)?;
        }
        Ok(())
    }
}
