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

use mica_relation_kernel::RelationId;
use mica_var::{CapabilityId, Value};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CapabilityOp {
    Read,
    Write,
    Invoke,
    Effect,
    Grant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityScope {
    All,
    Relation(RelationId),
    Method(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityGrant {
    ops: CapabilityOps,
    scope: CapabilityScope,
}

impl CapabilityGrant {
    pub fn new(ops: impl IntoIterator<Item = CapabilityOp>, scope: CapabilityScope) -> Self {
        Self {
            ops: CapabilityOps::new(ops),
            scope,
        }
    }

    pub fn all() -> Self {
        Self::new(
            [
                CapabilityOp::Read,
                CapabilityOp::Write,
                CapabilityOp::Invoke,
                CapabilityOp::Effect,
                CapabilityOp::Grant,
            ],
            CapabilityScope::All,
        )
    }

    pub fn relation(op: CapabilityOp, relation: RelationId) -> Self {
        Self::new([op], CapabilityScope::Relation(relation))
    }

    pub fn method(method: Value) -> Self {
        Self::new([CapabilityOp::Invoke], CapabilityScope::Method(method))
    }

    fn allows_relation(&self, op: CapabilityOp, relation: RelationId) -> bool {
        if !self.ops.contains(op) {
            return false;
        }
        match &self.scope {
            CapabilityScope::All => true,
            CapabilityScope::Relation(scope) => *scope == relation,
            CapabilityScope::Method(_) => false,
        }
    }

    fn allows_method(&self, method: &Value) -> bool {
        if !self.ops.contains(CapabilityOp::Invoke) {
            return false;
        }
        match &self.scope {
            CapabilityScope::All => true,
            CapabilityScope::Method(scope) => scope == method,
            CapabilityScope::Relation(_) => false,
        }
    }

    fn allows_effect(&self) -> bool {
        self.ops.contains(CapabilityOp::Effect) && matches!(self.scope, CapabilityScope::All)
    }

    fn allows_grant(&self) -> bool {
        self.ops.contains(CapabilityOp::Grant) && matches!(self.scope, CapabilityScope::All)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityOps(u8);

impl CapabilityOps {
    const READ: u8 = 1 << 0;
    const WRITE: u8 = 1 << 1;
    const INVOKE: u8 = 1 << 2;
    const EFFECT: u8 = 1 << 3;
    const GRANT: u8 = 1 << 4;

    fn new(ops: impl IntoIterator<Item = CapabilityOp>) -> Self {
        let mut bits = 0;
        for op in ops {
            bits |= Self::bit(op);
        }
        Self(bits)
    }

    fn contains(self, op: CapabilityOp) -> bool {
        self.0 & Self::bit(op) != 0
    }

    const fn bit(op: CapabilityOp) -> u8 {
        match op {
            CapabilityOp::Read => Self::READ,
            CapabilityOp::Write => Self::WRITE,
            CapabilityOp::Invoke => Self::INVOKE,
            CapabilityOp::Effect => Self::EFFECT,
            CapabilityOp::Grant => Self::GRANT,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityContext {
    root: bool,
    capabilities: BTreeMap<CapabilityId, CapabilityGrant>,
    next_id: u64,
}

impl Default for AuthorityContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl AuthorityContext {
    pub fn empty() -> Self {
        Self {
            root: false,
            capabilities: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn root() -> Self {
        Self {
            root: true,
            capabilities: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn mint(&mut self, grant: CapabilityGrant) -> Value {
        let id = self.allocate_id();
        self.capabilities.insert(id, grant);
        Value::capability(id)
    }

    pub fn grant_for(&self, capability: Value) -> Option<&CapabilityGrant> {
        self.capabilities.get(&capability.as_capability()?)
    }

    pub fn can_read_relation(&self, relation: RelationId) -> bool {
        if self.root {
            return true;
        }
        self.capabilities
            .values()
            .any(|grant| grant.allows_relation(CapabilityOp::Read, relation))
    }

    pub fn can_write_relation(&self, relation: RelationId) -> bool {
        if self.root {
            return true;
        }
        self.capabilities
            .values()
            .any(|grant| grant.allows_relation(CapabilityOp::Write, relation))
    }

    pub fn can_invoke_method(&self, method: &Value) -> bool {
        if self.root {
            return true;
        }
        self.capabilities
            .values()
            .any(|grant| grant.allows_method(method))
    }

    pub fn can_effect(&self) -> bool {
        if self.root {
            return true;
        }
        self.capabilities
            .values()
            .any(CapabilityGrant::allows_effect)
    }

    pub fn can_grant(&self) -> bool {
        if self.root {
            return true;
        }
        self.capabilities
            .values()
            .any(CapabilityGrant::allows_grant)
    }

    fn allocate_id(&mut self) -> CapabilityId {
        loop {
            let raw = self.next_id;
            self.next_id += 1;
            if let Some(id) = CapabilityId::new(raw)
                && !self.capabilities.contains_key(&id)
            {
                return id;
            }
        }
    }
}
