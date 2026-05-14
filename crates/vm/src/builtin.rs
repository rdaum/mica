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

use crate::{AuthorityContext, CapabilityGrant, Emission, RuntimeError};
use mica_relation_kernel::{RelationKernel, Transaction};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BuiltinContext<'ctx, 'kernel> {
    kernel: &'kernel RelationKernel,
    tx: &'ctx mut Transaction<'kernel>,
    authority: &'ctx mut AuthorityContext,
    pending_effects: &'ctx mut Vec<Emission>,
    task_snapshot: &'ctx [Value],
}

impl<'ctx, 'kernel> BuiltinContext<'ctx, 'kernel> {
    pub(crate) fn new(
        kernel: &'kernel RelationKernel,
        tx: &'ctx mut Transaction<'kernel>,
        authority: &'ctx mut AuthorityContext,
        pending_effects: &'ctx mut Vec<Emission>,
        task_snapshot: &'ctx [Value],
    ) -> Self {
        Self {
            kernel,
            tx,
            authority,
            pending_effects,
            task_snapshot,
        }
    }

    pub fn kernel(&self) -> &'kernel RelationKernel {
        self.kernel
    }

    pub fn tx(&mut self) -> &mut Transaction<'kernel> {
        self.tx
    }

    pub fn authority(&self) -> &AuthorityContext {
        self.authority
    }

    pub fn authority_mut(&mut self) -> &mut AuthorityContext {
        self.authority
    }

    pub fn task_snapshot(&self) -> &[Value] {
        self.task_snapshot
    }

    pub fn mint_capability(&mut self, grant: CapabilityGrant) -> Value {
        self.authority.mint(grant)
    }

    pub fn emit(&mut self, target: Identity, value: Value) -> Result<(), RuntimeError> {
        if !self.authority.can_effect() {
            return Err(RuntimeError::PermissionDenied {
                operation: "effect",
                target: Value::identity(target),
            });
        }
        self.pending_effects.push(Emission::new(target, value));
        Ok(())
    }
}

pub trait Builtin: Send + Sync {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError>;
}

impl<F> Builtin for F
where
    F: for<'ctx, 'kernel> Fn(
            &mut BuiltinContext<'ctx, 'kernel>,
            &[Value],
        ) -> Result<Value, RuntimeError>
        + Send
        + Sync,
{
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        self(context, args)
    }
}

#[derive(Clone, Default)]
pub struct BuiltinRegistry {
    builtins: BTreeMap<Symbol, Arc<dyn Builtin>>,
}

impl BuiltinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtin(mut self, name: impl AsRef<str>, builtin: impl Builtin + 'static) -> Self {
        self.insert(name, builtin);
        self
    }

    pub fn insert(&mut self, name: impl AsRef<str>, builtin: impl Builtin + 'static) {
        self.builtins
            .insert(Symbol::intern(name.as_ref()), Arc::new(builtin));
    }

    pub fn get(&self, name: Symbol) -> Option<Arc<dyn Builtin>> {
        self.builtins.get(&name).cloned()
    }

    pub fn contains(&self, name: Symbol) -> bool {
        self.builtins.contains_key(&name)
    }

    pub fn len(&self) -> usize {
        self.builtins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.builtins.is_empty()
    }
}
