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
use mica_relation_kernel::{
    RelationId, RelationKernel, RelationMetadata, RelationWorkspace, Transaction, TransientStore,
    Tuple,
};
use mica_var::{Identity, Symbol, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

const SYSTEM_ENDPOINT_ID: u64 = 0x00ef_0000_0000_0000;

pub const SYSTEM_ENDPOINT: Identity = match Identity::new(SYSTEM_ENDPOINT_ID) {
    Some(identity) => identity,
    None => panic!("system endpoint id is outside the identity payload range"),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeContext {
    principal: Option<Identity>,
    actor: Option<Identity>,
    endpoint: Identity,
}

impl RuntimeContext {
    pub fn new(principal: Option<Identity>, actor: Option<Identity>, endpoint: Identity) -> Self {
        Self {
            principal,
            actor,
            endpoint,
        }
    }

    pub fn principal(&self) -> Option<Identity> {
        self.principal
    }

    pub fn actor(&self) -> Option<Identity> {
        self.actor
    }

    pub fn endpoint(&self) -> Identity {
        self.endpoint
    }
}

impl Default for RuntimeContext {
    fn default() -> Self {
        Self::new(None, None, SYSTEM_ENDPOINT)
    }
}

pub struct BuiltinContext<'ctx, 'kernel> {
    kernel: &'kernel RelationKernel,
    tx: &'ctx mut Transaction<'kernel>,
    authority: &'ctx mut AuthorityContext,
    pending_effects: &'ctx mut Vec<Emission>,
    task_snapshot: &'ctx [Value],
    runtime_context: RuntimeContext,
    transient: Option<TransientAccess<'ctx>>,
}

pub(crate) enum TransientAccess<'ctx> {
    Exclusive(&'ctx mut TransientStore),
    Shared(&'ctx RwLock<TransientStore>),
}

impl<'ctx, 'kernel> BuiltinContext<'ctx, 'kernel> {
    pub(crate) fn new(
        kernel: &'kernel RelationKernel,
        tx: &'ctx mut Transaction<'kernel>,
        authority: &'ctx mut AuthorityContext,
        pending_effects: &'ctx mut Vec<Emission>,
        task_snapshot: &'ctx [Value],
        runtime_context: RuntimeContext,
        transient: Option<TransientAccess<'ctx>>,
    ) -> Self {
        Self {
            kernel,
            tx,
            authority,
            pending_effects,
            task_snapshot,
            runtime_context,
            transient,
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

    pub fn runtime_context(&self) -> RuntimeContext {
        self.runtime_context
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

    pub fn assert_transient(
        &mut self,
        scope: Identity,
        metadata: RelationMetadata,
        tuple: Tuple,
    ) -> Result<bool, RuntimeError> {
        let Some(transient) = self.transient.as_mut() else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("assert_transient"),
                message: "transient store is not available".to_owned(),
            });
        };
        match transient {
            TransientAccess::Exclusive(transient) => transient
                .assert(scope, metadata, tuple)
                .map_err(RuntimeError::Kernel),
            TransientAccess::Shared(transient) => transient
                .write()
                .unwrap()
                .assert(scope, metadata, tuple)
                .map_err(RuntimeError::Kernel),
        }
    }

    pub fn retract_transient(
        &mut self,
        scope: Identity,
        relation: mica_relation_kernel::RelationId,
        tuple: &Tuple,
    ) -> Result<bool, RuntimeError> {
        let Some(transient) = self.transient.as_mut() else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("retract_transient"),
                message: "transient store is not available".to_owned(),
            });
        };
        Ok(match transient {
            TransientAccess::Exclusive(transient) => transient.retract(scope, relation, tuple),
            TransientAccess::Shared(transient) => {
                transient.write().unwrap().retract(scope, relation, tuple)
            }
        })
    }

    pub fn scan_transient(
        &mut self,
        scopes: &[Identity],
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, RuntimeError> {
        let Some(transient) = self.transient.as_mut() else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("scan_transient"),
                message: "transient store is not available".to_owned(),
            });
        };
        match transient {
            TransientAccess::Exclusive(transient) => transient
                .scan(scopes, relation, bindings)
                .map_err(RuntimeError::Kernel),
            TransientAccess::Shared(transient) => transient
                .read()
                .unwrap()
                .scan(scopes, relation, bindings)
                .map_err(RuntimeError::Kernel),
        }
    }

    pub fn drop_transient_scope(&mut self, scope: Identity) -> Result<usize, RuntimeError> {
        let Some(transient) = self.transient.as_mut() else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("drop_transient_scope"),
                message: "transient store is not available".to_owned(),
            });
        };
        Ok(match transient {
            TransientAccess::Exclusive(transient) => transient.drop_scope(scope),
            TransientAccess::Shared(transient) => transient.write().unwrap().drop_scope(scope),
        })
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

pub struct ClientBuiltinContext<'ctx> {
    workspace: &'ctx mut dyn RelationWorkspace,
    authority: &'ctx mut AuthorityContext,
    pending_effects: &'ctx mut Vec<Emission>,
    runtime_context: RuntimeContext,
}

impl<'ctx> ClientBuiltinContext<'ctx> {
    pub(crate) fn new(
        workspace: &'ctx mut dyn RelationWorkspace,
        authority: &'ctx mut AuthorityContext,
        pending_effects: &'ctx mut Vec<Emission>,
        runtime_context: RuntimeContext,
    ) -> Self {
        Self {
            workspace,
            authority,
            pending_effects,
            runtime_context,
        }
    }

    pub fn authority(&self) -> &AuthorityContext {
        self.authority
    }

    pub fn authority_mut(&mut self) -> &mut AuthorityContext {
        self.authority
    }

    pub fn runtime_context(&self) -> RuntimeContext {
        self.runtime_context
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

    pub fn scan_relation(
        &self,
        relation: RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, RuntimeError> {
        self.workspace
            .scan_relation(relation, bindings)
            .map_err(RuntimeError::Kernel)
    }

    pub fn assert_tuple(&mut self, relation: RelationId, tuple: Tuple) -> Result<(), RuntimeError> {
        self.workspace
            .assert_tuple(relation, tuple)
            .map_err(RuntimeError::Kernel)
    }

    pub fn retract_tuple(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), RuntimeError> {
        self.workspace
            .retract_tuple(relation, tuple)
            .map_err(RuntimeError::Kernel)
    }

    pub fn replace_functional_tuple(
        &mut self,
        relation: RelationId,
        tuple: Tuple,
    ) -> Result<(), RuntimeError> {
        self.workspace
            .replace_functional_tuple(relation, tuple)
            .map_err(RuntimeError::Kernel)
    }
}

pub trait ClientBuiltin: Send + Sync {
    fn call(
        &self,
        context: &mut ClientBuiltinContext<'_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError>;
}

impl<F> ClientBuiltin for F
where
    F: for<'ctx> Fn(&mut ClientBuiltinContext<'ctx>, &[Value]) -> Result<Value, RuntimeError>
        + Send
        + Sync,
{
    fn call(
        &self,
        context: &mut ClientBuiltinContext<'_>,
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

#[derive(Clone, Default)]
pub struct ClientBuiltinRegistry {
    builtins: BTreeMap<Symbol, Arc<dyn ClientBuiltin>>,
}

impl ClientBuiltinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtin(
        mut self,
        name: impl AsRef<str>,
        builtin: impl ClientBuiltin + 'static,
    ) -> Self {
        self.insert(name, builtin);
        self
    }

    pub fn insert(&mut self, name: impl AsRef<str>, builtin: impl ClientBuiltin + 'static) {
        self.builtins
            .insert(Symbol::intern(name.as_ref()), Arc::new(builtin));
    }

    pub fn get(&self, name: Symbol) -> Option<Arc<dyn ClientBuiltin>> {
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
