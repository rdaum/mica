use crate::RuntimeError;
use mica_relation_kernel::{RelationKernel, Transaction};
use mica_var::{Symbol, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BuiltinContext<'ctx, 'kernel> {
    kernel: &'kernel RelationKernel,
    tx: &'ctx mut Transaction<'kernel>,
    pending_effects: &'ctx mut Vec<Value>,
}

impl<'ctx, 'kernel> BuiltinContext<'ctx, 'kernel> {
    pub(crate) fn new(
        kernel: &'kernel RelationKernel,
        tx: &'ctx mut Transaction<'kernel>,
        pending_effects: &'ctx mut Vec<Value>,
    ) -> Self {
        Self {
            kernel,
            tx,
            pending_effects,
        }
    }

    pub fn kernel(&self) -> &'kernel RelationKernel {
        self.kernel
    }

    pub fn tx(&mut self) -> &mut Transaction<'kernel> {
        self.tx
    }

    pub fn emit(&mut self, value: Value) {
        self.pending_effects.push(value);
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
