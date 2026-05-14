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

use mica_compiler::{CompileContext, compile_source};
use mica_relation_kernel::{
    ConflictPolicy, ProjectedStore, RelationMetadata, RelationRead, RelationWorkspace, Tuple,
};
use mica_var::{Identity, Symbol, Value};
use mica_vm::{
    AuthorityContext, BuiltinRegistry, ProgramResolver, RegisterVm, RuntimeContext, VmHostContext,
    VmHostResponse,
};
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "C" fn mica_browser_abi_version() -> u32 {
    1
}

#[unsafe(no_mangle)]
pub extern "C" fn mica_browser_projected_store_smoke() -> i64 {
    projected_store_smoke().unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn mica_browser_compile_vm_smoke() -> i64 {
    compile_vm_smoke().unwrap_or(-1)
}

fn projected_store_smoke() -> Option<i64> {
    let relation = Identity::new(0x100)?;
    let object = Value::identity(Identity::new(0x101)?);
    let mut store = ProjectedStore::new();
    store
        .create_relation(
            RelationMetadata::new(relation, Symbol::intern("Name"), 2)
                .with_index([0])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0],
                }),
        )
        .ok()?;
    store
        .replace_functional_tuple(
            relation,
            Tuple::from([object.clone(), Value::string("lamp")]),
        )
        .ok()?;
    Some(
        store
            .scan_relation(relation, &[Some(object), None])
            .ok()?
            .len() as i64,
    )
}

fn compile_vm_smoke() -> Option<i64> {
    let compiled = compile_source("return 40 + 2", &CompileContext::new()).ok()?;
    let kernel = mica_relation_kernel::RelationKernel::new();
    let mut tx = kernel.begin();
    let mut authority = AuthorityContext::root();
    let resolver = ProgramResolver::new();
    let builtins = BuiltinRegistry::new();
    let mut pending_effects = Vec::new();
    let task_snapshot = [];
    let mut host = VmHostContext::new(
        &mut tx,
        &mut authority,
        &resolver,
        &builtins,
        &mut pending_effects,
        &task_snapshot,
        RuntimeContext::default(),
    );
    let mut vm = RegisterVm::new(Arc::new(compiled.program));
    match vm.run_until_host_response(&mut host, 1_000, 8).ok()? {
        VmHostResponse::Complete(value) => value.as_int(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{compile_vm_smoke, projected_store_smoke};

    #[test]
    fn browser_smokes_retain_compiler_vm_and_projected_store() {
        assert_eq!(projected_store_smoke(), Some(1));
        assert_eq!(compile_vm_smoke(), Some(42));
    }
}
