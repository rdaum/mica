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

use crate::builtin::{RuntimePorts, TransientAccess};
use crate::program::{CompactListItem, CompactRelationArg, Opcode, OperandRef};
use crate::{
    AuthorityContext, BuiltinRegistry, CatchHandler, ClientBuiltinContext, ClientBuiltinRegistry,
    Emission, ErrorField, MailboxRecvRequest, Program, ProgramResolver, QueryBinding, Register,
    RuntimeBinaryOp, RuntimeContext, RuntimeError, RuntimeUnaryOp, SpawnRequest, SpawnTarget,
    SuspendKind,
};
use mica_relation_kernel::{
    ApplicableMethodCall, ComposedTransactionRead, DispatchRelations, RelationId, RelationRead,
    RelationWorkspace, ScanControl, Transaction, TransientStore, Tuple,
    applicable_method_calls_normalized, applicable_positional_methods, method_program_id,
    normalize_dispatch_roles,
};
use mica_var::{FunctionId, Identity, Symbol, Value, ValueKind};
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    program: usize,
    ip: usize,
    registers: Vec<Value>,
    return_register: Option<Register>,
    try_stack: Vec<TryRegion>,
    pending_finally: Vec<FinallyContinuation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TryRegion {
    catches: Vec<CatchHandler>,
    finally: Option<usize>,
    end: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FinallyContinuation {
    Normal(usize),
    Raise(Value),
    Return(Value),
}

enum ExpandedRelationArg {
    Value(Value),
    Query(Symbol),
    Hole,
}

impl Frame {
    fn root(program: usize, register_count: usize) -> Self {
        Self::new(program, register_count, None, Vec::new()).expect("root frame has no arguments")
    }

    fn new(
        program: usize,
        register_count: usize,
        return_register: Option<Register>,
        args: Vec<Value>,
    ) -> Result<Self, RuntimeError> {
        if args.len() > register_count {
            return Err(RuntimeError::InvalidCallArity {
                expected_min: 0,
                expected_max: register_count,
                actual: args.len(),
            });
        }

        let mut registers = vec![Value::nothing(); register_count];
        for (slot, arg) in registers.iter_mut().zip(args) {
            *slot = arg;
        }
        Ok(Self {
            program,
            ip: 0,
            registers,
            return_register,
            try_stack: Vec::new(),
            pending_finally: Vec::new(),
        })
    }

    pub fn program_index(&self) -> usize {
        self.program
    }

    pub fn ip(&self) -> usize {
        self.ip
    }

    pub fn registers(&self) -> &[Value] {
        &self.registers
    }

    pub fn return_register(&self) -> Option<Register> {
        self.return_register
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VmState {
    programs: Vec<Arc<Program>>,
    functions: Vec<CallableInfo>,
    resolved_programs: Vec<(Value, usize)>,
    frames: Vec<Frame>,
    pending_resume: Option<Register>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CallableInfo {
    program: usize,
    captures: Vec<Value>,
    min_arity: usize,
    max_arity: usize,
}

impl VmState {
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    pub fn current_frame(&self) -> Option<&Frame> {
        self.frames.last()
    }

    pub fn ip(&self) -> usize {
        self.current_frame().map_or(0, Frame::ip)
    }

    pub fn registers(&self) -> &[Value] {
        self.current_frame().map_or(&[], Frame::registers)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VmHostResponse {
    Continue,
    Commit,
    Suspend(SuspendKind),
    Spawn(SpawnRequest),
    Complete(Value),
    Abort(Value),
    RollbackRetry,
}

pub trait VmHost: RelationWorkspace {
    fn authority(&self) -> &AuthorityContext;

    fn authority_mut(&mut self) -> &mut AuthorityContext;

    fn emit(&mut self, target: Identity, value: Value) -> Result<(), RuntimeError>;

    fn validate_mailbox_receiver(&mut self, receiver: &Value) -> Result<(), RuntimeError>;

    fn resolve_program(
        &mut self,
        program_bytes_relation: mica_relation_kernel::RelationId,
        program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError>;

    fn call_builtin(&mut self, name: Symbol, args: &[Value]) -> Result<Value, RuntimeError>;
}

pub struct VmHostContext<'ctx, 'kernel> {
    tx: &'ctx mut Transaction<'kernel>,
    authority: &'ctx mut AuthorityContext,
    resolver: &'ctx ProgramResolver,
    builtins: &'ctx BuiltinRegistry,
    ports: RuntimePorts<'ctx>,
    task_snapshot: &'ctx [Value],
    runtime_context: RuntimeContext,
    transient: Option<TransientAccess<'ctx>>,
    transient_scopes: &'ctx [Identity],
}

impl<'ctx, 'kernel> VmHostContext<'ctx, 'kernel> {
    pub fn new(
        tx: &'ctx mut Transaction<'kernel>,
        authority: &'ctx mut AuthorityContext,
        resolver: &'ctx ProgramResolver,
        builtins: &'ctx BuiltinRegistry,
        ports: RuntimePorts<'ctx>,
        task_snapshot: &'ctx [Value],
        runtime_context: RuntimeContext,
    ) -> Self {
        Self {
            tx,
            authority,
            resolver,
            builtins,
            ports,
            task_snapshot,
            runtime_context,
            transient: None,
            transient_scopes: &[],
        }
    }

    pub fn with_transient(
        mut self,
        transient: &'ctx mut TransientStore,
        transient_scopes: &'ctx [Identity],
    ) -> Self {
        self.transient = Some(TransientAccess::Exclusive(transient));
        self.transient_scopes = transient_scopes;
        self
    }

    pub fn with_shared_transient(
        mut self,
        transient: &'ctx RwLock<TransientStore>,
        transient_scopes: &'ctx [Identity],
    ) -> Self {
        self.transient = Some(TransientAccess::Shared(transient));
        self.transient_scopes = transient_scopes;
        self
    }
}

impl RelationRead for VmHostContext<'_, '_> {
    fn scan_relation(
        &self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, mica_relation_kernel::KernelError> {
        match &self.transient {
            Some(TransientAccess::Exclusive(transient)) => {
                let reader =
                    ComposedTransactionRead::new(&*self.tx, transient, self.transient_scopes);
                reader.scan_relation(relation, bindings)
            }
            Some(TransientAccess::Shared(transient)) => {
                let transient = transient.read().unwrap();
                let reader =
                    ComposedTransactionRead::new(&*self.tx, &transient, self.transient_scopes);
                reader.scan_relation(relation, bindings)
            }
            None => self.tx.scan_relation(relation, bindings),
        }
    }

    fn visit_relation(
        &self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, mica_relation_kernel::KernelError>,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        match &self.transient {
            Some(TransientAccess::Exclusive(transient)) => {
                let reader =
                    ComposedTransactionRead::new(&*self.tx, transient, self.transient_scopes);
                reader.visit_relation(relation, bindings, visitor)
            }
            Some(TransientAccess::Shared(transient)) => {
                let transient = transient.read().unwrap();
                let reader =
                    ComposedTransactionRead::new(&*self.tx, &transient, self.transient_scopes);
                reader.visit_relation(relation, bindings, visitor)
            }
            None => self.tx.visit_relation(relation, bindings, visitor),
        }
    }

    fn cached_applicable_method_calls(
        &self,
        relations: DispatchRelations,
        selector: &Value,
        roles: &[(Value, Value)],
    ) -> Result<Option<Vec<ApplicableMethodCall>>, mica_relation_kernel::KernelError> {
        match &self.transient {
            Some(TransientAccess::Exclusive(transient)) => {
                let reader =
                    ComposedTransactionRead::new(&*self.tx, transient, self.transient_scopes);
                reader.cached_applicable_method_calls(relations, selector, roles)
            }
            Some(TransientAccess::Shared(transient)) => {
                let transient = transient.read().unwrap();
                let reader =
                    ComposedTransactionRead::new(&*self.tx, &transient, self.transient_scopes);
                reader.cached_applicable_method_calls(relations, selector, roles)
            }
            None => self
                .tx
                .cached_applicable_method_calls(relations, selector, roles),
        }
    }

    fn cached_method_program(
        &self,
        relation: mica_relation_kernel::RelationId,
        method: &Value,
    ) -> Result<Option<Option<Value>>, mica_relation_kernel::KernelError> {
        match &self.transient {
            Some(TransientAccess::Exclusive(transient)) => {
                let reader =
                    ComposedTransactionRead::new(&*self.tx, transient, self.transient_scopes);
                reader.cached_method_program(relation, method)
            }
            Some(TransientAccess::Shared(transient)) => {
                let transient = transient.read().unwrap();
                let reader =
                    ComposedTransactionRead::new(&*self.tx, &transient, self.transient_scopes);
                reader.cached_method_program(relation, method)
            }
            None => self.tx.cached_method_program(relation, method),
        }
    }
}

impl RelationWorkspace for VmHostContext<'_, '_> {
    fn assert_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.tx.assert_tuple(relation, tuple)
    }

    fn retract_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.tx.retract_tuple(relation, tuple)
    }

    fn replace_functional_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.tx.replace_functional_tuple(relation, tuple)
    }

    fn retract_matching(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
    ) -> Result<(), mica_relation_kernel::KernelError> {
        let tuples = self.scan_relation(relation, bindings)?;
        for tuple in tuples {
            self.retract_tuple(relation, tuple)?;
        }
        Ok(())
    }
}

impl VmHost for VmHostContext<'_, '_> {
    fn authority(&self) -> &AuthorityContext {
        self.authority
    }

    fn authority_mut(&mut self) -> &mut AuthorityContext {
        self.authority
    }

    fn emit(&mut self, target: Identity, value: Value) -> Result<(), RuntimeError> {
        if !self.authority.can_effect() {
            return Err(RuntimeError::PermissionDenied {
                operation: "effect",
                target: Value::identity(target),
            });
        }
        self.ports
            .pending_effects
            .push(Emission::new(target, value));
        Ok(())
    }

    fn validate_mailbox_receiver(&mut self, receiver: &Value) -> Result<(), RuntimeError> {
        let Some(mailbox_runtime) = self.ports.mailbox_runtime.as_ref() else {
            return Err(RuntimeError::InvalidBuiltinCall {
                name: Symbol::intern("mailbox_recv"),
                message: "mailbox runtime is not available".to_owned(),
            });
        };
        mailbox_runtime.validate_mailbox_receiver(receiver)
    }

    fn resolve_program(
        &mut self,
        program_bytes_relation: mica_relation_kernel::RelationId,
        program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError> {
        self.resolver
            .resolve(self, program_bytes_relation, program_id)
    }

    fn call_builtin(&mut self, name: Symbol, args: &[Value]) -> Result<Value, RuntimeError> {
        let builtin = self
            .builtins
            .get(name)
            .ok_or(RuntimeError::UnknownBuiltin { name })?;
        let transient = match self.transient.as_mut() {
            Some(TransientAccess::Exclusive(transient)) => {
                Some(TransientAccess::Exclusive(transient))
            }
            Some(TransientAccess::Shared(transient)) => Some(TransientAccess::Shared(transient)),
            None => None,
        };
        let mut context = crate::BuiltinContext::new(
            self.tx.kernel(),
            self.tx,
            self.authority,
            RuntimePorts {
                pending_effects: self.ports.pending_effects,
                pending_mailbox_sends: self.ports.pending_mailbox_sends,
                mailbox_runtime: self.ports.mailbox_runtime,
            },
            self.task_snapshot,
            self.runtime_context,
            transient,
        );
        builtin.call(&mut context, args)
    }
}

pub struct ProjectedVmHostContext<'ctx, W> {
    workspace: &'ctx mut W,
    authority: &'ctx mut AuthorityContext,
    resolver: &'ctx ProgramResolver,
    builtins: Option<&'ctx ClientBuiltinRegistry>,
    pending_effects: &'ctx mut Vec<Emission>,
    runtime_context: RuntimeContext,
}

impl<'ctx, W> ProjectedVmHostContext<'ctx, W> {
    pub fn new(
        workspace: &'ctx mut W,
        authority: &'ctx mut AuthorityContext,
        resolver: &'ctx ProgramResolver,
        pending_effects: &'ctx mut Vec<Emission>,
    ) -> Self {
        Self {
            workspace,
            authority,
            resolver,
            builtins: None,
            pending_effects,
            runtime_context: RuntimeContext::default(),
        }
    }

    pub fn with_builtins(mut self, builtins: &'ctx ClientBuiltinRegistry) -> Self {
        self.builtins = Some(builtins);
        self
    }

    pub fn with_runtime_context(mut self, runtime_context: RuntimeContext) -> Self {
        self.runtime_context = runtime_context;
        self
    }
}

impl<W: RelationWorkspace> RelationRead for ProjectedVmHostContext<'_, W> {
    fn scan_relation(
        &self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
    ) -> Result<Vec<Tuple>, mica_relation_kernel::KernelError> {
        self.workspace.scan_relation(relation, bindings)
    }

    fn visit_relation(
        &self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
        visitor: &mut dyn FnMut(&Tuple) -> Result<ScanControl, mica_relation_kernel::KernelError>,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.workspace.visit_relation(relation, bindings, visitor)
    }
}

impl<W: RelationWorkspace> RelationWorkspace for ProjectedVmHostContext<'_, W> {
    fn assert_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.workspace.assert_tuple(relation, tuple)
    }

    fn retract_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.workspace.retract_tuple(relation, tuple)
    }

    fn replace_functional_tuple(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        tuple: Tuple,
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.workspace.replace_functional_tuple(relation, tuple)
    }

    fn retract_matching(
        &mut self,
        relation: mica_relation_kernel::RelationId,
        bindings: &[Option<Value>],
    ) -> Result<(), mica_relation_kernel::KernelError> {
        self.workspace.retract_matching(relation, bindings)
    }
}

impl<W: RelationWorkspace> VmHost for ProjectedVmHostContext<'_, W> {
    fn authority(&self) -> &AuthorityContext {
        self.authority
    }

    fn authority_mut(&mut self) -> &mut AuthorityContext {
        self.authority
    }

    fn emit(&mut self, target: Identity, value: Value) -> Result<(), RuntimeError> {
        if !self.authority.can_effect() {
            return Err(RuntimeError::PermissionDenied {
                operation: "effect",
                target: Value::identity(target),
            });
        }
        self.pending_effects.push(Emission::new(target, value));
        Ok(())
    }

    fn validate_mailbox_receiver(&mut self, receiver: &Value) -> Result<(), RuntimeError> {
        Err(RuntimeError::InvalidMailboxCapability {
            operation: "recv",
            capability: receiver.clone(),
        })
    }

    fn resolve_program(
        &mut self,
        program_bytes_relation: mica_relation_kernel::RelationId,
        program_id: &Value,
    ) -> Result<Arc<Program>, RuntimeError> {
        self.resolver
            .resolve(self, program_bytes_relation, program_id)
    }

    fn call_builtin(&mut self, name: Symbol, args: &[Value]) -> Result<Value, RuntimeError> {
        let builtin = self
            .builtins
            .and_then(|builtins| builtins.get(name))
            .ok_or(RuntimeError::UnknownBuiltin { name })?;
        let workspace: &mut dyn RelationWorkspace = self.workspace;
        let mut context = ClientBuiltinContext::new(
            workspace,
            self.authority,
            self.pending_effects,
            self.runtime_context,
        );
        builtin.call(&mut context, args)
    }
}

#[derive(Clone, Debug)]
pub struct RegisterVm {
    state: VmState,
}

impl RegisterVm {
    pub fn new(program: Arc<Program>) -> Self {
        let register_count = program.register_count();
        Self {
            state: VmState {
                programs: vec![program],
                functions: Vec::new(),
                resolved_programs: Vec::new(),
                frames: vec![Frame::root(0, register_count)],
                pending_resume: None,
            },
        }
    }

    pub fn from_state(state: VmState) -> Self {
        Self { state }
    }

    pub fn snapshot_state(&self) -> VmState {
        self.state.clone()
    }

    pub fn restore_state(&mut self, state: &VmState) {
        self.state = state.clone();
    }

    pub fn resume_with(&mut self, value: Value) -> Result<(), RuntimeError> {
        let Some(register) = self.state.pending_resume else {
            return Ok(());
        };
        self.write_register(register, value)?;
        self.state.pending_resume = None;
        Ok(())
    }

    pub fn frame_count(&self) -> usize {
        self.state.frames.len()
    }

    pub fn register(&self, register: Register) -> Option<&Value> {
        self.current_frame()
            .ok()
            .and_then(|frame| frame.registers.get(register.0 as usize))
    }

    pub fn set_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        self.write_register(register, value)
    }

    pub fn run_until_host_response<H: VmHost>(
        &mut self,
        host: &mut H,
        instruction_budget: usize,
        max_call_depth: usize,
    ) -> Result<VmHostResponse, RuntimeError> {
        for _ in 0..instruction_budget {
            let response = self.step(host, max_call_depth)?;
            if response != VmHostResponse::Continue {
                return Ok(response);
            }
        }
        Err(RuntimeError::InstructionBudgetExceeded {
            budget: instruction_budget,
        })
    }

    fn step<H: VmHost>(
        &mut self,
        host: &mut H,
        max_call_depth: usize,
    ) -> Result<VmHostResponse, RuntimeError> {
        let (opcode, program, ip) = {
            let frame = self.current_frame_unchecked();
            let ip = frame.ip;
            let frame_program = &self.state.programs[frame.program];
            let program = Arc::as_ptr(frame_program);
            let opcode = frame_program
                .opcodes()
                .get(ip)
                .ok_or(RuntimeError::ProgramCounterOutOfBounds { ip })?;
            (opcode as *const Opcode, program, ip)
        };
        debug_assert_eq!(self.current_frame_unchecked().ip, ip);
        // SAFETY: both pointers come from an `Arc<Program>` owned by the VM state's program table.
        // The program allocation is stable while that table entry is live, and each arm copies or
        // resolves any table values it needs before operations that can remove the current frame.
        let program = unsafe { &*program };
        let opcode = unsafe { &*opcode };

        match opcode {
            Opcode::Load { dst, value } => {
                self.write_register_unchecked(*dst, program.constant(*value).clone());
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Move { dst, src } => {
                let value = self.read_register_unchecked(*src).clone();
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Unary { dst, op, src } => {
                let value = self.read_register_unchecked(*src);
                let value = match eval_unary(*op, value) {
                    Ok(value) => value,
                    Err(error) => return self.begin_raise(error),
                };
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Binary {
                dst,
                op,
                left,
                right,
            } => {
                let value = match eval_binary(
                    *op,
                    self.read_register_unchecked(*left),
                    self.read_register_unchecked(*right),
                ) {
                    Ok(value) => value,
                    Err(error) => return self.begin_raise(error),
                };
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::BuildList { dst, items } => {
                let value = self.build_list(program, program.list_items(*items));
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::BuildMap { dst, entries } => {
                let entries = program
                    .map_entries(*entries)
                    .iter()
                    .map(|(key, value)| {
                        (
                            self.resolve_operand_ref(program, *key),
                            self.resolve_operand_ref(program, *value),
                        )
                    })
                    .collect::<Vec<_>>();
                self.write_register_unchecked(*dst, Value::map(entries));
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::BuildRange { dst, start, end } => {
                let start = self.resolve_operand_ref(program, *start);
                let end = end.map(|end| self.resolve_operand_ref(program, end));
                self.write_register_unchecked(*dst, Value::range(start, end));
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Index {
                dst,
                collection,
                index,
            } => {
                let value = index_value(
                    self.read_register_unchecked(*collection),
                    &self.resolve_operand_ref(program, *index),
                );
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::SetIndex {
                dst,
                collection,
                index,
                value,
            } => {
                let value = set_index_value(
                    self.read_register_unchecked(*collection),
                    &self.resolve_operand_ref(program, *index),
                    self.resolve_operand_ref(program, *value),
                );
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ErrorField { dst, error, field } => {
                let value = error_field_value(self.read_register_unchecked(*error), *field);
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::One { dst, src } => {
                let value = match one_value(self.read_register_unchecked(*src)) {
                    Ok(value) => value,
                    Err(error) => return self.begin_raise(error),
                };
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::CollectionLen { dst, collection } => {
                let value = collection_len(self.read_register_unchecked(*collection));
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::CollectionKeyAt {
                dst,
                collection,
                index,
            } => {
                let value = collection_key_at(
                    self.read_register_unchecked(*collection),
                    self.read_register_unchecked(*index),
                );
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::CollectionValueAt {
                dst,
                collection,
                index,
            } => {
                let value = collection_value_at(
                    self.read_register_unchecked(*collection),
                    self.read_register_unchecked(*index),
                );
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ScanExists {
                dst,
                relation,
                bindings,
            } => {
                let relation = program.relation(*relation);
                require_read(host.authority(), relation)?;
                let bindings = self.resolve_bindings(program, program.bindings(*bindings));
                let exists = !host
                    .scan_relation(relation, &bindings)
                    .map_err(RuntimeError::Kernel)?
                    .is_empty();
                self.write_register_unchecked(*dst, Value::bool(exists));
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ScanBindings {
                dst,
                relation,
                bindings,
                outputs,
            } => {
                let relation = program.relation(*relation);
                require_read(host.authority(), relation)?;
                let bindings = self.resolve_bindings(program, program.bindings(*bindings));
                let rows = host
                    .scan_relation(relation, &bindings)
                    .map_err(RuntimeError::Kernel)?;
                let outputs = program.query_bindings(*outputs);
                let mut result = Vec::with_capacity(rows.len());
                'row: for row in rows {
                    let mut entries = Vec::<(Value, Value)>::with_capacity(outputs.len());
                    for output in outputs {
                        let key = Value::symbol(output.name);
                        let value = row.values()[output.position as usize].clone();
                        if let Some((_, existing)) = entries
                            .iter()
                            .find(|(existing_key, _)| existing_key == &key)
                        {
                            if existing != &value {
                                continue 'row;
                            }
                            continue;
                        }
                        entries.push((key, value));
                    }
                    result.push(Value::map(entries));
                }
                self.write_register_unchecked(*dst, Value::list(result));
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ScanValue { dst, relation, key } => {
                let relation = program.relation(*relation);
                require_read(host.authority(), relation)?;
                let key = self.resolve_operand_ref(program, *key);
                let value = host
                    .scan_relation(relation, &[Some(key), None])?
                    .first()
                    .map(|row| row.values()[1].clone())
                    .unwrap_or_else(Value::nothing);
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Assert { relation, values } => {
                let relation = program.relation(*relation);
                require_write(host.authority(), relation)?;
                host.assert_tuple(
                    relation,
                    self.resolve_tuple(program, program.operands(*values)),
                )?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Retract { relation, values } => {
                let relation = program.relation(*relation);
                require_write(host.authority(), relation)?;
                host.retract_tuple(
                    relation,
                    self.resolve_tuple(program, program.operands(*values)),
                )?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::RetractWhere { relation, bindings } => {
                let relation = program.relation(*relation);
                require_read(host.authority(), relation)?;
                require_write(host.authority(), relation)?;
                let bindings = self.resolve_bindings(program, program.bindings(*bindings));
                host.retract_matching(relation, &bindings)?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ScanDynamic {
                dst,
                relation,
                args,
            } => {
                let relation = program.relation(*relation);
                require_read(host.authority(), relation)?;
                let (bindings, outputs) =
                    self.resolve_relation_bindings(program, program.relation_args(*args))?;
                let rows = host
                    .scan_relation(relation, &bindings)
                    .map_err(RuntimeError::Kernel)?;
                let value = if outputs.is_empty() {
                    Value::bool(!rows.is_empty())
                } else {
                    Value::list(query_rows(rows, &outputs))
                };
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::AssertDynamic { relation, args } => {
                let relation = program.relation(*relation);
                require_write(host.authority(), relation)?;
                host.assert_tuple(
                    relation,
                    Tuple::new(
                        self.resolve_relation_values(program, program.relation_args(*args))?,
                    ),
                )?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::RetractDynamic { relation, args } => {
                let relation = program.relation(*relation);
                require_write(host.authority(), relation)?;
                let (bindings, _) =
                    self.resolve_relation_bindings(program, program.relation_args(*args))?;
                if bindings.iter().any(Option::is_none) {
                    require_read(host.authority(), relation)?;
                    host.retract_matching(relation, &bindings)?;
                } else {
                    host.retract_tuple(relation, Tuple::new(bindings.into_iter().flatten()))?;
                }
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ReplaceFunctional { relation, values } => {
                let relation = program.relation(*relation);
                require_write(host.authority(), relation)?;
                host.replace_functional_tuple(
                    relation,
                    self.resolve_tuple(program, program.operands(*values)),
                )?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Branch {
                condition,
                if_true,
                if_false,
            } => {
                let target = if truthy(self.read_register_unchecked(*condition)) {
                    if_true.0 as usize
                } else {
                    if_false.0 as usize
                };
                self.current_frame_mut_unchecked().ip = target;
                Ok(VmHostResponse::Continue)
            }
            Opcode::Jump { target } => {
                self.current_frame_mut_unchecked().ip = target.0 as usize;
                Ok(VmHostResponse::Continue)
            }
            Opcode::EnterTry {
                catches,
                finally,
                end,
            } => {
                let catches = program
                    .catches(*catches)
                    .iter()
                    .map(|catch| CatchHandler {
                        code: catch.code.map(|id| program.constant(id).clone()),
                        binding: catch.binding,
                        target: catch.target.0 as usize,
                    })
                    .collect();
                self.current_frame_mut_unchecked()
                    .try_stack
                    .push(TryRegion {
                        catches,
                        finally: finally.map(|target| target.0 as usize),
                        end: end.0 as usize,
                    });
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::ExitTry => self.exit_try_region(),
            Opcode::EndFinally => self.end_finally(),
            Opcode::Emit { target, value } => {
                let target_value = self.resolve_operand_ref(program, *target);
                let target = target_value
                    .as_identity()
                    .ok_or(RuntimeError::InvalidEffectTarget(target_value))?;
                let value = self.resolve_operand_ref(program, *value);
                host.emit(target, value)?;
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::LoadFunction {
                dst,
                program: callee,
                captures,
                min_arity,
                max_arity,
            } => {
                let callee = program.program(*callee);
                let callee_id = self.intern_program(Arc::clone(callee));
                let captures = self.resolve_operands(program, program.operands(*captures));
                let function = self.intern_function(CallableInfo {
                    program: callee_id,
                    captures,
                    min_arity: *min_arity as usize,
                    max_arity: *max_arity as usize,
                })?;
                self.write_register_unchecked(*dst, Value::function(function));
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::CallValue { dst, callee, args } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let callee = self.resolve_operand_ref(program, *callee);
                let function = callee
                    .as_function()
                    .ok_or_else(|| RuntimeError::InvalidCallable(callee.clone()))?;
                let callable = self.callable(function)?;
                let user_args = self.resolve_operands(program, program.operands(*args));
                if user_args.len() < callable.min_arity || user_args.len() > callable.max_arity {
                    return Err(RuntimeError::InvalidCallArity {
                        expected_min: callable.min_arity,
                        expected_max: callable.max_arity,
                        actual: user_args.len(),
                    });
                }
                let register_count = self.program_unchecked(callable.program).register_count();
                let mut args = callable.captures;
                args.extend(user_args);
                self.advance_ip_unchecked();
                self.state.frames.push(Frame::new(
                    callable.program,
                    register_count,
                    Some(*dst),
                    args,
                )?);
                Ok(VmHostResponse::Continue)
            }
            Opcode::Call {
                dst,
                program: callee,
                args,
            } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let args = self.resolve_operands(program, program.operands(*args));
                let callee = program.program(*callee);
                let callee_id = self.intern_program(Arc::clone(callee));
                let register_count = callee.register_count();
                self.advance_ip_unchecked();
                self.state
                    .frames
                    .push(Frame::new(callee_id, register_count, Some(*dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Opcode::BuiltinCall { dst, name, args } => {
                let args = self.resolve_operands(program, program.operands(*args));
                let value = host.call_builtin(*name, &args)?;
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::BuiltinCallDynamic { dst, name, args } => {
                let args = self.resolve_list_items(program, program.list_items(*args))?;
                let value = host.call_builtin(*name, &args)?;
                self.write_register_unchecked(*dst, value);
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Continue)
            }
            Opcode::Dispatch {
                dst,
                spec,
                selector,
                roles,
            } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let selector = self.resolve_operand_ref(program, *selector);
                let mut roles = program
                    .roles(*roles)
                    .iter()
                    .map(|(role, value)| {
                        (
                            program.constant(*role).clone(),
                            self.resolve_operand_ref(program, *value),
                        )
                    })
                    .collect::<Vec<_>>();
                normalize_dispatch_roles(&mut roles);
                let spec = program.dispatch_spec(*spec);
                let methods = applicable_method_calls_normalized(
                    host,
                    spec.relations,
                    selector.clone(),
                    &roles,
                )?;
                let entry =
                    select_authorized_method_call(host.authority(), selector.clone(), methods)?;
                let method = &entry.method;
                let program_id = method_program_id(host, spec.program_relation, method)?
                    .ok_or_else(|| RuntimeError::MissingMethodProgram {
                        method: method.clone(),
                    })?;
                let callee_id = self.resolve_program_id(host, spec.program_bytes, &program_id)?;
                let register_count = self.program_unchecked(callee_id).register_count();
                let args = entry.args.ok_or_else(|| {
                    RuntimeError::ProgramArtifact("method parameter position is invalid".to_owned())
                })?;
                self.advance_ip_unchecked();
                self.state
                    .frames
                    .push(Frame::new(callee_id, register_count, Some(*dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Opcode::DynamicDispatch {
                dst,
                spec,
                selector,
                roles,
            } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let selector = self.resolve_operand_ref(program, *selector);
                let roles = self.resolve_operand_ref(program, *roles);
                let mut roles = dynamic_dispatch_roles(&roles)?;
                normalize_dispatch_roles(&mut roles);
                let spec = program.dispatch_spec(*spec);
                let methods = applicable_method_calls_normalized(
                    host,
                    spec.relations,
                    selector.clone(),
                    &roles,
                )?;
                let entry =
                    select_authorized_method_call(host.authority(), selector.clone(), methods)?;
                let method = &entry.method;
                let program_id = method_program_id(host, spec.program_relation, method)?
                    .ok_or_else(|| RuntimeError::MissingMethodProgram {
                        method: method.clone(),
                    })?;
                let callee_id = self.resolve_program_id(host, spec.program_bytes, &program_id)?;
                let register_count = self.program_unchecked(callee_id).register_count();
                let args = entry.args.ok_or_else(|| {
                    RuntimeError::ProgramArtifact("method parameter position is invalid".to_owned())
                })?;
                self.advance_ip_unchecked();
                self.state
                    .frames
                    .push(Frame::new(callee_id, register_count, Some(*dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Opcode::PositionalDispatch {
                dst,
                spec,
                selector,
                args,
            } => {
                if self.state.frames.len() >= max_call_depth {
                    return Err(RuntimeError::MaxCallDepthExceeded {
                        max_depth: max_call_depth,
                    });
                }
                let selector = self.resolve_operand_ref(program, *selector);
                let args = self.resolve_operands(program, program.operands(*args));
                let spec = program.dispatch_spec(*spec);
                let methods =
                    applicable_positional_methods(host, spec.relations, selector.clone(), &args)?;
                let method = select_authorized_method(host.authority(), selector.clone(), methods)?;
                let program_id = method_program_id(host, spec.program_relation, &method)?
                    .ok_or_else(|| RuntimeError::MissingMethodProgram {
                        method: method.clone(),
                    })?;
                let callee_id = self.resolve_program_id(host, spec.program_bytes, &program_id)?;
                let register_count = self.program_unchecked(callee_id).register_count();
                self.advance_ip_unchecked();
                self.state
                    .frames
                    .push(Frame::new(callee_id, register_count, Some(*dst), args)?);
                Ok(VmHostResponse::Continue)
            }
            Opcode::SpawnDispatch {
                dst,
                selector,
                roles,
                delay,
            } => {
                let selector = self.resolve_operand_ref(program, *selector);
                let selector_symbol = selector
                    .as_symbol()
                    .ok_or_else(|| RuntimeError::InvalidSpawnSelector(selector.clone()))?;
                let mut spawn_roles = Vec::new();
                for (role, value) in program.roles(*roles) {
                    let role = program.constant(*role).clone();
                    let role_symbol = role
                        .as_symbol()
                        .ok_or_else(|| RuntimeError::InvalidSpawnRole(role.clone()))?;
                    spawn_roles.push((role_symbol, self.resolve_operand_ref(program, *value)));
                }
                let delay_millis = delay
                    .map(|delay| {
                        self.suspend_duration(self.resolve_operand_ref(program, delay))
                            .and_then(|kind| match kind {
                                SuspendKind::TimedMillis(millis) => Ok(millis),
                                _ => unreachable!(),
                            })
                    })
                    .transpose()?;
                normalize_spawn_roles(&mut spawn_roles);
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Spawn(SpawnRequest {
                    selector: selector_symbol,
                    target: SpawnTarget::NamedRoles(spawn_roles),
                    delay_millis,
                }))
            }
            Opcode::SpawnPositionalDispatch {
                dst,
                selector,
                args,
                delay,
            } => {
                let selector = self.resolve_operand_ref(program, *selector);
                let selector_symbol = selector
                    .as_symbol()
                    .ok_or_else(|| RuntimeError::InvalidSpawnSelector(selector.clone()))?;
                let args = self.resolve_operands(program, program.operands(*args));
                let delay_millis = delay
                    .map(|delay| {
                        self.suspend_duration(self.resolve_operand_ref(program, delay))
                            .and_then(|kind| match kind {
                                SuspendKind::TimedMillis(millis) => Ok(millis),
                                _ => unreachable!(),
                            })
                    })
                    .transpose()?;
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Spawn(SpawnRequest {
                    selector: selector_symbol,
                    target: SpawnTarget::PositionalArgs(args),
                    delay_millis,
                }))
            }
            Opcode::Commit => {
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Commit)
            }
            Opcode::Suspend { kind } => {
                self.advance_ip_unchecked();
                Ok(VmHostResponse::Suspend(program.suspend_kind(*kind).clone()))
            }
            Opcode::SuspendValue { dst, duration } => {
                let kind = duration
                    .map(|duration| {
                        self.suspend_duration(self.resolve_operand_ref(program, duration))
                    })
                    .transpose()?
                    .unwrap_or(SuspendKind::Never);
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Suspend(kind))
            }
            Opcode::CommitValue { dst } => {
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Suspend(SuspendKind::Commit))
            }
            Opcode::Read { dst, metadata } => {
                let metadata = metadata
                    .map(|metadata| self.resolve_operand_ref(program, metadata))
                    .unwrap_or_else(Value::nothing);
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Suspend(SuspendKind::WaitingForInput(
                    metadata,
                )))
            }
            Opcode::MailboxRecv {
                dst,
                receivers,
                timeout,
            } => {
                let receivers_value = self.resolve_operand_ref(program, *receivers);
                let Some(receivers) = receivers_value.with_list(<[Value]>::to_vec) else {
                    return Err(RuntimeError::InvalidBuiltinCall {
                        name: Symbol::intern("mailbox_recv"),
                        message: "mailbox_recv expects a list of receive caps".to_owned(),
                    });
                };
                if receivers.is_empty() {
                    return Err(RuntimeError::InvalidBuiltinCall {
                        name: Symbol::intern("mailbox_recv"),
                        message: "mailbox_recv expects at least one receive cap".to_owned(),
                    });
                }
                for receiver in &receivers {
                    host.validate_mailbox_receiver(receiver)?;
                }
                let timeout_millis = timeout
                    .map(|timeout| {
                        self.suspend_duration(self.resolve_operand_ref(program, timeout))
                            .and_then(|kind| match kind {
                                SuspendKind::TimedMillis(millis) => Ok(millis),
                                _ => unreachable!(),
                            })
                    })
                    .transpose()?;
                self.advance_ip_unchecked();
                self.state.pending_resume = Some(*dst);
                Ok(VmHostResponse::Suspend(SuspendKind::MailboxRecv(
                    MailboxRecvRequest {
                        receivers,
                        timeout_millis,
                    },
                )))
            }
            Opcode::RollbackRetry => {
                self.advance_ip_unchecked();
                Ok(VmHostResponse::RollbackRetry)
            }
            Opcode::Return { value } => {
                let value = self.resolve_operand_ref(program, *value);
                self.return_from_frame(value)
            }
            Opcode::Abort { error } => {
                let error = self.resolve_operand_ref(program, *error);
                Ok(VmHostResponse::Abort(error))
            }
            Opcode::Raise {
                error,
                message,
                value,
            } => {
                let error = self.resolve_operand_ref(program, *error);
                let message = message.map(|message| self.resolve_operand_ref(program, message));
                let value = value.map(|value| self.resolve_operand_ref(program, value));
                let error = normalize_raised_error(error, message, value)?;
                self.begin_raise(error)
            }
        }
    }

    fn return_from_frame(&mut self, value: Value) -> Result<VmHostResponse, RuntimeError> {
        {
            let frame = self.current_frame_mut_unchecked();
            if frame.pending_finally.pop().is_some() {
                // A return from inside a finally body replaces the control flow
                // that originally entered the finally.
            }
            while let Some(region) = frame.try_stack.pop() {
                if let Some(finally) = region.finally {
                    frame
                        .pending_finally
                        .push(FinallyContinuation::Return(value));
                    frame.ip = finally;
                    return Ok(VmHostResponse::Continue);
                }
            }
        }

        let frame = self
            .state
            .frames
            .pop()
            .ok_or(RuntimeError::EmptyCallStack)?;
        let Some(return_register) = frame.return_register else {
            return Ok(VmHostResponse::Complete(value));
        };
        self.write_register_unchecked(return_register, value);
        Ok(VmHostResponse::Continue)
    }

    fn exit_try_region(&mut self) -> Result<VmHostResponse, RuntimeError> {
        let frame = self.current_frame_mut()?;
        let region = frame.try_stack.pop().ok_or(RuntimeError::EmptyTryStack)?;
        if let Some(finally) = region.finally {
            frame
                .pending_finally
                .push(FinallyContinuation::Normal(region.end));
            frame.ip = finally;
        } else {
            frame.ip = region.end;
        }
        Ok(VmHostResponse::Continue)
    }

    fn end_finally(&mut self) -> Result<VmHostResponse, RuntimeError> {
        let continuation = self
            .current_frame_mut()?
            .pending_finally
            .pop()
            .ok_or(RuntimeError::EmptyTryStack)?;
        match continuation {
            FinallyContinuation::Normal(target) => {
                self.current_frame_mut_unchecked().ip = target;
                Ok(VmHostResponse::Continue)
            }
            FinallyContinuation::Raise(error) => self.begin_raise(error),
            FinallyContinuation::Return(value) => self.return_from_frame(value),
        }
    }

    fn begin_raise(&mut self, error: Value) -> Result<VmHostResponse, RuntimeError> {
        loop {
            let Some(frame) = self.state.frames.last_mut() else {
                return Ok(VmHostResponse::Abort(error));
            };

            if frame.pending_finally.pop().is_some() {
                // A raise from inside a finally body replaces the control flow
                // that originally entered the finally.
            }

            while let Some(region) = frame.try_stack.pop() {
                if let Some(handler) = matching_handler(&region.catches, &error) {
                    if let Some(binding) = handler.binding {
                        let register_count = frame.registers.len();
                        let slot = frame.registers.get_mut(binding.0 as usize).ok_or(
                            RuntimeError::RegisterOutOfBounds {
                                register: binding.0,
                                register_count,
                            },
                        )?;
                        *slot = error;
                    }
                    if let Some(finally) = region.finally {
                        frame.try_stack.push(TryRegion {
                            catches: Vec::new(),
                            finally: Some(finally),
                            end: region.end,
                        });
                    }
                    frame.ip = handler.target;
                    return Ok(VmHostResponse::Continue);
                }

                if let Some(finally) = region.finally {
                    frame
                        .pending_finally
                        .push(FinallyContinuation::Raise(error));
                    frame.ip = finally;
                    return Ok(VmHostResponse::Continue);
                }
            }

            self.state.frames.pop();
        }
    }

    fn advance_ip_unchecked(&mut self) {
        self.current_frame_mut_unchecked().ip += 1;
    }

    fn intern_program(&mut self, program: Arc<Program>) -> usize {
        if let Some((index, _)) = self
            .state
            .programs
            .iter()
            .enumerate()
            .find(|(_, loaded)| Arc::ptr_eq(loaded, &program))
        {
            return index;
        }
        let index = self.state.programs.len();
        self.state.programs.push(program);
        index
    }

    fn intern_function(&mut self, function: CallableInfo) -> Result<FunctionId, RuntimeError> {
        if let Some((index, _)) = self
            .state
            .functions
            .iter()
            .enumerate()
            .find(|(_, loaded)| **loaded == function)
        {
            return FunctionId::new(index as u64).ok_or_else(|| {
                RuntimeError::ProgramArtifact("function id exceeds value payload range".to_owned())
            });
        }
        let index = self.state.functions.len();
        let function_id = FunctionId::new(index as u64).ok_or_else(|| {
            RuntimeError::ProgramArtifact("function id exceeds value payload range".to_owned())
        })?;
        self.state.functions.push(function);
        Ok(function_id)
    }

    fn callable(&self, id: FunctionId) -> Result<CallableInfo, RuntimeError> {
        self.state
            .functions
            .get(id.raw() as usize)
            .cloned()
            .ok_or(RuntimeError::InvalidFunction(id.raw()))
    }

    fn resolve_program_id<H: VmHost>(
        &mut self,
        host: &mut H,
        program_bytes_relation: RelationId,
        program_id: &Value,
    ) -> Result<usize, RuntimeError> {
        if let Some((_, index)) = self
            .state
            .resolved_programs
            .iter()
            .find(|(resolved, _)| resolved == program_id)
        {
            return Ok(*index);
        }
        let program = host.resolve_program(program_bytes_relation, program_id)?;
        let index = self.intern_program(program);
        self.state
            .resolved_programs
            .push((program_id.clone(), index));
        Ok(index)
    }

    fn program_unchecked(&self, program: usize) -> &Program {
        self.state.programs[program].as_ref()
    }

    fn current_frame(&self) -> Result<&Frame, RuntimeError> {
        self.state.frames.last().ok_or(RuntimeError::EmptyCallStack)
    }

    fn current_frame_unchecked(&self) -> &Frame {
        debug_assert!(!self.state.frames.is_empty());
        self.state.frames.last().unwrap()
    }

    fn current_frame_mut(&mut self) -> Result<&mut Frame, RuntimeError> {
        self.state
            .frames
            .last_mut()
            .ok_or(RuntimeError::EmptyCallStack)
    }

    fn current_frame_mut_unchecked(&mut self) -> &mut Frame {
        debug_assert!(!self.state.frames.is_empty());
        self.state.frames.last_mut().unwrap()
    }

    fn read_register_unchecked(&self, register: Register) -> &Value {
        let frame = self
            .state
            .frames
            .last()
            .expect("validated VM execution requires a current frame");
        debug_assert!((register.0 as usize) < frame.registers.len());
        &frame.registers[register.0 as usize]
    }

    fn write_register(&mut self, register: Register, value: Value) -> Result<(), RuntimeError> {
        let frame = self.current_frame_mut()?;
        let register_count = frame.registers.len();
        let slot = frame.registers.get_mut(register.0 as usize).ok_or(
            RuntimeError::RegisterOutOfBounds {
                register: register.0,
                register_count,
            },
        )?;
        *slot = value;
        Ok(())
    }

    fn write_register_unchecked(&mut self, register: Register, value: Value) {
        let frame = self.current_frame_mut_unchecked();
        debug_assert!((register.0 as usize) < frame.registers.len());
        frame.registers[register.0 as usize] = value;
    }

    #[inline]
    fn resolve_operand_ref(&self, program: &Program, operand: OperandRef) -> Value {
        match operand {
            OperandRef::Register(register) => self.read_register_unchecked(register).clone(),
            OperandRef::Constant(id) => program.constant(id).clone(),
        }
    }

    #[inline]
    fn resolve_operands(&self, program: &Program, operands: &[OperandRef]) -> Vec<Value> {
        operands
            .iter()
            .map(|operand| self.resolve_operand_ref(program, *operand))
            .collect()
    }

    #[inline]
    fn build_list(&self, program: &Program, items: &[CompactListItem]) -> Value {
        let mut values = Vec::new();
        for item in items {
            match item {
                CompactListItem::Value(operand) => {
                    values.push(self.resolve_operand_ref(program, *operand));
                }
                CompactListItem::Splice(operand) => {
                    let splice = self.resolve_operand_ref(program, *operand);
                    let Some(()) = splice.with_list(|items| {
                        values.extend(items.iter().cloned());
                    }) else {
                        return Value::nothing();
                    };
                }
            }
        }
        Value::list(values)
    }

    fn resolve_list_items(
        &self,
        program: &Program,
        items: &[CompactListItem],
    ) -> Result<Vec<Value>, RuntimeError> {
        let mut values = Vec::new();
        for item in items {
            match item {
                CompactListItem::Value(operand) => {
                    values.push(self.resolve_operand_ref(program, *operand));
                }
                CompactListItem::Splice(operand) => {
                    let splice = self.resolve_operand_ref(program, *operand);
                    let Some(()) = splice.with_list(|items| {
                        values.extend(items.iter().cloned());
                    }) else {
                        return Err(RuntimeError::InvalidArgumentSplice(splice));
                    };
                }
            }
        }
        Ok(values)
    }

    #[inline]
    fn resolve_tuple(&self, program: &Program, values: &[OperandRef]) -> Tuple {
        Tuple::new(self.resolve_operands(program, values))
    }

    #[inline]
    fn resolve_bindings(
        &self,
        program: &Program,
        bindings: &[Option<OperandRef>],
    ) -> Vec<Option<Value>> {
        bindings
            .iter()
            .map(|binding| binding.map(|operand| self.resolve_operand_ref(program, operand)))
            .collect()
    }

    fn resolve_relation_values(
        &self,
        program: &Program,
        args: &[CompactRelationArg],
    ) -> Result<Vec<Value>, RuntimeError> {
        let mut values = Vec::new();
        self.visit_relation_args(program, args, |arg| {
            match arg {
                ExpandedRelationArg::Value(value) => values.push(value),
                ExpandedRelationArg::Query(_) | ExpandedRelationArg::Hole => {
                    return Err(RuntimeError::InvalidBuiltinCall {
                        name: Symbol::intern("relation"),
                        message: "relation value operation cannot contain query variables or holes"
                            .to_owned(),
                    });
                }
            }
            Ok(())
        })?;
        Ok(values)
    }

    fn resolve_relation_bindings(
        &self,
        program: &Program,
        args: &[CompactRelationArg],
    ) -> Result<(Vec<Option<Value>>, Vec<QueryBinding>), RuntimeError> {
        let mut bindings = Vec::new();
        let mut outputs = Vec::new();
        self.visit_relation_args(program, args, |arg| {
            match arg {
                ExpandedRelationArg::Value(value) => bindings.push(Some(value)),
                ExpandedRelationArg::Query(name) => {
                    let position = u16::try_from(bindings.len()).map_err(|_| {
                        RuntimeError::RelationArgumentCountExceeded {
                            count: bindings.len(),
                        }
                    })?;
                    outputs.push(QueryBinding { name, position });
                    bindings.push(None);
                }
                ExpandedRelationArg::Hole => bindings.push(None),
            }
            Ok(())
        })?;
        Ok((bindings, outputs))
    }

    fn visit_relation_args(
        &self,
        program: &Program,
        args: &[CompactRelationArg],
        mut visit: impl FnMut(ExpandedRelationArg) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        for arg in args {
            match arg {
                CompactRelationArg::Value(operand) => {
                    visit(ExpandedRelationArg::Value(
                        self.resolve_operand_ref(program, *operand),
                    ))?;
                }
                CompactRelationArg::Splice(operand) => {
                    let splice = self.resolve_operand_ref(program, *operand);
                    let Some(result) = splice.with_list(|items| {
                        for item in items {
                            visit(ExpandedRelationArg::Value(item.clone()))?;
                        }
                        Ok::<(), RuntimeError>(())
                    }) else {
                        return Err(RuntimeError::InvalidRelationSplice(splice));
                    };
                    result?;
                }
                CompactRelationArg::Query(name) => visit(ExpandedRelationArg::Query(*name))?,
                CompactRelationArg::Hole => visit(ExpandedRelationArg::Hole)?,
            }
        }
        Ok(())
    }

    fn suspend_duration(&self, value: Value) -> Result<SuspendKind, RuntimeError> {
        let seconds = if let Some(seconds) = value.as_int() {
            seconds as f64
        } else if let Some(seconds) = value.as_float() {
            seconds
        } else {
            return Err(RuntimeError::InvalidSuspendDuration(value));
        };
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(RuntimeError::InvalidSuspendDuration(value));
        }
        let millis = (seconds * 1_000.0).round();
        if millis > u64::MAX as f64 {
            return Err(RuntimeError::InvalidSuspendDuration(value));
        }
        Ok(SuspendKind::TimedMillis(millis as u64))
    }
}

fn truthy(value: &Value) -> bool {
    match value.kind() {
        ValueKind::Nothing => false,
        ValueKind::Bool => value.as_bool().unwrap_or(false),
        _ => true,
    }
}

fn eval_unary(op: RuntimeUnaryOp, value: &Value) -> Result<Value, Value> {
    match op {
        RuntimeUnaryOp::Not => Ok(Value::bool(!truthy(value))),
        RuntimeUnaryOp::Neg => value
            .checked_neg()
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid unary arithmetic", [value])),
    }
}

fn eval_binary(op: RuntimeBinaryOp, left: &Value, right: &Value) -> Result<Value, Value> {
    match op {
        RuntimeBinaryOp::Eq => Ok(Value::bool(left == right)),
        RuntimeBinaryOp::Ne => Ok(Value::bool(left != right)),
        RuntimeBinaryOp::Lt => Ok(Value::bool(left < right)),
        RuntimeBinaryOp::Le => Ok(Value::bool(left <= right)),
        RuntimeBinaryOp::Gt => Ok(Value::bool(left > right)),
        RuntimeBinaryOp::Ge => Ok(Value::bool(left >= right)),
        RuntimeBinaryOp::Add => left
            .checked_add(right)
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid addition", [left, right])),
        RuntimeBinaryOp::Sub => left
            .checked_sub(right)
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid subtraction", [left, right])),
        RuntimeBinaryOp::Mul => left
            .checked_mul(right)
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid multiplication", [left, right])),
        RuntimeBinaryOp::Div if is_zero(right) => {
            Err(arithmetic_error("E_DIV", "division by zero", [left, right]))
        }
        RuntimeBinaryOp::Div => left
            .checked_div(right)
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid division", [left, right])),
        RuntimeBinaryOp::Rem if is_zero(right) => Err(arithmetic_error(
            "E_DIV",
            "remainder by zero",
            [left, right],
        )),
        RuntimeBinaryOp::Rem => left
            .checked_rem(right)
            .ok_or_else(|| arithmetic_error("E_ARITH", "invalid remainder", [left, right])),
    }
}

fn is_zero(value: &Value) -> bool {
    value.as_int() == Some(0) || value.as_float() == Some(0.0)
}

fn arithmetic_error<const N: usize>(code: &str, message: &str, values: [&Value; N]) -> Value {
    Value::error(
        Symbol::intern(code),
        Some(message),
        Some(Value::list(values.into_iter().cloned())),
    )
}

fn index_value(collection: &Value, index: &Value) -> Value {
    if let Some((start, end)) = index.with_range(|start, end| (start.clone(), end.cloned()))
        && let Some(len) = collection.list_len()
    {
        return list_range_slice(collection, len, &start, end.as_ref())
            .unwrap_or_else(Value::nothing);
    }
    if let Some(index) = index.as_int()
        && index >= 0
        && let Some(value) = collection.list_get(index as usize)
    {
        return value;
    }
    collection.map_get(index).unwrap_or_else(Value::nothing)
}

fn list_range_slice(
    collection: &Value,
    len: usize,
    start: &Value,
    end: Option<&Value>,
) -> Option<Value> {
    let start = ordinal_index(start)?;
    let end_exclusive = match end {
        Some(end) => {
            let end = ordinal_index(end)?;
            if end < start {
                return None;
            }
            end.checked_add(1)?
        }
        None => len,
    };
    collection.list_slice(start, end_exclusive)
}

fn set_index_value(collection: &Value, index: &Value, value: Value) -> Value {
    collection
        .index_set(index, value)
        .unwrap_or_else(Value::nothing)
}

fn error_field_value(error: &Value, field: ErrorField) -> Value {
    if let Some(code) = error.as_error_code() {
        return match field {
            ErrorField::Code => Value::error_code(code),
            ErrorField::Message | ErrorField::Value => Value::nothing(),
        };
    }
    error
        .with_error(|error| match field {
            ErrorField::Code => Value::error_code(error.code()),
            ErrorField::Message => error.message().map_or_else(Value::nothing, Value::string),
            ErrorField::Value => error.value().cloned().unwrap_or_else(Value::nothing),
        })
        .unwrap_or_else(Value::nothing)
}

fn collection_len(collection: &Value) -> Value {
    let len = collection
        .list_len()
        .or_else(|| collection.map_len())
        .unwrap_or(0);
    i64::try_from(len)
        .ok()
        .and_then(|len| Value::int(len).ok())
        .unwrap_or_else(Value::nothing)
}

fn query_rows(rows: Vec<Tuple>, outputs: &[QueryBinding]) -> Vec<Value> {
    let mut result = Vec::with_capacity(rows.len());
    'row: for row in rows {
        let mut entries = Vec::<(Value, Value)>::with_capacity(outputs.len());
        for output in outputs {
            let key = Value::symbol(output.name);
            let value = row.values()[output.position as usize].clone();
            if let Some((_, existing)) = entries
                .iter()
                .find(|(existing_key, _)| existing_key == &key)
            {
                if existing != &value {
                    continue 'row;
                }
                continue;
            }
            entries.push((key, value));
        }
        result.push(Value::map(entries));
    }
    result
}

fn collection_key_at(collection: &Value, index: &Value) -> Value {
    let Some(index) = ordinal_index(index) else {
        return Value::nothing();
    };
    if collection.list_len().is_some() {
        return i64::try_from(index)
            .ok()
            .and_then(|index| Value::int(index).ok())
            .unwrap_or_else(Value::nothing);
    }
    collection
        .with_map(|entries| entries.get(index).map(|(key, _)| key.clone()))
        .flatten()
        .unwrap_or_else(Value::nothing)
}

fn collection_value_at(collection: &Value, index: &Value) -> Value {
    let Some(index) = ordinal_index(index) else {
        return Value::nothing();
    };
    collection
        .list_get(index)
        .or_else(|| {
            collection
                .with_map(|entries| entries.get(index).map(|(_, value)| value.clone()))
                .flatten()
        })
        .unwrap_or_else(Value::nothing)
}

fn one_value(value: &Value) -> Result<Value, Value> {
    let Some(len) = value.list_len() else {
        return Ok(Value::nothing());
    };
    match len {
        0 => Ok(Value::nothing()),
        1 => {
            let row = value.list_get(0).unwrap_or_else(Value::nothing);
            if row.map_len() == Some(1) {
                return Ok(row
                    .with_map(|entries| entries[0].1.clone())
                    .unwrap_or_else(Value::nothing));
            }
            Ok(row)
        }
        _ => Err(Value::error(
            Symbol::intern("E_AMBIGUOUS"),
            Some("one expected at most one result"),
            Some(value.clone()),
        )),
    }
}

fn select_authorized_method_call(
    authority: &AuthorityContext,
    selector: Value,
    methods: Vec<ApplicableMethodCall>,
) -> Result<ApplicableMethodCall, RuntimeError> {
    let mut selected = None;
    let mut ambiguous = Vec::new();
    for entry in methods {
        if !authority.can_invoke_method(&entry.method) {
            continue;
        }
        if let Some(previous) = selected.replace(entry) {
            if ambiguous.is_empty() {
                ambiguous.push(previous.method);
            }
            ambiguous.push(selected.as_ref().unwrap().method.clone());
        }
    }
    if !ambiguous.is_empty() {
        return Err(RuntimeError::AmbiguousDispatch {
            selector,
            methods: ambiguous,
        });
    }
    selected.ok_or(RuntimeError::NoApplicableMethod { selector })
}

fn dynamic_dispatch_roles(value: &Value) -> Result<Vec<(Value, Value)>, RuntimeError> {
    value
        .with_map(|entries| entries.to_vec())
        .ok_or_else(|| RuntimeError::InvalidBuiltinCall {
            name: Symbol::intern("invoke"),
            message: "invoke expects roles to be a map".to_owned(),
        })
}

fn normalize_spawn_roles(roles: &mut [(Symbol, Value)]) {
    roles.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
}

fn select_authorized_method(
    authority: &AuthorityContext,
    selector: Value,
    methods: Vec<Value>,
) -> Result<Value, RuntimeError> {
    let mut selected = None;
    let mut ambiguous = Vec::new();
    for method in methods {
        if !authority.can_invoke_method(&method) {
            continue;
        }
        if let Some(previous) = selected.replace(method) {
            if ambiguous.is_empty() {
                ambiguous.push(previous);
            }
            ambiguous.push(selected.as_ref().unwrap().clone());
        }
    }
    if !ambiguous.is_empty() {
        return Err(RuntimeError::AmbiguousDispatch {
            selector,
            methods: ambiguous,
        });
    }
    selected.ok_or(RuntimeError::NoApplicableMethod { selector })
}

fn ordinal_index(index: &Value) -> Option<usize> {
    let index = index.as_int()?;
    usize::try_from(index).ok()
}

fn require_read(
    authority: &AuthorityContext,
    relation: mica_relation_kernel::RelationId,
) -> Result<(), RuntimeError> {
    if authority.can_read_relation(relation) {
        Ok(())
    } else {
        Err(RuntimeError::PermissionDenied {
            operation: "read",
            target: Value::identity(relation),
        })
    }
}

fn require_write(
    authority: &AuthorityContext,
    relation: mica_relation_kernel::RelationId,
) -> Result<(), RuntimeError> {
    if authority.can_write_relation(relation) {
        Ok(())
    } else {
        Err(RuntimeError::PermissionDenied {
            operation: "write",
            target: Value::identity(relation),
        })
    }
}

fn matching_handler<'a>(catches: &'a [CatchHandler], error: &Value) -> Option<&'a CatchHandler> {
    let error_code = error.error_code_symbol();
    catches.iter().find(|catch| match &catch.code {
        Some(code) => code.error_code_symbol().is_some() && code.error_code_symbol() == error_code,
        None => true,
    })
}

fn normalize_raised_error(
    error: Value,
    message: Option<Value>,
    value: Option<Value>,
) -> Result<Value, RuntimeError> {
    let message = message
        .and_then(|message| {
            if message.kind() == ValueKind::Nothing {
                None
            } else {
                Some(error_message_text(message))
            }
        })
        .transpose()?;
    if let Some(code) = error.as_error_code() {
        return Ok(Value::error(code, message, value));
    }
    if let Some(result) = error.with_error(|existing| {
        let message = message.or_else(|| existing.message().map(str::to_owned));
        let value = value.or_else(|| existing.value().cloned());
        Value::error(existing.code(), message, value)
    }) {
        return Ok(result);
    }
    Err(RuntimeError::InvalidRaisedValue(error))
}

fn error_message_text(value: Value) -> Result<String, RuntimeError> {
    value
        .with_str(str::to_owned)
        .ok_or(RuntimeError::InvalidErrorMessage(value))
}
