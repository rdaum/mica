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

use crate::{
    AuthorityContext, BuiltinContext, BuiltinRegistry, CapabilityGrant, CapabilityOp, Effect,
    ErrorField, Instruction, ListItem, Operand, Program, ProgramResolver, QueryBinding, Register,
    RuntimeBinaryOp, RuntimeError, Scheduler, SchedulerError, SuspendKind, Task, TaskError,
    TaskLimits, TaskOutcome,
};
use mica_relation_kernel::{ConflictPolicy, RelationId, RelationKernel, RelationMetadata, Tuple};
use mica_var::{Identity, Symbol, Value};
use std::sync::Arc;

fn rel(id: u64) -> RelationId {
    Identity::new(id).unwrap()
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

fn sym(name: &str) -> Value {
    Value::symbol(Symbol::intern(name))
}

fn err(name: &str) -> Value {
    Value::error_code(Symbol::intern(name))
}

fn error(name: &str, message: Option<&str>, value: Option<Value>) -> Value {
    Value::error(Symbol::intern(name), message, value)
}

fn strv(value: &str) -> Value {
    Value::string(value)
}

fn reg(index: u16) -> Register {
    Register(index)
}

fn r(index: u16) -> Operand {
    Operand::Register(reg(index))
}

fn v(value: Value) -> Operand {
    Operand::Value(value)
}

fn item(value: Operand) -> ListItem {
    ListItem::Value(value)
}

fn splice(value: Operand) -> ListItem {
    ListItem::Splice(value)
}

fn kernel_with_world_relations() -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(1), Symbol::intern("Portable"), 2).with_index([0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(rel(2), Symbol::intern("LocatedIn"), 2)
                .with_index([0])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0],
                }),
        )
        .unwrap();
    kernel
}

fn run_program(
    kernel: &RelationKernel,
    program: Program,
    limit: usize,
) -> Result<TaskOutcome, crate::TaskError> {
    let mut task = Task::new(
        1,
        kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        TaskLimits {
            instruction_budget: limit,
            max_retries: 10,
            max_call_depth: 50,
        },
    );
    task.run()
}

fn run_program_with_builtins(
    kernel: &RelationKernel,
    program: Program,
    builtins: BuiltinRegistry,
) -> Result<TaskOutcome, crate::TaskError> {
    let mut task = Task::new_with_builtins(
        1,
        kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        Arc::new(builtins),
        TaskLimits::default(),
    );
    task.run()
}

fn run_program_with_authority(
    kernel: &RelationKernel,
    program: Program,
    authority: AuthorityContext,
) -> Result<TaskOutcome, crate::TaskError> {
    let mut task = Task::new_with_authority(
        1,
        kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        Arc::new(BuiltinRegistry::new()),
        authority,
        TaskLimits::default(),
    );
    task.run()
}

fn emit_first_arg(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    let value = args.first().cloned().unwrap_or_else(Value::nothing);
    context.emit(value.clone())?;
    Ok(value)
}

fn mint_read_located(
    context: &mut BuiltinContext<'_, '_>,
    _args: &[Value],
) -> Result<Value, RuntimeError> {
    Ok(context.mint_capability(CapabilityGrant::relation(CapabilityOp::Read, rel(2))))
}

#[test]
fn builtin_can_return_ephemeral_capability_value() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        1,
        [
            Instruction::BuiltinCall {
                dst: reg(0),
                name: Symbol::intern("mint_read_located"),
                args: vec![],
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();
    let builtins = BuiltinRegistry::new().with_builtin("mint_read_located", mint_read_located);

    let outcome = run_program_with_builtins(&kernel, program, builtins).unwrap();
    let TaskOutcome::Complete { value, .. } = outcome else {
        panic!("expected complete outcome");
    };
    assert!(value.as_capability().is_some());
    assert!(!value.is_persistable());
}

#[test]
fn authority_context_rejects_unminted_capability_values() {
    let context = AuthorityContext::empty();
    assert!(
        context
            .grant_for(Value::capability_raw(44).unwrap())
            .is_none()
    );

    let mut context = AuthorityContext::empty();
    let cap = context.mint(CapabilityGrant::relation(CapabilityOp::Read, rel(2)));
    assert!(context.grant_for(cap).is_some());
}

#[test]
fn program_artifacts_reject_capability_constants() {
    let program = Program::new(
        1,
        [Instruction::Load {
            dst: reg(0),
            value: Value::capability_raw(1).unwrap(),
        }],
    )
    .unwrap();

    assert!(matches!(
        program.to_bytes(),
        Err(RuntimeError::ProgramArtifact(message))
            if message == "capability values are not serializable"
    ));
}

#[test]
fn authority_context_gates_runtime_writes() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        0,
        [Instruction::ReplaceFunctional {
            relation: rel(2),
            values: vec![v(int(1)), v(int(2))],
        }],
    )
    .unwrap();

    assert_eq!(
        run_program_with_authority(&kernel, program, AuthorityContext::empty()).unwrap_err(),
        TaskError::Runtime(RuntimeError::PermissionDenied {
            operation: "write",
            target: Value::identity(rel(2)),
        })
    );
}

#[test]
fn authority_context_filters_dispatch_applicability() {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(rel(40), Symbol::intern("MethodSelector"), 2).with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(rel(41), Symbol::intern("Param"), 3).with_index([0, 1]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(rel(42), Symbol::intern("Delegates"), 3).with_index([0, 2, 1]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(rel(43), Symbol::intern("MethodProgram"), 2).with_index([0]),
        )
        .unwrap();
    kernel
        .create_relation(RelationMetadata::new(
            rel(44),
            Symbol::intern("ProgramBytes"),
            2,
        ))
        .unwrap();

    let method = int(100);
    let program_id = int(900);
    let mut seed = kernel.begin();
    seed.assert(rel(40), Tuple::from([method.clone(), sym("look")]))
        .unwrap();
    seed.assert(rel(41), Tuple::from([method.clone(), sym("actor"), int(1)]))
        .unwrap();
    seed.assert(rel(43), Tuple::from([method.clone(), program_id.clone()]))
        .unwrap();
    seed.commit().unwrap();

    let method_program = Program::new(
        1,
        [Instruction::Return {
            value: v(strv("ok")),
        }],
    )
    .unwrap();
    let mut resolver = ProgramResolver::new();
    resolver.insert(program_id, method_program);
    let resolver = Arc::new(resolver);
    let dispatch_program = Arc::new(
        Program::new(
            1,
            [
                Instruction::Dispatch {
                    dst: reg(0),
                    relations: mica_relation_kernel::DispatchRelations {
                        method_selector: rel(40),
                        param: rel(41),
                        delegates: rel(42),
                    },
                    program_relation: rel(43),
                    program_bytes: rel(44),
                    selector: v(sym("look")),
                    roles: vec![(sym("actor"), v(int(1)))],
                },
                Instruction::Return { value: r(0) },
            ],
        )
        .unwrap(),
    );

    let mut denied = Task::new_with_authority(
        1,
        &kernel,
        dispatch_program.clone(),
        resolver.clone(),
        Arc::new(BuiltinRegistry::new()),
        AuthorityContext::empty(),
        TaskLimits::default(),
    );
    assert_eq!(
        denied.run().unwrap_err(),
        TaskError::Runtime(RuntimeError::NoApplicableMethod {
            selector: sym("look"),
        })
    );

    let mut authority = AuthorityContext::empty();
    authority.mint(CapabilityGrant::method(method));
    let mut allowed = Task::new_with_authority(
        2,
        &kernel,
        dispatch_program,
        resolver,
        Arc::new(BuiltinRegistry::new()),
        authority,
        TaskLimits::default(),
    );
    assert_eq!(
        allowed.run().unwrap(),
        TaskOutcome::Complete {
            value: strv("ok"),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn task_runs_take_like_method_transactionally() {
    let kernel = kernel_with_world_relations();
    let actor = int(100);
    let item = int(200);
    let room = int(300);

    let mut seed = kernel.begin();
    seed.assert(rel(1), Tuple::from([item.clone(), Value::bool(true)]))
        .unwrap();
    seed.replace_functional(rel(2), Tuple::from([item.clone(), room]))
        .unwrap();
    seed.commit().unwrap();

    let program = Program::new(
        4,
        [
            Instruction::Load {
                dst: reg(0),
                value: item.clone(),
            },
            Instruction::Load {
                dst: reg(1),
                value: actor.clone(),
            },
            Instruction::ScanExists {
                dst: reg(2),
                relation: rel(1),
                bindings: vec![Some(r(0)), Some(v(Value::bool(true)))],
            },
            Instruction::Branch {
                condition: reg(2),
                if_true: 4,
                if_false: 7,
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(1)],
            },
            Instruction::Emit {
                value: v(strv("Taken.")),
            },
            Instruction::Return {
                value: v(Value::bool(true)),
            },
            Instruction::Emit {
                value: v(strv("You can't take that.")),
            },
            Instruction::Return {
                value: v(Value::bool(false)),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("Taken.")],
            retries: 0,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), actor])]
    );
}

#[test]
fn abort_rolls_back_current_transaction_and_pending_effects() {
    let kernel = kernel_with_world_relations();
    let item = int(200);
    let room = int(300);
    let actor = int(100);

    let mut seed = kernel.begin();
    seed.replace_functional(rel(2), Tuple::from([item.clone(), room.clone()]))
        .unwrap();
    seed.commit().unwrap();

    let program = Program::new(
        2,
        [
            Instruction::Load {
                dst: reg(0),
                value: item.clone(),
            },
            Instruction::Load {
                dst: reg(1),
                value: actor,
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(1)],
            },
            Instruction::Emit {
                value: v(strv("Taken.")),
            },
            Instruction::Abort {
                error: v(sym("abort")),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Aborted {
            error: sym("abort"),
            effects: vec![],
            retries: 0,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), room])]
    );
}

#[test]
fn explicit_commit_boundary_survives_later_abort() {
    let kernel = kernel_with_world_relations();
    let item = int(200);
    let room = int(300);
    let actor = int(100);
    let box_obj = int(400);

    let mut seed = kernel.begin();
    seed.replace_functional(rel(2), Tuple::from([item.clone(), room]))
        .unwrap();
    seed.commit().unwrap();

    let program = Program::new(
        3,
        [
            Instruction::Load {
                dst: reg(0),
                value: item.clone(),
            },
            Instruction::Load {
                dst: reg(1),
                value: actor.clone(),
            },
            Instruction::Load {
                dst: reg(2),
                value: box_obj,
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(1)],
            },
            Instruction::Emit {
                value: v(strv("Committed.")),
            },
            Instruction::Commit,
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(2)],
            },
            Instruction::Emit {
                value: v(strv("Rolled back.")),
            },
            Instruction::Abort {
                error: v(sym("abort")),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Aborted {
            error: sym("abort"),
            effects: vec![strv("Committed.")],
            retries: 0,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), actor])]
    );
}

#[test]
fn binary_divide_by_zero_raises_catchable_error() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        4,
        [
            Instruction::Load {
                dst: reg(0),
                value: int(1),
            },
            Instruction::Load {
                dst: reg(1),
                value: int(0),
            },
            Instruction::EnterTry {
                catches: vec![crate::CatchHandler {
                    code: Some(err("E_DIV")),
                    binding: Some(reg(3)),
                    target: 5,
                }],
                finally: None,
                end: 6,
            },
            Instruction::Binary {
                dst: reg(2),
                op: RuntimeBinaryOp::Div,
                left: reg(0),
                right: reg(1),
            },
            Instruction::ExitTry,
            Instruction::Return { value: r(3) },
            Instruction::Return {
                value: v(Value::nothing()),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: error(
                "E_DIV",
                Some("division by zero"),
                Some(Value::list([int(1), int(0)]))
            ),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn scan_bindings_returns_query_binding_maps() {
    let kernel = kernel_with_world_relations();
    let thing = int(200);
    let room = int(300);
    let mut seed = kernel.begin();
    seed.assert(rel(2), Tuple::from([thing.clone(), room.clone()]))
        .unwrap();
    seed.commit().unwrap();
    let program = Program::new(
        1,
        [
            Instruction::ScanBindings {
                dst: reg(0),
                relation: rel(2),
                bindings: vec![Some(v(thing)), None],
                outputs: vec![QueryBinding {
                    name: Symbol::intern("room"),
                    position: 1,
                }],
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::list([Value::map([(sym("room"), room)])]),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn one_extracts_single_query_binding_value() {
    let kernel = kernel_with_world_relations();
    let room = int(300);
    let program = Program::new(
        2,
        [
            Instruction::Load {
                dst: reg(0),
                value: Value::list([Value::map([(sym("room"), room.clone())])]),
            },
            Instruction::One {
                dst: reg(1),
                src: reg(0),
            },
            Instruction::Return { value: r(1) },
        ],
    )
    .unwrap();
    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: room,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn one_raises_on_ambiguous_query_results() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        2,
        [
            Instruction::Load {
                dst: reg(0),
                value: Value::list([
                    Value::map([(sym("room"), int(300))]),
                    Value::map([(sym("room"), int(301))]),
                ]),
            },
            Instruction::One {
                dst: reg(1),
                src: reg(0),
            },
        ],
    )
    .unwrap();

    assert!(matches!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Aborted { error, .. }
            if error.error_code_symbol() == Some(Symbol::intern("E_AMBIGUOUS"))
    ));
}

#[test]
fn suspend_commits_then_resume_continues_in_new_transaction() {
    let kernel = kernel_with_world_relations();
    let item = int(200);
    let actor = int(100);
    let box_obj = int(400);

    let program = Program::new(
        3,
        [
            Instruction::Load {
                dst: reg(0),
                value: item.clone(),
            },
            Instruction::Load {
                dst: reg(1),
                value: actor.clone(),
            },
            Instruction::Load {
                dst: reg(2),
                value: box_obj.clone(),
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(1)],
            },
            Instruction::Emit {
                value: v(strv("phase 1")),
            },
            Instruction::Suspend {
                kind: SuspendKind::TimedMillis(10),
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(2)],
            },
            Instruction::Emit {
                value: v(strv("phase 2")),
            },
            Instruction::Return {
                value: v(Value::bool(true)),
            },
        ],
    )
    .unwrap();

    let mut task = Task::new(
        1,
        &kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        TaskLimits::default(),
    );
    assert_eq!(
        task.run().unwrap(),
        TaskOutcome::Suspended {
            kind: SuspendKind::TimedMillis(10),
            effects: vec![strv("phase 1")],
            retries: 0,
        }
    );
    assert_eq!(
        kernel
            .snapshot()
            .scan(rel(2), &[Some(item.clone()), None])
            .unwrap(),
        vec![Tuple::from([item.clone(), actor])]
    );

    assert_eq!(
        task.run().unwrap(),
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("phase 2")],
            retries: 0,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), box_obj])]
    );
}

#[test]
fn task_retries_from_last_clean_state_on_commit_conflict() {
    let kernel = kernel_with_world_relations();
    let item = int(200);
    let room = int(300);
    let other = int(500);
    let actor = int(100);

    let mut seed = kernel.begin();
    seed.replace_functional(rel(2), Tuple::from([item.clone(), room]))
        .unwrap();
    seed.commit().unwrap();

    let program = Program::new(
        2,
        [
            Instruction::Load {
                dst: reg(0),
                value: item.clone(),
            },
            Instruction::Load {
                dst: reg(1),
                value: actor.clone(),
            },
            Instruction::ReplaceFunctional {
                relation: rel(2),
                values: vec![r(0), r(1)],
            },
            Instruction::Emit {
                value: v(strv("Taken.")),
            },
            Instruction::Return {
                value: v(Value::bool(true)),
            },
        ],
    )
    .unwrap();
    let mut task = Task::new(
        1,
        &kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        TaskLimits {
            instruction_budget: 100,
            max_retries: 2,
            max_call_depth: 50,
        },
    );

    let mut concurrent = kernel.begin();
    concurrent
        .replace_functional(rel(2), Tuple::from([item.clone(), other]))
        .unwrap();
    concurrent.commit().unwrap();

    assert_eq!(
        task.run().unwrap(),
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("Taken.")],
            retries: 1,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), actor])]
    );
}

#[test]
fn explicit_rollback_retry_stops_at_retry_limit() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        0,
        [
            Instruction::Emit {
                value: v(strv("discarded")),
            },
            Instruction::RollbackRetry,
        ],
    )
    .unwrap();
    let mut task = Task::new(
        1,
        &kernel,
        Arc::new(program),
        Arc::new(ProgramResolver::new()),
        TaskLimits {
            instruction_budget: 100,
            max_retries: 2,
            max_call_depth: 50,
        },
    );

    assert_eq!(
        task.run().unwrap_err(),
        TaskError::ConflictRetriesExceeded { retries: 2 }
    );
    assert_eq!(task.retries(), 2);
}

#[test]
fn scheduler_records_completed_task_and_delivers_effects() {
    let kernel = kernel_with_world_relations();
    let program = Arc::new(
        Program::new(
            0,
            [
                Instruction::Emit {
                    value: v(strv("done")),
                },
                Instruction::Return {
                    value: v(Value::bool(true)),
                },
            ],
        )
        .unwrap(),
    );
    let mut scheduler = Scheduler::new(kernel);

    let (task_id, outcome) = scheduler.submit(program).unwrap();
    assert_eq!(
        outcome,
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("done")],
            retries: 0,
        }
    );
    assert_eq!(scheduler.completed(task_id), Some(&outcome));
    assert!(scheduler.suspended(task_id).is_none());
    assert_eq!(
        scheduler.effects().effects(),
        &[Effect {
            task_id,
            value: strv("done"),
        }]
    );
}

#[test]
fn scheduler_parks_and_resumes_suspended_task() {
    let kernel = kernel_with_world_relations();
    let program = Arc::new(
        Program::new(
            0,
            [
                Instruction::Emit {
                    value: v(strv("before")),
                },
                Instruction::Suspend {
                    kind: SuspendKind::TimedMillis(1),
                },
                Instruction::Emit {
                    value: v(strv("after")),
                },
                Instruction::Return {
                    value: v(Value::bool(true)),
                },
            ],
        )
        .unwrap(),
    );
    let mut scheduler = Scheduler::new(kernel);

    let (task_id, first) = scheduler.submit(program).unwrap();
    assert_eq!(
        first,
        TaskOutcome::Suspended {
            kind: SuspendKind::TimedMillis(1),
            effects: vec![strv("before")],
            retries: 0,
        }
    );
    assert_eq!(scheduler.suspended_len(), 1);
    assert_eq!(
        scheduler.suspended(task_id).map(|task| task.kind()),
        Some(&SuspendKind::TimedMillis(1))
    );
    assert_eq!(
        scheduler.effects().effects(),
        &[Effect {
            task_id,
            value: strv("before"),
        }]
    );

    let second = scheduler.resume(task_id).unwrap();
    assert_eq!(
        second,
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("after")],
            retries: 0,
        }
    );
    assert_eq!(scheduler.suspended_len(), 0);
    assert_eq!(scheduler.completed(task_id), Some(&second));
    assert_eq!(
        scheduler.effects().effects(),
        &[
            Effect {
                task_id,
                value: strv("before"),
            },
            Effect {
                task_id,
                value: strv("after"),
            },
        ]
    );
}

#[test]
fn scheduler_does_not_deliver_pending_effects_from_abort() {
    let kernel = kernel_with_world_relations();
    let program = Arc::new(
        Program::new(
            0,
            [
                Instruction::Emit {
                    value: v(strv("discarded")),
                },
                Instruction::Abort {
                    error: v(sym("abort")),
                },
            ],
        )
        .unwrap(),
    );
    let mut scheduler = Scheduler::new(kernel);

    let (task_id, outcome) = scheduler.submit(program).unwrap();
    assert_eq!(
        outcome,
        TaskOutcome::Aborted {
            error: sym("abort"),
            effects: vec![],
            retries: 0,
        }
    );
    assert_eq!(scheduler.completed(task_id), Some(&outcome));
    assert!(scheduler.effects().effects().is_empty());
}

#[test]
fn scheduler_rejects_unknown_and_completed_resume() {
    let kernel = kernel_with_world_relations();
    let program = Arc::new(
        Program::new(
            0,
            [Instruction::Return {
                value: v(Value::nothing()),
            }],
        )
        .unwrap(),
    );
    let mut scheduler = Scheduler::new(kernel);

    assert_eq!(
        scheduler.resume(999).unwrap_err(),
        SchedulerError::UnknownTask(999)
    );
    let (task_id, _) = scheduler.submit(program).unwrap();
    assert_eq!(
        scheduler.resume(task_id).unwrap_err(),
        SchedulerError::TaskAlreadyCompleted(task_id)
    );
}

#[test]
fn direct_program_call_returns_into_caller_register() {
    let kernel = kernel_with_world_relations();
    let callee = Arc::new(Program::new(2, [Instruction::Return { value: r(0) }]).unwrap());
    let caller = Program::new(
        2,
        [
            Instruction::Load {
                dst: reg(0),
                value: int(42),
            },
            Instruction::Call {
                dst: reg(1),
                program: callee,
                args: vec![r(0)],
            },
            Instruction::Return { value: r(1) },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, caller, 100).unwrap(),
        TaskOutcome::Complete {
            value: int(42),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn builtin_call_invokes_registered_host_function() {
    let kernel = kernel_with_world_relations();
    let builtins = BuiltinRegistry::new().with_builtin("emit_first_arg", emit_first_arg);
    let program = Program::new(
        1,
        [
            Instruction::BuiltinCall {
                dst: reg(0),
                name: Symbol::intern("emit_first_arg"),
                args: vec![v(strv("hello"))],
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program_with_builtins(&kernel, restored, builtins).unwrap(),
        TaskOutcome::Complete {
            value: strv("hello"),
            effects: vec![strv("hello")],
            retries: 0,
        }
    );
}

#[test]
fn missing_builtin_call_is_runtime_error() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        1,
        [
            Instruction::BuiltinCall {
                dst: reg(0),
                name: Symbol::intern("missing_builtin"),
                args: vec![],
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap_err(),
        TaskError::Runtime(RuntimeError::UnknownBuiltin {
            name: Symbol::intern("missing_builtin")
        })
    );
}

#[test]
fn program_artifact_round_trips_range_slicing() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        3,
        [
            Instruction::BuildList {
                dst: reg(0),
                items: vec![
                    item(v(int(1))),
                    item(v(int(2))),
                    item(v(int(3))),
                    item(v(int(4))),
                ],
            },
            Instruction::BuildRange {
                dst: reg(1),
                start: v(int(1)),
                end: Some(v(int(2))),
            },
            Instruction::Index {
                dst: reg(2),
                collection: reg(0),
                index: r(1),
            },
            Instruction::Return { value: r(2) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program(&kernel, restored, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::list([int(2), int(3)]),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn program_artifact_round_trips_list_splices() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        3,
        [
            Instruction::BuildList {
                dst: reg(0),
                items: vec![item(v(int(2))), item(v(int(3)))],
            },
            Instruction::BuildList {
                dst: reg(1),
                items: vec![item(v(int(1))), splice(r(0)), item(v(int(4)))],
            },
            Instruction::Return { value: r(1) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program(&kernel, restored, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::list([int(1), int(2), int(3), int(4)]),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn program_artifact_round_trips_error_codes() {
    let kernel = kernel_with_world_relations();
    let code = err("E_NOT_PORTABLE");
    let program = Program::new(
        1,
        [
            Instruction::Load {
                dst: reg(0),
                value: code.clone(),
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program(&kernel, restored, 100).unwrap(),
        TaskOutcome::Complete {
            value: code,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn program_artifact_round_trips_rich_errors() {
    let kernel = kernel_with_world_relations();
    let error = error(
        "E_NOT_PORTABLE",
        Some("That cannot be taken."),
        Some(strv("lamp")),
    );
    let program = Program::new(
        1,
        [
            Instruction::Load {
                dst: reg(0),
                value: error.clone(),
            },
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program(&kernel, restored, 100).unwrap(),
        TaskOutcome::Complete {
            value: error,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn error_field_instruction_extracts_rich_error_parts() {
    let kernel = kernel_with_world_relations();
    let error = error(
        "E_NOT_PORTABLE",
        Some("That cannot be taken."),
        Some(strv("lamp")),
    );
    let program = Program::new(
        5,
        [
            Instruction::Load {
                dst: reg(0),
                value: error,
            },
            Instruction::ErrorField {
                dst: reg(1),
                error: reg(0),
                field: ErrorField::Code,
            },
            Instruction::ErrorField {
                dst: reg(2),
                error: reg(0),
                field: ErrorField::Message,
            },
            Instruction::ErrorField {
                dst: reg(3),
                error: reg(0),
                field: ErrorField::Value,
            },
            Instruction::BuildList {
                dst: reg(4),
                items: vec![item(r(1)), item(r(2)), item(r(3))],
            },
            Instruction::Return { value: r(4) },
        ],
    )
    .unwrap();
    let restored = Program::from_bytes(&program.to_bytes().unwrap()).unwrap();
    assert_eq!(restored, program);

    assert_eq!(
        run_program(&kernel, restored, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::list([
                err("E_NOT_PORTABLE"),
                strv("That cannot be taken."),
                strv("lamp")
            ]),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn raise_without_handler_aborts_with_rich_error() {
    let kernel = kernel_with_world_relations();
    let expected = error(
        "E_NOT_PORTABLE",
        Some("That cannot be taken."),
        Some(strv("lamp")),
    );
    let program = Program::new(
        0,
        [Instruction::Raise {
            error: v(err("E_NOT_PORTABLE")),
            message: Some(v(strv("That cannot be taken."))),
            value: Some(v(strv("lamp"))),
        }],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Aborted {
            error: expected,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn try_catches_raised_error_by_code() {
    let kernel = kernel_with_world_relations();
    let expected = error(
        "E_NOT_PORTABLE",
        Some("That cannot be taken."),
        Some(strv("lamp")),
    );
    let program = Program::new(
        2,
        [
            Instruction::EnterTry {
                catches: vec![crate::CatchHandler {
                    code: Some(err("E_NOT_PORTABLE")),
                    binding: Some(reg(1)),
                    target: 3,
                }],
                finally: None,
                end: 4,
            },
            Instruction::Raise {
                error: v(err("E_NOT_PORTABLE")),
                message: Some(v(strv("That cannot be taken."))),
                value: Some(v(strv("lamp"))),
            },
            Instruction::ExitTry,
            Instruction::Return { value: r(1) },
            Instruction::Return {
                value: v(Value::nothing()),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: expected,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn raise_unwinds_across_activation_to_caller_handler() {
    let kernel = kernel_with_world_relations();
    let callee = Arc::new(
        Program::new(
            0,
            [Instruction::Raise {
                error: v(err("E_NO_EXIT")),
                message: Some(v(strv("No exit."))),
                value: None,
            }],
        )
        .unwrap(),
    );
    let expected = error("E_NO_EXIT", Some("No exit."), None);
    let caller = Program::new(
        2,
        [
            Instruction::EnterTry {
                catches: vec![crate::CatchHandler {
                    code: Some(err("E_NO_EXIT")),
                    binding: Some(reg(1)),
                    target: 3,
                }],
                finally: None,
                end: 4,
            },
            Instruction::Call {
                dst: reg(0),
                program: callee,
                args: vec![],
            },
            Instruction::ExitTry,
            Instruction::Return { value: r(1) },
            Instruction::Return {
                value: v(Value::nothing()),
            },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, caller, 100).unwrap(),
        TaskOutcome::Complete {
            value: expected,
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn finally_runs_on_return() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        1,
        [
            Instruction::EnterTry {
                catches: vec![],
                finally: Some(3),
                end: 5,
            },
            Instruction::Return { value: v(int(7)) },
            Instruction::ExitTry,
            Instruction::Emit {
                value: v(strv("cleanup")),
            },
            Instruction::EndFinally,
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: int(7),
            effects: vec![strv("cleanup")],
            retries: 0,
        }
    );
}

#[test]
fn caught_exception_runs_finally_before_continuing() {
    let kernel = kernel_with_world_relations();
    let program = Program::new(
        1,
        [
            Instruction::EnterTry {
                catches: vec![crate::CatchHandler {
                    code: Some(err("E_NOT_PORTABLE")),
                    binding: None,
                    target: 3,
                }],
                finally: Some(5),
                end: 7,
            },
            Instruction::Raise {
                error: v(err("E_NOT_PORTABLE")),
                message: None,
                value: None,
            },
            Instruction::ExitTry,
            Instruction::Load {
                dst: reg(0),
                value: Value::bool(true),
            },
            Instruction::ExitTry,
            Instruction::Emit {
                value: v(strv("cleanup")),
            },
            Instruction::EndFinally,
            Instruction::Return { value: r(0) },
        ],
    )
    .unwrap();

    assert_eq!(
        run_program(&kernel, program, 100).unwrap(),
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("cleanup")],
            retries: 0,
        }
    );
}

#[test]
fn call_depth_limit_is_enforced() {
    let kernel = kernel_with_world_relations();
    let leaf = Arc::new(
        Program::new(
            0,
            [Instruction::Return {
                value: v(Value::nothing()),
            }],
        )
        .unwrap(),
    );
    let caller = Arc::new(
        Program::new(
            1,
            [
                Instruction::Call {
                    dst: reg(0),
                    program: leaf,
                    args: vec![],
                },
                Instruction::Return { value: r(0) },
            ],
        )
        .unwrap(),
    );
    let mut task = Task::new(
        1,
        &kernel,
        caller,
        Arc::new(ProgramResolver::new()),
        TaskLimits {
            instruction_budget: 100,
            max_retries: 1,
            max_call_depth: 1,
        },
    );

    assert_eq!(
        task.run().unwrap_err(),
        TaskError::Runtime(RuntimeError::MaxCallDepthExceeded { max_depth: 1 })
    );
}

#[test]
fn suspension_inside_callee_resumes_full_activation_stack() {
    let kernel = kernel_with_world_relations();
    let callee = Arc::new(
        Program::new(
            1,
            [
                Instruction::Emit {
                    value: v(strv("in callee")),
                },
                Instruction::Suspend {
                    kind: SuspendKind::TimedMillis(1),
                },
                Instruction::Return { value: r(0) },
            ],
        )
        .unwrap(),
    );
    let caller = Arc::new(
        Program::new(
            2,
            [
                Instruction::Load {
                    dst: reg(0),
                    value: int(7),
                },
                Instruction::Call {
                    dst: reg(1),
                    program: callee,
                    args: vec![r(0)],
                },
                Instruction::Return { value: r(1) },
            ],
        )
        .unwrap(),
    );
    let mut scheduler = Scheduler::new(kernel);

    let (task_id, first) = scheduler.submit(caller).unwrap();
    assert_eq!(
        first,
        TaskOutcome::Suspended {
            kind: SuspendKind::TimedMillis(1),
            effects: vec![strv("in callee")],
            retries: 0,
        }
    );
    assert_eq!(scheduler.suspended(task_id).unwrap().frame_count(), 2);

    assert_eq!(
        scheduler.resume(task_id).unwrap(),
        TaskOutcome::Complete {
            value: int(7),
            effects: vec![],
            retries: 0,
        }
    );
}

#[test]
fn commit_conflict_retries_restore_call_stack() {
    let kernel = kernel_with_world_relations();
    let item = int(200);
    let room = int(300);
    let other = int(500);
    let actor = int(100);

    let mut seed = kernel.begin();
    seed.replace_functional(rel(2), Tuple::from([item.clone(), room]))
        .unwrap();
    seed.commit().unwrap();

    let callee = Arc::new(
        Program::new(
            2,
            [
                Instruction::ReplaceFunctional {
                    relation: rel(2),
                    values: vec![r(0), r(1)],
                },
                Instruction::Emit {
                    value: v(strv("moved")),
                },
                Instruction::Return {
                    value: v(Value::bool(true)),
                },
            ],
        )
        .unwrap(),
    );
    let caller = Arc::new(
        Program::new(
            3,
            [
                Instruction::Load {
                    dst: reg(0),
                    value: item.clone(),
                },
                Instruction::Load {
                    dst: reg(1),
                    value: actor.clone(),
                },
                Instruction::Call {
                    dst: reg(2),
                    program: callee,
                    args: vec![r(0), r(1)],
                },
                Instruction::Return { value: r(2) },
            ],
        )
        .unwrap(),
    );
    let mut task = Task::new(
        1,
        &kernel,
        caller,
        Arc::new(ProgramResolver::new()),
        TaskLimits {
            instruction_budget: 100,
            max_retries: 2,
            max_call_depth: 50,
        },
    );

    let mut concurrent = kernel.begin();
    concurrent
        .replace_functional(rel(2), Tuple::from([item.clone(), other]))
        .unwrap();
    concurrent.commit().unwrap();

    assert_eq!(
        task.run().unwrap(),
        TaskOutcome::Complete {
            value: Value::bool(true),
            effects: vec![strv("moved")],
            retries: 1,
        }
    );
    assert_eq!(
        kernel.snapshot().scan(rel(2), &[Some(item), None]).unwrap(),
        vec![Tuple::from([int(200), actor])]
    );
}
