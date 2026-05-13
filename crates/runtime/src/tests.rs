use crate::{
    Effect, Instruction, Operand, Program, ProgramResolver, Register, RuntimeError, Scheduler,
    SchedulerError, SuspendKind, Task, TaskError, TaskLimits, TaskOutcome,
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
