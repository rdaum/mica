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

use mica_compiler::{CompileContext, MethodRelations, install_methods_from_source};
use mica_relation_kernel::{
    ConflictPolicy, DispatchRelations, RelationId, RelationKernel, RelationMetadata, Tuple,
};
use mica_runtime::{
    BuiltinRegistry, Instruction, Operand, Program, ProgramResolver, Register, RuntimeBinaryOp,
    Task, TaskLimits, TaskOutcome,
};
use mica_var::{Identity, Symbol, Value};
use micromeasure::{
    BenchContext, BenchmarkMainOptions, DiagnosticError, DiagnosticResult, MetricValue, Throughput,
    benchmark_main, black_box,
};
use std::sync::Arc;
use std::time::Duration;

const INTEGER_LOOP_ITERATIONS: u64 = 16_384;
const INTEGER_LOOP_BYTECODES: u64 = (INTEGER_LOOP_ITERATIONS * 3) + 4;
const REPEATED_DISPATCH_ITERATIONS: u64 = 1_024;
const MAX_CALL_DEPTH: usize = 64;

#[derive(Clone, Copy)]
struct Workload {
    executed_bytecodes: u64,
    // Count calls that cross from the VM into the relation workspace. Kernel-internal index
    // probes performed by one host call are intentionally not separate operations here.
    relation_reads: u64,
    relation_writes: u64,
    commit_boundaries: u64,
    kernel_commits: u64,
    dispatch_cache_lookups: u64,
    method_program_cache_lookups: u64,
    vm_resolved_program_lookups: u64,
    program_resolver_lookups: u64,
}

impl Workload {
    const fn task(executed_bytecodes: u64) -> Self {
        Self {
            executed_bytecodes,
            relation_reads: 0,
            relation_writes: 0,
            commit_boundaries: 1,
            kernel_commits: 0,
            dispatch_cache_lookups: 0,
            method_program_cache_lookups: 0,
            vm_resolved_program_lookups: 0,
            program_resolver_lookups: 0,
        }
    }
}

struct TaskBenchContext {
    kernel: RelationKernel,
    program: Arc<Program>,
    resolver: Arc<ProgramResolver>,
    builtins: Arc<BuiltinRegistry>,
    next_task_id: u64,
    workload: Workload,
}

impl TaskBenchContext {
    fn from_program(kernel: RelationKernel, program: Program, workload: Workload) -> Self {
        Self {
            kernel,
            program: Arc::new(program),
            resolver: Arc::new(ProgramResolver::new()),
            builtins: Arc::new(BuiltinRegistry::new()),
            next_task_id: 1,
            workload,
        }
    }

    fn minimal_return() -> Self {
        let program = Program::new(
            0,
            [Instruction::Return {
                value: value(Value::nothing()),
            }],
        )
        .unwrap();
        Self::from_program(RelationKernel::new(), program, Workload::task(1))
    }

    fn integer_loop() -> Self {
        let program = Program::new(
            4,
            [
                Instruction::Load {
                    dst: register(0),
                    value: int(0),
                },
                Instruction::Load {
                    dst: register(1),
                    value: int(1),
                },
                Instruction::Load {
                    dst: register(2),
                    value: int(INTEGER_LOOP_ITERATIONS as i64),
                },
                Instruction::Binary {
                    dst: register(0),
                    op: RuntimeBinaryOp::Add,
                    left: register(0),
                    right: register(1),
                },
                Instruction::Binary {
                    dst: register(3),
                    op: RuntimeBinaryOp::Lt,
                    left: register(0),
                    right: register(2),
                },
                Instruction::Branch {
                    condition: register(3),
                    if_true: 3,
                    if_false: 6,
                },
                Instruction::Return { value: operand(0) },
            ],
        )
        .unwrap();
        Self::from_program(
            RelationKernel::new(),
            program,
            Workload::task(INTEGER_LOOP_BYTECODES),
        )
    }

    fn indexed_relation_read() -> Self {
        let relation = relation_id(1);
        let key = int(7);
        let expected = int(11);
        let kernel = functional_kernel(relation, "Counter");
        let mut seed = kernel.begin();
        seed.replace_functional(relation, Tuple::from([key.clone(), expected.clone()]))
            .unwrap();
        seed.commit().unwrap();

        let program = Program::new(
            1,
            [
                Instruction::ScanValue {
                    dst: register(0),
                    relation,
                    key: value(key),
                },
                Instruction::Return { value: operand(0) },
            ],
        )
        .unwrap();
        let mut workload = Workload::task(2);
        workload.relation_reads = 1;
        Self::from_program(kernel, program, workload)
    }

    fn read_modify_write() -> Self {
        let relation = relation_id(1);
        let key = int(7);
        let kernel = functional_kernel(relation, "Counter");
        let mut seed = kernel.begin();
        seed.replace_functional(relation, Tuple::from([key.clone(), int(0)]))
            .unwrap();
        seed.commit().unwrap();

        let program = Program::new(
            3,
            [
                Instruction::ScanValue {
                    dst: register(0),
                    relation,
                    key: value(key.clone()),
                },
                Instruction::Load {
                    dst: register(2),
                    value: int(1),
                },
                Instruction::Binary {
                    dst: register(1),
                    op: RuntimeBinaryOp::Add,
                    left: register(0),
                    right: register(2),
                },
                Instruction::ReplaceFunctional {
                    relation,
                    values: vec![value(key), operand(1)],
                },
                Instruction::Return { value: operand(1) },
            ],
        )
        .unwrap();
        let mut workload = Workload::task(5);
        workload.relation_reads = 1;
        workload.relation_writes = 1;
        workload.kernel_commits = 1;
        Self::from_program(kernel, program, workload)
    }

    fn warm_positional_dispatch() -> Self {
        let (kernel, relations, receiver, resolver) = Self::positional_dispatch_fixture();
        let program = Arc::new(
            Program::new(
                1,
                [
                    Instruction::PositionalDispatch {
                        dst: register(0),
                        relations: relations.dispatch,
                        program_relation: relations.method_program,
                        program_bytes: relations.program_bytes,
                        selector: value(Value::symbol(Symbol::intern("benchmark"))),
                        args: vec![value(Value::identity(receiver))],
                    },
                    Instruction::Return { value: operand(0) },
                ],
            )
            .unwrap(),
        );
        let mut workload = Workload::task(4);
        workload.dispatch_cache_lookups = 1;
        workload.method_program_cache_lookups = 1;
        workload.vm_resolved_program_lookups = 1;
        workload.program_resolver_lookups = 1;
        Self::warm_dispatch_context(kernel, program, resolver, workload)
    }

    fn repeated_positional_dispatch() -> Self {
        let (kernel, relations, receiver, resolver) = Self::positional_dispatch_fixture();
        let program = Arc::new(
            Program::new(
                5,
                [
                    Instruction::Load {
                        dst: register(0),
                        value: int(0),
                    },
                    Instruction::Load {
                        dst: register(1),
                        value: int(1),
                    },
                    Instruction::Load {
                        dst: register(2),
                        value: int(REPEATED_DISPATCH_ITERATIONS as i64),
                    },
                    Instruction::PositionalDispatch {
                        dst: register(3),
                        relations: relations.dispatch,
                        program_relation: relations.method_program,
                        program_bytes: relations.program_bytes,
                        selector: value(Value::symbol(Symbol::intern("benchmark"))),
                        args: vec![value(Value::identity(receiver))],
                    },
                    Instruction::Binary {
                        dst: register(0),
                        op: RuntimeBinaryOp::Add,
                        left: register(0),
                        right: register(1),
                    },
                    Instruction::Binary {
                        dst: register(4),
                        op: RuntimeBinaryOp::Lt,
                        left: register(0),
                        right: register(2),
                    },
                    Instruction::Branch {
                        condition: register(4),
                        if_true: 3,
                        if_false: 7,
                    },
                    Instruction::Return { value: operand(3) },
                ],
            )
            .unwrap(),
        );
        let mut workload = Workload::task((REPEATED_DISPATCH_ITERATIONS * 6) + 4);
        workload.dispatch_cache_lookups = REPEATED_DISPATCH_ITERATIONS;
        workload.method_program_cache_lookups = REPEATED_DISPATCH_ITERATIONS;
        workload.vm_resolved_program_lookups = REPEATED_DISPATCH_ITERATIONS;
        workload.program_resolver_lookups = 1;
        Self::warm_dispatch_context(kernel, program, resolver, workload)
    }

    fn positional_dispatch_fixture() -> (
        RelationKernel,
        MethodRelations,
        Identity,
        Arc<ProgramResolver>,
    ) {
        let relations = method_relations();
        let method = identity(100);
        let method_program = identity(101);
        let prototype = identity(200);
        let receiver = identity(300);
        let kernel = RelationKernel::new();
        create_method_relations(&kernel, relations);

        let context = CompileContext::new()
            .with_method_relations(relations)
            .with_identity("benchmark_method", method)
            .with_program_identity("benchmark_method", method_program)
            .with_identity("benchmark_prototype", prototype);
        let mut install = kernel.begin();
        let installation = install_methods_from_source(
            "method #benchmark_method :benchmark\n\
               roles receiver @ #benchmark_prototype\n\
             do\n\
               return 7\n\
             end",
            &context,
            &mut install,
        )
        .unwrap();
        install
            .assert(
                relations.dispatch.delegates,
                Tuple::from([
                    Value::identity(receiver),
                    Value::identity(prototype),
                    int(0),
                ]),
            )
            .unwrap();
        install.commit().unwrap();

        let installed = installation.methods.into_iter().next().unwrap();
        let method_bytecodes = installed.compiled.program.instructions();
        assert!(matches!(method_bytecodes[0], Instruction::Load { .. }));
        assert!(matches!(method_bytecodes[1], Instruction::Return { .. }));
        let resolver = Arc::new(
            ProgramResolver::new().with_program(installed.program, installed.compiled.program),
        );
        (kernel, relations, receiver, resolver)
    }

    fn warm_dispatch_context(
        kernel: RelationKernel,
        program: Arc<Program>,
        resolver: Arc<ProgramResolver>,
        workload: Workload,
    ) -> Self {
        let mut context = Self {
            kernel,
            program,
            resolver,
            builtins: Arc::new(BuiltinRegistry::new()),
            next_task_id: 1,
            workload,
        };

        // Populate the snapshot dispatch caches before micromeasure opens its timing window.
        context.run_one();
        context
    }

    fn run_one(&mut self) {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        let mut task = Task::new_with_builtins(
            task_id,
            &self.kernel,
            Arc::clone(&self.program),
            Arc::clone(&self.resolver),
            Arc::clone(&self.builtins),
            TaskLimits {
                instruction_budget: INTEGER_LOOP_BYTECODES as usize + 1,
                max_retries: 0,
                max_call_depth: MAX_CALL_DEPTH,
            },
        );
        let outcome = task.run().unwrap();
        debug_assert!(matches!(outcome, TaskOutcome::Complete { .. }));
        black_box(outcome);
    }
}

impl BenchContext for TaskBenchContext {
    fn prepare(_num_chunks: usize) -> Self {
        Self::minimal_return()
    }
}

fn run_tasks(context: &mut TaskBenchContext, chunk_size: usize, _chunk_num: usize) {
    for _ in 0..chunk_size {
        context.run_one();
    }
}

fn workload_diagnostics(
    context: &mut TaskBenchContext,
    _chunk_size: usize,
    _chunk_num: usize,
) -> Result<DiagnosticResult, DiagnosticError> {
    let workload = context.workload;
    Ok(DiagnosticResult::new("work per task")
        .push_metric(MetricValue::integer(
            "executed_bytecodes_per_task",
            workload.executed_bytecodes as i64,
            "bytecodes",
        ))
        .push_metric(MetricValue::integer(
            "relation_operations_per_task",
            (workload.relation_reads + workload.relation_writes) as i64,
            "operations",
        ))
        .push_metric(MetricValue::integer(
            "relation_reads_per_task",
            workload.relation_reads as i64,
            "reads",
        ))
        .push_metric(MetricValue::integer(
            "relation_writes_per_task",
            workload.relation_writes as i64,
            "writes",
        ))
        .push_metric(MetricValue::integer(
            "commit_boundaries_per_task",
            workload.commit_boundaries as i64,
            "boundaries",
        ))
        .push_metric(MetricValue::integer(
            "kernel_commits_per_task",
            workload.kernel_commits as i64,
            "commits",
        ))
        .push_metric(MetricValue::integer(
            "dispatch_cache_lookups_per_task",
            workload.dispatch_cache_lookups as i64,
            "lookups",
        ))
        .push_metric(MetricValue::integer(
            "method_program_cache_lookups_per_task",
            workload.method_program_cache_lookups as i64,
            "lookups",
        ))
        .push_metric(MetricValue::integer(
            "vm_resolved_program_lookups_per_task",
            workload.vm_resolved_program_lookups as i64,
            "lookups",
        ))
        .push_metric(MetricValue::integer(
            "program_resolver_lookups_per_task",
            workload.program_resolver_lookups as i64,
            "lookups",
        )))
}

fn functional_kernel(relation: RelationId, name: &str) -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(relation, Symbol::intern(name), 2)
                .with_index([0])
                .with_conflict_policy(ConflictPolicy::Functional {
                    key_positions: vec![0],
                }),
        )
        .unwrap();
    kernel
}

fn method_relations() -> MethodRelations {
    MethodRelations {
        dispatch: DispatchRelations {
            method_selector: relation_id(40),
            param: relation_id(41),
            delegates: relation_id(42),
        },
        method_program: relation_id(43),
        program_bytes: relation_id(44),
    }
}

fn create_method_relations(kernel: &RelationKernel, relations: MethodRelations) {
    kernel
        .create_relation(
            RelationMetadata::new(
                relations.dispatch.method_selector,
                Symbol::intern("MethodSelector"),
                2,
            )
            .with_index([1, 0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(relations.dispatch.param, Symbol::intern("Param"), 4)
                .with_index([0, 1]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(relations.dispatch.delegates, Symbol::intern("Delegates"), 3)
                .with_index([0, 2, 1]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(relations.method_program, Symbol::intern("MethodProgram"), 2)
                .with_index([0]),
        )
        .unwrap();
    kernel
        .create_relation(
            RelationMetadata::new(relations.program_bytes, Symbol::intern("ProgramBytes"), 2)
                .with_index([0]),
        )
        .unwrap();
}

fn relation_id(raw: u64) -> RelationId {
    identity(raw)
}

fn identity(raw: u64) -> Identity {
    Identity::new(raw).unwrap()
}

fn register(index: u16) -> Register {
    Register(index)
}

fn operand(index: u16) -> Operand {
    Operand::Register(register(index))
}

fn value(value: Value) -> Operand {
    Operand::Value(value)
}

fn int(value: i64) -> Value {
    Value::int(value).unwrap()
}

benchmark_main!(
    BenchmarkMainOptions {
        filter_help: Some(
            "all, minimal, integer, read, write, dispatch, or a benchmark name substring"
                .to_owned(),
        ),
        runtime: micromeasure::BenchmarkRuntimeOptions {
            warm_up_duration: Duration::from_millis(100),
            benchmark_duration: Duration::from_secs(1),
            min_samples: 10,
            max_samples: 30,
        },
        ..Default::default()
    },
    |runner| {
        runner.group::<TaskBenchContext>("task", |group| {
            let minimal = || TaskBenchContext::minimal_return();
            group
                .throughput(Throughput::ops())
                .factory(&minimal)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_minimal_return", run_tasks);

            let integer = || TaskBenchContext::integer_loop();
            group
                .throughput(Throughput::ops())
                .factory(&integer)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_integer_loop", run_tasks);

            let read = || TaskBenchContext::indexed_relation_read();
            group
                .throughput(Throughput::ops())
                .factory(&read)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_indexed_relation_read", run_tasks);

            let write = || TaskBenchContext::read_modify_write();
            group
                .throughput(Throughput::ops())
                .factory(&write)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_read_modify_write_commit", run_tasks);

            let dispatch = || TaskBenchContext::warm_positional_dispatch();
            group
                .throughput(Throughput::ops())
                .factory(&dispatch)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_warm_positional_dispatch", run_tasks);

            let repeated_dispatch = || TaskBenchContext::repeated_positional_dispatch();
            group
                .throughput(Throughput::ops())
                .factory(&repeated_dispatch)
                .diagnostic_pass(workload_diagnostics)
                .bench("task_repeated_positional_dispatch", run_tasks);
        });
    }
);
