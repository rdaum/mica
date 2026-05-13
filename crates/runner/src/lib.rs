use mica_compiler::{CompileContext, SourceTaskError, submit_source_task};
use mica_relation_kernel::{ConflictPolicy, KernelError, RelationKernel, RelationMetadata, Tuple};
use mica_runtime::{
    Builtin, BuiltinContext, BuiltinRegistry, RuntimeError, Scheduler, TaskOutcome,
};
use mica_var::{Identity, Symbol, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const GENERATED_RELATION_ID_START: u64 = 0x00f0_0000_0000_0000;
const GENERATED_IDENTITY_ID_START: u64 = 0x00e0_0000_0000_0000;
const NAMED_IDENTITY_RELATION_ID: u64 = 0x00df_ffff_ffff_ffff;

pub struct SourceRunner {
    context: CompileContext,
    scheduler: Scheduler,
}

impl SourceRunner {
    pub fn new_empty() -> Self {
        let mut runner = Self {
            context: CompileContext::new(),
            scheduler: Scheduler::new(bootstrap_kernel())
                .with_builtins(Arc::new(default_builtins())),
        };
        runner.refresh_context_from_catalog();
        runner
    }

    pub fn run_source(&mut self, source: &str) -> Result<RunReport, SourceTaskError> {
        let submitted = submit_source_task(source, &self.context, &mut self.scheduler)?;
        self.refresh_context_from_catalog();
        Ok(RunReport {
            task_id: submitted.task_id,
            outcome: submitted.outcome,
        })
    }

    fn refresh_context_from_catalog(&mut self) {
        let snapshot = self.scheduler.kernel().snapshot();
        for metadata in snapshot.relation_metadata() {
            if let Some(name) = metadata.name().name() {
                self.context.define_relation(name, metadata.id());
            }
        }
        for tuple in snapshot
            .scan(named_identity_relation(), &[None, None])
            .unwrap_or_default()
        {
            if let [name, identity] = tuple.values()
                && let (Some(name), Some(identity)) = (name.as_symbol(), identity.as_identity())
                && let Some(name) = name.name()
            {
                self.context.define_identity(name, identity);
            }
        }
    }
}

fn bootstrap_kernel() -> RelationKernel {
    let kernel = RelationKernel::new();
    kernel
        .create_relation(
            RelationMetadata::new(
                named_identity_relation(),
                Symbol::intern("NamedIdentity"),
                2,
            )
            .with_conflict_policy(ConflictPolicy::Functional {
                key_positions: vec![0],
            }),
        )
        .unwrap();
    kernel
}

fn default_builtins() -> BuiltinRegistry {
    BuiltinRegistry::new()
        .with_builtin("emit", emit_builtin)
        .with_builtin("make_relation", MakeRelationBuiltin::new())
        .with_builtin("make_identity", MakeIdentityBuiltin::new())
}

fn emit_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    let value = args.first().cloned().unwrap_or_else(Value::nothing);
    context.emit(value.clone());
    Ok(value)
}

struct MakeRelationBuiltin {
    next_relation_id: AtomicU64,
}

struct MakeIdentityBuiltin {
    next_identity_id: AtomicU64,
}

impl MakeRelationBuiltin {
    fn new() -> Self {
        Self {
            next_relation_id: AtomicU64::new(GENERATED_RELATION_ID_START),
        }
    }
}

impl MakeIdentityBuiltin {
    fn new() -> Self {
        Self {
            next_identity_id: AtomicU64::new(GENERATED_IDENTITY_ID_START),
        }
    }
}

impl Builtin for MakeRelationBuiltin {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        let name = builtin_symbol_arg("make_relation", args, 0)?;
        let arity = builtin_arity_arg("make_relation", args, 1)?;
        if args.len() != 2 {
            return Err(invalid_builtin_call(
                "make_relation",
                "expected make_relation(:Name, arity)",
            ));
        }

        if let Some((relation, existing_arity)) = relation_named(context.kernel(), name) {
            if existing_arity == arity {
                return Ok(Value::identity(relation));
            }
            return Err(invalid_builtin_call(
                "make_relation",
                "relation name already exists with different arity",
            ));
        }

        loop {
            let Some(relation) =
                Identity::new(self.next_relation_id.fetch_add(1, Ordering::Relaxed))
            else {
                return Err(invalid_builtin_call(
                    "make_relation",
                    "generated relation identity exhausted",
                ));
            };
            let metadata = RelationMetadata::new(relation, name, arity);
            match context.kernel().create_relation(metadata) {
                Ok(_) => return Ok(Value::identity(relation)),
                Err(KernelError::RelationAlreadyExists(_)) => continue,
                Err(error) => return Err(RuntimeError::Kernel(error)),
            }
        }
    }
}

impl Builtin for MakeIdentityBuiltin {
    fn call(
        &self,
        context: &mut BuiltinContext<'_, '_>,
        args: &[Value],
    ) -> Result<Value, RuntimeError> {
        if args.len() != 1 {
            return Err(invalid_builtin_call(
                "make_identity",
                "expected make_identity(:name)",
            ));
        }
        let name = builtin_symbol_arg("make_identity", args, 0)?;

        if let Some(identity) = identity_named(context, name)? {
            return Ok(Value::identity(identity));
        }

        let Some(identity) = Identity::new(self.next_identity_id.fetch_add(1, Ordering::Relaxed))
        else {
            return Err(invalid_builtin_call(
                "make_identity",
                "generated identity exhausted",
            ));
        };
        context.tx().replace_functional(
            named_identity_relation(),
            Tuple::from([Value::symbol(name), Value::identity(identity)]),
        )?;
        Ok(Value::identity(identity))
    }
}

fn relation_named(kernel: &RelationKernel, name: Symbol) -> Option<(Identity, u16)> {
    let snapshot = kernel.snapshot();
    snapshot
        .relation_metadata()
        .find(|metadata| metadata.name() == name)
        .map(|metadata| (metadata.id(), metadata.arity()))
}

fn identity_named(
    context: &mut BuiltinContext<'_, '_>,
    name: Symbol,
) -> Result<Option<Identity>, RuntimeError> {
    let tuples = context.tx().scan(
        named_identity_relation(),
        &[Some(Value::symbol(name)), None],
    )?;
    Ok(tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity))
}

fn named_identity_relation() -> Identity {
    Identity::new(NAMED_IDENTITY_RELATION_ID).unwrap()
}

fn builtin_symbol_arg(name: &str, args: &[Value], index: usize) -> Result<Symbol, RuntimeError> {
    args.get(index)
        .and_then(Value::as_symbol)
        .ok_or_else(|| invalid_builtin_call(name, "expected symbol argument"))
}

fn builtin_arity_arg(name: &str, args: &[Value], index: usize) -> Result<u16, RuntimeError> {
    let Some(arity) = args.get(index).and_then(Value::as_int) else {
        return Err(invalid_builtin_call(name, "expected integer arity"));
    };
    u16::try_from(arity).map_err(|_| invalid_builtin_call(name, "arity must fit in u16"))
}

fn invalid_builtin_call(name: &str, message: &str) -> RuntimeError {
    RuntimeError::InvalidBuiltinCall {
        name: Symbol::intern(name),
        message: message.to_owned(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunReport {
    pub task_id: u64,
    pub outcome: TaskOutcome,
}

impl RunReport {
    pub fn render(&self) -> String {
        match &self.outcome {
            TaskOutcome::Complete {
                value,
                effects,
                retries,
            } => render_finished("complete", self.task_id, value, effects, *retries),
            TaskOutcome::Aborted {
                error,
                effects,
                retries,
            } => render_finished("aborted", self.task_id, error, effects, *retries),
            TaskOutcome::Suspended {
                kind,
                effects,
                retries,
            } => {
                let mut out = format!(
                    "task {} suspended: {:?} (retries: {})",
                    self.task_id, kind, retries
                );
                render_effects(&mut out, effects);
                out
            }
        }
    }
}

fn render_finished(
    label: &str,
    task_id: u64,
    value: &Value,
    effects: &[Value],
    retries: u8,
) -> String {
    let mut out = format!("task {task_id} {label}: {value:?} (retries: {retries})");
    render_effects(&mut out, effects);
    out
}

fn render_effects(out: &mut String, effects: &[Value]) {
    for effect in effects {
        out.push_str("\neffect: ");
        out.push_str(&format!("{effect:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::SourceRunner;
    use mica_runtime::TaskOutcome;
    use mica_var::{Symbol, Value};

    #[test]
    fn runner_executes_source_against_empty_kernel() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return 1 + 2").unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::int(3).unwrap()
        ));
    }

    #[test]
    fn runner_installs_default_emit_builtin() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return emit(\"hello\")").unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Complete { value, effects, .. }
                if value == Value::string("hello") && effects == vec![Value::string("hello")]
        ));
    }

    #[test]
    fn runner_aborts_on_divide_by_zero_before_builtin_effect() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return emit(1 / 0)").unwrap();

        assert!(matches!(
            report.outcome,
            TaskOutcome::Aborted { error, effects, .. }
                if error.error_code_symbol() == Some(Symbol::intern("E_DIV"))
                    && effects.is_empty()
        ));
    }

    #[test]
    fn runner_make_relation_refreshes_compile_context() {
        let mut runner = SourceRunner::new_empty();
        let made = runner.run_source("return make_relation(:Hog, 1)").unwrap();
        let relation = match made.outcome {
            TaskOutcome::Complete { value, .. } => value.as_identity().unwrap(),
            other => panic!("unexpected make_relation outcome: {other:?}"),
        };

        let asserted = runner.run_source("assert Hog(1)\nreturn true").unwrap();

        assert!(matches!(
            asserted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        assert_eq!(
            runner
                .scheduler
                .kernel()
                .snapshot()
                .scan(relation, &[Some(Value::int(1).unwrap())])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn runner_make_relation_is_idempotent_for_matching_arity() {
        let mut runner = SourceRunner::new_empty();
        let first = runner.run_source("return make_relation(:Hog, 1)").unwrap();
        let second = runner.run_source("return make_relation(:Hog, 1)").unwrap();

        assert!(matches!(
            (first.outcome, second.outcome),
            (
                TaskOutcome::Complete { value: first, .. },
                TaskOutcome::Complete { value: second, .. }
            ) if first == second
        ));
    }

    #[test]
    fn runner_make_identity_refreshes_compile_context() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:Object, 1)").unwrap();
        let made = runner.run_source("return make_identity(:root)").unwrap();
        let root = match made.outcome {
            TaskOutcome::Complete { value, .. } => value.as_identity().unwrap(),
            other => panic!("unexpected make_identity outcome: {other:?}"),
        };

        let asserted = runner
            .run_source("assert Object($root)\nreturn true")
            .unwrap();

        assert!(matches!(
            asserted.outcome,
            TaskOutcome::Complete { value, .. } if value == Value::bool(true)
        ));
        assert_eq!(
            runner
                .scheduler
                .kernel()
                .snapshot()
                .scan(
                    runner.context.relation("Object").unwrap(),
                    &[Some(Value::identity(root))]
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn runner_make_identity_is_idempotent_for_matching_name() {
        let mut runner = SourceRunner::new_empty();
        let first = runner.run_source("return make_identity(:root)").unwrap();
        let second = runner.run_source("return make_identity(:root)").unwrap();

        assert!(matches!(
            (first.outcome, second.outcome),
            (
                TaskOutcome::Complete { value: first, .. },
                TaskOutcome::Complete { value: second, .. }
            ) if first == second
        ));
    }

    #[test]
    fn report_renders_task_outcome() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return true").unwrap();

        assert_eq!(report.render(), "task 1 complete: true (retries: 0)");
    }
}
