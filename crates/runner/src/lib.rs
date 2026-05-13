use mica_compiler::{
    CompileContext, CompileError, HirItem, MethodInstallation, MethodRelations, SourceTaskError,
    install_methods, install_rules_from_source, parse, parse_semantic, submit_source_task,
};
use mica_relation_kernel::{
    ConflictPolicy, DispatchRelations, KernelError, RelationKernel, RelationMetadata, Tuple,
};
use mica_runtime::{
    Builtin, BuiltinContext, BuiltinRegistry, RuntimeError, Scheduler, TaskOutcome,
};
use mica_var::{Identity, Symbol, Value, ValueKind};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const GENERATED_RELATION_ID_START: u64 = 0x00f0_0000_0000_0000;
const GENERATED_IDENTITY_ID_START: u64 = 0x00e0_0000_0000_0000;
const GENERATED_METHOD_ID_START: u64 = 0x00d1_0000_0000_0000;
const NAMED_IDENTITY_RELATION_ID: u64 = 0x00df_ffff_ffff_ffff;
const METHOD_SELECTOR_RELATION_ID: u64 = 0x00df_ffff_ffff_fffe;
const PARAM_RELATION_ID: u64 = 0x00df_ffff_ffff_fffd;
const DELEGATES_RELATION_ID: u64 = 0x00df_ffff_ffff_fffc;
const METHOD_PROGRAM_RELATION_ID: u64 = 0x00df_ffff_ffff_fffb;
const PROGRAM_BYTES_RELATION_ID: u64 = 0x00df_ffff_ffff_fffa;

pub struct SourceRunner {
    context: CompileContext,
    scheduler: Scheduler,
    next_method_identity_id: u64,
}

impl SourceRunner {
    pub fn new_empty() -> Self {
        let mut runner = Self {
            context: CompileContext::new().with_method_relations(method_relations()),
            scheduler: Scheduler::new(bootstrap_kernel())
                .with_builtins(Arc::new(default_builtins())),
            next_method_identity_id: GENERATED_METHOD_ID_START,
        };
        runner.refresh_context_from_catalog();
        runner
    }

    pub fn run_source(&mut self, source: &str) -> Result<RunReport, SourceTaskError> {
        if let Some(installation) = self.install_methods_from_source(source)? {
            let value = installed_method_value(&installation);
            let (task_id, outcome) = self.scheduler.complete_immediate(value);
            self.refresh_context_from_catalog();
            return Ok(RunReport {
                task_id,
                outcome,
                identity_names: self.identity_names(),
                relation_names: self.relation_names(),
            });
        }

        if let Some(installation) =
            install_rules_from_source(source, &self.context, self.scheduler.kernel())?
        {
            let value = installed_rule_value(&installation.rules);
            let (task_id, outcome) = self.scheduler.complete_immediate(value);
            self.refresh_context_from_catalog();
            return Ok(RunReport {
                task_id,
                outcome,
                identity_names: self.identity_names(),
                relation_names: self.relation_names(),
            });
        }
        let submitted = submit_source_task(source, &self.context, &mut self.scheduler)?;
        self.refresh_context_from_catalog();
        Ok(RunReport {
            task_id: submitted.task_id,
            outcome: submitted.outcome,
            identity_names: self.identity_names(),
            relation_names: self.relation_names(),
        })
    }

    pub fn run_filein(&mut self, source: &str) -> Result<Vec<RunReport>, SourceTaskError> {
        let mut reports = Vec::new();
        let mut buffer = String::new();

        for line in source.lines() {
            if line.trim().is_empty() && buffer.trim().is_empty() {
                continue;
            }
            buffer.push_str(line);
            buffer.push('\n');
            if parse(&buffer).errors.is_empty() {
                reports.push(self.run_source(&buffer)?);
                buffer.clear();
            }
        }

        if !buffer.trim().is_empty() {
            reports.push(self.run_source(&buffer)?);
        }
        Ok(reports)
    }

    fn install_methods_from_source(
        &mut self,
        source: &str,
    ) -> Result<Option<MethodInstallation>, SourceTaskError> {
        let semantic = parse_semantic(source);
        if !semantic
            .hir
            .items
            .iter()
            .any(|item| matches!(item, HirItem::Method { .. }))
        {
            return Ok(None);
        }

        if !semantic.parse_errors.is_empty() {
            return Err(CompileError::ParseErrors {
                count: semantic.parse_errors.len(),
            }
            .into());
        }
        if let Some(diagnostic) = semantic.diagnostics.first() {
            return Err(CompileError::SemanticDiagnostic {
                diagnostic: diagnostic.clone(),
            }
            .into());
        }

        if let Some(item) = semantic
            .hir
            .items
            .iter()
            .find(|item| !matches!(item, HirItem::Method { .. }))
        {
            return Err(CompileError::Unsupported {
                node: item_id(item),
                span: semantic.span(item_id(item)).cloned(),
                message: "method definitions cannot be mixed with executable task code yet"
                    .to_owned(),
            }
            .into());
        }

        let mut install_context = self.context.clone();
        let mut next_method_identity_id = self.next_method_identity_id;
        let mut install_tx = self.scheduler.kernel().begin();
        for item in &semantic.hir.items {
            let HirItem::Method { identity, .. } = item else {
                continue;
            };
            let identity_name = identity.as_ref().ok_or_else(|| CompileError::Unsupported {
                node: item_id(item),
                span: semantic.span(item_id(item)).cloned(),
                message: "method installation requires an explicit identity".to_owned(),
            })?;
            let method_id = ensure_named_identity(
                &mut install_tx,
                identity_name,
                &mut next_method_identity_id,
            )?;
            let program_name = format!("{identity_name}_program");
            let program_id = ensure_named_identity(
                &mut install_tx,
                &program_name,
                &mut next_method_identity_id,
            )?;
            install_context.define_identity(identity_name, method_id);
            install_context.define_identity(&program_name, program_id);
            install_context.define_program_identity(identity_name, program_id);
        }

        let installation = install_methods(semantic, &install_context, &mut install_tx)?;
        install_tx.commit().map_err(CompileError::from)?;
        self.context = install_context;
        self.next_method_identity_id = next_method_identity_id;
        Ok(Some(installation))
    }

    fn refresh_context_from_catalog(&mut self) {
        let snapshot = self.scheduler.kernel().snapshot();
        for metadata in snapshot.relation_metadata() {
            if let Some(name) = metadata.name().name() {
                self.context.define_relation(name, metadata.id());
            }
        }
        for (identity, name) in self.identity_names() {
            self.context.define_identity(name, identity);
        }
        for (index, rule) in snapshot.rules().iter().enumerate() {
            self.context
                .define_identity(format!("rule{}", index + 1), rule.id());
        }
    }

    fn identity_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.scheduler.kernel().snapshot();
        snapshot
            .scan(named_identity_relation(), &[None, None])
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tuple| {
                let [name, identity] = tuple.values() else {
                    return None;
                };
                let name = name.as_symbol()?.name()?.to_owned();
                let identity = identity.as_identity()?;
                Some((identity, name))
            })
            .chain(
                snapshot
                    .rules()
                    .iter()
                    .enumerate()
                    .map(|(index, rule)| (rule.id(), format!("rule{}", index + 1))),
            )
            .collect()
    }

    fn relation_names(&self) -> BTreeMap<Identity, String> {
        let snapshot = self.scheduler.kernel().snapshot();
        snapshot
            .relation_metadata()
            .filter_map(|metadata| Some((metadata.id(), metadata.name().name()?.to_owned())))
            .collect()
    }
}

fn installed_rule_value(rules: &[mica_relation_kernel::RuleDefinition]) -> Value {
    match rules {
        [rule] => Value::identity(rule.id()),
        _ => Value::list(
            rules
                .iter()
                .map(|rule| Value::identity(rule.id()))
                .collect::<Vec<_>>(),
        ),
    }
}

fn installed_method_value(installation: &MethodInstallation) -> Value {
    match installation.methods.as_slice() {
        [method] => method.method.clone(),
        methods => Value::list(
            methods
                .iter()
                .map(|method| method.method.clone())
                .collect::<Vec<_>>(),
        ),
    }
}

fn ensure_named_identity(
    tx: &mut mica_relation_kernel::Transaction<'_>,
    name: &str,
    next_identity_id: &mut u64,
) -> Result<Identity, CompileError> {
    let symbol = Symbol::intern(name);
    let tuples = tx.scan(
        named_identity_relation(),
        &[Some(Value::symbol(symbol)), None],
    )?;
    if let Some(identity) = tuples
        .first()
        .and_then(|tuple| tuple.values().get(1))
        .and_then(Value::as_identity)
    {
        return Ok(identity);
    }

    let identity = loop {
        let Some(identity) = Identity::new(*next_identity_id) else {
            return Err(CompileError::Unsupported {
                node: mica_compiler::NodeId(0),
                span: None,
                message: "generated method identity exhausted".to_owned(),
            });
        };
        *next_identity_id += 1;
        if tx
            .scan(
                named_identity_relation(),
                &[None, Some(Value::identity(identity))],
            )?
            .is_empty()
        {
            break identity;
        }
    };
    tx.assert(
        named_identity_relation(),
        Tuple::from([Value::symbol(symbol), Value::identity(identity)]),
    )?;
    Ok(identity)
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
    for metadata in method_relation_metadata() {
        kernel.create_relation(metadata).unwrap();
    }
    kernel
}

fn method_relations() -> MethodRelations {
    MethodRelations {
        dispatch: DispatchRelations {
            method_selector: method_selector_relation(),
            param: param_relation(),
            delegates: delegates_relation(),
        },
        method_program: method_program_relation(),
        program_bytes: program_bytes_relation(),
    }
}

fn method_relation_metadata() -> [RelationMetadata; 5] {
    [
        RelationMetadata::new(
            method_selector_relation(),
            Symbol::intern("MethodSelector"),
            2,
        )
        .with_index([1, 0]),
        RelationMetadata::new(param_relation(), Symbol::intern("Param"), 3).with_index([0, 1]),
        RelationMetadata::new(delegates_relation(), Symbol::intern("Delegates"), 3)
            .with_index([0, 2, 1]),
        RelationMetadata::new(
            method_program_relation(),
            Symbol::intern("MethodProgram"),
            2,
        ),
        RelationMetadata::new(program_bytes_relation(), Symbol::intern("ProgramBytes"), 2),
    ]
}

fn default_builtins() -> BuiltinRegistry {
    BuiltinRegistry::new()
        .with_builtin("emit", emit_builtin)
        .with_builtin("make_relation", MakeRelationBuiltin::new())
        .with_builtin("make_identity", MakeIdentityBuiltin::new())
        .with_builtin("rules", rules_builtin)
        .with_builtin("describe_rule", describe_rule_builtin)
        .with_builtin("disable_rule", disable_rule_builtin)
        .with_builtin("fileout_rules", fileout_rules_builtin)
}

fn emit_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    let value = args.first().cloned().unwrap_or_else(Value::nothing);
    context.emit(value.clone());
    Ok(value)
}

fn rules_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call("rules", "expected rules(:Relation)"));
    }
    let name = builtin_symbol_arg("rules", args, 0)?;
    let Some((relation, _)) = relation_named(context.kernel(), name) else {
        return Ok(Value::list([]));
    };
    let rules = context
        .kernel()
        .snapshot()
        .rules()
        .iter()
        .filter(|rule| rule.active() && rule.rule().head_relation() == relation)
        .map(|rule| Value::identity(rule.id()))
        .collect::<Vec<_>>();
    Ok(Value::list(rules))
}

fn describe_rule_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "describe_rule",
            "expected describe_rule($rule)",
        ));
    }
    let rule_id = builtin_identity_arg("describe_rule", args, 0)?;
    let source = context
        .kernel()
        .snapshot()
        .rules()
        .iter()
        .find(|rule| rule.id() == rule_id)
        .map(|rule| Value::string(rule.source()))
        .unwrap_or_else(Value::nothing);
    Ok(source)
}

fn fileout_rules_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() > 1 {
        return Err(invalid_builtin_call(
            "fileout_rules",
            "expected fileout_rules() or fileout_rules(:Relation)",
        ));
    }
    let relation = if args.is_empty() {
        None
    } else {
        let name = builtin_symbol_arg("fileout_rules", args, 0)?;
        relation_named(context.kernel(), name).map(|(relation, _)| relation)
    };
    let snapshot = context.kernel().snapshot();
    let sources = snapshot
        .rules()
        .iter()
        .filter(|rule| rule.active())
        .filter(|rule| {
            relation
                .map(|relation| relation == rule.rule().head_relation())
                .unwrap_or(true)
        })
        .map(|rule| rule.source().trim().to_owned())
        .collect::<Vec<_>>();
    Ok(Value::string(sources.join("\n\n")))
}

fn disable_rule_builtin(
    context: &mut BuiltinContext<'_, '_>,
    args: &[Value],
) -> Result<Value, RuntimeError> {
    if args.len() != 1 {
        return Err(invalid_builtin_call(
            "disable_rule",
            "expected disable_rule($rule)",
        ));
    }
    let rule_id = builtin_identity_arg("disable_rule", args, 0)?;
    context.kernel().disable_rule(rule_id)?;
    Ok(Value::nothing())
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

fn method_selector_relation() -> Identity {
    Identity::new(METHOD_SELECTOR_RELATION_ID).unwrap()
}

fn param_relation() -> Identity {
    Identity::new(PARAM_RELATION_ID).unwrap()
}

fn delegates_relation() -> Identity {
    Identity::new(DELEGATES_RELATION_ID).unwrap()
}

fn method_program_relation() -> Identity {
    Identity::new(METHOD_PROGRAM_RELATION_ID).unwrap()
}

fn program_bytes_relation() -> Identity {
    Identity::new(PROGRAM_BYTES_RELATION_ID).unwrap()
}

fn item_id(item: &HirItem) -> mica_compiler::NodeId {
    match item {
        HirItem::Expr { id, .. }
        | HirItem::RelationRule { id, .. }
        | HirItem::Object { id, .. }
        | HirItem::Method { id, .. } => *id,
    }
}

fn builtin_symbol_arg(name: &str, args: &[Value], index: usize) -> Result<Symbol, RuntimeError> {
    args.get(index)
        .and_then(Value::as_symbol)
        .ok_or_else(|| invalid_builtin_call(name, "expected symbol argument"))
}

fn builtin_identity_arg(
    name: &str,
    args: &[Value],
    index: usize,
) -> Result<Identity, RuntimeError> {
    args.get(index)
        .and_then(Value::as_identity)
        .ok_or_else(|| invalid_builtin_call(name, "expected identity argument"))
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
    identity_names: BTreeMap<Identity, String>,
    relation_names: BTreeMap<Identity, String>,
}

impl RunReport {
    pub fn render(&self) -> String {
        match &self.outcome {
            TaskOutcome::Complete {
                value,
                effects,
                retries,
            } => render_finished(
                "complete",
                self.task_id,
                value,
                effects,
                *retries,
                &self.identity_names,
                &self.relation_names,
            ),
            TaskOutcome::Aborted {
                error,
                effects,
                retries,
            } => render_finished(
                "aborted",
                self.task_id,
                error,
                effects,
                *retries,
                &self.identity_names,
                &self.relation_names,
            ),
            TaskOutcome::Suspended {
                kind,
                effects,
                retries,
            } => {
                let mut out = format!(
                    "task {} suspended: {:?} (retries: {})",
                    self.task_id, kind, retries
                );
                render_effects(
                    &mut out,
                    effects,
                    &self.identity_names,
                    &self.relation_names,
                );
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
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) -> String {
    let mut out = format!(
        "task {task_id} {label}: {} (retries: {retries})",
        render_value(value, identity_names, relation_names)
    );
    render_effects(&mut out, effects, identity_names, relation_names);
    out
}

fn render_effects(
    out: &mut String,
    effects: &[Value],
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) {
    for effect in effects {
        out.push_str("\neffect: ");
        out.push_str(&render_value(effect, identity_names, relation_names));
    }
}

fn render_value(
    value: &Value,
    identity_names: &BTreeMap<Identity, String>,
    relation_names: &BTreeMap<Identity, String>,
) -> String {
    match value.kind() {
        ValueKind::Nothing => "nothing".to_owned(),
        ValueKind::Bool => value.as_bool().unwrap().to_string(),
        ValueKind::Int => value.as_int().unwrap().to_string(),
        ValueKind::Float => format!("{:?}", value.as_float().unwrap()),
        ValueKind::Identity => {
            let identity = value.as_identity().unwrap();
            match identity_names.get(&identity) {
                Some(name) => format!("${name}"),
                None => match relation_names.get(&identity) {
                    Some(name) => format!("relation(:{name})"),
                    None => format!("${}", identity.raw()),
                },
            }
        }
        ValueKind::Symbol => render_symbol(value.as_symbol().unwrap(), ":"),
        ValueKind::ErrorCode => render_symbol(value.as_error_code().unwrap(), ""),
        ValueKind::String => value.with_str(|value| format!("{value:?}")).unwrap(),
        ValueKind::Bytes => format!("{value:?}"),
        ValueKind::List => value
            .with_list(|values| {
                render_sequence(
                    "[",
                    "]",
                    values
                        .iter()
                        .map(|value| render_value(value, identity_names, relation_names)),
                )
            })
            .unwrap(),
        ValueKind::Map => value
            .with_map(|entries| {
                render_sequence(
                    "[",
                    "]",
                    entries.iter().map(|(key, value)| {
                        format!(
                            "{}: {}",
                            render_value(key, identity_names, relation_names),
                            render_value(value, identity_names, relation_names)
                        )
                    }),
                )
            })
            .unwrap(),
        ValueKind::Range => value
            .with_range(|start, end| match end {
                Some(end) => format!(
                    "{}..{}",
                    render_value(start, identity_names, relation_names),
                    render_value(end, identity_names, relation_names)
                ),
                None => format!("{}..$", render_value(start, identity_names, relation_names)),
            })
            .unwrap(),
        ValueKind::Error => value
            .with_error(|error| {
                let mut out = format!("error({}", render_symbol(error.code(), ""));
                if let Some(message) = error.message() {
                    out.push_str(", ");
                    out.push_str(&format!("{message:?}"));
                }
                if let Some(payload) = error.value() {
                    if error.message().is_none() {
                        out.push_str(", nothing");
                    }
                    out.push_str(", ");
                    out.push_str(&render_value(payload, identity_names, relation_names));
                }
                out.push(')');
                out
            })
            .unwrap(),
    }
}

fn render_symbol(symbol: Symbol, prefix: &str) -> String {
    match symbol.name() {
        Some(name) => format!("{prefix}{name}"),
        None => format!("{prefix}#{}", symbol.id()),
    }
}

fn render_sequence(open: &str, close: &str, items: impl IntoIterator<Item = String>) -> String {
    let mut out = open.to_owned();
    for (index, item) in items.into_iter().enumerate() {
        if index != 0 {
            out.push_str(", ");
        }
        out.push_str(&item);
    }
    out.push_str(close);
    out
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
        assert_eq!(
            made.render(),
            "task 1 complete: relation(:Hog) (retries: 0)"
        );
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
    fn report_renders_named_identities_in_values_and_effects() {
        let mut runner = SourceRunner::new_empty();
        let made = runner.run_source("return make_identity(:thing)").unwrap();
        let report = runner
            .run_source("return emit([$thing, {:owner -> $thing}])")
            .unwrap();

        assert_eq!(made.render(), "task 1 complete: $thing (retries: 0)");
        assert_eq!(
            report.render(),
            "task 2 complete: [$thing, [:owner: $thing]] (retries: 0)\neffect: [$thing, [:owner: $thing]]"
        );
    }

    #[test]
    fn runner_relation_calls_with_query_vars_return_binding_maps() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location($thing, $room)").unwrap();

        let report = runner.run_source("return Location($thing, ?room)").unwrap();

        assert_eq!(
            report.render(),
            "task 5 complete: [[:room: $room]] (retries: 0)"
        );
    }

    #[test]
    fn runner_relation_queries_allow_all_positions_free() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location($thing, $room)").unwrap();

        let report = runner.run_source("return Location(?what, ?where)").unwrap();

        assert_eq!(
            report.render(),
            "task 5 complete: [[:what: $thing, :where: $room]] (retries: 0)"
        );
    }

    #[test]
    fn runner_one_and_dot_read_project_binary_relations() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_identity(:thing)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner.run_source("make_relation(:Location, 2)").unwrap();
        runner.run_source("assert Location($thing, $room)").unwrap();

        let one = runner
            .run_source("return one Location($thing, ?room)")
            .unwrap();
        let dot = runner.run_source("return $thing.location").unwrap();

        assert_eq!(one.render(), "task 5 complete: $room (retries: 0)");
        assert_eq!(dot.render(), "task 6 complete: $room (retries: 0)");
    }

    #[test]
    fn runner_installs_relation_rules_and_queries_derived_tuples() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        let rule = runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn($alice, $room)")
            .unwrap();
        runner.run_source("assert LocatedIn($lamp, $room)").unwrap();

        let query = runner.run_source("return VisibleTo($alice, ?obj)").unwrap();

        assert_eq!(rule.render(), "task 3 complete: $rule1 (retries: 0)");
        assert_eq!(
            query.render(),
            "task 9 complete: [[:obj: $alice], [:obj: $lamp]] (retries: 0)"
        );
    }

    #[test]
    fn runner_inspects_and_disables_rules() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn($alice, $room)")
            .unwrap();
        runner.run_source("assert LocatedIn($lamp, $room)").unwrap();

        let rules = runner.run_source("return rules(:VisibleTo)").unwrap();
        let source = runner
            .run_source("return describe_rule(one rules(:VisibleTo))")
            .unwrap();
        let disabled = runner
            .run_source("disable_rule(one rules(:VisibleTo))")
            .unwrap();
        let query = runner.run_source("return VisibleTo($alice, ?obj)").unwrap();

        assert_eq!(rules.render(), "task 9 complete: [$rule1] (retries: 0)");
        assert_eq!(
            source.render(),
            "task 10 complete: \"VisibleTo(actor, obj) :-\\n  LocatedIn(actor, room),\\n  LocatedIn(obj, room)\" (retries: 0)"
        );
        assert_eq!(disabled.render(), "task 11 complete: nothing (retries: 0)");
        assert_eq!(query.render(), "task 12 complete: [] (retries: 0)");
    }

    #[test]
    fn runner_fileouts_active_rules() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source("VisibleTo(actor, obj) :- LocatedIn(actor, obj)")
            .unwrap();

        let fileout = runner
            .run_source("return fileout_rules(:VisibleTo)")
            .unwrap();

        assert_eq!(
            fileout.render(),
            "task 4 complete: \"VisibleTo(actor, obj) :- LocatedIn(actor, obj)\" (retries: 0)"
        );

        let TaskOutcome::Complete { value, .. } = fileout.outcome else {
            panic!("expected fileout to complete");
        };
        let source = value.with_str(str::to_owned).unwrap();
        let mut imported = SourceRunner::new_empty();
        imported.run_source("make_relation(:LocatedIn, 2)").unwrap();
        imported.run_source("make_relation(:VisibleTo, 2)").unwrap();
        let installed = imported.run_source(&source).unwrap();
        assert_eq!(installed.render(), "task 3 complete: $rule1 (retries: 0)");
    }

    #[test]
    fn runner_installs_rules_with_surface_negation() {
        let mut runner = SourceRunner::new_empty();
        runner.run_source("make_relation(:LocatedIn, 2)").unwrap();
        runner.run_source("make_relation(:HiddenFrom, 2)").unwrap();
        runner.run_source("make_relation(:VisibleTo, 2)").unwrap();
        runner
            .run_source(
                "VisibleTo(actor, obj) :-\n  LocatedIn(actor, room),\n  LocatedIn(obj, room),\n  not HiddenFrom(obj, actor)",
            )
            .unwrap();
        runner.run_source("make_identity(:alice)").unwrap();
        runner.run_source("make_identity(:lamp)").unwrap();
        runner.run_source("make_identity(:room)").unwrap();
        runner
            .run_source("assert LocatedIn($alice, $room)")
            .unwrap();
        runner.run_source("assert LocatedIn($lamp, $room)").unwrap();
        runner
            .run_source("assert HiddenFrom($lamp, $alice)")
            .unwrap();

        let query = runner.run_source("return VisibleTo($alice, ?obj)").unwrap();

        assert_eq!(
            query.render(),
            "task 11 complete: [[:obj: $alice]] (retries: 0)"
        );
    }

    #[test]
    fn runner_filein_installs_mud_verbs_and_invokes_dispatch() {
        let mut runner = SourceRunner::new_empty();
        let reports = runner
            .run_filein(
                "make_identity(:player)\n\
                 make_identity(:thing)\n\
                 make_identity(:portable)\n\
                 make_identity(:container)\n\
                 make_identity(:alice)\n\
                 make_identity(:coin)\n\
                 make_identity(:box)\n\
                 make_relation(:Delegates, 3)\n\
                 make_relation(:HeldBy, 2)\n\
                 make_relation(:In, 2)\n\
                 make_relation(:Portable, 1)\n\
                 assert Delegates($portable, $thing, 0)\n\
                 assert Delegates($coin, $portable, 0)\n\
                 assert Delegates($alice, $player, 0)\n\
                 assert Delegates($box, $container, 0)\n\
                 assert Portable($coin)\n\
                 method $get_thing :get\n\
                   roles actor: $player, item: $thing\n\
                 do\n\
                   if Portable(item)\n\
                     assert HeldBy(actor, item)\n\
                     return true\n\
                   else\n\
                     return false\n\
                   end\n\
                 end\n\
                 method $put_thing :put\n\
                   roles actor: $player, item: $thing, container: $container\n\
                 do\n\
                   if HeldBy(actor, item)\n\
                     assert In(item, container)\n\
                     return true\n\
                   else\n\
                     return false\n\
                   end\n\
                 end\n\
                 :get(item: $coin, actor: $alice)\n\
                 :put(container: $box, item: $coin, actor: $alice)\n\
                 return In($coin, ?container)\n",
            )
            .unwrap();

        assert_eq!(
            reports[16].render(),
            "task 17 complete: $get_thing (retries: 0)"
        );
        assert_eq!(
            reports[17].render(),
            "task 18 complete: $put_thing (retries: 0)"
        );
        assert_eq!(reports[18].render(), "task 19 complete: true (retries: 0)");
        assert_eq!(reports[19].render(), "task 20 complete: true (retries: 0)");
        assert_eq!(
            reports[20].render(),
            "task 21 complete: [[:container: $box]] (retries: 0)"
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
