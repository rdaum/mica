use mica_compiler::{CompileContext, SourceTaskError, submit_source_task};
use mica_relation_kernel::RelationKernel;
use mica_runtime::{Scheduler, TaskOutcome};
use mica_var::Value;

pub struct SourceRunner {
    context: CompileContext,
    scheduler: Scheduler,
}

impl SourceRunner {
    pub fn new_empty() -> Self {
        Self {
            context: CompileContext::new(),
            scheduler: Scheduler::new(RelationKernel::new()),
        }
    }

    pub fn run_source(&mut self, source: &str) -> Result<RunReport, SourceTaskError> {
        let submitted = submit_source_task(source, &self.context, &mut self.scheduler)?;
        Ok(RunReport {
            task_id: submitted.task_id,
            outcome: submitted.outcome,
        })
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
    use mica_var::Value;

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
    fn report_renders_task_outcome() {
        let mut runner = SourceRunner::new_empty();
        let report = runner.run_source("return true").unwrap();

        assert_eq!(report.render(), "task 1 complete: true (retries: 0)");
    }
}
