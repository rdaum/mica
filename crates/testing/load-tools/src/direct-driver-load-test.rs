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

//! Direct driver load test for Mica.
//!
//! This bypasses TCP transport and submits role-dispatch invocations directly to
//! the in-process compio driver thread. Each submitted task runs a Mica method
//! that repeatedly dispatches to another Mica method, so the hot path is driver
//! command handling, runtime task execution, VM dispatch, and relation-kernel
//! method lookup.

use clap::{Parser, ValueEnum};
use mica_driver::{CompioTaskDriverThread, DriverThreadError};
use mica_relation_kernel::FjallDurabilityMode;
use mica_runtime::{
    AuthorityContext, SourceRunner, TaskInput, TaskLimits, TaskOutcome, TaskRequest,
};
use mica_var::{Identity, Symbol, Value};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

const ENDPOINT_ID_START: u64 = 0x00ee_1000_0000_0000;

#[derive(Clone, Parser, Debug)]
#[command(
    name = "direct-driver-load-test",
    about = "Load test Mica driver/runtime dispatch without TCP transport"
)]
struct Args {
    #[arg(long, help = "Optional fresh Fjall store path")]
    store: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = DurabilityMode::Relaxed)]
    durability: DurabilityMode,

    #[arg(long, default_value_t = 1)]
    min_concurrency: usize,

    #[arg(long, default_value_t = 32)]
    max_concurrency: usize,

    #[arg(long, default_value_t = 1)]
    num_objects: usize,

    #[arg(long, default_value_t = 7000)]
    num_dispatch_iterations: usize,

    #[arg(long, default_value_t = 200)]
    num_invocations: usize,

    #[arg(long, default_value_t = 1_000_000_000)]
    instruction_budget: usize,

    #[arg(long, default_value_t = 5)]
    warmup_invocations: usize,

    #[arg(long)]
    output_file: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    swamp_mode: bool,

    #[arg(long, default_value_t = 30)]
    swamp_duration_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum DurabilityMode {
    Relaxed,
    Strict,
}

impl From<DurabilityMode> for FjallDurabilityMode {
    fn from(value: DurabilityMode) -> Self {
        match value {
            DurabilityMode::Relaxed => Self::Relaxed,
            DurabilityMode::Strict => Self::Strict,
        }
    }
}

#[derive(Clone, Debug)]
struct WorkloadConfig {
    actor: Identity,
    items: Value,
    iterations: Value,
}

#[derive(Clone, Debug)]
struct Results {
    concurrency: usize,
    invocations: usize,
    dispatch_calls: usize,
    wall_time: Duration,
    cumulative_time: Duration,
    per_dispatch_wall: Duration,
    throughput: f64,
    invocation_p50: Duration,
    invocation_p95: Duration,
    invocation_p99: Duration,
    invocation_max: Duration,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    validate_args(&args)?;

    let mut runner = open_runner(&args)?;
    runner
        .run_filein(&load_test_filein(args.num_objects))
        .map_err(|error| format!("failed to seed load-test world: {error:?}"))?;

    let workload = WorkloadConfig {
        actor: runner
            .named_identity(Symbol::intern("alice"))
            .map_err(|error| format!("failed to resolve #alice: {error:?}"))?,
        items: Value::list(
            (1..=args.num_objects)
                .map(|index| format!("test_item_{index}"))
                .map(|name| {
                    runner
                        .named_identity(Symbol::intern(&name))
                        .map(Value::identity)
                        .map_err(|error| format!("failed to resolve #{name}: {error:?}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        iterations: Value::int(args.num_dispatch_iterations as i64)
            .map_err(|error| format!("invalid iteration count: {error:?}"))?,
    };

    let driver =
        CompioTaskDriverThread::spawn(runner).map_err(|error| format!("driver failed: {error}"))?;
    let results = if args.swamp_mode {
        run_swamp_mode(&args, &workload, &driver)?
    } else {
        run_stepped_load(&args, &workload, &driver)?
    };
    write_csv(args.output_file.as_ref(), &results)?;
    driver
        .shutdown()
        .map_err(|error| format!("driver shutdown failed: {error}"))?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.min_concurrency == 0 || args.max_concurrency == 0 {
        return Err("concurrency values must be greater than zero".to_owned());
    }
    if args.min_concurrency > args.max_concurrency {
        return Err("--min-concurrency must be <= --max-concurrency".to_owned());
    }
    if args.num_objects == 0 {
        return Err("--num-objects must be greater than zero".to_owned());
    }
    if args.num_dispatch_iterations == 0 {
        return Err("--num-dispatch-iterations must be greater than zero".to_owned());
    }
    if args.num_invocations == 0 {
        return Err("--num-invocations must be greater than zero".to_owned());
    }
    if args.swamp_mode && args.swamp_duration_seconds == 0 {
        return Err("--swamp-duration-seconds must be greater than zero".to_owned());
    }
    Ok(())
}

fn open_runner(args: &Args) -> Result<SourceRunner, String> {
    let runner = match &args.store {
        Some(path) => {
            if path.exists() {
                return Err(format!(
                    "store path {} already exists; use a fresh path for this load test",
                    path.display()
                ));
            }
            SourceRunner::open_fjall(path, args.durability.into())
        }
        None => Ok(SourceRunner::new_empty()),
    }?;
    Ok(runner.with_task_limits(TaskLimits {
        instruction_budget: args.instruction_budget,
        ..TaskLimits::default()
    }))
}

fn load_test_filein(num_objects: usize) -> String {
    let mut source = String::new();
    source.push_str(
        "make_identity(:player)\n\
         make_identity(:test_item)\n\
         make_identity(:alice)\n",
    );
    for index in 1..=num_objects {
        source.push_str(&format!("make_identity(:test_item_{index})\n"));
    }
    source.push_str(
        "\n\
         make_relation(:Delegates, 3)\n\
         assert Delegates(#alice, #player, 0)\n",
    );
    for index in 1..=num_objects {
        source.push_str(&format!(
            "assert Delegates(#test_item_{index}, #test_item, 0)\n"
        ));
    }
    source.push_str(
        "\n\
         verb load_test(item @ #test_item)\n\
           return 1\n\
         end\n\
         \n\
         verb invoke_load_test(actor @ #player, iterations, items)\n\
           let i = 0\n\
           while i < iterations\n\
             for item in items\n\
               if :load_test(item: item) != 1\n\
                 raise E_INVARG, \"load test failed\"\n\
               end\n\
             end\n\
             i = i + 1\n\
           end\n\
           return 1\n\
         end\n",
    );
    source
}

fn run_stepped_load(
    args: &Args,
    workload: &WorkloadConfig,
    driver: &CompioTaskDriverThread,
) -> Result<Vec<Results>, String> {
    warm_up(args, workload, driver)?;

    let mut rows = Vec::new();
    let mut concurrency = args.min_concurrency as f64;
    while concurrency <= args.max_concurrency as f64 {
        let current = concurrency as usize;
        let result = run_fixed_concurrency(
            current,
            args.num_invocations,
            None,
            args.num_dispatch_iterations,
            args.num_objects,
            workload,
            driver,
        )?;
        print_result(&result);
        rows.push(result);

        let next = (concurrency * 1.25).max(concurrency + 1.0);
        concurrency = next;
    }
    Ok(rows)
}

fn run_swamp_mode(
    args: &Args,
    workload: &WorkloadConfig,
    driver: &CompioTaskDriverThread,
) -> Result<Vec<Results>, String> {
    warm_up(args, workload, driver)?;
    let result = run_fixed_concurrency(
        args.max_concurrency,
        usize::MAX,
        Some(Duration::from_secs(args.swamp_duration_seconds)),
        args.num_dispatch_iterations,
        args.num_objects,
        workload,
        driver,
    )?;
    print_result(&result);
    Ok(vec![result])
}

fn warm_up(
    args: &Args,
    workload: &WorkloadConfig,
    driver: &CompioTaskDriverThread,
) -> Result<(), String> {
    if args.warmup_invocations == 0 {
        return Ok(());
    }
    let result = run_fixed_concurrency(
        1,
        args.warmup_invocations,
        None,
        args.num_dispatch_iterations,
        args.num_objects,
        workload,
        driver,
    )?;
    eprintln!(
        "warmup: {} invocations in {}",
        result.invocations,
        format_duration(result.wall_time)
    );
    Ok(())
}

fn run_fixed_concurrency(
    concurrency: usize,
    invocations_per_worker: usize,
    duration_limit: Option<Duration>,
    iterations: usize,
    object_count: usize,
    workload: &WorkloadConfig,
    driver: &CompioTaskDriverThread,
) -> Result<Results, String> {
    let start = Instant::now();
    let stop_at = duration_limit.map(|duration| start + duration);
    let mut worker_results = Vec::with_capacity(concurrency);

    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let endpoint = endpoint(worker);
            handles.push(scope.spawn(move || {
                run_worker(driver, workload, endpoint, invocations_per_worker, stop_at)
            }));
        }
        for handle in handles {
            worker_results.push(
                handle
                    .join()
                    .map_err(|_| "worker thread panicked".to_owned())??,
            );
        }
        Ok::<(), String>(())
    })?;

    let wall_time = start.elapsed();
    let invocations = worker_results.iter().map(|result| result.invocations).sum();
    let cumulative_time = worker_results
        .iter()
        .map(|result| result.elapsed)
        .fold(Duration::ZERO, |acc, value| acc + value);
    let mut latencies = worker_results
        .into_iter()
        .flat_map(|result| result.latencies)
        .collect::<Vec<_>>();
    let dispatch_calls = invocations * ((iterations * object_count) + 1);
    let per_dispatch_wall = duration_per_count(wall_time, dispatch_calls);
    let throughput = if wall_time.is_zero() {
        0.0
    } else {
        dispatch_calls as f64 / wall_time.as_secs_f64()
    };
    let (p50, p95, p99, max) = percentiles(&mut latencies);

    Ok(Results {
        concurrency,
        invocations,
        dispatch_calls,
        wall_time,
        cumulative_time,
        per_dispatch_wall,
        throughput,
        invocation_p50: p50,
        invocation_p95: p95,
        invocation_p99: p99,
        invocation_max: max,
    })
}

#[derive(Debug)]
struct WorkerResult {
    invocations: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

fn run_worker(
    driver: &CompioTaskDriverThread,
    workload: &WorkloadConfig,
    endpoint: Identity,
    invocation_limit: usize,
    stop_at: Option<Instant>,
) -> Result<WorkerResult, String> {
    let start = Instant::now();
    let mut latencies = Vec::new();
    let mut invocations = 0;

    while invocations < invocation_limit && stop_at.is_none_or(|stop_at| Instant::now() < stop_at) {
        let request = invocation_request(endpoint, workload);
        let invocation_start = Instant::now();
        let submitted = driver
            .submit_invocation(endpoint, request)
            .map_err(format_driver_error)?;
        latencies.push(invocation_start.elapsed());
        assert_success(submitted.outcome)?;
        invocations += 1;
    }

    Ok(WorkerResult {
        invocations,
        elapsed: start.elapsed(),
        latencies,
    })
}

fn invocation_request(endpoint: Identity, workload: &WorkloadConfig) -> TaskRequest {
    TaskRequest {
        principal: None,
        actor: None,
        endpoint,
        authority: AuthorityContext::root(),
        input: TaskInput::Invocation {
            selector: Symbol::intern("invoke_load_test"),
            roles: vec![
                (Symbol::intern("actor"), Value::identity(workload.actor)),
                (Symbol::intern("iterations"), workload.iterations.clone()),
                (Symbol::intern("items"), workload.items.clone()),
            ],
        },
    }
}

fn assert_success(outcome: TaskOutcome) -> Result<(), String> {
    match outcome {
        TaskOutcome::Complete { value, .. } if value.as_int() == Some(1) => Ok(()),
        TaskOutcome::Complete { value, .. } => {
            Err(format!("unexpected task result: expected 1, got {value:?}"))
        }
        TaskOutcome::Aborted { error, .. } => Err(format!("task aborted: {error:?}")),
        TaskOutcome::Suspended { kind, .. } => {
            Err(format!("task suspended unexpectedly: {kind:?}"))
        }
    }
}

fn endpoint(worker: usize) -> Identity {
    Identity::new(ENDPOINT_ID_START + worker as u64).expect("worker endpoint identity out of range")
}

fn duration_per_count(duration: Duration, count: usize) -> Duration {
    if count == 0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(duration.as_secs_f64() / count as f64)
}

fn percentiles(latencies: &mut [Duration]) -> (Duration, Duration, Duration, Duration) {
    if latencies.is_empty() {
        return (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
    }
    latencies.sort_unstable();
    (
        percentile(latencies, 0.50),
        percentile(latencies, 0.95),
        percentile(latencies, 0.99),
        *latencies.last().unwrap(),
    )
}

fn percentile(sorted: &[Duration], percentile: f64) -> Duration {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn print_result(result: &Results) {
    println!(
        "conc={} invocations={} dispatch_calls={} wall={} per_dispatch={} throughput={:.2}/s invocation_p50={} p95={} p99={} max={}",
        result.concurrency,
        result.invocations,
        result.dispatch_calls,
        format_duration(result.wall_time),
        format_duration(result.per_dispatch_wall),
        result.throughput,
        format_duration(result.invocation_p50),
        format_duration(result.invocation_p95),
        format_duration(result.invocation_p99),
        format_duration(result.invocation_max),
    );
}

fn write_csv(output_file: Option<&PathBuf>, results: &[Results]) -> Result<(), String> {
    let Some(output_file) = output_file else {
        return Ok(());
    };
    let mut output =
        "concurrency,invocations,dispatch_calls,wall_time_ns,cumulative_time_ns,per_dispatch_wall_ns,throughput_per_sec,invocation_p50_ns,invocation_p95_ns,invocation_p99_ns,invocation_max_ns\n"
            .to_owned();
    for result in results {
        output.push_str(&format!(
            "{},{},{},{},{},{},{:.0},{},{},{},{}\n",
            result.concurrency,
            result.invocations,
            result.dispatch_calls,
            result.wall_time.as_nanos(),
            result.cumulative_time.as_nanos(),
            result.per_dispatch_wall.as_nanos(),
            result.throughput,
            result.invocation_p50.as_nanos(),
            result.invocation_p95.as_nanos(),
            result.invocation_p99.as_nanos(),
            result.invocation_max.as_nanos(),
        ));
    }
    fs::write(output_file, output)
        .map_err(|error| format!("failed to write {}: {error}", output_file.display()))
}

fn format_driver_error(error: DriverThreadError) -> String {
    format!("driver error: {error}")
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{:.3}s", duration.as_secs_f64())
    } else if duration.as_millis() > 0 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else if duration.as_micros() > 0 {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    } else {
        format!("{}ns", duration.as_nanos())
    }
}
