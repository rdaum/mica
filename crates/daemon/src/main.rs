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

use clap::Parser;
use compio::net::TcpListener;
use compio::runtime::Runtime;
use fast_telemetry_export::dogstatsd::DogStatsDConfig;
use mica_driver::{CompioTaskDriver, DriverEvent};
use mica_host_zmq::{ZmqHostSocket, ZmqSocketOptions};
use mica_runtime::{EmbeddingProviderKind, SourceRunner, TaskOutcome};
use mica_telnet_host::{
    ActorBinding as TelnetActorBinding, InProcessTelnetHost, serve_in_process as serve_telnet,
};
use mica_var::{Symbol, Value};
use mica_web_host::{InProcessWebHost, RequestBinding, serve_in_process as serve_web};
use mica_webtransport_host::{
    InProcessWebTransportHost, SessionBinding, WebTransportTlsConfig,
    bind_server_endpoint as bind_webtransport, serve_in_process as serve_webtransport,
};
use std::env;
use std::fs;
use std::future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};
use tracing_subscriber::EnvFilter;

mod external_http;
mod metrics;
#[allow(dead_code)]
mod rpc;

#[derive(Parser)]
#[command(
    name = "mica-daemon",
    about = "Run a Mica daemon with optional host endpoints"
)]
struct Cli {
    #[arg(long = "filein", value_name = "FILE")]
    fileins: Vec<PathBuf>,
    #[arg(long = "startup-source", value_name = "SOURCE")]
    startup_sources: Vec<String>,
    #[arg(long, value_enum, default_value_t = EmbeddingProviderMode::Deterministic)]
    embedding_provider: EmbeddingProviderMode,
    #[arg(long, default_value = "alice", value_name = "IDENTITY")]
    actor: String,
    #[arg(long, default_value = "web", value_name = "IDENTITY")]
    web_principal: String,
    #[arg(long, value_name = "THREADS")]
    driver_threads: Option<NonZeroUsize>,
    #[arg(long, value_name = "URI")]
    rpc_bind: Option<String>,
    #[arg(long, value_name = "ADDR")]
    telnet_bind: Option<SocketAddr>,
    #[arg(long, value_name = "ADDR")]
    web_bind: Option<SocketAddr>,
    #[arg(long, value_name = "ADDR")]
    webtransport_bind: Option<SocketAddr>,
    #[arg(long, default_value = "web", value_name = "IDENTITY")]
    webtransport_principal: String,
    #[arg(long, value_name = "FILE")]
    webtransport_cert: Option<PathBuf>,
    #[arg(long, value_name = "FILE")]
    webtransport_key: Option<PathBuf>,
    #[arg(long, value_name = "ADDR")]
    dogstatsd_endpoint: Option<String>,
    #[arg(long, default_value_t = 10, value_name = "SECONDS")]
    dogstatsd_interval_secs: u64,
    #[arg(long, value_name = "FILTER")]
    log_filter: Option<String>,
    #[arg(long)]
    no_log_ansi: bool,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Eq, PartialEq)]
enum EmbeddingProviderMode {
    Deterministic,
    Disabled,
    Vllm,
}

impl From<EmbeddingProviderMode> for EmbeddingProviderKind {
    fn from(value: EmbeddingProviderMode) -> Self {
        match value {
            EmbeddingProviderMode::Deterministic => Self::Deterministic,
            EmbeddingProviderMode::Disabled => Self::Disabled,
            EmbeddingProviderMode::Vllm => Self::Vllm,
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "mica-daemon stopped");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    init_tracing(&cli);
    Runtime::new()
        .map_err(|error| format!("failed to start compio runtime: {error}"))?
        .block_on(run_async(cli))
}

fn init_tracing(cli: &Cli) {
    let filter = cli
        .log_filter
        .clone()
        .or_else(|| env::var("MICA_LOG_FILTER").ok())
        .unwrap_or_else(|| "info".to_owned());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_log::LogTracer::init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(!cli.no_log_ansi)
        .try_init();
}

async fn run_async(cli: Cli) -> Result<(), String> {
    if cli.rpc_bind.is_none()
        && cli.telnet_bind.is_none()
        && cli.web_bind.is_none()
        && cli.webtransport_bind.is_none()
    {
        return Err("daemon needs at least one endpoint: use --rpc-bind, --telnet-bind, --web-bind, or --webtransport-bind".to_owned());
    }
    let configured_endpoints = [
        cli.rpc_bind.is_some(),
        cli.telnet_bind.is_some(),
        cli.web_bind.is_some(),
        cli.webtransport_bind.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    metrics::metrics()
        .endpoints_configured
        .set(configured_endpoints as i64);
    let webtransport_tls =
        if cli.webtransport_bind.is_some() {
            let cert = cli.webtransport_cert.as_ref().ok_or_else(|| {
                "--webtransport-cert is required with --webtransport-bind".to_owned()
            })?;
            let key = cli.webtransport_key.as_ref().ok_or_else(|| {
                "--webtransport-key is required with --webtransport-bind".to_owned()
            })?;
            Some(WebTransportTlsConfig::from_pem_files(cert, key)?)
        } else {
            None
        };
    let dogstatsd_endpoint = cli.dogstatsd_endpoint.clone();
    let dogstatsd_interval = Duration::from_secs(cli.dogstatsd_interval_secs.max(1));
    let mut runner = SourceRunner::new_empty_with_embedding_provider(cli.embedding_provider.into());
    for filein in &cli.fileins {
        let source = fs::read_to_string(filein)
            .map_err(|error| format!("failed to read {}: {error}", filein.display()))?;
        let include_base = filein.parent().unwrap_or_else(|| Path::new("."));
        runner
            .run_filein_with_include_loader(&source, |path| read_filein_include(include_base, path))
            .map_err(format_source_error)?;
        metrics::metrics().fileins_loaded.inc();
    }
    let telnet_actor = if cli.telnet_bind.is_some() {
        let actor_name = actor_name(&cli.actor)?;
        let actor = runner
            .named_identity(Symbol::intern(&actor_name))
            .map_err(format_source_error)?;
        Some(TelnetActorBinding {
            name: actor_name,
            identity: actor,
        })
    } else {
        None
    };
    let web_binding = if cli.web_bind.is_some() {
        let principal_name = actor_name(&cli.web_principal)?;
        let principal = runner
            .named_identity(Symbol::intern(&principal_name))
            .map_err(format_source_error)?;
        Some(RequestBinding {
            principal,
            actor: None,
        })
    } else {
        None
    };
    let webtransport_binding = if cli.webtransport_bind.is_some() {
        let principal_name = actor_name(&cli.webtransport_principal)?;
        let principal = runner
            .named_identity(Symbol::intern(&principal_name))
            .map_err(format_source_error)?;
        Some(SessionBinding {
            principal,
            actor: None,
        })
    } else {
        None
    };
    let driver = CompioTaskDriver::spawn_with_workers_and_external_handler(
        runner,
        cli.driver_threads,
        Some(external_http::handler()),
    )
    .map_err(format_driver_error)?;
    metrics::metrics().drivers_started.inc();
    if let Some(endpoint) = dogstatsd_endpoint {
        start_dogstatsd_export(endpoint, dogstatsd_interval);
    }
    for source in &cli.startup_sources {
        run_startup_source(&driver, source).await?;
    }
    if let Some(rpc_bind) = cli.rpc_bind {
        start_rpc_server(driver.clone(), rpc_bind)?;
    }
    if let Some(web_bind) = cli.web_bind {
        let binding = web_binding.expect("web principal should be resolved before driver spawn");
        let listener = TcpListener::bind(web_bind)
            .await
            .map_err(|error| format!("failed to bind web listener {web_bind}: {error}"))?;
        let local_addr = listener.local_addr().unwrap();
        tracing::info!(bind = %web_bind, local_addr = %local_addr, "web listener started");
        metrics::metrics()
            .endpoints_started
            .inc(metrics::DaemonEndpoint::Web);
        let host = InProcessWebHost::new(driver.clone());
        compio::runtime::spawn(async move {
            if let Err(error) = serve_web(listener, host, binding, None).await {
                tracing::error!(error = %error, "web host stopped");
            }
        })
        .detach();
    }
    if let Some(webtransport_bind) = cli.webtransport_bind {
        let binding = webtransport_binding
            .expect("WebTransport principal should be resolved before driver spawn");
        let tls = webtransport_tls.expect("WebTransport TLS should be loaded before driver spawn");
        let endpoint = bind_webtransport(webtransport_bind, tls).await?;
        let local_addr = endpoint.local_addr().unwrap();
        tracing::info!(
            bind = %webtransport_bind,
            local_addr = %local_addr,
            "WebTransport listener started"
        );
        metrics::metrics()
            .endpoints_started
            .inc(metrics::DaemonEndpoint::WebTransport);
        let host = InProcessWebTransportHost::new(driver.clone());
        if cli.telnet_bind.is_some() {
            compio::runtime::spawn(async move {
                if let Err(error) = serve_webtransport(endpoint, host, binding, None).await {
                    tracing::error!(error = %error, "WebTransport host stopped");
                }
            })
            .detach();
        } else {
            return serve_webtransport(endpoint, host, binding, None).await;
        }
    }
    if let Some(telnet_bind) = cli.telnet_bind {
        let actor = telnet_actor.expect("telnet actor should be resolved before driver spawn");
        let listener = TcpListener::bind(telnet_bind)
            .await
            .map_err(|error| format!("failed to bind telnet listener {telnet_bind}: {error}"))?;
        let local_addr = listener.local_addr().unwrap();
        tracing::info!(
            bind = %telnet_bind,
            local_addr = %local_addr,
            "telnet listener started"
        );
        metrics::metrics()
            .endpoints_started
            .inc(metrics::DaemonEndpoint::Telnet);
        return serve_telnet(listener, InProcessTelnetHost::new(driver), actor, None).await;
    }
    future::pending::<()>().await;
    Ok(())
}

fn start_rpc_server(driver: CompioTaskDriver, endpoint: String) -> Result<(), String> {
    let context = zmq::Context::new();
    let socket = ZmqHostSocket::bind(
        &context,
        zmq::ROUTER,
        &endpoint,
        ZmqSocketOptions::default(),
    )
    .map_err(|error| format!("failed to bind RPC socket {endpoint}: {error}"))?;
    tracing::info!(endpoint = %endpoint, "RPC listener started");
    metrics::metrics()
        .endpoints_started
        .inc(metrics::DaemonEndpoint::Rpc);
    compio::runtime::spawn(async move {
        let _context = context;
        let mut handler = rpc::RpcHandler::new(driver);
        if let Err(error) = rpc::serve_zmq_rpc_forever(&socket, &mut handler).await {
            tracing::error!(error = %error, "RPC server stopped");
        }
    })
    .detach();
    Ok(())
}

fn start_dogstatsd_export(endpoint: String, interval: Duration) {
    metrics::metrics().dogstatsd_configured.set(1);
    metrics::metrics().dogstatsd_exporters_started.inc();
    let config = DogStatsDConfig::new(endpoint).with_interval(interval);
    compio::runtime::spawn(async move {
        let mut daemon_state = metrics::DaemonMetricsDogStatsDState::new();
        let mut driver_state = mica_driver::metrics::DriverMetricsDogStatsDState::new();
        let mut relation_kernel_state =
            mica_relation_kernel::metrics::RelationKernelMetricsDogStatsDState::new();
        let mut runtime_state = mica_runtime::metrics::RuntimeMetricsDogStatsDState::new();
        let mut web_host_state = mica_web_host::metrics::WebHostMetricsDogStatsDState::new();
        let mut webtransport_host_state =
            mica_webtransport_host::metrics::WebTransportMetricsDogStatsDState::new();
        fast_telemetry_export::dogstatsd::run_compio(
            config,
            future::pending::<()>(),
            move |output| {
                metrics::metrics().export_dogstatsd_delta(output, &[], &mut daemon_state);
                mica_driver::metrics::metrics().export_dogstatsd_delta(
                    output,
                    &[],
                    &mut driver_state,
                );
                mica_relation_kernel::metrics::metrics().export_dogstatsd_delta(
                    output,
                    &[],
                    &mut relation_kernel_state,
                );
                mica_runtime::metrics::metrics().export_dogstatsd_delta(
                    output,
                    &[],
                    &mut runtime_state,
                );
                mica_web_host::metrics::metrics().export_dogstatsd_delta(
                    output,
                    &[],
                    &mut web_host_state,
                );
                mica_webtransport_host::metrics::metrics().export_dogstatsd_delta(
                    output,
                    &[],
                    &mut webtransport_host_state,
                );
                metrics::metrics().dogstatsd_export_ticks.inc();
            },
        )
        .await;
    })
    .detach();
}

fn read_filein_include(base: &Path, path: &str) -> Result<String, String> {
    let include_path = base.join(path);
    fs::read_to_string(&include_path)
        .map_err(|error| format!("failed to read {}: {error}", include_path.display()))
}

fn actor_name(actor: &str) -> Result<String, String> {
    let actor = actor.trim().trim_start_matches('#').trim_start_matches(':');
    if actor.is_empty()
        || !actor
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || actor.chars().next().is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err("actor must be a named identity such as alice or #alice".to_owned());
    }
    Ok(actor.to_owned())
}

fn log_startup_source_begin(source: &str) {
    tracing::info!(
        source = %source,
        description = startup_source_description(source),
        "startup source started"
    );
}

fn log_startup_source_end(source: &str, rendered_report: &str) {
    tracing::info!(
        source = %source,
        description = startup_source_description(source),
        report = %rendered_report,
        "startup source completed"
    );
}

async fn run_startup_source(driver: &CompioTaskDriver, source: &str) -> Result<(), String> {
    log_startup_source_begin(source);
    let is_source_retrieval_indexing = is_source_retrieval_indexing_source(source);
    let should_follow_spawned_child = startup_source_should_follow_spawned_child(source);
    let track_source_retrieval_indexing =
        is_source_retrieval_indexing && !should_follow_spawned_child;
    let start = Instant::now();
    if track_source_retrieval_indexing {
        metrics::source_retrieval_indexing_started();
    }
    let report = driver
        .submit_root_source_report(source.to_owned())
        .await
        .map_err(|error| {
            if track_source_retrieval_indexing {
                metrics::source_retrieval_indexing_failed(start.elapsed());
            }
            format_driver_error(error)
        })?;
    if !matches!(report.outcome, TaskOutcome::Suspended { .. }) {
        if track_source_retrieval_indexing {
            record_source_retrieval_indexing_report_outcome(start, &report.outcome);
        }
        log_startup_source_end(source, &report.render());
        return Ok(());
    }
    let tracked_task_id = report.task_id;
    loop {
        for event in driver.wait_events().await {
            match event {
                DriverEvent::TaskCompleted { task_id, value } if task_id == tracked_task_id => {
                    if should_follow_spawned_child
                        && let Some(child_task_id) = spawned_child_task_id(&value)
                    {
                        tracing::info!(
                            source = %source,
                            description = startup_source_description(source),
                            parent_task_id = task_id,
                            child_task_id,
                            "startup source spawned background task"
                        );
                        log_startup_source_end(
                            source,
                            &format!("spawned background task {child_task_id}"),
                        );
                        return Ok(());
                    }
                    if track_source_retrieval_indexing {
                        metrics::source_retrieval_indexing_completed(
                            start.elapsed(),
                            value.as_int(),
                        );
                    }
                    log_startup_source_end(source, &format!("completed with {}", value));
                    return Ok(());
                }
                DriverEvent::TaskAborted { task_id, error } if task_id == tracked_task_id => {
                    if track_source_retrieval_indexing {
                        metrics::source_retrieval_indexing_failed(start.elapsed());
                    }
                    return Err(format!(
                        "startup source {} aborted with {}",
                        startup_source_description(source),
                        error
                    ));
                }
                DriverEvent::TaskFailed { task_id, error } if task_id == tracked_task_id => {
                    if track_source_retrieval_indexing {
                        metrics::source_retrieval_indexing_failed(start.elapsed());
                    }
                    return Err(format!(
                        "startup source {} failed: {error}",
                        startup_source_description(source)
                    ));
                }
                DriverEvent::TaskSuspended { task_id, kind }
                    if task_id == tracked_task_id && !startup_suspend_can_resume(&kind) =>
                {
                    if track_source_retrieval_indexing {
                        metrics::source_retrieval_indexing_failed(start.elapsed());
                    }
                    return Err(format!(
                        "startup source {} suspended without an automatic resume: {:?}",
                        startup_source_description(source),
                        kind
                    ));
                }
                _ => {}
            }
        }
    }
}

fn spawned_child_task_id(value: &Value) -> Option<u64> {
    let task_id = value.as_int()?;
    if task_id <= 0 {
        return None;
    }
    Some(task_id as u64)
}

fn record_source_retrieval_indexing_report_outcome(start: Instant, outcome: &TaskOutcome) {
    match outcome {
        TaskOutcome::Complete { value, .. } => {
            metrics::source_retrieval_indexing_completed(start.elapsed(), value.as_int());
        }
        TaskOutcome::Aborted { .. } | TaskOutcome::Suspended { .. } => {
            metrics::source_retrieval_indexing_failed(start.elapsed());
        }
    }
}

fn startup_suspend_can_resume(kind: &mica_runtime::SuspendKind) -> bool {
    match kind {
        mica_runtime::SuspendKind::Commit
        | mica_runtime::SuspendKind::TimedMillis(_)
        | mica_runtime::SuspendKind::Spawn(_)
        | mica_runtime::SuspendKind::ExternalRequest(_) => true,
        mica_runtime::SuspendKind::MailboxRecv(request) => request.timeout_millis.is_some(),
        mica_runtime::SuspendKind::Never | mica_runtime::SuspendKind::WaitingForInput(_) => false,
    }
}

fn startup_source_description(source: &str) -> &'static str {
    if startup_source_should_follow_spawned_child(source) {
        "spawning source retrieval index prewarm"
    } else if is_source_retrieval_indexing_source(source) {
        "prewarming source retrieval index"
    } else {
        "running startup source"
    }
}

fn startup_source_should_follow_spawned_child(source: &str) -> bool {
    source.contains("spawn") && source.contains("source/run_retrieval_prewarm")
}

fn is_source_retrieval_indexing_source(source: &str) -> bool {
    source.contains("source/prewarm_retrieval_index")
        || source.contains("source/run_retrieval_prewarm")
}

fn format_source_error(error: mica_runtime::SourceTaskError) -> String {
    format!("error: {error:?}")
}

fn format_driver_error(error: mica_driver::DriverError) -> String {
    format!("error: {error}")
}
