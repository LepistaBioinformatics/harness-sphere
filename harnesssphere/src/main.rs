//! `harnesssphere` — composition root.
//!
//! Wires the ports↔adapters together and runs the supervisor. The only place that knows
//! all the concrete adapters. Sprint 1: Critical collectors (host, self) → stdout exporter.

mod config;
mod discovery;

use std::sync::Arc;

use config::Config;
use discovery::{Discovery, DiscoveryStats};
use harnesssphere_collectors::{
    ContainerCollector, EndpointProbeCollector, HostCollector, ProbeTarget, ProcessCollector,
    SelfCollector,
};
use std::sync::atomic::AtomicU64;
use harnesssphere_domain::{Layer, SignalExporter, SignalSource};
use harnesssphere_export::StdoutExporter;
use harnesssphere_runtime::{RuntimeConfig, Supervisor};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let path = std::env::args().nth(1);
    let cfg = match Config::load(path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(2);
        }
    };

    // --- Composition root: assembles the sources (ports) with their concrete adapters ---
    let mut sources: Vec<Box<dyn SignalSource>> = Vec::new();
    sources.push(Box::new(HostCollector::new(cfg.host_interval()))); // Critical
    match SelfCollector::new(cfg.self_interval()) {
        Ok(s) => sources.push(Box::new(s)), // Critical
        Err(e) => {
            // The watcher's own collector is mandatory: without it, there's no reason to start.
            eprintln!("fatal failure starting 'self' collector: {e}");
            std::process::exit(1);
        }
    }
    // Optional, harness-independent: watch external processes and probe endpoints.
    if !cfg.watch_processes.is_empty() {
        sources.push(Box::new(ProcessCollector::new(
            cfg.watch_processes.clone(),
            cfg.host_interval(),
        )));
    }
    if !cfg.probe_targets.is_empty() {
        let mut targets = Vec::with_capacity(cfg.probe_targets.len());
        for t in &cfg.probe_targets {
            // Strict on purpose: an unrecognised layer is a boot-time config error, loud.
            // Defaulting it would silently file a service under the wrong layer, which is
            // indistinguishable from correct output until someone reads a dashboard.
            match Layer::parse(&t.layer) {
                Some(layer) => targets.push(ProbeTarget::new(t.address.clone(), layer)),
                None => {
                    eprintln!(
                        "probe_targets: unknown layer '{}' for '{}' \
                         (expected one of: host, watcher, gateway, proxy, webapp, harness)",
                        t.layer, t.address
                    );
                    std::process::exit(2);
                }
            }
        }
        sources.push(Box::new(EndpointProbeCollector::new(
            targets,
            cfg.host_interval(),
        )));
    }
    if !cfg.container_cgroup.is_empty() {
        sources.push(Box::new(ContainerCollector::new(
            cfg.container_cgroup.clone(),
            cfg.container_id.clone(),
            cfg.host_interval(),
        )));
    }

    // --- Output adapter (driven) ---
    let exporter: Arc<dyn SignalExporter> = match cfg.exporter.as_str() {
        "stdout" => Arc::new(StdoutExporter::new()),
        "otlp" => build_otlp_exporter(&cfg),
        other => {
            eprintln!("unknown exporter '{other}' (use 'stdout' or 'otlp')");
            std::process::exit(2);
        }
    };

    let rt_cfg = RuntimeConfig {
        critical_threshold: cfg.critical_threshold,
        ..Default::default()
    };

    // --- Workspace discovery (control plane) ---
    // Registered BEFORE the supervisor starts so its counters exist from the first tick;
    // the scanning task is spawned separately and drives add/remove over the command
    // channel (DEC-20).
    let workspaces = Arc::new(AtomicU64::new(0));
    let scans = Arc::new(AtomicU64::new(0));
    let discovery = (!cfg.data_root.is_empty()).then(|| Discovery {
        data_root: std::path::PathBuf::from(&cfg.data_root),
        interval: cfg.discovery_interval(),
        session_interval: cfg.session_interval(),
        harness_name: cfg.session_source.clone(),
        workspaces: workspaces.clone(),
        scans: scans.clone(),
    });
    if discovery.is_some() {
        sources.push(Box::new(DiscoveryStats::new(
            workspaces.clone(),
            scans.clone(),
            cfg.self_interval(),
        )));
    }

    tracing::info!(
        sources = sources.len(),
        discovery = discovery.is_some(),
        exporter = %cfg.exporter,
        "HarnessSphere starting"
    );

    let supervisor = Supervisor::new(rt_cfg, sources, exporter);
    let discovery_task = discovery.map(|d| {
        let cmds = supervisor.commands();
        tokio::spawn(d.run(cmds))
    });

    let outcome = supervisor.run().await;
    // Stop scanning before reporting: a discovery tick landing after shutdown would try to
    // push commands into a supervisor that is gone, and log a misleading warning.
    if let Some(t) = discovery_task {
        t.abort();
    }
    match outcome {
        Ok(()) => {
            tracing::info!("shut down gracefully");
        }
        Err(fatal) => {
            tracing::error!(source = fatal.source, reason = %fatal.reason, "critical FATAL");
            eprintln!("FATAL: critical collector '{}' went down: {}", fatal.source, fatal.reason);
            std::process::exit(1);
        }
    }
}

#[cfg(feature = "otlp")]
fn build_otlp_exporter(cfg: &Config) -> Arc<dyn SignalExporter> {
    use harnesssphere_export::OtlpExporter;
    let host = hostname();
    let interval = std::time::Duration::from_secs(cfg.metric_export_interval_secs.max(1));
    match OtlpExporter::new(&cfg.otlp_endpoint, &cfg.service_name, &host, interval) {
        Ok(e) => {
            tracing::info!(endpoint = %cfg.otlp_endpoint, "OTLP/gRPC exporter active");
            Arc::new(e)
        }
        Err(e) => {
            eprintln!("failed to start OTLP exporter: {e}");
            std::process::exit(2);
        }
    }
}

#[cfg(not(feature = "otlp"))]
fn build_otlp_exporter(_cfg: &Config) -> Arc<dyn SignalExporter> {
    eprintln!(
        "exporter 'otlp' unavailable: rebuild with `--features otlp` \
         (cargo run -p harnesssphere --features otlp)"
    );
    std::process::exit(2);
}

#[cfg(feature = "otlp")]
fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_owned())
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_owned())
}
