//! `harnesssphere` — composition root.
//!
//! Wires the ports↔adapters together and runs the supervisor. The only place that knows
//! all the concrete adapters. Sprint 1: Critical collectors (host, self) → stdout exporter.

mod config;

use std::sync::Arc;

use config::Config;
use harnesssphere_collectors::{HostCollector, SelfCollector};
use harnesssphere_domain::{SignalExporter, SignalSource};
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

    tracing::info!(
        sources = sources.len(),
        exporter = %cfg.exporter,
        "HarnessSphere starting"
    );

    let supervisor = Supervisor::new(rt_cfg, sources, exporter);
    match supervisor.run().await {
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
