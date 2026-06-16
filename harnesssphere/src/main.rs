//! `harnesssphere` — composition root.
//!
//! Faz o *wiring* dos ports↔adapters e roda o supervisor. Único lugar que conhece todos
//! os adapters concretos. Sprint 1: coletores Critical (host, self) → exporter stdout.

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
            eprintln!("erro de configuração: {e}");
            std::process::exit(2);
        }
    };

    // --- Composition root: monta os sources (ports) com seus adapters concretos ---
    let mut sources: Vec<Box<dyn SignalSource>> = Vec::new();
    sources.push(Box::new(HostCollector::new(cfg.host_interval()))); // Critical
    match SelfCollector::new(cfg.self_interval()) {
        Ok(s) => sources.push(Box::new(s)), // Critical
        Err(e) => {
            // O coletor do próprio watcher é obrigatório: sem ele, não há razão para subir.
            eprintln!("falha fatal ao iniciar coletor 'self': {e}");
            std::process::exit(1);
        }
    }

    // --- Adapter de saída (driven) ---
    let exporter: Arc<dyn SignalExporter> = match cfg.exporter.as_str() {
        "stdout" => Arc::new(StdoutExporter::new()),
        other => {
            eprintln!(
                "exporter '{other}' indisponível neste build (sprint 1 só tem 'stdout'; \
                 OTLP chega na feature `otlp`)"
            );
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
        "HarnessSphere iniciando"
    );

    let supervisor = Supervisor::new(rt_cfg, sources, exporter);
    match supervisor.run().await {
        Ok(()) => {
            tracing::info!("encerrado graciosamente");
        }
        Err(fatal) => {
            tracing::error!(source = fatal.source, reason = %fatal.reason, "FATAL crítico");
            eprintln!("FATAL: coletor crítico '{}' caiu: {}", fatal.source, fatal.reason);
            std::process::exit(1);
        }
    }
}
