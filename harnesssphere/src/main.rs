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
        "otlp" => build_otlp_exporter(&cfg),
        other => {
            eprintln!("exporter '{other}' desconhecido (use 'stdout' ou 'otlp')");
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

#[cfg(feature = "otlp")]
fn build_otlp_exporter(cfg: &Config) -> Arc<dyn SignalExporter> {
    use harnesssphere_export::OtlpExporter;
    let host = hostname();
    match OtlpExporter::new(&cfg.otlp_endpoint, &cfg.service_name, &host) {
        Ok(e) => {
            tracing::info!(endpoint = %cfg.otlp_endpoint, "exporter OTLP/gRPC ativo");
            Arc::new(e)
        }
        Err(e) => {
            eprintln!("falha ao iniciar exporter OTLP: {e}");
            std::process::exit(2);
        }
    }
}

#[cfg(not(feature = "otlp"))]
fn build_otlp_exporter(_cfg: &Config) -> Arc<dyn SignalExporter> {
    eprintln!(
        "exporter 'otlp' indisponível: rebuilde com `--features otlp` \
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
