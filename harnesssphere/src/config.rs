//! Configuração via TOML + override por env. Sprint 1: intervalos e seleção de exporter.

use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Intervalo de coleta do host, em segundos.
    pub host_interval_secs: u64,
    /// Intervalo de coleta do próprio watcher, em segundos.
    pub self_interval_secs: u64,
    /// Falhas consecutivas a partir das quais um source Critical é fatal.
    pub critical_threshold: u32,
    /// Exporter ativo: "stdout" (default) ou "otlp" (feature `otlp`).
    pub exporter: String,
    /// Endpoint OTLP/gRPC (usado quando exporter = "otlp").
    pub otlp_endpoint: String,
    /// `service.name` no Resource OTel.
    pub service_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host_interval_secs: 5,
            self_interval_secs: 10,
            critical_threshold: 3,
            exporter: "stdout".to_owned(),
            otlp_endpoint: "http://localhost:4317".to_owned(),
            service_name: "harnesssphere".to_owned(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("falha ao ler config {p}: {e}"))?;
                toml::from_str(&raw)?
            }
            None => Config::default(),
        };
        if let Ok(v) = std::env::var("HARNESSSPHERE_EXPORTER") {
            cfg.exporter = v;
        }
        if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            cfg.otlp_endpoint = v;
        }
        Ok(cfg)
    }

    pub fn host_interval(&self) -> Duration {
        Duration::from_secs(self.host_interval_secs.max(1))
    }
    pub fn self_interval(&self) -> Duration {
        Duration::from_secs(self.self_interval_secs.max(1))
    }
}
