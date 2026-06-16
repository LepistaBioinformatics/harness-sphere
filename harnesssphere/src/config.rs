//! Configuration via TOML + env override. Sprint 1: intervals and exporter selection.

use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Host collection interval, in seconds.
    pub host_interval_secs: u64,
    /// Watcher self-collection interval, in seconds.
    pub self_interval_secs: u64,
    /// Consecutive failures at which a Critical source becomes fatal.
    pub critical_threshold: u32,
    /// Active exporter: "stdout" (default) or "otlp" (feature `otlp`).
    pub exporter: String,
    /// OTLP/gRPC endpoint (used when exporter = "otlp").
    pub otlp_endpoint: String,
    /// `service.name` in the OTel Resource.
    pub service_name: String,
    /// Cadence (seconds) of the periodic OTLP metrics reader.
    pub metric_export_interval_secs: u64,
    /// Enable the local OTLP ingest receiver (feature `ingest`).
    pub ingest_enabled: bool,
    /// Address the OTLP ingest receiver binds to (gRPC).
    pub ingest_endpoint: String,
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
            metric_export_interval_secs: 15,
            ingest_enabled: false,
            // Default to :4318 so a single instance with both exporter+ingest on defaults
            // doesn't form a telemetry loop with the :4317 OTLP exporter target.
            ingest_endpoint: "0.0.0.0:4318".to_owned(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let raw = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("failed to read config {p}: {e}"))?;
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
