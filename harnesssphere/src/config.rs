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
