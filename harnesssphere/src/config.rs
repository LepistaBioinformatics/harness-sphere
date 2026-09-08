//! Configuration via TOML + env override. Sprint 1: intervals and exporter selection.

use serde::Deserialize;

/// One `[[probe_targets]]` entry.
///
/// Both fields are REQUIRED -- there is no default layer. A target whose layer was
/// guessed would file the proxy or the webapp under whatever the guess was, which is the
/// exact defect this replaced: one collector stamping one layer on every target.
/// `deny_unknown_fields` is load-bearing, not tidiness. In TOML every bare key after a
/// `[[probe_targets]]` header belongs to THAT table until the next header, so a scalar
/// written below the probe list is silently absorbed into the last target. Without this,
/// serde drops it and the option reads as unset -- which is how `data_root` went missing
/// with no error anywhere.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeTargetCfg {
    /// `host:port`. The port is not optional.
    pub address: String,
    /// One of: host, watcher, gateway, proxy, webapp, harness.
    pub layer: String,
}
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
    /// Process executable-name substrings to watch (e.g. ["picoclaw"]). Empty = disabled.
    pub watch_processes: Vec<String>,
    /// Endpoints to TCP-probe for liveness/latency, each with the layer it belongs to.
    /// Empty = disabled.
    pub probe_targets: Vec<ProbeTargetCfg>,
    /// Root of crab-shell-proxy's data directory — the one containing `tenants/`.
    /// Empty = workspace discovery disabled.
    ///
    /// Replaces the old single-valued `session_dir`. That field could only ever name ONE
    /// workspace, while this stack creates one per (tenant, subscription, agent, user) at
    /// runtime, so it produced a metric that described a single member and read like it
    /// described the stack.
    pub data_root: String,
    /// How often to rescan the tenant tree for new or retired workspaces.
    pub discovery_interval_secs: u64,
    /// How often each discovered workspace's transcripts are re-read. Slower than
    /// discovery on purpose: finding a workspace is a directory glob, reading one is IO
    /// proportional to conversation history.
    pub session_interval_secs: u64,
    /// Label for the harness whose sessions are read (`harness.name`).
    pub session_source: String,
    /// A container's cgroup v2 directory to read (e.g.
    /// "/sys/fs/cgroup/system.slice/docker-<id>.scope"). Empty = disabled.
    pub container_cgroup: String,
    /// `container.id` label for the cgroup metrics. Empty → derived from the cgroup
    /// directory's name.
    pub container_id: String,
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
            watch_processes: Vec::new(),
            probe_targets: Vec::new(),
            data_root: String::new(),
            discovery_interval_secs: 30,
            session_interval_secs: 60,
            session_source: "picoclaw".to_owned(),
            container_cgroup: String::new(),
            // Empty → the collector derives the id from the cgroup directory's name.
            container_id: String::new(),
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
    pub fn discovery_interval(&self) -> Duration {
        Duration::from_secs(self.discovery_interval_secs.max(1))
    }
    pub fn session_interval(&self) -> Duration {
        Duration::from_secs(self.session_interval_secs.max(1))
    }
}
