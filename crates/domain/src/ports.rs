//! Hexagon ports — interfaces the domain defines and the adapters implement.

use crate::signal::{Layer, Signal};
use std::time::Duration;
use thiserror::Error;

/// Criticality of a source. Critical (Host/Self) persistent failure → process exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Criticality {
    Critical,
    Optional,
}

/// Static metadata for a source.
#[derive(Debug, Clone)]
pub struct SourceDescriptor {
    pub name: &'static str,
    pub layer: Layer,
    pub criticality: Criticality,
    pub default_interval: Duration,
}

/// Result of the initial target availability probe.
#[derive(Debug)]
pub enum ProbeResult {
    /// Target present; collection enabled.
    Ready,
    /// Target absent/unresponsive (Optional → degrade; not fatal).
    Unavailable(String),
    /// Does not apply to this host (e.g. no container).
    NotApplicable,
    /// Only Critical may return this — aborts boot.
    Fatal(String),
}

#[derive(Debug, Error)]
pub enum CollectError {
    #[error("target unavailable: {0}")]
    Unavailable(String),
    #[error("collection failed: {0}")]
    Failed(String),
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("export failed: {0}")]
    Failed(String),
}

/// Output channel from the sources to the domain pipeline.
///
/// Fire-and-forget and non-async: keeps the domain free of `tokio`. The runtime adapter
/// provides the implementation (bounded channel with a drop policy).
pub trait SignalSink: Send + Sync {
    fn emit(&self, signal: Signal);
}

/// Driven port: *pull*-based telemetry source (host, cgroup, scrape).
#[async_trait::async_trait]
pub trait SignalSource: Send + Sync + 'static {
    fn descriptor(&self) -> &SourceDescriptor;

    /// Detects target availability (once, at boot).
    async fn probe(&mut self) -> ProbeResult;

    /// One collection cycle; emits signals via `sink`. Errors are isolated by the runtime.
    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError>;
}

/// Driven port: signal destination (OTLP, stdout, ...).
#[async_trait::async_trait]
pub trait SignalExporter: Send + Sync + 'static {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError>;
    async fn shutdown(&self) {}
}
