//! Ports do hexágono — interfaces que o domínio define e os adapters implementam.

use crate::signal::{Layer, Signal};
use std::time::Duration;
use thiserror::Error;

/// Criticidade de um source. Critical (Host/Self) falha persistente → processo encerra.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Criticality {
    Critical,
    Optional,
}

/// Metadados estáticos de um source.
#[derive(Debug, Clone)]
pub struct SourceDescriptor {
    pub name: &'static str,
    pub layer: Layer,
    pub criticality: Criticality,
    pub default_interval: Duration,
}

/// Resultado do probe inicial de disponibilidade do alvo.
#[derive(Debug)]
pub enum ProbeResult {
    /// Alvo presente; coleta habilitada.
    Ready,
    /// Alvo ausente/sem resposta (Optional → degrade; não é fatal).
    Unavailable(String),
    /// Não se aplica a este host (ex.: sem container).
    NotApplicable,
    /// Só Critical pode retornar — aborta o boot.
    Fatal(String),
}

#[derive(Debug, Error)]
pub enum CollectError {
    #[error("alvo indisponível: {0}")]
    Unavailable(String),
    #[error("falha de coleta: {0}")]
    Failed(String),
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("falha de export: {0}")]
    Failed(String),
}

/// Canal de saída dos sources para o pipeline do domínio.
///
/// Fire-and-forget e não-async: mantém o domínio livre de `tokio`. O adapter de runtime
/// fornece a implementação (canal bounded com política de descarte).
pub trait SignalSink: Send + Sync {
    fn emit(&self, signal: Signal);
}

/// Port driven: fonte de telemetria por *pull* (host, cgroup, scrape).
#[async_trait::async_trait]
pub trait SignalSource: Send + Sync + 'static {
    fn descriptor(&self) -> &SourceDescriptor;

    /// Detecta disponibilidade do alvo (uma vez, no boot).
    async fn probe(&mut self) -> ProbeResult;

    /// Um ciclo de coleta; emite sinais via `sink`. Erro é isolado pelo runtime.
    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError>;
}

/// Port driven: destino dos sinais (OTLP, stdout, ...).
#[async_trait::async_trait]
pub trait SignalExporter: Send + Sync + 'static {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError>;
    async fn shutdown(&self) {}
}
