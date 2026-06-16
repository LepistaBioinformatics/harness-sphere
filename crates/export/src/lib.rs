//! `harnesssphere-export` — driven adapters de saída.
//!
//! O único lugar autorizado a depender de SDKs externos de telemetria.
//! - `StdoutExporter` (default): debug, prova o pipeline end-to-end sem rede.
//! - `OtlpExporter` (feature `otlp`): OTLP/gRPC via SDK 0.32, caminho de métricas.
//!   Confina as crates `opentelemetry*` (pré-1.0) — trocar o adapter não toca domínio
//!   nem runtime.

mod stdout;
pub use stdout::StdoutExporter;

#[cfg(feature = "otlp")]
mod otlp;
#[cfg(feature = "otlp")]
pub use otlp::OtlpExporter;
