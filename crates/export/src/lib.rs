//! `harnesssphere-export` — driven adapters de saída.
//!
//! O único lugar autorizado a depender de SDKs externos de telemetria. Sprint 1 entrega
//! o `StdoutExporter` (debug, prova o pipeline end-to-end sem rede). O `OtlpExporter`
//! (feature `otlp`) confina as crates `opentelemetry*` (pré-1.0) e entra no próximo
//! sprint — trocar o adapter não toca domínio nem runtime.

mod stdout;
pub use stdout::StdoutExporter;

#[cfg(feature = "otlp")]
mod otlp;
#[cfg(feature = "otlp")]
pub use otlp::OtlpExporter;
