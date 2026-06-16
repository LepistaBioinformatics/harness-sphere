//! `harnesssphere-export` — driven output adapters.
//!
//! The only place allowed to depend on external telemetry SDKs.
//! - `StdoutExporter` (default): debug, proves the end-to-end pipeline without a network.
//! - `OtlpExporter` (feature `otlp`): OTLP/gRPC via SDK 0.32, metrics path.
//!   Confines the `opentelemetry*` crates (pre-1.0) — swapping the adapter touches neither
//!   domain nor runtime.

mod stdout;
pub use stdout::StdoutExporter;

#[cfg(feature = "otlp")]
mod otlp;
#[cfg(feature = "otlp")]
pub use otlp::OtlpExporter;
