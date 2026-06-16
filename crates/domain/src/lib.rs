//! `harnesssphere-domain` — o hexágono.
//!
//! Modelo de sinal canônico, ports (interfaces) e políticas puras. **Zero IO, zero
//! dependência de OpenTelemetry.** Toda lógica testável vive aqui.

pub mod policy;
pub mod ports;
pub mod signal;

pub use policy::{classify_failure, BreakerState, CircuitBreaker, FailureAction};
pub use ports::{
    CollectError, Criticality, ExportError, ProbeResult, SignalExporter, SignalSink, SignalSource,
    SourceDescriptor,
};
pub use signal::{
    AttrValue, Attributes, Layer, LogRecord, Metric, MetricKind, Severity, Signal, Span, SpanKind,
    SpanStatus,
};
