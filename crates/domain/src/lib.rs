//! `harnesssphere-domain` — the hexagon.
//!
//! Canonical signal model, ports (interfaces) and pure policies. **Zero IO, zero
//! OpenTelemetry dependency.** All testable logic lives here.

pub mod enrich;
pub mod policy;
pub mod ports;
pub mod signal;

pub use enrich::Enricher;
pub use policy::{classify_failure, BreakerState, CircuitBreaker, FailureAction};
pub use ports::{
    CollectError, Criticality, ExportError, ProbeResult, Receiver, ReceiverDescriptor, RecvError,
    SignalExporter, SignalSink, SignalSource, SourceDescriptor,
};
pub use signal::{
    AttrValue, Attributes, Layer, LogRecord, Metric, MetricKind, Severity, Signal, Span, SpanKind,
    SpanStatus,
};
