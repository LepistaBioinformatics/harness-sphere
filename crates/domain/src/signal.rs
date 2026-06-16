//! Modelo de sinal canônico do HarnessSphere.
//!
//! Representação neutra de telemetria — independente de OpenTelemetry. O adapter de
//! export (`harnesssphere-export`) é o único lugar que traduz isto para OTLP, isolando
//! o domínio do churn das crates `opentelemetry*` (pré-1.0).

use std::time::SystemTime;

/// Camada monitorada (origem lógica do sinal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Host,
    Watcher,
    Container,
    Gateway,
    Harness,
    Tools,
    Api,
}

impl Layer {
    pub fn as_str(self) -> &'static str {
        match self {
            Layer::Host => "host",
            Layer::Watcher => "watcher",
            Layer::Container => "container",
            Layer::Gateway => "gateway",
            Layer::Harness => "harness",
            Layer::Tools => "tools",
            Layer::Api => "api",
        }
    }
}

/// Valor de atributo (subset OTel-compatível).
#[derive(Debug, Clone, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl From<&str> for AttrValue {
    fn from(v: &str) -> Self {
        AttrValue::Str(v.to_owned())
    }
}
impl From<String> for AttrValue {
    fn from(v: String) -> Self {
        AttrValue::Str(v)
    }
}
impl From<i64> for AttrValue {
    fn from(v: i64) -> Self {
        AttrValue::Int(v)
    }
}
impl From<f64> for AttrValue {
    fn from(v: f64) -> Self {
        AttrValue::Float(v)
    }
}
impl From<bool> for AttrValue {
    fn from(v: bool) -> Self {
        AttrValue::Bool(v)
    }
}

pub type Attributes = Vec<(String, AttrValue)>;

/// Tipo de instrumento métrico.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Counter,
    UpDownCounter,
    Histogram,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub kind: MetricKind,
    pub value: f64,
    pub unit: Option<String>,
    pub attributes: Attributes,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub severity: Severity,
    pub body: String,
    pub attributes: Attributes,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub name: String,
    pub kind: SpanKind,
    pub start: SystemTime,
    pub end: SystemTime,
    pub status: SpanStatus,
    pub attributes: Attributes,
}

/// Sinal canônico — unidade que flui dos sources/receivers até o exporter.
#[derive(Debug, Clone)]
pub enum Signal {
    Metric(Metric),
    Log(LogRecord),
    Span(Span),
}

impl Signal {
    /// Anexa `(key, value)` aos atributos do sinal (usado pelo Enricher).
    pub fn push_attr(&mut self, key: impl Into<String>, value: impl Into<AttrValue>) {
        let attrs = match self {
            Signal::Metric(m) => &mut m.attributes,
            Signal::Log(l) => &mut l.attributes,
            Signal::Span(s) => &mut s.attributes,
        };
        attrs.push((key.into(), value.into()));
    }
}

/// Construtores ergonômicos para os adapters de fonte.
impl Metric {
    pub fn now(name: impl Into<String>, kind: MetricKind, value: f64) -> Self {
        Metric {
            name: name.into(),
            kind,
            value,
            unit: None,
            attributes: Vec::new(),
            timestamp: SystemTime::now(),
        }
    }

    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<AttrValue>) -> Self {
        self.attributes.push((key.into(), value.into()));
        self
    }

    pub fn into_signal(self) -> Signal {
        Signal::Metric(self)
    }
}
