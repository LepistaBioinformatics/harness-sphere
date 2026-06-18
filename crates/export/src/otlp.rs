//! OTLP output adapter (feature `otlp`). The only place that depends on the
//! `opentelemetry*` SDK (pre-1.0) — isolates the churn from the domain.
//!
//! Scope: **metrics** via `SdkMeterProvider` + synchronous instruments; **traces**,
//! **logs**, and ingested **histograms** via plain OTLP/gRPC clients. Spans/logs/histograms
//! are grouped by `service.name` onto the Resource so SigNoz's Services/APM view works.
//!
//! Modeling decision: the sources emit **sampled absolute values**. Mapping an absolute
//! `UpDownCounter` to `add()` would sum them — wrong. So absolute values (Gauge and
//! UpDownCounter) become a **Gauge** in OTLP. TODO: migrate additive metrics
//! (e.g. `system.memory.usage` by state) to **observable** instruments to preserve the
//! semconv's summation semantics.

use async_trait::async_trait;
use harnesssphere_domain::{
    AttrValue, Attributes, ExportError, HistogramPoint, LogRecord, MetricKind, Severity, Signal,
    SignalExporter, Span, SpanKind, SpanStatus,
};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_client::LogsServiceClient, ExportLogsServiceRequest,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_client::TraceServiceClient, ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue as PbKeyValue};
use opentelemetry_proto::tonic::logs::v1::{
    LogRecord as PbLogRecord, ResourceLogs, ScopeLogs, SeverityNumber,
};
use opentelemetry_proto::tonic::metrics::v1::{
    metric as pb_metric, AggregationTemporality, Histogram as PbHistogram, HistogramDataPoint,
    Metric as PbMetric, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource as PbResource;
use opentelemetry_proto::tonic::trace::v1::{
    span as pb_span, status as pb_status, ResourceSpans, ScopeSpans, Span as PbSpan,
    Status as PbStatus,
};
use tonic::transport::Channel;

pub struct OtlpExporter {
    provider: SdkMeterProvider,
    meter: Meter,
    gauges: Mutex<HashMap<String, Gauge<f64>>>,
    counters: Mutex<HashMap<String, Counter<f64>>>,
    histograms: Mutex<HashMap<String, Histogram<f64>>>,
    trace_client: TraceServiceClient<Channel>,
    logs_client: LogsServiceClient<Channel>,
    metrics_client: MetricsServiceClient<Channel>,
}

impl OtlpExporter {
    /// Creates the OTLP/gRPC provider (tonic). `endpoint` e.g. `http://localhost:4317`.
    /// `export_interval` controls the cadence of the SDK's periodic reader.
    pub fn new(
        endpoint: &str,
        service_name: &str,
        host_name: &str,
        export_interval: Duration,
    ) -> Result<Self, ExportError> {
        let metric_exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| ExportError::Failed(format!("init OTLP metric exporter: {e}")))?;

        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .with_attribute(KeyValue::new("host.name", host_name.to_owned()))
            .build();

        let reader = PeriodicReader::builder(metric_exporter)
            .with_interval(export_interval)
            .build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();

        let meter = provider.meter("harnesssphere");

        // Spans/logs/histograms go out over plain OTLP/gRPC clients (lazy connect, no await).
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ExportError::Failed(format!("bad OTLP endpoint: {e}")))?
            .timeout(Duration::from_secs(5))
            .connect_lazy();
        let trace_client = TraceServiceClient::new(channel.clone());
        let logs_client = LogsServiceClient::new(channel.clone());
        let metrics_client = MetricsServiceClient::new(channel);

        Ok(OtlpExporter {
            provider,
            meter,
            gauges: Mutex::new(HashMap::new()),
            counters: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
            trace_client,
            logs_client,
            metrics_client,
        })
    }

    fn record_gauge(&self, name: &str, unit: &Option<String>, value: f64, attrs: &[KeyValue]) {
        let mut map = self.gauges.lock().unwrap();
        let g = map.entry(name.to_owned()).or_insert_with(|| {
            let mut b = self.meter.f64_gauge(name.to_owned());
            if let Some(u) = unit {
                b = b.with_unit(u.clone());
            }
            b.build()
        });
        g.record(value, attrs);
    }

    fn add_counter(&self, name: &str, unit: &Option<String>, value: f64, attrs: &[KeyValue]) {
        let mut map = self.counters.lock().unwrap();
        let c = map.entry(name.to_owned()).or_insert_with(|| {
            let mut b = self.meter.f64_counter(name.to_owned());
            if let Some(u) = unit {
                b = b.with_unit(u.clone());
            }
            b.build()
        });
        c.add(value, attrs);
    }

    fn record_histogram(&self, name: &str, unit: &Option<String>, value: f64, attrs: &[KeyValue]) {
        let mut map = self.histograms.lock().unwrap();
        let h = map.entry(name.to_owned()).or_insert_with(|| {
            let mut b = self.meter.f64_histogram(name.to_owned());
            if let Some(u) = unit {
                b = b.with_unit(u.clone());
            }
            b.build()
        });
        h.record(value, attrs);
    }
}

fn to_keyvalues(attrs: &Attributes) -> Vec<KeyValue> {
    attrs
        .iter()
        .map(|(k, v)| match v {
            AttrValue::Str(s) => KeyValue::new(k.clone(), s.clone()),
            AttrValue::Int(i) => KeyValue::new(k.clone(), *i),
            AttrValue::Float(f) => KeyValue::new(k.clone(), *f),
            AttrValue::Bool(b) => KeyValue::new(k.clone(), *b),
        })
        .collect()
}

// --- trace export helpers (canonical Span -> OTLP proto) ---

fn nanos(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn pb_kind(k: SpanKind) -> i32 {
    match k {
        SpanKind::Internal => pb_span::SpanKind::Internal as i32,
        SpanKind::Client => pb_span::SpanKind::Client as i32,
        SpanKind::Server => pb_span::SpanKind::Server as i32,
    }
}

fn pb_status_of(s: SpanStatus) -> Option<PbStatus> {
    let code = match s {
        SpanStatus::Unset => pb_status::StatusCode::Unset,
        SpanStatus::Ok => pb_status::StatusCode::Ok,
        SpanStatus::Error => pb_status::StatusCode::Error,
    };
    Some(PbStatus {
        message: String::new(),
        code: code as i32,
    })
}

fn pb_attrs(attrs: &Attributes) -> Vec<PbKeyValue> {
    attrs
        .iter()
        .map(|(k, v)| PbKeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(match v {
                    AttrValue::Str(s) => any_value::Value::StringValue(s.clone()),
                    AttrValue::Int(i) => any_value::Value::IntValue(*i),
                    AttrValue::Float(f) => any_value::Value::DoubleValue(*f),
                    AttrValue::Bool(b) => any_value::Value::BoolValue(*b),
                }),
            }),
            ..Default::default()
        })
        .collect()
}

fn attr_str(attrs: &Attributes, key: &str) -> Option<String> {
    attrs.iter().find_map(|(k, v)| match v {
        AttrValue::Str(s) if k == key => Some(s.clone()),
        _ => None,
    })
}

fn res_kv(key: &str, val: &str) -> PbKeyValue {
    PbKeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.to_owned())),
        }),
        ..Default::default()
    }
}

/// Groups spans by `service.name` so each group becomes a ResourceSpans with the service
/// identity (and host.name) on the **Resource** — required for SigNoz's Services/APM view.
fn build_trace_request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    let mut by_service: HashMap<String, Vec<PbSpan>> = HashMap::new();
    let mut host_of: HashMap<String, String> = HashMap::new();
    for sp in spans {
        let service = attr_str(&sp.attributes, "service.name").unwrap_or_else(|| "unknown".into());
        if let Some(h) = attr_str(&sp.attributes, "host.name") {
            host_of.entry(service.clone()).or_insert(h);
        }
        let pb = PbSpan {
            trace_id: sp.trace_id,
            span_id: sp.span_id,
            parent_span_id: sp.parent_span_id,
            name: sp.name,
            kind: pb_kind(sp.kind),
            start_time_unix_nano: nanos(sp.start),
            end_time_unix_nano: nanos(sp.end),
            attributes: pb_attrs(&sp.attributes),
            status: pb_status_of(sp.status),
            ..Default::default()
        };
        by_service.entry(service).or_default().push(pb);
    }

    let resource_spans = by_service
        .into_iter()
        .map(|(service, spans)| {
            let mut attributes = vec![res_kv("service.name", &service)];
            if let Some(h) = host_of.get(&service) {
                attributes.push(res_kv("host.name", h));
            }
            ResourceSpans {
                resource: Some(PbResource {
                    attributes,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();

    ExportTraceServiceRequest { resource_spans }
}

// --- log export helpers ---

fn pb_severity(s: Severity) -> i32 {
    (match s {
        Severity::Trace => SeverityNumber::Trace,
        Severity::Debug => SeverityNumber::Debug,
        Severity::Info => SeverityNumber::Info,
        Severity::Warn => SeverityNumber::Warn,
        Severity::Error => SeverityNumber::Error,
    }) as i32
}

fn build_logs_request(logs: Vec<LogRecord>) -> ExportLogsServiceRequest {
    let mut by_service: HashMap<String, Vec<PbLogRecord>> = HashMap::new();
    let mut host_of: HashMap<String, String> = HashMap::new();
    for l in logs {
        let service = attr_str(&l.attributes, "service.name").unwrap_or_else(|| "unknown".into());
        if let Some(h) = attr_str(&l.attributes, "host.name") {
            host_of.entry(service.clone()).or_insert(h);
        }
        let rec = PbLogRecord {
            time_unix_nano: nanos(l.timestamp),
            observed_time_unix_nano: nanos(l.timestamp),
            severity_number: pb_severity(l.severity),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(l.body)),
            }),
            attributes: pb_attrs(&l.attributes),
            trace_id: l.trace_id,
            span_id: l.span_id,
            ..Default::default()
        };
        by_service.entry(service).or_default().push(rec);
    }
    let resource_logs = by_service
        .into_iter()
        .map(|(service, log_records)| {
            let mut attributes = vec![res_kv("service.name", &service)];
            if let Some(h) = host_of.get(&service) {
                attributes.push(res_kv("host.name", h));
            }
            ResourceLogs {
                resource: Some(PbResource {
                    attributes,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest { resource_logs }
}

// --- histogram export helpers (forwards a pre-aggregated histogram as an OTLP metric) ---

fn build_histogram_request(histos: Vec<HistogramPoint>) -> ExportMetricsServiceRequest {
    let mut by_service: HashMap<String, Vec<PbMetric>> = HashMap::new();
    let mut host_of: HashMap<String, String> = HashMap::new();
    for hp in histos {
        let service = attr_str(&hp.attributes, "service.name").unwrap_or_else(|| "unknown".into());
        if let Some(h) = attr_str(&hp.attributes, "host.name") {
            host_of.entry(service.clone()).or_insert(h);
        }
        let dp = HistogramDataPoint {
            attributes: pb_attrs(&hp.attributes),
            start_time_unix_nano: nanos(hp.start_time),
            time_unix_nano: nanos(hp.timestamp),
            count: hp.count,
            sum: Some(hp.sum),
            bucket_counts: hp.bucket_counts,
            explicit_bounds: hp.explicit_bounds,
            min: hp.min,
            max: hp.max,
            ..Default::default()
        };
        let metric = PbMetric {
            name: hp.name,
            unit: hp.unit.unwrap_or_default(),
            data: Some(pb_metric::Data::Histogram(PbHistogram {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        };
        by_service.entry(service).or_default().push(metric);
    }
    let resource_metrics = by_service
        .into_iter()
        .map(|(service, metrics)| {
            let mut attributes = vec![res_kv("service.name", &service)];
            if let Some(h) = host_of.get(&service) {
                attributes.push(res_kv("host.name", h));
            }
            ResourceMetrics {
                resource: Some(PbResource {
                    attributes,
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest { resource_metrics }
}

#[async_trait]
impl SignalExporter for OtlpExporter {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError> {
        let mut spans: Vec<Span> = Vec::new();
        let mut logs: Vec<LogRecord> = Vec::new();
        let mut histos: Vec<HistogramPoint> = Vec::new();
        for sig in batch {
            match sig {
                Signal::Metric(m) => {
                    let attrs = to_keyvalues(&m.attributes);
                    match m.kind {
                        // Sampled absolute values → Gauge (see note at the top).
                        MetricKind::Gauge | MetricKind::UpDownCounter => {
                            self.record_gauge(&m.name, &m.unit, m.value, &attrs)
                        }
                        MetricKind::Counter => self.add_counter(&m.name, &m.unit, m.value, &attrs),
                        MetricKind::Histogram => {
                            self.record_histogram(&m.name, &m.unit, m.value, &attrs)
                        }
                    }
                }
                Signal::Histogram(h) => histos.push(h),
                Signal::Span(s) => spans.push(s),
                Signal::Log(l) => logs.push(l),
            }
        }

        if !spans.is_empty() {
            let req = build_trace_request(spans);
            let mut client = self.trace_client.clone();
            client
                .export(req)
                .await
                .map_err(|e| ExportError::Failed(format!("OTLP trace export: {e}")))?;
        }
        if !logs.is_empty() {
            let req = build_logs_request(logs);
            let mut client = self.logs_client.clone();
            client
                .export(req)
                .await
                .map_err(|e| ExportError::Failed(format!("OTLP logs export: {e}")))?;
        }
        if !histos.is_empty() {
            let req = build_histogram_request(histos);
            let mut client = self.metrics_client.clone();
            client
                .export(req)
                .await
                .map_err(|e| ExportError::Failed(format!("OTLP histogram export: {e}")))?;
        }
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!(error = %e, "OtlpExporter shutdown/flush failed");
        }
    }
}
