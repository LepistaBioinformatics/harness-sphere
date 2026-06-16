//! `harnesssphere-ingest` — driving adapter: local OTLP receiver (push).
//!
//! OpenClaw/Hermes push OTLP here; we convert it to the canonical signal model, enrich
//! it with host context, and emit it into the same pipeline the collectors feed. This is
//! the heart of the "single pane" role: AI telemetry passes through HarnessSphere and
//! gains host context on the way out.
//!
//! v1 scope: OTLP/gRPC **metrics** (Gauge + Sum). Traces/logs ingest is a planned
//! extension.

use async_trait::async_trait;
use harnesssphere_domain::{
    AttrValue, Enricher, Metric, MetricKind, Receiver, ReceiverDescriptor, RecvError, Signal,
    SignalSink,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{any_value, KeyValue as PbKeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point, NumberDataPoint};

pub struct OtlpReceiver {
    descriptor: ReceiverDescriptor,
    enricher: Arc<Enricher>,
}

impl OtlpReceiver {
    /// `endpoint` e.g. `0.0.0.0:4317`; `host_name` is stamped onto every ingested signal.
    pub fn new(endpoint: impl Into<String>, host_name: impl Into<String>) -> Self {
        OtlpReceiver {
            descriptor: ReceiverDescriptor {
                name: "otlp-ingest",
                endpoint: endpoint.into(),
            },
            enricher: Arc::new(Enricher::new(host_name)),
        }
    }
}

#[async_trait]
impl Receiver for OtlpReceiver {
    fn descriptor(&self) -> &ReceiverDescriptor {
        &self.descriptor
    }

    async fn serve(self: Box<Self>, sink: Arc<dyn SignalSink>) -> Result<(), RecvError> {
        let addr: SocketAddr = self
            .descriptor
            .endpoint
            .parse()
            .map_err(|e| RecvError::Bind(format!("bad endpoint {}: {e}", self.descriptor.endpoint)))?;

        let svc = MetricsSvc {
            sink,
            enricher: self.enricher.clone(),
        };
        Server::builder()
            .add_service(MetricsServiceServer::new(svc))
            .serve(addr)
            .await
            .map_err(|e| RecvError::Failed(e.to_string()))
    }
}

struct MetricsSvc {
    sink: Arc<dyn SignalSink>,
    enricher: Arc<Enricher>,
}

#[tonic::async_trait]
impl MetricsService for MetricsSvc {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut signals = convert(request.into_inner());
        for sig in &mut signals {
            self.enricher.enrich(sig);
        }
        let n = signals.len();
        for sig in signals {
            self.sink.emit(sig);
        }
        tracing::debug!(count = n, "ingested OTLP metrics");
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// Walks the OTLP request and flattens it into canonical signals.
fn convert(req: ExportMetricsServiceRequest) -> Vec<Signal> {
    let mut out = Vec::new();
    for rm in req.resource_metrics {
        for sm in rm.scope_metrics {
            for m in sm.metrics {
                let name = m.name;
                let unit = if m.unit.is_empty() { None } else { Some(m.unit) };
                match m.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in g.data_points {
                            push_number(&mut out, &name, &unit, MetricKind::Gauge, dp);
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        let kind = if s.is_monotonic {
                            MetricKind::Counter
                        } else {
                            MetricKind::UpDownCounter
                        };
                        for dp in s.data_points {
                            push_number(&mut out, &name, &unit, kind, dp);
                        }
                    }
                    // Histogram / ExponentialHistogram / Summary: not mapped in v1.
                    _ => {}
                }
            }
        }
    }
    out
}

fn push_number(
    out: &mut Vec<Signal>,
    name: &str,
    unit: &Option<String>,
    kind: MetricKind,
    dp: NumberDataPoint,
) {
    let value = match dp.value {
        Some(number_data_point::Value::AsDouble(d)) => d,
        Some(number_data_point::Value::AsInt(i)) => i as f64,
        None => return,
    };
    let mut metric = Metric::now(name, kind, value);
    if let Some(u) = unit {
        metric = metric.with_unit(u.clone());
    }
    for kv in dp.attributes {
        if let Some((k, v)) = to_attr(kv) {
            metric = metric.attr(k, v);
        }
    }
    out.push(metric.into_signal());
}

fn to_attr(kv: PbKeyValue) -> Option<(String, AttrValue)> {
    let value = kv.value?.value?;
    let av = match value {
        any_value::Value::StringValue(s) => AttrValue::Str(s),
        any_value::Value::IntValue(i) => AttrValue::Int(i),
        any_value::Value::DoubleValue(d) => AttrValue::Float(d),
        any_value::Value::BoolValue(b) => AttrValue::Bool(b),
        _ => return None,
    };
    Some((kv.key, av))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric as PbMetric, ResourceMetrics, ScopeMetrics, Sum,
    };

    fn double_dp(value: f64, attrs: Vec<PbKeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes: attrs,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn request_with(metric: PbMetric) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn maps_gauge_with_attribute_and_unit() {
        let kv = PbKeyValue {
            key: "system.memory.state".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("used".into())),
            }),
            ..Default::default()
        };
        let req = request_with(PbMetric {
            name: "system.memory.usage".into(),
            unit: "By".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![double_dp(123.0, vec![kv])],
            })),
            ..Default::default()
        });

        let signals = convert(req);
        assert_eq!(signals.len(), 1);
        let Signal::Metric(m) = &signals[0] else {
            panic!("expected a metric");
        };
        assert_eq!(m.name, "system.memory.usage");
        assert_eq!(m.kind, MetricKind::Gauge);
        assert_eq!(m.value, 123.0);
        assert_eq!(m.unit.as_deref(), Some("By"));
        assert!(m.attributes.iter().any(|(k, _)| k == "system.memory.state"));
    }

    #[test]
    fn monotonic_sum_maps_to_counter_nonmonotonic_to_updown() {
        let mono = request_with(PbMetric {
            name: "some.counter".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![double_dp(1.0, vec![])],
                is_monotonic: true,
                ..Default::default()
            })),
            ..Default::default()
        });
        let nonmono = request_with(PbMetric {
            name: "some.level".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![double_dp(1.0, vec![])],
                is_monotonic: false,
                ..Default::default()
            })),
            ..Default::default()
        });

        let Signal::Metric(c) = &convert(mono)[0] else {
            panic!("metric");
        };
        assert_eq!(c.kind, MetricKind::Counter);
        let Signal::Metric(l) = &convert(nonmono)[0] else {
            panic!("metric");
        };
        assert_eq!(l.kind, MetricKind::UpDownCounter);
    }
}
