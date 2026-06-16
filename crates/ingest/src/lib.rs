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
    AttrValue, Attributes, Enricher, Metric, MetricKind, Receiver, ReceiverDescriptor, RecvError,
    Signal, SignalSink,
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
///
/// Resource-level attributes (`service.name`, `gen_ai.provider.name`, `gen_ai.request.model`,
/// …) carry the origin identity, so they are merged onto every data point — otherwise an
/// ingested metric would lose which service/model it came from.
fn convert(req: ExportMetricsServiceRequest) -> Vec<Signal> {
    let mut out = Vec::new();
    for rm in req.resource_metrics {
        let resource_attrs: Attributes = rm
            .resource
            .map(|r| r.attributes.into_iter().filter_map(to_attr).collect())
            .unwrap_or_default();
        for sm in rm.scope_metrics {
            for m in sm.metrics {
                let name = m.name;
                let unit = if m.unit.is_empty() { None } else { Some(m.unit) };
                match m.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in g.data_points {
                            push_number(&mut out, &name, &unit, MetricKind::Gauge, dp, &resource_attrs);
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        let kind = if s.is_monotonic {
                            MetricKind::Counter
                        } else {
                            MetricKind::UpDownCounter
                        };
                        for dp in s.data_points {
                            push_number(&mut out, &name, &unit, kind, dp, &resource_attrs);
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
    resource_attrs: &Attributes,
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
    for (k, v) in resource_attrs {
        metric = metric.attr(k.clone(), v.clone());
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
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn str_kv(key: &str, val: &str) -> PbKeyValue {
        PbKeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.into())),
            }),
            ..Default::default()
        }
    }

    fn double_dp(value: f64, attrs: Vec<PbKeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes: attrs,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn request_with(metric: PbMetric, resource_attrs: Vec<PbKeyValue>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn maps_gauge_keeps_unit_dp_attr_and_resource_identity() {
        let req = request_with(
            PbMetric {
                name: "system.memory.usage".into(),
                unit: "By".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![double_dp(123.0, vec![str_kv("system.memory.state", "used")])],
                })),
                ..Default::default()
            },
            // origin identity lives on the Resource — must survive ingest
            vec![str_kv("service.name", "openclaw")],
        );

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
        // the dropped-resource bug regression check:
        assert!(
            m.attributes
                .iter()
                .any(|(k, v)| k == "service.name" && matches!(v, AttrValue::Str(s) if s == "openclaw")),
            "resource-level service.name must be merged onto the data point"
        );
    }

    #[test]
    fn monotonic_sum_maps_to_counter_nonmonotonic_to_updown() {
        let mono = request_with(
            PbMetric {
                name: "some.counter".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![double_dp(1.0, vec![])],
                    is_monotonic: true,
                    ..Default::default()
                })),
                ..Default::default()
            },
            vec![],
        );
        let nonmono = request_with(
            PbMetric {
                name: "some.level".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![double_dp(1.0, vec![])],
                    is_monotonic: false,
                    ..Default::default()
                })),
                ..Default::default()
            },
            vec![],
        );

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
