//! OTLP output adapter (feature `otlp`). The only place that depends on the
//! `opentelemetry*` SDK (pre-1.0) — isolates the churn from the domain.
//!
//! v1 scope: **metrics** path via `SdkMeterProvider` + synchronous instruments
//! (host/self only emit metrics). OTLP logs/spans arrive once the ingest/harness
//! produces them.
//!
//! Modeling decision: the sources emit **sampled absolute values**. Mapping an absolute
//! `UpDownCounter` to `add()` would sum them — wrong. So absolute values (Gauge and
//! UpDownCounter) become a **Gauge** in OTLP. TODO: migrate additive metrics
//! (e.g. `system.memory.usage` by state) to **observable** instruments to preserve the
//! semconv's summation semantics.

use async_trait::async_trait;
use harnesssphere_domain::{
    AttrValue, Attributes, ExportError, MetricKind, Signal, SignalExporter,
};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

pub struct OtlpExporter {
    provider: SdkMeterProvider,
    meter: Meter,
    gauges: Mutex<HashMap<String, Gauge<f64>>>,
    counters: Mutex<HashMap<String, Counter<f64>>>,
    histograms: Mutex<HashMap<String, Histogram<f64>>>,
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

        Ok(OtlpExporter {
            provider,
            meter,
            gauges: Mutex::new(HashMap::new()),
            counters: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
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

#[async_trait]
impl SignalExporter for OtlpExporter {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError> {
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
                // v1: no OTLP path for logs/spans yet (host/self don't emit them).
                Signal::Log(_) | Signal::Span(_) => {
                    tracing::debug!("log/span signal ignored by OtlpExporter v1 (metrics only)");
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!(error = %e, "OtlpExporter shutdown/flush failed");
        }
    }
}
