//! Adapter de saída OTLP (feature `otlp`). Único lugar que depende do SDK
//! `opentelemetry*` (pré-1.0) — isola o churn do domínio.
//!
//! Escopo v1: caminho de **métricas** via `SdkMeterProvider` + instrumentos síncronos
//! (host/self só emitem métricas). Logs/Spans OTLP chegam quando o ingest/harness os
//! produzir.
//!
//! Decisão de modelagem: as fontes emitem **valores absolutos amostrados**. Mapear um
//! `UpDownCounter` absoluto para `add()` somaria — errado. Então valores absolutos
//! (Gauge e UpDownCounter) viram **Gauge** no OTLP. TODO: migrar métricas aditivas
//! (ex.: `system.memory.usage` por estado) para instrumentos **observáveis** para
//! preservar a semântica de soma da semconv.

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
    /// Cria o provider OTLP/gRPC (tonic). `endpoint` ex.: `http://localhost:4317`.
    /// `export_interval` controla a cadência do reader periódico do SDK.
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
                        // Valores absolutos amostrados → Gauge (ver nota no topo).
                        MetricKind::Gauge | MetricKind::UpDownCounter => {
                            self.record_gauge(&m.name, &m.unit, m.value, &attrs)
                        }
                        MetricKind::Counter => self.add_counter(&m.name, &m.unit, m.value, &attrs),
                        MetricKind::Histogram => {
                            self.record_histogram(&m.name, &m.unit, m.value, &attrs)
                        }
                    }
                }
                // v1: ainda sem caminho OTLP de logs/spans (host/self não os emitem).
                Signal::Log(_) | Signal::Span(_) => {
                    tracing::debug!("sinal log/span ignorado pelo OtlpExporter v1 (só métricas)");
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!(error = %e, "falha no shutdown/flush do OtlpExporter");
        }
    }
}
