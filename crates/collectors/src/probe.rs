//! Endpoint probe collector (Optional) — TCP liveness + latency of host endpoints.
//!
//! A black-box health check for a co-located service (e.g. the PicoClaw gateway on
//! `localhost:18790`) that exposes no metrics of its own: we open a TCP connection and
//! record up/down and connect latency. Maps to `harnesssphere.endpoint.*`.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub struct EndpointProbeCollector {
    descriptor: SourceDescriptor,
    /// `host:port` targets to probe.
    targets: Vec<String>,
    connect_timeout: Duration,
}

impl EndpointProbeCollector {
    pub fn new(targets: Vec<String>, interval: Duration) -> Self {
        EndpointProbeCollector {
            descriptor: SourceDescriptor {
                name: "endpoint-probe",
                layer: Layer::Gateway,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            targets,
            connect_timeout: Duration::from_secs(2),
        }
    }
}

#[async_trait]
impl SignalSource for EndpointProbeCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        if self.targets.is_empty() {
            ProbeResult::NotApplicable
        } else {
            ProbeResult::Ready
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        for addr in &self.targets {
            let started = Instant::now();
            let up = matches!(
                timeout(self.connect_timeout, TcpStream::connect(addr)).await,
                Ok(Ok(_))
            );
            let elapsed = started.elapsed().as_secs_f64();
            sink.emit(
                Metric::now(
                    "harnesssphere.endpoint.up",
                    MetricKind::Gauge,
                    if up { 1.0 } else { 0.0 },
                )
                .attr("server.address", addr.clone())
                .into_signal(),
            );
            sink.emit(
                Metric::now("harnesssphere.endpoint.probe.duration", MetricKind::Gauge, elapsed)
                    .with_unit("s")
                    .attr("server.address", addr.clone())
                    .into_signal(),
            );
        }
        Ok(())
    }
}
