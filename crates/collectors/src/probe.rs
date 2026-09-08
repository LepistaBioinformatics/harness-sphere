//! Endpoint probe collector (Optional) — TCP liveness + latency of host endpoints.
//!
//! A black-box health check for a co-located service (e.g. the PicoClaw gateway on
//! `localhost:18790`) that exposes no metrics of its own: we open a TCP connection and
//! record up/down and connect latency. Maps to `harnesssphere.endpoint.*`.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor, LAYER_ATTR,
};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// One probe target and the layer it belongs to.
///
/// The layer is per target, not per collector: this one source probes the gateway, the
/// proxy and the webapp, which are three different layers. Stamping the collector's own
/// layer on all of them — which is what it used to do — made three of the six layers
/// unanswerable by construction.
#[derive(Debug, Clone)]
pub struct ProbeTarget {
    /// `host:port`. A port is required; there is no default.
    pub address: String,
    pub layer: Layer,
}

impl ProbeTarget {
    pub fn new(address: impl Into<String>, layer: Layer) -> Self {
        ProbeTarget {
            address: address.into(),
            layer,
        }
    }
}

pub struct EndpointProbeCollector {
    descriptor: SourceDescriptor,
    targets: Vec<ProbeTarget>,
    connect_timeout: Duration,
}

impl EndpointProbeCollector {
    pub fn new(targets: Vec<ProbeTarget>, interval: Duration) -> Self {
        EndpointProbeCollector {
            descriptor: SourceDescriptor {
                name: "endpoint-probe".to_owned(),
                // The descriptor's layer is only a fallback for signals this collector
                // emits about ITSELF; every probe signal carries its target's layer.
                layer: Layer::Watcher,
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
        for target in &self.targets {
            let started = Instant::now();
            let up = matches!(
                timeout(self.connect_timeout, TcpStream::connect(&target.address)).await,
                Ok(Ok(_))
            );
            let elapsed = started.elapsed().as_secs_f64();
            let layer = target.layer.as_str();
            // A target that is down emits an honest 0 rather than going absent -- which is
            // why this collector needs no startup ordering against the things it probes.
            sink.emit(
                Metric::now(
                    "harnesssphere.endpoint.up",
                    MetricKind::Gauge,
                    if up { 1.0 } else { 0.0 },
                )
                .attr("server.address", target.address.clone())
                .attr(LAYER_ATTR, layer.to_owned())
                .into_signal(),
            );
            sink.emit(
                Metric::now("harnesssphere.endpoint.probe.duration", MetricKind::Gauge, elapsed)
                    .with_unit("s")
                    .attr("server.address", target.address.clone())
                    .attr(LAYER_ATTR, layer.to_owned())
                    .into_signal(),
            );
        }
        Ok(())
    }
}
