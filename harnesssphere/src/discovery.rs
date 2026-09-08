//! Workspace discovery — the control plane that keeps the supervisor's source set in step
//! with a stack that creates and destroys workspaces at runtime.
//!
//! **This is a task, not a `SignalSource`** (DEC-20). It drives the supervisor over a
//! dedicated command channel. Routing control through the signal sink would make
//! "stop watching this container" inherit the drain's batching latency, the sink's
//! drop-newest backpressure and the circuit breaker — all correct for telemetry, all wrong
//! here.
//!
//! It lives in the composition root because it is composition: it is the only place that
//! knows both `SupervisorCmd` (runtime) and `SessionCollector` (collectors), and neither
//! crate depends on the other.

use async_trait::async_trait;
use harnesssphere_collectors::{discover, SessionCollector, Workspace};
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use harnesssphere_runtime::SupervisorCmd;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Publishes what discovery last saw.
///
/// Discovery reports on itself through an ordinary source rather than by holding a sink.
/// A sink clone held by a detached task would keep the signal channel open past shutdown,
/// so every exit would wait out the drain's flush timeout and log a spurious warning. A
/// counter plus a source has neither problem and gets its layer stamped like anything else.
pub struct DiscoveryStats {
    descriptor: SourceDescriptor,
    workspaces: Arc<AtomicU64>,
    scans: Arc<AtomicU64>,
}

impl DiscoveryStats {
    pub fn new(workspaces: Arc<AtomicU64>, scans: Arc<AtomicU64>, interval: Duration) -> Self {
        DiscoveryStats {
            descriptor: SourceDescriptor {
                name: "discovery".to_owned(),
                layer: Layer::Watcher,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            workspaces,
            scans,
        }
    }
}

#[async_trait]
impl SignalSource for DiscoveryStats {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }
    async fn probe(&mut self) -> ProbeResult {
        ProbeResult::Ready
    }
    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        sink.emit(
            Metric::now(
                "harnesssphere.discovery.workspaces",
                MetricKind::Gauge,
                self.workspaces.load(Ordering::Relaxed) as f64,
            )
            .into_signal(),
        );
        // A scan count that stops advancing is how you tell "no workspaces" from
        // "discovery died" -- two states that look identical on the gauge above.
        sink.emit(
            Metric::now(
                "harnesssphere.discovery.scans",
                MetricKind::Gauge,
                self.scans.load(Ordering::Relaxed) as f64,
            )
            .into_signal(),
        );
        Ok(())
    }
}

pub struct Discovery {
    pub data_root: PathBuf,
    pub interval: Duration,
    pub session_interval: Duration,
    pub harness_name: String,
    pub workspaces: Arc<AtomicU64>,
    pub scans: Arc<AtomicU64>,
}

impl Discovery {
    /// Scans on an interval and reconciles the supervisor's registry against what it finds.
    ///
    /// Never returns while the channel is open. Losing this task must not stop the
    /// watcher — the supervisor treats a closed command channel as "no more discovery",
    /// not as a shutdown.
    pub async fn run(self, cmds: mpsc::Sender<SupervisorCmd>) {
        // source name -> the workspace it watches, for exactly the set currently registered.
        let mut known: HashMap<String, Workspace> = HashMap::new();
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            // Bounded and cheap (FR-D13): a directory glob, never a transcript walk.
            let found = discover(&self.data_root);
            let current: HashMap<String, Workspace> = found
                .into_iter()
                .map(|w| (w.source_name(), w))
                .collect();

            for (name, ws) in &current {
                if known.contains_key(name) {
                    continue;
                }
                let collector =
                    SessionCollector::new(ws.clone(), &self.harness_name, self.session_interval);
                if cmds
                    .send(SupervisorCmd::Add(Box::new(collector)))
                    .await
                    .is_err()
                {
                    tracing::warn!("supervisor is gone — discovery stopping");
                    return;
                }
                tracing::info!(workspace = %name, "workspace discovered");
            }

            for name in known.keys() {
                if current.contains_key(name) {
                    continue;
                }
                if cmds
                    .send(SupervisorCmd::Remove(name.clone()))
                    .await
                    .is_err()
                {
                    tracing::warn!("supervisor is gone — discovery stopping");
                    return;
                }
                tracing::info!(workspace = %name, "workspace retired");
            }

            self.workspaces.store(current.len() as u64, Ordering::Relaxed);
            self.scans.fetch_add(1, Ordering::Relaxed);
            known = current;
        }
    }
}
