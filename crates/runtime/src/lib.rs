//! `harnesssphere-runtime` — supervisor/scheduler (driving).
//!
//! Orchestrates the ports: one task per `SignalSource`, 3-layer failure isolation
//! (Result → catch_unwind → task), circuit breaker and criticality policy. A single
//! drain batches and calls the `SignalExporter`.

use futures::FutureExt;
use harnesssphere_domain::{
    classify_failure, CircuitBreaker, Criticality, FailureAction, ProbeResult, Receiver, Signal,
    SignalExporter, SignalSink, SignalSource,
};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};

/// Bounded-channel-based sink. Under backpressure it drops the **newest** signal
/// (drop-newest) — `tokio::mpsc` doesn't allow popping from the front, so drop-oldest
/// would require another structure; left as a future improvement. The drop is counted in
/// a self metric.
#[derive(Clone)]
pub struct ChannelSink {
    tx: mpsc::Sender<Signal>,
    dropped: Arc<AtomicU64>,
}

impl ChannelSink {
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl SignalSink for ChannelSink {
    fn emit(&self, signal: Signal) {
        // Non-blocking: if the channel is full, the new signal is dropped (drop-newest).
        if self.tx.try_send(signal).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct RuntimeConfig {
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub batch_interval: Duration,
    /// Consecutive failures at which a Critical source becomes fatal.
    pub critical_threshold: u32,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            channel_capacity: 4096,
            batch_size: 512,
            batch_interval: Duration::from_secs(5),
            critical_threshold: 3,
        }
    }
}

/// Signals the top-level supervisor that a Critical source has died irrecoverably.
#[derive(Debug)]
pub struct FatalSignal {
    pub source: &'static str,
    pub reason: String,
}

pub struct Supervisor {
    cfg: RuntimeConfig,
    sources: Vec<Box<dyn SignalSource>>,
    receivers: Vec<Box<dyn Receiver>>,
    exporter: Arc<dyn SignalExporter>,
}

impl Supervisor {
    pub fn new(
        cfg: RuntimeConfig,
        sources: Vec<Box<dyn SignalSource>>,
        exporter: Arc<dyn SignalExporter>,
    ) -> Self {
        Supervisor {
            cfg,
            sources,
            receivers: Vec::new(),
            exporter,
        }
    }

    /// Adds driving ingest adapters (push). Always Optional — a receiver that fails to
    /// bind or serve is logged and never makes the process exit.
    pub fn with_receivers(mut self, receivers: Vec<Box<dyn Receiver>>) -> Self {
        self.receivers = receivers;
        self
    }

    /// Runs until Ctrl-C is received or a Critical source fails fatally.
    /// Returns `Err(FatalSignal)` in the fatal case (the binary converts it to exit != 0).
    pub async fn run(self) -> Result<(), FatalSignal> {
        let (tx, rx) = mpsc::channel::<Signal>(self.cfg.channel_capacity);
        let (fatal_tx, mut fatal_rx) = mpsc::channel::<FatalSignal>(4);
        let sink = ChannelSink {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        };

        // Drain: batch + export.
        let drain = tokio::spawn(drain_loop(
            rx,
            self.exporter.clone(),
            self.cfg.batch_size,
            self.cfg.batch_interval,
        ));

        // One task per source. The whole supervisor body is wrapped in catch_unwind so a
        // panic *anywhere* (probe, breaker, loop logic) can't make a Critical source die
        // silently — it escalates to a fatal, honoring "Critical failure → process exits".
        // (Per-tick `collect` panics are still caught inside the loop and only degrade.)
        let mut handles = Vec::new();
        for source in self.sources {
            let sink = sink.clone();
            let fatal_tx = fatal_tx.clone();
            let threshold = self.cfg.critical_threshold;
            let desc = source.descriptor().clone();
            handles.push(tokio::spawn(async move {
                let outcome =
                    AssertUnwindSafe(supervise_source(source, sink, fatal_tx.clone(), threshold))
                        .catch_unwind()
                        .await;
                if outcome.is_err() {
                    tracing::error!(source = desc.name, "supervisor task panicked");
                    if desc.criticality == Criticality::Critical {
                        let _ = fatal_tx
                            .send(FatalSignal {
                                source: desc.name,
                                reason: "supervisor task panicked".into(),
                            })
                            .await;
                    }
                }
            }));
        }
        // One task per receiver (driving/push adapters). Always Optional.
        for receiver in self.receivers {
            let desc = receiver.descriptor().clone();
            let recv_sink: Arc<dyn SignalSink> = Arc::new(sink.clone());
            handles.push(tokio::spawn(async move {
                tracing::info!(receiver = desc.name, endpoint = %desc.endpoint, "ingest receiver listening");
                if let Err(e) = receiver.serve(recv_sink).await {
                    tracing::warn!(receiver = desc.name, error = %e, "ingest receiver stopped (degraded)");
                }
            }));
        }

        drop(sink);
        drop(fatal_tx);

        let result = tokio::select! {
            fatal = fatal_rx.recv() => match fatal {
                Some(f) => Err(f),
                None => Ok(()),
            },
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c received — shutting down");
                Ok(())
            }
        };

        // Ordered shutdown / flush-on-fatal: stop the sources & receivers first (this drops
        // their sink clones), which closes the channel so the drain can flush whatever is
        // still buffered and export it — instead of dropping it on the floor with abort().
        for h in &handles {
            h.abort();
        }
        drop(handles);
        match tokio::time::timeout(Duration::from_secs(5), drain).await {
            Ok(_) => {}
            Err(_) => tracing::warn!("drain flush timed out on shutdown"),
        }
        self.exporter.shutdown().await;
        result
    }
}

async fn supervise_source(
    mut source: Box<dyn SignalSource>,
    sink: ChannelSink,
    fatal_tx: mpsc::Sender<FatalSignal>,
    critical_threshold: u32,
) {
    let desc = source.descriptor().clone();
    let mut breaker = CircuitBreaker::new(critical_threshold);

    // Initial probe.
    match source.probe().await {
        ProbeResult::Ready => {}
        ProbeResult::NotApplicable => {
            tracing::info!(source = desc.name, "not applicable on this host — disabled");
            return;
        }
        ProbeResult::Unavailable(msg) => {
            tracing::warn!(source = desc.name, %msg, "target unavailable at boot — degraded");
            breaker.trip_open();
        }
        ProbeResult::Fatal(msg) => {
            let _ = fatal_tx
                .send(FatalSignal {
                    source: desc.name,
                    reason: format!("probe fatal: {msg}"),
                })
                .await;
            return;
        }
    }

    let mut ticker = interval(desc.default_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        // Backoff when the breaker is open.
        if breaker.is_open() {
            tokio::time::sleep(breaker.backoff()).await;
        }

        let started = Instant::now();
        // Containment layers: Result (expected) + catch_unwind (panic).
        let outcome = AssertUnwindSafe(source.collect(&sink)).catch_unwind().await;

        match outcome {
            Ok(Ok(())) => {
                breaker.record_success();
                tracing::trace!(
                    source = desc.name,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "scrape ok"
                );
            }
            Ok(Err(err)) => {
                handle_failure(&desc.name, desc.criticality, &mut breaker, &fatal_tx, err.to_string())
                    .await;
            }
            Err(_panic) => {
                handle_failure(
                    &desc.name,
                    desc.criticality,
                    &mut breaker,
                    &fatal_tx,
                    "panic contained in the collector".to_string(),
                )
                .await;
            }
        }
    }
}

async fn handle_failure(
    name: &'static str,
    criticality: Criticality,
    breaker: &mut CircuitBreaker,
    fatal_tx: &mpsc::Sender<FatalSignal>,
    err: String,
) {
    breaker.record_failure();
    match classify_failure(criticality, breaker) {
        FailureAction::Degrade => {
            tracing::warn!(source = name, consecutive = breaker.consecutive_failures(), %err, "degraded");
        }
        FailureAction::Fatal => {
            tracing::error!(source = name, %err, "persistent CRITICAL failure — shutting down");
            let _ = fatal_tx
                .send(FatalSignal {
                    source: name,
                    reason: err,
                })
                .await;
        }
    }
}

async fn drain_loop(
    mut rx: mpsc::Receiver<Signal>,
    exporter: Arc<dyn SignalExporter>,
    batch_size: usize,
    batch_interval: Duration,
) {
    let mut buf: Vec<Signal> = Vec::with_capacity(batch_size);
    let mut flush = interval(batch_interval);
    flush.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(sig) => {
                    buf.push(sig);
                    if buf.len() >= batch_size {
                        export_batch(&exporter, &mut buf).await;
                    }
                }
                None => {
                    export_batch(&exporter, &mut buf).await;
                    break;
                }
            },
            _ = flush.tick() => {
                if !buf.is_empty() {
                    export_batch(&exporter, &mut buf).await;
                }
            }
        }
    }
}

async fn export_batch(exporter: &Arc<dyn SignalExporter>, buf: &mut Vec<Signal>) {
    let batch = std::mem::take(buf);
    if let Err(err) = exporter.export(batch).await {
        // Export failure NEVER blocks collection (NFR-04) — just log it.
        tracing::warn!(%err, "failed to export batch — dropped");
    }
}
