//! `harnesssphere-runtime` — supervisor/scheduler (driving).
//!
//! Orchestrates the ports: one task per `SignalSource`, 3-layer failure isolation
//! (Result → catch_unwind → task), circuit breaker and criticality policy. A single
//! drain batches and calls the `SignalExporter`.

use futures::FutureExt;
use harnesssphere_domain::{
    classify_failure, CircuitBreaker, Criticality, FailureAction, ProbeResult, Signal,
    SignalExporter, SignalSink, SignalSource,
};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{interval, Instant, MissedTickBehavior};

/// How long a retiring source gets to emit its terminal signal and return before the
/// supervisor gives up and aborts it.
const RETIRE_TIMEOUT: Duration = Duration::from_secs(5);

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
    pub source: String,
    pub reason: String,
}

/// A command to a *running* supervisor.
///
/// This is a **separate channel from the signal sink, on purpose** (DEC-20). Control
/// traffic on the data path would inherit the drain's batching latency, the sink's
/// drop-newest backpressure policy, and the circuit breaker — three behaviours that are
/// right for telemetry and wrong for "stop watching this container".
pub enum SupervisorCmd {
    /// Start watching a source discovered at runtime. Ignored if its name is already
    /// registered: names are unique per instance (FR-D8), so a duplicate is a bug in
    /// discovery, not a legitimate re-add.
    Add(Box<dyn SignalSource>),
    /// Retire a source by descriptor name. The source emits its own terminal signal
    /// before its task ends (DEC-12, DEC-21).
    Remove(String),
}

/// Where a source came from. Boot sources and discovered sources differ in exactly one
/// behaviour — see the `NotApplicable` arm in `supervise_source` (DEC-22).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Boot,
    Discovered,
}

struct SourceHandle {
    stop: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

pub struct Supervisor {
    cfg: RuntimeConfig,
    sources: Vec<Box<dyn SignalSource>>,
    exporter: Arc<dyn SignalExporter>,
    cmd_tx: mpsc::Sender<SupervisorCmd>,
    cmd_rx: mpsc::Receiver<SupervisorCmd>,
}

impl Supervisor {
    pub fn new(
        cfg: RuntimeConfig,
        sources: Vec<Box<dyn SignalSource>>,
        exporter: Arc<dyn SignalExporter>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        Supervisor {
            cfg,
            sources,
            exporter,
            cmd_tx,
            cmd_rx,
        }
    }

    /// A sender for driving add/remove while the supervisor runs. Take it *before*
    /// calling `run`, which consumes the supervisor.
    pub fn commands(&self) -> mpsc::Sender<SupervisorCmd> {
        self.cmd_tx.clone()
    }

    /// Runs until Ctrl-C is received or a Critical source fails fatally.
    /// Returns `Err(FatalSignal)` in the fatal case (the binary converts it to exit != 0).
    pub async fn run(mut self) -> Result<(), FatalSignal> {
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

        // The registry. Keyed by the owned descriptor name — which is why that field
        // stopped being `&'static str`: it is the handle discovery adds and removes by.
        let mut registry: HashMap<String, SourceHandle> = HashMap::new();
        let threshold = self.cfg.critical_threshold;

        for source in std::mem::take(&mut self.sources) {
            spawn_source(&mut registry, source, &sink, &fatal_tx, threshold, Origin::Boot);
        }

        // NOTE: `sink` and `fatal_tx` are deliberately NOT dropped here, unlike before.
        // The supervisor has to outlive its initial sources so it can spawn more, so it
        // keeps a clone of each and drops them explicitly at shutdown — otherwise the
        // drain never gets its close signal and the final flush is lost.
        let mut cmd_open = true;
        let result = loop {
            tokio::select! {
                fatal = fatal_rx.recv() => match fatal {
                    Some(f) => break Err(f),
                    None => break Ok(()),
                },
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("ctrl-c received — shutting down");
                    break Ok(());
                }
                cmd = self.cmd_rx.recv(), if cmd_open => match cmd {
                    Some(SupervisorCmd::Add(source)) => {
                        spawn_source(
                            &mut registry,
                            source,
                            &sink,
                            &fatal_tx,
                            threshold,
                            Origin::Discovered,
                        );
                    }
                    Some(SupervisorCmd::Remove(name)) => {
                        retire_source(&mut registry, &name).await;
                    }
                    // Discovery is gone. Keep collecting what is already registered:
                    // losing the discoverer must not stop the watcher.
                    None => cmd_open = false,
                },
            }
        };

        // Ordered shutdown / flush-on-fatal. abort() is correct HERE and only here: the
        // process is ending, so there is no "absent vs quiet" distinction left to draw.
        // Retiring a single source goes through `retire_source` instead.
        for (_, handle) in registry.drain() {
            handle.join.abort();
        }
        drop(sink);
        drop(fatal_tx);
        match tokio::time::timeout(Duration::from_secs(5), drain).await {
            Ok(_) => {}
            Err(_) => tracing::warn!("drain flush timed out on shutdown"),
        }
        self.exporter.shutdown().await;
        result
    }
}

/// Spawns one supervised source and registers it.
///
/// The whole supervisor body is wrapped in `catch_unwind` so a panic *anywhere* (probe,
/// breaker, loop logic) cannot make a Critical source die silently — it escalates to a
/// fatal, honouring "Critical failure → process exits". Per-tick `collect` panics are
/// still caught inside the loop and only degrade.
fn spawn_source(
    registry: &mut HashMap<String, SourceHandle>,
    source: Box<dyn SignalSource>,
    sink: &ChannelSink,
    fatal_tx: &mpsc::Sender<FatalSignal>,
    critical_threshold: u32,
    origin: Origin,
) {
    let desc = source.descriptor().clone();
    if registry.contains_key(&desc.name) {
        // Not a legitimate re-add: names are unique per instance (FR-D8), so this is
        // discovery emitting the same instance twice. Shadowing it would leak the old
        // task and leave two writers on one series.
        tracing::warn!(source = %desc.name, "duplicate source name — not added");
        return;
    }
    let (stop_tx, stop_rx) = oneshot::channel();
    let sink = sink.clone();
    let fatal_tx = fatal_tx.clone();
    let name = desc.name.clone();
    let join = tokio::spawn(async move {
        let outcome = AssertUnwindSafe(supervise_source(
            source,
            sink,
            fatal_tx.clone(),
            critical_threshold,
            origin,
            stop_rx,
        ))
        .catch_unwind()
        .await;
        if outcome.is_err() {
            tracing::error!(source = %desc.name, "supervisor task panicked");
            if desc.criticality == Criticality::Critical {
                let _ = fatal_tx
                    .send(FatalSignal {
                        source: desc.name.clone(),
                        reason: "supervisor task panicked".into(),
                    })
                    .await;
            }
        }
    });
    registry.insert(name, SourceHandle { stop: stop_tx, join });
}

/// Retires one source: signal, let it emit its own terminal signal, join, and abort only
/// if it overruns (DEC-21).
///
/// **Why not just `abort()`.** The shutdown path aborts and relies on the dropped sink
/// clones closing the channel to trigger the drain's flush. That is correct for process
/// exit and wrong for removing *one* source: the channel stays open because other sources
/// still hold clones, so nothing flushes, and `abort()` lands at an arbitrary await point
/// and discards whatever the task had collected. The retired instance would then go
/// **silent** — precisely the failure DEC-12 exists to prevent, reached by way of code
/// that already looked correct.
async fn retire_source(registry: &mut HashMap<String, SourceHandle>, name: &str) {
    let Some(handle) = registry.remove(name) else {
        tracing::debug!(source = %name, "retire: not registered");
        return;
    };
    // The receiver being gone means the task already ended; nothing to wait for.
    let _ = handle.stop.send(());
    let abort = handle.join.abort_handle();
    match tokio::time::timeout(RETIRE_TIMEOUT, handle.join).await {
        Ok(_) => tracing::info!(source = %name, "source retired"),
        Err(_) => {
            abort.abort();
            tracing::warn!(
                source = %name,
                "source did not stop in time — aborted, terminal signal may be lost"
            );
        }
    }
}

async fn supervise_source(
    mut source: Box<dyn SignalSource>,
    sink: ChannelSink,
    fatal_tx: mpsc::Sender<FatalSignal>,
    critical_threshold: u32,
    origin: Origin,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let desc = source.descriptor().clone();
    let mut breaker = CircuitBreaker::new(critical_threshold);

    // Initial probe. For a discovered source this runs at discovery, not at boot.
    match source.probe().await {
        ProbeResult::Ready => {}
        // DEC-22. `NotApplicable` PERMANENTLY drops a source, which is right at boot --
        // "there is no cgroup on this host" is a fact about the host and will not change.
        // For a source discovered at runtime it is a trap: a container still starting when
        // discovery probes it is a TRANSIENT condition, and dropping it would unwatch that
        // tenant until the process restarts, showing up as a tenant simply missing from
        // dashboards with no error anywhere.
        //
        // Enforced here rather than at the construction site: a convention about which
        // constructors may return the variant is broken by the next person who adds a
        // discovered source, whereas this rule cannot be bypassed.
        ProbeResult::NotApplicable if origin == Origin::Discovered => {
            tracing::warn!(
                source = %desc.name,
                "discovered source probed NotApplicable — treating as Unavailable (DEC-22)"
            );
            breaker.trip_open();
        }
        ProbeResult::NotApplicable => {
            tracing::info!(source = %desc.name, "not applicable on this host — disabled");
            return;
        }
        ProbeResult::Unavailable(msg) => {
            tracing::warn!(source = desc.name, %msg, "target unavailable at boot — degraded");
            breaker.trip_open();
        }
        ProbeResult::Fatal(msg) => {
            let _ = fatal_tx
                .send(FatalSignal {
                    source: desc.name.clone(),
                    reason: format!("probe fatal: {msg}"),
                })
                .await;
            return;
        }
    }

    let mut ticker = interval(desc.default_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            // Retirement. The source emits its own terminal signal (DEC-21) so a retired
            // instance goes ABSENT rather than silent, then the task returns normally and
            // `retire_source`'s join succeeds.
            _ = &mut stop_rx => {
                source.retire(&sink).await;
                tracing::info!(source = %desc.name, "emitted terminal signal — retiring");
                return;
            }
        }

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
    name: &str,
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
                    source: name.to_owned(),
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
