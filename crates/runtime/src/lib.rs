//! `harnesssphere-runtime` — supervisor/scheduler (driving).
//!
//! Orquestra os ports: uma task por `SignalSource`, isolamento de falha em 3 camadas
//! (Result → catch_unwind → task), circuit breaker e política de criticidade. Dreno
//! único faz batch e chama o `SignalExporter`.

use futures::FutureExt;
use harnesssphere_domain::{
    classify_failure, CircuitBreaker, Criticality, FailureAction, ProbeResult, Signal,
    SignalExporter, SignalSink, SignalSource,
};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};

/// Sink baseado em canal bounded. Sob backpressure descarta o sinal **mais novo**
/// (drop-newest) — `tokio::mpsc` não permite pop da frente, então drop-oldest exigiria
/// outra estrutura; fica como melhoria futura. O descarte é contado em métrica self.
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
        // Não-bloqueante: se o canal está cheio, o sinal novo é descartado (drop-newest).
        if self.tx.try_send(signal).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct RuntimeConfig {
    pub channel_capacity: usize,
    pub batch_size: usize,
    pub batch_interval: Duration,
    /// Falhas consecutivas a partir das quais um source Critical é fatal.
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

/// Sinaliza ao supervisor-mor que um source Critical morreu de forma irreversível.
#[derive(Debug)]
pub struct FatalSignal {
    pub source: &'static str,
    pub reason: String,
}

pub struct Supervisor {
    cfg: RuntimeConfig,
    sources: Vec<Box<dyn SignalSource>>,
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
            exporter,
        }
    }

    /// Roda até receber Ctrl-C ou até um source Critical falhar de forma fatal.
    /// Retorna `Err(FatalSignal)` no caso fatal (o binário converte em exit != 0).
    pub async fn run(self) -> Result<(), FatalSignal> {
        let (tx, rx) = mpsc::channel::<Signal>(self.cfg.channel_capacity);
        let (fatal_tx, mut fatal_rx) = mpsc::channel::<FatalSignal>(4);
        let sink = ChannelSink {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        };

        // Dreno: batch + export.
        let drain = tokio::spawn(drain_loop(
            rx,
            self.exporter.clone(),
            self.cfg.batch_size,
            self.cfg.batch_interval,
        ));

        // Uma task por source.
        let mut handles = Vec::new();
        for source in self.sources {
            let sink = sink.clone();
            let fatal_tx = fatal_tx.clone();
            let threshold = self.cfg.critical_threshold;
            handles.push(tokio::spawn(supervise_source(
                source, sink, fatal_tx, threshold,
            )));
        }
        drop(sink);
        drop(fatal_tx);

        let result = tokio::select! {
            fatal = fatal_rx.recv() => match fatal {
                Some(f) => Err(f),
                None => Ok(()),
            },
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c recebido — encerrando");
                Ok(())
            }
        };

        for h in handles {
            h.abort();
        }
        drain.abort();
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

    // Probe inicial.
    match source.probe().await {
        ProbeResult::Ready => {}
        ProbeResult::NotApplicable => {
            tracing::info!(source = desc.name, "não aplicável neste host — desabilitado");
            return;
        }
        ProbeResult::Unavailable(msg) => {
            tracing::warn!(source = desc.name, %msg, "alvo indisponível no boot — degraded");
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

        // Backoff quando o breaker está aberto.
        if breaker.is_open() {
            tokio::time::sleep(breaker.backoff()).await;
        }

        let started = Instant::now();
        // Camadas de contenção: Result (esperado) + catch_unwind (panic).
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
                    "panic contido no coletor".to_string(),
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
            tracing::error!(source = name, %err, "falha CRÍTICA persistente — encerrando");
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
        // Falha de export NUNCA bloqueia coleta (NFR-04) — só registra.
        tracing::warn!(%err, "falha ao exportar batch — descartado");
    }
}
