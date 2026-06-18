//! Integration: proves requirement #1 — a Critical source with a persistent failure
//! shuts the supervisor down (end-to-end fatal path), and a failing Optional does NOT.

use harnesssphere_domain::{
    CollectError, Criticality, ExportError, Layer, ProbeResult, Signal, SignalExporter, SignalSink,
    SignalSource, SourceDescriptor,
};
use harnesssphere_runtime::{RuntimeConfig, Supervisor};
use std::sync::Arc;
use std::time::Duration;

struct AlwaysFail {
    desc: SourceDescriptor,
}

#[async_trait::async_trait]
impl SignalSource for AlwaysFail {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.desc
    }
    async fn probe(&mut self) -> ProbeResult {
        ProbeResult::Ready
    }
    async fn collect(&mut self, _sink: &dyn SignalSink) -> Result<(), CollectError> {
        Err(CollectError::Failed("boom".into()))
    }
}

struct NoopExporter;
#[async_trait::async_trait]
impl SignalExporter for NoopExporter {
    async fn export(&self, _batch: Vec<Signal>) -> Result<(), ExportError> {
        Ok(())
    }
}

/// A source whose `probe()` panics — exercises the supervisor-task catch_unwind safety net.
struct PanicProbe {
    desc: SourceDescriptor,
}
#[async_trait::async_trait]
impl SignalSource for PanicProbe {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.desc
    }
    async fn probe(&mut self) -> ProbeResult {
        panic!("probe boom");
    }
    async fn collect(&mut self, _sink: &dyn SignalSink) -> Result<(), CollectError> {
        Ok(())
    }
}

fn source(name: &'static str, crit: Criticality) -> Box<dyn SignalSource> {
    Box::new(AlwaysFail {
        desc: SourceDescriptor {
            name,
            layer: Layer::Host,
            criticality: crit,
            default_interval: Duration::from_millis(2),
        },
    })
}

#[tokio::test]
async fn critical_persistent_failure_is_fatal() {
    let sources = vec![source("host", Criticality::Critical)];
    let exporter: Arc<dyn SignalExporter> = Arc::new(NoopExporter);
    let cfg = RuntimeConfig {
        critical_threshold: 1, // the first failure is already fatal (no accumulated backoff)
        ..Default::default()
    };
    let sup = Supervisor::new(cfg, sources, exporter);

    let res = tokio::time::timeout(Duration::from_secs(5), sup.run()).await;
    let inner = res.expect("supervisor did not return — fatal path stalled");
    let fatal = inner.expect_err("an always-failing Critical source should be FATAL");
    assert_eq!(fatal.source, "host");
}

#[tokio::test]
async fn critical_probe_panic_is_fatal() {
    // A panic in a Critical source's probe must not die silently — it escalates to fatal.
    let src: Box<dyn SignalSource> = Box::new(PanicProbe {
        desc: SourceDescriptor {
            name: "host",
            layer: Layer::Host,
            criticality: Criticality::Critical,
            default_interval: Duration::from_millis(5),
        },
    });
    let sup = Supervisor::new(
        RuntimeConfig {
            critical_threshold: 1,
            ..Default::default()
        },
        vec![src],
        Arc::new(NoopExporter),
    );
    let inner = tokio::time::timeout(Duration::from_secs(5), sup.run())
        .await
        .expect("supervisor did not return — panic escalation stalled");
    let fatal = inner.expect_err("a Critical probe panic should be FATAL");
    assert_eq!(fatal.source, "host");
}

#[tokio::test]
async fn optional_failure_never_kills_supervisor() {
    // An Optional that fails forever must NOT produce a fatal. We allow time for several
    // failure cycles and then require the supervisor to still be running (timeout).
    let sources = vec![source("gateway", Criticality::Optional)];
    let exporter: Arc<dyn SignalExporter> = Arc::new(NoopExporter);
    let cfg = RuntimeConfig {
        critical_threshold: 1,
        ..Default::default()
    };
    let sup = Supervisor::new(cfg, sources, exporter);

    // If run() returns within the window, it turned fatal — test failure.
    let res = tokio::time::timeout(Duration::from_millis(300), sup.run()).await;
    assert!(
        res.is_err(),
        "supervisor shut down with a failing Optional source — it should not have"
    );
}
