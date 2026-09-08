//! Integration: the supervisor's source set is mutable while it runs.
//!
//! These cover the three things `tasks.md` defines D-02/D-03/D-04 as done by, and they
//! are written against OBSERVED SIGNALS rather than internal state -- the point of DEC-12
//! is what a dashboard sees, so a test that inspected the registry would pass while the
//! property it protects was broken.

use harnesssphere_domain::{
    AttrValue, CollectError, Criticality, ExportError, Layer, Metric, MetricKind, ProbeResult,
    Signal, SignalExporter, SignalSink, SignalSource, SourceDescriptor, LAYER_ATTR,
};
use harnesssphere_runtime::{RuntimeConfig, Supervisor, SupervisorCmd};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Records every metric name it exports, so tests can assert on what a backend would see.
#[derive(Clone, Default)]
struct RecordingExporter {
    seen: Arc<Mutex<Vec<(String, f64)>>>,
    layers: Arc<Mutex<Vec<(String, Option<String>)>>>,
}
#[async_trait::async_trait]
impl SignalExporter for RecordingExporter {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError> {
        let mut seen = self.seen.lock().unwrap();
        let mut layers = self.layers.lock().unwrap();
        for s in batch {
            if let Signal::Metric(m) = s {
                let layer = m.attributes.iter().find(|(k, _)| k == LAYER_ATTR).map(
                    |(_, v)| match v {
                        AttrValue::Str(s) => s.clone(),
                        other => format!("{other:?}"),
                    },
                );
                layers.push((m.name.clone(), layer));
                seen.push((m.name.clone(), m.value));
            }
        }
        Ok(())
    }
}
impl RecordingExporter {
    fn names(&self) -> Vec<String> {
        self.seen.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
    }
    fn layers_of(&self, name: &str) -> Vec<Option<String>> {
        self.layers
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, l)| l.clone())
            .collect()
    }
    fn values_of(&self, name: &str) -> Vec<f64> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| *v)
            .collect()
    }
}

/// A stand-in for a per-instance source: emits `<metric>` = 1 each tick, and on retirement
/// emits the same series as 0 -- the "absent, not silent" contract of DEC-12.
struct Instance {
    desc: SourceDescriptor,
    metric: String,
}
impl Instance {
    fn boxed(name: &str, metric: &str) -> Box<dyn SignalSource> {
        Box::new(Instance {
            desc: SourceDescriptor {
                name: name.to_owned(),
                layer: Layer::Harness,
                criticality: Criticality::Optional,
                default_interval: Duration::from_millis(10),
            },
            metric: metric.to_owned(),
        })
    }
}
#[async_trait::async_trait]
impl SignalSource for Instance {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.desc
    }
    async fn probe(&mut self) -> ProbeResult {
        ProbeResult::Ready
    }
    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        sink.emit(Signal::Metric(Metric::now(&self.metric, MetricKind::Gauge, 1.0)));
        Ok(())
    }
    async fn retire(&mut self, sink: &dyn SignalSink) {
        sink.emit(Signal::Metric(Metric::now(&self.metric, MetricKind::Gauge, 0.0)));
    }
}

fn fast_cfg() -> RuntimeConfig {
    RuntimeConfig {
        critical_threshold: 3,
        batch_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

/// D-02: a source added to an ALREADY RUNNING supervisor is collected.
#[tokio::test]
async fn source_added_while_running_is_collected() {
    let exporter = RecordingExporter::default();
    let sup = Supervisor::new(fast_cfg(), vec![], Arc::new(exporter.clone()));
    let cmds = sup.commands();
    let handle = tokio::spawn(sup.run());

    // Nothing was registered at boot: the supervisor started with an empty source set.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        exporter.names().is_empty(),
        "expected no signals before anything was added, got {:?}",
        exporter.names()
    );

    cmds.send(SupervisorCmd::Add(Instance::boxed(
        "session:t/s/a/u",
        "harnesssphere.harness.messages",
    )))
    .await
    .expect("supervisor is not accepting commands");

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        exporter
            .names()
            .contains(&"harnesssphere.harness.messages".to_string()),
        "a source added at runtime never emitted; saw {:?}",
        exporter.names()
    );
    handle.abort();
}

/// D-04 + DEC-12: removal emits the terminal signal, and THEN collection stops.
///
/// The assertion is deliberately two-sided. Asserting only that emissions stop would pass
/// for a plain `abort()`, which is exactly the bug DEC-21 is written against.
#[tokio::test]
async fn removal_emits_a_terminal_signal_then_stops() {
    let exporter = RecordingExporter::default();
    let sup = Supervisor::new(fast_cfg(), vec![], Arc::new(exporter.clone()));
    let cmds = sup.commands();
    let handle = tokio::spawn(sup.run());

    cmds.send(SupervisorCmd::Add(Instance::boxed("inst-a", "inst.up")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        !exporter.values_of("inst.up").is_empty(),
        "source never started"
    );

    cmds.send(SupervisorCmd::Remove("inst-a".to_owned()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let values = exporter.values_of("inst.up");
    assert!(
        values.contains(&0.0),
        "a retired source must go ABSENT, not silent — no terminal 0 in {values:?}"
    );
    let after_terminal = values
        .iter()
        .position(|v| *v == 0.0)
        .map(|i| values[i + 1..].to_vec())
        .unwrap_or_default();
    assert!(
        after_terminal.iter().all(|v| *v == 0.0),
        "source kept collecting after retirement: {after_terminal:?}"
    );
    handle.abort();
}

/// D-03: names are unique per instance (FR-D8), so a duplicate add is a discovery bug.
/// Shadowing it would leak the first task and leave two writers on one series.
#[tokio::test]
async fn duplicate_name_is_rejected_not_shadowed() {
    let exporter = RecordingExporter::default();
    let sup = Supervisor::new(fast_cfg(), vec![], Arc::new(exporter.clone()));
    let cmds = sup.commands();
    let handle = tokio::spawn(sup.run());

    cmds.send(SupervisorCmd::Add(Instance::boxed("dup", "first.metric")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    cmds.send(SupervisorCmd::Add(Instance::boxed("dup", "second.metric")))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    assert!(
        !exporter.names().contains(&"second.metric".to_string()),
        "the duplicate was registered anyway — two tasks now write under one name"
    );

    // And removing the name must actually stop the FIRST source, not a shadow of it.
    cmds.send(SupervisorCmd::Remove("dup".to_owned())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let before = exporter.values_of("first.metric").len();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        before,
        exporter.values_of("first.metric").len(),
        "the original source is still collecting after its name was removed"
    );
    handle.abort();
}

/// Losing discovery must not stop the watcher: the command channel closing is not a
/// shutdown signal.
#[tokio::test]
async fn dropping_the_command_sender_does_not_stop_collection() {
    let exporter = RecordingExporter::default();
    let sup = Supervisor::new(
        fast_cfg(),
        vec![Instance::boxed("host", "boot.metric")],
        Arc::new(exporter.clone()),
    );
    let cmds = sup.commands();
    let handle = tokio::spawn(sup.run());

    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(cmds);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let before = exporter.values_of("boot.metric").len();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        exporter.values_of("boot.metric").len() > before,
        "collection stopped when the discovery channel closed"
    );
    assert!(!handle.is_finished(), "supervisor exited when discovery went away");
    handle.abort();
}

/// Every emitted signal carries its source's layer.
///
/// This is a regression test for dead metadata, not a new feature: `SourceDescriptor.layer`
/// was set by every collector and read by NOTHING -- `Layer::as_str()` had zero callers --
/// so all six layers were invisible to any backend, not merely Proxy and Webapp.
#[tokio::test]
async fn every_signal_carries_its_layer() {
    let exporter = RecordingExporter::default();
    let sup = Supervisor::new(
        fast_cfg(),
        vec![Instance::boxed("inst", "layered.metric")],
        Arc::new(exporter.clone()),
    );
    let handle = tokio::spawn(sup.run());
    tokio::time::sleep(Duration::from_millis(120)).await;

    let layers = exporter.layers_of("layered.metric");
    assert!(!layers.is_empty(), "source never emitted");
    assert!(
        layers.iter().all(|l| l.as_deref() == Some("harness")),
        "signals reached the exporter without their layer: {layers:?}"
    );
    handle.abort();
}

/// A source that stamps its own layer per signal keeps it: the supervisor fills the gap,
/// it does not overwrite. This is what lets one probe collector serve three layers.
#[tokio::test]
async fn a_source_may_override_the_layer_per_signal() {
    struct MultiLayer {
        desc: SourceDescriptor,
    }
    #[async_trait::async_trait]
    impl SignalSource for MultiLayer {
        fn descriptor(&self) -> &SourceDescriptor {
            &self.desc
        }
        async fn probe(&mut self) -> ProbeResult {
            ProbeResult::Ready
        }
        async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
            // Explicit per-target layers, as EndpointProbeCollector does.
            for layer in ["gateway", "proxy", "webapp"] {
                sink.emit(
                    Metric::now("multi.up", MetricKind::Gauge, 1.0)
                        .attr(LAYER_ATTR, layer.to_owned())
                        .into_signal(),
                );
            }
            Ok(())
        }
    }

    let exporter = RecordingExporter::default();
    let src: Box<dyn SignalSource> = Box::new(MultiLayer {
        desc: SourceDescriptor {
            name: "endpoint-probe".to_owned(),
            layer: Layer::Watcher,
            criticality: Criticality::Optional,
            default_interval: Duration::from_millis(10),
        },
    });
    let sup = Supervisor::new(fast_cfg(), vec![src], Arc::new(exporter.clone()));
    let handle = tokio::spawn(sup.run());
    tokio::time::sleep(Duration::from_millis(120)).await;

    let seen: std::collections::BTreeSet<_> = exporter
        .layers_of("multi.up")
        .into_iter()
        .flatten()
        .collect();
    assert!(
        seen.contains("gateway") && seen.contains("proxy") && seen.contains("webapp"),
        "per-signal layers were overwritten by the descriptor's: {seen:?}"
    );
    assert!(
        !seen.contains("watcher"),
        "the supervisor overwrote a layer the source had already set: {seen:?}"
    );
    handle.abort();
}
