//! Self collector (Critical) — HarnessSphere's own self-observability.
//! Maps to `process.*`.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::time::Duration;
use sysinfo::{get_current_pid, Pid, ProcessesToUpdate, System};

pub struct SelfCollector {
    descriptor: SourceDescriptor,
    sys: System,
    pid: Pid,
}

impl SelfCollector {
    pub fn new(interval: Duration) -> Result<Self, String> {
        let pid = get_current_pid().map_err(|e| format!("current pid unavailable: {e}"))?;
        Ok(SelfCollector {
            descriptor: SourceDescriptor {
                name: "self",
                layer: Layer::Watcher,
                criticality: Criticality::Critical,
                default_interval: interval,
            },
            sys: System::new(),
            pid,
        })
    }
}

#[async_trait]
impl SignalSource for SelfCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        self.sys
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        if self.sys.process(self.pid).is_some() {
            ProbeResult::Ready
        } else {
            ProbeResult::Fatal("could not inspect own process".into())
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        self.sys
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        let proc = self
            .sys
            .process(self.pid)
            .ok_or_else(|| CollectError::Failed("own process disappeared".into()))?;

        // Tag our own process metrics with an executable name so they don't collapse into
        // an unlabeled series when a dashboard groups process.* by process.executable.name.
        let cpu_frac = (proc.cpu_usage() as f64) / 100.0;
        sink.emit(
            Metric::now("process.cpu.utilization", MetricKind::Gauge, cpu_frac)
                .attr("process.executable.name", "harnesssphere")
                .into_signal(),
        );
        sink.emit(
            Metric::now(
                "process.memory.usage",
                MetricKind::UpDownCounter,
                proc.memory() as f64,
            )
            .with_unit("By")
            .attr("process.executable.name", "harnesssphere")
            .into_signal(),
        );
        sink.emit(
            Metric::now(
                "process.memory.virtual",
                MetricKind::UpDownCounter,
                proc.virtual_memory() as f64,
            )
            .with_unit("By")
            .attr("process.executable.name", "harnesssphere")
            .into_signal(),
        );

        Ok(())
    }
}
