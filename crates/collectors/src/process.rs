//! Process collector (Optional) — watches arbitrary host processes by name.
//!
//! Lets HarnessSphere observe a co-located component (e.g. the PicoClaw gateway) that
//! doesn't export its own telemetry: we read its CPU/memory from the OS and tag each
//! sample with the process name + pid. Harness-independent infrastructure visibility.
//! Maps to `process.*` with `process.executable.name` / `process.pid` attributes.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::time::Duration;
use sysinfo::{ProcessesToUpdate, System};

pub struct ProcessCollector {
    descriptor: SourceDescriptor,
    sys: System,
    /// Substrings matched against each process's executable name.
    names: Vec<String>,
}

impl ProcessCollector {
    pub fn new(names: Vec<String>, interval: Duration) -> Self {
        ProcessCollector {
            descriptor: SourceDescriptor {
                name: "process",
                layer: Layer::Host,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            sys: System::new(),
            names,
        }
    }
}

#[async_trait]
impl SignalSource for ProcessCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        if self.names.is_empty() {
            ProbeResult::NotApplicable
        } else {
            ProbeResult::Ready
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        self.sys.refresh_processes(ProcessesToUpdate::All, true);
        for (pid, proc_) in self.sys.processes() {
            let exe = proc_.name().to_string_lossy().to_string();
            if !self.names.iter().any(|n| exe.contains(n.as_str())) {
                continue;
            }
            let pid_i = pid.as_u32() as i64;
            // sysinfo reports CPU as a percentage of a single core; divide by 100 for a fraction.
            sink.emit(
                Metric::now("process.cpu.utilization", MetricKind::Gauge, proc_.cpu_usage() as f64 / 100.0)
                    .attr("process.executable.name", exe.clone())
                    .attr("process.pid", pid_i)
                    .into_signal(),
            );
            sink.emit(
                Metric::now("process.memory.usage", MetricKind::UpDownCounter, proc_.memory() as f64)
                    .with_unit("By")
                    .attr("process.executable.name", exe.clone())
                    .attr("process.pid", pid_i)
                    .into_signal(),
            );
            sink.emit(
                Metric::now(
                    "process.memory.virtual",
                    MetricKind::UpDownCounter,
                    proc_.virtual_memory() as f64,
                )
                .with_unit("By")
                .attr("process.executable.name", exe)
                .attr("process.pid", pid_i)
                .into_signal(),
            );
        }
        Ok(())
    }
}
