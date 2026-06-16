//! Coletor Host (Critical) — CPU, memória e swap via `sysinfo`.
//! Mapeia para semantic conventions `system.*`.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::time::Duration;
use sysinfo::System;

pub struct HostCollector {
    descriptor: SourceDescriptor,
    sys: System,
}

impl HostCollector {
    pub fn new(interval: Duration) -> Self {
        HostCollector {
            descriptor: SourceDescriptor {
                name: "host",
                layer: Layer::Host,
                criticality: Criticality::Critical,
                default_interval: interval,
            },
            sys: System::new(),
        }
    }
}

#[async_trait]
impl SignalSource for HostCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        // O host está sempre presente; se nem isto refresca, é fatal (Critical).
        self.sys.refresh_memory();
        if self.sys.total_memory() == 0 {
            ProbeResult::Fatal("não foi possível ler memória do host".into())
        } else {
            ProbeResult::Ready
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        self.sys.refresh_cpu_all();
        self.sys.refresh_memory();

        // sysinfo só dá utilização agregada (sem breakdown user/system/idle), então
        // emitimos sem `system.cpu.state` em vez de inventar um valor fora da semconv.
        let cpu_frac = (self.sys.global_cpu_usage() as f64) / 100.0;
        sink.emit(Metric::now("system.cpu.utilization", MetricKind::Gauge, cpu_frac).into_signal());

        let total = self.sys.total_memory();
        let used = self.sys.used_memory();
        let free = self.sys.free_memory();
        let available = self.sys.available_memory();

        for (state, bytes) in [("used", used), ("free", free), ("available", available)] {
            sink.emit(
                Metric::now("system.memory.usage", MetricKind::UpDownCounter, bytes as f64)
                    .with_unit("By")
                    .attr("system.memory.state", state)
                    .into_signal(),
            );
        }
        if total > 0 {
            sink.emit(
                Metric::now(
                    "system.memory.utilization",
                    MetricKind::Gauge,
                    used as f64 / total as f64,
                )
                .attr("system.memory.state", "used")
                .into_signal(),
            );
        }

        let total_swap = self.sys.total_swap();
        let used_swap = self.sys.used_swap();
        sink.emit(
            Metric::now("system.paging.usage", MetricKind::UpDownCounter, used_swap as f64)
                .with_unit("By")
                .attr("system.paging.state", "used")
                .into_signal(),
        );
        if total_swap > 0 {
            sink.emit(
                Metric::now(
                    "system.paging.utilization",
                    MetricKind::Gauge,
                    used_swap as f64 / total_swap as f64,
                )
                .into_signal(),
            );
        }

        Ok(())
    }
}
