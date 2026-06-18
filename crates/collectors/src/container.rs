//! Container collector (Optional) — reads a container's **cgroup v2** files directly.
//!
//! No Docker socket, no runtime API, no extra privileges: point it at a container's cgroup
//! v2 directory (e.g. `/sys/fs/cgroup/system.slice/docker-<id>.scope`) and it reads
//! `memory.current`, `memory.max`, `memory.events`, `cpu.stat` and `io.stat`. Maps to
//! `container.*` (+ `harnesssphere.container.*` where no semconv exists). Harness-independent.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

pub struct ContainerCollector {
    descriptor: SourceDescriptor,
    cgroup: PathBuf,
    container_id: String,
}

impl ContainerCollector {
    /// `cgroup` is the container's cgroup v2 directory; `container_id` labels the metrics.
    pub fn new(cgroup: impl Into<String>, container_id: impl Into<String>, interval: Duration) -> Self {
        ContainerCollector {
            descriptor: SourceDescriptor {
                name: "container",
                layer: Layer::Container,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            cgroup: PathBuf::from(cgroup.into()),
            container_id: container_id.into(),
        }
    }

    fn read(&self, file: &str) -> Option<String> {
        std::fs::read_to_string(self.cgroup.join(file)).ok()
    }
}

/// A single integer (e.g. `memory.current`).
fn parse_u64(content: &str) -> Option<u64> {
    content.trim().parse().ok()
}

/// `memory.max`: an integer, or the literal `max` (unlimited → None).
fn parse_mem_max(content: &str) -> Option<u64> {
    let t = content.trim();
    if t == "max" {
        None
    } else {
        t.parse().ok()
    }
}

/// Lines of `key value` (e.g. `cpu.stat`, `memory.events`).
fn parse_kv_lines(content: &str) -> HashMap<String, u64> {
    content
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let k = it.next()?;
            let v: u64 = it.next()?.parse().ok()?;
            Some((k.to_owned(), v))
        })
        .collect()
}

/// `io.stat`: sum `rbytes=`/`wbytes=` across all devices.
fn parse_io_stat(content: &str) -> (u64, u64) {
    let (mut r, mut w) = (0u64, 0u64);
    for line in content.lines() {
        for field in line.split_whitespace() {
            if let Some(v) = field.strip_prefix("rbytes=") {
                r += v.parse::<u64>().unwrap_or(0);
            } else if let Some(v) = field.strip_prefix("wbytes=") {
                w += v.parse::<u64>().unwrap_or(0);
            }
        }
    }
    (r, w)
}

#[async_trait]
impl SignalSource for ContainerCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        if self.cgroup.join("memory.current").is_file() {
            ProbeResult::Ready
        } else {
            ProbeResult::Unavailable(format!(
                "no cgroup v2 at {} (memory.current missing)",
                self.cgroup.display()
            ))
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        let id = self.container_id.clone();

        if let Some(used) = self.read("memory.current").as_deref().and_then(parse_u64) {
            sink.emit(
                Metric::now("container.memory.usage", MetricKind::UpDownCounter, used as f64)
                    .with_unit("By")
                    .attr("container.id", id.clone())
                    .into_signal(),
            );
        }
        if let Some(limit) = self.read("memory.max").as_deref().and_then(parse_mem_max) {
            sink.emit(
                Metric::now("harnesssphere.container.memory.limit", MetricKind::Gauge, limit as f64)
                    .with_unit("By")
                    .attr("container.id", id.clone())
                    .into_signal(),
            );
        }
        if let Some(ev) = self.read("memory.events").as_deref().map(parse_kv_lines) {
            if let Some(&oom) = ev.get("oom_kill") {
                sink.emit(
                    Metric::now("harnesssphere.container.memory.oom", MetricKind::Counter, oom as f64)
                        .attr("container.id", id.clone())
                        .into_signal(),
                );
            }
        }
        if let Some(cpu) = self.read("cpu.stat").as_deref().map(parse_kv_lines) {
            if let Some(&usec) = cpu.get("usage_usec") {
                sink.emit(
                    Metric::now("container.cpu.time", MetricKind::Counter, usec as f64 / 1_000_000.0)
                        .with_unit("s")
                        .attr("container.id", id.clone())
                        .into_signal(),
                );
            }
            if let Some(&thr) = cpu.get("throttled_usec") {
                sink.emit(
                    Metric::now(
                        "harnesssphere.container.cpu.throttled",
                        MetricKind::Counter,
                        thr as f64 / 1_000_000.0,
                    )
                    .with_unit("s")
                    .attr("container.id", id.clone())
                    .into_signal(),
                );
            }
        }
        if let Some(io) = self.read("io.stat").as_deref().map(parse_io_stat) {
            let (rbytes, wbytes) = io;
            sink.emit(
                Metric::now("container.disk.io", MetricKind::Counter, rbytes as f64)
                    .with_unit("By")
                    .attr("container.id", id.clone())
                    .attr("disk.io.direction", "read")
                    .into_signal(),
            );
            sink.emit(
                Metric::now("container.disk.io", MetricKind::Counter, wbytes as f64)
                    .with_unit("By")
                    .attr("container.id", id)
                    .attr("disk.io.direction", "write")
                    .into_signal(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_files() {
        assert_eq!(parse_u64("  12345\n"), Some(12345));
        assert_eq!(parse_mem_max("max\n"), None);
        assert_eq!(parse_mem_max("9999\n"), Some(9999));

        let cpu = parse_kv_lines("usage_usec 1000000\nnr_throttled 3\nthrottled_usec 500000\n");
        assert_eq!(cpu.get("usage_usec"), Some(&1_000_000));
        assert_eq!(cpu.get("throttled_usec"), Some(&500_000));

        let (r, w) = parse_io_stat("8:0 rbytes=100 wbytes=200 rios=1 wios=2\n259:0 rbytes=50 wbytes=0\n");
        assert_eq!((r, w), (150, 200));
    }
}
