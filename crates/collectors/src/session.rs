//! Session collector (Optional) — derives AI activity from a harness's on-disk session
//! files, the way ClawMetry's reader adapters do.
//!
//! PicoClaw doesn't export telemetry, but it writes chat-style session transcripts as
//! JSONL (`{role, content, tool_calls?}`) under `~/.picoclaw/workspace/sessions/`. We
//! parse them into message-by-role and tool-call counts. **Token counts are NOT derivable
//! here** — PicoClaw (like NanoClaw/Cursor) doesn't write token cost to disk.
//!
//! Emits `harnesssphere.harness.messages` (by `role`), `harnesssphere.tool.calls`,
//! `harnesssphere.harness.sessions` — all absolute Gauges tagged `harness.name`.

use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

pub struct SessionCollector {
    descriptor: SourceDescriptor,
    dir: PathBuf,
    source: String,
}

fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_owned()
}

impl SessionCollector {
    /// `dir` is the sessions directory (a leading `~/` is expanded); `source` labels the
    /// harness (e.g. "picoclaw").
    pub fn new(dir: impl Into<String>, source: impl Into<String>, interval: Duration) -> Self {
        SessionCollector {
            descriptor: SourceDescriptor {
                name: "session",
                layer: Layer::Harness,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            dir: PathBuf::from(expand_tilde(&dir.into())),
            source: source.into(),
        }
    }
}

#[async_trait]
impl SignalSource for SessionCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        if self.dir.is_dir() {
            ProbeResult::Ready
        } else {
            ProbeResult::Unavailable(format!("session dir not found: {}", self.dir.display()))
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| CollectError::Unavailable(format!("read {}: {e}", self.dir.display())))?;

        let mut roles: HashMap<String, u64> = HashMap::new();
        let mut tool_calls: u64 = 0;
        let mut sessions: u64 = 0;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            sessions += 1;
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                if let Some(role) = v.get("role").and_then(|r| r.as_str()) {
                    *roles.entry(role.to_owned()).or_default() += 1;
                }
                if let Some(tc) = v.get("tool_calls").and_then(|t| t.as_array()) {
                    tool_calls += tc.len() as u64;
                }
            }
        }

        for (role, count) in roles {
            sink.emit(
                Metric::now("harnesssphere.harness.messages", MetricKind::Gauge, count as f64)
                    .attr("role", role)
                    .attr("harness.name", self.source.clone())
                    .into_signal(),
            );
        }
        sink.emit(
            Metric::now("harnesssphere.tool.calls", MetricKind::Gauge, tool_calls as f64)
                .attr("harness.name", self.source.clone())
                .into_signal(),
        );
        sink.emit(
            Metric::now("harnesssphere.harness.sessions", MetricKind::Gauge, sessions as f64)
                .attr("harness.name", self.source.clone())
                .into_signal(),
        );
        Ok(())
    }
}
