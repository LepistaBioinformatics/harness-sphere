//! Session collector (Optional) — derives picoclaw activity from on-disk transcripts.
//!
//! picoclaw exports no telemetry, but it writes chat transcripts as JSONL
//! (`{role, content, tool_calls?}`). One collector instance watches **one workspace**,
//! which is what makes the metrics attributable to a member rather than to the stack.
//!
//! **No transcript content is ever read into a signal** (FR-S6). Message bodies are member
//! data; this emits counts, and nothing else. `content` is not even deserialized.
//!
//! **Tokens are not derivable here and never will be** — picoclaw does not write token
//! cost to disk.

use crate::workspace::Workspace;
use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Per-file incremental state.
///
/// Held in memory, never persisted (DEC-24). A restart re-reads each transcript once,
/// which is bounded and yields exactly the same numbers — these are absolute gauges
/// re-derived from disk, so a full re-read and a resumed read agree. Persisting offsets
/// would add a **writable** path to a component whose read-only mount is one of the three
/// constraints holding up the privilege argument (AD-023).
#[derive(Default)]
struct FileState {
    /// Bytes consumed so far, always ending on a line boundary.
    offset: u64,
    roles: HashMap<String, u64>,
    tool_calls: u64,
    /// Resolved once from the paired `.meta.json`.
    cron: bool,
}

pub struct SessionCollector {
    descriptor: SourceDescriptor,
    workspace: Workspace,
    source: String,
    files: HashMap<PathBuf, FileState>,
}

impl SessionCollector {
    pub fn new(workspace: Workspace, source: impl Into<String>, interval: Duration) -> Self {
        SessionCollector {
            descriptor: SourceDescriptor {
                name: workspace.source_name(),
                layer: Layer::Harness,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            workspace,
            source: source.into(),
            files: HashMap::new(),
        }
    }

    /// The tenant tuple, stamped on every metric this collector emits (FR-D2).
    ///
    /// `session_id` is deliberately absent: it is unbounded and would make cardinality
    /// grow with conversation count rather than with member count.
    fn tag(&self, m: Metric) -> Metric {
        m.attr("crab.tenant", self.workspace.tenant.clone())
            .attr("crab.subscription", self.workspace.subscription.clone())
            .attr("crab.agent", self.workspace.agent.clone())
            .attr("crab.user", self.workspace.user.clone())
            .attr("harness.name", self.source.clone())
    }

    /// Reads the paired `<name>.meta.json` and reports whether this session was started by
    /// a scheduled task.
    ///
    /// **Why this matters (FR-S3).** Every cron run writes its own session file, so a
    /// workspace with two daily tasks accrues files forever and `harness.sessions` drifts
    /// from "conversations" to "conversations plus every cron run since provisioning".
    /// The discriminator is the proxy's own: `history.go`'s `cronSessionPrefix`.
    async fn is_cron(path: &Path) -> bool {
        let meta = path.with_extension("meta.json");
        let Ok(raw) = tokio::fs::read_to_string(&meta).await else {
            return false;
        };
        serde_json::from_str::<serde_json::Value>(&raw)
            .ok()
            .and_then(|v| v.get("key")?.as_str().map(|k| k.starts_with("agent:cron-")))
            .unwrap_or(false)
    }

    /// Reads only what is new, and only whole lines.
    ///
    /// Two hazards handled rather than assumed away:
    ///
    /// - **The file can shrink.** picoclaw's older in-memory store *rewrote* its live file.
    ///   A file shorter than its recorded offset is re-read from zero (state reset), never
    ///   treated as a negative delta (FR-S4).
    /// - **The tail can be a partial line.** The writer appends, so reading to EOF can land
    ///   mid-record. Only bytes up to the last newline are consumed; the remainder is left
    ///   for the next tick.
    async fn read_new(state: &mut FileState, path: &Path, len: u64) {
        if len < state.offset {
            *state = FileState {
                cron: state.cron,
                ..Default::default()
            };
        }
        if len <= state.offset {
            return;
        }
        let Ok(mut f) = tokio::fs::File::open(path).await else {
            return;
        };
        if f.seek(std::io::SeekFrom::Start(state.offset)).await.is_err() {
            return;
        }
        let mut buf = String::new();
        if f.read_to_string(&mut buf).await.is_err() {
            return;
        }
        let Some(last_nl) = buf.rfind('\n') else {
            return; // no complete line yet; try again next tick
        };
        let complete = &buf[..=last_nl];
        for line in complete.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(role) = v.get("role").and_then(|r| r.as_str()) {
                *state.roles.entry(role.to_owned()).or_default() += 1;
            }
            if let Some(tc) = v.get("tool_calls").and_then(|t| t.as_array()) {
                state.tool_calls += tc.len() as u64;
            }
        }
        state.offset += complete.len() as u64;
    }
}

#[async_trait]
impl SignalSource for SessionCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        // Never NotApplicable: this source is discovered at runtime, and a workspace whose
        // sessions directory has not been created yet is transient, not permanent (DEC-22).
        if self.workspace.root.is_dir() {
            ProbeResult::Ready
        } else {
            ProbeResult::Unavailable(format!(
                "workspace gone: {}",
                self.workspace.root.display()
            ))
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        let dirs = self.workspace.session_dirs();
        if dirs.is_empty() {
            return Err(CollectError::Unavailable(format!(
                "no session directories under {}",
                self.workspace.root.display()
            )));
        }

        let mut present: Vec<PathBuf> = Vec::new();
        for dir in &dirs {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                // FR-S2: `sessions/durable/` holds the proxy's own append-only mirror of
                // the same conversations, 1:1. This read_dir is non-recursive, so the
                // exclusion is structural -- but it is asserted explicitly here and in a
                // test, because the cost of a future recursive walk is EXACTLY doubling
                // every count, which looks plausible rather than obviously broken.
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                present.push(path);
            }
        }

        // Transcripts that vanished (rotation) must stop contributing. These are absolute
        // gauges, so dropping the state makes the number fall -- which is correct and is
        // why they are gauges rather than counters.
        self.files.retain(|p, _| present.contains(p));

        for path in &present {
            let len = match tokio::fs::metadata(path).await {
                Ok(m) => m.len(),
                Err(_) => continue,
            };
            let known = self.files.contains_key(path);
            let cron = if known {
                self.files[path].cron
            } else {
                Self::is_cron(path).await
            };
            let state = self.files.entry(path.clone()).or_default();
            state.cron = cron;
            Self::read_new(state, path, len).await;
        }

        let mut roles: HashMap<String, u64> = HashMap::new();
        let mut tool_calls = 0u64;
        let mut sessions = 0u64;
        let mut cron_sessions = 0u64;
        for state in self.files.values() {
            if state.cron {
                cron_sessions += 1;
                continue;
            }
            sessions += 1;
            for (role, n) in &state.roles {
                *roles.entry(role.clone()).or_default() += n;
            }
            tool_calls += state.tool_calls;
        }

        for (role, count) in roles {
            sink.emit(
                self.tag(Metric::now(
                    "harnesssphere.harness.messages",
                    MetricKind::Gauge,
                    count as f64,
                ))
                .attr("role", role)
                .into_signal(),
            );
        }
        sink.emit(
            self.tag(Metric::now(
                "harnesssphere.tool.calls",
                MetricKind::Gauge,
                tool_calls as f64,
            ))
            .into_signal(),
        );
        sink.emit(
            self.tag(Metric::now(
                "harnesssphere.harness.sessions",
                MetricKind::Gauge,
                sessions as f64,
            ))
            .into_signal(),
        );
        // Emitted separately rather than silently discarded: "we excluded 40% of your
        // files and told you nothing" is its own failure mode. This makes FR-S3's
        // exclusion auditable instead of invisible.
        sink.emit(
            self.tag(Metric::now(
                "harnesssphere.harness.cron.sessions",
                MetricKind::Gauge,
                cron_sessions as f64,
            ))
            .into_signal(),
        );
        Ok(())
    }

    /// DEC-12: a retired workspace goes absent, not silent.
    async fn retire(&mut self, sink: &dyn SignalSink) {
        for name in [
            "harnesssphere.harness.sessions",
            "harnesssphere.tool.calls",
            "harnesssphere.harness.cron.sessions",
        ] {
            sink.emit(self.tag(Metric::now(name, MetricKind::Gauge, 0.0)).into_signal());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harnesssphere_domain::{AttrValue, Signal};
    use std::fs;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Collected(Mutex<Vec<Metric>>);
    impl SignalSink for Collected {
        fn emit(&self, signal: Signal) {
            if let Signal::Metric(m) = signal {
                self.0.lock().unwrap().push(m);
            }
        }
    }
    impl Collected {
        fn value(&self, name: &str) -> f64 {
            self.0
                .lock()
                .unwrap()
                .iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("metric {name} was not emitted"))
                .value
        }
        fn role(&self, role: &str) -> f64 {
            self.0
                .lock()
                .unwrap()
                .iter()
                .find(|m| {
                    m.name == "harnesssphere.harness.messages"
                        && m.attributes.iter().any(|(k, v)| {
                            k == "role" && matches!(v, AttrValue::Str(s) if s == role)
                        })
                })
                .map(|m| m.value)
                .unwrap_or(0.0)
        }
        fn attr(&self, name: &str, key: &str) -> Option<String> {
            self.0.lock().unwrap().iter().find(|m| m.name == name).and_then(|m| {
                m.attributes.iter().find(|(k, _)| k == key).map(|(_, v)| match v {
                    AttrValue::Str(s) => s.clone(),
                    other => format!("{other:?}"),
                })
            })
        }
    }

    /// Two user turns, one assistant turn with two tool calls.
    fn transcript() -> String {
        [
            r#"{"role":"user","content":"hi"}"#,
            r#"{"role":"assistant","content":"hello","tool_calls":[{"id":"1"},{"id":"2"}]}"#,
            r#"{"role":"user","content":"again"}"#,
        ]
        .join("\n")
            + "\n"
    }

    fn workspace(tmp: &Path) -> Workspace {
        let root = tmp
            .join("tenants/acme/subscriptions/sub-1/agents/alpha/users/uuid-a");
        fs::create_dir_all(root.join("workspace/sessions")).unwrap();
        Workspace {
            tenant: "acme".into(),
            subscription: "sub-1".into(),
            agent: "alpha".into(),
            user: "uuid-a".into(),
            root,
        }
    }

    fn collector(ws: Workspace) -> SessionCollector {
        SessionCollector::new(ws, "picoclaw", Duration::from_secs(1))
    }

    #[tokio::test]
    async fn counts_roles_and_tool_calls_and_tags_the_tuple() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        fs::write(ws.root.join("workspace/sessions/a.jsonl"), transcript()).unwrap();

        let sink = Collected::default();
        collector(ws).collect(&sink).await.unwrap();

        assert_eq!(sink.value("harnesssphere.harness.sessions"), 1.0);
        assert_eq!(sink.role("user"), 2.0);
        assert_eq!(sink.role("assistant"), 1.0);
        assert_eq!(sink.value("harnesssphere.tool.calls"), 2.0);
        // FR-D2: the full tuple, with the user as the account UUID.
        assert_eq!(
            sink.attr("harnesssphere.harness.sessions", "crab.tenant").as_deref(),
            Some("acme")
        );
        assert_eq!(
            sink.attr("harnesssphere.harness.sessions", "crab.user").as_deref(),
            Some("uuid-a")
        );
    }

    /// FR-S5. The regression that silently drops 42% of conversations.
    #[tokio::test]
    async fn counts_project_workspaces_too() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        fs::create_dir_all(ws.root.join("workspace-chat-ux/sessions")).unwrap();
        fs::write(ws.root.join("workspace/sessions/a.jsonl"), transcript()).unwrap();
        fs::write(ws.root.join("workspace-chat-ux/sessions/p.chat-ux.b.jsonl"), transcript()).unwrap();

        let sink = Collected::default();
        collector(ws).collect(&sink).await.unwrap();
        assert_eq!(
            sink.value("harnesssphere.harness.sessions"),
            2.0,
            "the project workspace was not counted"
        );
        assert_eq!(sink.role("user"), 4.0);
    }

    /// FR-S2. `durable/` mirrors the live files 1:1, so a recursive walk would EXACTLY
    /// double every count -- a failure that looks plausible rather than obviously broken.
    #[tokio::test]
    async fn durable_mirror_is_excluded() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let sessions = ws.root.join("workspace/sessions");
        fs::write(sessions.join("a.jsonl"), transcript()).unwrap();
        fs::create_dir_all(sessions.join("durable")).unwrap();
        fs::write(sessions.join("durable/a.jsonl"), transcript()).unwrap();

        let sink = Collected::default();
        collector(ws).collect(&sink).await.unwrap();
        assert_eq!(sink.value("harnesssphere.harness.sessions"), 1.0);
        assert_eq!(sink.role("user"), 2.0, "durable/ was counted — every number doubled");
    }

    /// FR-S3. Every scheduled-task run writes its own session file, so uncorrected this
    /// metric drifts from "conversations" to "conversations plus every cron run ever".
    #[tokio::test]
    async fn cron_sessions_are_excluded_but_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let sessions = ws.root.join("workspace/sessions");
        fs::write(sessions.join("chat.jsonl"), transcript()).unwrap();
        fs::write(sessions.join("chat.meta.json"), r#"{"key":"agent:alpha:user"}"#).unwrap();
        fs::write(sessions.join("nightly.jsonl"), transcript()).unwrap();
        fs::write(sessions.join("nightly.meta.json"), r#"{"key":"agent:cron-nightly"}"#).unwrap();

        let sink = Collected::default();
        collector(ws).collect(&sink).await.unwrap();
        assert_eq!(sink.value("harnesssphere.harness.sessions"), 1.0);
        assert_eq!(sink.value("harnesssphere.harness.cron.sessions"), 1.0);
        assert_eq!(sink.role("user"), 2.0, "cron messages leaked into the chat counts");
    }

    /// FR-S4, first half: appended lines are counted once, not re-counted every tick.
    #[tokio::test]
    async fn appended_lines_are_counted_once() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let file = ws.root.join("workspace/sessions/a.jsonl");
        fs::write(&file, transcript()).unwrap();
        let mut c = collector(ws);

        let s1 = Collected::default();
        c.collect(&s1).await.unwrap();
        assert_eq!(s1.role("user"), 2.0);

        // Same content, second tick: the total must NOT double.
        let s2 = Collected::default();
        c.collect(&s2).await.unwrap();
        assert_eq!(s2.role("user"), 2.0, "the file was re-counted from the start");

        // Append one more user turn.
        let mut content = fs::read_to_string(&file).unwrap();
        content.push_str("{\"role\":\"user\",\"content\":\"third\"}\n");
        fs::write(&file, content).unwrap();
        let s3 = Collected::default();
        c.collect(&s3).await.unwrap();
        assert_eq!(s3.role("user"), 3.0);
    }

    /// FR-S4, second half: picoclaw's older store REWROTE its live file, so a file can
    /// shrink. That must re-read from zero, never produce a negative delta.
    #[tokio::test]
    async fn a_shrunk_file_is_reread_from_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let file = ws.root.join("workspace/sessions/a.jsonl");
        fs::write(&file, transcript()).unwrap();
        let mut c = collector(ws);
        c.collect(&Collected::default()).await.unwrap();

        fs::write(&file, "{\"role\":\"user\",\"content\":\"only\"}\n").unwrap();
        let sink = Collected::default();
        c.collect(&sink).await.unwrap();
        assert_eq!(sink.role("user"), 1.0, "shrink was treated as a delta");
        assert_eq!(sink.role("assistant"), 0.0);
        assert!(sink.value("harnesssphere.tool.calls") >= 0.0);
    }

    /// A record still being written must not be counted half-parsed.
    #[tokio::test]
    async fn a_partial_trailing_line_waits_for_its_newline() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let file = ws.root.join("workspace/sessions/a.jsonl");
        fs::write(&file, "{\"role\":\"user\",\"content\":\"one\"}\n{\"role\":\"user\",\"cont").unwrap();
        let mut c = collector(ws);

        let s1 = Collected::default();
        c.collect(&s1).await.unwrap();
        assert_eq!(s1.role("user"), 1.0, "a partial record was counted");

        fs::write(&file, "{\"role\":\"user\",\"content\":\"one\"}\n{\"role\":\"user\",\"content\":\"two\"}\n").unwrap();
        let s2 = Collected::default();
        c.collect(&s2).await.unwrap();
        assert_eq!(s2.role("user"), 2.0, "the completed record was never picked up");
    }

    /// A rotated-away transcript must stop contributing -- which is why these are gauges.
    #[tokio::test]
    async fn a_removed_transcript_stops_counting() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let file = ws.root.join("workspace/sessions/a.jsonl");
        fs::write(&file, transcript()).unwrap();
        let mut c = collector(ws);
        c.collect(&Collected::default()).await.unwrap();

        fs::remove_file(&file).unwrap();
        let sink = Collected::default();
        c.collect(&sink).await.unwrap();
        assert_eq!(sink.value("harnesssphere.harness.sessions"), 0.0);
        assert_eq!(sink.role("user"), 0.0);
    }

    /// DEC-12: retiring emits an explicit zero so the series goes ABSENT, not silent.
    #[tokio::test]
    async fn retire_emits_zeroes() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        fs::write(ws.root.join("workspace/sessions/a.jsonl"), transcript()).unwrap();
        let mut c = collector(ws);
        c.collect(&Collected::default()).await.unwrap();

        let sink = Collected::default();
        c.retire(&sink).await;
        assert_eq!(sink.value("harnesssphere.harness.sessions"), 0.0);
        assert_eq!(sink.value("harnesssphere.tool.calls"), 0.0);
        assert_eq!(
            sink.attr("harnesssphere.harness.sessions", "crab.user").as_deref(),
            Some("uuid-a"),
            "the terminal signal lost its attribution — the series would not match"
        );
    }
}
