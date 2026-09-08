//! Learning collector (Optional) — what an agent has *retained*, as opposed to how much
//! it has talked.
//!
//! One instance per workspace, on a slower cadence than the session collector: skills,
//! memory files and the knowledge graph change rarely, while transcripts change
//! constantly (DEC-28).
//!
//! **No content is ever read into a signal** (DEC-27, FR-L9). Skill *names* are emitted,
//! because they are agent-authored capability names bounded at roughly ten per instance
//! (DEC-26). Entity names, relation names, observation text and memory file contents are
//! not — they are extracted from member conversations and their cardinality is unbounded.

use crate::workspace::Workspace;
use async_trait::async_trait;
use harnesssphere_domain::{
    CollectError, Criticality, Layer, Metric, MetricKind, ProbeResult, SignalSink, SignalSource,
    SourceDescriptor,
};
use std::path::Path;
use std::time::Duration;

#[derive(Default, Debug, PartialEq)]
struct Counts {
    skills: u64,
    skill_names: Vec<String>,
    memory_files: u64,
    memory_bytes: u64,
    entities: u64,
    relations: u64,
    observations: u64,
}

pub struct LearningCollector {
    descriptor: SourceDescriptor,
    workspace: Workspace,
    source: String,
}

impl LearningCollector {
    pub fn new(workspace: Workspace, source: impl Into<String>, interval: Duration) -> Self {
        LearningCollector {
            descriptor: SourceDescriptor {
                name: format!("learning:{}", workspace.source_name()),
                layer: Layer::Harness,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            workspace,
            source: source.into(),
        }
    }

    fn tag(&self, m: Metric) -> Metric {
        m.attr("crab.tenant", self.workspace.tenant.clone())
            .attr("crab.subscription", self.workspace.subscription.clone())
            .attr("crab.agent", self.workspace.agent.clone())
            .attr("crab.user", self.workspace.user.clone())
            .attr("harness.name", self.source.clone())
    }

    /// A skill is a directory containing `SKILL.md`, not merely a directory.
    ///
    /// **Measured:** the live workspace holds 10 directories under `workspace/skills` and
    /// 9 `SKILL.md` files — `shared-content/` is a directory that is not a skill. Counting
    /// directories overcounts, and the error grows with whatever else the agent leaves in
    /// that tree.
    fn count_skills(dir: &Path, out: &mut Counts) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() || !path.join("SKILL.md").is_file() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                out.skills += 1;
                out.skill_names.push(name.to_owned());
            }
        }
    }

    /// Only **non-empty** files count, and only their bytes are summed.
    ///
    /// **Measured:** `workspace/memory` holds four files, three of them **zero bytes** —
    /// `CONTEXT_RECOVERY.md`, `FILE_DELIVERY.md`, `MEMORY_ROUTING.md`, all created at
    /// provisioning. A plain file count reports 4 where the truth is 1: a **4× overcount**
    /// on the one metric whose entire purpose is to say whether the agent retained
    /// anything. Every freshly provisioned instance would read as already having memory.
    fn count_memory(dir: &Path, out: &mut Counts) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() || meta.len() == 0 {
                continue;
            }
            out.memory_files += 1;
            out.memory_bytes += meta.len();
        }
    }

    /// Parses the graph, counting **records** and summing observation array *lengths*.
    ///
    /// Two things this must not do. It must not count **lines**: the live file holds 20
    /// records while `wc -l` returns 19, because the last line carries no trailing
    /// newline — an undercount that is permanent and silent. And it must not read the
    /// observation *strings*: `observations` is an array of facts extracted from the
    /// member's conversations, so its length is the learning volume and its contents are
    /// member data (DEC-27).
    async fn count_graph(path: &Path, out: &mut Counts) {
        let Ok(raw) = tokio::fs::read_to_string(path).await else {
            return;
        };
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // A record that does not parse is skipped, never counted as either kind.
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            match v.get("type").and_then(|t| t.as_str()) {
                Some("entity") => {
                    out.entities += 1;
                    if let Some(obs) = v.get("observations").and_then(|o| o.as_array()) {
                        out.observations += obs.len() as u64;
                    }
                }
                Some("relation") => out.relations += 1,
                _ => {}
            }
        }
    }

    async fn gather(&self) -> Counts {
        let mut c = Counts::default();
        for dir in self.workspace.skill_dirs() {
            Self::count_skills(&dir, &mut c);
        }
        for dir in self.workspace.memory_dirs() {
            Self::count_memory(&dir, &mut c);
        }
        for file in self.workspace.graph_files() {
            Self::count_graph(&file, &mut c).await;
        }
        c.skill_names.sort();
        c.skill_names.dedup();
        c
    }
}

#[async_trait]
impl SignalSource for LearningCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        // Never NotApplicable: discovered at runtime, so a workspace that has not been
        // populated yet is transient, not permanent (DEC-22).
        if self.workspace.root.is_dir() {
            ProbeResult::Ready
        } else {
            ProbeResult::Unavailable(format!("workspace gone: {}", self.workspace.root.display()))
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        let c = self.gather().await;

        for (name, value) in [
            ("harnesssphere.harness.skills", c.skills),
            ("harnesssphere.harness.memory.files", c.memory_files),
            ("harnesssphere.graph.entities", c.entities),
            ("harnesssphere.graph.relations", c.relations),
            ("harnesssphere.graph.observations", c.observations),
        ] {
            sink.emit(
                self.tag(Metric::now(name, MetricKind::Gauge, value as f64))
                    .into_signal(),
            );
        }
        sink.emit(
            self.tag(Metric::now(
                "harnesssphere.harness.memory.bytes",
                MetricKind::Gauge,
                c.memory_bytes as f64,
            ))
            .with_unit("By")
            .into_signal(),
        );

        // One series per skill (DEC-26). Bounded by what the agent authored -- roughly ten
        // per instance -- which is what makes the name affordable as a label.
        for name in &c.skill_names {
            sink.emit(
                self.tag(Metric::now(
                    "harnesssphere.harness.skill",
                    MetricKind::Gauge,
                    1.0,
                ))
                .attr("skill.name", name.clone())
                .into_signal(),
            );
        }
        Ok(())
    }

    /// DEC-12: a retired workspace goes absent, not silent.
    async fn retire(&mut self, sink: &dyn SignalSink) {
        for name in [
            "harnesssphere.harness.skills",
            "harnesssphere.harness.memory.files",
            "harnesssphere.harness.memory.bytes",
            "harnesssphere.graph.entities",
            "harnesssphere.graph.relations",
            "harnesssphere.graph.observations",
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
    use std::path::PathBuf;
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
                .unwrap_or_else(|| panic!("{name} was not emitted"))
                .value
        }
        fn skill_names(&self) -> Vec<String> {
            let mut v: Vec<String> = self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.name == "harnesssphere.harness.skill")
                .filter_map(|m| {
                    m.attributes.iter().find(|(k, _)| k == "skill.name").map(|(_, v)| match v {
                        AttrValue::Str(s) => s.clone(),
                        other => format!("{other:?}"),
                    })
                })
                .collect();
            v.sort();
            v
        }
    }

    fn workspace(tmp: &Path) -> Workspace {
        let root = tmp.join("tenants/acme/subscriptions/s1/agents/alpha/users/u1");
        fs::create_dir_all(&root).unwrap();
        Workspace {
            tenant: "acme".into(),
            subscription: "s1".into(),
            agent: "alpha".into(),
            user: "u1".into(),
            root,
        }
    }

    fn skill(root: &Path, ws: &str, name: &str, with_manifest: bool) {
        let d: PathBuf = root.join(ws).join("skills").join(name);
        fs::create_dir_all(&d).unwrap();
        if with_manifest {
            fs::write(d.join("SKILL.md"), "# skill\n").unwrap();
        }
    }

    async fn run(ws: Workspace) -> Collected {
        let sink = Collected::default();
        LearningCollector::new(ws, "picoclaw", Duration::from_secs(1))
            .collect(&sink)
            .await
            .unwrap();
        sink
    }

    /// FR-L3. Measured on the live workspace: 10 directories, 9 SKILL.md.
    #[tokio::test]
    async fn a_directory_without_skill_md_is_not_a_skill() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        skill(&ws.root, "workspace", "github", true);
        skill(&ws.root, "workspace", "weather", true);
        skill(&ws.root, "workspace", "shared-content", false); // the real one, verbatim

        let sink = run(ws).await;
        assert_eq!(sink.value("harnesssphere.harness.skills"), 2.0);
        assert_eq!(sink.skill_names(), vec!["github", "weather"]);
    }

    /// FR-L3 across both workspace shapes.
    #[tokio::test]
    async fn skills_in_project_workspaces_are_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        skill(&ws.root, "workspace", "github", true);
        skill(&ws.root, "workspace-chat-ux", "summarize", true);

        let sink = run(ws).await;
        assert_eq!(sink.value("harnesssphere.harness.skills"), 2.0);
        assert_eq!(sink.skill_names(), vec!["github", "summarize"]);
    }

    /// FR-L5. The 4x overcount: three of the four real files are zero-byte scaffolds.
    #[tokio::test]
    async fn empty_memory_scaffolds_are_excluded() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let mem = ws.root.join("workspace/memory");
        fs::create_dir_all(&mem).unwrap();
        for empty in ["CONTEXT_RECOVERY.md", "FILE_DELIVERY.md", "MEMORY_ROUTING.md"] {
            fs::write(mem.join(empty), "").unwrap();
        }
        fs::write(mem.join("MEMORY.md"), "x".repeat(317)).unwrap();

        let sink = run(ws).await;
        assert_eq!(
            sink.value("harnesssphere.harness.memory.files"),
            1.0,
            "provisioning scaffolds were counted — a fresh instance reads as having memory"
        );
        assert_eq!(sink.value("harnesssphere.harness.memory.bytes"), 317.0);
    }

    /// FR-L7 + FR-L8. 20 records where `wc -l` says 19, and observations are summed.
    #[tokio::test]
    async fn graph_counts_records_not_lines_and_sums_observations() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let g = ws.root.join("memory-graph-chat-ux");
        fs::create_dir_all(&g).unwrap();

        let mut lines: Vec<String> = Vec::new();
        for i in 0..10 {
            lines.push(format!(
                r#"{{"type":"entity","name":"e{i}","observations":["a","b","c"]}}"#
            ));
        }
        for i in 0..10 {
            lines.push(format!(r#"{{"type":"relation","from":"e{i}","to":"e0"}}"#));
        }
        // NO trailing newline — exactly like the live file, where wc -l reports 19.
        fs::write(g.join("memory.jsonl"), lines.join("\n")).unwrap();

        let sink = run(ws).await;
        assert_eq!(sink.value("harnesssphere.graph.entities"), 10.0);
        assert_eq!(
            sink.value("harnesssphere.graph.relations"),
            10.0,
            "the last record was lost — lines were counted instead of records"
        );
        assert_eq!(sink.value("harnesssphere.graph.observations"), 30.0);
    }

    /// FR-L6. The graph is per PROJECT at the user root, not inside the workspace.
    #[tokio::test]
    async fn every_project_graph_is_summed() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        for project in ["memory-graph-chat-ux", "memory-graph-ops"] {
            let g = ws.root.join(project);
            fs::create_dir_all(&g).unwrap();
            fs::write(
                g.join("memory.jsonl"),
                "{\"type\":\"entity\",\"observations\":[\"a\"]}\n",
            )
            .unwrap();
        }
        let sink = run(ws).await;
        assert_eq!(sink.value("harnesssphere.graph.entities"), 2.0);
        assert_eq!(sink.value("harnesssphere.graph.observations"), 2.0);
    }

    /// FR-L7: a malformed record is skipped, never counted as either kind.
    #[tokio::test]
    async fn malformed_records_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let g = ws.root.join("memory-graph-x");
        fs::create_dir_all(&g).unwrap();
        fs::write(
            g.join("memory.jsonl"),
            "{\"type\":\"entity\",\"observations\":[\"a\"]}\nnot json at all\n{\"no_type\":1}\n",
        )
        .unwrap();

        let sink = run(ws).await;
        assert_eq!(sink.value("harnesssphere.graph.entities"), 1.0);
        assert_eq!(sink.value("harnesssphere.graph.relations"), 0.0);
    }

    /// FR-L9: nothing but the skill name ever leaves the disk.
    #[tokio::test]
    async fn no_entity_name_or_observation_text_is_emitted() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = workspace(tmp.path());
        let g = ws.root.join("memory-graph-x");
        fs::create_dir_all(&g).unwrap();
        fs::write(
            g.join("memory.jsonl"),
            r#"{"type":"entity","name":"SECRET_ENTITY","observations":["SECRET_FACT"]}"#,
        )
        .unwrap();

        let sink = run(ws).await;
        let all = format!("{:?}", sink.0.lock().unwrap());
        assert!(!all.contains("SECRET_ENTITY"), "an entity name reached a signal");
        assert!(!all.contains("SECRET_FACT"), "observation text reached a signal");
    }

    #[tokio::test]
    async fn an_empty_workspace_reports_zeroes_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = run(workspace(tmp.path())).await;
        assert_eq!(sink.value("harnesssphere.harness.skills"), 0.0);
        assert_eq!(sink.value("harnesssphere.graph.entities"), 0.0);
        assert_eq!(sink.value("harnesssphere.harness.memory.files"), 0.0);
        assert_eq!(sink.value("harnesssphere.harness.memory.bytes"), 0.0);
    }
}
