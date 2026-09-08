//! Workspace discovery — turns crab-shell-proxy's on-disk tenant tree into the set of
//! per-user workspaces to watch.
//!
//! This is the **disk surface** of DEC-10's two-surface reconcile. It is deliberately the
//! resilient one: it keeps working when the proxy is down, which is exactly when its
//! metrics matter most.
//!
//! **Bounded by construction (FR-D13).** Four levels of `read_dir` and one more inside the
//! user directory. It never walks a transcript; reading session *content* is the session
//! collector's job, on its own slower cadence.

use std::path::{Path, PathBuf};

/// One `(tenant, subscription, agent, user)` workspace.
///
/// The tuple is carried in full because the container name hashes it **one way**
/// (`<prefix>-<role>-<sha256(tenant::subs::user)[:16]>`), so it cannot be recovered from
/// the container. The directory path is the only place it survives in readable form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub tenant: String,
    pub subscription: String,
    pub agent: String,
    /// The mycelium account UUID — never the email (F1 DEC-4).
    pub user: String,
    /// `<data_root>/tenants/<tenant>/subscriptions/<subs>/agents/<agent>/users/<user>`
    pub root: PathBuf,
}

impl Workspace {
    /// Stable identity, and the supervisor's registry key (FR-D8). Unique per instance,
    /// which is what makes add/remove addressable.
    pub fn source_name(&self) -> String {
        format!(
            "session:{}/{}/{}/{}",
            self.tenant, self.subscription, self.agent, self.user
        )
    }

    /// Every session directory under this workspace.
    ///
    /// **There are TWO shapes, not one, and missing the second is silent.** Project
    /// conversations live in a sibling `workspace-<project>/sessions`, not in
    /// `workspace/sessions`. Measured on a live deployment: a collector globbing only the
    /// latter drops **5 of 12 conversations — 42%** — with no error and no warning, just a
    /// smaller number that looks correct.
    pub fn session_dirs(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let mut dirs: Vec<PathBuf> = entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_str()?;
                if name != "workspace" && !name.starts_with("workspace-") {
                    return None;
                }
                let sessions = e.path().join("sessions");
                sessions.is_dir().then_some(sessions)
            })
            .collect();
        dirs.sort();
        dirs
    }
}

/// Immediate subdirectory names of `dir`, sorted. Missing or unreadable → empty, never an
/// error: a tree that is being created underneath us is normal, not a fault.
fn subdirs(dir: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<(String, PathBuf)> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| Some((e.file_name().to_str()?.to_owned(), e.path())))
        .collect();
    out.sort();
    out
}

/// Walks `<data_root>/tenants/*/subscriptions/*/agents/*/users/*`.
///
/// Deterministically ordered, so two consecutive scans of an unchanged tree produce
/// identical output and the discovery diff is empty rather than churning the registry.
pub fn discover(data_root: &Path) -> Vec<Workspace> {
    let mut out = Vec::new();
    for (tenant, tdir) in subdirs(&data_root.join("tenants")) {
        for (subscription, sdir) in subdirs(&tdir.join("subscriptions")) {
            for (agent, adir) in subdirs(&sdir.join("agents")) {
                for (user, udir) in subdirs(&adir.join("users")) {
                    out.push(Workspace {
                        tenant: tenant.clone(),
                        subscription: subscription.clone(),
                        agent: agent.clone(),
                        user,
                        root: udir,
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tree(root: &Path, tenant: &str, subs: &str, agent: &str, user: &str) -> PathBuf {
        let p = root
            .join("tenants").join(tenant)
            .join("subscriptions").join(subs)
            .join("agents").join(agent)
            .join("users").join(user);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn discovers_the_full_tuple_from_the_path() {
        let tmp = tempfile::tempdir().unwrap();
        tree(tmp.path(), "acme", "sub-1", "alpha", "uuid-a");
        tree(tmp.path(), "acme", "sub-1", "beta", "uuid-b");

        let found = discover(tmp.path());
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].tenant, "acme");
        assert_eq!(found[0].subscription, "sub-1");
        assert_eq!(found[0].agent, "alpha");
        assert_eq!(found[0].user, "uuid-a");
        assert_eq!(found[0].source_name(), "session:acme/sub-1/alpha/uuid-a");
    }

    #[test]
    fn a_missing_tree_is_empty_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(discover(tmp.path()).is_empty());
        assert!(discover(Path::new("/nonexistent/xyz")).is_empty());
    }

    /// FR-S5. The regression that silently drops 42% of conversations.
    #[test]
    fn finds_both_the_main_and_the_project_session_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let u = tree(tmp.path(), "acme", "sub-1", "alpha", "uuid-a");
        fs::create_dir_all(u.join("workspace/sessions")).unwrap();
        fs::create_dir_all(u.join("workspace-chat-ux/sessions")).unwrap();
        // A sibling that is NOT a workspace must not be picked up.
        fs::create_dir_all(u.join("notes/sessions")).unwrap();

        let ws = &discover(tmp.path())[0];
        let dirs = ws.session_dirs();
        assert_eq!(dirs.len(), 2, "expected both session dirs, got {dirs:?}");
        assert!(dirs.iter().any(|d| d.ends_with("workspace/sessions")));
        assert!(dirs.iter().any(|d| d.ends_with("workspace-chat-ux/sessions")));
    }

    #[test]
    fn a_workspace_dir_without_sessions_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let u = tree(tmp.path(), "acme", "sub-1", "alpha", "uuid-a");
        fs::create_dir_all(u.join("workspace")).unwrap();
        assert!(discover(tmp.path())[0].session_dirs().is_empty());
    }
}
