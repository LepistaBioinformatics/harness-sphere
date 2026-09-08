//! The TOML files this repo ships must parse into the config the binary expects.
//!
//! This exists because of a failure that produced NO error at all. `probe_targets` became
//! a `[[table array]]` and was written in the MIDDLE of `config.zombie-crab.toml`. In TOML
//! every bare key after a table header belongs to THAT table until the next header, so
//! `data_root`, `discovery_interval_secs` and `session_interval_secs` were silently
//! absorbed into the LAST probe target. serde dropped them as unknown fields, `data_root`
//! read as unset, and the watcher booted with `discovery=false` — collecting host and
//! probe metrics perfectly while never discovering a single workspace.
//!
//! Nothing logged a warning. The container was healthy. The only symptom was a number that
//! never appeared.

use std::path::{Path, PathBuf};

fn repo_file(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path)
}

fn root_table(path: &str) -> toml::Table {
    let raw = std::fs::read_to_string(repo_file(path))
        .unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
    raw.parse::<toml::Table>()
        .unwrap_or_else(|e| panic!("{path} is not valid TOML: {e}"))
}

/// Every scalar setting must live in the ROOT table, not be swallowed by a table array.
#[test]
fn zombie_crab_config_keeps_its_scalars_at_the_root() {
    let t = root_table("config.zombie-crab.toml");
    assert_eq!(
        t.get("data_root").and_then(|v| v.as_str()),
        Some("/data"),
        "data_root is not at the root — a [[table array]] above it absorbed the key"
    );
    for key in ["discovery_interval_secs", "session_interval_secs", "exporter"] {
        assert!(t.contains_key(key), "{key} is missing from the root table");
    }
}

/// A probe target carries exactly `address` and `layer`. Anything else means a scalar
/// written below the probe list leaked into it — the exact bug described above.
#[test]
fn probe_targets_contain_nothing_but_address_and_layer() {
    for path in ["config.zombie-crab.toml", "config.example.toml"] {
        let t = root_table(path);
        let Some(targets) = t.get("probe_targets").and_then(|v| v.as_array()) else {
            continue;
        };
        for target in targets {
            let Some(table) = target.as_table() else {
                continue; // the example file ships an empty scalar array
            };
            let mut keys: Vec<&str> = table.keys().map(|k| k.as_str()).collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec!["address", "layer"],
                "{path}: a probe target absorbed stray keys ({keys:?}) — \
                 a scalar setting was written BELOW the [[probe_targets]] blocks"
            );
        }
    }
}

/// Valid TOML is not enough: the files must be valid *for this binary*.
#[test]
fn shipped_configs_are_parseable() {
    for path in ["config.zombie-crab.toml", "config.example.toml"] {
        let raw = std::fs::read_to_string(repo_file(path)).unwrap();
        raw.parse::<toml::Table>()
            .unwrap_or_else(|e| panic!("{path}: {e}"));
    }
}
