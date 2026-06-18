## Summary

<!-- Provide a brief description of the changes in this PR -->
<!-- Explain WHAT changed and WHY. Focus on the problem being solved and the solution implemented. -->

## Type of Change

<!-- Mark the relevant option with an 'x' -->

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Refactoring (no functional changes)
- [ ] Documentation update
- [ ] Performance / footprint improvement
- [ ] CI / deploy / dashboard
- [ ] Other (please describe):

## Changes Made

<!-- List the main changes made in this PR. Be specific about what was added, modified, or removed. -->

- <!-- e.g., Added `ContainerCollector` reading cgroup v2 -> container.* -->
- <!-- e.g., Extended the OTLP ingest receiver to accept logs + histograms -->
- <!-- e.g., Hardened the supervisor: a Critical panic now escalates to fatal -->

## Layer / area touched

<!-- Mark what this PR affects -->

- [ ] `domain` (canonical model, ports, policies — must stay IO/OTel-free)
- [ ] `runtime` (supervisor, breaker, drain)
- [ ] `collectors` (host, self, process, probe, session, container, …)
- [ ] `ingest` (OTLP receiver)
- [ ] `export` (stdout / OTLP)
- [ ] `harnesssphere` (bin / composition root / config)
- [ ] deploy / dashboard / CI / docs

## Related Issues

<!-- Use "Closes #123" to auto-close on merge, or "Relates to #123" otherwise. -->

Closes #

## Checklist

- [ ] `cargo build --workspace` is clean (and `--features otlp,ingest` if touched)
- [ ] `cargo test --workspace` passes; tests added for new logic
- [ ] No new compiler warnings; `cargo clippy` clean (if applicable)
- [ ] Hexagonal boundary respected (no `opentelemetry`/IO types leaked into `domain`)
- [ ] Resilience preserved (Critical = fatal only on persistent failure; Optional degrades, never crashes)
- [ ] New metric/attribute/span names follow OpenTelemetry semantic conventions (or are namespaced `harnesssphere.*`)
- [ ] Config changes documented in `config.example.toml`
- [ ] Docs / `.specs` / dashboard updated (if applicable)
- [ ] Commits follow conventional commit format

## Testing

<!-- Describe how you tested these changes: unit, integration, and end-to-end against SigNoz. -->

**Test Environment:**
- OS: <!-- e.g., Ubuntu 22.04 -->
- Rust version: <!-- e.g., stable 1.96 (edition 2024) -->

**Test Steps:**
1. <!-- e.g., run the watcher with exporter="otlp" against the deploy/signoz stack -->
2. <!-- e.g., query ClickHouse / the SigNoz UI for the expected metric/span/log -->
3.

**Test Commands:**
```bash
cargo test --workspace
# end-to-end (optional): point the watcher at deploy/signoz and verify the signal lands
```

## Screenshots / Logs

<!-- If applicable, add SigNoz screenshots, sample exported signals, or relevant logs. -->

## Additional Notes

<!-- Migration steps, feature-flag/secret requirements, deprecations, or reviewer notes. -->
