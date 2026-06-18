# STATE — HarnessSphere project memory

## Current status (2026-06-15)
- **PLAN approved.** **Hexagonal architecture (ports & adapters)**, scope **Option A**
  (unified sidecar collector), GenAI content **opt-in**, critical crash by **threshold**.
- **Scaffolding Sprint 1: COMPLETED and verified.** Workspace compiles (`cargo build`
  clean), domain tests pass (4/4), the binary runs end-to-end (host+self → stdout,
  graceful shutdown via SIGINT).

## Deploy & publish (2026-06-15)
- **`deploy/signoz/`** — SigNoz stack vendored from the official Apache-2.0 deploy. Brought
  up and **verified end-to-end**: HarnessSphere (OTLP exporter) → SigNoz collector →
  ClickHouse held 231 samples of `system.*`/`process.*` (`service.name=harnesssphere`).
  The OTel SDK logged `TonicMetricsClient.ExportSucceeded`. README documents "how each
  signal is collected".
- **Published:** https://github.com/LepistaBioinformatics/harness-sphere (PUBLIC, default
  branch `main`). Created via `gh repo create` after the user re-authenticated.

## Delivered structure
- `crates/domain` — canonical signal model, ports, policies (breaker + criticality). Pure.
- `crates/runtime` — supervisor (task per source, 3 containment layers, breaker, drain+batch).
- `crates/collectors` — `HostCollector` + `SelfCollector` (Critical).
- `crates/export` — `StdoutExporter` (default); `OtlpExporter` behind the `otlp` feature (to do).
- `harnesssphere` (bin) — composition root + TOML/env config.

## Recorded technical decisions
- **Toolchain: Rust stable 1.96.0**, **edition 2024**, MSRV `rust-version = 1.95`
  (`rust-toolchain.toml` pins `stable`). `sysinfo` at the current **0.39**.
- **`panic = "unwind"` kept in release** (not `abort`): panic containment
  (`catch_unwind`, FR-RES-03) depends on it. Size comes from `opt-level="z"`+`lto`+`strip`.
- Domain with no dependency on `opentelemetry*` (pre-1.0, churn) — SDK confined to `export/`.

## Resilience (design §2) — hardened (branch `feat/resilience-hardening`)
- ✅ **Comprehensive panic containment + Critical escalation:** the whole supervisor task
  is wrapped in `catch_unwind`, so a panic *anywhere* (probe, breaker, loop logic) makes a
  Critical source **fatal** instead of dying silently. (Per-tick `collect` panics are still
  caught inside the loop → degrade only.) Covers the old "JoinHandle observation" goal and
  the "probe panic not contained" gap. **Tested:** `critical_probe_panic_is_fatal`.
- ✅ **Flush-on-fatal:** shutdown aborts sources/receivers (drops their sink clones → closes
  the channel), then awaits the drain (5s timeout) so buffered signals are exported before
  exit — replaces the old `drain.abort()`.
- Still deferred (low value): **drop-newest** in `ChannelSink` (not drop-oldest, design
  §2.3); **re-spawn** of a dropped Optional source (today it's logged/disabled, not respawned).

Verified: Critical fatal path + Critical probe-panic → fatal + failing Optional does NOT
bring it down (`tests/crash.rs`, 3 tests); happy-path host+self→stdout; policy tests.

## Next steps (backlog)
1. ~~**OTLP adapter**~~ ✅ **DONE** (branch `feat/otlp-exporter`): `OtlpExporter`
   (`otlp` feature) — OTLP/gRPC via SDK 0.32, `SdkMeterProvider` + synchronous instruments,
   Resource (`service.name`, `host.name`). `PeriodicReader` with configurable interval
   (`metric_export_interval_secs`). Wiring in the bin via `exporter = "otlp"` +
   `OTEL_EXPORTER_OTLP_ENDPOINT`.
   **Verified (observed success path):** against a real `otelcol-contrib` (OTLP receiver→debug
   exporter), the collector received `system.cpu.utilization`,
   `system.memory.usage` (3 data points), `process.memory.usage`, etc. — 8 metrics /
   10 data points, with Resource `service.name=harnesssphere` and `host.name`. Also
   verified that **against a dead endpoint it does not bring down** the watcher.
   - **v1 scope: metrics only** (host/self do not emit log/span). OTLP Logs/Spans when
     ingest/harness produces them.
   - **Modeling:** sampled absolute values (Gauge **and** UpDownCounter) → **Gauge**
     in OTLP. TODO: migrate additive metrics (e.g., `system.memory.usage` by state) to
     **observable** instruments to preserve the semconv sum.
   - Push cadence = the SDK's periodic reader (default ~60s), decoupled from the drain
     batch. Evaluate `PeriodicReader` with an explicit interval.
2. **Ingest plane** (`crates/ingest`) — ✅ **v1 DONE** (branch `feat/ingest-plane`):
   `OtlpReceiver` (feature `ingest`) — OTLP/gRPC server (`tonic` 0.14 + `opentelemetry-proto`
   0.32) that converts incoming metrics (Gauge + Sum) to canonical signals, runs them
   through the `Enricher` (injects `host.name`), and emits into the same pipeline as the
   collectors. New `Receiver` driving port + `Supervisor::with_receivers`. Wiring in the
   bin via `ingest_enabled`/`ingest_endpoint`; best-effort anti-loop port guard.
   **Verified (observed success path):** instance A (OTLP exporter) → instance B (ingest
   receiver + stdout); B printed 20 enriched lines (e.g. `system.cpu.utilization … host.name`),
   proving receive→convert→enrich→export end-to-end. Converter mapping locked by 2 unit
   tests.
   - Metrics (Gauge/Sum) **and now traces** — see item 2b.
   - **Pending:** Hermes convention normalization (`llm.token_count.*`→`gen_ai.*`),
     `container.id` enrichment, content redaction, HTTP (:4318) receiver.
2b. **Traces end-to-end (Tier 1)** — ✅ **DONE** (branch `feat/traces-ingest-export`):
   canonical `Span` extended with `trace_id`/`span_id`/`parent_span_id`. Ingest adds an
   OTLP `TraceService` (proto `trace` feature) → canonical spans (resource attrs merged,
   enriched with `host.name`). Export adds a `TraceServiceClient` path that groups spans
   by `service.name` onto the **Resource** (required for SigNoz's Services/APM view).
   **Verified:** `telemetrygen` → ingest `:4319` → enrich → export → SigNoz: 100 spans in
   `signoz_traces`, `service=telemetrygen` in `top_level_operations` (the Services tab).
   - **Pending Tier 1:** OTLP **logs** ingest+export; **histogram** metric ingest (for
     `gen_ai.client.token.usage`); a real AI source emitting (PicoClaw/OpenClaw/Hermes).
3. Optional collectors: ✅ `process` (watch named processes → `process.*`) and
   `endpoint-probe` (TCP liveness/latency → `harnesssphere.endpoint.*`) — DONE
   (branch `feat/process-probe-collectors`), config-driven (`watch_processes`,
   `probe_targets`). **Verified:** watching `picoclaw` + probing `localhost:18790`,
   both landed in SigNoz (`process.executable.name=picoclaw`, `harnesssphere.endpoint.up`).
   Still TODO: `container` (cgroup v2), `prometheus` (scrape of OpenClaw
   `/api/diagnostics/prometheus`).
4. Release pipeline: `cross` + `cargo-zigbuild` for the 6 targets.

## Product findings
- **PicoClaw has no native telemetry export** (no `diagnostics`/`otel` subcommand or
  config; gateway `:18790` has no `/metrics`). So no tokens/spans from it.
- **ClawMetry approach:** reads each runtime's **on-disk session files** (DuckDB store);
  but PicoClaw/NanoClaw/Cursor **don't write token cost to disk**. PicoClaw session at
  `~/.picoclaw/workspace/sessions/*.jsonl` holds messages (`{role, content}`, roles
  user/assistant/tool) and `tool_calls`.
  ✅ **`SessionCollector` DONE** (branch `feat/picoclaw-session-collector`): parses the
  session JSONL → `harnesssphere.harness.messages` (by `role`), `harnesssphere.tool.calls`,
  `harnesssphere.harness.sessions` (absolute Gauges, tagged `harness.name`). Config
  `session_dir`/`session_source`. **Verified:** reading `~/.picoclaw/workspace/sessions`
  → SigNoz showed messages by role (user/assistant/tool) + tool.calls for `harness.name=picoclaw`.
  Tokens remain absent (not on disk). Dashboard messages/tool panels switched to `latest`.
- For real AI metrics/traces (incl. tokens), use OpenClaw/Hermes (OTLP) → ingest plane.
