# STATE — HarnessSphere project memory

## Current status (2026-06-15)
- **PLAN approved.** **Hexagonal architecture (ports & adapters)**, scope **Option A**
  (unified sidecar collector), GenAI content **opt-in**, critical crash by **threshold**.
- **Scaffolding Sprint 1: COMPLETED and verified.** Workspace compiles (`cargo build`
  clean), domain tests pass (4/4), the binary runs end-to-end (host+self → stdout,
  graceful shutdown via SIGINT).

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

## Known resilience gaps (deferred — design §2 promises, sprint 1 does not deliver)
- **3rd containment layer (JoinHandle observation):** the supervisor holds the handles
  but does not observe them to re-spawn (Optional) / escalate (Critical). Today there are
  only 2 layers (Result + catch_unwind in the tick).
- **Panic inside `probe()` is not contained** (no catch_unwind): it kills the task of that
  source silently. To be covered together with the 3rd layer.
- **Fatal uses `drain.abort()`** (discards the buffer) instead of the *flush → exit* promised
  in §2.2. Implement an ordered flush of the exporter before `exit(1)`.
- **Drop-newest** in the `ChannelSink` (not drop-oldest) — see design §2.3.

Verified in sprint 1: the Critical fatal path end-to-end (`tests/crash.rs`) **and** that a
failing Optional does not bring it down; happy-path host+self→stdout; policy tests.

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
2. **Ingest plane** (`crates/ingest`): local OTLP receiver (gRPC :4317/HTTP :4318) +
   Enricher (injects `host.*`/`container.id`, normalizes Hermes `llm.token_count.*`→`gen_ai.*`)
   + anti-loop guard + content redaction (default on).
3. Optional collectors: `container` (cgroup v2), `prometheus` (scrape of OpenClaw
   `/api/diagnostics/prometheus`).
4. Release pipeline: `cross` + `cargo-zigbuild` for the 6 targets.

## Product open items
- PicoClaw: confirm the exposure mechanism (doc returned 403). Optional/fallback.
- Confirm the exact formats emitted by OpenClaw/Hermes against real OTLP captures.
