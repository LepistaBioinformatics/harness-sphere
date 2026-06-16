# HarnessSphere — PROJECT

## Vision

A unified, single-binary agent/watcher that collects telemetry from **every layer
of a host running the Claw/Harness ecosystem** — infrastructure, container,
gateway, harness (AI), tools and API calls — and dispatches everything via **OpenTelemetry
(OTLP)** to any compatible backend (OTel Collector, Grafana/Tempo/Mimir/Loki,
vendors).

## Design principles (non-negotiable)

1. **Single binary, static and portable.** Runs on any Linux distro (musl), macOS
   (Intel + Apple Silicon) and Raspberry Pi (ARMv7/AArch64) with no external runtime.
2. **Never brings the host down.** The watcher is a passive observer; a failure in a
   monitored target must never bring HarnessSphere down.
3. **Graceful degradation by default.** Optional collectors that fail are isolated,
   marked as `degraded` and retried; the rest of the pipeline keeps running.
4. **Critical vs. Optional.** Only **Host** and the **Watcher itself** are mandatory; their
   persistent failure is fatal (exit ≠ 0). Everything else is best-effort.
5. **Extensible via traits + feature flags.** A new collector = a new module that implements
   `Collector`; the core does not change.
6. **Idiomatic OTel standard.** Metric/attribute names follow the official *semantic
   conventions* (system.\*, process.\*, container.\*, http.\*, rpc.\*, gen_ai.\*).

## Non-goals (v1)

- It is not a storage backend nor a dashboard (delegates to the Collector/backend).
- It does not do APM via third-party code instrumentation (it only observes from the
  outside + ingests the signals the harness exposes).
- It does not orchestrate nor restart the monitored targets.

## Target stack

- Rust stable 1.96 (edition 2024, MSRV 1.95), `tokio` runtime.
- `opentelemetry` 0.32.x + `opentelemetry_sdk` + `opentelemetry-otlp` (gRPC/HTTP).
- `sysinfo` (host/process), direct cgroup v2 reading (container), `tracing` +
  `tracing-opentelemetry` for self-observability.
- Cross-compilation via `cross` + `cargo-zigbuild`.

## Planning status

- [x] PLAN — design + OTel matrix — **APPROVED** (Scope A; opt-in content; crash by
  threshold). Decisions in `features/telemetry-core/context.md`.
- [ ] Scaffolding (Cargo workspace, traits, collection runtime) — **next, awaiting go**
- [ ] Critical collectors (host, self)
- [ ] Optional collectors (container, gateway, harness, tools, api)
- [ ] Release / cross-compile pipeline
