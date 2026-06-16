# Feature: telemetry-core — Specification

> HarnessSphere core: collection runtime, plugin model, resilience and OTel export.
> Traceable IDs to tie design → tasks → implementation.

## Functional requirements

### Architecture & extensibility
- **FR-ARCH-01** — Every collector implements the `Collector` trait and lives in an
  isolated module (`collectors/<layer>.rs`).
- **FR-ARCH-02** — Collectors are registered in a `CollectorRegistry`; the core iterates the
  registry without concretely knowing each collector.
- **FR-ARCH-03** — Adding a new collector **does not require** changing the core: just create
  the module, implement the trait and register it (behind a `cargo feature`).
- **FR-ARCH-04** — Each collector declares static metadata: `name`, `layer`,
  `criticality` (`Critical | Optional`) and `interval`.

### Resilience & graceful degradation
- **FR-RES-01** — Each collector runs in an independent `tokio` task; the scheduler never
  blocks one collector because of another.
- **FR-RES-02** — Every collection returns a `Result`; a collector error is captured, logged
  and exported as a failure metric, **without** propagating to the core.
- **FR-RES-03** — A `panic` inside a collector is contained (catch_unwind at the tick
  level); it does not abort the process nor other tasks.
- **FR-RES-04** — An **Optional** collector with N consecutive failures enters the
  `Degraded` state with exponential backoff and a *circuit breaker*; it auto-recovers when
  the target comes back.
- **FR-RES-05** — A **Critical** collector (Host, Self) with persistent failure above the
  threshold makes the process exit with a non-zero exit code (intentional fail-fast).
- **FR-RES-06** — The absence of an optional target (e.g., a non-existent container, a gateway
  that is down) is treated as `NotApplicable`/`Unavailable`, not as a fatal error.

### Telemetry (OTel)
- **FR-OTEL-01** — Exports the three signals (metrics, logs, traces) via OTLP
  (gRPC default, HTTP optional), with configurable endpoint/headers.
- **FR-OTEL-02** — Instrument and attribute names follow the official *semantic conventions*;
  see the matrix in `design.md`.
- **FR-OTEL-03** — Global `Resource` with `service.name=harnesssphere`,
  `service.version`, `host.*`, and host identity attributes.
- **FR-OTEL-04** — The watcher itself is self-instrumented (process.\* + loop/scraping
  metrics).

### Configuration & distribution
- **FR-CFG-01** — Configuration via file (TOML) + env vars (override), including which
  collectors to enable, intervals and OTLP endpoint.
- **FR-DIST-01** — The build produces a single static binary per target (musl/macOS/ARM).

## Non-functional requirements
- **NFR-01** — Low footprint: the watcher must not be a material source of load
  (target budget: < ~1% average CPU, < ~30 MB RSS in steady state; measured by itself
  via FR-OTEL-04).
- **NFR-02** — Lean binary (LTO + `opt-level="z"` + strip) and no dynamic dependencies
  on the Linux target (100% static musl).
- **NFR-03** — Zero crashes caused by a monitored target (consequence of FR-RES-\*).
- **NFR-04** — Resilient export overhead: an OTLP endpoint failure does not block
  collection (asynchronous export with bounded buffer/drop).

## Gray areas (to decide with the user before TASKS)
- **GA-01** — Exact identity of the "Gateway" and "Harness" targets: do they expose `/metrics`
  (Prometheus), their own OTLP, file logs, or a socket/admin API? Defines the scraping
  mode.
- **GA-02** — "Container running the harness": is the runtime Docker/containerd/podman? Do we
  read cgroup v2 directly (preferred, no socket) or via the runtime API?
- **GA-03** — Source of the AI signals (tokens/messages/search index/memory files):
  does the harness already emit OTLP/Prometheus, or do we need to derive them from logs/files
  on the host?
- **GA-04** — Tools sandbox: does the time/count per tool come from harness instrumentation
  or does the watcher observe processes/execution externally?
- **GA-05** — GenAI content privacy policy (prompts/completions): by default **do not**
  capture content (metrics/counters only), explicit opt-in.

## Recorded decisions (see context.md)
- **Scope = Option A** (unified sidecar collector: OTLP receiver + scrape + host/self/
  container + enrich + export). Adds requirements:
  - **FR-INGEST-01** — Local OTLP receiver (gRPC :4317 / HTTP :4318) that accepts pushes from
    OpenClaw/Hermes; Optional criticality.
  - **FR-INGEST-02** — Enricher injects `host.*`/`container.id` into every ingested signal and
    normalizes the dual convention (OpenInference `llm.token_count.*` → `gen_ai.*`).
  - **FR-INGEST-03** — Anti-loop protection: HarnessSphere's own exporter never
    feeds back into its own receiver.
- **GA-05 = explicit opt-in per layer.** The Enricher **redacts content by default**
  (FR-PRIV-01), even in passthrough; it only emits text with an explicit config flag.
- **Critical crash = persistent failure above threshold** (not the first error).

> Resolved gray areas recorded in `context.md`.
