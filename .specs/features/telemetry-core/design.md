# Feature: telemetry-core — Design

> Modular architecture, resilience model, OpenTelemetry mapping matrix and
> cross-compilation strategy. Verified versions (Jun/2026): `opentelemetry 0.32`.
> Cited semantic conventions checked against the official spec (system.\*, process.\*,
> container.\*, http.\*, rpc.\*, gen_ai.\*).

---

## 1. Modular Architecture & Extensibility (Ports & Adapters)

The architecture is **hexagonal (ports & adapters)**, applied with discipline (not dogma):

- **Domain (the hexagon):** **canonical** signal model (`Metric`/`LogRecord`/`Span`),
  **ports** (traits) and pure **policies** — criticality/crash, circuit breaker,
  enrichment, normalization (Hermes dual convention → `gen_ai.*`), redaction. **No
  dependency on IO nor on the OpenTelemetry SDK.**
- **Driving adapters** (start the flow inward): `OtlpReceiver` (push from OpenClaw/
  Hermes) and the supervisor/scheduler that triggers the pulls.
- **Driven adapters** (called by the domain): sources (`sysinfo`, `cgroup`,
  `prometheus-scrape`) and sinks (`otlp-exporter`, `stdout-exporter`).

> **Why hexagonal here (not speculative):** (1) there is plural IO on both sides;
> (2) the domain has pure logic that is testable without a network; (3) the `opentelemetry*`
> crates are **pre-1.0 (0.32)** and break the API frequently — confining the SDK in the
> export adapter protects the core. Guardrail: source adapters are thin, **one** canonical
> signal enum (no per-metric DTO), a port only where there is >1 real implementation.

### 1.1 Layered view

```
                       ┌──────────────────────────────────────────┐
                       │                  CORE                     │
                       │  ┌────────────┐   ┌───────────────────┐   │
   config (TOML/env) → │  │  Registry  │ → │ CollectionRuntime │   │
                       │  └────────────┘   │  (supervisor)     │   │
                       │        ▲          └─────────┬─────────┘   │
                       │        │ register           │ spawn       │
                       │  ┌─────┴──────────────────┐ │             │
                       │  │  trait Collector (dyn) │ │ mpsc<Signal>│
                       │  └────────────────────────┘ ▼             │
                       │                   ┌────────────────────┐  │
                       │                   │  OtelExporter      │──┼─→ OTLP (gRPC/HTTP)
                       │                   │ (metrics/log/trace)│  │
                       └───────────────────┴────────────────────┘  │
                                ▲           ▲          ▲
        ┌───────────────────────┼───────────┼──────────┼───────────────────────┐
        │   collectors/ (each layer = isolated module, behind a cargo feature)  │
        │  host*  self*  container  gateway  harness  tools  api                │
        │  (* = Critical; others = Optional)                                    │
        └──────────────────────────────────────────────────────────────────────┘
```

The **core does not know** any concrete collector. It only knows the `Collector` trait and
the `Registry`. Each layer is a module in `collectors/` compiled conditionally by
`cargo feature`.

### 1.2 Central trait

```rust
/// The single contract every collector implements. Object-safe (used as `dyn Collector`).
#[async_trait::async_trait]
pub trait Collector: Send + Sync + 'static {
    /// Static metadata of the collector (name, layer, criticality, interval).
    fn descriptor(&self) -> &CollectorDescriptor;

    /// Called once at boot. Detects target availability.
    /// `Unavailable`/`NotApplicable` for an Optional is NOT a fatal error.
    async fn probe(&mut self, cx: &CollectorCtx) -> ProbeResult;

    /// One collection cycle. Emits signals via `cx.emitter`. Returns Result —
    /// an error is isolated by the runtime, it never propagates to the core.
    async fn collect(&mut self, cx: &CollectorCtx) -> Result<(), CollectError>;
}

pub struct CollectorDescriptor {
    pub name: &'static str,              // "host", "harness", ...
    pub layer: Layer,                    // enum Layer
    pub criticality: Criticality,        // Critical | Optional
    pub default_interval: Duration,
}

pub enum Criticality { Critical, Optional }

pub enum ProbeResult {
    Ready,                 // target present, collection enabled
    Unavailable(String),   // target absent/unresponsive (Optional → degrade, not failure)
    NotApplicable,         // makes no sense on this host (e.g., no container)
    Fatal(String),         // only Critical can return this → aborts boot
}
```

`Collector` **is the `SignalSource` port** (driven). `CollectorCtx` carries the collector's
config and a `SignalSink` (channel into the domain pipeline). The collector "speaks" only in
**canonical domain signals**; the export adapter (`export/`) is the only one that translates
to OTLP. This way the pre-1.0 SDK never touches the core.

### 1.3 Registration and extensibility (adding a collector without touching the core)

```rust
// composition root (crate `harnesssphere`, bin) — wiring of ports↔adapters
pub fn build_registry(cfg: &Config) -> CollectorRegistry {
    let mut reg = CollectorRegistry::new();
    reg.register(Box::new(HostCollector::new(&cfg.host)));   // always (Critical)
    reg.register(Box::new(SelfCollector::new()));            // always (Critical)

    #[cfg(feature = "container")]
    reg.register(Box::new(ContainerCollector::new(&cfg.container)));
    #[cfg(feature = "gateway")]
    reg.register(Box::new(GatewayCollector::new(&cfg.gateway)));
    #[cfg(feature = "harness")]
    reg.register(Box::new(HarnessCollector::new(&cfg.harness)));
    #[cfg(feature = "tools")]
    reg.register(Box::new(ToolsCollector::new(&cfg.tools)));
    #[cfg(feature = "api")]
    reg.register(Box::new(ApiCollector::new(&cfg.api)));

    reg
}
```

**To add a new collector (e.g., a new gateway):**
1. Create `collectors/gateway_envoy.rs` implementing `Collector`.
2. Add the `cargo feature` in `Cargo.toml`.
3. One line `reg.register(...)` behind `#[cfg(feature = ...)]`.

The domain + runtime + export adapter **do not change**. The binary does not
break: disabled features do not even compile the module → zero cost. This is the single,
stable extension point.

> **Design decision:** dynamic dispatch (`Box<dyn Collector>`) instead of a static enum.
> The vtable cost is irrelevant at the collection scale (intervals of seconds) and we gain
> open extensibility. *Dynamic* plugins (`.so`/`dlopen`) are a **v1 non-goal**
> (unstable ABI in Rust); the extension is at compile-time via features.

### 1.4 Workspace layout (hexagonal, no `hs` prefix)

```
harness-sphere/
├─ Cargo.toml                # [workspace]
├─ .cargo/config.toml        # targets, cross linkers
├─ crates/
│  ├─ domain/                # pkg harnesssphere-domain — DOMAIN: canonical signal
│  │                         #   model, ports (SignalSource/Receiver/Exporter/Probe),
│  │                         #   policies (criticality, breaker, enrich, normalize,
│  │                         #   redact). ZERO IO, ZERO otel.
│  ├─ runtime/               # pkg harnesssphere-runtime — supervisor/scheduler that
│  │                         #   orchestrates the ports (driving)
│  ├─ collectors/            # pkg harnesssphere-collectors — driven source adapters:
│  │                         #   host, self (Critical) | container, prometheus (feature)
│  ├─ ingest/                # pkg harnesssphere-ingest — driving adapter: OTLP receiver
│  │                         #   + anti-loop guard
│  └─ export/                # pkg harnesssphere-export — driven adapter: OTLP exporter +
│                            #   SDK init + Resource (only place with `opentelemetry*`)
├─ harnesssphere/            # pkg harnesssphere (bin) — composition root: config → wiring
└─ .specs/
```

Package names `harnesssphere-*` (avoids collision on crates.io); short directories by
role. The domain is the only crate with no IO/OTel dependencies — it is where the testable
logic lives.

### 1.5 PULL plane (scrape) vs PUSH plane (ingest) — post-research refinement

Research into the ecosystem documentation (see `context.md`) revealed that the AI components
**push OTLP** and do **not** expose traces for scraping:

- **OpenClaw** → PUSH OTLP (default `:4318`) **+** Prometheus scrape at
  `/api/diagnostics/prometheus`.
- **Hermes** (`hermes-otel`) → PUSH OTLP (BatchSpanProcessor), attributes in dual
  convention (`gen_ai.*` and OpenInference `llm.token_count.*`).
- **PicoClaw** → lightweight (Go, <10MB); native exposure not confirmed (likely ClawMetry/
  logs). Treated as Optional with fallback.

Therefore the `Collector` trait (pull) does not cover push traffic. We introduce a second
contract and an OTel Collector-style pipeline:

```rust
/// PUSH plane: receives OTLP that the components push, enriches and re-exports.
#[async_trait::async_trait]
pub trait Receiver: Send + Sync + 'static {
    fn descriptor(&self) -> &ReceiverDescriptor;          // criticality ALWAYS Optional
    async fn serve(&mut self, cx: &ReceiverCtx, tx: SignalSink) -> Result<(), RecvError>;
}
```

```
   PUSH  OpenClaw/Hermes ──OTLP──▶ ┌───────────────┐
                                   │ OtlpReceiver   │┐
                                   └───────────────┘│   ┌──────────────┐    ┌──────────┐
   PULL  Host/Self/Container ────▶ ┌───────────────┐├──▶│  Enricher    │──▶ │ Exporter │─OTLP▶ backend
         OpenClaw /prometheus ───▶ │ Collectors     │┘   │ +host/cont.  │    │ 1 output │
                                   └───────────────┘    │ +normalize   │    └──────────┘
                                                        └──────────────┘
```

The **Enricher** injects `host.*`/`container.id` into every signal that enters through the
receiver and **normalizes** Hermes's dual convention (`llm.token_count.prompt` →
`gen_ai.usage.input_tokens`). This is the **differentiator**: correlating `gen_ai.*` spans
with resource pressure from the same host.

> **SCOPE FORK — DECIDED: Option A.** ✅
>
> **Option A — Unified sidecar collector (CHOSEN):** embeds the
> `OtlpReceiver` + Prometheus scrape + host/cgroup/self collectors + enrich + 1
> OTLP exporter. A single binary replaces "OTel Collector + node exporter". The AI
> telemetry *passes through* HarnessSphere and gains host context. Larger scope (runs a local
> OTLP server; mind telemetry loops).
>
> **Option B — Focused host agent:** only PULL (host/self/container + Prometheus scrape)
> exports OTLP; the AI components push directly to an external Collector. A smaller and
> simpler binary, but the "single pane" weakens — the AI telemetry is **not**
> enriched with host context by HarnessSphere.

---

## 2. Resilience & Fallback (Graceful Degradation)

### 2.1 Supervisor + per-task isolation

The `CollectionRuntime` is a supervisor: each collector runs in its **own tokio task** with
its own `tokio::time::interval`. There is no monolithic loop — so a slow or stuck collector
never delays the others (FR-RES-01).

```rust
async fn supervise(mut collector: Box<dyn Collector>, cx: CollectorCtx, ctl: SupervisorCtl) {
    let desc = collector.descriptor().clone();
    let mut breaker = CircuitBreaker::new(desc.criticality);
    let mut ticker = tokio::time::interval(cx.config.interval(&desc));

    // initial probe
    match collector.probe(&cx).await {
        ProbeResult::Fatal(e) => return ctl.report_fatal(&desc, e),   // only Critical reaches here
        ProbeResult::Unavailable(_) | ProbeResult::NotApplicable => breaker.trip_open(),
        ProbeResult::Ready => {}
    }

    loop {
        ticker.tick().await;
        if breaker.is_open() { /* backoff: sporadic re-probe */ ... continue; }

        let started = Instant::now();
        // FR-RES-03: a panic inside the tick is CONTAINED, it does not bring down the task nor the process.
        let outcome = AssertUnwindSafe(collector.collect(&cx)).catch_unwind().await;

        match outcome {
            Ok(Ok(())) => { breaker.record_success(); cx.emit_scrape_ok(&desc, started); }
            Ok(Err(e)) => { handle_failure(&desc, &mut breaker, &ctl, &cx, e.into()); }
            Err(panic) => { handle_failure(&desc, &mut breaker, &ctl, &cx, panic.into()); }
        }
    }
}
```

Three containment layers:
1. **`Result`** — expected error (timeout, connection refused, parse). → `handle_failure`.
2. **`catch_unwind`** (via `futures::FutureExt`) — an unexpected `panic` becomes `Err`,
   contained in the tick. The task survives. (FR-RES-03)
3. **Isolated task** — if the task itself dies, the `JoinHandle` is observed by the supervisor,
   which re-spawns it (for Optional) or escalates to fatal (for Critical).

### 2.2 Circuit breaker + criticality

```rust
fn handle_failure(desc, breaker, ctl, cx, err) {
    cx.emit_scrape_failure(desc, &err);          // metric + log (see matrix §3.2)
    breaker.record_failure();                     // exponential backoff
    match (desc.criticality, breaker.state()) {
        // Optional: degrade and continue. Auto-recovers when the target comes back.
        (Criticality::Optional, _) => tracing::warn!(collector=desc.name, %err, "degraded"),
        // Critical: tolerates a transient, but persistent failure is FATAL (fail-fast).
        (Criticality::Critical, BreakerState::Open) if breaker.consecutive() >= THRESHOLD =>
            ctl.report_fatal(desc, format!("critical collector down: {err}")),
        (Criticality::Critical, _) =>
            tracing::error!(collector=desc.name, %err, "critical transient failure"),
    }
}
```

| | **Critical** (Host, Self) | **Optional** (container, gateway, harness, tools, api) |
|---|---|---|
| Target absent at boot | `Fatal` → exit ≠ 0 | `Unavailable`/`NotApplicable` → breaker open, continue |
| Transient error | logs `error`, continues | logs `warn`, counts failure |
| Persistent failure (> THRESHOLD) | **process exits (exit ≠ 0)** | `Degraded` + backoff, re-probe, **never** kills the process |
| Recovery | — | breaker closes by itself when the target responds |

`ctl.report_fatal` signals the core for an ordered shutdown: flush the OTLP exporter →
exit with a non-zero code. This way even a critical crash **exports the reason** before dying.

### 2.3 Resilient export (NFR-04)

OTLP export runs off the collection path (batch + bounded channel). An unavailable OTLP
endpoint does **not** block collection: a bounded buffer and a self metric of dropped
items. A backend network failure ≠ a collector failure.

> **Implementation status (sprint 1):** the `ChannelSink` drops the **newest** signal
> (drop-newest) when the channel fills up — `tokio::mpsc` does not allow popping from the
> front; drop-oldest would require a different structure and remains an improvement. The drop
> count is already exposed (`harnesssphere.export.items.dropped` †).

---

## 3. OpenTelemetry Mapping Matrix

Conventions: `M` = Metric, `L` = Log, `T` = Trace/Span. Instruments: **G**auge
(observable), **C**ounter, **UDC** (UpDownCounter), **H**istogram. Names follow the official
semantic conventions; items outside the spec use their own `harnesssphere.*` namespace and are
marked with †.

### Resource (global, attached to every signal)
`service.name=harnesssphere`, `service.version`, `service.instance.id`,
`host.name`, `host.id`, `host.arch`, `os.type`.

### 3.a Host  — **CRITICAL**

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `system.cpu.utilization` | G (0..1) | `cpu`, `system.cpu.logical_number`, `system.cpu.state` (user/system/idle/iowait) |
| M | `system.cpu.time` | C (s) | same state attributes (cumulative alternative) |
| M | `system.memory.usage` | UDC (By) | `system.memory.state` (used/free/cached/buffered) |
| M | `system.memory.utilization` | G (0..1) | same |
| M | `system.paging.usage` / `system.paging.utilization` | UDC/G | swap |
| M | `system.disk.io` | C (By) | `system.device`, `disk.io.direction` (read/write) |
| M | `system.disk.operations` | C | `system.device`, direction |
| M | `system.disk.io_time` | C (s) | `system.device` |
| M | `system.filesystem.usage` | UDC (By) | `system.device`, `system.filesystem.state` (used/free/reserved), `mountpoint` |
| M | `system.filesystem.utilization` | G (0..1) | same |
| M | `system.network.io` | C (By) | `network.interface.name`, `network.io.direction` |
| M | `system.network.packet.count` / `system.network.packet.dropped` / `system.network.errors` | C | interface, direction |
| M | `system.network.connection.count` | UDC | `network.transport`, `system.network.state` |
| L | host health event | L | thresholds (disk full, imminent OOM) as structured WARN/ERROR log |
| T | — | — | **N/A** — the host is non-transactional; there is no span. |

### 3.b Watcher (HarnessSphere — self) — **CRITICAL**

Self-observability. Uses `process.*` (semconv) + its own `harnesssphere.*` namespace.

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `process.cpu.utilization` / `process.cpu.time` | G / C | `process.cpu.state` |
| M | `process.memory.usage` (RSS) / `process.memory.virtual` | UDC (By) | — |
| M | `process.thread.count` | UDC | — |
| M | `process.open_file_descriptors` | UDC | (Linux) |
| M | `harnesssphere.collector.scrape.duration` † | H (s) | `collector.name`, `collector.layer` — duration of one `collect()` |
| M | `harnesssphere.collection.loop.duration` † | H (s) | duration of the aggregated collection cycle |
| M | `harnesssphere.collector.scrapes` † | C | `collector.name`, `outcome` (success/error/panic) |
| M | `harnesssphere.collector.state` † | G (enum) | `collector.name` → 0=ready 1=degraded 2=unavailable |
| M | `harnesssphere.export.items.dropped` † | C | `signal` (metric/log/trace) — OTLP backpressure |
| L | scraping failure | L (WARN/ERROR) | `collector.name`, `error.type`, `error.message`, `exception.stacktrace` (if panic) |
| L | state transition | L (INFO) | breaker open/close, probe result |
| T | `harnesssphere.collection.cycle` † | T (span) | parent span per cycle; each collector = child span with `collector.name` and status (Ok/Error) |

### 3.c Container (if it exists) — **OPTIONAL**

Reads **cgroup v2** directly (no runtime socket). `container.*` namespace (semconv).

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `container.cpu.time` / `container.cpu.usage` | C / G | `container.id`, `container.name`, `cpu.mode` |
| M | `container.memory.usage` | UDC (By) | `container.id` (from `memory.current`) |
| M | `harnesssphere.container.memory.limit` † | G (By) | from `memory.max` (limit semconv still evolving) |
| M | `harnesssphere.container.memory.throttled` † | C | OOM events/`memory.events` |
| M | `container.disk.io` | C (By) | `container.id`, `disk.io.direction` (from `io.stat`) |
| M | `harnesssphere.container.cpu.throttled` † | C | `nr_throttled`/`throttled_usec` from `cpu.stat` |
| L | lifecycle | L | container went down / disappeared from the cgroup → WARN (and breaker degrades) |
| T | — | — | **N/A** — cgroup metrics are non-transactional; no span. |

### 3.d Gateway (harness control) — **OPTIONAL**

Route latency and connection health. **Real source (research):** OpenClaw exposes
Prometheus at `GET /api/diagnostics/prometheus` → active scrape (`openclaw_model_call_duration_seconds`,
`openclaw_run_*`, `openclaw_message_*`, `openclaw_liveness_*`, `openclaw_memory_bytes`),
mapped to the instruments below. Traffic that arrives via OTLP (Option A) is enriched
and forwarded. Where there is no `/metrics`, the watcher does an active health probe.

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `http.server.request.duration` | H (s) | `http.request.method`, `http.route`, `http.response.status_code`, `server.address` |
| M | `http.server.active_requests` | UDC | `http.request.method`, `http.route` |
| M | `harnesssphere.gateway.up` † | G (0/1) | `gateway.name`, `route` — watcher health probe |
| M | `harnesssphere.gateway.connections.active` † | UDC | `gateway.name`, `state` |
| M | `harnesssphere.gateway.probe.latency` † | H (s) | active health-check latency |
| L | dropped connection / upstream 5xx | L (WARN/ERROR) | `gateway.name`, `route`, `status_code` |
| T | (passthrough) | T | if the gateway propagates `traceparent`, the watcher forwards the context for correlation |

### 3.e Harness (AI) — **OPTIONAL**  ← the heart of the differentiator

Follows the **GenAI semantic conventions** (`gen_ai.*`). Verified attributes:
`gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`,
`gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`.

**Real source (research):** this layer is **not passively observable** — it is ingested
via OTLP that OpenClaw/Hermes push (Option A) and/or scrape of OpenClaw's Prometheus.
The Enricher (§1.5) **normalizes** to `gen_ai.*`: OpenClaw already uses semconv +
`openclaw.tokens`/`openclaw.harness.run`; Hermes uses a dual convention
(`gen_ai.usage.input_tokens` **and** OpenInference `llm.token_count.prompt`) → map both.
Memory files / search index hits are `harnesssphere.*` † (derived/observed; OpenClaw
exposes `openclaw.memory.pressure`/`openclaw_memory_bytes` as a partial proxy).

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `gen_ai.client.token.usage` | H (`{token}`) | `gen_ai.token.type` (input/output), `gen_ai.request.model`, `gen_ai.provider.name`, `gen_ai.operation.name` |
| M | `gen_ai.client.operation.duration` | H (s) | `gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.provider.name` |
| M | `harnesssphere.harness.messages` † | C | `role` (user/assistant/system/tool), `conversation.id?` — message count |
| M | `harnesssphere.harness.token.cache` † | C (`{token}`) | from `gen_ai.usage.cache_read.input_tokens` / `cache_creation.input_tokens` |
| M | `harnesssphere.harness.memory.files` † | G | number of memory files; `harnesssphere.harness.memory.bytes` † G (By) |
| M | `harnesssphere.harness.search_index.queries` † | C | `result` (hit/miss) — search index hits/misses |
| M | `harnesssphere.harness.search_index.hit_ratio` † | G (0..1) | derived (observable) |
| L | model error/refusal/cutoff | L (WARN/ERROR) | `gen_ai.response.finish_reasons`, `error.type` |
| T | `{gen_ai.operation.name} {model}` | T (span, CLIENT) | one AI transaction = span; `gen_ai.*` attrs (no content by default — GA-05) |
| T | `invoke_agent {agent}` / child spans | T | when the harness exposes an agent/turn structure |

> Prompt/completion content is **not** captured by default (privacy, GA-05);
> only counters/durations. Content capture is explicit opt-in.

### 3.f Tools — **OPTIONAL**

Execution of injected tools. The tool span follows the GenAI semconv
(`execute_tool {tool_name}`). **Real source:** OpenClaw emits
`openclaw.tool.execution.duration_ms` / `openclaw_tool_execution_total` and the span
`openclaw.tool.execution`; Hermes emits the span `tool.{name}` as a child of `api.{model}`. The
Enricher maps both to the instruments below.

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `harnesssphere.tool.execution.duration` † | H (s) | `gen_ai.tool.name`, `gen_ai.tool.type`, `outcome` (ok/error) |
| M | `harnesssphere.tool.calls` † | C | `gen_ai.tool.name`, `outcome` — calls per tool |
| L | tool execution error | L (ERROR) | `gen_ai.tool.name`, `error.type`, `error.message` |
| T | `execute_tool {tool_name}` | T (span, INTERNAL) | `gen_ai.tool.name`, `gen_ai.tool.call.id`; child of the parent AI span (3.e) |

### 3.g API Calls — **OPTIONAL**

Inbound and outbound HTTP/gRPC traffic. `http.*` and `rpc.*` (semconv).

| Signal | Name | Type | Attributes / notes |
|---|---|---|---|
| M | `http.client.request.duration` | H (s) | `http.request.method`, `server.address`, `http.response.status_code`, `network.protocol.version` |
| M | `http.server.request.duration` | H (s) | `http.request.method`, `http.route`, `http.response.status_code` |
| M | `http.client.request.body.size` / `...response.body.size` | H (By) | payload size (if available) |
| M | `rpc.client.duration` / `rpc.server.duration` | H (s) | `rpc.system` (grpc), `rpc.service`, `rpc.method`, `rpc.grpc.status_code` |
| M | `harnesssphere.api.requests` † | C | `direction` (inbound/outbound), `http.response.status_code`, class (2xx/4xx/5xx) |
| L | 4xx/5xx | L (WARN/ERROR) | method, route, status, latency |
| T | HTTP/gRPC client/server span | T | `SpanKind` Client/Server; correlates with AI spans (3.e) via trace context |

---

## 4. Compilation & Distribution Strategy (Cross-Compilation)

### 4.1 Targets

| Platform | Target triple | Strategy |
|---|---|---|
| Linux x86_64 (static) | `x86_64-unknown-linux-musl` | `cross` (100% static musl, runs on any distro) |
| Linux ARM64 (static) | `aarch64-unknown-linux-musl` | `cross` |
| Raspberry Pi 32-bit | `armv7-unknown-linux-musleabihf` | `cross` |
| Raspberry Pi 64-bit | `aarch64-unknown-linux-musl` | `cross` (same as ARM64) |
| macOS Intel | `x86_64-apple-darwin` | `cargo-zigbuild` (cross from Linux/CI) or native |
| macOS Apple Silicon | `aarch64-apple-darwin` | `cargo-zigbuild` or native |
| macOS Universal | `universal2-apple-darwin` | `cargo-zigbuild --target universal2-apple-darwin` (1 fat binary) |

### 4.2 Tools (recommendation)

- **`cross`** (cross-rs) — for all Linux/ARM/musl targets. Uses containers with
  ready-made toolchains; zero local cross-linker setup. It is the simplest and most
  reproducible path for musl + Raspberry Pi.
- **`cargo-zigbuild`** — uses `zig cc` as the linker to cross-compile **macOS
  (incl. universal2)** and versioned glibc from a Linux CI, with no need for a
  Mac. Solves the classic Apple cross-compile problem.
- Native macOS alternative: run `cargo build` on a macOS runner and merge with
  `lipo -create` (or let zigbuild's `universal2` do it).

> Recommendation: **`cross` (Linux/ARM) + `cargo-zigbuild` (macOS)** covers all
> targets from a single Linux pipeline. A macOS runner only if we want Apple signing/
> notarization.

### 4.3 `.cargo/config.toml` (sketch)

```toml
[target.x86_64-unknown-linux-musl]
rustflags = ["-C", "target-feature=+crt-static"]

[target.armv7-unknown-linux-musleabihf]
# linker provided by the `cross` image; nothing to pin locally
```

### 4.4 Release profile (lean binary — NFR-02)

```toml
[profile.release]
opt-level = "z"      # optimize for size
lto = true           # link-time optimization (fat)
codegen-units = 1    # better optimization, slower build
panic = "abort"      # smaller; pairs with catch_unwind? ⚠ see note
strip = true         # remove symbols
```

> ⚠ **Important design note:** `panic = "abort"` is **incompatible** with the
> `catch_unwind` strategy from §2.1 (which needs `panic = "unwind"` to contain collector
> panics). **Decision:** keep `panic = "unwind"` in release and obtain size via
> `opt-level="z"` + `lto` + `strip`. Resilience (FR-RES-03) takes priority over the
> last few KB of binary. (Trade-off to confirm at approval.)

### 4.5 Pipeline (high level)

```
matrix targets → (cross | cargo-zigbuild) build --release
              → strip/verify static (ldd should fail on the musl ones)
              → package: harnesssphere-<version>-<target>(.tar.gz)
              → checksums + (optional) cosign/sign + GitHub Release
```

---

## 5. Risks & open decisions (for approval)

1. **`panic=unwind` vs minimal binary** (§4.4) — I recommend unwind. Confirm.
2. **RESOLVED by research → becomes SCOPE FORK A vs B (§1.5).** OpenClaw and Hermes
   **push OTLP** (not trace scraping); OpenClaw also exposes Prometheus. Therefore
   the AI layer is **ingested/enriched**, not originated. Decision pending: Option A
   (unified sidecar collector, recommended) vs Option B (focused host agent).
   PicoClaw remains Optional, to be confirmed.
3. **Interpretation of "Critical fails → app crashes"** — CONFIRMED by the user: *persistent
   failure above THRESHOLD* (tolerates a transient error from a bad `/proc`), not
   *first error → crash*. It is the more robust engineering choice, but it is a reinterpretation
   of the requirement; **it needs explicit confirmation**.
4. **Dynamic dispatch vs `.so` plugins** — v1 uses compile-time features (decided);
   dynamic plugins remain a non-goal.
5. **GenAI content privacy** (GA-05) — default = no content. Confirm.
6. **`harnesssphere.*` † attributes** — several signals (memory files, search index, gateway
   up) have no official semconv; we use our own stable namespace. Review whether any should
   map to an existing convention.
