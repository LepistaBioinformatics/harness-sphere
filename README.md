# HarnessSphere

**One tiny watcher. Every layer of your host. All of it spoken in fluent OpenTelemetry.**

HarnessSphere is a single, self-contained binary that sits on a machine running the
Claw/Harness AI ecosystem and quietly watches *everything that matters* — the hardware,
the container, the gateway, the AI harness itself, the tools it runs, and the API calls
flowing in and out. It turns all of that into clean, standard **OpenTelemetry** signals
and ships them to whatever observability backend you already use.

No agents to babysit. No five different exporters duct-taped together. No runtime to
install. Just drop in one binary and start seeing.

---

## Why HarnessSphere exists

Running an AI agent in production means you're really running *several* systems stacked
on top of each other:

- a **host** that can run out of memory or peg its CPU,
- a **container** that can hit its cgroup limits,
- a **gateway** that routes model traffic and can get slow or flaky,
- an **AI harness** burning tokens, hitting its memory store, and calling tools,
- and a constant stream of **API calls** that can start returning errors.

Normally you'd watch each of those with a different tool and stitch the dashboards
together by hand. When something breaks at 3 a.m., you're tab-hopping between five panes
trying to figure out whether the model slowed down because of the gateway, the host, or
the moon.

HarnessSphere collapses that into **one pane of glass**. Because it lives on the same
host, it can do something the separate tools can't: **correlate an AI slowdown with the
exact resource pressure that caused it** — the same machine, the same timeline, the same
trace.

---

## The layers it watches

This is the heart of HarnessSphere. It models the host as **seven layers**, and each
layer is an isolated module. Two of them are **Critical** (the watcher refuses to run
blind without them); the rest are **Optional** (if they're missing or misbehaving, they
quietly step aside — they never take the watcher down).

> **Legend** — `M` Metric · `L` Log · `T` Trace/Span · instruments: **G**auge,
> **C**ounter, **UDC** UpDownCounter, **H**istogram.
> Status — ✅ shipping today · 🟡 designed & specified (on the roadmap).
> Keys prefixed with `harnesssphere.*` are our own namespace, used where no official
> OpenTelemetry semantic convention exists yet.

### 🖥️ Host — *Critical*

The physical (or virtual) machine underneath everything. If HarnessSphere can't read the
host, there's no point pretending to monitor anything — so this layer is mandatory.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `system.cpu.utilization` | G (0–1) | Fraction of CPU currently in use | ✅ |
| M | `system.memory.usage` | UDC (By) | Bytes of RAM by state — `used` / `free` / `available` | ✅ |
| M | `system.memory.utilization` | G (0–1) | Fraction of RAM in use | ✅ |
| M | `system.paging.usage` | UDC (By) | Swap currently used | ✅ |
| M | `system.paging.utilization` | G (0–1) | Fraction of swap in use | ✅ |
| M | `system.cpu.time` | C (s) | Cumulative CPU time per state (user/system/idle…) | 🟡 |
| M | `system.disk.io` / `system.disk.operations` / `system.disk.io_time` | C | Disk throughput, op counts, and busy time per device | 🟡 |
| M | `system.filesystem.usage` / `system.filesystem.utilization` | UDC / G | Space used vs. free per mount point | 🟡 |
| M | `system.network.io` | C (By) | Bytes sent/received per interface | 🟡 |
| M | `system.network.packet.count` / `system.network.packet.dropped` / `system.network.errors` | C | Packet counts, drops and errors per interface | 🟡 |
| M | `system.network.connection.count` | UDC | Open connections by transport/state | 🟡 |
| L | host health events | L | Structured warnings/errors (disk nearly full, OOM imminent) | 🟡 |
| T | — | — | *N/A — the host is non-transactional, so there are no spans* | — |

### 🛰️ The Watcher itself (Self) — *Critical*

HarnessSphere watches its own back. A monitoring tool you can't see is a liability, so it
reports its own footprint and how healthy its collection loop is. Also mandatory.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `process.cpu.utilization` | G | CPU the watcher itself is using | ✅ |
| M | `process.memory.usage` | UDC (By) | Watcher's resident memory (RSS) | ✅ |
| M | `process.memory.virtual` | UDC (By) | Watcher's virtual memory | ✅ |
| M | `process.thread.count` / `process.open_file_descriptors` | UDC | Threads and open file descriptors | 🟡 |
| M | `harnesssphere.collector.scrape.duration` | H (s) | How long one collector's scrape takes | 🟡 |
| M | `harnesssphere.collection.loop.duration` | H (s) | How long a full collection cycle takes | 🟡 |
| M | `harnesssphere.collector.scrapes` | C | Scrapes counted by outcome (`success`/`error`/`panic`) | 🟡 |
| M | `harnesssphere.collector.state` | G | Per-collector health: `0` ready · `1` degraded · `2` unavailable | 🟡 |
| M | `harnesssphere.export.items.dropped` | C | Signals dropped under backpressure, per signal type | 🟡 |
| L | scrape failures & state transitions | L | Which collector failed, why, with stack trace on panic | 🟡 |
| T | `harnesssphere.collection.cycle` | T | One span per cycle, with a child span per collector | 🟡 |

### 📦 Container — *Optional*

If the harness runs inside a container, HarnessSphere reads its **cgroup v2** stats
directly from the kernel — no Docker socket, no runtime API, no extra permissions.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `container.cpu.time` | C (s) | CPU time consumed by the container (`cpu.stat` → `usage_usec`) | ✅ |
| M | `container.memory.usage` | UDC (By) | Container memory in use (`memory.current`) | ✅ |
| M | `harnesssphere.container.memory.limit` | G (By) | The container's memory ceiling (`memory.max`; omitted when unlimited) | ✅ |
| M | `harnesssphere.container.memory.oom` | C | OOM-kill events (`memory.events` → `oom_kill`) | ✅ |
| M | `harnesssphere.container.cpu.throttled` | C (s) | CPU throttling time (`cpu.stat` → `throttled_usec`) | ✅ |
| M | `container.disk.io` | C (By) | Container disk I/O by direction (`io.stat` → rbytes/wbytes, tagged `disk.io.direction`) | ✅ |
| L | container lifecycle | L | Warns when the container vanishes from the cgroup | 🟡 |
| T | — | — | *N/A — cgroup metrics are non-transactional* | — |

### 🚪 Gateway — *Optional*

The control plane that routes the harness's model traffic. HarnessSphere measures route
latency and connection health — by scraping the gateway's Prometheus endpoint and/or by
receiving what it pushes over OTLP.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.endpoint.up` | G (0/1) | Is the gateway/endpoint reachable? (black-box TCP probe, tagged `server.address`) | ✅ |
| M | `harnesssphere.endpoint.probe.duration` | G (s) | Latency of the watcher's own TCP health probe | ✅ |
| M | `http.server.request.duration` | H (s) | Per-route latency, tagged with method, route and status code | 🟡 |
| M | `http.server.active_requests` | UDC | Requests in flight | 🟡 |
| M | `harnesssphere.gateway.connections.active` | UDC | Active connections by state | 🟡 |
| L | dropped connections / upstream 5xx | L | Gateway-side failures with route and status | 🟡 |
| T | trace passthrough | T | Propagated `traceparent` is forwarded so AI traces stay connected | 🟡 |

### 🧠 Harness (the AI) — *Optional* · the star of the show

This is what makes HarnessSphere special. It follows the official **GenAI semantic
conventions** (`gen_ai.*`), so token counts, request durations, and AI transactions land
in your backend in a standard, vendor-neutral shape.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.harness.messages` | G | Messages by `role` (user/assistant/system/tool) — the **absolute count present in the on-disk transcripts**, re-derived each scrape (not a since-start counter), tagged `harness.name` | ✅ |
| M | `harnesssphere.harness.sessions` | G | Number of session transcripts on disk (absolute, re-derived each scrape) | ✅ |
| M | `gen_ai.client.token.usage` | H (`{token}`) | Tokens consumed, split by `input`/`output`, model and provider *(needs a GenAI source — not derivable from disk)* | 🟡 |
| M | `gen_ai.client.operation.duration` | H (s) | End-to-end latency of each AI operation | 🟡 |
| M | `harnesssphere.harness.token.cache` | C (`{token}`) | Cache-read and cache-creation tokens | 🟡 |
| M | `harnesssphere.harness.memory.files` / `…memory.bytes` | G | Size of the harness's memory store | 🟡 |
| M | `harnesssphere.harness.search_index.queries` | C | Search-index lookups, tagged `hit`/`miss` | 🟡 |
| M | `harnesssphere.harness.search_index.hit_ratio` | G (0–1) | Search-index hit ratio | 🟡 |
| L | model errors / refusals / cutoffs | L | Finish reasons and error types | 🟡 |
| T | `{operation} {model}` | T | One span per AI transaction (e.g. `chat gpt-4o-mini`) | 🟡 |
| T | `invoke_agent {agent}` | T | Agent/turn structure when the harness exposes it | 🟡 |

> **Privacy by default:** prompt and completion *content* is **never** captured unless you
> explicitly opt in, per layer. By default you get counts, durations and status — not text.

> **Why Gauges, not Counters here?** The session collector re-reads the *full* transcripts on
> every scrape and reports the **absolute** total it finds — so `harness.messages`,
> `harness.sessions` and `tool.calls` are Gauges (an absolute sample), not additive Counters.
> Emitting an absolute value through the OTLP Counter path (`add()`) would double-count each
> tick. They survive restarts (the truth lives on disk) and can fall if transcripts are rotated
> away. The push-based `gen_ai.*`/`execute_tool` signals — true per-event deltas — remain
> Counters/Histograms.

### 🔧 Tools — *Optional*

Every tool the AI invokes, timed and counted, following the GenAI `execute_tool` span
convention.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.tool.calls` | G | Tool-call count present in the transcripts — **absolute, re-derived each scrape** (a Gauge sample, not a delta counter), tagged `harness.name` | ✅ |
| M | `harnesssphere.tool.execution.duration` | H (s) | How long each tool takes, by name and outcome *(needs a GenAI source)* | 🟡 |
| L | tool execution errors | L | Tool name, error type and message | 🟡 |
| T | `execute_tool {tool_name}` | T | A span per tool call, nested under its parent AI span | 🟡 |

### 🌐 API Calls — *Optional*

The HTTP and gRPC traffic flowing in and out, using the standard `http.*` and `rpc.*`
conventions.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `http.client.request.duration` / `http.server.request.duration` | H (s) | Outbound/inbound request latency with method, route, status | 🟡 |
| M | `http.client.request.body.size` / `…response.body.size` | H (By) | Payload sizes | 🟡 |
| M | `rpc.client.duration` / `rpc.server.duration` | H (s) | gRPC latency with service, method and status code | 🟡 |
| M | `harnesssphere.api.requests` | C | Request counts by direction and status class (2xx/4xx/5xx) | 🟡 |
| L | 4xx / 5xx responses | L | Method, route, status and latency | 🟡 |
| T | client/server spans | T | HTTP/gRPC spans, correlated with the AI traces above | 🟡 |

Every signal also carries a global **Resource** so you always know *where* it came from:
`service.name=harnesssphere`, `service.version`, plus `host.name`, `host.id`, `host.arch`
and `os.type`.

---

## How each signal is collected

HarnessSphere gathers data two ways: it **pulls** (reaches out and reads/scrapes a
target) and it **receives** (lets a target push to it). Here's exactly where every layer's
data comes from.

| Layer | Mechanism | How it actually works | Status |
|---|---|---|---|
| **Host** | Pull — `sysinfo` crate | On each tick the host collector refreshes [`sysinfo`](https://crates.io/crates/sysinfo), which reads the OS natively (`/proc` and `/sys` on Linux, equivalent APIs on macOS). CPU comes from the aggregate utilization, memory from total/used/free/available, swap from the paging counters. | ✅ |
| **Watcher (Self)** | Pull — `sysinfo` process API | The watcher looks up *its own* PID (`get_current_pid`) and reads that process's CPU and memory (RSS, virtual) — pure self-observation, no privileges needed. | ✅ |
| **Watched processes** | Pull — `sysinfo` process API, by name | Configure `watch_processes = ["picoclaw"]` and the watcher samples any co-located process matching those name substrings — `process.cpu.utilization`, `process.memory.usage/virtual`, tagged `process.executable.name` + `process.pid`. Harness-independent visibility into a component that exports nothing itself. | ✅ |
| **Container** | Pull — **cgroup v2**, read directly | Point `container_cgroup` at a container's cgroup v2 directory and it reads the kernel files (`memory.current`, `memory.max`, `cpu.stat`, `io.stat`, `memory.events`) straight from the filesystem. No Docker socket, no runtime API, no extra permissions. | ✅ |
| **Gateway (probe)** | Pull — active TCP probe | Configure `probe_targets = ["localhost:18790"]` and the watcher opens a TCP connection each tick, recording `harnesssphere.endpoint.up` (0/1) and `…probe.duration` — black-box liveness/latency for a gateway that exposes no metrics of its own. | ✅ |
| **Gateway (scrape)** | Pull — Prometheus scrape | Scrapes the gateway's Prometheus endpoint (e.g. OpenClaw's `/api/diagnostics/prometheus`) for richer route/connection metrics. | 🟡 |
| **Harness (sessions)** | Pull — on-disk session files | A harness like PicoClaw exports no telemetry but writes JSONL session transcripts under `~/.picoclaw/workspace/sessions/`. The session collector parses them into `harnesssphere.harness.messages` (by `role`), `harnesssphere.tool.calls` and `harnesssphere.harness.sessions`. **Token cost is not on disk**, so it isn't derived here. | ✅ |
| **Harness (AI)** | **Push — OTLP ingest** | For tokens, durations and `gen_ai.*` traces — data *internal to the app* — OpenClaw/Hermes **push OTLP** to HarnessSphere's local receiver; it converts each signal to the canonical model, **preserves the origin identity** (`service.name`, `gen_ai.*` from the OTLP Resource) and **enriches it with host context** (`host.name`) before forwarding. Metrics, traces, logs and histograms are all accepted. | ✅ ingest |
| **Tools** | **Push — OTLP ingest** | Tool spans/metrics arrive in the same push stream as the harness (e.g. Hermes' `execute_tool` spans); converted and enriched like everything else. | ✅ ingest |
| **API Calls** | **Push — OTLP ingest** | HTTP/gRPC metrics and spans the components emit, received over OTLP and enriched. | 🟡 |

**The journey of one signal.** A *pulled* signal (host/self) is read by a collector,
turned into a canonical `Metric`/`Log`/`Span`, and dropped onto an internal channel. A
*pushed* signal arrives at the OTLP receiver, is converted from OTLP to the same canonical
shape, **enriched** with host context, and dropped onto the same channel. From there a
single batching drain hands everything to the active exporter (stdout, or OTLP to your
backend). Because both paths converge on one model and one pipeline, AI telemetry and host
telemetry end up correlated — same host, same timeline.

> **Pull cadence** is configurable per collector (`host_interval_secs`,
> `self_interval_secs`). **Push** is driven by whatever the components send; the OTLP
> *export* to your backend ships on `metric_export_interval_secs`.

---

## It never takes itself down

HarnessSphere is a *watcher*, and a watcher that crashes is worse than no watcher at all.
Resilience isn't a feature here — it's the foundation.

- **Three layers of containment.** Every collector runs in its own task. A normal error
  is caught and reported. An unexpected panic is *contained* (via `catch_unwind`) so it
  can't escape. A dead task is observed and restarted.
- **Critical vs. Optional.** Only **Host** and **Self** are Critical. If one of them fails
  *persistently* (past a configurable threshold — a single transient hiccup is forgiven),
  the watcher flushes what it can and exits with a non-zero code, loudly. Everything else
  is Optional: it degrades, backs off, retries, and **never** brings the process down.
- **A missing target isn't an error.** No container? No gateway responding? That's
  `Unavailable`/`NotApplicable`, not a crash — the collector simply sits out and keeps
  probing.
- **A dead backend doesn't block collection.** If your OpenTelemetry endpoint disappears,
  export fails quietly in the background while collection keeps running.

---

## How it's built

HarnessSphere uses a **hexagonal (ports & adapters)** architecture, applied with
discipline rather than dogma:

- **The domain (the hexagon)** holds a *canonical signal model* and pure policies (circuit
  breaker, criticality, enrichment, normalization, redaction). It has **zero I/O and zero
  OpenTelemetry dependency** — which means all the important logic is unit-testable without
  a network, and the churny pre-1.0 OTel SDK can't leak into the core.
- **Driven adapters** are the edges that the core calls: the source collectors
  (`sysinfo`, cgroup, Prometheus scrape) and the exporters (OTLP, stdout).
- **Driving adapters** push work in: the OTLP receiver and the supervisor that schedules
  collection.

**Adding a new collector takes one new module and one line** — implement the `Collector`
trait, put it behind a Cargo feature, and register it. The core never changes.

```
crates/
  domain/       canonical signal model, ports, pure policies (no I/O, no OTel)
  runtime/      supervisor, scheduler, circuit breaker, batching drain
  collectors/   source adapters: host, self (Critical); process, endpoint-probe, session, container (Optional)
  ingest/       driving adapter: local OTLP receiver (feature `ingest`)
  export/       output adapters: stdout (default), OTLP (feature `otlp`)
harnesssphere/  the binary: config → wiring → run
```

---

## Born from the Claw/Harness ecosystem

HarnessSphere is designed to fit the real tools people run:

- **OpenClaw** pushes OTLP (and exposes Prometheus at `/api/diagnostics/prometheus`),
  emitting both standard `gen_ai.*` metrics and its own `openclaw.*` keys.
- **Hermes Agent** (via `hermes-otel`) pushes nested OTLP spans for sessions, LLM calls
  and tools.
- **PicoClaw** is the ultra-light option — perfect for the Raspberry Pi targets.

Because these components *push* their telemetry, HarnessSphere acts as a local
**collect-and-enrich** hub: it receives what they emit, stamps it with host context
(`host.name`, container id), normalizes differing conventions into clean `gen_ai.*`, and
forwards one tidy stream onward.

---

## Getting started

You'll need Rust (stable; the repo pins the toolchain via `rust-toolchain.toml`).

```bash
# Build it
cargo build --release

# Run it — prints canonical signals to your terminal (great for a first look)
./target/release/harnesssphere config.example.toml
```

By default HarnessSphere uses the **stdout** exporter, which prints each signal in a
human-readable line. When you're ready to send to a real backend, switch to **OTLP**:

```bash
# Build with the OTLP adapter
cargo build --release --features otlp

# Point it at any OpenTelemetry collector (gRPC)
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
HARNESSSPHERE_EXPORTER=otlp \
  ./target/release/harnesssphere config.example.toml
```

**Want to actually see it?** Spin up a local [SigNoz](https://signoz.io) and watch the
signals land in a dashboard — one command, see [`deploy/signoz/`](deploy/signoz/):

```bash
docker compose -f deploy/signoz/docker/docker-compose.yaml up -d   # UI at :8080, OTLP at :4317
```

### Configuration

Configure via a TOML file (passed as the first argument) with environment-variable
overrides:

| Key | Default | Meaning |
|---|---|---|
| `host_interval_secs` | `5` | How often to collect host metrics |
| `self_interval_secs` | `10` | How often to collect the watcher's own metrics |
| `critical_threshold` | `3` | Consecutive failures before a Critical collector is fatal |
| `exporter` | `"stdout"` | `"stdout"` or `"otlp"` |
| `otlp_endpoint` | `http://localhost:4317` | OTLP/gRPC endpoint (when `exporter = "otlp"`) |
| `service_name` | `harnesssphere` | `service.name` on the OTel Resource |
| `metric_export_interval_secs` | `15` | How often the OTLP metric reader ships |
| `ingest_enabled` | `false` | Enable the local OTLP/gRPC ingest receiver (feature `ingest`) |
| `ingest_endpoint` | `0.0.0.0:4318` | Address the ingest receiver binds to |
| `watch_processes` | `[]` | Executable-name substrings to watch (e.g. `["picoclaw"]`); empty = disabled |
| `probe_targets` | `[]` | `host:port` endpoints to TCP-probe for liveness/latency (each entry must include a port); empty = disabled |
| `session_dir` | `""` | Directory of harness session JSONL files (a leading `~/` is expanded); empty = disabled |
| `session_source` | `picoclaw` | `harness.name` label for the parsed sessions |
| `container_cgroup` | `""` | A container's cgroup v2 directory to read; empty = disabled |
| `container_id` | `""` | `container.id` label; empty → derived from the cgroup directory's name (works for Docker-style `docker-<id>.scope` paths — set it explicitly under Podman/systemd/custom cgroup managers) |

> Each Optional collector is **off until you configure it** — that's why a fresh run shows
> only Host and Self. Set the keys above (or use [`config.example.toml`](config.example.toml))
> to light up watched processes, probes, sessions and containers.

| Environment variable | Overrides |
|---|---|
| `HARNESSSPHERE_EXPORTER` | the active exporter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the OTLP endpoint |
| `RUST_LOG` | log verbosity (e.g. `info`, `warn`, `trace`) |

---

## Runs anywhere

One binary, statically linked, no runtime to install — from a beefy Linux server down to a
Raspberry Pi.

| Platform | Target | How |
|---|---|---|
| Linux x86_64 (static) | `x86_64-unknown-linux-musl` | `cross` |
| Linux ARM64 (static) | `aarch64-unknown-linux-musl` | `cross` |
| Raspberry Pi 32-bit | `armv7-unknown-linux-musleabihf` | `cross` |
| Raspberry Pi 64-bit | `aarch64-unknown-linux-musl` | `cross` |
| macOS (Intel + Apple Silicon) | `universal2-apple-darwin` | `cargo-zigbuild` |

The release profile is tuned for a small, dependency-free binary (`opt-level = "z"`, LTO,
stripped). Panic unwinding is kept on purpose — the resilience model depends on it.

---

## Project status

HarnessSphere is under active development. Here's the honest state of things:

**Working today**
- The full hexagonal core: domain model, supervisor/runtime, circuit breaker and
  criticality policy.
- **Critical** collectors — **Host** (CPU, memory, swap) and **Self** (the watcher's own
  process footprint).
- **Optional** collectors, each off until configured:
  - **Watched processes** — `process.*` for any co-located process matched by name.
  - **Endpoint probe** — `harnesssphere.endpoint.up`/`probe.duration` via black-box TCP.
  - **Sessions** — `harnesssphere.harness.messages`/`sessions` and `tool.calls` parsed
    from a harness's on-disk JSONL transcripts.
  - **Container** — `container.*` (+ `harnesssphere.container.*`) read straight from
    cgroup v2 (memory, CPU time/throttle, OOM-kills, disk I/O); unit-tested against a fake
    cgroup tree.
- **stdout** exporter and a verified **OTLP/gRPC** exporter — **metrics, traces, logs and
  histograms** — confirmed end-to-end against a real SigNoz/OpenTelemetry Collector.
- The **ingest plane** (feature `ingest`): a local OTLP/gRPC receiver that OpenClaw/Hermes
  push to. It converts incoming **metrics, traces, logs and histograms** to the canonical
  model, **enriches them with host context**, and forwards them through the same pipeline.
  Verified end-to-end: metrics (instance-to-instance), **traces** (spans landing in the
  **Services/APM** view, grouped by `service.name`) and **logs** (in the **Logs** tab).
- Resilience proven by tests: a persistently-failing Critical source exits non-zero; a
  failing Optional source never brings the watcher down.
- CI: `cargo-audit` security scan, PR-driven version bump + GitHub release, a manual
  crates.io publish workflow, and automated DeepSeek PR review.
- A bundled **SigNoz** stack and a ready-made dashboard — see [`deploy/signoz/`](deploy/signoz/).

**On the roadmap**
- **Gateway Prometheus scrape** (OpenClaw `/api/diagnostics/prometheus`) — the last Tier 2 item.
- Convention normalization in the ingest plane (Hermes `llm.token_count.*` → `gen_ai.*`),
  GenAI content redaction, and an HTTP (`:4318`) OTLP receiver.
- Remaining host signals (disk, network, filesystem) and the richer gateway/API metrics.
- The cross-compilation release pipeline (musl, Raspberry Pi, macOS universal).

The full technical specification, the complete OTel mapping matrix, and the design
decisions live in [`.specs/`](.specs/).

---

## License

MIT OR Apache-2.0.
