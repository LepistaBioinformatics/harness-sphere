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
| M | `container.cpu.time` / `container.cpu.usage` | C / G | CPU consumed by the container | 🟡 |
| M | `container.memory.usage` | UDC (By) | Container memory in use (`memory.current`) | 🟡 |
| M | `harnesssphere.container.memory.limit` | G (By) | The container's memory ceiling (`memory.max`) | 🟡 |
| M | `harnesssphere.container.memory.throttled` | C | OOM/throttle events (`memory.events`) | 🟡 |
| M | `harnesssphere.container.cpu.throttled` | C | CPU throttling (`nr_throttled` / `throttled_usec`) | 🟡 |
| M | `container.disk.io` | C (By) | Container disk I/O (`io.stat`) | 🟡 |
| L | container lifecycle | L | Warns when the container vanishes from the cgroup | 🟡 |
| T | — | — | *N/A — cgroup metrics are non-transactional* | — |

### 🚪 Gateway — *Optional*

The control plane that routes the harness's model traffic. HarnessSphere measures route
latency and connection health — by scraping the gateway's Prometheus endpoint and/or by
receiving what it pushes over OTLP.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `http.server.request.duration` | H (s) | Per-route latency, tagged with method, route and status code | 🟡 |
| M | `http.server.active_requests` | UDC | Requests in flight | 🟡 |
| M | `harnesssphere.gateway.up` | G (0/1) | Is the gateway/route reachable? | 🟡 |
| M | `harnesssphere.gateway.connections.active` | UDC | Active connections by state | 🟡 |
| M | `harnesssphere.gateway.probe.latency` | H (s) | Latency of the watcher's own health probe | 🟡 |
| L | dropped connections / upstream 5xx | L | Gateway-side failures with route and status | 🟡 |
| T | trace passthrough | T | Propagated `traceparent` is forwarded so AI traces stay connected | 🟡 |

### 🧠 Harness (the AI) — *Optional* · the star of the show

This is what makes HarnessSphere special. It follows the official **GenAI semantic
conventions** (`gen_ai.*`), so token counts, request durations, and AI transactions land
in your backend in a standard, vendor-neutral shape.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `gen_ai.client.token.usage` | H (`{token}`) | Tokens consumed, split by `input`/`output`, model and provider | 🟡 |
| M | `gen_ai.client.operation.duration` | H (s) | End-to-end latency of each AI operation | 🟡 |
| M | `harnesssphere.harness.messages` | C | Message counts by role (user/assistant/system/tool) | 🟡 |
| M | `harnesssphere.harness.token.cache` | C (`{token}`) | Cache-read and cache-creation tokens | 🟡 |
| M | `harnesssphere.harness.memory.files` / `…memory.bytes` | G | Size of the harness's memory store | 🟡 |
| M | `harnesssphere.harness.search_index.queries` | C | Search-index lookups, tagged `hit`/`miss` | 🟡 |
| M | `harnesssphere.harness.search_index.hit_ratio` | G (0–1) | Search-index hit ratio | 🟡 |
| L | model errors / refusals / cutoffs | L | Finish reasons and error types | 🟡 |
| T | `{operation} {model}` | T | One span per AI transaction (e.g. `chat gpt-4o-mini`) | 🟡 |
| T | `invoke_agent {agent}` | T | Agent/turn structure when the harness exposes it | 🟡 |

> **Privacy by default:** prompt and completion *content* is **never** captured unless you
> explicitly opt in, per layer. By default you get counts, durations and status — not text.

### 🔧 Tools — *Optional*

Every tool the AI invokes, timed and counted, following the GenAI `execute_tool` span
convention.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.tool.execution.duration` | H (s) | How long each tool takes, by name and outcome | 🟡 |
| M | `harnesssphere.tool.calls` | C | Call count per tool | 🟡 |
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
  collectors/   source adapters: host, self (Critical); container, prometheus (optional)
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
- **Host** and **Self** collectors (CPU, memory, swap, process footprint), Critical.
- **stdout** exporter and a verified **OTLP/gRPC** exporter (metrics), confirmed
  end-to-end against a real OpenTelemetry Collector.
- The **ingest plane** (feature `ingest`): a local OTLP/gRPC receiver that OpenClaw/Hermes
  push to. It converts incoming metrics to the canonical model, **enriches them with host
  context**, and forwards them through the same pipeline — verified end-to-end (one
  instance pushing into another, signals arriving enriched with `host.name`).
- Resilience proven by tests: a persistently-failing Critical source exits non-zero; a
  failing Optional source never brings the watcher down.

**On the roadmap**
- Convention normalization in the ingest plane (Hermes `llm.token_count.*` → `gen_ai.*`),
  `container.id` enrichment, content redaction, and an HTTP (`:4318`) receiver.
- The Optional collectors: container (cgroup v2), gateway/Prometheus scrape, harness,
  tools, API.
- OTLP for logs and traces (today the OTLP path covers metrics).
- The cross-compilation release pipeline.

The full technical specification, the complete OTel mapping matrix, and the design
decisions live in [`.specs/`](.specs/).

---

## License

MIT OR Apache-2.0.
