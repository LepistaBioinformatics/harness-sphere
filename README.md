# HarnessSphere

**The watcher for [zombie-crab](https://github.com/LepistaBioinformatics/zombie-crab-project) — and for nothing else.**

HarnessSphere is a single self-contained binary that watches one stack: the machine it
runs on, itself, and the four services zombie-crab runs. It turns what it finds into
standard **OpenTelemetry** metrics and ships them to whatever backend you point it at.

> ### This tool is exclusive to zombie-crab
>
> It started as a general-purpose watcher for any host running the Claw/Harness
> ecosystem. It is now the observability component of **zombie-crab-project**, which
> consumes it as a submodule at `crab/harness-sphere`.
>
> **If you came here for a generic OTel watcher, this is not it.** Collectors with no
> source in this stack were *deleted*, not left configurable — there is no flag that
> brings them back. Crates.io publishing is disabled; binary releases continue.
>
> **Three things it will never do:**
> - **Hold a Docker socket.** The stack's proxy already has one; a second would double
>   the blast radius. Container identity arrives over the proxy's read-only inventory API.
> - **Report token cost.** picoclaw does not write token counts to disk, and the only
>   path that ever existed was scraping an endpoint this stack does not run. This is
>   not deferred — it is gone.
> - **Read transcript content.** Message bodies are member data. Counts, names and
>   sizes only.

---

## Why it exists

zombie-crab is not one service, it is five things stacked on each other, and only two of
them can be asked how they are doing:

- a **host** that can run out of memory or peg its CPU,
- an **api gateway** (mycelium) routing every request,
- a **proxy** (crab-shell-proxy) that creates a container per tenant, on demand,
- a **webapp** (crab-exoskeleton) users actually touch,
- and **picoclaw agent containers** that appear and disappear with traffic.

Before HarnessSphere the stack emitted nothing. Not "not enough" — *nothing*: no metrics
endpoint, no collector, no dashboard. The first question anyone asks during an incident,
"was the box under pressure when that got slow?", had no answer at all.

Because the watcher sits on the same host and the same timeline, it can answer the one
thing separate tools cannot: **whether an agent slowed down because of the agent, or
because of the machine underneath it.**

---

## The six layers

The stack is modelled as exactly **six** layers — the machine, the watcher, and the four
things the machine runs. There is deliberately no `Other` variant and no fallback match
arm: a seventh kind of thing appearing here should break the build, not file itself under
a catch-all.

Two layers are **Critical** — the watcher refuses to run blind without them. The rest are
**Optional**: if they are missing or misbehaving they quietly step aside, and they never
take the watcher down.

> **Legend** — `M` Metric · `L` Log · instruments: **G**auge, **C**ounter,
> **UDC** UpDownCounter, **H**istogram.
> Status — ✅ shipping · 🟡 on the roadmap.
> `harnesssphere.*` keys are our own namespace, used where no OTel semantic convention exists.

### 🖥️ Host (hospedeiro) — *Critical*

The real machine. **Not this container:** Linux does not namespace `/proc`, so `sysinfo`
inside the container reads the host — measured, not assumed (a container capped at 512m
still reported the host's 33 GB). This is why the watcher mounts **no `/proc` and no
`/sys`** bind.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `system.cpu.utilization` | G (0–1) | Fraction of CPU currently in use | ✅ |
| M | `system.memory.usage` | UDC (By) | Bytes of RAM by state — `used` / `free` / `available` | ✅ |
| M | `system.memory.utilization` | G (0–1) | Fraction of RAM in use | ✅ |
| M | `system.paging.usage` | UDC (By) | Swap currently used | ✅ |
| M | `system.paging.utilization` | G (0–1) | Fraction of swap in use | ✅ |
| M | `system.cpu.time` | C (s) | Cumulative CPU time per state | 🟡 |
| M | `system.disk.io` / `system.filesystem.usage` / `system.network.io` | C / UDC | Disk, filesystem and network throughput | 🟡 |
| L | host health events | L | Structured warnings (disk nearly full, OOM imminent) | 🟡 |

### 🛰️ Watcher (self) — *Critical*

A monitoring tool you cannot see is a liability, so it reports its own footprint. A
watcher whose memory grows without bound is a watcher about to become the incident.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `process.cpu.utilization` | G | CPU the watcher itself is using | ✅ |
| M | `process.memory.usage` | UDC (By) | Watcher's resident memory (RSS) | ✅ |
| M | `process.memory.virtual` | UDC (By) | Watcher's virtual memory | ✅ |
| M | `harnesssphere.collector.state` | G | Per-collector health: `0` ready · `1` degraded · `2` unavailable | 🟡 |
| M | `harnesssphere.export.items.dropped` | C | Signals dropped under backpressure | 🟡 |

### 🚪 Gateway — *Optional*

mycelium-api-gateway, the control plane every request passes through.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.endpoint.up` | G (0/1) | Is it reachable? Black-box TCP probe, tagged `server.address` | ✅ |
| M | `harnesssphere.endpoint.probe.duration` | G (s) | Latency of the watcher's own TCP probe | ✅ |
| M | `http.server.request.duration` | H (s) | Per-route latency *(needs a source the gateway does not expose today)* | 🟡 |

### 🦀 Proxy — *Optional*

crab-shell-proxy, which owns the Docker socket and creates one picoclaw container per
`(tenant, subscription, agent, user)`.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.endpoint.up` / `…probe.duration` | G | Liveness and latency, tagged `server.address` and `harnesssphere.layer` | ✅ |
| M | instance inventory freshness | G | Whether `GET /v1/instances` is answering | 🟡 |

### 🖼️ Webapp — *Optional*

crab-exoskeleton-webapp, the chat UI.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.endpoint.up` / `…probe.duration` | G | Liveness and latency, tagged `server.address` and `harnesssphere.layer` | ✅ |

> **These were three anonymous rows until recently.** The probe collector stamped
> `Layer::Gateway` on *every* target — and worse, the layer was never emitted at all, so
> all six layers were invisible to any backend. Each target now names its own layer.

### 🧠 Harness (picoclaw) — *Optional*

The per-tenant agent containers this stack exists to run. Discovery rescans the tenant
tree and runs **one session collector per `(tenant, subscription, agent, user)`**, each
stamped with that tuple, so a number here describes one member rather than an average.

| Signal | Key | Type | What it tells you | Status |
|---|---|---|---|---|
| M | `harnesssphere.harness.messages` | G | Messages by `role`, absolute count across **both** session directories | ✅ |
| M | `harnesssphere.harness.sessions` | G | Conversations — `durable/` mirrors and cron runs excluded | ✅ |
| M | `harnesssphere.tool.calls` | G | Tool calls present in the transcripts | ✅ |
| M | `harnesssphere.harness.cron.sessions` | G | Scheduled-task runs, reported separately so the exclusion above is auditable | ✅ |
| M | `harnesssphere.discovery.workspaces` / `…discovery.scans` | G | How many workspaces are watched, and whether discovery is still scanning | ✅ |
| M | `container.cpu.time` / `container.memory.usage` | C / UDC | Per-container CPU and memory, read from cgroup v2 | ✅ |
| M | `harnesssphere.container.memory.limit` / `…memory.oom` / `…cpu.throttled` | G / C | Memory ceiling, OOM-kills, CPU throttling | ✅ |
| M | per-instance liveness, attributed by tenant | G | Which tenant's agent is up | 🟡 |

> **Why Gauges, not Counters?** The session collector reports the **absolute** total it
> finds on disk, re-derived each scrape. Pushing an absolute value through the OTLP
> Counter path (`add()`) would double-count every tick. These survive restarts — the
> truth lives on disk — and can legitimately fall when transcripts are rotated away.

> **Four distortions are corrected, and each has a test.** Project conversations live in a
> sibling `workspace-<project>/sessions` (missing it drops **42%** of conversations on the
> workspace measured); `sessions/durable/` mirrors the live files 1:1, so counting it
> **exactly doubles** every number; every scheduled-task run writes its own session file,
> drifting `harness.sessions` from *conversations* to *conversations plus every cron run
> since provisioning*; and transcripts are read **incrementally**, with a file that shrank
> re-read from zero rather than treated as a negative delta.

Every signal carries a Resource so you always know where it came from: `service.name`,
`service.version`, `host.name`, `host.id`, `host.arch`, `os.type`.

---

## How each signal is collected

Everything is **pull**. HarnessSphere has no receive path: nothing in this stack pushes
telemetry at it, and a receiver with no sender is code that can only ever be wrong.

| Layer | Mechanism | How it actually works |
|---|---|---|
| **Host** | `sysinfo` | Reads the OS natively. `/proc` is not namespaced, so this is the host, not the container. |
| **Watcher** | `sysinfo` process API | Looks up its own PID. Pure self-observation, no privileges needed. |
| **Watched processes** | `sysinfo`, by name | `watch_processes = ["picoclaw"]` samples any co-located process matching those substrings. |
| **Probes** | Active TCP connect | `probe_targets` opens a connection each tick, recording `up` (0/1) and duration. A target that is down reads an honest **0** rather than going absent — which is why the watcher needs no `depends_on` ordering and survives booting before the things it watches. |
| **Containers** | **cgroup v2, read directly** | Kernel files (`memory.current`, `memory.max`, `cpu.stat`, `io.stat`, `memory.events`) straight off the filesystem. No Docker socket, no runtime API. |
| **Workspaces** | Directory glob of the tenant tree | `tenants/*/subscriptions/*/agents/*/users/*`, rescanned on an interval. Bounded on purpose: a glob, never a transcript walk. Each workspace found becomes one session source, added to a **running** supervisor. |
| **Sessions** | On-disk JSONL transcripts, read incrementally | picoclaw exports no telemetry but writes JSONL under its workspace. Parsed into message, session and tool-call counts, per member. **Content is never read into a signal** — `content` is not even deserialized. |

> **Cadence** is per collector (`host_interval_secs`, `self_interval_secs`); the OTLP
> export ships on `metric_export_interval_secs`.

---

## It never takes itself down

A watcher that crashes is worse than no watcher at all.

- **Three layers of containment.** Every collector runs in its own task. A normal error is
  caught and reported; an unexpected panic is contained via `catch_unwind` so it cannot
  escape; a dead task is observed.
- **Critical vs Optional.** Only **Host** and **Watcher** are Critical. If one fails
  *persistently* — past a configurable threshold, so a single hiccup is forgiven — the
  watcher flushes what it can and exits non-zero, loudly. Everything else degrades, backs
  off, retries, and never brings the process down.
- **A missing target is not an error.** Nothing responding is `Unavailable`, not a crash:
  the collector sits out and keeps probing with backoff.
- **A dead backend does not block collection.** If your OTLP endpoint disappears, export
  fails quietly in the background while collection keeps running.

---

## How it's built

**Hexagonal (ports & adapters)**, applied with discipline rather than dogma. The domain
holds a canonical signal model and pure policies (circuit breaker, criticality,
enrichment) with **zero I/O and zero OpenTelemetry dependency** — so the important logic
is unit-testable without a network, and the churny pre-1.0 OTel SDK cannot leak into the
core.

```
crates/
  domain/       canonical signal model, ports, pure policies (no I/O, no OTel)
  runtime/      supervisor, scheduler, circuit breaker, batching drain
  collectors/   host, self (Critical); process, endpoint-probe, session, container (Optional)
  export/       stdout (default), OTLP (feature `otlp`)
harnesssphere/  the binary: config → wiring → run
```

---

## Getting started

You'll need Rust (stable; the toolchain is pinned via `rust-toolchain.toml`).

```bash
cargo build --release
./target/release/harnesssphere config.example.toml   # prints signals to your terminal
```

For a real backend, build with the OTLP adapter:

```bash
cargo build --release --features otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
HARNESSSPHERE_EXPORTER=otlp \
  ./target/release/harnesssphere config.example.toml
```

**In the stack.** zombie-crab-project runs this as a compose service built from this
directory, configured by `config.zombie-crab.toml`. To actually look at the numbers, that
repo carries an opt-in overlay (OTel Collector → Prometheus → Grafana):

```bash
docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up -d
```

A [SigNoz](https://signoz.io) stack remains vendored under [`deploy/signoz/`](deploy/signoz/)
for the day this emits traces. It emits metrics only today, so that engine would idle.

### Configuration

TOML file (first argument), with environment-variable overrides:

| Key | Default | Meaning |
|---|---|---|
| `host_interval_secs` | `5` | How often to collect host metrics |
| `self_interval_secs` | `10` | How often to collect the watcher's own metrics |
| `critical_threshold` | `3` | Consecutive failures before a Critical collector is fatal |
| `exporter` | `"stdout"` | `"stdout"` or `"otlp"` |
| `otlp_endpoint` | `http://localhost:4317` | OTLP/gRPC endpoint (when `exporter = "otlp"`) |
| `service_name` | `harnesssphere` | `service.name` on the OTel Resource |
| `metric_export_interval_secs` | `15` | How often the OTLP metric reader ships |
| `watch_processes` | `[]` | Executable-name substrings to watch; empty = disabled |
| `probe_targets` | `[]` | `host:port` endpoints to TCP-probe (a port is required); empty = disabled |
| `session_dir` | `""` | Directory of picoclaw session JSONL files (`~/` is expanded); empty = disabled |
| `session_source` | `picoclaw` | `harness.name` label for the parsed sessions |
| `container_cgroup` | `""` | A container's cgroup v2 directory; empty = disabled |
| `container_id` | `""` | `container.id` label; empty → derived from the cgroup directory name |

> Every Optional collector is **off until configured** — a fresh run shows Host and Self
> only. The single-valued keys (`session_dir`, `container_cgroup`, `container_id`) are
> deliberately empty in `config.zombie-crab.toml`: pointing them at one arbitrary instance
> would produce a metric that describes one tenant and reads like it describes the stack.

| Environment variable | Overrides |
|---|---|
| `HARNESSSPHERE_EXPORTER` | the active exporter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the OTLP endpoint |
| `RUST_LOG` | log verbosity |

---

## Runs anywhere

One binary, no runtime to install — from a Linux server down to a Raspberry Pi.

| Platform | Target | How |
|---|---|---|
| Linux x86_64 (static) | `x86_64-unknown-linux-musl` | `cross` |
| Linux ARM64 (static) | `aarch64-unknown-linux-musl` | `cross` |
| Raspberry Pi 32-bit | `armv7-unknown-linux-musleabihf` | `cross` |
| macOS (Intel + Apple Silicon) | `universal2-apple-darwin` | `cargo-zigbuild` |

The release profile is tuned small (`opt-level = "z"`, LTO, stripped). **Panic unwinding
is kept on purpose** — the resilience model depends on `catch_unwind`.

---

## Project status

### What is flowing right now

The Host, Watcher and probe series were read off the **live Prometheus** of a running
stack. The Harness series were verified end to end against a two-tenant tree carrying
every distortion at once — a project workspace, a `durable/` mirror and a cron session.

> **The names in this table are the Prometheus-exposed forms, not the OTel wire names.**
> The OTLP→Prometheus translation rewrites dots to underscores and appends a unit suffix,
> so what this tool *emits* as `system.memory.usage` is *scraped* as
> `system_memory_usage_bytes`. Query with the underscore names; declare instruments with
> the dotted ones. Elsewhere in this README, dotted names are the emitted form.

| Layer | Series | |
|---|---|---|
| **Host** | `system_cpu_utilization`, `system_memory_usage_bytes`, `system_memory_utilization`, `system_paging_usage_bytes`, `system_paging_utilization` | ✅ |
| **Watcher** | `process_cpu_utilization`, `process_memory_usage_bytes`, `process_memory_virtual_bytes` | ✅ |
| **Gateway** | `harnesssphere_endpoint_up`, `harnesssphere_endpoint_probe_duration_seconds` | ✅ |
| **Proxy** | `harnesssphere_endpoint_up`, `harnesssphere_endpoint_probe_duration_seconds` | ✅ |
| **Webapp** | `harnesssphere_endpoint_up`, `harnesssphere_endpoint_probe_duration_seconds` | ✅ |
| **Harness (picoclaw)** | `harnesssphere_harness_messages` (by `role`), `_harness_sessions`, `_tool_calls`, `_harness_cron_sessions` — **one series per workspace**, attributed with the full `(tenant, subscription, agent, user)` tuple | ✅ |

Also working: the hexagonal core (supervisor, circuit breaker, criticality policy), both
exporters verified end to end, resilience proven by tests (a persistently-failing Critical
source exits non-zero; a failing Optional source never brings the watcher down), a
published GHCR image, and a provisioned Grafana dashboard whose every query was checked
against live data.

### What is missing, and why

**1. The proxy's live instance inventory is not consumed yet.**

Discovery reads the **on-disk** tenant tree — DEC-10's resilient surface, and enough to
attribute every conversation to a member. What it cannot yet tell you is whether that
member's container is actually *running*; that needs `GET /v1/instances` on
crab-shell-proxy.

Until then the watcher sits in exactly the state FR-D5 calls the degraded one: session
metrics from disk, no live container state. **That is a designed fallback, not a gap in
the build** — it is the behaviour the whole two-surface design exists to guarantee when
the proxy is down.

**2. Per-instance liveness for agent containers.** A workspace is discovered and its
conversations counted whether or not its container is up. Probing
`<container-name>:18790` needs the inventory above, because harness-sphere **never
computes the container-name hash itself** — that would duplicate a preimage the proxy owns
and silently diverge the day the prefix or the hash changes.

**3. Per-container CPU and memory.** `ContainerCollector` works and is unit-tested, but it
takes one cgroup path and the watcher holds no Docker socket by design, so it has no way
to learn which cgroups exist.

**4. Token cost — permanently unavailable, not pending.** picoclaw writes no token counts
to disk and exposes no metrics endpoint. The only path that ever existed was scraping
OpenClaw's Prometheus endpoint, which this stack does not run, and that scraper has been
deleted. Nothing later in this roadmap brings it back.

### Next

- **The proxy inventory client**, and with it the two-surface reconcile: a workspace on
  disk with no running container becomes *provisioned-not-running* instead of
  indistinguishable from a live one, and a container with no directory is surfaced as an
  anomaly rather than dropped.
- **Per-instance liveness**, probing each agent container by the name the inventory
  reports.
- **Per-container CPU and memory**, which needs a cgroup source.

Remaining host signals (disk, network, filesystem) are further out and nothing depends on
them.

The specification and the design decisions live in [`.specs/`](.specs/) here, and in the
parent repository under `.specs/features/`.

---

## License

MIT OR Apache-2.0.
