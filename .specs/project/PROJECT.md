# HarnessSphere — PROJECT

> **This tool is exclusive to zombie-crab.** It was born generic — a watcher for
> any host running the Claw/Harness ecosystem — and that is no longer what it is.
> It is now the observability component of
> [zombie-crab-project](https://github.com/LepistaBioinformatics/zombie-crab-project),
> which runs it as a submodule at `crab/harness-sphere`. Decisions that would make
> it generic again are out of scope; decisions that make it fit that stack better
> are the point. The planning record lives in the parent repository, under
> `.specs/features/harness-sphere-integration/` and
> `.specs/features/harness-sphere-zombie-crab-scope/`.

## Vision

A single-binary watcher for **one stack**: every layer of a zombie-crab
deployment — the host, the watcher itself, the mycelium gateway, crab-shell-proxy,
the exoskeleton webapp, and the per-user picoclaw containers the proxy spawns —
dispatched via **OpenTelemetry (OTLP)** to any compatible backend.

**Why a dedicated tool rather than an off-the-shelf agent.** zombie-crab creates
and destroys a picoclaw container per `(tenant, subscription, agent, user)` at
runtime, and the container's name hashes that tuple one way. Nothing generic can
attribute a container to the member it belongs to; that attribution is the whole
job, and it is what this watcher is being shaped around.

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
   conventions* (system.\*, process.\*, container.\*, http.\*). `gen_ai.*` and `rpc.*`
   are **not** in that list: nothing in this stack produces either, and a convention
   with no producer is a promise rather than a standard.

## Non-goals

- It is not a storage backend nor a dashboard (delegates to the Collector/backend).
- It does not do APM via third-party code instrumentation (it only observes from the
  outside).
- It does not orchestrate nor restart the monitored targets.
- **It is not a general-purpose watcher.** Support for a host that is not a
  zombie-crab deployment is not a goal, and a collector with no source in that
  stack does not stay for symmetry.
- **It never holds a Docker socket.** crab-shell-proxy already mounts one and runs
  as root; a second socket-mounting service would double the blast radius of the
  stack's worst-case compromise. Instance identity comes from the proxy's read-only
  inventory endpoint instead.
- **It never instruments the components it watches.** picoclaw, mycelium and the
  webapp stay black boxes, observed from outside or derived from disk.
- **Token cost is not obtainable and this tool does not pretend otherwise.**
  picoclaw does not write token counts to disk, and the only path that ever existed
  here was scraping an OpenClaw Prometheus endpoint that this stack does not run.

## Target stack

- Rust stable 1.96 (edition 2024, MSRV 1.95), `tokio` runtime.
- `opentelemetry` 0.32.x + `opentelemetry_sdk` + `opentelemetry-otlp` (gRPC/HTTP).
- `sysinfo` (host/process), direct cgroup v2 reading (container), `tracing` +
  `tracing-opentelemetry` for self-observability.
- Cross-compilation via `cross` + `cargo-zigbuild`.

**Deployment shape, which contradicts design principle 1 and does so knowingly.**
It ships as a **compose service on `zombie_net`**, not as a systemd unit on the
host. The picoclaw containers publish no host ports — `18790` exists only on the
internal network — so a host binary would be structurally blind to the layer this
watcher exists for.

The obvious objection does not apply: a container still sees the *host's* memory
and CPU, because `sysinfo` reads `/proc/meminfo` and `/proc/stat` and Docker does
not namespace them. Measured, not assumed — a run capped at `-m 512m` reported the
host's 33 GB and ignored the cgroup limit entirely. **No `/proc` or `/sys` bind is
needed.** The converse is also true and is not a defect: this watcher cannot see
its *own* container ceiling through `HostCollector`.

## The six layers

The layer model **is** these six, in code — cut from seven, with no `Other` variant
and no fallback match arm anywhere, so a seventh kind of thing appearing in this
stack breaks the build rather than filing itself under a catch-all.

| Layer | Component | Criticality |
|---|---|---|
| Host | the machine (*hospedeiro*) | Critical |
| Watcher | this binary observing itself (*self*) | Critical |
| Gateway | `mycelium-gateway` | Optional |
| Proxy | `crab-shell-proxy` | Optional |
| Webapp | `chat-webapp` (repo: `crab-exoskeleton-webapp`) | Optional |
| Harness | the per-user `picoclaw` containers | Optional |

`Api` and `Tools` are gone: no zombie-crab source filled either. **`Container`
stopped being a layer and became a dimension** — everything here runs in a container, so
cgroup counters belong to the layer of whatever the container *is*, and a picoclaw
container's memory is a Harness signal. `tool.calls` survives the loss of the
`Tools` layer: it comes from picoclaw's session JSONL, which is a real source, and
moves under Harness.

## Planning status

- [x] PLAN — design + OTel matrix — **APPROVED** (Scope A; opt-in content; crash by
  threshold). Decisions in `features/telemetry-core/context.md`.
- [ ] Scaffolding (Cargo workspace, traits, collection runtime) — **next, awaiting go**
- [ ] Critical collectors (host, self)
- [x] Optional collectors (process, endpoint probe, session, container)
- [x] Scope reduction — Prometheus scraper and OTLP ingest deleted; `Layer` cut to six
- [ ] Dynamic per-tenant instance discovery — **the central remaining gap**
- [ ] Per-target probe layers (Gateway/Proxy/Webapp stop sharing one label)
- [ ] Release / cross-compile pipeline
