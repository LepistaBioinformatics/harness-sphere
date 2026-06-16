# Feature: telemetry-core — Context (user decisions + research)

> Resolves the gray areas GA-01..05 of the spec based on user decisions and research
> into the Claw/Harness ecosystem documentation (Jun/2026).

## User decisions

- **GA / Critical crash (confirmed):** "Host and Watcher mandatory" = **persistent
  failure above THRESHOLD → exit ≠ 0**. A transient error (a single bad `/proc`/read)
  is tolerated. (Resolves design risk #3.)
- **GA-01/03/04 (data source):** the user asked to consult the documentation of
  **openclaw, picoclaw, hermes agent** — see the research below.
- **Scope (FORK A vs B) — DECIDED: Option A** (unified sidecar collector). The
  HarnessSphere embeds a local OTLP receiver + Prometheus scrape + host/cgroup/
  self collectors + enrich + 1 OTLP exporter. The AI telemetry passes through the watcher and
  gains host context. (Resolves GA-01/03/04.)
- **GA-05 (GenAI content) — DECIDED: explicit opt-in.** Default = metrics/
  counters/durations only, no prompt/completion text. Content capture only with an explicit
  config flag **per layer**. The enricher must **redact** content by default,
  including in passthrough of whatever OpenClaw/Hermes send, unless the flag is on.

## Research — how the ecosystem exposes telemetry

### OpenClaw (gateway + harness) — primary source
- **PUSH OTLP/HTTP (protobuf)** to a collector; default `http://otel-collector:4318`,
  configurable via `diagnostics.otel.endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT`.
  Metrics+traces on by default; logs opt-in.
- Emits **GenAI semconv**: `gen_ai.client.token.usage`,
  `gen_ai.client.operation.duration` (with `OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental`,
  spans `{operation} {model}`).
- Emits its **own metrics** `openclaw.*`: `openclaw.tokens`, `openclaw.cost.usd`,
  `openclaw.run.duration_ms`, `openclaw.model_call.{duration_ms,request_bytes,response_bytes,time_to_first_byte_ms}`,
  `openclaw.tool.execution.duration_ms`, `openclaw.tool.loop.iterations`,
  `openclaw.harness.duration_ms`, `openclaw.session.*`, `openclaw.queue.*`.
- Its own spans: `openclaw.harness.run`, `openclaw.model.call`, `openclaw.model.usage`,
  `openclaw.run`, `openclaw.tool.execution`, `openclaw.session.stuck`, `openclaw.memory.pressure`.
- **ALSO exposes Prometheus** (plugin `diagnostics-prometheus`) at
  `GET /api/diagnostics/prometheus`: `openclaw_run_*`, `openclaw_model_call_*`,
  `openclaw_model_tokens_total`, `openclaw_model_cost_usd_total`, `openclaw_tool_execution_*`,
  `openclaw_message_received_total`, `openclaw_session_*`,
  `openclaw_liveness_event_loop_delay_p99_seconds`, `openclaw_liveness_cpu_core_ratio`,
  `openclaw_memory_bytes`. Limited cardinality (cap 2048 series, labels without raw IDs).

### Hermes Agent (+ plugin hermes-otel) — primary source
- **PUSH OTLP** via BatchSpanProcessor (non-blocking; parallel fan-out to multiple
  backends).
- Span hierarchy: `session.{platform}` → `llm.{model}` → `api.{model}` (token
  counts) → `tool.{name}`.
- Metrics: input/completion/total tokens, tool calls, API requests (duration/count),
  session summary (tool count, skills, api-call count, status).
- **Dual convention** of attributes: GenAI/Langfuse (`gen_ai.usage.input_tokens`,
  `gen_ai.content.prompt`) **and** OpenInference/Phoenix (`llm.token_count.prompt`,
  `input.value`). → HarnessSphere must normalize both to the `gen_ai.*` semconv.
- Uses **MLflow AI Gateway** as the default provider (routing, governance, budgets,
  guardrails, usage logs).

### PicoClaw — secondary source (to be confirmed)
- Ultra-light assistant in Go (<10 MB RAM), multi-arch (aligned with the Raspberry Pi target).
- Observability appears via **ClawMetry** (external dashboard), not confirmed whether there is
  native OTLP/Prometheus (docs returned 403). **TODO:** confirm the exposure
  mechanism; likely fallback = log parsing or the ClawMetry endpoint.

## Architectural consequence (user decision pending)

The key components **push OTLP** (they do not expose traces for scraping). To deliver
the "single pane" of the vision, HarnessSphere needs **two planes**:

1. **PULL plane (scrape):** Host, Self, Container (cgroup), and OpenClaw's Prometheus
   (`/api/diagnostics/prometheus`). → `Collector` trait (already designed).
2. **PUSH plane (ingest):** a **local OTLP receiver** (port e.g. 4318) where
   OpenClaw/Hermes push; HarnessSphere **enriches** with `host.*`/`container.*`/
   correlation and **re-exports** to the upstream backend. → new `Receiver` trait + processing
   pipeline (OTel Collector pattern: receiver → processor/enrich → exporter).

→ See the A vs B scope fork in `design.md` §1.5. Awaiting the user's decision.
