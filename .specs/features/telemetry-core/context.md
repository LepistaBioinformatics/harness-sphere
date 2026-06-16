# Feature: telemetry-core — Context (decisões do usuário + pesquisa)

> Resolve as gray areas GA-01..05 do spec com base em decisões do usuário e pesquisa
> na documentação do ecossistema Claw/Harness (jun/2026).

## Decisões do usuário

- **GA / Crash crítico (confirmado):** "Host e Watcher obrigatórios" = **falha
  persistente acima de THRESHOLD → exit ≠ 0**. Erro transitório (um `/proc`/leitura
  única ruim) é tolerado. (Resolve risco #3 do design.)
- **GA-01/03/04 (fonte dos dados):** usuário pediu para consultar a documentação de
  **openclaw, picoclaw, hermes agent** — ver pesquisa abaixo.
- **Escopo (FORK A vs B) — DECIDIDO: Opção A** (sidecar collector unificado). O
  HarnessSphere embute receiver OTLP local + scrape Prometheus + coletores host/cgroup/
  self + enrich + 1 exporter OTLP. A telemetria de IA passa pelo watcher e ganha contexto
  de host. (Resolve GA-01/03/04.)
- **GA-05 (conteúdo GenAI) — DECIDIDO: opt-in explícito.** Default = só métricas/
  contadores/durações, sem texto de prompts/completions. Captura de conteúdo só com flag
  de config explícita **por camada**. O enricher deve **redigir** conteúdo por padrão,
  inclusive em passthrough do que OpenClaw/Hermes mandarem, a menos que a flag esteja on.

## Pesquisa — como o ecossistema expõe telemetria

### OpenClaw (gateway + harness) — fonte primária
- **PUSH OTLP/HTTP (protobuf)** para collector; default `http://otel-collector:4318`,
  configurável via `diagnostics.otel.endpoint` ou `OTEL_EXPORTER_OTLP_ENDPOINT`.
  Metrics+traces ligados por padrão; logs opt-in.
- Emite **semconv GenAI**: `gen_ai.client.token.usage`,
  `gen_ai.client.operation.duration` (com `OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental`,
  spans `{operation} {model}`).
- Emite **métricas próprias** `openclaw.*`: `openclaw.tokens`, `openclaw.cost.usd`,
  `openclaw.run.duration_ms`, `openclaw.model_call.{duration_ms,request_bytes,response_bytes,time_to_first_byte_ms}`,
  `openclaw.tool.execution.duration_ms`, `openclaw.tool.loop.iterations`,
  `openclaw.harness.duration_ms`, `openclaw.session.*`, `openclaw.queue.*`.
- Spans próprios: `openclaw.harness.run`, `openclaw.model.call`, `openclaw.model.usage`,
  `openclaw.run`, `openclaw.tool.execution`, `openclaw.session.stuck`, `openclaw.memory.pressure`.
- **TAMBÉM expõe Prometheus** (plugin `diagnostics-prometheus`) em
  `GET /api/diagnostics/prometheus`: `openclaw_run_*`, `openclaw_model_call_*`,
  `openclaw_model_tokens_total`, `openclaw_model_cost_usd_total`, `openclaw_tool_execution_*`,
  `openclaw_message_received_total`, `openclaw_session_*`,
  `openclaw_liveness_event_loop_delay_p99_seconds`, `openclaw_liveness_cpu_core_ratio`,
  `openclaw_memory_bytes`. Cardinalidade limitada (cap 2048 séries, labels sem IDs crus).

### Hermes Agent (+ plugin hermes-otel) — fonte primária
- **PUSH OTLP** via BatchSpanProcessor (não-bloqueante; fan-out paralelo p/ múltiplos
  backends).
- Hierarquia de spans: `session.{platform}` → `llm.{model}` → `api.{model}` (token
  counts) → `tool.{name}`.
- Métricas: input/completion/total tokens, tool calls, API requests (duração/contagem),
  resumo de sessão (tool count, skills, api-call count, status).
- **Dupla convenção** de atributos: GenAI/Langfuse (`gen_ai.usage.input_tokens`,
  `gen_ai.content.prompt`) **e** OpenInference/Phoenix (`llm.token_count.prompt`,
  `input.value`). → HarnessSphere deve normalizar ambas para semconv `gen_ai.*`.
- Usa **MLflow AI Gateway** como provider default (routing, governança, budgets,
  guardrails, usage logs).

### PicoClaw — fonte secundária (a confirmar)
- Assistente ultra-leve em Go (<10 MB RAM), multi-arch (alinhado ao alvo Raspberry Pi).
- Observabilidade aparece via **ClawMetry** (dashboard externo), não confirmado se há
  OTLP/Prometheus nativo (docs retornaram 403). **TODO:** confirmar mecanismo de
  exposição; provável fallback = parsing de logs ou endpoint ClawMetry.

## Consequência arquitetural (decisão pendente do usuário)

Os componentes-chave **empurram OTLP** (não expõem traces para scraping). Para entregar
o "painel único" da visão, o HarnessSphere precisa de **dois planos**:

1. **Plano de PULL (scrape):** Host, Self, Container (cgroup), e Prometheus do OpenClaw
   (`/api/diagnostics/prometheus`). → trait `Collector` (já desenhado).
2. **Plano de PUSH (ingest):** um **receiver OTLP local** (porta ex. 4318) para onde
   OpenClaw/Hermes empurram; o HarnessSphere **enriquece** com `host.*`/`container.*`/
   correlação e **re-exporta** ao backend upstream. → novo trait `Receiver` + pipeline
   de processamento (padrão OTel Collector: receiver → processor/enrich → exporter).

→ Ver fork de escopo A vs B em `design.md` §1.5. Aguardando decisão do usuário.
