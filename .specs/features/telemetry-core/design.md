# Feature: telemetry-core — Design

> Arquitetura modular, modelo de resiliência, matriz de mapeamento OpenTelemetry e
> estratégia de cross-compilation. Versões verificadas (jun/2026): `opentelemetry 0.32`.
> Semantic conventions citadas conferidas contra a spec oficial (system.\*, process.\*,
> container.\*, http.\*, rpc.\*, gen_ai.\*).

---

## 1. Arquitetura Modular & Extensibilidade (Ports & Adapters)

A arquitetura é **hexagonal (ports & adapters)**, aplicada com disciplina (não dogma):

- **Domínio (hexágono):** modelo de sinal **canônico** (`Metric`/`LogRecord`/`Span`),
  **ports** (traits) e **políticas** puras — criticality/crash, circuit breaker,
  enriquecimento, normalização (dupla convenção Hermes → `gen_ai.*`), redação. **Sem
  dependência de IO nem do SDK OpenTelemetry.**
- **Driving adapters** (iniciam o fluxo p/ dentro): `OtlpReceiver` (push de OpenClaw/
  Hermes) e o supervisor/scheduler que dispara os pulls.
- **Driven adapters** (chamados pelo domínio): fontes (`sysinfo`, `cgroup`,
  `prometheus-scrape`) e sinks (`otlp-exporter`, `stdout-exporter`).

> **Por que hexagonal aqui (não especulativo):** (1) há IO plural nos dois lados;
> (2) o domínio tem lógica pura testável sem rede; (3) as crates `opentelemetry*` estão
> **pré-1.0 (0.32)** e quebram API com frequência — confinar o SDK no adapter de export
> protege o core. Guardrail: adapters de fonte são finos, **um** enum de sinal canônico
> (sem DTO por métrica), port só onde há >1 implementação real.

### 1.1 Visão em camadas

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
        │   collectors/ (cada camada = módulo isolado, atrás de cargo feature)  │
        │  host*  self*  container  gateway  harness  tools  api                │
        │  (* = Critical; demais = Optional)                                    │
        └──────────────────────────────────────────────────────────────────────┘
```

O **core não conhece** nenhum coletor concreto. Conhece apenas o trait `Collector` e o
`Registry`. Cada camada é um módulo em `collectors/` compilado condicionalmente por
`cargo feature`.

### 1.2 Trait central

```rust
/// Contrato único que todo coletor implementa. Object-safe (usado como `dyn Collector`).
#[async_trait::async_trait]
pub trait Collector: Send + Sync + 'static {
    /// Metadados estáticos do coletor (nome, camada, criticidade, intervalo).
    fn descriptor(&self) -> &CollectorDescriptor;

    /// Chamado uma vez no boot. Detecta disponibilidade do alvo.
    /// `Unavailable`/`NotApplicable` para Optional NÃO é erro fatal.
    async fn probe(&mut self, cx: &CollectorCtx) -> ProbeResult;

    /// Um ciclo de coleta. Emite sinais via `cx.emitter`. Retorna Result —
    /// erro é isolado pelo runtime, nunca propaga para o core.
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
    Ready,                 // alvo presente, coleta habilitada
    Unavailable(String),   // alvo ausente/sem resposta (Optional → degrade, não falha)
    NotApplicable,         // não faz sentido neste host (ex.: sem container)
    Fatal(String),         // só Critical pode retornar isto → aborta boot
}
```

`Collector` **é o port `SignalSource`** (driven). `CollectorCtx` carrega config do
coletor e um `SignalSink` (canal para o pipeline do domínio). O coletor "fala" só em
**sinais canônicos do domínio**; o adapter de export (`export/`) é o único que traduz
para OTLP. Assim o SDK pré-1.0 nunca toca o core.

### 1.3 Registro e extensibilidade (adicionar coletor sem tocar no core)

```rust
// composition root (crate `harnesssphere`, bin) — wiring de ports↔adapters
pub fn build_registry(cfg: &Config) -> CollectorRegistry {
    let mut reg = CollectorRegistry::new();
    reg.register(Box::new(HostCollector::new(&cfg.host)));   // sempre (Critical)
    reg.register(Box::new(SelfCollector::new()));            // sempre (Critical)

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

**Para adicionar um coletor novo (ex.: um novo gateway):**
1. Cria `collectors/gateway_envoy.rs` implementando `Collector`.
2. Adiciona a `cargo feature` no `Cargo.toml`.
3. Uma linha `reg.register(...)` atrás de `#[cfg(feature = ...)]`.

O domínio + runtime + adapter de export **não mudam**. O binário não
quebra: features desligadas nem compilam o módulo → zero custo. Esse é o ponto de
extensão único e estável.

> **Decisão de design:** dynamic dispatch (`Box<dyn Collector>`) em vez de enum estático.
> O custo de vtable é irrelevante na escala de coleta (intervalos de segundos) e ganhamos
> extensibilidade aberta. Plugins *dinâmicos* (`.so`/`dlopen`) são **não-objetivo da v1**
> (ABI instável em Rust); a extensão é em compile-time via features.

### 1.4 Layout do workspace (hexagonal, sem prefixo `hs`)

```
harness-sphere/
├─ Cargo.toml                # [workspace]
├─ .cargo/config.toml        # targets, linkers cross
├─ crates/
│  ├─ domain/                # pkg harnesssphere-domain — DOMÍNIO: modelo de sinal
│  │                         #   canônico, ports (SignalSource/Receiver/Exporter/Probe),
│  │                         #   políticas (criticality, breaker, enrich, normalize,
│  │                         #   redact). ZERO IO, ZERO otel.
│  ├─ runtime/               # pkg harnesssphere-runtime — supervisor/scheduler que
│  │                         #   orquestra os ports (driving)
│  ├─ collectors/            # pkg harnesssphere-collectors — driven adapters de fonte:
│  │                         #   host, self (Critical) | container, prometheus (feature)
│  ├─ ingest/                # pkg harnesssphere-ingest — driving adapter: receiver OTLP
│  │                         #   + guarda anti-loop
│  └─ export/                # pkg harnesssphere-export — driven adapter: exporter OTLP +
│                            #   init do SDK + Resource (único lugar com `opentelemetry*`)
├─ harnesssphere/            # pkg harnesssphere (bin) — composition root: config → wiring
└─ .specs/
```

Nomes de pacote `harnesssphere-*` (evita colisão em crates.io); diretórios curtos por
papel. O domínio é a única crate sem dependências de IO/OTel — é onde mora a lógica
testável.

### 1.5 Plano PULL (scrape) vs plano PUSH (ingest) — refinamento pós-pesquisa

A pesquisa na doc do ecossistema (ver `context.md`) revelou que os componentes de IA
**empurram OTLP** e **não** expõem traces para scraping:

- **OpenClaw** → PUSH OTLP (default `:4318`) **+** Prometheus scrape em
  `/api/diagnostics/prometheus`.
- **Hermes** (`hermes-otel`) → PUSH OTLP (BatchSpanProcessor), atributos em dupla
  convenção (`gen_ai.*` e OpenInference `llm.token_count.*`).
- **PicoClaw** → leve (Go, <10MB); exposição nativa não confirmada (provável ClawMetry/
  logs). Tratado como Optional com fallback.

Logo o trait `Collector` (pull) não cobre o tráfego push. Introduzimos um segundo
contrato e o pipeline estilo OTel Collector:

```rust
/// Plano PUSH: recebe OTLP que os componentes empurram, enriquece e re-exporta.
#[async_trait::async_trait]
pub trait Receiver: Send + Sync + 'static {
    fn descriptor(&self) -> &ReceiverDescriptor;          // criticidade SEMPRE Optional
    async fn serve(&mut self, cx: &ReceiverCtx, tx: SignalSink) -> Result<(), RecvError>;
}
```

```
   PUSH  OpenClaw/Hermes ──OTLP──▶ ┌───────────────┐
                                   │ OtlpReceiver   │┐
                                   └───────────────┘│   ┌──────────────┐    ┌──────────┐
   PULL  Host/Self/Container ────▶ ┌───────────────┐├──▶│  Enricher    │──▶ │ Exporter │─OTLP▶ backend
         OpenClaw /prometheus ───▶ │ Collectors     │┘   │ +host/cont.  │    │ (1 saída)│
                                   └───────────────┘    │ +normalize   │    └──────────┘
                                                        └──────────────┘
```

O **Enricher** injeta `host.*`/`container.id` em todo sinal que entra pelo receiver e
**normaliza** a dupla convenção do Hermes (`llm.token_count.prompt` →
`gen_ai.usage.input_tokens`). Esse é o **diferencial**: correlacionar spans `gen_ai.*`
com pressão de recurso do mesmo host.

> **FORK DE ESCOPO — DECIDIDO: Opção A.** ✅
>
> **Opção A — Sidecar collector unificado (ESCOLHIDA):** embute o
> `OtlpReceiver` + scrape Prometheus + coletores de host/cgroup/self + enrich + 1
> exporter OTLP. Um binário substitui "OTel Collector + node exporter". A telemetria de
> IA *passa pelo* HarnessSphere e ganha contexto de host. Escopo maior (roda um servidor
> OTLP local; cuidado com loops de telemetria).
>
> **Opção B — Agente de host focado:** só PULL (host/self/container + scrape Prometheus)
> exporta OTLP; os componentes de IA empurram direto para um Collector externo. Binário
> menor e mais simples, mas o "painel único" enfraquece — a telemetria de IA **não**
> é enriquecida com contexto de host pelo HarnessSphere.

---

## 2. Resiliência & Fallback (Graceful Degradation)

### 2.1 Supervisor + isolamento por task

O `CollectionRuntime` é um supervisor: cada coletor roda numa **task tokio própria** com
seu próprio `tokio::time::interval`. Não há um loop monolítico — assim um coletor lento ou
travado nunca atrasa os outros (FR-RES-01).

```rust
async fn supervise(mut collector: Box<dyn Collector>, cx: CollectorCtx, ctl: SupervisorCtl) {
    let desc = collector.descriptor().clone();
    let mut breaker = CircuitBreaker::new(desc.criticality);
    let mut ticker = tokio::time::interval(cx.config.interval(&desc));

    // probe inicial
    match collector.probe(&cx).await {
        ProbeResult::Fatal(e) => return ctl.report_fatal(&desc, e),   // só Critical chega aqui
        ProbeResult::Unavailable(_) | ProbeResult::NotApplicable => breaker.trip_open(),
        ProbeResult::Ready => {}
    }

    loop {
        ticker.tick().await;
        if breaker.is_open() { /* backoff: re-probe esporádico */ ... continue; }

        let started = Instant::now();
        // FR-RES-03: panic dentro do tick é CONTIDO, não derruba a task nem o processo.
        let outcome = AssertUnwindSafe(collector.collect(&cx)).catch_unwind().await;

        match outcome {
            Ok(Ok(())) => { breaker.record_success(); cx.emit_scrape_ok(&desc, started); }
            Ok(Err(e)) => { handle_failure(&desc, &mut breaker, &ctl, &cx, e.into()); }
            Err(panic) => { handle_failure(&desc, &mut breaker, &ctl, &cx, panic.into()); }
        }
    }
}
```

Três camadas de contenção:
1. **`Result`** — erro esperado (timeout, conexão recusada, parse). → `handle_failure`.
2. **`catch_unwind`** (via `futures::FutureExt`) — `panic` inesperado vira `Err`, contido
   no tick. A task sobrevive. (FR-RES-03)
3. **Task isolada** — se a própria task morrer, o `JoinHandle` é observado pelo supervisor,
   que a re-spawna (para Optional) ou escala para fatal (para Critical).

### 2.2 Circuit breaker + criticidade

```rust
fn handle_failure(desc, breaker, ctl, cx, err) {
    cx.emit_scrape_failure(desc, &err);          // métrica + log (ver matriz §3.2)
    breaker.record_failure();                     // backoff exponencial
    match (desc.criticality, breaker.state()) {
        // Optional: degrada e segue. Auto-recupera quando o alvo volta.
        (Criticality::Optional, _) => tracing::warn!(collector=desc.name, %err, "degraded"),
        // Critical: tolera transitório, mas falha persistente é FATAL (fail-fast).
        (Criticality::Critical, BreakerState::Open) if breaker.consecutive() >= THRESHOLD =>
            ctl.report_fatal(desc, format!("critical collector down: {err}")),
        (Criticality::Critical, _) =>
            tracing::error!(collector=desc.name, %err, "critical transient failure"),
    }
}
```

| | **Critical** (Host, Self) | **Optional** (container, gateway, harness, tools, api) |
|---|---|---|
| Alvo ausente no boot | `Fatal` → exit ≠ 0 | `Unavailable`/`NotApplicable` → breaker aberto, segue |
| Erro transitório | loga `error`, continua | loga `warn`, conta falha |
| Falha persistente (> THRESHOLD) | **processo encerra (exit ≠ 0)** | `Degraded` + backoff, re-probe, **nunca** mata o processo |
| Recuperação | — | breaker fecha sozinho quando alvo responde |

`ctl.report_fatal` sinaliza o core para shutdown ordenado: flush do exporter OTLP →
exit com código ≠ 0. Assim mesmo um crash crítico **exporta o motivo** antes de morrer.

### 2.3 Export resiliente (NFR-04)

O export OTLP roda fora do caminho de coleta (batch + canal bounded). Endpoint OTLP
indisponível **não** bloqueia coleta: buffer limitado e métrica self de itens
descartados. Falha de rede do backend ≠ falha de coletor.

> **Estado da implementação (sprint 1):** o `ChannelSink` descarta o sinal **mais novo**
> (drop-newest) quando o canal enche — `tokio::mpsc` não permite pop da frente; drop-oldest
> exigiria outra estrutura e fica como melhoria. Contagem de descarte já é exposta
> (`harnesssphere.export.items.dropped` †).

---

## 3. Matriz de Mapeamento OpenTelemetry

Convenções: `M` = Metric, `L` = Log, `T` = Trace/Span. Instrumentos: **G**auge
(observable), **C**ounter, **UDC** (UpDownCounter), **H**istogram. Nomes seguem semantic
conventions oficiais; itens fora da spec usam namespace próprio `harnesssphere.*` e estão
marcados com †.

### Resource (global, anexado a todo sinal)
`service.name=harnesssphere`, `service.version`, `service.instance.id`,
`host.name`, `host.id`, `host.arch`, `os.type`.

### 3.a Host  — **CRÍTICO**

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `system.cpu.utilization` | G (0..1) | `cpu`, `system.cpu.logical_number`, `system.cpu.state` (user/system/idle/iowait) |
| M | `system.cpu.time` | C (s) | mesmos atributos de state (alternativa cumulativa) |
| M | `system.memory.usage` | UDC (By) | `system.memory.state` (used/free/cached/buffered) |
| M | `system.memory.utilization` | G (0..1) | idem |
| M | `system.paging.usage` / `system.paging.utilization` | UDC/G | swap |
| M | `system.disk.io` | C (By) | `system.device`, `disk.io.direction` (read/write) |
| M | `system.disk.operations` | C | `system.device`, direction |
| M | `system.disk.io_time` | C (s) | `system.device` |
| M | `system.filesystem.usage` | UDC (By) | `system.device`, `system.filesystem.state` (used/free/reserved), `mountpoint` |
| M | `system.filesystem.utilization` | G (0..1) | idem |
| M | `system.network.io` | C (By) | `network.interface.name`, `network.io.direction` |
| M | `system.network.packet.count` / `system.network.packet.dropped` / `system.network.errors` | C | interface, direction |
| M | `system.network.connection.count` | UDC | `network.transport`, `system.network.state` |
| L | evento de saúde do host | L | thresholds (disco cheio, OOM iminente) como log estruturado WARN/ERROR |
| T | — | — | **N/A** — host é não-transacional; não há span. |

### 3.b Watcher (HarnessSphere — self) — **CRÍTICO**

Auto-observabilidade. Usa `process.*` (semconv) + namespace próprio `harnesssphere.*`.

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `process.cpu.utilization` / `process.cpu.time` | G / C | `process.cpu.state` |
| M | `process.memory.usage` (RSS) / `process.memory.virtual` | UDC (By) | — |
| M | `process.thread.count` | UDC | — |
| M | `process.open_file_descriptors` | UDC | (Linux) |
| M | `harnesssphere.collector.scrape.duration` † | H (s) | `collector.name`, `collector.layer` — tempo de um `collect()` |
| M | `harnesssphere.collection.loop.duration` † | H (s) | duração do ciclo agregado de coleta |
| M | `harnesssphere.collector.scrapes` † | C | `collector.name`, `outcome` (success/error/panic) |
| M | `harnesssphere.collector.state` † | G (enum) | `collector.name` → 0=ready 1=degraded 2=unavailable |
| M | `harnesssphere.export.items.dropped` † | C | `signal` (metric/log/trace) — backpressure do OTLP |
| L | falha de scraping | L (WARN/ERROR) | `collector.name`, `error.type`, `error.message`, `exception.stacktrace` (se panic) |
| L | transição de estado | L (INFO) | breaker open/close, probe result |
| T | `harnesssphere.collection.cycle` † | T (span) | span-pai por ciclo; cada coletor = child span com `collector.name` e status (Ok/Error) |

### 3.c Container (se existir) — **OPCIONAL**

Lê **cgroup v2** diretamente (sem socket de runtime). Namespace `container.*` (semconv).

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `container.cpu.time` / `container.cpu.usage` | C / G | `container.id`, `container.name`, `cpu.mode` |
| M | `container.memory.usage` | UDC (By) | `container.id` (de `memory.current`) |
| M | `harnesssphere.container.memory.limit` † | G (By) | de `memory.max` (semconv de limite ainda evoluindo) |
| M | `harnesssphere.container.memory.throttled` † | C | eventos OOM/`memory.events` |
| M | `container.disk.io` | C (By) | `container.id`, `disk.io.direction` (de `io.stat`) |
| M | `harnesssphere.container.cpu.throttled` † | C | `nr_throttled`/`throttled_usec` de `cpu.stat` |
| L | ciclo de vida | L | container caiu / sumiu do cgroup → WARN (e breaker degrada) |
| T | — | — | **N/A** — métricas de cgroup são não-transacionais; sem span. |

### 3.d Gateway (controle do harness) — **OPCIONAL**

Latência de rotas e saúde de conexões. **Fonte real (pesquisa):** OpenClaw expõe
Prometheus em `GET /api/diagnostics/prometheus` → scrape ativo (`openclaw_model_call_duration_seconds`,
`openclaw_run_*`, `openclaw_message_*`, `openclaw_liveness_*`, `openclaw_memory_bytes`),
mapeados para os instrumentos abaixo. Tráfego que chega por OTLP (Opção A) é enriquecido
e repassado. Onde não há `/metrics`, health-probe ativo do watcher.

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `http.server.request.duration` | H (s) | `http.request.method`, `http.route`, `http.response.status_code`, `server.address` |
| M | `http.server.active_requests` | UDC | `http.request.method`, `http.route` |
| M | `harnesssphere.gateway.up` † | G (0/1) | `gateway.name`, `route` — health probe do watcher |
| M | `harnesssphere.gateway.connections.active` † | UDC | `gateway.name`, `state` |
| M | `harnesssphere.gateway.probe.latency` † | H (s) | latência do health-check ativo |
| L | conexão derrubada / upstream 5xx | L (WARN/ERROR) | `gateway.name`, `route`, `status_code` |
| T | (passthrough) | T | se o gateway propaga `traceparent`, o watcher repassa contexto p/ correlação |

### 3.e Harness (IA) — **OPCIONAL**  ← coração do diferencial

Segue **GenAI semantic conventions** (`gen_ai.*`). Atributos verificados:
`gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`,
`gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`.

**Fonte real (pesquisa):** esta camada **não é observável passivamente** — é ingerida
via OTLP que OpenClaw/Hermes empurram (Opção A) e/ou scrape do Prometheus do OpenClaw.
O Enricher (§1.5) **normaliza** para `gen_ai.*`: OpenClaw já usa semconv +
`openclaw.tokens`/`openclaw.harness.run`; Hermes usa dupla convenção
(`gen_ai.usage.input_tokens` **e** OpenInference `llm.token_count.prompt`) → mapear ambas.
Memory files / search index hits são `harnesssphere.*` † (derivados/observados; OpenClaw
expõe `openclaw.memory.pressure`/`openclaw_memory_bytes` como proxy parcial).

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `gen_ai.client.token.usage` | H (`{token}`) | `gen_ai.token.type` (input/output), `gen_ai.request.model`, `gen_ai.provider.name`, `gen_ai.operation.name` |
| M | `gen_ai.client.operation.duration` | H (s) | `gen_ai.operation.name`, `gen_ai.request.model`, `gen_ai.provider.name` |
| M | `harnesssphere.harness.messages` † | C | `role` (user/assistant/system/tool), `conversation.id?` — contagem de mensagens |
| M | `harnesssphere.harness.token.cache` † | C (`{token}`) | de `gen_ai.usage.cache_read.input_tokens` / `cache_creation.input_tokens` |
| M | `harnesssphere.harness.memory.files` † | G | nº de arquivos de memória; `harnesssphere.harness.memory.bytes` † G (By) |
| M | `harnesssphere.harness.search_index.queries` † | C | `result` (hit/miss) — hits/misses do search index |
| M | `harnesssphere.harness.search_index.hit_ratio` † | G (0..1) | derivado (observable) |
| L | erro/refusal/cutoff do modelo | L (WARN/ERROR) | `gen_ai.response.finish_reasons`, `error.type` |
| T | `{gen_ai.operation.name} {model}` | T (span, CLIENT) | uma transação de IA = span; attrs `gen_ai.*` (sem conteúdo por padrão — GA-05) |
| T | `invoke_agent {agent}` / child spans | T | quando o harness expõe estrutura de agente/turno |

> Conteúdo de prompts/completions **não** é capturado por padrão (privacidade, GA-05);
> só contadores/durações. Captura de conteúdo é opt-in explícito.

### 3.f Tools — **OPCIONAL**

Execução de ferramentas injetadas. Span de tool segue semconv GenAI
(`execute_tool {tool_name}`). **Fonte real:** OpenClaw emite
`openclaw.tool.execution.duration_ms` / `openclaw_tool_execution_total` e span
`openclaw.tool.execution`; Hermes emite span `tool.{name}` filho de `api.{model}`. O
Enricher mapeia ambos para os instrumentos abaixo.

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `harnesssphere.tool.execution.duration` † | H (s) | `gen_ai.tool.name`, `gen_ai.tool.type`, `outcome` (ok/error) |
| M | `harnesssphere.tool.calls` † | C | `gen_ai.tool.name`, `outcome` — chamadas por ferramenta |
| L | erro de execução de tool | L (ERROR) | `gen_ai.tool.name`, `error.type`, `error.message` |
| T | `execute_tool {tool_name}` | T (span, INTERNAL) | `gen_ai.tool.name`, `gen_ai.tool.call.id`; child do span de IA pai (3.e) |

### 3.g API Calls — **OPCIONAL**

Tráfego HTTP/gRPC de entrada e saída. `http.*` e `rpc.*` (semconv).

| Sinal | Nome | Tipo | Atributos / notas |
|---|---|---|---|
| M | `http.client.request.duration` | H (s) | `http.request.method`, `server.address`, `http.response.status_code`, `network.protocol.version` |
| M | `http.server.request.duration` | H (s) | `http.request.method`, `http.route`, `http.response.status_code` |
| M | `http.client.request.body.size` / `...response.body.size` | H (By) | tamanho de payloads (se disponível) |
| M | `rpc.client.duration` / `rpc.server.duration` | H (s) | `rpc.system` (grpc), `rpc.service`, `rpc.method`, `rpc.grpc.status_code` |
| M | `harnesssphere.api.requests` † | C | `direction` (inbound/outbound), `http.response.status_code`, classe (2xx/4xx/5xx) |
| L | 4xx/5xx | L (WARN/ERROR) | método, rota, status, latência |
| T | span HTTP/gRPC client/server | T | `SpanKind` Client/Server; correlaciona com spans de IA (3.e) via trace context |

---

## 4. Estratégia de Compilação & Distribuição (Cross-Compilation)

### 4.1 Targets

| Plataforma | Target triple | Estratégia |
|---|---|---|
| Linux x86_64 (estático) | `x86_64-unknown-linux-musl` | `cross` (musl 100% estático, roda em qualquer distro) |
| Linux ARM64 (estático) | `aarch64-unknown-linux-musl` | `cross` |
| Raspberry Pi 32-bit | `armv7-unknown-linux-musleabihf` | `cross` |
| Raspberry Pi 64-bit | `aarch64-unknown-linux-musl` | `cross` (mesmo do ARM64) |
| macOS Intel | `x86_64-apple-darwin` | `cargo-zigbuild` (cross do Linux/CI) ou nativo |
| macOS Apple Silicon | `aarch64-apple-darwin` | `cargo-zigbuild` ou nativo |
| macOS Universal | `universal2-apple-darwin` | `cargo-zigbuild --target universal2-apple-darwin` (1 binário fat) |

### 4.2 Ferramentas (recomendação)

- **`cross`** (cross-rs) — para todos os targets Linux/ARM/musl. Usa containers com
  toolchains prontas; zero setup de cross-linker local. É o caminho mais simples e
  reproduzível para musl + Raspberry Pi.
- **`cargo-zigbuild`** — usa o `zig cc` como linker para cross-compilar **macOS
  (incl. universal2)** e glibc versionado a partir de uma CI Linux, sem precisar de um
  Mac. Resolve o problema clássico de cross-compile para Apple.
- Alternativa nativa macOS: rodar `cargo build` num runner macOS e fundir com
  `lipo -create` (ou deixar o `universal2` do zigbuild fazer).

> Recomendação: **`cross` (Linux/ARM) + `cargo-zigbuild` (macOS)** cobre todos os
> targets a partir de uma única pipeline Linux. Runner macOS só se quisermos assinatura/
> notarização Apple.

### 4.3 `.cargo/config.toml` (esboço)

```toml
[target.x86_64-unknown-linux-musl]
rustflags = ["-C", "target-feature=+crt-static"]

[target.armv7-unknown-linux-musleabihf]
# linker provido pela imagem do `cross`; nada a fixar localmente
```

### 4.4 Perfil de release (binário enxuto — NFR-02)

```toml
[profile.release]
opt-level = "z"      # otimiza tamanho
lto = true           # link-time optimization (fat)
codegen-units = 1    # melhor otimização, build mais lento
panic = "abort"      # menor; combina com catch_unwind? ⚠ ver nota
strip = true         # remove símbolos
```

> ⚠ **Nota de design importante:** `panic = "abort"` é **incompatível** com a estratégia
> de `catch_unwind` da §2.1 (que precisa de `panic = "unwind"` para conter panics de
> coletor). **Decisão:** manter `panic = "unwind"` no release e obter tamanho via
> `opt-level="z"` + `lto` + `strip`. A resiliência (FR-RES-03) tem prioridade sobre os
> últimos KB de binário. (Trade-off a confirmar na aprovação.)

### 4.5 Pipeline (alto nível)

```
matrix targets → (cross | cargo-zigbuild) build --release
              → strip/verifica estático (ldd deve falhar nos musl)
              → empacota: harnesssphere-<version>-<target>(.tar.gz)
              → checksums + (opcional) cosign/sign + GitHub Release
```

---

## 5. Riscos & decisões em aberto (para a aprovação)

1. **`panic=unwind` vs binário mínimo** (§4.4) — recomendo unwind. Confirmar.
2. **RESOLVIDO pela pesquisa → vira FORK DE ESCOPO A vs B (§1.5).** OpenClaw e Hermes
   **empurram OTLP** (não scrape de traces); OpenClaw também expõe Prometheus. Portanto
   a camada de IA é **ingerida/enriquecida**, não originada. Decisão pendente: Opção A
   (sidecar collector unificado, recomendado) vs Opção B (agente de host focado).
   PicoClaw fica como Optional a confirmar.
3. **Interpretação de "Crítico falha → app crasha"** — CONFIRMADO pelo usuário: *falha
   persistente acima de THRESHOLD* (tolera erro transitório de um `/proc` ruim), não
   *primeiro erro → crash*. É a escolha de engenharia mais robusta, mas é uma releitura
   do requisito; **precisa de confirmação explícita**.
4. **Dynamic dispatch vs plugins `.so`** — v1 usa features em compile-time (decidido);
   plugins dinâmicos ficam como não-objetivo.
5. **Privacidade de conteúdo GenAI** (GA-05) — default = sem conteúdo. Confirmar.
6. **Atributos `harnesssphere.*` †** — vários sinais (memory files, search index, gateway
   up) não têm semconv oficial; usamos namespace próprio estável. Revisar se algum deve
   mapear para convenção existente.
