# Feature: telemetry-core — Especificação

> Núcleo do HarnessSphere: runtime de coleta, modelo de plugins, resiliência e export OTel.
> IDs rastreáveis para amarrar design → tasks → implementação.

## Requisitos funcionais

### Arquitetura & extensibilidade
- **FR-ARCH-01** — Todo coletor implementa o trait `Collector` e vive num módulo
  isolado (`collectors/<layer>.rs`).
- **FR-ARCH-02** — Coletores são registrados num `CollectorRegistry`; o core itera o
  registry sem conhecer concretamente cada coletor.
- **FR-ARCH-03** — Adicionar um novo coletor **não exige** alterar o core: basta criar
  o módulo, implementar o trait e registrá-lo (atrás de uma `cargo feature`).
- **FR-ARCH-04** — Cada coletor declara metadados estáticos: `name`, `layer`,
  `criticality` (`Critical | Optional`) e `interval`.

### Resiliência & graceful degradation
- **FR-RES-01** — Cada coletor roda numa task `tokio` independente; o scheduler nunca
  bloqueia um coletor por causa de outro.
- **FR-RES-02** — Toda coleta retorna `Result`; erro de um coletor é capturado, logado
  e exportado como métrica de falha, **sem** propagar para o core.
- **FR-RES-03** — `panic` dentro de um coletor é contido (catch_unwind no nível do
  tick); não aborta o processo nem outras tasks.
- **FR-RES-04** — Coletor **Optional** com N falhas consecutivas entra em estado
  `Degraded` com backoff exponencial e *circuit breaker*; auto-recupera quando o alvo
  volta.
- **FR-RES-05** — Coletor **Critical** (Host, Self) com falha persistente acima do
  threshold faz o processo encerrar com exit code ≠ 0 (fail-fast intencional).
- **FR-RES-06** — Ausência de um alvo opcional (ex.: container inexistente, gateway
  fora) é tratada como `NotApplicable`/`Unavailable`, não como erro fatal.

### Telemetria (OTel)
- **FR-OTEL-01** — Exporta os três sinais (metrics, logs, traces) via OTLP
  (gRPC default, HTTP opcional), endpoint/headers configuráveis.
- **FR-OTEL-02** — Nomes de instrumentos e atributos seguem as *semantic conventions*
  oficiais; ver matriz em `design.md`.
- **FR-OTEL-03** — `Resource` global com `service.name=harnesssphere`,
  `service.version`, `host.*`, e atributos de identidade do host.
- **FR-OTEL-04** — O próprio watcher é auto-instrumentado (process.\* + métricas de
  loop/scraping).

### Configuração & distribuição
- **FR-CFG-01** — Configuração via arquivo (TOML) + env vars (override), incluindo quais
  coletores habilitar, intervalos e endpoint OTLP.
- **FR-DIST-01** — Build produz um único binário estático por target (musl/macOS/ARM).

## Requisitos não-funcionais
- **NFR-01** — Footprint baixo: o watcher não deve ser fonte material de carga
  (orçamento-alvo: < ~1% CPU médio, < ~30 MB RSS em estado estacionário; medido por ele
  mesmo via FR-OTEL-04).
- **NFR-02** — Binário enxuto (LTO + `opt-level="z"` + strip) e sem dependências
  dinâmicas no target Linux (musl 100% estático).
- **NFR-03** — Zero crash por causa de alvo monitorado (consequência de FR-RES-\*).
- **NFR-04** — Overhead de export resiliente: falha do endpoint OTLP não bloqueia
  coleta (export assíncrono com buffer/drop bounded).

## Gray areas (a decidir com o usuário antes de TASKS)
- **GA-01** — Identidade exata dos alvos "Gateway" e "Harness": expõem `/metrics`
  (Prometheus), OTLP próprio, logs em arquivo, ou socket/admin API? Define o modo de
  scraping.
- **GA-02** — "Container que roda o harness": runtime é Docker/containerd/podman? Lemos
  cgroup v2 diretamente (preferido, sem socket) ou via API do runtime?
- **GA-03** — Origem dos sinais de IA (tokens/mensagens/search index/memory files):
  o harness já emite OTLP/Prometheus, ou precisamos derivar de logs/arquivos no host?
- **GA-04** — Sandbox de tools: o tempo/contagem por tool vem de instrumentação do
  harness ou o watcher observa processos/execução externamente?
- **GA-05** — Política de privacidade de conteúdo GenAI (prompts/completions): por
  padrão **não** capturar conteúdo (só métricas/contadores), opt-in explícito.

## Decisões registradas (ver context.md)
- **Escopo = Opção A** (sidecar collector unificado: receiver OTLP + scrape + host/self/
  container + enrich + export). Adiciona requisitos:
  - **FR-INGEST-01** — Receiver OTLP local (gRPC :4317 / HTTP :4318) que aceita push de
    OpenClaw/Hermes; criticidade Optional.
  - **FR-INGEST-02** — Enricher injeta `host.*`/`container.id` em todo sinal ingerido e
    normaliza dupla convenção (OpenInference `llm.token_count.*` → `gen_ai.*`).
  - **FR-INGEST-03** — Proteção anti-loop: o exporter do próprio HarnessSphere nunca
    realimenta seu próprio receiver.
- **GA-05 = opt-in explícito por camada.** Enricher **redige conteúdo por padrão**
  (FR-PRIV-01), mesmo em passthrough; só emite texto com flag de config explícita.
- **Crash crítico = falha persistente acima de threshold** (não primeiro erro).

> Gray areas resolvidas registradas em `context.md`.
