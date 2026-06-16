# STATE — memória do projeto HarnessSphere

## Estado atual (2026-06-15)
- **PLAN aprovado.** Arquitetura **hexagonal (ports & adapters)**, escopo **Opção A**
  (sidecar collector unificado), conteúdo GenAI **opt-in**, crash crítico por **threshold**.
- **Sprint 1 de scaffolding: CONCLUÍDO e verificado.** Workspace compila (`cargo build`
  limpo), testes do domínio passam (4/4), binário roda end-to-end (host+self → stdout,
  shutdown gracioso via SIGINT).

## Estrutura entregue
- `crates/domain` — modelo de sinal canônico, ports, políticas (breaker + criticality). Puro.
- `crates/runtime` — supervisor (task por source, 3 camadas de contenção, breaker, dreno+batch).
- `crates/collectors` — `HostCollector` + `SelfCollector` (Critical).
- `crates/export` — `StdoutExporter` (default); `OtlpExporter` atrás da feature `otlp` (a fazer).
- `harnesssphere` (bin) — composition root + config TOML/env.

## Decisões técnicas registradas
- **Toolchain: Rust stable 1.96.0**, **edition 2024**, MSRV `rust-version = 1.95`
  (`rust-toolchain.toml` fixa `stable`). `sysinfo` na atual **0.39**.
- **`panic = "unwind"` mantido no release** (não `abort`): a contenção de panic
  (`catch_unwind`, FR-RES-03) depende disso. Tamanho vem de `opt-level="z"`+`lto`+`strip`.
- Domínio sem dependência de `opentelemetry*` (pré-1.0, churn) — SDK confinado em `export/`.

## Lacunas conhecidas da resiliência (deferred — design §2 promete, sprint 1 não entrega)
- **3ª camada de contenção (observação de JoinHandle):** o supervisor guarda os handles
  mas não os observa para re-spawn (Optional) / escalar (Critical). Hoje só há 2 camadas
  (Result + catch_unwind no tick).
- **Panic dentro de `probe()` não é contido** (sem catch_unwind): mata a task daquele
  source silenciosamente. Cobrir junto com a 3ª camada.
- **Fatal usa `drain.abort()`** (descarta buffer) em vez de *flush → exit* prometido em
  §2.2. Implementar flush ordenado do exporter antes do `exit(1)`.
- **Drop-newest** no `ChannelSink` (não drop-oldest) — ver design §2.3.

Verificado no sprint 1: caminho fatal Critical end-to-end (`tests/crash.rs`) **e** que
Optional falhando não derruba; happy-path host+self→stdout; testes de política.

## Próximos passos (backlog)
1. ~~**Adapter OTLP**~~ ✅ **FEITO** (branch `feat/otlp-exporter`): `OtlpExporter`
   (feature `otlp`) — OTLP/gRPC via SDK 0.32, `SdkMeterProvider` + instrumentos síncronos,
   Resource (`service.name`, `host.name`). `PeriodicReader` com intervalo configurável
   (`metric_export_interval_secs`). Wiring no bin via `exporter = "otlp"` +
   `OTEL_EXPORTER_OTLP_ENDPOINT`.
   **Verificado (caminho de sucesso observado):** contra um `otelcol-contrib` real
   (receiver OTLP→exporter debug), o collector recebeu `system.cpu.utilization`,
   `system.memory.usage` (3 data points), `process.memory.usage`, etc. — 8 métricas /
   10 data points, com Resource `service.name=harnesssphere` e `host.name`. Também
   verificado que **contra endpoint morto não derruba** o watcher.
   - **Escopo v1: só métricas** (host/self não emitem log/span). Logs/Spans OTLP quando
     ingest/harness os produzir.
   - **Modelagem:** valores absolutos amostrados (Gauge **e** UpDownCounter) → **Gauge**
     no OTLP. TODO: migrar métricas aditivas (ex.: `system.memory.usage` por estado) para
     instrumentos **observáveis** para preservar a soma da semconv.
   - Cadência de push = reader periódico do SDK (default ~60s), desacoplada do batch do
     drain. Avaliar `PeriodicReader` com intervalo explícito.
2. **Ingest plane** (`crates/ingest`): receiver OTLP local (gRPC :4317/HTTP :4318) +
   Enricher (injeta `host.*`/`container.id`, normaliza Hermes `llm.token_count.*`→`gen_ai.*`)
   + guarda anti-loop + redação de conteúdo (default on).
3. Coletores Optional: `container` (cgroup v2), `prometheus` (scrape do OpenClaw
   `/api/diagnostics/prometheus`).
4. Pipeline de release: `cross` + `cargo-zigbuild` para os 6 targets.

## Pendências de produto
- PicoClaw: confirmar mecanismo de exposição (doc retornou 403). Optional/fallback.
- Confirmar formatos exatos emitidos por OpenClaw/Hermes contra capturas reais de OTLP.
