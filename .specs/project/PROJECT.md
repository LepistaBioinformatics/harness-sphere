# HarnessSphere — PROJECT

## Visão

Agente/watcher unificado, single-binary, que coleta telemetria de **todas as camadas
de um host que executa o ecossistema Claw/Harness** — infraestrutura, container,
gateway, harness (IA), tools e chamadas de API — e despacha tudo via **OpenTelemetry
(OTLP)** para qualquer backend compatível (OTel Collector, Grafana/Tempo/Mimir/Loki,
vendors).

## Princípios de design (não-negociáveis)

1. **Single binary, estático e portátil.** Roda em qualquer distro Linux (musl), macOS
   (Intel + Apple Silicon) e Raspberry Pi (ARMv7/AArch64) sem runtime externo.
2. **Nunca derruba o host.** O watcher é um observador passivo; falha de um alvo
   monitorado jamais derruba o HarnessSphere.
3. **Graceful degradation por padrão.** Coletores opcionais que falham são isolados,
   marcados como `degraded` e re-tentados; o resto do pipeline continua.
4. **Crítico vs. Opcional.** Só **Host** e o **próprio Watcher** são obrigatórios; sua
   falha persistente é fatal (exit ≠ 0). Todo o resto é best-effort.
5. **Extensível por traits + feature flags.** Novo coletor = novo módulo que implementa
   `Collector`; o core não muda.
6. **Padrão OTel idiomático.** Nomes de métricas/atributos seguem as *semantic
   conventions* oficiais (system.\*, process.\*, container.\*, http.\*, rpc.\*, gen_ai.\*).

## Não-objetivos (v1)

- Não é um backend de armazenamento nem dashboard (delega ao Collector/backend).
- Não faz APM por instrumentação de código de terceiros (só observa de fora + ingest
  de sinais que o harness expõe).
- Não orquestra nem reinicia os alvos monitorados.

## Stack-alvo

- Rust stable 1.96 (edition 2024, MSRV 1.95), runtime `tokio`.
- `opentelemetry` 0.32.x + `opentelemetry_sdk` + `opentelemetry-otlp` (gRPC/HTTP).
- `sysinfo` (host/process), leitura direta de cgroup v2 (container), `tracing` +
  `tracing-opentelemetry` para self-observability.
- Cross-compilation via `cross` + `cargo-zigbuild`.

## Estado do planejamento

- [x] PLAN — design + matriz OTel — **APROVADO** (Escopo A; conteúdo opt-in; crash por
  threshold). Decisões em `features/telemetry-core/context.md`.
- [ ] Scaffolding (workspace Cargo, traits, runtime de coleta) — **próximo, aguardando go**
- [ ] Coletores críticos (host, self)
- [ ] Coletores opcionais (container, gateway, harness, tools, api)
- [ ] Pipeline de release / cross-compile
