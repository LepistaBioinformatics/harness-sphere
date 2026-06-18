# Visualize HarnessSphere with SigNoz

A ready-to-run [SigNoz](https://signoz.io) stack so you can point HarnessSphere at a real
OpenTelemetry backend and *see* the signals it collects — dashboards, metrics explorer,
traces and logs — in a local UI.

> **Attribution:** the files under `docker/` and `common/` are vendored from SigNoz's
> official deployment ([SigNoz/signoz](https://github.com/SigNoz/signoz), `deploy/`,
> Apache-2.0). They're included verbatim so the stack works out of the box. Pinned
> images: `signoz/signoz:v0.128.0`, `signoz/signoz-otel-collector:v0.144.5`,
> `clickhouse/clickhouse-server:25.5.6`, `signoz/zookeeper:3.7.1`.

## 1. Start SigNoz

```bash
docker compose -f deploy/signoz/docker/docker-compose.yaml up -d
```

The first start pulls a few GB of images and runs a ClickHouse migration — give it a
minute or two. Then open the UI:

- **SigNoz UI:** http://localhost:8080 (create a local account on first visit)
- **OTLP ingest:** `localhost:4317` (gRPC) and `localhost:4318` (HTTP)

Check health:

```bash
docker compose -f deploy/signoz/docker/docker-compose.yaml ps
```

## 2. Point HarnessSphere at it

Build with the OTLP exporter and send to SigNoz's collector on `:4317`:

```bash
cargo build --release --features otlp

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
HARNESSSPHERE_EXPORTER=otlp \
  ./target/release/harnesssphere config.example.toml
```

Within ~15s (the metric export interval) you'll see `system.*` and `process.*` metrics
for `service.name=harnesssphere` in SigNoz → **Metrics**.

## 3. (Optional) Relay AI telemetry through HarnessSphere

To exercise the ingest plane — OpenClaw/Hermes push to HarnessSphere, which enriches with
host context and forwards to SigNoz — build with both features and enable ingest on a
**non-colliding** port (SigNoz already uses `4317`/`4318` on the host):

```bash
cargo build --release --features otlp,ingest
# config: exporter="otlp", otlp_endpoint=http://localhost:4317,
#         ingest_enabled=true, ingest_endpoint=0.0.0.0:4319
```

Point OpenClaw/Hermes at `localhost:4319`; their signals arrive in SigNoz stamped with
`host.name` and their original `service.name`.

## Ready-made dashboard

Don't hunt metric-by-metric — import the bundled dashboard:

1. SigNoz UI -> **Dashboards** -> **New dashboard** -> **Import JSON**.
2. Upload [`dashboards/harnesssphere-host.json`](dashboards/harnesssphere-host.json).

It's laid out in five vendor-neutral sections, inner → outer (OTel semantic conventions
plus the `harnesssphere.*` namespace). ✅ = populated by the watcher today, ⏳ = needs an
AI/gateway source emitting:

1. **Harness (AI)** — `gen_ai.client.token.usage` ⏳, `gen_ai.client.operation.duration` ⏳,
   `harnesssphere.harness.messages` (by role) ✅, `harnesssphere.harness.sessions` ✅
   *(session collector — derived from on-disk session files)*.
2. **Tools** — `harnesssphere.tool.execution.duration` ⏳, `harnesssphere.tool.calls` ✅.
3. **Gateway & API** — `harnesssphere.endpoint.up` ✅, `harnesssphere.endpoint.probe.duration` ✅
   *(endpoint probe)*; `http.server.request.duration` ⏳, `harnesssphere.api.requests` ⏳.
4. **Processes** — `process.cpu.utilization` / `process.memory.usage` / `process.memory.virtual`,
   grouped by `process.executable.name` ✅ *(the watcher itself + any watched process, e.g.
   the PicoClaw gateway; the self collector's series carry no executable label)*.
5. **Host** — CPU, memory by `system.memory.state`, memory utilization, swap (`system.*`) ✅.

The ⏳ panels need an AI/gateway source exporting those signals to SigNoz — directly
(`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`) or via the HarnessSphere ingest plane
(which now forwards **metrics, traces, logs and histograms**). The **Services** tab needs
*traces* (build with `--features ingest,otlp`, set `ingest_enabled=true` on a free port);
ingested **logs** show up in the **Logs** tab.

## Tear down

```bash
docker compose -f deploy/signoz/docker/docker-compose.yaml down        # keep data
docker compose -f deploy/signoz/docker/docker-compose.yaml down -v     # wipe data
```
