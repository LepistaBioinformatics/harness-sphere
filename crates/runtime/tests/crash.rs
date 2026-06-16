//! Integração: prova o requisito #1 — um source Critical com falha persistente
//! encerra o supervisor (caminho fatal end-to-end), e um Optical falhando NÃO derruba.

use harnesssphere_domain::{
    CollectError, Criticality, ExportError, Layer, ProbeResult, Signal, SignalExporter, SignalSink,
    SignalSource, SourceDescriptor,
};
use harnesssphere_runtime::{RuntimeConfig, Supervisor};
use std::sync::Arc;
use std::time::Duration;

struct AlwaysFail {
    desc: SourceDescriptor,
}

#[async_trait::async_trait]
impl SignalSource for AlwaysFail {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.desc
    }
    async fn probe(&mut self) -> ProbeResult {
        ProbeResult::Ready
    }
    async fn collect(&mut self, _sink: &dyn SignalSink) -> Result<(), CollectError> {
        Err(CollectError::Failed("boom".into()))
    }
}

struct NoopExporter;
#[async_trait::async_trait]
impl SignalExporter for NoopExporter {
    async fn export(&self, _batch: Vec<Signal>) -> Result<(), ExportError> {
        Ok(())
    }
}

fn source(name: &'static str, crit: Criticality) -> Box<dyn SignalSource> {
    Box::new(AlwaysFail {
        desc: SourceDescriptor {
            name,
            layer: Layer::Host,
            criticality: crit,
            default_interval: Duration::from_millis(2),
        },
    })
}

#[tokio::test]
async fn critical_persistent_failure_is_fatal() {
    let sources = vec![source("host", Criticality::Critical)];
    let exporter: Arc<dyn SignalExporter> = Arc::new(NoopExporter);
    let cfg = RuntimeConfig {
        critical_threshold: 1, // primeira falha já é fatal (sem backoff acumulado)
        ..Default::default()
    };
    let sup = Supervisor::new(cfg, sources, exporter);

    let res = tokio::time::timeout(Duration::from_secs(5), sup.run()).await;
    let inner = res.expect("supervisor não retornou — caminho fatal travou");
    let fatal = inner.expect_err("source Critical sempre falhando deveria ser FATAL");
    assert_eq!(fatal.source, "host");
}

#[tokio::test]
async fn optional_failure_never_kills_supervisor() {
    // Um Optional que falha para sempre NÃO deve gerar fatal. Damos tempo para vários
    // ciclos de falha e então exigimos que o supervisor ainda esteja rodando (timeout).
    let sources = vec![source("gateway", Criticality::Optional)];
    let exporter: Arc<dyn SignalExporter> = Arc::new(NoopExporter);
    let cfg = RuntimeConfig {
        critical_threshold: 1,
        ..Default::default()
    };
    let sup = Supervisor::new(cfg, sources, exporter);

    // Se run() retornar dentro da janela, é porque virou fatal — falha do teste.
    let res = tokio::time::timeout(Duration::from_millis(300), sup.run()).await;
    assert!(
        res.is_err(),
        "supervisor encerrou com source Optional falhando — não deveria"
    );
}
