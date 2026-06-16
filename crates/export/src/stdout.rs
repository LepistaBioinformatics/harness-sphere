//! Debug exporter: serializes the canonical signal as a readable line on stdout.

use async_trait::async_trait;
use harnesssphere_domain::{ExportError, Signal, SignalExporter};

pub struct StdoutExporter;

impl StdoutExporter {
    pub fn new() -> Self {
        StdoutExporter
    }
}

impl Default for StdoutExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SignalExporter for StdoutExporter {
    async fn export(&self, batch: Vec<Signal>) -> Result<(), ExportError> {
        for sig in batch {
            match sig {
                Signal::Metric(m) => {
                    println!(
                        "METRIC {:<8?} {} = {}{} {:?}",
                        m.kind,
                        m.name,
                        m.value,
                        m.unit.map(|u| format!(" {u}")).unwrap_or_default(),
                        m.attributes
                    );
                }
                Signal::Log(l) => {
                    println!("LOG    {:?} {} {:?}", l.severity, l.body, l.attributes);
                }
                Signal::Span(s) => {
                    println!(
                        "SPAN   {} kind={:?} status={:?} {:?}",
                        s.name, s.kind, s.status, s.attributes
                    );
                }
            }
        }
        Ok(())
    }
}
