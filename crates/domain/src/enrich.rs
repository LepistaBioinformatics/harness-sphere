//! Enricher — pure domain transform applied to ingested signals.
//!
//! Stamps incoming push telemetry with host context so an AI span can be correlated
//! with the resource pressure of the very host it ran on. Pure and testable.
//! (Convention normalization — e.g. OpenInference `llm.token_count.*` → `gen_ai.*` — is
//! a planned extension.)

use crate::signal::{Attributes, Signal};

pub struct Enricher {
    /// Attributes injected into every ingested signal (e.g. `host.name`).
    attrs: Attributes,
}

impl Enricher {
    pub fn new(host_name: impl Into<String>) -> Self {
        Enricher {
            attrs: vec![("host.name".to_owned(), host_name.into().into())],
        }
    }

    pub fn enrich(&self, signal: &mut Signal) {
        for (k, v) in &self.attrs {
            // Don't duplicate a key the origin already carries (e.g. host.name from the
            // pushed Resource — the harness is co-located, so it's the same host).
            if !signal.attributes().iter().any(|(ek, _)| ek == k) {
                signal.push_attr(k.clone(), v.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Metric, MetricKind};

    #[test]
    fn injects_host_name() {
        let enr = Enricher::new("host-42");
        let mut sig = Metric::now("gen_ai.client.token.usage", MetricKind::Histogram, 1.0).into_signal();
        enr.enrich(&mut sig);
        if let Signal::Metric(m) = sig {
            assert!(m
                .attributes
                .iter()
                .any(|(k, _)| k == "host.name"));
        } else {
            panic!("expected metric");
        }
    }
}
