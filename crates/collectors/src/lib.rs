//! `harnesssphere-collectors` — driven adapters de fonte (pull).
//!
//! Critical (sempre compilado): `host`, `self`. Optional (feature-gated): container,
//! prometheus, etc. Adicionar um coletor = novo módulo implementando `SignalSource` +
//! 1 linha no composition root. O core não muda.

mod host;
mod self_watcher;

pub use host::HostCollector;
pub use self_watcher::SelfCollector;
