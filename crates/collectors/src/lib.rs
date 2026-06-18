//! `harnesssphere-collectors` — driven source adapters (pull).
//!
//! Critical (always compiled): `host`, `self`. Optional (feature-gated): container,
//! prometheus, etc. Adding a collector = a new module implementing `SignalSource` +
//! 1 line in the composition root. The core does not change.

mod container;
mod host;
mod probe;
mod process;
mod self_watcher;
mod session;

pub use container::ContainerCollector;
pub use host::HostCollector;
pub use probe::EndpointProbeCollector;
pub use process::ProcessCollector;
pub use self_watcher::SelfCollector;
pub use session::SessionCollector;
