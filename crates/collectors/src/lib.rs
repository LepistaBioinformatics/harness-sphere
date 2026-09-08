//! `harnesssphere-collectors` — driven source adapters (pull).
//!
//! Critical (always compiled): `host`, `self`. Optional (feature-gated): container.
//! Adding a collector = a new module implementing `SignalSource` +
//! 1 line in the composition root. The core does not change.

mod container;
mod host;
mod learning;
mod probe;
mod process;
mod self_watcher;
mod session;
mod workspace;

pub use container::ContainerCollector;
pub use host::HostCollector;
pub use learning::LearningCollector;
pub use probe::{EndpointProbeCollector, ProbeTarget};
pub use process::ProcessCollector;
pub use self_watcher::SelfCollector;
pub use session::SessionCollector;
pub use workspace::{discover, Workspace};
