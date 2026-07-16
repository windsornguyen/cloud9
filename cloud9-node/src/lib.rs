#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

//! Top-level orchestration for Cloud9 nodes.

mod auth;
mod command;
mod config;
mod runtime;
mod service;
mod store;
#[cfg(test)]
mod tests;
mod transport;

pub use auth::RaftKey;
pub use config::{NodeConfig, raft_config};

/// Launch the node's public KV API and Raft peer API.
pub async fn launch(config: NodeConfig) -> anyhow::Result<()> {
    service::launch(config).await
}
