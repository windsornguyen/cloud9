//! Top-level orchestration for Cloud9 nodes.

use cloud9_raft::ConsensusConfig;
use cloud9_storage::StorageOptions;
use tracing::{info, instrument};

/// Runtime configuration derived from CLI flags and config files.
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
    pub storage: StorageOptions,
    pub consensus: ConsensusConfig,
}

/// Launch the storage and consensus subsystems.
#[instrument(skip_all)]
pub async fn launch(config: NodeConfig) {
    info!(?config.storage, "initializing storage");
    info!(?config.consensus, "consensus subsystem ready");
}
