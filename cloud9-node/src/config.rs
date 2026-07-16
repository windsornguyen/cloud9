//! Node runtime configuration.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use cloud9_raft::{ConsensusConfig, NodeId};
use cloud9_storage::StorageOptions;

use crate::RaftKey;

/// Runtime configuration derived from CLI flags and config files.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Stable identity used by the Raft group.
    pub node_id: NodeId,
    /// Address for the public database API.
    pub client_addr: SocketAddr,
    /// Address for authenticated Raft peer traffic.
    pub raft_addr: SocketAddr,
    /// Complete mapping from Raft node IDs to peer addresses.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    /// Shared key used to authenticate peer messages.
    pub raft_key: RaftKey,
    /// Durable storage configuration.
    pub storage: StorageOptions,
    /// Raft state-machine configuration.
    pub consensus: ConsensusConfig,
}

impl NodeConfig {
    #[must_use]
    pub(crate) fn raft_dir(&self) -> PathBuf {
        Path::new(self.storage.data_dir.as_str()).join("raft")
    }
}

#[must_use]
/// Build the Raft configuration required by the current node runtime.
pub fn raft_config(node_id: NodeId) -> ConsensusConfig {
    let mut config = ConsensusConfig::new(node_id).with_parallel_disk_write(false);
    config.max_entries_per_msg = 1;
    config
}
