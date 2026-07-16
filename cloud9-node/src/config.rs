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
    pub node_id: NodeId,
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub raft_key: RaftKey,
    pub storage: StorageOptions,
    pub consensus: ConsensusConfig,
}

impl NodeConfig {
    #[must_use]
    pub(crate) fn raft_dir(&self) -> PathBuf {
        Path::new(self.storage.data_dir.as_str()).join("raft")
    }
}

#[must_use]
pub fn raft_config(node_id: NodeId) -> ConsensusConfig {
    let mut config = ConsensusConfig::new(node_id).with_parallel_disk_write(false);
    config.max_entries_per_msg = 1;
    config
}
