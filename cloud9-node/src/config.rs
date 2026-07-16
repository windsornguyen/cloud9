//! Node runtime configuration.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use cloud9_raft::{ConsensusConfig, NodeId};
use cloud9_storage::StorageOptions;

/// Runtime configuration derived from CLI flags and config files.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub storage: StorageOptions,
    pub consensus: ConsensusConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        let node_id = NodeId(0);
        let raft_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 19_091));
        Self {
            node_id,
            client_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 19_090)),
            raft_addr,
            peers: BTreeMap::from([(node_id, raft_addr)]),
            storage: StorageOptions::default(),
            consensus: raft_config(node_id),
        }
    }
}

impl NodeConfig {
    #[must_use]
    pub(crate) fn raft_dir(&self) -> PathBuf {
        Path::new(self.storage.data_dir.as_str()).join("raft")
    }
}

#[must_use]
pub fn raft_config(node_id: NodeId) -> ConsensusConfig {
    ConsensusConfig::new(node_id).with_parallel_disk_write(false)
}
