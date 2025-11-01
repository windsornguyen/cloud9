//! Consensus drivers used by Cloud9 clusters.

use serde::{Deserialize, Serialize};

pub mod raft;

/// Configuration for a specific consensus implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Name of the driver, e.g. `raft` or `flexible-paxos`.
    pub driver: String,
    /// Maximum number of concurrent proposals.
    pub max_inflight: usize,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self { driver: "raft".to_string(), max_inflight: 128 }
    }
}
