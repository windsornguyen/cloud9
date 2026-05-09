//! Storage trait for persisting Raft state.
//!
//! Per §3.8: "Raft servers must persist the following to stable storage
//! before responding to RPCs: currentTerm, votedFor, and log[]."
//!
//! Additionally, snapshot metadata must be persisted for log compaction (§5).

use std::future::Future;

use cloud9_raft::raft::Persistent;
use cloud9_raft::{LogIndex, Term};
use thiserror::Error;

/// Errors from storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Corruption detected: {0}")]
    Corruption(String),
}

/// Storage trait for persisting Raft state (§3.8).
///
/// Implementations must ensure durability before returning from write operations.
/// The `sync` parameter indicates whether to force sync to disk (fsync).
///
/// # Thread Safety
///
/// Implementations must be safe to use from async contexts. The trait uses
/// `Send + Sync` bounds to allow sharing across tasks.
pub trait Storage: Send + Sync {
    /// Load persisted Raft state on startup.
    ///
    /// Returns `None` if no state has been persisted (fresh start).
    fn load(&self) -> impl Future<Output = Result<Option<Persistent>, StorageError>> + Send;

    /// Save Raft state to stable storage.
    ///
    /// Per §3.8, this must complete before responding to any RPC.
    /// If `sync` is true, the implementation should fsync to ensure durability.
    fn save(
        &self,
        state: &Persistent,
        sync: bool,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Load snapshot data for transfer to a slow follower (§5).
    ///
    /// Returns the snapshot data as bytes. The format is opaque to Raft —
    /// the state machine defines it.
    fn load_snapshot(
        &self,
    ) -> impl Future<Output = Result<Option<SnapshotData>, StorageError>> + Send;

    /// Save snapshot data received from the leader (§5).
    ///
    /// Called when a follower receives `InstallSnapshot`. The implementation
    /// should atomically replace any existing snapshot.
    fn save_snapshot(
        &self,
        snapshot: SnapshotData,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}

/// Snapshot data for transfer between nodes (§5).
#[derive(Debug, Clone)]
pub struct SnapshotData {
    /// Index of the last entry included in the snapshot.
    pub last_included_index: LogIndex,
    /// Term of the last entry included in the snapshot.
    pub last_included_term: Term,
    /// The serialized state machine state.
    pub data: bytes::Bytes,
}

#[cfg(test)]
mod tests {
    // Tests will use in-memory implementation
}
