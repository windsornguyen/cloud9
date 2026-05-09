//! I/O layer for cloud9-raft.
#![cfg_attr(test, allow(clippy::unwrap_used))]
//!
//! This crate provides the integration between the pure Raft state machine
//! (`cloud9-raft`) and the outside world. It defines traits for:
//!
//! - **Storage**: Persisting Raft state (term, vote, log, snapshots)
//! - **Transport**: Sending and receiving messages between nodes
//!
//! The crate also provides coordination modules for:
//!
//! - **Read Index** (§6.4): Linearizable reads via heartbeat confirmation
//! - **Session** (§6.3): Client session tracking for exactly-once semantics
//! - Snapshot transfer coordination (§5) - TODO
//!
//! # Design Principle
//!
//! The core `cloud9-raft` crate is a pure state machine with no I/O.
//! This crate wraps it and handles all async operations, allowing users to
//! either use the provided implementations or supply their own.

mod read_index;
mod session;
mod storage;
mod transport;

pub use read_index::{ReadId, ReadIndexCoordinator, ReadIndexError, ReadIndexResult};
pub use session::{
    ClientId, ClientSession, DuplicateCheck, SequenceNum, SessionRequest, SessionTracker,
};
pub use storage::{SnapshotData, Storage, StorageError};
pub use transport::{Transport, TransportError};
