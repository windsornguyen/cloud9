#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(
    any(test, doctest),
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::cast_possible_truncation
    )
)]

//! Consensus drivers for Cloud9 clusters.
//!
//! This crate provides a library-style Raft implementation where the consensus
//! state machine is pure (no I/O). The caller drives the state machine and
//! handles all network and storage operations.
//!
//! # Design
//!
//! The Raft node is a deterministic state machine that:
//! - Accepts inputs: ticks, received messages, proposals
//! - Produces outputs: messages to send, entries to persist, entries to apply
//! - Does NOT perform I/O (network, disk) itself
//!
//! This design enables:
//! - Deterministic simulation testing
//! - Loom-based concurrency testing
//! - Integration with any async runtime
//! - Clean separation between consensus logic and I/O

pub mod raft;

// Re-export key Raft types at crate root for convenience
pub use raft::{
    Config as ConsensusConfig, ConfigChange, ConfigChangeError, Configuration, Members,
    MembershipMode, RaftNode,
};

use std::fmt;

use serde::{Deserialize, Serialize};

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// Index into the replicated log. Starts at 1 (index 0 is reserved).
pub type LogIndex = u64;

/// Monotonically increasing term number.
pub type Term = u64;

/// Command to be replicated and applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command(pub Vec<u8>);

/// A committed log entry ready to be applied to the state machine.
#[derive(Debug, Clone)]
pub struct CommittedEntry {
    /// Index in the log.
    pub index: LogIndex,
    /// Term when entry was proposed.
    pub term: Term,
    /// The command to apply.
    pub command: Command,
}

/// Configuration for a replica group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaConfig {
    /// All voting members of this group.
    pub voters: Vec<NodeId>,
    /// Non-voting members that receive log entries but don't vote.
    pub learners: Vec<NodeId>,
}

impl ReplicaConfig {
    /// Create a new config with the given voters.
    #[must_use]
    pub fn new(voters: Vec<NodeId>) -> Self {
        Self { voters, learners: Vec::new() }
    }

    /// Number of votes needed for quorum.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Check if this node is a voter.
    #[must_use]
    pub fn is_voter(&self, id: &NodeId) -> bool {
        self.voters.contains(id)
    }

    /// Check if this node is a learner.
    #[must_use]
    pub fn is_learner(&self, id: &NodeId) -> bool {
        self.learners.contains(id)
    }

    /// All nodes (voters + learners).
    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.voters.iter().chain(self.learners.iter())
    }
}

/// The consensus driver interface.
///
/// This trait abstracts over different consensus implementations.
/// Cloud9 ships with Raft, but the interface allows clean separation
/// between consensus logic and the rest of the database.
pub trait ConsensusDriver {
    /// Propose a command for replication.
    ///
    /// Returns the log index where this command will be placed if successful.
    /// The command is not committed until it appears in `poll_committed`.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader or cannot accept proposals.
    fn propose(&mut self, cmd: Command) -> Result<LogIndex, ProposeError>;

    /// Poll for committed entries ready to be applied.
    ///
    /// Returns entries in log order. The caller must apply them to the state
    /// machine and then call `advance_applied` to acknowledge.
    fn poll_committed(&mut self) -> Vec<CommittedEntry>;

    /// Notify that entries up to and including this index have been applied.
    fn advance_applied(&mut self, index: LogIndex);

    /// Get the current leader, if known.
    fn leader(&self) -> Option<NodeId>;

    /// Check if this node is currently the leader.
    fn is_leader(&self) -> bool;

    /// Request leadership transfer to the target node.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader or transfer fails.
    fn transfer_leadership(&mut self, target: NodeId) -> Result<(), TransferError>;
}

/// Error when proposing a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeError {
    /// This node is not the leader.
    NotLeader { leader_hint: Option<NodeId> },
    /// Too many proposals in flight.
    Throttled,
}

impl fmt::Display for ProposeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint: Some(id) } => write!(f, "not leader, try {id}"),
            Self::NotLeader { leader_hint: None } => write!(f, "not leader, leader unknown"),
            Self::Throttled => write!(f, "too many proposals in flight"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// Error when requesting a linearizable read index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadIndexError {
    /// This node is not the leader.
    NotLeader { leader_hint: Option<NodeId> },
    /// The leader has not committed an entry from its current term yet.
    CurrentTermNotCommitted,
    /// A previous read-index quorum round is still pending.
    ReadInProgress { read_index: LogIndex },
}

impl fmt::Display for ReadIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { leader_hint: Some(id) } => write!(f, "not leader, try {id}"),
            Self::NotLeader { leader_hint: None } => write!(f, "not leader, leader unknown"),
            Self::CurrentTermNotCommitted => {
                write!(f, "leader has not committed an entry from its current term")
            }
            Self::ReadInProgress { read_index } => {
                write!(f, "read-index quorum round pending at index {read_index}")
            }
        }
    }
}

impl std::error::Error for ReadIndexError {}

/// Error when transferring leadership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// This node is not the leader.
    NotLeader,
    /// Target node is not a voter in the configuration.
    TargetNotVoter,
    /// Target node's log is behind; wait for it to catch up.
    TargetLagging {
        /// Target's current match index.
        match_index: LogIndex,
        /// Leader's last log index.
        last_index: LogIndex,
    },
    /// Cannot transfer to self.
    TargetIsSelf,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader => write!(f, "not leader"),
            Self::TargetNotVoter => write!(f, "target is not a voter"),
            Self::TargetLagging { match_index, last_index } => {
                write!(f, "target lagging: {match_index}/{last_index}")
            }
            Self::TargetIsSelf => write!(f, "cannot transfer to self"),
        }
    }
}

impl std::error::Error for TransferError {}
