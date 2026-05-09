//! Events and effects for the Raft automaton.
//!
//! The Raft state machine is driven by events and produces effects:
//! `step : (State, Event) → (State', Effects)`
//!
//! Events are inputs from the environment (messages, timeouts).
//! Effects are outputs to the environment (send messages, persist, commit).

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

use super::log::Entry;
use super::membership::Configuration;

/// An input event to the Raft automaton.
#[derive(Debug, Clone)]
pub enum Event {
    /// A tick of logical time. Caller should inject this when deadline expires.
    Tick,
    /// A message received from another node.
    Message(Message),
    /// Disk write completed up to this index (§10.2.1 parallel disk write).
    ///
    /// When `Config::parallel_disk_write` is enabled, the IO layer should:
    /// 1. Persist entries and send messages in parallel
    /// 2. After disk write completes, inject this event with the last written index
    ///
    /// This updates the leader's own `match_index`, potentially allowing commit.
    DiskWriteComplete(LogIndex),
}

/// A message from one node to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub payload: Payload,
}

/// Message payload variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    /// `PreVote` RPC (§4.2.3) - checks if election would succeed without incrementing term.
    PreVoteRequest(PreVoteRequest),
    /// `PreVote` response
    PreVoteResponse(PreVoteResponse),
    /// `RequestVote` RPC (§3.4)
    VoteRequest(VoteRequest),
    /// `RequestVote` response
    VoteResponse(VoteResponse),
    /// `AppendEntries` RPC (§3.5)
    AppendRequest(AppendRequest),
    /// `AppendEntries` response
    AppendResponse(AppendResponse),
    /// `InstallSnapshot` RPC (§5) - sent when follower is too far behind.
    InstallSnapshotRequest(InstallSnapshotRequest),
    /// `InstallSnapshot` response
    InstallSnapshotResponse(InstallSnapshotResponse),
    /// Read-index heartbeat request (§6.4).
    ReadIndexRequest(ReadIndexRequest),
    /// Read-index heartbeat response (§6.4).
    ReadIndexResponse(ReadIndexResponse),
    /// Leadership transfer: target should start election immediately (§3.10)
    TimeoutNow,
}

/// `PreVote` request (§4.2.3).
///
/// Like `VoteRequest` but doesn't increment the sender's term. Used to check
/// if an election would succeed before disrupting the cluster.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PreVoteRequest {
    /// The term the candidate would use if elected.
    pub next_term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PreVoteResponse {
    /// The responder's current term.
    pub term: Term,
    /// Whether the pre-vote was granted.
    pub granted: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VoteRequest {
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VoteResponse {
    pub granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendRequest {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AppendResponse {
    pub success: bool,
    pub last_log_index: LogIndex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReadIndexRequest {
    pub id: u64,
    pub read_index: LogIndex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReadIndexResponse {
    pub id: u64,
    pub read_index: LogIndex,
}

/// `InstallSnapshot` request (§5, Figure 5.3).
///
/// Sent by the leader when a follower is too far behind and needs the snapshot
/// rather than log entries (§5: "if a follower's log is so far behind the
/// leader's that the leader has discarded the next entry it needs to send").
///
/// This message carries metadata only — actual snapshot data transfer is
/// handled by the I/O layer. The full RPC in Figure 5.3 includes `offset`,
/// `data[]`, and `done` fields for chunked transfer; we delegate that to I/O.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Index of the last entry included in the snapshot.
    /// Corresponds to `lastIncludedIndex` in Figure 5.3.
    pub last_included_index: LogIndex,
    /// Term of the last entry included in the snapshot.
    /// Corresponds to `lastIncludedTerm` in Figure 5.3.
    pub last_included_term: Term,
    /// Latest configuration as of `last_included_index`.
    pub configuration: Configuration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// True if the snapshot was accepted.
    /// Per §5, follower should accept if snapshot is more recent than its log.
    pub success: bool,
    /// Index of the last entry included in the accepted snapshot.
    pub last_included_index: LogIndex,
}

/// Effects produced by a step of the automaton.
///
/// The caller is responsible for:
/// 1. Persisting state if `persist` is true (before sending messages)
/// 2. Sending messages to the network
/// 3. Applying committed entries to the state machine
/// 4. Handling snapshot requests
#[derive(Debug, Default)]
pub struct Effects {
    /// Whether persistent state changed and must be written to disk.
    pub persist: bool,
    /// Messages to send to other nodes.
    pub messages: Vec<Message>,
    /// Snapshot to send to a follower (leader only).
    /// The I/O layer should transfer the snapshot data and then send
    /// an `InstallSnapshotRequest` to the follower.
    pub send_snapshots: Vec<SendSnapshot>,
}

/// Request to send a snapshot to a slow follower.
#[derive(Debug, Clone)]
pub struct SendSnapshot {
    /// The follower that needs the snapshot.
    pub to: NodeId,
    /// Index of the last entry included in the snapshot.
    pub last_included_index: LogIndex,
    /// Term of the last entry included in the snapshot.
    pub last_included_term: Term,
    /// Latest configuration as of `last_included_index`.
    pub configuration: Configuration,
}

impl Effects {
    pub fn none() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_persist(mut self) -> Self {
        self.persist = true;
        self
    }

    #[must_use]
    pub fn with_message(mut self, msg: Message) -> Self {
        self.messages.push(msg);
        self
    }

    #[must_use]
    pub fn with_messages(mut self, msgs: impl IntoIterator<Item = Message>) -> Self {
        self.messages.extend(msgs);
        self
    }

    #[must_use]
    pub fn with_send_snapshot(mut self, snapshot: SendSnapshot) -> Self {
        self.send_snapshots.push(snapshot);
        self
    }

    #[must_use]
    pub fn merge(mut self, other: Effects) -> Self {
        self.persist |= other.persist;
        self.messages.extend(other.messages);
        self.send_snapshots.extend(other.send_snapshots);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effects_builder() {
        let effects = Effects::none().with_persist().with_message(Message {
            from: NodeId(0),
            to: NodeId(1),
            term: 1,
            payload: Payload::TimeoutNow,
        });

        assert!(effects.persist);
        assert_eq!(effects.messages.len(), 1);
    }

    #[test]
    fn effects_merge() {
        let e1 = Effects::none().with_persist();
        let e2 = Effects::none().with_message(Message {
            from: NodeId(0),
            to: NodeId(1),
            term: 1,
            payload: Payload::TimeoutNow,
        });

        let merged = e1.merge(e2);
        assert!(merged.persist);
        assert_eq!(merged.messages.len(), 1);
    }
}
