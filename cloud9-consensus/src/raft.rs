//! Raft consensus implementation for Cloud9.

use serde::{Deserialize, Serialize};

/// Index into the log.
pub type LogIndex = u64;

/// Monotonic term number.
pub type Term = u64;

/// Identifier for a Raft peer.
pub type NodeId = String;

/// Persistent state on all servers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term server has seen.
    pub current_term: Term,
    /// Candidate that received vote in current term.
    pub voted_for: Option<NodeId>,
    /// Log entries; each entry contains command for state machine and term when entry was received by leader.
    pub log: Vec<LogEntry>,
}

/// Single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term when entry was received by leader.
    pub term: Term,
    /// Command for state machine.
    pub command: Vec<u8>,
}

/// Volatile state on all servers.
#[derive(Debug, Clone, Default)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of highest log entry applied to state machine.
    pub last_applied: LogIndex,
}

/// Volatile state on leaders.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each server, index of the next log entry to send to that server.
    pub next_index: Vec<LogIndex>,
    /// For each server, index of highest log entry known to be replicated on server.
    pub match_index: Vec<LogIndex>,
}

/// Arguments for RequestVote RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Candidate's term.
    pub term: Term,
    /// Candidate requesting vote.
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry.
    pub last_log_term: Term,
}

/// Response for RequestVote RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: Term,
    /// True if the follower granted its vote to the candidate.
    pub vote_granted: bool,
}

/// Arguments for AppendEntries RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: Term,
    /// Leader ID, so follower can redirect clients.
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: LogIndex,
    /// Term of prev_log_index entry.
    pub prev_log_term: Term,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

/// Response for AppendEntries RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: Term,
    /// True if follower contained entry matching prev_log_index and prev_log_term.
    pub success: bool,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self { current_term: 0, voted_for: None, log: Vec::new() }
    }
}

impl LeaderState {
    /// Create new leader state for the given number of servers.
    pub fn new(num_servers: usize, next_index: LogIndex) -> Self {
        Self { next_index: vec![next_index; num_servers], match_index: vec![0; num_servers] }
    }
}
