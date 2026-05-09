//! Core shared state for all Raft roles.
//!
//! The Core holds the "mathematical" Raft state that all invariants
//! are predicates over: term, `voted_for`, log, `commit_index`, config.
//!
//! Safety properties (term monotonicity, log matching, leader completeness,
//! state machine safety) are all about Core, independent of role.

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

use super::log::Log;
use super::membership::{Configuration, MembershipMode};

/// Static configuration for a Raft node.
///
/// # Timing Requirements (§3.9)
///
/// Raft requires: `broadcastTime ≪ electionTimeout ≪ MTBF`
///
/// Where:
/// - `broadcastTime`: RTT + disk persist (~0.5–20ms per §3.9)
/// - `electionTimeout`: when followers start elections
/// - `MTBF`: mean time between server failures (months)
///
/// The dissertation recommends (Chapter 9):
/// - Election timeout: 150–300ms ("conservative election timeout")
/// - Heartbeat interval: ½ × minimum election timeout
/// - Election randomness: ≥50ms range to avoid split votes
///
/// Default values assume 1 tick = 1ms. Adjust if your tick rate differs.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node's identity.
    pub id: NodeId,
    /// Election timeout range in ticks: [min, max).
    ///
    /// Per §3.9: "likely somewhere between 10–500ms"
    /// Per Chapter 9: "We recommend using a conservative election timeout
    /// such as 150–300ms"
    pub election_timeout: (u64, u64),
    /// Heartbeat interval in ticks.
    ///
    /// Per Chapter 9 benchmark: "heartbeat interval... was half of the
    /// minimum election timeout"
    pub heartbeat_interval: u64,
    /// Max entries per `AppendEntries` message.
    pub max_entries_per_msg: u64,
    /// How to handle membership changes.
    pub membership_mode: MembershipMode,
    /// Enable `PreVote` protocol (§4.2.3).
    ///
    /// When enabled, nodes send a "pre-vote" request before starting a real
    /// election. This prevents partitioned nodes from incrementing their term
    /// and disrupting the cluster when they rejoin.
    pub prevote: bool,
    /// Enable parallel disk write optimization (§10.2.1).
    ///
    /// When enabled, the leader can send `AppendEntries` to followers while its
    /// own disk write is in progress. The IO layer should signal completion
    /// via `Event::DiskWriteComplete`. This removes a disk write from the
    /// critical path, reducing latency.
    ///
    /// Safety: An entry can be committed before the leader writes it to disk,
    /// as long as a majority of followers have written it.
    pub parallel_disk_write: bool,
    /// Enable pipelining optimization (§10.2.2).
    ///
    /// When enabled, the leader updates `next_index` optimistically after
    /// sending `AppendEntries`, rather than waiting for acknowledgment. This
    /// allows multiple in-flight requests per follower, improving throughput.
    ///
    /// On timeout or rejection, `next_index` is reverted to `match_index + 1`.
    pub pipelining: bool,
}

impl Config {
    /// Create a new config with dissertation-recommended defaults.
    ///
    /// Assumes 1 tick = 1ms. Values from Ongaro's dissertation:
    /// - Election timeout: 150–300ms (Chapter 9)
    /// - Heartbeat interval: 75ms (half of min election timeout, Chapter 9)
    /// - `PreVote`: enabled by default (§4.2.3)
    /// - Parallel disk write: enabled by default (§10.2.1)
    /// - Pipelining: enabled by default (§10.2.2)
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_timeout: (150, 300),
            heartbeat_interval: 75,
            max_entries_per_msg: 100,
            membership_mode: MembershipMode::default(),
            prevote: true,
            parallel_disk_write: true,
            pipelining: true,
        }
    }

    /// Set the membership mode.
    #[must_use]
    pub fn with_membership_mode(mut self, mode: MembershipMode) -> Self {
        self.membership_mode = mode;
        self
    }

    /// Set election timeout range.
    #[must_use]
    pub fn with_election_timeout(mut self, min: u64, max: u64) -> Self {
        self.election_timeout = (min, max);
        self
    }

    /// Set heartbeat interval.
    #[must_use]
    pub fn with_heartbeat_interval(mut self, interval: u64) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Enable or disable `PreVote` (§4.2.3).
    #[must_use]
    pub fn with_prevote(mut self, enabled: bool) -> Self {
        self.prevote = enabled;
        self
    }

    /// Enable or disable parallel disk write optimization (§10.2.1).
    #[must_use]
    pub fn with_parallel_disk_write(mut self, enabled: bool) -> Self {
        self.parallel_disk_write = enabled;
        self
    }

    /// Enable or disable pipelining optimization (§10.2.2).
    #[must_use]
    pub fn with_pipelining(mut self, enabled: bool) -> Self {
        self.pipelining = enabled;
        self
    }
}

impl Default for Config {
    /// Default config with placeholder NodeId(0).
    /// Should be properly initialized before use.
    fn default() -> Self {
        Self::new(NodeId(0))
    }
}

/// State that must be persisted to stable storage before responding to RPCs.
///
/// From §3.8: "currentTerm, votedFor, and log[] must be persisted."
/// Additionally, we persist the bootstrap configuration for recovery.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Persistent {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Log,
    /// The initial cluster configuration before any log entries.
    /// Used when recovering and no config entry exists in log.
    pub bootstrap_config: Configuration,
}

/// The shared core state for all Raft roles.
///
/// This struct holds everything that:
/// 1. Must be persisted (term, `voted_for`, log, `bootstrap_config`)
/// 2. Is shared across all roles (`commit_index`, config)
/// 3. Tracks logical time (ticks, rng)
///
/// Role-specific state (`leader_id`, votes, `next_index`, `match_index`)
/// lives in the role structs.
pub struct Core {
    // Identity and configuration
    pub config: Config,

    // Persistent state (§3.8)
    pub persistent: Persistent,

    // Volatile state - all servers
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    // Logical time
    pub ticks: u64,
    rng: u64,
}

impl Core {
    /// Create a new Core with the given configuration.
    ///
    /// The `voters` slice is the initial cluster membership (bootstrap config).
    pub fn new(config: Config, voters: &[NodeId]) -> Self {
        let rng = config.id.0.wrapping_mul(0x0005_DEEC_E66D).wrapping_add(0xB);
        let bootstrap = Configuration::simple(voters.to_vec());
        let persistent = Persistent { bootstrap_config: bootstrap, ..Persistent::default() };
        Self { config, persistent, commit_index: 0, last_applied: 0, ticks: 0, rng }
    }

    /// Restore from persisted state.
    pub fn restore(config: Config, persistent: Persistent) -> Self {
        let rng = config.id.0.wrapping_mul(0x0005_DEEC_E66D).wrapping_add(0xB);
        Self { config, persistent, commit_index: 0, last_applied: 0, ticks: 0, rng }
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    #[inline]
    pub fn term(&self) -> Term {
        self.persistent.term
    }

    #[inline]
    pub fn log(&self) -> &Log {
        &self.persistent.log
    }

    #[inline]
    pub fn log_mut(&mut self) -> &mut Log {
        &mut self.persistent.log
    }

    /// Update term if the given term is higher. Returns true if term changed.
    ///
    /// Invariant: term is monotonically increasing.
    pub fn maybe_update_term(&mut self, term: Term) -> bool {
        if term > self.persistent.term {
            self.persistent.term = term;
            self.persistent.voted_for = None;
            true
        } else {
            false
        }
    }

    /// Increment term and vote for self. Used when becoming candidate.
    pub fn start_election(&mut self) {
        let can_vote_for_self = self.is_voter();
        self.persistent.term += 1;
        self.persistent.voted_for = can_vote_for_self.then_some(self.config.id);
    }

    /// Record a vote for a candidate.
    ///
    /// This is idempotent: voting for the same candidate twice is allowed.
    pub fn vote_for(&mut self, candidate: NodeId) {
        debug_assert!(
            self.persistent.voted_for.is_none() || self.persistent.voted_for == Some(candidate),
            "Attempted to vote for {candidate:?} but already voted for {:?}",
            self.persistent.voted_for
        );
        self.persistent.voted_for = Some(candidate);
    }

    /// Check if we can vote for a candidate (haven't voted or voted for them).
    pub fn can_vote_for(&self, candidate: NodeId) -> bool {
        self.persistent.voted_for.is_none() || self.persistent.voted_for == Some(candidate)
    }

    /// Check if candidate's log is at least as up-to-date as ours.
    ///
    /// "Raft determines which of two logs is more up-to-date by comparing
    /// the index and term of the last entries in the logs."
    pub fn log_is_up_to_date(&self, last_term: Term, last_index: LogIndex) -> bool {
        let our_term = self.persistent.log.last_term();
        let our_index = self.persistent.log.last_index();
        (last_term, last_index) >= (our_term, our_index)
    }

    /// Generate a random election timeout in [min, max).
    pub fn random_election_timeout(&mut self) -> u64 {
        let (min, max) = self.config.election_timeout;
        min + self.rand() % (max - min)
    }

    fn rand(&mut self) -> u64 {
        // xorshift64
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    /// Get the effective configuration.
    ///
    /// This is the latest configuration from the log, or the bootstrap config
    /// if no config entries exist. Per §4, config takes effect when appended.
    pub fn effective_config(&self) -> Configuration {
        self.config_at(self.persistent.log.last_index())
    }

    /// Get the effective configuration at a specific log index.
    pub fn config_at(&self, index: LogIndex) -> Configuration {
        self.persistent
            .log
            .config_at(index)
            .cloned()
            .unwrap_or_else(|| self.persistent.bootstrap_config.clone())
    }

    /// Check if there's a pending (uncommitted) configuration change.
    pub fn has_pending_config(&self) -> bool {
        self.persistent.log.pending_config_index(self.commit_index).is_some()
    }

    /// Get the index of the pending config change, if any.
    pub fn pending_config_index(&self) -> Option<LogIndex> {
        self.persistent.log.pending_config_index(self.commit_index)
    }

    /// Check if this node is in the effective configuration.
    pub fn is_voter(&self) -> bool {
        self.effective_config().is_voter(self.config.id)
    }
}

#[cfg(test)]
mod tests {
    use super::super::log::EntryPayload;
    use super::*;
    use crate::Command;

    fn test_core() -> Core {
        let config = Config::new(NodeId(0));
        Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)])
    }

    #[test]
    fn config_basics() {
        let config = Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(config.voter_count(), 3);
        let (q, _) = config.quorum_sizes();
        assert_eq!(q, 2);
        assert!(config.is_voter(NodeId(1)));
        assert!(!config.is_member(NodeId(99)));
    }

    #[test]
    fn config_peers() {
        let config = Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]);
        let peers: Vec<_> = config.voter_peers(NodeId(1)).collect();
        assert_eq!(peers, vec![NodeId(0), NodeId(2)]);
    }

    #[test]
    fn term_monotonicity() {
        let mut core = test_core();
        assert_eq!(core.term(), 0);

        assert!(core.maybe_update_term(5));
        assert_eq!(core.term(), 5);

        // Can't go backwards
        assert!(!core.maybe_update_term(3));
        assert_eq!(core.term(), 5);

        // Same term is no-op
        assert!(!core.maybe_update_term(5));
    }

    #[test]
    fn voting() {
        let mut core = test_core();
        core.persistent.term = 1;

        assert!(core.can_vote_for(NodeId(1)));
        core.vote_for(NodeId(1));
        assert!(!core.can_vote_for(NodeId(2)));
        assert!(core.can_vote_for(NodeId(1))); // Can vote for same candidate again
    }

    #[test]
    fn term_update_clears_vote() {
        let mut core = test_core();
        core.persistent.term = 1;
        core.vote_for(NodeId(1));

        core.maybe_update_term(2);
        assert!(core.can_vote_for(NodeId(2))); // Vote cleared
    }

    #[test]
    fn log_up_to_date() {
        use super::super::log::Entry;

        let mut core = test_core();

        // Empty log: any non-empty log is more up-to-date
        assert!(core.log_is_up_to_date(0, 0));
        assert!(core.log_is_up_to_date(1, 1));

        // Add an entry at term 2
        core.log_mut().append(Entry {
            term: 2,
            index: 1,
            payload: EntryPayload::Command(Command(vec![])),
        });

        // Same term, same index: up to date
        assert!(core.log_is_up_to_date(2, 1));
        // Higher term: up to date
        assert!(core.log_is_up_to_date(3, 0));
        // Same term, longer: up to date
        assert!(core.log_is_up_to_date(2, 5));
        // Lower term: not up to date
        assert!(!core.log_is_up_to_date(1, 100));
    }
}
