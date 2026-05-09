//! Raft consensus implementation.
//!
//! A library-style Raft: pure state machine, caller handles I/O.
//! Based on Diego Ongaro's dissertation.
//!
//! # Architecture
//!
//! The implementation follows the "deterministic event-driven automaton" model:
//!
//! ```text
//! step : (State, Event) → (State', Effects)
//! ```
//!
//! Where:
//! - `State = (Core, RoleState)` - shared state + role-specific state
//! - `Event` - tick or message
//! - `Effects` - messages to send, persistence required
//!
//! The state machine is factored into three role-specific sub-automata:
//! - `Follower` - responds to RPCs, times out to Candidate
//! - `Candidate` - runs elections, wins to Leader
//! - `Leader` - replicates log, sends heartbeats
//!
//! All roles share `Core` (term, `voted_for`, log, `commit_index`, config).
//!
//! # Timing (§3.9, Chapter 9)
//!
//! The library tracks logical ticks; the caller maps ticks to real time.
//! Default configuration assumes **1 tick = 1 millisecond**.
//!
//! From the dissertation:
//! - **Timing requirement (§3.9):** `broadcastTime ≪ electionTimeout ≪ MTBF`
//! - **Election timeout (Chapter 9):** 150–300ms recommended ("conservative")
//! - **Heartbeat interval (Chapter 9):** half of minimum election timeout
//!
//! # Usage
//!
//! ```ignore
//! // Default config uses dissertation-recommended values:
//! // - Election timeout: 150-300ms
//! // - Heartbeat interval: 75ms
//! // - PreVote: enabled (§4.2.3)
//! let config = Config::new(NodeId(0));
//! let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
//!
//! // 1 tick = 1ms (matches default config assumptions)
//! const TICK_MS: u64 = 1;
//!
//! loop {
//!     // Sleep until next deadline (efficient, no spinning)
//!     let ticks = node.ticks_until_deadline();
//!     sleep(Duration::from_millis(ticks * TICK_MS));
//!
//!     // Process tick
//!     let effects = node.tick();
//!     handle_effects(effects);
//!
//!     // Process received messages
//!     for msg in network.recv() {
//!         let effects = node.step(msg);
//!         handle_effects(effects);
//!     }
//!
//!     // Apply committed entries
//!     for entry in node.committed() {
//!         state_machine.apply(entry);
//!     }
//! }
//! ```

mod candidate;
mod core;
mod event;
mod follower;
mod leader;
mod log;
mod membership;
mod precandidate;

pub use candidate::Candidate;
pub use core::{Config, Core, Persistent};
pub use event::{
    AppendRequest, AppendResponse, Effects, Event, InstallSnapshotRequest, InstallSnapshotResponse,
    Message, Payload, PreVoteRequest, PreVoteResponse, ReadIndexRequest, ReadIndexResponse,
    SendSnapshot, VoteRequest, VoteResponse,
};
pub use follower::Follower;
pub use leader::Leader;
pub use log::{Entry, EntryPayload, Log};
pub use membership::{ConfigChange, ConfigChangeError, Configuration, Members, MembershipMode};
pub use precandidate::PreCandidate;

use crate::{
    Command, CommittedEntry, LogIndex, NodeId, ProposeError, ReadIndexError, TransferError,
};

/// Role transition result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Stay,
    ToFollower(Option<NodeId>),
    ToFollowerAfterContact(Option<NodeId>),
    ToPreCandidate,
    ToCandidate,
    ToLeader,
}

/// Result of a role state machine step.
#[derive(Debug)]
pub struct StepResult {
    pub transition: Transition,
    pub effects: Effects,
}

impl StepResult {
    pub fn none() -> Self {
        Self::stay(Effects::none())
    }

    pub fn stay(effects: Effects) -> Self {
        Self { transition: Transition::Stay, effects }
    }

    pub fn to_follower(leader: Option<NodeId>, effects: Effects) -> Self {
        Self { transition: Transition::ToFollower(leader), effects }
    }

    pub fn to_follower_after_contact(leader: Option<NodeId>, effects: Effects) -> Self {
        Self { transition: Transition::ToFollowerAfterContact(leader), effects }
    }

    pub fn to_precandidate(effects: Effects) -> Self {
        Self { transition: Transition::ToPreCandidate, effects }
    }

    pub fn to_candidate(effects: Effects) -> Self {
        Self { transition: Transition::ToCandidate, effects }
    }

    pub fn to_leader(effects: Effects) -> Self {
        Self { transition: Transition::ToLeader, effects }
    }
}

/// The current role and its associated state.
#[derive(Debug)]
pub enum RoleState {
    Follower(Follower),
    PreCandidate(PreCandidate),
    Candidate(Candidate),
    Leader(Leader),
}

impl RoleState {
    pub fn is_leader(&self) -> bool {
        matches!(self, RoleState::Leader(_))
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self, RoleState::Candidate(_))
    }

    pub fn is_precandidate(&self) -> bool {
        matches!(self, RoleState::PreCandidate(_))
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, RoleState::Follower(_))
    }
}

/// A Raft consensus node.
///
/// This is the main entry point. It orchestrates the role-specific automata
/// and provides a clean API for the caller.
pub struct RaftNode {
    core: Core,
    role: RoleState,
}

impl RaftNode {
    /// Create a new Raft node starting as a follower.
    ///
    /// `voters` is the initial cluster membership (bootstrap configuration).
    pub fn new(config: Config, voters: &[NodeId]) -> Self {
        let mut core = Core::new(config, voters);
        let follower = Follower::new(&mut core, None);
        Self { core, role: RoleState::Follower(follower) }
    }

    /// Restore a node from persisted state.
    pub fn restore(config: Config, persistent: Persistent) -> Self {
        let mut core = Core::restore(config, persistent);
        let follower = Follower::new(&mut core, None);
        Self { core, role: RoleState::Follower(follower) }
    }

    /// This node's ID.
    #[inline]
    pub fn id(&self) -> NodeId {
        self.core.id()
    }

    /// Current term.
    #[inline]
    pub fn term(&self) -> u64 {
        self.core.term()
    }

    /// Whether this node is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.role.is_leader()
    }

    /// Whether this node is a follower.
    #[inline]
    pub fn is_follower(&self) -> bool {
        self.role.is_follower()
    }

    /// Whether this node is a candidate.
    #[inline]
    pub fn is_candidate(&self) -> bool {
        self.role.is_candidate()
    }

    /// Whether this node is a pre-candidate (`PreVote` phase).
    #[inline]
    pub fn is_precandidate(&self) -> bool {
        self.role.is_precandidate()
    }

    /// Current leader, if known.
    pub fn leader(&self) -> Option<NodeId> {
        match &self.role {
            RoleState::Leader(_) => Some(self.core.id()),
            RoleState::Follower(f) => f.leader,
            RoleState::PreCandidate(_) | RoleState::Candidate(_) => None,
        }
    }

    /// Current commit index.
    #[inline]
    pub fn commit_index(&self) -> LogIndex {
        self.core.commit_index
    }

    /// Get the persistent state for saving.
    #[inline]
    pub fn persistent(&self) -> &Persistent {
        &self.core.persistent
    }

    /// Ticks until next deadline.
    ///
    /// The caller should sleep for this many ticks, then call `tick()`.
    /// This enables efficient event loops without spinning.
    pub fn ticks_until_deadline(&self) -> u64 {
        match &self.role {
            RoleState::Follower(f) => f.ticks_until_deadline(self.core.ticks),
            RoleState::PreCandidate(p) => p.ticks_until_deadline(self.core.ticks),
            RoleState::Candidate(c) => c.ticks_until_deadline(self.core.ticks),
            RoleState::Leader(l) => l.ticks_until_deadline(self.core.ticks),
        }
    }

    /// Advance logical time by one tick.
    ///
    /// Call this when the deadline from `ticks_until_deadline()` expires.
    pub fn tick(&mut self) -> Effects {
        self.step_event(Event::Tick)
    }

    /// Process a received message.
    pub fn step(&mut self, msg: Message) -> Effects {
        self.step_event(Event::Message(msg))
    }

    /// Signal completion of a disk write (§10.2.1 parallel disk write).
    ///
    /// When `parallel_disk_write` is enabled, the IO layer must call this
    /// after durably writing entries up to `index`. This allows the leader
    /// to count its own disk as a "vote" for commit quorum.
    pub fn disk_write_complete(&mut self, index: LogIndex) -> Effects {
        self.step_event(Event::DiskWriteComplete(index))
    }

    /// Propose a command for replication (leader only).
    pub fn propose(&mut self, cmd: Command) -> Result<(LogIndex, Effects), ProposeError> {
        match &mut self.role {
            RoleState::Leader(leader) => {
                let (index, effects, should_step_down) = leader.propose(&mut self.core, cmd);

                // Handle step-down if config change removed us
                if should_step_down {
                    let follower = Follower::new(&mut self.core, None);
                    self.role = RoleState::Follower(follower);
                }

                Ok((index, effects))
            }
            _ => Err(ProposeError::NotLeader { leader_hint: self.leader() }),
        }
    }

    /// Start a linearizable read-index quorum round (leader only).
    pub fn request_read_index(&mut self) -> Result<(LogIndex, Effects), ReadIndexError> {
        match &mut self.role {
            RoleState::Leader(leader) => leader.request_read_index(&self.core),
            _ => Err(ReadIndexError::NotLeader { leader_hint: self.leader() }),
        }
    }

    /// Whether a previously requested read index is confirmed and applied.
    pub fn read_index_ready(&self, read_index: LogIndex) -> bool {
        match &self.role {
            RoleState::Leader(leader) => leader.read_index_ready(&self.core, read_index),
            _ => false,
        }
    }

    /// Propose a configuration change (leader only).
    ///
    /// Per §4, the new configuration takes effect immediately when appended.
    pub fn propose_config_change(
        &mut self,
        change: &ConfigChange,
    ) -> Result<(LogIndex, Effects), ConfigChangeError> {
        match &mut self.role {
            RoleState::Leader(leader) => {
                let (index, effects, should_step_down) =
                    leader.propose_config_change(&mut self.core, change)?;

                // Handle step-down if config change removed us
                if should_step_down {
                    let follower = Follower::new(&mut self.core, None);
                    self.role = RoleState::Follower(follower);
                }

                Ok((index, effects))
            }
            _ => Err(ConfigChangeError::NotLeader),
        }
    }

    /// Transfer leadership to the target node (§3.11).
    ///
    /// Sends `TimeoutNow` to the target, causing it to start an election immediately.
    /// The target must be a voter and fully caught up with the leader's log.
    ///
    /// After calling this, the leader should stop accepting new proposals and
    /// wait for the target to win the election. Once the leader sees a higher
    /// term (from the target's election), it will step down automatically.
    pub fn transfer_leadership(&self, target: NodeId) -> Result<Effects, TransferError> {
        match &self.role {
            RoleState::Leader(leader) => leader.transfer_leadership(&self.core, target),
            _ => Err(TransferError::NotLeader),
        }
    }

    /// Get committed entries ready to be applied.
    ///
    /// Returns command entries in log order from `last_applied + 1` to `commit_index`.
    /// Config entries are not returned (they take effect on append, not commit).
    /// Call `advance()` after applying to acknowledge.
    pub fn committed(&self) -> impl Iterator<Item = CommittedEntry> + '_ {
        let start = self.core.last_applied + 1;
        let end = self.core.commit_index + 1;
        (start..end).filter_map(|idx| {
            self.core.log().get(idx).and_then(|entry| match &entry.payload {
                EntryPayload::Command(cmd) => Some(CommittedEntry {
                    index: entry.index,
                    term: entry.term,
                    command: cmd.clone(),
                }),
                EntryPayload::Config(_) => None,
            })
        })
    }

    /// Acknowledge that entries have been applied up to this index.
    pub fn advance(&mut self, applied_to: LogIndex) {
        self.core.last_applied = applied_to;
    }

    fn step_event(&mut self, event: Event) -> Effects {
        let result = match &mut self.role {
            RoleState::Follower(f) => f.step(&mut self.core, event),
            RoleState::PreCandidate(p) => p.step(&mut self.core, event),
            RoleState::Candidate(c) => c.step(&mut self.core, event),
            RoleState::Leader(l) => l.step(&mut self.core, event),
        };

        match result.transition {
            Transition::Stay => result.effects,
            transition => {
                let transition_effects = self.apply_transition(transition);
                result.effects.merge(transition_effects)
            }
        }
    }

    fn apply_transition(&mut self, transition: Transition) -> Effects {
        match transition {
            Transition::Stay => Effects::none(),
            Transition::ToFollower(leader) => {
                let follower = Follower::new(&mut self.core, leader);
                self.role = RoleState::Follower(follower);
                Effects::none()
            }
            Transition::ToFollowerAfterContact(leader) => {
                let follower = Follower::after_leader_contact(&mut self.core, leader);
                self.role = RoleState::Follower(follower);
                Effects::none()
            }
            Transition::ToPreCandidate => {
                let (precandidate, effects) = PreCandidate::new(&mut self.core);
                // Check for immediate win (single-node cluster)
                if precandidate.has_quorum(&self.core) {
                    // Skip to real candidate immediately
                    let (candidate, candidate_effects) = Candidate::new(&mut self.core);
                    if candidate.has_quorum(&self.core) {
                        let (leader, leader_effects) = Leader::new(&mut self.core);
                        self.role = RoleState::Leader(leader);
                        effects.merge(candidate_effects).merge(leader_effects)
                    } else {
                        self.role = RoleState::Candidate(candidate);
                        effects.merge(candidate_effects)
                    }
                } else {
                    self.role = RoleState::PreCandidate(precandidate);
                    effects
                }
            }
            Transition::ToCandidate => {
                let (candidate, effects) = Candidate::new(&mut self.core);
                // Check for immediate win (single-node cluster or joint consensus)
                if candidate.has_quorum(&self.core) {
                    let (leader, leader_effects) = Leader::new(&mut self.core);
                    self.role = RoleState::Leader(leader);
                    effects.merge(leader_effects)
                } else {
                    self.role = RoleState::Candidate(candidate);
                    effects
                }
            }
            Transition::ToLeader => {
                let (leader, effects) = Leader::new(&mut self.core);
                self.role = RoleState::Leader(leader);
                effects
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREE_VOTERS: &[NodeId] = &[NodeId(0), NodeId(1), NodeId(2)];

    /// Create a config with prevote disabled for existing tests.
    /// New PreVote-specific tests should use `Config::new()` which has prevote enabled.
    fn test_config(id: NodeId) -> Config {
        Config::new(id)
            .with_prevote(false)
            // Disable optimizations for backward-compatible tests
            .with_parallel_disk_write(false)
            .with_pipelining(false)
    }

    fn three_node_cluster() -> RaftNode {
        let config = test_config(NodeId(0));
        RaftNode::new(config, THREE_VOTERS)
    }

    // --- Basic state machine tests ---------------------------------------------

    #[test]
    fn starts_as_follower() {
        let node = three_node_cluster();

        assert!(node.role.is_follower());
        assert_eq!(node.term(), 0);
    }

    #[test]
    fn election_timeout_triggers_candidacy() {
        let mut node = three_node_cluster();

        // Tick until election timeout
        while node.role.is_follower() {
            node.tick();
        }

        assert!(node.role.is_candidate());
        assert_eq!(node.term(), 1);
    }

    #[test]
    fn single_node_becomes_leader_immediately() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0)]);

        // Tick until election timeout
        while !node.is_leader() {
            node.tick();
        }

        assert!(node.is_leader());
    }

    #[test]
    fn candidate_wins_with_majority() {
        let mut node = three_node_cluster();

        // Become candidate
        while node.role.is_follower() {
            node.tick();
        }

        // Receive vote from one peer (now have 2/3)
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);

        assert!(node.is_leader());
    }

    // --- Dissertation safety properties ----------------------------------------

    /// §3.2: Election Safety - at most one leader per term
    #[test]
    fn election_safety() {
        let mut node = three_node_cluster();

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        let leader_term = node.term();

        // Receive AppendEntries claiming leadership in same term (shouldn't happen)
        // Our node should stay leader (other node is wrong)
        let fake_leader = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: leader_term,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        node.step(fake_leader);

        // We should reject this (we're leader in this term)
        assert!(node.is_leader());
    }

    /// §3.6.2: Only commit current-term entries by counting
    #[test]
    fn only_commit_current_term() {
        let mut node = three_node_cluster();

        // Become leader at term 2
        node.core.persistent.term = 1;
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());
        assert_eq!(node.term(), 2);

        // Manually insert an old-term entry
        node.core.log_mut().append(Entry {
            term: 1, // Old term
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });

        // Force match_index to show it's replicated
        if let RoleState::Leader(ref mut leader) = node.role {
            for id in node.core.effective_config().voters() {
                leader.match_index.insert(id, 1);
            }
            let _ = leader.maybe_commit(&mut node.core);
        }

        // Should NOT commit because entry is from old term
        assert_eq!(node.commit_index(), 0);
    }

    /// §3.3: Terms increase monotonically
    #[test]
    fn term_monotonicity() {
        let mut node = three_node_cluster();

        let mut last_term = node.term();

        // Run through several elections
        for _ in 0..5 {
            while node.role.is_follower() {
                node.tick();
            }
            assert!(node.term() >= last_term);
            last_term = node.term();

            // Force back to follower with higher term
            node.core.maybe_update_term(node.term() + 1);
            node.role = RoleState::Follower(Follower::new(&mut node.core, None));
        }
    }

    /// §3.10: `TimeoutNow` triggers immediate election
    #[test]
    fn timeout_now() {
        let mut node = three_node_cluster();

        let initial_term = node.term();
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: initial_term,
            payload: Payload::TimeoutNow,
        };

        node.step(msg);

        assert!(node.role.is_candidate());
        assert!(node.term() > initial_term);
    }

    // --- API tests -------------------------------------------------------------

    #[test]
    fn propose_requires_leader() {
        let mut node = three_node_cluster();

        let result = node.propose(Command(vec![1]));
        assert!(matches!(result, Err(ProposeError::NotLeader { .. })));
    }

    #[test]
    fn leader_can_propose() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0)]);

        // Become leader
        while !node.is_leader() {
            node.tick();
        }

        let result = node.propose(Command(vec![42]));
        assert!(result.is_ok());

        let (index, effects) = result.unwrap();
        assert_eq!(index, 1);
        assert!(effects.persist);
    }

    #[test]
    fn ticks_until_deadline_prevents_spinning() {
        let node = three_node_cluster();

        // Should have a positive deadline (not spin immediately)
        assert!(node.ticks_until_deadline() >= 10);
    }

    #[test]
    fn committed_entries_iteration() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0)]);

        // Become leader
        while !node.is_leader() {
            node.tick();
        }

        // Propose some entries
        node.propose(Command(vec![1])).unwrap();
        node.propose(Command(vec![2])).unwrap();
        node.propose(Command(vec![3])).unwrap();

        // Get committed entries
        let committed: Vec<_> = node.committed().collect();
        assert_eq!(committed.len(), 3);
        assert_eq!(committed[0].index, 1);
        assert_eq!(committed[2].index, 3);

        // Advance and check again
        node.advance(2);
        let committed: Vec<_> = node.committed().collect();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].index, 3);
    }

    // --- Membership change tests (Chapter 4) ------------------------------------

    /// §4.1: Single-server add voter
    #[test]
    fn single_server_add_voter() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0)]);

        // Become leader
        while !node.is_leader() {
            node.tick();
        }

        // Add a new voter
        // Step 1: add learner
        let res = node.propose_config_change(&ConfigChange::AddLearner(NodeId(1))).unwrap();
        // Simulate learner catching up by acknowledging the config entry
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: res.0,
            }),
        };
        node.step(ack);

        // Step 2: promote to voter (requires learner to be caught up)
        let result = node.propose_config_change(&ConfigChange::AddVoter(NodeId(1)));
        assert!(result.is_ok());

        let (index, effects) = result.unwrap();
        assert_eq!(index, 2); // second config entry (first was learner)
        assert!(effects.persist);

        // New voter should be in the cluster
        assert!(node.core.effective_config().is_voter(NodeId(1)));
    }

    /// §4.1: Single-server remove voter
    #[test]
    fn single_server_remove_voter() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Remove a voter
        let result = node.propose_config_change(&ConfigChange::RemoveVoter(NodeId(2)));
        assert!(result.is_ok());

        // Removed voter should not be in the cluster
        assert!(!node.core.effective_config().is_voter(NodeId(2)));
    }

    /// §4: Only one config change at a time
    #[test]
    fn rejects_concurrent_config_changes() {
        let mut config = test_config(NodeId(0));
        config.membership_mode = MembershipMode::JointConsensus;
        let mut node = RaftNode::new(config, &[NodeId(0)]);

        // Become leader
        while !node.is_leader() {
            node.tick();
        }

        // Start a config change (creates joint config in JointConsensus mode)
        let members = Members::new([NodeId(0), NodeId(1)], []).unwrap();
        let result = node.propose_config_change(&ConfigChange::SetMembers(members));
        assert!(result.is_ok());

        // Second change should fail while first is pending
        let members = Members::new([NodeId(0), NodeId(2)], []).unwrap();
        let result = node.propose_config_change(&ConfigChange::SetMembers(members));
        assert!(matches!(result, Err(ConfigChangeError::ChangeInProgress)));
    }

    /// §4.2.2: Leader steps down when removed from config
    #[test]
    fn leader_steps_down_when_removed() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Remove self from config - new config is [1, 2]
        let _ = node.propose_config_change(&ConfigChange::RemoveVoter(NodeId(0)));
        let config_index = node.core.log().last_index();

        // After removing self, quorum is calculated with new config [1, 2]
        // Need majority (2/2) from the new config to commit
        // NodeId(0) is no longer a voter, so its match_index doesn't count
        let ack1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: config_index,
            }),
        };
        node.step(ack1);

        // Not committed yet - need NodeId(2) to ack too
        assert!(node.is_leader());

        let ack2 = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: config_index,
            }),
        };
        node.step(ack2);

        // Now committed, leader should step down
        assert!(node.role.is_follower());
    }

    /// §4.3: Joint consensus requires majorities from both configs
    #[test]
    fn joint_consensus_requires_both_majorities() {
        let mut config = test_config(NodeId(0));
        config.membership_mode = MembershipMode::JointConsensus;
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Change to a completely different set of voters (joint consensus)
        let members = Members::new([NodeId(0), NodeId(3), NodeId(4)], []).unwrap();
        let result = node.propose_config_change(&ConfigChange::SetMembers(members));
        assert!(result.is_ok());

        // The config should now be Joint
        assert!(node.core.effective_config().is_joint());

        // Entry should NOT be committed yet (only leader has it)
        let config_index = node.core.log().last_index();
        assert!(node.commit_index() < config_index);

        // Ack from old cluster peer (NodeId(1)) alone shouldn't commit
        // because we need majority from NEW config too
        let ack1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: config_index,
            }),
        };
        node.step(ack1);

        // Still not committed (have old majority 2/3, but new majority needs 2/3)
        // Leader counts as part of new config, so need one more from new
        assert!(node.commit_index() < config_index);
    }

    /// §4.3: Joint consensus auto-completes to `C_new`
    #[test]
    fn joint_consensus_auto_completes() {
        let mut config = test_config(NodeId(0));
        config.membership_mode = MembershipMode::JointConsensus;
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Change config to remove NodeId(2): [0,1,2] → [0,1]
        // Creates Joint{[0,1,2], [0,1]}
        let members = Members::new([NodeId(0), NodeId(1)], []).unwrap();
        let result = node.propose_config_change(&ConfigChange::SetMembers(members));
        assert!(result.is_ok());
        assert!(node.core.effective_config().is_joint());

        let joint_index = node.core.log().last_index();

        // For joint to commit: need 2/3 from [0,1,2] AND 2/2 from [0,1]
        // Leader (0) counts in both. NodeId(1) ack satisfies both majorities.
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: joint_index,
            }),
        };
        node.step(ack);

        // Joint config should have committed and auto-completed to Simple{[0,1]}
        // Log should now have: entry 1 = Joint, entry 2 = Simple (auto-appended)
        assert_eq!(node.core.log().last_index(), 2);

        // Config should be Simple after auto-completion
        // (but may still be Joint if C_new hasn't committed yet)
        // Let's check that C_new was appended
        let final_config = node.core.log().config_at(2);
        assert!(final_config.is_some());
        assert!(!final_config.unwrap().is_joint());
    }

    // =========================================================================
    // Efficiency / Non-Redundancy Tests
    //
    // These tests verify the implementation does no unnecessary work:
    // no duplicate messages, no wasted elections, no redundant persistence.
    // =========================================================================

    /// Candidate sends exactly N-1 vote requests (one per peer, none to self).
    #[test]
    fn no_duplicate_vote_requests() {
        // Test at role level: Candidate::new() returns initial effects
        let config = test_config(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);
        let (_candidate, effects) = Candidate::new(&mut core);

        // Collect all vote requests sent
        let vote_requests: Vec<_> = effects
            .messages
            .iter()
            .filter(|m| matches!(m.payload, Payload::VoteRequest(_)))
            .collect();

        // Should be exactly 4 (peers: 1, 2, 3, 4)
        assert_eq!(vote_requests.len(), 4, "expected exactly N-1 vote requests");

        // No duplicates (all different recipients)
        let recipients: std::collections::HashSet<_> = vote_requests.iter().map(|m| m.to).collect();
        assert_eq!(recipients.len(), 4, "vote requests should have unique recipients");

        // None to self
        assert!(!recipients.contains(&NodeId(0)), "should not send vote request to self");
    }

    /// Receiving the same vote twice produces no additional effects.
    #[test]
    fn duplicate_votes_are_idempotent() {
        let (mut core, mut candidate, initial_effects) = {
            let config = test_config(NodeId(0));
            let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
            let (candidate, effects) = Candidate::new(&mut core);
            (core, candidate, effects)
        };

        // Verify initial state
        assert_eq!(candidate.votes.len(), 1); // Self
        assert!(initial_effects.persist);

        // First vote from peer
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };

        let result = candidate.step(&mut core, Event::Message(vote.clone()));
        assert!(matches!(result.transition, Transition::ToLeader)); // Won election
        assert_eq!(candidate.votes.len(), 2);

        // Reset to pre-win state for duplicate test
        let config = test_config(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);
        let (mut candidate, _) = Candidate::new(&mut core);

        // First vote
        let vote1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        candidate.step(&mut core, Event::Message(vote1));
        assert_eq!(candidate.votes.len(), 2);

        // Duplicate vote from same peer
        let vote_dup = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        let result = candidate.step(&mut core, Event::Message(vote_dup));

        // No state change, no effects
        assert!(
            matches!(result.transition, Transition::Stay),
            "duplicate vote should not cause transition"
        );
        assert_eq!(candidate.votes.len(), 2, "vote count unchanged");
        assert!(result.effects.messages.is_empty(), "no messages from duplicate vote");
        assert!(!result.effects.persist, "no persist from duplicate vote");
    }

    /// Ticks before heartbeat deadline produce no messages.
    #[test]
    fn heartbeats_only_at_deadline() {
        // Test at role level: Leader step returns effects directly
        let mut config = test_config(NodeId(0));
        config.heartbeat_interval = 5; // Use larger interval for clearer test
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let (mut leader, _initial_effects) = Leader::new(&mut core);

        // Ticks before deadline should produce no messages
        let deadline = leader.heartbeat_deadline;
        while core.ticks + 1 < deadline {
            let result = leader.step(&mut core, Event::Tick);
            assert!(result.effects.messages.is_empty(), "no messages before heartbeat deadline");
        }

        // Tick at deadline should produce heartbeats
        let result = leader.step(&mut core, Event::Tick);
        assert!(!result.effects.messages.is_empty(), "should send heartbeats at deadline");
    }

    /// Leader sends empty entries to up-to-date followers (heartbeat only).
    #[test]
    fn up_to_date_followers_get_empty_heartbeats() {
        // Test at role level
        let mut config = test_config(NodeId(0));
        config.heartbeat_interval = 5;
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        let (mut leader, _) = Leader::new(&mut core);

        // Propose some entries
        let (_, _, _) = leader.propose(&mut core, Command(vec![1]));
        let (_, _, _) = leader.propose(&mut core, Command(vec![2]));

        // Simulate follower catching up
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 2 }),
        };
        leader.step(&mut core, Event::Message(ack));

        // Advance to heartbeat deadline
        while core.ticks + 1 < leader.heartbeat_deadline {
            leader.step(&mut core, Event::Tick);
        }
        let result = leader.step(&mut core, Event::Tick);

        // Heartbeat to follower should have empty entries (they're caught up)
        for msg in &result.effects.messages {
            if let Payload::AppendRequest(req) = &msg.payload {
                assert!(
                    req.entries.is_empty(),
                    "up-to-date follower should receive empty heartbeat, got {} entries",
                    req.entries.len()
                );
            }
        }
    }

    /// Rejection hint is used to efficiently adjust `next_index`.
    #[test]
    fn rejection_uses_hint_efficiently() {
        // Test at role level with pre-populated log
        let config = test_config(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        core.persistent.term = 1;

        // Add entries to log before creating leader
        for i in 1..=10u64 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }

        let (mut leader, _) = Leader::new(&mut core);

        // Leader initializes next_index to last_log_index + 1 = 11
        let next_before = leader.next_index[&NodeId(1)];
        assert_eq!(next_before, 11, "initial next_index should be last_log_index + 1");

        // Follower rejects with hint that it only has index 3
        let rejection = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendResponse(AppendResponse {
                success: false,
                last_log_index: 3, // Hint: follower has up to 3
            }),
        };
        leader.step(&mut core, Event::Message(rejection));

        // next_index should jump to hint+1 = 4, not decrement by 1 from 11
        let next_after = leader.next_index[&NodeId(1)];
        assert_eq!(next_after, 4, "should use hint efficiently: jump to hint+1");
    }

    /// Persist flag only set when state actually changes.
    #[test]
    fn no_redundant_persistence() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Initial ticks as follower - no persistence needed
        for _ in 0..5 {
            let effects = node.tick();
            // Ticks alone shouldn't require persistence (unless election triggered)
            if node.role.is_follower() {
                assert!(!effects.persist, "follower tick shouldn't require persistence");
            }
        }

        // Stale message - should not require persistence
        let stale_msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 0, // Old term (we're at term 0, but will reject as stale vote request)
            payload: Payload::VoteResponse(VoteResponse { granted: false }),
        };
        let effects = node.step(stale_msg);
        assert!(!effects.persist, "stale message shouldn't require persistence");
    }

    /// Leader never sends messages to itself.
    #[test]
    fn leader_never_sends_to_self() {
        // Test at role level
        let mut config = test_config(NodeId(0));
        config.heartbeat_interval = 5;
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let (mut leader, initial_effects) = Leader::new(&mut core);

        // Collect all messages from various operations
        let mut all_messages = Vec::new();
        all_messages.extend(initial_effects.messages);

        // Propose some entries
        let (_, propose_effects, _) = leader.propose(&mut core, Command(vec![1]));
        all_messages.extend(propose_effects.messages);

        // Trigger heartbeat
        while core.ticks + 1 < leader.heartbeat_deadline {
            leader.step(&mut core, Event::Tick);
        }
        let result = leader.step(&mut core, Event::Tick);
        all_messages.extend(result.effects.messages);

        // Verify no message has self as recipient
        for msg in &all_messages {
            assert_ne!(msg.to, NodeId(0), "leader should never send to self");
        }
    }

    /// Stale-term messages are rejected with minimal work.
    #[test]
    fn stale_messages_rejected_efficiently() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Advance to term 5
        node.core.persistent.term = 5;

        // Send message from old term
        let stale_append = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2, // Old term
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![Entry {
                    term: 2,
                    index: 1,
                    payload: EntryPayload::Command(Command(vec![1])),
                }],
                leader_commit: 0,
            }),
        };

        let effects = node.step(stale_append);

        // Should reject without modifying log
        assert_eq!(node.core.log().last_index(), 0, "log should be unchanged");
        assert!(!effects.persist, "no persistence for stale message");

        // Should send rejection response
        assert_eq!(effects.messages.len(), 1);
        if let Payload::AppendResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.success);
        }
    }

    /// Re-sending already-applied entries is idempotent.
    #[test]
    fn idempotent_append_entries() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        let entry = Entry { term: 1, index: 1, payload: EntryPayload::Command(Command(vec![42])) };

        // First append
        let append1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry.clone()],
                leader_commit: 0,
            }),
        };
        node.step(append1);
        assert_eq!(node.core.log().last_index(), 1);

        // Re-send same entry (network duplicate)
        let append2 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![entry.clone()],
                leader_commit: 0,
            }),
        };
        let effects = node.step(append2);

        // Log should still have exactly 1 entry (not 2)
        assert_eq!(node.core.log().last_index(), 1, "duplicate append should be idempotent");

        // Should still succeed (idempotent)
        if let Payload::AppendResponse(resp) = &effects.messages[0].payload {
            assert!(resp.success);
        }
    }

    /// Candidate doesn't restart election while leader heartbeats are recent.
    #[test]
    fn no_disruptive_election_with_active_leader() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Receive heartbeat to establish leader contact
        let heartbeat = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        node.step(heartbeat);

        // Try to become candidate via external vote request
        // (simulating another node trying to start election)
        let vote_req = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        let effects = node.step(vote_req);

        // Should deny vote due to recent leader contact
        assert_eq!(effects.messages.len(), 1);
        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted, "should deny vote when leader is active");
        }

        // Term should NOT have advanced (didn't process the higher term)
        assert_eq!(node.term(), 1, "should not advance term for disruptive election");
    }

    /// Single-node cluster commits immediately without network round-trip.
    #[test]
    fn single_node_commits_without_network() {
        // Test at role level
        let config = test_config(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0)]);
        let (mut leader, _) = Leader::new(&mut core);

        // Propose - should commit immediately
        let (index, effects, _) = leader.propose(&mut core, Command(vec![1]));

        // No messages needed (no peers)
        assert!(effects.messages.is_empty(), "single-node should need no messages");

        // Already committed (single-node quorum)
        assert_eq!(core.commit_index, index, "single-node should commit immediately");
    }

    /// Verify no wasted work when transitioning roles.
    #[test]
    fn minimal_effects_on_role_transition() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Get to candidate
        while node.role.is_follower() {
            node.tick();
        }

        // Higher term message forces step-down
        let higher_term = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 100,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        let effects = node.step(higher_term);

        // Should be follower now
        assert!(node.role.is_follower());

        // Effects should be minimal: persist term without acknowledging
        // AppendEntries before the follower consistency check runs.
        assert!(effects.persist, "term change requires persist");
        assert!(effects.messages.is_empty(), "must not ack unprocessed append");
    }

    // --- Dissertation Invariants & Efficiency Tests --------------------------
    //
    // Tests derived from Ongaro's dissertation to verify safety properties
    // and ensure no redundant work occurs.

    /// §3.4: Election restriction - candidates with stale logs are rejected quickly.
    /// Voters should reject candidates whose logs are less up-to-date.
    #[test]
    fn election_restriction_rejects_stale_candidates() {
        let config = test_config(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let follower = Follower::new(&mut core, None);

        // Give follower a log entry at term 5
        core.persistent.term = 5;
        core.log_mut().append(Entry {
            term: 5,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });

        let mut follower = follower;

        // Candidate with older term (term 3) should be rejected
        let stale_candidate = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 5,
            payload: Payload::VoteRequest(VoteRequest {
                last_log_term: 3,   // Older term
                last_log_index: 10, // Even with longer log
            }),
        };
        let result = follower.step(&mut core, Event::Message(stale_candidate));
        if let Payload::VoteResponse(resp) = &result.effects.messages[0].payload {
            assert!(!resp.granted, "should reject candidate with older log term");
        }

        // Candidate with same term but shorter log should be rejected
        let short_candidate = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 5,
            payload: Payload::VoteRequest(VoteRequest {
                last_log_term: 5,
                last_log_index: 0, // Shorter log
            }),
        };
        let result = follower.step(&mut core, Event::Message(short_candidate));
        if let Payload::VoteResponse(resp) = &result.effects.messages[0].payload {
            assert!(!resp.granted, "should reject candidate with shorter log");
        }
    }

    /// §3.4: Up-to-date candidates win votes efficiently (single RPC).
    #[test]
    fn up_to_date_candidate_wins_vote_in_single_rpc() {
        let config = test_config(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        core.persistent.term = 1;

        // Add entry to follower's log
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });

        let mut follower = Follower::new(&mut core, None);

        // Candidate with equal or better log gets vote immediately
        let good_candidate = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_term: 1, last_log_index: 1 }),
        };
        let result = follower.step(&mut core, Event::Message(good_candidate));

        // Should grant vote in single response (no back-and-forth)
        assert_eq!(result.effects.messages.len(), 1);
        if let Payload::VoteResponse(resp) = &result.effects.messages[0].payload {
            assert!(resp.granted, "should grant vote to up-to-date candidate");
        }
    }

    /// §4 (disruptive elections): Candidate also respects leader contact.
    #[test]
    fn candidate_respects_active_leader() {
        let config = test_config(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let (mut candidate, _) = Candidate::new(&mut core);

        // Simulate recent leader contact via AppendRequest
        let heartbeat = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        candidate.step(&mut core, Event::Message(heartbeat));

        // Now another candidate requests vote - should be denied
        let vote_req = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        let result = candidate.step(&mut core, Event::Message(vote_req));

        if let Payload::VoteResponse(resp) = &result.effects.messages[0].payload {
            assert!(!resp.granted, "candidate should deny vote when leader is active");
        }
    }

    /// Randomized election timeouts produce different deadlines (prevents synchronized elections).
    #[test]
    fn randomized_timeouts_differ_across_nodes() {
        let mut deadlines = Vec::new();

        // Create multiple nodes and collect their election deadlines
        for id in 0..10 {
            let config = test_config(NodeId(id)).with_election_timeout(100, 200);
            let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
            let follower = Follower::new(&mut core, None);
            deadlines.push(follower.election_deadline);
        }

        // All deadlines should be different (with high probability)
        let unique: std::collections::HashSet<_> = deadlines.iter().collect();
        assert!(
            unique.len() >= 8,
            "randomized timeouts should produce mostly unique deadlines, got {} unique out of 10",
            unique.len()
        );
    }

    /// After successful replication, leader doesn't re-send entries.
    #[test]
    fn no_redundant_entries_after_ack() {
        let config = test_config(NodeId(0)).with_heartbeat_interval(5);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        let (mut leader, _) = Leader::new(&mut core);

        // Propose entry
        leader.propose(&mut core, Command(vec![1]));

        // Follower acks
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 1 }),
        };
        leader.step(&mut core, Event::Message(ack));

        // Advance to heartbeat
        while core.ticks + 1 < leader.heartbeat_deadline {
            leader.step(&mut core, Event::Tick);
        }
        let result = leader.step(&mut core, Event::Tick);

        // Heartbeat should have no entries (follower is caught up)
        for msg in &result.effects.messages {
            if let Payload::AppendRequest(req) = &msg.payload {
                assert!(req.entries.is_empty(), "should not re-send already-acked entries");
            }
        }
    }

    /// After rejection with hint, leader jumps directly (no one-by-one decrement).
    #[test]
    fn rejection_hint_avoids_linear_backoff() {
        let config = test_config(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        core.persistent.term = 1;

        // Add many entries
        for i in 1..=100 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }

        let (mut leader, _) = Leader::new(&mut core);

        // Follower rejects, claiming to only have index 10
        let rejection = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendResponse(AppendResponse { success: false, last_log_index: 10 }),
        };
        leader.step(&mut core, Event::Message(rejection));

        // next_index should jump to 11, not 100 (one-by-one would be wasteful)
        let next = leader.next_index[&NodeId(1)];
        assert_eq!(next, 11, "should use hint to jump, not decrement one-by-one");
    }

    /// Term doesn't inflate from messages we choose to ignore.
    #[test]
    fn no_term_inflation_from_ignored_messages() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Establish leader contact
        let heartbeat = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        node.step(heartbeat);
        assert_eq!(node.term(), 1);

        // Disruptive vote request from higher term
        let disruptive = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 100, // Much higher term
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        node.step(disruptive);

        // Term should NOT have jumped to 100 (we ignored it due to active leader)
        assert_eq!(node.term(), 1, "term should not inflate from disruptive vote request");
    }

    #[test]
    fn higher_term_append_hint_does_not_activate_leader_guard() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);
        node.core.persistent.term = 1;
        node.core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });
        let (candidate, _) = Candidate::new(&mut node.core);
        node.role = RoleState::Candidate(candidate);

        node.step(Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 3,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 1,
                prev_log_term: 99,
                entries: vec![],
                leader_commit: 0,
            }),
        });

        assert!(node.is_follower());
        assert_eq!(node.leader(), Some(NodeId(1)));

        let effects = node.step(Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 4,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 1, last_log_term: 1 }),
        });

        assert_eq!(node.term(), 4);
        assert_eq!(node.core.persistent.voted_for, Some(NodeId(2)));
        assert!(matches!(
            effects.messages[0].payload,
            Payload::VoteResponse(VoteResponse { granted: true })
        ));
    }

    /// Persistence flag not set for read-only operations.
    #[test]
    fn no_persist_for_readonly_operations() {
        let config = test_config(NodeId(0)).with_heartbeat_interval(5);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        let (mut leader, _) = Leader::new(&mut core);

        // Tick (read-only operation for leader)
        let result = leader.step(&mut core, Event::Tick);
        assert!(!result.effects.persist, "tick alone should not require persist");

        // Receiving an ack (updates volatile state only)
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: core.term(),
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 0 }),
        };
        let result = leader.step(&mut core, Event::Message(ack));
        assert!(!result.effects.persist, "ack updates volatile state only");
    }

    /// Vote granted resets election timeout (prevents unnecessary re-elections).
    #[test]
    fn vote_grant_resets_timeout() {
        let config = test_config(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let mut follower = Follower::new(&mut core, None);

        // Advance time
        for _ in 0..5 {
            follower.step(&mut core, Event::Tick);
        }
        let deadline_before = follower.election_deadline;

        // Grant vote
        let vote_req = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        follower.step(&mut core, Event::Message(vote_req));

        // Deadline should have been reset (to a later time)
        assert!(
            follower.election_deadline > deadline_before,
            "granting vote should reset election timeout"
        );
    }

    /// Log entries are stable once written - new leader extends, doesn't conflict.
    /// (Conflicts at committed indices can't happen due to election restriction.)
    #[test]
    fn log_entries_extended_not_conflicted() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Receive entries from leader in term 1
        let append1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![Entry {
                    term: 1,
                    index: 1,
                    payload: EntryPayload::Command(Command(vec![42])),
                }],
                leader_commit: 1,
            }),
        };
        node.step(append1);
        assert_eq!(node.commit_index(), 1);

        // New leader in term 2 extends the log (must include committed entry)
        let append2 = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 1,
                prev_log_term: 1, // Matches existing committed entry
                entries: vec![Entry {
                    term: 2,
                    index: 2,
                    payload: EntryPayload::Command(Command(vec![99])),
                }],
                leader_commit: 1,
            }),
        };
        node.step(append2);

        // Original entry preserved, new entry added
        assert_eq!(node.core.log().last_index(), 2);
        assert_eq!(node.core.log().get(1).unwrap().term, 1);
        assert_eq!(node.core.log().get(2).unwrap().term, 2);
    }

    /// Leader broadcasts to all peers exactly once per heartbeat interval.
    #[test]
    fn exactly_one_broadcast_per_heartbeat() {
        let config = test_config(NodeId(0)).with_heartbeat_interval(10);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2), NodeId(3)]);
        let (mut leader, initial_effects) = Leader::new(&mut core);

        // Initial broadcast
        let initial_count = initial_effects.messages.len();
        assert_eq!(initial_count, 3, "should broadcast to all 3 peers initially");

        // Count messages over multiple heartbeat intervals
        let mut total_messages = 0;
        let mut heartbeat_count = 0;

        for _ in 0..100 {
            let result = leader.step(&mut core, Event::Tick);
            if !result.effects.messages.is_empty() {
                heartbeat_count += 1;
                total_messages += result.effects.messages.len();
            }
        }

        // Should have had ~10 heartbeats (100 ticks / 10 interval)
        assert!((9..=11).contains(&heartbeat_count));

        // Each heartbeat should send exactly 3 messages (one per peer)
        let expected = heartbeat_count * 3;
        assert_eq!(
            total_messages, expected,
            "should send exactly one message per peer per heartbeat"
        );
    }

    /// No response generated for unknown message types at same term.
    #[test]
    fn unknown_payloads_ignored_silently() {
        let config = test_config(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        core.persistent.term = 1; // Start at term 1
        let mut follower = Follower::new(&mut core, None);

        // VoteResponse to a follower (makes no sense, should be ignored)
        let nonsense = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1, // Same term, no update needed
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        let result = follower.step(&mut core, Event::Message(nonsense));

        // Should produce no effects (silently ignored)
        assert!(result.effects.messages.is_empty());
        assert!(!result.effects.persist, "ignored message should not require persist");
    }

    /// Joint consensus requires majorities from BOTH configs (§4.3).
    #[test]
    fn joint_consensus_requires_dual_majorities() {
        let mut config = test_config(NodeId(0));
        config.membership_mode = MembershipMode::JointConsensus;
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Start joint config: old=[0,1,2], new=[0,3,4]
        let members = Members::new([NodeId(0), NodeId(3), NodeId(4)], []).unwrap();
        node.propose_config_change(&ConfigChange::SetMembers(members)).unwrap();
        let config_index = node.core.log().last_index();

        // Ack from old config member only
        let old_ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: config_index,
            }),
        };
        node.step(old_ack);

        // Should NOT be committed (need new config majority too)
        assert!(
            node.commit_index() < config_index,
            "joint config needs majorities from both old AND new configs"
        );

        // Ack from new config member
        let new_ack = Message {
            from: NodeId(3),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: config_index,
            }),
        };
        node.step(new_ack);

        // Now should be committed (have 2/3 from old, 2/3 from new)
        assert!(node.commit_index() >= config_index, "should commit once both majorities achieved");
    }

    // --- Leadership Transfer Tests (§3.11) --------------------------------------

    /// §3.11: Leadership transfer sends `TimeoutNow` to caught-up target.
    #[test]
    fn leadership_transfer_to_caught_up_peer() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Simulate follower catching up (empty log, so match_index 0 is caught up)
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 0 }),
        };
        node.step(ack);

        // Transfer to caught-up peer
        let result = node.transfer_leadership(NodeId(1));
        assert!(result.is_ok());

        let effects = result.unwrap();
        assert_eq!(effects.messages.len(), 1);
        assert_eq!(effects.messages[0].to, NodeId(1));
        assert!(matches!(effects.messages[0].payload, Payload::TimeoutNow));
    }

    /// §3.11: Transfer fails if target is not caught up.
    #[test]
    fn leadership_transfer_fails_if_target_lagging() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());

        // Propose some entries (target hasn't acked them)
        node.propose(Command(vec![1])).unwrap();
        node.propose(Command(vec![2])).unwrap();

        // Transfer should fail - target is lagging
        let result = node.transfer_leadership(NodeId(1));
        assert!(matches!(
            result,
            Err(TransferError::TargetLagging { match_index: 0, last_index: 2 })
        ));
    }

    /// §3.11: Transfer fails if target is not a voter.
    #[test]
    fn leadership_transfer_fails_for_non_voter() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);

        // Try to transfer to non-existent node
        let result = node.transfer_leadership(NodeId(99));
        assert!(matches!(result, Err(TransferError::TargetNotVoter)));
    }

    /// §3.11: Transfer fails if target is self.
    #[test]
    fn leadership_transfer_fails_to_self() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);

        let result = node.transfer_leadership(NodeId(0));
        assert!(matches!(result, Err(TransferError::TargetIsSelf)));
    }

    /// §3.11: Transfer fails if not leader.
    #[test]
    fn leadership_transfer_fails_if_not_leader() {
        let config = test_config(NodeId(0));
        let node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

        // Still a follower
        assert!(node.role.is_follower());

        let result = node.transfer_leadership(NodeId(1));
        assert!(matches!(result, Err(TransferError::NotLeader)));
    }

    /// §3.11: Full transfer flow - target receives `TimeoutNow` and wins election.
    #[test]
    fn leadership_transfer_full_flow() {
        let config0 = test_config(NodeId(0));
        let config1 = test_config(NodeId(1));
        let voters = &[NodeId(0), NodeId(1), NodeId(2)];

        let mut node0 = RaftNode::new(config0, voters);
        let mut node1 = RaftNode::new(config1, voters);

        // Node 0 becomes leader
        while node0.role.is_follower() {
            node0.tick();
        }
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node0.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node0.step(vote);
        assert!(node0.is_leader());
        let leader_term = node0.term();

        // Node 1 acknowledges (catches up)
        let ack = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: leader_term,
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 0 }),
        };
        node0.step(ack);

        // Transfer leadership to node 1
        let effects = node0.transfer_leadership(NodeId(1)).unwrap();
        assert_eq!(effects.messages.len(), 1);

        // Deliver TimeoutNow to node 1
        node1.step(effects.messages[0].clone());

        // Node 1 should now be a candidate with incremented term
        assert!(node1.role.is_candidate());
        assert!(node1.term() > leader_term);

        // Node 1 wins election (gets vote from node 0)
        let vote_for_1 = Message {
            from: NodeId(0),
            to: NodeId(1),
            term: node1.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node1.step(vote_for_1);

        // Node 1 is now leader
        assert!(node1.is_leader());

        // When node 0 hears from new leader, it steps down
        let heartbeat = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node1.term(),
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        node0.step(heartbeat);

        // Node 0 is now follower
        assert!(node0.role.is_follower());
        assert_eq!(node0.leader(), Some(NodeId(1)));
    }

    // --- PreVote tests (§4.2.3) ------------------------------------------------

    /// §4.2.3: With `PreVote` enabled, follower transitions to `PreCandidate` on timeout.
    #[test]
    fn prevote_follower_becomes_precandidate() {
        // Use prevote-enabled config
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Tick until election timeout
        while node.role.is_follower() {
            node.tick();
        }

        // Should be PreCandidate, not Candidate
        assert!(node.role.is_precandidate());
        assert_eq!(node.term(), 0); // Term NOT incremented yet
    }

    /// §4.2.3: `PreCandidate` sends `PreVoteRequest`, not `VoteRequest`.
    #[test]
    fn prevote_sends_prevote_requests() {
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Tick until PreCandidate
        let mut effects = Effects::none();
        while node.role.is_follower() {
            effects = node.tick();
        }

        // Should have sent PreVoteRequest to peers
        assert_eq!(effects.messages.len(), 2);
        for msg in &effects.messages {
            assert!(matches!(msg.payload, Payload::PreVoteRequest(_)));
            if let Payload::PreVoteRequest(req) = &msg.payload {
                assert_eq!(req.next_term, 1); // Would be term 1 if elected
            }
        }
    }

    /// §4.2.3: `PreCandidate` proceeds to real Candidate after majority pre-votes.
    #[test]
    fn prevote_proceeds_to_candidate_on_majority() {
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Become PreCandidate
        while node.role.is_follower() {
            node.tick();
        }
        assert!(node.role.is_precandidate());

        // Receive pre-vote from one peer (majority = 2)
        let prevote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 0,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: 0, granted: true }),
        };
        node.step(prevote);

        // Should now be Candidate with incremented term
        assert!(node.role.is_candidate());
        assert_eq!(node.term(), 1);
    }

    /// §4.2.3: `PreVote` doesn't disrupt cluster - partitioned node can't increment term.
    #[test]
    fn prevote_prevents_term_disruption() {
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Become PreCandidate
        while node.role.is_follower() {
            node.tick();
        }

        // Simulate partition: no responses received
        // Keep timing out for many election cycles - term should NOT increase
        // The default election timeout is 150-300, so ~500 ticks should be multiple cycles
        for _ in 0..500 {
            node.tick();
            // Term should never increase while stuck in PreCandidate phase
            assert_eq!(node.term(), 0, "term should not increase during pre-vote");
        }

        // After all those ticks, should still be precandidate (or recycled to precandidate)
        // and term should still be 0
        assert!(node.role.is_precandidate());
        assert_eq!(node.term(), 0);
    }

    /// §4.2.3: Full `PreVote` → Vote → Leader flow.
    #[test]
    fn prevote_full_election_flow() {
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Phase 1: Become PreCandidate
        while node.role.is_follower() {
            node.tick();
        }
        assert!(node.role.is_precandidate());

        // Phase 2: Win pre-vote → become Candidate
        let prevote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 0,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: 0, granted: true }),
        };
        node.step(prevote);
        assert!(node.role.is_candidate());
        assert_eq!(node.term(), 1);

        // Phase 3: Win real vote → become Leader
        let vote = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };
        node.step(vote);
        assert!(node.is_leader());
    }

    /// §4.2.3: `PreCandidate` steps down on `AppendEntries` from leader.
    #[test]
    fn prevote_steps_down_on_leader_contact() {
        let config = Config::new(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Become PreCandidate
        while node.role.is_follower() {
            node.tick();
        }

        // Leader sends heartbeat
        let heartbeat = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        node.step(heartbeat);

        // Should become follower with known leader
        assert!(node.role.is_follower());
        assert_eq!(node.leader(), Some(NodeId(1)));
    }

    // --- Automatic leadership transfer tests (§4.2.2) --------------------------

    /// §4.2.2: When leader is removed from config, it transfers leadership.
    #[test]
    fn auto_transfer_on_removal() {
        let config = test_config(NodeId(0));
        let mut node = RaftNode::new(config, THREE_VOTERS);

        // Become leader
        while node.role.is_follower() {
            node.tick();
        }
        let effects = node.step(Message {
            from: NodeId(1),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        });
        assert!(node.is_leader());

        // Propagate heartbeats and get acks from all peers (make them caught up)
        for msg in &effects.messages {
            if let Payload::AppendRequest(_) = &msg.payload {
                node.step(Message {
                    from: msg.to,
                    to: msg.from,
                    term: node.term(),
                    payload: Payload::AppendResponse(AppendResponse {
                        success: true,
                        last_log_index: 0,
                    }),
                });
            }
        }

        // Add node 3 as learner then voter (to have someone to transfer to)
        let _ = node.propose_config_change(&ConfigChange::AddLearner(NodeId(3)));

        // Get node 3 caught up
        node.step(Message {
            from: NodeId(3),
            to: NodeId(0),
            term: node.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: node.core.log().last_index(),
            }),
        });

        let _ = node.propose_config_change(&ConfigChange::AddVoter(NodeId(3)));

        // Update everyone's match index again
        for peer in [NodeId(1), NodeId(2), NodeId(3)] {
            node.step(Message {
                from: peer,
                to: NodeId(0),
                term: node.term(),
                payload: Payload::AppendResponse(AppendResponse {
                    success: true,
                    last_log_index: node.core.log().last_index(),
                }),
            });
        }

        // Now remove ourselves (node 0)
        let result = node.propose_config_change(&ConfigChange::RemoveVoter(NodeId(0)));

        // The config change should succeed
        let (_, _effects) = result.expect("config change should succeed");

        // Get acknowledgment to commit the change
        for peer in [NodeId(1), NodeId(2), NodeId(3)] {
            let eff = node.step(Message {
                from: peer,
                to: NodeId(0),
                term: node.term(),
                payload: Payload::AppendResponse(AppendResponse {
                    success: true,
                    last_log_index: node.core.log().last_index(),
                }),
            });

            // Check if TimeoutNow was sent
            for msg in &eff.messages {
                if matches!(msg.payload, Payload::TimeoutNow) {
                    // Leadership transfer was initiated
                    assert!(node.role.is_follower());
                    return;
                }
            }
        }

        // Should have stepped down by now
        assert!(node.role.is_follower());
    }
}
