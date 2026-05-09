//! Follower role.
//!
//! A follower:
//! - Responds to `AppendEntries` from the leader
//! - Grants votes to candidates with up-to-date logs
//! - Starts an election if election timeout elapses
//!
//! Follower-specific state:
//! - `leader`: the current known leader (if any)
//! - `election_deadline`: when to start an election

use crate::NodeId;

use super::StepResult;
use super::core::Core;
use super::event::{
    AppendRequest, AppendResponse, Effects, Event, InstallSnapshotRequest, InstallSnapshotResponse,
    Message, Payload, PreVoteRequest, PreVoteResponse, ReadIndexRequest, ReadIndexResponse,
    VoteRequest, VoteResponse,
};
use super::log::Entry;

/// Follower role state.
#[derive(Debug, Clone)]
pub struct Follower {
    /// Known leader for this term.
    pub leader: Option<NodeId>,
    /// Tick at which to start an election.
    pub election_deadline: u64,
    /// Tick when we last heard from a leader (for disruptive-vote guard).
    pub last_contact_tick: Option<u64>,
}

impl Follower {
    /// Create a new follower with a random election deadline.
    pub fn new(core: &mut Core, leader: Option<NodeId>) -> Self {
        let timeout = core.random_election_timeout();
        Self { leader, election_deadline: core.ticks + timeout, last_contact_tick: None }
    }

    /// Create a follower after processing a leader-contacting RPC.
    pub fn after_leader_contact(core: &mut Core, leader: Option<NodeId>) -> Self {
        let mut follower = Self::new(core, leader);
        follower.last_contact_tick = Some(core.ticks);
        follower
    }

    /// Ticks until election timeout.
    pub fn ticks_until_deadline(&self, ticks: u64) -> u64 {
        self.election_deadline.saturating_sub(ticks)
    }

    /// Process an event. Returns transition (if any) and effects.
    pub fn step(&mut self, core: &mut Core, event: Event) -> StepResult {
        match event {
            Event::Tick => self.handle_tick(core),
            // DiskWriteComplete is only relevant for leader
            Event::DiskWriteComplete(_) => StepResult::none(),
            Event::Message(msg) => {
                // Guard against disruptive elections (§4): if we've heard from a leader
                // recently, do not update term or grant a vote.
                if matches!(msg.payload, Payload::VoteRequest(_) | Payload::PreVoteRequest(_))
                    && self.recently_heard_from_leader(core)
                {
                    return StepResult::stay(Self::reject_due_to_active_leader(core, &msg));
                }

                // PreVoteRequest doesn't update term (handled separately)
                if let Payload::PreVoteRequest(req) = msg.payload {
                    return StepResult::stay(Self::handle_prevote_request(
                        core, msg.from, msg.term, req,
                    ));
                }

                // Higher term: update and reset
                let mut term_updated = false;
                if msg.term > core.term() {
                    term_updated = core.maybe_update_term(msg.term);
                    self.reset_deadline(core);
                    if matches!(
                        msg.payload,
                        Payload::AppendRequest(_) | Payload::ReadIndexRequest(_)
                    ) {
                        self.leader = Some(msg.from);
                    }
                }

                // Stale term: reject
                if msg.term < core.term() {
                    return StepResult::stay(Self::reject_stale(core, &msg));
                }

                let StepResult { transition, mut effects } = match msg.payload {
                    Payload::VoteRequest(req) => self.handle_vote_request(core, msg.from, req),
                    Payload::AppendRequest(req) => self.handle_append_request(core, msg.from, &req),
                    Payload::ReadIndexRequest(req) => {
                        self.handle_read_index_request(core, msg.from, req)
                    }
                    Payload::InstallSnapshotRequest(req) => {
                        self.handle_install_snapshot(core, msg.from, req)
                    }
                    Payload::TimeoutNow => StepResult::to_candidate(Effects::none()),
                    _ => StepResult::none(),
                };

                if term_updated {
                    effects = effects.with_persist();
                }
                StepResult { transition, effects }
            }
        }
    }

    fn handle_tick(&mut self, core: &mut Core) -> StepResult {
        core.ticks += 1;
        if core.ticks >= self.election_deadline {
            // Use PreVote if enabled (§4.2.3)
            if core.config.prevote {
                StepResult::to_precandidate(Effects::none())
            } else {
                StepResult::to_candidate(Effects::none())
            }
        } else {
            StepResult::none()
        }
    }

    fn handle_vote_request(
        &mut self,
        core: &mut Core,
        from: NodeId,
        req: VoteRequest,
    ) -> StepResult {
        let dominated = core.log_is_up_to_date(req.last_log_term, req.last_log_index);
        let candidate_is_voter = core.effective_config().is_voter(from);
        let can_vote = core.can_vote_for(from);
        let granted = candidate_is_voter && can_vote && dominated;

        let mut effects = Effects::none();
        if granted {
            core.vote_for(from);
            self.reset_deadline(core);
            effects = effects.with_persist();
        }

        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted }),
        };
        StepResult::stay(effects.with_message(resp))
    }

    fn handle_append_request(
        &mut self,
        core: &mut Core,
        from: NodeId,
        req: &AppendRequest,
    ) -> StepResult {
        self.leader = Some(from);
        self.record_contact(core);
        self.reset_deadline(core);

        // Consistency check (§3.5)
        if req.prev_log_index > 0 && core.log().term_at(req.prev_log_index) != req.prev_log_term {
            let resp = Message {
                from: core.id(),
                to: from,
                term: core.term(),
                payload: Payload::AppendResponse(AppendResponse {
                    success: false,
                    last_log_index: core.log().last_index(),
                }),
            };
            return StepResult::stay(Effects::none().with_message(resp));
        }

        // Append entries (truncate conflicts)
        let mut persist = false;
        for entry in &req.entries {
            let existing_term = core.log().term_at(entry.index);
            if existing_term != 0 && existing_term != entry.term {
                // Conflict: truncate from here
                core.log_mut().truncate_after(entry.index - 1);
            }
            if entry.index > core.log().last_index() {
                core.log_mut().append(Entry {
                    term: entry.term,
                    index: entry.index,
                    payload: entry.payload.clone(),
                });
                persist = true;
                // Config entries take effect immediately (§4) - effective_config() reflects this
            }
        }

        // Update commit index
        if req.leader_commit > core.commit_index {
            core.commit_index = req.leader_commit.min(core.log().last_index());
        }

        let mut effects = Effects::none();
        if persist {
            effects = effects.with_persist();
        }

        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::AppendResponse(AppendResponse {
                success: true,
                last_log_index: core.log().last_index(),
            }),
        };
        StepResult::stay(effects.with_message(resp))
    }

    /// Handle read-index heartbeat request (§6.4).
    fn handle_read_index_request(
        &mut self,
        core: &mut Core,
        from: NodeId,
        req: ReadIndexRequest,
    ) -> StepResult {
        self.leader = Some(from);
        self.record_contact(core);
        self.reset_deadline(core);

        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::ReadIndexResponse(ReadIndexResponse {
                id: req.id,
                read_index: req.read_index,
            }),
        };
        StepResult::stay(Effects::none().with_message(resp))
    }

    /// Handle `InstallSnapshot` RPC (§5, Figure 5.3).
    ///
    /// Per Figure 5.3 receiver implementation:
    /// 1. Reply immediately if term < currentTerm (handled by caller)
    /// 2. Create/write snapshot file at given offset (delegated to I/O layer)
    /// 3. If done is false, reply and wait for more chunks (delegated to I/O)
    /// 4. If existing log entry has same index/term as snapshot's last entry,
    ///    retain entries following it and reply
    /// 5. Otherwise discard the log suffix
    /// 6. Reset state machine using snapshot contents (delegated to I/O layer)
    fn handle_install_snapshot(
        &mut self,
        core: &mut Core,
        from: NodeId,
        req: InstallSnapshotRequest,
    ) -> StepResult {
        self.leader = Some(from);
        self.record_contact(core);
        self.reset_deadline(core);

        // Reject if snapshot is not newer than what we have
        let current_snapshot_index = core.log().snapshot_index();
        if req.last_included_index <= current_snapshot_index {
            let resp = Message {
                from: core.id(),
                to: from,
                term: core.term(),
                payload: Payload::InstallSnapshotResponse(InstallSnapshotResponse {
                    success: false,
                    last_included_index: req.last_included_index,
                }),
            };
            return StepResult::stay(Effects::none().with_message(resp));
        }

        if req.last_included_index < core.commit_index
            && !core.log().snapshot_matches_prefix(req.last_included_index, req.last_included_term)
        {
            let resp = Message {
                from: core.id(),
                to: from,
                term: core.term(),
                payload: Payload::InstallSnapshotResponse(InstallSnapshotResponse {
                    success: false,
                    last_included_index: req.last_included_index,
                }),
            };
            return StepResult::stay(Effects::none().with_message(resp));
        }

        // §5, step 5: Discard entire log and install snapshot metadata
        core.log_mut().install_snapshot(
            req.last_included_index,
            req.last_included_term,
            req.configuration,
        );

        // Update commit index to at least snapshot index
        if req.last_included_index > core.commit_index {
            core.commit_index = req.last_included_index;
        }
        if req.last_included_index > core.last_applied {
            core.last_applied = req.last_included_index;
        }

        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::InstallSnapshotResponse(InstallSnapshotResponse {
                success: true,
                last_included_index: req.last_included_index,
            }),
        };
        StepResult::stay(Effects::none().with_persist().with_message(resp))
    }

    fn reject_stale(core: &Core, msg: &Message) -> Effects {
        let payload = match &msg.payload {
            Payload::VoteRequest(_) => Payload::VoteResponse(VoteResponse { granted: false }),
            Payload::AppendRequest(_) => Payload::AppendResponse(AppendResponse {
                success: false,
                last_log_index: core.log().last_index(),
            }),
            Payload::ReadIndexRequest(req) => Payload::ReadIndexResponse(ReadIndexResponse {
                id: req.id,
                read_index: req.read_index,
            }),
            _ => return Effects::none(),
        };
        Effects::none().with_message(Message {
            from: core.id(),
            to: msg.from,
            term: core.term(),
            payload,
        })
    }

    fn reject_due_to_active_leader(core: &Core, msg: &Message) -> Effects {
        // Deny vote without updating term to preserve the current leader.
        let payload = match &msg.payload {
            Payload::PreVoteRequest(_) => {
                Payload::PreVoteResponse(PreVoteResponse { term: core.term(), granted: false })
            }
            Payload::VoteRequest(_) => Payload::VoteResponse(VoteResponse { granted: false }),
            _ => return Effects::none(),
        };
        Effects::none().with_message(Message {
            from: core.id(),
            to: msg.from,
            term: core.term(),
            payload,
        })
    }

    fn handle_prevote_request(
        core: &Core,
        from: NodeId,
        msg_term: u64,
        req: PreVoteRequest,
    ) -> Effects {
        // Grant pre-vote if:
        // 1. Candidate's next_term is greater than our term
        // 2. Candidate's log is at least as up-to-date as ours
        let dominated = core.log_is_up_to_date(req.last_log_term, req.last_log_index);
        let candidate_is_voter = core.effective_config().is_voter(from);
        let term_ok = req.next_term > core.term();
        let granted = candidate_is_voter && term_ok && dominated;

        Effects::none().with_message(Message {
            from: core.id(),
            to: from,
            term: msg_term,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: core.term(), granted }),
        })
    }

    fn recently_heard_from_leader(&self, core: &Core) -> bool {
        if self.leader.is_none() {
            return false;
        }
        let Some(last_contact_tick) = self.last_contact_tick else {
            return false;
        };
        let min_timeout = core.config.election_timeout.0;
        core.ticks.saturating_sub(last_contact_tick) < min_timeout
    }

    fn reset_deadline(&mut self, core: &mut Core) {
        let timeout = core.random_election_timeout();
        self.election_deadline = core.ticks + timeout;
    }

    fn record_contact(&mut self, core: &Core) {
        self.last_contact_tick = Some(core.ticks);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;

    use super::super::Transition;
    use super::super::core::Config;
    use super::super::log::EntryPayload;
    use super::super::membership::Configuration;

    fn test_setup() -> (Core, Follower) {
        let config = Config::new(NodeId(0))
            .with_election_timeout(10, 20) // Use smaller values for tests
            .with_prevote(false); // Disable PreVote for existing tests
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let follower = Follower::new(&mut core, None);
        (core, follower)
    }

    #[test]
    fn initial_state() {
        let (_, follower) = test_setup();
        assert!(follower.leader.is_none());
        assert!(follower.election_deadline >= 10);
    }

    #[test]
    fn election_timeout_triggers_candidacy() {
        let (mut core, mut follower) = test_setup();

        // Tick until just before deadline (step increments ticks first)
        while core.ticks + 1 < follower.election_deadline {
            let StepResult { transition, .. } = follower.step(&mut core, Event::Tick);
            assert!(matches!(transition, Transition::Stay));
        }

        // Next tick should trigger transition
        let StepResult { transition, .. } = follower.step(&mut core, Event::Tick);
        assert!(matches!(transition, Transition::ToCandidate));
    }

    #[test]
    fn grants_vote_to_up_to_date_candidate() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { transition, effects } = follower.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));
        assert!(effects.persist);

        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(resp.granted);
        } else {
            panic!("expected VoteResponse");
        }
    }

    #[test]
    fn denies_vote_to_candidate_outside_config() {
        let config = Config::new(NodeId(0)).with_prevote(false);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1)]);
        core.persistent.term = 1;
        let mut follower = Follower::new(&mut core, None);

        let msg = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));
        assert_ne!(core.persistent.voted_for, Some(NodeId(2)));
        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        } else {
            panic!("expected VoteResponse");
        }
    }

    #[test]
    fn follower_with_known_leader_rejects_disruptive_vote() {
        let (mut core, _) = test_setup();
        core.persistent.term = 1;
        let mut follower = Follower::after_leader_contact(&mut core, Some(NodeId(1)));

        let msg = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));
        assert_eq!(core.term(), 1);
        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        } else {
            panic!("expected VoteResponse");
        }
    }

    #[test]
    fn denies_vote_to_stale_candidate() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Give us a log entry at term 1
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![])),
        });

        // Candidate has older log
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));
        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        } else {
            panic!("expected VoteResponse");
        }
    }

    #[test]
    fn votes_only_once_per_term() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Vote for node 1
        let msg1 = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        follower.step(&mut core, Event::Message(msg1));

        // Try to vote for node 2 in same term
        let msg2 = Message {
            from: NodeId(2),
            to: NodeId(0),
            term: 2,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };
        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg2));

        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        }
    }

    #[test]
    fn accepts_append_entries() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        let msg = Message {
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
                leader_commit: 0,
            }),
        };

        let StepResult { transition, effects } = follower.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));
        assert!(effects.persist);
        assert_eq!(core.log().last_index(), 1);
        assert_eq!(follower.leader, Some(NodeId(1)));
    }

    #[test]
    fn rejects_append_with_wrong_prev() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Request with prev_log_index=1, but we have no entries
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));
        if let Payload::AppendResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.success);
        }
    }

    #[test]
    fn timeout_now_triggers_election() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        let msg = Message { from: NodeId(1), to: NodeId(0), term: 1, payload: Payload::TimeoutNow };

        let StepResult { transition, .. } = follower.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToCandidate));
    }

    #[test]
    fn heartbeat_resets_deadline() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Advance time
        for _ in 0..5 {
            follower.step(&mut core, Event::Tick);
        }
        let deadline_before = follower.election_deadline;

        // Receive heartbeat
        let msg = Message {
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
        follower.step(&mut core, Event::Message(msg));

        assert!(follower.election_deadline > deadline_before);
    }

    // --- Snapshot tests ---

    #[test]
    fn accepts_install_snapshot() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Add some entries
        for i in 1..=3 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::InstallSnapshotRequest(InstallSnapshotRequest {
                last_included_index: 5,
                last_included_term: 1,
                configuration: Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]),
            }),
        };

        let StepResult { transition, effects } = follower.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));
        assert!(effects.persist);

        // Log should be cleared and snapshot installed
        assert_eq!(core.log().snapshot_index(), 5);
        assert_eq!(core.log().snapshot_term(), 1);
        assert!(core.log().is_empty());

        // Leader should be set
        assert_eq!(follower.leader, Some(NodeId(1)));

        // Response should be success
        if let Payload::InstallSnapshotResponse(resp) = &effects.messages[0].payload {
            assert!(resp.success);
            assert_eq!(resp.last_included_index, 5);
        } else {
            panic!("expected InstallSnapshotResponse");
        }
    }

    #[test]
    fn install_snapshot_resets_deadline() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 1;

        // Advance time
        for _ in 0..5 {
            follower.step(&mut core, Event::Tick);
        }
        let deadline_before = follower.election_deadline;

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::InstallSnapshotRequest(InstallSnapshotRequest {
                last_included_index: 3,
                last_included_term: 1,
                configuration: Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]),
            }),
        };
        follower.step(&mut core, Event::Message(msg));

        assert!(follower.election_deadline > deadline_before);
    }

    #[test]
    fn rejects_stale_snapshot() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 2;

        // We already have a snapshot at index 5
        core.log_mut().install_snapshot(5, 1, Configuration::simple(vec![NodeId(0)]));

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::InstallSnapshotRequest(InstallSnapshotRequest {
                last_included_index: 3, // Older than our snapshot
                last_included_term: 1,
                configuration: Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]),
            }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));

        // Should not change snapshot
        assert_eq!(core.log().snapshot_index(), 5);

        // Response should indicate not accepted
        if let Payload::InstallSnapshotResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.success);
            assert_eq!(resp.last_included_index, 3);
        } else {
            panic!("expected InstallSnapshotResponse");
        }
    }

    #[test]
    fn rejects_snapshot_that_would_discard_committed_suffix() {
        let (mut core, mut follower) = test_setup();
        core.persistent.term = 2;
        for i in 1..=5 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }
        core.commit_index = 5;

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::InstallSnapshotRequest(InstallSnapshotRequest {
                last_included_index: 3,
                last_included_term: 2,
                configuration: Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]),
            }),
        };

        let StepResult { effects, .. } = follower.step(&mut core, Event::Message(msg));

        assert_eq!(core.log().last_index(), 5);
        if let Payload::InstallSnapshotResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.success);
            assert_eq!(resp.last_included_index, 3);
        } else {
            panic!("expected InstallSnapshotResponse");
        }
    }
}
