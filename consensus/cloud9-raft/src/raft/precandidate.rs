//! `PreCandidate` role (§4.2.3).
//!
//! A pre-candidate:
//! - Does NOT increment term (avoids disrupting the cluster)
//! - Sends `PreVoteRequest` to all peers
//! - Wins pre-election with majority → becomes Candidate
//! - Times out → restarts pre-election (stays `PreCandidate`)
//! - Discovers higher term → becomes Follower
//! - Receives `AppendEntries` from leader → becomes Follower
//!
//! `PreVote` prevents partitioned nodes from incrementing their term and
//! disrupting the cluster when they rejoin. A pre-candidate only proceeds
//! to a real election if it can win the pre-vote (proving it can reach
//! a majority of nodes).

use crate::NodeId;

use super::StepResult;
use super::core::Core;
use super::event::{
    AppendRequest, Effects, Event, Message, Payload, PreVoteRequest, PreVoteResponse, VoteResponse,
};

/// `PreCandidate` role state.
#[derive(Debug, Clone)]
pub struct PreCandidate {
    /// Set of peers who granted their pre-vote (including self).
    pub pre_votes: std::collections::BTreeSet<NodeId>,
    /// Tick at which to restart pre-election.
    pub election_deadline: u64,
    /// Tick when we last heard from a leader (for disruptive-vote guard).
    pub last_contact_tick: u64,
}

impl PreCandidate {
    /// Create a new pre-candidate, starting a pre-election.
    ///
    /// Unlike `Candidate::new()`, this does NOT increment the term.
    /// It sends `PreVoteRequest` to check if an election would succeed.
    pub fn new(core: &mut Core) -> (Self, Effects) {
        let timeout = core.random_election_timeout();
        let mut precandidate = Self {
            pre_votes: std::collections::BTreeSet::new(),
            election_deadline: core.ticks + timeout,
            last_contact_tick: core.ticks.saturating_sub(core.config.election_timeout.0),
        };

        // A server outside its latest configuration may run pre-vote, but it
        // does not own a vote in that configuration.
        if core.is_voter() {
            precandidate.pre_votes.insert(core.id());
        }

        let config = core.effective_config();

        // Single-node cluster: immediate win
        if config.voter_count() == 1 && config.is_voter(core.id()) {
            return (precandidate, Effects::none());
        }

        // Send PreVoteRequest to all peers
        // The "next_term" is what we would use if we become a real candidate
        let req = PreVoteRequest {
            next_term: core.term() + 1,
            last_log_index: core.log().last_index(),
            last_log_term: core.log().last_term(),
        };

        let peers: Vec<_> = config.voter_peers(core.id()).collect();
        let messages: Vec<_> = peers
            .into_iter()
            .map(|peer| Message {
                from: core.id(),
                to: peer,
                term: core.term(), // Use current term, not incremented
                payload: Payload::PreVoteRequest(req),
            })
            .collect();

        (precandidate, Effects::none().with_messages(messages))
    }

    /// Ticks until election timeout.
    pub fn ticks_until_deadline(&self, ticks: u64) -> u64 {
        self.election_deadline.saturating_sub(ticks)
    }

    /// Check if we have enough pre-votes to proceed.
    pub fn has_quorum(&self, core: &Core) -> bool {
        let config = core.effective_config();
        config.has_quorum(|voter| self.pre_votes.contains(&voter))
    }

    /// Process an event. Returns transition (if any) and effects.
    pub fn step(&mut self, core: &mut Core, event: Event) -> StepResult {
        match event {
            Event::Tick => self.handle_tick(core),
            // DiskWriteComplete is only relevant for leader
            Event::DiskWriteComplete(_) => StepResult::none(),
            Event::Message(msg) => {
                // Handle pre-vote requests - use same disruptive vote guard
                if let Payload::PreVoteRequest(_) | Payload::VoteRequest(_) = msg.payload
                    && self.recently_heard_from_leader(core)
                {
                    return StepResult::stay(Self::deny_vote(core, &msg));
                }

                // Higher term: become follower
                if msg.term > core.term() {
                    core.maybe_update_term(msg.term);
                    let leader =
                        matches!(msg.payload, Payload::AppendRequest(_)).then_some(msg.from);
                    return StepResult::to_follower(leader, Effects::none().with_persist());
                }

                match msg.payload {
                    Payload::PreVoteResponse(resp) => {
                        self.handle_prevote_response(core, msg.from, resp)
                    }
                    Payload::AppendRequest(req) => self.handle_append_request(core, msg.from, req),
                    Payload::PreVoteRequest(req) => {
                        Self::handle_prevote_request(core, msg.from, msg.term, req)
                    }
                    Payload::VoteRequest(_) => Self::handle_vote_request(core, msg.from),
                    Payload::TimeoutNow => {
                        // Skip pre-vote, go directly to candidate
                        StepResult::to_candidate(Effects::none())
                    }
                    _ => StepResult::none(),
                }
            }
        }
    }

    fn handle_tick(&mut self, core: &mut Core) -> StepResult {
        core.ticks += 1;
        if self.recently_heard_from_leader(core) {
            // Stay pre-candidate but defer restart while leader is active
            self.election_deadline = core.ticks + core.random_election_timeout();
            return StepResult::none();
        }
        if core.ticks >= self.election_deadline {
            // Pre-election timeout: restart pre-election
            StepResult::to_precandidate(Effects::none())
        } else {
            StepResult::none()
        }
    }

    fn handle_prevote_response(
        &mut self,
        core: &mut Core,
        from: NodeId,
        resp: PreVoteResponse,
    ) -> StepResult {
        // If responder has higher term, step down
        if resp.term > core.term() {
            core.maybe_update_term(resp.term);
            return StepResult::to_follower(None, Effects::none().with_persist());
        }
        if resp.term < core.term() {
            return StepResult::none();
        }

        if !resp.granted {
            return StepResult::none();
        }

        // Record pre-vote
        self.pre_votes.insert(from);

        // Check for victory - proceed to real election
        if self.has_quorum(core) {
            StepResult::to_candidate(Effects::none())
        } else {
            StepResult::none()
        }
    }

    fn handle_append_request(
        &mut self,
        core: &Core,
        from: NodeId,
        _req: AppendRequest,
    ) -> StepResult {
        // Leader exists: step down
        self.last_contact_tick = core.ticks;
        StepResult::to_follower_after_contact(Some(from), Effects::none())
    }

    fn handle_prevote_request(
        core: &Core,
        from: NodeId,
        msg_term: u64,
        req: PreVoteRequest,
    ) -> StepResult {
        // Grant pre-vote if:
        // 1. Candidate's next_term is at least our term + 1
        // 2. Candidate's log is at least as up-to-date as ours
        let dominated = core.log_is_up_to_date(req.last_log_term, req.last_log_index);
        let candidate_is_voter = core.effective_config().is_voter(from);
        let term_ok = req.next_term > core.term();
        let granted = candidate_is_voter && term_ok && dominated;

        let resp = Message {
            from: core.id(),
            to: from,
            term: msg_term,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: core.term(), granted }),
        };
        StepResult::stay(Effects::none().with_message(resp))
    }

    fn handle_vote_request(core: &Core, from: NodeId) -> StepResult {
        // We're pre-candidate, deny real votes
        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: false }),
        };
        StepResult::stay(Effects::none().with_message(resp))
    }

    fn deny_vote(core: &Core, msg: &Message) -> Effects {
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

    fn recently_heard_from_leader(&self, core: &Core) -> bool {
        let min_timeout = core.config.election_timeout.0;
        core.ticks.saturating_sub(self.last_contact_tick) < min_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::Transition;
    use super::super::core::Config;

    fn test_setup() -> (Core, PreCandidate, Effects) {
        let config = Config::new(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let (precandidate, effects) = PreCandidate::new(&mut core);
        (core, precandidate, effects)
    }

    #[test]
    fn new_precandidate_does_not_increment_term() {
        let (core, _, _) = test_setup();
        // Term should remain 0 (not incremented like Candidate)
        assert_eq!(core.term(), 0);
    }

    #[test]
    fn new_precandidate_prevotes_for_self() {
        let (_, precandidate, _) = test_setup();
        assert_eq!(precandidate.pre_votes.len(), 1);
        assert!(precandidate.pre_votes.contains(&NodeId(0)));
    }

    #[test]
    fn non_voter_precandidate_does_not_prevote_for_self() {
        let config = Config::new(NodeId(0)).with_election_timeout(10, 20);
        let mut core = Core::new(config, &[NodeId(1), NodeId(2)]);
        let (precandidate, effects) = PreCandidate::new(&mut core);

        assert!(!precandidate.pre_votes.contains(&NodeId(0)));
        assert!(!precandidate.has_quorum(&core));
        assert_eq!(effects.messages.len(), 2);
    }

    #[test]
    fn new_precandidate_sends_prevote_requests() {
        let (core, _, effects) = test_setup();
        assert_eq!(effects.messages.len(), 2); // To peers 1 and 2

        for msg in &effects.messages {
            assert!(matches!(msg.payload, Payload::PreVoteRequest(_)));
            assert_eq!(msg.from, NodeId(0));
            assert_eq!(msg.term, core.term()); // Uses current term, not incremented
            if let Payload::PreVoteRequest(req) = &msg.payload {
                assert_eq!(req.next_term, 1); // Would use term 1 if elected
            }
        }
    }

    #[test]
    fn single_node_prevote_immediate() {
        let config = Config::new(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0)]);
        let (precandidate, effects) = PreCandidate::new(&mut core);

        // Already has quorum (1/1)
        assert!(precandidate.has_quorum(&core));
        assert!(effects.messages.is_empty());
    }

    #[test]
    fn proceeds_to_candidate_with_majority() {
        let (mut core, mut precandidate, _) = test_setup();

        // Receive pre-vote from peer 1
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 0,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: 0, granted: true }),
        };

        let StepResult { transition, .. } = precandidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToCandidate));
    }

    #[test]
    fn ignores_stale_prevote_response() {
        let (mut core, mut precandidate, _) = test_setup();
        core.persistent.term = 5;

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 4,
            payload: Payload::PreVoteResponse(PreVoteResponse { term: 4, granted: true }),
        };

        let StepResult { transition, .. } = precandidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));
        assert_eq!(precandidate.pre_votes.len(), 1);
    }

    #[test]
    fn prevote_timeout_restarts_prevote() {
        let (mut core, mut precandidate, _) = test_setup();

        // Tick until just before deadline
        while core.ticks + 1 < precandidate.election_deadline {
            let StepResult { transition, .. } = precandidate.step(&mut core, Event::Tick);
            assert!(matches!(transition, Transition::Stay));
        }

        // Next tick triggers new pre-election
        let StepResult { transition, .. } = precandidate.step(&mut core, Event::Tick);
        assert!(matches!(transition, Transition::ToPreCandidate));
    }

    #[test]
    fn steps_down_on_higher_term() {
        let (mut core, mut precandidate, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 5, // Higher than our term 0
            payload: Payload::PreVoteResponse(PreVoteResponse { term: 5, granted: false }),
        };

        let StepResult { transition, effects } = precandidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollower(None)));
        assert!(effects.persist);
        assert_eq!(core.term(), 5);
    }

    #[test]
    fn steps_down_on_append_from_leader() {
        let (mut core, mut precandidate, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 0,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let StepResult { transition, .. } = precandidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollowerAfterContact(Some(NodeId(1)))));
    }

    #[test]
    fn timeout_now_skips_to_candidate() {
        let (mut core, mut precandidate, _) = test_setup();

        let msg = Message { from: NodeId(1), to: NodeId(0), term: 0, payload: Payload::TimeoutNow };

        let StepResult { transition, .. } = precandidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToCandidate));
    }
}
