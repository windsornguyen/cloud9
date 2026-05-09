//! Candidate role.
//!
//! A candidate:
//! - Increments term and votes for self on entry
//! - Sends `RequestVote` to all peers
//! - Wins election with majority votes → becomes Leader
//! - Times out → restarts election (stays Candidate)
//! - Discovers higher term → becomes Follower
//! - Receives `AppendEntries` from leader → becomes Follower
//!
//! Candidate-specific state:
//! - `votes`: set of peers who voted for us
//! - `election_deadline`: when to restart election

use crate::NodeId;

use super::StepResult;
use super::core::Core;
use super::event::{
    AppendRequest, AppendResponse, Effects, Event, Message, Payload, PreVoteRequest,
    PreVoteResponse, VoteRequest, VoteResponse,
};

/// Candidate role state.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Set of peers who granted their vote (including self).
    pub votes: std::collections::BTreeSet<NodeId>,
    /// Tick at which to restart election.
    pub election_deadline: u64,
    /// Tick when we last heard from a leader (for disruptive-vote guard).
    pub last_contact_tick: u64,
}

impl Candidate {
    /// Create a new candidate, starting an election.
    ///
    /// This increments the term, votes for self, and sends `RequestVote` to all peers.
    pub fn new(core: &mut Core) -> (Self, Effects) {
        core.start_election();

        let timeout = core.random_election_timeout();
        let mut candidate = Self {
            votes: std::collections::BTreeSet::new(),
            election_deadline: core.ticks + timeout,
            last_contact_tick: core.ticks.saturating_sub(core.config.election_timeout.0),
        };

        // A server outside its latest configuration may campaign, but it
        // does not own a vote in that configuration.
        if core.is_voter() {
            candidate.votes.insert(core.id());
        }

        let config = core.effective_config();

        // Single-node cluster: immediate win
        if config.voter_count() == 1 && config.is_voter(core.id()) {
            return (candidate, Effects::none().with_persist());
        }

        // Send RequestVote to all peers
        let req = VoteRequest {
            last_log_index: core.log().last_index(),
            last_log_term: core.log().last_term(),
        };

        let peers: Vec<_> = config.voter_peers(core.id()).collect();
        let messages: Vec<_> = peers
            .into_iter()
            .map(|peer| Message {
                from: core.id(),
                to: peer,
                term: core.term(),
                payload: Payload::VoteRequest(req),
            })
            .collect();

        (candidate, Effects::none().with_persist().with_messages(messages))
    }

    /// Ticks until election timeout.
    pub fn ticks_until_deadline(&self, ticks: u64) -> u64 {
        self.election_deadline.saturating_sub(ticks)
    }

    /// Check if we have enough votes to win.
    ///
    /// For joint consensus, requires majorities from both old and new configs.
    pub fn has_quorum(&self, core: &Core) -> bool {
        let config = core.effective_config();
        config.has_quorum(|voter| self.votes.contains(&voter))
    }

    /// Process an event. Returns transition (if any) and effects.
    pub fn step(&mut self, core: &mut Core, event: Event) -> StepResult {
        match event {
            Event::Tick => self.handle_tick(core),
            // DiskWriteComplete is only relevant for leader
            Event::DiskWriteComplete(_) => StepResult::none(),
            Event::Message(msg) => {
                if matches!(msg.payload, Payload::VoteRequest(_) | Payload::PreVoteRequest(_))
                    && self.recently_heard_from_leader(core)
                {
                    // Deny vote without updating term to avoid disruptive elections.
                    return StepResult::stay(Self::deny_vote(core, &msg));
                }

                // PreVoteRequest doesn't update term (handled separately)
                if let Payload::PreVoteRequest(req) = msg.payload {
                    return StepResult::stay(Self::handle_prevote_request(
                        core, msg.from, msg.term, req,
                    ));
                }

                // Higher term: become follower
                if msg.term > core.term() {
                    core.maybe_update_term(msg.term);
                    let leader =
                        matches!(msg.payload, Payload::AppendRequest(_)).then_some(msg.from);
                    return StepResult::to_follower(leader, Effects::none().with_persist());
                }

                // Stale term: reject
                if msg.term < core.term() {
                    return StepResult::stay(Self::reject_stale(core, &msg));
                }

                match msg.payload {
                    Payload::VoteResponse(resp) => self.handle_vote_response(core, msg.from, resp),
                    Payload::AppendRequest(req) => self.handle_append_request(core, msg.from, req),
                    Payload::VoteRequest(_) => Self::handle_vote_request(core, msg.from),
                    Payload::TimeoutNow => {
                        // Already candidate, restart election
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
            // Stay candidate but defer election restart while leader is active
            self.election_deadline = core.ticks + core.random_election_timeout();
            return StepResult::none();
        }
        if core.ticks >= self.election_deadline {
            // Election timeout: start new election
            StepResult::to_candidate(Effects::none())
        } else {
            StepResult::none()
        }
    }

    fn handle_vote_response(
        &mut self,
        core: &mut Core,
        from: NodeId,
        resp: VoteResponse,
    ) -> StepResult {
        if !resp.granted {
            return StepResult::none();
        }

        // Record vote
        self.votes.insert(from);

        // Check for victory
        if self.has_quorum(core) {
            StepResult::to_leader(Effects::none())
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
        // Another leader exists for this term: step down
        // (This shouldn't happen if election safety holds, but handle gracefully)
        self.last_contact_tick = core.ticks;
        StepResult::to_follower_after_contact(Some(from), Effects::none())
    }

    fn handle_vote_request(core: &Core, from: NodeId) -> StepResult {
        // We already voted for ourselves this term
        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: false }),
        };
        StepResult::stay(Effects::none().with_message(resp))
    }

    fn recently_heard_from_leader(&self, core: &Core) -> bool {
        let min_timeout = core.config.election_timeout.0;
        core.ticks.saturating_sub(self.last_contact_tick) < min_timeout
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

    fn handle_prevote_request(
        core: &Core,
        from: NodeId,
        msg_term: u64,
        req: PreVoteRequest,
    ) -> Effects {
        // As a candidate, we've already voted for ourselves and incremented term.
        // Grant pre-vote only if their next_term is higher than ours and log is up-to-date.
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

    fn reject_stale(core: &Core, msg: &Message) -> Effects {
        let payload = match &msg.payload {
            Payload::VoteRequest(_) => Payload::VoteResponse(VoteResponse { granted: false }),
            Payload::AppendRequest(_) => Payload::AppendResponse(AppendResponse {
                success: false,
                last_log_index: core.log().last_index(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::Transition;
    use super::super::core::Config;
    use super::super::log::{Entry, EntryPayload};
    use crate::Command;

    fn test_setup() -> (Core, Candidate, Effects) {
        let config = Config::new(NodeId(0)).with_prevote(false);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        let (candidate, effects) = Candidate::new(&mut core);
        (core, candidate, effects)
    }

    #[test]
    fn new_candidate_increments_term() {
        let (core, _, _) = test_setup();
        assert_eq!(core.term(), 1);
    }

    #[test]
    fn new_candidate_votes_for_self() {
        let (core, candidate, _) = test_setup();
        assert_eq!(core.persistent.voted_for, Some(NodeId(0)));
        assert_eq!(candidate.votes.len(), 1);
    }

    #[test]
    fn non_voter_candidate_does_not_vote_for_self() {
        let config = Config::new(NodeId(0));
        let mut core = Core::new(config, &[NodeId(1), NodeId(2)]);
        let (candidate, effects) = Candidate::new(&mut core);

        assert_eq!(core.persistent.voted_for, None);
        assert!(!candidate.votes.contains(&NodeId(0)));
        assert!(!candidate.has_quorum(&core));
        assert_eq!(effects.messages.len(), 2);
    }

    #[test]
    fn new_candidate_sends_vote_requests() {
        let (_, _, effects) = test_setup();
        assert!(effects.persist);
        assert_eq!(effects.messages.len(), 2); // To peers 1 and 2

        for msg in &effects.messages {
            assert!(matches!(msg.payload, Payload::VoteRequest(_)));
            assert_eq!(msg.from, NodeId(0));
        }
    }

    #[test]
    fn single_node_wins_immediately() {
        let config = Config::new(NodeId(0));
        let mut core = Core::new(config, &[NodeId(0)]);
        let (candidate, effects) = Candidate::new(&mut core);

        // Already has quorum (1/1)
        assert!(candidate.has_quorum(&core));
        assert!(effects.messages.is_empty());
    }

    #[test]
    fn wins_with_majority() {
        let (mut core, mut candidate, _) = test_setup();

        // Receive vote from peer 1
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteResponse(VoteResponse { granted: true }),
        };

        let StepResult { transition, .. } = candidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToLeader));
    }

    #[test]
    fn election_timeout_restarts_election() {
        let (mut core, mut candidate, _) = test_setup();

        // Tick until just before deadline (step increments ticks first)
        while core.ticks + 1 < candidate.election_deadline {
            let StepResult { transition, .. } = candidate.step(&mut core, Event::Tick);
            assert!(matches!(transition, Transition::Stay));
        }

        // Next tick triggers new election
        let StepResult { transition, .. } = candidate.step(&mut core, Event::Tick);
        assert!(matches!(transition, Transition::ToCandidate));
    }

    #[test]
    fn steps_down_on_higher_term() {
        let (mut core, mut candidate, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 5, // Higher than our term 1
            payload: Payload::VoteResponse(VoteResponse { granted: false }),
        };

        let StepResult { transition, effects } = candidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollower(None)));
        assert!(effects.persist);
        assert_eq!(core.term(), 5);
    }

    #[test]
    fn higher_term_append_does_not_ack_without_consistency_check() {
        let (mut core, mut candidate, _) = test_setup();
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 2,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 1,
                prev_log_term: 2,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let StepResult { transition, effects } = candidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollower(Some(NodeId(1)))));
        assert!(effects.persist);
        assert!(effects.messages.is_empty());
    }

    #[test]
    fn steps_down_on_append_from_leader() {
        let (mut core, mut candidate, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1, // Same term
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let StepResult { transition, .. } = candidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollowerAfterContact(Some(NodeId(1)))));
    }

    #[test]
    fn denies_vote_to_other_candidates() {
        let (mut core, mut candidate, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { transition, effects } = candidate.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));

        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        }
    }

    #[test]
    fn ignores_duplicate_votes() {
        let (mut core, mut candidate, _) = test_setup();

        // Same peer votes twice
        for _ in 0..2 {
            let msg = Message {
                from: NodeId(1),
                to: NodeId(0),
                term: 1,
                payload: Payload::VoteResponse(VoteResponse { granted: true }),
            };
            candidate.step(&mut core, Event::Message(msg));
        }

        // Still only 2 votes (self + peer 1)
        assert_eq!(candidate.votes.len(), 2);
    }
}
