//! Property-based testing for Raft consensus.
#![allow(clippy::cast_possible_truncation)]
//!
//! Uses proptest to verify invariants hold under random event sequences.
//!
//! Run with: `cargo test --test property`

use std::collections::BTreeMap;

use proptest::prelude::*;

use cloud9_raft::raft::{
    AppendRequest, AppendResponse, Config, Event, Message, Payload, PreVoteRequest,
    PreVoteResponse, RaftNode, VoteRequest, VoteResponse,
};
use cloud9_raft::{LogIndex, NodeId, Term};

// --- Test Configuration ---

const TEST_NODES: &[NodeId] = &[NodeId(0), NodeId(1), NodeId(2)];

fn test_config(id: NodeId) -> Config {
    Config::new(id).with_prevote(false).with_election_timeout(5, 10)
}

// --- Arbitrary Generators ---

fn arb_node_id() -> impl Strategy<Value = NodeId> {
    (0..TEST_NODES.len()).prop_map(|i| TEST_NODES[i])
}

fn arb_term() -> impl Strategy<Value = Term> {
    0u64..10
}

fn arb_log_index() -> impl Strategy<Value = LogIndex> {
    0u64..20
}

fn arb_vote_request() -> impl Strategy<Value = VoteRequest> {
    (arb_log_index(), arb_term())
        .prop_map(|(last_log_index, last_log_term)| VoteRequest { last_log_index, last_log_term })
}

fn arb_vote_response() -> impl Strategy<Value = VoteResponse> {
    any::<bool>().prop_map(|granted| VoteResponse { granted })
}

fn arb_prevote_request() -> impl Strategy<Value = PreVoteRequest> {
    (arb_term(), arb_log_index(), arb_term()).prop_map(
        |(next_term, last_log_index, last_log_term)| PreVoteRequest {
            next_term,
            last_log_index,
            last_log_term,
        },
    )
}

fn arb_prevote_response() -> impl Strategy<Value = PreVoteResponse> {
    (arb_term(), any::<bool>()).prop_map(|(term, granted)| PreVoteResponse { term, granted })
}

fn arb_append_request() -> impl Strategy<Value = AppendRequest> {
    (arb_log_index(), arb_term(), arb_log_index()).prop_map(
        |(prev_log_index, prev_log_term, leader_commit)| AppendRequest {
            prev_log_index,
            prev_log_term,
            entries: vec![],
            leader_commit,
        },
    )
}

fn arb_append_response() -> impl Strategy<Value = AppendResponse> {
    (any::<bool>(), arb_log_index())
        .prop_map(|(success, last_log_index)| AppendResponse { success, last_log_index })
}

fn arb_payload() -> impl Strategy<Value = Payload> {
    prop_oneof![
        arb_vote_request().prop_map(Payload::VoteRequest),
        arb_vote_response().prop_map(Payload::VoteResponse),
        arb_prevote_request().prop_map(Payload::PreVoteRequest),
        arb_prevote_response().prop_map(Payload::PreVoteResponse),
        arb_append_request().prop_map(Payload::AppendRequest),
        arb_append_response().prop_map(Payload::AppendResponse),
        Just(Payload::TimeoutNow),
    ]
}

fn arb_message() -> impl Strategy<Value = Message> {
    (arb_node_id(), arb_node_id(), arb_term(), arb_payload())
        .prop_map(|(from, to, term, payload)| Message { from, to, term, payload })
}

fn arb_event() -> impl Strategy<Value = Event> {
    prop_oneof![Just(Event::Tick), arb_message().prop_map(Event::Message),]
}

// --- Invariant Checks ---

fn check_term_monotonicity(node: &RaftNode, previous_term: Term) -> bool {
    node.term() >= previous_term
}

fn check_commit_bound(node: &RaftNode) -> bool {
    node.commit_index() <= node.persistent().log.last_index()
}

fn check_vote_invariant(node: &RaftNode) -> bool {
    match node.persistent().voted_for {
        Some(voted) => TEST_NODES.contains(&voted) || voted == node.id(),
        None => true,
    }
}

fn check_log_continuity(node: &RaftNode) -> bool {
    let log = &node.persistent().log;
    for i in 1..=log.last_index() {
        if log.get(i).is_none() {
            return false;
        }
        if let Some(entry) = log.get(i)
            && entry.index != i
        {
            return false;
        }
    }
    true
}

// --- Single Node Property Tests ---

proptest! {
    #[test]
    fn single_node_invariants(events in prop::collection::vec(arb_event(), 0..50)) {
        let mut node = RaftNode::new(test_config(NodeId(0)), TEST_NODES);
        let mut prev_term = node.term();

        for event in events {
            match event {
                Event::Tick => { node.tick(); }
                Event::Message(msg) => { node.step(msg); }
                Event::DiskWriteComplete(_) => { /* Only relevant for leader */ }
            }

            prop_assert!(check_term_monotonicity(&node, prev_term),
                "Term decreased: {} -> {}", prev_term, node.term());
            prop_assert!(check_commit_bound(&node),
                "Commit index {} exceeds log length {}",
                node.commit_index(), node.persistent().log.last_index());
            prop_assert!(check_vote_invariant(&node),
                "Invalid voted_for state");
            prop_assert!(check_log_continuity(&node),
                "Log has gaps or index mismatch");

            prev_term = node.term();
        }
    }

    #[test]
    fn term_never_decreases(events in prop::collection::vec(arb_event(), 0..100)) {
        let mut node = RaftNode::new(test_config(NodeId(0)), TEST_NODES);
        let mut max_term = 0;

        for event in events {
            match event {
                Event::Tick => { node.tick(); }
                Event::Message(msg) => { node.step(msg); }
                Event::DiskWriteComplete(_) => { /* Only relevant for leader */ }
            }

            prop_assert!(node.term() >= max_term,
                "Term decreased from {} to {}", max_term, node.term());
            max_term = max_term.max(node.term());
        }
    }

    #[test]
    fn vote_once_per_term(candidates in prop::collection::vec(arb_node_id(), 1..5)) {
        let mut node = RaftNode::new(test_config(NodeId(0)), TEST_NODES);
        let mut votes_granted: BTreeMap<Term, NodeId> = BTreeMap::new();

        for candidate in candidates {
            let msg = Message {
                from: candidate,
                to: NodeId(0),
                term: 1,
                payload: Payload::VoteRequest(VoteRequest {
                    last_log_index: 0,
                    last_log_term: 0,
                }),
            };
            let effects = node.step(msg);

            for response in &effects.messages {
                if let Payload::VoteResponse(VoteResponse { granted: true }) = response.payload {
                    if let Some(prev_candidate) = votes_granted.get(&1) {
                        prop_assert_eq!(*prev_candidate, candidate,
                            "Voted for multiple candidates in same term");
                    } else {
                        votes_granted.insert(1, candidate);
                    }
                }
            }
        }
    }
}

// --- Cluster Property Tests ---

struct ClusterSim {
    nodes: Vec<RaftNode>,
    pending_messages: Vec<Message>,
}

impl ClusterSim {
    fn new(node_count: usize) -> Self {
        let voters: Vec<_> = (0..node_count).map(|i| NodeId(i as u64)).collect();
        let nodes = voters
            .iter()
            .map(|&id| {
                RaftNode::new(
                    Config::new(id).with_prevote(false).with_election_timeout(5, 10),
                    &voters,
                )
            })
            .collect();
        Self { nodes, pending_messages: vec![] }
    }

    fn tick_all(&mut self) {
        for node in &mut self.nodes {
            let effects = node.tick();
            self.pending_messages.extend(effects.messages);
        }
    }

    fn deliver_all(&mut self) {
        let messages = std::mem::take(&mut self.pending_messages);
        for msg in messages {
            if (msg.to.0 as usize) < self.nodes.len() {
                let effects = self.nodes[msg.to.0 as usize].step(msg);
                self.pending_messages.extend(effects.messages);
            }
        }
    }

    fn run_ticks(&mut self, count: usize) {
        for _ in 0..count {
            self.tick_all();
            self.deliver_all();
        }
    }

    fn leaders(&self) -> Vec<(NodeId, Term)> {
        self.nodes.iter().filter(|n| n.is_leader()).map(|n| (n.id(), n.term())).collect()
    }

    fn election_safety(&self) -> bool {
        let leaders = self.leaders();
        let mut terms_seen = std::collections::HashSet::new();
        for (_, term) in &leaders {
            if terms_seen.contains(term) {
                return false;
            }
            terms_seen.insert(*term);
        }
        true
    }
}

proptest! {
    #[test]
    fn cluster_election_safety(ticks in 10usize..100) {
        let mut cluster = ClusterSim::new(3);
        cluster.run_ticks(ticks);

        prop_assert!(cluster.election_safety(),
            "Election safety violated: multiple leaders in same term");
    }

    #[test]
    fn cluster_elects_leader(seed in any::<u64>()) {
        let _ = seed;
        let mut cluster = ClusterSim::new(3);
        cluster.run_ticks(50);

        let leaders = cluster.leaders();
        prop_assert!(!leaders.is_empty(), "No leader elected after 50 ticks");
    }
}
