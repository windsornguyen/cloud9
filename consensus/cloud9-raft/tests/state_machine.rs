//! State machine testing for Raft consensus.
#![allow(clippy::cast_possible_truncation, clippy::match_same_arms)]
//!
//! Uses proptest-state-machine to verify Raft properties by generating
//! random sequences of transitions and checking invariants after each step.
//!
//! Run with: `cargo test --test state_machine`
//! Verbose:  `PROPTEST_VERBOSE=1 cargo test --test state_machine -- --nocapture`

use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::test_runner::Config;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

use cloud9_raft::raft::{Config as RaftConfig, Effects, Message, RaftNode};
use cloud9_raft::{NodeId, Term};

// --- Configuration ---

const CLUSTER_SIZE: usize = 3;
const MAX_TERM: Term = 5;
const MAX_LOG_LEN: usize = 5;

fn node_ids() -> Vec<NodeId> {
    (0..CLUSTER_SIZE).map(|i| NodeId(i as u64)).collect()
}

fn raft_config(id: NodeId) -> RaftConfig {
    RaftConfig::new(id)
        .with_prevote(false) // Simpler state space
        .with_election_timeout(5, 10)
        .with_heartbeat_interval(3)
}

// --- Transitions ---

#[derive(Clone, Debug)]
pub enum Transition {
    /// Tick a specific node.
    Tick(usize),
    /// Deliver a message to its recipient.
    DeliverMessage(usize),
    /// Drop a message (network loss).
    DropMessage(usize),
    /// Propose a command on a node.
    Propose(usize),
}

// --- Reference State Machine ---

/// Abstract reference model of a Raft cluster.
///
/// Tracks bounds for the state machine test.
/// The SUT (`RaftClusterSUT`) is the source of truth; this just tracks
/// what transitions are valid.
#[derive(Clone, Debug)]
pub struct RaftClusterRef {
    /// Upper bound on terms seen.
    max_term: Term,
    /// Upper bound on log lengths.
    max_log_len: usize,
    /// Whether any node is leader.
    has_leader: bool,
    /// Number of pending messages.
    pending_count: usize,
}

impl RaftClusterRef {
    fn new() -> Self {
        Self { max_term: 0, max_log_len: 0, has_leader: false, pending_count: 0 }
    }
}

pub struct RaftStateMachine;

impl ReferenceStateMachine for RaftStateMachine {
    type State = RaftClusterRef;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RaftClusterRef::new()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let mut strategies: Vec<BoxedStrategy<Transition>> = vec![];

        // Tick any node (if we haven't exceeded term limit)
        if state.max_term < MAX_TERM {
            strategies.push((0..CLUSTER_SIZE).prop_map(Transition::Tick).boxed());
        }

        // Deliver or drop pending message
        if state.pending_count > 0 {
            strategies.push((0..state.pending_count).prop_map(Transition::DeliverMessage).boxed());
            strategies.push((0..state.pending_count).prop_map(Transition::DropMessage).boxed());
        }

        // Propose on a node (if log isn't too long and there's a leader)
        if state.max_log_len < MAX_LOG_LEN && state.has_leader {
            strategies.push((0..CLUSTER_SIZE).prop_map(Transition::Propose).boxed());
        }

        // If no strategies, just tick
        if strategies.is_empty() {
            return Just(Transition::Tick(0)).boxed();
        }

        proptest::strategy::Union::new(strategies).boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Tick(_) => {
                // Ticks may increase term
            }
            Transition::Propose(_) => {
                // Proposals may increase log length
            }
            Transition::DeliverMessage(_) | Transition::DropMessage(_) => {
                if state.pending_count > 0 {
                    state.pending_count -= 1;
                }
            }
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Tick(idx) => *idx < CLUSTER_SIZE && state.max_term < MAX_TERM,
            Transition::DeliverMessage(idx) | Transition::DropMessage(idx) => {
                *idx < state.pending_count
            }
            Transition::Propose(idx) => *idx < CLUSTER_SIZE && state.max_log_len < MAX_LOG_LEN,
        }
    }
}

// --- System Under Test ---

/// The actual Raft cluster being tested.
pub struct RaftClusterSUT {
    nodes: Vec<RaftNode>,
    pending_messages: Vec<Message>,
}

impl RaftClusterSUT {
    fn new() -> Self {
        let voters = node_ids();
        let nodes = voters.iter().map(|&id| RaftNode::new(raft_config(id), &voters)).collect();
        Self { nodes, pending_messages: vec![] }
    }

    fn process_effects(&mut self, effects: &Effects) {
        self.pending_messages.extend(effects.messages.iter().cloned());
    }

    fn leaders(&self) -> Vec<(usize, Term)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_leader())
            .map(|(i, n)| (i, n.term()))
            .collect()
    }

    fn leaders_by_term(&self) -> BTreeMap<Term, Vec<usize>> {
        let mut by_term: BTreeMap<Term, Vec<usize>> = BTreeMap::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if node.is_leader() {
                by_term.entry(node.term()).or_default().push(i);
            }
        }
        by_term
    }
}

impl StateMachineTest for RaftClusterSUT {
    type SystemUnderTest = Self;
    type Reference = RaftStateMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        Self::new()
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::Tick(idx) => {
                if idx < state.nodes.len() {
                    let effects = state.nodes[idx].tick();
                    state.process_effects(&effects);
                }
            }
            Transition::DeliverMessage(idx) => {
                if idx < state.pending_messages.len() {
                    let msg = state.pending_messages.remove(idx);
                    let to_idx = msg.to.0 as usize;
                    if to_idx < state.nodes.len() {
                        let effects = state.nodes[to_idx].step(msg);
                        state.process_effects(&effects);
                    }
                }
            }
            Transition::DropMessage(idx) => {
                if idx < state.pending_messages.len() {
                    state.pending_messages.remove(idx);
                }
            }
            Transition::Propose(idx) => {
                if idx < state.nodes.len()
                    && state.nodes[idx].is_leader()
                    && let Ok((_, effects)) = state.nodes[idx].propose(cloud9_raft::Command(vec![]))
                {
                    state.process_effects(&effects);
                }
            }
        }
        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // --- Election Safety ---
        // At most one leader per term
        let leaders_by_term = state.leaders_by_term();
        for (term, leaders) in &leaders_by_term {
            assert!(
                leaders.len() <= 1,
                "Election safety violated: term {} has {} leaders: {:?}",
                term,
                leaders.len(),
                leaders
            );
        }

        // --- Term Monotonicity ---
        // Each node's term should be non-negative (trivially true for u64)
        for node in &state.nodes {
            assert!(node.term() <= MAX_TERM + 10, "Term exceeded expected bounds");
        }

        // --- Commit Index Bounds ---
        // Commit index should never exceed log length
        for (i, node) in state.nodes.iter().enumerate() {
            let log_len = node.persistent().log.last_index();
            assert!(
                node.commit_index() <= log_len,
                "Node {} commit_index {} exceeds log length {}",
                i,
                node.commit_index(),
                log_len
            );
        }

        // --- Log Matching ---
        // If two nodes have entries with same index and term, all previous entries match
        for i in 0..state.nodes.len() {
            for j in (i + 1)..state.nodes.len() {
                let log_i = &state.nodes[i].persistent().log;
                let log_j = &state.nodes[j].persistent().log;

                let min_len = log_i.last_index().min(log_j.last_index());
                for idx in 1..=min_len {
                    let term_i = log_i.term_at(idx);
                    let term_j = log_j.term_at(idx);

                    if term_i == term_j && term_i > 0 {
                        // Same term at same index: check all previous entries match
                        for prev_idx in 1..idx {
                            let prev_term_i = log_i.term_at(prev_idx);
                            let prev_term_j = log_j.term_at(prev_idx);
                            assert_eq!(
                                prev_term_i, prev_term_j,
                                "Log matching violated: nodes {i} and {j} have same term {term_i} at index {idx}, \
                                 but different terms at index {prev_idx} ({prev_term_i} vs {prev_term_j})"
                            );
                        }
                    }
                }
            }
        }

        // --- State Machine Safety ---
        // Committed entries at same index must have same term
        for i in 0..state.nodes.len() {
            for j in (i + 1)..state.nodes.len() {
                let commit_i = state.nodes[i].commit_index();
                let commit_j = state.nodes[j].commit_index();
                let min_commit = commit_i.min(commit_j);

                for idx in 1..=min_commit {
                    let term_i = state.nodes[i].persistent().log.term_at(idx);
                    let term_j = state.nodes[j].persistent().log.term_at(idx);
                    assert_eq!(
                        term_i, term_j,
                        "State machine safety violated: committed entries at index {idx} \
                         have different terms: node {i} has term {term_i}, node {j} has term {term_j}"
                    );
                }
            }
        }
    }
}

// --- Test Entry Point ---

prop_state_machine! {
    #![proptest_config(Config {
        cases: 100,
        max_shrink_iters: 10000,
        .. Config::default()
    })]

    #[test]
    fn raft_state_machine_test(sequential 1..50 => RaftClusterSUT);
}

// --- Additional Focused Tests ---

#[cfg(test)]
mod focused_tests {
    use super::*;

    /// Verify cluster can elect a leader.
    #[test]
    fn cluster_elects_leader() {
        let mut cluster = RaftClusterSUT::new();

        // Run enough ticks for election
        for _ in 0..100 {
            for i in 0..CLUSTER_SIZE {
                let effects = cluster.nodes[i].tick();
                cluster.process_effects(&effects);
            }
            // Deliver all messages
            while let Some(msg) = cluster.pending_messages.pop() {
                let to_idx = msg.to.0 as usize;
                if to_idx < cluster.nodes.len() {
                    let effects = cluster.nodes[to_idx].step(msg);
                    cluster.process_effects(&effects);
                }
            }
        }

        let leaders = cluster.leaders();
        assert!(!leaders.is_empty(), "No leader elected after 100 rounds");
    }

    /// Verify election safety holds under rapid ticking.
    #[test]
    fn rapid_ticks_maintain_election_safety() {
        let mut cluster = RaftClusterSUT::new();

        for _ in 0..500 {
            // Tick all nodes
            for i in 0..CLUSTER_SIZE {
                let effects = cluster.nodes[i].tick();
                cluster.process_effects(&effects);
            }

            // Randomly deliver some messages
            let to_deliver = cluster.pending_messages.len() / 2;
            for _ in 0..to_deliver {
                if cluster.pending_messages.is_empty() {
                    break;
                }
                let msg = cluster.pending_messages.remove(0);
                let to_idx = msg.to.0 as usize;
                if to_idx < cluster.nodes.len() {
                    let effects = cluster.nodes[to_idx].step(msg);
                    cluster.process_effects(&effects);
                }
            }

            // Check election safety
            let leaders_by_term = cluster.leaders_by_term();
            for (term, leaders) in &leaders_by_term {
                assert!(leaders.len() <= 1, "Election safety violated at term {term}: {leaders:?}");
            }
        }
    }
}
