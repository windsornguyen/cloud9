//! Efficiency and non-redundancy integration tests.
#![allow(clippy::cast_possible_truncation, clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//!
//! These tests verify the implementation does no unnecessary work:
//! - No duplicate messages or wasted RPCs
//! - No redundant state changes or persistence
//! - No infinite loops or unbounded retries
//! - Efficient handling of edge cases (partitions, simultaneous events)
//!
//! Uses only the public API of cloud9-raft.

use std::collections::HashMap;

use cloud9_raft::raft::{
    AppendRequest, AppendResponse, Config, Entry, EntryPayload, Message, Payload, PreVoteResponse,
    RaftNode, VoteResponse,
};
use cloud9_raft::{Command, NodeId};

fn test_config(id: NodeId) -> Config {
    Config::new(id).with_election_timeout(10, 20).with_heartbeat_interval(3)
}

fn classify_message(payload: &Payload) -> &'static str {
    match payload {
        Payload::PreVoteRequest(_) => "PreVoteRequest",
        Payload::PreVoteResponse(_) => "PreVoteResponse",
        Payload::VoteRequest(_) => "VoteRequest",
        Payload::VoteResponse(_) => "VoteResponse",
        Payload::AppendRequest(_) => "AppendRequest",
        Payload::AppendResponse(_) => "AppendResponse",
        Payload::InstallSnapshotRequest(_) => "InstallSnapshotRequest",
        Payload::InstallSnapshotResponse(_) => "InstallSnapshotResponse",
        Payload::ReadIndexRequest(_) => "ReadIndexRequest",
        Payload::ReadIndexResponse(_) => "ReadIndexResponse",
        Payload::TimeoutNow => "TimeoutNow",
    }
}

/// Simulates a cluster for multi-node tests.
struct TestCluster {
    nodes: HashMap<NodeId, RaftNode>,
    /// Messages in flight.
    pending_messages: Vec<Message>,
    /// Count of messages sent by type for efficiency analysis.
    message_counts: HashMap<&'static str, usize>,
}

impl TestCluster {
    fn new(node_ids: &[NodeId]) -> Self {
        let mut nodes = HashMap::new();
        for &id in node_ids {
            let config = test_config(id);
            let node = RaftNode::new(config, node_ids);
            nodes.insert(id, node);
        }
        Self { nodes, pending_messages: Vec::new(), message_counts: HashMap::new() }
    }

    fn tick_all(&mut self) {
        let mut all_messages = Vec::new();
        for node in self.nodes.values_mut() {
            let effects = node.tick();
            all_messages.extend(effects.messages);
        }
        self.collect_messages(all_messages);
    }

    fn tick_node(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            let effects = node.tick();
            self.collect_messages(effects.messages);
        }
    }

    fn collect_messages(&mut self, messages: Vec<Message>) {
        for msg in messages {
            let msg_type = classify_message(&msg.payload);
            *self.message_counts.entry(msg_type).or_insert(0) += 1;
            self.pending_messages.push(msg);
        }
    }

    fn deliver_all(&mut self) {
        while !self.pending_messages.is_empty() {
            let messages = std::mem::take(&mut self.pending_messages);
            let mut new_messages = Vec::new();
            for msg in messages {
                if let Some(node) = self.nodes.get_mut(&msg.to) {
                    let effects = node.step(msg);
                    new_messages.extend(effects.messages);
                }
            }
            self.collect_messages(new_messages);
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().find(|(_, n)| n.is_leader()).map(|(&id, _)| id)
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes.iter().filter(|(_, n)| n.is_leader()).map(|(&id, _)| id).collect()
    }

    fn term(&self, id: NodeId) -> u64 {
        self.nodes.get(&id).map_or(0, cloud9_raft::RaftNode::term)
    }

    fn max_term(&self) -> u64 {
        self.nodes.values().map(cloud9_raft::RaftNode::term).max().unwrap_or(0)
    }

    fn message_count(&self, msg_type: &str) -> usize {
        *self.message_counts.get(msg_type).unwrap_or(&0)
    }

    fn reset_message_counts(&mut self) {
        self.message_counts.clear();
    }
}

// =============================================================================
// PreVote Efficiency Tests (§9.4)
// =============================================================================

/// `PreVote` prevents a partitioned server from disrupting the cluster on rejoin.
///
/// Per dissertation §9.4: "While a server is partitioned, it won't be able to
/// increment its term, since it can't receive permission from a majority."
#[test]
fn prevote_prevents_term_inflation_during_partition() {
    // 5-node cluster
    let ids: Vec<_> = (0..5).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Elect a leader (node 0)
    for _ in 0..50 {
        cluster.tick_all();
        cluster.deliver_all();
    }
    assert!(cluster.leader().is_some(), "should have a leader");
    let initial_term = cluster.max_term();

    // Partition node 4 (can't reach majority)
    // Simulate by only ticking node 4 and not delivering its messages
    cluster.reset_message_counts();

    // Node 4 times out repeatedly while partitioned
    for _ in 0..100 {
        cluster.tick_node(NodeId(4));
    }

    // With PreVote, node 4 should send PreVoteRequests, not VoteRequests
    // and should NOT increment its term
    let node4_term = cluster.term(NodeId(4));
    assert_eq!(node4_term, initial_term, "partitioned node should not inflate term with PreVote");

    // Should have sent PreVoteRequests, not VoteRequests
    let prevote_count = cluster.message_count("PreVoteRequest");
    let vote_count = cluster.message_count("VoteRequest");
    assert!(prevote_count > 0, "should send PreVoteRequests while partitioned");
    assert_eq!(vote_count, 0, "should NOT send VoteRequests without PreVote success");
}

/// When a partitioned server rejoins, it doesn't disrupt the existing leader.
///
/// This is a key `PreVote` invariant: a partitioned node shouldn't be able to
/// disrupt a healthy cluster when it reconnects.
#[test]
fn partitioned_server_rejoin_no_disruption() {
    let ids: Vec<_> = (0..3).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Elect a leader
    for _ in 0..50 {
        cluster.tick_all();
        cluster.deliver_all();
    }
    let leader_before = cluster.leader().expect("should have leader");
    let term_before = cluster.max_term();

    // Partition node 2 for a while (simulate by not delivering its messages)
    let partitioned = NodeId(2);
    for _ in 0..100 {
        cluster.tick_node(partitioned);
        // Don't deliver node 2's messages (it's partitioned)
        cluster.pending_messages.retain(|m| m.from != partitioned);
    }

    // Verify partitioned node's term hasn't inflated (PreVote should prevent this)
    let partitioned_term = cluster.term(partitioned);
    assert_eq!(
        partitioned_term, term_before,
        "partitioned node should NOT inflate term with PreVote enabled"
    );

    // Rejoin: clear stale messages and resume normal operation
    cluster.pending_messages.clear();
    for _ in 0..20 {
        cluster.tick_all();
        cluster.deliver_all();
    }

    let term_after = cluster.max_term();
    let leader_after = cluster.leader().expect("should still have leader");

    // Key invariant: term should be UNCHANGED
    // With PreVote, the partitioned node couldn't inflate its term,
    // so there's no reason for the cluster term to change
    assert_eq!(
        term_after, term_before,
        "term should be unchanged after rejoin: {term_before} vs {term_after}"
    );

    // Leader should be the same (no disruption)
    assert_eq!(
        leader_after, leader_before,
        "leader should be unchanged after partitioned node rejoins"
    );

    // Should still have exactly one leader
    assert_eq!(cluster.leaders().len(), 1, "should have exactly one leader");
}

// =============================================================================
// Election Efficiency Tests
// =============================================================================

/// Simultaneous candidate timeouts don't cause infinite election loops.
///
/// 3-node cluster variant - different quorum dynamics than 5-node.
/// With 3 nodes, quorum is 2, so split votes are less likely but still possible.
#[test]
fn simultaneous_elections_converge() {
    let ids: Vec<_> = (0..3).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Force all nodes to timeout simultaneously by advancing ticks without
    // any communication - this maximizes contention
    for _ in 0..25 {
        cluster.tick_all();
        // Don't deliver messages - simulates network delay
    }

    // Track how many rounds until convergence
    let mut rounds_to_leader = 0;
    for round in 0..100 {
        cluster.tick_all();
        cluster.deliver_all();
        if cluster.leader().is_some() {
            rounds_to_leader = round + 1;
            break;
        }
    }

    assert!(rounds_to_leader > 0, "should elect a leader after simultaneous timeout");

    // Convergence should happen in bounded time.
    // With randomized timeouts (10-20 ticks), worst case is several election
    // cycles. 50 rounds is very conservative - typically converges in < 10.
    assert!(
        rounds_to_leader < 50,
        "cluster should converge in bounded time, took {rounds_to_leader} rounds"
    );

    // Should have exactly one leader
    assert_eq!(cluster.leaders().len(), 1, "should converge to exactly one leader");

    // Term should be bounded - not inflating due to repeated failed elections.
    // Each failed election increments term by 1 (PreVote -> Candidate).
    // With randomization, shouldn't need many election rounds.
    let max_term = cluster.max_term();
    assert!(max_term < 10, "term should be bounded after convergence, got {max_term}");
}

/// Split vote situation eventually resolves due to randomized timeouts.
///
/// This test verifies that randomized election timeouts prevent livelock.
/// We force a split-vote scenario by having all nodes timeout simultaneously,
/// then verify the system recovers within bounded rounds.
#[test]
fn split_vote_resolves_with_randomization() {
    let ids: Vec<_> = (0..5).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Force simultaneous timeout - all nodes become PreCandidates at once
    // This maximizes chance of split votes
    for _ in 0..25 {
        cluster.tick_all();
        // Don't deliver - forces all to timeout together
    }

    // All nodes should now be PreCandidates (or about to be)
    // Now let them communicate - split votes likely
    let mut rounds_to_leader = 0;
    for round in 0..50 {
        cluster.tick_all();
        cluster.deliver_all();
        if cluster.leader().is_some() {
            rounds_to_leader = round + 1;
            break;
        }
    }

    assert!(rounds_to_leader > 0, "should eventually elect a leader after split votes");

    // Key invariant: randomization should resolve split votes reasonably quickly
    // Without randomization, 5 nodes could split-vote indefinitely
    // With randomization, expect resolution within ~10 rounds (very conservative)
    assert!(
        rounds_to_leader < 30,
        "split vote should resolve quickly with randomization, took {rounds_to_leader} rounds"
    );

    // Should have exactly one leader
    assert_eq!(cluster.leaders().len(), 1, "should have exactly one leader");

    // Term should be bounded - not inflated by repeated split votes
    let max_term = cluster.max_term();
    assert!(max_term < 10, "term should be bounded after split vote resolution, got {max_term}");
}

/// Candidate that receives heartbeat from valid leader steps down efficiently.
///
/// Per Raft: A candidate that receives `AppendEntries` from a leader with term >= its own
/// should recognize the leader and revert to follower.
///
/// Note: With `PreVote` enabled, the node transitions to `PreCandidate` first. When `PreCandidate`
/// receives `AppendEntries`, it steps down to Follower but doesn't automatically respond to
/// the message that caused the step-down (the message is consumed during transition).
/// To fully test the response, we send a second heartbeat after step-down.
#[test]
fn candidate_steps_down_on_leader_heartbeat() {
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

    // Trigger election timeout - with PreVote enabled, node becomes PreCandidate
    for _ in 0..25 {
        node.tick();
    }

    let node_term = node.term();
    assert_eq!(node_term, 0, "PreCandidate should not increment term");

    // Send heartbeat from "leader" - PreCandidate steps down to Follower
    let heartbeat = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: node_term,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };

    let effects = node.step(heartbeat.clone());

    // Node should step down to follower
    assert!(!node.is_leader(), "node should not be leader after receiving heartbeat");

    // PreCandidate step-down doesn't respond to the triggering message
    // (different from Follower which always responds to AppendRequest)
    // This is valid Raft behavior - the important thing is stepping down.

    // Now as Follower, send another heartbeat - should get response
    let effects2 = node.step(heartbeat);
    assert_eq!(effects2.messages.len(), 1, "follower should respond to heartbeat");
    if let Payload::AppendResponse(resp) = &effects2.messages[0].payload {
        assert!(resp.success, "should accept valid heartbeat");
    } else {
        panic!("expected AppendResponse");
    }

    // Verify we're still follower and not leader
    assert!(!node.is_leader());

    // Total message count should be reasonable (just 1 response after step-down)
    assert!(effects.messages.len() <= 1, "step-down should produce minimal messages");
}

// =============================================================================
// Replication Efficiency Tests
// =============================================================================

/// `AppendEntries` with already-committed entries is idempotent.
#[test]
fn redundant_append_entries_idempotent() {
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

    // Receive and commit some entries
    let entries: Vec<_> = (1..=5)
        .map(|i| Entry {
            term: 1,
            index: i,
            payload: EntryPayload::Command(Command(vec![i as u8])),
        })
        .collect();

    let append = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 1,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: entries.clone(),
            leader_commit: 5,
        }),
    };
    node.step(append);

    assert_eq!(node.commit_index(), 5);

    // Re-send same entries (network duplicate or retry)
    let append_dup = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 1,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 5,
        }),
    };
    let effects = node.step(append_dup);

    // Should succeed but not modify state
    assert_eq!(node.commit_index(), 5);

    // Should send success response
    assert_eq!(effects.messages.len(), 1);
    if let Payload::AppendResponse(resp) = &effects.messages[0].payload {
        assert!(resp.success);
    }
}

// =============================================================================
// Message Efficiency Tests
// =============================================================================

/// No messages are sent to self.
#[test]
fn no_messages_to_self() {
    let ids: Vec<_> = (0..3).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Track all messages seen (not just pending)
    let mut all_messages_seen = Vec::new();

    // Run cluster for a while, collecting messages at each step
    for _ in 0..100 {
        cluster.tick_all();
        // Capture messages before delivery
        all_messages_seen.extend(cluster.pending_messages.clone());
        cluster.deliver_all();
    }

    // Check all messages ever generated
    for msg in &all_messages_seen {
        assert_ne!(msg.to, msg.from, "should never send message to self");
    }

    // Sanity check: we actually saw some messages
    assert!(!all_messages_seen.is_empty(), "test should have generated some messages");
}

/// Heartbeat responses don't trigger unnecessary work when follower is caught up.
///
/// When a follower acknowledges it's up-to-date with the leader's log,
/// processing that ack should not generate new messages or require persistence.
#[test]
fn heartbeat_ack_minimal_work() {
    let ids: Vec<_> = (0..3).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    // Elect and stabilize
    for _ in 0..50 {
        cluster.tick_all();
        cluster.deliver_all();
    }
    let leader = cluster.leader().expect("should have leader");

    // Verify precondition: leader has empty log (commit_index = 0)
    // This ensures follower claiming index 0 is actually "caught up"
    let leader_commit = cluster.nodes[&leader].commit_index();
    assert_eq!(leader_commit, 0, "precondition: leader should have empty log for this test");

    cluster.reset_message_counts();

    // Send heartbeat ack from follower claiming to be at index 0 (caught up)
    let follower = if leader == NodeId(0) { NodeId(1) } else { NodeId(0) };
    let ack = Message {
        from: follower,
        to: leader,
        term: cluster.term(leader),
        payload: Payload::AppendResponse(AppendResponse {
            success: true,
            last_log_index: 0, // Same as leader - fully caught up
        }),
    };

    if let Some(node) = cluster.nodes.get_mut(&leader) {
        let effects = node.step(ack);

        // Key invariant: caught-up follower's ack should not trigger work
        assert!(
            effects.messages.is_empty(),
            "ack from caught-up follower should not generate messages"
        );
        assert!(!effects.persist, "ack from caught-up follower should not require persistence");
    }
}

// =============================================================================
// Persistence Efficiency Tests
// =============================================================================

/// Persistence flag only set when durable state actually changes.
///
/// Tests that the implementation doesn't wastefully persist on every operation.
/// Only term changes, vote grants, and log appends require persistence.
#[test]
fn persistence_only_on_state_change() {
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

    // First, establish a stable state by receiving a heartbeat
    // This sets up the node as follower with known leader at term 1
    let setup = Message {
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
    let setup_effects = node.step(setup);
    assert!(setup_effects.persist, "initial term change requires persistence");
    assert_eq!(node.term(), 1);

    // Now send heartbeat at SAME term - no persistence needed
    let same_term = Message {
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
    let effects_same = node.step(same_term);
    assert!(!effects_same.persist, "heartbeat at same term should NOT require persistence");

    // Send heartbeat at HIGHER term - persistence required
    let higher_term = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 5, // Higher term
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };
    let effects_higher = node.step(higher_term);
    assert!(effects_higher.persist, "term change requires persistence");
    assert_eq!(node.term(), 5);

    // Another heartbeat at term 5 - no persistence
    let same_again = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 5,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };
    let effects_same_again = node.step(same_again);
    assert!(
        !effects_same_again.persist,
        "repeated heartbeat at same term should NOT require persistence"
    );
}

/// Stale messages don't require persistence.
#[test]
fn stale_messages_no_persistence() {
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);

    // First, advance term by receiving a message
    let advance = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 5,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };
    node.step(advance);
    assert_eq!(node.term(), 5);

    // Now send stale message
    let stale = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 2, // Old term
        payload: Payload::VoteRequest(cloud9_raft::raft::VoteRequest {
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let effects = node.step(stale);

    assert!(!effects.persist, "stale message should not require persistence");
}

// =============================================================================
// Bounded Behavior Tests
// =============================================================================

/// Elections complete in bounded time/messages.
#[test]
fn election_bounded_messages() {
    let ids: Vec<_> = (0..5).map(NodeId).collect();
    let mut cluster = TestCluster::new(&ids);

    cluster.reset_message_counts();

    // Run until leader elected
    for round in 0..100 {
        cluster.tick_all();
        cluster.deliver_all();
        if cluster.leader().is_some() {
            // Check message counts are reasonable
            let total_messages: usize = cluster.message_counts.values().sum();
            // For 5 nodes, election should take O(n) messages per round
            // With a few rounds of split votes, should be < 500 messages total
            assert!(
                total_messages < 500,
                "election took {total_messages} messages in {round} rounds, should be bounded"
            );
            return;
        }
    }
    panic!("election did not complete in bounded time");
}

/// Duplicate votes from same peer don't cause extra state transitions.
///
/// This tests that receiving the same vote multiple times is handled gracefully.
/// The implementation should ignore duplicate votes - they should not count
/// toward quorum multiple times.
#[test]
fn duplicate_votes_ignored() {
    // Create a 5-node cluster and trigger election on node 0
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

    // Trigger election timeout - with PreVote, node becomes PreCandidate first
    for _ in 0..25 {
        node.tick();
    }

    let precandidate_term = node.term();
    assert_eq!(precandidate_term, 0, "PreCandidate should not increment term");

    // Send PreVote responses to transition to Candidate
    // Need 3 prevote grants (including self) for quorum in 5-node cluster
    for peer in [NodeId(1), NodeId(2)] {
        let prevote_resp = Message {
            from: peer,
            to: NodeId(0),
            term: precandidate_term,
            payload: Payload::PreVoteResponse(PreVoteResponse {
                term: precandidate_term,
                granted: true,
            }),
        };
        node.step(prevote_resp);
    }

    // Now node should be Candidate (term incremented to 1)
    let candidate_term = node.term();
    assert_eq!(candidate_term, 1, "Candidate should increment term");

    // Candidate has 1 vote (self). Need 3 for quorum in 5-node cluster.
    // Send ONE vote from NodeId(1)
    let vote1 = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: candidate_term,
        payload: Payload::VoteResponse(VoteResponse { granted: true }),
    };
    node.step(vote1);

    // Now have 2 votes (self + NodeId(1)). Still not quorum.
    assert!(!node.is_leader(), "should not be leader with only 2 votes in 5-node cluster");

    // Send DUPLICATE vote from NodeId(1) - should NOT count as third vote
    let vote_dup = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: candidate_term,
        payload: Payload::VoteResponse(VoteResponse { granted: true }),
    };
    node.step(vote_dup.clone());
    node.step(vote_dup);

    // Key invariant: duplicates should not grant quorum
    // If duplicates were counted, we'd have 4 votes and become leader
    assert!(
        !node.is_leader(),
        "duplicate votes should not count toward quorum - still only 2 real votes"
    );

    // Now send a real vote from NodeId(2) - should reach quorum
    let vote2 = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: candidate_term,
        payload: Payload::VoteResponse(VoteResponse { granted: true }),
    };
    node.step(vote2);

    // NOW we have 3 real votes and should be leader
    assert!(
        node.is_leader(),
        "should become leader with 3 real votes (self + NodeId(1) + NodeId(2))"
    );
}

/// `PreVote` responses from same peer are ignored after first.
///
/// In `PreVote`, we need quorum before transitioning to Candidate.
/// Duplicate responses from the same peer should not be double-counted.
#[test]
fn duplicate_prevote_responses_ignored() {
    let config = test_config(NodeId(0));
    let mut node = RaftNode::new(config, &[NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

    // Trigger election (will go to PreCandidate first)
    for _ in 0..25 {
        node.tick();
    }

    let initial_term = node.term();
    assert_eq!(initial_term, 0, "PreCandidate should not increment term");

    // Send ONE prevote response from NodeId(1)
    // Note: PreVoteResponse.term is the responder's CURRENT term (not next_term from request)
    // If we sent term: initial_term + 1, the PreCandidate would step down thinking
    // the responder has a higher term.
    let prevote_resp = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: initial_term,
        payload: Payload::PreVoteResponse(PreVoteResponse {
            term: initial_term, // Responder's current term
            granted: true,
        }),
    };
    node.step(prevote_resp.clone());

    // Send DUPLICATE from same peer - should not count toward quorum
    node.step(prevote_resp.clone());
    node.step(prevote_resp);

    // With 5 nodes, need 3 prevotes for quorum (including self = 1)
    // We only got 1 real vote from NodeId(1), so total = 2, not enough
    // If duplicates were counted, we'd have 4 and would transition to Candidate

    // Since we haven't reached quorum, term should NOT have incremented
    // (Candidate transition increments term)
    assert_eq!(
        node.term(),
        initial_term,
        "term should not increment without real quorum - duplicates should be ignored"
    );
}
