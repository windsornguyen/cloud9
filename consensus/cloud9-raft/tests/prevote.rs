//! `PreVote` extension tests (Dissertation §9.4).
#![allow(clippy::panic)]
//!
//! These tests cover §9.4-specific behaviors NOT tested elsewhere:
//! - Leader contact timeout (`recently_heard_from_leader` guard)
//! - Log up-to-dateness check in `PreVote` responses
//! - Leaders deny all prevotes
//! - `next_term` > voter's term requirement
//!
//! Note: Basic `PreVote` mechanics (term not incremented, `PreCandidate` transitions)
//! are tested in src/raft/precandidate.rs and src/raft/mod.rs.

use cloud9_raft::raft::{
    AppendRequest, Config, Entry, EntryPayload, Message, Payload, PreVoteRequest, PreVoteResponse,
    RaftNode, VoteResponse,
};
use cloud9_raft::{Command, NodeId};

fn prevote_config(id: NodeId) -> Config {
    Config::new(id).with_election_timeout(10, 20).with_heartbeat_interval(3)
}

const THREE_VOTERS: &[NodeId] = &[NodeId(0), NodeId(1), NodeId(2)];

// =============================================================================
// §9.4: Voters deny PreVote if they've recently heard from leader
// =============================================================================

/// "the voters have not received heartbeats from a valid leader for at least
/// a baseline election timeout"
///
/// A follower that recently received a heartbeat from the leader should deny
/// `PreVote` requests, preventing disruption from partitioned/slow servers.
#[test]
fn follower_denies_prevote_if_recently_heard_from_leader() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Receive heartbeat from leader - establishes leader contact
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
    assert_eq!(node.leader(), Some(NodeId(1)));

    // Immediately receive PreVoteRequest from another node
    // Should be DENIED because we recently heard from the leader
    let prevote_req = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: 1,
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: 2,
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let effects = node.step(prevote_req);

    // Should respond with denial
    assert_eq!(effects.messages.len(), 1);
    if let Payload::PreVoteResponse(resp) = &effects.messages[0].payload {
        assert!(!resp.granted, "should deny PreVote when recently heard from leader");
    } else {
        panic!("expected PreVoteResponse");
    }
}

/// After enough time passes without leader contact, follower should grant `PreVote`.
#[test]
fn follower_grants_prevote_after_leader_timeout() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Receive heartbeat from leader
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

    // Tick past the minimum election timeout (leader contact expires)
    // Election timeout is 10-20, so 15 ticks should be past the minimum
    for _ in 0..15 {
        node.tick();
    }

    // Now PreVoteRequest should be granted
    let prevote_req = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: 1,
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: 2,
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let effects = node.step(prevote_req);

    assert_eq!(effects.messages.len(), 1);
    if let Payload::PreVoteResponse(resp) = &effects.messages[0].payload {
        assert!(resp.granted, "should grant PreVote after leader contact timeout");
    } else {
        panic!("expected PreVoteResponse");
    }
}

// =============================================================================
// §9.4: PreVote respects log up-to-dateness (election restriction)
// =============================================================================

/// `PreVote` should be denied if the candidate's log is not up-to-date.
/// This is the same check as regular `VoteRequest`.
#[test]
fn prevote_denied_if_log_not_up_to_date() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Receive entries to make our log non-empty
    let entries =
        vec![Entry { term: 1, index: 1, payload: EntryPayload::Command(Command(vec![1])) }];
    let append = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 1,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
        }),
    };
    node.step(append);

    // Tick past leader contact timeout
    for _ in 0..15 {
        node.tick();
    }

    // PreVoteRequest with stale log (empty log, term 0)
    let stale_prevote = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: 1,
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: 2,
            last_log_index: 0, // We have index 1
            last_log_term: 0,  // We have term 1
        }),
    };
    let effects = node.step(stale_prevote);

    assert_eq!(effects.messages.len(), 1);
    if let Payload::PreVoteResponse(resp) = &effects.messages[0].payload {
        assert!(!resp.granted, "should deny PreVote when candidate's log is not up-to-date");
    } else {
        panic!("expected PreVoteResponse");
    }
}

/// `PreVote` granted when candidate's log is at least as up-to-date.
#[test]
fn prevote_granted_if_log_up_to_date() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Receive one entry
    let entries =
        vec![Entry { term: 1, index: 1, payload: EntryPayload::Command(Command(vec![1])) }];
    let append = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 1,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
        }),
    };
    node.step(append);

    // Tick past leader contact timeout
    for _ in 0..15 {
        node.tick();
    }

    // PreVoteRequest with up-to-date log
    let uptodate_prevote = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: 1,
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: 2,
            last_log_index: 1, // Same as ours
            last_log_term: 1,  // Same as ours
        }),
    };
    let effects = node.step(uptodate_prevote);

    assert_eq!(effects.messages.len(), 1);
    if let Payload::PreVoteResponse(resp) = &effects.messages[0].payload {
        assert!(resp.granted, "should grant PreVote when log is up-to-date");
    } else {
        panic!("expected PreVoteResponse");
    }
}

// =============================================================================
// §9.4: Leaders deny all PreVotes
// =============================================================================

/// A leader should deny all `PreVote` requests - it's a valid leader.
#[test]
fn leader_denies_all_prevotes() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Become PreCandidate → Candidate → Leader
    while node.is_follower() {
        node.tick();
    }
    // Win PreVote
    node.step(Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 0,
        payload: Payload::PreVoteResponse(PreVoteResponse { term: 0, granted: true }),
    });
    assert!(node.is_candidate());

    // Win real vote
    node.step(Message {
        from: NodeId(1),
        to: NodeId(0),
        term: node.term(),
        payload: Payload::VoteResponse(VoteResponse { granted: true }),
    });
    assert!(node.is_leader());
    let leader_term = node.term();

    // Receive PreVoteRequest from another node
    let prevote_req = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: leader_term,
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: leader_term + 1,
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let effects = node.step(prevote_req);

    // Leader should deny (it's the valid leader!)
    let prevote_responses: Vec<_> = effects
        .messages
        .iter()
        .filter(|m| matches!(m.payload, Payload::PreVoteResponse(_)))
        .collect();

    assert_eq!(prevote_responses.len(), 1);
    if let Payload::PreVoteResponse(resp) = &prevote_responses[0].payload {
        assert!(!resp.granted, "leader should deny PreVote requests");
    }
}

// =============================================================================
// §9.4: PreVote term requirements
// =============================================================================

/// `PreVote` `next_term` must be greater than voter's term.
#[test]
fn prevote_requires_higher_next_term() {
    let config = prevote_config(NodeId(0));
    let mut node = RaftNode::new(config, THREE_VOTERS);

    // Advance term to 2
    let heartbeat = Message {
        from: NodeId(1),
        to: NodeId(0),
        term: 2,
        payload: Payload::AppendRequest(AppendRequest {
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };
    node.step(heartbeat);
    assert_eq!(node.term(), 2);

    // Tick past leader contact timeout
    for _ in 0..15 {
        node.tick();
    }

    // PreVoteRequest with next_term <= our term (stale)
    let stale_prevote = Message {
        from: NodeId(2),
        to: NodeId(0),
        term: 1, // Message term doesn't matter for PreVote
        payload: Payload::PreVoteRequest(PreVoteRequest {
            next_term: 2, // Same as our term, not greater
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let effects = node.step(stale_prevote);

    assert_eq!(effects.messages.len(), 1);
    if let Payload::PreVoteResponse(resp) = &effects.messages[0].payload {
        assert!(!resp.granted, "should deny PreVote when next_term is not greater than our term");
    } else {
        panic!("expected PreVoteResponse");
    }
}
