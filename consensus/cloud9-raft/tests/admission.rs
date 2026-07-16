use cloud9_raft::raft::{Configuration, Message, Payload, Persistent, VoteResponse};
use cloud9_raft::{Command, ConsensusConfig, NodeId, ProposeError, RaftNode};

const VOTERS: &[NodeId] = &[NodeId(0), NodeId(1), NodeId(2)];

fn leader(config: ConsensusConfig) -> RaftNode {
    elect(RaftNode::new(configure(config), VOTERS))
}

fn configure(config: ConsensusConfig) -> ConsensusConfig {
    config.with_prevote(false).with_parallel_disk_write(false).with_pipelining(false)
}

fn elect(mut node: RaftNode) -> RaftNode {
    while !node.is_candidate() {
        node.tick();
    }
    node.step(Message {
        from: NodeId(1),
        to: NodeId(0),
        term: node.term(),
        payload: Payload::VoteResponse(VoteResponse { granted: true }),
    });
    assert!(node.is_leader());
    node
}

#[test]
fn leader_throttles_uncommitted_entries() {
    let mut config = ConsensusConfig::new(NodeId(0));
    config.max_uncommitted_entries = 2;
    let mut node = leader(config);

    assert!(node.propose(Command(vec![1])).is_ok());
    assert!(node.propose(Command(vec![2])).is_ok());
    assert!(matches!(node.propose(Command(vec![3])), Err(ProposeError::Throttled)));
}

#[test]
fn leader_throttles_uncommitted_command_bytes() {
    let mut config = ConsensusConfig::new(NodeId(0));
    config.max_uncommitted_bytes = 2;
    let mut node = leader(config);

    assert!(node.propose(Command(vec![1])).is_ok());
    assert!(node.propose(Command(vec![2])).is_ok());
    assert!(matches!(node.propose(Command(vec![3])), Err(ProposeError::Throttled)));
}

#[test]
fn compacted_prefix_does_not_consume_admission_capacity() {
    let mut config = ConsensusConfig::new(NodeId(0));
    config.max_uncommitted_entries = 1;
    let cluster = Configuration::simple(VOTERS.iter().copied());
    let mut persistent =
        Persistent { term: 1, bootstrap_config: cluster.clone(), ..Persistent::default() };
    persistent.log.install_snapshot(100, 1, cluster);
    let mut node = elect(RaftNode::restore(configure(config), persistent));

    assert!(node.propose(Command(vec![1])).is_ok());
}
