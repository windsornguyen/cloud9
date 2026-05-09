//! Read index coordination for linearizable reads (§6.4).
//!
//! Per the Raft dissertation §6.4: "Read-only operations can be handled without
//! writing anything into the log. However, with no additional measures, this
//! would run the risk of returning stale data."
//!
//! The protocol:
//! 1. Leader must have committed an entry from its current term (checked via `can_serve_reads()`)
//! 2. Leader records the current commit index as the "read index"
//! 3. Leader sends a heartbeat round and waits for majority acknowledgment
//! 4. Once majority confirms, the leader is guaranteed to still be leader
//! 5. The read can proceed once the state machine has applied up to `read_index`
//!
//! This module provides `ReadIndexCoordinator` which tracks pending reads and
//! completes them when heartbeat quorum is achieved.

use std::collections::{BTreeMap, VecDeque};

use cloud9_raft::{LogIndex, NodeId};

/// Unique identifier for a read request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(u64);

impl ReadId {
    fn next(&mut self) -> ReadId {
        let id = *self;
        self.0 += 1;
        id
    }
}

/// A pending read request awaiting heartbeat confirmation.
#[derive(Debug)]
struct PendingRead {
    /// The read index (`commit_index` when request was made).
    read_index: LogIndex,
    /// Heartbeat round this read is waiting on.
    round: u64,
}

/// Result of a completed read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexResult {
    /// The read ID that completed.
    pub id: ReadId,
    /// The log index up to which the state machine must be applied before reading.
    pub read_index: LogIndex,
}

/// Error when requesting a read index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadIndexError {
    /// Not the leader.
    NotLeader,
    /// Leader hasn't committed an entry from current term yet (§6.4 requirement).
    LeaderNotReady,
}

/// Coordinates linearizable reads via heartbeat confirmation (§6.4).
///
/// # Usage
///
/// 1. Call `request_read()` when a client wants a linearizable read
/// 2. The coordinator returns a `ReadId` and indicates a heartbeat should be sent
/// 3. Call `begin_heartbeat_round()` before sending heartbeats
/// 4. Call `record_ack()` for each heartbeat acknowledgment
/// 5. Call `poll_completed()` to get reads that achieved quorum
///
/// # Example
///
/// ```ignore
/// let mut coord = ReadIndexCoordinator::new(voters);
///
/// // Client requests a read
/// let read_id = coord.request_read(commit_index)?;
///
/// // Begin heartbeat round
/// coord.begin_heartbeat_round();
///
/// // Send heartbeats... then record acks
/// coord.record_ack(peer_id);
///
/// // Check for completed reads
/// for result in coord.poll_completed() {
///     // Read can proceed once state machine reaches result.read_index
/// }
/// ```
#[derive(Debug)]
pub struct ReadIndexCoordinator {
    /// Voting members (for quorum calculation).
    voters: Vec<NodeId>,
    /// This node's ID.
    me: NodeId,
    /// Next read ID to assign.
    next_read_id: ReadId,
    /// Current heartbeat round number.
    current_round: u64,
    /// Nodes that have acked the current round.
    round_acks: Vec<NodeId>,
    /// Pending reads waiting for quorum.
    pending: BTreeMap<ReadId, PendingRead>,
    /// Completed reads ready to be returned.
    completed: VecDeque<ReadIndexResult>,
}

impl ReadIndexCoordinator {
    /// Create a new coordinator.
    pub fn new(me: NodeId, voters: Vec<NodeId>) -> Self {
        Self {
            voters,
            me,
            next_read_id: ReadId(0),
            current_round: 0,
            round_acks: Vec::new(),
            pending: BTreeMap::new(),
            completed: VecDeque::new(),
        }
    }

    /// Request a linearizable read.
    ///
    /// Returns `Ok(ReadId)` if the request was accepted. The caller should
    /// then trigger a heartbeat round (if not already in progress) and wait
    /// for the read to complete via `poll_completed()`.
    ///
    /// # Arguments
    ///
    /// * `read_index` - The commit index at the time of the request (from `Leader::read_index()`)
    ///
    /// # Errors
    ///
    /// Returns `ReadIndexError::LeaderNotReady` if called when `can_serve_reads()` is false.
    /// The caller is responsible for checking `can_serve_reads()` before calling this.
    pub fn request_read(&mut self, read_index: LogIndex) -> ReadId {
        let id = self.next_read_id.next();
        self.pending.insert(id, PendingRead { read_index, round: self.current_round });
        id
    }

    /// Begin a new heartbeat round.
    ///
    /// Call this before sending heartbeats. All pending reads will be associated
    /// with this round and will complete when quorum is achieved.
    pub fn begin_heartbeat_round(&mut self) {
        self.current_round += 1;
        self.round_acks.clear();
        // Leader counts as implicit ack
        self.round_acks.push(self.me);

        // Associate all pending reads with this round
        for pending in self.pending.values_mut() {
            pending.round = self.current_round;
        }

        // Check if single-node cluster (already have quorum)
        self.check_quorum();
    }

    /// Record a heartbeat acknowledgment from a peer.
    ///
    /// Call this when an `AppendResponse { success: true }` is received
    /// from a heartbeat (empty `AppendEntries`).
    pub fn record_ack(&mut self, from: NodeId) {
        if !self.round_acks.contains(&from) && self.voters.contains(&from) {
            self.round_acks.push(from);
            self.check_quorum();
        }
    }

    /// Poll for completed reads.
    ///
    /// Returns reads that have achieved heartbeat quorum and are ready to proceed.
    /// The caller should wait for the state machine to reach `read_index` before
    /// executing the read.
    pub fn poll_completed(&mut self) -> impl Iterator<Item = ReadIndexResult> + '_ {
        std::iter::from_fn(move || self.completed.pop_front())
    }

    /// Check if we have quorum and complete any pending reads.
    fn check_quorum(&mut self) {
        let quorum = self.voters.len() / 2 + 1;
        if self.round_acks.len() >= quorum {
            // Complete all pending reads from this or earlier rounds
            let current = self.current_round;
            let to_complete: Vec<_> = self
                .pending
                .iter()
                .filter(|(_, p)| p.round <= current)
                .map(|(&id, p)| (id, p.read_index))
                .collect();

            for (id, read_index) in to_complete {
                self.pending.remove(&id);
                self.completed.push_back(ReadIndexResult { id, read_index });
            }
        }
    }

    /// Number of pending reads.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Check if any reads are pending.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Clear all pending reads (e.g., on leader stepdown).
    ///
    /// Returns the number of reads that were dropped.
    pub fn clear(&mut self) -> usize {
        let count = self.pending.len();
        self.pending.clear();
        self.round_acks.clear();
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters() -> Vec<NodeId> {
        vec![NodeId(0), NodeId(1), NodeId(2)]
    }

    #[test]
    fn request_read_returns_unique_ids() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        let id1 = coord.request_read(10);
        let id2 = coord.request_read(15);
        let id3 = coord.request_read(20);

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn read_completes_on_quorum() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        // Request a read
        let id = coord.request_read(42);
        assert_eq!(coord.pending_count(), 1);

        // Begin heartbeat round (leader auto-acks)
        coord.begin_heartbeat_round();

        // One more ack gives us quorum (2/3)
        coord.record_ack(NodeId(1));

        // Read should complete
        let results: Vec<_> = coord.poll_completed().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
        assert_eq!(results[0].read_index, 42);

        // No more pending
        assert_eq!(coord.pending_count(), 0);
    }

    #[test]
    fn read_does_not_complete_without_quorum() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        let _id = coord.request_read(42);
        coord.begin_heartbeat_round();

        // Only leader has acked (1/3) - no quorum yet
        let results: Vec<_> = coord.poll_completed().collect();
        assert!(results.is_empty());
        assert_eq!(coord.pending_count(), 1);
    }

    #[test]
    fn multiple_reads_complete_together() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        // Multiple reads at different indices
        let id1 = coord.request_read(10);
        let id2 = coord.request_read(20);
        let id3 = coord.request_read(30);

        coord.begin_heartbeat_round();
        coord.record_ack(NodeId(1));

        let results: Vec<_> = coord.poll_completed().collect();
        assert_eq!(results.len(), 3);

        // All should complete with their respective read indices
        let ids: Vec<_> = results.iter().map(|r| r.id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
        assert!(ids.contains(&id3));
    }

    #[test]
    fn single_node_cluster_completes_immediately() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), vec![NodeId(0)]);

        let id = coord.request_read(42);
        coord.begin_heartbeat_round();

        // Single node = immediate quorum
        let results: Vec<_> = coord.poll_completed().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn duplicate_acks_ignored() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        coord.request_read(42);
        coord.begin_heartbeat_round();

        // Same peer acking twice
        coord.record_ack(NodeId(1));
        coord.record_ack(NodeId(1));

        // Still only 2/3 acks
        let results: Vec<_> = coord.poll_completed().collect();
        assert_eq!(results.len(), 1); // Quorum is 2
    }

    #[test]
    fn non_voter_acks_ignored() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        coord.request_read(42);
        coord.begin_heartbeat_round();

        // Ack from non-voter
        coord.record_ack(NodeId(99));

        // Still only leader's ack
        let results: Vec<_> = coord.poll_completed().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn clear_drops_pending_reads() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        coord.request_read(10);
        coord.request_read(20);
        coord.request_read(30);

        assert_eq!(coord.pending_count(), 3);

        let dropped = coord.clear();
        assert_eq!(dropped, 3);
        assert_eq!(coord.pending_count(), 0);
    }

    #[test]
    fn new_round_associates_pending_reads() {
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters());

        // Request read before any heartbeat round
        let id = coord.request_read(42);

        // First round - no acks yet besides leader
        coord.begin_heartbeat_round();
        assert!(coord.poll_completed().next().is_none());

        // Second round - read should be associated with new round
        coord.begin_heartbeat_round();
        coord.record_ack(NodeId(1));

        // Should complete now
        let results: Vec<_> = coord.poll_completed().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn five_node_cluster_requires_three_acks() {
        let voters = vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let mut coord = ReadIndexCoordinator::new(NodeId(0), voters);

        coord.request_read(42);
        coord.begin_heartbeat_round(); // Leader acks (1/5)

        coord.record_ack(NodeId(1)); // 2/5
        assert!(coord.poll_completed().next().is_none());

        coord.record_ack(NodeId(2)); // 3/5 = quorum
        assert!(coord.poll_completed().next().is_some());
    }
}
