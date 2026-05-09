//! Cluster membership and configuration changes.
//!
//! Chapter 4 of the Raft dissertation describes two approaches for changing
//! cluster membership safely:
//!
//! - **Single-server changes (§4.1-4.2)**: Only add or remove one server at a time.
//!   This guarantees overlapping majorities between any two consecutive configs.
//!
//! - **Joint consensus (§4.3)**: Arbitrary configuration changes via an intermediate
//!   joint configuration that requires majorities from both old and new configs.
//!
//! Both approaches share key properties:
//! - Configuration takes effect when the entry is *appended*, not when committed
//! - Only one configuration change can be in flight at a time
//! - Leader not in new config must step down after committing the change

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// How to handle membership changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MembershipMode {
    /// Only allow adding or removing one server at a time (§4.1-4.2).
    ///
    /// Simpler and what most production systems use. The constraint that
    /// only one server changes at a time guarantees that any majority of
    /// the old config overlaps with any majority of the new config.
    #[default]
    SingleServer,

    /// Allow arbitrary configuration changes via joint consensus (§4.3).
    ///
    /// More flexible but more complex. Changes go through an intermediate
    /// joint configuration C_{old,new} that requires majorities from both
    /// the old and new configurations for any decision.
    JointConsensus,
}

/// A cluster configuration.
///
/// Can be either a simple configuration (single voter set) or a joint
/// configuration (requires majorities from both old and new).
///
/// Internally uses `BTreeSet` for O(log n) membership checks and zero-allocation
/// iteration. Serializes as sorted vectors for determinism.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Configuration {
    /// Simple configuration with a single set of voters.
    Simple {
        #[serde(with = "btreeset_as_vec")]
        voters: BTreeSet<NodeId>,
        #[serde(with = "btreeset_as_vec")]
        learners: BTreeSet<NodeId>,
    },

    /// Joint configuration during a transition (§4.3).
    ///
    /// Decisions require majorities from BOTH old and new configurations.
    /// This is the C_{old,new} state in the dissertation.
    Joint {
        /// The old configuration we're transitioning from.
        #[serde(with = "btreeset_as_vec")]
        old: BTreeSet<NodeId>,
        /// The new configuration we're transitioning to.
        #[serde(with = "btreeset_as_vec")]
        new: BTreeSet<NodeId>,
        /// Non-voting learners (replicated but not counted in quorums).
        #[serde(with = "btreeset_as_vec")]
        learners: BTreeSet<NodeId>,
    },
}

/// Serde helper to serialize `BTreeSet` as Vec for stable output.
mod btreeset_as_vec {
    use super::{BTreeSet, Deserialize, NodeId};
    use serde::{Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        set: &BTreeSet<NodeId>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = ser.serialize_seq(Some(set.len()))?;
        for id in set {
            seq.serialize_element(id)?;
        }
        seq.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<BTreeSet<NodeId>, D::Error> {
        let vec = Vec::<NodeId>::deserialize(de)?;
        Ok(vec.into_iter().collect())
    }
}

impl Configuration {
    /// Create a simple configuration from voters.
    pub fn simple(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Self::Simple { voters: voters.into_iter().collect(), learners: BTreeSet::new() }
    }

    /// Create a simple configuration with explicit learners.
    pub fn simple_with_learners(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self::Simple {
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
        }
    }

    /// Create a joint configuration for transitioning from old to new.
    pub fn joint(
        old: impl IntoIterator<Item = NodeId>,
        new: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self::Joint {
            old: old.into_iter().collect(),
            new: new.into_iter().collect(),
            learners: BTreeSet::new(),
        }
    }

    /// Create a joint configuration with learners.
    pub fn joint_with_learners(
        old: impl IntoIterator<Item = NodeId>,
        new: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self::Joint {
            old: old.into_iter().collect(),
            new: new.into_iter().collect(),
            learners: learners.into_iter().collect(),
        }
    }

    /// Get all unique voters in this configuration.
    ///
    /// For simple configs, iterates voters directly.
    /// For joint configs, returns union of old and new (no allocation).
    pub fn voters(&self) -> impl Iterator<Item = NodeId> + '_ {
        let (first, second): (&BTreeSet<NodeId>, Option<&BTreeSet<NodeId>>) = match self {
            Self::Simple { voters, .. } => (voters, None),
            Self::Joint { old, new, .. } => (old, Some(new)),
        };
        // Chain iterators; for Joint, filter duplicates from second set
        first.iter().copied().chain(
            second
                .into_iter()
                .flat_map(|s| s.iter().copied())
                .filter(move |id| !first.contains(id)),
        )
    }

    /// Check if a node is a voter in this configuration.
    pub fn is_voter(&self, id: NodeId) -> bool {
        match self {
            Self::Simple { voters, .. } => voters.contains(&id),
            Self::Joint { old, new, .. } => old.contains(&id) || new.contains(&id),
        }
    }

    /// Check if a node is a learner.
    pub fn is_learner(&self, id: NodeId) -> bool {
        match self {
            Self::Simple { learners, .. } | Self::Joint { learners, .. } => learners.contains(&id),
        }
    }

    /// All learners (non-voting). No allocation.
    pub fn learners(&self) -> impl Iterator<Item = NodeId> + '_ {
        let learners = match self {
            Self::Simple { learners, .. } | Self::Joint { learners, .. } => learners,
        };
        learners.iter().copied()
    }

    /// Number of unique voters in this configuration.
    pub fn voter_count(&self) -> usize {
        match self {
            Self::Simple { voters, .. } => voters.len(),
            Self::Joint { old, new, .. } => {
                // Count unique voters across both sets
                old.len() + new.iter().filter(|id| !old.contains(id)).count()
            }
        }
    }

    /// Iterate over voters except `me`. Useful for sending to peers.
    pub fn voter_peers(&self, me: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.voters().filter(move |&id| id != me)
    }

    /// All nodes (voters + learners). No allocation.
    pub fn all_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.voters().chain(self.learners())
    }

    /// Iterate over all replication targets (voters + learners) except `me`.
    pub fn replication_peers(&self, me: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.all_nodes().filter(move |&id| id != me)
    }

    /// Check if a node is any member (voter or learner).
    pub fn is_member(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.is_learner(id)
    }

    /// Check if this is a joint configuration.
    pub fn is_joint(&self) -> bool {
        matches!(self, Self::Joint { .. })
    }

    /// Number of votes needed for quorum in each voter set.
    ///
    /// For simple configs, returns (quorum, None).
    /// For joint configs, returns (`old_quorum`, `Some(new_quorum)`).
    pub fn quorum_sizes(&self) -> (usize, Option<usize>) {
        match self {
            Self::Simple { voters, .. } => (voters.len() / 2 + 1, None),
            Self::Joint { old, new, .. } => (old.len() / 2 + 1, Some(new.len() / 2 + 1)),
        }
    }

    /// Check if we have quorum given votes from each voter.
    ///
    /// `has_vote` is a function that returns true if the given node has voted/acked.
    pub fn has_quorum(&self, has_vote: impl Fn(NodeId) -> bool) -> bool {
        match self {
            Self::Simple { voters, .. } => {
                let votes = voters.iter().filter(|&&id| has_vote(id)).count();
                votes > voters.len() / 2
            }
            Self::Joint { old, new, .. } => {
                let old_votes = old.iter().filter(|&&id| has_vote(id)).count();
                let new_votes = new.iter().filter(|&&id| has_vote(id)).count();
                old_votes > old.len() / 2 && new_votes > new.len() / 2
            }
        }
    }

    /// Iterate over voters, yielding (voter, `old_member`, `new_member`).
    ///
    /// For simple configs, both `old_member` and `new_member` are true for all voters.
    /// For joint configs, indicates membership in each set.
    pub fn voter_membership(&self) -> Vec<(NodeId, bool, bool)> {
        match self {
            Self::Simple { voters, .. } => voters.iter().map(|&id| (id, true, true)).collect(),
            Self::Joint { old, new, .. } => {
                let mut result = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for &id in old {
                    seen.insert(id);
                    result.push((id, true, new.contains(&id)));
                }
                for &id in new {
                    if seen.insert(id) {
                        result.push((id, false, true));
                    }
                }
                result
            }
        }
    }

    /// Get the target configuration for completing a joint transition.
    ///
    /// Returns `Some(new_config)` if this is a joint config, None if simple.
    pub fn transition_target(&self) -> Option<Configuration> {
        match self {
            Self::Simple { .. } => None,
            Self::Joint { new, learners, .. } => {
                Some(Configuration::Simple { voters: new.clone(), learners: learners.clone() })
            }
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self::Simple { voters: BTreeSet::new(), learners: BTreeSet::new() }
    }
}

/// Canonical representation of a full membership set (voters + learners).
///
/// Ensures deterministic ordering, uniqueness, and no voter/learner overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

impl Members {
    /// Construct a membership set from unordered node lists.
    ///
    /// Duplicates are removed and any node that appears in both lists
    /// results in an error (explicit over silent promotion).
    pub fn new(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, ConfigChangeError> {
        use std::collections::BTreeSet;

        let mut voter_set = BTreeSet::new();
        for id in voters {
            voter_set.insert(id);
        }

        let mut learner_set = BTreeSet::new();
        for id in learners {
            if voter_set.contains(&id) {
                return Err(ConfigChangeError::Overlap(id));
            }
            learner_set.insert(id);
        }

        Ok(Self {
            voters: voter_set.into_iter().collect(),
            learners: learner_set.into_iter().collect(),
        })
    }

    /// Voters in deterministic order.
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    /// Learners in deterministic order.
    pub fn learners(&self) -> &[NodeId] {
        &self.learners
    }
}

/// A proposed configuration change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigChange {
    /// Add a voter to the cluster.
    AddVoter(NodeId),
    /// Remove a voter from the cluster.
    RemoveVoter(NodeId),
    /// Add a non-voting learner.
    AddLearner(NodeId),
    /// Remove a learner.
    RemoveLearner(NodeId),
    /// Set a completely new configuration (only valid in `JointConsensus` mode).
    SetMembers(Members),
}

impl ConfigChange {
    /// Validate this change against the current configuration and mode.
    pub fn validate(
        &self,
        current: &Configuration,
        mode: MembershipMode,
    ) -> Result<(), ConfigChangeError> {
        // Can't change during joint consensus
        if current.is_joint() {
            return Err(ConfigChangeError::ChangeInProgress);
        }

        let (voters, learners) = match current {
            Configuration::Simple { voters, learners } => (voters, learners),
            Configuration::Joint { .. } => unreachable!(),
        };

        match self {
            ConfigChange::AddVoter(id) => {
                if voters.contains(id) {
                    return Err(ConfigChangeError::AlreadyVoter(*id));
                }
                if !learners.contains(id) {
                    return Err(ConfigChangeError::PromoteRequiresLearner(*id));
                }
            }
            ConfigChange::RemoveVoter(id) => {
                if !voters.contains(id) {
                    return Err(ConfigChangeError::NotVoter(*id));
                }
                if voters.len() == 1 {
                    return Err(ConfigChangeError::WouldBeEmpty);
                }
            }
            ConfigChange::AddLearner(id) => {
                if voters.contains(id) {
                    return Err(ConfigChangeError::AlreadyVoter(*id));
                }
                if learners.contains(id) {
                    return Err(ConfigChangeError::AlreadyLearner(*id));
                }
            }
            ConfigChange::RemoveLearner(id) => {
                if !learners.contains(id) {
                    return Err(ConfigChangeError::NotLearner(*id));
                }
            }
            ConfigChange::SetMembers(members) => {
                let new_voters: BTreeSet<_> = members.voters().iter().copied().collect();
                let new_learners: BTreeSet<_> = members.learners().iter().copied().collect();
                // Reject no-op changes early
                if new_voters == *voters && new_learners == *learners {
                    return Err(ConfigChangeError::NoChange);
                }
                if mode == MembershipMode::SingleServer {
                    // Validate single-server constraint: at most one voter changes
                    let added: Vec<_> =
                        new_voters.iter().filter(|id| !voters.contains(id)).collect();
                    let removed: Vec<_> =
                        voters.iter().filter(|id| !new_voters.contains(id)).collect();

                    if added.len() + removed.len() > 1 {
                        return Err(ConfigChangeError::NotSingleChange {
                            added: added.len(),
                            removed: removed.len(),
                        });
                    }
                }
                if new_voters.is_empty() {
                    return Err(ConfigChangeError::WouldBeEmpty);
                }
                // Ensure no overlap between voters and learners
                if let Some(id) = new_voters.iter().find(|id| new_learners.contains(id)) {
                    return Err(ConfigChangeError::AlreadyVoter(*id));
                }
            }
        }

        Ok(())
    }

    /// Apply this change to produce the next configuration.
    ///
    /// For `SingleServer` mode, returns the new simple configuration directly.
    /// For `JointConsensus` mode, returns a joint configuration (for `SetMembers`)
    /// or simple configuration (for Add/Remove which are single changes).
    pub fn apply(&self, current: &Configuration, mode: MembershipMode) -> Configuration {
        let (voters, learners): (BTreeSet<NodeId>, BTreeSet<NodeId>) = match current {
            Configuration::Simple { voters, learners } => (voters.clone(), learners.clone()),
            Configuration::Joint { new, learners, .. } => (new.clone(), learners.clone()),
        };

        match self {
            ConfigChange::AddVoter(id) => {
                let mut new_voters = voters;
                new_voters.insert(*id);
                let mut new_learners = learners;
                new_learners.remove(id);
                Configuration::Simple { voters: new_voters, learners: new_learners }
            }
            ConfigChange::RemoveVoter(id) => {
                let mut new_voters = voters;
                new_voters.remove(id);
                Configuration::Simple { voters: new_voters, learners }
            }
            ConfigChange::AddLearner(id) => {
                let mut new_learners = learners;
                new_learners.insert(*id);
                Configuration::Simple { voters, learners: new_learners }
            }
            ConfigChange::RemoveLearner(id) => {
                let mut new_learners = learners;
                new_learners.remove(id);
                Configuration::Simple { voters, learners: new_learners }
            }
            ConfigChange::SetMembers(members) => {
                let new_voters: BTreeSet<_> = members.voters().iter().copied().collect();
                let new_learners: BTreeSet<_> = members.learners().iter().copied().collect();
                if mode == MembershipMode::JointConsensus && voters != new_voters {
                    // Enter joint consensus
                    Configuration::Joint { old: voters, new: new_voters, learners: new_learners }
                } else {
                    // Single-server mode or no actual change
                    Configuration::Simple { voters: new_voters, learners: new_learners }
                }
            }
        }
    }
}

/// Errors when proposing a configuration change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigChangeError {
    /// A configuration change is already in progress.
    ChangeInProgress,
    /// Node is already a voter.
    AlreadyVoter(NodeId),
    /// Node is already a learner.
    AlreadyLearner(NodeId),
    /// Same node listed as both voter and learner.
    Overlap(NodeId),
    /// To promote to voter, the node must already be a learner.
    PromoteRequiresLearner(NodeId),
    /// Learner is not sufficiently caught up to promote.
    LearnerNotCaughtUp { id: NodeId, have: u64, need: u64 },
    /// Node is not a voter.
    NotVoter(NodeId),
    /// Node is not a learner.
    NotLearner(NodeId),
    /// Change would result in an empty cluster.
    WouldBeEmpty,
    /// No actual change (voters and learners identical).
    NoChange,
    /// `SingleServer` mode requires exactly one change at a time.
    NotSingleChange { added: usize, removed: usize },
    /// This node is not the leader.
    NotLeader,
}

impl std::fmt::Display for ConfigChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChangeInProgress => write!(f, "configuration change already in progress"),
            Self::AlreadyVoter(id) => write!(f, "{id} is already a voter"),
            Self::AlreadyLearner(id) => write!(f, "{id} is already a learner"),
            Self::Overlap(id) => write!(f, "{id} cannot be both voter and learner"),
            Self::PromoteRequiresLearner(id) => {
                write!(f, "{id} must be added as a learner before becoming a voter")
            }
            Self::LearnerNotCaughtUp { id, have, need } => {
                write!(f, "{id} not caught up: have {have}, need {need}")
            }
            Self::NotVoter(id) => write!(f, "{id} is not a voter"),
            Self::NotLearner(id) => write!(f, "{id} is not a learner"),
            Self::WouldBeEmpty => write!(f, "change would result in empty cluster"),
            Self::NoChange => write!(f, "configuration unchanged"),
            Self::NotSingleChange { added, removed } => {
                write!(
                    f,
                    "single-server mode requires exactly one change, got {added} added and {removed} removed"
                )
            }
            Self::NotLeader => write!(f, "not the leader"),
        }
    }
}

impl std::error::Error for ConfigChangeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_config_quorum() {
        let config = Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]);

        // 0 votes: no quorum
        assert!(!config.has_quorum(|_| false));

        // 1 vote: no quorum
        assert!(!config.has_quorum(|id| id == NodeId(0)));

        // 2 votes: quorum
        assert!(config.has_quorum(|id| id == NodeId(0) || id == NodeId(1)));

        // 3 votes: quorum
        assert!(config.has_quorum(|_| true));
    }

    #[test]
    fn joint_config_quorum() {
        // Old: [0, 1, 2], New: [1, 2, 3]
        let config = Configuration::joint(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            vec![NodeId(1), NodeId(2), NodeId(3)],
        );

        // Need 2/3 from old AND 2/3 from new
        // Only node 1: no quorum (1/3 old, 1/3 new)
        assert!(!config.has_quorum(|id| id == NodeId(1)));

        // Nodes 1, 2: quorum (2/3 old, 2/3 new)
        assert!(config.has_quorum(|id| id == NodeId(1) || id == NodeId(2)));

        // Nodes 0, 1: no quorum (2/3 old, but only 1/3 new)
        assert!(!config.has_quorum(|id| id == NodeId(0) || id == NodeId(1)));

        // Nodes 1, 3: no quorum (1/3 old, 2/3 new)
        assert!(!config.has_quorum(|id| id == NodeId(1) || id == NodeId(3)));

        // Nodes 0, 1, 3: quorum (2/3 old, 2/3 new)
        assert!(config.has_quorum(|id| id == NodeId(0) || id == NodeId(1) || id == NodeId(3)));
    }

    #[test]
    fn single_server_add_voter() {
        let current =
            Configuration::simple_with_learners(vec![NodeId(0), NodeId(1)], vec![NodeId(2)]);
        let change = ConfigChange::AddVoter(NodeId(2));

        assert!(change.validate(&current, MembershipMode::SingleServer).is_ok());

        let new = change.apply(&current, MembershipMode::SingleServer);
        assert_eq!(new, Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]));
    }

    #[test]
    fn single_server_remove_voter() {
        let current = Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]);
        let change = ConfigChange::RemoveVoter(NodeId(2));

        assert!(change.validate(&current, MembershipMode::SingleServer).is_ok());

        let new = change.apply(&current, MembershipMode::SingleServer);
        assert_eq!(new, Configuration::simple(vec![NodeId(0), NodeId(1)]));
    }

    #[test]
    fn single_server_rejects_multiple_changes() {
        let current = Configuration::simple(vec![NodeId(0), NodeId(1)]);
        let change = ConfigChange::SetMembers(
            Members::new(
                vec![NodeId(2), NodeId(3)], // +2, -2
                vec![],
            )
            .unwrap(),
        );

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert!(matches!(err, ConfigChangeError::NotSingleChange { .. }));
    }

    #[test]
    fn joint_consensus_creates_joint_config() {
        let current = Configuration::simple(vec![NodeId(0), NodeId(1)]);
        let change =
            ConfigChange::SetMembers(Members::new(vec![NodeId(2), NodeId(3)], vec![]).unwrap());

        assert!(change.validate(&current, MembershipMode::JointConsensus).is_ok());

        let new = change.apply(&current, MembershipMode::JointConsensus);
        assert!(new.is_joint());
        assert_eq!(
            new,
            Configuration::joint(vec![NodeId(0), NodeId(1)], vec![NodeId(2), NodeId(3)])
        );
    }

    #[test]
    fn rejects_change_during_joint() {
        let current = Configuration::joint(vec![NodeId(0), NodeId(1)], vec![NodeId(1), NodeId(2)]);
        let change = ConfigChange::AddVoter(NodeId(3));

        let err = change.validate(&current, MembershipMode::JointConsensus).unwrap_err();
        assert_eq!(err, ConfigChangeError::ChangeInProgress);
    }

    #[test]
    fn rejects_duplicate_add() {
        let current = Configuration::simple(vec![NodeId(0), NodeId(1)]);
        let change = ConfigChange::AddVoter(NodeId(1));

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert_eq!(err, ConfigChangeError::AlreadyVoter(NodeId(1)));
    }

    #[test]
    fn rejects_remove_nonvoter() {
        let current = Configuration::simple(vec![NodeId(0), NodeId(1)]);
        let change = ConfigChange::RemoveVoter(NodeId(2));

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert_eq!(err, ConfigChangeError::NotVoter(NodeId(2)));
    }

    #[test]
    fn rejects_empty_cluster() {
        let current = Configuration::simple(vec![NodeId(0)]);
        let change = ConfigChange::RemoveVoter(NodeId(0));

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert_eq!(err, ConfigChangeError::WouldBeEmpty);
    }

    #[test]
    fn transition_target() {
        let simple = Configuration::simple_with_learners(vec![NodeId(0)], vec![NodeId(5)]);
        assert!(simple.transition_target().is_none());

        let joint = Configuration::joint(vec![NodeId(0), NodeId(1)], vec![NodeId(1), NodeId(2)]);
        assert_eq!(
            joint.transition_target(),
            Some(Configuration::simple_with_learners(vec![NodeId(1), NodeId(2)], vec![]))
        );
    }

    #[test]
    fn voter_membership() {
        let joint = Configuration::joint(vec![NodeId(0), NodeId(1)], vec![NodeId(1), NodeId(2)]);

        let membership = joint.voter_membership();
        // Node 0: in old, not in new
        // Node 1: in both
        // Node 2: not in old, in new
        assert!(membership.contains(&(NodeId(0), true, false)));
        assert!(membership.contains(&(NodeId(1), true, true)));
        assert!(membership.contains(&(NodeId(2), false, true)));
    }

    #[test]
    fn learners_can_be_added_and_promoted() {
        let current = Configuration::simple(vec![NodeId(0)]);
        let add = ConfigChange::AddLearner(NodeId(1));
        let cfg = add.apply(&current, MembershipMode::SingleServer);
        assert!(cfg.is_learner(NodeId(1)));
        assert!(!cfg.is_voter(NodeId(1)));

        // Promote learner to voter automatically removes from learner list
        let promote = ConfigChange::AddVoter(NodeId(1));
        let cfg = promote.apply(&cfg, MembershipMode::SingleServer);
        assert!(cfg.is_voter(NodeId(1)));
        assert!(!cfg.is_learner(NodeId(1)));
    }

    #[test]
    fn add_learner_rejects_duplicates() {
        let current = Configuration::simple_with_learners(vec![NodeId(0)], vec![NodeId(1)]);
        let change = ConfigChange::AddLearner(NodeId(1));

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert_eq!(err, ConfigChangeError::AlreadyLearner(NodeId(1)));
    }

    #[test]
    fn rejects_no_op_set_members() {
        let current =
            Configuration::simple_with_learners(vec![NodeId(0), NodeId(1)], vec![NodeId(2)]);
        let change = ConfigChange::SetMembers(
            Members::new(vec![NodeId(0), NodeId(1)], vec![NodeId(2)]).unwrap(),
        );

        let err = change.validate(&current, MembershipMode::SingleServer).unwrap_err();
        assert_eq!(err, ConfigChangeError::NoChange);

        // Same in JointConsensus mode
        let err = change.validate(&current, MembershipMode::JointConsensus).unwrap_err();
        assert_eq!(err, ConfigChangeError::NoChange);
    }
}
