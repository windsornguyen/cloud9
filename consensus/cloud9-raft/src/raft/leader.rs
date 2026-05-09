//! Leader role.
//!
//! A leader:
//! - Sends heartbeats (empty `AppendEntries`) periodically
//! - Replicates log entries to all followers
//! - Tracks replication progress per peer (`next_index`, `match_index`)
//! - Commits entries once replicated to a majority
//! - Steps down on discovering higher term
//!
//! Leader-specific state:
//! - `next_index`: next log index to send to each peer
//! - `match_index`: highest replicated index for each peer
//! - `heartbeat_deadline`: when to send next heartbeat

use std::collections::BTreeSet;

use crate::{Command, LogIndex, NodeId, ReadIndexError, TransferError};

use super::StepResult;
use super::core::Core;
use super::event::{
    AppendRequest, AppendResponse, Effects, Event, InstallSnapshotResponse, Message, Payload,
    PreVoteResponse, ReadIndexRequest, ReadIndexResponse, SendSnapshot, VoteResponse,
};
use super::log::{Entry, EntryPayload};
use super::membership::{ConfigChange, ConfigChangeError, Configuration};

/// Result of processing committed config entries.
enum ConfigCommitResult {
    /// Continue processing (appended `C_new`, may need to commit it).
    Continue(Effects),
    /// Must step down (removed from config), with optional leadership transfer.
    StepDown(Effects),
    /// No config changes, done.
    Done,
}

#[derive(Debug, Clone)]
struct PendingReadIndex {
    id: u64,
    read_index: LogIndex,
    configuration: Configuration,
    acks: BTreeSet<NodeId>,
}

/// Leader role state.
#[derive(Debug, Clone)]
pub struct Leader {
    /// Next log index to send to each peer (keyed by `NodeId`).
    pub next_index: std::collections::BTreeMap<NodeId, LogIndex>,
    /// Highest log index known to be replicated on each peer.
    pub match_index: std::collections::BTreeMap<NodeId, LogIndex>,
    /// Tick at which to send next heartbeat.
    pub heartbeat_deadline: u64,
    /// Highest index we've sent to each peer (for pipelining).
    /// When pipelining is enabled, this tracks what's in-flight.
    sent_index: std::collections::BTreeMap<NodeId, LogIndex>,
    /// Monotonic id for read-index quorum rounds.
    next_read_id: u64,
    /// Read-index quorum round waiting for follower confirmations.
    pending_read_index: Option<PendingReadIndex>,
    /// Highest read index confirmed by a quorum after the read was requested.
    confirmed_read_index: LogIndex,
}

impl Leader {
    /// Create a new leader, sending initial heartbeats.
    pub fn new(core: &mut Core) -> (Self, Effects) {
        let last_index = core.log().last_index();
        let mut next_index = std::collections::BTreeMap::new();
        let mut match_index = std::collections::BTreeMap::new();
        let mut sent_index = std::collections::BTreeMap::new();

        // Initialize replication state for all peers
        for id in core.effective_config().all_nodes() {
            next_index.insert(id, last_index + 1);
            match_index.insert(id, 0);
            sent_index.insert(id, 0);
        }

        // Our own match_index: if parallel_disk_write is disabled, assume our
        // entries are already on disk. Otherwise, IO layer must signal completion.
        if !core.config.parallel_disk_write {
            match_index.insert(core.id(), last_index);
        }
        // Note: sent_index for self is not meaningful

        let mut leader = Self {
            next_index,
            match_index,
            heartbeat_deadline: core.ticks + core.config.heartbeat_interval,
            sent_index,
            next_read_id: 1,
            pending_read_index: None,
            confirmed_read_index: 0,
        };

        // Send initial heartbeats
        let effects = leader.broadcast_append(core);

        (leader, effects)
    }

    /// Ticks until heartbeat.
    pub fn ticks_until_deadline(&self, ticks: u64) -> u64 {
        self.heartbeat_deadline.saturating_sub(ticks)
    }

    /// Check if this leader can serve a read at its current commit index (§6.4).
    pub fn can_serve_reads(&self, core: &Core) -> bool {
        Self::current_term_commit_index(core).is_some()
            && self.confirmed_read_index >= core.commit_index
            && core.last_applied >= core.commit_index
    }

    /// Get the highest read index confirmed by a quorum.
    #[inline]
    pub fn read_index(&self, _core: &Core) -> LogIndex {
        self.confirmed_read_index
    }

    /// Check whether a requested read index is confirmed and applied.
    pub fn read_index_ready(&self, core: &Core, read_index: LogIndex) -> bool {
        self.confirmed_read_index >= read_index && core.last_applied >= read_index
    }

    /// Start a read-index quorum round (§6.4).
    pub fn request_read_index(
        &mut self,
        core: &Core,
    ) -> Result<(LogIndex, Effects), ReadIndexError> {
        if let Some(pending) = &self.pending_read_index {
            return Err(ReadIndexError::ReadInProgress { read_index: pending.read_index });
        }

        if Self::current_term_commit_index(core).is_none() {
            return Err(ReadIndexError::CurrentTermNotCommitted);
        }

        let read_index = core.commit_index;
        let id = self.next_read_id;
        self.next_read_id += 1;

        let pending = PendingReadIndex {
            id,
            read_index,
            configuration: core.effective_config(),
            acks: BTreeSet::from([core.id()]),
        };

        if pending.configuration.has_quorum(|voter| pending.acks.contains(&voter)) {
            self.confirmed_read_index = self.confirmed_read_index.max(read_index);
            return Ok((read_index, Effects::none()));
        }

        let effects = Self::read_index_requests(core, &pending);
        self.pending_read_index = Some(pending);
        Ok((read_index, effects))
    }

    fn read_index_requests(core: &Core, pending: &PendingReadIndex) -> Effects {
        Effects::none().with_messages(pending.configuration.voter_peers(core.id()).map(|to| {
            Message {
                from: core.id(),
                to,
                term: core.term(),
                payload: Payload::ReadIndexRequest(ReadIndexRequest {
                    id: pending.id,
                    read_index: pending.read_index,
                }),
            }
        }))
    }

    fn current_term_commit_index(core: &Core) -> Option<LogIndex> {
        let term = core.term();
        for idx in (1..=core.commit_index).rev() {
            if core.log().term_at(idx) == term {
                return Some(idx);
            }
            // Optimization: stop if we hit an older term (entries are in term order)
            if core.log().term_at(idx) < term {
                break;
            }
        }
        None
    }

    /// Propose a command for replication.
    ///
    /// Returns (index, effects, `should_step_down`). The caller should check
    /// `should_step_down` and transition to follower if true.
    pub fn propose(&mut self, core: &mut Core, cmd: Command) -> (LogIndex, Effects, bool) {
        let index = core.log().last_index() + 1;
        let entry = Entry { term: core.term(), index, payload: EntryPayload::Command(cmd) };
        core.log_mut().append(entry);

        // Update own match_index only if parallel_disk_write is disabled.
        // When enabled, IO layer will signal disk completion via DiskWriteComplete.
        if !core.config.parallel_disk_write {
            self.match_index.insert(core.id(), index);
        }

        // Try to commit (important for single-node clusters)
        let (commit_effects, should_step_down) = self.maybe_commit(core);

        // Replicate to followers
        let effects = self.broadcast_append(core).with_persist().merge(commit_effects);

        (index, effects, should_step_down)
    }

    /// Propose a configuration change.
    ///
    /// Per §4, the new configuration takes effect immediately when appended
    /// (not when committed). Only one config change can be pending at a time.
    ///
    /// Returns (index, effects, `should_step_down`) on success.
    pub fn propose_config_change(
        &mut self,
        core: &mut Core,
        change: &ConfigChange,
    ) -> Result<(LogIndex, Effects, bool), ConfigChangeError> {
        // Validate the change
        let current = core.effective_config();
        change.validate(&current, core.config.membership_mode)?;

        // Enforce learner catch-up before promotion.
        if let ConfigChange::AddVoter(id) = *change {
            // If we're promoting a learner, ensure it is caught up.
            if current.is_learner(id) {
                let progressed = self.match_index.get(&id).copied().unwrap_or(0);
                let need = core.log().last_index();
                if progressed < need {
                    return Err(ConfigChangeError::LearnerNotCaughtUp {
                        id,
                        have: progressed,
                        need,
                    });
                }
            } else {
                // Should have been rejected by validate(), but double-check.
                return Err(ConfigChangeError::PromoteRequiresLearner(id));
            }
        }

        // Check for pending config change
        if core.has_pending_config() {
            return Err(ConfigChangeError::ChangeInProgress);
        }

        // Apply the change to get new configuration
        let new_config = change.apply(&current, core.config.membership_mode);

        // Create config entry
        let index = core.log().last_index() + 1;
        let entry = Entry { term: core.term(), index, payload: EntryPayload::Config(new_config) };
        core.log_mut().append(entry);

        // Config takes effect immediately (§4) - effective_config() reflects this

        // Update own match_index only if parallel_disk_write is disabled.
        if !core.config.parallel_disk_write {
            self.match_index.insert(core.id(), index);
        }

        // Reinitialize next_index for new voters
        self.reinit_progress(core);

        // Try to commit (important for single-node clusters)
        let (commit_effects, should_step_down) = self.maybe_commit(core);

        // Replicate to followers (including any new ones)
        let effects = self.broadcast_append(core).with_persist().merge(commit_effects);

        Ok((index, effects, should_step_down))
    }

    /// Reinitialize replication progress for any new voters.
    fn reinit_progress(&mut self, core: &Core) {
        let last_index = core.log().last_index();
        for id in core.effective_config().all_nodes() {
            self.next_index.entry(id).or_insert(last_index + 1);
            self.match_index.entry(id).or_insert(0);
            self.sent_index.entry(id).or_insert(0);
        }
        // Ensure our own progress is correct (only if parallel_disk_write disabled)
        if !core.config.parallel_disk_write {
            self.match_index.insert(core.id(), last_index);
        }
    }

    /// Transfer leadership to the target node (§3.11).
    ///
    /// Sends `TimeoutNow` to the target if it is caught up. The target will
    /// immediately start an election and (assuming it wins) become leader.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Target is self
    /// - Target is not a voter in the current configuration
    /// - Target's log is behind (caller should wait for catch-up)
    pub fn transfer_leadership(
        &self,
        core: &Core,
        target: NodeId,
    ) -> Result<Effects, TransferError> {
        // Cannot transfer to self
        if target == core.id() {
            return Err(TransferError::TargetIsSelf);
        }

        // Target must be a voter
        if !core.effective_config().is_voter(target) {
            return Err(TransferError::TargetNotVoter);
        }

        // Target must be caught up
        let last_index = core.log().last_index();
        let match_index = self.match_index.get(&target).copied().unwrap_or(0);
        if match_index < last_index {
            return Err(TransferError::TargetLagging { match_index, last_index });
        }

        // Send TimeoutNow to trigger immediate election
        let msg = Message {
            from: core.id(),
            to: target,
            term: core.term(),
            payload: Payload::TimeoutNow,
        };

        Ok(Effects::none().with_message(msg))
    }

    /// Process an event. Returns transition (if any) and effects.
    pub fn step(&mut self, core: &mut Core, event: Event) -> StepResult {
        match event {
            Event::Tick => self.handle_tick(core),
            Event::DiskWriteComplete(index) => self.handle_disk_write_complete(core, index),
            Event::Message(msg) => {
                // PreVoteRequest doesn't update term - deny as leader
                if let Payload::PreVoteRequest(_) = msg.payload {
                    return StepResult::stay(Self::deny_prevote(core, msg.from));
                }

                // Higher term: step down
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
                    Payload::AppendResponse(resp) => {
                        self.handle_append_response(core, msg.from, resp)
                    }
                    Payload::InstallSnapshotResponse(resp) => {
                        self.handle_install_snapshot_response(core, msg.from, resp)
                    }
                    Payload::ReadIndexResponse(resp) => {
                        self.handle_read_index_response(msg.from, resp)
                    }
                    Payload::VoteRequest(_) => Self::handle_vote_request(core, msg.from),
                    _ => StepResult::none(),
                }
            }
        }
    }

    /// Handle disk write completion (§10.2.1 parallel disk write).
    ///
    /// Called by the IO layer when persistent state has been written to disk.
    /// Updates the leader's own `match_index`, which may allow more commits.
    fn handle_disk_write_complete(&mut self, core: &mut Core, index: LogIndex) -> StepResult {
        // Update our own match_index to reflect persisted state
        let current = self.match_index.get(&core.id()).copied().unwrap_or(0);
        if index > current {
            self.match_index.insert(core.id(), index);

            // Check if we can now commit more entries
            let (effects, should_step_down) = self.maybe_commit(core);
            if should_step_down {
                return StepResult::to_follower(None, effects);
            }
            return StepResult::stay(effects);
        }
        StepResult::none()
    }

    fn handle_tick(&mut self, core: &mut Core) -> StepResult {
        core.ticks += 1;
        if core.ticks >= self.heartbeat_deadline {
            self.heartbeat_deadline = core.ticks + core.config.heartbeat_interval;
            let mut effects = self.broadcast_append(core);
            if let Some(pending) = &self.pending_read_index {
                effects = effects.merge(Self::read_index_requests(core, pending));
            }
            StepResult::stay(effects)
        } else {
            StepResult::none()
        }
    }

    fn handle_append_response(
        &mut self,
        core: &mut Core,
        from: NodeId,
        resp: AppendResponse,
    ) -> StepResult {
        if !core.effective_config().is_member(from) {
            return StepResult::none();
        }

        if resp.success {
            // Update match_index (confirmed replicated)
            self.match_index.insert(from, resp.last_log_index);

            // When pipelining is disabled, also update next_index
            // (when enabled, next_index was already optimistically updated)
            if !core.config.pipelining {
                self.next_index.insert(from, resp.last_log_index + 1);
            }

            // Check if we should step down after committing
            let (effects, should_step_down) = self.maybe_commit(core);
            if should_step_down {
                // Config change committed that removes us - step down
                return StepResult::to_follower(None, effects);
            }
            StepResult::stay(effects)
        } else {
            // Revert next_index based on hint and retry.
            // When pipelining, we may have optimistically advanced next_index,
            // so we revert to match_index + 1 (known good state).
            let match_idx = self.match_index.get(&from).copied().unwrap_or(0);
            let hinted = (resp.last_log_index + 1).max(1);
            // Use hint if it's between match_index and current next_index
            let new_next = if core.config.pipelining {
                // With pipelining: be more aggressive, use hint if valid
                hinted.max(match_idx + 1)
            } else {
                hinted
            };
            self.next_index.insert(from, new_next);
            StepResult::stay(self.send_append_to(core, from))
        }
    }

    fn handle_install_snapshot_response(
        &mut self,
        core: &mut Core,
        from: NodeId,
        resp: InstallSnapshotResponse,
    ) -> StepResult {
        if !resp.success || !core.effective_config().is_member(from) {
            return StepResult::none();
        }

        let current_match = self.match_index.get(&from).copied().unwrap_or(0);
        if resp.last_included_index <= current_match {
            return StepResult::none();
        }

        self.match_index.insert(from, resp.last_included_index);
        self.next_index.insert(from, resp.last_included_index + 1);

        let (effects, should_step_down) = self.maybe_commit(core);
        if should_step_down {
            return StepResult::to_follower(None, effects);
        }
        StepResult::stay(effects)
    }

    fn handle_read_index_response(&mut self, from: NodeId, resp: ReadIndexResponse) -> StepResult {
        let Some(pending) = &mut self.pending_read_index else {
            return StepResult::none();
        };
        if !pending.configuration.is_voter(from) {
            return StepResult::none();
        }
        if pending.id != resp.id || pending.read_index != resp.read_index {
            return StepResult::none();
        }

        pending.acks.insert(from);
        if pending.configuration.has_quorum(|voter| pending.acks.contains(&voter)) {
            let read_index = pending.read_index;
            self.confirmed_read_index = self.confirmed_read_index.max(read_index);
            self.pending_read_index = None;
        }

        StepResult::none()
    }

    fn handle_vote_request(core: &Core, from: NodeId) -> StepResult {
        // We're leader in this term, deny vote
        let resp = Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::VoteResponse(VoteResponse { granted: false }),
        };
        StepResult::stay(Effects::none().with_message(resp))
    }

    fn deny_prevote(core: &Core, from: NodeId) -> Effects {
        // As leader, always deny pre-votes - we're the authority for this term
        Effects::none().with_message(Message {
            from: core.id(),
            to: from,
            term: core.term(),
            payload: Payload::PreVoteResponse(PreVoteResponse {
                term: core.term(),
                granted: false,
            }),
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

    /// Try to advance `commit_index` based on `match_index` majority.
    ///
    /// For joint consensus, requires majorities from both old and new configs.
    /// Returns (effects, `should_step_down`) where:
    /// - effects: messages to send (e.g., for auto-completing joint consensus or leadership transfer)
    /// - `should_step_down`: true if we should step down (removed from config)
    pub fn maybe_commit(&mut self, core: &mut Core) -> (Effects, bool) {
        let mut effects = Effects::none();

        // Loop handles the case where committing C_{old,new} triggers C_new append,
        // which may itself commit immediately in single-node clusters.
        loop {
            let old_commit = core.commit_index;

            // Advance commit index to highest replicated entry from current term
            self.advance_commit_index(core);

            if core.commit_index == old_commit {
                break; // No progress
            }

            // Process any newly committed config entries
            match self.process_committed_configs(core, old_commit) {
                ConfigCommitResult::Continue(e) => {
                    effects = effects.merge(e);
                    // Continue loop to try committing any newly appended C_new
                }
                ConfigCommitResult::StepDown(e) => return (effects.merge(e), true),
                ConfigCommitResult::Done => break,
            }
        }

        (effects, false)
    }

    /// Find the highest log index that has quorum and update `commit_index`.
    ///
    /// Only commits entries from the current term (§3.6.2).
    fn advance_commit_index(&self, core: &mut Core) {
        let config = core.effective_config();
        let max_possible = core.log().last_index();

        // Search from highest to lowest, stopping at first quorum
        for candidate in (core.commit_index + 1..=max_possible).rev() {
            if core.log().term_at(candidate) != core.term() {
                continue; // Only commit entries from current term
            }

            let has_quorum = config.has_quorum(|voter| {
                self.match_index.get(&voter).copied().unwrap_or(0) >= candidate
            });

            if has_quorum {
                core.commit_index = candidate;
                return;
            }
        }
    }

    /// Process config entries that were just committed.
    ///
    /// Returns whether to continue (possibly appended `C_new`), step down, or done.
    fn process_committed_configs(
        &mut self,
        core: &mut Core,
        old_commit: LogIndex,
    ) -> ConfigCommitResult {
        for idx in (old_commit + 1)..=core.commit_index {
            let Some(entry) = core.log().get(idx) else { continue };
            let EntryPayload::Config(ref cfg) = entry.payload else { continue };

            // Check if we're removed from the config
            if !cfg.is_voter(core.id()) {
                // Try to transfer leadership before stepping down (§4.2.2)
                let transfer_effects = self.try_auto_transfer(core, cfg);
                return ConfigCommitResult::StepDown(transfer_effects);
            }

            // Auto-complete joint consensus (§4.3):
            // When C_{old,new} commits, immediately append C_new
            if let Some(target) = cfg.transition_target() {
                let effects = self.append_transition_target(core, target);
                return ConfigCommitResult::Continue(effects);
            }
        }
        ConfigCommitResult::Done
    }

    /// Try to automatically transfer leadership to a caught-up voter (§4.2.2).
    ///
    /// Called when the leader is removed from the configuration. Finds the most
    /// caught-up voter in the new config and sends `TimeoutNow` to trigger immediate
    /// election.
    fn try_auto_transfer(
        &self,
        core: &Core,
        new_config: &super::membership::Configuration,
    ) -> Effects {
        let last_index = core.log().last_index();

        // Find voters in the new config that are caught up (match_index == last_index)
        // Prefer the one with highest match_index
        let mut best_target: Option<(NodeId, LogIndex)> = None;

        for voter in new_config.voters() {
            if voter == core.id() {
                continue; // Skip self
            }
            let match_idx = self.match_index.get(&voter).copied().unwrap_or(0);
            if match_idx >= last_index {
                // Fully caught up - best possible target
                best_target = Some((voter, match_idx));
                break;
            }
            // Track best so far even if not fully caught up
            match best_target {
                None => best_target = Some((voter, match_idx)),
                Some((_, best_idx)) if match_idx > best_idx => {
                    best_target = Some((voter, match_idx));
                }
                _ => {}
            }
        }

        // Only send TimeoutNow if we found a caught-up target
        if let Some((target, match_idx)) = best_target
            && match_idx >= last_index
        {
            return Effects::none().with_message(Message {
                from: core.id(),
                to: target,
                term: core.term(),
                payload: Payload::TimeoutNow,
            });
        }

        // No suitable target found - just step down and let election happen
        Effects::none()
    }

    /// Append the `C_new` config to complete a joint consensus transition.
    fn append_transition_target(
        &mut self,
        core: &mut Core,
        target: super::membership::Configuration,
    ) -> Effects {
        let term = core.term();
        let new_index = core.log().last_index() + 1;
        core.log_mut().append(Entry {
            term,
            index: new_index,
            payload: EntryPayload::Config(target),
        });

        self.reinit_progress(core);
        // reinit_progress handles our own match_index based on parallel_disk_write

        self.broadcast_append(core).with_persist()
    }

    /// Send `AppendEntries` to all peers.
    fn broadcast_append(&mut self, core: &Core) -> Effects {
        core.effective_config()
            .replication_peers(core.id())
            .fold(Effects::none(), |eff, peer| eff.merge(self.send_append_to(core, peer)))
    }

    /// Send `AppendEntries` to a specific peer, or request snapshot if entries unavailable.
    ///
    /// Per §5: "if a follower's log is so far behind the leader's that the
    /// leader has discarded the next entry it needs to send to the follower",
    /// the leader sends `InstallSnapshot` instead.
    ///
    /// When pipelining is enabled (§10.2.2), optimistically updates `next_index`
    /// after sending, allowing multiple in-flight requests.
    fn send_append_to(&mut self, core: &Core, to: NodeId) -> Effects {
        let next = *self.next_index.get(&to).unwrap_or(&1);
        let snapshot_index = core.log().snapshot_index();

        // §5: Send snapshot if follower needs entries that have been compacted
        if next <= snapshot_index {
            return Effects::none().with_send_snapshot(SendSnapshot {
                to,
                last_included_index: snapshot_index,
                last_included_term: core.log().snapshot_term(),
                configuration: core.config_at(snapshot_index),
            });
        }

        let prev_index = next.saturating_sub(1);
        // Use snapshot term if prev_index is at or before snapshot
        let prev_term = if prev_index <= snapshot_index && prev_index > 0 {
            core.log().snapshot_term()
        } else {
            core.log().term_at(prev_index)
        };

        // Collect entries to send
        let last_log = core.log().last_index();
        let end = (next + core.config.max_entries_per_msg).min(last_log + 1);
        let entries: Vec<_> = core.log().slice(next, end).to_vec();
        let sent_up_to = entries.last().map_or(next, |entry| entry.index + 1);

        // Pipelining (§10.2.2): optimistically update next_index after sending
        if core.config.pipelining && sent_up_to > next {
            self.next_index.insert(to, sent_up_to);
        }

        let msg = Message {
            from: core.id(),
            to,
            term: core.term(),
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit: core.commit_index,
            }),
        };

        Effects::none().with_message(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::Transition;
    use super::super::core::Config;
    use super::super::event::VoteRequest;
    use super::super::log::EntryPayload;
    use super::super::membership::Configuration;

    fn test_setup() -> (Core, Leader, Effects) {
        let config = Config::new(NodeId(0))
            .with_heartbeat_interval(5) // Use smaller values for tests
            // Disable optimizations for backward-compatible tests
            .with_parallel_disk_write(false)
            .with_pipelining(false);
        let mut core = Core::new(config, &[NodeId(0), NodeId(1), NodeId(2)]);
        core.persistent.term = 1;
        let (leader, effects) = Leader::new(&mut core);
        (core, leader, effects)
    }

    #[test]
    fn new_leader_sends_heartbeats() {
        let (_, _, effects) = test_setup();
        assert_eq!(effects.messages.len(), 2); // To peers 1 and 2

        for msg in &effects.messages {
            assert!(matches!(msg.payload, Payload::AppendRequest(_)));
            if let Payload::AppendRequest(req) = &msg.payload {
                assert!(req.entries.is_empty()); // Heartbeat
            }
        }
    }

    #[test]
    fn leader_initializes_indices() {
        let (core, leader, _) = test_setup();

        for id in core.effective_config().all_nodes() {
            assert_eq!(leader.next_index[&id], 1); // last_index(0) + 1
        }
    }

    #[test]
    fn heartbeat_on_tick() {
        let (mut core, mut leader, _) = test_setup();

        // Tick until just before heartbeat deadline (step increments ticks first)
        while core.ticks + 1 < leader.heartbeat_deadline {
            let StepResult { effects, .. } = leader.step(&mut core, Event::Tick);
            assert!(effects.messages.is_empty());
        }

        // Next tick triggers heartbeat
        let StepResult { transition, effects } = leader.step(&mut core, Event::Tick);
        assert!(matches!(transition, Transition::Stay));
        assert_eq!(effects.messages.len(), 2);
    }

    #[test]
    fn propose_appends_and_replicates() {
        let (mut core, mut leader, _) = test_setup();

        let (index, effects, _) = leader.propose(&mut core, Command(vec![42]));
        assert_eq!(index, 1);
        assert!(effects.persist);
        assert_eq!(effects.messages.len(), 2);

        assert_eq!(core.log().last_index(), 1);
        assert_eq!(core.log().get(1).unwrap().term, 1);
    }

    #[test]
    fn single_node_commits_immediately() {
        let config = Config::new(NodeId(0)).with_parallel_disk_write(false).with_pipelining(false);
        let mut core = Core::new(config, &[NodeId(0)]);
        core.persistent.term = 1;
        let (mut leader, _) = Leader::new(&mut core);

        let (index, _, _) = leader.propose(&mut core, Command(vec![42]));
        assert_eq!(index, 1);
        assert_eq!(core.commit_index, 1); // Committed immediately
    }

    #[test]
    fn commits_on_majority_ack() {
        let (mut core, mut leader, _) = test_setup();

        // Propose an entry
        leader.propose(&mut core, Command(vec![42]));
        assert_eq!(core.commit_index, 0); // Not committed yet

        // Receive ack from one peer (now have 2/3 = majority)
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendResponse(AppendResponse { success: true, last_log_index: 1 }),
        };

        leader.step(&mut core, Event::Message(msg));
        assert_eq!(core.commit_index, 1); // Now committed
    }

    #[test]
    fn retries_on_rejection() {
        let (mut core, mut leader, _) = test_setup();

        // Add some entries
        for i in 1..=3 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }

        // Initialize next_index to 4 (after our entries)
        leader.next_index.insert(NodeId(1), 4);

        // Follower rejects (their log is behind)
        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::AppendResponse(AppendResponse {
                success: false,
                last_log_index: 0, // Follower has nothing
            }),
        };

        let StepResult { effects, .. } = leader.step(&mut core, Event::Message(msg));

        // Should decrement next_index and retry
        assert!(leader.next_index[&NodeId(1)] <= 1);
        assert_eq!(effects.messages.len(), 1);
    }

    #[test]
    fn steps_down_on_higher_term() {
        let (mut core, mut leader, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 5, // Higher than our term 1
            payload: Payload::AppendResponse(AppendResponse { success: false, last_log_index: 0 }),
        };

        let StepResult { transition, effects } = leader.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::ToFollower(None)));
        assert!(effects.persist);
        assert_eq!(core.term(), 5);
    }

    #[test]
    fn only_commits_current_term_entries() {
        let (mut core, mut leader, _) = test_setup();

        // Add an entry from a previous term
        core.log_mut().append(Entry {
            term: 0, // Old term
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });

        // Update match indices to show it's replicated everywhere
        for id in core.effective_config().all_nodes() {
            leader.match_index.insert(id, 1);
        }

        let _ = leader.maybe_commit(&mut core);

        // Should NOT commit because entry is from old term
        assert_eq!(core.commit_index, 0);

        // Add an entry from current term
        core.log_mut().append(Entry {
            term: 1, // Current term
            index: 2,
            payload: EntryPayload::Command(Command(vec![2])),
        });

        for id in core.effective_config().all_nodes() {
            leader.match_index.insert(id, 2);
        }

        let _ = leader.maybe_commit(&mut core);

        // Now commits up to 2 (includes the old entry indirectly)
        assert_eq!(core.commit_index, 2);
    }

    #[test]
    fn denies_vote_as_leader() {
        let (mut core, mut leader, _) = test_setup();

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::VoteRequest(VoteRequest { last_log_index: 0, last_log_term: 0 }),
        };

        let StepResult { transition, effects } = leader.step(&mut core, Event::Message(msg));
        assert!(matches!(transition, Transition::Stay));

        if let Payload::VoteResponse(resp) = &effects.messages[0].payload {
            assert!(!resp.granted);
        }
    }

    // --- Snapshot tests ---

    #[test]
    fn detects_slow_follower_needing_snapshot() {
        let (mut core, mut leader, _) = test_setup();

        // Add entries and take a snapshot
        for i in 1..=5 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }
        core.log_mut().truncate_prefix(3, 1); // Snapshot covers 1-3

        // Follower is behind (next_index = 1, but entry 1 is gone)
        leader.next_index.insert(NodeId(1), 1);

        // Attempt to send append - should detect snapshot needed
        let effects = leader.send_append_to(&core, NodeId(1));

        // Should emit SendSnapshot effect instead of AppendRequest
        assert_eq!(effects.send_snapshots.len(), 1);
        let snap = &effects.send_snapshots[0];
        assert_eq!(snap.to, NodeId(1));
        assert_eq!(snap.last_included_index, 3);
        assert_eq!(snap.last_included_term, 1);
        assert_eq!(
            snap.configuration,
            Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)])
        );

        // Should NOT emit AppendRequest message
        assert!(effects.messages.is_empty());
    }

    #[test]
    fn sends_append_when_entries_available() {
        let (mut core, mut leader, _) = test_setup();

        // Add entries and take a snapshot
        for i in 1..=5 {
            core.log_mut().append(Entry {
                term: 1,
                index: i,
                payload: EntryPayload::Command(Command(vec![i as u8])),
            });
        }
        core.log_mut().truncate_prefix(3, 1); // Snapshot covers 1-3

        // Follower needs entry 4 (which is still available)
        leader.next_index.insert(NodeId(1), 4);

        let effects = leader.send_append_to(&core, NodeId(1));

        // Should emit AppendRequest, not snapshot
        assert!(effects.send_snapshots.is_empty());
        assert_eq!(effects.messages.len(), 1);

        if let Payload::AppendRequest(req) = &effects.messages[0].payload {
            assert_eq!(req.prev_log_index, 3);
            assert_eq!(req.prev_log_term, 1); // From snapshot
            assert!(!req.entries.is_empty());
        } else {
            panic!("expected AppendRequest");
        }
    }

    #[test]
    fn install_snapshot_response_advances_follower_progress() {
        let (mut core, mut leader, _) = test_setup();
        leader.next_index.insert(NodeId(1), 1);
        leader.match_index.insert(NodeId(1), 0);

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::InstallSnapshotResponse(InstallSnapshotResponse {
                success: true,
                last_included_index: 3,
            }),
        };

        let StepResult { transition, .. } = leader.step(&mut core, Event::Message(msg));

        assert!(matches!(transition, Transition::Stay));
        assert_eq!(leader.match_index.get(&NodeId(1)), Some(&3));
        assert_eq!(leader.next_index.get(&NodeId(1)), Some(&4));
    }

    #[test]
    fn stale_install_snapshot_response_does_not_regress_progress() {
        let (mut core, mut leader, _) = test_setup();
        leader.next_index.insert(NodeId(1), 11);
        leader.match_index.insert(NodeId(1), 10);

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::InstallSnapshotResponse(InstallSnapshotResponse {
                success: true,
                last_included_index: 3,
            }),
        };

        leader.step(&mut core, Event::Message(msg));

        assert_eq!(leader.match_index.get(&NodeId(1)), Some(&10));
        assert_eq!(leader.next_index.get(&NodeId(1)), Some(&11));
    }

    // --- Read index tests (§6.4) ---

    #[test]
    fn cannot_serve_reads_without_current_term_commit() {
        let (mut core, leader, _) = test_setup();

        // No entries committed yet
        assert!(!leader.can_serve_reads(&core));

        // Add entry from previous term and "commit" it
        core.log_mut().append(Entry {
            term: 0, // Old term
            index: 1,
            payload: EntryPayload::Command(Command(vec![])),
        });
        core.commit_index = 1;

        // Still can't serve reads - no current term entry committed
        assert!(!leader.can_serve_reads(&core));
        let mut leader = leader;
        assert!(matches!(
            leader.request_read_index(&core),
            Err(ReadIndexError::CurrentTermNotCommitted)
        ));
    }

    #[test]
    fn can_serve_reads_after_current_term_commit_and_quorum() {
        // Use single-node cluster where propose commits immediately
        let config = Config::new(NodeId(0)).with_parallel_disk_write(false).with_pipelining(false);
        let mut single_core = Core::new(config, &[NodeId(0)]);
        single_core.persistent.term = 1;
        let (mut single_leader, _) = Leader::new(&mut single_core);

        // Initially can't serve reads
        assert!(!single_leader.can_serve_reads(&single_core));

        // Propose commits immediately in single-node cluster
        single_leader.propose(&mut single_core, Command(vec![42]));
        single_core.last_applied = 1;

        let (read_index, effects) = single_leader.request_read_index(&single_core).unwrap();
        assert_eq!(read_index, 1);
        assert!(effects.messages.is_empty());
        assert!(single_leader.can_serve_reads(&single_core));
        assert_eq!(single_leader.read_index(&single_core), 1);
    }

    #[test]
    fn read_index_waits_for_heartbeat_quorum() {
        let (mut core, mut leader, _) = test_setup();
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });
        core.commit_index = 1;
        core.last_applied = 1;

        assert!(!leader.can_serve_reads(&core));
        let (read_index, effects) = leader.request_read_index(&core).unwrap();

        assert_eq!(read_index, 1);
        assert_eq!(effects.messages.len(), 2);
        assert!(matches!(
            effects.messages[0].payload,
            Payload::ReadIndexRequest(ReadIndexRequest { id: 1, read_index: 1 })
        ));
        assert!(!leader.read_index_ready(&core, read_index));

        let msg = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::ReadIndexResponse(ReadIndexResponse { id: 1, read_index }),
        };
        leader.step(&mut core, Event::Message(msg));

        assert!(leader.read_index_ready(&core, read_index));
        assert!(leader.can_serve_reads(&core));
    }

    #[test]
    fn stale_read_index_response_is_ignored() {
        let (mut core, mut leader, _) = test_setup();
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });
        core.commit_index = 1;
        core.last_applied = 1;

        let (read_index, _) = leader.request_read_index(&core).unwrap();
        let stale = Message {
            from: NodeId(1),
            to: NodeId(0),
            term: 1,
            payload: Payload::ReadIndexResponse(ReadIndexResponse { id: 99, read_index }),
        };
        leader.step(&mut core, Event::Message(stale));

        assert!(!leader.read_index_ready(&core, read_index));
    }

    #[test]
    fn pending_read_index_retries_on_heartbeat() {
        let (mut core, mut leader, _) = test_setup();
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });
        core.commit_index = 1;
        core.last_applied = 1;

        let (read_index, _) = leader.request_read_index(&core).unwrap();
        leader.heartbeat_deadline = core.ticks;

        let StepResult { effects, .. } = leader.step(&mut core, Event::Tick);
        let retries = effects
            .messages
            .iter()
            .filter(|msg| matches!(msg.payload, Payload::ReadIndexRequest(_)))
            .count();

        assert_eq!(retries, 2);
        leader.step(
            &mut core,
            Event::Message(Message {
                from: NodeId(1),
                to: NodeId(0),
                term: 1,
                payload: Payload::ReadIndexResponse(ReadIndexResponse { id: 1, read_index }),
            }),
        );
        assert!(leader.read_index_ready(&core, read_index));
    }

    #[test]
    fn read_index_uses_request_configuration_for_quorum() {
        let (mut core, mut leader, _) = test_setup();
        core.log_mut().append(Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Command(Command(vec![1])),
        });
        core.commit_index = 1;
        core.last_applied = 1;

        let (read_index, _) = leader.request_read_index(&core).unwrap();
        core.log_mut().append(Entry {
            term: 1,
            index: 2,
            payload: EntryPayload::Config(Configuration::simple([
                NodeId(0),
                NodeId(1),
                NodeId(2),
                NodeId(3),
            ])),
        });

        assert!(core.effective_config().is_voter(NodeId(3)));
        leader.step(
            &mut core,
            Event::Message(Message {
                from: NodeId(1),
                to: NodeId(0),
                term: 1,
                payload: Payload::ReadIndexResponse(ReadIndexResponse { id: 1, read_index }),
            }),
        );

        assert!(leader.read_index_ready(&core, read_index));
    }

    #[test]
    fn read_index_returns_confirmed_index() {
        let config = Config::new(NodeId(0)).with_parallel_disk_write(false).with_pipelining(false);
        let mut core = Core::new(config, &[NodeId(0)]);
        core.persistent.term = 1;
        let (mut leader, _) = Leader::new(&mut core);

        // Propose multiple entries
        leader.propose(&mut core, Command(vec![1]));
        leader.propose(&mut core, Command(vec![2]));
        leader.propose(&mut core, Command(vec![3]));
        core.last_applied = 3;

        assert_eq!(core.commit_index, 3);
        let (read_index, _) = leader.request_read_index(&core).unwrap();
        assert_eq!(read_index, 3);
        assert_eq!(leader.read_index(&core), 3);
    }
}
