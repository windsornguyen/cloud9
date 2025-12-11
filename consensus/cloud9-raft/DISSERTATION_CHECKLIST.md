# Raft Dissertation Implementation Checklist

Tracking implementation against Diego Ongaro's dissertation verbatim.
Each section maps to specific tests.

## Chapter 3: Basic Raft Algorithm

### §3.2 Safety Properties (Figure 3.2)

- [x] **Election Safety**: At most one leader can be elected in a given term
  - Test: `election_safety`, `cluster_election_safety` ✓

- [x] **Leader Append-Only**: A leader never overwrites or deletes entries in its log; it only appends new entries
  - Test: `log_entries_extended_not_conflicted` ✓

- [x] **Log Matching**: If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index
  - Test: `log_matching_invariant` ✓

- [x] **Leader Completeness**: If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms
  - Test: `log_entries_extended_not_conflicted` (new leader must extend committed entries) ✓

- [x] **State Machine Safety**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index
  - Guaranteed by: Log Matching + Leader Completeness + commit rules ✓

### §3.3 Raft Basics

- [x] Server states: follower, candidate, leader
  - Test: `starts_as_follower`, `initial_state` ✓

- [x] Terms are numbered with consecutive integers, increase monotonically
  - Test: `term_monotonicity`, `term_never_decreases` ✓

- [x] Each term begins with an election
  - Structural: Candidate increments term when starting election ✓
  - Test: `new_candidate_increments_term` ✓

- [x] At most one leader per term (some terms have no leader due to split vote)
  - Test: `election_safety`, `cluster_election_safety` ✓

- [x] Terms act as logical clock; servers exchange current term on every RPC
  - Structural: All `Message` structs contain `term` field ✓

- [x] If one server's term < other's, it updates to larger value
  - Test: `steps_down_on_higher_term` (in candidate, leader, precandidate) ✓

- [x] If candidate/leader discovers term is out of date, immediately reverts to follower
  - Test: `steps_down_on_higher_term` ✓

- [x] Server rejects request with stale term number
  - Test: `stale_messages_rejected_efficiently`, `stale_messages_no_persistence` ✓

### §3.4 Leader Election

- [x] Servers start as followers
  - Test: `starts_as_follower`, `initial_state` ✓

- [x] Follower remains follower while receiving valid RPCs from leader/candidate
  - Test: `heartbeat_resets_deadline` ✓

- [x] Leaders send periodic heartbeats (empty AppendEntries)
  - Test: `heartbeat_on_tick`, `new_leader_sends_heartbeats`, `exactly_one_broadcast_per_heartbeat` ✓

- [x] Election timeout triggers candidacy
  - Test: `election_timeout_triggers_candidacy` ✓

- [x] To begin election: increment term, vote for self, send RequestVote to all
  - Test: `new_candidate_increments_term`, `new_candidate_votes_for_self`, `new_candidate_sends_vote_requests` ✓

- [x] Candidate wins with majority votes for same term
  - Test: `wins_with_majority`, `candidate_wins_with_majority` ✓

- [x] Each server votes for at most one candidate per term (first-come-first-served)
  - Test: `votes_only_once_per_term`, `vote_once_per_term` ✓

- [x] Candidate receiving AppendEntries from leader with term >= current: recognize leader, become follower
  - Test: `steps_down_on_append_from_leader`, `candidate_steps_down_on_leader_heartbeat` ✓

- [x] Split vote: no winner, new election after timeout
  - Test: `election_timeout_restarts_election`, `split_vote_resolves_with_randomization` ✓

- [x] Randomized election timeouts prevent repeated split votes
  - Test: `randomized_timeouts_differ_across_nodes`, `simultaneous_elections_converge` ✓

### §3.5 Log Replication

- [x] Leader appends command to log, issues AppendEntries to all
  - Test: `propose_appends_and_replicates`, `leader_can_propose` ✓

- [x] Entry committed once replicated on majority
  - Test: `commits_on_majority_ack`, `single_node_commits_immediately` ✓

- [x] Commit also commits all preceding entries
  - Structural: `commit_index` is monotonic; advancing it commits all prior entries ✓

- [x] Leader tracks commitIndex, includes in AppendEntries
  - Structural: `AppendRequest.leader_commit` field ✓
  - Test: `log_entries_extended_not_conflicted` (verifies commit propagation) ✓

- [x] Follower applies committed entries in log order
  - Test: `committed_entries_iteration` ✓

- [x] Log Matching Property part 1: Same index+term → same command
  - Structural: `Entry` contains `(term, index, payload)` - uniquely identified ✓

- [x] Log Matching Property part 2: Same index+term → identical prefix
  - Test: `log_matching_invariant` ✓

- [x] AppendEntries consistency check: prevLogIndex + prevLogTerm
  - Test: `rejects_append_with_wrong_prev` ✓

- [x] Leader maintains nextIndex per follower
  - Test: `leader_initializes_indices` ✓

- [x] On rejection, leader decrements nextIndex and retries
  - Test: `retries_on_rejection`, `rejection_hint_avoids_linear_backoff` ✓

- [x] Leader never overwrites/deletes own log entries
  - Structural: Leader only appends; `log_entries_extended_not_conflicted` verifies ✓

### §3.6 Safety

#### §3.6.1 Election Restriction

- [x] Voter denies vote if own log is more up-to-date than candidate's
  - Test: `test_voter_denies_if_own_log_more_up_to_date` ✓

- [x] "More up-to-date" = higher last term, or same term + longer
  - Test: `test_log_up_to_date_comparison` ✓

#### §3.6.2 Committing Entries from Previous Terms

- [x] Leader cannot conclude old entry committed just by counting replicas
  - Test: `test_only_commit_current_term_by_counting` ✓

- [x] Only commit current-term entries by counting; prior entries commit indirectly
  - Test: `test_only_commit_current_term_by_counting` ✓

### §3.7 Follower and Candidate Crashes

- [ ] RPCs retried indefinitely on failure
  - (Implementation detail, not directly testable in pure state machine)

- [x] RPCs are idempotent (repeated RPC causes no harm)
  - Test: `test_append_entries_idempotent` ✓

### §3.8 Persisted State and Server Restarts

- [x] Must persist: currentTerm, votedFor, log[]
  - Impl: `Effects.persist` flag signals when persistence required ✓
  - Test: `persistence_only_on_state_change`, `no_persist_for_readonly_operations` ✓

- [x] commitIndex can be reinitialized to zero on restart
  - Impl: `commit_index` is volatile; recovered from log scan after restart ✓

### §3.9 Timing and Availability

- [x] broadcastTime << electionTimeout << MTBF
  - Impl: Configurable via `Config::with_election_timeout()`, `with_heartbeat_interval()` ✓
  - Default: heartbeat=75ms, election=150-300ms (2:1 ratio minimum) ✓

### §3.10 Leadership Transfer (Extension)

- [x] TimeoutNow causes target to start election immediately
  - Test: `test_timeout_now_triggers_immediate_election` ✓

---

## Chapter 4: Cluster Membership Changes

### §4.1-4.2 Single-Server Changes

- [x] Add one server at a time, guaranteeing overlapping majorities
  - Impl: `MembershipMode::SingleServer` in `membership.rs`
  - Test: `test_single_server_add_voter`, `test_single_server_remove_voter`

- [x] Configuration takes effect when appended (not committed)
  - Impl: `effective_config()` returns latest config from log

- [x] Only one configuration change pending at a time
  - Impl: `has_pending_config()` check in `propose_config_change()`
  - Test: `test_rejects_change_during_joint`

### §4.3 Joint Consensus (Arbitrary Changes)

- [x] C_{old,new} requires majorities from both old and new configs
  - Impl: `Configuration::Joint` variant, `has_quorum()` checks both
  - Test: `test_joint_config_quorum`

- [x] Automatically append C_new when C_{old,new} commits
  - Impl: `process_committed_configs()` → `append_transition_target()`

- [x] Leader not in new config steps down after committing change
  - Impl: `try_auto_transfer()` sends TimeoutNow before stepping down

### §4.2.2 Learner Replicas

- [x] Non-voting learners receive log but don't count in quorum
  - Impl: `Configuration::learners`, `is_learner()`, `replication_peers()`
  - Test: `test_learners_can_be_added_and_promoted`

- [x] Must be learner before becoming voter (catch-up requirement)
  - Impl: `ConfigChangeError::PromoteRequiresLearner`
  - Test: `test_add_voter_rejects_non_learner`

---

## Chapter 5: Log Compaction

### §5 Snapshotting

- [x] Snapshot includes last included index and term
  - Impl: `Log::snapshot_index`, `Log::snapshot_term`

- [x] Log entries before snapshot can be discarded
  - Impl: `Log::truncate_prefix()`
  - Test: `test_truncate_prefix_discards_entries`

- [x] InstallSnapshot RPC for slow followers (Figure 5.3)
  - Impl: `InstallSnapshotRequest`, `InstallSnapshotResponse` in `event.rs`
  - Impl: `Follower::handle_install_snapshot()`

- [x] Leader detects follower needing snapshot (next_index <= snapshot_index)
  - Impl: `Leader::send_append_to()` emits `SendSnapshot` effect
  - Test: `test_detects_slow_follower_needing_snapshot`

- [x] Follower accepts snapshot if more recent than current state
  - Impl: `Log::install_snapshot()`
  - Test: `test_follower_accepts_newer_snapshot`

---

## Chapter 6: Client Interaction

### §6.3 Implementing Linearizable Semantics (Client Sessions)

- [x] Clients assigned unique identifiers
  - Impl: `ClientId` type in `cloud9-raft-io/session.rs`
  - Test: `test_register_client_returns_unique_ids`

- [x] Requests carry client_id and sequence number
  - Impl: `SessionRequest<T>` struct
  - Test: `test_session_request_creation`

- [x] Track last completed sequence per client
  - Impl: `SessionTracker`, `ClientSession`
  - Test: `test_record_completion_ignores_lower_sequence`

- [x] Duplicate requests return cached response
  - Impl: `SessionTracker::check_duplicate()` → `DuplicateCheck::Duplicate`
  - Test: `test_duplicate_request_returns_cached_response`

- [x] Session state serializable for replication
  - Impl: `SessionTracker` derives `Serialize, Deserialize`
  - Test: `test_tracker_serialization`

### §6.4 Processing Read-Only Queries

- [x] Leader must have committed entry from current term before serving reads
  - Impl: `Leader::can_serve_reads()` in `cloud9-raft/leader.rs`
  - Test: `test_cannot_serve_reads_without_current_term_commit`

- [x] Read index = commit index at time of request
  - Impl: `Leader::read_index()`
  - Test: `test_read_index_returns_commit_index`

- [x] Heartbeat round confirms leadership before serving read
  - Impl: `ReadIndexCoordinator` in `cloud9-raft-io/read_index.rs`
  - Test: `test_read_completes_on_quorum`

- [x] Read completes when majority acknowledges heartbeat
  - Impl: `ReadIndexCoordinator::record_ack()`, `check_quorum()`
  - Test: `test_five_node_cluster_requires_three_acks`

---

## Chapter 9: Leader Election Evaluation

### §9.4 PreVote (Extension)

Core PreVote mechanics tested in `src/raft/precandidate.rs` and `src/raft/mod.rs`:
- [x] PreCandidate does NOT increment term until majority grants prevote
- [x] PreCandidate sends PreVoteRequest (not VoteRequest)
- [x] PreCandidate proceeds to Candidate on majority prevotes
- [x] PreCandidate steps down on receiving heartbeat from leader
- [x] Partitioned node term stays constant (no inflation)

Additional §9.4 behaviors tested in `tests/prevote.rs`:
- [x] Voters deny prevote if they've recently heard from a leader
  - Test: `follower_denies_prevote_if_recently_heard_from_leader` ✓

- [x] Voters grant prevote after leader contact timeout expires
  - Test: `follower_grants_prevote_after_leader_timeout` ✓

- [x] Voters deny prevote if candidate's log is not up-to-date
  - Test: `prevote_denied_if_log_not_up_to_date` ✓

- [x] Voters grant prevote if candidate's log is up-to-date
  - Test: `prevote_granted_if_log_up_to_date` ✓

- [x] Leaders deny all prevotes (they are valid leaders)
  - Test: `leader_denies_all_prevotes` ✓

- [x] PreVote next_term must be greater than voter's term
  - Test: `prevote_requires_higher_next_term` ✓

---

## Chapter 10: Performance Optimizations

### §10.2.1 Parallel Disk Write

- [x] Leader writes to disk while simultaneously sending AppendEntries
  - Impl: `Config::parallel_disk_write` toggle (default: true)
  - Impl: `RaftNode::disk_write_complete()` for IO layer callback
  - Test: `parallel_disk_write_correctness` ✓
  - Test: `single_node_parallel_disk_write` ✓

- [x] Leader's match_index updates on disk completion (not on append)
  - Impl: `Leader::propose()` conditionally updates match_index
  - Impl: `Event::DiskWriteComplete` handler updates leader's match_index
  - Test: `naive_disk_write_blocks_until_self_written` (control) ✓

### §10.2.2 Pipelining

- [x] Leader updates next_index optimistically after sending
  - Impl: `Config::pipelining` toggle (default: true)
  - Impl: `Leader::send_append_to()` advances next_index without ACK
  - Test: `pipelining_sends_without_waiting` ✓

- [x] Multiple in-flight AppendEntries per follower
  - Test: `pipelining_sends_without_waiting` (5 entries sent in parallel) ✓

- [x] Rejection handling with pipelining
  - Impl: `Leader::handle_append_response()` uses hint for next_index
  - Test: `pipelining_handles_rejection` ✓

### §10.2 Performance Comparison

- [x] Optimized vs naive round-trip efficiency
  - Test: `optimized_commit_efficiency` ✓
  - Test: `naive_commit_efficiency` ✓
  - Test: `optimization_provides_benefit` ✓

---

## Test Status

Run: `cargo test --package cloud9-raft --package cloud9-raft-io`

Legend:
- [ ] Not implemented
- [x] Implemented and passing
