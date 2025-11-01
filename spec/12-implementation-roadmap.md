# Implementation Roadmap

**Question**: How do we build Cloud9 incrementally with provable correctness at each stage?

**Answer**: Six milestones with concrete deliverables and comprehensive testing gates.

## Philosophy

Build complexity incrementally. Each milestone must:
1. Add exactly one core capability
2. Prove correctness before proceeding
3. Maintain all previous guarantees
4. Ship production-quality tests

No milestone is "done" until its test suite catches real bugs and survives adversarial conditions.

## Testing Strategy Per Milestone

Each milestone requires multiple testing levels:

**Unit tests**: Fast, deterministic, cover individual components.

**Property tests**: QuickCheck-style invariant checking with random inputs.

**Integration tests**: Multi-component interaction under normal conditions.

**Loom tests**: Concurrency verification with exhaustive thread interleaving.

**Simulation tests**: Adversarial conditions (network partitions, crashes, clock skew) with deterministic seeds.

**Jepsen-style tests**: Distributed system verification with real network partitions, node crashes, and linearizability checking.

**Performance tests**: Regression detection, not optimization theater. Gate: "don't get slower without justification."

## Milestone 1: Single-Node MVCC KV

**Goal**: Prove we can build a correct transactional storage engine before adding distribution.

**What ships**:
- MVCC key-value storage (versioned by timestamp)
- Write-ahead log (WAL) for durability
- Crash recovery (replay WAL to reconstruct state)
- Single-node transactions (no replication, no Raft)
- Snapshot isolation guarantee

**What doesn't ship**:
- Replication
- Consensus
- Multi-node anything
- SQL (KV only)
- Distributed transactions

**API surface**:
```rust
trait MVCCStorage {
    fn write(&mut self, key: Key, value: Value, ts: Timestamp) -> Result<()>;
    fn read(&self, key: Key, ts: Timestamp) -> Result<Option<Value>>;
    fn scan(&self, range: Range<Key>, ts: Timestamp) -> Result<Vec<(Key, Value)>>;
    fn begin_txn(&mut self) -> TxnHandle;
    fn commit_txn(&mut self, txn: TxnHandle) -> Result<Timestamp>;
    fn abort_txn(&mut self, txn: TxnHandle);
}
```

**Storage layout**:
```
key@timestamp -> value
key@100 -> "v1"
key@150 -> "v2"  // Read at ts=120 returns "v1", ts=200 returns "v2"
```

**WAL format**:
```
[BeginTxn(txn_id)]
[Write(txn_id, key, value, provisional_ts)]
[CommitTxn(txn_id, commit_ts)]
```

**Correctness invariants**:
1. Read at timestamp T returns most recent write with ts ≤ T
2. Committed writes survive crashes
3. Aborted transactions leave no visible state
4. Snapshot reads see consistent point-in-time view

**Test requirements**:

**Unit tests**:
- Write and read single key at multiple timestamps
- Read returns correct version for given timestamp
- Scan returns keys in order with correct versions
- Abort removes uncommitted writes

**Property tests** (proptest/quickcheck):
```rust
#[test]
fn mvcc_snapshot_isolation(operations: Vec<Operation>) {
    let storage = MVCCStorage::new();
    let mut committed_state: BTreeMap<Timestamp, BTreeMap<Key, Value>> = BTreeMap::new();

    for op in operations {
        match op {
            Write(key, value, ts) => {
                storage.write(key, value, ts)?;
                committed_state.entry(ts).or_default().insert(key, value);
            }
            Read(key, ts) => {
                let result = storage.read(key, ts)?;
                let expected = committed_state
                    .range(..=ts)
                    .rev()
                    .find_map(|(_, state)| state.get(&key));
                assert_eq!(result, expected);
            }
        }
    }
}

#[test]
fn mvcc_write_write_conflict() {
    // Two concurrent writers to same key should serialize
    // First writer wins, second detects conflict
}
```

**Crash recovery tests**:
```rust
#[test]
fn recover_from_crash_during_commit() {
    let mut storage = MVCCStorage::new();
    storage.write(key, value, ts)?;

    // Simulate crash before commit completes
    drop(storage);  // Dirty shutdown, no flush

    // Recover and verify
    let storage = MVCCStorage::recover()?;
    assert_eq!(storage.read(key, ts)?, Some(value));
}

#[test]
fn replay_wal_reconstructs_state() {
    // Write sequence: Begin, Write(k1), Write(k2), Commit
    // Crash after commit but before state machine applies
    // Replay WAL should reconstruct exact state
}
```

**Loom tests** (concurrency verification):
```rust
#[test]
fn concurrent_reads_and_writes() {
    loom::model(|| {
        let storage = Arc::new(MVCCStorage::new());

        let writer = {
            let storage = storage.clone();
            loom::thread::spawn(move || {
                storage.write(key, value, ts)?;
            })
        };

        let reader = {
            let storage = storage.clone();
            loom::thread::spawn(move || {
                storage.read(key, ts)?;
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
        // Loom explores all possible interleavings
    });
}
```

**Gate**: Property tests must run 10,000+ random operation sequences without finding invariant violations. Loom tests must explore all thread interleavings without deadlocks or data races.

## Milestone 2: Raft Replication

**Goal**: Add consensus-based replication while maintaining single-range (no sharding) semantics.

**What ships**:
- Raft consensus implementation (leader election, log replication, safety)
- Multi-replica MVCC storage (replicate writes to quorum)
- Leaseholder reads (linearizable reads from leader)
- Follower reads at closed timestamp (bounded-stale reads)
- Raft-driven WAL (consensus log becomes WAL)

**What doesn't ship**:
- Multi-range sharding
- Cross-shard transactions
- SQL
- Timestamp oracle

**Consensus Driver Interface**:
```rust
trait ConsensusDriver {
    fn propose(&mut self, cmd: Command) -> Result<LogIndex>;
    fn poll_committed(&mut self) -> Vec<CommittedEntry>;
    fn leader(&self) -> Option<ReplicaId>;
    fn transfer_leadership(&mut self, target: ReplicaId) -> Result<()>;
}
```

**Replicated state machine**:
```
Raft log entry = MVCC operation (write/commit/abort)
Apply function = append to MVCC storage
```

**Closed timestamp protocol**:
```rust
struct ClosedTimestamp {
    timestamp: Timestamp,
    raft_index: LogIndex,
}

impl RaftLeader {
    fn advance_closed_timestamp(&mut self) {
        let safe_ts = self.hlc.now() - self.max_clock_skew;
        let min_active = self.active_txns.min_timestamp();
        self.closed_ts = Timestamp::min(safe_ts, min_active - 1);
        self.broadcast_closed_ts();  // Piggyback on heartbeats
    }
}

impl RaftFollower {
    fn can_serve_read(&self, read_ts: Timestamp) -> bool {
        read_ts <= self.closed_ts && self.applied_index >= self.closed_index
    }
}
```

**Correctness invariants**:
1. Quorum writes are durable (survive F failures in 2F+1 cluster)
2. Leader reads are linearizable
3. Follower reads see consistent snapshot at closed_ts
4. Log replication preserves order
5. Leadership changes don't lose committed data

**Test requirements**:

**Unit tests**:
- Leader election with 3/5/7 nodes
- Log replication to quorum
- Follower catches up after partition heals
- Closed timestamp advances correctly

**Simulation tests** (deterministic with seeds):
```rust
#[test]
fn partition_minority_nodes() {
    let mut sim = Simulation::new(seed);
    let cluster = sim.create_cluster(5);

    // Partition 2 nodes away from 3-node majority
    sim.partition(&[node1, node2], &[node3, node4, node5]);

    // Majority should elect leader and continue
    sim.step_until(leader_elected);
    assert!(cluster.majority().has_leader());

    // Write should succeed (quorum available)
    cluster.majority().write(key, value)?;

    // Heal partition
    sim.heal();

    // Minority nodes should catch up
    sim.step_until(all_nodes_synced);
    assert_eq!(cluster.all_nodes().read(key)?, Some(value));
}

#[test]
fn leader_crash_during_replication() {
    // Leader proposes write, replicates to 1 follower, crashes
    // New leader elected, should see write (was on quorum)
}

#[test]
fn follower_reads_bounded_stale() {
    // Leader writes at t=100, advances closed_ts=100
    // Follower sees closed_ts=100, serves read at ts=100
    // Verify read returns committed value
}
```

**Jepsen-style tests** (actual network, real crashes):
```rust
#[test]
fn jepsen_linearizability_check() {
    let cluster = deploy_cluster(5);

    // Concurrent clients issuing writes
    let mut history = vec![];
    for _ in 0..1000 {
        let client_id = random_client();
        let op = random_write();
        let result = cluster.execute(op);
        history.push((client_id, op, result));
    }

    // Inject faults
    cluster.kill_random_node();
    cluster.partition_random_nodes();

    // Check linearizability with Knossos/Elle
    assert!(knossos::check_linearizable(&history));
}
```

**Performance tests**:
```rust
#[test]
fn write_latency_regression() {
    // Measure quorum write latency (RTT + replication)
    // Gate: p50 < 10ms, p99 < 50ms (for local 3-node cluster)
}

#[test]
fn follower_read_latency() {
    // Gate: follower reads < 1ms when closed_ts current
}
```

**Gate**: Simulation tests must pass with 100 different random seeds. Jepsen tests must survive 10+ crash/partition scenarios without linearizability violations.

## Milestone 3: Timestamp Oracle + Lock Manager

**Goal**: Add external consistency primitives (HLC-based timestamping, commit-wait, write-write conflict detection).

**What ships**:
- Hybrid Logical Clock (HLC) implementation
- Timestamp oracle (assign commit timestamps)
- Lock manager (detect write-write conflicts)
- Commit-wait protocol (ensure real-time order)
- Write-skew prevention (detect read-write conflicts)

**What doesn't ship**:
- Cross-shard transactions (still single-range)
- SQL
- 2PC

**HLC implementation**:
```rust
struct HybridLogicalClock {
    physical: Timestamp,  // Wall clock
    logical: u64,          // Counter for same physical time
}

impl HybridLogicalClock {
    fn now(&mut self) -> Timestamp {
        let wall = system_time();
        if wall > self.physical {
            self.physical = wall;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        Timestamp::new(self.physical, self.logical)
    }

    fn observe(&mut self, remote: Timestamp) {
        let wall = system_time();
        self.physical = max(wall, remote.physical);
        if self.physical == remote.physical {
            self.logical = max(self.logical, remote.logical) + 1;
        } else {
            self.logical = 0;
        }
    }
}
```

**Commit-wait protocol**:
```rust
fn commit_transaction(txn: &Transaction, hlc: &HLC, epsilon: Duration) -> Result<()> {
    let commit_ts = select_commit_timestamp(txn, hlc);

    // Write commit record to Raft log
    replicate_commit(txn.txn_id, commit_ts)?;

    // Wait until all nodes' clocks > commit_ts
    let target = commit_ts + epsilon;
    while hlc.now() < target {
        sleep(1ms);
    }

    // Now safe: any future operation gets ts > commit_ts
    Ok(())
}
```

**Lock manager**:
```rust
struct LockManager {
    locks: HashMap<Key, Lock>,
}

struct Lock {
    txn_id: TxnId,
    timestamp: Timestamp,
    mode: LockMode,  // Shared | Exclusive
}

impl LockManager {
    fn acquire(&mut self, key: Key, txn_id: TxnId, mode: LockMode) -> Result<()> {
        if let Some(existing) = self.locks.get(&key) {
            if existing.txn_id != txn_id {
                return Err(LockConflict);
            }
        }
        self.locks.insert(key, Lock { txn_id, mode, ... });
        Ok(())
    }
}
```

**Write-skew detection**:
```rust
#[test]
fn prevent_write_skew() {
    // Classic scenario:
    // T1: read(x)=0, read(y)=0, write(x=1)
    // T2: read(x)=0, read(y)=0, write(y=1)
    // Without conflict detection: both commit (invariant x+y>0 violated)

    let storage = MVCCStorage::new();

    let t1 = storage.begin_txn();
    assert_eq!(storage.read_in_txn(&t1, "x")?, 0);
    assert_eq!(storage.read_in_txn(&t1, "y")?, 0);
    storage.write_in_txn(&t1, "x", 1)?;

    let t2 = storage.begin_txn();
    assert_eq!(storage.read_in_txn(&t2, "x")?, 0);
    assert_eq!(storage.read_in_txn(&t2, "y")?, 0);
    storage.write_in_txn(&t2, "y", 1)?;

    // One must abort due to read-write conflict
    let r1 = storage.commit_txn(t1);
    let r2 = storage.commit_txn(t2);
    assert!(r1.is_err() || r2.is_err());
}
```

**Correctness invariants**:
1. HLC never goes backward
2. Commit-wait ensures ts_commit < ts_next_operation
3. Write-write conflicts abort one transaction
4. Serializable isolation (no anomalies: write-skew, dirty read, lost update)

**Test requirements**:

**Unit tests**:
- HLC monotonicity under concurrent updates
- Commit-wait duration equals epsilon
- Lock acquisition blocks conflicting transactions

**Property tests**:
```rust
#[test]
fn hlc_causality(events: Vec<(NodeId, Event)>) {
    // Events with happens-before relationship must have ts1 < ts2
    // Property: if event A sends message to B, ts_B > ts_A
}

#[test]
fn serializable_isolation(txns: Vec<Transaction>) {
    // Run concurrent transactions
    // Verify committed state is equivalent to some serial order
}
```

**Anomaly tests** (based on Adya's formalization):
```rust
#[test]
fn no_g1a_aborted_reads() {
    // T1 writes x, aborts
    // T2 reads x (should not see T1's write)
}

#[test]
fn no_g1b_intermediate_reads() {
    // T1 writes x=1, then x=2, commits
    // T2 should never read x=1 (intermediate value)
}

#[test]
fn no_g1c_circular_information_flow() {
    // T1 writes x, T2 writes y, each reads the other
    // One must abort (no cycles in dependency graph)
}

#[test]
fn no_g2_item_write_skew() {
    // The classic write-skew scenario
    // Prevented by tracking read-write dependencies
}
```

**Performance tests**:
```rust
#[test]
fn commit_wait_latency() {
    // Gate: commit-wait adds ≤ epsilon latency (50-100ms on NTP, 10-50ms on PTP)
}

#[test]
fn lock_manager_overhead() {
    // Gate: lock acquisition < 10μs (in-memory hash lookup)
}
```

**Gate**: All Adya anomaly tests must pass. Property tests with 10,000+ random transaction schedules find zero serialization violations.

## Milestone 4: Distributed Transactions (2PC)

**Goal**: Enable transactions spanning multiple ranges with atomicity and external consistency.

**What ships**:
- Two-phase commit protocol (prepare, commit/abort)
- Transaction coordinator (drives 2PC)
- Cross-shard intent handling (provisional writes)
- Intent resolution (convert to committed values)
- Transaction recovery (handle coordinator crash)

**What doesn't ship**:
- Range splits/merges
- SQL
- Dynamic resharding

**Transaction coordinator**:
```rust
struct TransactionCoordinator {
    txn_id: TxnId,
    participants: Vec<RangeId>,
    state: TxnState,
}

enum TxnState {
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborted,
}

impl TransactionCoordinator {
    async fn execute_2pc(&mut self) -> Result<()> {
        // Phase 0: Write intents
        for participant in &self.participants {
            participant.write_intent(self.txn_id, &self.writes)?;
        }

        // Phase 1: Prepare
        let commit_ts = self.select_commit_timestamp();
        let votes = self.send_prepare(commit_ts).await?;

        if votes.all(|v| v == Vote::Prepared) {
            // Phase 2: Commit
            self.send_commit(commit_ts).await?;
            self.commit_wait(commit_ts).await;
            Ok(())
        } else {
            // Phase 2: Abort
            self.send_abort().await?;
            Err(TransactionAborted)
        }
    }
}
```

**Intent structure**:
```rust
struct Intent {
    txn_id: TxnId,
    key: Key,
    value: Value,
    provisional_ts: Timestamp,
}

// On-disk layout during transaction:
// key -> Intent { txn_id, value, provisional_ts }
// key@50 -> CommittedValue { "old_value" }

// After commit at ts=100:
// key@100 -> CommittedValue { "new_value" }
// key@50 -> CommittedValue { "old_value" }
```

**Transaction recovery**:
```rust
struct TransactionRecord {
    txn_id: TxnId,
    coordinator: NodeId,
    participants: Vec<RangeId>,
    commit_timestamp: Timestamp,
    status: TxnStatus,
    heartbeat: Timestamp,
}

async fn recover_transaction(txn_id: TxnId) -> Result<()> {
    let record = load_transaction_record(txn_id)?;

    if record.heartbeat_expired() {
        if record.status == TxnStatus::Prepared {
            // Coordinator crashed during commit
            // Query participants to recover decision
            let votes = query_participants(&record.participants).await?;
            if votes.all(|v| v == Vote::Prepared) {
                // All prepared, safe to commit
                commit_transaction(txn_id, record.commit_timestamp).await?;
            } else {
                // At least one aborted, must abort
                abort_transaction(txn_id).await?;
            }
        } else {
            // Coordinator crashed before prepare, safe to abort
            abort_transaction(txn_id).await?;
        }
    }
    Ok(())
}
```

**Correctness invariants**:
1. Atomicity: All participants commit or all abort
2. External consistency: Cross-shard commits respect real-time order
3. No partial visibility: Readers never see subset of transaction's writes
4. Crash recovery preserves atomicity

**Test requirements**:

**Unit tests**:
- 2PC with all participants voting prepared
- 2PC with one participant voting abort
- Intent resolution to committed values
- Transaction record persistence

**Integration tests**:
```rust
#[test]
fn cross_shard_transaction() {
    let cluster = Cluster::new(3);
    let range_a = cluster.create_range(key_range("a".."m"));
    let range_b = cluster.create_range(key_range("n".."z"));

    let txn = cluster.begin_transaction();
    txn.write("alice", "value1")?;  // Range A
    txn.write("zoe", "value2")?;    // Range B
    txn.commit()?;

    // Both writes visible or neither
    let read_txn = cluster.begin_read_only_transaction();
    assert_eq!(read_txn.read("alice")?, Some("value1"));
    assert_eq!(read_txn.read("zoe")?, Some("value2"));
}

#[test]
fn partial_abort_rolls_back_all() {
    // Write to 3 ranges, range 2 votes abort
    // Verify range 1 and 3 don't show writes
}
```

**Simulation tests**:
```rust
#[test]
fn coordinator_crash_during_prepare() {
    let mut sim = Simulation::new(seed);
    let cluster = sim.create_cluster(5);

    let txn = cluster.begin_transaction();
    txn.write("key1", "value1")?;  // Range A
    txn.write("key2", "value2")?;  // Range B

    // Crash coordinator after prepare sent, before commit
    sim.inject_crash(cluster.coordinator(), after = "prepare_sent");

    // New coordinator recovers transaction
    sim.step_until(transaction_recovered);

    // Verify atomicity: either both visible or neither
    let state = cluster.read_all_ranges();
    assert!(
        (state.has("key1") && state.has("key2")) ||
        (!state.has("key1") && !state.has("key2"))
    );
}

#[test]
fn participant_crash_during_commit() {
    // Participant crashes after voting prepared, before applying commit
    // New leader for that range should apply commit after recovery
}

#[test]
fn intent_cleanup_on_abort() {
    // Transaction aborts after writing intents
    // Verify intents are cleaned up, not visible to readers
}
```

**Jepsen-style tests**:
```rust
#[test]
fn distributed_bank_test() {
    // Classic Jepsen test: bank accounts across shards
    // Invariant: sum(all_accounts) = constant

    let cluster = deploy_cluster(5);
    let accounts = vec!["alice", "bob", "charlie"];
    let initial_sum = 1000;

    for account in &accounts {
        cluster.write(account, initial_sum / accounts.len())?;
    }

    // Concurrent transfers between accounts
    for _ in 0..1000 {
        let from = random_account();
        let to = random_account();
        let amount = random(1..100);

        cluster.transfer(from, to, amount)?;  // Cross-shard transaction

        // Inject random faults
        if random() { cluster.kill_random_node(); }
        if random() { cluster.partition_random(); }
    }

    // Check invariant
    let final_sum = cluster.sum_all_accounts();
    assert_eq!(final_sum, initial_sum);
}
```

**Performance tests**:
```rust
#[test]
fn cross_shard_latency() {
    // Gate: 2PC adds ≤ 2 RTT vs single-shard (prepare + commit)
}

#[test]
fn intent_resolution_throughput() {
    // Gate: resolve 10k+ intents/sec per node
}
```

**Gate**: Jepsen-style tests with 10,000+ cross-shard transactions survive random crashes/partitions without invariant violations. Transaction recovery handles all coordinator/participant crash scenarios.

## Milestone 5: Range Splits and Merges

**Goal**: Enable dynamic resharding under live traffic without downtime.

**What ships**:
- Range split protocol (split one range into two)
- Range merge protocol (merge two ranges into one)
- Online rebalancing (move ranges between nodes)
- Split/merge under live traffic (no downtime)
- Load-based split triggers

**What doesn't ship**:
- SQL (still KV only)
- Automatic rebalancing (manual only)

**Range split protocol**:
```rust
struct Range {
    id: RangeId,
    key_range: (Key, Key),  // [start, end)
    replicas: Vec<ReplicaId>,
    split_key: Option<Key>,  // Pending split point
}

async fn split_range(range_id: RangeId, split_key: Key) -> Result<(RangeId, RangeId)> {
    // 1. Find split point (key that divides load roughly in half)
    let split_key = find_split_key(&range_id)?;

    // 2. Freeze writes to range (brief quiesce)
    freeze_range(range_id).await?;

    // 3. Create two new ranges
    let left = create_range(range.start..split_key, range.replicas.clone())?;
    let right = create_range(split_key..range.end, range.replicas.clone())?;

    // 4. Copy data to new ranges (scan + replicate)
    copy_data(&range, &left, &right).await?;

    // 5. Update range registry (atomic switch)
    update_range_registry(range_id, &[left, right]).await?;

    // 6. Resume writes (route to new ranges)
    unfreeze_ranges(&[left, right]).await?;

    Ok((left.id, right.id))
}
```

**Online rebalancing**:
```rust
async fn rebalance_range(range_id: RangeId, target_nodes: Vec<NodeId>) -> Result<()> {
    // 1. Add new replicas as learners
    for node in target_nodes {
        add_replica(range_id, node, ReplicaRole::Learner).await?;
    }

    // 2. Wait for learners to catch up
    wait_for_catchup(range_id).await?;

    // 3. Promote learners to voters (joint consensus)
    promote_replicas(range_id, &target_nodes).await?;

    // 4. Remove old replicas
    remove_old_replicas(range_id).await?;

    Ok(())
}
```

**Load-based split triggers**:
```rust
struct LoadMonitor {
    range_id: RangeId,
    qps: u64,           // Queries per second
    bytes_per_sec: u64,
    cpu_percent: f64,
}

impl LoadMonitor {
    fn should_split(&self) -> bool {
        self.qps > SPLIT_QPS_THRESHOLD ||
        self.bytes_per_sec > SPLIT_BYTES_THRESHOLD ||
        self.cpu_percent > SPLIT_CPU_THRESHOLD
    }
}
```

**Correctness invariants**:
1. No data loss during split/merge
2. No downtime (writes continue during split)
3. Consistent routing (all clients see new ranges after split)
4. Cross-range transactions continue during split

**Test requirements**:

**Unit tests**:
- Split at various key boundaries
- Merge adjacent ranges
- Replica addition/removal
- Range registry updates

**Integration tests**:
```rust
#[test]
fn split_range_under_load() {
    let cluster = Cluster::new(3);
    let range = cluster.create_range(key_range("a".."z"));

    // Write load to range
    let writer = spawn_writer(&cluster, &range, qps = 1000);

    // Split range while writes continue
    let (left, right) = cluster.split_range(range.id, "m").await?;

    // Verify no writes lost
    let written_keys = writer.stop();
    for key in written_keys {
        assert!(cluster.has_key(key));
    }
}

#[test]
fn cross_range_transaction_during_split() {
    // Start transaction writing to range A and B
    // Split range A during transaction
    // Verify transaction still commits atomically
}
```

**Simulation tests**:
```rust
#[test]
fn concurrent_splits_and_merges() {
    let mut sim = Simulation::new(seed);
    let cluster = sim.create_cluster(5);

    // Start with 10 ranges
    let ranges = (0..10).map(|i| cluster.create_range(..)).collect();

    // Concurrent operations
    for _ in 0..100 {
        match random_op() {
            Op::Split => {
                let range = random(&ranges);
                cluster.split_range(range).await?;
            }
            Op::Merge => {
                let (r1, r2) = random_adjacent_ranges(&ranges);
                cluster.merge_ranges(r1, r2).await?;
            }
            Op::Write => {
                cluster.write(random_key(), random_value())?;
            }
        }
    }

    // Verify no data loss
    sim.check_all_writes_visible();
}

#[test]
fn rebalance_during_partition() {
    // Start rebalancing range to new nodes
    // Partition network mid-rebalance
    // Heal partition, verify rebalance completes or safely aborts
}
```

**Performance tests**:
```rust
#[test]
fn split_freeze_duration() {
    // Gate: writes blocked < 100ms during split
}

#[test]
fn rebalance_traffic_overhead() {
    // Gate: rebalancing adds < 10% latency to foreground writes
}
```

**Gate**: Splits complete in < 1 second with < 100ms write freeze. No data loss in 100+ random split/merge/rebalance sequences.

## Milestone 6: SQL + KV Unified Surface

**Goal**: Ship the complete Cloud9 vision: SQL and KV under one transaction.

**What ships**:
- SQL parser (PostgreSQL-compatible dialect)
- SQL planner (query to KV operations)
- SQL executor (scan, filter, join, aggregate)
- KV API (get/put/scan primitives)
- Cross-API transactions (SQL queries + KV operations in one transaction)
- Postgres wire protocol (pg clients connect directly)

**What doesn't ship** (deferred to future):
- Vector indexing
- Automatic rebalancing
- Full Postgres feature parity (foreign keys, triggers, etc.)

**Unified transaction API**:
```rust
trait Transaction {
    // SQL operations
    fn execute_sql(&self, query: &str) -> Result<ResultSet>;

    // KV operations
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn scan(&self, range: Range<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    fn commit(self) -> Result<Timestamp>;
    fn abort(self);
}
```

**Example: SQL + KV in one transaction**:
```sql
BEGIN;
  -- SQL: Complex analytics query
  SELECT user_id, COUNT(*)
  FROM orders
  WHERE amount > 1000
  GROUP BY user_id;

  -- KV: Fast state update
  PUT('agent:state:123', state_blob);

  -- Cross-API join (this is the magic)
  SELECT u.name, kv.session_data
  FROM users u
  JOIN kv_namespace('sessions') kv ON kv.user_id = u.id
  WHERE kv.last_active > NOW() - INTERVAL '1 hour';
COMMIT;
```

**SQL to KV lowering**:
```rust
struct QueryPlan {
    operations: Vec<Operation>,
}

enum Operation {
    Scan { range: Range<Key>, filter: Predicate },
    Get { key: Key },
    Put { key: Key, value: Value },
    Filter { predicate: Predicate },
    Join { left: Box<Operation>, right: Box<Operation>, condition: JoinCondition },
    Aggregate { group_by: Vec<Column>, aggregates: Vec<Aggregate> },
}

fn plan_sql_query(sql: &str) -> Result<QueryPlan> {
    // Parse SQL
    let ast = parse_sql(sql)?;

    // Optimize
    let optimized = optimize(ast)?;

    // Lower to KV operations
    let plan = lower_to_kv(optimized)?;

    Ok(plan)
}
```

**KV namespace projection**:
```rust
// Define schema for KV namespace (typed projection)
CREATE TABLE sessions AS KV_NAMESPACE('sessions') (
    user_id UUID PRIMARY KEY,
    session_data JSONB,
    last_active TIMESTAMP
);

// Now can join SQL tables with KV data
SELECT u.name, s.session_data
FROM users u
JOIN sessions s ON s.user_id = u.id;
```

**Postgres wire protocol**:
```rust
async fn handle_postgres_client(stream: TcpStream) -> Result<()> {
    let mut conn = PostgresConnection::new(stream);

    // Handshake
    conn.handshake().await?;

    // Query loop
    loop {
        match conn.read_message().await? {
            PgMessage::Query(sql) => {
                let result = execute_sql(&sql).await?;
                conn.send_result(result).await?;
            }
            PgMessage::Parse(stmt) => {
                let plan = parse_and_plan(&stmt).await?;
                conn.cache_plan(plan);
            }
            PgMessage::Execute(plan_id) => {
                let plan = conn.get_cached_plan(plan_id);
                let result = execute_plan(plan).await?;
                conn.send_result(result).await?;
            }
            PgMessage::Terminate => break,
        }
    }

    Ok(())
}
```

**Correctness invariants**:
1. SQL queries see same snapshot as concurrent KV operations
2. Cross-API joins return consistent results
3. SQL transactions have same ACID guarantees as KV transactions
4. Postgres clients can't tell Cloud9 from real Postgres (compatibility)

**Test requirements**:

**Unit tests**:
- Parse SQL into AST
- Plan optimization (predicate pushdown, index selection)
- Lower SQL to KV operations
- Execute KV operations in transaction

**Integration tests**:
```rust
#[test]
fn sql_kv_unified_transaction() {
    let cluster = Cluster::new(3);

    // Create SQL table
    cluster.execute_sql("CREATE TABLE users (id UUID, name TEXT)")?;
    cluster.execute_sql("INSERT INTO users VALUES ('123', 'alice')")?;

    // Same transaction: SQL + KV
    let txn = cluster.begin_transaction();

    // SQL read
    let result = txn.execute_sql("SELECT name FROM users WHERE id = '123'")?;
    assert_eq!(result[0]["name"], "alice");

    // KV write
    txn.put(b"session:123", b"active")?;

    txn.commit()?;

    // Verify both visible
    let new_txn = cluster.begin_read_only_transaction();
    assert_eq!(new_txn.execute_sql("SELECT name FROM users WHERE id = '123'")?, vec![...]);
    assert_eq!(new_txn.get(b"session:123")?, Some(b"active"));
}

#[test]
fn cross_api_join() {
    let cluster = Cluster::new(3);

    // SQL table
    cluster.execute_sql("CREATE TABLE users (id UUID, name TEXT)")?;
    cluster.execute_sql("INSERT INTO users VALUES ('123', 'alice')")?;

    // KV namespace
    cluster.put(b"sessions:123", b"session_data")?;

    // Define KV projection
    cluster.execute_sql(r#"
        CREATE TABLE sessions AS KV_NAMESPACE('sessions') (
            user_id UUID PRIMARY KEY,
            data BYTEA
        )
    "#)?;

    // Join SQL + KV
    let result = cluster.execute_sql(r#"
        SELECT u.name, s.data
        FROM users u
        JOIN sessions s ON s.user_id = u.id
    "#)?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["name"], "alice");
}
```

**Compatibility tests** (PostgreSQL test suite):
```rust
#[test]
fn run_postgres_test_suite() {
    // Run subset of PostgreSQL's regression tests
    // Gate: 90%+ pass rate on core features (SELECT, JOIN, WHERE, GROUP BY)
}
```

**Performance tests**:
```rust
#[test]
fn sql_kv_parity() {
    // Gate: KV operations accessed via SQL have < 10% overhead vs native KV API
}

#[test]
fn cross_api_join_performance() {
    // Gate: Join between SQL table and KV namespace < 2x slower than SQL-only join
}
```

**Gate**: Postgres compatibility tests pass 90%+. Cross-API joins return correct results under concurrent writes. Postgres wire protocol compatible with psql, libpq, and popular ORMs (SQLAlchemy, Diesel, pgx).

## Testing Infrastructure Requirements

Each milestone requires these testing capabilities:

**Deterministic simulation framework**:
- Control time, network, and node crashes
- Reproducible with seeds
- Fast iteration (seconds, not minutes)

**Jepsen-style verification**:
- Real network, real crashes
- Linearizability checking (Knossos/Elle)
- History analysis (dependency graphs)

**Property-based testing**:
- QuickCheck/proptest integration
- Invariant checking with random inputs
- Shrinking to minimal failing cases

**Loom for concurrency**:
- Exhaustive thread interleaving exploration
- Deadlock detection
- Data race detection

**Performance regression detection**:
- Automated benchmarking
- Statistical significance testing
- Alerts on regressions

## Milestone Gates Summary

**Milestone 1**: Property tests find zero MVCC invariant violations in 10,000+ random operation sequences.

**Milestone 2**: Simulation tests pass with 100 random seeds. Jepsen tests survive 10+ crash/partition scenarios.

**Milestone 3**: All Adya anomaly tests pass. Zero serialization violations in 10,000+ transaction schedules.

**Milestone 4**: Jepsen bank test passes 10,000+ cross-shard transactions with random faults.

**Milestone 5**: Splits complete in < 1 second with < 100ms write freeze. Zero data loss in 100+ split/merge/rebalance sequences.

**Milestone 6**: Postgres compatibility 90%+. Cross-API joins correct under concurrent writes.

## Development Timeline Estimate

**Milestone 1**: 2-3 months (MVCC + WAL + crash recovery + property tests)

**Milestone 2**: 3-4 months (Raft + replication + simulation tests)

**Milestone 3**: 2-3 months (HLC + commit-wait + lock manager + anomaly tests)

**Milestone 4**: 3-4 months (2PC + intent handling + recovery + Jepsen tests)

**Milestone 5**: 2-3 months (splits/merges + online rebalancing)

**Milestone 6**: 4-6 months (SQL parser + planner + executor + Postgres protocol)

**Total**: 16-23 months (assuming 1-2 engineers)

This is aggressive but achievable with:
- Ruthless scope discipline (cut non-essential features)
- Heavy reuse (RocksDB for storage, existing SQL parser)
- Test-first development (bugs caught early, not in production)

## Success Criteria

Cloud9 is production-ready when:

1. **Correctness**: All test gates pass. Zero known correctness bugs.

2. **Performance**: Within 2x of CockroachDB on standard benchmarks (YCSB, TPC-C).

3. **Stability**: Runs for 7+ days under load without crashes or leaks.

4. **Usability**: Postgres clients connect and execute queries without code changes.

5. **Documentation**: Every API has examples. Every failure mode has runbook.

**Not** production-ready until all of these are true. No shortcuts.

## Principles

**Build depth, not breadth**: Better to have bulletproof MVCC than half-finished SQL + KV + vector.

**Test like you'll regret not testing**: Every milestone gate must catch real bugs. If tests don't find issues, the tests are wrong.

**Cut scope aggressively**: If a milestone is slipping, cut features, don't cut tests.

**Ship when proven, not when scheduled**: Timelines are estimates. Correctness is non-negotiable.

**No resume-driven development**: Build what users need, not what looks good on LinkedIn. Complexity is a cost, not a feature.
