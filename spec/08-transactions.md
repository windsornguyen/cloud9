# Transaction Protocol

**Question**: How do we execute multi-shard transactions that maintain external consistency?

**Answer**: Two-phase commit (2PC) with MVCC intents and coordinator-driven commit timestamp assignment.

## Overview

Cloud9 transactions must satisfy:
1. **Atomicity**: All writes succeed or all fail (no partial writes visible)
2. **External consistency**: If T₁ finishes before T₂ starts in real time, T₁'s writes are visible to T₂
3. **Snapshot isolation**: Read-only transactions see a consistent point-in-time snapshot
4. **Lock-free reads**: Read-only transactions never block or wait for locks

This document specifies the protocol that achieves these guarantees across sharded data.

## Transaction Types

### Read-Only Transactions

**Characteristics**:
- No writes, no intents, no locks
- Pick snapshot timestamp `t_r` at start
- Never block, never wait for locks
- No 2PC coordination needed

**Protocol**:
```
1. Client starts transaction
2. Coordinator picks t_r = now() (from HLC or TSO)
3. All reads execute at t_r (MVCC snapshot)
4. Transaction completes immediately (no commit phase)
```

**Timestamp selection**:
- **HLC mode**: `t_r = coordinator.hlc.now().physical`
- **TSO mode**: `t_r = tso.get_read_timestamp()`

**Guarantee**: Because of commit-wait on writes, any `t_r` picked after a write's acknowledgment will be `> t_w`. External consistency follows.

**No commit-wait on reads**: Read-only transactions don't write, so no commit-wait latency.

### Single-Range Write Transactions

**Characteristics**:
- All writes fall within a single Raft range (shard)
- Simpler than cross-shard (no 2PC)
- Still use MVCC intents for atomicity

**Protocol**:
```
1. Client starts transaction, sends writes to coordinator
2. Coordinator picks provisional timestamp t_p
3. Write intents at t_p (not committed values yet)
4. Replicate intents via Raft to quorum
5. Convert intents to committed values at t_c = max(t_p, participants)
6. Commit-wait until now() > t_c + ε (HLC mode only)
7. Acknowledge to client
```

**Intent structure**:
```rust
struct Intent {
    key: Key,
    value: Value,
    txn_id: TxnId,
    timestamp: Timestamp,  // Provisional, may change at commit
}
```

**Why intents**: During steps 3-5, other transactions might read this key. Intent signals "write in progress, not yet committed."

### Cross-Shard Write Transactions (2PC)

**Characteristics**:
- Writes span multiple Raft ranges
- Requires two-phase commit for atomicity
- Coordinator drives the protocol

**Roles**:
- **Coordinator**: Picks commit timestamp, drives 2PC phases
- **Participants**: Raft ranges that hold written keys

**Protocol** (detailed in next section).

## Two-Phase Commit Protocol

### Phase 0: Intent Writing

```
For each participant range:
1. Coordinator sends WriteIntent RPC with:
   - txn_id: unique transaction identifier
   - writes: [(key, value), ...]
   - provisional_timestamp: t_p
2. Participant writes intents (not committed values)
3. Participant replicates via Raft to quorum
4. Participant responds: (status, read_timestamp)
   - status: OK | ABORT (conflict detected)
   - read_timestamp: max timestamp read during execution
```

**Intent format on disk**:
```
key -> Intent {
    txn_id: UUID,
    value: bytes,
    provisional_ts: Timestamp,
}
```

**Conflict detection**: If writing intent encounters existing intent or committed value with `t > provisional_ts`, abort immediately (write-write conflict).

### Phase 1: Prepare

```
For each participant:
1. Coordinator sends Prepare RPC with:
   - txn_id
   - commit_timestamp: t_c = max(participants.read_ts, coordinator.now())
2. Participant verifies:
   - All intents are still present (not rolled back)
   - No conflicts at t_c (no committed writes with t ∈ (provisional_ts, t_c])
   - Raft range is still leader
3. Participant writes PreparedRecord to Raft log
4. Participant responds: PREPARED | ABORT
```

**PreparedRecord**:
```rust
struct PreparedRecord {
    txn_id: TxnId,
    commit_timestamp: Timestamp,
    intent_keys: Vec<Key>,
}
```

**Why write PreparedRecord**: If coordinator crashes between prepare and commit, recovery process needs to know this range voted "yes" and must complete the commit.

**Abort conditions**:
- Intent missing (already rolled back by timeout)
- Write-write conflict detected at t_c
- Raft leadership lost (can't guarantee replication)

### Phase 2: Commit

```
If all participants vote PREPARED:
1. Coordinator writes CommitRecord to its own Raft log with:
   - txn_id
   - commit_timestamp: t_c
   - participants: [range_ids]
   - status: COMMITTED
2. Coordinator sends Commit RPC to all participants
3. Each participant:
   - Converts intents to committed values at t_c
   - Removes txn_id metadata
   - Writes CommitRecord to Raft log
   - Responds: COMMITTED
4. Coordinator commit-waits until now() > t_c + ε (HLC mode)
5. Coordinator acknowledges to client
```

**Committed value format**:
```
key@t_c -> Value {
    data: bytes,
    // No txn_id, this is a committed MVCC version
}
```

**If any participant votes ABORT**:
```
1. Coordinator writes CommitRecord with status: ABORTED
2. Coordinator sends Abort RPC to all participants
3. Each participant:
   - Removes intents for txn_id
   - Writes AbortRecord to Raft log
4. Coordinator returns error to client (no commit-wait)
```

### Commit Timestamp Selection

**Formula**:
```rust
fn select_commit_timestamp(
    coordinator: &Node,
    participants: &[ParticipantResponse],
) -> Timestamp {
    let max_participant_ts = participants
        .iter()
        .map(|p| p.read_timestamp)
        .max()
        .unwrap_or(0);

    let coordinator_now = coordinator.hlc.now();

    Timestamp::max(max_participant_ts, coordinator_now)
}
```

**Why max(participants, coordinator)**:
- **Participants' read_timestamp**: Highest timestamp read during intent phase. Must assign `t_c ≥` this to avoid read-after-write violations.
- **Coordinator's now()**: Ensures `t_c` respects real-time order at coordinator.

**Example**:
```
1. Participant A reads key@100 during intent phase → read_ts = 100
2. Participant B reads nothing → read_ts = 0
3. Coordinator clock = 95 (slightly behind due to skew)
4. Commit timestamp = max(100, 0, 95) = 100
```

Must use 100, not 95, because transaction observed data at t=100.

### Commit-Wait Protocol

**HLC mode** (commit-wait required):
```rust
fn commit_wait(commit_timestamp: Timestamp, clock: &HLC, epsilon: Duration) {
    let target = commit_timestamp + epsilon;
    while clock.now() < target {
        sleep(1ms);
    }
}
```

**Purpose**: Ensure all replicas' clocks advance past `t_c` before acknowledging. Any future operation gets timestamp `> t_c`, guaranteeing external consistency.

**Typical duration**: ~10-50ms (PTP/PHC on AWS), ~50-100ms (NTP), ~1-10ms (GPS/atomic).

**TSO mode** (no clock-based commit-wait):
```rust
fn safe_timestamp_fence(commit_timestamp: Timestamp, tso: &TSO) {
    // Ensure TSO won't hand out timestamps ≤ commit_timestamp
    tso.advance_minimum(commit_timestamp + 1);
}
```

**Purpose**: Guarantee future timestamps from TSO are `> t_c`. No time-based waiting, but still coordination overhead.

## MVCC Intent Handling

### Write Path: Creating Intents

```
Storage layout during transaction:
key -> Intent {
    txn_id: UUID,
    value: bytes,
    provisional_ts: 100,
}

key@50 -> CommittedValue { data: "old" }
```

**Intent semantics**: "Transaction `txn_id` intends to write this value at ~t=100, but not yet committed."

### Read Path: Encountering Intents

When a read at timestamp `t_r` encounters an intent:

**Case 1: Intent belongs to active transaction with `t_intent ≤ t_r`**
```
1. Check intent status (query coordinator or transaction record)
2. If COMMITTED: read the intent's value (it's now committed at t_c ≤ t_r)
3. If ABORTED: ignore intent, read older version
4. If ACTIVE: wait or push (see below)
```

**Case 2: Intent belongs to inactive/expired transaction**
```
1. If transaction record shows ABORTED: cleanup intent, read older version
2. If transaction expired (timeout): attempt to roll back intent
3. If transaction COMMITTED: resolve intent to committed value
```

**Case 3: Intent timestamp > `t_r`**
```
Ignore intent (it's in the reader's future), read older committed version.
```

### Intent Resolution: Wait vs Push

When reading encounters an active intent blocking the read:

**Wait strategy** (default):
```rust
fn read_with_wait(key: Key, read_ts: Timestamp) -> Result<Value> {
    loop {
        match storage.get(key, read_ts) {
            Ok(value) => return Ok(value),
            Err(IntentConflict { txn_id, intent_ts }) => {
                if intent_ts > read_ts {
                    // Intent is in our future, should not block us
                    return storage.get_older_version(key, read_ts);
                }

                // Wait for intent to resolve
                wait_for_transaction(txn_id, timeout)?;
            }
        }
    }
}
```

**Push strategy** (for high-priority transactions):
```rust
fn read_with_push(key: Key, read_ts: Timestamp, reader_priority: Priority) -> Result<Value> {
    match storage.get(key, read_ts) {
        Err(IntentConflict { txn_id, intent_ts, writer_priority }) => {
            if reader_priority > writer_priority {
                // Push writer's timestamp forward, forcing it to commit at higher ts
                coordinator.push_transaction(txn_id, read_ts + 1)?;
                // Re-read after push
                storage.get(key, read_ts)
            } else {
                wait_for_transaction(txn_id, timeout)
            }
        }
        Ok(value) => Ok(value),
    }
}
```

**Push semantics**: Force writer to commit at `t_c > read_ts`, making its writes invisible to this reader. Prevents deadlocks and priority inversion.

**When to push**:
- High-priority reader vs low-priority writer
- Read-only transaction blocked by long-running write
- Deadlock detection (cycle-breaking)

## Write-Write Conflict Detection

Two transactions writing the same key must serialize. Cloud9 uses **first-writer-wins** with intent-based detection.

### Conflict Scenarios

**Scenario 1: Intent-Intent conflict**
```
T1: write_intent(key, t=100) → OK
T2: write_intent(key, t=105) → encounters T1's intent → ABORT (T1 got there first)
```

**Scenario 2: Intent-Committed conflict**
```
key@90 = "old"
T1: write_intent(key, provisional_ts=100)
T2: commits key@110 = "newer" (different transaction)
T1: prepare at t_c=120 → detect conflict (committed value@110 > provisional@100) → ABORT
```

**Scenario 3: Committed-Intent conflict**
```
key@100 = "committed"
T1: write_intent(key, provisional_ts=95) → must check for committed values@(95, now()] → conflict → ABORT
```

### Detection Algorithm

```rust
fn write_intent(key: Key, txn_id: TxnId, provisional_ts: Timestamp) -> Result<()> {
    // Check for existing intent
    if let Some(existing_intent) = storage.get_intent(key) {
        if existing_intent.txn_id != txn_id {
            return Err(WriteConflict::Intent(existing_intent.txn_id));
        }
    }

    // Check for committed values after provisional_ts
    if let Some(newer_value) = storage.get_next_version(key, provisional_ts) {
        return Err(WriteConflict::Committed(newer_value.timestamp));
    }

    // Write intent
    storage.put_intent(key, Intent { txn_id, provisional_ts, ... });
    Ok(())
}

fn prepare_transaction(txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
    for key in transaction.intent_keys {
        // Re-check conflicts at commit_ts (may differ from provisional_ts)
        if let Some(newer_value) = storage.get_versions(key, commit_ts) {
            if newer_value.timestamp > transaction.provisional_ts {
                return Err(WriteConflict::Committed(newer_value.timestamp));
            }
        }
    }
    Ok(())
}
```

**Key insight**: Check conflicts twice:
1. At intent-write time (provisional timestamp)
2. At prepare time (final commit timestamp)

Between these two checks, another transaction might commit a conflicting write.

## Transaction Recovery

### Coordinator Failure

**Problem**: Coordinator crashes between prepare and commit. Participants are in prepared state, can't proceed without coordinator decision.

**Solution**: Transaction record recovery.

```rust
struct TransactionRecord {
    txn_id: TxnId,
    coordinator: NodeId,
    participants: Vec<RangeId>,
    commit_timestamp: Timestamp,
    status: TxnStatus,  // ACTIVE | PREPARED | COMMITTED | ABORTED
    heartbeat: Timestamp,
}

enum TxnStatus {
    Active,
    Prepared,
    Committed,
    Aborted,
}
```

**Recovery protocol**:
```
1. New coordinator detects TransactionRecord with status=PREPARED and expired heartbeat
2. Query all participants for their vote:
   - If any voted ABORT: abort transaction
   - If all voted PREPARED: commit transaction at recorded t_c
3. Complete phase 2 (send Commit/Abort to participants)
4. Update TransactionRecord to COMMITTED/ABORTED
```

**Timeout-based cleanup**:
```
If TransactionRecord heartbeat expires and status=ACTIVE:
1. Coordinator presumed dead
2. Abort transaction (haven't entered prepared state yet)
3. Send Abort to all participants with intents
4. Clean up intents
```

### Participant Failure

**Problem**: Participant crashes during transaction.

**Solution**: Raft replication handles participant failure.

```
1. Intents are replicated via Raft to quorum
2. PreparedRecord is replicated via Raft to quorum
3. If leader crashes, new leader takes over with same state
4. Transaction continues normally on new leader
```

**Key property**: Because intents/PreparedRecord are in Raft log, failover doesn't lose transaction state.

## Read-Only Transactions: Lock-Free Execution

### Snapshot Selection

```rust
fn start_read_only_transaction() -> ReadOnlyTxn {
    let snapshot_ts = coordinator.hlc.now();  // Or tso.get_read_timestamp()
    ReadOnlyTxn { snapshot_ts }
}
```

### Execution

```rust
fn read(txn: &ReadOnlyTxn, key: Key) -> Result<Value> {
    // Find most recent committed version ≤ snapshot_ts
    storage.get_version(key, txn.snapshot_ts)
}
```

**No locks taken**: MVCC allows reading old versions while writes proceed on newer versions.

**Intent handling**: If read encounters intent:
- **Intent timestamp > snapshot_ts**: Ignore intent (it's in the future), read older version
- **Intent timestamp ≤ snapshot_ts**: Intent must have committed by now (due to commit-wait), resolve to committed value

### Staleness Considerations

**Potential issue**: Replica might not have applied all commits ≤ `snapshot_ts` yet (replication lag).

**Solution**: Safe timestamp tracking (see Closed Timestamps section).

## Closed Timestamps: Follower Reads

### The Problem

```
1. Leader commits write at t_c = 100, performs commit-wait
2. Leader acknowledges to client
3. Client immediately sends read at t_r = 101 to follower
4. Follower hasn't applied commit@100 yet (replication lag)
5. Follower reads stale data
```

**Violation**: Write finished before read in real time, but read didn't see write.

### The Solution: Closed Timestamps

**Definition**: A **closed timestamp** `t_closed` is a timestamp below which the leader guarantees no new writes will be assigned.

**Leader protocol**:
```rust
impl RaftLeader {
    fn advance_closed_timestamp(&mut self) {
        // Pick safe timestamp: below current HLC, all in-flight txns above this
        let safe_ts = self.hlc.now() - self.max_clock_skew;

        // Ensure no active transactions have provisional_ts ≤ safe_ts
        let min_active_txn_ts = self.active_txns.iter()
            .map(|t| t.provisional_ts)
            .min()
            .unwrap_or(Timestamp::MAX);

        self.closed_timestamp = Timestamp::min(safe_ts, min_active_txn_ts - 1);

        // Replicate via Raft (piggybacked on heartbeats)
        self.broadcast_closed_timestamp(self.closed_timestamp);
    }
}
```

**Follower protocol**:
```rust
impl RaftFollower {
    fn can_serve_read(&self, read_ts: Timestamp) -> bool {
        // Serve read if:
        // 1. read_ts ≤ closed_timestamp (no future writes below read_ts)
        // 2. We've applied all Raft log entries up to closed_timestamp
        read_ts <= self.closed_timestamp && self.applied_index >= self.closed_index
    }

    fn read_at_closed_timestamp(&self, key: Key, read_ts: Timestamp) -> Result<Value> {
        if !self.can_serve_read(read_ts) {
            return Err(ReadError::NotSafe);  // Redirect to leader or wait
        }
        self.storage.get_version(key, read_ts)
    }
}
```

### Closed Timestamp Propagation

**Mechanism**: Piggyback closed timestamp on Raft heartbeats.

```rust
struct RaftHeartbeat {
    leader_id: NodeId,
    term: u64,
    commit_index: u64,
    closed_timestamp: Timestamp,  // <-- New field
}
```

**Frequency**: Every heartbeat interval (~100ms typical).

**Staleness bound**: Follower reads are bounded-stale by heartbeat interval.

```
If heartbeat every 100ms:
- Follower reads see data ≤ 100ms old
- Still lock-free, still no coordination
- Acceptable for most workloads
```

### Follower Read Guarantees

**What followers guarantee**:
- **Snapshot isolation**: Read sees consistent snapshot at `t_r`
- **Bounded staleness**: Read is at most `heartbeat_interval` old
- **Lock-free**: No blocking, no waiting

**What followers don't guarantee**:
- **External consistency without waiting**: If write commits at t=100 and follower hasn't received heartbeat yet, follower might serve read at t=99 (stale)

**Solution for external consistency**: Client reads from leader (or waits for follower to catch up).

**Use case**: Follower reads with bounded staleness are perfect for:
- Analytics queries (don't need latest data)
- Geographically distributed reads (low-latency local reads)
- Load balancing read traffic across replicas

## External Consistency: End-to-End

### Single-Shard Transaction

```
1. Client: Begin transaction
2. Coordinator: Pick t_p = hlc.now()
3. Coordinator: Write intents at t_p
4. Coordinator: Replicate via Raft
5. Coordinator: Commit at t_c = max(t_p, observed_reads)
6. Coordinator: Commit-wait until now() > t_c + ε
7. Coordinator: Return "success" to client
   [At this point, all nodes' clocks > t_c]
8. Client: Begin new read transaction (anywhere)
9. Any node: Pick t_r = hlc.now() > t_c (guaranteed by commit-wait)
10. Read sees write (t_r > t_c, so write is visible)
```

**Guarantee**: Write finished (step 7) before read started (step 8) in real time. External consistency satisfied.

### Cross-Shard Transaction

```
1. Client: Begin transaction
2. Coordinator: Write intents to participants A, B (provisional t_p)
3. Participants: Replicate intents via Raft
4. Coordinator: Prepare with t_c = max(A.read_ts, B.read_ts, coordinator.now())
5. Participants: Vote PREPARED
6. Coordinator: Commit transaction at t_c
7. Coordinator: Replicate CommitRecord via Raft
8. Coordinator: Send Commit to participants
9. Participants: Convert intents to committed values@t_c
10. Coordinator: Commit-wait until now() > t_c + ε
11. Coordinator: Return "success" to client
    [At this point, all nodes' clocks > t_c]
12. Client: Begin new read (anywhere, any shard)
13. Any node: Pick t_r = hlc.now() > t_c
14. Read sees all writes from committed transaction
```

**Guarantee**: Commit-wait at step 10 ensures any future operation (even immediately after) gets `t > t_c`. External consistency satisfied across shards.

### Why Commit-Wait Is Sufficient

**Without commit-wait**:
```
1. Leader commits at t_c = 100 (local clock)
2. Leader returns "success" immediately (no wait)
3. Client gets success at real time 100.001
4. Client sends read to different node at real time 100.001
5. Different node's clock = 99.995 (slight skew)
6. Read picks t_r = 99.995 < 100
7. Read doesn't see write (violation!)
```

**With commit-wait (ε = 10ms)**:
```
1. Leader commits at t_c = 100
2. Leader waits until now() > 110 (t_c + ε)
3. Leader returns "success" at real time 110
4. Client sends read at real time 110.001
5. Different node's clock ≥ 100 (guaranteed: now > 110 - ε = 100)
6. Read picks t_r ≥ 100
7. Read sees write (correct!)
```

**Key insight**: Commit-wait duration ε must cover:
- Maximum clock skew between nodes
- Replication propagation time
- Network jitter

PTP/chrony provides bounds on clock skew. Raft provides replication guarantees. ε encompasses both.

## Performance Optimizations

### Read Timestamp Caching

**Problem**: Every read queries HLC/TSO for timestamp, adds overhead.

**Solution**: Cache read timestamp for duration of transaction.

```rust
struct ReadOnlyTxn {
    snapshot_ts: Timestamp,
    max_staleness: Duration,
    started_at: Instant,
}

impl ReadOnlyTxn {
    fn is_valid(&self) -> bool {
        self.started_at.elapsed() < self.max_staleness
    }
}
```

**Benefit**: Single timestamp acquisition per transaction, not per read.

### Parallel Commit

**Problem**: Coordinator waits for all participants to ack commit before commit-wait.

**Optimization**: Start commit-wait immediately after deciding to commit, while sending commit messages in parallel.

```rust
async fn commit_transaction(txn: &Transaction) {
    // All prepared, decision is COMMIT
    let commit_ts = txn.commit_timestamp;

    // Start commit-wait timer immediately
    let commit_wait_future = async {
        commit_wait(commit_ts, &hlc, epsilon).await;
    };

    // Send commit to participants in parallel
    let commit_futures = txn.participants.iter().map(|p| {
        async { p.commit(txn.txn_id, commit_ts).await }
    });

    // Wait for both: commit-wait AND participant acks
    tokio::join!(commit_wait_future, futures::join_all(commit_futures));
}
```

**Benefit**: Commit-wait overlaps with network/replication latency, reducing total latency.

### Intent Cleanup: Async Resolution

**Problem**: Resolving intents to committed values synchronously delays transaction completion.

**Solution**: Return success to client after commit-wait, resolve intents asynchronously.

```rust
async fn commit_protocol(txn: &Transaction) {
    // Phase 2: Commit decision made
    write_commit_record(txn.txn_id, COMMITTED, txn.commit_ts).await;

    // Commit-wait
    commit_wait(txn.commit_ts).await;

    // Return success to client (intents still exist, but committed)
    client.send_success();

    // Async: resolve intents to committed values
    tokio::spawn(async move {
        for participant in txn.participants {
            participant.resolve_intents(txn.txn_id, txn.commit_ts).await;
        }
    });
}
```

**Benefit**: Client latency reduced. Intents marked committed (safe to read), full cleanup happens in background.

### TSO Timestamp Batching

**Problem**: TSO mode requires coordinator to contact TSO for every transaction (network hop).

**Solution**: Pre-fetch timestamp ranges.

```rust
struct TSOClient {
    current_batch: Range<Timestamp>,
    batch_size: u64,
}

impl TSOClient {
    async fn next_timestamp(&mut self) -> Timestamp {
        if self.current_batch.is_empty() {
            // Fetch new batch
            let start = tso.allocate_range(self.batch_size).await;
            self.current_batch = start..(start + self.batch_size);
        }
        self.current_batch.next().unwrap()
    }
}
```

**Benefit**: Amortize TSO network cost over multiple transactions.

## Comparison to Alternatives

### Percolator (Google Bigtable)

**Similarities**:
- MVCC with intents
- 2PC for cross-shard transactions
- Lock-free reads

**Differences**:
- **Percolator**: Primary lock optimization (one primary key per transaction)
- **Cloud9**: No primary lock (all participants symmetric)
- **Percolator**: Client-driven coordination (client acts as coordinator)
- **Cloud9**: Server-driven coordination (database picks coordinator)

**Why Cloud9's approach**:
- Server-driven coordination simplifies client libraries
- No risk of client failure leaving transaction state ambiguous
- Easier to implement recovery (coordinator is always a database node)

### Spanner

**Similarities**:
- MVCC with intents
- External consistency via commit-wait
- Read-only transactions at snapshot timestamp
- Closed timestamps for follower reads

**Differences**:
- **Spanner**: TrueTime (GPS + atomic clocks)
- **Cloud9**: HLC (PTP/NTP) or TSO
- **Spanner**: Paxos for replication
- **Cloud9**: Raft for replication

**Why Cloud9's approach**:
- TrueTime not available on public clouds (Cloud9 provides HLC alternative)
- Raft simpler to implement and reason about than Paxos
- Same correctness guarantees, slightly higher latency (acceptable trade-off)

### CockroachDB

**Similarities**:
- HLC + commit-wait for external consistency
- MVCC with intents
- Raft replication
- Transaction records for recovery

**Differences**:
- **CockroachDB**: Transaction record stored as regular key-value pair
- **Cloud9**: Transaction record in dedicated Raft log (TBD: final design choice)
- **CockroachDB**: Complex timestamp cache for push/priority
- **Cloud9**: Simpler wait-based approach initially (push as optimization)

**Cloud9's design learns from CockroachDB's production experience**: Battle-tested protocol, well-understood failure modes.

### Calvin (Deterministic Databases)

**Calvin offers strict serializability without clocks** by pre-ordering transactions through a sequencer and executing them deterministically. This is elegant in theory but imposes constraints that conflict with Cloud9's design goals.

#### How Calvin Works

1. Transactions declare read/write sets upfront (or use stored procedures)
2. Sequencer assigns global order
3. All replicas execute transactions in that order deterministically
4. No timestamp-based conflicts, no aborts from clock skew

#### Why Calvin Doesn't Fit Cloud9's Primary Use Case

**Problem 1: Requires Known-Upfront Transactions**

Calvin needs declared read/write sets or stored procedures. This is incompatible with Cloud9's interactive, agent-driven workloads:

```
AI Agent workflow (Cloud9 target):
1. GET agent:state:123
2. if state.mode == "search":
     GET vectors:query
     PUT agent:state:123
   else:
     GET sql:users WHERE condition
     PUT agent:cache:xyz
```

**With HLC+MVCC**: Works naturally. Agent reads, branches, writes.

**With Calvin**: Must either:
- Pre-declare all possible paths (impossible for dynamic logic)
- Split into multiple transactions (loses atomicity)
- Use "escape hatch" non-Calvin path (defeats the purpose)

**Problem 2: Batching Adds Latency**

Calvin optimizes throughput via batching. At Cloud9's target scale (hundreds to thousands RPS), batching adds pure latency:

- **Light load**: Wait for batch to fill before sequencing
- **Spiky load**: Queue behind batch boundary
- **Long transactions**: Head-of-line blocking (convoy effect)

**KV hot-path writes** need <5ms. Calvin's queueing conflicts with this.

**Problem 3: Deterministic Execution Constraints**

Calvin requires:
- No wall-clock reads in transactions
- No nondeterministic UDFs (no random(), no system calls)
- Fixed execution order (can't parallelize within transaction)

**Cloud9 promises**:
- WASM/Python UDFs (user-defined logic)
- Function shipping (arbitrary compute)
- Flexible execution

**These are incompatible.** Calvin's determinism would cripple extensibility.

**Problem 4: Read Path Limitations**

**MVCC+HLC provides**:
- Follower reads at closed-timestamp (no leader coordination)
- Bounded-staleness reads (explicit freshness/latency trade-off)
- Time-travel queries (`AS OF timestamp`)
- Lock-free read-only transactions

**Calvin's read path**:
- Reads wait for sequencer's stable prefix (coordination overhead)
- Or: Reintroduce snapshot machinery (now you have two systems)
- No natural "read at past timestamp" (conflicts with deterministic order)

**Cloud9's "lock-free RO transactions" feature requires MVCC**, not Calvin.

**Problem 5: Sequencer Dependency**

Calvin's sequencer is:
- The source of truth for ordering
- A hot, critical dependency
- Must be globally available (cross-region Paxos/Raft)

**Under partition**: Can't proceed without global order.

**Cloud9's goal**: "Works locally, syncs globally" (agent-friendly).

**Calvin prevents** partition-tolerant local progress.

#### When Calvin Does Win

**Calvin is optimal for**:
- **High write-write contention** (100+ txns/sec touching same keys)
- **Known procedures** (payment processing, inventory reservations)
- **Batch workloads** (analytics writes, bulk updates)
- **Predictable tail latency** (no abort storms)

**Example use case**:
```sql
-- Concert ticket reservation (high contention on same seats)
PROCEDURE reserve_ticket(user_id, seat_id):
  READ seats WHERE id = seat_id
  IF seat.available:
    UPDATE seats SET available = false WHERE id = seat_id
    INSERT reservations (user_id, seat_id)
```

**With standard 2PC**: 1000 concurrent attempts = massive abort storm.
**With Calvin**: Pre-ordered execution serializes cleanly, no aborts.

#### Cloud9's Strategy: Surgical Use

**Default architecture**: HLC + MVCC + Raft
- Interactive SQL queries
- KV hot-path operations (agent state)
- Low-contention workloads (different keys)
- Dynamic workflows (branching logic)
- Rich read modes (follower, time-travel, bounded-staleness)

**Optional Calvin lane** (future):
- Opt-in per table or procedure type
- Separate sequencer for high-contention procedures
- Used for 5-20% of workload (inventory, payments, rate limits)

**This gives you**:
- **Best of both**: Low-latency interactive (HLC) + high-contention optimization (Calvin)
- **No compromise**: Default path fits Cloud9's agent-driven goals
- **Future-proof**: Can add Calvin lane when proven necessary

#### Why Cloud9 Chooses HLC+MVCC as Baseline

**Cloud9's primary workloads**:
1. ✅ Concurrent AI agents (dynamic, interactive) → needs MVCC flexibility
2. ✅ SQL analytics (ad-hoc queries) → needs snapshot reads
3. ✅ KV hot-path (low-latency) → needs no batching overhead
4. ✅ Lock-free read-only transactions → requires MVCC
5. ✅ Local development → needs to work without sequencer

**Calvin violates**: 1, 3, 4, 5.

**Verdict**: HLC+MVCC+Raft is the correct foundation for Cloud9. Calvin remains an optional optimization for specific high-contention workloads, not the universal substrate.

## Summary

Cloud9 transaction protocol provides:

1. **External consistency**: Commit-wait ensures real-time order is respected
2. **Atomicity**: 2PC guarantees all-or-nothing across shards
3. **Lock-free reads**: MVCC allows readers to access old versions without blocking
4. **Follower reads**: Closed timestamps enable bounded-stale reads from any replica

**Key mechanisms**:
- **MVCC intents**: Provisional writes before commit
- **Coordinator-driven 2PC**: Reliable cross-shard atomicity
- **Commit timestamp selection**: `max(participants, coordinator)` ensures consistency
- **Commit-wait**: Time barrier for external consistency (HLC mode)
- **Closed timestamps**: Safe follower reads with bounded staleness

**Correctness foundation**: Built on MVCC (01-mvcc.md), timestamp strategies (02-timestamps.md), and external consistency guarantees (03-external-consistency.md).

**Deployment flexibility**: Supports HLC (AWS PTP/NTP) and TSO modes, with clock uncertainty bounds determined by infrastructure (05-aws-time-infrastructure.md).

**Production-tested protocol**: Draws from Spanner, CockroachDB, and Percolator—proven at scale.
