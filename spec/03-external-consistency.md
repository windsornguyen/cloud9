# External Consistency

## Formal Definition

A system provides **external consistency** (also called **strict serializability**) if:

1. Transactions execute in some serial order
2. This order respects real-time precedence: if T₁ finishes before T₂ starts, then T₁ appears before T₂ in the serial order

## What Users Experience

```
if write_ack_received_before(read_started):
    read_must_see_write()
```

**Concrete example**:
```
Time →

Client A: write(x=1) → ✓ success
Client A: tells Client B "I wrote x=1"
Client B: read(x) → expects x=1
```

If the write finished before the read started **in real time**, the read must see the write. This is the fundamental user expectation Cloud9 guarantees.

## Why It's Hard

The database doesn't know about the "Client A tells Client B" step. That communication happened outside the database (Slack, verbal, UI navigation, HTTP call between microservices, etc.).

Without special handling, this violation can occur:
```
1. Replica A commits write with timestamp t₁=100
2. Client A gets "success"
3. Client B immediately sends read to Replica B
4. Replica B's clock is at 99 (slightly behind due to skew)
5. Read gets timestamp t₂=99
6. Read doesn't see write (99 < 100)
```

## How Cloud9 Achieves External Consistency

### Write Path

1. Coordinator assigns commit timestamp `t_w` from HLC
2. Replicate via Raft to quorum
3. **Commit-wait**: wait until `now() > t_w + ε`
4. Acknowledge to client

**Guarantee**: By the time client gets "success," every replica's clock is past `t_w`. Any future operation (even immediately after) will get timestamp `> t_w`.

**Cost**: ~ε latency per write (where ε is clock uncertainty bound).

### Read Path

1. Pick snapshot timestamp `t_r` (usually `now()` from HLC)
2. Check: "has this replica applied all entries ≤ `t_r`?"
3. If yes: serve read
4. If no: wait until caught up (or redirect to leader)

**Guarantee**: Because of commit-wait, any `t_r` picked after a write's acknowledgment will be `> t_w`.

## The Commit-Wait Necessity

**Question**: Can we get external consistency without commit-wait?

**Answer**: No. Here's why:

### Why Synchronization Alone Isn't Enough

Even with perfectly synchronized clocks:

```
1. Replica A assigns t_w = 100 (from its clock)
2. Replica A commits, returns "success" immediately
3. Client gets success at real time 100.001
4. Client immediately sends read to Replica B at real time 100.001
5. Replica B's clock reads 100.0005 (slightly behind due to network/processing)
6. Read gets t_r = 100.0005
7. Replica B hasn't yet applied the write (replication lag)
8. Read misses the write
```

**Issue**: Even with synchronized clocks, there's a gap between:
- When leader commits locally, and
- When followers apply the commit

### What Commit-Wait Fixes

**Protocol**:
```
1. Replica A assigns t_w = 100
2. Replica A replicates to quorum
3. Replica A waits until its clock > 100 + ε
4. Now: all replicas' clocks are guaranteed > 100
5. Return "success" to client
```

**Guarantee**: Any future operation (anywhere in the cluster) gets timestamp > 100.

**The wait covers**: Clock uncertainty + replication propagation time.

### Can We Eliminate ε?

**No.** Here's why:

**If you use physical clocks**: Uncertainty is unavoidable due to:
- NTP sync error (milliseconds)
- Clock drift between syncs
- Network jitter
- Relativity (if we're being pedantic)

**If you use a TSO**: No ε for clock uncertainty, but:
- Still need to wait for "safe timestamp" propagation to followers
- Or accept that "read latest" might wait for TSO fence

**If you use deterministic ordering** (Calvin-style): No ε, but:
- Batching/sequencing latency instead
- Different programming model

### PACELC: The Inescapable Trade-Off

For external consistency with wall-clock-meaningful timestamps:
- Some barrier is unavoidable
- Either wait for time (commit-wait ~ε), OR
- Wait for order (sequencer/TSO propagation), OR
- Wait for batch (deterministic pre-ordering)

**PACELC theorem**: If Partition, choose Availability or Consistency; Else (no partition), choose Latency or Consistency.

For external consistency, even without network partitions, we must trade latency (commit-wait) for consistency (real-time order).

**Alternative**: Drop external consistency, use logical timestamps only (faster writes, but can violate user expectations).

**Cloud9's choice**: Pay the latency. External consistency is non-negotiable for "daily driver" database where users expect intuitive behavior.

### Optimizations

**Read-only transactions**: Don't pay commit-wait (no write to acknowledge).

**Bounded-staleness reads**: Explicitly tolerate staleness to avoid waiting.

**Follower reads at closed timestamp**: Serve from any replica at a safe, slightly-stale timestamp without coordination.

## Comparison to Weaker Models

### Snapshot Isolation (without external consistency)
- Can have write-skew anomalies
- Timestamps might not respect real-time order
- Cheaper (no commit-wait), but weaker guarantees

### Eventual Consistency
- No ordering guarantees
- Much cheaper, but unusable for Cloud9's goals

### Linearizability (single-object)
- Only for single-key operations
- Cloud9 provides this as a subset (single-key reads/writes are linearizable)

**External consistency = Strict Serializability**: Cloud9's target.

## Why Server-Side Timestamps

Cloud9 never allows clients to pass timestamps. All timestamp assignment and coordination happens server-to-server.

### Why Client Timestamps Don't Work

**Problem 1: Clients can't be trusted**
- Malicious client sends t = infinity → breaks future operations
- Buggy client sends stale timestamp → violates consistency
- Compromised client manipulates ordering

**Problem 2: Not all communication involves the client**
```
Client A → DB: write → t₁
Client A → tells human → human tells Client B (no t₁ passed)
Client B → DB: read → doesn't know about t₁
```

**Problem 3: API complexity**
- Every client library must track timestamps
- Developers must remember to propagate them
- Easy to get wrong, hard to debug

**Problem 4: Cross-database scenarios**
If DB1 and DB2 are independent systems, client forwarding t₁ from DB1 to DB2 is meaningless (different timestamp spaces).

### The Right Approach: Server-Side Timestamps

**Design**:
- Servers assign all timestamps (from HLC or TSO)
- Servers coordinate among themselves (commit-wait, gossip)
- Clients never see or send timestamps
- External consistency guaranteed by database internals

**Benefits**:
1. Security: clients can't manipulate time
2. Simplicity: client libraries are trivial
3. Correctness: database controls ordering
4. Works for all scenarios (even when clients never communicate)

**Cloud9**: Server-side only. Clients are dumb, database is smart.
