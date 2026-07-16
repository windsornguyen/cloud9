# Historical Cloud9 Design Notes

> This document preserves early design discussion. It is not normative and
> contains superseded decisions, including HLC-based timestamp guidance. Use
> [the current specifications](../spec/README.md) for implementation.

This document captures the theoretical foundations, architectural decisions, and market insights that define Cloud9.

## Vision

**Cloud9 is the distributed database that should have existed from the start.**

Spanner proved that external consistency is achievable with commit-wait and precise time. FoundationDB proved that SQL and KV can share one transactional core. Postgres proved that full ACID with referential integrity is what developers expect. CockroachDB proved that you can build this in the open.

**Nobody combined them all.**

Cloud9 is the synthesis: Spanner's correctness + Postgres's compatibility + FoundationDB's architecture + open-source transparency. No corporate compromises. No vendor lock-in. No "this feature costs extra." Just the theoretically optimal distributed database, available to everyone.

**If you finish a write and start a read, that read sees the write. Anywhere in the world. Always. Provably.**

That's the guarantee. The rest is engineering.

## Table of Contents

- [Why MVCC](#why-mvcc)
- [Why Not Lamport Clocks](#why-not-lamport-clocks)
- [Timestamp Strategies](#timestamp-strategies)
- [External Consistency Explained](#external-consistency-explained)
- [Client vs Server Timestamp Assignment](#client-vs-server-timestamp-assignment)
- [The Commit-Wait Necessity](#the-commit-wait-necessity)
- [Why Cloud9 Exists: Market Pain Points](#why-cloud9-exists-market-pain-points)

---

## Why MVCC

**Question**: How do we support lock-free read-only transactions and backups without blocking writes?

**Answer**: Multi-Version Concurrency Control (MVCC).

### The Core Idea

- Each write transaction T_w gets a commit timestamp t_w
- Values are versioned with their write timestamp (not overwritten)
- Each read-only transaction T_r picks a snapshot timestamp t_r
- T_r observes the most recent committed version with t_w ≤ t_r
- Writes with t_w > t_r are invisible to T_r

**Result**: Readers and writers operate on different versions. No locks, no blocking.

### Why This Is Natural

MVCC models how time actually works: the past is immutable, observers can choose which moment to examine. A backup reading at timestamp t_r sees a consistent point-in-time snapshot while new writes (t_w > t_r) continue.

### Alternatives Considered

**Two-Phase Locking (2PL)**:
- Readers take shared locks, writers take exclusive locks
- Backup would lock the entire database for reads OR block all writes
- No temporal queries ("read as of 5 minutes ago")
- Rejected: contradicts "lock-free read-only transactions" goal

**Optimistic Concurrency Control (OCC)**:
- Read without locks, validate at commit
- High abort rate under contention
- Backup could abort if overlapping writes occur
- Rejected: poor fit for long-running analytical queries

**Timestamp Ordering (TO)**:
- Single version per key, enforce timestamp order
- More aborts, no historical reads
- Rejected: need multi-version for temporal queries

**Verdict**: MVCC is the only scheme that satisfies Cloud9's requirements (lock-free reads, temporal queries, write concurrency). Every modern OLTP database (Postgres, Spanner, CockroachDB, TiDB) uses MVCC for this reason.

---

## Why Not Lamport Clocks

**Question**: Why can't we use Lamport clocks for timestamp assignment?

**Initial thought**: "All replicas are in the Cloud9 cluster, so Lamport clocks should work fine."

**Reality**: Lamport clocks can't guarantee external consistency, even within one cluster.

### The Problem

**Scenario**:
```
1. Client writes to Replica A → Lamport clock assigns L=50
2. Write commits, client gets "success"
3. Client immediately sends read to Replica B (in real time, right after)
4. Replica B's Lamport clock is at L=49 (hasn't heard from A yet)
5. Read gets timestamp L=49, doesn't see the write (L=49 < L=50)
```

**Violation**: Write finished before read started in real time, but read didn't see write.

### Why This Happens

Lamport clocks only advance when:
- A local event happens, OR
- A message **from another node** arrives

If Replica B hasn't received any messages from Replica A (or other nodes that know about the write) before the client's read arrives, B's clock can be arbitrarily behind—even though the write finished in real time.

### What Lamport Clocks Actually Solve

**Designed for**: Ordering events when all communication goes through the system.

**Perfect use cases**:
- Distributed tracing (causality in logs)
- CRDTs (eventual consistency with causal order)
- Event sourcing (ordering events in a distributed log)
- Deadlock detection (wait-for graph ordering)

**Key property**: If A → B via system messages, then L(A) < L(B).

**What they DON'T capture**: If A finishes before B starts in real time, but no message connects them, Lamport clocks don't guarantee L(A) < L(B).

### The Database-Specific Issue

In databases, external communication is constant:
- User sees write succeed in UI, refreshes page (new request to different server)
- Microservice A writes, calls microservice B via HTTP, B reads
- Client writes, tells colleague verbally, colleague reads

**None of these involve database messages**, so Lamport clocks can't track them.

### What If We Used a Centralized Oracle for Lamport?

**Approach**: All replicas get Lamport timestamps from a central service.

```
Replica A → Oracle: "assign timestamp" → L=50
Replica B → Oracle: "assign timestamp" → L=51
```

**This works, but**: You're no longer using Lamport clocks—you're using a **Timestamp Oracle (TSO)**. The oracle hands out strictly increasing values based on request order, which is fundamentally different from Lamport's "local counter + max(received)".

**Verdict**: If you have a TSO, use it directly. Don't call it "Lamport clocks."

---

## Timestamp Strategies

Cloud9 needs timestamps that respect **real-time order**. Three viable approaches:

### 1. Hybrid Logical Clocks (HLC) — Recommended

**Design**:
```rust
struct HybridTime {
    physical: u64,  // Wall-clock microseconds
    logical: u32,   // Tie-breaker for same physical time
}

fn next_timestamp(&mut self) -> HybridTime {
    let now = wall_clock_micros();
    if now > self.last_physical {
        HybridTime { physical: now, logical: 0 }
    } else {
        HybridTime { physical: self.last_physical, logical: self.last_logical + 1 }
    }
}
```

**How it works**:
- Physical component tracks wall-clock time
- Logical component breaks ties when physical time doesn't advance
- Even without messages, time moves forward (physical clock)

**External consistency**:
- Commit-wait: After assigning t_w, wait ~ε (clock uncertainty bound)
- Guarantees that any operation starting after commit has timestamp > t_w
- ε determined by clock synchronization (NTP/PTP)

**Pros**:
- Decentralized (each node has its own HLC)
- Scales well (no single point of bottleneck)
- Captures real-time order via physical component

**Cons**:
- Requires clock synchronization (NTP/PTP/chrony)
- Must measure and bound clock uncertainty ε
- Commit-wait adds latency (~ε per write)

**Used by**: CockroachDB, YugabyteDB

### 2. TrueTime (Spanner's Approach)

**Design**: Clock API that returns uncertainty interval [earliest, latest].

```rust
struct TrueTime {
    earliest: u64,
    latest: u64,
}

fn now() -> TrueTime {
    // GPS + atomic clocks give tight bounds
    TrueTime { earliest: ..., latest: ... }
}
```

**How it works**:
- Google uses GPS receivers + atomic clocks
- Publishes bounded uncertainty (typically ~1-7ms)
- Commit-wait until TT.after(commit_ts) is true

**External consistency**: Same as HLC but with tighter ε (hardware advantage).

**Pros**:
- Very tight uncertainty bounds (single-digit milliseconds)
- Proven at Google scale

**Cons**:
- Requires specialized hardware (GPS/atomic clocks)
- Not available on public clouds without custom setup
- Still pays commit-wait latency

**Availability**: Not available on AWS/GCP for customer deployments.

### 3. Timestamp Oracle (TSO) — Centralized

**Design**: Single service hands out strictly increasing timestamps.

```rust
struct TSO {
    counter: AtomicU64,
}

impl TSO {
    fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}
```

**How it works**:
- All nodes get timestamps from central oracle
- Oracle ensures strict monotonicity
- No commit-wait needed (ordering is explicit)

**External consistency**: Guaranteed by request serialization through oracle.

**Pros**:
- Simple reasoning (total order at oracle)
- No clock synchronization needed
- No commit-wait on writes

**Cons**:
- Oracle is single point of bottleneck
- Oracle must be highly available (itself replicated)
- Adds network hop to oracle for every transaction

**Used by**: FoundationDB (Sequencer), TiDB (PD's TSO)

### Cloud9's Strategy

**Primary mode**: HLC + commit-wait
- Decentralized, scales well
- Works on commodity cloud hardware (AWS Time Sync Service)
- Matches CockroachDB's approach

**Alternative mode** (configurable): TSO
- For deployments where clock synchronization is difficult
- Or when ε is too large (poor NTP sync)
- Trades write latency for simpler time discipline

**Future enhancement**: TrueTime-like with GPS/atomic clocks for Dedalus Cloud premium tier (colocated deployments).

---

## External Consistency Explained

**Question**: What is external consistency, and why do we need it?

### The User Expectation

```
Time →

Client A: write(x=1) → ✓ success
Client A: tells Client B "I wrote x=1"
Client B: read(x) → expects x=1
```

If the write finished before the read started **in real time**, the read must see the write. This is called **external consistency** or **strict serializability**.

### Why It's Hard

The database doesn't know about the "Client A tells Client B" step. That communication happened outside the database (Slack, verbal, UI navigation, etc.).

Without special handling, this can happen:
```
1. Replica A commits write with timestamp t₁=100
2. Client A gets "success"
3. Client B immediately sends read to Replica B
4. Replica B's clock is at 99 (slightly behind due to skew)
5. Read gets timestamp t₂=99
6. Read doesn't see write (99 < 100)
```

### The Solution: Commit-Wait

**Commit-wait protocol**:
```
1. Replica A assigns commit timestamp t_w
2. Replica A replicates write to quorum
3. Replica A WAITS until its clock (and all replicas' clocks) are > t_w
4. Only then: return "success" to client
```

**Guarantee**: By the time client gets "success," every replica's clock is past t_w. Any future operation (even immediately after) will get timestamp > t_w.

**Cost**: ~ε latency per write (where ε is clock uncertainty bound).

### PACELC: Why We Pay Latency

**PACELC theorem**: If Partition, choose Availability or Consistency; Else (no partition), choose Latency or Consistency.

For external consistency, even without network partitions, we must trade latency (commit-wait) for consistency (real-time order).

**Alternative**: Drop external consistency, use logical timestamps only (faster writes, but can violate user expectations).

**Cloud9's choice**: Pay the latency. External consistency is non-negotiable for "daily driver" database where users expect intuitive behavior.

---

## Client vs Server Timestamp Assignment

**Question**: Why not let clients pass timestamps between operations?

**Initial thought**: "Client gets t₁ from DB write, passes it to next DB read as min_timestamp."

### Why This Doesn't Work

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

### When Client Timestamps ARE Used

Some systems (Cassandra, Cosmos DB) let clients provide timestamps for **last-write-wins** in eventual consistency scenarios. This is different:
- Used for conflict resolution, not ordering guarantees
- No external consistency promise
- Application-level concern, not database correctness

**Cloud9**: Server-side only. Clients are dumb, database is smart.

---

## The Commit-Wait Necessity

**Question**: Can we get external consistency without commit-wait?

**Exploration**: "What if we just synchronize clocks really well?"

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

**Cloud9's choice**: Wait for time (HLC + commit-wait). The latency is proportional to clock quality (tight with PTP, larger with NTP).

### Optimizations

**Read-only transactions**: Don't pay commit-wait (no write to acknowledge).

**Bounded-staleness reads**: Explicitly tolerate staleness to avoid waiting.

**Follower reads at closed timestamp**: Serve from any replica at a safe, slightly-stale timestamp without coordination.

---

## Timestamp Strategies

Cloud9 supports two modes for timestamp assignment.

### Mode 1: HLC (Decentralized)

**Architecture**:
- Each node maintains a Hybrid Logical Clock
- Clocks synchronized via NTP/PTP (bounded skew)
- Nodes gossip closed timestamps to enable follower reads

**Timestamp assignment**:
```
HLC = (physical_micros, logical_counter)

On event:
  now = wall_clock()
  if now > hlc.physical:
    hlc = (now, 0)
  else:
    hlc = (hlc.physical, hlc.logical + 1)
```

**External consistency**:
- Measure clock uncertainty ε (from chrony/NTP stats)
- After assigning t_w, wait ~ε before ack
- Fail-stop if observed skew > max_offset

**When to use**: Default for Cloud9. Works on any cloud provider with NTP/PTP.

### Mode 2: TSO (Centralized)

**Architecture**:
- Dedicated timestamp oracle service (replicated for HA)
- All transaction coordinators request timestamps from TSO
- TSO returns strictly increasing u64 values

**Timestamp assignment**:
```
TSO maintains: atomic counter
On request: return fetch_add(1)
```

**External consistency**:
- Guaranteed by serialization through TSO
- No commit-wait needed for time uncertainty
- May still wait for "safe timestamp" propagation to followers

**When to use**:
- Clock synchronization is unreliable
- ε would be too large (>100ms)
- Willing to accept TSO as dependency

### Trade-Offs

| Aspect | HLC | TSO |
|--------|-----|-----|
| Scalability | High (decentralized) | Medium (oracle bottleneck) |
| Write latency | ~ε commit-wait | No commit-wait, but oracle hop |
| Dependency | Clock sync (NTP/PTP) | TSO service (must be HA) |
| Operational complexity | Clock monitoring | TSO operations |
| Failure mode | Fail-stop on skew > max | Block if TSO unreachable |

**Cloud9 default**: HLC (matches CockroachDB). TSO mode available for challenging clock environments.

---

## External Consistency Explained

### Formal Definition

A system provides **external consistency** (also called **strict serializability**) if:

1. Transactions execute in some serial order
2. This order respects real-time precedence: if T₁ finishes before T₂ starts, then T₁ appears before T₂ in the serial order

### In Practice

**What users experience**:
```
if write_ack_received_before(read_started):
    read_must_see_write()
```

**What the database must ensure**:
- Commit timestamps reflect real-time order
- Reads observe all commits with timestamps ≤ read timestamp
- No write can "appear in the future" from a reader's perspective

### How Cloud9 Achieves It

**Write path**:
1. Coordinator assigns t_w from HLC
2. Replicate via Raft to quorum
3. Commit-wait until now() > t_w + ε
4. Acknowledge to client

**Read path**:
1. Pick snapshot timestamp t_r (usually now() from HLC)
2. Check: "has this replica applied all entries ≤ t_r?"
3. If yes: serve read
4. If no: wait until caught up (or redirect to leader)

**Guarantee**: Because of commit-wait, any t_r picked after a write's acknowledgment will be > t_w.

### Comparison to Weaker Models

**Snapshot Isolation** (without external consistency):
- Can have write-skew anomalies
- Timestamps might not respect real-time order
- Cheaper (no commit-wait), but weaker guarantees

**Eventual Consistency**:
- No ordering guarantees
- Much cheaper, but unusable for Cloud9's goals

**Linearizability** (single-object):
- Only for single-key operations
- Cloud9 provides this as a subset (single-key reads/writes are linearizable)

**External consistency = Strict Serializability**: Cloud9's target.

---

## Server-to-Server Coordination

**Question**: How do replicas coordinate timestamps?

**Answer**: Depends on mode.

### HLC Mode: Gossip + Commit-Wait

**Mechanisms**:

1. **Raft replication**: Entries carry their commit timestamp
2. **Commit-wait**: Leader waits before ack
3. **Closed timestamp gossip**: Leaders advertise "no future writes ≤ t_c"
4. **Applied index tracking**: Each replica knows what it has applied

**Follower read protocol**:
```
1. Follower receives read at timestamp t_r
2. Check: applied_index includes all entries with commit_ts ≤ t_r?
3. If yes: serve read locally
4. If no: wait or redirect to leader
```

**Why this works**: Commit-wait ensures that by the time a client can issue a read, all replicas are past the commit timestamp.

### TSO Mode: Centralized Sequencing

**Mechanisms**:

1. **Coordinator → TSO**: Request timestamp for transaction
2. **TSO**: Return strictly increasing value
3. **Coordinator**: Use that timestamp for commit
4. **Followers**: Track "safe timestamp" (max applied from TSO)

**Read protocol**:
```
1. Read at timestamp t_r
2. Check: safe_timestamp ≥ t_r?
3. If yes: serve read
4. If no: wait for safe_timestamp to advance
```

**Why this works**: TSO serialization ensures global order; safe timestamp propagation ensures replicas are caught up.

### No Client Involvement

In both modes, **all coordination is server-to-server**:
- Raft messages carry timestamps
- Gossip messages advertise closed/safe timestamps
- Followers track applied state

**Clients**: Send operations, get success/failure. Never see timestamps (unless explicitly requesting "read as of T").

---

## Key Insights

### 1. MVCC Is the Right Primitive

For lock-free reads, temporal queries, and concurrent writes, MVCC is the only practical choice. All modern OLTP databases use it.

### 2. Lamport Clocks Are Insufficient

Pure Lamport clocks can't capture real-time happens-before relationships that occur outside the database. Need physical time component (HLC) or centralized sequencing (TSO).

### 3. Commit-Wait Is Unavoidable

For external consistency with physical timestamps, some barrier is required. Either:
- Wait for time (HLC/TrueTime commit-wait)
- Wait for order (TSO safe timestamp propagation)
- Wait for batch (deterministic sequencing)

Choose your poison; physics doesn't give you external consistency for free.

### 4. Server-Side Timestamps Are Non-Negotiable

Clients can't be trusted with timestamp assignment. All coordination must be server-to-server. Clients are dumb; database is smart.

### 5. The Design Is Validated

Independent analysis from multiple sources converged on the same architecture:
- **MVCC** for multi-version storage (enables lock-free reads)
- **HLC** for timestamps (captures real-time order without TrueTime hardware)
- **Raft** for replication (proven, battle-tested consensus)
- **Commit-wait** for external consistency (guarantees real-time ordering)
- **Rust** for implementation (memory safety + C-class performance)
- **Open-core** for business model (trust through transparency)

**What makes Cloud9 unique isn't the individual pieces—it's the synthesis**:

Every distributed database uses some of these components. None combine all of them with:
- SQL and KV unified under one transaction model
- True Postgres compatibility (foreign keys, triggers, constraints)
- Local-to-global deployment with the same binary
- MIT license with no vendor lock-in

**This isn't novel research—it's what distributed databases should have been from the start.** Spanner proved the foundation (external consistency via commit-wait). FoundationDB proved the layering (SQL+KV over one transactional core). Postgres proved the interface (wire compatibility, full ACID).

Cloud9 is the **disciplined execution** of combining these proven principles into a coherent whole, without the compromises forced by corporate constraints:
- Spanner compromised: SQL-only, no foreign keys, proprietary, cloud-only
- CockroachDB compromised: SQL-only, then went proprietary (BSL)
- YugabyteDB compromised: SQL and KV exist but aren't unified
- DynamoDB compromised: KV-only, eventual consistency, no transactions

**Cloud9 makes no compromises.** External consistency + SQL + KV + open source + local-to-global. The theoretically optimal design, executed without corporate baggage.

---

## TrueTime: Heuristic or Provably Correct?

**Question**: TrueTime syncs with GPS/atomic clocks every 30 seconds. Isn't this just a heuristic? Can we prove it maintains correctness?

**Answer**: It's **provably correct**, not heuristic.

### The 30-Second Sync Explained

The sync interval is **conservative by design**. Here's the math:

**Uncertainty formula**:
```
ε(t) = sync_error + drift_rate × time_since_sync
```

**Example calculation**:
- `sync_error` = 1 μs (GPS accuracy)
- `drift_rate` = 200 ppm (200 microseconds per second, typical quartz oscillator)
- `time_since_sync` = 30 seconds

```
ε = 1 μs + (200 μs/s × 30s) = 6001 μs ≈ 6ms
```

**The bound is mathematical**, not empirical guesswork.

### Why It's Provably Correct

**TrueTime invariant**: `TT.now()` returns `[earliest, latest]` where:
```
earliest ≤ absolute_true_time ≤ latest  (always)
```

**How it's maintained**:
1. At sync time t₀: measure offset from GPS/atomic clock → `sync_error`
2. Between syncs: bound grows linearly with known `drift_rate`
3. TrueTime daemon continuously computes: `ε(t) = sync_error + drift_rate × (t - t₀)`
4. Returns interval: `[now - ε, now + ε]`

**Formal proof**: If two TrueTime intervals don't overlap (`l₁ < e₂`), then the events happened in that order in absolute time.

**Spanner's external consistency** follows from this property + commit-wait protocol.

### What If Clocks Misbehave?

**Drift exceeds specification**:
- Next sync detects large offset
- ε grows beyond acceptable threshold
- System can refuse writes (fail-safe) or alert operators

**GPS outage**:
- Atomic clocks continue providing stable reference
- ε stays small for hours (atomic clock stability)
- Fallback: increase ε bound, continue with higher latency

**Both GPS and atomic fail**:
- ε grows unbounded
- System must stop writes or increase commit-wait proportionally
- Spanner paper: "conservatively refuse transactions" in this scenario

### Why 30 Seconds Specifically?

**Trade-offs**:
- **Shorter interval** (e.g., 1 second): Lower ε, higher sync overhead
- **Longer interval** (e.g., 5 minutes): Lower overhead, larger ε

**Google chose 30s** because:
1. With 200 ppm drift, 30s → ~6ms uncertainty (acceptable)
2. GPS/atomic clocks are stable enough to trust over 30s
3. Sync overhead is negligible (one request per 30s)
4. Safety margin: can tolerate missed sync without ε explosion

**It's not arbitrary**—it's an **engineered choice** based on hardware characteristics.

### The Difference from Heuristics

**Heuristic** would be: "We think clocks are usually within 10ms, so let's use that."

**TrueTime** is: "We measure sync error, we know drift rate from hardware specs, we compute ε = f(sync_error, drift_rate, time), and we prove that [now - ε, now + ε] contains true time."

**The correctness is proven**, assuming:
1. Sync error measurement is accurate (GPS is)
2. Drift rate is bounded (quartz spec sheets provide this)
3. No Byzantine faults (time masters don't lie maliciously)

All three are reasonable assumptions with continuous monitoring.

### For Cloud9

**We must do the same rigorous approach**:

```rust
struct TimeSource {
    last_sync: Instant,
    sync_error: Duration,
    drift_rate_ppm: f64,
}

impl TimeSource {
    fn uncertainty(&self) -> Duration {
        let elapsed = self.last_sync.elapsed();
        let drift = Duration::from_micros(
            (elapsed.as_micros() as f64 * self.drift_rate_ppm / 1_000_000.0) as u64
        );
        self.sync_error + drift
    }

    fn now_interval(&self) -> (Timestamp, Timestamp) {
        let now = Timestamp::now();
        let ε = self.uncertainty();
        (now - ε, now + ε)
    }
}
```

**Continuous monitoring**:
- Track actual vs expected sync offsets
- Alert if drift_rate exceeds spec
- Fail-stop if ε > max_offset

**Not heuristic—measured, bounded, proven.**

---

## Open Questions

### 1. Clock Uncertainty Bounds on AWS

**Question**: How tight can we make ε on AWS infrastructure?

**AWS Time Infrastructure Options**:

#### Option 1: Amazon Time Sync Service (NTP)
- Available on all EC2 instances
- Default NTP endpoint: `169.254.169.123`
- Leap-smear enabled (smooth out leap seconds)
- **Expected ε**: 50-100ms (typical NTP on cloud)

**Pros**: Zero setup, works everywhere
**Cons**: Too large for competitive write latency

#### Option 2: Amazon Time Sync Service (PTP with PHC)
- Available on Nitro-based instances (most modern EC2)
- Uses Precision Time Protocol with hardware clock (PHC)
- Access via Linux PTP APIs (`/dev/ptp0`)
- **Expected ε**: 10-50ms (measured in-region)

**Pros**:
- Much tighter than NTP
- Hardware timestamping support
- Still fully managed by AWS

**Cons**:
- Nitro instances only
- Requires PTP client configuration (chrony/ptp4l)
- Cross-region sync still limited by network

**Setup**:
```bash
# Use chrony with PHC
server 169.254.169.123 iburst minpoll 4 maxpoll 4
refclock PHC /dev/ptp0 poll 3 dpoll -2 offset 0
```

#### Option 3: AWS Outposts with Custom PTP
- Outposts racks in your datacenter/colo
- Bring your own GPS-disciplined PTP grandmaster
- Direct L2 PTP messaging (no VPC overhead)
- **Expected ε**: 1-10ms (GPS + hardware timestamping)

**Pros**:
- Near-TrueTime performance
- Full control over time source
- Can use GPS + atomic clocks

**Cons**:
- Requires Outposts (expensive)
- Operational complexity (manage PTP infrastructure)
- Hybrid cloud/on-prem model

#### Option 4: Colocated Deployments with GPS/Atomic
- Rent colo space (Equinix, etc.)
- Install GPS receivers + Rubidium/Cesium atomic clocks
- Run Cloud9 nodes with direct PTP feed
- **Expected ε**: <1ms (matches Spanner)

**Pros**:
- Best possible ε without TrueTime
- Full control over time infrastructure
- Can achieve single-digit millisecond uncertainty

**Cons**:
- Not "cloud-native"
- Hardware cost (GPS antennas, atomic clocks ~$5-10k each)
- Operational burden (roof rights for GPS, climate control for atomics)
- Doesn't work for pure AWS deployments

**Research needed**:
- Measure Amazon Time Sync Service with PTP/PHC across AZs
- Test clock stability across regions
- Benchmark Outposts vs native EC2
- Evaluate Open Compute Time Appliance for colo deployments

**Cloud9 deployment tiers**:
- **Standard** (NTP): ε ≈ 50ms, works everywhere, slow writes
- **Performance** (PTP/PHC): ε ≈ 10-20ms, Nitro instances, competitive
- **Premium** (Outposts/Colo): ε ≈ 1-5ms, custom hardware, Spanner-class

**Target**: Ship with PTP/PHC support on Nitro as default; offer Outposts/colo as premium tier for Dedalus Cloud.

### 2. TSO Scalability

**Question**: Can we shard the TSO or must it be a single global sequencer?

**Approaches**:
- Per-range TSO (CockroachDB HLC is effectively this)
- Batched timestamp allocation (request 1000 timestamps at once)
- Hybrid: HLC for local, TSO for cross-region

**Target**: Support 1M+ timestamp requests/sec if TSO mode is primary.

### 3. Garbage Collection Strategy

**Question**: How aggressively can we GC old versions without breaking running transactions?

**Constraints**:
- Must track oldest active read timestamp cluster-wide
- Long-running backups/analytics can prevent GC
- Need per-range GC policies

**Target**: GC old versions within minutes of becoming unreachable, not hours.

### 4. Lock-Free Read-Only Transaction Semantics

**Question**: Do read-only transactions pick timestamp at start or lazily per operation?

**Trade-offs**:
- Start timestamp: consistent snapshot across entire transaction
- Lazy timestamp: each read sees "slightly newer" data, less consistent

**Cloud9 choice**: Start timestamp (matches Spanner). Lazy mode can be opt-in for specific use cases.

---

## Why Cloud9 Exists: Market Pain Points

Based on extensive user feedback from production deployments of Spanner, DynamoDB, and competing systems, several consistent themes emerge that Cloud9 is designed to address.

### Spanner Pain Points

#### 1. Cost and Pricing Model
**The Problem**:
- Minimum $65/month (often $1000+/month for production)
- No on-demand pricing — must provision nodes for peak throughput
- Average throughput << peak throughput = wasted spend
- "Half the cost of DynamoDB" marketing ignores provisioning overhead

**User quote**: *"We had a huge spanner db with low throughput so had to add idle nodes just for storage which also ballooned costs."*

**User quote**: *"We were paying tens of thousands of dollars a month for Spanner plus tens of thousands of dollars a month for all the compute sitting in front of it."*

**Cloud9 answer**: Serverless on-demand pricing (pay per operation), plus self-hostable open-source option.

#### 2. GCP Platform Instability
**The Problem**:
- Constant version churn and breaking changes
- Undocumented features and performance gotchas
- Services deprecated without warning (Google Domains → Squarespace)
- Fear of product cancellation ("Will Spanner be shut down?")

**User quote**: *"Google runs their tech stack as if it's a startup that builds their CV. Everything is immature, tons of hacks, undocumented features."*

**User quote**: *"Much of the time GCP feels like a science project, and not a real business."*

**Cloud9 answer**: Open-source MIT license. Code can never be "shut down" by vendor. Community-driven development with stability guarantees.

#### 3. Support Quality
**The Problem**:
- Support ranges from "unhelpful" to "non-existent"
- Escalations go nowhere
- Bug reports closed as stale without resolution
- Sales team doesn't understand enterprise needs

**User quote**: *"Google's support is horrendous. They refer you to idiots that drag you through calls until your will for life dies."*

**User quote**: *"We have a bug reported back in 2020 that got closed recently without any action because it became stale."*

**User quote**: *"GCP support would suggest to ask in StackOverflow."*

**Cloud9 answer**: Community-driven support via GitHub Issues/Discussions. No support tax, no gatekeepers. Open development process.

#### 4. Documentation Gaps
**The Problem**:
- Performance characteristics poorly documented
- Sharding/partition behavior not explained clearly
- "Hot shard" problems discovered at scale
- No clear migration guides

**User quote**: *"Google's docs are incomplete; there are lots of performance gotchas that exist throughout the entire service, and they aren't clearly documented."*

**Cloud9 answer**: Comprehensive documentation from day one. Open-source allows reading the implementation. Design notes explain trade-offs.

#### 5. Operational Complexity
**The Problem**:
- GKE constant version churn forces infrastructure rework
- Network configuration complex (vs AWS/GCP VPC simplicity)
- Hidden costs (discovered $6k database in bill)
- Requires constant vigilance for breaking changes

**User quote**: *"50% of time making sure we are prepared for their shit and 50% our ambitious infra plans."*

**Cloud9 answer**: Single binary, minimal operational surface. Works identically local and global. No platform lock-in.

### DynamoDB Pain Points

#### 1. Data Model Limitations
**The Problem**:
- Key-value only, no SQL
- Must design access patterns upfront
- No ad-hoc queries or joins
- Single-table design patterns are complex

**User quote**: *"DynamoDB is fantastic for not doing things at scale... an entire RDBMS is way overkill for."*

**Cloud9 answer**: Both SQL and KV. Cross-API joins. Familiar relational model when needed.

#### 2. Capacity Planning Gotchas
**The Problem**:
- Hot partition issues (1000 WRU/partition limit)
- Shards get same quota regardless of traffic distribution
- Over-provision or queue requests to handle hot keys
- Not obvious from documentation

**User quote**: *"Even though you might have paid for 1000rps, that RPS volume is divided across all your shards."*

**Cloud9 answer**: Transparent sharding with automatic rebalancing. Cross-shard transactions at same consistency level.

#### 3. No Multi-Item Transactions
**The Problem**:
- TransactWriteItems limited to 25 items
- No true ACID across arbitrary keys
- Application must handle consistency

**Cloud9 answer**: Unbounded multi-key transactions with strict serializability.

### Common Theme: Trust and Lock-In

**Observation**: Users fear vendor lock-in more than they fear technical limitations.

**User quote**: *"Doing business with Google is a liability."*

**User quote**: *"I trust AWS to be a stable, long term foundation to build a product on, I don't trust GCP to be the same."*

**User quote**: *"Why am I going to sign up for a service that is surely to be canceled on a Google Whim™?"*

**Cloud9's fundamental answer**:
- Open-source MIT license removes vendor lock-in
- Self-hostable on any infrastructure
- Managed Dedalus Cloud offering for convenience, not lock-in
- Community can fork if Dedalus Labs disappears

### The Postgres Refuge

**Observation**: Many threads conclude "just use Postgres" because it's:
- Well-understood and stable
- Not vendor-locked
- Good enough for 99% of use cases

**User quote**: *"Postgres is a piece of software. Cloud Spanner/Dynamo etc are managed services. It makes no sense to directly compare."*

**User quote**: *"Golden Rule of data: Use PostgreSQL unless you have an extremely good reason not to."*

**Cloud9's position**: Be the **Postgres of distributed databases**:
- Open, trusted, boring technology
- Postgres wire compatibility
- Clear documentation and predictable behavior
- Available when you outgrow single-node Postgres

### Specific Technical Complaints

**Spanner**:
- DeWitt clause prevents independent benchmarking
- No protobuf column support in Cloud Spanner (only internal Spanner)
- Unclear whether Google services use Cloud Spanner or internal Spanner
- Write-through cache needed for read-heavy workloads (complexity + cost)

**DynamoDB**:
- Item size limits (400 KB max)
- Read/write unit calculations opaque (1 byte over 1KB = 2 RU charged)
- Connection management nightmare with Lambda/serverless

**Both**:
- Difficult to meaningfully compare offerings and value
- Lock-in makes switching costs prohibitive
- Enterprise architects push them for imagined scale needs

### Additional Insights from Developer Communities

#### Spanner Positioning Problem
**The Problem**:
- "Overkill for prototypes" — minimum cost too high for experimentation
- "Mosquito with a sledgehammer" — power users don't need, small users can't afford
- Recommendation is always "use Cloud SQL instead" — Spanner's own ecosystem recommends against it

**User quote**: *"Spanner is pricey - do you need that scale/availability? Cloud SQL would be more your speed."*

**User quote**: *"Spanner is a mosquito with a sledgehammer for most workloads."*

**User quote**: *"I'd say stick with cloud SQL for prototyping, Spanner is for production."*

**Implication**: Spanner has no **"grow into it"** story. You can't start small and scale up — the entry point is already enterprise-scale pricing.

**Cloud9 answer**: Start local (SQLite-level simplicity), scale to regional, scale to global — same binary, same semantics. No cliff between "prototype" and "production."

#### The Postgres Gravitational Pull
**The Problem**:
- Every Spanner discussion ends with "just use Postgres"
- Postgres-compatible offerings (AlloyDB, Cloud SQL) recommended over Spanner
- Even Google's own advocates suggest Postgres alternatives

**User quote**: *"Please, do Postgres, not MySQL. Let it die already."*

**User quote**: *"If you can sling postgres I'd go straight to alloydb."*

**Cloud9 answer**: Postgres wire compatibility from day one. Be where developers already are, not where they have to migrate to.

#### Developer Experience Friction
**The Problem**:
- No local development story ("can't install software on desktop")
- Cloud-only development is clunky (Cloud Shell, Cloud Editor)
- No emulator for cost-controlled local dev (unlike AlloyDB)
- Forces developers into specific GCP workflows
- Missing features vs Postgres (no stored procs, no ts_vector, limited data types)

**User quote**: *"Developing in the cloud is possible. If you go to cloud shell you can open a cloud version of vscode. Haven't used it much so not sure how well it works."*

**User quote**: *"Spanner [lacks] auto increment counters, ts_vector as type and a bunch more."*

**User quote**: *"Still no support for user-created stored functions/stored procs."*

**Cloud9 answer**: Single binary runs locally. Develop on laptop, deploy to cloud without changes. No forced cloud-development workflow. Full Postgres compatibility from day one.

### What Users Actually Want

**Synthesis from discussion**:

1. **Predictable, transparent pricing** — no surprise bills, no forced provisioning
2. **Stability and trust** — won't be deprecated, won't see 10x price increases
3. **Good enough for small scale, grows to large** — DynamoDB's free tier vs Spanner's $65/month floor
4. **Familiar interfaces** — SQL preferred, KV when needed
5. **Open and portable** — can leave vendor without rewrite
6. **Real support** — responsive humans who understand the problem
7. **Clear documentation** — performance characteristics, limits, gotchas all documented upfront
8. **Local development** — prototype locally, deploy globally without workflow changes

**Cloud9's design targets all eight points.**

### The Billing Horror Stories

**The most damaging feedback**: Silent, unexpected charges that destroy user trust.

#### The RAG Engine Incident (September 2024)
**What happened**:
- Google changed RAG Engine backend to use Spanner (Scaled Tier, 1000 PU)
- **No clear notification** to affected users (some got email, many didn't)
- Users who tried RAG Engine once got charged $30-800/day
- Charges appeared as "Cloud Spanner" even though users never enabled Spanner
- Spanner instances didn't show up in Spanner console (hidden)
- **Auto-provisioned in ALL regions** (US + EU) per project

**User quote**: *"$30/day for a service I didn't knowingly use seems extremely expensive. I deleted all my projects to make sure no keys were leaked."*

**User quote**: *"$300 for me 😭"*

**User quote**: *"Another victim here. $800 gone."*

**User quote**: *"What Google is doing on this one is, frankly, appalling. It's theft."*

**User quote**: *"I had bit faith in GCP to convince my company to switch from Azure. Now no way I can/will recommend anyone to use GCP."*

**The worst case** (£3,000 / $3,800 in one month):
- Dormant account (£0.02/month residual storage)
- Sept 3: charges spike to £60-70/day
- User never created RAG corpus, never uploaded data
- RAG UI shows nothing
- **Billing account frozen** → can't access account to delete resources
- **Catch-22**: Must pay disputed balance to unlock account to stop charges
- Support closes tickets, refuses escalation
- Balance climbing daily with no way to stop

**User quote**: *"The catch-22: billing suspension prevents me from accessing my account to delete the service/close the account, but Google says I must pay the disputed balance first to unlock it."*

**User quote**: *"Support has closed my tickets multiple times, refuses to escalate further, and won't deprovision the hidden resources."*

**Google's response**: *"After final review, charges are valid. These were provisioned as a necessary component of the Vertex AI RAG Engine service you activated... charges are considered legitimate."*

**User's dilemma**: *"Can I just refuse to pay? What happens if it goes to debt collections?"*

**The resolution process**:
- 2+ hour wait times for support
- Users had to manually delete RAG Engine (not obvious)
- Must delete **per region** (auto-enabled in multiple regions)
- Some got 90% refund as "one-time courtesy"
- Many charged for weeks before noticing
- Some accounts frozen, unable to delete resources
- Documentation says "free to use" but hides $2k/month Spanner cost

**User quote**: *"RAG Engine docs say it's 'free to use' but fail to mention the auto-provisioned Spanner instance costs £2k+/month."*

**User quote**: *"Those RAG Engine Cloud Spanner services were automatically enabled for ALL available regions and for each individual project. I needed to delete them one by one."*

**User quote**: *"GCP assistant is essentially useless and I couldn't find any live chat support... Wasted half my morning on this crap."*

**User quote**: *"Very poor customer communication."*

**User quote**: *"GCP will definitely beat estimates now LOL. Some MBA wearing a vest will get a big bonus in exchange for all the misery."*

**Separate incident** (2025): Gemini 2.5 Flash billing error generated **$70,000+ bills** for non-existent usage, charges climbing $10,000/day even after API keys deleted.

#### The Pattern
1. Service defaults change silently
2. Expensive resources provisioned automatically
3. Poor visibility (instances don't show in expected console)
4. Slow/unhelpful support response
5. Users lose trust permanently

**User response to billing disasters**: Migration away from GCP entirely.

**User quote**: *"Time to migrate. I can host my rag setup on my vps that I pay $12 a month for."*

**The exodus**: Users leave GCP for **$12/month VPS** rather than pay surprise Spanner bills.

**Cloud9's commitment**:
- **No silent defaults** — explicit opt-in for all paid tiers
- **Visible resource usage** — every replica, every shard visible in console
- **Billing transparency** — real-time cost tracking, no surprises
- **Zero-cost local mode** — develop and test for free
- **Community support** — no support tax, no wait queues
- **Self-hosting option** — run on $12/month VPS if desired, same guarantees

### Pricing Transparency Issues

**The "Basic vs Standard" confusion**:
- Documentation shows different tiers in different places
- Support agents have access to different pricing calculators
- Processing Units (PU) pricing not obvious
- "Wind down when not using" not possible (always-on charges)

**User quote**: *"Documentation appears to be inconsistent. Some suggest there is a 'basic' tier, but when you go to the estimate page, it starts with 'Standard'."*

**User quote**: *"Does anyone know how to lower the costs when you're in Dev mode? Is there a way to wind down the environment when you're not using it?"*

**Answer from community**: No. Spanner charges for provisioned capacity, not usage.

**Cloud9 answer**:
- Open-source = free local development
- Managed tier pricing published upfront
- Can "wind down" by stopping the binary (self-hosted)
- Pay-per-operation option (no idle charges)

### Early Adoption Concerns (2017 Launch)

**From initial Spanner launch discussions**:

#### Understanding Barrier
**The Problem**:
- Complex architecture hard to explain
- TrueTime concept not intuitive
- Users struggled to understand when Spanner is needed vs Postgres
- "Shitty article" complaints (marketing-heavy, light on substance)

**User quote**: *"That was a really shitty article. Can anyone explain the real world benefits of this?"*

**User quote**: *"All that babel about TrueTime and nowhere a description of the problem it solves."*

**Cloud9 answer**: Clear design notes (this document) explain trade-offs. No hand-waving about "mastering time."

#### Schema Modeling Constraints
**The Problem**:
- No explicit foreign keys outside parent/child relationships
- No multi-parent tables (can't model true many-to-many easily)
- Must choose access patterns upfront (parent/child determines sharding)
- No referential integrity constraints across tables
- No triggers
- No reference types for columns

**User quote**: *"How does a Many-to-Many relationship work? Are they all root level tables? Is there no explicit foreign keys?"*

**User quote**: *"You cannot put one table as a child of two others."*

**User quote**: *"There are also no triggers, and no reference types (so you can't define a column in BankAccount as type 'key of Bank')."*

**Answer from community**: *"No cross-table referential integrity constraints... implementing them would be costly in terms of latency."*

**The ACID debate**: Users argue Spanner isn't truly ACID because it lacks the "C" (consistency via constraints).

**User quote**: *"Spanner is not ACID. It's AID. It lacks the C of 'the data in the schema conforms to the business rules.' If you don't have foreign keys, triggers, range limitations, you don't have C."*

**Google's defense**: *"ACID for Spanner means the consistency rules definable for Spanner's not-really-an-RDBMS model are upheld."*

**Cloud9 answer**: Standard SQL foreign keys, constraints, and triggers. Postgres compatibility means familiar schema modeling. True ACID with full referential integrity.

#### Trust in Complexity
**The Problem**:
- Skepticism about needing atomic clocks
- "Why not just use Postgres?" dominates discussion
- Benefits unclear until extreme scale
- GPS/atomic clock dependency seems fragile

**User quote**: *"If some muppet decides to mess with GPS signals near the datacenter, what happens?"*

**User quote**: *"Almost no applications have important use cases that make such a solution a requirement for success."*

**Answer from community**: *"Uses 6 time masters. 3 GPS clocks with individual antennas, 3 atomic clocks. Kalman filter rejects bad GPS, falls back to atomic."*

**Cloud9 answer**:
- Works without atomic clocks (HLC on commodity hardware)
- Clear failure modes documented
- Benefits obvious from prototype to production (same binary)

#### The "Just Use Postgres" Reflex

**Consistent theme across all discussions**: Default to Postgres unless you absolutely can't.

**User quote**: *"Just use PG...until you can't."*

**User quote**: *"Are you sure your data doesn't fit in PostgreSQL? You should probably try PostgreSQL first."*

**User quote**: *"The super-power of Postgres is that it supports everything... doesn't suck at anything but horizontal scaling."*

**The Spanner problem**: No story for "start with Postgres, grow into Spanner." It's a hard cut-over.

**Cloud9's answer**:
- **Is** Postgres for small scale (wire-compatible, single binary)
- Grows to Spanner-class scale without migration
- No "Postgres vs Spanner" decision — it's both

### The "Overkill for Real Workloads" Pattern

**Recurring scenario**: Users evaluate Spanner for moderate scale, realize it's massive overkill.

**Real case** (9 months ago):
- 500 requests/second (read-heavy)
- 50 GB data
- Public API, no auth
- Looking to replace Firestore (query limitations)

**User calculations**: "200 processing units handles 15k QPS... I doubt it."

**Community response**:

**User quote**: *"Are you stupidly rich? Like the lost son of Sultan of Brunei? No? Then it's too expensive, consider other options."*

**User quote**: *"Sounds like bringing in a tank into a boxing fight."*

**User quote**: *"500 requests per second is not that much honestly. Spanner... is designed for much higher throughput."*

**User quote**: *"A lot of over-engineered tech choices are sometimes to compensate for lack of applying fundamentals with simpler and cheaper alternatives."*

**Googler's response**: Spanner = $146/month, Cloud SQL = $231/month (Spanner cheaper!)

**User's conclusion**: *"After more digging into the subject I realize I don't need it."*

### The Disconnect

**Google's pitch**: "Spanner is cheaper than Postgres!"

**Reality**:
- Users still choose Postgres
- Not because of cost
- Because Spanner feels wrong for the scale
- "PostgreSQL enters the chat" (final comment)

**Why this matters**:
- Even when Spanner is **cheaper**, users reject it
- The "overkill" perception is **psychological**, not economic
- Users want technology that feels appropriate to their scale
- Spanner positioned as "big company tech" → small companies avoid it

**Cloud9's advantage**:
- Same binary from prototype (500 RPS) to massive scale (500k RPS)
- No psychological barrier
- "Just use Cloud9" → natural default like "just use Postgres"
- Pricing scales with you (free → cheap → expensive as you grow)

### Cloud SQL Performance Issues

**Reported problems** with Google's managed Postgres/MySQL:

#### Performance Degradation
**The Problem**:
- CloudSQL slower than self-hosted on VMs
- Read locking happens frequently
- Replication lag even within same zone
- Trigger execution delays on replicas

**User quote**: *"My experience with CloudSQL was horrendous. It was slow and read locking happened ridiculously often. Once I spinned up a MySQL instance on a VM, everything worked flawlessly."*

**User quote**: *"We had significant slowness with cloudsql and moved to managing our own instances on VMs and haven't looked back."*

**User quote**: *"We've seen significant delay in some replicas (in the same zone) for some more complex triggers."*

**The irony**: Users migrate to Spanner not because they need global distribution, but because **CloudSQL is unreliable**.

#### The "Fast Reads" Trap
**User's goal**: "Need the database to be highly available and never waiting on locks... guaranteeing fast reads all the time."

**Community response**: Spanner doesn't solve this.
- Spanner still uses locks for read-write transactions
- Lock-free read-only transactions exist, but user may not know to use them
- "CloudSpanner would be a way to have GCP manage everything about scaling" (wrong expectation)

**User quote**: *"CloudSpanner starts to make sense once your DB is larger than 10TB and you need replication across the whole planet."*

**User's realization**: *"This is not my challenge, but rather guaranteeing fast reads all the time."*

**Recommendation given**: CloudSQL with read replicas (back to where they started).

**Cloud9 answer**:
- Lock-free read-only transactions by default (documented clearly)
- MVCC means readers never block writers
- Works locally for testing before cloud deployment
- No CloudSQL performance issues (you control the hardware)

### The Expectation Mismatch

**Pattern observed**:
1. User has performance issues with CloudSQL
2. User investigates Spanner as "better managed database"
3. Community asks: "Do you have billions of dollars?"
4. User realizes Spanner solves different problem
5. User sent back to CloudSQL or self-hosting

**The gap**: No managed database between "CloudSQL (unreliable)" and "Spanner (overkill)".

**Quote**: *"Do you have billions of dollars? [No] That would be awesome lol - but no."*

**Cloud9's positioning**:
- Fills the gap between CloudSQL and Spanner
- Self-hostable (control your own performance)
- OR managed tier (Dedalus Cloud)
- Same guarantees at all scales
- No "do you have billions?" barrier

---

## References

- [Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport's original paper
- [Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases](https://cse.buffalo.edu/tech-reports/2014-04.pdf) — HLC paper
- [Spanner: Google's Globally Distributed Database](https://research.google/pubs/pub39966/) — TrueTime and external consistency
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Kleppmann's book, MVCC and consistency models
- [CockroachDB: The Resilient Geo-Distributed SQL Database](https://dl.acm.org/doi/10.1145/3318464.3386134) — HLC in production
- [FoundationDB: A Distributed Unbundled Transactional Key Value Store](https://www.foundationdb.org/files/fdb-paper.pdf) — Sequencer architecture
