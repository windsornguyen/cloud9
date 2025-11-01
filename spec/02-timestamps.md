# Timestamp Strategies

## Why Not Lamport Clocks

Lamport clocks cannot guarantee external consistency, even within a single cluster.

### The Problem

Consider this scenario:

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
- A message from another node arrives

If Replica B hasn't received any messages from Replica A (or other nodes that know about the write) before the client's read arrives, B's clock can be arbitrarily behind—even though the write finished in real time.

### What Lamport Clocks Actually Solve

**Designed for**: Ordering events when all communication goes through the system.

**Perfect use cases**:
- Distributed tracing (causality in logs)
- CRDTs (eventual consistency with causal order)
- Event sourcing (ordering events in a distributed log)
- Deadlock detection (wait-for graph ordering)

**Key property**: If A → B via system messages, then L(A) < L(B).

**What they don't capture**: If A finishes before B starts in real time, but no message connects them, Lamport clocks don't guarantee L(A) < L(B).

### The Database-Specific Issue

In databases, external communication is constant:
- User sees write succeed in UI, refreshes page (new request to different server)
- Microservice A writes, calls microservice B via HTTP, B reads
- Client writes, tells colleague verbally, colleague reads

**None of these involve database messages**, so Lamport clocks can't track them.

## The Three Modes

Cloud9 needs timestamps that respect real-time order. Three viable approaches exist:

### 1. Hybrid Logical Clocks (HLC)

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

### 3. Timestamp Oracle (TSO)

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

## Trade-offs

| Aspect | HLC | TSO | TrueTime |
|--------|-----|-----|----------|
| Scalability | High (decentralized) | Medium (oracle bottleneck) | High (decentralized) |
| Write latency | ~ε commit-wait | No commit-wait, but oracle hop | ~ε commit-wait (tight) |
| Dependency | Clock sync (NTP/PTP) | TSO service (must be HA) | GPS + atomic clocks |
| Operational complexity | Clock monitoring | TSO operations | Specialized hardware |
| Failure mode | Fail-stop on skew > max | Block if TSO unreachable | Hardware-dependent |
| Typical ε | 10-50ms (PTP), 50-100ms (NTP) | N/A (logical ordering) | 1-7ms |

## Cloud9's Choice

**Primary mode**: HLC + commit-wait
- Decentralized, scales well
- Works on commodity cloud hardware (AWS Time Sync Service)
- Matches CockroachDB's approach
- Provides meaningful timestamps (wall-clock time)

**Alternative mode** (configurable): TSO
- For deployments where clock synchronization is difficult
- Or when ε is too large (poor NTP sync)
- Trades write latency for simpler time discipline

**Future enhancement**: TrueTime-like with GPS/atomic clocks for Dedalus Cloud premium tier (colocated deployments).

### Rationale

HLC provides the best balance of:
1. **Decentralization** - no single point of bottleneck
2. **Practical deployment** - works on standard cloud infrastructure
3. **Real-time semantics** - timestamps have wall-clock meaning
4. **External consistency** - commit-wait ensures correctness

TSO mode exists for edge cases where clock synchronization is unreliable, but HLC is the default because it scales better and provides more meaningful timestamps.

TrueTime represents the theoretical ideal but requires specialized hardware not available on public clouds. Cloud9 can achieve similar guarantees with HLC at slightly higher latency (~10-50ms vs ~1-7ms).

## Implementation Details

### HLC Mode: Commit-Wait Protocol

```
1. Coordinator assigns t_w from HLC
2. Replicate via Raft to quorum
3. Commit-wait until now() > t_w + ε
4. Acknowledge to client
```

**Clock uncertainty measurement**:
- Query chrony/NTP for current offset and jitter
- Set ε = max_observed_offset + drift_allowance
- Fail-stop if observed skew > max_offset (safety)

### TSO Mode: Centralized Sequencing

```
1. Coordinator requests timestamp from TSO
2. TSO returns strictly increasing u64
3. Coordinator uses timestamp for commit
4. No commit-wait needed for time uncertainty
```

**TSO availability**:
- TSO itself must be replicated (Raft or similar)
- Failure of TSO blocks all writes
- Can pre-allocate timestamp batches to reduce round-trips

## Why Commit-Wait Is Unavoidable

For external consistency with physical timestamps, some barrier is required:
- Wait for time (HLC/TrueTime commit-wait), OR
- Wait for order (TSO safe timestamp propagation), OR
- Wait for batch (deterministic sequencing)

The PACELC theorem applies: Even without network partitions, we must trade latency for consistency.

**Cloud9's choice**: Pay the latency. External consistency is non-negotiable for a "daily driver" database where users expect intuitive behavior.

### What Commit-Wait Fixes

Even with perfectly synchronized clocks, there's a gap between when a leader commits locally and when followers apply the commit. Commit-wait ensures:

```
By the time client gets "success," every replica's clock is past t_w.
Any future operation (even immediately after) will get timestamp > t_w.
```

This covers both clock uncertainty and replication propagation time.

## References

- [Time, Clocks, and the Ordering of Events in a Distributed System](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — Lamport's original paper
- [Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases](https://cse.buffalo.edu/tech-reports/2014-04.pdf) — HLC paper
- [Spanner: Google's Globally Distributed Database](https://research.google/pubs/pub39966/) — TrueTime and external consistency
- [CockroachDB: The Resilient Geo-Distributed SQL Database](https://dl.acm.org/doi/10.1145/3318464.3386134) — HLC in production
- [FoundationDB: A Distributed Unbundled Transactional Key Value Store](https://www.foundationdb.org/files/fdb-paper.pdf) — Sequencer architecture
