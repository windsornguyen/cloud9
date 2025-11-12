# Consensus Architecture

**Question**: How do we replicate data reliably and make it the foundation for operational flexibility?

**Answer**: Raft consensus with clean abstractions for operational features.

## Why Raft

Raft is the proven choice for distributed databases:

- **Battle-tested**: Used in etcd, CockroachDB, TiKV, Consul
- **Formally verified**: Proven correct in Coq/TLA+
- **Understandable**: Clear separation between leader election, log replication, and safety
- **Predictable**: Well-documented failure modes and performance characteristics

**Not novel. That's the point.**

Cloud9's goal isn't to innovate in consensus algorithms. It's to build a database that operators can trust. Raft is the consensus algorithm you can explain to your team, debug in production, and find papers about when things go wrong.

## The Consensus Driver Interface

Cloud9 treats consensus as an isolated component with a narrow interface. This isn't "pluggable consensus" (swapping algorithms at runtime). It's clean architecture: separating the replication mechanism from the state machine it replicates.

### The Interface

```rust
trait ConsensusDriver {
    // Propose a command for replication
    fn propose(&mut self, cmd: Command) -> Result<LogIndex>;

    // Apply committed entries to state machine
    fn poll_committed(&mut self) -> Vec<CommittedEntry>;

    // Read current leader
    fn leader(&self) -> Option<ReplicaId>;

    // Transfer leadership (operational control)
    fn transfer_leadership(&mut self, target: ReplicaId) -> Result<()>;

    // Add/remove replicas (reconfiguration)
    fn reconfigure(&mut self, new_config: RangeConfig) -> Result<()>;
}
```

**That's it.** The rest of Cloud9 (transaction coordinator, MVCC, SQL execution) doesn't care about Raft internals. It sees:
- A log of committed commands (linear order)
- A current leader (for write routing)
- Control knobs for operational needs

### Why This Boundary Matters

**Testability**: The state machine (MVCC storage engine) can be tested independently with a simulated consensus driver. No need to spin up 5-node Raft clusters to test transaction semantics.

**Debuggability**: When production debugging consensus issues, engineers only need to understand Raft. When debugging transaction issues, they only need to understand MVCC. Concerns don't bleed.

**Evolution**: If a better consensus algorithm emerges (peer-reviewed, formally verified, widely deployed), the interface provides a migration path. But this is a "someday, maybe" scenario, not a launch requirement.

## Operational Advantage: Zero-Downtime Upgrades

Raft's joint consensus protocol, combined with Cloud9's timestamped schema architecture, enables true zero-downtime upgrades—a capability that emerges naturally from the design rather than being retrofitted.

### The Rolling Upgrade Protocol

**Procedure**:
1. Add new replica running v2.0 binary as learner (receives log, doesn't vote)
2. Wait for learner to catch up on Raft log
3. Promote learner to voter using joint consensus (temporary state where both old and new quorum overlap)
4. Transfer leaseholder to v2.0 replica (leader can now be v2.0 node)
5. Remove old v1.0 replica from configuration
6. Repeat for all ranges in the cluster

**Why this works**:

**Schema changes are timestamped**: Queries don't ask "what version is this node running?" They ask "what was the schema at timestamp T?" Old nodes query at old timestamps (old schema), new nodes query at new timestamps (new schema). Both interpret the same MVCC key-value data correctly because schema interpretation is decoupled from binary version.

**Raft log format includes protocol markers**: Each log entry carries version information. Nodes negotiate compatible protocol during handshake. If v2.0 introduces new log entry types, v1.0 nodes can skip unknown entries (forward compatibility) or v2.0 nodes can write v1.0-compatible entries during the joint consensus phase (backward compatibility).

**Quorum never lost during membership changes**: Joint consensus ensures that no single point in time requires agreement from both old and new majorities. Writes continue flowing because either the old quorum or new quorum can commit—never stuck waiting for both.

**Leaseholder isolation**: Leadership can transfer to v2.0 nodes before v1.0 nodes are removed. The leaseholder (which handles reads) runs the new binary while followers (which only replicate) can still run old binaries. Read path and write path operate on different versions simultaneously.

### Why Postgres Can't Do This

**Postgres treats schema as global state**: `ALTER TABLE users ADD COLUMN` acquires `AccessExclusiveLock` and updates system catalogs (`pg_class`, `pg_attribute`) atomically across all nodes. Even with MVCC for table data, the schema change requires a coordination barrier where all nodes observe the same catalog version. Mixed-version clusters can't agree on schema.

**No protocol versioning in replication**: Postgres streaming replication and logical replication protocols lack version negotiation. If v16 changes the replication message format, v15 followers can't decode it. Upgrades require stopping all nodes, upgrading binaries, and restarting—downtime by necessity.

**Binary format incompatibilities**: Postgres heap tuple format, WAL record structure, and catalog schemas change between major versions. Mixed-version clusters would corrupt data. `pg_upgrade` exists specifically because in-place rolling upgrades are architecturally impossible.

### Cloud9's Architectural Advantages

**1. Schema is Raft-replicated metadata, not global locks**: DDL operations write new schema versions to the metadata keyspace with commit timestamps. Queries pick schema at transaction start time. No coordination needed—schema evolution is just another replicated write.

**2. Protocol versioning from day one**: RPC messages include version headers. Nodes handshake to negotiate compatible protocol. Raft log entries carry format versions. Mixed-version clusters are supported by design, not retrofitted.

**3. MVCC extends to schema**: The same mechanism that enables lock-free reads on data enables lock-free schema evolution. Transactions see (data@timestamp, schema@timestamp) pairs. Upgrading binaries doesn't change data layout—only schema interpretation.

**4. Per-range independence**: Each range can upgrade independently. No cluster-wide "flip the switch" moment. If a range upgrade fails, it doesn't cascade to other ranges. Fault isolation by design.

### Production Reality

CockroachDB added zero-downtime upgrades after years of production pain (v20.x+, ~2020). Early versions required careful orchestration and failed frequently. Spanner has this capability but is proprietary—can't verify implementation.

Cloud9 designs for it from the start: timestamped schemas, versioned protocols, Raft-based replication that supports gradual state evolution. The architecture assumes mixed-version operation is normal, not exceptional.

This is why Cloud9 can claim "one-click zero-downtime upgrades" as a day-one feature—the primitives are already present in the core design.

## Operational Features: Within Raft

The consensus driver interface enables operational flexibility without algorithmic complexity. These features live within Raft's existing framework:

### 1. Dynamic Leader Placement

**Problem**: The leader handles all writes. Placing the leader in the wrong datacenter adds cross-region latency to every write.

**Solution**: Raft's leadership transfer mechanism.

```
// Before: leader in us-east-1, app in eu-west-1
write latency = RTT(eu-west-1 → us-east-1) = ~80ms

// After: transfer_leadership(eu-west-1_replica)
write latency = local = ~5ms
```

**Cloud9 exposes this** via the range placement API:
```sql
ALTER RANGE users CONFIGURE LEADER PREFERENCE 'eu-west-1';
```

**Not a new algorithm.** Raft already has leadership transfer (§3.10 of the paper). Cloud9 just makes it operationally accessible.

### 2. Witness Replicas

**Problem**: You want 5-replica durability (survive 2 failures) but don't want to store 5 full copies of the data.

**Solution**: Witness replicas participate in quorum but don't store data.

```
3 full replicas + 2 witness = 5-node quorum
- Survives 2 failures (any 2 of the 5 can be down)
- Only 3× storage cost (not 5×)
```

**Mechanism**:
- Witness receives log entries, votes in elections, participates in quorum
- Witness doesn't apply entries to a state machine (no storage)
- Witness can't serve reads (it doesn't have data)

**Cloud9 usage**:
```sql
ALTER RANGE orders ADD REPLICA ON 'us-west-2' AS WITNESS;
```

**Not novel**: TiKV calls these "learner replicas without data." Raft's joint consensus (§6) handles configuration changes safely.

### 3. Learner Replicas

**Problem**: Adding a new full replica requires copying data. If you immediately add it to the quorum, the majority becomes unavailable during the copy (the new replica can't vote yet, but counts toward the quorum size).

**Solution**: Learner replicas receive log entries but don't vote.

**Protocol**:
1. Add replica as learner
2. Replica catches up (receives log, applies to state machine)
3. Once caught up, promote to voting member
4. Now safe: it won't block quorum

**Cloud9 automation**:
```sql
ALTER RANGE products ADD REPLICA ON 'eu-central-1';
-- Internally:
--   1. Add as learner
--   2. Stream snapshot + log
--   3. Auto-promote when caught up
```

**Not novel**: Raft's single-server membership changes (§4.1) + learner role (LogCabin implementation, etcd).

### 4. Per-Range Configuration

Every range (shard) has independent consensus configuration:

```rust
struct RangeConfig {
    replicas: Vec<ReplicaDescriptor>,
    leader_preference: Option<Region>,
    quorum_size: usize,
}

struct ReplicaDescriptor {
    id: ReplicaId,
    region: Region,
    role: ReplicaRole, // Voter | Witness | Learner
}
```

**Why per-range?**
- Multi-tenant: different tables have different SLAs
- Geo-distributed: hot data in 3 regions, cold data in 1
- Cost optimization: critical data = 5 replicas, logs = 3 replicas

**Example**:
```sql
-- User data: 5 replicas across 3 regions
ALTER RANGE users CONFIGURE REPLICAS 5 IN REGIONS ('us-east-1', 'eu-west-1', 'ap-southeast-1');

-- Logs: 3 replicas, same region
ALTER RANGE logs CONFIGURE REPLICAS 3 IN REGIONS ('us-east-1');
```

**Implementation**: Each range runs its own Raft group. Configurations are independent. The SQL layer maps keys to ranges and routes accordingly.

## What Cloud9 Does NOT Do

### 1. Ship Multiple Consensus Algorithms

**Not at launch.** Maybe not ever.

Shipping multiple algorithms means:
- Testing N² interactions (MVCC × algorithm permutations)
- Documenting N sets of operational behaviors
- Debugging production issues across N state machines
- Maintaining compatibility as algorithms evolve

**Cost >> benefit** for a young database.

If Raft proves inadequate (unlikely, given etcd/CockroachDB/TiKV operate at scale), we revisit. But the interface is ready.

### 2. Multi-Raft by Default

**Cloud9 uses multi-Raft** (each range = separate Raft group), but this isn't a feature users configure. It's an implementation detail.

**Why multi-Raft?**
- Scale: Single Raft group limits throughput (leader bottleneck)
- Geo-distribution: Different ranges can have replicas in different regions
- Load balancing: Spread leadership across nodes

**Users don't care.** They configure ranges (via SQL schema). The system maps ranges to Raft groups internally.

### 3. Real-Time Reconfiguration Guarantees

Raft's joint consensus (§6) ensures **safety** during configuration changes: no split-brain, no data loss.

What Raft doesn't guarantee: **zero downtime** for arbitrary changes.

**Example**: If you remove 2 replicas from a 3-replica range simultaneously, the range becomes unavailable (no quorum). This is correct behavior (you violated quorum math), not a Raft bug.

**Cloud9's position**: Provide operator guardrails (warnings, confirmation prompts) but don't prevent valid operations. If an operator says "drain this node NOW," we do it, even if it breaks quorum. Better a deliberate outage than a stuck operator.

## Addressing the "Pluggable Consensus" Critique

**Concern**: "Isn't 'extensible consensus' just resume-driven development? Adding complexity to claim buzzword compliance?"

**Answer**: No. Here's why:

### What Cloud9 Is NOT Doing

- Shipping multiple consensus algorithms at launch
- Promising "swap Raft for Paxos in production"
- Building abstractions for hypothetical future algorithms
- Adding configuration options users must understand

**None of these exist.**

### What Cloud9 IS Doing

**Clean separation of concerns:**
- The consensus driver replicates a log
- The state machine applies committed entries
- The interface between them is narrow and testable

**This isn't about extensibility for extensibility's sake.** It's about not letting Raft internals leak into transaction logic.

### The Analogy

Consider a database storage engine:
```rust
trait StorageEngine {
    fn put(&mut self, key: Key, value: Value);
    fn get(&self, key: Key) -> Option<Value>;
}
```

This doesn't mean "we ship 5 storage engines." It means:
- The SQL layer doesn't hardcode RocksDB calls
- Testing doesn't require spinning up RocksDB
- If RocksDB has a critical bug, swapping it isn't a SQL-layer rewrite

**Same principle for consensus.** The interface isn't for users. It's for maintainability.

## Why This Matters for Cloud9

Cloud9's goal: **daily driver database with zero surprises.**

Raft achieves this because:
- It's proven (safety, liveness)
- It's understandable (operators can reason about it)
- It's flexible (leadership transfer, witnesses, learners)

The consensus driver interface achieves:
- Testability (mock consensus for unit tests)
- Debuggability (isolate consensus bugs from transaction bugs)
- Evolvability (if needed, but not at launch)

**Not resume padding. Not pluggability theater. Just clean architecture that happens to enable future evolution if research advances.**

## Future: If Research Produces Better Algorithms

**Hypothetical**: A new consensus algorithm emerges with:
- Formal verification (Coq/TLA+ proof)
- Production deployment at scale (5+ years, multiple orgs)
- Clear operational advantages (lower latency / higher throughput / better availability)

**Then**: The consensus driver interface provides a migration path.

**Migration strategy**:
1. Implement new algorithm behind ConsensusDriver trait
2. Test exhaustively (months, not weeks)
3. Deploy on non-critical ranges (logs, analytics)
4. Migrate critical ranges only after proving stability
5. Maintain Raft as fallback for years

**This is a "someday, maybe" scenario.** Raft is good enough for etcd (Kubernetes control plane), CockroachDB (banks), and TiKV (PingCAP's flagship). It's good enough for Cloud9.

## Comparison to Other Databases

**CockroachDB**: Uses Raft, but Raft is deeply integrated into the KV layer. No clean interface.

**TiDB**: Uses Raft (via TiKV), also tightly coupled. Adding operational features (witnesses) requires TiKV changes.

**Spanner**: Uses Paxos, proprietary, not extensible.

**YugabyteDB**: Uses Raft, but also has tablet-level coupling.

**Cloud9**: Raft behind a clean interface. Same operational flexibility, better separation of concerns.

## Conclusion

**Cloud9 ships with Raft. Only Raft.**

But Raft is the foundation for operational control:
- Dynamic leader placement (write latency optimization)
- Witness replicas (storage cost optimization)
- Learner replicas (safe reconfiguration)
- Per-range configuration (multi-tenant flexibility)

The consensus driver interface isn't about swapping algorithms. It's about:
- Testing transaction logic without Raft
- Debugging consensus issues without understanding MVCC
- Maintaining a codebase where Raft concerns don't leak into SQL

**This is clean architecture, not resume-driven development.**

And if a better consensus algorithm emerges in 5 years? The interface is ready. But that's a decision for future Cloud9, after Raft proves insufficient. Which, given etcd/CockroachDB/TiKV, seems unlikely.

**For now: One consensus algorithm, done right.**
