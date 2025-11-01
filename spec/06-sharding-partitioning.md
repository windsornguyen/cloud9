# Sharding and Partitioning

**Question**: How do we scale horizontally while maintaining single-node performance?

**Answer**: Range-based sharding with the degenerate case: local mode = 1 range, 1 replica.

## The Core Idea

Cloud9's entire key-value space is a single MVCC-versioned namespace partitioned into contiguous ranges:

- **Range**: A contiguous interval of keys [start, end) with all MVCC versions
- **Raft Group**: Each range is replicated via Raft consensus (3+ replicas for distributed, 1 for local)
- **Leaseholder**: One replica per range holds a lease, serves all reads/writes for that range
- **Local Mode**: The entire keyspace = 1 range with 1 replica = zero network overhead

**Result**: Same binary scales from laptop (1 range, no replication) to global deployment (thousands of ranges, 3+ replicas each).

## Why This Is Natural

Physical libraries partition books by call number (contiguous ranges). One shelf = one range. One librarian = one leaseholder. A personal library is still a library—it just happens to have one shelf with one caretaker. Cloud9 uses the same model: local deployment is the degenerate case where all keys fit in one range on one node.

## Range Sharding Implementation

### Range Definition

```rust
struct Range {
    range_id: RangeID,           // Globally unique
    start_key: Key,              // Inclusive
    end_key: Key,                // Exclusive
    raft_group: RaftGroupID,     // Maps to Raft consensus group
    replicas: Vec<ReplicaID>,    // Where this range lives
    leaseholder: ReplicaID,      // Current lease holder
}
```

Ranges partition the keyspace exhaustively:
- Keys are lexicographically ordered
- No gaps: range[i].end_key == range[i+1].start_key
- No overlaps: ranges[i] and ranges[j] (i ≠ j) are disjoint

### Raft Group per Range

Each range = one Raft group:
- Raft log contains all writes to keys in [start_key, end_key)
- Replicas store the same MVCC key-value pairs
- Leaseholder serves reads (bypasses Raft quorum for performance)
- Writes go through Raft (majority quorum for durability)

**Local mode optimization**: 1 replica = 1 Raft group with quorum size 1. Raft becomes a glorified write-ahead log with zero network calls.

### Leaseholder Architecture

Leaseholder = the replica with the exclusive right to serve reads/writes for a range:

```rust
struct Lease {
    range_id: RangeID,
    replica_id: ReplicaID,
    start_time: HLCTimestamp,
    expiration: HLCTimestamp,
    sequence: u64,               // Fencing token
}
```

**Guarantees**:
- Only one leaseholder per range at any time (via fencing tokens)
- Reads bypass Raft consensus (leaseholder has the latest committed data)
- Writes use Raft but leaseholder coordinates
- Lease transfers when node fails or rebalancing occurs

**Why leases?**: Read-heavy workloads (80%+ of traffic) don't pay Raft quorum cost. Writes still go through Raft for durability. This is the CockroachDB/Spanner model.

## Auto-Split and Auto-Merge

Ranges dynamically split and merge based on:

### Split Triggers

```rust
struct SplitPolicy {
    max_size: usize,             // 64MB default (CockroachDB uses 512MB)
    max_qps: f64,                // 1000 QPS default
    max_latency_p99: Duration,   // 10ms default
}
```

**Split algorithm**:
1. Monitor range metrics (size, QPS, latency)
2. When threshold exceeded, propose split at median key
3. Raft quorum approves split point
4. Create two new ranges: [start, split) and [split, end)
5. Update range directory (metadata service)

**Why split?**:
- **Size**: Large ranges slow down Raft snapshots and rebalancing
- **QPS**: Hot ranges bottleneck on single leaseholder CPU
- **Latency**: Large ranges increase scan time, delaying transactions

### Merge Triggers

```rust
struct MergePolicy {
    min_size: usize,             // 16MB default
    min_qps: f64,                // 10 QPS default
}
```

**Merge algorithm**:
1. Detect adjacent ranges both below thresholds
2. Propose merge to both Raft groups
3. Quorum approves on both sides
4. Combine into single range [start_left, end_right)
5. Update range directory

**Why merge?**: Too many small ranges waste memory (each Raft group has overhead) and increase metadata directory size.

## Hotspot Handling

### Workload-Aware Splits

Sequential writes (e.g., auto-incrementing IDs) concentrate on one range. Cloud9 detects and mitigates:

```rust
struct HotspotDetector {
    write_skew_threshold: f64,   // 0.8 = 80% of writes to 20% of keys
    auto_salt: bool,             // Enable subkey salting
}
```

**Auto-subkey salting**:
- Detect sequential write pattern (e.g., `user:00001`, `user:00002`, ...)
- Inject hash prefix: `hash(key) % N || key` → `3:user:00001`, `7:user:00002`, ...
- Writes distribute across N ranges
- Reads reconstruct via scatter-gather (map phase) + merge (reduce phase)

**Trade-off**: Point lookups become range scans. Only enable for known sequential append workloads (logs, time-series).

### Load-Based Splits

High QPS on single range triggers split even if size < max_size:

```rust
fn should_split_on_qps(range: &Range, stats: &RangeStats) -> bool {
    stats.qps > range.split_policy.max_qps
        && stats.write_skew > 0.5  // Writes not uniformly distributed
}
```

Split at the key separating high-write and low-write regions.

## Placement Policies and Zone Configs

Ranges have configurable replication and placement:

```rust
struct ZoneConfig {
    num_replicas: usize,         // 3 for distributed, 1 for local
    constraints: Vec<Constraint>,
}

enum Constraint {
    RequireRegion(String),       // "us-west-2"
    PreferZone(String),          // "us-west-2a" (soft constraint)
    ProhibitDatacenter(String),  // "dc-deprecated"
}
```

**Examples**:
- **Local mode**: `ZoneConfig { num_replicas: 1, constraints: [] }`
- **Multi-region**: `ZoneConfig { num_replicas: 5, constraints: [RequireRegion("us-east"), RequireRegion("us-west"), RequireRegion("eu-central")] }`
- **Compliance**: `ZoneConfig { num_replicas: 3, constraints: [ProhibitDatacenter("china"), RequireRegion("eu")] }`

Cloud9 uses CockroachDB's zone config DSL:

```sql
ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;
ALTER TABLE sensitive_data CONFIGURE ZONE USING constraints = '[+region=eu]';
```

## Interleaved Tables (Co-location)

Foreign key relationships benefit from co-location:

```sql
CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    ...
);

CREATE TABLE order_items (
    order_id UUID,
    item_id UUID,
    ...
    PRIMARY KEY (order_id, item_id),
    INTERLEAVE IN PARENT orders (order_id)
);
```

**Physical layout**:
```
orders:    [order:A:..., order:B:..., order:C:...]
           └─ order_items: [order:A:item:1, order:A:item:2]
                          [order:B:item:1, order:B:item:3]
                          [order:C:item:5]
```

Child rows stored adjacent to parent in same range. Joins and FK checks stay local (no cross-range RPC).

**Why this matters**: `DELETE FROM orders WHERE order_id = X` and cascading deletes to `order_items` happen in one range, one Raft transaction. Distributed FK checks are the #1 performance killer in CockroachDB—interleaving eliminates them.

## Range Directory (Metadata Service)

The range directory maps keys to ranges:

```rust
struct RangeDirectory {
    // Meta ranges store range metadata
    meta1: Range,                // Root range (never splits)
    meta2: Vec<Range>,           // Second-level index
    user_ranges: Vec<Range>,     // Actual data ranges
}
```

**Two-level hierarchy** (Bigtable/Spanner model):
1. **Meta1**: Single range storing Meta2 range locations (tiny, fits in memory)
2. **Meta2**: Ranges storing user range locations (sharded, but rarely accessed)
3. **User ranges**: Actual data

**Lookup algorithm**:
```rust
fn lookup_range(key: &Key) -> Range {
    // 1. Meta1 lookup (cached, O(1))
    let meta2_range = meta1_cache.lookup(key);

    // 2. Meta2 lookup (cached, O(1) amortized)
    let user_range = meta2_cache.lookup(meta2_range, key);

    // 3. Return user range
    user_range
}
```

**Cache invalidation**: Range splits/merges broadcast invalidation to all nodes. Stale cache causes misdirected RPC, which returns `RangeNotFound` + correct range hint.

## Local Mode Implementation

Local mode = 1 range, 1 replica, no replication:

```rust
struct LocalModeConfig {
    range: Range {
        range_id: 1,
        start_key: Key::MIN,
        end_key: Key::MAX,
        raft_group: 1,
        replicas: vec![replica_local],
        leaseholder: replica_local,
    },
    zone_config: ZoneConfig {
        num_replicas: 1,
        constraints: vec![],
    },
}
```

**Optimizations enabled**:
- Raft quorum size = 1 (no network, just WAL append)
- Leaseholder never transfers (only one replica)
- Range never splits (unless user explicitly configures split policy)
- Metadata directory fits in memory (1 entry)

**Result**: Local mode has zero distributed systems overhead. It's Postgres-level performance with MVCC and versioned storage.

## Why This Solves Single-Node Performance

The insight: **local = distributed with N=1**.

Traditional databases have separate "embedded" and "clustered" modes (Cassandra, MongoDB). Cloud9 has one code path:
- Local: 1 range, 1 Raft group, 1 replica
- Distributed: N ranges, N Raft groups, 3+ replicas each

Raft with quorum=1 is just a write-ahead log. Range directory with 1 range is just a pointer. Leaseholder with 1 replica is just "this node."

**No special cases. No mode switching. The same binary scales from 1 node to 1000 nodes.**

## Comparison to Alternatives

### Hash-Based Sharding (DynamoDB, Cassandra)

- Keys hashed to partitions (e.g., `hash(key) % N`)
- **Problem**: Range scans impossible (keys scattered across partitions)
- **Problem**: Can't interleave related data (parent/child separated by hash)
- Rejected: SQL requires range scans for `ORDER BY`, `BETWEEN`, secondary indexes

### Directory-Based Sharding (MongoDB)

- Config server stores key → shard mapping
- **Problem**: Config server is SPOF (though replicated, it's still a bottleneck)
- **Problem**: Balancer moves entire chunks (10s of MB), causing thundering herd
- Rejected: Cloud9 rebalances at replica level (Raft snapshots), not chunk level

### Consistent Hashing (Riak, Dynamo)

- Keys map to ring positions, replicas at ring offsets
- **Problem**: No range scans
- **Problem**: Range splits require full rehash
- Rejected: Same as hash-based sharding

**Verdict**: Range sharding is the only approach that supports:
- Range scans (required for SQL)
- Co-location (required for FK performance)
- Fine-grained splits (required for hotspot handling)
- Local mode (1 range = entire keyspace)

Every SQL-compatible distributed database (Spanner, CockroachDB, TiDB, YugabyteDB) uses range sharding. Cloud9 follows this proven path.

## Implementation Checklist

- [ ] Range struct with Raft group mapping
- [ ] Leaseholder acquisition/transfer protocol
- [ ] Split/merge triggers and policies
- [ ] Range directory (Meta1/Meta2 hierarchy)
- [ ] Cache invalidation protocol
- [ ] Interleaved table support (physical key encoding)
- [ ] Zone config DSL and replication constraints
- [ ] Hotspot detector (write skew, auto-salting)
- [ ] Local mode optimization (quorum=1, no rebalancing)
- [ ] Metrics: range size, QPS, latency, write skew

## Key Insight

**Local mode is not a special case—it's the degenerate case of the general distributed model.** When N=1, Raft becomes a WAL, leaseholder becomes "local replica," range directory becomes a singleton. This means Cloud9 can optimize aggressively for single-node (no network calls, no coordination) while using the exact same code path as distributed mode.

This is the design that should have existed from the start: one binary, one model, scales from 1 to N nodes with zero architectural discontinuity.
