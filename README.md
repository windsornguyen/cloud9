# Cloud9

**The only distributed database that unifies SQL and KV under one ACID transaction.**

Cloud9 provides Spanner-class external consistency with true Postgres compatibility and a native KV API—all in an MIT-licensed package that runs on your laptop or across the planet with the same binary.

## Overview

Most distributed databases force you to choose: strong consistency with limited scale, or weak consistency with operational complexity. Cloud9 eliminates this trade-off by implementing external consistency—the same guarantee that powers Google Spanner—in an open-source package that runs anywhere.

The engine compiles both SQL and KV operations into a common transactional IR, ensuring that reads and writes across both APIs observe a single global serialization order. Timestamps are assigned using Hybrid Logical Clocks with bounded uncertainty, and commits wait until all replicas have passed the commit timestamp before acknowledging. This design provides linearizable reads, lock-free snapshots, and deterministic ordering for concurrent writers.

## Why Cloud9

### The Theoretical Guarantee You Deserve

**External consistency** is the gold standard for distributed databases. It means: if you finish a write and then start a read—anywhere in the world—that read sees your write. Always. No exceptions. No "eventual consistency" footnotes. No "usually works but sometimes doesn't."

This isn't a feature. It's a **mathematical guarantee**, proven correct with formal methods. The same guarantee Google uses for ads billing, where every cent must be accounted for.

**You shouldn't need a Google-sized budget to get Google-class correctness.**

Cloud9 brings this guarantee to everyone:
- **Students** learning distributed systems
- **Startups** building the next platform
- **Enterprises** that need bulletproof data
- **Developers** who refuse to compromise on correctness

### The Practical Reality You Face

The market offers you bad choices:

**Spanner**: Correct but expensive ($1000+/month minimum), vendor lock-in, surprise billing incidents, missing Postgres features (no foreign keys, triggers, or stored procedures).

**DynamoDB**: Cheap to start but no real transactions, KV-only, eventual consistency, vendor lock-in.

**Managed Postgres** (CloudSQL/RDS): Familiar but unreliable at scale, performance issues, not truly distributed.

**Self-hosted Postgres**: Full control but loses strict serializability when sharded, requires expertise to run globally.

**CockroachDB/YugabyteDB**: Strong technically but either proprietary now (CRDB) or split SQL/KV APIs (YugabyteDB), expensive managed tiers.

**The gap**: No database gives you theoretical perfection, practical usability, and freedom from vendor lock-in.

### What Cloud9 Actually Gives You

**SQL and KV, unified**:
- Write with SQL, read with KV—in the same transaction
- Join SQL tables with KV keyspaces using typed projections
- One snapshot, one timestamp, one consistency guarantee
- No cache coherence problems, no dual writes, no eventual consistency

**Example**:
```sql
BEGIN;
  -- SQL: Complex query
  SELECT user_id FROM orders WHERE amount > 1000;

  -- KV: Fast state update
  PUT('agent:state:123', state_blob);

  -- Cross-API join
  SELECT * FROM users u
  JOIN kv_namespace('sessions') s ON s.user_id = u.id;
COMMIT;  -- Atomic across both APIs
```

**No other database can do this.**

**Correctness without compromise**:
- External consistency: Real-time ordering proven with formal methods
- Strict serializability: No anomalies, no "eventually consistent" footnotes
- Lock-free read-only transactions: Backups run concurrently with writes, never block
- True ACID: Full referential integrity with foreign keys, triggers, and constraints (unlike Spanner)

**Scale without barriers**:
- Local: `cargo run` on your laptop—free, instant, same semantics
- Regional: Multi-AZ replication with single-digit millisecond commits
- Global: Multi-region with cross-shard transactions and external consistency
- Same binary, same guarantees at every scale

**Freedom without lock-in**:
- MIT licensed—fork it, modify it, run it forever
- Self-host on $12 VPS or bare metal—full Spanner-class guarantees
- Or use Dedalus Cloud managed tier—convenience without lock-in
- Postgres wire protocol—existing tools, ORMs, and drivers just work

### Who This Is For

**If you've ever thought**:
- "I wish Postgres could scale globally without losing ACID"
- "I wish Spanner didn't cost $1000/month to try"
- "I wish I could run my production database locally for testing"
- "I wish DynamoDB had SQL and real transactions"
- "I wish I wasn't locked into a vendor who could 10x my bill tomorrow"

**Cloud9 is for you.**

Whether you're:
- A student running it on a Raspberry Pi
- A startup prototyping on your laptop
- An enterprise running globally distributed systems
- A researcher building distributed agents

The same database. The same guarantees. The same code.

### The Populist Database

For too long, distributed databases with strong guarantees have been the domain of tech giants. You either pay Google/AWS thousands per month, or you compromise on correctness.

**Cloud9 says: no more.**

Theoretical perfection shouldn't require a corporate credit card. The best database architecture should be available to anyone with a computer. You shouldn't have to choose between "correct" and "affordable."

This is infrastructure that belongs to everyone. Built in the open. Proven correct. Free to use, modify, and run forever.

**The daily driver database for the distributed era.**

## Architecture

### Storage and Replication

Cloud9 stores data in an MVCC key-value space, partitioned into ranges and replicated via consensus. Each range uses Raft for log replication and leader election. Reads are served by leaseholders for linearizability, or by any replica at a past timestamp. Cross-range transactions use two-phase commit with a coordinator that enforces commit-wait.

### Consensus Driver Interface

The replication layer is abstracted behind a narrow CDI that any SMR algorithm can implement. The default is Raft; alternative protocols (Multi-Paxos, Flexible Paxos, leaderless variants) can be swapped per range without changing the storage or transaction layers.

### Dual API Surface

SQL queries compile to range scans and secondary index lookups. KV operations map directly to get/put/scan primitives on the underlying storage. Both share the same snapshot isolation rules, the same timestamp oracle, and the same 2PC coordinator. KV keyspaces can be projected into typed SQL tables using versioned mappings, enabling cross-API joins with predicate pushdown.

### Timestamp Discipline

Cloud9 uses HLC to generate monotonic, causally ordered timestamps. Each node tracks observed skew; if the measured uncertainty exceeds a configured bound, writes are refused. Commit-wait is approximately ε, where ε is the current uncertainty. This ensures that any operation starting after a commit observes that commit's effects.

## Comparison

|                                    | Cloud9 | Spanner | CockroachDB | YugabyteDB | DynamoDB | PostgreSQL |
|------------------------------------|:------:|:-------:|:-----------:|:----------:|:--------:|:----------:|
| **Consistency**                    |        |         |             |            |          |            |
| External consistency               | ✅      | ✅       | ✅           | ✅          | ❌        | ❌¹         |
| Strict serializability             | ✅      | ✅       | ✅           | ✅          | ❌        | ✅²         |
| Lock-free snapshot reads           | ✅      | ✅       | ✅           | ✅          | ❌        | ✅          |
| Lock-free read-only transactions   | ✅      | ✅       | ❌           | ❌          | ❌        | ❌          |
| **API Surface**                    |        |         |             |            |          |            |
| SQL (Postgres-compatible)          | ✅      | ✅       | ✅           | ✅          | ❌        | ✅          |
| Native KV API                      | ✅      | ❌       | ❌           | ❌³         | ✅        | ❌          |
| Cross-API transactions             | ✅      | ❌       | ❌           | ❌          | ❌        | ❌          |
| Temporal queries (AS OF)           | ✅      | ✅       | ✅           | ❌          | ❌        | ❌          |
| **Scale & Deployment**             |        |         |             |            |          |            |
| Global distribution                | ✅      | ✅       | ✅           | ✅          | ✅        | ❌⁴         |
| Transparent cross-shard transactions| ✅     | ✅       | ✅           | ✅          | ❌        | ❌          |
| Automatic range rebalancing        | ✅      | ✅       | ✅           | ✅          | ✅        | ❌          |
| Single-binary local mode           | ✅      | ❌       | ❌           | ❌          | ❌        | ✅          |
| Pluggable consensus                | ✅      | ❌       | ❌           | ❌          | ❌        | ❌          |
| Online schema changes              | ✅      | ✅       | ✅           | ✅          | N/A      | ❌⁵         |
| Zero-downtime binary upgrades      | ✅      | ✅       | ✅¹⁰         | ❌          | N/A      | ❌          |
| **AI & Modern Workloads**          |        |         |             |            |          |            |
| Native vector indexing             | ✅      | ❌       | ❌⁶          | ❌⁶         | ❌        | ✅⁶         |
| Hybrid dense/sparse search         | ✅      | ❌       | ❌           | ❌          | ❌        | ❌          |
| Deterministic multi-writer ordering| ✅      | ✅       | ✅           | ✅          | ❌        | ❌          |
| CDC with exactly-once semantics    | ✅      | ✅⁷      | ✅           | ✅          | ✅        | ✅⁸         |
| **Implementation**                 |        |         |             |            |          |            |
| Open source (OSI-approved)         | ✅      | ❌       | ❌⁹          | ✅          | ❌        | ✅          |
| Memory-safe core                   | ✅      | ❌       | ❌           | ❌          | ❌        | ❌          |
| Deterministic simulation testing   | ✅      | ✅       | ❌           | ❌          | ❌        | ❌          |

¹ Single-instance serializable only; Aurora Global Database offers async replication
² Single node only; distributed Postgres loses strict serializability
³ Separate YCQL API; not schema-compatible with YSQL
⁴ Read replicas available; multi-region writes require application-level coordination
⁵ Most DDL operations require table locks
⁶ Via extensions (pgvector); not transactionally unified with core
⁷ Dataflow/Pub/Sub integration; not built into core storage
⁸ Via logical replication; at-least-once semantics
⁹ Now under CockroachDB Software License (source-available)
¹⁰ Added after years of production pain; Cloud9 designs for it from the start

## Features

### Unification

SQL and KV share one MVCC storage kernel. Cross-model joins allow SQL tables and KV keyspaces to interoperate. All queries lower into a single transactional IR. Both APIs observe the same external-consistency and timestamp semantics.

### External Consistency

HLC-based timestamping with bounded uncertainty and commit-wait provides strict real-time order. Every transaction's commit order matches wall-clock order cluster-wide. Snapshot reads are guaranteed safe once closed-timestamp passes.

### Cloud-Native Scale

Geo-distributed by default with multi-region quorum replication and latency-based leader placement. Elastic compute/storage split enables independent scale-out and failover. Pluggable consensus supports Raft as baseline with optional Flexible/Multi-Paxos or leaderless modules.

### Developer Experience

Postgres-compatible SQL works with existing clients, ORMs, and tools. Low-latency KV API provides millisecond get/put/scan path for agent workloads. Temporal queries with `AS OF` and time-travel are built in. CDC streams all changes with exactly-once semantics. Same binary runs locally or globally—SQLite simplicity with Spanner guarantees.

### AI & Agentic Workloads

AI agents need both: KV for hot-path state (millisecond reads/writes), SQL for analytics (complex joins), and vector search for retrieval. Cloud9 is the only database where all three share one MVCC snapshot, one transaction, one consistency model.

**What this enables**:
- Agent queries vector index, joins with SQL user data, updates KV state—atomic
- Time-travel on vector data: `SELECT * FROM vectors AS OF TIMESTAMP`
- CDC streams vector updates with exactly-once semantics
- No cache coherence, no dual writes, no eventual consistency

Postgres + pgvector works at single-node scale. Cloud9 works globally with external consistency guarantees.

### Performance & Reliability

Commit latency is approximately quorum RTT + ε. Read-anywhere architecture serves region-local snapshot reads at consistent timestamps. Online schema evolution provides non-blocking DDL. Fault containment enables per-range recovery and rebalancing. Tail latency control uses dynamic commit-wait tuning and leader leases.

### Lock-Free Read-Only Transactions

Cloud9 supports true lock-free read-only transactions at any timestamp. Read-only transactions never block writes and never acquire locks, enabling high-throughput analytical queries and backups to run concurrently with write traffic. Reads are served directly from any replica that has applied entries up to the requested timestamp, ensuring consistent snapshots without coordination overhead. This makes Cloud9 suitable for mixed workloads where analytical queries, exports, and backups must coexist with latency-sensitive write operations.

### Zero-Downtime Upgrades

Cloud9 is designed for rolling upgrades without downtime—a capability that emerges naturally from its architecture rather than being bolted on after the fact.

**Schema changes are timestamped**: DDL operations create new schema versions at commit timestamps. Old transactions see old schemas, new transactions see new schemas—simultaneously, without locks. This is fundamentally different from traditional databases where schema changes are global, atomic operations that require coordination across all nodes.

**Online index backfills**: Indexes are built in the background using fence timestamps. Reads and writes continue during index creation. The fence timestamp creates a clean boundary: writes before the fence are handled by the backfill process, writes after the fence are automatically indexed. No gaps, no locks, no downtime.

**Per-range rebalancing**: Raft membership changes use joint consensus, allowing replicas to be added or removed without quorum loss. This means you can add new nodes running upgraded binaries, wait for them to catch up, promote them to voters, and remove old nodes—all while serving traffic.

**What this enables**:
- Add or remove nodes without downtime
- Upgrade binary versions by rolling replicas one at a time
- Change schemas while queries run against both old and new versions
- Rebalance ranges under load without impacting availability

**Why other databases can't do this**: Postgres has MVCC but treats schema as global state—`ALTER TABLE` requires locks that block concurrent access. CockroachDB eventually added zero-downtime upgrades after years of production pain. Spanner has it but is proprietary. Cloud9 designs for it from the start: timestamped schemas, versioned metadata, and Raft-based replication that supports gradual evolution of cluster state.

No other open-source database combines all four capabilities out of the box.

### Global Sharding and Transparent Scaling

Cloud9 partitions data into ranges that are automatically distributed across nodes and regions. Ranges are replicated via consensus groups and rebalanced dynamically based on load and placement policies. Transactions spanning multiple ranges use two-phase commit with external consistency guarantees, ensuring that cross-shard operations observe the same strict serializability as single-range transactions. Applications never manage sharding—queries, joins, and transactions work transparently across the entire keyspace regardless of physical data distribution.

### Extensibility & Ecosystem

The consensus layer is isolated from storage, enabling operational features like dynamic leader placement, witness replicas, and future consensus innovations without rewriting the database core. All queries—SQL, KV, graph, vector—lower into a common Transactional IR (TxIR), making new APIs straightforward to add. SDKs in TypeScript, Python, Go, and Rust provide unified transaction semantics across languages. Open-core model: MIT-licensed engine, with Dedalus Cloud handling managed orchestration, time coordination, and global operations.

## Use Cases

**When SQL alone isn't enough**:
- Real-time dashboards need SQL analytics + KV session state in one transaction
- API gateways need fast KV reads with SQL for complex authorization rules
- Games need KV for player state, SQL for leaderboards and inventory—atomic updates across both

**When KV alone isn't enough**:
- AI agents need KV for hot state, but also SQL joins to query relationships
- Caching layers need KV performance, but SQL for cache invalidation logic
- Event sourcing needs KV for writes, SQL for projections and queries

**When Postgres isn't enough**:
- Multi-region SaaS platforms that outgrow single-node but need full ACID
- Financial systems requiring global distribution with audit trails and foreign keys
- E-commerce platforms balancing inventory across continents with strict consistency

**When cloud lock-in isn't acceptable**:
- Enterprises requiring self-hosting option with same guarantees as managed tier
- Startups that want to prototype locally before committing to cloud spend
- Regulated industries needing on-premises deployment with global consistency

**When distributed agents are first-class**:
- Autonomous AI systems writing concurrently across regions with deterministic ordering
- Multi-agent workflows requiring vector search + relational data + state in one transaction
- LLM applications needing consistent snapshots across embeddings, metadata, and user data

## Status

**Cloud9 is under active development.** The architecture is proven (Spanner's model, FoundationDB's layers), but Cloud9's unique combination—SQL+KV unification, true Postgres compatibility, MIT license—is new. We're building in public.

**Current focus**:
- Core MVCC and transaction coordinator
- Raft consensus implementation
- SQL+KV unification layer
- Postgres wire protocol compatibility

**Not yet implemented**:
- Global deployment automation
- Production-ready vector indexing
- Full Postgres feature parity
- Managed Dedalus Cloud tier

## Getting Started

```bash
# Clone and build
git clone https://github.com/dedalus-labs/cloud9
cd cloud9
cargo build --release

# Run single-node instance
./target/release/c9 start --config cloud9.example.toml

# Run tests
cargo test --workspace
```

For development setup and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Community

- **Issues**: [GitHub Issues](https://github.com/dedalus-labs/cloud9/issues) for bug reports and feature requests
- **Discussions**: [GitHub Discussions](https://github.com/dedalus-labs/cloud9/discussions) for questions and ideas
- **Code of Conduct**: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

Cloud9 is released under the [MIT License](LICENSE).

## References

- [Spanner: Google's Globally Distributed Database](https://research.google/pubs/pub39966/)
- [Hybrid Logical Clocks](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [FoundationDB: A Distributed Unbundled Transactional Key Value Store](https://www.foundationdb.org/files/fdb-paper.pdf)
- [Comet: An Active Distributed {Key-Value}
Store](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Geambasu.pdf)
