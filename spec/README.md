# Cloud9 Specifications

This directory contains the complete technical specification for Cloud9, the distributed database that should have existed from the start. These documents explain design decisions, technical foundations, and implementation strategies.

## Reading Guide

### For Newcomers: Start Here

1. **[00-vision.md](00-vision.md)** - Understand why Cloud9 exists and what makes it unique
2. **[01-mvcc.md](01-mvcc.md)** - Core concurrency control mechanism
3. **[02-timestamps.md](02-timestamps.md)** - How Cloud9 handles distributed time
4. **[03-external-consistency.md](03-external-consistency.md)** - The fundamental guarantee Cloud9 provides

After these four, choose your path based on interest.

### Quick Reference by Topic

**Understanding timestamps and time synchronization:**
- [02-timestamps.md](02-timestamps.md) - Timestamp strategies (HLC, TrueTime, TSO)
- [03-external-consistency.md](03-external-consistency.md) - Why commit-wait is necessary
- [04-truetime-analysis.md](04-truetime-analysis.md) - Mathematical foundations of bounded clock uncertainty
- [05-aws-time-infrastructure.md](05-aws-time-infrastructure.md) - Practical deployment on AWS infrastructure

**Understanding transactions and consistency:**
- [01-mvcc.md](01-mvcc.md) - Multi-version concurrency control
- [03-external-consistency.md](03-external-consistency.md) - External consistency guarantees
- [08-transactions.md](08-transactions.md) - Complete transaction protocol (2PC, intents, commit-wait)

**Understanding distributed architecture:**
- [06-sharding-partitioning.md](06-sharding-partitioning.md) - Range-based sharding and replication
- [10-consensus.md](10-consensus.md) - Raft consensus and operational features
- [08-transactions.md](08-transactions.md) - Cross-shard transaction coordination

**Understanding data model and APIs:**
- [07-sql-kv-unification.md](07-sql-kv-unification.md) - How SQL and KV share one transactional core

**Understanding the market context:**
- [09-market-analysis.md](09-market-analysis.md) - Why Cloud9 exists, pain points with existing solutions

## Specification Index

### Foundations

#### [00-vision.md](00-vision.md)
Cloud9's core philosophy and goals. Why external consistency matters, what makes Cloud9 unique, and who it's for. Read this first to understand the "why" before diving into the "how."

**Key concepts:** External consistency guarantee, Spanner+Postgres+FoundationDB synthesis, no compromises philosophy, daily driver database

#### [01-mvcc.md](01-mvcc.md)
Multi-Version Concurrency Control (MVCC) enables lock-free read-only transactions and backups without blocking writes. Every write gets a commit timestamp, readers choose a snapshot timestamp and see a consistent point-in-time view.

**Key concepts:** Versioned storage, snapshot isolation, lock-free reads, temporal queries

**Why alternatives don't work:** Two-phase locking blocks reads, optimistic concurrency causes high abort rates, timestamp ordering without versions can't support historical reads

#### [02-timestamps.md](02-timestamps.md)
Distributed timestamp strategies for external consistency. Compares Hybrid Logical Clocks (HLC), TrueTime, and Timestamp Oracle (TSO). Explains why Lamport clocks are insufficient for databases.

**Key concepts:** HLC with commit-wait (default), TSO mode (alternative), clock uncertainty bounds (epsilon), why commit-wait is unavoidable

**Cloud9's choice:** HLC for most deployments, TSO for unreliable clock sync environments, TrueTime-class performance available with GPS/atomic clocks

#### [03-external-consistency.md](03-external-consistency.md)
Formal definition and implementation of external consistency (strict serializability). If a write finishes before a read starts in real time, the read must see the write. Explains commit-wait protocol and why server-side timestamps are required.

**Key concepts:** Real-time ordering, commit-wait necessity, PACELC trade-offs, why client timestamps don't work

**Core protocol:** Assign commit timestamp, replicate via Raft, commit-wait until now() > t_commit + epsilon, acknowledge client

### Time Infrastructure

#### [04-truetime-analysis.md](04-truetime-analysis.md)
Mathematical foundations of TrueTime. Proves why bounded clock uncertainty is not heuristic but formally correct. Explains the 30-second sync interval, drift calculations, and failure modes.

**Key concepts:** Uncertainty formula (epsilon = sync_error + drift_rate × time), formal invariant proof, fail-safe behavior, why it's not guesswork

**Key insight:** TrueTime is proven mathematics based on hardware specifications, not empirical tuning. Cloud9 implements the same rigorous approach with different infrastructure.

#### [05-aws-time-infrastructure.md](05-aws-time-infrastructure.md)
Practical time synchronization options for Cloud9 on AWS. Covers four deployment tiers from standard NTP (50-100ms uncertainty) to GPS/atomic clocks (<1ms uncertainty).

**Deployment tiers:**
- **Standard:** NTP (50-100ms epsilon, zero setup)
- **Performance:** PTP/PHC (10-50ms epsilon, recommended default)
- **Premium:** AWS Outposts with GPS (1-10ms epsilon, hybrid deployment)
- **Premium+:** Colocation with GPS/atomic (< 1ms epsilon, Spanner-class)

**Key concepts:** Clock uncertainty measurement, chrony monitoring, fail-stop behavior, operational considerations

### Implementation

#### [06-sharding-partitioning.md](06-sharding-partitioning.md)
Range-based sharding with the degenerate case: local mode = 1 range, 1 replica. Explains leaseholder architecture, auto-split/merge policies, hotspot handling, and interleaved tables for foreign key performance.

**Key concepts:** Range = contiguous key interval, Raft group per range, leaseholder serves reads/writes, local mode is distributed with N=1

**Why range sharding:** Enables range scans (required for SQL), co-location of related data, fine-grained splits, and seamless local-to-distributed scaling

**Core insight:** Local deployment is not a special case - it's the degenerate case where the general distributed model has N=1

#### [07-sql-kv-unification.md](07-sql-kv-unification.md)
How SQL and KV coexist as the same system, not separate systems bolted together. SQL tables and KV namespaces are both key prefixes in a single MVCC storage layer. Enables cross-API transactions and joins.

**Key concepts:** Unified key encoding (/table/ vs /kv/), TxIR compilation target, schema-on-read for KV, cross-API joins with KV() virtual table

**Killer feature:** Begin transaction, write to SQL tables, read from KV namespaces, join SQL and KV data, commit atomically at one timestamp

**Why others failed:** FoundationDB had experimental SQL, YugabyteDB has separate systems (YSQL vs YCQL), Spanner is SQL-only, DynamoDB is KV-only

#### [08-transactions.md](08-transactions.md)
Complete transaction protocol: two-phase commit (2PC) with MVCC intents, coordinator-driven commit timestamp assignment, and external consistency via commit-wait. Covers read-only transactions, cross-shard writes, intent resolution, and recovery.

**Transaction types:** Read-only (lock-free, no 2PC), single-range writes (simplified 2PC), cross-shard writes (full 2PC)

**Key protocols:** Intent writing (phase 0), prepare phase (conflict detection), commit phase (intent resolution), commit-wait (external consistency)

**Advanced topics:** Closed timestamps for follower reads, intent cleanup, transaction recovery, timestamp caching, parallel commit optimization

#### [10-consensus.md](10-consensus.md)
Raft consensus as the replication foundation. Explains why Raft (not Paxos or novel algorithms), the clean consensus driver interface, and operational features: dynamic leader placement, witness replicas, learner replicas, per-range configuration.

**Core principle:** Ship one consensus algorithm done right. Clean separation between consensus (log replication) and state machine (MVCC storage).

**Operational features:** Leadership transfer (latency optimization), witnesses (storage cost reduction), learners (safe reconfiguration), per-range config (multi-tenant flexibility)

**Not resume padding:** The interface exists for testability and maintainability, not to ship multiple algorithms at launch

### Market Context

#### [09-market-analysis.md](09-market-analysis.md)
Why Cloud9 exists based on real user feedback from Spanner, DynamoDB, and competing systems. Documents pain points: cost models, vendor lock-in fears, support quality, documentation gaps, and billing disasters.

**Spanner problems:** High minimum cost ($65-1000+/month), GCP platform instability, poor support, hidden performance gotchas, fear of product cancellation

**DynamoDB problems:** KV-only limitations, capacity planning complexity, no multi-item transactions, hot partition issues

**Common theme:** Users fear vendor lock-in more than technical limitations. "Doing business with Google is a liability."

**The missing middle:** Gap between Postgres (single-node) and Spanner/DynamoDB (enterprise-only, vendor lock-in). Cloud9 fills this gap with open-source, scale-from-laptop-to-global deployment.

**Billing horror stories:** RAG Engine incident (silent $30-800/day charges), Gemini billing errors ($70k+ bills), CloudSQL performance forcing migration to self-hosted VMs

## Recommended Reading Paths

### Path 1: Understand the Core Guarantees
For those who want to understand what Cloud9 promises and how it delivers:

1. [00-vision.md](00-vision.md) - The promise
2. [03-external-consistency.md](03-external-consistency.md) - The formal guarantee
3. [02-timestamps.md](02-timestamps.md) - How time enables the guarantee
4. [08-transactions.md](08-transactions.md) - The complete protocol

### Path 2: Deployment and Operations
For those planning to deploy Cloud9:

1. [00-vision.md](00-vision.md) - What you're deploying
2. [05-aws-time-infrastructure.md](05-aws-time-infrastructure.md) - Time sync options and deployment tiers
3. [06-sharding-partitioning.md](06-sharding-partitioning.md) - Scaling from local to distributed
4. [10-consensus.md](10-consensus.md) - Operational control (leader placement, replicas)

### Path 3: Implementation Study
For those building Cloud9 or similar systems:

1. [01-mvcc.md](01-mvcc.md) - Storage foundation
2. [02-timestamps.md](02-timestamps.md) - Timestamp strategies
3. [04-truetime-analysis.md](04-truetime-analysis.md) - Mathematical rigor
4. [08-transactions.md](08-transactions.md) - Transaction machinery
5. [06-sharding-partitioning.md](06-sharding-partitioning.md) - Distributed architecture
6. [10-consensus.md](10-consensus.md) - Replication layer
7. [07-sql-kv-unification.md](07-sql-kv-unification.md) - API unification

### Path 4: Market Positioning
For those evaluating Cloud9 vs alternatives:

1. [00-vision.md](00-vision.md) - Cloud9's positioning
2. [09-market-analysis.md](09-market-analysis.md) - Problems with existing solutions
3. [03-external-consistency.md](03-external-consistency.md) - The technical differentiator
4. [07-sql-kv-unification.md](07-sql-kv-unification.md) - Unique capabilities

## Design Principles

These specs embody Cloud9's core principles:

1. **No compromises:** External consistency + SQL + KV + open source + local-to-global
2. **Proven foundations:** MVCC, Raft, HLC, commit-wait - nothing novel, everything battle-tested
3. **Clean architecture:** Consensus driver interface, TxIR compilation target, clear separation of concerns
4. **Operational flexibility:** Leader placement, witness replicas, per-range configuration
5. **Developer experience:** Postgres wire compatibility, start on laptop, deploy to cloud unchanged
6. **No vendor lock-in:** MIT license, self-hostable, community-driven

## What's Not Here

These specs intentionally omit:

- **Implementation details:** Code belongs in source files with inline documentation
- **API references:** Generated from source, not duplicated in specs
- **Benchmarks:** Belong in performance testing suite with reproducible methodology
- **Roadmap timelines:** Tracked in GitHub issues/projects, not static documents

These specs explain **design decisions, trade-offs, and foundations**. Implementation lives in code. Operations guides live in docs. Roadmap lives in project management.

## Contributing

Found an error? Have a clarifying question? Open an issue.

Want to propose a design change? Write a spec amendment following the existing format: clear problem statement, alternatives considered with rationale, concrete examples, trade-off analysis.

Specs are living documents. As Cloud9 evolves through production deployment, these specs will be updated to reflect reality, not aspirational design.

## Status

**Current:** Specification phase (2025-01). Implementation beginning.

**Stability:** Design is stable for foundational components (MVCC, timestamps, transactions, consensus). Market analysis reflects 2024-2025 feedback. AWS infrastructure options current as of 2025-01.

**Updates:** Specs will be versioned when implementation reveals necessary changes. No silent edits - all changes tracked via git history.
