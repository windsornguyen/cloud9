# MVCC (Multi-Version Concurrency Control)

**Question**: How do we support lock-free read-only transactions and backups without blocking writes?

**Answer**: Multi-Version Concurrency Control (MVCC).

## The Core Idea

- Each write transaction T_w gets a commit timestamp t_w
- Values are versioned with their write timestamp (not overwritten)
- Each read-only transaction T_r picks a snapshot timestamp t_r
- T_r observes the most recent committed version with t_w ≤ t_r
- Writes with t_w > t_r are invisible to T_r

**Result**: Readers and writers operate on different versions. No locks, no blocking.

## Why This Is Natural

MVCC models how time actually works: the past is immutable, observers can choose which moment to examine. A backup reading at timestamp t_r sees a consistent point-in-time snapshot while new writes (t_w > t_r) continue.

## Alternatives Considered

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
