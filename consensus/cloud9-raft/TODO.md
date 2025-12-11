# Consensus TODO (toward production readiness)

A checklist of gaps to close before treating this Raft core as production-ready.

## Durability & Recovery
- Define WAL format and fsync semantics; crash/restart tests verifying term/vote/log invariants.
- Implement snapshotting + install-snapshot; integrate with log compaction and restart flow.
- Corruption detection/handling strategy (checksums, fail-fast, repair).

## API & Integration
- Network/storage traits with backpressure, timeouts, retry/backoff policy, and clear ordering/persist requirements.
- Hooks for metrics/tracing/logging (replication lag, election churn, snapshot progress).
- Config validation and operational guardrails (proposal size limits, membership change rate limits).

## Liveness & Efficiency
- Pre-vote and election tuning; joint config edge-case handling under partitions.
- Replication congestion control; efficient backoff/retry under rejections and slow followers.

## Deferred Dissertation Extensions
- **Lease-based reads (§6.4 alternative)**: Requires `realoj` crate for TrueTime-style intervals. Formula: `lease_end = heartbeat_ack_time + (election_timeout / clock_drift_bound)`. Faster than heartbeat quorum but assumes bounded clock drift.
- **Witnesses**: Vote in quorum but don't store data. Reduces storage cost while maintaining fault tolerance. Not in dissertation core but mentioned in related work.

## Testing
- Redundancy/efficiency integration tests (no duplicate vote requests, no redundant heartbeats/appends, bounded retries).
- Property/model-based testing; long-running stress/fuzz with packet loss/duplication/reordering/partitions.
- Persistence/restart loops and snapshot/install coverage; performance benchmarks.

## Code Quality
- Broader linting (clippy config with disallowed methods if desired).
- Eliminate remaining ad-hoc `unwrap`/`expect` in non-test code paths.
- Consistent result types across APIs; document invariants at API boundaries.

## Database/Service Integration
- Linearizability at the service boundary (read-index/lease semantics, fencing, duplicate suppression).
- Multi-raft/partitioning strategy and interaction with transaction layers (batching, idempotency).
- On-disk format evolution (WAL/snapshot versioning, migrations).
- Performance/SLO validation under realistic mixed workloads (latency/throughput/tail).
- Security: RPC authn/z, encryption in transit/at rest for WAL/snapshots, multi-tenant isolation.
- Operations: rolling upgrades/downgrades compatibility, safe tooling for config changes, health checks, auto-recovery procedures, alarms/metrics budgets (lag, election churn), capacity planning.
- Tooling/observability: rich tracing/metrics/logging with correlation IDs, admin/inspection APIs (dump log, force snapshot, replace nodes), chaos/soak testing in CI/CD.
- Disaster scenarios: behavior under prolonged partitions, split-brain prevention, recovery from majority loss (operator playbooks), and blast-radius analysis.
