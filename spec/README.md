# Cloud9 Specifications

Cloud9 is an open-source Spanner and MLIR for databases.

These documents define the target architecture. They distinguish implemented
behavior from planned behavior. The repository currently provides Raft, a
durable write-ahead log, replicated key-value operations, and Jepsen tests.

## Product Contract

Cloud9 combines one correctness plane with several database dialects and
physical engines:

- SQL for relational workloads.
- DynamoDB-style key-value operations.
- MongoDB-style document operations.
- S3-style object operations.
- ClickHouse-style analytical plans.

The dialects share transactions, timestamps, identity, placement, and
observability. They keep distinct semantics and physical layouts.

Cloud9 targets local development and planetary deployment. Local mode should
feel like SQLite. Distributed mode partitions data into Raft-replicated ranges.

## Architectural Decisions

### Multi-level database IR

Source APIs lower through typed intermediate representations (IRs). High-level
semantics remain visible until a legal lower level can preserve them. Physical
lowering may select row, key-value, document, object, or columnar execution.

See [07-sql-kv-unification.md](07-sql-kv-unification.md).

### Capability-gated TrueTime

Cloud9 exposes a TrueTime-shaped bounded-time API:

```text
now() -> [earliest, latest]
```

TrueTime mode starts only with a healthy, approved bounded-time provider. The
first production backend is AWS ClockBound on supported Linux EC2 hardware.
Cloud9 does not substitute a Hybrid Logical Clock when that provider fails.

Local mode has no bounded-time hardware requirement. It does not claim
hardware-backed TrueTime or cross-machine external consistency.

See [02-timestamps.md](02-timestamps.md),
[03-external-consistency.md](03-external-consistency.md), and
[05-aws-time-infrastructure.md](05-aws-time-infrastructure.md).

### Shared correctness, specialized execution

The shared plane owns transactions, multi-version concurrency control (MVCC),
replication, recovery, catalogs, and placement. Physical engines own their data
structures and hot paths.

Specialization is necessary for performance. It cannot weaken the shared
correctness contract.

## Reading Order

1. [Vision](00-vision.md)
2. [Multi-version concurrency control](01-mvcc.md)
3. [Timestamp model](02-timestamps.md)
4. [External consistency](03-external-consistency.md)
5. [Bounded-time analysis](04-truetime-analysis.md)
6. [AWS ClockBound backend](05-aws-time-infrastructure.md)
7. [Sharding and placement](06-sharding-partitioning.md)
8. [Multi-model intermediate representation](07-sql-kv-unification.md)
9. [Transaction protocol](08-transactions.md)
10. [Product rationale](09-market-analysis.md)
11. [Consensus and replication](10-consensus.md)
12. [Catalogs, schemas, and projections](11-indexes-schema.md)
13. [Implementation roadmap](12-implementation-roadmap.md)

## Specification Map

| File | Decision |
|------|----------|
| [00-vision.md](00-vision.md) | Product and consistency contract |
| [01-mvcc.md](01-mvcc.md) | Version visibility and retention |
| [02-timestamps.md](02-timestamps.md) | Bounded-time provider interface |
| [03-external-consistency.md](03-external-consistency.md) | Commit timestamp and commit-wait protocol |
| [04-truetime-analysis.md](04-truetime-analysis.md) | Proof obligations and uncertainty cost |
| [05-aws-time-infrastructure.md](05-aws-time-infrastructure.md) | ClockBound deployment requirements |
| [06-sharding-partitioning.md](06-sharding-partitioning.md) | Ranges, replicas, and locality |
| [07-sql-kv-unification.md](07-sql-kv-unification.md) | Database dialects and lowering pipeline |
| [08-transactions.md](08-transactions.md) | Single-range and distributed transactions |
| [09-market-analysis.md](09-market-analysis.md) | Product thesis and validation |
| [10-consensus.md](10-consensus.md) | Raft persistence and replication |
| [11-indexes-schema.md](11-indexes-schema.md) | Catalog, schema, index, and projection lifecycle |
| [12-implementation-roadmap.md](12-implementation-roadmap.md) | Dependency-ordered delivery plan |

## Status Language

Specifications use three status terms:

- **Implemented** means code and tests exist in this repository.
- **In progress** means an implementation exists but lacks a required proof.
- **Target** means architecture is specified but not implemented.

Performance goals are targets until a reproducible benchmark supports them.
Every result must name the workload, hardware, topology, durability mode, and
consistency mode.

## References

- [Spanner](https://research.google/pubs/pub39966/)
- [MLIR dialect conversion](https://mlir.llvm.org/docs/DialectConversion/)
- [AWS ClockBound](https://github.com/aws/clock-bound)
- [Amazon Time Sync on EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html)
- [Raft](https://raft.github.io/)
