# Cloud9 Vision

Cloud9 is an open-source Spanner and MLIR for databases.

It combines one distributed correctness plane with several database dialects
and physical engines. The same database can serve relational, key-value,
document, object, and analytical workloads without reducing every workload to
one physical model.

## Product Contract

Cloud9 targets two deployment extremes:

- Local development should feel like SQLite.
- Distributed deployment should scale across regions and continents.

The data model and transaction interfaces stay stable between them. Hardware
capabilities may differ. In particular, local mode does not claim
hardware-backed TrueTime.

## Database Dialects

Cloud9 treats APIs as source dialects:

- SQL for relational workloads.
- DynamoDB-style key-value operations.
- MongoDB-style document operations.
- S3-style object operations.
- ClickHouse-style analytical plans.

Each dialect preserves its source semantics. Compatibility is not an HTTP skin
over a generic row store.

## Multi-Level IR

Cloud9 uses several typed intermediate representations (IRs). This follows the
same principle as MLIR: preserve high-level meaning until a lower level can
represent it without loss.

Surface dialects lower into semantic IRs. Semantic IRs lower into transaction,
time, placement, and physical IRs. Physical IRs select row, key-value,
document, object, or columnar execution.

The shared layers own correctness. Specialized layers own performance.

## Core Guarantees

Cloud9 is designed around:

- atomic transactions across compatible dialects;
- multi-version concurrency control (MVCC);
- Raft-replicated state machines;
- explicit locality and placement;
- external consistency when bounded-time hardware is available;
- fail-closed behavior when a required invariant cannot be proven.

External consistency means real-time order constrains transaction order. If one
transaction finishes before another starts, the first must appear earlier.

## Bounded Time

Cloud9 exposes a TrueTime-shaped interval API:

```text
now() -> [earliest, latest]
```

The interval must contain real time. A valid bound permits commit-wait and safe
ordering across machines.

TrueTime mode requires a supported bounded-time source. The first target is AWS
ClockBound on supported Linux EC2 hardware.

Cloud9 refuses TrueTime mode when the provider is absent, unhealthy, or outside
the configured uncertainty bound. There is no silent fallback to Hybrid
Logical Clocks (HLCs).

## Performance Contract

Cloud9 targets specialized-database performance by preserving specialization. A
columnar analytical engine should not execute through a row-oriented hot path.
An object read should not become a document query.

Performance claims require reproducible benchmarks. Each result must state the
workload, topology, hardware, durability, and consistency mode.

## Current Boundary

The repository is an implementation in progress. Current code includes Raft,
durable log storage, replicated key-value operations, and Jepsen tests. The
remaining dialects, MVCC, distributed transactions, physical engines, and
bounded-time integration are target architecture.
