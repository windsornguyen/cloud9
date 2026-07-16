# Cloud9

**An open-source Spanner and MLIR for databases.**

Cloud9 is a database compiler and distributed storage system. It accepts SQL,
key-value, document, object, and analytical workloads. It lowers each workload
through typed intermediate representations into a domain-specific execution
engine.

The target is broad: SQLite-like local development and planetary deployment
from one codebase. The semantics stay stable as the topology changes.

## Status

Cloud9 is under active development. The repository currently contains a pure
Raft state machine, a durable write-ahead log, a replicated key-value service,
and a Jepsen harness.

Multi-version concurrency control, distributed transactions, SQL, document,
object, analytical dialects, and bounded-time integration remain under
development. The specifications describe the target architecture. They are not
a claim that each feature is complete.

## One Database, Many Dialects

Cloud9 treats database APIs as source languages:

- SQL dialects provide relational queries and transactions.
- DynamoDB-style APIs provide key-value and conditional operations.
- MongoDB-style APIs provide document queries and updates.
- S3-style APIs provide objects, metadata, versions, and byte ranges.
- ClickHouse-style plans provide columnar analytical execution.

These APIs share identity, transactions, timestamps, placement, and
observability. They do not share one forced physical layout. A columnar scan
and an object read need different data structures.

## MLIR for Databases

[MLIR](https://mlir.llvm.org/) preserves domain information through multiple
intermediate representation levels. Cloud9 applies that design to databases.

```text
SQL | KV | document | object | analytical dialects
                         |
                 semantic dialect IRs
                         |
              transactional and time IR
                         |
              placement and physical IRs
                         |
       row | KV | document | object | columnar engines
                         |
                 MVCC | Raft | storage
```

Each source dialect keeps its semantics until a legal lowering exists. SQL
nullability, DynamoDB conditions, MongoDB updates, and S3 versioning must not
disappear into a generic key-value operation too early.

Lowering selects physical operators and data placement. Common passes can
enforce transactions, authorization, locality, and cost rules. Specialized
passes can select indexes, columnar scans, object extents, or point reads.

## Time and External Consistency

Cloud9 defines a TrueTime-shaped API:

```text
now() -> [earliest, latest]
```

The interval must contain real time. Cloud9 can use that bound with commit-wait
to provide external consistency, also called strict serializability.

TrueTime mode is capability-gated. It starts only when the host provides a
supported bounded-time source. The first production target is
[AWS ClockBound](https://github.com/aws/clock-bound) on supported Linux EC2
hardware with Amazon Time Sync configured.

Missing or unhealthy time support is an error. Cloud9 does not silently replace
it with a weaker clock.

Local mode does not require bounded-time hardware. It keeps the same data model
and transaction interfaces, but it does not claim hardware-backed TrueTime.

## Storage Architecture

The shared correctness plane owns:

- bounded time and commit-wait;
- multi-version concurrency control (MVCC);
- transaction coordination;
- schemas, catalogs, and object metadata;
- range placement and replica routing;
- Raft replication and recovery.

Physical engines own their data structures and execution paths. Data may have
several transactional projections, such as a row layout for writes and a
columnar layout for scans. The catalog records which representation is
authoritative and which projections may lag.

## Local to Planetary

Local Cloud9 should feel like SQLite: one binary, one directory, and no control
plane. A local database uses one range and one replica.

Distributed Cloud9 partitions data into replicated ranges. Placement follows
data locality and workload shape.

Cross-range writes use distributed transactions. The target is to scale the
same logical database from one laptop to clusters that span regions and
continents.

## Performance

Cloud9 targets the native performance envelope of specialized systems. This is
a benchmark requirement, not a blanket performance claim.

Every performance claim must name the workload, topology, durability mode,
consistency mode, and comparison system. Domain-specific storage and lowering
exist so the benchmark can improve without weakening the common correctness
contract.

## Build

```bash
git clone https://github.com/windsornguyen/cloud9
cd cloud9
cargo build --release
cargo test --workspace
```

Run the current replicated key-value node with:

```bash
./target/release/c9 start --config cloud9.example.toml
```

See [the specifications](spec/README.md) for the target design and
[the Jepsen harness](jepsen/README.md) for current distributed tests.

## License

Cloud9 is released under the [MIT License](LICENSE).

## References

- [Spanner](https://research.google/pubs/pub39966/)
- [MLIR dialect conversion](https://mlir.llvm.org/docs/DialectConversion/)
- [AWS ClockBound](https://github.com/aws/clock-bound)
- [Amazon Time Sync on EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html)
- [Raft](https://raft.github.io/)
