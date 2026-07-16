# Multi-Model Intermediate Representation

Cloud9 is an MLIR for databases. Database APIs are source dialects that lower
through several typed intermediate representations (IRs).

The common layer is not one universal storage model. It is a conversion system
for preserving semantics while selecting transactions, placement, and physical
execution.

## Source Dialects

Cloud9 targets:

- SQL dialects for relational queries and transactions;
- DynamoDB-style key-value and conditional operations;
- MongoDB-style document queries and updates;
- S3-style objects, metadata, versions, and byte ranges;
- ClickHouse-style analytical plans.

Compatibility includes behavior, not only request syntax. Each frontend owns
its source types, error model, pagination, consistency options, and mutation
rules.

## Why Several IRs

Early lowering to generic key-value operations loses useful information:

- SQL predicates and nullability;
- key-value conditions and atomic counters;
- document paths and update operators;
- object versions, ranges, and multipart state;
- analytical projections, grouping, and ordering.

Cloud9 retains that information until a lower layer can represent it without
loss. This enables domain-specific optimization without duplicating
transactions and replication.

## IR Levels

### 1. Surface dialect IR

Each frontend parses requests into a typed semantic IR:

```text
SqlIR
KvIR
DocumentIR
ObjectIR
AnalyticalIR
```

These dialects describe source behavior. They are independent of network
encoding and physical storage.

### 2. Transaction IR

Transaction IR makes shared correctness explicit:

```text
Transaction {
    identity
    snapshot
    reads
    predicates
    mutations
    consistency
    authorization
}
```

Operations declare read sets, write sets, ranges, predicates, and effects.
Transactions also carry retry identity and required consistency.

This IR is the boundary for MVCC, conflict detection, atomic commit, and
bounded-time timestamp assignment.

### 3. Placement IR

Placement IR maps logical operations to ranges and replicas. It represents:

- partition keys and range boundaries;
- replica constraints;
- locality and residency policy;
- leaders and follower-read eligibility;
- data movement and repartitioning;
- cross-range transaction participants.

Placement decisions cannot alter source semantics.

### 4. Physical dialect IR

Physical dialects select data structures and operators:

```text
RowIR
PointIR
DocumentPhysicalIR
ObjectExtentIR
ColumnarIR
```

A SQL point lookup may lower to `PointIR`. An analytical scan may lower to
`ColumnarIR`. An object read may lower to extent and metadata operations.

Several physical projections may represent one logical dataset. The catalog
marks one representation authoritative and records freshness for derived
projections.

### 5. Replication IR

State-changing physical operations lower to deterministic commands before Raft
replication. A command includes all data needed for identical application on
every replica.

Replicated state machines do not call wall clocks, random generators, or
external services while applying a command.

## Legal Lowering

Every conversion declares which source operations it can preserve. A lowering
fails when the target cannot represent a required semantic.

Examples include:

- rejecting a document collation unsupported by the selected index;
- rejecting an object consistency mode unsupported by the target topology;
- preserving SQL null semantics through predicate lowering;
- retaining a key-value condition until conflict validation;
- retaining analytical ordering until a physical operator guarantees it.

Cloud9 does not approximate unsupported behavior.

## Cross-Dialect Data

Dialects may share data through an explicit catalog mapping. The mapping
defines identity, types, nullability, versioning, and ownership.

One physical representation may serve several dialects when their semantics
align. Otherwise Cloud9 maintains a transactional projection or rejects the
mapping.

Cross-dialect transactions use Transaction IR. Atomicity is available only
when every participating lowering supports the requested semantics.

## Example Lowerings

A conditional key-value write lowers as:

```text
KvIR conditional put
  -> Transaction IR predicate plus mutation
  -> PointIR read and versioned write
  -> deterministic Raft command
```

An object upload lowers as:

```text
ObjectIR put
  -> Transaction IR metadata mutation
  -> ObjectExtentIR data placement
  -> replicated version and extent metadata
```

An analytical query lowers as:

```text
AnalyticalIR scan, filter, aggregate
  -> snapshot and placement constraints
  -> ColumnarIR operators
  -> vectorized execution
```

These paths share catalogs, snapshots, and durability. They do not share one
forced hot path.

## Optimization Passes

Passes may:

- push predicates into compatible physical dialects;
- prune columns and object byte ranges;
- select indexes and projections;
- co-locate transaction participants;
- route safe reads to followers;
- fuse compatible physical operators;
- choose row, point, document, extent, or columnar execution.

Each pass must preserve types, effects, consistency, and authorization. The
validator rejects an IR that violates a declared invariant.

## Versioning and Observability

Serialized IR includes a version. Rolling upgrades accept only declared
version pairs.

Traces record the source operation, selected lowerings, placement decision, and
physical plan. Sensitive values may be redacted, but the decision path remains
inspectable.

This is the practical value of an MLIR design: operators can see where meaning
changed and where work was introduced.

## Test Contract

Each dialect needs:

- parser and type tests;
- source compatibility tests;
- legal and illegal lowering tests;
- differential tests against a reference system;
- optimization equivalence tests;
- deterministic replication tests;
- cross-dialect transaction tests;
- physical-engine benchmarks.

A performance result is valid only when the optimized and reference plans have
the same observable semantics.
