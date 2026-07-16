# Implementation Roadmap

Cloud9 ships by proving one correctness layer at a time. Later phases depend on
the invariants established earlier.

Dates are not part of this specification. A phase is complete when its exit
tests pass.

## Current Foundation

The repository currently contains:

- a pure Raft state machine;
- durable write-ahead log storage;
- a replicated key-value service;
- a process-level database interface;
- a Jepsen harness.

This foundation is not yet the complete database. MVCC, distributed
transactions, production bounded time, and the additional dialects remain
target work.

## Phase 1: Replication Kernel

Finish the smallest durable replicated state machine.

Deliver:

- snapshot installation and compaction;
- crash-safe log recovery;
- membership changes;
- deterministic application;
- explicit durability boundaries;
- metrics for term, commit index, apply index, and storage.

Exit tests:

- Raft model tests;
- crash and torn-write recovery;
- snapshot and log-prefix replacement;
- repeated leader changes;
- Jepsen linearizable register and key-value workloads.

## Phase 2: Bounded Time

Implement the `TimeSource` contract before transaction timestamps depend on it.

Deliver:

- typed time intervals and provider status;
- uncertainty policy;
- commit-wait primitive;
- deterministic mock provider;
- AWS ClockBound backend;
- startup and runtime capability checks.

Exit tests:

- interval contract and arithmetic;
- provider loss and recovery;
- excessive uncertainty rejection;
- suspend, clock-step, and leap-state handling;
- hardware integration on supported EC2;
- no silent consistency-mode transition.

## Phase 3: MVCC and Local Transactions

Build transaction semantics on one replicated range.

Deliver:

- versioned keys;
- snapshots and garbage collection;
- read-write conflict detection;
- atomic batches;
- retry identity;
- timestamp assignment and commit-wait;
- serializable local mode.

Exit tests:

- model-based transaction histories;
- write-write and read-write conflicts;
- crash recovery at every commit boundary;
- retry idempotency;
- snapshot retention and garbage collection;
- strict serializability with the bounded-time provider.

## Phase 4: Ranges and Distributed Transactions

Partition the database without changing transaction semantics.

Deliver:

- range metadata and routing;
- split, merge, and replica movement;
- distributed transaction records;
- two-phase commit across ranges;
- durable recovery of coordinator failure;
- safe-time tracking for follower reads.

Exit tests:

- split and merge under load;
- participant and coordinator failure;
- ambiguous client outcomes;
- leader change during prepare and commit-wait;
- cross-range Jepsen transactions;
- locality and residency policy enforcement.

## Phase 5: Database IR

Establish the multi-level lowering pipeline.

Deliver:

- versioned IR containers;
- types and effect declarations;
- Transaction IR;
- Placement IR;
- physical dialect interfaces;
- lowering legality checks;
- deterministic replication commands;
- plan tracing and validation.

Exit tests:

- legal and illegal conversion suites;
- optimizer equivalence tests;
- serialized IR compatibility;
- deterministic command generation;
- invariant validation after every pass.

## Phase 6: SQL and Key-Value Dialects

Prove the IR with relational and key-value workloads.

Deliver:

- a SQL parser, catalog, planner, and wire protocol;
- DynamoDB-style item and conditional operations;
- row and point physical dialects;
- secondary indexes;
- explicit cross-dialect mappings;
- compatibility error models.

Exit tests:

- SQL logic and transaction suites;
- key-value compatibility tests;
- differential tests against reference systems;
- cross-dialect transaction histories;
- row and point-operation benchmarks.

## Phase 7: Document and Object Dialects

Add document and object semantics without routing them through relational
compatibility layers.

Deliver:

- document paths, queries, updates, and indexes;
- object metadata, versions, ranges, and multipart state;
- document and object-extent physical dialects;
- transactional metadata and projection rules;
- garbage collection for versions and extents.

Exit tests:

- reference compatibility suites;
- versioning and conditional request histories;
- multipart recovery;
- range-read correctness;
- transactional cross-dialect mappings;
- document and object workload benchmarks.

## Phase 8: Analytical Dialect

Add locality-optimized analytical execution.

Deliver:

- analytical logical plans;
- columnar physical storage;
- vectorized operators;
- projection freshness rules;
- distributed scans and exchanges;
- cost-based physical selection.

Exit tests:

- query correctness against a reference engine;
- snapshot consistency during ingestion;
- projection recovery and rebuild;
- optimizer equivalence;
- named analytical benchmarks with full configuration.

## Phase 9: Planetary Operations

Make placement and failure domains first-class.

Deliver:

- multi-region placement policy;
- online rebalancing;
- regional failure handling;
- backup and point-in-time recovery;
- admission control and workload isolation;
- upgrade and downgrade protocols;
- operational compatibility checks.

Exit tests:

- region-loss exercises;
- long-running Jepsen campaigns;
- restore and disaster-recovery drills;
- mixed-version operation;
- residency policy audits;
- sustained workload benchmarks.

## Release Gates

Every phase must provide:

1. A written invariant.
2. A test that fails without the implementation.
3. Fault injection at the durability boundary.
4. Metrics that reveal invariant health.
5. A recovery procedure.
6. Reproducible performance results for performance claims.

Cloud9 does not label target behavior as implemented. It does not trade a
documented consistency guarantee for availability without an explicit mode
change.
