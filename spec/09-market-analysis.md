# Product Rationale

Cloud9 addresses fragmentation between database interfaces, execution engines,
and deployment models.

The product thesis is that one correctness plane can support several
specialized database domains when a multi-level IR preserves their semantics.

## The Problem

Teams commonly make coupled choices:

- SQL selects a relational engine.
- key-value APIs select a managed item store.
- document APIs select a document engine.
- object APIs select separate storage and metadata systems.
- analytical queries select a columnar engine and data pipeline.

Each system brings a different transaction model, identity model, operational
surface, and locality policy. Moving data between them weakens atomicity and
creates derived state that is difficult to inspect.

Distributed databases add another split. Local development often uses a
different engine from production. The application discovers semantic
differences during deployment.

## Cloud9's Position

Cloud9 combines:

- Spanner-style external consistency;
- SQLite-like local operation;
- SQL, key-value, document, object, and analytical dialects;
- domain-specific physical engines;
- one transaction, catalog, placement, and replication plane;
- an MLIR-style lowering and optimization system.

The differentiator is the conversion architecture. Cloud9 does not expose five
protocols over one generic row or key-value engine.

## Product Requirements

### Stable semantics

Changing topology must not change the data model. A local database and a
distributed database use the same source interfaces and transaction semantics.

Capability-dependent guarantees remain explicit. Local mode does not claim
hardware-backed TrueTime.

### Inspectable lowering

Users can inspect how a source request became a transaction, placement plan,
and physical plan. Unsupported behavior fails at a named conversion boundary.

### Domain-specific performance

Point operations, document updates, object ranges, relational joins, and
columnar scans need different data structures.

Cloud9 keeps one correctness plane while allowing each workload to use an
appropriate physical engine.

### Locality as policy

Placement, residency, and replica proximity are part of the plan. They are not
after-the-fact infrastructure hints.

### Open operation

Cloud9 is open source and self-hostable. File formats, protocols, limits, and
failure modes are documented and testable.

## Competitive Categories

| Category | Strength | Cloud9 requirement |
|----------|----------|--------------------|
| Relational databases | SQL and mature transactions | Preserve relational semantics |
| Key-value stores | Predictable point operations | Match conditional and item behavior |
| Document databases | Flexible nested data | Preserve paths and update operators |
| Object stores | Durable large objects | Preserve versions, ranges, and metadata |
| Analytical databases | Columnar execution | Keep vectorized, locality-aware plans |
| Distributed SQL | Scale and transactions | Add explicit bounded time and placement |
| Embedded databases | Simple local use | Keep one binary and one directory |

Cloud9 must earn comparison with each category on its native workload.

## Non-Claims

The architecture does not prove:

- complete compatibility with any named service;
- better performance than every specialized system;
- planetary scale in the current implementation;
- TrueTime behavior on unsupported hardware;
- zero-cost transactions across incompatible physical engines.

Those are measured outcomes, not taglines.

## Validation

Product claims require:

- compatibility suites against reference systems;
- Jepsen histories for consistency;
- crash and recovery tests;
- local setup and migration tests;
- named benchmarks for each physical domain;
- multi-region fault exercises;
- traces that show selected lowerings and placement.

Every benchmark reports workload, dataset, hardware, topology, durability,
consistency, and software versions.

## Success

Cloud9 succeeds when an application can begin locally, retain its semantics at
distributed scale, and use specialized execution without assembling separate
databases and consistency layers.
