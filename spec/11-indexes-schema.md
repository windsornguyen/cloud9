# Catalogs, Schemas, and Projections

Cloud9 stores catalog state as versioned transactional metadata. A transaction
resolves data and metadata at one compatible snapshot.

Indexes are physical projections. They are not required to use one data
structure across every dialect.

## Catalog

The catalog records:

- namespaces and logical identities;
- source-dialect types and constraints;
- physical representations;
- indexes and projections;
- placement and residency policy;
- ownership and authorization;
- schema versions and compatibility;
- backfill and garbage-collection state.

Catalog mutations use the same transaction protocol as data mutations.

## Schema Snapshots

A transaction reads one catalog snapshot. Planning and execution use that
snapshot even when a newer schema commits concurrently.

Metadata changes have explicit validity timestamps. A node cannot plan with
metadata it cannot interpret.

Historical data is readable only when the required schema and physical decoder
remain available.

## Physical Projections

A logical dataset may have several representations:

- row storage for relational writes;
- point storage for key-value access;
- document indexes for nested paths;
- object metadata and extent indexes;
- columnar projections for analytical scans;
- vector indexes for approximate search.

The catalog marks each representation as authoritative or derived. Derived
state includes a freshness watermark and rebuild procedure.

Dropping the authoritative representation requires an explicit migration that
first establishes another authority.

## Index Semantics

An index descriptor states:

- source fields or expressions;
- key encoding and collation;
- uniqueness;
- null and missing-value behavior;
- predicate and partial-index rules;
- physical dialect;
- placement;
- lifecycle state;
- freshness watermark.

The optimizer may select an index only when its semantics cover the source
operation.

Unique constraints use transactional reservations or equivalent serializable
validation. A local existence check is insufficient for a global unique index.

## Online Backfill

An index or projection follows a fenced lifecycle:

```text
Declared -> Backfilling -> Validating -> Readable -> Retiring
```

Creation proceeds as follows:

1. Commit the descriptor with fence timestamp `t_fence`.
2. Make writes at or after `t_fence` maintain the new projection.
3. Scan the authoritative representation at `t_fence`.
4. Write missing projection entries idempotently.
5. Catch up through a recorded high-water mark.
6. Validate contents against the authoritative representation.
7. Commit the `Readable` state and publication timestamp.

Queries cannot use the projection before publication. A failed backfill stays
unreadable and resumes from durable checkpoints.

## Concurrent Writes

Backfill and foreground writes may race on one logical item. Projection writes
therefore carry source version information.

An older backfill result cannot replace an entry produced by a newer
transaction. Deletes create projection tombstones where needed.

## Schema Changes

Schema changes fall into three classes:

- metadata-only changes;
- changes that require validation;
- changes that require a physical rewrite.

The planner declares the class before execution.

Adding a nullable field may be metadata-only. Adding a validated constraint
requires a scan. Changing an incompatible physical type requires a new
representation and migration.

Cloud9 does not label a rewrite as metadata-only to avoid operational cost.

## Cross-Dialect Mappings

A mapping between SQL, key-value, document, object, or analytical dialects
defines:

- shared logical identity;
- type conversion;
- null, missing, and default behavior;
- version and conditional-write semantics;
- authoritative representation;
- projection freshness;
- unsupported source operations.

Mappings are versioned catalog objects. A conversion that loses required
semantics is illegal.

## Vector Indexes

Vector indexes are physical projections with declared distance metrics and
recall behavior. Approximate search is valid only when the source operation
permits approximation.

The descriptor records algorithm parameters, training data version, and
freshness. Rebuilds publish through the same fenced lifecycle.

## Cache Invalidation

Nodes cache catalog and plan state by version. A transaction pins the version
it planned against.

New metadata invalidates future planning. It does not mutate an in-flight plan.
Nodes that cannot read the required catalog version become unready for that
operation.

## Garbage Collection

Retired metadata and physical state remain until:

1. no active transaction can reference them;
2. retention and backup policy permits deletion;
3. all readers support the successor format;
4. rollback is no longer allowed;
5. dependent projections no longer refer to them.

Deletion progress is durable and observable.

## Tests

Catalog and projection tests cover:

- schema snapshot consistency;
- legal and illegal type changes;
- unique conflicts across ranges;
- backfill races with inserts, updates, and deletes;
- crash recovery in every lifecycle state;
- stale projection rejection;
- cross-dialect mapping semantics;
- vector freshness and declared approximation;
- mixed-version metadata readers;
- garbage collection with active historical snapshots.
