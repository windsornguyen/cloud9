# Multi-Version Concurrency Control

Cloud9 uses multi-version concurrency control (MVCC) for transactional
snapshots. Writes create versions instead of overwriting visible state.

## Visibility Rule

A committed version has a commit timestamp. A read at timestamp `t_read` sees
the newest committed version whose timestamp is at or before `t_read`.

```text
visible(key, t_read) =
    max(version.commit_time <= t_read)
```

Later versions are invisible. Tombstones are versions that hide earlier data.
Uncommitted intents are never returned as committed values.

## Transaction State

A write transaction may create provisional intents. Each intent names its
transaction and proposed value.

The durable transaction record is authoritative. Valid transitions are:

- `Pending -> Preparing`
- `Pending -> Aborted`
- `Preparing -> Committed`
- `Preparing -> Aborted`

`Committed` and `Aborted` are terminal. A durable commit decision cannot
become an abort. Replica recovery and client retries must reach the same
terminal result.

## Snapshots

A transaction uses one snapshot across all participating ranges and physical
engines. The snapshot includes compatible catalog and schema versions.

Read-only transactions do not create intents. They may execute without
blocking writes after Cloud9 proves that each participant can serve the chosen
snapshot.

Physical engines may encode versions differently. They must implement the same
visibility and transaction-state contract.

## Conflict Detection

Serializable read-write transactions declare or derive:

- point and range reads;
- point and range writes;
- predicates that affect the result;
- observed version timestamps.

Prepare validates that no conflicting committed version or intent invalidates
the snapshot. Predicate validation must detect phantoms.

Cloud9 aborts on an unresolvable conflict. It does not return a result from a
weaker isolation level.

## Garbage Collection

A version can be removed only when no valid reader, backup, change stream, or
recovery operation can still require it.

Each range tracks a garbage-collection watermark. Advancing it requires proof
that:

1. no active snapshot is older;
2. retention policy permits deletion;
3. dependent projections have advanced;
4. backups and recovery points no longer reference the version.

Compaction preserves the newest visible version before the watermark. Deleting
all older versions without that anchor can resurrect stale data.

## Long-Lived Reads

Long transactions and backups hold the watermark back. Cloud9 exposes their
age and storage cost.

Retention pressure may reject a new long-lived operation. It may not silently
delete versions still covered by the operation's snapshot.

## Schema and Catalog Versions

Catalog changes are versioned transactionally. A query resolves data, schema,
indexes, and projection metadata at one compatible snapshot.

An online schema change creates new metadata and may start a backfill. It does
not make the new representation readable until the catalog records that its
required snapshot is complete.

## Tests

MVCC tests cover:

- version visibility at exact timestamp boundaries;
- tombstones and resurrection prevention;
- intent visibility and terminal transaction states;
- point, range, and predicate conflicts;
- snapshot consistency across physical engines;
- garbage collection with active readers;
- crash recovery during commit and cleanup;
- schema and data snapshot alignment.
