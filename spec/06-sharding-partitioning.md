# Sharding and Placement

Cloud9 partitions physical dialects into ranges. Each range is one
Raft-replicated state machine with an explicit placement policy.

Local mode starts with one range and one replica. Distributed mode adds ranges
and replicas without changing transaction semantics.

## Range Descriptor

```text
Range {
    id
    physical_dialect
    start
    end
    raft_group
    replicas
    generation
    placement_policy
}
```

`[start, end)` is interpreted by the named physical dialect. A row-key span,
object-extent span, and columnar partition need not share one encoding.

For each dialect namespace, live range descriptors must be exhaustive and
non-overlapping.

## Replication

Each distributed range has an independent Raft group. A committed command is
applied in the same order on every replica.

Local mode uses a one-replica group. It may remove network hops, but it retains
the same log and state-machine invariants.

## Ownership and Fencing

One replica coordinates writes for a range. Ownership is fenced by the Raft
term and a monotonically increasing lease sequence.

```text
Lease {
    range_id
    replica_id
    raft_term
    sequence
}
```

Timeout measurement uses a monotonic clock. A bounded UTC interval is used only
when a lease protocol needs a real-time proof.

A stale owner cannot commit after a newer fence is durable. Reads require a
ReadIndex, a valid lease proof, or an applied safe-time proof.

## Splits and Merges

Ranges split when size, load, or recovery cost exceeds policy. A split:

1. chooses a boundary valid for the physical dialect;
2. records child descriptors and generations transactionally;
3. transfers state through snapshots or shared immutable files;
4. activates routing only after both children can serve;
5. retires the parent after stale requests are fenced.

Adjacent ranges may merge when their placement and physical formats are
compatible. The merge has the same atomic routing requirement.

Cloud9 does not silently rewrite keys to spread a hotspot. Salting or
repartitioning changes access behavior and requires an explicit schema or
placement decision.

## Range Directory

The range directory maps a physical-dialect key to its current range
descriptor. Directory records are versioned, replicated metadata.

Clients and nodes may cache records. A stale route returns the current
generation and destination. The caller retries the same idempotent operation.

Directory availability must not depend on one unreplicated process. The root
metadata set remains small and strongly replicated.

## Placement Policy

Placement constraints describe:

- replica count;
- required and prohibited regions;
- failure-domain separation;
- preferred leader locality;
- data residency;
- storage and hardware class;
- physical-engine capability.

Hard constraints fail when the cluster cannot satisfy them. Preferences may
affect cost without weakening a hard constraint.

The planner lowers locality requirements from Placement IR into range
descriptors. Operators can inspect the resulting decision.

## Transaction Routing

A single-range transaction commits through one Raft group. A cross-range
transaction uses the distributed protocol in
[08-transactions.md](08-transactions.md).

Range movement and splitting preserve transaction identity. A request routed
across a generation change either reaches the authoritative range or returns a
typed retry result.

## Follower Reads

A follower serves a snapshot only when:

1. it has applied through the required log index;
2. its safe timestamp covers the snapshot;
3. the requested schema and projections are available;
4. placement policy permits serving from that replica.

Geographic proximity is not a consistency proof.

## Hotspots

Cloud9 can respond to a hotspot by:

- splitting at a measured boundary;
- moving the leader;
- adding replicas for safe reads;
- selecting a different physical projection;
- applying admission control;
- requesting an explicit repartitioning change.

The system reports which action it selected and why.

## Tests

Placement tests cover:

- exhaustive, non-overlapping ranges;
- split and merge during reads and writes;
- stale generation fencing;
- leader and replica movement;
- directory loss and recovery;
- hard locality and residency constraints;
- follower safe-time enforcement;
- cross-range transactions during topology changes;
- local one-replica recovery.
