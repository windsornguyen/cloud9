# Consensus and Replication

Cloud9 uses Raft for replicated ordering. Raft remains a small, deterministic
state machine whose transitions can be tested without storage or networking.

The database node supplies durable storage, transport, timers, and command
application around that state machine.

## Boundary

The Raft layer accepts:

- messages from peers;
- election and heartbeat ticks;
- client proposals;
- membership changes;
- snapshot completion events.

It emits:

- messages to send;
- log entries and hard state to persist;
- committed entries to apply;
- snapshots to install;
- leadership and membership changes.

The caller must persist required state before sending messages or exposing
effects that depend on it.

## Safety Invariants

Cloud9 preserves:

1. At most one leader per term.
2. Committed entries remain in every future leader's log.
3. A state machine applies each committed index once and in order.
4. A node never votes twice in one term.
5. A removed replica cannot regain authority without a new configuration.
6. Snapshot installation cannot discard a committed suffix.

Database commands add another invariant: applying one command at one log index
must produce the same state on every replica.

## Durable State

The storage boundary includes:

- current term and vote;
- log entries;
- commit and applied progress;
- membership configuration;
- snapshots and their metadata.

Write-ahead ordering is explicit. Recovery either reconstructs one valid Raft
state or fails with a corruption error.

Log truncation follows a durable snapshot. The snapshot records the included
index, term, membership, and database state checksum.

## Range Replication

Each distributed range maps to one Raft group. The group replicates
deterministic physical commands produced by the lowering pipeline.

Raft orders operations within a range. It does not provide atomicity across
ranges. The transaction protocol owns that boundary.

Local mode uses a one-replica Raft group. It may commit without network I/O but
still uses the durable log and application order.

## Linearizable Reads

A leader may serve a current read only after proving its authority. Valid
mechanisms include Raft ReadIndex or a lease protocol with a proven fence.

A follower requires an applied-index and safe-time proof. Being caught up at
some earlier instant is insufficient.

## Membership Changes

Replica changes use Raft's supported configuration-change protocol. Cloud9
allows one logical membership change at a time per group unless the
implementation proves a stronger rule.

Learners receive state before becoming voters. Removal is complete only after
the new configuration is committed and stale ownership is fenced.

## Upgrades

Raft log commands and snapshots are versioned. A mixed-version group may
commit only encodings understood by every replica required for the active
configuration.

An old node never skips an unknown command. Skipping would make replicas apply
different state.

Upgrade gates verify:

- RPC compatibility;
- log and snapshot compatibility;
- command semantics;
- minimum reader and writer versions;
- downgrade safety.

## Failure Handling

Loss of quorum stops new commits. Cloud9 does not acknowledge an uncommitted
proposal.

Disk corruption, impossible log state, and snapshot checksum failure are fatal
to that replica. Recovery uses another identical replica or a verified backup,
not a different storage implementation.

## Observability

Each group reports:

- term and role;
- leader and membership;
- last log, commit, and applied indexes;
- replication lag;
- snapshot progress;
- proposal latency;
- rejected stale messages;
- storage and checksum failures.

Logs identify the range, replica, term, and index.

## Tests

Consensus tests cover:

- the Raft state machine against a reference model;
- elections, partitions, and message reordering;
- conflicting log repair;
- crash recovery at every persistence boundary;
- snapshot creation and installation;
- membership changes and stale-replica fencing;
- deterministic database command application;
- linearizable reads;
- Jepsen histories under process, network, and disk faults.

## References

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Ongaro's Raft dissertation](https://github.com/ongardie/dissertation)
- [Raft resources](https://raft.github.io/)
