# External Consistency

Cloud9 provides external consistency only when a valid bounded-time provider
is active.

External consistency means transaction order respects real time. If transaction
`T1` completes before transaction `T2` begins, `T1` must appear before `T2` in
the serial history. This property is also called strict serializability.

## Required Invariants

The protocol depends on four invariants:

1. Every committed transaction has one global commit timestamp.
2. Conflict resolution follows commit timestamp order.
3. The bounded-time interval contains real UTC.
4. A commit is not acknowledged until its timestamp is certainly in the past.

Raft alone does not establish these invariants across independent ranges.
Multi-version concurrency control (MVCC), distributed transaction
coordination, and bounded time complete the protocol.

## Write Protocol

For a single-range transaction:

1. Evaluate reads and preconditions at a stable MVCC snapshot.
2. Read `[earliest, latest]` from the bounded-time provider.
3. Choose a commit timestamp at or after `latest`.
4. Replicate the deterministic commit command through Raft.
5. Apply the committed versions at that timestamp.
6. Wait until a new `earliest` is greater than the commit timestamp.
7. Return success.

For a cross-range transaction, two-phase commit adds prepare records and one
durable transaction decision. Every participant commits at the same timestamp.
The coordinator performs commit-wait before returning success.

Retries use the same transaction identity. A retry cannot create a second
logical commit.

## Why Commit-Wait Works

Assume `T1` returns before `T2` starts. When `T1` returns, real time is later
than `T1`'s commit timestamp because commit-wait has completed.

`T2` then chooses a timestamp at or after its provider's `latest` bound. That
bound is at or after real time. Therefore `T2` receives a later timestamp than
`T1`.

The serialization order now respects the observed real-time order.

## Read Protocol

A read-write transaction reads from its chosen MVCC snapshot and validates
conflicts before commit.

A read-only transaction may use an explicit timestamp after Cloud9 proves that
all participating ranges have applied through that timestamp. A current read
must also account for bounded-time uncertainty.

Follower reads require an applied-index or safe-time proof. Replica proximity
alone is insufficient.

## Provider Failure

If bounded time is unavailable or outside policy, operations that promise
external consistency fail with a typed time-source error.

Cloud9 does not:

- acknowledge first and wait later;
- use wall-clock point estimates as bounds;
- switch to an HLC consistency mode;
- route the request to a weaker implementation.

Recovery may restore service after the provider is healthy and the node has
re-established its clock contract.

## Local Mode

A single local process can provide serializable transactions through one
scheduler and MVCC. That property does not depend on bounded-time hardware.

Local mode does not claim cross-machine external consistency. Moving a
database into distributed TrueTime mode requires an explicit configuration and
capability check.

## Verification

Correctness tests must cover:

- overlapping and non-overlapping transactions;
- single-range and cross-range commits;
- coordinator failure before and after the durable decision;
- leader changes during commit-wait;
- bounded-time loss and excessive uncertainty;
- retry idempotency;
- follower reads and safe-time advancement;
- Jepsen strict-serializability histories.

The history must record invocation and completion times. Serializability alone
cannot verify the real-time ordering requirement.
