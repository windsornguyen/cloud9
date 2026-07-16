# Transaction Protocol

Cloud9 uses MVCC, durable transaction records, and two-phase commit for
serializable transactions across ranges. A healthy bounded-time provider adds
external consistency.

## Guarantees

A committed transaction provides:

- atomicity across all participants;
- serializable conflict ordering;
- one snapshot across compatible dialects;
- idempotent retry by transaction identity;
- external consistency in TrueTime mode.

Local mode can provide serializability without bounded-time hardware. It does
not claim cross-machine external consistency.

## Transaction Record

The coordinator stores one durable record:

```text
TransactionRecord {
    id
    state
    snapshot
    participants
    commit_timestamp
}
```

Valid state transitions are:

- `Pending -> Preparing`
- `Pending -> Aborted`
- `Preparing -> Committed`
- `Preparing -> Aborted`

`Committed` and `Aborted` are terminal. A durable commit decision cannot
become an abort. A retry reads the record and continues the recorded outcome.

## Read-Only Transactions

A read-only transaction chooses one snapshot. Every participant must prove it
has applied all commits through that snapshot.

The transaction creates no write intents. Reads may execute on followers when
their applied index, safe timestamp, schema, and projection state cover the
snapshot.

A current externally consistent read uses bounded time and safe-time
information. An explicit stale read uses the caller's timestamp and declared
staleness policy.

## Single-Range Writes

A single-range transaction uses one replicated command:

1. Read and evaluate at a stable snapshot.
2. Validate read versions, ranges, predicates, and conditions.
3. Obtain a valid bounded-time interval when the mode requires it.
4. Select a commit timestamp.
5. Replicate the transaction result through the range's Raft group.
6. Apply all versions atomically.
7. Perform commit-wait in TrueTime mode.
8. Return the durable result.

The command includes every value needed for deterministic application.

## Cross-Range Writes

Cross-range writes use two-phase commit (2PC). Each participant is a
Raft-replicated range.

### Prepare

1. Allocate a stable transaction identity.
2. Write provisional intents at each participant.
3. Validate point, range, and predicate reads.
4. Replicate a prepared record in each participant.
5. Return each participant's timestamp constraints.

Any validation failure aborts the transaction. Prepared participants retain
enough state to recover without the original client.

### Decision

The coordinator:

1. reads a valid bounded-time interval when required;
2. chooses one timestamp that satisfies all participant constraints;
3. stores the participant set and decision durably;
4. sends the same decision and timestamp to every participant.

The durable transaction record is the authority after prepare. A timeout is
not evidence of abort.

### Finalize

Each participant replicates the decision. Commit converts intents into versions
at the shared timestamp. Abort removes the provisional intents.

Cleanup may continue after the decision is durable. Visibility follows the
decision record, not cleanup completion.

The coordinator performs commit-wait before returning a committed result in
TrueTime mode.

## Timestamp Selection

In TrueTime mode, the commit timestamp's physical component is at or after:

- the bounded-time provider's `latest` value;
- every participant's observed version;
- every causally required predecessor.

Cloud9 acknowledges only after a fresh interval has:

```text
earliest > commit_timestamp.physical
```

The detailed proof is in
[03-external-consistency.md](03-external-consistency.md).

## Serializable Validation

Cloud9 records the effects required to validate the transaction:

- point reads and observed versions;
- range reads and range generations;
- predicates and selected indexes;
- point and range writes;
- source-dialect conditions.

Prepare rejects any intervening commit that changes the transaction's result.
Range and predicate validation must detect phantoms.

An optimization may reduce validation work only when it preserves this
contract.

## Intent Conflicts

An intent identifies its transaction. Readers never expose it as committed
data.

On conflict, a transaction may wait, push, or abort according to one
deterministic priority policy. The policy must prevent deadlock and preserve
the durable decision.

A participant cannot abort a transaction after discovering a committed
decision.

## Recovery

Any node can recover a prepared transaction:

1. Read the durable transaction record.
2. If committed, finalize every known participant.
3. If aborted, remove every known intent.
4. If still preparing, use the protocol's ownership and timeout rules to elect
   one recovery coordinator.
5. Replicate the recovered decision before cleanup.

The participant list must be complete before the transaction can commit.
Otherwise recovery could miss a write.

## Ambiguous Results

A client disconnect after submission may leave an unknown outcome. The API
returns the transaction identity with the ambiguity error.

The client resolves that identity. It does not resubmit the logical mutation
under a new identity.

## Cross-Dialect Transactions

Source dialects lower into one Transaction IR. A transaction may span dialects
only when every lowering supports the requested atomicity and consistency.

Object bytes, columnar projections, and metadata may use different physical
engines. The transaction record states which representations are authoritative
and which updates may complete asynchronously.

Derived projections cannot become authoritative by accident.

## Performance Rules

Valid optimizations include:

- one-phase commit for proven single-range transactions;
- parallel prepare;
- batching Raft proposals;
- asynchronous cleanup after a durable decision;
- safe follower reads;
- co-location through explicit placement;
- parallel commit with a proof that the durable record and intents imply one
  outcome.

An optimization must state the invariant that removes a protocol step.

## Tests

Transaction tests cover:

- single-range and cross-range atomicity;
- point, range, predicate, and dialect-specific conflicts;
- coordinator and participant crashes at every state transition;
- retries and ambiguous results;
- leader changes during prepare, decision, and commit-wait;
- range splits and movement during 2PC;
- bounded-time failure;
- follower snapshot safety;
- projection authority and freshness;
- Jepsen serializability and strict-serializability histories.
