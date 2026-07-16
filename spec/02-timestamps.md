# Timestamp Model

Cloud9 uses bounded physical time for external consistency. The provider
returns an interval that contains real UTC:

```text
now() -> [earliest, latest]
```

The interval is a correctness input. It is not an estimate used only for
observability.

## Provider Contract

A bounded-time provider returns:

```text
TimeInterval {
    earliest
    latest
    status
}
```

The provider must guarantee:

1. Real UTC is within the closed interval.
2. `earliest <= latest`.
3. The reported status is healthy.
4. The interval width is within configured policy.
5. Synchronization status and time-scale behavior are defined.

Cloud9 validates every observable condition. The platform and provider remain
responsible for the real-time containment guarantee.

## Commit Timestamps

A commit timestamp contains bounded physical time and a deterministic
tie-breaker:

```text
CommitTimestamp {
    physical
    logical
}
```

The physical component is at or after the provider's `latest` bound. Cloud9
rounds upward when timestamp precision requires it. The logical component
orders transactions that share a physical value.

The tie-breaker does not replace bounded time. It only completes the order
among concurrent transactions.

## Commit-Wait

Cloud9 may acknowledge a commit only after:

```text
now().earliest > commit_timestamp.physical
```

Raft replication and transaction decision durability happen before this wait.
The response happens after it.

This order is part of the transaction protocol:

1. Read a valid time interval.
2. Select the commit timestamp.
3. Make the commit decision durable.
4. Wait until the commit timestamp is certainly in the past.
5. Acknowledge the transaction.

## Provider Modes

### TrueTime mode

TrueTime mode requires an approved bounded-time provider. The first production
backend is AWS ClockBound on supported Linux EC2 hardware with Amazon Time Sync
and a precision hardware clock.

Startup fails when the required capability is absent. Operations that depend
on bounded time fail when the provider becomes unhealthy.

### Local mode

Local mode may use a local physical clock plus logical ordering. It supports
single-process development without special hardware.

Local mode is a distinct consistency mode. It does not advertise
hardware-backed TrueTime or cross-machine external consistency.

## Hybrid Logical Clocks

A Hybrid Logical Clock (HLC) can carry causal metadata and order events. It
cannot prove a bound around real UTC by itself.

Cloud9 may use HLC-style metadata inside a subsystem. It cannot use an HLC as a
silent replacement for a failed bounded-time provider.

## Failure Rules

Cloud9 fails closed when:

- the provider cannot return an interval;
- provider status is unhealthy;
- interval width exceeds policy;
- timestamps move outside the provider contract;
- the host loses the required clock capability.

The node becomes unready for operations that require bounded time. It does not
change consistency mode.

## Separate Clocks

Elapsed-time mechanisms use a monotonic clock. This includes timeouts, retries,
election timers, and lease duration measurement.

Transaction timestamps use bounded UTC. Mixing these clock domains is an
error.

## Tests

The timestamp layer requires:

- provider contract tests;
- malformed and excessive interval tests;
- provider loss and recovery tests;
- leap-state tests;
- commit-wait boundary tests;
- clock-step and suspend tests;
- Jepsen histories that verify real-time transaction order.

Tests must include the actual production provider. A mock proves protocol
logic, not the host time guarantee.
