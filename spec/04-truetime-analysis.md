# Bounded-Time Analysis

Cloud9 needs a bounded interval around real UTC. A synchronized point estimate
is insufficient.

Let a provider return:

```text
TT.now() = [earliest, latest]
```

The provider contract is:

```text
earliest <= real_utc <= latest
```

Define uncertainty as:

```text
epsilon = latest - earliest
```

Cloud9 treats this containment rule as a correctness assumption. The deployment
must use an approved provider that can uphold it.

## Commit Rule

Cloud9 chooses:

```text
commit_time >= TT.now().latest
```

It returns success only after:

```text
TT.now().earliest > commit_time
```

At response time, real UTC is therefore later than `commit_time`.

If another transaction begins after that response, its `latest` bound is later
than real UTC at the first response. Its commit timestamp must be later than
the first timestamp. This establishes real-time order for non-overlapping
transactions.

## What Bounded Time Does Not Prove

Bounded time does not provide:

- serializable conflict handling;
- atomic commit across ranges;
- durable replication;
- idempotent retries;
- safe follower reads.

MVCC, transaction coordination, Raft, and recovery provide those properties.
Bounded time connects their serialization order to real time.

## ClockBound's Role

AWS ClockBound exposes an interval and clock status to local clients. Cloud9
uses it as the first implementation of the bounded-time provider contract.

ClockBound is not the transaction protocol. Cloud9 still validates provider
health, enforces uncertainty policy, assigns timestamps, and performs
commit-wait.

Cloud9 should describe this mode as TrueTime-shaped. Google TrueTime is a
specific Google service. The shared idea is an API that returns a trustworthy
time interval.

## Hardware Boundary

Cloud9's first TrueTime mode requires supported Linux EC2 hardware, Amazon Time
Sync, a precision hardware clock, and ClockBound. The exact supported instance
families and drivers follow current AWS documentation.

An ordinary NTP-synchronized system clock does not satisfy this mode. An HLC
also does not satisfy it. Either could support a separately named consistency
mode, but neither may appear as an automatic fallback.

## Uncertainty Cost

Commit-wait latency grows with the uncertainty interval. A wider bound is still
correct when it remains within policy, but it delays acknowledgements.

Performance work should reduce measured uncertainty without weakening the
containment guarantee. Benchmarks must report interval width and commit-wait
time.

## Failure Model

Cloud9 rejects the provider when:

- status reports unsynchronized or unknown time;
- uncertainty exceeds configured policy;
- the interval is malformed;
- the host loses the required hardware path;
- the provider daemon or client interface is unavailable.

These failures remove readiness for TrueTime-dependent operations. Existing
data remains durable. The node does not silently change its consistency
contract.

## Proof Obligations

A production backend needs evidence for:

1. Real UTC containment under normal operation.
2. Detection of source loss and clock steps.
3. Correct leap-second behavior.
4. Safe behavior across suspend, resume, and migration.
5. Correct interval propagation into commit-wait.
6. A maximum accepted uncertainty policy.
7. End-to-end histories that verify strict serializability.

Unit tests can prove Cloud9's interval arithmetic. Hardware integration tests
must prove the provider assumptions.
