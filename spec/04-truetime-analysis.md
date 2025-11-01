# TrueTime Analysis

## Overview

TrueTime is not a heuristic - it is a provably correct approach to bounded clock uncertainty in distributed systems. This document explains the mathematical foundations, correctness guarantees, and implications for Cloud9.

## The 30-Second Sync Explained

TrueTime synchronizes with GPS and atomic clocks every 30 seconds. This interval is not arbitrary - it is an engineered choice based on hardware characteristics.

**Uncertainty formula**:
```
ε(t) = sync_error + drift_rate × time_since_sync
```

**Example calculation**:
- `sync_error` = 1 μs (GPS accuracy)
- `drift_rate` = 200 ppm (200 microseconds per second, typical quartz oscillator)
- `time_since_sync` = 30 seconds

```
ε = 1 μs + (200 μs/s × 30s) = 6001 μs ≈ 6ms
```

**The bound is mathematical, not empirical guesswork.**

## Mathematical Proof of Correctness

### TrueTime Invariant

`TT.now()` returns an interval `[earliest, latest]` where:
```
earliest ≤ absolute_true_time ≤ latest  (always)
```

### How the Invariant Is Maintained

1. At sync time t₀: measure offset from GPS/atomic clock → `sync_error`
2. Between syncs: bound grows linearly with known `drift_rate`
3. TrueTime daemon continuously computes: `ε(t) = sync_error + drift_rate × (t - t₀)`
4. Returns interval: `[now - ε, now + ε]`

### Formal Proof

**Theorem**: If two TrueTime intervals don't overlap (`l₁ < e₂`), then the events happened in that order in absolute time.

**Proof sketch**:
- Event 1 occurs at absolute time `t₁`, TrueTime returns `[e₁, l₁]`
- By invariant: `e₁ ≤ t₁ ≤ l₁`
- Event 2 occurs at absolute time `t₂`, TrueTime returns `[e₂, l₂]`
- By invariant: `e₂ ≤ t₂ ≤ l₂`
- If `l₁ < e₂`, then `t₁ ≤ l₁ < e₂ ≤ t₂`
- Therefore `t₁ < t₂` (absolute time ordering)

**Spanner's external consistency** follows from this property combined with the commit-wait protocol.

## Why It's Not Heuristic

### The Difference from Heuristics

**Heuristic approach** would be: "We think clocks are usually within 10ms, so let's use that."

**TrueTime approach** is: "We measure sync error, we know drift rate from hardware specs, we compute ε = f(sync_error, drift_rate, time), and we prove that [now - ε, now + ε] contains true time."

**The correctness is proven**, assuming:
1. Sync error measurement is accurate (GPS provides this)
2. Drift rate is bounded (quartz spec sheets provide this)
3. No Byzantine faults (time masters don't lie maliciously)

All three are reasonable assumptions with continuous monitoring.

### Why 30 Seconds Specifically

**Trade-offs**:
- **Shorter interval** (e.g., 1 second): Lower ε, higher sync overhead
- **Longer interval** (e.g., 5 minutes): Lower overhead, larger ε

**Google chose 30s** because:
1. With 200 ppm drift, 30s → ~6ms uncertainty (acceptable for write latency)
2. GPS/atomic clocks are stable enough to trust over 30s
3. Sync overhead is negligible (one request per 30s)
4. Safety margin: can tolerate missed sync without ε explosion

**It's not arbitrary - it's an engineered choice based on hardware characteristics.**

## Failure Modes

### Drift Exceeds Specification
- Next sync detects large offset
- ε grows beyond acceptable threshold
- System can refuse writes (fail-safe) or alert operators
- **Response**: Increase commit-wait proportionally or reject transactions

### GPS Outage
- Atomic clocks continue providing stable reference
- ε stays small for hours (atomic clock stability)
- Fallback: increase ε bound, continue with higher latency
- **Response**: Graceful degradation with documented impact

### Both GPS and Atomic Fail
- ε grows unbounded
- System must stop writes or increase commit-wait proportionally
- Spanner paper: "conservatively refuse transactions" in this scenario
- **Response**: Fail-stop to preserve correctness

### Key Safety Property

**TrueTime never violates the invariant.** If uncertainty cannot be bounded, the system:
1. Increases ε (and thus commit-wait latency)
2. OR refuses to assign timestamps
3. Never silently returns incorrect bounds

This is **fail-safe**, not fail-fast: the system prioritizes correctness over availability.

## Implications for Cloud9

### Cloud9 Must Implement the Same Rigorous Approach

```rust
struct TimeSource {
    last_sync: Instant,
    sync_error: Duration,
    drift_rate_ppm: f64,
}

impl TimeSource {
    fn uncertainty(&self) -> Duration {
        let elapsed = self.last_sync.elapsed();
        let drift = Duration::from_micros(
            (elapsed.as_micros() as f64 * self.drift_rate_ppm / 1_000_000.0) as u64
        );
        self.sync_error + drift
    }

    fn now_interval(&self) -> (Timestamp, Timestamp) {
        let now = Timestamp::now();
        let ε = self.uncertainty();
        (now - ε, now + ε)
    }
}
```

### Continuous Monitoring Required

- Track actual vs expected sync offsets
- Alert if drift_rate exceeds spec
- Fail-stop if ε > max_offset
- Log all sync errors and clock adjustments

**Not heuristic - measured, bounded, proven.**

### Cloud9 Time Synchronization Options

Cloud9 implements the same mathematical rigor as TrueTime, but adapts to available infrastructure:

#### 1. HLC Mode (Default)
- Use NTP/PTP for clock synchronization
- Measure and track ε using chrony statistics
- Commit-wait duration = ε (typically 10-50ms on cloud)
- **Advantage**: Works on commodity hardware
- **Trade-off**: Larger ε than TrueTime

#### 2. GPS + Atomic Clocks (Premium)
- Install GPS receivers and atomic clocks (colo/on-prem)
- Direct PTP feed to Cloud9 nodes
- Achieve ε < 1ms (TrueTime-class performance)
- **Advantage**: Minimal commit-wait latency
- **Trade-off**: Hardware cost and operational complexity

#### 3. TSO Mode (Alternative)
- Use centralized timestamp oracle instead of physical time
- No clock synchronization needed
- External consistency guaranteed by serialization
- **Advantage**: Simpler when clock sync is unreliable
- **Trade-off**: Oracle becomes bottleneck

### The Key Insight

**The protocol (commit-wait + bounded uncertainty) is what matters, not the specific hardware.**

TrueTime achieves tight bounds (ε < 7ms) because Google has GPS + atomic clocks. Cloud9 can achieve the same correctness with looser bounds (ε = 10-50ms) using NTP/PTP. The latency differs, but the guarantees are identical.

**External consistency is provable in both cases** - the math doesn't change, only the constant ε.

### What Cloud9 Learns from TrueTime

1. **Bounded uncertainty is non-negotiable**: Must measure and enforce ε
2. **Fail-safe is correct**: Refuse transactions rather than violate invariants
3. **Continuous monitoring is essential**: Track clock health in real-time
4. **Hardware determines ε, protocol ensures correctness**: Both matter
5. **Document the math**: Users trust provable systems over heuristics

## Summary

TrueTime is not a heuristic. It is a formally proven approach to bounded clock uncertainty:

- **Sync every 30 seconds** is an engineered choice based on drift rate math
- **ε = sync_error + drift_rate × time** is a proven bound on uncertainty
- **[earliest, latest] contains absolute time** is a maintained invariant
- **External consistency follows** from this invariant + commit-wait
- **Failure modes are explicit** and preserve correctness (fail-safe)

Cloud9 implements the same rigorous approach, adapting to available time infrastructure while maintaining identical correctness guarantees.
