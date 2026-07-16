# AWS ClockBound Backend

Cloud9's first production bounded-time backend uses AWS ClockBound. It is
available only on hosts that satisfy the declared hardware and software
contract.

## Deployment Contract

A TrueTime-enabled node requires:

- supported Linux on EC2;
- an AWS precision-time placement group;
- a currently supported Nitro instance family;
- a supported Elastic Network Adapter (ENA) driver;
- the Amazon Time Sync precision hardware clock (PHC);
- a healthy ClockBound daemon and client library;
- permission to read the ClockBound shared-memory segment.

The current instance and driver matrix belongs to AWS documentation. Cloud9
should test capabilities instead of embedding a stale family list.

## Data Path

```text
Amazon Time Sync
        |
EC2 precision hardware clock
        |
clock synchronization service
        |
ClockBound daemon
        |
ClockBound client
        |
Cloud9 TimeSource
        |
timestamp assignment and commit-wait
```

The provider returns an earliest time, latest time, and status. Cloud9 converts
those values into its internal time interval without discarding the PHC error
bound.

## Startup Checks

A node configured for TrueTime mode becomes ready only after it verifies:

1. The required PHC is present.
2. ClockBound is installed and reachable.
3. The daemon reports a synchronized status.
4. Returned intervals are well formed.
5. Uncertainty is within configured policy.
6. The PHC and synchronization service match deployment policy.
7. Time-scale and leap behavior match cluster policy.

Failure names the missing capability. Startup does not switch to another clock
implementation.

## Runtime Checks

Every time sample carries provider status. Cloud9 rejects a sample before using
it when status is unhealthy or its interval violates policy.

The node publishes:

- interval width;
- provider status;
- sample failures;
- commit-wait duration;
- time since the last healthy sample;
- readiness for TrueTime-dependent operations.

Alerts should fire before uncertainty reaches the rejection threshold.

All nodes use one time scale. AWS NTP smears leap seconds while the PHC does
not. Cloud9 rejects a mixed configuration.

## Failure Behavior

When ClockBound becomes unavailable, Cloud9 fails operations whose correctness
depends on bounded time. The node reports a typed provider error and becomes
unready for those operations.

Cloud9 does not:

- read the ordinary system clock as a substitute;
- replace the provider with an HLC;
- reuse an expired interval;
- accept an interval whose status is unknown;
- acknowledge a commit before commit-wait completes.

Service resumes after ClockBound is healthy and the node re-establishes the
provider contract.

## ClockBound and TrueTime

ClockBound supplies a host-local bounded-time interval. Cloud9 supplies the
database protocol that consumes it.

The integration is TrueTime-shaped because both expose an interval around real
time. Cloud9 does not claim to run Google's TrueTime service.

## Security Boundary

The time path is trusted infrastructure. A process that can falsify ClockBound
state or its shared-memory data can violate external consistency.

Deployments must restrict that interface, pin supported versions, and include
time configuration in host attestation and change control.

## Local Development

Local mode does not emulate ClockBound and does not claim hardware-backed
TrueTime. Tests may inject a deterministic bounded-time provider to exercise
protocol logic.

Production certification still requires the supported EC2 path. A mock cannot
prove the host clock guarantee.

## Validation

The AWS test suite must cover:

- clean startup on supported hardware;
- rejection on unsupported hardware;
- daemon stop and restart;
- PHC loss or synchronization failure;
- excessive uncertainty;
- clock steps and leap state;
- process suspend and resume;
- leader change during commit-wait;
- strict-serializability histories under network and process faults.

## References

- [AWS ClockBound](https://github.com/aws/clock-bound)
- [Amazon Time Sync on EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configure-ec2-ntp.html)
- [Microsecond-accurate clocks on EC2](https://aws.amazon.com/blogs/compute/its-about-time-microsecond-accurate-clocks-on-amazon-ec2-instances/)
