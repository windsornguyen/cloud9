# cloud9-consensus Architecture

Design decisions for our Raft implementation.

## Philosophy

**Pure state machine, no I/O.**

```rust
// The entire API
fn step(&mut self, event: Event) -> Effects
fn propose(&mut self, cmd: Command) -> Result<(LogIndex, Effects), ProposeError>
```

Raft is a deterministic function: `(State, Event) → (State', Effects)`. The caller handles all I/O (network, disk). This enables:

- **Deterministic simulation testing** (like FoundationDB)
- **Loom-based concurrency testing**
- **Any async runtime** (tokio, async-std, smol, none)
- **Embeddability** (no threads, no background tasks)

## Why This Approach?

### Proven at Scale

TiKV (raft-rs) and CockroachDB (etcd/raft) both use pure state machine Raft. This isn't academic—it's battle-tested at exabyte scale.

### Testing Advantage

FoundationDB's success comes from deterministic simulation. They simulate years of failures in minutes. Our pure design enables identical testing:

```rust
fn simulate_partition_heal() {
    let mut sim = Simulator::new(seed: 42);
    sim.partition(node_0, node_1);
    sim.run_ticks(10000);
    sim.heal_partition();
    sim.run_until_stable();
    assert!(sim.all_committed_same());
}
```

No mocks, no flaky tests, no "works on my machine."

### Multi-Raft Efficiency

For Spanner-like sharding, we'll run thousands of Raft groups per node. Pure state machines mean:

- No threads per group
- No async runtime overhead per group
- Predictable memory (N groups × state size)
- Batch processing across groups

## Comparison to Alternatives

| Aspect | raft-rs (TiKV) | async-raft | cloud9-consensus |
|--------|---------------|------------|------------------|
| I/O model | Pure state machine | Async runtime | Pure state machine |
| Dependencies | protobuf | tokio | serde only |
| Runtime | Any | tokio only | Any |
| Snapshot data | Passes through | Streams through | Coordinated out-of-band |
| Complexity | Medium | Higher | Minimal |

## Log Compaction / Snapshots (Chapter 5)

### The Design Question

How should Raft handle snapshots? Three options:

1. **Raft buffers snapshot data** - Messages contain bytes
2. **Raft streams snapshot data** - AsyncRead/Write handles
3. **Raft coordinates, caller transfers** - Out-of-band data movement

### Our Decision: Coordination Only

Raft is a **coordinator**, not a data conduit.

```rust
pub struct Effects {
    // ... existing fields ...

    /// Followers that need snapshots (can't use AppendEntries).
    /// Caller should transfer snapshot out-of-band, then notify Raft.
    pub snapshot_needed: Vec<SnapshotTarget>,
}

impl RaftNode {
    /// Notify: snapshot sent to peer.
    pub fn snapshot_sent(&mut self, peer: NodeId, meta: SnapshotMeta) -> Effects;

    /// Notify: snapshot received and installed.
    pub fn snapshot_installed(&mut self, meta: SnapshotMeta) -> Effects;

    /// Compact log prefix after local snapshot.
    pub fn compact(&mut self, up_to: LogIndex) -> Result<(), CompactError>;
}
```

### Rationale

**Raft doesn't need the bytes.** Snapshot data is opaque to consensus. Raft only needs:

1. "Follower X is too far behind" (can't send AppendEntries)
2. "Snapshot covering index Y was transferred"
3. "Log can be truncated to index Z"

**Zero memory overhead.** Snapshots can be gigabytes. Buffering them violates our "minimal, pure" philosophy.

**Caller controls transport.** They might use:
- HTTP streaming
- gRPC
- rsync
- S3 upload/download
- Shared filesystem

We don't dictate. We coordinate.

**Chunking is a transport concern.** The dissertation specifies chunking because it describes a complete system. For a library, chunking strategy depends on network characteristics the caller knows better than we do.

### Comparison to Other Implementations

| Aspect | raft-rs | async-raft | cloud9-consensus |
|--------|---------|------------|------------------|
| Data location | In Raft messages | Streamed via handles | Out-of-band |
| Memory overhead | Full snapshot | Chunked buffer | Zero |
| Transport | Fixed (messages) | Fixed (async stream) | Caller's choice |
| Chunking | None (single msg) | In Raft | Caller's choice |
| Complexity | Medium | Higher | Lower in Raft |

### Trade-off

**Caller responsibility increases.** They must implement:
- Reliable snapshot transfer
- Progress tracking / retries
- Coordination with Raft notifications

But our users are building distributed systems. They already have opinions about transport. We shouldn't force ours on them.

### Election Timer During Snapshot

The dissertation says snapshot chunks reset the election timer. Our solution:

```rust
pub enum RoleState {
    Follower(Follower),
    InstallingSnapshot,  // Special state: no elections, no timeouts
    PreCandidate(PreCandidate),
    Candidate(Candidate),
    Leader(Leader),
}
```

During `InstallingSnapshot`, the follower doesn't start elections. When installation completes, it resumes as Follower with updated log state.

## Future: Production Hardening

For Spanner-like scale, we'll add:

### 1. Batching API

```rust
fn propose_batch(&mut self, cmds: &[Command]) -> Vec<(LogIndex, Effects)>
fn step_batch(&mut self, events: &[Event]) -> Effects
```

### 2. Storage Trait

```rust
trait LogStorage {
    fn append(&mut self, entries: &[Entry]);
    fn get(&self, index: LogIndex) -> Option<&Entry>;
    fn truncate_before(&mut self, index: LogIndex);
}
```

Allows disk-backed log for large groups.

### 3. Zero-Copy Messages

```rust
// Current: clones
pub struct Command(pub Vec<u8>);

// Future: reference-counted
pub struct Command(pub Bytes);
```

### 4. Metrics Hooks

```rust
pub struct Effects {
    pub metrics: Option<StepMetrics>,
}
```

None of these are architectural changes—they're additive optimizations on a solid foundation.

## Summary

Our Raft is intentionally minimal:

- **Pure state machine** - deterministic, testable, embeddable
- **No I/O** - caller handles network/disk
- **Minimal dependencies** - just serde
- **Coordination-only snapshots** - zero memory overhead

This isn't the most feature-rich Raft. It's the most composable one. For a database that needs to run thousands of Raft groups efficiently, that's the right trade-off.
