# Cloud9 Jepsen

This harness is a Jepsen `db/DB` wrapper for the real Cloud9 `c9` binary. It
uploads `target/release/c9` to every DB node, writes a per-node `cloud9.toml`,
starts `c9 start --config /opt/cloud9/cloud9.toml` with Jepsen's
`start-daemon!`, and drives Cloud9's public KV API with a shared
linearizable register workload.

Each node requires the same 256-bit `cluster.raft_key`; peer RPC bodies are
authenticated with HMAC-SHA256 before deserialization. The checked-in example
key is only for local and Jepsen testing.

Cloud9 is a relational database first: Postgres-compatible SQL and native KV are
peer APIs over one MVCC storage layer, one transactional IR, one timestamp
system, and one transaction coordinator. The KV workload here is the smallest
front door Jepsen can drive today, not a separate product direction.

The workload maps one shared register to `namespace/key`, writes JSON values as
value bodies, and implements CAS with S3-style ETag preconditions. This KV
surface is only one Cloud9 API front door; SQL and KV are intended to lower into
the same transactional IR.

## Build

Build on a Linux Jepsen control host with the same CPU architecture as the DB
nodes. The helper rejects host-native macOS builds because Jepsen uploads this
binary directly to Debian.

```bash
./jepsen/scripts/build-target.sh
```

## Run

From `jepsen/`, with SSH-reachable Jepsen DB nodes:

```bash
lein run test \
  --nodes-file ~/nodes \
  --username root \
  --time-limit 60 \
  --concurrency 5n \
  --stagger 0.01 \
  --binary ../target/release/c9
```

Useful knobs:

```bash
lein run test --help
lein run test --nodes-file ~/nodes --username root --time-limit 60 --concurrency 5n --nemesis-mode kill-leader
lein run serve
```

The harness discovers the current Raft leader before opening client sessions,
rediscovers it after failover, then heals the final fault and reads from every
client thread before checking the history. Followers reject KV RPCs rather than
serving node-local state.

## Current Limit

`c9 start` persists Raft hard state and log entries before sending network
effects, then reconstructs KV state by replaying committed commands. Snapshot
transfer, log compaction, read forwarding, and richer nemesis coverage are not
complete yet.
