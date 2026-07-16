# Cloud9 Jepsen

This harness is a Jepsen `db/DB` wrapper for the real Cloud9 `c9` binary. It
uploads `target/release/c9` to every DB node, writes a per-node `cloud9.toml`,
starts `c9 start --config /opt/cloud9/cloud9.toml` with Jepsen's
`start-daemon!`, and drives Cloud9's public KV API with a shared
linearizable register workload.

Each node requires the same 256-bit `cluster.raft_key`; peer RPC bodies are
authenticated with HMAC-SHA256 before deserialization. The checked-in example
key is only for local and Jepsen testing.

Cloud9 treats SQL, key-value, document, object, and analytical APIs as source
dialects. The KV API is the first implemented dialect and the smallest
interface Jepsen can drive today. It does not define the final storage model.

The workload maps one shared register to `namespace/key`, writes JSON values as
value bodies, and implements CAS with S3-style ETag preconditions. The target
architecture lowers this request through the shared transaction IR into a
point-operation physical dialect.

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
complete yet. Without snapshot-backed reclamation, the WAL grows to its 4 GiB
limit and the node then fails closed.

Peer HMAC authenticates message bodies but does not provide confidentiality or
replay protection. Keep this transport on an isolated test network until it is
replaced with mutually authenticated, replay-resistant transport security.
