# Indexes and Schema Management

**Question**: How do we manage secondary indexes and schema changes in a distributed MVCC system with range sharding?

**Answer**: Indexes are sharded key-value ranges with online backfills using fence timestamps. Schema changes are versioned, timestamped metadata that queries evaluate at read time.

## The Core Principle

In Cloud9, everything is an MVCC key-value range:
- Table data: `/table/{name}/{pk}/{col}@t → value`
- Secondary indexes: `/index/{name}/{indexed_col}/{pk}@t → ∅`
- Vector indexes: `/vector/{name}/{embedding_prefix}/{pk}@t → metadata`

Indexes aren't separate systems. They're key prefixes that participate in the same sharding, replication, and transaction protocols as tables.

**Result**: Indexes scale horizontally. Creating an index doesn't lock the table. Queries can use old schema during migration.

## Why This Matters: Zero-Downtime DDL

Traditional databases lock tables during schema changes because schema is global state. Cloud9 versions schemas by timestamp, enabling operational capabilities that other databases can't match:

**1. Rolling upgrades**: Old nodes query at old timestamps (old schema), new nodes query at new timestamps (new schema). Both can coexist in the same cluster, reading the same data with different schema interpretations. This eliminates the upgrade coordination problem that plagues traditional databases.

**2. Zero-downtime DDL**: `ALTER TABLE` writes a new schema version with a commit timestamp—no locks acquired, no data rewritten. Queries started before the DDL commit see the old schema. Queries started after see the new schema. Both execute concurrently against the same underlying MVCC key-value ranges.

**3. Time-travel on schema**: Query data "as of" any timestamp, including historical schemas. `SELECT * FROM users AS OF TIMESTAMP '2024-01-15 10:00:00'` reconstructs not just the data state but the schema state at that moment. This enables reproducible debugging and compliance auditing.

**Why Postgres can't do this**: Postgres has MVCC for table data but treats schema metadata as global state. `ALTER TABLE users ADD COLUMN` acquires an `AccessExclusiveLock` on the table and updates system catalogs (`pg_class`, `pg_attribute`) atomically across all nodes. Even with MVCC protecting concurrent reads of user data, the schema change itself requires a barrier where all nodes observe the same catalog version. This is why Postgres major version upgrades require `pg_upgrade` and downtime.

**Cloud9's advantage**: Schema is versioned metadata replicated via Raft, not a global lock-protected catalog. DDL operations are timestamped writes to the metadata keyspace. Queries pick their schema version based on transaction start timestamp. The same separation that makes lock-free reads possible for data makes lock-free schema evolution possible for DDL.

This is the missing piece that prevents existing databases from supporting true zero-downtime upgrades.

## Secondary Indexes

### Index Key Encoding

**Local (non-unique) index**:
```
/index/{index_name}/{indexed_value}/{pk}@version → ∅
```

**Example**:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email TEXT,
    created_at TIMESTAMP
);

CREATE INDEX users_email_idx ON users(email);
```

**Physical layout**:
```
Table:
  /table/users/42/email@100    → "alice@example.com"
  /table/users/42/created_at@100 → "2024-01-15T10:00:00Z"

Index:
  /index/users_email_idx/alice@example.com/42@100 → ∅
```

**Key insight**: The primary key is part of the index key. This ensures uniqueness (multiple users with same email) and enables direct lookups without storing the full row.

**Why `→ ∅` (empty value)**: The index entry is a pointer. The actual data lives in the table. During query execution, we:
1. Scan index: `/index/users_email_idx/{email}/` → list of PKs
2. Lookup table: `/table/users/{pk}/` → full rows

**Global (unique) index**:
```
/index/{index_name}/{indexed_value}@version → primary_key
```

**Example**:
```sql
CREATE UNIQUE INDEX users_email_unique ON users(email);
```

**Physical layout**:
```
/index/users_email_unique/alice@example.com@100 → 42
```

**Uniqueness enforcement**: Before inserting, check if index key exists. If so, abort (duplicate). This check happens within the transaction (MVCC semantics apply).

### Multi-Column Indexes

**Schema**:
```sql
CREATE INDEX users_location_idx ON users(country, city);
```

**Encoding**:
```
/index/users_location_idx/{country}/{city}/{pk}@version → ∅
```

**Example**:
```
/index/users_location_idx/US/Seattle/42@100 → ∅
/index/users_location_idx/US/Seattle/99@100 → ∅
/index/users_location_idx/US/Portland/55@100 → ∅
```

**Query optimization**:
```sql
SELECT * FROM users WHERE country = 'US' AND city = 'Seattle';
```
→ Scan `/index/users_location_idx/US/Seattle/` (prefix scan)

```sql
SELECT * FROM users WHERE country = 'US';
```
→ Scan `/index/users_location_idx/US/` (partial prefix scan)

```sql
SELECT * FROM users WHERE city = 'Seattle';
```
→ Cannot use index (prefix doesn't match). Full table scan or different index.

**Ordering matters**: Most selective column should be first (generally).

### Covering Indexes

**Problem**: Index scan → table lookup adds latency. If query only needs indexed columns, avoid table lookup.

**Solution**: Store additional columns in index value.

**Schema**:
```sql
CREATE INDEX users_email_covering ON users(email) INCLUDE (created_at);
```

**Encoding**:
```
/index/users_email_covering/{email}/{pk}@version → CBOR({created_at: ...})
```

**Query**:
```sql
SELECT email, created_at FROM users WHERE email = 'alice@example.com';
```
→ Index scan returns `{email, created_at}`. No table lookup needed.

**Trade-off**: Index size increases (storing extra data). Worth it for hot query patterns.

## Local vs Global Indexes

**Local index**: Partitioned alongside table data.
- Index entries for range `/table/users/[0, 1000)/` live on same Raft group
- Co-location enables single-range transactions (fast)
- Range scans may require cross-shard scatter-gather

**Global index**: Independently sharded by indexed column.
- Index entries sharded by indexed value (e.g., email), not by PK
- Writes require cross-shard transactions (index range ≠ table range)
- Range scans are local to index shard (fast for queries, slow for writes)

**Cloud9's default**: Local indexes (CockroachDB model).

**When to use global**:
- Heavily skewed access patterns (e.g., `WHERE email = X` queries common, email values scattered)
- Willing to pay cross-shard transaction cost on writes

**Configuration**:
```sql
CREATE INDEX users_email_idx ON users(email) LOCAL;  -- Default
CREATE INDEX users_email_idx ON users(email) GLOBAL; -- Explicit
```

### Local Index Implementation

**Key encoding includes shard hint**:
```
/table/users/{pk}/...           → Table row
/index/users_email_idx/{pk}/{email}/... → Local index entry
```

**Why `{pk}` first in index key**: Ensures index entry co-locates with table row. Both hash to same range.

**Downside**: Query `WHERE email = 'alice@example.com'` must scan all ranges (scatter-gather). Mitigated by:
- Range caching (hot ranges stay local)
- Parallel scatter-gather (coordinated at SQL layer)

### Global Index Implementation

**Key encoding by indexed column**:
```
/table/users/{pk}/...                  → Table row (sharded by PK)
/index/users_email_idx/{email}/{pk}/... → Global index entry (sharded by email)
```

**Write path**:
```
INSERT INTO users (id, email) VALUES (42, 'alice@example.com');
→ Cross-shard transaction:
  1. Write to table range (PK=42 → range A)
  2. Write to index range (email=alice → range B)
  3. 2PC commit across ranges A and B
```

**Read path**:
```sql
SELECT * FROM users WHERE email = 'alice@example.com';
→ Single-shard index scan:
  1. Scan /index/users_email_idx/alice@example.com/ (range B, local)
  2. Lookup PKs in table (range A, may be remote)
```

**Trade-off**: Writes pay 2PC cost. Reads benefit from index locality.

## Online Index Backfills

**Problem**: Creating an index on existing table requires scanning all rows. Blocking writes during backfill is unacceptable.

**Solution**: Fence timestamp with incremental backfill.

### The Protocol

**Phases**:
1. **Schema registration** (instant): Add index metadata to catalog, mark as `BACKFILLING`
2. **Backfill** (background): Scan table, write index entries, track progress
3. **Validation** (fast): Verify no concurrent writes were missed
4. **Activation** (instant): Mark index as `PUBLIC`, queries start using it

### Fence Timestamp Mechanism

**Fence timestamp `t_fence`**: The moment when index creation begins.

**Invariant**:
- Writes with `t_w < t_fence`: Not automatically indexed (backfill handles them)
- Writes with `t_w ≥ t_fence`: Automatically indexed (transaction includes index write)

**Implementation**:
```rust
struct IndexMetadata {
    index_id: IndexID,
    table_id: TableID,
    status: IndexStatus,
    fence_timestamp: Timestamp,  // Set at backfill start
}

enum IndexStatus {
    Backfilling,  // Schema registered, backfill in progress
    Validating,   // Backfill complete, verifying consistency
    Public,       // Index ready for queries
}
```

### Backfill Algorithm

```
1. Coordinator starts backfill at t_fence = now()
2. Update catalog: index status = BACKFILLING, fence = t_fence
3. For each range in table:
     a. Scan rows at snapshot timestamp t_fence
     b. For each row:
          - Compute index key from indexed columns
          - Write index entry with timestamp t_fence
     c. Checkpoint progress
4. Once all ranges scanned:
     - Set status = VALIDATING
     - Check for concurrent writes in range [t_fence, now()]
     - If any writes without index entries: retry
5. Set status = PUBLIC
6. Index is live
```

**Key properties**:
- Backfill reads at `t_fence` (consistent snapshot, no locks)
- Concurrent writes at `t > t_fence` automatically include index entries
- No gap: every row is either backfilled or indexed by transaction

### Concurrent Write Handling

**Scenario**:
```
t=100: Start backfill (t_fence = 100)
t=105: Backfill reads row PK=42 (email=alice@example.com), writes index entry
t=110: Concurrent UPDATE: row PK=42 email → bob@example.com
t=120: Backfill completes
```

**What happens**:
```
At t=110, the UPDATE transaction checks catalog:
  - Index exists, status = BACKFILLING
  - t_w (110) ≥ t_fence (100)
  → Write index entries for BOTH old and new values:
    - Delete: /index/users_email_idx/alice@example.com/42@110
    - Insert: /index/users_email_idx/bob@example.com/42@110
```

**Result**: Index remains consistent. No missing entries.

### Validation Phase

**Why needed**: Ensure backfill didn't miss any writes due to race conditions.

**Algorithm**:
```rust
fn validate_backfill(index: &Index, t_fence: Timestamp) -> Result<()> {
    let now = hlc.now();

    // Check all writes in [t_fence, now)
    for txn in storage.transactions_in_range(t_fence, now) {
        for write in txn.writes {
            if write.table_id == index.table_id {
                // Verify corresponding index entry exists
                let index_key = compute_index_key(&index, &write);
                let entry = storage.get(index_key, txn.commit_ts);
                if entry.is_none() {
                    return Err(ValidationError::MissingIndexEntry);
                }
            }
        }
    }

    Ok(())
}
```

**If validation fails**: Retry backfill for affected rows.

**Typical duration**: Milliseconds (checking small transaction window).

### Incremental Backfill Checkpointing

**Problem**: Backfilling large table takes hours. Coordinator crashes mid-backfill.

**Solution**: Checkpoint progress, resume from last checkpoint.

**Metadata**:
```rust
struct BackfillProgress {
    index_id: IndexID,
    completed_ranges: Vec<RangeID>,
    current_range: RangeID,
    last_key: Key,  // Resume from here
}
```

**Resume protocol**:
```
1. Coordinator recovers, sees index status = BACKFILLING
2. Load BackfillProgress from catalog
3. Skip completed_ranges
4. Resume current_range from last_key
5. Continue backfill
```

**Checkpoint frequency**: Every 1M rows or 60 seconds (configurable).

## Vector Indexes

Vector similarity search (embeddings, RAG applications) requires specialized indexes. Cloud9 supports HNSW (Hierarchical Navigable Small World) and IVF-PQ (Inverted File with Product Quantization).

### Key Insight: Vector Indexes Are Sharded Indexes

Unlike relational indexes (exact match), vector indexes perform approximate nearest neighbor (ANN) search. But they still live in the same MVCC key space.

**Encoding**:
```
/vector/{index_name}/hnsw/{layer}/{node_id}@version → neighbors
/vector/{index_name}/ivf/{cluster_id}/{vector_id}@version → quantized_embedding
```

### HNSW Index

**Structure**: Multi-layer graph where each node is a vector. Layers form increasingly sparse skip lists.

**Schema**:
```sql
CREATE TABLE documents (
    id UUID PRIMARY KEY,
    content TEXT,
    embedding VECTOR(768)  -- Embedding dimension
);

CREATE INDEX documents_embedding_hnsw ON documents
    USING hnsw(embedding)
    WITH (m = 16, ef_construction = 200);
```

**Parameters**:
- `m`: Max edges per node (connectivity)
- `ef_construction`: Search breadth during construction

**Physical layout**:
```
Table:
  /table/documents/{uuid}/content@t → "..."
  /table/documents/{uuid}/embedding@t → BLOB(float32[768])

HNSW index (layer 0, densest):
  /vector/documents_embedding_hnsw/hnsw/0/node_A@t → {neighbors: [B, C, D], embedding: ...}
  /vector/documents_embedding_hnsw/hnsw/0/node_B@t → {neighbors: [A, E], embedding: ...}

HNSW index (layer 1, sparser):
  /vector/documents_embedding_hnsw/hnsw/1/node_X@t → {neighbors: [Y], embedding: ...}
```

**Query**:
```sql
SELECT id, content
FROM documents
ORDER BY embedding <-> '[0.1, 0.2, ...]'
LIMIT 10;
```

**Execution**:
1. Parse embedding from query
2. Start at top HNSW layer, find entry node
3. Greedy search through graph layers (navigate to nearest neighbors)
4. Reach layer 0, collect k nearest nodes
5. Lookup table rows for node IDs
6. Return results

**MVCC semantics**: HNSW nodes are versioned. Query at `t_r` sees graph structure as of `t_r`. Concurrent inserts write new nodes at higher timestamps (invisible to past readers).

### IVF-PQ Index

**Structure**: Inverted file index with product quantization (compression).

**Schema**:
```sql
CREATE INDEX documents_embedding_ivf ON documents
    USING ivf_pq(embedding)
    WITH (clusters = 1024, subvectors = 8);
```

**Parameters**:
- `clusters`: Number of Voronoi cells (IVF buckets)
- `subvectors`: Product quantization splits (compression level)

**Physical layout**:
```
Cluster centroids:
  /vector/documents_embedding_ivf/ivf/centroids@t → BLOB(float32[1024][768])

Inverted lists:
  /vector/documents_embedding_ivf/ivf/cluster_0/{uuid}@t → quantized_embedding
  /vector/documents_embedding_ivf/ivf/cluster_1/{uuid}@t → quantized_embedding
  ...
```

**Query**:
1. Find nearest cluster centroids (typically scan top 10-100 clusters)
2. Scan inverted lists for those clusters
3. Compute approximate distances using quantized embeddings
4. Return top k results

**Sharding**: Clusters shard independently. Query coordinator scans top clusters in parallel.

### Vector Index Backfills

**Challenge**: HNSW/IVF construction is computationally expensive (graph building, clustering, quantization).

**Solution**: Offline construction with fence timestamp.

**Protocol**:
1. Mark index as `BACKFILLING` at `t_fence`
2. Snapshot all embeddings at `t_fence`
3. Build HNSW graph / cluster IVF offline (hours for large datasets)
4. Write constructed index entries in batches
5. Validate: check writes in `[t_fence, now)` were indexed
6. Activate index

**Concurrent writes**: New vectors during backfill are indexed incrementally (inserted into partial graph/clusters). Graph may be suboptimal until next rebuild.

**Optimization**: Periodic rebuild for hot indexes (reclustering, graph optimization).

## Schema Changes (Online DDL)

Schema = table definitions, column types, constraints, indexes. Cloud9 treats schema as versioned metadata.

### Timestamped Schema Evolution

**Core idea**: Schema changes don't rewrite data. They create new schema versions. Queries evaluate schema based on their read timestamp.

**Example**:
```sql
-- t=100: Initial schema
CREATE TABLE products (
    id INT PRIMARY KEY,
    name TEXT
);

-- t=200: Add column
ALTER TABLE products ADD COLUMN price DECIMAL;

-- t=300: Query at t=150 (before ALTER)
SELECT * FROM products;  -- Sees schema v1 (no price column)

-- t=300: Query at t=250 (after ALTER)
SELECT * FROM products;  -- Sees schema v2 (includes price column)
```

### Schema Versioning

**Catalog structure**:
```rust
struct TableSchema {
    table_id: TableID,
    version: u32,
    valid_from: Timestamp,  // When this version became active
    valid_to: Option<Timestamp>,  // When superseded (None = current)
    columns: Vec<ColumnDef>,
    indexes: Vec<IndexDef>,
    constraints: Vec<Constraint>,
}
```

**Multiple versions coexist**:
```
Table "products":
  - Schema v1: valid [0, 200), columns: [id, name]
  - Schema v2: valid [200, ∞), columns: [id, name, price]
```

**Query protocol**:
```rust
fn get_schema_at_timestamp(table_id: TableID, ts: Timestamp) -> TableSchema {
    catalog.get_version(table_id)
        .filter(|v| v.valid_from <= ts && v.valid_to > ts)
        .unwrap()
}
```

### ADD COLUMN

**Operation**:
```sql
ALTER TABLE products ADD COLUMN price DECIMAL DEFAULT 0.0;
```

**Protocol**:
```
1. Coordinator picks t_schema = now()
2. Create schema v2 with new column:
     - valid_from = t_schema
     - columns += {price: DECIMAL, default: 0.0}
3. Write schema v2 to catalog (replicated via Raft)
4. Mark schema v1: valid_to = t_schema
5. Acknowledge client
```

**No data rewrite**: Existing rows don't gain a `price` column. Instead:
- Queries at `t < t_schema`: Use schema v1, don't project `price`
- Queries at `t ≥ t_schema`: Use schema v2, apply default value `0.0` if column missing

**Physical layout**:
```
Before:
  /table/products/42/name@50 → "Widget"

After ALTER at t=200:
  /table/products/42/name@50 → "Widget"  (unchanged)

Query at t=250:
  SELECT id, name, price FROM products WHERE id = 42;
  → Read: /table/products/42/*@250
  → Sees: {name: "Widget"}
  → Schema v2 fills default: {name: "Widget", price: 0.0}
```

**First write with new schema**:
```sql
-- t=300
UPDATE products SET price = 9.99 WHERE id = 42;
→ Writes: /table/products/42/price@300 → 9.99
```

**Subsequent queries**:
```sql
-- t=400
SELECT * FROM products WHERE id = 42;
→ Reads: /table/products/42/name@50 → "Widget"
→ Reads: /table/products/42/price@300 → 9.99
→ Returns: {id: 42, name: "Widget", price: 9.99}
```

### DROP COLUMN

**Operation**:
```sql
ALTER TABLE products DROP COLUMN description;
```

**Protocol**:
```
1. Coordinator picks t_schema = now()
2. Create schema v3 without column:
     - valid_from = t_schema
     - columns -= {description}
3. Write schema v3 to catalog
4. Mark schema v2: valid_to = t_schema
5. Acknowledge client
```

**Data retention**: Old column values remain in storage (MVCC). Queries at `t ≥ t_schema` don't project them.

**Garbage collection**: Eventual compaction removes dropped columns for rows with no active snapshots below `t_schema`.

### RENAME COLUMN

**Operation**:
```sql
ALTER TABLE products RENAME COLUMN name TO product_name;
```

**Protocol**:
```
1. Create schema v4 with renamed column:
     - columns: [id, product_name, price]
     - Add mapping: product_name → physical key "name"
2. Write schema v4 to catalog
```

**Physical layout unchanged**: Keys still use `/table/products/{id}/name@t`. Schema layer maps `product_name` → `name` at query time.

**Why**: Avoid rewriting all keys (expensive). Logical rename is sufficient.

### ALTER COLUMN TYPE

**Problem**: Changing column type requires rewriting values (e.g., `INT → BIGINT`, `TEXT → JSON`).

**Protocol**:
```sql
ALTER TABLE products ALTER COLUMN price TYPE NUMERIC(10,2);
```

**Two modes**:

**Mode 1: Compatible type change** (no rewrite):
- `INT → BIGINT`: Just widen reads
- `VARCHAR(50) → VARCHAR(100)`: No storage change
- **Protocol**: Create new schema version with updated type. Queries cast on read.

**Mode 2: Incompatible type change** (requires rewrite):
- `TEXT → JSON`: Parse required
- `INT → ENUM`: Validation required
- **Protocol**:
  1. Create new column with target type (`price_new NUMERIC`)
  2. Backfill: copy and convert data
  3. Drop old column, rename new column
  4. Validate: check all rows converted

**Cloud9's approach**: Default to Mode 1 (lazy casting). Mode 2 requires explicit `USING` clause:
```sql
ALTER TABLE products ALTER COLUMN price TYPE JSON USING price::JSON;
```

### Schema Change Transactions

**Question**: Can a transaction span schema changes?

**Answer**: Yes, but with constraints.

**Scenario**:
```
T1 at t=150: Reads products table (schema v1, no price column)
T2 at t=200: Executes ALTER TABLE ... ADD COLUMN price
T1 at t=210: Continues, tries to read price column
```

**Conflict**: T1 started before schema change but tries to use new schema.

**Resolution**: Transaction's schema is locked at start timestamp.

**Rule**: Transaction T with `t_start` uses schema valid at `t_start`. Schema changes at `t > t_start` are invisible to T.

**Implementation**:
```rust
struct Transaction {
    txn_id: TxnID,
    start_ts: Timestamp,
    schema_snapshot: HashMap<TableID, SchemaVersion>,  // Captured at start
}

fn execute_query(txn: &Transaction, query: &Query) {
    let schema = txn.schema_snapshot.get(&query.table_id);
    // Use this schema, ignore newer versions
}
```

### Catalog Management

**Catalog = metadata storage**: Tables, columns, indexes, constraints, users, permissions.

**Storage**: Special key prefix `/catalog/` in same MVCC storage.

**Encoding**:
```
/catalog/tables/{table_id}@version → TableSchema
/catalog/indexes/{index_id}@version → IndexMetadata
/catalog/constraints/{constraint_id}@version → Constraint
```

**Replication**: Catalog keys replicate via Raft like any other key. Catalog updates are transactional.

**Caching**: Nodes cache catalog entries. Invalidation on schema change (Raft replication includes invalidation message).

**Bootstrapping**: Initial catalog (system tables) created at cluster init:
```
/catalog/tables/0@0 → Schema(pg_tables)
/catalog/tables/1@0 → Schema(pg_indexes)
/catalog/tables/2@0 → Schema(pg_catalog)
```

## KV → SQL Projections as Versioned Schema Mappings

Recall from SQL-KV unification: KV namespaces can be queried via `KV()` virtual table. These projections are schema mappings.

**Example**:
```sql
CREATE KV MAPPING product_cache (
    key TEXT,
    value JSON (
        name TEXT,
        price DECIMAL
    )
);
```

**Catalog entry**:
```rust
struct KVMapping {
    mapping_id: MappingID,
    namespace: String,
    version: u32,
    valid_from: Timestamp,
    valid_to: Option<Timestamp>,
    schema: Vec<ColumnDef>,
}
```

**Versioned evolution**:
```sql
-- v1 at t=100
CREATE KV MAPPING product_cache_v1 (
    key TEXT,
    value JSON (name TEXT, price_cents INT)
);

-- v2 at t=200
CREATE KV MAPPING product_cache_v2 (
    key TEXT,
    value JSON (name TEXT, price DECIMAL, currency TEXT)
);
```

**Query routing**:
```sql
SELECT * FROM KV('product_cache') WHERE key = 'prod_123';
→ Executor checks mapping versions valid at query timestamp
→ Applies appropriate schema
```

**Union view** (common pattern):
```sql
CREATE VIEW products AS
    SELECT key, name, price_cents / 100.0 AS price
    FROM KV('product_cache')
    WHERE value->>'version' = '1'
  UNION ALL
    SELECT key, name, price
    FROM KV('product_cache')
    WHERE value->>'version' = '2';
```

**Benefit**: Multiple schema versions coexist. No data migration. Queries unify at runtime.

## Performance Considerations

### Index Maintenance Overhead

**Write path**: Each INSERT/UPDATE/DELETE on indexed table requires:
- Write to table row: 1 key-value pair
- Write to each index: N key-value pairs (N = number of indexes)

**Latency impact**: Local indexes co-locate with table (single range, fast). Global indexes require cross-shard transactions (2PC, slower).

**Optimization**: Batch index writes within transaction. Cloud9 buffers all writes, commits in one Raft round.

### Backfill Throttling

**Problem**: Full-speed backfill saturates I/O, degrades query performance.

**Solution**: Rate limiting.

**Configuration**:
```sql
ALTER INDEX users_email_idx SET BACKFILL RATE LIMIT 1000 ROWS/SEC;
```

**Implementation**: Backfill coordinator sleeps between batches to respect rate limit.

### Schema Cache Invalidation

**Problem**: Nodes cache schema. Schema change requires invalidating all caches. Stampede on catalog range.

**Solution**: Gossip-based invalidation.

**Protocol**:
1. Coordinator writes new schema version to catalog
2. Coordinator broadcasts invalidation message via Raft heartbeat
3. Each node invalidates local cache on receiving message
4. Nodes lazily refetch schema on next query

**Typical latency**: ~100ms (one Raft heartbeat interval).

## Comparison to Other Databases

### Spanner

**Similarities**:
- Indexes are sharded tables
- Online schema changes (no blocking)
- MVCC semantics for indexes

**Differences**:
- **Spanner**: No local indexes (all indexes global, cross-shard writes)
- **Cloud9**: Local indexes default (co-location benefits)
- **Spanner**: Schema changes are synchronous (TrueTime barrier)
- **Cloud9**: Schema changes are timestamped versions (MVCC)

### CockroachDB

**Similarities**:
- Local and global indexes
- Online backfills with checkpointing
- Fence timestamp mechanism
- Versioned schema

**Differences**:
- **CockroachDB**: Interleaved tables (deprecated in v22.1)
- **Cloud9**: Explicit co-location via range configuration
- **CockroachDB**: Backfill runs on leaseholder (single-threaded bottleneck)
- **Cloud9**: Distributed backfill (parallel range scanning)

### Postgres

**Similarities**:
- Rich index types (B-tree, GiST, GIN)
- Covering indexes (`INCLUDE`)
- Online index creation (`CONCURRENTLY`)

**Differences**:
- **Postgres**: Single-node, shared-buffer concurrency
- **Cloud9**: Distributed, MVCC across shards
- **Postgres**: VACUUM required for garbage collection
- **Cloud9**: MVCC compaction automatic (configurable)

### DynamoDB

**Similarities**:
- Global secondary indexes (GSI) are independently sharded
- Asynchronous index backfills

**Differences**:
- **DynamoDB**: No local indexes for multi-column queries
- **Cloud9**: Rich local index support
- **DynamoDB**: GSI eventual consistency (separate table)
- **Cloud9**: Indexes transactional (same MVCC semantics)

## Implementation Checklist

- [ ] Index key encoding (local and global)
- [ ] Secondary index write path (transaction integration)
- [ ] Secondary index read path (planner integration)
- [ ] Unique constraint enforcement
- [ ] Multi-column and covering indexes
- [ ] Online backfill coordinator
- [ ] Fence timestamp tracking
- [ ] Incremental backfill with checkpointing
- [ ] Validation phase (consistency check)
- [ ] HNSW vector index (graph structure)
- [ ] IVF-PQ vector index (clustering + quantization)
- [ ] Vector index query planner (ANN search)
- [ ] Schema versioning (catalog storage)
- [ ] ADD/DROP/RENAME COLUMN
- [ ] ALTER COLUMN TYPE (compatible and incompatible)
- [ ] Schema snapshot per transaction
- [ ] Catalog caching and invalidation
- [ ] KV mapping versioning
- [ ] Backfill throttling
- [ ] Metrics: index size, backfill progress, query selectivity

## Key Insights

**Indexes are just sharded MVCC ranges.** They aren't special. They follow the same replication, sharding, and transaction protocols as tables. This uniformity simplifies the system and enables operational flexibility (move indexes independently, replicate differently, etc.).

**Online DDL via timestamped schema.** Schema changes don't rewrite data. They create new metadata versions. Queries at different timestamps see different schemas. This enables zero-downtime migrations.

**Fence timestamps enable lock-free backfills.** Concurrent writes during backfill are automatically indexed if they occur after the fence. No read locks, no write locks. Backfill can take hours without blocking traffic.

**Vector indexes fit the same model.** HNSW and IVF-PQ are complex structures, but they live in the MVCC key space. Queries see graph/cluster state as of their snapshot timestamp. This is the only way to make vector search transactional.

**Schema versioning is schema-on-read.** Multiple schema versions coexist. KV mappings are versioned schemas for unstructured data. This is the bridge between "move fast with KV" and "lock down with SQL."

Cloud9 doesn't invent new index structures. It applies MVCC and range sharding uniformly, making indexes operational first-class citizens—not bolted-on afterthoughts.
