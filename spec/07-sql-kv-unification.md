# SQL and KV Unification

**Question**: How do SQL and KV coexist without being separate systems bolted together?

**Answer**: They don't coexist—they're the same thing. SQL tables and KV namespaces are both key prefixes in a single MVCC key-value space.

## The Core Insight

Every distributed database has an MVCC key-value layer at its core. Most databases then build SQL or KV APIs on top as separate, incompatible systems:

- Spanner: SQL-only, no KV access to the underlying layer
- DynamoDB: KV-only, no SQL
- CockroachDB: SQL-only, abandoned KV API experiments
- YugabyteDB: Both SQL and KV, but they're separate systems (YCQL vs YSQL)

**Cloud9 makes them the same system.** A SQL table is a key prefix. A KV namespace is a key prefix. Both compile to the same transactional IR, execute in the same transaction coordinator, and write to the same MVCC storage.

**Result**: BEGIN a transaction, write to SQL tables, read from KV namespaces, join SQL rows with KV data. One transaction, one commit timestamp, one consistency model.

## Key Encoding: The Foundation

All data in Cloud9 lives in a single MVCC key space. Keys are byte strings with structure:

```
[prefix][primary_key][column_or_suffix][version]
```

The prefix determines whether something is SQL or KV. The rest is just bytes.

### SQL Table Encoding

**Schema**:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT
);
```

**Key encoding**:
```
Table prefix: /table/users/
Row with id=42:
  - /table/users/42/name  → "Alice"
  - /table/users/42/email → "alice@example.com"

With timestamp:
  - /table/users/42/name@t=100  → "Alice"
  - /table/users/42/email@t=100 → "alice@example.com"
```

**Structure**:
- `/table/{table_name}/{pk}/{column}@{version}` → `value`
- Primary key is part of the key path
- Each column is a separate versioned key
- Row = all keys with same prefix `/table/{table_name}/{pk}/`

**Benefits**:
- Point reads: single key lookup
- Range scans: iterate keys with common prefix
- Columnar access: read only needed columns
- MVCC: append-only versioned values

### KV Namespace Encoding

**API**:
```rust
kv.put("sessions", "sess_abc123", session_data);
```

**Key encoding**:
```
Namespace prefix: /kv/sessions/
Key "sess_abc123":
  - /kv/sessions/sess_abc123@t=100 → session_data
```

**Structure**:
- `/kv/{namespace}/{key}@{version}` → `value`
- User-provided key is opaque bytes
- No column decomposition (value is blob)
- Namespace = all keys with prefix `/kv/{namespace}/`

**Benefits**:
- Simple model: just put/get/delete
- No schema required
- Arbitrary byte keys and values
- Same MVCC versioning as SQL

### Multi-Column Primary Keys

**Schema**:
```sql
CREATE TABLE events (
    user_id INT,
    timestamp BIGINT,
    event_type TEXT,
    PRIMARY KEY (user_id, timestamp)
);
```

**Key encoding**:
```
/table/events/{user_id}/{timestamp}/event_type@t → value
```

**Example**:
```
/table/events/42/1609459200/event_type@100 → "login"
/table/events/42/1609459201/event_type@100 → "click"
```

**Range scan**:
```sql
SELECT * FROM events WHERE user_id = 42;
```
→ Scan `/table/events/42/` prefix

### Secondary Indexes

**Schema**:
```sql
CREATE INDEX users_email_idx ON users(email);
```

**Key encoding**:
```
Index prefix: /index/users_email_idx/
Mapping: email → primary key
  - /index/users_email_idx/{email}@{version} → primary_key

Example:
  - /index/users_email_idx/alice@example.com@100 → 42
```

**Query**:
```sql
SELECT * FROM users WHERE email = 'alice@example.com';
```

**Plan**:
1. Index lookup: `/index/users_email_idx/alice@example.com@t_r` → `42`
2. Table lookup: `/table/users/42/*@t_r` → full row

**Uniqueness constraint**:
- Check index key doesn't exist before inserting
- Enforced by transactional write to index key

## Cross-API Transactions

Because SQL and KV share the same transactional core, a single transaction can span both:

```rust
// Begin transaction (both SQL and KV)
let txn = db.begin().await?;

// Write to SQL table
txn.execute("INSERT INTO users (id, name) VALUES (42, 'Alice')").await?;

// Write to KV namespace
txn.kv_put("sessions", "sess_abc", session_data).await?;

// Commit both atomically
txn.commit().await?;
```

**What happens**:
```
Writes in transaction:
  - /table/users/42/name@t_w     → "Alice"
  - /kv/sessions/sess_abc@t_w    → session_data

Commit protocol:
  1. Coordinator assigns t_w from HLC
  2. Lock all keys (both SQL and KV)
  3. Check for conflicts
  4. Write all mutations with timestamp t_w
  5. Commit-wait until now() > t_w + ε
  6. Release locks, acknowledge client
```

**Guarantee**: Both writes commit or both abort. No partial commits. Both visible at the same timestamp `t_w`.

### Read-Write Cross-API Transaction

```rust
let txn = db.begin().await?;

// Read from KV
let config = txn.kv_get("configs", "app_config").await?;

// Use config to decide SQL write
if config.feature_enabled {
    txn.execute("INSERT INTO features (name) VALUES ('new_feature')").await?;
}

txn.commit().await?;
```

**Serialization**: The read from KV establishes a read timestamp. The SQL write must not conflict with any concurrent transaction. Standard MVCC conflict detection applies across both APIs.

## Cross-API Joins: The Killer Feature

No other database allows this: join SQL tables with KV namespaces in a single query.

### KV → SQL Join

**Scenario**: KV namespace `user_sessions` stores session blobs. SQL table `users` stores structured user data. Join them.

**API**:
```sql
SELECT
    u.name,
    u.email,
    kv_decode(s.value, 'last_active') AS last_active
FROM
    users u
    INNER JOIN KV('user_sessions') s ON s.key = u.id::TEXT
WHERE
    u.id IN (1, 2, 3);
```

**Key insight**: `KV(namespace)` is a virtual table with schema `(key BYTES, value BYTES)`.

**Execution plan**:
1. Scan `/table/users/*` for id IN (1,2,3) → rows `{id, name, email}`
2. For each row, lookup `/kv/user_sessions/{id}@t_r` → session blob
3. Decode `last_active` field from blob using `kv_decode()`
4. Return joined result

**Typed mapping**: `kv_decode(value, field)` extracts typed fields from KV blobs:
```sql
kv_decode(value, 'last_active')           → BIGINT (Unix timestamp)
kv_decode(value, 'user_agent')            → TEXT
kv_decode(value, 'ip_address', 'INET')    → INET type
```

Cloud9 supports schema-on-read: KV values can be JSON, MessagePack, Protobuf, etc. The decode function interprets bytes at query time.

### SQL → KV Join

**Scenario**: SQL table `orders` references KV namespace `product_catalog` (frequently updated, no schema).

**Query**:
```sql
SELECT
    o.order_id,
    o.quantity,
    kv_decode(p.value, 'name') AS product_name,
    kv_decode(p.value, 'price') AS product_price
FROM
    orders o
    INNER JOIN KV('product_catalog') p ON p.key = o.product_id
WHERE
    o.user_id = 42;
```

**Execution**:
1. Scan `/table/orders/*` for `user_id = 42`
2. For each order, lookup `/kv/product_catalog/{product_id}@t_r`
3. Decode product name and price
4. Return results

**Why this matters**: Product catalog can be updated via KV API (fast, no migrations), while orders use SQL (structured, constraints). Best of both worlds.

### Multi-Way Joins

**Query**:
```sql
SELECT
    u.name,
    o.order_id,
    kv_decode(p.value, 'name') AS product_name,
    kv_decode(s.value, 'status') AS session_status
FROM
    users u
    INNER JOIN orders o ON o.user_id = u.id
    INNER JOIN KV('product_catalog') p ON p.key = o.product_id
    LEFT JOIN KV('user_sessions') s ON s.key = u.id::TEXT
WHERE
    u.id = 42;
```

**Plan**: Standard join optimization. KV namespaces are just another relation. Optimizer can reorder, choose hash joins, nested loops, etc.

## Schema-on-Read for KV Namespaces

KV values are opaque bytes. No schema enforced. But SQL queries need types.

**Solution**: Schema-on-read with typed mappings.

### Mapping Declarations

**Define a mapping** (optional, improves query performance):
```sql
CREATE KV MAPPING product_catalog (
    key TEXT,
    value JSON (
        name TEXT,
        price DECIMAL,
        inventory INT,
        metadata JSON
    )
);
```

**Now query with type safety**:
```sql
SELECT
    key,
    value->>'name' AS name,
    CAST(value->>'price' AS DECIMAL) AS price
FROM
    KV('product_catalog')
WHERE
    CAST(value->>'inventory' AS INT) > 0;
```

**Benefits**:
- Planner knows types, can push filters
- No migration required (KV data unchanged)
- Multiple mappings can exist for same namespace (versioned schemas)

### Versioned Mappings

**Problem**: KV namespace schema evolves over time. Old and new formats coexist.

**Solution**: Versioned mappings with discriminator.

**Example**:
```sql
-- Version 1 (old format)
CREATE KV MAPPING product_catalog_v1 (
    key TEXT,
    value JSON (
        name TEXT,
        price_cents INT
    )
) WHERE value->>'version' = '1';

-- Version 2 (new format)
CREATE KV MAPPING product_catalog_v2 (
    key TEXT,
    value JSON (
        name TEXT,
        price DECIMAL,
        currency TEXT
    )
) WHERE value->>'version' = '2';

-- Union view
CREATE VIEW products AS
    SELECT key, value->>'name' AS name, value->>'price_cents'::INT / 100.0 AS price
    FROM KV('product_catalog')
    WHERE value->>'version' = '1'
  UNION ALL
    SELECT key, value->>'name' AS name, value->>'price'::DECIMAL AS price
    FROM KV('product_catalog')
    WHERE value->>'version' = '2';
```

**Query**:
```sql
SELECT * FROM products WHERE price > 10.00;
```

**Execution**: Planner knows to scan both mappings, normalize price, filter.

**No data migration needed**: Old and new formats coexist. Query layer unifies them.

## Transactional IR (TxIR): The Compilation Target

Both SQL and KV APIs compile to a common intermediate representation: **TxIR** (Transactional IR).

### TxIR Operations

```rust
enum TxIROperation {
    /// Read a single key at snapshot timestamp
    Get { key: Bytes, snapshot: Timestamp },

    /// Scan a key range at snapshot timestamp
    Scan { start: Bytes, end: Bytes, snapshot: Timestamp },

    /// Write a key-value pair (buffered until commit)
    Put { key: Bytes, value: Bytes },

    /// Delete a key (buffered until commit)
    Delete { key: Bytes },

    /// Check if key exists (for constraints)
    Exists { key: Bytes, snapshot: Timestamp },
}

struct TxIRPlan {
    operations: Vec<TxIROperation>,
    read_set: HashSet<Bytes>,
    write_set: HashMap<Bytes, Bytes>,
}
```

### SQL Compilation

**SQL**:
```sql
INSERT INTO users (id, name) VALUES (42, 'Alice');
```

**TxIR**:
```rust
TxIRPlan {
    operations: [
        // Check primary key doesn't exist
        Exists { key: b"/table/users/42/name", snapshot: t_r },

        // Write columns
        Put { key: b"/table/users/42/name", value: b"Alice" },
    ],
    read_set: { b"/table/users/42/name" },
    write_set: { b"/table/users/42/name" => b"Alice" },
}
```

**SQL**:
```sql
SELECT name FROM users WHERE id = 42;
```

**TxIR**:
```rust
TxIRPlan {
    operations: [
        Get { key: b"/table/users/42/name", snapshot: t_r },
    ],
    read_set: { b"/table/users/42/name" },
    write_set: {},
}
```

### KV Compilation

**KV**:
```rust
txn.kv_put("sessions", "sess_abc", session_data);
```

**TxIR**:
```rust
TxIRPlan {
    operations: [
        Put { key: b"/kv/sessions/sess_abc", value: session_data },
    ],
    read_set: {},
    write_set: { b"/kv/sessions/sess_abc" => session_data },
}
```

**KV**:
```rust
txn.kv_get("sessions", "sess_abc");
```

**TxIR**:
```rust
TxIRPlan {
    operations: [
        Get { key: b"/kv/sessions/sess_abc", snapshot: t_r },
    ],
    read_set: { b"/kv/sessions/sess_abc" },
    write_set: {},
}
```

### Cross-API Transaction Compilation

**Mixed transaction**:
```rust
let txn = db.begin().await?;
txn.execute("INSERT INTO users (id, name) VALUES (42, 'Alice')").await?;
txn.kv_put("sessions", "sess_abc", session_data).await?;
txn.commit().await?;
```

**Combined TxIR**:
```rust
TxIRPlan {
    operations: [
        // SQL INSERT
        Exists { key: b"/table/users/42/name", snapshot: t_r },
        Put { key: b"/table/users/42/name", value: b"Alice" },

        // KV PUT
        Put { key: b"/kv/sessions/sess_abc", value: session_data },
    ],
    read_set: { b"/table/users/42/name" },
    write_set: {
        b"/table/users/42/name" => b"Alice",
        b"/kv/sessions/sess_abc" => session_data,
    },
}
```

**Execution**: Transaction coordinator doesn't care whether operations came from SQL or KV. Just executes TxIR, locks keys, checks conflicts, commits.

## Transaction Coordinator: API-Agnostic

The coordinator implements standard MVCC + 2PL over TxIR:

```rust
struct TransactionCoordinator {
    txn_id: TxnID,
    read_timestamp: Timestamp,
    write_buffer: HashMap<Bytes, Bytes>,
    read_set: HashSet<Bytes>,
}

impl TransactionCoordinator {
    /// Execute a TxIR plan (from SQL or KV)
    async fn execute(&mut self, plan: TxIRPlan) -> Result<()> {
        for op in plan.operations {
            match op {
                TxIROperation::Get { key, snapshot } => {
                    let value = self.storage.get(&key, snapshot).await?;
                    self.read_set.insert(key);
                    // Return value to caller
                }
                TxIROperation::Put { key, value } => {
                    self.write_buffer.insert(key.clone(), value);
                }
                TxIROperation::Delete { key } => {
                    self.write_buffer.insert(key, TOMBSTONE);
                }
                // ... other operations
            }
        }
        Ok(())
    }

    /// Commit: acquire locks, check conflicts, write
    async fn commit(&mut self) -> Result<Timestamp> {
        let commit_ts = self.hlc.now();

        // 1. Acquire locks for write set
        self.lock_manager.acquire_locks(&self.write_buffer.keys()).await?;

        // 2. Validate read set (no writes since read_timestamp)
        for key in &self.read_set {
            let latest = self.storage.get_timestamp(key).await?;
            if latest > self.read_timestamp {
                return Err(Error::Conflict);
            }
        }

        // 3. Write all buffered mutations with commit_ts
        for (key, value) in &self.write_buffer {
            self.storage.put(key, value, commit_ts).await?;
        }

        // 4. Commit-wait
        self.commit_wait(commit_ts).await;

        // 5. Release locks
        self.lock_manager.release_locks(&self.write_buffer.keys()).await?;

        Ok(commit_ts)
    }
}
```

**Key point**: Coordinator has no notion of "SQL" vs "KV". Just byte strings and MVCC semantics.

## Why No Other Database Has Done This

### FoundationDB Came Close

**What FDB got right**:
- SQL (experimental) and KV share one transactional core
- Key-prefix-based namespacing
- ACID transactions span both APIs

**What FDB didn't do**:
- SQL layer was always experimental, never production-ready
- No cross-API joins (SQL couldn't query KV directly)
- No schema-on-read mappings for KV
- No Postgres wire compatibility

**Cloud9 completes the vision**: Production SQL (Postgres-compatible) + production KV + cross-API joins + unified transaction model.

### Why Others Failed

**Spanner**:
- SQL-only from the start
- No KV API exposed to users
- Google's internal use cases didn't need it

**CockroachDB**:
- Started SQL-only
- Tried adding KV via "system ranges" but abandoned it
- BSL license killed open experimentation

**YugabyteDB**:
- Has both YSQL (Postgres fork) and YCQL (Cassandra-like KV)
- But they're **separate systems**: different APIs, different consistency, can't mix in one transaction
- No unification

**DynamoDB, Cassandra, etc.**:
- KV-only, no SQL
- Adding SQL is bolting a query engine on top (Athena, Spark SQL)
- Not transactional unification

**Fauna**:
- Tries to unify with GraphQL + FQL
- But no Postgres compatibility, no raw KV API
- Different consistency model (Calvin-style)

### The Technical Barriers

**Why this is hard**:

1. **Key encoding conflicts**: SQL tables need structured keys (row/column). KV needs opaque keys. Most systems can't reconcile this.

2. **Query optimization**: SQL query planner needs to understand KV as a relation. This requires extending the optimizer.

3. **Type systems**: SQL is strongly typed. KV is untyped bytes. Bridging them requires schema-on-read with runtime type coercion.

4. **Transaction semantics**: SQL transactions use read/write locks. KV transactions often use optimistic concurrency. Unifying requires choosing one (Cloud9: MVCC + 2PL).

5. **Wire protocol**: Postgres wire protocol doesn't understand KV. Extending it without breaking clients is hard.

**Cloud9's approach**:
- Key encoding with clear prefixes (`/table/` vs `/kv/`)
- TxIR as compilation target (both APIs produce same IR)
- Schema-on-read with explicit mappings
- MVCC + 2PL as universal transaction model
- Extended Postgres protocol with `KV()` virtual table function

## Concrete Example: End-to-End Transaction

**Scenario**: E-commerce checkout. SQL for orders, KV for session and inventory cache.

```rust
let txn = db.begin().await?;

// 1. Check KV session is valid
let session = txn.kv_get("sessions", user_session_id).await?;
if session.is_expired() {
    return Err(Error::SessionExpired);
}

// 2. Read product from KV cache
let product = txn.kv_get("product_cache", product_id).await?;
let price = product.decode_field("price")?;

// 3. Insert SQL order
txn.execute(
    "INSERT INTO orders (user_id, product_id, price, quantity) VALUES ($1, $2, $3, $4)",
    &[&user_id, &product_id, &price, &quantity]
).await?;

// 4. Update KV inventory
let inventory = txn.kv_get("inventory", product_id).await?;
let new_inventory = inventory - quantity;
txn.kv_put("inventory", product_id, new_inventory.encode()).await?;

// 5. Commit atomically
txn.commit().await?;
```

**TxIR generated**:
```rust
TxIRPlan {
    operations: [
        // Step 1: Session check
        Get { key: b"/kv/sessions/sess_abc", snapshot: t_r },

        // Step 2: Product lookup
        Get { key: b"/kv/product_cache/prod_123", snapshot: t_r },

        // Step 3: Order insert
        Exists { key: b"/table/orders/{order_id}/user_id", snapshot: t_r },
        Put { key: b"/table/orders/{order_id}/user_id", value: encode(user_id) },
        Put { key: b"/table/orders/{order_id}/product_id", value: encode(product_id) },
        Put { key: b"/table/orders/{order_id}/price", value: encode(price) },
        Put { key: b"/table/orders/{order_id}/quantity", value: encode(quantity) },

        // Step 4: Inventory update
        Get { key: b"/kv/inventory/prod_123", snapshot: t_r },
        Put { key: b"/kv/inventory/prod_123", value: encode(new_inventory) },
    ],
    read_set: {
        b"/kv/sessions/sess_abc",
        b"/kv/product_cache/prod_123",
        b"/table/orders/{order_id}/user_id",
        b"/kv/inventory/prod_123",
    },
    write_set: {
        b"/table/orders/{order_id}/user_id" => encode(user_id),
        b"/table/orders/{order_id}/product_id" => encode(product_id),
        b"/table/orders/{order_id}/price" => encode(price),
        b"/table/orders/{order_id}/quantity" => encode(quantity),
        b"/kv/inventory/prod_123" => encode(new_inventory),
    },
}
```

**Commit protocol**:
1. Coordinator assigns `t_w = 1000` from HLC
2. Acquire locks on all write keys (both SQL and KV)
3. Validate read set: no key has version `> t_r` (no concurrent writes)
4. Write all mutations with version `t_w = 1000`
5. Replicate to quorum via Raft
6. Commit-wait until `now() > 1000 + ε`
7. Release locks, acknowledge client

**Guarantee**: All writes (SQL order + KV inventory) commit at `t_w = 1000` or all abort. No partial commit. Externally consistent.

## Performance Characteristics

### SQL Workloads

**Point reads**: Single key lookup (`/table/{name}/{pk}/{col}`)
- Same as traditional KV: O(1) with index

**Range scans**: Prefix iteration (`/table/{name}/{pk_start}/` to `/table/{name}/{pk_end}/`)
- Same as traditional SQL: O(log N + K) where K = rows returned

**Joins**: Standard join algorithms (nested loop, hash join, merge join)
- No overhead vs traditional SQL

### KV Workloads

**Point reads/writes**: Single key lookup/insert
- Same as dedicated KV stores

**Range scans**: Prefix iteration within namespace
- Same as dedicated KV stores

### Cross-API Joins

**Overhead**: Minimal if KV mapping is defined
- Planner knows types, can push filters
- Same execution as SQL-SQL joins

**Without mapping**: Schema-on-read at runtime
- Parse JSON/MessagePack/Protobuf per row
- Slower, but still correct

**Optimization**: Create mapping for hot namespaces

## Limitations and Trade-offs

### KV Values Are Opaque

**Implication**: Can't index into KV value fields without a mapping.

**Example**: Can't do `WHERE kv_decode(value, 'price') > 10` efficiently without a mapping that tells the planner how to extract `price`.

**Solution**: Create mapping for hot query patterns.

### No Column-Level Security on KV

SQL has column-level permissions (`GRANT SELECT (name) ON users TO role`). KV namespaces are key-value; no column concept.

**Workaround**: Use separate namespaces for sensitive data, control access at namespace level.

### Schema Evolution Requires Coordination

**SQL**: ALTER TABLE is a schema change, locks table.

**KV**: No schema, but mappings are versioned. Adding a new mapping doesn't lock data, but queries must handle multiple versions.

**Trade-off**: KV is more flexible (no locks), but requires application logic to handle versions.

## Future Enhancements

### KV → SQL Promotion

**Idea**: Start with KV namespace, promote to SQL table when schema stabilizes.

```sql
-- Promote KV namespace to table (inferred schema from mapping)
PROMOTE KV NAMESPACE product_catalog TO TABLE products;
```

**Effect**: Copies data, creates columns, drops KV namespace. Useful for prototyping.

### Automatic Mapping Inference

**Idea**: Analyze KV values, infer JSON schema, auto-create mapping.

```sql
ANALYZE KV NAMESPACE product_catalog;
-- Cloud9 samples values, infers { name: TEXT, price: DECIMAL, ... }
-- Creates mapping automatically
```

### Foreign Keys Across APIs

**Idea**: SQL foreign key can reference KV namespace.

```sql
ALTER TABLE orders
    ADD CONSTRAINT fk_product
    FOREIGN KEY (product_id)
    REFERENCES KV('product_catalog')(key);
```

**Challenge**: KV values can be deleted without SQL knowing. Requires trigger-like mechanism.

## Summary

**SQL and KV are unified in Cloud9 because**:

1. **Single key space**: Both are prefixes in the same MVCC storage
2. **Shared TxIR**: Both APIs compile to same transactional IR
3. **Cross-API transactions**: BEGIN spans both, commit atomically
4. **Cross-API joins**: KV namespaces are queryable as virtual tables
5. **Schema-on-read**: KV values get typed at query time via mappings
6. **One consistency model**: External consistency for both APIs

**Why this matters**:

- **Developers get flexibility**: Prototype with KV, harden with SQL
- **Operations get simplicity**: One database, one backup, one transaction log
- **Applications get correctness**: No data synchronization bugs between systems

**The killer feature**: Start a transaction, write to SQL, read from KV, commit atomically. No other database lets you do this.

**Cloud9 completes what FoundationDB started**: Production-ready SQL + KV unification with Postgres compatibility and external consistency.
