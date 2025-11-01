# Cloud9 Vision

**Cloud9 is the distributed database that should have existed from the start.**

Spanner proved that external consistency is achievable with commit-wait and precise time. FoundationDB proved that SQL and KV can share one transactional core. Postgres proved that full ACID with referential integrity is what developers expect. CockroachDB proved that you can build this in the open.

**Nobody combined them all.**

Cloud9 is the synthesis: Spanner's correctness + Postgres's compatibility + FoundationDB's architecture + open-source transparency. No corporate compromises. No vendor lock-in. No "this feature costs extra." Just the theoretically optimal distributed database, available to everyone.

## The Core Guarantee

**If you finish a write and start a read, that read sees the write. Anywhere in the world. Always. Provably.**

That's external consistency. It's a mathematical guarantee, proven with formal methods. The same guarantee Google uses for ads billing, where every cent must be accounted for.

## What Makes Cloud9 Unique

Every distributed database uses proven components (MVCC, Raft, HLC, commit-wait). None combine all of them with:
- SQL and KV unified under one transaction model
- True Postgres compatibility (foreign keys, triggers, constraints)
- Local-to-global deployment with the same binary
- MIT license with no vendor lock-in

**This isn't novel research—it's what distributed databases should have been from the start.**

Spanner proved the foundation (external consistency via commit-wait). FoundationDB proved the layering (SQL+KV over one transactional core). Postgres proved the interface (wire compatibility, full ACID).

Cloud9 is the **disciplined execution** of combining these proven principles into a coherent whole, without the compromises forced by corporate constraints:
- Spanner compromised: SQL-only, no foreign keys, proprietary, cloud-only
- CockroachDB compromised: SQL-only, then went proprietary (BSL)
- YugabyteDB compromised: SQL and KV exist but aren't unified
- DynamoDB compromised: KV-only, eventual consistency, no transactions

**Cloud9 makes no compromises.** External consistency + SQL + KV + open source + local-to-global.

## The Target

**You shouldn't need a Google-sized budget to get Google-class correctness.**

Cloud9 brings external consistency to:
- Students learning distributed systems
- Startups building the next platform
- Enterprises that need bulletproof data
- Developers who refuse to compromise on correctness

Whether you run it on a Raspberry Pi or across continents, the same database, the same guarantees, the same code.

**The daily driver database for the distributed era.**
