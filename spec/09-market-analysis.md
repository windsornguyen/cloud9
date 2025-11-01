# Market Analysis: Why Cloud9 Exists

Based on extensive user feedback from production deployments of Spanner, DynamoDB, and competing systems, several consistent themes emerge that Cloud9 is designed to address.

## Spanner Pain Points

### 1. Cost and Pricing Model

**The Problem**:
- Minimum $65/month (often $1000+/month for production)
- No on-demand pricing — must provision nodes for peak throughput
- Average throughput << peak throughput = wasted spend
- "Half the cost of DynamoDB" marketing ignores provisioning overhead

**User quote**: *"We had a huge spanner db with low throughput so had to add idle nodes just for storage which also ballooned costs."*

**User quote**: *"We were paying tens of thousands of dollars a month for Spanner plus tens of thousands of dollars a month for all the compute sitting in front of it."*

**Cloud9 answer**: Serverless on-demand pricing (pay per operation), plus self-hostable open-source option.

### 2. GCP Platform Instability

**The Problem**:
- Constant version churn and breaking changes
- Undocumented features and performance gotchas
- Services deprecated without warning (Google Domains → Squarespace)
- Fear of product cancellation ("Will Spanner be shut down?")

**User quote**: *"Google runs their tech stack as if it's a startup that builds their CV. Everything is immature, tons of hacks, undocumented features."*

**User quote**: *"Much of the time GCP feels like a science project, and not a real business."*

**Cloud9 answer**: Open-source MIT license. Code can never be "shut down" by vendor. Community-driven development with stability guarantees.

### 3. Support Quality

**The Problem**:
- Support ranges from "unhelpful" to "non-existent"
- Escalations go nowhere
- Bug reports closed as stale without resolution
- Sales team doesn't understand enterprise needs

**User quote**: *"Google's support is horrendous. They refer you to idiots that drag you through calls until your will for life dies."*

**User quote**: *"We have a bug reported back in 2020 that got closed recently without any action because it became stale."*

**User quote**: *"GCP support would suggest to ask in StackOverflow."*

**Cloud9 answer**: Community-driven support via GitHub Issues/Discussions. No support tax, no gatekeepers. Open development process.

### 4. Documentation Gaps

**The Problem**:
- Performance characteristics poorly documented
- Sharding/partition behavior not explained clearly
- "Hot shard" problems discovered at scale
- No clear migration guides

**User quote**: *"Google's docs are incomplete; there are lots of performance gotchas that exist throughout the entire service, and they aren't clearly documented."*

**Cloud9 answer**: Comprehensive documentation from day one. Open-source allows reading the implementation. Design notes explain trade-offs.

### 5. Operational Complexity

**The Problem**:
- GKE constant version churn forces infrastructure rework
- Network configuration complex (vs AWS/GCP VPC simplicity)
- Hidden costs (discovered $6k database in bill)
- Requires constant vigilance for breaking changes

**User quote**: *"50% of time making sure we are prepared for their shit and 50% our ambitious infra plans."*

**Cloud9 answer**: Single binary, minimal operational surface. Works identically local and global. No platform lock-in.

## DynamoDB Pain Points

### 1. Data Model Limitations

**The Problem**:
- Key-value only, no SQL
- Must design access patterns upfront
- No ad-hoc queries or joins
- Single-table design patterns are complex

**User quote**: *"DynamoDB is fantastic for not doing things at scale... an entire RDBMS is way overkill for."*

**Cloud9 answer**: Both SQL and KV. Cross-API joins. Familiar relational model when needed.

### 2. Capacity Planning Gotchas

**The Problem**:
- Hot partition issues (1000 WRU/partition limit)
- Shards get same quota regardless of traffic distribution
- Over-provision or queue requests to handle hot keys
- Not obvious from documentation

**User quote**: *"Even though you might have paid for 1000rps, that RPS volume is divided across all your shards."*

**Cloud9 answer**: Transparent sharding with automatic rebalancing. Cross-shard transactions at same consistency level.

### 3. No Multi-Item Transactions

**The Problem**:
- TransactWriteItems limited to 25 items
- No true ACID across arbitrary keys
- Application must handle consistency

**Cloud9 answer**: Unbounded multi-key transactions with strict serializability.

## Common Theme: Trust and Lock-In

**Observation**: Users fear vendor lock-in more than they fear technical limitations.

**User quote**: *"Doing business with Google is a liability."*

**User quote**: *"I trust AWS to be a stable, long term foundation to build a product on, I don't trust GCP to be the same."*

**User quote**: *"Why am I going to sign up for a service that is surely to be canceled on a Google Whim™?"*

**Cloud9's fundamental answer**:
- Open-source MIT license removes vendor lock-in
- Self-hostable on any infrastructure
- Managed Dedalus Cloud offering for convenience, not lock-in
- Community can fork if Dedalus Labs disappears

## The Postgres Refuge

**Observation**: Many threads conclude "just use Postgres" because it's:
- Well-understood and stable
- Not vendor-locked
- Good enough for 99% of use cases

**User quote**: *"Postgres is a piece of software. Cloud Spanner/Dynamo etc are managed services. It makes no sense to directly compare."*

**User quote**: *"Golden Rule of data: Use PostgreSQL unless you have an extremely good reason not to."*

**Cloud9's position**: Be the **Postgres of distributed databases**:
- Open, trusted, boring technology
- Postgres wire compatibility
- Clear documentation and predictable behavior
- Available when you outgrow single-node Postgres

## Specific Technical Complaints

**Spanner**:
- DeWitt clause prevents independent benchmarking
- No protobuf column support in Cloud Spanner (only internal Spanner)
- Unclear whether Google services use Cloud Spanner or internal Spanner
- Write-through cache needed for read-heavy workloads (complexity + cost)

**DynamoDB**:
- Item size limits (400 KB max)
- Read/write unit calculations opaque (1 byte over 1KB = 2 RU charged)
- Connection management nightmare with Lambda/serverless

**Both**:
- Difficult to meaningfully compare offerings and value
- Lock-in makes switching costs prohibitive
- Enterprise architects push them for imagined scale needs

## Additional Insights from Developer Communities

### Spanner Positioning Problem

**The Problem**:
- "Overkill for prototypes" — minimum cost too high for experimentation
- "Mosquito with a sledgehammer" — power users don't need, small users can't afford
- Recommendation is always "use Cloud SQL instead" — Spanner's own ecosystem recommends against it

**User quote**: *"Spanner is pricey - do you need that scale/availability? Cloud SQL would be more your speed."*

**User quote**: *"Spanner is a mosquito with a sledgehammer for most workloads."*

**User quote**: *"I'd say stick with cloud SQL for prototyping, Spanner is for production."*

**Implication**: Spanner has no **"grow into it"** story. You can't start small and scale up — the entry point is already enterprise-scale pricing.

**Cloud9 answer**: Start local (SQLite-level simplicity), scale to regional, scale to global — same binary, same semantics. No cliff between "prototype" and "production."

### The Postgres Gravitational Pull

**The Problem**:
- Every Spanner discussion ends with "just use Postgres"
- Postgres-compatible offerings (AlloyDB, Cloud SQL) recommended over Spanner
- Even Google's own advocates suggest Postgres alternatives

**User quote**: *"Please, do Postgres, not MySQL. Let it die already."*

**User quote**: *"If you can sling postgres I'd go straight to alloydb."*

**Cloud9 answer**: Postgres wire compatibility from day one. Be where developers already are, not where they have to migrate to.

### Developer Experience Friction

**The Problem**:
- No local development story ("can't install software on desktop")
- Cloud-only development is clunky (Cloud Shell, Cloud Editor)
- No emulator for cost-controlled local dev (unlike AlloyDB)
- Forces developers into specific GCP workflows
- Missing features vs Postgres (no stored procs, no ts_vector, limited data types)

**User quote**: *"Developing in the cloud is possible. If you go to cloud shell you can open a cloud version of vscode. Haven't used it much so not sure how well it works."*

**User quote**: *"Spanner [lacks] auto increment counters, ts_vector as type and a bunch more."*

**User quote**: *"Still no support for user-created stored functions/stored procs."*

**Cloud9 answer**: Single binary runs locally. Develop on laptop, deploy to cloud without changes. No forced cloud-development workflow. Full Postgres compatibility from day one.

## What Users Actually Want

**Synthesis from discussion**:

1. **Predictable, transparent pricing** — no surprise bills, no forced provisioning
2. **Stability and trust** — won't be deprecated, won't see 10x price increases
3. **Good enough for small scale, grows to large** — DynamoDB's free tier vs Spanner's $65/month floor
4. **Familiar interfaces** — SQL preferred, KV when needed
5. **Open and portable** — can leave vendor without rewrite
6. **Real support** — responsive humans who understand the problem
7. **Clear documentation** — performance characteristics, limits, gotchas all documented upfront
8. **Local development** — prototype locally, deploy globally without workflow changes

**Cloud9's design targets all eight points.**

## The Billing Horror Stories

**The most damaging feedback**: Silent, unexpected charges that destroy user trust.

### The RAG Engine Incident (September 2024)

**What happened**:
- Google changed RAG Engine backend to use Spanner (Scaled Tier, 1000 PU)
- **No clear notification** to affected users (some got email, many didn't)
- Users who tried RAG Engine once got charged $30-800/day
- Charges appeared as "Cloud Spanner" even though users never enabled Spanner
- Spanner instances didn't show up in Spanner console (hidden)
- **Auto-provisioned in ALL regions** (US + EU) per project

**User quote**: *"$30/day for a service I didn't knowingly use seems extremely expensive. I deleted all my projects to make sure no keys were leaked."*

**User quote**: *"$300 for me 😭"*

**User quote**: *"Another victim here. $800 gone."*

**User quote**: *"What Google is doing on this one is, frankly, appalling. It's theft."*

**User quote**: *"I had bit faith in GCP to convince my company to switch from Azure. Now no way I can/will recommend anyone to use GCP."*

**The worst case** (£3,000 / $3,800 in one month):
- Dormant account (£0.02/month residual storage)
- Sept 3: charges spike to £60-70/day
- User never created RAG corpus, never uploaded data
- RAG UI shows nothing
- **Billing account frozen** → can't access account to delete resources
- **Catch-22**: Must pay disputed balance to unlock account to stop charges
- Support closes tickets, refuses escalation
- Balance climbing daily with no way to stop

**User quote**: *"The catch-22: billing suspension prevents me from accessing my account to delete the service/close the account, but Google says I must pay the disputed balance first to unlock it."*

**User quote**: *"Support has closed my tickets multiple times, refuses to escalate further, and won't deprovision the hidden resources."*

**Google's response**: *"After final review, charges are valid. These were provisioned as a necessary component of the Vertex AI RAG Engine service you activated... charges are considered legitimate."*

**User's dilemma**: *"Can I just refuse to pay? What happens if it goes to debt collections?"*

**The resolution process**:
- 2+ hour wait times for support
- Users had to manually delete RAG Engine (not obvious)
- Must delete **per region** (auto-enabled in multiple regions)
- Some got 90% refund as "one-time courtesy"
- Many charged for weeks before noticing
- Some accounts frozen, unable to delete resources
- Documentation says "free to use" but hides $2k/month Spanner cost

**User quote**: *"RAG Engine docs say it's 'free to use' but fail to mention the auto-provisioned Spanner instance costs £2k+/month."*

**User quote**: *"Those RAG Engine Cloud Spanner services were automatically enabled for ALL available regions and for each individual project. I needed to delete them one by one."*

**User quote**: *"GCP assistant is essentially useless and I couldn't find any live chat support... Wasted half my morning on this crap."*

**User quote**: *"Very poor customer communication."*

**User quote**: *"GCP will definitely beat estimates now LOL. Some MBA wearing a vest will get a big bonus in exchange for all the misery."*

**Separate incident** (2025): Gemini 2.5 Flash billing error generated **$70,000+ bills** for non-existent usage, charges climbing $10,000/day even after API keys deleted.

### The Pattern

1. Service defaults change silently
2. Expensive resources provisioned automatically
3. Poor visibility (instances don't show in expected console)
4. Slow/unhelpful support response
5. Users lose trust permanently

**User response to billing disasters**: Migration away from GCP entirely.

**User quote**: *"Time to migrate. I can host my rag setup on my vps that I pay $12 a month for."*

**The exodus**: Users leave GCP for **$12/month VPS** rather than pay surprise Spanner bills.

**Cloud9's commitment**:
- **No silent defaults** — explicit opt-in for all paid tiers
- **Visible resource usage** — every replica, every shard visible in console
- **Billing transparency** — real-time cost tracking, no surprises
- **Zero-cost local mode** — develop and test for free
- **Community support** — no support tax, no wait queues
- **Self-hosting option** — run on $12/month VPS if desired, same guarantees

## Pricing Transparency Issues

**The "Basic vs Standard" confusion**:
- Documentation shows different tiers in different places
- Support agents have access to different pricing calculators
- Processing Units (PU) pricing not obvious
- "Wind down when not using" not possible (always-on charges)

**User quote**: *"Documentation appears to be inconsistent. Some suggest there is a 'basic' tier, but when you go to the estimate page, it starts with 'Standard'."*

**User quote**: *"Does anyone know how to lower the costs when you're in Dev mode? Is there a way to wind down the environment when you're not using it?"*

**Answer from community**: No. Spanner charges for provisioned capacity, not usage.

**Cloud9 answer**:
- Open-source = free local development
- Managed tier pricing published upfront
- Can "wind down" by stopping the binary (self-hosted)
- Pay-per-operation option (no idle charges)

## Early Adoption Concerns (2017 Launch)

**From initial Spanner launch discussions**:

### Understanding Barrier

**The Problem**:
- Complex architecture hard to explain
- TrueTime concept not intuitive
- Users struggled to understand when Spanner is needed vs Postgres
- "Shitty article" complaints (marketing-heavy, light on substance)

**User quote**: *"That was a really shitty article. Can anyone explain the real world benefits of this?"*

**User quote**: *"All that babel about TrueTime and nowhere a description of the problem it solves."*

**Cloud9 answer**: Clear design notes (this document) explain trade-offs. No hand-waving about "mastering time."

### Schema Modeling Constraints

**The Problem**:
- No explicit foreign keys outside parent/child relationships
- No multi-parent tables (can't model true many-to-many easily)
- Must choose access patterns upfront (parent/child determines sharding)
- No referential integrity constraints across tables
- No triggers
- No reference types for columns

**User quote**: *"How does a Many-to-Many relationship work? Are they all root level tables? Is there no explicit foreign keys?"*

**User quote**: *"You cannot put one table as a child of two others."*

**User quote**: *"There are also no triggers, and no reference types (so you can't define a column in BankAccount as type 'key of Bank')."*

**Answer from community**: *"No cross-table referential integrity constraints... implementing them would be costly in terms of latency."*

**The ACID debate**: Users argue Spanner isn't truly ACID because it lacks the "C" (consistency via constraints).

**User quote**: *"Spanner is not ACID. It's AID. It lacks the C of 'the data in the schema conforms to the business rules.' If you don't have foreign keys, triggers, range limitations, you don't have C."*

**Google's defense**: *"ACID for Spanner means the consistency rules definable for Spanner's not-really-an-RDBMS model are upheld."*

**Cloud9 answer**: Standard SQL foreign keys, constraints, and triggers. Postgres compatibility means familiar schema modeling. True ACID with full referential integrity.

### Trust in Complexity

**The Problem**:
- Skepticism about needing atomic clocks
- "Why not just use Postgres?" dominates discussion
- Benefits unclear until extreme scale
- GPS/atomic clock dependency seems fragile

**User quote**: *"If some muppet decides to mess with GPS signals near the datacenter, what happens?"*

**User quote**: *"Almost no applications have important use cases that make such a solution a requirement for success."*

**Answer from community**: *"Uses 6 time masters. 3 GPS clocks with individual antennas, 3 atomic clocks. Kalman filter rejects bad GPS, falls back to atomic."*

**Cloud9 answer**:
- Works without atomic clocks (HLC on commodity hardware)
- Clear failure modes documented
- Benefits obvious from prototype to production (same binary)

## The "Just Use Postgres" Reflex

**Consistent theme across all discussions**: Default to Postgres unless you absolutely can't.

**User quote**: *"Just use PG...until you can't."*

**User quote**: *"Are you sure your data doesn't fit in PostgreSQL? You should probably try PostgreSQL first."*

**User quote**: *"The super-power of Postgres is that it supports everything... doesn't suck at anything but horizontal scaling."*

**The Spanner problem**: No story for "start with Postgres, grow into Spanner." It's a hard cut-over.

**Cloud9's answer**:
- **Is** Postgres for small scale (wire-compatible, single binary)
- Grows to Spanner-class scale without migration
- No "Postgres vs Spanner" decision — it's both

## The "Overkill for Real Workloads" Pattern

**Recurring scenario**: Users evaluate Spanner for moderate scale, realize it's massive overkill.

**Real case** (9 months ago):
- 500 requests/second (read-heavy)
- 50 GB data
- Public API, no auth
- Looking to replace Firestore (query limitations)

**User calculations**: "200 processing units handles 15k QPS... I doubt it."

**Community response**:

**User quote**: *"Are you stupidly rich? Like the lost son of Sultan of Brunei? No? Then it's too expensive, consider other options."*

**User quote**: *"Sounds like bringing in a tank into a boxing fight."*

**User quote**: *"500 requests per second is not that much honestly. Spanner... is designed for much higher throughput."*

**User quote**: *"A lot of over-engineered tech choices are sometimes to compensate for lack of applying fundamentals with simpler and cheaper alternatives."*

**Googler's response**: Spanner = $146/month, Cloud SQL = $231/month (Spanner cheaper!)

**User's conclusion**: *"After more digging into the subject I realize I don't need it."*

### The Disconnect

**Google's pitch**: "Spanner is cheaper than Postgres!"

**Reality**:
- Users still choose Postgres
- Not because of cost
- Because Spanner feels wrong for the scale
- "PostgreSQL enters the chat" (final comment)

**Why this matters**:
- Even when Spanner is **cheaper**, users reject it
- The "overkill" perception is **psychological**, not economic
- Users want technology that feels appropriate to their scale
- Spanner positioned as "big company tech" → small companies avoid it

**Cloud9's advantage**:
- Same binary from prototype (500 RPS) to massive scale (500k RPS)
- No psychological barrier
- "Just use Cloud9" → natural default like "just use Postgres"
- Pricing scales with you (free → cheap → expensive as you grow)

## Cloud SQL Performance Issues

**Reported problems** with Google's managed Postgres/MySQL:

### Performance Degradation

**The Problem**:
- CloudSQL slower than self-hosted on VMs
- Read locking happens frequently
- Replication lag even within same zone
- Trigger execution delays on replicas

**User quote**: *"My experience with CloudSQL was horrendous. It was slow and read locking happened ridiculously often. Once I spinned up a MySQL instance on a VM, everything worked flawlessly."*

**User quote**: *"We had significant slowness with cloudsql and moved to managing our own instances on VMs and haven't looked back."*

**User quote**: *"We've seen significant delay in some replicas (in the same zone) for some more complex triggers."*

**The irony**: Users migrate to Spanner not because they need global distribution, but because **CloudSQL is unreliable**.

### The "Fast Reads" Trap

**User's goal**: "Need the database to be highly available and never waiting on locks... guaranteeing fast reads all the time."

**Community response**: Spanner doesn't solve this.
- Spanner still uses locks for read-write transactions
- Lock-free read-only transactions exist, but user may not know to use them
- "CloudSpanner would be a way to have GCP manage everything about scaling" (wrong expectation)

**User quote**: *"CloudSpanner starts to make sense once your DB is larger than 10TB and you need replication across the whole planet."*

**User's realization**: *"This is not my challenge, but rather guaranteeing fast reads all the time."*

**Recommendation given**: CloudSQL with read replicas (back to where they started).

**Cloud9 answer**:
- Lock-free read-only transactions by default (documented clearly)
- MVCC means readers never block writers
- Works locally for testing before cloud deployment
- No CloudSQL performance issues (you control the hardware)

## The Expectation Mismatch

**Pattern observed**:
1. User has performance issues with CloudSQL
2. User investigates Spanner as "better managed database"
3. Community asks: "Do you have billions of dollars?"
4. User realizes Spanner solves different problem
5. User sent back to CloudSQL or self-hosting

**The gap**: No managed database between "CloudSQL (unreliable)" and "Spanner (overkill)".

**Quote**: *"Do you have billions of dollars? [No] That would be awesome lol - but no."*

**Cloud9's positioning**:
- Fills the gap between CloudSQL and Spanner
- Self-hostable (control your own performance)
- OR managed tier (Dedalus Cloud)
- Same guarantees at all scales
- No "do you have billions?" barrier

## The Missing Middle

**Synthesis of all observations**:

There is a massive gap in the market between:
- **Postgres/MySQL** (single-node, no horizontal scale)
- **Spanner/DynamoDB** (enterprise-only, high cost floor, vendor lock-in)

Users in this gap need:
- Multi-region capability (not planet-scale, but 2-3 regions)
- ACID transactions (not just eventual consistency)
- Familiar SQL interface (not KV-only)
- Reasonable cost (not $1000+/month minimum)
- Trust and portability (not vendor lock-in)

**Cloud9 is built specifically for this missing middle**:
- Start on laptop (zero cost)
- Deploy to 3 regions (reasonable cost)
- Scale to planet-scale (enterprise cost, but optional)
- Open-source MIT (never locked in)
- Postgres-compatible (familiar interface)

## Summary: The Market Opportunity

Cloud9 exists because the current landscape forces users into false choices:

1. **Cost vs Scale**: Pay enterprise prices from day one, or stay single-node forever
2. **Lock-in vs Power**: Accept vendor control, or give up distributed features
3. **Simplicity vs Capability**: Use simple database or learn proprietary API
4. **Local vs Global**: Develop in cloud or deploy single-node

**Cloud9 eliminates all four false choices**:
- Cost scales with usage (free local → expensive global)
- Open-source removes lock-in without sacrificing power
- Postgres compatibility gives capability without learning curve
- Same binary works local and global (no development gap)

The market doesn't need another distributed database. It needs a distributed database that acts like Postgres: boring, predictable, trusted, and available when you need it.

**That's Cloud9.**
