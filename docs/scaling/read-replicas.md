# Read Replicas — design

Status: design / skeleton. v0.2 ships the single-region follower path; v0.3
extends to multi-region. As of 2026-05-11 the runtime has only single-writer
shards; this doc describes the read-scale story and the trait skeleton that
lives in `crates/basin-shard/src/follower.rs`.

## 1. Current state (v0.1)

The shard owner is a single-writer-per-shard process. Every
`(project, partition)` writes through one `Arc<RwLock<PartitionState>>` on
exactly one machine. The write path is:

1. `ProjectHandle::write_batch` encodes `(table, batch)` into the WAL payload
   format documented in `basin-shard/src/in_process.rs`.
2. `Wal::append` returns once the entry is durable (LocalWal: file fsync +
   queued upload; RaftWal v0.2: quorum ack).
3. The shard pushes `(lsn, batch)` onto an in-memory tail keyed by table.
4. The background compactor drains the tail into Parquet via
   `basin-storage`, commits through `basin-catalog`, and truncates the WAL.

Reads land on the same process. `ProjectHandle::read` streams the Parquet
base from storage, then appends the in-memory tail. There is no follower
today — read traffic competes with write traffic for the single per-shard
runtime, and the only horizontal axis is sharding (one project on one node).

Why this is the bottleneck the doc fixes: single-writer is the right
serialization model for correctness (LSN ordering, commit-conflict
retries, tail vs Parquet snapshot semantics), but it caps read throughput
at one machine's CPU + IO budget. Read scale = follower replicas that
consume the WAL and serve identical reads.

## 2. Architecture

### 2.1 Roles

Each `(project, partition)` ("shard") has exactly one **leader** and zero
or more **followers**:

- **Leader**: today's `InProcessShard`. Owns writes, owns the catalog
  commit, owns WAL truncation. Identical to current behaviour.
- **Follower**: a new `FollowerShard` that consumes the WAL produced by
  its leader, applies each entry to its own in-memory state (same
  `PartitionState` shape as the leader), and serves reads. Followers
  never write to the WAL and never commit to the catalog.

The follower's view of the world is **read-only** and **lag-bounded**.
It reads Parquet through the same `basin-storage` API as the leader
(stateless — both see the same S3-compatible bucket objects via the catalog), and
re-builds the in-memory tail by replaying WAL entries with LSN strictly
greater than the leader's `last_compacted_lsn` at any given moment.

### 2.2 WAL streaming

The leader-side WAL (`crates/basin-wal`) already exposes
`read_from(project, partition, since_lsn)` — this is the catchup primitive
for cold-load today. Followers reuse it; the addition is a streaming
variant:

```rust
// New on the Wal trait (default impl in terms of read_from + poll).
async fn tail(
    &self,
    project: &ProjectId,
    partition: &PartitionKey,
    since_lsn: Lsn,
) -> Result<Pin<Box<dyn Stream<Item = Result<WalEntry>> + Send>>>;
```

Followers open one `tail` stream per resident `(project, partition)` and
apply entries in LSN order. The streaming impl can be:

- **v0.2 local FS / single region**: a poll loop on top of `read_from`,
  bounded by `flush_interval` (200 ms today). Sufficient when the
  leader's WAL backing object store is the same bucket the followers
  read from.
- **v0.2.x optimisation**: leader pushes new segments to a fanout
  channel (Redis stream / NATS / Fly machine→machine TCP) so followers
  don't pay the listing cost. Out of scope for the initial PR.
- **v0.3 multi-region**: regional buckets with cross-region
  replication, plus optional WAL shipping over Fly's private network.

### 2.3 Follower lag SLA

Each follower exposes `current_lsn()` (highest LSN it has applied) and
`is_caught_up(threshold)` (true when `leader_high_water - current_lsn`
is within `threshold`). The router uses these to decide whether a
follower is eligible to serve a given read.

Default SLAs:

| tier | freshness target | implementation |
|------|------------------|----------------|
| `Eventual` (default) | within 5 s of leader | follower picked round-robin from all `is_caught_up(5s)` followers |
| `Bounded(n)` | within `n` ms of leader | filtered list; falls through to leader if none qualify |
| `ReadYourWrites` | client's last-seen LSN ≤ follower's `current_lsn` | client sticks an `x-basin-rylw-lsn` header (or pgwire parameter) and the router picks a follower or leader that satisfies it |
| `Strong` | leader only | always leader; for SELECTs in a write txn |

The `Strong` and `ReadYourWrites` tiers compose with the existing
single-writer guarantees; `Eventual` is the throughput win.

### 2.4 Read-your-writes

The leader returns the `Lsn` it just appended to the client (via a
pgwire `NoticeResponse` or — more usefully — a basin-rest response
header `x-basin-lsn`). Clients echo this back on subsequent reads. Pure
clients that don't echo get `Eventual`; SDKs that do (basin-js) get
read-your-writes for free by default.

This means `basin-js` adds two lines to `BasinClient`:

- record `lastSeenLsn` on every successful mutation response.
- send `lastSeenLsn` on every subsequent read.

That gives PostgREST-style apps read-your-writes correctness through
the follower fleet without an explicit user knob.

## 3. Routing

`basin-router` today picks a shard endpoint with stable hashing of
`ProjectId`. With followers in the picture, the unit of routing
changes from "one endpoint per project" to "one **set** of endpoints
per project, tagged leader/follower".

### 3.1 New shape

`ShardMap` grows from `Vec<endpoint>` to a small struct per slot:

```rust
pub struct ShardSlot {
    pub leader: String,            // pgwire endpoint
    pub followers: Vec<Follower>,  // (endpoint, last-known lag)
}
pub struct Follower { pub endpoint: String, pub lag_ms: u64 }
```

Selection per connection:

1. Resolve `ProjectId → ShardSlot`.
2. Parse intent from the SQL (or from a sidechannel — see below).
   - INSERT / UPDATE / DELETE / DDL / `BEGIN`-in-flight → `leader`.
   - SELECT with no open txn → eligible for `followers`.
3. From eligible followers, pick by load-balance policy:
   - **round-robin** (default v0.2)
   - **weighted by lag** (v0.2.1, `1/(lag_ms+1)` as weight)
   - **session-pinned** (sticky to one follower for the connection's
     lifetime, to reduce schema-cache churn).

### 3.2 Where the intent comes from

Two channels, used in this order:

1. **Pgwire**: the simple-query handler already has a parsed AST by
   the time it picks a session. A new `QueryIntent::ReadOnly` flag is
   the natural place to set this; the parser already separates SELECT
   from DML for the rate-limit path.
2. **basin-rest**: the HTTP method already tells us — GET → read,
   everything else → write. The `x-basin-rylw-lsn` header overrides
   the default if present.

For the pgwire path we keep the existing project→shard hash for the
*leader* (so leader pinning is consistent with v0.1 cluster) and
deterministically derive the follower set for that project from
`basin-placement`.

### 3.3 Failover at the router edge

When a follower returns a connection error mid-query the router:

1. Marks the follower as down for `follower_down_cooldown` (default
   2 s).
2. Retries the query against another eligible follower if any.
3. Falls back to the leader as last resort.

This is identical to the existing pgwire client-reconnect pattern, just
at a finer granularity than project.

## 4. Placement

`basin-placement` becomes the source of truth for "shard X has leader
on machine A and followers on B, C". Today the crate is essentially
empty (Phase 3); this is where it gets shape.

### 4.1 Data model

```rust
pub struct ShardLayout {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub leader: MachineId,
    pub followers: Vec<MachineId>,
    pub generation: u64, // bumped on every failover; routers cache by gen
}
```

Persisted in a strongly-consistent store. v0.2 candidates: Postgres
advisory locks (already deployed for the catalog), or FoundationDB. The
choice is deferred to the v0.2 placement ADR.

### 4.2 Replication factor

- v0.2 single-region default: RF=2 (1 leader + 1 follower) — survives a
  Fly machine restart with bounded reader downtime.
- v0.2 high-read default: RF=3 (1 leader + 2 followers).
- v0.3 multi-region: RF=3 across two regions (leader + local follower +
  remote follower).

### 4.3 Promotion / failover

When the leader machine dies:

1. Fly Machines health check fires.
2. basin-placement detects via lease expiry (the leader holds a TTL
   lease on the layout row; failure to renew = vacated).
3. A follower with the highest `current_lsn` is promoted. The
   promotion is a catalog write: bump `generation`, set
   `leader = promoted_follower`, demote the dead machine to a tombstone
   in `followers`.
4. Routers refresh on next request. In-flight pgwire connections to
   the dead leader hit the failover path described in 3.3.

A promoted follower must reconcile any uncommitted tail before
accepting writes. Concretely: re-read from WAL up to its known
high-water, take ownership of `truncate` and `append`, then start
accepting writes. The WAL trait's `high_water` + `truncate` already
support this — promotion is "follower drops its read-only flag" plus
"new leader's WAL handle is the same handle followers were already
tailing".

### 4.4 Multi-region (v0.3)

Two design points beyond single-region:

- **WAL shipping latency**: cross-region object-store replication is on the
  order of seconds. Multi-region followers therefore default to the
  `Eventual` lag tier with `≤ 30 s` rather than `≤ 5 s`. Clients that
  need read-your-writes either route to leader region or run a basin-cli
  command that waits for the regional follower to catch up.
- **Region failover**: full region loss promotes a remote follower.
  Cross-region promotion is opt-in per project (some users prefer to
  stay write-unavailable rather than risk a longer lag window before
  serving writes from a region that may be behind).

## 5. Concrete trait additions in basin-shard

The skeleton lives at `crates/basin-shard/src/follower.rs`. Public
surface that lands in v0.2:

```rust
#[async_trait]
pub trait ShardFollower: Send + Sync {
    /// Apply one WAL record to the follower's state. Returns the new
    /// `current_lsn` after apply.
    async fn apply_wal_record(&self, entry: &WalEntry) -> Result<Lsn>;

    /// Highest LSN this follower has applied. Stable across calls
    /// from the same task once they observe a given record.
    async fn current_lsn(&self) -> Lsn;

    /// Cheap predicate the router consults on each read. True iff
    /// `(leader_high_water - current_lsn) <= threshold`.
    async fn is_caught_up(&self, threshold: Duration) -> bool;

    /// Read-only path. Identical surface to `ProjectHandle::read`.
    async fn read(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>>;
}
```

Plus a `FollowerShard` struct that owns:

- a `basin-storage::Storage` handle (same as the leader; storage is
  shared via the object store).
- an `Arc<dyn Wal>` it tails (read-only side of the same WAL the
  leader appends to).
- the per-partition `Arc<RwLock<PartitionState>>` map, mirroring
  `InProcessShard`'s state but with no writer side.
- a `WalTailer` background task: `tail` stream → `apply_wal_record`
  loop, one per resident partition.

A new `ReplicaRole::{Leader, Follower}` enum lets the same
`Shard::new` accept either role. The leader keeps today's behaviour
verbatim.

## 6. Phasing

### v0.2 — single region followers

- `ShardFollower` trait + `FollowerShard` impl in basin-shard.
- `Wal::tail` streaming impl on `LocalWal`.
- `basin-placement::ShardLayout` model + lease-based leader election.
- `basin-router::ShardMap` grows leader/follower split.
- Lag-tier API (`Eventual` / `Bounded` / `ReadYourWrites` / `Strong`).
- Fly-deployed RF=2 default for the cloud-managed offering.

### v0.3 — multi-region followers

- Cross-region WAL shipping (bucket replication or Fly machine→machine).
- Region-aware router (prefer local region's followers; fall back to
  remote followers; fall back to leader region).
- Opt-in cross-region promotion.
- Per-project overrides for "always read from leader region" (low-lag
  freshness customers) and "RF=5 across three regions" (read-heavy
  globally distributed workloads).

### Out of scope for both

- Synchronous replication (every write waits for follower ack). Not
  needed for the read-scale story; the WAL already gives us durable
  writes without it.
- Per-table replication policy. Today the unit is the partition; per-
  table follower selection waits for the catalog to grow a
  per-table replication-class column.

## 7. Open questions

- **WAL fanout substrate**: polling the object store works for v0.2 but adds
  200 ms to the lower bound on follower lag. A leader-push channel (Fly
  machine→machine TCP, or NATS) gets us to ≤ 50 ms. Decision deferred
  until we measure polling in dev.
- **Snapshot install for new followers**: a fresh follower replaying
  from `Lsn::ZERO` against a project with months of WAL is wasteful.
  v0.2.1 should add a "follower joins from latest Parquet snapshot +
  WAL since that snapshot's LSN" path. The catalog already records
  snapshot LSN per commit.
- **CV refresh on followers**: `Shard::run_cv_refresh` is leader-only
  today. Open question whether followers materialise CVs locally
  (cheaper reads, more memory) or just stream the leader's CV results
  (less memory, network hop per CV read).
