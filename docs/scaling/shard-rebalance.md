# Shard split + rebalance (v0.2)

Status: design + skeleton. Manual via admin API in v0.2; automatic rebalance
based on load metrics in v0.3.

## 1. Current state

Today (`v0.1`):

- **One shard per process.** `basin_shard::Shard` wraps a single
  `InProcessShard` — an in-process `HashMap<(TenantId, PartitionKey),
  PartitionState>` (see `crates/basin-shard/src/in_process.rs`). The whole
  map lives behind one outer `Mutex`; each `PartitionState` lives behind
  its own `tokio::sync::RwLock`.
- **Single-writer-per-(tenant, partition).** A write acquires the partition
  write lock, appends to the WAL (`Wal::append` — fsync on `LocalWal`, future
  Raft quorum on `RaftWal`), then pushes the `(Lsn, RecordBatch)` onto the
  in-RAM tail. Throughput per partition is bounded by WAL fsync latency +
  Arrow IPC serialisation cost.
- **Router-side tenant → shard mapping is process-level.**
  `basin_router::sharding::ShardMap` picks `endpoints[hash(tenant_ulid) %
  N]`, where `N` is the static endpoint count read from
  `BASIN_SHARD_ENDPOINTS` at startup. Whale pins via `BASIN_TENANT_PINS`.
  **Resharding live is explicitly out of scope of v0.1** (see comment block
  at the top of `sharding.rs`).
- **Partition key is opaque.** `basin_common::PartitionKey` is a validated
  path-like string (`year=2026/month=04`, `region:us-east-1`,
  `_default`, ...). The engine derives it per row from the table's
  `PartitionSpec` (today: `Unpartitioned` → `_default`, or `RangeMonthly`
  on a `TIMESTAMPTZ` column → `year=YYYY/month=MM`). There is no
  hash-bucketed sub-sharding inside a tenant; one `(tenant, partition)` =
  one writer.
- **`basin-placement` is a stub.** Module-level docs declare it Phase 3
  work, intended to host a stateless service backed by a strongly-
  consistent store (etcd / FoundationDB / Postgres advisory locks) with
  consistent hashing + virtual nodes. No code yet beyond the crate
  declaration.

### Per-shard write throughput ceiling

The per-`(tenant, partition)` ceiling is set by:

1. **WAL append latency** — `LocalWal` batches with 200 ms / 1 MB cap; in
   practice a few thousand small batches/sec per partition.
2. **In-memory tail mutation under `RwLock`** — write-side acquisition is
   brief but serialises with the read side and compaction snapshots.
3. **Compactor catch-up** — the background compactor drains the tail into
   Parquet every 30 s. If write throughput sustainably exceeds drain
   throughput, RAM grows without bound; in practice this is the real
   ceiling.

The classic write-scale answer is: **make more partitions so more writers
run in parallel**. The unit of horizontal write scale is the
`(tenant, partition)` pair — the shard owner already executes them
independently.

## 2. Shard key model — hash vs range

### What basin uses today

Three keys are in play:

| Layer | Key used | Type |
|------|---------|------|
| Router → shard endpoint | `hash(tenant_ulid) % N` | hash |
| `(tenant, partition)` → in-memory state | `(TenantId, PartitionKey)` exact | range-ish (the key is opaque, but tables pick range partition specs) |
| Storage layout (object keys) | `…/{tenant}/{table}/data/{partition}/{ulid}.parquet` | range (partition is the dir) |

So the router uses **hash** (over tenant) while the partition layer uses
**range** (the user's `PARTITION BY` clause). For the write-scale story
we want to split *within a tenant* (sub-tenant sharding), which means we
extend the partition layer, not the router.

### Recommendation — hash-bucketed sub-partitions on top of the existing range partition spec

Add a new variant to `PartitionSpec`:

```rust
PartitionSpec::Hash {
    column: String,        // primary-key-ish column
    bucket_count: u32,     // initial bucket count (always a power of 2)
}
```

Or compose it with `RangeMonthly` (Iceberg-style transforms):

```rust
PartitionSpec::Composite(Vec<PartitionTransform>)
// PartitionTransform::Month { column }
// PartitionTransform::Bucket { column, n }
```

The composite path is the long-term shape and matches Iceberg's transform
model byte-for-byte. v0.2 ships the simple `Hash` variant; v0.3 promotes
to composite when we have a second user.

#### Why hash, not range, for the sub-partition

- **Write scale** is the goal. Range-bucketed sub-partitions concentrate
  writes on whichever bucket has "now" (timestamp partitions: the current
  month sees 100% of writes; auto-increment id partitions: the highest
  bucket sees 100%). Hash spreads writes uniformly.
- **No skew engineering required.** A range bucket needs the user to
  pick boundaries; hash needs them to pick a `bucket_count`. The latter
  is a single integer.
- **Splitting is mechanical.** Doubling `bucket_count` from `N` to `2N`
  re-bucketing is `new_bucket = old_bucket | (hash >> log2(N))` — every
  old bucket cleanly splits into two new ones with no data movement
  between unrelated buckets. (Range splits require either picking a
  midpoint or replaying data through new boundary checks.)
- **Reads stay fine.** Predicate pushdown on the hash column still
  prunes to one bucket (point lookup) or all buckets (range scan); the
  range partition still prunes by time. The reader concatenates buckets
  identically to today's `_default` reader.

Range partitioning **stays available** because users want it (cold-tier
policies use it, time-travel queries use it). What we add is a hash
*sub-bucket* underneath.

### Concrete shape

The composite `PartitionKey` string becomes:

```
year=2026/month=05/bucket=0007        // RangeMonthly + Hash(bucket_count=16)
bucket=0003                            // Hash only
_default                               // Unpartitioned (no change)
```

Storage layout follows:

```
tenants/{ulid}/{table}/data/year=2026/month=05/bucket=0007/{ulid}.parquet
```

Each new `bucket=` segment is one more `(tenant, partition)` pair → one
more independent writer. A tenant going from 1 → 16 buckets gets 16×
write parallelism without any router change.

## 3. Shard split protocol

The goal: take one `(tenant, partition)` writer at write capacity, split
it into two, with zero lost writes and a bounded read-staleness window.

### Pre-condition

The split is initiated by an admin (v0.2 `basin-cli` command) or by the
rebalancer (v0.3). The catalog learns that `table T`'s
`PartitionSpec::Hash.bucket_count` will go from `N` to `2N` at
`epoch = current_epoch + 1`.

### Five-phase split

```text
phase 0  pre-split      catalog adds new buckets, no traffic
phase 1  dual-write     writes land in both old + new buckets
phase 2  catchup        compactor drains the old tail through the new layout
phase 3  cutover        atomic catalog epoch bump; readers see new layout
phase 4  drop old       reclaim old bucket's storage
```

#### Phase 0 — pre-split

1. Catalog op: `Catalog::propose_partition_split(tenant, table, new_spec)`
   writes a *proposed* spec row alongside the live one (does **not**
   replace it). Both have an `epoch` integer; the proposed row's epoch
   is `live + 1`.
2. New bucket directories exist as catalog rows but carry **zero data
   files**. No writer routes to them yet.
3. Storage layout for the new buckets is just `mkdir`-equivalent on
   object storage (no-op until first file lands).

#### Phase 1 — dual-write window

1. Catalog op: `Catalog::open_dual_write(tenant, table)` flips a flag
   that says "writers SHOULD append to both old AND new buckets". The
   engine's INSERT path checks this on every batch.
2. Per-batch, the engine:
   - Computes the **old** partition key (`bucket_count = N`).
   - Computes the **new** partition key (`bucket_count = 2N`).
   - If they differ (in general they do — every other row), it splits
     the `RecordBatch` row-wise into two batches and calls
     `Shard::get(tenant, old_key).write_batch` AND
     `Shard::get(tenant, new_key).write_batch`. WAL appends both.
3. **Cost during this window**: 1× WAL bandwidth (one fsync per row
   either way), but 2× catalog row counts and 2× active partitions in
   memory. Bounded by definition since the window is short.
4. Reads in this window still go to the **old** layout only. The new
   bucket's files aren't visible because the live spec hasn't moved.

#### Phase 2 — catchup

1. The compactor drains the **old** partition's WAL tail into Parquet
   under the **new** bucket layout. (Same compactor that ships today —
   it already rewrites each batch to a Parquet file under
   `data/{partition}/`; here the `partition` is the new-layout key.)
2. The old partition's existing Parquet files are **rewritten** into
   their new-layout buckets. This is a one-shot pass — read each old
   file, partition row-wise by the new hash, write 2 new files. The
   storage layer does this through a `Storage::repartition_file`
   helper.
3. Snapshot conditions to advance: every WAL entry with
   `lsn ≤ dual_write_open_lsn` has been compacted under the new layout
   AND every old Parquet file has been re-partitioned under the new
   layout.

#### Phase 3 — cutover

1. Catalog op: `Catalog::commit_partition_split(tenant, table,
   expected_proposed_epoch)`. Atomically:
   - Promotes the proposed spec to live.
   - Bumps `table.partition_epoch` from `N` to `2N`.
   - Drops the dual-write flag.
2. **This is the only step where a reader sees a different layout
   than it did one millisecond ago.** Readers re-resolve the partition
   spec on their next query (cached per-session, invalidated by the
   epoch); writers stop dual-writing.
3. Optimistic concurrency: the catalog commit takes the proposed
   epoch as a precondition. A second split racing in returns
   `CommitConflict` and aborts.
4. From this point forward, writes go only to the new layout, reads
   read only from the new layout, the old partition's WAL is
   already-truncated (catchup did that), and the old partition is
   idle.

#### Phase 4 — drop old

1. The old `(tenant, old_bucket)` partition state has nothing left in
   it; the eviction loop drops it on its next tick.
2. The old Parquet files have been replaced (`replace_data_files`
   call during phase 2 swapped them out), so they're not catalogued
   any more.
3. The object-store tombstones for the old files get reaped by the
   existing storage GC pass (`Storage::delete_file` best-effort).

### Concurrent writes during split

The concrete race: a client SQL `INSERT` lands during phase 2 (catchup).
What happens?

- The `Catalog::open_dual_write` flag is set, so the engine's INSERT
  path dual-writes (phase 1 behaviour). It does **not** matter that
  catchup is in flight — dual-write is the live policy until cutover.
- Each row goes to BOTH old and new partition WALs. Both fsync. Then the
  client gets ACK'd. **The new-bucket WAL is the source of truth for
  the post-cutover read path; the old-bucket WAL is for the
  pre-cutover read path; both have it.**
- The race happens at cutover: between "old spec live + dual-write on"
  and "new spec live + dual-write off" there is a single catalog
  transaction. Either the write landed under the old policy (both
  buckets get it) or the new policy (only the new bucket gets it).

**Epoch bumping**: every shard write call carries `(tenant, partition,
partition_epoch)`. The catalog's `append_data_files` will reject commits
with stale epochs (already does, via `expected_snapshot`). On stale
epoch the engine reloads the spec and retries — same retry loop the
compactor already runs for `CommitConflict`.

We do **not** need a 2PC across the cluster. The atomic catalog flip is
the linearisation point; a slow writer that's still in phase-1 mode
when phase-3 lands gets a `CommitConflict` and retries, which re-reads
the spec and proceeds under the new layout.

## 4. Rebalance protocol

The other half of the write-scale story: when one shard *machine* is hot
and another is cold, **move whole shards** between machines.

### Trigger

- v0.2: manual via `basin-cli admin rebalance <shard_id> <target_machine>`.
- v0.3: automatic via `basin-placement::Rebalancer` reading
  per-machine `(qps, mem, disk, write_lag)` metrics every 30 s.

### Six-step move

Each step assumes Fly Machines + an S3-compatible object store (Tigris in
basin-cloud; any backend for self-hosted). The shard is currently on
machine `M_src`, moving to `M_tgt`. The bucket is shared.

1. **Snapshot to object store** — already done by the running compactor; the
   Parquet base IS in the bucket. The WAL tail (last few seconds of writes) is
   the only thing not yet flushed.
2. **Force a WAL flush + Parquet drain on `M_src`** —
   `Shard::flush_to_parquet()` (already exists). After this, the bucket has
   every row that's been ACK'd up to time `t`.
3. **Bring up `M_tgt`** — spawn a new Fly machine running the same
   `basin-engine` binary, configured with the same bucket and the
   shared placement-service endpoint. The embedded catalog is local to
   the engine — `M_tgt` does not need a separate Postgres connection.
   It comes up cold (no resident tenants).
4. **Drain → quiesce `M_src`** — placement service flips the
   `(tenant, partition) → machine` mapping from `M_src` to `M_tgt`. New
   client connections route to `M_tgt`. In-flight connections on `M_src`
   stay attached but their writes start failing fast with
   `BasinError::shard_moved` (a new variant the router catches and
   reconnects the client transparently).
5. **Replay WAL tail on `M_tgt`** — `M_tgt` lazy-loads each
   `(tenant, partition)` on first access (today's cold-start path via
   `replay_wal_into`). The WAL is in the bucket; replay is a few-megabyte
   read + IPC decode.
6. **Drop `M_src`** — once `M_src` reports zero resident partitions for
   the moved set, the placement service hard-terminates it (Fly
   machine destroy). The bucket is unchanged.

The whole move is **2–10 seconds** worth of unavailability for the
moved partitions (steps 4–5). Other partitions on `M_src` are
unaffected.

### Why this works on Fly + shared object storage

- **The object store is the shared substrate.** Both machines see the same
  Parquet files and the same WAL segments. The move is purely a "who owns
  the in-memory state" decision — no data copies.
- **Catalog is the linearisation point.** Same as the split: the
  placement-service flip is an atomic transaction against the
  placement store (`UPDATE shard_placement SET machine = $tgt WHERE
  shard_id = $id`). In v0.3 that store is `basin-placement`'s own
  embedded catalog, accessed via loopback pgwire; operators who want
  external durability can point placement at any pgwire backend.
  Writers see either old or new owner, never both.
- **WAL replay is the catchup mechanism.** `M_tgt` doesn't need state
  transfer from `M_src`; it reconstructs state from the bucket. The cost is
  the replay-time RAM and a small read burst — both bounded by
  the size of the tail (≤ a minute's worth of writes, by design of the
  compaction interval).

## 5. API additions

### `basin-shard/src/split.rs` (new)

```rust
/// Drive a hash-bucket split for a single (tenant, table).
#[async_trait]
pub trait ShardSplitter: Send + Sync {
    /// Phase 0: register the new spec. Returns the proposed epoch.
    async fn prepare_split(
        &self,
        tenant: &TenantId,
        table: &TableName,
        new_spec: PartitionSpec,
    ) -> Result<SplitPlan>;

    /// Phase 1: open the dual-write window.
    async fn open_dual_write(&self, plan: &SplitPlan) -> Result<()>;

    /// Phase 2: re-partition existing Parquet files under the new layout
    /// and drain the old WAL tail through the new buckets.
    async fn catchup(&self, plan: &SplitPlan) -> Result<CatchupReport>;

    /// Phase 3: atomic catalog flip. Returns the new live epoch.
    async fn cutover(&self, plan: &SplitPlan) -> Result<Epoch>;

    /// Phase 4: drop the old layout's residual resources.
    async fn drop_old(&self, plan: &SplitPlan) -> Result<()>;
}

pub struct SplitPlan {
    pub tenant: TenantId,
    pub table: TableName,
    pub old_spec: PartitionSpec,
    pub new_spec: PartitionSpec,
    pub proposed_epoch: Epoch,
    pub dual_write_open_lsn: Option<Lsn>, // set by open_dual_write
}

pub struct CatchupReport {
    pub files_repartitioned: u64,
    pub bytes_rewritten: u64,
    pub tail_entries_drained: u64,
}
```

### `basin-placement/src/rebalance.rs` (new)

```rust
/// Plan + execute shard moves between machines.
#[async_trait]
pub trait Rebalancer: Send + Sync {
    /// Inspect the per-machine load map and produce a (possibly empty)
    /// move plan. Implementations are free to be conservative.
    async fn plan_moves(&self, load: &LoadMap) -> Result<Vec<MovePlan>>;

    /// Execute one move atomically. The implementation:
    ///  - flushes M_src,
    ///  - flips the placement row in the catalog,
    ///  - waits for M_tgt to report the moved partitions resident,
    ///  - tears down M_src if it's now empty.
    async fn execute_move(&self, plan: &MovePlan) -> Result<MoveReport>;
}

pub struct LoadMap { /* machine_id -> (qps, mem_bytes, disk_bytes, write_lag_ms) */ }

pub struct MovePlan {
    pub shard_id: ShardId,
    pub src: MachineId,
    pub tgt: MachineId,
    pub reason: MoveReason,
}

pub enum MoveReason {
    WriteQpsHot,        // src is above QPS threshold
    StorageHot,         // src is above bytes-resident threshold
    Manual,             // basin-cli admin command
    DrainAndShutdown,   // src is being decommissioned
}

pub struct MoveReport {
    pub flush_duration_ms: u64,
    pub catalog_flip_duration_ms: u64,
    pub catchup_duration_ms: u64,
    pub unavailability_window_ms: u64,
}
```

## 6. Operational triggers

A shard split is appropriate when **one `(tenant, table)` partition is
the bottleneck**. Signals:

| Signal | Threshold (v0.2 default) | Source |
|------|---|---|
| Per-partition write QPS | sustained > 5000/s for 5 min | engine counters |
| Per-partition WAL tail bytes | > 256 MB resident | shard stats |
| Per-partition compactor lag | drain rate < write rate for > 2 min | compactor |
| Per-partition Parquet bytes | > 50 GB on object storage | catalog |
| Manual | any | `basin-cli admin split` |

A rebalance is appropriate when **the machine is the bottleneck** (the
shard mix on one box exceeds capacity):

| Signal | Threshold (v0.2 default) | Source |
|------|---|---|
| Machine write QPS | > 80% of measured capacity | router rate-limit metrics |
| Machine RAM | > 70% | Fly metrics |
| Machine disk (WAL local mirror) | > 70% | shard stats |
| Drain | machine being scheduled-out by Fly | Fly machine events |
| Manual | any | `basin-cli admin rebalance` |

## 7. Phasing

### v0.2 (this milestone) — manual everything

- `PartitionSpec::Hash` variant ships in `basin-catalog`.
- `ShardSplitter` trait + a `LocalShardSplitter` impl that runs the
  five-phase protocol against the in-process shard. No multi-machine
  coordination yet — splits run on one machine.
- `Rebalancer` trait + a `LocalRebalancer` impl that's a no-op (only
  one machine exists in v0.2 deployments by default).
- `basin-cli admin split <project> <table> --bucket-count <N>` —
  triggers the five-phase split, prints progress.
- `basin-cli admin rebalance <shard_id> <machine>` — currently a
  documented-but-erroring command; lights up when placement comes online.

### v0.3 — automatic rebalance

- `basin-placement` grows a real `(shard_id) → machine_id` catalog
  table backed by the embedded engine catalog (loopback pgwire); the
  same `BASIN_AUTH_CATALOG_DSN`-shaped override exists for operators
  who want this state on external Postgres.
- `RemoteShardSplitter` runs the split protocol coordinated across
  multiple machines (the dual-write window is cluster-wide).
- `RemoteRebalancer` polls metrics, generates plans, executes moves on
  thresholds. Operator-tunable thresholds.

### v0.4 — composite partition transforms

- `PartitionSpec::Composite(Vec<PartitionTransform>)` lands.
- Hash + RangeMonthly together: writes spread across hash buckets within
  the active month, time-travel + cold-tier policies still work on the
  range component.
- Splits can target either component (re-hash without re-time, or vice
  versa).

## 8. Open questions

- **Hash function choice.** `DefaultHasher` is randomly seeded
  per-process — fine for the router (every router computes its own map
  consistently), wrong for partition assignment (which must be stable
  cross-process and cross-restart). The new hash variant uses
  `xxhash3_64` (or `siphash` with a hard-coded seed) so two machines
  compute the same bucket from the same row.
- **Re-partition WAL re-writing.** Phase 2 rewrites old Parquet
  in-place but the WAL tail also needs replaying through the new
  layout. Naive approach: read each WAL entry, decode the
  `RecordBatch`, re-partition the rows, append to the new partitions'
  WALs, truncate the old. Cost = one extra WAL round-trip per
  in-flight row at split time. Acceptable for v0.2.
- **Bucket count power-of-two requirement.** Doubling is clean
  (`new_bucket = old | (hash_bit_k)`). Non-power-of-two splits need
  full re-hash. v0.2 requires powers of two and rejects others at
  catalog parse time.
- **What about pgwire connections during a rebalance?** The router
  catches `BasinError::shard_moved`, looks up the new endpoint, and
  rebinds the upstream connection mid-session. This is a 2026-mid
  follow-up — the rebalance design works without it, just at a higher
  unavailability cost (clients see a disconnect + reconnect).
