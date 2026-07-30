---
title: "Multi-bucket throughput striping — design"
nav_section: storage
sidebar_position: 41
summary: "Striping one hot project's data across multiple pooled buckets for write bandwidth. Extends the multi-bucket pool design; config-driven, flag-gated, default OFF."
---

# Multi-bucket throughput striping — single-project write-bandwidth design

Status: **Design + Stage-2a routing seam implemented + production real-bucket
wiring (config-driven, flag-gated, default OFF).**
Extends `docs/multi-bucket-pool-design.md` (Stage 1: per-PROJECT bucket
assignment). This document covers the *throughput* lever — striping ONE hot
project's data across multiple pooled buckets — that Stage 1 deliberately does
not provide.

Tracks engine task #36 (single-table ingest throughput via multi-bucket).

---

## 0. TL;DR

- Stage 1 gives **noisy-neighbour isolation** (one project ↔ one bucket) but
  **does not raise a single project's write bandwidth** — that project still
  lands on one bucket and inherits one bucket's req/s + bandwidth cap.
- The throughput lever is **partition→bucket striping**: map each of a
  project's partitions to one of N pooled buckets so concurrent partition
  commits hit different buckets, giving aggregate write bandwidth ≈
  (#buckets × per-bucket cap).
- **Honest bottleneck finding:** on `shared-cpu-4x` a single node is CPU-bound
  on Vortex encode + Parquet stats, not bucket req/s. Multi-bucket striping
  only wins once you can drive enough *concurrent* PUTs to saturate one
  bucket — i.e. **more nodes** (multi-node, #28/#35), or a single bigger node
  *after* the #27 ingest-decay clone is gone and encode is no longer the wall.
  Below that point striping costs catalog round-trips and buys nothing.
- **The hard part is addressing.** A `DataFileRef.path`
  (`crates/basin-catalog/src/metadata.rs:245-247`) is a key *relative to one
  store*. DataFusion reads it against the single object store registered per
  session (`crates/basin-engine/src/session.rs:2809-2810`). Striping data
  across buckets means a file reference must carry **which bucket** — a path
  alone is ambiguous. This document specifies that addressing scheme and why
  the **catalog stays on a single primary bucket** while only **data files**
  stripe.

---

## 1. How a data file resolves a store TODAY (code-grounded)

### 1.1 Write (PUT)

`crates/basin-storage/src/writer.rs:216-423`:

1. `write_batch_with_options` builds a **store-relative** key via
   `data_file_key` (`crates/basin-storage/src/paths.rs:24-70`):
   `{root?}/projects/{project}/tables/{table}/data/{partition…}/yyyy/mm/dd/{ulid}.{ext}`.
2. It PUTs the body to `storage.project_store(project)`
   (`writer.rs:378-382`) and, if encrypting, a `.wrapped` sidecar
   (`writer.rs:384-391`).
3. It returns `DataFile { path: key, … }` (`writer.rs:411-422`) — the key is
   the SAME store-relative string regardless of which store it landed in.

`project_store` → `project_object_store`
(`crates/basin-storage/src/lib.rs:1330-1365`) resolves the backing store in
priority order: **BYO override → pooled per-project assignment (Stage 1) →
single shared `Inner::object_store`**. It then wraps it in a
`ProjectScopedStore` for the per-project concurrency budget. There is exactly
**one** store returned per `(project)` — partition is not a parameter.

### 1.2 Read (LIST + GET)

`crates/basin-storage/src/reader.rs:159-241`: `list_data_files[_stream]` LISTs
`projects/{p}/tables/{t}/{tier}/…` against the single `project_store(project)`
and treats every `.parquet`/`.vortex` object it finds as a live file. **File
existence is sourced from the object-store LIST, not the catalog**
(`reader.rs:214-235`). So the union read of a table is "LIST one store under
the table prefix".

DataFusion scan: `session.rs:2803-2810` registers `project_object_store(project)`
under the single base URL `basin://engine/` (`session.rs:68`). Every
`DataFileRef.path` the planner turns into a `basin://engine/<path>` object URL
resolves against **that one registered store**.

### 1.3 Per-partition segment catalog (parts / HEAD / chunks)

`crates/basin-catalog/src/object_store_catalog.rs`:

- Manifest: `{root}{project}/{schema}/{table}/v{N}.json` + `HEAD` (lines 27-28).
- Per-partition segment chain:
  `{root}{project}/{schema}/{table}/parts/{partition_id}/v{M}.json` +
  `HEAD` (lines 257-258, `part_segment_key`/`part_head_key` 621-641).
- #27 chunked baseline:
  `{root}{project}/{schema}/{table}/parts/{pid}/chunks/{hash}.json`
  (`baseline_chunk_key` 1293-1304), an immutable content-addressed
  `Vec<DataFileRef>`.

**All of these live on `ObjectStoreCatalog.store`** — a single global store
(`object_store_catalog.rs:437`). In production `main.rs:326` sets
`catalog_object_store = object_store.clone()`, so catalog and data share one
physical bucket. The per-partition commit CAS is `PutMode::Create` on
`v{M+1}.json` against `self.store` (lines 1382-1387); the chunk load is
`self.store.get(chunks/{hash}.json)` (lines 1322-1337). The CAS, HEAD, and
chunk reads are **all on the catalog's single store and know nothing about the
data store**.

**Consequence:** `DataFileRef.path` is interpreted by the *reader* against the
data store and by the *catalog* only as an opaque string key. The catalog never
GETs a data file; it only records its path + stats. This separation is what
makes data-file striping tractable without restriping the catalog.

---

## 2. Honest bottleneck analysis — when does multi-bucket win?

Prior measurement (recorded in session memory `project_basin_multinode`):
**single-node ingest on `shared-cpu-4x` was CPU-bound** — granting 3× the PUT
budget yielded only ~1.5×, and *bigger* nodes were *worse* (request
oversubscription against one bucket's connection pool). So the binding
constraint ordering is:

1. **CPU** (Vortex `encode_with_mode`, Parquet stats extraction,
   `compute_sketches`/`compute_bloom_filters` in `writer.rs`) — the wall on a
   single small node. Striping buckets does nothing here.
2. **Per-PUT latency × concurrency** — to saturate one bucket's req/s you must
   have enough *in-flight* PUTs. One node's encode throughput caps in-flight
   PUT generation well below a Tigris bucket's req/s ceiling.
3. **Per-bucket req/s + bandwidth cap** — the constraint multi-bucket actually
   removes. You only reach it when (2) is satisfied: many concurrent committers.

So **multi-bucket striping pays off when, and only when:**

- **Multiple nodes** ingest one project concurrently (multi-node, #28/#35):
  each node owns a disjoint set of partitions (the existing
  `partition_router` fnv1a ownership, `crates/basin-engine/src/partition_router.rs:61-75`),
  so striping partitions across buckets means node A's partitions and node B's
  partitions naturally hit different buckets — aggregate bandwidth scales with
  node count up to (#buckets × cap). **This is the primary win.**
- **Or** a single large node *after* two preconditions: (a) #27 ingest-decay
  clone removed so per-commit cost is flat, and (b) encode is parallelised
  enough that PUT generation outruns one bucket's req/s. Until both hold, a
  bigger node just oversubscribes one bucket and regresses.

**Anti-goal:** enabling striping on a CPU-bound single small node. It adds an
assignment round-trip and splits the page/footer caches across buckets for zero
bandwidth gain. The flag stays OFF by default precisely because the win is
conditional on the deployment shape above.

**What proves the win (dev validation, §6):** N real pooled buckets, multi-node
(fra+jnb or 2×fra), one project, partition count ≫ bucket count, loader driving
enough concurrency to saturate ≥1 bucket at single-bucket baseline — then
measure aggregate sustained rows/s vs the single-bucket baseline. Expect ≈ linear
in min(#buckets, #nodes) until CPU re-binds.

---

## 3. Striping design: partition → bucket

### 3.1 Requirements (restated)

(a) a partition's DATA files always PUT/read from the **same** bucket;
(b) the union table read still finds **every** partition's files across all
buckets; (c) **exactly-once** and **survives node restart**; (d) **no external
DB**; (e) OFF flag = byte-identical single-bucket behaviour.

### 3.2 The map: deterministic, stable, registry-anchored

For a project assigned a **stripe set** of buckets
`S = [b_0, b_1, … b_{k-1}]` (an ordered, append-only list persisted in the
project's assignment record), a partition's bucket is:

```
bucket_of(partition_id) = S[ fnv1a(partition_id) % k ]
```

- **Deterministic + stable:** `fnv1a` is the same hash already used for
  partition→node ownership (`partition_router.rs:61-75`); reusing it keeps the
  one-hash mental model. `k` and the order of `S` are **frozen at assignment
  time** and only ever *appended to* (never reordered, never shrunk while files
  exist) so `% k` cannot remap an existing partition's already-written files.
  *(Growing the stripe set is a §3.6 migration concern, deferred — Stage-2a
  fixes `k` at assignment.)*
- **Restart-safe:** `S` is read back from the assignment record (catalog,
  no external DB) exactly like the Stage-1 single assignment; the map is
  recomputed identically on every node.
- **No data loss on rebalance:** because `S` is frozen, `bucket_of` is a pure
  function of `(partition_id, S)`; two nodes, or the same node after restart,
  compute the identical bucket for a partition.

### 3.3 Addressing: how a read knows which bucket (THE hard problem)

A `DataFileRef.path` is store-relative and the reader resolves it against ONE
registered store. Three candidate schemes; we choose **C**.

- **Scheme A — bake bucket into the key string** (`b0/projects/…`): rejected.
  It pollutes every path, breaks `data_file_key`'s project-prefix isolation
  invariant (`paths.rs:208-233`), and the catalog/DataFusion treat paths as
  opaque so a stale prefix can't be re-pointed.
- **Scheme B — a second registered object store per bucket in DataFusion**:
  rejected for Stage-2a. DataFusion registers one store per URL authority; you
  *can* register `basin://b0/`, `basin://b1/` … and emit per-file URLs, but
  that touches the planner's URL construction (`session.rs:3185,3740,4181`) and
  the scan-pruning path broadly — too large to land safely now.
- **Scheme C — bucket is a pure function of the partition, resolved at I/O
  time (CHOSEN).** The file ref carries **no** bucket; the bucket is recovered
  by re-applying `bucket_of(partition_id)` whenever the path is opened. The
  partition is already encoded **in the path itself**
  (`paths.rs:54-65`: `…/data/{partition…}/…`), so a reader can recover the
  partition from any data-file key and re-derive its bucket deterministically.
  No schema change to `DataFileRef`; no catalog migration; the OFF path is
  literally unchanged because `bucket_of` collapses to the single store when
  `k == 1`.

Scheme C makes striping a **routing-only** change: the *write* PUTs to
`store_for_partition(project, partition)`, the *LIST* unions across `S`, and a
*GET by path* re-derives the partition from the path → bucket → store. The
catalog and `DataFileRef` are untouched.

### 3.4 Does the catalog stripe too? — **No. Catalog stays on a primary bucket.**

Decision: **only DATA files stripe; the per-partition catalog
(parts/HEAD/chunks), the table manifest, the registry, and assignments all stay
on the single primary catalog bucket** (`ObjectStoreCatalog.store`,
`object_store_catalog.rs:437`).

Rationale:

- The catalog's CAS (`PutMode::Create` on `v{M+1}.json`,
  `object_store_catalog.rs:1382-1387`) is the exactly-once linearization point.
  Keeping it on one bucket means the **create-if-absent CAS is per-key on one
  consistent store** — no cross-bucket coordination, no two-phase commit. This
  is the #34 durable-barrier story unchanged: a partition's segment commit is
  one atomic PUT on one store; the data files it references were already
  durably PUT to their stripe bucket *before* the commit (write-before-commit
  ordering in `writer.rs` then the engine's `append_data_files_in_partition`).
- Catalog objects are **tiny and low-rate** (one segment per commit, HEAD
  pointer, occasional chunk). They are not the bandwidth bottleneck; striping
  them would buy nothing and would multiply the CAS surface across buckets.
- Data files are **large and high-rate** — exactly what a per-bucket bandwidth
  cap throttles. Striping precisely the hot, fat objects is the whole win.

Crash-safety with split stores: the ordering invariant is **data PUT (stripe
bucket) → segment CAS (catalog bucket)**. A crash between them leaves an
orphaned data object in a stripe bucket that no segment references — identical
to today's behaviour for an un-committed direct write, and reclaimed by the
same vacuum/GC path. A crash after the CAS is fully committed. The catalog CAS
remains the single linearization point; the stripe bucket only ever holds
immutable, ULID-named, write-once data objects, so a re-PUT after crash is a
harmless overwrite of identical content (or a fresh ULID — never a lost or
doubled *committed* file, because commit = the catalog CAS).

Per-bucket create-if-absent: still works, because the only CAS is the catalog
segment CAS on the single catalog store. Data-file PUTs are plain
`PutMode::Overwrite`-equivalent writes of unique ULID keys — no CAS needed,
no per-bucket CAS contention.

### 3.5 Union read across the stripe set

`list_data_files` becomes: for each bucket `b` in `S`, LIST
`projects/{p}/tables/{t}/{tier}/…` and concatenate. With `k == 1` (OFF, or a
single-bucket assignment) this is byte-identical to today's single LIST. The
per-file `tier`/stats handling is unchanged; only the *set of stores listed*
grows from 1 to `k`. Count + path-set correctness is the union of disjoint
per-partition subtrees (a partition lives in exactly one bucket), so no file is
double-counted and none is missed.

For the DataFusion GET-by-path: re-derive the partition segment from the path
(`…/data/{partition}/…`), apply `bucket_of`, fetch from that bucket's store.
*(Stage-2a implements the routing primitives and the union LIST; wiring the
per-file GET re-derivation into the DataFusion scan is the Scheme-C follow-up —
see §5 deferred.)*

### 3.6 Deferred: growing the stripe set / rebalancing

Adding a bucket to `S` after files exist would remap `% k` and strand files.
The migration story (mirroring `docs/multi-bucket-pool-design.md` §"Online
consolidation"): freeze writes to affected partitions, server-side copy their
objects to the new `bucket_of` under the grown `S`, verify, then atomically
bump the assignment's `k`. This is the same crash-safe intent-record state
machine as #37 and is **out of scope for Stage-2a** (which fixes `k` at
assignment time, exactly like Stage 1 fixes the single bucket).

---

## 4. The flag and the no-op guarantee

`BASIN_BUCKET_POOL` (default OFF) gates everything, unchanged from Stage 1.
A new optional `BASIN_BUCKET_POOL_STRIPE` (default 1) sets the per-project
stripe width `k`. With the flag OFF, **or** `k == 1`, every routing call
collapses to the single store and the behaviour is byte-identical to today —
proven by the existing `flag_off_is_a_noop_identical_to_today` test plus a new
`stripe_width_one_is_single_bucket` test.

### 4.1 Pointing the pool at REAL buckets (the wiring fix)

Striping only does real work if the pooled buckets are **real** provider
buckets. Until the operator names them, the pool registers a single bootstrap
entry with an **empty endpoint**, which the production `S3BucketResolver`
(`services/basin-server/src/main.rs`) maps to the process-default store — so
every "stripe" slot resolves to the one default bucket and no real striping
happens. The fix is three config knobs, all consulted by
`BucketPool`/`BucketPoolBuckets::from_env`
(`crates/basin-storage/src/bucket_pool.rs`):

| Env var | Meaning | Default |
| --- | --- | --- |
| `BASIN_BUCKET_POOL_BUCKETS` | Comma-separated REAL provider bucket names, e.g. `basin-pool-0,basin-pool-1,basin-pool-2,basin-pool-3`. Entry *i* is registered under the stable `bucket_id` `pool-000i` (deterministic, identical on every node). | unset → empty → today's placeholder/default-store behaviour |
| `BASIN_BUCKET_POOL_ENDPOINT` | The S3 endpoint shared by those buckets. | the engine's existing storage endpoint (`AWS_ENDPOINT_URL_S3`) so Tigris is used |
| `BASIN_BUCKET_POOL_REGION` | The region literal shared by those buckets. | the engine's existing region (`AWS_REGION`/`AWS_DEFAULT_REGION`), else `auto` |

Credentials are NOT configured per bucket: the registered entries carry
`credentials_ref = None`, so the resolver authenticates with the
**process-default** keys (`BASIN_STORAGE_*` / `AWS_*`) — the same keys the
single-bucket path uses. (The `credentials_ref` mechanism remains available for
a future per-bucket-credential pool; Stage-2a's real-bucket pool shares one
credential set across the endpoint.)

When the list is set, the registered `BucketRegistryEntry` rows now carry the
real `bucket_name` + the configured `endpoint` + `region` (instead of a
generated name + empty endpoint), so the resolver builds an `AmazonS3` against
the **real** bucket. When the list is **unset/empty**, `choose_one_bucket` still
emits the legacy placeholder entry (generated name, empty endpoint) and the pool
behaves exactly as before — a provable no-op (test
`empty_list_keeps_legacy_bootstrap_entry_shape` +
`no_real_buckets_or_flag_off_is_a_noop`).

**Cross-node registry consistency.** On pool init,
`BucketPool::prepopulate_registry` seeds the durable registry from the configured
list, idempotently (create-if-absent per `bucket_id`). Because the id→name
mapping is positional (`pool-000i` ↔ `names[i]`) and derived purely from the
configured list, every node — and a restarted process — converges on the same
registry without coordination. Existing entries and their `assigned_count` are
left untouched; the write is skipped when nothing changed. The growth ceiling is
`min(BASIN_BUCKET_POOL_MAX, len(BASIN_BUCKET_POOL_BUCKETS))`, so a generated id
without a real bucket behind it is never registered (test
`configured_real_buckets_cap_growth_at_list_length`).

---

## 5. What Stage-2a implements vs defers

**Implemented (this change, behind the flag, fully tested):**

- `StripeAssignment`: an ordered, frozen, append-only stripe set `S` persisted
  in the project's assignment record (additive, back-compatible — a Stage-1
  single-bucket assignment deserialises to `k == 1`).
- `BucketPool::store_for_partition(project, partition_id)` — the deterministic
  `S[fnv1a(pid) % k]` resolver over the per-process cache, returning the same
  store for the same partition across restarts.
- `BucketPool::routed_stores(project)` — the full stripe set of stores for a
  project, for the union LIST.
- `ensure_assignment` extended to allocate `k` distinct pooled buckets on first
  write (reusing the Stage-1 least-loaded chooser per slot) and persist the
  ordered set; re-read stable on restart.
- Tests prove: distribution across N buckets (deterministic + stable across
  `invalidate_all`); write-P-then-read-P resolves the same bucket and
  round-trips; union read aggregates count + exact path-set across all buckets;
  per-partition CAS / segment commit still exactly-once with split stores; OFF
  and `k==1` are byte-identical no-ops.

**Deferred (documented seam, NOT half-implemented):**

- Wiring `store_for_partition` into the **DataFusion scan GET-by-path** (Scheme
  C §3.5) — needs the planner's per-file URL/store resolution to re-derive the
  partition; the routing primitive lands now, the scan wiring is the follow-up.
- Hooking the storage **write/read entry points** (`write_batch`, `read`,
  `list_data_files`) to call `store_for_partition`/`routed_stores` instead of
  the per-project `routed_store` — gated behind a second internal switch so the
  Stage-1 per-project behaviour is the default until the scan wiring lands.
- Growing/rebalancing `S` (§3.6) — the #37-style migration.

**Wired since (this change):**

- Production instantiation in `services/basin-server/src/main.rs`: the
  `BucketPool` + `S3BucketResolver` are constructed when `BASIN_BUCKET_POOL` is
  ON, and the registry is pre-seeded with the operator-configured REAL buckets
  (`BASIN_BUCKET_POOL_BUCKETS` + endpoint/region) so striping works against real
  Tigris buckets, not generated placeholders (§4.1). The single-bucket /
  empty-list path is byte-identical to before.

Correctness-first: the deferred items are exactly the ones that, if
half-done, could mis-route or lose committed data (a GET resolving the wrong
bucket → "file not found" on a live file). The routing math + registry +
union-LIST primitive are pure and independently testable, so they land now; the
scan integration lands as a single reviewed follow-up with its own end-to-end
round-trip gate.

---

## 6. Dev throughput validation (the path to proving the win)

1. Provision N real Tigris pooled buckets (operator task), sharing the engine's
   existing Tigris endpoint + credentials. Name them per the list you'll set in
   step 2, e.g. `basin-pool-0 … basin-pool-3`.
2. Set the env on the engine (the `BucketPool` + `S3BucketResolver` are now
   constructed automatically when `BASIN_BUCKET_POOL` is ON, and the registry is
   pre-seeded from the list — §4.1). Set `BASIN_BUCKET_POOL=on`,
   `BASIN_BUCKET_POOL_BUCKETS=basin-pool-0,basin-pool-1,basin-pool-2,basin-pool-3`,
   `BASIN_BUCKET_POOL_STRIPE=N`, `BASIN_BUCKET_POOL_MAX≥N`. The endpoint/region
   default to the engine's existing Tigris endpoint/region, so they need not be
   set unless the pool buckets live elsewhere.
3. Deploy **multi-node** (fra+jnb or 2×fra; the win needs concurrent committers
   per §2). One project, table partitioned so partition count ≫ N.
4. Loader: enough concurrency to saturate ≥1 bucket at the single-bucket
   baseline (the same loader config that hit the single-bucket req/s wall).
5. Measure sustained rows/s vs the single-bucket baseline. Expect ≈ linear in
   min(N, #nodes) until CPU re-binds; record the crossover. Restore the
   fra=1/jnb=1 @ 8gb baseline and drop test tables afterward (dev scale-safety).

### 6.1 Exact dev runbook (env to set on basin-engine-dev)

Given N pre-created Tigris buckets sharing the engine's existing Tigris endpoint
+ creds (so NO per-bucket credentials needed), set on `basin-engine-dev`:

```
BASIN_BUCKET_POOL=on
BASIN_BUCKET_POOL_BUCKETS=basin-pool-0,basin-pool-1,basin-pool-2,basin-pool-3
BASIN_BUCKET_POOL_STRIPE=4
BASIN_BUCKET_POOL_MAX=4
# endpoint/region inherit the engine's existing storage endpoint
# (AWS_ENDPOINT_URL_S3) + region (AWS_REGION); set these ONLY if the pool
# buckets live at a different endpoint/region than the primary store:
# BASIN_BUCKET_POOL_ENDPOINT=https://t3.storage.dev
# BASIN_BUCKET_POOL_REGION=auto
```

On boot every node logs `bucket pool ENABLED (#36) … real_buckets=4` and
idempotently pre-seeds the registry (`pool-0000 … pool-0003` ↔
`basin-pool-0 … basin-pool-3`). Leaving `BASIN_BUCKET_POOL_BUCKETS` unset (but
the flag ON) keeps the existing single-default-bucket behaviour, so the rollout
is safe to stage. Do NOT create the buckets from the engine — the operator
pre-creates them out-of-band.
