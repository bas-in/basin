# Multi-bucket storage pool — design

Status: **Stage 1 implemented (flag-gated, default OFF)** — the foundation
(bucket registry + deterministic project→bucket assignment + routing through
the single object-store chokepoint) is in the engine behind `BASIN_BUCKET_POOL`.
Consolidation/migration (#37), dedicated-tier promotion, and multi-provider
pools remain design-only. Tracks engine tasks #36 (bounded tiered pool +
routing — Stage 1 done), #37 (online consolidation on scale-down — deferred),
#38 (tests — Stage 1 allocation/stability/ceiling/round-trip done; the
crash-injection migration matrix lands with #37).

## Stage 1 — what shipped

Behind `BASIN_BUCKET_POOL` (default OFF; OFF is a provable no-op routing to
today's single bucket):

- `basin-catalog::bucket_pool` — the persisted record shapes:
  `BucketRegistryEntry` (`bucket_id → bucket_name/endpoint/region/credentials_ref
  + assigned_count`; credentials referenced, never inlined), `BucketAssignment`
  (`bucket_id + tier`), and the `BucketRegistry` global list. `Catalog` gains
  `get/put_bucket_registry`, `get_bucket_assignment`, and
  `assign_bucket_if_absent` (a create-if-absent CAS — the linearization point);
  implemented for the in-memory and object-store backends.
- `basin-storage::bucket_pool::BucketPool` — env config (`BASIN_BUCKET_POOL`,
  `BASIN_BUCKET_POOL_MAX`, `BASIN_BUCKET_POOL_WATERMARK`), a per-process
  assignment cache + resolved-store cache, a `BucketResolver` (maps a registry
  entry's credentials-ref to a real `ObjectStore`; the engine wires the
  S3 resolver, tests use in-memory stores), and `ensure_assignment` (the async,
  catalog-backed, idempotent warm).
- Routing hooks the single chokepoint `Storage::project_object_store`: BYO
  override → pooled assignment (if warmed) → single shared store. The write/read
  entries (`write_batch`, `read`, `list_data_files`) call
  `Storage::ensure_bucket_assignment` first so the sync routing call only reads
  the cache.

Deferred (NOT in Stage 1): consolidation/migration (#37), dedicated promotion,
multi-provider, the sustained-PUT-rate load signal (Stage 1 uses the cheap
per-bucket assigned-project count; see `BucketPool::choose_bucket`).

## Problem

Today every project's objects live under a per-project key **prefix** in a
**single** bucket (Tigris in cloud; one `AWS_*` endpoint). That is simple and
already gives logical isolation, but a single bucket is a shared blast radius
and a shared throughput/rate-limit ceiling: one hot project's PUT/LIST storm
can throttle every other project on the same bucket, and per-bucket request
caps put a hard lid on aggregate write bandwidth (the sustained-drain limit
behind the <30-min-1B work). Provider quotas (objects/bucket, req/s/bucket)
are per-bucket, so scaling write bandwidth eventually requires *more buckets*,
not just more prefixes.

The naive fix — one bucket per project — does not scale: bucket creation is
slow and rate-limited, providers cap buckets-per-account (often O(100–1000)),
and most projects are tiny, so a bucket-per-project wastes the entire quota on
idle tenants. We want bucket **count ∝ load, not ∝ project count**.

## Design: a bounded, tiered bucket pool

A fixed-ceiling **pool** of buckets, with two tiers:

- **Pooled (shared) buckets** — the default. Small/idle projects share a
  pooled bucket, isolated by the existing per-project key prefix. Many
  projects per bucket. This is exactly today's model, but with N pooled
  buckets instead of 1, and projects hashed/assigned across them.
- **Dedicated buckets** — a heavy project (sustained high write bandwidth, or
  near a provider per-bucket cap) is promoted to its own bucket so its load
  can't throttle neighbours and it gets the bucket's full req/s budget.

Key invariant: **the pool size is bounded** by `BASIN_BUCKET_POOL_MAX` (and a
per-region cap). We never provision unboundedly. New buckets are created
lazily, only when (a) every pooled bucket is above a load/occupancy watermark,
or (b) a project crosses the dedicated-promotion threshold — and only up to the
ceiling. At the ceiling we stop adding buckets and pack projects more densely
into existing pooled buckets (degrading gracefully to today's single-bucket
contention rather than failing).

### Assignment / routing

A new **bucket-assignment catalog record** maps `project_id → bucket_id` (plus
tier). It lives in the same object-store catalog as everything else (no
external DB — see the project's "no external DB" rule). Assignment is decided
once at project creation (or at promotion/consolidation) and then STABLE: the
object key for a project's data is `s3://<bucket-of(project)>/<project-prefix>/…`.

Routing rule (deterministic, catalog-driven):
1. Look up `project_id` in the assignment record. If present → that bucket.
2. If absent (first write), assign: pick the pooled bucket with the lowest
   current load below the occupancy watermark; if all are above it and we're
   under the pool ceiling, create a new pooled bucket; else pick the least-full
   pooled bucket anyway (graceful degrade). Persist the assignment, then route.

The assignment must be read on the write path, so it is cached per-process
(like table metadata) and invalidated on the rare assignment change. Storage
endpoint/credentials per bucket come from a small **bucket registry** record
(`bucket_id → endpoint, region, credentials-ref`); credentials are referenced,
never inlined into the catalog.

### Load signal

"Load" for assignment/promotion uses signals the engine already has or can
cheaply maintain per bucket: sustained PUT rate and bytes/s (from the existing
inflight/metrics counters, aggregated per bucket), object count, and proximity
to provider per-bucket caps. Promotion to dedicated fires when a project's
sustained share of its pooled bucket's budget exceeds a threshold for a
sustained window (hysteresis to avoid flapping).

## Online consolidation on scale-down (#37)

When projects are **vacuumed** (deleted/emptied) a pooled bucket can become
sparsely occupied, and a previously-hot (dedicated) project can go cold. To
reclaim buckets toward the lower end of the bound — and to delete now-empty
buckets so we don't sit at the ceiling forever — we **consolidate**: migrate a
project's objects from bucket A into bucket B, then delete A if it becomes
empty.

Migration must be **crash-safe and exactly-once** (no lost or doubled
objects), online (no write-stall for the project beyond a brief cutover), and
must survive a node bounce mid-migration. The protocol:

1. **Plan** — pick a source bucket below the reclaim watermark and a target
   pooled bucket with headroom. Record an intent record in the catalog:
   `migrating(project, from=A, to=B, phase=copy)`.
2. **Copy** — server-side copy every live object of the project from
   `A/<prefix>` to `B/<prefix>` (provider copy API; no data round-trips
   through the engine). Copy is idempotent: re-copying an object is a no-op
   overwrite, so a crash → restart re-runs copy harmlessly. New writes during
   this phase still go to A (assignment unchanged yet).
3. **Verify** — confirm every catalog-referenced live object for the project
   exists in B with matching size/etag. If any mismatch, re-copy. Only a
   fully-verified copy may proceed.
4. **Cutover** — atomically flip the assignment record `project → B` (single
   catalog write, the linearization point). After this, reads and writes
   resolve to B. Writes that were in flight to A during the window are handled
   by the same torn-write/forwarding discipline used elsewhere; the cutover
   itself is a single atomic pointer write so a crash either leaves it at A
   (re-run cutover) or at B (done).
5. **Drain + delete** — objects newly written to A between phase 2 and cutover
   (if any) are copied to B and verified again (a short second pass, bounded
   because writes to A stop at cutover), then A's now-orphaned project objects
   are deleted. When A holds no live project, delete the bucket. Deletion is
   the LAST step and is itself idempotent (delete-if-exists).

Crash safety: the intent record's `phase` makes the whole thing a resumable
state machine — on restart, read the intent and resume from `phase`. Because
copy/verify/delete are each idempotent and cutover is a single atomic write,
the migration is exactly-once regardless of where a crash lands. This mirrors
the loader's purge-and-verify discipline but at the object level and inside the
engine, where it belongs.

## Bounds & safety knobs

- `BASIN_BUCKET_POOL_MAX` — hard ceiling on total buckets (and a per-region
  cap). Never exceeded; at the ceiling we pack denser, never fail writes.
- Occupancy watermark (when to grow), reclaim watermark (when to consolidate),
  with a gap between them so we don't thrash create↔delete (hysteresis).
- Dedicated-promotion threshold + sustained window.
- A migration runs at most K at a time and is rate-limited (provider copy/list
  budgets); consolidation is best-effort background work, never on the write
  path.

## Tests (#38)

Allocation:
- Round-robin/least-loaded assignment fills pooled buckets evenly; assignment
  is stable across restarts (re-read from catalog, not recomputed).
- Pool growth stops at `BASIN_BUCKET_POOL_MAX`; past the ceiling, new projects
  pack into the least-full pooled bucket (graceful degrade, no error).
- Promotion to dedicated fires only after the sustained threshold (no flap on
  a brief spike).

Consolidation / migration (the exactly-once matrix — the high-risk part):
- Migrate a project A→B with concurrent writes; assert post-cutover that B
  holds exactly the project's live set (count + per-object etag) and A holds
  none — no lost, no doubled objects.
- **Crash injection at every phase boundary** (copy, verify, cutover, drain,
  delete): kill the node mid-phase, restart, assert the migration resumes from
  the intent record and converges to the same exactly-once result. Cutover
  crash specifically: assert reads/writes resolve correctly whether the crash
  landed before or after the atomic flip.
- Bucket deletion only after A is provably empty of live project objects;
  deleting a bucket that still has a live project is rejected.
- A vacuumed (deleted) project's objects are fully removed and its bucket
  reclaimed when it empties.
- End-to-end: provision many small projects → all share few pooled buckets;
  drive one hot → it promotes to dedicated; vacuum the small ones → buckets
  consolidate back down toward the lower bound; bucket count tracks load, not
  project count, throughout.

## Non-goals / deferred

- Cross-provider pools (e.g. Tigris + a second provider): the registry's
  `endpoint/credentials-ref` per bucket already allows it, but mixing
  providers in one project's lifetime is out of scope for v1.
- Automatic re-balancing of already-assigned projects purely for evenness
  (only load-driven promotion and vacuum-driven consolidation move a project).
