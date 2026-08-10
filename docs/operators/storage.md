---
title: "Storage layer — operator runbook"
nav_section: operations
sidebar_position: 51
summary: "Day-2 ops guide for Basin's storage layer: Vortex/Parquet object store, disk cache, page cache, I/O scheduler, KMS envelope encryption, and hot-cold tiering."
tags: [operations, storage, cache, encryption, tiering, object-store]
---

# Storage layer — operator runbook

Day-2 operator's guide to Basin's multi-tier storage stack:
object store (Parquet / Vortex), NVMe disk cache, in-RAM page cache,
per-project fair-share I/O scheduler, KMS envelope encryption, and
the hot-tier memtable flush path.

---

## Architecture in one page

```
Client query
  │
  ▼
basin-engine (reader.rs / writer.rs)
  │
  ├─ Page cache (RAM, decoded Arrow batches)   ← first probe on read
  │     on miss ↓
  ├─ Disk cache (NVMe, raw Parquet bytes)      ← second probe on read
  │     on miss ↓
  ├─ ProjectScopedStore (per-project semaphore + I/O scheduler)
  │
  └─ ObjectStore (S3 / Tigris / GCS / LocalFS)
```

**Write path** (after WAL durability):

```
engine flush → writer.rs (optionally envelope-encrypts)
             → ProjectScopedStore → ObjectStore PUT
             (invalidates disk-cache and page-cache entries for the new path)
```

**Hot-tier path** (HTAP, Phase 5.14):

```
INSERT/UPDATE → MemTableRegistry (per-project BTreeMap)
              → FlushTask triggers on soft cap / age → cold tier (above)
```

The key files:
- `crates/basin-storage/src/disk_cache.rs` — NVMe tier
- `crates/basin-storage/src/page_cache.rs` — RAM tier
- `crates/basin-storage/src/scheduler.rs` — I/O fair-share
- `crates/basin-storage/src/encryption.rs` — KMS envelope
- `crates/basin-hottier/src/budget.rs` — memtable config
- `crates/basin-hottier/src/registry.rs` — memtable registry

---

## Metrics

> **Status — read before wiring a dashboard.** Basin does not serve a
> Prometheus scrape endpoint today. The names below are the planned
> OTLP / Prometheus-convention names; the engine emits structured `tracing`
> records that become OTLP metrics when an OpenTelemetry layer is attached
> (`BASIN_OTLP_ENDPOINT`, default `http://localhost:4318`). The only HTTP
> metrics route that exists is `GET /metrics/inflight`, which returns a small
> JSON in-flight/latency snapshot. Treat every `curl .../metrics` recipe on
> this page as the intended shape, not a working command.

### Page cache

| metric | type | what it tells you |
|---|---|---|
| `basin_page_cache_hits_total` | counter | Decoded Arrow batches served from RAM — no decode, no I/O |
| `basin_page_cache_misses_total` | counter | Cache miss — fell through to disk cache or ObjectStore + decode |
| `basin_page_cache_evictions_total` | counter | Entries popped by LRU to stay under `max_bytes` |
| `basin_page_cache_current_bytes` | gauge | Current RAM consumption of the page cache |

**Signal**: `hits / (hits + misses)` is the page-cache hit rate. Under healthy
steady-state workloads this should be > 80 % for SaaS access patterns (same
SELECT repeated). A hit rate below 50 % means the working set does not fit in
the RAM budget — increase `BASIN_PAGE_CACHE_MAX_BYTES` or reduce the number of
distinct queries.

### Disk cache

| metric | type | what it tells you |
|---|---|---|
| `basin_disk_cache_hits_total` | counter | Raw Parquet bytes served from local NVMe — no ObjectStore RPC |
| `basin_disk_cache_misses_total` | counter | Fell through to ObjectStore (cold S3 or equivalent) |
| `basin_disk_cache_evictions_total` | counter | LRU evictions to stay under `max_bytes` |
| `basin_disk_cache_current_bytes` | gauge | On-disk cache footprint |

**Signal**: `hits / (hits + misses)` is the disk-cache hit rate. A sustained
miss rate above 40 % with consistently-repeated queries indicates the disk
cache `max_bytes` is too small for the working set. Check available NVMe space
and increase `BASIN_DISK_CACHE_MAX_BYTES`.

### I/O scheduler (per-project fair-share)

The scheduler tracks per-project I/O pressure. These are surfaced via
`Storage::project_stats()` (consumed by basin-engine's noisy-project
detector) and via the `/admin/v1/usage` endpoint.

| field | what it tells you |
|---|---|
| `ProjectIoStats::in_flight` | Active ObjectStore RPCs for this project |
| `ProjectIoStats::queue_depth_high` | Queued point-reads waiting for a global budget permit |
| `ProjectIoStats::queue_depth_low` | Queued bulk-reads / PUTs waiting for a permit |
| `ProjectIoStats::arrivals_per_sec` | Arrival rate in the trailing 1-second window |

**Global RPC budget**: `DEFAULT_GLOBAL_BUDGET = 4`. ADR 0008 explains why
fewer than 4 concurrent ObjectStore RPCs deadlocks the Parquet range fan-out.
This cap is the load-bearing concurrency limiter — do not raise it above 8
without understanding the Parquet page-reader fan-out behaviour.

### Memtable (hot-tier, HTAP)

| metric | source | what it tells you |
|---|---|---|
| `basin_budget_over_cap_seconds_total{cap=memtable_bytes}` | `LeaseMetrics` | Seconds a project's memtable was at or above its hard cap |
| `ProjectMemState::bytes_allocated` | `MemTableRegistry` | Current hot-tier bytes for a project |

---

## Configuration knobs

### Page cache

| env var | default | description |
|---|---|---|
| `BASIN_PAGE_CACHE_MAX_BYTES` | 1 073 741 824 (1 GiB) | Byte budget for the in-RAM decoded-Arrow cache |

### Disk cache

| env var | default | description |
|---|---|---|
| `BASIN_DISK_CACHE_MAX_BYTES` | 10 737 418 240 (10 GiB) | Byte budget for the on-NVMe raw-Parquet cache |
| `BASIN_DISK_CACHE_ROOT` | `$TMPDIR/basin-disk-cache` | Directory for cached `.frag` files. Production server overrides this to the NVMe mount point. |

> **Important**: the disk cache treats `BASIN_DISK_CACHE_ROOT` as
> exclusively owned. Never point two Basin process instances at the
> same root — the LRU index is in-RAM and they will corrupt each
> other's on-disk fragments.

### Memtable (hot-tier)

| env var | default | description |
|---|---|---|
| `BASIN_MEMTABLE_HARD_CAP` | 268 435 456 (256 MiB) | Project-level hard cap. New writes block (await semaphore) when this is reached. |
| `BASIN_MEMTABLE_SOFT_CAP` | 201 326 592 (192 MiB) | Project-level soft cap. Background flush is triggered. Writes continue. |
| `BASIN_MEMTABLE_TABLE_CAP` | 16 777 216 (16 MiB) | Per-table soft cap. Triggers a per-table flush independently of the project cap. |
| `BASIN_MEMTABLE_MAX_AGE_SECS` | 60 | Seconds before a memtable entry is flushed regardless of size. |

Per-project overrides:

```sql
-- Override the hard cap for a specific project (range: 1 MiB – 64 GiB).
ALTER PROJECT acme_prod SET basin.memtable_hard_cap = 536870912;  -- 512 MiB
```

### Encryption

Envelope encryption is configured per-deployment by wiring an
`EncryptionProvider` implementation (see
`crates/basin-storage/src/encryption.rs`). For the static-key dev
implementation (`StaticKeyEncryption`) the key is passed via:

| env var | description |
|---|---|
| `BASIN_ENCRYPTION_KEY` | 32-byte hex-encoded AES-256 key used by `StaticKeyEncryption`. Only for local/dev. Production uses the KMS adapter. |

In production, each project has its own CMK. The provider is looked up by
project ID at write time. The data file layout on object storage is:

```
<path>.parquet          — AES-256-GCM encrypted body: nonce(12) || ciphertext+tag
<path>.parquet.key      — Wrapped (KMS-encrypted) data key sidecar
```

On read, the sidecar is unwrapped by the KMS call before decryption.

---

## Common alerts

### ALERT: page cache hit rate below 50 %

**Trigger**: `rate(basin_page_cache_hits_total[5m]) /
(rate(basin_page_cache_hits_total[5m]) + rate(basin_page_cache_misses_total[5m])) < 0.5`

**Typical cause**: Working set has grown beyond `BASIN_PAGE_CACHE_MAX_BYTES`,
OR a runaway project is flooding the cache with unique queries (evicting
others' entries).

**Remediation**:

1. Check `basin_page_cache_current_bytes`. If it is at the configured cap,
   the budget is too small.
2. Identify the top-volume project via `/admin/v1/usage` — look for
   `ops_total` or `bytes_read_total` outliers.
3. Either increase `BASIN_PAGE_CACHE_MAX_BYTES` and restart, or reduce the
   whale project's query rate (per-project QPS cap via `basin_budget_over_cap_seconds_total{cap=rest_qps}`).

---

### ALERT: disk cache eviction rate sustained

**Trigger**: `rate(basin_disk_cache_evictions_total[5m]) > 100`

**Cause**: The NVMe cache is cycling through its LRU faster than the working
set fits. Either the cache budget is too small or a large table scan is
thrashing it.

**Remediation**:

1. Check `basin_disk_cache_current_bytes`. If at the cap, compare against
   available disk space (`df -h $BASIN_DISK_CACHE_ROOT`).
2. Increase `BASIN_DISK_CACHE_MAX_BYTES` if headroom is available.
3. If a large bulk scan is the culprit, it gets Low priority in the I/O
   scheduler (1 s deadline class) — it shouldn't affect point-read latency.
   Confirm this by checking `ProjectIoStats::queue_depth_high` for other
   projects (should be near 0).

---

### ALERT: memtable hard cap reached (write stall)

**Trigger**: `basin_budget_over_cap_seconds_total{cap=memtable_bytes}` rising
monotonically for a project.

**Cause**: The project's in-memory write buffer has reached its hard cap and
new writes are blocking, waiting for the flush task to drain memtable bytes
below the cap.

**Immediate check**:

```sql
-- Check which projects are near their memtable cap.
-- Requires basin_admin.project_counters_snapshot (Phase 5.16).
SELECT project_id, bytes_allocated, memtable_hard_cap
FROM basin_admin.project_memtable_state
ORDER BY bytes_allocated DESC
LIMIT 10;
```

**Remediation** (in order of preference):

1. **Increase hard cap temporarily** — if this is a legitimate write spike:
   ```sql
   ALTER PROJECT acme_prod SET basin.memtable_hard_cap = 536870912;
   ```
2. **Check flush task health** — if the flush task is stalled (disk full,
   ObjectStore unavailable), writes will stack up behind the hard cap even
   if the soft-cap flush is triggered. Check object store connectivity and
   disk space on the replica running the project.
3. **Force a flush** — trigger an immediate flush for the project:
   ```sql
   ALTER PROJECT acme_prod FLUSH MEMTABLE;
   ```

---

### ALERT: KMS wrap/unwrap errors

**Trigger**: Errors containing `envelope decrypt` or `aes-gcm decrypt` in
replica logs at ERROR level.

**Cause**: Either the KMS is unavailable, the project's CMK has been rotated
without migrating existing sidecars, or a sidecar file is corrupted.

**Diagnosis**:

```bash
# Check replica logs for the failing project
grep -E 'envelope (decrypt|wrap)|aes-gcm|WrappedKey' /var/log/basin-server.log | tail -50
```

**Recovery**:

- **KMS unavailable**: all reads of encrypted files fail. Fix the KMS
  connectivity. Reads of unencrypted (plaintext) projects are unaffected.
- **Key rotation without sidecar migration**: re-wrap existing sidecars using
  the KMS migration tool (contact the platform team). The old wrapped keys must
  still be unwrappable by the KMS during the migration window.
- **Corrupted sidecar**: identify the specific file from the error log (the
  object path is logged). Restore from a backup or, if the data is in the WAL,
  replay the WAL to regenerate the Parquet file.

---

## Common operations

### Inspect disk cache on-disk state

```bash
# How many .frag files and total bytes
find $BASIN_DISK_CACHE_ROOT -name '*.frag' | wc -l
du -sh $BASIN_DISK_CACHE_ROOT
```

The cache uses SHA-256 of `(object_path, byte_range)` as the file name — the
on-disk files are not human-readable Parquet files; they are raw byte ranges.
Don't attempt to interpret them. Deleting them is safe: the LRU index will miss
on next read and re-fetch from the object store.

### Flush disk cache entirely (emergency — clears warm reads)

```bash
# Safe to do while the server is running; the in-RAM LRU index will
# detect the missing files and treat them as cache misses on next access.
rm -rf $BASIN_DISK_CACHE_ROOT/*.frag
```

Note: after a full cache flush, the first few minutes of read traffic will
go cold against the ObjectStore. Query latency will be higher than normal
until the cache repopulates. Alert on `basin_disk_cache_hits_total` rate
dropping to near 0 after the flush and recovering over the next 5–15 min.

### Check page cache hit rate manually

```bash
curl -s http://localhost:9090/metrics | grep basin_page_cache
```

Sample output:

```
basin_page_cache_hits_total 48321
basin_page_cache_misses_total 1203
basin_page_cache_evictions_total 88
basin_page_cache_current_bytes 872415232
```

Hit rate = 48321 / (48321 + 1203) ≈ 97.6 % — healthy.

### Verify encryption is active for a project

```bash
# Sidecar files have the .key extension alongside the .parquet body.
# If no .key files exist for a project, it's plaintext (expected for
# projects without encryption configured).
aws s3 ls s3://my-bucket/projects/<project_uuid>/ --recursive | grep '\.key$' | head -5
```

---

## Troubleshooting

### Reads are slow (high p99 latency)

Work through the cache layers:

1. **Page cache**: check `basin_page_cache_hits_total` rate. If near 0, the
   cache is cold (post-restart) or the budget is too small. Expected warmup:
   5–15 minutes under normal traffic.
2. **Disk cache**: check `basin_disk_cache_hits_total` rate. If near 0, the
   NVMe cache is cold or the budget is too small. Disk cache warms faster than
   page cache because it holds raw bytes (no decode needed to populate).
3. **ObjectStore latency**: if both caches are cold and disk reads are slow,
   the object store is the bottleneck. Check `ProjectIoStats::in_flight` — if
   it is pegged at `DEFAULT_GLOBAL_BUDGET = 4`, every project is I/O-bound.
   Check network connectivity and object store health dashboard.
4. **I/O scheduler starvation**: if one project has `queue_depth_high > 100`
   while others are flowing, check whether the stalled project owns a whale
   partition (see `basin_lease_holdings_total` in the lease-ownership runbook).
   High-priority reads should drain first; if they're not, check whether the
   global budget itself is depleted by bulk Low-priority work from another
   project.

### Object store PUT fails at high rate

PUTs (flush, compaction) fail with ObjectStore errors:

1. Check connectivity: `curl -v $OBJECT_STORE_ENDPOINT/healthz`.
2. Check credentials: IAM role / service-account token expiry.
3. Check disk on the replica for intermediate buffer overflow: the writer
   buffers the encoded Parquet file in RAM before PUTting — at 256 MiB
   per project hard cap, a full memtable for 4 projects simultaneously
   requires ~1 GiB of headroom. If the process is OOM-killed during a flush,
   the WAL marker path (Phase 5.14.C2) ensures recovery on restart.
4. For multipart uploads that abort mid-stream, check for stale S3 multipart
   parts accumulating in the bucket:
   ```bash
   aws s3api list-multipart-uploads --bucket my-bucket | jq '.Uploads | length'
   ```
   Abort them with the S3 lifecycle rule `AbortIncompleteMultipartUpload` or
   manually via `aws s3api abort-multipart-upload`.

---

## Failure modes summary

| Failure | Visible signal | Behaviour | Recovery |
|---|---|---|---|
| **Page cache full** | `basin_page_cache_evictions_total` rising; hit rate drops | LRU evicts; next reads decode from disk cache. Performance degrades, no data loss. | Increase `BASIN_PAGE_CACHE_MAX_BYTES`; restart. |
| **Disk cache full** | `basin_disk_cache_evictions_total` rising; hit rate drops | LRU evicts on-disk frags; reads fall to ObjectStore. Latency rises. No data loss. | Increase `BASIN_DISK_CACHE_MAX_BYTES` or free NVMe space. |
| **Disk cache root deleted** | Next write fails `NotFound` on the root dir; cache recreates it on next insert | Cache degrades to no-cache mode; all reads go to ObjectStore. | Ensure `BASIN_DISK_CACHE_ROOT` exists and is writable; restart. |
| **Memtable hard cap** | `basin_budget_over_cap_seconds_total{cap=memtable_bytes}` rising | Writes block (await semaphore) until flush drains bytes below cap. | Check flush task; increase hard cap; fix ObjectStore if flush is stalled. |
| **KMS unavailable** | ERROR-level log: `aes-gcm decrypt` / `envelope decrypt` | Reads of encrypted files fail with `BasinError::Storage`. Plaintext projects unaffected. | Restore KMS connectivity; no data loss. |
| **ObjectStore unavailable** | All disk-cache misses become errors; WAL flushes queue up | Reads from cache still succeed (warm window). Writes queue in the hot-tier. | Restore ObjectStore; replay WAL on recovery. |
| **Sidecar (`.key`) lost** | Decrypt error on specific file path | That specific encrypted Parquet file is unreadable. Other files unaffected. | Restore sidecar from backup or regenerate from WAL replay. |

---

## Cross-references

- [ADR 0008 — Noisy-neighbor fairness / per-project fair-share I/O](../decisions/0008-noisy-neighbor-fairness.md)
- [ADR 0016 — HTAP hot-tier architecture](../decisions/0016-htap-hot-tier-architecture.md)
- [Lease ownership runbook](./lease-ownership.md) — the `memtable_bytes` cap connects to the lease budget system.
- `crates/basin-storage/src/scheduler.rs` — `DEFAULT_GLOBAL_BUDGET` constant and per-project stats.
- `crates/basin-hottier/src/budget.rs` — all `BASIN_MEMTABLE_*` env-var constants.
