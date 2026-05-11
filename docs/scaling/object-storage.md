# Object Storage Scaling — Cloudflare R2 (zero egress) with S3 fallback

Status: design + skeleton (2026-05-11)
Scope: how `basin-storage` and `basin-wal` reach a real object store in
deployed configurations, and why R2 is the default for the managed cloud.

This is the scaling-story companion to `docs/architecture.md` §Layer 4. The
hot-path durability boundary is still the Raft WAL on local NVMe (ADR 0009);
this document is about the *cold* boundary: where Parquet (and old WAL
segments) actually live for the long term.

---

## 1. Current state of `basin-storage`

The crate is already storage-backend-agnostic. There is no bespoke trait —
every read/write goes through `Arc<dyn object_store::ObjectStore>`, the
trait from `apache/arrow-rs/object_store`.

Key facts (verified against `crates/basin-storage/src/`):

- `StorageConfig` in `src/lib.rs` carries one field for backend selection:
  `pub object_store: Arc<dyn ObjectStore>`. The crate never names a concrete
  backend type internally — `LocalFileSystem`, `InMemory`, `AmazonS3` are all
  legal substitutions at the call site.
- `src/paths.rs::data_file_key` and `src/tier.rs::Tier` already encode the
  tier layout: `tenants/{tenant}/tables/{table}/data/...` for hot,
  `tenants/{tenant}/tables/{table}/cold/...` for cold. A single bucket-side
  lifecycle rule on the `cold/` prefix can transition class to S3-IA / R2-IA
  without any engine code change.
- `src/lib.rs::delete_stream_for` already has a special case for the
  `AmazonS3` backend (detected by `Debug` prefix) that rides the bulk
  `DeleteObjects` API. R2 prints `"AmazonS3(...)"` too (it *is* the AmazonS3
  backend pointed at a different endpoint), so it inherits this fast path
  for free.
- `basin-wal` (`crates/basin-wal/src/lib.rs::WalConfig`) takes the same
  `Arc<dyn ObjectStore>` shape. WAL segments live at
  `{root}/wal/{tenant}/{partition}/{ulid}.seg` — orthogonal prefix from
  Parquet data; lifecycle rules are configured separately.
- The workspace already pulls `object_store = { version = "0.11", features =
  ["aws", "gcp", "http"] }` (root `Cargo.toml` L71). The `aws` feature is
  what we need for R2; no new dep is required.

Today's only real-world wiring (in `services/basin-server/src/main.rs`)
builds a `LocalFileSystem` rooted at `BASIN_DATA_DIR`. This is honest for
single-machine deployments and the Fly volume case, but it doesn't scale
across machines or regions and it doesn't decouple compute from storage.
Everything below is a small extension to that wiring — no trait changes.

## 2. Why R2

Cloudflare R2 is S3-API-compatible, but the pricing curve is materially
different from AWS S3:

| Item            | AWS S3 (us-east-1)         | Cloudflare R2          |
|-----------------|----------------------------|------------------------|
| Storage         | $0.023 / GB-month          | $0.015 / GB-month      |
| Egress to net   | $0.09 / GB (after 100 GB)  | **$0.00 / GB**         |
| Egress to peer  | $0.02 / GB (intra-region)  | $0.00 / GB             |
| Class A (write) | $0.005 / 1k                | $0.0045 / 1k           |
| Class B (read)  | $0.0004 / 1k               | $0.00036 / 1k          |
| Data ingress    | $0.00                      | $0.00                  |

The line that drives the architecture is **egress = $0.00**. Three concrete
implications:

1. **Globally distributed read replicas pay zero per-byte to hit central
   storage.** A reader in Sydney pulling a Parquet file written in
   Johannesburg costs the same as a colocated reader. With S3 the same
   pattern is roughly $90 per TB read, per replica, per access. This is the
   reason the managed cloud's read-replica story (ADR 0004) is even
   financially viable on R2.
2. **CDN-style fan-out is free.** When the dashboard or a SDK consumer
   reads a Parquet file through a signed URL, R2 charges the operator
   nothing for the byte stream that leaves Cloudflare's edge. S3 would
   charge per consumer.
3. **Writes are free too.** R2's write side (`PutObject`) costs only the
   Class A operation; the bytes themselves are uncharged. Compactor
   write-amplification (writing the cold-tier copy of a hot file) is not
   penalised on the egress line.

What R2 charges for is small and bounded: storage GB-month and Class A/B
ops. For a Parquet workload those operations are amortised by row-group
size (we target ~128 MB Parquet files), so the op-count per GB is in the
single digits.

The OSS engine doesn't *require* R2 — `BASIN_STORAGE_BACKEND` selects per
deployment. R2 is the cloud's default; self-hosters can keep
`LocalFileSystem` or `s3` and the same code paths run.

## 3. R2 backend: how to wire it

Cloudflare's S3-compatible endpoint takes this shape:

    https://<account-id>.r2.cloudflarestorage.com

Region must be the literal string `auto`. R2 returns 400 on any other
region for SigV4 reasons. Credentials are an access-key-id / secret-pair
generated under the bucket's IAM settings.

Because `object_store::aws::AmazonS3Builder` already accepts a custom
endpoint, **R2 is "just" an `AmazonS3` constructed with three knobs**:

```rust
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use object_store::ObjectStore;

fn build_r2(cfg: &R2Config) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&cfg.endpoint)       // https://<acc>.r2.cloudflarestorage.com
        .with_region("auto")                // R2 requires "auto"
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.access_key_id)
        .with_secret_access_key(&cfg.secret_access_key)
        // Required when not running behind a TLS-terminating proxy that
        // injects v2 path-style — R2 expects virtual-hosted-style today.
        .with_virtual_hosted_style_request(true)
        .build()?;
    Ok(Arc::new(store))
}
```

There is no new trait, no new transport, no `r2`-feature flag. The
`AmazonS3` backend's `delete_stream` bulk path works against R2 unchanged
(R2 implements the SigV4 `DeleteObjects` action).

Self-hostable example: `crates/basin-storage/src/backends/r2.rs` (see §6).

## 4. Migration path

There is no breaking migration. The existing S3 backend (if anyone is
running one) keeps working; R2 is an additional value of
`BASIN_STORAGE_BACKEND`.

| `BASIN_STORAGE_BACKEND` | Backend                                  | Notes                  |
|-------------------------|------------------------------------------|------------------------|
| `local` (default)       | `object_store::local::LocalFileSystem`   | today's behaviour      |
| `s3`                    | `AmazonS3` with `us-east-1` (or env)     | classic AWS S3         |
| `r2`                    | `AmazonS3` with `auto` + R2 endpoint     | new                    |
| `memory` (tests)        | `object_store::memory::InMemory`         | unchanged              |

The catalog stores object keys verbatim. If you ever do want to migrate
real data between backends, the operation is a `rclone copy` against the
S3 API on both sides — Basin doesn't need to know.

## 5. Cold-tier story

The cold-tier mechanic is already implemented in `basin-storage` and lives
under the `cold/` segment of the table prefix. The R2 angle is purely
operational:

- After N days (configurable, default 30) the compactor rewrites a hot
  Parquet file to its cold-tier sibling key via `paths::rewrite_to_cold`.
  See `src/tier.rs`.
- A bucket-side lifecycle rule on the `tenants/*/tables/*/cold/` prefix
  flips the storage class to **R2 Infrequent Access** (or S3-IA). Setup is
  one-time via `wrangler r2 bucket lifecycle ...` or the dashboard.
- Old WAL segments follow the same shape: a lifecycle rule on the
  `wal/{tenant}/...` prefix transitions segments older than the
  configured retention window (today: drop entirely; tomorrow: cold-tier
  for replay-from-archive scenarios).
- The engine doesn't need to know which class a key is in — it just GETs
  the key and pays whatever the bucket policy charges. The catalog records
  `tier: Hot|Cold` so the planner can prefer hot files when both are
  acceptable answers.

R2-specific note: R2's Infrequent Access class also has zero egress. The
storage discount is real (~$0.01/GB-month vs $0.015 standard) and there is
no retrieval fee, unlike S3-IA which charges $0.01/GB on read.

## 6. Configuration shape

The engine reads four env vars at boot. The credentials never enter the
fly.toml — they ride `fly secrets set`.

| Env var                       | Required for     | Example                                                  |
|-------------------------------|------------------|----------------------------------------------------------|
| `BASIN_STORAGE_BACKEND`       | always (default `local`) | `r2` / `s3` / `local` / `memory`                  |
| `BASIN_STORAGE_BUCKET`        | `r2`, `s3`       | `basin-engine-dev`                                       |
| `BASIN_STORAGE_ENDPOINT`      | `r2`             | `https://<account-id>.r2.cloudflarestorage.com`          |
| `BASIN_STORAGE_REGION`        | `r2`, `s3`       | `auto` (R2) or `us-east-1` (S3)                          |
| `BASIN_STORAGE_ACCESS_KEY_ID` | `r2`, `s3` (secret) | from `fly secrets set`                                |
| `BASIN_STORAGE_SECRET_ACCESS_KEY` | `r2`, `s3` (secret) | from `fly secrets set`                            |
| `BASIN_STORAGE_ROOT_PREFIX`   | optional         | `warehouse` — sub-key all tenant data under one prefix   |

In `services/basin-server/src/main.rs` the existing `LocalFileSystem`
construction grows a small dispatch:

```rust
let store: Arc<dyn ObjectStore> = match std::env::var("BASIN_STORAGE_BACKEND")
    .as_deref()
    .unwrap_or("local")
{
    "local" => Arc::new(LocalFileSystem::new_with_prefix(&cfg.data_dir)?),
    "memory" => Arc::new(object_store::memory::InMemory::new()),
    "r2" | "s3" => {
        let backend = basin_storage::backends::r2::R2Config::from_env()?;
        backend.build_object_store()?
    }
    other => bail!("unknown BASIN_STORAGE_BACKEND={other}"),
};
```

`R2Config::from_env` does the env-var parsing and produces a ready
`Arc<dyn ObjectStore>` via the `AmazonS3Builder` path in §3. The same
helper covers S3 (different defaults for `region`; no required endpoint).

The WAL takes the same `Arc<dyn ObjectStore>` and can either share the
Parquet bucket (different prefix) or use a separate bucket — controlled by
`BASIN_WAL_BACKEND` mirroring `BASIN_STORAGE_BACKEND`. For Fly's case the
NVMe-backed local WAL stays — durability is local-fsync — and R2 is only
the eventual flush target.

## 7. What this design does NOT change

- The hot-path append latency. The WAL still fsyncs locally and acks
  before R2 sees anything. R2 only enters the picture on the background
  flush + on Parquet writes (which are already async, batched, and
  off-thread).
- The tenant isolation invariant. Every key still goes through
  `paths::data_file_key` and begins with `tenants/{tenant}/`. R2 is
  one bucket, many tenants, prefix-isolated — same as the S3 model.
- The catalog. R2 stores opaque keys; the catalog is unchanged.
- Tests. `InMemory` and `LocalFileSystem` remain the test backends; no
  test needs network or real R2 credentials.

## 8. Open questions (out of scope for this skeleton)

- Multi-region buckets. R2's "jurisdictional" bucket regions (`auto`
  picks Cloudflare's choice; `eu`, `fedramp` are pinned) — do we want to
  pin per-tenant? Likely yes for compliance, deferred.
- Signed URLs for direct-from-edge reads (so the SDK could skip the
  engine entirely on big Parquet downloads). R2 supports presigned URLs
  via the S3 API; this is a basin-cloud surface, not basin-engine.
- Per-tenant credentials. Currently one bucket-wide key; R2 supports
  bucket-scoped keys, but per-tenant scoping would require Cloudflare API
  Tokens with object-prefix scoping — out of scope for v0.1.
