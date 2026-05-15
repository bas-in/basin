# Object Storage Scaling — S3-compatible backends (Tigris, R2, S3, MinIO)

Status: design + skeleton (2026-05-11)
Scope: how `basin-storage` and `basin-wal` reach a real object store in
deployed configurations, and which backends are supported.

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
  tier layout: `projects/{project}/tables/{table}/data/...` for hot,
  `projects/{project}/tables/{table}/cold/...` for cold. A single bucket-side
  lifecycle rule on the `cold/` prefix can transition class to S3-IA / R2-IA
  without any engine code change.
- `src/lib.rs::delete_stream_for` already has a special case for the
  `AmazonS3` backend (detected by `Debug` prefix) that rides the bulk
  `DeleteObjects` API. Tigris, R2, and other S3-compatible stores also print
  `"AmazonS3(...)"` (they *are* the AmazonS3 backend pointed at a different
  endpoint), so they all inherit this fast path for free.
- `basin-wal` (`crates/basin-wal/src/lib.rs::WalConfig`) takes the same
  `Arc<dyn ObjectStore>` shape. WAL segments live at
  `{root}/wal/{project}/{partition}/{ulid}.seg` — orthogonal prefix from
  Parquet data; lifecycle rules are configured separately.
- The workspace already pulls `object_store = { version = "0.11", features =
  ["aws", "gcp", "http"] }` (root `Cargo.toml` L71). The `aws` feature is
  what we need for S3-compatible backends; no new dep is required.

Today's only real-world wiring (in `services/basin-server/src/main.rs`)
builds a `LocalFileSystem` rooted at `BASIN_DATA_DIR`. This is honest for
single-machine deployments and the Fly volume case, but it doesn't scale
across machines or regions and it doesn't decouple compute from storage.
Everything below is a small extension to that wiring — no trait changes.

## 2. Backend cost comparison

The three most common cloud backends have materially different pricing curves:

| Item            | AWS S3 (us-east-1)         | Cloudflare R2          | Tigris (Fly-native)    |
|-----------------|----------------------------|------------------------|------------------------|
| Storage         | $0.023 / GB-month          | $0.015 / GB-month      | $0.02 / GB-month       |
| Egress to net   | $0.09 / GB (after 100 GB)  | **$0.00 / GB**         | $0.01 / GB             |
| Egress Fly-intl | $0.02 / GB (intra-region)  | $0.00 / GB             | **$0.00 / GB** (Fly ↔ Tigris) |
| Class A (write) | $0.005 / 1k                | $0.0045 / 1k           | $0.05 / 1M             |
| Class B (read)  | $0.0004 / 1k               | $0.00036 / 1k          | $0.005 / 1M            |
| Data ingress    | $0.00                      | $0.00                  | $0.00                  |

The basin-cloud managed service runs on **Fly.io + Tigris**. Tigris is Fly's
native S3-compatible store; traffic between Fly Machines and Tigris does not
egress to the public internet, keeping latency low and egress cost zero for
intra-Fly traffic.

For self-hosted deployments: `BASIN_STORAGE_BACKEND` selects per deployment.
All three backends above work, plus MinIO, Wasabi, Backblaze B2, and any
S3-compatible endpoint. `LocalFileSystem` remains the default for single-machine
and development setups.

## 3. S3-compatible backend: how to wire it

All S3-compatible endpoints (Tigris, R2, AWS S3, MinIO, etc.) use the same
`AmazonS3Builder` under the hood. Examples:

**Tigris** (basin-cloud default):

    endpoint: https://fly.storage.tigris.dev
    region:   auto

**Cloudflare R2**:

    endpoint: https://<account-id>.r2.cloudflarestorage.com
    region:   auto   # R2 requires "auto"; returns 400 on any other value

Credentials are an access-key-id / secret-pair generated in the provider's
dashboard or provisioned automatically (e.g. `fly storage create` sets
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_ENDPOINT_URL_S3`
automatically for Tigris).

Because `object_store::aws::AmazonS3Builder` already accepts a custom
endpoint, **every S3-compatible store is "just" an `AmazonS3` constructed with three knobs**:

```rust
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use object_store::ObjectStore;

fn build_s3_compatible(cfg: &S3LikeConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&cfg.endpoint)       // e.g. https://fly.storage.tigris.dev
        .with_region("auto")                // Tigris / R2 use "auto"; AWS uses a real region
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.access_key_id)
        .with_secret_access_key(&cfg.secret_access_key)
        // Required when not running behind a TLS-terminating proxy that
        // injects v2 path-style — Tigris / R2 expect virtual-hosted-style today.
        .with_virtual_hosted_style_request(true)
        .build()?;
    Ok(Arc::new(store))
}
```

There is no new trait, no new transport, no provider-specific feature flag. The
`AmazonS3` backend's `delete_stream` bulk path works against all S3-compatible
stores (they all implement the SigV4 `DeleteObjects` action).

Self-hostable example: `crates/basin-storage/src/backends/r2.rs` (see §6).

## 4. Migration path

There is no breaking migration. All existing backends keep working.

| `BASIN_STORAGE_BACKEND` | Backend                                  | Notes                  |
|-------------------------|------------------------------------------|------------------------|
| `local` (default)       | `object_store::local::LocalFileSystem`   | today's behaviour      |
| `s3`                    | `AmazonS3` with `us-east-1` (or env)     | classic AWS S3         |
| `r2`                    | `AmazonS3` with `auto` + R2 endpoint     | Cloudflare R2          |
| `tigris`                | `AmazonS3` with `auto` + Tigris endpoint | Fly-native; basin-cloud default |
| `memory` (tests)        | `object_store::memory::InMemory`         | unchanged              |

The catalog stores object keys verbatim. If you ever do want to migrate
real data between backends, the operation is a `rclone copy` against the
S3 API on both sides — Basin doesn't need to know.

## 5. Cold-tier story

The cold-tier mechanic is already implemented in `basin-storage` and lives
under the `cold/` segment of the table prefix. The operational setup is
provider-specific but the engine code is unchanged across backends:

- After N days (configurable, default 30) the compactor rewrites a hot
  Parquet file to its cold-tier sibling key via `paths::rewrite_to_cold`.
  See `src/tier.rs`.
- A bucket-side lifecycle rule on the `projects/*/tables/*/cold/` prefix
  flips the storage class to the infrequent-access tier for your provider
  (S3-IA on AWS S3, R2 Infrequent Access on Cloudflare R2, or provider
  equivalent). Setup is one-time via the provider dashboard or CLI.
- Old WAL segments follow the same shape: a lifecycle rule on the
  `wal/{project}/...` prefix transitions segments older than the
  configured retention window (today: drop entirely; tomorrow: cold-tier
  for replay-from-archive scenarios).
- The engine doesn't need to know which class a key is in — it just GETs
  the key and pays whatever the bucket policy charges. The catalog records
  `tier: Hot|Cold` so the planner can prefer hot files when both are
  acceptable answers.

## 6. Configuration shape

The engine reads four env vars at boot. The credentials never enter the
fly.toml — they ride `fly secrets set`.

| Env var                       | Required for     | Example                                                  |
|-------------------------------|------------------|----------------------------------------------------------|
| `BASIN_STORAGE_BACKEND`       | always (default `local`) | `tigris` / `r2` / `s3` / `local` / `memory`       |
| `BASIN_STORAGE_BUCKET`        | S3-compatible backends   | `basin-engine-dev`                                  |
| `BASIN_STORAGE_ENDPOINT`      | S3-compatible backends   | `https://fly.storage.tigris.dev` (Tigris) or `https://<account-id>.r2.cloudflarestorage.com` (R2) |
| `BASIN_STORAGE_REGION`        | S3-compatible backends   | `auto` (Tigris / R2) or `us-east-1` (S3)           |
| `BASIN_STORAGE_ACCESS_KEY_ID` | S3-compatible (secret)   | from `fly secrets set` or `fly storage create`      |
| `BASIN_STORAGE_SECRET_ACCESS_KEY` | S3-compatible (secret) | from `fly secrets set` or `fly storage create`  |
| `BASIN_STORAGE_ROOT_PREFIX`   | optional         | `warehouse` — sub-key all project data under one prefix   |

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

`S3LikeConfig::from_env` does the env-var parsing and produces a ready
`Arc<dyn ObjectStore>` via the `AmazonS3Builder` path in §3. The same
helper covers all S3-compatible backends (different endpoint / region defaults
per provider; Tigris defaults to `https://fly.storage.tigris.dev`; R2
requires an explicit endpoint; plain `s3` uses AWS defaults).

The WAL takes the same `Arc<dyn ObjectStore>` and can either share the
Parquet bucket (different prefix) or use a separate bucket — controlled by
`BASIN_WAL_BACKEND` mirroring `BASIN_STORAGE_BACKEND`. For Fly's case the
NVMe-backed local WAL stays — durability is local-fsync — and the object
store is only the eventual flush target.

## 7. What this design does NOT change

- The hot-path append latency. The WAL still fsyncs locally and acks
  before the object store sees anything. The object store only enters the
  picture on the background flush + on Parquet writes (which are already
  async, batched, and off-thread).
- The project isolation invariant. Every key still goes through
  `paths::data_file_key` and begins with `projects/{project}/`. The bucket is
  shared across projects, prefix-isolated — same model for all backends.
- The catalog. The object store holds opaque keys; the catalog is unchanged.
- Tests. `InMemory` and `LocalFileSystem` remain the test backends; no
  test needs network or real credentials.

## 8. Open questions (out of scope for this skeleton)

- Multi-region buckets. Tigris, R2, and S3 all offer cross-region replication
  at the bucket level — pin per-project? Likely yes for compliance, deferred.
- Signed URLs for direct-from-edge reads (so the SDK could skip the
  engine entirely on big Parquet downloads). All three backends support
  presigned URLs via the S3 API; this is a basin-cloud surface, not basin-engine.
- Per-project credentials. Currently one bucket-wide key; per-project scoping
  would require provider-specific IAM/token APIs — out of scope for v0.1.
