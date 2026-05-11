//! Object-store backend constructors.
//!
//! The crate is backend-agnostic — `StorageConfig::object_store` accepts
//! any `Arc<dyn ObjectStore>`. This module is a thin convenience layer for
//! the `services/basin-server` binary (and downstream operators) that wants
//! a single call site for "build me an object store from env vars".
//!
//! Today's contents:
//!
//! - [`r2`] — Cloudflare R2 / Tigris / AWS S3 / MinIO etc. (same builder,
//!   different endpoint / region defaults). Picked by
//!   `BASIN_STORAGE_BACKEND=r2|s3|tigris`, or inferred from Fly's
//!   `AWS_ENDPOINT_URL_S3` when no explicit backend is set. The config
//!   type is `S3LikeConfig` (with `R2Config` retained as a
//!   backward-compat alias).
//!
//! `local` and `memory` backends don't live here because they don't need
//! configuration plumbing — `LocalFileSystem::new_with_prefix(path)` and
//! `InMemory::new()` are one-liners at the call site.
//!
//! See `docs/scaling/object-storage.md` for the full design.

pub mod r2;
