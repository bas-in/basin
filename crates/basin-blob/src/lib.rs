//! `basin-blob` — catalog-backed object storage for Basin (ADR 0021).
//!
//! Objects are rows in `storage.objects`; access control reuses the RLS
//! engine; bytes live in the same `object_store` the engine uses under the
//! path `<project_prefix>/storage/<bucket>/<object_path>`.
//!
//! # Crate structure
//!
//! - [`model`] — `Bucket` and `Object` row types (the data model).
//! - [`paths`] — object-key construction helpers (path convention).
//! - [`store`] — [`BlobStore`], the primary storage-ops API.
//! - [`error`] — crate-local error type.
//!
//! # Scope note (Phase 5.17.A)
//!
//! This crate intentionally contains **no HTTP layer** and **no RLS
//! evaluation** — those land in `basin-rest` (5.17.B) and Phase 5.17.C
//! respectively.  `BlobStore` takes an `Arc<dyn ObjectStore>` so callers
//! supply the already-configured store (no reach into `basin-engine`).

#![forbid(unsafe_code)]

pub mod error;
pub mod mime;
pub mod model;
pub mod paths;
pub mod postgres;
pub mod rls;
pub mod signing;
pub mod store;

pub use error::{BlobError, Result};
pub use mime::sniff as mime_sniff;
pub use model::{Bucket, BucketId, Object, ObjectId};
pub use postgres::PostgresBlobCatalog;
pub use rls::{
    CallerCtx, ObjectPolicy, ObjectPolicyCommand, ObjectRlsStore, ObjectUsing,
};
pub use signing::BlobSigningSecret;
pub use store::{BlobCatalog, BlobStore};
