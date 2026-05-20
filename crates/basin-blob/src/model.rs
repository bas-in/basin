//! Data model for `storage.buckets` and `storage.objects`.
//!
//! These types are the canonical row representations for the two catalog
//! tables that make up the blob-storage data model (ADR 0021).  They are
//! intentionally plain Rust structs with `serde` derive so they can be
//! stored in any catalog backend (Postgres, in-memory for tests, etc.)
//! without pulling in a specific ORM.
//!
//! **`storage.buckets`**
//! ```text
//! id               uuid / UUIDv4
//! name             text  (unique per project)
//! public           bool  (true → reads bypass RLS)
//! file_size_limit  Option<u64>  bytes; None = unlimited
//! allowed_mime_types  Vec<String>  (empty = allow all)
//! created_at       DateTime<Utc>
//! updated_at       DateTime<Utc>
//! ```
//!
//! **`storage.objects`**
//! ```text
//! id          uuid / UUIDv4
//! bucket_id   uuid  (FK → storage.buckets.id)
//! path        text  (logical path within the bucket)
//! size        u64   (bytes)
//! mime_type   Option<String>
//! metadata    serde_json::Value  (arbitrary user metadata)
//! owner       Option<uuid>  (auth.uid() at upload time)
//! created_at  DateTime<Utc>
//! updated_at  DateTime<Utc>
//! etag        String  (hex-encoded MD5 or content hash)
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Newtype wrapping a [`Uuid`] that identifies a single bucket row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketId(pub Uuid);

impl BucketId {
    /// Generate a new random bucket ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for BucketId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for BucketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Newtype wrapping a [`Uuid`] that identifies a single object row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectId(pub Uuid);

impl ObjectId {
    /// Generate a new random object ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// storage.buckets
// ---------------------------------------------------------------------------

/// A row in `storage.buckets`.
///
/// One bucket lives entirely within a single project.  The `name` is unique
/// per project (the catalog layer is responsible for enforcing uniqueness).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    /// Stable identifier — never changes after creation.
    pub id: BucketId,

    /// Human-readable bucket name, unique within the project.
    pub name: String,

    /// When `true` read requests bypass RLS (no JWT required).
    pub public: bool,

    /// Maximum upload size in bytes.  `None` means unlimited.
    pub file_size_limit: Option<u64>,

    /// MIME types that may be stored in this bucket.
    /// An **empty** list means *all* types are allowed.
    pub allowed_mime_types: Vec<String>,

    /// Row creation timestamp (UTC).
    pub created_at: DateTime<Utc>,

    /// Last-modified timestamp (UTC).
    pub updated_at: DateTime<Utc>,
}

impl Bucket {
    /// Construct a new bucket with sensible defaults.
    ///
    /// * `public = false`
    /// * `file_size_limit = None` (unlimited)
    /// * `allowed_mime_types = []` (all types allowed)
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: BucketId::new(),
            name: name.into(),
            public: false,
            file_size_limit: None,
            allowed_mime_types: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Check whether the given MIME type is permitted by this bucket's policy.
    ///
    /// Returns `true` if the `allowed_mime_types` list is empty (= allow all)
    /// or if the list contains `mime_type`.
    pub fn allows_mime_type(&self, mime_type: &str) -> bool {
        self.allowed_mime_types.is_empty()
            || self.allowed_mime_types.iter().any(|m| m == mime_type)
    }

    /// Check whether the payload `size` (bytes) fits within this bucket's
    /// per-object size limit.  Returns `true` if no limit is configured.
    pub fn allows_size(&self, size: u64) -> bool {
        self.file_size_limit.map_or(true, |limit| size <= limit)
    }
}

// ---------------------------------------------------------------------------
// storage.objects
// ---------------------------------------------------------------------------

/// A row in `storage.objects`.
///
/// Each row represents exactly one stored blob.  The bytes themselves live in
/// `object_store` under the key produced by [`crate::paths::blob_key`].
/// Overwriting an existing path replaces the row and the bytes (no
/// versioning in v1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Object {
    /// Stable identifier.
    pub id: ObjectId,

    /// The bucket this object belongs to.
    pub bucket_id: BucketId,

    /// Logical path within the bucket (e.g. `"avatars/alice.png"`).
    /// Never starts or ends with `/`; never contains `..`.
    pub path: String,

    /// Byte length of the stored blob.
    pub size: u64,

    /// MIME type as reported by the client or sniffed server-side.
    pub mime_type: Option<String>,

    /// Arbitrary user-supplied JSON metadata (e.g. custom headers, tags).
    pub metadata: serde_json::Value,

    /// The `auth.uid()` at upload time, or `None` for anonymous uploads.
    pub owner: Option<Uuid>,

    /// Row creation timestamp (UTC).
    pub created_at: DateTime<Utc>,

    /// Last-modified timestamp (UTC).  Updated on overwrite.
    pub updated_at: DateTime<Utc>,

    /// Content hash / ETag (hex-encoded).  Suitable for `If-None-Match`
    /// caching and integrity checks.
    pub etag: String,
}

impl Object {
    /// Construct a new object row.
    ///
    /// `etag` should be the hex-encoded content hash (e.g. MD5 or SHA-256)
    /// computed over the payload bytes.  When not available, callers may pass
    /// an empty string and update it after the upload completes.
    pub fn new(
        bucket_id: BucketId,
        path: impl Into<String>,
        size: u64,
        mime_type: Option<String>,
        metadata: serde_json::Value,
        owner: Option<Uuid>,
        etag: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: ObjectId::new(),
            bucket_id,
            path: path.into(),
            size,
            mime_type,
            metadata,
            owner,
            created_at: now,
            updated_at: now,
            etag: etag.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_new_defaults() {
        let b = Bucket::new("avatars");
        assert_eq!(b.name, "avatars");
        assert!(!b.public);
        assert!(b.file_size_limit.is_none());
        assert!(b.allowed_mime_types.is_empty());
    }

    #[test]
    fn bucket_allows_mime_type_empty_list() {
        let b = Bucket::new("all-types");
        assert!(b.allows_mime_type("image/png"));
        assert!(b.allows_mime_type("application/octet-stream"));
    }

    #[test]
    fn bucket_allows_mime_type_restricted() {
        let mut b = Bucket::new("images-only");
        b.allowed_mime_types = vec!["image/png".to_string(), "image/jpeg".to_string()];
        assert!(b.allows_mime_type("image/png"));
        assert!(b.allows_mime_type("image/jpeg"));
        assert!(!b.allows_mime_type("text/plain"));
    }

    #[test]
    fn bucket_allows_size_no_limit() {
        let b = Bucket::new("unlimited");
        assert!(b.allows_size(u64::MAX));
    }

    #[test]
    fn bucket_allows_size_with_limit() {
        let mut b = Bucket::new("capped");
        b.file_size_limit = Some(1024);
        assert!(b.allows_size(1024));
        assert!(!b.allows_size(1025));
    }

    #[test]
    fn object_new_round_trip_serde() {
        let bucket_id = BucketId::new();
        let obj = Object::new(
            bucket_id,
            "folder/file.txt",
            42,
            Some("text/plain".to_string()),
            serde_json::json!({"custom": true}),
            None,
            "deadbeef",
        );
        let json = serde_json::to_string(&obj).unwrap();
        let back: Object = serde_json::from_str(&json).unwrap();
        assert_eq!(obj, back);
    }
}
