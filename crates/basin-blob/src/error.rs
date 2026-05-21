//! Crate-local error type for `basin-blob`.

use thiserror::Error;

/// The result type used throughout `basin-blob`.
pub type Result<T, E = BlobError> = std::result::Result<T, E>;

/// Errors that can occur in blob-storage operations.
#[derive(Debug, Error)]
pub enum BlobError {
    /// The requested bucket does not exist in this project.
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    /// The requested object does not exist in this bucket.
    #[error("object not found: {bucket}/{path}")]
    ObjectNotFound { bucket: String, path: String },

    /// A bucket with this name already exists in the project.
    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    /// The object path is invalid (e.g. contains `..` traversal).
    #[error("invalid object path: {0}")]
    InvalidPath(String),

    /// The MIME type is not in the bucket's `allowed_mime_types` list.
    #[error("mime type not allowed: {0}")]
    MimeTypeNotAllowed(String),

    /// The upload exceeds the bucket's `file_size_limit`.
    #[error("payload too large: {size} bytes exceeds limit of {limit}")]
    PayloadTooLarge { size: u64, limit: u64 },

    /// An error propagated from the underlying `object_store`.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Serialisation / deserialisation error (metadata JSON).
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// An error from the Postgres-backed catalog (connection, query, or
    /// migration failure).  Carries a human-readable message; the HTTP layer
    /// maps this to a 500.
    #[error("blob catalog error: {0}")]
    Catalog(String),
}
