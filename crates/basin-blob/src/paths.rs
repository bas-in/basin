//! Object-key construction for `basin-blob`.
//!
//! All blob bytes live in the **same** `object_store` the engine uses for
//! Parquet / Vortex data files.  To prevent key collisions between engine
//! data files and user blobs the path convention is:
//!
//! ```text
//! projects/<project_ulid>/storage/<bucket_name>/<object_path>
//! ```
//!
//! where:
//! - `projects/<project_ulid>/` is the project-isolation prefix shared with
//!   the engine (`basin-storage`).
//! - `storage/` is a fixed segment that separates user blobs from engine
//!   table data (which lives under `tables/`).
//! - `<bucket_name>` is the bucket's human-readable name.
//! - `<object_path>` is the user-supplied logical path (e.g. `avatars/a.png`).
//!
//! **Path safety**: [`blob_key`] and [`bucket_prefix`] validate that
//! `object_path` does not contain `..` segments and does not start/end with
//! `/`.  An [`Err`] is returned for invalid paths so callers are forced to
//! handle the traversal-guard early.

use basin_common::ProjectId;
use object_store::path::Path as ObjectPath;

use crate::error::{BlobError, Result};

const PROJECTS_SEGMENT: &str = "projects";
const STORAGE_SEGMENT: &str = "storage";

/// Validate a user-supplied object path.
///
/// Rules:
/// - Must not be empty.
/// - Must not start or end with `/`.
/// - Must not contain a `..` path component.
fn validate_object_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(BlobError::InvalidPath(
            "object path must not be empty".to_string(),
        ));
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(BlobError::InvalidPath(format!(
            "object path must not start or end with '/': {path}"
        )));
    }
    for component in path.split('/') {
        if component == ".." {
            return Err(BlobError::InvalidPath(format!(
                "object path must not contain '..': {path}"
            )));
        }
    }
    Ok(())
}

/// Build the absolute object-store key for a stored blob.
///
/// Layout:
/// `projects/<project>/storage/<bucket>/<object_path>`
///
/// Returns `Err(BlobError::InvalidPath)` if `object_path` is unsafe.
pub fn blob_key(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    bucket_name: &str,
    object_path: &str,
) -> Result<ObjectPath> {
    validate_object_path(object_path)?;
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p = p.child(project.as_prefix().as_str());
    p = p.child(STORAGE_SEGMENT);
    p = p.child(bucket_name);
    // object_path may contain '/' (directory structure inside a bucket);
    // split on '/' and append each segment so ObjectPath normalises correctly.
    for segment in object_path.split('/') {
        p = p.child(segment);
    }
    Ok(p)
}

/// Build the prefix under which all objects in one bucket live.
///
/// Layout: `projects/<project>/storage/<bucket>/`
///
/// Useful for listing all objects in a bucket.
pub fn bucket_prefix(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    bucket_name: &str,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p = p.child(project.as_prefix().as_str());
    p = p.child(STORAGE_SEGMENT);
    p.child(bucket_name)
}

/// Build the prefix under which all blob storage for a project lives.
///
/// Layout: `projects/<project>/storage/`
pub fn project_storage_prefix(root: Option<&ObjectPath>, project: &ProjectId) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p = p.child(project.as_prefix().as_str());
    p.child(STORAGE_SEGMENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_key_layout() {
        let project = ProjectId::new();
        let key = blob_key(None, &project, "avatars", "alice/photo.png").unwrap();
        let s = key.as_ref();
        assert!(
            s.starts_with(&format!(
                "projects/{project}/storage/avatars/alice/photo.png"
            )),
            "unexpected key: {s}"
        );
    }

    #[test]
    fn blob_key_with_root_prefix() {
        let project = ProjectId::new();
        let root = ObjectPath::from("warehouse");
        let key = blob_key(Some(&root), &project, "docs", "readme.md").unwrap();
        assert!(key.as_ref().starts_with("warehouse/projects/"));
    }

    #[test]
    fn blob_key_rejects_traversal() {
        let project = ProjectId::new();
        assert!(blob_key(None, &project, "b", "../evil").is_err());
        assert!(blob_key(None, &project, "b", "a/../../etc/passwd").is_err());
    }

    #[test]
    fn blob_key_rejects_leading_slash() {
        let project = ProjectId::new();
        assert!(blob_key(None, &project, "b", "/secret").is_err());
    }

    #[test]
    fn blob_key_rejects_trailing_slash() {
        let project = ProjectId::new();
        assert!(blob_key(None, &project, "b", "folder/").is_err());
    }

    #[test]
    fn blob_key_rejects_empty_path() {
        let project = ProjectId::new();
        assert!(blob_key(None, &project, "b", "").is_err());
    }

    #[test]
    fn bucket_prefix_layout() {
        let project = ProjectId::new();
        let prefix = bucket_prefix(None, &project, "avatars");
        assert_eq!(
            prefix.as_ref(),
            format!("projects/{project}/storage/avatars")
        );
    }

    #[test]
    fn project_storage_prefix_layout() {
        let project = ProjectId::new();
        let prefix = project_storage_prefix(None, &project);
        assert_eq!(prefix.as_ref(), format!("projects/{project}/storage"));
    }

    #[test]
    fn blob_keys_always_project_scoped() {
        for _ in 0..64 {
            let project = ProjectId::new();
            let key = blob_key(None, &project, "b", "f.bin").unwrap();
            let expected_prefix = format!("projects/{project}/");
            assert!(
                key.as_ref().starts_with(&expected_prefix),
                "key missing project prefix: {}",
                key
            );
        }
    }
}
