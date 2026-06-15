//! Storage client — bindings for `/storage/v1/*` (ADR 0021 surface).
//!
//! Routes verified against `crates/basin-rest/src/server.rs:435-490`
//! (handlers in `routes/storage.rs` and `storage_sign.rs`):
//!
//! - `POST   /storage/v1/bucket`                                  create bucket
//! - `GET    /storage/v1/bucket/:name`                            bucket metadata
//! - `DELETE /storage/v1/bucket/:name`                            delete + purge
//! - `POST   /storage/v1/object/:bucket/*path`                    upload (JWT)
//! - `GET    /storage/v1/object/:bucket/*path`                    download (JWT)
//! - `DELETE /storage/v1/object/:bucket/*path`                    delete (JWT)
//! - `POST   /storage/v1/object/list/:bucket`                     list
//! - `DELETE /storage/v1/object/:bucket`                          bulk delete
//! - `GET    /storage/v1/object/public/:project_id/:bucket/*path` public dl
//! - `POST   /storage/v1/object/sign/upload/:bucket/*path`        mint signed URL
//! - `GET    /storage/v1/object/sign/:project/:bucket/*path`      signed download

use reqwest::Method;
use serde_json::{json, Value};

use crate::error::BasinError;
use crate::http::{encode_object_path, urlencode, Transport};
use crate::types::{Bucket, SignedUrl, StorageObject};

/// A downloaded object with its bytes and a couple of useful headers.
#[derive(Debug, Clone)]
pub struct DownloadResult {
    /// Raw object bytes.
    pub data: Vec<u8>,
    /// `Content-Type` header, if present.
    pub content_type: Option<String>,
    /// `ETag` header, if present.
    pub etag: Option<String>,
}

/// Storage client for bucket-level operations. Scope to a bucket with
/// [`from_bucket`](Self::from_bucket).
#[derive(Clone)]
pub struct StorageClient {
    transport: Transport,
    project_id: Option<String>,
}

impl StorageClient {
    pub(crate) fn new(transport: Transport, project_id: Option<String>) -> Self {
        Self {
            transport,
            project_id,
        }
    }

    /// `POST /storage/v1/bucket` (201).
    pub async fn create_bucket(
        &self,
        name: &str,
        public: bool,
        file_size_limit: Option<i64>,
        allowed_mime_types: &[String],
    ) -> Result<Bucket, BasinError> {
        let body = json!({
            "name": name,
            "public": public,
            "file_size_limit": file_size_limit,
            "allowed_mime_types": allowed_mime_types,
        });
        self.transport
            .request_json(Method::POST, "/storage/v1/bucket", &[], Some(&body), true)
            .await
    }

    /// `GET /storage/v1/bucket/:name`.
    pub async fn get_bucket(&self, name: &str) -> Result<Bucket, BasinError> {
        let path = format!("/storage/v1/bucket/{}", urlencode(name));
        self.transport
            .request_json(Method::GET, &path, &[], None, true)
            .await
    }

    /// `DELETE /storage/v1/bucket/:name` — also purges the object bytes.
    pub async fn delete_bucket(&self, name: &str) -> Result<(), BasinError> {
        let path = format!("/storage/v1/bucket/{}", urlencode(name));
        self.transport
            .request_discard(Method::DELETE, &path, &[], None, true)
            .await
    }

    /// Return an object-level client scoped to one bucket.
    pub fn from_bucket(&self, bucket: &str) -> StorageBucketClient {
        StorageBucketClient {
            transport: self.transport.clone(),
            bucket: bucket.to_string(),
            project_id: self.project_id.clone(),
        }
    }
}

/// Object-level storage operations scoped to a single bucket.
#[derive(Clone)]
pub struct StorageBucketClient {
    transport: Transport,
    bucket: String,
    project_id: Option<String>,
}

impl StorageBucketClient {
    fn object_path(&self, path: &str) -> String {
        format!(
            "/storage/v1/object/{}/{}",
            urlencode(&self.bucket),
            encode_object_path(path)
        )
    }

    /// `POST /storage/v1/object/:bucket/*path` — upload raw bytes.
    pub async fn upload(
        &self,
        path: &str,
        data: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<StorageObject, BasinError> {
        let mut headers = Vec::new();
        if let Some(ct) = content_type {
            headers.push(("content-type".to_string(), ct.to_string()));
        }
        let resp = self
            .transport
            .request(
                Method::POST,
                &self.object_path(path),
                &[],
                None,
                Some(data),
                &headers,
                true,
            )
            .await?;
        let text = resp.text().await.map_err(BasinError::from)?;
        serde_json::from_str(&text).map_err(|e| BasinError::Decode(e.to_string()))
    }

    /// `GET /storage/v1/object/:bucket/*path` — authenticated download.
    pub async fn download(&self, path: &str) -> Result<DownloadResult, BasinError> {
        let resp = self
            .transport
            .request(Method::GET, &self.object_path(path), &[], None, None, &[], true)
            .await?;
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let data = resp.bytes().await.map_err(BasinError::from)?.to_vec();
        Ok(DownloadResult {
            data,
            content_type,
            etag,
        })
    }

    /// `DELETE /storage/v1/object/:bucket/*path` — single delete.
    pub async fn remove(&self, path: &str) -> Result<(), BasinError> {
        self.transport
            .request_discard(Method::DELETE, &self.object_path(path), &[], None, true)
            .await
    }

    /// `POST /storage/v1/object/list/:bucket`.
    pub async fn list(
        &self,
        prefix: Option<&str>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<StorageObject>, BasinError> {
        let body = json!({ "prefix": prefix, "limit": limit, "offset": offset });
        let path = format!("/storage/v1/object/list/{}", urlencode(&self.bucket));
        Ok(self
            .transport
            .request_json_opt(Method::POST, &path, &[], Some(&body), true)
            .await?
            .unwrap_or_default())
    }

    /// `DELETE /storage/v1/object/:bucket` with `{ prefixes: [...] }`.
    ///
    /// Bulk-deletes every object matching any prefix. RLS-denied objects are
    /// skipped server-side.
    pub async fn remove_by_prefixes(&self, prefixes: &[String]) -> Result<(), BasinError> {
        let body = json!({ "prefixes": prefixes });
        let path = format!("/storage/v1/object/{}", urlencode(&self.bucket));
        self.transport
            .request_discard(Method::DELETE, &path, &[], Some(&body), true)
            .await
    }

    /// Build the no-JWT public URL.
    ///
    /// `GET /storage/v1/object/public/:project_id/:bucket/*path`. Only works
    /// for buckets with `public = true`. Errors if no project id is known.
    pub fn get_public_url(&self, path: &str) -> Result<String, BasinError> {
        let project = self.project_id.as_ref().ok_or_else(|| {
            BasinError::invalid_request(
                "public URLs require a project id (set project_id on the client builder)",
            )
        })?;
        Ok(format!(
            "{}/storage/v1/object/public/{}/{}/{}",
            self.transport.base_url,
            urlencode(project),
            urlencode(&self.bucket),
            encode_object_path(path)
        ))
    }

    /// `POST /storage/v1/object/sign/upload/:bucket/*path` — mint a signed URL.
    ///
    /// TTL is capped at 7 days server-side; default `3600` s. Despite the
    /// `upload` literal in the path this mints a **download** URL — that
    /// segment only disambiguates the axum router (see `storage_sign.rs`).
    pub async fn create_signed_url(
        &self,
        path: &str,
        expires_in: i64,
    ) -> Result<SignedUrl, BasinError> {
        let body = json!({ "expires_in": expires_in });
        let req_path = format!(
            "/storage/v1/object/sign/upload/{}/{}",
            urlencode(&self.bucket),
            encode_object_path(path)
        );
        let data: Value = self
            .transport
            .request_json(Method::POST, &req_path, &[], Some(&body), true)
            .await?;
        Ok(SignedUrl::from_value(&data, &self.transport.base_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    fn bucket() -> StorageBucketClient {
        StorageBucketClient {
            transport: Transport::new("http://h".into(), None, Client::new()),
            bucket: "media".into(),
            project_id: Some("01PROJ".into()),
        }
    }

    #[test]
    fn object_path_encodes_segments() {
        let b = bucket();
        assert_eq!(
            b.object_path("a b/c.png"),
            "/storage/v1/object/media/a%20b/c.png"
        );
    }

    #[test]
    fn public_url_built_with_project() {
        let b = bucket();
        assert_eq!(
            b.get_public_url("x/y.png").unwrap(),
            "http://h/storage/v1/object/public/01PROJ/media/x/y.png"
        );
    }

    #[test]
    fn public_url_requires_project() {
        let mut b = bucket();
        b.project_id = None;
        assert!(b.get_public_url("x").is_err());
    }
}
