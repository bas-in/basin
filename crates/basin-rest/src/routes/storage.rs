//! `/storage/v1/*` handlers — object-storage HTTP surface (ADR 0021, Phase 5.17.B).
//!
//! ## Routes
//!
//! ```text
//! POST   /storage/v1/object/:bucket/:path*           — upload (JWT required)
//! GET    /storage/v1/object/:bucket/:path*           — download (JWT required)
//! DELETE /storage/v1/object/:bucket/:path*           — single delete (JWT required)
//! GET    /storage/v1/object/public/:bucket/:path*    — fast path for public buckets (no JWT)
//! POST   /storage/v1/object/list/:bucket             — list objects (prefix in body; JWT required)
//! DELETE /storage/v1/object/:bucket                  — bulk delete (prefixes[] in body; JWT required)
//! POST   /storage/v1/bucket                          — create bucket
//! GET    /storage/v1/bucket/:name                    — get bucket metadata
//! DELETE /storage/v1/bucket/:name                    — delete bucket + purge objects
//! ```
//!
//! ## Auth seam
//!
//! All routes except `GET /storage/v1/object/public/…` require a valid JWT
//! (verified via [`crate::server::authorize`]).  Public-bucket reads are
//! gateless but still validate that the bucket's `public` flag is true;
//! private buckets reject the request with 401.
//!
//! // 5.17.C: RLS — replace the JWT-gate seam below with `storage.objects`
//! //           RLS evaluation once Phase 5.17.C lands.
//!
//! ## Persistence
//!
//! The catalog backend is chosen at construction (`BlobStore<Arc<dyn BlobCatalog>>`):
//! a durable `PostgresBlobCatalog` when `BASIN_BLOB_PG_URL` is set (the only
//! correct backend for multi-replica deployments — closes #54 P0), otherwise an
//! in-memory catalog for tests / single-process dev.
//!
//! ## MIME sniffing
//!
//! The content-type is taken from the `Content-Type` request header when
//! present, then verified against the bucket's `allowed_mime_types[]` list by
//! `BlobStore::put_object`.  Basin-rest never trusts a file extension for MIME.
//!
//! ## Path safety
//!
//! `basin-blob`'s `paths::blob_key` rejects `..` traversal; the HTTP layer
//! passes the raw `*path` capture directly so the guard fires at the earliest
//! possible point.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_blob::{
    model::Bucket,
    rls::{self as blob_rls, CallerCtx, ObjectPolicyCommand},
    BlobError,
};
use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// Shared BlobStore type alias for the in-memory catalog path
// ---------------------------------------------------------------------------

// DefaultBlobStore is used as the concrete type in server::Inner.

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct CreateBucketRequest {
    pub name: String,
    #[serde(default)]
    pub public: bool,
    pub file_size_limit: Option<u64>,
    #[serde(default)]
    pub allowed_mime_types: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BucketResponse {
    pub id: String,
    pub name: String,
    pub public: bool,
    pub file_size_limit: Option<u64>,
    pub allowed_mime_types: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Bucket> for BucketResponse {
    fn from(b: Bucket) -> Self {
        Self {
            id: b.id.to_string(),
            name: b.name,
            public: b.public,
            file_size_limit: b.file_size_limit,
            allowed_mime_types: b.allowed_mime_types,
            created_at: b.created_at.to_rfc3339(),
            updated_at: b.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListObjectsRequest {
    #[serde(default)]
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObjectResponse {
    pub id: String,
    pub bucket_id: String,
    pub path: String,
    pub size: u64,
    pub mime_type: Option<String>,
    pub metadata: Value,
    pub owner: Option<String>,
    pub etag: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<basin_blob::Object> for ObjectResponse {
    fn from(o: basin_blob::Object) -> Self {
        Self {
            id: o.id.to_string(),
            bucket_id: o.bucket_id.to_string(),
            path: o.path,
            size: o.size,
            mime_type: o.mime_type,
            metadata: o.metadata,
            owner: o.owner.map(|u| u.to_string()),
            etag: o.etag,
            created_at: o.created_at.to_rfc3339(),
            updated_at: o.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkDeleteRequest {
    pub prefixes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

pub(crate) fn blob_err_to_api_pub(e: BlobError) -> ApiError {
    blob_err_to_api(e)
}

fn blob_err_to_api(e: BlobError) -> ApiError {
    match e {
        BlobError::BucketNotFound(name) => ApiError::not_found(format!("bucket not found: {name}")),
        BlobError::ObjectNotFound { bucket, path } => {
            ApiError::not_found(format!("object not found: {bucket}/{path}"))
        }
        BlobError::BucketAlreadyExists(name) => {
            ApiError::invalid(format!("bucket already exists: {name}"))
        }
        BlobError::InvalidPath(msg) => ApiError::invalid(format!("invalid path: {msg}")),
        BlobError::MimeTypeNotAllowed(mt) => {
            ApiError::invalid(format!("mime type not allowed: {mt}"))
        }
        BlobError::PayloadTooLarge { size, limit } => {
            ApiError::new(
                crate::errors::ErrorCode::PayloadTooLarge,
                format!("payload too large: {size} bytes exceeds limit of {limit}"),
            )
        }
        BlobError::ObjectStore(e) => ApiError::internal(format!("object store error: {e}")),
        BlobError::Serde(e) => ApiError::invalid(format!("serialisation error: {e}")),
        BlobError::Catalog(msg) => ApiError::internal(format!("blob catalog error: {msg}")),
    }
}

/// Parse project_id from the JWT claims.
fn project_from_claims(claims: &basin_auth::Claims) -> ProjectId {
    claims.project_id
}

/// Compute a SHA-256 hex etag over the body bytes.
fn compute_etag(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

// ---------------------------------------------------------------------------
// Bucket CRUD
// ---------------------------------------------------------------------------

/// `POST /storage/v1/bucket` — create a bucket.
#[axum::debug_handler]
pub(crate) async fn create_bucket(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(body): Json<CreateBucketRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    let mut bucket = Bucket::new(body.name);
    bucket.public = body.public;
    bucket.file_size_limit = body.file_size_limit;
    bucket.allowed_mime_types = body.allowed_mime_types;

    let created = state
        .blob_store
        .create_bucket(&project, bucket)
        .await
        .map_err(blob_err_to_api)?;

    let resp: BucketResponse = created.into();
    Ok((StatusCode::CREATED, Json(resp)).into_response())
}

/// `GET /storage/v1/bucket/:name` — get bucket metadata.
#[axum::debug_handler]
pub(crate) async fn get_bucket(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    let bucket = state
        .blob_store
        .get_bucket(&project, &name)
        .await
        .map_err(blob_err_to_api)?;

    let resp: BucketResponse = bucket.into();
    Ok(Json(resp).into_response())
}

/// `DELETE /storage/v1/bucket/:name` — delete a bucket and purge all of its
/// object bytes from `object_store` (closes #54 P0 — DELETE bucket no longer
/// leaks orphaned blobs).
#[axum::debug_handler]
pub(crate) async fn delete_bucket(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = claims.project_id;
    // Drop the bucket + every object row and purge each object's bytes from
    // object_store. Closes the #54 P0 leak where DELETE bucket left orphaned
    // blobs behind. Returns 404 if the bucket does not exist.
    let purged = state
        .blob_store
        .delete_bucket(&project, &name)
        .await
        .map_err(blob_err_to_api)?;
    Ok((
        StatusCode::OK,
        Json(json!({"message": "bucket deleted", "objects_purged": purged})),
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Object upload / download / delete
// ---------------------------------------------------------------------------

/// `POST /storage/v1/object/:bucket/:path*` — upload a blob (JWT-gated).
///
/// 5.17.C: The owner field is stamped from `auth.uid()` at upload time.
/// INSERT-command policies are not evaluated here — Basin sets the owner
/// automatically, and an authenticated upload is always permitted (the bucket
/// access gate is the JWT check above).
#[axum::debug_handler]
pub(crate) async fn upload_object(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((bucket, path)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    // Sniff MIME from the Content-Type header (never trust the file extension).
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string());

    let etag = compute_etag(&body);
    let owner = Some(claims.user_id);

    let object = state
        .blob_store
        .put_object(
            &project,
            &bucket,
            &path,
            body,
            mime,
            Value::Null,
            owner,
            etag,
        )
        .await
        .map_err(blob_err_to_api)?;

    let resp: ObjectResponse = object.into();
    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// `GET /storage/v1/object/:bucket/:path*` — download a blob (JWT-gated).
///
/// 5.17.C: RLS — gate on `storage.objects` policy (owner = auth.uid() etc.).
/// Public buckets bypass RLS on the `/public/` route; this route is JWT-gated
/// and always evaluates policies when RLS is enabled on the project.
#[axum::debug_handler]
pub(crate) async fn download_object(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((bucket, path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    // 5.17.C: Check bucket public flag — public buckets bypass RLS for reads.
    let bucket_meta = state
        .blob_store
        .get_bucket(&project, &bucket)
        .await
        .map_err(blob_err_to_api)?;

    let (meta, data) = state
        .blob_store
        .get_object(&project, &bucket, &path)
        .await
        .map_err(blob_err_to_api)?;

    // 5.17.C: Enforce RLS unless the bucket is public.
    if !bucket_meta.public {
        let caller = CallerCtx::authenticated(claims.user_id);
        let (rls_enabled, applicable) = state
            .blob_store
            .objects_rls_applicable(&project, ObjectPolicyCommand::Select, &caller.role);
        blob_rls::check_object_access(rls_enabled, &applicable, &meta, &caller, ObjectPolicyCommand::Select)
            .map_err(|msg| ApiError::forbidden(msg))?;
    }

    let mut builder = axum::http::Response::builder().status(StatusCode::OK);

    if let Some(ref ct) = meta.mime_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    } else {
        builder = builder.header(
            header::CONTENT_TYPE,
            "application/octet-stream",
        );
    }
    builder = builder.header(header::ETAG, &meta.etag);
    builder = builder.header(header::CONTENT_LENGTH, data.len().to_string());

    let response = builder
        .body(axum::body::Body::from(data))
        .map_err(|e| ApiError::internal(format!("response build error: {e}")))?;
    Ok(response)
}

/// `DELETE /storage/v1/object/:bucket/:path*` — single delete (JWT-gated).
///
/// 5.17.C: RLS — gate on `storage.objects` policy before deleting.
#[axum::debug_handler]
pub(crate) async fn delete_object(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((bucket, path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    // 5.17.C: Fetch the object metadata to evaluate RLS before deleting.
    let caller = CallerCtx::authenticated(claims.user_id);
    let (rls_enabled, applicable) = state
        .blob_store
        .objects_rls_applicable(&project, ObjectPolicyCommand::Delete, &caller.role);

    if rls_enabled {
        // Load the object row to evaluate the USING predicate.
        let (meta, _) = state
            .blob_store
            .get_object(&project, &bucket, &path)
            .await
            .map_err(blob_err_to_api)?;
        blob_rls::check_object_access(rls_enabled, &applicable, &meta, &caller, ObjectPolicyCommand::Delete)
            .map_err(|msg| ApiError::forbidden(msg))?;
    }

    state
        .blob_store
        .delete_object(&project, &bucket, &path)
        .await
        .map_err(blob_err_to_api)?;

    Ok((StatusCode::OK, Json(json!({"message": "object deleted"}))).into_response())
}

/// `GET /storage/v1/object/public/:project_id/:bucket/:path*` — public-bucket fast path (no JWT).
///
/// The project ID is embedded in the URL so the server can identify the project
/// without a bearer token.  Returns 401 when the bucket's `public` flag is
/// false — never falls through to JWT auth.
///
/// ## Auth / RLS chain (6.SEC.P1 fix)
///
/// The request is evaluated with `CallerCtx::anonymous()` (`role = "anon"`,
/// `uid = None`) — the same RLS path the authenticated `download_object` uses,
/// just with the anonymous principal.  Concretely:
///
/// 1. Private bucket (`public = false`) → 401 before any object is fetched.
/// 2. Public bucket, RLS off → permit (same behaviour as before the fix).
/// 3. Public bucket, RLS on, no applicable `SELECT` policy → 403 (default-deny).
/// 4. Public bucket, RLS on, matching policy → permit.
///
/// This closes the bypass where an operator could mark a bucket `public = true`
/// and add an object-level RLS policy (e.g. `FOR ROLE anon USING (metadata->>'restricted' IS NULL)`)
/// that would be completely skipped by the old alias path.
#[axum::debug_handler]
pub(crate) async fn download_public_object(
    State(state): State<Arc<Inner>>,
    _headers: HeaderMap,
    Path((project_str, bucket, path)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    // Public path: no JWT. Parse the project from the path segment.
    let project: ProjectId = project_str
        .parse()
        .map_err(|_| ApiError::not_found(format!("project not found: {project_str}")))?;

    // Verify the bucket is public before serving bytes.
    let bucket_meta = state
        .blob_store
        .get_bucket(&project, &bucket)
        .await
        .map_err(blob_err_to_api)?;

    if !bucket_meta.public {
        return Err(ApiError::unauthenticated(
            "bucket is private; use the authenticated endpoint",
        ));
    }

    let (obj_meta, data) = state
        .blob_store
        .get_object(&project, &bucket, &path)
        .await
        .map_err(blob_err_to_api)?;

    // 6.SEC.P1 fix: evaluate object-level RLS with the anonymous principal so
    // that operator-configured `anon`-role policies are enforced here, just as
    // they would be on the authenticated path.  This closes the alias bypass
    // where the public path silently skipped all policy evaluation.
    let caller = CallerCtx::anonymous();
    let (rls_enabled, applicable) = state
        .blob_store
        .objects_rls_applicable(&project, ObjectPolicyCommand::Select, &caller.role);
    blob_rls::check_object_access(rls_enabled, &applicable, &obj_meta, &caller, ObjectPolicyCommand::Select)
        .map_err(|msg| ApiError::forbidden(msg))?;

    let mut builder = axum::http::Response::builder().status(StatusCode::OK);

    if let Some(ref ct) = obj_meta.mime_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/octet-stream");
    }
    builder = builder.header(header::ETAG, &obj_meta.etag);
    builder = builder.header(header::CONTENT_LENGTH, data.len().to_string());

    let response = builder
        .body(axum::body::Body::from(data))
        .map_err(|e| ApiError::internal(format!("response build error: {e}")))?;
    Ok(response)
}

/// `POST /storage/v1/object/list/:bucket` — list objects with optional prefix (JWT-gated).
///
/// v1 lists from the in-memory catalog; full pagination is a follow-up.
/// 5.17.C: RLS — filter result set by `storage.objects` policy after JWT.
#[axum::debug_handler]
pub(crate) async fn list_objects(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    body: Option<Json<ListObjectsRequest>>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    let req = body.map(|Json(r)| r).unwrap_or(ListObjectsRequest {
        prefix: None,
        limit: None,
        offset: None,
    });

    // Verify bucket exists and check public flag.
    let bucket_meta = state
        .blob_store
        .get_bucket(&project, &bucket)
        .await
        .map_err(blob_err_to_api)?;

    // List from the catalog via the list_objects helper on the catalog.
    let objects = state
        .blob_store
        .list_objects(&project, &bucket, req.prefix.as_deref())
        .await
        .map_err(blob_err_to_api)?;

    // 5.17.C: Apply RLS filter. Public buckets skip the filter for reads.
    let filtered = if bucket_meta.public {
        objects
    } else {
        let caller = CallerCtx::authenticated(claims.user_id);
        let (rls_enabled, applicable) = state
            .blob_store
            .objects_rls_applicable(&project, ObjectPolicyCommand::Select, &caller.role);
        blob_rls::filter_objects(rls_enabled, &applicable, objects, &caller)
    };

    let limit = req.limit.unwrap_or(filtered.len());
    let offset = req.offset.unwrap_or(0);
    let page: Vec<ObjectResponse> = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(ObjectResponse::from)
        .collect();

    Ok(Json(page).into_response())
}

/// `DELETE /storage/v1/object/:bucket` with `{prefixes: []}` body — bulk delete (JWT-gated).
///
/// Deletes every object whose path matches one of the supplied prefixes.
/// 5.17.C: RLS — each object is individually checked against the DELETE policy.
/// Objects that fail the policy check are skipped (not counted as deleted).
#[axum::debug_handler]
pub(crate) async fn bulk_delete_objects(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Json(body): Json<BulkDeleteRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = project_from_claims(&claims);

    // Verify bucket exists.
    let _b = state
        .blob_store
        .get_bucket(&project, &bucket)
        .await
        .map_err(blob_err_to_api)?;

    // 5.17.C: Build RLS context once for the entire bulk operation.
    let caller = CallerCtx::authenticated(claims.user_id);
    let (rls_enabled, applicable) = state
        .blob_store
        .objects_rls_applicable(&project, ObjectPolicyCommand::Delete, &caller.role);

    let mut deleted = 0usize;
    for prefix in &body.prefixes {
        let matching = state
            .blob_store
            .list_objects(&project, &bucket, Some(prefix))
            .await
            .map_err(blob_err_to_api)?;
        for obj in matching {
            // 5.17.C: Skip objects the caller's policy doesn't permit deleting.
            if blob_rls::check_object_access(
                rls_enabled,
                &applicable,
                &obj,
                &caller,
                ObjectPolicyCommand::Delete,
            )
            .is_err()
            {
                continue;
            }
            state
                .blob_store
                .delete_object(&project, &bucket, &obj.path)
                .await
                .map_err(blob_err_to_api)?;
            deleted += 1;
        }
    }

    Ok(Json(json!({"deleted": deleted})).into_response())
}
