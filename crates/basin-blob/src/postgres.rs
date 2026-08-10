//! Postgres-backed durable [`BlobCatalog`] implementation (closes #54 P0).
//!
//! The original `basin-rest` blob store used [`crate::store::InMemoryBlobCatalog`],
//! which keeps all bucket/object metadata in per-process `HashMap`s. That is
//! fatal for the multi-replica deployment the cloud roadmap targets:
//!
//! - A process restart loses every bucket and object row (the bytes survive in
//!   `object_store`, but become unreachable orphans).
//! - Two replicas behind a load balancer keep *independent* catalogs, so an
//!   upload routed to replica A is invisible to replica B — the customer-facing
//!   symptom is "my upload landed in a different bucket".
//! - `DELETE bucket` could not purge object bytes because the in-memory catalog
//!   had no durable enumeration of what the bucket held across replicas.
//!
//! [`PostgresBlobCatalog`] fixes all three by persisting `storage.buckets` and
//! `storage.objects` rows in Postgres, keyed by `(project_id, bucket, path)`.
//! Every replica shares the same database, so metadata is consistent and
//! durable across restarts.
//!
//! # Connection pooling
//!
//! Mirrors `basin-catalog`'s `postgres.rs`: a `deadpool_postgres::Pool` (default
//! 16 connections, override via `BASIN_BLOB_PG_POOL_SIZE`) with `Fast`
//! recycling. Each operation checks a connection out for the duration of the
//! call. `tokio_postgres::NoTls` is hard-coded for the PoC; production needs a
//! TLS connector at the connect site (same caveat as the catalog crate).
//!
//! # Schema
//!
//! Tables live in a dedicated Postgres schema (default `basin_blob`; the test
//! suite scopes each run to a unique schema). They are *separate* from the
//! `basin-catalog` schema so the blob catalog can share a Postgres instance
//! without coupling to the hot catalog crate's tables.
//!
//! | Table                    | Key                                | Notes                          |
//! |--------------------------|------------------------------------|--------------------------------|
//! | `<schema>.storage_buckets` | `(project_id, name)`             | One row per bucket.            |
//! | `<schema>.storage_objects` | `(project_id, bucket_id, path)`  | One row per object.            |
//!
//! Migration runs at `connect` time via `CREATE SCHEMA / TABLE IF NOT EXISTS`,
//! so a fresh database and a restart both converge to the same shape.

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

use basin_common::ProjectId;

use crate::{
    error::{BlobError, Result},
    model::{Bucket, BucketId, Object, ObjectId},
    store::BlobCatalog,
};

/// Default schema for blob catalog tables when none is supplied.
const DEFAULT_SCHEMA: &str = "basin_blob";

/// Default pool size if `BASIN_BLOB_PG_POOL_SIZE` is unset.
const DEFAULT_POOL_SIZE: usize = 16;
const POOL_SIZE_ENV: &str = "BASIN_BLOB_PG_POOL_SIZE";

/// Postgres-backed implementation of [`BlobCatalog`].
///
/// Cheap to wrap in [`std::sync::Arc`] and share across handlers. Backed by a
/// `deadpool_postgres::Pool` so concurrent storage operations don't queue
/// behind a single shared connection.
pub struct PostgresBlobCatalog {
    pool: Pool,
    schema: String,
}

impl std::fmt::Debug for PostgresBlobCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresBlobCatalog")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

fn pool_size_from_env() -> usize {
    std::env::var(POOL_SIZE_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_POOL_SIZE)
}

/// Reject schema identifiers that aren't a safe bare identifier so they can be
/// interpolated into DDL without an injection risk.
fn validate_schema_ident(schema: &str) -> Result<()> {
    let ok = !schema.is_empty()
        && schema.len() <= 63
        && schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && schema
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(BlobError::Catalog(format!(
            "invalid blob catalog schema identifier: {schema:?}"
        )))
    }
}

impl PostgresBlobCatalog {
    /// Connect using the default schema (`basin_blob`). Builds a pool and runs
    /// idempotent migrations before returning.
    pub async fn connect(conn_str: &str) -> Result<Self> {
        Self::connect_with_schema(conn_str, DEFAULT_SCHEMA).await
    }

    /// Connect into a caller-chosen schema. The test suite uses a unique schema
    /// per run so leftovers don't accumulate.
    pub async fn connect_with_schema(conn_str: &str, schema: &str) -> Result<Self> {
        validate_schema_ident(schema)?;
        let pg_config: tokio_postgres::Config = conn_str
            .parse()
            .map_err(|e| BlobError::Catalog(format!("postgres conn_str parse: {e}")))?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, NoTls, mgr_config);
        let pool = Pool::builder(manager)
            .max_size(pool_size_from_env())
            .build()
            .map_err(|e| BlobError::Catalog(format!("build blob pool: {e}")))?;
        let cat = Self {
            pool,
            schema: schema.to_owned(),
        };
        cat.migrate().await?;
        Ok(cat)
    }

    /// Wrap in an `Arc<dyn BlobCatalog>` for use with `BlobStore::new`.
    pub fn arc_dyn(self) -> std::sync::Arc<dyn BlobCatalog> {
        std::sync::Arc::new(self)
    }

    async fn client(&self) -> Result<deadpool_postgres::Client> {
        self.pool
            .get()
            .await
            .map_err(|e| BlobError::Catalog(format!("blob pool get: {e}")))
    }

    /// Run `CREATE SCHEMA / TABLE IF NOT EXISTS` for the blob catalog tables.
    /// Safe to call repeatedly (every replica calls it at startup).
    pub async fn migrate(&self) -> Result<()> {
        let schema = &self.schema;
        let stmts = [
            format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.storage_buckets (
                    project_id          TEXT NOT NULL,
                    id                  UUID NOT NULL,
                    name                TEXT NOT NULL,
                    public              BOOLEAN NOT NULL DEFAULT FALSE,
                    file_size_limit     BIGINT,
                    allowed_mime_types  JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at          TIMESTAMPTZ NOT NULL,
                    updated_at          TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (project_id, name)
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.storage_objects (
                    project_id   TEXT NOT NULL,
                    bucket_id    UUID NOT NULL,
                    id           UUID NOT NULL,
                    path         TEXT NOT NULL,
                    size         BIGINT NOT NULL,
                    mime_type    TEXT,
                    metadata     JSONB NOT NULL DEFAULT 'null'::jsonb,
                    owner        UUID,
                    etag         TEXT NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL,
                    updated_at   TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (project_id, bucket_id, path)
                )"
            ),
            // Lookup index for purge / list-by-bucket sweeps.
            format!(
                "CREATE INDEX IF NOT EXISTS storage_objects_bucket_idx
                 ON {schema}.storage_objects (project_id, bucket_id)"
            ),
        ];
        let client = self.client().await?;
        for stmt in stmts {
            client
                .batch_execute(&stmt)
                .await
                .map_err(|e| BlobError::Catalog(format!("blob migrate: {e}")))?;
        }
        Ok(())
    }

    fn row_to_bucket(row: &tokio_postgres::Row) -> Result<Bucket> {
        let id: uuid::Uuid = row.get("id");
        let name: String = row.get("name");
        let public: bool = row.get("public");
        let file_size_limit: Option<i64> = row.get("file_size_limit");
        let allowed_mime_types_json: serde_json::Value = row.get("allowed_mime_types");
        let allowed_mime_types: Vec<String> = serde_json::from_value(allowed_mime_types_json)?;
        Ok(Bucket {
            id: BucketId(id),
            name,
            public,
            // Clamp negatives defensively: a negative limit is meaningless.
            file_size_limit: file_size_limit.and_then(|v| u64::try_from(v).ok()),
            allowed_mime_types,
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn row_to_object(row: &tokio_postgres::Row) -> Result<Object> {
        let id: uuid::Uuid = row.get("id");
        let bucket_id: uuid::Uuid = row.get("bucket_id");
        let size: i64 = row.get("size");
        let metadata: serde_json::Value = row.get("metadata");
        Ok(Object {
            id: ObjectId(id),
            bucket_id: BucketId(bucket_id),
            path: row.get("path"),
            size: u64::try_from(size).unwrap_or(0),
            mime_type: row.get("mime_type"),
            metadata,
            owner: row.get("owner"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            etag: row.get("etag"),
        })
    }
}

#[async_trait]
impl BlobCatalog for PostgresBlobCatalog {
    async fn insert_bucket(&self, project: &ProjectId, bucket: Bucket) -> Result<()> {
        let sch = &self.schema;
        let client = self.client().await?;
        let file_size_limit: Option<i64> =
            bucket.file_size_limit.and_then(|v| i64::try_from(v).ok());
        let allowed_mime_types_json = serde_json::to_value(&bucket.allowed_mime_types)?;
        // ON CONFLICT DO NOTHING + a 0-row check turns "already exists" into the
        // BucketAlreadyExists error the in-memory impl returns.
        let inserted = client
            .execute(
                &format!(
                    "INSERT INTO {sch}.storage_buckets
                       (project_id, id, name, public, file_size_limit,
                        allowed_mime_types, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (project_id, name) DO NOTHING"
                ),
                &[
                    &project.to_string(),
                    &bucket.id.0,
                    &bucket.name,
                    &bucket.public,
                    &file_size_limit,
                    &allowed_mime_types_json,
                    &bucket.created_at,
                    &bucket.updated_at,
                ],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("insert_bucket: {e}")))?;
        if inserted == 0 {
            return Err(BlobError::BucketAlreadyExists(bucket.name));
        }
        Ok(())
    }

    async fn get_bucket_by_name(&self, project: &ProjectId, name: &str) -> Result<Option<Bucket>> {
        let sch = &self.schema;
        let client = self.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT id, name, public, file_size_limit, allowed_mime_types,
                            created_at, updated_at
                     FROM {sch}.storage_buckets
                     WHERE project_id = $1 AND name = $2"
                ),
                &[&project.to_string(), &name],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("get_bucket_by_name: {e}")))?;
        match row {
            Some(r) => Ok(Some(Self::row_to_bucket(&r)?)),
            None => Ok(None),
        }
    }

    async fn upsert_object(&self, project: &ProjectId, object: Object) -> Result<()> {
        let sch = &self.schema;
        let client = self.client().await?;
        let size: i64 = i64::try_from(object.size).unwrap_or(i64::MAX);
        // Overwrite-by-path: keep the original row id stable is not required by
        // the model (overwrites get a fresh row in-memory too), so ON CONFLICT
        // replaces the mutable columns.
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}.storage_objects
                       (project_id, bucket_id, id, path, size, mime_type,
                        metadata, owner, etag, created_at, updated_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                     ON CONFLICT (project_id, bucket_id, path) DO UPDATE SET
                        id         = EXCLUDED.id,
                        size       = EXCLUDED.size,
                        mime_type  = EXCLUDED.mime_type,
                        metadata   = EXCLUDED.metadata,
                        owner      = EXCLUDED.owner,
                        etag       = EXCLUDED.etag,
                        updated_at = EXCLUDED.updated_at"
                ),
                &[
                    &project.to_string(),
                    &object.bucket_id.0,
                    &object.id.0,
                    &object.path,
                    &size,
                    &object.mime_type,
                    &object.metadata,
                    &object.owner,
                    &object.etag,
                    &object.created_at,
                    &object.updated_at,
                ],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("upsert_object: {e}")))?;
        Ok(())
    }

    async fn get_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<Option<Object>> {
        let sch = &self.schema;
        let client = self.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT id, bucket_id, path, size, mime_type, metadata,
                            owner, etag, created_at, updated_at
                     FROM {sch}.storage_objects
                     WHERE project_id = $1 AND bucket_id = $2 AND path = $3"
                ),
                &[&project.to_string(), &bucket_id.0, &path],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("get_object_by_path: {e}")))?;
        match row {
            Some(r) => Ok(Some(Self::row_to_object(&r)?)),
            None => Ok(None),
        }
    }

    async fn delete_object_by_path(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        path: &str,
    ) -> Result<bool> {
        let sch = &self.schema;
        let client = self.client().await?;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.storage_objects
                     WHERE project_id = $1 AND bucket_id = $2 AND path = $3"
                ),
                &[&project.to_string(), &bucket_id.0, &path],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("delete_object_by_path: {e}")))?;
        Ok(n > 0)
    }

    async fn list_objects_by_prefix(
        &self,
        project: &ProjectId,
        bucket_id: BucketId,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>> {
        let sch = &self.schema;
        let client = self.client().await?;
        // LIKE on a literal prefix; escape the LIKE metacharacters so a path
        // containing `%` or `_` doesn't widen the match.
        let rows = match prefix {
            Some(p) => {
                let pattern = format!(
                    "{}%",
                    p.replace('\\', "\\\\")
                        .replace('%', "\\%")
                        .replace('_', "\\_")
                );
                client
                    .query(
                        &format!(
                            "SELECT id, bucket_id, path, size, mime_type, metadata,
                                    owner, etag, created_at, updated_at
                             FROM {sch}.storage_objects
                             WHERE project_id = $1 AND bucket_id = $2
                               AND path LIKE $3 ESCAPE '\\'
                             ORDER BY path"
                        ),
                        &[&project.to_string(), &bucket_id.0, &pattern],
                    )
                    .await
            }
            None => {
                client
                    .query(
                        &format!(
                            "SELECT id, bucket_id, path, size, mime_type, metadata,
                                    owner, etag, created_at, updated_at
                             FROM {sch}.storage_objects
                             WHERE project_id = $1 AND bucket_id = $2
                             ORDER BY path"
                        ),
                        &[&project.to_string(), &bucket_id.0],
                    )
                    .await
            }
        }
        .map_err(|e| BlobError::Catalog(format!("list_objects_by_prefix: {e}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(Self::row_to_object(row)?);
        }
        Ok(out)
    }

    async fn delete_bucket_by_name(&self, project: &ProjectId, name: &str) -> Result<Vec<String>> {
        let sch = &self.schema;
        let project_str = project.to_string();
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BlobError::Catalog(format!("delete_bucket begin: {e}")))?;

        // Lock + read the bucket row so we know its id and can 404 cleanly.
        let bucket_row = tx
            .query_opt(
                &format!(
                    "SELECT id FROM {sch}.storage_buckets
                     WHERE project_id = $1 AND name = $2
                     FOR UPDATE"
                ),
                &[&project_str, &name],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("delete_bucket lock: {e}")))?;
        let Some(bucket_row) = bucket_row else {
            tx.rollback().await.ok();
            return Err(BlobError::BucketNotFound(name.to_string()));
        };
        let bucket_id: uuid::Uuid = bucket_row.get("id");

        // Delete the object rows, returning their paths so the caller can purge
        // the bytes from object_store.
        let object_rows = tx
            .query(
                &format!(
                    "DELETE FROM {sch}.storage_objects
                     WHERE project_id = $1 AND bucket_id = $2
                     RETURNING path"
                ),
                &[&project_str, &bucket_id],
            )
            .await
            .map_err(|e| BlobError::Catalog(format!("delete_bucket objects: {e}")))?;
        let mut paths: Vec<String> = object_rows.iter().map(|r| r.get("path")).collect();

        tx.execute(
            &format!(
                "DELETE FROM {sch}.storage_buckets
                 WHERE project_id = $1 AND name = $2"
            ),
            &[&project_str, &name],
        )
        .await
        .map_err(|e| BlobError::Catalog(format!("delete_bucket bucket: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| BlobError::Catalog(format!("delete_bucket commit: {e}")))?;

        paths.sort();
        Ok(paths)
    }
}

// ---------------------------------------------------------------------------
// Tests — env-gated on a live Postgres (BASIN_AUTH_TEST_POSTGRES_URL).
//
// Skip cleanly when the env var is absent so CI without a database stays
// green. Each test scopes itself to a unique schema and drops it on the way
// out so reruns don't accumulate state.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::store::BlobStore;
    use crate::Bucket;
    use bytes::Bytes;
    use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
    use std::sync::Arc;

    const URL_ENV: &str = "BASIN_AUTH_TEST_POSTGRES_URL";

    fn unique_schema() -> String {
        format!(
            "basin_blob_test_{}",
            uuid::Uuid::new_v4().simple().to_string()
        )
    }

    async fn drop_schema(url: &str, schema: &str) {
        let Ok((client, conn)) = tokio_postgres::connect(url, NoTls).await else {
            return;
        };
        let handle = tokio::spawn(async move {
            let _ = conn.await;
        });
        let _ = client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        drop(client);
        let _ = handle.await;
    }

    /// Restart survival: a fresh `PostgresBlobCatalog` over the same schema
    /// sees bucket + object metadata written by a prior instance.
    #[tokio::test]
    async fn metadata_survives_restart() {
        let Ok(url) = std::env::var(URL_ENV) else {
            eprintln!("{URL_ENV} unset; skipping metadata_survives_restart");
            return;
        };
        let schema = unique_schema();
        let project = ProjectId::new();

        // Instance A: write a bucket + object, then drop the handle (simulated
        // process shutdown).
        let bucket = {
            let cat_a = PostgresBlobCatalog::connect_with_schema(&url, &schema)
                .await
                .expect("connect A");
            let bucket = Bucket::new("avatars");
            cat_a
                .insert_bucket(&project, bucket.clone())
                .await
                .expect("insert_bucket");
            let obj = Object::new(
                bucket.id,
                "alice/photo.png",
                42,
                Some("image/png".to_string()),
                serde_json::json!({"k": "v"}),
                None,
                "deadbeef",
            );
            cat_a.upsert_object(&project, obj).await.expect("upsert");
            bucket
            // cat_a dropped here → connection pool torn down ("restart").
        };

        // Instance B: a brand-new catalog over the same Postgres schema.
        let cat_b = PostgresBlobCatalog::connect_with_schema(&url, &schema)
            .await
            .expect("connect B");

        let fetched_bucket = cat_b
            .get_bucket_by_name(&project, "avatars")
            .await
            .expect("get_bucket")
            .expect("bucket present after restart");
        assert_eq!(fetched_bucket.id, bucket.id);
        assert!(!fetched_bucket.public);

        let fetched_obj = cat_b
            .get_object_by_path(&project, bucket.id, "alice/photo.png")
            .await
            .expect("get_object")
            .expect("object present after restart");
        assert_eq!(fetched_obj.size, 42);
        assert_eq!(fetched_obj.etag, "deadbeef");
        assert_eq!(fetched_obj.mime_type.as_deref(), Some("image/png"));
        assert_eq!(fetched_obj.metadata, serde_json::json!({"k": "v"}));

        drop(cat_b);
        drop_schema(&url, &schema).await;
    }

    /// DELETE-bucket purge: bytes are removed from object_store and the bucket
    /// row + object rows disappear from the durable catalog.
    #[tokio::test]
    async fn delete_bucket_purges_objects() {
        let Ok(url) = std::env::var(URL_ENV) else {
            eprintln!("{URL_ENV} unset; skipping delete_bucket_purges_objects");
            return;
        };
        let schema = unique_schema();
        let project = ProjectId::new();

        let catalog: Arc<dyn BlobCatalog> = Arc::new(
            PostgresBlobCatalog::connect_with_schema(&url, &schema)
                .await
                .expect("connect"),
        );
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = BlobStore::new(Arc::new(catalog), obj_store.clone(), None);

        store
            .create_bucket(&project, Bucket::new("docs"))
            .await
            .expect("create_bucket");
        store
            .put_object(
                &project,
                "docs",
                "a/one.txt",
                Bytes::from_static(b"one"),
                Some("text/plain".to_string()),
                serde_json::Value::Null,
                None,
                "e1",
            )
            .await
            .expect("put one");
        store
            .put_object(
                &project,
                "docs",
                "a/two.txt",
                Bytes::from_static(b"two"),
                None,
                serde_json::Value::Null,
                None,
                "e2",
            )
            .await
            .expect("put two");

        // Bytes are present before delete.
        let key = store.object_key(&project, "docs", "a/one.txt").unwrap();
        assert!(
            obj_store.get(&key).await.is_ok(),
            "bytes present pre-delete"
        );

        // Delete the bucket → both objects purged.
        let purged = store
            .delete_bucket(&project, "docs")
            .await
            .expect("delete_bucket");
        assert_eq!(purged, 2, "both objects' bytes purged");

        // Bytes gone from object_store.
        assert!(
            matches!(
                obj_store.get(&key).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "bytes purged from object_store"
        );

        // Bucket row gone from the durable catalog.
        let after = store.get_bucket(&project, "docs").await;
        assert!(matches!(after, Err(BlobError::BucketNotFound(_))));

        drop(store);
        drop_schema(&url, &schema).await;
    }

    /// Duplicate bucket names in the same project surface as
    /// `BucketAlreadyExists`, matching the in-memory catalog's contract.
    #[tokio::test]
    async fn duplicate_bucket_errors() {
        let Ok(url) = std::env::var(URL_ENV) else {
            eprintln!("{URL_ENV} unset; skipping duplicate_bucket_errors");
            return;
        };
        let schema = unique_schema();
        let project = ProjectId::new();
        let cat = PostgresBlobCatalog::connect_with_schema(&url, &schema)
            .await
            .expect("connect");
        cat.insert_bucket(&project, Bucket::new("dup"))
            .await
            .expect("first insert");
        let err = cat
            .insert_bucket(&project, Bucket::new("dup"))
            .await
            .unwrap_err();
        assert!(matches!(err, BlobError::BucketAlreadyExists(_)));
        drop(cat);
        drop_schema(&url, &schema).await;
    }
}
