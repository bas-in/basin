//! `basin-analytical` — analytical query path.
//!
//! Phase 5 v0.1. A separate compute pool that reads the same Iceberg-style
//! Parquet files `basin-storage` writes, executed by a fresh DuckDB connection
//! per query. The OLTP path through `basin-shard` + `basin-engine` stays the
//! same; this crate is what the router will route at when a planned query
//! crosses the "this is analytical" heuristic threshold (see
//! `docs/architecture.md` section 7).
//!
//! ## Why DuckDB
//!
//! DuckDB is the C++ vectorised analytical engine designed for exactly this
//! workload. Pointing it at a list of Parquet files via `read_parquet([...])`
//! gives us late-materialised, columnar, cache-friendly aggregation that
//! comfortably beats DataFusion-on-the-OLTP-path on wide group-bys and full
//! scans without us writing a single line of execution code.
//!
//! ## Tenant isolation
//!
//! DuckDB has no model of tenants. Isolation is enforced *before* SQL ever
//! reaches DuckDB, by Basin:
//!
//! 1. `query` requires a [`TenantId`]. Every code path uses it.
//! 2. The file list registered as a DuckDB view comes from
//!    `basin_storage::Storage::list_data_files(tenant, table)`, which
//!    physically prefixes every key with `tenants/{tenant}/`.
//! 3. The submitted SQL is rejected if it contains the strings `read_parquet`,
//!    `read_csv`, `read_json`, or `attach`. Those are the only documented
//!    DuckDB constructs that can name a path the engine didn't choose. A
//!    string match is crude but it's defence-in-depth: even if a future
//!    extension adds another way in, the existing tenant-prefix invariant on
//!    the registered file paths still holds for the views we expose. See
//!    [`validate_sql`].
//!
//! ## v0.1 scope
//!
//! - Local filesystem only. The DuckDB scanner needs an absolute filesystem
//!   path; `object_store::LocalFileSystem` does not expose its root
//!   publicly, so we accept the root as an explicit `PathBuf` in
//!   [`AnalyticalConfig`]. v0.2 will wire DuckDB's `httpfs` extension and
//!   accept `s3://` URLs from a real `object_store::aws::AmazonS3` backend.
//! - One fresh DuckDB connection per query. v0.2 will cache per-tenant
//!   sessions to amortise startup. v0.1 measures correctly even without that.
//! - All current data files are registered. We do not yet honour Iceberg
//!   snapshot pinning (`AS OF SNAPSHOT n`); that's also v0.2.

#![forbid(unsafe_code)]

mod validate;

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use basin_common::{BasinError, Result, TableName, TenantId};

pub use validate::validate_sql;

/// Configuration for [`AnalyticalEngine`].
#[derive(Clone)]
pub struct AnalyticalConfig {
    pub storage: basin_storage::Storage,
    pub catalog: Arc<dyn basin_catalog::Catalog>,
    /// Absolute filesystem root for the underlying `LocalFileSystem`. Required
    /// for v0.1 because DuckDB's `read_parquet` needs absolute paths and
    /// `object_store::LocalFileSystem` keeps its root private. The
    /// `basin-server` binary that wires this up reads `BASIN_DATA_DIR`. When
    /// `None`, every call to [`AnalyticalEngine::query`] returns
    /// [`BasinError::Internal`] — the v0.1 implementation has no other
    /// supported backend.
    pub local_fs_root: Option<PathBuf>,
}

impl std::fmt::Debug for AnalyticalConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyticalConfig")
            .field("local_fs_root", &self.local_fs_root)
            .finish_non_exhaustive()
    }
}

/// Analytical query engine. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct AnalyticalEngine {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: AnalyticalConfig,
}

impl AnalyticalEngine {
    pub fn new(cfg: AnalyticalConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner { cfg }),
        })
    }

    /// Tables visible to `tenant`. Delegates to the catalog so the analytical
    /// path agrees with the OLTP path on table existence.
    #[tracing::instrument(skip(self), fields(tenant = %tenant))]
    pub async fn list_tables(&self, tenant: &TenantId) -> Result<Vec<TableName>> {
        self.inner.cfg.catalog.list_tables(tenant).await
    }

    /// Run an analytical SQL query for `tenant`. The flow is:
    ///
    /// 1. Reject obvious file-system escape patterns in the submitted SQL.
    /// 2. Resolve the tenant's tables via the catalog.
    /// 3. For each table, list its current Parquet files via
    ///    [`basin_storage::Storage::list_data_files`].
    /// 4. Open a fresh DuckDB connection. Register one VIEW per table backed
    ///    by `read_parquet([...])`.
    /// 5. Execute the SQL and collect Arrow `RecordBatch`es.
    ///
    /// All of step 4-5 runs inside `tokio::task::spawn_blocking` because the
    /// `duckdb` crate is synchronous.
    #[tracing::instrument(
        skip(self, sql),
        fields(tenant = %tenant, sql = %sql.lines().next().unwrap_or("")),
    )]
    pub async fn query(&self, tenant: &TenantId, sql: &str) -> Result<Vec<RecordBatch>> {
        validate::validate_sql(sql)?;

        let root = self.inner.cfg.local_fs_root.clone().ok_or_else(|| {
            BasinError::internal(
                "analytical path requires LocalFileSystem in v0.1 \
                 (set AnalyticalConfig::local_fs_root)",
            )
        })?;

        // Resolve tables visible to this tenant. We register every table the
        // tenant owns, not just the ones the SQL parser thinks are referenced
        // — that keeps us out of the SQL-parsing business entirely and makes
        // the isolation argument (above) crisper. If the tenant has many
        // tables, v0.2 can switch to lazy registration.
        let tables = self.inner.cfg.catalog.list_tables(tenant).await?;

        // Snapshot the file set for each table. DuckDB will see exactly these
        // files; concurrent writes on another shard owner won't be visible
        // until the next query (snapshot isolation, v0.1 flavour).
        let mut registered: Vec<(TableName, Vec<String>)> =
            Vec::with_capacity(tables.len());
        for t in tables {
            let files = self.inner.cfg.storage.list_data_files(tenant, &t).await?;
            let mut paths = Vec::with_capacity(files.len());
            for f in files {
                let abs = root.join(f.path.as_ref());
                let s = abs
                    .to_str()
                    .ok_or_else(|| {
                        BasinError::internal(format!(
                            "non-UTF-8 path under local FS root: {}",
                            abs.display()
                        ))
                    })?
                    .to_string();
                paths.push(s);
            }
            // Tenant-prefix safety net: the storage layer guarantees this, but
            // we re-assert here. A leak would be P0; cheap to check, infinite
            // upside if it ever catches anything.
            let prefix = format!("tenants/{tenant}/");
            for p in &paths {
                if !p.contains(&prefix) {
                    return Err(BasinError::isolation(format!(
                        "analytical path leaked: {p} missing prefix {prefix}"
                    )));
                }
            }
            registered.push((t, paths));
        }

        let sql = sql.to_owned();
        // DuckDB is sync; bridge via spawn_blocking so we don't stall the
        // async runtime on its work-stealing thread. The result type
        // (`Vec<RecordBatch>`) is `Send`, so this composes cleanly.
        let batches = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
            run_query_blocking(&sql, &registered)
        })
        .await
        .map_err(|e| BasinError::internal(format!("analytical join: {e}")))??;

        Ok(batches)
    }
}

/// Synchronous DuckDB driver. Lives outside the async surface because the
/// `duckdb` crate's connection and statement types are `!Send` (they hold
/// raw FFI handles); creating and destroying them all inside one
/// `spawn_blocking` keeps lifetimes inside that single thread.
fn run_query_blocking(
    sql: &str,
    tables: &[(TableName, Vec<String>)],
) -> Result<Vec<RecordBatch>> {
    use duckdb::Connection;

    let conn = Connection::open_in_memory()
        .map_err(|e| BasinError::internal(format!("duckdb open: {e}")))?;

    for (name, paths) in tables {
        let view_sql = build_view_sql(name, paths);
        conn.execute_batch(&view_sql)
            .map_err(|e| BasinError::internal(format!("duckdb register {name}: {e}")))?;
    }

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| BasinError::internal(format!("duckdb prepare: {e}")))?;
    let arrow = stmt
        .query_arrow([])
        .map_err(|e| BasinError::internal(format!("duckdb query: {e}")))?;
    Ok(arrow.collect())
}

/// Build a DuckDB DDL string that registers `name` as a VIEW backed by the
/// listed Parquet files. Identifiers and paths are escaped per DuckDB's
/// SQL grammar to defend against injection-via-table-name and
/// injection-via-path-with-quote.
fn build_view_sql(name: &TableName, paths: &[String]) -> String {
    // Empty file list: register an empty view by selecting from a typed
    // zero-row literal. DuckDB rejects `read_parquet([])`. Returning a view
    // with no rows is the right semantic so `SELECT count(*)` etc. still
    // type-checks.
    if paths.is_empty() {
        return format!(
            "CREATE OR REPLACE VIEW \"{escaped_name}\" AS SELECT * FROM (SELECT 1) WHERE 1=0",
            escaped_name = escape_ident(name.as_str())
        );
    }
    let mut s = String::with_capacity(64 + paths.len() * 96);
    s.push_str("CREATE OR REPLACE VIEW \"");
    s.push_str(&escape_ident(name.as_str()));
    s.push_str("\" AS SELECT * FROM read_parquet([");
    for (i, p) in paths.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push('\'');
        s.push_str(&escape_sql_str(p));
        s.push('\'');
    }
    // union_by_name=true so files written under slightly different schemas
    // (e.g. after an ADD COLUMN) still align column-wise.
    s.push_str("], union_by_name=true)");
    s
}

fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn escape_sql_str(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_catalog::{DataFileRef, InMemoryCatalog};
    use basin_common::{PartitionKey, TableName, TenantId};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn schema_with_category() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
        ]))
    }

    fn batch_with_category(start: i64, len: i64) -> RecordBatch {
        let ids: Int64Array = (start..start + len).collect();
        let cats: Vec<String> = (0..len)
            .map(|i| {
                // 4 categories, deterministic distribution.
                let v = (start + i) % 4;
                format!("cat-{v}")
            })
            .collect();
        let cats_arr: StringArray = cats.iter().map(|s| Some(s.as_str())).collect();
        RecordBatch::try_new(
            schema_with_category(),
            vec![Arc::new(ids), Arc::new(cats_arr)],
        )
        .unwrap()
    }

    /// Build an [`AnalyticalEngine`] backed by a fresh tempdir-rooted
    /// LocalFileSystem and an in-memory catalog. Returns the engine plus the
    /// shared `Storage` and `Catalog` so the caller can seed data.
    fn fixture() -> (
        TempDir,
        AnalyticalEngine,
        basin_storage::Storage,
        Arc<dyn basin_catalog::Catalog>,
    ) {
        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = AnalyticalEngine::new(AnalyticalConfig {
            storage: storage.clone(),
            catalog: catalog.clone(),
            local_fs_root: Some(dir.path().to_path_buf()),
        })
        .unwrap();
        (dir, engine, storage, catalog)
    }

    /// Write one batch and commit its `DataFile` to the catalog so a later
    /// `list_tables` returns the table.
    async fn seed_one_batch(
        storage: &basin_storage::Storage,
        catalog: &Arc<dyn basin_catalog::Catalog>,
        tenant: &TenantId,
        table: &TableName,
        batch: RecordBatch,
    ) {
        let part = PartitionKey::default_key();
        catalog
            .create_table(tenant, table, batch.schema().as_ref())
            .await
            .ok();
        let df = storage
            .write_batch(tenant, table, &part, &batch)
            .await
            .unwrap();
        let meta = catalog.load_table(tenant, table).await.unwrap();
        catalog
            .append_data_files(
                tenant,
                table,
                meta.current_snapshot,
                vec![DataFileRef {
                    path: df.path.as_ref().to_string(),
                    size_bytes: df.size_bytes,
                    row_count: df.row_count,
                }],
            )
            .await
            .unwrap();
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn query_simple_select() {
        let (_dir, engine, storage, catalog) = fixture();
        let tenant = TenantId::new();
        let table = TableName::new("t").unwrap();
        seed_one_batch(&storage, &catalog, &tenant, &table, batch_with_category(0, 100)).await;

        let batches = engine.query(&tenant, "SELECT * FROM t").await.unwrap();
        assert_eq!(total_rows(&batches), 100);
        assert!(!batches.is_empty());
        let cols: Vec<_> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"category".to_string()));
    }

    #[tokio::test]
    async fn query_aggregate() {
        let (_dir, engine, storage, catalog) = fixture();
        let tenant = TenantId::new();
        let table = TableName::new("t").unwrap();
        seed_one_batch(&storage, &catalog, &tenant, &table, batch_with_category(0, 100)).await;

        // DuckDB returns `sum(BIGINT)` as a `HUGEINT` (i128) by default. Cast
        // to BIGINT in SQL so the resulting Arrow type is the workspace-known
        // `Int64Array`, regardless of DuckDB's promotion rules.
        let batches = engine
            .query(
                &tenant,
                "SELECT count(*) AS n, CAST(sum(id) AS BIGINT) AS s FROM t",
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&batches), 1);
        let n = batches[0]
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 100);
        // 0 + 1 + ... + 99
        assert_eq!(s, (0..100).sum::<i64>());
    }

    #[tokio::test]
    async fn query_group_by() {
        let (_dir, engine, storage, catalog) = fixture();
        let tenant = TenantId::new();
        let table = TableName::new("t").unwrap();
        seed_one_batch(&storage, &catalog, &tenant, &table, batch_with_category(0, 100)).await;

        let batches = engine
            .query(
                &tenant,
                "SELECT category, count(*) AS n FROM t GROUP BY category ORDER BY category",
            )
            .await
            .unwrap();
        assert_eq!(total_rows(&batches), 4);
        // DuckDB may emit either `Utf8` or `Utf8View` for the category column
        // depending on its `produce_arrow_string_view` default; accept both.
        let cats = collect_string_col(&batches, "category");
        let ns = collect_int64_col(&batches, "n");
        for i in 0..4 {
            assert_eq!(cats[i], format!("cat-{i}"));
            // 100 rows / 4 categories = 25 each.
            assert_eq!(ns[i], 25);
        }
    }

    fn collect_string_col(batches: &[RecordBatch], name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for b in batches {
            let any = b.column_by_name(name).unwrap().as_any();
            if let Some(arr) = any.downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            } else if let Some(arr) = any.downcast_ref::<arrow_array::StringViewArray>() {
                for i in 0..arr.len() {
                    out.push(arr.value(i).to_string());
                }
            } else {
                panic!(
                    "column {name} has unexpected type {:?}",
                    b.column_by_name(name).unwrap().data_type()
                );
            }
        }
        out
    }

    fn collect_int64_col(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(arr.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn query_rejects_read_parquet_escape() {
        let (_dir, engine, _, _) = fixture();
        let tenant = TenantId::new();
        let err = engine
            .query(&tenant, "SELECT * FROM read_parquet('/etc/passwd')")
            .await
            .unwrap_err();
        assert!(
            matches!(err, BasinError::InvalidIdent(_)),
            "expected InvalidIdent, got {err:?}"
        );

        // Also blocked: read_csv, read_json, attach. Each must reject.
        for sql in [
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM read_json('/etc/passwd')",
            "ATTACH '/some/path' AS evil",
        ] {
            let err = engine.query(&tenant, sql).await.unwrap_err();
            assert!(
                matches!(err, BasinError::InvalidIdent(_)),
                "expected InvalidIdent for {sql:?}, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let (_dir, engine, storage, catalog) = fixture();
        let a = TenantId::new();
        let b = TenantId::new();
        let table = TableName::new("t").unwrap();
        // Same table name, distinct row sets per tenant. A returns ids 0..50;
        // B returns ids 1000..1100. If isolation breaks, the assertions below
        // would see ids from the other tenant's prefix.
        seed_one_batch(&storage, &catalog, &a, &table, batch_with_category(0, 50)).await;
        seed_one_batch(&storage, &catalog, &b, &table, batch_with_category(1000, 100)).await;

        let ra = engine.query(&a, "SELECT id FROM t ORDER BY id").await.unwrap();
        let rb = engine.query(&b, "SELECT id FROM t ORDER BY id").await.unwrap();

        let ids_a: Vec<i64> = ra
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();
        let ids_b: Vec<i64> = rb
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(ids_a.len(), 50);
        assert_eq!(ids_b.len(), 100);
        assert!(ids_a.iter().all(|&x| (0..50).contains(&x)));
        assert!(ids_b.iter().all(|&x| (1000..1100).contains(&x)));
    }

    #[tokio::test]
    async fn requires_local_fs_root() {
        // No local_fs_root => Internal error per the v0.1 contract.
        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = AnalyticalEngine::new(AnalyticalConfig {
            storage,
            catalog,
            local_fs_root: None,
        })
        .unwrap();
        let err = engine
            .query(&TenantId::new(), "SELECT 1")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Internal(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn list_tables_delegates_to_catalog() {
        let (_dir, engine, storage, catalog) = fixture();
        let tenant = TenantId::new();
        let t1 = TableName::new("alpha").unwrap();
        let t2 = TableName::new("beta").unwrap();
        seed_one_batch(&storage, &catalog, &tenant, &t1, batch_with_category(0, 1)).await;
        seed_one_batch(&storage, &catalog, &tenant, &t2, batch_with_category(0, 1)).await;
        let mut listed = engine.list_tables(&tenant).await.unwrap();
        listed.sort();
        assert_eq!(listed, vec![t1, t2]);
    }
}
