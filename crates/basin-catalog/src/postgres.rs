//! Postgres-backed durable [`Catalog`] implementation.
//!
//! Replaces the volatile [`crate::InMemoryCatalog`] for production: every
//! operation persists to a Postgres schema (default `basin_catalog`) so a
//! process restart does not lose tables, schemas, or snapshot history.
//!
//! Concurrency model mirrors the in-memory implementation: optimistic
//! concurrency on `expected_snapshot`, monotonic `SnapshotId` per
//! `(tenant, table)`. Atomicity for `append_data_files` is provided by a
//! single Postgres transaction with `SELECT ... FOR UPDATE` on the table
//! row, which serializes concurrent appenders on the same `(tenant, table)`
//! without blocking commits to other tables.
//!
//! TLS: `tokio_postgres::NoTls` is hard-coded for the PoC. Production
//! deployments need to swap to `tokio_postgres_rustls` or `tokio_postgres
//! _native_tls` at the connect site.

use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use basin_common::{BasinError, Result, TableName, TenantId};
use chrono::Utc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::instrument;

use crate::metadata::{DataFileRef, TableMetadata};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::Catalog;

const DEFAULT_SCHEMA: &str = "basin_catalog";

/// Postgres-backed implementation of [`Catalog`].
///
/// Cheap to wrap in [`std::sync::Arc`] and share across the engine, router,
/// and analytical pool. The underlying `tokio_postgres::Client` is itself a
/// thin handle around an `mpsc` to the connection driver task; concurrent
/// read-only queries are fine via `&Client`. Transactions need `&mut Client`
/// in `tokio_postgres` 0.7, so the client is wrapped in a `tokio::sync::
/// Mutex`. The mutex is only held for the duration of a single transaction
/// (begin → commit), which is the same scope as a per-table lock would have
/// covered anyway because `append_data_files` is the only multi-statement
/// path. For read-heavy workloads we could split into a pool later.
pub struct PostgresCatalog {
    client: Mutex<Client>,
    schema: String,
}

impl PostgresCatalog {
    /// Connect using the default schema (`basin_catalog`). Spawns the
    /// connection driver task and runs idempotent migrations before
    /// returning.
    pub async fn connect(conn_str: &str) -> Result<Self> {
        Self::connect_with_schema(conn_str, DEFAULT_SCHEMA).await
    }

    /// Connect into a caller-chosen schema. Used by the test suite to scope
    /// each test run to a unique schema so leftovers can't accumulate.
    pub async fn connect_with_schema(conn_str: &str, schema: &str) -> Result<Self> {
        validate_schema_ident(schema)?;
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .map_err(|e| BasinError::catalog(format!("postgres connect: {e}")))?;
        // Drop the driver task without join; if the connection dies, every
        // subsequent `client.query` returns an error which we map to
        // `BasinError::Catalog`.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres connection driver exited");
            }
        });
        let cat = Self {
            client: Mutex::new(client),
            schema: schema.to_owned(),
        };
        cat.migrate().await?;
        Ok(cat)
    }

    /// Run `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` for
    /// every catalog table. Safe to call repeatedly; the only changes the
    /// caller will observe are first-run table creation.
    pub async fn migrate(&self) -> Result<()> {
        // Schema name is validated at construction; safe to interpolate.
        let schema = &self.schema;
        let stmts = [
            format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.namespaces (
                    tenant_id  TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.tables (
                    tenant_id          TEXT NOT NULL REFERENCES {schema}.namespaces(tenant_id),
                    table_name         TEXT NOT NULL,
                    schema_json        JSONB NOT NULL,
                    current_snapshot   BIGINT NOT NULL DEFAULT 0,
                    format_version     SMALLINT NOT NULL DEFAULT 2,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, table_name)
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.snapshots (
                    tenant_id     TEXT NOT NULL,
                    table_name    TEXT NOT NULL,
                    snapshot_id   BIGINT NOT NULL,
                    parent_id     BIGINT,
                    operation     TEXT NOT NULL,
                    committed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    summary_json  JSONB NOT NULL,
                    data_files    JSONB NOT NULL,
                    PRIMARY KEY (tenant_id, table_name, snapshot_id),
                    FOREIGN KEY (tenant_id, table_name)
                        REFERENCES {schema}.tables(tenant_id, table_name)
                        ON DELETE CASCADE
                )"
            ),
        ];
        let client = self.client.lock().await;
        for stmt in stmts {
            client
                .batch_execute(&stmt)
                .await
                .map_err(|e| BasinError::catalog(format!("migrate: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn create_namespace(&self, tenant: &TenantId) -> Result<()> {
        let sql = format!(
            "INSERT INTO {schema}.namespaces (tenant_id) VALUES ($1)
             ON CONFLICT (tenant_id) DO NOTHING",
            schema = self.schema
        );
        let client = self.client.lock().await;
        client
            .execute(&sql, &[&tenant.to_string()])
            .await
            .map_err(|e| BasinError::catalog(format!("create_namespace: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self, schema), fields(tenant = %tenant, table = %table))]
    async fn create_table(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: &Schema,
    ) -> Result<TableMetadata> {
        let schema_json = serde_json::to_value(schema)
            .map_err(|e| BasinError::catalog(format!("serialise arrow schema: {e}")))?;
        let now = Utc::now();
        let genesis_summary = SnapshotSummary {
            operation: SnapshotOperation::Genesis,
            added_files: 0,
            added_rows: 0,
            added_bytes: 0,
        };
        let genesis_summary_json = serde_json::to_value(&genesis_summary)
            .map_err(|e| BasinError::catalog(format!("serialise genesis summary: {e}")))?;
        let empty_files: Vec<DataFileRef> = Vec::new();
        let empty_files_json = serde_json::to_value(&empty_files)
            .map_err(|e| BasinError::catalog(format!("serialise data files: {e}")))?;

        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();

        // Ensure namespace exists, then attempt table insert. ON CONFLICT
        // turns "already exists" into a CommitConflict-shaped catalog error;
        // mirrors `InMemoryCatalog::create_table`.
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("begin: {e}")))?;
        tx.execute(
            &format!(
                "INSERT INTO {sch}.namespaces (tenant_id) VALUES ($1)
                 ON CONFLICT (tenant_id) DO NOTHING"
            ),
            &[&tenant_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("ensure namespace: {e}")))?;

        let inserted = tx
            .execute(
                &format!(
                    "INSERT INTO {sch}.tables (tenant_id, table_name, schema_json, current_snapshot, format_version)
                     VALUES ($1, $2, $3, 0, 2)
                     ON CONFLICT (tenant_id, table_name) DO NOTHING"
                ),
                &[&tenant_str, &table_str, &schema_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert table: {e}")))?;
        if inserted == 0 {
            tx.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "table {tenant}/{table} already exists"
            )));
        }

        tx.execute(
            &format!(
                "INSERT INTO {sch}.snapshots
                   (tenant_id, table_name, snapshot_id, parent_id, operation, committed_at, summary_json, data_files)
                 VALUES ($1, $2, 0, NULL, 'genesis', $3, $4, $5)"
            ),
            &[
                &tenant_str,
                &table_str,
                &now,
                &genesis_summary_json,
                &empty_files_json,
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("insert genesis snapshot: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("commit create_table: {e}")))?;

        Ok(TableMetadata {
            tenant: *tenant,
            table: table.clone(),
            schema: Arc::new(schema.clone()),
            current_snapshot: SnapshotId::GENESIS,
            snapshots: vec![Snapshot {
                id: SnapshotId::GENESIS,
                parent: None,
                committed_at: now,
                data_files: empty_files,
                summary: genesis_summary,
            }],
            format_version: 2,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn load_table(&self, tenant: &TenantId, table: &TableName) -> Result<TableMetadata> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();

        let client = self.client.lock().await;
        let row_opt = client
            .query_opt(
                &format!(
                    "SELECT schema_json, current_snapshot, format_version
                     FROM {sch}.tables
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant_str, &table_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("load_table: {e}")))?;
        let Some(row) = row_opt else {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        };
        let schema_json: serde_json::Value = row.get(0);
        let current: i64 = row.get(1);
        let format_version: i16 = row.get(2);
        let arrow_schema: Schema = serde_json::from_value(schema_json)
            .map_err(|e| BasinError::catalog(format!("deserialise arrow schema: {e}")))?;

        let snapshots = fetch_snapshots(&client, sch, &tenant_str, &table_str).await?;
        Ok(TableMetadata {
            tenant: *tenant,
            table: table.clone(),
            schema: Arc::new(arrow_schema),
            current_snapshot: SnapshotId(current as u64),
            snapshots,
            format_version: format_version as u8,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn drop_table(&self, tenant: &TenantId, table: &TableName) -> Result<()> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!("DELETE FROM {sch}.tables WHERE tenant_id = $1 AND table_name = $2"),
                &[&tenant.to_string(), &table.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_table: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_tables(&self, tenant: &TenantId) -> Result<Vec<TableName>> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT table_name FROM {sch}.tables WHERE tenant_id = $1 ORDER BY table_name"
                ),
                &[&tenant.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_tables: {e}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get(0);
            let parsed = TableName::new(name)
                .map_err(|e| BasinError::catalog(format!("list_tables: bad ident: {e}")))?;
            out.push(parsed);
        }
        Ok(out)
    }

    #[instrument(
        skip(self, files),
        fields(
            tenant = %tenant,
            table = %table,
            expected_snapshot = %expected_snapshot,
            file_count = files.len(),
        ),
    )]
    async fn append_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();

        let added_files = files.len() as u64;
        let added_rows: u64 = files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
        let summary = SnapshotSummary {
            operation: SnapshotOperation::Append,
            added_files,
            added_rows,
            added_bytes,
        };
        let summary_json = serde_json::to_value(&summary)
            .map_err(|e| BasinError::catalog(format!("serialise summary: {e}")))?;
        let files_json = serde_json::to_value(&files)
            .map_err(|e| BasinError::catalog(format!("serialise data files: {e}")))?;

        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("begin append: {e}")))?;

        // FOR UPDATE serializes appenders on this (tenant, table) without
        // blocking other tables. The row's `current_snapshot` is the
        // optimistic-concurrency token: caller must have observed the same
        // value as is currently in the database.
        let row = tx
            .query_opt(
                &format!(
                    "SELECT current_snapshot FROM {sch}.tables
                     WHERE tenant_id = $1 AND table_name = $2
                     FOR UPDATE"
                ),
                &[&tenant_str, &table_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("lock row: {e}")))?;
        let Some(row) = row else {
            tx.rollback().await.ok();
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        };
        let current: i64 = row.get(0);
        if (current as u64) != expected_snapshot.0 {
            tx.rollback().await.ok();
            return Err(BasinError::CommitConflict(format!(
                "{tenant}/{table}: expected snapshot {expected_snapshot}, current is {current}"
            )));
        }

        let new_id = expected_snapshot.next();
        let parent_id_pg: i64 = expected_snapshot.0 as i64;
        let new_id_pg: i64 = new_id.0 as i64;
        let now = Utc::now();
        tx.execute(
            &format!(
                "INSERT INTO {sch}.snapshots
                   (tenant_id, table_name, snapshot_id, parent_id, operation, committed_at, summary_json, data_files)
                 VALUES ($1, $2, $3, $4, 'append', $5, $6, $7)"
            ),
            &[
                &tenant_str,
                &table_str,
                &new_id_pg,
                &parent_id_pg,
                &now,
                &summary_json,
                &files_json,
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("insert append snapshot: {e}")))?;

        tx.execute(
            &format!(
                "UPDATE {sch}.tables SET current_snapshot = $3
                 WHERE tenant_id = $1 AND table_name = $2"
            ),
            &[&tenant_str, &table_str, &new_id_pg],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("update current_snapshot: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("commit append: {e}")))?;
        drop(client);

        // Re-read full metadata so the caller sees exactly what's persisted.
        // Cheaper than reconstructing in-memory because schema_json
        // round-trip is the only network hop avoided, and correctness is
        // worth more than that microsecond.
        self.load_table(tenant, table).await
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn list_snapshots(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<Snapshot>> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();
        let client = self.client.lock().await;
        // Existence check so callers get NotFound (matching InMemoryCatalog)
        // rather than an empty list when the table is missing.
        let exists = client
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.tables WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant_str, &table_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_snapshots: {e}")))?;
        if exists.is_none() {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        fetch_snapshots(&client, sch, &tenant_str, &table_str).await
    }
}

async fn fetch_snapshots(
    client: &Client,
    schema: &str,
    tenant_str: &str,
    table_str: &str,
) -> Result<Vec<Snapshot>> {
    let rows = client
        .query(
            &format!(
                "SELECT snapshot_id, parent_id, committed_at, summary_json, data_files
                 FROM {schema}.snapshots
                 WHERE tenant_id = $1 AND table_name = $2
                 ORDER BY snapshot_id ASC"
            ),
            &[&tenant_str, &table_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("fetch snapshots: {e}")))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get(0);
        let parent: Option<i64> = row.get(1);
        let committed_at: chrono::DateTime<Utc> = row.get(2);
        let summary_json: serde_json::Value = row.get(3);
        let files_json: serde_json::Value = row.get(4);
        let summary: SnapshotSummary = serde_json::from_value(summary_json)
            .map_err(|e| BasinError::catalog(format!("deserialise summary: {e}")))?;
        let data_files: Vec<DataFileRef> = serde_json::from_value(files_json)
            .map_err(|e| BasinError::catalog(format!("deserialise data files: {e}")))?;
        out.push(Snapshot {
            id: SnapshotId(id as u64),
            parent: parent.map(|p| SnapshotId(p as u64)),
            committed_at,
            data_files,
            summary,
        });
    }
    Ok(out)
}

fn validate_schema_ident(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::catalog("schema name is empty"));
    }
    if s.len() > 63 {
        return Err(BasinError::catalog("schema name longer than 63 chars"));
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(BasinError::catalog(format!(
            "schema name must start with [A-Za-z_]: {s:?}"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(BasinError::catalog(format!(
                "schema name has invalid char {c:?}: {s:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Tests run against the live Postgres at 127.0.0.1:5432 (peer auth as
    //! `pc`). Each test allocates a unique schema name so concurrent tests
    //! cannot collide and a panic mid-test only leaks one schema. The
    //! `SchemaGuard` `Drop` runs `DROP SCHEMA ... CASCADE` on a fresh client
    //! to clean up.
    //!
    //! If Postgres is not reachable on 5432, every test prints a skip message
    //! and returns Ok — the durable-catalog suite must remain runnable in
    //! environments without local Postgres (CI without a sidecar, hermetic
    //! sandboxes, etc.).

    use std::sync::Arc;
    use std::time::Duration;

    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{BasinError, TableName, TenantId};
    use tokio_postgres::NoTls;
    use ulid::Ulid;

    use super::*;

    const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

    /// Per-test schema name. Postgres identifiers cap at 63; ULID strings are
    /// 26 chars, so the prefix + ULID fits comfortably and is mixed-case-
    /// insensitive enough to be safe.
    fn unique_schema() -> String {
        format!("basin_catalog_test_{}", Ulid::new().to_string().to_lowercase())
    }

    /// Drops the schema (and every table inside) in `Drop`. Uses a fresh
    /// connection so cleanup runs even if the catalog under test has been
    /// moved or dropped.
    struct SchemaGuard {
        schema: String,
    }

    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            let schema = self.schema.clone();
            // Cleanup needs a fresh connection on a fresh runtime because
            // Drop runs from sync context (and the test's runtime may be
            // shutting down). A spawned thread isolates us from any panic-
            // mid-drop fallout.
            let _ = std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("schema cleanup runtime: {e}");
                        return;
                    }
                };
                rt.block_on(async {
                    let connect = tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio_postgres::connect(PG_URL, NoTls),
                    )
                    .await;
                    let (client, conn) = match connect {
                        Ok(Ok(pair)) => pair,
                        _ => return,
                    };
                    let driver = tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    let _ = client
                        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                        .await;
                    drop(client);
                    let _ = tokio::time::timeout(Duration::from_millis(200), driver).await;
                });
            })
            .join();
        }
    }

    /// Connect or skip. Returns `None` if Postgres is unreachable so each
    /// test can `eprintln!` and exit cleanly.
    async fn try_connect() -> Option<(PostgresCatalog, SchemaGuard)> {
        let schema = unique_schema();
        match tokio::time::timeout(
            Duration::from_secs(2),
            PostgresCatalog::connect_with_schema(PG_URL, &schema),
        )
        .await
        {
            Ok(Ok(cat)) => Some((cat, SchemaGuard { schema })),
            Ok(Err(e)) => {
                eprintln!("postgres unreachable, skipping test: {e}");
                None
            }
            Err(_) => {
                eprintln!("postgres connect timed out, skipping test");
                None
            }
        }
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn file(path: &str, rows: u64, bytes: u64) -> DataFileRef {
        DataFileRef {
            path: path.into(),
            size_bytes: bytes,
            row_count: rows,
        }
    }

    #[tokio::test]
    async fn create_load_drop_table() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("users").unwrap();

        cat.create_namespace(&t).await.unwrap();
        let meta = cat.create_table(&t, &tbl, &schema()).await.unwrap();
        assert_eq!(meta.format_version, 2);
        assert_eq!(meta.current_snapshot, SnapshotId::GENESIS);
        assert_eq!(meta.snapshots.len(), 1);
        assert_eq!(
            meta.snapshots[0].summary.operation,
            SnapshotOperation::Genesis
        );

        let loaded = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(loaded.tenant, t);
        assert_eq!(loaded.table, tbl);
        assert_eq!(loaded.current_snapshot, SnapshotId::GENESIS);

        cat.drop_table(&t, &tbl).await.unwrap();
    }

    #[tokio::test]
    async fn drop_then_load_returns_not_found() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("ghost").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.drop_table(&t, &tbl).await.unwrap();
        let err = cat.load_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");

        let err = cat.drop_table(&t, &tbl).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        let tbl = TableName::new("orders").unwrap();

        let meta_a = cat.create_table(&a, &tbl, &schema()).await.unwrap();
        let meta_b = cat.create_table(&b, &tbl, &schema()).await.unwrap();
        assert_eq!(meta_a.tenant, a);
        assert_eq!(meta_b.tenant, b);
        assert_ne!(meta_a.tenant, meta_b.tenant);

        cat.append_data_files(
            &a,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a/data/0.parquet", 10, 100)],
        )
        .await
        .unwrap();
        let only_b = cat.load_table(&b, &tbl).await.unwrap();
        assert_eq!(only_b.current_snapshot, SnapshotId::GENESIS);

        let list_a = cat.list_tables(&a).await.unwrap();
        let list_b = cat.list_tables(&b).await.unwrap();
        assert_eq!(list_a, vec![tbl.clone()]);
        assert_eq!(list_b, vec![tbl.clone()]);

        cat.drop_table(&a, &tbl).await.unwrap();
        cat.load_table(&b, &tbl).await.unwrap();
    }

    #[tokio::test]
    async fn append_advances_snapshot() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("events").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        let meta = cat
            .append_data_files(
                &t,
                &tbl,
                SnapshotId::GENESIS,
                vec![file("p/0.parquet", 42, 1024)],
            )
            .await
            .unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(1));
        assert_eq!(meta.snapshots.len(), 2);
        let head = meta.current().unwrap();
        assert_eq!(head.summary.operation, SnapshotOperation::Append);
        assert_eq!(head.summary.added_rows, 42);
        assert_eq!(head.summary.added_bytes, 1024);
        assert_eq!(head.parent, Some(SnapshotId::GENESIS));

        let snaps = cat.list_snapshots(&t, &tbl).await.unwrap();
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps.first().unwrap().id, SnapshotId::GENESIS);
        assert_eq!(snaps.last().unwrap().id, SnapshotId(1));
    }

    #[tokio::test]
    async fn concurrent_append_one_wins() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let cat = Arc::new(cat);
        let t = TenantId::new();
        let tbl = TableName::new("race").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        let c1 = cat.clone();
        let c2 = cat.clone();
        let t1 = t;
        let t2 = t;
        let tbl1 = tbl.clone();
        let tbl2 = tbl.clone();
        let h1 = tokio::spawn(async move {
            c1.append_data_files(
                &t1,
                &tbl1,
                SnapshotId::GENESIS,
                vec![file("a.parquet", 1, 10)],
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            c2.append_data_files(
                &t2,
                &tbl2,
                SnapshotId::GENESIS,
                vec![file("b.parquet", 1, 10)],
            )
            .await
        });
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();

        let conflicts = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(BasinError::CommitConflict(_))))
            .count();
        let oks = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        assert_eq!(oks, 1, "exactly one append must win: {r1:?} {r2:?}");
        assert_eq!(conflicts, 1, "exactly one append must conflict: {r1:?} {r2:?}");

        let head = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(head.current_snapshot, SnapshotId(1));
    }

    #[tokio::test]
    async fn optimistic_retry() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("retry").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();

        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("a.parquet", 1, 10)],
        )
        .await
        .unwrap();

        let err = cat
            .append_data_files(
                &t,
                &tbl,
                SnapshotId::GENESIS,
                vec![file("b.parquet", 1, 10)],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::CommitConflict(_)));

        let fresh = cat.load_table(&t, &tbl).await.unwrap();
        let meta = cat
            .append_data_files(
                &t,
                &tbl,
                fresh.current_snapshot,
                vec![file("b.parquet", 1, 10)],
            )
            .await
            .unwrap();
        assert_eq!(meta.current_snapshot, SnapshotId(2));
        assert_eq!(meta.snapshots.len(), 3);
    }

    #[tokio::test]
    async fn survives_simulated_restart() {
        // Distinguishing test: durability across catalog instances. Create a
        // table, commit a snapshot, drop the catalog (= simulate process
        // exit), open a fresh `PostgresCatalog` against the same schema, and
        // assert the data is still there.
        let schema_name = unique_schema();
        let _guard = SchemaGuard {
            schema: schema_name.clone(),
        };
        let cat1 = match tokio::time::timeout(
            Duration::from_secs(2),
            PostgresCatalog::connect_with_schema(PG_URL, &schema_name),
        )
        .await
        {
            Ok(Ok(c)) => c,
            _ => {
                eprintln!("postgres unreachable, skipping survives_simulated_restart");
                return;
            }
        };
        let t = TenantId::new();
        let tbl = TableName::new("durable").unwrap();
        cat1.create_table(&t, &tbl, &schema()).await.unwrap();
        cat1.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("post-restart.parquet", 7, 256)],
        )
        .await
        .unwrap();
        // Drop = simulate restart. The driver task lives until the connection
        // closes, but the in-memory `PostgresCatalog` value is gone.
        drop(cat1);

        let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema_name)
            .await
            .expect("reconnect");
        let loaded = cat2.load_table(&t, &tbl).await.unwrap();
        assert_eq!(loaded.current_snapshot, SnapshotId(1));
        assert_eq!(loaded.snapshots.len(), 2);
        let head = loaded.current().unwrap();
        assert_eq!(head.summary.added_rows, 7);
        assert_eq!(head.summary.added_bytes, 256);
        assert_eq!(head.data_files.len(), 1);
        assert_eq!(head.data_files[0].path, "post-restart.parquet");
        let list = cat2.list_tables(&t).await.unwrap();
        assert_eq!(list, vec![tbl]);
    }

    #[test]
    fn schema_ident_validation() {
        validate_schema_ident("basin_catalog").unwrap();
        validate_schema_ident("_x").unwrap();
        assert!(validate_schema_ident("").is_err());
        assert!(validate_schema_ident("1bad").is_err());
        assert!(validate_schema_ident("with-dash").is_err());
        assert!(validate_schema_ident("with space").is_err());
    }
}
