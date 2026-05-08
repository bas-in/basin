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

use crate::metadata::{CvDef, DataFileRef, PartitionSpec, Policy, TableMetadata};
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
            // Phase 5.5 forward-compat: add the partition spec column on
            // existing deployments. New deployments get it via the table-
            // creation script above; this `ADD COLUMN IF NOT EXISTS` is a
            // no-op there. Stored as JSONB so future spec variants don't
            // need another migration.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS partition_spec_json JSONB"
            ),
            // Phase 5.6 forward-compat: row-level-security state. `rls_enabled`
            // gates the engine's predicate-injection path; `policies_json`
            // holds the (possibly empty) `Vec<Policy>` for that table. Both
            // columns default to "no RLS" so old rows are equivalent to a
            // freshly created table without any policy commands run.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS rls_enabled BOOLEAN NOT NULL DEFAULT FALSE"
            ),
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS policies_json JSONB"
            ),
            // Phase 5.5: tiered-storage age policy. `cold_after_seconds` NULL
            // means the policy is disabled (the default for back-compat);
            // `cold_age_column` NULL means fall back to the partition column
            // at sweep time. Both are additive — pre-tiering rows come back
            // as `(None, None)` which preserves the prior behaviour.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS cold_after_seconds BIGINT"
            ),
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS cold_age_column TEXT"
            ),
            // Phase 5.7 forward-compat: per-table bloom-filter column set.
            // Stored as a JSONB array of column names; NULL / absent means
            // "no bloom filters" — the default for back-compat. The writer
            // reads this on every put to decide which columns get a native
            // Parquet bloom filter section.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS bloom_filter_columns_json JSONB"
            ),
            // Phase 5.7 B3 forward-compat: per-table override of the
            // Parquet writer's `max_row_group_size`. NULL means "use the
            // writer default" (currently 65k rows) — the back-compat path.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS row_group_rows BIGINT"
            ),
            // Continuous-aggregate definition (basin-cv). NULL means "this
            // is a regular table" — the default for back-compat. Stored as
            // JSONB so the v0.2 incremental-refresh extensions to `CvDef`
            // (watermark column, refresh-policy variants) don't need
            // another migration.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS continuous_aggregate_json JSONB"
            ),
            // Phase 5.7 B2 forward-compat: cluster-column list for
            // physical sort-on-write. JSONB array of column names; NULL /
            // absent means "no cluster columns" — the default for
            // back-compat. The writer consults this on every put.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS cluster_columns_json JSONB"
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
            removed_files: 0,
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

        let default_spec_json = serde_json::to_value(PartitionSpec::Unpartitioned)
            .map_err(|e| BasinError::catalog(format!("serialise partition spec: {e}")))?;
        let empty_policies_json = serde_json::to_value::<Vec<Policy>>(Vec::new())
            .map_err(|e| BasinError::catalog(format!("serialise policies: {e}")))?;
        let inserted = tx
            .execute(
                &format!(
                    "INSERT INTO {sch}.tables (tenant_id, table_name, schema_json, current_snapshot, format_version, partition_spec_json, rls_enabled, policies_json)
                     VALUES ($1, $2, $3, 0, 2, $4, FALSE, $5)
                     ON CONFLICT (tenant_id, table_name) DO NOTHING"
                ),
                &[&tenant_str, &table_str, &schema_json, &default_spec_json, &empty_policies_json],
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
            partition_spec: PartitionSpec::Unpartitioned,
            rls_enabled: false,
            policies: Vec::new(),
            cold_after_seconds: None,
            cold_age_column: None,
            bloom_filter_columns: Vec::new(),
            row_group_rows: None,
            continuous_aggregate: None,
            cluster_columns: Vec::new(),
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
                    "SELECT schema_json, current_snapshot, format_version, partition_spec_json,
                            rls_enabled, policies_json, cold_after_seconds, cold_age_column,
                            bloom_filter_columns_json, row_group_rows,
                            continuous_aggregate_json, cluster_columns_json
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
        let partition_spec_json: Option<serde_json::Value> = row.get(3);
        let rls_enabled: bool = row.get(4);
        let policies_json: Option<serde_json::Value> = row.get(5);
        let cold_after_seconds_pg: Option<i64> = row.get(6);
        let cold_age_column: Option<String> = row.get(7);
        let bloom_filter_columns_json: Option<serde_json::Value> = row.get(8);
        let row_group_rows_pg: Option<i64> = row.get(9);
        let continuous_aggregate_json: Option<serde_json::Value> = row.get(10);
        let cluster_columns_json: Option<serde_json::Value> = row.get(11);
        let arrow_schema: Schema = serde_json::from_value(schema_json)
            .map_err(|e| BasinError::catalog(format!("deserialise arrow schema: {e}")))?;
        let partition_spec = match partition_spec_json {
            Some(v) => serde_json::from_value(v).map_err(|e| {
                BasinError::catalog(format!("deserialise partition spec: {e}"))
            })?,
            None => PartitionSpec::Unpartitioned,
        };
        let policies: Vec<Policy> = match policies_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise policies: {e}")))?,
            None => Vec::new(),
        };
        // Postgres BIGINT is i64; clamp negatives to None defensively (a
        // negative threshold has no meaning and shouldn't propagate).
        let cold_after_seconds = cold_after_seconds_pg.and_then(|v| u64::try_from(v).ok());
        let bloom_filter_columns: Vec<String> = match bloom_filter_columns_json {
            Some(v) => serde_json::from_value(v).map_err(|e| {
                BasinError::catalog(format!("deserialise bloom_filter_columns: {e}"))
            })?,
            None => Vec::new(),
        };
        // Postgres BIGINT is i64; clamp negatives / wider-than-usize defensively.
        // A negative row-group size has no meaning and shouldn't propagate; an
        // overflow on 32-bit hosts likewise falls back to "use the default".
        let row_group_rows: Option<usize> = row_group_rows_pg
            .and_then(|v| if v >= 0 { usize::try_from(v).ok() } else { None });
        let continuous_aggregate: Option<CvDef> = match continuous_aggregate_json {
            Some(v) => Some(serde_json::from_value(v).map_err(|e| {
                BasinError::catalog(format!("deserialise continuous_aggregate: {e}"))
            })?),
            None => None,
        };
        let cluster_columns: Vec<String> = match cluster_columns_json {
            Some(v) => serde_json::from_value(v).map_err(|e| {
                BasinError::catalog(format!("deserialise cluster_columns: {e}"))
            })?,
            None => Vec::new(),
        };

        let snapshots = fetch_snapshots(&client, sch, &tenant_str, &table_str).await?;
        Ok(TableMetadata {
            tenant: *tenant,
            table: table.clone(),
            schema: Arc::new(arrow_schema),
            current_snapshot: SnapshotId(current as u64),
            snapshots,
            format_version: format_version as u8,
            partition_spec,
            rls_enabled,
            policies,
            cold_after_seconds,
            cold_age_column,
            bloom_filter_columns,
            row_group_rows,
            continuous_aggregate,
            cluster_columns,
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

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn drop_namespace(&self, tenant: &TenantId) -> Result<()> {
        // Single transaction: delete all tables (cascades to snapshots via
        // the FK) and the namespace row. One round-trip vs N from the
        // default impl. Idempotent: missing rows are not an error.
        let sch = &self.schema;
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("begin drop_namespace: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.tables WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace tables: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.namespaces WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace namespace: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("commit drop_namespace: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_tenant_data_files(&self, tenant: &TenantId) -> Result<Vec<DataFileRef>> {
        // Single SELECT that union-flattens every table's snapshot.data_files
        // for this tenant. Each row is one snapshot's JSONB array of file
        // refs; we deserialise per-row and concatenate. One round-trip vs
        // the default impl's (list_tables, then N × load_table → fetch_snapshots).
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT data_files FROM {sch}.snapshots WHERE tenant_id = $1"
                ),
                &[&tenant.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_tenant_data_files: {e}")))?;
        let mut out: Vec<DataFileRef> = Vec::new();
        for row in rows {
            let files_json: serde_json::Value = row.get(0);
            let files: Vec<DataFileRef> = serde_json::from_value(files_json).map_err(|e| {
                BasinError::catalog(format!("deserialise data files: {e}"))
            })?;
            out.extend(files);
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
            removed_files: 0,
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

    #[instrument(
        skip(self, removed_paths, added_files),
        fields(
            tenant = %tenant,
            table = %table,
            expected_snapshot = %expected_snapshot,
            removed = removed_paths.len(),
            added = added_files.len(),
        ),
    )]
    async fn replace_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();

        let added_files_count = added_files.len() as u64;
        let added_rows: u64 = added_files.iter().map(|f| f.row_count).sum();
        let added_bytes: u64 = added_files.iter().map(|f| f.size_bytes).sum();
        let removed_files_count = removed_paths.len() as u64;
        // Same delta-only convention as `append_data_files`: the snapshot row
        // records the new files written by this commit. The engine handles
        // physical removal of the old files from object storage.
        let summary = SnapshotSummary {
            operation: SnapshotOperation::Replace,
            added_files: added_files_count,
            added_rows,
            added_bytes,
            removed_files: removed_files_count,
        };
        let summary_json = serde_json::to_value(&summary)
            .map_err(|e| BasinError::catalog(format!("serialise summary: {e}")))?;
        let files_json = serde_json::to_value(&added_files)
            .map_err(|e| BasinError::catalog(format!("serialise data files: {e}")))?;
        // Suppress unused-warning. Path list is acted on by the engine; we
        // record only the count in the snapshot summary.
        let _ = removed_paths;

        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("begin replace: {e}")))?;

        // Same FOR UPDATE row-lock pattern as append: serialises commits on
        // the same (tenant, table) without blocking other tables.
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
                 VALUES ($1, $2, $3, $4, 'replace', $5, $6, $7)"
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
        .map_err(|e| BasinError::catalog(format!("insert replace snapshot: {e}")))?;

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
            .map_err(|e| BasinError::catalog(format!("commit replace: {e}")))?;
        drop(client);

        self.load_table(tenant, table).await
    }

    #[instrument(skip(self, spec), fields(tenant = %tenant, table = %table))]
    async fn set_partition_spec(
        &self,
        tenant: &TenantId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        let sch = &self.schema;
        let json = serde_json::to_value(&spec)
            .map_err(|e| BasinError::catalog(format!("serialise partition spec: {e}")))?;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET partition_spec_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_partition_spec: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, policies), fields(tenant = %tenant, table = %table))]
    async fn set_rls_state(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let sch = &self.schema;
        let json = serde_json::to_value(&policies)
            .map_err(|e| BasinError::catalog(format!("serialise policies: {e}")))?;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET rls_enabled = $3, policies_json = $4
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &rls_enabled, &json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_rls_state: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_tier_policy(
        &self,
        tenant: &TenantId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let sch = &self.schema;
        // Stored as BIGINT (i64); reject thresholds that don't fit before
        // we even hit the database to keep the error message close to the
        // caller. u64::MAX / 2 ≈ 1.4e10 years is plenty of headroom.
        let cas_pg: Option<i64> = match cold_after_seconds {
            Some(v) => Some(i64::try_from(v).map_err(|_| {
                BasinError::catalog(format!("cold_after_seconds {v} overflows BIGINT"))
            })?),
            None => None,
        };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET cold_after_seconds = $3, cold_age_column = $4
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[
                    &tenant.to_string(),
                    &table.to_string(),
                    &cas_pg,
                    &cold_age_column,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_tier_policy: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, columns), fields(tenant = %tenant, table = %table, n = columns.len()))]
    async fn set_bloom_filter_columns(
        &self,
        tenant: &TenantId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let sch = &self.schema;
        let json = serde_json::to_value(&columns).map_err(|e| {
            BasinError::catalog(format!("serialise bloom_filter_columns: {e}"))
        })?;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET bloom_filter_columns_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_bloom_filter_columns: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_row_group_rows(
        &self,
        tenant: &TenantId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let sch = &self.schema;
        // Stored as BIGINT (i64); reject sizes that don't fit before we even
        // hit the database. A `usize::MAX` row-group size is nonsensical
        // anyway; the practical range is single-digit thousands to ~1M.
        let rows_pg: Option<i64> = match rows {
            Some(v) => Some(i64::try_from(v).map_err(|_| {
                BasinError::catalog(format!("row_group_rows {v} overflows BIGINT"))
            })?),
            None => None,
        };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET row_group_rows = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &rows_pg],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_row_group_rows: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, def), fields(tenant = %tenant, table = %table))]
    async fn set_continuous_aggregate(
        &self,
        tenant: &TenantId,
        table: &TableName,
        def: Option<CvDef>,
    ) -> Result<()> {
        let sch = &self.schema;
        let json: Option<serde_json::Value> = match &def {
            Some(d) => Some(serde_json::to_value(d).map_err(|e| {
                BasinError::catalog(format!("serialise continuous_aggregate: {e}"))
            })?),
            None => None,
        };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET continuous_aggregate_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_continuous_aggregate: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_cluster_columns(
        &self,
        tenant: &TenantId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let sch = &self.schema;
        // Empty list serialises to a JSONB array [] (not NULL). Either is
        // valid storage; we keep the array so a follow-up UPDATE that
        // *clears* the spec ends up with NULL again only if explicitly
        // chosen — keeps round-tripping deterministic.
        let json: Option<serde_json::Value> = if columns.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&columns).map_err(|e| {
                BasinError::catalog(format!("serialise cluster_columns: {e}"))
            })?)
        };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET cluster_columns_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_cluster_columns: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, schema), fields(tenant = %tenant, table = %table))]
    async fn set_schema(
        &self,
        tenant: &TenantId,
        table: &TableName,
        schema: Schema,
    ) -> Result<()> {
        let sch = &self.schema;
        let schema_json = serde_json::to_value(&schema)
            .map_err(|e| BasinError::catalog(format!("serialise arrow schema: {e}")))?;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET schema_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &schema_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_schema: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
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

    #[instrument(skip(self), fields(tenant = %tenant, src = %src_table, dst = %dst_table))]
    async fn fork_table(
        &self,
        tenant: &TenantId,
        src_table: &TableName,
        dst_table: &TableName,
    ) -> Result<TableMetadata> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let src_str = src_table.to_string();
        let dst_str = dst_table.to_string();
        let mut client = self.client.lock().await;
        let txn = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("fork txn begin: {e}")))?;

        // 1. Lock & verify source row exists; fail fast if it doesn't.
        let src_row = txn
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.tables \
                     WHERE tenant_id = $1 AND table_name = $2 \
                     FOR UPDATE"
                ),
                &[&tenant_str, &src_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("fork src lookup: {e}")))?;
        if src_row.is_none() {
            return Err(BasinError::not_found(format!("{tenant}/{src_table}")));
        }

        // 2. Copy the table row into the dst row (`INSERT … SELECT …`).
        // `ON CONFLICT DO NOTHING` lets us detect a pre-existing dst by
        // counting affected rows.
        let inserted = txn
            .execute(
                &format!(
                    "INSERT INTO {sch}.tables (
                        tenant_id, table_name, schema_json, current_snapshot,
                        format_version, partition_spec_json, rls_enabled,
                        policies_json, cold_after_seconds, cold_age_column,
                        bloom_filter_columns_json, row_group_rows,
                        continuous_aggregate_json, cluster_columns_json
                     )
                     SELECT tenant_id, $3, schema_json, current_snapshot,
                            format_version, partition_spec_json, rls_enabled,
                            policies_json, cold_after_seconds, cold_age_column,
                            bloom_filter_columns_json, row_group_rows,
                            continuous_aggregate_json, cluster_columns_json
                     FROM {sch}.tables
                     WHERE tenant_id = $1 AND table_name = $2
                     ON CONFLICT (tenant_id, table_name) DO NOTHING"
                ),
                &[&tenant_str, &src_str, &dst_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("fork insert table: {e}")))?;
        if inserted == 0 {
            return Err(BasinError::catalog(format!(
                "fork_table: {tenant}/{dst_table} already exists",
            )));
        }

        // 3. Copy every snapshot row, retargeted at the new table name.
        // Snapshot ids are preserved verbatim — the fork has identical
        // history up to its creation point, then diverges.
        txn.execute(
            &format!(
                "INSERT INTO {sch}.snapshots (
                    tenant_id, table_name, snapshot_id, parent_id,
                    operation, committed_at, summary_json, data_files
                 )
                 SELECT tenant_id, $3, snapshot_id, parent_id,
                        operation, committed_at, summary_json, data_files
                 FROM {sch}.snapshots
                 WHERE tenant_id = $1 AND table_name = $2"
            ),
            &[&tenant_str, &src_str, &dst_str],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("fork copy snapshots: {e}")))?;

        txn.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("fork commit: {e}")))?;

        // Re-fetch through the public API for a metadata value identical
        // to what `load_table` produces.
        drop(client);
        self.load_table(tenant, dst_table).await
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, snapshot = %snapshot_id))]
    async fn rollback_to_snapshot(
        &self,
        tenant: &TenantId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();
        let snap_id_i64 = snapshot_id.0 as i64;
        let mut client = self.client.lock().await;
        let txn = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("rollback txn begin: {e}")))?;

        // 1. Existence + snapshot-in-history check, with row lock to keep
        // a concurrent commit from racing the truncate.
        let row = txn
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.tables \
                     WHERE tenant_id = $1 AND table_name = $2 \
                     FOR UPDATE"
                ),
                &[&tenant_str, &table_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("rollback table lookup: {e}")))?;
        if row.is_none() {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        let snap_row = txn
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.snapshots \
                     WHERE tenant_id = $1 AND table_name = $2 \
                       AND snapshot_id = $3"
                ),
                &[&tenant_str, &table_str, &snap_id_i64],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("rollback snapshot lookup: {e}")))?;
        if snap_row.is_none() {
            return Err(BasinError::not_found(format!(
                "{tenant}/{table}: snapshot {snapshot_id} not in history",
            )));
        }

        // 2. Drop snapshots newer than the target.
        txn.execute(
            &format!(
                "DELETE FROM {sch}.snapshots \
                 WHERE tenant_id = $1 AND table_name = $2 \
                   AND snapshot_id > $3"
            ),
            &[&tenant_str, &table_str, &snap_id_i64],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("rollback snapshot delete: {e}")))?;

        // 3. Move the head pointer back.
        txn.execute(
            &format!(
                "UPDATE {sch}.tables SET current_snapshot = $3 \
                 WHERE tenant_id = $1 AND table_name = $2"
            ),
            &[&tenant_str, &table_str, &snap_id_i64],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("rollback head update: {e}")))?;

        txn.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("rollback commit: {e}")))?;

        // Drop the lock guard before re-entering load_table (which also
        // grabs the lock). Re-fetch through the public API to keep the
        // returned metadata identical to what `load_table` would yield.
        drop(client);
        self.load_table(tenant, table).await
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
            column_stats: std::collections::BTreeMap::new(),
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

    #[tokio::test]
    async fn replace_data_files_concurrent_one_wins() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let cat = Arc::new(cat);
        let t = TenantId::new();
        let tbl = TableName::new("reprace").unwrap();
        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &tbl,
            SnapshotId::GENESIS,
            vec![file("seed.parquet", 1, 10)],
        )
        .await
        .unwrap();

        let c1 = cat.clone();
        let c2 = cat.clone();
        let t1 = t;
        let t2 = t;
        let tbl1 = tbl.clone();
        let tbl2 = tbl.clone();
        let h1 = tokio::spawn(async move {
            c1.replace_data_files(
                &t1,
                &tbl1,
                SnapshotId(1),
                vec!["seed.parquet".to_string()],
                vec![file("a.parquet", 1, 10)],
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            c2.replace_data_files(
                &t2,
                &tbl2,
                SnapshotId(1),
                vec!["seed.parquet".to_string()],
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
        assert_eq!(oks, 1, "exactly one replace must win: {r1:?} {r2:?}");
        assert_eq!(
            conflicts, 1,
            "exactly one replace must conflict: {r1:?} {r2:?}"
        );

        let head = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(head.current_snapshot, SnapshotId(2));
        let snap = head.current().unwrap();
        assert_eq!(snap.summary.operation, SnapshotOperation::Replace);
        assert_eq!(snap.summary.removed_files, 1);
    }
}
