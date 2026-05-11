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
use basin_common::{BasinError, ChangeOp, Result, TableName, TenantId};
use chrono::Utc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::instrument;

use crate::domains::{self, DomainDef, DomainError, BASIN_DOMAIN_KEY};
use crate::enums::{self, EnumError, EnumTypeDef, BASIN_ENUM_TYPE_KEY};
use crate::functions::SqlFunctionDef;
use crate::metadata::{
    CheckConstraint, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy, SecondaryIndex,
    TableMetadata, UniqueConstraint,
};
use crate::procedures::{self, ProcedureError, SqlProcedureDef};
use crate::reactors::{self, ReactorDef, ReactorError, ReactorOps};
use crate::sequences::{compute_next, SequenceDef, SequenceError};
use crate::snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};
use crate::tenant_storage_config::TenantStorageConfig;
use crate::{Catalog, ProjectSnapshotEntry};

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
            // Phase 6 forward-compat (ADR 0009): per-table home region
            // for the multi-region scaffolding. NULL / absent means "not
            // pinned" — the default for back-compat. v0.1 records the
            // value but does not yet route on it; the cross-region
            // forwarding / replication that will consume it is future
            // work, see ADR 0009.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS home_region TEXT"
            ),
            // Constraint enforcement (PK / CHECK / FK). Each is a JSONB
            // payload — empty array / null means "no constraints"
            // (back-compat: old rows deserialise to empty vecs).
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS pk_columns_json JSONB"
            ),
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS check_constraints_json JSONB"
            ),
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS foreign_keys_json JSONB"
            ),
            // UNIQUE constraints and secondary-index declarations. Stored
            // separately because uniqueness is enforced by the engine write
            // path, while indexes are currently metadata/introspection only.
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS unique_constraints_json JSONB"
            ),
            format!(
                "ALTER TABLE {schema}.tables
                 ADD COLUMN IF NOT EXISTS indexes_json JSONB"
            ),
            // Per-tenant SQL function definitions. One row per
            // (tenant, name); body is stored as raw SQL, args/return as
            // JSONB so future scalar / argument-type extensions don't
            // need another migration.
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.sql_functions (
                    tenant_id    TEXT NOT NULL,
                    name         TEXT NOT NULL,
                    args_json    JSONB NOT NULL,
                    return_json  JSONB NOT NULL,
                    body         TEXT NOT NULL,
                    language     TEXT NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, name)
                )"
            ),
            // Per-tenant sequences. The persisted state is the row-locked
            // `current_value` field; `nextval` does an UPDATE … RETURNING
            // inside a row lock so concurrent callers always see distinct
            // values without an ad-hoc mutex layer. `started` distinguishes
            // "the next nextval should return start" from "the next nextval
            // should return current_value + increment", matching the
            // in-memory `SequenceState::started` flag.
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.sequences (
                    tenant_id      TEXT NOT NULL,
                    name           TEXT NOT NULL,
                    start_value    BIGINT NOT NULL,
                    increment      BIGINT NOT NULL,
                    min_value      BIGINT NOT NULL,
                    max_value      BIGINT NOT NULL,
                    cache_size     BIGINT NOT NULL,
                    cycle          BOOLEAN NOT NULL,
                    current_value  BIGINT NOT NULL,
                    started        BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, name)
                )"
            ),
            // Per-tenant reactors. Composite-key shape mirrors
            // [`crate::in_memory`]: name is unique per `(tenant, table)`,
            // not per tenant. `seq` is a per-row monotonic counter so
            // `lookup_reactors_for` can replay reactors in registration
            // order. The bitset for ops is stored as a SMALLINT (`u8` is
            // the underlying repr).
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.reactors (
                    tenant_id        TEXT NOT NULL,
                    table_name       TEXT NOT NULL,
                    name             TEXT NOT NULL,
                    ops_bits         SMALLINT NOT NULL,
                    when_predicate   TEXT,
                    body             TEXT NOT NULL,
                    seq              BIGINT NOT NULL,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, table_name, name)
                )"
            ),
            // Sequence used to assign reactor `seq` registration indices.
            // One global sequence keeps assignment monotonic across all
            // tenants without a per-tenant counter row; lookup orders by
            // the value, so cross-tenant interleaving is fine.
            format!("CREATE SEQUENCE IF NOT EXISTS {schema}.reactor_seq START 1"),
            // Per-tenant `CREATE TYPE … AS ENUM` rows. `labels` is the
            // ordered JSONB array of label strings; `ALTER TYPE … ADD
            // VALUE` appends to it inside a row-locked transaction.
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.enum_types (
                    tenant_id  TEXT NOT NULL,
                    name       TEXT NOT NULL,
                    labels     JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, name)
                )"
            ),
            // Per-tenant `CREATE DOMAIN` rows. `base_type_json` is the
            // serialised `SqlArgType` so future variants don't require a
            // migration; `check_predicate` is nullable for "pure type
            // alias, no constraint".
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.domains (
                    tenant_id        TEXT NOT NULL,
                    name             TEXT NOT NULL,
                    base_type_json   JSONB NOT NULL,
                    check_predicate  TEXT,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, name)
                )"
            ),
            // Per-tenant `CREATE PROCEDURE … LANGUAGE sql` rows. `args_json`
            // is the serialised `Vec<SqlFunctionArg>`; `body` is stored
            // verbatim so the engine reparses on each `CALL`.
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.procedures (
                    tenant_id  TEXT NOT NULL,
                    name       TEXT NOT NULL,
                    body       TEXT NOT NULL,
                    args_json  JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, name)
                )"
            ),
            // Per-tenant storage config (KMS routing + provider extras).
            // One row per tenant; `config_json` carries the full
            // `TenantStorageConfig` shape so future fields don't need
            // another migration. `INSERT … ON CONFLICT DO UPDATE` for
            // `set_tenant_storage_config` matches the existing
            // `register_sql_function` upsert pattern.
            format!(
                "CREATE TABLE IF NOT EXISTS {schema}.tenant_storage_config (
                    tenant_id   TEXT PRIMARY KEY,
                    config_json JSONB NOT NULL,
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
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
            // Phase 6 multi-region scaffolding (ADR 0009). New tables are
            // not pinned by default; `set_home_region` is the only mutator.
            home_region: None,
            // Phase 5.7 B1 (parallel agent) — indexes placeholder so the
            // metadata struct stays buildable; B1 wires real reads.
            indexes: Vec::new(),
            pk_columns: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
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
                            continuous_aggregate_json, cluster_columns_json,
                            home_region,
                            pk_columns_json, check_constraints_json, foreign_keys_json,
                            unique_constraints_json, indexes_json
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
        let pk_columns_json: Option<serde_json::Value> = row.get(13);
        let check_constraints_json: Option<serde_json::Value> = row.get(14);
        let foreign_keys_json: Option<serde_json::Value> = row.get(15);
        let unique_constraints_json: Option<serde_json::Value> = row.get(16);
        let indexes_json: Option<serde_json::Value> = row.get(17);
        let arrow_schema: Schema = serde_json::from_value(schema_json)
            .map_err(|e| BasinError::catalog(format!("deserialise arrow schema: {e}")))?;
        let partition_spec = match partition_spec_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise partition spec: {e}")))?,
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
        let row_group_rows: Option<usize> = row_group_rows_pg.and_then(|v| {
            if v >= 0 {
                usize::try_from(v).ok()
            } else {
                None
            }
        });
        let continuous_aggregate: Option<CvDef> = match continuous_aggregate_json {
            Some(v) => Some(serde_json::from_value(v).map_err(|e| {
                BasinError::catalog(format!("deserialise continuous_aggregate: {e}"))
            })?),
            None => None,
        };
        let cluster_columns: Vec<String> = match cluster_columns_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise cluster_columns: {e}")))?,
            None => Vec::new(),
        };
        let home_region: Option<String> = row.get(12);
        let pk_columns: Vec<String> = match pk_columns_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise pk_columns: {e}")))?,
            None => Vec::new(),
        };
        let check_constraints: Vec<CheckConstraint> = match check_constraints_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise check_constraints: {e}")))?,
            None => Vec::new(),
        };
        let foreign_keys: Vec<ForeignKeyDef> = match foreign_keys_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise foreign_keys: {e}")))?,
            None => Vec::new(),
        };
        let unique_constraints: Vec<UniqueConstraint> = match unique_constraints_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise unique_constraints: {e}")))?,
            None => Vec::new(),
        };
        let indexes: Vec<SecondaryIndex> = match indexes_json {
            Some(v) => serde_json::from_value(v)
                .map_err(|e| BasinError::catalog(format!("deserialise indexes: {e}")))?,
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
            home_region,
            indexes,
            pk_columns,
            check_constraints,
            foreign_keys,
            unique_constraints,
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
            &format!("DELETE FROM {sch}.sql_functions WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace sql_functions: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.sequences WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace sequences: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.reactors WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace reactors: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.enum_types WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace enum_types: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.domains WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace domains: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.procedures WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace procedures: {e}")))?;
        tx.execute(
            &format!("DELETE FROM {sch}.tenant_storage_config WHERE tenant_id = $1"),
            &[&tenant.to_string()],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("drop_namespace tenant_storage_config: {e}")))?;
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
                &format!("SELECT data_files FROM {sch}.snapshots WHERE tenant_id = $1"),
                &[&tenant.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_tenant_data_files: {e}")))?;
        let mut out: Vec<DataFileRef> = Vec::new();
        for row in rows {
            let files_json: serde_json::Value = row.get(0);
            let files: Vec<DataFileRef> = serde_json::from_value(files_json)
                .map_err(|e| BasinError::catalog(format!("deserialise data files: {e}")))?;
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
        let json = serde_json::to_value(&columns)
            .map_err(|e| BasinError::catalog(format!("serialise bloom_filter_columns: {e}")))?;
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
            Some(
                serde_json::to_value(&columns)
                    .map_err(|e| BasinError::catalog(format!("serialise cluster_columns: {e}")))?,
            )
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

    #[instrument(skip(self), fields(tenant = %tenant, table = %table))]
    async fn set_home_region(
        &self,
        tenant: &TenantId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables SET home_region = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &region],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_home_region: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, check_constraints, foreign_keys), fields(tenant = %tenant, table = %table))]
    async fn set_table_constraints(
        &self,
        tenant: &TenantId,
        table: &TableName,
        pk_columns: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    ) -> Result<()> {
        let sch = &self.schema;
        let pk_json: Option<serde_json::Value> = if pk_columns.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&pk_columns)
                    .map_err(|e| BasinError::catalog(format!("serialise pk_columns: {e}")))?,
            )
        };
        let check_json: Option<serde_json::Value> =
            if check_constraints.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&check_constraints).map_err(|e| {
                    BasinError::catalog(format!("serialise check_constraints: {e}"))
                })?)
            };
        let fk_json: Option<serde_json::Value> = if foreign_keys.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&foreign_keys)
                    .map_err(|e| BasinError::catalog(format!("serialise foreign_keys: {e}")))?,
            )
        };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables
                     SET pk_columns_json = $3,
                         check_constraints_json = $4,
                         foreign_keys_json = $5
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[
                    &tenant.to_string(),
                    &table.to_string(),
                    &pk_json,
                    &check_json,
                    &fk_json,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_table_constraints: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, unique_constraints), fields(tenant = %tenant, table = %table))]
    async fn set_unique_constraints(
        &self,
        tenant: &TenantId,
        table: &TableName,
        unique_constraints: Vec<UniqueConstraint>,
    ) -> Result<()> {
        let sch = &self.schema;
        let unique_json: Option<serde_json::Value> =
            if unique_constraints.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&unique_constraints).map_err(|e| {
                    BasinError::catalog(format!("serialise unique_constraints: {e}"))
                })?)
            };
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables
                     SET unique_constraints_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &unique_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_unique_constraints: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, columns), fields(tenant = %tenant, table = %table, name = %name))]
    async fn create_index(
        &self,
        tenant: &TenantId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        if columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "create_index: column list cannot be empty".into(),
            ));
        }

        let mut meta = self.load_table(tenant, table).await?;
        for col in columns {
            if meta.schema.field_with_name(col).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "create_index: column {col:?} not in table {tenant}/{table} schema"
                )));
            }
        }
        if meta.indexes.iter().any(|i| i.name == name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(BasinError::catalog(format!(
                "create_index: {tenant}/{table}: index {name:?} already exists"
            )));
        }
        meta.indexes.push(SecondaryIndex {
            name: name.to_string(),
            columns: columns.to_vec(),
        });
        let indexes_json = serde_json::to_value(&meta.indexes)
            .map_err(|e| BasinError::catalog(format!("serialise indexes: {e}")))?;
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables
                     SET indexes_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &indexes_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("create_index: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, name = %name))]
    async fn drop_index(&self, tenant: &TenantId, table: &TableName, name: &str) -> Result<()> {
        let mut meta = self.load_table(tenant, table).await?;
        let before = meta.indexes.len();
        meta.indexes.retain(|i| i.name != name);
        if meta.indexes.len() == before {
            return Err(BasinError::not_found(format!(
                "index {name:?} on {tenant}/{table}"
            )));
        }
        let indexes_json: Option<serde_json::Value> = if meta.indexes.is_empty() {
            None
        } else {
            Some(
                serde_json::to_value(&meta.indexes)
                    .map_err(|e| BasinError::catalog(format!("serialise indexes: {e}")))?,
            )
        };
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}.tables
                     SET indexes_json = $3
                     WHERE tenant_id = $1 AND table_name = $2"
                ),
                &[&tenant.to_string(), &table.to_string(), &indexes_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_index: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        Ok(())
    }

    #[instrument(skip(self, schema), fields(tenant = %tenant, table = %table))]
    async fn set_schema(&self, tenant: &TenantId, table: &TableName, schema: Schema) -> Result<()> {
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
    async fn list_snapshots(&self, tenant: &TenantId, table: &TableName) -> Result<Vec<Snapshot>> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let table_str = table.to_string();
        let client = self.client.lock().await;
        // Existence check so callers get NotFound (matching InMemoryCatalog)
        // rather than an empty list when the table is missing.
        let exists = client
            .query_opt(
                &format!("SELECT 1 FROM {sch}.tables WHERE tenant_id = $1 AND table_name = $2"),
                &[&tenant_str, &table_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_snapshots: {e}")))?;
        if exists.is_none() {
            return Err(BasinError::not_found(format!("{tenant}/{table}")));
        }
        fetch_snapshots(&client, sch, &tenant_str, &table_str).await
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_snapshots_project_wide(
        &self,
        tenant: &TenantId,
    ) -> Result<Vec<ProjectSnapshotEntry>> {
        // Single-pass JOIN: fan-in every snapshot for this tenant in commit
        // order. The outer ORDER BY mirrors the default impl's tie-breakers
        // so InMemory and Postgres produce byte-identical timelines.
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT t.table_name, s.snapshot_id, s.parent_id, s.committed_at,
                            s.operation, s.summary_json
                     FROM {sch}.tables t
                     JOIN {sch}.snapshots s
                       ON s.tenant_id = t.tenant_id AND s.table_name = t.table_name
                     WHERE t.tenant_id = $1
                     ORDER BY s.committed_at ASC, t.table_name ASC, s.snapshot_id ASC"
                ),
                &[&tenant.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_snapshots_project_wide: {e}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let table_name: String = row.get(0);
            let snap_id: i64 = row.get(1);
            let parent: Option<i64> = row.get(2);
            let committed_at: chrono::DateTime<Utc> = row.get(3);
            let _operation_text: String = row.get(4);
            let summary_json: serde_json::Value = row.get(5);
            let summary: SnapshotSummary = serde_json::from_value(summary_json)
                .map_err(|e| BasinError::catalog(format!("deserialise summary: {e}")))?;
            let table = TableName::new(table_name).map_err(|e| {
                BasinError::catalog(format!("list_snapshots_project_wide bad ident: {e}"))
            })?;
            out.push(ProjectSnapshotEntry {
                table,
                snapshot_id: SnapshotId(snap_id as u64),
                parent_id: parent.map(|p| SnapshotId(p as u64)),
                committed_at,
                operation: summary.operation,
                summary,
            });
        }
        Ok(out)
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
                        continuous_aggregate_json, cluster_columns_json,
                        home_region,
                        pk_columns_json, check_constraints_json, foreign_keys_json
                     )
                     SELECT tenant_id, $3, schema_json, current_snapshot,
                            format_version, partition_spec_json, rls_enabled,
                            policies_json, cold_after_seconds, cold_age_column,
                            bloom_filter_columns_json, row_group_rows,
                            continuous_aggregate_json, cluster_columns_json,
                            home_region,
                            pk_columns_json, check_constraints_json, foreign_keys_json
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

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_sql_function(&self, def: SqlFunctionDef) -> Result<()> {
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        let args_json = serde_json::to_value(&def.args)
            .map_err(|e| BasinError::catalog(format!("serialise fn args: {e}")))?;
        let return_json = serde_json::to_value(&def.return_type)
            .map_err(|e| BasinError::catalog(format!("serialise fn return: {e}")))?;
        let language = serde_json::to_value(def.language)
            .map_err(|e| BasinError::catalog(format!("serialise fn language: {e}")))?
            .as_str()
            .unwrap_or("sql")
            .to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}.sql_functions \
                     (tenant_id, name, args_json, return_json, body, language) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (tenant_id, name) DO UPDATE \
                     SET args_json = EXCLUDED.args_json, \
                         return_json = EXCLUDED.return_json, \
                         body = EXCLUDED.body, \
                         language = EXCLUDED.language"
                ),
                &[
                    &tenant_str,
                    &def.name,
                    &args_json,
                    &return_json,
                    &def.body,
                    &language,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_sql_function: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_sql_function(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.sql_functions \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant_str, &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_sql_function: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "{tenant}: sql function {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_sql_function(&self, tenant: &TenantId, name: &str) -> Option<SqlFunctionDef> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT args_json, return_json, body, language \
                     FROM {sch}.sql_functions \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant_str, &name],
            )
            .await
            .ok()
            .flatten()?;
        let args_json: serde_json::Value = row.get(0);
        let return_json: serde_json::Value = row.get(1);
        let body: String = row.get(2);
        let language_str: String = row.get(3);
        let args = serde_json::from_value(args_json).ok()?;
        let return_type = serde_json::from_value(return_json).ok()?;
        let language = serde_json::from_value(serde_json::Value::String(language_str)).ok()?;
        Some(SqlFunctionDef {
            tenant: *tenant,
            name: name.to_string(),
            args,
            return_type,
            body,
            language,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_sql_functions(&self, tenant: &TenantId) -> Vec<SqlFunctionDef> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT name, args_json, return_json, body, language \
                     FROM {sch}.sql_functions \
                     WHERE tenant_id = $1"
                ),
                &[&tenant_str],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                let args_json: serde_json::Value = row.get(1);
                let return_json: serde_json::Value = row.get(2);
                let body: String = row.get(3);
                let language_str: String = row.get(4);
                let args = serde_json::from_value(args_json).ok()?;
                let return_type = serde_json::from_value(return_json).ok()?;
                let language =
                    serde_json::from_value(serde_json::Value::String(language_str)).ok()?;
                Some(SqlFunctionDef {
                    tenant: *tenant,
                    name,
                    args,
                    return_type,
                    body,
                    language,
                })
            })
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn create_sequence(&self, def: SequenceDef) -> Result<()> {
        if def.increment == 0 {
            return Err(BasinError::InvalidSchema(
                "sequence increment must be non-zero".into(),
            ));
        }
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        // Genesis stored value mirrors `SequenceState::genesis`: the
        // first hand-out lands on `start` after the standard advance.
        let stored = def.start.wrapping_sub(def.increment);
        let cache_size_pg: i64 = def.cache_size as i64;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "INSERT INTO {sch}.sequences \
                     (tenant_id, name, start_value, increment, min_value, max_value, \
                      cache_size, cycle, current_value, started) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE) \
                     ON CONFLICT (tenant_id, name) DO NOTHING"
                ),
                &[
                    &tenant_str,
                    &def.name,
                    &def.start,
                    &def.increment,
                    &def.min_value,
                    &def.max_value,
                    &cache_size_pg,
                    &def.cycle,
                    &stored,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("create_sequence: {e}")))?;
        if n == 0 {
            return Err(BasinError::catalog(format!(
                "sequence {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_sequence(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.sequences \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_sequence: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_sequence(&self, tenant: &TenantId, name: &str) -> Option<SequenceDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT start_value, increment, min_value, max_value, cache_size, cycle \
                     FROM {sch}.sequences \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .ok()
            .flatten()?;
        let start: i64 = row.get(0);
        let increment: i64 = row.get(1);
        let min_value: i64 = row.get(2);
        let max_value: i64 = row.get(3);
        let cache_size_pg: i64 = row.get(4);
        let cycle: bool = row.get(5);
        Some(SequenceDef {
            tenant: *tenant,
            name: name.to_string(),
            start,
            increment,
            min_value,
            max_value,
            cache_size: cache_size_pg.max(0) as u64,
            cycle,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn nextval(&self, tenant: &TenantId, name: &str) -> Result<i64> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let mut client = self.client.lock().await;
        let txn = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("nextval txn: {e}")))?;

        // FOR UPDATE serializes concurrent nextval calls on the same
        // (tenant, name) without ad-hoc locks — the row lock is the
        // serialisation primitive. Two sequences (or two tenants) never
        // block each other beyond their respective row locks.
        let row = txn
            .query_opt(
                &format!(
                    "SELECT start_value, increment, min_value, max_value, cache_size, \
                            cycle, current_value, started \
                     FROM {sch}.sequences \
                     WHERE tenant_id = $1 AND name = $2 \
                     FOR UPDATE"
                ),
                &[&tenant_str, &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("nextval lookup: {e}")))?;
        let Some(row) = row else {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?}"
            )));
        };
        let def = SequenceDef {
            tenant: *tenant,
            name: name.to_string(),
            start: row.get(0),
            increment: row.get(1),
            min_value: row.get(2),
            max_value: row.get(3),
            cache_size: (row.get::<_, i64>(4)).max(0) as u64,
            cycle: row.get(5),
        };
        let last: i64 = row.get(6);
        let started: bool = row.get(7);
        let v = match compute_next(&def, last, started) {
            Ok(v) => v,
            Err(SequenceError::Exhausted) => {
                return Err(BasinError::catalog(format!(
                    "{tenant}: sequence {name:?} exhausted"
                )));
            }
            Err(SequenceError::InvalidIncrement) => {
                return Err(BasinError::InvalidSchema(format!(
                    "{tenant}: sequence {name:?} has zero increment"
                )));
            }
        };
        txn.execute(
            &format!(
                "UPDATE {sch}.sequences \
                 SET current_value = $3, started = TRUE \
                 WHERE tenant_id = $1 AND name = $2"
            ),
            &[&tenant_str, &name, &v],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("nextval update: {e}")))?;
        txn.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("nextval commit: {e}")))?;
        Ok(v)
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn currval(&self, tenant: &TenantId, name: &str) -> Result<i64> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let row_opt = client
            .query_opt(
                &format!(
                    "SELECT current_value, started FROM {sch}.sequences \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("currval: {e}")))?;
        let Some(row) = row_opt else {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?}"
            )));
        };
        let started: bool = row.get(1);
        if !started {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?} has not been advanced"
            )));
        }
        Ok(row.get(0))
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name, value = value, advance = advance))]
    async fn setval(
        &self,
        tenant: &TenantId,
        name: &str,
        value: i64,
        advance: bool,
    ) -> Result<i64> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let mut client = self.client.lock().await;
        let txn = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("setval txn: {e}")))?;
        let row = txn
            .query_opt(
                &format!(
                    "SELECT increment FROM {sch}.sequences \
                     WHERE tenant_id = $1 AND name = $2 \
                     FOR UPDATE"
                ),
                &[&tenant_str, &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("setval lookup: {e}")))?;
        let Some(row) = row else {
            return Err(BasinError::not_found(format!(
                "{tenant}: sequence {name:?}"
            )));
        };
        let increment: i64 = row.get(0);
        // Same trick as the in-memory backend: `advance == false` stores
        // `value - increment` so the next started-state advance lands on
        // exactly `value`.
        let stored = if advance {
            value
        } else {
            value.wrapping_sub(increment)
        };
        txn.execute(
            &format!(
                "UPDATE {sch}.sequences \
                 SET current_value = $3, started = TRUE \
                 WHERE tenant_id = $1 AND name = $2"
            ),
            &[&tenant_str, &name, &stored],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("setval update: {e}")))?;
        txn.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("setval commit: {e}")))?;
        Ok(value)
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, table = %def.table, name = %def.name))]
    async fn register_reactor(&self, def: ReactorDef) -> Result<()> {
        if def.ops.is_empty() {
            return Err(BasinError::InvalidSchema(
                "reactor ops bitset is empty".into(),
            ));
        }
        reactors::validate_body(&def.body).map_err(reactor_err_to_basin)?;
        if let Some(p) = &def.when_predicate {
            reactors::validate_predicate(p).map_err(reactor_err_to_basin)?;
        }
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        let table_str = def.table.to_string();
        let ops_bits: i16 = def.ops.bits() as i16;
        let mut client = self.client.lock().await;
        let txn = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("register_reactor txn: {e}")))?;
        // Allocate a registration index from the shared sequence; this is
        // monotonic across all tenants which is sufficient for ordering
        // within any single (tenant, table). Done before the INSERT so we
        // can roll the txn back cleanly on conflict.
        let seq_row = txn
            .query_one(&format!("SELECT nextval('{sch}.reactor_seq')"), &[])
            .await
            .map_err(|e| BasinError::catalog(format!("reactor seq nextval: {e}")))?;
        let seq: i64 = seq_row.get(0);
        let n = txn
            .execute(
                &format!(
                    "INSERT INTO {sch}.reactors \
                     (tenant_id, table_name, name, ops_bits, when_predicate, body, seq) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7) \
                     ON CONFLICT (tenant_id, table_name, name) DO NOTHING"
                ),
                &[
                    &tenant_str,
                    &table_str,
                    &def.name,
                    &ops_bits,
                    &def.when_predicate,
                    &def.body,
                    &seq,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_reactor: {e}")))?;
        if n == 0 {
            txn.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "reactor {:?} on {}/{} already exists",
                def.name, def.tenant, def.table
            )));
        }
        txn.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("register_reactor commit: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, name = %name))]
    async fn drop_reactor(&self, tenant: &TenantId, table: &TableName, name: &str) -> Result<()> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.reactors \
                     WHERE tenant_id = $1 AND table_name = $2 AND name = $3"
                ),
                &[&tenant.to_string(), &table.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_reactor: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "{tenant}/{table}: reactor {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, table = %table, op = ?op))]
    async fn lookup_reactors_for(
        &self,
        tenant: &TenantId,
        table: &TableName,
        op: ChangeOp,
    ) -> Vec<ReactorDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT name, ops_bits, when_predicate, body \
                     FROM {sch}.reactors \
                     WHERE tenant_id = $1 AND table_name = $2 \
                     ORDER BY seq ASC"
                ),
                &[&tenant.to_string(), &table.to_string()],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                let ops_bits: i16 = row.get(1);
                let when_predicate: Option<String> = row.get(2);
                let body: String = row.get(3);
                let ops = ReactorOps::from_bits(ops_bits as u8)?;
                if !ops.matches(op) {
                    return None;
                }
                Some(ReactorDef {
                    tenant: *tenant,
                    table: table.clone(),
                    name,
                    ops,
                    when_predicate,
                    body,
                })
            })
            .collect()
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_reactors(&self, tenant: &TenantId) -> Vec<ReactorDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT table_name, name, ops_bits, when_predicate, body \
                     FROM {sch}.reactors \
                     WHERE tenant_id = $1 \
                     ORDER BY seq ASC"
                ),
                &[&tenant.to_string()],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let table_str: String = row.get(0);
                let name: String = row.get(1);
                let ops_bits: i16 = row.get(2);
                let when_predicate: Option<String> = row.get(3);
                let body: String = row.get(4);
                let table = TableName::new(table_str).ok()?;
                let ops = ReactorOps::from_bits(ops_bits as u8)?;
                Some(ReactorDef {
                    tenant: *tenant,
                    table,
                    name,
                    ops,
                    when_predicate,
                    body,
                })
            })
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_enum_type(&self, def: EnumTypeDef) -> Result<()> {
        // Validate first so duplicate-label / empty-list errors take
        // precedence over the SQL-level uniqueness check, matching the
        // in-memory ordering.
        enums::validate_new(&def).map_err(enum_err_to_basin)?;
        let labels_json = serde_json::to_value(&def.labels)
            .map_err(|e| BasinError::catalog(format!("serialise enum labels: {e}")))?;
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("register_enum_type txn: {e}")))?;
        // Cross-namespace collision: a domain with the same name on the
        // same tenant is rejected so column resolution stays unambiguous.
        let dom_row = tx
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.domains \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant_str, &def.name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_enum_type collision: {e}")))?;
        if dom_row.is_some() {
            tx.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "type {}/{} collides with an existing domain",
                def.tenant, def.name,
            )));
        }
        let n = tx
            .execute(
                &format!(
                    "INSERT INTO {sch}.enum_types (tenant_id, name, labels) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (tenant_id, name) DO NOTHING"
                ),
                &[&tenant_str, &def.name, &labels_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_enum_type: {e}")))?;
        if n == 0 {
            tx.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "enum type {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("register_enum_type commit: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_enum_type(&self, tenant: &TenantId, name: &str) -> Option<EnumTypeDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT labels FROM {sch}.enum_types \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .ok()
            .flatten()?;
        let labels_json: serde_json::Value = row.get(0);
        let labels: Vec<String> = serde_json::from_value(labels_json).ok()?;
        Some(EnumTypeDef {
            tenant: *tenant,
            name: name.to_string(),
            labels,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name, value = %value))]
    async fn add_enum_value(&self, tenant: &TenantId, name: &str, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(BasinError::InvalidSchema(
                "ALTER TYPE ADD VALUE: label cannot be empty".into(),
            ));
        }
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("add_enum_value txn: {e}")))?;
        // FOR UPDATE row-locks the enum row so a concurrent
        // add_enum_value waits for our commit. The read-then-append
        // dance is the same primitive as `nextval`'s row-locked update.
        let row = tx
            .query_opt(
                &format!(
                    "SELECT labels FROM {sch}.enum_types \
                     WHERE tenant_id = $1 AND name = $2 \
                     FOR UPDATE"
                ),
                &[&tenant_str, &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("add_enum_value lookup: {e}")))?;
        let Some(row) = row else {
            return Err(BasinError::not_found(format!(
                "{tenant}: enum type {name:?}"
            )));
        };
        let labels_json: serde_json::Value = row.get(0);
        let labels: Vec<String> = serde_json::from_value(labels_json)
            .map_err(|e| BasinError::catalog(format!("deserialise enum labels: {e}")))?;
        if labels.iter().any(|l| l == value) {
            return Err(BasinError::catalog(format!(
                "enum {name:?} already contains value {value:?}"
            )));
        }
        // Append via JSONB concat. Equivalent to `labels.push(value)` in
        // the in-memory backend; the row lock ensures the read above
        // sees the same array we mutate here.
        let value_arr =
            serde_json::Value::Array(vec![serde_json::Value::String(value.to_string())]);
        tx.execute(
            &format!(
                "UPDATE {sch}.enum_types \
                 SET labels = labels || $3::jsonb \
                 WHERE tenant_id = $1 AND name = $2"
            ),
            &[&tenant_str, &name, &value_arr],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("add_enum_value update: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("add_enum_value commit: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_enum_type(&self, tenant: &TenantId, name: &str) -> Result<()> {
        // Refcount enforcement: load every table's Arrow schema and
        // reject the drop if any column carries `BASIN_ENUM_TYPE=<name>`.
        // Mirrors the in-memory backend's `tables_referencing_type`
        // approach exactly — same metadata key, same lazy scan.
        let referencing = self.tables_referencing_type(tenant, name, true).await?;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop enum {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.enum_types \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_enum_type: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "{tenant}: enum type {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_enum_types(&self, tenant: &TenantId) -> Vec<EnumTypeDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT name, labels FROM {sch}.enum_types \
                     WHERE tenant_id = $1"
                ),
                &[&tenant.to_string()],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                let labels_json: serde_json::Value = row.get(1);
                let labels: Vec<String> = serde_json::from_value(labels_json).ok()?;
                Some(EnumTypeDef {
                    tenant: *tenant,
                    name,
                    labels,
                })
            })
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_domain(&self, def: DomainDef) -> Result<()> {
        domains::validate_new(&def).map_err(domain_err_to_basin)?;
        let base_type_json = serde_json::to_value(def.base_type)
            .map_err(|e| BasinError::catalog(format!("serialise domain base_type: {e}")))?;
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        let mut client = self.client.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|e| BasinError::catalog(format!("register_domain txn: {e}")))?;
        // Cross-namespace collision: an enum with the same name on the
        // same tenant is rejected — same rule as register_enum_type, in
        // the opposite direction.
        let enum_row = tx
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}.enum_types \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant_str, &def.name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_domain collision: {e}")))?;
        if enum_row.is_some() {
            tx.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "domain {}/{} collides with an existing enum type",
                def.tenant, def.name,
            )));
        }
        let n = tx
            .execute(
                &format!(
                    "INSERT INTO {sch}.domains (tenant_id, name, base_type_json, check_predicate) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (tenant_id, name) DO NOTHING"
                ),
                &[
                    &tenant_str,
                    &def.name,
                    &base_type_json,
                    &def.check_predicate,
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_domain: {e}")))?;
        if n == 0 {
            tx.rollback().await.ok();
            return Err(BasinError::catalog(format!(
                "domain {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        tx.commit()
            .await
            .map_err(|e| BasinError::catalog(format!("register_domain commit: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_domain(&self, tenant: &TenantId, name: &str) -> Option<DomainDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT base_type_json, check_predicate FROM {sch}.domains \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .ok()
            .flatten()?;
        let base_type_json: serde_json::Value = row.get(0);
        let base_type = serde_json::from_value(base_type_json).ok()?;
        let check_predicate: Option<String> = row.get(1);
        Some(DomainDef {
            tenant: *tenant,
            name: name.to_string(),
            base_type,
            check_predicate,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_domain(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let referencing = self.tables_referencing_type(tenant, name, false).await?;
        if !referencing.is_empty() {
            return Err(BasinError::catalog(format!(
                "cannot drop domain {name:?}: still referenced by table column(s) {referencing:?}; \
                 drop the column(s) first (v0.1 has no CASCADE)"
            )));
        }
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.domains \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_domain: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!("{tenant}: domain {name:?}")));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_domains(&self, tenant: &TenantId) -> Vec<DomainDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT name, base_type_json, check_predicate FROM {sch}.domains \
                     WHERE tenant_id = $1"
                ),
                &[&tenant.to_string()],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                let base_type_json: serde_json::Value = row.get(1);
                let base_type = serde_json::from_value(base_type_json).ok()?;
                let check_predicate: Option<String> = row.get(2);
                Some(DomainDef {
                    tenant: *tenant,
                    name,
                    base_type,
                    check_predicate,
                })
            })
            .collect()
    }

    #[instrument(skip(self, def), fields(tenant = %def.tenant, name = %def.name))]
    async fn register_procedure(&self, def: SqlProcedureDef) -> Result<()> {
        procedures::validate_new(&def).map_err(procedure_err_to_basin)?;
        let args_json = serde_json::to_value(&def.args)
            .map_err(|e| BasinError::catalog(format!("serialise procedure args: {e}")))?;
        let sch = &self.schema;
        let tenant_str = def.tenant.to_string();
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "INSERT INTO {sch}.procedures (tenant_id, name, body, args_json) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (tenant_id, name) DO NOTHING"
                ),
                &[&tenant_str, &def.name, &def.body, &args_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("register_procedure: {e}")))?;
        if n == 0 {
            return Err(BasinError::catalog(format!(
                "procedure {}/{} already exists",
                def.tenant, def.name,
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn drop_procedure(&self, tenant: &TenantId, name: &str) -> Result<()> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}.procedures \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("drop_procedure: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "{tenant}: procedure {name:?}"
            )));
        }
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant, name = %name))]
    async fn lookup_procedure(&self, tenant: &TenantId, name: &str) -> Option<SqlProcedureDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT body, args_json FROM {sch}.procedures \
                     WHERE tenant_id = $1 AND name = $2"
                ),
                &[&tenant.to_string(), &name],
            )
            .await
            .ok()
            .flatten()?;
        let body: String = row.get(0);
        let args_json: serde_json::Value = row.get(1);
        let args = serde_json::from_value(args_json).ok()?;
        Some(SqlProcedureDef {
            tenant: *tenant,
            name: name.to_string(),
            args,
            body,
        })
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn list_procedures(&self, tenant: &TenantId) -> Vec<SqlProcedureDef> {
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = match client
            .query(
                &format!(
                    "SELECT name, body, args_json FROM {sch}.procedures \
                     WHERE tenant_id = $1"
                ),
                &[&tenant.to_string()],
            )
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .filter_map(|row| {
                let name: String = row.get(0);
                let body: String = row.get(1);
                let args_json: serde_json::Value = row.get(2);
                let args = serde_json::from_value(args_json).ok()?;
                Some(SqlProcedureDef {
                    tenant: *tenant,
                    name,
                    args,
                    body,
                })
            })
            .collect()
    }

    #[instrument(skip(self, config), fields(tenant = %tenant))]
    async fn set_tenant_storage_config(
        &self,
        tenant: &TenantId,
        config: TenantStorageConfig,
    ) -> Result<()> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let config_json = serde_json::to_value(&config)
            .map_err(|e| BasinError::catalog(format!("serialise tenant_storage_config: {e}")))?;
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}.tenant_storage_config \
                     (tenant_id, config_json, updated_at) \
                     VALUES ($1, $2, now()) \
                     ON CONFLICT (tenant_id) DO UPDATE \
                     SET config_json = EXCLUDED.config_json, \
                         updated_at = now()"
                ),
                &[&tenant_str, &config_json],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("set_tenant_storage_config: {e}")))?;
        Ok(())
    }

    #[instrument(skip(self), fields(tenant = %tenant))]
    async fn get_tenant_storage_config(
        &self,
        tenant: &TenantId,
    ) -> Result<Option<TenantStorageConfig>> {
        let sch = &self.schema;
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT config_json FROM {sch}.tenant_storage_config \
                     WHERE tenant_id = $1"
                ),
                &[&tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("get_tenant_storage_config: {e}")))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let config_json: serde_json::Value = row.get(0);
        let config: TenantStorageConfig = serde_json::from_value(config_json)
            .map_err(|e| BasinError::catalog(format!("deserialise tenant_storage_config: {e}")))?;
        Ok(Some(config))
    }
}

impl PostgresCatalog {
    /// Walk every table owned by `tenant`, returning `<table>.<column>`
    /// labels for every column whose Arrow `Field` carries the requested
    /// type metadata. `is_enum == true` checks the `BASIN_ENUM_TYPE`
    /// key; `false` checks `BASIN_DOMAIN`. Mirrors
    /// `InMemoryCatalog::tables_referencing_type`.
    async fn tables_referencing_type(
        &self,
        tenant: &TenantId,
        type_name: &str,
        is_enum: bool,
    ) -> Result<Vec<String>> {
        let key = if is_enum {
            BASIN_ENUM_TYPE_KEY
        } else {
            BASIN_DOMAIN_KEY
        };
        let sch = &self.schema;
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT table_name, schema_json FROM {sch}.tables \
                     WHERE tenant_id = $1"
                ),
                &[&tenant.to_string()],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("tables_referencing_type: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let table_str: String = row.get(0);
            let schema_json: serde_json::Value = row.get(1);
            let arrow_schema: Schema = match serde_json::from_value(schema_json) {
                Ok(s) => s,
                Err(_) => continue,
            };
            for f in arrow_schema.fields() {
                if f.metadata().get(key).map(|s| s.as_str()) == Some(type_name) {
                    out.push(format!("{table_str}.{}", f.name()));
                }
            }
        }
        Ok(out)
    }
}

/// Map [`EnumError`] into the cross-crate [`BasinError`] surface.
/// Mirrors `crate::in_memory::enum_err_to_basin`.
fn enum_err_to_basin(e: EnumError) -> BasinError {
    match e {
        EnumError::DuplicateLabel(l) => {
            BasinError::InvalidSchema(format!("enum label {l:?} listed more than once"))
        }
        EnumError::EmptyLabelList => {
            BasinError::InvalidSchema("enum type must have at least one label".into())
        }
        EnumError::EmptyLabel => {
            BasinError::InvalidSchema("enum label must be a non-empty string".into())
        }
        EnumError::Duplicate => BasinError::Catalog("enum type already exists".into()),
        EnumError::NotFound => BasinError::NotFound("enum type not found".into()),
        EnumError::LabelAlreadyExists(l) => {
            BasinError::Catalog(format!("enum already contains value {l:?}"))
        }
    }
}

/// Map [`DomainError`] into the cross-crate [`BasinError`] surface.
/// Mirrors `crate::in_memory::domain_err_to_basin`.
fn domain_err_to_basin(e: DomainError) -> BasinError {
    match e {
        DomainError::Duplicate => BasinError::Catalog("domain already exists".into()),
        DomainError::NotFound => BasinError::NotFound("domain not found".into()),
        DomainError::InvalidPredicate(msg) => {
            BasinError::InvalidSchema(format!("domain CHECK predicate: {msg}"))
        }
    }
}

/// Map [`ProcedureError`] into the cross-crate [`BasinError`] surface.
/// Mirrors `crate::in_memory::procedure_err_to_basin`.
fn procedure_err_to_basin(e: ProcedureError) -> BasinError {
    match e {
        ProcedureError::InvalidBody(msg) => {
            BasinError::InvalidSchema(format!("procedure body: {msg}"))
        }
        ProcedureError::DisallowedStatement(msg) => BasinError::InvalidSchema(msg),
        ProcedureError::DuplicateArgName(name) => {
            BasinError::InvalidSchema(format!("duplicate procedure argument name {name:?}"))
        }
        ProcedureError::InvalidName(msg) => BasinError::InvalidIdent(msg),
    }
}

/// Map [`ReactorError`] into the cross-crate [`BasinError`] surface.
/// Mirrors `crate::in_memory::reactor_err_to_basin` but lives here to
/// avoid a cross-module dependency from `postgres.rs` into the in-memory
/// implementation.
fn reactor_err_to_basin(e: ReactorError) -> BasinError {
    match e {
        ReactorError::Duplicate => {
            BasinError::catalog("reactor already registered for this (tenant, table)")
        }
        ReactorError::InvalidBody(msg) => BasinError::InvalidSchema(format!("reactor body: {msg}")),
        ReactorError::InvalidPredicate(msg) => {
            BasinError::InvalidSchema(format!("reactor predicate: {msg}"))
        }
        ReactorError::NoOps => BasinError::InvalidSchema("reactor ops bitset is empty".into()),
        ReactorError::MultiStatementBody => {
            BasinError::InvalidSchema("reactor body must be a single SQL statement".into())
        }
        ReactorError::NotFound => BasinError::not_found("reactor not found"),
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
        format!(
            "basin_catalog_test_{}",
            Ulid::new().to_string().to_lowercase()
        )
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
    async fn unique_constraints_and_indexes_round_trip() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("users").unwrap();

        cat.create_table(&t, &tbl, &schema()).await.unwrap();
        cat.set_unique_constraints(
            &t,
            &tbl,
            vec![UniqueConstraint {
                name: "users_name_key".into(),
                columns: vec!["name".into()],
            }],
        )
        .await
        .unwrap();
        cat.create_index(&t, &tbl, "users_name_idx", &["name".into()], false)
            .await
            .unwrap();

        let loaded = cat.load_table(&t, &tbl).await.unwrap();
        assert_eq!(
            loaded.unique_constraints,
            vec![UniqueConstraint {
                name: "users_name_key".into(),
                columns: vec!["name".into()],
            }]
        );
        assert_eq!(
            loaded.indexes,
            vec![SecondaryIndex {
                name: "users_name_idx".into(),
                columns: vec!["name".into()],
            }]
        );

        cat.drop_index(&t, &tbl, "users_name_idx").await.unwrap();
        let loaded = cat.load_table(&t, &tbl).await.unwrap();
        assert!(loaded.indexes.is_empty());
        assert_eq!(loaded.unique_constraints[0].name, "users_name_key");
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
        assert_eq!(
            conflicts, 1,
            "exactly one append must conflict: {r1:?} {r2:?}"
        );

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

    /// Migration Manager v0.2: the Postgres single-query path returns the
    /// same logical shape as the InMemory default impl. This test runs end-
    /// to-end against the live database; it pg_alive-skips if Postgres is
    /// unreachable.
    #[tokio::test]
    async fn list_snapshots_project_wide_postgres_one_query() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let alpha = TableName::new("alpha").unwrap();
        let beta = TableName::new("beta").unwrap();
        cat.create_table(&t, &alpha, &schema()).await.unwrap();
        cat.create_table(&t, &beta, &schema()).await.unwrap();
        cat.append_data_files(
            &t,
            &alpha,
            SnapshotId::GENESIS,
            vec![file("a1.parquet", 1, 10)],
        )
        .await
        .unwrap();
        cat.append_data_files(
            &t,
            &beta,
            SnapshotId::GENESIS,
            vec![file("b1.parquet", 1, 10)],
        )
        .await
        .unwrap();
        cat.append_data_files(&t, &alpha, SnapshotId(1), vec![file("a2.parquet", 1, 10)])
            .await
            .unwrap();

        let entries = cat.list_snapshots_project_wide(&t).await.unwrap();
        // 2 tables × 1 genesis + (2 + 1) appends = 5 rows.
        assert_eq!(entries.len(), 5, "got {entries:?}");
        // Strictly non-decreasing by committed_at — the SQL ORDER BY makes
        // this a primary correctness assertion.
        for w in entries.windows(2) {
            assert!(w[0].committed_at <= w[1].committed_at);
        }
        let alpha_count = entries.iter().filter(|e| e.table == alpha).count();
        let beta_count = entries.iter().filter(|e| e.table == beta).count();
        assert_eq!(alpha_count, 3, "alpha rows = {alpha_count}");
        assert_eq!(beta_count, 2, "beta rows = {beta_count}");
        // Genesis rows have no parent and snapshot_id 0.
        let genesis_count = entries
            .iter()
            .filter(|e| e.parent_id.is_none() && e.snapshot_id == SnapshotId::GENESIS)
            .count();
        assert_eq!(genesis_count, 2);

        // Cross-check: project-wide rollback to a cutoff after the first
        // round-of-appends rewinds both tables to id 1.
        // Re-fetch each table's first-append committed_at as the cutoff.
        let alpha_first_append = entries
            .iter()
            .find(|e| e.table == alpha && e.snapshot_id == SnapshotId(1))
            .unwrap()
            .committed_at;
        let beta_first_append = entries
            .iter()
            .find(|e| e.table == beta && e.snapshot_id == SnapshotId(1))
            .unwrap()
            .committed_at;
        let cutoff = alpha_first_append.max(beta_first_append);
        let pairs = cat
            .rollback_to_snapshot_project_wide(&t, cutoff)
            .await
            .unwrap();
        assert_eq!(pairs.len(), 2, "{pairs:?}");
        for (_table, head) in pairs {
            assert_eq!(head, SnapshotId(1));
        }
    }

    // -------------------- SQL function round-trip --------------------

    #[tokio::test]
    async fn sql_function_round_trip() {
        use crate::functions::{
            SqlArgType, SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType,
        };
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        let def = SqlFunctionDef {
            tenant: t,
            name: "double_it".into(),
            args: vec![SqlFunctionArg {
                name: "x".into(),
                data_type: SqlArgType::BigInt,
            }],
            return_type: SqlReturnType::Scalar(SqlArgType::BigInt),
            body: "SELECT x * 2".into(),
            language: SqlFunctionLanguage::Sql,
        };
        cat.register_sql_function(def.clone()).await.unwrap();
        let got = cat.lookup_sql_function(&t, "double_it").await.unwrap();
        assert_eq!(got, def);
        let listed = cat.list_sql_functions(&t).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], def);

        cat.drop_sql_function(&t, "double_it").await.unwrap();
        assert!(cat.lookup_sql_function(&t, "double_it").await.is_none());

        let err = cat.drop_sql_function(&t, "double_it").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    // -------------------- Sequence round-trip --------------------

    fn seq_def(tenant: TenantId, name: &str, start: i64, increment: i64) -> SequenceDef {
        SequenceDef {
            tenant,
            name: name.into(),
            start,
            increment,
            min_value: if increment > 0 { 1 } else { i64::MIN + 1 },
            max_value: if increment > 0 { i64::MAX } else { -1 },
            cache_size: 1,
            cycle: false,
        }
    }

    #[tokio::test]
    async fn sequence_create_lookup_drop() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        let def = seq_def(t, "s", 1, 1);
        cat.create_sequence(def.clone()).await.unwrap();
        let looked_up = cat.lookup_sequence(&t, "s").await.unwrap();
        assert_eq!(looked_up, def);

        // Duplicate create rejected with Catalog error.
        let err = cat
            .create_sequence(seq_def(t, "s", 100, 2))
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)));

        cat.drop_sequence(&t, "s").await.unwrap();
        assert!(cat.lookup_sequence(&t, "s").await.is_none());
        let err = cat.drop_sequence(&t, "s").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn sequence_nextval_currval_setval() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.create_sequence(seq_def(t, "s", 1, 1)).await.unwrap();

        // currval before any nextval is NotFound.
        let err = cat.currval(&t, "s").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));

        assert_eq!(cat.nextval(&t, "s").await.unwrap(), 1);
        assert_eq!(cat.nextval(&t, "s").await.unwrap(), 2);
        assert_eq!(cat.nextval(&t, "s").await.unwrap(), 3);
        assert_eq!(cat.currval(&t, "s").await.unwrap(), 3);

        // setval(advance=true) — next nextval returns 100+1.
        assert_eq!(cat.setval(&t, "s", 100, true).await.unwrap(), 100);
        assert_eq!(cat.nextval(&t, "s").await.unwrap(), 101);

        // setval(advance=false) — next nextval returns exactly 200.
        assert_eq!(cat.setval(&t, "s", 200, false).await.unwrap(), 200);
        assert_eq!(cat.nextval(&t, "s").await.unwrap(), 200);
    }

    /// The load-bearing durability test: persisted state must survive a
    /// catalog-handle drop and reconnect without handing out duplicate
    /// values.
    #[tokio::test]
    async fn sequence_survives_simulated_restart() {
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
                eprintln!("postgres unreachable, skipping sequence_survives_simulated_restart");
                return;
            }
        };
        let t = TenantId::new();
        cat1.create_namespace(&t).await.unwrap();
        cat1.create_sequence(seq_def(t, "s", 1, 1)).await.unwrap();
        let mut pre = Vec::new();
        for _ in 0..5 {
            pre.push(cat1.nextval(&t, "s").await.unwrap());
        }
        assert_eq!(pre, vec![1, 2, 3, 4, 5]);
        drop(cat1);

        let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema_name)
            .await
            .expect("reconnect");
        // 6th nextval continues the sequence; no duplicates.
        let n6 = cat2.nextval(&t, "s").await.unwrap();
        assert!(
            n6 > 5,
            "post-restart nextval {n6} must not duplicate any pre-restart value"
        );
        // For cache_size = 1 the next value is exactly 6 (no gap).
        assert_eq!(n6, 6);
    }

    #[tokio::test]
    async fn sequence_cross_tenant_isolation() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        cat.create_namespace(&a).await.unwrap();
        cat.create_namespace(&b).await.unwrap();
        cat.create_sequence(seq_def(a, "shared", 1, 1))
            .await
            .unwrap();

        // Tenant B can't see tenant A's sequence.
        let err = cat.nextval(&b, "shared").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
        assert!(cat.lookup_sequence(&b, "shared").await.is_none());

        // Independent advance.
        assert_eq!(cat.nextval(&a, "shared").await.unwrap(), 1);
        cat.create_sequence(seq_def(b, "shared", 100, 1))
            .await
            .unwrap();
        assert_eq!(cat.nextval(&b, "shared").await.unwrap(), 100);
        assert_eq!(cat.nextval(&a, "shared").await.unwrap(), 2);
    }

    /// Concurrent `nextval` calls must always return distinct values —
    /// row-level `FOR UPDATE` is the serialisation primitive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sequence_nextval_concurrent_no_duplicates() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let cat = Arc::new(cat);
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.create_sequence(seq_def(t, "s", 1, 1)).await.unwrap();

        let mut handles = Vec::with_capacity(10);
        for _ in 0..10 {
            let cat = cat.clone();
            handles.push(tokio::spawn(async move { cat.nextval(&t, "s").await }));
        }
        let mut values: Vec<i64> = Vec::new();
        for h in handles {
            values.push(h.await.unwrap().unwrap());
        }
        let unique: std::collections::HashSet<i64> = values.iter().copied().collect();
        assert_eq!(unique.len(), 10, "concurrent nextvals: {values:?}");
        let mut sorted = values.clone();
        sorted.sort();
        assert_eq!(sorted, (1..=10).collect::<Vec<_>>());
    }

    // -------------------- Reactor round-trip --------------------

    #[tokio::test]
    async fn reactor_register_lookup_drop() {
        use basin_common::ChangeOp;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("orders").unwrap();
        cat.create_namespace(&t).await.unwrap();
        let def = ReactorDef {
            tenant: t,
            table: tbl.clone(),
            name: "after_paid".into(),
            ops: ReactorOps::INSERT | ReactorOps::UPDATE,
            when_predicate: Some("NEW.status = 'paid'".into()),
            body: "INSERT INTO billing_events (order_id) VALUES (NEW.id)".into(),
        };
        cat.register_reactor(def.clone()).await.unwrap();

        let dup = cat.register_reactor(def.clone()).await.unwrap_err();
        assert!(matches!(dup, BasinError::Catalog(_)));

        let hits = cat.lookup_reactors_for(&t, &tbl, ChangeOp::Update).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], def);

        // DELETE op doesn't match — the bitset only enables INSERT|UPDATE.
        let no_hits = cat.lookup_reactors_for(&t, &tbl, ChangeOp::Delete).await;
        assert!(no_hits.is_empty());

        let listed = cat.list_reactors(&t).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], def);

        cat.drop_reactor(&t, &tbl, "after_paid").await.unwrap();
        let err = cat.drop_reactor(&t, &tbl, "after_paid").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
        assert!(cat.list_reactors(&t).await.is_empty());
    }

    #[tokio::test]
    async fn reactor_registration_order_preserved() {
        use basin_common::ChangeOp;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        let tbl = TableName::new("t").unwrap();
        cat.create_namespace(&t).await.unwrap();
        for n in ["one", "two", "three"] {
            let def = ReactorDef {
                tenant: t,
                table: tbl.clone(),
                name: n.into(),
                ops: ReactorOps::INSERT,
                when_predicate: None,
                body: "SELECT 1".into(),
            };
            cat.register_reactor(def).await.unwrap();
        }
        let hits = cat.lookup_reactors_for(&t, &tbl, ChangeOp::Insert).await;
        let names: Vec<&str> = hits.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["one", "two", "three"]);
    }

    #[tokio::test]
    async fn reactor_cross_tenant_isolation() {
        use basin_common::ChangeOp;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        let tbl = TableName::new("t").unwrap();
        cat.create_namespace(&a).await.unwrap();
        cat.create_namespace(&b).await.unwrap();
        let def = ReactorDef {
            tenant: a,
            table: tbl.clone(),
            name: "r".into(),
            ops: ReactorOps::INSERT,
            when_predicate: None,
            body: "SELECT 1".into(),
        };
        cat.register_reactor(def).await.unwrap();
        // Tenant B sees nothing.
        assert!(cat
            .lookup_reactors_for(&b, &tbl, ChangeOp::Insert)
            .await
            .is_empty());
        assert!(cat.list_reactors(&b).await.is_empty());
        // Tenant B can register its own reactor with the same (table, name).
        let def_b = ReactorDef {
            tenant: b,
            table: tbl.clone(),
            name: "r".into(),
            ops: ReactorOps::INSERT,
            when_predicate: None,
            body: "SELECT 2".into(),
        };
        cat.register_reactor(def_b).await.unwrap();
        let a_hits = cat.lookup_reactors_for(&a, &tbl, ChangeOp::Insert).await;
        let b_hits = cat.lookup_reactors_for(&b, &tbl, ChangeOp::Insert).await;
        assert_eq!(a_hits.len(), 1);
        assert_eq!(b_hits.len(), 1);
        assert_ne!(a_hits[0].body, b_hits[0].body);
    }

    /// Reactors must survive a catalog handle drop.
    #[tokio::test]
    async fn reactor_survives_simulated_restart() {
        use basin_common::ChangeOp;
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
                eprintln!("postgres unreachable, skipping reactor_survives_simulated_restart");
                return;
            }
        };
        let t = TenantId::new();
        let tbl = TableName::new("t").unwrap();
        cat1.create_namespace(&t).await.unwrap();
        let def = ReactorDef {
            tenant: t,
            table: tbl.clone(),
            name: "r".into(),
            ops: ReactorOps::INSERT,
            when_predicate: None,
            body: "SELECT 1".into(),
        };
        cat1.register_reactor(def.clone()).await.unwrap();
        drop(cat1);

        let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema_name)
            .await
            .expect("reconnect");
        let hits = cat2.lookup_reactors_for(&t, &tbl, ChangeOp::Insert).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], def);
    }

    // -------------------- Enum / domain round-trip --------------------

    fn enum_def(t: TenantId, name: &str, labels: &[&str]) -> EnumTypeDef {
        EnumTypeDef {
            tenant: t,
            name: name.into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn domain_def(
        t: TenantId,
        name: &str,
        base: crate::functions::SqlArgType,
        check: Option<&str>,
    ) -> DomainDef {
        DomainDef {
            tenant: t,
            name: name.into(),
            base_type: base,
            check_predicate: check.map(|s| s.to_string()),
        }
    }

    #[tokio::test]
    async fn enum_type_round_trip() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.register_enum_type(enum_def(t, "color", &["red", "green", "blue"]))
            .await
            .unwrap();
        let looked_up = cat.lookup_enum_type(&t, "color").await.unwrap();
        assert_eq!(looked_up.labels, vec!["red", "green", "blue"]);

        cat.add_enum_value(&t, "color", "yellow").await.unwrap();
        let after = cat.lookup_enum_type(&t, "color").await.unwrap();
        assert_eq!(after.labels, vec!["red", "green", "blue", "yellow"]);

        let listed = cat.list_enum_types(&t).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "color");

        cat.drop_enum_type(&t, "color").await.unwrap();
        assert!(cat.lookup_enum_type(&t, "color").await.is_none());

        let err = cat.drop_enum_type(&t, "color").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn enum_value_uniqueness_enforced() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();

        // Duplicate label at registration time is rejected before persisting.
        let err = cat
            .register_enum_type(enum_def(t, "x", &["a", "b", "a"]))
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
        assert!(cat.lookup_enum_type(&t, "x").await.is_none());

        // Duplicate add-value is rejected with a Catalog error.
        cat.register_enum_type(enum_def(t, "y", &["a", "b"]))
            .await
            .unwrap();
        let err = cat.add_enum_value(&t, "y", "a").await.unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
        let after = cat.lookup_enum_type(&t, "y").await.unwrap();
        assert_eq!(after.labels, vec!["a", "b"]);
    }

    /// Concurrent `add_enum_value` calls must serialise via the row lock —
    /// final label list contains both new values exactly once and ordering
    /// is deterministic (append order matches commit order).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn enum_add_value_atomic() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let cat = Arc::new(cat);
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.register_enum_type(enum_def(t, "e", &["base"]))
            .await
            .unwrap();

        let mut handles = Vec::with_capacity(8);
        for i in 0..8 {
            let cat = cat.clone();
            let label = format!("v{i}");
            handles.push(tokio::spawn(async move {
                cat.add_enum_value(&t, "e", &label).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        let after = cat.lookup_enum_type(&t, "e").await.unwrap();
        // Exactly the original label plus all 8 additions, no dupes lost
        // and no value missing.
        assert_eq!(after.labels.len(), 9);
        assert_eq!(after.labels[0], "base");
        let mut tail = after.labels[1..].to_vec();
        tail.sort();
        let mut expected: Vec<String> = (0..8).map(|i| format!("v{i}")).collect();
        expected.sort();
        assert_eq!(tail, expected);
    }

    #[tokio::test]
    async fn domain_round_trip() {
        use crate::functions::SqlArgType;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.register_domain(domain_def(
            t,
            "positive_int",
            SqlArgType::Int,
            Some("VALUE > 0"),
        ))
        .await
        .unwrap();
        let looked_up = cat.lookup_domain(&t, "positive_int").await.unwrap();
        assert_eq!(looked_up.base_type, SqlArgType::Int);
        assert_eq!(looked_up.check_predicate.as_deref(), Some("VALUE > 0"));

        let listed = cat.list_domains(&t).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "positive_int");

        cat.drop_domain(&t, "positive_int").await.unwrap();
        assert!(cat.lookup_domain(&t, "positive_int").await.is_none());

        let err = cat.drop_domain(&t, "positive_int").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    /// The load-bearing durability test: persisted enum state must
    /// survive a catalog-handle drop and reconnect.
    #[tokio::test]
    async fn enum_survives_simulated_restart() {
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
                eprintln!("postgres unreachable, skipping enum_survives_simulated_restart");
                return;
            }
        };
        let t = TenantId::new();
        cat1.create_namespace(&t).await.unwrap();
        cat1.register_enum_type(enum_def(t, "color", &["red", "green"]))
            .await
            .unwrap();
        cat1.add_enum_value(&t, "color", "blue").await.unwrap();
        drop(cat1);

        let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema_name)
            .await
            .expect("reconnect");
        let after = cat2.lookup_enum_type(&t, "color").await.unwrap();
        assert_eq!(after.labels, vec!["red", "green", "blue"]);
    }

    #[tokio::test]
    async fn enum_cross_tenant_isolation() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        cat.create_namespace(&a).await.unwrap();
        cat.create_namespace(&b).await.unwrap();
        cat.register_enum_type(enum_def(a, "shared", &["x", "y"]))
            .await
            .unwrap();
        // Tenant B sees nothing.
        assert!(cat.lookup_enum_type(&b, "shared").await.is_none());
        assert!(cat.list_enum_types(&b).await.is_empty());
        // Tenant B can register the same name independently.
        cat.register_enum_type(enum_def(b, "shared", &["one"]))
            .await
            .unwrap();
        let a_def = cat.lookup_enum_type(&a, "shared").await.unwrap();
        let b_def = cat.lookup_enum_type(&b, "shared").await.unwrap();
        assert_eq!(a_def.labels, vec!["x", "y"]);
        assert_eq!(b_def.labels, vec!["one"]);
    }

    #[tokio::test]
    async fn domain_cross_tenant_isolation() {
        use crate::functions::SqlArgType;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        cat.create_namespace(&a).await.unwrap();
        cat.create_namespace(&b).await.unwrap();
        cat.register_domain(domain_def(a, "d", SqlArgType::Int, Some("VALUE > 0")))
            .await
            .unwrap();
        assert!(cat.lookup_domain(&b, "d").await.is_none());
        assert!(cat.list_domains(&b).await.is_empty());

        cat.register_domain(domain_def(b, "d", SqlArgType::Text, None))
            .await
            .unwrap();
        let a_def = cat.lookup_domain(&a, "d").await.unwrap();
        let b_def = cat.lookup_domain(&b, "d").await.unwrap();
        assert_eq!(a_def.base_type, SqlArgType::Int);
        assert_eq!(b_def.base_type, SqlArgType::Text);
    }

    #[tokio::test]
    async fn drop_blocked_when_referenced() {
        use std::collections::HashMap;
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.register_enum_type(enum_def(t, "status", &["a", "b"]))
            .await
            .unwrap();

        // Build a table whose `status` column carries the
        // `BASIN_ENUM_TYPE` metadata marker so the catalog refcount sees
        // it as a referencer.
        let mut md = HashMap::new();
        md.insert(BASIN_ENUM_TYPE_KEY.to_string(), "status".to_string());
        let f1 = Field::new("id", DataType::Int64, false);
        let f2 = Field::new("status", DataType::Utf8, false).with_metadata(md);
        let referencing_schema = Schema::new(vec![f1, f2]);
        let tbl = TableName::new("orders").unwrap();
        cat.create_table(&t, &tbl, &referencing_schema)
            .await
            .unwrap();

        let err = cat.drop_enum_type(&t, "status").await.unwrap_err();
        assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(
            msg.contains("orders.status") || msg.contains("references"),
            "error should mention the column, got: {err}"
        );

        // After we drop the table the type can be removed.
        cat.drop_table(&t, &tbl).await.unwrap();
        cat.drop_enum_type(&t, "status").await.unwrap();
    }

    // -------------------- Procedure round-trip --------------------

    fn proc_def(t: TenantId, name: &str, body: &str) -> SqlProcedureDef {
        SqlProcedureDef {
            tenant: t,
            name: name.into(),
            args: Vec::new(),
            body: body.into(),
        }
    }

    #[tokio::test]
    async fn procedure_round_trip() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        let def = proc_def(t, "p", "INSERT INTO log VALUES ('hi')");
        cat.register_procedure(def.clone()).await.unwrap();
        let got = cat.lookup_procedure(&t, "p").await.unwrap();
        assert_eq!(got, def);
        let listed = cat.list_procedures(&t).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], def);

        // Re-registering the same (tenant, name) is rejected.
        let dup = cat.register_procedure(def.clone()).await.unwrap_err();
        assert!(matches!(dup, BasinError::Catalog(_)), "got {dup:?}");

        cat.drop_procedure(&t, "p").await.unwrap();
        assert!(cat.lookup_procedure(&t, "p").await.is_none());

        let err = cat.drop_procedure(&t, "p").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[tokio::test]
    async fn procedure_args_json_round_trips() {
        use crate::functions::{SqlArgType, SqlFunctionArg};
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        let def = SqlProcedureDef {
            tenant: t,
            name: "archive".into(),
            args: vec![
                SqlFunctionArg {
                    name: "tid".into(),
                    data_type: SqlArgType::Text,
                },
                SqlFunctionArg {
                    name: "cutoff".into(),
                    data_type: SqlArgType::TimestampTz,
                },
                SqlFunctionArg {
                    name: "n".into(),
                    data_type: SqlArgType::BigInt,
                },
                SqlFunctionArg {
                    name: "active".into(),
                    data_type: SqlArgType::Boolean,
                },
            ],
            body: "INSERT INTO archive SELECT * FROM events WHERE id = n; \
                   DELETE FROM events WHERE id = n"
                .into(),
        };
        cat.register_procedure(def.clone()).await.unwrap();
        let got = cat.lookup_procedure(&t, "archive").await.unwrap();
        assert_eq!(got, def);
        assert_eq!(got.args.len(), 4);
        assert_eq!(got.args[0].data_type, SqlArgType::Text);
        assert_eq!(got.args[1].data_type, SqlArgType::TimestampTz);
        assert_eq!(got.args[2].data_type, SqlArgType::BigInt);
        assert_eq!(got.args[3].data_type, SqlArgType::Boolean);
    }

    /// The load-bearing durability test: persisted procedure state must
    /// survive a catalog-handle drop and reconnect.
    #[tokio::test]
    async fn procedure_survives_simulated_restart() {
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
                eprintln!("postgres unreachable, skipping procedure_survives_simulated_restart");
                return;
            }
        };
        let t = TenantId::new();
        cat1.create_namespace(&t).await.unwrap();
        let def = proc_def(
            t,
            "rotate",
            "INSERT INTO archive SELECT * FROM events; DELETE FROM events",
        );
        cat1.register_procedure(def.clone()).await.unwrap();
        drop(cat1);

        let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema_name)
            .await
            .expect("reconnect");
        let after = cat2.lookup_procedure(&t, "rotate").await.unwrap();
        assert_eq!(after, def);
    }

    #[tokio::test]
    async fn procedure_cross_tenant_isolation() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
        cat.create_namespace(&a).await.unwrap();
        cat.create_namespace(&b).await.unwrap();
        cat.register_procedure(proc_def(a, "shared", "INSERT INTO log VALUES ('a')"))
            .await
            .unwrap();
        // Tenant B sees nothing.
        assert!(cat.lookup_procedure(&b, "shared").await.is_none());
        assert!(cat.list_procedures(&b).await.is_empty());
        // Tenant B can register the same name independently.
        cat.register_procedure(proc_def(b, "shared", "INSERT INTO log VALUES ('b')"))
            .await
            .unwrap();
        let a_def = cat.lookup_procedure(&a, "shared").await.unwrap();
        let b_def = cat.lookup_procedure(&b, "shared").await.unwrap();
        assert_eq!(a_def.body, "INSERT INTO log VALUES ('a')");
        assert_eq!(b_def.body, "INSERT INTO log VALUES ('b')");
        // A's listing only shows A's row.
        let listed_a = cat.list_procedures(&a).await;
        assert_eq!(listed_a.len(), 1);
        assert_eq!(listed_a[0].tenant, a);
    }

    #[tokio::test]
    async fn procedure_drop_namespace_cleans_table() {
        let Some((cat, _guard)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
        cat.create_namespace(&t).await.unwrap();
        cat.register_procedure(proc_def(t, "p1", "INSERT INTO log VALUES ('x')"))
            .await
            .unwrap();
        cat.register_procedure(proc_def(t, "p2", "DELETE FROM log"))
            .await
            .unwrap();
        assert_eq!(cat.list_procedures(&t).await.len(), 2);

        cat.drop_namespace(&t).await.unwrap();
        assert!(cat.list_procedures(&t).await.is_empty());
        assert!(cat.lookup_procedure(&t, "p1").await.is_none());
        assert!(cat.lookup_procedure(&t, "p2").await.is_none());
    }
}
