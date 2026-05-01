//! `basin-engine` — single-process SQL execution engine for the Basin PoC.
//!
//! This is the layer that, in a production deployment, would split across
//! shard owners, the placement service, and the analytical pool. For the
//! local PoC we collapse all of that into one in-process [`Engine`] that:
//!
//! 1. Holds a `basin_storage::Storage` (the Parquet substrate) and a
//!    `basin_catalog::Catalog` (table metadata + snapshots).
//! 2. Hands out per-tenant [`TenantSession`]s. Each session is the only API
//!    surface a router or test should program against.
//! 3. Compiles and executes SQL via DataFusion against the calling tenant's
//!    namespace. Tenant isolation is structural: there is no API path on
//!    [`TenantSession`] that exposes another tenant's data.
//!
//! The module-level types declared here are the *public contract* the router
//! depends on. Their bodies are filled in by [`Engine::new`] /
//! [`TenantSession::execute`].
//!
//! ## Supported SQL (PoC scope)
//!
//! - `CREATE TABLE name (col TYPE, ...)`
//! - `INSERT INTO name VALUES (...), (...)` — multi-row literal inserts
//! - `SELECT ... FROM name [WHERE simple-predicate]`
//! - `SHOW TABLES`
//!
//! ## Prepared statements (extended-query support)
//!
//! [`TenantSession::prepare`] / [`TenantSession::bind`] /
//! [`TenantSession::execute_bound`] back the Postgres extended-query path the
//! router speaks. The v1 implementation parses `$N` placeholders out of the
//! SQL, substitutes literal values at bind time, and re-runs the simple-query
//! pipeline. This unblocks every `tokio-postgres` / `asyncpg` / JDBC client
//! that defaults to `Parse`/`Bind`/`Execute`. Plan caching is deliberately
//! deferred to v2.
//!
//! Out of scope for the PoC: `UPDATE`, `DELETE`, transactions, JOINs across
//! tables, foreign keys.

#![forbid(unsafe_code)]

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_common::{Result, TenantId};

/// Engine configuration.
///
/// `storage` and `catalog` are shared across all tenant sessions; per-tenant
/// scoping happens *inside* the engine, not by handing out per-tenant
/// instances.
///
/// `shard` is optional. When `Some`, INSERTs route through the shard owner's
/// WAL-acked write path, and SELECTs trigger a synchronous compaction beforehand
/// so the Parquet base reflects the in-RAM tail (Option A in the design notes).
/// When `None`, the engine falls back to its legacy synchronous Parquet write
/// path — kept for tests that haven't yet been migrated to construct a shard.
#[derive(Clone)]
pub struct EngineConfig {
    pub storage: basin_storage::Storage,
    pub catalog: Arc<dyn basin_catalog::Catalog>,
    pub shard: Option<basin_shard::Shard>,
}

/// The Basin SQL engine. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

pub(crate) struct EngineInner {
    pub(crate) cfg: EngineConfig,
    // additional state (DataFusion runtime, per-tenant context cache) lives
    // in the implementation module; intentionally not exposed here.
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        Self {
            inner: Arc::new(EngineInner { cfg }),
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Open a session bound to `tenant`. The catalog namespace is created on
    /// demand if it does not yet exist.
    pub async fn open_session(&self, tenant: TenantId) -> Result<TenantSession> {
        crate::session::open(self.clone(), tenant).await
    }
}

/// A handle to the engine scoped to a single tenant. All [`execute`] calls
/// run as this tenant; there is no reset / impersonate API by design.
///
/// [`execute`]: TenantSession::execute
pub struct TenantSession {
    pub(crate) engine: Engine,
    pub(crate) tenant: TenantId,
    pub(crate) ctx: datafusion::prelude::SessionContext,
    pub(crate) state: Arc<crate::session::SessionState>,
}

impl TenantSession {
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// Run one SQL statement. Returns either a result set ([`ExecResult::Rows`])
    /// or a side-effect tag for DML/DDL ([`ExecResult::Empty`]).
    #[tracing::instrument(skip(self, sql), fields(tenant=%self.tenant, sql=%sql.lines().next().unwrap_or("")))]
    pub async fn execute(&self, sql: &str) -> Result<ExecResult> {
        crate::executor::execute(self, sql).await
    }

    /// Prepare an SQL statement with `$N`-style parameter placeholders. Returns
    /// a [`StatementHandle`] the caller can later [`bind`](Self::bind) and a
    /// [`StatementSchema`] describing parameter and result-column types as
    /// best they can be inferred up front (unknowns default to TEXT).
    #[tracing::instrument(skip(self, sql), fields(tenant=%self.tenant, sql=%sql.lines().next().unwrap_or("")))]
    pub async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)> {
        crate::prepared::prepare(self, sql).await
    }

    /// Bind concrete values to a previously [`prepare`](Self::prepare)d
    /// statement. The number of `params` must equal the placeholder count.
    pub async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement> {
        crate::prepared::bind(self, handle, params).await
    }

    /// Execute a previously [`bind`](Self::bind)-produced statement. Output
    /// shape matches [`execute`](Self::execute).
    pub async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
        crate::prepared::execute_bound(self, bound).await
    }

    /// Return the schema cached at [`prepare`](Self::prepare) time.
    pub async fn describe_statement(
        &self,
        handle: &StatementHandle,
    ) -> Result<StatementSchema> {
        crate::prepared::describe_statement(self, handle).await
    }

    /// Forget a prepared statement. Idempotent.
    pub async fn close_statement(&self, handle: &StatementHandle) {
        crate::prepared::close_statement(self, handle).await
    }
}

pub use crate::prepared::{BoundStatement, ScalarParam, StatementHandle, StatementSchema};

/// Outcome of one SQL statement.
#[derive(Debug)]
pub enum ExecResult {
    /// DDL or DML succeeded with no result set. `tag` is the Postgres-style
    /// command tag (`"CREATE TABLE"`, `"INSERT 0 3"`, etc.) the router will
    /// surface back to the client.
    Empty { tag: String },

    /// A result set. `schema` is the projected schema; `batches` is the
    /// (possibly streamed-and-collected) Arrow batches in projection order.
    Rows {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    },
}

mod convert;
mod ddl;
mod dml;
mod executor;
mod fast_select;
mod prepared;
mod session;
mod types;
mod udf;
mod vector_search;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, StringArray};
    use basin_catalog::InMemoryCatalog;
    use basin_common::TenantId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;

    fn engine_in(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Pull the column `name` out of the first batch as i64 values.
    fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
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

    fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn create_then_select_empty() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        let res = sess
            .execute("CREATE TABLE users (id BIGINT NOT NULL, name TEXT)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "CREATE TABLE"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id, name FROM users").await.unwrap();
        match res {
            ExecResult::Rows { schema, batches } => {
                assert_eq!(schema.fields().len(), 2);
                assert_eq!(schema.field(0).name(), "id");
                assert_eq!(schema.field(1).name(), "name");
                assert_eq!(total_rows(&batches), 0);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn insert_then_select_returns_rows() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        let res = sess
            .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 3"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id, name FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 3);
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);
                assert_eq!(
                    col_string(&batches, "name"),
                    vec!["a".to_string(), "b".to_string(), "c".to_string()]
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn insert_two_batches_select_returns_all() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t VALUES (1), (2)").await.unwrap();
        sess.execute("INSERT INTO t VALUES (3), (4), (5)")
            .await
            .unwrap();

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 5);
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn select_with_where_filter() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        sess.execute(
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
        )
        .await
        .unwrap();

        let res = sess.execute("SELECT id, name FROM t WHERE id = 3").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                assert_eq!(col_i64(&batches, "id"), vec![3]);
                assert_eq!(col_string(&batches, "name"), vec!["c".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn tenant_isolation_engine_level() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let sa = eng.open_session(a).await.unwrap();
        let sb = eng.open_session(b).await.unwrap();

        for s in [&sa, &sb] {
            s.execute("CREATE TABLE shared (id BIGINT NOT NULL, who TEXT NOT NULL)")
                .await
                .unwrap();
        }
        sa.execute("INSERT INTO shared VALUES (1, 'A1'), (2, 'A2')")
            .await
            .unwrap();
        sb.execute("INSERT INTO shared VALUES (10, 'B1'), (20, 'B2'), (30, 'B3')")
            .await
            .unwrap();

        let ra = sa.execute("SELECT id, who FROM shared ORDER BY id").await.unwrap();
        let rb = sb.execute("SELECT id, who FROM shared ORDER BY id").await.unwrap();
        let (ba, bb) = match (ra, rb) {
            (ExecResult::Rows { batches: ba, .. }, ExecResult::Rows { batches: bb, .. }) => (ba, bb),
            _ => panic!("expected rows"),
        };
        assert_eq!(col_i64(&ba, "id"), vec![1, 2]);
        assert_eq!(col_i64(&bb, "id"), vec![10, 20, 30]);
        for w in col_string(&ba, "who") {
            assert!(w.starts_with('A'), "leaked: {w}");
        }
        for w in col_string(&bb, "who") {
            assert!(w.starts_with('B'), "leaked: {w}");
        }
    }

    #[tokio::test]
    async fn show_tables_after_create() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE alpha (id BIGINT)").await.unwrap();
        sess.execute("CREATE TABLE beta (id BIGINT)").await.unwrap();

        let res = sess.execute("SHOW TABLES").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let mut names = col_string(&batches, "table_name");
                names.sort();
                assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn prepare_bind_execute_roundtrip() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
            .await
            .unwrap();

        // Prepare an INSERT with two parameters.
        let (insert_h, insert_schema) = sess
            .prepare("INSERT INTO events VALUES ($1, $2)")
            .await
            .unwrap();
        assert_eq!(insert_schema.param_types.len(), 2);
        assert!(insert_schema.columns.is_empty());

        for (i, body) in [(1i64, "first"), (2, "second"), (3, "third")] {
            let bound = sess
                .bind(
                    &insert_h,
                    vec![ScalarParam::Int8(i), ScalarParam::Text(body.into())],
                )
                .await
                .unwrap();
            sess.execute_bound(bound).await.unwrap();
        }

        // Prepare a SELECT and execute against id=2.
        let (select_h, _) = sess
            .prepare("SELECT id, body FROM events WHERE id = $1")
            .await
            .unwrap();
        let bound = sess
            .bind(&select_h, vec![ScalarParam::Int8(2)])
            .await
            .unwrap();
        let res = sess.execute_bound(bound).await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                assert_eq!(col_i64(&batches, "id"), vec![2]);
                assert_eq!(col_string(&batches, "body"), vec!["second".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        sess.close_statement(&insert_h).await;
        sess.close_statement(&select_h).await;
        // close is idempotent.
        sess.close_statement(&insert_h).await;
    }

    #[tokio::test]
    async fn prepare_infers_param_type_from_where_predicate() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, body TEXT NOT NULL)")
            .await
            .unwrap();

        let (_, schema) = sess
            .prepare("SELECT id, body FROM t WHERE id = $1")
            .await
            .unwrap();
        assert_eq!(schema.param_types.len(), 1);
        assert_eq!(
            schema.param_types[0],
            arrow_schema::DataType::Int64,
            "expected param to infer to BIGINT/Int64"
        );
        // SELECT plans cleanly so we should also have a real column list.
        assert_eq!(schema.columns.len(), 2);
    }

    #[tokio::test]
    async fn prepare_infers_param_types_from_insert_values() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, body TEXT NOT NULL)")
            .await
            .unwrap();

        let (_, schema) = sess
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .await
            .unwrap();
        assert_eq!(schema.param_types.len(), 2);
        assert_eq!(schema.param_types[0], arrow_schema::DataType::Int64);
        assert_eq!(schema.param_types[1], arrow_schema::DataType::Utf8);
        // INSERT yields no rows, so columns is empty.
        assert!(schema.columns.is_empty());
    }

    #[tokio::test]
    async fn create_table_invalid_type_returns_error() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        let err = sess
            .execute("CREATE TABLE bad (ts TIMESTAMP)")
            .await
            .unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::InvalidSchema(_)),
            "got {err:?}"
        );
    }
}
