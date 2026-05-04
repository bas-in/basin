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
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Optional analytical (DuckDB) backend. When `Some`, the executor's
    /// router checks `analytical_route::is_analytical` before each statement
    /// and forwards qualifying queries here. Wired in via
    /// [`Engine::with_analytical`] so existing `EngineConfig` literals across
    /// the workspace stay byte-stable.
    pub(crate) analytical: Option<basin_analytical::AnalyticalEngine>,
    /// Counter incremented every time a statement is successfully served by
    /// the analytical path. Exposed via [`Engine::analytical_routing_count`]
    /// so integration tests can assert that routing actually happened
    /// without resorting to log scraping.
    pub(crate) analytical_routing_count: AtomicU64,
    /// Per-tenant noisy-tenant detector. Reads its bit when a session is
    /// opened (to choose `target_partitions`) and bumps it after every
    /// successful `TenantSession::execute`. See `noisy_detector` module
    /// for the full rationale.
    pub(crate) noisy_detector: crate::noisy_detector::NoisyDetector,
    // additional state (DataFusion runtime, per-tenant context cache) lives
    // in the implementation module; intentionally not exposed here.
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                cfg,
                analytical: None,
                analytical_routing_count: AtomicU64::new(0),
                noisy_detector: crate::noisy_detector::NoisyDetector::new(),
            }),
        }
    }

    /// Attach an [`AnalyticalEngine`](basin_analytical::AnalyticalEngine) to
    /// this engine. Returns a new `Engine` (cheap; only the inner `Arc` is
    /// rebuilt) so callers can keep the original analytical-less engine
    /// around if they need it.
    ///
    /// When attached, the router's [`is_analytical`](crate::analytical_route)
    /// heuristic decides per statement whether the analytical path is taken;
    /// any failure on that path falls back to the OLTP DataFusion engine
    /// transparently. See `analytical_route` and `executor::execute` for
    /// the heuristic and the fallback contract.
    pub fn with_analytical(self, analytical: basin_analytical::AnalyticalEngine) -> Self {
        let cfg = self.inner.cfg.clone();
        Self {
            inner: Arc::new(EngineInner {
                cfg,
                analytical: Some(analytical),
                analytical_routing_count: AtomicU64::new(0),
                noisy_detector: crate::noisy_detector::NoisyDetector::new(),
            }),
        }
    }

    /// O(1) lookup: is `tenant`'s recent query rate above the noisy
    /// threshold? Returns `false` for tenants we've never seen. See
    /// [`crate::noisy_detector`] for the threshold and decay constants.
    ///
    /// This is a *hint* the engine consumes when constructing a session
    /// (to downshift `target_partitions`); it is also intended to be read
    /// by the `basin-storage` fair-share scheduler to demote a tenant's
    /// I/O priority. It is not a hard cap.
    pub fn is_noisy(&self, tenant: &TenantId) -> bool {
        self.inner.noisy_detector.is_noisy(tenant)
    }

    /// Crate-private: bump this tenant's query-rate counter. Called from
    /// `TenantSession::execute` after the statement completes (successfully
    /// or not — every attempt counts toward the rate, since failed queries
    /// still consumed I/O budget).
    pub(crate) fn record_query(&self, tenant: &TenantId) {
        self.inner.noisy_detector.record_query(tenant);
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Optional analytical engine attached via [`Engine::with_analytical`].
    pub(crate) fn analytical(&self) -> Option<&basin_analytical::AnalyticalEngine> {
        self.inner.analytical.as_ref()
    }

    /// Internal hook used by the executor to record successful analytical
    /// routings. Crate-private so external callers can't tamper with the
    /// counter (only [`Engine::analytical_routing_count`] is exposed).
    pub(crate) fn note_analytical_routed(&self) {
        self.inner.analytical_routing_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of statements successfully routed to the analytical path
    /// since this `Engine` was built. Test-only instrumentation; production
    /// code should rely on tracing for routing visibility.
    pub fn analytical_routing_count(&self) -> u64 {
        self.inner.analytical_routing_count.load(Ordering::Relaxed)
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
        let result = crate::executor::execute(self, sql).await;
        // Bump the noisy-tenant rate estimator regardless of success: a
        // failed query still consumed I/O permits + planner time, which is
        // exactly what the detector is meant to throttle. O(1).
        self.engine.record_query(&self.tenant);
        result
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

mod analytical_route;
mod convert;
mod ddl;
mod dml;
mod dml_mutate;
mod executor;
mod fast_select;
mod noisy_detector;
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

    // --- UPDATE / DELETE -------------------------------------------------------

    async fn seed_five_rows(sess: &TenantSession) {
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
        sess.execute(
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn delete_removes_matching_rows() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess.execute("DELETE FROM t WHERE id = 3").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 4);
                let ids = col_i64(&batches, "id");
                assert!(!ids.contains(&3), "id 3 still present: {ids:?}");
                assert_eq!(ids, vec![1, 2, 4, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_with_no_matches_is_noop() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        // Snapshot before to make sure no new file gets written.
        let cat = eng.config().catalog.clone();
        let table = basin_common::TableName::new("t").unwrap();
        let before = cat.load_table(&sess.tenant(), &table).await.unwrap();

        let res = sess.execute("DELETE FROM t WHERE id = 999").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 0"),
            other => panic!("unexpected: {other:?}"),
        }

        let after = cat.load_table(&sess.tenant(), &table).await.unwrap();
        assert_eq!(
            before.current_snapshot, after.current_snapshot,
            "no-op DELETE must not advance snapshot"
        );
    }

    #[tokio::test]
    async fn delete_all_rows_drops_files() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        // `WHERE id < 1000` matches every row in the table.
        let res = sess.execute("DELETE FROM t WHERE id < 1000").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 5"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => assert_eq!(total_rows(&batches), 0),
            other => panic!("unexpected: {other:?}"),
        }

        // The catalog snapshot must have recorded the removal.
        let table = basin_common::TableName::new("t").unwrap();
        let head = eng
            .config()
            .catalog
            .load_table(&sess.tenant(), &table)
            .await
            .unwrap();
        let cur = head.current().unwrap();
        assert_eq!(
            cur.summary.operation,
            basin_catalog::SnapshotOperation::Replace
        );
        assert_eq!(cur.summary.removed_files, 1);
    }

    #[tokio::test]
    async fn update_changes_matching_rows() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("UPDATE t SET name = 'X' WHERE id = 2")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess
            .execute("SELECT name FROM t WHERE id = 2")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                assert_eq!(col_string(&batches, "name"), vec!["X".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_does_not_touch_other_rows() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        sess.execute("UPDATE t SET name = 'X' WHERE id = 2")
            .await
            .unwrap();

        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 5);
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4, 5]);
                let names = col_string(&batches, "name");
                assert_eq!(names[0], "a");
                assert_eq!(names[1], "X");
                assert_eq!(names[2], "c");
                assert_eq!(names[3], "d");
                assert_eq!(names[4], "e");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_with_no_matches_is_noop() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let cat = eng.config().catalog.clone();
        let table = basin_common::TableName::new("t").unwrap();
        let before = cat.load_table(&sess.tenant(), &table).await.unwrap();

        let res = sess
            .execute("UPDATE t SET name = 'X' WHERE id = 999")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 0"),
            other => panic!("unexpected: {other:?}"),
        }

        let after = cat.load_table(&sess.tenant(), &table).await.unwrap();
        assert_eq!(before.current_snapshot, after.current_snapshot);
    }

    #[tokio::test]
    async fn delete_then_select_through_engine() {
        // Distinguishing test: the round trip exercises the same `execute`
        // entry point a router would call, including the post-commit refresh
        // that puts the new file set in front of DataFusion's listing table.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        sess.execute("DELETE FROM t WHERE id = 1").await.unwrap();
        sess.execute("DELETE FROM t WHERE id = 5").await.unwrap();

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![2, 3, 4]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_via_prepared_statement() {
        // Distinguishing test: prepared-statement support has its own
        // placeholder substitution path; UPDATE has to flow through it
        // identically to INSERT/SELECT for ORM compatibility.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let (h, schema) = sess
            .prepare("UPDATE t SET name = $1 WHERE id = $2")
            .await
            .unwrap();
        assert_eq!(schema.param_types.len(), 2);
        assert_eq!(schema.param_types[0], arrow_schema::DataType::Utf8);
        assert_eq!(schema.param_types[1], arrow_schema::DataType::Int64);

        let bound = sess
            .bind(
                &h,
                vec![ScalarParam::Text("Y".into()), ScalarParam::Int8(4)],
            )
            .await
            .unwrap();
        let res = sess.execute_bound(bound).await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // --- Compound WHERE: AND / OR / IS NULL / IN ----------------------------

    #[tokio::test]
    async fn update_with_and_predicate() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("UPDATE t SET name = 'Z' WHERE id > 2 AND name = 'd'")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id, name FROM t ORDER BY id").await.unwrap();
        let names = match res {
            ExecResult::Rows { batches, .. } => col_string(&batches, "name"),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(names, vec!["a", "b", "c", "Z", "e"]);
    }

    #[tokio::test]
    async fn update_with_or_predicate() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("UPDATE t SET name = 'X' WHERE id = 1 OR id = 2")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 2"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id, name FROM t ORDER BY id").await.unwrap();
        let names = match res {
            ExecResult::Rows { batches, .. } => col_string(&batches, "name"),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(names, vec!["X", "X", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn delete_with_in_predicate() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("DELETE FROM t WHERE id IN (1, 2, 3)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 3"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![4, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_with_is_null() {
        // Seed a table with explicit NULLs in `name`. seed_five_rows uses
        // NOT NULL columns, so we build our own here.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT)")
            .await
            .unwrap();
        sess.execute(
            "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL), (5, 'e')",
        )
        .await
        .unwrap();

        let res = sess.execute("DELETE FROM t WHERE name IS NULL").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 2"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 3, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn delete_with_compound_rejects_unsupported() {
        // A function-call WHERE isn't representable in our predicate
        // language; the engine must surface a clean InvalidSchema error
        // rather than a partial DELETE.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let err = sess
            .execute("DELETE FROM t WHERE upper(name) = 'A'")
            .await
            .unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::InvalidSchema(_)),
            "got {err:?}"
        );

        // The table contents must be untouched after the rejection.
        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_with_lte_gte_predicates() {
        // `<=` / `>=` synthesise to `Lt OR Eq` / `Gt OR Eq` in the
        // compound predicate. Make sure the synthesis matches semantics.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("UPDATE t SET name = 'Q' WHERE id <= 2")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 2"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess
            .execute("UPDATE t SET name = 'R' WHERE id >= 5")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id, name FROM t ORDER BY id").await.unwrap();
        let names = match res {
            ExecResult::Rows { batches, .. } => col_string(&batches, "name"),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(names, vec!["Q", "Q", "c", "d", "R"]);
    }

    #[tokio::test]
    async fn update_with_and_prunes_file() {
        // Two files, two non-overlapping id ranges. The AND has one branch
        // that fits one file's range; the file outside that range must be
        // skipped entirely (its DataFileRef carries through to the new
        // snapshot unchanged).
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();

        // File 1: ids 0..1000 with name='other'.
        let mut s1 = String::from("INSERT INTO t VALUES ");
        for i in 0..1000 {
            if i > 0 {
                s1.push(',');
            }
            s1.push_str(&format!("({i}, 'other')"));
        }
        sess.execute(&s1).await.unwrap();

        // File 2: ids 999_000..999_500 — same row mostly, plus one with
        // name='foo' so the matched count is 1.
        let mut s2 = String::from("INSERT INTO t VALUES ");
        for i in 999_000..999_500 {
            if i > 999_000 {
                s2.push(',');
            }
            let n = if i == 999_100 { "foo" } else { "other" };
            s2.push_str(&format!("({i}, '{n}')"));
        }
        sess.execute(&s2).await.unwrap();

        let table = basin_common::TableName::new("t").unwrap();
        let storage = eng.config().storage.clone();
        let before_paths: std::collections::HashSet<String> = storage
            .list_data_files(&sess.tenant(), &table)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.path.as_ref().to_string())
            .collect();
        assert_eq!(before_paths.len(), 2, "expected two files seeded");

        // The AND predicate's `id > 999_000` branch is outside file 1's
        // range entirely; pruning must skip file 1.
        let res = sess
            .execute("UPDATE t SET name = 'Q' WHERE id > 999000 AND name = 'foo'")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let after_paths: std::collections::HashSet<String> = storage
            .list_data_files(&sess.tenant(), &table)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.path.as_ref().to_string())
            .collect();
        let kept_unchanged: Vec<&String> =
            after_paths.iter().filter(|p| before_paths.contains(*p)).collect();
        assert_eq!(
            kept_unchanged.len(),
            1,
            "expected 1 of 2 parent files to pass through unchanged"
        );

        let after = eng
            .config()
            .catalog
            .load_table(&sess.tenant(), &table)
            .await
            .unwrap();
        let head = after.current().unwrap();
        assert_eq!(head.summary.removed_files, 1);
    }

    #[tokio::test]
    async fn viability_update_delete_pruning() {
        // Insert 1M rows in 10 files; DELETE one row from the last file.
        // 9 of 10 files must survive the rewrite by-path-equality (proving
        // file-level pruning kept them out of the rewrite loop entirely).
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();

        const ROWS_PER_FILE: i64 = 100_000;
        const FILE_COUNT: i64 = 10;
        for f in 0..FILE_COUNT {
            let mut sql = String::with_capacity((ROWS_PER_FILE as usize) * 30);
            sql.push_str("INSERT INTO t VALUES ");
            let start = f * ROWS_PER_FILE;
            for i in 0..ROWS_PER_FILE {
                if i > 0 {
                    sql.push(',');
                }
                let id = start + i;
                sql.push_str(&format!("({id}, 'r')"));
            }
            sess.execute(&sql).await.unwrap();
        }

        let table = basin_common::TableName::new("t").unwrap();
        let storage = eng.config().storage.clone();
        let before_paths: std::collections::HashSet<String> = storage
            .list_data_files(&sess.tenant(), &table)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.path.as_ref().to_string())
            .collect();
        assert_eq!(before_paths.len(), FILE_COUNT as usize);

        // 999_999 lives only in the last file (ids 900_000..999_999).
        let res = sess
            .execute("DELETE FROM t WHERE id = 999999")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1"),
            other => panic!("unexpected: {other:?}"),
        }

        let after_paths: std::collections::HashSet<String> = storage
            .list_data_files(&sess.tenant(), &table)
            .await
            .unwrap()
            .into_iter()
            .map(|f| f.path.as_ref().to_string())
            .collect();
        let kept_unchanged: Vec<&String> =
            after_paths.iter().filter(|p| before_paths.contains(*p)).collect();
        assert_eq!(
            kept_unchanged.len(),
            (FILE_COUNT - 1) as usize,
            "expected {} of {} files to survive pruning unchanged",
            FILE_COUNT - 1,
            FILE_COUNT
        );

        let after = eng
            .config()
            .catalog
            .load_table(&sess.tenant(), &table)
            .await
            .unwrap();
        let head = after.current().unwrap();
        assert_eq!(head.summary.removed_files, 1);

        // Sanity: the table now has 1M-1 rows.
        let res = sess.execute("SELECT id FROM t").await.unwrap();
        let total = match res {
            ExecResult::Rows { batches, .. } => total_rows(&batches),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(total, (FILE_COUNT * ROWS_PER_FILE - 1) as usize);
    }
}
