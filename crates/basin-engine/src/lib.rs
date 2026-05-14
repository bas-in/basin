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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_common::{
    ChangeEventSink, EventSinkRegistry, Result, TableName, TenantCounterRegistry,
    TenantCountersSnapshot, TenantId,
};
use uuid::Uuid;

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
    /// Counter incremented when the planner detects a vector `ORDER BY <->
    /// LIMIT k` query and routes it to the HNSW fast path. Tests assert on
    /// this so they can prove the rewrite actually fired vs. accidentally
    /// falling through to brute-force.
    pub(crate) vector_routing_count: AtomicU64,
    /// Per-tenant noisy-tenant detector. Reads its bit when a session is
    /// opened (to choose `target_partitions`) and bumps it after every
    /// successful `TenantSession::execute`. See `noisy_detector` module
    /// for the full rationale.
    pub(crate) noisy_detector: crate::noisy_detector::NoisyDetector,
    /// Per-tenant ops/bytes/errors/latency aggregator (Phase 6 telemetry).
    /// Shared with `Storage` and (when present) `Wal` so byte counters cover
    /// every layer.
    pub(crate) tenant_counters: Arc<TenantCounterRegistry>,
    /// Pluggable change-event sinks (pre/post-commit). With both lists
    /// empty the executor's mutation path is a single `is_empty()` check
    /// past the no-op branch — see `dml_events`.
    pub(crate) event_sinks: RwLock<EventSinkRegistry>,
    /// Monotonic per-`(tenant, table)` event sequence numbers. Bumped
    /// under this mutex once per emitted event, just before the
    /// pre-commit fan-out, so all sinks observe the same seq for one
    /// committed mutation.
    pub(crate) event_seq: Mutex<HashMap<(TenantId, TableName), u64>>,
    /// Process-wide webhook subscription registry. Cheap to clone
    /// (`Arc` inside). The post-commit `WebhookSink` (in
    /// `basin-webhooks`) reads this same registry; the engine's
    /// `ALTER TABLE … SUBSCRIBE WEBHOOK …` SQL surface mutates it.
    pub(crate) webhook_registry: crate::webhook_registry::WebhookRegistry,
    // additional state (DataFusion runtime, per-tenant context cache) lives
    // in the implementation module; intentionally not exposed here.
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        crate::cron_glue::install();
        crate::net_glue::install();
        crate::geo_glue::install();
        crate::trgm_glue::install();
        let tenant_counters = Arc::new(TenantCounterRegistry::new());
        // Share the registry with storage (and the shard's WAL when present)
        // so per-tenant byte counters cover engine + storage + WAL.
        cfg.storage.attach_tenant_counters(tenant_counters.clone());
        // Hand storage the catalog handle so the encryption call path can
        // look up per-tenant `TenantStorageConfig` and route to the
        // tenant's CMK without owning a registry of its own.
        cfg.storage.attach_catalog(cfg.catalog.clone());
        if let Some(shard) = cfg.shard.as_ref() {
            shard.wal().attach_tenant_counters(tenant_counters.clone());
        }
        let mut registry = EventSinkRegistry::new();
        // Opt-in debug helper: with `BASIN_TRACE_CHANGE_EVENTS=1` every
        // committed mutation logs a structured `tracing::info!` line. Default
        // off; nothing is attached unless the env var is set at engine-build
        // time.
        if std::env::var("BASIN_TRACE_CHANGE_EVENTS").as_deref() == Ok("1") {
            registry.register_post_commit(Arc::new(crate::events::TracingSink));
        }
        let catalog = cfg.catalog.clone();
        let inner = Arc::new(EngineInner {
            cfg,
            analytical: None,
            analytical_routing_count: AtomicU64::new(0),
            vector_routing_count: AtomicU64::new(0),
            noisy_detector: crate::noisy_detector::NoisyDetector::new(),
            tenant_counters,
            event_sinks: RwLock::new(registry),
            event_seq: Mutex::new(HashMap::new()),
            webhook_registry: crate::webhook_registry::WebhookRegistry::new(),
        });
        attach_reactor_sink(&inner, catalog);
        Self { inner }
    }

    /// Persist a per-tenant storage config (KMS routing + provider
    /// extras). Passthrough to [`basin_storage::Storage::set_tenant_storage_config`];
    /// invalidates the in-process cache so the next encryption call
    /// picks up the new config.
    pub async fn set_tenant_storage_config(
        &self,
        tenant: &TenantId,
        config: basin_storage::TenantStorageConfig,
    ) -> Result<(), basin_common::BasinError> {
        self.inner
            .cfg
            .storage
            .set_tenant_storage_config(tenant, config)
            .await
    }

    /// Look up a tenant's persisted storage config. Passthrough to
    /// [`basin_storage::Storage::get_tenant_storage_config`].
    pub async fn get_tenant_storage_config(
        &self,
        tenant: &TenantId,
    ) -> Result<Option<basin_storage::TenantStorageConfig>, basin_common::BasinError> {
        self.inner
            .cfg
            .storage
            .get_tenant_storage_config(tenant)
            .await
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
        let tenant_counters = self.inner.tenant_counters.clone();
        let mut registry = EventSinkRegistry::new();
        if std::env::var("BASIN_TRACE_CHANGE_EVENTS").as_deref() == Ok("1") {
            registry.register_post_commit(Arc::new(crate::events::TracingSink));
        }
        let catalog = cfg.catalog.clone();
        let inner = Arc::new(EngineInner {
            cfg,
            analytical: Some(analytical),
            analytical_routing_count: AtomicU64::new(0),
            vector_routing_count: AtomicU64::new(0),
            noisy_detector: crate::noisy_detector::NoisyDetector::new(),
            tenant_counters,
            event_sinks: RwLock::new(registry),
            event_seq: Mutex::new(HashMap::new()),
            webhook_registry: crate::webhook_registry::WebhookRegistry::new(),
        });
        attach_reactor_sink(&inner, catalog);
        Self { inner }
    }

    /// Attach a [`ChangeEventSink`] that runs synchronously *before* the
    /// catalog commit. An `Err` from any pre-commit sink aborts the
    /// mutation; the row is never visible. See ADR 0012.
    pub fn attach_pre_commit_sink(&self, sink: Arc<dyn ChangeEventSink>) {
        self.inner
            .event_sinks
            .write()
            .expect("event_sinks lock poisoned")
            .register_pre_commit(sink);
    }

    /// Attach a [`ChangeEventSink`] that runs *after* the catalog commit
    /// succeeds. Each post-commit sink is dispatched on its own
    /// `tokio::spawn`; errors are logged but do not roll back.
    pub fn attach_post_commit_sink(&self, sink: Arc<dyn ChangeEventSink>) {
        self.inner
            .event_sinks
            .write()
            .expect("event_sinks lock poisoned")
            .register_post_commit(sink);
    }

    pub(crate) fn event_sinks(&self) -> &RwLock<EventSinkRegistry> {
        &self.inner.event_sinks
    }

    /// Allocate the next per-`(tenant, table)` sequence number. Crate-
    /// private; used by the mutation path right before pre-commit fan-out.
    pub(crate) fn next_event_seq(&self, tenant: &TenantId, table: &TableName) -> u64 {
        let mut map = self
            .inner
            .event_seq
            .lock()
            .expect("event_seq lock poisoned");
        let entry = map.entry((*tenant, table.clone())).or_insert(0);
        *entry += 1;
        *entry
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

    /// Plain-data snapshot of `tenant`'s telemetry counters. Returns
    /// `Default::default()` for tenants with no recorded activity yet.
    /// Cheap (one HashMap probe + atomic loads + ≤128-element sort).
    pub fn tenant_counters(&self, tenant: &TenantId) -> TenantCountersSnapshot {
        self.inner
            .tenant_counters
            .snapshot(tenant)
            .unwrap_or_default()
    }

    /// Crate-private shared registry handle so the executor / session can
    /// bump op + latency counters without re-locking the registry per call.
    pub(crate) fn tenant_counters_registry(&self) -> &Arc<TenantCounterRegistry> {
        &self.inner.tenant_counters
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// Process-wide webhook subscription registry. Shared with the
    /// `WebhookSink` post-commit sink (in `basin-webhooks`) so the
    /// `ALTER TABLE … SUBSCRIBE WEBHOOK …` SQL surface and HTTP
    /// delivery talk to the same map. Cheap to clone (`Arc` inside).
    pub fn webhook_registry(&self) -> &crate::webhook_registry::WebhookRegistry {
        &self.inner.webhook_registry
    }

    /// Optional analytical engine attached via [`Engine::with_analytical`].
    pub(crate) fn analytical(&self) -> Option<&basin_analytical::AnalyticalEngine> {
        self.inner.analytical.as_ref()
    }

    /// Internal hook used by the executor to record successful analytical
    /// routings. Crate-private so external callers can't tamper with the
    /// counter (only [`Engine::analytical_routing_count`] is exposed).
    pub(crate) fn note_analytical_routed(&self) {
        self.inner
            .analytical_routing_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Number of statements successfully routed to the analytical path
    /// since this `Engine` was built. Test-only instrumentation; production
    /// code should rely on tracing for routing visibility.
    pub fn analytical_routing_count(&self) -> u64 {
        self.inner.analytical_routing_count.load(Ordering::Relaxed)
    }

    /// Crate-private hook bumped by `executor::execute` when a vector
    /// `ORDER BY <-> LIMIT k` query is dispatched to the HNSW fast path.
    pub(crate) fn note_vector_routed(&self) {
        self.inner
            .vector_routing_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Number of `ORDER BY <-> LIMIT k` statements served via the HNSW
    /// fast path since this `Engine` was built. Test-only.
    pub fn vector_routing_count(&self) -> u64 {
        self.inner.vector_routing_count.load(Ordering::Relaxed)
    }

    /// Open a session bound to `tenant`. The catalog namespace is created on
    /// demand if it does not yet exist.
    ///
    /// The session's `current_user` is the literal `"anonymous"` — RLS
    /// policies that rely on an authenticated principal will treat the
    /// session that way. To plumb a real principal through (e.g. from a
    /// pgwire handshake or JWT), use [`Engine::open_session_as`].
    pub async fn open_session(&self, tenant: TenantId) -> Result<TenantSession> {
        crate::session::open(
            self.clone(),
            tenant,
            ANONYMOUS_USER.to_string(),
            Arc::new(AuthContext::anonymous()),
        )
        .await
    }

    /// Open a session bound to `tenant` and stamp `current_user` with the
    /// given principal name. The principal is exactly the string returned
    /// by SQL's `current_user` / `current_role` (we don't distinguish them
    /// in v0.1 — both resolve to this same value).
    ///
    /// `current_user` is purely consumed by the row-level-security predicate
    /// evaluator; the rest of the engine behaves identically regardless.
    ///
    /// Auth session functions (`auth.uid()` / `auth.role()` / `auth.jwt()`)
    /// will return `NULL` / `'anon'` for sessions opened this way. To supply
    /// JWT-derived claims, use [`Engine::open_session_with_auth`].
    pub async fn open_session_as(
        &self,
        tenant: TenantId,
        current_user: impl Into<String>,
    ) -> Result<TenantSession> {
        crate::session::open(
            self.clone(),
            tenant,
            current_user.into(),
            Arc::new(AuthContext::anonymous()),
        )
        .await
    }

    /// Open a session bound to `tenant`, setting both the `current_user`
    /// principal and the auth context used by `auth.uid()`, `auth.role()`,
    /// and `auth.jwt()`. Called by the pgwire router when a JWT connection
    /// is authenticated — the JWT claims are decoded and carried into the
    /// session so RLS policies can reference them without re-verifying the
    /// token on every query.
    pub async fn open_session_with_auth(
        &self,
        tenant: TenantId,
        current_user: impl Into<String>,
        auth: AuthContext,
    ) -> Result<TenantSession> {
        crate::session::open(self.clone(), tenant, current_user.into(), Arc::new(auth)).await
    }
}

/// Build the reactor sink and register it with the freshly-built
/// engine. Holds a `Weak<EngineInner>` so reactor bodies can re-enter
/// the engine without keeping it alive past user drop.
fn attach_reactor_sink(inner: &Arc<EngineInner>, catalog: Arc<dyn basin_catalog::Catalog>) {
    let sink = crate::reactor_sink::ReactorSink::new(catalog, Arc::downgrade(inner));
    inner
        .event_sinks
        .write()
        .expect("event_sinks lock poisoned")
        .register_pre_commit(Arc::new(sink));
}

/// Default principal stamped on a session opened without explicit auth. RLS
/// policies that quote `current_user` against this constant will, by design,
/// not match an authenticated user's rows.
pub const ANONYMOUS_USER: &str = "anonymous";

/// Authentication context stamped onto a session at open time. Carries the
/// claims needed to back `auth.uid()`, `auth.role()`, and `auth.jwt()` SQL
/// functions (Supabase-compatible session functions). All fields are `None`
/// / `"anon"` for unauthenticated sessions, matching the Supabase contract
/// that `auth.uid()` returns `NULL` rather than erroring when no JWT is
/// present.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// UUID of the authenticated user (`sub` / `user_id` JWT claim). `None`
    /// for anonymous/service connections.
    pub auth_uid: Option<Uuid>,
    /// Role string: `"authenticated"`, `"anon"`, or `"service_role"`.
    pub auth_role: String,
    /// Full JWT claims as a JSON value. `None` for non-JWT sessions.
    pub auth_claims: Option<serde_json::Value>,
}

impl AuthContext {
    /// Anonymous (unauthenticated) context — `auth.uid()` → NULL,
    /// `auth.role()` → `'anon'`.
    pub fn anonymous() -> Self {
        Self {
            auth_uid: None,
            auth_role: "anon".to_string(),
            auth_claims: None,
        }
    }

    /// Build an authenticated context from decoded JWT claims. `user_id` maps
    /// to `auth.uid()`, `role` to `auth.role()`, and `claims` (the full JSON
    /// object) to `auth.jwt()`. Called by the pgwire router after successfully
    /// verifying the bearer JWT; the context is then passed to
    /// [`Engine::open_session_with_auth`].
    pub fn from_jwt(user_id: Uuid, role: impl Into<String>, claims: serde_json::Value) -> Self {
        Self {
            auth_uid: Some(user_id),
            auth_role: role.into(),
            auth_claims: Some(claims),
        }
    }
}

/// A handle to the engine scoped to a single tenant. All [`execute`] calls
/// run as this tenant; there is no reset / impersonate API by design.
///
/// [`execute`]: TenantSession::execute
pub struct TenantSession {
    pub(crate) engine: Engine,
    pub(crate) tenant: TenantId,
    /// Principal name that resolves SQL's `current_user`. Stamped at session
    /// open time and never mutated thereafter. Read by the RLS predicate
    /// rewriter; the rest of the engine ignores it.
    pub(crate) current_user: String,
    /// Auth context for `auth.uid()` / `auth.role()` / `auth.jwt()` UDFs.
    /// Captured once at session-open time by `register_auth_udfs`; only
    /// accessed through the UDF closures thereafter (hence the allow).
    #[allow(dead_code)]
    pub(crate) auth_context: Arc<AuthContext>,
    pub(crate) ctx: datafusion::prelude::SessionContext,
    pub(crate) state: Arc<crate::session::SessionState>,
}

impl TenantSession {
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// Principal that resolves SQL's `current_user` for RLS predicates. For
    /// sessions opened via [`Engine::open_session`] this is
    /// [`ANONYMOUS_USER`].
    pub fn current_user(&self) -> &str {
        &self.current_user
    }

    /// Run one SQL statement. Returns either a result set ([`ExecResult::Rows`])
    /// or a side-effect tag for DML/DDL ([`ExecResult::Empty`]).
    #[tracing::instrument(skip(self, sql), fields(tenant=%self.tenant, sql=%sql.lines().next().unwrap_or("")))]
    pub async fn execute(&self, sql: &str) -> Result<ExecResult> {
        let started = std::time::Instant::now();
        let result = crate::executor::execute(self, sql).await;
        // Bump the noisy-tenant rate estimator regardless of success: a
        // failed query still consumed I/O permits + planner time, which is
        // exactly what the detector is meant to throttle. O(1).
        self.engine.record_query(&self.tenant);
        // Per-tenant op + latency + error counters (Phase 6 telemetry).
        let tc = self
            .engine
            .tenant_counters_registry()
            .for_tenant(&self.tenant);
        tc.record_op();
        let elapsed_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        tc.record_latency_ms(elapsed_ms);
        if result.is_err() {
            tc.record_error();
        }
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
    pub async fn describe_statement(&self, handle: &StatementHandle) -> Result<StatementSchema> {
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

mod alter;
mod analytical_route;
mod constraints;
mod convert;
mod cost_check;
mod cron_glue;
mod cv_ddl;
pub mod cv_time_bucket;
mod ddl;
mod dml;
mod dml_mutate;
mod enum_ordinal;
mod events;
mod executor;
mod fast_select;
mod function_ddl;
mod generated_cols;
mod geo_glue;
mod info_schema_provider;
mod lifecycle;
mod net_glue;
mod noisy_detector;
mod prepared;
mod procedure_ddl;
pub mod reactor_ddl;
mod reactor_sink;
mod rls;
mod seq_ddl;
mod seq_udf;
mod session;
mod sql_functions;
mod trgm_glue;
mod pg_operators;
mod type_ddl;
mod types;
mod udf;
mod vector_planner;
mod vector_search;
pub mod webhook_ddl;
pub mod webhook_registry;

pub use webhook_ddl::{
    exec_subscribe_webhook, exec_unsubscribe_webhook, match_alter_table_subscribe_webhook,
    match_alter_table_unsubscribe_webhook, SubscribeIntent, UnsubscribeIntent,
};
pub use webhook_registry::{
    SubscriptionError, WebhookOps, WebhookRegistry, WebhookSubscription, WebhookSubscriptionId,
};

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
            disk_cache: None,
            page_cache: None,
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

        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
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
        sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
            .await
            .unwrap();

        let res = sess
            .execute("SELECT id, name FROM t WHERE id = 3")
            .await
            .unwrap();
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

        let ra = sa
            .execute("SELECT id, who FROM shared ORDER BY id")
            .await
            .unwrap();
        let rb = sb
            .execute("SELECT id, who FROM shared ORDER BY id")
            .await
            .unwrap();
        let (ba, bb) = match (ra, rb) {
            (ExecResult::Rows { batches: ba, .. }, ExecResult::Rows { batches: bb, .. }) => {
                (ba, bb)
            }
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

        sess.execute("CREATE TABLE alpha (id BIGINT)")
            .await
            .unwrap();
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

        let (_, schema) = sess.prepare("INSERT INTO t VALUES ($1, $2)").await.unwrap();
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

        // INTERVAL is still unsupported in v0.1 (no Arrow physical mapping
        // wired). Bare TIMESTAMP used to live here — it's now accepted (see
        // tests/timestamp_no_tz.rs).
        let err = sess
            .execute("CREATE TABLE bad (i INTERVAL)")
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
        sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
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

        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
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

        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
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
        sess.execute("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, NULL), (5, 'e')")
            .await
            .unwrap();

        let res = sess
            .execute("DELETE FROM t WHERE name IS NULL")
            .await
            .unwrap();
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

        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
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
        let kept_unchanged: Vec<&String> = after_paths
            .iter()
            .filter(|p| before_paths.contains(*p))
            .collect();
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
        let kept_unchanged: Vec<&String> = after_paths
            .iter()
            .filter(|p| before_paths.contains(*p))
            .collect();
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

    // --- Phase 5.11.B declarative lifecycle ---

    use arrow_array::TimestampMicrosecondArray;

    fn col_ts_micros(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(arr.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn auto_update_advances_on_update() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();

        sess.execute(
            "CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL, \
             updated_at TIMESTAMPTZ AUTO_UPDATE)",
        )
        .await
        .unwrap();
        sess.execute("INSERT INTO t VALUES (1, 'a', '2026-01-01T00:00:00Z')")
            .await
            .unwrap();
        let res = sess
            .execute("SELECT updated_at FROM t WHERE id = 1")
            .await
            .unwrap();
        let before = match res {
            ExecResult::Rows { batches, .. } => col_ts_micros(&batches, "updated_at")[0],
            other => panic!("{other:?}"),
        };

        // Update only `name`; AUTO_UPDATE must stamp `updated_at`.
        sess.execute("UPDATE t SET name = 'b' WHERE id = 1")
            .await
            .unwrap();

        let res = sess
            .execute("SELECT updated_at FROM t WHERE id = 1")
            .await
            .unwrap();
        let after = match res {
            ExecResult::Rows { batches, .. } => col_ts_micros(&batches, "updated_at")[0],
            other => panic!("{other:?}"),
        };
        assert!(
            after > before,
            "auto-update should advance (before={before}, after={after})"
        );
    }

    #[tokio::test]
    async fn auto_update_does_not_override_explicit_set() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, updated_at TIMESTAMPTZ AUTO_UPDATE)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t VALUES (1, '2026-01-01T00:00:00Z')")
            .await
            .unwrap();
        // Explicit SET wins over AUTO_UPDATE.
        sess.execute("UPDATE t SET updated_at = '2025-06-15T00:00:00Z' WHERE id = 1")
            .await
            .unwrap();
        let res = sess
            .execute("SELECT updated_at FROM t WHERE id = 1")
            .await
            .unwrap();
        let micros = match res {
            ExecResult::Rows { batches, .. } => col_ts_micros(&batches, "updated_at")[0],
            other => panic!("{other:?}"),
        };
        // 2025-06-15T00:00:00Z is well before 2026-01-01.
        let cutoff: i64 = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert!(micros < cutoff, "explicit SET should win, got {micros}");
    }

    #[tokio::test]
    async fn soft_delete_rewrites_to_update_and_filters_select() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        sess.execute(
            "CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL, \
             deleted_at TIMESTAMPTZ SOFT DELETE)",
        )
        .await
        .unwrap();
        sess.execute("INSERT INTO t VALUES (1, 'a', NULL), (2, 'b', NULL), (3, 'c', NULL)")
            .await
            .unwrap();

        // DELETE rewrite: tag returns DELETE 1.
        let res = sess.execute("DELETE FROM t WHERE id = 2").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 1"),
            other => panic!("{other:?}"),
        }

        // SELECT default: id=2 is hidden.
        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 3]);
            }
            other => panic!("{other:?}"),
        }

        // INCLUDE DELETED opt-out shows it.
        let res = sess
            .execute("SELECT id FROM t ORDER BY id INCLUDE DELETED")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn second_soft_delete_column_rejected() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        let err = sess
            .execute(
                "CREATE TABLE t (id BIGINT, a TIMESTAMPTZ SOFT DELETE, \
                 b TIMESTAMPTZ SOFT DELETE)",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, basin_common::BasinError::InvalidSchema(_)));
    }

    #[tokio::test]
    async fn audit_to_logs_one_row_per_mutation() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        sess.execute(
            "CREATE TABLE foo (id BIGINT NOT NULL, name TEXT NOT NULL) AUDIT TO foo_audit",
        )
        .await
        .unwrap();
        sess.execute("INSERT INTO foo VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
        sess.execute("UPDATE foo SET name = 'B' WHERE id = 2")
            .await
            .unwrap();
        sess.execute("DELETE FROM foo WHERE id = 1").await.unwrap();

        let res = sess
            .execute("SELECT op FROM foo_audit ORDER BY audit_id")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let ops = col_string(&batches, "op");
                assert_eq!(
                    ops,
                    vec![
                        "insert".to_string(),
                        "insert".to_string(),
                        "update".to_string(),
                        "delete".to_string(),
                    ]
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn audit_isolation_per_tenant() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let sa = eng.open_session(a).await.unwrap();
        let sb = eng.open_session(b).await.unwrap();
        for s in [&sa, &sb] {
            s.execute(
                "CREATE TABLE foo (id BIGINT NOT NULL, name TEXT NOT NULL) AUDIT TO foo_audit",
            )
            .await
            .unwrap();
        }
        sa.execute("INSERT INTO foo VALUES (1, 'A1')")
            .await
            .unwrap();
        sb.execute("INSERT INTO foo VALUES (1, 'B1'), (2, 'B2')")
            .await
            .unwrap();

        let res_a = sa.execute("SELECT op FROM foo_audit").await.unwrap();
        let res_b = sb.execute("SELECT op FROM foo_audit").await.unwrap();
        match (res_a, res_b) {
            (ExecResult::Rows { batches: ba, .. }, ExecResult::Rows { batches: bb, .. }) => {
                assert_eq!(total_rows(&ba), 1);
                assert_eq!(total_rows(&bb), 2);
            }
            _ => panic!("expected rows on both"),
        }
    }

    #[tokio::test]
    async fn composition_auto_update_audit_soft_delete() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        sess.execute(
            "CREATE TABLE foo (\
             id BIGINT NOT NULL,\
             name TEXT NOT NULL,\
             updated_at TIMESTAMPTZ AUTO_UPDATE,\
             deleted_at TIMESTAMPTZ SOFT DELETE\
             ) AUDIT TO foo_audit",
        )
        .await
        .unwrap();
        sess.execute("INSERT INTO foo VALUES (1, 'a', '2026-01-01T00:00:00Z', NULL)")
            .await
            .unwrap();
        sess.execute("UPDATE foo SET name = 'b' WHERE id = 1")
            .await
            .unwrap();
        sess.execute("DELETE FROM foo WHERE id = 1").await.unwrap();

        // Audit table: insert + update + delete. The soft-delete row
        // must be op='delete' even though the underlying write was an
        // UPDATE.
        let res = sess
            .execute("SELECT op FROM foo_audit ORDER BY audit_id")
            .await
            .unwrap();
        let ops = match res {
            ExecResult::Rows { batches, .. } => col_string(&batches, "op"),
            other => panic!("{other:?}"),
        };
        assert_eq!(ops, vec!["insert", "update", "delete"]);

        // Soft-delete hides id=1 by default.
        let res = sess.execute("SELECT id FROM foo").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 0);
            }
            other => panic!("{other:?}"),
        }
        // INCLUDE DELETED reveals it.
        let res = sess
            .execute("SELECT id FROM foo INCLUDE DELETED")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1]);
            }
            other => panic!("{other:?}"),
        }
    }

    // --- auth.uid() / auth.role() / auth.jwt() session functions ---

    #[tokio::test]
    async fn auth_uid_returns_null_for_anonymous_session() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        // `open_session` uses anonymous AuthContext.
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        let res = sess.execute("SELECT auth_uid()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                assert!(arr.is_null(0), "expected NULL for anonymous auth_uid()");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_uid_schema_dot_form_works() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        // `auth.uid()` should be rewritten to `auth_uid()` before reaching the engine.
        let res = sess.execute("SELECT auth.uid()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                // Anonymous session → NULL.
                assert!(
                    arr.is_null(0),
                    "expected NULL for auth.uid() in anonymous session"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_role_returns_anon_for_anonymous_session() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        let res = sess.execute("SELECT auth_role()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                assert_eq!(arr.value(0), "anon");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_jwt_returns_null_for_anonymous_session() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        let res = sess.execute("SELECT auth_jwt()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                assert!(
                    arr.is_null(0),
                    "expected NULL for auth_jwt() in anonymous session"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_uid_returns_uuid_for_authenticated_session() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let user_id = uuid::Uuid::new_v4();
        let auth = AuthContext::from_jwt(
            user_id,
            "authenticated",
            serde_json::json!({"user_id": user_id.to_string()}),
        );
        let sess = eng
            .open_session_with_auth(TenantId::new(), "alice", auth)
            .await
            .unwrap();
        let res = sess.execute("SELECT auth_uid()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                assert!(!arr.is_null(0));
                assert_eq!(arr.value(0), user_id.to_string());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn auth_role_returns_authenticated_for_jwt_session() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let auth =
            AuthContext::from_jwt(uuid::Uuid::new_v4(), "authenticated", serde_json::json!({}));
        let sess = eng
            .open_session_with_auth(TenantId::new(), "alice", auth)
            .await
            .unwrap();
        let res = sess.execute("SELECT auth.role()").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let arr = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                assert_eq!(arr.value(0), "authenticated");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
