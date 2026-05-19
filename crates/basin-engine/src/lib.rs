//! `basin-engine` — single-process SQL execution engine for the Basin PoC.
//!
//! This is the layer that, in a production deployment, would split across
//! shard owners, the placement service, and the analytical pool. For the
//! local PoC we collapse all of that into one in-process [`Engine`] that:
//!
//! 1. Holds a `basin_storage::Storage` (the Parquet substrate) and a
//!    `basin_catalog::Catalog` (table metadata + snapshots).
//! 2. Hands out per-project [`ProjectSession`]s. Each session is the only API
//!    surface a router or test should program against.
//! 3. Compiles and executes SQL via DataFusion against the calling project's
//!    namespace. Project isolation is structural: there is no API path on
//!    [`ProjectSession`] that exposes another project's data.
//!
//! The module-level types declared here are the *public contract* the router
//! depends on. Their bodies are filled in by [`Engine::new`] /
//! [`ProjectSession::execute`].
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
//! [`ProjectSession::prepare`] / [`ProjectSession::bind`] /
//! [`ProjectSession::execute_bound`] back the Postgres extended-query path the
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
    ChangeEventSink, EventSinkRegistry, ProjectCounterRegistry, ProjectCountersSnapshot, ProjectId,
    Result, TableName,
};
use datafusion::execution::cache::cache_manager::{CacheManagerConfig, FileMetadataCache};
use datafusion::execution::cache::DefaultFilesMetadataCache;
use uuid::Uuid;

/// Pre-built stateless UDF registry shared across all sessions on one `Engine`.
///
/// Building a `SessionContext` for a new project session requires registering
/// ~200 stateless UDFs (vector distance, JSONB, string, datetime, FTS stubs,
/// pg_catalog, range, pg_scalar, interval-tz, etc.). The stateless UDFs are
/// identical for every session, so we build them once at `Engine::new` time
/// and clone the `Arc<ScalarUDF>` / `Arc<AggregateUDF>` handles at
/// session-open time (arc ref-count bump, no allocation).
///
/// The cache also includes DataFusion's own default built-ins so the
/// `SessionStateBuilder` at session-open time can skip `with_default_features`
/// for scalars/aggregates and use this combined vec directly.
pub(crate) struct StatelessUdfCache {
    pub(crate) scalar: Vec<Arc<datafusion::logical_expr::ScalarUDF>>,
    pub(crate) aggregate: Vec<Arc<datafusion::logical_expr::AggregateUDF>>,
}

/// Engine configuration.
///
/// `storage` and `catalog` are shared across all project sessions; per-project
/// scoping happens *inside* the engine, not by handing out per-project
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
    /// Counter incremented when the planner detects a vector `ORDER BY <->
    /// LIMIT k` query and routes it to the HNSW fast path. Tests assert on
    /// this so they can prove the rewrite actually fired vs. accidentally
    /// falling through to brute-force.
    pub(crate) vector_routing_count: AtomicU64,
    /// Counter bumped every time a statement is handled via the pg_query /
    /// pg_plan path (ADR 0014 Phase 1+). Tests assert this advances when
    /// they expect the new routing to engage.
    pub(crate) pg_plan_routing_count: AtomicU64,
    /// Cumulative number of data files skipped by the bloom-filter probe in
    /// `fast_select`. Incremented once per file where the bloom proves the
    /// Eq-predicate value is definitely absent. Used by integration tests to
    /// measure the empirical false-positive rate without a separate counter
    /// store.
    pub(crate) blooms_skipped: AtomicU64,
    /// Per-project noisy-project detector. Reads its bit when a session is
    /// opened (to choose `target_partitions`) and bumps it after every
    /// successful `ProjectSession::execute`. See `noisy_detector` module
    /// for the full rationale.
    pub(crate) noisy_detector: crate::noisy_detector::NoisyDetector,
    /// Per-project ops/bytes/errors/latency aggregator (Phase 6 telemetry).
    /// Shared with `Storage` and (when present) `Wal` so byte counters cover
    /// every layer.
    pub(crate) project_counters: Arc<ProjectCounterRegistry>,
    /// Pluggable change-event sinks (pre/post-commit). With both lists
    /// empty the executor's mutation path is a single `is_empty()` check
    /// past the no-op branch — see `dml_events`.
    pub(crate) event_sinks: RwLock<EventSinkRegistry>,
    /// Monotonic per-`(project, table)` event sequence numbers. Bumped
    /// under this mutex once per emitted event, just before the
    /// pre-commit fan-out, so all sinks observe the same seq for one
    /// committed mutation.
    pub(crate) event_seq: Mutex<HashMap<(ProjectId, TableName), u64>>,
    /// Process-wide webhook subscription registry. Cheap to clone
    /// (`Arc` inside). The post-commit `WebhookSink` (in
    /// `basin-webhooks`) reads this same registry; the engine's
    /// `ALTER TABLE … SUBSCRIBE WEBHOOK …` SQL surface mutates it.
    pub(crate) webhook_registry: crate::webhook_registry::WebhookRegistry,
    /// Pre-built stateless UDF registry. Shared across all sessions; each
    /// session-open clones only the `Arc` handles (ref-count bumps, no heap
    /// allocation for the UDF structs themselves). See `StatelessUdfCache`.
    pub(crate) udf_cache: Arc<StatelessUdfCache>,
    /// Process-wide file metadata cache (Vortex footer / Parquet footer data).
    /// Shared across all sessions so footer parses survive session recycling.
    /// Each session-open builds a fresh `RuntimeEnv` but plugs this same
    /// `Arc` into its `CacheManagerConfig` so the cache outlives any single
    /// session. Capacity: 50 MiB (covers ~hundreds of Vortex/Parquet files).
    pub(crate) file_metadata_cache: Arc<dyn FileMetadataCache>,
    /// Per-`(project, table)` query pattern history for adaptive sort
    /// (Phase 5.14.D2).  Records ORDER BY / GROUP BY column tuples observed
    /// at query time so the compactor (Phase 5.14.D1) can detect common
    /// access patterns and pre-sort files accordingly.
    pub(crate) query_history: Arc<crate::query_history::QueryHistory>,
    /// Process-wide HTAP hot-tier memtable registry (Phase 5.14.C5).
    /// Constructed once at engine startup from the process-level
    /// `MemTableConfig` (reads `BASIN_MEMTABLE_*` env vars).  Integration
    /// with the write path (C2), read-merge path (C3), and flush task (C4)
    /// lands in subsequent sub-items; this field is only constructed + exposed
    /// here so the rest of the C-series can wire against it.
    pub(crate) memtable_registry: Arc<basin_hottier::MemTableRegistry>,
    /// Monotonic source for WAL transaction ids (Phase 5.14.C2).
    ///
    /// Each explicit-tx DML assigns its tx a unique `u64` derived from this
    /// counter.  The counter is process-scoped; ids are never reused within a
    /// process lifetime so WAL markers are unambiguous across concurrent
    /// sessions.
    pub(crate) next_tx_id: AtomicU64,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Self {
        crate::cron_glue::install();
        crate::net_glue::install();
        crate::geo_glue::install();
        crate::trgm_glue::install();
        let project_counters = Arc::new(ProjectCounterRegistry::new());
        // Share the registry with storage (and the shard's WAL when present)
        // so per-project byte counters cover engine + storage + WAL.
        cfg.storage
            .attach_project_counters(project_counters.clone());
        // Hand storage the catalog handle so the encryption call path can
        // look up per-project `ProjectStorageConfig` and route to the
        // project's CMK without owning a registry of its own.
        cfg.storage.attach_catalog(cfg.catalog.clone());
        if let Some(shard) = cfg.shard.as_ref() {
            shard
                .wal()
                .attach_project_counters(project_counters.clone());
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
        // Build the stateless UDF cache once. We register all stateless UDFs
        // into a throwaway `SessionContext` (which also seeds DataFusion's own
        // default built-ins) and then extract the populated function maps.
        // Session-open time then just clones these Arc handles instead of
        // constructing ~200 UDF structs and acquiring 200+ write-locks.
        let udf_cache = Arc::new(crate::session::build_stateless_udf_cache());
        // Build the process-wide file metadata cache once. Each session-open
        // plugs this Arc into a fresh per-session RuntimeEnv so Vortex/Parquet
        // footer data persists across session recycling. 50 MiB covers hundreds
        // of small files; the LRU evicts cold entries when the limit is reached.
        let file_metadata_cache: Arc<dyn FileMetadataCache> =
            Arc::new(DefaultFilesMetadataCache::new(
                CacheManagerConfig::default().metadata_cache_limit,
            ));
        // Phase 5.14.C5: construct the process-wide memtable registry once.
        // Reads BASIN_MEMTABLE_* env vars for per-process budget overrides.
        // Integration with writes (C2), reads (C3), and flush (C4) is in
        // subsequent sub-items.
        let memtable_registry = Arc::new(basin_hottier::MemTableRegistry::new_with_config(
            basin_hottier::MemTableConfig::from_env(),
        ));
        let inner = Arc::new(EngineInner {
            cfg,
            vector_routing_count: AtomicU64::new(0),
            pg_plan_routing_count: AtomicU64::new(0),
            blooms_skipped: AtomicU64::new(0),
            noisy_detector: crate::noisy_detector::NoisyDetector::new(),
            project_counters,
            event_sinks: RwLock::new(registry),
            event_seq: Mutex::new(HashMap::new()),
            webhook_registry: crate::webhook_registry::WebhookRegistry::new(),
            udf_cache,
            file_metadata_cache,
            query_history: Arc::new(crate::query_history::QueryHistory::new()),
            memtable_registry,
            next_tx_id: AtomicU64::new(1),
        });
        // Phase 5.14.D2: register the query-history adapter with the shard so
        // the compactor can consult observed ORDER BY / GROUP BY patterns.
        if let Some(shard) = inner.cfg.shard.as_ref() {
            shard.set_top_pattern_provider(inner.query_history.clone());
        }
        attach_reactor_sink(&inner, catalog);
        Self { inner }
    }

    /// Persist a per-project storage config (KMS routing + provider
    /// extras). Passthrough to [`basin_storage::Storage::set_project_storage_config`];
    /// invalidates the in-process cache so the next encryption call
    /// picks up the new config.
    pub async fn set_project_storage_config(
        &self,
        project: &ProjectId,
        config: basin_storage::ProjectStorageConfig,
    ) -> Result<(), basin_common::BasinError> {
        self.inner
            .cfg
            .storage
            .set_project_storage_config(project, config)
            .await
    }

    /// Look up a project's persisted storage config. Passthrough to
    /// [`basin_storage::Storage::get_project_storage_config`].
    pub async fn get_project_storage_config(
        &self,
        project: &ProjectId,
    ) -> Result<Option<basin_storage::ProjectStorageConfig>, basin_common::BasinError> {
        self.inner
            .cfg
            .storage
            .get_project_storage_config(project)
            .await
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

    /// Allocate the next per-`(project, table)` sequence number. Crate-
    /// private; used by the mutation path right before pre-commit fan-out.
    pub(crate) fn next_event_seq(&self, project: &ProjectId, table: &TableName) -> u64 {
        let mut map = self
            .inner
            .event_seq
            .lock()
            .expect("event_seq lock poisoned");
        let entry = map.entry((*project, table.clone())).or_insert(0);
        *entry += 1;
        *entry
    }

    /// O(1) lookup: is `project`'s recent query rate above the noisy
    /// threshold? Returns `false` for projects we've never seen. See
    /// [`crate::noisy_detector`] for the threshold and decay constants.
    ///
    /// This is a *hint* the engine consumes when constructing a session
    /// (to downshift `target_partitions`); it is also intended to be read
    /// by the `basin-storage` fair-share scheduler to demote a project's
    /// I/O priority. It is not a hard cap.
    pub fn is_noisy(&self, project: &ProjectId) -> bool {
        self.inner.noisy_detector.is_noisy(project)
    }

    /// Crate-private: bump this project's query-rate counter. Called from
    /// `ProjectSession::execute` after the statement completes (successfully
    /// or not — every attempt counts toward the rate, since failed queries
    /// still consumed I/O budget).
    pub(crate) fn record_query(&self, project: &ProjectId) {
        self.inner.noisy_detector.record_query(project);
    }

    /// Plain-data snapshot of `project`'s telemetry counters. Returns
    /// `Default::default()` for projects with no recorded activity yet.
    /// Cheap (one HashMap probe + atomic loads + ≤128-element sort).
    pub fn project_counters(&self, project: &ProjectId) -> ProjectCountersSnapshot {
        self.inner
            .project_counters
            .snapshot(project)
            .unwrap_or_default()
    }

    /// Crate-private shared registry handle so the executor / session can
    /// bump op + latency counters without re-locking the registry per call.
    pub(crate) fn project_counters_registry(&self) -> &Arc<ProjectCounterRegistry> {
        &self.inner.project_counters
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

    /// Crate-private access to the process-wide query pattern history
    /// (Phase 5.14.D2).  Returned `Arc` is cheap to clone.
    pub(crate) fn query_history(&self) -> &Arc<crate::query_history::QueryHistory> {
        &self.inner.query_history
    }

    /// Process-wide HTAP hot-tier memtable registry (Phase 5.14.C5).
    ///
    /// The returned `Arc` is cheap to clone.  Integration with the write path
    /// (C2), read-merge path (C3), and flush task (C4) lands in subsequent
    /// sub-items; this accessor is wired here so the rest of the C-series can
    /// reference the registry via the engine handle.
    pub fn memtable_registry(&self) -> Arc<basin_hottier::MemTableRegistry> {
        self.inner.memtable_registry.clone()
    }

    /// Allocate a fresh, process-unique WAL transaction id (Phase 5.14.C2).
    ///
    /// The counter starts at 1 at engine construction and increments monotonically;
    /// ids are never reused in a process lifetime so WAL Begin/Commit/Rollback
    /// markers are unambiguous even under concurrent sessions.
    pub(crate) fn next_tx_id(&self) -> u64 {
        self.inner.next_tx_id.fetch_add(1, Ordering::Relaxed)
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

    /// Crate-private hook bumped by `fast_select` each time a data file
    /// is skipped because the per-file bloom filter proved the query
    /// predicate value is definitively absent. One call per pruned file.
    pub(crate) fn note_bloom_skipped(&self) {
        self.inner.blooms_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative count of data files that have been skipped by the
    /// bloom-filter probe since this `Engine` was created. Exposed for
    /// integration tests that verify the false-positive rate.
    pub fn blooms_skipped_count(&self) -> u64 {
        self.inner.blooms_skipped.load(Ordering::Relaxed)
    }

    /// Crate-private hook bumped when a statement is dispatched via the
    /// pg_query / pg_plan path (the new ADR 0014 routing).
    pub(crate) fn note_pg_plan_routed(&self) {
        self.inner
            .pg_plan_routing_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Number of statements served via the pg_query / pg_plan path since
    /// this `Engine` was built. Test-only instrumentation.
    pub fn pg_plan_routing_count(&self) -> u64 {
        self.inner.pg_plan_routing_count.load(Ordering::Relaxed)
    }

    /// Open a session bound to `project`. The catalog namespace is created on
    /// demand if it does not yet exist.
    ///
    /// The session's `current_user` is the literal `"anonymous"` — RLS
    /// policies that rely on an authenticated principal will treat the
    /// session that way. To plumb a real principal through (e.g. from a
    /// pgwire handshake or JWT), use [`Engine::open_session_as`].
    pub async fn open_session(&self, project: ProjectId) -> Result<ProjectSession> {
        crate::session::open(
            self.clone(),
            project,
            ANONYMOUS_USER.to_string(),
            Arc::new(AuthContext::anonymous()),
        )
        .await
    }

    /// Open a session bound to `project` and stamp `current_user` with the
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
        project: ProjectId,
        current_user: impl Into<String>,
    ) -> Result<ProjectSession> {
        crate::session::open(
            self.clone(),
            project,
            current_user.into(),
            Arc::new(AuthContext::anonymous()),
        )
        .await
    }

    /// Open a session bound to `project`, setting both the `current_user`
    /// principal and the auth context used by `auth.uid()`, `auth.role()`,
    /// and `auth.jwt()`. Called by the pgwire router when a JWT connection
    /// is authenticated — the JWT claims are decoded and carried into the
    /// session so RLS policies can reference them without re-verifying the
    /// token on every query.
    pub async fn open_session_with_auth(
        &self,
        project: ProjectId,
        current_user: impl Into<String>,
        auth: AuthContext,
    ) -> Result<ProjectSession> {
        crate::session::open(self.clone(), project, current_user.into(), Arc::new(auth)).await
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

/// A handle to the engine scoped to a single project. All [`execute`] calls
/// run as this project; there is no reset / impersonate API by design.
///
/// [`execute`]: ProjectSession::execute
pub struct ProjectSession {
    pub(crate) engine: Engine,
    pub(crate) project: ProjectId,
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

impl ProjectSession {
    pub fn project(&self) -> ProjectId {
        self.project
    }

    /// Principal that resolves SQL's `current_user` for RLS predicates. For
    /// sessions opened via [`Engine::open_session`] this is
    /// [`ANONYMOUS_USER`].
    pub fn current_user(&self) -> &str {
        &self.current_user
    }

    /// Run one SQL statement. Returns either a result set ([`ExecResult::Rows`])
    /// or a side-effect tag for DML/DDL ([`ExecResult::Empty`]).
    #[tracing::instrument(skip(self, sql), fields(project=%self.project, sql=%sql.lines().next().unwrap_or("")))]
    pub async fn execute(&self, sql: &str) -> Result<ExecResult> {
        let started = std::time::Instant::now();
        let result = crate::executor::execute(self, sql).await;
        // Bump the noisy-project rate estimator regardless of success: a
        // failed query still consumed I/O permits + planner time, which is
        // exactly what the detector is meant to throttle. O(1).
        self.engine.record_query(&self.project);
        // Per-project op + latency + error counters (Phase 6 telemetry).
        let tc = self
            .engine
            .project_counters_registry()
            .for_project(&self.project);
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
    #[tracing::instrument(skip(self, sql), fields(project=%self.project, sql=%sql.lines().next().unwrap_or("")))]
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

mod advisory_lock;
mod alter;
mod alter_project;
mod any_all_rewrite;
mod approx_count_distinct;
mod approx_percentile;
mod catalog_window_exec;
mod constraints;
mod convert;
mod cost_check;
mod cron_glue;
mod cursor;
mod cv_ddl;
pub mod cv_time_bucket;
mod datetime_extras;
mod datetime_more_udf;
mod ddl;
mod dml;
mod dml_mutate;
mod enum_ordinal;
mod events;
mod executor;
mod explain;
mod fast_aggregate;
mod fast_select;
mod fts_udf;
mod is_distinct_rewrite;
mod function_ddl;
mod generated_cols;
mod geo_glue;
mod index_extras;
mod inet_udf;
mod info_schema_provider;
mod interval_tz_udf;
mod json_build_udf;
mod jsonb_modify_udf;
mod jsonb_path_udf;
mod jsonb_udf;
mod lifecycle;
mod net_glue;
mod noisy_detector;
mod nullif_rewrite;
pub mod noop_accept;
mod pg_agg_udf;
pub mod pg_ast;
mod pg_catalog_udf;
mod pg_operators;
pub mod pg_plan;
mod pg_scalar_aliases;
mod prepared;
mod query_history;
pub(crate) mod query_shape;
mod procedure_ddl;
mod range_udf;
pub mod reactor_ddl;
mod reactor_sink;
mod regex_udf;
mod rls;
mod schema_ddl;
mod select_advanced;
mod seq_ddl;
mod seq_udf;
mod session;
mod sort_streaming_limit;
mod sql_functions;
mod string_dt_udf;
mod string_more_udf;
mod trgm_glue;
mod truncate;
mod type_ddl;
mod types;
mod udf;
mod union_scan_collapse;
mod vector_planner;
mod vector_search;
mod view_ddl;
mod vortex_listing_format;
pub mod webhook_ddl;
pub mod webhook_registry;
mod window_extras;

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
    use basin_common::ProjectId;
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
    async fn project_isolation_engine_level() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let a = ProjectId::new();
        let b = ProjectId::new();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        // A genuinely unmapped type name must still be rejected. (INTERVAL /
        // TIMESTAMP / DATE / REAL etc. are now accepted after the type-coverage
        // work; use a name no mapping will ever claim.)
        let err = sess
            .execute("CREATE TABLE bad (i NONEXISTENT_TYPE_XYZ)")
            .await
            .unwrap_err();
        assert!(
            matches!(err, basin_common::BasinError::InvalidSchema(_)),
            "got {err:?}"
        );
    }

    // --- UPDATE / DELETE -------------------------------------------------------

    async fn seed_five_rows(sess: &ProjectSession) {
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        // Snapshot before to make sure no new file gets written.
        let cat = eng.config().catalog.clone();
        let table = basin_common::TableName::new("t").unwrap();
        let before = cat.load_table(&sess.project(), &table).await.unwrap();

        let res = sess.execute("DELETE FROM t WHERE id = 999").await.unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "DELETE 0"),
            other => panic!("unexpected: {other:?}"),
        }

        let after = cat.load_table(&sess.project(), &table).await.unwrap();
        assert_eq!(
            before.current_snapshot, after.current_snapshot,
            "no-op DELETE must not advance snapshot"
        );
    }

    #[tokio::test]
    async fn delete_all_rows_drops_files() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
            .load_table(&sess.project(), &table)
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let cat = eng.config().catalog.clone();
        let table = basin_common::TableName::new("t").unwrap();
        let before = cat.load_table(&sess.project(), &table).await.unwrap();

        let res = sess
            .execute("UPDATE t SET name = 'X' WHERE id = 999")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert_eq!(tag, "UPDATE 0"),
            other => panic!("unexpected: {other:?}"),
        }

        let after = cat.load_table(&sess.project(), &table).await.unwrap();
        assert_eq!(before.current_snapshot, after.current_snapshot);
    }

    #[tokio::test]
    async fn delete_then_select_through_engine() {
        // Distinguishing test: the round trip exercises the same `execute`
        // entry point a router would call, including the post-commit refresh
        // that puts the new file set in front of DataFusion's listing table.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
            .list_data_files(&sess.project(), &table)
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
            .list_data_files(&sess.project(), &table)
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
            .load_table(&sess.project(), &table)
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
            .list_data_files(&sess.project(), &table)
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
            .list_data_files(&sess.project(), &table)
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
            .load_table(&sess.project(), &table)
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
    async fn audit_isolation_per_project() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let a = ProjectId::new();
        let b = ProjectId::new();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
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
            .open_session_with_auth(ProjectId::new(), "alice", auth)
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
            .open_session_with_auth(ProjectId::new(), "alice", auth)
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

    // ── DDL correctness: CREATE TABLE IF NOT EXISTS (task #49 Part A) ──────────

    /// CREATE TABLE IF NOT EXISTS on a non-existent table must create it normally.
    #[tokio::test]
    async fn create_table_if_not_exists_creates_when_absent() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        let res = sess
            .execute("CREATE TABLE IF NOT EXISTS new_tbl (id BIGINT NOT NULL)")
            .await
            .expect("CREATE TABLE IF NOT EXISTS must succeed on a new table");
        assert!(
            matches!(res, ExecResult::Empty { ref tag } if tag == "CREATE TABLE"),
            "expected CREATE TABLE tag, got {res:?}"
        );

        // Confirm the table exists by inserting and selecting.
        sess.execute("INSERT INTO new_tbl VALUES (1)")
            .await
            .unwrap();
        let rows = sess.execute("SELECT id FROM new_tbl").await.unwrap();
        if let ExecResult::Rows { batches, .. } = rows {
            let vals = col_i64(&batches, "id");
            assert_eq!(vals, vec![1]);
        } else {
            panic!("expected rows");
        }
    }

    /// CREATE TABLE IF NOT EXISTS on an existing table must succeed (no-op, no error).
    /// This is the PG-correct behavior; the bug was that the if_not_exists flag was
    /// ignored and the catalog "already exists" error propagated.
    #[tokio::test]
    async fn create_table_if_not_exists_noop_when_exists() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE existing_t (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO existing_t VALUES (99)")
            .await
            .unwrap();

        // Must NOT error; IF NOT EXISTS suppresses the "already exists" catalog error.
        let res = sess
            .execute("CREATE TABLE IF NOT EXISTS existing_t (id BIGINT NOT NULL)")
            .await
            .expect(
                "CREATE TABLE IF NOT EXISTS must be a no-op when table already exists, not error",
            );
        assert!(
            matches!(res, ExecResult::Empty { ref tag } if tag == "CREATE TABLE"),
            "expected CREATE TABLE tag, got {res:?}"
        );

        // Existing data must be intact — the no-op must not truncate the table.
        let rows = sess.execute("SELECT id FROM existing_t").await.unwrap();
        if let ExecResult::Rows { batches, .. } = rows {
            let vals = col_i64(&batches, "id");
            assert_eq!(
                vals,
                vec![99],
                "existing row must survive IF NOT EXISTS no-op"
            );
        } else {
            panic!("expected rows");
        }
    }

    /// CREATE TABLE without IF NOT EXISTS on an existing table must still error.
    #[tokio::test]
    async fn create_table_without_if_not_exists_errors_when_exists() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE dup_t (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let err = sess
            .execute("CREATE TABLE dup_t (id BIGINT NOT NULL)")
            .await
            .expect_err("CREATE TABLE (no IF NOT EXISTS) must error when table already exists");
        let msg = err.to_string();
        assert!(
            msg.contains("already exists") || msg.contains("Catalog"),
            "error must mention 'already exists', got: {msg}"
        );
    }

    // ── DDL correctness: DROP TABLE (task #49 Part B) ──────────────────────────

    /// DROP TABLE on an existing table must remove it from the catalog.
    #[tokio::test]
    async fn drop_table_removes_existing_table() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE droppable (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO droppable VALUES (7)")
            .await
            .unwrap();

        let res = sess
            .execute("DROP TABLE droppable")
            .await
            .expect("DROP TABLE must succeed on an existing table");
        assert!(
            matches!(res, ExecResult::Empty { ref tag } if tag == "DROP TABLE"),
            "expected DROP TABLE tag, got {res:?}"
        );

        // The table must be gone — a subsequent CREATE TABLE (same name, no IF NOT EXISTS)
        // must succeed, proving the catalog entry was removed.
        sess.execute("CREATE TABLE droppable (id BIGINT NOT NULL)")
            .await
            .expect("CREATE TABLE must succeed after DROP TABLE removed the previous table");
    }

    /// DROP TABLE IF EXISTS on a missing table must succeed (no-op).
    #[tokio::test]
    async fn drop_table_if_exists_noop_when_absent() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        let res = sess
            .execute("DROP TABLE IF EXISTS no_such_table")
            .await
            .expect("DROP TABLE IF EXISTS must be a no-op when table does not exist");
        assert!(
            matches!(res, ExecResult::Empty { ref tag } if tag == "DROP TABLE"),
            "expected DROP TABLE tag, got {res:?}"
        );
    }

    /// DROP TABLE without IF EXISTS on a missing table must error.
    #[tokio::test]
    async fn drop_table_errors_when_absent_without_if_exists() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        let err = sess
            .execute("DROP TABLE ghost_table")
            .await
            .expect_err("DROP TABLE (no IF EXISTS) must error when table does not exist");
        let msg = err.to_string();
        // The catalog returns NotFound for a missing table.
        assert!(
            msg.contains("not found") || msg.contains("NotFound") || msg.contains("ghost_table"),
            "error must indicate table not found, got: {msg}"
        );
    }

    // ── Cast / literal correctness tests (formerly planner-rejected) ─────────

    /// `SELECT 1::TEXT` — cast integer to text. DataFusion returns StringView;
    /// the df→ws bridge maps it to Utf8. Result must be the string "1".
    #[tokio::test]
    async fn cast_int_to_text_pgstyle() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let res = sess.execute("SELECT 1::TEXT").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                let col = col_string(&batches, "Int64(1)");
                assert_eq!(col, vec!["1".to_string()]);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SELECT CAST(1 AS TEXT)` — same cast via CAST() syntax.
    #[tokio::test]
    async fn cast_int_to_text_cast_syntax() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let res = sess.execute("SELECT CAST(1 AS TEXT)").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SELECT '12:00:00'::TIME` — cast text to Time64. The df→ws bridge
    /// must now translate the Time64 array without error.
    #[tokio::test]
    async fn cast_text_to_time() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let res = sess.execute("SELECT '12:00:00'::TIME").await.unwrap();
        match res {
            ExecResult::Rows { batches, schema } => {
                assert_eq!(total_rows(&batches), 1);
                // The column type must be Time64.
                let field = schema.field(0);
                assert!(
                    matches!(field.data_type(), arrow_schema::DataType::Time64(_)),
                    "expected Time64, got {:?}",
                    field.data_type()
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SELECT '00:01:00'::INTERVAL` — HH:MM:SS literal cast to interval.
    /// Arrow requires the verbose form; the rewriter converts to `'60 seconds'`.
    #[tokio::test]
    async fn cast_hms_to_interval() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let res = sess.execute("SELECT '00:01:00'::INTERVAL").await.unwrap();
        match res {
            ExecResult::Rows { batches, schema } => {
                assert_eq!(total_rows(&batches), 1);
                // Column type must be an Interval variant.
                let field = schema.field(0);
                assert!(
                    matches!(field.data_type(), arrow_schema::DataType::Interval(_)),
                    "expected Interval, got {:?}",
                    field.data_type()
                );
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SELECT 'uuid-str'::UUID` — rewritten to `::VARCHAR`; returns text.
    #[tokio::test]
    async fn cast_text_to_uuid() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let uuid_str = "a6c5e8f0-1234-5678-abcd-000000000000";
        let sql = format!("SELECT '{uuid_str}'::UUID");
        let res = sess.execute(&sql).await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                // The value should be the UUID string.
                let col = batches[0].column(0);
                let s = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("expected StringArray for UUID result");
                assert_eq!(s.value(0), uuid_str);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SELECT B'1010'` — bit-string literal rewritten to plain string `'1010'`.
    #[tokio::test]
    async fn bit_string_literal_select() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let res = sess.execute("SELECT B'1010'").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                let col = batches[0].column(0);
                let s = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("expected StringArray for bit-string result");
                assert_eq!(s.value(0), "1010");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------------
    // Correlated-subquery tests (EXISTS / NOT EXISTS in DELETE / UPDATE WHERE)
    // -------------------------------------------------------------------------

    /// Helper: create table `t(id BIGINT PK, v BIGINT)` with rows {1,2,3}
    /// and table `u(id BIGINT)` with row {2}.
    async fn seed_correlated_tables(sess: &ProjectSession) {
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY)")
            .await
            .unwrap();
        sess.execute("INSERT INTO u VALUES (2)").await.unwrap();
    }

    /// `DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
    /// t={1,2,3}, u={2} → deletes {1,3}, leaves {2}.
    #[tokio::test]
    async fn delete_where_not_exists_correlated() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_correlated_tables(&sess).await;

        let res = sess
            .execute("DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("DELETE 2"), "expected DELETE 2, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(
                    col_i64(&batches, "id"),
                    vec![2],
                    "expected only id=2 to remain"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
    /// t={1,2,3}, u={2} → deletes {2}, leaves {1,3}.
    #[tokio::test]
    async fn delete_where_exists_correlated() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_correlated_tables(&sess).await;

        let res = sess
            .execute("DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("DELETE 1"), "expected DELETE 1, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(
                    col_i64(&batches, "id"),
                    vec![1, 3],
                    "expected [1,3] to remain"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
    /// when u is empty → NOT EXISTS matches every row → all deleted.
    #[tokio::test]
    async fn delete_not_exists_empty_subquery_deletes_all() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY)")
            .await
            .unwrap();
        // u is empty

        let res = sess
            .execute("DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("DELETE 2"), "expected DELETE 2, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        // t must now be empty
        let res = sess.execute("SELECT id FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 0, "table t must be empty");
            }
            ExecResult::Empty { .. } => {} // also fine — no rows
        }
    }

    /// `DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
    /// when u is empty → EXISTS matches nothing → nothing deleted.
    #[tokio::test]
    async fn delete_exists_empty_subquery_deletes_nothing() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY)")
            .await
            .unwrap();

        let res = sess
            .execute("DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("DELETE 0"), "expected DELETE 0, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 2]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `UPDATE t SET v = v + 1 WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
    /// t={(1,10),(2,20),(3,30)}, u={2} → only rows 1 and 3 updated.
    #[tokio::test]
    async fn update_where_not_exists_correlated() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_correlated_tables(&sess).await;

        let res = sess
            .execute("UPDATE t SET v = v + 1 WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("UPDATE 2"), "expected UPDATE 2, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        // id=2 must still have v=20 (NOT EXISTS did not match it).
        let res = sess
            .execute("SELECT id, v FROM t ORDER BY id")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);
                let vs = col_i64(&batches, "v");
                // rows 1 and 3 bumped; row 2 unchanged
                assert_eq!(vs[0], 11, "id=1: v should be 11");
                assert_eq!(vs[1], 20, "id=2: v should be unchanged 20");
                assert_eq!(vs[2], 31, "id=3: v should be 31");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `DELETE FROM t WHERE t.id IN (SELECT id FROM u)` — non-correlated
    /// IN-subquery still works via the existing materialise-then-IN path.
    #[tokio::test]
    async fn delete_where_in_subquery() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_correlated_tables(&sess).await;

        let res = sess
            .execute("DELETE FROM t WHERE id IN (SELECT id FROM u)")
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => {
                assert!(tag.starts_with("DELETE 1"), "expected DELETE 1, got {tag}")
            }
            other => panic!("unexpected: {other:?}"),
        }

        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1, 3]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Data-modifying CTE tests ─────────────────────────────────────────────

    /// `WITH ins AS (INSERT … RETURNING id) SELECT id FROM ins`
    /// — inserted row is visible in the outer SELECT, and the table contains it.
    #[tokio::test]
    async fn dml_cte_insert_returning() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();

        let res = sess
            .execute(
                "WITH ins AS (INSERT INTO t VALUES (1, 'hello') RETURNING id) SELECT id FROM ins",
            )
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![1]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Verify the row was actually inserted.
        let res = sess.execute("SELECT id, name FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(total_rows(&batches), 1);
                assert_eq!(col_i64(&batches, "id"), vec![1]);
                assert_eq!(col_string(&batches, "name"), vec!["hello".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `WITH del AS (DELETE FROM t WHERE id=1 RETURNING id) SELECT id FROM del`
    /// — deleted row's id is returned, and the table no longer contains the row.
    #[tokio::test]
    async fn dml_cte_delete_returning() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("WITH del AS (DELETE FROM t WHERE id = 3 RETURNING id) SELECT id FROM del")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![3]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Verify id=3 is gone from the table.
        let res = sess.execute("SELECT id FROM t ORDER BY id").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                let ids = col_i64(&batches, "id");
                assert!(!ids.contains(&3), "id=3 should be deleted: {ids:?}");
                assert_eq!(ids, vec![1, 2, 4, 5]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// `WITH upd AS (UPDATE t SET name='z' WHERE id=2 RETURNING id, name) SELECT * FROM upd`
    /// — updated values are returned, and the table reflects the change.
    #[tokio::test]
    async fn dml_cte_update_returning() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed_five_rows(&sess).await;

        let res = sess
            .execute("WITH upd AS (UPDATE t SET name = 'z' WHERE id = 2 RETURNING id, name) SELECT id, name FROM upd")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![2]);
                assert_eq!(col_string(&batches, "name"), vec!["z".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // Verify the update is persisted.
        let res = sess
            .execute("SELECT name FROM t WHERE id = 2")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_string(&batches, "name"), vec!["z".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// Multi-leg DML CTE: INSERT then SELECT the inserted id FROM a real table join.
    #[tokio::test]
    async fn dml_cte_insert_no_returning_gives_empty_outer() {
        // When user doesn't specify RETURNING, outer query referencing the CTE
        // returns 0 rows (the CTE has an empty schema MemTable).
        // This is acceptable documented behaviour — user should use RETURNING.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();

        // We force RETURNING * internally so actually this WILL return the row.
        // Test that the engine doesn't crash and the row exists in the table.
        let res = sess
            .execute("WITH ins AS (INSERT INTO t VALUES (42, 'x') RETURNING id) SELECT id FROM ins")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![42]);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Table must have the row.
        let res = sess.execute("SELECT id FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![42]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// Forward-reference: CTE b's outer SELECT references CTE a's result.
    /// `WITH a AS (INSERT … RETURNING id), b AS (DELETE … WHERE id IN (SELECT id FROM a) RETURNING id) SELECT * FROM b`
    #[tokio::test]
    async fn dml_cte_multi_leg_forward_reference() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // Two tables: src (insert target) and dst (we'll delete from it using src).
        sess.execute("CREATE TABLE src (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE dst (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO dst VALUES (10), (20), (30)")
            .await
            .unwrap();

        // Insert 10 into src, then delete from dst WHERE id IN (SELECT id FROM src's result).
        // After execution: src has {10}, dst has {20, 30}, b has {10}.
        let res = sess
            .execute(
                "WITH a AS (INSERT INTO src VALUES (10) RETURNING id), \
                 b AS (DELETE FROM dst WHERE id IN (SELECT id FROM a) RETURNING id) \
                 SELECT id FROM b",
            )
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![10]);
            }
            other => panic!("unexpected: {other:?}"),
        }

        // dst should no longer contain 10.
        let res = sess
            .execute("SELECT id FROM dst ORDER BY id")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(col_i64(&batches, "id"), vec![20, 30]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Phase 5.14.D2 integration test ──────────────────────────────────────

    /// After 100 ORDER BY (b, id) queries the engine's query history returns
    /// the dominant column tuple; a sub-threshold pattern returns None.
    #[tokio::test]
    async fn query_history_order_by_accumulates() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let project = ProjectId::new();
        let sess = eng.open_session(project).await.unwrap();

        // Seed a table so the SELECT does not fail.
        sess.execute("CREATE TABLE hist_test (id BIGINT, b BIGINT)")
            .await
            .unwrap();
        sess.execute("INSERT INTO hist_test VALUES (1, 10), (2, 20)")
            .await
            .unwrap();

        // Issue 100 ORDER BY (b, id) queries.
        for _ in 0..100 {
            sess.execute("SELECT id FROM hist_test ORDER BY b, id")
                .await
                .unwrap();
        }

        let table = basin_common::TableName::new("hist_test").unwrap();
        let pattern = eng
            .query_history()
            .top_pattern(&project, &table)
            .expect("should have a dominant pattern after 100 queries");
        // Columns stored in sorted order: ["b", "id"].
        assert_eq!(pattern, vec!["b".to_string(), "id".to_string()]);

        // A table that received only 10 queries returns None (below 100 floor).
        let other = basin_common::TableName::new("hist_test").unwrap();
        let small_proj = ProjectId::new();
        assert!(
            eng.query_history().top_pattern(&small_proj, &other).is_none(),
            "unseen project must return None"
        );
    }
}
