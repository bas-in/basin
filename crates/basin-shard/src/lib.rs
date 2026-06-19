//! `basin-shard` — stateful shard owner.
//!
//! ## Status
//!
//! v0.1: **in-process map** of `(project_id, partition_key) → in-memory state`,
//! with lazy load from WAL + Parquet, idle eviction, and a background
//! compactor that drains WAL segments into Parquet via the storage layer
//! and commits via the catalog.
//!
//! v0.2 (deferred): a placement service that spreads projects across many
//! shard-owner processes. Today everything runs in-process; the API is
//! shaped so that swap is a backend change.
//!
//! ## Why this exists
//!
//! Two dashboard cards fail honestly today:
//! - `scaling_concurrency`: 3.4× speedup at 64 readers (bar 4×). Single
//!   process saturates one runtime.
//! - `scaling_noisy_neighbor`: 42× p99 degradation under a noisy project.
//!
//! Shard owners give each project their own slice of in-memory state plus
//! eviction; the noisy project's full scans no longer block the quiet
//! project's point queries because they live in different per-project
//! data structures and run on separate task groups.
//!
//! ## Public API
//!
//! - [`Shard`]: cheap-to-clone handle.
//! - [`ShardConfig`]: storage + catalog + WAL + eviction knobs.
//! - [`ProjectHandle`]: lazy-loaded per-project state — write/read.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_common::{PartitionKey, ProjectId, Result, TableName};

/// Default cadence of the hot-tier clean-retention sweep, seconds
/// (`BASIN_HOTTIER_SWEEP_SECS`).
const DEFAULT_CLEAN_SWEEP_SECS: u64 = 30;

/// Default cadence of the stripe-merge compaction sweep, seconds
/// (`BASIN_STRIPE_MERGE_SECS`). `0` disables the sweep entirely.
const DEFAULT_STRIPE_MERGE_SECS: u64 = 60;

/// Multi-node phase 1 (ADR 0023) — lease enforcement mode, sourced from
/// `BASIN_LEASE_MODE` by `basin-server`.
///
/// - [`LeaseMode::Off`] (default): exactly today's behaviour. With no
///   registry attached nothing fences; with a registry attached the shard
///   acquires/renews/fences best-effort (the Phase 6.X.A semantics every
///   existing lease test pins): `get` errors with `CommitConflict` when a
///   peer holds a live lease, and a write that lost its lease falls back to
///   an unfenced (epoch-`None`) WAL append.
/// - [`LeaseMode::Required`]: single-writer is **enforced**, not just
///   deployment-shape. A write may only proceed while this replica holds the
///   `(project, partition)` writer lease — no held epoch means the write is
///   refused with the typed [`basin_common::BasinError::LeaseNotHeld`]
///   before it reaches the WAL. Lease-acquire failures (peer holds it /
///   coordinator unreachable) fail **closed for writes** and **open for
///   reads**: `get` still returns a handle so reads continue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LeaseMode {
    /// No enforcement — current single-replica / best-effort behaviour.
    #[default]
    Off,
    /// Writer lease must be held for every write; fail-closed otherwise.
    Required,
}

impl LeaseMode {
    /// Parse a `BASIN_LEASE_MODE` value. `None` / empty / `"off"` is
    /// [`LeaseMode::Off`]; `"required"` is [`LeaseMode::Required`]; anything
    /// else is a hard error (mirrors
    /// `basin_router::parse_lease_cache_ttl_env`'s strict-parse idiom — a
    /// typo'd enforcement knob must not silently mean "off").
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("off") => Ok(Self::Off),
            Some("required") => Ok(Self::Required),
            Some(other) => Err(format!(
                "BASIN_LEASE_MODE must be 'off' or 'required', got {other:?}"
            )),
        }
    }

    /// [`Self::parse`] applied to the `BASIN_LEASE_MODE` env var.
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::var("BASIN_LEASE_MODE").ok().as_deref())
    }
}

/// Knobs for [`Shard::new`].
#[derive(Clone)]
pub struct ShardConfig {
    pub storage: basin_storage::Storage,
    pub catalog: Arc<dyn basin_catalog::Catalog>,
    pub wal: Arc<dyn basin_wal::Wal>,
    /// Projects idle for at least this long are evicted by the eviction loop.
    /// Default 5 minutes.
    pub eviction_idle: Duration,
    /// How often the compactor checks for WAL segments to drain. Default 30s.
    pub compaction_interval: Duration,
    /// How often the eviction loop runs. Default 60s.
    pub eviction_interval: Duration,
    /// S4 age-based residency: how often the background loop runs the
    /// hot-tier clean-retention sweep
    /// ([`basin_hottier::MemTableRegistry::enforce_clean_budgets`]) against
    /// the registry wired in via [`Shard::set_memtable_registry`]. The sweep
    /// only evicts redundant clean (flushed-and-retained) rows — it never
    /// flushes; the shard compactor remains the sole durable flusher.
    /// Default 30 s (`BASIN_HOTTIER_SWEEP_SECS`).
    pub clean_sweep_interval: Duration,
    /// How often the background loop runs the stripe-merge compaction sweep
    /// — merging a table's overlapping per-stripe cold files into fewer
    /// files with disjoint PK ranges so keyset pagination
    /// (`WHERE pk > $1 ORDER BY pk LIMIT k`) prunes to 1-2 file opens
    /// instead of opening every stripe file. Default 60 s
    /// (`BASIN_STRIPE_MERGE_SECS`); a zero duration (env value `0`)
    /// disables the sweep.
    pub stripe_merge_interval: Duration,
    /// Phase 6.X.A — optional lease registry (ADR 0023). When present, the
    /// shard acquires a lease per `(project, partition)` it owns and the
    /// background heartbeat renews held leases on a fixed cadence; a renewal
    /// failure (lost lease) drops the partition's in-memory state. When
    /// absent, the shard runs in single-replica / no-lease mode exactly as
    /// before — `get` neither acquires nor fences.
    pub lease_registry: Option<Arc<dyn basin_catalog::LeaseRegistry>>,
    /// Multi-node phase 1 — lease enforcement mode (`BASIN_LEASE_MODE`).
    /// [`LeaseMode::Off`] (default) preserves today's behaviour exactly;
    /// [`LeaseMode::Required`] refuses writes with
    /// [`basin_common::BasinError::LeaseNotHeld`] whenever this replica does
    /// not hold the partition's writer lease, while reads continue. Only
    /// meaningful when `lease_registry` is set (required mode without a
    /// registry refuses every write — `basin-server` rejects that combination
    /// at startup).
    pub lease_mode: LeaseMode,
    /// This replica's stable lease holder id (e.g. `host:pid` or a uuid).
    /// Defaults to a per-process random id; only meaningful when
    /// `lease_registry` is set.
    pub replica_id: String,
    /// Lease TTL handed to `acquire` / `renew`. Default 15 s
    /// (`BASIN_LEASE_TTL_SECS`).
    pub lease_ttl: Duration,
    /// Heartbeat renew cadence. Default 5 s (`BASIN_LEASE_RENEW_SECS`).
    pub lease_renew_interval: Duration,
    /// Phase 6.X.D — optional budget coordinator (ADR 0023). When attached,
    /// the lease heartbeat tick *also* pushes per-`(project, partition)`
    /// usage deltas to the coordinator and writes the returned slice budget
    /// into each registered [`basin_catalog::SliceBudgetView`]. v1 ships
    /// zero-delta heartbeats — the slice itself is the load-bearing
    /// cross-replica primitive; per-cap usage telemetry feeds the
    /// coordinator in a follow-on once each cap consumer exposes an
    /// observable counter. **Closes the multi-instance cap-bypass P0.**
    pub budget_coordinator: Option<Arc<dyn basin_catalog::BudgetCoordinator>>,
    /// Phase 6.X.D — slice views the heartbeat tick should push slices into.
    /// Same [`basin_catalog::SliceBudgetView`] handle each cap consumer was
    /// built with; the heartbeat fans the coordinator's slice answer out to
    /// every registered view (typically one per cap consumer crate).
    pub slice_views: Vec<basin_catalog::SliceBudgetView>,
    /// Phase 6.X.D — slice gates whose token counters should be refilled
    /// after every heartbeat tick. Refilling is necessary for the qps-style
    /// caps (`RestQps` / `PgQps` / `NetOutboundQps`) where the slice is
    /// "tokens per heartbeat window" — without periodic refill the gate
    /// would drain and stay drained forever. Byte-style caps
    /// (`MemtableBytes` / `RealtimeBytes`) read the slice directly via
    /// [`basin_catalog::SliceBudgetView::slice_for_sync`] and don't need
    /// the gate refill.
    pub slice_gates: Vec<basin_catalog::SliceGate>,
    /// Phase 6.X.F — optional lease observability sink (ADR 0023). When
    /// attached, the lease acquire / renew / heartbeat sites bump the
    /// corresponding counters; absent it, the metric emission sites are
    /// no-ops (zero-cost back-compat). One handle per process; consumers
    /// snapshot it via [`basin_common::LeaseMetrics::snapshot_replicas`]
    /// and [`basin_common::LeaseMetrics::snapshot_over_cap`].
    pub lease_metrics: Option<Arc<basin_common::LeaseMetrics>>,
}

impl ShardConfig {
    pub fn new(
        storage: basin_storage::Storage,
        catalog: Arc<dyn basin_catalog::Catalog>,
        wal: Arc<dyn basin_wal::Wal>,
    ) -> Self {
        Self {
            storage,
            catalog,
            wal,
            eviction_idle: Duration::from_secs(300),
            compaction_interval: Duration::from_secs(30),
            eviction_interval: Duration::from_secs(60),
            clean_sweep_interval: Duration::from_secs(env_secs(
                "BASIN_HOTTIER_SWEEP_SECS",
                DEFAULT_CLEAN_SWEEP_SECS,
            )),
            stripe_merge_interval: Duration::from_secs(env_secs(
                "BASIN_STRIPE_MERGE_SECS",
                DEFAULT_STRIPE_MERGE_SECS,
            )),
            lease_registry: None,
            lease_mode: LeaseMode::Off,
            replica_id: default_replica_id(),
            lease_ttl: Duration::from_secs(env_secs(
                "BASIN_LEASE_TTL_SECS",
                basin_catalog::DEFAULT_LEASE_TTL_SECS,
            )),
            lease_renew_interval: Duration::from_secs(env_secs(
                "BASIN_LEASE_RENEW_SECS",
                basin_catalog::DEFAULT_LEASE_RENEW_SECS,
            )),
            budget_coordinator: None,
            slice_views: Vec::new(),
            slice_gates: Vec::new(),
            lease_metrics: None,
        }
    }

    /// Phase 6.X.F — attach the observability sink. The lease event sites
    /// then bump counters on `lease_metrics`; consumers (basin-cloud
    /// exporter, `/metrics` endpoint) snapshot via
    /// [`basin_common::LeaseMetrics::snapshot_replicas`]. Optional —
    /// shards without a metrics handle behave identically.
    pub fn with_lease_metrics(mut self, metrics: Arc<basin_common::LeaseMetrics>) -> Self {
        self.lease_metrics = Some(metrics);
        self
    }

    /// Phase 6.X.D — attach a budget coordinator + the cap consumers'
    /// slice views/gates. The lease heartbeat then becomes the budget
    /// heartbeat too: each tick pushes a usage delta and writes the slice
    /// answer back into every registered view.
    pub fn with_budget_coordinator(
        mut self,
        coordinator: Arc<dyn basin_catalog::BudgetCoordinator>,
        views: Vec<basin_catalog::SliceBudgetView>,
        gates: Vec<basin_catalog::SliceGate>,
    ) -> Self {
        self.budget_coordinator = Some(coordinator);
        self.slice_views = views;
        self.slice_gates = gates;
        self
    }

    /// Attach a lease registry + holder id so [`Shard::spawn_background`] runs
    /// the per-partition lease heartbeat (Phase 6.X.A, ADR 0023).
    pub fn with_lease_registry(
        mut self,
        registry: Arc<dyn basin_catalog::LeaseRegistry>,
        replica_id: impl Into<String>,
    ) -> Self {
        self.lease_registry = Some(registry);
        self.replica_id = replica_id.into();
        self
    }

    /// Multi-node phase 1 — set the lease enforcement mode
    /// (`BASIN_LEASE_MODE`). See [`LeaseMode`] for the exact semantics of
    /// each value. Call together with [`Self::with_lease_registry`];
    /// [`LeaseMode::Required`] without a registry refuses every write.
    pub fn with_lease_mode(mut self, mode: LeaseMode) -> Self {
        self.lease_mode = mode;
        self
    }
}

/// Stats. Implementations push these out via tracing for the dashboard.
#[derive(Clone, Debug, Default)]
pub struct ShardStats {
    pub resident_projects: usize,
    pub resident_partitions: usize,
    pub evictions: u64,
    pub compactions: u64,
    pub bytes_in_wal: u64,
    /// Phase 5.14.D2: count of compactions where the output file was sorted by
    /// the query-history top-K pattern rather than the declared `cluster_columns`.
    pub compactions_with_adaptive_sort: u64,
    /// S4 age-based residency: cumulative bytes of redundant clean
    /// (flushed-and-retained) hot-tier rows evicted by the background
    /// clean-retention sweep. Stays 0 until a registry is wired in via
    /// [`Shard::set_memtable_registry`].
    pub clean_sweep_evictions_bytes: u64,
    /// Count of completed stripe-merge compaction passes (one per
    /// `(project, table)` whose overlapping cold files were merged into
    /// fewer files with disjoint PK ranges and committed).
    pub stripe_merges: u64,
}

/// Handle to the shard map. Cheap to clone (Arc inside).
#[derive(Clone)]
pub struct Shard {
    inner: Arc<dyn ShardImpl>,
}

impl Shard {
    pub fn new(cfg: ShardConfig) -> Self {
        let inner = in_process::InProcessShard::new(cfg);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Handle to the underlying WAL. Exposed so the engine can plumb its
    /// per-project counter registry into the WAL alongside the storage layer.
    pub fn wal(&self) -> &Arc<dyn basin_wal::Wal> {
        self.inner.wal()
    }

    /// This replica's stable lease holder id. Used by the engine's
    /// partition-write owner resolution to identify "self" within the shared
    /// `BASIN_SHARD_PEERS` list.
    pub fn replica_id(&self) -> &str {
        self.inner.replica_id()
    }

    /// The configured lease registry, if any. The engine's partition
    /// forwarding consults this to confirm/observe the live owner of a
    /// partition. `None` in single-replica / no-lease mode.
    pub fn lease_registry(&self) -> Option<&Arc<dyn basin_catalog::LeaseRegistry>> {
        self.inner.lease_registry()
    }

    /// Get a handle to `(project, partition)`. Lazy-loads the state from WAL +
    /// Parquet on first access; subsequent calls return a cheap clone.
    pub async fn get(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
    ) -> Result<ProjectHandle> {
        self.inner.get(project, partition).await
    }

    /// Spawn the eviction + compaction + clean-retention-sweep background
    /// loops. Returns a handle that, when dropped, signals shutdown. Call
    /// this once at server boot. May be called before
    /// [`Shard::set_memtable_registry`] — the sweep picks up the registry
    /// on its next tick after wiring.
    pub fn spawn_background(&self) -> ShardBackgroundHandle {
        self.inner.clone_arc().spawn_background()
    }

    pub fn stats(&self) -> ShardStats {
        self.inner.stats()
    }

    /// Synchronously drain every resident partition's in-memory tail into
    /// Parquet, committing through the catalog and truncating the WAL.
    ///
    /// Engines that share read paths with batch analytics use this to make a
    /// just-written batch visible to a downstream `SELECT` that reads through
    /// the Parquet base (no tail-merge), trading a small latency hit on the
    /// query for not having to teach the analytical pipeline about tails.
    pub async fn flush_to_parquet(&self) -> Result<()> {
        self.inner.flush_to_parquet().await
    }

    /// Run one stripe-merge sweep synchronously (merge cold files with
    /// overlapping PK zone maps into disjoint sorted runs). The background
    /// tick does this on `BASIN_STRIPE_MERGE_SECS` cadence; benchmarks and
    /// tests that quiesce the background loops call this once to reach the
    /// same steady-state file layout deterministically.
    pub async fn run_stripe_merge_once(&self) -> Result<()> {
        self.inner.run_stripe_merge_once().await
    }

    /// Run one pass of the tiered-storage sweep. For every `(project, table)`
    /// the shard has touched (i.e. has resident state for, OR has been seen
    /// to live in the catalog under a known project), copy data files older
    /// than `cold_after_seconds` into the cold tier and atomically swap the
    /// catalog over.
    ///
    /// Exposed for tests and one-shot ops; the in-process compactor will
    /// also call this from its background tick once we surface a tier
    /// interval knob (TODO: align with `compaction_interval`).
    pub async fn run_tiering_sweep(&self) -> Result<()> {
        self.inner.run_tiering_sweep().await
    }

    /// ADR 0027 Phase 4 — rewrite every cold-tier data file for `(project,
    /// table)` that is missing the table's promoted JSONB shadow column(s).
    ///
    /// This is what a Basin background compactor does over time ("eventual
    /// backfill").  Calling this eagerly models the steady state of a live
    /// deployment where the compactor has had time to cover every file.
    ///
    /// Idempotent: files that already carry all shadow columns are skipped.
    /// Returns the count of files that were rewritten.
    ///
    /// The bench calls this after `flush_to_parquet()` in the JSONB
    /// steady-state path to ensure all seeded cold-tier files carry the
    /// shadow column before the timed measurement.
    pub async fn run_promoted_column_backfill_sweep(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<usize> {
        self.inner
            .run_promoted_column_backfill_sweep(project, table)
            .await
    }

    /// Run one CV refresh sweep across every resident project. Returns the
    /// count of CVs that were re-materialised this pass.
    ///
    /// The shard owner is the natural choice of driver because it already
    /// has the per-project residency map; reusing it avoids a parallel
    /// "list of resident projects" structure inside `basin-cv`. The actual
    /// refresh logic lives in [`basin_cv::CvRefresher`] — this method
    /// just walks the resident project set, calls the supplied
    /// [`CvRefreshDriver`] once per project, and sums the resulting
    /// `refreshed` counts.
    ///
    /// `driver` is normally a `&basin_cv::CvRefresher`, which implements
    /// [`CvRefreshDriver`] in the basin-cv crate. The trait is declared
    /// here rather than in basin-cv to keep the dependency graph one-way:
    /// `basin-engine -> basin-shard`, `basin-cv -> basin-engine ->
    /// basin-shard`, and `basin-shard` itself stays leaf-free.
    ///
    /// Mirrors [`Shard::run_tiering_sweep`]'s shape so a future production
    /// driver that ticks both at the same cadence has a uniform API.
    pub async fn run_cv_refresh<D: CvRefreshDriver>(&self, driver: &D) -> Result<usize> {
        let projects = self.inner.resident_projects().await;
        let mut total = 0usize;
        for t in &projects {
            total = total.saturating_add(driver.refresh_project(t).await?);
        }
        Ok(total)
    }

    /// Phase 5.14.D2: register the adaptive-sort query-pattern provider.
    /// The engine calls this once at startup so the compactor can detect
    /// common ORDER BY / GROUP BY access patterns and pre-sort files.
    pub fn set_top_pattern_provider(&self, provider: Arc<dyn TopPatternProvider>) {
        self.inner.set_top_pattern_provider(provider);
    }

    /// Wire the engine's HTAP hot-tier memtable registry into the background
    /// clean-retention sweep (S4 age-based residency).
    ///
    /// Called once from `Engine::new`. The sweep tick then runs
    /// [`basin_hottier::MemTableRegistry::enforce_clean_budgets`] every
    /// [`ShardConfig::clean_sweep_interval`], evicting clean rows that have
    /// outlived `BASIN_HOTTIER_RETAIN_SECS` or exceed the per-project clean
    /// cap. Clean rows are byte-identical to their cold images, so eviction
    /// is read-transparent — nothing is flushed here; the shard compactor
    /// remains the sole durable flusher.
    ///
    /// Safe to call after [`Shard::spawn_background`] (the production
    /// order — the server spawns the loops before the engine exists): the
    /// sweep re-reads the wiring cell on every tick.
    pub fn set_memtable_registry(&self, registry: Arc<basin_hottier::MemTableRegistry>) {
        self.inner.set_memtable_registry(registry);
    }

    /// Wire the engine's GIN row-group bloom registry into the compactor.
    ///
    /// Called once from `Engine::new` so the compactor can re-index compacted
    /// files into the same registry the engine's `@>` prune reads from.  On
    /// the shard path, INSERT writes go directly to WAL + tail (bypassing the
    /// engine's `maintain_secondary_indexes_on_insert`), so compaction is the
    /// only point where GIN row-group summaries can be populated for
    /// compacted files.  Without this, the completeness guard in
    /// `apply_gin_pruning_for_query` would always see un-indexed live files
    /// and fall back to a full scan.
    pub fn set_gin_rowgroup_registry(
        &self,
        registry: Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    ) {
        self.inner.set_gin_rowgroup_registry(registry);
    }

    /// Wire the engine's JSONB posting-list registry into the compactor
    /// (Inv-W5 / W9).  Same lifecycle as the GIN row-group registry above;
    /// see `set_gin_rowgroup_registry` for rationale.
    pub fn set_jsonb_posting_registry(
        &self,
        registry: Arc<basin_storage::index::jsonb_posting::JsonbPostingRegistry>,
    ) {
        self.inner.set_jsonb_posting_registry(registry);
    }

    /// Wire the engine's secondary B-tree index registry into the compactor.
    ///
    /// Called once from `Engine::new`. On the shard path INSERT writes go
    /// straight to WAL + tail (bypassing the engine's
    /// `maintain_secondary_indexes_on_insert`), so compaction is the only
    /// point where a single-column index's `(value → file/row-group/row)`
    /// locations can be populated for shard-written data. Without this the
    /// secondary-index probe never has entries under any compacted file path
    /// and every point query falls back to a full scan.
    pub fn set_secondary_index_registry(&self, registry: Arc<dyn SecondaryIndexSink>) {
        self.inner.set_secondary_index_registry(registry);
    }

    /// Phase 6.X.C (ADR 0023) — voluntarily hand off the lease for
    /// `(project, partition)` to `to_holder`. While the handoff is in
    /// progress new writes against this partition fail with
    /// [`basin_common::BasinError::LeaseHandoffInProgress`] so the router
    /// can retry against the new owner; reads continue. On completion the
    /// outgoing replica releases the lease, the candidate `acquire`s under
    /// a fresh epoch, and replay of the WAL on the new owner picks up any
    /// post-handoff entries.
    ///
    /// Target: **< 500 ms p99 stall** for the yielded partition
    /// (small-memtable case). Larger memtables are dominated by the
    /// cold-tier write step; operators should size partitions accordingly.
    /// See the `InProcessShard::yield_partition` docstring for the full
    /// state machine.
    ///
    /// No-op (returns `Ok`) if this replica doesn't hold the lease, or if
    /// no lease registry is configured.
    pub async fn yield_partition(
        &self,
        project: &ProjectId,
        partition: &PartitionKey,
        to_holder: &str,
    ) -> Result<()> {
        self.inner
            .yield_partition(project, partition, to_holder)
            .await
    }

    /// Drop the shard's in-memory tail (and cached schema) for
    /// `(project, table)` across every resident partition.
    ///
    /// SECURITY (sec_catalog_leakage P1): without this, rows that were
    /// INSERTed into a table and never flushed past the shard's tail
    /// survive `DROP TABLE` — the catalog row is removed but the read
    /// path still merges the tail batches over the (now-absent) cold
    /// tier, leaking data to a fresh session that re-issues
    /// `SELECT * FROM <dropped>` (or even re-creates the same table
    /// name). `exec_drop_table` calls this immediately after the
    /// catalog-side `drop_table` succeeds.
    pub async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()> {
        self.inner.drop_table(project, table).await
    }

    /// Cheap O(resident-partitions) predicate: does any resident partition
    /// hold un-flushed in-memory tail rows for `(project, table)`?
    ///
    /// This is the no-flush correctness signal a read path uses to decide
    /// whether cold-file-only guarantees are authoritative.  It never lists
    /// or scans object storage and never drains the tail — it only inspects
    /// the resident per-partition tail maps. See [`ShardImpl::has_pending_tail`].
    pub async fn has_pending_tail(&self, project: &ProjectId, table: &TableName) -> bool {
        self.inner.has_pending_tail(project, table).await
    }

    /// Cheap O(resident-partitions) count of un-flushed in-memory tail rows for
    /// `(project, table)`, summed across every resident partition.
    ///
    /// Like [`Shard::has_pending_tail`] this never lists or scans object
    /// storage and never drains the tail — it only inspects the resident
    /// per-partition tail maps. The read path uses it to decide whether a
    /// small pending tail can be merged on-read (via the shard's own
    /// tail-merging `ProjectHandle::read`) instead of paying a synchronous
    /// flush. See [`ShardImpl::pending_tail_rows`].
    pub async fn pending_tail_rows(&self, project: &ProjectId, table: &TableName) -> usize {
        self.inner.pending_tail_rows(project, table).await
    }

    /// Cold-tier read for `(project, table)` unioned with the un-flushed
    /// in-memory tail of EVERY resident partition.
    ///
    /// This is the merge-on-read primitive the auto-commit read path uses when
    /// it skips the synchronous flush for a small pending tail: the cold side
    /// already spans every stripe partition's flushed data, and this merges
    /// the still-resident tail of `s1`, `s2`, … (not only `default_key()`), so
    /// read-own-write holds for striped multi-row INSERTs. See
    /// [`ShardImpl::read_table_merging_tails`].
    pub async fn read_table_merging_tails(
        &self,
        project: &ProjectId,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        self.inner
            .read_table_merging_tails(project, table, opts)
            .await
    }

    /// Test-only: pull out the concrete in-process implementation so the
    /// inline tests can drive its synchronous helpers. Returns `None` if a
    /// future backend swap replaces the in-process map.
    #[cfg(test)]
    pub(crate) fn impl_handle(&self) -> Option<Arc<in_process::InProcessShard>> {
        self.inner.as_in_process()
    }
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").finish_non_exhaustive()
    }
}

/// Per-project state handle. Holding one keeps the project resident.
#[derive(Clone)]
pub struct ProjectHandle {
    pub(crate) inner: Arc<dyn ProjectHandleImpl>,
}

impl ProjectHandle {
    /// Append a batch into this project's table under the **async** WAL
    /// contract (`synchronous_commit = off`, the default): the write is
    /// ack'd once it sits in the WAL's in-RAM buffer and becomes durable on
    /// the next WAL flush (200 ms tick / 1 MiB pressure / explicit flush).
    /// The batch is ack'd before it has reached Parquet.
    pub async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()> {
        self.inner.write_batch(table, batch).await
    }

    /// [`Self::write_batch`] with an explicit durability mode. `durable =
    /// false` is exactly `write_batch`. `durable = true` is the
    /// `synchronous_commit = on` path: the call only returns once the WAL
    /// has group-committed the entry to the backing store (segment PUT, plus
    /// fsync on an `FsyncOnPut`-wrapped local store) — ack-after-durable.
    /// The executor passes the session's `basin.synchronous_commit` value
    /// here.
    pub async fn write_batch_opts(
        &self,
        table: &TableName,
        batch: RecordBatch,
        durable: bool,
    ) -> Result<()> {
        if durable {
            self.inner.write_batch_durable(table, batch).await
        } else {
            self.inner.write_batch(table, batch).await
        }
    }

    /// Read all rows currently visible for a table — both the in-RAM tail
    /// (WAL-resident, not yet flushed) and the Parquet base — in one stream.
    pub async fn read(
        &self,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        self.inner.read(table, opts).await
    }

    pub fn last_active(&self) -> Instant {
        self.inner.last_active()
    }

    pub fn project(&self) -> ProjectId {
        self.inner.project()
    }
}

/// Returned from [`Shard::spawn_background`]; drop to stop the loops.
pub struct ShardBackgroundHandle {
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    pub(crate) join: tokio::task::JoinHandle<()>,
}

impl ShardBackgroundHandle {
    /// Stop the eviction + compaction loops, awaiting their completion.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.join.await;
    }
}

/// Sink for the per-`(project, table, col)` secondary B-tree index.
///
/// The concrete registry (`basin_engine::secondary_index::ProjectIndexRegistry`)
/// lives in `basin-engine`, which depends on `basin-shard` — so the compactor
/// cannot name that type directly without closing a dependency cycle. This
/// trait is the same escape hatch [`TopPatternProvider`] uses: the engine
/// implements it on its registry and wires it in via
/// [`Shard::set_secondary_index_registry`] at startup; the compactor calls it
/// when it writes a new compacted file.
///
/// Locations are passed as primitive tuples (`key`, `file_path`, `row_group`,
/// `row`) rather than the engine's `IndexLocation` for the same
/// dependency-direction reason.
pub trait SecondaryIndexSink: Send + Sync {
    /// Insert a batch of `(key_text, file_path, row_group, row)` locations
    /// into the in-RAM index for `(project, table, col)`. Mirrors
    /// `ProjectIndexRegistry::insert_batch`.
    fn insert_batch_locations(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        entries: Vec<(String, String, u32, u64)>,
    );

    /// Remove every index location that references `file_path` for
    /// `(project, table, col)`. Used when a compaction replaces files.
    /// Mirrors `ProjectIndexRegistry::remove_file_from_index`.
    fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    );
}

/// Driver the shard hands each resident project to during
/// [`Shard::run_cv_refresh`]. Implementations live in `basin-cv`
/// (`CvRefresher` is the canonical one); the trait sits in `basin-shard`
/// purely so `Shard::run_cv_refresh` can call it without taking a
/// Pluggable query-pattern provider for adaptive sort (Phase 5.14.D2).
/// Declared here so `basin-shard`'s compactor can call it without a
/// direct dependency on `basin-engine`. `basin-engine::QueryHistory`
/// implements this trait and registers itself via `Shard::set_top_pattern_provider`.
pub trait TopPatternProvider: Send + Sync {
    /// Return the most-frequently-observed GROUP BY / ORDER BY column tuple
    /// for `(project, table)`, or `None` when there is insufficient history.
    fn top_pattern(&self, project: &ProjectId, table: &TableName) -> Option<Vec<String>>;
    /// Record a divergence between the observed access pattern and the
    /// table's declared `CLUSTER BY` columns.  Used for telemetry only.
    fn record_cluster_delta(
        &self,
        project: &ProjectId,
        table: &TableName,
        observed: &[String],
        declared: &[String],
    );
}

/// `basin-cv` dependency (which would otherwise close a cycle through
/// `basin-engine`).
#[async_trait]
pub trait CvRefreshDriver: Send + Sync {
    /// Refresh every CV under `project` whose interval has elapsed.
    /// Returns the count of CVs that were re-materialised on this call;
    /// `NotDue` / `Failed` outcomes do not count.
    async fn refresh_project(&self, project: &ProjectId) -> Result<usize>;
}

#[async_trait]
pub(crate) trait ShardImpl: Send + Sync {
    async fn get(&self, project: &ProjectId, partition: &PartitionKey) -> Result<ProjectHandle>;
    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle;
    fn stats(&self) -> ShardStats;
    fn clone_arc(&self) -> Arc<dyn ShardImpl>;
    fn wal(&self) -> &Arc<dyn basin_wal::Wal>;
    /// This replica's stable lease holder id (see [`ShardConfig::replica_id`]).
    /// Exposed so the engine's partition-write owner resolution can identify
    /// "self" within a shared peer list.
    fn replica_id(&self) -> &str;
    /// The configured lease registry, if any. Exposed so the engine's
    /// partition forwarding can ask `owner_of(project, partition)` directly
    /// (multi-node bulk-ingest forwarding). `None` in single-replica / no-lease
    /// mode.
    fn lease_registry(&self) -> Option<&Arc<dyn basin_catalog::LeaseRegistry>>;
    async fn flush_to_parquet(&self) -> Result<()>;
    async fn run_tiering_sweep(&self) -> Result<()>;
    /// One synchronous stripe-merge sweep (see `Shard::run_stripe_merge_once`).
    /// Default: no-op for backends without per-file cold data.
    async fn run_stripe_merge_once(&self) -> Result<()> {
        Ok(())
    }
    /// ADR 0027 Phase 4 — rewrite all cold-tier data files that are missing
    /// the promoted JSONB shadow column(s) for `(project, table)`.  Returns
    /// the count of files that were rewritten.  Default: returns 0 (no-op for
    /// backends that do not carry per-file cold data).
    async fn run_promoted_column_backfill_sweep(
        &self,
        _project: &ProjectId,
        _table: &TableName,
    ) -> Result<usize> {
        Ok(0)
    }
    /// Projects the shard has resident state for. Used by the CV
    /// refresher (which only refreshes CVs whose project is currently
    /// loaded) and any future per-project background driver. Default
    /// impl returns the empty set so a backend that doesn't carry
    /// per-project residency state opts out gracefully.
    async fn resident_projects(&self) -> Vec<ProjectId> {
        Vec::new()
    }
    /// Phase 5.14.D2: register the top-pattern provider. Default no-op.
    fn set_top_pattern_provider(&self, _provider: Arc<dyn TopPatternProvider>) {}
    /// Wire the hot-tier memtable registry into the clean-retention sweep
    /// (S4). Default no-op for backends without a hot tier.
    fn set_memtable_registry(&self, _registry: Arc<basin_hottier::MemTableRegistry>) {}
    /// Wire the GIN row-group registry (populated at INSERT time by the engine
    /// and read at query time for `@>` prune decisions) into the compactor so
    /// newly compacted files are also indexed.  Default no-op.
    fn set_gin_rowgroup_registry(
        &self,
        _registry: Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    ) {
    }
    /// Wire the JSONB posting-list registry (Inv-W5 / W9) into the compactor.
    /// Mirrors `set_gin_rowgroup_registry` — populated lazily on first wire-up.
    /// Default no-op.
    fn set_jsonb_posting_registry(
        &self,
        _registry: Arc<basin_storage::index::jsonb_posting::JsonbPostingRegistry>,
    ) {
    }
    /// Wire the secondary B-tree index sink into the compactor so newly
    /// compacted files register their single-column index values. Mirrors
    /// `set_gin_rowgroup_registry` — populated lazily on first wire-up.
    /// Default no-op.
    fn set_secondary_index_registry(&self, _registry: Arc<dyn SecondaryIndexSink>) {}
    /// Phase 6.X.C — voluntary lease handoff. Default returns Ok (a backend
    /// that doesn't model leases has nothing to hand off).
    async fn yield_partition(
        &self,
        _project: &ProjectId,
        _partition: &PartitionKey,
        _to_holder: &str,
    ) -> Result<()> {
        Ok(())
    }
    /// Drop all in-memory tail and cached schema entries for
    /// `(project, table)` across every resident partition. Called from
    /// `exec_drop_table` so a freshly-opened session can't read rows that
    /// were inserted into the dropped table but never flushed past the
    /// shard's tail (sec_catalog_leakage P1). Default no-op for backends
    /// that don't keep an in-memory tail.
    async fn drop_table(&self, _project: &ProjectId, _table: &TableName) -> Result<()> {
        Ok(())
    }
    /// Cheap O(resident-partitions) check: is there any un-flushed in-memory
    /// tail data for `(project, table)`?  Returns `true` when at least one
    /// resident partition holds a non-empty tail batch for the table.
    ///
    /// Unlike `flush_to_parquet`, this does NOT list or scan object storage
    /// and does NOT drain anything — it only inspects the resident
    /// per-partition tail maps under their locks.  Read paths use it to decide
    /// whether tail rows might be uncovered by a cold-file presence guard,
    /// WITHOUT paying a flush.  Default `false` for backends that keep no
    /// in-memory tail (their writes land directly in the cold tier).
    async fn has_pending_tail(&self, _project: &ProjectId, _table: &TableName) -> bool {
        false
    }
    /// Cheap O(resident-partitions) count of un-flushed in-memory tail rows for
    /// `(project, table)`, summed across every resident partition.
    ///
    /// Same no-flush, no-list contract as [`ShardImpl::has_pending_tail`] — it
    /// only reads the resident per-partition tail batch row counts under their
    /// locks. Read paths use it to size the small-tail merge-on-read decision
    /// (a small tail is merged via the shard's tail-merging `read`, a large
    /// tail is flushed). Default `0` for backends that keep no in-memory tail.
    async fn pending_tail_rows(&self, _project: &ProjectId, _table: &TableName) -> usize {
        0
    }
    /// Cold read for `(project, table)` merged with the un-flushed in-memory
    /// tail of EVERY resident partition (not just `default_key()`).
    ///
    /// `ProjectHandle::read` merges only its own partition's tail; under
    /// statement-affine striping a table's tail is spread across `s1`, `s2`, …
    /// partitions, so the small-tail merge-on-read decision must union them
    /// all. The cold side is already (project, table)-scoped so it spans every
    /// stripe; only the per-partition tail needs the fan-out. Default delegates
    /// to a `default_key()` handle read (correct for single-partition backends
    /// that never stripe).
    async fn read_table_merging_tails(
        &self,
        project: &ProjectId,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        let handle = self.get(project, &PartitionKey::default_key()).await?;
        handle.read(table, opts).await
    }
    /// Test-only downcast for the inline test suite.
    #[cfg(test)]
    fn as_in_process(&self) -> Option<Arc<in_process::InProcessShard>> {
        None
    }
}

#[async_trait]
pub(crate) trait ProjectHandleImpl: Send + Sync {
    async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()>;
    /// Synchronous-commit variant of [`Self::write_batch`]: only returns
    /// once the WAL append is durable on the backing store (group-commit).
    /// Default delegates to `write_batch` so backends whose plain append is
    /// already durable-on-ack need no override.
    async fn write_batch_durable(&self, table: &TableName, batch: RecordBatch) -> Result<()> {
        self.write_batch(table, batch).await
    }
    async fn read(
        &self,
        table: &TableName,
        opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>>;
    fn last_active(&self) -> Instant;
    fn project(&self) -> ProjectId;
}

/// Read a `u64` seconds value from an env var, falling back to `default` when
/// unset or unparseable. Used for the lease TTL / renew cadence and
/// clean-sweep interval knobs.
fn env_secs(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Per-process default lease holder id: `host:pid` plus a short random suffix
/// so two processes on the same host with the same pid (after recycle) don't
/// collide. Only used when a lease registry is attached.
fn default_replica_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_string());
    let pid = std::process::id();
    // Nanosecond start salt disambiguates a pid recycled on the same host.
    let salt = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{host}:{pid}:{salt}")
}

mod follower;
mod in_process;
pub mod lock_registry;
pub mod lock_wait;
pub mod split;

pub use follower::{
    promote, FollowerConfig, FollowerShard, FollowerStats, LagTier, ReplicaRole, ShardFollower,
};
pub use lock_registry::{LockEntry, LockHandle, LockRegistry};
pub use split::{CatchupReport, Epoch, LocalShardSplitter, ShardSplitter, SplitPlan};

#[cfg(test)]
mod lease_mode_tests {
    use super::LeaseMode;

    #[test]
    fn parse_accepts_off_required_and_default() {
        assert_eq!(LeaseMode::parse(None).unwrap(), LeaseMode::Off);
        assert_eq!(LeaseMode::parse(Some("")).unwrap(), LeaseMode::Off);
        assert_eq!(LeaseMode::parse(Some("off")).unwrap(), LeaseMode::Off);
        assert_eq!(LeaseMode::parse(Some("OFF")).unwrap(), LeaseMode::Off);
        assert_eq!(
            LeaseMode::parse(Some("required")).unwrap(),
            LeaseMode::Required
        );
        assert_eq!(
            LeaseMode::parse(Some("  Required ")).unwrap(),
            LeaseMode::Required
        );
    }

    #[test]
    fn parse_rejects_unknown_values() {
        // A typo'd enforcement knob must not silently mean "off".
        assert!(LeaseMode::parse(Some("on")).is_err());
        assert!(LeaseMode::parse(Some("enforced")).is_err());
        assert!(LeaseMode::parse(Some("1")).is_err());
    }
}
