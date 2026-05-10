//! CV refresh tick loop. Once every refresh-interval-floor seconds (in
//! production) or once per [`CvRefresher::tick`] (in tests), walk every
//! resident tenant's CV set, run each due CV's source SQL, and atomically
//! swap the materialised file in via [`basin_catalog::Catalog::replace_data_files`].
//!
//! ## Clock injection
//!
//! [`CvRefresher`] takes a [`Clock`] so tests can drive ticks at deterministic
//! times. Production code uses [`SystemClock`] which delegates to `Utc::now`.
//!
//! ## Per-tenant queue
//!
//! Tenants are processed sequentially within a single tick to keep the
//! per-tenant resource footprint bounded. The "queue" shape lets a future
//! production driver split the per-tenant work onto a thread pool without
//! changing the API; v0.1's implementation is a sequential walk.
//!
//! ## Incremental refresh
//!
//! When the CV's stored `query_sql` matches the supported time-bucket shape
//! (`date_trunc(_, col)` or `time_bucket(_, col)` in the GROUP BY) AND the
//! CV has been refreshed at least once before, the tick re-aggregates only
//! source rows whose bucket-column value is `>= last_bucket_max` — the
//! start of the last bucket the previous refresh touched. Late rows that
//! arrived in that bucket are merged in; brand-new buckets are appended.
//! Source rows that belong to older, fully-closed buckets are not
//! re-scanned. The watermark/bucket-spanning correctness invariant is
//! documented in `crate::time_bucket`.
//!
//! Bodies whose GROUP BY shape isn't recognised (`extract`, multi-key
//! aggregates without a time-bucket, etc.) fall back to full
//! re-execution. The CV remains correct — just not faster.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
use arrow_schema::{DataType, TimeUnit};
use async_trait::async_trait;
use basin_catalog::{Catalog, DataFileRef};
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};
use basin_engine::{Engine, ExecResult, TenantSession};
use basin_shard::CvRefreshDriver;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Mutex;

use crate::store::{concat_batches, CvStore};
use crate::types::{CvRefreshOutcome, CvRefreshState, CvSpec};
use basin_engine::cv_time_bucket::{detect_time_bucket, rewrite_for_watermark, BucketInfo};

/// Source of time. Production uses [`SystemClock`]; tests use [`TestClock`]
/// to drive ticks at deterministic instants.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// Real wall-clock, delegates to [`Utc::now`].
#[derive(Clone, Copy, Default, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Manually-advanced clock. Cheap to clone; the inner cell is an
/// [`AtomicI64`] of unix milliseconds so [`Clock::now`] is sync and lock-free.
#[derive(Clone, Debug)]
pub struct TestClock {
    ms: Arc<AtomicI64>,
}

impl TestClock {
    pub fn new(t: DateTime<Utc>) -> Self {
        Self {
            ms: Arc::new(AtomicI64::new(t.timestamp_millis())),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        self.ms.store(t.timestamp_millis(), Ordering::Relaxed);
    }

    pub fn advance(&self, d: Duration) {
        self.ms.fetch_add(d.num_milliseconds(), Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(self.ms.load(Ordering::Relaxed))
            .unwrap_or_else(|| Utc::now())
    }
}

/// Per-refresh observation hook. Tests register one of these via
/// [`CvRefresher::set_spy`] to inspect which SQL the refresher executed
/// for each CV (full vs. incremental, plus the actual rewritten SQL when
/// incremental). Production never installs a spy.
pub trait RefreshSpy: Send + Sync + 'static {
    fn record(&self, cv_name: &str, mode: RefreshMode, sql: &str);
}

/// How a single refresh executed. Surfaced to [`RefreshSpy`] (and to
/// callers via [`CvRefreshOutcome::Refreshed`] in v0.2 — for now the
/// outcome carries the row count only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshMode {
    /// Full re-execution of the user's SELECT body.
    Full,
    /// Incremental: only rows where `bucket_col >= watermark` were
    /// scanned. The matview's prior `bucket >= watermark` rows were
    /// dropped before the new aggregation was appended.
    Incremental,
}

/// One refresh attempt's outcome, paired with the CV name it concerns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshOutcome {
    pub tenant: TenantId,
    pub cv_name: String,
    pub outcome: CvRefreshOutcome,
}

/// Driver loop. Hold one of these for the lifetime of the process; call
/// [`CvRefresher::tick`] from a periodic timer in production, or directly
/// from tests at controlled instants.
#[derive(Clone)]
pub struct CvRefresher {
    inner: Arc<RefresherInner>,
}

struct RefresherInner {
    store: CvStore,
    clock: Arc<dyn Clock>,
    /// Tenants the refresher is responsible for. CV is opt-in per tenant —
    /// the router (or test) registers each tenant once via
    /// [`CvRefresher::register_tenant`]. Mirrors `basin-cron`'s shape.
    tenants: Mutex<Vec<TenantId>>,
    /// Per-tenant timestamp of the most recent tick. Used only as a hook
    /// for future "skip the CV refresh entirely if no work has accumulated"
    /// logic; v0.1 always does a per-CV due-check inside `tick_tenant`.
    #[allow(dead_code)]
    last_tick: Mutex<HashMap<TenantId, DateTime<Utc>>>,
    /// Optional test spy. None in production.
    spy: Mutex<Option<Arc<dyn RefreshSpy>>>,
}

impl CvRefresher {
    /// Build a refresher over `engine` with `clock`.
    pub fn new(engine: Engine, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: Arc::new(RefresherInner {
                store: CvStore::new(engine),
                clock,
                tenants: Mutex::new(Vec::new()),
                last_tick: Mutex::new(HashMap::new()),
                spy: Mutex::new(None),
            }),
        }
    }

    /// Convenience: build with [`SystemClock`].
    pub fn with_system_clock(engine: Engine) -> Self {
        Self::new(engine, Arc::new(SystemClock))
    }

    /// Reference to the underlying [`CvStore`]. Useful for tests that
    /// want to register or inspect CVs directly.
    pub fn store(&self) -> &CvStore {
        &self.inner.store
    }

    /// Install a test [`RefreshSpy`]. Idempotent — replaces any prior spy.
    pub async fn set_spy(&self, spy: Arc<dyn RefreshSpy>) {
        *self.inner.spy.lock().await = Some(spy);
    }

    /// Mark `tenant` as resident. Idempotent.
    pub async fn register_tenant(&self, tenant: TenantId) {
        let mut tenants = self.inner.tenants.lock().await;
        if !tenants.contains(&tenant) {
            tenants.push(tenant);
        }
    }

    /// Run one tick. Walks every registered tenant, refreshes every CV
    /// whose interval has elapsed, and returns one [`RefreshOutcome`] per
    /// CV.
    pub async fn tick(&self) -> Result<Vec<RefreshOutcome>> {
        let now = self.inner.clock.now();
        let tenants = self.inner.tenants.lock().await.clone();
        let mut outcomes = Vec::new();
        for tenant in tenants {
            let per_tenant = self.tick_tenant(&tenant, now).await?;
            outcomes.extend(per_tenant);
        }
        Ok(outcomes)
    }

    /// Convenience for one-shot tests: take an explicit `now`.
    pub async fn tick_at(&self, now: DateTime<Utc>) -> Result<Vec<RefreshOutcome>> {
        let tenants = self.inner.tenants.lock().await.clone();
        let mut outcomes = Vec::new();
        for tenant in tenants {
            let per_tenant = self.tick_tenant(&tenant, now).await?;
            outcomes.extend(per_tenant);
        }
        Ok(outcomes)
    }

    async fn tick_tenant(
        &self,
        tenant: &TenantId,
        now: DateTime<Utc>,
    ) -> Result<Vec<RefreshOutcome>> {
        let cvs = self.inner.store.list_cvs(tenant).await?;
        let mut outcomes = Vec::with_capacity(cvs.len());
        for spec in cvs {
            let outcome = self.refresh_one(tenant, &spec, now).await;
            outcomes.push(RefreshOutcome {
                tenant: *tenant,
                cv_name: spec.name.clone(),
                outcome,
            });
        }
        Ok(outcomes)
    }

    /// Walk every CV under `tenant` once, refreshing those whose interval
    /// has elapsed. Returns the count of CVs that were re-materialised
    /// (`NotDue` / `Failed` outcomes don't count).
    pub async fn refresh_tenant_now(&self, tenant: &TenantId) -> Result<usize> {
        let now = self.inner.clock.now();
        let outcomes = self.tick_tenant(tenant, now).await?;
        let refreshed = outcomes
            .iter()
            .filter(|o| matches!(o.outcome, CvRefreshOutcome::Refreshed { .. }))
            .count();
        Ok(refreshed)
    }

    /// One-shot: refresh `name` under `tenant` immediately, ignoring the
    /// due-check. `force_full = true` forces a full re-execution (the
    /// `WITH (full=true)` opt-out for `REFRESH MATERIALIZED VIEW`); the
    /// default is to use the same incremental-vs-full logic the periodic
    /// tick uses.
    pub async fn refresh_named_now(
        &self,
        tenant: &TenantId,
        name: &str,
        force_full: bool,
    ) -> Result<u64> {
        let spec = self
            .inner
            .store
            .get_cv(tenant, name)
            .await?
            .ok_or_else(|| BasinError::not_found(format!("CV {name} on {tenant}")))?;
        let now = self.inner.clock.now();
        self.do_refresh(tenant, &spec, now, force_full).await
    }

    /// Refresh a single CV. Errors are caught and surfaced as
    /// [`CvRefreshOutcome::Failed`] so one bad CV cannot stall the rest
    /// of the tick.
    async fn refresh_one(
        &self,
        tenant: &TenantId,
        spec: &CvSpec,
        now: DateTime<Utc>,
    ) -> CvRefreshOutcome {
        // Due check: skip if interval hasn't elapsed.
        if let Some(last) = spec.last_refreshed_at {
            let elapsed = now - last;
            if elapsed < Duration::seconds(spec.refresh_interval_secs as i64) {
                return CvRefreshOutcome::NotDue;
            }
        }

        match self.do_refresh(tenant, spec, now, false).await {
            Ok(rows) => CvRefreshOutcome::Refreshed { rows_written: rows },
            Err(e) => CvRefreshOutcome::Failed(format!("{e}")),
        }
    }

    /// Inner refresh body. Picks incremental or full based on
    /// `force_full`, the CV's prior watermark, and whether the SELECT body
    /// matches a recognised time-bucket shape.
    async fn do_refresh(
        &self,
        tenant: &TenantId,
        spec: &CvSpec,
        now: DateTime<Utc>,
        force_full: bool,
    ) -> Result<u64> {
        // Decide full vs. incremental.
        let bucket = (!force_full)
            .then(|| detect_time_bucket(&spec.query_sql))
            .flatten();
        match (bucket, spec.last_bucket_max) {
            (Some(info), Some(watermark)) => {
                self.refresh_incremental(tenant, spec, &info, watermark, now)
                    .await
            }
            (info, _) => {
                // Either no bucket detected, force_full set, or first
                // refresh: do a full rebuild and (if a bucket *is*
                // detectable) compute the watermark for the next tick.
                self.refresh_full(tenant, spec, info, now).await
            }
        }
    }

    /// Full re-execution: re-runs the source query and replaces the
    /// materialised file. When `bucket` is `Some`, also computes the new
    /// watermark by scanning the source table's max bucket-column value.
    async fn refresh_full(
        &self,
        tenant: &TenantId,
        spec: &CvSpec,
        bucket: Option<BucketInfo>,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        let engine = self.inner.store.engine();
        let catalog: Arc<dyn Catalog> = engine.config().catalog.clone();
        let storage = engine.config().storage.clone();
        let table = TableName::new(spec.name.clone()).map_err(|e| {
            BasinError::internal(format!("CV name {} invalid: {e}", spec.name))
        })?;

        let sess = engine.open_session(*tenant).await?;
        self.notify_spy(&spec.name, RefreshMode::Full, &spec.query_sql)
            .await;
        let res = sess.execute(&spec.query_sql).await?;
        let (schema, batches) = match res {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { tag } => {
                return Err(BasinError::internal(format!(
                    "CV {} source query returned non-rows ({tag})",
                    spec.name
                )));
            }
        };
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        // Compute the next watermark: start of the bucket containing the
        // current max source-ts. We re-query the source table directly —
        // cheap (one aggregate row) and resilient to projections that
        // strip the timestamp column.
        let new_watermark = match &bucket {
            Some(info) => self
                .compute_watermark(&sess, &spec.source_table, info)
                .await?,
            None => spec.last_bucket_max,
        };

        if row_count == 0 {
            let state = CvRefreshState {
                last_refreshed_at: now,
                last_bucket_max: new_watermark,
                last_row_count: 0,
            };
            self.inner
                .store
                .update_refresh_state(tenant, &spec.name, state)
                .await?;
            return Ok(0);
        }

        let merged = concat_batches(&schema, &batches)?;
        let part = PartitionKey::default_key();
        let written = storage.write_batch(tenant, &table, &part, &merged).await?;
        let meta = catalog.load_table(tenant, &table).await?;
        let removed_paths: Vec<String> = meta
            .snapshots
            .iter()
            .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
            .collect();
        let added = vec![DataFileRef {
            path: written.path.as_ref().to_string(),
            size_bytes: written.size_bytes,
            row_count: written.row_count,
            column_stats: written.column_stats.clone(),
        }];
        catalog
            .replace_data_files(
                tenant,
                &table,
                meta.current_snapshot,
                removed_paths.clone(),
                added,
            )
            .await?;
        for path in &removed_paths {
            if path == written.path.as_ref() {
                continue;
            }
            let p = object_store::path::Path::from(path.as_str());
            let _ = storage.delete_file(tenant, &p).await;
        }

        let state = CvRefreshState {
            last_refreshed_at: now,
            last_bucket_max: new_watermark,
            last_row_count: written.row_count,
        };
        self.inner
            .store
            .update_refresh_state(tenant, &spec.name, state)
            .await?;
        Ok(written.row_count)
    }

    /// Incremental refresh. Re-aggregates source rows where
    /// `<bucket_column> >= watermark`, deletes the matview's prior
    /// `<bucket_alias> >= watermark` rows, appends the freshly aggregated
    /// rows. The output is byte-equivalent to a full rebuild for the
    /// supported watermark range; rows that arrived late in
    /// already-finalised buckets are missed (documented v0.1 limitation).
    async fn refresh_incremental(
        &self,
        tenant: &TenantId,
        spec: &CvSpec,
        bucket: &BucketInfo,
        watermark: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        let engine = self.inner.store.engine();
        let catalog: Arc<dyn Catalog> = engine.config().catalog.clone();
        let storage = engine.config().storage.clone();
        let table = TableName::new(spec.name.clone()).map_err(|e| {
            BasinError::internal(format!("CV name {} invalid: {e}", spec.name))
        })?;

        // Build the rewritten SELECT. If rewrite fails (multi-table FROM,
        // weird shape) we fall back to a full refresh.
        let rewritten = match rewrite_for_watermark(
            &spec.query_sql,
            &bucket.bucket_column,
            watermark,
        ) {
            Ok(s) => s,
            Err(_) => {
                return self.refresh_full(tenant, spec, Some(bucket.clone()), now).await;
            }
        };

        let sess = engine.open_session(*tenant).await?;
        self.notify_spy(&spec.name, RefreshMode::Incremental, &rewritten)
            .await;

        // 1. Re-aggregate the watermark-and-newer source rows.
        let new_res = sess.execute(&rewritten).await?;
        let (new_schema, new_batches) = match new_res {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { tag } => {
                return Err(BasinError::internal(format!(
                    "CV {} incremental source returned non-rows ({tag})",
                    spec.name
                )));
            }
        };

        // 2. Read the existing matview rows so we can keep the ones strictly
        //    older than the watermark and replace the newer ones with the
        //    fresh aggregation.
        let kept_sql = format!("SELECT * FROM {}", spec.name);
        let kept_res = sess.execute(&kept_sql).await?;
        let (existing_schema, existing_batches) = match kept_res {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { tag } => {
                return Err(BasinError::internal(format!(
                    "CV {} matview SELECT returned non-rows ({tag})",
                    spec.name
                )));
            }
        };

        let kept_batches = filter_below_watermark(
            &existing_batches,
            &bucket.bucket_alias,
            watermark,
        )?;

        // 3. Combine kept rows + freshly aggregated rows. The schemas are
        //    expected to match (the new rewrite preserves the projection
        //    list). If they disagree (CAST coercion drift, etc.), prefer the
        //    new aggregation's schema and skip the kept rows for
        //    correctness — this is a rare edge case and re-aggregation
        //    over the entire source state still happens via the full-fallback
        //    path on the next tick.
        let combined_schema = if new_schema.fields() == existing_schema.fields() {
            new_schema.clone()
        } else {
            new_schema.clone()
        };
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        if new_schema.fields() == existing_schema.fields() {
            all_batches.extend(kept_batches.into_iter());
        }
        all_batches.extend(new_batches.into_iter());
        let merged = if all_batches.is_empty() {
            // Nothing to materialise. Stamp the refresh state and bail.
            let new_watermark = self
                .compute_watermark(&sess, &spec.source_table, bucket)
                .await?
                .or(Some(watermark));
            let state = CvRefreshState {
                last_refreshed_at: now,
                last_bucket_max: new_watermark,
                last_row_count: 0,
            };
            self.inner
                .store
                .update_refresh_state(tenant, &spec.name, state)
                .await?;
            return Ok(0);
        } else if all_batches.len() == 1 {
            all_batches.remove(0)
        } else {
            let refs: Vec<&RecordBatch> = all_batches.iter().collect();
            arrow_select::concat::concat_batches(&combined_schema, refs)
                .map_err(|e| BasinError::internal(format!("concat: {e}")))?
        };

        // 4. Write the combined result and swap.
        let part = PartitionKey::default_key();
        let written = storage.write_batch(tenant, &table, &part, &merged).await?;
        let meta = catalog.load_table(tenant, &table).await?;
        let removed_paths: Vec<String> = meta
            .snapshots
            .iter()
            .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
            .collect();
        let added = vec![DataFileRef {
            path: written.path.as_ref().to_string(),
            size_bytes: written.size_bytes,
            row_count: written.row_count,
            column_stats: written.column_stats.clone(),
        }];
        catalog
            .replace_data_files(
                tenant,
                &table,
                meta.current_snapshot,
                removed_paths.clone(),
                added,
            )
            .await?;
        for path in &removed_paths {
            if path == written.path.as_ref() {
                continue;
            }
            let p = object_store::path::Path::from(path.as_str());
            let _ = storage.delete_file(tenant, &p).await;
        }

        // 5. Compute the next watermark from the source's current max ts.
        let new_watermark = self
            .compute_watermark(&sess, &spec.source_table, bucket)
            .await?
            .or(Some(watermark));
        let state = CvRefreshState {
            last_refreshed_at: now,
            last_bucket_max: new_watermark,
            last_row_count: written.row_count,
        };
        self.inner
            .store
            .update_refresh_state(tenant, &spec.name, state)
            .await?;
        Ok(written.row_count)
    }

    /// Query the source table for `max(<bucket_column>)`, then floor the
    /// result to the bucket's start. Returns `None` when the source is
    /// empty (no rows).
    async fn compute_watermark(
        &self,
        sess: &TenantSession,
        source_table: &str,
        bucket: &BucketInfo,
    ) -> Result<Option<DateTime<Utc>>> {
        if source_table.is_empty() {
            return Ok(None);
        }
        let sql = format!(
            "SELECT max({col}) AS m FROM {tbl}",
            col = bucket.bucket_column,
            tbl = source_table,
        );
        let res = match sess.execute(&sql).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { .. } => return Ok(None),
        };
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let arr = batch.column(0);
            if arr.is_null(0) {
                return Ok(None);
            }
            let raw: Option<DateTime<Utc>> = match arr.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, _) => arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .and_then(|a| DateTime::<Utc>::from_timestamp_micros(a.value(0))),
                DataType::Timestamp(TimeUnit::Millisecond, _) => arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .and_then(|a| DateTime::<Utc>::from_timestamp_millis(a.value(0))),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .and_then(|a| {
                        let v = a.value(0);
                        DateTime::<Utc>::from_timestamp(v / 1_000_000_000, (v % 1_000_000_000) as u32)
                    }),
                DataType::Timestamp(TimeUnit::Second, _) => arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .and_then(|a| DateTime::<Utc>::from_timestamp(a.value(0), 0)),
                _ => None,
            };
            if let Some(ts) = raw {
                return Ok(Some(bucket.width.floor(ts)));
            }
        }
        Ok(None)
    }

    async fn notify_spy(&self, cv_name: &str, mode: RefreshMode, sql: &str) {
        if let Some(spy) = self.inner.spy.lock().await.clone() {
            spy.record(cv_name, mode, sql);
        }
    }

    /// Spawn the production tick loop on the current tokio runtime. Polls
    /// every 60 seconds — each CV's `refresh_interval_secs` is enforced
    /// per-CV inside [`Self::tick`], so a 60-second polling cadence is the
    /// finest granularity v0.1 supports. The returned
    /// [`tokio::task::JoinHandle`] can be used to abort the loop on
    /// shutdown.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.tick().await {
                    tracing::warn!(error = %e, "cv refresh tick failed");
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        })
    }
}

/// Filter `batches` to rows whose `<bucket_alias>` value is strictly less
/// than `watermark`. The matview's bucket column may be a Timestamp
/// (date_trunc returns TIMESTAMPTZ in PG) or a string (when the user wraps
/// it in `cast(... AS TEXT)`). We accept both representations and skip the
/// kept-row filter on any other shape — the worst-case effect is the
/// incremental refresh's "kept" row set is empty, which means the next
/// full-fallback tick will rebuild correctness.
fn filter_below_watermark(
    batches: &[RecordBatch],
    bucket_alias: &str,
    watermark: DateTime<Utc>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    // Locate the bucket column once (all batches share the schema).
    let schema = batches[0].schema();
    let col_idx = schema
        .index_of(bucket_alias)
        .ok()
        .or_else(|| {
            // Tolerate case-insensitive match — Postgres folds unquoted
            // identifiers to lowercase, but Arrow preserves whatever case
            // the projection emitted.
            schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(bucket_alias))
        });
    let Some(col_idx) = col_idx else {
        // Couldn't find the column — keep nothing, and rely on the new
        // aggregation alone. This is rare; the `cast(... AS TEXT)` shape
        // still works because the alias is preserved.
        return Ok(Vec::new());
    };

    let mut kept: Vec<RecordBatch> = Vec::new();
    for batch in batches {
        let arr = batch.column(col_idx);
        let mask = build_lt_watermark_mask(arr.as_ref(), watermark);
        let Some(mask) = mask else {
            // Unsupported column type — bail to a safe default ("keep
            // nothing"). The fresh aggregation's rows will form the entire
            // matview state.
            return Ok(Vec::new());
        };
        let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| BasinError::internal(format!("filter: {e}")))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}

/// Build a boolean mask: `true` where `arr[i] < watermark`. Handles the
/// shapes the bucket-alias column may surface as: timestamp (raw
/// `date_trunc`) or string (the `cast(... AS TEXT)` shape some users adopt
/// to dodge timestamp-cast ambiguity in the engine).
fn build_lt_watermark_mask(
    arr: &dyn Array,
    watermark: DateTime<Utc>,
) -> Option<arrow_array::BooleanArray> {
    match arr.data_type() {
        DataType::Timestamp(unit, _) => {
            let bound = match unit {
                TimeUnit::Second => watermark.timestamp(),
                TimeUnit::Millisecond => watermark.timestamp_millis(),
                TimeUnit::Microsecond => watermark.timestamp_micros(),
                TimeUnit::Nanosecond => watermark.timestamp_nanos_opt()?,
            };
            let values: Vec<Option<bool>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        Some(false)
                    } else {
                        let v = match unit {
                            TimeUnit::Second => arr
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .unwrap()
                                .value(i),
                            TimeUnit::Millisecond => arr
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap()
                                .value(i),
                            TimeUnit::Microsecond => arr
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap()
                                .value(i),
                            TimeUnit::Nanosecond => arr
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap()
                                .value(i),
                        };
                        Some(v < bound)
                    }
                })
                .collect();
            Some(arrow_array::BooleanArray::from(values))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let watermark_str = watermark.format("%Y-%m-%d %H:%M:%S").to_string();
            let watermark_str_t = watermark.format("%Y-%m-%dT%H:%M:%S").to_string();
            let arr_strs: Vec<Option<&str>> = if matches!(arr.data_type(), DataType::LargeUtf8) {
                let s = arr.as_any().downcast_ref::<arrow_array::LargeStringArray>()?;
                (0..s.len())
                    .map(|i| if s.is_null(i) { None } else { Some(s.value(i)) })
                    .collect()
            } else {
                let s = arr.as_any().downcast_ref::<arrow_array::StringArray>()?;
                (0..s.len())
                    .map(|i| if s.is_null(i) { None } else { Some(s.value(i)) })
                    .collect()
            };
            let values: Vec<Option<bool>> = arr_strs
                .iter()
                .map(|opt| match opt {
                    None => Some(false),
                    Some(s) => {
                        // Compare as strings — works for ISO-8601 sortable
                        // formats. We accept both `space` and `T` separators
                        // so post-DataFusion display variants don't trip us.
                        let s_norm = s.replace('T', " ");
                        let w_norm = watermark_str.clone();
                        let _ = watermark_str_t;
                        Some(s_norm.as_str() < w_norm.as_str())
                    }
                })
                .collect();
            Some(arrow_array::BooleanArray::from(values))
        }
        _ => None,
    }
}

/// Adapter so [`basin_shard::Shard::run_cv_refresh`] can drive the
/// refresher without `basin-shard` taking a dependency on `basin-cv`.
/// The trait lives in `basin-shard`; the implementation lives here.
#[async_trait]
impl CvRefreshDriver for CvRefresher {
    async fn refresh_tenant(&self, tenant: &TenantId) -> Result<usize> {
        self.refresh_tenant_now(tenant).await
    }
}
