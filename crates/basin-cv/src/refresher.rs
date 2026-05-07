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

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use basin_catalog::{Catalog, DataFileRef};
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Mutex;

use crate::store::{concat_batches, CvStore};
use crate::types::{CvRefreshOutcome, CvRefreshState, CvSpec};

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

        match self.do_refresh(tenant, spec, now).await {
            Ok(rows) => CvRefreshOutcome::Refreshed { rows_written: rows },
            Err(e) => CvRefreshOutcome::Failed(format!("{e}")),
        }
    }

    /// Inner refresh body. Re-runs the source query in full, writes a new
    /// Parquet file under the CV's table prefix, and atomically swaps the
    /// catalog manifest to point at it.
    ///
    /// v0.2 will narrow this to an incremental scan over rows whose bucket
    /// key exceeds `last_bucket_max`, and will merge the existing
    /// materialised file's rows that pre-date the watermark with the new
    /// rows so the CV's row count grows monotonically.
    async fn do_refresh(
        &self,
        tenant: &TenantId,
        spec: &CvSpec,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        let engine = self.inner.store.engine();
        let catalog: Arc<dyn Catalog> = engine.config().catalog.clone();
        let storage = engine.config().storage.clone();
        let table = TableName::new(spec.name.clone()).map_err(|e| {
            BasinError::internal(format!("CV name {} invalid: {e}", spec.name))
        })?;

        // 1. Run the source query against `tenant`'s session.
        let sess = engine.open_session(*tenant).await?;
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
        if row_count == 0 {
            // No rows: skip the file write but still stamp the refresh
            // timestamp so the next tick measures from `now`.
            let state = CvRefreshState {
                last_refreshed_at: now,
                last_bucket_max: spec.last_bucket_max,
                last_row_count: 0,
            };
            self.inner
                .store
                .update_refresh_state(tenant, &spec.name, state)
                .await?;
            return Ok(0);
        }

        let merged = concat_batches(&schema, &batches)?;

        // 2. Write the new materialisation as a Parquet file under the
        //    CV's prefix. The default partition keeps it under
        //    `tenants/<t>/tables/<cv>/data/_default/...`.
        let part = PartitionKey::default_key();
        let written = storage
            .write_batch(tenant, &table, &part, &merged)
            .await?;

        // 3. Atomically swap the catalog manifest. The `removed_paths` list
        //    is the prior snapshot's `data_files` — typically one file from
        //    the bootstrap or the previous refresh. We tolerate an empty
        //    prior file set (first refresh of a CV that bootstrapped with
        //    zero rows).
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
        }];
        // `replace_data_files` tolerates `removed_paths` containing every
        // historical file the snapshot chain has seen; the catalog is the
        // ground truth for the live set, and the engine's read path will
        // consult the new manifest after we update.
        let after = catalog
            .replace_data_files(
                tenant,
                &table,
                meta.current_snapshot,
                removed_paths.clone(),
                added,
            )
            .await?;
        let _ = after;

        // 4. Best-effort: physically delete the prior files so the next
        //    `list_data_files` listing doesn't see ghosts. A failure here
        //    is wasted bytes, not a correctness issue.
        for path in &removed_paths {
            // Skip the file we just wrote — it's the new manifest entry.
            if path == written.path.as_ref() {
                continue;
            }
            let p = object_store::path::Path::from(path.as_str());
            let _ = storage.delete_file(tenant, &p).await;
        }

        // 5. Stamp the refresh state.
        let state = CvRefreshState {
            last_refreshed_at: now,
            last_bucket_max: spec.last_bucket_max, // v0.2 will compute from data
            last_row_count: written.row_count,
        };
        self.inner
            .store
            .update_refresh_state(tenant, &spec.name, state)
            .await?;

        Ok(written.row_count)
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
