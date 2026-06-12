//! Background hot-tier overlay reconciler (delta-update design, commit 5).
//!
//! The delta-UPDATE fast path parks committed UPDATE/DELETE row images in the
//! process-wide `MemTableRegistry` overlay and, since the cardinality cap was
//! raised to 10k keys, a single statement can park ~150x more overlay rows
//! than before. Historically that overlay drained ONLY when a conflicting
//! cold-path mutation ran `materialize_hot_overlay_into_cold` — a workload
//! that never takes the cold path accumulates overlay without bound, paying:
//!
//!   * an unbounded **read tax**: tombstone suppression is O(rows scanned)
//!     and the decoded-overlay memo is rebuilt per write-generation, and
//!   * unbounded **durability exposure**: overlay entries are NOT WAL-logged;
//!     a crash loses every committed-but-unmaterialized overlay write.
//!
//! This task ticks every [`DEFAULT_TICK_SECS`] (env-tunable) and drives the
//! same `materialize_overlay_for_table` the foreground cold path uses, for
//! every table whose overlay is non-empty AND has crossed a bytes / age /
//! count threshold.
//!
//! ## Knobs (read ONCE at engine construction — i.e. under the test
//! conventions' ENV_LOCK — so a live tick never races test env mutation)
//!
//! | env                                | default | meaning                              |
//! |------------------------------------|---------|--------------------------------------|
//! | `BASIN_OVERLAY_RECONCILE_SECS`     | 5       | tick cadence; `0` disables the task  |
//! | `BASIN_OVERLAY_RECONCILE_BYTES`    | 8 MiB   | dirty-overlay bytes trigger          |
//! | `BASIN_OVERLAY_RECONCILE_AGE_SECS` | 15      | oldest-entry age trigger             |
//!
//! The count trigger is `pending > BASIN_DELTA_UPDATE_MAX_KEYS / 2` (default
//! 10_000 / 2 — mirrors `dml_mutate::DELTA_UPDATE_MAX_KEYS_DEFAULT`, the cap
//! that sizes the largest single-statement overlay write).
//!
//! ## Age signal
//!
//! The cheapest available age signal is `MemTable::oldest_row_age()` (one
//! atomic read). It tracks the oldest write of ANY resident entry — including
//! clean retained rows — and resets only when the memtable is fully empty, so
//! it can OVERSTATE the age of the dirty overlay. The criterion is consulted
//! only when `update_count + tombstone_count > 0`, so the worst case is
//! materializing a young overlay early: wasteful-but-safe (one extra narrowed
//! rewrite), never incorrect, and exact dirty-age tracking would need a new
//! basin-hottier surface this change deliberately avoids.
//!
//! ## Concurrency story (per-table serialization)
//!
//! * **Self-overlap is structurally impossible**: one tokio task, one table
//!   at a time, every materialize awaited before the next — no in-flight set
//!   is needed.
//! * **Racing a foreground materialize / cold CoW is safe**: both sides
//!   snapshot the dirty overlay, write replacement files, and commit through
//!   `Catalog::replace_data_files` with the snapshot id they loaded —
//!   optimistic concurrency. The loser's commit returns `CommitConflict`,
//!   its `mark_flushed` ack never runs (the overlay stays dirty for the next
//!   tick / next foreground op), and its already-written replacement file is
//!   an uncommitted orphan that every cold lister filters out via the
//!   `live_data_files()` intersect (#94/#95 defense-in-depth). The reconciler
//!   logs the conflict and simply retries on a later tick.
//! * **Writes landing mid-materialize survive**: `dirty_snapshot` captures
//!   per-key seqs and `mark_flushed` acks only those seqs, so an overlay
//!   write that lands after the snapshot carries a higher seq and stays
//!   DIRTY (the e2d5ab2 ack semantics, unchanged).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Weak;
use std::time::Duration;

use crate::EngineInner;

/// Default tick cadence, seconds. `BASIN_OVERLAY_RECONCILE_SECS` overrides;
/// `0` disables the reconciler entirely.
pub const DEFAULT_TICK_SECS: u64 = 5;

/// Default dirty-overlay bytes threshold (8 MiB).
/// `BASIN_OVERLAY_RECONCILE_BYTES` overrides.
pub const DEFAULT_BYTES_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Default oldest-entry age threshold, seconds.
/// `BASIN_OVERLAY_RECONCILE_AGE_SECS` overrides.
///
/// Lowered 60 → 15 (small-overlay read-tax fix): a small-but-present overlay
/// (e.g. 10 tombstones from a bounded DELETE) never crosses the bytes/count
/// triggers, so the age trigger is its ONLY drain path — and while it waits,
/// every read of the table pays the overlay tax AND the overlay-gated fast
/// paths (keyset / unordered-LIMIT pushdown, GIN file pruning, posting-probe
/// short-circuits) stay disabled. Materializing a handful of entries is one
/// narrowed rewrite (cheap); the worst-case churn is one such rewrite per
/// 15s+tick per actively-written table, which is comparable to the
/// background flush cadence. `oldest_row_age` can OVERSTATE the dirty age
/// (it tracks any resident entry, including clean retained rows) — that only
/// makes a drain happen sooner, which is wasteful-but-safe (see module docs).
pub const DEFAULT_AGE_SECS: u64 = 15;

/// Mirrors `dml_mutate::DELTA_UPDATE_MAX_KEYS_DEFAULT` (private to that
/// module): the delta-UPDATE cardinality cap that sizes the largest
/// single-statement overlay write. The count trigger fires at half of it.
const DELTA_UPDATE_MAX_KEYS_DEFAULT: u64 = 10_000;

/// Cumulative number of tables the reconciler successfully materialized.
static RECONCILED_TABLES: AtomicU64 = AtomicU64::new(0);
/// Cumulative dirty-overlay bytes covered by successful reconciles
/// (the `bytes_dirty()` reading taken just before each materialize).
static RECONCILED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Process-cumulative count of tables drained by the background reconciler.
/// Monotonic; test/observability surface (mirrors the engine's counter
/// conventions, but process-global because the task outlives no engine).
pub fn reconciled_tables_count() -> u64 {
    RECONCILED_TABLES.load(Ordering::Relaxed)
}

/// Process-cumulative dirty-overlay bytes drained by the background
/// reconciler. Monotonic.
pub fn reconciled_bytes_count() -> u64 {
    RECONCILED_BYTES.load(Ordering::Relaxed)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// Spawn the background reconciler for one engine. Called from
/// `Engine::new`; a no-op when no tokio runtime is current (sync unit tests)
/// or when `BASIN_OVERLAY_RECONCILE_SECS=0` (kill switch). Holds only a
/// `Weak<EngineInner>` so the task exits — rather than pinning the engine's
/// storage/catalog graph alive — once the engine is dropped (same rationale
/// as `attach_reactor_sink`).
pub(crate) fn spawn(inner: Weak<EngineInner>) {
    let Ok(rt_handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let tick_secs = env_u64("BASIN_OVERLAY_RECONCILE_SECS", DEFAULT_TICK_SECS);
    if tick_secs == 0 {
        return;
    }
    let bytes_threshold = env_u64("BASIN_OVERLAY_RECONCILE_BYTES", DEFAULT_BYTES_THRESHOLD);
    let age_threshold = Duration::from_secs(env_u64(
        "BASIN_OVERLAY_RECONCILE_AGE_SECS",
        DEFAULT_AGE_SECS,
    ));
    let count_threshold = env_u64("BASIN_DELTA_UPDATE_MAX_KEYS", DELTA_UPDATE_MAX_KEYS_DEFAULT) / 2;
    rt_handle.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(tick_secs)).await;
            // Upgrade per tick: the strong handle lives only for the tick's
            // duration, so dropping the last user `Engine` clone ends the
            // task on its next wake-up.
            let Some(strong) = inner.upgrade() else {
                return;
            };
            let engine = crate::Engine { inner: strong };
            run_tick(&engine, bytes_threshold, age_threshold, count_threshold).await;
        }
    });
}

/// One reconcile pass: select overlay-bearing tables past a threshold and
/// materialize them. Sequential per table (see the module docs' concurrency
/// story); errors are logged and retried on a later tick — the overlay stays
/// dirty until a materialize's catalog commit actually lands.
async fn run_tick(
    engine: &crate::Engine,
    bytes_threshold: u64,
    age_threshold: Duration,
    count_threshold: u64,
) {
    let registry = engine.memtable_registry();
    for (project, table, entry) in registry.tables_iter() {
        // O(1) overlay-presence gate — identical to the materialize prologue,
        // applied here so empty/INSERT-residency-only tables cost two atomic
        // loads per tick.
        let pending = entry.memtable.update_count() + entry.memtable.tombstone_count();
        if pending == 0 {
            continue;
        }
        let dirty_bytes = entry.memtable.bytes_dirty();
        let age = entry.memtable.oldest_row_age();
        let due = dirty_bytes > bytes_threshold || age > age_threshold || pending > count_threshold;
        if !due {
            continue;
        }
        match crate::dml_mutate::materialize_overlay_for_table(engine, project, &table).await {
            Ok(()) => {
                RECONCILED_TABLES.fetch_add(1, Ordering::Relaxed);
                RECONCILED_BYTES.fetch_add(dirty_bytes, Ordering::Relaxed);
                tracing::info!(
                    target: "basin_engine",
                    project = %project,
                    table = %table,
                    pending,
                    dirty_bytes,
                    "overlay reconciler materialized table overlay into cold"
                );
            }
            Err(e) => {
                // CommitConflict (lost an optimistic race to a foreground
                // mutation) or a transient storage/catalog error: the overlay
                // is still dirty — no ack ran — so the next tick retries.
                tracing::debug!(
                    target: "basin_engine",
                    project = %project,
                    table = %table,
                    error = %e,
                    "overlay reconcile attempt skipped; will retry next tick"
                );
            }
        }
    }
}
