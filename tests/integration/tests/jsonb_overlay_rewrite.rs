//! Regression: JSONB column + Vortex shard engine + a pending fast-path UPDATE
//! overlay made the next bulk UPDATE-by-range (and a plain filtered SELECT /
//! COUNT(*)) fail with:
//!
//!   internal: concat batches for rewrite: Invalid argument error:
//!   It is not possible to concatenate arrays of different data types
//!   (Binary, LargeBinary)
//!
//! ## Root cause
//!
//! JSONB's catalog physical type is Arrow `LargeBinary` (see
//! `basin_engine::types`: `JSONB | JSON => DataType::LargeBinary`). Two
//! producers disagree on the physical type for the SAME logical column:
//!
//!   * Vortex COLD decode: `LargeBinary` round-trips through Vortex's
//!     `BinaryView`, which `normalize_view_types_schema` maps to plain
//!     32-bit `Binary`. So cold-read JSONB columns are `Binary`.
//!   * Memtable Arrow-IPC override rows (written by the hot-tier UPDATE fast
//!     path): decoded straight from the IPC blob, they preserve the WRITER's
//!     catalog types — JSONB stays `LargeBinary`.
//!
//! `materialize_hot_overlay_into_cold` reads the cold base RAW (`Binary`),
//! appends the override rows (`LargeBinary`), and hands the mixed vector to
//! `write_replacement` → `concat_batches(catalog_schema=LargeBinary, …)`,
//! which arrow rejects. The same Binary-vs-LargeBinary divergence poisons the
//! fast-path SELECT scan (cold `Binary` batches assembled under a catalog
//! `LargeBinary` projection schema).
//!
//! ## Fix
//!
//! Normalize batches to the catalog physical type at the READ/merge boundary
//! (`hot_tombstone::normalize_batch_to_schema`), never the writer — casting
//! on-disk would orphan existing files. Applied at the `write_replacement`
//! and per-file-rewrite concat sites and at the overlay-materialize merge, and
//! the override-row reprojection casts to the cold scan's schema.
//!
//! This test reproduces the exact shape: 10k-row JSONB table on the shard +
//! Vortex engine, flushed so Vortex stripes exist, ONE fast-path single-row
//! UPDATE to plant an overlay, then (a) a bulk UPDATE-by-range that must
//! succeed with correct values and (b) a selective SELECT + COUNT(*) that must
//! both succeed.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, LargeBinaryArray, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialise env-var mutation across test threads in this binary.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// Run `fut` with `BASIN_HOTTIER_UPDATE_FASTPATH=1` so the single-row UPDATE
/// takes the overlay-writing fast path (planting the memtable override that
/// triggers the bug) rather than a cold copy-on-write rewrite.
async fn with_update_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
    out
}

/// `SELECT COUNT(*) FROM events WHERE <pred>` → the i64 count.
async fn count_where(sess: &basin_engine::ProjectSession, pred: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM events WHERE {pred}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("COUNT(*) WHERE {pred} failed: {other:?}"),
    };
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count Int64")
        .value(0)
}

/// `SELECT status FROM events WHERE id = {id}` → the status text. Decodes
/// whichever physical type the engine returns for the JSONB-adjacent text col.
async fn status_for_id(sess: &basin_engine::ProjectSession, id: i64) -> String {
    let sql = format!("SELECT status FROM events WHERE id = {id}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("SELECT status WHERE id={id} failed: {other:?}"),
    };
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no row for id={id}"));
    let col = b.column(0);
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(0).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        return arr.value(0).to_string();
    }
    panic!("status column unexpected type {:?}", col.data_type());
}

/// Decode the `payload` JSONB for one id (sanity that the JSONB column itself
/// survives the rewrite). Returns the raw JSON bytes as a String.
async fn payload_for_id(sess: &basin_engine::ProjectSession, id: i64) -> String {
    let sql = format!("SELECT payload FROM events WHERE id = {id}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("SELECT payload WHERE id={id} failed: {other:?}"),
    };
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no payload row for id={id}"));
    let col = b.column(0);
    if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        return String::from_utf8_lossy(arr.value(0)).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        return String::from_utf8_lossy(arr.value(0)).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(0).to_string();
    }
    panic!("payload column unexpected type {:?}", col.data_type());
}

/// The bug: with a pending fast-path UPDATE overlay on a JSONB table, the next
/// bulk UPDATE-by-range concat-fails on the Binary/LargeBinary mismatch, and a
/// subsequent filtered SELECT / COUNT also errors in the scan path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonb_overlay_then_bulk_update_and_select_succeed() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Plain CREATE TABLE → Vortex is the default on-disk format (#161), which
    // is the format whose BinaryView→Binary narrowing triggers the bug. `id`
    // is the single-column PK so the hot-tier UPDATE fast path is eligible.
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            status TEXT, \
            created_at BIGINT, \
            payload JSONB)",
    )
    .await
    .expect("CREATE TABLE events");

    // Seed 10k rows so the flush produces real Vortex stripes (the column_stats
    // / view-type narrowing only kicks in on a materialized file). Each row
    // carries a non-trivial JSONB payload. created_at = id so the range
    // predicate below selects a known prefix.
    const N: i64 = 10_000;
    let mut stmt = String::from("INSERT INTO events VALUES ");
    for k in 0..N {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!(
            "({k}, 'active', {k}, '{{\"k\":{k},\"tag\":\"t{m}\"}}')",
            m = k % 7
        ));
    }
    sess.execute(&stmt).await.expect("seed 10k rows");
    shard.flush_to_parquet().await.expect("flush to cold");

    // Bind the catalog-driven cold listing (flush + fast path skip refresh).
    let _ = sess.execute("SELECT 1 FROM events LIMIT 0").await.unwrap();

    // ── Plant the overlay: ONE fast-path single-row UPDATE ──────────────────
    // id=5000's status becomes 'pinned'. This writes a MemRowValue::Update
    // override whose IPC-decoded payload column is LargeBinary, diverging from
    // the Binary the cold Vortex scan produces for every other row.
    with_update_fastpath_on(async {
        sess.execute("UPDATE events SET status = 'pinned' WHERE id = 5000")
            .await
            .expect("fast-path single-row UPDATE must succeed");
    })
    .await;

    // ── (a) Bulk UPDATE by range — the reported failing statement ───────────
    // Must succeed (previously: concat batches for rewrite Binary/LargeBinary).
    sess.execute("UPDATE events SET status = 'expired' WHERE created_at < 3000")
        .await
        .expect("bulk UPDATE by range must succeed (was Binary/LargeBinary concat failure)");

    // Re-bind the listing after the rewrite.
    let _ = sess.execute("SELECT 1 FROM events LIMIT 0").await.unwrap();

    // ── Correctness of the bulk UPDATE ──────────────────────────────────────
    // Every row with created_at < 3000 (ids 0..3000) is now 'expired'.
    assert_eq!(
        count_where(&sess, "status = 'expired'").await,
        3000,
        "all rows with created_at < 3000 must be 'expired' after bulk UPDATE"
    );
    // The overlay row id=5000 was created_at=5000 (NOT < 3000) so the bulk
    // UPDATE did not touch it; its overlay status 'pinned' must survive.
    assert_eq!(
        status_for_id(&sess, 5000).await,
        "pinned",
        "overlay-set status must survive a non-overlapping bulk UPDATE"
    );
    // A row inside the range keeps its (now updated) status and original
    // payload — the JSONB column round-tripped through the rewrite intact.
    assert_eq!(status_for_id(&sess, 100).await, "expired");
    assert_eq!(
        payload_for_id(&sess, 100).await,
        "{\"k\":100,\"tag\":\"t2\"}",
        "JSONB payload must survive the copy-on-write rewrite unchanged"
    );
    // A row outside the range is untouched.
    assert_eq!(status_for_id(&sess, 9000).await, "active");

    // ── (b) Filtered SELECT + COUNT(*) — the poisoned GAP rows ──────────────
    // A selective SELECT * with a predicate + LIMIT must succeed and return
    // catalog-typed JSONB (the scan-path Binary/LargeBinary GAP).
    let res = sess
        .execute(
            "SELECT id, status, payload FROM events WHERE created_at < 50 ORDER BY id LIMIT 10",
        )
        .await
        .expect("filtered SELECT * must succeed (scan-path Binary/LargeBinary GAP)");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("SELECT returned non-Rows");
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 10,
        "ORDER BY id LIMIT 10 must return exactly 10 rows"
    );

    // COUNT(*) WHERE over the JSONB table must succeed.
    assert_eq!(
        count_where(&sess, "created_at >= 9000").await,
        1000,
        "COUNT(*) WHERE over JSONB table must succeed and be correct"
    );
    // Total row count is stable (no rows lost/duplicated by the rewrite).
    assert_eq!(count_where(&sess, "id >= 0").await, N);

    bg.shutdown().await;
    wal.close().await.unwrap();
}
