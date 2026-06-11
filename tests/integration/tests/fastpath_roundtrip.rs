//! Round-trip correctness tests for the OLTP fastpaths
//! (`BASIN_HOTTIER_DELETE_FASTPATH` / `BASIN_HOTTIER_UPDATE_FASTPATH`).
//!
//! These tests prove the seven canonical shapes that must hold before the
//! fastpath env vars can be flipped to ON by default:
//!
//! 1. `delete_fastpath_then_select_excludes_row`        — DELETE removes the row.
//! 2. `update_fastpath_then_select_returns_new_value`   — UPDATE shows the new value.
//! 3. `update_fastpath_then_select_no_duplicate_row`    — no cold + hot duplicate.
//! 4. `update_fastpath_then_count_unchanged`            — UPDATE is row-count-neutral.
//! 5. `delete_fastpath_then_count_excludes_tombstone`   — DELETE reduces the count.
//! 6. `delete_then_reinsert_same_pk_visible`            — re-INSERT after DELETE visible.
//! 7. `tombstone_survives_compaction`                   — tombstone survives flush+compact.
//!
//! ## Environment isolation
//!
//! `BASIN_HOTTIER_DELETE_FASTPATH` / `BASIN_HOTTIER_UPDATE_FASTPATH` are
//! process-wide env vars. Tests here hold `ENV_LOCK` (a `tokio::sync::Mutex`)
//! across every await so parallel tests in this binary don't race on the gate.
//!
//! ## Engine shape
//!
//! Uses the simple `engine_in(dir)` pattern (no shard, no WAL) — the
//! fastpath writes directly to the process-wide `MemTableRegistry` and the
//! merge-on-read overlay is exercised by the plain `ProjectSession::execute`
//! path.  The `tombstone_survives_compaction` test uses the shard + WAL shape
//! from `pk_point_probe_shard.rs` to exercise the flush path.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Env isolation ─────────────────────────────────────────────────────────────

/// Serialises env-var mutation across parallel test threads. A tokio mutex so
/// it can be held across `.await` points (the gate must stay set while DML
/// futures run).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// RAII guard: restores `key` to `prev` on drop.
struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: ENV_LOCK is held for the duration of the test; single-threaded
        // ownership of the env var is guaranteed for this binary.
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

fn set_env(key: &'static str) -> EnvGuard {
    let prev = std::env::var(key).ok();
    // SAFETY: ENV_LOCK is held by the caller (obtained before calling this).
    unsafe { std::env::set_var(key, "1") };
    EnvGuard { key, prev }
}

// ── Engine helpers ────────────────────────────────────────────────────────────

/// Lightweight engine: no shard, no WAL — suitable for all round-trip tests
/// except `tombstone_survives_compaction`.
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

/// Full engine with shard + WAL — mirrors `pk_point_probe_shard.rs`.
async fn build_engine_with_shard() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(
            LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
        ),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(
                LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap(),
            ),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, bg, wal)
}

/// Shard + WAL engine with **no background flush task spawned**. The INSERT
/// tail therefore never auto-drains, so a test can observe `has_pending_tail`
/// being true and assert the synchronous read-your-writes flush gate. Returns
/// a clone of the `Shard` handle so the test can probe the tail directly.
async fn build_engine_with_unflushed_shard() -> (TempDir, TempDir, Engine, Shard, Arc<dyn Wal>) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_secs(3600),
            flush_max_bytes: 1024 * 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    // Deliberately do NOT call `shard.spawn_background()` — the tail must stay
    // resident so the read-your-writes gate is the only thing that can flush it.
    let shard_probe = shard.clone();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    (storage_dir, wal_dir, engine, shard_probe, wal)
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

// ── Read helpers ──────────────────────────────────────────────────────────────

/// `SELECT COUNT(*) FROM t` → i64.
async fn count_all(sess: &ProjectSession, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let b = batches.first().expect("COUNT(*) no batches");
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT(*) must be Int64")
                .value(0)
        }
        other => panic!("COUNT(*) got {other:?}"),
    }
}

/// Total row count from a `SELECT *`.
async fn row_count(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// `SELECT v FROM t WHERE id = pk` → Some(value) or None.
async fn fetch_v(sess: &ProjectSession, table: &str, pk: i64) -> Option<i64> {
    let sql = format!("SELECT v FROM {table} WHERE id = {pk}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("v must be Int64");
                return Some(arr.value(0));
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

/// Seed `(id BIGINT PRIMARY KEY, v BIGINT)` with `n` rows `(i, i * 10)`.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
    )
    .await;
    let batch = 500_i64;
    let mut i = 1_i64;
    while i <= n {
        let lo = i;
        let hi = (i + batch - 1).min(n);
        let mut stmt = format!("INSERT INTO {table} (id, v) VALUES ");
        let mut first = true;
        for k in lo..=hi {
            if !first {
                stmt.push(',');
            }
            first = false;
            stmt.push_str(&format!("({k}, {})", k * 10));
        }
        exec(sess, &stmt).await;
        i = hi + 1;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// INSERT → DELETE (fastpath) → SELECT same PK returns 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_fastpath_then_select_excludes_row() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 10).await;

    exec(&sess, "DELETE FROM t WHERE id = 5").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id = 5").await;
    assert_eq!(
        n, 0,
        "fastpath DELETE must suppress the cold-tier row on SELECT; \
         got {n} rows — tombstone overlay not applied"
    );
}

/// INSERT → UPDATE (fastpath) → SELECT returns the new column value, exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_fastpath_then_select_returns_new_value() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 10).await;

    exec(&sess, "UPDATE t SET v = 99999 WHERE id = 3").await;

    let v = fetch_v(&sess, "t", 3).await;
    assert_eq!(
        v,
        Some(99999),
        "fastpath UPDATE must surface the new value on point lookup; \
         got {v:?} — UpdateOverlay not applied"
    );
}

/// Same as above but explicitly asserts row count = 1 (no cold + hot duplicate).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_fastpath_then_select_no_duplicate_row() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 10).await;

    exec(&sess, "UPDATE t SET v = 77777 WHERE id = 7").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id = 7").await;
    assert_eq!(
        n, 1,
        "fastpath UPDATE must produce exactly 1 row for a PK lookup; \
         got {n} — the cold-tier row and the hot-tier override are both visible \
         (UpdateOverlay is not suppressing the stale cold row)"
    );
}

/// INSERT → UPDATE (fastpath) → SELECT COUNT(*) returns the original row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_fastpath_then_count_unchanged() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 20).await;

    let before = count_all(&sess, "t").await;
    exec(&sess, "UPDATE t SET v = 1 WHERE id = 10").await;
    let after = count_all(&sess, "t").await;

    assert_eq!(before, 20);
    assert_eq!(
        after, 20,
        "UPDATE must not change COUNT(*); got {after} (before={before})"
    );
}

/// INSERT N → DELETE 1 (fastpath) → SELECT COUNT(*) = N-1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_fastpath_then_count_excludes_tombstone() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 50).await;

    exec(&sess, "DELETE FROM t WHERE id IN (10, 20, 30)").await;

    let n = count_all(&sess, "t").await;
    assert_eq!(
        n, 47,
        "COUNT(*) after fastpath DELETE of 3 rows must be 47; got {n} \
         — tombstone overlay not reducing the count"
    );
}

/// INSERT → DELETE (fastpath) → INSERT same PK → SELECT returns the new row.
///
/// The tombstone must be overridden by the re-INSERT so the row is visible
/// with the new value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_then_reinsert_same_pk_visible() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 10).await;

    exec(&sess, "DELETE FROM t WHERE id = 5").await;

    // Re-insert same PK with a different value; must be visible.
    exec(&sess, "INSERT INTO t (id, v) VALUES (5, 9999)").await;

    let n = row_count(&sess, "SELECT * FROM t WHERE id = 5").await;
    assert_eq!(
        n, 1,
        "re-inserted row must be visible after fastpath DELETE of the same PK; \
         got {n} rows"
    );
    let v = fetch_v(&sess, "t", 5).await;
    assert_eq!(
        v,
        Some(9999),
        "re-inserted row must show the new value 9999; got {v:?}"
    );
}

/// INSERT → DELETE (fastpath) → force flush+compact → SELECT same PK = 0 rows.
///
/// The tombstone must survive the compaction path (materialised as a removed
/// cold-tier file) so the deleted row is not resurrected post-compaction.
///
/// NOTE: this test is marked #[ignore] because it depends on the compaction
/// path consuming memtable tombstones into the cold tier — not yet
/// implemented. Un-ignore when the compaction path emits a deletion vector
/// or drops the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "blocked on compaction consuming memtable tombstones into cold-tier deletion vectors"]
async fn tombstone_survives_compaction() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let (_sd, _wd, engine, bg, wal) = build_engine_with_shard().await;
    let sess = open(&engine).await;

    exec(
        &sess,
        "CREATE TABLE tsc (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Seed rows.
    let mut stmt = "INSERT INTO tsc (id, v) VALUES ".to_string();
    for k in 1i64..=100 {
        if k > 1 { stmt.push(','); }
        stmt.push_str(&format!("({k}, {})", k * 10));
    }
    exec(&sess, &stmt).await;

    // Fastpath DELETE.
    exec(&sess, "DELETE FROM tsc WHERE id = 42").await;

    // Force a shard flush + compaction by waiting for the background flush
    // cycle to drain. The shard background flushes the WAL on its interval;
    // we give it a generous window and then trigger a manual flush via the
    // compaction API if available. Otherwise rely on the background interval.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Post-compaction: deleted row must remain absent.
    let n = row_count(&sess, "SELECT * FROM tsc WHERE id = 42").await;
    assert_eq!(
        n, 0,
        "tombstone must survive compaction: row 42 must remain absent; \
         got {n} rows — tombstone was lost during the flush/compact cycle"
    );

    let total = count_all(&sess, "tsc").await;
    assert_eq!(
        total, 99,
        "COUNT(*) post-compaction must be 99; got {total}"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Read-your-writes through the shard INSERT tail (#205).
///
/// INSERT populates the shard *tail*, not the memtable. A fast-path UPDATE's
/// read-before-write reads memtable + cold (NOT the tail), so if the eager
/// `pre_mutation_flush` were skipped unconditionally, a just-INSERTed-same-row
/// would be invisible to the UPDATE's read and the UPDATE would no-op — a
/// read-your-writes regression.
///
/// The fix gates the flush on `Shard::has_pending_tail`: when a tail exists the
/// flush still fires synchronously *before* the fast-path read, so the row is
/// drained to cold and the UPDATE sees it. This test runs with **no background
/// flush task spawned**, so the only thing that can drain the tail is the
/// gate itself. We assert: (1) the tail is genuinely pending right after the
/// INSERT, and (2) the UPDATE in the same session takes effect on SELECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_then_update_same_row_with_pending_tail_takes_effect() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let (_sd, _wd, engine, shard, wal) = build_engine_with_unflushed_shard().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE ryw (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // INSERT lands in the shard tail (no flush task is running to drain it).
    exec(&sess, "INSERT INTO ryw (id, v) VALUES (1, 10)").await;

    // Sanity: the tail is genuinely pending and un-flushed at this point —
    // otherwise the test would not be exercising the gate it claims to.
    let table = basin_common::TableName::new("ryw").unwrap();
    assert!(
        shard.has_pending_tail(&project, &table).await,
        "INSERT must leave a pending tail before the UPDATE — \
         the read-your-writes gate is only meaningful with a tail present"
    );

    // UPDATE the just-INSERTed row in the same session. The gate must flush the
    // tail first so the fast-path read sees v=10 and writes the override.
    exec(&sess, "UPDATE ryw SET v = 999 WHERE id = 1").await;

    let v = fetch_v(&sess, "ryw", 1).await;
    assert_eq!(
        v,
        Some(999),
        "UPDATE of a just-INSERTed (tail-resident) row must take effect; \
         got {v:?} — the has_pending_tail flush gate failed to surface the \
         tail row to the UPDATE's read-before-write (read-your-writes break)"
    );

    // Row count must be exactly 1 (no duplicate, no resurrection).
    let n = row_count(&sess, "SELECT * FROM ryw WHERE id = 1").await;
    assert_eq!(n, 1, "expected exactly one row for id=1; got {n}");

    wal.close().await.unwrap();
}
