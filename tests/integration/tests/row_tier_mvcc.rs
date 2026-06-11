//! S4 row-tier MVCC version chains — end-to-end isolation guarantees that the
//! single-version memtable could NOT provide.
//!
//! Background (`crates/basin-hottier/src/memtable.rs`): the memtable now keeps a
//! per-key MVCC **version chain** instead of one value per key. An overlay
//! overwrite PUSHES a new version and RETAINS the prior one, so a transaction
//! that pinned its `hot_seq_watermark` before the overwrite can still resolve
//! the version it is entitled to (`get_with_seq` / `snapshot_with_seq` at the
//! watermark). Previously the overwrite destroyed the prior version, so a pinned
//! reader whose entitled image was itself a HOT overlay version saw `None` and
//! fell through to the (older) cold image — a documented isolation residual.
//!
//! The decisive new case here: A's entitled value is a HOT overlay version (B
//! wrote it before A's BEGIN), and B then overwrites the SAME key one or more
//! times after A pinned. With a single-version memtable the first override is
//! destroyed and A regresses to the cold row; with version chains A keeps seeing
//! its pinned overlay version. Mirrors `tx_overlay_isolation.rs`'s two-session
//! pattern and forces the auto-commit fast paths ON.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across parallel tests in this binary (the
// `BASIN_HOTTIER_*_FASTPATH` gates are process-wide; same convention as
// `tx_overlay_isolation.rs`).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ── Engine / session builders ─────────────────────────────────────────────────

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

async fn open(eng: &Engine, project: ProjectId) -> ProjectSession {
    eng.open_session(project).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Run `fut` with BOTH auto-commit fast paths forced ON for its duration, under
/// the env lock so other tests never observe the flipped gate.
async fn with_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev_u = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    let prev_d = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    let prev_kill = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE");
    let out = fut.await;
    restore("BASIN_HOTTIER_UPDATE_FASTPATH", prev_u);
    restore("BASIN_HOTTIER_DELETE_FASTPATH", prev_d);
    restore("BASIN_HOTTIER_FASTPATH_DISABLE", prev_kill);
    out
}

fn restore(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

// ── Read helpers ───────────────────────────────────────────────────────────────

fn int_value(c: &Arc<dyn Array>, row: usize) -> i64 {
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        a.value(row)
    } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
        a.value(row) as i64
    } else {
        panic!("column is not an int array: {:?}", c.data_type());
    }
}

/// `SELECT val FROM t WHERE id = <id>` → single value (None when absent).
async fn val_at(sess: &ProjectSession, table: &str, id: i64) -> Option<i64> {
    let sql = format!("SELECT val FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    for b in &batches {
        if b.num_rows() > 0 {
            return Some(int_value(b.column(0), 0));
        }
    }
    None
}

async fn count_rows(sess: &ProjectSession, table: &str) -> i64 {
    match sess
        .execute(&format!("SELECT count(*) FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => int_value(batches[0].column(0), 0),
        other => panic!("expected rows from count(*), got {other:?}"),
    }
}

/// Seed `(id INT PRIMARY KEY, val INT)` with rows `(i, i*10)`, committed cold.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id INT PRIMARY KEY, val INT)"),
    )
    .await;
    for i in 1..=n {
        exec(
            sess,
            &format!("INSERT INTO {table} (id, val) VALUES ({i}, {})", i * 10),
        )
        .await;
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// THE previously-impossible case. A's entitled value for row 3 is a HOT overlay
/// version (B wrote val=100 before A's BEGIN). After A pins, B overwrites row 3
/// TWICE more (200, then 300). With the single-version memtable the first
/// override (val=100, the one A is entitled to) was DESTROYED by the later
/// pushes and A regressed to the cold row; with version chains the val=100
/// version is RETAINED and A keeps reading it. After COMMIT A sees the live head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_overlay_version_survives_two_subsequent_overwrites() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;

        // B writes a HOT overlay override BEFORE A's tx — this is the version A
        // will be entitled to once it pins.
        exec(&b, "UPDATE t SET val = 100 WHERE id = 3").await;
        assert_eq!(val_at(&b, "t", 3).await, Some(100), "B's pre-tx override");

        // A opens a tx and reads row 3 — pins its hot-tier watermark AT the
        // val=100 version.
        exec(&a, "BEGIN").await;
        assert_eq!(
            val_at(&a, "t", 3).await,
            Some(100),
            "A pins the hot overlay version (val=100)"
        );

        // B overwrites the SAME key TWICE more, after A pinned.
        exec(&b, "UPDATE t SET val = 200 WHERE id = 3").await;
        exec(&b, "UPDATE t SET val = 300 WHERE id = 3").await;
        assert_eq!(val_at(&b, "t", 3).await, Some(300), "B sees its latest write");

        // A re-reads: MUST still see val=100. This is the chain payoff — the
        // version A pinned was overwritten twice but is retained, not destroyed.
        // (Single-version memtable would surface the cold row here, NOT 100.)
        assert_eq!(
            val_at(&a, "t", 3).await,
            Some(100),
            "A's pinned overlay version must survive two later overwrites"
        );
        // A second re-read is just as stable.
        assert_eq!(val_at(&a, "t", 3).await, Some(100), "still stable");

        exec(&a, "COMMIT").await;
        assert_eq!(
            val_at(&a, "t", 3).await,
            Some(300),
            "post-COMMIT A sees the live head"
        );
    })
    .await;
}

/// Single subsequent overwrite of a pinned overlay version (the 1-overwrite
/// point of the 1..3 sweep). A pins B's val=100 override, B overwrites once to
/// 200; A must still see 100.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_overlay_version_survives_one_overwrite() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;
        exec(&b, "UPDATE t SET val = 100 WHERE id = 4").await;

        exec(&a, "BEGIN").await;
        assert_eq!(val_at(&a, "t", 4).await, Some(100), "A pins val=100");

        exec(&b, "UPDATE t SET val = 200 WHERE id = 4").await;

        assert_eq!(
            val_at(&a, "t", 4).await,
            Some(100),
            "A's pinned overlay version survives one overwrite"
        );
        exec(&a, "COMMIT").await;
        assert_eq!(val_at(&a, "t", 4).await, Some(200), "post-COMMIT live head");
    })
    .await;
}

/// Three subsequent overwrites (the top of the 1..3 sweep).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_overlay_version_survives_three_overwrites() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;
        exec(&b, "UPDATE t SET val = 100 WHERE id = 2").await;

        exec(&a, "BEGIN").await;
        assert_eq!(val_at(&a, "t", 2).await, Some(100), "A pins val=100");

        for v in [200, 300, 400] {
            exec(&b, &format!("UPDATE t SET val = {v} WHERE id = 2")).await;
        }
        assert_eq!(val_at(&b, "t", 2).await, Some(400), "B's latest");

        assert_eq!(
            val_at(&a, "t", 2).await,
            Some(100),
            "A's pinned overlay version survives three overwrites"
        );
        exec(&a, "COMMIT").await;
        assert_eq!(val_at(&a, "t", 2).await, Some(400), "post-COMMIT live head");
    })
    .await;
}

/// Delete-then-reinsert chain. A pins B's val=100 override. B then DELETEs the
/// row (tombstone version) and re-INSERTs it as a brand-new value. A must keep
/// seeing val=100 throughout; the tombstone and the resurrection are both newer
/// than A's watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_reader_unaffected_by_delete_then_reinsert_chain() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 5).await;
        exec(&b, "UPDATE t SET val = 100 WHERE id = 1").await;

        exec(&a, "BEGIN").await;
        assert_eq!(val_at(&a, "t", 1).await, Some(100), "A pins val=100");
        let c0 = count_rows(&a, "t").await;

        // B deletes row 1 (tombstone version), then re-inserts it with a new val.
        exec(&b, "DELETE FROM t WHERE id = 1").await;
        assert_eq!(val_at(&b, "t", 1).await, None, "B sees its delete");
        exec(&b, "INSERT INTO t (id, val) VALUES (1, 777)").await;
        assert_eq!(val_at(&b, "t", 1).await, Some(777), "B sees the reinsert");

        // A's pinned read is unmoved by either: tombstone and reinsert post-date
        // A's watermark; the val=100 version it pinned is retained.
        assert_eq!(
            val_at(&a, "t", 1).await,
            Some(100),
            "A unaffected by B's delete-then-reinsert chain"
        );
        // COUNT(*) stays stable under the churn (no row appeared/disappeared for A).
        assert_eq!(
            count_rows(&a, "t").await,
            c0,
            "A's in-tx COUNT(*) stable under delete+reinsert churn"
        );

        exec(&a, "COMMIT").await;
        assert_eq!(val_at(&a, "t", 1).await, Some(777), "post-COMMIT live head");
    })
    .await;
}

/// Unpinned (auto-commit) readers ALWAYS observe the newest version, regardless
/// of how many overwrites preceded — the chain never delays the head for a
/// non-transactional read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpinned_reader_always_sees_newest() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&b, "t", 5).await;
        for v in [11, 22, 33, 44] {
            exec(&b, &format!("UPDATE t SET val = {v} WHERE id = 3")).await;
            assert_eq!(
                val_at(&b, "t", 3).await,
                Some(v),
                "auto-commit reader sees newest version after each overwrite"
            );
        }
    })
    .await;
}

/// COUNT(*) stability under pure overwrite churn in an open tx: B overwrites
/// several distinct rows multiple times after A pinned; the row COUNT A sees
/// must not move (overwrites change values, never cardinality).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_tx_count_stable_under_overwrite_churn() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let a = open(&eng, project).await;
    let b = open(&eng, project).await;

    with_fastpath_on(async {
        seed(&a, "t", 8).await;

        exec(&a, "BEGIN").await;
        let c0 = count_rows(&a, "t").await;
        assert_eq!(c0, 8, "A's pinned count");

        // B churns several rows, each overwritten twice, after A pinned.
        for id in 1..=5 {
            exec(&b, &format!("UPDATE t SET val = {} WHERE id = {id}", id * 1000)).await;
            exec(&b, &format!("UPDATE t SET val = {} WHERE id = {id}", id * 2000)).await;
        }

        // A's count is unchanged through the churn.
        assert_eq!(
            count_rows(&a, "t").await,
            c0,
            "A's in-tx COUNT(*) stable under overwrite churn"
        );

        exec(&a, "COMMIT").await;
        assert_eq!(count_rows(&a, "t").await, 8, "post-COMMIT count unchanged");
    })
    .await;
}
