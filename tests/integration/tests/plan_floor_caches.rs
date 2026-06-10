//! Correctness tests for the per-(table, snapshot, hot-epoch) DataFusion
//! provider cache (Fix B+C) that cuts the small-query DataFusion floor.
//!
//! ## What the cache does
//!
//! On every auto-commit SELECT, `refresh_table_inner` used to rebuild each
//! referenced table's DataFusion provider from scratch: a bloom-laden
//! `TableMetadata` clone, schema conversion, per-file `ListingTable::try_new`,
//! the optional `HtapUnionTable` overlay wrapper, and `register_table`. The
//! provider cache memoises the FINAL built `Arc<dyn TableProvider>` keyed by
//! `(table, live_snapshot, hot_tier_epoch)`; a HIT only re-registers the cached
//! provider (cheap). A small per-session head-probe cache supplies the live
//! snapshot id on a hit without the bloom clone.
//!
//! ## Why these tests
//!
//! The cache is a pure correctness hazard if its invalidation surface is wrong.
//! Each test drives a stream of identical aggregate queries (`SUM`/`COUNT`,
//! which force the DataFusion scan path — NOT the metadata COUNT(*) shortcut)
//! across a mutation that MUST change the answer, and asserts the post-mutation
//! query observes the fresh result rather than a stale cached provider:
//!
//! * INSERT (new snapshot after flush) → fresh sum.
//! * fast-path UPDATE overlay (hot epoch bumps) → fresh value in the aggregate.
//! * fast-path DELETE tombstone (hot epoch bumps) → count drops.
//! * DDL ALTER (schema change / catalog epoch bump) → new column visible, no
//!   stale provider.
//! * in-tx writes are visible to in-tx reads (cache bypassed inside a tx).
//! * an RLS table returns the policy-filtered rows on every (cached) read.
//! * two tables stay independent (no cross-table key collision).
//!
//! A final smoke test prints first-vs-second timings for an identical query and
//! asserts only correctness (timing is environment-dependent on debug CI).
//!
//! ## Isolation
//!
//! `BASIN_HOTTIER_{UPDATE,DELETE}_FASTPATH` are process-wide env vars; tests
//! that flip them hold `ENV_LOCK` across the await (matching
//! `count_star_metadata_shortcut.rs`). Each test builds a fresh
//! `(TempDir, Engine, Session, ProjectId)` so the catalog + registry are
//! per-test isolated.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Serialises env-var mutation across the parallel test threads in this binary.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ─── harness ─────────────────────────────────────────────────────────────────

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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Run a scalar single-cell `i64` aggregate query (e.g. `SUM(v)` / `COUNT(*)`
/// with a WHERE that forces the scan path). Returns the first cell, treating a
/// NULL (empty table SUM) as 0.
async fn scalar_i64(sess: &ProjectSession, sql: &str) -> i64 {
    use arrow_array::Int64Array;
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed for {sql:?}: {e:?}"));
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows for {sql:?}, got {other:?}"),
    };
    let b = batches.first().expect("query returned no batches");
    let arr = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("first column must be Int64");
    if arr.is_null(0) {
        0
    } else {
        arr.value(0)
    }
}

async fn row_count(sess: &ProjectSession, sql: &str) -> usize {
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed for {sql:?}: {e:?}"));
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected Rows for {sql:?}, got {other:?}"),
    }
}

/// Create `(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)` and seed `1..=n`.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
    )
    .await;
    let mut stmt = format!("INSERT INTO {table} (id, v) VALUES ");
    for k in 1..=n {
        if k > 1 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k}, {})", k * 10));
    }
    exec(sess, &stmt).await;
}

/// A scan-forcing SUM over the whole table. `WHERE id >= 0` keeps it off the
/// metadata COUNT(*) shortcut and exercises the real DataFusion provider.
fn sum_sql(table: &str) -> String {
    format!("SELECT SUM(v) FROM {table} WHERE id >= 0")
}

// ─── tests ────────────────────────────────────────────────────────────────────

/// INSERT flushes a new snapshot; the provider-cache key includes the snapshot
/// id, so the post-INSERT aggregate must see the new rows (key miss → rebuild).
#[tokio::test]
async fn provider_cache_insert_visible_in_aggregate() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await; // sum = 10+20+30+40+50 = 150

    // First read populates the cache; second read is a HIT.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 150);
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 150);

    // New rows: id 6,7 → v 60,70. Snapshot advances → key miss → fresh sum.
    exec(&sess, "INSERT INTO t (id, v) VALUES (6, 60), (7, 70)").await;
    assert_eq!(
        scalar_i64(&sess, &sum_sql("t")).await,
        150 + 60 + 70,
        "post-INSERT aggregate served a stale cached provider (missing new rows)"
    );
}

/// Fast-path UPDATE writes an overlay to the memtable registry and bumps the
/// hot-tier epoch. The provider-cache key includes the hot epoch, so the
/// post-UPDATE aggregate must reflect the new value through the overlay-aware
/// provider (a cached plain ListingTable would miss the overlay).
#[tokio::test]
async fn provider_cache_fast_path_update_visible_in_aggregate() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await; // sum = 150

    // Warm + HIT.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 150);
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 150);

    // Fast-path UPDATE: id=3 v 30 → 1000. Delta = +970. Hot epoch bumps.
    exec(&sess, "UPDATE t SET v = 1000 WHERE id = 3").await;
    assert_eq!(
        scalar_i64(&sess, &sum_sql("t")).await,
        150 - 30 + 1000,
        "post-UPDATE aggregate served a stale cached provider (missing overlay)"
    );

    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
}

/// Fast-path DELETE writes a tombstone and bumps the hot-tier epoch; the
/// post-DELETE COUNT must drop (key miss via hot epoch → overlay-aware rebuild).
#[tokio::test]
async fn provider_cache_fast_path_delete_drops_count() {
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 5).await;

    let count_sql = "SELECT COUNT(*) FROM t WHERE id >= 0"; // scan path
    assert_eq!(scalar_i64(&sess, count_sql).await, 5);
    assert_eq!(scalar_i64(&sess, count_sql).await, 5);

    let res = sess
        .execute("DELETE FROM t WHERE id IN (1, 2)")
        .await
        .expect("fast-path DELETE must succeed");
    if let ExecResult::Empty { tag } = res {
        assert_eq!(tag, "DELETE 2", "expected fast-path DELETE of 2 rows");
    } else {
        panic!("expected Empty from DELETE, got {res:?}");
    }

    assert_eq!(
        scalar_i64(&sess, count_sql).await,
        3,
        "post-DELETE count served a stale cached provider (tombstones not applied)"
    );

    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
}

/// DDL `ALTER TABLE ... ADD COLUMN` bumps the catalog epoch and flushes the
/// caches at dispatch top. A query after the ALTER must see the new column and
/// must NOT serve a provider built against the old schema.
#[tokio::test]
async fn provider_cache_alter_add_column_no_stale_schema() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 3).await;

    // Warm the cache against the 2-column schema.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 60); // 10+20+30
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 60);

    exec(&sess, "ALTER TABLE t ADD COLUMN w BIGINT").await;
    // Pre-ALTER files lack `w`; reads must null-fill it (SUM over all-NULL
    // is NULL -> rendered 0 by the helper), and a post-ALTER write to the
    // new column must be visible through any cached provider.
    assert_eq!(
        scalar_i64(&sess, "SELECT SUM(w) FROM t WHERE id >= 0").await,
        0,
        "all-NULL new column must aggregate to NULL/0, not error"
    );
    exec(&sess, "UPDATE t SET w = 21 WHERE id = 1").await;
    assert_eq!(
        scalar_i64(&sess, "SELECT SUM(w) FROM t WHERE id >= 0").await,
        21,
        "post-ALTER query did not see the new column write — stale cached provider"
    );
    // Original column still correct.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 60);
}

/// Inside a transaction the provider cache is bypassed (and cleared at the tx
/// boundary), so an in-tx read sees the transaction's own uncommitted writes.
#[tokio::test]
async fn provider_cache_in_tx_reads_see_tx_writes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 3).await; // sum = 60

    // Warm the auto-commit cache before the tx.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 60);

    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (10, 100)").await;
    // In-tx read must see the +100 write (cache must not serve the pre-tx
    // auto-commit provider).
    assert_eq!(
        scalar_i64(&sess, &sum_sql("t")).await,
        160,
        "in-tx read did not see this tx's own write — cache leaked into the tx"
    );
    exec(&sess, "COMMIT").await;

    // After COMMIT the auto-commit path must see the committed row, and must
    // not resurrect the stale pre-tx cached provider.
    assert_eq!(
        scalar_i64(&sess, &sum_sql("t")).await,
        160,
        "post-COMMIT read served a stale pre-tx cached provider"
    );
}

/// A ROLLBACK clears the cache and restores the pre-tx state; the next
/// auto-commit read must not see the rolled-back write nor serve a tx-built
/// provider.
#[tokio::test]
async fn provider_cache_rollback_restores_state() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 3).await; // sum = 60

    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 60);

    exec(&sess, "BEGIN").await;
    exec(&sess, "INSERT INTO t (id, v) VALUES (10, 100)").await;
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, 160);
    exec(&sess, "ROLLBACK").await;

    assert_eq!(
        scalar_i64(&sess, &sum_sql("t")).await,
        60,
        "post-ROLLBACK read saw the rolled-back write or a stale tx provider"
    );
}

/// An RLS-enabled table is cacheable (RLS is a query-time rewrite, not baked
/// into the provider). Every (cached) read must return the policy-filtered row
/// set — the cache must never bypass RLS.
#[tokio::test]
async fn provider_cache_rls_table_stays_filtered() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec(
        &sess,
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL, v BIGINT NOT NULL)",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO docs (id, tenant, v) VALUES \
         (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40)",
    )
    .await;
    exec(&sess, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    exec(
        &sess,
        "CREATE POLICY tenant_a ON docs FOR ALL TO PUBLIC USING (tenant = 'a')",
    )
    .await;

    // Only tenant 'a' rows (ids 1 and 3) are visible. Repeat to exercise the
    // cache HIT path; RLS must hold on every read.
    for _ in 0..3 {
        assert_eq!(
            row_count(&sess, "SELECT id FROM docs WHERE id >= 0").await,
            2,
            "RLS filter not applied on a cached read"
        );
        assert_eq!(
            scalar_i64(&sess, "SELECT SUM(v) FROM docs WHERE id >= 0").await,
            40, // 10 + 30
            "RLS-filtered aggregate wrong on a cached read"
        );
    }
}

/// Two independent tables must not collide in the per-session cache: mutating
/// one must not change the other's cached answer, and each sees its own writes.
#[tokio::test]
async fn provider_cache_two_tables_independent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "a", 3).await; // sum = 60
    seed(&sess, "b", 4).await; // sum = 10+20+30+40 = 100

    // Warm both.
    assert_eq!(scalar_i64(&sess, &sum_sql("a")).await, 60);
    assert_eq!(scalar_i64(&sess, &sum_sql("b")).await, 100);

    // Mutate only `a`.
    exec(&sess, "INSERT INTO a (id, v) VALUES (9, 900)").await;

    assert_eq!(
        scalar_i64(&sess, &sum_sql("a")).await,
        960,
        "table `a` did not see its own new row"
    );
    assert_eq!(
        scalar_i64(&sess, &sum_sql("b")).await,
        100,
        "table `b` answer changed after mutating `a` — cache key collision"
    );
}

/// Smoke: the SECOND identical query should be no slower than the first by a
/// wide margin in the steady state (the provider cache skips the rebuild). We
/// PRINT the timings and assert only correctness — debug-mode CI timing is too
/// noisy to gate on.
#[tokio::test]
async fn provider_cache_second_query_smoke_timing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, "t", 2_000).await;
    // expected sum = 10 * (2000 * 2001 / 2) = 10 * 2_001_000 = 20_010_000
    let expected = 10i64 * (2_000 * 2_001 / 2);

    // One untimed warm-up to amortise first-touch codegen.
    assert_eq!(scalar_i64(&sess, &sum_sql("t")).await, expected);

    let t1 = Instant::now();
    let r1 = scalar_i64(&sess, &sum_sql("t")).await;
    let ms1 = t1.elapsed().as_secs_f64() * 1000.0;

    let t2 = Instant::now();
    let r2 = scalar_i64(&sess, &sum_sql("t")).await;
    let ms2 = t2.elapsed().as_secs_f64() * 1000.0;

    println!("provider_cache smoke: query1={ms1:.3}ms query2={ms2:.3}ms (cache HIT expected on both)");

    assert_eq!(r1, expected, "first query result wrong");
    assert_eq!(r2, expected, "second query result wrong");
}
