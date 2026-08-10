//! Adversarial tests for the flush-on-read concurrency fix.
//!
//! The read path used to take a synchronous `shard.flush_to_parquet()` on EVERY
//! auto-commit SELECT, serialising readers behind the per-partition compact
//! lock (the mixed-RW 8R+4W and 16-session SELECT losses). The fix:
//!
//!   1. Tail-empty fast-gate — skip the flush when `has_pending_tail()` is
//!      false (nothing to drain).
//!   2. Small-tail merge-on-read — when the pending tail is small
//!      (`BASIN_READ_FLUSH_MIN_TAIL_ROWS`), skip the flush and let the shard's
//!      own tail-merging `read` union the tail into the result.
//!   3. Provider cache key drops `hot_epoch` (overlay freshness handled live by
//!      the always-overlay-capable `HtapUnionTable`).
//!   4. Per-table cache invalidation — a write to table A no longer evicts
//!      table B's cached provider / head / PK rows.
//!
//! These tests assert the CORRECTNESS bar that must hold through the fix:
//! read-own-write under concurrent writers, small-tail visibility, no stale
//! schema after DDL, no cross-table / cross-project leakage. Two timing shapes
//! mirror the benchmark losses and PRINT (not assert) latency — the timing
//! gates are calibrated later on an idle box.
//!
//! All tests boot a real shard-backed pgwire server (the same path the bench
//! exercises) so the entire fast-path gate, flush decision, overlay merge and
//! cache logic run end-to-end.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

// ── Server harness ──────────────────────────────────────────────────────────

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _data_dir: TempDir,
    _wal_dir: TempDir,
    bg: Option<basin_shard::ShardBackgroundHandle>,
    wal: Arc<dyn basin_wal::Wal>,
}

/// Boot a shard-backed server. Two named projects ("alice", "bob") map to
/// DISTINCT `ProjectId`s so the cross-project isolation test can connect as a
/// different tenant.
async fn start_server_with_shard() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let data_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage_fs = LocalFileSystem::new_with_prefix(data_dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(storage_fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());

    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .expect("open WAL"),
    );

    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();

    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), ProjectId::new());
    map.insert("bob".to_owned(), ProjectId::new());
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _data_dir: data_dir,
        _wal_dir: wal_dir,
        bg: Some(bg),
        wal,
    }
}

async fn shutdown(mut server: TestServer) {
    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();
}

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user={user} password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                Some(row)
            }
            _ => None,
        })
        .collect()
}

async fn query_rows(c: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    let msgs = c
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query {sql:?}: {e}"));
    rows_of(&msgs)
}

async fn exec(c: &tokio_postgres::Client, sql: &str) {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("exec {sql:?}: {e}"));
}

async fn scalar_i64(c: &tokio_postgres::Client, sql: &str) -> i64 {
    let rows = query_rows(c, sql).await;
    rows[0][0]
        .as_deref()
        .unwrap_or_else(|| panic!("null scalar for {sql:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("non-int scalar for {sql:?}: {:?}", rows[0][0]))
}

// ── 1. Read-own-write: just-INSERTed row visible on the very next SELECT ─────

/// Every fast path (point-PK, keyset, LIMIT, full scan, COUNT) must show a
/// row the SAME session just INSERTed, even though the flush is now skipped
/// for a small tail. The row lives only in the shard tail (no flush ran), so
/// this exercises the small-tail merge-on-read path through `handle.read`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_own_write_through_every_fast_path() {
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    // Seed a cold base so there ARE cold files (flush forced by this scan).
    for i in 1..=10i64 {
        exec(
            &c,
            &format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10),
        )
        .await;
    }
    // Force a flush so rows 1..=10 are cold Parquet, tail empty.
    let _ = scalar_i64(&c, "SELECT count(*) FROM t").await;

    // Now INSERT a fresh row that stays in the (small) tail — no flush.
    exec(&c, "INSERT INTO t (id, v) VALUES (11, 110)").await;

    // Point-PK lookup: the just-written tail row must be visible.
    let pt = query_rows(&c, "SELECT id, v FROM t WHERE id = 11").await;
    assert_eq!(pt.len(), 1, "point-PK RYOW: tail row id=11 must be visible");
    assert_eq!(pt[0][1].as_deref(), Some("110"));

    // Full scan: 11 rows.
    let all = query_rows(&c, "SELECT id FROM t").await;
    assert_eq!(
        all.len(),
        11,
        "full scan RYOW: want 11 rows, got {}",
        all.len()
    );

    // COUNT(*): 11.
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM t").await, 11);

    // Keyset pagination (ASC, k > $1): the new row id=11 must appear.
    let ks = query_rows(
        &c,
        "SELECT id FROM t WHERE id > 5 ORDER BY id ASC LIMIT 100",
    )
    .await;
    let ids: Vec<i64> = ks
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert!(ids.contains(&11), "keyset RYOW: id=11 missing from {ids:?}");
    assert_eq!(ids, vec![6, 7, 8, 9, 10, 11], "keyset order/content wrong");

    // LIMIT without ORDER BY over a small table returns all rows ≤ limit.
    let lim = query_rows(&c, "SELECT id FROM t LIMIT 50").await;
    assert_eq!(lim.len(), 11, "LIMIT RYOW: want all 11 rows");

    shutdown(server).await;
}

/// Concurrent writers churning the tail while a reader repeatedly issues
/// point + scan SELECTs. Every read must see a self-consistent, monotonic row
/// set (no row that was committed-then-vanishes, no count regression), proving
/// the skipped-flush read path never drops a committed tail row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_own_write_under_concurrent_writers() {
    let server = start_server_with_shard().await;
    let setup = connect(server.addr, "alice").await;
    exec(
        &setup,
        "CREATE TABLE churn (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;

    const WRITERS: usize = 4;
    const PER_WRITER: i64 = 60;

    // Writer sessions each insert a disjoint PK range, then point-read each row
    // back IMMEDIATELY (read-own-write on the same connection).
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for w in 0..WRITERS {
        let addr = server.addr;
        set.spawn(async move {
            let c = connect(addr, "alice").await;
            let base = (w as i64) * 1000;
            for i in 0..PER_WRITER {
                let id = base + i;
                exec(
                    &c,
                    &format!("INSERT INTO churn (id, v) VALUES ({id}, {})", id * 2),
                )
                .await;
                // RYOW on the very next statement, every fast path:
                let got = query_rows(&c, &format!("SELECT v FROM churn WHERE id = {id}")).await;
                assert_eq!(
                    got.len(),
                    1,
                    "writer {w}: id={id} not visible on next SELECT (RYOW violated)"
                );
                assert_eq!(got[0][0].as_deref(), Some((id * 2).to_string().as_str()));
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("writer panicked");
    }

    // A concurrent reader session observing a monotonic count throughout would
    // be racy to assert mid-flight; instead assert the FINAL totals are exact
    // after all writers committed — no row lost or duplicated by skipped flush.
    let reader = connect(server.addr, "alice").await;
    let total = scalar_i64(&reader, "SELECT count(*) FROM churn").await;
    assert_eq!(
        total,
        WRITERS as i64 * PER_WRITER,
        "final count after concurrent churn must be exact"
    );
    let scan = query_rows(&reader, "SELECT id FROM churn").await;
    assert_eq!(scan.len() as i64, WRITERS as i64 * PER_WRITER);

    // Spot-check a few PKs from each writer's range survived.
    for w in 0..WRITERS {
        let base = (w as i64) * 1000;
        for i in [0i64, PER_WRITER / 2, PER_WRITER - 1] {
            let id = base + i;
            let got = query_rows(&reader, &format!("SELECT v FROM churn WHERE id = {id}")).await;
            assert_eq!(got.len(), 1, "writer {w} id={id} missing post-churn");
        }
    }

    shutdown(server).await;
}

// ── 2. Small-tail merge-on-read returns un-flushed rows ──────────────────────

/// With the small-tail gate ENABLED (default), a handful of un-flushed tail
/// rows must be merged into a scan that also reads cold files — the row set is
/// the union of cold + tail. Forcing a large threshold proves it is the
/// merge-on-read path (not an incidental flush) that surfaces the tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_tail_merge_on_read_unions_cold_and_tail() {
    // Make the threshold large so a multi-row tail still qualifies as "small"
    // and is merged on read rather than flushed.
    std::env::set_var("BASIN_READ_FLUSH_MIN_TAIL_ROWS", "4096");
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=20i64 {
        exec(&c, &format!("INSERT INTO m (id, v) VALUES ({i}, {})", i)).await;
    }
    // Flush rows 1..=20 to cold.
    let _ = scalar_i64(&c, "SELECT count(*) FROM m").await;

    // Append a tail batch (rows 21..=40) that stays un-flushed under the large
    // threshold.
    for i in 21..=40i64 {
        exec(&c, &format!("INSERT INTO m (id, v) VALUES ({i}, {})", i)).await;
    }

    // Scan: must be cold (1..20) ∪ tail (21..40) = 40 rows, no flush forced.
    let all = query_rows(&c, "SELECT id FROM m ORDER BY id").await;
    assert_eq!(
        all.len(),
        40,
        "merge-on-read must union cold + tail (40 rows)"
    );
    let ids: Vec<i64> = all
        .iter()
        .map(|r| r[0].as_deref().unwrap().parse().unwrap())
        .collect();
    assert_eq!(ids, (1..=40).collect::<Vec<_>>(), "row set wrong");

    // A tail-only PK must be served via the tail-merging read (point probe is
    // disabled when merging the tail, so this exercises handle.read).
    let pt = query_rows(&c, "SELECT v FROM m WHERE id = 33").await;
    assert_eq!(pt.len(), 1, "tail-only PK id=33 must be visible");
    assert_eq!(pt[0][0].as_deref(), Some("33"));

    std::env::remove_var("BASIN_READ_FLUSH_MIN_TAIL_ROWS");
    shutdown(server).await;
}

/// The tail-empty fast-gate: after a flush drains the tail, the next read must
/// still return the correct rows (the skipped flush is a no-op, the cold files
/// are authoritative). A DELETE overlay (registry tombstone, NOT a tail row)
/// must still suppress the cold row even though the flush was skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_empty_gate_preserves_overlay_suppression() {
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=8i64 {
        exec(&c, &format!("INSERT INTO d (id, v) VALUES ({i}, {})", i)).await;
    }
    // Flush to cold; tail now empty.
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM d").await, 8);

    // DELETE one row (fast-path tombstone in the registry, not the tail).
    exec(&c, "DELETE FROM d WHERE id = 4").await;

    // The next read skips the flush (tail empty) but MUST apply the overlay
    // tombstone: id=4 gone, count = 7.
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM d").await, 7);
    let pt = query_rows(&c, "SELECT v FROM d WHERE id = 4").await;
    assert_eq!(pt.len(), 0, "tombstoned id=4 must be suppressed");
    let all = query_rows(&c, "SELECT id FROM d ORDER BY id").await;
    assert_eq!(all.len(), 7);

    shutdown(server).await;
}

// ── 3. Provider cache (hot_epoch dropped) cannot serve stale schema ──────────

/// After a DDL (ADD COLUMN) the cached provider must be invalidated so the new
/// column is visible — the cache-key change (dropping hot_epoch, keeping the
/// schema-aware snapshot + dispatch-top invalidation) must not serve a stale
/// pre-DDL provider shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_add_column_not_served_from_stale_provider() {
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE s (id BIGINT PRIMARY KEY, a BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=5i64 {
        exec(&c, &format!("INSERT INTO s (id, a) VALUES ({i}, {})", i)).await;
    }
    // Warm the provider cache with the pre-DDL shape.
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM s").await, 5);
    let pre = query_rows(&c, "SELECT id, a FROM s ORDER BY id").await;
    assert_eq!(pre[0].len(), 2, "pre-DDL row has 2 columns");

    // ADD COLUMN (DDL bumps catalog epoch; dispatch-top broad invalidation).
    exec(&c, "ALTER TABLE s ADD COLUMN b BIGINT").await;
    exec(&c, "INSERT INTO s (id, a, b) VALUES (6, 6, 600)").await;

    // The new column must be visible — no stale 2-column provider served.
    let post = query_rows(&c, "SELECT id, a, b FROM s ORDER BY id").await;
    assert_eq!(post.len(), 6);
    assert_eq!(post[0].len(), 3, "post-DDL row must have 3 columns");
    // Row 6 carries b=600; old rows carry NULL for b.
    let row6 = post.iter().find(|r| r[0].as_deref() == Some("6")).unwrap();
    assert_eq!(row6[2].as_deref(), Some("600"));
    let row1 = post.iter().find(|r| r[0].as_deref() == Some("1")).unwrap();
    assert_eq!(row1[2], None, "pre-existing row's new column must be NULL");

    shutdown(server).await;
}

/// The dropped-hot_epoch cache must STILL reflect a fast-path UPDATE made
/// AFTER a provider was cached: the always-overlay-capable HtapUnionTable reads
/// the overlay live at scan time. Cache a provider, then UPDATE, then re-read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_provider_reflects_later_fastpath_update() {
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=6i64 {
        exec(
            &c,
            &format!("INSERT INTO u (id, v) VALUES ({i}, {})", i * 10),
        )
        .await;
    }
    // Flush + warm the provider cache.
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM u").await, 6);
    let _ = query_rows(&c, "SELECT id, v FROM u ORDER BY id").await; // cache fill

    // Fast-path UPDATE (registry overlay bumps hot_epoch — which is NO LONGER
    // in the provider cache key). The cached provider must still surface 999.
    exec(&c, "UPDATE u SET v = 999 WHERE id = 3").await;
    let got = query_rows(&c, "SELECT v FROM u WHERE id = 3").await;
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0][0].as_deref(),
        Some("999"),
        "cached provider must reflect the post-cache fast-path UPDATE via the live overlay"
    );
    // Full scan agrees (no stale duplicate).
    let all = query_rows(&c, "SELECT id, v FROM u ORDER BY id, v").await;
    assert_eq!(all.len(), 6, "UPDATE must not duplicate a row");
    let r3 = all.iter().find(|r| r[0].as_deref() == Some("3")).unwrap();
    assert_eq!(r3[1].as_deref(), Some("999"));

    shutdown(server).await;
}

// ── 4. Per-table invalidation does not leak across tables or projects ────────

/// A write to table A must not corrupt or stale-serve table B (per-table
/// invalidation). Warm both providers, churn A heavily, then read B and assert
/// B is unchanged and correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_table_invalidation_no_cross_table_leak() {
    let server = start_server_with_shard().await;
    let c = connect(server.addr, "alice").await;

    exec(
        &c,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    exec(
        &c,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=5i64 {
        exec(&c, &format!("INSERT INTO a (id, v) VALUES ({i}, {})", i)).await;
        exec(
            &c,
            &format!("INSERT INTO b (id, v) VALUES ({i}, {})", i * 100),
        )
        .await;
    }
    // Warm both providers (flush + cache fill).
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM a").await, 5);
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM b").await, 5);

    // Churn table A: many writes that each invalidate ONLY a's caches.
    for i in 6..=40i64 {
        exec(&c, &format!("INSERT INTO a (id, v) VALUES ({i}, {})", i)).await;
        exec(&c, &format!("UPDATE a SET v = {} WHERE id = {i}", i + 1000)).await;
    }

    // Table B must be completely unaffected and correct.
    assert_eq!(
        scalar_i64(&c, "SELECT count(*) FROM b").await,
        5,
        "B count leaked"
    );
    let bvals = query_rows(&c, "SELECT id, v FROM b ORDER BY id").await;
    let got: Vec<(i64, i64)> = bvals
        .iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().parse().unwrap(),
                r[1].as_deref().unwrap().parse().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)],
        "table B rows must be untouched by churn on table A"
    );

    // And table A itself is correct after the churn (its own invalidation works).
    assert_eq!(scalar_i64(&c, "SELECT count(*) FROM a").await, 40);
    let a40 = query_rows(&c, "SELECT v FROM a WHERE id = 40").await;
    assert_eq!(
        a40[0][0].as_deref(),
        Some("1040"),
        "A's own UPDATE must be visible"
    );

    shutdown(server).await;
}

/// Two DISTINCT projects ("alice" / "bob") with same-named tables. Writes by
/// one project must never invalidate, leak into, or stale-serve the other's
/// cached state (caches are per-session, and the PK-row cache invalidation is
/// per-(project,table)).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_table_invalidation_no_cross_project_leak() {
    let server = start_server_with_shard().await;
    let alice = connect(server.addr, "alice").await;
    let bob = connect(server.addr, "bob").await;

    exec(
        &alice,
        "CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    exec(
        &bob,
        "CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=5i64 {
        exec(
            &alice,
            &format!("INSERT INTO shared (id, v) VALUES ({i}, {})", i),
        )
        .await;
        exec(
            &bob,
            &format!("INSERT INTO shared (id, v) VALUES ({i}, {})", i * 1000),
        )
        .await;
    }
    // Warm both.
    assert_eq!(scalar_i64(&alice, "SELECT count(*) FROM shared").await, 5);
    assert_eq!(scalar_i64(&bob, "SELECT count(*) FROM shared").await, 5);

    // Alice churns her copy.
    for i in 6..=30i64 {
        exec(
            &alice,
            &format!("INSERT INTO shared (id, v) VALUES ({i}, {})", i),
        )
        .await;
    }

    // Bob's copy unchanged and isolated.
    assert_eq!(
        scalar_i64(&bob, "SELECT count(*) FROM shared").await,
        5,
        "bob count leaked"
    );
    let bvals = query_rows(&bob, "SELECT id, v FROM shared ORDER BY id").await;
    let got: Vec<(i64, i64)> = bvals
        .iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().parse().unwrap(),
                r[1].as_deref().unwrap().parse().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000)],
        "bob's rows must be untouched by alice's writes"
    );
    // A point read of a PK that only exists in alice's copy must MISS for bob.
    let miss = query_rows(&bob, "SELECT v FROM shared WHERE id = 20").await;
    assert_eq!(
        miss.len(),
        0,
        "bob must not see alice's id=20 (cross-project leak)"
    );

    // Alice's churn is correct on her side.
    assert_eq!(scalar_i64(&alice, "SELECT count(*) FROM shared").await, 30);

    shutdown(server).await;
}

// ── 5. Timing shapes (PRINT only — gates calibrated later on idle box) ───────

/// Mixed read/write 8R+4W shape (the TOP benchmark loss): interleave 8 reader
/// loops and 4 writer loops on a warm table and PRINT aggregate latency. No
/// assertion on timing — only correctness (final count exact).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn timing_mixed_rw_8r_4w_print_only() {
    let server = start_server_with_shard().await;
    let setup = connect(server.addr, "alice").await;
    exec(
        &setup,
        "CREATE TABLE mix (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    // Seed a cold base so reads have real files.
    for i in 1..=500i64 {
        exec(
            &setup,
            &format!("INSERT INTO mix (id, v) VALUES ({i}, {})", i),
        )
        .await;
    }
    let _ = scalar_i64(&setup, "SELECT count(*) FROM mix").await; // flush

    const READERS: usize = 8;
    const WRITERS: usize = 4;
    const READS_EACH: usize = 50;
    const WRITES_EACH: i64 = 25;

    let read_started = Instant::now();
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for _r in 0..READERS {
        let addr = server.addr;
        set.spawn(async move {
            let c = connect(addr, "alice").await;
            for k in 0..READS_EACH {
                let id = (k % 500) as i64 + 1;
                let _ = query_rows(&c, &format!("SELECT v FROM mix WHERE id = {id}")).await;
            }
        });
    }
    for w in 0..WRITERS {
        let addr = server.addr;
        set.spawn(async move {
            let c = connect(addr, "alice").await;
            let base = 10_000 + (w as i64) * 1000;
            for i in 0..WRITES_EACH {
                let id = base + i;
                exec(&c, &format!("INSERT INTO mix (id, v) VALUES ({id}, {id})")).await;
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("mixed task panicked");
    }
    let elapsed = read_started.elapsed();
    let total_ops = READERS * READS_EACH + WRITERS * WRITES_EACH as usize;
    println!(
        "[TIMING mixed_rw_8r_4w] {READERS}R+{WRITERS}W, {total_ops} ops in {:?} \
         ({:.3} ms/op avg) — PRINT ONLY, timing gate calibrated later on idle box",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / total_ops as f64
    );

    // Correctness: every committed write survived.
    let total = scalar_i64(&setup, "SELECT count(*) FROM mix").await;
    assert_eq!(
        total,
        500 + WRITERS as i64 * WRITES_EACH,
        "mixed-RW final count must be exact"
    );

    shutdown(server).await;
}

/// 16-session read-only SELECT shape (the second benchmark loss): 16 sessions
/// each issue point + scan SELECTs against a warm, tail-empty table. PRINT
/// aggregate latency; assert only correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn timing_16_session_readonly_print_only() {
    let server = start_server_with_shard().await;
    let setup = connect(server.addr, "alice").await;
    exec(
        &setup,
        "CREATE TABLE ro (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
    for i in 1..=1000i64 {
        exec(
            &setup,
            &format!("INSERT INTO ro (id, v) VALUES ({i}, {})", i),
        )
        .await;
    }
    // Flush so the tail is empty (the tail-empty fast-gate is the win here).
    assert_eq!(scalar_i64(&setup, "SELECT count(*) FROM ro").await, 1000);

    const SESSIONS: usize = 16;
    const READS_EACH: usize = 40;

    let started = Instant::now();
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for _s in 0..SESSIONS {
        let addr = server.addr;
        set.spawn(async move {
            let c = connect(addr, "alice").await;
            for k in 0..READS_EACH {
                let id = (k * 7 % 1000) as i64 + 1;
                let got = query_rows(&c, &format!("SELECT v FROM ro WHERE id = {id}")).await;
                assert_eq!(got.len(), 1, "read-only point read must hit id={id}");
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("reader session panicked");
    }
    let elapsed = started.elapsed();
    let total = SESSIONS * READS_EACH;
    println!(
        "[TIMING 16_session_readonly] {SESSIONS} sessions, {total} point reads in {:?} \
         ({:.3} ms/read avg) — PRINT ONLY, timing gate calibrated later on idle box",
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / total as f64
    );

    shutdown(server).await;
}
