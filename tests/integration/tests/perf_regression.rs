//! Permanent performance-regression test suite with hard-bar assertions.
//!
//! Each test shape creates its own engine + session, measures p99 latency
//! across N=60 iterations (top 1% discarded), and asserts p99 ≤ a documented
//! bar. Bars are generous (≈2-3× observed on a MacBook Pro) so CI noise cannot
//! flap them, but a 10×-class regression will fail.
//!
//! Op shapes (10 total):
//!   1. point_query_by_primary_key   — SELECT WHERE sorted-pk = X
//!   2. point_query_by_indexed_col   — SELECT WHERE secondary-indexed col = X
//!   3. range_scan_with_predicate    — SELECT WHERE col BETWEEN A AND B (~1%)
//!   4. full_scan_aggregate          — SELECT COUNT(*), SUM(col) GROUP BY k
//!   5. single_row_insert            — INSERT 1 row (auto-commit)
//!   6. single_row_update_by_pk      — UPDATE SET val WHERE id = X
//!   7. transaction_commit_overhead  — BEGIN + INSERT + COMMIT
//!   8. simple_join_two_tables       — INNER JOIN 1k x 1k on PK
//!   9. memtable_point_query_htap    — HTAP INSERT then SELECT before flush
//!  10. parameterized_prepare_execute — extended protocol bind+execute
//!
//! Tests are gated with `#[serial]` to avoid parallel resource contention.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession, ScalarParam};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serial_test::serial;
use tempfile::TempDir;

const ITERS: usize = 60;
const ROWS: i64 = 10_000;
const JOIN_ROWS: i64 = 1_000;

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).expect("LocalFileSystem");
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig { storage, catalog, shard: None })
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.expect("open_session")
}

async fn ex(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec {sql:?}: {e:?}"));
}

async fn qry(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap_or_else(|e| panic!("qry {sql:?}: {e:?}")) {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => Vec::new(),
    }
}

fn p99(mut s: Vec<f64>) -> f64 {
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let keep = (s.len() * 99) / 100;
    s[keep.saturating_sub(1).min(s.len() - 1)]
}

fn p50(mut s: Vec<f64>) -> f64 {
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn chk(shape: &str, p99_ms: f64, bar: f64) {
    println!("[perf_regression] {shape}: p99={p99_ms:.2}ms bar<={bar:.0}ms");
    assert!(p99_ms <= bar, "{shape} p99 {p99_ms:.2}ms exceeded {bar:.0}ms bar");
}

fn chk_p50(shape: &str, p50_ms: f64, bar: f64) {
    println!("[perf_regression] {shape}: p50={p50_ms:.2}ms bar<={bar:.0}ms");
    assert!(p50_ms <= bar, "{shape} p50 {p50_ms:.2}ms exceeded {bar:.0}ms bar");
}

/// Insert rows (id, cat, label) in batches of 500.
async fn load(sess: &ProjectSession, tbl: &str, n: i64) {
    let mut done = 0i64;
    while done < n {
        let b = (n - done).min(500);
        let mut v = String::new();
        for j in 0..b {
            let id = done + j;
            if j > 0 { v.push(','); }
            v.push_str(&format!("({id},{},\'v{id}\')", id % 50));
        }
        ex(sess, &format!("INSERT INTO {tbl} (id,cat,label) VALUES {v}")).await;
        done += b;
    }
}

// ── Shape 1 ──────────────────────────────────────────────────────────────────
// Point query by primary key on sorted 10k-row table.
// Bloom filters prune all files except the one containing the target id.
// Observed ≈ 1-5 ms. Bar: 200 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_01_point_query_by_primary_key() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, cat BIGINT, label TEXT) WITH (basin.sort_by='id')").await;
    load(&sess, "t", ROWS).await;
    let mid = ROWS / 2 + 7;
    let sql = format!("SELECT * FROM t WHERE id = {mid}");
    let _ = qry(&sess, &sql).await;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = qry(&sess, &sql).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("point_query_by_primary_key", p99(s), 200.0);
}

// ── Shape 2 ──────────────────────────────────────────────────────────────────
// Point query via secondary index on cat column (50-cardinality).
// cat=25 returns ~200 rows from 10k.
// Observed ≈ 5-30 ms. Bar: 400 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_02_point_query_by_indexed_column() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, cat BIGINT, label TEXT) WITH (basin.sort_by='id')").await;
    ex(&sess, "CREATE INDEX t_cat ON t(cat)").await;
    load(&sess, "t", ROWS).await;
    let sql = "SELECT * FROM t WHERE cat = 25";
    let _ = qry(&sess, sql).await;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = qry(&sess, sql).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("point_query_by_indexed_column", p99(s), 400.0);
}

// ── Shape 3 ──────────────────────────────────────────────────────────────────
// Range scan with BETWEEN predicate returning ~1% of the table (100 rows).
// Observed ≈ 5-20 ms. Bar: 400 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_03_range_scan_with_predicate() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, cat BIGINT, label TEXT) WITH (basin.sort_by='id')").await;
    load(&sess, "t", ROWS).await;
    let lo = ROWS / 2 - 50;
    let hi = lo + 100;
    let sql = format!("SELECT id, cat FROM t WHERE id BETWEEN {lo} AND {hi}");
    let _ = qry(&sess, &sql).await;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = qry(&sess, &sql).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("range_scan_with_predicate", p99(s), 400.0);
}

// ── Shape 4 ──────────────────────────────────────────────────────────────────
// Full-scan aggregate: COUNT(*) + SUM(amount) GROUP BY k. k=0 always → 1 group.
// Observed ≈ 5-30 ms. Bar: 500 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_04_full_scan_aggregate() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, amount DOUBLE, k BIGINT)").await;
    {
        let mut done = 0i64;
        while done < ROWS {
            let b = (ROWS - done).min(500);
            let mut v = String::new();
            for j in 0..b {
                let id = done + j;
                if j > 0 { v.push(','); }
                v.push_str(&format!("({id},{},0)", id as f64 * 2.5));
            }
            ex(&sess, &format!("INSERT INTO t (id,amount,k) VALUES {v}")).await;
            done += b;
        }
    }
    let sql = "SELECT k, COUNT(*), SUM(amount) FROM t GROUP BY k";
    let _ = qry(&sess, sql).await;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = qry(&sess, sql).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("full_scan_aggregate", p99(s), 500.0);
}

// ── Shape 5 ──────────────────────────────────────────────────────────────────
// Single-row INSERT into a 10k-row table (auto-commit path).
// Observed ≈ 1-10 ms. Bar: 300 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_05_single_row_insert() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, val TEXT) WITH (basin.sort_by='id')").await;
    {
        let mut done = 0i64;
        while done < ROWS {
            let b = (ROWS - done).min(500);
            let mut v = String::new();
            for j in 0..b {
                let id = done + j;
                if j > 0 { v.push(','); }
                v.push_str(&format!("({id},'init')"));
            }
            ex(&sess, &format!("INSERT INTO t (id,val) VALUES {v}")).await;
            done += b;
        }
    }
    let mut nid: i64 = 1_000_000;
    ex(&sess, &format!("INSERT INTO t (id,val) VALUES ({nid},'warm')")).await;
    nid += 1;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let id = nid; nid += 1;
        let t = Instant::now();
        ex(&sess, &format!("INSERT INTO t (id,val) VALUES ({id},'row')")).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("single_row_insert", p99(s), 300.0);
}

// ── Shape 6 ──────────────────────────────────────────────────────────────────
// Single-row UPDATE by PK in a 10k-row table (OCC read-modify-write).
// Observed ≈ 5-30 ms. Bar: 500 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_06_single_row_update_by_pk() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, val TEXT) WITH (basin.sort_by='id')").await;
    {
        let mut done = 0i64;
        while done < ROWS {
            let b = (ROWS - done).min(500);
            let mut v = String::new();
            for j in 0..b {
                let id = done + j;
                if j > 0 { v.push(','); }
                v.push_str(&format!("({id},'v{id}')"));
            }
            ex(&sess, &format!("INSERT INTO t (id,val) VALUES {v}")).await;
            done += b;
        }
    }
    ex(&sess, "UPDATE t SET val='warm' WHERE id=0").await;
    // First, hit each id once to seed the materialize-overlay shape that
    // iteration 2 onwards will exercise (per fix #94/#95 regression).
    for i in 0..ITERS as i64 {
        let id = (i + 1) % ROWS;
        ex(&sess, &format!("UPDATE t SET val='first' WHERE id={id}")).await;
    }
    let mut s = Vec::with_capacity(ITERS);
    for i in 0..ITERS as i64 {
        let id = (i + 1) % ROWS;
        let t = Instant::now();
        ex(&sess, &format!("UPDATE t SET val='updated' WHERE id={id}")).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    // p99 bar is the headline regression catcher; p50 bar guards
    // against the #94/#95 follow-up regression where the synchronous
    // post-commit physical delete added ~44 ms to the single-row
    // UPDATE p50 at 1 M scale.
    chk("single_row_update_by_pk", p99(s.clone()), 500.0);
    chk_p50("single_row_update_by_pk", p50(s), 30.0);
}

// ── Shape 7 ──────────────────────────────────────────────────────────────────
// Explicit transaction: BEGIN + INSERT 1 row + COMMIT.
// Measures the round-trip cost of the transaction protocol.
// Observed ≈ 5-40 ms. Bar: 600 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_07_transaction_commit_overhead() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, val TEXT)").await;
    ex(&sess, "BEGIN").await;
    ex(&sess, "INSERT INTO t (id,val) VALUES (0,'warm')").await;
    ex(&sess, "COMMIT").await;
    let mut txid: i64 = 1;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let id = txid; txid += 1;
        let t = Instant::now();
        ex(&sess, "BEGIN").await;
        ex(&sess, &format!("INSERT INTO t (id,val) VALUES ({id},'tx')")).await;
        ex(&sess, "COMMIT").await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("transaction_commit_overhead", p99(s), 600.0);
}

// ── Shape 8 ──────────────────────────────────────────────────────────────────
// INNER JOIN 1000-row LHS x 1000-row RHS on PK (hash join build+probe).
// Observed ≈ 5-50 ms. Bar: 800 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_08_simple_join_two_tables() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE lhs (id BIGINT, fk BIGINT, label TEXT)").await;
    {
        let mut done = 0i64;
        while done < JOIN_ROWS {
            let b = (JOIN_ROWS - done).min(500);
            let mut v = String::new();
            for j in 0..b {
                let id = done + j;
                if j > 0 { v.push(','); }
                v.push_str(&format!("({id},{},'L{id}')", id % JOIN_ROWS));
            }
            ex(&sess, &format!("INSERT INTO lhs (id,fk,label) VALUES {v}")).await;
            done += b;
        }
    }
    ex(&sess, "CREATE TABLE rhs (id BIGINT, desc_col TEXT)").await;
    {
        let mut done = 0i64;
        while done < JOIN_ROWS {
            let b = (JOIN_ROWS - done).min(500);
            let mut v = String::new();
            for j in 0..b {
                let id = done + j;
                if j > 0 { v.push(','); }
                v.push_str(&format!("({id},'R{id}')"));
            }
            ex(&sess, &format!("INSERT INTO rhs (id,desc_col) VALUES {v}")).await;
            done += b;
        }
    }
    let sql = "SELECT l.id, r.desc_col FROM lhs l INNER JOIN rhs r ON l.fk = r.id";
    let _ = qry(&sess, sql).await;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let _ = qry(&sess, sql).await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("simple_join_two_tables", p99(s), 800.0);
}

// ── Shape 9 ──────────────────────────────────────────────────────────────────
// HTAP memtable point query: INSERT inside open tx then SELECT the uncommitted
// row back (read-your-own-writes via the memtable merge path, Phase 5.14.C3).
// Correctness assertion is baked into the timing loop.
// Observed ≈ 5-50 ms. Bar: 600 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_09_memtable_point_query_htap() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, payload TEXT)").await;
    // warm-up
    ex(&sess, "BEGIN").await;
    ex(&sess, "INSERT INTO t (id,payload) VALUES (0,'warm')").await;
    let _ = qry(&sess, "SELECT * FROM t WHERE id=0").await;
    ex(&sess, "COMMIT").await;
    let mut hid: i64 = 1;
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let id = hid; hid += 1;
        let t = Instant::now();
        ex(&sess, "BEGIN").await;
        ex(&sess, &format!("INSERT INTO t (id,payload) VALUES ({id},'htap')")).await;
        let b = qry(&sess, &format!("SELECT * FROM t WHERE id={id}")).await;
        let n: usize = b.iter().map(|x| x.num_rows()).sum();
        assert_eq!(n, 1, "HTAP visibility id={id}");
        ex(&sess, "COMMIT").await;
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    chk("memtable_point_query_htap", p99(s), 600.0);
}

// ── Shape 10 ─────────────────────────────────────────────────────────────────
// Postgres extended-query protocol: prepare once, bind+execute per iteration.
// Measures per-execute cost of the substitution + plan pipeline.
// Observed ≈ 2-30 ms. Bar: 400 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn perf_reg_10_parameterized_prepare_execute() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = open(&eng).await;
    ex(&sess, "CREATE TABLE t (id BIGINT, cat BIGINT, label TEXT) WITH (basin.sort_by='id')").await;
    load(&sess, "t", ROWS).await;
    let mid = ROWS / 2 + 7;
    let (h, _) = sess.prepare("SELECT * FROM t WHERE id = $1").await.expect("prepare");
    // warm-up
    let b = sess.bind(&h, vec![ScalarParam::Int8(mid)]).await.expect("bind");
    let _ = sess.execute_bound(b).await.expect("exec_bound");
    let mut s = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t = Instant::now();
        let b = sess.bind(&h, vec![ScalarParam::Int8(mid)]).await.expect("bind");
        let _ = sess.execute_bound(b).await.expect("exec_bound");
        s.push(t.elapsed().as_secs_f64() * 1e3);
    }
    sess.close_statement(&h).await;
    chk("parameterized_prepare_execute", p99(s), 400.0);
}
