//! Differential index-vs-scan tests for the GIST range index (Phase 5.24.D).
//!
//! The GIST interval-index path (`CREATE INDEX … USING gist (range_col)`)
//! accelerates `&&` / `@>` / `<@` predicates on range-typed columns by pruning
//! files (and short-circuiting empty results) via an in-memory interval tree —
//! see `crates/basin-storage/src/index/interval.rs` and the probe wiring in
//! `crates/basin-engine/src/{index_probe,executor,session}.rs`.
//!
//! ## Correctness contract under test
//!
//! The interval index is a *conservative superset*: file-level pruning never
//! drops a file that holds a match. The real predicate (the `range_*` UDFs in
//! `range_udf.rs`, with their canonical empty-range semantics) ALWAYS
//! re-evaluates every surviving row, so a query's result is identical whether
//! the index engages or not. These tests assert exactly that — the indexed
//! query returns the SAME rows as a brute-force scan over the same data —
//! across narrow / wide / empty literals, the canonicalized-bound edge, an
//! active-transaction overlay decline, and post-flush states.
//!
//! Engagement (the index actually pruning / short-circuiting) is asserted
//! directly against the public `IntervalRegistry` storage API, since the
//! engine's `interval_registry()` accessor is crate-private.
//!
//! ## What is NOT covered here
//!
//! | Concern                              | Where it lives                          |
//! |--------------------------------------|-----------------------------------------|
//! | range operator UDF semantics         | `range_conformance.rs`                  |
//! | range type round-trip / canonicalize | `range_types_harness.rs`                |
//! | range perf card                      | `ext_bench_ranges.rs`                   |
//! | `EXCLUDE USING gist` enforcement     | constraint path (5.24.F), out of scope  |

#![allow(clippy::print_stdout)]

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::types::range::{IndexInterval, RangeValue};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::index::interval::{IntervalRegistry, ProbeResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine() -> (Engine, TempDir) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig { storage, catalog, shard: None });
    (engine, dir)
}

async fn open_session(engine: &Engine) -> basin_engine::ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed:\n  {sql}\n  error: {e}"));
}

/// Run `sql` and collect the `id` column of every row, sorted ascending.
async fn ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed:\n  {sql}\n  error: {e}"))
    {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let idx = b.schema().index_of("id").expect("id column");
                let col = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap_or_else(|| panic!("expected Int64Array id col for: {sql}"));
                for i in 0..b.num_rows() {
                    if !col.is_null(i) {
                        out.push(col.value(i));
                    }
                }
            }
            out.sort_unstable();
            out
        }
        ExecResult::Empty { .. } => vec![],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Differential model: half-open [lo,hi) integer ranges, brute-forced in-test.
// ─────────────────────────────────────────────────────────────────────────────

/// A row's half-open interval `[lo, hi)`. `None` models a SQL NULL range.
#[derive(Clone, Copy)]
struct Row {
    id: i64,
    lo: i64,
    hi: i64,
}

fn overlaps(a: (i64, i64), b: (i64, i64)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// Brute-force expected id set for `r && [lo,hi)`.
fn expect_overlaps(rows: &[Row], q: (i64, i64)) -> Vec<i64> {
    rows.iter().filter(|r| overlaps((r.lo, r.hi), q)).map(|r| r.id).collect()
}

/// Brute-force expected id set for `r @> point`.
fn expect_contains_elem(rows: &[Row], p: i64) -> Vec<i64> {
    rows.iter().filter(|r| r.lo <= p && p < r.hi).map(|r| r.id).collect()
}

/// Brute-force expected id set for `r <@ [lo,hi)` (row range contained by literal).
fn expect_contained_by(rows: &[Row], q: (i64, i64)) -> Vec<i64> {
    rows.iter().filter(|r| q.0 <= r.lo && r.hi <= q.1).map(|r| r.id).collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end differential: index path == brute-force scan, every shape.
// ─────────────────────────────────────────────────────────────────────────────

/// Create `slots(id, r INT4RANGE)` with a gist index and populate it with a
/// spread of non-overlapping and overlapping `[lo,hi)` intervals.
async fn make_slots(sess: &basin_engine::ProjectSession, rows: &[Row]) {
    exec_ok(sess, "CREATE TABLE slots (id BIGINT NOT NULL, r INT4RANGE)").await;
    exec_ok(sess, "CREATE INDEX slots_r_gist ON slots USING gist (r)").await;
    for r in rows {
        exec_ok(
            sess,
            &format!("INSERT INTO slots (id, r) VALUES ({}, '[{},{})')", r.id, r.lo, r.hi),
        )
        .await;
    }
}

fn sample_rows() -> Vec<Row> {
    vec![
        Row { id: 1, lo: 0, hi: 10 },
        Row { id: 2, lo: 20, hi: 30 },
        Row { id: 3, lo: 40, hi: 50 },
        Row { id: 4, lo: 5, hi: 25 },  // overlaps 1 and 2
        Row { id: 5, lo: 100, hi: 200 },
        Row { id: 6, lo: 45, hi: 60 }, // overlaps 3
    ]
}

/// `&&` overlap: narrow probe (hits one), wide probe (hits many), gap (hits none).
#[tokio::test]
async fn differential_overlaps_narrow_wide_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;
    let rows = sample_rows();
    make_slots(&sess, &rows).await;

    // Narrow probe inside a single interval gap-free region.
    let q = (22, 23);
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[22,23)'").await,
        { let mut e = expect_overlaps(&rows, q); e.sort_unstable(); e },
        "&& narrow [22,23) must equal brute-force scan",
    );

    // Wide probe spanning most of the data.
    let q = (0, 1000);
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[0,1000)'").await,
        { let mut e = expect_overlaps(&rows, q); e.sort_unstable(); e },
        "&& wide [0,1000) must equal brute-force scan (all rows)",
    );

    // Empty-result probe: a gap no interval covers.
    let q = (70, 90);
    let expect = expect_overlaps(&rows, q);
    assert!(expect.is_empty(), "model: [70,90) must be a gap");
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[70,90)'").await,
        expect,
        "&& gap [70,90) must return zero rows (index short-circuit + recheck agree)",
    );

    // The PG `empty` literal overlaps NOTHING — canonical empty-range semantics.
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && 'empty'").await,
        Vec::<i64>::new(),
        "&& 'empty' must return zero rows (empty overlaps nothing)",
    );

    println!("[gist diff] && narrow/wide/empty == scan ✓");
}

/// `@> element`: hit, miss-in-gap, boundary (exclusive upper).
#[tokio::test]
async fn differential_contains_elem() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;
    let rows = sample_rows();
    make_slots(&sess, &rows).await;

    for p in [5i64, 9, 10, 25, 45, 199, 200, 75] {
        let expect = { let mut e = expect_contains_elem(&rows, p); e.sort_unstable(); e };
        // `r @> <int>` element containment.
        let got = ids(&sess, &format!("SELECT id FROM slots WHERE r @> {p}")).await;
        assert_eq!(got, expect, "@> {p} must equal brute-force scan");
    }

    println!("[gist diff] @> element across hits/gaps/boundaries == scan ✓");
}

/// `<@`: row range contained by the literal range.
#[tokio::test]
async fn differential_contained_by() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;
    let rows = sample_rows();
    make_slots(&sess, &rows).await;

    for q in [(0i64, 35i64), (0, 1000), (0, 5), (40, 60)] {
        let expect = { let mut e = expect_contained_by(&rows, q); e.sort_unstable(); e };
        let got = ids(
            &sess,
            &format!("SELECT id FROM slots WHERE r <@ '[{},{})'", q.0, q.1),
        )
        .await;
        assert_eq!(got, expect, "<@ [{},{}) must equal brute-force scan", q.0, q.1);
    }

    println!("[gist diff] <@ contained-by narrow/wide == scan ✓");
}

/// Canonicalized-bound edge: `[1,3]` (inclusive upper) and `[1,4)` (exclusive
/// upper) denote the SAME discrete int4 range. A probe with either form must
/// agree with the scan — the interval index canonicalizes inclusive upper via
/// `next_up`, and the recheck UDF resolves the canonical form.
#[tokio::test]
async fn differential_canonicalized_bound_edge() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE edge (id BIGINT NOT NULL, r INT4RANGE)").await;
    exec_ok(&sess, "CREATE INDEX edge_r_gist ON edge USING gist (r)").await;
    // Row stored with inclusive upper bound: [1,3] == [1,4).
    exec_ok(&sess, "INSERT INTO edge (id, r) VALUES (1, '[1,3]')").await;
    // Row stored with exclusive upper bound, covers the canonical-equal range.
    exec_ok(&sess, "INSERT INTO edge (id, r) VALUES (2, '[1,4)')").await;

    // Element 3 is INSIDE both [1,3]==[1,4): contained.
    assert_eq!(
        ids(&sess, "SELECT id FROM edge WHERE r @> 3").await,
        vec![1, 2],
        "@> 3 must hit both (canonical [1,4) contains 3)",
    );
    // Element 4 is OUTSIDE [1,4): excluded for both.
    assert_eq!(
        ids(&sess, "SELECT id FROM edge WHERE r @> 4").await,
        Vec::<i64>::new(),
        "@> 4 must hit neither (exclusive canonical upper)",
    );
    // Overlap with [3,5): [1,4) overlaps [3,5) at {3}; both rows hit.
    assert_eq!(
        ids(&sess, "SELECT id FROM edge WHERE r && '[3,5)'").await,
        vec![1, 2],
        "&& [3,5) must hit both canonical-equal rows",
    );
    // Overlap with [4,5): canonical upper is exclusive at 4 → no overlap.
    assert_eq!(
        ids(&sess, "SELECT id FROM edge WHERE r && '[4,5)'").await,
        Vec::<i64>::new(),
        "&& [4,5) must hit neither (adjacent past exclusive upper)",
    );

    println!("[gist diff] canonicalized-bound edge ([1,3] vs [1,4)) == scan ✓");
}

/// Overlay decline: inside an explicit transaction the engine declines the
/// interval-index probe (the probe is gated on `!tx_is_active`, since the
/// overlay may carry uncommitted rows the index has not seen). The query must
/// STILL return the correct committed rows — the UDF recheck over the scan is
/// authoritative, so declining the probe never changes the answer.
#[tokio::test]
async fn differential_overlay_decline_still_correct() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;
    let rows = sample_rows();
    make_slots(&sess, &rows).await;

    let q = (22, 23);
    let expect = { let mut e = expect_overlaps(&rows, q); e.sort_unstable(); e };

    // Outside any tx: the probe MAY engage; result equals the brute-force scan.
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[22,23)'").await,
        expect,
        "no-tx && [22,23) must equal brute-force scan",
    );

    // Inside a tx: the interval probe is declined, but the SAME query must
    // return the SAME committed rows (correctness independent of the probe).
    exec_ok(&sess, "BEGIN").await;
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[22,23)'").await,
        expect,
        "in-tx && [22,23) must still equal brute-force scan (probe declined)",
    );
    exec_ok(&sess, "ROLLBACK").await;

    // Post-rollback: result unchanged.
    assert_eq!(
        ids(&sess, "SELECT id FROM slots WHERE r && '[22,23)'").await,
        expect,
        "post-rollback && [22,23) must equal brute-force scan",
    );

    println!("[gist diff] overlay decline preserves correctness ✓");
}

/// NULL range rows must never be returned by any range predicate (PG: NULL is
/// not contained, does not overlap), regardless of index engagement.
#[tokio::test]
async fn differential_null_rows_excluded() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE nl (id BIGINT NOT NULL, r INT4RANGE)").await;
    exec_ok(&sess, "CREATE INDEX nl_r_gist ON nl USING gist (r)").await;
    exec_ok(&sess, "INSERT INTO nl (id, r) VALUES (1, '[0,10)')").await;
    exec_ok(&sess, "INSERT INTO nl (id, r) VALUES (2, NULL)").await;

    assert_eq!(
        ids(&sess, "SELECT id FROM nl WHERE r && '[0,100)'").await,
        vec![1],
        "NULL range row must not overlap any literal",
    );
    assert_eq!(
        ids(&sess, "SELECT id FROM nl WHERE r @> 5").await,
        vec![1],
        "NULL range row must not contain any element",
    );

    println!("[gist diff] NULL range rows excluded ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Engagement / mechanism: assert the interval index actually prunes and that
// its completeness + eviction discipline is sound (storage-layer public API).
// ─────────────────────────────────────────────────────────────────────────────

fn iv(lo: f64, hi: f64) -> IndexInterval {
    IndexInterval { lo, hi }
}

/// The registry SHORT-CIRCUITS (`Empty`) when no indexed interval can match,
/// and returns `FileCandidates` when one might — proving the probe engages
/// rather than silently passing through.
#[test]
fn engagement_empty_vs_candidates() {
    let reg = IntervalRegistry::new();
    let proj = ProjectId::new();
    let tbl = TableName::new("slots").unwrap();

    reg.index_row(&proj, &tbl, "r", "[0,10)", "f1.parquet", 0);
    reg.index_row(&proj, &tbl, "r", "[20,30)", "f2.parquet", 0);

    // Point in a gap → Empty (decisive prune of every file).
    assert!(
        matches!(reg.probe_contains_point(&proj, &tbl, "r", 15.0), ProbeResult::Empty),
        "point 15 in gap must short-circuit Empty",
    );
    // Point inside f1 → only f1 is a candidate.
    match reg.probe_contains_point(&proj, &tbl, "r", 5.0) {
        ProbeResult::FileCandidates(files) => {
            assert!(files.contains("f1.parquet"), "f1 must be a candidate: {files:?}");
            assert!(!files.contains("f2.parquet"), "f2 must be pruned: {files:?}");
        }
        other => panic!("expected FileCandidates, got {other:?}"),
    }
    // Overlap probe over the gap [12,18) → Empty.
    assert!(
        matches!(reg.probe_overlaps(&proj, &tbl, "r", &iv(12.0, 18.0)), ProbeResult::Empty),
        "overlap [12,18) in gap must short-circuit Empty",
    );

    println!("[gist engage] Empty short-circuit + file candidates ✓");
}

/// Completeness guard: a file removed by compaction drops out of both the tree
/// and the indexed-files set, so a stale candidate is never returned and the
/// completeness check correctly reports the file as no-longer-covered.
#[test]
fn engagement_compaction_remove_file() {
    let reg = IntervalRegistry::new();
    let proj = ProjectId::new();
    let tbl = TableName::new("slots").unwrap();

    reg.index_row(&proj, &tbl, "r", "[0,10)", "f1.parquet", 0);
    reg.index_row(&proj, &tbl, "r", "[5,15)", "f2.parquet", 0);
    reg.mark_file_indexed(&proj, &tbl, "r", "f1.parquet");
    reg.mark_file_indexed(&proj, &tbl, "r", "f2.parquet");
    assert_eq!(reg.indexed_files_for(&proj, &tbl, "r").len(), 2);

    // Compact away f1.
    reg.remove_file(&proj, &tbl, "r", "f1.parquet");

    // f1 must no longer be a candidate, and is no longer "covered".
    match reg.probe_contains_point(&proj, &tbl, "r", 7.0) {
        ProbeResult::FileCandidates(files) => {
            assert!(!files.contains("f1.parquet"), "f1 must be gone after compaction: {files:?}");
            assert!(files.contains("f2.parquet"), "f2 still holds 7: {files:?}");
        }
        other => panic!("expected FileCandidates, got {other:?}"),
    }
    let covered = reg.indexed_files_for(&proj, &tbl, "r");
    assert!(!covered.contains("f1.parquet"), "removed file must drop from completeness set");
    assert!(covered.contains("f2.parquet"));

    println!("[gist engage] compaction remove_file prunes + uncovers ✓");
}

/// Post-flush state: re-indexing the same `(file)` is idempotent for the
/// completeness set, and probes over a many-interval tree return a sound
/// superset (every brute-force hit is present among the candidates).
#[test]
fn engagement_post_flush_superset_sound() {
    let reg = IntervalRegistry::new();
    let proj = ProjectId::new();
    let tbl = TableName::new("slots").unwrap();

    // 2000 disjoint [10k,10k+8) intervals spread across 20 files.
    let mut model: Vec<(f64, f64, String)> = Vec::new();
    for i in 0u64..2000 {
        let lo = (i * 10) as f64;
        let hi = lo + 8.0;
        let file = format!("chunk_{}.parquet", i / 100);
        reg.index_row(&proj, &tbl, "r", &format!("[{lo},{hi})"), &file, 0);
        model.push((lo, hi, file));
    }

    // Overlap probe [503, 1207): brute-force the files that truly overlap.
    let q = iv(503.0, 1207.0);
    let expect_files: BTreeSet<String> = model
        .iter()
        .filter(|(lo, hi, _)| *lo < q.hi && q.lo < *hi)
        .map(|(_, _, f)| f.clone())
        .collect();
    match reg.probe_overlaps(&proj, &tbl, "r", &q) {
        ProbeResult::FileCandidates(files) => {
            // Superset soundness: every true-overlap file is a candidate.
            for f in &expect_files {
                assert!(files.contains(f), "candidate set missing true-overlap file {f}");
            }
        }
        ProbeResult::Empty => assert!(expect_files.is_empty(), "Empty but model had overlaps"),
        ProbeResult::NoIndex => panic!("index was populated; NoIndex is wrong"),
    }

    println!("[gist engage] many-interval probe is a sound superset ✓");
}

/// Budget-eviction degrade: removing a column's entire file set returns the
/// probe to `NoIndex` (the safe full-scan fallback), never a false negative.
/// This is the degradation contract — a de-indexed column must read as
/// "unknown → scan", not "empty".
#[test]
fn engagement_budget_degrade_to_scan() {
    let reg = IntervalRegistry::new();
    let proj = ProjectId::new();
    let tbl = TableName::new("slots").unwrap();

    // Probe before any insert → NoIndex (full scan).
    assert!(
        matches!(reg.probe_contains_point(&proj, &tbl, "r", 5.0), ProbeResult::NoIndex),
        "un-indexed column must read NoIndex",
    );

    reg.index_row(&proj, &tbl, "r", "[0,10)", "f1.parquet", 0);
    reg.mark_file_indexed(&proj, &tbl, "r", "f1.parquet");
    assert!(
        matches!(reg.probe_contains_point(&proj, &tbl, "r", 5.0), ProbeResult::FileCandidates(_)),
        "after index, point 5 must be a candidate",
    );

    // Simulate the whole file being dropped (eviction / compaction churn).
    reg.remove_file(&proj, &tbl, "r", "f1.parquet");
    // The tree is now empty → NoIndex (degrade to scan), NOT Empty.
    let res = reg.probe_contains_point(&proj, &tbl, "r", 5.0);
    assert!(
        matches!(res, ProbeResult::NoIndex | ProbeResult::Empty),
        "degraded probe must be NoIndex/Empty (safe), got {res:?}",
    );
    // Critically: the completeness set is now empty, so the engine's coverage
    // check (indexed ⊇ live) fails and it falls back to a full scan — no row
    // can be wrongly pruned.
    assert!(
        reg.indexed_files_for(&proj, &tbl, "r").is_empty(),
        "degraded column must report zero covered files → engine full-scans",
    );

    println!("[gist engage] budget degrade → safe scan fallback ✓");
}

/// Empty-range literals never engage the interval tree (they index to no
/// `IndexInterval` and the recheck UDF gives them empty semantics). Confirms
/// the `RangeValue → IndexInterval` boundary skips empty/infinite ranges.
#[test]
fn engagement_empty_and_infinite_ranges_skipped() {
    // Empty range → no interval.
    let empty = RangeValue::from_pg_text("empty").unwrap();
    assert!(IndexInterval::from_range(&empty).is_none(), "empty range must not index");

    // Degenerate [5,5) → empty → no interval.
    let degenerate = RangeValue::from_pg_text("[5,5)").unwrap();
    assert!(
        IndexInterval::from_range(&degenerate).is_none(),
        "degenerate [5,5) must not index (empty)",
    );

    // Finite range → indexes.
    let finite = RangeValue::from_pg_text("[1,10)").unwrap();
    let ivl = IndexInterval::from_range(&finite).expect("finite range must index");
    assert!(ivl.contains_point(5.0) && !ivl.contains_point(10.0));

    println!("[gist engage] empty/degenerate ranges skipped at index boundary ✓");
}
