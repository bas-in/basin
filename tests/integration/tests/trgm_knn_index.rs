//! Differential correctness for trigram-index-assisted kNN.
//!
//! `ORDER BY name <-> 'needle' LIMIT k` on a `gin_trgm_ops`-indexed TEXT column
//! routes through the trigram posting list: the postings generate the candidate
//! row set (rows sharing >= 1 needle trigram, which bounds distance < 1) and the
//! `<->` distance is recomputed EXACTLY on every candidate. The result is exact
//! top-k, identical to a full sequential scan + sort.
//!
//! ## The invariant pinned here
//!
//! For EVERY shape, the index-backed kNN must return the SAME rows in the SAME
//! distance order as an un-indexed twin (full scan + sort). Because PostgreSQL
//! breaks ties at distance 1 arbitrarily — and the index path's tie order may
//! differ from DataFusion's — the oracle compares the canonical `(distance, id)`
//! ordering (which normalises the arbitrary tie-break) AND separately asserts
//! the raw returned order is distance-non-decreasing (the engine really ranked).
//!
//! Coverage:
//!   * many candidates (k < #near rows)        — `many_candidates_*`
//!   * fewer than k candidates (boundary fill) — `fewer_candidates_than_k_*`
//!   * zero candidates (all distance 1)        — `zero_candidates_*`
//!   * ties at the distance-1 boundary         — folded into the fill tests
//!   * k = 1 / 10 / larger-than-table          — `k_one`, `k_ten`, `k_larger_*`
//!   * overlay present → declines, still exact — `overlay_present_*`
//!   * post-flush + compaction                 — `post_flush_and_compaction_*`
//!   * engagement counter asserted             — `index_path_engages` + others

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn build_engine_with_shard() -> (TempDir, TempDir, Engine, Shard, Arc<dyn Wal>) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
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
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (storage_dir, wal_dir, engine, shard, wal)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Ordered `(id, name)` rows returned by `sql` (cols: id Int64, name Utf8).
async fn rows_for(sess: &ProjectSession, sql: &str) -> Vec<(i64, String)> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let mut out: Vec<(i64, String)> = Vec::new();
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 id from {sql:?}"));
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap_or_else(|| panic!("expected Utf8 name from {sql:?}"));
        for r in 0..b.num_rows() {
            let nm = if names.is_null(r) {
                String::new()
            } else {
                names.value(r).to_string()
            };
            out.push((ids.value(r), nm));
        }
    }
    out
}

/// Deterministic synthetic name. ~1/13 rows carry the rare token "zephyrine";
/// many carry "smith"/"smyth". Mixed alphabet for a realistic trigram space.
fn name_for(i: i64) -> String {
    const FIRST: &[&str] = &[
        "alice", "alyce", "bob", "carol", "dave", "erin", "frank", "grace",
    ];
    const LAST: &[&str] = &["smith", "smyth", "jones", "brown", "taylor"];
    let first = FIRST[(i as usize) % FIRST.len()];
    let last = LAST[((i as usize) / FIRST.len()) % LAST.len()];
    if i % 13 == 0 {
        format!("zephyrine {first} {last}")
    } else {
        format!("{first} {last}")
    }
}

const ROWS: i64 = 260;

/// Seed `people(id bigint, name text)` over several cold-file INSERT batches.
/// Optionally build the trigram GIN index.
async fn seed(dir: &TempDir, with_index: bool) -> (Engine, ProjectSession, ProjectId) {
    let eng = engine_in(dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    exec(&sess, "CREATE TABLE people (id bigint, name text)").await;
    let batch = 40i64;
    let mut off = 0i64;
    while off < ROWS {
        let hi = (off + batch).min(ROWS);
        let mut vals: Vec<String> = Vec::new();
        for i in off..hi {
            let nm = name_for(i).replace('\'', "''");
            vals.push(format!("({i}, '{nm}')"));
        }
        exec(
            &sess,
            &format!("INSERT INTO people (id, name) VALUES {}", vals.join(", ")),
        )
        .await;
        off = hi;
    }
    if with_index {
        exec(
            &sess,
            "CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)",
        )
        .await;
    }
    (eng, sess, project)
}

/// Exact trigram distance of `name` to `needle` (`1 - similarity`).
fn dist(name: &str, needle: &str) -> f32 {
    1.0 - basin_trgm::similarity(name, needle)
}

/// `(distance, id)` pairs for the returned rows, sorted ascending.
fn dist_id(rows: &[(i64, String)], needle: &str) -> Vec<(f32, i64)> {
    let mut v: Vec<(f32, i64)> = rows
        .iter()
        .map(|(id, nm)| (dist(nm, needle), *id))
        .collect();
    v.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    v
}

/// Assert two top-k answers are EQUIVALENT under PG's arbitrary tie-break.
///
/// Both `got` and `oracle` are correct top-k answers for the same query; they
/// may disagree ONLY on WHICH rows occupy the final equal-distance tier (PG
/// breaks ties at any distance arbitrarily, and the index path's physical order
/// differs from DataFusion's). The invariant that MUST hold exactly:
///   * the multiset of distances is identical (same shape of result), and
///   * every row whose distance is strictly below the k-th (boundary) distance
///     is present in BOTH sets (those rows are unambiguous — no tie can drop
///     them); only the boundary tier may differ in membership.
fn assert_topk_equiv(got: &[(i64, String)], oracle: &[(i64, String)], needle: &str, ctx: &str) {
    let g = dist_id(got, needle);
    let o = dist_id(oracle, needle);
    assert_eq!(g.len(), o.len(), "{ctx}: row count differs");

    // Distance multiset must match exactly.
    let gd: Vec<f32> = g.iter().map(|(d, _)| *d).collect();
    let od: Vec<f32> = o.iter().map(|(d, _)| *d).collect();
    assert_eq!(gd, od, "{ctx}: distance multiset differs (wrong ranking)");

    if g.is_empty() {
        return;
    }
    // Boundary distance = the k-th (last) distance. Rows strictly below it are
    // unambiguous and must match as a set; the boundary tier may differ.
    let boundary = g.last().unwrap().0;
    let below = |v: &[(f32, i64)]| -> Vec<i64> {
        let mut ids: Vec<i64> = v
            .iter()
            .filter(|(d, _)| *d < boundary)
            .map(|(_, id)| *id)
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        below(&g),
        below(&o),
        "{ctx}: the unambiguous (below-boundary) rows must be identical"
    );
}

/// Assert the index-backed kNN equals the full-scan oracle for
/// `ORDER BY name <-> '<needle>' LIMIT <k>`, EXACTLY (same rows, same distance
/// order modulo the arbitrary distance-tie break), and that the engagement
/// counter advanced on the indexed run.
async fn assert_knn_exact(needle: &str, k: usize) {
    let q = format!("SELECT id, name FROM people ORDER BY name <-> '{needle}' LIMIT {k}");

    // Oracle: NO index → full sequential scan + sort.
    let odir = TempDir::new().unwrap();
    let (_oe, osess, _op) = seed(&odir, false).await;
    let oracle = rows_for(&osess, &q).await;

    // Indexed: trigram-GIN candidate fast path.
    let idir = TempDir::new().unwrap();
    let (ieng, isess, _ip) = seed(&idir, true).await;
    let before = ieng.trgm_knn_routing_count();
    let indexed = rows_for(&isess, &q).await;
    let after = ieng.trgm_knn_routing_count();

    let want = (ROWS as usize).min(k);
    assert_eq!(oracle.len(), want, "oracle row count for `{q}`");
    assert_eq!(indexed.len(), want, "indexed row count for `{q}`");

    // Exact top-k under PG's arbitrary tie-break at the boundary distance.
    assert_topk_equiv(&indexed, &oracle, needle, &format!("index kNN `{q}`"));

    // The indexed run's RAW order must be distance-non-decreasing (the engine
    // genuinely ranked by distance, not just returned the right set).
    let mut prev = f32::MIN;
    for (_, nm) in &indexed {
        let d = dist(nm, needle);
        assert!(
            d + 1e-6 >= prev,
            "indexed order not non-decreasing for `{q}`: {d} < {prev}"
        );
        prev = d;
    }

    // The fast path engaged.
    assert_eq!(
        after,
        before + 1,
        "trgm-kNN fast path must engage for `{q}`"
    );
}

#[tokio::test]
async fn many_candidates_k_small() {
    // "smith" shares trigrams with a large fraction of rows → many candidates,
    // k well under the candidate count. Pure index path, no fill.
    assert_knn_exact("smith", 10).await;
}

#[tokio::test]
async fn many_candidates_k_one() {
    assert_knn_exact("smith", 1).await;
}

#[tokio::test]
async fn many_candidates_literal_lhs() {
    // The needle on the left of `<->` must route too (distance is symmetric).
    let q = "SELECT id, name FROM people ORDER BY 'smith' <-> name LIMIT 8";

    let odir = TempDir::new().unwrap();
    let (_oe, osess, _op) = seed(&odir, false).await;
    let oracle = rows_for(&osess, q).await;

    let idir = TempDir::new().unwrap();
    let (ieng, isess, _ip) = seed(&idir, true).await;
    let before = ieng.trgm_knn_routing_count();
    let indexed = rows_for(&isess, q).await;
    assert_eq!(
        ieng.trgm_knn_routing_count(),
        before + 1,
        "literal-LHS must engage"
    );
    assert_topk_equiv(&indexed, &oracle, "smith", "literal-LHS kNN");
}

#[tokio::test]
async fn fewer_candidates_than_k_boundary_fill() {
    // "zephyrine" appears in only ROWS/13 ≈ 20 rows; ask for k=50 > candidate
    // count. The ~20 near rows rank first (distance < 1), then the remaining
    // ~30 slots are filled with arbitrary distance-1 rows. PG returns exactly
    // 50 rows; the boundary fill must match the oracle's (distance,id) multiset.
    assert_knn_exact("zephyrine", 50).await;
}

#[tokio::test]
async fn fewer_candidates_than_k_just_over() {
    // k exactly straddling the near-row count exercises the fill boundary at its
    // tightest (a couple of fill rows beyond all near rows).
    assert_knn_exact("zephyrine", 22).await;
}

#[tokio::test]
async fn zero_candidates_all_distance_one() {
    // A needle whose trigrams appear in NO row → zero candidates. Every row is
    // distance 1; PG still returns k rows (arbitrary). The fill path supplies
    // them and the (distance,id) multiset must equal the oracle.
    assert_knn_exact("qwxzj", 10).await;
}

#[tokio::test]
async fn zero_candidates_k_one() {
    assert_knn_exact("qwxzj", 1).await;
}

#[tokio::test]
async fn k_ten_common() {
    assert_knn_exact("smyth", 10).await;
}

#[tokio::test]
async fn k_larger_than_table() {
    // k > ROWS: every row is returned, ranked. No index narrowing benefit, but
    // the result must still be exact and ordered.
    assert_knn_exact("smith", (ROWS as usize) + 25).await;
}

#[tokio::test]
async fn k_equals_table_size() {
    assert_knn_exact("zephyrine", ROWS as usize).await;
}

/// Seed a `pk_people(id bigint PRIMARY KEY, name text)` table so a by-PK UPDATE
/// lands in the live hot-tier overlay (rather than a cold copy-on-write). Builds
/// the trigram GIN index when requested.
async fn seed_pk(dir: &TempDir, with_index: bool) -> (Engine, ProjectSession) {
    let eng = engine_in(dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE pk_people (id bigint PRIMARY KEY, name text)",
    )
    .await;
    for i in 0..ROWS {
        let nm = name_for(i).replace('\'', "''");
        exec(
            &sess,
            &format!("INSERT INTO pk_people (id, name) VALUES ({i}, '{nm}')"),
        )
        .await;
    }
    if with_index {
        exec(
            &sess,
            "CREATE INDEX pk_people_trgm ON pk_people USING gin (name gin_trgm_ops)",
        )
        .await;
    }
    (eng, sess)
}

#[tokio::test]
async fn overlay_present_declines_but_exact() {
    // A live by-PK UPDATE leaves a hot-tier overlay; the pruned cold reader is
    // overlay-blind, so the kNN must DECLINE to the standard (overlay-aware)
    // scan + sort and still return the correct result reflecting the new value.
    let q = "SELECT id, name FROM pk_people ORDER BY name <-> 'zephyrine' LIMIT 30";

    let idir = TempDir::new().unwrap();
    let (ieng, isess) = seed_pk(&idir, true).await;
    exec(
        &isess,
        "UPDATE pk_people SET name = 'zephyrine alice smith' WHERE id = 1",
    )
    .await;
    let before = ieng.trgm_knn_routing_count();
    let indexed = rows_for(&isess, q).await;
    // Declined: the fast-path counter must NOT advance while an overlay exists.
    assert_eq!(
        ieng.trgm_knn_routing_count(),
        before,
        "trgm-kNN must decline to scan + sort while a live overlay exists"
    );

    let odir = TempDir::new().unwrap();
    let (_oe, osess) = seed_pk(&odir, false).await;
    exec(
        &osess,
        "UPDATE pk_people SET name = 'zephyrine alice smith' WHERE id = 1",
    )
    .await;
    let oracle = rows_for(&osess, q).await;

    assert_topk_equiv(&indexed, &oracle, "zephyrine", "overlay-present kNN");
    assert!(
        indexed.iter().any(|(id, _)| *id == 1),
        "the updated row (id=1, now carrying 'zephyrine') must appear in the top-k"
    );
}

#[tokio::test]
async fn short_needle_declines_but_exact() {
    // A needle with NO alphanumeric run produces zero trigrams (pg_trgm returns
    // `{}`), so the candidate probe declines and the standard scan + sort runs.
    // Every row is distance 1; the result must still be exact (k arbitrary rows).
    let q = "SELECT id, name FROM people ORDER BY name <-> '!!' LIMIT 5";

    let idir = TempDir::new().unwrap();
    let (ieng, isess, _ip) = seed(&idir, true).await;
    let before = ieng.trgm_knn_routing_count();
    let indexed = rows_for(&isess, q).await;
    assert_eq!(
        ieng.trgm_knn_routing_count(),
        before,
        "a needle with no trigram must decline the index path"
    );

    let odir = TempDir::new().unwrap();
    let (_oe, osess, _op) = seed(&odir, false).await;
    let oracle = rows_for(&osess, q).await;
    assert_topk_equiv(&indexed, &oracle, "!!", "no-trigram-needle kNN");
}

#[tokio::test]
async fn index_path_engages() {
    // Sanity: a vanilla indexed kNN advances the engagement counter exactly once
    // (proves the differential tests exercise the index path, not a silent
    // both-sides full scan).
    let idir = TempDir::new().unwrap();
    let (ieng, isess, _ip) = seed(&idir, true).await;
    let before = ieng.trgm_knn_routing_count();
    let _ = rows_for(
        &isess,
        "SELECT id, name FROM people ORDER BY name <-> 'smith' LIMIT 5",
    )
    .await;
    assert_eq!(
        ieng.trgm_knn_routing_count(),
        before + 1,
        "indexed kNN must engage the trgm-kNN fast path exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_flush_and_compaction_matches_oracle() {
    // Shard-written (WAL + tail) then compacted into Parquet; the CREATE INDEX
    // backfill walks the cold live files. Index-backed kNN must equal an
    // un-indexed oracle over the same logical data, and engage the fast path.
    let (_sd, _wd, engine, shard, wal) = build_engine_with_shard().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    exec(&sess, "CREATE TABLE people (id bigint, name text)").await;
    for i in 0..ROWS {
        let nm = name_for(i).replace('\'', "''");
        exec(
            &sess,
            &format!("INSERT INTO people (id, name) VALUES ({i}, '{nm}')"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();
    exec(
        &sess,
        "CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)",
    )
    .await;

    for (needle, k) in [("smith", 10usize), ("zephyrine", 50), ("smyth", 1)] {
        let q = format!("SELECT id, name FROM people ORDER BY name <-> '{needle}' LIMIT {k}");
        let before = engine.trgm_knn_routing_count();
        let got = rows_for(&sess, &q).await;
        assert_eq!(
            engine.trgm_knn_routing_count(),
            before + 1,
            "post-compaction kNN must engage the fast path for `{q}`"
        );

        let odir = TempDir::new().unwrap();
        let (_oe, osess, _op) = seed(&odir, false).await;
        let oracle = rows_for(&osess, &q).await;
        assert_topk_equiv(&got, &oracle, needle, &format!("post-compaction `{q}`"));
    }
    wal.close().await.unwrap();
}
