//! Differential correctness for the trigram GIN index (`gin_trgm_ops`).
//!
//! `CREATE INDEX … USING gin (text_col gin_trgm_ops)` makes `name % 'needle'`
//! (lowered by the rewriter to `similarity(name,'needle') >= t`) index-backed:
//! the trigram posting list prunes to the files/rows that share enough needle
//! trigrams, then the `similarity()` predicate re-evaluates every survivor.
//!
//! ## The invariant pinned here
//!
//! The trigram probe is a PURE ACCELERATOR over a conservative superset (the
//! count-based bound `i >= ceil(t * Q)` is provably necessary for a match — see
//! `index_probe::trgm_min_shared`). So for EVERY shape the result on the INDEXED
//! table must equal the result on an un-indexed twin (full sequential scan +
//! `similarity()` recheck). This binary asserts that equality across:
//!
//!   * a SELECTIVE needle (rare; few rows match),
//!   * a COMMON needle (high selectivity),
//!   * THRESHOLD changes via the `pg_trgm.similarity_threshold` GUC,
//!   * CASE FOLDING (mixed-case needle vs lowercase data),
//!   * the UNICODE / ASCII-word boundary (non-ASCII bytes are word separators),
//!   * an OVERLAY present (a live UPDATE overlay → pruning declines, still correct),
//!   * POST-FLUSH + POST-COMPACTION (shard-written then compacted data).
//!
//! BUDGET-PRESSURE eviction lives in its own binary (`trgm_gin_budget.rs`)
//! because `BASIN_GIN_POSTING_BUDGET` is read once per process via `OnceLock`:
//! a tiny budget set there would leak into every test in this binary.
//!
//! It also asserts the index actually ENGAGES (the posting registry holds
//! trigram pairs and seals the live files) so the differential is meaningful
//! and not a silent full-scan fallback on both sides.
//!
//! No timing is asserted (the box benchmarks separately); this is a pure
//! correctness oracle.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
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

/// Sorted `id` list returned by `sql` (first column must be Int64).
async fn ids_for(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let mut ids: Vec<i64> = Vec::new();
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 ids from {sql:?}"));
        for r in 0..col.len() {
            ids.push(col.value(r));
        }
    }
    ids.sort_unstable();
    ids
}

/// Deterministic synthetic name; ~1/13 rows carry the rare needle "zephyrine",
/// many carry "smith"/"smyth". Mixed alphabet so trigram space is realistic.
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

/// Seed `people(id bigint, name text)` over several INSERT batches (one cold
/// file each via the session engine). Optionally build the trigram GIN index.
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

/// The full-scan oracle: same data, NO index, on a fresh engine.
async fn oracle_ids(query_tail: &str, threshold: f32) -> Vec<i64> {
    let dir = TempDir::new().unwrap();
    let (_eng, sess, _p) = seed(&dir, false).await;
    if (threshold - 0.3).abs() > f32::EPSILON {
        exec(
            &sess,
            &format!("SET pg_trgm.similarity_threshold = {threshold}"),
        )
        .await;
    }
    ids_for(&sess, &format!("SELECT id FROM people WHERE {query_tail}")).await
}

/// Index-backed result: same data WITH the trigram GIN index.
async fn indexed_ids(query_tail: &str, threshold: f32) -> Vec<i64> {
    let dir = TempDir::new().unwrap();
    let (_eng, sess, _p) = seed(&dir, true).await;
    if (threshold - 0.3).abs() > f32::EPSILON {
        exec(
            &sess,
            &format!("SET pg_trgm.similarity_threshold = {threshold}"),
        )
        .await;
    }
    ids_for(&sess, &format!("SELECT id FROM people WHERE {query_tail}")).await
}

/// The gate: index-backed `%` returns EXACTLY the same rows as the no-index
/// full scan over the same data, at the given threshold.
async fn assert_index_matches_oracle(query_tail: &str, threshold: f32) {
    let oracle = oracle_ids(query_tail, threshold).await;
    let indexed = indexed_ids(query_tail, threshold).await;
    assert_eq!(
        indexed, oracle,
        "index-backed `{query_tail}` (t={threshold}) must equal full-scan oracle"
    );
}

#[tokio::test]
async fn selective_needle_matches_oracle() {
    // Rare needle "zephyrine" — few rows match; the index prunes hard.
    assert_index_matches_oracle("name % 'zephyrine'", 0.3).await;
}

#[tokio::test]
async fn common_needle_matches_oracle() {
    // Common root "smith" — high selectivity.
    assert_index_matches_oracle("name % 'smith'", 0.3).await;
}

#[tokio::test]
async fn literal_lhs_matches_oracle() {
    // `'needle' % name` (literal on the left) must be detected and pruned too.
    assert_index_matches_oracle("'smyth' % name", 0.3).await;
}

#[tokio::test]
async fn threshold_changes_match_oracle() {
    // Sweep the GUC: a higher threshold tightens both the predicate AND the
    // count-based prune bound. The two must move together (no dropped match).
    for t in [0.1_f32, 0.3, 0.5, 0.7, 0.9] {
        assert_index_matches_oracle("name % 'smith'", t).await;
        assert_index_matches_oracle("name % 'zephyrine'", t).await;
    }
}

#[tokio::test]
async fn case_folding_matches_oracle() {
    // Mixed-case needle vs lowercase data: `similarity()` and the index both
    // ASCII case-fold, so the upper-case needle must find the same rows.
    assert_index_matches_oracle("name % 'SMITH'", 0.3).await;
    assert_index_matches_oracle("name % 'Zephyrine'", 0.3).await;
}

#[tokio::test]
async fn unicode_ascii_boundary_matches_oracle() {
    // A needle with non-ASCII bytes: those bytes are word separators (v0.1
    // pg_trgm ASCII rule), so the trigram set is taken over the ASCII runs
    // only. Index and oracle must agree regardless.
    assert_index_matches_oracle("name % 'smïth'", 0.3).await;
    assert_index_matches_oracle("name % 'café smith'", 0.3).await;
}

#[tokio::test]
async fn overlay_present_declines_but_correct() {
    // A live fast-path UPDATE overlay disables GIN pruning (the bare cold reader
    // can't see overlay rows). The read must still equal a full-scan oracle that
    // includes the updated row.
    let dir = TempDir::new().unwrap();
    let (_eng, sess, _p) = seed(&dir, true).await;
    // Flip id=1 to carry "zephyrine" (it did not before: 1 % 13 != 0).
    exec(
        &sess,
        "UPDATE people SET name = 'zephyrine alice smith' WHERE id = 1",
    )
    .await;
    let got = ids_for(&sess, "SELECT id FROM people WHERE name % 'zephyrine'").await;

    // Oracle: rebuild the same data + the same update with NO index.
    let odir = TempDir::new().unwrap();
    let (_oe, osess, _op) = seed(&odir, false).await;
    exec(
        &osess,
        "UPDATE people SET name = 'zephyrine alice smith' WHERE id = 1",
    )
    .await;
    let oracle = ids_for(&osess, "SELECT id FROM people WHERE name % 'zephyrine'").await;

    assert_eq!(
        got, oracle,
        "overlay-present read must equal full-scan oracle"
    );
    assert!(
        got.contains(&1),
        "the updated row must appear in the match set"
    );
}

#[tokio::test]
async fn index_actually_engages() {
    // Sanity: the index must hold trigram pairs and seal the live files, so the
    // differential above exercises the index path (not a silent both-sides
    // full scan). Uses the test accessor on the registry.
    let dir = TempDir::new().unwrap();
    let (eng, _sess, project) = seed(&dir, true).await;
    let reg = eng.gin_index_registry_for_test();
    assert!(
        reg.project_pair_count(&project) > 0,
        "trigram posting list must hold pairs after CREATE INDEX backfill"
    );
    let table = TableName::new("people").unwrap();
    let indexed = reg.indexed_files_for(&project, &table, "name");
    assert!(
        !indexed.is_empty(),
        "CREATE INDEX must seal at least one live file as fully indexed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_flush_and_compaction_matches_oracle() {
    // Shard-written (WAL + tail) then compacted into Parquet: the CREATE INDEX
    // backfill walks the cold live files. Index-backed % must equal an
    // un-indexed oracle over the same logical data.
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
    // Compact into Parquet, THEN build the index (backfill over cold files).
    shard.flush_to_parquet().await.unwrap();
    exec(
        &sess,
        "CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)",
    )
    .await;

    let table = TableName::new("people").unwrap();
    let reg = engine.gin_index_registry_for_test();
    assert!(
        reg.indexed_files_for(&project, &table, "name").len() >= 1,
        "post-compaction backfill must seal the compacted file"
    );

    for tail in [
        "name % 'smith'",
        "name % 'zephyrine'",
        "name % 'alyce smyth'",
    ] {
        let got = ids_for(&sess, &format!("SELECT id FROM people WHERE {tail}")).await;
        let oracle = oracle_ids(tail, 0.3).await;
        assert_eq!(got, oracle, "post-compaction `{tail}` must equal oracle");
    }
    wal.close().await.unwrap();
}
