//! Differential correctness for the row-granular JSONB GIN tier.
//!
//! The row tier (`index_probe::GinIndexRegistry::probe_row_selection` +
//! `ReadOptions::row_selection` in the storage reader) narrows a `@>` scan from
//! "decode every row of every candidate file" to "decode only the rows whose
//! GIN postings could match", WITHIN each candidate file. It is the fix for the
//! 1M `@>` dense-needle loss: a needle present in every file but matching few
//! rows per file prunes nothing at file / row-group granularity, so PG's
//! row-granular posting lists win. The row tier closes that gap.
//!
//! ## The invariant pinned here
//!
//! The row tier is a PURE ACCELERATOR over a conservative superset: the row
//! selection is always a superset of the true matches (raw-bytes containment),
//! and the `jsonb_contains` UDF re-checks every emitted row. So for EVERY query
//! shape the result with the row tier ENABLED must equal the result with it
//! DISABLED (`BASIN_GIN_ROW_TIER=0`), which in turn must equal a full table
//! scan with no index. This binary asserts that three-way equality across:
//!
//!   * a SELECTIVE needle (few rows per file match — the row tier narrows),
//!   * a DENSE needle whose term appears in (nearly) every row (density cap →
//!     fall back to full file decode; result still correct),
//!   * an ABSENT needle (no row matches — fully pruned),
//!   * a MULTI-TERM AND needle (per-file offset-list intersection),
//!   * the bench shape itself at a smaller scale (correctness only, no timing).
//!
//! It also covers the OVERLAY decline: a live hot-tier UPDATE overlay disables
//! all GIN pruning (row tier included), so reads under an overlay still equal
//! the full-scan oracle.
//!
//! No timing is asserted here (the box runs benchmarks separately); this is a
//! pure correctness oracle. The env var is serialised across the binary's test
//! threads.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises `BASIN_GIN_ROW_TIER` mutation across this binary's parallel
// threads (same pattern as gin_overlay_update.rs / update_hottier.rs).
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig { storage, catalog, shard: None })
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

/// Run `fut` with `BASIN_GIN_ROW_TIER` pinned to `value`, restoring the prior
/// value after. Serialised by `ENV_LOCK`.
async fn with_row_tier<F, R>(value: &str, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_GIN_ROW_TIER").ok();
    std::env::set_var("BASIN_GIN_ROW_TIER", value);
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_GIN_ROW_TIER", v),
        None => std::env::remove_var("BASIN_GIN_ROW_TIER"),
    }
    out
}

/// Seed a multi-file `docs(id bigint, payload jsonb)` table where:
///   * `tag` appears on EVERY row (a dense needle term),
///   * `kv:tag="rare"` appears on a sparse subset (a selective term),
///   * each INSERT lands in its own cold file (multi-file candidate set).
/// Returns `(engine, session, project)`.
async fn seed(dir: &TempDir, with_index: bool) -> (Engine, ProjectSession, ProjectId) {
    let eng = engine_in(dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    exec(&sess, "CREATE TABLE docs (id bigint, payload jsonb)").await;
    // Three files, 6 rows each. `tag` on every row; `rare` on a few rows
    // scattered across files; `mid` on a medium subset.
    for file in 0..3i64 {
        let base = file * 6 + 1;
        let mut vals: Vec<String> = Vec::new();
        for r in 0..6i64 {
            let id = base + r;
            // rare tag on ids 3, 11, 17 (one per file, scattered offsets)
            let is_rare = matches!(id, 3 | 11 | 17);
            let is_mid = id % 2 == 0;
            let mut obj = format!("{{\"tag\":\"t\",\"id\":{id}");
            if is_rare {
                obj.push_str(",\"kind\":\"rare\"");
            }
            if is_mid {
                obj.push_str(",\"band\":\"mid\"");
            }
            obj.push('}');
            vals.push(format!("({id}, '{obj}')"));
        }
        exec(
            &sess,
            &format!("INSERT INTO docs (id, payload) VALUES {}", vals.join(", ")),
        )
        .await;
    }
    if with_index {
        exec(&sess, "CREATE INDEX docs_gin ON docs USING gin (payload)").await;
    }
    (eng, sess, project)
}

/// The full-scan oracle: same data, NO index, row tier irrelevant.
async fn oracle_ids(needle: &str) -> Vec<i64> {
    let dir = TempDir::new().unwrap();
    let (_eng, sess, _p) = seed(&dir, false).await;
    ids_for(&sess, &format!("SELECT id FROM docs WHERE payload @> '{needle}'")).await
}

/// Run the same `@>` query against the indexed table with the row tier ON, and
/// assert it equals the no-index full-scan oracle.
async fn assert_row_tier_matches_oracle(needle: &str) {
    let oracle = oracle_ids(needle).await;
    let on = with_row_tier("1", async {
        let dir = TempDir::new().unwrap();
        let (_eng, sess, _p) = seed(&dir, true).await;
        ids_for(&sess, &format!("SELECT id FROM docs WHERE payload @> '{needle}'")).await
    })
    .await;
    let off = with_row_tier("0", async {
        let dir = TempDir::new().unwrap();
        let (_eng, sess, _p) = seed(&dir, true).await;
        ids_for(&sess, &format!("SELECT id FROM docs WHERE payload @> '{needle}'")).await
    })
    .await;
    assert_eq!(
        on, oracle,
        "row-tier ON must equal full-scan oracle for needle {needle}"
    );
    assert_eq!(
        off, oracle,
        "row-tier OFF must equal full-scan oracle for needle {needle}"
    );
    assert_eq!(on, off, "row-tier ON must equal row-tier OFF for needle {needle}");
}

#[tokio::test]
async fn selective_needle_matches_oracle() {
    // `kind:rare` is selective — few rows per file → the row tier narrows.
    assert_row_tier_matches_oracle("{\"kind\":\"rare\"}").await;
}

#[tokio::test]
async fn dense_needle_matches_oracle() {
    // `tag:"t"` is on every row — density cap drops the row posting, file
    // decodes in full. Result must still be the whole table.
    assert_row_tier_matches_oracle("{\"tag\":\"t\"}").await;
}

#[tokio::test]
async fn absent_needle_matches_oracle() {
    // No row carries `nope` — the row tier proves every file prunable.
    assert_row_tier_matches_oracle("{\"nope\":\"x\"}").await;
}

#[tokio::test]
async fn multi_term_and_needle_matches_oracle() {
    // tag (dense, skipped) AND kind:rare (selective) AND band:mid — the per-file
    // offset intersection. id 6/12/18 are mid; rare ids are 3,11,17; the AND of
    // {tag} ∩ {rare} ∩ {mid} is empty (rare ids are all odd), so the row tier
    // must prune to nothing and equal the oracle's empty set.
    assert_row_tier_matches_oracle("{\"tag\":\"t\",\"kind\":\"rare\",\"band\":\"mid\"}").await;
    // tag AND kind:rare alone → the rare ids 3,11,17.
    assert_row_tier_matches_oracle("{\"tag\":\"t\",\"kind\":\"rare\"}").await;
}

#[tokio::test]
async fn bench_shape_correctness_at_small_scale() {
    // The bench shape: a needle on the dense `tag` plus a value selector. Walk a
    // handful of distinct value needles and assert the indexed answer equals the
    // full-scan oracle for each (correctness only — no timing on this box).
    for id in [3i64, 6, 11, 17, 18] {
        let needle = format!("{{\"tag\":\"t\",\"id\":{id}}}");
        assert_row_tier_matches_oracle(&needle).await;
    }
}

#[tokio::test]
async fn overlay_present_row_tier_declines_but_stays_correct() {
    // A live hot-tier UPDATE overlay disables GIN pruning entirely (the
    // `table_has_live_overlay` gate at the top of apply_gin_pruning_for_query),
    // so the row tier must NOT fire — and the read must still be correct.
    let oracle_after = {
        // Oracle: no index, same mutation, full scan.
        let dir = TempDir::new().unwrap();
        let (_eng, sess, _p) = seed(&dir, false).await;
        exec(
            &sess,
            "UPDATE docs SET payload = '{\"tag\":\"t\",\"id\":3,\"kind\":\"rare\",\"flipped\":true}' WHERE id = 3",
        )
        .await;
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"flipped\":true}'").await
    };
    let indexed_after = with_row_tier("1", async {
        let dir = TempDir::new().unwrap();
        let (_eng, sess, _p) = seed(&dir, true).await;
        exec(
            &sess,
            "UPDATE docs SET payload = '{\"tag\":\"t\",\"id\":3,\"kind\":\"rare\",\"flipped\":true}' WHERE id = 3",
        )
        .await;
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"flipped\":true}'").await
    })
    .await;
    assert_eq!(
        indexed_after, oracle_after,
        "row tier under a live overlay must equal the full-scan oracle"
    );
    assert_eq!(indexed_after, vec![3], "only the flipped row matches");
}
