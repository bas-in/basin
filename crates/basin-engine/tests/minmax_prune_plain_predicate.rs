//! Regression guard for the broadened catalog file-prune
//! (`apply_minmax_file_pruning_for_query`, session.rs, #17).
//!
//! The Wave-2 prune work was unit-tested at the per-file `file_survives_*`
//! level only, so it never drove a survivor / empty re-registration through
//! REAL DataFusion planning. That gap hid a `type_coercion` planning failure:
//! the pruned re-registration (`register_pruned_listing_table` /
//! `register_empty_table`) declared the BARE catalog schema, dropping the ADR
//! 0027 promoted-JSONB shadow columns the physical files actually carry. A plan
//! resolved against the original (extended) registration then hit a provider
//! whose schema was missing those fields, so DataFusion's `type_coercion`
//! analyzer pass failed with `internal: create plan: type_coercion`.
//!
//! These tests use PLAIN predicates (NO `id+0` / `name||''` projection trick on
//! the predicate) so they exercise the exact planning path the prune touches,
//! and assert exact results. The prune is ON by default; it must NEVER error or
//! change a result, so we deliberately do NOT set BASIN_DISABLE_MINMAX_PRUNE.
//!
//! Two engine shapes are used:
//!   * a single-shard Parquet engine (text columns carry min/max stats, so the
//!     string-range / out-of-domain prune actually FIRES and re-registers), and
//!   * the same engine with a PROMOTED JSONB path, whose shadow column the
//!     re-registration used to drop — the exact `type_coercion` repro.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Shard-backed engine over tempdir storage + WAL (mirrors `gin_scale.rs`).
/// A shard lets us `flush_to_parquet()` deterministically so each INSERT lands
/// in its OWN cold file — giving the per-file prune multiple files to narrow.
async fn shard_engine() -> (Engine, Arc<dyn basin_catalog::Catalog>, TempDir, TempDir, basin_shard::Shard)
{
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
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
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal,
    ));
    let eng = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });
    (eng, catalog, storage_dir, wal_dir, shard)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Run a SELECT and flatten `(id, name)`, panicking (with the planner error) if
/// the query errors — a panic here means the regression reproduced.
async fn id_name_rows(sess: &ProjectSession, sql: &str) -> Vec<(i64, String)> {
    let batches: Vec<RecordBatch> = match sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query {sql:?} errored (regression?): {e:?}"))
    {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().expect("id Int64");
        let name = b.column(1).as_any().downcast_ref::<StringArray>().expect("name Utf8");
        for r in 0..b.num_rows() {
            out.push((id.value(r), name.value(r).to_string()));
        }
    }
    out.sort();
    out
}

/// `valb (id BIGINT, name TEXT)` PARQUET (text min/max stats), 4 rows, one row
/// per cold file (each INSERT flushed) so the prune has 4 files to narrow.
async fn seed_valb(sess: &ProjectSession, shard: &basin_shard::Shard) {
    exec(sess, "CREATE TABLE valb (id BIGINT, name TEXT) WITH (basin.file_format='parquet')").await;
    for (id, name) in [(1, "amy"), (2, "bob"), (3, "cat"), (4, "dan")] {
        exec(sess, &format!("INSERT INTO valb (id, name) VALUES ({id}, '{name}')")).await;
        shard.flush_to_parquet().await.unwrap();
    }
}

#[tokio::test]
async fn int_eq_in_domain() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    assert_eq!(
        id_name_rows(&sess, "SELECT id, name FROM valb WHERE id = 3").await,
        vec![(3, "cat".to_string())]
    );
}

#[tokio::test]
async fn int_eq_out_of_domain_is_empty_not_error() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    let got = id_name_rows(&sess, "SELECT id, name FROM valb WHERE id = 99").await;
    assert!(got.is_empty(), "out-of-domain int eq must be empty, got {got:?}");
}

#[tokio::test]
async fn int_in_list() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    assert_eq!(
        id_name_rows(&sess, "SELECT id, name FROM valb WHERE id IN (1, 4)").await,
        vec![(1, "amy".to_string()), (4, "dan".to_string())]
    );
}

#[tokio::test]
async fn string_eq() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    assert_eq!(
        id_name_rows(&sess, "SELECT id, name FROM valb WHERE name = 'bob'").await,
        vec![(2, "bob".to_string())]
    );
}

/// String range — the shape the Int64-only fast-path predecessor/successor
/// trick does NOT match, so it falls through to DataFusion and the prune FIRES
/// (survivors = a strict subset → `register_pruned_listing_table`).
#[tokio::test]
async fn string_range_gte_fires_prune() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    assert_eq!(
        id_name_rows(&sess, "SELECT id, name FROM valb WHERE name >= 'c'").await,
        vec![(3, "cat".to_string()), (4, "dan".to_string())],
    );
}

/// String range with EVERY file pruned → `register_empty_table`. Must be an
/// empty result, NOT an error.
#[tokio::test]
async fn string_range_all_pruned_is_empty_not_error() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    let got = id_name_rows(&sess, "SELECT id, name FROM valb WHERE name >= 'zzz'").await;
    assert!(got.is_empty(), "all-pruned string range must be empty, got {got:?}");
}

/// Control: no WHERE → the prune is a no-op (bails on a missing predicate). The
/// full table must come back unchanged.
#[tokio::test]
async fn no_predicate_control_returns_all() {
    let (eng, _c, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_valb(&sess, &shard).await;
    assert_eq!(
        id_name_rows(&sess, "SELECT id, name FROM valb").await,
        vec![
            (1, "amy".to_string()),
            (2, "bob".to_string()),
            (3, "cat".to_string()),
            (4, "dan".to_string()),
        ]
    );
}

// ── promoted-JSONB shadow-column repro ─────────────────────────────────────
//
// THE failing case: a table with a promoted JSONB path. The physical Parquet
// files carry a `__promoted$payload$category` shadow column. The original
// (un-pruned) registration declares the EXTENDED schema including that shadow
// column; the pruned re-registration used to declare the BARE catalog schema
// (no shadow column). A query that projects `payload->>'category'` (rewritten
// to the shadow column) plans against the extended schema, then the prune
// swaps in a provider missing that column → `type_coercion` failure. With the
// fix the pruned provider declares the SAME extended schema, so it plans.

/// `epj (id BIGINT, name TEXT, payload JSONB)` with `payload.category`
/// promoted, one row per cold Parquet file.
async fn seed_promoted(
    sess: &ProjectSession,
    catalog: &Arc<dyn basin_catalog::Catalog>,
    shard: &basin_shard::Shard,
) {
    let project = sess.project();
    let table = TableName::new("epj").unwrap();
    exec(
        sess,
        "CREATE TABLE epj (id BIGINT, name TEXT, payload JSONB) WITH (basin.file_format='parquet')",
    )
    .await;
    catalog
        .promote_jsonb_path(&project, &table, "payload", "category")
        .await
        .unwrap();
    let rows = [
        (1, "amy", r#"{"category":"books"}"#),
        (2, "bob", r#"{"category":"movies"}"#),
        (3, "cat", r#"{"category":"music"}"#),
        (4, "dan", r#"{"category":"games"}"#),
    ];
    for (id, name, payload) in rows {
        exec(
            sess,
            &format!("INSERT INTO epj (id, name, payload) VALUES ({id}, '{name}', '{payload}')"),
        )
        .await;
        shard.flush_to_parquet().await.unwrap();
    }
}

/// Reads `(id, category)` from a SELECT that projects the promoted shadow path.
async fn id_cat_rows(sess: &ProjectSession, sql: &str) -> Vec<(i64, String)> {
    let batches: Vec<RecordBatch> = match sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query {sql:?} errored (regression?): {e:?}"))
    {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().expect("id Int64");
        let cat = b.column(1).as_any().downcast_ref::<StringArray>().expect("cat Utf8");
        for r in 0..b.num_rows() {
            out.push((id.value(r), cat.value(r).to_string()));
        }
    }
    out.sort();
    out
}

/// String-range prune that FIRES (survivor subset) on a promoted-JSONB table
/// while the projection references the promoted shadow column. This is the
/// exact `type_coercion` repro: pre-fix the survivor ListingTable dropped the
/// shadow column the projection needs.
#[tokio::test]
async fn promoted_jsonb_string_range_prune_plans_and_is_exact() {
    let (eng, catalog, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_promoted(&sess, &catalog, &shard).await;

    let got = id_cat_rows(
        &sess,
        "SELECT id, payload->>'category' AS category FROM epj WHERE name >= 'c'",
    )
    .await;
    assert_eq!(
        got,
        vec![(3, "music".to_string()), (4, "games".to_string())],
        "string-range prune over a promoted-JSONB table must return exact shadow values"
    );
}

/// All-pruned (empty MemTable) on a promoted-JSONB table while projecting the
/// shadow column: empty result, NOT a `type_coercion` error.
#[tokio::test]
async fn promoted_jsonb_all_pruned_is_empty_not_error() {
    let (eng, catalog, _sd, _wd, shard) = shard_engine().await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_promoted(&sess, &catalog, &shard).await;

    let got = id_cat_rows(
        &sess,
        "SELECT id, payload->>'category' AS category FROM epj WHERE name >= 'zzz'",
    )
    .await;
    assert!(got.is_empty(), "all-pruned promoted-JSONB query must be empty, got {got:?}");
}
