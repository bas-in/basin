//! `DISTINCT ON` end-to-end correctness + lowering tests.
//!
//! Basin lowers `DISTINCT ON` at optimizer position 0 (see
//! `src/is_distinct_rewrite.rs`) into a grouped `first_value` aggregate that
//! projects the ON columns straight from the group keys and trims the
//! per-aggregate ORDER BY to the non-key tail. These tests pin:
//!
//! * first-row-per-group semantics on multi-group data (PG `DISTINCT ON`);
//! * row coherence on ORDER-BY ties — all returned columns must come from
//!   ONE input row, never a mix of two tied rows;
//! * NULLs in the sort key (PG defaults: DESC ⇒ NULLS FIRST, ASC ⇒ NULLS
//!   LAST);
//! * LIMIT / no-LIMIT and ORDER BY direction variants;
//! * output equivalence against DataFusion's stock `DISTINCT ON` lowering
//!   (`ReplaceDistinctWithAggregate`, exercised through a vanilla
//!   `SessionContext` that does not carry Basin's rule) on a deterministic
//!   dataset;
//! * that the Basin lowering actually fires in a real engine session
//!   (EXPLAIN shows grouped `first_value` with no aggregate over the ON
//!   column).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, TimestampMicrosecondArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// Timestamp column as `Option<i64>` micros (None = SQL NULL).
fn col_ts_opt(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn micros(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .timestamp_micros()
}

/// Shared 3-group TIMESTAMP dataset. Per-group latest `created_at` row:
/// u1 → id 11, u2 → id 20, u3 → id 30.
async fn seed_events(sess: &ProjectSession) {
    sess.execute(
        "CREATE TABLE events (user_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         amount BIGINT NOT NULL, created_at TIMESTAMP)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO events VALUES \
         (1, 10, 100, '2026-01-01T00:00:01Z'), \
         (1, 11, 110, '2026-01-01T00:00:03Z'), \
         (1, 12, 120, '2026-01-01T00:00:02Z'), \
         (2, 20, 200, '2026-01-02T00:00:02Z'), \
         (2, 21, 210, '2026-01-02T00:00:01Z'), \
         (3, 30, 300, '2026-01-03T00:00:01Z')",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn distinct_on_first_row_per_group_matches_expected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, amount, created_at \
         FROM events ORDER BY user_id, created_at DESC",
    )
    .await;

    assert_eq!(col_i64(&batches, "user_id"), vec![1, 2, 3]);
    assert_eq!(col_i64(&batches, "id"), vec![11, 20, 30]);
    assert_eq!(col_i64(&batches, "amount"), vec![110, 200, 300]);
    assert_eq!(
        col_ts_opt(&batches, "created_at"),
        vec![
            Some(micros("2026-01-01T00:00:03Z")),
            Some(micros("2026-01-02T00:00:02Z")),
            Some(micros("2026-01-03T00:00:01Z")),
        ]
    );
}

#[tokio::test]
async fn distinct_on_with_limit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, amount, created_at \
         FROM events ORDER BY user_id, created_at DESC LIMIT 2",
    )
    .await;

    assert_eq!(col_i64(&batches, "user_id"), vec![1, 2]);
    assert_eq!(col_i64(&batches, "id"), vec![11, 20]);
}

/// Ties on the full sort key: PG leaves the winning row unspecified, but the
/// returned columns must all come from ONE row. The dataset maintains the
/// invariant `amount == id * 10`, so a row-incoherent result (columns mixed
/// from two tied rows) breaks the invariant.
#[tokio::test]
async fn distinct_on_ties_are_row_coherent() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE tied (user_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         amount BIGINT NOT NULL, created_at TIMESTAMP)",
    )
    .await
    .unwrap();
    // u1: ids 11 and 12 tie at the max created_at; id 10 is older.
    // u2: ids 21, 22, 23 ALL tie.
    sess.execute(
        "INSERT INTO tied VALUES \
         (1, 10, 100, '2026-01-01T00:00:01Z'), \
         (1, 11, 110, '2026-01-01T00:00:09Z'), \
         (1, 12, 120, '2026-01-01T00:00:09Z'), \
         (2, 21, 210, '2026-01-02T00:00:05Z'), \
         (2, 22, 220, '2026-01-02T00:00:05Z'), \
         (2, 23, 230, '2026-01-02T00:00:05Z')",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, amount, created_at \
         FROM tied ORDER BY user_id, created_at DESC",
    )
    .await;

    let user_ids = col_i64(&batches, "user_id");
    let ids = col_i64(&batches, "id");
    let amounts = col_i64(&batches, "amount");
    let ts = col_ts_opt(&batches, "created_at");
    assert_eq!(user_ids, vec![1, 2]);

    // Group u1: winner must be one of the tied rows (11 or 12), at the max
    // timestamp, with amount from the SAME row.
    assert!(
        ids[0] == 11 || ids[0] == 12,
        "u1 winner must be a tied-max row, got {}",
        ids[0]
    );
    assert_eq!(amounts[0], ids[0] * 10, "u1 columns mixed across tied rows");
    assert_eq!(ts[0], Some(micros("2026-01-01T00:00:09Z")));

    // Group u2: all three rows tie; winner arbitrary but coherent.
    assert!(
        (21..=23).contains(&ids[1]),
        "u2 winner must be a tied row, got {}",
        ids[1]
    );
    assert_eq!(amounts[1], ids[1] * 10, "u2 columns mixed across tied rows");
    assert_eq!(ts[1], Some(micros("2026-01-02T00:00:05Z")));
}

/// NULLs in the sort key. PG defaults: `DESC` ⇒ NULLS FIRST (the NULL row
/// wins), `ASC` ⇒ NULLS LAST (the smallest non-NULL row wins).
#[tokio::test]
async fn distinct_on_nulls_in_sort_key_desc_and_asc() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE ev_null (user_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         amount BIGINT NOT NULL, created_at TIMESTAMP)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO ev_null VALUES \
         (1, 10, 100, '2026-01-01T00:00:01Z'), \
         (1, 11, 110, NULL), \
         (2, 20, 200, '2026-01-02T00:00:01Z'), \
         (2, 21, 210, '2026-01-02T00:00:02Z')",
    )
    .await
    .unwrap();

    // DESC: NULLS FIRST — the NULL-created_at row wins group 1.
    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, created_at \
         FROM ev_null ORDER BY user_id, created_at DESC",
    )
    .await;
    assert_eq!(col_i64(&batches, "user_id"), vec![1, 2]);
    assert_eq!(col_i64(&batches, "id"), vec![11, 21]);
    assert_eq!(
        col_ts_opt(&batches, "created_at"),
        vec![None, Some(micros("2026-01-02T00:00:02Z"))]
    );

    // ASC: NULLS LAST — the earliest non-NULL row wins group 1.
    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, created_at \
         FROM ev_null ORDER BY user_id, created_at ASC",
    )
    .await;
    assert_eq!(col_i64(&batches, "user_id"), vec![1, 2]);
    assert_eq!(col_i64(&batches, "id"), vec![10, 20]);
}

/// ORDER BY direction variants: DESC on the ON column (groups emitted in
/// reverse key order) with an ASC tail (earliest row per group wins).
#[tokio::test]
async fn distinct_on_order_by_direction_variants() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) user_id, id, amount \
         FROM events ORDER BY user_id DESC, created_at ASC",
    )
    .await;

    assert_eq!(col_i64(&batches, "user_id"), vec![3, 2, 1]);
    // Earliest created_at per group: u3 → 30, u2 → 21, u1 → 10.
    assert_eq!(col_i64(&batches, "id"), vec![30, 21, 10]);
    assert_eq!(col_i64(&batches, "amount"), vec![300, 210, 100]);
}

/// PG allows the ON column to be absent from the select list.
#[tokio::test]
async fn distinct_on_key_not_in_select_list() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (user_id) id \
         FROM events ORDER BY user_id, created_at DESC",
    )
    .await;

    assert_eq!(col_i64(&batches, "id"), vec![11, 20, 30]);
}

/// Multi-column ON list with a non-key ordering tail.
#[tokio::test]
async fn distinct_on_multi_key() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE mk (a BIGINT NOT NULL, b BIGINT NOT NULL, \
         id BIGINT NOT NULL, seq BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(
        "INSERT INTO mk VALUES \
         (1, 1, 100, 1), \
         (1, 1, 101, 2), \
         (1, 2, 110, 5), \
         (2, 1, 200, 9), \
         (2, 1, 201, 3)",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT DISTINCT ON (a, b) a, b, id \
         FROM mk ORDER BY a, b, seq DESC",
    )
    .await;

    assert_eq!(col_i64(&batches, "a"), vec![1, 1, 2]);
    assert_eq!(col_i64(&batches, "b"), vec![1, 2, 1]);
    // Highest seq per (a, b): (1,1) → 101, (1,2) → 110, (2,1) → 200.
    assert_eq!(col_i64(&batches, "id"), vec![101, 110, 200]);
}

/// Equivalence against DataFusion's stock `DISTINCT ON` lowering: a vanilla
/// `SessionContext` (no Basin optimizer rules) runs the IDENTICAL query over
/// the IDENTICAL data and must produce the IDENTICAL result. The dataset has
/// a unique sort key per group, so both lowerings are fully deterministic.
#[tokio::test]
async fn distinct_on_matches_stock_datafusion_lowering() {
    use datafusion::prelude::{SessionConfig, SessionContext};

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // BIGINT created_at on both sides — keeps the comparison free of
    // timestamp-literal parsing differences between the two engines.
    let values = "(1, 10, 100, 5), (1, 11, 110, 9), (1, 12, 120, 7), \
                  (2, 20, 200, 4), (2, 21, 210, 2), \
                  (3, 30, 300, 8), (3, 31, 310, 6)";

    sess.execute(
        "CREATE TABLE ev_eq (user_id BIGINT NOT NULL, id BIGINT NOT NULL, \
         amount BIGINT NOT NULL, created_at BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(&format!("INSERT INTO ev_eq VALUES {values}"))
        .await
        .unwrap();

    let query = "SELECT DISTINCT ON (user_id) user_id, id, amount, created_at \
                 FROM ev_eq ORDER BY user_id, created_at DESC LIMIT 100";

    let engine_batches = rows(&sess, query).await;

    // Vanilla DataFusion context: stock ReplaceDistinctWithAggregate lowering.
    let cfg = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let ctx = SessionContext::new_with_config(cfg);
    ctx.sql(&format!(
        "CREATE TABLE ev_eq AS \
         SELECT column1 AS user_id, column2 AS id, column3 AS amount, \
                column4 AS created_at \
         FROM (VALUES {values}) AS v"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let stock_batches = ctx.sql(query).await.unwrap().collect().await.unwrap();

    // Extract per-column i64 vectors from the stock result (DataFusion's
    // bundled arrow version differs from the workspace one, so each side
    // uses its own downcast).
    fn stock_col_i64(
        batches: &[datafusion::arrow::record_batch::RecordBatch],
        name: &str,
    ) -> Vec<i64> {
        use datafusion::arrow::array::Int64Array as DfInt64Array;
        let mut out = Vec::new();
        for b in batches {
            let arr = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<DfInt64Array>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(arr.value(i));
            }
        }
        out
    }

    for col in ["user_id", "id", "amount", "created_at"] {
        assert_eq!(
            col_i64(&engine_batches, col),
            stock_col_i64(&stock_batches, col),
            "column {col} diverges from the stock DISTINCT ON lowering"
        );
    }
    // Expected winners (highest created_at per group): pins both lowerings
    // against the PG-semantics ground truth, not merely against each other.
    assert_eq!(col_i64(&engine_batches, "id"), vec![11, 20, 30]);
}

/// The Basin lowering must actually fire inside a real engine session:
/// EXPLAIN shows a grouped first_value plan with NO aggregate over the ON
/// column (the stock rule would emit `first_value(user_id …)`) and no
/// ROW_NUMBER window.
#[tokio::test]
async fn distinct_on_lowering_fires_in_engine_plan() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess).await;

    let batches = rows(
        &sess,
        "EXPLAIN SELECT DISTINCT ON (user_id) user_id, id, amount, created_at \
         FROM events ORDER BY user_id, created_at DESC LIMIT 100",
    )
    .await;

    let mut plan = String::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            plan.push_str(arr.value(i));
            plan.push('\n');
        }
    }

    assert!(
        plan.contains("first_value("),
        "DISTINCT ON must lower to grouped first_value; plan:\n{plan}"
    );
    // The ON column is projected from the group key — never aggregated.
    for (idx, _) in plan.match_indices("first_value(") {
        let arg_end = plan[idx..].find(')').map(|e| idx + e).unwrap_or(plan.len());
        let arg = &plan[idx..arg_end];
        assert!(
            !arg.contains("user_id"),
            "ON column must not be aggregated: {arg}\nplan:\n{plan}"
        );
    }
    assert!(
        !plan.to_lowercase().contains("row_number"),
        "DISTINCT ON must not lower through a window; plan:\n{plan}"
    );
}
