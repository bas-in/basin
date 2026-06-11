//! End-to-end checks for `ARRAY_AGG(expr ORDER BY sortkey)` and its
//! neighbours after the `array_agg` override in `pg_agg_udf::PgArrayAggUdaf`
//! (vectorized ordered accumulator replacing DataFusion's per-row
//! `ScalarValue` buffering).
//!
//! Covered:
//! - ORDER BY ASC / DESC inside the aggregate (the benchmark shape)
//! - NULLs in the sort key (PG defaults: ASC → NULLS LAST, DESC → NULLS FIRST)
//! - NULLs in the aggregated value (kept, like PG)
//! - groups spanning multiple row blocks / record batches
//! - DISTINCT paths (delegated to the DataFusion builtin) stay correct
//! - plain `ARRAY_AGG` (no ORDER BY) and `STRING_AGG` are unregressed
//! - zero-row input yields SQL NULL, not an empty array

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, ListArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
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

/// Extract `(user_id, list-of-text)` rows from a
/// `SELECT user_id, ARRAY_AGG(...) ... GROUP BY user_id` result.
/// `None` for the list means the SQL value was NULL.
fn collect_user_lists(res: ExecResult) -> Vec<(i64, Option<Vec<Option<String>>>)> {
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let user_idx = (0..b.num_columns())
            .find(|i| b.schema().field(*i).name().eq_ignore_ascii_case("user_id"))
            .expect("user_id column not present");
        let list_idx = (0..b.num_columns())
            .find(|i| {
                matches!(
                    b.schema().field(*i).data_type(),
                    arrow_schema::DataType::List(_)
                )
            })
            .expect("list column not present");
        let users = b
            .column(user_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("user_id must be Int64");
        let lists = b
            .column(list_idx)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("aggregate column must be List");
        for r in 0..b.num_rows() {
            let list = if lists.is_null(r) {
                None
            } else {
                let inner = lists.value(r);
                let sa = inner
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("list elements must be Utf8")
                    .clone();
                Some(
                    (0..sa.len())
                        .map(|i| {
                            if sa.is_null(i) {
                                None
                            } else {
                                Some(sa.value(i).to_string())
                            }
                        })
                        .collect(),
                )
            };
            out.push((users.value(r), list));
        }
    }
    out
}

fn texts(vals: &[&str]) -> Option<Vec<Option<String>>> {
    Some(vals.iter().map(|s| Some(s.to_string())).collect())
}

async fn setup_events(sess: &basin_engine::ProjectSession) {
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();
    // Insert deliberately shuffled by created_at so neither insertion order
    // nor reverse insertion order accidentally matches the expected output.
    sess.execute(
        "INSERT INTO events VALUES \
         (1, 'paid',      3), \
         (2, 'pending',  20), \
         (1, 'pending',   1), \
         (2, 'shipped',  40), \
         (1, 'shipped',   4), \
         (2, 'paid',     30), \
         (1, 'review',    2), \
         (2, 'review',   10)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn array_agg_order_by_desc() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    setup_events(&sess).await;

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at DESC) \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    let got = collect_user_lists(res);
    assert_eq!(
        got,
        vec![
            (1, texts(&["shipped", "paid", "review", "pending"])),
            (2, texts(&["shipped", "paid", "pending", "review"])),
        ],
        "per-user statuses must be ordered by created_at DESC"
    );
}

#[tokio::test]
async fn array_agg_order_by_asc() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    setup_events(&sess).await;

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at ASC) \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    let got = collect_user_lists(res);
    assert_eq!(
        got,
        vec![
            (1, texts(&["pending", "review", "paid", "shipped"])),
            (2, texts(&["review", "pending", "paid", "shipped"])),
        ],
        "per-user statuses must be ordered by created_at ASC"
    );
}

/// PG semantics for NULL sort keys: ASC defaults to NULLS LAST, DESC to
/// NULLS FIRST.
#[tokio::test]
async fn array_agg_order_by_null_sort_keys() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO events VALUES \
         (1, 'b', 2), (1, 'nokey', NULL), (1, 'a', 1)",
    )
    .await
    .unwrap();

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at ASC) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_user_lists(res),
        vec![(1, texts(&["a", "b", "nokey"]))],
        "ASC must place NULL keys last (PG default)"
    );

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at DESC) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_user_lists(res),
        vec![(1, texts(&["nokey", "b", "a"]))],
        "DESC must place NULL keys first (PG default)"
    );
}

/// NULL *values* are kept by array_agg (PG keeps them) and travel with their
/// sort key.
#[tokio::test]
async fn array_agg_order_by_null_values_kept() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO events VALUES (1, 'x', 3), (1, NULL, 1), (1, 'y', 2)")
        .await
        .unwrap();

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at ASC) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_user_lists(res),
        vec![(
            1,
            Some(vec![None, Some("y".to_string()), Some("x".to_string())])
        )],
        "NULL value must appear at its sort-key position"
    );
}

/// A single group spanning several INSERT statements (and therefore several
/// row blocks / record batches) must still come back globally sorted —
/// exercises multiple `update_batch` calls and chunk concatenation.
#[tokio::test]
async fn array_agg_order_by_multi_batch_group() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();

    // 3 inserts of 400 rows each; created_at = 1200 - i so ascending order by
    // key reverses the insertion order across all batches.
    let total: i64 = 1200;
    for chunk in 0..3i64 {
        let mut sql = String::from("INSERT INTO events VALUES ");
        for i in (chunk * 400)..((chunk + 1) * 400) {
            if i > chunk * 400 {
                sql.push_str(", ");
            }
            write!(sql, "(1, 's{i}', {})", total - i).unwrap();
        }
        sess.execute(&sql).await.unwrap();
    }

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at ASC) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    let got = collect_user_lists(res);
    assert_eq!(got.len(), 1, "one group expected");
    let (user, list) = &got[0];
    assert_eq!(*user, 1);
    let list = list.as_ref().expect("group must not be NULL");
    assert_eq!(list.len(), total as usize, "all rows aggregated");
    // created_at = 1200 - i ascending ⇒ i descending ⇒ s1199, s1198, ..., s0.
    let expected: Vec<Option<String>> = (0..total).rev().map(|i| Some(format!("s{i}"))).collect();
    assert_eq!(*list, expected, "1200-row group must be globally sorted");
}

/// DISTINCT paths are delegated to the DataFusion builtin and must keep
/// working. `ARRAY_AGG(DISTINCT x ORDER BY x)` is the only DISTINCT+ORDER BY
/// combination PG (and DataFusion) allow.
#[tokio::test]
async fn array_agg_distinct_paths() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO events VALUES \
         (1, 'b', 1), (1, 'a', 2), (1, 'b', 3), (1, 'c', 4), (1, 'a', 5)",
    )
    .await
    .unwrap();

    // DISTINCT with ORDER BY on the same column: deterministic sorted output.
    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(DISTINCT status ORDER BY status) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_user_lists(res),
        vec![(1, texts(&["a", "b", "c"]))],
        "DISTINCT + ORDER BY must dedupe and sort"
    );

    // Bare DISTINCT: order unspecified — compare as a sorted multiset.
    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(DISTINCT status) \
             FROM events GROUP BY user_id",
        )
        .await
        .unwrap();
    let got = collect_user_lists(res);
    assert_eq!(got.len(), 1);
    let mut vals = got[0].1.clone().expect("non-NULL");
    vals.sort();
    assert_eq!(
        vals,
        vec![Some("a".into()), Some("b".into()), Some("c".into())],
        "bare DISTINCT must dedupe"
    );
}

/// Plain `ARRAY_AGG` (no ORDER BY) still goes through the builtin vectorized
/// GroupsAccumulator — content must be the full multiset of inputs.
#[tokio::test]
async fn array_agg_without_order_by_unregressed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    setup_events(&sess).await;

    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status) \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    let got = collect_user_lists(res);
    assert_eq!(got.len(), 2);
    for (user, list) in got {
        let mut vals = list.expect("non-NULL");
        vals.sort();
        assert_eq!(
            vals,
            vec![
                Some("paid".into()),
                Some("pending".into()),
                Some("review".into()),
                Some("shipped".into()),
            ],
            "user {user}: plain ARRAY_AGG must keep all rows"
        );
    }
}

/// STRING_AGG shares no code with the array_agg override but is the
/// neighbouring benchmark card — pin its behaviour (multiset of parts).
#[tokio::test]
async fn string_agg_unregressed() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    setup_events(&sess).await;

    let res = sess
        .execute(
            "SELECT user_id, STRING_AGG(status, ',') AS s \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let mut rows: Vec<(i64, Vec<String>)> = Vec::new();
    for b in &batches {
        let users = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("user_id must be Int64");
        // string_agg output may be Utf8 or LargeUtf8 depending on mapping.
        let texts: Vec<String> =
            if let Some(sa) = b.column(1).as_any().downcast_ref::<StringArray>() {
                (0..sa.len()).map(|i| sa.value(i).to_string()).collect()
            } else if let Some(sa) = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
            {
                (0..sa.len()).map(|i| sa.value(i).to_string()).collect()
            } else {
                panic!("string_agg column must be Utf8/LargeUtf8");
            };
        for r in 0..b.num_rows() {
            let mut parts: Vec<String> = texts[r].split(',').map(|s| s.to_string()).collect();
            parts.sort();
            rows.push((users.value(r), parts));
        }
    }
    let expected_parts = vec![
        "paid".to_string(),
        "pending".to_string(),
        "review".to_string(),
        "shipped".to_string(),
    ];
    assert_eq!(
        rows,
        vec![(1, expected_parts.clone()), (2, expected_parts)],
        "STRING_AGG must contain every status exactly once per user"
    );
}

/// Zero input rows: PG returns SQL NULL (not '{}') for an ungrouped
/// ARRAY_AGG with ORDER BY.
#[tokio::test]
async fn array_agg_order_by_empty_input_is_null() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT ARRAY_AGG(status ORDER BY created_at DESC) FROM events")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "ungrouped aggregate yields exactly one row");
    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let col = b.column(0);
    assert!(
        col.is_null(0),
        "ARRAY_AGG over zero rows must be SQL NULL, got {col:?}"
    );
}
