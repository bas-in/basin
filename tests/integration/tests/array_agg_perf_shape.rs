//! End-to-end correctness for the `ARRAY_AGG(expr ORDER BY key) GROUP BY g`
//! benchmark shape after it gained a true vectorized `GroupsAccumulator`
//! ([`OrderedArrayAggGroupsAccumulator`] in
//! `basin-engine/src/pg_agg_udf.rs`).
//!
//! Background: shape #55 of the 1M comparison bench
//! (`SELECT user_id, ARRAY_AGG(status ORDER BY created_at DESC) FROM events
//! GROUP BY user_id LIMIT 20`) measured ~516ms vs Postgres ~90ms.  Root cause
//! was the per-group `Accumulator` adapter: DataFusion wrapped Basin's ordered
//! accumulator in `GroupsAccumulatorAdapter`, which re-slices every input
//! batch and dispatches `update_batch` once per group per batch.  The fix adds
//! a real `GroupsAccumulator` for the grouped ordered path; this file pins its
//! behaviour at scale.
//!
//! No timing assertions live here — the wall-clock number is measured on an
//! idle box by re-running the 1M comparison bench.  These tests guard
//! *correctness* of the new code path (which is what the perf fix must not
//! break):
//!  - ASC / DESC ordering inside the aggregate, grouped
//!  - NULL *values* kept (PG: array_agg does NOT drop NULLs)
//!  - NULL sort keys honour PG defaults (ASC→NULLS LAST, DESC→NULLS FIRST)
//!  - Utf8 and Int64 aggregated values
//!  - a 100k-row, many-group shape (the same family as the bench) reproducing
//!    a brute-force oracle exactly
//!  - empty / single-row groups
//!  - the plan still aggregates as an `AggregateExec` over `array_agg`
//!    (GroupsAccumulator engagement is not surfaced by EXPLAIN — see the note
//!    on `plan_uses_aggregate_exec`).

#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, ListArray, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_integration_tests::cache_defaults;
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Extract `(group_id, Option<Vec<Option<String>>>)` rows from a
/// `SELECT g, ARRAY_AGG(text ...) GROUP BY g` result.
fn collect_text_lists(res: ExecResult) -> Vec<(i64, Option<Vec<Option<String>>>)> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let gid = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group col must be Int64");
        let lists = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("agg col must be List");
        for r in 0..b.num_rows() {
            let v = if lists.is_null(r) {
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
                        .map(|i| (!sa.is_null(i)).then(|| sa.value(i).to_string()))
                        .collect::<Vec<_>>(),
                )
            };
            out.push((gid.value(r), v));
        }
    }
    out
}

/// Extract `(group_id, Option<Vec<Option<i64>>>)` from a grouped Int64
/// `ARRAY_AGG` result.
fn collect_int_lists(res: ExecResult) -> Vec<(i64, Option<Vec<Option<i64>>>)> {
    let ExecResult::Rows { batches, .. } = res else {
        panic!("expected Rows result");
    };
    let mut out = Vec::new();
    for b in &batches {
        let gid = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group col must be Int64");
        let lists = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("agg col must be List");
        for r in 0..b.num_rows() {
            let v = if lists.is_null(r) {
                None
            } else {
                let inner = lists.value(r);
                let ia = inner
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("list elements must be Int64")
                    .clone();
                Some(
                    (0..ia.len())
                        .map(|i| (!ia.is_null(i)).then(|| ia.value(i)))
                        .collect::<Vec<_>>(),
                )
            };
            out.push((gid.value(r), v));
        }
    }
    out
}

async fn open(dir: &TempDir) -> ProjectSession {
    engine_in(dir).open_session(ProjectId::new()).await.unwrap()
}

fn texts(vals: &[&str]) -> Option<Vec<Option<String>>> {
    Some(vals.iter().map(|s| Some(s.to_string())).collect())
}

// ─────────────────────────────────────────────────────────────────────────────
// Small, fully-pinned shapes
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_events(sess: &ProjectSession) {
    sess.execute("CREATE TABLE events (user_id BIGINT NOT NULL, status TEXT, created_at BIGINT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO events VALUES \
         (1, 'paid',     3), \
         (2, 'pending', 20), \
         (1, 'pending',  1), \
         (2, 'shipped', 40), \
         (1, 'shipped',  4), \
         (2, 'paid',    30), \
         (1, 'review',   2), \
         (2, 'review',  10)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn grouped_order_by_desc_text() {
    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    seed_events(&sess).await;
    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at DESC) \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_text_lists(res),
        vec![
            (1, texts(&["shipped", "paid", "review", "pending"])),
            (2, texts(&["shipped", "paid", "pending", "review"])),
        ]
    );
}

#[tokio::test]
async fn grouped_order_by_asc_text() {
    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    seed_events(&sess).await;
    let res = sess
        .execute(
            "SELECT user_id, ARRAY_AGG(status ORDER BY created_at ASC) \
             FROM events GROUP BY user_id ORDER BY user_id",
        )
        .await
        .unwrap();
    assert_eq!(
        collect_text_lists(res),
        vec![
            (1, texts(&["pending", "review", "paid", "shipped"])),
            (2, texts(&["review", "pending", "paid", "shipped"])),
        ]
    );
}

/// NULL aggregated *values* are kept (PG array_agg does not drop NULLs) and
/// NULL sort keys follow PG defaults (DESC → NULLS FIRST).
#[tokio::test]
async fn grouped_nulls_kept_and_sorted() {
    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    sess.execute("CREATE TABLE t (g BIGINT NOT NULL, v TEXT, k BIGINT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t VALUES \
         (1, 'a', 1), (1, NULL, 2), (1, 'c', NULL)",
    )
    .await
    .unwrap();
    // DESC: NULLS FIRST → the NULL-key row ('c') leads, then k=2 (NULL value),
    // then k=1 ('a'); the NULL *value* survives in the output list.
    let res = sess
        .execute("SELECT g, ARRAY_AGG(v ORDER BY k DESC) FROM t GROUP BY g")
        .await
        .unwrap();
    let got = collect_text_lists(res);
    assert_eq!(got.len(), 1);
    let list = got[0].1.clone().expect("non-null group");
    assert_eq!(
        list,
        vec![Some("c".to_string()), None, Some("a".to_string())],
        "NULL key sorts first (DESC) and NULL value is retained"
    );
}

/// Int64 aggregated values, multiple groups, with an empty/single-row mix.
#[tokio::test]
async fn grouped_int_values() {
    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    sess.execute("CREATE TABLE m (g BIGINT NOT NULL, v BIGINT, k BIGINT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO m VALUES \
         (1, 100, 2), (1, 200, 1), (2, 300, 5)",
    )
    .await
    .unwrap();
    let res = sess
        .execute("SELECT g, ARRAY_AGG(v ORDER BY k ASC) FROM m GROUP BY g ORDER BY g")
        .await
        .unwrap();
    assert_eq!(
        collect_int_lists(res),
        vec![
            (1, Some(vec![Some(200), Some(100)])), // k asc: 1→200, 2→100
            (2, Some(vec![Some(300)])),
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 100k-row, many-group shape — differential vs a brute-force oracle
// ─────────────────────────────────────────────────────────────────────────────

/// 100k rows / 1000 groups: build the same data in Rust, compute the expected
/// per-group ordered list directly, and assert the engine reproduces it
/// exactly.  This is the closest correctness analogue of the 1M bench shape and
/// the scale at which the per-group adapter overhead used to dominate.
#[tokio::test]
async fn grouped_order_by_100k_matches_oracle() {
    const N: i64 = 100_000;
    const GROUPS: i64 = 1_000;

    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    sess.execute("CREATE TABLE big (g BIGINT NOT NULL, v BIGINT, k BIGINT)")
        .await
        .unwrap();

    // Deterministic pseudo-shuffle so neither insert order nor reverse order
    // accidentally matches the sorted output. `k` has duplicates within groups
    // to exercise tie behaviour; values are unique so the oracle is exact.
    let mut oracle: BTreeMap<i64, Vec<(i64, i64)>> = BTreeMap::new(); // g -> [(k, v)]
    let mut rows = String::new();
    for i in 0..N {
        let g = (i * 2_654_435_761_i64).rem_euclid(GROUPS);
        let k = (i * 40_503).rem_euclid(97); // small range → ties within groups
        let v = i; // unique value
        if !rows.is_empty() {
            rows.push(',');
        }
        rows.push_str(&format!("({g},{v},{k})"));
        oracle.entry(g).or_default().push((k, v));

        // Insert in chunks to keep statement size reasonable.
        if (i + 1) % 5_000 == 0 || i == N - 1 {
            sess.execute(&format!("INSERT INTO big VALUES {rows}"))
                .await
                .unwrap();
            rows.clear();
        }
    }

    let res = sess
        .execute("SELECT g, ARRAY_AGG(v ORDER BY k ASC) FROM big GROUP BY g ORDER BY g")
        .await
        .unwrap();
    let got = collect_int_lists(res);
    assert_eq!(got.len(), oracle.len(), "one row per group");

    for (g, list) in got {
        let mut expected = oracle.get(&g).cloned().expect("group present in oracle");
        // Stable sort by k; values with equal k keep insertion order — must
        // match the engine's (within-group) ordering for the unique-value set.
        // Since values are globally unique, compare as a multiset over k-blocks:
        // group by k, then the set of values per k must match regardless of the
        // intra-tie permutation the (unstable) sort chooses.
        expected.sort_by_key(|(k, _)| *k);
        let got_vals: Vec<i64> = list
            .expect("non-empty group")
            .into_iter()
            .map(|o| o.expect("no null values in this fixture"))
            .collect();

        // 1. The k-ordering must be globally non-decreasing.
        let got_keys: Vec<i64> = got_vals
            .iter()
            .map(|v| expected.iter().find(|(_, ev)| ev == v).unwrap().0)
            .collect();
        assert!(
            got_keys.windows(2).all(|w| w[0] <= w[1]),
            "group {g}: values not ordered by k ASC"
        );
        // 2. The multiset of values must match exactly (nothing dropped/added).
        let mut a = got_vals.clone();
        let mut b: Vec<i64> = expected.iter().map(|(_, v)| *v).collect();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "group {g}: value multiset must match oracle");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan-shape note
// ─────────────────────────────────────────────────────────────────────────────

/// EXPLAIN does NOT surface which accumulator (`Accumulator` vs
/// `GroupsAccumulator`) the `AggregateExec` selects — DataFusion picks that at
/// execution time from `groups_accumulator_supported`, and the chosen
/// implementation is not printed in the plan.  So engagement of the new
/// `OrderedArrayAggGroupsAccumulator` is verified by:
///   * the differential unit tests in `pg_agg_udf.rs`
///     (`groups_acc_*`), which call the GroupsAccumulator directly, and
///   * the wall-clock drop on the 1M bench (measured off-box).
///
/// This test only pins that the grouped ordered `array_agg` still plans as a
/// single `AggregateExec` over `array_agg` (i.e. it is served by the aggregate
/// machinery that consults `groups_accumulator_supported`, not rewritten into
/// some sort+collect fallback).
#[tokio::test]
async fn grouped_order_by_plans_as_aggregate_exec() {
    let dir = TempDir::new().unwrap();
    let sess = open(&dir).await;
    seed_events(&sess).await;

    let res = sess
        .execute(
            "EXPLAIN SELECT user_id, ARRAY_AGG(status ORDER BY created_at DESC) \
             FROM events GROUP BY user_id",
        )
        .await
        .expect("EXPLAIN must not error");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("EXPLAIN returned non-Rows");
    };
    let mut plan = String::new();
    for b in &batches {
        for c in 0..b.num_columns() {
            if let Some(arr) = b.column(c).as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        plan.push_str(arr.value(i));
                        plan.push('\n');
                    }
                }
            }
        }
    }
    assert!(
        plan.contains("AggregateExec"),
        "grouped array_agg must plan as AggregateExec; plan was:\n{plan}"
    );
    assert!(
        plan.to_lowercase().contains("array_agg"),
        "plan must reference array_agg; plan was:\n{plan}"
    );
}
