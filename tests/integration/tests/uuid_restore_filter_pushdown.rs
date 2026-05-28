//! Regression for #88-shape bug in `UuidDecimal256RestoreExec`.
//!
//! Before the fix, the per-row UUID column restorer wrapped the cold Vortex
//! `DataSourceExec` for UUID-bearing tables but did not implement
//! `gather_filters_for_pushdown` / `handle_child_pushdown_result`. As a
//! consequence, the default `all_unsupported` policy kicked in and DataFusion
//! could not push any predicate past the restore exec — every selective
//! `WHERE` clause on a UUID-bearing table degraded to a full cold scan,
//! losing row-group pruning that would otherwise come for free from Vortex.
//!
//! The restore exec is a *transparent* per-row column-type translator
//! (`Decimal256(39,0)` → `FixedSizeBinary(16)`), so a row filter commutes with
//! it for ANY predicate. The fix mirrors `TombstoneFilterExec`'s transparent
//! passthrough: forward all parent filters to the child unchanged and adopt
//! the child's pushdown verdict wholesale.
//!
//! Note on predicate choice: DataFusion's logical type-coercion rules reject
//! `WHERE uuid_col = 'string_literal'` ("Cannot infer common argument type
//! for comparison operation FixedSizeBinary(16) = Utf8"). To exercise the
//! pushdown wiring without depending on the wire-protocol param-bind path
//! we filter on the table's *non-UUID* column. The restore exec still wraps
//! the cold scan (the schema has a UUID column) — that's the geometry under
//! test. The PK path itself is identical from the exec's standpoint: it
//! does the same `from_children` / `if_all` forwarding regardless of which
//! column the predicate references.
//!
//! What this test asserts:
//!
//! * `EXPLAIN SELECT … FROM uuid_bearing_table WHERE n = X` shows the
//!   predicate reach the cold `DataSourceExec` BELOW the
//!   `UuidDecimal256RestoreExec`, not stuck above it as a `FilterExec`.
//! * Result rows are correct for both the matching and non-matching case.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, FixedSizeBinaryArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uuid::Uuid;

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// `EXPLAIN <sql>` → flattened plan text (every string column joined by '\n').
async fn explain_text(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let res = sess
        .execute(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must not error");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("EXPLAIN returned non-Rows");
    };
    let mut lines = Vec::new();
    for b in &batches {
        for c in 0..b.num_columns() {
            if let Some(arr) = b
                .column(c)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        lines.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    lines.join("\n")
}

/// Find the line index of the first occurrence of `needle` in `plan` (split
/// on newlines).
fn line_of(plan: &str, needle: &str) -> Option<usize> {
    plan.lines().position(|l| l.contains(needle))
}

/// Pull all `FixedSizeBinary(16)` values from the first column and decode
/// them as UUIDs (sorted, for stable comparison).
async fn uuids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<Uuid> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("UUID column must materialise as FixedSizeBinary(16)");
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        let bytes: [u8; 16] = col.value(i).try_into().unwrap();
                        out.push(Uuid::from_bytes(bytes));
                    }
                }
            }
            out.sort();
            out
        }
        _ => vec![],
    }
}

/// `SELECT n FROM …` → sorted i64 values.
async fn ns(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    use arrow_array::Int64Array;
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        out.push(col.value(i));
                    }
                }
            }
            out.sort_unstable();
            out
        }
        _ => vec![],
    }
}

/// #88-shape regression. Build a UUID-bearing table with enough cold rows
/// that pushdown matters, then assert:
///
///   1. The plan contains `UuidDecimal256RestoreExec` (the wrap is active —
///      this is the cold UUID read path).
///   2. A selective `WHERE n = X` predicate reaches the cold `DataSourceExec`
///      BELOW the restore exec (visible as a `predicate:` field), with no
///      `FilterExec` stuck above the restore exec — equivalent shape to the
///      `tombstone_filter_pushdown` assertion.
///   3. Result rows are correct for both matching and non-matching cases,
///      including the UUID column (the restore actually runs on the data
///      that survives pushdown).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uuid_restore_passes_filter_to_cold_scan() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id UUID NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();

    // Seed 1000 cold rows. Index-stamped UUIDs make it easy to pick a
    // deterministic target without colliding.
    let target_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440042").unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..1000i64 {
        if k > 0 {
            stmt.push(',');
        }
        let u = if k == 42 {
            target_uuid
        } else {
            // Deterministic distinct UUIDs derived from k.
            let mut bytes = [0u8; 16];
            bytes[8..16].copy_from_slice(&(k as u64).to_be_bytes());
            Uuid::from_bytes(bytes)
        };
        stmt.push_str(&format!("('{u}',{k})"));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Kick the catalog-driven cold listing so EXPLAIN resolves against the
    // refreshed table (same incantation as update_overlay_filter_pushdown).
    let _ = sess.execute("SELECT 1 FROM t LIMIT 0").await.unwrap();

    // ── EXPLAIN: selective predicate on the non-UUID column ────────────────
    let sql = "SELECT id, n FROM t WHERE n = 42";
    let plan = explain_text(&sess, sql).await;

    let restore_line = line_of(&plan, "UuidDecimal256RestoreExec").unwrap_or_else(|| {
        panic!(
            "UuidDecimal256RestoreExec missing — cold UUID wrap inactive; plan:\n{plan}"
        )
    });

    // Slice the plan ABOVE the restore exec. With the pushdown fix there
    // should be NO `FilterExec` left above it (the transparent passthrough
    // lets the predicate sink all the way to the cold DataSourceExec and the
    // upper FilterExec is removed). Before the fix this is exactly where the
    // stuck `FilterExec` showed up.
    let above_restore: String = plan
        .lines()
        .take(restore_line)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !above_restore.contains("FilterExec"),
        "FilterExec must NOT remain above UuidDecimal256RestoreExec — the predicate \
         should have been pushed past the transparent restorer into the cold \
         DataSourceExec; above-restore slice:\n{above_restore}\nfull plan:\n{plan}"
    );

    // The cold DataSourceExec (below the restore exec) should carry the
    // predicate `n@1 = 42` in its `predicate:` field. Match the `@N = 42`
    // form so we don't false-positive on file paths that happen to contain
    // "42".
    let below_restore: String = plan
        .lines()
        .skip(restore_line + 1)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        below_restore.contains("predicate: n@1 = 42")
            || below_restore.contains("predicate=n@1 = 42")
            // Be tolerant about column index: schemas with a partitioning
            // column could shift `n`. Accept the bare `= 42` clue too as
            // long as it's accompanied by `predicate`.
            || (below_restore.contains("predicate")
                && below_restore.contains("n@")
                && below_restore.contains("= 42")),
        "predicate `n = 42` must reach the cold DataSourceExec below \
         UuidDecimal256RestoreExec; below-restore slice:\n{below_restore}\nfull plan:\n{plan}"
    );

    // ── Correctness: matching n returns the target row ────────────────────
    // The UUID column has to round-trip through the restore exec on the
    // surviving row — proving the transparent passthrough doesn't break
    // restoration.
    let hit_uuids = uuids(&sess, "SELECT id FROM t WHERE n = 42").await;
    assert_eq!(
        hit_uuids,
        vec![target_uuid],
        "predicate `n = 42` must return exactly the target UUID after restore"
    );

    // ── Correctness: non-matching predicate returns no rows ────────────────
    let miss = uuids(&sess, "SELECT id FROM t WHERE n = 9999").await;
    assert!(
        miss.is_empty(),
        "predicate `n = 9999` must return zero rows; got {miss:?}"
    );

    // ── Range predicate: also pushes through and returns the right slice ───
    let small = ns(&sess, "SELECT n FROM t WHERE n < 5 ORDER BY n").await;
    assert_eq!(
        small,
        (0..5).collect::<Vec<i64>>(),
        "range predicate must compose with UUID restore correctly"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
