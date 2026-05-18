//! Viability test: `ALTER TABLE ... ADD COLUMN` end-to-end through the
//! SQL surface.
//!
//! Card: `viability_alter_add_column`
//! Bar: `rows_visible == 10 && pre_alter_tags_all_null && post_alter_tags_match`
//!
//! This is the load-bearing integration test for Basin's schema-evolution
//! claim. The earlier round of work claimed end-to-end ADD COLUMN was
//! wired up; this test exercises the full path:
//!
//! 1. CREATE TABLE.
//! 2. INSERT 5 rows under the original schema.
//! 3. ALTER TABLE ... ADD COLUMN tag TEXT (the load-bearing line).
//! 4. INSERT 5 more rows under the wider schema, supplying a value for
//!    the new column.
//! 5. SELECT id, payload, tag FROM events ORDER BY id and assert:
//!    - 10 rows visible.
//!    - The first 5 (pre-ALTER) rows show `tag = NULL` (Parquet schema
//!      evolution: missing columns project to NULL on read).
//!    - The last 5 (post-ALTER) rows show their inserted `tag` values.
//!
//! If this test fails, the ALTER ADD COLUMN claim is false. The likely
//! failure modes (and where to look) are documented inline in
//! `crates/basin-engine/src/executor.rs::exec_alter_table` and
//! `crates/basin-engine/src/session.rs::refresh_table`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct EventRow {
    id: i64,
    payload: Option<String>,
    tag: Option<String>,
}

/// Collect every batch in `result` into a flat `Vec<EventRow>`, projecting
/// (id, payload, tag) by name. `tag` may be missing or NULL for pre-ALTER
/// rows; `payload` is required-non-null in the table schema, but we tolerate
/// NULL here for safety.
fn collect_all_rows(result: ExecResult) -> Vec<EventRow> {
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => {
            panic!("expected ExecResult::Rows for SELECT, got Empty with tag={tag:?}")
        }
    };

    let mut out = Vec::new();
    for batch in batches.iter() {
        let id_arr = batch
            .column_by_name("id")
            .expect("id column missing")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column not Int64Array");
        let payload_arr = batch
            .column_by_name("payload")
            .expect("payload column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column not StringArray");
        let tag_arr = batch
            .column_by_name("tag")
            .expect("tag column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag column not StringArray");

        for i in 0..batch.num_rows() {
            let id = id_arr.value(i);
            let payload = if payload_arr.is_null(i) {
                None
            } else {
                Some(payload_arr.value(i).to_string())
            };
            let tag = if tag_arr.is_null(i) {
                None
            } else {
                Some(tag_arr.value(i).to_string())
            };
            out.push(EventRow { id, payload, tag });
        }
    }
    out
}

#[tokio::test]
async fn viability_alter_add_column() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs,
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let project = ProjectId::new();
    let _table = TableName::new("events").unwrap();
    let session = engine.open_session(project).await.unwrap();

    // 1. CREATE TABLE.
    session
        .execute("CREATE TABLE events (id BIGINT, ts BIGINT, payload TEXT)")
        .await
        .expect("CREATE TABLE");

    // 2. INSERT 5 rows BEFORE the ALTER.
    for i in 0..5_i64 {
        session
            .execute(&format!(
                "INSERT INTO events (id, ts, payload) VALUES ({}, {}, 'before-{}')",
                i,
                i * 1000,
                i
            ))
            .await
            .expect("INSERT before-ALTER");
    }

    // 3. ALTER TABLE ADD COLUMN — the critical line.
    let alter_result = session
        .execute("ALTER TABLE events ADD COLUMN tag TEXT")
        .await;
    assert!(
        alter_result.is_ok(),
        "ALTER TABLE ADD COLUMN failed: {:?}",
        alter_result.err()
    );

    // 4. INSERT 5 rows AFTER the ALTER, including the new column.
    for i in 5..10_i64 {
        let tag = format!("after-{i}");
        session
            .execute(&format!(
                "INSERT INTO events (id, ts, payload, tag) VALUES ({}, {}, 'after-{}', '{}')",
                i,
                i * 1000,
                i,
                tag
            ))
            .await
            .expect("INSERT after-ALTER");
    }

    // 5. SELECT * — assert all 10 rows visible.
    let result = session
        .execute("SELECT id, payload, tag FROM events ORDER BY id")
        .await
        .expect("SELECT after ALTER");
    let rows = collect_all_rows(result);
    assert_eq!(rows.len(), 10, "expected 10 rows, got {}", rows.len());

    // 6. The first 5 rows must show tag = NULL (legacy rows pre-ADD COLUMN).
    let mut pre_alter_tags_all_null = true;
    for (i, row) in rows.iter().enumerate().take(5) {
        assert_eq!(row.id, i as i64, "row order broken at {i}");
        if row.tag.is_some() {
            pre_alter_tags_all_null = false;
        }
        assert!(
            row.tag.is_none(),
            "pre-ALTER row {i} should have tag=NULL, got {:?}",
            row.tag
        );
        let want_payload = format!("before-{i}");
        assert_eq!(row.payload.as_deref(), Some(want_payload.as_str()));
    }

    // 7. The last 5 rows must show their inserted tag values.
    let mut post_alter_tags_match = true;
    for (j, row) in rows.iter().enumerate().skip(5).take(5) {
        let i = j as i64;
        assert_eq!(row.id, i, "row order broken at {j}");
        let want_tag = format!("after-{i}");
        if row.tag.as_deref() != Some(want_tag.as_str()) {
            post_alter_tags_match = false;
        }
        assert_eq!(
            row.tag.as_deref(),
            Some(want_tag.as_str()),
            "post-ALTER row {j} expected tag={want_tag:?}, got {:?}",
            row.tag
        );
        let want_payload = format!("after-{i}");
        assert_eq!(row.payload.as_deref(), Some(want_payload.as_str()));
    }

    let rows_visible = rows.len();
    let pass = rows_visible == 10 && pre_alter_tags_all_null && post_alter_tags_match;

    println!(
        "[VIABILITY alter_add_column] rows_visible={rows_visible}, pre_alter_tags_all_null={pre_alter_tags_all_null}, post_alter_tags_match={post_alter_tags_match}"
    );

    report_viability(
        "alter_add_column",
        "ALTER TABLE ... ADD COLUMN end-to-end",
        "CREATE TABLE -> INSERT -> ALTER TABLE ADD COLUMN -> INSERT (wider) -> SELECT \
         shows 10 rows: the 5 pre-ALTER rows project the new column to NULL (Parquet \
         schema evolution), the 5 post-ALTER rows carry their inserted values.",
        pass,
        PrimaryMetric {
            label: "rows_visible_after_alter".into(),
            value: rows_visible as f64,
            unit: "rows".into(),
            bar: BarOp::eq(10.0),
        },
        json!({
            "rows_visible": rows_visible,
            "pre_alter_tags_all_null": pre_alter_tags_all_null,
            "post_alter_tags_match": post_alter_tags_match,
        }),
    );

    assert!(pass, "alter_add_column viability bar not met");
}
