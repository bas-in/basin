//! S3 port of `viability_alter_add_column.rs`.
//!
//! Card: `viability_alter_add_column` (real-cloud dashboard).
//! Bar: `rows_visible == 10 && pre_alter_tags_all_null && post_alter_tags_match`
//! — same shape as LocalFS. The point of this card on real S3 is to verify
//! that Parquet schema evolution works when the data files live in a real
//! object store: the writer's footer-based schema, the reader's
//! evolution-on-read code path, and the catalog round-trip all behave
//! identically end-to-end.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_viability_alter_add_column";

#[derive(Debug, Clone)]
struct EventRow {
    id: i64,
    payload: Option<String>,
    tag: Option<String>,
}

fn collect_all_rows(result: ExecResult) -> Vec<EventRow> {
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!(
            "expected ExecResult::Rows for SELECT, got Empty with tag={tag:?}"
        ),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_alter_add_column() {
    basin_common::telemetry::try_init_for_tests();

    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let _table = TableName::new("events").unwrap();
    let session = engine.open_session(tenant).await.unwrap();

    session
        .execute("CREATE TABLE events (id BIGINT, ts BIGINT, payload TEXT)")
        .await
        .expect("CREATE TABLE");

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

    let alter_result = session
        .execute("ALTER TABLE events ADD COLUMN tag TEXT")
        .await;
    assert!(
        alter_result.is_ok(),
        "ALTER TABLE ADD COLUMN failed: {:?}",
        alter_result.err()
    );

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

    let result = session
        .execute("SELECT id, payload, tag FROM events ORDER BY id")
        .await
        .expect("SELECT after ALTER");
    let rows = collect_all_rows(result);
    assert_eq!(rows.len(), 10, "expected 10 rows, got {}", rows.len());

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
    let pass =
        rows_visible == 10 && pre_alter_tags_all_null && post_alter_tags_match;

    println!(
        "[S3 viability_alter_add_column] rows_visible={rows_visible}, pre_alter_tags_all_null={pre_alter_tags_all_null}, post_alter_tags_match={post_alter_tags_match}"
    );

    report_real_viability(
        "alter_add_column",
        "ALTER TABLE ... ADD COLUMN end-to-end (real S3)",
        "Same shape as the LocalFS card, run against a real S3-compatible backend: \
         CREATE TABLE -> INSERT -> ALTER TABLE ADD COLUMN -> INSERT (wider) -> SELECT \
         shows 10 rows: the 5 pre-ALTER rows project the new column to NULL via Parquet \
         schema evolution; the 5 post-ALTER rows carry their inserted values.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(pass, "alter_add_column viability bar not met");
}
