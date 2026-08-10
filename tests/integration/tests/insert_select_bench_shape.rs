//! End-to-end coverage for the benchmark DML-pipeline shape #100:
//!
//!   `INSERT INTO events_copy SELECT * FROM events WHERE id < 10000`
//!
//! against the exact `events` schema the Postgres-compare card seeds
//! (BIGINT pk, BIGINT/DOUBLE/TEXT columns, `created_at BIGINT`, and a
//! JSONB `payload`). This shape GAPped on the fresh 1M card even though
//! `events_copy` is declared column-for-column identical to `events`:
//!
//! * The benchmark's JSONB suite (#28-#37) hammers `payload->>'category'`,
//!   so the auto-promoter (AUTO_PROMOTE_MIN_HITS = 3) promotes the path and
//!   the DataFusion-registered schema for `events` gains a hidden
//!   `__promoted$payload$category` shadow column. The DML-pipeline suite
//!   runs LAST, so by the time shape #100 executes, its wildcard source
//!   `SELECT * FROM events` materialises 7 columns against a 6-column
//!   target and `exec_insert_select` rejected it ("source has 7 columns,
//!   target has 6"). The fix drops `__promoted$…` columns from the
//!   materialised source before mapping.
//! * Secondarily, `exec_insert_select` required byte-identical Arrow types
//!   between source and target; it now casts (safe=false, so lossy casts
//!   still error like Postgres) — covering view-typed sources.
//!
//! The test uses the real engine (LocalFileSystem storage + InMemoryCatalog
//! + a Shard with background flush), promotes the same JSONB path the
//! benchmark suite auto-promotes, and forces the seeded slice cold with
//! `flush_to_parquet()` (which backfills the shadow column into the cold
//! file) — reproducing the exact registered-schema state the 1M card was
//! in when the shape failed.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow_schema::DataType;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a real engine: LocalFileSystem storage + InMemoryCatalog + a Shard
/// with background flush. Returns the Shard so the test can
/// `flush_to_parquet()` to force seeded data cold, and the bg handle / wal so
/// the test can shut the background loop down cleanly.
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
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// Deterministic JSONB payload for row `i` — same shape as the benchmark
/// seed's `payload_for` (category / device.os / tags[] / metadata.score).
fn payload_for(i: i64) -> String {
    let category = match i % 3 {
        0 => "purchase",
        1 => "signup",
        _ => "click",
    };
    let device_os = if i % 2 == 0 { "ios" } else { "android" };
    format!(
        r#"{{"category":"{category}","device":{{"os":"{device_os}","version":"1.{}"}},"tags":[{},{}],"metadata":{{"score":{}}}}}"#,
        i % 9,
        i % 3,
        i % 5,
        i % 100
    )
}

fn status_for(i: i64) -> &'static str {
    match i % 4 {
        0 => "pending",
        1 => "active",
        2 => "completed",
        _ => "archived",
    }
}

/// Run a query and return its row batches (panics on non-Rows).
async fn query(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

/// Extract a column as `Vec<Option<i64>>`, casting whatever runtime Arrow
/// encoding the tier produced (Int64 either way for these columns).
fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    col_via_cast(batches, name, &DataType::Int64, |a, i| {
        a.as_any().downcast_ref::<Int64Array>().unwrap().value(i)
    })
}

fn col_f64(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    col_via_cast(batches, name, &DataType::Float64, |a, i| {
        a.as_any().downcast_ref::<Float64Array>().unwrap().value(i)
    })
}

/// Extract a string column. Cold (Vortex) reads surface `Utf8View`, hot
/// memtable reads `Utf8` — the cast canonicalises both.
fn col_str(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    col_via_cast(batches, name, &DataType::Utf8, |a, i| {
        a.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .to_string()
    })
}

/// Extract a binary (JSONB) column. Cold reads surface `BinaryView`/`Binary`,
/// hot reads the catalog `LargeBinary` — the cast canonicalises all three.
fn col_bytes(batches: &[RecordBatch], name: &str) -> Vec<Option<Vec<u8>>> {
    col_via_cast(batches, name, &DataType::LargeBinary, |a, i| {
        a.as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(i)
            .to_vec()
    })
}

fn col_via_cast<T>(
    batches: &[RecordBatch],
    name: &str,
    canon: &DataType,
    get: impl Fn(&dyn Array, usize) -> T,
) -> Vec<Option<T>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap_or_else(|_| {
            panic!(
                "column {name:?} missing from result schema {:?}",
                b.schema()
            )
        });
        let arr = arrow::compute::cast(b.column(idx), canon)
            .unwrap_or_else(|e| panic!("cast column {name:?} to {canon:?}: {e}"));
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(get(arr.as_ref(), i)));
            }
        }
    }
    out
}

/// The exact benchmark shape (#100) at 1k scale: seed `events` with JSONB
/// payloads, promote `payload->>'category'` (the same path the benchmark's
/// JSONB suite auto-promotes before the DML-pipeline suite runs), force the
/// slice cold (the flush backfills the `__promoted$payload$category` shadow
/// column into the file, so the registered DataFusion schema is wider than
/// the catalog schema), then run
/// `INSERT INTO events_copy SELECT * FROM events WHERE id < 10000` into a
/// column-for-column identical scratch table and verify every column —
/// including the JSONB payload bytes and NULL payloads — round-trips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_star_round_trips_jsonb_events() {
    const ROWS: i64 = 1000;
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Exact benchmark `events` DDL (compare_postgres_common seed).
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL, \
            payload JSONB)",
    )
    .await
    .unwrap();

    // 1k rows, benchmark-shaped values; every 100th payload NULL so the
    // nullable JSONB column's null slots round-trip through the copy too.
    let mut stmt = String::with_capacity(ROWS as usize * 240);
    stmt.push_str("INSERT INTO events VALUES ");
    for i in 0..ROWS {
        if i > 0 {
            stmt.push(',');
        }
        let payload = if i % 100 == 99 {
            "NULL".to_string()
        } else {
            format!("'{}'::jsonb", payload_for(i))
        };
        stmt.push_str(&format!(
            "({i}, {}, {}, '{}', {}, {payload})",
            i % 100,
            i as f64 * 0.5,
            status_for(i),
            1_700_000_000 + i
        ));
    }
    sess.execute(&stmt).await.unwrap();

    // Reproduce the registered-schema state the benchmark card is in when
    // shape #100 runs: the JSONB suite's repeated `payload->>'category'`
    // queries auto-promote the path (AUTO_PROMOTE_MIN_HITS = 3) long before
    // the DML-pipeline suite (which runs LAST). Promote it deterministically
    // here, exactly like the promoted-column tests do.
    let events = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &events, "payload", "category")
        .await
        .unwrap();

    // Force the seeded slice cold. The flush backfills the
    // `__promoted$payload$category` shadow column into the cold file, so the
    // DataFusion-registered `events` schema now carries 7 columns while the
    // catalog schema (and `events_copy`) carry 6 — the exact state in which
    // the wildcard source used to fail with "source has 7 columns, target
    // has 6".
    shard.flush_to_parquet().await.unwrap();

    // Exact benchmark scratch DDL + statement (suite #100).
    sess.execute(
        "CREATE TABLE events_copy (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL, \
            payload JSONB)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO events_copy SELECT * FROM events WHERE id < 10000")
        .await
        .expect("benchmark shape #100 must succeed against cold JSONB-bearing events");

    // Full round-trip: every column equal, row for row, in id order.
    let src = query(
        &sess,
        "SELECT id, user_id, amount, status, created_at, payload FROM events ORDER BY id",
    )
    .await;
    let dst = query(
        &sess,
        "SELECT id, user_id, amount, status, created_at, payload FROM events_copy ORDER BY id",
    )
    .await;

    let ids = col_i64(&dst, "id");
    assert_eq!(
        ids.len(),
        ROWS as usize,
        "events_copy must hold all 1k rows"
    );
    assert_eq!(col_i64(&src, "id"), ids, "id mismatch");
    assert_eq!(
        col_i64(&src, "user_id"),
        col_i64(&dst, "user_id"),
        "user_id mismatch"
    );
    assert_eq!(
        col_f64(&src, "amount"),
        col_f64(&dst, "amount"),
        "amount mismatch"
    );
    assert_eq!(
        col_str(&src, "status"),
        col_str(&dst, "status"),
        "status mismatch"
    );
    assert_eq!(
        col_i64(&src, "created_at"),
        col_i64(&dst, "created_at"),
        "created_at mismatch"
    );
    let src_payload = col_bytes(&src, "payload");
    let dst_payload = col_bytes(&dst, "payload");
    assert_eq!(src_payload, dst_payload, "JSONB payload bytes mismatch");
    assert_eq!(
        dst_payload.iter().filter(|p| p.is_none()).count(),
        (ROWS / 100) as usize,
        "NULL payloads must survive the copy"
    );

    // The copied bytes are real JSONB, not opaque blobs: a jsonb operator
    // over the copy agrees with the source.
    let src_signup = col_i64(
        &query(
            &sess,
            "SELECT COUNT(*) AS c FROM events WHERE payload->>'category' = 'signup'",
        )
        .await,
        "c",
    );
    let dst_signup = col_i64(
        &query(
            &sess,
            "SELECT COUNT(*) AS c FROM events_copy WHERE payload->>'category' = 'signup'",
        )
        .await,
        "c",
    );
    assert_eq!(src_signup, dst_signup, "jsonb ->> over the copy must agree");
    assert!(
        src_signup[0].unwrap_or(0) > 0,
        "seed must contain signup payloads for the operator check to be meaningful"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
