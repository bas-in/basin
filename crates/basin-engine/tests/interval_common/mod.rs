//! Shared harness for the INTERVAL storage round-trip tests.
//!
//! Lives in a subdirectory so Cargo does not compile it as its own test
//! binary. Two binaries include it: `interval_storage_round_trip.rs` (the
//! DataFusion read path) and `interval_storage_round_trip_owned.rs` (the same
//! trip with `BASIN_OWNED_ENGINE=1`). They are separate binaries precisely
//! because that env var is process-global — setting it in one test would
//! silently change the configuration another test believes it is measuring.
//!
//! ## What is being proven
//!
//! Neither storage format can encode Arrow's `Interval(MonthDayNano)`:
//!
//!   vortex 0.71:    "Array encoding not implemented for Arrow data type
//!                    Interval(MonthDayNano)"
//!   parquet 58.4.0: "Attempting to write an Arrow interval type MonthDayNano
//!                    to parquet" (and NYI on read)
//!
//! So basin-storage stores interval columns as `LargeBinary`, 16 bytes per
//! row — `months(i32) | days(i32) | nanos(i64)`, little-endian — marked
//! `BASIN_TYPE=INTERVAL`. These tests are the proof that the disguise is
//! lossless FIELD BY FIELD, not merely that the write stopped erroring.
//!
//! ## Why the 1-mon-vs-30-days case is the centre of this file
//!
//! An earlier proposal was to store intervals as a single Int64 of
//! microseconds and carry months/days in the `BASIN_TYPE` sidecar. That is
//! unsound twice over: `BASIN_TYPE` is per-COLUMN static metadata, so it
//! cannot carry a per-ROW month count, and i64 cannot hold
//! months(i32)+days(i32)+nanos(i64) = 128 bits anyway. It would have
//! silently collapsed `INTERVAL '1 mon'` and `INTERVAL '30 days'` into the
//! same stored value.
//!
//! PostgreSQL keeps them apart. Verified live against PG 18.2:
//!
//! ```text
//! SELECT INTERVAL '1 mon', INTERVAL '30 days';    -- 1 mon | 30 days
//! SELECT INTERVAL '1 mon' - INTERVAL '30 days';   -- 1 mon -30 days
//! SELECT extract(day from INTERVAL '1 mon');      -- 0
//! SELECT extract(month from INTERVAL '30 days');  -- 0
//! ```
//!
//! (`INTERVAL '1 mon' = INTERVAL '30 days'` is `t` — PG's equality operator
//! normalises a month to 30 days — which is exactly why equality is NOT the
//! test here. The stored fields are.)
//!
//! Every expected value below was read off that live server before being
//! written down.

use std::sync::Arc;

use arrow_array::{Array, IntervalMonthDayNanoArray, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_over(dir: &TempDir, catalog: Arc<dyn basin_catalog::Catalog>) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed [{sql}]: {e}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows for [{sql}], got {other:?}"),
    }
}

/// Every regular file under `root` with the given extension.
fn files_with_ext(root: &std::path::Path, ext: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(ext))
            {
                out.push(p.to_string_lossy().into_owned());
            }
        }
    }
    out
}

/// Flatten `(id, iv)` batches into `(id, Option<(months, days, nanos)>)`,
/// sorted by id. Reading the three fields SEPARATELY is the whole point: a
/// single scalar could not tell `1 mon` from `30 days`.
fn collect_intervals(batches: &[RecordBatch]) -> Vec<(i64, Option<(i32, i32, i64)>)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("id is Int64");
        let ivs = b.column_by_name("iv").expect("iv column");
        assert_eq!(
            ivs.data_type(),
            &arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            "iv must come back as the LOGICAL interval type, not the stored \
             LargeBinary disguise"
        );
        let ivs = ivs
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("iv is IntervalMonthDayNano");
        for r in 0..b.num_rows() {
            let v = if ivs.is_null(r) {
                None
            } else {
                let v = ivs.value(r);
                Some((v.months, v.days, v.nanoseconds))
            };
            out.push((ids.value(r), v));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    out
}

const NANOS_PER_HOUR: i64 = 3_600_000_000_000;

/// The six rows, and the `(months, days, nanos)` each MUST decode to. The
/// trailing comment on each line is the text `psql` prints for that literal
/// on PG 18.2.
fn expected() -> Vec<(i64, Option<(i32, i32, i64)>)> {
    vec![
        (1, Some((1, 0, 0))),                  // 1 mon
        (2, Some((0, 30, 0))),                 // 30 days
        (3, Some((0, -3, 0))),                 // -3 days
        (4, Some((0, 0, 0))),                  // 00:00:00
        (5, None),                             // NULL
        (6, Some((0, 1, 2 * NANOS_PER_HOUR))), // 1 day 02:00:00
    ]
}

/// Write six intervals with a fresh engine, drop it, then read them back
/// through a SECOND engine over the same directory. The second engine shares
/// only the catalog (schema) and the on-disk files — every in-process cache,
/// hot buffer and page cache is new — so the values it returns can only have
/// come off disk.
pub async fn round_trip_through_disk(with_clause: &str, ext: &str) {
    let dir = TempDir::new().unwrap();
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();

    {
        let eng = engine_over(&dir, catalog.clone());
        let sess = eng.open_session(project).await.unwrap();
        exec_ok(
            &sess,
            &format!("CREATE TABLE ivt (id BIGINT NOT NULL, iv INTERVAL){with_clause}"),
        )
        .await;
        exec_ok(
            &sess,
            "INSERT INTO ivt VALUES \
             (1, INTERVAL '1 mon'), \
             (2, INTERVAL '30 days'), \
             (3, INTERVAL '-3 days'), \
             (4, INTERVAL '0'), \
             (5, NULL), \
             (6, INTERVAL '1 day 2 hours')",
        )
        .await;
    }

    let on_disk = files_with_ext(dir.path(), ext);
    assert!(
        !on_disk.is_empty(),
        "no {ext} file was written — the interval column never reached disk"
    );

    let eng2 = engine_over(&dir, catalog.clone());
    let sess2 = eng2.open_session(project).await.unwrap();
    let got = collect_intervals(&rows(&sess2, "SELECT id, iv FROM ivt ORDER BY id").await);

    assert_eq!(
        got,
        expected(),
        "interval fields did not survive the {ext} round-trip"
    );

    // The corruption the rejected Int64-microseconds route would have caused,
    // pinned explicitly rather than left implicit in the vec comparison above.
    let one_month = got[0].1.expect("row 1 is not NULL");
    let thirty_days = got[1].1.expect("row 2 is not NULL");
    assert_ne!(
        one_month, thirty_days,
        "INTERVAL '1 mon' and INTERVAL '30 days' came back identical — the \
         months/days split was lost in storage"
    );
    assert_eq!(one_month, (1, 0, 0), "1 mon must be months=1, days=0");
    assert_eq!(thirty_days, (0, 30, 0), "30 days must be months=0, days=30");
}
