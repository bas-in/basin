//! Bare `TIMESTAMP` (without time zone) acceptance tests.
//!
//! Earlier PoC drops rejected `TIMESTAMP` at DDL parse time and forced
//! callers onto `TIMESTAMPTZ`. PostgREST/pgAdmin compat work surfaced this
//! as a real port-blocker — the wider downstream stack (router OID 1114,
//! info_schema "timestamp without time zone", convert.rs Arrow bridge) was
//! already wired for the no-tz variant; only the DDL gate was held shut.
//!
//! This file pins the gate-open behaviour. It also pins that TIMESTAMP and
//! TIMESTAMPTZ remain DISTINCT types end-to-end (different `atttypid`) so a
//! customer porting from PG doesn't silently end up with a different
//! physical column type from what they declared. The pgwire/OID
//! round-trip test lives in `tests/integration/tests/timestamp_no_tz_pgwire.rs`
//! because the engine crate doesn't depend on basin-router or
//! tokio-postgres.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
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

fn col_string(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
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

fn col_ts_micros(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

#[tokio::test]
async fn create_table_timestamp_without_tz_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, ts TIMESTAMP)")
        .await
        .expect("bare TIMESTAMP must be accepted in DDL");
}

#[tokio::test]
async fn insert_timestamp_without_tz_round_trips() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, '2026-01-15T12:34:56Z')")
        .await
        .unwrap();

    let batches = rows(&sess, "SELECT id, ts FROM t WHERE id = 1").await;
    assert_eq!(col_i64(&batches, "id"), vec![1]);
    let micros = col_ts_micros(&batches, "ts");
    let expected = chrono::DateTime::parse_from_rfc3339("2026-01-15T12:34:56Z")
        .unwrap()
        .timestamp_micros();
    assert_eq!(micros, vec![expected]);
}

#[tokio::test]
async fn timestamp_without_tz_in_pg_type_view() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let batches = rows(
        &sess,
        "SELECT typname FROM pg_catalog.pg_type WHERE typname = 'timestamp'",
    )
    .await;
    let names = col_string(&batches, "typname");
    assert_eq!(
        names,
        vec!["timestamp".to_string()],
        "pg_type must expose 'timestamp' as a row alongside 'timestamptz'"
    );
}

#[tokio::test]
async fn timestamp_without_tz_in_information_schema_columns() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name, data_type, udt_name \
         FROM information_schema.columns \
         WHERE table_name = 'events' AND column_name = 'ts'",
    )
    .await;
    let dtypes = col_string(&batches, "data_type");
    let udts = col_string(&batches, "udt_name");
    assert_eq!(dtypes, vec!["timestamp without time zone".to_string()]);
    assert_eq!(udts, vec!["timestamp".to_string()]);
}

#[tokio::test]
async fn timestamp_and_timestamptz_distinct_types() {
    // Both columns ride on `Timestamp(Microsecond, _)` at the Arrow level
    // but their `atttypid` (and the OID basin-router advertises) MUST differ:
    // 1114 for TIMESTAMP, 1184 for TIMESTAMPTZ. A customer porting from PG
    // depends on this distinction (e.g. `WHERE atttypid = 1114` filters).
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE both (\
            id BIGINT NOT NULL, \
            naive TIMESTAMP NOT NULL, \
            zoned TIMESTAMPTZ NOT NULL\
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT attname, atttypid FROM pg_catalog.pg_attribute \
         WHERE attrelid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'both') \
         ORDER BY attnum",
    )
    .await;
    let names = col_string(&batches, "attname");
    let oids = col_i64(&batches, "atttypid");
    let pairs: Vec<(String, i64)> = names.into_iter().zip(oids.into_iter()).collect();
    let expected: Vec<(&str, i64)> = vec![("id", 20), ("naive", 1114), ("zoned", 1184)];
    let got: Vec<(&str, i64)> = pairs.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    assert_eq!(
        got, expected,
        "TIMESTAMP and TIMESTAMPTZ must surface as distinct OIDs (1114 vs 1184)"
    );
}
