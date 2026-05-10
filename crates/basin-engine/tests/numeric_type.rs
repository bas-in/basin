//! `NUMERIC` / `DECIMAL` (Arrow `Decimal128(p, s)`) end-to-end acceptance.
//!
//! Earlier PoC drops rejected `NUMERIC(p, s)` columns at DDL time, leaving
//! customers with a real schema gap (PostgREST schemas with `numeric` price
//! / amount columns hit the gate on first contact). This file pins the
//! end-to-end behaviour:
//!
//! - DDL accepts `NUMERIC`, `NUMERIC(p)`, `NUMERIC(p, s)`, `DECIMAL(...)`.
//! - Out-of-range precision/scale is rejected so a user can't silently end
//!   up with a smaller column than declared.
//! - Numeric literals coerce on INSERT and round-trip on SELECT preserving
//!   the column's `(precision, scale)`.
//! - `information_schema.columns` and `pg_attribute` advertise PG's
//!   `numeric` / OID 1700.
//!
//! pgwire round-trip lives in
//! `tests/integration/tests/numeric_type_pgwire.rs`.

use std::sync::Arc;

use arrow_array::{Array, Decimal128Array, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
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

async fn rows(sess: &TenantSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
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
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

#[tokio::test]
async fn create_table_numeric_basic() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (price NUMERIC(10, 2))")
        .await
        .expect("NUMERIC(10, 2) must be accepted at DDL");
}

#[tokio::test]
async fn create_table_numeric_default() {
    // Bare `NUMERIC` (no parens) is PG's "arbitrary precision". Arrow
    // can only express a fixed precision, so we pin (38, 0) — Arrow's
    // Decimal128 max precision.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (qty NUMERIC)")
        .await
        .expect("bare NUMERIC must be accepted (defaults to Decimal128(38, 0))");

    // Bare `NUMERIC` surfaces under the same `data_type=numeric` label.
    // The exact (38, 0) precision/scale are not exposed in v0.1's
    // `information_schema.columns` view (no `numeric_precision` /
    // `numeric_scale` columns yet), so the type-name assertion is the
    // contract we pin here. INSERT-side coverage validates the (38, 0)
    // shape — see `insert_negative_and_high_precision_numeric`.
    let batches = rows(
        &sess,
        "SELECT data_type FROM information_schema.columns \
         WHERE table_name = 't' AND column_name = 'qty'",
    )
    .await;
    let dt = col_string(&batches, "data_type");
    assert_eq!(dt, vec!["numeric".to_string()]);

    // Round-trip an integer through the bare-NUMERIC column to confirm
    // the (38, 0) default scale: `42` stores as i128 42 with no scaling.
    sess.execute("INSERT INTO t VALUES (42)").await.unwrap();
    let batches = rows(&sess, "SELECT qty FROM t").await;
    let arr = batches[0]
        .column_by_name("qty")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("bare NUMERIC must surface as Decimal128");
    assert_eq!(arr.value(0), 42);
    match arr.data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!(*p, 38, "bare NUMERIC must default to precision 38");
            assert_eq!(*s, 0, "bare NUMERIC must default to scale 0");
        }
        other => panic!("expected Decimal128, got {other:?}"),
    }
}

#[tokio::test]
async fn create_table_numeric_only_precision() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (n NUMERIC(10))")
        .await
        .expect("NUMERIC(p) must default scale to 0");
}

#[tokio::test]
async fn numeric_decimal_synonym() {
    // `DECIMAL(p, s)` is a PG synonym for `NUMERIC(p, s)`; sqlparser surfaces
    // them as separate AST variants but they map to the same Arrow type.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (price DECIMAL(10, 2))")
        .await
        .expect("DECIMAL(10, 2) must be accepted as a NUMERIC synonym");
    sess.execute("INSERT INTO t VALUES (1.50)").await.unwrap();

    let batches = rows(&sess, "SELECT price FROM t").await;
    let arr = batches[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("DECIMAL column must surface as Decimal128 to the caller");
    assert_eq!(arr.len(), 1);
    // 1.50 with scale=2 → integer representation 150.
    assert_eq!(arr.value(0), 150);
    match arr.data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!(*p, 10);
            assert_eq!(*s, 2);
        }
        other => panic!("expected Decimal128(10, 2), got {other:?}"),
    }
}

#[tokio::test]
async fn numeric_precision_out_of_range_rejected() {
    // Arrow Decimal128 maxes out at precision 38; `NUMERIC(50, 0)` must
    // error at DDL so the caller doesn't end up with a silently-truncated
    // column.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute("CREATE TABLE t (n NUMERIC(50, 0))")
        .await
        .expect_err("NUMERIC(50, 0) must be rejected (Arrow Decimal128 max precision = 38)");
    assert!(
        format!("{err}").to_ascii_lowercase().contains("precision"),
        "expected precision-related error, got: {err}"
    );
}

#[tokio::test]
async fn numeric_scale_exceeds_precision_rejected() {
    // PG accepts scale > precision (it's effectively `NUMERIC(p, s)` with
    // implicit padding); Arrow doesn't. Reject up front.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let err = sess
        .execute("CREATE TABLE t (n NUMERIC(5, 10))")
        .await
        .expect_err("NUMERIC(5, 10) must be rejected (scale > precision)");
    assert!(
        format!("{err}").to_ascii_lowercase().contains("scale"),
        "expected scale-related error, got: {err}"
    );
}

#[tokio::test]
async fn insert_numeric_literal() {
    // INSERT round-trip of a fractional literal preserving the column's
    // declared scale (the integer storage value is `1.50 * 10^2 = 150`).
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, price NUMERIC(10, 2))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 1.50), (2, 99.99), (3, 0)")
        .await
        .unwrap();

    let batches = rows(&sess, "SELECT id, price FROM t ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);
    let arr = batches[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(arr.value(0), 150);
    assert_eq!(arr.value(1), 9_999);
    assert_eq!(arr.value(2), 0);
    // Render side-check: the textual form preserves the column's scale.
    assert_eq!(arr.value_as_string(0), "1.50");
    assert_eq!(arr.value_as_string(1), "99.99");
    assert_eq!(arr.value_as_string(2), "0.00");
}

#[tokio::test]
async fn insert_negative_and_high_precision_numeric() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, n NUMERIC(38, 6))")
        .await
        .unwrap();
    // 38-digit precision with 6 fractional digits ⇒ 32 integer digits.
    sess.execute(
        "INSERT INTO t VALUES \
            (1, -42.000001), \
            (2, 12345678901234567890123456789012.345678)",
    )
    .await
    .unwrap();
    let batches = rows(&sess, "SELECT id, n FROM t ORDER BY id").await;
    let arr = batches[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(arr.value(0), -42_000_001);
    assert_eq!(arr.value_as_string(0), "-42.000001");
    assert_eq!(arr.value_as_string(1), "12345678901234567890123456789012.345678");
}

#[tokio::test]
async fn numeric_in_information_schema_columns() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, price NUMERIC(10, 2))")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name, data_type, udt_name FROM information_schema.columns \
         WHERE table_name = 't' AND column_name = 'price'",
    )
    .await;
    let dtypes = col_string(&batches, "data_type");
    let udts = col_string(&batches, "udt_name");
    assert_eq!(dtypes, vec!["numeric".to_string()]);
    assert_eq!(udts, vec!["numeric".to_string()]);
}

#[tokio::test]
async fn numeric_in_pg_attribute() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, price NUMERIC(10, 2))")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT attname, atttypid FROM pg_catalog.pg_attribute \
         WHERE attrelid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't') \
         ORDER BY attnum",
    )
    .await;
    let names = col_string(&batches, "attname");
    let oids = col_i64(&batches, "atttypid");
    let pairs: Vec<(String, i64)> = names.into_iter().zip(oids.into_iter()).collect();
    let expected: Vec<(&str, i64)> = vec![("id", 20), ("price", 1700)];
    let got: Vec<(&str, i64)> = pairs.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    assert_eq!(
        got, expected,
        "NUMERIC must surface in pg_attribute as OID 1700"
    );
}
