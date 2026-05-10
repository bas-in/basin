//! Integration tests for engine-side routing of the three Phase 5.11.M
//! expansion views: `information_schema.columns`, `pg_catalog.pg_attribute`,
//! and `pg_catalog.pg_namespace`.
//!
//! Each test opens a `TenantSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit. The asserts
//! cover:
//!
//! - One row per (table, column) for the calling tenant.
//! - Postgres-flavoured type names (`"integer"`, `"text"`,
//!   `"timestamp with time zone"`, `"jsonb"`, `"uuid"`) — never raw Arrow
//!   names like `"Int64"` or `"LargeBinary"`.
//! - `is_nullable` reports `"YES"` / `"NO"` (PG style, not bool).
//! - `attnum` is 1-based and `attrelid` matches `pg_class.oid`.
//! - `atttypid` matches what `basin-router` advertises in `RowDescription`.
//! - Cross-tenant isolation: A's columns / pg_attribute rows / namespace
//!   oid are invisible to B.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int16Array, Int64Array, StringArray};
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
            if arr.is_null(i) {
                out.push(String::new());
            } else {
                out.push(arr.value(i).to_string());
            }
        }
    }
    out
}

fn col_string_opt(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(arr.value(i).to_string()));
            }
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

fn col_i16(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i16> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_bool(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<bool> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn rows(sess: &TenantSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_information_schema_columns_basic() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE orders (\
            id BIGINT NOT NULL, \
            label TEXT, \
            shipped BOOLEAN NOT NULL\
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT table_name, column_name, ordinal_position \
         FROM information_schema.columns \
         WHERE table_name = 'orders' \
         ORDER BY ordinal_position",
    )
    .await;
    assert_eq!(total_rows(&batches), 3);
    let names = col_string(&batches, "column_name");
    assert_eq!(
        names,
        vec!["id".to_string(), "label".to_string(), "shipped".to_string()]
    );
    let ords: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column_by_name("ordinal_position")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap();
            (0..arr.len())
                .map(|i| arr.value(i) as i64)
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ords, vec![1, 2, 3]);
}

#[tokio::test]
async fn columns_show_correct_pg_type_names() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE wide (\
            i_small SMALLINT NOT NULL, \
            i_big BIGINT NOT NULL, \
            t TEXT NOT NULL, \
            ts TIMESTAMPTZ NOT NULL, \
            j JSONB NOT NULL, \
            u UUID NOT NULL, \
            b BOOLEAN NOT NULL, \
            blob BYTEA NOT NULL\
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name, data_type, udt_name \
         FROM information_schema.columns \
         WHERE table_name = 'wide' \
         ORDER BY ordinal_position",
    )
    .await;
    let names = col_string(&batches, "column_name");
    let dtypes = col_string(&batches, "data_type");
    let udts = col_string(&batches, "udt_name");
    let pairs: Vec<(String, String, String)> = names
        .into_iter()
        .zip(dtypes.into_iter())
        .zip(udts.into_iter())
        .map(|((n, d), u)| (n, d, u))
        .collect();
    let expected: Vec<(&str, &str, &str)> = vec![
        ("i_small", "smallint", "int2"),
        ("i_big", "bigint", "int8"),
        ("t", "text", "text"),
        ("ts", "timestamp with time zone", "timestamptz"),
        ("j", "jsonb", "jsonb"),
        ("u", "uuid", "uuid"),
        ("b", "boolean", "bool"),
        ("blob", "bytea", "bytea"),
    ];
    let got: Vec<(&str, &str, &str)> = pairs
        .iter()
        .map(|(n, d, u)| (n.as_str(), d.as_str(), u.as_str()))
        .collect();
    assert_eq!(got, expected, "expected PG-style names, got {got:?}");
}

#[tokio::test]
async fn columns_show_nullable_correctly() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE mixed (\
            required BIGINT NOT NULL, \
            optional TEXT\
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name, is_nullable \
         FROM information_schema.columns \
         WHERE table_name = 'mixed' \
         ORDER BY ordinal_position",
    )
    .await;
    let names = col_string(&batches, "column_name");
    let nullable = col_string(&batches, "is_nullable");
    assert_eq!(names, vec!["required".to_string(), "optional".to_string()]);
    assert_eq!(
        nullable,
        vec!["NO".to_string(), "YES".to_string()],
        "PG-style YES/NO, not bool"
    );
}

#[tokio::test]
async fn pg_attribute_attnum_is_one_based() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE t (\
            first_col BIGINT NOT NULL, \
            second_col TEXT NOT NULL, \
            third_col BOOLEAN NOT NULL\
         )",
    )
    .await
    .unwrap();

    // Constrain to this table's pg_class oid so we don't pick up rows for
    // any future system tables.
    let class = rows(
        &sess,
        "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't'",
    )
    .await;
    let table_oid = col_i64(&class, "oid")[0];

    let batches = rows(
        &sess,
        &format!(
            "SELECT attname, attnum, attrelid FROM pg_catalog.pg_attribute \
             WHERE attrelid = {table_oid} ORDER BY attnum"
        ),
    )
    .await;
    let attnames = col_string(&batches, "attname");
    let attnums = col_i16(&batches, "attnum");
    assert_eq!(attnames.len(), 3);
    assert_eq!(
        attnames,
        vec![
            "first_col".to_string(),
            "second_col".to_string(),
            "third_col".to_string()
        ]
    );
    assert_eq!(
        attnums,
        vec![1_i16, 2, 3],
        "attnum must be 1-based (not 0-based as in Arrow)"
    );
    let attrelids = col_i64(&batches, "attrelid");
    for r in attrelids {
        assert_eq!(
            r, table_oid,
            "every attribute row must point at its table's pg_class.oid"
        );
    }
}

#[tokio::test]
async fn pg_attribute_atttypid_matches_router() {
    // For the same Arrow types basin-router maps in `arrow_to_pg_type`,
    // pg_attribute.atttypid should report the same OIDs. This test
    // pins the cross-layer agreement.
    //
    // Reference OIDs: int4=23, int8=20, text=25, timestamptz=1184, bool=16,
    // jsonb=3802, uuid=2950.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE typed (\
            i_big BIGINT NOT NULL, \
            t TEXT NOT NULL, \
            ts TIMESTAMPTZ NOT NULL, \
            b BOOLEAN NOT NULL, \
            j JSONB NOT NULL, \
            u UUID NOT NULL\
         )",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "SELECT attname, atttypid FROM pg_catalog.pg_attribute \
         WHERE attrelid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'typed') \
         ORDER BY attnum",
    )
    .await;
    let names = col_string(&batches, "attname");
    let oids = col_i64(&batches, "atttypid");
    let pairs: Vec<(String, i64)> = names.into_iter().zip(oids.into_iter()).collect();
    let expected: Vec<(&str, i64)> = vec![
        ("i_big", 20),
        ("t", 25),
        ("ts", 1184),
        ("b", 16),
        ("j", 3802),
        ("u", 2950),
    ];
    let got: Vec<(&str, i64)> = pairs.iter().map(|(n, o)| (n.as_str(), *o)).collect();
    assert_eq!(got, expected, "atttypid must match basin-router's PG OIDs");
}

#[tokio::test]
async fn pg_namespace_one_row_per_tenant() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let batches = rows(
        &sess,
        "SELECT oid, nspname, nspowner FROM pg_catalog.pg_namespace",
    )
    .await;
    assert_eq!(total_rows(&batches), 1, "exactly one namespace per tenant");
    let names = col_string(&batches, "nspname");
    assert_eq!(names, vec!["public".to_string()]);
    let oids = col_i64(&batches, "oid");
    assert!(oids[0] > 0, "namespace oid must be a positive FNV hash");
    let owners = col_i64(&batches, "nspowner");
    assert_eq!(owners, vec![0_i64], "v0.1 placeholder owner is 0");
}

#[tokio::test]
async fn cross_tenant_isolation_columns() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    sa.execute("CREATE TABLE only_a (secret_a BIGINT NOT NULL)")
        .await
        .unwrap();
    sb.execute("CREATE TABLE only_b (secret_b TEXT NOT NULL)")
        .await
        .unwrap();

    let cols_a = col_string(
        &rows(
            &sa,
            "SELECT column_name FROM information_schema.columns ORDER BY column_name",
        )
        .await,
        "column_name",
    );
    let cols_b = col_string(
        &rows(
            &sb,
            "SELECT column_name FROM information_schema.columns ORDER BY column_name",
        )
        .await,
        "column_name",
    );
    assert_eq!(cols_a, vec!["secret_a".to_string()]);
    assert_eq!(cols_b, vec!["secret_b".to_string()]);
    assert!(
        !cols_a.contains(&"secret_b".to_string()),
        "tenant A leaked tenant B's column: {cols_a:?}"
    );
    assert!(
        !cols_b.contains(&"secret_a".to_string()),
        "tenant B leaked tenant A's column: {cols_b:?}"
    );
}

#[tokio::test]
async fn cross_tenant_isolation_pg_attribute() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    sa.execute("CREATE TABLE only_a (secret_a BIGINT NOT NULL)")
        .await
        .unwrap();
    sb.execute("CREATE TABLE only_b (secret_b TEXT NOT NULL)")
        .await
        .unwrap();

    let names_a = col_string(
        &rows(
            &sa,
            "SELECT attname FROM pg_catalog.pg_attribute ORDER BY attname",
        )
        .await,
        "attname",
    );
    let names_b = col_string(
        &rows(
            &sb,
            "SELECT attname FROM pg_catalog.pg_attribute ORDER BY attname",
        )
        .await,
        "attname",
    );
    assert_eq!(names_a, vec!["secret_a".to_string()]);
    assert_eq!(names_b, vec!["secret_b".to_string()]);
    assert!(
        !names_a.contains(&"secret_b".to_string()),
        "tenant A leaked tenant B's pg_attribute row: {names_a:?}"
    );
}

#[tokio::test]
async fn cross_tenant_isolation_pg_namespace() {
    // Each tenant's namespace oid hashes (tenant, "public") so two tenants
    // must see distinct values.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();

    let oid_a = col_i64(
        &rows(&sa, "SELECT oid FROM pg_catalog.pg_namespace").await,
        "oid",
    );
    let oid_b = col_i64(
        &rows(&sb, "SELECT oid FROM pg_catalog.pg_namespace").await,
        "oid",
    );
    assert_eq!(oid_a.len(), 1);
    assert_eq!(oid_b.len(), 1);
    assert_ne!(
        oid_a[0], oid_b[0],
        "two tenants must hash to disjoint namespace oids"
    );
}

#[tokio::test]
async fn columns_postgrest_predicate_filters_correctly() {
    // PostgREST issues `WHERE table_schema = 'public' AND table_name = $1`
    // on startup against `information_schema.columns`. Locks in that the
    // DataFusion filter on top of our full-batch scan returns exactly
    // the requested table's columns.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, total BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE other (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'orders' \
         ORDER BY ordinal_position",
    )
    .await;
    let names = col_string(&batches, "column_name");
    assert_eq!(names, vec!["id".to_string(), "total".to_string()]);
}

#[tokio::test]
async fn columns_default_is_null_for_plain_column() {
    // Plain `BIGINT NOT NULL` columns have no DEFAULT in catalog metadata
    // today (5.11.M scope) so column_default must be NULL — clients
    // probing this for "is there a default?" need it to round-trip as
    // SQL NULL, not the empty string.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT column_name, column_default FROM information_schema.columns \
         WHERE table_name = 't'",
    )
    .await;
    assert_eq!(total_rows(&batches), 1);
    let defaults = col_string_opt(&batches, "column_default");
    assert_eq!(defaults, vec![None]);
}

#[tokio::test]
async fn pg_attribute_attisdropped_always_false_v01() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, label TEXT)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT attname, attisdropped FROM pg_catalog.pg_attribute \
         WHERE attrelid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't')",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    let dropped = col_bool(&batches, "attisdropped");
    for d in dropped {
        assert!(!d, "attisdropped is always false in v0.1");
    }
}
