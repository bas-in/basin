//! Integration tests for engine-side routing of `pg_catalog.pg_type`
//! (Phase 5.11.M Tier 3).
//!
//! pgAdmin's column-detail query joins `pg_attribute` against `pg_type` to
//! resolve PG type names from `atttypid`. These tests pin both the
//! standalone SELECT path and the JOIN that pgAdmin runs.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
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

async fn rows(sess: &TenantSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn select_pg_type_routes() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    // Filter to numeric types — `int2`, `int4`, `int8`, `float4`,
    // `float8`, `numeric` all carry typcategory='N'.
    let batches = rows(
        &sess,
        "SELECT typname, typcategory FROM pg_catalog.pg_type WHERE typcategory = 'N'",
    )
    .await;
    let names = col_string(&batches, "typname");
    for required in ["int2", "int4", "int8", "float4", "float8", "numeric"] {
        assert!(
            names.iter().any(|n| n == required),
            "numeric pg_type missing {required:?}: {names:?}"
        );
    }
}

#[tokio::test]
async fn pg_type_join_pg_attribute_works() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE accounts (\
             id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             created_at TIMESTAMPTZ NOT NULL\
         )",
    )
    .await
    .unwrap();

    // Replicate pgAdmin's column-detail query: JOIN pg_attribute against
    // pg_type on the type OID. attrelid is filtered to our table via a
    // sub-SELECT against pg_class.
    let batches = rows(
        &sess,
        "SELECT a.attname, t.typname \
         FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
         WHERE a.attrelid = (SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = 'accounts')",
    )
    .await;

    // Build (attname → typname) and assert each seeded column resolves.
    let attnames = col_string(&batches, "attname");
    let typnames = col_string(&batches, "typname");
    assert_eq!(attnames.len(), 3, "accounts has 3 columns");
    let mut by_attname = std::collections::HashMap::new();
    for (a, t) in attnames.iter().zip(typnames.iter()) {
        by_attname.insert(a.clone(), t.clone());
    }
    assert_eq!(by_attname.get("id").map(String::as_str), Some("int8"));
    assert_eq!(by_attname.get("email").map(String::as_str), Some("text"));
    assert_eq!(
        by_attname.get("created_at").map(String::as_str),
        Some("timestamptz")
    );
}

#[tokio::test]
async fn pg_type_oid_stable_across_calls() {
    // A tenant must see the same `pg_type.oid` for `int4` (and friends)
    // across multiple session SELECTs. The static row set guarantees this;
    // the test pins the contract.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(TenantId::new()).await.unwrap();

    let b1 = rows(
        &sess,
        "SELECT oid FROM pg_catalog.pg_type WHERE typname = 'int4'",
    )
    .await;
    let b2 = rows(
        &sess,
        "SELECT oid FROM pg_catalog.pg_type WHERE typname = 'int4'",
    )
    .await;
    let v1 = col_i64(&b1, "oid");
    let v2 = col_i64(&b2, "oid");
    assert_eq!(v1, vec![23]);
    assert_eq!(v2, vec![23]);
}
