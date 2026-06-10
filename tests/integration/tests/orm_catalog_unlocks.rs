//! End-to-end ORM catalog introspection coverage (pg_index / pg_sequence /
//! pg_enum completions).
//!
//! These exercise the catalog completions that unblock SQLAlchemy / Drizzle
//! style schema reflection against Basin's `pg_catalog`:
//!
//!  1. `pg_index` now emits one row per index (the implicit `<table>_pkey`
//!     plus each `CREATE INDEX`), with `indexrelid` matching a `pg_class`
//!     relkind='i' row so the standard `pg_index JOIN pg_class ON
//!     indexrelid = oid` resolves. `indisunique` / `indisprimary` are correct.
//!  2. `pg_sequence` emits one row per `CREATE SEQUENCE` with PG column
//!     semantics, plus a matching `pg_class` relkind='S' row.
//!  3. `pg_type` emits a `typtype='e'` row per `CREATE TYPE ... AS ENUM` and
//!     `pg_enum` lists its labels in sort order.
//!
//! Engine sessions only — no pgwire needed.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int16Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute [{sql}]: {e}"));
}

/// Collect a query's results into one flat list of rows. Each row is a Vec of
/// per-column string renderings so a test can assert on values regardless of
/// the underlying Arrow type. NULLs render as the literal `"<null>"`.
async fn rows(sess: &ProjectSession, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut headers: Vec<String> = Vec::new();
            let mut out: Vec<Vec<String>> = Vec::new();
            for b in &batches {
                if headers.is_empty() {
                    headers = b
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                }
                for r in 0..b.num_rows() {
                    let mut row: Vec<String> = Vec::with_capacity(b.num_columns());
                    for c in 0..b.num_columns() {
                        row.push(render_cell(b.column(c), r));
                    }
                    out.push(row);
                }
            }
            (headers, out)
        }
        Ok(other) => panic!("non-rows result for [{sql}]: {other:?}"),
        Err(e) => panic!("execute [{sql}]: {e}"),
    }
}

fn render_cell(col: &dyn Array, r: usize) -> String {
    if col.is_null(r) {
        return "<null>".to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return a.value(r).to_string();
    }
    "<unknown>".to_string()
}

/// Index of a named column in the header list.
fn col(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .position(|h| h == name)
        .unwrap_or_else(|| panic!("column {name:?} not in {headers:?}"))
}

// ---------------------------------------------------------------------------
// 1. pg_index — PK index + single-col index + unique index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_index_emits_pk_and_user_indexes() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT, tenant BIGINT)").await;
    exec(&sess, "CREATE INDEX users_tenant_idx ON users(tenant)").await;
    exec(&sess, "CREATE UNIQUE INDEX users_email_uq ON users(email)").await;

    let (h, data) = rows(
        &sess,
        "SELECT indexrelid, indrelid, indnatts, indisunique, indisprimary, indisvalid, indkey \
         FROM pg_catalog.pg_index ORDER BY indisprimary DESC, indisunique DESC",
    )
    .await;

    assert_eq!(data.len(), 3, "expected pkey + 2 user indexes, got {data:?}");

    // Build a name->row lookup by joining indexrelid against pg_class below;
    // first verify the three expected shapes are present by their flags.
    let unique_i = col(&h, "indisunique");
    let primary_i = col(&h, "indisprimary");
    let valid_i = col(&h, "indisvalid");
    let natts_i = col(&h, "indnatts");

    // Every row must be valid.
    for row in &data {
        assert_eq!(row[valid_i], "true", "indisvalid must be true: {row:?}");
        assert_eq!(row[natts_i], "1", "all indexes are single-column here: {row:?}");
    }

    // Exactly one primary, and it is unique.
    let primaries: Vec<&Vec<String>> = data.iter().filter(|r| r[primary_i] == "true").collect();
    assert_eq!(primaries.len(), 1, "exactly one PK index");
    assert_eq!(primaries[0][unique_i], "true", "PK index is unique");

    // Two unique indexes total (PK + the explicit UNIQUE index).
    let uniques = data.iter().filter(|r| r[unique_i] == "true").count();
    assert_eq!(uniques, 2, "PK + unique index both report indisunique");

    // The non-unique tenant index must be present.
    let non_unique = data.iter().filter(|r| r[unique_i] == "false").count();
    assert_eq!(non_unique, 1, "the plain tenant index is non-unique");
}

#[tokio::test]
async fn pg_index_join_pg_class_resolves_names() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)").await;
    exec(&sess, "CREATE UNIQUE INDEX users_email_uq ON users(email)").await;

    // The classic ORM introspection join: index relation name + parent table
    // name resolved by joining pg_index against pg_class twice.
    let (h, data) = rows(
        &sess,
        "SELECT ic.relname AS index_name, tc.relname AS table_name, \
                i.indisunique, i.indisprimary \
         FROM pg_catalog.pg_index i \
         JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid \
         JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid \
         ORDER BY i.indisprimary DESC",
    )
    .await;

    assert_eq!(data.len(), 2, "pkey + unique index: {data:?}");
    let iname = col(&h, "index_name");
    let tname = col(&h, "table_name");
    let prim = col(&h, "indisprimary");
    let uniq = col(&h, "indisunique");

    // First row is the PK index (ordered indisprimary DESC).
    assert_eq!(data[0][iname], "users_pkey");
    assert_eq!(data[0][tname], "users");
    assert_eq!(data[0][prim], "true");
    assert_eq!(data[0][uniq], "true");

    // Second is the user unique index.
    assert_eq!(data[1][iname], "users_email_uq");
    assert_eq!(data[1][tname], "users");
    assert_eq!(data[1][prim], "false");
    assert_eq!(data[1][uniq], "true");
}

#[tokio::test]
async fn pg_index_join_pg_attribute_resolves_columns() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT)").await;

    // indkey carries the column attnum; pg_attribute resolves attnum->name.
    // The full SQLAlchemy-shaped chain: pg_index -> pg_class(index) ->
    // pg_class(table) -> pg_attribute.
    let (h, data) = rows(
        &sess,
        "SELECT ic.relname AS index_name, a.attname AS column_name \
         FROM pg_catalog.pg_index i \
         JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid \
         JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid \
         JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid \
              AND CAST(a.attnum AS BIGINT) = CAST(i.indkey AS BIGINT) \
         WHERE i.indisprimary = true",
    )
    .await;

    assert_eq!(data.len(), 1, "PK index keys on exactly one column: {data:?}");
    assert_eq!(data[0][col(&h, "index_name")], "t_pkey");
    assert_eq!(data[0][col(&h, "column_name")], "id");
}

// ---------------------------------------------------------------------------
// 2. pg_sequence — values + matching pg_class 'S' row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_sequence_row_values_and_pg_class_row() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE SEQUENCE order_id_seq START WITH 100 INCREMENT BY 5 \
         MINVALUE 10 MAXVALUE 1000 CACHE 7 CYCLE",
    )
    .await;

    let (h, data) = rows(
        &sess,
        "SELECT seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle, seqtypid \
         FROM pg_catalog.pg_sequence",
    )
    .await;
    assert_eq!(data.len(), 1, "one sequence row: {data:?}");
    let row = &data[0];
    assert_eq!(row[col(&h, "seqstart")], "100");
    assert_eq!(row[col(&h, "seqincrement")], "5");
    assert_eq!(row[col(&h, "seqmin")], "10");
    assert_eq!(row[col(&h, "seqmax")], "1000");
    assert_eq!(row[col(&h, "seqcache")], "7");
    assert_eq!(row[col(&h, "seqcycle")], "true");
    assert_eq!(row[col(&h, "seqtypid")], "20", "int8 element type");

    // pg_class must carry a relkind='S' row whose oid joins to seqrelid.
    let (h2, data2) = rows(
        &sess,
        "SELECT c.relname, c.relkind \
         FROM pg_catalog.pg_sequence s \
         JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid",
    )
    .await;
    assert_eq!(data2.len(), 1, "seqrelid must join to a pg_class row");
    assert_eq!(data2[0][col(&h2, "relname")], "order_id_seq");
    assert_eq!(data2[0][col(&h2, "relkind")], "S");
}

// ---------------------------------------------------------------------------
// 3. pg_type enum + pg_enum labels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_type_and_pg_enum_for_user_enum() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").await;

    // pg_type must carry a typtype='e' / typcategory='E' row for `mood`.
    let (h, data) = rows(
        &sess,
        "SELECT typname, typtype, typcategory FROM pg_catalog.pg_type \
         WHERE typname = 'mood'",
    )
    .await;
    assert_eq!(data.len(), 1, "exactly one enum type row: {data:?}");
    assert_eq!(data[0][col(&h, "typtype")], "e");
    assert_eq!(data[0][col(&h, "typcategory")], "E");

    // pg_enum labels in sort order, joined against pg_type by oid.
    let (h2, data2) = rows(
        &sess,
        "SELECT e.enumlabel, e.enumsortorder \
         FROM pg_catalog.pg_enum e \
         JOIN pg_catalog.pg_type t ON t.oid = e.enumtypid \
         WHERE t.typname = 'mood' \
         ORDER BY e.enumsortorder",
    )
    .await;
    let labels: Vec<&str> = data2
        .iter()
        .map(|r| r[col(&h2, "enumlabel")].as_str())
        .collect();
    assert_eq!(labels, vec!["sad", "ok", "happy"], "labels in declaration order");

    // Sort order is 1-based ascending.
    assert_eq!(data2[0][col(&h2, "enumsortorder")], "1");
    assert_eq!(data2[1][col(&h2, "enumsortorder")], "2");
    assert_eq!(data2[2][col(&h2, "enumsortorder")], "3");
}
