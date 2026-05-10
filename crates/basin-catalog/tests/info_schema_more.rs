//! Catalog-side integration tests for the Phase 5.11.M Tier 3
//! follow-up views: `pg_catalog.pg_index`, `pg_catalog.pg_constraint`,
//! `information_schema.views`, and `information_schema.schemata`.
//!
//! Engine-side SELECT routing is exercised in
//! `crates/basin-engine/tests/info_schema_more_routing.rs`. This file
//! pins the catalog-API surface against the in-memory backend so changes
//! to the rust-side `InfoSchemaQuery::*` shape break here first, before
//! cascading through DataFusion glue.

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog, CvDef, InMemoryCatalog};
use basin_common::{TableName, TenantId};

fn name(s: &str) -> TableName {
    TableName::new(s).unwrap()
}

fn col_str<'a>(b: &'a RecordBatch, n: &str) -> &'a StringArray {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

#[tokio::test]
async fn pg_index_returns_empty_for_v01() {
    // Basin v0.1 has no user-defined index surface (Phase 5.7 B1
    // queued). The view exists for compatibility but always reports
    // zero rows.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    cat.create_table(&t, &name("orders"), &schema)
        .await
        .unwrap();
    cat.create_table(&t, &name("widgets"), &schema)
        .await
        .unwrap();

    let batch = InfoSchemaQuery::pg_index(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
    // Schema sanity: column names are PG-flavoured.
    let s = batch.schema();
    let names: Vec<String> = s.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        names,
        vec![
            "indexrelid".to_string(),
            "indrelid".to_string(),
            "indnatts".to_string(),
            "indisunique".to_string(),
            "indisprimary".to_string(),
            "indkey".to_string(),
        ]
    );
}

#[tokio::test]
async fn pg_constraint_lists_not_null_rows() {
    // Pin the shape that pg_constraint emits one row per constraint
    // surface. With one NOT NULL column on the table and no PK / FK /
    // CHECK declared, the table contributes exactly one `contype='n'`
    // row. Full PK / FK / CHECK coverage lives in the engine-side
    // `constraints` test.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]);
    cat.create_table(&t, &name("orders"), &schema)
        .await
        .unwrap();

    let batch = InfoSchemaQuery::pg_constraint(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1, "expected one NOT NULL row for `id`");
    let s = batch.schema();
    let names: Vec<String> = s.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        names,
        vec![
            "oid".to_string(),
            "conname".to_string(),
            "connamespace".to_string(),
            "contype".to_string(),
            "conrelid".to_string(),
            "conkey".to_string(),
            "confrelid".to_string(),
            "confkey".to_string(),
        ]
    );
}

#[tokio::test]
async fn views_lists_materialized_views() {
    // Catalog-level test: register a continuous matview directly via
    // `set_continuous_aggregate` on a freshly-created table, then
    // assert it shows up in `information_schema.views`. The engine-side
    // routing test exercises the full `CREATE MATERIALIZED VIEW ... WITH
    // (basin.continuous, ...)` path.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ]);
    // The CV target table mirrors the source query's projection.
    cat.create_table(&t, &name("daily_totals"), &schema)
        .await
        .unwrap();
    // A non-CV table should not appear.
    cat.create_table(&t, &name("plain_table"), &schema)
        .await
        .unwrap();

    let cv = CvDef {
        source_table: "events".to_string(),
        query_sql: "SELECT bucket, sum(n) AS total FROM events GROUP BY bucket".to_string(),
        refresh_interval_secs: 60,
        last_refreshed_at_unix_ms: None,
        last_bucket_max_unix_ms: None,
    };
    cat.set_continuous_aggregate(&t, &name("daily_totals"), Some(cv))
        .await
        .unwrap();

    let batch = InfoSchemaQuery::views(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1, "exactly one matview should appear");
    assert_eq!(col_str(&batch, "table_catalog").value(0), "basin");
    assert_eq!(col_str(&batch, "table_schema").value(0), "public");
    assert_eq!(col_str(&batch, "table_name").value(0), "daily_totals");
    assert!(
        col_str(&batch, "view_definition")
            .value(0)
            .contains("FROM events"),
        "view_definition should expose the stored query body, got: {:?}",
        col_str(&batch, "view_definition").value(0)
    );
    assert_eq!(col_str(&batch, "check_option").value(0), "NONE");
    assert_eq!(col_str(&batch, "is_updatable").value(0), "NO");
    assert_eq!(col_str(&batch, "is_insertable_into").value(0), "NO");
}

#[tokio::test]
async fn views_excludes_plain_tables() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![Field::new("c", DataType::Int64, false)]);
    cat.create_table(&t, &name("only_table"), &schema)
        .await
        .unwrap();

    let batch = InfoSchemaQuery::views(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn schemata_one_row_per_tenant() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::schemata(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(&batch, "catalog_name").value(0), "basin");
    assert_eq!(col_str(&batch, "schema_name").value(0), "public");
    assert_eq!(col_str(&batch, "schema_owner").value(0), "");
    // Character-set columns are nullable; v0.1 reports NULL.
    let cs_cat = batch
        .column(
            batch
                .schema()
                .index_of("default_character_set_catalog")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let cs_schema = batch
        .column(
            batch
                .schema()
                .index_of("default_character_set_schema")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let cs_name = batch
        .column(
            batch
                .schema()
                .index_of("default_character_set_name")
                .unwrap(),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(cs_cat.is_null(0));
    assert!(cs_schema.is_null(0));
    assert!(cs_name.is_null(0));
}

#[tokio::test]
async fn cross_tenant_isolation_views() {
    // Tenant A registers a continuous matview; tenant B must not see
    // it via `information_schema.views`. Same P0 invariant the rest of
    // the catalog views enforce.
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let schema = Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ]);
    cat.create_table(&a, &name("a_mv"), &schema).await.unwrap();
    cat.set_continuous_aggregate(
        &a,
        &name("a_mv"),
        Some(CvDef {
            source_table: "a_src".to_string(),
            query_sql: "SELECT bucket, sum(n) AS total FROM a_src GROUP BY bucket".to_string(),
            refresh_interval_secs: 60,
            last_refreshed_at_unix_ms: None,
            last_bucket_max_unix_ms: None,
        }),
    )
    .await
    .unwrap();

    // Tenant B has nothing.
    let ba = InfoSchemaQuery::views(&cat, &a).await.unwrap();
    let bb = InfoSchemaQuery::views(&cat, &b).await.unwrap();
    let names_a: Vec<&str> = (0..ba.num_rows())
        .map(|i| col_str(&ba, "table_name").value(i))
        .collect();
    let names_b: Vec<&str> = (0..bb.num_rows())
        .map(|i| col_str(&bb, "table_name").value(i))
        .collect();
    assert_eq!(names_a, vec!["a_mv"]);
    assert!(
        names_b.is_empty(),
        "tenant B must not see tenant A's matview, got: {names_b:?}"
    );
}
