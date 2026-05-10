//! Catalog-side integration tests for the Phase 5.11.M expansion views:
//! `information_schema.columns`, `pg_catalog.pg_attribute`, and
//! `pg_catalog.pg_namespace`.
//!
//! Engine-side SELECT routing is exercised in
//! `crates/basin-engine/tests/info_schema_columns_routing.rs`. This file
//! pins the catalog-API surface against the in-memory backend so changes
//! to the rust-side `InfoSchemaQuery::*` shape break here first, before
//! cascading through DataFusion glue.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog, InMemoryCatalog};
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

fn col_i16<'a>(b: &'a RecordBatch, n: &str) -> &'a Int16Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int16Array>().unwrap()
}

fn col_i32<'a>(b: &'a RecordBatch, n: &str) -> &'a Int32Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int32Array>().unwrap()
}

fn col_i64<'a>(b: &'a RecordBatch, n: &str) -> &'a Int64Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap()
}

fn col_bool<'a>(b: &'a RecordBatch, n: &str) -> &'a BooleanArray {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
}

/// Build a `Schema` that exercises the type-mapping table: int4/int8/text/
/// boolean/timestamptz, plus JSONB-tagged LargeBinary and UUID-tagged
/// FixedSizeBinary(16). Mirrors what `basin-engine::ddl::schema_from_columns`
/// produces.
fn rich_schema() -> Schema {
    let mut jsonb_meta = HashMap::new();
    jsonb_meta.insert("BASIN_TYPE".to_string(), "JSONB".to_string());
    let mut uuid_meta = HashMap::new();
    uuid_meta.insert("BASIN_TYPE".to_string(), "UUID".to_string());
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::Int32, true),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("body", DataType::LargeBinary, true).with_metadata(jsonb_meta),
        Field::new("rid", DataType::FixedSizeBinary(16), false).with_metadata(uuid_meta),
    ])
}

#[tokio::test]
async fn columns_view_emits_one_row_per_column() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = Arc::new(rich_schema());
    cat.create_table(&t, &name("orders"), s.as_ref())
        .await
        .unwrap();

    let batch = InfoSchemaQuery::columns(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 6);
    let col_names: Vec<&str> = (0..batch.num_rows())
        .map(|i| col_str(&batch, "column_name").value(i))
        .collect();
    assert_eq!(col_names, vec!["id", "count", "label", "ts", "body", "rid"]);
    let ords: Vec<i32> = (0..batch.num_rows())
        .map(|i| col_i32(&batch, "ordinal_position").value(i))
        .collect();
    assert_eq!(ords, vec![1, 2, 3, 4, 5, 6]);
    for i in 0..batch.num_rows() {
        assert_eq!(col_str(&batch, "table_catalog").value(i), "basin");
        assert_eq!(col_str(&batch, "table_schema").value(i), "public");
        assert_eq!(col_str(&batch, "table_name").value(i), "orders");
    }
}

#[tokio::test]
async fn columns_view_pg_type_names() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = Arc::new(rich_schema());
    cat.create_table(&t, &name("t"), s.as_ref()).await.unwrap();

    let batch = InfoSchemaQuery::columns(&cat, &t).await.unwrap();
    let mut by_name: HashMap<String, (String, String)> = HashMap::new();
    for i in 0..batch.num_rows() {
        let n = col_str(&batch, "column_name").value(i).to_string();
        let d = col_str(&batch, "data_type").value(i).to_string();
        let u = col_str(&batch, "udt_name").value(i).to_string();
        by_name.insert(n, (d, u));
    }
    assert_eq!(
        by_name.get("id").unwrap(),
        &("bigint".to_string(), "int8".to_string())
    );
    assert_eq!(
        by_name.get("count").unwrap(),
        &("integer".to_string(), "int4".to_string())
    );
    assert_eq!(
        by_name.get("label").unwrap(),
        &("text".to_string(), "text".to_string())
    );
    assert_eq!(
        by_name.get("ts").unwrap(),
        &(
            "timestamp with time zone".to_string(),
            "timestamptz".to_string()
        )
    );
    assert_eq!(
        by_name.get("body").unwrap(),
        &("jsonb".to_string(), "jsonb".to_string())
    );
    assert_eq!(
        by_name.get("rid").unwrap(),
        &("uuid".to_string(), "uuid".to_string())
    );
}

#[tokio::test]
async fn columns_view_is_nullable_pg_style() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("required", DataType::Int64, false),
        Field::new("optional", DataType::Utf8, true),
    ]);
    cat.create_table(&t, &name("t"), &schema).await.unwrap();

    let batch = InfoSchemaQuery::columns(&cat, &t).await.unwrap();
    let mut by_name: HashMap<String, String> = HashMap::new();
    for i in 0..batch.num_rows() {
        by_name.insert(
            col_str(&batch, "column_name").value(i).to_string(),
            col_str(&batch, "is_nullable").value(i).to_string(),
        );
    }
    assert_eq!(by_name.get("required").unwrap(), "NO");
    assert_eq!(by_name.get("optional").unwrap(), "YES");
}

#[tokio::test]
async fn columns_view_cross_tenant_isolation() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let schema_a = Schema::new(vec![Field::new("a_col", DataType::Int64, false)]);
    let schema_b = Schema::new(vec![Field::new("b_col", DataType::Utf8, false)]);
    cat.create_table(&a, &name("only_a"), &schema_a)
        .await
        .unwrap();
    cat.create_table(&b, &name("only_b"), &schema_b)
        .await
        .unwrap();

    let ba = InfoSchemaQuery::columns(&cat, &a).await.unwrap();
    let bb = InfoSchemaQuery::columns(&cat, &b).await.unwrap();
    let names_a: Vec<&str> = (0..ba.num_rows())
        .map(|i| col_str(&ba, "column_name").value(i))
        .collect();
    let names_b: Vec<&str> = (0..bb.num_rows())
        .map(|i| col_str(&bb, "column_name").value(i))
        .collect();
    assert_eq!(names_a, vec!["a_col"]);
    assert_eq!(names_b, vec!["b_col"]);
    assert!(!names_a.contains(&"b_col"));
    assert!(!names_b.contains(&"a_col"));
}

#[tokio::test]
async fn pg_attribute_emits_one_row_per_column() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("first", DataType::Int64, false),
        Field::new("second", DataType::Utf8, true),
        Field::new("third", DataType::Boolean, false),
    ]);
    cat.create_table(&t, &name("t"), &schema).await.unwrap();

    let batch = InfoSchemaQuery::pg_attribute(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    let names: Vec<&str> = (0..batch.num_rows())
        .map(|i| col_str(&batch, "attname").value(i))
        .collect();
    assert_eq!(names, vec!["first", "second", "third"]);
    let attnums: Vec<i16> = (0..batch.num_rows())
        .map(|i| col_i16(&batch, "attnum").value(i))
        .collect();
    assert_eq!(attnums, vec![1_i16, 2, 3]);
    let notnulls: Vec<bool> = (0..batch.num_rows())
        .map(|i| col_bool(&batch, "attnotnull").value(i))
        .collect();
    assert_eq!(notnulls, vec![true, false, true]);
    let dropped: Vec<bool> = (0..batch.num_rows())
        .map(|i| col_bool(&batch, "attisdropped").value(i))
        .collect();
    assert_eq!(dropped, vec![false, false, false]);
}

#[tokio::test]
async fn pg_attribute_attrelid_matches_pg_class_oid() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]);
    cat.create_table(&t, &name("widgets"), &schema)
        .await
        .unwrap();

    let pg_class = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    assert_eq!(pg_class.num_rows(), 1);
    let class_oid = col_i64(&pg_class, "oid").value(0);

    let pg_attr = InfoSchemaQuery::pg_attribute(&cat, &t).await.unwrap();
    assert_eq!(pg_attr.num_rows(), 2);
    let attrelids: Vec<i64> = (0..pg_attr.num_rows())
        .map(|i| col_i64(&pg_attr, "attrelid").value(i))
        .collect();
    for r in attrelids {
        assert_eq!(r, class_oid, "attrelid must equal the table's pg_class.oid");
    }
}

#[tokio::test]
async fn pg_attribute_atttypid_pg_oids() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = Arc::new(rich_schema());
    cat.create_table(&t, &name("t"), s.as_ref()).await.unwrap();

    let batch = InfoSchemaQuery::pg_attribute(&cat, &t).await.unwrap();
    let mut by_name: HashMap<String, i64> = HashMap::new();
    for i in 0..batch.num_rows() {
        by_name.insert(
            col_str(&batch, "attname").value(i).to_string(),
            col_i64(&batch, "atttypid").value(i),
        );
    }
    assert_eq!(by_name.get("id"), Some(&20)); // int8
    assert_eq!(by_name.get("count"), Some(&23)); // int4
    assert_eq!(by_name.get("label"), Some(&25)); // text
    assert_eq!(by_name.get("ts"), Some(&1184)); // timestamptz
    assert_eq!(by_name.get("body"), Some(&3802)); // jsonb (BASIN_TYPE marker)
    assert_eq!(by_name.get("rid"), Some(&2950)); // uuid (BASIN_TYPE marker)
}

#[tokio::test]
async fn pg_attribute_cross_tenant_isolation() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();
    let schema_a = Schema::new(vec![Field::new("a_col", DataType::Int64, false)]);
    let schema_b = Schema::new(vec![Field::new("b_col", DataType::Utf8, false)]);
    cat.create_table(&a, &name("only_a"), &schema_a)
        .await
        .unwrap();
    cat.create_table(&b, &name("only_b"), &schema_b)
        .await
        .unwrap();

    let ba = InfoSchemaQuery::pg_attribute(&cat, &a).await.unwrap();
    let bb = InfoSchemaQuery::pg_attribute(&cat, &b).await.unwrap();
    let names_a: Vec<&str> = (0..ba.num_rows())
        .map(|i| col_str(&ba, "attname").value(i))
        .collect();
    let names_b: Vec<&str> = (0..bb.num_rows())
        .map(|i| col_str(&bb, "attname").value(i))
        .collect();
    assert_eq!(names_a, vec!["a_col"]);
    assert_eq!(names_b, vec!["b_col"]);
}

#[tokio::test]
async fn pg_namespace_one_row_per_tenant() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::pg_namespace(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(&batch, "nspname").value(0), "public");
    assert!(
        col_i64(&batch, "oid").value(0) > 0,
        "namespace oid must be a positive FNV hash"
    );
    assert_eq!(col_i64(&batch, "nspowner").value(0), 0);
}

#[tokio::test]
async fn pg_namespace_oid_disjoint_per_tenant() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let ba = InfoSchemaQuery::pg_namespace(&cat, &a).await.unwrap();
    let bb = InfoSchemaQuery::pg_namespace(&cat, &b).await.unwrap();
    let oid_a = col_i64(&ba, "oid").value(0);
    let oid_b = col_i64(&bb, "oid").value(0);
    assert_ne!(
        oid_a, oid_b,
        "two tenants must have disjoint namespace oids"
    );
}

#[tokio::test]
async fn pg_namespace_oid_matches_pg_class_relnamespace() {
    // A table's pg_class.relnamespace must equal pg_namespace.oid for the
    // tenant. PostgREST and pgAdmin both join on this.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let schema = Schema::new(vec![Field::new("c", DataType::Int64, false)]);
    cat.create_table(&t, &name("t"), &schema).await.unwrap();

    let class = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    let ns = InfoSchemaQuery::pg_namespace(&cat, &t).await.unwrap();
    let class_ns = col_i64(&class, "relnamespace").value(0);
    let ns_oid = col_i64(&ns, "oid").value(0);
    assert_eq!(
        class_ns, ns_oid,
        "pg_class.relnamespace must match pg_namespace.oid"
    );
}

#[tokio::test]
async fn empty_tenant_returns_empty_columns_and_attribute() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let cols = InfoSchemaQuery::columns(&cat, &t).await.unwrap();
    assert_eq!(cols.num_rows(), 0);
    let attrs = InfoSchemaQuery::pg_attribute(&cat, &t).await.unwrap();
    assert_eq!(attrs.num_rows(), 0);
    // pg_namespace still emits the single `public` row even when no tables
    // exist — the namespace is the tenant boundary, not the table set.
    let ns = InfoSchemaQuery::pg_namespace(&cat, &t).await.unwrap();
    assert_eq!(ns.num_rows(), 1);
}
