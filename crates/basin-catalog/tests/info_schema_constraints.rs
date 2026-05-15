//! Catalog-side integration tests for the Phase 5.11.M Tier 3 constraint
//! introspection views: `information_schema.table_constraints`,
//! `.key_column_usage`, and `.referential_constraints`.
//!
//! Engine-side SELECT routing lives in
//! `crates/basin-engine/tests/info_schema_constraints_routing.rs`. This
//! file pins the rust-API surface against the in-memory backend.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog, InMemoryCatalog};
use basin_common::{TableName, ProjectId};

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

/// Build an Arrow schema where each `(name, type, nullable)` triple
/// becomes a Field.
fn schema_with(cols: &[(&str, DataType, bool)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t, nullable)| Field::new(*n, t.clone(), *nullable))
            .collect::<Vec<_>>(),
    ))
}

#[tokio::test]
async fn not_null_columns_appear_in_table_constraints() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    let s = schema_with(&[
        ("id", DataType::Int64, false),      // NOT NULL
        ("payload", DataType::Utf8, true),   // nullable
        ("created", DataType::Int64, false), // NOT NULL
    ]);
    cat.create_table(&t, &name("orders"), &s).await.unwrap();

    let batch = InfoSchemaQuery::table_constraints(&cat, &t).await.unwrap();
    assert_eq!(
        batch.num_rows(),
        2,
        "exactly two NOT NULL columns must produce two rows"
    );

    let constraint_names: Vec<&str> = (0..batch.num_rows())
        .map(|i| col_str(&batch, "constraint_name").value(i))
        .collect();
    assert!(constraint_names.contains(&"orders_id_not_null"));
    assert!(constraint_names.contains(&"orders_created_not_null"));

    for i in 0..batch.num_rows() {
        assert_eq!(col_str(&batch, "constraint_catalog").value(i), "basin");
        assert_eq!(col_str(&batch, "constraint_schema").value(i), "public");
        assert_eq!(col_str(&batch, "table_catalog").value(i), "basin");
        assert_eq!(col_str(&batch, "table_schema").value(i), "public");
        assert_eq!(col_str(&batch, "table_name").value(i), "orders");
        assert_eq!(col_str(&batch, "constraint_type").value(i), "NOT NULL");
        assert_eq!(col_str(&batch, "is_deferrable").value(i), "NO");
        assert_eq!(col_str(&batch, "initially_deferred").value(i), "NO");
    }
}

#[tokio::test]
async fn nullable_columns_absent_from_table_constraints() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    // Every column is nullable: zero NOT NULL rows should land.
    let s = schema_with(&[("a", DataType::Int64, true), ("b", DataType::Utf8, true)]);
    cat.create_table(&t, &name("loose"), &s).await.unwrap();

    let batch = InfoSchemaQuery::table_constraints(&cat, &t).await.unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "tables with no NOT NULL columns must contribute no constraint rows"
    );

    // Schema must still be reported correctly even when empty.
    let s = batch.schema();
    assert_eq!(s.field(0).name(), "constraint_catalog");
    assert_eq!(s.field(6).name(), "constraint_type");
}

#[tokio::test]
async fn key_column_usage_empty_v01() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema_with(&[("id", DataType::Int64, false)]);
    cat.create_table(&t, &name("widgets"), &s).await.unwrap();

    let batch = InfoSchemaQuery::key_column_usage(&cat, &t).await.unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "v0.1 ships no PK / UNIQUE / FK; key_column_usage must be empty"
    );
    let s = batch.schema();
    // Pin the column shape so the v0.2 PK expansion can't accidentally
    // drift the contract.
    assert_eq!(s.field(0).name(), "constraint_catalog");
    assert_eq!(s.field(2).name(), "constraint_name");
    assert_eq!(s.field(6).name(), "column_name");
    assert_eq!(s.field(7).name(), "ordinal_position");
    assert_eq!(s.field(8).name(), "position_in_unique_constraint");
    assert!(s.field(8).is_nullable());
}

#[tokio::test]
async fn referential_constraints_empty_v01() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema_with(&[("id", DataType::Int64, false)]);
    cat.create_table(&t, &name("widgets"), &s).await.unwrap();

    let batch = InfoSchemaQuery::referential_constraints(&cat, &t)
        .await
        .unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "v0.1 has no FOREIGN KEY surface; referential_constraints must be empty"
    );
    let s = batch.schema();
    assert_eq!(s.field(0).name(), "constraint_catalog");
    assert_eq!(s.field(2).name(), "constraint_name");
    assert_eq!(s.field(6).name(), "match_option");
    assert_eq!(s.field(7).name(), "update_rule");
    assert_eq!(s.field(8).name(), "delete_rule");
}

#[tokio::test]
async fn cross_project_isolation_constraints() {
    let cat = InMemoryCatalog::new();
    let a = ProjectId::new();
    let b = ProjectId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let s_a = schema_with(&[("a_only", DataType::Int64, false)]);
    cat.create_table(&a, &name("tbl_a"), &s_a).await.unwrap();
    let s_b = schema_with(&[("b_only", DataType::Int64, false)]);
    cat.create_table(&b, &name("tbl_b"), &s_b).await.unwrap();

    // A's constraints visible only to A.
    let batch_a = InfoSchemaQuery::table_constraints(&cat, &a).await.unwrap();
    let table_names_a: Vec<&str> = (0..batch_a.num_rows())
        .map(|i| col_str(&batch_a, "table_name").value(i))
        .collect();
    let cnames_a: Vec<&str> = (0..batch_a.num_rows())
        .map(|i| col_str(&batch_a, "constraint_name").value(i))
        .collect();
    assert_eq!(batch_a.num_rows(), 1);
    assert!(table_names_a.contains(&"tbl_a"));
    assert!(!table_names_a.contains(&"tbl_b"));
    assert!(!cnames_a.contains(&"tbl_b_b_only_not_null"));

    // Symmetric: B sees only B.
    let batch_b = InfoSchemaQuery::table_constraints(&cat, &b).await.unwrap();
    let table_names_b: Vec<&str> = (0..batch_b.num_rows())
        .map(|i| col_str(&batch_b, "table_name").value(i))
        .collect();
    assert_eq!(batch_b.num_rows(), 1);
    assert_eq!(table_names_b, vec!["tbl_b"]);

    // key_column_usage / referential_constraints stay empty for both,
    // but the cross-project invariant still holds (no rows == no leak).
    assert_eq!(
        InfoSchemaQuery::key_column_usage(&cat, &a)
            .await
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        InfoSchemaQuery::referential_constraints(&cat, &b)
            .await
            .unwrap()
            .num_rows(),
        0
    );
}
