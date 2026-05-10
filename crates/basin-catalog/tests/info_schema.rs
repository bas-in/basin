//! Integration tests for `basin_catalog::info_schema` (Phase 5.11.M starter).
//!
//! Covers the rust-API surface ([`InfoSchemaQuery::tables`] /
//! [`InfoSchemaQuery::pg_class`]) against the in-memory backend. Engine-side
//! SELECT routing (so `SELECT * FROM information_schema.tables` works through
//! pgwire) is the next follow-up and lives in `basin-engine`.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{
    info_schema::InfoSchemaQuery, Catalog, CvDef, InMemoryCatalog, Policy, PolicyCommand,
};
use basin_common::{TableName, TenantId};

fn schema(cols: &[(&str, DataType)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        cols.iter()
            .map(|(n, t)| Field::new(*n, t.clone(), false))
            .collect::<Vec<_>>(),
    ))
}

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

fn col_f32<'a>(b: &'a RecordBatch, n: &str) -> &'a Float32Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
}

#[tokio::test]
async fn tables_view_lists_only_calling_tenants_tables() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&a, &name("alpha"), &s).await.unwrap();
    cat.create_table(&a, &name("beta"), &s).await.unwrap();
    cat.create_table(&b, &name("gamma"), &s).await.unwrap();

    let batch_a = InfoSchemaQuery::tables(&cat, &a).await.unwrap();
    let names_a: Vec<&str> = (0..batch_a.num_rows())
        .map(|i| col_str(&batch_a, "table_name").value(i))
        .collect();
    assert_eq!(batch_a.num_rows(), 2, "tenant A must see exactly 2 tables");
    assert!(names_a.contains(&"alpha"));
    assert!(names_a.contains(&"beta"));
    assert!(
        !names_a.contains(&"gamma"),
        "tenant A must NOT see tenant B's table"
    );

    let batch_b = InfoSchemaQuery::tables(&cat, &b).await.unwrap();
    let names_b: Vec<&str> = (0..batch_b.num_rows())
        .map(|i| col_str(&batch_b, "table_name").value(i))
        .collect();
    assert_eq!(batch_b.num_rows(), 1);
    assert_eq!(names_b, vec!["gamma"]);

    // Every row in tenant A's batch reports schema = "public" and
    // catalog = "basin"; that's the contract pgAdmin / PostgREST
    // assert against.
    for i in 0..batch_a.num_rows() {
        assert_eq!(col_str(&batch_a, "table_catalog").value(i), "basin");
        assert_eq!(col_str(&batch_a, "table_schema").value(i), "public");
        assert_eq!(col_str(&batch_a, "table_type").value(i), "BASE TABLE");
    }
}

#[tokio::test]
async fn tables_view_includes_materialized_views_as_base_table() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&t, &name("source"), &s).await.unwrap();
    cat.create_table(&t, &name("agg"), &s).await.unwrap();
    cat.set_continuous_aggregate(
        &t,
        &name("agg"),
        Some(CvDef {
            source_table: "source".into(),
            query_sql: "SELECT count(*) FROM source".into(),
            refresh_interval_secs: 60,
            last_refreshed_at_unix_ms: None,
            last_bucket_max_unix_ms: None,
        }),
    )
    .await
    .unwrap();

    let batch = InfoSchemaQuery::tables(&cat, &t).await.unwrap();
    // Both source and agg are visible; both report `BASE TABLE` to
    // match PG (information_schema predates matviews).
    let mut found_agg = false;
    for i in 0..batch.num_rows() {
        let n = col_str(&batch, "table_name").value(i);
        let ty = col_str(&batch, "table_type").value(i);
        assert_eq!(
            ty, "BASE TABLE",
            "PG reports matviews as BASE TABLE in information_schema, got {ty} for {n}"
        );
        if n == "agg" {
            found_agg = true;
        }
    }
    assert!(
        found_agg,
        "matview must appear in information_schema.tables"
    );
}

#[tokio::test]
async fn pg_class_returns_consistent_oid_per_tuple() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&t, &name("widgets"), &s).await.unwrap();

    let b1 = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    let b2 = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();

    assert_eq!(b1.num_rows(), 1);
    assert_eq!(b2.num_rows(), 1);
    let oid1 = col_i64(&b1, "oid").value(0);
    let oid2 = col_i64(&b2, "oid").value(0);
    assert_eq!(
        oid1, oid2,
        "oid must be stable across queries for the same (tenant, table)"
    );
    assert!(oid1 > 0, "oid must be positive (FNV masked to 63 bits)");
}

#[tokio::test]
async fn pg_class_relkind_is_correct() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema(&[("k", DataType::Int64)]);

    cat.create_table(&t, &name("base"), &s).await.unwrap();
    cat.create_table(&t, &name("matview"), &s).await.unwrap();
    cat.set_continuous_aggregate(
        &t,
        &name("matview"),
        Some(CvDef {
            source_table: "base".into(),
            query_sql: "SELECT count(*) FROM base".into(),
            refresh_interval_secs: 60,
            last_refreshed_at_unix_ms: None,
            last_bucket_max_unix_ms: None,
        }),
    )
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    for i in 0..batch.num_rows() {
        let n = col_str(&batch, "relname").value(i);
        let kind = col_str(&batch, "relkind").value(i);
        match n {
            "base" => assert_eq!(kind, "r", "base table must be relkind 'r'"),
            "matview" => assert_eq!(kind, "m", "matview must be relkind 'm'"),
            other => panic!("unexpected table {other}"),
        }
    }
}

#[tokio::test]
async fn pg_class_relrowsecurity_reflects_rls() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&t, &name("guarded"), &s).await.unwrap();
    cat.create_table(&t, &name("plain"), &s).await.unwrap();

    cat.set_rls_state(
        &t,
        &name("guarded"),
        true,
        vec![Policy {
            name: "p1".into(),
            applies_to_roles: vec![],
            command: PolicyCommand::All,
            using_expr: "true".into(),
            with_check_expr: None,
        }],
    )
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    let mut saw_guarded = false;
    let mut saw_plain = false;
    for i in 0..batch.num_rows() {
        let n = col_str(&batch, "relname").value(i);
        let rls = col_bool(&batch, "relrowsecurity").value(i);
        match n {
            "guarded" => {
                assert!(rls, "RLS-enabled table must report relrowsecurity=true");
                saw_guarded = true;
            }
            "plain" => {
                assert!(!rls, "non-RLS table must report relrowsecurity=false");
                saw_plain = true;
            }
            _ => {}
        }
    }
    assert!(saw_guarded && saw_plain);
}

#[tokio::test]
async fn cross_tenant_isolation() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&a, &name("a_only_1"), &s).await.unwrap();
    cat.create_table(&a, &name("a_only_2"), &s).await.unwrap();
    cat.create_table(&b, &name("b_only_1"), &s).await.unwrap();

    // Tenant A's pg_class must contain ZERO rows belonging to tenant B.
    let batch_a = InfoSchemaQuery::pg_class(&cat, &a).await.unwrap();
    let names_a: Vec<&str> = (0..batch_a.num_rows())
        .map(|i| col_str(&batch_a, "relname").value(i))
        .collect();
    assert_eq!(batch_a.num_rows(), 2);
    assert!(!names_a.contains(&"b_only_1"));

    // Symmetric: tenant B sees only its own tables.
    let batch_b = InfoSchemaQuery::pg_class(&cat, &b).await.unwrap();
    let names_b: Vec<&str> = (0..batch_b.num_rows())
        .map(|i| col_str(&batch_b, "relname").value(i))
        .collect();
    assert_eq!(batch_b.num_rows(), 1);
    assert_eq!(names_b, vec!["b_only_1"]);

    // Same-name tables in different tenants get disjoint oids by
    // construction (the tenant ULID is hashed in).
    cat.create_table(&b, &name("a_only_1"), &s).await.unwrap();
    let batch_a2 = InfoSchemaQuery::pg_class(&cat, &a).await.unwrap();
    let batch_b2 = InfoSchemaQuery::pg_class(&cat, &b).await.unwrap();
    let oid_a = (0..batch_a2.num_rows())
        .find(|&i| col_str(&batch_a2, "relname").value(i) == "a_only_1")
        .map(|i| col_i64(&batch_a2, "oid").value(i))
        .unwrap();
    let oid_b = (0..batch_b2.num_rows())
        .find(|&i| col_str(&batch_b2, "relname").value(i) == "a_only_1")
        .map(|i| col_i64(&batch_b2, "oid").value(i))
        .unwrap();
    assert_ne!(
        oid_a, oid_b,
        "same table name across tenants must have disjoint oids"
    );
}

#[tokio::test]
async fn empty_tenant_returns_empty_batch() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let tables = InfoSchemaQuery::tables(&cat, &t).await.unwrap();
    assert_eq!(tables.num_rows(), 0);
    let s = tables.schema();
    assert_eq!(s.field(0).name(), "table_catalog");
    assert_eq!(s.field(1).name(), "table_schema");
    assert_eq!(s.field(2).name(), "table_name");
    assert_eq!(s.field(3).name(), "table_type");

    let pgc = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    assert_eq!(pgc.num_rows(), 0);
    let s = pgc.schema();
    assert_eq!(s.field(0).name(), "oid");
    assert_eq!(s.field(0).data_type(), &DataType::Int64);
    assert_eq!(s.field(1).name(), "relname");
    assert_eq!(s.field(2).name(), "relnamespace");
    assert_eq!(s.field(3).name(), "relkind");
    assert_eq!(s.field(4).name(), "relrowsecurity");
    assert_eq!(s.field(4).data_type(), &DataType::Boolean);
    assert_eq!(s.field(5).name(), "relispartition");
    assert_eq!(s.field(6).name(), "reltuples");
    assert_eq!(s.field(6).data_type(), &DataType::Float32);
}

/// Tenant with no tables but no `create_namespace` either: the in-memory
/// backend returns an empty `list_tables`, not an error. Locks in that
/// the views don't crash on the truly-blank tenant.
#[tokio::test]
async fn unknown_tenant_returns_empty_batch() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    let tables = InfoSchemaQuery::tables(&cat, &t).await.unwrap();
    assert_eq!(tables.num_rows(), 0);
    let pgc = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    assert_eq!(pgc.num_rows(), 0);
}

/// `relispartition` is hardcoded to `false` in v0.1; the partition_spec
/// machinery is per-table physical layout, not PG inheritance partitioning.
#[tokio::test]
async fn pg_class_relispartition_is_false() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&t, &name("t1"), &s).await.unwrap();
    let batch = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(!col_bool(&batch, "relispartition").value(0));
}

/// `reltuples` defaults to 0 for a freshly-created table (genesis
/// snapshot has no data files). Locks in the f32 type.
#[tokio::test]
async fn pg_class_reltuples_zero_on_empty_table() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = schema(&[("k", DataType::Int64)]);
    cat.create_table(&t, &name("t1"), &s).await.unwrap();
    let batch = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    assert_eq!(col_f32(&batch, "reltuples").value(0), 0.0);
}
