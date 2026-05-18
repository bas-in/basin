//! Integration tests for the enum-type and domain catalog APIs.
//!
//! These cover the rust-API surface (`Catalog::register_enum_type`,
//! `Catalog::register_domain`, …) against the in-memory backend. The
//! engine-side SQL surface lives in
//! `crates/basin-engine/tests/enum_domain_round_trip.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{
    Catalog, DomainDef, EnumTypeDef, InMemoryCatalog, SqlArgType, BASIN_DOMAIN_KEY,
    BASIN_ENUM_TYPE_KEY,
};
use basin_common::{BasinError, ProjectId, TableName};

fn enum_def(t: ProjectId, name: &str, labels: &[&str]) -> EnumTypeDef {
    EnumTypeDef {
        project: t,
        name: name.into(),
        labels: labels.iter().map(|s| s.to_string()).collect(),
    }
}

fn domain_def(t: ProjectId, name: &str, base: SqlArgType, check: Option<&str>) -> DomainDef {
    DomainDef {
        project: t,
        name: name.into(),
        base_type: base,
        check_predicate: check.map(|s| s.to_string()),
    }
}

#[tokio::test]
async fn enum_round_trip() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_enum_type(enum_def(t, "color", &["red", "green", "blue"]))
        .await
        .unwrap();

    let looked_up = cat.lookup_enum_type(&t, "color").await.unwrap();
    assert_eq!(looked_up.labels, vec!["red", "green", "blue"]);

    cat.add_enum_value(&t, "color", "yellow").await.unwrap();
    let after = cat.lookup_enum_type(&t, "color").await.unwrap();
    assert_eq!(after.labels, vec!["red", "green", "blue", "yellow"]);

    let listed = cat.list_enum_types(&t).await;
    assert_eq!(listed.len(), 1);

    cat.drop_enum_type(&t, "color").await.unwrap();
    assert!(cat.lookup_enum_type(&t, "color").await.is_none());
}

#[tokio::test]
async fn enum_value_uniqueness_enforced() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    let err = cat
        .register_enum_type(enum_def(t, "x", &["a", "b", "a"]))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");

    // Adding a value that already exists is rejected.
    cat.register_enum_type(enum_def(t, "y", &["a", "b"]))
        .await
        .unwrap();
    let err = cat.add_enum_value(&t, "y", "a").await.unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
}

#[tokio::test]
async fn enum_empty_label_list_rejected() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    let err = cat
        .register_enum_type(enum_def(t, "empty", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)));
}

#[tokio::test]
async fn enum_duplicate_registration_rejected() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_enum_type(enum_def(t, "z", &["a"]))
        .await
        .unwrap();
    let err = cat
        .register_enum_type(enum_def(t, "z", &["b"]))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
}

#[tokio::test]
async fn add_enum_value_on_unknown_errors() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    let err = cat.add_enum_value(&t, "missing", "x").await.unwrap_err();
    assert!(matches!(err, BasinError::NotFound(_)));
}

#[tokio::test]
async fn drop_enum_unknown_errors() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    let err = cat.drop_enum_type(&t, "ghost").await.unwrap_err();
    assert!(matches!(err, BasinError::NotFound(_)));
}

#[tokio::test]
async fn domain_round_trip() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_domain(domain_def(
        t,
        "positive_int",
        SqlArgType::Int,
        Some("VALUE > 0"),
    ))
    .await
    .unwrap();
    let looked_up = cat.lookup_domain(&t, "positive_int").await.unwrap();
    assert_eq!(looked_up.base_type, SqlArgType::Int);
    assert_eq!(looked_up.check_predicate.unwrap(), "VALUE > 0");

    let listed = cat.list_domains(&t).await;
    assert_eq!(listed.len(), 1);

    cat.drop_domain(&t, "positive_int").await.unwrap();
    assert!(cat.lookup_domain(&t, "positive_int").await.is_none());
}

#[tokio::test]
async fn domain_predicate_must_parse() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    let err = cat
        .register_domain(domain_def(t, "bad", SqlArgType::Int, Some("@@@")))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
}

#[tokio::test]
async fn cross_project_isolation() {
    let cat = InMemoryCatalog::new();
    let a = ProjectId::new();
    let b = ProjectId::new();
    cat.register_enum_type(enum_def(a, "shared", &["x", "y"]))
        .await
        .unwrap();
    cat.register_domain(domain_def(a, "d", SqlArgType::Int, Some("VALUE > 0")))
        .await
        .unwrap();

    // Project B sees neither the enum nor the domain.
    assert!(cat.lookup_enum_type(&b, "shared").await.is_none());
    assert!(cat.lookup_domain(&b, "d").await.is_none());
    assert!(cat.list_enum_types(&b).await.is_empty());
    assert!(cat.list_domains(&b).await.is_empty());

    // Project B can register its own type with the same name without
    // colliding against project A's row.
    cat.register_enum_type(enum_def(b, "shared", &["one"]))
        .await
        .unwrap();
    let b_def = cat.lookup_enum_type(&b, "shared").await.unwrap();
    assert_eq!(b_def.labels, vec!["one"]);
    let a_def = cat.lookup_enum_type(&a, "shared").await.unwrap();
    assert_eq!(a_def.labels, vec!["x", "y"]);
}

#[tokio::test]
async fn enum_and_domain_namespace_collision_rejected() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_enum_type(enum_def(t, "name", &["a"]))
        .await
        .unwrap();
    let err = cat
        .register_domain(domain_def(t, "name", SqlArgType::Int, None))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
}

#[tokio::test]
async fn drop_enum_blocked_when_referenced() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_enum_type(enum_def(t, "status", &["a", "b"]))
        .await
        .unwrap();

    // Build a table whose `status` column carries the
    // `BASIN_ENUM_TYPE` metadata marker so the catalog refcount sees
    // it as a referencer.
    let mut md = HashMap::new();
    md.insert(BASIN_ENUM_TYPE_KEY.to_string(), "status".to_string());
    let f1 = Field::new("id", DataType::Int64, false);
    let f2 = Field::new("status", DataType::Utf8, false).with_metadata(md);
    let schema = Schema::new(vec![f1, f2]);
    cat.create_table(&t, &TableName::new("orders").unwrap(), &schema)
        .await
        .unwrap();

    let err = cat.drop_enum_type(&t, "status").await.unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
    let msg = format!("{err}");
    assert!(
        msg.contains("orders.status") || msg.contains("references"),
        "error should mention the column, got: {err}"
    );

    // After we drop the table the type can be removed.
    cat.drop_table(&t, &TableName::new("orders").unwrap())
        .await
        .unwrap();
    cat.drop_enum_type(&t, "status").await.unwrap();
}

#[tokio::test]
async fn drop_domain_blocked_when_referenced() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.register_domain(domain_def(
        t,
        "positive_int",
        SqlArgType::Int,
        Some("VALUE > 0"),
    ))
    .await
    .unwrap();

    let mut md = HashMap::new();
    md.insert(BASIN_DOMAIN_KEY.to_string(), "positive_int".to_string());
    let f1 = Field::new("id", DataType::Int64, false);
    let f2 = Field::new("qty", DataType::Int64, false).with_metadata(md);
    let schema = Schema::new(vec![f1, f2]);
    cat.create_table(&t, &TableName::new("orders").unwrap(), &schema)
        .await
        .unwrap();

    let err = cat.drop_domain(&t, "positive_int").await.unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");

    cat.drop_table(&t, &TableName::new("orders").unwrap())
        .await
        .unwrap();
    cat.drop_domain(&t, "positive_int").await.unwrap();
}

#[tokio::test]
async fn drop_namespace_clears_enums_and_domains() {
    let cat = Arc::new(InMemoryCatalog::new());
    let t = ProjectId::new();
    cat.register_enum_type(enum_def(t, "e", &["a"]))
        .await
        .unwrap();
    cat.register_domain(domain_def(t, "d", SqlArgType::Int, None))
        .await
        .unwrap();
    cat.drop_namespace(&t).await.unwrap();
    assert!(cat.lookup_enum_type(&t, "e").await.is_none());
    assert!(cat.lookup_domain(&t, "d").await.is_none());
}
