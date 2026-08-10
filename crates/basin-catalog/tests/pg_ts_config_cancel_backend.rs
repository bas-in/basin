//! Tests for `pg_catalog.pg_ts_config` and `pg_cancel_backend(pid)`.
//!
//! Phase 5.24.F — verifies both new catalog surfaces introduced alongside the
//! existing `info_schema.rs` / `replication_slots.rs` patterns.

use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use basin_catalog::{
    info_schema::{CancelBackendRegistry, InfoSchemaQuery},
    Catalog, InMemoryCatalog,
};
use basin_common::ProjectId;

// ─────────────────────────────────────────────────────────────────────────────
// pg_catalog.pg_ts_config
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pg_ts_config_returns_two_rows() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();

    assert_eq!(
        batch.num_rows(),
        2,
        "must emit exactly 2 configs (english + simple)"
    );
}

#[tokio::test]
async fn pg_ts_config_includes_english_and_simple() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();
    let cfgnames = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cfgname must be StringArray");

    let names: Vec<&str> = (0..cfgnames.len()).map(|i| cfgnames.value(i)).collect();

    assert!(names.contains(&"english"), "must include 'english' config");
    assert!(names.contains(&"simple"), "must include 'simple' config");
}

#[tokio::test]
async fn pg_ts_config_schema_columns_correct() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();
    let schema = batch.schema();

    // Must have exactly 5 columns matching PG's pg_ts_config layout.
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        ["oid", "cfgname", "cfgnamespace", "cfgowner", "cfgparser"]
    );
}

#[tokio::test]
async fn pg_ts_config_oids_are_pg_system_oids() {
    // PG 15 system OIDs: english=12824, simple=12801. Using those OIDs means
    // clients that look up a config by OID (e.g. `SET default_text_search_config
    // = pg_catalog.english`) still resolve correctly.
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();
    let oids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("oid must be Int64Array");
    let cfgnames = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cfgname must be StringArray");

    let mut oid_by_name = std::collections::HashMap::new();
    for i in 0..batch.num_rows() {
        oid_by_name.insert(cfgnames.value(i).to_string(), oids.value(i));
    }

    assert_eq!(
        oid_by_name.get("english").copied(),
        Some(12824i64),
        "english config must use PG system OID 12824"
    );
    assert_eq!(
        oid_by_name.get("simple").copied(),
        Some(12801i64),
        "simple config must use PG system OID 12801"
    );
}

#[tokio::test]
async fn pg_ts_config_cfgparser_is_default_parser() {
    // All configs share the default parser (OID 3722).
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();
    let parsers = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cfgparser must be Int64Array");

    for i in 0..batch.num_rows() {
        assert_eq!(
            parsers.value(i),
            3722i64,
            "cfgparser must be 3722 (default text-search parser) for all rows"
        );
    }
}

#[tokio::test]
async fn pg_ts_config_oids_are_stable_across_calls() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();
    cat.create_namespace(&p).await.unwrap();

    let batch1 = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();
    let batch2 = InfoSchemaQuery::pg_ts_config(&cat, &p).await.unwrap();

    let oids1 = batch1
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let oids2 = batch2
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let v1: Vec<i64> = (0..oids1.len()).map(|i| oids1.value(i)).collect();
    let v2: Vec<i64> = (0..oids2.len()).map(|i| oids2.value(i)).collect();

    assert_eq!(v1, v2, "pg_ts_config oids must be stable across calls");
}

#[tokio::test]
async fn pg_ts_config_cross_project_cfgnamespace_differs() {
    // cfgnamespace is a per-project hash; two different projects must produce
    // different cfgnamespace values (proving no accidental sharing).
    let cat = InMemoryCatalog::new();
    let p1 = ProjectId::new();
    let p2 = ProjectId::new();
    cat.create_namespace(&p1).await.unwrap();
    cat.create_namespace(&p2).await.unwrap();

    let b1 = InfoSchemaQuery::pg_ts_config(&cat, &p1).await.unwrap();
    let b2 = InfoSchemaQuery::pg_ts_config(&cat, &p2).await.unwrap();

    let ns1 = b1
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let ns2 = b2
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_ne!(ns1, ns2, "cfgnamespace must differ between projects");
}

// ─────────────────────────────────────────────────────────────────────────────
// pg_cancel_backend
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn cancel_backend_returns_false_for_unknown_pid() {
    let registry = CancelBackendRegistry::new();
    let p = ProjectId::new();

    let batch = InfoSchemaQuery::pg_cancel_backend(&registry, &p, 9999).unwrap();
    let result = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("result must be BooleanArray");

    assert_eq!(batch.num_rows(), 1, "must return exactly one row");
    assert!(
        !result.value(0),
        "must return false for an unregistered pid"
    );
}

#[test]
fn cancel_backend_returns_true_for_registered_pid() {
    let registry = CancelBackendRegistry::new();
    let p = ProjectId::new();

    registry.register(42);
    let batch = InfoSchemaQuery::pg_cancel_backend(&registry, &p, 42).unwrap();
    let result = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    assert!(result.value(0), "must return true when pid is registered");
}

#[test]
fn cancel_backend_sets_is_cancelled_flag() {
    let registry = CancelBackendRegistry::new();

    registry.register(7);
    assert!(!registry.is_cancelled(7), "not cancelled before call");

    let p = ProjectId::new();
    InfoSchemaQuery::pg_cancel_backend(&registry, &p, 7).unwrap();
    assert!(registry.is_cancelled(7), "must be cancelled after call");
}

#[test]
fn cancel_backend_deregister_clears_pid() {
    let registry = CancelBackendRegistry::new();
    let p = ProjectId::new();

    registry.register(99);
    registry.deregister(99);

    // After deregister, pid is unknown → cancel returns false.
    let batch = InfoSchemaQuery::pg_cancel_backend(&registry, &p, 99).unwrap();
    let result = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!result.value(0), "deregistered pid must return false");
}

#[test]
fn cancel_backend_idempotent_double_cancel() {
    let registry = CancelBackendRegistry::new();
    let p = ProjectId::new();

    registry.register(5);

    // First cancel → true.
    let b1 = InfoSchemaQuery::pg_cancel_backend(&registry, &p, 5).unwrap();
    let r1 = b1
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(r1.value(0));

    // Second cancel → still true (flag already set, pid still present).
    let b2 = InfoSchemaQuery::pg_cancel_backend(&registry, &p, 5).unwrap();
    let r2 = b2
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(r2.value(0));
}

#[test]
fn cancel_backend_multiple_pids_independent() {
    let registry = CancelBackendRegistry::new();
    let p = ProjectId::new();

    registry.register(1);
    registry.register(2);
    registry.register(3);

    // Cancel only pid 2.
    InfoSchemaQuery::pg_cancel_backend(&registry, &p, 2).unwrap();

    assert!(!registry.is_cancelled(1), "pid 1 must not be cancelled");
    assert!(registry.is_cancelled(2), "pid 2 must be cancelled");
    assert!(!registry.is_cancelled(3), "pid 3 must not be cancelled");
}

#[test]
fn pg_cancel_backend_proc_row_has_correct_fields() {
    let p = ProjectId::new();
    let row = InfoSchemaQuery::pg_cancel_backend_proc_row(&p);

    assert_eq!(row.proname, "pg_cancel_backend");
    assert_eq!(row.prokind, "f");
    assert_eq!(row.pronargs, 1i16);
    assert_eq!(row.prorettype, 16i64, "return type must be bool (OID 16)");
    assert_eq!(row.proargtypes, "23", "arg type must be int4 (OID 23)");
    assert_eq!(row.prolang, 14i64, "language must be SQL (OID 14)");
}

#[test]
fn pg_cancel_backend_proc_row_oid_is_stable() {
    let p = ProjectId::new();
    let row1 = InfoSchemaQuery::pg_cancel_backend_proc_row(&p);
    let row2 = InfoSchemaQuery::pg_cancel_backend_proc_row(&p);
    assert_eq!(
        row1.oid, row2.oid,
        "proc row oid must be stable across calls"
    );
}

#[test]
fn pg_cancel_backend_proc_row_oid_differs_per_project() {
    let p1 = ProjectId::new();
    let p2 = ProjectId::new();
    let r1 = InfoSchemaQuery::pg_cancel_backend_proc_row(&p1);
    let r2 = InfoSchemaQuery::pg_cancel_backend_proc_row(&p2);
    assert_ne!(r1.oid, r2.oid, "proc row oid must differ between projects");
}

#[test]
fn cancel_backend_registry_is_cheap_to_clone() {
    // Arc clone: both clones share the same underlying map.
    let reg1 = CancelBackendRegistry::new();
    let reg2 = reg1.clone();

    reg1.register(11);
    // Visible through clone.
    assert!(!reg2.is_cancelled(11));
    reg2.cancel(11);
    assert!(
        reg1.is_cancelled(11),
        "cancel through clone must be visible on original"
    );
}
