//! Catalog-side integration tests for the Phase 5.11.M Tier 3 views:
//! `pg_catalog.pg_proc` and `information_schema.routines`.
//!
//! Engine-side SELECT routing is exercised in
//! `crates/basin-engine/tests/info_schema_routines_routing.rs`. This file
//! pins the catalog-API surface against the in-memory backend so changes
//! to the rust-side `InfoSchemaQuery::pg_proc` / `routines` shape break
//! here first, before cascading through DataFusion glue.

use arrow_array::{Array, Int16Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::{
    info_schema::InfoSchemaQuery, Catalog, InMemoryCatalog, SqlArgType, SqlFunctionArg,
    SqlFunctionDef, SqlFunctionLanguage, SqlProcedureDef, SqlReturnType,
};
use basin_common::ProjectId;

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

fn col_i64<'a>(b: &'a RecordBatch, n: &str) -> &'a Int64Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap()
}

fn make_func(
    project: ProjectId,
    name: &str,
    args: Vec<(&str, SqlArgType)>,
    ret: SqlArgType,
    body: &str,
) -> SqlFunctionDef {
    SqlFunctionDef {
        project,
        name: name.to_string(),
        args: args
            .into_iter()
            .map(|(n, t)| SqlFunctionArg {
                name: n.into(),
                data_type: t,
            })
            .collect(),
        return_type: SqlReturnType::Scalar(ret),
        body: body.to_string(),
        language: SqlFunctionLanguage::Sql,
    }
}

fn make_proc(
    project: ProjectId,
    name: &str,
    args: Vec<(&str, SqlArgType)>,
    body: &str,
) -> SqlProcedureDef {
    SqlProcedureDef {
        project,
        name: name.to_string(),
        args: args
            .into_iter()
            .map(|(n, t)| SqlFunctionArg {
                name: n.into(),
                data_type: t,
            })
            .collect(),
        body: body.to_string(),
    }
}

#[tokio::test]
async fn pg_proc_lists_user_functions() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(
        t,
        "add_one",
        vec![("x", SqlArgType::BigInt)],
        SqlArgType::BigInt,
        "SELECT x + 1",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(&batch, "proname").value(0), "add_one");
    assert_eq!(col_str(&batch, "prokind").value(0), "f");
    assert_eq!(col_i16(&batch, "pronargs").value(0), 1);
    // BIGINT is PG int8 = OID 20
    assert_eq!(col_i64(&batch, "prorettype").value(0), 20);
    assert_eq!(col_str(&batch, "proargtypes").value(0), "20");
    assert_eq!(col_str(&batch, "prosrc").value(0), "SELECT x + 1");
    assert_eq!(col_i64(&batch, "prolang").value(0), 14);
    assert!(col_i64(&batch, "oid").value(0) > 0);
}

#[tokio::test]
async fn pg_proc_lists_user_procedures() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_procedure(make_proc(
        t,
        "log_one",
        vec![("g", SqlArgType::Text)],
        "INSERT INTO log VALUES (g)",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(&batch, "proname").value(0), "log_one");
    assert_eq!(col_str(&batch, "prokind").value(0), "p");
    assert_eq!(col_i16(&batch, "pronargs").value(0), 1);
    // Procedures have no return type — prorettype is 0.
    assert_eq!(col_i64(&batch, "prorettype").value(0), 0);
    // TEXT is PG text = OID 25
    assert_eq!(col_str(&batch, "proargtypes").value(0), "25");
    assert_eq!(col_i64(&batch, "prolang").value(0), 14);
}

#[tokio::test]
async fn pg_proc_oid_stable_per_tuple() {
    // Two consecutive calls must produce the same oid for the same
    // (project, name). Stability is the property pgAdmin / DataGrip rely on
    // when caching catalog rows across reconnects.
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(t, "f", vec![], SqlArgType::BigInt, "SELECT 1"))
        .await
        .unwrap();

    let b1 = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    let b2 = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    let oid1 = col_i64(&b1, "oid").value(0);
    let oid2 = col_i64(&b2, "oid").value(0);
    assert_eq!(oid1, oid2, "oid must be stable across calls");
    assert!(oid1 > 0);
}

#[tokio::test]
async fn routines_returns_correct_routine_type() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(
        t,
        "my_func",
        vec![],
        SqlArgType::BigInt,
        "SELECT 1",
    ))
    .await
    .unwrap();
    cat.register_procedure(make_proc(t, "my_proc", vec![], "INSERT INTO t VALUES (1)"))
        .await
        .unwrap();

    let batch = InfoSchemaQuery::routines(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 2);

    // Build a name→routine_type map; row order isn't pinned since
    // list_sql_functions / list_procedures don't guarantee order.
    let mut by_name = std::collections::HashMap::new();
    for i in 0..batch.num_rows() {
        by_name.insert(
            col_str(&batch, "routine_name").value(i).to_string(),
            col_str(&batch, "routine_type").value(i).to_string(),
        );
    }
    assert_eq!(by_name.get("my_func").unwrap(), "FUNCTION");
    assert_eq!(by_name.get("my_proc").unwrap(), "PROCEDURE");
}

#[tokio::test]
async fn routines_data_type_null_for_procedures() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(t, "f", vec![], SqlArgType::Text, "SELECT 'hi'"))
        .await
        .unwrap();
    cat.register_procedure(make_proc(t, "p", vec![], "INSERT INTO log VALUES ('hi')"))
        .await
        .unwrap();

    let batch = InfoSchemaQuery::routines(&cat, &t).await.unwrap();
    let arr = col_str(&batch, "data_type");
    let mut by_name: std::collections::HashMap<String, Option<String>> =
        std::collections::HashMap::new();
    for i in 0..batch.num_rows() {
        let n = col_str(&batch, "routine_name").value(i).to_string();
        let dt = if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        };
        by_name.insert(n, dt);
    }
    assert_eq!(by_name.get("f").unwrap(), &Some("text".to_string()));
    assert_eq!(
        by_name.get("p").unwrap(),
        &None,
        "procedure data_type must be SQL NULL"
    );
}

#[tokio::test]
async fn routines_is_deterministic_heuristic() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    // Pure SELECT — deterministic.
    cat.register_sql_function(make_func(
        t,
        "pure",
        vec![("x", SqlArgType::BigInt)],
        SqlArgType::BigInt,
        "SELECT x + 1",
    ))
    .await
    .unwrap();
    // nextval → not deterministic.
    cat.register_sql_function(make_func(
        t,
        "with_seq",
        vec![],
        SqlArgType::BigInt,
        "SELECT nextval('s')",
    ))
    .await
    .unwrap();
    // now() → not deterministic.
    cat.register_sql_function(make_func(
        t,
        "with_now",
        vec![],
        SqlArgType::TimestampTz,
        "SELECT now()",
    ))
    .await
    .unwrap();
    // current_timestamp → not deterministic.
    cat.register_sql_function(make_func(
        t,
        "with_ts",
        vec![],
        SqlArgType::TimestampTz,
        "SELECT current_timestamp",
    ))
    .await
    .unwrap();
    // random() → not deterministic.
    cat.register_sql_function(make_func(
        t,
        "with_rand",
        vec![],
        SqlArgType::Double,
        "SELECT random()",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::routines(&cat, &t).await.unwrap();
    let mut by_name = std::collections::HashMap::new();
    for i in 0..batch.num_rows() {
        by_name.insert(
            col_str(&batch, "routine_name").value(i).to_string(),
            col_str(&batch, "is_deterministic").value(i).to_string(),
        );
    }
    assert_eq!(by_name.get("pure").unwrap(), "YES");
    assert_eq!(by_name.get("with_seq").unwrap(), "NO");
    assert_eq!(by_name.get("with_now").unwrap(), "NO");
    assert_eq!(by_name.get("with_ts").unwrap(), "NO");
    assert_eq!(by_name.get("with_rand").unwrap(), "NO");
}

#[tokio::test]
async fn cross_project_isolation() {
    let cat = InMemoryCatalog::new();
    let a = ProjectId::new();
    let b = ProjectId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();
    cat.register_sql_function(make_func(
        a,
        "only_a_fn",
        vec![],
        SqlArgType::BigInt,
        "SELECT 1",
    ))
    .await
    .unwrap();
    cat.register_procedure(make_proc(
        b,
        "only_b_proc",
        vec![],
        "INSERT INTO log VALUES ('hi')",
    ))
    .await
    .unwrap();

    let pa = InfoSchemaQuery::pg_proc(&cat, &a).await.unwrap();
    let pb = InfoSchemaQuery::pg_proc(&cat, &b).await.unwrap();
    let names_a: Vec<&str> = (0..pa.num_rows())
        .map(|i| col_str(&pa, "proname").value(i))
        .collect();
    let names_b: Vec<&str> = (0..pb.num_rows())
        .map(|i| col_str(&pb, "proname").value(i))
        .collect();
    assert_eq!(names_a, vec!["only_a_fn"]);
    assert_eq!(names_b, vec!["only_b_proc"]);
    assert!(!names_a.contains(&"only_b_proc"));
    assert!(!names_b.contains(&"only_a_fn"));

    let ra = InfoSchemaQuery::routines(&cat, &a).await.unwrap();
    let rb = InfoSchemaQuery::routines(&cat, &b).await.unwrap();
    let rnames_a: Vec<&str> = (0..ra.num_rows())
        .map(|i| col_str(&ra, "routine_name").value(i))
        .collect();
    let rnames_b: Vec<&str> = (0..rb.num_rows())
        .map(|i| col_str(&rb, "routine_name").value(i))
        .collect();
    assert_eq!(rnames_a, vec!["only_a_fn"]);
    assert_eq!(rnames_b, vec!["only_b_proc"]);
}

#[tokio::test]
async fn argtypes_format() {
    // (BIGINT, TEXT) → proargtypes = "20 25" (space-separated PG OIDs).
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(
        t,
        "two_args",
        vec![("a", SqlArgType::BigInt), ("b", SqlArgType::Text)],
        SqlArgType::BigInt,
        "SELECT a + length(b)",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(col_str(&batch, "proargtypes").value(0), "20 25");
    assert_eq!(col_i16(&batch, "pronargs").value(0), 2);
}

#[tokio::test]
async fn empty_project_returns_empty_pg_proc_and_routines() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    let p = InfoSchemaQuery::pg_proc(&cat, &t).await.unwrap();
    assert_eq!(p.num_rows(), 0);
    let r = InfoSchemaQuery::routines(&cat, &t).await.unwrap();
    assert_eq!(r.num_rows(), 0);
}
