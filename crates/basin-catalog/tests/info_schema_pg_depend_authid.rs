//! Catalog-side integration tests for the Phase 5.11.M tail views:
//! `pg_catalog.pg_depend` and `pg_catalog.pg_authid`.
//!
//! Engine-side SELECT routing is exercised in
//! `crates/basin-engine/tests/info_schema_pg_depend_authid_routing.rs`.

use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{
    info_schema::InfoSchemaQuery, Catalog, CvDef, InMemoryCatalog, SqlArgType, SqlFunctionArg,
    SqlFunctionDef, SqlFunctionLanguage, SqlReturnType,
};
use basin_common::{TableName, TenantId};

fn tname(s: &str) -> TableName {
    TableName::new(s).unwrap()
}

fn cv_schema() -> Schema {
    Schema::new(vec![
        Field::new("bucket", DataType::Int64, false),
        Field::new("total", DataType::Int64, false),
    ])
}

fn make_cv(source: &str) -> CvDef {
    CvDef {
        source_table: source.to_string(),
        query_sql: format!("SELECT bucket, sum(n) AS total FROM {source} GROUP BY bucket"),
        refresh_interval_secs: 60,
        last_refreshed_at_unix_ms: None,
        last_bucket_max_unix_ms: None,
    }
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

fn col_i32<'a>(b: &'a RecordBatch, n: &str) -> &'a Int32Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int32Array>().unwrap()
}

fn col_bool<'a>(b: &'a RecordBatch, n: &str) -> &'a BooleanArray {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
}

fn make_func(
    tenant: TenantId,
    name: &str,
    args: Vec<(&str, SqlArgType)>,
    ret: SqlArgType,
    body: &str,
) -> SqlFunctionDef {
    SqlFunctionDef {
        tenant,
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

#[tokio::test]
async fn pg_depend_includes_function_type_deps() {
    // Function with TEXT + BIGINT args returning BIGINT must produce at
    // least 3 rows: one per arg + one for the return type. All three
    // should reference pg_type as the catalog of the depended-on object.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.register_sql_function(make_func(
        t,
        "fmt_id",
        vec![("label", SqlArgType::Text), ("n", SqlArgType::BigInt)],
        SqlArgType::BigInt,
        "SELECT n",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_depend(&cat, &t).await.unwrap();
    assert!(
        batch.num_rows() >= 3,
        "expected >= 3 rows for fn(TEXT, BIGINT) RETURNS BIGINT, got {}",
        batch.num_rows()
    );

    // Collect (refobjid, deptype) per row and the unique refclassid.
    let refobjids = col_i64(&batch, "refobjid");
    let refclassids = col_i64(&batch, "refclassid");
    let deptypes = col_str(&batch, "deptype");
    let mut seen_type_oids = Vec::new();
    let mut seen_refclassids = std::collections::HashSet::new();
    for i in 0..batch.num_rows() {
        seen_type_oids.push(refobjids.value(i));
        seen_refclassids.insert(refclassids.value(i));
        assert_eq!(deptypes.value(i), "n", "v0.1 only emits 'n' (normal) deps");
    }
    // BIGINT (return + arg) → OID 20; TEXT → OID 25.
    assert!(
        seen_type_oids.contains(&20),
        "missing BIGINT (20): {seen_type_oids:?}"
    );
    assert!(
        seen_type_oids.contains(&25),
        "missing TEXT (25): {seen_type_oids:?}"
    );
    // refclassid is the synthetic pg_type catalog OID — it must be a
    // single stable value within the tenant.
    assert_eq!(
        seen_refclassids.len(),
        1,
        "all type-deps must point at the same pg_type catalog: {seen_refclassids:?}"
    );
    let pg_type_classid = *seen_refclassids.iter().next().unwrap();
    assert!(pg_type_classid > 0);
}

#[tokio::test]
async fn pg_depend_returns_empty_for_tenant_with_no_objects() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::pg_depend(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn pg_authid_one_row_per_tenant() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::pg_authid(&cat, &t).await.unwrap();
    assert_eq!(batch.num_rows(), 1, "exactly one row per tenant");
    assert_eq!(col_str(&batch, "rolname").value(0), &t.to_string());
    // Defaults: rolinherit + rolcanlogin true; everything else false.
    assert!(!col_bool(&batch, "rolsuper").value(0));
    assert!(col_bool(&batch, "rolinherit").value(0));
    assert!(!col_bool(&batch, "rolcreaterole").value(0));
    assert!(!col_bool(&batch, "rolcreatedb").value(0));
    assert!(col_bool(&batch, "rolcanlogin").value(0));
    assert!(!col_bool(&batch, "rolreplication").value(0));
    assert_eq!(col_i32(&batch, "rolconnlimit").value(0), -1);
}

#[tokio::test]
async fn pg_authid_oid_stable() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let b1 = InfoSchemaQuery::pg_authid(&cat, &t).await.unwrap();
    let b2 = InfoSchemaQuery::pg_authid(&cat, &t).await.unwrap();
    assert_eq!(col_i64(&b1, "oid").value(0), col_i64(&b2, "oid").value(0));
    assert!(col_i64(&b1, "oid").value(0) > 0);
}

#[tokio::test]
async fn pg_authid_cross_tenant_isolation() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();

    let ba = InfoSchemaQuery::pg_authid(&cat, &a).await.unwrap();
    let bb = InfoSchemaQuery::pg_authid(&cat, &b).await.unwrap();
    let oid_a = col_i64(&ba, "oid").value(0);
    let oid_b = col_i64(&bb, "oid").value(0);
    assert_ne!(
        oid_a, oid_b,
        "different tenants must hash to different oids"
    );

    let name_a = col_str(&ba, "rolname").value(0).to_string();
    let name_b = col_str(&bb, "rolname").value(0).to_string();
    assert_eq!(name_a, a.to_string());
    assert_eq!(name_b, b.to_string());
    // Each tenant only sees its own row.
    assert_eq!(ba.num_rows(), 1);
    assert_eq!(bb.num_rows(), 1);
    assert_ne!(name_a, name_b);
}

#[tokio::test]
async fn pg_authid_password_is_null() {
    // Credential leak invariant: rolpassword + rolvaliduntil must both be
    // SQL NULL. PG hashes are sensitive; Basin doesn't track per-role
    // passwords at all in v0.1 and surfacing anything here would be a
    // forward-compat foot-gun.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::pg_authid(&cat, &t).await.unwrap();
    let pw = col_str(&batch, "rolpassword");
    let vu = col_str(&batch, "rolvaliduntil");
    assert!(pw.is_null(0), "rolpassword must be NULL");
    assert!(vu.is_null(0), "rolvaliduntil must be NULL");
}

#[tokio::test]
async fn pg_depend_cv_source_table_edge() {
    // Register a continuous matview over a base table; pg_depend should
    // surface exactly one normal-deptype row whose objid is the matview's
    // pg_class oid and whose refobjid is the source table's pg_class oid.
    // Both ends use the same FNV-1a hash family pg_class consults, so the
    // oids must match the row pg_class returns for each name.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = cv_schema();
    cat.create_table(&t, &tname("events"), &s).await.unwrap();
    cat.create_table(&t, &tname("events_daily"), &s)
        .await
        .unwrap();
    cat.set_continuous_aggregate(&t, &tname("events_daily"), Some(make_cv("events")))
        .await
        .unwrap();

    // Resolve the expected oids via pg_class (same hash family pg_depend uses).
    let class = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    let class_relnames = col_str(&class, "relname");
    let class_oids = col_i64(&class, "oid");
    let mut cv_oid: Option<i64> = None;
    let mut src_oid: Option<i64> = None;
    for i in 0..class.num_rows() {
        match class_relnames.value(i) {
            "events_daily" => cv_oid = Some(class_oids.value(i)),
            "events" => src_oid = Some(class_oids.value(i)),
            _ => {}
        }
    }
    let cv_oid = cv_oid.expect("events_daily must appear in pg_class");
    let src_oid = src_oid.expect("events must appear in pg_class");

    // pg_depend should now have exactly one row matching our (cv, src) pair.
    let dep = InfoSchemaQuery::pg_depend(&cat, &t).await.unwrap();
    let objids = col_i64(&dep, "objid");
    let refobjids = col_i64(&dep, "refobjid");
    let classids = col_i64(&dep, "classid");
    let refclassids = col_i64(&dep, "refclassid");
    let objsubids = col_i32(&dep, "objsubid");
    let refobjsubids = col_i32(&dep, "refobjsubid");
    let deptypes = col_str(&dep, "deptype");

    let mut hits = 0usize;
    for i in 0..dep.num_rows() {
        if objids.value(i) == cv_oid && refobjids.value(i) == src_oid {
            hits += 1;
            assert_eq!(deptypes.value(i), "n", "CV→src dep must be normal");
            assert_eq!(objsubids.value(i), 0);
            assert_eq!(refobjsubids.value(i), 0);
            // classid and refclassid both reference pg_class — they must
            // be equal (same catalog table on both ends of the edge).
            assert_eq!(
                classids.value(i),
                refclassids.value(i),
                "CV→src classids must both point at pg_class"
            );
            assert!(
                classids.value(i) > 0,
                "pg_class catalog oid must be positive"
            );
        }
    }
    assert_eq!(hits, 1, "expected exactly one CV→source pg_depend edge");
}

#[tokio::test]
async fn pg_depend_cv_source_table_edge_pgdump_ordering_pattern() {
    // Simulate pg_dump's "find tables that views depend on" query, joining
    // pg_class as the matview side, pg_depend as the edge, and pg_class as
    // the source-table side. Done in Rust against the catalog batches
    // (engine-side SQL routing is covered separately) so the catalog API
    // contract is locked: oids in pg_depend must JOIN cleanly against
    // pg_class.oid for both endpoints.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = cv_schema();
    cat.create_table(&t, &tname("orders"), &s).await.unwrap();
    cat.create_table(&t, &tname("my_mv"), &s).await.unwrap();
    cat.set_continuous_aggregate(&t, &tname("my_mv"), Some(make_cv("orders")))
        .await
        .unwrap();

    let class = InfoSchemaQuery::pg_class(&cat, &t).await.unwrap();
    let dep = InfoSchemaQuery::pg_depend(&cat, &t).await.unwrap();

    // Build oid→relname index from pg_class.
    let class_relnames = col_str(&class, "relname");
    let class_oids = col_i64(&class, "oid");
    let mut by_oid: std::collections::HashMap<i64, &str> = std::collections::HashMap::new();
    for i in 0..class.num_rows() {
        by_oid.insert(class_oids.value(i), class_relnames.value(i));
    }

    // pg_dump pattern (in SQL):
    //   SELECT t.relname FROM pg_class t
    //   JOIN pg_depend d ON d.refobjid = t.oid
    //   JOIN pg_class v ON v.oid = d.objid
    //   WHERE v.relname = 'my_mv'
    let dep_objids = col_i64(&dep, "objid");
    let dep_refobjids = col_i64(&dep, "refobjid");
    let mut sources: Vec<&str> = Vec::new();
    for i in 0..dep.num_rows() {
        let (Some(v_name), Some(t_name)) = (
            by_oid.get(&dep_objids.value(i)),
            by_oid.get(&dep_refobjids.value(i)),
        ) else {
            continue;
        };
        if *v_name == "my_mv" {
            sources.push(*t_name);
        }
    }
    assert_eq!(
        sources,
        vec!["orders"],
        "pg_dump-style JOIN must resolve my_mv → orders via pg_depend"
    );
}

#[tokio::test]
async fn pg_depend_no_cv_no_extra_rows() {
    // Regression test: a tenant with only a function (no CVs) must still
    // produce exactly the function-type-dep rows — the CV expansion must
    // not add spurious rows when no continuous_aggregate is registered.
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let s = cv_schema();
    cat.create_table(&t, &tname("plain"), &s).await.unwrap();
    cat.register_sql_function(make_func(
        t,
        "id_pass",
        vec![("n", SqlArgType::BigInt)],
        SqlArgType::BigInt,
        "SELECT n",
    ))
    .await
    .unwrap();

    let batch = InfoSchemaQuery::pg_depend(&cat, &t).await.unwrap();
    // 1 arg + 1 return = 2 type-dep rows; no CV rows expected.
    assert_eq!(
        batch.num_rows(),
        2,
        "expected only function arg+return rows when no CV is registered"
    );
    let refclassids = col_i64(&batch, "refclassid");
    let mut seen = std::collections::HashSet::new();
    for i in 0..batch.num_rows() {
        seen.insert(refclassids.value(i));
    }
    // All rows here reference pg_type (function arg + return); no
    // pg_class-on-both-ends edge has snuck in.
    assert_eq!(
        seen.len(),
        1,
        "function-only deps should all share one refclassid (pg_type)"
    );
}

#[tokio::test]
async fn cross_tenant_isolation_cv_deps() {
    // Tenant A registers a CV; tenant B must see zero pg_depend rows
    // attributable to A. Cross-tenant leak is a P0 invariant.
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();
    let s = cv_schema();
    cat.create_table(&a, &tname("a_src"), &s).await.unwrap();
    cat.create_table(&a, &tname("a_mv"), &s).await.unwrap();
    cat.set_continuous_aggregate(&a, &tname("a_mv"), Some(make_cv("a_src")))
        .await
        .unwrap();

    // A sees its CV → src edge.
    let dep_a = InfoSchemaQuery::pg_depend(&cat, &a).await.unwrap();
    assert_eq!(
        dep_a.num_rows(),
        1,
        "tenant A should see exactly its CV→src row"
    );

    // B sees nothing — no tables, no functions, no CVs.
    let dep_b = InfoSchemaQuery::pg_depend(&cat, &b).await.unwrap();
    assert_eq!(
        dep_b.num_rows(),
        0,
        "tenant B must not see any rows attributable to A"
    );
}
