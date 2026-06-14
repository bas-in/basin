//! Catalog-side tests for `pg_catalog.pg_type` (Phase 5.11.M Tier 3).
//!
//! `pg_type` is a static catalog of PG built-in types Basin's pgwire
//! layer advertises. Engine-side SELECT routing is exercised in
//! `crates/basin-engine/tests/info_schema_pg_type_routing.rs`.

use arrow_array::{Array, BooleanArray, Int16Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::{info_schema::InfoSchemaQuery, Catalog, InMemoryCatalog};
use basin_common::ProjectId;

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

fn col_i16<'a>(b: &'a RecordBatch, n: &str) -> &'a Int16Array {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx).as_any().downcast_ref::<Int16Array>().unwrap()
}

fn col_bool<'a>(b: &'a RecordBatch, n: &str) -> &'a BooleanArray {
    let idx = b.schema().index_of(n).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
}

fn typname_to_oid(b: &RecordBatch, want: &str) -> Option<i64> {
    let names = col_str(b, "typname");
    let oids = col_i64(b, "oid");
    for i in 0..b.num_rows() {
        if names.value(i) == want {
            return Some(oids.value(i));
        }
    }
    None
}

#[tokio::test]
async fn pg_type_lists_basin_supported_types() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    let batch = InfoSchemaQuery::pg_type(&cat, &t).await.unwrap();
    assert!(
        batch.num_rows() >= 14,
        "expected at least 14 PG types, got {}",
        batch.num_rows()
    );

    // Schema sanity.
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "oid".to_string(),
            "typname".to_string(),
            "typnamespace".to_string(),
            "typtype".to_string(),
            "typcategory".to_string(),
            "typlen".to_string(),
            "typbyval".to_string(),
            // Added for psycopg / SQLAlchemy connect-time type-cache resolution.
            "typarray".to_string(),
            "typelem".to_string(),
            "typdelim".to_string(),
        ]
    );

    // Core types every Basin project needs.
    for required in [
        "bool",
        "int4",
        "int8",
        "text",
        "timestamptz",
        "jsonb",
        "uuid",
    ] {
        assert!(
            typname_to_oid(&batch, required).is_some(),
            "pg_type missing {required:?}"
        );
    }

    // Every row's `typtype` is `'b'` (base type) for v0.1.
    let typtype = col_str(&batch, "typtype");
    for i in 0..batch.num_rows() {
        assert_eq!(typtype.value(i), "b", "row {i} typtype should be 'b'");
    }

    // Spot-check a fixed-size pass-by-value row.
    let lens = col_i16(&batch, "typlen");
    let byval = col_bool(&batch, "typbyval");
    let names_arr = col_str(&batch, "typname");
    for i in 0..batch.num_rows() {
        if names_arr.value(i) == "bool" {
            assert_eq!(lens.value(i), 1);
            assert!(byval.value(i));
        }
        if names_arr.value(i) == "text" {
            assert_eq!(lens.value(i), -1);
            assert!(!byval.value(i));
        }
    }
}

/// The OIDs the static `pg_type` table publishes must agree with what the
/// router's `pg_type_oid_for_field` reports in `pg_attribute.atttypid` for
/// the same logical type. We can't import the router (catalog can't depend
/// on router), so this test pins the OIDs against the documented PG values
/// — the same source of truth `basin-router::types::arrow_to_pg_type`
/// reads.
#[tokio::test]
async fn pg_type_oids_match_router() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    let batch = InfoSchemaQuery::pg_type(&cat, &t).await.unwrap();

    // (typname, expected_oid) — pins the OIDs Basin's pgwire advertises
    // for the same logical types via `arrow_to_pg_type`.
    let expected: &[(&str, i64)] = &[
        ("bool", 16),
        ("int8", 20),
        ("int4", 23),
        ("text", 25),
        ("float8", 701),
        ("timestamptz", 1184),
        ("uuid", 2950),
        ("jsonb", 3802),
    ];
    for (name, oid) in expected {
        let got =
            typname_to_oid(&batch, name).unwrap_or_else(|| panic!("pg_type missing {name:?}"));
        assert_eq!(got, *oid, "{name} OID");
    }
}

#[tokio::test]
async fn pg_type_per_project_typnamespace_consistent() {
    let cat = InMemoryCatalog::new();
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();

    let b1 = InfoSchemaQuery::pg_type(&cat, &t).await.unwrap();
    let b2 = InfoSchemaQuery::pg_type(&cat, &t).await.unwrap();
    let nsp1 = col_i64(&b1, "typnamespace");
    let nsp2 = col_i64(&b2, "typnamespace");
    assert_eq!(b1.num_rows(), b2.num_rows());
    for i in 0..b1.num_rows() {
        assert_eq!(nsp1.value(i), nsp2.value(i), "typnamespace must be stable");
    }
    // Every row in a single batch must share the same namespace value
    // (single `pg_catalog` namespace per project).
    let first = nsp1.value(0);
    for i in 1..b1.num_rows() {
        assert_eq!(nsp1.value(i), first, "all rows share one typnamespace");
    }

    // A different project must hash to a different namespace value.
    let t2 = ProjectId::new();
    let b3 = InfoSchemaQuery::pg_type(&cat, &t2).await.unwrap();
    let nsp3 = col_i64(&b3, "typnamespace");
    assert_ne!(
        nsp1.value(0),
        nsp3.value(0),
        "distinct projects must produce distinct typnamespace hashes"
    );
}
