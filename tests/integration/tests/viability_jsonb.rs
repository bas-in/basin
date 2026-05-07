//! Viability test: `JSONB` column type end-to-end through the SQL surface.
//!
//! Card: `viability_jsonb`
//! Bar: `roundtrip_passed == true && canonical_form_observed == true`
//!
//! Goal of v0.1: prove the basic JSONB pipeline holds together.
//!
//! 1. CREATE TABLE with a `JSONB` column.
//! 2. INSERT two rows whose payload literals are JSON strings written in
//!    *different* key orders. The engine canonicalises (sorts keys, strips
//!    whitespace) before storing — so two semantically-equal documents
//!    produce byte-identical Parquet payloads, the property the v0.2 `@>`
//!    containment operator will key off.
//! 3. SELECT both rows back, parse the payload bytes, and assert:
//!    - all rows visible
//!    - the parsed JSON contains the values the user supplied
//!    - the *bytes* on the wire match a canonical-form re-serialisation,
//!      which proves the canonicalisation actually fired (rather than the
//!      bytes happening to round-trip through `serde_json::Value`)
//!
//! Path operators (`->`, `->>`, `@>`) are explicitly out of scope here —
//! see the TODOs at the bottom of this file for the v0.2 plan.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, LargeBinaryArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Re-serialise `v` as canonical-form JSON. Mirrors what the engine's INSERT
/// path does (basin_engine::dml::canonicalize_json + serde_json::to_vec):
/// objects' keys are sorted alphabetically, no insignificant whitespace.
/// Used by the test as the ground-truth oracle the engine's stored bytes
/// must match byte-for-byte.
fn canonicalize(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<String, Value> = map
                .into_iter()
                .map(|(k, val)| (k, canonicalize(val)))
                .collect();
            let mut out = serde_json::Map::with_capacity(sorted.len());
            for (k, vv) in sorted {
                out.insert(k, vv);
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

fn canonical_bytes(s: &str) -> Vec<u8> {
    let v: serde_json::Value = serde_json::from_str(s).expect("test JSON parse");
    serde_json::to_vec(&canonicalize(v)).expect("test JSON re-serialise")
}

#[tokio::test]
async fn viability_jsonb() {
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
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let sess = engine.open_session(tenant.clone()).await.unwrap();

    // Step 1 — CREATE TABLE with a JSONB column. JSONB has no dedicated
    // Arrow `DataType`, so the engine plants a `BASIN_TYPE=JSONB` marker on
    // the field's metadata; the underlying storage type is `LargeBinary`.
    sess.execute("CREATE TABLE docs (id BIGINT NOT NULL, payload JSONB)")
        .await
        .expect("CREATE TABLE docs");

    // Catalog sanity: the column lands as LargeBinary with the JSONB tag.
    let table = TableName::new("docs").unwrap();
    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    let payload_field = meta
        .schema
        .field_with_name("payload")
        .expect("payload column missing");
    assert_eq!(
        payload_field.data_type(),
        &DataType::LargeBinary,
        "JSONB should map to LargeBinary at the Arrow layer; got {:?}",
        payload_field.data_type()
    );
    assert_eq!(
        payload_field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("JSONB"),
        "payload field is missing the BASIN_TYPE=JSONB marker"
    );

    // Step 2 — INSERT two rows. Both write the same logical fields but in
    // *different* key orders; canonicalisation should produce identical
    // shapes for matching documents (and we'll use that property below).
    sess.execute(r#"INSERT INTO docs VALUES (1, '{"name":"alice","tags":["a","b"]}')"#)
        .await
        .expect("INSERT row 1");
    sess.execute(r#"INSERT INTO docs VALUES (2, '{"tags":["c"],"name":"bob"}')"#)
        .await
        .expect("INSERT row 2");

    // Step 3 — SELECT them back, ordered by id so row 0 is alice and row 1
    // is bob regardless of physical file ordering.
    let result = sess
        .execute("SELECT id, payload FROM docs ORDER BY id")
        .await
        .expect("SELECT");

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!(
            "expected ExecResult::Rows for SELECT, got Empty with tag={tag:?}"
        ),
    };

    let mut rows: Vec<(i64, Vec<u8>)> = Vec::new();
    for batch in batches.iter() {
        let id_arr = batch
            .column_by_name("id")
            .expect("id column missing")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column not Int64Array");
        let payload_arr = batch
            .column_by_name("payload")
            .expect("payload column missing")
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("payload column not LargeBinaryArray");
        for r in 0..batch.num_rows() {
            let id = id_arr.value(r);
            assert!(
                !payload_arr.is_null(r),
                "payload was NULL at row {r}; expected populated JSON"
            );
            rows.push((id, payload_arr.value(r).to_vec()));
        }
    }

    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows.len(), 2, "expected 2 rows back, got {}", rows.len());

    // Parse the stored bytes back into JSON values for the semantic checks.
    let alice_json: serde_json::Value =
        serde_json::from_slice(&rows[0].1).expect("alice payload not valid JSON");
    let bob_json: serde_json::Value =
        serde_json::from_slice(&rows[1].1).expect("bob payload not valid JSON");

    // Logical content checks (the "round-trip passed" half of the bar).
    assert_eq!(rows[0].0, 1, "first row should be id=1");
    assert_eq!(alice_json["name"], json!("alice"));
    assert_eq!(alice_json["tags"], json!(["a", "b"]));
    assert_eq!(rows[1].0, 2, "second row should be id=2");
    assert_eq!(bob_json["name"], json!("bob"));
    assert_eq!(bob_json["tags"], json!(["c"]));

    // Canonical-form check (the "canonical form observed" half).
    //
    // Both rows' bytes must match what `canonical_bytes` produces — same
    // keys-sorted, no-whitespace serialisation. This is what makes JSONB
    // useful as a deduplication / equality key: two semantically-equal
    // documents written in different orders produce byte-identical Parquet
    // cells. The bob INSERT proves it: the user wrote `{"tags":...,"name":...}`
    // (keys descending) but the cell on disk has `{"name":...,"tags":...}`
    // (alphabetical).
    let alice_canonical = canonical_bytes(r#"{"name":"alice","tags":["a","b"]}"#);
    let bob_canonical = canonical_bytes(r#"{"tags":["c"],"name":"bob"}"#);
    assert_eq!(
        rows[0].1, alice_canonical,
        "alice's stored bytes don't match canonical form;\n  got: {:?}\n  expected: {:?}",
        String::from_utf8_lossy(&rows[0].1),
        String::from_utf8_lossy(&alice_canonical),
    );
    assert_eq!(
        rows[1].1, bob_canonical,
        "bob's stored bytes don't match canonical form;\n  got: {:?}\n  expected: {:?}",
        String::from_utf8_lossy(&rows[1].1),
        String::from_utf8_lossy(&bob_canonical),
    );

    // Bonus check: bob's stored bytes must start with `{"name"` (not
    // `{"tags"`) — this is the load-bearing observable proof that
    // canonicalisation rearranged the keys we wrote.
    assert!(
        rows[1].1.starts_with(br#"{"name""#),
        "bob's bytes should start with `{{\"name\"` after key sort; got {:?}",
        String::from_utf8_lossy(&rows[1].1),
    );

    // Bonus check: invalid JSON literals should be rejected with a clean
    // `InvalidSchema` error, not an internal panic. The "}{" between the
    // braces makes it obviously malformed.
    let bad = sess
        .execute(r#"INSERT INTO docs VALUES (99, '{"x":}{')"#)
        .await;
    assert!(bad.is_err(), "malformed JSON should be rejected on INSERT");

    // Bonus check: NULL is allowed (column has no NOT NULL).
    sess.execute("INSERT INTO docs VALUES (3, NULL)")
        .await
        .expect("NULL JSONB INSERT should succeed");

    let roundtrip_passed = true;
    let canonical_form_observed =
        rows[1].1.starts_with(br#"{"name""#) && rows[1].1 == bob_canonical;
    let pass = roundtrip_passed && canonical_form_observed;

    println!(
        "[VIABILITY JSONB] roundtrip={roundtrip_passed} canonical={canonical_form_observed} \
         alice_bytes={alice_bytes:?} bob_bytes={bob_bytes:?}",
        alice_bytes = String::from_utf8_lossy(&rows[0].1),
        bob_bytes = String::from_utf8_lossy(&rows[1].1),
    );

    report_viability(
        "jsonb",
        "JSONB column type",
        "JSONB column accepts JSON-string literals on INSERT, stores them \
         canonically (keys sorted, no whitespace) as LargeBinary, and \
         returns them on SELECT as binary bytes that round-trip through \
         serde_json. Different key orders on INSERT produce byte-identical \
         stored payloads.",
        pass,
        PrimaryMetric {
            label: "jsonb_roundtrip_and_canonical".into(),
            value: if pass { 1.0 } else { 0.0 },
            unit: "boolean".into(),
            bar: BarOp::eq(1.0),
        },
        json!({
            "rows_returned": rows.len(),
            "alice_bytes": String::from_utf8_lossy(&rows[0].1),
            "bob_bytes": String::from_utf8_lossy(&rows[1].1),
            "alice_canonical": String::from_utf8_lossy(&alice_canonical),
            "bob_canonical": String::from_utf8_lossy(&bob_canonical),
            "rejected_malformed": true,
            "null_accepted": true,
        }),
    );

    assert!(pass, "JSONB viability bar not met (roundtrip={roundtrip_passed}, canonical={canonical_form_observed})");
}

// TODO(v0.2): JSONB path operators and constructors.
//
// - `payload -> 'name'`  → JSONB sub-document extract (returns JSONB)
// - `payload ->> 'name'` → text extract (returns TEXT)
// - `payload @> '{...}'` → containment predicate (returns BOOL); cheap on
//   canonical bytes because it reduces to a recursive subset check on
//   the parsed `serde_json::Value` (and later: a Parquet bloom-filter
//   accelerated path on a per-leaf-key index).
// - `jsonb_build_object('k', v, ...)`            → constructor UDF
// - `jsonb_array_length(payload->'tags')`        → length scalar
// - `jsonb_path_exists(payload, '$.tags[*] ? (@ == "b")')` — the SQL/JSON
//   path operator family. Lower priority; gated on a JSONPath crate.
//
// Indexing on JSONB paths is the v0.3 step: Parquet doesn't have a native
// JSONB row group statistic, so the cheap-and-cheerful first cut is to
// project a few hot path expressions out as separate columns at INSERT
// time (`payload->>'tenant_id'` mat-view-style) and let the existing
// bloom-filter / row-group min-max prune those.
