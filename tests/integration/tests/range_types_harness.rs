//! Phase 5.24.A — range types + GIST test harness.
//!
//! **Task:** TASK.md Phase 5.24.A — range types + GIST harness, RED.
//! **Test-first convention:** this file lands BEFORE any range-type or GIST
//! implementation. Every test is `#[ignore]`d until the implementation tasks
//! 5.24.B–F close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                              | Slice                                    | Closed by |
//! |--------------------------------------|------------------------------------------|-----------|
//! | `range_type_round_trip`              | type round-trip                          | 5.24.B    |
//! | `range_operators_containment`        | operator semantics — containment/overlap | 5.24.C    |
//! | `range_operators_positional`         | operator semantics — positional          | 5.24.D    |
//! | `range_union_intersection`           | operator semantics — set ops             | 5.24.D    |
//! | `range_gist_index`                   | GIST perf                                | 5.24.E    |
//! | `range_exclusion_constraint_scheduling` | scheduling pattern                    | 5.24.F    |
//!
//! ## How to flip a slice green
//!
//! Once the corresponding 5.24.B–F task lands, drop the `#[ignore]` on the
//! relevant test (or replace it with a targeted `#[ignore = "gated on #NNN"]`
//! if only part of the slice is resolved), and verify that
//! `cargo test --test range_types_harness` passes without `--ignored`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helper ────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Run `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results so that assertion failures are
/// as close to the problematic SQL as possible.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return the first boolean scalar from the first row/column.
/// Panics if the result is not a single-cell boolean result.
async fn scalar_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("expected BooleanArray for boolean result");
            arr.value(0)
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — Type round-trip
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that all six PG built-in range types are recognised as column types:
//   int4range, int8range, numrange, tsrange, tstzrange, daterange
//
// Each type is created as a column, a representative literal is inserted, and
// the row is selected back to confirm the engine accepted the DDL and DML
// without an error.  The assertion does not inspect the wire encoding (that is
// the pgwire harness's concern); it only checks that:
//   1. CREATE TABLE with the range column succeeds.
//   2. INSERT with a well-formed range literal succeeds.
//   3. SELECT returns at least one row (non-empty result).
//
// Closed by: 5.24.B (parser + type-system recognition for all six range types).
//
// RED until 5.24.B: the engine does not yet recognise int4range/int8range/
// numrange/tsrange/tstzrange/daterange as column types; CREATE TABLE will fail
// with an unknown-type error.

/// Phase 5.24.A — round-trip for all six built-in PG range types.
///
/// Creates a table with one column per range type, inserts a representative
/// literal for each, and asserts the SELECT returns one row.  Validates that
/// the pgwire parser and the engine type system accept every range type.
///
/// Six types exercised:
///   int4range  — integer range (int4 bounds)
///   int8range  — big-integer range (int8 bounds)
///   numrange   — arbitrary-precision numeric range
///   tsrange    — timestamp without time zone range
///   tstzrange  — timestamp with time zone range
///   daterange  — date range
///
/// Closed by: 5.24.B (range type recognition in parser + catalog).
#[tokio::test]
async fn range_type_round_trip() {
    // Slice: "type round-trip"
    //
    // IMPORTANT: every assertion here reflects correct PostgreSQL semantics.
    // Do NOT weaken them to match Basin's current (wrong) output — fix the
    // engine in 5.24.B and let the assertions go green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── int4range ─────────────────────────────────────────────────────────────
    // PG notation: '[1,10)' — inclusive lower, exclusive upper, integers.
    sess.execute(
        "CREATE TABLE rt_int4 (id BIGSERIAL PRIMARY KEY, r int4range NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with int4range column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute("INSERT INTO rt_int4 (r) VALUES ('[1,10)')")
        .await
        .expect(
            "RANGE ROUND-TRIP: INSERT '[1,10)' into int4range column must succeed. \
             Closed by 5.24.B.",
        );

    let n4 = row_count(&sess, "SELECT r FROM rt_int4").await;
    assert_eq!(
        n4, 1,
        "RANGE ROUND-TRIP: SELECT from int4range table must return 1 row. \
         got={n4}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] int4range: {n4} row returned (expected 1)");

    // ── int8range ─────────────────────────────────────────────────────────────
    // PG notation: '[100,200)' — int8 (bigint) bounds.
    sess.execute(
        "CREATE TABLE rt_int8 (id BIGSERIAL PRIMARY KEY, r int8range NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with int8range column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute("INSERT INTO rt_int8 (r) VALUES ('[100,200)')")
        .await
        .expect(
            "RANGE ROUND-TRIP: INSERT '[100,200)' into int8range column must succeed. \
             Closed by 5.24.B.",
        );

    let n8 = row_count(&sess, "SELECT r FROM rt_int8").await;
    assert_eq!(
        n8, 1,
        "RANGE ROUND-TRIP: SELECT from int8range table must return 1 row. \
         got={n8}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] int8range: {n8} row returned (expected 1)");

    // ── numrange ──────────────────────────────────────────────────────────────
    // PG notation: '[1.5,2.5)' — arbitrary-precision numeric bounds.
    sess.execute(
        "CREATE TABLE rt_num (id BIGSERIAL PRIMARY KEY, r numrange NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with numrange column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute("INSERT INTO rt_num (r) VALUES ('[1.5,2.5)')")
        .await
        .expect(
            "RANGE ROUND-TRIP: INSERT '[1.5,2.5)' into numrange column must succeed. \
             Closed by 5.24.B.",
        );

    let nnum = row_count(&sess, "SELECT r FROM rt_num").await;
    assert_eq!(
        nnum, 1,
        "RANGE ROUND-TRIP: SELECT from numrange table must return 1 row. \
         got={nnum}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] numrange: {nnum} row returned (expected 1)");

    // ── tsrange ───────────────────────────────────────────────────────────────
    // PG notation: '[2024-01-01 00:00:00,2024-01-02 00:00:00)' — timestamp (no tz).
    sess.execute(
        "CREATE TABLE rt_ts (id BIGSERIAL PRIMARY KEY, r tsrange NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with tsrange column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute(
        "INSERT INTO rt_ts (r) VALUES \
         ('[2024-01-01 00:00:00,2024-01-02 00:00:00)')",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: INSERT tsrange literal must succeed. \
         Closed by 5.24.B.",
    );

    let nts = row_count(&sess, "SELECT r FROM rt_ts").await;
    assert_eq!(
        nts, 1,
        "RANGE ROUND-TRIP: SELECT from tsrange table must return 1 row. \
         got={nts}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] tsrange: {nts} row returned (expected 1)");

    // ── tstzrange ─────────────────────────────────────────────────────────────
    // PG notation: '[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)' — timestamp with tz.
    sess.execute(
        "CREATE TABLE rt_tstz (id BIGSERIAL PRIMARY KEY, r tstzrange NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with tstzrange column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute(
        "INSERT INTO rt_tstz (r) VALUES \
         ('[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)')",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: INSERT tstzrange literal must succeed. \
         Closed by 5.24.B.",
    );

    let ntstz = row_count(&sess, "SELECT r FROM rt_tstz").await;
    assert_eq!(
        ntstz, 1,
        "RANGE ROUND-TRIP: SELECT from tstzrange table must return 1 row. \
         got={ntstz}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] tstzrange: {ntstz} row returned (expected 1)");

    // ── daterange ─────────────────────────────────────────────────────────────
    // PG notation: '[2024-01-01,2024-01-31)' — date (not timestamp) bounds.
    sess.execute(
        "CREATE TABLE rt_date (id BIGSERIAL PRIMARY KEY, r daterange NOT NULL)",
    )
    .await
    .expect(
        "RANGE ROUND-TRIP: CREATE TABLE with daterange column must succeed. \
         Closed by 5.24.B.",
    );

    sess.execute("INSERT INTO rt_date (r) VALUES ('[2024-01-01,2024-01-31)')")
        .await
        .expect(
            "RANGE ROUND-TRIP: INSERT '[2024-01-01,2024-01-31)' into daterange column \
             must succeed. Closed by 5.24.B.",
        );

    let ndate = row_count(&sess, "SELECT r FROM rt_date").await;
    assert_eq!(
        ndate, 1,
        "RANGE ROUND-TRIP: SELECT from daterange table must return 1 row. \
         got={ndate}. Closed by 5.24.B."
    );
    println!("[5.24.A round-trip] daterange: {ndate} row returned (expected 1)");

    // ── Canonicalization discriminator ─────────────────────────────────────────
    // This assertion distinguishes a *genuine* range type from a text/opaque
    // fallback (where '[1,10)' is just stored as a string). PG canonicalizes
    // ranges over discrete types (int4, int8, date): the inclusive-upper form
    // '[1,9]'::int4range is normalized to the half-open form '[1,10)'. Hence:
    //
    //   '[1,9]'::int4range = '[1,10)'::int4range  → TRUE  (same canonical range)
    //
    // A text fallback compares the raw literals ('[1,9]' vs '[1,10)') and yields
    // FALSE, or rejects the typed equality outright. Only a real int4range
    // implementation (5.24.B) returns TRUE here.
    let canonical_eq = scalar_bool(
        &sess,
        "SELECT '[1,9]'::int4range = '[1,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A round-trip] '[1,9]'::int4range = '[1,10)'::int4range → {canonical_eq} \
         (expected true — discrete-range canonicalization)"
    );
    assert!(
        canonical_eq,
        "RANGE ROUND-TRIP: '[1,9]'::int4range = '[1,10)'::int4range must be TRUE — \
         PG canonicalizes discrete int4ranges to the half-open '[lo,hi)' form, so the \
         inclusive-upper '[1,9]' equals '[1,10)'. A text/opaque fallback fails this. \
         Closed by 5.24.B."
    );

    // Same discriminator for daterange (also a discrete type): '[2024-01-01,2024-01-30]'
    // canonicalizes to '[2024-01-01,2024-01-31)'.
    let date_canonical_eq = scalar_bool(
        &sess,
        "SELECT '[2024-01-01,2024-01-30]'::daterange = '[2024-01-01,2024-01-31)'::daterange",
    )
    .await;
    println!(
        "[5.24.A round-trip] daterange canonicalization eq → {date_canonical_eq} (expected true)"
    );
    assert!(
        date_canonical_eq,
        "RANGE ROUND-TRIP: '[2024-01-01,2024-01-30]'::daterange must equal \
         '[2024-01-01,2024-01-31)'::daterange — daterange canonicalizes to half-open form. \
         Closed by 5.24.B."
    );

    println!(
        "[5.24.A round-trip] PASSED — all 6 range types (int4range, int8range, numrange, \
         tsrange, tstzrange, daterange) round-tripped, and discrete ranges canonicalize correctly"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — Operator semantics: containment and overlap
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies the three PG range containment/overlap operators against known
// values, mirroring the PG spec exactly:
//
//   @>  (contains element or range)
//      '[1,10)'::int4range @> 5         → true   (5 is inside [1,10))
//      '[1,10)'::int4range @> '[2,8)'   → true   ([2,8) ⊆ [1,10))
//      '[1,10)'::int4range @> '[2,15)'  → false  (15 outside [1,10))
//
//   <@  (element/range is contained by)
//      5 <@ '[1,10)'::int4range         → true
//      '[2,8)'::int4range <@ '[1,10)'   → true
//      '[2,15)'::int4range <@ '[1,10)'  → false
//
//   &&  (ranges overlap)
//      '[1,5)'::int4range && '[3,8)'    → true   (overlap at [3,5))
//      '[1,5)'::int4range && '[5,10)'   → false  (adjacent, no overlap — [1,5) ends before 5)
//      '[1,5)'::int4range && '[6,10)'   → false  (disjoint)
//
// Closed by: 5.24.C (containment/overlap operator implementation).
//
// RED until 5.24.C: the engine does not yet implement these operators for
// range types; every scalar_bool call below will panic with an engine error.

/// Phase 5.24.A — range operator semantics: `@>`, `<@`, `&&`.
///
/// Asserts correct PG semantics for containment (element-in-range, range-in-range)
/// and overlap across integer ranges.  The expected true/false values are derived
/// directly from the PostgreSQL documentation and verified against a real PG
/// instance.
///
/// Closed by: 5.24.C (containment + overlap operators for all range types).
#[tokio::test]
async fn range_operators_containment() {
    // Slice: "operator semantics — containment/overlap"
    //
    // IMPORTANT: every assertion reflects correct PG semantics. Do NOT weaken
    // them to match Basin's current output — implement 5.24.C instead.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── @> element containment ────────────────────────────────────────────────
    // '[1,10)'::int4range @> 5 → true  (5 ∈ [1,10))
    let contains_elem_true = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range @> 5",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,10)' @> 5 → {contains_elem_true} (expected true)"
    );
    assert!(
        contains_elem_true,
        "CONTAINMENT(@>): '[1,10)'::int4range @> 5 must be TRUE (5 is inside [1,10)). \
         Closed by 5.24.C."
    );

    // '[1,10)'::int4range @> 10 → false  (upper bound is exclusive)
    let contains_elem_false = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range @> 10",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,10)' @> 10 → {contains_elem_false} (expected false)"
    );
    assert!(
        !contains_elem_false,
        "CONTAINMENT(@>): '[1,10)'::int4range @> 10 must be FALSE \
         (10 is the exclusive upper bound). Closed by 5.24.C."
    );

    // ── @> range containment ──────────────────────────────────────────────────
    // '[1,10)'::int4range @> '[2,8)' → true  ([2,8) ⊆ [1,10))
    let contains_sub_true = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range @> '[2,8)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,10)' @> '[2,8)' → {contains_sub_true} (expected true)"
    );
    assert!(
        contains_sub_true,
        "CONTAINMENT(@>): '[1,10)' @> '[2,8)' must be TRUE ([2,8) ⊆ [1,10)). \
         Closed by 5.24.C."
    );

    // '[1,10)'::int4range @> '[2,15)' → false  (15 exceeds upper bound)
    let contains_sub_false = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range @> '[2,15)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,10)' @> '[2,15)' → {contains_sub_false} (expected false)"
    );
    assert!(
        !contains_sub_false,
        "CONTAINMENT(@>): '[1,10)' @> '[2,15)' must be FALSE (15 exceeds [1,10)). \
         Closed by 5.24.C."
    );

    // ── <@ element containment (reverse) ─────────────────────────────────────
    // 5 <@ '[1,10)'::int4range → true
    let elem_in_range = scalar_bool(
        &sess,
        "SELECT 5 <@ '[1,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] 5 <@ '[1,10)' → {elem_in_range} (expected true)"
    );
    assert!(
        elem_in_range,
        "CONTAINMENT(<@): 5 <@ '[1,10)' must be TRUE. Closed by 5.24.C."
    );

    // '[2,8)'::int4range <@ '[1,10)' → true
    let range_in_range = scalar_bool(
        &sess,
        "SELECT '[2,8)'::int4range <@ '[1,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[2,8)' <@ '[1,10)' → {range_in_range} (expected true)"
    );
    assert!(
        range_in_range,
        "CONTAINMENT(<@): '[2,8)' <@ '[1,10)' must be TRUE. Closed by 5.24.C."
    );

    // '[2,15)'::int4range <@ '[1,10)' → false
    let range_not_in_range = scalar_bool(
        &sess,
        "SELECT '[2,15)'::int4range <@ '[1,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[2,15)' <@ '[1,10)' → {range_not_in_range} (expected false)"
    );
    assert!(
        !range_not_in_range,
        "CONTAINMENT(<@): '[2,15)' <@ '[1,10)' must be FALSE (15 > 10). \
         Closed by 5.24.C."
    );

    // ── && overlap ────────────────────────────────────────────────────────────
    // '[1,5)'::int4range && '[3,8)' → true  (overlap at [3,5))
    let overlap_true = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range && '[3,8)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,5)' && '[3,8)' → {overlap_true} (expected true)"
    );
    assert!(
        overlap_true,
        "OVERLAP(&&): '[1,5)' && '[3,8)' must be TRUE (overlap at [3,5)). \
         Closed by 5.24.C."
    );

    // '[1,5)'::int4range && '[5,10)' → false  (adjacent ranges do NOT overlap)
    // PG semantics: [1,5) ends at 5 (exclusive), [5,10) starts at 5 (inclusive).
    // No point is in both ranges.
    let overlap_adjacent = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range && '[5,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,5)' && '[5,10)' → {overlap_adjacent} (expected false, adjacent)"
    );
    assert!(
        !overlap_adjacent,
        "OVERLAP(&&): '[1,5)' && '[5,10)' must be FALSE — adjacent ranges do not overlap \
         in PG range semantics. Closed by 5.24.C."
    );

    // '[1,5)'::int4range && '[6,10)' → false  (disjoint)
    let overlap_disjoint = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range && '[6,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A containment] '[1,5)' && '[6,10)' → {overlap_disjoint} (expected false)"
    );
    assert!(
        !overlap_disjoint,
        "OVERLAP(&&): '[1,5)' && '[6,10)' must be FALSE (disjoint ranges). \
         Closed by 5.24.C."
    );

    println!(
        "[5.24.A containment] PASSED — @>, <@, && semantics all match PG expectations"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — Operator semantics: positional
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies the five PG range positional operators against known values:
//
//   <<  (strictly left of)
//      '[1,5)'  << '[7,10)' → true   (5 ≤ 7, and ranges are disjoint on the left)
//      '[1,5)'  << '[4,10)' → false  (overlap)
//
//   >>  (strictly right of)
//      '[7,10)' >> '[1,5)'  → true
//      '[4,10)' >> '[1,5)'  → false
//
//   &<  (does not extend to the right of)
//      '[1,5)'  &< '[3,10)' → true   (upper of LHS ≤ upper of RHS)
//      '[1,10)' &< '[3,7)'  → false  (upper of LHS > upper of RHS)
//
//   &>  (does not extend to the left of)
//      '[5,10)' &> '[1,7)'  → true   (lower of LHS ≥ lower of RHS)
//      '[1,10)' &> '[3,7)'  → false  (lower of LHS < lower of RHS)
//
//   -|- (adjacent)
//      '[1,5)'  -|- '[5,10)' → true  (touch at 5, no gap/overlap)
//      '[1,5)'  -|- '[6,10)' → false (gap at 5)
//
// Closed by: 5.24.D (positional range operator implementation).
//
// RED until 5.24.D: the engine does not yet implement <<, >>, &<, &>, -|-.

/// Phase 5.24.A — range operator semantics: `<<`, `>>`, `&<`, `&>`, `-|-`.
///
/// Asserts correct PG semantics for all five positional range operators on
/// int4range values.  Expected booleans are derived from the PostgreSQL
/// documentation and verified against a real PG instance.
///
/// Closed by: 5.24.D (positional operators for range types).
#[tokio::test]
async fn range_operators_positional() {
    // Slice: "operator semantics — positional"
    //
    // IMPORTANT: every assertion reflects correct PG semantics. Do NOT weaken
    // them to match Basin's current output — implement 5.24.D instead.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── << strictly left of ───────────────────────────────────────────────────
    // '[1,5)' << '[7,10)' → true  (entirely left: max(LHS)=4 < min(RHS)=7)
    let strict_left_true = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range << '[7,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,5)' << '[7,10)' → {strict_left_true} (expected true)"
    );
    assert!(
        strict_left_true,
        "POSITIONAL(<<): '[1,5)' << '[7,10)' must be TRUE. Closed by 5.24.D."
    );

    // '[1,5)' << '[4,10)' → false  (overlap at 4)
    let strict_left_false = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range << '[4,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,5)' << '[4,10)' → {strict_left_false} (expected false)"
    );
    assert!(
        !strict_left_false,
        "POSITIONAL(<<): '[1,5)' << '[4,10)' must be FALSE (overlap). Closed by 5.24.D."
    );

    // ── >> strictly right of ──────────────────────────────────────────────────
    // '[7,10)' >> '[1,5)' → true
    let strict_right_true = scalar_bool(
        &sess,
        "SELECT '[7,10)'::int4range >> '[1,5)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[7,10)' >> '[1,5)' → {strict_right_true} (expected true)"
    );
    assert!(
        strict_right_true,
        "POSITIONAL(>>): '[7,10)' >> '[1,5)' must be TRUE. Closed by 5.24.D."
    );

    // '[4,10)' >> '[1,5)' → false  (overlap at [4,5))
    let strict_right_false = scalar_bool(
        &sess,
        "SELECT '[4,10)'::int4range >> '[1,5)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[4,10)' >> '[1,5)' → {strict_right_false} (expected false)"
    );
    assert!(
        !strict_right_false,
        "POSITIONAL(>>): '[4,10)' >> '[1,5)' must be FALSE (overlap). Closed by 5.24.D."
    );

    // ── &< does not extend to the right of ───────────────────────────────────
    // '[1,5)' &< '[3,10)' → true   (upper(LHS)=5 ≤ upper(RHS)=10)
    let not_right_true = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range &< '[3,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,5)' &< '[3,10)' → {not_right_true} (expected true)"
    );
    assert!(
        not_right_true,
        "POSITIONAL(&<): '[1,5)' &< '[3,10)' must be TRUE (LHS upper ≤ RHS upper). \
         Closed by 5.24.D."
    );

    // '[1,10)' &< '[3,7)' → false  (upper(LHS)=10 > upper(RHS)=7)
    let not_right_false = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range &< '[3,7)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,10)' &< '[3,7)' → {not_right_false} (expected false)"
    );
    assert!(
        !not_right_false,
        "POSITIONAL(&<): '[1,10)' &< '[3,7)' must be FALSE (LHS extends past RHS upper). \
         Closed by 5.24.D."
    );

    // ── &> does not extend to the left of ────────────────────────────────────
    // '[5,10)' &> '[1,7)' → true   (lower(LHS)=5 ≥ lower(RHS)=1)
    let not_left_true = scalar_bool(
        &sess,
        "SELECT '[5,10)'::int4range &> '[1,7)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[5,10)' &> '[1,7)' → {not_left_true} (expected true)"
    );
    assert!(
        not_left_true,
        "POSITIONAL(&>): '[5,10)' &> '[1,7)' must be TRUE (LHS lower ≥ RHS lower). \
         Closed by 5.24.D."
    );

    // '[1,10)' &> '[3,7)' → false  (lower(LHS)=1 < lower(RHS)=3)
    let not_left_false = scalar_bool(
        &sess,
        "SELECT '[1,10)'::int4range &> '[3,7)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,10)' &> '[3,7)' → {not_left_false} (expected false)"
    );
    assert!(
        !not_left_false,
        "POSITIONAL(&>): '[1,10)' &> '[3,7)' must be FALSE (LHS extends left of RHS). \
         Closed by 5.24.D."
    );

    // ── -|- adjacent ──────────────────────────────────────────────────────────
    // '[1,5)' -|- '[5,10)' → true  (touch at 5 with no gap — [1,5) ends just before 5,
    //                                 [5,10) starts at 5; together they cover [1,10))
    let adjacent_true = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range -|- '[5,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,5)' -|- '[5,10)' → {adjacent_true} (expected true)"
    );
    assert!(
        adjacent_true,
        "POSITIONAL(-|-): '[1,5)' -|- '[5,10)' must be TRUE (adjacent, no gap/overlap). \
         Closed by 5.24.D."
    );

    // '[1,5)' -|- '[6,10)' → false  (gap of 1 between 4 and 6)
    let adjacent_false = scalar_bool(
        &sess,
        "SELECT '[1,5)'::int4range -|- '[6,10)'::int4range",
    )
    .await;
    println!(
        "[5.24.A positional] '[1,5)' -|- '[6,10)' → {adjacent_false} (expected false)"
    );
    assert!(
        !adjacent_false,
        "POSITIONAL(-|-): '[1,5)' -|- '[6,10)' must be FALSE (gap between ranges). \
         Closed by 5.24.D."
    );

    println!(
        "[5.24.A positional] PASSED — <<, >>, &<, &>, -|- semantics all match PG expectations"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — Operator semantics: set operations (+, *, -)
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies PG range set-operation operators:
//
//   +  (union)
//      '[1,5)'::int4range + '[3,10)' → '[1,10)'  (merged overlapping ranges)
//      Requires: ranges must overlap or be adjacent, else PG raises an error.
//
//   *  (intersection)
//      '[1,10)'::int4range * '[3,7)' → '[3,7)'   (common sub-range)
//      '[1,5)'::int4range  * '[7,10)' → empty    (disjoint)
//
//   -  (difference)
//      '[1,10)'::int4range - '[3,7)' → error/empty  (non-contiguous remainder)
//      '[1,10)'::int4range - '[1,5)' → '[5,10)'    (right remainder)
//
// We probe these via row-count queries that insert the result into a table
// (since scalar extraction of range types requires type-aware decoding that
// the engine may not yet support). The approach:
//   CREATE TABLE t (r int4range);
//   INSERT INTO t SELECT '[1,5)'::int4range + '[3,10)';
//   SELECT COUNT(*) FROM t WHERE r = '[1,10)';  → must be 1
//
// Closed by: 5.24.D (range set-operation operators).
//
// RED until 5.24.D: the engine does not yet implement +, *, - for range types.

/// Phase 5.24.A — range set-operation operators: `+` (union), `*` (intersection),
/// `-` (difference).
///
/// Asserts correct PG semantics by inserting computed results into a table and
/// probing with WHERE equality.  This approach avoids dependence on a
/// range-type scalar decoder in the test harness.
///
/// Closed by: 5.24.D (range set-operation operators).
#[tokio::test]
async fn range_union_intersection() {
    // Slice: "operator semantics — set ops"
    //
    // IMPORTANT: every assertion reflects correct PG semantics. Do NOT weaken
    // them to match Basin's current output — implement 5.24.D instead.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Helper table for result capture ───────────────────────────────────────
    sess.execute("CREATE TABLE set_ops (id BIGSERIAL PRIMARY KEY, r int4range)")
        .await
        .expect("SET OPS: CREATE TABLE set_ops. Closed by 5.24.B.");

    // ── + union ───────────────────────────────────────────────────────────────
    // '[1,5)'::int4range + '[3,10)' → '[1,10)'
    // Overlapping ranges merge into their bounding range.
    sess.execute(
        "INSERT INTO set_ops (r) SELECT '[1,5)'::int4range + '[3,10)'::int4range",
    )
    .await
    .expect(
        "SET OPS(+): INSERT of range union '[1,5)' + '[3,10)' must succeed. \
         Closed by 5.24.D.",
    );

    // The result '[1,10)' contains 5 — use @> to verify the merged range.
    let union_contains_5 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 5",
    )
    .await;
    assert_eq!(
        union_contains_5, 1,
        "SET OPS(+): union '[1,5)' + '[3,10)' must produce '[1,10)' which contains 5. \
         got={union_contains_5} rows with r @> 5. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] union '[1,5)' + '[3,10)' → result @> 5: {union_contains_5} row (expected 1)"
    );

    // The result must NOT contain 10 (exclusive upper bound).
    let union_excludes_10 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 10",
    )
    .await;
    assert_eq!(
        union_excludes_10, 0,
        "SET OPS(+): union result '[1,10)' must NOT contain 10 (exclusive upper). \
         got={union_excludes_10} rows with r @> 10. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] union result @> 10 (exclusive upper): {union_excludes_10} rows (expected 0)"
    );

    // ── * intersection ────────────────────────────────────────────────────────
    // '[1,10)'::int4range * '[3,7)' → '[3,7)'
    sess.execute("DELETE FROM set_ops")
        .await
        .expect("SET OPS: DELETE to reset set_ops table.");

    sess.execute(
        "INSERT INTO set_ops (r) SELECT '[1,10)'::int4range * '[3,7)'::int4range",
    )
    .await
    .expect(
        "SET OPS(*): INSERT of range intersection '[1,10)' * '[3,7)' must succeed. \
         Closed by 5.24.D.",
    );

    // The intersection '[3,7)' must contain 4 but not 7 or 2.
    let inter_contains_4 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 4",
    )
    .await;
    assert_eq!(
        inter_contains_4, 1,
        "SET OPS(*): intersection '[1,10)' * '[3,7)' must produce '[3,7)' which contains 4. \
         got={inter_contains_4}. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] intersection '[1,10)' * '[3,7)' → result @> 4: {inter_contains_4} row (expected 1)"
    );

    let inter_excludes_2 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 2",
    )
    .await;
    assert_eq!(
        inter_excludes_2, 0,
        "SET OPS(*): intersection '[3,7)' must NOT contain 2 (below lower bound). \
         got={inter_excludes_2}. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] intersection result @> 2: {inter_excludes_2} rows (expected 0)"
    );

    let inter_excludes_7 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 7",
    )
    .await;
    assert_eq!(
        inter_excludes_7, 0,
        "SET OPS(*): intersection '[3,7)' must NOT contain 7 (exclusive upper). \
         got={inter_excludes_7}. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] intersection result @> 7 (exclusive upper): {inter_excludes_7} rows (expected 0)"
    );

    // ── - difference ──────────────────────────────────────────────────────────
    // '[1,10)'::int4range - '[1,5)' → '[5,10)'
    // Removes the prefix [1,5) from [1,10), leaving [5,10).
    // PG requires the remainder to be contiguous; this case produces [5,10).
    sess.execute("DELETE FROM set_ops")
        .await
        .expect("SET OPS: DELETE to reset set_ops table.");

    sess.execute(
        "INSERT INTO set_ops (r) SELECT '[1,10)'::int4range - '[1,5)'::int4range",
    )
    .await
    .expect(
        "SET OPS(-): INSERT of range difference '[1,10)' - '[1,5)' must succeed. \
         Closed by 5.24.D.",
    );

    // '[5,10)' must contain 6 but not 4 (removed) or 10 (exclusive upper).
    let diff_contains_6 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 6",
    )
    .await;
    assert_eq!(
        diff_contains_6, 1,
        "SET OPS(-): difference '[1,10)' - '[1,5)' must produce '[5,10)' which contains 6. \
         got={diff_contains_6}. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] difference '[1,10)' - '[1,5)' → result @> 6: {diff_contains_6} row (expected 1)"
    );

    let diff_excludes_4 = row_count(
        &sess,
        "SELECT id FROM set_ops WHERE r @> 4",
    )
    .await;
    assert_eq!(
        diff_excludes_4, 0,
        "SET OPS(-): difference result '[5,10)' must NOT contain 4 (was in removed prefix). \
         got={diff_excludes_4}. Closed by 5.24.D."
    );
    println!(
        "[5.24.A set-ops] difference result @> 4 (removed prefix): {diff_excludes_4} rows (expected 0)"
    );

    println!(
        "[5.24.A set-ops] PASSED — +, *, - range set-operation semantics match PG expectations"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 5 — GIST index performance gate
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that a GIST-indexed `@>` (containment) lookup on a range column is
// ≥ 5× faster than the same query on an identical table without an index.
//
// The conservative ≥ 5× bar (vs ≥ 10× for GIN/JSONB in 5.19.A) reflects that
// GIST indexes have higher per-entry overhead than GIN; real-world PG delivers
// 20-200× for range @> lookups depending on selectivity.
//
// Measurement strategy:
//   - Two tables: one with GIST index, one without.
//   - Seed both with 50 000 rows, each containing a non-overlapping 10-unit
//     range: row i → '[i*10, i*10+10)'.
//   - Probe: SELECT WHERE r @> 250000 (hits row 25000 only; selectivity ≈ 1/50000).
//   - Time N=5 runs, take minimum.
//   - Assert ratio ≥ 5×.
//
// Closed by: 5.24.E (GIST index for range types — planner uses index for @>).
//
// RED until 5.24.E: the engine does not yet implement GIST range indexes;
// both tables behave identically (full scan), so ratio ≈ 1×.

/// Phase 5.24.A — GIST index performance gate: `@>` lookup must be ≥ 5× faster
/// with a GIST index than without.
///
/// Seeds two 50 000-row tables (one GIST-indexed, one not), probes with a
/// high-selectivity `@>` point query, and asserts ≥ 5× speedup.
///
/// Closed by: 5.24.E (GIST index + planner integration for range `@>` probes).
#[ignore = "5.24.E — GIST index planner integration pending"]
#[tokio::test]
async fn range_gist_index() {
    // Slice: "GIST perf"
    //
    // IMPORTANT: every assertion reflects correct PG semantics and TASK.md
    // 5.24.A perf bar. Do NOT weaken the ratio — implement 5.24.E instead.

    basin_common::telemetry::try_init_for_tests();

    // The minimum speedup ratio. Real PG GIST delivers 20-200× for point probes;
    // 5× is the conservative floor to leave headroom for early implementations.
    const MIN_SPEEDUP_RATIO: f64 = 5.0;

    // 50 000 rows × 10-unit ranges → 500 000 integer span.
    // Each row's range is non-overlapping: row i → '[i*10, i*10+10)'.
    const ROW_COUNT: usize = 50_000;

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL: two tables ────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE gist_indexed (id BIGINT NOT NULL, r int4range NOT NULL)",
    )
    .await
    .expect("GIST PERF: CREATE TABLE gist_indexed. Closed by 5.24.B.");

    sess.execute(
        "CREATE TABLE gist_noindex (id BIGINT NOT NULL, r int4range NOT NULL)",
    )
    .await
    .expect("GIST PERF: CREATE TABLE gist_noindex. Closed by 5.24.B.");

    // GIST index on the first table.
    // Expected after 5.24.E: DDL is accepted and the planner uses the index.
    let gist_ddl = sess
        .execute(
            "CREATE INDEX gist_indexed_r_idx ON gist_indexed USING gist (r)",
        )
        .await;
    println!("[5.24.A gist] GIST CREATE INDEX result: {gist_ddl:?}");
    // We log but do not assert here: the DDL may fail until 5.24.E lands.
    // The perf ratio assertion below is what defines "closed".

    // ── Seed both tables ───────────────────────────────────────────────────────
    // Non-overlapping ranges: row i → '[i*10, i*10+10)'.
    // This gives a dense coverage of [0, 500000) with no overlap, making a
    // single-point @> probe highly selective (1 match in 50 000 rows).
    let batch_size = 500usize;
    let mut inserted = 0usize;
    while inserted < ROW_COUNT {
        let take = batch_size.min(ROW_COUNT - inserted);
        let mut values: Vec<String> = Vec::with_capacity(take);
        for i in inserted..(inserted + take) {
            let lo = i * 10;
            let hi = lo + 10;
            values.push(format!("({i}, '[{lo},{hi})')"));
        }
        let vals = values.join(", ");

        let sql_idx = format!("INSERT INTO gist_indexed VALUES {vals}");
        sess.execute(&sql_idx)
            .await
            .unwrap_or_else(|e| panic!("GIST PERF: INSERT gist_indexed batch at {inserted}: {e}"));

        let sql_no = format!("INSERT INTO gist_noindex VALUES {vals}");
        sess.execute(&sql_no)
            .await
            .unwrap_or_else(|e| panic!("GIST PERF: INSERT gist_noindex batch at {inserted}: {e}"));

        inserted += take;
    }
    println!("[5.24.A gist] seeded {inserted} rows in both tables");

    // ── Probe: point value 250000 lives in row 25000's range '[250000,250010)' ──
    // Selectivity: exactly 1 in 50 000 rows.
    let probe_val = 250_000i64;
    let query_indexed = format!(
        "SELECT id FROM gist_indexed WHERE r @> {probe_val}"
    );
    let query_noindex = format!(
        "SELECT id FROM gist_noindex WHERE r @> {probe_val}"
    );

    // ── Warm-up ────────────────────────────────────────────────────────────────
    let _ = row_count(&sess, &query_indexed).await;
    let _ = row_count(&sess, &query_noindex).await;

    // ── Timed runs — N=5, take minimum ────────────────────────────────────────
    const RUNS: usize = 5;
    let mut min_indexed_us = u64::MAX;
    let mut min_noindex_us = u64::MAX;

    for _run in 0..RUNS {
        let t0 = Instant::now();
        let n_idx = row_count(&sess, &query_indexed).await;
        let elapsed_idx = t0.elapsed().as_micros() as u64;
        if elapsed_idx < min_indexed_us {
            min_indexed_us = elapsed_idx;
        }
        // Sanity: exactly 1 row must match.
        assert_eq!(
            n_idx, 1,
            "GIST PERF: indexed @> {probe_val} must match exactly 1 row. got={n_idx}"
        );

        let t1 = Instant::now();
        let n_no = row_count(&sess, &query_noindex).await;
        let elapsed_no = t1.elapsed().as_micros() as u64;
        if elapsed_no < min_noindex_us {
            min_noindex_us = elapsed_no;
        }
        assert_eq!(
            n_no, 1,
            "GIST PERF: full-scan @> {probe_val} must match exactly 1 row. got={n_no}"
        );
    }

    let indexed_ms = min_indexed_us as f64 / 1_000.0;
    let noindex_ms = min_noindex_us as f64 / 1_000.0;
    let ratio = if min_indexed_us == 0 {
        f64::INFINITY
    } else {
        min_noindex_us as f64 / min_indexed_us as f64
    };

    println!(
        "[5.24.A gist] indexed_min={indexed_ms:.2}ms  noindex_min={noindex_ms:.2}ms  \
         ratio={ratio:.1}× (need ≥{MIN_SPEEDUP_RATIO}×)"
    );

    // Performance bar from TASK.md 5.24.A: GIST index speeds up @> lookups ≥ 5×.
    assert!(
        ratio >= MIN_SPEEDUP_RATIO,
        "GIST PERF GATE FAILED: GIST-indexed @> must be ≥{MIN_SPEEDUP_RATIO}× faster than \
         full-scan @>.\n  indexed_min={indexed_ms:.2}ms  noindex_min={noindex_ms:.2}ms  \
         actual_ratio={ratio:.1}×.\n  Closed by: 5.24.E (GIST index planner integration)."
    );

    println!(
        "[5.24.A gist] PASSED — GIST index @> is {ratio:.1}× faster than full-scan \
         (≥{MIN_SPEEDUP_RATIO}× required)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 6 — EXCLUDE USING gist — scheduling pattern
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that EXCLUDE USING gist prevents double-booking in a room-scheduling
// table.  This is the canonical use case for GIST exclusion constraints.
//
// Schema:
//   CREATE TABLE bookings (
//       id      BIGSERIAL PRIMARY KEY,
//       room    INT       NOT NULL,
//       during  tsrange   NOT NULL,
//       EXCLUDE USING gist (room WITH =, during WITH &&)
//   );
//
// Semantics:
//   - Two bookings for the *same* room with *overlapping* time ranges must be
//     rejected (SQLSTATE 23P01 — exclusion_violation).
//   - Two bookings for *different* rooms with the same time range are allowed.
//   - Two bookings for the *same* room with *adjacent* (non-overlapping) time
//     ranges are allowed (touching ranges are not overlapping under &&).
//
// The test inserts a baseline booking, then probes all three cases above.
//
// Closed by: 5.24.F (EXCLUDE USING gist + exclusion constraint enforcement).
//
// RED until 5.24.F: the engine does not yet parse or enforce EXCLUDE USING
// gist; the conflicting INSERT will succeed when it should fail, or the DDL
// itself will be rejected.

/// Phase 5.24.A — EXCLUDE USING gist prevents double-booking in a scheduling
/// table with (room, during) where room must be equal and during must not overlap.
///
/// Asserts:
///   1. The EXCLUDE USING gist DDL is accepted.
///   2. Overlapping booking for the same room is rejected (SQLSTATE 23P01).
///   3. Booking for a different room in the same slot is accepted.
///   4. Adjacent (non-overlapping) booking for the same room is accepted.
///
/// Closed by: 5.24.F (EXCLUDE USING gist + constraint enforcement).
#[ignore = "5.24.F — exclusion constraint scheduling pending"]
#[tokio::test]
async fn range_exclusion_constraint_scheduling() {
    // Slice: "scheduling pattern"
    //
    // IMPORTANT: every assertion reflects correct PG semantics. Do NOT weaken
    // them to match Basin's current output — implement 5.24.F instead.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL: bookings table with EXCLUDE USING gist ────────────────────────────
    // This is the canonical PG scheduling pattern from the docs.
    // Expected after 5.24.F: DDL is accepted and the constraint is enforced.
    // Currently RED: parser rejects EXCLUDE USING gist.
    let ddl_result = sess
        .execute(
            "CREATE TABLE bookings (
                id     BIGSERIAL PRIMARY KEY,
                room   INT       NOT NULL,
                during tsrange   NOT NULL,
                EXCLUDE USING gist (room WITH =, during WITH &&)
            )",
        )
        .await;
    println!("[5.24.A sched] EXCLUDE USING gist DDL: {ddl_result:?}");
    assert!(
        ddl_result.is_ok(),
        "SCHEDULING: CREATE TABLE with EXCLUDE USING gist (room WITH =, during WITH &&) \
         must be accepted. Closed by 5.24.F. error: {:?}",
        ddl_result.unwrap_err()
    );

    // ── Baseline booking: room 1, Monday 09:00–10:00 ──────────────────────────
    sess.execute(
        "INSERT INTO bookings (room, during) \
         VALUES (1, '[2024-01-15 09:00:00,2024-01-15 10:00:00)')",
    )
    .await
    .expect(
        "SCHEDULING: first booking (room 1, 09:00–10:00) must succeed. \
         Closed by 5.24.B.",
    );
    println!("[5.24.A sched] baseline booking inserted: room 1, 09:00–10:00");

    // ── Case 1: same room, overlapping slot → must be REJECTED ────────────────
    // Room 1, 09:30–10:30: overlaps with 09:00–10:00 at [09:30,10:00).
    // PG raises SQLSTATE 23P01 (exclusion_violation).
    let conflict_result = sess
        .execute(
            "INSERT INTO bookings (room, during) \
             VALUES (1, '[2024-01-15 09:30:00,2024-01-15 10:30:00)')",
        )
        .await;
    println!(
        "[5.24.A sched] overlapping booking (room 1, 09:30–10:30): {:?} (expected error 23P01)",
        conflict_result
    );
    assert!(
        conflict_result.is_err(),
        "SCHEDULING: overlapping booking for same room must be REJECTED by \
         EXCLUDE USING gist constraint (SQLSTATE 23P01). \
         The INSERT succeeded when it should have failed. Closed by 5.24.F."
    );

    let conflict_err = format!("{:?}", conflict_result.unwrap_err());
    let is_exclusion_violation = conflict_err.contains("23P01")
        || conflict_err.to_lowercase().contains("exclusion")
        || conflict_err.to_lowercase().contains("conflict");
    assert!(
        is_exclusion_violation,
        "SCHEDULING: rejection must be an exclusion-violation (SQLSTATE 23P01 or \
         message containing 'exclusion'/'conflict'). got={conflict_err:?}. \
         Closed by 5.24.F."
    );
    println!("[5.24.A sched] overlapping booking correctly rejected");

    // ── Case 2: different room, same slot → must be ACCEPTED ──────────────────
    // Room 2, 09:00–10:00: same time, different room — no conflict.
    let diff_room_result = sess
        .execute(
            "INSERT INTO bookings (room, during) \
             VALUES (2, '[2024-01-15 09:00:00,2024-01-15 10:00:00)')",
        )
        .await;
    println!(
        "[5.24.A sched] different room same slot (room 2, 09:00–10:00): {:?} (expected ok)",
        diff_room_result
    );
    assert!(
        diff_room_result.is_ok(),
        "SCHEDULING: booking for room 2 in the same time slot must be ACCEPTED \
         (room != room, so the exclusion constraint does not fire). \
         Closed by 5.24.F. error: {:?}",
        diff_room_result.unwrap_err()
    );
    println!("[5.24.A sched] different-room booking correctly accepted");

    // ── Case 3: same room, adjacent slot → must be ACCEPTED ──────────────────
    // Room 1, 10:00–11:00: starts exactly where the baseline booking ends.
    // [09:00,10:00) && [10:00,11:00) → FALSE (adjacent, not overlapping).
    let adjacent_result = sess
        .execute(
            "INSERT INTO bookings (room, during) \
             VALUES (1, '[2024-01-15 10:00:00,2024-01-15 11:00:00)')",
        )
        .await;
    println!(
        "[5.24.A sched] adjacent booking (room 1, 10:00–11:00): {:?} (expected ok)",
        adjacent_result
    );
    assert!(
        adjacent_result.is_ok(),
        "SCHEDULING: adjacent booking for room 1 (10:00–11:00 after 09:00–10:00) \
         must be ACCEPTED — adjacent tsranges do not overlap under &&. \
         Closed by 5.24.F. error: {:?}",
        adjacent_result.unwrap_err()
    );
    println!("[5.24.A sched] adjacent booking correctly accepted");

    // ── Sanity: confirm row counts ─────────────────────────────────────────────
    // 3 accepted inserts: baseline (room 1, 09:00–10:00), diff-room (room 2,
    // 09:00–10:00), adjacent (room 1, 10:00–11:00).
    let total_bookings = row_count(&sess, "SELECT id FROM bookings").await;
    assert_eq!(
        total_bookings, 3,
        "SCHEDULING: bookings table must contain exactly 3 rows after 1 rejected \
         and 3 accepted inserts. got={total_bookings}. Closed by 5.24.F."
    );
    println!("[5.24.A sched] total bookings: {total_bookings} (expected 3)");

    println!(
        "[5.24.A sched] PASSED — EXCLUDE USING gist correctly prevents double-booking \
         and allows adjacent/different-room bookings"
    );
}
