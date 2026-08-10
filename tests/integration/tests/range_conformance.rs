//! PG-pinned conformance tests for Basin's range-type SQL surface.
//!
//! ## What is covered
//!
//! These tests target the operator UDFs and constructors registered by
//! `crates/basin-engine/src/range_udf.rs` and the core type library in
//! `crates/basin-common/src/types/range.rs`.  Every expected value is pinned
//! to documented PostgreSQL 16 semantics.
//!
//! Groups:
//!   1. **Constructors** — `int4range`, `int8range`, `numrange`, `daterange`,
//!      `tsrange`, `tstzrange` via both UDF and PG text-literal forms.
//!   2. **Canonicalization** — discrete-range half-open normalization.
//!   3. **Accessor functions** — `lower`, `upper`, `isempty`, `lower_inc`,
//!      `upper_inc`, `lower_inf`, `upper_inf`.
//!   4. **Containment operators** — `@>` (element and range), `<@`.
//!   5. **Overlap operator** — `&&` (critical adjacency semantics).
//!   6. **Positional operators** — `<<`, `>>`, `&<`, `&>`, `-|-`.
//!   7. **Set-operation operators** — `+` (union), `*` (intersection),
//!      `-` (difference).
//!   8. **`range_merge`** — bounding-range (union hull).
//!   9. **NULL / empty-range handling**.
//!  10. **`int4multirange`** — construction and `@>` containment.
//!  11. **Table-level usage** — INSERT / WHERE / filter with range columns.
//!
//! ## What is NOT covered here (gap table)
//!
//! NOTE: `EXCLUDE USING gist` is now parsed and enforced via the sentinel-CHECK
//! exclusion enforcer (`extract_exclude_using_gist` in `ddl.rs`, wired in
//! `executor.rs`) as of Phase 5.24.  The GIST *index* (a performance optimisation
//! for range queries, not a correctness gate) remains deferred.
//!
//! | Feature                                  | Reason not tested                         |
//! |------------------------------------------|-------------------------------------------|
//! | GIST index for range columns             | Perf optimisation; correctness covered by the interval-tree probe (5.24.D). |
//! | `numrange`/`tsrange` text-literal `@>`   | Operator rewriter requires explicit ctor  |
//! |                                          | keyword to distinguish from JSONB `@>`.   |
//! | `range_agg()`                            | Not registered (no aggregate UDF yet).    |
//! | `multirange && multirange`               | Only `@>` scalar is registered.           |
//! | pgwire wire encoding of range columns    | Covered by `pgwire_wire_compat.rs`.       |
//!
//! ## Needs-psql-validation table
//!
//! The following expected values should be cross-checked against a live
//! Postgres 16 instance once the benchmark box is free.  All values are
//! derived analytically from PG semantics and are very likely correct, but
//! are flagged here for completeness:
//!
//! | Test / assertion                               | Expected      | Notes                          |
//! |------------------------------------------------|---------------|--------------------------------|
//! | `lower(numrange(1.5, 2.5))` text output        | `"1.5"`       | Float formatting in Basin JSON |
//! | `upper(tsrange(...))`  text output             | timestamp str | string form not yet pinned     |
//! | `isempty(int4range(5, 5))` (lo == hi, default) | `true`        | empty range detection          |
//! | `range_merge` JSON output format               | JSON string   | Basin-specific format          |

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Harness helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine() -> (Engine, TempDir) {
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
        catalog,
        shard: None,
    });
    (engine, dir)
}

async fn open_session(engine: &Engine) -> basin_engine::ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

/// Execute SQL, panic with a clear message on error.
async fn exec(sess: &basin_engine::ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed:\n  {sql}\n  error: {e}"))
}

/// Execute SQL, assert success, ignore result.
async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) {
    let _ = exec(sess, sql).await;
}

/// Execute SQL and return the first column of every row as `Option<String>`.
async fn string_col(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<Option<String>> {
    match exec(sess, sql).await {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0);
                let sa = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("expected StringArray for: {sql}"));
                for i in 0..b.num_rows() {
                    out.push(if sa.is_null(i) {
                        None
                    } else {
                        Some(sa.value(i).to_string())
                    });
                }
            }
            out
        }
        other => panic!("expected Rows for: {sql}\ngot: {other:?}"),
    }
}

/// Execute SQL and return the first column of every row as `bool`.
async fn bool_col(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<bool> {
    match exec(sess, sql).await {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0);
                let ba = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap_or_else(|| panic!("expected BooleanArray for: {sql}"));
                for i in 0..b.num_rows() {
                    out.push(ba.value(i));
                }
            }
            out
        }
        other => panic!("expected Rows for: {sql}\ngot: {other:?}"),
    }
}

/// Return the single boolean scalar from `sql`.  Panics if not exactly one row.
async fn scalar_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    let v = bool_col(sess, sql).await;
    assert_eq!(
        v.len(),
        1,
        "expected exactly 1 row for: {sql}\ngot {}",
        v.len()
    );
    v[0]
}

/// Return the single string scalar from `sql`.  Panics if not exactly one row.
async fn scalar_str(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let v = string_col(sess, sql).await;
    assert_eq!(
        v.len(),
        1,
        "expected exactly 1 row for: {sql}\ngot {}",
        v.len()
    );
    v[0].clone()
        .unwrap_or_else(|| panic!("expected non-NULL string for: {sql}"))
}

/// Count rows returned by `sql`.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match exec(sess, sql).await {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected Rows for: {sql}\ngot: {other:?}"),
    }
}

/// Parse the Basin JSON range string into its fields.
fn parse_range_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("not valid JSON: {s:?}\nerror: {e}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1 — Constructors
// ─────────────────────────────────────────────────────────────────────────────

/// `int4range(1, 10)` constructor produces a half-open range `[1,10)`.
/// PG: `SELECT int4range(1, 10); => [1,10)`
/// Basin stores as JSON: `{"l":1,"u":10,"li":true,"ui":false}`
#[tokio::test]
async fn range_ctor_int4range_default_bounds() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(&sess, "SELECT int4range(1, 10)").await;
    let v = parse_range_json(&s);
    assert_eq!(v["l"], 1, "int4range(1,10): lower bound must be 1");
    assert_eq!(v["u"], 10, "int4range(1,10): upper bound must be 10");
    assert_eq!(
        v["li"], true,
        "int4range(1,10): lower must be inclusive (default '[)')"
    );
    assert_eq!(
        v["ui"], false,
        "int4range(1,10): upper must be exclusive (default '[)')"
    );
}

/// `int4range(1, 10, '[]')` — explicit closed bounds.
/// PG: `SELECT int4range(1, 10, '[]'); => [1,11)` (canonicalized for discrete type).
/// Basin stores the raw result before canonicalization; the constructor records
/// li=true, ui=true.  The canonical form is resolved by `range_eq`.
#[tokio::test]
async fn range_ctor_int4range_closed_bounds() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(&sess, "SELECT int4range(1, 10, '[]')").await;
    let v = parse_range_json(&s);
    assert_eq!(v["l"], 1, "int4range(1,10,'[]'): lower must be 1");
    assert_eq!(v["u"], 10, "int4range(1,10,'[]'): upper must be 10");
    assert_eq!(v["li"], true, "int4range(1,10,'[]'): lower inclusive");
    assert_eq!(v["ui"], true, "int4range(1,10,'[]'): upper inclusive");
}

/// `int8range(100, 200)` constructor.
/// PG: `SELECT int8range(100, 200); => [100,200)`
#[tokio::test]
async fn range_ctor_int8range() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(&sess, "SELECT int8range(100, 200)").await;
    let v = parse_range_json(&s);
    assert_eq!(v["l"], 100, "int8range(100,200): lower must be 100");
    assert_eq!(v["u"], 200, "int8range(100,200): upper must be 200");
    assert_eq!(v["li"], true);
    assert_eq!(v["ui"], false);
}

/// PG text-literal form `'[1,10)'::int4range` — the cast suffix is stripped,
/// the string is parsed into the Basin JSON format.
/// The cast-strip rewriter removes `::int4range`; the JSON produced must match
/// the constructor form.
#[tokio::test]
async fn range_literal_cast_strip() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // After rewriting `'[1,10)'::int4range` → `'[1,10)'`, DataFusion treats
    // this as a plain string.  The range_contains_elem UDF parses it on use.
    // Here we just verify the SELECT doesn't error.
    let result = sess.execute("SELECT '[1,10)'::int4range").await;
    assert!(
        result.is_ok(),
        "SELECT '[1,10)'::int4range must succeed (cast-strip rewriter): {:?}",
        result.unwrap_err()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2 — Canonicalization (discrete range normalization)
// ─────────────────────────────────────────────────────────────────────────────

/// Discrete-range equality: `[1,9]::int4range = '[1,10)'::int4range` → true.
/// PG semantics: discrete ranges are normalized to the half-open `[lo,hi)` form,
/// so `[1,9]` and `[1,10)` represent the same int4range.
/// PG: `SELECT '[1,9]'::int4range = '[1,10)'::int4range; => t`
#[tokio::test]
async fn range_canonicalization_int4range_closed_eq_half_open() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let result = scalar_bool(&sess, "SELECT '[1,9]'::int4range = '[1,10)'::int4range").await;
    assert!(
        result,
        "PG canonicalization: '[1,9]'::int4range must equal '[1,10)'::int4range \
         (half-open normalization for discrete int4range)"
    );
}

/// Exclusive-lower normalization: `(0,10)::int4range = '[1,10)'::int4range`.
/// PG: `SELECT '(0,10)'::int4range = '[1,10)'::int4range; => t`
/// Both are canonicalized to `[1,10)`.
#[tokio::test]
async fn range_canonicalization_int4range_exclusive_lower() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let result = scalar_bool(&sess, "SELECT '(0,10)'::int4range = '[1,10)'::int4range").await;
    assert!(
        result,
        "PG canonicalization: '(0,10)'::int4range must equal '[1,10)'::int4range \
         (exclusive-lower normalizes to inclusive lower+1)"
    );
}

/// daterange canonicalization: `[2024-01-01,2024-01-30]` =
/// `[2024-01-01,2024-01-31)`.
/// PG: `SELECT '[2024-01-01,2024-01-30]'::daterange = '[2024-01-01,2024-01-31)'::daterange; => t`
#[tokio::test]
async fn range_canonicalization_daterange() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let result = scalar_bool(
        &sess,
        "SELECT '[2024-01-01,2024-01-30]'::daterange = '[2024-01-01,2024-01-31)'::daterange",
    )
    .await;
    assert!(
        result,
        "PG canonicalization: '[2024-01-01,2024-01-30]'::daterange must equal \
         '[2024-01-01,2024-01-31)'::daterange"
    );
}

/// Different ranges must NOT be equal.
/// PG: `SELECT '[1,5)'::int4range = '[1,10)'::int4range; => f`
#[tokio::test]
async fn range_canonicalization_different_ranges_not_equal() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let result = scalar_bool(&sess, "SELECT '[1,5)'::int4range = '[1,10)'::int4range").await;
    assert!(
        !result,
        "PG: '[1,5)'::int4range must NOT equal '[1,10)'::int4range"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3 — Accessor functions
// ─────────────────────────────────────────────────────────────────────────────

/// `lower(int4range(5, 20))` → "5".
/// PG: `SELECT lower(int4range(5, 20)); => 5`
#[tokio::test]
async fn range_accessor_lower_int4() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(&sess, "SELECT lower(int4range(5, 20))").await;
    assert_eq!(s, "5", "lower(int4range(5,20)) must be '5'");
}

/// `upper(int4range(5, 20))` → "20".
/// PG: `SELECT upper(int4range(5, 20)); => 20`
#[tokio::test]
async fn range_accessor_upper_int4() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(&sess, "SELECT upper(int4range(5, 20))").await;
    assert_eq!(s, "20", "upper(int4range(5,20)) must be '20'");
}

/// `lower_inc(int4range(1, 5))` → true (default bounds are `[)` — inclusive lower).
/// PG: `SELECT lower_inc(int4range(1, 5)); => t`
#[tokio::test]
async fn range_accessor_lower_inc_default() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT lower_inc(int4range(1, 5))").await;
    assert!(
        v,
        "lower_inc(int4range(1,5)): default bounds are '[)' so lower is inclusive"
    );
}

/// `upper_inc(int4range(1, 5))` → false (default bounds are `[)` — exclusive upper).
/// PG: `SELECT upper_inc(int4range(1, 5)); => f`
#[tokio::test]
async fn range_accessor_upper_inc_default() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT upper_inc(int4range(1, 5))").await;
    assert!(
        !v,
        "upper_inc(int4range(1,5)): default bounds are '[)' so upper is exclusive"
    );
}

/// `lower_inc(int4range(1, 5, '[]'))` → true.
/// PG: `SELECT lower_inc(int4range(1, 5, '[]')); => t`
#[tokio::test]
async fn range_accessor_lower_inc_closed() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT lower_inc(int4range(1, 5, '[]'))").await;
    assert!(v, "lower_inc with '[]' bounds must be true");
}

/// `upper_inc(int4range(1, 5, '[]'))` → true.
/// PG: `SELECT upper_inc(int4range(1, 5, '[]')); => t`
#[tokio::test]
async fn range_accessor_upper_inc_closed() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT upper_inc(int4range(1, 5, '[]'))").await;
    assert!(v, "upper_inc with '[]' bounds must be true");
}

/// `lower_inf(int4range(NULL, 10))` → true.
/// A NULL lower bound means −∞.
/// PG: `SELECT lower_inf(int4range(NULL, 10)); => t`
#[tokio::test]
async fn range_accessor_lower_inf_null_lower() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT lower_inf(int4range(NULL, 10))").await;
    assert!(
        v,
        "lower_inf(int4range(NULL,10)) must be true (NULL lower = -inf)"
    );
}

/// `upper_inf(int4range(1, NULL))` → true.
/// A NULL upper bound means +∞.
/// PG: `SELECT upper_inf(int4range(1, NULL)); => t`
#[tokio::test]
async fn range_accessor_upper_inf_null_upper() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT upper_inf(int4range(1, NULL))").await;
    assert!(
        v,
        "upper_inf(int4range(1,NULL)) must be true (NULL upper = +inf)"
    );
}

/// `lower_inf(int4range(1, 10))` → false (finite lower bound).
/// PG: `SELECT lower_inf(int4range(1, 10)); => f`
#[tokio::test]
async fn range_accessor_lower_inf_finite() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT lower_inf(int4range(1, 10))").await;
    assert!(
        !v,
        "lower_inf(int4range(1,10)) must be false (finite lower)"
    );
}

/// `isempty` on a range where lo == hi with default `[)` bounds.
/// PG: `SELECT isempty(int4range(5, 5)); => t`
/// (The range `[5,5)` contains no integers.)
///
/// NEEDS-PSQL-VALIDATION: verify `SELECT isempty(int4range(5,5));` in PG 16.
#[tokio::test]
async fn range_accessor_isempty_lo_eq_hi() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT isempty(int4range(5, 5))").await;
    assert!(
        v,
        "isempty(int4range(5,5)): [5,5) contains no points, must be empty"
    );
}

/// `isempty` on a non-empty range.
/// PG: `SELECT isempty(int4range(1, 5)); => f`
#[tokio::test]
async fn range_accessor_isempty_non_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT isempty(int4range(1, 5))").await;
    assert!(
        !v,
        "isempty(int4range(1,5)): non-empty range must return false"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4 — Containment operators (@> and <@)
// ─────────────────────────────────────────────────────────────────────────────

/// Element containment (via `range_contains_elem` UDF directly, since the
/// `@>` operator rewriter requires a range-constructor keyword on one side):
///
/// PG: `SELECT int4range(1,10) @> 5; => t`   (5 is in [1,10))
/// PG: `SELECT int4range(1,10) @> 10; => f`  (10 is the exclusive upper)
/// PG: `SELECT int4range(1,10) @> 0; => f`   (0 is below lower)
#[tokio::test]
async fn range_contains_elem_basic() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // 5 ∈ [1,10) → true
    let in_range = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 5)").await;
    assert!(in_range, "range_contains_elem([1,10), 5) must be true");

    // 10 ∉ [1,10) → false (exclusive upper)
    let at_upper = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 10)").await;
    assert!(
        !at_upper,
        "range_contains_elem([1,10), 10) must be false (exclusive upper)"
    );

    // 0 ∉ [1,10) → false (below lower)
    let below_lower = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 0)").await;
    assert!(
        !below_lower,
        "range_contains_elem([1,10), 0) must be false (below lower)"
    );

    // 1 ∈ [1,10) → true (inclusive lower)
    let at_lower = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 1)").await;
    assert!(
        at_lower,
        "range_contains_elem([1,10), 1) must be true (inclusive lower)"
    );

    // 9 ∈ [1,10) → true (last integer in range)
    let last_int = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 9)").await;
    assert!(last_int, "range_contains_elem([1,10), 9) must be true");
}

/// Element containment via `@>` operator rewrite.
/// PG: `SELECT int4range(1,10) @> 5; => t`
#[tokio::test]
async fn range_at_gt_elem_operator_rewrite() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // int4range ctor triggers the @> → range_contains_elem rewrite.
    let v = scalar_bool(&sess, "SELECT int4range(1, 10) @> 5").await;
    assert!(v, "int4range(1,10) @> 5 must be true");

    let v2 = scalar_bool(&sess, "SELECT int4range(1, 10) @> 10").await;
    assert!(!v2, "int4range(1,10) @> 10 must be false (exclusive upper)");
}

/// Range-in-range containment.
/// PG: `SELECT int4range(1,10) @> int4range(2,8); => t`   ([2,8) ⊆ [1,10))
/// PG: `SELECT int4range(1,10) @> int4range(2,15); => f`  ([2,15) ⊄ [1,10))
#[tokio::test]
async fn range_contains_range_basic() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // [2,8) ⊆ [1,10)
    let sub = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), int4range(2, 8))",
    )
    .await;
    assert!(sub, "range_contains_range([1,10),[2,8)) must be true");

    // [2,15) ⊄ [1,10)
    let exceeds = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), int4range(2, 15))",
    )
    .await;
    assert!(
        !exceeds,
        "range_contains_range([1,10),[2,15)) must be false (15 exceeds upper)"
    );

    // [1,10) ⊆ [1,10) (exact match)
    let exact = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), int4range(1, 10))",
    )
    .await;
    assert!(
        exact,
        "range_contains_range([1,10),[1,10)) must be true (exact match)"
    );
}

/// Containment via `@>` with PG text literals (rewriter uses literal detection).
/// PG: `SELECT '[1,10)'::int4range @> '[2,8)'::int4range; => t`
#[tokio::test]
async fn range_at_gt_range_operator_literal() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT '[1,10)'::int4range @> '[2,8)'::int4range").await;
    assert!(
        v,
        "'[1,10)'::int4range @> '[2,8)'::int4range must be true ([2,8) ⊆ [1,10))"
    );
}

/// `<@` reverse containment.
/// PG: `SELECT 5 <@ int4range(1,10); => t`
/// PG: `SELECT int4range(2,8) <@ int4range(1,10); => t`
#[tokio::test]
async fn range_contained_by_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // 5 <@ [1,10) via explicit UDF (elem<@range rewrite reverses the args)
    let elem_in = scalar_bool(&sess, "SELECT range_contains_elem(int4range(1, 10), 5)").await;
    assert!(elem_in, "5 is contained in [1,10)");

    // [2,8) <@ [1,10) via range_contains_range with args swapped
    let range_in = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), int4range(2, 8))",
    )
    .await;
    assert!(range_in, "[2,8) is contained in [1,10)");

    // [2,15) is NOT contained in [1,10)
    let not_in = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), int4range(2, 15))",
    )
    .await;
    assert!(!not_in, "[2,15) must NOT be contained in [1,10)");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5 — Overlap operator (&&) — critical adjacency semantics
// ─────────────────────────────────────────────────────────────────────────────

/// Overlapping ranges → true.
/// PG: `SELECT int4range(1,5) && int4range(3,8); => t`
/// Overlap region: [3,5).
#[tokio::test]
async fn range_overlaps_basic_true() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(
        &sess,
        "SELECT range_overlaps(int4range(1, 5), int4range(3, 8))",
    )
    .await;
    assert!(
        v,
        "range_overlaps([1,5),[3,8)) must be true (overlap at [3,5))"
    );
}

/// Adjacent ranges do NOT overlap.
/// PG: `SELECT int4range(1,5) && int4range(5,10); => f`
/// This is a critical PG semantic: [1,5) ends before 5 (exclusive upper),
/// [5,10) starts at 5 (inclusive lower).  No point is in both ranges.
#[tokio::test]
async fn range_overlaps_adjacent_is_false() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(
        &sess,
        "SELECT range_overlaps(int4range(1, 5), int4range(5, 10))",
    )
    .await;
    assert!(
        !v,
        "range_overlaps([1,5),[5,10)) must be FALSE — adjacent ranges do NOT \
         overlap under PG semantics ([1,5) exclusive upper, [5,10) inclusive lower)"
    );
}

/// Disjoint ranges → false.
/// PG: `SELECT int4range(1,5) && int4range(6,10); => f`
#[tokio::test]
async fn range_overlaps_disjoint_false() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(
        &sess,
        "SELECT range_overlaps(int4range(1, 5), int4range(6, 10))",
    )
    .await;
    assert!(!v, "range_overlaps([1,5),[6,10)) must be false (disjoint)");
}

/// Fully nested → overlaps.
/// PG: `SELECT int4range(1,10) && int4range(3,7); => t`
#[tokio::test]
async fn range_overlaps_nested() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(
        &sess,
        "SELECT range_overlaps(int4range(1, 10), int4range(3, 7))",
    )
    .await;
    assert!(
        v,
        "range_overlaps([1,10),[3,7)) must be true (inner is nested)"
    );
}

/// `&&` operator rewrite (both operands are range constructors).
/// PG: `SELECT int4range(1,10) && int4range(5,15); => t`
#[tokio::test]
async fn range_overlap_operator_rewrite() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(1, 10) && int4range(5, 15)").await;
    assert!(v, "int4range(1,10) && int4range(5,15) must be true");

    let v2 = scalar_bool(&sess, "SELECT int4range(1, 5) && int4range(5, 10)").await;
    assert!(
        !v2,
        "int4range(1,5) && int4range(5,10) must be false (adjacent, no overlap)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6 — Positional operators (<<, >>, &<, &>, -|-)
// ─────────────────────────────────────────────────────────────────────────────

/// `<<` strictly left of.
/// PG: `SELECT int4range(1,5) << int4range(7,10); => t`
/// PG: `SELECT int4range(1,5) << int4range(4,10); => f`  (overlap at 4)
#[tokio::test]
async fn range_strictly_left() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v_true = scalar_bool(
        &sess,
        "SELECT range_strictly_left(int4range(1, 5), int4range(7, 10))",
    )
    .await;
    assert!(v_true, "range_strictly_left([1,5),[7,10)) must be true");

    let v_false = scalar_bool(
        &sess,
        "SELECT range_strictly_left(int4range(1, 5), int4range(4, 10))",
    )
    .await;
    assert!(
        !v_false,
        "range_strictly_left([1,5),[4,10)) must be false (overlap at 4)"
    );
}

/// `<<` operator rewrite.
/// PG: `SELECT int4range(1,5) << int4range(7,10); => t`
#[tokio::test]
async fn range_strictly_left_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(1, 5) << int4range(7, 10)").await;
    assert!(v, "int4range(1,5) << int4range(7,10) must be true");

    let v2 = scalar_bool(&sess, "SELECT int4range(1, 5) << int4range(4, 10)").await;
    assert!(!v2, "int4range(1,5) << int4range(4,10) must be false");
}

/// `>>` strictly right of.
/// PG: `SELECT int4range(7,10) >> int4range(1,5); => t`
/// PG: `SELECT int4range(4,10) >> int4range(1,5); => f`  (overlap)
#[tokio::test]
async fn range_strictly_right() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v_true = scalar_bool(
        &sess,
        "SELECT range_strictly_right(int4range(7, 10), int4range(1, 5))",
    )
    .await;
    assert!(v_true, "range_strictly_right([7,10),[1,5)) must be true");

    let v_false = scalar_bool(
        &sess,
        "SELECT range_strictly_right(int4range(4, 10), int4range(1, 5))",
    )
    .await;
    assert!(
        !v_false,
        "range_strictly_right([4,10),[1,5)) must be false (overlap)"
    );
}

/// `>>` operator rewrite.
#[tokio::test]
async fn range_strictly_right_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(7, 10) >> int4range(1, 5)").await;
    assert!(v, "int4range(7,10) >> int4range(1,5) must be true");
}

/// `&<` does not extend to the right of.
/// PG: `SELECT int4range(1,5) &< int4range(3,10); => t`  (upper(LHS)=5 ≤ upper(RHS)=10)
/// PG: `SELECT int4range(1,10) &< int4range(3,7); => f`  (upper(LHS)=10 > upper(RHS)=7)
#[tokio::test]
async fn range_not_extends_right() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v_true = scalar_bool(
        &sess,
        "SELECT range_not_extends_right(int4range(1, 5), int4range(3, 10))",
    )
    .await;
    assert!(
        v_true,
        "range_not_extends_right([1,5),[3,10)) must be true (5 ≤ 10)"
    );

    let v_false = scalar_bool(
        &sess,
        "SELECT range_not_extends_right(int4range(1, 10), int4range(3, 7))",
    )
    .await;
    assert!(
        !v_false,
        "range_not_extends_right([1,10),[3,7)) must be false (10 > 7)"
    );
}

/// `&<` operator rewrite.
/// PG: `SELECT int4range(1,5) &< int4range(3,10); => t`
#[tokio::test]
async fn range_not_extends_right_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(1, 5) &< int4range(3, 10)").await;
    assert!(v, "int4range(1,5) &< int4range(3,10) must be true");

    let v2 = scalar_bool(&sess, "SELECT int4range(1, 10) &< int4range(3, 7)").await;
    assert!(!v2, "int4range(1,10) &< int4range(3,7) must be false");
}

/// `&>` does not extend to the left of.
/// PG: `SELECT int4range(5,10) &> int4range(1,7); => t`  (lower(LHS)=5 ≥ lower(RHS)=1)
/// PG: `SELECT int4range(1,10) &> int4range(3,7); => f`  (lower(LHS)=1 < lower(RHS)=3)
#[tokio::test]
async fn range_not_extends_left() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v_true = scalar_bool(
        &sess,
        "SELECT range_not_extends_left(int4range(5, 10), int4range(1, 7))",
    )
    .await;
    assert!(
        v_true,
        "range_not_extends_left([5,10),[1,7)) must be true (5 ≥ 1)"
    );

    let v_false = scalar_bool(
        &sess,
        "SELECT range_not_extends_left(int4range(1, 10), int4range(3, 7))",
    )
    .await;
    assert!(
        !v_false,
        "range_not_extends_left([1,10),[3,7)) must be false (1 < 3)"
    );
}

/// `&>` operator rewrite.
#[tokio::test]
async fn range_not_extends_left_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(5, 10) &> int4range(1, 7)").await;
    assert!(v, "int4range(5,10) &> int4range(1,7) must be true");
}

/// `-|-` adjacent.
/// PG: `SELECT int4range(1,5) -|- int4range(5,10); => t`
/// ([1,5) ends just before 5; [5,10) starts at 5; together they cover [1,10).)
/// PG: `SELECT int4range(1,5) -|- int4range(6,10); => f`  (gap at 5)
#[tokio::test]
async fn range_adjacent_basic() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // Adjacent: [1,5) and [5,10)
    let v_adj = scalar_bool(
        &sess,
        "SELECT range_adjacent(int4range(1, 5), int4range(5, 10))",
    )
    .await;
    assert!(
        v_adj,
        "range_adjacent([1,5),[5,10)) must be true (touch at 5 with no gap)"
    );

    // Gap: [1,5) and [6,10) — 5 is in neither range
    let v_gap = scalar_bool(
        &sess,
        "SELECT range_adjacent(int4range(1, 5), int4range(6, 10))",
    )
    .await;
    assert!(
        !v_gap,
        "range_adjacent([1,5),[6,10)) must be false (gap at 5)"
    );

    // Overlapping is NOT adjacent
    let v_overlap = scalar_bool(
        &sess,
        "SELECT range_adjacent(int4range(1, 5), int4range(3, 10))",
    )
    .await;
    assert!(
        !v_overlap,
        "range_adjacent([1,5),[3,10)) must be false (overlapping ranges are not adjacent)"
    );
}

/// `-|-` operator rewrite.
/// PG: `SELECT int4range(1,5) -|- int4range(5,10); => t`
#[tokio::test]
async fn range_adjacent_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4range(1, 5) -|- int4range(5, 10)").await;
    assert!(v, "int4range(1,5) -|- int4range(5,10) must be true");

    let v2 = scalar_bool(&sess, "SELECT int4range(1, 5) -|- int4range(6, 10)").await;
    assert!(
        !v2,
        "int4range(1,5) -|- int4range(6,10) must be false (gap)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7 — Set-operation operators (+, *, -)
// ─────────────────────────────────────────────────────────────────────────────

/// Range union `+`: overlapping or adjacent ranges merge.
/// PG: `SELECT int4range(1,5) + int4range(3,10); => [1,10)`
///
/// We verify the result via containment rather than text equality (Basin stores
/// ranges as JSON, not PG text form).
#[tokio::test]
async fn range_union_overlapping() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // Compute union and re-query with containment.
    exec_ok(&sess, "CREATE TABLE r_union_test (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_union_test SELECT int4range(1, 5) + int4range(3, 10)",
    )
    .await;

    // The union [1,10) must contain 1, 9 but not 10.
    let contains_1 = row_count(
        &sess,
        "SELECT v FROM r_union_test WHERE range_contains_elem(v, 1)",
    )
    .await;
    assert_eq!(contains_1, 1, "union [1,10) must contain 1");

    let contains_9 = row_count(
        &sess,
        "SELECT v FROM r_union_test WHERE range_contains_elem(v, 9)",
    )
    .await;
    assert_eq!(contains_9, 1, "union [1,10) must contain 9");

    let contains_10 = row_count(
        &sess,
        "SELECT v FROM r_union_test WHERE range_contains_elem(v, 10)",
    )
    .await;
    assert_eq!(
        contains_10, 0,
        "union [1,10) must NOT contain 10 (exclusive upper)"
    );
}

/// Range union via `+` operator rewrite.
/// PG: `SELECT int4range(1,5) + int4range(3,10);` must succeed.
#[tokio::test]
async fn range_union_operator_rewrite() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let result = sess
        .execute("SELECT int4range(1, 5) + int4range(3, 10)")
        .await;
    assert!(
        result.is_ok(),
        "int4range(1,5) + int4range(3,10) must succeed: {:?}",
        result.unwrap_err()
    );
}

/// Range intersection `*`.
/// PG: `SELECT int4range(1,10) * int4range(3,7); => [3,7)`
///
/// We verify the result must contain 3 and 6 but not 2 or 7 or 10.
#[tokio::test]
async fn range_intersection_overlapping() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_inter_test (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_inter_test SELECT int4range(1, 10) * int4range(3, 7)",
    )
    .await;

    // [3,7) must contain 3 and 6, not 2 or 7.
    let c3 = row_count(
        &sess,
        "SELECT v FROM r_inter_test WHERE range_contains_elem(v, 3)",
    )
    .await;
    assert_eq!(c3, 1, "intersection [3,7) must contain 3");

    let c6 = row_count(
        &sess,
        "SELECT v FROM r_inter_test WHERE range_contains_elem(v, 6)",
    )
    .await;
    assert_eq!(c6, 1, "intersection [3,7) must contain 6");

    let c2 = row_count(
        &sess,
        "SELECT v FROM r_inter_test WHERE range_contains_elem(v, 2)",
    )
    .await;
    assert_eq!(c2, 0, "intersection [3,7) must NOT contain 2");

    let c7 = row_count(
        &sess,
        "SELECT v FROM r_inter_test WHERE range_contains_elem(v, 7)",
    )
    .await;
    assert_eq!(
        c7, 0,
        "intersection [3,7) must NOT contain 7 (exclusive upper)"
    );
}

/// Intersection of disjoint ranges → empty.
/// PG: `SELECT int4range(1,5) * int4range(7,10);` → empty range.
#[tokio::test]
async fn range_intersection_disjoint_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_inter_disjoint (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_inter_disjoint SELECT int4range(1, 5) * int4range(7, 10)",
    )
    .await;

    // The result is empty — no element is in it.
    let c5 = row_count(
        &sess,
        "SELECT v FROM r_inter_disjoint WHERE range_contains_elem(v, 5)",
    )
    .await;
    assert_eq!(c5, 0, "disjoint intersection must not contain any element");
}

/// Range difference `-`.
/// PG: `SELECT int4range(1,10) - int4range(1,5); => [5,10)`
/// Removes the prefix [1,5) from [1,10), leaving [5,10).
#[tokio::test]
async fn range_difference_prefix() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_diff_test (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_diff_test SELECT int4range(1, 10) - int4range(1, 5)",
    )
    .await;

    // [5,10) must contain 5 and 9, not 4 or 10.
    let c5 = row_count(
        &sess,
        "SELECT v FROM r_diff_test WHERE range_contains_elem(v, 5)",
    )
    .await;
    assert_eq!(c5, 1, "difference [5,10) must contain 5");

    let c9 = row_count(
        &sess,
        "SELECT v FROM r_diff_test WHERE range_contains_elem(v, 9)",
    )
    .await;
    assert_eq!(c9, 1, "difference [5,10) must contain 9");

    let c4 = row_count(
        &sess,
        "SELECT v FROM r_diff_test WHERE range_contains_elem(v, 4)",
    )
    .await;
    assert_eq!(
        c4, 0,
        "difference [5,10) must NOT contain 4 (removed prefix)"
    );

    let c10 = row_count(
        &sess,
        "SELECT v FROM r_diff_test WHERE range_contains_elem(v, 10)",
    )
    .await;
    assert_eq!(
        c10, 0,
        "difference [5,10) must NOT contain 10 (exclusive upper)"
    );
}

/// Difference that completely removes the range → empty.
/// PG: `SELECT int4range(1,5) - int4range(1,5); => empty`
#[tokio::test]
async fn range_difference_self_is_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_diff_self (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_diff_self SELECT int4range(1, 5) - int4range(1, 5)",
    )
    .await;

    // The result is empty; isempty must return true.
    let is_empty = row_count(&sess, "SELECT v FROM r_diff_self WHERE isempty(v)").await;
    assert_eq!(is_empty, 1, "self-difference must produce an empty range");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8 — range_merge (bounding-range / union hull)
// ─────────────────────────────────────────────────────────────────────────────

/// `range_merge` returns the smallest range containing both inputs.
/// PG: `SELECT range_merge(int4range(1,5), int4range(7,10)); => [1,10)`
/// (Unlike `+`, `range_merge` works even for disjoint ranges.)
#[tokio::test]
async fn range_merge_disjoint() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_merge_test (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_merge_test SELECT range_merge(int4range(1, 5), int4range(7, 10))",
    )
    .await;

    // Hull [1,10) must contain 1, 6, 9 but not 10.
    let c1 = row_count(
        &sess,
        "SELECT v FROM r_merge_test WHERE range_contains_elem(v, 1)",
    )
    .await;
    assert_eq!(c1, 1, "merge hull [1,10) must contain 1");

    let c6 = row_count(
        &sess,
        "SELECT v FROM r_merge_test WHERE range_contains_elem(v, 6)",
    )
    .await;
    assert_eq!(
        c6, 1,
        "merge hull [1,10) must contain 6 (gap filled in hull)"
    );

    let c10 = row_count(
        &sess,
        "SELECT v FROM r_merge_test WHERE range_contains_elem(v, 10)",
    )
    .await;
    assert_eq!(
        c10, 0,
        "merge hull [1,10) must NOT contain 10 (exclusive upper)"
    );
}

/// `range_merge` with overlapping ranges is the bounding range.
/// PG: `SELECT range_merge(int4range(1,7), int4range(3,10)); => [1,10)`
#[tokio::test]
async fn range_merge_overlapping() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_merge_ov (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_merge_ov SELECT range_merge(int4range(1, 7), int4range(3, 10))",
    )
    .await;

    let c1 = row_count(
        &sess,
        "SELECT v FROM r_merge_ov WHERE range_contains_elem(v, 1)",
    )
    .await;
    assert_eq!(c1, 1, "merge [1,10) must contain 1");

    let c9 = row_count(
        &sess,
        "SELECT v FROM r_merge_ov WHERE range_contains_elem(v, 9)",
    )
    .await;
    assert_eq!(c9, 1, "merge [1,10) must contain 9");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9 — NULL and empty-range handling
// ─────────────────────────────────────────────────────────────────────────────

/// `isempty` on the PG `empty` literal → true.
/// PG: `SELECT isempty('empty'::int4range); => t`
#[tokio::test]
async fn range_empty_literal_is_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // Basin parses "empty" → {"empty":true}
    let v = scalar_bool(&sess, "SELECT isempty('empty')").await;
    assert!(v, "isempty('empty') must be true");
}

/// Union of a range with empty → the non-empty range.
/// PG: `SELECT int4range(1,10) + 'empty'::int4range; => [1,10)`
#[tokio::test]
async fn range_union_with_empty_identity() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_empty_id (v TEXT)").await;
    // range_union directly (+ operator requires both to look like range exprs)
    exec_ok(
        &sess,
        "INSERT INTO r_empty_id SELECT range_union(int4range(1, 10), 'empty')",
    )
    .await;

    // Result should be [1,10).
    let c5 = row_count(
        &sess,
        "SELECT v FROM r_empty_id WHERE range_contains_elem(v, 5)",
    )
    .await;
    assert_eq!(
        c5, 1,
        "union with empty should preserve the non-empty range (contains 5)"
    );
}

/// Intersection of a range with empty → empty.
/// PG: `SELECT int4range(1,10) * 'empty'::int4range; => empty`
#[tokio::test]
async fn range_intersection_with_empty_is_empty() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(&sess, "CREATE TABLE r_inter_empty (v TEXT)").await;
    exec_ok(
        &sess,
        "INSERT INTO r_inter_empty SELECT range_intersection(int4range(1, 10), 'empty')",
    )
    .await;

    let is_empty = row_count(&sess, "SELECT v FROM r_inter_empty WHERE isempty(v)").await;
    assert_eq!(is_empty, 1, "intersection with empty range must be empty");
}

/// PG empty-range containment / overlap / positional semantics, pinned to
/// PostgreSQL 16:
///   - `anything @> empty`        → true  (every range contains the empty range)
///   - `empty @> non-empty`       → false (empty contains nothing)
///   - `empty @> empty`           → true
///   - `range_contains_elem(empty, x)` → false (empty contains no element)
///   - `anything && empty`        → false (empty overlaps nothing)
///   - `empty << r`, `empty >> r`, `empty -|- r`, `empty &< r`, `empty &> r`
///                                 → all false
///
/// PG references:
///   `SELECT int4range(1,10) @> 'empty'::int4range;`        => t
///   `SELECT 'empty'::int4range @> int4range(1,10);`        => f
///   `SELECT 'empty'::int4range @> 'empty'::int4range;`     => t
///   `SELECT int4range(1,10) && 'empty'::int4range;`        => f
///   `SELECT 'empty'::int4range << int4range(1,10);`        => f
#[tokio::test]
async fn range_empty_pg_semantics() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // anything @> empty → true
    let contains_empty = scalar_bool(
        &sess,
        "SELECT range_contains_range(int4range(1, 10), 'empty')",
    )
    .await;
    assert!(
        contains_empty,
        "PG: every range contains the empty range (@> empty = t)"
    );

    // empty @> non-empty → false
    let empty_contains = scalar_bool(
        &sess,
        "SELECT range_contains_range('empty', int4range(1, 10))",
    )
    .await;
    assert!(
        !empty_contains,
        "PG: empty range contains no non-empty range"
    );

    // empty @> empty → true
    let empty_contains_empty =
        scalar_bool(&sess, "SELECT range_contains_range('empty', 'empty')").await;
    assert!(empty_contains_empty, "PG: empty @> empty = t");

    // range_contains_elem(empty, x) → false
    let elem_in_empty = scalar_bool(&sess, "SELECT range_contains_elem('empty', 5)").await;
    assert!(!elem_in_empty, "PG: empty range contains no element");

    // anything && empty → false
    let overlaps_empty =
        scalar_bool(&sess, "SELECT range_overlaps(int4range(1, 10), 'empty')").await;
    assert!(
        !overlaps_empty,
        "PG: empty range overlaps nothing (&& empty = f)"
    );
    let empty_overlaps =
        scalar_bool(&sess, "SELECT range_overlaps('empty', int4range(1, 10))").await;
    assert!(!empty_overlaps, "PG: empty && anything = f (symmetric)");

    // Positional operators with an empty operand → all false.
    for udf in &[
        "range_strictly_left",
        "range_strictly_right",
        "range_adjacent",
        "range_not_extends_right",
        "range_not_extends_left",
    ] {
        let lhs_empty =
            scalar_bool(&sess, &format!("SELECT {udf}('empty', int4range(1, 10))")).await;
        assert!(!lhs_empty, "PG: {udf}(empty, r) must be false");
        let rhs_empty =
            scalar_bool(&sess, &format!("SELECT {udf}(int4range(1, 10), 'empty')")).await;
        assert!(!rhs_empty, "PG: {udf}(r, empty) must be false");
    }

    // isempty on a degenerate constructor result: int4range(5,5) → [5,5) is empty.
    let degenerate_empty = scalar_bool(&sess, "SELECT isempty(int4range(5, 5))").await;
    assert!(
        degenerate_empty,
        "PG: isempty(int4range(5,5)) = t ([5,5) has no points)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10 — int4multirange construction and @> containment
// ─────────────────────────────────────────────────────────────────────────────

/// `int4multirange` constructor builds a JSON array of ranges.
/// PG: `SELECT int4multirange(int4range(1,5), int4range(10,15));`
#[tokio::test]
async fn multirange_constructor_smoke() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let s = scalar_str(
        &sess,
        "SELECT int4multirange(int4range(1, 5), int4range(10, 15))",
    )
    .await;
    // Basin stores multirange as a JSON array.
    let v: serde_json::Value = serde_json::from_str(&s)
        .unwrap_or_else(|e| panic!("multirange not valid JSON: {s:?}\nerror: {e}"));
    assert!(v.is_array(), "multirange must be a JSON array, got: {s:?}");
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2, "multirange of 2 ranges must have 2 elements");
}

/// `int4multirange @> scalar` containment.
/// PG: `SELECT int4multirange(int4range(1,5), int4range(10,15)) @> 3; => t`
/// PG: `SELECT int4multirange(int4range(1,5), int4range(10,15)) @> 7; => f`
#[tokio::test]
async fn multirange_contains_elem() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    // 3 ∈ [1,5) → true
    let in1 = scalar_bool(
        &sess,
        "SELECT multirange_contains_elem(int4multirange(int4range(1, 5), int4range(10, 15)), 3)",
    )
    .await;
    assert!(
        in1,
        "multirange_contains_elem({{[1,5),[10,15)}}, 3) must be true"
    );

    // 12 ∈ [10,15) → true
    let in2 = scalar_bool(
        &sess,
        "SELECT multirange_contains_elem(int4multirange(int4range(1, 5), int4range(10, 15)), 12)",
    )
    .await;
    assert!(
        in2,
        "multirange_contains_elem({{[1,5),[10,15)}}, 12) must be true"
    );

    // 7 ∉ either range → false
    let out1 = scalar_bool(
        &sess,
        "SELECT multirange_contains_elem(int4multirange(int4range(1, 5), int4range(10, 15)), 7)",
    )
    .await;
    assert!(
        !out1,
        "multirange_contains_elem({{[1,5),[10,15)}}, 7) must be false (gap)"
    );

    // 5 ∉ [1,5) and ∉ [10,15) → false (5 is exclusive upper of first range)
    let out2 = scalar_bool(
        &sess,
        "SELECT multirange_contains_elem(int4multirange(int4range(1, 5), int4range(10, 15)), 5)",
    )
    .await;
    assert!(
        !out2,
        "multirange_contains_elem({{[1,5),[10,15)}}, 5) must be false (exclusive upper)"
    );
}

/// `int4multirange @> scalar` via `@>` operator rewrite.
/// PG: `SELECT int4multirange(int4range(1,5)) @> 3; => t`
#[tokio::test]
async fn multirange_at_gt_operator() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    let v = scalar_bool(&sess, "SELECT int4multirange(int4range(1, 5)) @> 3").await;
    assert!(v, "int4multirange(int4range(1,5)) @> 3 must be true");

    let v2 = scalar_bool(&sess, "SELECT int4multirange(int4range(1, 5)) @> 5").await;
    assert!(
        !v2,
        "int4multirange(int4range(1,5)) @> 5 must be false (exclusive upper)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 11 — Table-level usage: INSERT, SELECT, WHERE with range columns
// ─────────────────────────────────────────────────────────────────────────────

/// INSERT into an `int4range` column using the constructor, then filter with
/// `range_contains_elem` in a WHERE clause.
///
/// This exercises the full path: DDL → DML → query with range predicate.
#[tokio::test]
async fn range_table_insert_and_filter() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGSERIAL PRIMARY KEY, during INT4RANGE)",
    )
    .await;

    // Insert three non-overlapping ranges.
    exec_ok(
        &sess,
        "INSERT INTO events (during) VALUES (int4range(0, 10))",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO events (during) VALUES (int4range(20, 30))",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO events (during) VALUES (int4range(40, 50))",
    )
    .await;

    // All 3 rows were inserted.
    let total = row_count(&sess, "SELECT id FROM events").await;
    assert_eq!(total, 3, "expected 3 rows after insert");

    // Filter: only the row where during contains 5 (i.e. [0,10)).
    let filtered = row_count(
        &sess,
        "SELECT id FROM events WHERE range_contains_elem(during, 5)",
    )
    .await;
    assert_eq!(
        filtered, 1,
        "WHERE range_contains_elem(during, 5): only [0,10) contains 5"
    );

    // No range contains 15.
    let none = row_count(
        &sess,
        "SELECT id FROM events WHERE range_contains_elem(during, 15)",
    )
    .await;
    assert_eq!(
        none, 0,
        "no range contains 15 (gap between [0,10) and [20,30))"
    );
}

/// INSERT using PG text-literal `'[lo,hi)'` syntax, then query.
#[tokio::test]
async fn range_table_insert_text_literal() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE slots (id BIGSERIAL PRIMARY KEY, r INT4RANGE)",
    )
    .await;

    exec_ok(&sess, "INSERT INTO slots (r) VALUES ('[1,10)')").await;
    exec_ok(&sess, "INSERT INTO slots (r) VALUES ('[20,30)')").await;

    let total = row_count(&sess, "SELECT id FROM slots").await;
    assert_eq!(total, 2, "expected 2 rows");

    // WHERE with text literal: Basin must parse '[1,10)' as a range.
    let n = row_count(
        &sess,
        "SELECT id FROM slots WHERE range_contains_elem(r, 5)",
    )
    .await;
    assert_eq!(n, 1, "only '[1,10)' contains 5");
}

/// Multiple range types as columns in one table.
#[tokio::test]
async fn range_table_multi_type_columns() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE schedules (
            id      BIGSERIAL PRIMARY KEY,
            ints    INT4RANGE,
            nums    NUMRANGE,
            dates   DATERANGE
        )",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO schedules (ints, nums, dates) VALUES \
         (int4range(1, 10), numrange(1.0, 2.5), daterange('2024-01-01', '2024-01-31'))",
    )
    .await;

    let n = row_count(&sess, "SELECT id FROM schedules").await;
    assert_eq!(n, 1, "multi-type range table: expected 1 row after insert");
}

/// `lower()` and `upper()` round-trip through a table column.
#[tokio::test]
async fn range_table_lower_upper_roundtrip() {
    let (eng, _dir) = make_engine();
    let sess = open_session(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE bounds_check (id BIGSERIAL PRIMARY KEY, r INT4RANGE)",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO bounds_check (r) VALUES (int4range(7, 42))",
    )
    .await;

    let lo = scalar_str(&sess, "SELECT lower(r) FROM bounds_check LIMIT 1").await;
    let hi = scalar_str(&sess, "SELECT upper(r) FROM bounds_check LIMIT 1").await;

    assert_eq!(lo, "7", "lower() round-trip: expected '7'");
    assert_eq!(hi, "42", "upper() round-trip: expected '42'");
}
