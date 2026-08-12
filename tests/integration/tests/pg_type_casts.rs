//! PG-compatible type cast and conversion integration tests.
//!
//! Exercises the cast forms that DataFusion handles natively (via
//! `PostgreSqlDialect`) plus the PG-specific UDFs added in Phase 5.11.A /
//! Phase 5.11.N:
//!
//! | Cast form                              | Capability row                        |
//! |----------------------------------------|---------------------------------------|
//! | `CAST(x AS type)` SQL standard form    | explicit CAST syntax                  |
//! | `x::type` PG shorthand                 | `::` cast operator                    |
//! | `'42'::int` string → integer           | string-to-integer implicit cast       |
//! | `'1.5'::float8` string → float         | string-to-float implicit cast         |
//! | `'1.50'::numeric(10,2)` str → numeric  | string-to-numeric (precision) cast    |
//! | `1::text` integer → text               | integer-to-text cast                  |
//! | `42::bigint` int4 → int8 promotion     | int4 → int8 numeric promotion         |
//! | `'2024-01-15'::date` string → date     | string-to-date literal cast           |
//! | `CAST(42 AS double precision)`         | int → float explicit cast             |
//! | `to_number('1,234.56', '9,999.99')`    | PG to_number format function          |
//! | `to_date('2024-01-15', 'YYYY-MM-DD')`  | PG to_date format function            |
//! | `to_char(date, 'YYYY-MM-DD')`          | PG to_char format function (date)     |
//! | `CAST(score AS BIGINT)` float → int    | float-to-integer truncating cast      |

use std::sync::Arc;

use arrow_array::{Array, Date32Array, Float64Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Helper: run a SELECT returning one row / one column of type i64.
async fn select_i64(s: &basin_engine::ProjectSession, sql: &str) -> i64 {
    let ExecResult::Rows { batches, .. } = s
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql:?} — {e}"))
    else {
        panic!("expected Rows from {sql:?}");
    };
    let mut vals = Vec::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64Array from {sql:?}"));
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    assert_eq!(
        vals.len(),
        1,
        "expected 1 row from {sql:?}, got {}",
        vals.len()
    );
    vals[0]
}

/// Helper: run a SELECT returning one row / one column of type f64.
async fn select_f64(s: &basin_engine::ProjectSession, sql: &str) -> f64 {
    let ExecResult::Rows { batches, .. } = s
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql:?} — {e}"))
    else {
        panic!("expected Rows from {sql:?}");
    };
    let mut vals = Vec::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("expected Float64Array from {sql:?}"));
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    assert_eq!(
        vals.len(),
        1,
        "expected 1 row from {sql:?}, got {}",
        vals.len()
    );
    vals[0]
}

/// Helper: run a SELECT returning one row / one column of type text.
async fn select_str(s: &basin_engine::ProjectSession, sql: &str) -> String {
    let ExecResult::Rows { batches, .. } = s
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql:?} — {e}"))
    else {
        panic!("expected Rows from {sql:?}");
    };
    let mut vals = Vec::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("expected StringArray from {sql:?}"));
        for i in 0..arr.len() {
            vals.push(arr.value(i).to_owned());
        }
    }
    assert_eq!(
        vals.len(),
        1,
        "expected 1 row from {sql:?}, got {}",
        vals.len()
    );
    vals[0].clone()
}

// ─── CAST(x AS type) — SQL standard form ────────────────────────────────────

/// `CAST(x AS type)` SQL-standard explicit cast syntax works end-to-end
/// through the DataFusion planner.
#[tokio::test]
async fn cast_sql_standard_explicit() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    // integer literal → bigint (identity-ish)
    let v = select_i64(&s, "SELECT CAST(42 AS BIGINT)").await;
    assert_eq!(v, 42, "CAST(42 AS BIGINT)");

    // The decimal-literal → BIGINT case used to live here, asserting
    // `CAST(3.9 AS BIGINT) == 3` under the comment "truncates to 3". Postgres
    // returns 4. It now has its own test — see
    // `cast_numeric_literal_to_integer_rounds` below — so that this test, which
    // is about *syntax* reaching the planner, stays green while the rounding
    // fidelity gap is tracked separately rather than being silently bundled in.
}

/// `CAST(3.9 AS BIGINT)` — a bare decimal literal is **`numeric`** in Postgres,
/// not `float8` (`SELECT pg_typeof(3.9)` → `numeric`), so this exercises the
/// numeric→int cast, which rounds **half away from zero**.
///
/// Measured on PG 18.2:
///
/// ```text
/// SELECT CAST(3.9 AS BIGINT);   -- 4
/// SELECT CAST(0.5 AS BIGINT);   -- 1
/// SELECT CAST(2.5 AS BIGINT);   -- 3
/// SELECT CAST(-2.5 AS BIGINT);  -- -3
/// ```
///
/// Contrast `cast_float_to_integer_rounds`, where the source is a genuine
/// `float8` column and ties go to **even** (`0.5 → 0`, `2.5 → 2`). Same SQL
/// keyword, two different tie rules, selected by the source type. Neither
/// truncates. See `docs/migration/df-removal/12-pg-type-fidelity.md` §8.
///
/// # Why this is `#[ignore]`d
///
/// Basin truncates here too, returning 3 where PG returns 4. Known fidelity
/// gap, tracked in the doc above. The assertion states Postgres semantics and
/// must not be weakened back to truncation; remove the `#[ignore]` once the
/// numeric cast path rounds half away from zero.
#[tokio::test]
#[ignore = "Basin truncates numeric->int; PG rounds half away from zero. Known \
            fidelity gap — see docs/migration/df-removal/12-pg-type-fidelity.md \
            §8. Do not fix by weakening the assertion."]
async fn cast_numeric_literal_to_integer_rounds() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_i64(&s, "SELECT CAST(3.9 AS BIGINT)").await;
    assert_eq!(v, 4, "CAST(3.9 AS BIGINT): PG rounds to 4, does not truncate");

    // Ties: numeric rounds AWAY FROM ZERO (unlike float8, which goes to even).
    let v = select_i64(&s, "SELECT CAST(0.5 AS BIGINT)").await;
    assert_eq!(v, 1, "CAST(0.5 AS BIGINT): numeric tie away from zero → 1");

    let v = select_i64(&s, "SELECT CAST(2.5 AS BIGINT)").await;
    assert_eq!(v, 3, "CAST(2.5 AS BIGINT): numeric tie away from zero → 3");

    let v = select_i64(&s, "SELECT CAST(-2.5 AS BIGINT)").await;
    assert_eq!(v, -3, "CAST(-2.5 AS BIGINT): numeric tie away from zero → -3");
}

// ─── x::type — PG shorthand cast ────────────────────────────────────────────

/// `x::type` PG shorthand is parsed by `PostgreSqlDialect` and lowered to the
/// same CAST node — must produce the same result as the SQL-standard form.
#[tokio::test]
async fn cast_pg_double_colon_operator() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_i64(&s, "SELECT 42::BIGINT").await;
    assert_eq!(v, 42, "42::BIGINT");

    let v = select_f64(&s, "SELECT 1::FLOAT8").await;
    assert!((v - 1.0).abs() < f64::EPSILON, "1::FLOAT8 == 1.0");
}

// ─── string literal → integer ───────────────────────────────────────────────

/// `'42'::int` — string literal cast to integer. PG allows this; Basin must
/// route it through DataFusion's CAST(Utf8 → Int64).
#[tokio::test]
async fn cast_string_to_integer() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_i64(&s, "SELECT '42'::BIGINT").await;
    assert_eq!(v, 42, "'42'::BIGINT");

    let v = select_i64(&s, "SELECT CAST('-7' AS BIGINT)").await;
    assert_eq!(v, -7, "CAST('-7' AS BIGINT)");
}

// ─── string literal → float ─────────────────────────────────────────────────

/// `'1.5'::float8` — string literal cast to double precision.
#[tokio::test]
async fn cast_string_to_float() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_f64(&s, "SELECT '1.5'::FLOAT8").await;
    assert!((v - 1.5).abs() < 1e-10, "'1.5'::FLOAT8 == 1.5");

    let v = select_f64(&s, "SELECT CAST('3.14' AS DOUBLE PRECISION)").await;
    // 3.14 is the literal being cast, not an approximation of π.
    #[allow(clippy::approx_constant)]
    {
        assert!((v - 3.14).abs() < 1e-10, "CAST('3.14' AS DOUBLE PRECISION)");
    }
}

// ─── string literal → numeric ────────────────────────────────────────────────

/// `'1.50'::numeric(10,2)` — string literal cast to NUMERIC. Basin stores
/// NUMERIC as Arrow Decimal128; the cast must produce the right scaled value.
#[tokio::test]
async fn cast_string_to_numeric_in_table() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE prices (id BIGINT NOT NULL, price NUMERIC(10,2))")
        .await
        .unwrap();
    s.execute("INSERT INTO prices VALUES (1, '1.50'), (2, '99.99')")
        .await
        .unwrap();

    let ExecResult::Rows { batches, .. } = s
        .execute("SELECT id FROM prices WHERE price = '1.50'::NUMERIC(10,2)")
        .await
        .expect("cast string to numeric in WHERE")
    else {
        panic!("expected Rows");
    };
    // We only check the query doesn't error and returns the matching row.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "expected 1 matching row for price = '1.50'");
}

// ─── integer → text ─────────────────────────────────────────────────────────

/// `1::text` — integer to text cast (very common in PG for logging / concat).
#[tokio::test]
async fn cast_integer_to_text() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_str(&s, "SELECT CAST(42 AS TEXT)").await;
    assert_eq!(v, "42", "CAST(42 AS TEXT)");

    let v = select_str(&s, "SELECT 100::TEXT").await;
    assert_eq!(v, "100", "100::TEXT");
}

// ─── int4 → int8 numeric promotion ──────────────────────────────────────────

/// `INTEGER` (int4) promoted to `BIGINT` (int8) — common in arithmetic
/// with mixed-width columns. DataFusion handles this via the type coercion
/// rules; we verify it passes through the Basin engine without error.
#[tokio::test]
async fn cast_int4_to_int8_promotion() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE nums (id BIGINT NOT NULL, small INTEGER)")
        .await
        .unwrap();
    s.execute("INSERT INTO nums VALUES (1, 32767)")
        .await
        .unwrap();

    // Selecting the INTEGER column and casting it to BIGINT.
    let ExecResult::Rows { batches, .. } = s
        .execute("SELECT CAST(small AS BIGINT) FROM nums WHERE id = 1")
        .await
        .expect("int4 -> int8 cast")
    else {
        panic!("expected Rows");
    };
    let mut vals = Vec::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    assert_eq!(vals, vec![32767_i64], "int4 -> int8: 32767");
}

// ─── string → date literal cast ──────────────────────────────────────────────

/// `'2024-01-15'::date` — string literal cast to Date32. Exercises the
/// PG-specific `::date` syntax which DataFusion's PostgreSqlDialect parses
/// as `CAST('2024-01-15' AS DATE)`.
#[tokio::test]
async fn cast_string_to_date() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    // Verify the cast doesn't error. DataFusion returns Date32 (days since epoch).
    let ExecResult::Rows { batches, .. } = s
        .execute("SELECT '2024-01-15'::DATE AS d")
        .await
        .expect("string ::date cast")
    else {
        panic!("expected Rows");
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "expected 1 row from date cast");

    // Confirm the returned type is Date32 and the value is 2024-01-15.
    // 2024-01-15 is day 19737 since 1970-01-01.
    let batch = &batches[0];
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Date32Array for ::date cast");
    // 2024-01-15: days from 1970-01-01. Compute via chrono.
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let expected_days = expected.signed_duration_since(epoch).num_days() as i32;
    assert_eq!(
        arr.value(0),
        expected_days,
        "'2024-01-15'::date days-since-epoch"
    );
}

// ─── float → integer rounding cast ──────────────────────────────────────────

/// `CAST(score AS BIGINT)` from a FLOAT8 column — Postgres **rounds**, it does
/// not truncate. `3.9 → 4`, `-2.7 → -3`.
///
/// The tie rule differs by source type, and the difference is load-bearing:
///
/// - `float4`/`float8` → integer rounds **half to even** (banker's rounding),
///   because the cast goes through the C library's `rint()` under the default
///   IEEE-754 rounding mode: `0.5 → 0`, `1.5 → 2`, `2.5 → 2`, `-0.5 → 0`.
/// - `numeric` → integer rounds **half away from zero**, since `numeric` is a
///   decimal type that never touches `rint()`: `0.5 → 1`, `1.5 → 2`,
///   `2.5 → 3`, `-0.5 → -1`.
///
/// They agree on every input whose fraction is not exactly `.5`, which is what
/// makes the distinction easy to conflate. Truncation toward zero is `trunc()`,
/// a separate function — a cast is not a call to it.
///
/// Verified against PostgreSQL 18.2; see the docs' §8.1 Numeric Types: "When
/// rounding values, the `numeric` type rounds ties away from zero, while (on
/// most machines) the `real` and `double precision` types round ties to the
/// nearest even number."
/// <https://www.postgresql.org/docs/18/datatype-numeric.html>
///
/// This test previously asserted `3.9 → 3` and `-2.7 → -2` under the comment
/// "PG truncates toward zero", which pinned behaviour Postgres does not have.
///
/// # Why this is `#[ignore]`d
///
/// Basin currently **truncates** float→int casts, yielding
/// `[3, -2, 0, 1, 2, 0]` where Postgres gives `[4, -3, 0, 2, 2, 0]`. That is a
/// known fidelity gap, tracked in
/// `docs/migration/df-removal/12-pg-type-fidelity.md` §8.
///
/// The assertion is deliberately left stating **Postgres** semantics rather
/// than Basin's. It is ignored, not weakened: the engine is not to be changed
/// to chase this test, and the expectation is not to be relaxed back to
/// truncation. Remove the `#[ignore]` when the cast path implements
/// round-half-to-even — the test then passes as written and becomes the
/// regression guard.
#[tokio::test]
#[ignore = "Basin truncates float->int; PG rounds half-to-even. Known fidelity \
            gap — see docs/migration/df-removal/12-pg-type-fidelity.md §8. \
            Do not fix by weakening the assertion."]
async fn cast_float_to_integer_rounds() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE scores (id BIGINT NOT NULL, score DOUBLE PRECISION)")
        .await
        .unwrap();
    // Rows 1-2 are the plain rounding cases; rows 3-6 are the exact ties that
    // distinguish half-to-even (float) from half-away-from-zero (numeric).
    s.execute(
        "INSERT INTO scores VALUES \
         (1, 3.9), (2, -2.7), (3, 0.5), (4, 1.5), (5, 2.5), (6, -0.5)",
    )
    .await
    .unwrap();

    let ExecResult::Rows { batches, .. } = s
        .execute("SELECT CAST(score AS BIGINT) FROM scores ORDER BY id")
        .await
        .expect("float -> bigint cast")
    else {
        panic!("expected Rows");
    };
    let mut vals = Vec::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..arr.len() {
            vals.push(arr.value(i));
        }
    }
    // PG rounds, half to even for float sources:
    //   3.9 → 4, -2.7 → -3, 0.5 → 0, 1.5 → 2, 2.5 → 2, -0.5 → 0.
    // Truncation would give [3, -2, 0, 1, 2, 0] — note it coincides on 0.5 and
    // -0.5, so the 1.5/2.5 pair is what actually pins the tie rule.
    assert_eq!(
        vals,
        vec![4, -3, 0, 2, 2, 0],
        "float-to-int rounds half to even (PG 18.2), it does not truncate"
    );
}

// ─── to_number(text, format) ─────────────────────────────────────────────────

/// `to_number('1,234.56', '9,999.99')` — PG format-driven numeric parse.
/// Returns Float64. Verifies the basin `to_number` UDF handles thousands
/// separators and decimal point correctly.
#[tokio::test]
async fn to_number_with_format() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_f64(&s, "SELECT to_number('1234.56', '9999.99')").await;
    assert!(
        (v - 1234.56).abs() < 1e-6,
        "to_number without separator: {v}"
    );

    let v = select_f64(&s, "SELECT to_number('1,234.56', '9,999.99')").await;
    assert!(
        (v - 1234.56).abs() < 1e-6,
        "to_number with thousands sep: {v}"
    );

    let v = select_f64(&s, "SELECT to_number('-42', 'S99')").await;
    assert!((v - (-42.0)).abs() < 1e-6, "to_number negative: {v}");
}

// ─── to_date(text, format) ───────────────────────────────────────────────────

/// `to_date('2024-01-15', 'YYYY-MM-DD')` — PG format-driven date parse.
/// Returns Date32. Verifies the basin `to_date` UDF handles the PG format
/// picture and produces the correct days-since-epoch value.
#[tokio::test]
async fn to_date_with_format() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let ExecResult::Rows { batches, .. } = s
        .execute("SELECT to_date('2024-01-15', 'YYYY-MM-DD')")
        .await
        .expect("to_date query")
    else {
        panic!("expected Rows");
    };
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Date32Array from to_date");
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let expected_days = expected.signed_duration_since(epoch).num_days() as i32;
    assert_eq!(arr.value(0), expected_days, "to_date: days since epoch");

    // Alternative format.
    let ExecResult::Rows { batches: b2, .. } = s
        .execute("SELECT to_date('15/01/2024', 'DD/MM/YYYY')")
        .await
        .expect("to_date alt format")
    else {
        panic!("expected Rows");
    };
    let arr2 = b2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("Date32Array from to_date alt");
    assert_eq!(arr2.value(0), expected_days, "to_date alt format same day");
}

// ─── to_char(date, format) ───────────────────────────────────────────────────

/// `to_char(current_date, 'YYYY-MM-DD')` — renders a date column as a
/// formatted string. Verifies the existing `to_char` UDF handles Date32 input.
#[tokio::test]
#[ignore = "DATE INSERT coercion not yet wired into the bulk-insert builder; tracked as v0.2 work"]
async fn to_char_date_format() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    s.execute("CREATE TABLE events (id BIGINT NOT NULL, dt DATE)")
        .await
        .unwrap();
    s.execute("INSERT INTO events VALUES (1, '2024-03-07')")
        .await
        .unwrap();

    let v = select_str(
        &s,
        "SELECT to_char(dt, 'YYYY-MM-DD') FROM events WHERE id = 1",
    )
    .await;
    assert_eq!(v, "2024-03-07", "to_char date formatting");
}

// ─── int → float explicit widening cast ─────────────────────────────────────

/// `CAST(42 AS double precision)` — integer literal cast to floating-point.
/// Common in PG arithmetic where division must be float-typed.
#[tokio::test]
async fn cast_int_to_double_precision() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    let v = select_f64(
        &s,
        "SELECT CAST(7 AS DOUBLE PRECISION) / CAST(2 AS DOUBLE PRECISION)",
    )
    .await;
    assert!(
        (v - 3.5).abs() < 1e-10,
        "CAST int to double: 7/2 = 3.5, got {v}"
    );
}
