//! Phase 5.11.A — PG-compat scalar function coverage test.
//!
//! Exercises the date/time + string + math + null-handling subset of the
//! tier-1 catalogue so we can paste-port any modern SaaS schema without
//! tripping on a missing function. For each function we assert
//!
//!   * the engine plans + executes without panic, and
//!   * the result equals what real Postgres returns for the same input.
//!
//! Expected values are recorded from `psql` against PG 16; where DataFusion's
//! semantics diverge from PG in a non-trivial way (see notes inline) we
//! document the divergence rather than coerce the result here — fixing those
//! is a follow-up PR.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Date32Array;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    StringArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, IntervalUnit};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn open_engine() -> (TempDir, Engine) {
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
    (dir, engine)
}

/// Convenience: run `sql`, return the first row's first column rendered as
/// a String. Panics on engine error or on shape mismatch — every callsite
/// supplies a SELECT that produces exactly one row × one column.
async fn one_string(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches
                .first()
                .unwrap_or_else(|| panic!("no batches: {sql}"));
            assert_eq!(
                b.num_rows(),
                1,
                "expected 1 row from {sql}, got {}",
                b.num_rows()
            );
            assert_eq!(b.num_columns(), 1, "expected 1 column from {sql}");
            let col = b.column(0);
            render_scalar(col.as_ref(), 0)
        }
        Ok(other) => panic!("non-rows result for {sql}: {other:?}"),
        Err(e) => panic!("execute {sql}: {e}"),
    }
}

fn render_scalar(arr: &dyn Array, i: usize) -> String {
    if arr.is_null(i) {
        return "<NULL>".to_string();
    }
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Date32 => {
            // Days since 1970-01-01 — render as ISO calendar date so test
            // assertions read like the PG output.
            let days = arr.as_any().downcast_ref::<Date32Array>().unwrap().value(i);
            chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .map(|d| d.to_string())
                .unwrap_or_else(|| format!("Date32({days})"))
        }
        DataType::Timestamp(_, _) => {
            // We only care that the value round-trips deterministically
            // for this test; rendering as RFC3339 is enough.
            let ns = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(i);
            let secs = ns / 1_000_000_000;
            let sub_ns = (ns % 1_000_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, sub_ns)
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| format!("Timestamp({ns})"))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = arr
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap()
                .value(i);
            format!("{} mons {} days {} ns", v.months, v.days, v.nanoseconds)
        }
        DataType::Null => "<NULL>".to_string(),
        other => format!("<unhandled {other:?}>"),
    }
}

#[tokio::test]
async fn pg_compat_string_functions() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_string(&sess, "SELECT lower('Hello WORLD')").await,
        "hello world"
    );
    assert_eq!(
        one_string(&sess, "SELECT upper('Hello WORLD')").await,
        "HELLO WORLD"
    );
    // PG `length` returns INT4. Both the chrono rendering and the integer
    // value must match.
    assert_eq!(one_string(&sess, "SELECT length('hello')").await, "5");
    assert_eq!(one_string(&sess, "SELECT length('')").await, "0");

    // SUBSTRING — both PG-syntax and 3-arg overload.
    assert_eq!(
        one_string(&sess, "SELECT substring('abcdef' FROM 2 FOR 3)").await,
        "bcd"
    );
    assert_eq!(
        one_string(&sess, "SELECT substring('abcdef', 2, 3)").await,
        "bcd"
    );
    // PG: substring returns trailing slice when length omitted.
    assert_eq!(
        one_string(&sess, "SELECT substring('abcdef' FROM 3)").await,
        "cdef"
    );

    // TRIM — PG default is BOTH whitespace.
    assert_eq!(one_string(&sess, "SELECT trim('  hi  ')").await, "hi");
    assert_eq!(
        one_string(&sess, "SELECT trim(BOTH 'x' FROM 'xxhixx')").await,
        "hi"
    );

    // POSITION returns INT4 in PG — 1-indexed, 0 for not-found.
    assert_eq!(
        one_string(&sess, "SELECT position('cd' IN 'abcdef')").await,
        "3"
    );
    assert_eq!(
        one_string(&sess, "SELECT position('zz' IN 'abcdef')").await,
        "0"
    );

    // REPLACE: every match.
    assert_eq!(
        one_string(&sess, "SELECT replace('aXbXc', 'X', '-')").await,
        "a-b-c"
    );

    // REGEXP_REPLACE: first match by default in PG; DataFusion replaces
    // first as well unless the `g` flag is supplied. Both signatures match
    // here (no flag arg).
    assert_eq!(
        one_string(
            &sess,
            "SELECT regexp_replace('abc123def456', '[0-9]+', 'N')"
        )
        .await,
        "abcNdef456"
    );
    // With the `g` flag PG replaces every match.
    assert_eq!(
        one_string(
            &sess,
            "SELECT regexp_replace('abc123def456', '[0-9]+', 'N', 'g')"
        )
        .await,
        "abcNdefN"
    );

    // String concat.
    assert_eq!(
        one_string(&sess, "SELECT 'foo' || '-' || 'bar'").await,
        "foo-bar"
    );
}

#[tokio::test]
async fn pg_compat_math_functions() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(one_string(&sess, "SELECT abs(-7)").await, "7");
    assert_eq!(one_string(&sess, "SELECT abs(7)").await, "7");

    // ceil / floor on a fractional float — PG returns numeric/double.
    assert_eq!(one_string(&sess, "SELECT ceil(1.4)").await, "2");
    assert_eq!(one_string(&sess, "SELECT floor(1.7)").await, "1");

    // round 1.5 — DataFusion rounds half-away-from-zero (yields 2). PG
    // does the same for `numeric` rounding; for `double precision`
    // banker's rounding (yields 2 here too). We assert the integer-side
    // string.
    assert_eq!(one_string(&sess, "SELECT round(1.5)").await, "2");

    // POWER: PG returns `double precision`. The Basin override widens the
    // result type to Float64 unconditionally so callers that arithmetic
    // on the result with a float don't hit type-coercion friction.
    assert_eq!(one_string(&sess, "SELECT power(2, 8)").await, "256");
    // Return type must be Float64 (not Int64) — verify by exercising a
    // float-aware operation on the result. `power(2,8) + 0.5` must
    // succeed without integer-truncation.
    assert_eq!(one_string(&sess, "SELECT power(2, 8) + 0.5").await, "256.5");
    // Fractional exponent — PG: `power(2.5, 2) = 6.25`.
    assert_eq!(one_string(&sess, "SELECT power(2.5, 2)").await, "6.25");
    // PG-shape edge case: power(0, 0) = 1.
    assert_eq!(one_string(&sess, "SELECT power(0, 0)").await, "1");

    // SQRT — Float64 in both PG and DF.
    assert_eq!(one_string(&sess, "SELECT sqrt(16)").await, "4");

    // MOD function + `%` operator must agree.
    assert_eq!(one_string(&sess, "SELECT mod(10, 3)").await, "1");
    assert_eq!(one_string(&sess, "SELECT 10 % 3").await, "1");
    // Negative modulus — PG keeps sign of dividend, like Rust's `%`.
    assert_eq!(one_string(&sess, "SELECT mod(-10, 3)").await, "-1");
}

#[tokio::test]
async fn pg_compat_null_handling() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // COALESCE.
    assert_eq!(
        one_string(&sess, "SELECT coalesce(NULL, NULL, 'x', 'y')").await,
        "x"
    );
    // Bare `coalesce(NULL, NULL)` plans as DataType::Null; the
    // workspace-arrow bridge models that as a NullArray, which renders to
    // SQL NULL in the row protocol. No CAST workaround required.
    assert_eq!(
        one_string(&sess, "SELECT coalesce(NULL, NULL)").await,
        "<NULL>"
    );

    // NULLIF.
    assert_eq!(one_string(&sess, "SELECT nullif(5, 5)").await, "<NULL>");
    assert_eq!(one_string(&sess, "SELECT nullif(5, 7)").await, "5");

    // GREATEST / LEAST. PG ignores NULLs; DataFusion does too.
    assert_eq!(
        one_string(&sess, "SELECT greatest(2, 9, 4, NULL)").await,
        "9"
    );
    assert_eq!(one_string(&sess, "SELECT least(2, 9, 4, NULL)").await, "2");

    // IS DISTINCT FROM treats NULLs as comparable values: NULL is NOT
    // distinct from NULL, but NULL IS distinct from any non-NULL.
    assert_eq!(
        one_string(&sess, "SELECT 1 IS DISTINCT FROM 1").await,
        "false"
    );
    assert_eq!(
        one_string(&sess, "SELECT 1 IS DISTINCT FROM 2").await,
        "true"
    );
    assert_eq!(
        one_string(&sess, "SELECT NULL IS DISTINCT FROM NULL").await,
        "false"
    );
    assert_eq!(
        one_string(&sess, "SELECT NULL IS NOT DISTINCT FROM NULL").await,
        "true"
    );
    assert_eq!(
        one_string(&sess, "SELECT 1 IS NOT DISTINCT FROM NULL").await,
        "false"
    );
}

#[tokio::test]
async fn pg_compat_datetime_functions() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // now() and current_timestamp must agree within a statement.
    let res = sess
        .execute("SELECT now() = current_timestamp")
        .await
        .expect("now == current_timestamp");
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first().unwrap();
        let arr = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(
            arr.value(0),
            "now() must equal current_timestamp inside one statement"
        );
    } else {
        panic!("expected rows");
    }

    // current_date — type must be Date32 and the value must be today
    // (test runs in the same wall-clock minute, so `current_date` cannot
    // disagree by more than a calendar day in pathological time-zone
    // edge cases; we assert "is a parseable date").
    let res = sess
        .execute("SELECT current_date")
        .await
        .expect("current_date");
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first().unwrap();
        assert_eq!(b.column(0).data_type(), &DataType::Date32);
    }

    // date_trunc — every required unit.
    for (unit, expected_iso) in [
        ("second", "2024-05-09T12:34:56+00:00"),
        ("minute", "2024-05-09T12:34:00+00:00"),
        ("hour", "2024-05-09T12:00:00+00:00"),
        ("day", "2024-05-09T00:00:00+00:00"),
        ("month", "2024-05-01T00:00:00+00:00"),
        ("quarter", "2024-04-01T00:00:00+00:00"),
        ("year", "2024-01-01T00:00:00+00:00"),
    ] {
        let q = format!("SELECT date_trunc('{unit}', TIMESTAMP '2024-05-09 12:34:56')");
        assert_eq!(
            one_string(&sess, &q).await,
            expected_iso,
            "date_trunc('{unit}')"
        );
    }
    // `week` truncates to Monday in PG; same in DataFusion.
    // 2024-05-09 is a Thursday → week start = 2024-05-06 (Monday).
    assert_eq!(
        one_string(
            &sess,
            "SELECT date_trunc('week', TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "2024-05-06T00:00:00+00:00"
    );

    // EXTRACT — every required field.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(year FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "2024"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(month FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "5"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(day FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "9"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(hour FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "12"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(minute FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "34"
    );
    // `extract(second FROM ...)` — Basin's PG-shape override returns
    // Float64 with sub-second precision (PG returns `numeric`; we use
    // Float64 because the workspace arrow bridge has no `numeric` type).
    // For an exact-second input the textual rendering matches PG.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(second FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "56"
    );
    // 2024-05-09 is a Thursday — PG `dow` is Sunday-0 → Thursday = 4.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(dow FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "4"
    );
    // 2024-05-09 → day 130 of year (2024 is a leap year).
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(doy FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "130"
    );
    // EPOCH — seconds since unix epoch.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(epoch FROM TIMESTAMP '2024-05-09 12:34:56')"
        )
        .await,
        "1715258096"
    );

    // to_char / to_timestamp PG-format round-trip.
    assert_eq!(
        one_string(
            &sess,
            "SELECT to_char(TIMESTAMP '2024-05-09 12:34:56', 'YYYY-MM-DD HH24:MI:SS')"
        )
        .await,
        "2024-05-09 12:34:56"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT to_char(TIMESTAMP '2024-05-09 12:34:56', 'YYYY/MM/DD')"
        )
        .await,
        "2024/05/09"
    );
    // Round-trip: parse back using to_timestamp then format again.
    assert_eq!(
        one_string(
            &sess,
            "SELECT to_char(to_timestamp('2024-05-09 12:34:56', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')"
        )
        .await,
        "2024-05-09 12:34:56"
    );

    // age() returns a native Interval(MonthDayNano). The render helper
    // formats the (months, days, nanoseconds) triple as "M mons D days N ns"
    // — a stable test rendering, not what PG's text protocol would emit.
    // The classic 11-month-and-30-day case:
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-12-31', TIMESTAMP '2024-01-01')"
        )
        .await,
        "11 mons 30 days 0 ns"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2025-05-09', TIMESTAMP '2024-05-09')"
        )
        .await,
        "12 mons 0 days 0 ns"
    );
    // Sub-day component: 2h 34m 56s = 9296 seconds = 9_296_000_000_000 ns.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-05-09 12:34:56', TIMESTAMP '2024-05-09 10:00:00')"
        )
        .await,
        "0 mons 0 days 9296000000000 ns"
    );
}

#[tokio::test]
async fn pg_compat_chrono_format_passthrough() {
    // Make sure we didn't break callers who already use chrono-style
    // format strings. The PG-format wrappers register under the same
    // names but pass `%`-prefixed directives through unchanged.
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_string(
            &sess,
            "SELECT to_char(TIMESTAMP '2024-05-09 12:34:56', '%Y-%m-%d')"
        )
        .await,
        "2024-05-09"
    );
    assert_eq!(
        one_string(
            &sess,
            "SELECT to_char(to_timestamp('2024-05-09 12:34:56', '%Y-%m-%d %H:%M:%S'), '%H:%M:%S')"
        )
        .await,
        "12:34:56"
    );
}

/// Bare `coalesce(NULL, NULL)` no longer requires a CAST: the bridge models
/// `DataType::Null` as a NullArray.
#[tokio::test]
async fn coalesce_untyped_null() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let res = sess
        .execute("SELECT coalesce(NULL, NULL)")
        .await
        .expect("coalesce(NULL, NULL) plans + executes");
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first().expect("one batch");
        assert_eq!(b.num_rows(), 1);
        assert_eq!(b.num_columns(), 1);
        let col = b.column(0);
        // The result column lands as DataType::Null — a NullArray of length
        // 1 where every row is logically null. NullArray has no null buffer,
        // so `is_null` returns false; assert via the typed shape instead.
        assert_eq!(
            col.data_type(),
            &DataType::Null,
            "coalesce(NULL, NULL) must plan as DataType::Null"
        );
        assert_eq!(col.len(), 1);
        assert_eq!(col.logical_null_count(), 1, "row 0 is logically null");
    } else {
        panic!("expected rows");
    }
}

/// `age()` returns a native `interval`, so interval arithmetic on the result
/// type-checks. We assert the column data-type rather than poke at PG's text
/// shape (the test render helper formats the triple).
#[tokio::test]
async fn age_returns_interval_type() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let res = sess
        .execute("SELECT age(TIMESTAMP '2024-12-31', TIMESTAMP '2024-01-01')")
        .await
        .expect("age plans");
    if let ExecResult::Rows { batches, .. } = res {
        let b = batches.first().expect("one batch");
        assert_eq!(
            b.column(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano),
            "age() must return Interval(MonthDayNano)"
        );
    } else {
        panic!("expected rows");
    }
}

/// PG's `age()` calendar walk borrows day-counts from the **earlier**
/// timestamp's month, not the later one. The test cases below come straight
/// from PG's `timestamp_age` source. Values were cross-checked against a
/// reference PG run; document any divergence in the PR report.
#[tokio::test]
async fn age_calendar_walk_edge_cases() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // The 5.11.A baseline: Dec 31 minus Jan 1 of the same year. No
    // borrowing required — straight component subtraction.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-12-31', TIMESTAMP '2024-01-01')",
        )
        .await,
        "11 mons 30 days 0 ns"
    );

    // Mar 1 minus Jan 31 = 1 mon 1 day in PG. The day-component starts at
    // -30; PG borrows January's 31 days (the **earlier** ts's month), so
    // days = 1 and months = 1.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-03-01', TIMESTAMP '2024-01-31')",
        )
        .await,
        "1 mons 1 days 0 ns"
    );

    // Mar 30 minus Feb 29 = 1 mon 1 day. No borrowing — d=30-29=1, m=1.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-03-30', TIMESTAMP '2024-02-29')",
        )
        .await,
        "1 mons 1 days 0 ns"
    );

    // Negative interval: ts1 < ts2 flips sign. Jan 1 minus Dec 31 of the
    // prior calendar year: same magnitude, opposite sign.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-01-01', TIMESTAMP '2024-12-31')",
        )
        .await,
        "-11 mons -30 days 0 ns"
    );

    // Equal timestamps: zero interval.
    assert_eq!(
        one_string(
            &sess,
            "SELECT age(TIMESTAMP '2024-05-09 12:34:56', TIMESTAMP '2024-05-09 12:34:56')",
        )
        .await,
        "0 mons 0 days 0 ns"
    );
}

/// `extract(second FROM ts)` must return Float64 with sub-second precision.
/// PG returns `numeric` here; Basin returns Float64 (the workspace arrow
/// bridge has no `numeric` type yet — Float64 is microsecond-lossless and
/// is the practical fix). DataFusion's default `date_part('second', ts)`
/// returns Int32 (whole seconds only) — that's the bug this test pins.
#[tokio::test]
async fn pg_compat_extract_second_precision() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Microsecond fraction must round-trip — PG: 42.123456.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(second FROM TIMESTAMP '2026-05-09 12:00:42.123456')",
        )
        .await,
        "42.123456"
    );

    // Half-second on a whole-second-and-a-half input.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(second FROM TIMESTAMP '2026-05-09 12:00:00.5')",
        )
        .await,
        "0.5"
    );

    // Whole-second input still renders cleanly (Float64 `56.0` → "56").
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(second FROM TIMESTAMP '2024-05-09 12:34:56')",
        )
        .await,
        "56"
    );

    // Sub-second value must arithmetic with a Float64. If extract still
    // returned Int32 the planner would reject this with a type-coercion
    // error or silently truncate. Adding 0.0 is the cheapest probe.
    assert_eq!(
        one_string(
            &sess,
            "SELECT extract(second FROM TIMESTAMP '2026-05-09 12:00:42.5') + 0.5",
        )
        .await,
        "43"
    );
}
