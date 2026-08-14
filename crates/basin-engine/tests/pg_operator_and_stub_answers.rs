//! The `@>` / `<@` / `&&` **operators**, and the stubs the UDF audit found
//! answering plausible wrong values, measured against PostgreSQL 18.2.
//!
//! ## Why the operator cases repeat `array_containment_matches_postgres.rs`
//!
//! They do not. That file calls `array_contains(...)` and `arrays_overlap(...)`
//! **by name**. This one goes through the operator spellings a client actually
//! writes, because for `@>` and `<@` those were two different code paths:
//! `pg_operators::rewrite_array_op_once` sent them to DataFusion's
//! `list_has_all`, not to the fixed `array_contains`. The gap was visible:
//! `list_has_all(ARRAY[1,2,NULL], ARRAY[NULL])` is true, and PostgreSQL's
//! `ARRAY[1,2,NULL] @> ARRAY[NULL]` is **false**. Testing the function name
//! could never have caught it.
//!
//! The `::type[]` cases are the second half. `array_extract_left` widened past
//! a `]` only when the preceding word was `array`, so for `int[]` the whole
//! left operand became the two characters `[]`; `array_extract_right` stopped
//! before the cast entirely. Measured before the fix:
//!
//! * `ARRAY[]::int[] <@ ARRAY[1,2]`  — parse error (PG: `t`)
//! * `ARRAY[1,2] @> ARRAY[]::int[]`  — returned the list `[1]` (PG: `t`)
//! * `ARRAY[1,2] && ARRAY[]::int[]`  — returned the list `[0]` (PG: `f`)
//! * `ARRAY[1,2]::int[] @> ARRAY[1]` — parse error (PG: `t`)
//! * `(ARRAY['a','b'])::varchar(50)[] && ARRAY['b']` — parse error (PG: `t`)
//!
//! ## JSONB is a different route, and stays one
//!
//! `looks_like_array` matches `'{…}'::` , which is also how a JSONB literal is
//! spelled — the reason the reroute was held back. It turns out not to be
//! reachable: `udf::rewrite_json_operators` runs earlier in the executor chain,
//! strips the `::jsonb` cast, and has already replaced `jsonb @> literal` with
//! `jsonb_contains(…)` before the array pass sees the text. The JSONB cases
//! below are here to keep that true, not because they were ever at risk of
//! becoming `array_contains`.
//!
//! ## Every expected value is `psql` output
//!
//! Copied verbatim from the PostgreSQL 18.2 server this branch develops
//! against. Where PostgreSQL raises, the test asserts Basin raises too — for
//! `power` and `date_bin` that is the whole point, since both used to return a
//! confident wrong value (`NaN`, `inf`, and a 30-day month) instead.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn session(dir: &TempDir) -> ProjectSession {
    engine_in(dir).open_session(ProjectId::new()).await.unwrap()
}

/// `"t"`, `"f"` or `"NULL"` — the three spellings `psql` prints, so an expected
/// value can be read straight off a session transcript.
///
/// Deliberately strict about the *type*: the `::type[]` bug did not make these
/// queries fail, it made them return a **list** where a boolean was asked for,
/// so a helper that stringified whatever came back would have passed.
async fn one_bool(sess: &ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            for b in &batches {
                if b.num_columns() == 0 || b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap_or_else(|| {
                        panic!(
                            "{sql}: expected a boolean column, got {:?}",
                            b.column(0).data_type()
                        )
                    });
                return if arr.is_null(0) {
                    "NULL".to_string()
                } else if arr.value(0) {
                    "t".to_string()
                } else {
                    "f".to_string()
                };
            }
            panic!("{sql}: no rows")
        }
        Ok(other) => panic!("{sql}: expected rows, got {other:?}"),
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// First column of the first row rendered the way `psql` renders it, with a
/// SQL NULL as the literal string `NULL`.
async fn one_value(sess: &ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            for b in &batches {
                if b.num_columns() == 0 || b.num_rows() == 0 {
                    continue;
                }
                let col = b.column(0);
                if col.is_null(0) {
                    return "NULL".to_string();
                }
                let f = arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &arrow::util::display::FormatOptions::default(),
                )
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
                return f.value(0).to_string();
            }
            panic!("{sql}: no rows")
        }
        Ok(other) => panic!("{sql}: expected rows, got {other:?}"),
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// The error message, for the cases where PostgreSQL refuses.
async fn one_error(sess: &ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Err(e) => e.to_string(),
        Ok(other) => panic!("{sql}: expected an error, got {other:?}"),
    }
}

async fn check_bools(sess: &ProjectSession, cases: &[(&str, &str)]) {
    for (sql, pg) in cases {
        assert_eq!(&one_bool(sess, sql).await, pg, "{sql}");
    }
}

async fn check_values(sess: &ProjectSession, cases: &[(&str, &str)]) {
    for (sql, pg) in cases {
        assert_eq!(&one_value(sess, sql).await, pg, "{sql}");
    }
}

// ── the operators themselves ─────────────────────────────────────────────────

/// `@>`, `<@` and `&&` written as operators reach PG's semantics, NULL
/// elements included.
#[tokio::test]
async fn array_operators_match_postgres() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_bools(
        &sess,
        &[
            ("SELECT ARRAY[1,2] @> ARRAY[1]", "t"),
            ("SELECT ARRAY[1,2] @> ARRAY[3]", "f"),
            ("SELECT ARRAY[1,2] <@ ARRAY[1,2,3]", "t"),
            ("SELECT ARRAY[1,2,3] <@ ARRAY[1,2]", "f"),
            ("SELECT ARRAY[1,2] && ARRAY[2,3]", "t"),
            ("SELECT ARRAY[1,2] && ARRAY[3,4]", "f"),
            // The one the old `list_has_all` route got wrong: a NULL element
            // is never contained, and the answer is false, not NULL.
            ("SELECT ARRAY[1,2,NULL] @> ARRAY[NULL]::int[]", "f"),
            ("SELECT ARRAY[1,NULL] && ARRAY[NULL,2]::int[]", "f"),
            // Set semantics, and an empty right operand contained by all.
            ("SELECT ARRAY[1,2,3] @> ARRAY[2,2,2]", "t"),
        ],
    )
    .await;
}

/// A `::type[]` cast belongs to its operand.
#[tokio::test]
async fn array_operators_handle_type_cast_operands() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_bools(
        &sess,
        &[
            // Was a parse error.
            ("SELECT ARRAY[]::int[] <@ ARRAY[1,2]", "t"),
            ("SELECT ARRAY[1,2]::int[] @> ARRAY[1]", "t"),
            ("SELECT (ARRAY['a','b'])::varchar(50)[] && ARRAY['b']", "t"),
            ("SELECT (ARRAY['a','b'])::varchar(50)[] && ARRAY['z']", "f"),
            // Was the list `[1]` / `[0]` rather than a boolean at all.
            ("SELECT ARRAY[1,2] @> ARRAY[]::int[]", "t"),
            ("SELECT ARRAY[1,2] && ARRAY[]::int[]", "f"),
            ("SELECT ARRAY[]::int[] @> ARRAY[]::int[]", "t"),
            ("SELECT ARRAY[]::int[] @> ARRAY[1]", "f"),
        ],
    )
    .await;
}

/// JSONB containment keeps its own route and its own answers.
#[tokio::test]
async fn jsonb_containment_is_unaffected_by_the_array_route() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_bools(
        &sess,
        &[
            (r#"SELECT '{"a":1}'::jsonb @> '{"a":1}'::jsonb"#, "t"),
            (r#"SELECT '{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb"#, "t"),
            (r#"SELECT '{"a":1}'::jsonb @> '{"a":1,"b":2}'::jsonb"#, "f"),
            (r#"SELECT '{"a":1}'::jsonb @> '{"b":1}'::jsonb"#, "f"),
            // Nested objects, and an array nested inside an object.
            (
                r#"SELECT '{"a":{"b":1,"c":2}}'::jsonb @> '{"a":{"b":1}}'::jsonb"#,
                "t",
            ),
            (
                r#"SELECT '{"a":[1,2,3]}'::jsonb @> '{"a":[1,2]}'::jsonb"#,
                "t",
            ),
            (r#"SELECT '{"a":[1,2,3]}'::jsonb @> '{"a":[]}'::jsonb"#, "t"),
            (r#"SELECT '{"a":1}'::jsonb @> '{}'::jsonb"#, "t"),
            // `<@` is the mirror.
            (r#"SELECT '{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb"#, "t"),
            (r#"SELECT '{"a":1,"b":2}'::jsonb <@ '{"a":1}'::jsonb"#, "f"),
        ],
    )
    .await;
}

// ── the stubs ────────────────────────────────────────────────────────────────

/// `isfinite` was hardcoded `true`, and Basin does materialise infinities.
#[tokio::test]
async fn isfinite_matches_postgres() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_bools(
        &sess,
        &[
            ("SELECT isfinite('infinity'::timestamp)", "f"),
            ("SELECT isfinite('-infinity'::timestamp)", "f"),
            ("SELECT isfinite('2020-01-01'::timestamp)", "t"),
            ("SELECT isfinite(NULL::timestamp)", "NULL"),
        ],
    )
    .await;
}

/// `array_lower` / `array_upper` read the `dim` argument they used to ignore.
///
/// The empty-array rows are the surprising ones: an empty array has zero
/// dimensions in PostgreSQL, so both bounds are NULL rather than `1` and `0`.
#[tokio::test]
async fn array_bounds_match_postgres() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_values(
        &sess,
        &[
            ("SELECT array_lower(ARRAY[1,2,3],1)", "1"),
            ("SELECT array_lower(ARRAY[1,2,3],2)", "NULL"),
            ("SELECT array_upper(ARRAY[1,2,3],1)", "3"),
            ("SELECT array_upper(ARRAY[1,2,3],2)", "NULL"),
            ("SELECT array_upper(ARRAY[1,2,3],0)", "NULL"),
            ("SELECT array_upper(ARRAY[1,2,3],-1)", "NULL"),
            ("SELECT array_upper(ARRAY[[1,2,3],[4,5,6]],1)", "2"),
            ("SELECT array_upper(ARRAY[[1,2,3],[4,5,6]],2)", "3"),
            ("SELECT array_upper(ARRAY[[1,2,3],[4,5,6]],3)", "NULL"),
            ("SELECT array_lower(ARRAY[[1,2,3],[4,5,6]],2)", "1"),
            ("SELECT array_upper(ARRAY[]::int[],1)", "NULL"),
            ("SELECT array_lower(ARRAY[]::int[],1)", "NULL"),
            ("SELECT array_upper(NULL::int[],1)", "NULL"),
            ("SELECT array_upper(ARRAY[1,2,3],NULL)", "NULL"),
        ],
    )
    .await;
}

/// `pg_size_pretty` was the constant `'0 bytes'`.
///
/// The threshold is 10 units, not 1 — `1048576` is `1024 kB`, not `1 MB` — and
/// the sign rides along on the magnitude test.
#[tokio::test]
async fn pg_size_pretty_matches_postgres() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_values(
        &sess,
        &[
            ("SELECT pg_size_pretty(0::bigint)", "0 bytes"),
            ("SELECT pg_size_pretty(1023::bigint)", "1023 bytes"),
            ("SELECT pg_size_pretty(10239::bigint)", "10239 bytes"),
            ("SELECT pg_size_pretty(10240::bigint)", "10 kB"),
            ("SELECT pg_size_pretty(1048576::bigint)", "1024 kB"),
            ("SELECT pg_size_pretty(10485760::bigint)", "10 MB"),
            ("SELECT pg_size_pretty(123456789::bigint)", "118 MB"),
            ("SELECT pg_size_pretty(1073741824::bigint)", "1024 MB"),
            ("SELECT pg_size_pretty((-1024)::bigint)", "-1024 bytes"),
            ("SELECT pg_size_pretty(1125899906842624::bigint)", "1024 TB"),
        ],
    )
    .await;
}

/// `pg_typeof` was the constant `'unknown'` — itself a real PG type name,
/// which is what made it quiet.
///
/// `pg_typeof(1)` is `bigint` here and `integer` on PostgreSQL: Basin plans an
/// unadorned integer literal as `Int64`, a decision made long before this
/// function runs. `1::int` and a real `integer` column both name themselves
/// correctly, which is what the second row pins.
#[tokio::test]
async fn pg_typeof_names_the_arrow_type() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    check_values(
        &sess,
        &[
            ("SELECT pg_typeof(1::BIGINT)", "bigint"),
            ("SELECT pg_typeof(1::int)", "integer"),
            ("SELECT pg_typeof('x'::TEXT)", "text"),
            ("SELECT pg_typeof(true)", "boolean"),
            ("SELECT pg_typeof(1.5::float8)", "double precision"),
            (
                "SELECT pg_typeof('2020-01-01'::timestamp)",
                "timestamp without time zone",
            ),
            // PG spells an array type with a single `[]` however deep it is.
            ("SELECT pg_typeof(ARRAY[1,2]::int[])", "integer[]"),
        ],
    )
    .await;
}

/// `make_interval` returned TEXT, built by flattening years to 365 days and
/// months to 30. It now returns an interval with PG's own three fields, so
/// `1 year` stays a year rather than becoming 365 days.
#[tokio::test]
async fn make_interval_returns_an_interval() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    // Rendered by Arrow rather than by PostgreSQL, so the *text* differs; what
    // is pinned is the month/day/nanosecond decomposition PG uses.
    //   PG: make_interval(hours => 1)    -> 01:00:00
    //   PG: make_interval(years => 1)    -> 1 year
    //   PG: make_interval(1,2,3,4,5,6,7) -> 1 year 2 mons 25 days 05:06:07
    check_values(
        &sess,
        &[
            ("SELECT make_interval(hours => 1)", "1 hours"),
            ("SELECT make_interval(years => 1)", "12 mons"),
            (
                "SELECT make_interval(1,2,3,4,5,6,7)",
                "14 mons 25 days 5 hours 6 mins 7.000000000 secs",
            ),
        ],
    )
    .await;
}

/// `power` returned IEEE specials where PostgreSQL raises.
#[tokio::test]
async fn power_raises_where_postgres_raises() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    // PG: ERROR: a negative number raised to a non-integer power yields a
    //     complex result   —   Basin used to answer NaN.
    let e = one_error(&sess, "SELECT power(-2, 0.5)").await;
    assert!(
        e.contains("a negative number raised to a non-integer power"),
        "got: {e}"
    );

    // PG: ERROR: zero raised to a negative power is undefined
    //     —   Basin used to answer inf.
    for sql in ["SELECT power(0,-1)", "SELECT power(0.0,-1)"] {
        let e = one_error(&sess, sql).await;
        assert!(e.contains("zero raised to a negative power"), "got: {e}");
    }

    // The shapes PostgreSQL does answer still answer.
    check_values(
        &sess,
        &[
            ("SELECT power(2,3)", "8.0"),
            ("SELECT power(-2,2)", "4.0"),
            ("SELECT power(2.0,0.5)", "1.4142135623730951"),
        ],
    )
    .await;
}

/// `date_bin` substituted 30-day months for a stride PostgreSQL rejects.
#[tokio::test]
async fn date_bin_refuses_month_and_year_strides() {
    let dir = TempDir::new().unwrap();
    let sess = session(&dir).await;

    // PG: ERROR: timestamps cannot be binned into intervals containing months
    //     or years. Basin answered `2020-03-01`, which looks like a calendar
    //     month boundary and is one only for this origin.
    for stride in ["1 month", "1 year"] {
        let sql = format!(
            "SELECT date_bin('{stride}'::interval, '2020-03-15'::timestamp, \
             '2020-01-01'::timestamp)"
        );
        let e = one_error(&sess, &sql).await;
        assert!(
            e.contains("cannot be binned into intervals containing months or years"),
            "{sql}: got {e}"
        );
    }

    // A fixed-width stride is unaffected.
    check_values(
        &sess,
        &[(
            "SELECT date_bin('15 minutes'::interval, '2020-03-15 10:22:00'::timestamp, \
             '2020-01-01'::timestamp)",
            "2020-03-15T10:15:00",
        )],
    )
    .await;
}
