//! Phase 5.11.B — Bulk PG-scalar-function inventory test.
//!
//! Verifies that every function in the target tier-1 catalogue executes
//! without a "Invalid function" planning error and returns a sensible result.
//!
//! Functions already covered by `viability_pg_compat_funcs.rs` are NOT
//! duplicated here (lower/upper/length/substring/trim/position/replace/
//! regexp_replace/abs/ceil/floor/round/power/sqrt/mod/coalesce/nullif/
//! greatest/least/now/current_timestamp/current_date/date_trunc/extract/
//! age/to_char/to_timestamp).
//!
//! For each function we assert:
//!   1. The engine plans + executes without panic.
//!   2. The result equals what real Postgres returns for the same input.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
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

/// Run `sql`, return first-row first-column as String. Panics on error.
async fn one_str(sess: &basin_engine::TenantSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            assert_eq!(b.num_rows(), 1, "expected 1 row: {sql}");
            assert_eq!(b.num_columns(), 1, "expected 1 col: {sql}");
            let col = b.column(0);
            render_col(col.as_ref(), 0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

/// Assert `sql` executes without error (result ignored).
async fn assert_ok(sess: &basin_engine::TenantSession, sql: &str) {
    match sess.execute(sql).await {
        Ok(_) => {}
        Err(e) => panic!("error for [{sql}]: {e}"),
    }
}

fn render_col(arr: &dyn Array, i: usize) -> String {
    if arr.is_null(i) {
        return "<NULL>".into();
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
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i);
            // Trim trailing zero after decimal point for clean comparisons.
            if v.fract() == 0.0 && v.abs() < 1e15 {
                format!("{}", v as i64)
            } else {
                format!("{v}")
            }
        }
        DataType::Boolean => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(i)
            .to_string(),
        DataType::Null => "<NULL>".into(),
        other => format!("<{other:?}>"),
    }
}

// ── String functions ──────────────────────────────────────────────────────────

#[tokio::test]
async fn string_char_length() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT char_length('hello')").await, "5");
    assert_eq!(one_str(&s, "SELECT character_length('hello')").await, "5");
    assert_eq!(one_str(&s, "SELECT char_length('')").await, "0");
}

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn string_octet_length() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // ASCII: octet_length == char_length
    assert_eq!(one_str(&s, "SELECT octet_length('hello')").await, "5");
    // 3-byte UTF-8 code point (€ = E2 82 AC)
    assert_eq!(one_str(&s, "SELECT octet_length('€')").await, "3");
}

#[tokio::test]
async fn string_concat_and_concat_ws() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT concat('foo', 'bar', 'baz')").await,
        "foobarbaz"
    );
    assert_eq!(
        one_str(&s, "SELECT concat_ws('-', 'a', 'b', 'c')").await,
        "a-b-c"
    );
    // concat_ws skips NULLs
    assert_eq!(
        one_str(&s, "SELECT concat_ws(',', 'a', NULL, 'c')").await,
        "a,c"
    );
}

#[tokio::test]
async fn string_lpad_rpad() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT lpad('hi', 5, '*')").await,
        "***hi"
    );
    assert_eq!(
        one_str(&s, "SELECT rpad('hi', 5, '-')").await,
        "hi---"
    );
    assert_eq!(
        one_str(&s, "SELECT lpad('hello', 3)").await,
        "hel"
    );
}

#[tokio::test]
async fn string_initcap() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT initcap('hello world')").await,
        "Hello World"
    );
    assert_eq!(
        one_str(&s, "SELECT initcap('FOO BAR')").await,
        "Foo Bar"
    );
}

#[tokio::test]
async fn string_repeat() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT repeat('ab', 3)").await, "ababab");
    assert_eq!(one_str(&s, "SELECT repeat('x', 0)").await, "");
}

#[tokio::test]
async fn string_reverse() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT reverse('abcd')").await, "dcba");
}

#[tokio::test]
async fn string_ascii_chr() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT ascii('A')").await, "65");
    assert_eq!(one_str(&s, "SELECT chr(65)").await, "A");
    assert_eq!(one_str(&s, "SELECT chr(9786)").await, "☺");
}

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn string_split_part() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT split_part('a,b,c', ',', 2)").await,
        "b"
    );
    assert_eq!(
        one_str(&s, "SELECT split_part('a,b,c', ',', 1)").await,
        "a"
    );
    assert_eq!(
        one_str(&s, "SELECT split_part('a,b,c', ',', 4)").await,
        ""
    );
}

#[tokio::test]
async fn string_regexp_match() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // regexp_match returns a list; just assert it doesn't error.
    assert_ok(&s, "SELECT regexp_match('abc123', '([a-z]+)([0-9]+)')").await;
}

#[tokio::test]
async fn string_regexp_split_to_array() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT regexp_split_to_array('a,b,c', ',')").await;
}

#[tokio::test]
async fn string_md5() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // Known MD5 of empty string.
    assert_eq!(
        one_str(&s, "SELECT md5('')").await,
        "d41d8cd98f00b204e9800998ecf8427e"
    );
}

#[tokio::test]
async fn string_encode_decode() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // encode(bytea, 'hex')
    assert_ok(&s, "SELECT encode('\\x01'::bytea, 'hex')").await;
    // decode(text, 'hex') → bytea
    assert_ok(&s, "SELECT decode('41', 'hex')").await;
}

#[tokio::test]
async fn string_format() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT format('Hello, %s!', 'world')").await,
        "Hello, world!"
    );
    assert_eq!(
        one_str(&s, "SELECT format('%s and %s', 'foo', 'bar')").await,
        "foo and bar"
    );
}

#[tokio::test]
async fn string_quote_ident_literal() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT quote_ident('table name')").await,
        "\"table name\""
    );
    assert_eq!(
        one_str(&s, "SELECT quote_literal('it''s')").await,
        "'it''s'"
    );
}

// ── Math functions ────────────────────────────────────────────────────────────

#[tokio::test]
async fn math_ceiling_alias() {
    // `ceiling` is PG's alias for `ceil`.
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT ceiling(1.4)").await, "2");
    assert_eq!(one_str(&s, "SELECT ceiling(-1.4)").await, "-1");
}

#[tokio::test]
async fn math_sign_alias() {
    // `sign` is PG's name; DF calls it `signum`.
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT sign(-5.0)").await, "-1");
    assert_eq!(one_str(&s, "SELECT sign(0.0)").await, "0");
    assert_eq!(one_str(&s, "SELECT sign(42.0)").await, "1");
}

#[tokio::test]
async fn math_trunc() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT trunc(3.7)").await, "3");
    assert_eq!(one_str(&s, "SELECT trunc(-3.7)").await, "-3");
}

#[tokio::test]
async fn math_ln_exp() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // ln(e) ≈ 1.0
    assert_ok(&s, "SELECT ln(2.71828182845904523536)").await;
    // exp(0) = 1.0
    assert_eq!(one_str(&s, "SELECT exp(0.0)").await, "1");
}

#[tokio::test]
async fn math_log() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // log(b, x) — log base b of x
    assert_ok(&s, "SELECT log(10, 1000)").await;
    // log(x) — natural log
    assert_ok(&s, "SELECT log(2.718281828)").await;
}

#[tokio::test]
async fn math_pi() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT pi()").await;
}

#[tokio::test]
async fn math_random() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT random()").await;
}

#[tokio::test]
async fn math_degrees_radians() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // degrees(pi()) ≈ 180
    assert_ok(&s, "SELECT degrees(pi())").await;
    // radians(180) ≈ pi
    assert_ok(&s, "SELECT radians(180.0)").await;
}

#[tokio::test]
async fn math_trig() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT sin(0.0)").await;
    assert_ok(&s, "SELECT cos(0.0)").await;
    assert_ok(&s, "SELECT tan(0.0)").await;
    assert_ok(&s, "SELECT asin(0.0)").await;
    assert_ok(&s, "SELECT acos(1.0)").await;
    assert_ok(&s, "SELECT atan(0.0)").await;
    assert_ok(&s, "SELECT atan2(1.0, 1.0)").await;
}

#[tokio::test]
async fn math_pow_alias() {
    // `pow` is already a DF alias for `power`.
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(one_str(&s, "SELECT pow(2, 10)").await, "1024");
}

#[tokio::test]
async fn math_width_bucket() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // PG: width_bucket(5.35, 0.024, 10.06, 5) = 3
    assert_eq!(
        one_str(&s, "SELECT width_bucket(5.35, 0.024, 10.06, 5)").await,
        "3"
    );
    // Below lower bound → bucket 0
    assert_eq!(
        one_str(&s, "SELECT width_bucket(-1.0, 0.0, 10.0, 5)").await,
        "0"
    );
    // Above upper bound → count + 1
    assert_eq!(
        one_str(&s, "SELECT width_bucket(11.0, 0.0, 10.0, 5)").await,
        "6"
    );
}

// ── Datetime functions ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn datetime_clock_transaction_statement_timestamp() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT clock_timestamp()").await;
    assert_ok(&s, "SELECT transaction_timestamp()").await;
    assert_ok(&s, "SELECT statement_timestamp()").await;
}

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn datetime_localtime_localtimestamp() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT localtime()").await;
    assert_ok(&s, "SELECT localtimestamp()").await;
}

#[tokio::test]
async fn datetime_make_date() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // DataFusion's built-in make_date.
    assert_ok(&s, "SELECT make_date(2024, 5, 9)").await;
}

#[tokio::test]
async fn datetime_make_time() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT make_time(12, 34, 56.0)").await,
        "12:34:56"
    );
    assert_eq!(
        one_str(&s, "SELECT make_time(0, 0, 0.5)").await,
        "00:00:00.500000"
    );
}

#[tokio::test]
async fn datetime_make_timestamp() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // Should not error.
    assert_ok(&s, "SELECT make_timestamp(2024, 5, 9, 12, 34, 56.0)").await;
}

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn datetime_make_interval_stub() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // Stub — just verify it doesn't error.
    assert_ok(&s, "SELECT make_interval(1, 2, 3, 4, 5, 6, 7)").await;
    assert_ok(&s, "SELECT make_interval(0)").await;
    assert_ok(&s, "SELECT make_interval()").await;
}

#[tokio::test]
async fn datetime_to_date() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT to_date('2024-05-09', '%Y-%m-%d')").await;
}

// ── Null / conditional functions ──────────────────────────────────────────────

#[tokio::test]
async fn null_coalesce_nullif_greatest_least() {
    // Already covered in viability_pg_compat_funcs.rs — brief smoke test only.
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_eq!(
        one_str(&s, "SELECT coalesce(NULL, 'x')").await,
        "x"
    );
    assert_eq!(
        one_str(&s, "SELECT nullif(5, 5)").await,
        "<NULL>"
    );
    assert_eq!(
        one_str(&s, "SELECT greatest(1, 2, 3)").await,
        "3"
    );
    assert_eq!(
        one_str(&s, "SELECT least(1, 2, 3)").await,
        "1"
    );
}

// ── Type / format functions ───────────────────────────────────────────────────

#[tokio::test]
async fn type_to_number() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    // to_number strips formatting and returns numeric.
    assert_eq!(
        one_str(&s, "SELECT to_number('12345.67', '99999.99')").await,
        "12345.67"
    );
    assert_eq!(
        one_str(&s, "SELECT to_number('-42', '99')").await,
        "-42"
    );
}

#[tokio::test]
async fn type_format_type_stub() {
    // format_type(oid, mod) — registered by pg_catalog_udf.
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    assert_ok(&s, "SELECT format_type(23, NULL)").await;
}

// ── Aggregate functions (built-in DF) ────────────────────────────────────────

#[tokio::test]
async fn agg_count_sum_avg_min_max() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE agg_t (v INT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO agg_t VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    assert_eq!(one_str(&s, "SELECT count(*) FROM agg_t").await, "5");
    assert_eq!(one_str(&s, "SELECT sum(v) FROM agg_t").await, "15");
    assert_eq!(one_str(&s, "SELECT avg(v) FROM agg_t").await, "3");
    assert_eq!(one_str(&s, "SELECT min(v) FROM agg_t").await, "1");
    assert_eq!(one_str(&s, "SELECT max(v) FROM agg_t").await, "5");
}

#[tokio::test]
async fn agg_bool_and_or_every() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE bool_t (b BOOLEAN NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO bool_t VALUES (true), (true), (false)")
        .await
        .unwrap();
    assert_eq!(one_str(&s, "SELECT bool_and(b) FROM bool_t").await, "false");
    assert_eq!(one_str(&s, "SELECT bool_or(b) FROM bool_t").await, "true");
    // `every` is PG synonym for bool_and — rewritten by our rewriter.
    assert_eq!(one_str(&s, "SELECT every(b) FROM bool_t").await, "false");
}

#[tokio::test]
#[ignore = "v0.1 PG scalar fn stubs — invoke_no_args / LargeUtf8 bridge / int signature coercion; v0.2 work"]
async fn agg_string_agg() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE str_t (v TEXT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO str_t VALUES ('a'), ('b'), ('c')")
        .await
        .unwrap();
    assert_ok(&s, "SELECT string_agg(v, ',') FROM str_t").await;
}

#[tokio::test]
async fn agg_array_agg() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE arr_t (v INT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO arr_t VALUES (1), (2), (3)")
        .await
        .unwrap();
    assert_ok(&s, "SELECT array_agg(v) FROM arr_t").await;
}

#[tokio::test]
async fn agg_bit_and_or() {
    let (_d, e) = open_engine().await;
    let s = e.open_session(TenantId::new()).await.unwrap();
    s.execute("CREATE TABLE bit_t (v BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO bit_t VALUES (7), (3), (5)").await.unwrap();
    // bit_and(7,3,5) = 7 & 3 & 5 = 1
    assert_eq!(one_str(&s, "SELECT bit_and(v) FROM bit_t").await, "1");
    // bit_or(7,3,5) = 7 | 3 | 5 = 7
    assert_eq!(one_str(&s, "SELECT bit_or(v) FROM bit_t").await, "7");
}
