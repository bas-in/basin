//! 3-way PG-compatibility differential — Basin vs Neon vs Supabase.
//!
//! # Purpose
//!
//! Anyone running the OSS test-suite can verify that Basin Cloud's pgwire
//! behaviour matches the two leading Postgres-as-a-service incumbents
//! (Neon, Supabase) across a corpus of ~60 representative SQL shapes:
//! basic SELECT/INSERT/UPDATE/DELETE across types (int, text, jsonb,
//! timestamp, uuid, numeric, arrays), window functions, CTEs (incl.
//! recursive), all JOIN flavours, correlated/EXISTS/IN subqueries,
//! aggregate FILTER, DDL (CREATE/ALTER/INDEX), basic RLS, and `psql`-style
//! `pg_catalog` probes.
//!
//! The test is **not pass/fail on divergence** — it emits a JSON report to
//! `benchmark/data/3way_pg_compat.json` with one row per shape and a
//! summary block. The TEST itself only fails on a panic or harness error.
//!
//! Skipping is the default: if any of the three DSNs are absent the test
//! prints a skip message and returns `Ok`. This keeps `cargo test` green
//! on every developer machine that has not opted into a 3-way run.
//!
//! # Running
//!
//! ```sh
//! # All three URLs must be reachable libpq endpoints. The test uses
//! # tokio-postgres directly so any sslmode=require/disable string works
//! # the same as it would for `psql`.
//! BASIN_3WAY_BASIN_URL='postgres://...' \
//! BASIN_3WAY_NEON_URL='postgres://...?sslmode=require' \
//! BASIN_3WAY_SUPABASE_URL='postgres://...?sslmode=require' \
//!   cargo test -p basin-integration-tests --test 3way_pg_compat -- --nocapture
//! ```
//!
//! Without the env vars (CI default):
//! ```sh
//! cargo test -p basin-integration-tests --test 3way_pg_compat -- --nocapture
//! # → "skip: set BASIN_3WAY_*_URL for 3-way compat run"
//! ```
//!
//! See `docs/operators/3way-pg-compat.md` for the operator runbook (how to
//! obtain endpoints, interpret the JSON output, and feed it into a
//! cross-run comparison).
//!
//! # Output format
//!
//! `benchmark/data/3way_pg_compat.json` is rewritten on every successful
//! 3-way run. Schema:
//!
//! ```json
//! {
//!   "run_id": "uuid",
//!   "timestamp": "RFC3339",
//!   "shapes": [
//!     {
//!       "name": "select_one_int",
//!       "category": "basic_select",
//!       "sql": "SELECT 1::int",
//!       "basin_ok": true, "neon_ok": true, "supabase_ok": true,
//!       "basin_value": "[[1]]",
//!       "neon_value":  "[[1]]",
//!       "supabase_value": "[[1]]",
//!       "basin_error": null, "neon_error": null, "supabase_error": null,
//!       "matches_neon": true,
//!       "matches_supabase": true,
//!       "all_three_match": true
//!     }
//!   ],
//!   "summary": {
//!     "total_shapes": N,
//!     "basin_ok": N, "neon_ok": N, "supabase_ok": N,
//!     "all_three_match": N,
//!     "basin_diverges_from_neon": M,
//!     "basin_diverges_from_supabase": M,
//!     "neon_diverges_from_supabase": M
//!   }
//! }
//! ```

#![allow(clippy::print_stdout)]

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Shape corpus
// ─────────────────────────────────────────────────────────────────────────────

/// One SQL shape to fire at all three endpoints.
///
/// `sql` is executed via the simple-query protocol so multi-statement
/// scripts (DDL + INSERT + SELECT) run in a single round-trip and the
/// captured "value" is the last SELECT's rows.
#[derive(Clone, Debug)]
struct Shape {
    name: &'static str,
    category: &'static str,
    sql: String,
}

impl Shape {
    fn new(name: &'static str, category: &'static str, sql: impl Into<String>) -> Self {
        Self {
            name,
            category,
            sql: sql.into(),
        }
    }
}

/// Per-shape SQL, namespaced under a unique schema so we don't collide
/// with anything else in the target databases. Each schema-prefixed shape
/// drops + creates inside its own statement chain so a re-run is idempotent.
fn build_corpus(ns: &str) -> Vec<Shape> {
    let mut s: Vec<Shape> = Vec::new();

    // ── basic SELECT / literals ──────────────────────────────────────────────
    s.push(Shape::new("select_one_int", "basic_select", "SELECT 1::int"));
    s.push(Shape::new(
        "select_text_literal",
        "basic_select",
        "SELECT 'hello'::text",
    ));
    s.push(Shape::new(
        "select_now_type",
        "basic_select",
        "SELECT pg_typeof(now())::text",
    ));
    s.push(Shape::new(
        "select_numeric",
        "basic_select",
        "SELECT 123.456::numeric",
    ));
    s.push(Shape::new(
        "select_bool_true",
        "basic_select",
        "SELECT TRUE",
    ));
    s.push(Shape::new(
        "select_null",
        "basic_select",
        "SELECT NULL::int",
    ));
    s.push(Shape::new(
        "select_array_int",
        "basic_select",
        "SELECT ARRAY[1,2,3]::int[]",
    ));
    s.push(Shape::new(
        "select_jsonb_literal",
        "basic_select",
        "SELECT '{\"a\":1,\"b\":[2,3]}'::jsonb",
    ));
    s.push(Shape::new(
        "select_uuid_cast",
        "basic_select",
        "SELECT '00000000-0000-0000-0000-000000000001'::uuid",
    ));
    s.push(Shape::new(
        "select_timestamp_cast",
        "basic_select",
        "SELECT '2026-01-01 00:00:00+00'::timestamptz",
    ));
    s.push(Shape::new(
        "select_coalesce",
        "basic_select",
        "SELECT COALESCE(NULL::int, 7)",
    ));
    s.push(Shape::new(
        "select_case",
        "basic_select",
        "SELECT CASE WHEN 1=1 THEN 'a' ELSE 'b' END",
    ));

    // ── DDL + DML round-trip per type ────────────────────────────────────────
    s.push(Shape::new(
        "ddl_int_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_int;
             CREATE TABLE {ns}.t_int (id INT PRIMARY KEY, v INT);
             INSERT INTO {ns}.t_int VALUES (1, 10), (2, 20), (3, 30);
             SELECT id, v FROM {ns}.t_int ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_text_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_text;
             CREATE TABLE {ns}.t_text (id INT PRIMARY KEY, v TEXT);
             INSERT INTO {ns}.t_text VALUES (1, 'a'), (2, 'b');
             SELECT id, v FROM {ns}.t_text ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_jsonb_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_jsonb;
             CREATE TABLE {ns}.t_jsonb (id INT PRIMARY KEY, v JSONB);
             INSERT INTO {ns}.t_jsonb VALUES (1, '{{\"k\":1}}'), (2, '[1,2,3]');
             SELECT id, v FROM {ns}.t_jsonb ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_timestamp_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_ts;
             CREATE TABLE {ns}.t_ts (id INT PRIMARY KEY, v TIMESTAMPTZ);
             INSERT INTO {ns}.t_ts VALUES (1, '2026-01-01 00:00:00+00'), (2, '2026-06-01 12:00:00+00');
             SELECT id, v FROM {ns}.t_ts ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_uuid_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_uuid;
             CREATE TABLE {ns}.t_uuid (id INT PRIMARY KEY, v UUID);
             INSERT INTO {ns}.t_uuid VALUES \
               (1, '00000000-0000-0000-0000-000000000001'), \
               (2, '00000000-0000-0000-0000-000000000002');
             SELECT id, v FROM {ns}.t_uuid ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_numeric_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_num;
             CREATE TABLE {ns}.t_num (id INT PRIMARY KEY, v NUMERIC(10,2));
             INSERT INTO {ns}.t_num VALUES (1, 1.50), (2, 99.99);
             SELECT id, v FROM {ns}.t_num ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_int_array_roundtrip",
        "dml_types",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_arr;
             CREATE TABLE {ns}.t_arr (id INT PRIMARY KEY, v INT[]);
             INSERT INTO {ns}.t_arr VALUES (1, ARRAY[1,2,3]), (2, ARRAY[4,5]);
             SELECT id, v FROM {ns}.t_arr ORDER BY id"
        ),
    ));

    // ── UPDATE / DELETE ─────────────────────────────────────────────────────
    s.push(Shape::new(
        "update_simple",
        "update_delete",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_upd;
             CREATE TABLE {ns}.t_upd (id INT PRIMARY KEY, v INT);
             INSERT INTO {ns}.t_upd VALUES (1, 1), (2, 2), (3, 3);
             UPDATE {ns}.t_upd SET v = v * 10 WHERE id >= 2;
             SELECT id, v FROM {ns}.t_upd ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "delete_simple",
        "update_delete",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_del;
             CREATE TABLE {ns}.t_del (id INT PRIMARY KEY, v INT);
             INSERT INTO {ns}.t_del VALUES (1, 1), (2, 2), (3, 3);
             DELETE FROM {ns}.t_del WHERE v = 2;
             SELECT id, v FROM {ns}.t_del ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "update_returning",
        "update_delete",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_ret;
             CREATE TABLE {ns}.t_ret (id INT PRIMARY KEY, v INT);
             INSERT INTO {ns}.t_ret VALUES (1, 1);
             UPDATE {ns}.t_ret SET v = 99 WHERE id = 1 RETURNING id, v"
        ),
    ));

    // ── Window functions ────────────────────────────────────────────────────
    let win_setup = format!(
        "DROP TABLE IF EXISTS {ns}.t_win;
         CREATE TABLE {ns}.t_win (g INT, v INT);
         INSERT INTO {ns}.t_win VALUES \
           (1, 10), (1, 20), (1, 30), (2, 100), (2, 200), (2, 300);"
    );
    s.push(Shape::new(
        "window_row_number",
        "window_fn",
        format!(
            "{win_setup}
             SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn
             FROM {ns}.t_win ORDER BY g, v"
        ),
    ));
    s.push(Shape::new(
        "window_rank",
        "window_fn",
        format!(
            "{win_setup}
             SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v DESC) AS rk
             FROM {ns}.t_win ORDER BY g, v"
        ),
    ));
    s.push(Shape::new(
        "window_lag",
        "window_fn",
        format!(
            "{win_setup}
             SELECT g, v, LAG(v) OVER (PARTITION BY g ORDER BY v) AS prev
             FROM {ns}.t_win ORDER BY g, v"
        ),
    ));
    s.push(Shape::new(
        "window_lead",
        "window_fn",
        format!(
            "{win_setup}
             SELECT g, v, LEAD(v) OVER (PARTITION BY g ORDER BY v) AS nxt
             FROM {ns}.t_win ORDER BY g, v"
        ),
    ));
    s.push(Shape::new(
        "window_sum_running",
        "window_fn",
        format!(
            "{win_setup}
             SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs
             FROM {ns}.t_win ORDER BY g, v"
        ),
    ));

    // ── CTEs (recursive + non-recursive) ────────────────────────────────────
    s.push(Shape::new(
        "cte_simple",
        "cte",
        "WITH a AS (SELECT 1 AS x UNION ALL SELECT 2) SELECT x FROM a ORDER BY x",
    ));
    s.push(Shape::new(
        "cte_recursive_count",
        "cte",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) \
         SELECT n FROM r ORDER BY n",
    ));
    s.push(Shape::new(
        "cte_multi",
        "cte",
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) \
         SELECT x, y FROM a, b",
    ));

    // ── JOINs ───────────────────────────────────────────────────────────────
    let join_setup = format!(
        "DROP TABLE IF EXISTS {ns}.j_l;
         DROP TABLE IF EXISTS {ns}.j_r;
         CREATE TABLE {ns}.j_l (id INT PRIMARY KEY, name TEXT);
         CREATE TABLE {ns}.j_r (id INT PRIMARY KEY, l_id INT, val TEXT);
         INSERT INTO {ns}.j_l VALUES (1, 'one'), (2, 'two'), (3, 'three');
         INSERT INTO {ns}.j_r VALUES (10, 1, 'a'), (11, 1, 'b'), (12, 2, 'c'), (13, 99, 'orphan');"
    );
    s.push(Shape::new(
        "join_inner",
        "join",
        format!(
            "{join_setup}
             SELECT l.name, r.val FROM {ns}.j_l l JOIN {ns}.j_r r ON r.l_id = l.id ORDER BY l.id, r.id"
        ),
    ));
    s.push(Shape::new(
        "join_left",
        "join",
        format!(
            "{join_setup}
             SELECT l.name, r.val FROM {ns}.j_l l LEFT JOIN {ns}.j_r r ON r.l_id = l.id ORDER BY l.id, r.id NULLS FIRST"
        ),
    ));
    s.push(Shape::new(
        "join_right",
        "join",
        format!(
            "{join_setup}
             SELECT l.name, r.val FROM {ns}.j_l l RIGHT JOIN {ns}.j_r r ON r.l_id = l.id ORDER BY r.id"
        ),
    ));
    s.push(Shape::new(
        "join_full",
        "join",
        format!(
            "{join_setup}
             SELECT l.name, r.val FROM {ns}.j_l l FULL JOIN {ns}.j_r r ON r.l_id = l.id \
             ORDER BY COALESCE(l.id, 1000), COALESCE(r.id, 1000)"
        ),
    ));
    s.push(Shape::new(
        "join_cross_count",
        "join",
        format!(
            "{join_setup}
             SELECT COUNT(*) FROM {ns}.j_l l CROSS JOIN {ns}.j_r r"
        ),
    ));

    // ── Subqueries: correlated / EXISTS / IN ────────────────────────────────
    s.push(Shape::new(
        "subq_exists",
        "subquery",
        format!(
            "{join_setup}
             SELECT name FROM {ns}.j_l l WHERE EXISTS \
               (SELECT 1 FROM {ns}.j_r r WHERE r.l_id = l.id) ORDER BY l.id"
        ),
    ));
    s.push(Shape::new(
        "subq_in",
        "subquery",
        format!(
            "{join_setup}
             SELECT name FROM {ns}.j_l l WHERE id IN \
               (SELECT l_id FROM {ns}.j_r) ORDER BY l.id"
        ),
    ));
    s.push(Shape::new(
        "subq_correlated_count",
        "subquery",
        format!(
            "{join_setup}
             SELECT name, (SELECT COUNT(*) FROM {ns}.j_r r WHERE r.l_id = l.id) AS c \
             FROM {ns}.j_l l ORDER BY l.id"
        ),
    ));
    s.push(Shape::new(
        "subq_not_in",
        "subquery",
        format!(
            "{join_setup}
             SELECT name FROM {ns}.j_l l WHERE id NOT IN \
               (SELECT l_id FROM {ns}.j_r WHERE l_id IS NOT NULL) ORDER BY l.id"
        ),
    ));

    // ── Aggregates (with FILTER) ────────────────────────────────────────────
    let agg_setup = format!(
        "DROP TABLE IF EXISTS {ns}.t_agg;
         CREATE TABLE {ns}.t_agg (g INT, v INT);
         INSERT INTO {ns}.t_agg VALUES (1, 1), (1, 2), (1, 3), (2, 10), (2, 20), (2, 30);"
    );
    s.push(Shape::new(
        "agg_sum_group",
        "aggregate",
        format!(
            "{agg_setup}
             SELECT g, SUM(v) AS s FROM {ns}.t_agg GROUP BY g ORDER BY g"
        ),
    ));
    s.push(Shape::new(
        "agg_count_distinct",
        "aggregate",
        format!(
            "{agg_setup}
             SELECT COUNT(DISTINCT v) FROM {ns}.t_agg"
        ),
    ));
    s.push(Shape::new(
        "agg_filter",
        "aggregate",
        format!(
            "{agg_setup}
             SELECT g, COUNT(*) FILTER (WHERE v > 5) AS bigs FROM {ns}.t_agg GROUP BY g ORDER BY g"
        ),
    ));
    s.push(Shape::new(
        "agg_having",
        "aggregate",
        format!(
            "{agg_setup}
             SELECT g, SUM(v) AS s FROM {ns}.t_agg GROUP BY g HAVING SUM(v) > 5 ORDER BY g"
        ),
    ));
    s.push(Shape::new(
        "agg_avg_numeric",
        "aggregate",
        format!(
            "{agg_setup}
             SELECT g, ROUND(AVG(v)::numeric, 2) AS a FROM {ns}.t_agg GROUP BY g ORDER BY g"
        ),
    ));

    // ── DDL coverage ────────────────────────────────────────────────────────
    s.push(Shape::new(
        "ddl_create_drop",
        "ddl",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_ddl;
             CREATE TABLE {ns}.t_ddl (id INT PRIMARY KEY);
             SELECT COUNT(*) FROM {ns}.t_ddl"
        ),
    ));
    s.push(Shape::new(
        "ddl_alter_add_column",
        "ddl",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_alt;
             CREATE TABLE {ns}.t_alt (id INT PRIMARY KEY);
             INSERT INTO {ns}.t_alt VALUES (1), (2);
             ALTER TABLE {ns}.t_alt ADD COLUMN extra TEXT DEFAULT 'x';
             SELECT id, extra FROM {ns}.t_alt ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_alter_drop_column",
        "ddl",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_drp;
             CREATE TABLE {ns}.t_drp (id INT, extra TEXT);
             INSERT INTO {ns}.t_drp VALUES (1, 'a'), (2, 'b');
             ALTER TABLE {ns}.t_drp DROP COLUMN extra;
             SELECT id FROM {ns}.t_drp ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "ddl_create_index",
        "ddl",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_idx;
             CREATE TABLE {ns}.t_idx (id INT PRIMARY KEY, v TEXT);
             CREATE INDEX t_idx_v ON {ns}.t_idx (v);
             INSERT INTO {ns}.t_idx VALUES (1, 'a'), (2, 'b');
             SELECT id FROM {ns}.t_idx WHERE v = 'a'"
        ),
    ));
    s.push(Shape::new(
        "ddl_truncate",
        "ddl",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_trc;
             CREATE TABLE {ns}.t_trc (id INT);
             INSERT INTO {ns}.t_trc VALUES (1), (2), (3);
             TRUNCATE {ns}.t_trc;
             SELECT COUNT(*) FROM {ns}.t_trc"
        ),
    ));

    // ── RLS basics ──────────────────────────────────────────────────────────
    //
    // RLS support varies across vendors — Basin may not implement it; Neon
    // and Supabase do. The point of these shapes is to record the divergence,
    // not to assert agreement. We never run as a non-superuser here, so the
    // policy is informational only — the SELECT after enabling RLS without
    // any policy will return zero rows on Neon/Supabase but may return all
    // rows on Basin (or error). All three behaviours are valid data points.
    s.push(Shape::new(
        "rls_enable_no_policy",
        "rls",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_rls;
             CREATE TABLE {ns}.t_rls (id INT, owner TEXT);
             INSERT INTO {ns}.t_rls VALUES (1, 'a'), (2, 'b');
             ALTER TABLE {ns}.t_rls ENABLE ROW LEVEL SECURITY;
             SELECT id FROM {ns}.t_rls ORDER BY id"
        ),
    ));
    s.push(Shape::new(
        "rls_create_policy",
        "rls",
        format!(
            "DROP TABLE IF EXISTS {ns}.t_rls2;
             CREATE TABLE {ns}.t_rls2 (id INT, owner TEXT);
             INSERT INTO {ns}.t_rls2 VALUES (1, 'a'), (2, 'b');
             ALTER TABLE {ns}.t_rls2 ENABLE ROW LEVEL SECURITY;
             CREATE POLICY p_all ON {ns}.t_rls2 FOR SELECT USING (true);
             SELECT id FROM {ns}.t_rls2 ORDER BY id"
        ),
    ));

    // ── pg_catalog probes (what `psql` sends) ───────────────────────────────
    s.push(Shape::new(
        "catalog_version",
        "pg_catalog",
        "SELECT version()",
    ));
    s.push(Shape::new(
        "catalog_current_schema",
        "pg_catalog",
        "SELECT current_schema()",
    ));
    s.push(Shape::new(
        "catalog_current_user",
        "pg_catalog",
        "SELECT current_user",
    ));
    s.push(Shape::new(
        "catalog_search_path",
        "pg_catalog",
        "SHOW search_path",
    ));
    s.push(Shape::new(
        "catalog_pg_class_count",
        "pg_catalog",
        // Sanity check on pg_catalog visibility — vendor-specific counts
        // are expected to differ but the query must succeed everywhere.
        "SELECT COUNT(*) > 0 FROM pg_class WHERE relkind = 'r'",
    ));
    s.push(Shape::new(
        "catalog_pg_namespace_public",
        "pg_catalog",
        "SELECT nspname FROM pg_namespace WHERE nspname = 'public'",
    ));
    s.push(Shape::new(
        "catalog_psql_d_shape",
        "pg_catalog",
        // The exact query `psql \d` sends (trimmed): list relations in
        // visible namespaces. Vendors return different row counts (informational).
        "SELECT n.nspname, c.relname, c.relkind \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','v','m','p') AND n.nspname NOT IN ('pg_catalog','information_schema') \
         ORDER BY n.nspname, c.relname LIMIT 5",
    ));
    s.push(Shape::new(
        "catalog_information_schema_tables",
        "pg_catalog",
        "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = 'public'",
    ));

    // ── String / math expressions ───────────────────────────────────────────
    s.push(Shape::new(
        "expr_string_concat",
        "expressions",
        "SELECT 'foo' || 'bar'",
    ));
    s.push(Shape::new(
        "expr_string_length",
        "expressions",
        "SELECT LENGTH('hello world')",
    ));
    s.push(Shape::new(
        "expr_substring",
        "expressions",
        "SELECT SUBSTRING('hello' FROM 2 FOR 3)",
    ));
    s.push(Shape::new(
        "expr_upper_lower",
        "expressions",
        "SELECT UPPER('abc'), LOWER('XYZ')",
    ));
    s.push(Shape::new(
        "expr_math",
        "expressions",
        "SELECT 2 + 3 * 4, 10 / 3, 10 % 3",
    ));
    s.push(Shape::new(
        "expr_jsonb_extract",
        "expressions",
        "SELECT '{\"a\":1,\"b\":{\"c\":2}}'::jsonb -> 'b' ->> 'c'",
    ));
    s.push(Shape::new(
        "expr_jsonb_path",
        "expressions",
        "SELECT '{\"x\":[1,2,3]}'::jsonb #> '{x,1}'",
    ));
    s.push(Shape::new(
        "expr_array_length",
        "expressions",
        "SELECT ARRAY_LENGTH(ARRAY[10,20,30], 1)",
    ));
    s.push(Shape::new(
        "expr_array_contains",
        "expressions",
        "SELECT ARRAY[1,2,3] @> ARRAY[2]",
    ));

    // ── Date/time arithmetic ────────────────────────────────────────────────
    s.push(Shape::new(
        "date_extract_year",
        "datetime",
        "SELECT EXTRACT(YEAR FROM TIMESTAMP '2026-05-21 10:00:00')",
    ));
    s.push(Shape::new(
        "date_interval_add",
        "datetime",
        "SELECT (TIMESTAMP '2026-01-01' + INTERVAL '1 day')::date",
    ));
    s.push(Shape::new(
        "date_age",
        "datetime",
        "SELECT AGE(TIMESTAMP '2026-12-31', TIMESTAMP '2026-01-01')",
    ));

    s
}

// ─────────────────────────────────────────────────────────────────────────────
// Probe — run one shape against one endpoint, capture rows or error string
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
struct ProbeResult {
    ok: bool,
    /// Rendered value as a JSON-encoded 2-D array of cell strings, e.g.
    /// `"[[\"1\",\"a\"],[\"2\",\"b\"]]"`. NULL cells render as the bare
    /// string `"NULL"`. Empty result-set renders as `"[]"`.
    value: String,
    error: Option<String>,
}

/// Render the SimpleQuery row stream from the LAST RowDescription burst as a
/// stable JSON-encoded 2-D string. This matches the test's comparison rule
/// (cells_equal at the string level) used by `differential_pg.rs`.
fn render_rows(msgs: &[SimpleQueryMessage]) -> String {
    // Collect the trailing run of rows (after any leading CommandComplete
    // bursts from the DDL/DML statements). We just take ALL rows in the
    // message list — pg only emits rows for SELECT/RETURNING anyway.
    let mut grid: Vec<Vec<String>> = Vec::new();
    for m in msgs {
        if let SimpleQueryMessage::Row(row) = m {
            let mut cells: Vec<String> = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                cells.push(match row.get(i) {
                    Some(s) => s.to_owned(),
                    None => "NULL".to_owned(),
                });
            }
            grid.push(cells);
        }
    }
    serde_json::to_string(&grid).unwrap_or_else(|_| "[]".to_owned())
}

async fn probe(client: &Client, sql: &str) -> ProbeResult {
    match client.simple_query(sql).await {
        Ok(msgs) => ProbeResult {
            ok: true,
            value: render_rows(&msgs),
            error: None,
        },
        Err(e) => ProbeResult {
            ok: false,
            value: "[]".to_owned(),
            error: Some(format!("{e}")),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Comparison
// ─────────────────────────────────────────────────────────────────────────────

/// Two probe results are considered "matching" when:
/// - Both succeeded AND their rendered values are byte-equal, OR
/// - Both failed (error strings need NOT match — different vendors emit
///   different messages for the same SQLSTATE; we only care about the
///   ok/err categorical agreement at this level).
///
/// JSONB normalisation could be folded in later; the current rule is
/// deliberately strict-text so the JSON output is comparable run-over-run.
fn matches(a: &ProbeResult, b: &ProbeResult) -> bool {
    if a.ok != b.ok {
        return false;
    }
    if a.ok {
        a.value == b.value
    } else {
        // Both errored — treat as match for this differential.
        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Output report
// ─────────────────────────────────────────────────────────────────────────────

fn output_path() -> PathBuf {
    // The test runs from the workspace root (cargo's default). The benchmark
    // data directory lives at <workspace>/benchmark/data/.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut p = PathBuf::from(manifest_dir);
    // tests/integration → ../../benchmark/data
    p.push("../../benchmark/data");
    p = p.canonicalize().unwrap_or(p);
    p.push("3way_pg_compat.json");
    p
}

fn write_report(report: &Value) -> std::io::Result<PathBuf> {
    let path = output_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_string_pretty(report).unwrap();
    std::fs::write(&path, body)?;
    Ok(path)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test entry point
// ─────────────────────────────────────────────────────────────────────────────

const BASIN_URL_ENV: &str = "BASIN_3WAY_BASIN_URL";
const NEON_URL_ENV: &str = "BASIN_3WAY_NEON_URL";
const SUPABASE_URL_ENV: &str = "BASIN_3WAY_SUPABASE_URL";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_pg_compat() {
    // ── env-gated skip ───────────────────────────────────────────────────────
    let basin_url = env::var(BASIN_URL_ENV).ok();
    let neon_url = env::var(NEON_URL_ENV).ok();
    let supabase_url = env::var(SUPABASE_URL_ENV).ok();

    let (basin_url, neon_url, supabase_url) =
        match (basin_url.as_deref(), neon_url.as_deref(), supabase_url.as_deref()) {
            (Some(b), Some(n), Some(s)) if !b.is_empty() && !n.is_empty() && !s.is_empty() => {
                (b.to_owned(), n.to_owned(), s.to_owned())
            }
            _ => {
                println!(
                    "skip: set BASIN_3WAY_*_URL for 3-way compat run \
                     (need {BASIN_URL_ENV}, {NEON_URL_ENV}, {SUPABASE_URL_ENV})"
                );
                return;
            }
        };

    println!("=== 3-way pg-compat differential ===");
    println!("  Basin    : (BASIN_3WAY_BASIN_URL set)");
    println!("  Neon     : (BASIN_3WAY_NEON_URL set)");
    println!("  Supabase : (BASIN_3WAY_SUPABASE_URL set)");

    // ── connect to all three (sequentially, with a timeout each) ────────────
    let basin = match connect(&basin_url, "basin").await {
        Ok(c) => c,
        Err(e) => {
            println!("error: failed to connect to Basin: {e} — aborting (test still PASS)");
            return;
        }
    };
    let neon = match connect(&neon_url, "neon").await {
        Ok(c) => c,
        Err(e) => {
            println!("error: failed to connect to Neon: {e} — aborting (test still PASS)");
            return;
        }
    };
    let supabase = match connect(&supabase_url, "supabase").await {
        Ok(c) => c,
        Err(e) => {
            println!("error: failed to connect to Supabase: {e} — aborting (test still PASS)");
            return;
        }
    };

    // ── isolate this run's tables under a unique schema in each db ──────────
    let run_id = Uuid::new_v4();
    // Schema names must be valid SQL idents — strip dashes.
    let ns = format!("basin_3way_{}", run_id.simple());
    println!("  schema   : {ns}");

    // Best-effort CREATE SCHEMA on each endpoint. We don't fail the test if
    // any of them errors — the per-shape DDL still uses the namespaced ident
    // and any failure shows up uniformly in the report.
    for (label, c) in [("basin", &basin), ("neon", &neon), ("supabase", &supabase)] {
        let stmt = format!("CREATE SCHEMA IF NOT EXISTS {ns}");
        if let Err(e) = c.simple_query(&stmt).await {
            println!("  warn: CREATE SCHEMA failed on {label}: {e}");
        }
    }

    // ── run the corpus ──────────────────────────────────────────────────────
    let corpus = build_corpus(&ns);
    println!("  shapes   : {} total", corpus.len());

    let mut shape_rows: Vec<Value> = Vec::with_capacity(corpus.len());
    let mut summary_basin_ok = 0usize;
    let mut summary_neon_ok = 0usize;
    let mut summary_supabase_ok = 0usize;
    let mut summary_all_three_match = 0usize;
    let mut basin_diverges_from_neon = 0usize;
    let mut basin_diverges_from_supabase = 0usize;
    let mut neon_diverges_from_supabase = 0usize;

    for shape in &corpus {
        let b = probe(&basin, &shape.sql).await;
        let n = probe(&neon, &shape.sql).await;
        let s = probe(&supabase, &shape.sql).await;

        if b.ok {
            summary_basin_ok += 1;
        }
        if n.ok {
            summary_neon_ok += 1;
        }
        if s.ok {
            summary_supabase_ok += 1;
        }

        let matches_neon = matches(&b, &n);
        let matches_supabase = matches(&b, &s);
        let neon_vs_supabase = matches(&n, &s);
        let all_three_match = matches_neon && matches_supabase;

        if !matches_neon {
            basin_diverges_from_neon += 1;
        }
        if !matches_supabase {
            basin_diverges_from_supabase += 1;
        }
        if !neon_vs_supabase {
            neon_diverges_from_supabase += 1;
        }
        if all_three_match {
            summary_all_three_match += 1;
        }

        // Console line — short for readability when running with --nocapture.
        let tag = if all_three_match {
            "MATCH"
        } else if matches_neon || matches_supabase {
            "PART "
        } else {
            "DIFF "
        };
        println!(
            "  [{tag}] {:32} basin={} neon={} supa={}",
            shape.name,
            if b.ok { "ok " } else { "err" },
            if n.ok { "ok " } else { "err" },
            if s.ok { "ok " } else { "err" },
        );

        shape_rows.push(json!({
            "name": shape.name,
            "category": shape.category,
            "sql": shape.sql,
            "basin_ok": b.ok,
            "neon_ok": n.ok,
            "supabase_ok": s.ok,
            "basin_value": b.value,
            "neon_value":  n.value,
            "supabase_value": s.value,
            "basin_error": b.error,
            "neon_error": n.error,
            "supabase_error": s.error,
            "matches_neon": matches_neon,
            "matches_supabase": matches_supabase,
            "neon_matches_supabase": neon_vs_supabase,
            "all_three_match": all_three_match,
        }));
    }

    // ── best-effort cleanup ─────────────────────────────────────────────────
    for (label, c) in [("basin", &basin), ("neon", &neon), ("supabase", &supabase)] {
        let stmt = format!("DROP SCHEMA IF EXISTS {ns} CASCADE");
        if let Err(e) = c.simple_query(&stmt).await {
            println!("  warn: DROP SCHEMA failed on {label}: {e}");
        }
    }

    // ── build report ────────────────────────────────────────────────────────
    let report = json!({
        "run_id": run_id.to_string(),
        "timestamp": Utc::now().to_rfc3339(),
        "namespace": ns,
        "shapes": shape_rows,
        "summary": {
            "total_shapes": corpus.len(),
            "basin_ok": summary_basin_ok,
            "neon_ok": summary_neon_ok,
            "supabase_ok": summary_supabase_ok,
            "all_three_match": summary_all_three_match,
            "basin_diverges_from_neon": basin_diverges_from_neon,
            "basin_diverges_from_supabase": basin_diverges_from_supabase,
            "neon_diverges_from_supabase": neon_diverges_from_supabase,
        }
    });

    match write_report(&report) {
        Ok(p) => println!("\nwrote JSON report → {}", p.display()),
        Err(e) => println!("\nerror writing JSON report: {e}"),
    }

    println!("\n=== summary ===");
    println!("  total           : {}", corpus.len());
    println!("  basin ok        : {summary_basin_ok}");
    println!("  neon ok         : {summary_neon_ok}");
    println!("  supabase ok     : {summary_supabase_ok}");
    println!("  all-three match : {summary_all_three_match}");
    println!("  basin ≠ neon    : {basin_diverges_from_neon}");
    println!("  basin ≠ supabase: {basin_diverges_from_supabase}");
    println!("  neon  ≠ supabase: {neon_diverges_from_supabase}");

    // The test does not assert "all match" — the JSON is the data.
}

/// Connect with a 15s timeout so a misconfigured DSN cannot hang CI.
async fn connect(url: &str, label: &str) -> Result<Client, String> {
    let fut = async {
        let (client, connection) =
            tokio_postgres::connect(url, NoTls).await.map_err(|e| e.to_string())?;
        // Spawn the connection driver. If it returns an error we just log.
        let driver_label = label.to_owned();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[3way] {driver_label} connection driver exited: {e}");
            }
        });
        Ok::<Client, String>(client)
    };
    tokio::time::timeout(Duration::from_secs(15), fut)
        .await
        .map_err(|_| format!("connect to {label} timed out after 15s"))?
}

// ─────────────────────────────────────────────────────────────────────────────
// Compile-time sanity: the corpus is non-empty and every shape carries SQL.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn corpus_is_non_empty() {
    let corpus = build_corpus("test_ns");
    assert!(!corpus.is_empty(), "corpus must not be empty");
    assert!(
        corpus.len() >= 50,
        "task brief requires ~50-100 shapes, got {}",
        corpus.len()
    );
    for sh in &corpus {
        assert!(!sh.sql.trim().is_empty(), "shape {} has empty SQL", sh.name);
        assert!(!sh.name.is_empty(), "shape with empty name");
        assert!(!sh.category.is_empty(), "shape {} missing category", sh.name);
    }
}
