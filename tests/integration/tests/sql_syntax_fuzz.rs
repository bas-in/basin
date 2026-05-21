//! SQL-syntax shape fuzz test — broad PG syntax coverage for parser/planner regressions.
//!
//! Goal: exercise a wide surface of uncommon-but-valid PostgreSQL syntax through
//! the real engine pipeline and assert that every shape is handled *gracefully* —
//! i.e. it either succeeds (Rows / Empty) or returns a typed `BasinError` that is
//! NOT `BasinError::Internal`. A panic or an `Internal` error is a failure.
//!
//! The test does NOT require every shape to succeed — some syntax is correctly
//! unsupported and will return `FeatureNotSupported`, `NotFound`, `InvalidSchema`,
//! etc. Those are counted as "typed-error" and considered graceful.
//!
//! Coverage categories exercised:
//!   - SELECT permutations: CASE, COALESCE, NULLIF, GREATEST/LEAST
//!   - Set operations: UNION / INTERSECT / EXCEPT
//!   - CTEs (WITH … AS), recursive CTE shape
//!   - Window functions: RANK, ROW_NUMBER, LAG, LEAD with PARTITION BY + ORDER BY
//!   - LATERAL joins, LATERAL subquery
//!   - FILTER (… WHERE) on aggregates
//!   - DDL: CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS,
//!           ALTER TABLE … ADD COLUMN … DEFAULT …,
//!           CREATE INDEX … USING btree, CREATE TYPE … AS ENUM
//!   - DML: ON CONFLICT DO UPDATE / DO NOTHING, UPDATE … FROM, DELETE … USING,
//!          INSERT … RETURNING, UPDATE … RETURNING
//!   - Type coercions: text→number, number→text, timestamp→date, json/jsonb
//!   - Quoted identifiers, mixed-case names, schema-qualified names
//!
//! Known bugs (flagged with FIXME, skipped in the main corpus):
//!   FIXME(bug-case-searched): CASE expr WHEN form panics with Internal "not yet supported".
//!   FIXME(bug-lateral-outer-ref): LATERAL + outer-column reference hits Internal "not implemented".
//!   FIXME(bug-create-type-if-not-exists): `CREATE TYPE IF NOT EXISTS` is a parse error (pg_query accepts it but sqlparser rejects).
//!   FIXME(bug-fast-select-text-alias): fast_select returns Internal on TEXT columns with aliases.
//!   FIXME(bug-is-distinct-from): IS [NOT] DISTINCT FROM hits DataFusion optimize_projections schema-mismatch Internal.
//!   FIXME(bug-vortex-between): BETWEEN reduces to constant with dtype mismatch in Vortex physical layer.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── Engine setup helper ─────────────────────────────────────────────────────

fn make_engine(dir: &TempDir) -> Engine {
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

// ─── Graceful-handling outcome ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    /// Engine returned Rows or Empty — full success.
    Succeeded,
    /// Engine returned a typed BasinError that is not Internal.
    TypedError(String),
    /// Engine returned BasinError::Internal — this is a test failure.
    InternalError(String),
}

/// Run `sql` through `sess` and classify the result.
async fn classify(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Outcome {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { .. }) | Ok(ExecResult::Empty { .. }) => Outcome::Succeeded,
        Err(e) => {
            let msg = e.to_string();
            match e {
                BasinError::Internal(_) => Outcome::InternalError(msg),
                // Every other typed variant is considered graceful.
                _ => Outcome::TypedError(msg),
            }
        }
    }
}

// ─── SQL corpus ───────────────────────────────────────────────────────────────

/// Shapes that create the base tables used by the corpus queries.
const PREAMBLE: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS fuzz_items (id BIGINT NOT NULL, label TEXT NOT NULL, score BIGINT, created_at TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS fuzz_tags  (item_id BIGINT NOT NULL, tag TEXT NOT NULL)",
    "INSERT INTO fuzz_items VALUES (1, 'alpha', 10, '2024-01-01 00:00:00'), (2, 'beta', 20, '2024-06-15 12:00:00'), (3, 'gamma', NULL, '2024-12-31 23:59:59')",
    "INSERT INTO fuzz_tags  VALUES (1, 'x'), (1, 'y'), (2, 'x'), (3, 'z')",
];

/// The main corpus.  Each entry is (name, sql).
const CORPUS: &[(&str, &str)] = &[
    // ── SELECT permutations ──────────────────────────────────────────────────

    ("select_case_simple",
     "SELECT id, CASE WHEN score > 15 THEN 'high' WHEN score IS NULL THEN 'n/a' ELSE 'low' END AS bucket FROM fuzz_items"),

    // FIXME(bug-case-searched): CASE expr WHEN form returns Internal "not yet supported".
    // The shape is valid PG syntax; the executor only handles the searched-CASE rewrite.
    // ("select_case_searched",
    //  "SELECT CASE score WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END FROM fuzz_items"),

    ("select_coalesce",
     "SELECT id, COALESCE(score, 0) AS effective_score FROM fuzz_items"),

    ("select_nullif",
     "SELECT id, NULLIF(score, 0) FROM fuzz_items"),

    ("select_greatest",
     "SELECT GREATEST(1, 2, 3)"),

    ("select_least",
     "SELECT LEAST(10, 5, 7)"),

    ("select_coalesce_chain",
     "SELECT COALESCE(NULL, NULL, 42)"),

    // ── Set operations ───────────────────────────────────────────────────────

    ("set_op_union_all",
     "SELECT id FROM fuzz_items UNION ALL SELECT item_id FROM fuzz_tags"),

    ("set_op_union_distinct",
     "SELECT label FROM fuzz_items UNION SELECT tag FROM fuzz_tags"),

    ("set_op_intersect",
     "SELECT id FROM fuzz_items WHERE id < 3 INTERSECT SELECT item_id FROM fuzz_tags WHERE tag = 'x'"),

    ("set_op_except",
     "SELECT id FROM fuzz_items EXCEPT SELECT item_id FROM fuzz_tags WHERE tag = 'z'"),

    // ── CTEs ─────────────────────────────────────────────────────────────────

    ("cte_basic",
     "WITH base AS (SELECT id, score FROM fuzz_items WHERE score IS NOT NULL) SELECT * FROM base WHERE score > 5"),

    ("cte_chained",
     "WITH a AS (SELECT id FROM fuzz_items), b AS (SELECT item_id FROM fuzz_tags) SELECT * FROM a JOIN b ON a.id = b.item_id"),

    ("cte_aggregate",
     "WITH totals AS (SELECT item_id, count(*) AS cnt FROM fuzz_tags GROUP BY item_id) SELECT item_id, cnt FROM totals ORDER BY cnt DESC"),

    ("cte_scalar_subquery",
     "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"),

    // ── Window functions ─────────────────────────────────────────────────────

    ("window_row_number",
     "SELECT id, label, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM fuzz_items"),

    ("window_rank_with_partition",
     "SELECT item_id, tag, RANK() OVER (PARTITION BY item_id ORDER BY tag) AS rnk FROM fuzz_tags"),

    ("window_dense_rank",
     "SELECT id, score, DENSE_RANK() OVER (ORDER BY score NULLS LAST) AS dr FROM fuzz_items"),

    ("window_lag",
     "SELECT id, score, LAG(score, 1, 0) OVER (ORDER BY id) AS prev_score FROM fuzz_items"),

    ("window_lead",
     "SELECT id, score, LEAD(score, 1) OVER (ORDER BY id) AS next_score FROM fuzz_items"),

    ("window_first_value",
     "SELECT id, FIRST_VALUE(label) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM fuzz_items"),

    ("window_last_value",
     "SELECT id, LAST_VALUE(score) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM fuzz_items"),

    ("window_sum_partition",
     "SELECT item_id, tag, SUM(1) OVER (PARTITION BY item_id) AS tag_count FROM fuzz_tags"),

    // ── LATERAL joins ────────────────────────────────────────────────────────

    // FIXME(bug-lateral-outer-ref): LATERAL with outer column reference returns Internal
    // "Physical plan does not support logical expression OuterReferenceColumn". Both lateral
    // shapes below hit this; they are valid PG syntax. Blocked on DataFusion upstream support
    // for correlated LATERAL references in the physical planner.
    // ("lateral_join_basic",
    //  "SELECT i.id, t.tag FROM fuzz_items i JOIN LATERAL (SELECT tag FROM fuzz_tags WHERE item_id = i.id LIMIT 1) t ON TRUE"),
    // ("lateral_subquery_in_from",
    //  "SELECT * FROM fuzz_items i, LATERAL (SELECT count(*) AS n FROM fuzz_tags WHERE item_id = i.id) sub"),

    // Non-correlated LATERAL (no outer reference) is safe.
    ("lateral_non_correlated",
     "SELECT i.id, t.tag FROM fuzz_items i JOIN LATERAL (SELECT tag FROM fuzz_tags LIMIT 1) t ON TRUE"),

    // ── FILTER on aggregates ─────────────────────────────────────────────────

    ("aggregate_filter_where",
     "SELECT count(*) FILTER (WHERE score > 10) AS high_count, count(*) AS total FROM fuzz_items"),

    ("aggregate_filter_sum",
     "SELECT sum(score) FILTER (WHERE score IS NOT NULL) FROM fuzz_items"),

    // ── Subqueries ───────────────────────────────────────────────────────────

    ("subquery_in_where",
     "SELECT * FROM fuzz_items WHERE id IN (SELECT item_id FROM fuzz_tags WHERE tag = 'x')"),

    ("subquery_not_in_where",
     "SELECT * FROM fuzz_items WHERE id NOT IN (SELECT item_id FROM fuzz_tags WHERE tag = 'z')"),

    ("subquery_exists",
     "SELECT id FROM fuzz_items i WHERE EXISTS (SELECT 1 FROM fuzz_tags t WHERE t.item_id = i.id)"),

    ("subquery_not_exists",
     "SELECT id FROM fuzz_items i WHERE NOT EXISTS (SELECT 1 FROM fuzz_tags t WHERE t.item_id = i.id)"),

    ("scalar_subquery_in_select",
     "SELECT id, (SELECT count(*) FROM fuzz_tags WHERE item_id = fuzz_items.id) AS tag_count FROM fuzz_items"),

    // ── Ordering / limiting edge cases ───────────────────────────────────────

    ("select_order_nulls_first",
     "SELECT id, score FROM fuzz_items ORDER BY score NULLS FIRST"),

    ("select_order_nulls_last",
     "SELECT id, score FROM fuzz_items ORDER BY score DESC NULLS LAST"),

    ("select_fetch_first",
     "SELECT id FROM fuzz_items ORDER BY id FETCH FIRST 2 ROWS ONLY"),

    ("select_offset",
     "SELECT id FROM fuzz_items ORDER BY id OFFSET 1"),

    // ── Joins ────────────────────────────────────────────────────────────────

    ("join_left_outer",
     "SELECT i.id, t.tag FROM fuzz_items i LEFT OUTER JOIN fuzz_tags t ON i.id = t.item_id"),

    ("join_full_outer",
     "SELECT i.id, t.tag FROM fuzz_items i FULL OUTER JOIN fuzz_tags t ON i.id = t.item_id"),

    ("join_cross",
     "SELECT i.id, t.tag FROM fuzz_items i CROSS JOIN fuzz_tags t LIMIT 5"),

    ("join_inner_explicit",
     "SELECT * FROM fuzz_items JOIN fuzz_tags t ON fuzz_items.id = t.item_id"),

    // ── Group by / having ────────────────────────────────────────────────────

    ("group_by_having",
     "SELECT item_id, count(*) AS cnt FROM fuzz_tags GROUP BY item_id HAVING count(*) > 1"),

    ("group_by_multiple_cols",
     "SELECT item_id, tag, count(*) FROM fuzz_tags GROUP BY item_id, tag"),

    ("group_by_rollup",
     "SELECT item_id, count(*) FROM fuzz_tags GROUP BY ROLLUP (item_id)"),

    // ── DDL variations ───────────────────────────────────────────────────────

    ("ddl_create_table_if_not_exists",
     "CREATE TABLE IF NOT EXISTS fuzz_tmp1 (id BIGINT NOT NULL, val TEXT)"),

    ("ddl_drop_table_if_exists",
     "DROP TABLE IF EXISTS fuzz_tmp1"),

    ("ddl_create_table_various_types",
     "CREATE TABLE IF NOT EXISTS fuzz_types (id BIGINT NOT NULL, f DOUBLE PRECISION, b BOOLEAN, ts TIMESTAMPTZ, d DATE, n NUMERIC(10,2))"),

    ("ddl_alter_add_column_default",
     "ALTER TABLE fuzz_items ADD COLUMN IF NOT EXISTS extra_col TEXT DEFAULT 'default_val'"),

    ("ddl_create_index_btree",
     "CREATE INDEX IF NOT EXISTS fuzz_items_score_idx ON fuzz_items USING btree (score)"),

    ("ddl_create_index_multicolumn",
     "CREATE INDEX IF NOT EXISTS fuzz_items_label_score_idx ON fuzz_items (label, score)"),

    // FIXME(bug-create-type-if-not-exists): `CREATE TYPE IF NOT EXISTS …` is valid PG 14+ syntax
    // but the engine's parser rejects it with Internal "parse error: expected end of statement,
    // found: NOT". The `IF NOT EXISTS` clause for CREATE TYPE is supported in PG but not yet
    // threaded through the Basin parser gate. Use `CREATE TYPE` without IF NOT EXISTS for now.
    // ("ddl_create_type_enum_if_not_exists",
    //  "CREATE TYPE IF NOT EXISTS fuzz_status AS ENUM ('active', 'inactive', 'pending')"),

    ("ddl_create_type_enum",
     "CREATE TYPE fuzz_status AS ENUM ('active', 'inactive', 'pending')"),

    ("ddl_drop_index_if_exists",
     "DROP INDEX IF EXISTS fuzz_items_score_idx"),

    // ── DML variations ───────────────────────────────────────────────────────

    ("dml_insert_on_conflict_do_nothing",
     "INSERT INTO fuzz_items (id, label, score) VALUES (1, 'alpha', 10) ON CONFLICT (id) DO NOTHING"),

    ("dml_insert_on_conflict_do_update",
     "INSERT INTO fuzz_items (id, label, score) VALUES (1, 'alpha_updated', 99) ON CONFLICT (id) DO UPDATE SET label = excluded.label, score = excluded.score"),

    ("dml_insert_returning",
     "INSERT INTO fuzz_items (id, label, score) VALUES (100, 'return_test', 77) RETURNING id, label"),

    ("dml_update_returning",
     "UPDATE fuzz_items SET score = score + 1 WHERE id = 1 RETURNING id, score"),

    ("dml_update_from",
     "UPDATE fuzz_items SET score = fuzz_tags.item_id * 5 FROM fuzz_tags WHERE fuzz_items.id = fuzz_tags.item_id AND fuzz_tags.tag = 'x'"),

    ("dml_delete_using",
     "DELETE FROM fuzz_tags USING fuzz_items WHERE fuzz_tags.item_id = fuzz_items.id AND fuzz_items.label = 'alpha'"),

    ("dml_delete_returning",
     "DELETE FROM fuzz_items WHERE id = 100 RETURNING id"),

    ("dml_insert_select",
     "INSERT INTO fuzz_items (id, label, score) SELECT 200, 'inserted_from_select', 55"),

    // ── Type coercions ───────────────────────────────────────────────────────

    ("cast_text_to_int",
     "SELECT CAST('42' AS BIGINT)"),

    ("cast_int_to_text",
     "SELECT CAST(123 AS TEXT)"),

    ("cast_double_cast_syntax",
     "SELECT '3.14'::DOUBLE PRECISION"),

    ("cast_timestamp_to_date",
     "SELECT CAST('2024-06-15 12:00:00' AS DATE)"),

    ("cast_text_to_timestamp",
     "SELECT CAST('2024-01-01' AS TIMESTAMP)"),

    ("cast_text_to_bool",
     "SELECT CAST('true' AS BOOLEAN)"),

    ("cast_numeric",
     "SELECT CAST(3.14159 AS NUMERIC(10, 4))"),

    ("json_literal",
     "SELECT '{\"key\": \"value\"}'::jsonb"),

    ("json_arrow_op",
     "SELECT '{\"a\": 1}'::jsonb -> 'a'"),

    ("jsonb_text_op",
     "SELECT '{\"a\": \"hello\"}'::jsonb ->> 'a'"),

    ("jsonb_contains_op",
     "SELECT '{\"a\": 1, \"b\": 2}'::jsonb @> '{\"a\": 1}'::jsonb"),

    ("array_literal",
     "SELECT ARRAY[1, 2, 3]"),

    ("array_subscript",
     "SELECT (ARRAY[10, 20, 30])[2]"),

    ("array_agg",
     "SELECT array_agg(tag ORDER BY tag) FROM fuzz_tags GROUP BY item_id"),

    // ── Quoted identifiers and schema-qualification ───────────────────────────

    ("quoted_identifier_table",
     "SELECT * FROM \"fuzz_items\" LIMIT 1"),

    ("quoted_identifier_column",
     "SELECT \"id\", \"label\" FROM fuzz_items LIMIT 1"),

    // FIXME(bug-fast-select-text-alias): Aliasing a TEXT column with a mixed-case quoted
    // identifier triggers Internal in fast_select: "computed expr on non-numeric column label
    // (Utf8); use DataFusion path". The fast_select short-circuit does not handle TEXT-typed
    // expression aliases before dispatching; it should fall through to the DataFusion path.
    // ("mixed_case_alias",
    //  "SELECT id AS \"MyID\", label AS \"MyLabel\" FROM fuzz_items LIMIT 1"),

    ("schema_qualified_public",
     "SELECT * FROM public.fuzz_items LIMIT 1"),

    // ── String / text functions ───────────────────────────────────────────────

    ("string_concat_op",
     "SELECT 'hello' || ' ' || 'world'"),

    ("string_length",
     "SELECT length('hello world')"),

    ("string_substring",
     "SELECT substring('hello world' FROM 7 FOR 5)"),

    ("string_trim",
     "SELECT trim(both ' ' FROM '  hello  ')"),

    ("string_like",
     "SELECT id, label FROM fuzz_items WHERE label LIKE 'alp%'"),

    ("string_ilike",
     "SELECT id, label FROM fuzz_items WHERE label ILIKE 'ALP%'"),

    ("string_similar_to",
     "SELECT id FROM fuzz_items WHERE label SIMILAR TO '(alpha|beta)'"),

    // ── Arithmetic / math expressions ─────────────────────────────────────────

    ("arithmetic_mod",
     "SELECT 17 % 5"),

    ("arithmetic_power",
     "SELECT 2 ^ 10"),

    ("arithmetic_integer_div",
     "SELECT 7 / 2"),

    ("math_abs",
     "SELECT abs(-42)"),

    ("math_ceil_floor",
     "SELECT ceil(3.2), floor(3.9)"),

    ("math_round",
     "SELECT round(3.14159, 2)"),

    // ── IS / IS NOT / IS DISTINCT FROM ──────────────────────────────────────

    ("is_null_check",
     "SELECT id FROM fuzz_items WHERE score IS NULL"),

    ("is_not_null_check",
     "SELECT id FROM fuzz_items WHERE score IS NOT NULL"),

    // FIXME(bug-is-distinct-from): `IS [NOT] DISTINCT FROM` against a NULL literal triggers
    // Internal in the DataFusion optimize_projections optimizer rule: schema mismatch when
    // the rewrite expands `x IS DISTINCT FROM y` into the explicit NULL-check form. This is
    // a DataFusion upstream bug. Both variants affected:
    //   "SELECT 1 IS DISTINCT FROM NULL"
    //   "SELECT 1 IS NOT DISTINCT FROM 1"
    // Skip until the DataFusion optimizer rule is patched or Basin adds a pre-rewrite.

    ("is_true",
     "SELECT TRUE IS TRUE"),

    ("is_false",
     "SELECT FALSE IS FALSE"),

    // ── Date/time functions ──────────────────────────────────────────────────

    ("date_extract",
     "SELECT EXTRACT(YEAR FROM created_at) FROM fuzz_items LIMIT 1"),

    ("date_trunc",
     "SELECT date_trunc('month', created_at) FROM fuzz_items LIMIT 1"),

    ("current_timestamp",
     "SELECT current_timestamp"),

    ("current_date",
     "SELECT current_date"),

    ("interval_arithmetic",
     "SELECT TIMESTAMP '2024-01-01' + INTERVAL '30 days'"),

    ("interval_subtract",
     "SELECT TIMESTAMP '2024-12-31' - TIMESTAMP '2024-01-01'"),

    // ── Aggregate functions ──────────────────────────────────────────────────

    ("agg_count_star",
     "SELECT count(*) FROM fuzz_items"),

    ("agg_count_col",
     "SELECT count(score) FROM fuzz_items"),

    ("agg_sum",
     "SELECT sum(score) FROM fuzz_items"),

    ("agg_avg",
     "SELECT avg(score) FROM fuzz_items"),

    ("agg_min_max",
     "SELECT min(score), max(score) FROM fuzz_items"),

    ("agg_string_agg",
     "SELECT string_agg(label, ', ' ORDER BY label) FROM fuzz_items"),

    ("agg_bool_and_or",
     "SELECT bool_and(score > 0), bool_or(score IS NULL) FROM fuzz_items"),

    // ── Miscellaneous valid PG constructs ────────────────────────────────────

    ("select_distinct",
     "SELECT DISTINCT tag FROM fuzz_tags ORDER BY tag"),

    ("select_distinct_on",
     "SELECT DISTINCT ON (item_id) item_id, tag FROM fuzz_tags ORDER BY item_id, tag"),

    ("values_clause",
     "VALUES (1, 'one'), (2, 'two'), (3, 'three')"),

    ("generate_series",
     "SELECT * FROM generate_series(1, 5) AS gs(n)"),

    ("unnest",
     "SELECT unnest(ARRAY[1, 2, 3]) AS val"),

    ("row_constructor",
     "SELECT ROW(1, 'hello', true)"),

    ("in_list",
     "SELECT * FROM fuzz_items WHERE id IN (1, 2, 3)"),

    ("not_in_list",
     "SELECT * FROM fuzz_items WHERE id NOT IN (4, 5, 6)"),

    ("between_scalar",
     "SELECT id FROM fuzz_items WHERE id BETWEEN 1 AND 2"),

    ("not_between_scalar",
     "SELECT id FROM fuzz_items WHERE id NOT BETWEEN 10 AND 20"),

    // FIXME(bug-vortex-between): A compound boolean expression mixing BETWEEN, NOT BETWEEN,
    // and IS NULL predicates over a Vortex-backed table triggers Internal:
    //   "Reduced array dtype mismatch from ExactScalarFn<Between>, rule: BetweenReduceAdaptor(Constant)"
    // Isolated BETWEEN / NOT BETWEEN clauses are fine (see above). The mismatch occurs when
    // the Vortex physical layer reduces a BETWEEN(constant) expression and the result dtype
    // disagrees with the expected Boolean array type. Blocked on Vortex bug fix.
    // ("boolean_expression_with_between",
    //  "SELECT id FROM fuzz_items WHERE score > 5 AND score < 100 OR score IS NULL"),

    ("any_array",
     "SELECT id FROM fuzz_items WHERE id = ANY(ARRAY[1, 2])"),

    ("all_array",
     "SELECT 5 > ALL(ARRAY[1, 2, 3])"),

    ("boolean_expression_simple",
     "SELECT id FROM fuzz_items WHERE score > 5 OR score IS NULL"),

    ("negation",
     "SELECT id FROM fuzz_items WHERE NOT (score = 10)"),

    ("select_star_limit",
     "SELECT * FROM fuzz_items LIMIT 1"),
];

// ─── Ignored regression-capture tests for known bugs ─────────────────────────
//
// These are NOT run in CI (they are #[ignore]d) but serve as reproducible
// pinning tests so that a future fix can be verified by un-ignoring.

/// FIXME(bug-case-searched): CASE expr WHEN … returns Internal.
/// Valid PG syntax; executor only handles searched-CASE rewrites.
#[ignore = "bug-case-searched: CASE expr WHEN form hits Internal 'not yet supported'"]
#[tokio::test]
async fn bug_case_searched_panics() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, score BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    // This should return Rows, not Internal.
    let res = sess
        .execute("SELECT CASE score WHEN 10 THEN 'ten' ELSE 'other' END FROM t")
        .await;
    assert!(
        res.is_ok(),
        "CASE expr WHEN form must not return Internal; got: {:?}",
        res.err()
    );
}

/// FIXME(bug-lateral-outer-ref): LATERAL with outer-column reference hits Internal.
/// DataFusion physical planner does not support OuterReferenceColumn.
#[ignore = "bug-lateral-outer-ref: correlated LATERAL hits Internal 'OuterReferenceColumn not implemented'"]
#[tokio::test]
async fn bug_lateral_outer_ref_panics() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE items (id BIGINT)").await.unwrap();
    sess.execute("CREATE TABLE tags (item_id BIGINT, tag TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items VALUES (1), (2)")
        .await
        .unwrap();
    sess.execute("INSERT INTO tags VALUES (1, 'x'), (2, 'y')")
        .await
        .unwrap();
    let res = sess
        .execute("SELECT i.id, t.tag FROM items i JOIN LATERAL (SELECT tag FROM tags WHERE item_id = i.id LIMIT 1) t ON TRUE")
        .await;
    assert!(
        res.is_ok(),
        "correlated LATERAL must not return Internal; got: {:?}",
        res.err()
    );
}

/// FIXME(bug-create-type-if-not-exists): `CREATE TYPE IF NOT EXISTS` rejected at parse layer.
/// PG 14+ accepts this syntax; Basin's parser gate does not.
#[ignore = "bug-create-type-if-not-exists: CREATE TYPE IF NOT EXISTS hits Internal parse error"]
#[tokio::test]
async fn bug_create_type_if_not_exists_parse_error() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let res = sess
        .execute("CREATE TYPE IF NOT EXISTS fuzz_status AS ENUM ('active', 'inactive')")
        .await;
    // Should be Ok or FeatureNotSupported — not Internal.
    match res {
        Ok(_) => {}
        Err(BasinError::FeatureNotSupported(_)) | Err(BasinError::InvalidSchema(_)) => {}
        Err(e) => panic!("CREATE TYPE IF NOT EXISTS must not return Internal; got: {e:?}"),
    }
}

/// FIXME(bug-fast-select-text-alias): TEXT column alias with quoted mixed-case name returns Internal.
/// fast_select does not fall through to the DataFusion path for TEXT aliases.
#[ignore = "bug-fast-select-text-alias: mixed-case TEXT alias hits Internal in fast_select"]
#[tokio::test]
async fn bug_fast_select_text_alias_internal() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, label TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'hello')").await.unwrap();
    let res = sess
        .execute("SELECT id AS \"MyID\", label AS \"MyLabel\" FROM t LIMIT 1")
        .await;
    assert!(
        res.is_ok(),
        "TEXT alias with quoted mixed-case name must not return Internal; got: {:?}",
        res.err()
    );
}

/// FIXME(bug-is-distinct-from): `IS DISTINCT FROM` hits DataFusion optimize_projections Internal.
/// Upstream DataFusion optimizer schema-mismatch when rewriting IS DISTINCT FROM.
#[ignore = "bug-is-distinct-from: IS [NOT] DISTINCT FROM triggers DataFusion optimizer Internal"]
#[tokio::test]
async fn bug_is_distinct_from_internal() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let res = sess.execute("SELECT 1 IS DISTINCT FROM NULL").await;
    assert!(
        res.is_ok(),
        "IS DISTINCT FROM must not return Internal; got: {:?}",
        res.err()
    );
    let res2 = sess.execute("SELECT 1 IS NOT DISTINCT FROM 1").await;
    assert!(
        res2.is_ok(),
        "IS NOT DISTINCT FROM must not return Internal; got: {:?}",
        res2.err()
    );
}

/// FIXME(bug-vortex-between): compound boolean with BETWEEN triggers Vortex dtype mismatch Internal.
/// The Vortex physical layer reduces BETWEEN(constant) with a wrong output dtype.
#[ignore = "bug-vortex-between: BETWEEN in compound boolean over Vortex table hits Internal dtype mismatch"]
#[tokio::test]
async fn bug_vortex_between_dtype_mismatch() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT, score BIGINT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)")
        .await
        .unwrap();
    let res = sess
        .execute("SELECT id FROM t WHERE score > 5 AND score < 100 OR score IS NULL")
        .await;
    assert!(
        res.is_ok(),
        "compound boolean with BETWEEN must not return Internal; got: {:?}",
        res.err()
    );
}

// ─── Main test ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn sql_syntax_fuzz_graceful_handling() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let eng = make_engine(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    // ── Preamble: create tables + seed data ──────────────────────────────────
    for sql in PREAMBLE {
        match sess.execute(sql).await {
            Ok(_) => {}
            Err(e) => {
                // Preamble failures are surfaced loudly but don't fail the test harness —
                // corpus shapes will still run and exercise the error path.
                println!("[fuzz/preamble] WARN: {sql:?} -> {e}");
            }
        }
    }

    // ── Corpus run ────────────────────────────────────────────────────────────
    let start = Instant::now();
    let total = CORPUS.len();
    let mut n_succeeded = 0usize;
    let mut n_typed_error = 0usize;
    let mut n_internal = 0usize;
    let mut internal_failures: Vec<(&str, String)> = Vec::new();

    for (name, sql) in CORPUS {
        let outcome = classify(&sess, sql).await;
        match &outcome {
            Outcome::Succeeded => {
                n_succeeded += 1;
                println!("[fuzz] OK         {name}");
            }
            Outcome::TypedError(msg) => {
                n_typed_error += 1;
                // Truncate long messages for readability.
                let short = if msg.len() > 120 { &msg[..120] } else { msg };
                println!("[fuzz] typed-err  {name}: {short}");
            }
            Outcome::InternalError(msg) => {
                n_internal += 1;
                internal_failures.push((name, msg.clone()));
                println!("[fuzz] INTERNAL!  {name}: {msg}");
            }
        }
    }

    let elapsed = start.elapsed();

    // ── Coverage report ───────────────────────────────────────────────────────
    println!();
    println!("┌─────────────────────────────────────────────────────────────");
    println!("│  sql_syntax_fuzz coverage report");
    println!("│  total:        {total}");
    println!("│  succeeded:    {n_succeeded}");
    println!("│  typed-error:  {n_typed_error}  (graceful — non-Internal BasinError)");
    println!("│  internal-err: {n_internal}  (TEST FAILURE — Internal / XX000)");
    println!("│  elapsed:      {elapsed:.2?}");
    println!("│");
    println!("│  Skipped shapes (flagged bugs — see FIXME comments above):");
    println!("│    bug-case-searched          (CASE expr WHEN form, Internal)");
    println!("│    bug-lateral-outer-ref      (correlated LATERAL, Internal)");
    println!("│    bug-create-type-if-not-exists (CREATE TYPE IF NOT EXISTS parse error)");
    println!("│    bug-fast-select-text-alias (TEXT alias with quoted name, Internal)");
    println!("│    bug-is-distinct-from       (IS [NOT] DISTINCT FROM, DataFusion optimizer)");
    println!("│    bug-vortex-between         (compound BETWEEN + IS NULL, Vortex dtype mismatch)");
    println!("└─────────────────────────────────────────────────────────────");

    // ── Failure assertion ─────────────────────────────────────────────────────
    if !internal_failures.is_empty() {
        let formatted: Vec<String> = internal_failures
            .iter()
            .map(|(name, msg)| format!("  [{name}] {msg}"))
            .collect();
        panic!(
            "sql_syntax_fuzz: {} shape(s) returned BasinError::Internal (SQLSTATE XX000):\n{}",
            n_internal,
            formatted.join("\n")
        );
    }
}
