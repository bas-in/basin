//! Parser-level integration tests for ADR 0014 Phase 1.
//!
//! These tests call `basin_engine::pg_ast` directly — no pgwire, no engine
//! boot, no tokio required. They prove that:
//!
//! 1. The real PostgreSQL parser (libpg_query) accepts the full set of PG
//!    statement forms that sqlparser-rs 0.52 previously could not model.
//! 2. `stmt_kind` classifies each statement correctly so the executor can
//!    dispatch without textual pre-screens.
//! 3. `reject_unsupported` blocks every "known-but-not-yet-shipped" kind
//!    with SQLSTATE 0A000 and lets every supported kind through.
//!
//! The headline test is the subquery-in-WHERE case that previously caused
//! sqlparser to emit a confusing error — pg_query parses it cleanly and
//! returns `StmtKind::Select`.

use basin_common::BasinError;
use basin_engine::pg_ast::{self, StmtKind};

// ──────────────────────────────────────────────────────────────────────────────
// Helper
// ──────────────────────────────────────────────────────────────────────────────

/// Parse `sql`, expect exactly one statement, return its `StmtKind`.
fn first_kind(sql: &str) -> StmtKind {
    let tree = pg_ast::parse(sql).unwrap_or_else(|e| panic!("parse {sql:?} failed: {e}"));
    let node = tree
        .stmts()
        .next()
        .unwrap_or_else(|| panic!("no statements in {sql:?}"));
    pg_ast::stmt_kind(node)
}

// ──────────────────────────────────────────────────────────────────────────────
// Parse-success tests — pg_query accepts and classifies correctly
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn create_domain_is_accepted() {
    // sqlparser 0.52 rejects CREATE DOMAIN entirely; pg_query parses it fine.
    assert_eq!(
        first_kind("CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)"),
        StmtKind::CreateDomain,
    );
}

#[test]
fn alter_type_add_value_is_accepted() {
    // sqlparser 0.52 has no AlterType node; this required a textual pre-screen.
    assert_eq!(
        first_kind("ALTER TYPE color ADD VALUE 'purple'"),
        StmtKind::AlterType,
    );
}

#[test]
fn refresh_materialized_view_is_accepted() {
    // REFRESH MATERIALIZED VIEW has no sqlparser AST node at all.
    assert_eq!(
        first_kind("REFRESH MATERIALIZED VIEW sales_summary"),
        StmtKind::RefreshMatView,
    );
}

#[test]
fn refresh_materialized_view_with_options_is_a_syntax_error() {
    // `REFRESH MATERIALIZED VIEW … WITH (full = true)` is a Basin-specific
    // extension handled by a textual pre-screen in executor.rs *before* pg_query
    // sees the SQL. Real PostgreSQL does not accept this syntax, so pg_query
    // correctly rejects it with a parse error (SQLSTATE 42601). The pre-screen
    // in `cv_ddl::match_refresh_materialized_view` intercepts the statement
    // before it reaches the pg_query gate, so Basin users never see this error.
    // This test documents the behaviour so it is not mistakenly "fixed" to
    // expect parse success.
    let err = pg_ast::parse("REFRESH MATERIALIZED VIEW sales_summary WITH (full = true)")
        .expect_err("Basin-specific WITH (full=true) syntax is not valid PG SQL");
    assert!(
        matches!(err, BasinError::InvalidSchema(_)),
        "expected parse error (42601 / InvalidSchema), got {err:?}",
    );
}

#[test]
fn alter_function_rename_is_accepted() {
    // sqlparser 0.52 has no AlterFunction node; this required a textual
    // pre-screen in executor.rs.
    //
    // libpg_query routes `ALTER FUNCTION … RENAME TO` through `RenameStmt`,
    // NOT `AlterFunctionStmt`. The Phase 1 `stmt_kind` classifier therefore
    // returns `StmtKind::Other` for this form, which causes the executor to
    // fall through to the existing textual pre-screen / legacy pipeline. That
    // is intentional for Phase 1: the task spec explicitly says "if the
    // foundation surfaces RenameStmt under a different variant, document and
    // test what it returns." Agents 2–4 will add a `RenameStmt` arm to
    // `stmt_kind` so it returns `StmtKind::AlterFunction` and the pre-screen
    // can be retired.
    //
    // The critical thing proven here is that pg_query *parses* the statement
    // without error — sqlparser's failure was a parse error, not a dispatch
    // gap.
    let tree = pg_ast::parse("ALTER FUNCTION foo(int) RENAME TO bar")
        .expect("pg_query must parse ALTER FUNCTION … RENAME TO without error");
    let node = tree.stmts().next().expect("at least one statement");
    let kind = pg_ast::stmt_kind(node);
    // Accepted by the parser; falls into Other (RenameStmt) pending Phase 2.
    assert_eq!(kind, StmtKind::Other, "expected Other (RenameStmt path) for ALTER FUNCTION RENAME TO in Phase 1");
}

#[test]
fn create_procedure_language_sql_is_accepted() {
    // sqlparser 0.52 only parses the T-SQL `AS BEGIN … END` shape.
    assert_eq!(
        first_kind("CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$"),
        StmtKind::CreateProcedure,
    );
}

#[test]
fn create_type_as_enum_is_accepted() {
    assert_eq!(
        first_kind("CREATE TYPE color AS ENUM ('red', 'green')"),
        StmtKind::CreateType,
    );
}

#[test]
fn create_function_language_sql_is_accepted() {
    assert_eq!(
        first_kind("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"),
        StmtKind::CreateFunction,
    );
}

#[test]
fn create_sequence_is_accepted() {
    assert_eq!(
        first_kind("CREATE SEQUENCE s START 100"),
        StmtKind::CreateSequence,
    );
}

#[test]
fn select_with_subquery_in_where_is_accepted() {
    // Headline test: sqlparser 0.52 rejected `WHERE c IN (SELECT …)` in many
    // UPDATE/DELETE positions; pg_query accepts the full PG syntax.
    assert_eq!(
        first_kind("SELECT * FROM t WHERE c IN (SELECT id FROM u)"),
        StmtKind::Select,
    );
}

#[test]
fn insert_on_conflict_do_update_is_accepted() {
    // ON CONFLICT … DO UPDATE is currently 🛠 in CAPABILITIES.md (clause
    // parsed but ignored at execution time). This test proves the parser
    // accepts it, which is the precondition for Phase 2 execution support.
    assert_eq!(
        first_kind(
            "INSERT INTO t VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET c = excluded.c"
        ),
        StmtKind::Insert,
    );
}

#[test]
fn cte_select_is_accepted() {
    // Common Table Expressions were accepted by sqlparser but occasionally
    // mis-classified; pg_query parses them cleanly as SelectStmt.
    assert_eq!(
        first_kind("WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"),
        StmtKind::Select,
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Unsupported-rejection tests — reject_unsupported returns Err with 0A000
// ──────────────────────────────────────────────────────────────────────────────

/// Assert that `reject_unsupported` returns a `FeatureNotSupported` error
/// whose message contains "0A000".
fn assert_rejected(sql: &str) {
    let tree = pg_ast::parse(sql).unwrap_or_else(|e| panic!("parse {sql:?} failed: {e}"));
    let err = pg_ast::reject_unsupported(&tree)
        .expect_err(&format!("{sql:?} should have been rejected"));
    match err {
        BasinError::FeatureNotSupported(msg) => {
            assert!(
                msg.contains("0A000"),
                "{sql:?}: expected SQLSTATE 0A000 in rejection message, got: {msg}",
            );
        }
        other => panic!("{sql:?}: expected FeatureNotSupported, got {other:?}"),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// LISTEN / NOTIFY / UNLISTEN — remain in is_unsupported(); rejected with 0A000
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn listen_is_rejected() {
    assert_rejected("LISTEN ch1");
}

#[test]
fn notify_is_rejected() {
    assert_rejected("NOTIFY ch1, 'hi'");
}

#[test]
fn unlisten_is_rejected() {
    assert_rejected("UNLISTEN ch1");
}

// ──────────────────────────────────────────────────────────────────────────────
// Noop-accepted statements — reject_unsupported passes them through (Ok)
// because they are intercepted by noop_accept::try_accept_as_noop before
// reject_unsupported runs in the executor. Testing here proves the pg_ast
// classification is correct; end-to-end acceptance is proven in
// tests/integration/tests/noop_accept.rs.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn prepare_is_noop_accepted() {
    // Previously rejected with 0A000; now in noop_accept set.
    assert_allowed("PREPARE stmt AS SELECT 1");
}

#[test]
fn execute_is_noop_accepted() {
    assert_allowed("EXECUTE stmt");
}

#[test]
fn deallocate_is_noop_accepted() {
    assert_allowed("DEALLOCATE stmt");
}

#[test]
fn begin_is_noop_accepted() {
    // Basin is auto-commit; BEGIN is silently accepted.
    assert_allowed("BEGIN");
}

#[test]
fn commit_is_noop_accepted() {
    assert_allowed("COMMIT");
}

#[test]
fn rollback_is_noop_accepted() {
    assert_allowed("ROLLBACK");
}

#[test]
fn savepoint_is_noop_accepted() {
    assert_allowed("SAVEPOINT sp1");
}

// PREPARE / DECLARE CURSOR / LOCK TABLE / VACUUM / ANALYZE / CLUSTER /
// CREATE EXTENSION / BEGIN / COMMIT / ROLLBACK / CREATE TRIGGER were all
// noop-accepted by a later sibling agent; their `_is_rejected` tests are
// stale and the canonical `_is_noop_accepted` tests below cover them.

#[test]
fn drop_trigger_is_noop_accepted() {
    assert_allowed("DROP TRIGGER trg ON t");
}

#[test]
fn create_extension_is_noop_accepted() {
    // Basin ships its own extension-equivalents natively (ADR 0002); loading
    // an external extension is a no-op.
    assert_allowed("CREATE EXTENSION pgcrypto");
}

#[test]
fn drop_extension_is_noop_accepted() {
    assert_allowed("DROP EXTENSION pgcrypto");
}

#[test]
fn lock_table_is_noop_accepted() {
    // Accepted by noop_accept since sibling agent a0f45b8b.
    assert_allowed("LOCK TABLE t");
}

#[test]
fn vacuum_is_noop_accepted() {
    assert_allowed("VACUUM");
}

#[test]
fn analyze_is_noop_accepted() {
    assert_allowed("ANALYZE");
}

#[test]
fn cluster_is_noop_accepted() {
    assert_allowed("CLUSTER t");
}

#[test]
fn merge_is_noop_accepted() {
    // MERGE INTO … USING … WHEN MATCHED / NOT MATCHED accepted in v0.1;
    // not executed (see noop_accept.rs and CAPABILITIES.md).
    assert_allowed(
        "MERGE INTO t USING src ON t.id = src.id \
         WHEN MATCHED THEN UPDATE SET val = src.val \
         WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)",
    );
}

#[test]
fn reindex_is_noop_accepted() {
    assert_allowed("REINDEX TABLE t");
}

#[test]
fn declare_cursor_is_noop_accepted() {
    // Cursor lifecycle is handled by sibling agent a193aadd (real impl) or
    // falls through. Either way it is no longer in is_unsupported().
    assert_allowed("DECLARE c CURSOR FOR SELECT 1");
}

// ──────────────────────────────────────────────────────────────────────────────
// Supported-passes-through tests — reject_unsupported returns Ok(())
// ──────────────────────────────────────────────────────────────────────────────

/// Assert that `reject_unsupported` returns `Ok(())` — the statement is
/// either supported or falls into `StmtKind::Other` (legacy pipeline).
fn assert_allowed(sql: &str) {
    let tree = pg_ast::parse(sql).unwrap_or_else(|e| panic!("parse {sql:?} failed: {e}"));
    pg_ast::reject_unsupported(&tree)
        .unwrap_or_else(|e| panic!("{sql:?} should pass through, got: {e}"));
}

#[test]
fn supported_statements_pass_through() {
    assert_allowed("SELECT 1");
    assert_allowed("CREATE TABLE t (id int)");
    assert_allowed("INSERT INTO t VALUES (1)");
    assert_allowed("UPDATE t SET id = 2");
    assert_allowed("DELETE FROM t");
}
