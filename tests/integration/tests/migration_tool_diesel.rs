//! Phase 5.25.D — Diesel migration-tool compatibility test.
//!
//! Exercises the `diesel` CLI (`diesel migration run` / `revert` / `generate`)
//! against a live Basin in-process server and verifies:
//!   (a) `diesel migration run` exits 0
//!   (b) The `__diesel_schema_migrations` tracking table exists and has the
//!       expected version rows (one per applied migration).
//!   (c) The migrated schema contains every application table and column in
//!       the expected `SchemaSnapshot`.
//!   (d) A JSONB-column round-trip via plain SQL (simulating what Diesel's
//!       `Jsonb` type mapping would produce at the ORM level).
//!   (e) `diesel migration revert` rolls back the most-recent migration.
//!
//! ## Graceful degradation
//!
//! The `diesel` CLI requires:
//!   - The `diesel` binary on PATH (`cargo install diesel_cli --no-default-features --features postgres`)
//!   - `libpq` available for linking (the diesel CLI links against it)
//!
//! If the `diesel` binary is NOT on PATH the test is marked `#[ignore]` and
//! skips cleanly — Cargo prints "ignored" rather than "failed".
//!
//! To run manually once the tools are available:
//!
//! ```text
//! cargo test -p basin-integration-tests --test migration_tool_diesel \
//!     -- --ignored
//! ```
//!
//! ## Diesel migration tracking
//!
//! Diesel stores applied migrations in `__diesel_schema_migrations` with schema:
//!   `version  VARCHAR(50) NOT NULL PRIMARY KEY`
//!   `run_on   TIMESTAMP   NOT NULL DEFAULT now()`
//!
//! The version strings for Diesel are the directory name prefixes, e.g.
//! `"2024-01-01-000001"` for a directory named
//! `2024-01-01-000001_create_users`.
//!
//! ## Known Basin compatibility gaps (as of Phase 5.25.D)
//!
//! 1. **`__diesel_schema_migrations` table creation** — Diesel creates this
//!    table via `CREATE TABLE IF NOT EXISTS` on first run.  Basin must support
//!    `IF NOT EXISTS` on `CREATE TABLE` (verified by `ddl_completeness.rs`).
//!
//! 2. **`SHOW search_path`** — Diesel CLI may issue `SHOW search_path` at
//!    startup.  Basin must handle this `SHOW` variant.
//!
//! 3. **`SET search_path`** — Diesel may set the search path before running
//!    migrations.  Basin's pgwire handler must accept and no-op it.
//!
//! 4. **`SELECT pg_advisory_lock(…)`** — Some versions of Diesel CLI acquire
//!    an advisory lock before writing to `__diesel_schema_migrations`.  Basin
//!    must return a no-op row rather than an error.
//!
//! 5. **JSONB column type** — The posts migration creates a `JSONB` column.
//!    Basin must accept `JSONB` in DDL and return valid JSONB on SELECT.
//!
//! ## Fixture location
//!
//! `tests/integration/fixtures/migration-tool-scaffold/diesel/migrations/`
//! contains 4 migration directories:
//!   2024-01-01-000001_create_users/{up,down}.sql
//!   2024-01-02-000002_create_teams/{up,down}.sql
//!   2024-01-03-000003_create_posts/{up,down}.sql  ← includes JSONB column
//!   2024-01-04-000004_add_users_meta/{up,down}.sql

#![allow(clippy::print_stdout, clippy::print_stderr)]

// Pull in the shared scaffold helpers without re-declaring the module.
#[path = "migration_tool_common.rs"]
mod common;

use common::{
    assert_schema_matches_snapshot, run_crud_battery, spawn_basin_server_with_slug, CrudQuery,
    SchemaSnapshot,
};
use std::path::PathBuf;
use std::process::Command;
use tokio_postgres::NoTls;

// ── Helper: detect diesel CLI ─────────────────────────────────────────────────

/// Returns `true` when the `diesel` CLI binary is on PATH and can be invoked.
///
/// The diesel CLI links against libpq; it may be installed via:
/// ```text
/// cargo install diesel_cli --no-default-features --features postgres
/// ```
fn diesel_available() -> bool {
    // `diesel --version` exits 0 when the binary is present.
    Command::new("diesel")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Absolute path to the diesel migrations fixture directory.
fn fixture_migrations_dir() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR must be set by cargo test");
    PathBuf::from(manifest)
        .join("fixtures")
        .join("migration-tool-scaffold")
        .join("diesel")
        .join("migrations")
}

// ── Helper: run `diesel migration <subcommand>` ───────────────────────────────

/// Run `diesel migration <args>` with the given DATABASE_URL and
/// `--migration-dir` pointing at the fixture directory.
///
/// Returns `(stdout, stderr, success)`.
fn run_diesel_migration(database_url: &str, migrations_dir: &PathBuf, args: &[&str]) -> (String, String, bool) {
    let output = Command::new("diesel")
        .arg("migration")
        .args(args)
        .arg("--migration-dir")
        .arg(migrations_dir)
        .env("DATABASE_URL", database_url)
        .output()
        .expect("failed to spawn `diesel` process");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status.success())
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Run Diesel migrations (run/revert) against a live Basin instance and verify
/// the tracking table, final schema, and a JSONB column round-trip.
///
/// # Ignored when diesel CLI is absent
///
/// This test is `#[ignore]`'d by default.  To run it you need the `diesel`
/// CLI on PATH (with postgres feature enabled).  Install via:
///
/// ```text
/// cargo install diesel_cli --no-default-features --features postgres
/// ```
///
/// In CI, the same `cargo install` command works after ensuring `libpq-dev`
/// (Debian/Ubuntu) or `libpq` (macOS Homebrew) is installed first.
///
/// # Basin compatibility gaps
///
/// See the module-level documentation for the full list.  The most likely
/// failure point is `JSONB` column support in DDL (migration 3) and any
/// advisory-lock call Diesel may issue against `__diesel_schema_migrations`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires diesel CLI (with postgres feature) on PATH; \
            install via `cargo install diesel_cli --no-default-features --features postgres`; \
            also requires libpq (libpq-dev on Debian/Ubuntu, postgresql on Homebrew)"]
async fn diesel_migration_run_revert() {
    // ── Runtime guard: skip cleanly if diesel binary is absent ────────────────
    if !diesel_available() {
        eprintln!(
            "[diesel_migration_run_revert] SKIP — \
             `diesel` binary not found on PATH. \
             Install diesel_cli with postgres feature: \
             `cargo install diesel_cli --no-default-features --features postgres`. \
             libpq must be installed first (libpq-dev on Debian/Ubuntu, \
             postgresql on macOS Homebrew)."
        );
        return;
    }

    // ── Spin up Basin in-process ───────────────────────────────────────────────
    let server = spawn_basin_server_with_slug("diesel_test").await;
    let port = server.addr.port();
    let user = &server.project_slug;

    println!(
        "[diesel] Basin in-process server at 127.0.0.1:{port}, user={user}"
    );

    // ── Locate fixture migrations directory ───────────────────────────────────
    let migrations_dir = fixture_migrations_dir();
    assert!(
        migrations_dir.exists(),
        "diesel fixture migrations directory not found: {}",
        migrations_dir.display()
    );

    // Diesel uses a postgres:// DSN as DATABASE_URL.
    // sslmode=disable required because the in-process Basin server has no TLS.
    let db_url = format!(
        "postgres://{user}:ignored@127.0.0.1:{port}/basin?sslmode=disable"
    );

    // ── (a) diesel migration run ──────────────────────────────────────────────
    //
    // Runs all pending migrations.  Known compatibility gaps that may surface
    // here (see module-level docs):
    //   - __diesel_schema_migrations CREATE TABLE IF NOT EXISTS
    //   - SHOW / SET search_path
    //   - pg_advisory_lock (some Diesel CLI versions)
    //   - JSONB column in migration 3
    //   - ALTER TABLE ADD COLUMN in migration 4
    //
    // We capture output and print it regardless of exit status so CI logs show
    // the exact error when Basin rejects a statement.
    let (stdout_run, stderr_run, success_run) =
        run_diesel_migration(&db_url, &migrations_dir, &["run"]);
    println!("[diesel migration run stdout]\n{stdout_run}");
    if !stderr_run.is_empty() {
        eprintln!("[diesel migration run stderr]\n{stderr_run}");
    }

    assert!(
        success_run,
        "diesel migration run exited non-zero.\n\
         This likely means Basin rejected a SQL statement Diesel issued.\n\
         Check stderr above for the exact error and update the module-level \
         compatibility-gap list accordingly.\n\
         stderr: {stderr_run}"
    );

    // ── (b) assert __diesel_schema_migrations is populated ────────────────────
    //
    // Diesel stores one row per applied migration in __diesel_schema_migrations.
    // After running all 4 migrations there should be exactly 4 rows.
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after diesel migration run");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let history_rows = client
        .query(
            "SELECT version FROM __diesel_schema_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("query __diesel_schema_migrations — did diesel create the table?");

    assert_eq!(
        history_rows.len(),
        4,
        "expected 4 rows in __diesel_schema_migrations after `diesel migration run`, got {}.\n\
         Rows: {:?}",
        history_rows.len(),
        history_rows
            .iter()
            .map(|r| { let v: &str = r.get(0); v.to_owned() })
            .collect::<Vec<_>>()
    );

    // The version prefix is the directory name up to (and including) the
    // numeric suffix: e.g. "2024-01-01-000001".
    let expected_versions = [
        "2024-01-01-000001",
        "2024-01-02-000002",
        "2024-01-03-000003",
        "2024-01-04-000004",
    ];
    for (i, row) in history_rows.iter().enumerate() {
        let version: &str = row.get(0);
        assert_eq!(
            version, expected_versions[i],
            "__diesel_schema_migrations row {i}: expected version={}, got={version}",
            expected_versions[i]
        );
    }

    println!("[diesel] __diesel_schema_migrations: 4 rows, versions verified");

    // ── (c) assert migrated schema matches expectations ───────────────────────
    //
    // Check application tables only (not __diesel_schema_migrations itself).
    let expected = SchemaSnapshot::new()
        .add_table(
            "users",
            vec![
                ("id", "integer"),
                ("email", "text"),
                ("created_at", "timestamp without time zone"),
                ("display_name", "text"),
            ],
        )
        .add_table(
            "teams",
            vec![
                ("id", "integer"),
                ("name", "text"),
                ("created_at", "timestamp without time zone"),
            ],
        )
        .add_table(
            "posts",
            vec![
                ("id", "integer"),
                ("author_id", "integer"),
                ("title", "text"),
                ("body", "text"),
                ("metadata", "jsonb"),
                ("published", "boolean"),
                ("created_at", "timestamp without time zone"),
            ],
        );

    assert_schema_matches_snapshot(&server.conn_str, &expected).await;
    println!("[diesel] schema snapshot: 3 application tables verified");

    // ── (d) JSONB column round-trip ───────────────────────────────────────────
    //
    // Simulate what Diesel's `Jsonb` type mapping would do at the ORM level:
    // insert a row with a JSONB value, then select it back and verify the
    // round-trip is lossless.  We use plain SQL here because we cannot link
    // the Diesel ORM crate from the integration-test harness (it would require
    // `diesel` as a Cargo dependency + code-gen).  The SQL path exercises the
    // same wire protocol that Diesel Client uses.
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert user for jsonb test",
                sql: "INSERT INTO users (email) VALUES ('diesel@example.com')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert post with jsonb metadata",
                sql: "INSERT INTO posts (author_id, title, body, metadata) \
                      VALUES (1, 'JSONB test', 'body text', \
                      '{\"tags\":[\"rust\",\"diesel\"],\"priority\":1}'::jsonb)",
                expected_first_col: None,
            },
            CrudQuery {
                label: "select jsonb field via ->>",
                // Extract the first tag from the metadata JSONB column.
                // Diesel's Jsonb type mapping round-trips through serde_json::Value.
                sql: "SELECT metadata->>'tags' FROM posts WHERE title = 'JSONB test'",
                // The ->> operator returns the JSON array as a text string.
                expected_first_col: Some("[\"rust\",\"diesel\"]"),
            },
            CrudQuery {
                label: "insert team",
                sql: "INSERT INTO teams (name) VALUES ('diesel-team')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "count users",
                sql: "SELECT count(*) FROM users",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "count posts",
                sql: "SELECT count(*) FROM posts",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "count teams",
                sql: "SELECT count(*) FROM teams",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "update post title",
                sql: "UPDATE posts SET title = 'JSONB test (updated)' \
                      WHERE title = 'JSONB test'",
                expected_first_col: None,
            },
            CrudQuery {
                label: "verify updated title",
                sql: "SELECT title FROM posts WHERE title = 'JSONB test (updated)'",
                expected_first_col: Some("JSONB test (updated)"),
            },
            CrudQuery {
                label: "delete post",
                sql: "DELETE FROM posts WHERE title = 'JSONB test (updated)'",
                expected_first_col: None,
            },
            CrudQuery {
                label: "count posts after delete",
                sql: "SELECT count(*) FROM posts",
                expected_first_col: Some("0"),
            },
        ],
    )
    .await;

    println!("[diesel] JSONB round-trip + CRUD battery: all queries passed");

    // ── (e) diesel migration revert ───────────────────────────────────────────
    //
    // Roll back the most-recent migration (migration 4: add_users_meta).
    // After revert, `users.display_name` should be gone.
    let (stdout_revert, stderr_revert, success_revert) =
        run_diesel_migration(&db_url, &migrations_dir, &["revert"]);
    println!("[diesel migration revert stdout]\n{stdout_revert}");
    if !stderr_revert.is_empty() {
        eprintln!("[diesel migration revert stderr]\n{stderr_revert}");
    }

    assert!(
        success_revert,
        "diesel migration revert exited non-zero.\n\
         stderr: {stderr_revert}"
    );

    // After revert, __diesel_schema_migrations should have 3 rows.
    let (client2, conn2) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after diesel migration revert");
    tokio::spawn(async move {
        let _ = conn2.await;
    });

    let history_after_revert = client2
        .query(
            "SELECT version FROM __diesel_schema_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("query __diesel_schema_migrations after revert");

    assert_eq!(
        history_after_revert.len(),
        3,
        "expected 3 rows in __diesel_schema_migrations after `diesel migration revert`, got {}",
        history_after_revert.len()
    );

    // display_name should be gone from users.
    let cols_after_revert = client2
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'users' \
             ORDER BY ordinal_position",
            &[],
        )
        .await
        .expect("query information_schema.columns for users after revert");

    let col_names: Vec<String> = cols_after_revert
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    assert!(
        !col_names.contains(&"display_name".to_owned()),
        "users.display_name should not exist after `diesel migration revert`, \
         but columns are: {col_names:?}"
    );

    println!(
        "[diesel] migration revert: __diesel_schema_migrations has 3 rows, \
         users.display_name removed — correct"
    );
    println!("[diesel_migration_run_revert] PASSED");
}
