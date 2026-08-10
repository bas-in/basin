//! Phase 5.25.C — golang-migrate compatibility test.
//!
//! Exercises golang-migrate's `migrate` CLI against a live Basin in-process
//! server and verifies:
//!   (a) `migrate up` exits 0
//!   (b) The `schema_migrations` table exists and is populated with the
//!       correct version numbers.
//!   (c) The migrated schema contains every application table and column in
//!       the expected `SchemaSnapshot`.
//!   (d) `migrate down N` rolls back the last N migrations and the tables
//!       are dropped.
//!   (e) `migrate version` prints the current version without error.
//!   (f) `migrate force V` manually sets the version (dirty-state recovery).
//!
//! ## Graceful degradation
//!
//! The `migrate` binary (golang-migrate CLI) is almost certainly not installed
//! in most developer environments and CI configurations.  The test guards as
//! follows:
//!
//! - If the `migrate` binary is NOT on PATH the test is marked `#[ignore]`
//!   (see `golang_migrate_up_down_version_force`) — Cargo prints it as
//!   "ignored" rather than "failed".
//! - When the binary IS present the test runs fully and panics on any
//!   assertion failure.
//!
//! ## golang-migrate internals relevant to Basin compatibility
//!
//! golang-migrate uses a `schema_migrations` table with two columns:
//!   `version  BIGINT NOT NULL PRIMARY KEY`
//!   `dirty    BOOLEAN NOT NULL`
//!
//! It acquires a `pg_advisory_lock` before writing to `schema_migrations` to
//! prevent concurrent runs.  Basin may stub advisory locks as no-ops; the
//! test will surface any failure here.
//!
//! ## Known Basin compatibility gaps (as of Phase 5.25.C)
//!
//! 1. **`pg_advisory_lock(…)` / `pg_advisory_unlock(…)`** — golang-migrate
//!    acquires an advisory lock before writing to `schema_migrations`.  Basin
//!    does not implement advisory locks; it must return a no-op row (i.e., a
//!    single boolean `true` row) rather than an error.  Until this is resolved
//!    `migrate up` is expected to fail even when the binary is present.
//!
//! 2. **`SET search_path`** — golang-migrate issues `SET search_path = public`
//!    on connect.  Basin's pgwire handler must accept and no-op this statement.
//!
//! 3. **`SELECT current_schema()`** — golang-migrate probes the active schema
//!    at startup; Basin must handle this built-in function.
//!
//! 4. **`CREATE TABLE IF NOT EXISTS schema_migrations …`** — golang-migrate
//!    creates its tracking table if absent.  Basin must support `IF NOT EXISTS`
//!    on `CREATE TABLE`.
//!
//! ## Fixture location
//!
//! `tests/integration/fixtures/migration-tool-scaffold/golang-migrate/`
//! contains 5 up/down migration pairs:
//!   000001_create_users.{up,down}.sql
//!   000002_create_teams.{up,down}.sql
//!   000003_create_posts.{up,down}.sql
//!   000004_add_users_display_name.{up,down}.sql
//!   000005_create_comments.{up,down}.sql

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

// ── Helper: detect migrate CLI ────────────────────────────────────────────────

/// Returns `true` when the `migrate` binary (golang-migrate CLI) is on PATH.
fn migrate_available() -> bool {
    // golang-migrate CLI exits 2 with usage info when invoked with no args,
    // but that still means the binary is present and executable.  We check
    // only that the spawn succeeds, not the exit code.
    Command::new("migrate").arg("-version").output().is_ok()
}

/// Absolute path to the golang-migrate fixture directory.
fn fixture_dir() -> PathBuf {
    let manifest =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by cargo test");
    PathBuf::from(manifest)
        .join("fixtures")
        .join("migration-tool-scaffold")
        .join("golang-migrate")
}

// ── Helper: run `migrate` subcommand ─────────────────────────────────────────

/// Run the `migrate` CLI with the given extra args, capturing output.
///
/// `database_url` must be a postgres:// DSN that golang-migrate understands.
/// `fixture_path` is passed as `-path`.
///
/// Returns `(stdout, stderr, success)`.
fn run_migrate(
    database_url: &str,
    fixture_path: &PathBuf,
    args: &[&str],
) -> (String, String, bool) {
    let output = Command::new("migrate")
        .arg("-path")
        .arg(fixture_path)
        .arg("-database")
        .arg(database_url)
        .args(args)
        .output()
        .expect("failed to spawn `migrate` process");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status.success())
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Run golang-migrate up/down/version/force against a live Basin instance.
///
/// # Ignored when golang-migrate is absent
///
/// This test is `#[ignore]`'d by default.  To run it you need the `migrate`
/// binary (golang-migrate CLI) on PATH:
///
/// ```text
/// cargo test -p basin-integration-tests --test migration_tool_golang_migrate \
///     -- --ignored
/// ```
///
/// In CI, install golang-migrate via:
///
/// ```yaml
/// - name: Install golang-migrate
///   run: |
///     curl -L https://github.com/golang-migrate/migrate/releases/download/v4.18.1/migrate.linux-amd64.tar.gz \
///       | tar xz -C /usr/local/bin
/// ```
///
/// # Basin compatibility gaps
///
/// The most likely failure point is `pg_advisory_lock` — golang-migrate calls
/// `SELECT pg_advisory_lock($1)` before writing to `schema_migrations`.
/// Basin currently stubs advisory locks; if the stub returns an error instead
/// of a no-op row, `migrate up` will fail.  See module-level docs for the
/// full gap list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires golang-migrate CLI (`migrate` binary) on PATH; \
            install from https://github.com/golang-migrate/migrate/releases; \
            see comment above for CI install steps"]
async fn golang_migrate_up_down_version_force() {
    // ── Runtime guard: skip cleanly if migrate binary is absent ───────────────
    if !migrate_available() {
        eprintln!(
            "[golang_migrate] SKIP — \
             `migrate` binary not found on PATH. \
             Install golang-migrate CLI (≥ v4.x) to enable this test. \
             See the ignore attribute comment for CI install steps."
        );
        return;
    }

    // ── Spin up Basin in-process ───────────────────────────────────────────────
    let server = spawn_basin_server_with_slug("golang_migrate_test").await;
    let port = server.addr.port();
    let user = &server.project_slug;

    println!("[golang_migrate] Basin in-process server at 127.0.0.1:{port}, user={user}");

    // ── Locate fixture directory ───────────────────────────────────────────────
    let fixture_path = fixture_dir();
    assert!(
        fixture_path.exists(),
        "golang-migrate fixture directory not found: {}",
        fixture_path.display()
    );

    // golang-migrate uses a postgres:// DSN (not JDBC).
    // Format: postgres://user:password@host:port/dbname?sslmode=disable
    //
    // Note: `sslmode=disable` is required because Basin's in-process server
    // is started without TLS.
    let db_url = format!("postgres://{user}:ignored@127.0.0.1:{port}/basin?sslmode=disable");

    // ── (a) migrate up ────────────────────────────────────────────────────────
    //
    // Known compatibility gaps that may surface here (see module-level docs):
    //   - pg_advisory_lock / pg_advisory_unlock
    //   - SET search_path
    //   - SELECT current_schema()
    //   - CREATE TABLE IF NOT EXISTS schema_migrations
    //
    // We capture output and print it regardless of exit status so CI logs
    // show the exact error when Basin rejects a statement.
    let (stdout, stderr, success) = run_migrate(&db_url, &fixture_path, &["up"]);
    println!("[golang_migrate up stdout]\n{stdout}");
    if !stderr.is_empty() {
        eprintln!("[golang_migrate up stderr]\n{stderr}");
    }

    assert!(
        success,
        "migrate up exited non-zero.\n\
         This likely means Basin rejected a SQL statement or advisory-lock call.\n\
         Check stderr above for the exact error and update the module-level \
         compatibility-gap list accordingly.\n\
         stderr: {stderr}"
    );

    // ── (b) assert schema_migrations table is populated ───────────────────────
    //
    // golang-migrate creates a `schema_migrations` table with columns:
    //   version BIGINT NOT NULL PRIMARY KEY
    //   dirty   BOOLEAN NOT NULL
    //
    // After `migrate up` (all 5 migrations), the table should have exactly
    // 1 row (golang-migrate stores only the LATEST version, not a history).
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after migrate up");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let sm_rows = client
        .query(
            "SELECT version, dirty FROM schema_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("query schema_migrations — did migrate up create the table?");

    assert_eq!(
        sm_rows.len(),
        1,
        "expected 1 row in schema_migrations after migrate up (golang-migrate stores \
         only the latest version), got {}.\nRows: {:?}",
        sm_rows.len(),
        sm_rows
            .iter()
            .map(|r| {
                let ver: i64 = r.get(0);
                let dirty: bool = r.get(1);
                format!("version={ver} dirty={dirty}")
            })
            .collect::<Vec<_>>()
    );

    let latest_version: i64 = sm_rows[0].get(0);
    let dirty: bool = sm_rows[0].get(1);
    assert_eq!(
        latest_version, 5,
        "schema_migrations: expected version=5 after `migrate up`, got version={latest_version}"
    );
    assert!(
        !dirty,
        "schema_migrations: dirty=true after `migrate up` — migration may have failed mid-run"
    );

    println!("[golang_migrate] schema_migrations: version=5, dirty=false — correct");

    // ── (c) assert migrated schema matches expectations ───────────────────────
    //
    // We check only application tables (not schema_migrations itself).
    // Extra columns (e.g. id columns, NOT NULL constraints) are permitted —
    // `assert_schema_matches_snapshot` is lenient about extras.
    let expected_after_up = SchemaSnapshot::new()
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
                ("published", "boolean"),
                ("created_at", "timestamp without time zone"),
            ],
        )
        .add_table(
            "comments",
            vec![
                ("id", "integer"),
                ("post_id", "integer"),
                ("author_id", "integer"),
                ("body", "text"),
                ("created_at", "timestamp without time zone"),
            ],
        );

    assert_schema_matches_snapshot(&server.conn_str, &expected_after_up).await;
    println!("[golang_migrate] schema snapshot after up: all 4 application tables verified");

    // ── Optional CRUD battery against the migrated schema ─────────────────────
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert user",
                sql: "INSERT INTO users (email, display_name) \
                      VALUES ('alice@example.com', 'Alice')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert team",
                sql: "INSERT INTO teams (name) VALUES ('engineering')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert post",
                sql: "INSERT INTO posts (author_id, title, body) \
                      VALUES (1, 'Hello Basin', 'First post body')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "count users",
                sql: "SELECT count(*) FROM users",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "count teams",
                sql: "SELECT count(*) FROM teams",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "count posts",
                sql: "SELECT count(*) FROM posts",
                expected_first_col: Some("1"),
            },
        ],
    )
    .await;

    println!("[golang_migrate] CRUD battery: all 6 queries passed");

    // ── (e) migrate version ───────────────────────────────────────────────────
    //
    // `migrate version` should print the current version (5) to stdout/stderr
    // and exit 0.
    let (stdout_ver, stderr_ver, success_ver) = run_migrate(&db_url, &fixture_path, &["version"]);
    println!("[golang_migrate version stdout]\n{stdout_ver}");
    if !stderr_ver.is_empty() {
        eprintln!("[golang_migrate version stderr]\n{stderr_ver}");
    }
    assert!(
        success_ver,
        "migrate version exited non-zero.\nstderr: {stderr_ver}"
    );
    // The version number appears in stdout or stderr depending on the CLI
    // version.  Either is fine — we just check the command succeeded.
    let version_output = format!("{stdout_ver}{stderr_ver}");
    assert!(
        version_output.contains('5'),
        "migrate version output did not contain '5' (current version).\n\
         Output: {version_output}"
    );
    println!("[golang_migrate] migrate version: reported version=5 — correct");

    // ── (d) migrate down 2 ────────────────────────────────────────────────────
    //
    // Roll back the last 2 migrations (000005_create_comments and
    // 000004_add_users_display_name).  After rollback:
    //   - `comments` table should be gone
    //   - `users.display_name` column should be gone
    //   - `users`, `teams`, `posts` should still exist
    let (stdout_down, stderr_down, success_down) =
        run_migrate(&db_url, &fixture_path, &["down", "2"]);
    println!("[golang_migrate down 2 stdout]\n{stdout_down}");
    if !stderr_down.is_empty() {
        eprintln!("[golang_migrate down 2 stderr]\n{stderr_down}");
    }
    assert!(
        success_down,
        "migrate down 2 exited non-zero.\nstderr: {stderr_down}"
    );

    // Re-open a connection after rollback (the previous client connection
    // might be stale after DDL).
    let (client2, conn2) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after migrate down 2");
    tokio::spawn(async move {
        let _ = conn2.await;
    });

    // Verify schema_migrations now shows version=3, dirty=false.
    let sm_after_down = client2
        .query(
            "SELECT version, dirty FROM schema_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("query schema_migrations after migrate down 2");

    assert_eq!(
        sm_after_down.len(),
        1,
        "expected 1 row in schema_migrations after migrate down 2, got {}",
        sm_after_down.len()
    );
    let ver_after: i64 = sm_after_down[0].get(0);
    let dirty_after: bool = sm_after_down[0].get(1);
    assert_eq!(
        ver_after, 3,
        "schema_migrations: expected version=3 after `migrate down 2`, got version={ver_after}"
    );
    assert!(
        !dirty_after,
        "schema_migrations: dirty=true after `migrate down 2`"
    );

    // Verify the application schema reflects the rollback.
    let expected_after_down = SchemaSnapshot::new()
        .add_table(
            "users",
            // display_name should be gone after rolling back migration 4
            vec![
                ("id", "integer"),
                ("email", "text"),
                ("created_at", "timestamp without time zone"),
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
                ("published", "boolean"),
                ("created_at", "timestamp without time zone"),
            ],
        );

    assert_schema_matches_snapshot(&server.conn_str, &expected_after_down).await;
    println!("[golang_migrate] schema snapshot after down 2: rollback verified");

    // Also verify that `comments` table is gone.
    let rows_comments = client2
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'comments'",
            &[],
        )
        .await
        .expect("check comments table existence");
    assert!(
        rows_comments.is_empty(),
        "`comments` table should not exist after `migrate down 2`, but it does"
    );

    println!(
        "[golang_migrate] migrate down 2: schema_migrations version=3, \
         comments table gone — correct"
    );

    // ── (f) migrate force ─────────────────────────────────────────────────────
    //
    // `migrate force V` manually sets the version in schema_migrations without
    // running any SQL.  This is the recovery path for dirty migrations.
    // We force to version 1 and verify schema_migrations reflects it.
    let (stdout_force, stderr_force, success_force) =
        run_migrate(&db_url, &fixture_path, &["force", "1"]);
    println!("[golang_migrate force 1 stdout]\n{stdout_force}");
    if !stderr_force.is_empty() {
        eprintln!("[golang_migrate force 1 stderr]\n{stderr_force}");
    }
    assert!(
        success_force,
        "migrate force 1 exited non-zero.\nstderr: {stderr_force}"
    );

    let sm_after_force = client2
        .query(
            "SELECT version, dirty FROM schema_migrations ORDER BY version",
            &[],
        )
        .await
        .expect("query schema_migrations after migrate force 1");
    assert_eq!(
        sm_after_force.len(),
        1,
        "expected 1 row in schema_migrations after migrate force 1, got {}",
        sm_after_force.len()
    );
    let ver_forced: i64 = sm_after_force[0].get(0);
    let dirty_forced: bool = sm_after_force[0].get(1);
    assert_eq!(
        ver_forced, 1,
        "schema_migrations: expected version=1 after `migrate force 1`, got version={ver_forced}"
    );
    assert!(
        !dirty_forced,
        "schema_migrations: dirty=true after `migrate force 1` — \
         force should always set dirty=false"
    );

    println!(
        "[golang_migrate] migrate force 1: schema_migrations version=1, dirty=false — correct"
    );
    println!("[golang_migrate_up_down_version_force] PASSED");
}
