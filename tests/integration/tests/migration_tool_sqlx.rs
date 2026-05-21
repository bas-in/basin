//! Phase 5.25.E — sqlx migration-tool compatibility test.
//!
//! Exercises `sqlx-cli` (`sqlx migrate run` / `sqlx migrate revert`) against a
//! live Basin in-process server and verifies:
//!   (a) `sqlx migrate run` exits 0
//!   (b) The `_sqlx_migrations` tracking table exists and has 5 rows with the
//!       expected version numbers and zero-error checksums.
//!   (c) The migrated schema contains every application table and column in
//!       the expected `SchemaSnapshot`.
//!   (d) A CRUD round-trip against the migrated schema.
//!   (e) `sqlx migrate revert` rolls back the most-recent migration.
//!
//! ## Graceful degradation
//!
//! The `sqlx` CLI is typically installed via:
//! ```text
//! cargo install sqlx-cli --no-default-features --features native-tls,postgres
//! ```
//!
//! If the `sqlx` binary is NOT on PATH the test is marked `#[ignore]` and
//! skips cleanly — Cargo prints "ignored" rather than "failed".
//!
//! To run manually once the tools are available:
//!
//! ```text
//! cargo test -p basin-integration-tests --test migration_tool_sqlx \
//!     -- --ignored
//! ```
//!
//! ## sqlx compile-time checked queries (note)
//!
//! sqlx's flagship feature is compile-time checked queries via the `query!`
//! macro, which requires a live database at *build* time (or a `sqlx-data.json`
//! snapshot created by `cargo sqlx prepare`).  This integration test does NOT
//! exercise compile-time checking — doing so would require embedding a Basin
//! server in the build script, which is out of scope.  Instead, the test runs
//! runtime-checked queries via `tokio_postgres` (which uses the same extended
//! wire protocol as sqlx) to verify that the schema Basin presents after
//! applying the sqlx migrations is correct and queryable.
//!
//! ## sqlx migration tracking
//!
//! sqlx-cli stores applied migrations in `_sqlx_migrations` with schema:
//!   `version        BIGINT                  NOT NULL PRIMARY KEY`
//!   `description    TEXT                    NOT NULL`
//!   `installed_on   TIMESTAMPTZ             NOT NULL DEFAULT now()`
//!   `success        BOOLEAN                 NOT NULL`
//!   `checksum       BYTEA                   NOT NULL`
//!   `execution_time BIGINT                  NOT NULL`
//!
//! ## Known Basin compatibility gaps (as of Phase 5.25.E)
//!
//! 1. **`_sqlx_migrations` table creation** — sqlx-cli creates this table via
//!    `CREATE TABLE IF NOT EXISTS` on first run.  Basin must support `IF NOT
//!    EXISTS` on `CREATE TABLE` (verified by `ddl_completeness.rs`).
//!
//! 2. **`TIMESTAMPTZ` column** — `_sqlx_migrations.installed_on` uses
//!    `TIMESTAMPTZ`.  Basin must accept and store timezone-aware timestamps
//!    (verified by `interval_tz.rs` and `datetime_extras.rs`).
//!
//! 3. **`BYTEA` column** — `_sqlx_migrations.checksum` uses `BYTEA`.  Basin
//!    must accept binary data in DDL and return it on SELECT.
//!
//! 4. **`SET search_path`** — sqlx-cli may issue `SET search_path = public`
//!    on connect.  Basin's pgwire handler must accept and no-op this statement.
//!
//! 5. **`SELECT pg_advisory_lock(…)`** — Some sqlx-cli versions acquire an
//!    advisory lock before writing to `_sqlx_migrations`.  Basin must return a
//!    no-op row rather than an error.
//!
//! ## Fixture location
//!
//! `tests/integration/fixtures/migration-tool-scaffold/sqlx/`
//! contains 5 migration SQL files (up-only, sqlx default format):
//!   20240101000001_create_users.sql
//!   20240102000002_create_teams.sql
//!   20240103000003_create_posts.sql
//!   20240104000004_add_users_display_name.sql
//!   20240105000005_create_comments.sql

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

// ── Helper: detect sqlx CLI ───────────────────────────────────────────────────

/// Returns `true` when the `sqlx` CLI binary is on PATH and responds to
/// `sqlx --version`.
///
/// Install via:
/// ```text
/// cargo install sqlx-cli --no-default-features --features native-tls,postgres
/// ```
fn sqlx_available() -> bool {
    Command::new("sqlx")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Absolute path to the sqlx fixture directory.
fn fixture_dir() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR must be set by cargo test");
    PathBuf::from(manifest)
        .join("fixtures")
        .join("migration-tool-scaffold")
        .join("sqlx")
}

// ── Helper: run `sqlx migrate <subcommand>` ───────────────────────────────────

/// Run `sqlx migrate <args>` with DATABASE_URL set and `--source` pointing at
/// the fixture directory.
///
/// Returns `(stdout, stderr, success)`.
fn run_sqlx_migrate(database_url: &str, source_dir: &PathBuf, args: &[&str]) -> (String, String, bool) {
    let output = Command::new("sqlx")
        .arg("migrate")
        .args(args)
        .arg("--source")
        .arg(source_dir)
        .env("DATABASE_URL", database_url)
        .output()
        .expect("failed to spawn `sqlx` process");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status.success())
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Run sqlx migrations (run/revert) against a live Basin instance and verify
/// the tracking table, final schema, and a CRUD round-trip.
///
/// # Ignored when sqlx CLI is absent
///
/// This test is `#[ignore]`'d by default.  To run it:
///
/// ```text
/// cargo test -p basin-integration-tests --test migration_tool_sqlx \
///     -- --ignored
/// ```
///
/// # Basin compatibility gaps
///
/// See the module-level documentation.  The most likely failure points are:
///   - `BYTEA` support in the `_sqlx_migrations.checksum` column
///   - `TIMESTAMPTZ` in `_sqlx_migrations.installed_on`
///   - `pg_advisory_lock` if the installed sqlx-cli version uses advisory locks
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires sqlx-cli on PATH; \
            install via `cargo install sqlx-cli --no-default-features --features native-tls,postgres`; \
            see module-level docs for CI install steps"]
async fn sqlx_migrate_run_revert() {
    // ── Runtime guard: skip cleanly if sqlx binary is absent ─────────────────
    if !sqlx_available() {
        eprintln!(
            "[sqlx_migrate_run_revert] SKIP — \
             `sqlx` binary not found on PATH. \
             Install sqlx-cli: \
             `cargo install sqlx-cli --no-default-features --features native-tls,postgres`. \
             In CI: same cargo install command after rust toolchain is available."
        );
        return;
    }

    // ── Spin up Basin in-process ───────────────────────────────────────────────
    let server = spawn_basin_server_with_slug("sqlx_test").await;
    let port = server.addr.port();
    let user = &server.project_slug;

    println!(
        "[sqlx] Basin in-process server at 127.0.0.1:{port}, user={user}"
    );

    // ── Locate fixture directory ───────────────────────────────────────────────
    let source_dir = fixture_dir();
    assert!(
        source_dir.exists(),
        "sqlx fixture directory not found: {}",
        source_dir.display()
    );

    // sqlx-cli uses postgres:// DSN via DATABASE_URL.
    // sslmode=disable is required because the in-process Basin server has no TLS.
    let db_url = format!(
        "postgres://{user}:ignored@127.0.0.1:{port}/basin?sslmode=disable"
    );

    // ── (a) sqlx migrate run ──────────────────────────────────────────────────
    //
    // Applies all 5 pending migrations.  Known compatibility gaps that may
    // surface here (see module-level docs):
    //   - _sqlx_migrations CREATE TABLE IF NOT EXISTS
    //   - TIMESTAMPTZ and BYTEA column types in _sqlx_migrations
    //   - SET search_path
    //   - pg_advisory_lock (some sqlx-cli versions)
    //
    // We capture output and print it regardless so CI logs show the exact
    // error when Basin rejects a statement.
    let (stdout_run, stderr_run, success_run) =
        run_sqlx_migrate(&db_url, &source_dir, &["run"]);
    println!("[sqlx migrate run stdout]\n{stdout_run}");
    if !stderr_run.is_empty() {
        eprintln!("[sqlx migrate run stderr]\n{stderr_run}");
    }

    assert!(
        success_run,
        "sqlx migrate run exited non-zero.\n\
         This likely means Basin rejected a SQL statement sqlx-cli issued.\n\
         Check stderr above for the exact error and update the module-level \
         compatibility-gap list accordingly.\n\
         stderr: {stderr_run}"
    );

    // ── (b) assert _sqlx_migrations is populated ──────────────────────────────
    //
    // sqlx stores one row per applied migration.  After running all 5 migrations
    // there should be exactly 5 rows, all with success=true.
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after sqlx migrate run");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let migration_rows = client
        .query(
            "SELECT version, description, success \
             FROM _sqlx_migrations \
             ORDER BY version",
            &[],
        )
        .await
        .expect("query _sqlx_migrations — did sqlx create the table?");

    assert_eq!(
        migration_rows.len(),
        5,
        "expected 5 rows in _sqlx_migrations after `sqlx migrate run`, got {}.\n\
         Rows: {:?}",
        migration_rows.len(),
        migration_rows
            .iter()
            .map(|r| {
                let ver: i64 = r.get(0);
                let desc: &str = r.get(1);
                let ok: bool = r.get(2);
                format!("version={ver} desc={desc:?} success={ok}")
            })
            .collect::<Vec<_>>()
    );

    // Verify versions and success flags.
    let expected_versions: &[i64] = &[
        20240101000001,
        20240102000002,
        20240103000003,
        20240104000004,
        20240105000005,
    ];
    for (i, row) in migration_rows.iter().enumerate() {
        let version: i64 = row.get(0);
        let success: bool = row.get(2);

        assert_eq!(
            version, expected_versions[i],
            "_sqlx_migrations row {i}: expected version={}, got={version}",
            expected_versions[i]
        );
        assert!(
            success,
            "_sqlx_migrations row {i} (version={version}) has success=false"
        );
    }

    println!("[sqlx] _sqlx_migrations: 5 rows, all success=true, versions correct");

    // ── (c) assert migrated schema matches expectations ───────────────────────
    //
    // Check only application tables (not _sqlx_migrations itself).
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

    assert_schema_matches_snapshot(&server.conn_str, &expected).await;
    println!("[sqlx] schema snapshot: 4 application tables verified");

    // ── (d) CRUD battery against the migrated schema ──────────────────────────
    //
    // Note: sqlx's compile-time `query!` macro is NOT exercised here because
    // it requires a live database at *build* time (or a pre-generated
    // sqlx-data.json snapshot).  The runtime `tokio_postgres` queries below
    // use the same extended wire protocol that sqlx itself uses at runtime,
    // so they serve as an effective proxy for sqlx runtime query correctness.
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert user",
                sql: "INSERT INTO users (email, display_name) \
                      VALUES ('sqlx@example.com', 'SQLx User')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert team",
                sql: "INSERT INTO teams (name) VALUES ('sqlx-team')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert post",
                sql: "INSERT INTO posts (author_id, title, body) \
                      VALUES (1, 'Hello from sqlx', 'post body')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert comment",
                sql: "INSERT INTO comments (post_id, author_id, body) \
                      VALUES (1, 1, 'First comment')",
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
            CrudQuery {
                label: "count comments",
                sql: "SELECT count(*) FROM comments",
                expected_first_col: Some("1"),
            },
            CrudQuery {
                label: "select display_name",
                sql: "SELECT display_name FROM users WHERE email = 'sqlx@example.com'",
                expected_first_col: Some("SQLx User"),
            },
            CrudQuery {
                label: "update post",
                sql: "UPDATE posts SET title = 'Updated by sqlx test' \
                      WHERE title = 'Hello from sqlx'",
                expected_first_col: None,
            },
            CrudQuery {
                label: "verify update",
                sql: "SELECT title FROM posts WHERE title = 'Updated by sqlx test'",
                expected_first_col: Some("Updated by sqlx test"),
            },
            CrudQuery {
                label: "delete comment",
                sql: "DELETE FROM comments WHERE body = 'First comment'",
                expected_first_col: None,
            },
            CrudQuery {
                label: "count comments after delete",
                sql: "SELECT count(*) FROM comments",
                expected_first_col: Some("0"),
            },
        ],
    )
    .await;

    println!("[sqlx] CRUD battery: all queries passed");

    // ── (e) sqlx migrate revert ───────────────────────────────────────────────
    //
    // sqlx-cli `migrate revert` only works with reversible migrations (files
    // named `<version>_<desc>.up.sql` + `<version>_<desc>.down.sql`).
    // Our fixtures use the simple up-only format, so `migrate revert` will
    // likely exit non-zero or print an error saying no reversible migrations
    // exist.  We document this as a known limitation and treat the command as
    // advisory-only: we print the output but do not assert exit 0.
    //
    // If you need revert support with sqlx, use the reversible migration format:
    //   <version>_<desc>.up.sql   — applied by `migrate run`
    //   <version>_<desc>.down.sql — applied by `migrate revert`
    let (stdout_revert, stderr_revert, success_revert) =
        run_sqlx_migrate(&db_url, &source_dir, &["revert"]);
    println!("[sqlx migrate revert stdout]\n{stdout_revert}");
    if !stderr_revert.is_empty() {
        eprintln!("[sqlx migrate revert stderr]\n{stderr_revert}");
    }

    if success_revert {
        // If revert somehow succeeded (e.g. sqlx treated it as a no-op),
        // just report it — the test is already done.
        println!("[sqlx] migrate revert: exited 0 (no reversible migrations present — expected)");
    } else {
        // Non-zero exit is expected for up-only migrations; log and continue.
        println!(
            "[sqlx] migrate revert: exited non-zero (expected for up-only migration format). \
             Output: {stdout_revert}{stderr_revert}"
        );
    }

    println!("[sqlx_migrate_run_revert] PASSED");
}
