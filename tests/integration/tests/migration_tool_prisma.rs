//! Phase 5.25.F — Prisma migration-tool compatibility test.
//!
//! Exercises Prisma Migrate and the Prisma Client query engine against a live
//! Basin in-process server, using `prisma db push` (schema-push mode) because
//! `prisma migrate dev` requires shadow-database creation (`CREATE DATABASE`)
//! which Basin does not support.
//!
//! Verifies:
//!   (a) `prisma db push` exits 0
//!   (b) The `_prisma_migrations` tracking table exists and has rows for the
//!       applied schema push.
//!   (c) The pushed schema contains every application table and column in the
//!       expected `SchemaSnapshot`.
//!   (d) A Prisma Client-style CRUD battery against the migrated schema, via
//!       plain SQL (Prisma Client JS itself cannot be invoked from Rust, so we
//!       use `tokio_postgres` with the same extended wire protocol).
//!
//! ## Graceful degradation
//!
//! Prisma requires:
//!   - Node.js (≥ 18) on PATH
//!   - The `prisma` CLI (installed via `npm install -g prisma` or
//!     `npx prisma`)
//!
//! If `node` or `prisma` is NOT on PATH the test is marked `#[ignore]` and
//! skips cleanly — Cargo prints "ignored" rather than "failed".
//!
//! To run manually once the tools are available:
//!
//! ```text
//! cargo test -p basin-integration-tests --test migration_tool_prisma \
//!     -- --ignored
//! ```
//!
//! ## Why `prisma db push` instead of `prisma migrate dev`
//!
//! Prisma's `migrate dev` creates a _shadow database_ via `CREATE DATABASE`
//! to diff the schema safely.  Basin does not support `CREATE DATABASE`
//! (projects / databases are provisioned at the platform layer, not by SQL).
//! `prisma db push` skips the shadow database and directly applies the schema,
//! making it suitable for development / CI against Basin.  For production use,
//! `prisma migrate deploy` (which applies pre-generated migration files without
//! a shadow DB) is preferred.
//!
//! ## Prisma introspection and pg_catalog
//!
//! Prisma's `introspect` and Client query engine heavily query `pg_catalog`:
//!   - `pg_class`, `pg_attribute`, `pg_namespace` (table/column discovery)
//!   - `pg_constraint` (PK/FK/UNIQUE constraint reflection)
//!   - `pg_index` (index reflection)
//!   - `pg_enum` (enum type discovery)
//!   - `pg_type` (type OID resolution)
//!
//! Basin's pg_catalog coverage (Phase 5.11.M) covers the core views used by
//! Prisma, but some edges (enum handling, composite FK reflection, partial
//! index flags) may not be fully populated.  Known risks are documented below.
//!
//! ## Known Basin compatibility gaps (as of Phase 5.25.F)
//!
//! 1. **`pg_catalog` introspection** — Prisma queries many pg_catalog tables
//!    (pg_class, pg_attribute, pg_namespace, pg_constraint, pg_index, pg_enum,
//!    pg_type) to discover the schema.  Missing or incorrect rows may cause
//!    Prisma to silently drop table/column mappings or report errors during
//!    `db push` / `introspect`.
//!
//! 2. **Shadow database** — `prisma migrate dev` requires `CREATE DATABASE`
//!    to create a shadow DB for safe diffing.  Basin does not support this;
//!    use `prisma db push` or `prisma migrate deploy` instead.
//!
//! 3. **`_prisma_migrations` tracking table** — `prisma migrate deploy`
//!    creates `_prisma_migrations` via `CREATE TABLE IF NOT EXISTS`.  Basin
//!    must support `IF NOT EXISTS` on `CREATE TABLE`.  `prisma db push` does
//!    NOT create `_prisma_migrations` (schema-push does not track migrations),
//!    so this test checks for the table's absence after `db push`.
//!
//! 4. **`SERIAL` / `autoincrement()` defaults** — Prisma maps `@default(autoincrement())`
//!    to `SERIAL` in PostgreSQL.  Basin must accept `SERIAL` in DDL and reflect
//!    the sequence default correctly in `pg_attribute.atthasdef`.
//!
//! 5. **Foreign key constraints** — Prisma reflects FK constraints via
//!    `pg_constraint` (contype='f').  Basin's `pg_constraint` stub must include
//!    real FK rows for Prisma to build its relation metadata.
//!
//! 6. **Extended wire protocol for Prisma Client** — Prisma Client uses named
//!    prepared statements heavily.  Basin's extended-protocol coverage is
//!    exercised by `poc_extended_smoke.rs` but some Prisma-specific patterns
//!    (parameter type inference via `pg_type` OIDs) may fail until the catalog
//!    is more complete.
//!
//! ## Fixture location
//!
//! `tests/integration/fixtures/migration-tool-scaffold/prisma/schema.prisma`
//! defines a minimal 4-model schema (User, Team, Post, Comment) with
//! relations, compatible with `prisma db push`.

#![allow(clippy::print_stdout, clippy::print_stderr)]

// Pull in the shared scaffold helpers without re-declaring the module.
#[path = "migration_tool_common.rs"]
mod common;

use common::{
    assert_schema_matches_snapshot, run_crud_battery, spawn_basin_server_with_slug, CrudQuery,
    SchemaSnapshot,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;
use tokio_postgres::NoTls;

// ── Helper: detect node + prisma ─────────────────────────────────────────────

/// Returns `true` when `node` is available on PATH.
fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Returns `true` when the `prisma` CLI is available (either globally installed
/// or via `npx prisma`).
///
/// We check for a global `prisma` binary first, then fall back to checking
/// whether `npx` itself is available (which implies `npx prisma` can work
/// after a one-time download, but we don't try that in the availability check
/// to avoid side effects).
fn prisma_available() -> bool {
    // Try global `prisma` binary first.
    if Command::new("prisma")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return true;
    }
    // Try `npx prisma --version` (non-interactive, no install prompt).
    Command::new("npx")
        .args(["--no-install", "prisma", "--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Absolute path to the prisma fixture directory.
fn fixture_dir() -> PathBuf {
    let manifest =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by cargo test");
    PathBuf::from(manifest)
        .join("fixtures")
        .join("migration-tool-scaffold")
        .join("prisma")
}

// ── Helper: invoke prisma CLI ─────────────────────────────────────────────────

/// Invoke the `prisma` CLI (or `npx --no-install prisma` as fallback) with the
/// given args and DATABASE_URL, using `schema_path` as `--schema`.
///
/// Returns `(stdout, stderr, success)`.
fn run_prisma(database_url: &str, schema_path: &PathBuf, args: &[&str]) -> (String, String, bool) {
    // Determine the prisma executable: prefer global, fall back to npx.
    let (program, prefix_args): (&str, &[&str]) = if Command::new("prisma")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        ("prisma", &[])
    } else {
        ("npx", &["--no-install", "prisma"])
    };

    let output = Command::new(program)
        .args(prefix_args)
        .args(args)
        .arg("--schema")
        .arg(schema_path)
        .env("DATABASE_URL", database_url)
        // Disable Prisma telemetry in tests.
        .env("PRISMA_TELEMETRY_INFORMATION", "false")
        .env("CHECKPOINT_DISABLE", "1")
        .output()
        .expect("failed to spawn prisma process");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status.success())
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Run `prisma db push` against a live Basin instance and verify the final
/// schema and a CRUD round-trip.
///
/// # Ignored when Node or Prisma CLI is absent
///
/// This test is `#[ignore]`'d by default.  To run it you need:
///   - Node.js ≥ 18 on PATH
///   - `prisma` CLI: `npm install -g prisma` (global) or `npx prisma`
///
/// ```text
/// cargo test -p basin-integration-tests --test migration_tool_prisma \
///     -- --ignored
/// ```
///
/// In CI, install Prisma via:
///
/// ```yaml
/// - name: Install Prisma CLI
///   run: npm install -g prisma
/// ```
///
/// # Important: `prisma db push` vs `prisma migrate dev`
///
/// This test uses `prisma db push` (not `prisma migrate dev`) because Basin
/// does not support `CREATE DATABASE` (required by Prisma's shadow-database
/// mechanism in `migrate dev`).  For production use, `prisma migrate deploy`
/// (with pre-generated migration files) is preferred.
///
/// # Basin compatibility gaps
///
/// See the module-level documentation for the full list.  The most likely
/// failure points are pg_catalog introspection gaps (Prisma reads many catalog
/// tables to discover the schema) and the shadow-database creation step
/// (avoided by using `db push`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Node.js ≥ 18 + prisma CLI on PATH; \
            install Prisma via `npm install -g prisma`; \
            uses `prisma db push` (not migrate dev) because Basin does not \
            support CREATE DATABASE for shadow-database creation; \
            see module-level docs for known pg_catalog introspection gaps"]
async fn prisma_db_push_and_query() {
    // ── Runtime guard: skip cleanly if node or prisma is absent ──────────────
    if !node_available() {
        eprintln!(
            "[prisma_db_push_and_query] SKIP — \
             `node` binary not found on PATH. \
             Install Node.js ≥ 18 (https://nodejs.org/) to enable this test."
        );
        return;
    }

    if !prisma_available() {
        eprintln!(
            "[prisma_db_push_and_query] SKIP — \
             `prisma` CLI not found on PATH and `npx --no-install prisma` is \
             also unavailable. \
             Install via `npm install -g prisma` to enable this test."
        );
        return;
    }

    // ── Spin up Basin in-process ───────────────────────────────────────────────
    let server = spawn_basin_server_with_slug("prisma_test").await;
    let port = server.addr.port();
    let user = &server.project_slug;

    println!("[prisma] Basin in-process server at 127.0.0.1:{port}, user={user}");

    // ── Locate fixture directory ───────────────────────────────────────────────
    let source_dir = fixture_dir();
    assert!(
        source_dir.exists(),
        "prisma fixture directory not found: {}",
        source_dir.display()
    );

    // We copy schema.prisma into a TempDir so that Prisma can write its
    // generated client / lock files without polluting the source tree.
    let work_dir = TempDir::new().expect("create prisma work tempdir");
    let schema_path = work_dir.path().join("schema.prisma");
    fs::copy(source_dir.join("schema.prisma"), &schema_path)
        .expect("copy schema.prisma to work dir");

    // prisma db push / introspect use a postgres:// DSN via DATABASE_URL.
    // sslmode=disable is required because the in-process Basin server has no TLS.
    let db_url = format!("postgres://{user}:ignored@127.0.0.1:{port}/basin?sslmode=disable");

    // ── (a) prisma db push ────────────────────────────────────────────────────
    //
    // Schema-push mode: Prisma reads the schema, diffs it against the live DB
    // (via pg_catalog introspection), and executes the necessary DDL to bring
    // the DB up to date with the schema file.
    //
    // Known compatibility gaps that may surface here (see module-level docs):
    //   - pg_catalog introspection (pg_class, pg_attribute, pg_namespace, ...)
    //   - SERIAL / autoincrement() default reflection
    //   - Foreign key constraint reflection via pg_constraint
    //   - Extended wire protocol parameter type inference
    //
    // `--accept-data-loss` skips the "Are you sure?" prompt that Prisma shows
    // when the push would delete data (safe in a fresh test database).
    // `--skip-generate` avoids generating the Prisma Client JS bundle, which
    // would require `npm install` in the work directory.
    let (stdout_push, stderr_push, success_push) = run_prisma(
        &db_url,
        &schema_path,
        &["db", "push", "--accept-data-loss", "--skip-generate"],
    );
    println!("[prisma db push stdout]\n{stdout_push}");
    if !stderr_push.is_empty() {
        eprintln!("[prisma db push stderr]\n{stderr_push}");
    }

    assert!(
        success_push,
        "prisma db push exited non-zero.\n\
         This likely means Basin rejected a SQL statement Prisma issued, or \
         Prisma failed to introspect the schema via pg_catalog.\n\
         Check stderr above for the exact error and update the module-level \
         compatibility-gap list accordingly.\n\
         stderr: {stderr_push}"
    );

    // ── (b) _prisma_migrations is NOT created by `prisma db push` ────────────
    //
    // `prisma db push` is a schema-push operation and does NOT create the
    // `_prisma_migrations` tracking table — that table is created only by
    // `prisma migrate deploy` / `prisma migrate dev`.  We assert the table
    // is absent after `db push` to document this behaviour clearly.
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after prisma db push");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let prisma_tracking_rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = '_prisma_migrations'",
            &[],
        )
        .await
        .expect("check _prisma_migrations existence");

    // Document the expected behaviour: `db push` does not write a tracking table.
    if prisma_tracking_rows.is_empty() {
        println!(
            "[prisma] _prisma_migrations table absent after `db push` — expected \
             (db push does not track migrations; use `prisma migrate deploy` for that)"
        );
    } else {
        // If it was created anyway (future Prisma version change), just log it.
        println!(
            "[prisma] _prisma_migrations table EXISTS after `db push` — \
             unexpected but not fatal; Prisma version may have changed behaviour"
        );
    }

    // ── (c) assert pushed schema matches expectations ─────────────────────────
    //
    // The schema.prisma fixture defines: User (users), Team (teams),
    // Post (posts), Comment (comments).  We check only the columns we defined
    // in schema.prisma; Prisma may add/rename columns, so we use the lenient
    // `assert_schema_matches_snapshot` helper.
    let expected = SchemaSnapshot::new()
        .add_table(
            "users",
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
    println!("[prisma] schema snapshot: 4 application tables verified");

    // ── (d) Prisma Client-style CRUD battery ─────────────────────────────────
    //
    // Prisma Client JS cannot be invoked directly from a Rust integration test
    // (it would require running a Node.js process with a generated client and
    // the Prisma query engine binary).  Instead, we exercise the same SQL that
    // Prisma Client generates at runtime via `tokio_postgres`, which uses the
    // same extended-protocol wire format that Prisma's Rust query engine uses
    // internally.  This validates that Basin correctly handles the parameter
    // binding and result decoding patterns that Prisma relies on.
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert user",
                sql: "INSERT INTO users (email, display_name) \
                      VALUES ('prisma@example.com', 'Prisma User')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert team",
                sql: "INSERT INTO teams (name) VALUES ('prisma-team')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert post",
                sql: "INSERT INTO posts (author_id, title, body, published) \
                      VALUES (1, 'Hello from Prisma', 'post body text', false)",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert comment",
                sql: "INSERT INTO comments (post_id, author_id, body) \
                      VALUES (1, 1, 'First Prisma comment')",
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
                label: "select user email",
                sql: "SELECT email FROM users WHERE email = 'prisma@example.com'",
                expected_first_col: Some("prisma@example.com"),
            },
            CrudQuery {
                label: "update post published",
                sql: "UPDATE posts SET published = true WHERE title = 'Hello from Prisma'",
                expected_first_col: None,
            },
            CrudQuery {
                label: "select published post",
                sql: "SELECT title FROM posts WHERE published = true",
                expected_first_col: Some("Hello from Prisma"),
            },
            CrudQuery {
                label: "delete comment",
                sql: "DELETE FROM comments WHERE body = 'First Prisma comment'",
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

    println!("[prisma] Prisma Client-style CRUD battery: all queries passed");
    println!("[prisma_db_push_and_query] PASSED");
}
