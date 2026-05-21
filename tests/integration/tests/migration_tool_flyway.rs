//! Phase 5.25.B — Flyway migration-tool compatibility test.
//!
//! Runs Flyway's standard `migrate` command against a live Basin in-process
//! server and verifies:
//!   (a) `flyway migrate` exits 0
//!   (b) `flyway_schema_history` exists and has exactly 10 rows with the
//!       expected version strings V1..V10.
//!   (c) The migrated schema contains every application table and column
//!       listed in the expected `SchemaSnapshot`.
//!
//! ## Graceful degradation
//!
//! Flyway is a Java-based CLI tool that may not be installed in all
//! environments.  The test guards as follows:
//!
//! - If the `flyway` binary is NOT on PATH the test is marked `#[ignore]`
//!   (see `flyway_migrate_10_migrations`) — Cargo prints it as "ignored"
//!   rather than "failed".
//! - When the binary IS present the test runs fully and panics on any
//!   assertion failure.
//!
//! ## Known Basin compatibility gaps (discovered during development)
//!
//! These gaps may cause `flyway migrate` to fail until fixed:
//!
//! 1. **`SET search_path TO …`** — Flyway issues this statement on connect.
//!    Basin's pgwire handler must accept and no-op it (or honour it).  If not,
//!    Flyway aborts with "ERROR: unrecognised SET option".
//!
//! 2. **`SELECT pg_advisory_lock(…)`** — Flyway acquires an advisory lock
//!    before writing to `flyway_schema_history`.  Basin does not implement
//!    advisory locks; it must return a no-op row rather than an error.
//!
//! 3. **`SHOW transaction_isolation`** — Flyway checks isolation level at
//!    startup.  Basin must handle this SHOW variant.
//!
//! 4. **DDL `ALTER TABLE … ADD COLUMN … DEFAULT …`** — V4/V6/V9 use inline
//!    defaults; Basin's ALTER TABLE parser must accept the full syntax.
//!
//! 5. **`CREATE TABLE … PRIMARY KEY (…, …)`** — composite PKs used in V3/V8.
//!    Basin's DDL layer must handle composite primary key constraints.
//!
//! Until gaps 1–3 are resolved `flyway migrate` is expected to fail even when
//! the binary is present.  The test captures stdout/stderr and prints them for
//! inspection, then asserts exit-0 — so the test will show exactly which error
//! Flyway encounters.
//!
//! ## Fixture location
//!
//! `tests/integration/fixtures/migration-tool-scaffold/flyway/sql/`
//! contains V1__create_users.sql … V10__create_audit_log.sql.

#![allow(clippy::print_stdout, clippy::print_stderr)]

// Pull in the shared scaffold helpers without re-declaring the module.
// Each per-tool file uses `#[path = …]` to import the shared scaffold module.
#[path = "migration_tool_common.rs"]
mod common;

use common::{
    assert_schema_matches_snapshot, run_crud_battery, spawn_basin_server_with_slug, CrudQuery,
    SchemaSnapshot,
};
use std::path::PathBuf;
use std::process::Command;
use tokio_postgres::NoTls;

// ── Helper: detect flyway ─────────────────────────────────────────────────────

/// Returns `true` when the `flyway` binary exists on PATH and can be invoked.
fn flyway_available() -> bool {
    Command::new("flyway").arg("-version").output().is_ok()
}

/// Absolute path to the flyway SQL fixtures bundled with the test suite.
fn fixture_sql_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is the crate root of `basin-integration-tests`.
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR must be set by cargo test");
    PathBuf::from(manifest)
        .join("fixtures")
        .join("migration-tool-scaffold")
        .join("flyway")
        .join("sql")
}

// ── Test ──────────────────────────────────────────────────────────────────────

/// Run 10 Flyway migrations against a live Basin instance and verify the
/// history table and final schema.
///
/// # Ignored when Flyway is absent
///
/// This test is `#[ignore]`'d by default.  To run it you need both `flyway`
/// (≥ 9.x Community Edition) and Java 17+ on PATH:
///
/// ```text
/// cargo test -p basin-integration-tests --test migration_tool_flyway \
///     -- --ignored
/// ```
///
/// In CI, install Flyway via:
///
/// ```yaml
/// - name: Install Flyway
///   run: |
///     wget -qO- https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/10.21.0/flyway-commandline-10.21.0-linux-x64.tar.gz \
///       | tar xz -C /opt
///     echo "/opt/flyway-10.21.0" >> $GITHUB_PATH
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires flyway CLI (+ Java 17+) on PATH; see comment above for CI install instructions"]
async fn flyway_migrate_10_migrations() {
    // ── Runtime guard: skip cleanly if flyway binary is absent ────────────────
    if !flyway_available() {
        eprintln!(
            "[flyway_migrate_10_migrations] SKIP — \
             `flyway` binary not found on PATH. \
             Install Flyway Community Edition (≥ 9.x) and Java 17+ to enable \
             this test. See the ignore attribute comment for CI install steps."
        );
        return;
    }

    // ── Spin up Basin in-process ───────────────────────────────────────────────
    let server = spawn_basin_server_with_slug("flyway_test").await;
    let port = server.addr.port();
    let user = &server.project_slug;

    println!(
        "[flyway] Basin in-process server at 127.0.0.1:{port}, user={user}"
    );

    // ── Locate fixture SQL directory ───────────────────────────────────────────
    let sql_dir = fixture_sql_dir();
    assert!(
        sql_dir.exists(),
        "fixture SQL directory not found: {}",
        sql_dir.display()
    );

    // ── Build Flyway JDBC URL ──────────────────────────────────────────────────
    // Flyway uses JDBC under the hood; the PostgreSQL driver URL format is:
    //   jdbc:postgresql://host:port/dbname
    let jdbc_url = format!("jdbc:postgresql://127.0.0.1:{port}/basin");

    // ── Run: flyway migrate ────────────────────────────────────────────────────
    //
    // Known compatibility gaps that may surface here (see module-level docs):
    //   - SET search_path
    //   - pg_advisory_lock
    //   - SHOW transaction_isolation
    //   - composite PRIMARY KEY in DDL
    //   - ALTER TABLE ADD COLUMN DEFAULT
    //
    // We capture output and print it regardless of exit status so CI logs show
    // the exact error when Basin rejects a statement.
    let migrate_output = Command::new("flyway")
        .arg(format!("-url={jdbc_url}"))
        .arg(format!("-user={user}"))
        .arg("-password=ignored")
        // Tell Flyway where the SQL scripts live:
        .arg(format!("-locations=filesystem:{}", sql_dir.display()))
        // Disable Flyway's own connectivity check (Basin may not handle all
        // of Flyway's startup probes — we want the error on `migrate`, not
        // on `validateOnMigrate`):
        .arg("-validateOnMigrate=false")
        // Disable color codes that clutter CI logs:
        .arg("-outputType=json")
        .arg("migrate")
        .output()
        .expect("failed to spawn flyway process");

    let stdout = String::from_utf8_lossy(&migrate_output.stdout);
    let stderr = String::from_utf8_lossy(&migrate_output.stderr);

    println!("[flyway migrate stdout]\n{stdout}");
    if !stderr.is_empty() {
        eprintln!("[flyway migrate stderr]\n{stderr}");
    }

    // ── (a) assert exit 0 ─────────────────────────────────────────────────────
    assert!(
        migrate_output.status.success(),
        "flyway migrate exited with status {:?}.\n\
         This likely means Basin rejected a SQL statement Flyway issued.\n\
         Check the stderr above for the exact error and update the module-level \
         compatibility-gap list accordingly.",
        migrate_output.status.code()
    );

    // ── (b) assert flyway_schema_history has 10 rows ──────────────────────────
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect to Basin after flyway migrate");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let history_rows = client
        .query(
            "SELECT installed_rank, version, description, success \
             FROM flyway_schema_history \
             ORDER BY installed_rank",
            &[],
        )
        .await
        .expect("query flyway_schema_history — did flyway create the table?");

    assert_eq!(
        history_rows.len(),
        10,
        "expected 10 rows in flyway_schema_history, got {}.\n\
         Rows: {:?}",
        history_rows.len(),
        history_rows
            .iter()
            .map(|r| {
                let rank: i32 = r.get(0);
                let ver: &str = r.get(1);
                format!("rank={rank} ver={ver}")
            })
            .collect::<Vec<_>>()
    );

    // Verify each row's version and success flag.
    let expected_versions = [
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
    ];
    for (i, row) in history_rows.iter().enumerate() {
        let rank: i32 = row.get(0);
        let version: &str = row.get(1);
        let success: bool = row.get(3);

        assert_eq!(
            version, expected_versions[i],
            "flyway_schema_history row {i}: expected version={}, got={version}",
            expected_versions[i]
        );
        assert!(
            success,
            "flyway_schema_history row {i} (version={version}, rank={rank}) has success=false"
        );
    }

    println!("[flyway] flyway_schema_history: 10 rows, all success=true");

    // ── (c) assert migrated schema matches expectations ────────────────────────
    //
    // We check only application tables (not flyway_schema_history itself).
    // Extra columns are permitted — this matches the lenient behavior of
    // `assert_schema_matches_snapshot`.
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
                ("description", "text"),
            ],
        )
        .add_table(
            "team_members",
            vec![
                ("team_id", "integer"),
                ("user_id", "integer"),
                ("joined_at", "timestamp without time zone"),
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
                ("updated_at", "timestamp without time zone"),
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
        )
        .add_table("tags", vec![("id", "integer"), ("name", "text")])
        .add_table(
            "post_tags",
            vec![("post_id", "integer"), ("tag_id", "integer")],
        )
        .add_table(
            "audit_log",
            vec![
                ("id", "integer"),
                ("table_name", "text"),
                ("row_id", "integer"),
                ("action", "text"),
                ("changed_at", "timestamp without time zone"),
            ],
        );

    assert_schema_matches_snapshot(&server.conn_str, &expected).await;

    println!("[flyway] schema snapshot: all 8 application tables verified");

    // ── Optional CRUD battery against the migrated schema ─────────────────────
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert user",
                sql: "INSERT INTO users (email) VALUES ('alice@example.com')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "insert team",
                sql: "INSERT INTO teams (name) VALUES ('engineering')",
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
                label: "insert post",
                sql: "INSERT INTO posts (author_id, title, body) \
                      VALUES (1, 'Hello Basin', 'First post body')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "count posts",
                sql: "SELECT count(*) FROM posts",
                expected_first_col: Some("1"),
            },
        ],
    )
    .await;

    println!("[flyway] CRUD battery: all 6 queries passed");
    println!("[flyway_migrate_10_migrations] PASSED");
}
