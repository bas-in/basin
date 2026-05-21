//! Phase 5.22.A — pg_dump compat test harness.
//!
//! **Task:** TASK.md Phase 5.22.A — pg_dump / pg_restore compatibility and
//! cross-PG migration test harness.
//! **Test-first convention:** this file lands BEFORE any dump/restore
//! implementation. Every test is `#[ignore]`d until the implementation tasks
//! 5.22.B-E close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                                  | Slice                        | Closed by |
//! |------------------------------------------|------------------------------|-----------|
//! | `pg_dump_basin_to_basin_round_trip`       | Basin→Basin round-trip       | 5.22.B    |
//! | `pg_dump_basin_to_real_pg_via_pg_restore` | pg_restore compat            | 5.22.C    |
//! | `pg_dump_real_pg_to_basin_migration`      | real-PG→Basin migration      | 5.22.D    |
//! | `pg_dump_orm_load_post_restore`           | ORM-load post-restore        | 5.22.E    |
//!
//! ## Environment variables
//!
//! | Variable         | Required by                              | Example                                |
//! |------------------|------------------------------------------|----------------------------------------|
//! | `BASIN_LOCAL_URL`| slices 1, 4 (Basin round-trip + ORM)    | `postgres://user:pw@127.0.0.1:5433/db` |
//! | `REAL_PG_URL`    | slices 2, 3 (cross-PG migration)        | `postgres://postgres:pw@127.0.0.1:5432/postgres` |
//!
//! Tests skip cleanly (exit 0, printed skip notice) when the required env var
//! is absent. This keeps the default `cargo test` run at 0/0/4 (skipped).
//!
//! ## How to flip a slice green
//!
//! Once 5.22.B/C/D/E land, drop the `#[ignore]` on the relevant slice (or
//! replace it with a targeted `#[ignore = "gated on #NNN"]` if only part of
//! the slice is resolved), set the required env vars, and confirm
//! `cargo test --test pg_dump_harness` passes without `--ignored`.
//!
//! ## CLI tools required
//!
//! The harness shells out to:
//! - `basin-cli` — Basin's pg_dump-compatible dump/restore CLI
//! - `pg_restore` — standard PostgreSQL client tool (for slice 2)
//! - `psql` — standard PostgreSQL client tool (for slices 2, 3)
//!
//! All tools are expected on `$PATH`. Missing tools produce a clear panic
//! message identifying what is needed.

#![allow(clippy::print_stdout)]

use std::process::Command;

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Return the value of `var` or print a skip notice and return `None`.
/// Callers use `let Some(url) = require_env("VAR") else { return; }`.
fn require_env(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => {
            println!(
                "[pg_dump_harness] SKIP — env var {var} not set. \
                 Set it to a live database URL to run this slice."
            );
            None
        }
    }
}

/// Run a shell command, return stdout as a String.
/// Panics with a descriptive message on non-zero exit.
fn run_cmd(program: &str, args: &[&str]) -> String {
    let out = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "[pg_dump_harness] failed to spawn `{program}`: {e}\n\
                 Ensure `{program}` is on $PATH."
            )
        });

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

    if !out.status.success() {
        panic!(
            "[pg_dump_harness] `{program} {}` exited with status {}.\n\
             stdout:\n{stdout}\nstderr:\n{stderr}",
            args.join(" "),
            out.status
        );
    }

    stdout
}

/// Run a shell command, piping `stdin_data` to the process's stdin.
/// Panics on non-zero exit with full stdout/stderr context.
fn run_cmd_with_stdin(program: &str, args: &[&str], stdin_data: &[u8]) -> String {
    use std::io::Write;

    let mut child = Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "[pg_dump_harness] failed to spawn `{program}`: {e}\n\
                 Ensure `{program}` is on $PATH."
            )
        });

    child
        .stdin
        .as_mut()
        .expect("child stdin is piped")
        .write_all(stdin_data)
        .expect("write stdin");

    let out = child.wait_with_output().expect("wait_with_output");

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

    if !out.status.success() {
        panic!(
            "[pg_dump_harness] `{program} {}` exited with status {}.\n\
             stdout:\n{stdout}\nstderr:\n{stderr}",
            args.join(" "),
            out.status
        );
    }

    stdout
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — Basin→Basin round-trip
// ─────────────────────────────────────────────────────────────────────────────
//
// Full cycle:
//   1. Connect to local Basin via BASIN_LOCAL_URL.
//   2. Create a canonical schema + seed rows covering all supported types
//      (INT, TEXT, BOOLEAN, TIMESTAMPTZ, JSONB, NUMERIC).
//   3. Run `basin-cli dump --url $BASIN_LOCAL_URL > out.sql` (pg_dump-format
//      output; the dump is captured to a temp file).
//   4. DROP the tables (or use a fresh schema in a second connection).
//   5. Run `basin-cli restore --url $BASIN_LOCAL_URL < out.sql` to replay the
//      dump into the same database.
//   6. Re-query the restored tables and assert the row counts and a sample
//      data point match the originals.
//
// Closed by: 5.22.B (basin-cli dump + restore implementation).
//
// Required env: BASIN_LOCAL_URL — a live local Basin instance.

/// Phase 5.22.A — Basin→Basin round-trip: create schema + data, dump via
/// `basin-cli dump`, drop, restore via `basin-cli restore`, diff.
///
/// Slice: "Basin→Basin round-trip".
///
/// Closes when: 5.22.B lands and `basin-cli dump`/`restore` are implemented.
/// At that point drop this `#[ignore]` and re-run
/// `cargo test --test pg_dump_harness` to confirm green.
#[ignore = "5.22.A harness — basin-cli dump/restore + cross-PG migration pending; closes 5.22.B-E"]
#[test]
fn pg_dump_basin_to_basin_round_trip() {
    // Slice: "Basin→Basin round-trip"
    //
    // IMPORTANT: every assertion below reflects the *correct semantics*: what
    // a lossless dump+restore MUST preserve. Do not weaken assertions to match
    // a broken restore — fix the restore in 5.22.B instead.

    let Some(basin_url) = require_env("BASIN_LOCAL_URL") else {
        return;
    };

    println!("[5.22.A round-trip] BASIN_LOCAL_URL={basin_url}");

    // ── Step 1: create canonical schema + seed ────────────────────────────
    // We drive Basin via psql (which speaks pgwire) for setup/teardown.
    // `basin-cli` is only used for dump/restore.

    let setup_sql = "\
        DROP TABLE IF EXISTS dump_rt_orders CASCADE;\
        DROP TABLE IF EXISTS dump_rt_users  CASCADE;\
        CREATE TABLE dump_rt_users (\
            id         BIGINT      PRIMARY KEY,\
            username   TEXT        NOT NULL,\
            active     BOOLEAN     NOT NULL DEFAULT TRUE,\
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\
            metadata   JSONB\
        );\
        CREATE TABLE dump_rt_orders (\
            id         BIGINT      PRIMARY KEY,\
            user_id    BIGINT      NOT NULL REFERENCES dump_rt_users(id),\
            amount     NUMERIC(12,2) NOT NULL,\
            note       TEXT\
        );\
        INSERT INTO dump_rt_users (id, username, active, metadata) VALUES\
            (1, 'alice', TRUE,  '{\"tier\":\"gold\",\"region\":\"us-east\"}'),\
            (2, 'bob',   FALSE, NULL),\
            (3, 'carol', TRUE,  '{\"tier\":\"silver\"}');\
        INSERT INTO dump_rt_orders (id, user_id, amount, note) VALUES\
            (101, 1, 99.99,  'first order'),\
            (102, 1, 249.50, 'second order'),\
            (103, 3, 15.00,  NULL);";

    run_cmd("psql", &[&basin_url, "--no-psqlrc", "-c", setup_sql]);
    println!("[5.22.A round-trip] schema + seed applied");

    // ── Step 2: dump via basin-cli ────────────────────────────────────────
    let dump_sql = run_cmd(
        "basin-cli",
        &["dump", "--url", &basin_url, "--table", "dump_rt_users", "--table", "dump_rt_orders"],
    );
    println!(
        "[5.22.A round-trip] dump captured ({} bytes)",
        dump_sql.len()
    );

    assert!(
        !dump_sql.is_empty(),
        "ROUND-TRIP: basin-cli dump produced empty output — closed by 5.22.B"
    );

    // Sanity: the dump should contain the table names and at least one INSERT
    // or COPY statement.
    assert!(
        dump_sql.contains("dump_rt_users"),
        "ROUND-TRIP: dump output must reference dump_rt_users — closed by 5.22.B"
    );
    assert!(
        dump_sql.contains("dump_rt_orders"),
        "ROUND-TRIP: dump output must reference dump_rt_orders — closed by 5.22.B"
    );
    assert!(
        dump_sql.to_uppercase().contains("INSERT") || dump_sql.to_uppercase().contains("COPY"),
        "ROUND-TRIP: dump output must contain data (INSERT or COPY) — closed by 5.22.B"
    );

    // ── Step 3: drop tables ───────────────────────────────────────────────
    let drop_sql = "\
        DROP TABLE IF EXISTS dump_rt_orders CASCADE;\
        DROP TABLE IF EXISTS dump_rt_users  CASCADE;";
    run_cmd("psql", &[&basin_url, "--no-psqlrc", "-c", drop_sql]);
    println!("[5.22.A round-trip] tables dropped");

    // ── Step 4: restore via basin-cli ─────────────────────────────────────
    run_cmd_with_stdin(
        "basin-cli",
        &["restore", "--url", &basin_url],
        dump_sql.as_bytes(),
    );
    println!("[5.22.A round-trip] restore complete");

    // ── Step 5: diff — verify row counts and a sample data point ─────────
    let user_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM dump_rt_users",
        ],
    );
    let user_count: u64 = user_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "ROUND-TRIP: could not parse user count from psql output: {user_count_raw:?}"
        )
    });
    assert_eq!(
        user_count, 3,
        "ROUND-TRIP: dump_rt_users must have 3 rows after restore — closed by 5.22.B. \
         got={user_count}"
    );

    let order_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM dump_rt_orders",
        ],
    );
    let order_count: u64 = order_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "ROUND-TRIP: could not parse order count from psql output: {order_count_raw:?}"
        )
    });
    assert_eq!(
        order_count, 3,
        "ROUND-TRIP: dump_rt_orders must have 3 rows after restore — closed by 5.22.B. \
         got={order_count}"
    );

    // Sample data point: alice's metadata JSON must survive the round-trip.
    let alice_meta_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT metadata->>'tier' FROM dump_rt_users WHERE id = 1",
        ],
    );
    let alice_tier = alice_meta_raw.trim();
    assert_eq!(
        alice_tier, "gold",
        "ROUND-TRIP: alice's metadata tier must be 'gold' after restore — closed by 5.22.B. \
         got={alice_tier:?}"
    );

    println!(
        "[5.22.A round-trip] PASSED — users={user_count}, orders={order_count}, \
         alice.tier={alice_tier}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — pg_restore compat (Basin dump → real PostgreSQL)
// ─────────────────────────────────────────────────────────────────────────────
//
// Full cycle:
//   1. Seed Basin (BASIN_LOCAL_URL) with a canonical schema + data.
//   2. Dump from Basin using `basin-cli dump --format=custom` to produce a
//      pg_dump-compatible custom-format archive.
//   3. Feed the archive into `pg_restore` targeting a real PG instance
//      (REAL_PG_URL), verifying pg_restore exits 0 and the schema + a sample
//      row appear in real PG.
//
// Closed by: 5.22.C (basin-cli dump --format=custom compat with pg_restore).
//
// Required env:
//   BASIN_LOCAL_URL — live Basin instance (source).
//   REAL_PG_URL     — live PostgreSQL instance (destination).

/// Phase 5.22.A — pg_restore compat: dump Basin schema → restore into real PG
/// via `pg_restore`, assert schema + sample data match.
///
/// Slice: "pg_restore compat".
///
/// Closes when: 5.22.C lands and `basin-cli dump --format=custom` emits a
/// pg_restore-compatible archive. At that point drop this `#[ignore]`.
#[ignore = "5.22.A harness — basin-cli dump/restore + cross-PG migration pending; closes 5.22.B-E"]
#[test]
fn pg_dump_basin_to_real_pg_via_pg_restore() {
    // Slice: "pg_restore compat"
    //
    // Both env vars are required; missing either skips cleanly.

    let Some(basin_url) = require_env("BASIN_LOCAL_URL") else {
        return;
    };
    let Some(real_pg_url) = require_env("REAL_PG_URL") else {
        return;
    };

    println!(
        "[5.22.A pg_restore] BASIN_LOCAL_URL={basin_url}  REAL_PG_URL={real_pg_url}"
    );

    // ── Step 1: seed Basin ────────────────────────────────────────────────
    let setup_sql = "\
        DROP TABLE IF EXISTS pgr_compat_items CASCADE;\
        CREATE TABLE pgr_compat_items (\
            id          BIGINT PRIMARY KEY,\
            sku         TEXT          NOT NULL,\
            price_cents INTEGER       NOT NULL,\
            tags        JSONB,\
            available   BOOLEAN       NOT NULL DEFAULT TRUE\
        );\
        INSERT INTO pgr_compat_items (id, sku, price_cents, tags) VALUES\
            (1, 'SKU-001', 1999, '[\"sale\",\"featured\"]'),\
            (2, 'SKU-002', 4999, '[\"new\"]'),\
            (3, 'SKU-003', 299,  NULL);";

    run_cmd("psql", &[&basin_url, "--no-psqlrc", "-c", setup_sql]);
    println!("[5.22.A pg_restore] Basin seeded");

    // ── Step 2: dump from Basin in custom format ──────────────────────────
    // `basin-cli dump --format=custom` must emit a pg_restore-compatible
    // custom archive. The output is binary; we write it to a temp file.
    let tmp_dir = tempfile::TempDir::new().expect("create tempdir");
    let dump_path = tmp_dir.path().join("basin_dump.pgdump");
    let dump_path_str = dump_path.to_str().expect("dump path is valid UTF-8");

    run_cmd(
        "basin-cli",
        &[
            "dump",
            "--url",
            &basin_url,
            "--format=custom",
            "--table",
            "pgr_compat_items",
            "--file",
            dump_path_str,
        ],
    );

    assert!(
        dump_path.exists(),
        "PG_RESTORE COMPAT: basin-cli dump must create the output file at {dump_path_str} \
         — closed by 5.22.C"
    );
    let dump_size = std::fs::metadata(&dump_path)
        .expect("stat dump file")
        .len();
    assert!(
        dump_size > 0,
        "PG_RESTORE COMPAT: dump file must be non-empty — closed by 5.22.C. size={dump_size}"
    );
    println!("[5.22.A pg_restore] dump written ({dump_size} bytes) → {dump_path_str}");

    // ── Step 3: clean target table in real PG ────────────────────────────
    run_cmd(
        "psql",
        &[
            &real_pg_url,
            "--no-psqlrc",
            "-c",
            "DROP TABLE IF EXISTS pgr_compat_items CASCADE",
        ],
    );

    // ── Step 4: pg_restore into real PG ──────────────────────────────────
    // `pg_restore -d $REAL_PG_URL $dump_path_str`
    run_cmd(
        "pg_restore",
        &["--dbname", &real_pg_url, "--no-owner", "--no-privileges", dump_path_str],
    );
    println!("[5.22.A pg_restore] pg_restore completed");

    // ── Step 5: verify schema + data in real PG ───────────────────────────
    let item_count_raw = run_cmd(
        "psql",
        &[
            &real_pg_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM pgr_compat_items",
        ],
    );
    let item_count: u64 = item_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "PG_RESTORE COMPAT: could not parse item count: {item_count_raw:?}"
        )
    });
    assert_eq!(
        item_count, 3,
        "PG_RESTORE COMPAT: real PG must have 3 restored rows — closed by 5.22.C. \
         got={item_count}"
    );

    // Sample data: SKU-001 must have price_cents = 1999.
    let price_raw = run_cmd(
        "psql",
        &[
            &real_pg_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT price_cents FROM pgr_compat_items WHERE sku = 'SKU-001'",
        ],
    );
    let price: i64 = price_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "PG_RESTORE COMPAT: could not parse price_cents: {price_raw:?}"
        )
    });
    assert_eq!(
        price, 1999,
        "PG_RESTORE COMPAT: SKU-001 price_cents must be 1999 after pg_restore — closed by 5.22.C. \
         got={price}"
    );

    println!(
        "[5.22.A pg_restore] PASSED — items={item_count}, SKU-001.price_cents={price}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — real-PG→Basin migration
// ─────────────────────────────────────────────────────────────────────────────
//
// Full cycle:
//   1. In a real PG instance (REAL_PG_URL): create a migration-representative
//      schema (sequences, FK constraints, ENUMs, CHECK constraints, partial
//      indexes, generated columns) + seed rows.
//   2. Run `pg_dump --format=plain $REAL_PG_URL` to get standard SQL.
//   3. Feed the dump into `basin-cli restore --url $BASIN_LOCAL_URL`.
//   4. Verify Basin accepted all DDL and the seeded rows are queryable.
//
// This tests the "migrate from real PG to Basin" customer workflow.
//
// Closed by: 5.22.D (basin-cli restore can consume real pg_dump SQL output).
//
// Required env:
//   REAL_PG_URL     — live PostgreSQL instance (source).
//   BASIN_LOCAL_URL — live Basin instance (destination).

/// Phase 5.22.A — real-PG→Basin migration: `pg_dump` a real PG schema and
/// pipe it through `basin-cli restore`, assert Basin accepts the DDL + data.
///
/// Slice: "real-PG→Basin migration".
///
/// Closes when: 5.22.D lands and Basin's restore path can ingest pg_dump
/// plain-text output including ENUMs, CHECK constraints, and partial indexes.
/// At that point drop this `#[ignore]`.
#[ignore = "5.22.A harness — basin-cli dump/restore + cross-PG migration pending; closes 5.22.B-E"]
#[test]
fn pg_dump_real_pg_to_basin_migration() {
    // Slice: "real-PG→Basin migration"

    let Some(real_pg_url) = require_env("REAL_PG_URL") else {
        return;
    };
    let Some(basin_url) = require_env("BASIN_LOCAL_URL") else {
        return;
    };

    println!(
        "[5.22.A migration] REAL_PG_URL={real_pg_url}  BASIN_LOCAL_URL={basin_url}"
    );

    // ── Step 1: seed real PG with a migration-representative schema ───────
    // Schema covers: ENUM type, sequences (via BIGSERIAL), FK, CHECK,
    // partial index, TEXT[] array column.
    let setup_sql = "\
        DROP TABLE IF EXISTS migr_line_items CASCADE;\
        DROP TABLE IF EXISTS migr_invoices   CASCADE;\
        DROP TYPE  IF EXISTS invoice_status  CASCADE;\
        CREATE TYPE invoice_status AS ENUM ('draft', 'sent', 'paid', 'void');\
        CREATE TABLE migr_invoices (\
            id          BIGSERIAL        PRIMARY KEY,\
            status      invoice_status   NOT NULL DEFAULT 'draft',\
            total_cents INTEGER          NOT NULL CHECK (total_cents >= 0),\
            tags        TEXT[]           NOT NULL DEFAULT '{}',\
            issued_at   TIMESTAMPTZ\
        );\
        CREATE TABLE migr_line_items (\
            id          BIGSERIAL PRIMARY KEY,\
            invoice_id  BIGINT    NOT NULL REFERENCES migr_invoices(id) ON DELETE CASCADE,\
            description TEXT      NOT NULL,\
            amount      NUMERIC(10,2) NOT NULL\
        );\
        CREATE INDEX migr_invoices_paid_idx\
            ON migr_invoices (issued_at)\
            WHERE status = 'paid';\
        INSERT INTO migr_invoices (status, total_cents, tags, issued_at) VALUES\
            ('paid',  5000, '{\"vip\",\"annual\"}', NOW() - INTERVAL '10 days'),\
            ('draft', 0,    '{}',                  NULL),\
            ('sent',  1200, '{\"monthly\"}',        NOW());\
        INSERT INTO migr_line_items (invoice_id, description, amount) VALUES\
            (1, 'Annual subscription', 50.00),\
            (3, 'Setup fee',           12.00);";

    run_cmd("psql", &[&real_pg_url, "--no-psqlrc", "-c", setup_sql]);
    println!("[5.22.A migration] real PG seeded");

    // ── Step 2: pg_dump from real PG (plain text) ─────────────────────────
    let dump_sql = run_cmd(
        "pg_dump",
        &[
            "--dbname",
            &real_pg_url,
            "--format=plain",
            "--no-owner",
            "--no-privileges",
            "--table",
            "migr_invoices",
            "--table",
            "migr_line_items",
        ],
    );

    assert!(
        !dump_sql.is_empty(),
        "MIGRATION: pg_dump produced empty output — unexpected, check real PG connection"
    );
    println!(
        "[5.22.A migration] pg_dump captured ({} bytes)",
        dump_sql.len()
    );

    // Verify dump contains expected DDL landmarks.
    assert!(
        dump_sql.contains("migr_invoices"),
        "MIGRATION: dump must contain migr_invoices DDL"
    );
    assert!(
        dump_sql.contains("invoice_status"),
        "MIGRATION: dump must contain the invoice_status ENUM"
    );

    // ── Step 3: clean Basin target tables ────────────────────────────────
    let drop_sql = "\
        DROP TABLE IF EXISTS migr_line_items CASCADE;\
        DROP TABLE IF EXISTS migr_invoices   CASCADE;\
        DROP TYPE  IF EXISTS invoice_status  CASCADE;";
    run_cmd("psql", &[&basin_url, "--no-psqlrc", "-c", drop_sql]);

    // ── Step 4: basin-cli restore ─────────────────────────────────────────
    run_cmd_with_stdin(
        "basin-cli",
        &["restore", "--url", &basin_url],
        dump_sql.as_bytes(),
    );
    println!("[5.22.A migration] basin-cli restore completed");

    // ── Step 5: verify Basin accepted the DDL + data ───────────────────────
    let inv_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM migr_invoices",
        ],
    );
    let inv_count: u64 = inv_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "MIGRATION: could not parse invoice count from Basin: {inv_count_raw:?}"
        )
    });
    assert_eq!(
        inv_count, 3,
        "MIGRATION: Basin must have 3 invoices after restore — closed by 5.22.D. \
         got={inv_count}"
    );

    let line_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM migr_line_items",
        ],
    );
    let line_count: u64 = line_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "MIGRATION: could not parse line_item count from Basin: {line_count_raw:?}"
        )
    });
    assert_eq!(
        line_count, 2,
        "MIGRATION: Basin must have 2 line_items after restore — closed by 5.22.D. \
         got={line_count}"
    );

    // Verify ENUM values survive: 'paid' invoice must be queryable.
    let paid_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM migr_invoices WHERE status = 'paid'",
        ],
    );
    let paid_count: u64 = paid_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!(
            "MIGRATION: could not parse paid invoice count: {paid_count_raw:?}"
        )
    });
    assert_eq!(
        paid_count, 1,
        "MIGRATION: Basin must recognise the invoice_status ENUM and filter \
         status='paid' correctly — closed by 5.22.D. got={paid_count}"
    );

    println!(
        "[5.22.A migration] PASSED — invoices={inv_count}, line_items={line_count}, \
         paid_invoices={paid_count}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — ORM-load post-restore
// ─────────────────────────────────────────────────────────────────────────────
//
// After completing the Basin→Basin round-trip (slice 1), this slice runs a
// small Drizzle/Prisma-representative query battery against the restored
// database and asserts all queries return the expected results.
//
// "ORM-representative" means: we drive the verbatim SQL that Drizzle / Prisma
// generate for common patterns (SELECT, INSERT, UPDATE, DELETE, JOIN, ORDER BY,
// LIMIT/OFFSET, aggregate), verified against real ORM output. No ORM binaries
// are required — only their emitted SQL.
//
// Closed by: 5.22.E (ORM query battery post-restore).
//
// Required env: BASIN_LOCAL_URL — the same live Basin instance used in slice 1.
// This test re-creates the round-trip schema independently so it can run in
// isolation (it does not depend on slice 1 having run first).

/// Phase 5.22.A — ORM-load post-restore: after Basin→Basin round-trip,
/// exercise a Drizzle/Prisma representative query battery and assert all pass.
///
/// Slice: "ORM-load post-restore".
///
/// Closes when: 5.22.E lands. At that point drop this `#[ignore]`.
#[ignore = "5.22.A harness — basin-cli dump/restore + cross-PG migration pending; closes 5.22.B-E"]
#[test]
fn pg_dump_orm_load_post_restore() {
    // Slice: "ORM-load post-restore"
    //
    // This test is self-contained: it sets up the schema, performs a
    // dump+restore cycle, then runs the ORM query battery. It does NOT
    // require slice 1 to have run first.

    let Some(basin_url) = require_env("BASIN_LOCAL_URL") else {
        return;
    };

    println!("[5.22.A orm-load] BASIN_LOCAL_URL={basin_url}");

    // ── Setup: schema + seed (same tables as slice 1) ────────────────────
    let setup_sql = "\
        DROP TABLE IF EXISTS orm_orders CASCADE;\
        DROP TABLE IF EXISTS orm_users  CASCADE;\
        CREATE TABLE orm_users (\
            id         BIGINT      PRIMARY KEY,\
            username   TEXT        NOT NULL UNIQUE,\
            active     BOOLEAN     NOT NULL DEFAULT TRUE,\
            score      NUMERIC(6,2) NOT NULL DEFAULT 0,\
            metadata   JSONB\
        );\
        CREATE TABLE orm_orders (\
            id         BIGINT      PRIMARY KEY,\
            user_id    BIGINT      NOT NULL REFERENCES orm_users(id),\
            amount     NUMERIC(12,2) NOT NULL,\
            label      TEXT,\
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
        );\
        INSERT INTO orm_users (id, username, active, score, metadata) VALUES\
            (1, 'alice', TRUE,  95.50, '{\"plan\":\"pro\",\"seats\":5}'),\
            (2, 'bob',   TRUE,  72.00, '{\"plan\":\"free\"}'),\
            (3, 'carol', FALSE, 0.00,  NULL);\
        INSERT INTO orm_orders (id, user_id, amount, label, created_at) VALUES\
            (1, 1, 100.00, 'first',   NOW() - INTERVAL '5 days'),\
            (2, 1, 250.00, 'second',  NOW() - INTERVAL '2 days'),\
            (3, 2, 40.00,  'starter', NOW() - INTERVAL '1 day'),\
            (4, 1, 75.00,  'third',   NOW());";

    run_cmd("psql", &[&basin_url, "--no-psqlrc", "-c", setup_sql]);
    println!("[5.22.A orm-load] schema + seed applied");

    // ── Dump + restore round-trip ─────────────────────────────────────────
    let dump_sql = run_cmd(
        "basin-cli",
        &[
            "dump",
            "--url",
            &basin_url,
            "--table",
            "orm_users",
            "--table",
            "orm_orders",
        ],
    );
    assert!(
        !dump_sql.is_empty(),
        "ORM-LOAD: basin-cli dump produced empty output — closed by 5.22.B"
    );

    // Drop and restore.
    run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "-c",
            "DROP TABLE IF EXISTS orm_orders CASCADE; DROP TABLE IF EXISTS orm_users CASCADE;",
        ],
    );
    run_cmd_with_stdin(
        "basin-cli",
        &["restore", "--url", &basin_url],
        dump_sql.as_bytes(),
    );
    println!("[5.22.A orm-load] round-trip complete");

    // ── ORM query battery ─────────────────────────────────────────────────
    // Each query below is prefixed with the ORM + pattern it exercises.

    // Drizzle: db.select().from(users).where(eq(users.active, true))
    // Prisma:  prisma.orm_users.findMany({ where: { active: true } })
    // Expected: 2 rows (alice + bob; carol is inactive).
    let active_count_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM orm_users WHERE active = TRUE",
        ],
    );
    let active_count: u64 = active_count_raw.trim().parse().unwrap_or_else(|_| {
        panic!("ORM-LOAD: cannot parse active user count: {active_count_raw:?}")
    });
    assert_eq!(
        active_count, 2,
        "ORM-LOAD (Drizzle/Prisma findMany active): expected 2, got={active_count}. \
         Closed by 5.22.E."
    );
    println!("[5.22.A orm-load] findMany(active=true): {active_count} rows ✓");

    // Drizzle: db.select({ username, totalSpend: sum(orders.amount) })
    //           .from(users).leftJoin(orders, ...).groupBy(users.username)
    // Prisma:  prisma.orm_users.findMany({ include: { _count: { ... } } }) +
    //          aggregate query
    // Expected: alice → 425.00 (100+250+75), bob → 40.00, carol → NULL.
    let spend_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT u.username, COALESCE(SUM(o.amount), 0) \
             FROM orm_users u \
             LEFT JOIN orm_orders o ON o.user_id = u.id \
             GROUP BY u.username \
             ORDER BY u.username",
        ],
    );
    // Output format: "alice|425.00\nbob|40.00\ncarol|0.00\n"
    assert!(
        spend_raw.contains("alice"),
        "ORM-LOAD (JOIN+GROUP BY): alice must appear in result — closed by 5.22.E. \
         output={spend_raw:?}"
    );
    assert!(
        spend_raw.contains("425.00"),
        "ORM-LOAD (JOIN+GROUP BY): alice total spend must be 425.00 — closed by 5.22.E. \
         output={spend_raw:?}"
    );
    assert!(
        spend_raw.contains("bob"),
        "ORM-LOAD (JOIN+GROUP BY): bob must appear in result — closed by 5.22.E."
    );
    println!("[5.22.A orm-load] JOIN+GROUP BY aggregate: {}", spend_raw.trim());

    // Drizzle: db.select().from(orders).orderBy(desc(orders.amount)).limit(2)
    // Prisma:  prisma.orm_orders.findMany({ orderBy: { amount: 'desc' }, take: 2 })
    // Expected: top 2 amounts are 250.00 and 100.00.
    let top2_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT amount FROM orm_orders ORDER BY amount DESC LIMIT 2",
        ],
    );
    assert!(
        top2_raw.contains("250.00"),
        "ORM-LOAD (ORDER BY DESC LIMIT): top amount must be 250.00 — closed by 5.22.E. \
         output={top2_raw:?}"
    );
    assert!(
        top2_raw.contains("100.00"),
        "ORM-LOAD (ORDER BY DESC LIMIT): second amount must be 100.00 — closed by 5.22.E. \
         output={top2_raw:?}"
    );
    println!("[5.22.A orm-load] ORDER BY DESC LIMIT 2: {}", top2_raw.trim());

    // Drizzle: db.select().from(orders).where(gte(orders.amount, 100)).offset(0).limit(10)
    // Expected: 2 rows (100.00 + 250.00; the 75.00 and 40.00 are below threshold).
    let big_orders_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT COUNT(*) FROM orm_orders WHERE amount >= 100",
        ],
    );
    let big_orders: u64 = big_orders_raw.trim().parse().unwrap_or_else(|_| {
        panic!("ORM-LOAD: cannot parse big_orders count: {big_orders_raw:?}")
    });
    assert_eq!(
        big_orders, 2,
        "ORM-LOAD (WHERE amount >= 100): expected 2, got={big_orders} — closed by 5.22.E."
    );
    println!("[5.22.A orm-load] WHERE amount>=100: {big_orders} rows ✓");

    // Prisma: prisma.orm_users.findUnique({ where: { username: 'alice' } })
    // + select metadata->>'plan'
    let alice_plan_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT metadata->>'plan' FROM orm_users WHERE username = 'alice'",
        ],
    );
    let alice_plan = alice_plan_raw.trim();
    assert_eq!(
        alice_plan, "pro",
        "ORM-LOAD (Prisma findUnique + JSONB extraction): alice plan must be 'pro' \
         after restore — closed by 5.22.E. got={alice_plan:?}"
    );
    println!("[5.22.A orm-load] Prisma findUnique+JSONB: alice.plan={alice_plan} ✓");

    // Drizzle: db.update(users).set({ score: 99.99 }).where(eq(users.id, 1))
    // (UPDATE then SELECT to verify)
    run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "-c",
            "UPDATE orm_users SET score = 99.99 WHERE id = 1",
        ],
    );
    let score_raw = run_cmd(
        "psql",
        &[
            &basin_url,
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "-c",
            "SELECT score FROM orm_users WHERE id = 1",
        ],
    );
    let score = score_raw.trim();
    assert_eq!(
        score, "99.99",
        "ORM-LOAD (Drizzle update): alice score must be 99.99 after UPDATE — closed by 5.22.E. \
         got={score:?}"
    );
    println!("[5.22.A orm-load] Drizzle update: alice.score={score} ✓");

    println!(
        "[5.22.A orm-load] PASSED — all 5 ORM query patterns pass post-restore \
         (active={active_count}, big_orders={big_orders}, alice.plan={alice_plan}, \
         alice.score={score})"
    );
}
