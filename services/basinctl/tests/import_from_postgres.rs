//! Integration tests for `basinctl import-from-postgres` and `basinctl fn`.
//!
//! basinctl is a bin-only crate, so these tests drive the compiled binary
//! via `CARGO_BIN_EXE_basinctl` (Cargo sets it for integration tests of
//! `[[bin]]` targets).
//!
//! Two tiers:
//!
//! 1. **Non-ignored** — pure CLI-surface checks (no network): `fn` prints
//!    the roadmap, `import-from-postgres` refuses to run without
//!    `--source`, and a bogus source fails with a non-zero exit and a
//!    `connect to source Postgres` error. (The DDL-translation unit suite
//!    lives in `src/translate.rs` — pure, network-free.)
//!
//! 2. **`#[ignore]`d** — end-to-end runs that need live endpoints. These
//!    are genuinely pending external wiring (a throwaway source Postgres
//!    and a running Basin), not silenced failures: provide
//!    `IMPORT_TEST_SOURCE_URL` (Postgres with CREATE privileges) and
//!    `IMPORT_TEST_TARGET_URL` (Basin pgwire), then run
//!    `cargo test -p basinctl -- --ignored`.

use std::process::{Command, Output};

fn basinctl(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_basinctl"))
        .args(args)
        // Make sure an ambient BASIN_URL can't leak a real endpoint into
        // the no-network tier.
        .env_remove("BASIN_URL")
        .output()
        .expect("spawn basinctl")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

// ---------------------------------------------------------------------------
// Tier 1: no network needed
// ---------------------------------------------------------------------------

#[test]
fn fn_stub_prints_wasm_roadmap_and_succeeds() {
    let out = basinctl(&["fn"]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let s = stdout(&out);
    assert!(s.contains("WASM functions toolchain"), "stdout: {s}");
    assert!(s.contains("fn deploy"), "stdout: {s}");
    assert!(s.contains("docs/functions.md"), "stdout: {s}");
}

#[test]
fn import_requires_source_flag() {
    let out = basinctl(&["import-from-postgres"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("--source"),
        "stderr: {}",
        stderr(&out)
    );
}

#[test]
fn import_unreachable_source_fails_with_context() {
    // Reserved TEST-NET-1 address + 1s connect_timeout: fails fast without
    // touching anything real. --dry-run keeps the target out of the path.
    let out = basinctl(&[
        "import-from-postgres",
        "--source",
        "postgres://u:p@192.0.2.1:5432/db?connect_timeout=1",
        "--dry-run",
    ]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("connect to source Postgres"),
        "stderr: {}",
        stderr(&out)
    );
}

// ---------------------------------------------------------------------------
// Tier 2: live endpoints (pending external wiring — see module docs)
// ---------------------------------------------------------------------------

fn live_urls() -> (String, String) {
    let src = std::env::var("IMPORT_TEST_SOURCE_URL")
        .expect("set IMPORT_TEST_SOURCE_URL to a throwaway source Postgres");
    let dst = std::env::var("IMPORT_TEST_TARGET_URL")
        .expect("set IMPORT_TEST_TARGET_URL to a running Basin pgwire endpoint");
    (src, dst)
}

async fn pg_connect(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect {url}: {e}"));
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// Seed a small but representative source: serial PK, FK pair, an enum, a
/// trigger + plpgsql function (must be *skipped* loudly), and one
/// non-binary-safe column (interval) to force the CSV path on one table.
const SEED: &str = r#"
DROP TABLE IF EXISTS imp_orders;
DROP TABLE IF EXISTS imp_users;
DROP TYPE IF EXISTS imp_status;
CREATE TYPE imp_status AS ENUM ('new', 'shipped');
CREATE TABLE imp_users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE imp_orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES imp_users(id) ON DELETE CASCADE,
    status imp_status NOT NULL DEFAULT 'new',
    grace INTERVAL,
    total NUMERIC(10,2) NOT NULL
);
CREATE OR REPLACE FUNCTION imp_touch() RETURNS trigger LANGUAGE plpgsql AS
  $$ BEGIN NEW.created_at = now(); RETURN NEW; END $$;
CREATE TRIGGER imp_users_touch BEFORE UPDATE ON imp_users
  FOR EACH ROW EXECUTE FUNCTION imp_touch();
INSERT INTO imp_users (email)
  SELECT 'u' || g || '@example.com' FROM generate_series(1, 500) g;
INSERT INTO imp_orders (user_id, status, grace, total)
  SELECT (g % 500) + 1,
         CASE WHEN g % 2 = 0 THEN 'new'::imp_status ELSE 'shipped' END,
         make_interval(days => g % 30),
         (g % 1000)::numeric / 7
  FROM generate_series(1, 2000) g;
"#;

#[tokio::test]
#[ignore = "needs live endpoints: IMPORT_TEST_SOURCE_URL (throwaway Postgres) + IMPORT_TEST_TARGET_URL (Basin) — pending external wiring, run with --ignored"]
async fn live_roundtrip_imports_schema_and_rows() {
    let (src_url, dst_url) = live_urls();
    let src = pg_connect(&src_url).await;
    src.batch_execute(SEED).await.expect("seed source");

    let out = basinctl(&[
        "import-from-postgres",
        "--source",
        &src_url,
        "--target",
        &dst_url,
        "--table",
        "imp_users",
        "--table",
        "imp_orders",
        "--jobs",
        "2",
    ]);
    let (so, se) = (stdout(&out), stderr(&out));
    assert!(out.status.success(), "stdout:\n{so}\nstderr:\n{se}");

    // Row counts verified by the tool itself; double-check independently.
    let dst = pg_connect(&dst_url).await;
    for (table, want) in [("imp_users", "500"), ("imp_orders", "2000")] {
        let msgs = dst
            .simple_query(&format!("SELECT count(*) FROM {table}"))
            .await
            .expect("count on Basin");
        let got = msgs
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
                _ => None,
            })
            .expect("count row");
        assert_eq!(got, want, "count of {table}");
    }

    // The skip report must name the trigger and the plpgsql function with
    // their Basin migration hints.
    assert!(se.contains("imp_users_touch"), "stderr:\n{se}");
    assert!(se.contains("reactor"), "stderr:\n{se}");
    assert!(se.contains("imp_touch"), "stderr:\n{se}");
    assert!(se.contains("WASM") || se.contains("Wasm"), "stderr:\n{se}");
    // Summary present, counts verified.
    assert!(so.contains("== import summary =="), "stdout:\n{so}");
    assert!(so.contains("counts match"), "stdout:\n{so}");
    // interval column forces imp_orders onto the CSV path.
    assert!(so.contains("(csv)"), "stdout:\n{so}");
    assert!(so.contains("(binary)"), "stdout:\n{so}");
}

#[tokio::test]
#[ignore = "needs live endpoint: IMPORT_TEST_SOURCE_URL (throwaway Postgres) — pending external wiring, run with --ignored"]
async fn live_dry_run_prints_translated_ddl_without_writing() {
    let src_url = std::env::var("IMPORT_TEST_SOURCE_URL")
        .expect("set IMPORT_TEST_SOURCE_URL to a throwaway source Postgres");
    let src = pg_connect(&src_url).await;
    src.batch_execute(SEED).await.expect("seed source");

    let out = basinctl(&[
        "import-from-postgres",
        "--source",
        &src_url,
        "--table",
        "imp_users",
        "--table",
        "imp_orders",
        "--dry-run",
    ]);
    let so = stdout(&out);
    assert!(out.status.success(), "stderr:\n{}", stderr(&out));
    // serial → Basin identity convention.
    assert!(
        so.contains("BIGINT GENERATED BY DEFAULT AS IDENTITY"),
        "stdout:\n{so}"
    );
    // Enum recreated ahead of the tables.
    assert!(
        so.contains("CREATE TYPE \"imp_status\" AS ENUM ('new', 'shipped')"),
        "stdout:\n{so}"
    );
    // FK rewritten to a bare Basin table name.
    assert!(so.contains("REFERENCES \"imp_users\""), "stdout:\n{so}");
}
