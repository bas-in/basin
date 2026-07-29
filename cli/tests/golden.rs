//! tests/golden.rs — insta-backed golden-output snapshot suite.
//!
//! Locks the human-readable text of every command's `--help` (and a handful
//! of deterministic happy-path read commands) so accidental UX regressions
//! (rewording, flag-shape drift, command-table reorder) surface as a diff
//! during CI rather than as a confused-user bug report.
//!
//! Maintenance: when a help string changes on purpose, refresh with
//!     cargo insta accept            # accept all pending changes
//!     INSTA_UPDATE=auto cargo test  # accept missing/changed snapshots in-place
//!     INSTA_UPDATE=always cargo test --test golden  # accept regardless
//!
//! TASKS.md line 365 ("Golden output tests").
//!
//! Coverage scope (one snapshot per item):
//!   1. Top-level `basin --help`.
//!   2. Per-top-level-command `<cmd> --help` — driven through a macro so
//!      the cost of adding a command is one `golden!()` row, not a bespoke
//!      test function.
//!   3. A few stable plain-text or `--json` happy paths that don't need a
//!      stub server (no network, no UUIDs, no timestamps): `version` (build
//!      string, with os/arch redacted), `help projects` (clap's help-of-help
//!      mirror), and `docs --json db` (deterministic `{opened:false,url}`
//!      JSON shape).
//!
//! Explicitly *out* of scope here:
//!   - `--version` (basin doesn't accept it as a global flag; surfaces an
//!     error — covered by integration.rs instead).
//!   - `docs db` plain mode (would shell out to `open(1)` and try to launch
//!     a browser; the `--json` flag is the CI-safe path).
//!   - Any list/get response — those need the stub server (integration.rs).

use std::process::Command;

/// run spawns the freshly-built `basin` binary with `args` and returns
/// (exit_code, stdout, stderr). All env that could redirect to a real
/// network or pick up the developer's on-disk config is scrubbed so the
/// snapshots are reproducible across machines.
fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_basin"))
        .args(args)
        // Strip ambient auth + endpoint so a stray `basin orgs list` (or
        // similar) can't reach api.basin.run. Help paths don't hit the
        // network, but defence-in-depth.
        .env_remove("BASIN_TOKEN")
        .env_remove("BASIN_API_URL")
        .env("BASIN_API", "http://127.0.0.1:1")
        // Force no-color so ANSI escapes never leak into snapshots on a
        // terminal that auto-detects truthy TERM.
        .env("NO_COLOR", "1")
        // Isolate config away from the user's real ~/.config/basin.
        .env("XDG_CONFIG_HOME", std::env::temp_dir())
        .output()
        .expect("spawn basin");
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (code, stdout, stderr)
}

/// snapshot_help captures `basin <cmd> --help` under a stable snapshot name.
/// Each invocation runs in its own #[test] (so failures isolate to the
/// guilty command), but the body is identical, hence the helper.
fn snapshot_help(cmd: &str) {
    let (_code, stdout, _stderr) = run(&[cmd, "--help"]);
    // Help is rendered locally by the CLI's own dispatch (basin uses a
    // hand-rolled, not clap-derive, help printer); no need to redact.
    insta::assert_snapshot!(format!("help_{}", cmd.replace('-', "_")), stdout);
}

// ── 1. Top-level help / version / help-of-help / docs JSON ─────────────────

#[test]
fn golden_top_level_help() {
    let (_code, stdout, _) = run(&["--help"]);
    insta::assert_snapshot!("top_level_help", stdout);
}

/// `basin version` prints `basin <ver> built <date> for <os>/<arch>`.
/// `<os>/<arch>` is host-specific (`macos/aarch64` on the Apple Silicon
/// dev box, `linux/x86_64` in CI). We redact it so the snapshot is the
/// same regardless of where the test runs.
#[test]
fn golden_version_plain() {
    let (_code, stdout, _) = run(&["version"]);
    insta::with_settings!({
        filters => vec![
            // "for macos/aarch64" → "for <os>/<arch>"
            (r"for [a-z0-9_]+/[a-z0-9_]+", "for <os>/<arch>"),
            // "built unknown" stays stable when BASIN_BUILD_DATE is unset;
            // tolerate any ISO-ish date if a CI build injects one.
            (r"built \S+ for", "built <date> for"),
        ]
    }, {
        insta::assert_snapshot!("version_plain", stdout);
    });
}

/// `basin help projects` is the clap-style "help of a subcommand" alias.
/// It mirrors `basin projects --help` — snapshotting it independently
/// catches a future regression where the two paths diverge.
#[test]
fn golden_help_of_help_projects() {
    let (_code, stdout, _) = run(&["help", "projects"]);
    insta::assert_snapshot!("help_subcommand_projects", stdout);
}

/// `basin docs --json db` returns a deterministic 2-field object — no
/// timestamps, no UUIDs — when launched outside a TTY (opened: false).
#[test]
fn golden_docs_json_db() {
    let (_code, stdout, _) = run(&["docs", "--json", "db"]);
    insta::assert_snapshot!("docs_json_db", stdout);
}

// ── 2. Per-top-level-command `<cmd> --help` ────────────────────────────────
//
// One #[test] per command. Adding a new command is one `golden_help!` row.
// `transfers` is NOT here: it's a sub-command of `projects`, not a
// top-level command.

macro_rules! golden_help {
    ($name:ident, $cmd:expr) => {
        #[test]
        fn $name() {
            snapshot_help($cmd);
        }
    };
}

// Identity / account commands.
golden_help!(golden_help_login, "login");
golden_help!(golden_help_logout, "logout");
golden_help!(golden_help_whoami, "whoami");

// Org + membership.
golden_help!(golden_help_orgs, "orgs");
golden_help!(golden_help_members, "members");
golden_help!(golden_help_invitations, "invitations");

// Project lifecycle.
golden_help!(golden_help_projects, "projects");
golden_help!(golden_help_init, "init");
golden_help!(golden_help_link, "link");
golden_help!(golden_help_unlink, "unlink");
golden_help!(golden_help_status, "status");

// SQL / data plane.
golden_help!(golden_help_sql, "sql");
golden_help!(golden_help_dump, "dump");
golden_help!(golden_help_restore, "restore");
golden_help!(golden_help_migrate_from_pg, "migrate-from-pg");
golden_help!(golden_help_rpc, "rpc");
golden_help!(golden_help_db, "db");
golden_help!(golden_help_tables, "tables");
golden_help!(golden_help_rows, "rows");
golden_help!(golden_help_storage, "storage");
golden_help!(golden_help_functions, "functions");
golden_help!(golden_help_rls, "rls");
golden_help!(golden_help_migrations, "migrations");

// Branches / generation / snapshots / backups.
golden_help!(golden_help_branches, "branches");
golden_help!(golden_help_gen, "gen");
golden_help!(golden_help_backups, "backups");

// Observability.
golden_help!(golden_help_logs, "logs");
golden_help!(golden_help_realtime, "realtime");
golden_help!(golden_help_metrics, "metrics");
golden_help!(golden_help_activity, "activity");
golden_help!(golden_help_audit, "audit");

// Project knobs / addresses.
golden_help!(golden_help_api_keys, "api-keys");
golden_help!(golden_help_pgwire, "pgwire");
golden_help!(golden_help_domains, "domains");
golden_help!(golden_help_webhooks, "webhooks");
golden_help!(golden_help_alerts, "alerts");
golden_help!(golden_help_erd, "erd");
golden_help!(golden_help_queries, "queries");
golden_help!(golden_help_engine_pin, "engine-pin");
golden_help!(golden_help_extensions, "extensions");
golden_help!(golden_help_oauth_providers, "oauth-providers");
golden_help!(golden_help_email, "email");
golden_help!(golden_help_byo, "byo");

// Org-level auth.
golden_help!(golden_help_saml, "saml");
golden_help!(golden_help_scim, "scim");
golden_help!(golden_help_oauth_apps, "oauth-apps");

// Miscellany.
golden_help!(golden_help_docs, "docs");
golden_help!(golden_help_secrets, "secrets");
golden_help!(golden_help_tokens, "tokens");
golden_help!(golden_help_config, "config");
golden_help!(golden_help_completion, "completion");
golden_help!(golden_help_version, "version");
