//! restore — `basin restore` with two modes:
//!
//! ## Cloud backup restore (calls `POST /v1/projects/:ref/backups/restore`)
//!
//!   basin restore --project=<ref> --snapshot=<id> [--yes]
//!   basin restore --project=<ref> --as-of=<rfc3339> [--yes]
//!
//! Enqueues a restore job via the cloud management API.  The cloud records
//! the job and (once engine wiring lands) triggers the engine-side restore.
//! While `engine_ok: false` is returned the cloud still creates the DB row;
//! the status is surfaced faithfully so the caller can track via
//! `basin backups restore-jobs list`.
//!
//! ## Dump-file replay (legacy; kept for `basin dump | basin restore` pipelines)
//!
//!   basin restore --project=<ref> [dump.sql | -]
//!
//! Splits the dump on statement boundaries and POSTs each statement to
//! `POST /v1/projects/{ref}/sql/query` with `writes_enabled=true`.
//!
//! ### Mode selection (priority order)
//!
//!  1. `--snapshot=<id>` → cloud backup restore, `source_kind=snapshot`.
//!  2. `--as-of=<rfc3339>` → cloud PITR restore, `source_kind=timestamp`.
//!  3. Positional file path (or stdin) → dump-file replay.
//!
//! ### Route
//!
//!   POST /v1/projects/:ref/backups/restore
//!   body: { source_kind: "snapshot"|"timestamp", snapshot_id?: "...", target_timestamp?: "..." }
//!   response: { job: { id, status, ... }, engine_ok: bool, engine_msg?: string }

use std::io::Read;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::{engine_admin_path, require_client, require_engine_client, GlobalFlags};
use crate::output::{print_json, read_line};

use super::help::help_for_command;
use super::parse_or_silent;

// ── RestoreOpts ───────────────────────────────────────────────────────────────

/// RestoreOpts captures the parsed CLI flags for `basin restore`
/// when running in dump-replay mode.
pub struct RestoreOpts {
    pub project_ref: String,
    /// Path to the dump file, or "-" to read from stdin.
    pub file: String,
}

// ── SQL splitting helper ──────────────────────────────────────────────────────

/// split_sql_statements splits a dump into individual SQL statements.
///
/// Heuristic: splits on lines that end with `;` after trimming whitespace
/// (pg_dump emits one statement per line in plain text format). Multi-line
/// statements (e.g. CREATE TABLE with a parenthesised body) are kept together
/// as a single chunk. Comment-only lines (starting with `--`) are skipped.
/// Empty statements are dropped.
///
/// This is intentionally conservative: sending a larger batch as one call
/// also works for the SQL query API, but splitting at statement boundaries
/// lets the engine report per-statement errors clearly.
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in sql.lines() {
        let trimmed = line.trim();
        // Skip pure-comment lines and blank lines between statements.
        if trimmed.starts_with("--") {
            continue;
        }
        if !current.is_empty() || !trimmed.is_empty() {
            current.push_str(line);
            current.push('\n');
        }
        if trimmed.ends_with(';') && !current.trim().is_empty() {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
            current.clear();
        }
    }
    let trailing = current.trim().to_string();
    if !trailing.is_empty() {
        statements.push(trailing);
    }
    statements
}

// ── Core implementation (dump-replay mode) ────────────────────────────────────

/// run_restore replays the SQL from `sql_content` into the target project.
///
/// Returns the count of statements executed and the count of errors.
pub fn run_restore(
    g: &GlobalFlags,
    opts: &RestoreOpts,
    sql_content: &str,
) -> CliResult<(usize, usize)> {
    let c = require_client(g)?;
    let statements = split_sql_statements(sql_content);
    let total = statements.len();
    let mut errors = 0usize;

    for (i, stmt) in statements.iter().enumerate() {
        crate::printinfo!(g, "restore: statement {}/{} …", i + 1, total);
        let body = json!({
            "sql": stmt,
            "writes_enabled": true,
        });
        let result: Result<serde_json::Value, _> = c.do_json_timeout(
            Method::POST,
            &format!("/v1/projects/{}/sql/query", opts.project_ref),
            Some(body),
            Duration::from_secs(300),
        );
        if let Err(e) = result {
            errors += 1;
            // Non-fatal: log and continue so the rest of the dump is applied.
            eprintln!("restore: statement {} error (continuing): {e}", i + 1);
        }
    }

    Ok((total, errors))
}

// ── Cloud restore response shape ──────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, Serialize)]
struct RestoreJobInner {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    project_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    initiated_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct CloudRestoreResp {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job: Option<RestoreJobInner>,
    #[serde(default)]
    engine_ok: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    engine_msg: String,
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_restore(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("restore")
        .arg(Arg::new("project").long("project"))
        // Cloud-restore flags:
        .arg(Arg::new("snapshot").long("snapshot"))
        .arg(Arg::new("as-of").long("as-of"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        // Dump-replay flag:
        .arg(Arg::new("file").index(1)) // positional dump.sql
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));

    let m = parse_or_silent(cmd, args)?;

    if m.get_flag("help") {
        help_for_command(
            "restore",
            "Restore a project — from a cloud backup/PITR, a self-hosted engine snapshot, or by replaying a dump file.",
            &[
                "Cloud backup restore (default; calls POST /v1/projects/:ref/backups/restore):",
                "  --snapshot=<id>       Restore from a specific backup snapshot (source_kind=snapshot).",
                "  --as-of=<rfc3339>     Point-in-time restore to a timestamp (source_kind=timestamp).",
                "  --yes                 Skip the confirmation prompt.",
                "  --project=<ref>       Project ref (required, or auto-detected from ./basin/config.toml).",
                "",
                "Self-hosted engine mode (BASIN_MODE=engine or global --engine flag):",
                "  --snapshot=<engine_snapshot_id>  Restore from engine_snapshot_id returned by `basin snapshots create`.",
                "  --as-of=<rfc3339>                Point-in-time restore on the engine.",
                "  --project=<ulid>                 Engine project ULID (not a cloud ref).",
                "  Requires BASIN_ADMIN_TOKEN or --admin-token with is_admin:true JWT.",
                "  Calls POST /admin/v1/projects/:id/restore on the engine.",
                "",
                "Dump-file replay (inverse of `basin dump`):",
                "  dump.sql              Path to the dump file produced by `basin dump`.",
                "                        Use `-` to read from stdin (or pipe: basin dump ... | basin restore ...).",
                "  --project=<ref>       Project ref (required, or auto-detected from ./basin/config.toml).",
                "",
                "Mode is selected automatically:",
                "  --snapshot / --as-of present → engine restore (engine mode) or cloud restore (cloud mode).",
                "  File path / stdin present    → dump-file replay (cloud mode only).",
            ],
        );
        return Ok(());
    }

    // Resolve project ref.
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = if !project.is_empty() {
        project
    } else {
        let cwd = std::env::current_dir().map_err(|e| msg(format!("restore: cwd: {e}")))?;
        let wp = crate::config::load_working_project(&cwd)?;
        match wp {
            Some(w) if !w.project_ref.is_empty() => w.project_ref,
            _ => {
                return Err(msg(
                    "restore: --project is required (or run `basin link` first)",
                ))
            }
        }
    };

    let snapshot = m.get_one::<String>("snapshot").cloned().unwrap_or_default();
    let as_of = m.get_one::<String>("as-of").cloned().unwrap_or_default();
    let yes = m.get_flag("yes");

    // ── Mode: backup restore (engine or cloud) ────────────────────────────────
    if !snapshot.is_empty() || !as_of.is_empty() {
        if g.engine_mode {
            return engine_restore(g, &project_ref, &snapshot, &as_of, yes);
        }
        return cloud_restore(g, &project_ref, &snapshot, &as_of, yes);
    }

    // ── Mode: dump-file replay ────────────────────────────────────────────────
    let file = m.get_one::<String>("file").cloned().unwrap_or_default();

    let sql_content = if file.is_empty() || file == "-" {
        // Read from stdin.
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| msg(format!("restore: read stdin: {e}")))?;
        if s.trim().is_empty() {
            return Err(msg(
                "restore: no SQL input (provide a file path or pipe a dump via stdin)\n\
                 hint: use --snapshot=<id> or --as-of=<rfc3339> for cloud backup restore",
            ));
        }
        s
    } else {
        std::fs::read_to_string(&file)
            .map_err(|e| msg(format!("restore: read {file}: {e}")))?
    };

    let opts = RestoreOpts {
        project_ref,
        file,
    };

    let (total, errors) = run_restore(g, &opts, &sql_content)?;

    if errors > 0 {
        if !g.quiet {
            eprintln!("restore: {total} statement(s), {errors} error(s)");
        }
        if g.json {
            // JSON shape: { "statements": N, "errors": N, "ok": false }
            crate::output::print_json(
                &mut std::io::stdout(),
                &json!({ "statements": total, "errors": errors, "ok": false }),
            )?;
        }
        return Err(msg(format!("restore finished with {errors} error(s)")));
    }

    if g.json {
        // JSON shape: { "statements": N, "errors": 0, "ok": true }
        crate::output::print_json(
            &mut std::io::stdout(),
            &json!({ "statements": total, "errors": 0, "ok": true }),
        )?;
    } else if !g.quiet {
        println!("restore: {total} statement(s) applied, 0 errors");
    }
    Ok(())
}

// ── engine_restore ────────────────────────────────────────────────────────────

/// engine_restore calls POST /admin/v1/projects/:id/restore on the self-hosted
/// engine directly.
///
/// - `snapshot` non-empty → body `{ engine_snapshot_id: "…" }` (named-snapshot restore)
/// - `as_of` non-empty    → body `{ as_of: "…" }` (PITR; RFC-3339 timestamp)
///
/// Returns `{ ok: true, restored_tables: N }` on success.
fn engine_restore(
    g: &GlobalFlags,
    project_ref: &str,
    snapshot: &str,
    as_of: &str,
    yes: bool,
) -> CliResult<()> {
    if !snapshot.is_empty() && !as_of.is_empty() {
        return Err(msg(
            "restore: --snapshot and --as-of are mutually exclusive; use one or the other",
        ));
    }

    let desc = if !snapshot.is_empty() {
        format!("engine snapshot {:?}", snapshot)
    } else {
        format!("timestamp {:?}", as_of)
    };

    if !yes {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to restore project {:?} from {} non-interactively under --json",
                project_ref, desc
            )));
        }
        eprint!(
            "Restore project {:?} from {}? This will overwrite the project's data. [y/N] ",
            project_ref, desc
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_engine_client(g)?;

    let body = if !snapshot.is_empty() {
        json!({ "engine_snapshot_id": snapshot })
    } else {
        json!({ "as_of": as_of })
    };

    let resp: serde_json::Value = c.do_json(
        Method::POST,
        &engine_admin_path(project_ref, "restore"),
        Some(body),
    )?;

    if g.json {
        // Engine shape: { ok: bool, restored_tables: N }
        return print_json(&mut std::io::stdout(), &resp);
    }

    let restored = resp
        .get("restored_tables")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    println!(
        "Restore from {desc} complete ({restored} table(s) restored)."
    );
    Ok(())
}

// ── cloud_restore ─────────────────────────────────────────────────────────────

/// cloud_restore calls POST /v1/projects/:ref/backups/restore.
///
/// - snapshot non-empty → source_kind=snapshot
/// - as_of non-empty    → source_kind=timestamp (PITR)
///
/// The cloud creates the restore job row and (when engine wiring is complete)
/// triggers the engine-side restore. While engine_ok=false the job row still
/// exists and is trackable via `basin backups restore-jobs list`.
fn cloud_restore(
    g: &GlobalFlags,
    project_ref: &str,
    snapshot: &str,
    as_of: &str,
    yes: bool,
) -> CliResult<()> {
    if !snapshot.is_empty() && !as_of.is_empty() {
        return Err(msg(
            "restore: --snapshot and --as-of are mutually exclusive; use one or the other",
        ));
    }

    let (source_kind, desc) = if !snapshot.is_empty() {
        ("snapshot", format!("snapshot {:?}", snapshot))
    } else {
        ("timestamp", format!("timestamp {:?}", as_of))
    };

    if !yes {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to restore project {:?} from {} non-interactively under --json",
                project_ref, desc
            )));
        }
        eprint!(
            "Restore project {:?} from {}? This will overwrite the project's data. [y/N] ",
            project_ref, desc
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    let mut body = json!({ "source_kind": source_kind });
    if !snapshot.is_empty() {
        body["snapshot_id"] = json!(snapshot);
    }
    if !as_of.is_empty() {
        body["target_timestamp"] = json!(as_of);
    }

    let resp: CloudRestoreResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{project_ref}/backups/restore"),
        Some(body),
    )?;

    if g.json {
        // JSON shape: { job: { id, status, source_kind, ... }, engine_ok: bool, engine_msg?: string }
        return print_json(&mut std::io::stdout(), &resp);
    }

    if let Some(j) = &resp.job {
        println!("Restore job queued: {} (status: {})", j.id, j.status);
    } else {
        println!("Restore job queued for project {project_ref}.");
    }
    if !resp.engine_ok {
        if !resp.engine_msg.is_empty() {
            println!(
                "  engine: {} (engine wiring pending; job will be picked up automatically)",
                resp.engine_msg
            );
        } else {
            println!("  note: engine step is pending; track progress with `basin backups restore-jobs list --project={project_ref}`");
        }
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "bso_org_test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "bso_org_test".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── unit: split_sql_statements ────────────────────────────────────────────

    #[test]
    fn split_single_statement() {
        let stmts = split_sql_statements("SELECT 1;");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1;");
    }

    #[test]
    fn split_multi_statement() {
        let sql = "CREATE TABLE users (id int);\nINSERT INTO users VALUES (1);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "stmts: {stmts:?}");
    }

    #[test]
    fn split_skips_comment_lines() {
        let sql = "-- header\nCREATE TABLE t (id int);\n-- comment\nINSERT INTO t VALUES (1);";
        let stmts = split_sql_statements(sql);
        // Comments are stripped; 2 real statements remain.
        assert_eq!(stmts.len(), 2, "stmts: {stmts:?}");
    }

    #[test]
    fn split_multi_line_create_table() {
        let sql = "CREATE TABLE orders (\n  id int,\n  name text\n);\nINSERT INTO orders VALUES (1, 'x');";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "stmts: {stmts:?}");
        assert!(stmts[0].contains("CREATE TABLE orders"), "stmt0: {}", stmts[0]);
    }

    #[test]
    fn split_empty_input() {
        assert_eq!(split_sql_statements("").len(), 0);
        assert_eq!(split_sql_statements("   \n  -- only comments\n").len(), 0);
    }

    #[test]
    fn split_no_trailing_semicolon() {
        // A statement without a trailing semicolon is still included.
        let stmts = split_sql_statements("SELECT 1");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 1");
    }

    // ── unit: arg parsing ─────────────────────────────────────────────────────

    #[test]
    fn missing_project_returns_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        // No project and no link file → error.
        let err = cmd_restore(&g, &[]).unwrap_err();
        assert!(
            err.to_string().contains("project") || err.to_string().contains("link"),
            "error should mention --project: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        cmd_restore(&g, &["--help".into()]).unwrap();
    }

    #[test]
    fn snapshot_and_as_of_mutually_exclusive() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--snapshot=snap-1".into(),
                "--as-of=2026-01-01T00:00:00Z".into(),
                "--yes".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "err: {err}");
    }

    // ── cloud restore: --snapshot ─────────────────────────────────────────────

    #[test]
    fn cloud_restore_snapshot_yes() {
        let _cfg = with_temp_config_dir();
        let captured = std::sync::Arc::new(std::sync::Mutex::new(serde_json::Value::Null));
        let cap2 = std::sync::Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/backups/restore");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"job":{"id":"job-1","status":"pending","source_kind":"snapshot"},"engine_ok":false,"engine_msg":"engine_unconfigured"}"#)
        });
        let g = flags(&srv.url);
        cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--snapshot=snap-abc".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["source_kind"].as_str().unwrap(), "snapshot");
        assert_eq!(body["snapshot_id"].as_str().unwrap(), "snap-abc");
        assert!(body.get("target_timestamp").is_none());
    }

    #[test]
    fn cloud_restore_pitr_yes() {
        let _cfg = with_temp_config_dir();
        let captured = std::sync::Arc::new(std::sync::Mutex::new(serde_json::Value::Null));
        let cap2 = std::sync::Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"job":{"id":"job-2","status":"pending","source_kind":"timestamp"},"engine_ok":false,"engine_msg":"engine_unconfigured"}"#)
        });
        let g = flags(&srv.url);
        cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--as-of=2026-01-15T12:00:00Z".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["source_kind"].as_str().unwrap(), "timestamp");
        assert_eq!(body["target_timestamp"].as_str().unwrap(), "2026-01-15T12:00:00Z");
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn cloud_restore_json_flag_requires_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags_json(&srv.url);
        let err = cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--snapshot=snap-1".into(),
                // no --yes
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("confirmation required"), "err: {err}");
    }

    #[test]
    fn cloud_restore_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"job":{"id":"job-j","status":"pending","source_kind":"snapshot"},"engine_ok":false,"engine_msg":"engine_unconfigured"}"#)
        });
        let g = flags_json(&srv.url);
        cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--snapshot=snap-j".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn cloud_restore_409_propagates() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                409,
                r#"{"error":{"code":"restore_in_progress","message":"A restore is already in progress."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                "--snapshot=snap-1".into(),
                "--yes".into(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 409);
    }

    // ── dump-replay mode ──────────────────────────────────────────────────────

    #[test]
    fn run_restore_posts_statements() {
        let _cfg = with_temp_config_dir();

        let posted: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let posted2 = std::sync::Arc::clone(&posted);

        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            if let Some(sql) = body["sql"].as_str() {
                posted2.lock().unwrap().push(sql.to_string());
            }
            assert_eq!(body["writes_enabled"], true, "writes_enabled must be true");
            Resp::ok(r#"{"data":{}}"#)
        });

        let g = flags(&srv.url);
        let sql = "CREATE TABLE t (id int);\nINSERT INTO t VALUES (1);";
        let opts = RestoreOpts {
            project_ref: "proj1".into(),
            file: String::new(),
        };

        let (total, errors) = run_restore(&g, &opts, sql).unwrap();
        assert_eq!(total, 2, "expected 2 statements");
        assert_eq!(errors, 0, "expected 0 errors");

        let p = posted.lock().unwrap();
        assert_eq!(p.len(), 2, "expected 2 POSTs, got: {p:?}");
        assert!(
            p[0].contains("CREATE TABLE"),
            "first stmt should be CREATE TABLE: {}",
            p[0]
        );
        assert!(
            p[1].contains("INSERT INTO"),
            "second stmt should be INSERT INTO: {}",
            p[1]
        );
    }

    #[test]
    fn run_restore_continues_on_error() {
        let _cfg = with_temp_config_dir();

        // First POST succeeds, second fails.
        let call_count = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let call_count2 = std::sync::Arc::clone(&call_count);

        let srv = TestServer::start(move |_req: &Req| {
            let mut n = call_count2.lock().unwrap();
            *n += 1;
            let call = *n;
            drop(n);
            if call == 1 {
                Resp::ok(r#"{"data":{}}"#)
            } else {
                Resp::status(500, r#"{"error":{"code":"err","message":"kaboom"}}"#)
            }
        });

        let g = flags(&srv.url);
        let sql = "SELECT 1;\nSELECT 2;";
        let opts = RestoreOpts {
            project_ref: "proj1".into(),
            file: String::new(),
        };

        let (total, errors) = run_restore(&g, &opts, sql).unwrap();
        assert_eq!(total, 2);
        assert_eq!(errors, 1, "expected 1 error from the failing second stmt");
    }

    #[test]
    fn run_restore_sets_writes_enabled_true() {
        let _cfg = with_temp_config_dir();

        let writes_flags: std::sync::Arc<std::sync::Mutex<Vec<bool>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writes_flags2 = std::sync::Arc::clone(&writes_flags);

        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            writes_flags2
                .lock()
                .unwrap()
                .push(body["writes_enabled"].as_bool().unwrap_or(false));
            Resp::ok(r#"{"data":{}}"#)
        });

        let g = flags(&srv.url);
        let opts = RestoreOpts {
            project_ref: "p1".into(),
            file: String::new(),
        };
        run_restore(&g, &opts, "INSERT INTO t VALUES (1);").unwrap();

        let flags_vec = writes_flags.lock().unwrap();
        assert!(
            flags_vec.iter().all(|&f| f),
            "all calls must set writes_enabled=true: {flags_vec:?}"
        );
    }

    #[test]
    fn cmd_restore_reads_from_file() {
        let _cfg = with_temp_config_dir();

        let posted: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let posted2 = std::sync::Arc::clone(&posted);

        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            if let Some(sql) = body["sql"].as_str() {
                posted2.lock().unwrap().push(sql.to_string());
            }
            Resp::ok(r#"{"data":{}}"#)
        });

        let dir = std::env::temp_dir().join(format!("basin-restore-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dump_path = dir.join("dump.sql");
        std::fs::write(&dump_path, "CREATE TABLE t (id int);\nINSERT INTO t VALUES (42);")
            .unwrap();

        let g = flags(&srv.url);
        cmd_restore(
            &g,
            &[
                "--project=proj1".into(),
                dump_path.display().to_string(),
            ],
        )
        .unwrap();

        let p = posted.lock().unwrap();
        assert_eq!(p.len(), 2, "expected 2 POSTs");
        assert!(p[0].contains("CREATE TABLE"));
        assert!(p[1].contains("INSERT INTO"));

        std::fs::remove_dir_all(&dir).ok();
    }
}
