//! restore — `basin restore --project=<ref> [dump.sql | -]`
//!
//! Replays a dump file (produced by `basin dump`) into a project.
//!
//! Input sources (in priority order):
//!   1. A positional file path argument (dump.sql).
//!   2. Stdin when the path is `-` or when no path is given and stdin is
//!      not a TTY (i.e. the output of `basin dump | basin restore`).
//!
//! Transport: the CLI splits the SQL on statement boundaries (`;` at end
//! of line, matching pg_dump's output) and POSTs each statement batch to
//! POST /v1/projects/{ref}/sql/query with writes_enabled=true. This is
//! the same mechanism used by `migrate_from_pg::basin_restore_sql`.
//!
//! Engine-native restore:
//!   When basin-engine ships a dedicated restore endpoint (e.g.
//!   POST /v1/projects/{ref}/restore) that accepts a streamed dump body,
//!   replace the SQL-splitting + batch-post logic below with a single
//!   streaming call to that endpoint.
//!   See: // TODO(5.22.D-engine-api)

use std::io::Read;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};

use super::help::help_for_command;
use super::parse_or_silent;

// ── RestoreOpts ───────────────────────────────────────────────────────────────

/// RestoreOpts captures the parsed CLI flags for `basin restore`.
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

// ── Core implementation ───────────────────────────────────────────────────────

/// run_restore replays the SQL from `sql_content` into the target project.
///
/// Returns the count of statements executed and the count of errors.
///
/// TODO(5.22.D-engine-api): When basin-engine exposes a native restore
/// endpoint (e.g. `POST /v1/projects/{ref}/restore` accepting an
/// `application/octet-stream` or `text/plain` body), replace the statement-
/// splitting + per-statement POST loop below with a single streaming call to
/// that endpoint. The `RestoreOpts` struct and command wiring here should
/// remain unchanged.
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
        crate::printinfo!(
            g,
            "restore: statement {}/{} …",
            i + 1,
            total
        );
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
            eprintln!(
                "restore: statement {} error (continuing): {e}",
                i + 1
            );
        }
    }

    Ok((total, errors))
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_restore(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("restore")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("file").index(1)) // positional dump.sql
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));

    let m = parse_or_silent(cmd, args)?;

    if m.get_flag("help") {
        help_for_command(
            "restore",
            "Replay a dump file into a project (inverse of `basin dump`).",
            &[
                "--project=<ref>   Project ref (required, or auto-detected from ./basin/config.toml).",
                "dump.sql          Path to the dump file produced by `basin dump`.",
                "                  Use `-` to read from stdin (or pipe: basin dump ... | basin restore ...).",
                "",
                "Engine-native restore endpoint (POST /v1/projects/{ref}/restore) will be wired",
                "once basin-engine ships it. See TODO(5.22.D-engine-api) in restore.rs.",
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

    // Resolve input: positional arg, then stdin.
    let file = m.get_one::<String>("file").cloned().unwrap_or_default();

    let sql_content = if file.is_empty() || file == "-" {
        // Read from stdin.
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| msg(format!("restore: read stdin: {e}")))?;
        if s.trim().is_empty() {
            return Err(msg(
                "restore: no SQL input (provide a file path or pipe a dump via stdin)",
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
        return Err(msg(format!(
            "restore finished with {errors} error(s)"
        )));
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

    // ── stub-server: run_restore posts each statement ─────────────────────────

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
            assert_eq!(
                body["writes_enabled"], true,
                "writes_enabled must be true"
            );
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

    // ── stub-server: run_restore tolerates server errors ──────────────────────

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

    // ── stub-server: run_restore writes_enabled=true check ────────────────────

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

    // ── stub-server: cmd_restore with file path ───────────────────────────────

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
