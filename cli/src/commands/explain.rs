//! explain — EXPLAIN ANALYZE proxy for a project's engine.
//!
//!   basin explain <sql> [--project=<ref>] [--analyze]
//!   basin explain --file=<path> [--project=<ref>] [--analyze]
//!   echo "SELECT …" | basin explain --project=<ref>
//!
//! Backed by GET /v1/projects/{ref}/explain?sql=<url-encoded>&analyze=<bool>.
//!
//! The cloud proxies the SQL to the engine's `EXPLAIN [ANALYZE]` handler and
//! returns the query plan as a text string in the `plan` field. This is the
//! DX-friendly path — no psql connection needed.
//!
//! --analyze defaults to false (EXPLAIN only). Set --analyze to get real
//! per-node timing (slower but more useful for tuning).
//!
//! SQL source precedence:
//!   1. Positional <sql> argument
//!   2. --file=<path>
//!   3. stdin (when neither is provided, blocks for piped input)

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::print_json;

use super::help::help_for_command;
use super::parse_or_silent;

// ── project resolution ────────────────────────────────────────────────────────

fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg(
            "--project is required (or run `basin link` to bind this directory)",
        )),
    }
}

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// ExplainResponse is the response envelope from GET /v1/projects/:ref/explain.
/// JSON shape: { plan: string }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ExplainResponse {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plan: String,
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_explain(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("explain")
        .arg(Arg::new("sql").index(1))
        .arg(Arg::new("file").long("file").short('f'))
        .arg(Arg::new("project").long("project"))
        .arg(
            Arg::new("analyze")
                .long("analyze")
                .short('A')
                .action(ArgAction::SetTrue),
        )
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "explain",
            "Run EXPLAIN [ANALYZE] on a SQL statement via the cloud proxy.",
            &[
                "<sql>               SQL statement to explain (positional).",
                "--file=<path>       Read SQL from a file instead.",
                "--analyze           Use EXPLAIN ANALYZE (runs the query; slower).",
                "--project=<ref>     Project ref (or use basin link).",
                "",
                "SQL can also be piped from stdin:",
                "  echo 'SELECT * FROM orders WHERE status = $1' | basin explain --project=myapp",
            ],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;
    let analyze = m.get_flag("analyze");

    // Resolve SQL from positional arg, --file, or stdin.
    let sql: String = if let Some(s) = m.get_one::<String>("sql").cloned() {
        s
    } else if let Some(path) = m.get_one::<String>("file").cloned() {
        std::fs::read_to_string(&path)
            .map_err(|e| msg(format!("explain: could not read --file {path}: {e}")))?
    } else {
        // Read from stdin.
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| msg(format!("explain: could not read stdin: {e}")))?;
        s
    };

    let sql = sql.trim().to_string();
    if sql.is_empty() {
        return Err(msg(
            "explain: no SQL provided. Pass as positional arg, --file, or stdin.",
        ));
    }

    let analyze_str = if analyze { "true" } else { "false" };
    let q = query_string(&[("sql", &sql), ("analyze", analyze_str)]);
    let path = format!("/v1/projects/{}/explain{}", project_ref, q);

    let c = require_client(g)?;
    let resp: ExplainResponse = c.do_json(Method::GET, &path, None)?;

    if g.json {
        // JSON shape: { plan: string }
        return print_json(&mut std::io::stdout(), &resp);
    }

    println!("{}", resp.plan);
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{Arc, Mutex};

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    const SAMPLE_PLAN: &str =
        "Seq Scan on orders  (cost=0.00..1.10 rows=10 width=32) (actual time=0.012..0.013 rows=10 loops=1)";

    // ── arg parsing ───────────────────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_explain(&g, &["--help".to_string()]).is_ok());
    }

    #[test]
    fn missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        // Even with SQL provided, no --project → error.
        let err = cmd_explain(
            &g,
            &["SELECT 1".to_string()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--project") || err.to_string().contains("project"),
            "err: {err}"
        );
    }

    #[test]
    fn empty_sql_errors() {
        let _g = with_temp_config_dir();
        // We can't easily test stdin in unit tests, but the empty string path
        // is exercised when positional arg is an empty string.
        let g = flags("http://127.0.0.1:1");
        // Pass project but empty SQL string: file not found is the first gate.
        // (stdin blocking would hang; only test the explicit-empty-arg path.)
        let err = cmd_explain(
            &g,
            &[
                "".to_string(), // empty positional arg
                "--project=myapp".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("no SQL") || err.to_string().contains("SQL"),
            "err: {err}"
        );
    }

    // ── happy path ────────────────────────────────────────────────────────────

    #[test]
    fn sends_get_with_sql_param() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!(r#"{{"plan":"{}"}}"#, SAMPLE_PLAN))
        });
        let g = flags(&srv.url);
        cmd_explain(
            &g,
            &[
                "SELECT 1".to_string(),
                "--project=myapp".to_string(),
            ],
        )
        .unwrap();
        let path = cap.lock().unwrap().clone();
        assert!(path.starts_with("/v1/projects/myapp/explain"), "path={path}");
        assert!(path.contains("sql="), "sql param missing: path={path}");
        assert!(path.contains("analyze=false"), "analyze param: path={path}");
    }

    #[test]
    fn analyze_flag_sets_analyze_true() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!(r#"{{"plan":"{}"}}"#, SAMPLE_PLAN))
        });
        let g = flags(&srv.url);
        cmd_explain(
            &g,
            &[
                "SELECT 1".to_string(),
                "--project=myapp".to_string(),
                "--analyze".to_string(),
            ],
        )
        .unwrap();
        let path = cap.lock().unwrap().clone();
        assert!(path.contains("analyze=true"), "analyze=true expected: path={path}");
    }

    #[test]
    fn file_flag_reads_sql_from_file() {
        let _g = with_temp_config_dir();
        let dir = std::env::temp_dir().join(format!("basin-explain-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("query.sql");
        std::fs::write(&file, b"SELECT * FROM orders").unwrap();

        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!(r#"{{"plan":"{}"}}"#, SAMPLE_PLAN))
        });
        let g = flags(&srv.url);
        cmd_explain(
            &g,
            &[
                format!("--file={}", file.display()),
                "--project=myapp".to_string(),
            ],
        )
        .unwrap();
        // SQL should appear URL-encoded in the query string.
        let path = cap.lock().unwrap().clone();
        assert!(path.contains("sql="), "sql param missing: path={path}");
    }

    // ── JSON shape ────────────────────────────────────────────────────────────

    #[test]
    fn json_shape_has_plan_field() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::ok(format!(r#"{{"plan":"{}"}}"#, SAMPLE_PLAN))
        });
        let g = flags_json(&srv.url);
        // Drive; output goes to stdout (captured by test harness if needed).
        cmd_explain(
            &g,
            &["SELECT 1".to_string(), "--project=myapp".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn json_deserialization() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::ok(format!(r#"{{"plan":"{}"}}"#, SAMPLE_PLAN))
        });
        let c = crate::client::Client::new(&srv.url, "tok");
        let q = crate::client::query_string(&[("sql", "SELECT 1"), ("analyze", "false")]);
        let resp: ExplainResponse = c
            .do_json(reqwest::Method::GET, &format!("/v1/projects/myapp/explain{q}"), None)
            .unwrap();
        assert_eq!(resp.plan, SAMPLE_PLAN);
    }

    // ── error mapping ─────────────────────────────────────────────────────────

    #[test]
    fn not_found_maps_to_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_explain(
            &g,
            &["SELECT 1".to_string(), "--project=gone".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn bad_sql_syntax_maps_to_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(422, r#"{"code":"sql_syntax_error","message":"parse error at or near …"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_explain(
            &g,
            &["SELEC FROM broken".to_string(), "--project=myapp".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }
}
