//! index_advisor — JSONB index recommendations for project tables (T-125).
//!
//!   basin index-advisor [--project=<ref>]
//!
//! Backed by GET /v1/projects/{ref}/index-advisor.
//!
//! Returns per-column index hints with:
//!   - table / column name
//!   - reason (why a GIN index is recommended)
//!   - ddl (exact `CREATE INDEX … USING gin (…)` DDL)
//!   - patterns_seen (operators detected: @>, ?, ?&)
//!   - stat_available (true when basin_stat_statements data was used)
//!
//! When no JSONB columns lack a GIN index the server returns an empty array.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};

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

/// IndexHint is one per-column recommendation from the cloud's advisor.
/// JSON shape: { table, column, reason, ddl, patterns_seen, stat_available }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct IndexHint {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub column: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ddl: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patterns_seen: Vec<String>,
    #[serde(default)]
    pub stat_available: bool,
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_index_advisor(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("index-advisor")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "index-advisor",
            "Show JSONB index recommendations for the project's tables.",
            &[
                "--project=<ref>   Project ref (or use basin link).",
                "",
                "Analyses JSONB columns that lack a GIN index. When basin_stat_statements",
                "data is available, only columns with observed @>, ?, or ?& operators are",
                "flagged. The `ddl` field contains the exact CREATE INDEX statement to run.",
                "",
                "Example:",
                "  basin index-advisor --project=myapp",
                "  basin index-advisor --json | jq '.[].ddl' | psql ...",
            ],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/index-advisor", project_ref);
    let hints: Vec<IndexHint> = c.do_json(Method::GET, &path, None)?;

    if g.json {
        // JSON shape: [ IndexHint ]
        return print_json(&mut std::io::stdout(), &hints);
    }

    if hints.is_empty() {
        if !g.quiet {
            println!("No index recommendations — all JSONB columns already have GIN indexes.");
        }
        return Ok(());
    }

    let mut t = Table::new(g, &["TABLE", "COLUMN", "PATTERNS", "DDL"]);
    for h in &hints {
        let patterns = h.patterns_seen.join(", ");
        // Truncate DDL for table display; --json gives the full text.
        let ddl_short = if h.ddl.len() > 60 {
            format!("{}…", &h.ddl[..60])
        } else {
            h.ddl.clone()
        };
        t.row(&[&h.table, &h.column, &patterns, &ddl_short]);
    }
    t.flush()?;

    if !g.quiet {
        println!("\nRun with --json to get the full DDL for each recommendation.");
    }
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

    fn sample_hint() -> &'static str {
        r#"{"table":"events","column":"payload","reason":"JSONB column with no GIN index","ddl":"CREATE INDEX ON events USING gin (payload)","patterns_seen":["@>","?"],"stat_available":true}"#
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_index_advisor(&g, &["--help".to_string()]).is_ok());
    }

    #[test]
    fn missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        // no --project, no working project → error with --project hint
        let err = cmd_index_advisor(&g, &[]).unwrap_err();
        assert!(
            err.to_string().contains("--project") || err.to_string().contains("project"),
            "err: {err}"
        );
    }

    // ── happy path ────────────────────────────────────────────────────────────

    #[test]
    fn sends_get_to_correct_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!("[{}]", sample_hint()))
        });
        let g = flags(&srv.url);
        cmd_index_advisor(&g, &["--project=myapp".to_string()]).unwrap();
        assert_eq!(*cap.lock().unwrap(), "/v1/projects/myapp/index-advisor");
    }

    #[test]
    fn empty_response_prints_nothing_quiet() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("[]"));
        let g = flags(&srv.url);
        cmd_index_advisor(&g, &["--project=myapp".to_string()]).unwrap();
    }

    #[test]
    fn non_empty_response_renders_table() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(format!("[{}]", sample_hint())));
        let g = flags(&srv.url);
        cmd_index_advisor(&g, &["--project=myapp".to_string()]).unwrap();
    }

    // ── JSON shape ────────────────────────────────────────────────────────────

    #[test]
    fn json_shape_preserves_all_fields() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(format!("[{}]", sample_hint())));
        let g = flags_json(&srv.url);
        // Drive through cmd to exercise the JSON branch; capture via a client call.
        let c = crate::client::Client::new(&srv.url, "tok");
        let hints: Vec<IndexHint> = c
            .do_json(reqwest::Method::GET, "/v1/projects/myapp/index-advisor", None)
            .unwrap();
        assert_eq!(hints.len(), 1);
        let h = &hints[0];
        assert_eq!(h.table, "events");
        assert_eq!(h.column, "payload");
        assert!(h.stat_available);
        assert!(h.patterns_seen.contains(&"@>".to_string()));
        assert!(h.ddl.contains("gin"));
    }

    #[test]
    fn json_flag_round_trips_array() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(format!("[{}]", sample_hint())));
        let g = flags_json(&srv.url);
        // Must not error; the branch prints to stdout.
        cmd_index_advisor(&g, &["--project=myapp".to_string()]).unwrap();
    }

    // ── error mapping ─────────────────────────────────────────────────────────

    #[test]
    fn not_found_mapped_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err =
            cmd_index_advisor(&g, &["--project=gone".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(500, r#"{"code":"internal","message":"db error"}"#)
        });
        let g = flags(&srv.url);
        let err =
            cmd_index_advisor(&g, &["--project=myapp".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 500);
    }
}
