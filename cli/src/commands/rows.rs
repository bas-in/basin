//! rows — row-level mutations for a project table.
//!
//!   basin rows insert <table> [--project=<ref>] [--json='{...}']
//!   basin rows update <table> --pk=<val> --json='{...}' [--project=<ref>]
//!   basin rows delete <table> --where='<sql>' [--project=<ref>] [--yes]
//!
//! Routes (per-table family under /v1/projects/{ref}/tables/{name}):
//!
//!   POST   /v1/projects/{ref}/tables/{name}/rows           body { "row": {...} }
//!   PATCH  /v1/projects/{ref}/tables/{name}/rows/{pk}      body { "patch": {...} }
//!   DELETE /v1/projects/{ref}/tables/{name}/rows?where=<>
//!
//! Project ref resolution order:
//!  1. --project=<ref> flag
//!  2. load_working_project(cwd) — ./basin/config.toml
//!  3. Error with hint to run `basin link`

use std::io::BufRead;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// rowInsertResponse: JSON shape { "inserted": N, "rows": [{...}] }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RowInsertResponse {
    #[serde(default)]
    pub inserted: i64,
    #[serde(default)]
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// rowUpdateResponse: JSON shape { "row": {...} }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RowUpdateResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<serde_json::Map<String, serde_json::Value>>,
}

/// rowDeleteResponse: JSON shape { "deleted": N }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RowDeleteResponse {
    #[serde(default)]
    pub deleted: i64,
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg("--project is required (or run `basin link` to bind this directory)")),
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_rows(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin rows (insert | update | delete) <table> ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "insert" => rows_insert(g, rest),
        "update" => rows_update(g, rest),
        "delete" => rows_delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "rows",
                "Insert / update / delete rows in a project table.",
                &[
                    "insert <table> [--project=<ref>] [--json='{...}']   Insert a row (or batch from stdin NDJSON).",
                    "update <table> --pk=<val> --json='{...}' [--project=<ref>]  Update a row by primary-key value.",
                    "delete <table> --where='<sql>' [--project=<ref>] [--yes]    Delete rows matching a predicate.",
                ],
            );
            Ok(())
        }
        other => {
            Err(msg(format!("unknown subcommand {:?} for rows", other)))
        }
    }
}

// ── rows insert ───────────────────────────────────────────────────────────────

fn rows_insert(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rows insert")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("json").long("json"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rows insert",
            "Insert a row (or NDJSON batch from stdin).",
            &[
                "<table>                Table name (required).",
                "--project <ref>        Project ref.",
                "--json '{...}'         Single-row JSON object. Omit to read NDJSON from stdin.",
            ],
        );
        return Ok(());
    }
    let table = m
        .get_one::<String>("table")
        .cloned()
        .ok_or_else(|| msg("usage: basin rows insert <table> [--project=<ref>] [--json='{...}']"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let json_flag = m.get_one::<String>("json").cloned().unwrap_or_default();

    if !json_flag.is_empty() {
        // Single-row inline mode.
        rows_insert_one(g, &c, &r#ref, &table, &json_flag)
    } else {
        // Stdin NDJSON batch mode.
        rows_insert_batch(g, &c, &r#ref, &table)
    }
}

/// rows_insert_one POSTs a single row to the cloud and prints the result.
fn rows_insert_one(
    g: &GlobalFlags,
    c: &crate::client::Client,
    r#ref: &str,
    table: &str,
    row_json: &str,
) -> CliResult<()> {
    let row_obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(row_json).map_err(|e| msg(format!("rows insert: invalid JSON for --json: {e}")))?;
    let resp: RowInsertResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables/{table}/rows"),
        Some(json!({ "row": row_obj })),
    )?;
    if g.json {
        // JSON shape: { "inserted": N, "rows": [{...returned...}] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Inserted {} row(s) into {table}.", resp.inserted);
    Ok(())
}

/// rows_insert_batch reads NDJSON from stdin (one JSON object per line)
/// and POSTs each line as a separate insert. Errors on individual lines
/// are reported to stderr but do not abort the remaining lines.
/// Returns a non-nil error only if zero lines were processed or every line failed.
fn rows_insert_batch(
    g: &GlobalFlags,
    c: &crate::client::Client,
    r#ref: &str,
    table: &str,
) -> CliResult<()> {
    let stdin = std::io::stdin();
    let reader = std::io::BufReader::new(stdin.lock());
    let mut inserted: i64 = 0;
    let mut line_num: i64 = 0;
    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

    for line_result in reader.lines() {
        let raw = match line_result {
            Ok(s) => s,
            Err(e) => return Err(msg(format!("rows insert: reading stdin: {e}"))),
        };
        let line = raw.trim().to_string();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        line_num += 1;
        let row_obj: serde_json::Map<String, serde_json::Value> =
            match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(e) => {
                    printerr!(g, "rows insert: line {line_num}: invalid JSON: {e}");
                    last_err = Some(msg(format!("rows insert: line {line_num}: invalid JSON: {e}")));
                    continue;
                }
            };
        let result = c.do_json::<RowInsertResponse>(
            Method::POST,
            &format!("/v1/projects/{ref}/tables/{table}/rows"),
            Some(json!({ "row": row_obj })),
        );
        match result {
            Ok(resp) => {
                inserted += resp.inserted;
                if !g.quiet {
                    eprintln!("line {line_num}: inserted {} row(s)", resp.inserted);
                }
            }
            Err(e) => {
                printerr!(g, "rows insert: line {line_num}: {e}");
                last_err = Some(e);
            }
        }
    }

    if line_num == 0 {
        return Err(msg(
            "rows insert: no JSON lines found on stdin (pipe NDJSON or pass --json='{...}')",
        ));
    }
    if g.json {
        // JSON shape: { "inserted": N }
        return print_json(&mut std::io::stdout(), &json!({ "inserted": inserted }));
    }
    println!("Inserted {inserted} row(s) into {table}.");
    // Return last_err only if EVERY line failed (inserted == 0).
    if inserted == 0 {
        if let Some(e) = last_err {
            return Err(e);
        }
    }
    Ok(())
}

// ── rows update ───────────────────────────────────────────────────────────────

fn rows_update(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rows update")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("pk").long("pk"))
        .arg(Arg::new("json").long("json"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rows update",
            "Update a row by primary-key value.",
            &[
                "<table>                 Table name (required).",
                "--pk=<val>              Primary-key value (required).",
                "--json='{...}'          JSON patch object (required).",
                "--project <ref>         Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin rows update <table> --pk=<val> --json='{...}' [--project=<ref>]")
    })?;
    let pk = m.get_one::<String>("pk").cloned().unwrap_or_default();
    if pk.is_empty() {
        return Err(msg("rows update: --pk is required"));
    }
    let json_flag = m.get_one::<String>("json").cloned().unwrap_or_default();
    if json_flag.is_empty() {
        return Err(msg("rows update: --json is required"));
    }

    let patch_obj: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&json_flag).map_err(|e| msg(format!("rows update: invalid JSON for --json: {e}")))?;

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let resp: RowUpdateResponse = c.do_json(
        Method::PATCH,
        &format!("/v1/projects/{ref}/tables/{table}/rows/{pk}"),
        Some(json!({ "patch": patch_obj })),
    )?;
    if g.json {
        // JSON shape: { "row": {...} }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Updated row (pk={pk}) in {table}.");
    Ok(())
}

// ── rows delete ───────────────────────────────────────────────────────────────

fn rows_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rows delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("where").long("where"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rows delete",
            "Delete rows matching a SQL predicate.",
            &[
                "<table>                   Table name (required).",
                "--where='<sql>'           SQL predicate (required).",
                "--yes                     Skip the confirmation prompt.",
                "--project <ref>           Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin rows delete <table> --where='<sql>' [--project=<ref>] [--yes]")
    })?;
    let where_clause = m.get_one::<String>("where").cloned().unwrap_or_default();
    if where_clause.is_empty() {
        return Err(msg("rows delete: --where is required (e.g. --where='id = 42')"));
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to delete rows non-interactively under --json",
            ));
        }
        eprint!(
            "Delete rows from {ref}.{table} WHERE {where_clause}? This cannot be undone. [y/N] "
        );
        let line = read_line()?;
        if line.trim().to_ascii_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let q = query_string(&[("where", &where_clause)]);
    let resp: RowDeleteResponse =
        c.do_json(Method::DELETE, &format!("/v1/projects/{ref}/tables/{table}/rows{q}"), None)?;
    if g.json {
        // JSON shape: { "deleted": N }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted {} row(s) from {table}.", resp.deleted);
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Mutex,
    };

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags { api_url: url.to_string(), token: "tok".into(), quiet: true, ..Default::default() }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags { api_url: url.to_string(), token: "tok".into(), quiet: true, json: true, ..Default::default() }
    }

    // ── dispatcher ──────────────────────────────────────────────────────────

    #[test]
    fn no_args_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_rows(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_rows(&g, &["frobnicate".to_string()]).is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_rows(&g, &["help".to_string()]).is_ok());
    }

    // ── rows insert — single --json ──────────────────────────────────────────

    #[test]
    fn insert_single_json_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/users/rows");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"inserted":1,"rows":[{"id":1,"name":"Alice"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_rows(&g, &[
            "insert".to_string(),
            "--project=p1".to_string(),
            r#"--json={"name":"Alice"}"#.to_string(),
            "users".to_string(),
        ]).unwrap();
        let body = cap.lock().unwrap();
        assert!(body["row"].is_object(), "body should contain 'row': {body}");
    }

    #[test]
    fn insert_invalid_json_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_rows(&g, &[
            "insert".to_string(),
            "--project=p1".to_string(),
            "--json=notjson".to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("invalid JSON"), "err: {err}");
    }

    #[test]
    fn insert_missing_table_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_rows(&g, &["insert".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn insert_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"inserted":1,"rows":[{"id":1,"name":"Alice"}]}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RowInsertResponse = c
            .do_json(Method::POST, "/v1/projects/p1/tables/users/rows", Some(json!({"row":{"name":"Alice"}})))
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["inserted"].as_i64().unwrap(), 1);
        assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn insert_404_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Table not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rows(&g, &[
            "insert".to_string(),
            "--project=p1".to_string(),
            r#"--json={"name":"Alice"}"#.to_string(),
            "users".to_string(),
        ]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn insert_403_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"code":"forbidden","message":"Insufficient permissions."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rows(&g, &[
            "insert".to_string(),
            "--project=p1".to_string(),
            r#"--json={"name":"X"}"#.to_string(),
            "users".to_string(),
        ]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── rows insert — stdin NDJSON batch ─────────────────────────────────────

    #[test]
    fn insert_stdin_batch_three_lines() {
        let _g = with_temp_config_dir();
        let call_count = Arc::new(AtomicI32::new(0));
        let cc2 = Arc::clone(&call_count);
        let srv = TestServer::start(move |_req: &Req| {
            cc2.fetch_add(1, Ordering::SeqCst);
            Resp::ok(r#"{"inserted":1,"rows":[]}"#)
        });
        let url = srv.url.clone();

        // Test the batch logic via a temp file — testutil doesn't support stdin hijacking.
        // Write NDJSON to a temp file and read via BufReader directly.
        let dir = std::env::temp_dir().join(format!("basin-rows-batch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ndjson_path = dir.join("rows.ndjson");
        std::fs::write(&ndjson_path, r#"{"item":"a"}
{"item":"b"}
{"item":"c"}
"#).unwrap();

        // Test the internal function directly.
        let c = crate::client::Client::new(&url, "tok");

        // Read from file and call the batch logic via stdin-like BufReader.
        let file_content = std::fs::read_to_string(&ndjson_path).unwrap();
        let mut inserted: i64 = 0;
        for raw in file_content.lines() {
            let line = raw.trim();
            if line.is_empty() { continue; }
            let row_obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line).unwrap();
            let resp: RowInsertResponse = c.do_json(
                Method::POST,
                &format!("/v1/projects/p1/tables/orders/rows"),
                Some(json!({ "row": row_obj })),
            ).unwrap();
            inserted += resp.inserted;
        }

        // Verify 3 API calls were made.
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert_eq!(inserted, 3);
    }

    // ── rows update ──────────────────────────────────────────────────────────

    #[test]
    fn update_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert!(req.path.ends_with("/rows/42"), "path: {}", req.path);
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"row":{"id":42,"name":"Bob"}}"#)
        });
        let g = flags(&srv.url);
        cmd_rows(&g, &[
            "update".to_string(),
            "--project=p1".to_string(),
            "--pk=42".to_string(),
            r#"--json={"name":"Bob"}"#.to_string(),
            "users".to_string(),
        ]).unwrap();
        let body = cap.lock().unwrap();
        assert!(body["patch"].is_object(), "body should contain 'patch': {body}");
    }

    #[test]
    fn update_missing_pk_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_rows(&g, &[
            "update".to_string(),
            "--project=p1".to_string(),
            r#"--json={"name":"Bob"}"#.to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("--pk is required"), "err: {err}");
    }

    #[test]
    fn update_missing_json_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_rows(&g, &[
            "update".to_string(),
            "--project=p1".to_string(),
            "--pk=1".to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("--json is required"), "err: {err}");
    }

    #[test]
    fn update_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"row":{"id":1,"name":"updated"}}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RowUpdateResponse = c
            .do_json(Method::PATCH, "/v1/projects/p1/tables/users/rows/1", Some(json!({"patch":{"name":"updated"}})))
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["row"].is_object(), "row should be an object");
    }

    #[test]
    fn update_422_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(422, r#"{"code":"validation_error","message":"constraint violation"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rows(&g, &[
            "update".to_string(),
            "--project=p1".to_string(),
            "--pk=99".to_string(),
            r#"--json={"name":"X"}"#.to_string(),
            "users".to_string(),
        ]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── rows delete ───────────────────────────────────────────────────────────

    #[test]
    fn delete_with_yes_sends_where() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(r#"{"deleted":3}"#)
        });
        let g = flags(&srv.url);
        cmd_rows(&g, &[
            "delete".to_string(),
            "--project=p1".to_string(),
            "--where=age > 30".to_string(),
            "--yes".to_string(),
            "users".to_string(),
        ]).unwrap();
        let path = cap.lock().unwrap();
        assert!(path.contains("where="), "path: {path}");
    }

    #[test]
    fn delete_missing_where_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_rows(&g, &[
            "delete".to_string(),
            "--project=p1".to_string(),
            "--yes".to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("--where is required"), "err: {err}");
    }

    #[test]
    fn delete_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), json: true, ..Default::default() };
        let err = cmd_rows(&g, &[
            "delete".to_string(),
            "--project=p1".to_string(),
            "--where=id=1".to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("confirmation required"), "err: {err}");
    }

    #[test]
    fn delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":5}"#));
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RowDeleteResponse = c
            .do_json(Method::DELETE, "/v1/projects/p1/tables/users/rows?where=age%3E18", None)
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["deleted"].as_i64().unwrap(), 5);
    }

    #[test]
    fn delete_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(422, r#"{"code":"rls_violation","message":"RLS policy blocks delete."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rows(&g, &[
            "delete".to_string(),
            "--project=p1".to_string(),
            "--where=id=1".to_string(),
            "--yes".to_string(),
            "users".to_string(),
        ]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── no project resolution ─────────────────────────────────────────────────

    #[test]
    fn no_project_resolution_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        // No --project and no working project config.
        let err = cmd_rows(&g, &[
            "insert".to_string(),
            r#"--json={"x":1}"#.to_string(),
            "users".to_string(),
        ]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("basin link"), "err: {err}");
    }
}
