//! sql — `basin sql --project <ref> [-e "SELECT 1" | -f query.sql | <stdin>]`
//!
//! SQL source priority: -e/--exec inline, then -f/--file, then stdin.
//! Empty input is rejected before we hit the network.

use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_all_stdin, read_maybe_file, stringify_cell, Table};
use crate::types::QueryResult;

use super::help::help_for_command;
use super::parse_or_silent;

pub fn cmd_sql(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("sql")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("exec").long("exec").short('e'))
        .arg(Arg::new("file").long("file").short('f'))
        .arg(Arg::new("writes").long("writes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "sql",
            "Run a SQL query against a project's basin engine.",
            &[
                "--project=<ref>      Project ref (required).",
                "-e/--exec '<sql>'    Inline query.",
                "-f/--file <path>     Read SQL from file.",
                "--writes             Permit DML/DDL (defaults to read-only).",
                "(if neither -e nor -f is set, SQL is read from stdin)",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }

    let mut sql_body = m.get_one::<String>("exec").cloned().unwrap_or_default();
    if sql_body.is_empty() {
        if let Some(path) = m.get_one::<String>("file") {
            sql_body = read_maybe_file(path).map_err(|e| msg(format!("read {path}: {e}")))?;
        }
    }
    if sql_body.is_empty() {
        sql_body = read_all_stdin()?;
    }
    let sql_body = sql_body.trim().to_string();
    if sql_body.is_empty() {
        return Err(msg("no SQL provided (use -e, -f, or pipe via stdin)"));
    }

    let c = require_client(g)?;
    let writes = m.get_flag("writes");
    let body = json!({ "sql": sql_body, "writes_enabled": writes });
    let res: QueryResult = c.do_json_timeout(
        Method::POST,
        &format!("/v1/projects/{project}/sql/query"),
        Some(body),
        Duration::from_secs(120),
    )?;
    if g.json {
        // JSON shape: QueryResult — { columns: [string], rows: [[any]],
        // rows_affected: int?, elapsed_ms: int }
        return print_json(&mut std::io::stdout(), &res);
    }
    render_rows(
        g,
        &res.columns,
        &res.rows,
        res.rows_affected,
        res.elapsed_ms,
    )
}

/// render_rows is the table-output path shared by `basin sql` and table
/// row dumps. Empty result sets emit a tidy "(no rows)".
pub fn render_rows(
    g: &GlobalFlags,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    affected: Option<i64>,
    elapsed_ms: i64,
) -> CliResult<()> {
    if columns.is_empty() && rows.is_empty() {
        match affected {
            Some(n) => println!("{n} row(s) affected ({elapsed_ms}ms)"),
            None => println!("(no rows; {elapsed_ms}ms)"),
        }
        return Ok(());
    }
    let mut headers: Vec<String> = columns.to_vec();
    if headers.is_empty() && !rows.is_empty() {
        for i in 0..rows[0].len() {
            headers.push(format!("col{}", i + 1));
        }
    }
    let header_refs: Vec<&str> = headers.iter().map(|s| s.as_str()).collect();
    let mut t = Table::new(g, &header_refs);
    for r in rows {
        let mut cells = Vec::with_capacity(headers.len());
        for i in 0..headers.len() {
            cells.push(r.get(i).map(stringify_cell).unwrap_or_default());
        }
        t.row_strings(cells);
    }
    t.flush()?;
    if !g.quiet {
        eprintln!("({} row(s) · {}ms)", rows.len(), elapsed_ms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{Arc, Mutex};

    fn stub_capturing(captured: Arc<Mutex<String>>) -> TestServer {
        TestServer::start(move |req: &Req| {
            assert_eq!(req.path, "/v1/projects/p1/sql/query");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            if let Some(s) = body.get("sql").and_then(|v| v.as_str()) {
                *captured.lock().unwrap() = s.to_string();
            }
            Resp::ok(r#"{"columns":["x"],"rows":[[1]],"elapsed_ms":1}"#)
        })
    }

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn inline_exec_is_sent() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let srv = stub_capturing(Arc::clone(&cap));
        cmd_sql(
            &flags(&srv.url),
            &["--project=p1".into(), "-e".into(), "SELECT 1".into()],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "SELECT 1");
    }

    #[test]
    fn inline_beats_file() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let srv = stub_capturing(Arc::clone(&cap));
        let dir = std::env::temp_dir().join(format!("sql-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("q.sql");
        std::fs::write(&path, "FROM_FILE").unwrap();
        cmd_sql(
            &flags(&srv.url),
            &[
                "--project=p1".into(),
                "-e".into(),
                "INLINE".into(),
                "-f".into(),
                path.display().to_string(),
            ],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "INLINE");
    }

    #[test]
    fn requires_project() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_sql(&g, &["-e".into(), "SELECT 1".into()]).is_err());
    }
}
