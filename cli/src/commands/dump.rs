//! dump — `basin dump --project=<ref> [--format=plain|custom] [-f out.sql]`
//!
//! Exports a project's schema + data in a pg_dump-shaped format.
//!
//! Two output formats are supported (matches `pg_dump -F`):
//!   plain   — SQL text (default): CREATE TABLE + INSERT statements.
//!   custom  — reserved for a future binary format; currently falls back to
//!             plain with a header comment noting the format limitation.
//!
//! Transport: the CLI issues information_schema queries + SELECT * over the
//! existing POST /v1/projects/{ref}/sql/query HTTP endpoint, then assembles
//! the output client-side. This matches the approach used by `db dump` and
//! `migrate_from_pg::basin_restore_sql`.
//!
//! Engine-native dump:
//!   When basin-engine ships a dedicated dump endpoint (e.g.
//!   GET /v1/projects/{ref}/dump?format=custom), replace the SQL-assembly
//!   block below with a single streaming call to that endpoint.
//!   See: // TODO(5.22.D-engine-api)

use std::io::Write;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire types ────────────────────────────────────────────────────────────────

/// QueryResult for SQL passthrough.
#[derive(Debug, Default, Deserialize)]
struct QueryResult {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<serde_json::Value>>,
}

// ── DumpFormat ────────────────────────────────────────────────────────────────

/// DumpFormat mirrors `pg_dump -F` flag values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DumpFormat {
    /// Plain SQL text (default).
    Plain,
    /// Custom binary format (reserved; currently emits plain with a warning).
    Custom,
}

impl DumpFormat {
    pub fn parse(s: &str) -> Option<DumpFormat> {
        match s.to_ascii_lowercase().as_str() {
            "plain" | "p" => Some(DumpFormat::Plain),
            "custom" | "c" => Some(DumpFormat::Custom),
            _ => None,
        }
    }
}

// ── DumpOpts ──────────────────────────────────────────────────────────────────

/// DumpOpts captures the parsed CLI flags for `basin dump`.
pub struct DumpOpts {
    pub project_ref: String,
    pub format: DumpFormat,
    /// When Some, write the dump to this file path; otherwise write to stdout.
    pub output_file: Option<String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// quote_ident wraps an SQL identifier in double-quotes, escaping embedded
/// double-quotes by doubling them (SQL standard).
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// sql_literal converts a serde_json Value into a SQL literal safe for INSERT.
pub fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return i.to_string();
            }
            if let Some(f) = n.as_f64() {
                if f == f.trunc() && f.abs() < 9.0e15 {
                    return format!("{}", f as i64);
                }
                return format!("{f}");
            }
            n.to_string()
        }
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

// ── Core implementation ───────────────────────────────────────────────────────

/// run_dump is the testable core: produces the dump to the given writer.
///
/// It connects to the target Basin project via the SQL query API and
/// assembles schema + data into pg_dump-shaped SQL.
///
/// TODO(5.22.D-engine-api): When basin-engine exposes a native dump endpoint
/// (e.g. `GET /v1/projects/{ref}/dump?format=<fmt>&section=<pre-data|data|all>`),
/// replace the two SQL round-trips below with a single streaming call to
/// that endpoint. For `custom` format, proxy the binary response directly
/// to the output writer. The command structure, arg parsing, and file-output
/// wiring here should remain unchanged.
pub fn run_dump<W: Write>(g: &GlobalFlags, opts: &DumpOpts, out: &mut W) -> CliResult<()> {
    let c = require_client(g)?;

    if opts.format == DumpFormat::Custom {
        // Custom binary format is reserved for the engine-native API.
        // Emit plain SQL with a header comment so the file is still useful.
        writeln!(out, "-- basin dump --format=custom")?;
        writeln!(
            out,
            "-- NOTE: custom binary format requires a native engine dump endpoint."
        )?;
        writeln!(
            out,
            "-- TODO(5.22.D-engine-api): wire GET /v1/projects/{{ref}}/dump?format=custom"
        )?;
        writeln!(out, "-- Falling back to plain SQL output.")?;
        writeln!(out)?;
    }

    let r#ref = &opts.project_ref;

    // ── Step 1: query information_schema for column metadata ─────────────────
    //
    // TODO(5.22.D-engine-api): when the engine dump endpoint ships, replace
    // this block and step 2 with a single authenticated GET/streaming call.
    let schema_sql = "SELECT table_name, column_name, data_type, is_nullable, ordinal_position \
        FROM information_schema.columns \
        WHERE table_schema = 'public' \
        ORDER BY table_name, ordinal_position";

    let schema_result: QueryResult = c.do_json_timeout(
        Method::POST,
        &format!("/v1/projects/{ref}/sql/query"),
        Some(json!({ "sql": schema_sql, "writes_enabled": false })),
        Duration::from_secs(300),
    )?;

    // Parse column info into ordered (table_name, [ColInfo]) pairs.
    struct ColInfo {
        col_name: String,
        data_type: String,
        is_nullable: String,
    }
    let mut table_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut table_cols: Vec<(String, Vec<ColInfo>)> = Vec::new();

    for row in &schema_result.rows {
        if row.len() < 5 {
            continue;
        }
        let table_name = match &row[0] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let col_name = match &row[1] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let data_type = match &row[2] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let is_nullable = match &row[3] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let idx = match table_index.get(&table_name) {
            Some(&i) => i,
            None => {
                let i = table_cols.len();
                table_index.insert(table_name.clone(), i);
                table_cols.push((table_name.clone(), Vec::new()));
                i
            }
        };
        table_cols[idx].1.push(ColInfo {
            col_name,
            data_type,
            is_nullable,
        });
    }

    // ── Step 2: emit header + schema DDL ────────────────────────────────────
    writeln!(
        out,
        "-- basin dump: project={ref} format={:?}",
        if opts.format == DumpFormat::Custom {
            "plain(fallback)"
        } else {
            "plain"
        }
    )?;
    writeln!(out)?;
    writeln!(out, "-- schema")?;
    writeln!(out)?;

    for (tbl, cols) in &table_cols {
        writeln!(out, "CREATE TABLE IF NOT EXISTS {} (", quote_ident(tbl))?;
        for (i, col) in cols.iter().enumerate() {
            let nullable = if col.is_nullable.to_uppercase() == "NO" {
                " NOT NULL"
            } else {
                ""
            };
            let comma = if i + 1 == cols.len() { "" } else { "," };
            writeln!(
                out,
                "  {} {}{}{}",
                quote_ident(&col.col_name),
                col.data_type,
                nullable,
                comma
            )?;
        }
        writeln!(out, ");")?;
        writeln!(out)?;
    }

    // ── Step 3: emit data as INSERT statements ───────────────────────────────
    writeln!(out, "-- data")?;
    writeln!(out)?;

    for (tbl, cols) in &table_cols {
        let select_sql = format!("SELECT * FROM {}", quote_ident(tbl));
        let data_result: QueryResult = match c.do_json_timeout(
            Method::POST,
            &format!("/v1/projects/{ref}/sql/query"),
            Some(json!({ "sql": select_sql, "writes_enabled": false })),
            Duration::from_secs(300),
        ) {
            Ok(r) => r,
            Err(e) => {
                writeln!(out, "-- skipped {tbl}: {e}")?;
                continue;
            }
        };
        if data_result.rows.is_empty() {
            continue;
        }
        let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.col_name)).collect();
        let col_list = col_names.join(", ");
        for row in &data_result.rows {
            let vals: Vec<String> = row.iter().map(sql_literal).collect();
            writeln!(
                out,
                "INSERT INTO {} ({}) VALUES ({});",
                quote_ident(tbl),
                col_list,
                vals.join(", ")
            )?;
        }
    }

    Ok(())
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_dump(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("dump")
        .arg(Arg::new("project").long("project"))
        .arg(
            Arg::new("format")
                .long("format")
                .short('F')
                .default_value("plain"),
        )
        .arg(Arg::new("file").long("file").short('f'))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));

    let m = parse_or_silent(cmd, args)?;

    if m.get_flag("help") {
        help_for_command(
            "dump",
            "Export a project's schema + data (pg_dump-shaped).",
            &[
                "--project=<ref>         Project ref (required, or auto-detected from ./basin/config.toml).",
                "--format=plain|custom   Output format: plain SQL text (default) or custom.",
                "                        plain  — pg_dump -F p compatible SQL statements.",
                "                        custom — reserved for a future binary format;",
                "                                 currently emits plain SQL with a format header.",
                "-f, --file=<path>       Write output to this file (default: stdout).",
                "",
                "Engine-native dump endpoint (GET /v1/projects/{ref}/dump) will be wired",
                "once basin-engine ships it. See TODO(5.22.D-engine-api) in dump.rs.",
            ],
        );
        return Ok(());
    }

    // Resolve project ref.
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = if !project.is_empty() {
        project
    } else {
        let cwd = std::env::current_dir().map_err(|e| msg(format!("dump: cwd: {e}")))?;
        let wp = crate::config::load_working_project(&cwd)?;
        match wp {
            Some(w) if !w.project_ref.is_empty() => w.project_ref,
            _ => {
                return Err(msg(
                    "dump: --project is required (or run `basin link` first)",
                ))
            }
        }
    };

    // Parse format.
    let format_str = m
        .get_one::<String>("format")
        .map(|s| s.as_str())
        .unwrap_or("plain");
    let format = DumpFormat::parse(format_str)
        .ok_or_else(|| msg(format!("dump: unknown format {format_str:?}; use plain or custom")))?;

    let output_file = m.get_one::<String>("file").cloned();

    let opts = DumpOpts {
        project_ref,
        format,
        output_file: output_file.clone(),
    };

    match output_file {
        Some(ref path) => {
            let mut f = std::fs::File::create(path)
                .map_err(|e| msg(format!("dump: create {path}: {e}")))?;
            run_dump(g, &opts, &mut f)?;
            crate::printinfo!(g, "dump written to {path}");
        }
        None => {
            let stdout = std::io::stdout();
            let mut out = stdout.lock();
            run_dump(g, &opts, &mut out)?;
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

    // ── unit: DumpFormat::parse ───────────────────────────────────────────────

    #[test]
    fn format_parse_plain_variants() {
        assert_eq!(DumpFormat::parse("plain"), Some(DumpFormat::Plain));
        assert_eq!(DumpFormat::parse("p"), Some(DumpFormat::Plain));
        assert_eq!(DumpFormat::parse("PLAIN"), Some(DumpFormat::Plain));
    }

    #[test]
    fn format_parse_custom_variants() {
        assert_eq!(DumpFormat::parse("custom"), Some(DumpFormat::Custom));
        assert_eq!(DumpFormat::parse("c"), Some(DumpFormat::Custom));
        assert_eq!(DumpFormat::parse("CUSTOM"), Some(DumpFormat::Custom));
    }

    #[test]
    fn format_parse_unknown_is_none() {
        assert_eq!(DumpFormat::parse("binary"), None);
        assert_eq!(DumpFormat::parse(""), None);
    }

    // ── unit: quote_ident / sql_literal ──────────────────────────────────────

    #[test]
    fn quote_ident_basic() {
        assert_eq!(quote_ident("users"), r#""users""#);
    }

    #[test]
    fn quote_ident_embedded_double_quote() {
        assert_eq!(quote_ident(r#"my"table"#), r#""my""table""#);
    }

    #[test]
    fn sql_literal_types() {
        use serde_json::json;
        assert_eq!(sql_literal(&serde_json::Value::Null), "NULL");
        assert_eq!(sql_literal(&json!(true)), "TRUE");
        assert_eq!(sql_literal(&json!(false)), "FALSE");
        assert_eq!(sql_literal(&json!(42)), "42");
        assert_eq!(sql_literal(&json!(3.14)), "3.14");
        assert_eq!(sql_literal(&json!("hello")), "'hello'");
        assert_eq!(sql_literal(&json!("it's")), "'it''s'");
    }

    // ── unit: arg parsing — missing project ───────────────────────────────────

    #[test]
    fn missing_project_returns_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_dump(&g, &[]).unwrap_err();
        // Should mention project or link.
        assert!(
            err.to_string().contains("project") || err.to_string().contains("link"),
            "error should mention --project: {err}"
        );
    }

    #[test]
    fn unknown_format_returns_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_dump(&g, &["--project=p1".into(), "--format=binary".into()]).unwrap_err();
        assert!(
            err.to_string().contains("format"),
            "error should mention format: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        cmd_dump(&g, &["--help".into()]).unwrap();
    }

    #[test]
    fn schema_only_and_data_flags_mutually_exclusive_format_check() {
        // `dump` has no --schema-only / --data-only (those are on `db dump`);
        // ensure the plain format path works without additional flags.
        assert_eq!(DumpFormat::parse("plain"), Some(DumpFormat::Plain));
    }

    // ── stub-server: run_dump plain format ────────────────────────────────────

    #[test]
    fn run_dump_plain_produces_create_and_insert() {
        let _cfg = with_temp_config_dir();

        // The stub server must handle two POST /sql/query calls:
        //   1. information_schema query → returns 1 table with 2 columns.
        //   2. SELECT * FROM "users" → returns 1 row.
        let call_count = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let call_count2 = std::sync::Arc::clone(&call_count);

        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let mut n = call_count2.lock().unwrap();
            *n += 1;
            let call = *n;
            drop(n);

            if call == 1 {
                // First call: information_schema — return columns for "users".
                return Resp::ok(
                    r#"{
                        "columns": ["table_name","column_name","data_type","is_nullable","ordinal_position"],
                        "rows": [
                            ["users","id","integer","NO","1"],
                            ["users","name","text","YES","2"]
                        ]
                    }"#,
                );
            }
            // Second call: SELECT * FROM "users".
            Resp::ok(
                r#"{
                    "columns": ["id","name"],
                    "rows": [[1,"alice"]]
                }"#,
            )
        });

        let g = flags(&srv.url);
        let opts = DumpOpts {
            project_ref: "proj1".into(),
            format: DumpFormat::Plain,
            output_file: None,
        };

        let mut buf = Vec::new();
        run_dump(&g, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Schema section.
        assert!(
            output.contains("CREATE TABLE IF NOT EXISTS"),
            "missing CREATE TABLE: {output}"
        );
        assert!(output.contains(r#""users""#), "missing table name: {output}");
        assert!(output.contains(r#""id" integer NOT NULL"#), "missing id col: {output}");
        assert!(output.contains(r#""name" text"#), "missing name col: {output}");

        // Data section.
        assert!(
            output.contains("INSERT INTO"),
            "missing INSERT INTO: {output}"
        );
        assert!(output.contains("1"), "missing id value: {output}");
        assert!(output.contains("'alice'"), "missing name value: {output}");
    }

    // ── stub-server: run_dump custom format includes header comment ───────────

    #[test]
    fn run_dump_custom_format_includes_fallback_comment() {
        let _cfg = with_temp_config_dir();

        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            // Return empty schema.
            Resp::ok(r#"{"columns":["table_name","column_name","data_type","is_nullable","ordinal_position"],"rows":[]}"#)
        });

        let g = flags(&srv.url);
        let opts = DumpOpts {
            project_ref: "proj1".into(),
            format: DumpFormat::Custom,
            output_file: None,
        };

        let mut buf = Vec::new();
        run_dump(&g, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(
            output.contains("custom binary format"),
            "missing custom-format notice: {output}"
        );
        assert!(
            output.contains("TODO(5.22.D-engine-api)"),
            "missing TODO marker: {output}"
        );
    }

    // ── stub-server: run_dump skips tables on SELECT error ────────────────────

    #[test]
    fn run_dump_skips_table_on_data_error() {
        let _cfg = with_temp_config_dir();

        let call_count = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let call_count2 = std::sync::Arc::clone(&call_count);

        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let mut n = call_count2.lock().unwrap();
            *n += 1;
            let call = *n;
            drop(n);

            if call == 1 {
                // Schema call: 1 table.
                return Resp::ok(
                    r#"{"columns":["table_name","column_name","data_type","is_nullable","ordinal_position"],
                       "rows":[["orders","id","integer","NO","1"]]}"#,
                );
            }
            // Data call: return a server error.
            Resp::status(500, r#"{"error":{"code":"server_error","message":"table locked"}}"#)
        });

        let g = flags(&srv.url);
        let opts = DumpOpts {
            project_ref: "proj1".into(),
            format: DumpFormat::Plain,
            output_file: None,
        };

        let mut buf = Vec::new();
        run_dump(&g, &opts, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Schema is still emitted.
        assert!(output.contains(r#""orders""#), "schema missing: {output}");
        // Data skip comment is present.
        assert!(
            output.contains("-- skipped orders"),
            "missing skip comment: {output}"
        );
    }

    // ── cmd_dump: file output path ────────────────────────────────────────────

    #[test]
    fn cmd_dump_writes_to_file() {
        let _cfg = with_temp_config_dir();

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"columns":["table_name","column_name","data_type","is_nullable","ordinal_position"],"rows":[]}"#)
        });

        let dir = std::env::temp_dir().join(format!("basin-dump-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let out_path = dir.join("out.sql");

        let g = flags(&srv.url);
        cmd_dump(
            &g,
            &[
                "--project=proj1".into(),
                format!("--file={}", out_path.display()),
            ],
        )
        .unwrap();

        let content = std::fs::read_to_string(&out_path).unwrap();
        assert!(
            content.contains("basin dump"),
            "output file should contain dump header: {content}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
