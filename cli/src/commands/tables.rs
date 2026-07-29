//! tables — list / describe / create / alter / drop / import-csv / export-csv / columns.
//!
//!   basin tables list        --project <ref>
//!   basin tables describe    <name> --project <ref>
//!   basin tables show-rows   <name> --project <ref> [--page N --page-size N]
//!   basin tables create      <name> --column=<spec>... [--project <ref>]
//!   basin tables alter       <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project <ref>]
//!   basin tables drop        <name> [--yes] [--project <ref>]
//!   basin tables import-csv  <name> [--file=<path>] [--header=true|false] [--project <ref>]
//!   basin tables export-csv  <name> [--project <ref>]
//!   basin tables columns add    <table> --column=<spec> [--project <ref>]
//!   basin tables columns alter  <table> --name=<col> [--type=<t>] [--nullable=true|false] [--project <ref>]
//!   basin tables columns drop   <table> --name=<col> [--yes] [--project <ref>]
//!
//! Column spec grammar:
//!   <name>:<type>[,nullable=<bool>][,pk=<bool>][,default=<expr>]

use std::io::Write as IoWrite;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;
use crate::types::TableInfo;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// Column is one column descriptor from GET /v1/projects/{ref}/tables/{name}.
/// JSON shape: { name, type, nullable, pk, default? }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Column {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    pub col_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub pk: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// TableDetail is the nested object in GET /v1/projects/{ref}/tables/{name}.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TableDetail {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default)]
    pub columns: Vec<Column>,
}

/// RowsPage is the response from GET /tables/{name}/rows.
/// JSON shape: { columns: [string], rows: [[any]], total, page, page_size }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RowsPage {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    pub total: i64,
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub page_size: i64,
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml.
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

/// bool_str renders a bool as "yes" or "no" matching Go's boolStr.
fn bool_str(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

/// div_ceil is ceiling integer division, matching Go's divCeil.
fn div_ceil(a: i64, b: i64) -> i64 {
    if b <= 0 {
        return 0;
    }
    if a % b == 0 {
        a / b
    } else {
        a / b + 1
    }
}

// ── columnSpec ────────────────────────────────────────────────────────────────
//
// Grammar: <name>:<type>[,key=value]*
// Keys: nullable (true|false), pk (true|false), primary_key (true|false), default (<expr>)

#[derive(Debug, Clone)]
struct ColumnSpec {
    name: String,
    col_type: String,
    nullable: Option<bool>,
    pk: bool,
    default: Option<String>,
}

/// parse_column_spec parses one --column / --add-column flag value.
///
/// Grammar: <name>:<type>[,<key>=<value>]*
/// Commas inside type parameters (e.g. NUMERIC(10,2)) are handled by
/// scanning for the first comma followed by a recognised key name.
fn parse_column_spec(s: &str) -> CliResult<ColumnSpec> {
    if s.is_empty() {
        return Err(msg("column spec is empty"));
    }
    let colon = s.find(':').ok_or_else(|| {
        msg(format!(
            "column spec {s:?}: missing ':' separator (want name:type[,key=val])"
        ))
    })?;
    let name = s[..colon].trim().to_string();
    let rest = &s[colon + 1..];
    if name.is_empty() {
        return Err(msg(format!("column spec {s:?}: name is empty")));
    }

    // Find the type boundary: the first comma at paren depth 0.
    // This correctly handles types like NUMERIC(10,2) whose commas are at depth > 0.
    let mut split_at: Option<usize> = None;
    let mut depth: usize = 0;
    for (i, b) in rest.bytes().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                split_at = Some(i);
                break;
            }
            _ => {}
        }
    }

    let (type_part, kv_part) = match split_at {
        None => (rest, ""),
        Some(i) => (&rest[..i], &rest[i + 1..]),
    };

    let col_type = type_part.trim().to_string();
    if col_type.is_empty() {
        return Err(msg(format!("column spec {s:?}: type is empty")));
    }

    let mut cs = ColumnSpec {
        name,
        col_type,
        nullable: None,
        pk: false,
        default: None,
    };
    if kv_part.is_empty() {
        return Ok(cs);
    }

    // Parse key=value tokens. "default=" is special: once seen, consumes the rest.
    let mut remaining = kv_part;
    while !remaining.is_empty() {
        let lower_rem = remaining.to_ascii_lowercase();
        if lower_rem.starts_with("default=") {
            let def_val = remaining["default=".len()..].trim().to_string();
            cs.default = Some(def_val);
            break;
        }

        // Find the end of this token (next comma, or end of string).
        let comma_idx = remaining.find(',');

        let (token, next_remaining) = match comma_idx {
            None => (remaining, ""),
            Some(i) => (&remaining[..i], &remaining[i + 1..]),
        };
        remaining = next_remaining;

        let eq = token.find('=').ok_or_else(|| {
            msg(format!(
                "column spec {s:?}: malformed key/value {token:?} (want key=value)"
            ))
        })?;
        let k = token[..eq].trim().to_ascii_lowercase();
        let v = token[eq + 1..].trim();
        match k.as_str() {
            "nullable" => {
                let b = parse_bool_val(v)
                    .map_err(|e| msg(format!("column spec {s:?}: nullable: {e}")))?;
                cs.nullable = Some(b);
            }
            "pk" | "primary_key" => {
                let b =
                    parse_bool_val(v).map_err(|e| msg(format!("column spec {s:?}: pk: {e}")))?;
                cs.pk = b;
            }
            _ => {
                return Err(msg(format!(
                    "column spec {s:?}: unknown key {k:?} (want nullable, pk, default)"
                )));
            }
        }
    }
    Ok(cs)
}

/// parse_bool_val accepts "true", "false", "1", "0", "yes", "no".
fn parse_bool_val(s: &str) -> CliResult<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(msg(format!("expected true/false, got {s:?}"))),
    }
}

/// quote_ident wraps an identifier in double-quotes, doubling internal quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// column_spec_to_sql renders a ColumnSpec into a SQL column definition fragment.
fn column_spec_to_sql(cs: &ColumnSpec) -> String {
    let mut s = format!("{} {}", quote_ident(&cs.name), cs.col_type);
    if let Some(false) = cs.nullable {
        s.push_str(" NOT NULL");
    }
    if let Some(def) = &cs.default {
        s.push_str(" DEFAULT ");
        s.push_str(def);
    }
    s
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_tables(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin tables (list | describe | show-rows | create | alter | drop | import-csv | export-csv | columns) ...",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => tables_list(g, rest),
        "describe" => tables_describe(g, rest),
        "show-rows" => tables_show_rows(g, rest),
        "create" => tables_create(g, rest),
        "alter" => tables_alter(g, rest),
        "drop" => tables_drop(g, rest),
        "import-csv" => tables_import_csv(g, rest),
        "export-csv" => tables_export_csv(g, rest),
        "columns" => tables_columns(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "tables",
                "List / describe / manage tables and their columns.",
                &[
                    "list        --project <ref>                                 List tables.",
                    "describe    <name> --project <ref>                          Show columns + RLS.",
                    "show-rows   <name> --project <ref> [--page N --page-size N] Paginated rows.",
                    "create      <name> --column=<spec>... [--project <ref>]     Create a table.",
                    "alter       <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project <ref>]  Alter a table.",
                    "drop        <name> [--yes] [--project <ref>]                Drop a table.",
                    "import-csv  <name> [--file=<path>] [--header=true|false] [--project <ref>]  Import CSV.",
                    "export-csv  <name> [--project <ref>]                        Export CSV to stdout.",
                    "columns     add|alter|drop <table> ...                      Manage columns.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for tables", other);
            Err(crate::error::silent())
        }
    }
}

// ── tables list ───────────────────────────────────────────────────────────────

fn tables_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables list",
            "List tables in a project.",
            &["--project <ref>   Project ref (required)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let c = require_client(g)?;
    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        tables: Vec<TableInfo>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{project}/tables"), None)?;
    if g.json {
        // JSON shape: { tables: [ TableInfo ] }
        return print_json(&mut std::io::stdout(), &json!({ "tables": resp.tables }));
    }
    let mut t = Table::new(g, &["NAME", "SCHEMA", "ROWS"]);
    for row in &resp.tables {
        t.row(&[&row.name, &row.schema, &row.rows.to_string()]);
    }
    t.flush()
}

// ── tables describe ───────────────────────────────────────────────────────────

fn tables_describe(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables describe")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables describe",
            "Show columns for a table.",
            &[
                "<name>             Table name (required).",
                "--project <ref>    Project ref (required).",
            ],
        );
        return Ok(());
    }
    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin tables describe <name> --project <ref>"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let c = require_client(g)?;
    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        table: Option<TableDetail>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/tables/{name}"),
        None,
    )?;
    if g.json {
        // JSON shape: { table: { name: string, schema: string, columns: [ Column ] } }
        return print_json(&mut std::io::stdout(), &json!({ "table": resp.table }));
    }
    let Some(tbl) = resp.table else {
        println!("(empty)");
        return Ok(());
    };
    println!("Table: {}.{}", tbl.schema, tbl.name);
    let mut t = Table::new(g, &["COLUMN", "TYPE", "NULL", "PK", "DEFAULT"]);
    for col in &tbl.columns {
        let def = col.default.as_deref().unwrap_or("");
        t.row(&[
            &col.name,
            &col.col_type,
            bool_str(col.nullable),
            bool_str(col.pk),
            def,
        ]);
    }
    t.flush()
}

// ── tables show-rows ──────────────────────────────────────────────────────────

fn tables_show_rows(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables show-rows")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("page").long("page"))
        .arg(Arg::new("page-size").long("page-size"))
        .arg(Arg::new("sort").long("sort"))
        .arg(Arg::new("filter").long("filter"))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables show-rows",
            "Show paginated rows for a table.",
            &[
                "<name>                 Table name (required).",
                "--project <ref>        Project ref (required).",
                "--page <n>             Page number (default 1).",
                "--page-size <n>        Rows per page (default 50).",
                "--sort <col:dir>       Sort column (e.g. id:desc).",
                "--filter <expr>        Filter expression.",
            ],
        );
        return Ok(());
    }
    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin tables show-rows <name> --project <ref>"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let page = m
        .get_one::<String>("page")
        .cloned()
        .unwrap_or_else(|| "1".into());
    let page_size = m
        .get_one::<String>("page-size")
        .cloned()
        .unwrap_or_else(|| "50".into());
    let sort = m.get_one::<String>("sort").cloned().unwrap_or_default();
    let filter = m.get_one::<String>("filter").cloned().unwrap_or_default();

    let c = require_client(g)?;
    let q = query_string(&[
        ("page", &page),
        ("page_size", &page_size),
        ("sort", &sort),
        ("filter", &filter),
    ]);
    let resp: RowsPage = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/tables/{name}/rows{q}"),
        None,
    )?;
    if g.json {
        // JSON shape: RowsPage — { columns: [string], rows: [[any]], total, page, page_size }
        return print_json(&mut std::io::stdout(), &resp);
    }
    crate::commands::sql::render_rows(g, &resp.columns, &resp.rows, None, 0)?;
    if !g.quiet {
        let total_pages = div_ceil(resp.total, resp.page_size.max(1));
        eprintln!(
            "(page {}/{} · {} total)",
            resp.page, total_pages, resp.total
        );
    }
    Ok(())
}

// ── tables create ─────────────────────────────────────────────────────────────

/// tableCreateResponse: JSON shape { "created": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableCreateResponse {
    #[serde(default)]
    created: bool,
}

fn tables_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables create")
        .arg(Arg::new("project").long("project"))
        // --column is repeatable
        .arg(Arg::new("column").long("column").action(ArgAction::Append))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables create",
            "Create a table with the given column specs.",
            &[
                "<name>                                              Table name (required).",
                "--column=<spec>                                     Column spec: name:type[,...]. Repeatable.",
                "--project <ref>                                     Project ref.",
            ],
        );
        return Ok(());
    }
    let name = m.get_one::<String>("name").cloned().ok_or_else(|| {
        msg("usage: basin tables create <name> --column=<spec>... [--project=<ref>]")
    })?;

    let column_strs: Vec<String> = m
        .get_many::<String>("column")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if column_strs.is_empty() {
        return Err(msg(
            "tables create: at least one --column=<spec> is required",
        ));
    }

    let mut specs = Vec::new();
    for raw in &column_strs {
        let cs = parse_column_spec(raw).map_err(|e| msg(format!("tables create: {e}")))?;
        specs.push(cs);
    }

    let mut col_defs: Vec<String> = specs.iter().map(column_spec_to_sql).collect();
    let pk_cols: Vec<String> = specs
        .iter()
        .filter(|cs| cs.pk)
        .map(|cs| quote_ident(&cs.name))
        .collect();
    if !pk_cols.is_empty() {
        col_defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
    }
    let sql = format!(
        "CREATE TABLE {} (\n  {}\n)",
        quote_ident(&name),
        col_defs.join(",\n  ")
    );

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: TableCreateResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables"),
        Some(json!({ "sql": sql })),
    )?;
    if g.json {
        // JSON shape: { "created": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Created table {name}.");
    Ok(())
}

// ── tables alter ──────────────────────────────────────────────────────────────

/// tableAlterResponse: JSON shape { "altered": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableAlterResponse {
    #[serde(default)]
    altered: bool,
}

fn tables_alter(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables alter")
        .arg(Arg::new("project").long("project"))
        .arg(
            Arg::new("add-column")
                .long("add-column")
                .action(ArgAction::Append),
        )
        .arg(
            Arg::new("drop-column")
                .long("drop-column")
                .action(ArgAction::Append),
        )
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables alter",
            "Alter a table by adding or dropping columns.",
            &[
                "<name>                                     Table name (required).",
                "--add-column=<spec>                        Column spec to add. Repeatable.",
                "--drop-column=<col>                        Column name to drop. Repeatable.",
                "--project <ref>                            Project ref.",
            ],
        );
        return Ok(());
    }
    let name = m.get_one::<String>("name").cloned().ok_or_else(|| {
        msg("usage: basin tables alter <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project=<ref>]")
    })?;

    let add_columns: Vec<String> = m
        .get_many::<String>("add-column")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    let drop_columns: Vec<String> = m
        .get_many::<String>("drop-column")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();

    if add_columns.is_empty() && drop_columns.is_empty() {
        return Err(msg(
            "tables alter: at least one --add-column or --drop-column is required",
        ));
    }

    let mut add_specs = Vec::new();
    for raw in &add_columns {
        let cs = parse_column_spec(raw).map_err(|e| msg(format!("tables alter: {e}")))?;
        add_specs.push(cs);
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let mut actions: Vec<String> = add_specs
        .iter()
        .map(|cs| format!("ADD COLUMN {}", column_spec_to_sql(cs)))
        .collect();
    for col in &drop_columns {
        actions.push(format!("DROP COLUMN {}", quote_ident(col.trim())));
    }
    let sql = format!("ALTER TABLE {} {}", quote_ident(&name), actions.join(", "));

    let resp: TableAlterResponse = c.do_json(
        Method::PATCH,
        &format!("/v1/projects/{ref}/tables/{name}"),
        Some(json!({ "sql": sql })),
    )?;
    if g.json {
        // JSON shape: { "altered": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Altered table {name}.");
    Ok(())
}

// ── tables drop ───────────────────────────────────────────────────────────────

/// tableDropResponse: JSON shape { "dropped": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableDropResponse {
    #[serde(default)]
    dropped: bool,
}

fn tables_drop(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables drop")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables drop",
            "Drop a table (confirm interactively unless --yes is passed).",
            &[
                "<name>             Table name (required).",
                "--project <ref>    Project ref.",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin tables drop <name> [--yes] [--project=<ref>]"))?;

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to drop table {:?} non-interactively under --json",
                name
            )));
        }
        eprint!(
            "Drop table {:?} in project {:?}? This cannot be undone. [y/N] ",
            name, r#ref
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let q = query_string(&[("confirm_name", &name)]);
    let resp: TableDropResponse = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/tables/{name}{q}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "dropped": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Dropped table {name}.");
    Ok(())
}

// ── tables import-csv ─────────────────────────────────────────────────────────

/// tableImportCSVResponse: JSON shape { "rows_imported": N, "rows_rejected": N, "rejections": [...], "elapsed_ms": N }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct TableImportCSVResponse {
    #[serde(default)]
    rows_imported: i64,
    #[serde(default)]
    rows_rejected: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    rejections: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<i64>,
}

fn tables_import_csv(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables import-csv")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("file").long("file"))
        .arg(Arg::new("header").long("header"))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables import-csv",
            "Import CSV data into a table (multipart/form-data upload).",
            &[
                "<name>                   Table name (required).",
                "--file <path>            Path to CSV file (default: stdin).",
                "--header true|false      Whether CSV has a header row (default: true).",
                "--project <ref>          Project ref.",
            ],
        );
        return Ok(());
    }
    let name = m.get_one::<String>("name").cloned().ok_or_else(|| {
        msg("usage: basin tables import-csv <name> [--file=<path>] [--header=true|false] [--project=<ref>]")
    })?;
    let header = m
        .get_one::<String>("header")
        .cloned()
        .unwrap_or_else(|| "true".into());
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    // Open the CSV source — either a named file or stdin.
    let csv_bytes: Vec<u8> = if let Some(file_path) = m.get_one::<String>("file") {
        std::fs::read(file_path)
            .map_err(|e| msg(format!("tables import-csv: open {:?}: {e}", file_path)))?
    } else {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .map_err(|e| msg(format!("tables import-csv: reading stdin: {e}")))?;
        buf
    };

    let resp = post_multipart_csv(
        &c,
        &format!("/v1/projects/{ref}/tables/{name}/import-csv"),
        csv_bytes,
        &header,
    )?;
    if g.json {
        // JSON shape: { "rows_imported": N, "rows_rejected": N, "rejections": [...], "elapsed_ms": N }
        return print_json(&mut std::io::stdout(), &resp);
    }
    print!("Imported {} row(s) into {name}", resp.rows_imported);
    if resp.rows_rejected > 0 {
        print!(" ({} rejected)", resp.rows_rejected);
    }
    println!(".");
    Ok(())
}

use std::io::Read as StdRead;

/// post_multipart_csv sends a CSV body as multipart/form-data to the cloud's
/// import endpoint. We use reqwest blocking directly (not the shared Client)
/// to avoid adding new methods to client.rs, mirroring Go's local postMultipartCSV.
fn post_multipart_csv(
    c: &crate::client::Client,
    path: &str,
    csv_bytes: Vec<u8>,
    header: &str,
) -> CliResult<TableImportCSVResponse> {
    // Build the multipart body manually.
    // Boundary: a stable string — fine for non-interactive use.
    let boundary = format!("BasinCSVBoundary{}", std::process::id());

    let mut body: Vec<u8> = Vec::new();
    // header field
    write!(
        body,
        "--{boundary}\r\nContent-Disposition: form-data; name=\"header\"\r\n\r\n{header}\r\n"
    )
    .unwrap();
    // file field
    write!(body, "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"data.csv\"\r\nContent-Type: text/csv\r\n\r\n").unwrap();
    body.extend_from_slice(&csv_bytes);
    write!(body, "\r\n--{boundary}--\r\n").unwrap();

    let content_type = format!("multipart/form-data; boundary={boundary}");
    let url = format!("{}{}", c.base_url, path);

    let http = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| msg(format!("build http client: {e}")))?;

    let mut req = http
        .post(&url)
        .header("Content-Type", content_type)
        .header("Accept", "application/json")
        .body(body);
    if !c.token.is_empty() {
        req = req.bearer_auth(&c.token);
    }

    let resp = req
        .send()
        .map_err(|e| msg(format!("tables import-csv: {e}")))?;
    let status = resp.status().as_u16();
    let text = resp
        .text()
        .map_err(|e| msg(format!("tables import-csv: read response: {e}")))?;

    if (200..300).contains(&status) {
        return crate::client::unwrap_envelope(&text);
    }
    Err(Box::new(crate::client::parse_api_error(status, &text)))
}

// ── tables export-csv ─────────────────────────────────────────────────────────

fn tables_export_csv(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables export-csv")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables export-csv",
            "Export a table's contents as CSV to stdout.",
            &[
                "<name>             Table name (required).",
                "--project <ref>    Project ref.",
            ],
        );
        return Ok(());
    }
    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin tables export-csv <name> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    // Stream the CSV directly to stdout — no per-request deadline (large tables).
    let url = format!("{}/v1/projects/{ref}/tables/{name}/export-csv", c.base_url);
    let http = reqwest::blocking::Client::builder()
        // No timeout — large tables may stream for a while, matching Go's httpStream.
        .build()
        .map_err(|e| msg(format!("build http client: {e}")))?;

    let mut req = http.get(&url).header("Accept", "text/csv");
    if !c.token.is_empty() {
        req = req.bearer_auth(&c.token);
    }

    let mut resp = req
        .send()
        .map_err(|e| msg(format!("tables export-csv: {e}")))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        let text = resp
            .text()
            .map_err(|e| msg(format!("tables export-csv: {e}")))?;
        return Err(Box::new(crate::client::parse_api_error(status, &text)));
    }
    // Stream CSV bytes to stdout.
    let mut stdout = std::io::stdout();
    let mut buf = [0u8; 8192];
    loop {
        let n = resp
            .read(&mut buf)
            .map_err(|e| msg(format!("tables export-csv: streaming: {e}")))?;
        if n == 0 {
            break;
        }
        stdout
            .write_all(&buf[..n])
            .map_err(|e| msg(format!("tables export-csv: write stdout: {e}")))?;
    }
    Ok(())
}

// ── tables columns ────────────────────────────────────────────────────────────

fn tables_columns(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin tables columns (add | alter | drop) <table> ...",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "add" => columns_add(g, rest),
        "alter" => columns_alter(g, rest),
        "drop" => columns_drop(g, rest),
        other => Err(msg(format!(
            "unknown subcommand {:?} for tables columns",
            other
        ))),
    }
}

// ── tables columns add ────────────────────────────────────────────────────────

/// columnAddResponse: JSON shape { "added": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnAddResponse {
    #[serde(default)]
    added: bool,
}

fn columns_add(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables columns add")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("column").long("column"))
        .arg(Arg::new("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables columns add",
            "Add a column to a table.",
            &[
                "<table>              Table name (required).",
                "--column=<spec>      Column spec: name:type[,...] (required).",
                "--project <ref>      Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("name").cloned().ok_or_else(|| {
        msg("usage: basin tables columns add <table> --column=<spec> [--project=<ref>]")
    })?;
    let col_spec_str = m.get_one::<String>("column").cloned().unwrap_or_default();
    if col_spec_str.is_empty() {
        return Err(msg("tables columns add: --column=<spec> is required"));
    }
    let cs =
        parse_column_spec(&col_spec_str).map_err(|e| msg(format!("tables columns add: {e}")))?;

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let mut body = json!({
        "name": cs.name,
        "type": cs.col_type,
    });
    if let Some(nullable) = cs.nullable {
        body["nullable"] = json!(nullable);
    }
    if let Some(default) = &cs.default {
        body["default"] = json!(default);
    }

    let resp: ColumnAddResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables/{table}/columns"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "added": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Added column {} to table {table}.", cs.name);
    Ok(())
}

// ── tables columns alter ──────────────────────────────────────────────────────

/// columnAlterResponse: JSON shape { "altered": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnAlterResponse {
    #[serde(default)]
    altered: bool,
}

fn columns_alter(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables columns alter")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("col-name").long("name"))
        .arg(Arg::new("col-type").long("type"))
        .arg(Arg::new("nullable").long("nullable"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables columns alter",
            "Alter a column on a table.",
            &[
                "<table>                    Table name (required).",
                "--name=<col>               Column name to alter (required).",
                "--type=<t>                 New type (optional).",
                "--nullable=true|false      Set nullability (optional).",
                "--project <ref>            Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin tables columns alter <table> --name=<col> [--type=<t>] [--nullable=true|false] [--project=<ref>]")
    })?;
    let col_name = m.get_one::<String>("col-name").cloned().unwrap_or_default();
    if col_name.is_empty() {
        return Err(msg("tables columns alter: --name is required"));
    }
    let col_type = m.get_one::<String>("col-type").cloned().unwrap_or_default();
    let nullable_str = m.get_one::<String>("nullable").cloned().unwrap_or_default();
    if col_type.is_empty() && nullable_str.is_empty() {
        return Err(msg(
            "tables columns alter: at least one of --type or --nullable is required",
        ));
    }

    let mut body = json!({});
    if !col_type.is_empty() {
        body["type"] = json!(col_type);
    }
    if !nullable_str.is_empty() {
        let b = parse_bool_val(&nullable_str)
            .map_err(|e| msg(format!("tables columns alter: --nullable: {e}")))?;
        body["nullable"] = json!(b);
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let resp: ColumnAlterResponse = c.do_json(
        Method::PATCH,
        &format!("/v1/projects/{ref}/tables/{table}/columns/{col_name}"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "altered": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Altered column {col_name} on table {table}.");
    Ok(())
}

// ── tables columns drop ───────────────────────────────────────────────────────

/// columnDropResponse: JSON shape { "dropped": bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnDropResponse {
    #[serde(default)]
    dropped: bool,
}

fn columns_drop(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tables columns drop")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("col-name").long("name"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tables columns drop",
            "Drop a column from a table.",
            &[
                "<table>              Table name (required).",
                "--name=<col>         Column name to drop (required).",
                "--yes                Skip the confirmation prompt.",
                "--project <ref>      Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin tables columns drop <table> --name=<col> [--yes] [--project=<ref>]")
    })?;
    let col_name = m.get_one::<String>("col-name").cloned().unwrap_or_default();
    if col_name.is_empty() {
        return Err(msg("tables columns drop: --name is required"));
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to drop column {:?} non-interactively under --json",
                col_name
            )));
        }
        eprint!(
            "Drop column {:?} from table {table}? This cannot be undone. [y/N] ",
            col_name
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let resp: ColumnDropResponse = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/tables/{table}/columns/{col_name}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "dropped": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Dropped column {col_name} from table {table}.");
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

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_args_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_tables(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_tables(&g, &["help".to_string()]).is_ok());
    }

    // ── parseColumnSpec ─────────────────────────────────────────────────────

    #[test]
    fn parse_column_spec_simple() {
        let cs = parse_column_spec("email:text").unwrap();
        assert_eq!(cs.name, "email");
        assert_eq!(cs.col_type, "text");
        assert!(cs.nullable.is_none());
        assert!(!cs.pk);
        assert!(cs.default.is_none());
    }

    #[test]
    fn parse_column_spec_with_pk_and_nullable() {
        let cs = parse_column_spec("id:bigint,pk=true,nullable=false").unwrap();
        assert_eq!(cs.name, "id");
        assert_eq!(cs.col_type, "bigint");
        assert!(cs.pk);
        assert_eq!(cs.nullable, Some(false));
    }

    #[test]
    fn parse_column_spec_with_default() {
        let cs = parse_column_spec("created_at:timestamptz,default=now()").unwrap();
        assert_eq!(cs.default.as_deref(), Some("now()"));
    }

    #[test]
    fn parse_column_spec_numeric_type_with_parens() {
        // NUMERIC(10,2) has a comma inside the type — should NOT be split there.
        let cs = parse_column_spec("price:NUMERIC(10,2),nullable=false").unwrap();
        assert_eq!(cs.col_type, "NUMERIC(10,2)");
        assert_eq!(cs.nullable, Some(false));
    }

    #[test]
    fn parse_column_spec_empty_errors() {
        assert!(parse_column_spec("").is_err());
    }

    #[test]
    fn parse_column_spec_missing_colon_errors() {
        assert!(parse_column_spec("nocol").is_err());
    }

    #[test]
    fn parse_column_spec_empty_name_errors() {
        assert!(parse_column_spec(":text").is_err());
    }

    #[test]
    fn parse_column_spec_empty_type_errors() {
        assert!(parse_column_spec("col:").is_err());
    }

    #[test]
    fn parse_column_spec_bad_key_errors() {
        // "size" is not a known key
        assert!(parse_column_spec("col:text,size=10").is_err());
    }

    #[test]
    fn parse_column_spec_malformed_kv_errors() {
        // no '=' in token
        assert!(parse_column_spec("col:text,nullable").is_err());
    }

    // ── tables list ─────────────────────────────────────────────────────────

    #[test]
    fn list_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(&g, &["list".to_string()]).unwrap_err();
        assert!(err.to_string().contains("--project"), "err: {err}");
    }

    #[test]
    fn list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1/tables");
            Resp::ok(r#"{"tables":[{"name":"users","schema":"public","row_count":42}]}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"tables":[{"name":"t1","schema":"public","row_count":0}]}"#)
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        #[derive(Deserialize)]
        struct R {
            #[serde(default)]
            tables: Vec<TableInfo>,
        }
        let resp: R = c
            .do_json(Method::GET, "/v1/projects/p1/tables", None)
            .unwrap();
        print_json(&mut buf, &json!({ "tables": resp.tables })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["tables"][0]["name"].as_str().unwrap(), "t1");
    }

    #[test]
    fn list_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"code":"not_found","message":"Project not found."}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_tables(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── tables describe ─────────────────────────────────────────────────────

    #[test]
    fn describe_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.path, "/v1/projects/p1/tables/users");
            Resp::ok(
                r#"{"table":{"name":"users","schema":"public","columns":[{"name":"id","type":"bigint","nullable":false,"pk":true}]}}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "describe".to_string(),
                "--project=p1".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn describe_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_tables(&g, &["describe".to_string(), "--project=p1".to_string()]).is_err());
    }

    // ── tables create ───────────────────────────────────────────────────────

    #[test]
    fn create_happy_path_sql_shape() {
        let _g = with_temp_config_dir();
        let captured_sql = Arc::new(Mutex::new(String::new()));
        let cap2 = Arc::clone(&captured_sql);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body["sql"].as_str().unwrap_or("").to_string();
            Resp::ok(r#"{"created":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "create".to_string(),
                "--project=p1".to_string(),
                "--column=id:bigint,pk=true,nullable=false".to_string(),
                "--column=email:text,nullable=false".to_string(),
                "--column=created_at:timestamptz,default=now()".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let sql = captured_sql.lock().unwrap();
        assert!(sql.contains("CREATE TABLE"), "sql: {sql}");
        assert!(sql.contains("PRIMARY KEY"), "sql: {sql}");
        assert!(sql.contains("NOT NULL"), "sql: {sql}");
        assert!(sql.contains("DEFAULT now()"), "sql: {sql}");
    }

    #[test]
    fn create_missing_column_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "create".to_string(),
                "--project=p1".to_string(),
                "foo".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--column"), "err: {err}");
    }

    #[test]
    fn create_malformed_column_spec_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_tables(
            &g,
            &[
                "create".to_string(),
                "--project=p1".to_string(),
                "--column=badspec".to_string(),
                "foo".to_string()
            ]
        )
        .is_err());
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"created":true}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TableCreateResponse = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables",
                Some(json!({"sql":"CREATE TABLE x (a bigint)"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["created"].as_bool().unwrap());
    }

    #[test]
    fn create_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(422, r#"{"code":"bad_sql","message":"Syntax error."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_tables(
            &g,
            &[
                "create".to_string(),
                "--project=p1".to_string(),
                "--column=id:bigint".to_string(),
                "foo".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── tables alter ─────────────────────────────────────────────────────────

    #[test]
    fn alter_add_column_sql() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let cap2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "PATCH");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body["sql"].as_str().unwrap_or("").to_string();
            Resp::ok(r#"{"altered":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "alter".to_string(),
                "--project=p1".to_string(),
                "--add-column=bio:text".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let sql = cap.lock().unwrap();
        assert!(sql.contains("ADD COLUMN"), "sql: {sql}");
    }

    #[test]
    fn alter_drop_column_sql() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let cap2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body["sql"].as_str().unwrap_or("").to_string();
            Resp::ok(r#"{"altered":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "alter".to_string(),
                "--project=p1".to_string(),
                "--drop-column=old_field".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let sql = cap.lock().unwrap();
        assert!(sql.contains("DROP COLUMN"), "sql: {sql}");
    }

    #[test]
    fn alter_both_sql() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let cap2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body["sql"].as_str().unwrap_or("").to_string();
            Resp::ok(r#"{"altered":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "alter".to_string(),
                "--project=p1".to_string(),
                "--add-column=score:int".to_string(),
                "--drop-column=old_col".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let sql = cap.lock().unwrap();
        assert!(
            sql.contains("ADD COLUMN") && sql.contains("DROP COLUMN"),
            "sql: {sql}"
        );
    }

    #[test]
    fn alter_no_flags_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "alter".to_string(),
                "--project=p1".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--add-column or --drop-column"),
            "err: {err}"
        );
    }

    // ── tables drop ──────────────────────────────────────────────────────────

    #[test]
    fn drop_with_yes_sends_confirm_name() {
        let _g = with_temp_config_dir();
        let confirmed = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&confirmed);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            // path contains ?confirm_name=orders
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(r#"{"dropped":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "drop".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
                "orders".to_string(),
            ],
        )
        .unwrap();
        let path = confirmed.lock().unwrap();
        assert!(path.contains("confirm_name"), "path: {path}");
    }

    #[test]
    fn drop_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"dropped":true}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TableDropResponse = c
            .do_json(
                Method::DELETE,
                "/v1/projects/p1/tables/orders?confirm_name=orders",
                None,
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["dropped"].as_bool().unwrap());
    }

    #[test]
    fn drop_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Table not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_tables(
            &g,
            &[
                "drop".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
                "missing".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── tables import-csv ─────────────────────────────────────────────────────

    #[test]
    fn import_csv_from_file() {
        let _g = with_temp_config_dir();
        // Write a temp CSV file.
        let dir = std::env::temp_dir().join(format!("basin-tbl-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("data.csv");
        let csv_content = "id,name\n1,Alice\n2,Bob\n";
        std::fs::write(&csv_path, csv_content).unwrap();

        let received_body = Arc::new(Mutex::new(Vec::<u8>::new()));
        let rb2 = Arc::clone(&received_body);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/users/import-csv");
            *rb2.lock().unwrap() = req.body.as_bytes().to_vec();
            Resp::ok(r#"{"rows_imported":2,"rows_rejected":0}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "import-csv".to_string(),
                "--project=p1".to_string(),
                format!("--file={}", csv_path.display()),
                "users".to_string(),
            ],
        )
        .unwrap();

        // Verify multipart body contains CSV data.
        let body = received_body.lock().unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert!(body_str.contains("1,Alice"), "body: {body_str}");
        assert!(
            body_str.contains("multipart")
                || body_str.contains("BasinCSVBoundary")
                || body_str.contains("form-data"),
            "body: {body_str}"
        );
    }

    #[test]
    fn import_csv_header_false() {
        let _g = with_temp_config_dir();
        let dir = std::env::temp_dir().join(format!("basin-tbl-test2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("data.csv");
        std::fs::write(&csv_path, "1,Alice\n2,Bob\n").unwrap();

        let captured = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.body.clone();
            Resp::ok(r#"{"rows_imported":2,"rows_rejected":0}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "import-csv".to_string(),
                "--project=p1".to_string(),
                "--header=false".to_string(),
                format!("--file={}", csv_path.display()),
                "products".to_string(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        // The body should contain the "header" field value "false".
        assert!(
            body.contains("false"),
            "body should contain header=false: {body}"
        );
    }

    #[test]
    fn import_csv_json_shape() {
        let _g = with_temp_config_dir();
        let dir = std::env::temp_dir().join(format!("basin-tbl-test3-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("data.csv");
        std::fs::write(&csv_path, "a,b\n1,2\n").unwrap();

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"rows_imported":5,"rows_rejected":1,"rejections":["row 3: bad value"]}"#)
        });
        let _g = flags_json(&srv.url);
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TableImportCSVResponse = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables/t1/import-csv",
                Some(json!({})),
            )
            .unwrap();
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["rows_imported"].as_i64().unwrap(), 5);
        assert_eq!(v["rows_rejected"].as_i64().unwrap(), 1);
    }

    // ── tables export-csv ─────────────────────────────────────────────────────

    #[test]
    fn export_csv_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Table not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_tables(
            &g,
            &[
                "export-csv".to_string(),
                "--project=p1".to_string(),
                "missing".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── tables columns add ────────────────────────────────────────────────────

    #[test]
    fn columns_add_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/users/columns");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"added":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "add".to_string(),
                "--project=p1".to_string(),
                "--column=bio:text,nullable=true".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap();
        assert_eq!(body["name"].as_str().unwrap(), "bio");
        assert_eq!(body["type"].as_str().unwrap(), "text");
    }

    #[test]
    fn columns_add_missing_spec_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "add".to_string(),
                "--project=p1".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--column"), "err: {err}");
    }

    #[test]
    fn columns_add_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"added":true}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ColumnAddResponse = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables/t1/columns",
                Some(json!({"name":"x","type":"int"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["added"].as_bool().unwrap());
    }

    // ── tables columns alter ──────────────────────────────────────────────────

    #[test]
    fn columns_alter_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/projects/p1/tables/users/columns/email");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"altered":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "alter".to_string(),
                "--project=p1".to_string(),
                "--name=email".to_string(),
                "--nullable=false".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap();
        assert!(!body["nullable"].as_bool().unwrap());
    }

    #[test]
    fn columns_alter_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "alter".to_string(),
                "--project=p1".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name"), "err: {err}");
    }

    #[test]
    fn columns_alter_no_changes_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "alter".to_string(),
                "--project=p1".to_string(),
                "--name=email".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--type or --nullable"),
            "err: {err}"
        );
    }

    #[test]
    fn columns_alter_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"altered":true}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ColumnAlterResponse = c
            .do_json(
                Method::PATCH,
                "/v1/projects/p1/tables/t1/columns/col1",
                Some(json!({"type":"bigint"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["altered"].as_bool().unwrap());
    }

    // ── tables columns drop ───────────────────────────────────────────────────

    #[test]
    fn columns_drop_with_yes_calls_delete() {
        let _g = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let c2 = Arc::clone(&called);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/p1/tables/users/columns/bio");
            *c2.lock().unwrap() = true;
            Resp::ok(r#"{"dropped":true}"#)
        });
        let g = flags(&srv.url);
        cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--name=bio".to_string(),
                "--yes".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap();
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn columns_drop_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "users".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name"), "err: {err}");
    }

    #[test]
    fn columns_drop_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"dropped":true}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ColumnDropResponse = c
            .do_json(Method::DELETE, "/v1/projects/p1/tables/t1/columns/x", None)
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["dropped"].as_bool().unwrap());
    }

    #[test]
    fn columns_drop_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Column not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_tables(
            &g,
            &[
                "columns".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--name=x".to_string(),
                "--yes".to_string(),
                "t1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }
}
