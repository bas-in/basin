//! gen — `basin gen <subcommand>` dispatcher.
//!
//! Currently implements:
//!
//!   basin gen types <typescript|go|python> [--project=<ref>] [--schema=public]
//!                                          [--output=<path>] [--package=<name>]
//!                                          [--watch]
//!
//! Future subcommands (gen migrations, etc.) will be added here as the
//! platform surface grows.
//!
//! gen types queries information_schema.columns via POST /v1/projects/{ref}/sql/query
//! and emits database.ts / database.go / database.py via a Postgres-type→lang-type
//! mapping table.

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::time::Duration;

use reqwest::Method;
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::types::QueryResult;

use super::help::help_for_command;

// ── Language target ───────────────────────────────────────────────────────────

/// LangTarget identifies the output language for a type-generation pass.
/// The zero value (0) is intentionally invalid so accidental zero-value
/// usage produces ok=false from map_type rather than a silent wrong mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LangTarget {
    /// LangTypeScript targets TypeScript; nullable form is `T | null`.
    TypeScript = 1,
    /// LangGo targets Go; nullable form is `*T`, JSONB uses `json.RawMessage`.
    Go = 2,
    /// LangPython targets Python + Pydantic; nullable form is `Optional[T]`.
    Python = 3,
}

// ── Type mapping table ────────────────────────────────────────────────────────
//
// Single source of truth: Postgres column type → target-language type.
// Keys are lower-cased data_type strings exactly as information_schema returns them.
// Mirrors gen_types_map.go faithfully.

/// MappedType is the result of a map_type lookup for one (PgType, LangTarget) pair.
#[derive(Debug, Clone)]
pub struct MappedType {
    /// Type — the non-nullable target type (used when the column is NOT NULL).
    pub typ: &'static str,
    /// Nullable — the nullable form (used when the column is nullable).
    pub nullable: &'static str,
    /// Import — an optional import statement. Empty for primitives.
    pub import: &'static str,
}

struct PgRow {
    ts: MappedType,
    go: MappedType,
    py: MappedType,
}

macro_rules! row {
    (ts: ($t:literal, $n:literal, $i:literal),
     go: ($gt:literal, $gn:literal, $gi:literal),
     py: ($pt:literal, $pn:literal, $pi:literal)) => {
        PgRow {
            ts: MappedType { typ: $t, nullable: $n, import: $i },
            go: MappedType { typ: $gt, nullable: $gn, import: $gi },
            py: MappedType { typ: $pt, nullable: $pn, import: $pi },
        }
    };
}

fn type_table() -> HashMap<&'static str, PgRow> {
    let mut m: HashMap<&'static str, PgRow> = HashMap::new();

    // ── Boolean ──────────────────────────────────────────────────────────────
    m.insert("boolean", row!(
        ts: ("boolean",            "boolean | null",           ""),
        go: ("bool",               "*bool",                    ""),
        py: ("bool",               "Optional[bool]",           "from typing import Optional")
    ));
    m.insert("bool", row!(
        ts: ("boolean",            "boolean | null",           ""),
        go: ("bool",               "*bool",                    ""),
        py: ("bool",               "Optional[bool]",           "from typing import Optional")
    ));

    // ── Integer family ───────────────────────────────────────────────────────
    m.insert("smallint", row!(
        ts: ("number",             "number | null",            ""),
        go: ("int16",              "*int16",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));
    m.insert("int2", row!(
        ts: ("number",             "number | null",            ""),
        go: ("int16",              "*int16",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));
    m.insert("integer", row!(
        ts: ("number",             "number | null",            ""),
        go: ("int32",              "*int32",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));
    m.insert("int4", row!(
        ts: ("number",             "number | null",            ""),
        go: ("int32",              "*int32",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));
    m.insert("bigint", row!(
        ts: ("bigint",             "bigint | null",            ""),
        go: ("int64",              "*int64",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));
    m.insert("int8", row!(
        ts: ("bigint",             "bigint | null",            ""),
        go: ("int64",              "*int64",                   ""),
        py: ("int",                "Optional[int]",            "from typing import Optional")
    ));

    // ── Floating-point family ────────────────────────────────────────────────
    m.insert("real", row!(
        ts: ("number",             "number | null",            ""),
        go: ("float32",            "*float32",                 ""),
        py: ("float",              "Optional[float]",          "from typing import Optional")
    ));
    m.insert("float4", row!(
        ts: ("number",             "number | null",            ""),
        go: ("float32",            "*float32",                 ""),
        py: ("float",              "Optional[float]",          "from typing import Optional")
    ));
    m.insert("double precision", row!(
        ts: ("number",             "number | null",            ""),
        go: ("float64",            "*float64",                 ""),
        py: ("float",              "Optional[float]",          "from typing import Optional")
    ));
    m.insert("float8", row!(
        ts: ("number",             "number | null",            ""),
        go: ("float64",            "*float64",                 ""),
        py: ("float",              "Optional[float]",          "from typing import Optional")
    ));

    // ── Exact numeric ────────────────────────────────────────────────────────
    m.insert("numeric", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("Decimal",            "Optional[Decimal]",        "from decimal import Decimal\nfrom typing import Optional")
    ));
    m.insert("decimal", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("Decimal",            "Optional[Decimal]",        "from decimal import Decimal\nfrom typing import Optional")
    ));

    // ── Text family ──────────────────────────────────────────────────────────
    m.insert("text", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));
    m.insert("character varying", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));
    m.insert("varchar", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));
    m.insert("character", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));
    m.insert("char", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));

    // ── Binary ───────────────────────────────────────────────────────────────
    m.insert("bytea", row!(
        ts: ("Uint8Array",         "Uint8Array | null",        ""),
        go: ("[]byte",             "[]byte",                   ""),
        py: ("bytes",              "Optional[bytes]",          "from typing import Optional")
    ));

    // ── UUID ─────────────────────────────────────────────────────────────────
    m.insert("uuid", row!(
        ts: ("string",             "string | null",            ""),
        go: ("[16]byte",           "*[16]byte",                ""),
        py: ("UUID",               "Optional[UUID]",           "from uuid import UUID\nfrom typing import Optional")
    ));

    // ── JSON / JSONB ─────────────────────────────────────────────────────────
    m.insert("jsonb", row!(
        ts: ("Record<string, unknown>", "Record<string, unknown> | null", ""),
        go: ("json.RawMessage",    "json.RawMessage",          "encoding/json"),
        py: ("Any",                "Optional[Any]",            "from typing import Any, Optional")
    ));
    m.insert("json", row!(
        ts: ("Record<string, unknown>", "Record<string, unknown> | null", ""),
        go: ("json.RawMessage",    "json.RawMessage",          "encoding/json"),
        py: ("Any",                "Optional[Any]",            "from typing import Any, Optional")
    ));

    // ── Timestamps ───────────────────────────────────────────────────────────
    m.insert("timestamp with time zone", row!(
        ts: ("string",             "string | null",            ""),
        go: ("time.Time",          "*time.Time",               "time"),
        py: ("datetime",           "Optional[datetime]",       "from datetime import datetime\nfrom typing import Optional")
    ));
    m.insert("timestamptz", row!(
        ts: ("string",             "string | null",            ""),
        go: ("time.Time",          "*time.Time",               "time"),
        py: ("datetime",           "Optional[datetime]",       "from datetime import datetime\nfrom typing import Optional")
    ));
    m.insert("timestamp without time zone", row!(
        ts: ("string",             "string | null",            ""),
        go: ("time.Time",          "*time.Time",               "time"),
        py: ("datetime",           "Optional[datetime]",       "from datetime import datetime\nfrom typing import Optional")
    ));
    m.insert("timestamp", row!(
        ts: ("string",             "string | null",            ""),
        go: ("time.Time",          "*time.Time",               "time"),
        py: ("datetime",           "Optional[datetime]",       "from datetime import datetime\nfrom typing import Optional")
    ));

    // ── Date ─────────────────────────────────────────────────────────────────
    m.insert("date", row!(
        ts: ("string",             "string | null",            ""),
        go: ("time.Time",          "*time.Time",               "time"),
        py: ("date",               "Optional[date]",           "from datetime import date\nfrom typing import Optional")
    ));

    // ── Time ─────────────────────────────────────────────────────────────────
    m.insert("time without time zone", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("time",               "Optional[time]",           "from datetime import time\nfrom typing import Optional")
    ));
    m.insert("time with time zone", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("time",               "Optional[time]",           "from datetime import time\nfrom typing import Optional")
    ));
    m.insert("time", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("time",               "Optional[time]",           "from datetime import time\nfrom typing import Optional")
    ));

    // ── Interval ─────────────────────────────────────────────────────────────
    // Listed as 🚫 in CAPABILITIES.md alongside MONEY/XML, but information_schema
    // may surface it from snapshots or future engine builds. Map to string.
    m.insert("interval", row!(
        ts: ("string",             "string | null",            ""),
        go: ("string",             "*string",                  ""),
        py: ("str",                "Optional[str]",            "from typing import Optional")
    ));

    // ── Vector ───────────────────────────────────────────────────────────────
    m.insert("vector", row!(
        ts: ("number[]",           "number[] | null",          ""),
        go: ("[]float32",          "[]float32",                ""),
        py: ("list[float]",        "Optional[list[float]]",    "from typing import Optional")
    ));

    m
}

/// map_type returns the MappedType for (pg_name, lang). Returns None when the
/// Postgres type name is not in the table — callers should fall back to a
/// safe default and emit a warning comment.
pub fn map_type(pg_name: &str, lang: LangTarget) -> Option<MappedType> {
    let table = type_table();
    let row = table.get(pg_name)?;
    match lang {
        LangTarget::TypeScript => Some(row.ts.clone()),
        LangTarget::Go => Some(row.go.clone()),
        LangTarget::Python => Some(row.py.clone()),
    }
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

/// SchemaColumn is one row from the information_schema.columns result set.
#[derive(Debug, Clone)]
struct SchemaColumn {
    table_name: String,
    column_name: String,
    data_type: String,
    is_nullable: String,
}

/// TableColumns holds the ordered columns for one table.
struct TableColumns {
    columns: Vec<SchemaColumn>,
}

/// parse_schema_columns walks the QueryResult rows and groups them by table_name.
/// Returns: map[table]columns, ordered table names, total column count.
fn parse_schema_columns(res: &QueryResult) -> (HashMap<String, TableColumns>, Vec<String>, usize) {
    let mut tables: HashMap<String, TableColumns> = HashMap::new();
    let mut table_order: Vec<String> = Vec::new();
    let mut total_cols = 0usize;

    for row in &res.rows {
        if row.len() < 5 {
            continue;
        }
        let table_name = string_val(&row[0]);
        let column_name = string_val(&row[1]);
        let data_type = string_val(&row[2]).to_lowercase();
        let is_nullable = string_val(&row[3]);
        if table_name.is_empty() || column_name.is_empty() {
            continue;
        }
        if !tables.contains_key(&table_name) {
            tables.insert(table_name.clone(), TableColumns { columns: Vec::new() });
            table_order.push(table_name.clone());
        }
        tables.get_mut(&table_name).unwrap().columns.push(SchemaColumn {
            table_name: table_name.clone(),
            column_name,
            data_type,
            is_nullable,
        });
        total_cols += 1;
    }
    table_order.sort();
    (tables, table_order, total_cols)
}

/// string_val coerces a serde_json::Value (from JSON unmarshal) to String.
fn string_val(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// is_identifier returns true when s consists only of letters, digits, or underscores.
fn is_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// to_pascal_case converts snake_case or mixed identifiers to PascalCase.
/// "user_profile" → "UserProfile", "orders" → "Orders".
fn to_pascal_case(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let mut b = String::new();
    let mut upper = true;
    for c in s.chars() {
        if c == '_' || c == '-' || c == ' ' {
            upper = true;
            continue;
        }
        if upper {
            b.extend(c.to_uppercase());
            upper = false;
        } else {
            b.push(c);
        }
    }
    b
}

// ── TypeScript emitter ────────────────────────────────────────────────────────

fn emit_typescript(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");

    // Per-table interface declarations first.
    for tbl in order {
        let tc = &tables[tbl];
        let iface_name = format!("Table_{}", to_pascal_case(tbl));
        let _ = write!(w, "export interface {iface_name} {{\n");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let ts_type = match map_type(&col.data_type, LangTarget::TypeScript) {
                Some(mt) => {
                    if nullable { mt.nullable.to_string() } else { mt.typ.to_string() }
                }
                None => {
                    let _ = write!(w, "  // WARNING: unknown pg type {}\n", col.data_type);
                    if nullable { "unknown | null".to_string() } else { "unknown".to_string() }
                }
            };
            let _ = write!(w, "  {}: {};\n", col.column_name, ts_type);
        }
        let _ = write!(w, "}}\n\n");
    }

    // Database wrapper following the supabase-shape convention.
    let _ = write!(w, "export interface Database {{\n");
    let _ = write!(w, "  Tables: {{\n");
    for tbl in order {
        let iface_name = format!("Table_{}", to_pascal_case(tbl));
        let _ = write!(w, "    {tbl}: {iface_name};\n");
    }
    let _ = write!(w, "  }};\n");
    let _ = write!(w, "}}\n");
}

// ── Go emitter ────────────────────────────────────────────────────────────────

fn emit_go(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
    pkg_name: &str,
) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");
    let _ = write!(w, "package {pkg_name}\n\n");

    // Collect imports from all mapped types.
    let mut imports: BTreeSet<&str> = BTreeSet::new();
    for tbl in order {
        for col in &tables[tbl].columns {
            if let Some(mt) = map_type(&col.data_type, LangTarget::Go) {
                if !mt.import.is_empty() {
                    imports.insert(mt.import);
                }
            }
        }
    }
    if !imports.is_empty() {
        let _ = write!(w, "import (\n");
        for imp in &imports {
            let _ = write!(w, "\t\"{imp}\"\n");
        }
        let _ = write!(w, ")\n\n");
    }

    // One struct per table with json and db tags.
    for tbl in order {
        let tc = &tables[tbl];
        let struct_name = to_pascal_case(tbl);
        let _ = write!(w, "type {struct_name} struct {{\n");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let go_type = match map_type(&col.data_type, LangTarget::Go) {
                Some(mt) => {
                    if nullable { mt.nullable.to_string() } else { mt.typ.to_string() }
                }
                None => {
                    let _ = write!(w, "\t// WARNING: unknown pg type {}\n", col.data_type);
                    "interface{}".to_string()
                }
            };
            let field_name = to_pascal_case(&col.column_name);
            let _ = write!(
                w,
                "\t{field_name} {go_type} `json:\"{cn}\" db:\"{cn}\"`\n",
                cn = col.column_name
            );
        }
        let _ = write!(w, "}}\n\n");
    }
}

// ── Python emitter ────────────────────────────────────────────────────────────

fn emit_python(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
) {
    let _ = write!(w, "# Code generated by basin gen types — DO NOT EDIT.\n\n");

    // Collect imports from all mapped types. Use a BTreeSet so they sort
    // alphabetically when emitted (mirroring Go's sort.Strings(importList)).
    let mut imports: BTreeSet<String> = BTreeSet::new();
    imports.insert("from pydantic import BaseModel".to_string());

    for tbl in order {
        for col in &tables[tbl].columns {
            if let Some(mt) = map_type(&col.data_type, LangTarget::Python) {
                if !mt.import.is_empty() {
                    // Imports may be multi-line
                    // (e.g. "from decimal import Decimal\nfrom typing import Optional").
                    for line in mt.import.split('\n') {
                        let line = line.trim();
                        if !line.is_empty() {
                            imports.insert(line.to_string());
                        }
                    }
                }
            }
        }
    }
    for imp in &imports {
        let _ = write!(w, "{imp}\n");
    }
    let _ = write!(w, "\n");

    // One class per table.
    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = write!(w, "class {class_name}(BaseModel):\n");
        if tc.columns.is_empty() {
            let _ = write!(w, "    pass\n");
        }
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let py_type = match map_type(&col.data_type, LangTarget::Python) {
                Some(mt) => {
                    if nullable { mt.nullable.to_string() } else { mt.typ.to_string() }
                }
                None => {
                    let _ = write!(w, "    # WARNING: unknown pg type {}\n", col.data_type);
                    if nullable { "Optional[None]".to_string() } else { "None".to_string() }
                }
            };
            let _ = write!(w, "    {}: {py_type}\n", col.column_name);
        }
        let _ = write!(w, "\n");
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_gen(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "gen",
                "Code-generation utilities.",
                &["types <typescript|go|python>   Generate typed database interfaces."],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "types" => cmd_gen_types(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "gen",
                "Code-generation utilities.",
                &["types <typescript|go|python>   Generate typed database interfaces."],
            );
            Ok(())
        }
        other => Err(msg(format!("gen: unknown subcommand {:?} (available: types)", other))),
    }
}

// ── gen types ─────────────────────────────────────────────────────────────────

/// cmd_gen_types is the public entry point called by the dispatcher.
/// It writes generated source to stdout (or to --output=<path>) and
/// informational/JSON metadata to stderr.
pub fn cmd_gen_types(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    gen_types(g, args, &mut std::io::stdout(), &mut std::io::stderr())
}

/// gen_types is the testable inner implementation. Tests inject their own
/// Write values to capture output without touching os.Stdout or os.Stderr.
///
/// - out    — destination for the generated source code (stdout in production)
/// - err_out — destination for progress/JSON metadata (stderr in production)
pub fn gen_types(
    g: &GlobalFlags,
    args: &[String],
    out: &mut dyn Write,
    err_out: &mut dyn Write,
) -> CliResult<()> {
    // ── flag parsing (manual, matching the Go stdlib-only convention) ─────────
    let mut project = String::new();
    let mut schema = "public".to_string();
    let mut output_path = String::new();
    let mut pkg_name = "database".to_string();
    let mut watch = false;
    let mut positional: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "--help" | "-h" => {
                gen_types_usage();
                return Ok(());
            }
            _ if a.starts_with("--project=") => {
                project = a.trim_start_matches("--project=").to_string();
            }
            "--project" => {
                i += 1;
                if i >= args.len() {
                    return Err(msg("gen types: --project requires a value"));
                }
                project = args[i].clone();
            }
            _ if a.starts_with("--schema=") => {
                schema = a.trim_start_matches("--schema=").to_string();
            }
            "--schema" => {
                i += 1;
                if i >= args.len() {
                    return Err(msg("gen types: --schema requires a value"));
                }
                schema = args[i].clone();
            }
            _ if a.starts_with("--output=") => {
                output_path = a.trim_start_matches("--output=").to_string();
            }
            "--output" => {
                i += 1;
                if i >= args.len() {
                    return Err(msg("gen types: --output requires a value"));
                }
                output_path = args[i].clone();
            }
            _ if a.starts_with("--package=") => {
                pkg_name = a.trim_start_matches("--package=").to_string();
            }
            "--package" => {
                i += 1;
                if i >= args.len() {
                    return Err(msg("gen types: --package requires a value"));
                }
                pkg_name = args[i].clone();
            }
            "--watch" => {
                watch = true;
            }
            _ if a.starts_with('-') => {
                return Err(msg(format!("gen types: unknown flag {:?}", a)));
            }
            _ => {
                positional.push(a.clone());
            }
        }
        i += 1;
    }

    if positional.is_empty() {
        gen_types_usage();
        return Err(msg("gen types: <lang> is required (typescript|go|python)"));
    }
    let lang_str = positional[0].to_lowercase();
    let lang = match lang_str.as_str() {
        "typescript" | "ts" => LangTarget::TypeScript,
        "go" => LangTarget::Go,
        "python" | "py" => LangTarget::Python,
        _ => {
            return Err(msg(format!(
                "gen types: unknown language {:?} (want typescript, go, or python)",
                positional[0]
            )))
        }
    };

    // --watch stub — full implementation deferred to a later batch.
    if watch {
        let _ = write!(err_out, "basin: watch not yet implemented\n");
        return Ok(());
    }

    // ── project ref resolution ─────────────────────────────────────────────
    if project.is_empty() {
        // Fall back to working-project context (./basin/config.toml).
        if let Ok(cwd) = std::env::current_dir() {
            if let Ok(Some(wp)) = load_working_project(&cwd) {
                project = wp.project_ref;
            }
        }
    }
    if project.is_empty() {
        return Err(msg("gen types: --project is required (or link a project with `basin link`)"));
    }

    // ── schema name validation ─────────────────────────────────────────────
    // Only allow alphanumeric + underscore to prevent SQL injection via
    // the schema name (it is embedded directly in the query string).
    if !is_identifier(&schema) {
        return Err(msg(format!(
            "gen types: invalid schema name {:?} (must be alphanumeric + underscore)",
            schema
        )));
    }

    // ── HTTP query ────────────────────────────────────────────────────────
    let c = require_client(g)?;
    let sql_query = format!(
        "SELECT table_name, column_name, data_type, is_nullable, ordinal_position \
         FROM information_schema.columns \
         WHERE table_schema = '{}' \
         ORDER BY table_name, ordinal_position",
        schema
    );
    let req_body = json!({ "sql": sql_query, "writes_enabled": false });
    let res: QueryResult = c
        .do_json_timeout(
            Method::POST,
            &format!("/v1/projects/{project}/sql/query"),
            Some(req_body),
            Duration::from_secs(120),
        )
        .map_err(|e| msg(format!("gen types: query failed: {e}")))?;

    // ── parse result rows ─────────────────────────────────────────────────
    let (tables, table_order, total_cols) = parse_schema_columns(&res);

    // ── emit source ───────────────────────────────────────────────────────
    let mut buf: Vec<u8> = Vec::new();
    match lang {
        LangTarget::TypeScript => emit_typescript(&mut buf, &tables, &table_order),
        LangTarget::Go => emit_go(&mut buf, &tables, &table_order, &pkg_name),
        LangTarget::Python => emit_python(&mut buf, &tables, &table_order),
    }
    let src = String::from_utf8(buf).unwrap_or_default();

    // Determine output destination.
    if !output_path.is_empty() {
        std::fs::write(&output_path, src.as_bytes())
            .map_err(|e| msg(format!("gen types: write {output_path}: {e}")))?;
    } else {
        out.write_all(src.as_bytes())
            .map_err(|e| msg(format!("gen types: write output: {e}")))?;
    }

    // ── optional JSON metadata (always to err_out) ─────────────────────
    let out_display = if output_path.is_empty() { "-" } else { &output_path };
    if g.json {
        // JSON shape: { "lang": string, "tables": int, "columns": int, "output_path": string }
        let meta = json!({
            "lang": lang_str,
            "tables": table_order.len(),
            "columns": total_cols,
            "output_path": out_display,
        });
        // Use a Vec<u8> buffer — print_json requires W: Sized, so we can't
        // pass &mut dyn Write directly.
        let mut buf: Vec<u8> = Vec::new();
        crate::output::print_json(&mut buf, &meta)?;
        err_out.write_all(&buf).map_err(|e| msg(format!("gen types: write metadata: {e}")))?;
    } else if !g.quiet {
        let _ = write!(
            err_out,
            "generated {} table(s), {} column(s) → {}\n",
            table_order.len(),
            total_cols,
            out_display,
        );
    }
    Ok(())
}

fn gen_types_usage() {
    help_for_command(
        "gen types",
        "Generate typed database interfaces from information_schema.",
        &[
            "<lang>              Target language: typescript, go, python (required).",
            "--project=<ref>     Project ref (required if not linked).",
            "--schema=public     Schema name to introspect (default: public).",
            "--output=<path>     Write to file instead of stdout.",
            "--package=<name>    Go package name (default: database, Go only).",
            "--watch             Re-emit on every migration push (stub — coming soon).",
        ],
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    // ── stub schema rows (mirrors cmd_gen_test.go's stubSchemaRows) ───────────
    //
    // Table "orders" (4 columns):
    //   id       uuid      NOT NULL
    //   user_id  uuid      YES (nullable)
    //   total    numeric   NOT NULL
    //   metadata jsonb     YES (nullable)
    //
    // Table "users" (5 columns):
    //   id          uuid                     NOT NULL
    //   email       text                     NOT NULL
    //   created_at  timestamp with time zone YES (nullable)
    //   embedding   vector                   YES (nullable)
    //   score       integer                  NOT NULL
    fn stub_schema_server(t: &TestServer) -> &TestServer {
        t
    }

    fn build_schema_server() -> TestServer {
        let rows: Vec<Vec<serde_json::Value>> = vec![
            vec!["orders".into(), "id".into(),       "uuid".into(),                    "NO".into(),  1.into()],
            vec!["orders".into(), "user_id".into(),  "uuid".into(),                    "YES".into(), 2.into()],
            vec!["orders".into(), "total".into(),    "numeric".into(),                 "NO".into(),  3.into()],
            vec!["orders".into(), "metadata".into(), "jsonb".into(),                   "YES".into(), 4.into()],
            vec!["users".into(),  "id".into(),       "uuid".into(),                    "NO".into(),  1.into()],
            vec!["users".into(),  "email".into(),    "text".into(),                    "NO".into(),  2.into()],
            vec!["users".into(),  "created_at".into(),"timestamp with time zone".into(),"YES".into(),3.into()],
            vec!["users".into(),  "embedding".into(), "vector".into(),                 "YES".into(), 4.into()],
            vec!["users".into(),  "score".into(),    "integer".into(),                 "NO".into(),  5.into()],
        ];
        let payload = serde_json::json!({
            "data": {
                "columns": ["table_name","column_name","data_type","is_nullable","ordinal_position"],
                "rows": rows,
                "elapsed_ms": 5
            },
            "error": null
        });
        let payload_str = serde_json::to_string(&payload).unwrap();
        TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert!(req.path.contains("/sql/query"), "unexpected path: {}", req.path);
            Resp::ok(payload_str.clone())
        })
    }

    fn build_empty_schema_server() -> TestServer {
        let payload = serde_json::json!({
            "data": {
                "columns": ["table_name","column_name","data_type","is_nullable","ordinal_position"],
                "rows": [],
                "elapsed_ms": 1
            },
            "error": null
        });
        let payload_str = serde_json::to_string(&payload).unwrap();
        TestServer::start(move |_req: &Req| Resp::ok(payload_str.clone()))
    }

    fn build_500_server() -> TestServer {
        TestServer::start(|_req: &Req| {
            Resp::status(
                500,
                r#"{"error":{"code":"internal_error","message":"something went wrong"}}"#,
            )
        })
    }

    fn g_for(srv: &TestServer) -> GlobalFlags {
        GlobalFlags { api_url: srv.url.clone(), token: "test-token".into(), ..Default::default() }
    }

    fn run_gen(g: &GlobalFlags, args: &[&str]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let mut out = Vec::new();
        let mut err_out = Vec::new();
        gen_types(g, &args, &mut out, &mut err_out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    fn run_gen_both(
        g: &GlobalFlags,
        args: &[&str],
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let mut out = Vec::new();
        let mut err_out = Vec::new();
        gen_types(g, &args, &mut out, &mut err_out)?;
        Ok((String::from_utf8(out).unwrap(), String::from_utf8(err_out).unwrap()))
    }

    // ── Snapshot tests ────────────────────────────────────────────────────────

    #[test]
    fn snapshot_typescript() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["typescript", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.ts").expect("read testdata/expected.ts");
        assert_eq!(
            got, expected,
            "TypeScript snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_go() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["go", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.go").expect("read testdata/expected.go");
        assert_eq!(
            got, expected,
            "Go snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_python() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["python", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.py").expect("read testdata/expected.py");
        assert_eq!(
            got, expected,
            "Python snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    // ── Error paths ───────────────────────────────────────────────────────────

    #[test]
    fn unknown_lang() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let err = run_gen(&g_for(&srv), &["ruby", "--project=test-ref"]).unwrap_err();
        assert!(err.to_string().contains("unknown language"), "err={err}");
    }

    #[test]
    fn missing_project() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        // No --project passed and no working-project config → error.
        let err = run_gen(&g_for(&srv), &["typescript"]).unwrap_err();
        assert!(err.to_string().contains("--project is required"), "err={err}");
    }

    #[test]
    fn server_error_500() {
        let _cfg = with_temp_config_dir();
        let srv = build_500_server();
        let err = run_gen(&g_for(&srv), &["typescript", "--project=test-ref"]).unwrap_err();
        assert!(err.to_string().contains("query failed"), "err={err}");
    }

    #[test]
    fn empty_schema() {
        let _cfg = with_temp_config_dir();
        let srv = build_empty_schema_server();
        let got = run_gen(&g_for(&srv), &["typescript", "--project=test-ref"]).unwrap();
        assert!(got.contains("Code generated by basin gen types"), "missing header: {got}");
        assert!(got.contains("export interface Database"), "missing Database interface: {got}");
        assert!(!got.contains("export interface Table_"), "unexpected table interface: {got}");
    }

    // ── --output=<path> writes to file ───────────────────────────────────────

    #[test]
    fn output_to_file() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let tmp = std::env::temp_dir().join(format!("gen-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let out_path = tmp.join("database.ts");

        let g = g_for(&srv);
        let args: Vec<String> = vec![
            "typescript".into(),
            "--project=test-ref".into(),
            format!("--output={}", out_path.display()),
        ];
        let mut out = Vec::new();
        let mut err_out = Vec::new();
        gen_types(&g, &args, &mut out, &mut err_out).unwrap();

        let file_content = std::fs::read_to_string(&out_path).expect("output file not created");
        // Content must match the stdout case.
        let stdout_got = run_gen(&g, &["typescript", "--project=test-ref"]).unwrap();
        assert_eq!(file_content, stdout_got, "file content differs from writer output");
        std::fs::remove_dir_all(&tmp).ok();
    }

    // ── JSON metadata shape ───────────────────────────────────────────────────

    #[test]
    fn json_metadata() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let mut g = g_for(&srv);
        g.json = true;
        let (_out, err_str) =
            run_gen_both(&g, &["typescript", "--project=test-ref"]).unwrap();
        let meta: serde_json::Value = serde_json::from_str(&err_str).expect("JSON metadata parse");
        assert_eq!(meta["lang"].as_str(), Some("typescript"));
        assert_eq!(meta["tables"].as_i64(), Some(2));
        assert_eq!(meta["columns"].as_i64(), Some(9));
        assert_eq!(meta["output_path"].as_str(), Some("-"));
    }

    // ── --watch stub ──────────────────────────────────────────────────────────

    #[test]
    fn watch_stub() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "test-token".into(),
            api_url: "http://127.0.0.1:0".into(),
            ..Default::default()
        };
        run_gen(&g, &["typescript", "--project=test-ref", "--watch"]).unwrap();
    }

    // ── Dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn dispatcher_unknown_subcommand() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags::default();
        let err = cmd_gen(&g, &["migrations".into()]).unwrap_err();
        assert!(err.to_string().contains("unknown subcommand"), "err={err}");
    }

    #[test]
    fn dispatcher_no_args() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags::default();
        cmd_gen(&g, &[]).unwrap();
    }

    // ── to_pascal_case helper ─────────────────────────────────────────────────

    #[test]
    fn pascal_case_cases() {
        let cases = &[
            ("users", "Users"),
            ("user_profile", "UserProfile"),
            ("order_line_item", "OrderLineItem"),
            ("id", "Id"),
            ("", ""),
            ("already_pascal", "AlreadyPascal"),
        ];
        for (input, want) in cases {
            let got = to_pascal_case(input);
            assert_eq!(&got, want, "to_pascal_case({input:?})");
        }
    }

    // ── is_identifier helper ──────────────────────────────────────────────────

    #[test]
    fn identifier_cases() {
        let cases: &[(&str, bool)] = &[
            ("public", true),
            ("my_schema", true),
            ("schema123", true),
            ("", false),
            ("bad-schema", false),
            ("bad schema", false),
            ("drop table", false),
            ("'; DROP TABLE", false),
        ];
        for (input, want) in cases {
            let got = is_identifier(input);
            assert_eq!(got, *want, "is_identifier({input:?})");
        }
    }

    // ── parse_schema_columns ──────────────────────────────────────────────────

    #[test]
    fn parse_schema_columns_basic() {
        let rows: Vec<Vec<serde_json::Value>> = vec![
            vec!["orders".into(), "id".into(),       "uuid".into(),    "NO".into(),  1.into()],
            vec!["orders".into(), "user_id".into(),  "uuid".into(),    "YES".into(), 2.into()],
            vec!["orders".into(), "total".into(),    "numeric".into(), "NO".into(),  3.into()],
            vec!["orders".into(), "metadata".into(), "jsonb".into(),   "YES".into(), 4.into()],
            vec!["users".into(),  "id".into(),       "uuid".into(),    "NO".into(),  1.into()],
            vec!["users".into(),  "email".into(),    "text".into(),    "NO".into(),  2.into()],
            vec!["users".into(),  "created_at".into(),"timestamp with time zone".into(),"YES".into(),3.into()],
            vec!["users".into(),  "embedding".into(), "vector".into(), "YES".into(), 4.into()],
            vec!["users".into(),  "score".into(),    "integer".into(), "NO".into(),  5.into()],
        ];
        let res = QueryResult {
            columns: vec![
                "table_name".into(), "column_name".into(), "data_type".into(),
                "is_nullable".into(), "ordinal_position".into(),
            ],
            rows,
            ..Default::default()
        };
        let (tables, order, total) = parse_schema_columns(&res);
        assert_eq!(order.len(), 2);
        assert_eq!(total, 9);
        assert_eq!(order[0], "orders");
        assert_eq!(order[1], "users");
        assert_eq!(tables["orders"].columns.len(), 4);
        assert_eq!(tables["users"].columns.len(), 5);
    }

    // ── TypeScript emit shape ─────────────────────────────────────────────────

    #[test]
    fn ts_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["typescript", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "export interface Table_Orders",
            "export interface Table_Users",
            "export interface Database",
            "Tables:",
            "id: string;",
            "user_id: string | null;",
            "total: string;",
            "metadata: Record<string, unknown> | null;",
            "created_at: string | null;",
            "embedding: number[] | null;",
            "score: number;",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Go emit shape ─────────────────────────────────────────────────────────

    #[test]
    fn go_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["go", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "package database",
            r#""encoding/json""#,
            r#""time""#,
            "type Orders struct",
            "type Users struct",
            "Id [16]byte",
            "UserId *[16]byte",
            "Total string",
            "Metadata json.RawMessage",
            "Email string",
            "CreatedAt *time.Time",
            "Embedding []float32",
            "Score int32",
            r#"json:"id""#,
            r#"db:"id""#,
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Python emit shape ─────────────────────────────────────────────────────

    #[test]
    fn py_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["python", "--project=test-ref"]).unwrap();
        let checks = &[
            "# Code generated by basin gen types — DO NOT EDIT.",
            "from pydantic import BaseModel",
            "from uuid import UUID",
            "class Orders(BaseModel):",
            "class Users(BaseModel):",
            "id: UUID",
            "user_id: Optional[UUID]",
            "email: str",
            "created_at: Optional[datetime]",
            "embedding: Optional[list[float]]",
            "score: int",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── --package flag ────────────────────────────────────────────────────────

    #[test]
    fn go_package_flag() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got =
            run_gen(&g_for(&srv), &["go", "--project=test-ref", "--package=mydb"]).unwrap();
        assert!(got.contains("package mydb"), "expected 'package mydb' in:\n{got}");
    }

    // ── unknown flag ──────────────────────────────────────────────────────────

    #[test]
    fn unknown_flag() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), api_url: "http://127.0.0.1:0".into(), ..Default::default() };
        let err = run_gen(&g, &["typescript", "--project=ref", "--no-such-flag"]).unwrap_err();
        assert!(!err.to_string().is_empty(), "expected error for unknown flag");
    }

    // ── language aliases ──────────────────────────────────────────────────────

    #[test]
    fn lang_alias_ts() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["ts", "--project=test-ref"]).unwrap();
        assert!(got.contains("export interface Database"), "'ts' alias did not emit TypeScript");
    }

    #[test]
    fn lang_alias_py() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["py", "--project=test-ref"]).unwrap();
        assert!(got.contains("class Orders(BaseModel)"), "'py' alias did not emit Python");
    }

    // ── Type mapping table completeness ───────────────────────────────────────

    #[test]
    fn map_type_table_completeness() {
        let table = type_table();
        let langs = [LangTarget::TypeScript, LangTarget::Go, LangTarget::Python];
        for (pg_name, _row) in &table {
            for &lang in &langs {
                let mt = map_type(pg_name, lang).expect(
                    &format!("map_type({pg_name:?}, {lang:?}) returned None but key exists")
                );
                assert!(!mt.typ.is_empty(), "map_type({pg_name:?}, {lang:?}).typ is empty");
                assert!(!mt.nullable.is_empty(), "map_type({pg_name:?}, {lang:?}).nullable is empty");
            }
        }
    }

    // ── Unknown type path (ok=false) ──────────────────────────────────────────

    #[test]
    fn map_type_unknown() {
        assert!(map_type("money", LangTarget::TypeScript).is_none(), "money should be unknown");
        assert!(map_type("xml", LangTarget::Go).is_none(), "xml should be unknown");
        assert!(map_type("point", LangTarget::Python).is_none(), "point should be unknown");
        assert!(map_type("not_a_real_pg_type", LangTarget::TypeScript).is_none());
        assert!(map_type("", LangTarget::Go).is_none(), "empty string should be unknown");
    }

    // ── Specific type mappings ─────────────────────────────────────────────────

    #[test]
    fn map_boolean() {
        let mt = map_type("boolean", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "boolean");
        assert_eq!(mt.nullable, "boolean | null");
        let mt = map_type("boolean", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "bool");
        assert_eq!(mt.nullable, "*bool");
        let mt = map_type("boolean", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "bool");
        assert_eq!(mt.nullable, "Optional[bool]");
        assert_eq!(mt.import, "from typing import Optional");
    }

    #[test]
    fn map_uuid() {
        let mt = map_type("uuid", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "string");
        let mt = map_type("uuid", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "[16]byte");
        assert_eq!(mt.nullable, "*[16]byte");
        let mt = map_type("uuid", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "UUID");
        assert_eq!(mt.nullable, "Optional[UUID]");
        assert!(mt.import.contains("from uuid import UUID"));
    }

    #[test]
    fn map_jsonb() {
        let mt = map_type("jsonb", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "Record<string, unknown>");
        assert_eq!(mt.nullable, "Record<string, unknown> | null");
        let mt = map_type("jsonb", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "json.RawMessage");
        assert_eq!(mt.nullable, "json.RawMessage"); // slice is reference type
        assert_eq!(mt.import, "encoding/json");
        let mt = map_type("jsonb", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "Any");
        assert_eq!(mt.import, "from typing import Any, Optional");
    }

    #[test]
    fn map_numeric() {
        let mt = map_type("numeric", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "Decimal");
        assert_eq!(mt.nullable, "Optional[Decimal]");
        assert!(mt.import.contains("from decimal import Decimal"));
    }

    #[test]
    fn map_vector() {
        let mt = map_type("vector", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "number[]");
        assert_eq!(mt.nullable, "number[] | null");
        let mt = map_type("vector", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "[]float32");
        assert_eq!(mt.nullable, "[]float32"); // slice is reference type
        let mt = map_type("vector", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "list[float]");
        assert_eq!(mt.nullable, "Optional[list[float]]");
    }

    #[test]
    fn map_timestamp_with_tz() {
        let mt = map_type("timestamp with time zone", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "time.Time");
        assert_eq!(mt.import, "time");
        let mt = map_type("timestamp with time zone", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "datetime");
        assert!(mt.import.contains("from datetime import datetime"));
    }

    #[test]
    fn map_bytea() {
        let mt = map_type("bytea", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "Uint8Array");
        assert_eq!(mt.nullable, "Uint8Array | null");
        let mt = map_type("bytea", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "[]byte");
        assert_eq!(mt.nullable, "[]byte"); // nil == null in JSON
    }

    #[test]
    fn map_interval() {
        let mt = map_type("interval", LangTarget::TypeScript).unwrap();
        assert_eq!(mt.typ, "string");
        let mt = map_type("interval", LangTarget::Go).unwrap();
        assert_eq!(mt.typ, "string");
        let mt = map_type("interval", LangTarget::Python).unwrap();
        assert_eq!(mt.typ, "str");
    }
}
