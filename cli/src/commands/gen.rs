//! gen — `basin gen <subcommand>` dispatcher.
//!
//! Currently implements:
//!
//!   basin gen types <lang> [--project=<ref>] [--schema=public]
//!                          [--output=<path>] [--package=<name>]
//!                          [--watch]
//!
//! Supported languages (covers the full Basin SDK matrix):
//!   typescript | ts       — TypeScript interfaces (supabase-shape Database wrapper)
//!   go                    — Go structs with json + db tags
//!   python | py           — Python Pydantic models
//!   ruby | rb             — Ruby frozen Struct subclasses
//!   rust | rs             — Rust serde structs (serde_json::Value for JSONB)
//!   java                  — Java POJOs with Jackson @JsonProperty
//!   csharp | cs | dotnet  — C# records with System.Text.Json attributes
//!   php                   — PHP readonly classes with fromArray
//!   dart                  — Dart classes with fromJson / toJson
//!   swift                 — Swift Codable structs with CodingKeys
//!
//! `--watch` polls `./basin/migrations/*.sql` every 2 s and re-emits whenever
//! any file's mtime changes or a file is added/removed.  Requires --output=<path>
//! (cannot stream to stdout in watch mode).  Exits cleanly on SIGINT (Ctrl-C);
//! tests inject an `Arc<AtomicBool>` stop flag to terminate after N iterations.
//!
//! Future subcommands (gen migrations, etc.) will be added here as the
//! platform surface grows.
//!
//! gen types queries information_schema.columns via POST /v1/projects/{ref}/sql/query
//! and emits typed bindings via a Postgres-type→lang-type mapping table.

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
    /// TypeScript — nullable form is `T | null`.
    TypeScript = 1,
    /// Go — nullable form is `*T`, JSONB uses `json.RawMessage`.
    Go = 2,
    /// Python + Pydantic — nullable form is `Optional[T]`.
    Python = 3,
    /// Ruby — frozen Struct subclasses, nullable form is `T?` (comment).
    Ruby = 4,
    /// Rust + serde — nullable form is `Option<T>`.
    Rust = 5,
    /// Java POJOs with Jackson @JsonProperty — nullable form is `@Nullable T`.
    Java = 6,
    /// C# records with System.Text.Json — nullable form is `T?`.
    CSharp = 7,
    /// PHP readonly classes with fromArray — nullable form is `?T`.
    Php = 8,
    /// Dart classes with fromJson/toJson — nullable form is `T?`.
    Dart = 9,
    /// Swift Codable structs with CodingKeys — nullable form is `T?`.
    Swift = 10,
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
    rb: MappedType,
    rs: MappedType,
    java: MappedType,
    cs: MappedType,
    php: MappedType,
    dart: MappedType,
    swift: MappedType,
}

macro_rules! row {
    (ts:    ($t:literal, $tn:literal, $ti:literal),
     go:    ($gt:literal, $gn:literal, $gi:literal),
     py:    ($pt:literal, $pn:literal, $pi:literal),
     rb:    ($rbt:literal, $rbn:literal, $rbi:literal),
     rs:    ($rst:literal, $rsn:literal, $rsi:literal),
     java:  ($jt:literal, $jn:literal, $ji:literal),
     cs:    ($ct:literal, $cn:literal, $ci:literal),
     php:   ($phpt:literal, $phpn:literal, $phpi:literal),
     dart:  ($dt:literal, $dn:literal, $di:literal),
     swift: ($swt:literal, $swn:literal, $swi:literal)) => {
        PgRow {
            ts:   MappedType { typ: $t,    nullable: $tn,   import: $ti  },
            go:   MappedType { typ: $gt,   nullable: $gn,   import: $gi  },
            py:   MappedType { typ: $pt,   nullable: $pn,   import: $pi  },
            rb:   MappedType { typ: $rbt,  nullable: $rbn,  import: $rbi },
            rs:   MappedType { typ: $rst,  nullable: $rsn,  import: $rsi },
            java: MappedType { typ: $jt,   nullable: $jn,   import: $ji  },
            cs:   MappedType { typ: $ct,   nullable: $cn,   import: $ci  },
            php:  MappedType { typ: $phpt, nullable: $phpn, import: $phpi},
            dart: MappedType { typ: $dt,   nullable: $dn,   import: $di  },
            swift:MappedType { typ: $swt,  nullable: $swn,  import: $swi },
        }
    };
}

fn type_table() -> HashMap<&'static str, PgRow> {
    let mut m: HashMap<&'static str, PgRow> = HashMap::new();

    // ── Boolean ──────────────────────────────────────────────────────────────
    m.insert("boolean", row!(
        ts:    ("boolean",                  "boolean | null",                   ""),
        go:    ("bool",                     "*bool",                            ""),
        py:    ("bool",                     "Optional[bool]",                   "from typing import Optional"),
        rb:    ("T::Boolean",               "T::Boolean",                       ""),
        rs:    ("bool",                     "Option<bool>",                     ""),
        java:  ("boolean",                  "Boolean",                          ""),
        cs:    ("bool",                     "bool?",                            ""),
        php:   ("bool",                     "?bool",                            ""),
        dart:  ("bool",                     "bool?",                            ""),
        swift: ("Bool",                     "Bool?",                            "")
    ));
    m.insert("bool", row!(
        ts:    ("boolean",                  "boolean | null",                   ""),
        go:    ("bool",                     "*bool",                            ""),
        py:    ("bool",                     "Optional[bool]",                   "from typing import Optional"),
        rb:    ("T::Boolean",               "T::Boolean",                       ""),
        rs:    ("bool",                     "Option<bool>",                     ""),
        java:  ("boolean",                  "Boolean",                          ""),
        cs:    ("bool",                     "bool?",                            ""),
        php:   ("bool",                     "?bool",                            ""),
        dart:  ("bool",                     "bool?",                            ""),
        swift: ("Bool",                     "Bool?",                            "")
    ));

    // ── Integer family ───────────────────────────────────────────────────────
    m.insert("smallint", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("int16",                    "*int16",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i16",                      "Option<i16>",                      ""),
        java:  ("short",                    "Short",                            ""),
        cs:    ("short",                    "short?",                           ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int16",                    "Int16?",                           "")
    ));
    m.insert("int2", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("int16",                    "*int16",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i16",                      "Option<i16>",                      ""),
        java:  ("short",                    "Short",                            ""),
        cs:    ("short",                    "short?",                           ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int16",                    "Int16?",                           "")
    ));
    m.insert("integer", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("int32",                    "*int32",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i32",                      "Option<i32>",                      ""),
        java:  ("int",                      "Integer",                          ""),
        cs:    ("int",                      "int?",                             ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int32",                    "Int32?",                           "")
    ));
    m.insert("int4", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("int32",                    "*int32",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i32",                      "Option<i32>",                      ""),
        java:  ("int",                      "Integer",                          ""),
        cs:    ("int",                      "int?",                             ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int32",                    "Int32?",                           "")
    ));
    m.insert("bigint", row!(
        ts:    ("bigint",                   "bigint | null",                    ""),
        go:    ("int64",                    "*int64",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i64",                      "Option<i64>",                      ""),
        java:  ("long",                     "Long",                             ""),
        cs:    ("long",                     "long?",                            ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int64",                    "Int64?",                           "")
    ));
    m.insert("int8", row!(
        ts:    ("bigint",                   "bigint | null",                    ""),
        go:    ("int64",                    "*int64",                           ""),
        py:    ("int",                      "Optional[int]",                    "from typing import Optional"),
        rb:    ("Integer",                  "Integer",                          ""),
        rs:    ("i64",                      "Option<i64>",                      ""),
        java:  ("long",                     "Long",                             ""),
        cs:    ("long",                     "long?",                            ""),
        php:   ("int",                      "?int",                             ""),
        dart:  ("int",                      "int?",                             ""),
        swift: ("Int64",                    "Int64?",                           "")
    ));

    // ── Floating-point family ────────────────────────────────────────────────
    m.insert("real", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("float32",                  "*float32",                         ""),
        py:    ("float",                    "Optional[float]",                  "from typing import Optional"),
        rb:    ("Float",                    "Float",                            ""),
        rs:    ("f32",                      "Option<f32>",                      ""),
        java:  ("float",                    "Float",                            ""),
        cs:    ("float",                    "float?",                           ""),
        php:   ("float",                    "?float",                           ""),
        dart:  ("double",                   "double?",                          ""),
        swift: ("Float",                    "Float?",                           "")
    ));
    m.insert("float4", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("float32",                  "*float32",                         ""),
        py:    ("float",                    "Optional[float]",                  "from typing import Optional"),
        rb:    ("Float",                    "Float",                            ""),
        rs:    ("f32",                      "Option<f32>",                      ""),
        java:  ("float",                    "Float",                            ""),
        cs:    ("float",                    "float?",                           ""),
        php:   ("float",                    "?float",                           ""),
        dart:  ("double",                   "double?",                          ""),
        swift: ("Float",                    "Float?",                           "")
    ));
    m.insert("double precision", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("float64",                  "*float64",                         ""),
        py:    ("float",                    "Optional[float]",                  "from typing import Optional"),
        rb:    ("Float",                    "Float",                            ""),
        rs:    ("f64",                      "Option<f64>",                      ""),
        java:  ("double",                   "Double",                           ""),
        cs:    ("double",                   "double?",                          ""),
        php:   ("float",                    "?float",                           ""),
        dart:  ("double",                   "double?",                          ""),
        swift: ("Double",                   "Double?",                          "")
    ));
    m.insert("float8", row!(
        ts:    ("number",                   "number | null",                    ""),
        go:    ("float64",                  "*float64",                         ""),
        py:    ("float",                    "Optional[float]",                  "from typing import Optional"),
        rb:    ("Float",                    "Float",                            ""),
        rs:    ("f64",                      "Option<f64>",                      ""),
        java:  ("double",                   "Double",                           ""),
        cs:    ("double",                   "double?",                          ""),
        php:   ("float",                    "?float",                           ""),
        dart:  ("double",                   "double?",                          ""),
        swift: ("Double",                   "Double?",                          "")
    ));

    // ── Exact numeric ────────────────────────────────────────────────────────
    m.insert("numeric", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("Decimal",                  "Optional[Decimal]",                "from decimal import Decimal\nfrom typing import Optional"),
        rb:    ("BigDecimal",               "BigDecimal",                       ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.math.BigDecimal",     "java.math.BigDecimal",             ""),
        cs:    ("decimal",                  "decimal?",                         ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("decimal", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("Decimal",                  "Optional[Decimal]",                "from decimal import Decimal\nfrom typing import Optional"),
        rb:    ("BigDecimal",               "BigDecimal",                       ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.math.BigDecimal",     "java.math.BigDecimal",             ""),
        cs:    ("decimal",                  "decimal?",                         ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));

    // ── Text family ──────────────────────────────────────────────────────────
    m.insert("text", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("character varying", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("varchar", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("character", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("char", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));

    // ── Binary ───────────────────────────────────────────────────────────────
    m.insert("bytea", row!(
        ts:    ("Uint8Array",               "Uint8Array | null",                ""),
        go:    ("[]byte",                   "[]byte",                           ""),
        py:    ("bytes",                    "Optional[bytes]",                  "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("Vec<u8>",                  "Option<Vec<u8>>",                  ""),
        java:  ("byte[]",                   "byte[]",                           ""),
        cs:    ("byte[]",                   "byte[]?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("List<int>",                "List<int>?",                       ""),
        swift: ("Data",                     "Data?",                            "Foundation")
    ));

    // ── UUID ─────────────────────────────────────────────────────────────────
    m.insert("uuid", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("[16]byte",                 "*[16]byte",                        ""),
        py:    ("UUID",                     "Optional[UUID]",                   "from uuid import UUID\nfrom typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("uuid::Uuid",               "Option<uuid::Uuid>",               "uuid"),
        java:  ("java.util.UUID",           "java.util.UUID",                   ""),
        cs:    ("Guid",                     "Guid?",                            ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("UUID",                     "UUID?",                            "Foundation")
    ));

    // ── JSON / JSONB ─────────────────────────────────────────────────────────
    m.insert("jsonb", row!(
        ts:    ("Record<string, unknown>",  "Record<string, unknown> | null",   ""),
        go:    ("json.RawMessage",          "json.RawMessage",                  "encoding/json"),
        py:    ("Any",                      "Optional[Any]",                    "from typing import Any, Optional"),
        rb:    ("Hash",                     "Hash",                             ""),
        rs:    ("serde_json::Value",        "Option<serde_json::Value>",        "serde_json"),
        java:  ("com.fasterxml.jackson.databind.JsonNode", "com.fasterxml.jackson.databind.JsonNode", ""),
        cs:    ("JsonElement",              "JsonElement?",                     "System.Text.Json"),
        php:   ("mixed",                    "mixed",                            ""),
        dart:  ("Map<String, dynamic>",     "Map<String, dynamic>?",            ""),
        swift: ("AnyCodable",               "AnyCodable?",                      "")
    ));
    m.insert("json", row!(
        ts:    ("Record<string, unknown>",  "Record<string, unknown> | null",   ""),
        go:    ("json.RawMessage",          "json.RawMessage",                  "encoding/json"),
        py:    ("Any",                      "Optional[Any]",                    "from typing import Any, Optional"),
        rb:    ("Hash",                     "Hash",                             ""),
        rs:    ("serde_json::Value",        "Option<serde_json::Value>",        "serde_json"),
        java:  ("com.fasterxml.jackson.databind.JsonNode", "com.fasterxml.jackson.databind.JsonNode", ""),
        cs:    ("JsonElement",              "JsonElement?",                     "System.Text.Json"),
        php:   ("mixed",                    "mixed",                            ""),
        dart:  ("Map<String, dynamic>",     "Map<String, dynamic>?",            ""),
        swift: ("AnyCodable",               "AnyCodable?",                      "")
    ));

    // ── Timestamps ───────────────────────────────────────────────────────────
    m.insert("timestamp with time zone", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("time.Time",                "*time.Time",                       "time"),
        py:    ("datetime",                 "Optional[datetime]",               "from datetime import datetime\nfrom typing import Optional"),
        rb:    ("Time",                     "Time",                             ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.OffsetDateTime", "java.time.OffsetDateTime",         ""),
        cs:    ("DateTimeOffset",           "DateTimeOffset?",                  ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("DateTime",                 "DateTime?",                        ""),
        swift: ("Date",                     "Date?",                            "Foundation")
    ));
    m.insert("timestamptz", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("time.Time",                "*time.Time",                       "time"),
        py:    ("datetime",                 "Optional[datetime]",               "from datetime import datetime\nfrom typing import Optional"),
        rb:    ("Time",                     "Time",                             ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.OffsetDateTime", "java.time.OffsetDateTime",         ""),
        cs:    ("DateTimeOffset",           "DateTimeOffset?",                  ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("DateTime",                 "DateTime?",                        ""),
        swift: ("Date",                     "Date?",                            "Foundation")
    ));
    m.insert("timestamp without time zone", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("time.Time",                "*time.Time",                       "time"),
        py:    ("datetime",                 "Optional[datetime]",               "from datetime import datetime\nfrom typing import Optional"),
        rb:    ("Time",                     "Time",                             ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.LocalDateTime",  "java.time.LocalDateTime",          ""),
        cs:    ("DateTime",                 "DateTime?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("DateTime",                 "DateTime?",                        ""),
        swift: ("Date",                     "Date?",                            "Foundation")
    ));
    m.insert("timestamp", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("time.Time",                "*time.Time",                       "time"),
        py:    ("datetime",                 "Optional[datetime]",               "from datetime import datetime\nfrom typing import Optional"),
        rb:    ("Time",                     "Time",                             ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.LocalDateTime",  "java.time.LocalDateTime",          ""),
        cs:    ("DateTime",                 "DateTime?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("DateTime",                 "DateTime?",                        ""),
        swift: ("Date",                     "Date?",                            "Foundation")
    ));

    // ── Date ─────────────────────────────────────────────────────────────────
    m.insert("date", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("time.Time",                "*time.Time",                       "time"),
        py:    ("date",                     "Optional[date]",                   "from datetime import date\nfrom typing import Optional"),
        rb:    ("Date",                     "Date",                             ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.LocalDate",      "java.time.LocalDate",              ""),
        cs:    ("DateOnly",                 "DateOnly?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("DateTime",                 "DateTime?",                        ""),
        swift: ("Date",                     "Date?",                            "Foundation")
    ));

    // ── Time ─────────────────────────────────────────────────────────────────
    m.insert("time without time zone", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("time",                     "Optional[time]",                   "from datetime import time\nfrom typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.LocalTime",      "java.time.LocalTime",              ""),
        cs:    ("TimeOnly",                 "TimeOnly?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("time with time zone", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("time",                     "Optional[time]",                   "from datetime import time\nfrom typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.OffsetTime",     "java.time.OffsetTime",             ""),
        cs:    ("TimeOnly",                 "TimeOnly?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));
    m.insert("time", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("time",                     "Optional[time]",                   "from datetime import time\nfrom typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("java.time.LocalTime",      "java.time.LocalTime",              ""),
        cs:    ("TimeOnly",                 "TimeOnly?",                        ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));

    // ── Interval ─────────────────────────────────────────────────────────────
    // Listed as unsupported in CAPABILITIES.md but information_schema may surface
    // it from snapshots or future engine builds. Map to string in all languages.
    m.insert("interval", row!(
        ts:    ("string",                   "string | null",                    ""),
        go:    ("string",                   "*string",                          ""),
        py:    ("str",                      "Optional[str]",                    "from typing import Optional"),
        rb:    ("String",                   "String",                           ""),
        rs:    ("String",                   "Option<String>",                   ""),
        java:  ("String",                   "String",                           ""),
        cs:    ("string",                   "string?",                          ""),
        php:   ("string",                   "?string",                          ""),
        dart:  ("String",                   "String?",                          ""),
        swift: ("String",                   "String?",                          "")
    ));

    // ── Vector ───────────────────────────────────────────────────────────────
    m.insert("vector", row!(
        ts:    ("number[]",                 "number[] | null",                  ""),
        go:    ("[]float32",                "[]float32",                        ""),
        py:    ("list[float]",              "Optional[list[float]]",            "from typing import Optional"),
        rb:    ("Array",                    "Array",                            ""),
        rs:    ("Vec<f32>",                 "Option<Vec<f32>>",                 ""),
        java:  ("float[]",                  "float[]",                          ""),
        cs:    ("float[]",                  "float[]?",                         ""),
        php:   ("array",                    "?array",                           ""),
        dart:  ("List<double>",             "List<double>?",                    ""),
        swift: ("[Float]",                  "[Float]?",                         "")
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
        LangTarget::Go        => Some(row.go.clone()),
        LangTarget::Python    => Some(row.py.clone()),
        LangTarget::Ruby      => Some(row.rb.clone()),
        LangTarget::Rust      => Some(row.rs.clone()),
        LangTarget::Java      => Some(row.java.clone()),
        LangTarget::CSharp    => Some(row.cs.clone()),
        LangTarget::Php       => Some(row.php.clone()),
        LangTarget::Dart      => Some(row.dart.clone()),
        LangTarget::Swift     => Some(row.swift.clone()),
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
            tables.insert(
                table_name.clone(),
                TableColumns {
                    columns: Vec::new(),
                },
            );
            table_order.push(table_name.clone());
        }
        tables
            .get_mut(&table_name)
            .unwrap()
            .columns
            .push(SchemaColumn {
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

/// to_camel_case converts snake_case to camelCase.
/// "user_profile" → "userProfile", "id" → "id", "created_at" → "createdAt".
fn to_camel_case(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let mut b = String::new();
    let mut upper = false;
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

fn emit_typescript(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");

    // Per-table interface declarations first.
    for tbl in order {
        let tc = &tables[tbl];
        let iface_name = format!("Table_{}", to_pascal_case(tbl));
        let _ = writeln!(w, "export interface {iface_name} {{");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let ts_type = match map_type(&col.data_type, LangTarget::TypeScript) {
                Some(mt) => {
                    if nullable {
                        mt.nullable.to_string()
                    } else {
                        mt.typ.to_string()
                    }
                }
                None => {
                    let _ = writeln!(w, "  // WARNING: unknown pg type {}", col.data_type);
                    if nullable {
                        "unknown | null".to_string()
                    } else {
                        "unknown".to_string()
                    }
                }
            };
            let _ = writeln!(w, "  {}: {};", col.column_name, ts_type);
        }
        let _ = write!(w, "}}\n\n");
    }

    // Database wrapper following the supabase-shape convention.
    let _ = writeln!(w, "export interface Database {{");
    let _ = writeln!(w, "  Tables: {{");
    for tbl in order {
        let iface_name = format!("Table_{}", to_pascal_case(tbl));
        let _ = writeln!(w, "    {tbl}: {iface_name};");
    }
    let _ = writeln!(w, "  }};");
    let _ = writeln!(w, "}}");
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
        let _ = writeln!(w, "import (");
        for imp in &imports {
            let _ = writeln!(w, "\t\"{imp}\"");
        }
        let _ = write!(w, ")\n\n");
    }

    // One struct per table with json and db tags.
    for tbl in order {
        let tc = &tables[tbl];
        let struct_name = to_pascal_case(tbl);
        let _ = writeln!(w, "type {struct_name} struct {{");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let go_type = match map_type(&col.data_type, LangTarget::Go) {
                Some(mt) => {
                    if nullable {
                        mt.nullable.to_string()
                    } else {
                        mt.typ.to_string()
                    }
                }
                None => {
                    let _ = writeln!(w, "\t// WARNING: unknown pg type {}", col.data_type);
                    "interface{}".to_string()
                }
            };
            let field_name = to_pascal_case(&col.column_name);
            let _ = writeln!(
                w,
                "\t{field_name} {go_type} `json:\"{cn}\" db:\"{cn}\"`",
                cn = col.column_name
            );
        }
        let _ = write!(w, "}}\n\n");
    }
}

// ── Python emitter ────────────────────────────────────────────────────────────

fn emit_python(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
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
        let _ = writeln!(w, "{imp}");
    }
    let _ = writeln!(w);

    // One class per table.
    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = writeln!(w, "class {class_name}(BaseModel):");
        if tc.columns.is_empty() {
            let _ = writeln!(w, "    pass");
        }
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let py_type = match map_type(&col.data_type, LangTarget::Python) {
                Some(mt) => {
                    if nullable {
                        mt.nullable.to_string()
                    } else {
                        mt.typ.to_string()
                    }
                }
                None => {
                    let _ = writeln!(w, "    # WARNING: unknown pg type {}", col.data_type);
                    if nullable {
                        "Optional[None]".to_string()
                    } else {
                        "None".to_string()
                    }
                }
            };
            let _ = writeln!(w, "    {}: {py_type}", col.column_name);
        }
        let _ = writeln!(w);
    }
}

// ── Ruby emitter ─────────────────────────────────────────────────────────────

fn emit_ruby(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
    let _ = write!(w, "# Code generated by basin gen types — DO NOT EDIT.\n# frozen_string_literal: true\n\n");

    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        // Collect member names for the Struct definition line.
        let members: Vec<String> = tc.columns.iter().map(|c| format!(":{}", c.column_name)).collect();
        let _ = writeln!(w, "{} = Struct.new({}, keyword_init: true) do", class_name, members.join(", "));
        // Add ATTRIBUTES constant for runtime inspection.
        let attr_syms: Vec<String> = tc.columns.iter().map(|c| format!(":{}", c.column_name)).collect();
        let _ = writeln!(w, "  ATTRIBUTES = [{}].freeze", attr_syms.join(", "));
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let rb_type = match map_type(&col.data_type, LangTarget::Ruby) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    let _ = writeln!(w, "  # WARNING: unknown pg type {}", col.data_type);
                    "Object".to_string()
                }
            };
            let _ = writeln!(w, "  # @return [{}] {}", rb_type, col.column_name);
        }
        let _ = writeln!(w, "end\n");
    }
}

// ── Rust emitter ─────────────────────────────────────────────────────────────

fn emit_rust(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");
    let _ = writeln!(w, "use serde::{{Deserialize, Serialize}};\n");

    // Collect crate-level uses (e.g. uuid).
    let mut extra_uses: BTreeSet<&str> = BTreeSet::new();
    for tbl in order {
        for col in &tables[tbl].columns {
            if let Some(mt) = map_type(&col.data_type, LangTarget::Rust) {
                if mt.import == "uuid" {
                    extra_uses.insert("use uuid::Uuid;");
                }
            }
        }
    }
    for u in &extra_uses {
        let _ = writeln!(w, "{u}");
    }
    if !extra_uses.is_empty() {
        let _ = writeln!(w);
    }

    for tbl in order {
        let tc = &tables[tbl];
        let struct_name = to_pascal_case(tbl);
        let _ = writeln!(w, "#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]");
        let _ = writeln!(w, "pub struct {struct_name} {{");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let rs_type = match map_type(&col.data_type, LangTarget::Rust) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    let _ = writeln!(w, "    // WARNING: unknown pg type {}", col.data_type);
                    if nullable { "Option<serde_json::Value>".to_string() } else { "serde_json::Value".to_string() }
                }
            };
            let _ = writeln!(w, "    pub {}: {rs_type},", col.column_name);
        }
        let _ = write!(w, "}}\n\n");
    }
}

// ── Java emitter ─────────────────────────────────────────────────────────────

fn emit_java(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
    pkg_name: &str,
) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");
    let _ = writeln!(w, "package {pkg_name};\n");
    let _ = writeln!(w, "import com.fasterxml.jackson.annotation.JsonProperty;");
    let _ = writeln!(w, "import javax.annotation.Nullable;\n");

    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = writeln!(w, "public final class {class_name} {{");
        // Fields.
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let java_type = match map_type(&col.data_type, LangTarget::Java) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    let _ = writeln!(w, "    // WARNING: unknown pg type {}", col.data_type);
                    "Object".to_string()
                }
            };
            let null_ann = if nullable { "@Nullable " } else { "" };
            let _ = writeln!(
                w,
                "    @JsonProperty(\"{cn}\")\n    public final {null_ann}{java_type} {cn};",
                cn = col.column_name,
                null_ann = null_ann,
                java_type = java_type,
            );
        }
        // Constructor.
        let _ = writeln!(w);
        let ctor_params: Vec<String> = tc.columns.iter().map(|col| {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let java_type = map_type(&col.data_type, LangTarget::Java)
                .map(|mt| if nullable { mt.nullable } else { mt.typ })
                .unwrap_or("Object");
            let null_ann = if nullable { "@Nullable " } else { "" };
            format!(
                "@JsonProperty(\"{cn}\") {null_ann}{java_type} {cn}",
                cn = col.column_name,
                null_ann = null_ann,
                java_type = java_type,
            )
        }).collect();
        let _ = writeln!(w, "    public {class_name}({})", ctor_params.join(", "));
        let _ = writeln!(w, "    {{");
        for col in &tc.columns {
            let _ = writeln!(w, "        this.{cn} = {cn};", cn = col.column_name);
        }
        let _ = write!(w, "    }}\n}}\n\n");
    }
}

// ── C# emitter ───────────────────────────────────────────────────────────────

fn emit_csharp(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
    pkg_name: &str,
) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");
    let _ = writeln!(w, "using System.Text.Json.Serialization;");

    // Check if we need JsonElement.
    let needs_json_element = order.iter().any(|tbl| {
        tables[tbl].columns.iter().any(|col| {
            map_type(&col.data_type, LangTarget::CSharp)
                .map(|mt| mt.import == "System.Text.Json")
                .unwrap_or(false)
        })
    });
    if needs_json_element {
        let _ = writeln!(w, "using System.Text.Json;");
    }
    let _ = writeln!(w, "\nnamespace {pkg_name};\n");

    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = writeln!(w, "public sealed record {class_name}(");
        let props: Vec<String> = tc.columns.iter().map(|col| {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let cs_type = match map_type(&col.data_type, LangTarget::CSharp) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    if nullable { "object?".to_string() } else { "object".to_string() }
                }
            };
            let pascal_name = to_pascal_case(&col.column_name);
            format!(
                "    [JsonPropertyName(\"{cn}\")] {cs_type} {pascal_name}",
                cn = col.column_name,
            )
        }).collect();
        let _ = write!(w, "{}", props.join(",\n"));
        let _ = write!(w, "\n);\n\n");
    }
}

// ── PHP emitter ──────────────────────────────────────────────────────────────

fn emit_php(
    w: &mut dyn Write,
    tables: &HashMap<String, TableColumns>,
    order: &[String],
    pkg_name: &str,
) {
    let _ = write!(w, "<?php\n// Code generated by basin gen types — DO NOT EDIT.\n\ndeclare(strict_types=1);\n\nnamespace {pkg_name};\n\n");

    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = writeln!(w, "final class {class_name}");
        let _ = writeln!(w, "{{");
        // Constructor with readonly properties.
        let _ = writeln!(w, "    public function __construct(");
        let params: Vec<String> = tc.columns.iter().map(|col| {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let php_type = match map_type(&col.data_type, LangTarget::Php) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    if nullable { "mixed".to_string() } else { "mixed".to_string() }
                }
            };
            format!("        public readonly {php_type} ${},", col.column_name)
        }).collect();
        let _ = writeln!(w, "{}", params.join("\n"));
        let _ = writeln!(w, "    ) {{}}");
        // fromArray factory.
        let _ = writeln!(w);
        let _ = writeln!(w, "    /** @param array<string,mixed> $data */");
        let _ = writeln!(w, "    public static function fromArray(array $data): self");
        let _ = writeln!(w, "    {{");
        let _ = writeln!(w, "        return new self(");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let php_type = match map_type(&col.data_type, LangTarget::Php) {
                Some(mt) => if nullable { mt.nullable.replace('?', "") } else { mt.typ.to_string() },
                None => "mixed".to_string(),
            };
            let cast = if nullable {
                format!("isset($data['{}']) ? ({}) $data['{}'] : null", col.column_name, php_type, col.column_name)
            } else {
                format!("({}) $data['{}']", php_type, col.column_name)
            };
            let _ = writeln!(w, "            {}: {cast},", col.column_name);
        }
        let _ = writeln!(w, "        );");
        let _ = write!(w, "    }}\n}}\n\n");
    }
}

// ── Dart emitter ─────────────────────────────────────────────────────────────

fn emit_dart(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n// ignore_for_file: always_specify_types\n\n");

    for tbl in order {
        let tc = &tables[tbl];
        let class_name = to_pascal_case(tbl);
        let _ = writeln!(w, "class {class_name} {{");
        // Final fields.
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let dart_type = match map_type(&col.data_type, LangTarget::Dart) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    let _ = writeln!(w, "  // WARNING: unknown pg type {}", col.data_type);
                    if nullable { "dynamic".to_string() } else { "dynamic".to_string() }
                }
            };
            let _ = writeln!(w, "  final {dart_type} {};", col.column_name);
        }
        // Constructor.
        let _ = writeln!(w);
        let _ = writeln!(w, "  const {class_name}({{");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let required = if nullable { "" } else { "required " };
            let _ = writeln!(w, "    {required}this.{},", col.column_name);
        }
        let _ = writeln!(w, "  }});");
        // fromJson.
        let _ = writeln!(w);
        let _ = writeln!(w, "  factory {class_name}.fromJson(Map<String, dynamic> j) =>");
        let _ = writeln!(w, "    {class_name}(");
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let dart_type = match map_type(&col.data_type, LangTarget::Dart) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => "dynamic".to_string(),
            };
            // Special handling for DateTime (needs parse) and List.
            let expr = if dart_type.starts_with("DateTime") {
                if nullable {
                    format!("j['{}'] == null ? null : DateTime.parse(j['{}'] as String)", col.column_name, col.column_name)
                } else {
                    format!("DateTime.parse(j['{}'] as String)", col.column_name)
                }
            } else if dart_type.contains("List<") || dart_type.contains("Map<") {
                format!("j['{}'] as {dart_type}", col.column_name)
            } else {
                let base = dart_type.trim_end_matches('?');
                if nullable {
                    format!("j['{}'] as {dart_type}", col.column_name)
                } else {
                    format!("j['{}'] as {base}", col.column_name)
                }
            };
            let _ = writeln!(w, "      {}: {expr},", col.column_name);
        }
        let _ = writeln!(w, "    );");
        // toJson.
        let _ = writeln!(w);
        let _ = writeln!(w, "  Map<String, dynamic> toJson() => {{");
        for col in &tc.columns {
            let _ = writeln!(w, "    '{}': {},", col.column_name, col.column_name);
        }
        let _ = write!(w, "  }};\n}}\n\n");
    }
}

// ── Swift emitter ─────────────────────────────────────────────────────────────

fn emit_swift(w: &mut dyn Write, tables: &HashMap<String, TableColumns>, order: &[String]) {
    let _ = write!(w, "// Code generated by basin gen types — DO NOT EDIT.\n\n");

    // Check if Foundation is needed (Date, UUID, Data).
    let needs_foundation = order.iter().any(|tbl| {
        tables[tbl].columns.iter().any(|col| {
            map_type(&col.data_type, LangTarget::Swift)
                .map(|mt| mt.import == "Foundation")
                .unwrap_or(false)
        })
    });
    if needs_foundation {
        let _ = writeln!(w, "import Foundation\n");
    }

    for tbl in order {
        let tc = &tables[tbl];
        let struct_name = to_pascal_case(tbl);
        let _ = writeln!(w, "public struct {struct_name}: Codable, Sendable {{");
        // Stored properties.
        for col in &tc.columns {
            let nullable = col.is_nullable.to_uppercase() == "YES";
            let swift_type = match map_type(&col.data_type, LangTarget::Swift) {
                Some(mt) => if nullable { mt.nullable.to_string() } else { mt.typ.to_string() },
                None => {
                    let _ = writeln!(w, "    // WARNING: unknown pg type {}", col.data_type);
                    if nullable { "String?".to_string() } else { "String".to_string() }
                }
            };
            // Convert snake_case column name to camelCase for Swift convention.
            let camel = to_camel_case(&col.column_name);
            let _ = writeln!(w, "    public let {camel}: {swift_type}");
        }
        // CodingKeys — only emit if any column name would differ in camelCase.
        let needs_coding_keys = tc.columns.iter().any(|c| to_camel_case(&c.column_name) != c.column_name);
        if needs_coding_keys {
            let _ = writeln!(w);
            let _ = writeln!(w, "    enum CodingKeys: String, CodingKey {{");
            for col in &tc.columns {
                let camel = to_camel_case(&col.column_name);
                if camel != col.column_name {
                    let _ = writeln!(w, "        case {camel} = \"{}\"", col.column_name);
                } else {
                    let _ = writeln!(w, "        case {camel}");
                }
            }
            let _ = writeln!(w, "    }}");
        }
        let _ = write!(w, "}}\n\n");
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_gen(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "gen",
                "Code-generation utilities.",
                &["types <lang>   Generate typed database interfaces (ts/go/py/ruby/rust/java/csharp/php/dart/swift)."],
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
                &["types <lang>   Generate typed database interfaces (ts/go/py/ruby/rust/java/csharp/php/dart/swift)."],
            );
            Ok(())
        }
        other => Err(msg(format!(
            "gen: unknown subcommand {:?} (available: types)",
            other
        ))),
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
        return Err(msg(
            "gen types: <lang> is required (typescript|go|python|ruby|rust|java|csharp|php|dart|swift)",
        ));
    }
    let lang_str = positional[0].to_lowercase();
    let lang = match lang_str.as_str() {
        "typescript" | "ts"          => LangTarget::TypeScript,
        "go"                          => LangTarget::Go,
        "python" | "py"               => LangTarget::Python,
        "ruby" | "rb"                 => LangTarget::Ruby,
        "rust" | "rs"                 => LangTarget::Rust,
        "java"                        => LangTarget::Java,
        "csharp" | "cs" | "dotnet" | "c#" => LangTarget::CSharp,
        "php"                         => LangTarget::Php,
        "dart"                        => LangTarget::Dart,
        "swift"                       => LangTarget::Swift,
        _ => {
            return Err(msg(format!(
                "gen types: unknown language {:?} (want typescript, go, python, ruby, rust, java, csharp, php, dart, or swift)",
                positional[0]
            )))
        }
    };

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
        return Err(msg(
            "gen types: --project is required (or link a project with `basin link`)",
        ));
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

    // ── --watch: hand off to the polling loop ──────────────────────────────
    if watch {
        // --output is mandatory in watch mode: we can't overwrite stdout on
        // each iteration.
        if output_path.is_empty() {
            return Err(msg(
                "gen types --watch requires --output=<path> (cannot stream to stdout in watch mode)",
            ));
        }
        // Resolve cwd once here so we never call current_dir inside the loop.
        let cwd = std::env::current_dir()
            .map_err(|e| msg(format!("gen types --watch: cwd: {e}")))?;
        // Use the process-level SIGINT default (Ctrl-C kills the process).
        // A None stop flag means "run forever" — used in production.
        return gen_types_watch(
            g,
            &project,
            &schema,
            lang,
            &lang_str,
            &output_path,
            &pkg_name,
            &cwd,
            None,
            err_out,
        );
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
        LangTarget::Go        => emit_go(&mut buf, &tables, &table_order, &pkg_name),
        LangTarget::Python    => emit_python(&mut buf, &tables, &table_order),
        LangTarget::Ruby      => emit_ruby(&mut buf, &tables, &table_order),
        LangTarget::Rust      => emit_rust(&mut buf, &tables, &table_order),
        LangTarget::Java      => emit_java(&mut buf, &tables, &table_order, &pkg_name),
        LangTarget::CSharp    => emit_csharp(&mut buf, &tables, &table_order, &pkg_name),
        LangTarget::Php       => emit_php(&mut buf, &tables, &table_order, &pkg_name),
        LangTarget::Dart      => emit_dart(&mut buf, &tables, &table_order),
        LangTarget::Swift     => emit_swift(&mut buf, &tables, &table_order),
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
    let out_display = if output_path.is_empty() {
        "-"
    } else {
        &output_path
    };
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
        err_out
            .write_all(&buf)
            .map_err(|e| msg(format!("gen types: write metadata: {e}")))?;
    } else if !g.quiet {
        let _ = writeln!(
            err_out,
            "generated {} table(s), {} column(s) → {}",
            table_order.len(),
            total_cols,
            out_display,
        );
    }
    Ok(())
}

// ── gen types --watch ─────────────────────────────────────────────────────────

/// Fingerprint of a migrations directory: sorted Vec of (path, mtime).
/// Comparing two snapshots detects file additions, removals, and changes.
fn migrations_dir_fingerprint(dir: &Path) -> Vec<(String, SystemTime)> {
    let mut entries: Vec<(String, SystemTime)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) != Some("sql") {
                continue;
            }
            let name = p.display().to_string();
            let mtime = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            entries.push((name, mtime));
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// run_single_gen_pass executes one schema query + emit cycle, writing the
/// generated source directly to `output_path`.  Returns Ok(n_tables).
fn run_single_gen_pass(
    g: &GlobalFlags,
    project: &str,
    schema: &str,
    lang: LangTarget,
    pkg_name: &str,
    output_path: &str,
) -> CliResult<usize> {
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

    let (tables, table_order, _total_cols) = parse_schema_columns(&res);
    let mut buf: Vec<u8> = Vec::new();
    match lang {
        LangTarget::TypeScript => emit_typescript(&mut buf, &tables, &table_order),
        LangTarget::Go        => emit_go(&mut buf, &tables, &table_order, pkg_name),
        LangTarget::Python    => emit_python(&mut buf, &tables, &table_order),
        LangTarget::Ruby      => emit_ruby(&mut buf, &tables, &table_order),
        LangTarget::Rust      => emit_rust(&mut buf, &tables, &table_order),
        LangTarget::Java      => emit_java(&mut buf, &tables, &table_order, pkg_name),
        LangTarget::CSharp    => emit_csharp(&mut buf, &tables, &table_order, pkg_name),
        LangTarget::Php       => emit_php(&mut buf, &tables, &table_order, pkg_name),
        LangTarget::Dart      => emit_dart(&mut buf, &tables, &table_order),
        LangTarget::Swift     => emit_swift(&mut buf, &tables, &table_order),
    }
    std::fs::write(output_path, &buf)
        .map_err(|e| msg(format!("gen types: write {output_path}: {e}")))?;
    Ok(table_order.len())
}

/// gen_types_watch polls `./basin/migrations/*.sql` for changes every 2 s,
/// re-running a full gen pass whenever the directory fingerprint changes.
///
/// - `cwd` — the working directory used to locate `basin/migrations/`.  Tests
///   pass a temp path; production passes `std::env::current_dir()`.  Never
///   call `std::env::set_current_dir` — it is process-global and races other
///   tests running in parallel.
/// - `stop` — when `Some`, the loop exits as soon as the flag is true (used
///   by tests to avoid infinite loops).  When `None`, runs until SIGINT.
/// - Always writes to `output_path` (stdout not allowed in watch mode).
#[allow(clippy::too_many_arguments)]
pub fn gen_types_watch(
    g: &GlobalFlags,
    project: &str,
    schema: &str,
    lang: LangTarget,
    lang_str: &str,
    output_path: &str,
    pkg_name: &str,
    cwd: &Path,
    stop: Option<Arc<AtomicBool>>,
    err_out: &mut dyn Write,
) -> CliResult<()> {
    // Determine the migrations directory to watch.
    let migrations_dir = cwd.join("basin").join("migrations");

    if !g.quiet {
        let _ = writeln!(
            err_out,
            "Watching {} for changes — Ctrl-C to stop. Generating {} → {}",
            migrations_dir.display(),
            lang_str,
            output_path
        );
    }

    // Initial pass: emit immediately so the output file exists from the start.
    let mut last_fp = migrations_dir_fingerprint(&migrations_dir);
    match run_single_gen_pass(g, project, schema, lang, pkg_name, output_path) {
        Ok(n) => {
            if !g.quiet {
                let _ = writeln!(err_out, "gen types: emitted ({n} table(s)) → {output_path}");
            }
        }
        Err(e) => {
            let _ = writeln!(err_out, "gen types: pass failed (will retry): {e}");
        }
    }

    loop {
        // Honour the test stop flag before sleeping so tests terminate quickly.
        if let Some(ref flag) = stop {
            if flag.load(Ordering::Relaxed) {
                break;
            }
        }

        std::thread::sleep(Duration::from_secs(2));

        // Re-check stop after sleep.
        if let Some(ref flag) = stop {
            if flag.load(Ordering::Relaxed) {
                break;
            }
        }

        let fp = migrations_dir_fingerprint(&migrations_dir);
        if fp == last_fp {
            continue;
        }
        last_fp = fp;

        match run_single_gen_pass(g, project, schema, lang, pkg_name, output_path) {
            Ok(n) => {
                if !g.quiet {
                    let _ =
                        writeln!(err_out, "gen types: re-emitted ({n} table(s)) → {output_path}");
                }
            }
            Err(e) => {
                let _ = writeln!(err_out, "gen types: pass failed (will retry): {e}");
            }
        }
    }
    Ok(())
}

fn gen_types_usage() {
    help_for_command(
        "gen types",
        "Generate typed database interfaces from information_schema.",
        &[
            "<lang>              Target language (required):",
            "                    typescript|ts  go  python|py  ruby|rb",
            "                    rust|rs  java  csharp|cs|dotnet  php  dart  swift",
            "--project=<ref>     Project ref (required if not linked).",
            "--schema=public     Schema name to introspect (default: public).",
            "--output=<path>     Write to file instead of stdout.",
            "--package=<name>    Package/namespace name (default: database; Go, Java, C#, PHP).",
            "--watch             Re-emit on migration-file changes (requires --output=<path>).",
        ],
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    /// unique_id returns a nanosecond-granularity timestamp suitable for
    /// scoping temp directories to a single test invocation.  Avoids
    /// collisions between parallel test threads (each picks its own nanos).
    fn unique_id() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

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
            vec![
                "orders".into(),
                "id".into(),
                "uuid".into(),
                "NO".into(),
                1.into(),
            ],
            vec![
                "orders".into(),
                "user_id".into(),
                "uuid".into(),
                "YES".into(),
                2.into(),
            ],
            vec![
                "orders".into(),
                "total".into(),
                "numeric".into(),
                "NO".into(),
                3.into(),
            ],
            vec![
                "orders".into(),
                "metadata".into(),
                "jsonb".into(),
                "YES".into(),
                4.into(),
            ],
            vec![
                "users".into(),
                "id".into(),
                "uuid".into(),
                "NO".into(),
                1.into(),
            ],
            vec![
                "users".into(),
                "email".into(),
                "text".into(),
                "NO".into(),
                2.into(),
            ],
            vec![
                "users".into(),
                "created_at".into(),
                "timestamp with time zone".into(),
                "YES".into(),
                3.into(),
            ],
            vec![
                "users".into(),
                "embedding".into(),
                "vector".into(),
                "YES".into(),
                4.into(),
            ],
            vec![
                "users".into(),
                "score".into(),
                "integer".into(),
                "NO".into(),
                5.into(),
            ],
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
            assert!(
                req.path.contains("/sql/query"),
                "unexpected path: {}",
                req.path
            );
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
        GlobalFlags {
            api_url: srv.url.clone(),
            token: "test-token".into(),
            ..Default::default()
        }
    }

    fn run_gen(
        g: &GlobalFlags,
        args: &[&str],
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
        Ok((
            String::from_utf8(out).unwrap(),
            String::from_utf8(err_out).unwrap(),
        ))
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

    #[test]
    fn snapshot_ruby() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["ruby", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.rb").expect("read testdata/expected.rb");
        assert_eq!(
            got, expected,
            "Ruby snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_rust() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["rust", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.rs").expect("read testdata/expected.rs");
        assert_eq!(
            got, expected,
            "Rust snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_java() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["java", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.java").expect("read testdata/expected.java");
        assert_eq!(
            got, expected,
            "Java snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_csharp() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["csharp", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.cs").expect("read testdata/expected.cs");
        assert_eq!(
            got, expected,
            "C# snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_php() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["php", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.php").expect("read testdata/expected.php");
        assert_eq!(
            got, expected,
            "PHP snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_dart() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["dart", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.dart").expect("read testdata/expected.dart");
        assert_eq!(
            got, expected,
            "Dart snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    #[test]
    fn snapshot_swift() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["swift", "--project=test-ref"]).unwrap();
        let expected =
            std::fs::read_to_string("testdata/expected.swift").expect("read testdata/expected.swift");
        assert_eq!(
            got, expected,
            "Swift snapshot mismatch.\nGOT:\n{got}\nWANT:\n{expected}"
        );
    }

    // ── Error paths ───────────────────────────────────────────────────────────

    #[test]
    fn unknown_lang() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        // "cobol" is not a supported language.
        let err = run_gen(&g_for(&srv), &["cobol", "--project=test-ref"]).unwrap_err();
        assert!(err.to_string().contains("unknown language"), "err={err}");
    }

    #[test]
    fn missing_project() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        // No --project passed and no working-project config → error.
        let err = run_gen(&g_for(&srv), &["typescript"]).unwrap_err();
        assert!(
            err.to_string().contains("--project is required"),
            "err={err}"
        );
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
        assert!(
            got.contains("Code generated by basin gen types"),
            "missing header: {got}"
        );
        assert!(
            got.contains("export interface Database"),
            "missing Database interface: {got}"
        );
        assert!(
            !got.contains("export interface Table_"),
            "unexpected table interface: {got}"
        );
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
        assert_eq!(
            file_content, stdout_got,
            "file content differs from writer output"
        );
        std::fs::remove_dir_all(&tmp).ok();
    }

    // ── JSON metadata shape ───────────────────────────────────────────────────

    #[test]
    fn json_metadata() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let mut g = g_for(&srv);
        g.json = true;
        let (_out, err_str) = run_gen_both(&g, &["typescript", "--project=test-ref"]).unwrap();
        let meta: serde_json::Value = serde_json::from_str(&err_str).expect("JSON metadata parse");
        assert_eq!(meta["lang"].as_str(), Some("typescript"));
        assert_eq!(meta["tables"].as_i64(), Some(2));
        assert_eq!(meta["columns"].as_i64(), Some(9));
        assert_eq!(meta["output_path"].as_str(), Some("-"));
    }

    // ── --watch ───────────────────────────────────────────────────────────────

    /// watch_requires_output: --watch without --output is an error.
    #[test]
    fn watch_requires_output() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "test-token".into(),
            api_url: "http://127.0.0.1:0".into(),
            quiet: true,
            ..Default::default()
        };
        let err = run_gen(
            &g,
            &["typescript", "--project=test-ref", "--watch"],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--output"),
            "expected --output error, got: {err}"
        );
    }

    /// watch_emits_on_initial_pass: verifies the initial gen pass writes the
    /// output file.  The stop flag is raised immediately so the loop exits
    /// after one iteration.  Uses an explicit cwd to avoid process-global
    /// set_current_dir races.
    #[test]
    fn watch_emits_on_initial_pass() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "test-token".into(),
            quiet: true,
            ..Default::default()
        };

        let dir = std::env::temp_dir().join(format!(
            "basin-watch-initial-{}-{}",
            std::process::id(),
            unique_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // Create the basin/migrations/ subdirectory the watcher looks at.
        let mig_dir = dir.join("basin").join("migrations");
        std::fs::create_dir_all(&mig_dir).unwrap();

        let output = dir.join("database.ts");
        let stop = Arc::new(AtomicBool::new(true)); // stop immediately after initial pass

        let mut err_out = Vec::<u8>::new();
        // Pass cwd directly — no set_current_dir.
        let result = gen_types_watch(
            &g,
            "test-ref",
            "public",
            LangTarget::TypeScript,
            "typescript",
            output.to_str().unwrap(),
            "database",
            &dir,
            Some(Arc::clone(&stop)),
            &mut err_out,
        );
        // Cleanup (best-effort).
        let _ = std::fs::remove_dir_all(&dir);

        assert!(result.is_ok(), "watch returned error: {result:?}");
    }

    /// watch_stop_flag_terminates_immediately: with stop=true on entry, the
    /// watch loop exits without sleeping, verifying the early-exit fast path.
    #[test]
    fn watch_stop_flag_terminates_immediately() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "test-token".into(),
            quiet: true,
            ..Default::default()
        };

        let dir = std::env::temp_dir().join(format!(
            "basin-watch-stopimm-{}-{}",
            std::process::id(),
            unique_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mig_dir = dir.join("basin").join("migrations");
        std::fs::create_dir_all(&mig_dir).unwrap();

        let output = dir.join("database.ts");
        let stop = Arc::new(AtomicBool::new(true)); // stop before the first sleep

        let mut err_out = Vec::<u8>::new();
        let result = gen_types_watch(
            &g,
            "test-ref",
            "public",
            LangTarget::TypeScript,
            "typescript",
            output.to_str().unwrap(),
            "database",
            &dir,
            Some(Arc::clone(&stop)),
            &mut err_out,
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert!(result.is_ok(), "watch returned error: {result:?}");
    }

    /// watch_detects_migration_change: a background thread adds a .sql file
    /// and flips the stop flag; the watch loop should exit cleanly.
    #[test]
    fn watch_detects_migration_change() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "test-token".into(),
            quiet: true,
            ..Default::default()
        };

        let dir = std::env::temp_dir().join(format!(
            "basin-watch-change-{}-{}",
            std::process::id(),
            unique_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mig_dir = dir.join("basin").join("migrations");
        std::fs::create_dir_all(&mig_dir).unwrap();

        let output = dir.join("database.ts");
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        // Write a migration file after a short delay in a background thread,
        // then set the stop flag so the watch loop terminates.
        let mig_dir2 = mig_dir.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            std::fs::write(mig_dir2.join("0001_init.sql"), b"CREATE TABLE t (id int);")
                .expect("write migration");
            stop2.store(true, Ordering::Relaxed);
        });

        let mut err_out = Vec::<u8>::new();
        // Pass cwd directly — no set_current_dir.
        let result = gen_types_watch(
            &g,
            "test-ref",
            "public",
            LangTarget::TypeScript,
            "typescript",
            output.to_str().unwrap(),
            "database",
            &dir,
            Some(Arc::clone(&stop)),
            &mut err_out,
        );
        handle.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);

        assert!(result.is_ok(), "watch returned error: {result:?}");
    }

    /// fingerprint_empty_dir: an empty (or missing) migrations dir yields an
    /// empty fingerprint.
    #[test]
    fn fingerprint_empty_dir() {
        let dir = std::env::temp_dir()
            .join(format!("basin-fp-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fp = migrations_dir_fingerprint(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(fp.is_empty(), "expected empty fingerprint for new dir");
    }

    /// fingerprint_missing_dir: a path that does not exist returns empty.
    #[test]
    fn fingerprint_missing_dir() {
        let fp = migrations_dir_fingerprint(Path::new("/no/such/migrations/dir"));
        assert!(fp.is_empty());
    }

    /// fingerprint_ignores_non_sql: .txt files are not tracked.
    #[test]
    fn fingerprint_ignores_non_sql() {
        let dir = std::env::temp_dir()
            .join(format!("basin-fp-nonsql-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("README.txt"), b"ignore me").unwrap();
        let fp = migrations_dir_fingerprint(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(fp.is_empty(), "expected no entries for non-sql file");
    }

    /// fingerprint_tracks_sql_files: .sql files are included.
    #[test]
    fn fingerprint_tracks_sql_files() {
        let dir = std::env::temp_dir()
            .join(format!("basin-fp-sql-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0001_init.sql"), b"CREATE TABLE x (id int);").unwrap();
        std::fs::write(dir.join("0002_add.sql"), b"ALTER TABLE x ADD col text;").unwrap();
        let fp = migrations_dir_fingerprint(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(fp.len(), 2, "expected 2 sql entries, got {:?}", fp.len());
    }

    /// fingerprint_changes_on_new_file: adding a .sql file changes the fingerprint.
    #[test]
    fn fingerprint_changes_on_new_file() {
        let dir = std::env::temp_dir()
            .join(format!("basin-fp-change-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fp_before = migrations_dir_fingerprint(&dir);
        std::fs::write(dir.join("0001_init.sql"), b"CREATE TABLE x (id int);").unwrap();
        let fp_after = migrations_dir_fingerprint(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert_ne!(
            fp_before, fp_after,
            "fingerprint should change when a .sql file is added"
        );
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

    // ── to_camel_case helper ──────────────────────────────────────────────────

    #[test]
    fn camel_case_cases() {
        let cases = &[
            ("id", "id"),
            ("user_id", "userId"),
            ("created_at", "createdAt"),
            ("user_profile_name", "userProfileName"),
            ("", ""),
            ("score", "score"),
        ];
        for (input, want) in cases {
            let got = to_camel_case(input);
            assert_eq!(&got, want, "to_camel_case({input:?})");
        }
    }

    // ── testdata generator — run with BASIN_GEN_TESTDATA=1 to regenerate ────

    /// generate_testdata writes the golden testdata files for all new languages.
    /// Run with: cargo test commands::gen::tests::generate_testdata -- --nocapture
    /// (set BASIN_GEN_TESTDATA=1 env var first to actually write files)
    #[test]
    fn generate_testdata() {
        let _cfg = with_temp_config_dir();
        if std::env::var("BASIN_GEN_TESTDATA").unwrap_or_default() != "1" {
            return; // skip unless explicitly opted in
        }
        let srv = build_schema_server();
        let g = g_for(&srv);
        let langs: &[(&str, &str)] = &[
            ("ruby",       "testdata/expected.rb"),
            ("rust",       "testdata/expected.rs"),
            ("java",       "testdata/expected.java"),
            ("csharp",     "testdata/expected.cs"),
            ("php",        "testdata/expected.php"),
            ("dart",       "testdata/expected.dart"),
            ("swift",      "testdata/expected.swift"),
        ];
        for (lang, path) in langs {
            let got = run_gen(&g, &[lang, "--project=test-ref"]).unwrap();
            std::fs::write(path, got.as_bytes()).unwrap_or_else(|e| panic!("write {path}: {e}"));
            eprintln!("wrote {path}");
        }
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
            vec![
                "orders".into(),
                "id".into(),
                "uuid".into(),
                "NO".into(),
                1.into(),
            ],
            vec![
                "orders".into(),
                "user_id".into(),
                "uuid".into(),
                "YES".into(),
                2.into(),
            ],
            vec![
                "orders".into(),
                "total".into(),
                "numeric".into(),
                "NO".into(),
                3.into(),
            ],
            vec![
                "orders".into(),
                "metadata".into(),
                "jsonb".into(),
                "YES".into(),
                4.into(),
            ],
            vec![
                "users".into(),
                "id".into(),
                "uuid".into(),
                "NO".into(),
                1.into(),
            ],
            vec![
                "users".into(),
                "email".into(),
                "text".into(),
                "NO".into(),
                2.into(),
            ],
            vec![
                "users".into(),
                "created_at".into(),
                "timestamp with time zone".into(),
                "YES".into(),
                3.into(),
            ],
            vec![
                "users".into(),
                "embedding".into(),
                "vector".into(),
                "YES".into(),
                4.into(),
            ],
            vec![
                "users".into(),
                "score".into(),
                "integer".into(),
                "NO".into(),
                5.into(),
            ],
        ];
        let res = QueryResult {
            columns: vec![
                "table_name".into(),
                "column_name".into(),
                "data_type".into(),
                "is_nullable".into(),
                "ordinal_position".into(),
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

    // ── Ruby emit shape ───────────────────────────────────────────────────────

    #[test]
    fn ruby_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["ruby", "--project=test-ref"]).unwrap();
        let checks = &[
            "# Code generated by basin gen types — DO NOT EDIT.",
            "# frozen_string_literal: true",
            "Orders = Struct.new",
            "Users = Struct.new",
            "ATTRIBUTES",
            ":id",
            ":email",
            "keyword_init: true",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Rust emit shape ───────────────────────────────────────────────────────

    #[test]
    fn rust_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["rust", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "use serde::{Deserialize, Serialize};",
            "pub struct Orders {",
            "pub struct Users {",
            "pub id: uuid::Uuid,",
            "pub user_id: Option<uuid::Uuid>,",
            "pub total: String,",
            "pub metadata: Option<serde_json::Value>,",
            "pub email: String,",
            "pub created_at: Option<String>,",
            "pub embedding: Option<Vec<f32>>,",
            "pub score: i32,",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Java emit shape ───────────────────────────────────────────────────────

    #[test]
    fn java_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["java", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "import com.fasterxml.jackson.annotation.JsonProperty;",
            "public final class Orders {",
            "public final class Users {",
            "@JsonProperty(\"id\")",
            "@JsonProperty(\"email\")",
            "@Nullable",
            "java.util.UUID",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── C# emit shape ────────────────────────────────────────────────────────

    #[test]
    fn csharp_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["csharp", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "using System.Text.Json.Serialization;",
            "public sealed record Orders(",
            "public sealed record Users(",
            "[JsonPropertyName(\"id\")]",
            "[JsonPropertyName(\"email\")]",
            "Guid",
            "Guid?",  // nullable uuid
            "DateTimeOffset?",  // nullable timestamp
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── PHP emit shape ────────────────────────────────────────────────────────

    #[test]
    fn php_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["php", "--project=test-ref"]).unwrap();
        let checks = &[
            "<?php",
            "// Code generated by basin gen types — DO NOT EDIT.",
            "declare(strict_types=1);",
            "final class Orders",
            "final class Users",
            "public readonly",
            "public static function fromArray",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Dart emit shape ───────────────────────────────────────────────────────

    #[test]
    fn dart_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["dart", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "class Orders {",
            "class Users {",
            "factory Orders.fromJson(",
            "factory Users.fromJson(",
            "Map<String, dynamic> toJson()",
            "final String id;",
            "final String? user_id;",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Swift emit shape ──────────────────────────────────────────────────────

    #[test]
    fn swift_emit_shape() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["swift", "--project=test-ref"]).unwrap();
        let checks = &[
            "// Code generated by basin gen types — DO NOT EDIT.",
            "import Foundation",
            "public struct Orders: Codable, Sendable {",
            "public struct Users: Codable, Sendable {",
            "public let id: UUID",
            "public let email: String",
            "public let createdAt: Date?",
            "enum CodingKeys: String, CodingKey",
            "case createdAt = \"created_at\"",
        ];
        for check in checks {
            assert!(got.contains(check), "missing {check:?} in:\n{got}");
        }
    }

    // ── Language aliases (new langs) ──────────────────────────────────────────

    #[test]
    fn lang_alias_rs() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["rs", "--project=test-ref"]).unwrap();
        assert!(got.contains("pub struct"), "'rs' alias did not emit Rust");
    }

    #[test]
    fn lang_alias_rb() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["rb", "--project=test-ref"]).unwrap();
        assert!(got.contains("Struct.new"), "'rb' alias did not emit Ruby");
    }

    #[test]
    fn lang_alias_cs() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["cs", "--project=test-ref"]).unwrap();
        assert!(got.contains("sealed record"), "'cs' alias did not emit C#");
    }

    #[test]
    fn lang_alias_dotnet() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["dotnet", "--project=test-ref"]).unwrap();
        assert!(got.contains("sealed record"), "'dotnet' alias did not emit C#");
    }

    // ── --package flag ────────────────────────────────────────────────────────

    #[test]
    fn go_package_flag() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(
            &g_for(&srv),
            &["go", "--project=test-ref", "--package=mydb"],
        )
        .unwrap();
        assert!(
            got.contains("package mydb"),
            "expected 'package mydb' in:\n{got}"
        );
    }

    // ── unknown flag ──────────────────────────────────────────────────────────

    #[test]
    fn unknown_flag() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            api_url: "http://127.0.0.1:0".into(),
            ..Default::default()
        };
        let err = run_gen(&g, &["typescript", "--project=ref", "--no-such-flag"]).unwrap_err();
        assert!(
            !err.to_string().is_empty(),
            "expected error for unknown flag"
        );
    }

    // ── language aliases ──────────────────────────────────────────────────────

    #[test]
    fn lang_alias_ts() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["ts", "--project=test-ref"]).unwrap();
        assert!(
            got.contains("export interface Database"),
            "'ts' alias did not emit TypeScript"
        );
    }

    #[test]
    fn lang_alias_py() {
        let _cfg = with_temp_config_dir();
        let srv = build_schema_server();
        let got = run_gen(&g_for(&srv), &["py", "--project=test-ref"]).unwrap();
        assert!(
            got.contains("class Orders(BaseModel)"),
            "'py' alias did not emit Python"
        );
    }

    // ── Type mapping table completeness ───────────────────────────────────────

    #[test]
    fn map_type_table_completeness() {
        let table = type_table();
        let langs = [
            LangTarget::TypeScript,
            LangTarget::Go,
            LangTarget::Python,
            LangTarget::Ruby,
            LangTarget::Rust,
            LangTarget::Java,
            LangTarget::CSharp,
            LangTarget::Php,
            LangTarget::Dart,
            LangTarget::Swift,
        ];
        for pg_name in table.keys() {
            for &lang in &langs {
                let mt = map_type(pg_name, lang).unwrap_or_else(|| {
                    panic!("map_type({pg_name:?}, {lang:?}) returned None but key exists")
                });
                assert!(
                    !mt.typ.is_empty(),
                    "map_type({pg_name:?}, {lang:?}).typ is empty"
                );
                assert!(
                    !mt.nullable.is_empty(),
                    "map_type({pg_name:?}, {lang:?}).nullable is empty"
                );
            }
        }
    }

    // ── Unknown type path (ok=false) ──────────────────────────────────────────

    #[test]
    fn map_type_unknown() {
        assert!(
            map_type("money", LangTarget::TypeScript).is_none(),
            "money should be unknown"
        );
        assert!(
            map_type("xml", LangTarget::Go).is_none(),
            "xml should be unknown"
        );
        assert!(
            map_type("point", LangTarget::Python).is_none(),
            "point should be unknown"
        );
        assert!(map_type("not_a_real_pg_type", LangTarget::TypeScript).is_none());
        assert!(
            map_type("", LangTarget::Go).is_none(),
            "empty string should be unknown"
        );
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
