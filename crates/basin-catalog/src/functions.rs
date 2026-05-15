//! Per-project SQL function catalog.
//!
//! Stores `LANGUAGE sql` scalar functions registered via the engine's
//! `register_sql_function` API. The shape is deliberately narrow for v0.1:
//!
//! * Only `LANGUAGE sql` (the body is a single SQL `SELECT` statement).
//! * Only scalar return types — `RETURNS TABLE` is a follow-up phase.
//! * Only scalar argument types — composite-row arguments
//!   (e.g. `display_name(u users)`) are rejected at registration. v0.2 work.
//!
//! The catalog is a single shared `HashMap<(ProjectId, String), SqlFunctionDef>`
//! gated by a tokio mutex so two projects — or two functions in the same
//! project — never block each other beyond the mutex hold (HashMap probe).
//! Per-project cost is `O(bytes-of-functions)`, no per-project heavy resources.

use serde::{Deserialize, Serialize};

use basin_common::ProjectId;

/// Implementation language for a user-defined function. v0.1 ships
/// `LANGUAGE sql` only; the variant is reserved for the future Wasm /
/// PL/pgSQL phases (see ADR 0012).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SqlFunctionLanguage {
    #[default]
    Sql,
}

/// Argument type. Mirrors the scalar types the engine already understands;
/// composite-row types are deliberately absent because they're a v0.2
/// extension. New variants are additive — old serialised payloads
/// deserialise to whatever variants existed when they were written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlArgType {
    Text,
    Int,
    BigInt,
    Double,
    Boolean,
    /// `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ` — microsecond-precision
    /// instant stamped to UTC.
    TimestampTz,
    /// `TIMESTAMP` (no time zone) — microsecond-precision wall clock,
    /// no zone string. Bridges to Arrow `Timestamp(Microsecond, None)`
    /// and PG OID 1114. Distinct from [`Self::TimestampTz`] (OID 1184)
    /// at the wire and OID level.
    Timestamp,
    Date,
    Bytea,
}

/// One argument of a [`SqlFunctionDef`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFunctionArg {
    pub name: String,
    pub data_type: SqlArgType,
}

/// Return shape. v0.1 supports only [`SqlReturnType::Scalar`]; the `Table`
/// variant is reserved for `RETURNS TABLE` (5.11.E follow-up).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SqlReturnType {
    Scalar(SqlArgType),
    /// Reserved for `RETURNS TABLE (col1 type1, col2 type2, ...)`. Not
    /// produced by the v0.1 registration API; the variant exists so the
    /// catalog payload is forward-compatible without a migration.
    Table(Vec<(String, SqlArgType)>),
}

/// Catalog row for a user-defined `LANGUAGE sql` function. Stored
/// per-project; two projects registering the same function name are
/// independent rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFunctionDef {
    pub project: ProjectId,
    /// Unqualified function name within the project. Validated to be a
    /// SQL identifier and to not collide with a built-in at registration.
    pub name: String,
    pub args: Vec<SqlFunctionArg>,
    pub return_type: SqlReturnType,
    /// Raw SQL of the function body. Validated at registration to parse
    /// as a single `SELECT` statement; stored verbatim so the engine
    /// inliner reparses on demand (no AST-level Serialize round-trip).
    pub body: String,
    pub language: SqlFunctionLanguage,
}
