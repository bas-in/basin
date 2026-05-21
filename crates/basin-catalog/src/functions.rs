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

/// Implementation language for a user-defined function. `Sql` is the v0.1
/// shipping variant; `Wasm` is the Phase 5.11.J addition — the escape hatch
/// for the ~5 % of cases `LANGUAGE sql` can't express. `Javascript`
/// (Phase 5.11.W6) is **sugar over `Wasm`**: the body is a compiled
/// WebAssembly **component** produced client-side by ComponentizeJS / Javy
/// (see `basin functions deploy`); the engine stores the compiled bytes
/// and the source side-by-side. New variants are additive; historic
/// serialised payloads that pre-date a field deserialise to whatever
/// default the enum / struct provides.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SqlFunctionLanguage {
    #[default]
    Sql,
    /// `LANGUAGE wasm` — body is base64-encoded WebAssembly bytes. The
    /// function name exported by the WASM module must match the SQL
    /// function name. Each invocation spins up a fresh `wasmtime::Store`
    /// with a CPU-epoch budget (default 100 ms) and a linear-memory cap
    /// (default 16 MiB). Sandboxed by construction via `wasmtime`.
    Wasm,
    /// `LANGUAGE javascript` (Phase 5.11.W6) — sugar over `Wasm`. Body is
    /// a base64-encoded Wasm **component** (component-model format),
    /// compiled client-side from JS by ComponentizeJS / Javy. The engine
    /// stores the compiled bytes verbatim; invocation flows through
    /// `basin-fn`'s [`ComponentHarness`] rather than the bare-module
    /// scalar-UDF path used by `Wasm`. JavaScript functions are
    /// **request-style** (not DataFusion scalar UDFs) — see
    /// `basin-fn::runtime::FunctionRuntime`.
    Javascript,
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

/// Catalog row for a user-defined function. Stored per-project; two
/// projects registering the same function name are independent rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlFunctionDef {
    pub project: ProjectId,
    /// Unqualified function name within the project. Validated to be a
    /// SQL identifier and to not collide with a built-in at registration.
    pub name: String,
    pub args: Vec<SqlFunctionArg>,
    pub return_type: SqlReturnType,
    /// Body text. The interpretation depends on `language`:
    /// * `Sql` — a single SQL `SELECT` statement (validated at registration).
    /// * `Wasm` — base64-encoded WebAssembly module bytes (core module).
    /// * `Javascript` — base64-encoded Wasm **component** bytes, compiled
    ///   client-side from JS by ComponentizeJS / Javy. The original source
    ///   text travels in [`Self::source`] for `EXPLAIN` / debugging.
    pub body: String,
    pub language: SqlFunctionLanguage,
    /// Monotonically-increasing revision counter. Bumped on every
    /// `CREATE OR REPLACE FUNCTION` so callers can detect a redeploy
    /// (e.g. CDN invalidation, stale-component eviction in the runtime
    /// registry). Starts at `1`; historic serialised payloads that
    /// pre-date this field deserialise to `1` via [`default_one`] (so a
    /// migrated row looks like a fresh first registration rather than a
    /// nonsensical version `0`).
    #[serde(default = "default_one")]
    pub version: i32,
    /// Original source text for languages that compile through an
    /// intermediate format (currently `Javascript` → Wasm component).
    /// `None` for `Sql` and `Wasm` (whose `body` is already the canonical
    /// representation). Stored for `EXPLAIN FUNCTION` and for the
    /// `basin functions deploy` redeploy diff. Historic serialised
    /// payloads default to `None`.
    #[serde(default)]
    pub source: Option<String>,
}

/// Serde default helper — version is `1` for newly-created functions and
/// for pre-existing serialised rows that lack the field (so a migrated
/// row reads as a fresh first registration). The next
/// `register_sql_function` REPLACE bumps it to `2`.
fn default_one() -> i32 {
    1
}
