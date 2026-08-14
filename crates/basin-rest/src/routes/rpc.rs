//! `POST /rest/v1/rpc/<fn>` — invoke a catalog UDF over HTTP (ADR 0019).
//!
//! ## Contract (from ADR 0019)
//!
//! - Request body: JSON object `{ "arg_name": value, … }`. Empty body or `{}`
//!   is valid for zero-argument functions.
//! - Response: the function's return value serialised as JSON.
//!   - Scalar return → the value itself (e.g. `42`, `"hello"`, `true`).
//!   - `RETURNS TABLE` or multi-row result → JSON array of row objects.
//! - Auth: identical to `/rest/v1/<table>` routes — JWT or API-key bearer,
//!   verified before touching the engine.
//! - Both `LANGUAGE sql` (5.11.D) and `LANGUAGE wasm` (5.11.J) dispatch
//!   transparently: the engine already routes by language; we just build
//!   `SELECT <fn>(args…)` and hand it to the session.
//!
//! ## Arrow IPC transport
//!
//! When the request carries `Accept: application/vnd.apache.arrow.stream`
//! and the result is a multi-row table, the response is an Arrow IPC stream
//! instead of JSON. Scalar results (`RETURNS` non-table) are always JSON — the
//! Arrow format requires a schema, which isn't meaningful for a bare scalar.
//! Pagination headers (`x-basin-next-cursor`, `x-basin-row-count`) apply here
//! the same as on the `/rest/v1/:table` GET path.
//!
//! ## SQL injection defence
//!
//! - The function name flows through [`crate::parser::validate_ident`] —
//!   enforces `[A-Za-z_][A-Za-z0-9_]*` and length ≤ 63.
//! - Each argument *value* is classified into [`crate::parser::Literal`] and
//!   rendered via [`crate::parser::render_literal`] / [`quote_sql_string`] —
//!   the same path used by every other REST handler.
//! - Argument *names* (the JSON object keys that become the positional order
//!   for the call) are not spliced into SQL at all — we use positional
//!   argument ordering based on the catalog's `SqlFunctionDef`.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_engine::ExecResult;
use serde_json::Value;

use basin_catalog::Catalog as _;

use crate::arrow_ipc::{batches_to_arrow_ipc, wants_arrow};
use crate::errors::ApiError;
use crate::json::batches_to_json;
use crate::parser::{json_to_literal, render_literal, validate_ident};
use crate::server::{authorize, Inner};

/// `POST /rest/v1/rpc/:fn_name`
///
/// Parses the JSON body as named arguments, builds `SELECT <fn>(args…)`,
/// executes it, and returns the result as JSON (or Arrow IPC when the caller
/// sends `Accept: application/vnd.apache.arrow.stream`).
#[axum::debug_handler]
pub(crate) async fn post_rpc(
    State(state): State<Arc<Inner>>,
    Path(fn_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let fn_ident = validate_ident(&fn_name)?;

    // Parse body — accept empty / missing body as an empty arg set.
    let args_obj: serde_json::Map<String, Value> = if body.is_empty() {
        serde_json::Map::new()
    } else {
        let v: Value = serde_json::from_slice(&body)
            .map_err(|e| ApiError::invalid(format!("body is not valid JSON: {e}")))?;
        match v {
            Value::Object(o) => o,
            Value::Null => serde_json::Map::new(),
            _ => {
                return Err(ApiError::invalid(
                    "RPC body must be a JSON object of named arguments or empty",
                ))
            }
        }
    };

    // Build `SELECT fn(arg1, arg2, …)`.
    //
    // We use name-based ordering: look up the function definition in the
    // catalog so we can emit positional args in declaration order. If the
    // function isn't in the catalog we still attempt the call — the engine
    // will return an appropriate error. This lets built-ins (or any future
    // function registered outside the catalog) still work.
    let sql = build_rpc_sql(&state, claims.project_id, &fn_ident, &args_obj).await?;

    let session = state
        .cfg
        .engine
        .open_session(claims.project_id)
        .await
        .map_err(ApiError::from)?;
    let res = session.execute(&sql).await.map_err(ApiError::from)?;

    let accept_arrow = wants_arrow(&headers);
    Ok(render_rpc_result(res, accept_arrow))
}

/// Build `SELECT fn(…)` from the JSON arg map.
///
/// If the catalog has a definition for `fn_name` we emit args in the
/// declaration order, mapping by name. If the catalog doesn't know the
/// function (e.g. a built-in) and args are present we fall back to
/// positional insertion order. Zero-arg calls always work.
async fn build_rpc_sql(
    state: &Arc<Inner>,
    project: basin_common::ProjectId,
    fn_ident: &str,
    args: &serde_json::Map<String, Value>,
) -> Result<String, ApiError> {
    // Try catalog lookup to get declared arg order.
    let maybe_def = state
        .cfg
        .engine
        .config()
        .catalog
        .lookup_sql_function(&project, fn_ident)
        .await;

    let arg_values: Vec<String> = if args.is_empty() {
        Vec::new()
    } else if let Some(def) = &maybe_def {
        // Emit in declaration order; missing args become NULL.
        def.args
            .iter()
            .map(|arg| {
                let lit = match args.get(&arg.name) {
                    Some(v) => json_to_literal(v),
                    None => Ok(crate::parser::Literal::Null),
                };
                lit.and_then(|l| render_literal(&l))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        // No catalog entry — fall back to insertion order.
        args.values()
            .map(|v| json_to_literal(v).and_then(|l| render_literal(&l)))
            .collect::<Result<Vec<_>, _>>()?
    };

    let mut sql = format!("SELECT {fn_ident}(");
    for (i, val) in arg_values.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(val);
    }
    sql.push(')');
    Ok(sql)
}

/// Render the engine result for an RPC call.
///
/// - Single-row / single-column result (scalar function): unwrap the inner
///   value directly so callers see `42` rather than `[{"fn(42)": 42}]`.
///   Scalar results are always JSON even when `accept_arrow` is set, because
///   a bare scalar has no meaningful schema for an IPC stream.
/// - Multi-column or multi-row result (`RETURNS TABLE`): return Arrow IPC when
///   `accept_arrow` is true, otherwise JSON array of row objects.
/// - Empty / tag-only result: return `{"ok": true, "tag": "…"}`.
fn render_rpc_result(res: ExecResult, accept_arrow: bool) -> Response {
    match res {
        ExecResult::Empty { tag } => {
            Json(serde_json::json!({ "ok": true, "tag": tag })).into_response()
        }
        ExecResult::Rows { schema, batches } => {
            // Scalar unwrap: single column, single row → bare JSON value.
            // Arrow IPC is not used for scalars (no meaningful schema shape).
            if schema.fields().len() == 1 {
                let rows = batches_to_json(&schema, &batches);
                if let Value::Array(ref arr) = rows {
                    if arr.len() == 1 {
                        let row = &arr[0];
                        if let Value::Object(map) = row {
                            if let Some((_k, v)) = map.iter().next() {
                                return Json(v.clone()).into_response();
                            }
                        }
                    }
                }
            }

            if accept_arrow {
                // Multi-row / multi-column → Arrow IPC stream.
                // No pagination for RPC results (RPC functions control their
                // own LIMIT/paging via SQL).
                batches_to_arrow_ipc(&schema, &batches, None)
            } else {
                let rows = batches_to_json(&schema, &batches);
                Json(rows).into_response()
            }
        }
    }
}
