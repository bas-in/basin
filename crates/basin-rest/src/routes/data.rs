//! `/rest/v1/<table>` handlers.
//!
//! Auth is enforced at the per-request level (not via middleware) so the
//! handler has direct access to the verified [`basin_auth::Claims`] and can
//! open a tenant-scoped engine session itself. The pattern matches what
//! basin-router does for pgwire connections.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query as AxumQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_engine::ExecResult;
use serde_json::Value;

use crate::errors::ApiError;
use crate::json::batches_to_json;
use crate::parser::{
    build_delete_sql, build_insert_sql, build_select_sql, build_update_sql, decode_cursor,
    encode_cursor, json_to_literal, parse_query, validate_ident, Literal,
};
use crate::server::{authorize, Inner};

type Pairs = Vec<(String, String)>;

/// Cap a streaming response at this many rows before opting into NDJSON; below
/// the threshold the buffered JSON path is fine.
const STREAM_ROW_THRESHOLD: usize = 10_000;
/// Same idea for serialised payload bytes.
const STREAM_BYTE_THRESHOLD: usize = 1 << 20; // 1 MiB

#[axum::debug_handler]
pub(crate) async fn get_table(
    State(state): State<Arc<Inner>>,
    Path(table): Path<String>,
    AxumQuery(raw): AxumQuery<Pairs>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let _ident = validate_ident(&table)?;

    // Peel off cursor / stream / limit before passing the rest to the
    // generic filter parser. `limit` flows through `parse_query` as before so
    // existing behaviour is preserved when no cursor is in play.
    let mut cursor_str: Option<String> = None;
    let mut force_stream = false;
    let mut limit_supplied = false;
    let mut filtered: Vec<(&str, &str)> = Vec::with_capacity(raw.len());
    for (k, v) in raw.iter() {
        match k.as_str() {
            "cursor" => cursor_str = Some(v.clone()),
            "stream" => {
                force_stream = matches!(v.as_str(), "1" | "true" | "yes");
            }
            "limit" => {
                limit_supplied = true;
                filtered.push((k.as_str(), v.as_str()));
            }
            _ => filtered.push((k.as_str(), v.as_str())),
        }
    }

    let mut q = parse_query(filtered.into_iter())?;
    let cursor_supplied = cursor_str.is_some();
    if let Some(c) = cursor_str.as_deref() {
        q.after_id = Some(decode_cursor(c)?);
    }

    let effective_limit = q
        .limit
        .unwrap_or(state.cfg.default_page_size as u64)
        .min(state.cfg.max_page_size as u64);
    let sql = build_select_sql(
        &table,
        &q,
        state.cfg.default_page_size as u64,
        state.cfg.max_page_size as u64,
    )?;

    let session = state
        .cfg
        .engine
        .open_session(claims.tenant_id)
        .await
        .map_err(ApiError::from)?;
    let res = session.execute(&sql).await.map_err(ApiError::from)?;

    let want_wrapped = cursor_supplied || limit_supplied;
    Ok(render_get_response(
        res,
        want_wrapped,
        effective_limit,
        force_stream,
    ))
}

/// Render the GET response, choosing between buffered JSON and NDJSON
/// streaming based on size + the explicit `?stream=true` override.
fn render_get_response(
    res: ExecResult,
    want_wrapped: bool,
    effective_limit: u64,
    force_stream: bool,
) -> Response {
    let (schema, batches) = match res {
        ExecResult::Empty { tag } => {
            return Json(serde_json::json!({ "ok": true, "tag": tag })).into_response();
        }
        ExecResult::Rows { schema, batches } => (schema, batches),
    };
    let rows_value = batches_to_json(&schema, &batches);
    let row_count = rows_value.as_array().map(|a| a.len()).unwrap_or(0);

    // Compute next_cursor (only meaningful when caller is paginating).
    let next_cursor = if want_wrapped && row_count as u64 >= effective_limit {
        last_id(&rows_value).map(encode_cursor)
    } else {
        None
    };

    let oversize = row_count >= STREAM_ROW_THRESHOLD;
    let big_payload = if force_stream { false } else { estimate_bytes(&rows_value) >= STREAM_BYTE_THRESHOLD };
    if force_stream || oversize || big_payload {
        return ndjson_stream(rows_value, next_cursor);
    }

    if want_wrapped {
        // Cursor pagination is meaningful only if the table has an `id`
        // column; non-eligible tables just won't advance (next_cursor stays
        // null) — see lib.rs for the v0.1 caveat.
        let body = serde_json::json!({
            "rows": rows_value,
            "next_cursor": next_cursor,
        });
        Json(body).into_response()
    } else {
        Json(rows_value).into_response()
    }
}

/// Pull the `id` field out of the last row, if present and an integer.
fn last_id(rows: &Value) -> Option<i64> {
    let arr = rows.as_array()?;
    let last = arr.last()?;
    last.get("id")?.as_i64()
}

/// Cheap byte-budget estimator. We avoid serialising twice by computing a
/// compact serde_json size; a few percent of slack vs. the wire bytes is fine
/// for a threshold check.
fn estimate_bytes(v: &Value) -> usize {
    serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0)
}

/// Emit one JSON object per line; final line is `{"_basin_next_cursor":"…"}`
/// when paginating.
fn ndjson_stream(rows: Value, next_cursor: Option<String>) -> Response {
    let arr = match rows {
        Value::Array(a) => a,
        other => vec![other],
    };
    let mut buf: Vec<u8> = Vec::with_capacity(64 * arr.len() + 64);
    for row in arr {
        if let Ok(line) = serde_json::to_vec(&row) {
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
    }
    if let Some(tok) = next_cursor {
        let line = serde_json::json!({ "_basin_next_cursor": tok });
        if let Ok(bytes) = serde_json::to_vec(&line) {
            buf.extend_from_slice(&bytes);
            buf.push(b'\n');
        }
    }
    // We chunk a single in-memory buffer into 64 KiB pieces so axum sends
    // the body via `Transfer-Encoding: chunked` — the streaming property the
    // client cares about — without making us hold a second materialised copy.
    let stream = futures::stream::iter(
        buf.chunks(64 * 1024)
            .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
            .collect::<Vec<_>>(),
    );
    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    resp
}

#[axum::debug_handler]
pub(crate) async fn post_table(
    State(state): State<Arc<Inner>>,
    Path(table): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    validate_ident(&table)?;

    if body.is_empty() {
        return Err(ApiError::invalid("POST body is empty"));
    }
    let json: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::invalid(format!("body is not valid JSON: {e}")))?;

    // Accept either a single object or an array of objects. Build (columns,
    // rows) where columns is the union of keys (in first-seen order) and
    // rows align with that column order, padding NULLs for missing keys.
    let raw_rows: Vec<&Map> = match &json {
        Value::Object(o) => vec![o],
        Value::Array(arr) => {
            let mut v = Vec::with_capacity(arr.len());
            for (i, el) in arr.iter().enumerate() {
                match el {
                    Value::Object(o) => v.push(o),
                    _ => {
                        return Err(ApiError::invalid(format!(
                            "POST body[{i}] is not a JSON object"
                        )))
                    }
                }
            }
            v
        }
        _ => {
            return Err(ApiError::invalid(
                "POST body must be a JSON object or array of objects",
            ))
        }
    };
    if raw_rows.is_empty() {
        return Err(ApiError::invalid("POST body has no rows"));
    }

    let mut columns: Vec<String> = Vec::new();
    let mut col_set: HashMap<String, usize> = HashMap::new();
    for row in &raw_rows {
        for k in row.keys() {
            if !col_set.contains_key(k) {
                let validated = validate_ident(k)?;
                col_set.insert(validated.clone(), columns.len());
                columns.push(validated);
            }
        }
    }

    let mut rows: Vec<Vec<Literal>> = Vec::with_capacity(raw_rows.len());
    for row in &raw_rows {
        let mut out = Vec::with_capacity(columns.len());
        for col in &columns {
            match row.get(col) {
                Some(v) => out.push(json_to_literal(v)?),
                None => out.push(Literal::Null),
            }
        }
        rows.push(out);
    }

    let sql = build_insert_sql(&table, &columns, &rows)?;
    let session = state
        .cfg
        .engine
        .open_session(claims.tenant_id)
        .await
        .map_err(ApiError::from)?;
    let res = session.execute(&sql).await.map_err(ApiError::from)?;
    let body = render_exec_result(res);
    // POST returns 201 Created on success per REST conventions.
    Ok((StatusCode::CREATED, body).into_response())
}

#[axum::debug_handler]
pub(crate) async fn patch_table(
    State(state): State<Arc<Inner>>,
    Path(table): Path<String>,
    AxumQuery(raw): AxumQuery<Pairs>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    validate_ident(&table)?;
    let q = parse_query(raw.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;

    if body.is_empty() {
        return Err(ApiError::invalid("PATCH body is empty"));
    }
    let json: Value = serde_json::from_slice(&body)
        .map_err(|e| ApiError::invalid(format!("body is not valid JSON: {e}")))?;

    let sql = build_update_sql(&table, &q, &json)?;
    let session = state
        .cfg
        .engine
        .open_session(claims.tenant_id)
        .await
        .map_err(ApiError::from)?;
    let res = session.execute(&sql).await.map_err(ApiError::from)?;
    Ok(render_exec_result(res).into_response())
}

#[axum::debug_handler]
pub(crate) async fn delete_table(
    State(state): State<Arc<Inner>>,
    Path(table): Path<String>,
    AxumQuery(raw): AxumQuery<Pairs>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    validate_ident(&table)?;
    let q = parse_query(raw.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
    // The engine doesn't support DELETE yet either; build the SQL anyway so
    // we surface a 501 with E_ENGINE_UNSUPPORTED rather than a 500. The day
    // the engine learns DELETE, the build-then-execute path lights up.
    let sql = build_delete_sql(&table, &q)?;
    let session = state
        .cfg
        .engine
        .open_session(claims.tenant_id)
        .await
        .map_err(ApiError::from)?;
    match session.execute(&sql).await {
        Ok(res) => Ok(render_exec_result(res).into_response()),
        Err(e) => {
            // Engine says "not supported" / "DELETE not supported" → translate
            // to E_ENGINE_UNSUPPORTED so the client SDK can surface a clean
            // message instead of a generic 500.
            let msg = e.to_string().to_ascii_lowercase();
            if msg.contains("delete") && (msg.contains("not supported") || msg.contains("unsupported"))
            {
                Err(ApiError::unsupported(format!(
                    "DELETE not supported by engine: {e}"
                )))
            } else {
                Err(ApiError::from(e))
            }
        }
    }
}

type Map = serde_json::Map<String, Value>;

fn render_exec_result(res: ExecResult) -> Json<Value> {
    match res {
        ExecResult::Empty { tag } => Json(serde_json::json!({ "ok": true, "tag": tag })),
        ExecResult::Rows { schema, batches } => Json(batches_to_json(&schema, &batches)),
    }
}
