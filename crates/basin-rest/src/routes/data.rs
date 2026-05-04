//! `/rest/v1/<table>` handlers.
//!
//! Auth is enforced at the per-request level (not via middleware) so the
//! handler has direct access to the verified [`basin_auth::Claims`] and can
//! open a tenant-scoped engine session itself. The pattern matches what
//! basin-router does for pgwire connections.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query as AxumQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_engine::ExecResult;
use serde_json::Value;

use crate::errors::ApiError;
use crate::json::batches_to_json;
use crate::parser::{
    build_delete_sql, build_insert_sql, build_select_sql, build_update_sql, json_to_literal,
    parse_query, validate_ident, Literal,
};
use crate::server::{authorize, Inner};

type Pairs = Vec<(String, String)>;

#[axum::debug_handler]
pub(crate) async fn get_table(
    State(state): State<Arc<Inner>>,
    Path(table): Path<String>,
    AxumQuery(raw): AxumQuery<Pairs>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let _ident = validate_ident(&table)?;
    let q = parse_query(raw.iter().map(|(k, v)| (k.as_str(), v.as_str())))?;
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
    Ok(render_exec_result(res).into_response())
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
