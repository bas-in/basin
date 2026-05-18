//! Per-project store for async HTTP responses, backed by ordinary Basin
//! tables.
//!
//! Three tables get created lazily on first use:
//!
//! - `_net_http_response` — terminal-state rows pg_net consumers read.
//! - `_net_http_request_queue` — pending requests waiting for the runner.
//!   pg_net keeps these in `net._http_request_queue`; the underscore-prefixed
//!   name is a v0.1 concession to Basin's flat identifier model (the engine's
//!   `Ident` validator rejects a `.` in the table name).
//! - `_net_allowed_hosts` — surfaceable allowlist mirror. Optional in v0.1
//!   (the in-memory [`AllowList`] is the source of truth) but materialising
//!   it as a table is what lets the SQL surface land later as a pure
//!   `INSERT/SELECT` form.
//!
//! All reads and writes go through `Engine::open_session`, so:
//!
//! 1. Project isolation is the same as for user SQL.
//! 2. RLS policies on `_net_http_response` itself are honoured (you can
//!    shape who can read which response under a single project).

use std::sync::Arc;

use arrow_array::Array;
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Utc};

use crate::types::{HttpResponse, RequestId, ResponseRow};

/// Underscore-prefixed because Basin's `Ident` validator rejects a `.`
/// character in v0.1; once a `net` schema is wired in the table name flips
/// to `net._http_response` to match pg_net byte-for-byte.
pub(crate) const RESPONSE_TABLE: &str = "_net_http_response";

/// Store front for the per-project response table.
///
/// Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct ResponseStore {
    inner: Arc<ResponseStoreInner>,
}

struct ResponseStoreInner {
    engine: Engine,
}

impl ResponseStore {
    pub fn new(engine: Engine) -> Self {
        Self {
            inner: Arc::new(ResponseStoreInner { engine }),
        }
    }

    /// Reference to the engine the store writes through.
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Idempotently create the response table under `project`. Safe to call
    /// before every operation.
    pub async fn ensure_tables(&self, project: &ProjectId) -> Result<()> {
        let sess = self.inner.engine.open_session(*project).await?;
        let existing = list_table_names(&sess).await?;
        if !existing.contains(&RESPONSE_TABLE.to_string()) {
            sess.execute(&format!(
                "CREATE TABLE {RESPONSE_TABLE} (
                    id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    status BIGINT NOT NULL,
                    headers TEXT NOT NULL,
                    body TEXT NOT NULL,
                    error TEXT,
                    completed_unix_ms BIGINT NOT NULL
                )"
            ))
            .await?;
        }
        Ok(())
    }

    /// Append one row. Used by the async runner once a response has come back
    /// (or once a terminal failure has been classified).
    pub async fn record(
        &self,
        project: &ProjectId,
        id: RequestId,
        request_id: RequestId,
        resp: &HttpResponse,
    ) -> Result<()> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
        let headers_json = serde_json::to_string(&resp.headers)
            .map_err(|e| BasinError::internal(format!("headers serialise: {e}")))?;
        let body_text = String::from_utf8_lossy(&resp.body).into_owned();
        let error_sql = match &resp.error {
            Some(e) => format!("'{}'", sql_escape(e)),
            None => "NULL".to_string(),
        };
        let now = Utc::now().timestamp_millis();
        sess.execute(&format!(
            "INSERT INTO {RESPONSE_TABLE} VALUES \
             ('{id}', '{request_id}', {status}, '{headers}', '{body}', {error}, {now})",
            id = id,
            request_id = request_id,
            status = resp.status as i64,
            headers = sql_escape(&headers_json),
            body = sql_escape(&body_text),
            error = error_sql,
            now = now,
        ))
        .await?;
        Ok(())
    }

    /// Fetch one response row by id. Returns `None` until the runner has
    /// recorded it.
    pub async fn get(&self, project: &ProjectId, id: RequestId) -> Result<Option<ResponseRow>> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
        let res = sess
            .execute(&format!(
                "SELECT id, request_id, status, headers, body, error, completed_unix_ms \
                 FROM {RESPONSE_TABLE} WHERE id = '{id}'"
            ))
            .await?;
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { .. } => return Ok(None),
        };
        for b in batches {
            if b.num_rows() == 0 {
                continue;
            }
            let ids = downcast_str(&b, "id")?;
            let req_ids = downcast_str(&b, "request_id")?;
            let status = downcast_i64(&b, "status")?;
            let headers = downcast_str(&b, "headers")?;
            let body = downcast_str(&b, "body")?;
            let err = downcast_opt_str(&b, "error")?;
            let completed = downcast_i64(&b, "completed_unix_ms")?;
            if let Some(i) = (0..b.num_rows()).next() {
                let row = ResponseRow {
                    id: parse_uuid(&ids[i])?,
                    request_id: parse_uuid(&req_ids[i])?,
                    status: status[i] as u16,
                    headers: headers[i].clone(),
                    body: body[i].clone(),
                    error: err[i].clone(),
                    completed_at: unix_ms_to_dt(completed[i]),
                };
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    /// List every recorded response for `project`. Useful for tests.
    pub async fn list(&self, project: &ProjectId) -> Result<Vec<ResponseRow>> {
        self.ensure_tables(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
        let res = sess
            .execute(&format!(
                "SELECT id, request_id, status, headers, body, error, completed_unix_ms \
                 FROM {RESPONSE_TABLE} ORDER BY completed_unix_ms"
            ))
            .await?;
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { .. } => return Ok(Vec::new()),
        };
        let mut out = Vec::new();
        for b in batches {
            let ids = downcast_str(&b, "id")?;
            let req_ids = downcast_str(&b, "request_id")?;
            let status = downcast_i64(&b, "status")?;
            let headers = downcast_str(&b, "headers")?;
            let body = downcast_str(&b, "body")?;
            let err = downcast_opt_str(&b, "error")?;
            let completed = downcast_i64(&b, "completed_unix_ms")?;
            for i in 0..b.num_rows() {
                out.push(ResponseRow {
                    id: parse_uuid(&ids[i])?,
                    request_id: parse_uuid(&req_ids[i])?,
                    status: status[i] as u16,
                    headers: headers[i].clone(),
                    body: body[i].clone(),
                    error: err[i].clone(),
                    completed_at: unix_ms_to_dt(completed[i]),
                });
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn unix_ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

fn parse_uuid(s: &str) -> Result<RequestId> {
    uuid::Uuid::parse_str(s).map_err(|e| BasinError::internal(format!("uuid: {e}")))
}

async fn list_table_names(sess: &basin_engine::ProjectSession) -> Result<Vec<String>> {
    let res = sess.execute("SHOW TABLES").await?;
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "expected rows from SHOW TABLES, got Empty({tag})"
            )))
        }
    };
    let mut out = Vec::new();
    for b in batches {
        let names = downcast_str(&b, "table_name")?;
        out.extend(names);
    }
    Ok(out)
}

fn downcast_i64(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<i64>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Int64")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(arr.value(i));
    }
    Ok(v)
}

fn downcast_str(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<String>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Utf8")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(arr.value(i).to_string());
    }
    Ok(v)
}

fn downcast_opt_str(b: &arrow_array::RecordBatch, col: &str) -> Result<Vec<Option<String>>> {
    let arr = b
        .column_by_name(col)
        .ok_or_else(|| BasinError::internal(format!("missing col {col}")))?;
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| BasinError::internal(format!("col {col} not Utf8")))?;
    let mut v = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        v.push(if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        });
    }
    Ok(v)
}
