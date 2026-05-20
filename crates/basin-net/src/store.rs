//! Per-project store for async HTTP responses, backed by ordinary Basin
//! tables.
//!
//! # Phase 5.18.C — `net` reserved schema migration
//!
//! The canonical namespace for net tables is now `net` (`ReservedSchema::Net`
//! per ADR 0022). The primary table is `net._http_response`; the legacy flat
//! name `_net_http_response` is preserved as a back-compat read alias.
//!
//! **Back-compat**: [`ensure_tables`] checks for both the new name
//! (`_http_response`, created fresh) and the legacy name
//! (`_net_http_response`, pre-5.18.C). When the legacy table exists it is
//! recognised as the source of truth until an explicit one-time migration
//! renames it. All new writes go to the canonical name; the legacy table
//! name is surfaced as [`LEGACY_RESPONSE_TABLE`] so operators can script
//! the rename.
//!
//! ## Canonical SQL surface (`net` schema)
//!
//! ```sql
//! SELECT * FROM net._http_response WHERE id = $request_id;
//! ```
//!
//! Three tables get created lazily on first use:
//!
//! - `_http_response` (canonical; was `_net_http_response`) — terminal-state
//!   rows pg_net consumers read.
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
//! 2. RLS policies on `_http_response` / `_net_http_response` itself are
//!    honoured (you can shape who can read which response under a single
//!    project).

use std::sync::Arc;

use arrow_array::Array;
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::{Engine, ExecResult};
use chrono::{DateTime, Utc};

use crate::types::{HttpResponse, RequestId, ResponseRow};

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.C — canonical reserved-schema constants + back-compat aliases
// ─────────────────────────────────────────────────────────────────────────────

/// The reserved schema name for net system tables (ADR 0022 / 5.18.C).
pub const RESERVED_SCHEMA: &str = "net";

/// Canonical bare table name for the HTTP response store within the `net`
/// schema (i.e. the SQL surface is `net._http_response`).
///
/// In the flat catalog the physical name is `_http_response` (without the
/// `net_` prefix that the legacy name carried).
pub(crate) const RESPONSE_TABLE: &str = "_http_response";

/// Legacy flat table name used before Phase 5.18.C. Physical tables created
/// before this migration have this name. [`ensure_tables`] recognises both
/// names; new tables are created under [`RESPONSE_TABLE`].
pub(crate) const LEGACY_RESPONSE_TABLE: &str = "_net_http_response";

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
    ///
    /// # Phase 5.18.C back-compat
    ///
    /// Checks for both the canonical name (`_http_response`) and the legacy
    /// name (`_net_http_response`). If neither exists, creates the canonical
    /// table. If only the legacy table exists it is left in place (data is
    /// preserved) — queries that reference the legacy name continue to work
    /// because [`effective_response_table`] falls back to the legacy name when
    /// only the legacy table is present.
    pub async fn ensure_tables(&self, project: &ProjectId) -> Result<()> {
        let sess = self.inner.engine.open_session(*project).await?;
        let existing = list_table_names(&sess).await?;
        let has_canonical = existing.contains(&RESPONSE_TABLE.to_string());
        let has_legacy = existing.contains(&LEGACY_RESPONSE_TABLE.to_string());
        if !has_canonical && !has_legacy {
            // Neither exists — create the canonical table.
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
        // If only the legacy table exists we leave it in place for back-compat.
        // A one-time ALTER TABLE RENAME migration can be applied out-of-band.
        Ok(())
    }

    /// Return the effective table name to use for reads/writes: the canonical
    /// `_http_response` when the table has been migrated (or newly created),
    /// falling back to the legacy `_net_http_response` for pre-5.18.C
    /// deployments.
    async fn effective_response_table(&self, project: &ProjectId) -> Result<&'static str> {
        let sess = self.inner.engine.open_session(*project).await?;
        let existing = list_table_names(&sess).await?;
        if existing.contains(&RESPONSE_TABLE.to_string()) {
            Ok(RESPONSE_TABLE)
        } else {
            Ok(LEGACY_RESPONSE_TABLE)
        }
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
        let table = self.effective_response_table(project).await?;
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
            "INSERT INTO {table} VALUES \
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
        let table = self.effective_response_table(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
        let res = sess
            .execute(&format!(
                "SELECT id, request_id, status, headers, body, error, completed_unix_ms \
                 FROM {table} WHERE id = '{id}'"
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
        let table = self.effective_response_table(project).await?;
        let sess = self.inner.engine.open_session(*project).await?;
        let res = sess
            .execute(&format!(
                "SELECT id, request_id, status, headers, body, error, completed_unix_ms \
                 FROM {table} ORDER BY completed_unix_ms"
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

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.C — unit tests for namespace constants and back-compat aliases
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod namespace_tests {
    use super::*;

    #[test]
    fn reserved_schema_is_net() {
        assert_eq!(RESERVED_SCHEMA, "net");
    }

    #[test]
    fn canonical_response_table_name() {
        // The canonical name (post-5.18.C) strips the legacy `net_` prefix.
        assert_eq!(RESPONSE_TABLE, "_http_response");
    }

    #[test]
    fn legacy_response_table_name_preserved() {
        // The legacy name must remain unchanged so back-compat checks work.
        assert_eq!(LEGACY_RESPONSE_TABLE, "_net_http_response");
    }

    #[test]
    fn canonical_and_legacy_differ() {
        // Sanity: if they were equal the back-compat logic would be a no-op.
        assert_ne!(RESPONSE_TABLE, LEGACY_RESPONSE_TABLE);
    }

    #[test]
    fn legacy_name_contains_schema_name() {
        // The legacy name `_net_http_response` embeds the schema name `net`
        // as an internal component (with underscore delimiters). This differs
        // from the cron/auth convention where the prefix comes first; the
        // underscore prefix here is a pg_net convention for "internal" tables.
        assert!(
            LEGACY_RESPONSE_TABLE.contains(RESERVED_SCHEMA),
            "legacy table name {LEGACY_RESPONSE_TABLE:?} should contain schema name {RESERVED_SCHEMA:?}"
        );
    }
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
