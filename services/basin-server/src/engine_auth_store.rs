#![allow(clippy::too_many_arguments)]
//! `EngineAuthStore` — the default in-process [`basin_auth::store::AuthStore`].
//!
//! Instead of opening an outbound TCP connection, every auth query is executed
//! via [`basin_engine::Engine::open_session_as`]`(project_id,
//! "basin_auth_service")`. Auth data lives in the `auth` reserved schema per
//! ADR 0022 / Phase 5.18.C (`auth.users`, `auth.refresh_tokens`, etc.).
//! DDL uses a trusted system session (`open_system_session`) that bypasses the
//! `guard_reserved_schema_for_user_ddl` security check. DML uses a regular
//! service session with bare table names (DataFusion strips schema qualifiers
//! at query time). No TCP connection or pgwire listener is needed.
//!
//! This eliminates the loopback bootstrap chicken-and-egg:
//!
//!  - Old: start pgwire listener → wait for bind → basin-auth connects back
//!    over TCP → fill OnceLock → JWT/API-key paths live.
//!  - New: build engine → build `EngineAuthStore` → build `AuthService` (via
//!    [`basin_auth::AuthService::with_store`]) → build `StackedProjectResolver`
//!    → start pgwire (JWT/API-key active from first connection).
//!
//! ## Known engine limitations / workarounds
//!
//! Two methods contain workarounds for current engine gaps:
//!
//! - **`find_magic_link_email_token`**: uses two sequential queries instead of
//!   a JOIN because the engine does not yet support cross-table JOINs (tracked).
//! - **`insert_api_key`**: uses INSERT followed by a SELECT instead of
//!   `INSERT … RETURNING` because the engine does not yet support `RETURNING`
//!   clauses (tracked).
//!
//! ## Parameter handling
//!
//! UUIDs and timestamps are serialised to `ScalarParam::Text` (ISO-8601 /
//! RFC3339) because the engine's type-coercion layer accepts those strings
//! wherever `TIMESTAMPTZ` or `UUID` is expected.
//!
//! ## Phase 5.18.C — canonical auth schema constants
//!
//! All auth tables live in the reserved `auth` schema (ADR 0022).
//! DDL creates them as `auth.<table>` via a trusted system session.
//! DML references bare names (DataFusion pre-registers every qualified table
//! under its bare name at session open, so `users` = `auth.users`).

// Phase 5.18.C auth table bare names (schema = "auth", ADR 0022)
const AUTH_SCHEMA: &str = "auth";
const T_USERS: &str = "users";
const T_REFRESH_TOKENS: &str = "refresh_tokens";
const T_EMAIL_TOKENS: &str = "email_tokens";
const T_API_KEYS: &str = "api_keys";
const T_USER_SESSION_SETTINGS: &str = "user_session_settings";
const T_MAGIC_LINKS: &str = "magic_links";
const T_REVOKED_REFRESH_TOKENS: &str = "revoked_refresh_tokens";
const T_PROJECT_CREDENTIALS: &str = "project_credentials";

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use basin_auth::store::{
    ApiKeyRow, AuthMagicLinkRow, AuthStore, AuthUser, EmailTokenRow, MagicLinkEmailTokenRow,
    ProjectCredentialRow, RefreshRevocationRow,
};
use basin_auth::{api_keys::ApiKeyDescriptor, project_credentials::ProjectCredentialDescriptor};
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::{Engine, ExecResult, ProjectSession, ScalarParam};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// An [`AuthStore`] backed by the in-process [`Engine`] instead of an
/// external Postgres connection. All auth tables are stored in the reserved
/// `auth` schema of [`basin_auth::INTERNAL_AUTH_PROJECT_ID`]'s project
/// (ADR 0022 / Phase 5.18.C).
pub struct EngineAuthStore {
    engine: Arc<Engine>,
    /// Parsed form of `INTERNAL_AUTH_PROJECT_ID`.
    auth_project: ProjectId,
}

impl EngineAuthStore {
    /// Build a store that talks to `engine`.
    ///
    /// The `schema` parameter is accepted for API compatibility but is ignored:
    /// Phase 5.18.C stores all auth tables in the reserved `auth` schema
    /// (`AUTH_SCHEMA` constant) per ADR 0022.
    pub fn new(engine: Arc<Engine>, _schema: String, auth_project: ProjectId) -> Self {
        Self {
            engine,
            auth_project,
        }
    }

    /// Open a regular session as the internal auth-service principal.
    /// Used for DML (INSERT/UPDATE/DELETE/SELECT). Tables are pre-registered
    /// at session open under their bare names so DML uses bare table names.
    async fn session(&self) -> Result<ProjectSession> {
        self.engine
            .open_session_as(self.auth_project, "basin_auth_service")
            .await
    }

    /// Open a trusted system session for DDL in the reserved `auth` schema.
    /// This session bypasses `guard_reserved_schema_for_user_ddl`.
    /// ONLY used by `migrate()` — never exposed to user SQL paths.
    async fn system_session(&self) -> Result<ProjectSession> {
        self.engine.open_system_session(self.auth_project).await
    }

    /// Execute a DDL statement via the trusted system session.
    /// Used ONLY by `migrate()` for `CREATE TABLE / CREATE INDEX` in the
    /// reserved `auth` schema.
    async fn system_exec_plain(&self, sql: &str) -> Result<u64> {
        let sess = self.system_session().await?;
        match sess.execute(sql).await? {
            ExecResult::Empty { tag } => {
                let n = tag
                    .rsplit_once(' ')
                    .and_then(|(_, s)| s.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(n)
            }
            ExecResult::Rows { batches, .. } => {
                Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
            }
        }
    }

    /// Execute a parameterised statement. Returns the row-count tag.
    async fn exec_params(&self, sql: &str, params: Vec<ScalarParam>) -> Result<u64> {
        let sess = self.session().await?;
        let (handle, _) = sess
            .prepare(sql)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore prepare: {e}")))?;
        let bound = sess
            .bind(&handle, params)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore bind: {e}")))?;
        let result = sess
            .execute_bound(bound)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore execute_bound: {e}")))?;
        sess.close_statement(&handle).await;
        match result {
            ExecResult::Empty { tag } => {
                let n = tag
                    .rsplit_once(' ')
                    .and_then(|(_, s)| s.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(n)
            }
            ExecResult::Rows { batches, .. } => {
                Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
            }
        }
    }

    /// Execute a parameterised SELECT and return Arrow batches.
    async fn query_params(
        &self,
        sql: &str,
        params: Vec<ScalarParam>,
    ) -> Result<(Arc<arrow_schema::Schema>, Vec<arrow_array::RecordBatch>)> {
        let sess = self.session().await?;
        let (handle, _) = sess
            .prepare(sql)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore prepare: {e}")))?;
        let bound = sess
            .bind(&handle, params)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore bind: {e}")))?;
        let result = sess
            .execute_bound(bound)
            .await
            .map_err(|e| BasinError::catalog(format!("EngineAuthStore execute_bound: {e}")))?;
        sess.close_statement(&handle).await;
        match result {
            ExecResult::Rows { schema, batches } => Ok((schema, batches)),
            ExecResult::Empty { .. } => Ok((Arc::new(arrow_schema::Schema::empty()), Vec::new())),
        }
    }
}

// ---------------------------------------------------------------------------
// Column-extraction helpers
// ---------------------------------------------------------------------------

/// Look up a column index by name in the batch schema. Returns `None` if the
/// column is not present. Used to look up values by name rather than position,
/// which is necessary because the engine may return columns in alphabetical
/// order rather than the SELECT-specified order.
fn col_idx(batch: &arrow_array::RecordBatch, name: &str) -> Option<usize> {
    batch.schema().index_of(name).ok()
}

/// Get a TEXT value by column name.
fn get_named_text(batch: &arrow_array::RecordBatch, row: usize, name: &str) -> Option<String> {
    let col = col_idx(batch, name)?;
    get_text(batch, row, col)
}

/// Get a UUID value by column name.
fn get_named_uuid(batch: &arrow_array::RecordBatch, row: usize, name: &str) -> Option<Uuid> {
    let col = col_idx(batch, name)?;
    get_uuid_col(batch, row, col)
}

/// Get a TIMESTAMPTZ value by column name.
fn get_named_ts(batch: &arrow_array::RecordBatch, row: usize, name: &str) -> Option<DateTime<Utc>> {
    let col = col_idx(batch, name)?;
    get_ts(batch, row, col)
}

/// Get an i64 value by column name.
fn get_named_i64(batch: &arrow_array::RecordBatch, row: usize, name: &str) -> Option<i64> {
    let col = col_idx(batch, name)?;
    get_i64(batch, row, col)
}

fn get_text(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<String> {
    use arrow_array::Array;
    let c = batch.column(col);
    if c.is_null(row) {
        return None;
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        return Some(a.value(row).to_string());
    }
    // DataFusion may return Utf8View (StringViewArray) for certain expressions
    // (e.g. string functions, projections over Parquet Utf8View columns). Fall
    // through to this path so TEXT columns are always readable.
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::StringViewArray>() {
        return Some(a.value(row).to_string());
    }
    None
}

fn get_i64(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<i64> {
    use arrow_array::Array;
    let c = batch.column(col);
    if c.is_null(row) {
        return None;
    }
    c.as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .map(|a| a.value(row))
}

fn get_ts(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<DateTime<Utc>> {
    use arrow_array::Array;
    use chrono::TimeZone;
    let c = batch.column(col);
    if c.is_null(row) {
        return None;
    }
    if let Some(a) = c
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
    {
        let micros = a.value(row);
        let secs = micros / 1_000_000;
        let nanos = ((micros % 1_000_000) * 1_000) as u32;
        return Utc.timestamp_opt(secs, nanos).single();
    }
    // Fallback: TEXT column with RFC3339 (engine may render timestamps as text).
    get_text(batch, row, col).and_then(|s| s.parse::<DateTime<Utc>>().ok())
}

fn get_uuid_col(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<Uuid> {
    // Engine stores UUID as TEXT; parse it.
    get_text(batch, row, col).and_then(|s| s.parse::<Uuid>().ok())
}

#[allow(dead_code)]
fn get_bool(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<bool> {
    use arrow_array::Array;
    let c = batch.column(col);
    if c.is_null(row) {
        return None;
    }
    c.as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .map(|a| a.value(row))
        // If the engine renders booleans as text in some paths:
        .or_else(|| get_text(batch, row, col).and_then(|s| s.parse::<bool>().ok()))
}

// ---------------------------------------------------------------------------
// ScalarParam helpers for common types
// ---------------------------------------------------------------------------

fn p_text(s: impl Into<String>) -> ScalarParam {
    ScalarParam::Text(s.into())
}

fn p_uuid(u: Uuid) -> ScalarParam {
    ScalarParam::Text(u.to_string())
}

fn p_ts(ts: DateTime<Utc>) -> ScalarParam {
    ScalarParam::Text(ts.to_rfc3339())
}

fn p_i64(n: i64) -> ScalarParam {
    ScalarParam::Int8(n)
}

// ---------------------------------------------------------------------------
// AuthStore implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl AuthStore for EngineAuthStore {
    // --- Migration ----------------------------------------------------------

    async fn migrate(&self, _schema: &str) -> Result<()> {
        // Phase 5.18.C (ADR 0022): auth tables are created in the reserved
        // `auth` schema via a trusted system session. The `_schema` argument is
        // kept for interface compatibility but is ignored — the canonical schema
        // is always AUTH_SCHEMA = "auth".
        //
        // Engine-specific DDL notes:
        //
        // 1. UUID columns are declared as TEXT. The engine's Vortex storage
        //    backend does not yet support FixedSizeBinary(16) — the Arrow
        //    physical type that UUID maps to. Using TEXT is consistent with how
        //    the engine stores all UUID values at runtime (p_uuid serialises
        //    to ScalarParam::Text), so round-trips are lossless.
        //
        // 2. REFERENCES … ON DELETE CASCADE FK constraints are omitted.
        //    The engine enforces FKs by re-reading the referenced column, which
        //    requires the column type to be encodable. Auth referential
        //    integrity is maintained by the application layer.
        let s = AUTH_SCHEMA;
        let stmts: &[&str] = &[
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_USERS} (
                    user_id           TEXT PRIMARY KEY,
                    project_id         TEXT NOT NULL,
                    email             TEXT NOT NULL,
                    password_hash     TEXT NOT NULL,
                    email_verified_at TIMESTAMPTZ,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (project_id, email)
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_REFRESH_TOKENS} (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      TEXT NOT NULL,
                    project_id    TEXT NOT NULL,
                    expires_at   TIMESTAMPTZ NOT NULL,
                    revoked_at   TIMESTAMPTZ,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_EMAIL_TOKENS} (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      TEXT NOT NULL,
                    project_id    TEXT NOT NULL,
                    purpose      TEXT NOT NULL,
                    expires_at   TIMESTAMPTZ NOT NULL,
                    consumed_at  TIMESTAMPTZ,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_API_KEYS} (
                    id            BIGSERIAL PRIMARY KEY,
                    project_id     TEXT NOT NULL,
                    user_id       TEXT NOT NULL,
                    name          TEXT NOT NULL,
                    key_hash      TEXT NOT NULL,
                    key_bcrypt    TEXT NOT NULL,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_used_at  TIMESTAMPTZ,
                    revoked_at    TIMESTAMPTZ,
                    UNIQUE (project_id, user_id, name)
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_USER_SESSION_SETTINGS} (
                    project_id   TEXT NOT NULL,
                    user_id      TEXT NOT NULL,
                    key          TEXT NOT NULL,
                    value        TEXT NOT NULL,
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (project_id, user_id, key)
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_MAGIC_LINKS} (
                    id           BIGSERIAL PRIMARY KEY,
                    email        TEXT NOT NULL,
                    token_hash   TEXT NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at   TIMESTAMPTZ NOT NULL,
                    consumed_at  TIMESTAMPTZ
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_REVOKED_REFRESH_TOKENS} (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      TEXT NOT NULL,
                    revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at   TIMESTAMPTZ NOT NULL
                )"
            ),
            &format!(
                "CREATE TABLE IF NOT EXISTS {s}.{T_PROJECT_CREDENTIALS} (
                    id            BIGSERIAL PRIMARY KEY,
                    project_id     TEXT NOT NULL,
                    pgwire_user   TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    dbname        TEXT NOT NULL DEFAULT 'basin',
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    rotated_at    TIMESTAMPTZ
                )"
            ),
            &format!(
                "CREATE INDEX IF NOT EXISTS users_project_email \
                 ON {s}.{T_USERS} (project_id, email)"
            ),
            &format!(
                "CREATE INDEX IF NOT EXISTS api_keys_key_hash \
                 ON {s}.{T_API_KEYS} (key_hash)"
            ),
            &format!(
                "CREATE INDEX IF NOT EXISTS magic_links_token_hash \
                 ON {s}.{T_MAGIC_LINKS} (token_hash)"
            ),
            &format!(
                "CREATE INDEX IF NOT EXISTS revoked_refresh_tokens_user_id \
                 ON {s}.{T_REVOKED_REFRESH_TOKENS} (user_id)"
            ),
            &format!(
                "CREATE INDEX IF NOT EXISTS project_credentials_pgwire_user \
                 ON {s}.{T_PROJECT_CREDENTIALS} (pgwire_user)"
            ),
        ];

        for stmt in stmts {
            self.system_exec_plain(stmt)
                .await
                .map_err(|e| BasinError::catalog(format!("migrate: {e}")))?;
        }
        Ok(())
    }

    // --- Users --------------------------------------------------------------

    async fn create_user(
        &self,
        project: &ProjectId,
        email: &str,
        password_hash: &str,
        user_id: Uuid,
    ) -> Result<Uuid> {
        let sql = format!(
            "INSERT INTO {T_USERS} (user_id, project_id, email, password_hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (project_id, email) DO NOTHING"
        );
        let inserted = self
            .exec_params(
                &sql,
                vec![
                    p_uuid(user_id),
                    p_text(project.to_string()),
                    p_text(email),
                    p_text(password_hash),
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("create_user: {e}")))?;
        if inserted == 0 {
            return Err(BasinError::CommitConflict(format!(
                "user {email} already exists for project {project}"
            )));
        }
        Ok(user_id)
    }

    async fn find_user_by_email(
        &self,
        project: &ProjectId,
        email: &str,
    ) -> Result<Option<AuthUser>> {
        let sql = format!(
            "SELECT user_id, email, password_hash, email_verified_at
             FROM {T_USERS}
             WHERE project_id = $1 AND email = $2"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_email: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(AuthUser {
                user_id: get_named_uuid(b, r, "user_id")
                    .ok_or_else(|| BasinError::internal("find_user_by_email: missing user_id"))?,
                email: get_named_text(b, r, "email").unwrap_or_default(),
                password_hash: get_named_text(b, r, "password_hash").unwrap_or_default(),
                email_verified_at: get_named_ts(b, r, "email_verified_at"),
            })
        })
    }

    async fn find_user_by_id(
        &self,
        project: &ProjectId,
        user_id: Uuid,
    ) -> Result<Option<AuthUser>> {
        let sql = format!(
            "SELECT user_id, email, password_hash, email_verified_at
             FROM {T_USERS}
             WHERE user_id = $1 AND project_id = $2"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_id: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(AuthUser {
                user_id: get_named_uuid(b, r, "user_id")
                    .ok_or_else(|| BasinError::internal("find_user_by_id: missing user_id"))?,
                email: get_named_text(b, r, "email").unwrap_or_default(),
                password_hash: get_named_text(b, r, "password_hash").unwrap_or_default(),
                email_verified_at: get_named_ts(b, r, "email_verified_at"),
            })
        })
    }

    async fn any_user_by_email(&self, email: &str) -> Result<Option<()>> {
        let sql = format!("SELECT 1 FROM {T_USERS} WHERE email = $1 LIMIT 1");
        let (_, batches) = self
            .query_params(&sql, vec![p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("any_user_by_email: {e}")))?;
        Ok(first_row_opt(&batches, |_, _| Ok(()))?)
    }

    async fn latest_user_by_email(&self, email: &str) -> Result<Option<(Uuid, ProjectId)>> {
        let sql = format!(
            "SELECT user_id, project_id FROM {T_USERS}
             WHERE email = $1
             ORDER BY created_at DESC
             LIMIT 1"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("latest_user_by_email: {e}")))?;
        first_row_opt(&batches, |b, r| {
            let user_id = get_named_uuid(b, r, "user_id")
                .ok_or_else(|| BasinError::internal("latest_user_by_email: missing user_id"))?;
            let project_str = get_named_text(b, r, "project_id")
                .ok_or_else(|| BasinError::internal("latest_user_by_email: missing project_id"))?;
            let project: ProjectId = project_str.parse().map_err(|e| {
                BasinError::internal(format!("latest_user_by_email project parse: {e}"))
            })?;
            Ok((user_id, project))
        })
    }

    async fn mark_email_verified(&self, project: &ProjectId, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {T_USERS} SET email_verified_at = now()
             WHERE user_id = $1 AND project_id = $2"
        );
        self.exec_params(&sql, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified: {e}")))?;
        Ok(())
    }

    async fn mark_email_verified_if_null(&self, project: &ProjectId, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {T_USERS} SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1 AND project_id = $2"
        );
        self.exec_params(&sql, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified_if_null: {e}")))?;
        Ok(())
    }

    async fn update_password(
        &self,
        project: &ProjectId,
        user_id: Uuid,
        password_hash: &str,
    ) -> Result<()> {
        let sql = format!(
            "UPDATE {T_USERS} SET password_hash = $1
             WHERE user_id = $2 AND project_id = $3"
        );
        self.exec_params(
            &sql,
            vec![
                p_text(password_hash),
                p_uuid(user_id),
                p_text(project.to_string()),
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("update_password: {e}")))?;
        Ok(())
    }

    // --- Email tokens -------------------------------------------------------

    async fn insert_email_token(
        &self,
        project: &ProjectId,
        user_id: Uuid,
        token_hash: &str,
        purpose: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {T_EMAIL_TOKENS}
               (token_hash, user_id, project_id, purpose, expires_at)
             VALUES ($1, $2, $3, $4, $5)"
        );
        self.exec_params(
            &sql,
            vec![
                p_text(token_hash),
                p_uuid(user_id),
                p_text(project.to_string()),
                p_text(purpose),
                p_ts(expires_at),
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("insert_email_token: {e}")))?;
        Ok(())
    }

    async fn find_email_token(
        &self,
        project: &ProjectId,
        token_hash: &str,
    ) -> Result<Option<EmailTokenRow>> {
        let sql = format!(
            "SELECT user_id, expires_at, consumed_at, purpose
             FROM {T_EMAIL_TOKENS}
             WHERE token_hash = $1 AND project_id = $2"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(token_hash), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("find_email_token: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(EmailTokenRow {
                user_id: get_named_uuid(b, r, "user_id")
                    .ok_or_else(|| BasinError::internal("find_email_token: missing user_id"))?,
                expires_at: get_named_ts(b, r, "expires_at")
                    .ok_or_else(|| BasinError::internal("find_email_token: missing expires_at"))?,
                consumed_at: get_named_ts(b, r, "consumed_at"),
                purpose: get_named_text(b, r, "purpose").unwrap_or_default(),
            })
        })
    }

    /// Workaround: uses two sequential queries instead of a single JOIN because
    /// the engine does not yet support cross-table JOINs. Fetches the email
    /// token row first, then looks up the user's email by `user_id`.
    async fn find_magic_link_email_token(
        &self,
        project: &ProjectId,
        token_hash: &str,
    ) -> Result<Option<MagicLinkEmailTokenRow>> {
        // Note: basin engine does not yet support JOIN across tables. To work
        // around this, we execute two queries: first get the email_token row,
        // then look up the user's email.
        let sql_token = format!(
            "SELECT user_id, expires_at, consumed_at, purpose
             FROM {T_EMAIL_TOKENS}
             WHERE token_hash = $1 AND project_id = $2"
        );
        let (_, batches) = self
            .query_params(
                &sql_token,
                vec![p_text(token_hash), p_text(project.to_string())],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_magic_link_email_token token: {e}")))?;

        let token_row = match first_row_opt(&batches, |b, r| {
            Ok((
                get_named_uuid(b, r, "user_id"),
                get_named_ts(b, r, "expires_at"),
                get_named_ts(b, r, "consumed_at"),
                get_named_text(b, r, "purpose"),
            ))
        })? {
            Some((Some(uid), Some(exp), consumed_at, purpose)) => {
                (uid, exp, consumed_at, purpose.unwrap_or_default())
            }
            _ => return Ok(None),
        };

        let (user_id, expires_at, consumed_at, purpose) = token_row;

        // Look up the user's email.
        let sql_user =
            format!("SELECT email FROM {T_USERS} WHERE user_id = $1 AND project_id = $2");
        let (_, user_batches) = self
            .query_params(
                &sql_user,
                vec![p_uuid(user_id), p_text(project.to_string())],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_magic_link_email_token user: {e}")))?;
        let email = match first_row_opt(&user_batches, |b, r| {
            Ok(get_named_text(b, r, "email").unwrap_or_default())
        })? {
            Some(e) => e,
            None => return Ok(None),
        };

        Ok(Some(MagicLinkEmailTokenRow {
            user_id,
            expires_at,
            consumed_at,
            purpose,
            email,
        }))
    }

    /// Atomically mark a token as consumed by filtering on `consumed_at IS NULL`.
    /// The single-row `UPDATE` is the serialisation point: concurrent callers
    /// receive a row-count of 0 and are rejected by the flow layer.
    async fn consume_email_token(&self, project: &ProjectId, token_hash: &str) -> Result<u64> {
        let sql = format!(
            "UPDATE {T_EMAIL_TOKENS} SET consumed_at = now()
             WHERE token_hash = $1 AND project_id = $2 AND consumed_at IS NULL"
        );
        self.exec_params(&sql, vec![p_text(token_hash), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_email_token: {e}")))
    }

    // --- Revoked refresh tokens ---------------------------------------------

    /// Record a revoked JTI. `ON CONFLICT DO NOTHING` makes this idempotent —
    /// the refresh rotation path calls it once per rotation, but a retry or
    /// duplicate delivery must not turn into an error.
    async fn insert_refresh_revocation(
        &self,
        jti: &str,
        user_id: Uuid,
        expires_at: DateTime<Utc>,
    ) -> Result<u64> {
        let sql = format!(
            "INSERT INTO {T_REVOKED_REFRESH_TOKENS}
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO NOTHING"
        );
        self.exec_params(&sql, vec![p_text(jti), p_uuid(user_id), p_ts(expires_at)])
            .await
            .map_err(|e| BasinError::catalog(format!("insert_refresh_revocation: {e}")))
    }

    async fn upsert_refresh_revocation(
        &self,
        jti: &str,
        user_id: Uuid,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {T_REVOKED_REFRESH_TOKENS}
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO UPDATE SET revoked_at = now()"
        );
        self.exec_params(&sql, vec![p_text(jti), p_uuid(user_id), p_ts(expires_at)])
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_refresh_revocation: {e}")))?;
        Ok(())
    }

    async fn list_refresh_revocations(&self, user_id: Uuid) -> Result<Vec<RefreshRevocationRow>> {
        let sql = format!(
            "SELECT token_hash, revoked_at, expires_at
             FROM {T_REVOKED_REFRESH_TOKENS}
             WHERE user_id = $1 AND expires_at > now()"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_refresh_revocations: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(RefreshRevocationRow {
                token_hash: get_named_text(b, r, "token_hash").unwrap_or_default(),
                revoked_at: get_named_ts(b, r, "revoked_at").unwrap_or_else(Utc::now),
            })
        })
    }

    async fn upsert_blanket_revocation(
        &self,
        blanket_key: &str,
        user_id: Uuid,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {T_REVOKED_REFRESH_TOKENS}
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO UPDATE
               SET revoked_at = now(),
                   expires_at = EXCLUDED.expires_at"
        );
        self.exec_params(
            &sql,
            vec![p_text(blanket_key), p_uuid(user_id), p_ts(expires_at)],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("upsert_blanket_revocation: {e}")))?;
        Ok(())
    }

    // --- API keys -----------------------------------------------------------

    /// Workaround: uses INSERT followed by a SELECT to recover `(id,
    /// created_at)` because the engine does not yet support `RETURNING`
    /// clauses. Returns `CommitConflict` if `(project, user, name)` already
    /// exists.
    async fn insert_api_key(
        &self,
        project: &ProjectId,
        user_id: Uuid,
        name: &str,
        key_hash: &str,
        key_bcrypt: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        // `RETURNING` is not yet supported by the engine. Insert, then
        // SELECT the row back by the unique (project, user, name) key.
        let sql_insert = format!(
            "INSERT INTO {T_API_KEYS}
               (project_id, user_id, name, key_hash, key_bcrypt)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (project_id, user_id, name) DO NOTHING"
        );
        let inserted = self
            .exec_params(
                &sql_insert,
                vec![
                    p_text(project.to_string()),
                    p_uuid(user_id),
                    p_text(name),
                    p_text(key_hash),
                    p_text(key_bcrypt),
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_api_key insert: {e}")))?;
        if inserted == 0 {
            return Err(BasinError::CommitConflict(format!(
                "api key {name:?} already exists for user"
            )));
        }

        let sql_select = format!(
            "SELECT id, created_at FROM {T_API_KEYS}
             WHERE project_id = $1 AND user_id = $2 AND name = $3
             LIMIT 1"
        );
        let (_, batches) = self
            .query_params(
                &sql_select,
                vec![p_text(project.to_string()), p_uuid(user_id), p_text(name)],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_api_key select: {e}")))?;
        match first_row_opt(&batches, |b, r| {
            Ok((
                get_named_i64(b, r, "id").unwrap_or(0),
                get_named_ts(b, r, "created_at").unwrap_or_else(Utc::now),
            ))
        })? {
            Some(pair) => Ok(pair),
            None => Err(BasinError::internal(
                "insert_api_key: inserted row not found in subsequent SELECT",
            )),
        }
    }

    async fn find_api_keys_by_hash(&self, key_hash: &str) -> Result<Vec<ApiKeyRow>> {
        let sql = format!(
            "SELECT id, project_id, user_id, key_bcrypt, revoked_at
             FROM {T_API_KEYS}
             WHERE key_hash = $1"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(key_hash)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_api_keys_by_hash: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(ApiKeyRow {
                id: get_named_i64(b, r, "id").unwrap_or(0),
                project_id: get_named_text(b, r, "project_id").unwrap_or_default(),
                user_id: get_named_uuid(b, r, "user_id").unwrap_or_else(Uuid::nil),
                key_bcrypt: get_named_text(b, r, "key_bcrypt").unwrap_or_default(),
                revoked_at: get_named_ts(b, r, "revoked_at"),
            })
        })
    }

    async fn touch_api_key(&self, id: i64) -> Result<()> {
        let sql = format!("UPDATE {T_API_KEYS} SET last_used_at = now() WHERE id = $1");
        // Best-effort — ignore errors per PostgresAuthStore.
        let _ = self.exec_params(&sql, vec![p_i64(id)]).await;
        Ok(())
    }

    async fn revoke_api_key(&self, project: &ProjectId, key_id: i64) -> Result<()> {
        let sql = format!(
            "UPDATE {T_API_KEYS} SET revoked_at = now()
             WHERE id = $1 AND project_id = $2 AND revoked_at IS NULL"
        );
        let n = self
            .exec_params(&sql, vec![p_i64(key_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("revoke_api_key: {e}")))?;
        if n == 0 {
            // Check if the key exists at all (to distinguish NotFound vs.
            // already-revoked).
            let sql_exists =
                format!("SELECT 1 FROM {T_API_KEYS} WHERE id = $1 AND project_id = $2");
            let (_, batches) = self
                .query_params(
                    &sql_exists,
                    vec![p_i64(key_id), p_text(project.to_string())],
                )
                .await
                .map_err(|e| BasinError::catalog(format!("revoke_api_key check: {e}")))?;
            let exists = batches.iter().any(|b| b.num_rows() > 0);
            if !exists {
                return Err(BasinError::not_found(format!("api key {key_id}")));
            }
            // Already revoked — idempotent.
        }
        Ok(())
    }

    async fn list_api_keys(
        &self,
        project: &ProjectId,
        user_id: Uuid,
    ) -> Result<Vec<ApiKeyDescriptor>> {
        let sql = format!(
            "SELECT id, name, created_at, last_used_at, revoked_at
             FROM {T_API_KEYS}
             WHERE project_id = $1 AND user_id = $2
             ORDER BY id ASC"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_api_keys: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(ApiKeyDescriptor {
                id: get_named_i64(b, r, "id").unwrap_or(0),
                name: get_named_text(b, r, "name").unwrap_or_default(),
                created_at: get_named_ts(b, r, "created_at").unwrap_or_else(Utc::now),
                last_used_at: get_named_ts(b, r, "last_used_at"),
                revoked_at: get_named_ts(b, r, "revoked_at"),
            })
        })
    }

    // --- Session settings ---------------------------------------------------

    async fn upsert_session_setting(
        &self,
        project: &ProjectId,
        user_id: Uuid,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {T_USER_SESSION_SETTINGS}
               (project_id, user_id, key, value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (project_id, user_id, key)
             DO UPDATE SET value = EXCLUDED.value, updated_at = now()"
        );
        self.exec_params(
            &sql,
            vec![
                p_text(project.to_string()),
                p_uuid(user_id),
                p_text(key),
                p_text(value),
            ],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("upsert_session_setting: {e}")))?;
        Ok(())
    }

    async fn list_session_settings(
        &self,
        project: &ProjectId,
        user_id: Uuid,
    ) -> Result<HashMap<String, String>> {
        let sql = format!(
            "SELECT key, value
             FROM {T_USER_SESSION_SETTINGS}
             WHERE project_id = $1 AND user_id = $2"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_session_settings: {e}")))?;
        let mut map = HashMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let k = get_named_text(batch, row, "key").unwrap_or_default();
                let v = get_named_text(batch, row, "value").unwrap_or_default();
                map.insert(k, v);
            }
        }
        Ok(map)
    }

    // --- Project credentials -------------------------------------------------

    async fn insert_project_credential(
        &self,
        project: &ProjectId,
        pgwire_user: &str,
        password_hash: &str,
        dbname: &str,
    ) -> Result<bool> {
        let sql = format!(
            "INSERT INTO {T_PROJECT_CREDENTIALS}
               (project_id, pgwire_user, password_hash, dbname)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (pgwire_user) DO NOTHING"
        );
        let n = self
            .exec_params(
                &sql,
                vec![
                    p_text(project.to_string()),
                    p_text(pgwire_user),
                    p_text(password_hash),
                    p_text(dbname),
                ],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_project_credential: {e}")))?;
        Ok(n > 0)
    }

    async fn find_project_credential(
        &self,
        pgwire_user: &str,
    ) -> Result<Option<ProjectCredentialRow>> {
        let sql = format!(
            "SELECT project_id, password_hash, dbname
             FROM {T_PROJECT_CREDENTIALS}
             WHERE pgwire_user = $1"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(pgwire_user)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_project_credential: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(ProjectCredentialRow {
                project_id: get_named_text(b, r, "project_id").unwrap_or_default(),
                password_hash: get_named_text(b, r, "password_hash").unwrap_or_default(),
                dbname: get_named_text(b, r, "dbname").unwrap_or_default(),
            })
        })
    }

    /// Workaround: UPDATE followed by SELECT because the engine does not yet
    /// support `RETURNING`. Returns `None` if `pgwire_user` is not found.
    async fn rotate_project_credential(
        &self,
        pgwire_user: &str,
        password_hash: &str,
    ) -> Result<Option<ProjectCredentialRow>> {
        // Engine doesn't support RETURNING; update then select.
        let sql_update = format!(
            "UPDATE {T_PROJECT_CREDENTIALS}
               SET password_hash = $1, rotated_at = now()
             WHERE pgwire_user = $2"
        );
        let n = self
            .exec_params(
                &sql_update,
                vec![p_text(password_hash), p_text(pgwire_user)],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("rotate_project_credential update: {e}")))?;
        if n == 0 {
            return Ok(None);
        }
        let sql_select = format!(
            "SELECT project_id, dbname FROM {T_PROJECT_CREDENTIALS}
             WHERE pgwire_user = $1"
        );
        let (_, batches) = self
            .query_params(&sql_select, vec![p_text(pgwire_user)])
            .await
            .map_err(|e| BasinError::catalog(format!("rotate_project_credential select: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(ProjectCredentialRow {
                project_id: get_named_text(b, r, "project_id").unwrap_or_default(),
                dbname: get_named_text(b, r, "dbname").unwrap_or_default(),
                password_hash: String::new(), // Not returned after rotation.
            })
        })
    }

    async fn list_project_credentials(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<ProjectCredentialDescriptor>> {
        let sql = format!(
            "SELECT id, project_id, pgwire_user, dbname, created_at, rotated_at
             FROM {T_PROJECT_CREDENTIALS}
             WHERE project_id = $1
             ORDER BY id ASC"
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("list_project_credentials: {e}")))?;
        all_rows(&batches, |b, r| {
            let t_str = get_named_text(b, r, "project_id").unwrap_or_default();
            let t: ProjectId = t_str.parse().map_err(|e| {
                BasinError::internal(format!("list_project_credentials project parse: {e}"))
            })?;
            Ok(ProjectCredentialDescriptor {
                id: get_named_i64(b, r, "id").unwrap_or(0),
                project_id: t,
                pgwire_user: get_named_text(b, r, "pgwire_user").unwrap_or_default(),
                dbname: get_named_text(b, r, "dbname").unwrap_or_default(),
                created_at: get_named_ts(b, r, "created_at").unwrap_or_else(Utc::now),
                rotated_at: get_named_ts(b, r, "rotated_at"),
            })
        })
    }

    async fn list_legacy_project_credentials(&self) -> Result<Vec<(ProjectId, String)>> {
        let sql = format!(
            "SELECT project_id, pgwire_user
             FROM {T_PROJECT_CREDENTIALS}
             WHERE pgwire_user LIKE 'project_%'
             ORDER BY id ASC"
        );
        let (_, batches) = self
            .query_params(&sql, vec![])
            .await
            .map_err(|e| BasinError::catalog(format!("list_legacy_project_credentials: {e}")))?;
        all_rows(&batches, |b, r| {
            let t_str = get_named_text(b, r, "project_id").unwrap_or_default();
            let t: ProjectId = t_str.parse().map_err(|e| {
                BasinError::internal(format!(
                    "list_legacy_project_credentials project parse: {e}"
                ))
            })?;
            Ok((t, get_named_text(b, r, "pgwire_user").unwrap_or_default()))
        })
    }

    async fn delete_project_credential(&self, pgwire_user: &str) -> Result<()> {
        let sql = format!(
            "DELETE FROM {T_PROJECT_CREDENTIALS}
             WHERE pgwire_user = $1"
        );
        let n = self
            .exec_params(&sql, vec![p_text(pgwire_user)])
            .await
            .map_err(|e| BasinError::catalog(format!("delete_project_credential: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "pgwire_user {pgwire_user:?} not found for deletion"
            )));
        }
        Ok(())
    }

    // --- Auth magic links ---------------------------------------------------

    async fn insert_auth_magic_link(
        &self,
        email: &str,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {T_MAGIC_LINKS} (email, token_hash, expires_at)
             VALUES ($1, $2, $3)"
        );
        self.exec_params(
            &sql,
            vec![p_text(email), p_text(token_hash), p_ts(expires_at)],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("insert_auth_magic_link: {e}")))?;
        Ok(())
    }

    async fn list_active_auth_magic_links(&self) -> Result<Vec<AuthMagicLinkRow>> {
        let sql = format!(
            "SELECT id, email, token_hash
             FROM {T_MAGIC_LINKS}
             WHERE consumed_at IS NULL AND expires_at > now()"
        );
        let (_, batches) = self
            .query_params(&sql, vec![])
            .await
            .map_err(|e| BasinError::catalog(format!("list_active_auth_magic_links: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(AuthMagicLinkRow {
                id: get_named_i64(b, r, "id").unwrap_or(0),
                email: get_named_text(b, r, "email").unwrap_or_default(),
                token_hash: get_named_text(b, r, "token_hash").unwrap_or_default(),
            })
        })
    }

    /// Atomically mark the project-agnostic magic-link row as consumed.
    /// Mirrors the `consume_email_token` invariant: the `consumed_at IS NULL`
    /// guard ensures single-use even under concurrent requests for the same link.
    async fn consume_auth_magic_link(&self, id: i64) -> Result<u64> {
        let sql = format!(
            "UPDATE {T_MAGIC_LINKS}
             SET consumed_at = now()
             WHERE id = $1 AND consumed_at IS NULL"
        );
        self.exec_params(&sql, vec![p_i64(id)])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_auth_magic_link: {e}")))
    }

    async fn mark_email_verified_by_user_id(&self, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {T_USERS}
             SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1"
        );
        self.exec_params(&sql, vec![p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified_by_user_id: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Batch-iteration helpers
// ---------------------------------------------------------------------------

/// Call `f(batch, row_idx)` for the first row across all batches.
/// Returns `Ok(None)` if there are no rows.
fn first_row_opt<T>(
    batches: &[arrow_array::RecordBatch],
    f: impl Fn(&arrow_array::RecordBatch, usize) -> Result<T>,
) -> Result<Option<T>> {
    for batch in batches {
        if batch.num_rows() > 0 {
            return f(batch, 0).map(Some);
        }
    }
    Ok(None)
}

/// Call `f(batch, row_idx)` for every row across all batches, collecting
/// the results into a `Vec`.
fn all_rows<T>(
    batches: &[arrow_array::RecordBatch],
    f: impl Fn(&arrow_array::RecordBatch, usize) -> Result<T>,
) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            out.push(f(batch, row)?);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// AuthStore conformance tests — EngineAuthStore (Phase 5.10)
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    //! Full `AuthStore` conformance suite against the in-process engine backend.
    //!
    //! Run with:
    //!
    //! ```bash
    //! cargo test -p basin-server --features test-utils
    //! ```
    //!
    //! The `test-utils` feature enables `basin_auth::store::conformance` (gated
    //! by `#[cfg(any(test, feature = "test-utils"))]` in basin-auth).

    use std::sync::Arc;

    use basin_auth::store::conformance::test_auth_store_conformance;
    use basin_auth::store::AuthStore;
    use basin_catalog::InMemoryCatalog;
    use basin_common::ProjectId;
    use basin_engine::{Engine, EngineConfig};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::EngineAuthStore;

    fn build_engine(dir: &TempDir) -> Arc<Engine> {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        Arc::new(Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        }))
    }

    async fn build_store(dir: &TempDir) -> Arc<dyn AuthStore> {
        let engine = build_engine(dir);
        let schema = "basin_auth".to_string();
        let auth_project: ProjectId = basin_auth::INTERNAL_AUTH_PROJECT_ID
            .parse()
            .expect("INTERNAL_AUTH_PROJECT_ID must be valid");
        let store = Arc::new(EngineAuthStore::new(engine, schema.clone(), auth_project));
        store
            .migrate(&schema)
            .await
            .expect("EngineAuthStore migration must succeed");
        store as Arc<dyn AuthStore>
    }

    /// Full `AuthStore` conformance suite against `EngineAuthStore`.
    ///
    /// Covers all Phase 5.10 invariants:
    ///  1. User uniqueness per project + cross-project same email
    ///  2. Single-use email tokens
    ///  3. Refresh rotation + blanket-revoke sentinel
    ///  4. API key lifecycle (insert → validate → revoke → validate fails)
    ///  5. Self-routing credential parsing + malformed input handling
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_auth_store_conformance() {
        let dir = TempDir::new().expect("tempdir");
        let store = build_store(&dir).await;
        test_auth_store_conformance(store).await;
    }
}
