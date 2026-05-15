//! `EngineAuthStore` — the default in-process [`basin_auth::store::AuthStore`].
//!
//! Instead of opening an outbound TCP connection, every auth query is executed
//! via [`basin_engine::Engine::open_session_as`]`(project_id,
//! "basin_auth_service")`. Auth data lives in each project's own `basin_auth.*`
//! schema (using the flat-table-name convention `{schema}_<table>`). No TCP
//! connection or pgwire listener is needed.
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

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use basin_auth::store::{
    ApiKeyRow, AuthMagicLinkRow, AuthStore, AuthUser, EmailTokenRow, MagicLinkEmailTokenRow,
    RefreshRevocationRow, ProjectCredentialRow,
};
use basin_auth::{api_keys::ApiKeyDescriptor, project_credentials::ProjectCredentialDescriptor};
use basin_common::{BasinError, Result, ProjectId};
use basin_engine::{Engine, ExecResult, ScalarParam, ProjectSession};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// An [`AuthStore`] backed by the in-process [`Engine`] instead of an
/// external Postgres connection. All auth tables are stored under the
/// well-known [`basin_auth::INTERNAL_AUTH_PROJECT_ID`] project.
pub struct EngineAuthStore {
    engine: Arc<Engine>,
    /// The table-name prefix used for every auth table. Corresponds to
    /// `AuthConfig::catalog_schema` (default `"basin_auth"`).
    schema: String,
    /// Parsed form of `INTERNAL_AUTH_PROJECT_ID`.
    auth_project: ProjectId,
}

impl EngineAuthStore {
    /// Build a store that talks to `engine` and uses `schema` as the table
    /// prefix (e.g. `"basin_auth"` → tables named `basin_auth_users`, etc.).
    pub fn new(engine: Arc<Engine>, schema: String, auth_project: ProjectId) -> Self {
        Self {
            engine,
            schema,
            auth_project,
        }
    }

    fn sch(&self) -> &str {
        &self.schema
    }

    /// Open a session as the internal auth-service principal.
    async fn session(&self) -> Result<ProjectSession> {
        self.engine
            .open_session_as(self.auth_project, "basin_auth_service")
            .await
    }

    /// Execute a DDL/DML statement with no parameters.
    async fn exec_plain(&self, sql: &str) -> Result<u64> {
        let sess = self.session().await?;
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

fn get_uuid(batch: &arrow_array::RecordBatch, row: usize, col: usize) -> Option<Uuid> {
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

    async fn migrate(&self, schema: &str) -> Result<()> {
        // Mirror basin_auth::schema::run_migrations but run each statement
        // through the engine session instead of tokio_postgres.
        let sch = schema;
        let stmts = vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_users (
                    user_id           UUID PRIMARY KEY,
                    project_id         TEXT NOT NULL,
                    email             TEXT NOT NULL,
                    password_hash     TEXT NOT NULL,
                    email_verified_at TIMESTAMPTZ,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (project_id, email)
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_refresh_tokens (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      UUID NOT NULL REFERENCES {sch}_users(user_id) ON DELETE CASCADE,
                    project_id    TEXT NOT NULL,
                    expires_at   TIMESTAMPTZ NOT NULL,
                    revoked_at   TIMESTAMPTZ,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_email_tokens (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      UUID NOT NULL REFERENCES {sch}_users(user_id) ON DELETE CASCADE,
                    project_id    TEXT NOT NULL,
                    purpose      TEXT NOT NULL,
                    expires_at   TIMESTAMPTZ NOT NULL,
                    consumed_at  TIMESTAMPTZ,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_api_keys (
                    id            BIGSERIAL PRIMARY KEY,
                    project_id     TEXT NOT NULL,
                    user_id       UUID NOT NULL REFERENCES {sch}_users(user_id) ON DELETE CASCADE,
                    name          TEXT NOT NULL,
                    key_hash      TEXT NOT NULL,
                    key_bcrypt    TEXT NOT NULL,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_used_at  TIMESTAMPTZ,
                    revoked_at    TIMESTAMPTZ,
                    UNIQUE (project_id, user_id, name)
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_user_session_settings (
                    project_id   TEXT NOT NULL,
                    user_id     UUID NOT NULL REFERENCES {sch}_users(user_id) ON DELETE CASCADE,
                    key         TEXT NOT NULL,
                    value       TEXT NOT NULL,
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (project_id, user_id, key)
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_auth_magic_links (
                    id           BIGSERIAL PRIMARY KEY,
                    email        TEXT NOT NULL,
                    token_hash   TEXT NOT NULL,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at   TIMESTAMPTZ NOT NULL,
                    consumed_at  TIMESTAMPTZ
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_auth_revoked_refresh_tokens (
                    token_hash   TEXT PRIMARY KEY,
                    user_id      UUID NOT NULL REFERENCES {sch}_users(user_id) ON DELETE CASCADE,
                    revoked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    expires_at   TIMESTAMPTZ NOT NULL
                )"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {sch}_auth_project_credentials (
                    id            BIGSERIAL PRIMARY KEY,
                    project_id     TEXT NOT NULL,
                    pgwire_user   TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    dbname        TEXT NOT NULL DEFAULT 'basin',
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                    rotated_at    TIMESTAMPTZ
                )"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {sch}_users_project_email \
                 ON {sch}_users (project_id, email)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {sch}_api_keys_key_hash \
                 ON {sch}_api_keys (key_hash)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {sch}_auth_magic_links_token_hash \
                 ON {sch}_auth_magic_links (token_hash)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {sch}_auth_revoked_refresh_tokens_user_id \
                 ON {sch}_auth_revoked_refresh_tokens (user_id)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS {sch}_auth_project_credentials_pgwire_user \
                 ON {sch}_auth_project_credentials (pgwire_user)"
            ),
        ];

        for stmt in stmts {
            self.exec_plain(&stmt)
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
            "INSERT INTO {sch}_users (user_id, project_id, email, password_hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (project_id, email) DO NOTHING",
            sch = self.sch()
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

    async fn find_user_by_email(&self, project: &ProjectId, email: &str) -> Result<Option<AuthUser>> {
        let sql = format!(
            "SELECT user_id, email, password_hash, email_verified_at
             FROM {sch}_users
             WHERE project_id = $1 AND email = $2",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_email: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(AuthUser {
                user_id: get_uuid(b, r, 0)
                    .ok_or_else(|| BasinError::internal("find_user_by_email: missing user_id"))?,
                email: get_text(b, r, 1).unwrap_or_default(),
                password_hash: get_text(b, r, 2).unwrap_or_default(),
                email_verified_at: get_ts(b, r, 3),
            })
        })
    }

    async fn find_user_by_id(&self, project: &ProjectId, user_id: Uuid) -> Result<Option<AuthUser>> {
        let sql = format!(
            "SELECT user_id, email, password_hash, email_verified_at
             FROM {sch}_users
             WHERE user_id = $1 AND project_id = $2",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_id: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(AuthUser {
                user_id: get_uuid(b, r, 0)
                    .ok_or_else(|| BasinError::internal("find_user_by_id: missing user_id"))?,
                email: get_text(b, r, 1).unwrap_or_default(),
                password_hash: get_text(b, r, 2).unwrap_or_default(),
                email_verified_at: get_ts(b, r, 3),
            })
        })
    }

    async fn any_user_by_email(&self, email: &str) -> Result<Option<()>> {
        let sql = format!(
            "SELECT 1 FROM {sch}_users WHERE email = $1 LIMIT 1",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("any_user_by_email: {e}")))?;
        Ok(first_row_opt(&batches, |_, _| Ok(()))?)
    }

    async fn latest_user_by_email(&self, email: &str) -> Result<Option<(Uuid, ProjectId)>> {
        let sql = format!(
            "SELECT user_id, project_id FROM {sch}_users
             WHERE email = $1
             ORDER BY created_at DESC
             LIMIT 1",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(email)])
            .await
            .map_err(|e| BasinError::catalog(format!("latest_user_by_email: {e}")))?;
        first_row_opt(&batches, |b, r| {
            let user_id = get_uuid(b, r, 0)
                .ok_or_else(|| BasinError::internal("latest_user_by_email: missing user_id"))?;
            let project_str = get_text(b, r, 1)
                .ok_or_else(|| BasinError::internal("latest_user_by_email: missing project_id"))?;
            let project: ProjectId = project_str.parse().map_err(|e| {
                BasinError::internal(format!("latest_user_by_email project parse: {e}"))
            })?;
            Ok((user_id, project))
        })
    }

    async fn mark_email_verified(&self, project: &ProjectId, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {sch}_users SET email_verified_at = now()
             WHERE user_id = $1 AND project_id = $2",
            sch = self.sch()
        );
        self.exec_params(&sql, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified: {e}")))?;
        Ok(())
    }

    async fn mark_email_verified_if_null(&self, project: &ProjectId, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {sch}_users SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1 AND project_id = $2",
            sch = self.sch()
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
            "UPDATE {sch}_users SET password_hash = $1
             WHERE user_id = $2 AND project_id = $3",
            sch = self.sch()
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
            "INSERT INTO {sch}_email_tokens
               (token_hash, user_id, project_id, purpose, expires_at)
             VALUES ($1, $2, $3, $4, $5)",
            sch = self.sch()
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
             FROM {sch}_email_tokens
             WHERE token_hash = $1 AND project_id = $2",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(token_hash), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("find_email_token: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(EmailTokenRow {
                user_id: get_uuid(b, r, 0)
                    .ok_or_else(|| BasinError::internal("find_email_token: missing user_id"))?,
                expires_at: get_ts(b, r, 1)
                    .ok_or_else(|| BasinError::internal("find_email_token: missing expires_at"))?,
                consumed_at: get_ts(b, r, 2),
                purpose: get_text(b, r, 3).unwrap_or_default(),
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
             FROM {sch}_email_tokens
             WHERE token_hash = $1 AND project_id = $2",
            sch = self.sch()
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
                get_uuid(b, r, 0),
                get_ts(b, r, 1),
                get_ts(b, r, 2),
                get_text(b, r, 3),
            ))
        })? {
            Some((Some(uid), Some(exp), consumed_at, purpose)) => {
                (uid, exp, consumed_at, purpose.unwrap_or_default())
            }
            _ => return Ok(None),
        };

        let (user_id, expires_at, consumed_at, purpose) = token_row;

        // Look up the user's email.
        let sql_user = format!(
            "SELECT email FROM {sch}_users WHERE user_id = $1 AND project_id = $2",
            sch = self.sch()
        );
        let (_, user_batches) = self
            .query_params(&sql_user, vec![p_uuid(user_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("find_magic_link_email_token user: {e}")))?;
        let email = match first_row_opt(&user_batches, |b, r| {
            Ok(get_text(b, r, 0).unwrap_or_default())
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
            "UPDATE {sch}_email_tokens SET consumed_at = now()
             WHERE token_hash = $1 AND project_id = $2 AND consumed_at IS NULL",
            sch = self.sch()
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
            "INSERT INTO {sch}_auth_revoked_refresh_tokens
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO NOTHING",
            sch = self.sch()
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
            "INSERT INTO {sch}_auth_revoked_refresh_tokens
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO UPDATE SET revoked_at = now()",
            sch = self.sch()
        );
        self.exec_params(&sql, vec![p_text(jti), p_uuid(user_id), p_ts(expires_at)])
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_refresh_revocation: {e}")))?;
        Ok(())
    }

    async fn list_refresh_revocations(&self, user_id: Uuid) -> Result<Vec<RefreshRevocationRow>> {
        let sql = format!(
            "SELECT token_hash, revoked_at, expires_at
             FROM {sch}_auth_revoked_refresh_tokens
             WHERE user_id = $1 AND expires_at > now()",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_refresh_revocations: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(RefreshRevocationRow {
                token_hash: get_text(b, r, 0).unwrap_or_default(),
                revoked_at: get_ts(b, r, 1).unwrap_or_else(Utc::now),
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
            "INSERT INTO {sch}_auth_revoked_refresh_tokens
               (token_hash, user_id, revoked_at, expires_at)
             VALUES ($1, $2, now(), $3)
             ON CONFLICT (token_hash) DO UPDATE
               SET revoked_at = now(),
                   expires_at = EXCLUDED.expires_at",
            sch = self.sch()
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
            "INSERT INTO {sch}_api_keys
               (project_id, user_id, name, key_hash, key_bcrypt)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (project_id, user_id, name) DO NOTHING",
            sch = self.sch()
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
            "SELECT id, created_at FROM {sch}_api_keys
             WHERE project_id = $1 AND user_id = $2 AND name = $3
             LIMIT 1",
            sch = self.sch()
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
                get_i64(b, r, 0).unwrap_or(0),
                get_ts(b, r, 1).unwrap_or_else(Utc::now),
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
             FROM {sch}_api_keys
             WHERE key_hash = $1",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(key_hash)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_api_keys_by_hash: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(ApiKeyRow {
                id: get_i64(b, r, 0).unwrap_or(0),
                project_id: get_text(b, r, 1).unwrap_or_default(),
                user_id: get_uuid(b, r, 2).unwrap_or_else(Uuid::nil),
                key_bcrypt: get_text(b, r, 3).unwrap_or_default(),
                revoked_at: get_ts(b, r, 4),
            })
        })
    }

    async fn touch_api_key(&self, id: i64) -> Result<()> {
        let sql = format!(
            "UPDATE {sch}_api_keys SET last_used_at = now() WHERE id = $1",
            sch = self.sch()
        );
        // Best-effort — ignore errors per PostgresAuthStore.
        let _ = self.exec_params(&sql, vec![p_i64(id)]).await;
        Ok(())
    }

    async fn revoke_api_key(&self, project: &ProjectId, key_id: i64) -> Result<()> {
        let sql = format!(
            "UPDATE {sch}_api_keys SET revoked_at = now()
             WHERE id = $1 AND project_id = $2 AND revoked_at IS NULL",
            sch = self.sch()
        );
        let n = self
            .exec_params(&sql, vec![p_i64(key_id), p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("revoke_api_key: {e}")))?;
        if n == 0 {
            // Check if the key exists at all (to distinguish NotFound vs.
            // already-revoked).
            let sql_exists = format!(
                "SELECT 1 FROM {sch}_api_keys WHERE id = $1 AND project_id = $2",
                sch = self.sch()
            );
            let (_, batches) = self
                .query_params(&sql_exists, vec![p_i64(key_id), p_text(project.to_string())])
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
             FROM {sch}_api_keys
             WHERE project_id = $1 AND user_id = $2
             ORDER BY id ASC",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_api_keys: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(ApiKeyDescriptor {
                id: get_i64(b, r, 0).unwrap_or(0),
                name: get_text(b, r, 1).unwrap_or_default(),
                created_at: get_ts(b, r, 2).unwrap_or_else(Utc::now),
                last_used_at: get_ts(b, r, 3),
                revoked_at: get_ts(b, r, 4),
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
            "INSERT INTO {sch}_user_session_settings
               (project_id, user_id, key, value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (project_id, user_id, key)
             DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
            sch = self.sch()
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
             FROM {sch}_user_session_settings
             WHERE project_id = $1 AND user_id = $2",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string()), p_uuid(user_id)])
            .await
            .map_err(|e| BasinError::catalog(format!("list_session_settings: {e}")))?;
        let mut map = HashMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let k = get_text(batch, row, 0).unwrap_or_default();
                let v = get_text(batch, row, 1).unwrap_or_default();
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
            "INSERT INTO {sch}_auth_project_credentials
               (project_id, pgwire_user, password_hash, dbname)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (pgwire_user) DO NOTHING",
            sch = self.sch()
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
             FROM {sch}_auth_project_credentials
             WHERE pgwire_user = $1",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(pgwire_user)])
            .await
            .map_err(|e| BasinError::catalog(format!("find_project_credential: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(ProjectCredentialRow {
                project_id: get_text(b, r, 0).unwrap_or_default(),
                password_hash: get_text(b, r, 1).unwrap_or_default(),
                dbname: get_text(b, r, 2).unwrap_or_default(),
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
            "UPDATE {sch}_auth_project_credentials
               SET password_hash = $1, rotated_at = now()
             WHERE pgwire_user = $2",
            sch = self.sch()
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
            "SELECT project_id, dbname FROM {sch}_auth_project_credentials
             WHERE pgwire_user = $1",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql_select, vec![p_text(pgwire_user)])
            .await
            .map_err(|e| BasinError::catalog(format!("rotate_project_credential select: {e}")))?;
        first_row_opt(&batches, |b, r| {
            Ok(ProjectCredentialRow {
                project_id: get_text(b, r, 0).unwrap_or_default(),
                dbname: get_text(b, r, 1).unwrap_or_default(),
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
             FROM {sch}_auth_project_credentials
             WHERE project_id = $1
             ORDER BY id ASC",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![p_text(project.to_string())])
            .await
            .map_err(|e| BasinError::catalog(format!("list_project_credentials: {e}")))?;
        all_rows(&batches, |b, r| {
            let t_str = get_text(b, r, 1).unwrap_or_default();
            let t: ProjectId = t_str.parse().map_err(|e| {
                BasinError::internal(format!("list_project_credentials project parse: {e}"))
            })?;
            Ok(ProjectCredentialDescriptor {
                id: get_i64(b, r, 0).unwrap_or(0),
                project_id: t,
                pgwire_user: get_text(b, r, 2).unwrap_or_default(),
                dbname: get_text(b, r, 3).unwrap_or_default(),
                created_at: get_ts(b, r, 4).unwrap_or_else(Utc::now),
                rotated_at: get_ts(b, r, 5),
            })
        })
    }

    async fn list_legacy_project_credentials(&self) -> Result<Vec<(ProjectId, String)>> {
        let sql = format!(
            "SELECT project_id, pgwire_user
             FROM {sch}_auth_project_credentials
             WHERE pgwire_user LIKE 'project_%'
             ORDER BY id ASC",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![])
            .await
            .map_err(|e| BasinError::catalog(format!("list_legacy_project_credentials: {e}")))?;
        all_rows(&batches, |b, r| {
            let t_str = get_text(b, r, 0).unwrap_or_default();
            let t: ProjectId = t_str.parse().map_err(|e| {
                BasinError::internal(format!("list_legacy_project_credentials project parse: {e}"))
            })?;
            Ok((t, get_text(b, r, 1).unwrap_or_default()))
        })
    }

    async fn delete_project_credential(&self, pgwire_user: &str) -> Result<()> {
        let sql = format!(
            "DELETE FROM {sch}_auth_project_credentials
             WHERE pgwire_user = $1",
            sch = self.sch()
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
            "INSERT INTO {sch}_auth_magic_links (email, token_hash, expires_at)
             VALUES ($1, $2, $3)",
            sch = self.sch()
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
             FROM {sch}_auth_magic_links
             WHERE consumed_at IS NULL AND expires_at > now()",
            sch = self.sch()
        );
        let (_, batches) = self
            .query_params(&sql, vec![])
            .await
            .map_err(|e| BasinError::catalog(format!("list_active_auth_magic_links: {e}")))?;
        all_rows(&batches, |b, r| {
            Ok(AuthMagicLinkRow {
                id: get_i64(b, r, 0).unwrap_or(0),
                email: get_text(b, r, 1).unwrap_or_default(),
                token_hash: get_text(b, r, 2).unwrap_or_default(),
            })
        })
    }

    /// Atomically mark the project-agnostic magic-link row as consumed.
    /// Mirrors the `consume_email_token` invariant: the `consumed_at IS NULL`
    /// guard ensures single-use even under concurrent requests for the same link.
    async fn consume_auth_magic_link(&self, id: i64) -> Result<u64> {
        let sql = format!(
            "UPDATE {sch}_auth_magic_links
             SET consumed_at = now()
             WHERE id = $1 AND consumed_at IS NULL",
            sch = self.sch()
        );
        self.exec_params(&sql, vec![p_i64(id)])
            .await
            .map_err(|e| BasinError::catalog(format!("consume_auth_magic_link: {e}")))
    }

    async fn mark_email_verified_by_user_id(&self, user_id: Uuid) -> Result<()> {
        let sql = format!(
            "UPDATE {sch}_users
             SET email_verified_at = COALESCE(email_verified_at, now())
             WHERE user_id = $1",
            sch = self.sch()
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
