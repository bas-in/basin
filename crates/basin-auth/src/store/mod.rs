//! `AuthStore` — the persistence abstraction for basin-auth.
//!
//! Concrete implementations:
//!   - [`postgres::PostgresAuthStore`] — wraps `tokio_postgres::Client`; the
//!     existing production path.
//!   - `EngineAuthStore` (in `basin-server`) — talks to basin engine's own SQL
//!     executor without a separate connection. basin-auth never imports it.
//!
//! The trait is deliberately high-level: every method matches a concrete
//! operation the flow modules need. There is no "execute arbitrary SQL"
//! escape hatch — callers write to the trait, implementations write SQL.

/// Shared conformance test runner for the `AuthStore` trait. Available in
/// test builds (including integration tests) and when the `test-utils`
/// feature is enabled (for cross-crate test suites such as
/// `services/basin-server/src/engine_auth_store.rs`).
#[cfg(any(test, feature = "test-utils"))]
pub mod conformance;
pub mod postgres;

use std::collections::HashMap;

use async_trait::async_trait;
use basin_common::{ProjectId, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::UserId;

// ---------------------------------------------------------------------------
// Row types returned from the store
// ---------------------------------------------------------------------------

/// A user row as returned by find-by-email / find-by-id. Carries only what
/// the flow modules inspect; no internal columns (e.g. raw password hash is
/// not exposed — use `password_hash` specifically where needed).
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: UserId,
    pub email: String,
    pub password_hash: String,
    pub email_verified_at: Option<DateTime<Utc>>,
}

/// An API key row for the validate path. The `key_bcrypt` is needed for the
/// bcrypt verify step; the rest are returned to the caller on success.
#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub id: i64,
    pub project_id: String,
    pub user_id: Uuid,
    pub key_bcrypt: String,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// A project-credential row for the validate / rotate paths.
#[derive(Debug, Clone)]
pub struct ProjectCredentialRow {
    pub project_id: String,
    pub dbname: String,
    pub password_hash: String,
}

// ---------------------------------------------------------------------------
// The trait itself
// ---------------------------------------------------------------------------

/// Storage abstraction for basin-auth. The two concrete implementations are
/// [`postgres::PostgresAuthStore`] (wraps an external `tokio_postgres::Client`,
/// active when `BASIN_AUTH_CATALOG_DSN` is set) and `EngineAuthStore` in
/// `basin-server` (routes SQL through the in-process Basin engine, the default
/// path when no external DSN is configured).
///
/// All methods return `crate::Result` (i.e. `basin_common::Result`). SQL
/// errors are wrapped with [`basin_common::BasinError::catalog`].
/// Implementations must be `Send + Sync` so they can be wrapped in `Arc` and
/// shared across async tasks.
#[async_trait]
pub trait AuthStore: Send + Sync {
    /// Run idempotent `CREATE TABLE / INDEX IF NOT EXISTS` DDL against `schema`
    /// (used as a table-name prefix, e.g. `"basin_auth"` → `basin_auth_users`).
    /// Safe to call on every startup; statements are no-ops when the objects
    /// already exist. Called automatically by [`crate::AuthService::with_store`]
    /// before the service is returned to callers.
    async fn migrate(&self, schema: &str) -> Result<()>;

    // --- Users --------------------------------------------------------------

    /// Insert a new user row. Returns the new `UserId` on success.
    /// Returns `CommitConflict` if `(project, email)` already exists.
    async fn create_user(
        &self,
        project: &ProjectId,
        email: &str,
        password_hash: &str,
        user_id: UserId,
    ) -> Result<UserId>;

    /// Look up a user by `(project, email)`. Returns `None` if not found.
    async fn find_user_by_email(
        &self,
        project: &ProjectId,
        email: &str,
    ) -> Result<Option<AuthUser>>;

    /// Look up a user by `(project, user_id)`. Returns `None` if not found.
    async fn find_user_by_id(
        &self,
        project: &ProjectId,
        user_id: UserId,
    ) -> Result<Option<AuthUser>>;

    /// Return any user (from any project) matching `email`. Used by the
    /// project-agnostic email-link flow to check existence before issuing a
    /// link. Returns `None` if no user has this email in any project.
    async fn any_user_by_email(&self, email: &str) -> Result<Option<()>>;

    /// Return the most-recently-created user matching `email` across all
    /// projects. Used by `email_link::consume`.
    async fn latest_user_by_email(&self, email: &str) -> Result<Option<(UserId, ProjectId)>>;

    /// Mark `email_verified_at = now()` for `(project, user_id)`.
    async fn mark_email_verified(&self, project: &ProjectId, user_id: UserId) -> Result<()>;

    /// Mark `email_verified_at = COALESCE(email_verified_at, now())` — sets
    /// the timestamp only if it hasn't been set yet. Used by magic-link
    /// flows where a user may already be verified.
    async fn mark_email_verified_if_null(&self, project: &ProjectId, user_id: UserId)
        -> Result<()>;

    /// Update the password hash for a user.
    async fn update_password(
        &self,
        project: &ProjectId,
        user_id: UserId,
        password_hash: &str,
    ) -> Result<()>;

    // --- Email tokens -------------------------------------------------------

    /// Insert an email token row (verify, reset, magic-link purpose).
    async fn insert_email_token(
        &self,
        project: &ProjectId,
        user_id: UserId,
        token_hash: &str,
        purpose: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Look up an email token by hash + project. Returns the full row
    /// columns needed for validation.
    async fn find_email_token(
        &self,
        project: &ProjectId,
        token_hash: &str,
    ) -> Result<Option<EmailTokenRow>>;

    /// Look up a magic-link email token by hash + project, joining the user
    /// table to return the user's email in one round trip.
    async fn find_magic_link_email_token(
        &self,
        project: &ProjectId,
        token_hash: &str,
    ) -> Result<Option<MagicLinkEmailTokenRow>>;

    /// Single-use atomic consume: sets `consumed_at = now()` only when it is
    /// currently `NULL`. Returns the number of rows updated; `0` means the
    /// token was already consumed or never existed — both cases map to an
    /// error in the flow layer.
    async fn consume_email_token(&self, project: &ProjectId, token_hash: &str) -> Result<u64>;

    // --- Revoked refresh tokens ---------------------------------------------

    /// Record that a refresh JWT `jti` has been rotated / signed-out.
    /// Uses `ON CONFLICT DO NOTHING` so the call is idempotent.
    async fn insert_refresh_revocation(
        &self,
        jti: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<u64>;

    /// Upsert a refresh-revocation row with `ON CONFLICT DO UPDATE SET
    /// revoked_at = now()`. Used by `signout`.
    async fn upsert_refresh_revocation(
        &self,
        jti: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Return all non-expired revocation rows for `user_id`.
    async fn list_refresh_revocations(&self, user_id: UserId) -> Result<Vec<RefreshRevocationRow>>;

    /// Insert (or update) the blanket-revoke sentinel for a user. The sentinel
    /// key is `BLANKET:<user_id>` and it is stored in the same
    /// `auth_revoked_refresh_tokens` table as individual JTIs. When the refresh
    /// flow detects a previously-rotated token being replayed (a token-theft
    /// signal), it calls this to revoke *all* outstanding refresh tokens for the
    /// user in one write — even tokens whose JTIs were never individually
    /// recorded. The `list_refresh_revocations` response then contains this
    /// sentinel; the refresh flow checks for it before accepting any token.
    async fn upsert_blanket_revocation(
        &self,
        blanket_key: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<()>;

    // --- API keys -----------------------------------------------------------

    /// Insert a new API key row. Returns `(id, created_at)` on success, or
    /// `CommitConflict` if `(project, user, name)` is already taken.
    async fn insert_api_key(
        &self,
        project: &ProjectId,
        user_id: UserId,
        name: &str,
        key_hash: &str,
        key_bcrypt: &str,
    ) -> Result<(i64, DateTime<Utc>)>;

    /// Look up API key rows by sha256 hash. May return 0, 1, or (very
    /// rarely) more rows — bcrypt verify in the flow code picks the match.
    async fn find_api_keys_by_hash(&self, key_hash: &str) -> Result<Vec<ApiKeyRow>>;

    /// Best-effort `last_used_at = now()` bump.
    async fn touch_api_key(&self, id: i64) -> Result<()>;

    /// Soft-delete: set `revoked_at = now()` where id + project match.
    /// Returns `NotFound` if the key doesn't belong to the project.
    async fn revoke_api_key(&self, project: &ProjectId, key_id: i64) -> Result<()>;

    /// List a user's API keys within a project. Ordered by id ascending.
    async fn list_api_keys(
        &self,
        project: &ProjectId,
        user_id: UserId,
    ) -> Result<Vec<crate::api_keys::ApiKeyDescriptor>>;

    // --- Session settings ---------------------------------------------------

    /// Upsert a single session-setting row.
    async fn upsert_session_setting(
        &self,
        project: &ProjectId,
        user_id: UserId,
        key: &str,
        value: &str,
    ) -> Result<()>;

    /// Return all session-setting rows for `(project, user)`.
    async fn list_session_settings(
        &self,
        project: &ProjectId,
        user_id: UserId,
    ) -> Result<HashMap<String, String>>;

    // --- Project credentials (pgwire) ----------------------------------------

    /// Insert a new credential row. Returns `true` if the row was inserted
    /// (i.e. `pgwire_user` was unique), `false` on conflict (so the caller
    /// can retry with a fresh username).
    async fn insert_project_credential(
        &self,
        project: &ProjectId,
        pgwire_user: &str,
        password_hash: &str,
        dbname: &str,
    ) -> Result<bool>;

    /// Look up a credential row by `pgwire_user`.
    async fn find_project_credential(
        &self,
        pgwire_user: &str,
    ) -> Result<Option<ProjectCredentialRow>>;

    /// Rotate the password for an existing credential row. Returns the
    /// updated `(project_id, dbname)` or `None` if not found.
    async fn rotate_project_credential(
        &self,
        pgwire_user: &str,
        password_hash: &str,
    ) -> Result<Option<ProjectCredentialRow>>;

    /// List every credential row belonging to `project`.
    async fn list_project_credentials(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<crate::project_credentials::ProjectCredentialDescriptor>>;

    /// Return all credential rows across all projects whose `pgwire_user`
    /// is in the old `project_<hex>` format (i.e. starts with `"project_"`).
    /// Used by the upgrade migration to find credentials that need rotation.
    ///
    /// Returns `(project_id, pgwire_user)` pairs.
    async fn list_legacy_project_credentials(&self) -> Result<Vec<(ProjectId, String)>>;

    /// Delete a single credential row by `pgwire_user`. Called after a
    /// successful migration INSERT to clean up the old-format row.
    /// Returns `NotFound` when the row is already gone (idempotent).
    async fn delete_project_credential(&self, pgwire_user: &str) -> Result<()>;

    // --- Auth magic-links (project-agnostic email-link flow) -----------------

    /// Insert a new auth_magic_links row (the project-agnostic email-link
    /// flow, not the per-project `email_tokens` magic-link).
    async fn insert_auth_magic_link(
        &self,
        email: &str,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Scan unconsumed, unexpired auth_magic_links rows and return all of
    /// them for bcrypt verification in the flow layer.
    async fn list_active_auth_magic_links(&self) -> Result<Vec<AuthMagicLinkRow>>;

    /// Single-use atomic consume for the project-agnostic magic-link row.
    /// Sets `consumed_at = now()` where `id = $1 AND consumed_at IS NULL`.
    /// Returns the row count; `0` means already consumed (duplicate token use).
    async fn consume_auth_magic_link(&self, id: i64) -> Result<u64>;

    /// Mark `email_verified_at = COALESCE(email_verified_at, now())` for
    /// a user by id (no project filter — used in the project-agnostic path).
    async fn mark_email_verified_by_user_id(&self, user_id: UserId) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Supplementary row types
// ---------------------------------------------------------------------------

/// Row returned by `find_email_token` / `find_magic_link_email_token`.
#[derive(Debug, Clone)]
pub struct EmailTokenRow {
    pub user_id: UserId,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub purpose: String,
}

/// Extended row returned by `find_magic_link_email_token` (includes the
/// user's email from the joined `users` table).
#[derive(Debug, Clone)]
pub struct MagicLinkEmailTokenRow {
    pub user_id: UserId,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub purpose: String,
    pub email: String,
}

/// A row from `auth_revoked_refresh_tokens`.
#[derive(Debug, Clone)]
pub struct RefreshRevocationRow {
    pub token_hash: String,
    pub revoked_at: DateTime<Utc>,
}

/// A row from `auth_magic_links` (the project-agnostic flow table).
#[derive(Debug, Clone)]
pub struct AuthMagicLinkRow {
    pub id: i64,
    pub email: String,
    pub token_hash: String,
}
