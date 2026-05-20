//! `basin-auth` — opinionated auth crate for Basin.
//!
//! See [ADR 0005](../../../docs/decisions/0005-auth-system.md) for the full
//! design + rationale. The short version: per-project `auth.users` rows,
//! email/password (bcrypt), JWT (HS256), refresh tokens, magic link,
//! password reset, email verification.
//!
//! **SMTP credentials are required at startup** — a missing
//! `BASIN_AUTH_SMTP_*` is a fatal error, not a warning.
//!
//! ## What's in v1
//!
//! - `signup(project, email, password)`
//! - `signin(project, email, password) -> Tokens`
//! - `verify_email(project, token)`
//! - `request_password_reset(project, email)` (sends email)
//! - `reset_password(project, token, new_password)`
//! - `request_magic_link(project, email)` (sends email)
//! - `signin_with_magic_link(project, token) -> Tokens`
//! - `verify_jwt(token) -> Claims` (used by `basin-router` and `basin-rest`)
//! - `refresh(refresh_token) -> Tokens`
//! - `signout(refresh_token)`
//!
//! ## Cross-crate trait integration
//!
//! `basin_router::ProjectResolver` lives in the `basin-router` crate. Rust's
//! orphan rule prevents us from impl'ing it for `AuthService` from here, so
//! this crate exposes [`AuthService::verify_jwt`] and the
//! [`jwt_project_resolver`] helper. The basin-router integration PR will wrap
//! one of these into its own `ProjectResolver` impl on its side.

#![forbid(unsafe_code)]

pub mod api_keys;
pub mod config;
pub mod oauth;
pub mod project_credentials;
pub mod store;

pub use project_credentials::{is_legacy_pgwire_user, ConnectionInfo, ProjectCredentialDescriptor};
pub use store::AuthStore;
pub mod email;
pub mod jwt;
pub mod password;
pub mod rate_limit;
pub mod schema;
pub mod session_settings;
pub mod tokens;

pub mod flows {
    pub mod email_link;
    pub mod magic;
    pub mod refresh;
    pub mod reset;
    pub mod signin;
    pub mod signup;
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, Result};
use chrono::{DateTime, Utc};
use rustls::ClientConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::instrument;
use uuid::Uuid;

pub use crate::api_keys::{ApiKeyDescriptor, ApiKeySecret, IssuedApiKey};
pub use crate::config::{
    AuthConfig, SmtpConfig, SmtpTls, DEFAULT_LOOPBACK_CATALOG_DSN, INTERNAL_AUTH_PROJECT_ID,
    INTERNAL_AUTH_USERNAME,
};
pub use crate::email::{Mailer, Outbound, SmtpMailer, StubMailer};
pub use crate::jwt::Claims;

/// True iff the DSN is a `postgres://` URL carrying `sslmode=disable`. In
/// that case the loopback catalog path uses `NoTls` directly — building the
/// rustls connector would be wasted work and would also drag in the
/// process-wide CryptoProvider install for no reason.
///
/// The check is intentionally permissive: any `sslmode=disable` (with or
/// without surrounding `&`/`?`) qualifies. Anything else falls back to the
/// rustls connector so external managed-PG DSNs keep working.
fn is_loopback_disable_tls(dsn: &str) -> bool {
    if !(dsn.starts_with("postgres://") || dsn.starts_with("postgresql://")) {
        return false;
    }
    // Parse the query string portion. Don't pull in `url` just for this; do a
    // small bespoke walk that catches the two shapes we care about.
    let Some(query_start) = dsn.find('?') else {
        return false;
    };
    let query = &dsn[query_start + 1..];
    query
        .split('&')
        .any(|kv| kv.eq_ignore_ascii_case("sslmode=disable"))
}

/// Idempotently installs aws-lc-rs as the process-wide rustls CryptoProvider.
/// rustls 0.23 refuses to auto-pick when both `aws-lc-rs` and `ring` are
/// enabled (which is our workspace state — pgwire/lettre/etc pull `ring`,
/// basin-router pulls `aws-lc-rs`). Mirrors the same helper basin-router uses
/// for its server-side TLS bootstrap so the choice is consistent.
fn ensure_default_crypto_provider() {
    use rustls::crypto::{aws_lc_rs, CryptoProvider};
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Ignore the result: another crate (basin-router, pgwire's secure
        // example path) may have installed a provider already — that's fine.
        let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    });
}

/// Per-user identifier. UUID v4 for now; could move to ULID later, but the
/// JWT consumers all parse strings so the wire format is what matters.
pub type UserId = Uuid;

/// Tokens returned from sign-in / refresh.
#[derive(Debug, Clone)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
    pub access_expires_at: DateTime<Utc>,
    pub refresh_expires_at: DateTime<Utc>,
}

/// Shared inner state. Cheap to wrap in `Arc` and clone.
pub(crate) struct Inner {
    pub(crate) cfg: AuthConfig,
    pub(crate) store: Arc<dyn AuthStore>,
    pub(crate) jwt: jwt::JwtKeys,
    pub(crate) mailer: Arc<dyn Mailer>,
    pub(crate) ip_limiter: rate_limit::PerKey,
    pub(crate) email_limiter: rate_limit::PerKey,
    /// In-memory state cache used by the OAuth flow when the concrete store
    /// does not implement `OAuthStore` (e.g. `EngineAuthStore` in integration
    /// tests). For Postgres-backed stores the DB is used directly instead.
    pub(crate) oauth_state_cache: Arc<oauth::OAuthStateCache>,
}

/// Top-level auth handle. `Clone` is cheap; share across the engine, router,
/// REST layer, etc.
#[derive(Clone)]
pub struct AuthService {
    inner: Arc<Inner>,
}

impl AuthService {
    /// External Postgres path: connects to the DSN in `BASIN_AUTH_CATALOG_DSN`
    /// (or `cfg.catalog_dsn`), spawns the `tokio_postgres` driver task, and
    /// runs migrations before returning. Use this when deploying with a
    /// dedicated Postgres instance for auth state; for the default in-process
    /// path use [`AuthService::with_store`] with an `EngineAuthStore`.
    pub async fn connect(cfg: AuthConfig) -> Result<Self> {
        let mailer: Arc<dyn Mailer> = Arc::new(SmtpMailer::from_config(&cfg.smtp)?);
        Self::connect_with_mailer(cfg, mailer).await
    }

    /// Connect with a caller-supplied mailer. Used by tests with `StubMailer`
    /// so we don't actually send mail; could also be used by an operator
    /// wanting to wrap SMTP with extra logging.
    pub async fn connect_with_mailer(cfg: AuthConfig, mailer: Arc<dyn Mailer>) -> Result<Self> {
        schema::validate_schema_ident(&cfg.catalog_schema)?;
        let dsn = cfg.effective_dsn();

        // Loopback path: when basin-auth's catalog lives inside basin engine's
        // own pgwire on 127.0.0.1, the connection is plaintext (engine accepts
        // unencrypted on loopback) and there's no point spinning up rustls.
        // Detect by URL shape: `postgres://...sslmode=disable` short-circuits
        // to `NoTls` — saves an unnecessary handshake. External Postgres still
        // goes through the rustls path so managed PG (Neon, RDS, Supabase)
        // keeps working when an operator opts back in via env override.
        let client = if is_loopback_disable_tls(&dsn) {
            let (client, connection) =
                tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| BasinError::catalog(format!("auth pg connect: {e}")))?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "basin-auth postgres driver exited");
                }
            });
            client
        } else {
            // rustls 0.23 requires *some* CryptoProvider be installed
            // process-wide before `ClientConfig::builder()` is reached; both
            // `aws-lc-rs` and `ring` are pulled in transitively by the
            // workspace, so it can't auto-pick. basin-router installs
            // aws-lc-rs at startup the same way; we mirror that here for the
            // connect path that runs first when the auth service is the entry
            // point. `install_default` is a no-op if a provider is already
            // installed, so this stays safe under multi-init.
            ensure_default_crypto_provider();
            // Trust the Mozilla CA bundle from webpki-roots so this works
            // against managed Postgres (Neon, RDS, Supabase, Crunchy) without
            // depending on the container's system trust store.
            // `MakeRustlsConnect` only engages when the server answers `S` to
            // the SSLRequest, so plaintext-only Postgres (e.g. Fly intra-VPC
            // PG) keeps working through this path.
            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls_config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls = MakeRustlsConnect::new(tls_config);
            let (client, connection) = tokio_postgres::connect(&dsn, tls)
                .await
                .map_err(|e| BasinError::catalog(format!("auth pg connect: {e}")))?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "basin-auth postgres driver exited");
                }
            });
            client
        };
        let store: Arc<dyn AuthStore> = Arc::new(store::postgres::PostgresAuthStore::new(
            client,
            cfg.catalog_schema.clone(),
        ));
        Self::with_store(cfg, store, mailer).await
    }

    /// Construct an `AuthService` using a caller-supplied [`AuthStore`]. This
    /// is the in-process default path used by `basin-server`: it injects an
    /// `EngineAuthStore` that routes all auth SQL through the Basin engine
    /// directly — no `BASIN_AUTH_CATALOG_DSN` or TCP connection required.
    /// Runs migrations against the store before returning.
    pub async fn with_store(
        cfg: AuthConfig,
        store: Arc<dyn AuthStore>,
        mailer: Arc<dyn Mailer>,
    ) -> Result<Self> {
        schema::validate_schema_ident(&cfg.catalog_schema)?;
        let jwt = jwt::JwtKeys::new(&cfg.jwt_secret);
        let ip_limiter = rate_limit::PerKey::per_minute(cfg.rate_limit_per_ip_per_min, "ip");
        let email_limiter = rate_limit::PerKey::per_minute(cfg.rate_limit_per_ip_per_min, "email");
        let svc = Self {
            inner: Arc::new(Inner {
                cfg,
                store,
                jwt,
                mailer,
                ip_limiter,
                email_limiter,
                oauth_state_cache: Arc::new(oauth::OAuthStateCache::new()),
            }),
        };
        svc.migrate().await?;
        Ok(svc)
    }

    /// Run the idempotent `CREATE TABLE IF NOT EXISTS` migration. Safe to
    /// call repeatedly; first call is the only one that does work.
    pub async fn migrate(&self) -> Result<()> {
        self.inner
            .store
            .migrate(&self.inner.cfg.catalog_schema)
            .await
    }

    // --- public flows -------------------------------------------------------

    #[instrument(skip(self, password), fields(project = %project, email = %email))]
    pub async fn signup(&self, project: &ProjectId, email: &str, password: &str) -> Result<UserId> {
        flows::signup::signup(&self.inner, project, email, password).await
    }

    #[instrument(skip(self), fields(project = %project, user_id = %user))]
    pub async fn request_email_verification(
        &self,
        project: &ProjectId,
        user: UserId,
    ) -> Result<()> {
        flows::signup::request_email_verification(&self.inner, project, user).await
    }

    #[instrument(skip(self, token), fields(project = %project))]
    pub async fn verify_email(&self, project: &ProjectId, token: &str) -> Result<()> {
        flows::signup::verify_email(&self.inner, project, token).await
    }

    #[instrument(skip(self, password), fields(project = %project, email = %email))]
    pub async fn signin(&self, project: &ProjectId, email: &str, password: &str) -> Result<Tokens> {
        flows::signin::signin(&self.inner, project, email, password).await
    }

    #[instrument(skip(self, refresh_token))]
    pub async fn refresh(&self, refresh_token: &str) -> Result<Tokens> {
        flows::refresh::refresh(&self.inner, refresh_token).await
    }

    #[instrument(skip(self, refresh_token))]
    pub async fn signout(&self, refresh_token: &str) -> Result<()> {
        flows::refresh::signout(&self.inner, refresh_token).await
    }

    #[instrument(skip(self), fields(project = %project, email = %email))]
    pub async fn request_password_reset(&self, project: &ProjectId, email: &str) -> Result<()> {
        flows::reset::request_password_reset(&self.inner, project, email).await
    }

    #[instrument(skip(self, token, new_password), fields(project = %project))]
    pub async fn reset_password(
        &self,
        project: &ProjectId,
        token: &str,
        new_password: &str,
    ) -> Result<()> {
        flows::reset::reset_password(&self.inner, project, token, new_password).await
    }

    #[instrument(skip(self), fields(project = %project, email = %email))]
    pub async fn request_magic_link(&self, project: &ProjectId, email: &str) -> Result<()> {
        flows::magic::request_magic_link(&self.inner, project, email).await
    }

    /// True iff outbound mail is wired up. Routes that depend on email
    /// (the new project-agnostic `/auth/v1/magic-link`) check this and
    /// return 503 with `E_EMAIL_DISABLED` instead of attempting a doomed
    /// SMTP send.
    pub fn is_email_enabled(&self) -> bool {
        self.inner.cfg.is_email_enabled()
    }

    /// Project-agnostic email-link login (request). Body is just an email;
    /// the user is resolved at consume time. Always returns Ok on a
    /// well-formed email — a missing user is silently dropped to defeat
    /// enumeration probes.
    #[instrument(skip(self), fields(email = %email))]
    pub async fn request_email_link(&self, email: &str) -> Result<()> {
        flows::email_link::request(&self.inner, email).await
    }

    /// Project-agnostic email-link login (consume). Single-use.
    #[instrument(skip(self, token))]
    pub async fn consume_email_link(&self, token: &str) -> Result<Tokens> {
        flows::email_link::consume(&self.inner, token).await
    }

    #[instrument(skip(self, token), fields(project = %project))]
    pub async fn signin_with_magic_link(&self, project: &ProjectId, token: &str) -> Result<Tokens> {
        flows::magic::signin_with_magic_link(&self.inner, project, token).await
    }

    /// Decode and verify a JWT issued by this service. Cheap — no DB lookup.
    pub fn verify_jwt(&self, jwt: &str) -> Result<Claims> {
        self.inner.jwt.verify(jwt)
    }

    // --- API keys -----------------------------------------------------------

    /// Mint a long-lived API key. The plaintext secret is returned exactly
    /// once — store it client-side or hand it to the user immediately.
    #[instrument(skip(self), fields(project = %project, user_id = %user_id))]
    pub async fn issue_api_key(
        &self,
        user_id: UserId,
        project: &ProjectId,
        name: &str,
    ) -> Result<IssuedApiKey> {
        api_keys::issue(&self.inner, project, user_id, name).await
    }

    /// Look up an API key by its plaintext secret and return the owning
    /// `(project, user)`. Bumps `last_used_at` on success.
    #[instrument(skip(self, raw))]
    pub async fn validate_api_key(&self, raw: &str) -> Result<(ProjectId, UserId)> {
        api_keys::validate(&self.inner, raw).await
    }

    /// Revoke an API key by id within a project. NotFound if the key doesn't
    /// belong to `project`. Idempotent if already revoked.
    #[instrument(skip(self), fields(project = %project, key_id))]
    pub async fn revoke_api_key(&self, key_id: i64, project: &ProjectId) -> Result<()> {
        api_keys::revoke(&self.inner, key_id, project).await
    }

    /// List a user's API keys. Never returns the secret.
    #[instrument(skip(self), fields(project = %project, user_id = %user_id))]
    pub async fn list_api_keys(
        &self,
        user_id: UserId,
        project: &ProjectId,
    ) -> Result<Vec<ApiKeyDescriptor>> {
        api_keys::list(&self.inner, project, user_id).await
    }

    // --- per-project pgwire credentials -------------------------------------

    /// Provision a fresh `(pgwire_user, password)` pair for a project.
    /// Returns the connection URL; the plaintext password is part of this
    /// response and **not stored anywhere**.
    #[instrument(skip(self), fields(project = %project))]
    pub async fn provision_project_db(
        &self,
        project: &ProjectId,
        dbname: Option<&str>,
    ) -> Result<project_credentials::ConnectionInfo> {
        project_credentials::provision(&self.inner, project, dbname).await
    }

    /// Validate a `(pgwire_user, password)` pair from a pgwire startup
    /// handshake. Returns the resolved `ProjectId` on success;
    /// `BasinError::InvalidIdent("invalid pgwire credentials")` on any
    /// failure (uniform — no user-existence leak).
    #[instrument(skip(self, password), fields(user))]
    pub async fn validate_pgwire_credentials(
        &self,
        user: &str,
        password: &str,
    ) -> Result<ProjectId> {
        project_credentials::validate(&self.inner, user, password).await
    }

    /// Rotate the password for an existing pgwire credential row. Returns
    /// the new connection URL (with the new plaintext password). Old
    /// password validates as `28P01` after this call.
    #[instrument(skip(self), fields(pgwire_user))]
    pub async fn rotate_pgwire_password(
        &self,
        pgwire_user: &str,
    ) -> Result<project_credentials::ConnectionInfo> {
        project_credentials::rotate(&self.inner, pgwire_user).await
    }

    /// Public-facing descriptors for every credential row a project owns.
    /// Never includes the bcrypt hash or the plaintext password.
    #[instrument(skip(self), fields(project = %project))]
    pub async fn list_project_credentials(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<project_credentials::ProjectCredentialDescriptor>> {
        project_credentials::list(&self.inner, project).await
    }

    /// Returns all credentials across all projects that are in the legacy
    /// `project_<hex>` format. Used by the upgrade migration to discover rows
    /// that need to be rotated to the new `{project_id}_{hex}` format.
    pub async fn list_legacy_credentials(&self) -> Result<Vec<(ProjectId, String)>> {
        project_credentials::list_legacy(&self.inner).await
    }

    /// Rotates a single credential from the legacy `project_<hex>` format to
    /// the new `{project_id}_{hex}` format. Inserts the new credential row
    /// first, then deletes the old row — safe to retry if interrupted.
    ///
    /// Returns `(new_pgwire_user, plaintext_password)`. The caller is
    /// responsible for propagating the new credential to any downstream store
    /// (e.g. basin-cloud's `project_pgwire_credentials` table).
    pub async fn migrate_legacy_credential(
        &self,
        project: &ProjectId,
        old_pgwire_user: &str,
    ) -> Result<(String, String)> {
        project_credentials::migrate_legacy_credential(&self.inner, project, old_pgwire_user).await
    }

    /// Startup migration: scans for all `project_<hex>`-format pgwire credentials
    /// (pre-ADR-0013) and rotates each one to the new `{ulid}_{hex}` format that
    /// encodes the project ULID for self-routing. Per-credential failures are
    /// logged but do not abort the batch, so partial progress is persisted and
    /// the next startup attempt resumes from where this one left off. Returns
    /// the count of successfully migrated rows.
    pub async fn migrate_legacy_credentials(&self) -> Result<u64> {
        let legacy = self.list_legacy_credentials().await?;
        let total = legacy.len();
        if total == 0 {
            return Ok(0);
        }
        tracing::info!(
            count = total,
            "legacy pgwire credentials found; rotating to new format"
        );
        let mut migrated: u64 = 0;
        for (project, old_user) in legacy {
            match self.migrate_legacy_credential(&project, &old_user).await {
                Ok((new_user, _plaintext)) => {
                    tracing::info!(
                        project = %project,
                        old_pgwire_user = %old_user,
                        new_pgwire_user = %new_user,
                        "migrated legacy pgwire credential"
                    );
                    migrated += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        project = %project,
                        old_pgwire_user = %old_user,
                        error = %e,
                        "failed to migrate legacy pgwire credential; will retry on next startup"
                    );
                }
            }
        }
        Ok(migrated)
    }

    // --- OAuth (Phase 5.10.O) -----------------------------------------------

    /// Start an OAuth authorization flow. Returns the redirect URL (302 to
    /// the provider) and the raw state value. Stores the in-flight
    /// `(state_hash, pkce_verifier)` row in the DB or in the in-memory cache.
    ///
    /// Pass `oauth_store = None` when using the in-process `EngineAuthStore`
    /// (e.g. integration tests). Pass `Some(&postgres_store)` for the
    /// external-Postgres path.
    pub async fn begin_oauth_authorize<S>(
        &self,
        oauth_store: Option<&S>,
        project_id: &ProjectId,
        provider: &str,
        redirect_to: &str,
    ) -> Result<oauth::AuthorizeRedirect>
    where
        S: oauth::OAuthStore,
    {
        oauth::begin_authorize(&self.inner, oauth_store, project_id, provider, redirect_to)
            .await
    }

    /// Handle an OAuth callback. Returns tokens + redirect_to.
    pub async fn handle_oauth_callback<S>(
        &self,
        enc: &dyn oauth::EncryptionProvider,
        oauth_store: Option<&S>,
        provider_hint: &str,
        code: &str,
        state_val: &str,
        redirect_callback_base: &str,
    ) -> Result<oauth::OAuthCallbackResult>
    where
        S: oauth::OAuthStore,
    {
        oauth::handle_callback(
            &self.inner,
            enc,
            oauth_store,
            provider_hint,
            code,
            state_val,
            redirect_callback_base,
        )
        .await
    }

    /// Register a mock OAuth provider in the in-memory cache. Only available
    /// in test builds (or when the `test-utils` feature is enabled). Used by
    /// integration tests that drive the OAuth flow without a real DB.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn register_mock_oauth_provider(
        &self,
        project_id: &ProjectId,
        provider: &str,
        client_id: &str,
        client_secret: &str,
        authorize_url: &str,
        token_url: &str,
        userinfo_url: &str,
        scopes: &str,
        redirect_uri: &str,
    ) {
        self.inner.oauth_state_cache.register_mock_provider(
            project_id,
            provider,
            client_id,
            client_secret,
            authorize_url,
            token_url,
            userinfo_url,
            scopes,
            redirect_uri,
        );
    }

    // --- session settings ---------------------------------------------------

    /// Upsert a per-user session setting. `key` must be in
    /// [`session_settings::ALLOWED_KEYS`]; the value is validated per-key.
    #[instrument(skip(self, value), fields(project = %project, user_id = %user_id, key))]
    pub async fn set_session_setting(
        &self,
        user_id: UserId,
        project: &ProjectId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        session_settings::set(&self.inner, project, user_id, key, value).await
    }

    /// Read every session setting for a user. Empty map means "no
    /// overrides; engine defaults apply".
    #[instrument(skip(self), fields(project = %project, user_id = %user_id))]
    pub async fn get_session_settings(
        &self,
        user_id: UserId,
        project: &ProjectId,
    ) -> Result<HashMap<String, String>> {
        session_settings::get_all(&self.inner, project, user_id).await
    }
}

/// Helper that bundles `AuthService` into a closure usable by
/// `basin-router::ProjectResolver`-shaped code.
///
/// `basin-router` defines an async `ProjectResolver` trait we can't impl
/// from this side of the orphan rule. The follow-up integration PR will
/// either:
///
/// 1. Add a `JwtProjectResolver(Arc<AuthService>)` newtype inside
///    `basin-router` and impl `ProjectResolver` for it, calling
///    `svc.verify_jwt(...)` and returning `claims.project_id`; or
/// 2. Generalise `ProjectResolver` to take a closure, in which case this
///    helper plugs in directly.
///
/// Either way, the cross-crate seam is `AuthService::verify_jwt`.
pub fn jwt_project_resolver(svc: AuthService) -> impl Fn(&str) -> Result<ProjectId> + Send + Sync {
    move |jwt: &str| -> Result<ProjectId> { svc.verify_jwt(jwt).map(|c| c.project_id) }
}

/// Marker async trait the integration PR can implement on its side. Kept
/// here for documentation; not used by the router yet.
#[async_trait]
pub trait JwtVerifier: Send + Sync {
    async fn verify(&self, jwt: &str) -> Result<Claims>;
}

#[async_trait]
impl JwtVerifier for AuthService {
    async fn verify(&self, jwt: &str) -> Result<Claims> {
        self.verify_jwt(jwt)
    }
}

// --- shared internal helpers --------------------------------------------------

/// Newtype wrapper around the email validation step. Keeps the spelling
/// consistent across flows.
pub(crate) fn normalise_email(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(BasinError::InvalidIdent("email is empty".into()));
    }
    if !trimmed.contains('@') {
        return Err(BasinError::InvalidIdent(format!(
            "email {trimmed:?} missing '@'"
        )));
    }
    if trimmed.len() > 320 {
        return Err(BasinError::InvalidIdent(
            "email longer than 320 chars".into(),
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

pub(crate) fn ttl_or_default(d: Duration) -> chrono::Duration {
    chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::seconds(60))
}

#[cfg(test)]
mod tests {
    //! End-to-end tests against the live Postgres on `127.0.0.1:5432`. Each
    //! test allocates a unique `basin_auth_test_<ulid>` schema with a
    //! `Drop`-guard cleanup so concurrent tests don't collide and a panic
    //! mid-test only leaks one schema.
    //!
    //! If Postgres is unreachable, every test prints a one-line skip and
    //! returns `Ok` — the suite must remain runnable in environments without
    //! local Postgres.

    use std::sync::Arc;
    use std::time::Duration;

    use basin_common::ProjectId;
    use tokio_postgres::NoTls;
    use ulid::Ulid;

    use super::*;

    const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

    fn unique_schema() -> String {
        format!("basin_auth_test_{}", Ulid::new().to_string().to_lowercase())
    }

    /// Drop-guard. Mirrors the pattern in `basin-catalog::postgres`.
    struct SchemaGuard {
        schema: String,
    }

    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            let schema = self.schema.clone();
            let _ = std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("basin-auth schema cleanup runtime: {e}");
                        return;
                    }
                };
                rt.block_on(async {
                    let connect = tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio_postgres::connect(PG_URL, NoTls),
                    )
                    .await;
                    let (client, conn) = match connect {
                        Ok(Ok(pair)) => pair,
                        _ => return,
                    };
                    let driver = tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    let _ = client
                        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                        .await;
                    drop(client);
                    let _ = tokio::time::timeout(Duration::from_millis(200), driver).await;
                });
            })
            .join();
        }
    }

    fn base_cfg(schema: &str) -> AuthConfig {
        AuthConfig {
            jwt_secret: vec![9u8; 32],
            token_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(86_400),
            catalog_dsn: Some(PG_URL.to_owned()),
            catalog_schema: schema.to_owned(),
            smtp: SmtpConfig {
                host: "smtp.invalid".into(),
                port: 587,
                username: "u".into(),
                password: "p".into(),
                from_email: "noreply@example.com".into(),
                from_name: Some("Basin".into()),
                tls: SmtpTls::StartTls,
            },
            // Low cost so signup is fast in tests; production cfg is 12.
            bcrypt_cost: 4,
            password_min_len: 10,
            // High enough to never trigger inside a single test run.
            rate_limit_per_ip_per_min: 1000,
            email_enabled: true,
            pgwire_public_host: "127.0.0.1:5433".into(),
        }
    }

    /// Returns `(svc, mailer, guard)` or None if Postgres is unreachable.
    async fn try_connect() -> Option<(AuthService, StubMailer, SchemaGuard)> {
        let schema = unique_schema();
        let cfg = base_cfg(&schema);
        let mailer = StubMailer::new(cfg.smtp.from_email.clone());
        let svc = match tokio::time::timeout(
            Duration::from_secs(2),
            AuthService::connect_with_mailer(cfg, Arc::new(mailer.clone())),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                eprintln!("postgres unreachable, skipping basin-auth test: {e}");
                return None;
            }
            Err(_) => {
                eprintln!("postgres connect timed out, skipping basin-auth test");
                return None;
            }
        };
        Some((svc, mailer, SchemaGuard { schema }))
    }

    fn last_token(mailer: &StubMailer) -> String {
        let log = mailer.sent();
        let body = &log.last().expect("at least one email sent").body;
        // Templates are `...?token=<HEX>\n...`; pull the hex out.
        let needle = "token=";
        let start = body.find(needle).expect("body has token=") + needle.len();
        let tail = &body[start..];
        let end = tail
            .find(|c: char| !c.is_ascii_hexdigit())
            .unwrap_or(tail.len());
        tail[..end].to_owned()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signup_creates_user() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "alice@example.com", "longenoughpassword")
            .await
            .unwrap();

        // Spot check: row exists, hash isn't the plaintext.
        let user_row = svc
            .inner
            .store
            .find_user_by_id(&t, user)
            .await
            .unwrap()
            .expect("user must exist");
        assert_eq!(user_row.email, "alice@example.com");
        assert_ne!(user_row.password_hash, "longenoughpassword");
        assert!(user_row.password_hash.starts_with("$2"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signup_rejects_short_password() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let err = svc
            .signup(&t, "alice@example.com", "short")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidIdent(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signup_rejects_duplicate_email() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        svc.signup(&t, "dup@example.com", "longenoughpassword")
            .await
            .unwrap();
        let err = svc
            .signup(&t, "dup@example.com", "longenoughpassword")
            .await
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("exists")
                || err.to_string().to_lowercase().contains("duplicate"),
            "expected duplicate error, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signup_allows_same_email_different_projects() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let a = ProjectId::new();
        let b = ProjectId::new();
        svc.signup(&a, "shared@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.signup(&b, "shared@example.com", "longenoughpassword")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signin_rejects_before_email_verification() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        svc.signup(&t, "u@example.com", "longenoughpassword")
            .await
            .unwrap();
        let err = svc
            .signin(&t, "u@example.com", "longenoughpassword")
            .await
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("verif"),
            "expected verification error, got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_email_then_signin_succeeds() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "u2@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        let token = last_token(&mailer);
        svc.verify_email(&t, &token).await.unwrap();
        let toks = svc
            .signin(&t, "u2@example.com", "longenoughpassword")
            .await
            .unwrap();
        let claims = svc.verify_jwt(&toks.access_token).unwrap();
        assert_eq!(claims.project_id, t);
        assert_eq!(claims.user_id, user);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signin_rejects_wrong_password() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "wp@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        let token = last_token(&mailer);
        svc.verify_email(&t, &token).await.unwrap();
        let err = svc
            .signin(&t, "wp@example.com", "wrongpasswordlong")
            .await
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_rotates_and_revokes_old() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "rf@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        svc.verify_email(&t, &last_token(&mailer)).await.unwrap();

        let toks = svc
            .signin(&t, "rf@example.com", "longenoughpassword")
            .await
            .unwrap();
        let new = svc.refresh(&toks.refresh_token).await.unwrap();
        assert_ne!(new.refresh_token, toks.refresh_token);

        // Old refresh must now fail.
        let err = svc.refresh(&toks.refresh_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("invalid")
                || err.to_string().to_lowercase().contains("revoked")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signout_revokes_refresh() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "so@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        svc.verify_email(&t, &last_token(&mailer)).await.unwrap();
        let toks = svc
            .signin(&t, "so@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.signout(&toks.refresh_token).await.unwrap();
        assert!(svc.refresh(&toks.refresh_token).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn password_reset_token_one_time_use() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "pr@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        svc.verify_email(&t, &last_token(&mailer)).await.unwrap();

        svc.request_password_reset(&t, "pr@example.com")
            .await
            .unwrap();
        let token = last_token(&mailer);
        svc.reset_password(&t, &token, "newlongpassword!")
            .await
            .unwrap();
        let err = svc
            .reset_password(&t, &token, "anothernewpassword")
            .await
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("invalid")
                || err.to_string().to_lowercase().contains("consumed")
                || err.to_string().to_lowercase().contains("expired")
        );

        // New password works.
        svc.signin(&t, "pr@example.com", "newlongpassword!")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn magic_link_one_time_use() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "ml@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        svc.verify_email(&t, &last_token(&mailer)).await.unwrap();

        svc.request_magic_link(&t, "ml@example.com").await.unwrap();
        let token = last_token(&mailer);
        let _toks = svc.signin_with_magic_link(&t, &token).await.unwrap();
        assert!(svc.signin_with_magic_link(&t, &token).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_token_rejected() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = svc
            .signup(&t, "ex@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        let token = last_token(&mailer);

        // Fake an expired email token by stamping `expires_at` in the past.
        // We need a direct Postgres connection for this raw UPDATE since the
        // AuthStore trait doesn't expose a "backdate token" method (by design
        // — no production code should need it).
        {
            let (raw_client, conn) = tokio_postgres::connect(PG_URL, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let schema = &svc.inner.cfg.catalog_schema;
            raw_client
                .execute(
                    &format!(
                        "UPDATE {schema}_email_tokens SET expires_at = now() - INTERVAL '1 hour'
                         WHERE token_hash = $1"
                    ),
                    &[&tokens::hash_token(&token)],
                )
                .await
                .unwrap();
        }

        let err = svc.verify_email(&t, &token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("expired")
                || err.to_string().to_lowercase().contains("invalid")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_round_trip() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let now = chrono::Utc::now();
        let (jwt, exp) = svc
            .inner
            .jwt
            .issue(
                &t,
                Uuid::new_v4(),
                "j@example.com",
                &["user".to_string()],
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let c = svc.verify_jwt(&jwt).unwrap();
        assert_eq!(c.project_id, t);
        assert_eq!(c.exp, exp.timestamp());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_jwt_rejects_tampered_signature() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let now = chrono::Utc::now();
        let (jwt, _) = svc
            .inner
            .jwt
            .issue(
                &ProjectId::new(),
                Uuid::new_v4(),
                "j@example.com",
                &[],
                now,
                Duration::from_secs(60),
            )
            .unwrap();
        let mut bytes = jwt.into_bytes();
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();
        assert!(svc.verify_jwt(&tampered).is_err());
    }

    // --- API key tests ------------------------------------------------------

    /// Helper: signup, verify email, and return the verified user_id.
    async fn make_verified_user(
        svc: &AuthService,
        mailer: &StubMailer,
        project: &ProjectId,
        email: &str,
    ) -> UserId {
        let user = svc
            .signup(project, email, "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(project, user).await.unwrap();
        let token = last_token(mailer);
        svc.verify_email(project, &token).await.unwrap();
        user
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_round_trip() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "ak@example.com").await;
        let issued = svc.issue_api_key(user, &t, "ci pipeline").await.unwrap();
        assert!(!issued.secret.is_empty());
        assert_eq!(issued.name, "ci pipeline");

        let (got_t, got_u) = svc.validate_api_key(&issued.secret).await.unwrap();
        assert_eq!(got_t, t);
        assert_eq!(got_u, user);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_revoke_invalidates() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "ak2@example.com").await;
        let issued = svc.issue_api_key(user, &t, "deploy").await.unwrap();
        svc.validate_api_key(&issued.secret).await.unwrap();

        svc.revoke_api_key(issued.id, &t).await.unwrap();
        let err = svc.validate_api_key(&issued.secret).await.unwrap_err();
        assert!(err.to_string().to_lowercase().contains("invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_unknown_rejected() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let err = svc
            .validate_api_key("nope-not-a-real-key")
            .await
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_list_returns_descriptors() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "akl@example.com").await;
        let _ = svc.issue_api_key(user, &t, "one").await.unwrap();
        let _ = svc.issue_api_key(user, &t, "two").await.unwrap();
        let list = svc.list_api_keys(user, &t).await.unwrap();
        assert_eq!(list.len(), 2);
        let names: Vec<&str> = list.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"one"));
        assert!(names.contains(&"two"));
        // Last-used / revoked must be None on a freshly-issued key.
        for d in &list {
            assert!(d.revoked_at.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_revoke_wrong_project_not_found() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let other = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "akw@example.com").await;
        let issued = svc.issue_api_key(user, &t, "wrong-project").await.unwrap();
        let err = svc.revoke_api_key(issued.id, &other).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_duplicate_name_rejected() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "akd@example.com").await;
        let _ = svc.issue_api_key(user, &t, "dup").await.unwrap();
        let err = svc.issue_api_key(user, &t, "dup").await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("exists")
                || err.to_string().to_lowercase().contains("conflict"),
            "got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_key_last_used_at_bumped() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "aklu@example.com").await;
        let issued = svc.issue_api_key(user, &t, "ts").await.unwrap();
        // Before validation, last_used_at must be NULL.
        let pre = svc.list_api_keys(user, &t).await.unwrap();
        assert!(pre
            .iter()
            .find(|d| d.id == issued.id)
            .unwrap()
            .last_used_at
            .is_none());

        svc.validate_api_key(&issued.secret).await.unwrap();
        let post = svc.list_api_keys(user, &t).await.unwrap();
        assert!(post
            .iter()
            .find(|d| d.id == issued.id)
            .unwrap()
            .last_used_at
            .is_some());
    }

    // --- session-setting tests ----------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_settings_round_trip() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "sess@example.com").await;
        svc.set_session_setting(user, &t, "timezone", "America/New_York")
            .await
            .unwrap();
        svc.set_session_setting(user, &t, "language", "en-US")
            .await
            .unwrap();
        let got = svc.get_session_settings(user, &t).await.unwrap();
        assert_eq!(
            got.get("timezone").map(|s| s.as_str()),
            Some("America/New_York")
        );
        assert_eq!(got.get("language").map(|s| s.as_str()), Some("en-US"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_settings_upsert_replaces_value() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "sess2@example.com").await;
        svc.set_session_setting(user, &t, "language", "en")
            .await
            .unwrap();
        svc.set_session_setting(user, &t, "language", "fr-FR")
            .await
            .unwrap();
        let got = svc.get_session_settings(user, &t).await.unwrap();
        assert_eq!(got.get("language").map(|s| s.as_str()), Some("fr-FR"));
        assert_eq!(got.len(), 1, "upsert must not duplicate");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_settings_reject_unknown_key() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "sess3@example.com").await;
        let err = svc
            .set_session_setting(user, &t, "search_path", "public")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidIdent(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn from_env_fatal_on_missing_smtp_host() {
        // This is a pure unit-of-config test (no PG needed). It's also covered
        // in `config::tests`; mirrored here so a single `cargo test -p
        // basin-auth from_env_fatal_on_missing_smtp_host` finds it.
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _g = LOCK.lock().unwrap();

        let preserved: Vec<(&str, Option<String>)> = [
            "BASIN_AUTH_JWT_SECRET",
            "BASIN_AUTH_SMTP_HOST",
            "BASIN_AUTH_SMTP_PORT",
            "BASIN_AUTH_SMTP_USERNAME",
            "BASIN_AUTH_SMTP_PASSWORD",
            "BASIN_AUTH_SMTP_FROM",
            "BASIN_AUTH_SMTP_TLS",
        ]
        .iter()
        .map(|v| (*v, std::env::var(v).ok()))
        .collect();

        std::env::set_var("BASIN_AUTH_JWT_SECRET", "a".repeat(64));
        std::env::remove_var("BASIN_AUTH_SMTP_HOST");
        std::env::set_var("BASIN_AUTH_SMTP_PORT", "587");
        std::env::set_var("BASIN_AUTH_SMTP_USERNAME", "u");
        std::env::set_var("BASIN_AUTH_SMTP_PASSWORD", "p");
        std::env::set_var("BASIN_AUTH_SMTP_FROM", "n@example.com");
        std::env::set_var("BASIN_AUTH_SMTP_TLS", "starttls");

        let err = AuthConfig::from_env().expect_err("must fail without SMTP host");
        assert!(err.to_string().contains("BASIN_AUTH_SMTP_HOST"));

        // Restore env so we don't leak across tests.
        for (k, v) in preserved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }

    // --- email-link login (project-agnostic) ---------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn email_link_round_trip() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let user = make_verified_user(&svc, &mailer, &t, "el@example.com").await;
        svc.request_email_link("el@example.com").await.unwrap();
        let raw = last_token(&mailer);
        let toks = svc.consume_email_link(&raw).await.unwrap();
        let claims = svc.verify_jwt(&toks.access_token).unwrap();
        assert_eq!(claims.user_id, user);
        assert_eq!(claims.project_id, t);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn email_link_single_use() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let _ = make_verified_user(&svc, &mailer, &t, "el2@example.com").await;
        svc.request_email_link("el2@example.com").await.unwrap();
        let raw = last_token(&mailer);
        svc.consume_email_link(&raw).await.unwrap();
        let err = svc.consume_email_link(&raw).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("invalid"),
            "got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn email_link_unknown_email_silent() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        // No user exists for this address; the request must succeed (204
        // semantics on the wire) and not insert any DB row.
        svc.request_email_link("ghost@example.com").await.unwrap();
        // Verify no row was inserted: list_active_auth_magic_links should be empty.
        let links = svc
            .inner
            .store
            .list_active_auth_magic_links()
            .await
            .unwrap();
        assert_eq!(
            links.len(),
            0,
            "no row should be inserted for an unknown email"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn email_link_disabled_returns_error() {
        // Construct a service whose AuthConfig has `email_enabled = false`
        // — even though SMTP host is non-empty. The flow check is the
        // logical OR of both signals.
        let schema = unique_schema();
        let mut cfg = base_cfg(&schema);
        cfg.email_enabled = false;
        let mailer = StubMailer::new(cfg.smtp.from_email.clone());
        let svc = match tokio::time::timeout(
            Duration::from_secs(2),
            AuthService::connect_with_mailer(cfg, Arc::new(mailer)),
        )
        .await
        {
            Ok(Ok(s)) => s,
            _ => return,
        };
        let _g = SchemaGuard { schema };
        let err = svc
            .request_email_link("anyone@example.com")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("E_EMAIL_DISABLED"), "got {err:?}");
        assert!(!svc.is_email_enabled());
    }

    // --- refresh-token rotation + revocation list ---------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_old_token_after_rotate_returns_revoked() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let _ = make_verified_user(&svc, &mailer, &t, "rt1@example.com").await;
        let toks = svc
            .signin(&t, "rt1@example.com", "longenoughpassword")
            .await
            .unwrap();
        let new = svc.refresh(&toks.refresh_token).await.unwrap();
        assert_ne!(new.refresh_token, toks.refresh_token);
        // Old must now fail with the "revoked" wording.
        let err = svc.refresh(&toks.refresh_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("revoked"),
            "got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_reuse_after_double_rotation_revokes_all() {
        // The reuse-detection security path: A → rotated to B → rotated to
        // C (current). An attacker presenting A is the trigger; the result
        // is a blanket revoke that also kills C.
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let _ = make_verified_user(&svc, &mailer, &t, "reuse@example.com").await;
        let a = svc
            .signin(&t, "reuse@example.com", "longenoughpassword")
            .await
            .unwrap();
        let b = svc.refresh(&a.refresh_token).await.unwrap();
        // Sleep briefly so revoked_at on B is strictly after revoked_at on A.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let c = svc.refresh(&b.refresh_token).await.unwrap();

        // Replay A — this is the leak signal.
        let err = svc.refresh(&a.refresh_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("revoked"),
            "got {err:?}"
        );

        // C must now also be invalid (blanket sentinel).
        let err = svc.refresh(&c.refresh_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("revoked"),
            "after reuse-detection, current refresh must also be revoked: {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_jwt_audience_required() {
        // An access token presented to /refresh must fail (different aud).
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let _ = make_verified_user(&svc, &mailer, &t, "aud@example.com").await;
        let toks = svc
            .signin(&t, "aud@example.com", "longenoughpassword")
            .await
            .unwrap();
        let err = svc.refresh(&toks.access_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("invalid"),
            "got {err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_signout_then_use_returns_revoked() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = ProjectId::new();
        let _ = make_verified_user(&svc, &mailer, &t, "so2@example.com").await;
        let toks = svc
            .signin(&t, "so2@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.signout(&toks.refresh_token).await.unwrap();
        let err = svc.refresh(&toks.refresh_token).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("revoked"),
            "got {err:?}"
        );
    }
}
