//! `basin-auth` — opinionated auth crate for Basin.
//!
//! See [ADR 0005](../../../docs/decisions/0005-auth-system.md) for the full
//! design + rationale. The short version: per-tenant `auth.users` rows,
//! email/password (bcrypt), JWT (HS256), refresh tokens, magic link,
//! password reset, email verification.
//!
//! **SMTP credentials are required at startup** — a missing
//! `BASIN_AUTH_SMTP_*` is a fatal error, not a warning.
//!
//! ## What's in v1
//!
//! - `signup(tenant, email, password)`
//! - `signin(tenant, email, password) -> Tokens`
//! - `verify_email(tenant, token)`
//! - `request_password_reset(tenant, email)` (sends email)
//! - `reset_password(tenant, token, new_password)`
//! - `request_magic_link(tenant, email)` (sends email)
//! - `signin_with_magic_link(tenant, token) -> Tokens`
//! - `verify_jwt(token) -> Claims` (used by `basin-router` and `basin-rest`)
//! - `refresh(refresh_token) -> Tokens`
//! - `signout(refresh_token)`
//!
//! ## Cross-crate trait integration
//!
//! `basin_router::TenantResolver` lives in the `basin-router` crate. Rust's
//! orphan rule prevents us from impl'ing it for `AuthService` from here, so
//! this crate exposes [`AuthService::verify_jwt`] and the
//! [`jwt_tenant_resolver`] helper. The basin-router integration PR will wrap
//! one of these into its own `TenantResolver` impl on its side.

#![forbid(unsafe_code)]

pub mod config;
pub mod email;
pub mod jwt;
pub mod password;
pub mod rate_limit;
pub mod schema;
pub mod tokens;

pub mod flows {
    pub mod magic;
    pub mod refresh;
    pub mod reset;
    pub mod signin;
    pub mod signup;
}

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::instrument;
use uuid::Uuid;

pub use crate::config::{AuthConfig, SmtpConfig, SmtpTls};
pub use crate::email::{Mailer, Outbound, SmtpMailer, StubMailer};
pub use crate::jwt::Claims;

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
    pub(crate) client: Mutex<Client>,
    pub(crate) jwt: jwt::JwtKeys,
    pub(crate) mailer: Arc<dyn Mailer>,
    pub(crate) ip_limiter: rate_limit::PerKey,
    pub(crate) email_limiter: rate_limit::PerKey,
}

impl Inner {
    pub(crate) fn schema(&self) -> &str {
        &self.cfg.catalog_schema
    }
}

/// Top-level auth handle. `Clone` is cheap; share across the engine, router,
/// REST layer, etc.
#[derive(Clone)]
pub struct AuthService {
    inner: Arc<Inner>,
}

impl AuthService {
    /// Connect using the configuration produced by `AuthConfig::from_env` (or
    /// hand-built). Spawns the Postgres driver task and runs migrations
    /// before returning.
    pub async fn connect(cfg: AuthConfig) -> Result<Self> {
        let mailer: Arc<dyn Mailer> = Arc::new(SmtpMailer::from_config(&cfg.smtp)?);
        Self::connect_with_mailer(cfg, mailer).await
    }

    /// Connect with a caller-supplied mailer. Used by tests with `StubMailer`
    /// so we don't actually send mail; could also be used by an operator
    /// wanting to wrap SMTP with extra logging.
    pub async fn connect_with_mailer(cfg: AuthConfig, mailer: Arc<dyn Mailer>) -> Result<Self> {
        schema::validate_schema_ident(&cfg.catalog_schema)?;
        let (client, connection) = tokio_postgres::connect(&cfg.catalog_dsn, NoTls)
            .await
            .map_err(|e| BasinError::catalog(format!("auth pg connect: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "basin-auth postgres driver exited");
            }
        });
        let jwt = jwt::JwtKeys::new(&cfg.jwt_secret);
        let ip_limiter = rate_limit::PerKey::per_minute(cfg.rate_limit_per_ip_per_min, "ip");
        let email_limiter = rate_limit::PerKey::per_minute(cfg.rate_limit_per_ip_per_min, "email");
        let svc = Self {
            inner: Arc::new(Inner {
                cfg,
                client: Mutex::new(client),
                jwt,
                mailer,
                ip_limiter,
                email_limiter,
            }),
        };
        svc.migrate().await?;
        Ok(svc)
    }

    /// Run the idempotent `CREATE TABLE IF NOT EXISTS` migration. Safe to
    /// call repeatedly; first call is the only one that does work.
    pub async fn migrate(&self) -> Result<()> {
        let client = self.inner.client.lock().await;
        schema::run_migrations(&client, self.inner.schema()).await
    }

    // --- public flows -------------------------------------------------------

    #[instrument(skip(self, password), fields(tenant = %tenant, email = %email))]
    pub async fn signup(
        &self,
        tenant: &TenantId,
        email: &str,
        password: &str,
    ) -> Result<UserId> {
        flows::signup::signup(&self.inner, tenant, email, password).await
    }

    #[instrument(skip(self), fields(tenant = %tenant, user_id = %user))]
    pub async fn request_email_verification(
        &self,
        tenant: &TenantId,
        user: UserId,
    ) -> Result<()> {
        flows::signup::request_email_verification(&self.inner, tenant, user).await
    }

    #[instrument(skip(self, token), fields(tenant = %tenant))]
    pub async fn verify_email(&self, tenant: &TenantId, token: &str) -> Result<()> {
        flows::signup::verify_email(&self.inner, tenant, token).await
    }

    #[instrument(skip(self, password), fields(tenant = %tenant, email = %email))]
    pub async fn signin(
        &self,
        tenant: &TenantId,
        email: &str,
        password: &str,
    ) -> Result<Tokens> {
        flows::signin::signin(&self.inner, tenant, email, password).await
    }

    #[instrument(skip(self, refresh_token))]
    pub async fn refresh(&self, refresh_token: &str) -> Result<Tokens> {
        flows::refresh::refresh(&self.inner, refresh_token).await
    }

    #[instrument(skip(self, refresh_token))]
    pub async fn signout(&self, refresh_token: &str) -> Result<()> {
        flows::refresh::signout(&self.inner, refresh_token).await
    }

    #[instrument(skip(self), fields(tenant = %tenant, email = %email))]
    pub async fn request_password_reset(
        &self,
        tenant: &TenantId,
        email: &str,
    ) -> Result<()> {
        flows::reset::request_password_reset(&self.inner, tenant, email).await
    }

    #[instrument(skip(self, token, new_password), fields(tenant = %tenant))]
    pub async fn reset_password(
        &self,
        tenant: &TenantId,
        token: &str,
        new_password: &str,
    ) -> Result<()> {
        flows::reset::reset_password(&self.inner, tenant, token, new_password).await
    }

    #[instrument(skip(self), fields(tenant = %tenant, email = %email))]
    pub async fn request_magic_link(&self, tenant: &TenantId, email: &str) -> Result<()> {
        flows::magic::request_magic_link(&self.inner, tenant, email).await
    }

    #[instrument(skip(self, token), fields(tenant = %tenant))]
    pub async fn signin_with_magic_link(
        &self,
        tenant: &TenantId,
        token: &str,
    ) -> Result<Tokens> {
        flows::magic::signin_with_magic_link(&self.inner, tenant, token).await
    }

    /// Decode and verify a JWT issued by this service. Cheap — no DB lookup.
    pub fn verify_jwt(&self, jwt: &str) -> Result<Claims> {
        self.inner.jwt.verify(jwt)
    }
}

/// Helper that bundles `AuthService` into a closure usable by
/// `basin-router::TenantResolver`-shaped code.
///
/// `basin-router` defines an async `TenantResolver` trait we can't impl
/// from this side of the orphan rule. The follow-up integration PR will
/// either:
///
/// 1. Add a `JwtTenantResolver(Arc<AuthService>)` newtype inside
///    `basin-router` and impl `TenantResolver` for it, calling
///    `svc.verify_jwt(...)` and returning `claims.tenant_id`; or
/// 2. Generalise `TenantResolver` to take a closure, in which case this
///    helper plugs in directly.
///
/// Either way, the cross-crate seam is `AuthService::verify_jwt`.
pub fn jwt_tenant_resolver(svc: AuthService) -> impl Fn(&str) -> Result<TenantId> + Send + Sync {
    move |jwt: &str| -> Result<TenantId> { svc.verify_jwt(jwt).map(|c| c.tenant_id) }
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

    use basin_common::TenantId;
    use tokio_postgres::NoTls;
    use ulid::Ulid;

    use super::*;

    const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

    fn unique_schema() -> String {
        format!(
            "basin_auth_test_{}",
            Ulid::new().to_string().to_lowercase()
        )
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
            catalog_dsn: PG_URL.to_owned(),
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
        let t = TenantId::new();
        let user = svc
            .signup(&t, "alice@example.com", "longenoughpassword")
            .await
            .unwrap();

        // Spot check: row exists, hash isn't the plaintext.
        let client = svc.inner.client.lock().await;
        let row = client
            .query_one(
                &format!(
                    "SELECT email, password_hash FROM {sch}.users WHERE user_id = $1",
                    sch = svc.inner.schema()
                ),
                &[&user],
            )
            .await
            .unwrap();
        let email: String = row.get(0);
        let hash: String = row.get(1);
        assert_eq!(email, "alice@example.com");
        assert_ne!(hash, "longenoughpassword");
        assert!(hash.starts_with("$2"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signup_rejects_short_password() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
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
        let t = TenantId::new();
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
    async fn signup_allows_same_email_different_tenants() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let a = TenantId::new();
        let b = TenantId::new();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        assert_eq!(claims.tenant_id, t);
        assert_eq!(claims.user_id, user);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signin_rejects_wrong_password() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
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
        let t = TenantId::new();
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
        assert!(err.to_string().to_lowercase().contains("invalid")
            || err.to_string().to_lowercase().contains("revoked"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signout_revokes_refresh() {
        let Some((svc, mailer, _g)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
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
        let t = TenantId::new();
        let user = svc
            .signup(&t, "pr@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        svc.verify_email(&t, &last_token(&mailer)).await.unwrap();

        svc.request_password_reset(&t, "pr@example.com").await.unwrap();
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
        let t = TenantId::new();
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
        let t = TenantId::new();
        let user = svc
            .signup(&t, "ex@example.com", "longenoughpassword")
            .await
            .unwrap();
        svc.request_email_verification(&t, user).await.unwrap();
        let token = last_token(&mailer);

        // Fake an expired email token by stamping `expires_at` in the past.
        let client = svc.inner.client.lock().await;
        client
            .execute(
                &format!(
                    "UPDATE {sch}.email_tokens SET expires_at = now() - INTERVAL '1 hour'
                     WHERE token_hash = $1",
                    sch = svc.inner.schema()
                ),
                &[&tokens::hash_token(&token)],
            )
            .await
            .unwrap();
        drop(client);

        let err = svc.verify_email(&t, &token).await.unwrap_err();
        assert!(err.to_string().to_lowercase().contains("expired")
            || err.to_string().to_lowercase().contains("invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_round_trip() {
        let Some((svc, _m, _g)) = try_connect().await else {
            return;
        };
        let t = TenantId::new();
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
        assert_eq!(c.tenant_id, t);
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
                &TenantId::new(),
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
}
