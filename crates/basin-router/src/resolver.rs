//! Map a pgwire `user` parameter to a `ProjectId`.
//!
//! The PoC ships a `StaticProjectResolver` driven by a `HashMap`; production
//! deployments will swap in a network-backed resolver (etcd/FDB lookup,
//! project-control-plane RPC, etc.). The trait is async because any real
//! implementation will hit the network.
//!
//! [`JwtProjectResolver`] is the auth-aware variant: the pgwire `user`
//! parameter carries the bearer JWT verbatim (optionally prefixed with
//! `"Bearer "`); the resolver verifies the token via
//! `basin_auth::AuthService` and returns the embedded `project_id` claim.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, Result};

/// Resolves a pgwire `user` to a `ProjectId`. Implementations must be safe to
/// share across connections; `&self` is the only handle the router holds.
#[async_trait]
pub trait ProjectResolver: Send + Sync {
    async fn resolve(&self, username: &str) -> Result<ProjectId>;

    /// Variant that also receives the cleartext-password from the pgwire
    /// startup handshake. Default impl drops the password and falls
    /// through to `resolve(username)` — so existing JWT / API-key /
    /// static resolvers don't need to change. The
    /// [`ProjectCredentialsResolver`] overrides this to bcrypt-validate
    /// the `(user, password)` pair against the per-project credential row
    /// and resolve the project from there.
    async fn resolve_credentials(&self, username: &str, _password: &str) -> Result<ProjectId> {
        self.resolve(username).await
    }
}

/// In-memory resolver keyed on username -> project id. Useful for the PoC and
/// for tests; not a production tool.
#[derive(Debug, Clone, Default)]
pub struct StaticProjectResolver {
    map: HashMap<String, ProjectId>,
}

impl StaticProjectResolver {
    pub fn new(map: HashMap<String, ProjectId>) -> Self {
        Self { map }
    }

    pub fn with_entry(mut self, user: impl Into<String>, project: ProjectId) -> Self {
        self.map.insert(user.into(), project);
        self
    }
}

#[async_trait]
impl ProjectResolver for StaticProjectResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        self.map.get(username).copied().ok_or_else(|| {
            BasinError::not_found(format!("no project mapped for user {username:?}"))
        })
    }
}

/// JWT-backed resolver: the pgwire `user` parameter is the access token.
///
/// pgwire has no native bearer-token slot, so we overload `user`: clients
/// connect with `user=<jwt>` (optionally `user=Bearer <jwt>`) and the
/// resolver verifies the token through [`basin_auth::AuthService`] before
/// returning the embedded `project_id` claim. This keeps the wire format
/// untouched — every existing pgwire driver works as long as it can put
/// arbitrary text in `user`. See `basin-router/src/lib.rs` for the
/// integration story.
#[derive(Clone)]
pub struct JwtProjectResolver {
    auth: Arc<basin_auth::AuthService>,
}

impl JwtProjectResolver {
    pub fn new(auth: Arc<basin_auth::AuthService>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl ProjectResolver for JwtProjectResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        // Optional `Bearer ` prefix mirrors the HTTP Authorization header so
        // operators can paste tokens copied from the REST flow.
        let token = username.strip_prefix("Bearer ").unwrap_or(username);
        let claims = self
            .auth
            .verify_jwt(token)
            .map_err(|e| BasinError::not_found(format!("invalid jwt: {e}")))?;
        Ok(claims.project_id)
    }
}

/// API-key-backed resolver. The pgwire `user` parameter carries an opaque
/// long-lived API key (optionally `Bearer ...`-prefixed); the resolver
/// looks it up in the auth DB and returns the owning project. Pair with
/// [`StackedProjectResolver`] in front of [`JwtProjectResolver`] when a
/// deployment serves both credential types on the same listener.
#[derive(Clone)]
pub struct ApiKeyProjectResolver {
    auth: Arc<basin_auth::AuthService>,
}

impl ApiKeyProjectResolver {
    pub fn new(auth: Arc<basin_auth::AuthService>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl ProjectResolver for ApiKeyProjectResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        let token = username.strip_prefix("Bearer ").unwrap_or(username);
        let (project, _user) = self
            .auth
            .validate_api_key(token)
            .await
            .map_err(|e| BasinError::not_found(format!("invalid api key: {e}")))?;
        Ok(project)
    }
}

/// Resolves projects by `(pgwire_user, password)` pair against the
/// `auth_project_credentials` table. Built atop `basin_auth::AuthService`
/// so revocation, rotation, and bcrypt validation all live in one place.
///
/// The customer-facing surface is the connection URL `provision_project_db`
/// hands back: `postgres://<pgwire_user>:<password>@<host>/<dbname>`.
/// When pgwire startup arrives with that URL's `(user, password)`, this
/// resolver bcrypt-verifies and returns the project id; failure surfaces
/// as a uniform `BasinError::InvalidIdent("invalid pgwire credentials")`
/// — never leaks user existence vs. wrong password.
#[derive(Clone)]
pub struct ProjectCredentialsResolver {
    auth: Arc<basin_auth::AuthService>,
}

impl ProjectCredentialsResolver {
    pub fn new(auth: Arc<basin_auth::AuthService>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl ProjectResolver for ProjectCredentialsResolver {
    async fn resolve(&self, _username: &str) -> Result<ProjectId> {
        // The credentials path requires the password slot. Without it we
        // can't bcrypt-verify; refuse loudly so a misconfigured stack
        // doesn't silently accept any password.
        Err(BasinError::InvalidIdent(
            "invalid pgwire credentials".into(),
        ))
    }

    async fn resolve_credentials(&self, username: &str, password: &str) -> Result<ProjectId> {
        self.auth
            .validate_pgwire_credentials(username, password)
            .await
    }
}

/// Resolves projects by trying each backing resolver in order until one
/// succeeds. Used by basin-server when JWT, API-key, per-project credentials,
/// and static fallback are in play on the same listener.
#[derive(Clone)]
pub struct StackedProjectResolver {
    resolvers: Vec<Arc<dyn ProjectResolver>>,
}

impl StackedProjectResolver {
    pub fn new(resolvers: Vec<Arc<dyn ProjectResolver>>) -> Self {
        Self { resolvers }
    }
}

#[async_trait]
impl ProjectResolver for StackedProjectResolver {
    async fn resolve(&self, username: &str) -> Result<ProjectId> {
        let mut last_err = None;
        for r in &self.resolvers {
            match r.resolve(username).await {
                Ok(tid) => return Ok(tid),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err
            .unwrap_or_else(|| BasinError::not_found(format!("no resolver matched {username:?}"))))
    }

    async fn resolve_credentials(&self, username: &str, password: &str) -> Result<ProjectId> {
        let mut last_err = None;
        for r in &self.resolvers {
            match r.resolve_credentials(username, password).await {
                Ok(tid) => return Ok(tid),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err
            .unwrap_or_else(|| BasinError::not_found(format!("no resolver matched {username:?}"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_resolver_lookup() {
        let t = ProjectId::new();
        let mut map = HashMap::new();
        map.insert("alice".to_owned(), t);
        let r = StaticProjectResolver::new(map);

        assert_eq!(r.resolve("alice").await.unwrap(), t);
        let err = r.resolve("bob").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    // --- StackedProjectResolver tests ----------------------------------------
    //
    // Use a counting resolver so we can assert that the second backing
    // resolver is *not* polled when the first one already succeeds. This
    // matters for the JWT-then-static stack basin-server mounts: a valid JWT
    // must short-circuit before the static map is consulted.

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingResolver {
        inner: Arc<dyn ProjectResolver>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ProjectResolver for CountingResolver {
        async fn resolve(&self, username: &str) -> Result<ProjectId> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.resolve(username).await
        }
    }

    fn static_with(user: &str, t: ProjectId) -> Arc<dyn ProjectResolver> {
        Arc::new(StaticProjectResolver::default().with_entry(user, t))
    }

    #[tokio::test]
    async fn stacked_resolver_uses_first_match() {
        let t1 = ProjectId::new();
        let t2 = ProjectId::new();
        let second_calls = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(CountingResolver {
            inner: static_with("alice", t2),
            calls: second_calls.clone(),
        });
        let stacked = StackedProjectResolver::new(vec![static_with("alice", t1), second]);

        assert_eq!(stacked.resolve("alice").await.unwrap(), t1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stacked_resolver_falls_through_on_first_err() {
        let t = ProjectId::new();
        let stacked = StackedProjectResolver::new(vec![
            // First resolver has no entry for "alice" -> NotFound.
            Arc::new(StaticProjectResolver::default()),
            static_with("alice", t),
        ]);
        assert_eq!(stacked.resolve("alice").await.unwrap(), t);
    }

    #[tokio::test]
    async fn stacked_resolver_returns_last_err_when_all_fail() {
        let stacked = StackedProjectResolver::new(vec![
            Arc::new(StaticProjectResolver::default().with_entry("bob", ProjectId::new())),
            Arc::new(StaticProjectResolver::default().with_entry("carol", ProjectId::new())),
        ]);
        let err = stacked.resolve("alice").await.unwrap_err();
        // Last resolver is the carol-only map; its NotFound message mentions
        // alice (the username it was queried with), confirming the error came
        // from the *last* poll, not the first.
        assert!(
            matches!(err, BasinError::NotFound(ref m) if m.contains("alice")),
            "got {err:?}"
        );
    }

    // SECURITY regression: the bare `StaticProjectResolver` accepts ANY
    // password — its impl of `ProjectResolver` does NOT override
    // `resolve_credentials`, so the trait's default forwards the call to
    // `resolve(username)`, completely ignoring the password slot. With a
    // populated map this means a single name in the map (the old default
    // `BASIN_PROJECTS=alice=*`) silently authenticates any pgwire client
    // that types that username — regardless of the password they send.
    //
    // This test pins both halves of the contract:
    //
    // 1. A populated static resolver returns the mapped project for ANY
    //    password — the load-bearing observation that motivates leaving
    //    it OUT of the production stack unless explicitly populated.
    //
    // 2. An EMPTY static resolver (the new default `BASIN_PROJECTS=""`)
    //    returns NotFound for every username, password pair — confirming
    //    that if basin-server skips appending it (because the operator
    //    didn't set BASIN_PROJECTS) we do not silently let any user in.
    //
    // The basin-server fix is structural: only `push` the static resolver
    // into the resolver-stack vec when `static_resolver_has_entries` is
    // true. The test below is the unit-level check that "empty static map
    // means no auth bypass" — exactly the property the production wiring
    // now relies on.
    #[tokio::test]
    async fn static_resolver_accepts_any_password_when_populated() {
        let t = ProjectId::new();
        let r = StaticProjectResolver::default().with_entry("alice", t);
        // Any password works — this is exactly why we keep it out of the
        // stack when empty.
        assert_eq!(
            r.resolve_credentials("alice", "wrong-password")
                .await
                .unwrap(),
            t
        );
        assert_eq!(r.resolve_credentials("alice", "").await.unwrap(), t);
    }

    #[tokio::test]
    async fn empty_static_resolver_rejects_unknown_user() {
        // Matches the production wiring when BASIN_PROJECTS is unset:
        // the static resolver is built with no entries (or omitted from
        // the stack entirely). Either way, lookups must surface NotFound,
        // never auto-provision or fall through to a default.
        let r = StaticProjectResolver::default();
        let err = r.resolve("alice").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
        let err = r
            .resolve_credentials("alice", "any-password")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn stack_without_static_rejects_unknown_user() {
        // Simulates the new production stack when BASIN_PROJECTS is unset:
        // only credential / JWT / API-key resolvers are mounted, all of
        // which return errors for an unknown username. The stack must
        // surface that as NotFound rather than ever falling through to a
        // static map.
        //
        // We model the cred / JWT / API-key resolvers with plain
        // `StaticProjectResolver::default()` instances (empty maps) since
        // their failure shape — NotFound for an unmatched username — is
        // what the stack consumes. The point of this test is the *absence*
        // of the populated static resolver at the tail of the vec.
        let stacked = StackedProjectResolver::new(vec![
            Arc::new(StaticProjectResolver::default()),
            Arc::new(StaticProjectResolver::default()),
            Arc::new(StaticProjectResolver::default()),
            // ↑ no populated static resolver appended at the tail.
        ]);
        let err = stacked
            .resolve_credentials("alice", "any-password")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    // --- JwtProjectResolver tests --------------------------------------------
    //
    // These tests stand up a live `AuthService` against PG. They mirror the
    // skip-cleanly pattern used by `basin-auth`'s own tests so the suite is
    // still runnable without local PG/SMTP.

    use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
    use std::time::Duration;
    use ulid::Ulid;
    use uuid::Uuid;

    const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

    fn unique_schema() -> String {
        format!(
            "basin_router_jwt_{}",
            Ulid::new().to_string().to_lowercase()
        )
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
            bcrypt_cost: 4,
            password_min_len: 10,
            rate_limit_per_ip_per_min: 1000,
            email_enabled: true,
            pgwire_public_host: "127.0.0.1:5433".into(),
        }
    }

    /// Drop-guard so a panicking test doesn't leak a per-test PG schema.
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
                        eprintln!("jwt resolver schema cleanup runtime: {e}");
                        return;
                    }
                };
                rt.block_on(async {
                    let connect = tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio_postgres::connect(PG_URL, tokio_postgres::NoTls),
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

    async fn try_connect() -> Option<(AuthService, SchemaGuard)> {
        let schema = unique_schema();
        let cfg = base_cfg(&schema);
        let mailer = StubMailer::new(cfg.smtp.from_email.clone());
        let svc = match tokio::time::timeout(
            Duration::from_secs(2),
            AuthService::connect_with_mailer(cfg, Arc::new(mailer)),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                eprintln!("postgres unreachable, skipping jwt resolver test: {e}");
                return None;
            }
            Err(_) => {
                eprintln!("postgres connect timed out, skipping jwt resolver test");
                return None;
            }
        };
        Some((svc, SchemaGuard { schema }))
    }

    /// Issue a token directly via the AuthService's JWT helper, bypassing the
    /// signup/email flow — these tests are about the resolver, not the auth
    /// flows. We use the `JwtVerifier` trait's underlying `verify_jwt` to
    /// confirm the token round-trips, then drive the resolver against it.
    fn issue_via_auth(svc: &AuthService, project: &ProjectId) -> String {
        // Reach into the same `verify_jwt`-compatible signing key by going
        // through a fresh `JwtKeys` configured with the same secret as the
        // service. The service's secret is `[9u8; 32]` per `base_cfg`.
        let keys = basin_auth::jwt::JwtKeys::new(&[9u8; 32]);
        let now = chrono::Utc::now();
        let (jwt, _) = keys
            .issue(
                project,
                Uuid::new_v4(),
                "x@example.com",
                &[],
                now,
                Duration::from_secs(60),
            )
            .expect("issue");
        // Sanity check: the service can verify it.
        let _ = svc.verify_jwt(&jwt).expect("self-issued jwt verifies");
        jwt
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_resolver_returns_project_for_valid_token() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let project = ProjectId::new();
        let jwt = issue_via_auth(&svc, &project);
        let resolver = JwtProjectResolver::new(Arc::new(svc));
        let got = resolver.resolve(&jwt).await.expect("resolve");
        assert_eq!(got, project);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_resolver_rejects_tampered_token() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let project = ProjectId::new();
        let jwt = issue_via_auth(&svc, &project);
        let mut bytes = jwt.into_bytes();
        // Flip the last char of the signature; same trick as basin-auth's
        // tampered-signature test.
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();

        let resolver = JwtProjectResolver::new(Arc::new(svc));
        let err = resolver.resolve(&tampered).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_resolver_strips_bearer_prefix() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let project = ProjectId::new();
        let jwt = issue_via_auth(&svc, &project);
        let with_prefix = format!("Bearer {jwt}");
        let resolver = JwtProjectResolver::new(Arc::new(svc));
        let got = resolver.resolve(&with_prefix).await.expect("resolve");
        assert_eq!(got, project);
    }
}
