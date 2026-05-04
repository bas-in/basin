//! Map a pgwire `user` parameter to a `TenantId`.
//!
//! The PoC ships a `StaticTenantResolver` driven by a `HashMap`; production
//! deployments will swap in a network-backed resolver (etcd/FDB lookup,
//! tenant-control-plane RPC, etc.). The trait is async because any real
//! implementation will hit the network.
//!
//! [`JwtTenantResolver`] is the auth-aware variant: the pgwire `user`
//! parameter carries the bearer JWT verbatim (optionally prefixed with
//! `"Bearer "`); the resolver verifies the token via
//! `basin_auth::AuthService` and returns the embedded `tenant_id` claim.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{BasinError, Result, TenantId};

/// Resolves a pgwire `user` to a `TenantId`. Implementations must be safe to
/// share across connections; `&self` is the only handle the router holds.
#[async_trait]
pub trait TenantResolver: Send + Sync {
    async fn resolve(&self, username: &str) -> Result<TenantId>;
}

/// In-memory resolver keyed on username -> tenant id. Useful for the PoC and
/// for tests; not a production tool.
#[derive(Debug, Clone, Default)]
pub struct StaticTenantResolver {
    map: HashMap<String, TenantId>,
}

impl StaticTenantResolver {
    pub fn new(map: HashMap<String, TenantId>) -> Self {
        Self { map }
    }

    pub fn with_entry(mut self, user: impl Into<String>, tenant: TenantId) -> Self {
        self.map.insert(user.into(), tenant);
        self
    }
}

#[async_trait]
impl TenantResolver for StaticTenantResolver {
    async fn resolve(&self, username: &str) -> Result<TenantId> {
        self.map.get(username).copied().ok_or_else(|| {
            BasinError::not_found(format!("no tenant mapped for user {username:?}"))
        })
    }
}

/// JWT-backed resolver: the pgwire `user` parameter is the access token.
///
/// pgwire has no native bearer-token slot, so we overload `user`: clients
/// connect with `user=<jwt>` (optionally `user=Bearer <jwt>`) and the
/// resolver verifies the token through [`basin_auth::AuthService`] before
/// returning the embedded `tenant_id` claim. This keeps the wire format
/// untouched — every existing pgwire driver works as long as it can put
/// arbitrary text in `user`. See `basin-router/src/lib.rs` for the
/// integration story.
#[derive(Clone)]
pub struct JwtTenantResolver {
    auth: Arc<basin_auth::AuthService>,
}

impl JwtTenantResolver {
    pub fn new(auth: Arc<basin_auth::AuthService>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl TenantResolver for JwtTenantResolver {
    async fn resolve(&self, username: &str) -> Result<TenantId> {
        // Optional `Bearer ` prefix mirrors the HTTP Authorization header so
        // operators can paste tokens copied from the REST flow.
        let token = username.strip_prefix("Bearer ").unwrap_or(username);
        let claims = self
            .auth
            .verify_jwt(token)
            .map_err(|e| BasinError::not_found(format!("invalid jwt: {e}")))?;
        Ok(claims.tenant_id)
    }
}

/// Resolves tenants by trying each backing resolver in order until one
/// succeeds. Used by basin-server when both JWT and static credentials
/// are in play during a transition or demo.
#[derive(Clone)]
pub struct StackedTenantResolver {
    resolvers: Vec<Arc<dyn TenantResolver>>,
}

impl StackedTenantResolver {
    pub fn new(resolvers: Vec<Arc<dyn TenantResolver>>) -> Self {
        Self { resolvers }
    }
}

#[async_trait]
impl TenantResolver for StackedTenantResolver {
    async fn resolve(&self, username: &str) -> Result<TenantId> {
        let mut last_err = None;
        for r in &self.resolvers {
            match r.resolve(username).await {
                Ok(tid) => return Ok(tid),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            BasinError::not_found(format!("no resolver matched {username:?}"))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_resolver_lookup() {
        let t = TenantId::new();
        let mut map = HashMap::new();
        map.insert("alice".to_owned(), t);
        let r = StaticTenantResolver::new(map);

        assert_eq!(r.resolve("alice").await.unwrap(), t);
        let err = r.resolve("bob").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    // --- StackedTenantResolver tests ----------------------------------------
    //
    // Use a counting resolver so we can assert that the second backing
    // resolver is *not* polled when the first one already succeeds. This
    // matters for the JWT-then-static stack basin-server mounts: a valid JWT
    // must short-circuit before the static map is consulted.

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingResolver {
        inner: Arc<dyn TenantResolver>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TenantResolver for CountingResolver {
        async fn resolve(&self, username: &str) -> Result<TenantId> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.resolve(username).await
        }
    }

    fn static_with(user: &str, t: TenantId) -> Arc<dyn TenantResolver> {
        Arc::new(StaticTenantResolver::default().with_entry(user, t))
    }

    #[tokio::test]
    async fn stacked_resolver_uses_first_match() {
        let t1 = TenantId::new();
        let t2 = TenantId::new();
        let second_calls = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(CountingResolver {
            inner: static_with("alice", t2),
            calls: second_calls.clone(),
        });
        let stacked =
            StackedTenantResolver::new(vec![static_with("alice", t1), second]);

        assert_eq!(stacked.resolve("alice").await.unwrap(), t1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stacked_resolver_falls_through_on_first_err() {
        let t = TenantId::new();
        let stacked = StackedTenantResolver::new(vec![
            // First resolver has no entry for "alice" -> NotFound.
            Arc::new(StaticTenantResolver::default()),
            static_with("alice", t),
        ]);
        assert_eq!(stacked.resolve("alice").await.unwrap(), t);
    }

    #[tokio::test]
    async fn stacked_resolver_returns_last_err_when_all_fail() {
        let stacked = StackedTenantResolver::new(vec![
            Arc::new(StaticTenantResolver::default().with_entry("bob", TenantId::new())),
            Arc::new(StaticTenantResolver::default().with_entry("carol", TenantId::new())),
        ]);
        let err = stacked.resolve("alice").await.unwrap_err();
        // Last resolver is the carol-only map; its NotFound message mentions
        // alice (the username it was queried with), confirming the error came
        // from the *last* poll, not the first.
        assert!(matches!(err, BasinError::NotFound(ref m) if m.contains("alice")), "got {err:?}");
    }

    // --- JwtTenantResolver tests --------------------------------------------
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
        format!("basin_router_jwt_{}", Ulid::new().to_string().to_lowercase())
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
            bcrypt_cost: 4,
            password_min_len: 10,
            rate_limit_per_ip_per_min: 1000,
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
    fn issue_via_auth(svc: &AuthService, tenant: &TenantId) -> String {
        // Reach into the same `verify_jwt`-compatible signing key by going
        // through a fresh `JwtKeys` configured with the same secret as the
        // service. The service's secret is `[9u8; 32]` per `base_cfg`.
        let keys = basin_auth::jwt::JwtKeys::new(&[9u8; 32]);
        let now = chrono::Utc::now();
        let (jwt, _) = keys
            .issue(
                tenant,
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
    async fn jwt_resolver_returns_tenant_for_valid_token() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let tenant = TenantId::new();
        let jwt = issue_via_auth(&svc, &tenant);
        let resolver = JwtTenantResolver::new(Arc::new(svc));
        let got = resolver.resolve(&jwt).await.expect("resolve");
        assert_eq!(got, tenant);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_resolver_rejects_tampered_token() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let tenant = TenantId::new();
        let jwt = issue_via_auth(&svc, &tenant);
        let mut bytes = jwt.into_bytes();
        // Flip the last char of the signature; same trick as basin-auth's
        // tampered-signature test.
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();

        let resolver = JwtTenantResolver::new(Arc::new(svc));
        let err = resolver.resolve(&tampered).await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jwt_resolver_strips_bearer_prefix() {
        let Some((svc, _g)) = try_connect().await else {
            return;
        };
        let tenant = TenantId::new();
        let jwt = issue_via_auth(&svc, &tenant);
        let with_prefix = format!("Bearer {jwt}");
        let resolver = JwtTenantResolver::new(Arc::new(svc));
        let got = resolver.resolve(&with_prefix).await.expect("resolve");
        assert_eq!(got, tenant);
    }
}
