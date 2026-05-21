//! Synchronous HTTP client. Wraps `reqwest::Client` plus the per-project
//! [`AllowList`] / [`RateLimit`] / [`GuardConfig`] gates.
//!
//! Each call funnels through [`HttpClient::send`], which is the single
//! point where the safety checks fire. Higher-level helpers
//! ([`HttpClient::http_get`] / [`HttpClient::http_post`]) construct an
//! [`HttpRequest`] and delegate to `send`.

use std::sync::Arc;

use basin_common::ProjectId;
use reqwest::Method;
use tokio::time::timeout;

use crate::guards::{AllowList, GuardConfig, RateLimit, RebindingGuardedResolver};
use crate::types::{HttpError, HttpRequest, HttpResponse};

/// HTTP client scoped to a single Basin process. Cheap to clone (`Arc`
/// internals).
#[derive(Clone, Debug)]
pub struct HttpClient {
    inner: Arc<HttpClientInner>,
}

#[derive(Debug)]
struct HttpClientInner {
    reqwest: reqwest::Client,
    allow: AllowList,
    rate: RateLimit,
    cfg: GuardConfig,
}

impl HttpClient {
    /// Build a new client with default guards. Reads
    /// `BASIN_NET_MAX_BODY_BYTES` and `BASIN_NET_TIMEOUT_SECS` once.
    pub fn new() -> Self {
        Self::with_config(GuardConfig::from_env(), AllowList::new(), RateLimit::new())
    }

    /// Same as [`HttpClient::new`] but with explicit guards. Tests use this
    /// to dial the body-cap or timeout in isolation.
    pub fn with_config(cfg: GuardConfig, allow: AllowList, rate: RateLimit) -> Self {
        // We honour our own per-call timeout via `tokio::time::timeout`
        // rather than reqwest's. reqwest's timeout starts when the request
        // is built, ours starts when `send` is called — closer to what a
        // SQL caller would expect from `pg_net.timeout_milliseconds`.
        // TLS is on by default; we don't expose `danger_accept_invalid_certs`
        // by design — letting SQL disable cert verification would be the
        // single biggest footgun in this crate.
        //
        // The `dns_resolver` hook installs a [`RebindingGuardedResolver`]
        // that re-checks every resolved IP against the SSRF denylist before
        // hyper opens the TCP socket. Without this, an allowlisted hostname
        // whose authoritative server hands back `127.0.0.1` would walk
        // straight past the parse-time check in [`AllowList::check`]. The
        // `allow_loopback_for_tests` flag (loud name on purpose) opts out
        // of the denylist for test fixtures that spawn a local mock server.
        let resolver: Arc<RebindingGuardedResolver> = if cfg.allow_loopback_for_tests {
            Arc::new(RebindingGuardedResolver::with_loopback_allowed_for_tests())
        } else {
            Arc::new(RebindingGuardedResolver::new())
        };
        let reqwest = reqwest::Client::builder()
            .user_agent("basin-net/0.1")
            .dns_resolver(resolver)
            .build()
            .expect("reqwest::Client::build cannot fail with default config");
        Self {
            inner: Arc::new(HttpClientInner {
                reqwest,
                allow,
                rate,
                cfg,
            }),
        }
    }

    /// Allowlist accessor. Tests and the SQL surface (once landed) edit
    /// allowed hosts through this.
    pub fn allowlist(&self) -> &AllowList {
        &self.inner.allow
    }

    /// Rate-limit accessor. Mostly useful for tests that want to assert the
    /// limiter is shared across calls.
    pub fn rate_limit(&self) -> &RateLimit {
        &self.inner.rate
    }

    /// Guard config snapshot.
    pub fn config(&self) -> &GuardConfig {
        &self.inner.cfg
    }

    /// Convenience: opt `host` into `project`'s allowlist. Equivalent to
    /// `INSERT INTO _net_allowed_hosts VALUES ('host')` once the SQL
    /// surface lands.
    pub async fn allow_host(&self, project: &ProjectId, host: impl Into<String>) {
        self.inner.allow.allow(project, host).await;
    }

    /// `http_get(url)` — synchronous GET. Mirrors the Postgres `http`
    /// extension's signature.
    pub async fn http_get(
        &self,
        project: &ProjectId,
        url: &str,
    ) -> Result<HttpResponse, HttpError> {
        self.send(project, &HttpRequest::get(url)).await
    }

    /// `http_post(url, body, content_type)` — synchronous POST.
    pub async fn http_post(
        &self,
        project: &ProjectId,
        url: &str,
        body: impl Into<Vec<u8>>,
        content_type: &str,
    ) -> Result<HttpResponse, HttpError> {
        self.send(project, &HttpRequest::post(url, body, content_type))
            .await
    }

    /// The single safety choke-point. The order of checks here is
    /// deliberate:
    ///
    /// 1. Allowlist — denies before consuming any rate-limit budget.
    /// 2. Rate limit — applied to allowlisted hosts only.
    /// 3. Body cap on the *outgoing* request.
    /// 4. Timeout-bounded dispatch.
    /// 5. Body cap on the *response*.
    pub async fn send(
        &self,
        project: &ProjectId,
        req: &HttpRequest,
    ) -> Result<HttpResponse, HttpError> {
        // 1. Allowlist + SSRF safety. The SSRF gate runs inside
        // `check_with` (IP-literal denylist + userinfo rejection); when
        // `allow_loopback_for_tests` is set, only the IP-class slice of
        // that gate is suppressed.
        self.inner
            .allow
            .check_with(project, &req.url, self.inner.cfg.allow_loopback_for_tests)
            .await?;
        // 2. Rate limit.
        self.inner.rate.check(project)?;
        // 3. Outgoing body cap.
        if let Some(b) = &req.body {
            if b.len() > self.inner.cfg.max_body_bytes {
                return Err(HttpError::BodyTooLarge {
                    cap: self.inner.cfg.max_body_bytes,
                    actual: b.len(),
                });
            }
        }
        // 4. Build and dispatch.
        let method = match req.method.as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            other => return Err(HttpError::Transport(format!("unsupported method: {other}"))),
        };
        let mut builder = self.inner.reqwest.request(method, &req.url);
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        if let Some(b) = &req.body {
            builder = builder.body(b.clone());
        }
        let to = req.timeout.unwrap_or(self.inner.cfg.timeout);
        let resp = timeout(to, builder.send())
            .await
            .map_err(|_| HttpError::Timeout(to))?
            .map_err(|e| HttpError::Transport(format!("{e}")))?;
        let status = resp.status().as_u16();
        let mut headers = std::collections::BTreeMap::new();
        for (k, v) in resp.headers() {
            if let Ok(vs) = v.to_str() {
                headers.insert(k.as_str().to_lowercase(), vs.to_string());
            }
        }
        // 5. Response body cap. We could stream-truncate to make the cap
        // strictly enforce-then-error, but for v0.1 the simpler
        // bytes()-then-check matches the synchronous semantics of the
        // `http` extension and keeps the code paths short.
        let bytes = timeout(to, resp.bytes())
            .await
            .map_err(|_| HttpError::Timeout(to))?
            .map_err(|e| HttpError::Transport(format!("body read: {e}")))?;
        if bytes.len() > self.inner.cfg.max_body_bytes {
            return Err(HttpError::BodyTooLarge {
                cap: self.inner.cfg.max_body_bytes,
                actual: bytes.len(),
            });
        }
        Ok(HttpResponse {
            status,
            headers,
            body: bytes.to_vec(),
            error: None,
        })
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}
