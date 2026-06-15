#![deny(missing_docs)]
#![doc = include_str!("../README.md")]
//!
//! ---
//!
//! ## Crate layout
//!
//! - [`Client`] — the entry point, built via [`Client::builder`].
//! - [`auth`] — every `/auth/v1/*` flow with mutex-guarded auto-refresh, full
//!   MFA lifecycle, and OAuth authorize-URL minting.
//! - [`mod@query`] — the fluent [`QueryBuilder`] over `/rest/v1/:table` with
//!   cursor pagination, plus insert/update/delete.
//! - [`storage`] — bucket + object CRUD, signed/public URLs.
//! - [`functions`] — `rpc()` over `/rest/v1/rpc/:fn` and HTTP-handler
//!   `invoke()` over `/fn/v1/:name`.
//! - [`mod@error`] — [`BasinError`] and the [`ApiError`] envelope carrying the
//!   stable code, HTTP status, and SQLSTATE.
//! - [`realtime`] (feature `realtime`) — a `futures::Stream` of change /
//!   presence events with reconnect + backoff.
//! - [`arrow`] (feature `arrow`) — native Arrow IPC transport with JSON
//!   fallback.
//!
//! Every binding is derived from the server's routes, verified against
//! `crates/basin-rest/src/server.rs`; see the per-module docs for route
//! citations.

pub mod auth;
pub mod error;
pub mod functions;
pub mod query;
pub mod storage;
pub mod types;

mod http;

#[cfg(feature = "realtime")]
pub mod realtime;

#[cfg(feature = "arrow")]
pub mod arrow;

pub use auth::{project_id_from_jwt, AuthClient};
pub use error::{ApiError, BasinError, ERROR_CODES};
pub use functions::{FunctionsClient, InvokeBody};
pub use query::{QueryBuilder, QueryResult, Scalar};
pub use storage::{DownloadResult, StorageBucketClient, StorageClient};

use std::sync::Arc;
use std::time::Duration;

use crate::auth::project_id_from_jwt as pid_from_jwt;
use crate::http::Transport;

/// The Basin client — the entry point for every API surface.
///
/// Construct one with [`Client::builder`]. The client is cheap to [`Clone`]
/// (it shares one connection pool and one session lock), so clone it freely
/// across tasks rather than building several.
///
/// ```no_run
/// # async fn ex() -> Result<(), basin::BasinError> {
/// use basin::Client;
///
/// let client = Client::builder("http://localhost:8080")
///     .token("my-api-key-or-jwt")
///     .project_id("01J...")
///     .build()?;
///
/// let orders = client
///     .table("orders")
///     .select("id,total")
///     .gte("total", 100i64)
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    auth: AuthClient,
    functions: FunctionsClient,
    storage: StorageClient,
    explicit_project_id: Option<String>,
    key: Option<String>,
}

impl Client {
    /// Start building a client against the given base URL
    /// (e.g. `http://localhost:8080`).
    pub fn builder(url: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(url.into())
    }

    fn from_builder(b: ClientBuilder) -> Result<Self, BasinError> {
        let http_client = match b.http_client {
            Some(c) => c,
            None => reqwest::Client::builder()
                .timeout(b.timeout)
                .build()
                .map_err(|e| BasinError::Network(e.to_string()))?,
        };
        let transport = Transport::new(b.url, b.token.clone(), http_client);

        // Resolve the project id used for public storage URLs / realtime.
        let resolved_project_id = b.project_id.clone().or_else(|| {
            b.token.as_deref().and_then(pid_from_jwt)
        });

        let auth = AuthClient::new(transport.clone(), b.project_id.clone());
        let functions = FunctionsClient::new(transport.clone());
        let storage = StorageClient::new(transport.clone(), resolved_project_id);

        // Wire the transport's token getter to the auth client so every request
        // uses the freshest session token (auto-refreshing near expiry).
        let auth_for_getter = auth.clone();
        let getter: crate::http::TokenGetter = Arc::new(move || {
            let auth = auth_for_getter.clone();
            Box::pin(async move { auth.access_token().await })
        });
        transport.set_token_getter(getter);

        Ok(Self {
            transport,
            auth,
            functions,
            storage,
            explicit_project_id: b.project_id,
            key: b.token,
        })
    }

    /// The auth client for `/auth/v1/*`.
    pub fn auth(&self) -> &AuthClient {
        &self.auth
    }

    /// The functions client (`rpc()` and HTTP-handler `invoke()`).
    pub fn functions(&self) -> &FunctionsClient {
        &self.functions
    }

    /// The storage client for `/storage/v1/*`.
    pub fn storage(&self) -> &StorageClient {
        &self.storage
    }

    /// Begin a query against `/rest/v1/:table`.
    pub fn table(&self, name: &str) -> QueryBuilder {
        QueryBuilder::new(self.transport.clone(), name)
    }

    /// `POST /rest/v1/rpc/:fn_name` — convenience alias for
    /// [`FunctionsClient::rpc`].
    pub async fn rpc(
        &self,
        fn_name: &str,
        args: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, BasinError> {
        self.functions.rpc(fn_name, args).await
    }

    /// `GET /health` → `"ok"`.
    pub async fn health(&self) -> Result<String, BasinError> {
        let resp = self
            .transport
            .request(reqwest::Method::GET, "/health", &[], None, None, &[], false)
            .await?;
        resp.text().await.map_err(BasinError::from)
    }

    /// Resolve the effective project id (explicit, else from the JWT session,
    /// else from a JWT key). Used internally for realtime and public URLs.
    pub async fn resolve_project_id(&self) -> Option<String> {
        if let Some(p) = &self.explicit_project_id {
            return Some(p.clone());
        }
        if let Some(s) = self.auth.get_session().await {
            if let Some(p) = pid_from_jwt(&s.access_token) {
                return Some(p);
            }
        }
        self.key.as_deref().and_then(pid_from_jwt)
    }

    /// The base URL the client was built with (trailing slash trimmed).
    pub fn base_url(&self) -> &str {
        &self.transport.base_url
    }

    #[cfg(any(feature = "realtime", feature = "arrow"))]
    pub(crate) fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Construct a [`realtime::RealtimeClient`] for
    /// `GET /realtime/v1/ws/:project`.
    ///
    /// Requires the `realtime` feature. The project id must be resolvable
    /// (explicit, or from a JWT) at connect time.
    #[cfg(feature = "realtime")]
    pub fn realtime(&self) -> realtime::RealtimeClient {
        realtime::RealtimeClient::new(self.clone())
    }
}

/// Builder for [`Client`].
pub struct ClientBuilder {
    url: String,
    token: Option<String>,
    project_id: Option<String>,
    timeout: Duration,
    http_client: Option<reqwest::Client>,
}

impl ClientBuilder {
    fn new(url: String) -> Self {
        Self {
            url,
            token: None,
            project_id: None,
            timeout: Duration::from_secs(30),
            http_client: None,
        }
    }

    /// Set the auth token — a JWT or a raw API-key secret. Both are sent as
    /// `Authorization: Bearer <token>` (the server tries JWT verify first, then
    /// API-key lookup).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Set the project ULID. Optional when the token is a JWT carrying a
    /// `project_id` claim; required for public storage URLs and realtime when
    /// using a raw API key.
    pub fn project_id(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Override the default 30 s request timeout. Ignored if a custom
    /// [`reqwest::Client`] is supplied via [`http_client`](Self::http_client).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Supply a pre-configured [`reqwest::Client`] (for proxies, custom TLS,
    /// connection pooling, or test transports). Takes precedence over
    /// [`timeout`](Self::timeout).
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Build the [`Client`].
    pub fn build(self) -> Result<Client, BasinError> {
        Client::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_resolves_project_from_jwt() {
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"project_id":"01FROMJWT"}"#);
        let jwt = format!("h.{payload}.s");
        let client = Client::builder("http://h").token(jwt).build().unwrap();
        // Public URL resolution should pick up the JWT project id.
        let url = client
            .storage()
            .from_bucket("b")
            .get_public_url("x")
            .unwrap();
        assert!(url.contains("/public/01FROMJWT/"));
    }

    #[test]
    fn builder_explicit_project_wins() {
        let client = Client::builder("http://h")
            .token("rawkey")
            .project_id("01EXPLICIT")
            .build()
            .unwrap();
        let url = client
            .storage()
            .from_bucket("b")
            .get_public_url("x")
            .unwrap();
        assert!(url.contains("/public/01EXPLICIT/"));
    }

    #[test]
    fn base_url_accessor() {
        let client = Client::builder("http://h:9/").build().unwrap();
        assert_eq!(client.base_url(), "http://h:9");
    }
}
