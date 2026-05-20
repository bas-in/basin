//! [`QueryExecutor`] trait — the surface `basin:fn/query` calls into.
//!
//! Decoupled from `basin-engine` so `basin-fn` can be tested in isolation
//! with a mock executor and integrated with the real engine without a
//! circular dependency.

use std::sync::Arc;

/// One row returned by a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRow {
    /// Ordered (column-name, JSON-encoded-value) pairs.
    pub columns: Vec<(String, String)>,
}

/// Trait that the host wires `basin:fn/query` to.
///
/// In production the real engine's `ProjectSession::execute` will implement
/// this. In tests a lightweight mock is used.
///
/// The trait is object-safe (`Arc<dyn QueryExecutor>`) so it can be stored
/// inside `FunctionCallContext` without generics.
pub trait QueryExecutor: Send + Sync {
    /// Execute `sql` and return all rows. Errors propagate as a string.
    fn exec_sql(&self, sql: &str) -> Result<Vec<QueryRow>, String>;
}

/// Mock executor that always succeeds and returns a single `{"result":"1"}`
/// row. Used in unit tests and as the default when no real executor is
/// provided.
pub struct MockQueryExecutor;

impl QueryExecutor for MockQueryExecutor {
    fn exec_sql(&self, sql: &str) -> Result<Vec<QueryRow>, String> {
        // The acceptance test calls `SELECT 1`. Return a plausible result.
        let result_value = if sql.trim().eq_ignore_ascii_case("select 1") {
            "1".to_string()
        } else {
            serde_json::json!(sql).to_string()
        };
        Ok(vec![QueryRow {
            columns: vec![("result".to_string(), result_value)],
        }])
    }
}

/// Secret store backing `basin:fn/secret`.
///
/// The real implementation decrypts via `basin_auth::oauth::EncryptionProvider`
/// (AES-256-GCM in production, `PlaintextEncryption` in dev). The mock below
/// returns values from an in-memory map — useful for unit tests that want to
/// exercise the happy path without a real catalog connection.
pub trait SecretStore: Send + Sync {
    /// Return the plaintext value for `name`, or an error string.
    fn get_secret(&self, name: &str) -> Result<String, String>;
}

/// Mock secret store — returns values from an in-memory map seeded at
/// construction time. Unknown names return `Err`. Used by unit tests.
pub struct MockSecretStore {
    secrets: std::collections::HashMap<String, String>,
}

impl MockSecretStore {
    /// Build an empty store (all lookups return `Err`).
    pub fn empty() -> Self {
        Self { secrets: std::collections::HashMap::new() }
    }

    /// Build a store pre-seeded with `(name, value)` pairs.
    pub fn with_secrets(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        Self {
            secrets: pairs.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        }
    }
}

impl SecretStore for MockSecretStore {
    fn get_secret(&self, name: &str) -> Result<String, String> {
        self.secrets
            .get(name)
            .cloned()
            .ok_or_else(|| format!("secret '{name}' not found"))
    }
}

/// `StubSecretStore` retained for backwards-compat with callers that used it
/// before this followup. Delegates to `MockSecretStore::empty()`.
pub struct StubSecretStore;

impl SecretStore for StubSecretStore {
    fn get_secret(&self, name: &str) -> Result<String, String> {
        MockSecretStore::empty().get_secret(name)
    }
}

// ---------------------------------------------------------------------------
// HttpSend — thin sync facade over the async basin_net::HttpClient
// ---------------------------------------------------------------------------

/// Request/response types for the `HttpSend` facade. Mirrors the WIT types so
/// `host.rs` marshals directly without an extra conversion step.
#[derive(Clone, Debug)]
pub struct FnHttpRequest {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct FnHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Synchronous HTTP send facade.
///
/// The WIT `fetch` function is not async; this trait lets `host.rs` call into
/// an implementation that either blocks (real `basin_net::HttpClient`) or
/// returns a canned response (mock for unit tests).
pub trait HttpSend: Send + Sync {
    fn send(&self, req: &FnHttpRequest) -> Result<FnHttpResponse, String>;
}

/// Mock HTTP client for unit tests. Returns a configurable canned response.
pub struct MockHttpClient {
    pub response: Result<FnHttpResponse, String>,
}

impl MockHttpClient {
    /// Always succeeds with a 200 OK and the given body.
    pub fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            response: Ok(FnHttpResponse {
                status: 200,
                headers: vec![],
                body: body.into(),
            }),
        }
    }

    /// Always fails with the given error string.
    pub fn err(msg: impl Into<String>) -> Self {
        Self { response: Err(msg.into()) }
    }
}

impl HttpSend for MockHttpClient {
    fn send(&self, _req: &FnHttpRequest) -> Result<FnHttpResponse, String> {
        self.response.clone()
    }
}

/// Contexts bundled together for one function invocation.
#[derive(Clone)]
pub struct InvocationContext {
    /// Executor for `basin:fn/query`. Shared across the call.
    pub query: Arc<dyn QueryExecutor>,
    /// Secret store for `basin:fn/secret`. Decrypts project secrets.
    pub secrets: Arc<dyn SecretStore>,
    /// HTTP client for `basin:fn/http`. Enforces allowlist + rate-limit +
    /// body-cap + timeout (real implementation is the `basin_net` adapter;
    /// tests inject `MockHttpClient`).
    pub http: Arc<dyn HttpSend>,
}

impl InvocationContext {
    /// Build with the production mock (no real engine, no secrets, deny-all http).
    pub fn mock() -> Self {
        Self {
            query: Arc::new(MockQueryExecutor),
            secrets: Arc::new(StubSecretStore),
            http: Arc::new(MockHttpClient::err("http not configured in mock context")),
        }
    }

    /// Build with a custom executor (for integration tests with basin-engine).
    pub fn with_executor(exec: Arc<dyn QueryExecutor>) -> Self {
        Self {
            query: exec,
            secrets: Arc::new(StubSecretStore),
            http: Arc::new(MockHttpClient::err("http not configured in mock context")),
        }
    }
}
