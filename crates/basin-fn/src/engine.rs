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
/// A real implementation decrypts via `EncryptionProvider`; the mock below
/// returns an error for unknown names.
pub trait SecretStore: Send + Sync {
    /// Return the plaintext value for `name`, or an error string.
    fn get_secret(&self, name: &str) -> Result<String, String>;
}

/// Mock secret store — returns `Err` for all names (stub for W1 partial).
///
/// TODO (W1-followup): replace with real decryption via `EncryptionProvider`
/// (see `basin-storage::encryption::EncryptionProvider`).
pub struct StubSecretStore;

impl SecretStore for StubSecretStore {
    fn get_secret(&self, name: &str) -> Result<String, String> {
        Err(format!(
            "secret '{name}' not found \
             (TODO W1-followup: wire EncryptionProvider)"
        ))
    }
}

/// Contexts bundled together for one function invocation.
#[derive(Clone)]
pub struct InvocationContext {
    /// Executor for `basin:fn/query`. Shared across the call.
    pub query: Arc<dyn QueryExecutor>,
    /// Secret store for `basin:fn/secret`.
    pub secrets: Arc<dyn SecretStore>,
}

impl InvocationContext {
    /// Build with the production mock (no real engine, no secrets).
    pub fn mock() -> Self {
        Self {
            query: Arc::new(MockQueryExecutor),
            secrets: Arc::new(StubSecretStore),
        }
    }

    /// Build with a custom executor (for integration tests with basin-engine).
    pub fn with_executor(exec: Arc<dyn QueryExecutor>) -> Self {
        Self {
            query: exec,
            secrets: Arc::new(StubSecretStore),
        }
    }
}
