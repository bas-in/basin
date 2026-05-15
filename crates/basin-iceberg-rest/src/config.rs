//! Configuration for the Iceberg REST surface.

/// Mount-time configuration. v0.1 only carries auth-toggle + a default
/// table location prefix; future fields land additively.
#[derive(Clone, Debug)]
pub struct IcebergRestConfig {
    /// When true (default), every request must carry a non-empty
    /// `Authorization: Bearer <token>` header. When false (test-only),
    /// requests pass through with no auth check. The eventual production
    /// path replaces the stub bearer check with `basin_auth::AuthService::
    /// verify_jwt`; see `auth.rs` for the migration note.
    pub require_bearer: bool,
    /// Base location written into the `metadata.location` field of every
    /// `LoadTableResponse`. Defaults to `s3://basin/`. The translator
    /// concatenates `<base_location>/<project>/<table>/` for each table.
    pub base_location: String,
}

impl Default for IcebergRestConfig {
    fn default() -> Self {
        Self {
            require_bearer: true,
            base_location: "s3://basin/".to_string(),
        }
    }
}

impl IcebergRestConfig {
    /// Test-only convenience: disable auth + pin the location prefix to a
    /// stable test value. Use the `Default` impl in production wiring.
    pub fn for_tests() -> Self {
        Self {
            require_bearer: false,
            base_location: "s3://basin-test/".to_string(),
        }
    }
}
