//! `AuthConfig` and the env-var loader.
//!
//! Per [ADR 0005](../../../docs/decisions/0005-auth-system.md), missing SMTP
//! credentials are a **fatal startup error**, not a warning. Half-configured
//! email is the source of every "you can't sign in" support ticket in
//! production auth systems, so we refuse to construct an `AuthConfig` without
//! the full SMTP set.
//!
//! `from_env()` returns a single error that lists *every* missing required
//! variable so the operator gets one round trip instead of fix-one-rerun-
//! discover-the-next.

use std::time::Duration;

use basin_common::{BasinError, Result};

/// How basin-auth speaks TLS to the SMTP relay.
///
/// The `None` variant exists for local dev only — never use it against a
/// public relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmtpTls {
    /// STARTTLS upgrade (port 587 typical).
    StartTls,
    /// Implicit TLS / SMTPS (port 465 typical).
    Implicit,
    /// No transport encryption. Local dev only.
    None,
}

/// SMTP credentials. **Required** at startup; see module docs.
#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub from_email: String,
    pub from_name: Option<String>,
    pub tls: SmtpTls,
}

/// Top-level auth configuration. Construct with [`AuthConfig::from_env`].
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub jwt_secret: Vec<u8>,
    pub token_ttl: Duration,
    pub refresh_ttl: Duration,
    /// Catalog DSN. `None` means "use the loopback default" — basin-auth will
    /// connect through basin engine's own pgwire listener on `127.0.0.1` as
    /// the reserved [`INTERNAL_AUTH_USERNAME`] user. That's the shape that
    /// unblocks self-hosting basin without provisioning a separate Postgres.
    /// Set this to an external `postgres://...` URL to keep using an outside
    /// catalog DB (managed PG, RDS, Neon, etc.).
    pub catalog_dsn: Option<String>,
    pub catalog_schema: String,
    pub smtp: SmtpConfig,
    pub bcrypt_cost: u32,
    pub password_min_len: usize,
    pub rate_limit_per_ip_per_min: u32,
    /// `false` disables every email-driven flow (magic-link, verify, reset).
    /// Defaults to `true`. Operators with no SMTP relay set this to false
    /// and the magic-link endpoint returns 503 instead of crashing on send.
    pub email_enabled: bool,
    /// Host:port that goes into the per-project `postgres://...` connection
    /// URL handed back from `AuthService::provision_project_db`. Set to the
    /// public-facing pgwire endpoint (the managed cloud uses
    /// `db.basin.run:5432`, matching the `basin.run` domain the CLI defaults
    /// to). For local development the default is `127.0.0.1:5433`.
    pub pgwire_public_host: String,
}

impl AuthConfig {
    /// True iff the SMTP block is meaningfully configured. Routes that
    /// depend on outbound mail (magic-link request) check this and refuse
    /// with 503 rather than queueing a doomed `lettre` send.
    pub fn is_email_enabled(&self) -> bool {
        self.email_enabled && !self.smtp.host.trim().is_empty()
    }

    /// Resolve the catalog DSN to a concrete connection string.
    ///
    /// If `catalog_dsn` is `Some`, returns it verbatim. Otherwise returns the
    /// loopback default that points at basin engine's own pgwire listener:
    /// `postgres://basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable`.
    ///
    /// The loopback username [`INTERNAL_AUTH_USERNAME`] maps through the
    /// auto-injected static-project entry to the reserved
    /// [`INTERNAL_AUTH_PROJECT_ID`]. Engine accepts plaintext socket on
    /// loopback, so `sslmode=disable` is correct and a TLS handshake is
    /// unnecessary.
    pub fn effective_dsn(&self) -> String {
        self.catalog_dsn
            .clone()
            .unwrap_or_else(|| DEFAULT_LOOPBACK_CATALOG_DSN.to_owned())
    }
}

/// Reserved system project for basin-auth's own catalog. Used by basin-server
/// to auto-inject the [`INTERNAL_AUTH_USERNAME`] → project mapping into the
/// static resolver so basin-auth can authenticate as itself over the loopback
/// pgwire path.
///
/// 26-char Crockford base-32 ULID (no I, L, O, U). Deterministic — the
/// constant is the contract. It is the same value as
/// [`basin_common::RESERVED_SYSTEM_PROJECT_ID`]; the storage layer keys its
/// "never pool-route auth" rule off that shared constant, so the two must stay
/// identical (re-exported here, not re-spelled, to make drift impossible).
pub const INTERNAL_AUTH_PROJECT_ID: &str = basin_common::RESERVED_SYSTEM_PROJECT_ID;

/// Reserved pgwire username basin-auth uses when connecting back to basin
/// engine over the loopback catalog path.
pub const INTERNAL_AUTH_USERNAME: &str = "basin_auth";

/// Default loopback catalog DSN. Used when `BASIN_AUTH_CATALOG_DSN` is unset
/// and no explicit DSN was passed in code. The username matches
/// [`INTERNAL_AUTH_USERNAME`] so basin-server's auto-injected static-project
/// entry resolves it to [`INTERNAL_AUTH_PROJECT_ID`].
pub const DEFAULT_LOOPBACK_CATALOG_DSN: &str =
    "postgres://basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable";

impl AuthConfig {
    /// Reads env vars per ADR 0005. Returns a single structured error listing
    /// every missing required variable rather than failing on the first one,
    /// so operators get one round trip to get the deploy right.
    pub fn from_env() -> Result<Self> {
        let mut missing: Vec<&'static str> = Vec::new();
        let mut invalid: Vec<String> = Vec::new();

        // --- JWT secret (required, at least 32 bytes) -----------------------
        let jwt_secret = match std::env::var("BASIN_AUTH_JWT_SECRET") {
            Ok(hex_or_raw) => {
                // Accept hex-encoded (preferred per ADR) or raw bytes; both
                // work as long as the resulting key material is >= 32 bytes.
                let bytes = match hex::decode(hex_or_raw.trim()) {
                    Ok(b) => b,
                    Err(_) => hex_or_raw.into_bytes(),
                };
                if bytes.len() < MIN_SECRET_BYTES {
                    invalid.push(format!(
                        "BASIN_AUTH_JWT_SECRET must be at least {MIN_SECRET_BYTES} bytes (was {})",
                        bytes.len()
                    ));
                    Vec::new()
                } else {
                    bytes
                }
            }
            Err(_) => {
                missing.push("BASIN_AUTH_JWT_SECRET");
                Vec::new()
            }
        };

        // --- SMTP block (every field required, fatal if missing) ------------
        let smtp_host = required_str("BASIN_AUTH_SMTP_HOST", &mut missing);
        let smtp_port_raw = required_str("BASIN_AUTH_SMTP_PORT", &mut missing);
        let smtp_username = required_str("BASIN_AUTH_SMTP_USERNAME", &mut missing);
        let smtp_password = required_str("BASIN_AUTH_SMTP_PASSWORD", &mut missing);
        let smtp_from = required_str("BASIN_AUTH_SMTP_FROM", &mut missing);
        let smtp_tls_raw = required_str("BASIN_AUTH_SMTP_TLS", &mut missing);

        let smtp_port: u16 = match smtp_port_raw.as_deref() {
            Some(s) => match s.trim().parse() {
                Ok(p) => p,
                Err(_) => {
                    invalid.push(format!("BASIN_AUTH_SMTP_PORT not a u16: {s:?}"));
                    0
                }
            },
            None => 0,
        };

        let smtp_tls = match smtp_tls_raw.as_deref() {
            Some(s) => match parse_tls(s) {
                Ok(t) => t,
                Err(e) => {
                    invalid.push(e);
                    SmtpTls::StartTls
                }
            },
            None => SmtpTls::StartTls,
        };

        // --- Optional knobs -------------------------------------------------
        let token_ttl =
            optional_duration_secs("BASIN_AUTH_TOKEN_TTL", DEFAULT_TOKEN_TTL_SECS, &mut invalid);
        let refresh_ttl = optional_duration_secs(
            "BASIN_AUTH_REFRESH_TTL",
            DEFAULT_REFRESH_TTL_SECS,
            &mut invalid,
        );
        let bcrypt_cost = optional_u32("BASIN_AUTH_BCRYPT_COST", DEFAULT_BCRYPT_COST, &mut invalid);
        let password_min_len = optional_usize(
            "BASIN_AUTH_PASSWORD_MIN_LEN",
            DEFAULT_PASSWORD_MIN_LEN,
            &mut invalid,
        );
        let rate_limit_per_ip_per_min = optional_u32(
            "BASIN_AUTH_RATE_LIMIT_PER_IP_PER_MIN",
            DEFAULT_RATE_LIMIT_PER_IP_PER_MIN,
            &mut invalid,
        );

        // Catalog DSN is now optional. Unset or empty means "use the loopback
        // default" (basin engine's own pgwire on 127.0.0.1:5433 via the
        // reserved system project). Set this to keep using an external Postgres
        // — e.g. a managed PG instance during migration off it.
        let catalog_dsn = match std::env::var("BASIN_AUTH_CATALOG_DSN") {
            Ok(s) if !s.trim().is_empty() => Some(s),
            _ => None,
        };
        let catalog_schema = std::env::var("BASIN_AUTH_CATALOG_SCHEMA")
            .unwrap_or_else(|_| DEFAULT_CATALOG_SCHEMA.to_owned());
        let from_name = std::env::var("BASIN_AUTH_EMAIL_FROM_NAME").ok();

        // --- One error covering everything we found -------------------------
        if !missing.is_empty() || !invalid.is_empty() {
            let mut msg = String::from("AuthConfig::from_env failed");
            if !missing.is_empty() {
                msg.push_str("; missing required env vars: ");
                msg.push_str(&missing.join(", "));
            }
            if !invalid.is_empty() {
                msg.push_str("; invalid: ");
                msg.push_str(&invalid.join("; "));
            }
            return Err(BasinError::internal(msg));
        }

        // Resolve per-region pgwire public host.
        //
        // Priority (highest wins):
        //   1. BASIN_PGWIRE_REGION_HOSTS lookup for the current FLY_REGION
        //   2. BASIN_AUTH_PGWIRE_PUBLIC_HOST (single-host fallback)
        //   3. Hard-coded local dev default "127.0.0.1:5433"
        //
        // BASIN_PGWIRE_REGION_HOSTS format mirrors the Go backend:
        //   jnb:jnb.db.basin.to,fra:fra.db.basin.to
        let pgwire_public_host = resolve_pgwire_host();

        Ok(Self {
            jwt_secret,
            token_ttl,
            refresh_ttl,
            catalog_dsn,
            catalog_schema,
            smtp: SmtpConfig {
                host: smtp_host.expect("missing-check above guarantees Some"),
                port: smtp_port,
                username: smtp_username.expect("missing-check above guarantees Some"),
                password: smtp_password.expect("missing-check above guarantees Some"),
                from_email: smtp_from.expect("missing-check above guarantees Some"),
                from_name,
                tls: smtp_tls,
            },
            bcrypt_cost,
            password_min_len,
            rate_limit_per_ip_per_min,
            // SMTP host validated above as required, so email is always
            // enabled for env-built configs.
            email_enabled: true,
            pgwire_public_host,
        })
    }
}

fn required_str(var: &'static str, missing: &mut Vec<&'static str>) -> Option<String> {
    match std::env::var(var) {
        Ok(s) if !s.is_empty() => Some(s),
        _ => {
            missing.push(var);
            None
        }
    }
}

fn parse_tls(s: &str) -> std::result::Result<SmtpTls, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "starttls" | "start_tls" | "tls" => Ok(SmtpTls::StartTls),
        "implicit" | "smtps" => Ok(SmtpTls::Implicit),
        "none" | "off" | "disabled" => Ok(SmtpTls::None),
        other => Err(format!(
            "BASIN_AUTH_SMTP_TLS must be one of starttls|implicit|none, got {other:?}"
        )),
    }
}

fn optional_duration_secs(var: &str, default_secs: u64, invalid: &mut Vec<String>) -> Duration {
    match std::env::var(var) {
        Ok(s) => match s.trim().parse::<u64>() {
            Ok(n) => Duration::from_secs(n),
            Err(_) => {
                invalid.push(format!("{var} not a non-negative integer (seconds): {s:?}"));
                Duration::from_secs(default_secs)
            }
        },
        Err(_) => Duration::from_secs(default_secs),
    }
}

fn optional_u32(var: &str, default: u32, invalid: &mut Vec<String>) -> u32 {
    match std::env::var(var) {
        Ok(s) => match s.trim().parse::<u32>() {
            Ok(n) => n,
            Err(_) => {
                invalid.push(format!("{var} not a u32: {s:?}"));
                default
            }
        },
        Err(_) => default,
    }
}

fn optional_usize(var: &str, default: usize, invalid: &mut Vec<String>) -> usize {
    match std::env::var(var) {
        Ok(s) => match s.trim().parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                invalid.push(format!("{var} not a usize: {s:?}"));
                default
            }
        },
        Err(_) => default,
    }
}

/// Resolve the pgwire public hostname to embed in connection URLs.
///
/// Resolution order:
/// 1. If `BASIN_PGWIRE_REGION_HOSTS` is set, parse the `region:host` map and
///    look up `FLY_REGION`. When a match is found, that host wins.
/// 2. Fall back to `BASIN_AUTH_PGWIRE_PUBLIC_HOST` (single-host override).
/// 3. Default to `127.0.0.1:5433` for local development.
///
/// Logs the resolved host at INFO level so operators can confirm the right
/// region endpoint was picked at startup.
fn resolve_pgwire_host() -> String {
    // Try region-specific host first.
    let region_map_raw = std::env::var("BASIN_PGWIRE_REGION_HOSTS").ok();
    if let Some(ref map_str) = region_map_raw {
        let fly_region = std::env::var("FLY_REGION").ok();
        if let Some(ref region) = fly_region {
            // Parse "jnb:jnb.db.basin.to,fra:fra.db.basin.to" into a lookup.
            for entry in map_str.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if let Some((r, h)) = entry.split_once(':') {
                    if r.trim() == region.trim() {
                        let host = h.trim().to_owned();
                        tracing::info!(
                            fly_region = %region,
                            pgwire_public_host = %host,
                            "pgwire public host resolved from BASIN_PGWIRE_REGION_HOSTS",
                        );
                        return host;
                    }
                }
            }
            // Region map was set but our region wasn't in it — warn and fall
            // through so a single-host override or the default still works.
            tracing::warn!(
                fly_region = %region,
                region_hosts = %map_str,
                "FLY_REGION not found in BASIN_PGWIRE_REGION_HOSTS; falling back to BASIN_AUTH_PGWIRE_PUBLIC_HOST",
            );
        }
    }

    // Single-host fallback / local dev default.
    let host = std::env::var("BASIN_AUTH_PGWIRE_PUBLIC_HOST")
        .unwrap_or_else(|_| "127.0.0.1:5433".to_owned());
    tracing::info!(
        pgwire_public_host = %host,
        "pgwire public host resolved from BASIN_AUTH_PGWIRE_PUBLIC_HOST (or default)",
    );
    host
}

const MIN_SECRET_BYTES: usize = 32;
const DEFAULT_TOKEN_TTL_SECS: u64 = 60 * 60;
const DEFAULT_REFRESH_TTL_SECS: u64 = 60 * 60 * 24 * 30;
const DEFAULT_BCRYPT_COST: u32 = 12;
const DEFAULT_PASSWORD_MIN_LEN: usize = 10;
const DEFAULT_RATE_LIMIT_PER_IP_PER_MIN: u32 = 20;
const DEFAULT_CATALOG_SCHEMA: &str = "basin_auth";

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env mutation must be single-threaded across these tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_all() {
        for v in [
            "BASIN_AUTH_JWT_SECRET",
            "BASIN_AUTH_TOKEN_TTL",
            "BASIN_AUTH_REFRESH_TTL",
            "BASIN_AUTH_SMTP_HOST",
            "BASIN_AUTH_SMTP_PORT",
            "BASIN_AUTH_SMTP_USERNAME",
            "BASIN_AUTH_SMTP_PASSWORD",
            "BASIN_AUTH_SMTP_FROM",
            "BASIN_AUTH_SMTP_TLS",
            "BASIN_AUTH_RATE_LIMIT_PER_IP_PER_MIN",
            "BASIN_AUTH_PASSWORD_MIN_LEN",
            "BASIN_AUTH_BCRYPT_COST",
            "BASIN_AUTH_EMAIL_FROM_NAME",
            "BASIN_AUTH_CATALOG_DSN",
            "BASIN_AUTH_CATALOG_SCHEMA",
        ] {
            std::env::remove_var(v);
        }
    }

    fn full_valid_env() {
        std::env::set_var(
            "BASIN_AUTH_JWT_SECRET",
            "0011223344556677889900112233445566778899001122334455667788990011",
        );
        std::env::set_var("BASIN_AUTH_SMTP_HOST", "smtp.example.com");
        std::env::set_var("BASIN_AUTH_SMTP_PORT", "587");
        std::env::set_var("BASIN_AUTH_SMTP_USERNAME", "u");
        std::env::set_var("BASIN_AUTH_SMTP_PASSWORD", "p");
        std::env::set_var("BASIN_AUTH_SMTP_FROM", "noreply@example.com");
        std::env::set_var("BASIN_AUTH_SMTP_TLS", "starttls");
    }

    #[test]
    fn happy_path() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_all();
        full_valid_env();
        let cfg = AuthConfig::from_env().expect("ok");
        assert_eq!(cfg.smtp.host, "smtp.example.com");
        assert_eq!(cfg.smtp.port, 587);
        assert_eq!(cfg.smtp.tls, SmtpTls::StartTls);
        assert_eq!(cfg.bcrypt_cost, DEFAULT_BCRYPT_COST);
        assert_eq!(cfg.password_min_len, DEFAULT_PASSWORD_MIN_LEN);
        assert!(cfg.jwt_secret.len() >= 32);
        clear_all();
    }

    #[test]
    fn from_env_fatal_on_missing_smtp_host() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_all();
        full_valid_env();
        std::env::remove_var("BASIN_AUTH_SMTP_HOST");
        let err = AuthConfig::from_env().expect_err("must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("BASIN_AUTH_SMTP_HOST"),
            "error must name the missing variable, got {msg}"
        );
        clear_all();
    }

    #[test]
    fn collects_every_missing_smtp_var() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_all();
        std::env::set_var("BASIN_AUTH_JWT_SECRET", "a".repeat(64));
        let err = AuthConfig::from_env().expect_err("must fail");
        let msg = err.to_string();
        for v in [
            "BASIN_AUTH_SMTP_HOST",
            "BASIN_AUTH_SMTP_PORT",
            "BASIN_AUTH_SMTP_USERNAME",
            "BASIN_AUTH_SMTP_PASSWORD",
            "BASIN_AUTH_SMTP_FROM",
            "BASIN_AUTH_SMTP_TLS",
        ] {
            assert!(msg.contains(v), "missing-var report missed {v}: {msg}");
        }
        clear_all();
    }

    #[test]
    fn rejects_short_jwt_secret() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_all();
        full_valid_env();
        std::env::set_var("BASIN_AUTH_JWT_SECRET", "tooshort");
        let err = AuthConfig::from_env().expect_err("must fail");
        assert!(err.to_string().contains("BASIN_AUTH_JWT_SECRET"));
        clear_all();
    }

    #[test]
    fn parse_tls_variants() {
        assert_eq!(parse_tls("starttls").unwrap(), SmtpTls::StartTls);
        assert_eq!(parse_tls("Implicit").unwrap(), SmtpTls::Implicit);
        assert_eq!(parse_tls("NONE").unwrap(), SmtpTls::None);
        assert!(parse_tls("garbage").is_err());
    }

    /// `effective_dsn()` falls back to the loopback default when
    /// `catalog_dsn` is `None`. This is the path basin-server takes when no
    /// external Postgres is configured — basin-auth connects back through
    /// basin engine's own pgwire on `127.0.0.1:5433`.
    #[test]
    fn effective_dsn_falls_back_to_loopback() {
        let cfg = AuthConfig {
            jwt_secret: vec![9u8; 32],
            token_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            catalog_dsn: None,
            catalog_schema: "basin_auth".into(),
            smtp: SmtpConfig {
                host: "smtp.invalid".into(),
                port: 587,
                username: "u".into(),
                password: "p".into(),
                from_email: "n@example.com".into(),
                from_name: None,
                tls: SmtpTls::StartTls,
            },
            bcrypt_cost: 4,
            password_min_len: 10,
            rate_limit_per_ip_per_min: 100,
            email_enabled: true,
            pgwire_public_host: "127.0.0.1:5433".into(),
        };
        assert_eq!(cfg.effective_dsn(), DEFAULT_LOOPBACK_CATALOG_DSN);
        assert!(cfg.effective_dsn().contains("basin_auth:basin_auth"));
        assert!(cfg.effective_dsn().contains("sslmode=disable"));
    }
}
