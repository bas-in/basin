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
    pub catalog_dsn: String,
    pub catalog_schema: String,
    pub smtp: SmtpConfig,
    pub bcrypt_cost: u32,
    pub password_min_len: usize,
    pub rate_limit_per_ip_per_min: u32,
}

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
        let token_ttl = optional_duration_secs("BASIN_AUTH_TOKEN_TTL", DEFAULT_TOKEN_TTL_SECS, &mut invalid);
        let refresh_ttl =
            optional_duration_secs("BASIN_AUTH_REFRESH_TTL", DEFAULT_REFRESH_TTL_SECS, &mut invalid);
        let bcrypt_cost = optional_u32("BASIN_AUTH_BCRYPT_COST", DEFAULT_BCRYPT_COST, &mut invalid);
        let password_min_len =
            optional_usize("BASIN_AUTH_PASSWORD_MIN_LEN", DEFAULT_PASSWORD_MIN_LEN, &mut invalid);
        let rate_limit_per_ip_per_min = optional_u32(
            "BASIN_AUTH_RATE_LIMIT_PER_IP_PER_MIN",
            DEFAULT_RATE_LIMIT_PER_IP_PER_MIN,
            &mut invalid,
        );

        let catalog_dsn = std::env::var("BASIN_AUTH_CATALOG_DSN")
            .unwrap_or_else(|_| DEFAULT_CATALOG_DSN.to_owned());
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

const MIN_SECRET_BYTES: usize = 32;
const DEFAULT_TOKEN_TTL_SECS: u64 = 60 * 60;
const DEFAULT_REFRESH_TTL_SECS: u64 = 60 * 60 * 24 * 30;
const DEFAULT_BCRYPT_COST: u32 = 12;
const DEFAULT_PASSWORD_MIN_LEN: usize = 10;
const DEFAULT_RATE_LIMIT_PER_IP_PER_MIN: u32 = 20;
const DEFAULT_CATALOG_SCHEMA: &str = "basin_auth";
const DEFAULT_CATALOG_DSN: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

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
}
