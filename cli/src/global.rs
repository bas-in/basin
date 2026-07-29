//! global — top-level flag parsing + the credential lookup ladder.
//!
//! A small set of flags applies uniformly to every subcommand
//! (--api-url, --token, --org, --json, -q/--quiet, --no-color). We strip
//! them off argv before dispatch so each subcommand parses only its own
//! flags (mirroring Go's hand-rolled global pass).
//!
//! ## Self-hosted / OSS engine mode
//!
//! Set `BASIN_MODE=engine` (or pass `--engine` globally) to route management
//! commands directly to a self-hosted basin-server instead of the cloud
//! control plane.  The engine's admin routes require a JWT with
//! `is_admin: true`; supply it via `BASIN_ADMIN_TOKEN` or `--admin-token`.
//!
//! Quick-start:
//!
//!   export BASIN_API=http://localhost:3000
//!   export BASIN_MODE=engine
//!   export BASIN_ADMIN_TOKEN=<jwt-with-is_admin-true>
//!   basin snapshots create --project=<project-ulid>

use crate::client::Client;
use crate::config::{read_config_file, ConfigFile};
use crate::error::{msg, silent, CliResult};

/// GlobalFlags carries the flags every subcommand understands.
#[derive(Debug, Clone)]
pub struct GlobalFlags {
    pub api_url: String,
    pub token: String,
    pub org_slug: String,
    pub json: bool,
    pub quiet: bool,
    pub no_color: bool,
    /// engine_mode: when true the CLI targets the self-hosted OSS engine's
    /// admin API (`/admin/v1/projects/:id/…`) instead of the cloud
    /// control-plane (`/v1/projects/:ref/…`).
    /// Set via BASIN_MODE=engine or --engine.
    pub engine_mode: bool,
    /// admin_token: the `is_admin: true` JWT for the engine admin routes.
    /// Set via BASIN_ADMIN_TOKEN or --admin-token.
    /// Only used when engine_mode is true.
    pub admin_token: String,
}

impl Default for GlobalFlags {
    fn default() -> Self {
        GlobalFlags {
            api_url: default_api_url(),
            token: String::new(),
            org_slug: String::new(),
            json: false,
            quiet: false,
            no_color: false,
            engine_mode: false,
            admin_token: String::new(),
        }
    }
}

/// env_or returns $k or `dflt` when empty.
pub fn env_or(k: &str, dflt: &str) -> String {
    match std::env::var(k) {
        Ok(v) if !v.is_empty() => v,
        _ => dflt.to_string(),
    }
}

/// default_api_url builds the fallback API URL from $BASIN_DOMAIN (or
/// "basin.run"). One env var retargets the binary at a different cloud.
pub fn default_api_url() -> String {
    let d = std::env::var("BASIN_DOMAIN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "basin.run".to_string());
    format!("https://api.{d}")
}

/// parse_global_flags strips the top-level flags off argv and returns
/// the residual subcommand args. Unrecognised tokens pass through.
pub fn parse_global_flags(argv: &[String]) -> CliResult<(GlobalFlags, Vec<String>)> {
    let engine_mode_from_env = matches!(
        std::env::var("BASIN_MODE").as_deref(),
        Ok("engine") | Ok("ENGINE")
    );
    let mut g = GlobalFlags {
        api_url: env_or("BASIN_API", &default_api_url()),
        token: std::env::var("BASIN_TOKEN").unwrap_or_default(),
        no_color: std::env::var_os("NO_COLOR").is_some(),
        engine_mode: engine_mode_from_env,
        admin_token: std::env::var("BASIN_ADMIN_TOKEN").unwrap_or_default(),
        ..Default::default()
    };
    let mut rest: Vec<String> = Vec::with_capacity(argv.len());
    let mut i = 0;
    while i < argv.len() {
        let a = &argv[i];
        match a.as_str() {
            "--" => {
                rest.extend_from_slice(&argv[i + 1..]);
                break;
            }
            "--json" => g.json = true,
            "--no-color" => g.no_color = true,
            "-q" | "--quiet" => g.quiet = true,
            "--engine" => g.engine_mode = true,
            "-h" | "--help" => {
                if rest.is_empty() {
                    rest.push("help".to_string());
                } else {
                    rest.push(a.clone());
                }
            }
            "--api-url" => {
                if i + 1 >= argv.len() {
                    return Err(msg("--api-url requires a value"));
                }
                g.api_url = argv[i + 1].clone();
                i += 1;
            }
            "--token" => {
                if i + 1 >= argv.len() {
                    return Err(msg("--token requires a value"));
                }
                g.token = argv[i + 1].clone();
                i += 1;
            }
            "--admin-token" => {
                if i + 1 >= argv.len() {
                    return Err(msg("--admin-token requires a value"));
                }
                g.admin_token = argv[i + 1].clone();
                i += 1;
            }
            s if s.starts_with("--api-url=") => {
                g.api_url = s.trim_start_matches("--api-url=").to_string()
            }
            s if s.starts_with("--token=") => {
                g.token = s.trim_start_matches("--token=").to_string()
            }
            s if s.starts_with("--admin-token=") => {
                g.admin_token = s.trim_start_matches("--admin-token=").to_string()
            }
            s if s.starts_with("--org=") => g.org_slug = s.trim_start_matches("--org=").to_string(),
            _ => rest.push(a.clone()),
        }
        i += 1;
    }
    Ok((g, rest))
}

/// resolve_token applies the lookup ladder: --token, then per-org config
/// entry (when --org set), then default_token.
pub fn resolve_token(g: &GlobalFlags, loaded: Option<&ConfigFile>) -> String {
    if !g.token.is_empty() {
        return g.token.clone();
    }
    let Some(cf) = loaded else {
        return String::new();
    };
    if !g.org_slug.is_empty() {
        if let Some(t) = cf.tokens.get(&g.org_slug) {
            if !t.is_empty() {
                return t.clone();
            }
        }
    }
    cf.default_token.clone()
}

/// require_client builds an authenticated [`Client`], printing a uniform
/// "not signed in" message + returning the [`silent`] sentinel when no
/// credentials are available.
pub fn require_client(g: &GlobalFlags) -> CliResult<Client> {
    let cfg = read_config_file().ok().flatten();
    let tok = resolve_token(g, cfg.as_ref());
    if tok.is_empty() {
        eprintln!(
            "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>."
        );
        return Err(silent());
    }
    let mut api_url = g.api_url.clone();
    if let Some(cf) = &cfg {
        if !cf.api_url.is_empty()
            && std::env::var("BASIN_API")
                .map(|v| v.is_empty())
                .unwrap_or(true)
            && api_url == default_api_url()
        {
            api_url = cf.api_url.clone();
        }
    }
    Ok(Client::new(&api_url, &tok))
}

/// require_engine_client builds a [`Client`] for the self-hosted engine admin
/// API.  Requires `g.engine_mode == true` and a non-empty admin token sourced
/// from `BASIN_ADMIN_TOKEN` / `--admin-token`.
///
/// Returns the [`silent`] sentinel with a clear message on misconfiguration.
pub fn require_engine_client(g: &GlobalFlags) -> CliResult<Client> {
    if g.admin_token.is_empty() {
        eprintln!(
            "basin: engine mode requires an admin token.\n\
             Set BASIN_ADMIN_TOKEN=<jwt-with-is_admin-true> or pass --admin-token=<token>.\n\
             The token is the `is_admin: true` JWT that basin-server accepts on /admin/v1/* routes."
        );
        return Err(silent());
    }
    Ok(Client::new(&g.api_url, &g.admin_token))
}

/// engine_admin_path returns the admin route path for a project operation.
///
/// `project_id` must be the engine project ULID (the `:project_id` path
/// parameter the engine expects on `/admin/v1/projects/:project_id/…`).
///
/// `suffix` is the trailing path segment, e.g. `"snapshot"`, `"restore"`,
/// `"fork"`.
pub fn engine_admin_path(project_id: &str, suffix: &str) -> String {
    format!("/admin/v1/projects/{project_id}/{suffix}")
}
