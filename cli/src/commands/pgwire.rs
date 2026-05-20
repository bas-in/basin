//! pgwire — pgwire credentials + engine-keys management.
//!
//!   basin pgwire show          [--project=<ref>]
//!   basin pgwire reveal        [--project=<ref>]
//!   basin pgwire rotate        [--project=<ref>] [--yes]
//!   basin pgwire engine-keys list   [--project=<ref>]
//!   basin pgwire engine-keys rotate [--project=<ref>] [--yes]
//!
//! Routes:
//!   GET    /v1/projects/{ref}/pgwire              → show (masked password)
//!   POST   /v1/projects/{ref}/pgwire/reveal       → full DSN once
//!   POST   /v1/projects/{ref}/pgwire/rotate       → new password (or DSN)
//!   GET    /v1/projects/{ref}/engine-keys         → engine JWT keys (masked)
//!   POST   /v1/projects/{ref}/engine-keys/rotate  → new engine JWT keys
//!
//! Password masking convention: "••••••••<last4>" where last4 is the
//! final 4 characters of the password when longer than 8 chars; otherwise
//! "••••••••" with no suffix (mirrors Go's maskPassword exactly).
//!
//! DSN construction for `reveal` uses percent-encoding on the password so
//! special characters (%, @, /) are encoded and the DSN is safe to paste
//! into psql / libpq without shell quoting issues.
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd).
//! Error with hint at `basin link` if neither is available.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────

/// PgwireCredentials is the read shape from GET /v1/projects/{ref}/pgwire.
/// Password is always masked on this endpoint; use reveal to get the full DSN.
/// JSON shape: { host, port, user, dbname, password (masked), sslmode }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PgwireCredentials {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dbname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sslmode: String,
}

/// PgwireRevealResponse is the once-only response from
/// POST /v1/projects/{ref}/pgwire/reveal. Password contains the full
/// plaintext; DSN may be pre-built by the server or assembled client-side.
/// JSON shape: { host, port, user, dbname, password, sslmode, dsn? }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PgwireRevealResponse {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dbname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sslmode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dsn: String,
}

/// PgwireRotateResponse is the response from POST /v1/projects/{ref}/pgwire/rotate.
/// JSON shape: { host, port, user, dbname, password (new plaintext), sslmode, dsn? }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PgwireRotateResponse {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dbname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sslmode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dsn: String,
}

/// EngineKey is one JWT key returned by GET /v1/projects/{ref}/engine-keys.
/// JSON shape: { id, name, role, prefix, masked_key, created_at }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EngineKey {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub role: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub masked_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// EngineKeysResponse is the list response from GET /v1/projects/{ref}/engine-keys.
/// JSON shape: { keys: [ EngineKey ] }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EngineKeysResponse {
    #[serde(default)]
    pub keys: Vec<EngineKey>,
}

/// EngineKeysRotateResponse is the response from
/// POST /v1/projects/{ref}/engine-keys/rotate.
/// JSON shape: { keys: [ EngineKey ], anon_key?: string, service_key?: string }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EngineKeysRotateResponse {
    #[serde(default)]
    pub keys: Vec<EngineKey>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub anon_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service_key: String,
}

// ── Helpers ───────────────────────────────────────────────────────────

/// mask_password returns a masked representation of a password for display.
/// Shows "••••••••" plus the last 4 characters only when the password
/// is longer than 8 characters — matches Go's maskPassword exactly.
pub fn mask_password(pw: &str) -> String {
    let runes: Vec<char> = pw.chars().collect();
    if runes.len() > 8 {
        let last4: String = runes[runes.len() - 4..].iter().collect();
        return format!("••••••••{last4}");
    }
    "••••••••".to_string()
}

/// mask_engine_key masks an engine key for display.
fn mask_engine_key(k: &EngineKey) -> String {
    if !k.masked_key.is_empty() {
        return k.masked_key.clone();
    }
    if !k.prefix.is_empty() {
        return format!("{}••••••••", k.prefix);
    }
    "(masked)".to_string()
}

/// build_dsn assembles a postgres:// DSN from the reveal response fields.
/// Uses percent-encoding so that special characters in the password
/// (%, @, /) are correctly encoded and the DSN is safe to paste into psql.
/// If the server already returned a DSN, that is used verbatim.
pub fn build_dsn(r: &PgwireRevealResponse) -> String {
    if !r.dsn.is_empty() {
        return r.dsn.clone();
    }
    let host = if r.host.is_empty() { "localhost" } else { &r.host };
    let port = if r.port == 0 { 5432 } else { r.port };
    let sslmode = if r.sslmode.is_empty() { "require" } else { &r.sslmode };
    let user_enc = percent_encode_userinfo(&r.user);
    let pass_enc = percent_encode_userinfo(&r.password);
    format!(
        "postgres://{}:{}@{}:{}/{}?sslmode={}",
        user_enc, pass_enc, host, port, r.dbname, sslmode
    )
}

/// build_rotate_dsn assembles a postgres:// DSN from the rotate response.
/// Same encoding logic as build_dsn but operates on PgwireRotateResponse.
pub fn build_rotate_dsn(r: &PgwireRotateResponse) -> String {
    if !r.dsn.is_empty() {
        return r.dsn.clone();
    }
    let host = if r.host.is_empty() { "localhost" } else { &r.host };
    let port = if r.port == 0 { 5432 } else { r.port };
    let sslmode = if r.sslmode.is_empty() { "require" } else { &r.sslmode };
    let user_enc = percent_encode_userinfo(&r.user);
    let pass_enc = percent_encode_userinfo(&r.password);
    format!(
        "postgres://{}:{}@{}:{}/{}?sslmode={}",
        user_enc, pass_enc, host, port, r.dbname, sslmode
    )
}

/// percent_encode_userinfo percent-encodes a string for the userinfo
/// component of a URI (RFC 3986). All characters except unreserved are
/// encoded. Specifically: @ → %40, % → %25, : → %3A, / → %2F, etc.
/// This mirrors Go's url.UserPassword encoding.
fn percent_encode_userinfo(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            // RFC 3986 unreserved characters + userinfo sub-delims (but NOT @:/)
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~'
            | b'!'
            | b'$'
            | b'&'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b'+'
            | b','
            | b';'
            | b'=' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors with a helpful hint
/// when neither is available.
fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg("--project is required (or run `basin link` to bind this directory)")),
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_pgwire(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return show(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "show" => show(g, rest),
        "reveal" => reveal(g, rest),
        "rotate" => rotate(g, rest),
        "engine-keys" => engine_keys(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "pgwire",
                "Manage pgwire credentials and engine JWT keys.",
                &[
                    "show         [--project=<ref>]            Show pgwire metadata (password masked).",
                    "reveal       [--project=<ref>]            Print the full connection DSN (shown ONCE).",
                    "rotate       [--project=<ref>] [--yes]    Rotate the pgwire password (new DSN shown ONCE).",
                    "engine-keys list   [--project=<ref>]      List engine JWT keys (masked).",
                    "engine-keys rotate [--project=<ref>] [--yes]  Rotate engine JWT keys.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for pgwire");
            Err(silent())
        }
    }
}

// ── pgwire show ───────────────────────────────────────────────────────

fn show(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("pgwire show")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "pgwire show",
            "Show pgwire metadata for a project (password masked).",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let creds: PgwireCredentials =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/pgwire"), None)?;
    if g.json {
        // JSON shape: PgwireCredentials { host, port, user, dbname, password (masked), sslmode }
        // Password is always masked on this path — use `reveal` for the full DSN.
        return print_json(&mut std::io::stdout(), &creds);
    }
    let port = if creds.port == 0 { 5432 } else { creds.port };
    let sslmode = if creds.sslmode.is_empty() { "require".to_string() } else { creds.sslmode.clone() };
    println!("host:     {}", creds.host);
    println!("port:     {}", port);
    println!("user:     {}", creds.user);
    println!("dbname:   {}", creds.dbname);
    println!("password: {}", mask_password(&creds.password));
    println!("sslmode:  {}", sslmode);
    println!();
    println!("Run `basin pgwire reveal` to get the full DSN.");
    Ok(())
}

// ── pgwire reveal ─────────────────────────────────────────────────────

fn reveal(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("pgwire reveal")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "pgwire reveal",
            "Print the full connection DSN (shown ONCE — the server reveals the password once).",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: PgwireRevealResponse =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/pgwire/reveal"), None)?;
    let dsn = build_dsn(&resp);
    if g.json {
        // JSON shape: { dsn: string } — full DSN, shown once.
        return print_json(&mut std::io::stdout(), &serde_json::json!({ "dsn": dsn }));
    }
    println!("{dsn}");
    Ok(())
}

// ── pgwire rotate ─────────────────────────────────────────────────────

fn rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("pgwire rotate")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "pgwire rotate",
            "Rotate the pgwire password (new DSN shown ONCE).",
            &[
                "--project <ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to rotate pgwire credentials non-interactively under --json",
            ));
        }
        eprint!(
            "Rotate pgwire password for project {:?}? All existing connections will be dropped. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: PgwireRotateResponse =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/pgwire/rotate"), None)?;
    let dsn = build_rotate_dsn(&resp);
    if g.json {
        // JSON shape: { dsn: string } — new DSN after rotation, shown once.
        return print_json(&mut std::io::stdout(), &serde_json::json!({ "dsn": dsn }));
    }
    if !g.quiet {
        eprintln!("⚠  Store this DSN now — the password will NOT be shown again.");
    }
    println!("{dsn}");
    Ok(())
}

// ── pgwire engine-keys dispatcher ────────────────────────────────────

fn engine_keys(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin pgwire engine-keys (list | rotate) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => engine_keys_list(g, rest),
        "rotate" => engine_keys_rotate(g, rest),
        other => {
            printerr!(g, "unknown subcommand {other:?} for pgwire engine-keys");
            Err(silent())
        }
    }
}

// ── pgwire engine-keys list ───────────────────────────────────────────

fn engine_keys_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("pgwire engine-keys list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "pgwire engine-keys list",
            "List engine JWT keys for a project (masked).",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: EngineKeysResponse =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/engine-keys"), None)?;
    if g.json {
        // JSON shape: EngineKeysResponse { keys: [ EngineKey ] }
        // masked_key or prefix shown; full key never present in list.
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.keys.is_empty() {
        println!("(no engine keys)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "NAME", "ROLE", "KEY (MASKED)", "CREATED"]);
    for k in &resp.keys {
        t.row(&[&k.id, &k.name, &k.role, &mask_engine_key(k), &k.created_at]);
    }
    t.flush()
}

// ── pgwire engine-keys rotate ─────────────────────────────────────────

fn engine_keys_rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("pgwire engine-keys rotate")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "pgwire engine-keys rotate",
            "Rotate engine JWT keys for a project.",
            &[
                "--project <ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to rotate engine keys non-interactively under --json",
            ));
        }
        eprint!(
            "Rotate engine JWT keys for project {:?}? SDK clients using the old keys must be updated. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: EngineKeysRotateResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/engine-keys/rotate"),
        None,
    )?;
    if g.json {
        // JSON shape: EngineKeysRotateResponse { keys: [ EngineKey ], anon_key?: string, service_key?: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Engine keys rotated.");
    if !resp.anon_key.is_empty() {
        if !g.quiet {
            eprintln!("⚠  Store these keys now — they will NOT be shown again.");
        }
        println!("anon_key:    {}", resp.anon_key);
    }
    if !resp.service_key.is_empty() {
        println!("service_key: {}", resp.service_key);
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    fn sample_creds() -> &'static str {
        r#"{"host":"db.basin.run","port":5432,"user":"proj_user","dbname":"proj_db","password":"••••abcd","sslmode":"require"}"#
    }

    fn sample_reveal() -> &'static str {
        r#"{"host":"db.basin.run","port":5432,"user":"proj_user","dbname":"proj_db","password":"supersecretpassword1234","sslmode":"require"}"#
    }

    fn sample_engine_key(id: &str, name: &str, role: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"{name}","role":"{role}","prefix":"ek_","masked_key":"ek_••••••••","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── mask_password helper ──────────────────────────────────────────

    #[test]
    fn mask_password_empty_returns_bullets() {
        assert_eq!(mask_password(""), "••••••••");
    }

    #[test]
    fn mask_password_short_no_suffix() {
        // ≤8 chars: no suffix
        assert_eq!(mask_password("abcd"), "••••••••");
        assert_eq!(mask_password("abcde"), "••••••••");
        assert_eq!(mask_password("abcdefgh"), "••••••••"); // exactly 8
    }

    #[test]
    fn mask_password_long_shows_last4() {
        // >8 chars: last 4 shown
        assert_eq!(mask_password("supersecret1234"), "••••••••1234");
        assert_eq!(mask_password("abc@xyz!pass_end"), "••••••••_end");
    }

    // ── build_dsn helper ──────────────────────────────────────────────

    #[test]
    fn build_dsn_simple_password() {
        let r = PgwireRevealResponse {
            host: "db.basin.run".into(),
            port: 5432,
            user: "proj_user".into(),
            dbname: "proj_db".into(),
            password: "simplepassword".into(),
            sslmode: "require".into(),
            dsn: String::new(),
        };
        let dsn = build_dsn(&r);
        assert!(
            dsn.starts_with("postgres://proj_user:simplepassword@db.basin.run:5432/proj_db"),
            "got: {dsn}"
        );
        assert!(dsn.contains("sslmode=require"), "got: {dsn}");
    }

    #[test]
    fn build_dsn_percent_encodes_at_sign() {
        // url.UserPassword-equivalent: @ → %40
        let r = PgwireRevealResponse {
            host: "db.basin.run".into(),
            port: 5432,
            user: "proj_user".into(),
            dbname: "proj_db".into(),
            password: "p@ss%/word".into(),
            sslmode: "require".into(),
            dsn: String::new(),
        };
        let dsn = build_dsn(&r);
        // Raw @ must not appear (it would break the authority boundary).
        assert!(!dsn.contains("p@ss"), "unencoded @ found in: {dsn}");
        // The encoded @ must be present.
        assert!(dsn.contains("p%40ss"), "missing %40 in: {dsn}");
        // The encoded % must be present.
        assert!(dsn.contains("%25"), "missing %25 in: {dsn}");
    }

    #[test]
    fn build_dsn_uses_prebuilt_dsn_verbatim() {
        let prebuilt = "postgres://user:pw@host:5432/db?sslmode=require";
        let r = PgwireRevealResponse {
            dsn: prebuilt.to_string(),
            ..Default::default()
        };
        assert_eq!(build_dsn(&r), prebuilt);
    }

    #[test]
    fn build_dsn_defaults_for_zero_values() {
        let r = PgwireRevealResponse {
            user: "u".into(),
            dbname: "mydb".into(),
            password: "pw".into(),
            ..Default::default()
        };
        let dsn = build_dsn(&r);
        assert!(dsn.contains("localhost"), "expected localhost: {dsn}");
        assert!(dsn.contains(":5432"), "expected port 5432: {dsn}");
        assert!(dsn.contains("sslmode=require"), "expected sslmode=require: {dsn}");
    }

    // ── dispatcher ────────────────────────────────────────────────────

    #[test]
    fn no_args_delegates_to_show_which_needs_project() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_pgwire(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_pgwire(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_pgwire(&g, &["help".into()]).is_ok());
    }

    // ── pgwire show ───────────────────────────────────────────────────

    #[test]
    fn show_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/pgwire");
            Resp::ok(sample_creds())
        });
        let g = flags(&srv.url);
        cmd_pgwire(&g, &["show".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn show_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(sample_creds()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let creds: PgwireCredentials =
            c.do_json(Method::GET, "/v1/projects/proj1/pgwire", None).unwrap();
        assert_eq!(creds.host, "db.basin.run");
        assert_eq!(creds.user, "proj_user");
    }

    #[test]
    fn show_no_project_flag_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_pgwire(&g, &["show".into()]).is_err());
    }

    #[test]
    fn show_404_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"pgwire credentials not configured"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_pgwire(&g, &["show".into(), "--project=proj1".into()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── pgwire reveal ─────────────────────────────────────────────────

    #[test]
    fn reveal_happy_path_returns_dsn() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/pgwire/reveal");
            Resp::ok(sample_reveal())
        });
        let g = flags(&srv.url);
        cmd_pgwire(&g, &["reveal".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn reveal_json_shape_contains_dsn_key() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(sample_reveal()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PgwireRevealResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/pgwire/reveal", None).unwrap();
        let dsn = build_dsn(&resp);
        assert!(dsn.starts_with("postgres://"), "got: {dsn}");
        // JSON output shape: { dsn: string }
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &serde_json::json!({ "dsn": dsn })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["dsn"].as_str().unwrap().starts_with("postgres://"));
    }

    #[test]
    fn reveal_prebuilt_dsn_used_verbatim() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"dsn":"postgres://user:pass@host:5432/db?sslmode=require"}"#)
        });
        let g = flags(&srv.url);
        // Just check it doesn't error; verbatim DSN would be printed.
        cmd_pgwire(&g, &["reveal".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn reveal_special_chars_in_password_encoded() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"proj_user","dbname":"proj_db","password":"p@ss%word:/test","sslmode":"require"}"#)
        });
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PgwireRevealResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/pgwire/reveal", None).unwrap();
        let dsn = build_dsn(&resp);
        assert!(!dsn.contains("p@ss"), "unencoded @ found in DSN: {dsn}");
        assert!(dsn.contains("p%40ss"), "missing %40 in DSN: {dsn}");
    }

    // ── pgwire rotate ─────────────────────────────────────────────────

    #[test]
    fn rotate_with_yes_calls_endpoint() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/pgwire/rotate");
            Resp::ok(sample_reveal())
        });
        let g = flags(&srv.url);
        cmd_pgwire(
            &g,
            &["rotate".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap();
    }

    #[test]
    fn rotate_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err =
            cmd_pgwire(&g, &["rotate".into(), "--project=proj1".into()]).unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn rotate_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(sample_reveal()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PgwireRotateResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/pgwire/rotate", None).unwrap();
        let dsn = build_rotate_dsn(&resp);
        assert!(dsn.starts_with("postgres://"), "got: {dsn}");
        // JSON output: { dsn: string }
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &serde_json::json!({ "dsn": dsn })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["dsn"].as_str().unwrap().starts_with("postgres://"));
    }

    // ── pgwire engine-keys list ───────────────────────────────────────

    #[test]
    fn engine_keys_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/engine-keys");
            Resp::ok(r#"{"keys":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_pgwire(
            &g,
            &["engine-keys".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn engine_keys_list_populated() {
        let _g = with_temp_config_dir();
        let k1 = sample_engine_key("ek1", "anon", "anon");
        let k2 = sample_engine_key("ek2", "service_role", "service_role");
        let body = format!(r#"{{"keys":[{k1},{k2}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let g = flags(&srv.url);
        cmd_pgwire(
            &g,
            &["engine-keys".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn engine_keys_list_json_shape() {
        let _g = with_temp_config_dir();
        let k = sample_engine_key("ek1", "anon", "anon");
        let body = format!(r#"{{"keys":[{k}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: EngineKeysResponse =
            c.do_json(Method::GET, "/v1/projects/proj1/engine-keys", None).unwrap();
        assert_eq!(resp.keys.len(), 1);
        assert_eq!(resp.keys[0].id, "ek1");
    }

    // ── pgwire engine-keys rotate ─────────────────────────────────────

    #[test]
    fn engine_keys_rotate_with_yes() {
        let _g = with_temp_config_dir();
        let k = sample_engine_key("ek1", "anon", "anon");
        let body_resp = format!(
            r#"{{"keys":[{k}],"anon_key":"eyJnewAnonKey","service_key":"eyJnewServiceKey"}}"#
        );
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/engine-keys/rotate");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_pgwire(
            &g,
            &["engine-keys".into(), "rotate".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap();
    }

    #[test]
    fn engine_keys_rotate_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_pgwire(
            &g,
            &["engine-keys".into(), "rotate".into(), "--project=proj1".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn engine_keys_rotate_json_shape() {
        let _g = with_temp_config_dir();
        let k = sample_engine_key("ek1", "anon", "anon");
        let body_resp = format!(
            r#"{{"keys":[{k}],"anon_key":"eyJanonNew","service_key":"eyJserviceNew"}}"#
        );
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: EngineKeysRotateResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/engine-keys/rotate", None).unwrap();
        assert_eq!(resp.anon_key, "eyJanonNew");
        assert_eq!(resp.service_key, "eyJserviceNew");
    }

    #[test]
    fn engine_keys_unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err =
            cmd_pgwire(&g, &["engine-keys".into(), "nope".into()]).unwrap_err();
        // unknown sub → silent error
        assert!(crate::error::is_silent(err.as_ref()) || !err.to_string().is_empty());
    }

    #[test]
    fn engine_keys_no_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_pgwire(&g, &["engine-keys".into()]).is_err());
    }
}
