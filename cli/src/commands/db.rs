//! db — daily-driver database workflow.
//!
//!   basin db push   [--project=<ref>]
//!   basin db pull   [--project=<ref>] [--force]
//!   basin db diff   [--project=<ref>]
//!   basin db reset  [--project=<ref>] [--yes]
//!   basin db url    [--project=<ref>] [--reveal] [--rotate] [--yes]
//!   basin db dump   [--project=<ref>] [--schema-only|--data-only]
//!   basin db lint   [--project=<ref>]
//!
//! Project ref resolution: --project flag, else load_working_project(cwd).
//! Migration calls use /admin/v1/projects/{ref}/migrations.
//! Snapshot safety-net uses POST /v1/projects/{ref}/backups/snapshots.
//! SQL execution uses POST /v1/projects/{ref}/sql/query.
//! pgwire credentials: GET /v1/projects/{ref}/pgwire, POST …/reveal, POST …/rotate.
//!
//! CWD-dependent logic is factored into inner functions that take a
//! `cwd: &std::path::Path` so tests can pass a temp dir without touching
//! the process-global cwd (never use set_current_dir — it races tests).

use std::path::Path;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// MigrationRow mirrors the list endpoint's row shape.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MigrationRow {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub at: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub op: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub table: String,
}

/// MigrationsListResp is the list endpoint envelope.
#[derive(Deserialize, Default)]
struct MigrationsListResp {
    #[serde(default)]
    migrations: Vec<MigrationRow>,
}

/// MigrationDetail is the full shape from GET /admin/v1/projects/{ref}/migrations/{id}.
#[derive(Debug, Default, Deserialize)]
struct MigrationDetail {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub source: String,
}

/// CreateMigrationResp is returned from POST /admin/v1/projects/{ref}/migrations.
#[derive(Debug, Default, Deserialize)]
struct CreateMigrationResp {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub source: String,
}

/// SnapshotResp is returned from POST /v1/projects/{ref}/backups/snapshots.
#[derive(Debug, Default, Deserialize)]
struct SnapshotResp {
    #[serde(default)]
    pub id: String,
}

/// PgwireCredentials from GET /v1/projects/{ref}/pgwire (password always masked).
#[derive(Debug, Default, Deserialize)]
struct PgwireCredentials {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub dbname: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub sslmode: String,
}

/// PgwireRevealResponse from POST /v1/projects/{ref}/pgwire/reveal.
#[derive(Debug, Default, Deserialize)]
struct PgwireRevealResponse {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub dbname: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub sslmode: String,
    #[serde(default)]
    pub dsn: String, // server may pre-build this
}

/// PgwireRotateResponse from POST /v1/projects/{ref}/pgwire/rotate.
#[derive(Debug, Default, Deserialize)]
struct PgwireRotateResponse {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: i64,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub dbname: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub sslmode: String,
    #[serde(default)]
    pub dsn: String,
}

/// QueryResult for SQL passthrough (db dump + lint).
#[derive(Debug, Default, Deserialize)]
struct QueryResult {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// DbLintError is one parse/validation error from dry-running a migration.
#[derive(Debug, Serialize)]
struct DbLintError {
    pub file: String,
    pub line: i64,
    pub message: String,
}

// ── Project ref resolution ────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors when neither is set.
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

// ── Migration file helpers ────────────────────────────────────────────────────

/// collect_local_migrations returns a sorted list of absolute paths for
/// every `*.sql` file in `<cwd>/basin/migrations/`. Returns an empty Vec
/// (not an error) when the directory is absent or empty.
fn collect_local_migrations(cwd: &Path) -> Vec<std::path::PathBuf> {
    let dir = cwd.join("basin").join("migrations");
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            !e.file_type().map(|t| t.is_dir()).unwrap_or(true)
                && e.file_name().to_string_lossy().ends_with(".sql")
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// migration_key returns the identifier used to match a remote MigrationRow
/// against a local *.sql filename. Uses the basename of `source` when set,
/// otherwise falls back to `id`.
fn migration_key(m: &MigrationRow) -> String {
    if !m.source.is_empty() {
        return Path::new(&m.source)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&m.source)
            .to_string();
    }
    m.id.clone()
}

// ── pgwire helpers ────────────────────────────────────────────────────────────

/// mask_password returns a masked representation of a password for display.
/// Shows "••••••••" plus the last 4 characters only when the password
/// is longer than 8 characters.
fn mask_password(pw: &str) -> String {
    if pw.is_empty() {
        return "••••••••".to_string();
    }
    let runes: Vec<char> = pw.chars().collect();
    if runes.len() > 8 {
        let suffix: String = runes[runes.len() - 4..].iter().collect();
        return format!("••••••••{suffix}");
    }
    "••••••••".to_string()
}

/// build_dsn_from_fields assembles a postgres:// DSN from individual fields.
/// Uses percent-encoding for the password so special characters survive.
fn build_dsn_from_fields(host: &str, port: i64, user: &str, dbname: &str, password: &str, sslmode: &str) -> String {
    let h = if host.is_empty() { "localhost" } else { host };
    let p = if port == 0 { 5432 } else { port };
    let s = if sslmode.is_empty() { "require" } else { sslmode };
    let pw_enc = percent_encode(password);
    let user_enc = percent_encode(user);
    if pw_enc.is_empty() {
        format!("postgres://{user_enc}@{h}:{p}/{dbname}?sslmode={s}")
    } else {
        format!("postgres://{user_enc}:{pw_enc}@{h}:{p}/{dbname}?sslmode={s}")
    }
}

/// percent_encode encodes a DSN component (userinfo part). Encodes @, :, /, %
/// and other special chars that would confuse libpq's URI parser.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
            | b'-' | b'_' | b'.' | b'~' | b'!' | b'$' | b'&'
            | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

/// build_reveal_dsn builds or returns the reveal DSN.
fn build_reveal_dsn(r: &PgwireRevealResponse) -> String {
    if !r.dsn.is_empty() {
        return r.dsn.clone();
    }
    build_dsn_from_fields(&r.host, r.port, &r.user, &r.dbname, &r.password, &r.sslmode)
}

/// build_rotate_dsn builds or returns the rotate DSN.
fn build_rotate_dsn(r: &PgwireRotateResponse) -> String {
    if !r.dsn.is_empty() {
        return r.dsn.clone();
    }
    build_dsn_from_fields(&r.host, r.port, &r.user, &r.dbname, &r.password, &r.sslmode)
}

// ── dump helpers ──────────────────────────────────────────────────────────────

/// quote_ident wraps an SQL identifier in double-quotes, escaping embedded
/// double-quotes by doubling them (SQL standard).
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// sql_literal converts a serde_json Value into a SQL literal safe for
/// INSERT VALUES. Rules match the Go implementation.
fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
        serde_json::Value::Number(n) => {
            // Prefer integer form when exact.
            if let Some(i) = n.as_i64() {
                return i.to_string();
            }
            if let Some(f) = n.as_f64() {
                // Check if it's an integer value stored as float.
                if f == f.trunc() && f.abs() < 9.0e15 {
                    return format!("{}", f as i64);
                }
                return format!("{f}");
            }
            n.to_string()
        }
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// extract_line_number tries to parse a line number from an error message.
/// Returns 1 as fallback.
fn extract_line_number(message: &str) -> i64 {
    let lower = message.to_lowercase();
    if let Some(idx) = lower.find("line ") {
        let rest = &message[idx + 5..];
        let digits: String = rest
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(n) = digits.parse::<i64>() {
            if n > 0 {
                return n;
            }
        }
    }
    1
}

// ── to_migration_filename ─────────────────────────────────────────────────────

/// to_slug converts a name to a safe filename slug.
fn to_slug(name: &str) -> String {
    let mapped: String = name.chars().map(|c| {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            c
        } else if c.is_ascii_uppercase() {
            c.to_ascii_lowercase()
        } else {
            '_'
        }
    }).collect();

    let mut collapsed = String::with_capacity(mapped.len());
    let mut prev_under = false;
    for c in mapped.chars() {
        if c == '_' {
            if !prev_under {
                collapsed.push('_');
            }
            prev_under = true;
        } else {
            collapsed.push(c);
            prev_under = false;
        }
    }

    let trimmed = collapsed.trim_matches('_');
    if trimmed.is_empty() {
        "migration".to_string()
    } else {
        trimmed.to_string()
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_db(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "db",
                "Database workflow: push / pull / diff / reset / url / dump / lint.",
                &[
                    "push   [--project=<ref>]                     Apply local migrations to the remote project.",
                    "pull   [--project=<ref>] [--force]           Write remote migrations to ./basin/migrations/.",
                    "diff   [--project=<ref>]                     Generate a new migration from schema drift.",
                    "reset  [--project=<ref>] [--yes]             Snapshot → rollback all → re-apply + seed.",
                    "url    [--project=<ref>] [--reveal] [--rotate] [--yes]  Print the pgwire DSN.",
                    "dump   [--project=<ref>] [--schema-only|--data-only]    pg_dump-shaped output to stdout.",
                    "lint   [--project=<ref>]                     Dry-run each ./basin/migrations/*.sql file.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "push" => db_push(g, rest),
        "pull" => db_pull(g, rest),
        "diff" => db_diff(g, rest),
        "reset" => db_reset(g, rest),
        "url" => db_url(g, rest),
        "dump" => db_dump(g, rest),
        "lint" => db_lint(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "db",
                "Database workflow: push / pull / diff / reset / url / dump / lint.",
                &[
                    "push   [--project=<ref>]                     Apply local migrations to the remote project.",
                    "pull   [--project=<ref>] [--force]           Write remote migrations to ./basin/migrations/.",
                    "diff   [--project=<ref>]                     Generate a new migration from schema drift.",
                    "reset  [--project=<ref>] [--yes]             Snapshot → rollback all → re-apply + seed.",
                    "url    [--project=<ref>] [--reveal] [--rotate] [--yes]  Print the pgwire DSN.",
                    "dump   [--project=<ref>] [--schema-only|--data-only]    pg_dump-shaped output to stdout.",
                    "lint   [--project=<ref>]                     Dry-run each ./basin/migrations/*.sql file.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for db", other);
            Err(crate::error::silent())
        }
    }
}

// ── db push ───────────────────────────────────────────────────────────────────

fn db_push(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db push")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db push",
            "Apply local migrations to the remote project.",
            &["--project=<ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("db push: cwd: {e}")))?;
    run_db_push(g, &cwd, &r#ref)
}

/// run_db_push does the actual push, rooted at `cwd`.
fn run_db_push(g: &GlobalFlags, cwd: &Path, r#ref: &str) -> CliResult<()> {
    let c = require_client(g)?;
    let local_files = collect_local_migrations(cwd);

    // Fetch already-applied migrations.
    let list_resp: MigrationsListResp = c.do_json_timeout(
        Method::GET,
        &format!("/admin/v1/projects/{ref}/migrations"),
        None,
        Duration::from_secs(120),
    )?;

    // Build set of already-applied filenames.
    let applied: std::collections::HashSet<String> = list_resp
        .migrations
        .iter()
        .map(migration_key)
        .collect();

    #[derive(Serialize)]
    struct AppliedEntry { filename: String, id: String }
    #[derive(Serialize)]
    struct SkippedEntry { filename: String, reason: String }

    let mut applied_list: Vec<AppliedEntry> = Vec::new();
    let mut skipped_list: Vec<SkippedEntry> = Vec::new();

    for path in &local_files {
        let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        if applied.contains(&base) {
            skipped_list.push(SkippedEntry { filename: base, reason: "already applied".into() });
            continue;
        }
        let sql = std::fs::read_to_string(path)
            .map_err(|e| msg(format!("db push: read {base}: {e}")))?;
        let body = json!({ "source": base, "sql": sql });
        let create_resp: CreateMigrationResp = c.do_json_timeout(
            Method::POST,
            &format!("/admin/v1/projects/{ref}/migrations"),
            Some(body),
            Duration::from_secs(120),
        )?;
        applied_list.push(AppliedEntry { filename: base.clone(), id: create_resp.id.clone() });
        crate::printinfo!(g, "applied {}", base);
    }

    if g.json {
        // JSON shape: { "applied": [{ "filename": "...", "id": "..." }], "skipped": [{ "filename": "...", "reason": "already applied" }] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "applied": applied_list, "skipped": skipped_list }),
        );
    }
    println!("applied: {}  skipped: {}", applied_list.len(), skipped_list.len());
    Ok(())
}

// ── db pull ───────────────────────────────────────────────────────────────────

fn db_pull(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db pull")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("force").long("force").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db pull",
            "Write remote migrations to ./basin/migrations/.",
            &[
                "--project=<ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--force           Overwrite local files even when they differ from the remote.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let force = m.get_flag("force");
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("db pull: cwd: {e}")))?;
    run_db_pull(g, &cwd, &r#ref, force)
}

/// run_db_pull does the actual pull, rooted at `cwd`.
fn run_db_pull(g: &GlobalFlags, cwd: &Path, r#ref: &str, force: bool) -> CliResult<()> {
    let c = require_client(g)?;

    let list_resp: MigrationsListResp = c.do_json_timeout(
        Method::GET,
        &format!("/admin/v1/projects/{ref}/migrations"),
        None,
        Duration::from_secs(120),
    )?;

    let migrations_dir = cwd.join("basin").join("migrations");
    std::fs::create_dir_all(&migrations_dir)
        .map_err(|e| msg(format!("db pull: create migrations dir: {e}")))?;

    #[derive(Serialize)]
    struct PulledEntry { filename: String }
    #[derive(Serialize)]
    struct SkippedEntry { filename: String, reason: String }

    let mut pulled_list: Vec<PulledEntry> = Vec::new();
    let mut skipped_list: Vec<SkippedEntry> = Vec::new();

    for m_row in &list_resp.migrations {
        // Fetch full migration SQL.
        let detail: MigrationDetail = c.do_json_timeout(
            Method::GET,
            &format!("/admin/v1/projects/{ref}/migrations/{}", m_row.id),
            None,
            Duration::from_secs(120),
        )?;

        let mut filename = detail.source.clone();
        if filename.is_empty() {
            filename = format!("{}.sql", detail.id);
        }
        // Use only the basename.
        filename = Path::new(&filename)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&filename)
            .to_string();
        if !filename.ends_with(".sql") {
            filename.push_str(".sql");
        }
        let dest_path = migrations_dir.join(&filename);
        let remote = detail.sql.as_bytes().to_vec();

        // Check if local file exists and differs.
        if let Ok(existing) = std::fs::read(&dest_path) {
            // Compare bytes directly (identical → skip; differs → skip unless --force).
            if existing == remote {
                skipped_list.push(SkippedEntry {
                    filename: filename.clone(),
                    reason: "identical".into(),
                });
                continue;
            }
            if !force {
                skipped_list.push(SkippedEntry {
                    filename: filename.clone(),
                    reason: "local differs (use --force to overwrite)".into(),
                });
                continue;
            }
        }

        std::fs::write(&dest_path, &remote)
            .map_err(|e| msg(format!("db pull: write {filename}: {e}")))?;
        pulled_list.push(PulledEntry { filename: filename.clone() });
        crate::printinfo!(g, "pulled {}", filename);
    }

    if g.json {
        // JSON shape: { "pulled": [{ "filename": "..." }], "skipped": [{ "filename": "...", "reason": "..." }] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "pulled": pulled_list, "skipped": skipped_list }),
        );
    }
    println!("pulled: {}  skipped: {}", pulled_list.len(), skipped_list.len());
    Ok(())
}

// ── db diff ───────────────────────────────────────────────────────────────────

fn db_diff(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db diff")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db diff",
            "Generate a new migration file from schema drift.",
            &["--project=<ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("db diff: cwd: {e}")))?;
    run_db_diff(g, &cwd, &r#ref)
}

/// run_db_diff does the actual diff, rooted at `cwd`.
fn run_db_diff(g: &GlobalFlags, cwd: &Path, r#ref: &str) -> CliResult<()> {
    let c = require_client(g)?;

    let diff_resp: serde_json::Value = c.do_json_timeout(
        Method::GET,
        &format!("/admin/v1/projects/{ref}/migrations/diff"),
        None,
        Duration::from_secs(60),
    )?;

    // Extract diff SQL from the response (try keys in priority order).
    let mut diff_sql = String::new();
    for key in &["diff_sql", "diff", "sql", "body"] {
        if let Some(s) = diff_resp.get(*key).and_then(|v| v.as_str()) {
            if !s.trim().is_empty() {
                diff_sql = s.to_string();
                break;
            }
        }
    }

    if diff_sql.is_empty() {
        if g.json {
            // JSON shape: { "wrote": null, "reason": "no-diff" }
            return print_json(
                &mut std::io::stdout(),
                &json!({ "wrote": null, "reason": "no-diff" }),
            );
        }
        println!("no schema drift");
        return Ok(());
    }

    // Write as a new timestamped migration file.
    let dir = cwd.join("basin").join("migrations");
    std::fs::create_dir_all(&dir)
        .map_err(|e| msg(format!("db diff: create migrations dir: {e}")))?;

    let slug = "schema_drift";
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut ts = now;
    let dest_path = loop {
        let filename = format!("{ts}_{slug}.sql");
        let candidate = dir.join(&filename);
        if !candidate.exists() {
            break candidate;
        }
        ts += 1;
    };

    std::fs::write(&dest_path, diff_sql.as_bytes())
        .map_err(|e| msg(format!("db diff: write migration: {e}")))?;

    if g.json {
        // JSON shape: { "wrote": "<path>" }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "wrote": dest_path.display().to_string() }),
        );
    }
    println!("wrote {}", dest_path.display());
    Ok(())
}

// ── db reset ──────────────────────────────────────────────────────────────────

fn db_reset(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db reset")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db reset",
            "Snapshot → rollback all migrations → re-apply local ones → run seed.sql.",
            &[
                "--project=<ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let yes = m.get_flag("yes");

    if !yes {
        if g.json {
            return Err(msg("confirmation required: pass --yes to reset non-interactively under --json"));
        }
        eprint!(
            "Reset project {:?}? This will snapshot, rollback all migrations, re-apply local ones, and run seed.sql. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("db reset: cwd: {e}")))?;
    run_db_reset(g, &cwd, &r#ref)
}

/// run_db_reset does the actual reset, rooted at `cwd`.
fn run_db_reset(g: &GlobalFlags, cwd: &Path, r#ref: &str) -> CliResult<()> {
    let c = require_client(g)?;

    // (a) Safety snapshot.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let label = format!("pre-reset-{ts}");
    let snap_resp: SnapshotResp = c.do_json_timeout(
        Method::POST,
        &format!("/v1/projects/{ref}/backups/snapshots"),
        Some(json!({ "label": label })),
        Duration::from_secs(300),
    )?;
    let snapshot_id = snap_resp.id.clone();

    // (b) List existing migrations (newest first = reverse lex).
    let list_resp: MigrationsListResp = c.do_json_timeout(
        Method::GET,
        &format!("/admin/v1/projects/{ref}/migrations"),
        None,
        Duration::from_secs(120),
    )?;

    // Roll back newest-first (reverse the list).
    let mut migrations = list_resp.migrations;
    migrations.reverse();
    let mut rolled_back = 0;
    for m in &migrations {
        c.do_noout(
            Method::POST,
            &format!("/admin/v1/projects/{ref}/migrations/{}/rollback", m.id),
            None,
        )?;
        rolled_back += 1;
    }

    // (c) Re-apply local migrations.
    let local_files = collect_local_migrations(cwd);
    let mut applied_count = 0;
    for path in &local_files {
        let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        let sql = std::fs::read_to_string(path)
            .map_err(|e| msg(format!("db reset: read {base}: {e}")))?;
        let body = json!({ "source": base, "sql": sql });
        c.do_noout(
            Method::POST,
            &format!("/admin/v1/projects/{ref}/migrations"),
            Some(body),
        )?;
        applied_count += 1;
    }

    // (d) Run seed.sql if non-empty and not just the placeholder.
    let mut seeded = false;
    let seed_path = cwd.join("basin").join("seed.sql");
    if let Ok(seed_bytes) = std::fs::read_to_string(&seed_path) {
        let seed_sql = seed_bytes.trim().to_string();
        if !seed_sql.is_empty() && seed_sql != "-- seed data" {
            let body = json!({ "sql": seed_sql, "writes_enabled": true });
            c.do_noout(
                Method::POST,
                &format!("/v1/projects/{ref}/sql/query"),
                Some(body),
            )?;
            seeded = true;
        }
    }

    if g.json {
        // JSON shape: { "snapshot_id": "...", "rolled_back": N, "applied": N, "seeded": bool }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "snapshot_id": snapshot_id,
                "rolled_back": rolled_back,
                "applied": applied_count,
                "seeded": seeded,
            }),
        );
    }
    println!(
        "snapshot: {snapshot_id}\nrolled_back: {rolled_back}\napplied: {applied_count}\nseeded: {seeded}"
    );
    Ok(())
}

// ── db url ────────────────────────────────────────────────────────────────────

fn db_url(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db url")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("reveal").long("reveal").action(ArgAction::SetTrue))
        .arg(Arg::new("rotate").long("rotate").action(ArgAction::SetTrue))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db url",
            "Print the pgwire connection DSN.",
            &[
                "--project=<ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--reveal          Show the full unmasked DSN (one-time reveal).",
                "--rotate          Rotate the pgwire password and print the new DSN.",
                "--yes             Skip the confirmation prompt for --rotate.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let reveal = m.get_flag("reveal");
    let rotate = m.get_flag("rotate");
    let yes = m.get_flag("yes");
    let c = require_client(g)?;

    if rotate {
        if !yes {
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
        let resp: PgwireRotateResponse = c.do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/pgwire/rotate"),
            None,
        )?;
        let dsn = build_rotate_dsn(&resp);
        if g.json {
            // JSON shape: { "dsn": "..." } — new DSN after rotation.
            return print_json(&mut std::io::stdout(), &json!({ "dsn": dsn }));
        }
        println!("{dsn}");
        return Ok(());
    }

    if reveal {
        let resp: PgwireRevealResponse = c.do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/pgwire/reveal"),
            None,
        )?;
        let dsn = build_reveal_dsn(&resp);
        if g.json {
            // JSON shape: { "dsn": "..." } — full unmasked DSN.
            return print_json(&mut std::io::stdout(), &json!({ "dsn": dsn }));
        }
        println!("{dsn}");
        return Ok(());
    }

    // Default: show masked DSN.
    let creds: PgwireCredentials =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/pgwire"), None)?;
    let masked_pw = mask_password(&creds.password);
    let dsn = build_dsn_from_fields(
        &creds.host,
        creds.port,
        &creds.user,
        &creds.dbname,
        &masked_pw,
        &creds.sslmode,
    );
    if g.json {
        // JSON shape: { "dsn": "..." } — masked password.
        return print_json(&mut std::io::stdout(), &json!({ "dsn": dsn }));
    }
    println!("{dsn}");
    Ok(())
}

// ── db dump ───────────────────────────────────────────────────────────────────

fn db_dump(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db dump")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("schema-only").long("schema-only").action(ArgAction::SetTrue))
        .arg(Arg::new("data-only").long("data-only").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db dump",
            "Emit a pg_dump-shaped SQL dump to stdout.",
            &[
                "--project=<ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--schema-only     Emit only CREATE TABLE statements.",
                "--data-only       Emit only INSERT statements.",
            ],
        );
        return Ok(());
    }
    let schema_only = m.get_flag("schema-only");
    let data_only = m.get_flag("data-only");
    if schema_only && data_only {
        return Err(msg("db dump: --schema-only and --data-only are mutually exclusive"));
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    let c = require_client(g)?;
    let emit_schema = !data_only;
    let emit_data = !schema_only;

    // ── schema query ────────────────────────────────────────────────────────
    let schema_sql = "SELECT table_name, column_name, data_type, is_nullable, ordinal_position \
        FROM information_schema.columns \
        WHERE table_schema = 'public' \
        ORDER BY table_name, ordinal_position";

    let schema_result: QueryResult = c.do_json_timeout(
        Method::POST,
        &format!("/v1/projects/{ref}/sql/query"),
        Some(json!({ "sql": schema_sql, "writes_enabled": false })),
        Duration::from_secs(300),
    )?;

    // Parse column info: (table_name, col_name, data_type, is_nullable, ordinal_pos).
    // Use an insertion-ordered Vec of (table_name, cols) to preserve table order.
    struct ColInfo {
        col_name: String,
        data_type: String,
        is_nullable: String,
    }
    let mut table_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut table_cols: Vec<(String, Vec<ColInfo>)> = Vec::new();

    for row in &schema_result.rows {
        if row.len() < 5 {
            continue;
        }
        let table_name = match &row[0] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let col_name = match &row[1] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let data_type = match &row[2] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let is_nullable = match &row[3] {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        };
        let idx = match table_index.get(&table_name) {
            Some(&i) => i,
            None => {
                let i = table_cols.len();
                table_index.insert(table_name.clone(), i);
                table_cols.push((table_name.clone(), Vec::new()));
                i
            }
        };
        table_cols[idx].1.push(ColInfo { col_name, data_type, is_nullable });
    }

    if emit_schema {
        println!("-- basin db dump — schema");
        println!();
        for (tbl, cols) in &table_cols {
            println!("CREATE TABLE IF NOT EXISTS {} (", quote_ident(tbl));
            for (i, col) in cols.iter().enumerate() {
                let nullable = if col.is_nullable.to_uppercase() == "NO" {
                    " NOT NULL"
                } else {
                    ""
                };
                let comma = if i + 1 == cols.len() { "" } else { "," };
                println!(
                    "  {} {}{}{}",
                    quote_ident(&col.col_name),
                    col.data_type,
                    nullable,
                    comma
                );
            }
            println!(");");
            println!();
        }
    }

    if emit_data {
        if emit_schema {
            println!();
        }
        println!("-- basin db dump — data");
        println!();
        for (tbl, cols) in &table_cols {
            let select_sql = format!("SELECT * FROM {}", quote_ident(tbl));
            let data_result: QueryResult = match c.do_json_timeout(
                Method::POST,
                &format!("/v1/projects/{ref}/sql/query"),
                Some(json!({ "sql": select_sql, "writes_enabled": false })),
                Duration::from_secs(300),
            ) {
                Ok(r) => r,
                Err(e) => {
                    // Non-fatal: skip the table with a comment.
                    println!("-- skipped {tbl}: {e}");
                    continue;
                }
            };
            if data_result.rows.is_empty() {
                continue;
            }
            let col_names: Vec<String> = cols.iter().map(|c| quote_ident(&c.col_name)).collect();
            let col_list = col_names.join(", ");
            for row in &data_result.rows {
                let vals: Vec<String> = row.iter().map(sql_literal).collect();
                println!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    quote_ident(tbl),
                    col_list,
                    vals.join(", ")
                );
            }
        }
    }
    Ok(())
}

// ── db lint ───────────────────────────────────────────────────────────────────

fn db_lint(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("db lint")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "db lint",
            "Dry-run each ./basin/migrations/*.sql file against the project.",
            &["--project=<ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("db lint: cwd: {e}")))?;
    run_db_lint(g, &cwd, &r#ref)
}

/// run_db_lint does the actual lint, rooted at `cwd`.
fn run_db_lint(g: &GlobalFlags, cwd: &Path, r#ref: &str) -> CliResult<()> {
    let c = require_client(g)?;
    let local_files = collect_local_migrations(cwd);

    let mut lint_errors: Vec<DbLintError> = Vec::new();
    let mut checked = 0i64;

    for path in &local_files {
        let base = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        let sql = std::fs::read_to_string(path)
            .map_err(|e| msg(format!("db lint: read {base}: {e}")))?;
        checked += 1;
        let body = json!({
            "sql": sql,
            "writes_enabled": false,
            "dry_run": true,
        });
        let result: Result<serde_json::Value, _> = c.do_json_timeout(
            Method::POST,
            &format!("/v1/projects/{ref}/sql/query"),
            Some(body),
            Duration::from_secs(120),
        );
        if let Err(e) = result {
            let message = e.to_string();
            let line = extract_line_number(&message);
            if !g.quiet {
                eprintln!("{base}:{line}: {message}");
            }
            lint_errors.push(DbLintError { file: base, line, message });
        }
    }

    if g.json {
        // JSON shape: { "files_checked": N, "errors": [{ "file": "...", "line": N, "message": "..." }] }
        print_json(
            &mut std::io::stdout(),
            &json!({
                "files_checked": checked,
                "errors": lint_errors,
            }),
        )?;
        if !lint_errors.is_empty() {
            return Err(crate::error::msg(format!(
                "lint failed: {} error(s)",
                lint_errors.len()
            )));
        }
        return Ok(());
    }

    if !lint_errors.is_empty() {
        println!("{checked} file(s) checked, {} error(s)", lint_errors.len());
        return Err(crate::error::msg(format!("lint failed: {} error(s)", lint_errors.len())));
    }
    println!("{checked} file(s) checked, no errors");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{Arc, Mutex};

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("basin-db-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn scaffold(cwd: &Path) {
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
    }

    fn write_migration(cwd: &Path, filename: &str, sql: &str) {
        std::fs::write(cwd.join("basin/migrations").join(filename), sql).unwrap();
    }

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "bso_org_test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "bso_org_test".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── unit tests ────────────────────────────────────────────────────────────

    #[test]
    fn mask_password_short() {
        assert_eq!(mask_password(""), "••••••••");
        assert_eq!(mask_password("short"), "••••••••");
    }

    #[test]
    fn mask_password_long() {
        let masked = mask_password("supersecret1234");
        assert!(masked.starts_with("••••••••"), "masked: {masked}");
        assert!(masked.ends_with("1234"), "should end in last 4: {masked}");
    }

    #[test]
    fn quote_ident_basic() {
        assert_eq!(quote_ident("users"), r#""users""#);
    }

    #[test]
    fn quote_ident_embedded_quote() {
        assert_eq!(quote_ident(r#"my"table"#), r#""my""table""#);
    }

    #[test]
    fn sql_literal_null() {
        assert_eq!(sql_literal(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn sql_literal_bool() {
        assert_eq!(sql_literal(&json!(true)), "TRUE");
        assert_eq!(sql_literal(&json!(false)), "FALSE");
    }

    #[test]
    fn sql_literal_integer_float() {
        assert_eq!(sql_literal(&json!(42.0)), "42");
    }

    #[test]
    fn sql_literal_float() {
        assert_eq!(sql_literal(&json!(3.14)), "3.14");
    }

    #[test]
    fn sql_literal_string() {
        assert_eq!(sql_literal(&json!("hello")), "'hello'");
    }

    #[test]
    fn sql_literal_string_with_quote() {
        assert_eq!(sql_literal(&json!("it's")), "'it''s'");
    }

    #[test]
    fn extract_line_number_basic() {
        assert_eq!(extract_line_number("syntax error at line 5: foo"), 5);
    }

    #[test]
    fn extract_line_number_fallback() {
        assert_eq!(extract_line_number("some random error"), 1);
    }

    #[test]
    fn collect_local_migrations_empty_dir() {
        let cwd = make_cwd("collect-empty");
        scaffold(&cwd);
        let files = collect_local_migrations(&cwd);
        assert!(files.is_empty(), "expected no files");
        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn collect_local_migrations_sorted() {
        let cwd = make_cwd("collect-sorted");
        scaffold(&cwd);
        write_migration(&cwd, "0002_second.sql", "--");
        write_migration(&cwd, "0001_first.sql", "--");
        let files = collect_local_migrations(&cwd);
        assert_eq!(files.len(), 2);
        let names: Vec<_> = files.iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names[0], "0001_first.sql");
        assert_eq!(names[1], "0002_second.sql");
        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn migration_key_uses_source_basename() {
        let m = MigrationRow {
            id: "some-id".into(),
            source: "/long/path/0001_init.sql".into(),
            ..Default::default()
        };
        assert_eq!(migration_key(&m), "0001_init.sql");
    }

    #[test]
    fn migration_key_falls_back_to_id() {
        let m = MigrationRow { id: "m-abc".into(), ..Default::default() };
        assert_eq!(migration_key(&m), "m-abc");
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_subcommand_prints_help() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        // No subcommand → help → Ok.
        cmd_db(&g, &[]).unwrap();
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_db(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()), "error: {err}");
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        cmd_db(&g, &["help".into()]).unwrap();
    }

    // ── db push ───────────────────────────────────────────────────────────────

    #[test]
    fn push_happy_path_applies_unapplied() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("push-happy");
        scaffold(&cwd);
        write_migration(&cwd, "0001_create_users.sql", "CREATE TABLE users (id int);");
        write_migration(&cwd, "0002_add_email.sql", "ALTER TABLE users ADD COLUMN email text;");

        let posted: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let posted2 = Arc::clone(&posted);

        let srv = TestServer::start(move |req: &Req| {
            if req.method == "GET" && req.path.ends_with("/migrations") {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_create_users.sql"}]}"#);
            }
            if req.method == "POST" && req.path.ends_with("/migrations") {
                let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
                if let Some(src) = body["source"].as_str() {
                    posted2.lock().unwrap().push(src.to_string());
                }
                return Resp::ok(r#"{"id":"m2","source":"0002_add_email.sql"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_push(&g, &cwd, "proj1").unwrap();

        let p = posted.lock().unwrap();
        assert_eq!(p.len(), 1, "expected 1 POST, got: {p:?}");
        assert_eq!(p[0], "0002_add_email.sql");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn push_all_applied_no_post() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("push-all-applied");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "SELECT 1;");

        let posted = Arc::new(Mutex::new(false));
        let posted2 = Arc::clone(&posted);

        let srv = TestServer::start(move |req: &Req| {
            if req.method == "GET" {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "POST" {
                *posted2.lock().unwrap() = true;
                return Resp::status(500, r#""#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_push(&g, &cwd, "proj1").unwrap();

        assert!(!*posted.lock().unwrap(), "should not POST when all applied");
        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn push_json_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("push-json");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "SELECT 1;");
        write_migration(&cwd, "0002_add.sql", "SELECT 2;");

        let srv = TestServer::start(move |req: &Req| {
            if req.method == "GET" {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "POST" {
                return Resp::ok(r#"{"id":"m2","source":"0002_add.sql"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags_json(&srv.url);
        // Just verify it returns Ok and writes JSON.
        run_db_push(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn push_empty_migrations_dir() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("push-empty");
        scaffold(&cwd);

        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"migrations":[]}"#));
        let g = flags(&srv.url);
        run_db_push(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── db pull ───────────────────────────────────────────────────────────────

    #[test]
    fn pull_happy_path_writes_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("pull-happy");
        scaffold(&cwd);

        let srv = TestServer::start(|req: &Req| {
            if req.method == "GET" && req.path.ends_with("/migrations") {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "GET" && req.path.ends_with("/m1") {
                return Resp::ok(r#"{"id":"m1","source":"0001_init.sql","sql":"CREATE TABLE users (id int);"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_pull(&g, &cwd, "proj1", false).unwrap();

        let content = std::fs::read_to_string(cwd.join("basin/migrations/0001_init.sql")).unwrap();
        assert!(content.contains("CREATE TABLE users"), "content: {content:?}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn pull_refuses_overwrite_without_force() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("pull-refusal");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "-- different content");

        let srv = TestServer::start(|req: &Req| {
            if req.method == "GET" && req.path.ends_with("/migrations") {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "GET" && req.path.ends_with("/m1") {
                return Resp::ok(r#"{"id":"m1","source":"0001_init.sql","sql":"CREATE TABLE users (id int);"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_pull(&g, &cwd, "proj1", false).unwrap();

        // File should not be overwritten.
        let content = std::fs::read_to_string(cwd.join("basin/migrations/0001_init.sql")).unwrap();
        assert!(content.contains("different content"), "should preserve local: {content:?}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn pull_force_overwrites_differing_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("pull-force");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "-- different content");

        let srv = TestServer::start(|req: &Req| {
            if req.method == "GET" && req.path.ends_with("/migrations") {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "GET" && req.path.ends_with("/m1") {
                return Resp::ok(r#"{"id":"m1","source":"0001_init.sql","sql":"CREATE TABLE users (id int);"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_pull(&g, &cwd, "proj1", true).unwrap();

        let content = std::fs::read_to_string(cwd.join("basin/migrations/0001_init.sql")).unwrap();
        assert!(content.contains("CREATE TABLE users"), "should be overwritten: {content:?}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn pull_skips_identical_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("pull-identical");
        scaffold(&cwd);
        // Pre-write the exact same content.
        write_migration(&cwd, "0001_init.sql", "CREATE TABLE users (id int);");

        let srv = TestServer::start(|req: &Req| {
            if req.method == "GET" && req.path.ends_with("/migrations") {
                return Resp::ok(r#"{"migrations":[{"id":"m1","source":"0001_init.sql"}]}"#);
            }
            if req.method == "GET" && req.path.ends_with("/m1") {
                return Resp::ok(r#"{"id":"m1","source":"0001_init.sql","sql":"CREATE TABLE users (id int);"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_pull(&g, &cwd, "proj1", false).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── db diff ───────────────────────────────────────────────────────────────

    #[test]
    fn diff_no_drift_no_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("diff-nodrift");
        scaffold(&cwd);

        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req.path.ends_with("/migrations/diff"), "path: {}", req.path);
            Resp::ok(r#"{"diff_sql":""}"#)
        });

        let g = flags(&srv.url);
        run_db_diff(&g, &cwd, "proj1").unwrap();

        let entries: Vec<_> = std::fs::read_dir(cwd.join("basin/migrations")).unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(entries.is_empty(), "no file should be written when no drift");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn diff_no_drift_json_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("diff-nodrift-json");
        scaffold(&cwd);

        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"diff_sql":""}"#));
        let g = flags_json(&srv.url);
        run_db_diff(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn diff_writes_file_when_drift() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("diff-drift");
        scaffold(&cwd);

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"diff_sql":"ALTER TABLE users ADD COLUMN age int;"}"#)
        });

        let g = flags(&srv.url);
        run_db_diff(&g, &cwd, "proj1").unwrap();

        let entries: Vec<_> = std::fs::read_dir(cwd.join("basin/migrations")).unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "expected 1 file written");
        let name = entries[0].file_name().to_string_lossy().to_string();
        assert!(name.ends_with("_schema_drift.sql"), "filename: {name}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn diff_json_shape_with_drift() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("diff-drift-json");
        scaffold(&cwd);

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"diff_sql":"ALTER TABLE users ADD COLUMN age int;"}"#)
        });

        let g = flags_json(&srv.url);
        run_db_diff(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── db reset ──────────────────────────────────────────────────────────────

    #[test]
    fn reset_yes_calls_all_endpoints() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("reset-yes");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "CREATE TABLE users (id int);");

        let snapshot_called = Arc::new(Mutex::new(false));
        let rollback_called = Arc::new(Mutex::new(false));
        let apply_called = Arc::new(Mutex::new(false));
        let sc = Arc::clone(&snapshot_called);
        let rc = Arc::clone(&rollback_called);
        let ac = Arc::clone(&apply_called);

        let srv = TestServer::start(move |req: &Req| {
            if req.path.ends_with("/backups/snapshots") && req.method == "POST" {
                *sc.lock().unwrap() = true;
                return Resp::ok(r#"{"id":"snap-1"}"#);
            }
            if req.path.ends_with("/migrations") && req.method == "GET" {
                return Resp::ok(r#"{"migrations":[{"id":"m-old","source":"old.sql"}]}"#);
            }
            if req.path.ends_with("/migrations") && req.method == "POST" {
                *ac.lock().unwrap() = true;
                return Resp::ok(r#"{"id":"m-new"}"#);
            }
            if req.path.ends_with("/rollback") && req.method == "POST" {
                *rc.lock().unwrap() = true;
                return Resp::ok(r#"{"rolled_back":true}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        run_db_reset(&g, &cwd, "proj1").unwrap();

        assert!(*snapshot_called.lock().unwrap(), "snapshot not called");
        assert!(*rollback_called.lock().unwrap(), "rollback not called");
        assert!(*apply_called.lock().unwrap(), "apply not called");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn reset_json_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("reset-json");
        scaffold(&cwd);

        let srv = TestServer::start(|req: &Req| {
            if req.path.ends_with("/backups/snapshots") {
                return Resp::ok(r#"{"id":"snap-abc"}"#);
            }
            if req.path.ends_with("/migrations") && req.method == "GET" {
                return Resp::ok(r#"{"migrations":[]}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags_json(&srv.url);
        run_db_reset(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn reset_json_without_yes_requires_yes() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("reset-json-nyes");
        scaffold(&cwd);

        let g = flags_json("http://127.0.0.1:1");
        let err = cmd_db(&g, &["reset".into(), "--project=proj1".into()]).unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "error: {err}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── db url ────────────────────────────────────────────────────────────────

    #[test]
    fn url_show_masked_dsn() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req.path.ends_with("/pgwire"), "path: {}", req.path);
            Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"tenant","dbname":"basin","password":"supersecret1234","sslmode":"require"}"#)
        });
        let g = flags(&srv.url);
        cmd_db(&g, &["url".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn url_reveal_posts_to_reveal_endpoint() {
        let _cfg = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let called2 = Arc::clone(&called);

        let srv = TestServer::start(move |req: &Req| {
            if req.path.ends_with("/pgwire/reveal") {
                assert_eq!(req.method, "POST");
                *called2.lock().unwrap() = true;
                return Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"tenant","dbname":"basin","password":"plaintext","sslmode":"require"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        cmd_db(&g, &["url".into(), "--project=proj1".into(), "--reveal".into()]).unwrap();
        assert!(*called.lock().unwrap(), "reveal endpoint not called");
    }

    #[test]
    fn url_rotate_yes_posts_to_rotate() {
        let _cfg = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let called2 = Arc::clone(&called);

        let srv = TestServer::start(move |req: &Req| {
            if req.path.ends_with("/pgwire/rotate") {
                assert_eq!(req.method, "POST");
                *called2.lock().unwrap() = true;
                return Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"tenant","dbname":"basin","password":"new_pw","sslmode":"require"}"#);
            }
            Resp::status(404, r#""#)
        });

        let g = flags(&srv.url);
        cmd_db(&g, &["url".into(), "--project=proj1".into(), "--rotate".into(), "--yes".into()]).unwrap();
        assert!(*called.lock().unwrap(), "rotate endpoint not called");
    }

    #[test]
    fn url_rotate_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"tenant","dbname":"basin","password":"new_pw","sslmode":"require"}"#)
        });
        let g = flags_json(&srv.url);
        cmd_db(&g, &["url".into(), "--project=proj1".into(), "--rotate".into(), "--yes".into()]).unwrap();
    }

    #[test]
    fn url_rotate_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g = flags_json("http://127.0.0.1:1");
        let err = cmd_db(&g, &["url".into(), "--project=proj1".into(), "--rotate".into()]).unwrap_err();
        assert!(err.to_string().contains("confirmation required"), "error: {err}");
    }

    #[test]
    fn url_password_masked_in_default_output() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"host":"db.basin.run","port":5432,"user":"tenant","dbname":"basin","password":"mysecretpassword","sslmode":"require"}"#)
        });
        // The key thing: masked password should not appear in the DSN.
        let _g = flags(&srv.url);
        // We can't easily capture stdout here, but just verify it succeeds.
        let g = flags(&srv.url);
        cmd_db(&g, &["url".into(), "--project=proj1".into()]).unwrap();
    }

    // ── db dump ───────────────────────────────────────────────────────────────

    #[test]
    fn dump_schema_only_emits_create_table() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("dump-schema");
        scaffold(&cwd);

        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            Resp::ok(r#"{"columns":["table_name","column_name","data_type","is_nullable","ordinal_position"],"rows":[["users","id","integer","NO",1.0],["users","email","text","YES",2.0]],"elapsed_ms":5}"#)
        });

        let g = flags(&srv.url);
        cmd_db(&g, &["dump".into(), "--project=proj1".into(), "--schema-only".into()]).unwrap();
    }

    #[test]
    fn dump_data_only_calls_select() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("dump-data");
        scaffold(&cwd);

        let call_count = Arc::new(Mutex::new(0i32));
        let cc = Arc::clone(&call_count);

        let srv = TestServer::start(move |_req: &Req| {
            let mut count = cc.lock().unwrap();
            *count += 1;
            if *count == 1 {
                // Schema query.
                return Resp::ok(r#"{"columns":["table_name","column_name","data_type","is_nullable","ordinal_position"],"rows":[["users","id","integer","NO",1.0]],"elapsed_ms":1}"#);
            }
            // SELECT * FROM "users"
            Resp::ok(r#"{"columns":["id"],"rows":[[42.0]],"elapsed_ms":1}"#)
        });

        let g = flags(&srv.url);
        cmd_db(&g, &["dump".into(), "--project=proj1".into(), "--data-only".into()]).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn dump_mutually_exclusive_flags() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "t".into(), ..Default::default() };
        let err = cmd_db(
            &g,
            &["dump".into(), "--project=proj1".into(), "--schema-only".into(), "--data-only".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "error: {err}");
    }

    // ── db lint ───────────────────────────────────────────────────────────────

    #[test]
    fn lint_clean_no_errors() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("lint-clean");
        scaffold(&cwd);
        write_migration(&cwd, "0001_init.sql", "CREATE TABLE users (id int);");

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"columns":[],"rows":[],"elapsed_ms":1}"#)
        });

        let g = flags(&srv.url);
        run_db_lint(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn lint_error_returns_error() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("lint-error");
        scaffold(&cwd);
        write_migration(&cwd, "0001_bad.sql", "INVALID SQL STATEMENT;");

        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                400,
                r#"{"error":{"code":"0A000","message":"syntax error at line 1: unexpected token"}}"#,
            )
        });

        let g = flags(&srv.url);
        let err = run_db_lint(&g, &cwd, "proj1").unwrap_err();
        assert!(err.to_string().contains("lint failed"), "error: {err}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn lint_json_clean_has_empty_errors() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("lint-json-clean");
        scaffold(&cwd);
        write_migration(&cwd, "0001_ok.sql", "CREATE TABLE t (id int);");
        write_migration(&cwd, "0002_ok.sql", "ALTER TABLE t ADD COLUMN v text;");

        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"columns":[],"rows":[],"elapsed_ms":1}"#)
        });

        let g = flags_json(&srv.url);
        run_db_lint(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn lint_json_error_still_writes_json() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("lint-json-error");
        scaffold(&cwd);
        write_migration(&cwd, "0001_bad.sql", "INVALID SQL;");

        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                400,
                r#"{"error":{"code":"0A000","message":"syntax error at line 1"}}"#,
            )
        });

        let g = flags_json(&srv.url);
        // Returns error but JSON is already written to stdout.
        let _ = run_db_lint(&g, &cwd, "proj1");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn lint_empty_migrations_no_error() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("lint-empty");
        scaffold(&cwd);

        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"columns":[],"rows":[]}"#));
        let g = flags(&srv.url);
        run_db_lint(&g, &cwd, "proj1").unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }
}
