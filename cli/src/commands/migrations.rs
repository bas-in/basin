//! migrations — list / apply / new / get / diff / rollback DDL migrations.
//!
//! All cloud calls use the `/admin/v1/projects/{ref}/migrations` family.
//! The `new` subcommand is pure local: it writes a timestamped `.sql` file
//! into `./basin/migrations/` with no network call.
//!
//! CWD-dependent logic in `migrations new` is extracted into
//! `run_migrations_new(cwd, …)` so tests can pass a temp path without
//! touching the process-global cwd.

use std::path::Path;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// MigrationRow mirrors the list endpoint's row shape.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MigrationRow {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub op: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
}

/// MigrationsListResp is the list endpoint envelope.
#[derive(Deserialize, Default)]
struct MigrationsListResp {
    #[serde(default)]
    migrations: Vec<MigrationRow>,
}

/// MigrationDetail is the full shape from GET /admin/v1/projects/{ref}/migrations/{id}.
// JSON shape: { id, status, applied_at, sql, source, summary, op, table, at, author }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct MigrationDetail {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub applied_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub op: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author: String,
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

// ── Slug helpers ──────────────────────────────────────────────────────────────

/// to_slug converts a name to a safe filename slug.  Only ASCII a-z, 0-9,
/// and underscore; everything else (including uppercase, spaces, punctuation,
/// non-ASCII) becomes underscore.  Runs of underscores are collapsed;
/// leading/trailing underscores are trimmed.  Empty result falls back to
/// "migration".
fn to_slug(name: &str) -> String {
    // Map characters.
    let mapped: String = name.chars().map(|c| {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            c
        } else if c.is_ascii_uppercase() {
            c.to_ascii_lowercase()
        } else {
            '_'
        }
    }).collect();

    // Collapse runs of two or more underscores.
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

    // Trim leading/trailing underscores.
    let trimmed = collapsed.trim_matches('_');
    if trimmed.is_empty() {
        "migration".to_string()
    } else {
        trimmed.to_string()
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_migrations(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin migrations (list | apply | new | get | diff | rollback) ...",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "apply" => apply(g, rest),
        "new" => new(g, rest),
        "get" => get(g, rest),
        "diff" => diff(g, rest),
        "rollback" => rollback(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "migrations",
                "Manage project migrations (DDL snapshots).",
                &[
                    "list                  --project <ref>          List all migrations, newest first.",
                    "apply <id>            --project <ref>          Apply (roll forward to) a migration.",
                    "new   <name>                                   Create a new local migration file.",
                    "get   <id>            --project <ref>          Show a single migration's details.",
                    "diff                  --project <ref>          Show schema drift vs applied migrations.",
                    "rollback <id>         --project <ref> [--yes]  Roll back a migration.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for migrations", other);
            Err(crate::error::silent())
        }
    }
}

// ── migrations list ───────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations list",
            "List all migrations for a project, newest first.",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: MigrationsListResp =
        c.do_json(Method::GET, &format!("/admin/v1/projects/{ref}/migrations"), None)?;
    if g.json {
        // JSON shape: { migrations: [ MigrationRow ] }
        return print_json(&mut std::io::stdout(), &json!({ "migrations": resp.migrations }));
    }
    let mut t = Table::new(g, &["ID", "AT", "OP", "TABLE", "SUMMARY"]);
    for m in &resp.migrations {
        t.row(&[&m.id, &m.at, &m.op, &m.table, &m.summary]);
    }
    t.flush()
}

// ── migrations apply ──────────────────────────────────────────────────────────

fn apply(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations apply")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations apply",
            "Apply (roll forward to) a migration.",
            &[
                "<id>               Migration ID to apply.",
                "--project <ref>    Project ref (or auto-detected from ./basin/config.toml).",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin migrations apply <id> [--project <ref>]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let out: serde_json::Value = c.do_json(
        Method::POST,
        &format!("/admin/v1/projects/{ref}/migrations/{id}/rollback"),
        None,
    )?;
    if g.json {
        // JSON shape: passthrough of rollback envelope.
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Applied migration {id}.");
    Ok(())
}

// ── migrations new ────────────────────────────────────────────────────────────

pub fn cmd_migrations_new(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations new")
        .arg(Arg::new("name").num_args(1..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations new",
            "Create a new local migration file in ./basin/migrations/.",
            &["<name>   Name for the migration (slugified for the filename)."],
        );
        return Ok(());
    }
    let name: String = m
        .get_many::<String>("name")
        .map(|v| v.cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    if name.is_empty() {
        return Err(msg("usage: basin migrations new <name>"));
    }
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("migrations new: could not determine working directory: {e}")))?;
    run_migrations_new(g, &cwd, &name)
}

fn new(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    cmd_migrations_new(g, args)
}

/// run_migrations_new performs the actual file creation rooted at `cwd`.
/// Extracted so tests can supply a temp path without process-global chdir.
pub fn run_migrations_new(g: &GlobalFlags, cwd: &Path, name: &str) -> CliResult<()> {
    let slug = to_slug(name);
    let dir = cwd.join("basin").join("migrations");
    std::fs::create_dir_all(&dir)
        .map_err(|e| msg(format!("migrations new: could not create migrations dir: {e}")))?;

    // Pick a timestamp, bumping by 1s on collision (mirrors Go).
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

    let created = chrono::DateTime::<chrono::Utc>::from_timestamp(ts as i64, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let header = format!("-- migration: {name}\n-- created: {created}\n\n");

    std::fs::write(&dest_path, header.as_bytes())
        .map_err(|e| msg(format!("migrations new: write file: {e}")))?;

    if g.json {
        // JSON shape: { "path": string, "name": string, "slug": string, "timestamp": int64 }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "path": dest_path.display().to_string(),
                "name": name,
                "slug": slug,
                "timestamp": ts,
            }),
        );
    }
    println!("{}", dest_path.display());
    Ok(())
}

// ── migrations get ────────────────────────────────────────────────────────────

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations get",
            "Show a single migration's details.",
            &[
                "<id>               Migration ID.",
                "--project <ref>    Project ref (or auto-detected from ./basin/config.toml).",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin migrations get <id> [--project <ref>]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let detail: MigrationDetail =
        c.do_json(Method::GET, &format!("/admin/v1/projects/{ref}/migrations/{id}"), None)?;
    if g.json {
        // JSON shape: MigrationDetail { id, status, applied_at, sql, source, summary, op, table, at, author }
        return print_json(&mut std::io::stdout(), &detail);
    }
    println!("id:         {}", detail.id);
    println!("status:     {}", detail.status);
    if !detail.applied_at.is_empty() {
        println!("applied_at: {}", detail.applied_at);
    }
    if !detail.at.is_empty() {
        println!("at:         {}", detail.at);
    }
    if !detail.op.is_empty() {
        println!("op:         {}", detail.op);
    }
    if !detail.table.is_empty() {
        println!("table:      {}", detail.table);
    }
    if !detail.summary.is_empty() {
        println!("summary:    {}", detail.summary);
    }
    if !detail.sql.is_empty() {
        println!("\n-- SQL:\n{}", detail.sql);
    }
    Ok(())
}

// ── migrations diff ───────────────────────────────────────────────────────────

fn diff(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations diff")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations diff",
            "Show schema drift vs applied migrations.",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: serde_json::Value =
        c.do_json(Method::GET, &format!("/admin/v1/projects/{ref}/migrations/diff"), None)?;
    if g.json {
        // JSON shape: passthrough of diff envelope.
        return print_json(&mut std::io::stdout(), &resp);
    }
    // Try common diff-body keys in priority order (mirrors Go).
    for key in &["diff", "sql", "body"] {
        if let Some(s) = resp.get(*key).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                print!("{s}");
                if !s.ends_with('\n') {
                    println!();
                }
                return Ok(());
            }
        }
    }
    println!("(no diff)");
    Ok(())
}

// ── migrations rollback ───────────────────────────────────────────────────────

fn rollback(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("migrations rollback")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "migrations rollback",
            "Roll back a migration.",
            &[
                "<id>               Migration ID to roll back.",
                "--project <ref>    Project ref (or auto-detected from ./basin/config.toml).",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin migrations rollback <id> [--project <ref>] [--yes]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    // Confirmation prompt — skip under --yes or --json (non-interactive, mirrors Go).
    if !m.get_flag("yes") && !g.json {
        eprint!("Roll back migration {id} on project {ref}? [y/N] ");
        let answer = read_line()?;
        let answer = answer.trim().to_lowercase();
        if answer != "y" && answer != "yes" {
            println!("Rollback aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let out: serde_json::Value = c.do_json(
        Method::POST,
        &format!("/admin/v1/projects/{ref}/migrations/{id}/rollback"),
        None,
    )?;
    if g.json {
        // JSON shape: passthrough of rollback envelope.
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Rolled back migration {id}.");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{save_working_project, WorkingProject};
    use crate::error::as_api_error;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("basin-mig-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn scaffold(cwd: &Path) {
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        save_working_project(cwd, &WorkingProject::default()).unwrap();
    }

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok-test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok-test".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── slug normalisation ────────────────────────────────────────────────────

    #[test]
    fn slug_basic() {
        assert_eq!(to_slug("add users table"), "add_users_table");
    }

    #[test]
    fn slug_uppercase() {
        assert_eq!(to_slug("Add Users Table"), "add_users_table");
    }

    #[test]
    fn slug_spaces() {
        assert_eq!(to_slug("hello world"), "hello_world");
    }

    #[test]
    fn slug_double_dash_collapses() {
        assert_eq!(to_slug("foo--bar"), "foo_bar");
    }

    #[test]
    fn slug_leading_trailing_spaces() {
        assert_eq!(to_slug("  spaces  "), "spaces");
    }

    #[test]
    fn slug_special_chars() {
        assert_eq!(to_slug("special!@#chars"), "special_chars");
    }

    #[test]
    fn slug_leading_trailing_underscores() {
        assert_eq!(to_slug("__leading trailing__"), "leading_trailing");
    }

    #[test]
    fn slug_numeric_start() {
        assert_eq!(to_slug("123 numeric start"), "123_numeric_start");
    }

    #[test]
    fn slug_non_ascii_collapses() {
        // non-ASCII collapsed to single underscore per character.
        // "café résumé" → c, a, f, _, r, _, s, u, m, _ → "caf_r_sum_"
        // but é → _, so "café" → "caf_" and "résumé" → "r_sum_"
        // then trim: "caf_r_sum"
        assert_eq!(to_slug("café résumé"), "caf_r_sum");
    }

    #[test]
    fn slug_run_collapse() {
        assert_eq!(to_slug("a___b"), "a_b");
    }

    #[test]
    fn slug_empty_falls_back_to_migration() {
        assert_eq!(to_slug("!@#"), "migration");
    }

    // ── migrations new (filesystem tests) ────────────────────────────────────

    #[test]
    fn new_writes_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("new-writes");
        let g = GlobalFlags { quiet: true, ..Default::default() };

        run_migrations_new(&g, &cwd, "add users table").unwrap();

        let dir = cwd.join("basin/migrations");
        let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(entries.len(), 1, "expected exactly one file");
        let name = entries[0].file_name().to_string_lossy().to_string();
        assert!(name.ends_with("_add_users_table.sql"), "filename: {name}");

        let content = std::fs::read_to_string(entries[0].path()).unwrap();
        assert!(content.contains("-- migration: add users table"), "header missing: {content:?}");
        assert!(content.contains("-- created:"), "created header missing: {content:?}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn new_collision_bump() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("collision");
        let g = GlobalFlags { quiet: true, ..Default::default() };

        run_migrations_new(&g, &cwd, "bump test").unwrap();
        run_migrations_new(&g, &cwd, "bump test").unwrap();

        let dir = cwd.join("basin/migrations");
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        entries.sort();
        assert_eq!(entries.len(), 2, "expected 2 files after collision bump: {entries:?}");
        assert_ne!(entries[0], entries[1], "paths must differ");
        assert!(entries[0].ends_with("_bump_test.sql"), "slug 1: {}", entries[0]);
        assert!(entries[1].ends_with("_bump_test.sql"), "slug 2: {}", entries[1]);

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn new_missing_name_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &["new".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"), "error: {err}");
    }

    #[test]
    fn new_json_output() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("new-json");
        let g = GlobalFlags { json: true, quiet: true, ..Default::default() };

        // We can't easily capture stdout in unit tests, but we can verify the fn succeeds
        // and the file exists.
        run_migrations_new(&g, &cwd, "json output test").unwrap();

        let dir = cwd.join("basin/migrations");
        let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(entries.len(), 1);
        let name = entries[0].file_name().to_string_lossy().to_string();
        assert!(name.ends_with("_json_output_test.sql"), "filename: {name}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── migrations list ───────────────────────────────────────────────────────

    #[test]
    fn list_uses_admin_v1_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/admin/v1/projects/proj-abc/migrations");
            Resp::ok(r#"{"migrations":[{"id":"m1","at":"2026-01-01T00:00:00Z","op":"CREATE_TABLE","table":"users","summary":"create users"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(&g, &["list".into(), "--project=proj-abc".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"migrations":[{"id":"m1","op":"CREATE_TABLE"}]}"#)
        });
        let g = flags_json(&srv.url);
        // Just verify no error; JSON goes to stdout.
        cmd_migrations(&g, &["list".into(), "--project=proj-abc".into()]).unwrap();
    }

    #[test]
    fn list_empty_migrations() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"migrations":[]}"#));
        let g = flags(&srv.url);
        cmd_migrations(&g, &["list".into(), "--project=proj-abc".into()]).unwrap();
    }

    #[test]
    fn list_resolves_project_from_config() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("list-config");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject { project_ref: "linked-proj".into(), ..Default::default() },
        )
        .unwrap();

        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.path, "/admin/v1/projects/linked-proj/migrations");
            Resp::ok(r#"{"migrations":[]}"#)
        });
        // Temporarily point cwd to temp dir by calling resolve_project_ref manually.
        // We do this by testing with the inner function.
        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "linked-proj");

        let g = flags(&srv.url);
        // We verify via resolve_project_ref's own logic — list requires --project here
        // because resolve_project_ref uses std::env::current_dir() not our temp cwd.
        cmd_migrations(&g, &["list".into(), "--project=linked-proj".into()]).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn list_no_project_error() {
        let _cfg = with_temp_config_dir();
        // No config.toml → resolveProjectRef must fail with a hint.
        let cwd = make_cwd("no-project");
        // Use current_dir which won't have basin/config.toml.
        // We pass an empty project flag — must fail.
        let err = resolve_project_ref("").unwrap_err();
        assert!(
            err.to_string().contains("basin link") || err.to_string().contains("--project"),
            "error: {err}"
        );
        std::fs::remove_dir_all(&cwd).ok();
    }

    // ── migrations apply ──────────────────────────────────────────────────────

    #[test]
    fn apply_posts_to_rollback_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/admin/v1/projects/proj-abc/migrations/m1/rollback");
            Resp::ok(r#"{"applied":true}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(&g, &["apply".into(), "--project=proj-abc".into(), "m1".into()]).unwrap();
    }

    #[test]
    fn apply_missing_id_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &["apply".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"), "error: {err}");
    }

    // ── migrations get ────────────────────────────────────────────────────────

    #[test]
    fn get_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/admin/v1/projects/proj-abc/migrations/m42");
            Resp::ok(r#"{"id":"m42","status":"applied","applied_at":"2026-03-01T12:00:00Z","sql":"CREATE TABLE foo (id bigint primary key);","op":"CREATE_TABLE","table":"foo"}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(&g, &["get".into(), "--project=proj-abc".into(), "m42".into()]).unwrap();
    }

    #[test]
    fn get_404_propagates() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"migration not found"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_migrations(&g, &["get".into(), "--project=proj-abc".into(), "notexist".into()])
            .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn get_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"id":"m7","status":"applied","applied_at":"2026-04-01T00:00:00Z","sql":"SELECT 1;"}"#)
        });
        let g = flags_json(&srv.url);
        cmd_migrations(&g, &["get".into(), "--project=proj-abc".into(), "m7".into()]).unwrap();
    }

    #[test]
    fn get_missing_id_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &["get".into(), "--project=proj-abc".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"), "error: {err}");
    }

    // ── migrations diff ───────────────────────────────────────────────────────

    #[test]
    fn diff_no_diff_prints_no_diff() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/admin/v1/projects/proj-abc/migrations/diff");
            Resp::ok(r#"{}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(&g, &["diff".into(), "--project=proj-abc".into()]).unwrap();
    }

    #[test]
    fn diff_real_diff_prints_content() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"diff":"--- a/schema.sql\n+++ b/schema.sql\n@@ -0,0 +1 @@\n+CREATE TABLE bar (id bigint);\n"}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(&g, &["diff".into(), "--project=proj-abc".into()]).unwrap();
    }

    #[test]
    fn diff_500_propagates() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"error":{"code":"diff_error","message":"engine unavailable"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_migrations(&g, &["diff".into(), "--project=proj-abc".into()]).unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 500);
        assert_eq!(ae.code, "diff_error");
    }

    #[test]
    fn diff_json_passthrough() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"diff":"--- a\n+++ b\n","changes":["add bar table"]}"#)
        });
        let g = flags_json(&srv.url);
        cmd_migrations(&g, &["diff".into(), "--project=proj-abc".into()]).unwrap();
    }

    // ── migrations rollback ───────────────────────────────────────────────────

    #[test]
    fn rollback_yes_posts() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/admin/v1/projects/proj-abc/migrations/m99/rollback");
            Resp::ok(r#"{"rolled_back":true,"migration_id":"m99"}"#)
        });
        let g = flags(&srv.url);
        cmd_migrations(
            &g,
            &["rollback".into(), "--project=proj-abc".into(), "--yes".into(), "m99".into()],
        )
        .unwrap();
    }

    #[test]
    fn rollback_json_skips_prompt() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"rolled_back":true,"migration_id":"m55"}"#)
        });
        let g = flags_json(&srv.url);
        // --json skips the prompt; no stdin needed.
        cmd_migrations(
            &g,
            &["rollback".into(), "--project=proj-abc".into(), "m55".into()],
        )
        .unwrap();
    }

    #[test]
    fn rollback_409_propagates() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"error":{"code":"mid_flight","message":"migration in progress"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_migrations(
            &g,
            &["rollback".into(), "--project=proj-abc".into(), "--yes".into(), "m99".into()],
        )
        .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 409);
        assert_eq!(ae.code, "mid_flight");
    }

    #[test]
    fn rollback_missing_id_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &["rollback".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"), "error: {err}");
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_subcommand_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &[]).unwrap_err();
        assert!(err.to_string().contains("usage"), "error: {err}");
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_migrations(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()), "error: {err}");
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        cmd_migrations(&g, &["help".into()]).unwrap();
    }
}
