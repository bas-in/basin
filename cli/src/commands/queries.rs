//! queries — saved query lifecycle + query history.
//!
//!   basin queries save   --name=<n> --sql=<s> [--description=<d>] [--public=<bool>] [--project=<ref>]
//!   basin queries list   [--project=<ref>]
//!   basin queries get    <id>
//!   basin queries fork   <id> [--name=<n>] [--project=<ref>]
//!   basin queries delete <id> [--project=<ref>] [--yes]
//!   basin queries history [--project=<ref>] [--limit=N] [--clear --yes]
//!
//! Cloud routes:
//!   - POST   /v1/projects/{ref}/sql/saved              — save
//!   - GET    /v1/projects/{ref}/sql/saved              — list
//!   - GET    /v1/sql/saved/{id}                        — get (top-level, not project-scoped)
//!   - DELETE /v1/sql/saved/{id}                        — delete (top-level)
//!   - POST   /v1/sql/saved/{id}/fork                   — fork (top-level)
//!   - GET    /v1/projects/{ref}/sql/history            — history list
//!   - DELETE /v1/projects/{ref}/sql/history            — history clear
//!
//! Note: get/delete/fork are mounted at the top-level /v1/sql/saved/{id}
//! route (not under /v1/projects/{ref}/...) because saved queries carry
//! the project_id in the row and the cloud enforces ownership/visibility
//! inside the handler rather than via route gating.
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd).
//! Error with hint at `basin link` if neither is available.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────

/// SavedQuery is the wire shape for a saved query.
/// JSON shape: { id, project_id, name, sql, description, public, created_at, updated_at, owner_id }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SavedQuery {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default)]
    pub public: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner_id: String,
}

/// QueryHistoryEntry is one row from the query history.
/// JSON shape: { id, project_id, sql, duration_ms, executed_at, user_id }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueryHistoryEntry {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(default)]
    pub duration_ms: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executed_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_id: String,
}

#[derive(Deserialize)]
struct SaveQueryResp {
    #[serde(default)]
    query: Option<SavedQuery>,
}

#[derive(Deserialize)]
struct ListQueriesResp {
    #[serde(default)]
    queries: Vec<SavedQuery>,
}

#[derive(Deserialize)]
struct GetQueryResp {
    #[serde(default)]
    query: Option<SavedQuery>,
}

#[derive(Deserialize)]
struct ForkQueryResp {
    #[serde(default)]
    query: Option<SavedQuery>,
}

#[derive(Deserialize)]
struct DeleteQueryResp {
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    id: String,
}

#[derive(Deserialize)]
struct HistoryListResp {
    #[serde(default)]
    history: Vec<QueryHistoryEntry>,
}

#[derive(Deserialize)]
struct HistoryClearResp {
    #[serde(default)]
    cleared: bool,
}

// ── Helpers ───────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors with a helpful hint
/// when neither is available.
fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg(
            "--project is required (or run `basin link` to bind this directory)",
        )),
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_queries(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return queries_list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "save" => queries_save(g, rest),
        "list" => queries_list(g, rest),
        "get" => queries_get(g, rest),
        "fork" => queries_fork(g, rest),
        "delete" => queries_delete(g, rest),
        "history" => queries_history(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "queries",
                "Manage saved SQL queries and query history.",
                &[
                    "save    --name=<n> --sql=<s> [--description=<d>] [--public=bool] [--project=<ref>]  Save a query.",
                    "list    [--project=<ref>]                                                            List saved queries.",
                    "get     <id>                                                                         Get a saved query by ID.",
                    "fork    <id> [--name=<n>] [--project=<ref>]                                         Fork a saved query.",
                    "delete  <id> [--project=<ref>] [--yes]                                              Delete a saved query.",
                    "history [--project=<ref>] [--limit=N] [--clear --yes]                               List or clear history.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for queries");
            Err(silent())
        }
    }
}

// ── queries save ─────────────────────────────────────────────────────

fn queries_save(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries save")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("sql").long("sql"))
        .arg(Arg::new("description").long("description"))
        .arg(Arg::new("public").long("public").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries save",
            "Save a SQL query for reuse.",
            &[
                "--name <n>            Name for the saved query (required).",
                "--sql <s>             SQL text to save (required).",
                "--description <d>     Optional description.",
                "--public              Make visible to all org members.",
                "--project <ref>       Project ref (or auto-detected from ./basin/config.toml).",
            ],
        );
        return Ok(());
    }
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("--name is required"));
    }
    let sql = m.get_one::<String>("sql").cloned().unwrap_or_default();
    if sql.is_empty() {
        return Err(msg("--sql is required"));
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let description = m
        .get_one::<String>("description")
        .cloned()
        .unwrap_or_default();
    let public = m.get_flag("public");

    let c = require_client(g)?;
    let mut body = json!({ "name": name, "sql": sql, "public": public });
    if !description.is_empty() {
        body["description"] = json!(description);
    }
    let resp: SaveQueryResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/sql/saved"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "query": { id, name, sql, description, public, created_at } }
        return print_json(&mut std::io::stdout(), &json!({ "query": resp.query }));
    }
    if let Some(q) = &resp.query {
        println!("Saved query {:?} (id={})", q.name, q.id);
    } else {
        println!("Query saved.");
    }
    Ok(())
}

// ── queries list ─────────────────────────────────────────────────────

fn queries_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries list",
            "List saved SQL queries for a project.",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: ListQueriesResp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/sql/saved"), None)?;
    if g.json {
        // JSON shape: { "queries": [ SavedQuery ] }
        return print_json(&mut std::io::stdout(), &json!({ "queries": resp.queries }));
    }
    if resp.queries.is_empty() {
        println!("(no saved queries)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "NAME", "PUBLIC", "CREATED"]);
    for q in &resp.queries {
        let pub_str = if q.public { "true" } else { "false" };
        t.row(&[&q.id, &q.name, pub_str, &q.created_at]);
    }
    t.flush()
}

// ── queries get ──────────────────────────────────────────────────────
//
// Route: GET /v1/sql/saved/{id}  (top-level — see cloud router anchor sql-saved-by-id)

fn queries_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries get")
        // --project is accepted for flag consistency but not needed: the cloud
        // resolves ownership/visibility from the query's own project_id.
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries get",
            "Get a saved query by ID (top-level route; not project-scoped).",
            &[
                "<id>               Query ID (required).",
                "--project <ref>    Accepted for consistency; not sent to the server.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin queries get <id> [--project=<ref>]"))?;
    // --project accepted but not required (ignored by server).
    let c = require_client(g)?;
    // Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
    let resp: GetQueryResp = c
        .do_json(Method::GET, &format!("/v1/sql/saved/{id}"), None)
        .map_err(|err| {
            if let Some(ae) = as_api_error(err.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("query {:?} not found", id));
                }
                if ae.http_status == 403 {
                    return msg(format!(
                        "access denied: query {:?} is private or owned by another user",
                        id
                    ));
                }
            }
            err
        })?;
    let q = resp
        .query
        .ok_or_else(|| msg(format!("query {:?} not found", id)))?;
    if g.json {
        // JSON shape: { "query": { id, name, sql, description, public, created_at } }
        return print_json(&mut std::io::stdout(), &json!({ "query": q }));
    }
    println!("id:          {}", q.id);
    println!("name:        {}", q.name);
    println!("public:      {}", q.public);
    if !q.description.is_empty() {
        println!("description: {}", q.description);
    }
    println!("created_at:  {}", q.created_at);
    if !q.sql.is_empty() {
        println!("\n-- SQL:\n{}", q.sql);
    }
    Ok(())
}

// ── queries fork ─────────────────────────────────────────────────────
//
// Route: POST /v1/sql/saved/{id}/fork  (top-level)

fn queries_fork(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries fork")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries fork",
            "Fork a saved query (top-level route).",
            &[
                "<id>               Query ID to fork (required).",
                "--name <n>         Name for the forked query (optional).",
                "--project <ref>    Project ref (accepted; not sent to server today).",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin queries fork <id> [--name=<n>] [--project=<ref>]"))?;
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    // --project accepted but not currently forwarded to the cloud.
    let c = require_client(g)?;
    let mut body = json!({});
    if !name.is_empty() {
        body["name"] = json!(name);
    }
    // Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
    let resp: ForkQueryResp = c
        .do_json(
            Method::POST,
            &format!("/v1/sql/saved/{id}/fork"),
            Some(body),
        )
        .map_err(|err| {
            if let Some(ae) = as_api_error(err.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("query {:?} not found", id));
                }
            }
            err
        })?;
    if g.json {
        // JSON shape: { "query": { id, name, sql, description, public, created_at } }
        return print_json(&mut std::io::stdout(), &json!({ "query": resp.query }));
    }
    if let Some(q) = &resp.query {
        println!("Forked query {:?} → new id={} name={:?}", id, q.id, q.name);
    } else {
        println!("Forked query {}.", id);
    }
    Ok(())
}

// ── queries delete ───────────────────────────────────────────────────
//
// Route: DELETE /v1/sql/saved/{id}  (top-level)

fn queries_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries delete")
        // --project accepted for consistency; not sent to the server.
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries delete",
            "Delete a saved query permanently (top-level route).",
            &[
                "<id>               Query ID to delete (required).",
                "--project <ref>    Accepted for consistency; not sent to the server.",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin queries delete <id> [--project=<ref>] [--yes]"))?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete query {:?} non-interactively under --json",
                id
            )));
        }
        eprint!("Delete saved query {:?}? This cannot be undone. [y/N] ", id);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
    let resp: DeleteQueryResp = c
        .do_json(Method::DELETE, &format!("/v1/sql/saved/{id}"), None)
        .map_err(|err| {
            if let Some(ae) = as_api_error(err.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("query {:?} not found", id));
                }
            }
            err
        })?;
    if g.json {
        // JSON shape: { "deleted": true, "id": "<query id>" }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "deleted": resp.deleted, "id": resp.id }),
        );
    }
    println!("Deleted query {}.", id);
    Ok(())
}

// ── queries history ──────────────────────────────────────────────────

fn queries_history(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("queries history")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit"))
        .arg(Arg::new("clear").long("clear").action(ArgAction::SetTrue))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "queries history",
            "List or clear query history for a project.",
            &[
                "--project <ref>   Project ref (or auto-detected from ./basin/config.toml).",
                "--limit N         Maximum history entries to return (default: 50).",
                "--clear           Delete query history instead of listing.",
                "--yes             Skip the confirmation prompt for --clear.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let limit: i64 = m
        .get_one::<String>("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let clear = m.get_flag("clear");
    let yes = m.get_flag("yes");
    let c = require_client(g)?;

    if clear {
        history_clear(g, &c, &r#ref, yes)
    } else {
        history_list(g, &c, &r#ref, limit)
    }
}

fn history_list(
    g: &GlobalFlags,
    c: &crate::client::Client,
    r#ref: &str,
    limit: i64,
) -> CliResult<()> {
    let qs = query_string(&[("limit", &limit.to_string())]);
    let resp: HistoryListResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/sql/history{qs}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "history": [ QueryHistoryEntry ] }
        return print_json(&mut std::io::stdout(), &json!({ "history": resp.history }));
    }
    if resp.history.is_empty() {
        println!("(no query history)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "SQL", "DURATION_MS", "EXECUTED_AT"]);
    for h in &resp.history {
        let snippet = if h.sql.chars().count() > 60 {
            let s: String = h.sql.chars().take(57).collect();
            format!("{s}...")
        } else {
            h.sql.clone()
        };
        t.row(&[
            &h.id,
            &snippet,
            &format!("{:.0}", h.duration_ms),
            &h.executed_at,
        ]);
    }
    t.flush()
}

fn history_clear(
    g: &GlobalFlags,
    c: &crate::client::Client,
    r#ref: &str,
    yes: bool,
) -> CliResult<()> {
    if !yes {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to clear history non-interactively under --json",
            ));
        }
        eprint!(
            "Clear all query history for project {:?}? This cannot be undone. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }
    let resp: HistoryClearResp = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/sql/history"),
        None,
    )?;
    if g.json {
        // JSON shape: { "cleared": true }
        return print_json(&mut std::io::stdout(), &json!({ "cleared": resp.cleared }));
    }
    println!("Query history cleared.");
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

    fn sample_saved_query(id: &str, name: &str) -> String {
        format!(
            r#"{{"id":"{id}","project_id":"proj1","name":"{name}","sql":"SELECT 1","description":"test query","public":false,"created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    fn sample_history_entry(id: &str) -> String {
        format!(
            r#"{{"id":"{id}","project_id":"proj1","sql":"SELECT now()","duration_ms":12.5,"executed_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── dispatcher ────────────────────────────────────────────────────

    #[test]
    fn no_args_delegates_to_list_which_needs_project() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_queries(&g, &[]).is_err());
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
        let err = cmd_queries(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_queries(&g, &["help".into()]).is_ok());
    }

    // ── queries save ──────────────────────────────────────────────────

    #[test]
    fn save_happy_path() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q1", "My Query");
        let body_resp = format!(r#"{{"query":{q}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/sql/saved");
            let b: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(b["name"].as_str().unwrap(), "My Query");
            assert_eq!(b["sql"].as_str().unwrap(), "SELECT 1");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_queries(
            &g,
            &[
                "save".into(),
                "--project=proj1".into(),
                "--name=My Query".into(),
                "--sql=SELECT 1".into(),
                "--description=test".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn save_missing_name_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_queries(
            &g,
            &[
                "save".into(),
                "--project=proj1".into(),
                "--sql=SELECT 1".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name"), "got: {err}");
    }

    #[test]
    fn save_missing_sql_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_queries(
            &g,
            &["save".into(), "--project=proj1".into(), "--name=foo".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--sql"), "got: {err}");
    }

    #[test]
    fn save_json_shape() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q1", "My Query");
        let body_resp = format!(r#"{{"query":{q}}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: SaveQueryResp = c
            .do_json(
                Method::POST,
                "/v1/projects/proj1/sql/saved",
                Some(json!({"name":"My Query","sql":"SELECT 1","public":false})),
            )
            .unwrap();
        assert!(resp.query.is_some());
        assert_eq!(resp.query.as_ref().unwrap().id, "q1");
        // Shape via print_json
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "query": resp.query })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["query"]["id"].as_str().unwrap(), "q1");
    }

    // ── queries list ──────────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let _g = with_temp_config_dir();
        let q1 = sample_saved_query("q1", "Alpha");
        let q2 = sample_saved_query("q2", "Beta");
        let body_resp = format!(r#"{{"queries":[{q1},{q2}]}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/sql/saved");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_queries(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_empty_prints_message() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"queries":[]}"#));
        let g = flags(&srv.url);
        cmd_queries(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q1", "Alpha");
        let body_resp = format!(r#"{{"queries":[{q}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ListQueriesResp = c
            .do_json(Method::GET, "/v1/projects/proj1/sql/saved", None)
            .unwrap();
        assert_eq!(resp.queries.len(), 1);
        assert_eq!(resp.queries[0].id, "q1");
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "queries": resp.queries })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["queries"].is_array());
        assert_eq!(v["queries"][0]["id"].as_str().unwrap(), "q1");
    }

    // ── queries get ───────────────────────────────────────────────────

    #[test]
    fn get_uses_top_level_route() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q1", "Alpha");
        let body_resp = format!(r#"{{"query":{q}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/sql/saved/q1");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_queries(&g, &["get".into(), "q1".into()]).unwrap();
    }

    #[test]
    fn get_404_friendly_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"query not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_queries(&g, &["get".into(), "missing".into()]).unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    #[test]
    fn get_403_friendly_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"access denied"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_queries(&g, &["get".into(), "priv".into()]).unwrap_err();
        assert!(err.to_string().contains("private"), "got: {err}");
    }

    #[test]
    fn get_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_queries(&g, &["get".into()]).unwrap_err();
        assert!(err.to_string().contains("usage:"), "got: {err}");
    }

    // ── queries fork ──────────────────────────────────────────────────

    #[test]
    fn fork_uses_top_level_route_with_name() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q2", "Fork of Alpha");
        let body_resp = format!(r#"{{"query":{q}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/sql/saved/q1/fork");
            let b: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(b["name"].as_str().unwrap_or(""), "Fork of Alpha");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_queries(
            &g,
            &["fork".into(), "--name=Fork of Alpha".into(), "q1".into()],
        )
        .unwrap();
    }

    #[test]
    fn fork_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_queries(&g, &["fork".into()]).unwrap_err();
        assert!(err.to_string().contains("usage:"), "got: {err}");
    }

    #[test]
    fn fork_json_shape() {
        let _g = with_temp_config_dir();
        let q = sample_saved_query("q2", "Fork of Alpha");
        let body_resp = format!(r#"{{"query":{q}}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ForkQueryResp = c
            .do_json(Method::POST, "/v1/sql/saved/q1/fork", Some(json!({})))
            .unwrap();
        assert_eq!(resp.query.as_ref().unwrap().id, "q2");
    }

    // ── queries delete ────────────────────────────────────────────────

    #[test]
    fn delete_with_yes_uses_top_level_delete() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/sql/saved/q1");
            Resp::ok(r#"{"deleted":true,"id":"q1"}"#)
        });
        let g = flags(&srv.url);
        cmd_queries(&g, &["delete".into(), "--yes".into(), "q1".into()]).unwrap();
    }

    #[test]
    fn delete_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_queries(
            &g,
            &["delete".into(), "--project=proj1".into(), "q1".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn delete_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_queries(&g, &["delete".into(), "--yes".into()]).unwrap_err();
        assert!(err.to_string().contains("usage:"), "got: {err}");
    }

    #[test]
    fn delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":true,"id":"q1"}"#));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: DeleteQueryResp = c.do_json(Method::DELETE, "/v1/sql/saved/q1", None).unwrap();
        assert!(resp.deleted);
        assert_eq!(resp.id, "q1");
        // JSON output shape
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "deleted": resp.deleted, "id": resp.id })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["deleted"].as_bool().unwrap());
        assert_eq!(v["id"].as_str().unwrap(), "q1");
    }

    // ── queries history ───────────────────────────────────────────────

    #[test]
    fn history_list_happy_path() {
        let _g = with_temp_config_dir();
        let h1 = sample_history_entry("h1");
        let h2 = sample_history_entry("h2");
        let body_resp = format!(r#"{{"history":[{h1},{h2}]}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            // history always appends ?limit=<n> (default 50); req.path
            // includes the query string from the HTTP request line.
            assert!(
                req.path.starts_with("/v1/projects/proj1/sql/history"),
                "unexpected path: {}",
                req.path
            );
            assert!(req.path.contains("limit="), "expected limit param in path");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_queries(&g, &["history".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn history_list_json_shape() {
        let _g = with_temp_config_dir();
        let h = sample_history_entry("h1");
        let body_resp = format!(r#"{{"history":[{h}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let qs = query_string(&[("limit", "50")]);
        let resp: HistoryListResp = c
            .do_json(
                Method::GET,
                &format!("/v1/projects/proj1/sql/history{qs}"),
                None,
            )
            .unwrap();
        assert_eq!(resp.history.len(), 1);
        assert_eq!(resp.history[0].id, "h1");
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "history": resp.history })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["history"][0]["id"].as_str().unwrap(), "h1");
    }

    #[test]
    fn history_clear_with_yes_sends_delete() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/sql/history");
            Resp::ok(r#"{"cleared":true}"#)
        });
        let g = flags(&srv.url);
        cmd_queries(
            &g,
            &[
                "history".into(),
                "--project=proj1".into(),
                "--clear".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn history_clear_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_queries(
            &g,
            &["history".into(), "--project=proj1".into(), "--clear".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn history_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"no access"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_queries(&g, &["history".into(), "--project=proj1".into()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }
}
