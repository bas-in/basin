//! commands — the subcommand dispatch table + shared command helpers.
//!
//! Each `commands/<area>.rs` exposes a handler with the signature
//! `fn(&GlobalFlags, &[String]) -> CliResult<()>` and is registered in
//! [`all`] below. During the Go→Rust port many modules are stubs that
//! return "not yet ported"; porting one means replacing its body +
//! adding a `#[cfg(test)]` module — NOT editing this file. The dispatch
//! entries are pre-wired so parallel work never contends on this table.

use crate::error::{silent, CliResult};
use crate::global::GlobalFlags;

pub mod activity;
pub mod alerts;
pub mod api_keys;
pub mod audit;
pub mod backups;
pub mod branches;
pub mod byo;
pub mod completion;
pub mod config_cmd;
pub mod db;
pub mod docs;
pub mod domains;
pub mod email;
pub mod engine_pin;
pub mod erd;
pub mod extensions;
pub mod gen;
pub mod help;
pub mod init;
pub mod invitations;
pub mod link;
pub mod login;
pub mod logout;
pub mod logs;
pub mod members;
pub mod metrics;
pub mod migrations;
pub mod oauth_apps;
pub mod oauth_providers;
pub mod orgs;
pub mod pgwire;
pub mod projects;
pub mod queries;
pub mod rls;
pub mod realtime;
pub mod rows;
pub mod rpc;
pub mod saml;
pub mod scim;
pub mod secrets;
pub mod snapshots;
pub mod sql;
pub mod status;
pub mod tables;
pub mod tokens;
pub mod unlink;
pub mod version;
pub mod webhooks;
pub mod whoami;

/// CommandHandler is the per-subcommand entrypoint.
pub type CommandHandler = fn(&GlobalFlags, &[String]) -> CliResult<()>;

/// Entry is one row of the dispatch table.
pub struct Entry {
    pub name: &'static str,
    pub help: &'static str,
    pub run: CommandHandler,
}

macro_rules! entry {
    ($name:literal, $help:literal, $run:path) => {
        Entry { name: $name, help: $help, run: $run }
    };
}

/// all returns the canonical subcommand table. Order shapes the
/// `basin --help` summary: auth → org/project reads → mutations →
/// engine surfaces → utilities.
pub fn all() -> Vec<Entry> {
    vec![
        entry!("login", "Sign in by pasting a personal access token.", login::cmd_login),
        entry!("logout", "Forget the locally-stored token.", logout::cmd_logout),
        entry!("whoami", "Print the active user + accessible orgs.", whoami::cmd_whoami),
        entry!("orgs", "List / get / create / update / delete orgs + branding.", orgs::cmd_orgs),
        entry!("members", "List / invite / remove / role-change org members.", members::cmd_members),
        entry!("invitations", "List / resend / revoke pending org invitations.", invitations::cmd_invitations),
        entry!("projects", "List / get / create / delete / pause / resume projects.", projects::cmd_projects),
        entry!("init", "Scaffold a basin project directory (./basin/).", init::cmd_init),
        entry!("link", "Bind ./basin/ to a remote project (writes project_ref).", link::cmd_link),
        entry!("status", "Show the linked project + migration drift summary.", status::cmd_status),
        entry!("unlink", "Remove the project binding from ./basin/config.toml.", unlink::cmd_unlink),
        entry!("sql", "Run a SQL query against a project.", sql::cmd_sql),
        entry!("rpc", "Invoke a user-defined database function (T26.4).", rpc::cmd_rpc),
        entry!("db", "Push / pull / diff / reset / url / dump / lint the project database.", db::cmd_db),
        entry!("tables", "List / describe / create / alter / drop tables + CSV import/export.", tables::cmd_tables),
        entry!("rows", "Insert / update / delete rows in a project table.", rows::cmd_rows),
        entry!("rls", "Enable / disable RLS + manage row-level security policies.", rls::cmd_rls),
        entry!("migrations", "List / apply / new / get / diff / rollback DDL migrations.", migrations::cmd_migrations),
        entry!("branches", "List / create / get / merge / delete / events on project branches.", branches::cmd_branches),
        entry!("gen", "Generate language bindings (typescript / go / python) from schema.", gen::cmd_gen),
        entry!("snapshots", "List / create / restore project snapshots.", snapshots::cmd_snapshots),
        entry!("backups", "Manage backup policy / snapshots / restore jobs.", backups::cmd_backups),
        entry!("logs", "Stream live project logs (or poll history).", logs::cmd_logs),
        entry!("realtime", "Subscribe to table event streams over SSE (T26.1).", realtime::cmd_realtime),
        entry!("api-keys", "List / create / rotate / revoke project-scoped API keys.", api_keys::cmd_api_keys),
        entry!("pgwire", "Show / reveal / rotate pgwire credentials and engine JWT keys.", pgwire::cmd_pgwire),
        entry!("domains", "Add / verify / cert / list / remove custom project domains.", domains::cmd_domains),
        entry!("webhooks", "Manage project webhooks: create / patch / test / redeliver / deliveries.", webhooks::cmd_webhooks),
        entry!("alerts", "Manage alert rules + view fired events.", alerts::cmd_alerts),
        entry!("audit", "List org audit events (--org=<slug> required).", audit::cmd_audit),
        entry!("activity", "List project activity events.", activity::cmd_activity),
        entry!("metrics", "Show project or org metrics (--range / --metric).", metrics::cmd_metrics),
        entry!("erd", "Export project ERD (svg / dot / json).", erd::cmd_erd),
        entry!("queries", "Save / list / get / fork / delete / history SQL queries.", queries::cmd_queries),
        entry!("engine-pin", "Get / put / delete / events on the project's engine version pin.", engine_pin::cmd_engine_pin),
        entry!("extensions", "List the project's installed engine extensions.", extensions::cmd_extensions),
        entry!("oauth-providers", "List / get / set / delete project OAuth providers.", oauth_providers::cmd_oauth_providers),
        entry!("email", "Manage project email templates + view allowance.", email::cmd_email),
        entry!("byo", "Configure bring-your-own bucket / kms / engine.", byo::cmd_byo),
        entry!("saml", "Get / put / test / enable / disable org SAML SSO.", saml::cmd_saml),
        entry!("scim", "SCIM config + provisioning tokens.", scim::cmd_scim),
        entry!("oauth-apps", "Manage org OAuth applications (clients).", oauth_apps::cmd_oauth_apps),
        entry!("docs", "Open docs.basin.run for a given subcommand.", docs::cmd_docs),
        entry!("secrets", "List / set / remove project env-overrides.", secrets::cmd_secrets),
        entry!("tokens", "Manage org-scoped personal access tokens.", tokens::cmd_tokens),
        entry!("config", "Read or write CLI config (telemetry, default org).", config_cmd::cmd_config),
        entry!("completion", "Print a shell completion script for bash, zsh, or fish.", completion::cmd_completion),
        entry!("version", "Print binary version + build metadata.", version::cmd_version),
        entry!("help", "Show help for the CLI or a subcommand.", help::cmd_help),
    ]
}

/// parse_or_silent runs a clap `Command` against `args`, printing any
/// parse error and returning the [`silent`] sentinel so `main` doesn't
/// re-print. Commands disable clap's auto help flag and carry their own
/// `--help` bool so a help request returns `Ok` (exit 0) instead.
pub fn parse_or_silent(cmd: clap::Command, args: &[String]) -> CliResult<clap::ArgMatches> {
    match cmd.no_binary_name(true).disable_help_flag(true).try_get_matches_from(args) {
        Ok(m) => Ok(m),
        Err(e) => {
            let _ = e.print();
            Err(silent())
        }
    }
}
