//! whoami — print the active user + every accessible org.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::error::CliResult;
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;
use crate::types::{Org, User};

use super::help::help_for_command;
use super::parse_or_silent;

#[derive(Deserialize)]
struct MeResp {
    #[serde(default)]
    user: Option<User>,
}

#[derive(Deserialize)]
struct OrgsResp {
    #[serde(default)]
    orgs: Vec<Org>,
}

pub fn cmd_whoami(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("whoami")
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command("whoami", "Print the active user + accessible orgs.", &[]);
        return Ok(());
    }

    let c = require_client(g)?;
    let me: MeResp = c.do_json(Method::GET, "/auth/v1/user", None)?;
    let orgs = match c.do_json::<OrgsResp>(Method::GET, "/v1/orgs", None) {
        Ok(o) => o.orgs,
        Err(e) => {
            printerr!(g, "warning: could not list orgs: {e}");
            Vec::new()
        }
    };

    if g.json {
        // JSON shape: { user: User, orgs: [ Org ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "user": me.user, "orgs": orgs }),
        );
    }

    if let Some(u) = &me.user {
        let label = if u.name.is_empty() { &u.email } else { &u.name };
        println!("{label} <{}>", u.email);
    }
    if orgs.is_empty() {
        println!("  (no orgs)");
        return Ok(());
    }
    let mut t = Table::new(g, &["SLUG", "NAME", "PLAN", "ID"]);
    for o in &orgs {
        t.row(&[&o.slug, &o.name, &o.plan, &o.id]);
    }
    t.flush()
}
