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
    let cmd = Command::new("whoami").arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_whoami(&g, &["--help".into()]).is_ok());
    }

    #[test]
    fn no_token_returns_silent_error() {
        let _g = with_temp_config_dir();
        // No --token, no env var, empty config — require_client emits the
        // canonical "not signed in" error.
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_whoami(&g, &[]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()), "error: {err}");
    }

    #[test]
    fn happy_path_calls_user_then_orgs() {
        let _g = with_temp_config_dir();
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let cc = Arc::clone(&calls);
        let srv = TestServer::start(move |req: &Req| {
            cc.lock().unwrap().push(req.path.clone());
            match req.path.as_str() {
                "/auth/v1/user" => Resp::ok(
                    r#"{"user":{"id":"u-1","email":"alice@example.com","name":"Alice"}}"#,
                ),
                "/v1/orgs" => Resp::ok(
                    r#"{"orgs":[{"id":"o-1","slug":"acme","name":"Acme","plan":"pro"}]}"#,
                ),
                _ => Resp::status(404, "{}"),
            }
        });
        let g = flags(&srv.url);
        cmd_whoami(&g, &[]).unwrap();
        let recorded = calls.lock().unwrap().clone();
        assert!(
            recorded.contains(&"/auth/v1/user".to_string()),
            "expected /auth/v1/user in {recorded:?}"
        );
        assert!(
            recorded.contains(&"/v1/orgs".to_string()),
            "expected /v1/orgs in {recorded:?}"
        );
    }

    #[test]
    fn user_endpoint_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let g = flags(&srv.url);
        assert!(cmd_whoami(&g, &[]).is_err());
    }

    #[test]
    fn orgs_failure_is_swallowed_with_warning() {
        let _g = with_temp_config_dir();
        // /auth/v1/user succeeds, /v1/orgs fails → whoami still returns Ok
        // (warning printed to stderr, orgs treated as empty).
        let srv = TestServer::start(|req: &Req| {
            if req.path == "/auth/v1/user" {
                return Resp::ok(r#"{"user":{"id":"u-1","email":"alice@example.com"}}"#);
            }
            Resp::status(500, r#"{"code":"internal","message":"boom"}"#)
        });
        let g = flags(&srv.url);
        cmd_whoami(&g, &[]).unwrap();
    }

    #[test]
    fn json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| match req.path.as_str() {
            "/auth/v1/user" => {
                Resp::ok(r#"{"user":{"id":"u-1","email":"alice@example.com","name":"Alice"}}"#)
            }
            "/v1/orgs" => Resp::ok(
                r#"{"orgs":[{"id":"o-1","slug":"acme","name":"Acme","plan":"pro"}]}"#,
            ),
            _ => Resp::status(404, "{}"),
        });
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let me: MeResp = c
            .do_json(reqwest::Method::GET, "/auth/v1/user", None)
            .unwrap();
        let orgs: OrgsResp = c
            .do_json(reqwest::Method::GET, "/v1/orgs", None)
            .unwrap();
        print_json(
            &mut buf,
            &json!({ "user": me.user, "orgs": orgs.orgs }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["user"]["email"].as_str().unwrap(), "alice@example.com");
        assert_eq!(v["orgs"][0]["slug"].as_str().unwrap(), "acme");
    }
}
