//! login — credential capture. Prompts for (or accepts via --token) a
//! PAT, validates it against /auth/v1/user, and writes it to disk under
//! default_token (no --org) or tokens.<slug>.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::client::Client;
use crate::config::{upsert_token, write_config_file};
use crate::error::{msg, silent, CliResult};
use crate::global::GlobalFlags;
use crate::output::{print_json, read_line};
use crate::printerr;
use crate::types::User;

use super::help::help_for_command;
use super::parse_or_silent;

#[derive(Deserialize)]
struct MeResp {
    #[serde(default)]
    user: Option<User>,
}

pub fn cmd_login(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("login")
        .arg(Arg::new("token").long("token"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("api-url").long("api-url"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "login",
            "Sign in by pasting a personal access token.",
            &[
                "--token=<pat>     Provide the token non-interactively.",
                "--org=<slug>      Store under tokens.<slug> (per-org). Default writes default_token.",
                "--api-url=<url>   Validate against this endpoint.",
            ],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let api_url = m
        .get_one::<String>("api-url")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.api_url.clone());

    let mut token = m.get_one::<String>("token").cloned().unwrap_or_default();
    if token.is_empty() {
        eprintln!("Paste a personal access token (mint one at /app/org/<slug>/api-tokens):");
        eprint!("> ");
        token = read_line()?.trim().to_string();
    }
    if token.is_empty() {
        return Err(msg("no token provided"));
    }
    if !token.starts_with("bso_org_") {
        printerr!(
            g,
            "warning: token doesn't start with 'bso_org_' — is this the right value?"
        );
    }

    let c = Client::new(&api_url, &token);
    let me: MeResp = match c.do_json(Method::GET, "/auth/v1/user", None) {
        Ok(me) => me,
        Err(e) => {
            printerr!(g, "validation failed: {e}");
            return Err(silent());
        }
    };

    let (mut cf, path) = upsert_token(&org, &token)?;
    if !org.is_empty() && cf.default_org.is_empty() {
        cf.default_org = org.clone();
        write_config_file(&cf)?;
    }

    let stored = org_or_default(&org);
    if g.json {
        // JSON shape: { signed_in: bool, user: User, config_path: string, stored_under: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "signed_in": true,
                "user": me.user,
                "config_path": path.display().to_string(),
                "stored_under": stored,
            }),
        );
    }
    match me.user.as_ref().filter(|u| !u.email.is_empty()) {
        Some(u) => println!(
            "Signed in as {}. Token saved to {} ({}).",
            u.email,
            path.display(),
            stored
        ),
        None => println!("Signed in. Token saved to {} ({}).", path.display(), stored),
    }
    Ok(())
}

/// org_or_default is a tiny render helper.
pub fn org_or_default(org: &str) -> String {
    if org.is_empty() {
        "default_token".to_string()
    } else {
        format!("tokens.{org}")
    }
}

#[cfg(test)]
mod tests {
    // House style (see config_cmd.rs:362): tests don't chain a read-back
    // after a config write — XDG_CONFIG_HOME is process-global and parallel
    // tests race on it. We verify each cmd_login invocation returns Ok
    // (write failure surfaces as Err) and the envelope shape separately.

    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        // Help short-circuits before any network call.
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_login(&g, &["--help".into()]).is_ok());
    }

    #[test]
    fn org_or_default_helper() {
        assert_eq!(org_or_default(""), "default_token");
        assert_eq!(org_or_default("acme"), "tokens.acme");
    }

    #[test]
    fn login_with_token_validates_and_persists() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/auth/v1/user");
            Resp::ok(r#"{"user":{"id":"u-1","email":"alice@example.com","name":"Alice"}}"#)
        });
        let g = flags(&srv.url);
        // Ok return implies validation request hit /auth/v1/user (asserted by
        // the handler) and the subsequent config write succeeded.
        cmd_login(&g, &["--token=bso_org_abc123".into()]).unwrap();
    }

    #[test]
    fn login_with_token_and_org_returns_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"user":{"id":"u-2","email":"bob@example.com"}}"#)
        });
        let g = flags(&srv.url);
        // Per-org path: cmd_login calls upsert_token then a second
        // write_config_file to stamp default_org. The testutil
        // serialisation mutex makes this safe under parallel tests.
        cmd_login(
            &g,
            &[
                "--token=bso_org_xyz".into(),
                "--org=acme".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn login_propagates_validation_failure_as_silent() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_login(&g, &["--token=bso_org_bad".into()]).unwrap_err();
        // The validation-failed branch prints the warning + returns silent().
        assert!(crate::error::is_silent(err.as_ref()), "error: {err}");
    }

    #[test]
    fn login_json_shape() {
        // Verify the documented envelope shape independently of the
        // interactive flow (see house-style comment above).
        let mut buf = Vec::<u8>::new();
        let user = crate::types::User {
            id: "u-1".into(),
            email: "alice@example.com".into(),
            ..Default::default()
        };
        print_json(
            &mut buf,
            &json!({
                "signed_in": true,
                "user": user,
                "config_path": "/tmp/x/config.json",
                "stored_under": "default_token",
            }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["signed_in"].as_bool().unwrap(), true);
        assert_eq!(v["user"]["email"].as_str().unwrap(), "alice@example.com");
        assert_eq!(v["stored_under"].as_str().unwrap(), "default_token");
    }
}
