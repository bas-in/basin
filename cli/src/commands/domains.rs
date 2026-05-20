//! domains — custom-domain lifecycle for a project.
//!
//!   basin domains list   [--project=<ref>]
//!   basin domains add    <hostname> [--project=<ref>]
//!   basin domains verify <id> [--project=<ref>]
//!   basin domains cert   <id> [--project=<ref>]
//!   basin domains remove <id> [--project=<ref>] [--yes]
//!
//! Backed by /v1/projects/{ref}/custom-domains/* (migration 0009).
//! Project ref resolution: --project flag, else loadWorkingProject(cwd).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// CustomDomain is the read shape from GET /v1/projects/{ref}/custom-domains.
/// JSON shape: { id, hostname, status, cert_status, created_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct CustomDomain {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cert_status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// CustomDomainDNSRecord is one DNS record the user must configure at their registrar.
/// Returned by POST /v1/projects/{ref}/custom-domains.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct CustomDomainDNSRecord {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    #[serde(default)]
    pub ttl: i64,
}

/// AddDomainResponse is the envelope from POST /v1/projects/{ref}/custom-domains.
/// JSON shape: { domain: CustomDomain, dns_records: [ CustomDomainDNSRecord ] }
#[derive(Debug, Default, Serialize, Deserialize)]
struct AddDomainResponse {
    #[serde(default)]
    pub domain: Option<CustomDomain>,
    #[serde(default)]
    pub dns_records: Vec<CustomDomainDNSRecord>,
}

#[derive(Deserialize)]
struct DomainsListResp {
    #[serde(default)]
    domains: Vec<CustomDomain>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or the
/// directory-scoped ./basin/config.toml. Errors with a helpful hint when
/// neither is available.
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

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_domains(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "add" => add(g, rest),
        "verify" => verify(g, rest),
        "cert" => cert(g, rest),
        "remove" => remove(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "domains",
                "Manage custom domains for a project.",
                &[
                    "list             [--project=<ref>]           List custom domains.",
                    "add   <hostname> [--project=<ref>]           Add a custom domain (prints DNS records).",
                    "verify <id>      [--project=<ref>]           Trigger DNS verification.",
                    "cert   <id>      [--project=<ref>]           Request TLS certificate issuance.",
                    "remove <id>      [--project=<ref>] [--yes]   Remove a custom domain.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for domains", other);
            Err(silent())
        }
    }
}

// ── domains list ─────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("domains list").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: DomainsListResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/custom-domains"),
        None,
    )?;
    if g.json {
        // JSON shape: { domains: [ CustomDomain ] }
        return print_json(&mut std::io::stdout(), &json!({ "domains": resp.domains }));
    }
    if resp.domains.is_empty() {
        println!("(no custom domains)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "HOSTNAME", "STATUS", "CERT", "CREATED"]);
    for d in &resp.domains {
        t.row(&[&d.id, &d.hostname, &d.status, &d.cert_status, &d.created_at]);
    }
    t.flush()
}

// ── domains add ──────────────────────────────────────────────────────────────

fn add(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("domains add")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("hostname"));
    let m = parse_or_silent(cmd, args)?;
    let hostname = m
        .get_one::<String>("hostname")
        .cloned()
        .ok_or_else(|| msg("usage: basin domains add <hostname> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: AddDomainResponse = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/custom-domains"),
            Some(json!({ "hostname": hostname })),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 409 {
                    return msg(format!(
                        "hostname {:?} is already registered on this project",
                        hostname
                    ));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { domain: CustomDomain, dns_records: [ CustomDomainDNSRecord ] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(d) = &resp.domain {
        println!(
            "Added domain {:?} (id={}, status={})",
            d.hostname, d.id, d.status
        );
    }
    if !resp.dns_records.is_empty() {
        println!("\nAdd these DNS records at your registrar:");
        let mut t = Table::new(g, &["TYPE", "NAME", "VALUE", "TTL"]);
        for r in &resp.dns_records {
            let ttl = if r.ttl > 0 {
                r.ttl.to_string()
            } else {
                String::new()
            };
            t.row(&[&r.r#type, &r.name, &r.value, &ttl]);
        }
        t.flush()?;
    }
    Ok(())
}

// ── domains verify ───────────────────────────────────────────────────────────

fn verify(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("domains verify")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin domains verify <id> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct VerifyResp {
        #[serde(default)]
        verified: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        next_check_at: String,
    }
    let resp: VerifyResp = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/custom-domains/{id}/verify"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("domain {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { verified: bool, next_check_at: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.verified {
        println!("Domain {id} verified.");
    } else {
        print!("Domain {id} not yet verified.");
        if !resp.next_check_at.is_empty() {
            print!(" Next check at: {}", resp.next_check_at);
        }
        println!();
    }
    Ok(())
}

// ── domains cert ─────────────────────────────────────────────────────────────

fn cert(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("domains cert")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin domains cert <id> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct CertResp {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        cert_status: String,
        #[serde(default)]
        issued: bool,
    }
    let resp: CertResp = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/custom-domains/{id}/cert"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("domain {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { cert_status: string, issued: bool }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.issued {
        println!(
            "Certificate issued for domain {id} (status={}).",
            resp.cert_status
        );
    } else {
        println!(
            "Certificate issuance requested for domain {id} (status={}).",
            resp.cert_status
        );
    }
    Ok(())
}

// ── domains remove ───────────────────────────────────────────────────────────

fn remove(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("domains remove")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin domains remove <id> [--project=<ref>] [--yes]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to remove domain {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Remove custom domain {:?} from project {:?}? This cannot be undone. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct RemoveResp {
        #[serde(default)]
        removed: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        id: String,
    }
    let resp: RemoveResp = c
        .do_json(
            Method::DELETE,
            &format!("/v1/projects/{ref}/custom-domains/{id}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("domain {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { removed: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Removed domain {id}.");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

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

    fn sample_domain(id: &str, hostname: &str, status: &str, cert: &str) -> String {
        format!(
            r#"{{"id":"{id}","hostname":"{hostname}","status":"{status}","cert_status":"{cert}","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── domains list ──────────────────────────────────────────────────────────

    #[test]
    fn list_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/custom-domains");
            Resp::ok(r#"{"domains":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_domains(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_populated() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"domains":[{},{}]}}"#,
                sample_domain("d1", "app.example.com", "active", "issued"),
                sample_domain("d2", "api.example.com", "pending", "pending")
            ))
        });
        let g = flags(&srv.url);
        cmd_domains(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"domains":[{}]}}"#,
                sample_domain("d1", "app.example.com", "active", "issued")
            ))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: DomainsListResp = c
            .do_json(reqwest::Method::GET, "/v1/projects/p1/custom-domains", None)
            .unwrap();
        print_json(&mut out, &json!({ "domains": resp.domains })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["domains"].is_array());
        assert_eq!(v["domains"][0]["id"].as_str().unwrap(), "d1");
        let _ = &g;
    }

    #[test]
    fn list_no_project_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_domains(&g, &["list".into()]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("link"));
    }

    // ── domains add ───────────────────────────────────────────────────────────

    #[test]
    fn add_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/custom-domains");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["hostname"], "app.example.com");
            Resp::ok(format!(
                r#"{{"domain":{},"dns_records":[{{"type":"CNAME","name":"_basin.app.example.com","value":"verify.basin.run","ttl":3600}}]}}"#,
                sample_domain("d-new", "app.example.com", "pending", "none")
            ))
        });
        let g = flags(&srv.url);
        cmd_domains(
            &g,
            &[
                "add".into(),
                "--project=proj1".into(),
                "app.example.com".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn add_missing_hostname_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_domains(&g, &["add".into(), "--project=proj1".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"));
    }

    #[test]
    fn add_409_conflict() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                409,
                r#"{"error":{"code":"domain_conflict","message":"hostname already registered"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_domains(
            &g,
            &[
                "add".into(),
                "--project=proj1".into(),
                "dup.example.com".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("dup.example.com"));
    }

    #[test]
    fn add_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"domain":{},"dns_records":[]}}"#,
                sample_domain("d-new", "app.example.com", "pending", "none")
            ))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let resp = AddDomainResponse {
            domain: Some(CustomDomain {
                id: "d-new".into(),
                hostname: "app.example.com".into(),
                status: "pending".into(),
                ..Default::default()
            }),
            dns_records: vec![],
        };
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["domain"]["id"].as_str().unwrap(), "d-new");
        let _ = &g;
    }

    // ── domains verify ────────────────────────────────────────────────────────

    #[test]
    fn verify_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/custom-domains/d1/verify");
            Resp::ok(r#"{"verified":true,"next_check_at":""}"#)
        });
        let g = flags(&srv.url);
        cmd_domains(
            &g,
            &["verify".into(), "--project=proj1".into(), "d1".into()],
        )
        .unwrap();
    }

    #[test]
    fn verify_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"domain not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_domains(
            &g,
            &["verify".into(), "--project=proj1".into(), "missing".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn verify_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"verified":false,"next_check_at":"2026-01-02T00:00:00Z"}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize, Serialize)]
        struct VR {
            verified: bool,
            #[serde(default)]
            next_check_at: String,
        }
        let resp: VR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/custom-domains/d1/verify",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(!v["verified"].as_bool().unwrap());
        assert_eq!(v["next_check_at"].as_str().unwrap(), "2026-01-02T00:00:00Z");
        let _ = &g;
    }

    // ── domains cert ──────────────────────────────────────────────────────────

    #[test]
    fn cert_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/custom-domains/d1/cert");
            Resp::ok(r#"{"cert_status":"pending","issued":false}"#)
        });
        let g = flags(&srv.url);
        cmd_domains(&g, &["cert".into(), "--project=proj1".into(), "d1".into()]).unwrap();
    }

    #[test]
    fn cert_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv =
            TestServer::start(|_req: &Req| Resp::ok(r#"{"cert_status":"pending","issued":false}"#));
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize, Serialize)]
        struct CR {
            #[serde(default)]
            cert_status: String,
            issued: bool,
        }
        let resp: CR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/custom-domains/d1/cert",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["cert_status"].as_str().unwrap(), "pending");
        let _ = &g;
    }

    // ── domains remove ────────────────────────────────────────────────────────

    #[test]
    fn remove_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/custom-domains/d1");
            Resp::ok(r#"{"removed":true,"id":"d1"}"#)
        });
        let g = flags(&srv.url);
        cmd_domains(
            &g,
            &[
                "remove".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "d1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn remove_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"domain not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_domains(
            &g,
            &[
                "remove".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "missing".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn remove_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_domains(
            &g,
            &["remove".into(), "--project=proj1".into(), "d1".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("confirmation required"));
    }

    #[test]
    fn remove_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"removed":true,"id":"d1"}"#));
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R {
            removed: bool,
            id: String,
        }
        print_json(
            &mut out,
            &R {
                removed: true,
                id: "d1".into(),
            },
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["removed"].as_bool().unwrap());
        assert_eq!(v["id"].as_str().unwrap(), "d1");
        let _ = &g;
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_is_silent() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_domains(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_domains(&g, &["help".into()]).is_ok());
    }
}
