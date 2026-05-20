//! erd — Entity-Relationship Diagram export for a project.
//!
//!   basin erd export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]
//!
//! Fetches GET /v1/projects/{ref}/erd?format=<format> and streams the response
//! body to --output=<path>, or stdout when --output is absent.
//!
//! For --format=json the cloud returns:
//!   { "tables": [...], "relationships": [...] }
//!
//! For --format=svg and --format=dot the response body is raw text/SVG or
//! Graphviz DOT source.
//!
//! Project ref resolution: --project= flag, else loadWorkingProject(cwd).

use std::io::Write;

use clap::{Arg, ArgAction, Command};
use serde::{Deserialize, Serialize};

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::print_json;
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// ERDColumn is a column entry inside an ERDTable.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ERDColumn {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub pk: bool,
}

/// ERDTable is one table entry in the JSON ERD response.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ERDTable {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ERDColumn>,
}

/// ERDRelationship describes one inferred or explicit FK relationship.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ERDRelationship {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from_table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from_column: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_table: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_column: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub via: String,
}

/// ERDResponse is the JSON shape from GET /v1/projects/{ref}/erd?format=json.
/// JSON shape: { "tables": [...], "relationships": [...] }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ERDResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<ERDTable>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationships: Vec<ERDRelationship>,
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

pub fn cmd_erd(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            printerr!(
                g,
                "usage: basin erd export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]"
            );
            return Err(silent());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "export" => export(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "erd",
                "Export the project Entity-Relationship Diagram.",
                &[
                    "export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]",
                    "  --format  Output format: svg (default), dot, or json.",
                    "  --output  Write to this path (defaults to stdout).",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for erd", other);
            Err(silent())
        }
    }
}

// ── erd export ────────────────────────────────────────────────────────────────

fn export(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("erd export")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("format").long("format").default_value("svg"))
        .arg(Arg::new("output").long("output"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "erd export",
            "Export the project Entity-Relationship Diagram.",
            &[
                "--project <ref>        Project ref (required, or use basin link).",
                "--format svg|dot|json  Output format (default: svg).",
                "--output <path>        Write to this file (default: stdout).",
            ],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;

    let format = m
        .get_one::<String>("format")
        .cloned()
        .unwrap_or_else(|| "svg".into());
    match format.as_str() {
        "svg" | "dot" | "json" => {}
        _ => {
            return Err(msg(format!(
                "--format must be svg, dot, or json (got {:?})",
                format
            )));
        }
    }

    let output_path = m.get_one::<String>("output").cloned().unwrap_or_default();

    let c = require_client(g)?;

    // Build URL with format query param.
    let path = format!("/v1/projects/{ref}/erd?format={format}");

    // Use the client's token and base_url but do a raw request so we can
    // stream the body without JSON-decoding it for svg/dot.
    let url = format!("{}{}", c.base_url, path);
    let http = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| msg(format!("build http client: {e}")))?;

    let mut req = http
        .request(reqwest::Method::GET, &url)
        .header(
            "User-Agent",
            format!("basin-cli/{}", crate::client::version()),
        )
        .header(
            "Accept",
            "application/json, image/svg+xml, text/vnd.graphviz, text/plain, */*",
        );
    if !c.token.is_empty() {
        req = req.bearer_auth(&c.token);
    }

    let resp = req.send().map_err(|e| msg(format!("erd export: {e}")))?;
    let status = resp.status();
    let body = resp
        .bytes()
        .map_err(|e| msg(format!("erd export: read response: {e}")))?;

    if !status.is_success() {
        // Parse error the same way client.rs does.
        let ae = crate::client::parse_api_error(status.as_u16(), &String::from_utf8_lossy(&body));
        return Err(Box::new(ae));
    }

    // For JSON format with --json flag: decode into typed struct and pretty-print.
    if format == "json" && g.json {
        // The cloud may wrap in {"data":{...}} — unwrap the same way unwrap_envelope does.
        let text = String::from_utf8_lossy(&body);
        let erd: ERDResponse = match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(v) => {
                if let Some(data) = v.get("data") {
                    if !data.is_null() {
                        serde_json::from_value(data.clone())
                            .map_err(|e| msg(format!("erd export: decode JSON data: {e}")))?
                    } else {
                        serde_json::from_value(v)
                            .map_err(|e| msg(format!("erd export: decode JSON: {e}")))?
                    }
                } else {
                    serde_json::from_value(v)
                        .map_err(|e| msg(format!("erd export: decode JSON: {e}")))?
                }
            }
            Err(e) => return Err(msg(format!("erd export: decode JSON: {e}"))),
        };
        // JSON shape: { "tables": [...], "relationships": [...] }
        return print_json(&mut std::io::stdout(), &erd);
    }

    // Determine output destination.
    if !output_path.is_empty() {
        let mut f = std::fs::File::create(&output_path)
            .map_err(|e| msg(format!("erd export: create output file: {e}")))?;
        f.write_all(&body)
            .map_err(|e| msg(format!("erd export: write output: {e}")))?;
    } else {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        out.write_all(&body)
            .map_err(|e| msg(format!("erd export: write output: {e}")))?;
        // Add trailing newline for svg/dot if output is stdout and body doesn't end with one.
        if format != "json" && !body.is_empty() && body.last() != Some(&b'\n') {
            writeln!(out).map_err(|e| msg(format!("erd export: write newline: {e}")))?;
        }
    }

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

    fn make_erd_server() -> TestServer {
        TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            let format = if req.path.contains("format=svg") {
                "svg"
            } else if req.path.contains("format=dot") {
                "dot"
            } else if req.path.contains("format=json") {
                "json"
            } else {
                "svg" // default
            };
            match format {
                "json" => Resp::ok(
                    r#"{"tables":[{"name":"users","schema":"public"}],"relationships":[]}"#,
                ),
                "svg" => Resp::ok("<svg xmlns='http://www.w3.org/2000/svg'><g/></svg>"),
                "dot" => Resp::ok("digraph G { users; }"),
                _ => Resp::status(400, r#"{"code":"bad_format"}"#),
            }
        })
    }

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_erd(&g, &[]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_erd(&g, &["render".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_erd(&g, &["help".to_string()]).is_ok());
    }

    // ── erd export -- format=svg ────────────────────────────────────────────

    #[test]
    fn export_svg_happy_path() {
        let _g = with_temp_config_dir();
        let srv = make_erd_server();
        let g = flags(&srv.url);
        // This should succeed and write SVG to stdout.
        cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=svg".to_string(),
            ],
        )
        .unwrap();
    }

    // ── erd export -- format=dot ────────────────────────────────────────────

    #[test]
    fn export_dot_happy_path() {
        let _g = with_temp_config_dir();
        let srv = make_erd_server();
        let g = flags(&srv.url);
        cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=dot".to_string(),
            ],
        )
        .unwrap();
    }

    // ── erd export -- format=json --json shape lock ─────────────────────────

    #[test]
    fn export_json_shape() {
        let _g = with_temp_config_dir();
        let srv = make_erd_server();
        let g = flags_json(&srv.url);
        // With --json flag: decode into ERDResponse and pretty-print.
        // Just verify it doesn't error; the JSON is verified in the struct test below.
        cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=json".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn export_json_struct_shape() {
        let _g = with_temp_config_dir();
        // Verify the ERDResponse deserialises correctly.
        let json_body = r#"{"tables":[{"name":"users","schema":"public"}],"relationships":[]}"#;
        let erd: ERDResponse = serde_json::from_str(json_body).unwrap();
        assert_eq!(erd.tables.len(), 1);
        assert_eq!(erd.tables[0].name, "users");
        assert!(erd.relationships.is_empty());
    }

    // ── erd export -- format=json raw passthrough (without --json) ───────────

    #[test]
    fn export_json_raw_passthrough() {
        let _g = with_temp_config_dir();
        let srv = make_erd_server();
        let g = flags(&srv.url); // no --json flag
        cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=json".to_string(),
            ],
        )
        .unwrap();
    }

    // ── erd export -- output to file ────────────────────────────────────────

    #[test]
    fn export_to_file() {
        let _g = with_temp_config_dir();
        let srv = make_erd_server();
        let g = flags(&srv.url);
        let tmp_dir = std::env::temp_dir().join(format!("erd-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let out_path = tmp_dir.join("erd.svg");
        let out_str = out_path.to_str().unwrap().to_string();
        cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=svg".to_string(),
                format!("--output={out_str}"),
            ],
        )
        .unwrap();
        let content = std::fs::read_to_string(&out_path).unwrap();
        assert!(
            content.contains("<svg"),
            "expected SVG in file, got: {content:?}"
        );
    }

    // ── erd export -- invalid format ────────────────────────────────────────

    #[test]
    fn export_invalid_format_errors() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{}"#));
        let g = flags(&srv.url);
        let err = cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=pdf".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--format"), "err={err}");
    }

    // ── erd export -- missing project ───────────────────────────────────────

    #[test]
    fn export_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_erd(&g, &["export".to_string()]).unwrap_err();
        assert!(
            err.is::<crate::error::Silent>() || err.to_string().contains("project"),
            "err={err}"
        );
    }

    // ── erd export -- 404 error mapping ─────────────────────────────────────

    #[test]
    fn export_404_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_erd(
            &g,
            &[
                "export".to_string(),
                "--project=proj1".to_string(),
                "--format=svg".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }
}
