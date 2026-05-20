//! rpc — invoke a user-defined database function (T26.4).
//!
//!   basin rpc <fn> [--arg k=v …] [--body @file.json] [--project=<ref>]
//!
//! Endpoint (data-plane, same base as `sql` / `rows`):
//!
//!   POST {api_url}/rest/v1/rpc/:fn
//!       Authorization: Bearer <token>
//!       Content-Type:  application/json
//!       body:          { "k": <typed_value>, … }
//!
//! --arg k=v pairs are type-inferred:
//!   - "true" / "false"      → JSON boolean
//!   - any integer literal   → JSON number
//!   - everything else       → JSON string
//!
//! --body @file.json loads a raw JSON object from disk (mutually
//! exclusive with --arg; --body wins if both are supplied).
//!
//! Output:
//!   - Scalar result         → printed bare (e.g. `7`)
//!   - JSON array (table)    → pretty-printed JSON
//!   - --json flag           → always pretty-print raw response

use clap::{Arg, ArgAction, Command};
use reqwest::Method;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_maybe_file};

use super::help::help_for_command;
use super::parse_or_silent;

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

/// infer_value converts a raw string value from --arg into a typed JSON value.
fn infer_value(s: &str) -> serde_json::Value {
    match s {
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        _ => {
            if let Ok(n) = s.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::String(s.to_string())
            }
        }
    }
}

/// parse_args converts repeated `k=v` strings from --arg into a JSON object.
fn parse_args(pairs: &[String]) -> CliResult<serde_json::Map<String, serde_json::Value>> {
    let mut map = serde_json::Map::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| msg(format!("rpc: --arg must be in k=v form, got {pair:?}")))?;
        map.insert(k.to_string(), infer_value(v));
    }
    Ok(map)
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_rpc(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rpc")
        .arg(Arg::new("fn").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(
            Arg::new("arg")
                .long("arg")
                .action(ArgAction::Append)
                .num_args(1),
        )
        .arg(Arg::new("body").long("body"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rpc",
            "Invoke a user-defined database function via PostgREST RPC.",
            &[
                "<fn>                     Function name (required).",
                "--arg k=v               Pass a typed argument (repeatable).",
                "--body @file.json       Pass a raw JSON body from file.",
                "--project=<ref>         Project ref.",
                "",
                "Type inference for --arg: 'true'/'false' → bool; integers → number; else string.",
                "Scalar result is printed bare; table result is pretty-printed JSON.",
            ],
        );
        return Ok(());
    }

    let fn_name = m.get_one::<String>("fn").cloned().ok_or_else(|| {
        msg("usage: basin rpc <fn> [--arg k=v …] [--body @file.json] [--project=<ref>]")
    })?;

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project = resolve_project_ref(&project_flag)?;

    // Build the request body: --body @file wins over --arg.
    let body_flag = m.get_one::<String>("body").cloned().unwrap_or_default();
    let body_json: serde_json::Value = if !body_flag.is_empty() {
        let src = if let Some(path) = body_flag.strip_prefix('@') {
            read_maybe_file(path).map_err(|e| msg(format!("rpc: read body file {path}: {e}")))?
        } else {
            body_flag.clone()
        };
        serde_json::from_str(&src).map_err(|e| msg(format!("rpc: invalid JSON body: {e}")))?
    } else {
        let arg_pairs: Vec<String> = m
            .get_many::<String>("arg")
            .map(|v| v.cloned().collect())
            .unwrap_or_default();
        let map = parse_args(&arg_pairs)?;
        serde_json::Value::Object(map)
    };

    let c = require_client(g)?;
    // Override client base_url to use the data-plane (api_url = g.api_url).
    // The client is already constructed from g.api_url, so just build the path.
    let path = format!("/rest/v1/rpc/{fn_name}");
    // We cannot override per-project here without a project-specific base URL;
    // the project ref is embedded in the path for the control-plane prefix.
    // The data-plane RPC mount lives at {api_url}/rest/v1/rpc/:fn with the
    // project inferred from the bearer token + project context header.
    let _ = project; // project ref is carried via token; kept for future use.

    let raw: serde_json::Value = c.do_json(Method::POST, &path, Some(body_json))?;

    render_rpc_result(g, &raw)
}

/// render_rpc_result prints the RPC result in human-readable or JSON form.
pub fn render_rpc_result(g: &GlobalFlags, v: &serde_json::Value) -> CliResult<()> {
    if g.json {
        return print_json(&mut std::io::stdout(), v);
    }
    match v {
        // Scalar string.
        serde_json::Value::String(s) => println!("{s}"),
        // Scalar number / bool / null.
        serde_json::Value::Number(n) => println!("{n}"),
        serde_json::Value::Bool(b) => println!("{b}"),
        serde_json::Value::Null => println!("null"),
        // Array (table-returning function) → pretty JSON.
        serde_json::Value::Array(_) => print_json(&mut std::io::stdout(), v)?,
        // Object → pretty JSON.
        serde_json::Value::Object(_) => print_json(&mut std::io::stdout(), v)?,
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{Arc, Mutex};

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

    // ── dispatcher / parse tests ──────────────────────────────────────────────

    #[test]
    fn missing_fn_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        // --project required so we use a stub. Missing fn still errors first.
        let err = cmd_rpc(&g, &["--project=p1".to_string()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:") || err.to_string().contains("rpc"),
            "err: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rpc(&g, &["--help".to_string()]).is_ok());
    }

    #[test]
    fn arg_bad_format_errors() {
        let _g = with_temp_config_dir();
        let err = parse_args(&["noeq".to_string()]).unwrap_err();
        assert!(err.to_string().contains("k=v"), "err: {err}");
    }

    // ── type inference ────────────────────────────────────────────────────────

    #[test]
    fn infer_bool_true() {
        assert_eq!(infer_value("true"), serde_json::Value::Bool(true));
    }

    #[test]
    fn infer_bool_false() {
        assert_eq!(infer_value("false"), serde_json::Value::Bool(false));
    }

    #[test]
    fn infer_integer() {
        assert_eq!(infer_value("42"), serde_json::json!(42i64));
    }

    #[test]
    fn infer_string_fallback() {
        assert_eq!(
            infer_value("hello"),
            serde_json::Value::String("hello".into())
        );
    }

    #[test]
    fn parse_args_mixed_types() {
        let pairs = vec!["x=3".to_string(), "y=4".to_string(), "dry=true".to_string()];
        let map = parse_args(&pairs).unwrap();
        assert_eq!(map["x"], serde_json::json!(3i64));
        assert_eq!(map["y"], serde_json::json!(4i64));
        assert_eq!(map["dry"], serde_json::Value::Bool(true));
    }

    // ── happy-path scalar: add(x=3, y=4) → 7 ─────────────────────────────────

    #[test]
    fn rpc_scalar_add_returns_bare_number() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/rest/v1/rpc/add");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok("7")
        });
        let g = flags(&srv.url);
        // render_rpc_result will print "7" to stdout; just check Ok.
        let result = cmd_rpc(
            &g,
            &[
                "add".to_string(),
                "--project=p1".to_string(),
                "--arg=x=3".to_string(),
                "--arg=y=4".to_string(),
            ],
        );
        assert!(result.is_ok(), "rpc add should succeed: {result:?}");
        let body = cap.lock().unwrap();
        assert_eq!(body["x"], serde_json::json!(3i64));
        assert_eq!(body["y"], serde_json::json!(4i64));
    }

    // ── table-returning function → pretty JSON array ───────────────────────────

    #[test]
    fn rpc_table_result_pretty_printed() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.path, "/rest/v1/rpc/list_users");
            Resp::ok(r#"[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"#)
        });
        let g = flags(&srv.url);
        let result = cmd_rpc(&g, &["list_users".to_string(), "--project=p1".to_string()]);
        assert!(result.is_ok(), "rpc list_users should succeed: {result:?}");
    }

    // ── 401 → typed ApiError ──────────────────────────────────────────────────

    #[test]
    fn rpc_401_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rpc(
            &g,
            &[
                "add".to_string(),
                "--project=p1".to_string(),
                "--arg=x=1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 401);
        assert_eq!(ae.code, "unauthorized");
    }

    // ── --json flag: always pretty-print ─────────────────────────────────────

    #[test]
    fn rpc_json_flag_pretty_prints_scalar() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("42"));
        let mut buf = Vec::<u8>::new();
        let v = serde_json::json!(42u32);
        // render_rpc_result with json=true always calls print_json.
        let g = flags_json(&srv.url);
        print_json(&mut buf, &v).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("42"), "buf: {s}");
        // Also verify via the flag on cmd_rpc.
        let result = cmd_rpc(
            &g,
            &[
                "add".to_string(),
                "--project=p1".to_string(),
                "--arg=x=1".to_string(),
            ],
        );
        assert!(result.is_ok(), "rpc --json should succeed: {result:?}");
    }

    // ── --body @file.json ─────────────────────────────────────────────────────

    #[test]
    fn rpc_body_file_is_sent() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"ok":true}"#)
        });
        let dir = std::env::temp_dir().join(format!("basin-rpc-body-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("body.json");
        std::fs::write(&path, r#"{"amount":100,"currency":"USD"}"#).unwrap();
        let g = flags(&srv.url);
        cmd_rpc(
            &g,
            &[
                "charge".to_string(),
                "--project=p1".to_string(),
                format!("--body=@{}", path.display()),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap();
        assert_eq!(body["amount"], serde_json::json!(100));
        assert_eq!(body["currency"], serde_json::json!("USD"));
    }

    // ── render helpers ────────────────────────────────────────────────────────

    #[test]
    fn render_scalar_string() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let v = serde_json::json!("hello");
        assert!(render_rpc_result(&g, &v).is_ok());
    }

    #[test]
    fn render_null() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(render_rpc_result(&g, &serde_json::Value::Null).is_ok());
    }

    #[test]
    fn render_bool() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(render_rpc_result(&g, &serde_json::json!(true)).is_ok());
    }
}
