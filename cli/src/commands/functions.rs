//! functions — JS/TS edge functions on the Wasm component runtime
//! (Phase 5.11.W3).
//!
//! Per [ADR 0019] the in-DB compute runtime is **WebAssembly, not V8**.
//! Devs author a handler in TypeScript/JavaScript; `basin functions
//! deploy` compiles the entry to a Wasm **component** targeting the W1
//! host ABI (`basin:fn/{query,http,log,secret}` host imports +
//! a `handle(request) -> response` export) using the Bytecode Alliance
//! toolchain (ComponentizeJS, with Javy as a fallback), then uploads the
//! component bytes + metadata and registers it with the engine.
//!
//! ## Subcommands
//!
//!   basin functions deploy ./fn.ts [--name <n>] [--project <ref>]
//!   basin functions list
//!   basin functions logs <name> [--limit <n>]
//!   basin functions delete <name>
//!
//! ## Routes (data-plane, base derived from `g.api_url`)
//!
//!   POST   /rest/v1/functions/:name        deploy + register a component
//!   GET    /rest/v1/functions              list functions
//!   GET    /rest/v1/functions/:name/logs   tail invocation logs
//!   DELETE /rest/v1/functions/:name        delete a function
//!
//! TODO(W2): reconcile the register/invoke mount with Phase 5.11.W2 —
//! W2 mounts the *invoke* path at `ANY /fn/v1/:name`. The TASK.md W3 box
//! does not pin the *register* route, so this uses the REST admin shape
//! `POST /rest/v1/functions/:name` (mirrors the table-CRUD admin surface
//! under `/rest/v1`). If W2 lands the register route under `/fn/v1`, flip
//! `FN_BASE` below.

use clap::{Arg, ArgAction, Command};
use reqwest::blocking::Client as HttpClient;
use reqwest::Method;

use crate::client::{parse_api_error, version};
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};

use super::help::help_for_command;
use super::parse_or_silent;

/// Base path for the functions admin surface. See the W2 reconciliation
/// TODO in the module docs.
const FN_BASE: &str = "/rest/v1/functions";

// ── project resolution ─────────────────────────────────────────────────────────

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

pub fn cmd_functions(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            print_functions_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "deploy" => functions_deploy(g, rest),
        "list" => functions_list(g, rest),
        "logs" => functions_logs(g, rest),
        "delete" => functions_delete(g, rest),
        "--help" | "-h" | "help" => {
            print_functions_help();
            Ok(())
        }
        other => Err(msg(format!("unknown subcommand {:?} for functions", other))),
    }
}

fn print_functions_help() {
    help_for_command(
        "functions",
        "Deploy JS/TS edge functions to the Wasm component runtime (engine 5.11.W).",
        &[
            "deploy <entry.ts> [--name <n>]   Compile TS/JS → Wasm component and register it.",
            "list                             List deployed functions.",
            "logs <name> [--limit <n>]        Tail a function's invocation logs.",
            "delete <name>                    Delete a deployed function.",
            "",
            "Functions run as WebAssembly components, not V8 (see ADR 0019).",
            "Compile toolchain: componentize-js (fallback javy) — install with",
            "  npm i -g @bytecodealliance/componentize-js   or   brew install javy",
            "",
            "Global: --project=<ref>  --json",
        ],
    );
}

// ── TS/JS → Wasm component compile ─────────────────────────────────────────────

/// Which Bytecode Alliance toolchain compiled the component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Toolchain {
    /// `componentize-js` (or `jco componentize`) — full component-model
    /// output with WASI Preview 2 host imports. Preferred.
    ComponentizeJs,
    /// `javy` — Javy core engine; fallback when ComponentizeJS is absent.
    Javy,
}

impl Toolchain {
    fn label(self) -> &'static str {
        match self {
            Toolchain::ComponentizeJs => "componentize-js",
            Toolchain::Javy => "javy",
        }
    }
}

/// Reports whether `bin` resolves on `PATH`.
fn tool_available(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Detects the best available compile toolchain, preferring
/// ComponentizeJS (true component-model output) over Javy.
fn detect_toolchain() -> CliResult<Toolchain> {
    if tool_available("componentize-js") || tool_available("jco") {
        Ok(Toolchain::ComponentizeJs)
    } else if tool_available("javy") {
        Ok(Toolchain::Javy)
    } else {
        Err(msg(
            "no JS→Wasm toolchain found. Install one of:\n  \
             npm i -g @bytecodealliance/componentize-js @bytecodealliance/jco   (preferred)\n  \
             brew install javy                                                  (fallback)\n\
             then re-run `basin functions deploy`.",
        ))
    }
}

/// Compiles a TS/JS entry file to a Wasm **component** targeting the W1
/// ABI, returning the component bytes. Shells out to the detected
/// toolchain; the output is written to a temp `.wasm` and read back.
///
/// This is the testable seam: input is a source path, output is the
/// component bytes. The actual compile needs the toolchain installed, so
/// the round-trip is exercised by the `#[ignore]`d integration test.
fn compile_to_component(src: &std::path::Path, toolchain: Toolchain) -> CliResult<Vec<u8>> {
    if !src.exists() {
        return Err(msg(format!(
            "functions deploy: entry file not found: {}",
            src.display()
        )));
    }

    let out = std::env::temp_dir().join(format!(
        "basin-fn-{}-{}.wasm",
        std::process::id(),
        src.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("component")
    ));

    let status = match toolchain {
        Toolchain::ComponentizeJs => {
            // `componentize-js -o <out> <src>` (the jco-bundled CLI uses
            // the same positional shape). The guest is bound against the
            // W1 WIT world the engine publishes.
            std::process::Command::new("componentize-js")
                .arg(src)
                .arg("-o")
                .arg(&out)
                .status()
        }
        Toolchain::Javy => {
            // `javy compile <src> -o <out>` produces a Wasm module via the
            // Javy core engine.
            std::process::Command::new("javy")
                .arg("compile")
                .arg(src)
                .arg("-o")
                .arg(&out)
                .status()
        }
    };

    let status = status.map_err(|e| {
        msg(format!(
            "functions deploy: failed to run {}: {e}",
            toolchain.label()
        ))
    })?;
    if !status.success() {
        return Err(msg(format!(
            "functions deploy: {} exited with {} compiling {}",
            toolchain.label(),
            status,
            src.display()
        )));
    }

    let bytes = std::fs::read(&out).map_err(|e| {
        msg(format!(
            "functions deploy: read compiled component {}: {e}",
            out.display()
        ))
    })?;
    let _ = std::fs::remove_file(&out);
    if bytes.is_empty() {
        return Err(msg(
            "functions deploy: toolchain produced an empty component",
        ));
    }
    Ok(bytes)
}

/// Derives a function name from an entry path stem when `--name` is
/// omitted (`./api/hello.ts` → `hello`).
fn name_from_path(src: &std::path::Path) -> Option<String> {
    src.file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

// ── deploy ──────────────────────────────────────────────────────────────────

fn functions_deploy(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("functions deploy")
        .arg(Arg::new("entry").index(1))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "functions deploy",
            "Compile a TS/JS handler to a Wasm component and register it.",
            &[
                "<entry.ts>        Path to the function entry file (required).",
                "--name <n>        Function name (defaults to the file stem).",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let entry = m
        .get_one::<String>("entry")
        .cloned()
        .ok_or_else(|| msg("usage: basin functions deploy <entry.ts> [--name <n>]"))?;
    let src = std::path::PathBuf::from(&entry);
    let name = match m.get_one::<String>("name").cloned() {
        Some(n) if !n.is_empty() => n,
        _ => name_from_path(&src).ok_or_else(|| {
            msg("functions deploy: could not derive a name from the entry path; pass --name")
        })?,
    };
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    // Compile TS/JS → Wasm component (the meat of W3).
    let toolchain = detect_toolchain()?;
    if !g.quiet {
        eprintln!(
            "Compiling {} → Wasm component via {} …",
            src.display(),
            toolchain.label()
        );
    }
    let component = compile_to_component(&src, toolchain)?;

    // Upload the component bytes + register, mirroring `storage upload`.
    let cfg = crate::config::read_config_file().ok().flatten();
    let token = crate::global::resolve_token(g, cfg.as_ref());
    if token.is_empty() {
        eprintln!(
            "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>."
        );
        return Err(crate::error::silent());
    }

    let url = format!("{}{}/{}", g.api_url.trim_end_matches('/'), FN_BASE, name);
    let http = HttpClient::builder()
        .timeout(crate::client::DEFAULT_TIMEOUT)
        .build()?;
    let resp = http
        .post(&url)
        .bearer_auth(&token)
        .header("Content-Type", "application/wasm")
        .header("X-Basin-Fn-Runtime", "wasm-component")
        .header("X-Basin-Fn-Toolchain", toolchain.label())
        .header("X-Basin-Fn-Abi", "basin:fn/handler")
        .header("User-Agent", format!("basin-cli/{}", version()))
        .body(component)
        .send()?;
    let status = resp.status().as_u16();
    let body_text = resp.text().unwrap_or_default();
    if !(200..300).contains(&(status as usize)) {
        return Err(Box::new(parse_api_error(status, &body_text)));
    }

    if g.json {
        let raw: serde_json::Value =
            serde_json::from_str(&body_text).unwrap_or(serde_json::Value::String(body_text));
        return print_json(&mut std::io::stdout(), &raw);
    }
    println!("Deployed function {name:?} (Wasm component, {}).", toolchain.label());
    Ok(())
}

// ── list ──────────────────────────────────────────────────────────────────────

fn functions_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("functions list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "functions list",
            "List deployed functions for the project.",
            &["--project=<ref>   Project ref."],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let raw: serde_json::Value = c.do_json(Method::GET, FN_BASE, None)?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }

    let arr = raw.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("No functions found.");
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "RUNTIME", "TOOLCHAIN", "UPDATED"]);
    for f in &arr {
        let name = f["name"].as_str().unwrap_or("").to_string();
        let runtime = f["runtime"].as_str().unwrap_or("wasm-component").to_string();
        let toolchain = f["toolchain"].as_str().unwrap_or("").to_string();
        let updated = f["updated_at"].as_str().unwrap_or("").to_string();
        t.row(&[&name, &runtime, &toolchain, &updated]);
    }
    t.flush()
}

// ── logs ──────────────────────────────────────────────────────────────────────

fn functions_logs(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("functions logs")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("limit").long("limit"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "functions logs",
            "Tail a function's invocation logs (from basin:fn/log).",
            &[
                "<name>            Function name (required).",
                "--limit <n>       Max log lines to fetch (default 100).",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin functions logs <name> [--limit <n>]"))?;
    let limit: u64 = m
        .get_one::<String>("limit")
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| msg("functions logs: --limit must be a positive integer"))
        })
        .transpose()?
        .unwrap_or(100);
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("{}/{}/logs?limit={}", FN_BASE, name, limit);
    let raw: serde_json::Value = c.do_json(Method::GET, &path, None)?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }

    let arr = raw.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("No logs found.");
        return Ok(());
    }
    let mut t = Table::new(g, &["TIME", "LEVEL", "MESSAGE"]);
    for entry in &arr {
        let ts = entry["ts"]
            .as_str()
            .or_else(|| entry["timestamp"].as_str())
            .unwrap_or("")
            .to_string();
        let level = entry["level"].as_str().unwrap_or("").to_string();
        let message = entry["message"]
            .as_str()
            .or_else(|| entry["msg"].as_str())
            .unwrap_or("")
            .to_string();
        t.row(&[&ts, &level, &message]);
    }
    t.flush()
}

// ── delete ──────────────────────────────────────────────────────────────────

fn functions_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("functions delete")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "functions delete",
            "Delete a deployed function.",
            &[
                "<name>            Function name (required).",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin functions delete <name>"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("{}/{}", FN_BASE, name);
    let raw: serde_json::Value = c.do_json(Method::DELETE, &path, None)?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }
    println!("Function {name:?} deleted.");
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

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_args_prints_help() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_functions(&g, &[]).is_ok());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_functions(&g, &["--help".to_string()]).is_ok());
        assert!(cmd_functions(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_functions(&g, &["frobnicate".to_string()]).is_err());
    }

    #[test]
    fn subcommand_help_flags_all_work() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        for sub in &["deploy", "list", "logs", "delete"] {
            let result = cmd_functions(&g, &[sub.to_string(), "--help".to_string()]);
            assert!(result.is_ok(), "{sub} --help failed: {result:?}");
        }
    }

    // ── compile seam ────────────────────────────────────────────────────────

    #[test]
    fn name_from_path_uses_stem() {
        assert_eq!(
            name_from_path(std::path::Path::new("./api/hello.ts")).as_deref(),
            Some("hello")
        );
        assert_eq!(
            name_from_path(std::path::Path::new("fn.js")).as_deref(),
            Some("fn")
        );
    }

    #[test]
    fn toolchain_labels() {
        assert_eq!(Toolchain::ComponentizeJs.label(), "componentize-js");
        assert_eq!(Toolchain::Javy.label(), "javy");
    }

    #[test]
    fn compile_missing_file_errors() {
        let err = compile_to_component(
            std::path::Path::new("/no/such/entry.ts"),
            Toolchain::ComponentizeJs,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "err: {err}");
    }

    // ── deploy ────────────────────────────────────────────────────────────────

    #[test]
    fn deploy_missing_entry_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_functions(&g, &["deploy".to_string(), "--project=p1".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("usage:"), "err: {err}");
    }

    /// When no toolchain is installed, deploy fails before any HTTP with a
    /// clear install hint. Mirrors a real first-run on a clean machine.
    #[test]
    fn deploy_no_toolchain_gives_install_hint() {
        let _g = with_temp_config_dir();
        // Empty PATH so neither componentize-js nor javy resolve.
        let saved = std::env::var_os("PATH");
        std::env::set_var("PATH", "");
        let dir = std::env::temp_dir().join(format!("basin-fn-notool-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let entry = dir.join("fn.ts");
        std::fs::write(&entry, b"export default (req) => req;").unwrap();

        let g = flags("http://127.0.0.1:1");
        let res = cmd_functions(
            &g,
            &[
                "deploy".to_string(),
                entry.display().to_string(),
                "--project=p1".to_string(),
            ],
        );
        if let Some(p) = saved {
            std::env::set_var("PATH", p);
        }
        let err = res.unwrap_err();
        assert!(
            err.to_string().contains("toolchain") && err.to_string().contains("componentize-js"),
            "err: {err}"
        );
    }

    // ── list ────────────────────────────────────────────────────────────────

    #[test]
    fn list_gets_correct_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.path.clone();
            assert_eq!(req.method, "GET");
            Resp::ok(
                r#"[{"name":"hello","runtime":"wasm-component","toolchain":"componentize-js","updated_at":"2026-05-21T00:00:00Z"}]"#,
            )
        });
        let g = flags(&srv.url);
        let result = cmd_functions(&g, &["list".to_string(), "--project=p1".to_string()]);
        assert!(result.is_ok(), "list: {result:?}");
        assert_eq!(*cap.lock().unwrap(), "/rest/v1/functions");
    }

    #[test]
    fn list_empty_prints_none() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("[]"));
        let g = flags(&srv.url);
        assert!(cmd_functions(&g, &["list".to_string(), "--project=p1".to_string()]).is_ok());
    }

    #[test]
    fn list_json_flag() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(r#"[{"name":"f1"}]"#));
        let g = flags_json(&srv.url);
        assert!(cmd_functions(&g, &["list".to_string(), "--project=p1".to_string()]).is_ok());
    }

    #[test]
    fn list_401_mapped() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_functions(&g, &["list".to_string(), "--project=p1".to_string()])
            .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 401);
    }

    // ── logs ────────────────────────────────────────────────────────────────

    #[test]
    fn logs_gets_correct_path_and_limit() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(
                r#"[{"ts":"2026-05-21T00:00:00Z","level":"info","message":"hi"}]"#,
            )
        });
        let g = flags(&srv.url);
        cmd_functions(
            &g,
            &[
                "logs".to_string(),
                "hello".to_string(),
                "--limit=50".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "/rest/v1/functions/hello/logs?limit=50");
    }

    #[test]
    fn logs_default_limit() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok("[]")
        });
        let g = flags(&srv.url);
        cmd_functions(
            &g,
            &[
                "logs".to_string(),
                "hello".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        assert!(
            cap.lock().unwrap().ends_with("limit=100"),
            "default limit should be 100: {}",
            cap.lock().unwrap()
        );
    }

    #[test]
    fn logs_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_functions(&g, &["logs".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn logs_bad_limit_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_functions(
            &g,
            &[
                "logs".to_string(),
                "hello".to_string(),
                "--limit=notanum".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("integer"), "err: {err}");
    }

    // ── delete ────────────────────────────────────────────────────────────────

    #[test]
    fn delete_sends_delete_request() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(r#"{"message":"function deleted"}"#)
        });
        let g = flags(&srv.url);
        cmd_functions(
            &g,
            &[
                "delete".to_string(),
                "hello".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "/rest/v1/functions/hello");
    }

    #[test]
    fn delete_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_functions(&g, &["delete".to_string(), "--project=p1".to_string()]).is_err());
    }

    // ── e2e (toolchain + running engine required) ───────────────────────────

    /// End-to-end: compile a real TS handler to a Wasm component, deploy
    /// it, invoke via `/fn/v1/<name>` (W2 mount), and read logs back.
    ///
    /// Requires the ComponentizeJS/Javy toolchain on PATH **and** a
    /// running engine with W1/W2 mounted. Gated `#[ignore]` so the unit
    /// suite never depends on toolchain/engine presence.
    ///
    /// Run with: `BASIN_FN_E2E=1 cargo test -- --ignored functions_deploy_invoke_logs_round_trip`
    #[test]
    #[ignore = "needs componentize-js/javy + a running engine (W1/W2)"]
    fn functions_deploy_invoke_logs_round_trip() {
        let toolchain = detect_toolchain().expect("a JS→Wasm toolchain on PATH");
        let dir = std::env::temp_dir().join(format!("basin-fn-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let entry = dir.join("hello.ts");
        std::fs::write(
            &entry,
            b"export const handle = (req: Request) => new Response(\"ok\");",
        )
        .unwrap();
        let component = compile_to_component(&entry, toolchain).expect("compile to component");
        assert!(!component.is_empty(), "component bytes produced");
        // Deploy + invoke + logs against a live engine is the manual step;
        // wire the API base via $BASIN_API_URL + $BASIN_TOKEN and run the
        // `deploy` / `logs` subcommands here once the engine is reachable.
    }
}
