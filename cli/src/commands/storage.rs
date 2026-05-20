//! storage — object storage bucket and object management (Phase 5.17).
//!
//! ## Subcommands
//!
//!   basin storage buckets list
//!   basin storage buckets create <name> [--public]
//!   basin storage buckets delete <name>
//!   basin storage upload <bucket> <path> --file <localfile>
//!   basin storage download <bucket> <path> [--out <localfile>]
//!   basin storage list <bucket> [--prefix p]
//!   basin storage rm <bucket> <path...>
//!   basin storage sign <bucket> <path> [--expires-in 3600]
//!
//! ## Routes (data-plane, same base as `rpc` / `realtime` — derived from `g.api_url`)
//!
//!   POST   /storage/v1/bucket                       create bucket
//!   GET    /storage/v1/bucket                       list buckets
//!   GET    /storage/v1/bucket/:name                 get bucket metadata
//!   DELETE /storage/v1/bucket/:name                 delete bucket
//!   POST   /storage/v1/object/:bucket/*path         upload object
//!   GET    /storage/v1/object/:bucket/*path         download object
//!   DELETE /storage/v1/object/:bucket/*path         single delete
//!   POST   /storage/v1/object/list/:bucket          list objects (prefix in body)
//!   DELETE /storage/v1/object/:bucket               bulk delete ({prefixes:[]})
//!   POST   /storage/v1/object/sign/:bucket/*path    mint signed URL

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

// ── project resolution ────────────────────────────────────────────────────────

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

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_storage(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            print_storage_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "buckets" => storage_buckets(g, rest),
        "upload" => storage_upload(g, rest),
        "download" => storage_download(g, rest),
        "list" => storage_list(g, rest),
        "rm" => storage_rm(g, rest),
        "sign" => storage_sign(g, rest),
        "--help" | "-h" | "help" => {
            print_storage_help();
            Ok(())
        }
        other => Err(msg(format!("unknown subcommand {:?} for storage", other))),
    }
}

fn print_storage_help() {
    help_for_command(
        "storage",
        "Manage object storage buckets and objects (engine 5.17).",
        &[
            "buckets list                              List all buckets.",
            "buckets create <name> [--public]          Create a bucket.",
            "buckets delete <name>                     Delete a bucket.",
            "upload <bucket> <path> --file <file>      Upload an object.",
            "download <bucket> <path> [--out <file>]   Download an object.",
            "list <bucket> [--prefix p]                List objects in a bucket.",
            "rm <bucket> <path…>                       Bulk-delete objects by prefix.",
            "sign <bucket> <path> [--expires-in 3600]  Mint a signed URL.",
            "",
            "Global: --project=<ref>  --json",
        ],
    );
}

// ── buckets dispatcher ────────────────────────────────────────────────────────

fn storage_buckets(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "storage buckets",
                "Manage object storage buckets.",
                &[
                    "list                   List all buckets.",
                    "create <name> [--public]  Create a bucket.",
                    "delete <name>           Delete a bucket.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => buckets_list(g, rest),
        "create" => buckets_create(g, rest),
        "delete" => buckets_delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "storage buckets",
                "Manage object storage buckets.",
                &[
                    "list                      List all buckets.",
                    "create <name> [--public]  Create a bucket.",
                    "delete <name>             Delete a bucket.",
                ],
            );
            Ok(())
        }
        other => Err(msg(format!(
            "unknown subcommand {:?} for storage buckets",
            other
        ))),
    }
}

// ── buckets list ──────────────────────────────────────────────────────────────

fn buckets_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage buckets list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage buckets list",
            "List all buckets for the project.",
            &["--project=<ref>   Project ref."],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let raw: serde_json::Value = c.do_json(Method::GET, "/storage/v1/bucket", None)?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }

    let arr = raw.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("No buckets found.");
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "PUBLIC", "CREATED"]);
    for b in &arr {
        let name = b["name"].as_str().unwrap_or("").to_string();
        let public = b["public"].as_bool().unwrap_or(false).to_string();
        let created = b["created_at"].as_str().unwrap_or("").to_string();
        t.row(&[&name, &public, &created]);
    }
    t.flush()
}

// ── buckets create ────────────────────────────────────────────────────────────

fn buckets_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage buckets create")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("public").long("public").action(ArgAction::SetTrue))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage buckets create",
            "Create a new object storage bucket.",
            &[
                "<name>            Bucket name (required).",
                "--public          Make the bucket publicly readable.",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let name = m.get_one::<String>("name").cloned().ok_or_else(|| {
        msg("usage: basin storage buckets create <name> [--public] [--project=<ref>]")
    })?;
    let public = m.get_flag("public");
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let body = serde_json::json!({"name": name, "public": public});
    let raw: serde_json::Value = c.do_json(Method::POST, "/storage/v1/bucket", Some(body))?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }
    let visibility = if public { "public" } else { "private" };
    println!("Bucket {name:?} created ({visibility}).");
    Ok(())
}

// ── buckets delete ────────────────────────────────────────────────────────────

fn buckets_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage buckets delete")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage buckets delete",
            "Delete a bucket (contents not purged in engine 5.17.B).",
            &[
                "<name>            Bucket name (required).",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage buckets delete <name> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/storage/v1/bucket/{}", name);
    let raw: serde_json::Value = c.do_json(Method::DELETE, &path, None)?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }
    println!("Bucket {name:?} deleted.");
    Ok(())
}

// ── upload ────────────────────────────────────────────────────────────────────

fn storage_upload(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage upload")
        .arg(Arg::new("bucket").index(1))
        .arg(Arg::new("path").index(2))
        .arg(Arg::new("file").long("file"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage upload",
            "Upload a local file as an object.",
            &[
                "<bucket>           Bucket name (required).",
                "<path>             Object path within the bucket (required).",
                "--file <localfile> Local file to upload (required).",
                "--project=<ref>    Project ref.",
            ],
        );
        return Ok(());
    }

    let bucket = m
        .get_one::<String>("bucket")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage upload <bucket> <path> --file <localfile>"))?;
    let obj_path = m
        .get_one::<String>("path")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage upload <bucket> <path> --file <localfile>"))?;
    let file_path = m
        .get_one::<String>("file")
        .cloned()
        .ok_or_else(|| msg("storage upload: --file is required"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let bytes = std::fs::read(&file_path)
        .map_err(|e| msg(format!("storage upload: read {file_path}: {e}")))?;

    // Guess MIME type from extension; fall back to octet-stream.
    let mime = guess_mime(&file_path);

    // Build the token for auth — reuse require_client to get credentials.
    let cfg = crate::config::read_config_file().ok().flatten();
    let token = crate::global::resolve_token(g, cfg.as_ref());
    if token.is_empty() {
        eprintln!(
            "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>."
        );
        return Err(crate::error::silent());
    }

    let url = format!(
        "{}/storage/v1/object/{}/{}",
        g.api_url.trim_end_matches('/'),
        bucket,
        obj_path
    );
    let http = HttpClient::builder()
        .timeout(crate::client::DEFAULT_TIMEOUT)
        .build()?;
    let resp = http
        .post(&url)
        .bearer_auth(&token)
        .header("Content-Type", mime)
        .header("User-Agent", format!("basin-cli/{}", version()))
        .body(bytes)
        .send()?;
    let status = resp.status().as_u16();
    let body_text = resp.text().unwrap_or_default();
    if !(200..300).contains(&(status as usize)) {
        return Err(Box::new(parse_api_error(status, &body_text)));
    }

    if g.json {
        let raw: serde_json::Value = serde_json::from_str(&body_text)
            .unwrap_or(serde_json::Value::String(body_text.clone()));
        return print_json(&mut std::io::stdout(), &raw);
    }
    println!("Uploaded {obj_path} to bucket {bucket:?}.");
    Ok(())
}

/// guess_mime returns a basic MIME type from the file extension, defaulting
/// to `application/octet-stream`.
fn guess_mime(path: &str) -> &'static str {
    let p = path.to_ascii_lowercase();
    if p.ends_with(".json") {
        "application/json"
    } else if p.ends_with(".txt") {
        "text/plain"
    } else if p.ends_with(".html") || p.ends_with(".htm") {
        "text/html"
    } else if p.ends_with(".csv") {
        "text/csv"
    } else if p.ends_with(".png") {
        "image/png"
    } else if p.ends_with(".jpg") || p.ends_with(".jpeg") {
        "image/jpeg"
    } else if p.ends_with(".gif") {
        "image/gif"
    } else if p.ends_with(".pdf") {
        "application/pdf"
    } else if p.ends_with(".zip") {
        "application/zip"
    } else {
        "application/octet-stream"
    }
}

// ── download ──────────────────────────────────────────────────────────────────

fn storage_download(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage download")
        .arg(Arg::new("bucket").index(1))
        .arg(Arg::new("path").index(2))
        .arg(Arg::new("out").long("out"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage download",
            "Download an object to stdout or a local file.",
            &[
                "<bucket>          Bucket name (required).",
                "<path>            Object path within the bucket (required).",
                "--out <localfile> Write to this file instead of stdout.",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let bucket = m
        .get_one::<String>("bucket")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage download <bucket> <path> [--out <localfile>]"))?;
    let obj_path = m
        .get_one::<String>("path")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage download <bucket> <path> [--out <localfile>]"))?;
    let out_file = m.get_one::<String>("out").cloned().unwrap_or_default();
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let cfg = crate::config::read_config_file().ok().flatten();
    let token = crate::global::resolve_token(g, cfg.as_ref());
    if token.is_empty() {
        eprintln!(
            "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>."
        );
        return Err(crate::error::silent());
    }

    let url = format!(
        "{}/storage/v1/object/{}/{}",
        g.api_url.trim_end_matches('/'),
        bucket,
        obj_path
    );
    let http = HttpClient::builder()
        .timeout(crate::client::DEFAULT_TIMEOUT)
        .build()?;
    let resp = http
        .get(&url)
        .bearer_auth(&token)
        .header("User-Agent", format!("basin-cli/{}", version()))
        .send()?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&(status as usize)) {
        let body_text = resp.text().unwrap_or_default();
        return Err(Box::new(parse_api_error(status, &body_text)));
    }
    let bytes = resp.bytes()?;

    if out_file.is_empty() {
        use std::io::Write;
        std::io::stdout().write_all(&bytes)?;
    } else {
        std::fs::write(&out_file, &bytes)
            .map_err(|e| msg(format!("storage download: write {out_file}: {e}")))?;
        if !g.quiet {
            eprintln!("Written {} bytes to {out_file}.", bytes.len());
        }
    }
    Ok(())
}

// ── list ──────────────────────────────────────────────────────────────────────

fn storage_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage list")
        .arg(Arg::new("bucket").index(1))
        .arg(Arg::new("prefix").long("prefix"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage list",
            "List objects in a bucket.",
            &[
                "<bucket>          Bucket name (required).",
                "--prefix p        Filter by path prefix.",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let bucket = m
        .get_one::<String>("bucket")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage list <bucket> [--prefix p]"))?;
    let prefix = m.get_one::<String>("prefix").cloned().unwrap_or_default();
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/storage/v1/object/list/{}", bucket);
    let body = if prefix.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::json!({"prefix": prefix})
    };
    let raw: serde_json::Value = c.do_json(Method::POST, &path, Some(body))?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }

    let arr = raw.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("No objects found.");
        return Ok(());
    }
    let mut t = Table::new(g, &["PATH", "SIZE", "MIME", "UPDATED"]);
    for obj in &arr {
        let p = obj["path"].as_str().unwrap_or("").to_string();
        let size = obj["size"].as_u64().map(|s| s.to_string()).unwrap_or_default();
        let mime = obj["mime_type"].as_str().unwrap_or("").to_string();
        let updated = obj["updated_at"].as_str().unwrap_or("").to_string();
        t.row(&[&p, &size, &mime, &updated]);
    }
    t.flush()
}

// ── rm (bulk delete) ──────────────────────────────────────────────────────────

fn storage_rm(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage rm")
        .arg(Arg::new("bucket").index(1))
        .arg(
            Arg::new("paths")
                .index(2)
                .action(ArgAction::Append)
                .num_args(1..),
        )
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage rm",
            "Bulk-delete objects by prefix.",
            &[
                "<bucket>          Bucket name (required).",
                "<path…>           One or more object paths / prefixes (required).",
                "--project=<ref>   Project ref.",
            ],
        );
        return Ok(());
    }

    let bucket = m
        .get_one::<String>("bucket")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage rm <bucket> <path…>"))?;
    let prefixes: Vec<String> = m
        .get_many::<String>("paths")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if prefixes.is_empty() {
        return Err(msg("storage rm: at least one path is required"));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/storage/v1/object/{}", bucket);
    let body = serde_json::json!({"prefixes": prefixes});
    let raw: serde_json::Value = c.do_json(Method::DELETE, &path, Some(body))?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }
    let deleted = raw["deleted"].as_u64().unwrap_or(0);
    println!("Deleted {deleted} object(s) from bucket {bucket:?}.");
    Ok(())
}

// ── sign ──────────────────────────────────────────────────────────────────────

fn storage_sign(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("storage sign")
        .arg(Arg::new("bucket").index(1))
        .arg(Arg::new("path").index(2))
        .arg(Arg::new("expires-in").long("expires-in"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "storage sign",
            "Mint a signed download URL for an object.",
            &[
                "<bucket>              Bucket name (required).",
                "<path>                Object path within the bucket (required).",
                "--expires-in <secs>   TTL in seconds (default 3600; max 604800).",
                "--project=<ref>       Project ref.",
            ],
        );
        return Ok(());
    }

    let bucket = m
        .get_one::<String>("bucket")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage sign <bucket> <path> [--expires-in 3600]"))?;
    let obj_path = m
        .get_one::<String>("path")
        .cloned()
        .ok_or_else(|| msg("usage: basin storage sign <bucket> <path> [--expires-in 3600]"))?;
    let expires_in: u64 = m
        .get_one::<String>("expires-in")
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| msg("storage sign: --expires-in must be a positive integer"))
        })
        .transpose()?
        .unwrap_or(3600);
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let _ = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/storage/v1/object/sign/{}/{}", bucket, obj_path);
    let body = serde_json::json!({"expires_in": expires_in});
    let raw: serde_json::Value = c.do_json(Method::POST, &path, Some(body))?;

    if g.json {
        return print_json(&mut std::io::stdout(), &raw);
    }
    let signed_url = raw["signedUrl"]
        .as_str()
        .or_else(|| raw["signed_url"].as_str())
        .unwrap_or("");
    println!("{signed_url}");
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
        assert!(cmd_storage(&g, &[]).is_ok());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["--help".to_string()]).is_ok());
        assert!(cmd_storage(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["frobnicate".to_string()]).is_err());
    }

    // ── buckets list ──────────────────────────────────────────────────────────

    #[test]
    fn buckets_list_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            *c2.lock().unwrap() = req.path.clone();
            assert_eq!(req.method, "GET");
            Resp::ok(
                r#"[{"name":"my-bucket","public":false,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}]"#,
            )
        });
        let g = flags(&srv.url);
        let result = cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
            ],
        );
        assert!(result.is_ok(), "buckets list: {result:?}");
        let path = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/bucket", "path: {path}");
    }

    #[test]
    fn buckets_list_empty_prints_none() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("[]"));
        let g = flags(&srv.url);
        let result = cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
            ],
        );
        assert!(result.is_ok(), "empty list: {result:?}");
    }

    #[test]
    fn buckets_list_json_flag() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::ok(r#"[{"name":"b1","public":true}]"#)
        });
        let g = flags_json(&srv.url);
        let result = cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
            ],
        );
        assert!(result.is_ok(), "buckets list --json: {result:?}");
    }

    // ── buckets create ────────────────────────────────────────────────────────

    #[test]
    fn buckets_create_posts_name_and_public() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/storage/v1/bucket");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"id":"1","name":"photos","public":true,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "create".to_string(),
                "photos".to_string(),
                "--public".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap();
        assert_eq!(body["name"], "photos");
        assert_eq!(body["public"], true);
    }

    #[test]
    fn buckets_create_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("usage:") || err.to_string().contains("create"),
            "err: {err}"
        );
    }

    #[test]
    fn buckets_create_401_mapped() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "create".to_string(),
                "x".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 401);
    }

    // ── buckets delete ────────────────────────────────────────────────────────

    #[test]
    fn buckets_delete_sends_delete_request() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(r#"{"message":"bucket deleted"}"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "delete".to_string(),
                "old-bucket".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let path = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/bucket/old-bucket", "path: {path}");
    }

    #[test]
    fn buckets_delete_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(
            &g,
            &[
                "buckets".to_string(),
                "delete".to_string(),
                "--project=p1".to_string(),
            ]
        )
        .is_err());
    }

    // ── upload ────────────────────────────────────────────────────────────────

    #[test]
    fn upload_posts_file_bytes() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(r#"{"id":"obj1","path":"data/hello.txt","size":13}"#)
        });

        let dir = std::env::temp_dir()
            .join(format!("basin-storage-upload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("hello.txt");
        std::fs::write(&file, b"Hello, world!").unwrap();

        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "upload".to_string(),
                "my-bucket".to_string(),
                "data/hello.txt".to_string(),
                format!("--file={}", file.display()),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let path = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/object/my-bucket/data/hello.txt", "path: {path}");
    }

    #[test]
    fn upload_missing_bucket_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["upload".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn upload_missing_file_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_storage(
            &g,
            &[
                "upload".to_string(),
                "b".to_string(),
                "p".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--file"),
            "err: {err}"
        );
    }

    #[test]
    fn upload_401_mapped() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(401, r#"{"code":"unauthorized","message":"bad token"}"#)
        });
        let dir = std::env::temp_dir()
            .join(format!("basin-storage-up401-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("x.txt");
        std::fs::write(&file, b"x").unwrap();

        let g = flags(&srv.url);
        let err = cmd_storage(
            &g,
            &[
                "upload".to_string(),
                "b".to_string(),
                "p".to_string(),
                format!("--file={}", file.display()),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 401);
    }

    // ── download ──────────────────────────────────────────────────────────────

    #[test]
    fn download_gets_correct_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok("file-content-bytes")
        });
        let g = flags(&srv.url);
        let out_file = std::env::temp_dir()
            .join(format!("basin-dl-{}", std::process::id()))
            .join("out.txt");
        std::fs::create_dir_all(out_file.parent().unwrap()).unwrap();

        cmd_storage(
            &g,
            &[
                "download".to_string(),
                "my-bucket".to_string(),
                "docs/readme.txt".to_string(),
                format!("--out={}", out_file.display()),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let path = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/object/my-bucket/docs/readme.txt", "path: {path}");
    }

    #[test]
    fn download_404_mapped() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"object not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_storage(
            &g,
            &[
                "download".to_string(),
                "b".to_string(),
                "no/such/path".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── list ──────────────────────────────────────────────────────────────────

    #[test]
    fn list_posts_correct_path_and_body() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new((String::new(), serde_json::Value::Null)));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = (req.path.clone(), body);
            Resp::ok(r#"[{"path":"imgs/cat.png","size":1024,"mime_type":"image/png","updated_at":"2026-01-01T00:00:00Z"}]"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "list".to_string(),
                "photos".to_string(),
                "--prefix=imgs/".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let (path, body) = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/object/list/photos", "path: {path}");
        assert_eq!(body["prefix"], "imgs/");
    }

    #[test]
    fn list_missing_bucket_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["list".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn list_no_prefix_sends_empty_body() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok("[]")
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "list".to_string(),
                "b".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap().clone();
        // No prefix key when not supplied, or empty object.
        assert!(body.get("prefix").is_none() || body["prefix"].is_null());
    }

    // ── rm ────────────────────────────────────────────────────────────────────

    #[test]
    fn rm_sends_prefixes_body() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new((String::new(), serde_json::Value::Null)));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = (req.path.clone(), body);
            Resp::ok(r#"{"deleted":2}"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "rm".to_string(),
                "photos".to_string(),
                "old/a.png".to_string(),
                "old/b.png".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let (path, body) = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/object/photos", "path: {path}");
        let prefixes = body["prefixes"].as_array().unwrap();
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.iter().any(|v| v == "old/a.png"));
        assert!(prefixes.iter().any(|v| v == "old/b.png"));
    }

    #[test]
    fn rm_missing_bucket_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["rm".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn rm_no_paths_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_storage(
            &g,
            &[
                "rm".to_string(),
                "b".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("at least one path"),
            "err: {err}"
        );
    }

    // ── sign ──────────────────────────────────────────────────────────────────

    #[test]
    fn sign_posts_correct_path_and_body() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new((String::new(), serde_json::Value::Null)));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = (req.path.clone(), body);
            Resp::ok(r#"{"signedUrl":"https://api.basin.run/storage/v1/object/sign/proj/bucket/img.png?token=abc&expires=9999","expiresAt":"2026-01-01T01:00:00Z"}"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "sign".to_string(),
                "bucket".to_string(),
                "img.png".to_string(),
                "--expires-in=7200".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let (path, body) = cap.lock().unwrap().clone();
        assert_eq!(path, "/storage/v1/object/sign/bucket/img.png", "path: {path}");
        assert_eq!(body["expires_in"], 7200u64);
    }

    #[test]
    fn sign_missing_bucket_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["sign".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn sign_bad_expires_in_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_storage(
            &g,
            &[
                "sign".to_string(),
                "b".to_string(),
                "p".to_string(),
                "--expires-in=notanumber".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("integer") || err.to_string().contains("expires-in"),
            "err: {err}"
        );
    }

    #[test]
    fn sign_default_expires_in() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(r#"{"signedUrl":"https://example.com/signed"}"#)
        });
        let g = flags(&srv.url);
        cmd_storage(
            &g,
            &[
                "sign".to_string(),
                "b".to_string(),
                "p".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap().clone();
        assert_eq!(body["expires_in"], 3600u64, "default TTL should be 3600s");
    }

    // ── MIME helper ───────────────────────────────────────────────────────────

    #[test]
    fn guess_mime_known_extensions() {
        assert_eq!(guess_mime("photo.jpg"), "image/jpeg");
        assert_eq!(guess_mime("photo.PNG"), "image/png");
        assert_eq!(guess_mime("data.json"), "application/json");
        assert_eq!(guess_mime("doc.pdf"), "application/pdf");
        assert_eq!(guess_mime("archive.zip"), "application/zip");
        assert_eq!(guess_mime("page.html"), "text/html");
        assert_eq!(guess_mime("page.htm"), "text/html");
        assert_eq!(guess_mime("notes.txt"), "text/plain");
        assert_eq!(guess_mime("table.csv"), "text/csv");
    }

    #[test]
    fn guess_mime_unknown_falls_back() {
        assert_eq!(guess_mime("binary.bin"), "application/octet-stream");
        assert_eq!(guess_mime("noextension"), "application/octet-stream");
    }

    // ── help subcommands ──────────────────────────────────────────────────────

    #[test]
    fn buckets_help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_storage(&g, &["buckets".to_string(), "--help".to_string()]).is_ok());
        assert!(cmd_storage(&g, &["buckets".to_string(), "help".to_string()]).is_ok());
    }

    #[test]
    fn subcommand_help_flags_all_work() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        for sub in &["upload", "download", "list", "rm", "sign"] {
            let result = cmd_storage(&g, &[sub.to_string(), "--help".to_string()]);
            assert!(result.is_ok(), "{sub} --help failed: {result:?}");
        }
    }
}
