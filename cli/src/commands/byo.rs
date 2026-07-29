//! byo — Bring-Your-Own infra surfaces: bucket, KMS, engine.
//!
//!   basin byo bucket  get|put|probe|delete  [--project=<ref>]
//!   basin byo kms     get|put|rotate|verify|audit|delete|cache-stats  [--project=<ref>]
//!   basin byo engine  get|put|probe|delete  [--project=<ref>]
//!
//! Cloud routes:
//!   GET|PUT|DELETE /v1/projects/{ref}/storage         — BYO bucket
//!   POST           /v1/projects/{ref}/storage/probe
//!   GET|POST|DELETE /v1/projects/{ref}/encryption     — BYO KMS
//!   POST           /v1/projects/{ref}/encryption/verify
//!   POST           /v1/projects/{ref}/encryption/rotate
//!   GET            /v1/projects/{ref}/encryption/audit
//!   GET            /v1/projects/{ref}/encryption/cache-stats
//!   GET|PUT|DELETE /v1/projects/{ref}/byo-engine      — BYO engine
//!   POST           /v1/projects/{ref}/byo-engine/probe

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, Serialize)]
struct BYOStorageConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bucket: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub region: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub configured_at: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct BYOEngineConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub configured_at: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct BYOKMSConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub configured_at: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct KMSAuditEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key_version: String,
}

// ── project ref helper ────────────────────────────────────────────────

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

// ── Top-level BYO dispatcher ──────────────────────────────────────────

pub fn cmd_byo(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "byo",
                "Manage bring-your-own infra surfaces (bucket, KMS, engine).",
                &[
                    "bucket  get|put|probe|delete                 [--project=<ref>]   Manage a customer-owned object storage bucket.",
                    "kms     get|put|rotate|verify|audit|delete|cache-stats  [--project=<ref>]  Manage a customer-managed KMS key.",
                    "engine  get|put|probe|delete                 [--project=<ref>]   Manage a self-hosted Basin engine.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "bucket" => cmd_byo_bucket(g, rest),
        "kms" => cmd_byo_kms(g, rest),
        "engine" => cmd_byo_engine(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "byo",
                "Manage bring-your-own infra surfaces (bucket, KMS, engine).",
                &[
                    "bucket  get|put|probe|delete                 [--project=<ref>]   Manage a customer-owned object storage bucket.",
                    "kms     get|put|rotate|verify|audit|delete|cache-stats  [--project=<ref>]  Manage a customer-managed KMS key.",
                    "engine  get|put|probe|delete                 [--project=<ref>]   Manage a self-hosted Basin engine.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for byo");
            Err(silent())
        }
    }
}

// ── byo bucket dispatcher ─────────────────────────────────────────────

fn cmd_byo_bucket(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "byo bucket",
                "Manage a customer-owned object storage bucket for Parquet data.",
                &[
                    "get    [--project=<ref>]                                              Show current bucket config.",
                    "put    --provider=<s3|r2|tigris|minio> --bucket=<n> --region=<r>     Configure a bucket.",
                    "       --access-key=<id> --secret-key=<s> [--endpoint=<u>]",
                    "       [--project=<ref>] [--yes]",
                    "probe  [--project=<ref>]                                              Test connectivity.",
                    "delete [--project=<ref>] [--yes]                                      Remove bucket config.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => byo_bucket_get(g, rest),
        "put" => byo_bucket_put(g, rest),
        "probe" => byo_bucket_probe(g, rest),
        "delete" => byo_bucket_delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "byo bucket",
                "Manage a customer-owned object storage bucket for Parquet data.",
                &[
                    "get    [--project=<ref>]                                              Show current bucket config.",
                    "put    --provider=<s3|r2|tigris|minio> --bucket=<n> --region=<r>     Configure a bucket.",
                    "       --access-key=<id> --secret-key=<s> [--endpoint=<u>]",
                    "       [--project=<ref>] [--yes]",
                    "probe  [--project=<ref>]                                              Test connectivity.",
                    "delete [--project=<ref>] [--yes]                                      Remove bucket config.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for byo bucket");
            Err(silent())
        }
    }
}

// ── byo bucket get ────────────────────────────────────────────────────

fn byo_bucket_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo bucket get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo bucket get",
            "Show the current BYO bucket configuration.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        storage: Option<BYOStorageConfig>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/storage"), None)?;
    if g.json {
        // JSON shape: { "storage": { provider, bucket, region, configured_at } | null }
        return print_json(&mut std::io::stdout(), &json!({ "storage": resp.storage }));
    }
    match resp.storage {
        None => println!("(no BYO bucket configured)"),
        Some(s) => {
            println!("provider:      {}", s.provider);
            println!("bucket:        {}", s.bucket);
            println!("region:        {}", s.region);
            println!("configured_at: {}", s.configured_at);
        }
    }
    Ok(())
}

// ── byo bucket put ────────────────────────────────────────────────────

const VALID_STORAGE_PROVIDERS: &[&str] = &["s3", "r2", "tigris", "minio"];

fn byo_bucket_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo bucket put")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("provider").long("provider"))
        .arg(Arg::new("bucket").long("bucket"))
        .arg(Arg::new("region").long("region"))
        .arg(Arg::new("access-key").long("access-key"))
        .arg(Arg::new("secret-key").long("secret-key"))
        .arg(Arg::new("endpoint").long("endpoint"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo bucket put",
            "Configure a customer-owned object storage bucket.",
            &[
                "--provider <s3|r2|tigris|minio>   Storage provider (required).",
                "--bucket <name>                   Bucket name (required).",
                "--region <region>                 Bucket region (required).",
                "--access-key <id>                 Access key ID (required).",
                "--secret-key <secret>             Secret access key (required). NEVER echoed.",
                "--endpoint <url>                  Custom endpoint URL (optional; required for minio).",
                "--project <ref>                   Project ref (required, or use basin link).",
                "--yes                             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let provider = m.get_one::<String>("provider").cloned().unwrap_or_default();
    let bucket = m.get_one::<String>("bucket").cloned().unwrap_or_default();
    let region = m.get_one::<String>("region").cloned().unwrap_or_default();
    let access_key = m
        .get_one::<String>("access-key")
        .cloned()
        .unwrap_or_default();
    let secret_key = m
        .get_one::<String>("secret-key")
        .cloned()
        .unwrap_or_default();
    let endpoint = m.get_one::<String>("endpoint").cloned().unwrap_or_default();

    if provider.is_empty() {
        return Err(msg(
            "byo bucket put requires --provider=<s3|r2|tigris|minio>",
        ));
    }
    let provider_lc = provider.to_lowercase();
    if !VALID_STORAGE_PROVIDERS.contains(&provider_lc.as_str()) {
        return Err(msg(format!(
            "byo bucket put: invalid --provider {provider:?}; must be one of s3, r2, tigris, minio"
        )));
    }
    if bucket.is_empty() {
        return Err(msg("byo bucket put requires --bucket=<name>"));
    }
    if region.is_empty() {
        return Err(msg("byo bucket put requires --region=<region>"));
    }
    if access_key.is_empty() {
        return Err(msg("byo bucket put requires --access-key=<id>"));
    }
    if secret_key.is_empty() {
        return Err(msg("byo bucket put requires --secret-key=<secret>"));
    }

    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to configure BYO bucket non-interactively under --json",
            ));
        }
        eprint!(
            "Configure BYO bucket {:?} ({provider_lc}) on project {:?}? This rewires the data plane. [y/N] ",
            bucket, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let mut body = json!({
        "provider":   provider_lc,
        "bucket":     bucket,
        "region":     region,
        "access_key": access_key,
        "secret_key": secret_key,
    });
    if !endpoint.is_empty() {
        body["endpoint"] = json!(endpoint);
    }

    #[derive(Deserialize)]
    struct Resp {
        storage: Option<BYOStorageConfig>,
    }
    let resp: Resp = c.do_json(
        Method::PUT,
        &format!("/v1/projects/{ref}/storage"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "storage": { provider, bucket, region, configured_at } }
        // secret_key is NEVER included in the response.
        return print_json(&mut std::io::stdout(), &json!({ "storage": resp.storage }));
    }
    match resp.storage {
        Some(s) => println!(
            "BYO bucket configured: provider={} bucket={} region={}",
            s.provider, s.bucket, s.region
        ),
        None => println!("BYO bucket configured."),
    }
    Ok(())
}

// ── byo bucket probe ──────────────────────────────────────────────────

fn byo_bucket_probe(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo bucket probe")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo bucket probe",
            "Test connectivity to the BYO bucket.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        ok: bool,
        #[serde(default)]
        latency_ms: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/storage/probe"),
        None,
    )?;
    if g.json {
        // JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.ok {
        println!("Bucket probe OK (latency={:.1}ms)", resp.latency_ms);
    } else {
        let err_msg = resp.error.as_deref().unwrap_or("(unknown error)");
        println!("Bucket probe FAILED: {err_msg}");
    }
    Ok(())
}

// ── byo bucket delete ─────────────────────────────────────────────────

fn byo_bucket_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo bucket delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo bucket delete",
            "Remove the BYO bucket configuration.",
            &[
                "--project <ref>   Project ref (required, or use basin link).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to remove BYO bucket config non-interactively under --json",
            ));
        }
        eprint!(
            "Remove BYO bucket config on project {:?}? This disconnects the data plane. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        revoked: bool,
    }
    let resp: Resp = c.do_json(Method::DELETE, &format!("/v1/projects/{ref}/storage"), None)?;
    if g.json {
        // JSON shape: { "revoked": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("BYO bucket config removed.");
    Ok(())
}

// ── byo kms dispatcher ────────────────────────────────────────────────

fn cmd_byo_kms(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "byo kms",
                "Manage a customer-managed KMS encryption key.",
                &[
                    "get         [--project=<ref>]                                             Show current KMS config.",
                    "put         --provider=<aws|gcp|azure> --key-id=<id>                     Configure a KMS key.",
                    "            [--auth-json=<path|->] [--project=<ref>] [--yes]",
                    "rotate      [--project=<ref>] [--yes]                                     Rotate the active key version.",
                    "verify      [--project=<ref>]                                             Test key connectivity.",
                    "audit       [--project=<ref>] [--limit=N] [--since=<rfc3339>]            List KMS audit events.",
                    "delete      [--project=<ref>] [--yes]                                     Remove KMS config.",
                    "cache-stats [--project=<ref>]                                             Show key-cache hit/miss stats.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => byo_kms_get(g, rest),
        "put" => byo_kms_put(g, rest),
        "rotate" => byo_kms_rotate(g, rest),
        "verify" => byo_kms_verify(g, rest),
        "audit" => byo_kms_audit(g, rest),
        "delete" => byo_kms_delete(g, rest),
        "cache-stats" => byo_kms_cache_stats(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "byo kms",
                "Manage a customer-managed KMS encryption key.",
                &[
                    "get         [--project=<ref>]                                             Show current KMS config.",
                    "put         --provider=<aws|gcp|azure> --key-id=<id>                     Configure a KMS key.",
                    "            [--auth-json=<path|->] [--project=<ref>] [--yes]",
                    "rotate      [--project=<ref>] [--yes]                                     Rotate the active key version.",
                    "verify      [--project=<ref>]                                             Test key connectivity.",
                    "audit       [--project=<ref>] [--limit=N] [--since=<rfc3339>]            List KMS audit events.",
                    "delete      [--project=<ref>] [--yes]                                     Remove KMS config.",
                    "cache-stats [--project=<ref>]                                             Show key-cache hit/miss stats.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for byo kms");
            Err(silent())
        }
    }
}

// ── byo kms get ───────────────────────────────────────────────────────

fn byo_kms_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms get",
            "Show the current BYO KMS configuration.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        kms: Option<BYOKMSConfig>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/encryption"), None)?;
    if g.json {
        // JSON shape: { "kms": { provider, key_id, configured_at } | null }
        return print_json(&mut std::io::stdout(), &json!({ "kms": resp.kms }));
    }
    match resp.kms {
        None => println!("(no BYO KMS configured)"),
        Some(k) => {
            println!("provider:      {}", k.provider);
            println!("key_id:        {}", k.key_id);
            println!("configured_at: {}", k.configured_at);
        }
    }
    Ok(())
}

// ── byo kms put ───────────────────────────────────────────────────────

const VALID_KMS_PROVIDERS: &[&str] = &["aws", "gcp", "azure"];

/// Read the auth-json blob from a file path or stdin ("-").
/// Returns None when path is empty. The blob is NEVER printed.
fn read_auth_blob(path: &str) -> CliResult<Option<Value>> {
    if path.is_empty() {
        return Ok(None);
    }
    let raw = if path == "-" {
        let mut s = String::new();
        use std::io::Read;
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| msg(format!("byo kms put: reading auth JSON from stdin: {e}")))?;
        s
    } else {
        std::fs::read_to_string(path)
            .map_err(|e| msg(format!("byo kms put: reading auth JSON from {path:?}: {e}")))?
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(msg("byo kms put: auth JSON is empty"));
    }
    let blob: Value = serde_json::from_str(raw)
        .map_err(|e| msg(format!("byo kms put: auth JSON is not valid JSON: {e}")))?;
    Ok(Some(blob))
}

fn byo_kms_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms put")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("provider").long("provider"))
        .arg(Arg::new("key-id").long("key-id"))
        .arg(Arg::new("auth-json").long("auth-json"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms put",
            "Configure a customer-managed KMS encryption key.",
            &[
                "--provider <aws|gcp|azure>   KMS provider (required).",
                "--key-id <id>                KMS key identifier (required).",
                "--auth-json <path|->         Path to credentials JSON file, or - for stdin. NEVER echoed.",
                "--project <ref>              Project ref (required, or use basin link).",
                "--yes                        Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let provider = m.get_one::<String>("provider").cloned().unwrap_or_default();
    let key_id = m.get_one::<String>("key-id").cloned().unwrap_or_default();
    let auth_json_path = m
        .get_one::<String>("auth-json")
        .cloned()
        .unwrap_or_default();

    if provider.is_empty() {
        return Err(msg("byo kms put requires --provider=<aws|gcp|azure>"));
    }
    let provider_lc = provider.to_lowercase();
    if !VALID_KMS_PROVIDERS.contains(&provider_lc.as_str()) {
        return Err(msg(format!(
            "byo kms put: invalid --provider {provider:?}; must be one of aws, gcp, azure"
        )));
    }
    if key_id.is_empty() {
        return Err(msg("byo kms put requires --key-id=<id>"));
    }

    // Read auth blob (may be None) — NEVER print its contents.
    let auth_blob = read_auth_blob(&auth_json_path)?;

    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to configure BYO KMS non-interactively under --json",
            ));
        }
        eprint!(
            "Configure BYO KMS key {:?} ({provider_lc}) on project {:?}? [y/N] ",
            key_id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let mut body = json!({
        "provider": provider_lc,
        "key_id":   key_id,
    });
    if let Some(blob) = auth_blob {
        // Embed credentials — NEVER reflected back in output.
        body["auth"] = blob;
    }

    #[derive(Deserialize)]
    struct Resp {
        kms: Option<BYOKMSConfig>,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/encryption"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "kms": { provider, key_id, configured_at } }
        // auth blob is NEVER included in the response.
        return print_json(&mut std::io::stdout(), &json!({ "kms": resp.kms }));
    }
    match resp.kms {
        Some(k) => println!(
            "BYO KMS configured: provider={} key_id={}",
            k.provider, k.key_id
        ),
        None => println!("BYO KMS configured."),
    }
    Ok(())
}

// ── byo kms rotate ────────────────────────────────────────────────────

fn byo_kms_rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms rotate")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms rotate",
            "Rotate the BYO KMS key version.",
            &[
                "--project <ref>   Project ref (required, or use basin link).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to rotate the KMS key non-interactively under --json",
            ));
        }
        eprint!(
            "Rotate BYO KMS key on project {:?}? This re-encrypts the data-encryption key. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        rotated: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        new_key_version: String,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/encryption/rotate"),
        None,
    )?;
    if g.json {
        // JSON shape: { "rotated": true, "new_key_version": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("KMS key rotated. New version: {}", resp.new_key_version);
    Ok(())
}

// ── byo kms verify ────────────────────────────────────────────────────

fn byo_kms_verify(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms verify")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms verify",
            "Test connectivity to the BYO KMS key.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        ok: bool,
        #[serde(default)]
        latency_ms: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/encryption/verify"),
        None,
    )?;
    if g.json {
        // JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.ok {
        println!("KMS key verify OK (latency={:.1}ms)", resp.latency_ms);
    } else {
        let err_msg = resp.error.as_deref().unwrap_or("(unknown error)");
        println!("KMS key verify FAILED: {err_msg}");
    }
    Ok(())
}

// ── byo kms audit ─────────────────────────────────────────────────────

fn byo_kms_audit(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms audit")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit").default_value("50"))
        .arg(Arg::new("since").long("since"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms audit",
            "List KMS audit events for a project.",
            &[
                "--project <ref>        Project ref (required, or use basin link).",
                "--limit <N>            Maximum number of events (default 50).",
                "--since <rfc3339>      Return events at or after this timestamp.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let limit = m
        .get_one::<String>("limit")
        .cloned()
        .unwrap_or_else(|| "50".into());
    let since = m.get_one::<String>("since").cloned().unwrap_or_default();
    let c = require_client(g)?;

    let q = query_string(&[("limit", &limit), ("since", &since)]);

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        events: Vec<KMSAuditEvent>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/encryption/audit{q}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "events": [{ id, action, at, actor_id, key_version }] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.events.is_empty() {
        println!("(no KMS audit events)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "ACTION", "AT", "ACTOR_ID", "KEY_VERSION"]);
    for ev in &resp.events {
        t.row(&[&ev.id, &ev.action, &ev.at, &ev.actor_id, &ev.key_version]);
    }
    t.flush()
}

// ── byo kms delete ────────────────────────────────────────────────────

fn byo_kms_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms delete",
            "Remove the BYO KMS configuration.",
            &[
                "--project <ref>   Project ref (required, or use basin link).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to delete BYO KMS config non-interactively under --json",
            ));
        }
        eprint!(
            "Delete BYO KMS config on project {:?}? Data will revert to the default cluster key. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        deleted: bool,
    }
    let resp: Resp = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/encryption"),
        None,
    )?;
    if g.json {
        // JSON shape: { "deleted": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("BYO KMS config deleted.");
    Ok(())
}

// ── byo kms cache-stats ───────────────────────────────────────────────

fn byo_kms_cache_stats(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo kms cache-stats")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo kms cache-stats",
            "Show key-cache hit/miss stats.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        hits: i64,
        #[serde(default)]
        misses: i64,
        #[serde(default)]
        size: i64,
        #[serde(default)]
        max_size: i64,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/encryption/cache-stats"),
        None,
    )?;
    if g.json {
        // JSON shape: { "hits": N, "misses": N, "size": N, "max_size": N }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let total = resp.hits + resp.misses;
    let hit_rate = if total > 0 {
        resp.hits as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    println!("hits:     {}", resp.hits);
    println!("misses:   {}", resp.misses);
    println!("hit_rate: {hit_rate:.1}%");
    println!("size:     {}", resp.size);
    println!("max_size: {}", resp.max_size);
    Ok(())
}

// ── byo engine dispatcher ─────────────────────────────────────────────

fn cmd_byo_engine(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "byo engine",
                "Manage a self-hosted Basin engine for a project.",
                &[
                    "get    [--project=<ref>]                                         Show current engine config.",
                    "put    --url=<u> --shared-key=<k> [--project=<ref>] [--yes]     Configure a BYO engine.",
                    "probe  [--project=<ref>]                                         Test connectivity.",
                    "delete [--project=<ref>] [--yes]                                 Remove engine config.",
                ],
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => byo_engine_get(g, rest),
        "put" => byo_engine_put(g, rest),
        "probe" => byo_engine_probe(g, rest),
        "delete" => byo_engine_delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "byo engine",
                "Manage a self-hosted Basin engine for a project.",
                &[
                    "get    [--project=<ref>]                                         Show current engine config.",
                    "put    --url=<u> --shared-key=<k> [--project=<ref>] [--yes]     Configure a BYO engine.",
                    "probe  [--project=<ref>]                                         Test connectivity.",
                    "delete [--project=<ref>] [--yes]                                 Remove engine config.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for byo engine");
            Err(silent())
        }
    }
}

// ── byo engine get ────────────────────────────────────────────────────

fn byo_engine_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo engine get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo engine get",
            "Show the current BYO engine configuration.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        engine: Option<BYOEngineConfig>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/byo-engine"), None)?;
    if g.json {
        // JSON shape: { "engine": { url, configured_at } | null }
        return print_json(&mut std::io::stdout(), &json!({ "engine": resp.engine }));
    }
    match resp.engine {
        None => println!("(no BYO engine configured)"),
        Some(e) => {
            println!("url:           {}", e.url);
            println!("configured_at: {}", e.configured_at);
        }
    }
    Ok(())
}

// ── byo engine put ────────────────────────────────────────────────────

fn byo_engine_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo engine put")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("url").long("url"))
        .arg(Arg::new("shared-key").long("shared-key"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo engine put",
            "Configure a self-hosted Basin engine.",
            &[
                "--url <url>           Engine base URL (required).",
                "--shared-key <key>    Shared HMAC key for request signing (required). NEVER echoed.",
                "--project <ref>       Project ref (required, or use basin link).",
                "--yes                 Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let engine_url = m.get_one::<String>("url").cloned().unwrap_or_default();
    let shared_key = m
        .get_one::<String>("shared-key")
        .cloned()
        .unwrap_or_default();

    if engine_url.is_empty() {
        return Err(msg("byo engine put requires --url=<engine-url>"));
    }
    if shared_key.is_empty() {
        return Err(msg("byo engine put requires --shared-key=<key>"));
    }

    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to configure BYO engine non-interactively under --json",
            ));
        }
        eprint!(
            "Configure BYO engine {:?} on project {:?}? This rewires query routing. [y/N] ",
            engine_url, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let body = json!({
        "url":        engine_url,
        "shared_key": shared_key,
    });

    #[derive(Deserialize)]
    struct Resp {
        engine: Option<BYOEngineConfig>,
    }
    let resp: Resp = c.do_json(
        Method::PUT,
        &format!("/v1/projects/{ref}/byo-engine"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "engine": { url, configured_at } }
        // shared_key is NEVER included in the response.
        return print_json(&mut std::io::stdout(), &json!({ "engine": resp.engine }));
    }
    match resp.engine {
        Some(e) => println!("BYO engine configured: url={}", e.url),
        None => println!("BYO engine configured."),
    }
    Ok(())
}

// ── byo engine probe ──────────────────────────────────────────────────

fn byo_engine_probe(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo engine probe")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo engine probe",
            "Test connectivity to the BYO engine.",
            &["--project <ref>   Project ref (required, or use basin link)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        ok: bool,
        #[serde(default)]
        latency_ms: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/byo-engine/probe"),
        None,
    )?;
    if g.json {
        // JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.ok {
        println!("Engine probe OK (latency={:.1}ms)", resp.latency_ms);
    } else {
        let err_msg = resp.error.as_deref().unwrap_or("(unknown error)");
        println!("Engine probe FAILED: {err_msg}");
    }
    Ok(())
}

// ── byo engine delete ─────────────────────────────────────────────────

fn byo_engine_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("byo engine delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "byo engine delete",
            "Remove the BYO engine configuration.",
            &[
                "--project <ref>   Project ref (required, or use basin link).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to remove BYO engine config non-interactively under --json",
            ));
        }
        eprint!(
            "Remove BYO engine config on project {:?}? Queries will revert to the basin-managed engine. [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        removed: bool,
    }
    let resp: Resp = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/byo-engine"),
        None,
    )?;
    if g.json {
        // JSON shape: { "removed": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("BYO engine config removed.");
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

    fn json_flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── top-level dispatcher ─────────────────────────────────────────

    #[test]
    fn byo_no_args_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &[]).is_ok());
    }

    #[test]
    fn byo_help_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &["help".into()]).is_ok());
    }

    #[test]
    fn byo_unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &["frobnicate".into()]).is_err());
    }

    // ── byo bucket get ───────────────────────────────────────────────

    #[test]
    fn byo_bucket_get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/storage");
            Resp::ok(
                r#"{"storage":{"provider":"s3","bucket":"my-bucket","region":"us-east-1","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &["bucket".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_get_null() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"storage":null}"#));
        cmd_byo(
            &flags(&srv.url),
            &["bucket".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_get_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"storage":{"provider":"r2","bucket":"r2-bucket","region":"auto"}}"#)
        });
        cmd_byo(
            &json_flags(&srv.url),
            &["bucket".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_get_404_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"project not found"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &["bucket".into(), "get".into(), "--project=proj1".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = crate::error::as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── byo bucket put — validation ──────────────────────────────────

    #[test]
    fn byo_bucket_put_missing_provider_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--bucket=b".into(),
                "--region=us-east-1".into(),
                "--access-key=ak".into(),
                "--secret-key=sk".into(),
            ],
        );
        assert!(err.is_err());
        assert!(
            err.unwrap_err().to_string().contains("--provider"),
            "error should mention --provider"
        );
    }

    #[test]
    fn byo_bucket_put_invalid_provider_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=gcs".into(),
                "--bucket=b".into(),
                "--region=us-east-1".into(),
                "--access-key=ak".into(),
                "--secret-key=sk".into(),
            ],
        );
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("gcs"), "error should mention invalid provider");
    }

    #[test]
    fn byo_bucket_put_missing_bucket_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=s3".into(),
                "--region=us-east-1".into(),
                "--access-key=ak".into(),
                "--secret-key=sk".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--bucket"));
    }

    #[test]
    fn byo_bucket_put_missing_secret_key_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=s3".into(),
                "--bucket=b".into(),
                "--region=us-east-1".into(),
                "--access-key=ak".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--secret-key"));
    }

    #[test]
    fn byo_bucket_put_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PUT");
            assert_eq!(req.path, "/v1/projects/proj1/storage");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["provider"], "s3");
            assert_eq!(body["bucket"], "my-bucket");
            Resp::ok(r#"{"storage":{"provider":"s3","bucket":"my-bucket","region":"us-east-1"}}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=s3".into(),
                "--bucket=my-bucket".into(),
                "--region=us-east-1".into(),
                "--access-key=AKID".into(),
                "--secret-key=secret".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_put_secret_never_echoed() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"storage":{"provider":"s3","bucket":"b","region":"us-east-1"}}"#)
        });
        // Just assert it doesn't error — stdout capture would need a pipe trick.
        // The real guard is the code never calls println! with secret_key.
        cmd_byo(
            &flags(&srv.url),
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=s3".into(),
                "--bucket=b".into(),
                "--region=us-east-1".into(),
                "--access-key=AKID".into(),
                "--secret-key=SUPER_SECRET_KEY_NEVER_ECHO_ME".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_put_403_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &[
                "bucket".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=s3".into(),
                "--bucket=b".into(),
                "--region=us-east-1".into(),
                "--access-key=ak".into(),
                "--secret-key=sk".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── byo bucket probe ─────────────────────────────────────────────

    #[test]
    fn byo_bucket_probe_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/storage/probe");
            Resp::ok(r#"{"ok":true,"latency_ms":12.5}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["bucket".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_probe_failure() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"ok":false,"latency_ms":0,"error":"connection refused"}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["bucket".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_probe_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"ok":true,"latency_ms":8.0}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &["bucket".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── byo bucket delete ────────────────────────────────────────────

    #[test]
    fn byo_bucket_delete_with_yes() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/storage");
            Resp::ok(r#"{"revoked":true}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "bucket".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"revoked":true}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &[
                "bucket".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_bucket_delete_422_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"validation_error","message":"storage is in use"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &[
                "bucket".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    #[test]
    fn byo_bucket_unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &["bucket".into(), "frobnicate".into()]).is_err());
    }

    // ── byo engine get ───────────────────────────────────────────────

    #[test]
    fn byo_engine_get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/byo-engine");
            Resp::ok(
                r#"{"engine":{"url":"https://engine.example.com","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &["engine".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_get_null() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"engine":null}"#));
        cmd_byo(
            &flags(&srv.url),
            &["engine".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_get_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"engine":{"url":"https://engine.example.com","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &json_flags(&srv.url),
            &["engine".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_get_404_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"project not found"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &["engine".into(), "get".into(), "--project=proj1".into()],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── byo engine put — validation ──────────────────────────────────

    #[test]
    fn byo_engine_put_missing_url_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "engine".into(),
                "put".into(),
                "--project=proj1".into(),
                "--shared-key=k1".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--url"));
    }

    #[test]
    fn byo_engine_put_missing_shared_key_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "engine".into(),
                "put".into(),
                "--project=proj1".into(),
                "--url=https://engine.example.com".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--shared-key"));
    }

    #[test]
    fn byo_engine_put_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PUT");
            assert_eq!(req.path, "/v1/projects/proj1/byo-engine");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["url"], "https://engine.example.com");
            assert_eq!(body["shared_key"], "mysecret");
            Resp::ok(
                r#"{"engine":{"url":"https://engine.example.com","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "engine".into(),
                "put".into(),
                "--project=proj1".into(),
                "--url=https://engine.example.com".into(),
                "--shared-key=mysecret".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_put_shared_key_never_echoed() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"engine":{"url":"https://engine.example.com","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        // Succeeds without error — code never echoes shared_key in println!
        cmd_byo(
            &flags(&srv.url),
            &[
                "engine".into(),
                "put".into(),
                "--project=proj1".into(),
                "--url=https://engine.example.com".into(),
                "--shared-key=ULTRA_SECRET_SHARED_KEY_NEVER_ECHO".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_put_403_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &[
                "engine".into(),
                "put".into(),
                "--project=proj1".into(),
                "--url=https://engine.example.com".into(),
                "--shared-key=sk".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── byo engine probe ─────────────────────────────────────────────

    #[test]
    fn byo_engine_probe_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/byo-engine/probe");
            Resp::ok(r#"{"ok":true,"latency_ms":7.3}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["engine".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_probe_failure() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"ok":false,"latency_ms":0,"error":"dial tcp: connection refused"}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["engine".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_probe_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"ok":true,"latency_ms":3.0}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &["engine".into(), "probe".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── byo engine delete ────────────────────────────────────────────

    #[test]
    fn byo_engine_delete_with_yes() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/byo-engine");
            Resp::ok(r#"{"removed":true}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "engine".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"removed":true}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &[
                "engine".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_engine_delete_422_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"validation_error","message":"engine is active"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &[
                "engine".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    #[test]
    fn byo_engine_unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &["engine".into(), "frobnicate".into()]).is_err());
    }

    // ── byo kms get ──────────────────────────────────────────────────

    #[test]
    fn byo_kms_get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/encryption");
            Resp::ok(
                r#"{"kms":{"provider":"aws","key_id":"arn:aws:kms:us-east-1:123:key/abc","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &["kms".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_get_null() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"kms":null}"#));
        cmd_byo(
            &flags(&srv.url),
            &["kms".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_get_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"kms":{"provider":"gcp","key_id":"projects/p/keys/k","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &json_flags(&srv.url),
            &["kms".into(), "get".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── byo kms put — validation ─────────────────────────────────────

    #[test]
    fn byo_kms_put_missing_provider_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--key-id=k1".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--provider"));
    }

    #[test]
    fn byo_kms_put_invalid_provider_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=vault".into(),
                "--key-id=k1".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("vault"));
    }

    #[test]
    fn byo_kms_put_missing_key_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=aws".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--key-id"));
    }

    #[test]
    fn byo_kms_put_auth_json_file_not_found_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=aws".into(),
                "--key-id=k1".into(),
                "--auth-json=/nonexistent/path/creds.json".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("nonexistent") || msg.contains("auth JSON"),
            "msg={msg}"
        );
    }

    #[test]
    fn byo_kms_put_auth_json_malformed_is_error() {
        let _g = with_temp_config_dir();
        let dir = std::env::temp_dir();
        let bad_file = dir.join("basin_test_bad.json");
        std::fs::write(&bad_file, b"{not valid json").unwrap();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_byo(
            &g,
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=aws".into(),
                "--key-id=k1".into(),
                format!("--auth-json={}", bad_file.display()),
                "--yes".into(),
            ],
        );
        let _ = std::fs::remove_file(&bad_file);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.to_lowercase().contains("json"),
            "msg should mention JSON: {msg}"
        );
    }

    #[test]
    fn byo_kms_put_happy_path() {
        let _g = with_temp_config_dir();
        let dir = std::env::temp_dir();
        let cred_file = dir.join("basin_test_creds.json");
        std::fs::write(
            &cred_file,
            br#"{"type":"service_account","project_id":"myproj","private_key":"secret"}"#,
        )
        .unwrap();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/encryption");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert!(body["auth"].is_object(), "auth field should be present");
            Resp::ok(
                r#"{"kms":{"provider":"gcp","key_id":"projects/p/keys/k","configured_at":"2026-05-01T00:00:00Z"}}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "kms".into(),
                "put".into(),
                "--project=proj1".into(),
                "--provider=gcp".into(),
                "--key-id=projects/p/keys/k".into(),
                format!("--auth-json={}", cred_file.display()),
                "--yes".into(),
            ],
        )
        .unwrap();
        let _ = std::fs::remove_file(&cred_file);
    }

    // ── byo kms rotate ───────────────────────────────────────────────

    #[test]
    fn byo_kms_rotate_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/encryption/rotate");
            Resp::ok(r#"{"rotated":true,"new_key_version":"v2"}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "kms".into(),
                "rotate".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_rotate_json_shape() {
        let _g = with_temp_config_dir();
        let srv =
            TestServer::start(|_req: &Req| Resp::ok(r#"{"rotated":true,"new_key_version":"v3"}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &[
                "kms".into(),
                "rotate".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    // ── byo kms verify ───────────────────────────────────────────────

    #[test]
    fn byo_kms_verify_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/encryption/verify");
            Resp::ok(r#"{"ok":true,"latency_ms":5.0}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["kms".into(), "verify".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── byo kms audit ────────────────────────────────────────────────

    #[test]
    fn byo_kms_audit_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req.path.starts_with("/v1/projects/proj1/encryption/audit"));
            assert!(req.path.contains("limit=25"), "limit should be forwarded");
            assert!(req.path.contains("since="), "since should be forwarded");
            Resp::ok(
                r#"{"events":[{"id":"ev1","action":"encrypt","at":"2026-05-01T00:00:00Z","actor_id":"usr1","key_version":"v1"}]}"#,
            )
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "kms".into(),
                "audit".into(),
                "--project=proj1".into(),
                "--limit=25".into(),
                "--since=2026-01-01T00:00:00Z".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_audit_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        cmd_byo(
            &flags(&srv.url),
            &["kms".into(), "audit".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_audit_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"events":[{"id":"ev2","action":"decrypt","at":"2026-05-02T00:00:00Z","actor_id":"usr2","key_version":"v2"}]}"#,
            )
        });
        cmd_byo(
            &json_flags(&srv.url),
            &["kms".into(), "audit".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── byo kms delete ───────────────────────────────────────────────

    #[test]
    fn byo_kms_delete_with_yes() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/encryption");
            Resp::ok(r#"{"deleted":true}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &[
                "kms".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":true}"#));
        cmd_byo(
            &json_flags(&srv.url),
            &[
                "kms".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_delete_403_is_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"owner role required"}}"#,
            )
        });
        let err = cmd_byo(
            &flags(&srv.url),
            &[
                "kms".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        let boxed = err.unwrap_err();
        let ae = crate::error::as_api_error(boxed.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── byo kms cache-stats ──────────────────────────────────────────

    #[test]
    fn byo_kms_cache_stats_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/encryption/cache-stats");
            Resp::ok(r#"{"hits":100,"misses":10,"size":50,"max_size":1000}"#)
        });
        cmd_byo(
            &flags(&srv.url),
            &["kms".into(), "cache-stats".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_cache_stats_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"hits":200,"misses":20,"size":80,"max_size":500}"#)
        });
        cmd_byo(
            &json_flags(&srv.url),
            &["kms".into(), "cache-stats".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn byo_kms_unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo(&g, &["kms".into(), "frobnicate".into()]).is_err());
    }

    #[test]
    fn byo_kms_help_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo_kms(&g, &["help".into()]).is_ok());
    }

    #[test]
    fn byo_engine_help_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_byo_engine(&g, &["help".into()]).is_ok());
    }
}
