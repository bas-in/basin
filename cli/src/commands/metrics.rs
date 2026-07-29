//! metrics — project and org-level metrics surface.
//!
//!   basin metrics [--project=<ref>] [--org=<slug>] [--range=24h|7d|30d] [--metric=<name>]
//!
//! Exactly one of --project (or a working project) and --org must resolve.
//! Mutually exclusive: if both resolve, the command errors.
//!
//! Project-level: GET /v1/projects/{ref}/metrics?range=...&metric=...
//! Org-level:     GET /v1/orgs/{slug}/metrics?range=...&metric=...
//!
//! JSON shape (project and org paths share the same envelope):
//!   { "metrics": [{ "name": string, "value": number, "unit": string, "timestamp": string }] }

use clap::{Arg, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// Metric is one data point returned by the /metrics endpoints.
/// JSON shape: { name, value, unit, timestamp }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Metric {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default)]
    pub value: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub unit: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub timestamp: String,
}

#[derive(Deserialize, Serialize)]
struct MetricsResp {
    #[serde(default)]
    metrics: Vec<Metric>,
}

// ── cmd_metrics ───────────────────────────────────────────────────────────────

pub fn cmd_metrics(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    // Handle help before clap parse so we match the Go pattern of checking
    // args[0] directly.
    if let Some(first) = args.first() {
        if first == "--help" || first == "-h" || first == "help" {
            help_for_command(
                "metrics",
                "Fetch project or org-level metrics.",
                &[
                    "[--project=<ref>] [--org=<slug>] [--range=24h|7d|30d] [--metric=<name>]",
                    "Exactly one of --project (or working project) and --org must be set.",
                ],
            );
            return Ok(());
        }
    }

    let cmd = Command::new("metrics")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("range").long("range"))
        .arg(Arg::new("metric").long("metric"));
    let m = parse_or_silent(cmd, args)?;

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let org_flag = m.get_one::<String>("org").cloned().unwrap_or_default();

    // Resolve org: explicit --org flag, then fall back to g.org_slug.
    let org = if !org_flag.is_empty() {
        org_flag
    } else {
        g.org_slug.clone()
    };

    // Resolve project ref: --project flag, then working project (only when no org).
    let ref_val = if !project_flag.is_empty() {
        project_flag
    } else if org.is_empty() {
        // No --org: try working project.
        let cwd =
            std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
        match load_working_project(&cwd)? {
            Some(w) if !w.project_ref.is_empty() => w.project_ref,
            _ => String::new(),
        }
    } else {
        String::new()
    };

    // Mutual exclusivity check.
    if !ref_val.is_empty() && !org.is_empty() {
        return Err(msg(
            "--project and --org are mutually exclusive: use one or the other",
        ));
    }
    if ref_val.is_empty() && org.is_empty() {
        return Err(msg(
            "one of --project or --org is required (or run `basin link` to bind this directory)",
        ));
    }

    let range_val = m.get_one::<String>("range").cloned().unwrap_or_default();
    let metric_val = m.get_one::<String>("metric").cloned().unwrap_or_default();

    let c = require_client(g)?;
    let qs = query_string(&[("metric", &metric_val), ("range", &range_val)]);

    let path = if !ref_val.is_empty() {
        format!("/v1/projects/{ref_val}/metrics{qs}")
    } else {
        format!("/v1/orgs/{org}/metrics{qs}")
    };

    let resp: MetricsResp = c.do_json(Method::GET, &path, None).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 403 {
                return msg(
                    "access denied: check that your token has read access to this resource",
                );
            }
        }
        e
    })?;

    if g.json {
        // JSON shape: { "metrics": [{ "name": string, "value": number, "unit": string, "timestamp": string }] }
        return print_json(&mut std::io::stdout(), &json!({ "metrics": resp.metrics }));
    }
    if resp.metrics.is_empty() {
        println!("(no metrics)");
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "VALUE", "UNIT", "TIMESTAMP"]);
    for m in &resp.metrics {
        t.row(&[&m.name, &format!("{}", m.value), &m.unit, &m.timestamp]);
    }
    t.flush()
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

    fn sample_metrics_json() -> &'static str {
        r#"[{"name":"queries_per_sec","value":42.5,"unit":"qps","timestamp":"2026-01-01T00:00:00Z"},{"name":"cache_hit_ratio","value":0.97,"unit":"ratio","timestamp":"2026-01-01T00:00:00Z"}]"#
    }

    // ── project metrics — happy path ────────────────────────────────────────

    #[test]
    fn project_metrics_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(
                req.path.contains("/v1/projects/proj1/metrics"),
                "path={}",
                req.path
            );
            Resp::ok(format!(r#"{{"metrics":{}}}"#, sample_metrics_json()))
        });
        let g = flags(&srv.url);
        cmd_metrics(&g, &["--project=proj1".to_string()]).unwrap();
    }

    // ── query-string passthrough ────────────────────────────────────────────

    #[test]
    fn project_metrics_query_string_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("range=7d"), "path={}", req.path);
            assert!(
                req.path.contains("metric=cache_hit_ratio"),
                "path={}",
                req.path
            );
            Resp::ok(r#"{"metrics":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_metrics(
            &g,
            &[
                "--project=proj1".to_string(),
                "--range=7d".to_string(),
                "--metric=cache_hit_ratio".to_string(),
            ],
        )
        .unwrap();
    }

    // ── JSON shape ──────────────────────────────────────────────────────────

    #[test]
    fn project_metrics_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"metrics":{}}}"#, sample_metrics_json()))
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: MetricsResp = c
            .do_json(reqwest::Method::GET, "/v1/projects/proj1/metrics", None)
            .unwrap();
        print_json(&mut buf, &json!({ "metrics": resp.metrics })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let metrics = v["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0]["name"].as_str().unwrap(), "queries_per_sec");
    }

    // ── org metrics — happy path ────────────────────────────────────────────

    #[test]
    fn org_metrics_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(
                req.path.contains("/v1/orgs/acme/metrics"),
                "path={}",
                req.path
            );
            Resp::ok(format!(r#"{{"metrics":{}}}"#, sample_metrics_json()))
        });
        let g = flags(&srv.url);
        cmd_metrics(&g, &["--org=acme".to_string()]).unwrap();
    }

    // ── metrics empty result ────────────────────────────────────────────────

    #[test]
    fn metrics_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"metrics":[]}"#));
        let g = flags(&srv.url);
        cmd_metrics(&g, &["--project=proj1".to_string()]).unwrap();
    }

    // ── validation ──────────────────────────────────────────────────────────

    #[test]
    fn metrics_missing_project_and_org_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_metrics(&g, &[]).unwrap_err();
        assert!(err.to_string().contains("required"), "err={err}");
    }

    #[test]
    fn metrics_both_project_and_org_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_metrics(
            &g,
            &["--project=proj1".to_string(), "--org=acme".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "err={err}");
    }

    // ── error mapping ───────────────────────────────────────────────────────

    #[test]
    fn metrics_403_gives_access_denied() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"code":"forbidden","message":"access denied"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_metrics(&g, &["--project=proj1".to_string()]).unwrap_err();
        assert!(err.to_string().contains("access denied"), "err={err}");
    }

    #[test]
    fn metrics_404_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_metrics(&g, &["--project=proj1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── help ────────────────────────────────────────────────────────────────

    #[test]
    fn metrics_help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_metrics(&g, &["help".to_string()]).is_ok());
    }
}
