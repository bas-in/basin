//! `basinctl` — a small administrative CLI for a running Basin pgwire endpoint.
//!
//! Six subcommands: `ping`, `tenants`, `tables`, `query`, `version`,
//! `reset-auth`. Connection string is taken from `--url`, else `BASIN_URL`,
//! else (for `ping`) the positional argument. See
//! `services/basinctl/README.md` for examples.

use std::process::ExitCode;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use tokio_postgres::NoTls;

#[derive(Debug, Parser)]
#[command(name = "basinctl", version, about = "Basin admin CLI")]
struct Cli {
    /// Connection string. Falls back to BASIN_URL.
    #[arg(long, global = true)]
    url: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Cmd {
    /// Connect and run `SELECT 1`. Prints "OK in <ms>".
    Ping {
        /// Optional connection string positional override.
        url: Option<String>,
    },
    /// Print `current_user` from the connected session.
    Tenants,
    /// List tables (`SHOW TABLES`).
    Tables,
    /// Run an arbitrary SQL statement and print rows.
    Query {
        /// SQL to execute.
        sql: String,
    },
    /// Print version + git short SHA.
    Version,
    /// Drop every basin-auth catalog table (users, sessions, api keys, …).
    ///
    /// After today's loopback migration basin-auth's tables live inside the
    /// same engine that serves application traffic. Operators occasionally
    /// need to wipe just the auth namespace (test envs, schema-incompat
    /// upgrades, broken-state recovery) without touching customer data
    /// tables. This command connects as the reserved `basin_auth` user and
    /// runs `DROP TABLE IF EXISTS` for each `basin_auth_*` table.
    ///
    /// The next `basin-server` restart rebootstraps the schema empty via
    /// `basin_auth::schema::run_migrations`.
    #[command(name = "reset-auth")]
    ResetAuth {
        /// Required confirmation. Without it the command refuses to run.
        #[arg(long)]
        yes: bool,
        /// Override the engine connection string. Defaults to the loopback
        /// pgwire endpoint with the reserved `basin_auth` credentials.
        #[arg(
            long,
            default_value = "postgres://basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable"
        )]
        engine_url: String,
        /// Also drop `basin_auth_auth_tenant_credentials` (the per-tenant
        /// pgwire login table). Off by default — most resets keep the
        /// customer-facing wire credentials intact and only clear the
        /// user / session / api-key state.
        #[arg(long)]
        include_tenant_creds: bool,
    },
}

/// The basin-auth catalog tables, in safe drop order (children before
/// parents so the `REFERENCES basin_auth_users(user_id) ON DELETE CASCADE`
/// links don't trip if the engine ever decides to validate them on DROP).
///
/// These names are derived from `basin_auth::schema::run_migrations` with
/// `schema = "basin_auth"`. Keep this list in sync if that module's table
/// set changes; `auth_loopback_smoke.rs` is the cross-check.
const AUTH_TABLES_CORE: &[&str] = &[
    "basin_auth_auth_revoked_refresh_tokens",
    "basin_auth_auth_magic_links",
    "basin_auth_user_session_settings",
    "basin_auth_api_keys",
    "basin_auth_email_tokens",
    "basin_auth_refresh_tokens",
    "basin_auth_users",
];

/// Extra table dropped only when `--include-tenant-creds` is passed. Kept
/// separate because clearing this row set logs every pgwire-using tenant
/// out — most reset scenarios want to keep wire creds intact.
const AUTH_TABLE_TENANT_CREDS: &str = "basin_auth_auth_tenant_credentials";

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Version => {
            let ver = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
            let sha = option_env!("BASINCTL_GIT_SHA").unwrap_or("unknown");
            println!("basinctl {ver} ({sha})");
            Ok(())
        }
        Cmd::Ping { ref url } => {
            let conn = resolve_url(cli.url.as_deref(), url.as_deref())?;
            let client = connect(&conn).await?;
            let t0 = Instant::now();
            client.simple_query("SELECT 1").await.context("SELECT 1")?;
            println!("OK in {} ms", t0.elapsed().as_millis());
            Ok(())
        }
        Cmd::Tenants => {
            let client = connect(&resolve_url(cli.url.as_deref(), None)?).await?;
            let row = client
                .query_one("SELECT current_user", &[])
                .await
                .context("SELECT current_user")?;
            let user: &str = row.try_get(0).context("read current_user")?;
            println!("{user}");
            Ok(())
        }
        Cmd::Tables => {
            let client = connect(&resolve_url(cli.url.as_deref(), None)?).await?;
            // SHOW TABLES is Basin's per-tenant table listing; see CAPABILITIES.md.
            let rows = client
                .simple_query("SHOW TABLES")
                .await
                .context("SHOW TABLES")?;
            for msg in rows {
                if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                    if let Some(v) = r.get(0) {
                        println!("{v}");
                    }
                }
            }
            Ok(())
        }
        Cmd::Query { sql } => {
            let client = connect(&resolve_url(cli.url.as_deref(), None)?).await?;
            let rows = client
                .simple_query(&sql)
                .await
                .with_context(|| format!("query: {sql}"))?;
            print_simple(&rows);
            Ok(())
        }
        Cmd::ResetAuth {
            yes,
            engine_url,
            include_tenant_creds,
        } => reset_auth(yes, &engine_url, include_tenant_creds).await,
    }
}

/// Drop every basin-auth catalog table. See `Cmd::ResetAuth` for rationale.
async fn reset_auth(yes: bool, engine_url: &str, include_tenant_creds: bool) -> Result<()> {
    if !yes {
        return Err(anyhow!(
            "reset-auth refuses to run without --yes (this drops all basin-auth tables)"
        ));
    }

    // Compose the full drop list once so the preview and the execution agree.
    let mut targets: Vec<&str> = AUTH_TABLES_CORE.to_vec();
    if include_tenant_creds {
        // Insert just before `basin_auth_users` (the FK parent) so we still
        // drop child rows first.
        let insert_at = targets.len().saturating_sub(1);
        targets.insert(insert_at, AUTH_TABLE_TENANT_CREDS);
    }

    println!(
        "reset-auth: dropping {} basin-auth table(s):",
        targets.len()
    );
    for t in &targets {
        println!("  - {t}");
    }

    let client = connect(engine_url).await?;
    for t in &targets {
        // `DROP TABLE IF EXISTS` is idempotent — a partial / already-clean
        // state still lands at the desired empty schema.
        let stmt = format!("DROP TABLE IF EXISTS {t}");
        client
            .simple_query(&stmt)
            .await
            .with_context(|| format!("drop table {t}"))?;
    }

    println!(
        "reset-auth: ok — {} table(s) dropped. Restart basin-server to rebootstrap the schema empty.",
        targets.len()
    );
    Ok(())
}

fn resolve_url(flag: Option<&str>, positional: Option<&str>) -> Result<String> {
    if let Some(u) = flag {
        return Ok(u.to_string());
    }
    if let Some(u) = positional {
        return Ok(u.to_string());
    }
    std::env::var("BASIN_URL")
        .map_err(|_| anyhow!("no connection string: pass --url, BASIN_URL env, or positional arg"))
}

async fn connect(url: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .with_context(|| format!("connect to {url}"))?;
    // Drop the connection task in the background; on error it just closes.
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// Print rows from `simple_query` with `|` separators and column-padded
/// widths. Header row is the column names, separator row is dashes.
fn print_simple(msgs: &[tokio_postgres::SimpleQueryMessage]) {
    // Find the first row to discover columns; on no-rows queries print nothing.
    let mut headers: Vec<String> = Vec::new();
    let mut data: Vec<Vec<String>> = Vec::new();
    for m in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            if headers.is_empty() {
                headers = (0..r.columns().len())
                    .map(|i| r.columns()[i].name().to_string())
                    .collect();
            }
            let mut row = Vec::with_capacity(headers.len());
            for i in 0..headers.len() {
                row.push(r.get(i).unwrap_or("").to_string());
            }
            data.push(row);
        }
    }
    if headers.is_empty() {
        return;
    }
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let mx = data.iter().map(|r| r[i].len()).max().unwrap_or(0);
            mx.max(h.len())
        })
        .collect();
    let line = |cells: &[String]| {
        let parts: Vec<String> = cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
            .collect();
        parts.join(" | ")
    };
    println!("{}", line(&headers));
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));
    for r in &data {
        println!("{}", line(r));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("parse")
    }

    #[test]
    fn ping_with_positional() {
        let cli = parse(&["basinctl", "ping", "postgres://h/db"]);
        assert_eq!(
            cli.cmd,
            Cmd::Ping {
                url: Some("postgres://h/db".into())
            }
        );
    }

    #[test]
    fn ping_no_arg() {
        let cli = parse(&["basinctl", "ping"]);
        assert_eq!(cli.cmd, Cmd::Ping { url: None });
    }

    #[test]
    fn tenants_subcommand() {
        let cli = parse(&["basinctl", "tenants"]);
        assert_eq!(cli.cmd, Cmd::Tenants);
    }

    #[test]
    fn tables_subcommand() {
        let cli = parse(&["basinctl", "tables"]);
        assert_eq!(cli.cmd, Cmd::Tables);
    }

    #[test]
    fn query_takes_sql() {
        let cli = parse(&["basinctl", "query", "SELECT 1"]);
        assert_eq!(
            cli.cmd,
            Cmd::Query {
                sql: "SELECT 1".into()
            }
        );
    }

    #[test]
    fn version_subcommand() {
        let cli = parse(&["basinctl", "version"]);
        assert_eq!(cli.cmd, Cmd::Version);
    }

    #[test]
    fn url_flag_global() {
        let cli = parse(&["basinctl", "--url", "postgres://x/y", "tables"]);
        assert_eq!(cli.url.as_deref(), Some("postgres://x/y"));
        assert_eq!(cli.cmd, Cmd::Tables);
    }

    #[test]
    fn reset_auth_defaults() {
        let cli = parse(&["basinctl", "reset-auth", "--yes"]);
        match cli.cmd {
            Cmd::ResetAuth {
                yes,
                engine_url,
                include_tenant_creds,
            } => {
                assert!(yes);
                assert!(!include_tenant_creds);
                assert!(
                    engine_url.starts_with("postgres://basin_auth:basin_auth@127.0.0.1:5433/"),
                    "unexpected default engine_url: {engine_url}"
                );
            }
            other => panic!("expected ResetAuth, got {other:?}"),
        }
    }

    #[test]
    fn reset_auth_custom_engine_url() {
        let cli = parse(&[
            "basinctl",
            "reset-auth",
            "--yes",
            "--engine-url",
            "postgres://basin_auth:pw@10.0.0.1:5433/basin?sslmode=disable",
            "--include-tenant-creds",
        ]);
        match cli.cmd {
            Cmd::ResetAuth {
                yes,
                engine_url,
                include_tenant_creds,
            } => {
                assert!(yes);
                assert!(include_tenant_creds);
                assert_eq!(
                    engine_url,
                    "postgres://basin_auth:pw@10.0.0.1:5433/basin?sslmode=disable"
                );
            }
            other => panic!("expected ResetAuth, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reset_auth_refuses_without_yes() {
        // No connection attempt should be made — the `--yes` gate fires
        // first. Pass a deliberately-bogus URL to prove that.
        let err = reset_auth(false, "postgres://invalid:0/never", false)
            .await
            .expect_err("should refuse");
        let msg = format!("{err:#}");
        assert!(msg.contains("--yes"), "unexpected error: {msg}");
    }

    #[test]
    fn auth_tables_core_is_seven() {
        // Cross-check against `basin_auth::schema::run_migrations` —
        // 8 tables total, 7 in the default reset set, 1 gated behind
        // `--include-tenant-creds`.
        assert_eq!(AUTH_TABLES_CORE.len(), 7);
        assert!(AUTH_TABLES_CORE
            .iter()
            .all(|t| t.starts_with("basin_auth_")));
        assert!(AUTH_TABLE_TENANT_CREDS.starts_with("basin_auth_"));
    }
}
