//! `basinctl` — a small administrative CLI for a running Basin pgwire endpoint.
//!
//! Five subcommands: `ping`, `tenants`, `tables`, `query`, `version`. Connection
//! string is taken from `--url`, else `BASIN_URL`, else (for `ping`) the
//! positional argument. See `services/basinctl/README.md` for examples.

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
}

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
    }
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
}
