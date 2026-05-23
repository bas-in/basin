//! tx — `basin tx --project=<ref>` — interactive transaction REPL.
//!
//! Opens an interactive prompt where the user types SQL one statement at
//! a time. Every statement goes through the SAME pgwire connection so
//! transaction state (BEGIN / SAVEPOINT / COMMIT / ROLLBACK) persists
//! across prompts. The connection DSN is fetched once at startup from
//!   POST /v1/projects/{ref}/pgwire/reveal
//! and then a single `tokio_postgres::Client` is held for the lifetime
//! of the session.
//!
//! Surface (intentionally minimal):
//!   basin (proj) tx > BEGIN;
//!   basin (proj) tx > INSERT INTO foo VALUES (1);
//!   basin (proj) tx > SAVEPOINT s1;
//!   basin (proj) tx > ROLLBACK TO s1;
//!   basin (proj) tx > COMMIT;
//!   basin (proj) tx > \q       -- also \quit, \exit
//!   basin (proj) tx > \h       -- in-REPL help
//!
//! Multi-line statements are accumulated until a trailing ';' (or '\g')
//! is seen on a logical line, mirroring psql's terminator behaviour.
//! Empty lines are no-ops. SQL execution errors print to stderr and the
//! REPL keeps running so the user can issue ROLLBACK by hand.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;

use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};

use super::help::help_for_command;
use super::parse_or_silent;
use super::pgwire::{build_dsn, PgwireRevealResponse};

// ── Public entry ─────────────────────────────────────────────────────

pub fn cmd_tx(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tx")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tx",
            "Open an interactive transaction REPL against a project's pgwire endpoint.",
            &[
                "--project=<ref>   Project ref (required — no working-dir fallback here).",
                "",
                "Inside the REPL:",
                "  ;            terminate a statement (multi-line accumulates until ';')",
                "  \\q | \\quit | \\exit   leave the REPL",
                "  \\h           show in-REPL help",
            ],
        );
        return Ok(());
    }
    let project = m
        .get_one::<String>("project")
        .cloned()
        .ok_or_else(|| msg("--project is required"))?;
    if project.is_empty() {
        return Err(msg("--project is required"));
    }

    // Fetch the DSN once at startup. We deliberately don't fall back to
    // load_working_project here — `tx` is an explicit op, the caller
    // should name the project.
    let c = require_client(g)?;
    let resp: PgwireRevealResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{}/pgwire/reveal", project),
        None,
    )?;
    let dsn = build_dsn(&resp);

    run_interactive(&project, &dsn)
}

// ── Live readline-driven REPL ────────────────────────────────────────

fn run_interactive(project: &str, dsn: &str) -> CliResult<()> {
    // Single-thread tokio runtime is plenty for one REPL connection.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| msg(format!("tokio runtime: {e}")))?;

    rt.block_on(async move {
        let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| msg(format!("connect pgwire: {e}")))?;
        // Spawn the connection driver — required by tokio_postgres.
        // We don't care about its result; if the conn dies, subsequent
        // queries will surface the error and the REPL prints + continues.
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let mut rl: rustyline::DefaultEditor =
            rustyline::DefaultEditor::new().map_err(|e| msg(format!("readline init: {e}")))?;
        let prompt = format!("basin ({}) tx > ", project);
        let cont_prompt = "...> ";

        loop {
            match rl.readline(&prompt) {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if let Some(action) = meta_command(trimmed) {
                        match action {
                            MetaAction::Quit => break,
                            MetaAction::Help => {
                                print_repl_help();
                                continue;
                            }
                        }
                    }
                    // Accumulate until ';' (or '\g') terminator.
                    let sql = if is_terminated(trimmed) {
                        trimmed.to_string()
                    } else {
                        let mut buf = line.clone();
                        loop {
                            match rl.readline(cont_prompt) {
                                Ok(more) => {
                                    buf.push('\n');
                                    buf.push_str(&more);
                                    if is_terminated(buf.trim_end()) {
                                        break;
                                    }
                                }
                                // ^D in continuation: bail out of this stmt.
                                Err(rustyline::error::ReadlineError::Eof) => break,
                                Err(rustyline::error::ReadlineError::Interrupted) => {
                                    buf.clear();
                                    break;
                                }
                                Err(e) => {
                                    return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                                        msg(format!("readline: {e}")),
                                    );
                                }
                            }
                        }
                        buf
                    };

                    let sql_trimmed = sql.trim();
                    if sql_trimmed.is_empty() {
                        continue;
                    }
                    // `add_history_entry` returns Result<bool, ReadlineError>
                    // (bool = whether it was actually added — rustyline
                    // dedupes consecutive duplicates). We don't care
                    // about either; history is best-effort.
                    let _ = rl.add_history_entry(sql_trimmed);

                    match client.simple_query(sql_trimmed).await {
                        Ok(msgs) => print_simple_query_result(&msgs),
                        Err(e) => eprintln!("error: {e}"),
                    }
                }
                Err(rustyline::error::ReadlineError::Eof) => break,
                // ^C: discard current line, redraw prompt — same as psql.
                Err(rustyline::error::ReadlineError::Interrupted) => continue,
                Err(e) => {
                    return Err::<(), Box<dyn std::error::Error + Send + Sync>>(msg(format!(
                        "readline: {e}"
                    )));
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
}

// ── Pure helpers (testable in isolation) ─────────────────────────────

/// Meta-command (backslash-prefixed) action.
#[derive(Debug, PartialEq, Eq)]
pub enum MetaAction {
    Quit,
    Help,
}

/// `meta_command` returns the action for a backslash-prefixed REPL
/// command, or `None` if the input is regular SQL. The input is assumed
/// to already be `trim`'d. Unknown meta commands return `None` so they
/// fall through to the SQL path and surface a helpful pgwire error.
pub fn meta_command(line: &str) -> Option<MetaAction> {
    match line {
        "\\q" | "\\quit" | "\\exit" => Some(MetaAction::Quit),
        "\\h" => Some(MetaAction::Help),
        _ => None,
    }
}

/// `is_terminated` reports whether a logical input line is a complete
/// statement (ends with `;` or `\g`, ignoring trailing whitespace).
pub fn is_terminated(s: &str) -> bool {
    let t = s.trim_end();
    t.ends_with(';') || t.ends_with("\\g")
}

/// `accumulate_sql` joins an iterator of input lines into a complete SQL
/// statement, stopping at the first line whose trimmed form is
/// terminated by `;` or `\g`. Empty trimmed lines are skipped at the
/// start but preserved inside a multi-line stmt. Returns `None` if the
/// iterator is exhausted before any terminator is seen.
///
/// This is the unit-testable core of the multi-line accumulator; the
/// live REPL calls it implicitly through `rl.readline` loops.
pub fn accumulate_sql<I, S>(mut lines: I) -> Option<String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let mut buf = String::new();
    // Skip leading empties.
    let first = loop {
        let l = lines.next()?;
        if !l.as_ref().trim().is_empty() {
            break l;
        }
    };
    buf.push_str(first.as_ref());
    if is_terminated(buf.trim_end()) {
        return Some(buf.trim().to_string());
    }
    for l in lines {
        buf.push('\n');
        buf.push_str(l.as_ref());
        if is_terminated(buf.trim_end()) {
            return Some(buf.trim().to_string());
        }
    }
    None
}

/// `print_repl_help` writes the inline `\h` help block to stdout.
fn print_repl_help() {
    println!("Commands:");
    println!("  \\q | \\quit | \\exit   Leave the REPL.");
    println!("  \\h                   This help.");
    println!();
    println!("Any other input is treated as SQL. Multi-line statements");
    println!("are accumulated until a trailing ';' (or '\\g').");
    println!("Transaction state persists across statements until you");
    println!("COMMIT or ROLLBACK (or exit, which rolls back implicitly).");
}

/// `print_simple_query_result` writes a compact summary of a
/// `simple_query` response — a single CommandComplete tag per statement
/// is printed for DML/DDL, while SELECT results print a row count plus
/// a fixed-width table of columns × rows.
fn print_simple_query_result(msgs: &[tokio_postgres::SimpleQueryMessage]) {
    use tokio_postgres::SimpleQueryMessage::*;
    let mut current_cols: Option<Vec<String>> = None;
    let mut current_rows: Vec<Vec<String>> = Vec::new();
    for m in msgs {
        match m {
            Row(r) => {
                let mut cells: Vec<String> = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    cells.push(r.get(i).unwrap_or("").to_string());
                }
                current_rows.push(cells);
            }
            RowDescription(cols) => {
                current_cols = Some(cols.iter().map(|c| c.name().to_string()).collect());
                current_rows.clear();
            }
            CommandComplete(n) => {
                if let Some(cols) = current_cols.take() {
                    print_table(&cols, &current_rows);
                    current_rows.clear();
                }
                // psql-style tag: most operations send their own tag
                // (INSERT 0 N, UPDATE N, DELETE N, BEGIN, COMMIT, ...).
                // tokio_postgres only exposes the row count; we print
                // the bare number so the user sees affected-row info
                // without us guessing the verb.
                println!("OK ({} rows)", n);
            }
            _ => {}
        }
    }
}

/// `print_table` writes a fixed-width column table to stdout for SELECT
/// results. Tiny on purpose — this is a REPL convenience, not a pager.
fn print_table(cols: &[String], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = cols.iter().map(|c| c.len()).collect();
    for r in rows {
        for (i, cell) in r.iter().enumerate() {
            if i < widths.len() && cell.len() > widths[i] {
                widths[i] = cell.len();
            }
        }
    }
    let header: Vec<String> = cols
        .iter()
        .zip(widths.iter())
        .map(|(c, w)| format!("{:<width$}", c, width = w))
        .collect();
    println!("{}", header.join(" | "));
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));
    for r in rows {
        let cells: Vec<String> = r
            .iter()
            .zip(widths.iter())
            .map(|(c, w)| format!("{:<width$}", c, width = w))
            .collect();
        println!("{}", cells.join(" | "));
    }
    println!("({} rows)", rows.len());
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── arg parsing ──────────────────────────────────────────────────

    #[test]
    fn project_flag_is_required() {
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_tx(&g, &[]).unwrap_err();
        assert!(
            err.to_string().contains("--project is required")
                || crate::error::is_silent(err.as_ref()),
            "got: {err}"
        );
    }

    #[test]
    fn empty_project_value_errors() {
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_tx(&g, &["--project=".into()]).unwrap_err();
        assert!(
            err.to_string().contains("--project is required"),
            "got: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_tx(&g, &["--help".into()]).is_ok());
    }

    // ── meta_command ─────────────────────────────────────────────────

    #[test]
    fn meta_command_quit_variants() {
        assert_eq!(meta_command("\\q"), Some(MetaAction::Quit));
        assert_eq!(meta_command("\\quit"), Some(MetaAction::Quit));
        assert_eq!(meta_command("\\exit"), Some(MetaAction::Quit));
    }

    #[test]
    fn meta_command_help() {
        assert_eq!(meta_command("\\h"), Some(MetaAction::Help));
    }

    #[test]
    fn meta_command_unknown_falls_through_to_sql() {
        // `\d` and friends are deliberately NOT implemented — they
        // should fall through and let pgwire / server reject them.
        assert_eq!(meta_command("\\d"), None);
        assert_eq!(meta_command("SELECT 1;"), None);
        assert_eq!(meta_command(""), None);
    }

    // ── is_terminated ────────────────────────────────────────────────

    #[test]
    fn is_terminated_semicolon_and_backslash_g() {
        assert!(is_terminated("SELECT 1;"));
        assert!(is_terminated("SELECT 1;   "));
        assert!(is_terminated("SELECT 1\\g"));
        assert!(!is_terminated("SELECT 1"));
        assert!(!is_terminated(""));
        assert!(!is_terminated("BEGIN"));
    }

    // ── accumulate_sql ───────────────────────────────────────────────

    #[test]
    fn accumulate_sql_single_line_terminator() {
        let lines = vec!["BEGIN;"];
        assert_eq!(
            accumulate_sql(lines.into_iter()),
            Some("BEGIN;".to_string())
        );
    }

    #[test]
    fn accumulate_sql_joins_multiline_until_semicolon() {
        let lines = vec!["INSERT INTO foo", "  VALUES (1,", "  2);"];
        let out = accumulate_sql(lines.into_iter()).unwrap();
        // Joined with newlines, trimmed.
        assert!(out.starts_with("INSERT INTO foo"));
        assert!(out.ends_with(");"));
        assert!(out.contains("VALUES (1,"));
    }

    #[test]
    fn accumulate_sql_skips_leading_blanks() {
        let lines = vec!["", "   ", "COMMIT;"];
        assert_eq!(
            accumulate_sql(lines.into_iter()),
            Some("COMMIT;".to_string())
        );
    }

    #[test]
    fn accumulate_sql_no_terminator_returns_none() {
        let lines = vec!["SELECT 1", "FROM t"];
        assert_eq!(accumulate_sql(lines.into_iter()), None);
    }

    #[test]
    fn accumulate_sql_empty_iterator_returns_none() {
        let lines: Vec<&str> = vec![];
        assert_eq!(accumulate_sql(lines.into_iter()), None);
    }

    #[test]
    fn accumulate_sql_backslash_g_terminator() {
        let lines = vec!["SELECT 1\\g"];
        let out = accumulate_sql(lines.into_iter()).unwrap();
        assert!(out.ends_with("\\g"));
    }

    // ── empty-line discipline ────────────────────────────────────────

    /// Empty / whitespace-only input lines must be no-ops at the REPL
    /// level. This is partially exercised by `accumulate_sql_skips_
    /// leading_blanks` above, but we keep an explicit unit case so the
    /// rule is documented and regressed against.
    #[test]
    fn empty_input_lines_are_noops_in_accumulator() {
        assert_eq!(accumulate_sql(std::iter::empty::<&str>()), None);
        assert_eq!(accumulate_sql(vec![""].into_iter()), None);
        assert_eq!(accumulate_sql(vec!["", "", ""].into_iter()), None);
    }
}
