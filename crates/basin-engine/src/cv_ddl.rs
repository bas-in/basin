// Several pg_query-based AST matchers + helpers here are staged for ADR 0014
// Phase 2 when the routing flips off the textual pre-screen. Suppress the
// dead-code warnings until that wires up.
#![allow(dead_code)]

//! `CREATE/REFRESH/DROP MATERIALIZED VIEW` SQL surface.
//!
//! Three statement shapes route through this module:
//!
//! 1. `CREATE MATERIALIZED VIEW <name> WITH (basin.continuous,
//!    refresh_interval = '<duration>') AS <select_query>` — the continuous
//!    aggregate definition. sqlparser accepts a standard `CREATE MATERIALIZED
//!    VIEW ... AS ...`, but its `WITH (...)` parser rejects two forms we
//!    care about:
//!
//!    * a bare-identifier flag (`basin.continuous`) — the dot makes it a
//!      compound identifier, not a key=value pair.
//!    * any key=value pair whose key contains a dot.
//!
//!    We pre-screen the raw SQL with [`extract_basin_cv_options`] to lift
//!    the Basin-specific options out, then hand the remainder to sqlparser
//!    as a vanilla `CREATE MATERIALIZED VIEW`.
//!
//! 2. `REFRESH MATERIALIZED VIEW <name>` — sqlparser 0.52 has no `REFRESH`
//!    statement at all. [`match_refresh_materialized_view`] handles the
//!    full statement textually.
//!
//! 3. `DROP MATERIALIZED VIEW <name>` — sqlparser 0.52's `DROP` parser
//!    accepts only `TABLE`, `VIEW`, `INDEX`, `ROLE`, `SCHEMA`, `DATABASE`,
//!    `FUNCTION`, `PROCEDURE`, `STAGE`, `TRIGGER`, `SECRET`, `SEQUENCE`,
//!    `TYPE`. [`match_drop_materialized_view`] handles the full statement
//!    textually.
//!
//! ## Why the engine does the bootstrap directly
//!
//! `basin-cv` already ships the Rust API (`CvStore::register_cv` /
//! `CvRefresher::tick`) and is the production driver. `basin-cv` depends
//! on `basin-engine`, so we cannot import it here without creating a
//! cycle. The implementation in this module mirrors `basin-cv`'s logic:
//! it goes through the same `Engine::open_session` execution path and the
//! same `Catalog::set_continuous_aggregate` /
//! `Catalog::replace_data_files` mutators. A CV created via the SQL
//! surface here is byte-identical (catalog and storage state) to one
//! created via `CvStore::register_cv`, so the production
//! `basin-cv::CvRefresher` will pick it up on its next tick exactly as if
//! the Rust API had created it.

use std::sync::Arc;

use arrow_array::RecordBatch;
use basin_catalog::{Catalog, CvDef, DataFileRef, SnapshotId};
use basin_common::{BasinError, PartitionKey, Result, TableName};
use chrono::Utc;

use crate::session::refresh_table;
use crate::{ExecResult, ProjectSession};

/// Continuous-aggregate options lifted out of a `CREATE MATERIALIZED VIEW
/// ... WITH (...)` clause.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CvOptions {
    /// `basin.continuous` flag must be present and true. The flag's
    /// existence in the option list is what flags the view as a continuous
    /// aggregate (vs. a regular materialised view).
    pub continuous: bool,
    /// `refresh_interval = '<duration>'`. Required when `continuous = true`
    /// for the `basin.continuous` form; *optional* for the
    /// `timescaledb.continuous` form (Timescale sets the cadence separately
    /// via `add_continuous_aggregate_policy`). Parsed at the point of
    /// registration; stored as seconds in the catalog (one `BIGINT`).
    pub refresh_interval_secs: Option<u64>,
    /// True when the flag was the TimescaleDB form `timescaledb.continuous`
    /// rather than Basin's native `basin.continuous`. The materialisation
    /// engine is identical; this only tags the resulting `CvDef` so the
    /// Timescale info-schema view and policy functions can recognise it.
    pub timescaledb: bool,
}

/// Pre-screen result for a `CREATE MATERIALIZED VIEW ... WITH (...)`. The
/// returned `String` is the SQL with the entire `WITH (...)` clause
/// stripped — sqlparser sees a vanilla `CREATE MATERIALIZED VIEW name AS
/// query`. `Some(_)` means we recognised and lifted Basin options;
/// `None` means the statement does not start with `CREATE MATERIALIZED
/// VIEW` (caller falls through to the standard sqlparser path
/// untouched).
pub(crate) fn extract_basin_cv_options(sql: &str) -> Result<(String, Option<CvOptions>)> {
    let trimmed = sql.trim_start();
    if !starts_with_kw(trimmed, "CREATE") {
        return Ok((sql.to_string(), None));
    }
    // Skip CREATE.
    let after_create = skip_word(trimmed).trim_start();
    // Optional `OR REPLACE` — we don't support it, but accepting the token
    // here keeps the recogniser tight to "CREATE MATERIALIZED VIEW" only.
    let after_or_replace = if starts_with_kw(after_create, "OR") {
        let after_or = skip_word(after_create).trim_start();
        if starts_with_kw(after_or, "REPLACE") {
            skip_word(after_or).trim_start()
        } else {
            return Ok((sql.to_string(), None));
        }
    } else {
        after_create
    };
    if !starts_with_kw(after_or_replace, "MATERIALIZED") {
        return Ok((sql.to_string(), None));
    }
    let after_mat = skip_word(after_or_replace).trim_start();
    if !starts_with_kw(after_mat, "VIEW") {
        return Ok((sql.to_string(), None));
    }
    // From here we know the statement is CREATE [OR REPLACE] MATERIALIZED
    // VIEW. Find the WITH (...) clause if present, lift its body out, and
    // return the cleaned SQL plus the parsed Basin options.
    let leading_len = sql.len() - trimmed.len();
    let after_view_pos = leading_len + (trimmed.len() - skip_word(after_mat).len());
    let bytes = sql.as_bytes();
    let mut i = after_view_pos;
    let mut found: Option<(usize, usize, String)> = None; // (with_kw_start, after_close_paren, inside)
    while i < bytes.len() {
        // Skip strings, quoted idents, comments, and parens.
        if let Some(next) = skip_skippable(bytes, i) {
            i = next;
            continue;
        }
        if at_word_boundary(bytes, i)
            && bytes_match_kw(&bytes[i..], b"WITH")
            && at_word_boundary_end(bytes, i + 4)
        {
            // Skip whitespace, expect `(`.
            let mut j = i + 4;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                // Find matching `)`. Track depth so nested parens (e.g.
                // function calls inside option values) don't confuse us.
                let open = j;
                let mut depth = 1i32;
                let mut k = j + 1;
                while k < bytes.len() && depth > 0 {
                    match bytes[k] {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        b'\'' => {
                            // Skip string content so a `)` inside a literal
                            // doesn't close us early.
                            k += 1;
                            while k < bytes.len() {
                                if bytes[k] == b'\'' {
                                    if k + 1 < bytes.len() && bytes[k + 1] == b'\'' {
                                        k += 2;
                                        continue;
                                    }
                                    break;
                                }
                                k += 1;
                            }
                        }
                        _ => {}
                    }
                    k += 1;
                }
                if depth == 0 && k < bytes.len() {
                    let inside = sql[open + 1..k].to_string();
                    found = Some((i, k + 1, inside));
                    // Don't break — we want the *first* WITH after VIEW;
                    // a second WITH is invalid in this position anyway.
                    break;
                } else {
                    return Err(BasinError::InvalidSchema(
                        "CREATE MATERIALIZED VIEW: unterminated WITH (...) clause".into(),
                    ));
                }
            }
            // `WITH` not followed by `(` — could be CTE on the inner
            // SELECT. Stop scanning; CTEs always come after `AS`, so a
            // `WITH` we see here without `(` means we've left the view's
            // own option clause and entered the query body.
            break;
        }
        // Stop scanning once we hit `AS` followed by whitespace/keyword
        // boundary — the option clause is always before `AS <query>`.
        if at_word_boundary(bytes, i)
            && bytes_match_kw(&bytes[i..], b"AS")
            && at_word_boundary_end(bytes, i + 2)
        {
            break;
        }
        i += 1;
    }

    let Some((start, end, inside)) = found else {
        return Ok((
            sql.to_string(),
            Some(CvOptions {
                continuous: false,
                refresh_interval_secs: None,
                timescaledb: false,
            }),
        ));
    };

    let opts = parse_cv_options(&inside)?;
    let mut stripped = String::with_capacity(sql.len());
    stripped.push_str(&sql[..start]);
    // Trim a trailing whitespace before the WITH so we don't get double
    // spaces; sqlparser tolerates either, but cleaner output helps debugging.
    let stripped = stripped
        .trim_end_matches(|c: char| c.is_whitespace())
        .to_string();
    let mut stripped = stripped;
    stripped.push(' ');
    stripped.push_str(sql[end..].trim_start());
    Ok((stripped, Some(opts)))
}

/// Parse the body of a `WITH (...)` clause for a continuous aggregate.
/// Recognised tokens (case-insensitive):
///
/// - `basin.continuous` — the flag.
/// - `refresh_interval = '<duration>'` — duration value.
///
/// Anything else is rejected as `InvalidSchema` so a typo doesn't silently
/// turn into a regular materialised view.
fn parse_cv_options(inside: &str) -> Result<CvOptions> {
    let mut continuous = false;
    let mut timescaledb = false;
    let mut refresh_interval_secs: Option<u64> = None;
    for raw in split_top_level_commas(inside) {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        // TimescaleDB form: `timescaledb.continuous` (= true is also
        // tolerated, e.g. `timescaledb.continuous = true`). This is the
        // canonical Timescale syntax for a continuous aggregate. The
        // materialisation engine is identical to `basin.continuous`; the only
        // difference is the cadence comes from `add_continuous_aggregate_policy`
        // rather than an inline `refresh_interval`.
        {
            let key = token.split('=').next().unwrap_or(token).trim();
            if key.eq_ignore_ascii_case("timescaledb.continuous") {
                continuous = true;
                timescaledb = true;
                continue;
            }
        }
        // Flag form: `basin.continuous` (or just `continuous`).
        if token.eq_ignore_ascii_case("basin.continuous")
            || token.eq_ignore_ascii_case("continuous")
        {
            continuous = true;
            continue;
        }
        // key = value form. The key may contain a dot (e.g.
        // `basin.refresh_interval`); we accept both `basin.<k>` and bare
        // `<k>` for the same set of recognised keys.
        let Some(eq) = token.find('=') else {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE MATERIALIZED VIEW: unrecognised option {token:?}"
            )));
        };
        let key = token[..eq].trim();
        let value = token[eq + 1..].trim();
        let key_norm = key
            .strip_prefix("basin.")
            .or_else(|| key.strip_prefix("BASIN."))
            .unwrap_or(key);
        match key_norm.to_ascii_lowercase().as_str() {
            "refresh_interval" => {
                let lit = unquote_string_literal(value).ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "refresh_interval must be a string literal, got {value:?}"
                    ))
                })?;
                refresh_interval_secs = Some(parse_duration_secs(&lit)?);
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "CREATE MATERIALIZED VIEW: unsupported option {other:?}"
                )));
            }
        }
    }
    Ok(CvOptions {
        continuous,
        refresh_interval_secs,
    })
}

/// Strip surrounding `'...'` from a SQL string literal. Returns `None` for
/// anything else. We don't need the full SQL escape grammar here — only
/// `''` doubled-quote escapes are allowed in PG string literals, and we
/// translate them to a single quote.
fn unquote_string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    if s.len() < 2 || !s.starts_with('\'') || !s.ends_with('\'') {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    Some(inner.replace("''", "'"))
}

/// Parse a duration like `'5m'`, `'1 hour'`, `'30s'`, `'2 hours 30 minutes'`
/// into seconds. Accepts a small set of PG / humantime suffixes; rejects
/// anything else with `InvalidSchema`. Whitespace between the number and
/// the suffix is optional and multiple components compose additively.
fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(BasinError::InvalidSchema(
            "refresh_interval: empty duration".into(),
        ));
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut total: u64 = 0;
    let mut saw_component = false;
    while i < bytes.len() {
        // Skip whitespace between components.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        // Read digits.
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == num_start {
            return Err(BasinError::InvalidSchema(format!(
                "refresh_interval: expected digit at {:?}",
                &s[num_start..]
            )));
        }
        let num: u64 = s[num_start..i].parse().map_err(|_| {
            BasinError::InvalidSchema(format!(
                "refresh_interval: bad number {:?}",
                &s[num_start..i]
            ))
        })?;
        // Optional whitespace between number and unit.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        // Read unit (alphabetic).
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }
        let unit = &s[unit_start..i];
        let factor: u64 = match unit.to_ascii_lowercase().as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => 1,
            "m" | "min" | "mins" | "minute" | "minutes" => 60,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3600,
            "d" | "day" | "days" => 86_400,
            "" => {
                return Err(BasinError::InvalidSchema(format!(
                    "refresh_interval: missing unit after {num}"
                )));
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "refresh_interval: unknown unit {other:?}"
                )));
            }
        };
        total =
            total
                .checked_add(num.checked_mul(factor).ok_or_else(|| {
                    BasinError::InvalidSchema("refresh_interval: overflow".into())
                })?)
                .ok_or_else(|| BasinError::InvalidSchema("refresh_interval: overflow".into()))?;
        saw_component = true;
    }
    if !saw_component {
        return Err(BasinError::InvalidSchema(format!(
            "refresh_interval: empty duration {s:?}"
        )));
    }
    Ok(total)
}

/// Split `text` by top-level commas, respecting parentheses and string
/// literals. Used to chunk a `WITH (...)` body into individual options.
fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b',' if depth == 0 => {
                out.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start <= bytes.len() {
        out.push(&text[start..]);
    }
    out
}

/// Recognise `REFRESH MATERIALIZED VIEW <name> [WITH (full = true)]`
/// (with optional trailing `;`). Returns `(name, force_full)` on a match,
/// or `None` when the statement is something else. sqlparser 0.52 has no
/// `REFRESH` AST node, so we handle the full statement textually.
///
/// `WITH (full = true)` opts out of the v0.1 incremental-refresh path and
/// re-executes the entire SELECT body. Mirrors PG's
/// `REFRESH MATERIALIZED VIEW CONCURRENTLY` opt-out shape (different keyword,
/// same role: tell the system "yes, I want a full rebuild").
pub(crate) fn match_refresh_materialized_view(sql: &str) -> Result<Option<(String, bool)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "REFRESH") {
        return Ok(None);
    }
    let after_refresh = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_refresh, "MATERIALIZED") {
        return Err(BasinError::InvalidSchema(
            "REFRESH: only MATERIALIZED VIEW is supported".into(),
        ));
    }
    let after_mat = skip_word(after_refresh).trim_start();
    if !starts_with_kw(after_mat, "VIEW") {
        return Err(BasinError::InvalidSchema(
            "REFRESH MATERIALIZED: expected VIEW".into(),
        ));
    }
    let after_view = skip_word(after_mat).trim_start();
    let (name, rest) = read_simple_identifier(after_view)?;
    let rest = rest.trim();
    // Optional `WITH (full = true)` — the v0.1 opt-out for "skip
    // incremental, do a full rebuild".
    let force_full = if starts_with_kw(rest, "WITH") {
        let after_with = skip_word(rest).trim_start();
        if !after_with.starts_with('(') {
            return Err(BasinError::InvalidSchema(
                "REFRESH MATERIALIZED VIEW: expected `(` after WITH".into(),
            ));
        }
        // Find the matching `)`. We don't expect nested parens here; the
        // option grammar is intentionally tiny.
        let close = after_with.find(')').ok_or_else(|| {
            BasinError::InvalidSchema("REFRESH MATERIALIZED VIEW: unterminated WITH (...)".into())
        })?;
        let inside = after_with[1..close].trim();
        let trail = after_with[close + 1..].trim();
        if !trail.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "REFRESH MATERIALIZED VIEW: unexpected trailing input {trail:?}"
            )));
        }
        parse_refresh_options(inside)?
    } else {
        if !rest.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "REFRESH MATERIALIZED VIEW: unexpected trailing input {rest:?}"
            )));
        }
        false
    };
    Ok(Some((name, force_full)))
}

/// Parse the body of a `WITH (...)` clause attached to a `REFRESH
/// MATERIALIZED VIEW`. The only recognised key is `full = true | false`.
fn parse_refresh_options(inside: &str) -> Result<bool> {
    let mut force_full = false;
    for raw in split_top_level_commas(inside) {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let Some(eq) = token.find('=') else {
            return Err(BasinError::InvalidSchema(format!(
                "REFRESH MATERIALIZED VIEW: unrecognised option {token:?}"
            )));
        };
        let key = token[..eq].trim().to_ascii_lowercase();
        let value = token[eq + 1..].trim();
        match key.as_str() {
            "full" => {
                force_full = match value.to_ascii_lowercase().as_str() {
                    "true" | "t" | "1" => true,
                    "false" | "f" | "0" => false,
                    other => {
                        return Err(BasinError::InvalidSchema(format!(
                            "REFRESH MATERIALIZED VIEW: full = {other:?} (expected true/false)"
                        )));
                    }
                };
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "REFRESH MATERIALIZED VIEW: unsupported option {other:?}"
                )));
            }
        }
    }
    Ok(force_full)
}

/// Recognise `DROP MATERIALIZED VIEW <name>` (with optional `IF EXISTS`
/// and trailing `;`). Returns `(name, if_exists)` on a match, or `None`
/// when the statement is something else. sqlparser 0.52's `DROP` does not
/// accept `MATERIALIZED VIEW`, so we handle the full statement textually.
pub(crate) fn match_drop_materialized_view(sql: &str) -> Result<Option<(String, bool)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "DROP") {
        return Ok(None);
    }
    let after_drop = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_drop, "MATERIALIZED") {
        return Ok(None);
    }
    let after_mat = skip_word(after_drop).trim_start();
    if !starts_with_kw(after_mat, "VIEW") {
        return Err(BasinError::InvalidSchema(
            "DROP MATERIALIZED: expected VIEW".into(),
        ));
    }
    let after_view = skip_word(after_mat).trim_start();
    // Optional `IF EXISTS`.
    let (after_if, if_exists) = if starts_with_kw(after_view, "IF") {
        let after_if = skip_word(after_view).trim_start();
        if !starts_with_kw(after_if, "EXISTS") {
            return Err(BasinError::InvalidSchema(
                "DROP MATERIALIZED VIEW IF: expected EXISTS".into(),
            ));
        }
        (skip_word(after_if).trim_start(), true)
    } else {
        (after_view, false)
    };
    let (name, rest) = read_simple_identifier(after_if)?;
    let rest = rest.trim();
    // Tolerate optional CASCADE / RESTRICT, matching PG's DROP grammar.
    if !rest.is_empty()
        && !rest.eq_ignore_ascii_case("cascade")
        && !rest.eq_ignore_ascii_case("restrict")
    {
        return Err(BasinError::InvalidSchema(format!(
            "DROP MATERIALIZED VIEW: unexpected trailing input {rest:?}"
        )));
    }
    Ok(Some((name, if_exists)))
}

/// Execute a `CREATE MATERIALIZED VIEW ... WITH (basin.continuous, ...) AS
/// <select_query>`.
///
/// Mirrors the bootstrap path in `basin_cv::CvStore::register_cv`:
///
/// 1. Run the source query in `sess` to learn the schema and bootstrap
///    rows.
/// 2. Create a regular catalog table under `name` with that schema.
/// 3. Write the bootstrap rows as the table's first Parquet file.
/// 4. Stamp a `CvDef` on the catalog row via
///    `Catalog::set_continuous_aggregate`.
///
/// The `basin-cv` refresher (running in production) consults
/// `TableMetadata::continuous_aggregate` on each tick to find CVs to
/// refresh; a CV created by this path is byte-identical to one created
/// by the Rust API.
pub(crate) async fn exec_create_materialized_view(
    sess: &ProjectSession,
    name: &str,
    source_sql: &str,
    refresh_interval_secs: u64,
    timescaledb: bool,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;

    // Bounce early if a table or CV with the same name already exists.
    // Catalog::create_table itself returns a Catalog error on conflict,
    // but checking up front gives us a cleaner error message.
    let existing = sess
        .engine
        .config()
        .catalog
        .list_tables(&sess.project)
        .await?;
    if existing.iter().any(|t| t.as_str() == table.as_str()) {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE MATERIALIZED VIEW: relation {name:?} already exists"
        )));
    }

    // 1. Run the source query against the calling session's project.
    //    Box::pin breaks the async recursion through `crate::executor::execute`.
    let res = Box::pin(crate::executor::execute(sess, source_sql)).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE MATERIALIZED VIEW: source query is not a SELECT (got {tag})"
            )));
        }
    };
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Err(BasinError::InvalidSchema(
            "CREATE MATERIALIZED VIEW: source query returned no rows; cannot infer materialised schema".into(),
        ));
    }

    // 2. Create the catalog table with the inferred schema.
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog
        .create_table(&sess.project, &table, schema.as_ref())
        .await?;

    // 3. Write the bootstrap rows as a single Parquet file.
    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else {
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow_select::concat::concat_batches(&schema, refs)
            .map_err(|e| BasinError::internal(format!("CREATE MATERIALIZED VIEW: concat: {e}")))?
    };
    let part = PartitionKey::default_key();
    let written = sess
        .engine
        .config()
        .storage
        .write_batch(&sess.project, &table, &part, &merged)
        .await?;
    catalog
        .append_data_files(
            &sess.project,
            &table,
            SnapshotId::GENESIS,
            vec![DataFileRef {
                path: written.path.as_ref().to_string(),
                size_bytes: written.size_bytes,
                row_count: written.row_count,
                column_stats: written.column_stats.clone(),
                bloom_filters: ::std::collections::BTreeMap::new(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            }],
        )
        .await?;

    // 4. Stamp the CV definition.
    let now = Utc::now();
    let def = CvDef {
        source_table: source_table_hint(source_sql).unwrap_or_default(),
        query_sql: source_sql.to_string(),
        refresh_interval_secs,
        last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
        last_bucket_max_unix_ms: None,
        timescaledb,
        cagg_policy: None,
    };
    catalog
        .set_continuous_aggregate(&sess.project, &table, Some(def))
        .await?;

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE MATERIALIZED VIEW".into(),
    })
}

/// Execute a plain `CREATE MATERIALIZED VIEW <name> AS <select_query>` (without
/// `WITH (basin.continuous)`).  This is a one-time snapshot: the source query is
/// run immediately, its rows are persisted as a regular table (no refresh
/// interval), and no `CvDef` is stamped.  `REFRESH MATERIALIZED VIEW` on a
/// snapshot view re-executes the query and replaces the stored rows.
///
/// Errors if the source query returns no rows (the schema cannot be inferred
/// from an empty result).
pub(crate) async fn exec_create_snapshot_materialized_view(
    sess: &ProjectSession,
    name: &str,
    source_sql: &str,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;

    let existing = sess
        .engine
        .config()
        .catalog
        .list_tables(&sess.project)
        .await?;
    if existing.iter().any(|t| t.as_str() == table.as_str()) {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE MATERIALIZED VIEW: relation {name:?} already exists"
        )));
    }

    let res = Box::pin(crate::executor::execute(sess, source_sql)).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE MATERIALIZED VIEW: source query is not a SELECT (got {tag})"
            )));
        }
    };

    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog
        .create_table(&sess.project, &table, schema.as_ref())
        .await?;

    if !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0) {
        let merged = if batches.len() == 1 {
            batches[0].clone()
        } else {
            let refs: Vec<&RecordBatch> = batches.iter().collect();
            arrow_select::concat::concat_batches(&schema, refs).map_err(|e| {
                BasinError::internal(format!("CREATE MATERIALIZED VIEW: concat: {e}"))
            })?
        };
        let part = PartitionKey::default_key();
        let written = sess
            .engine
            .config()
            .storage
            .write_batch(&sess.project, &table, &part, &merged)
            .await?;
        catalog
            .append_data_files(
                &sess.project,
                &table,
                SnapshotId::GENESIS,
                vec![DataFileRef {
                    path: written.path.as_ref().to_string(),
                    size_bytes: written.size_bytes,
                    row_count: written.row_count,
                    column_stats: written.column_stats.clone(),
                    bloom_filters: ::std::collections::BTreeMap::new(),
                    hll_sketches: ::std::collections::BTreeMap::new(),
                    tdigest_sketches: ::std::collections::BTreeMap::new(),
                }],
            )
            .await?;
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE MATERIALIZED VIEW".into(),
    })
}

/// Execute a `REFRESH MATERIALIZED VIEW <name> [WITH (full=true)]`.
///
/// Mirrors `basin_cv::CvRefresher::do_refresh`:
///
/// 1. Read the CV definition from the catalog. Bail if the table isn't a
///    CV.
/// 2. Decide refresh mode:
///    - `force_full = true`, or no prior watermark, or unsupported GROUP
///      BY shape → re-execute the entire stored `query_sql`.
///    - Otherwise → incremental: re-aggregate source rows where
///      `<bucket_col> >= watermark`, drop matview rows where
///      `<bucket_alias> >= watermark`, append the new aggregation.
/// 3. Atomically swap the materialised file in via
///    `Catalog::replace_data_files`.
/// 4. Stamp a fresh `last_refreshed_at_unix_ms` and the new
///    `last_bucket_max_unix_ms` on the `CvDef`.
///
/// Unlike the production refresher, this is a one-shot — the
/// `refresh_interval` is **not** consulted. A user-issued `REFRESH
/// MATERIALIZED VIEW` always re-materialises (matching PG's semantics).
pub(crate) async fn exec_refresh_materialized_view(
    sess: &ProjectSession,
    name: &str,
    force_full: bool,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();

    // Load the CV definition. A non-CV table or a missing one are both
    // user-visible errors.
    let meta = catalog
        .load_table(&sess.project, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => {
                BasinError::not_found(format!("materialized view {name:?} does not exist"))
            }
            other => other,
        })?;
    let def = meta.continuous_aggregate.clone().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "REFRESH MATERIALIZED VIEW: {name:?} is not a continuous aggregate"
        ))
    })?;

    let bucket_info = (!force_full)
        .then(|| crate::cv_time_bucket::detect_time_bucket(&def.query_sql))
        .flatten();
    let watermark = def
        .last_bucket_max_unix_ms
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis);

    match (bucket_info, watermark) {
        (Some(info), Some(wm)) => do_refresh_incremental(sess, &table, &meta, def, &info, wm).await,
        (info, _) => do_refresh_full(sess, &table, &meta, def, info).await,
    }
}

async fn do_refresh_full(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    def: CvDef,
    bucket_info: Option<crate::cv_time_bucket::BucketInfo>,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    let res = Box::pin(crate::executor::execute(sess, &def.query_sql)).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "REFRESH MATERIALIZED VIEW: source query returned non-rows ({tag})"
            )));
        }
    };

    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let now = Utc::now();

    let new_watermark_ms = match &bucket_info {
        Some(info) => compute_watermark_ms(sess, &def.source_table, info).await,
        None => def.last_bucket_max_unix_ms,
    };

    if row_count == 0 {
        let new_def = CvDef {
            last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
            last_bucket_max_unix_ms: new_watermark_ms,
            ..def
        };
        catalog
            .set_continuous_aggregate(&sess.project, table, Some(new_def))
            .await?;
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
        return Ok(ExecResult::Empty {
            tag: "REFRESH MATERIALIZED VIEW".into(),
        });
    }

    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else {
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow_select::concat::concat_batches(&schema, refs)
            .map_err(|e| BasinError::internal(format!("REFRESH MATERIALIZED VIEW: concat: {e}")))?
    };
    let part = PartitionKey::default_key();
    let written = storage
        .write_batch(&sess.project, table, &part, &merged)
        .await?;

    let removed_paths: Vec<String> = meta
        .snapshots
        .iter()
        .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
        .collect();
    let added = vec![DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
        bloom_filters: ::std::collections::BTreeMap::new(),
        hll_sketches: ::std::collections::BTreeMap::new(),
        tdigest_sketches: ::std::collections::BTreeMap::new(),
    }];
    catalog
        .replace_data_files(
            &sess.project,
            table,
            meta.current_snapshot,
            removed_paths.clone(),
            added,
        )
        .await?;

    for path in &removed_paths {
        if path == written.path.as_ref() {
            continue;
        }
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.project, &p).await;
    }

    let new_def = CvDef {
        last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
        last_bucket_max_unix_ms: new_watermark_ms,
        ..def
    };
    catalog
        .set_continuous_aggregate(&sess.project, table, Some(new_def))
        .await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;

    Ok(ExecResult::Empty {
        tag: "REFRESH MATERIALIZED VIEW".into(),
    })
}

async fn do_refresh_incremental(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    def: CvDef,
    bucket: &crate::cv_time_bucket::BucketInfo,
    watermark: chrono::DateTime<Utc>,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    // Build the rewritten SELECT. On any failure, fall through to full.
    let rewritten = match crate::cv_time_bucket::rewrite_for_watermark(
        &def.query_sql,
        &bucket.bucket_column,
        watermark,
    ) {
        Ok(s) => s,
        Err(_) => {
            return do_refresh_full(sess, table, meta, def, Some(bucket.clone())).await;
        }
    };

    let new_res = Box::pin(crate::executor::execute(sess, &rewritten)).await?;
    let (new_schema, new_batches) = match new_res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "REFRESH MATERIALIZED VIEW: incremental returned non-rows ({tag})"
            )));
        }
    };

    let kept_sql = format!("SELECT * FROM {}", table.as_str());
    let kept_res = Box::pin(crate::executor::execute(sess, &kept_sql)).await?;
    let (existing_schema, existing_batches) = match kept_res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "REFRESH MATERIALIZED VIEW: matview SELECT returned non-rows ({tag})"
            )));
        }
    };

    let kept_batches = filter_below_watermark(&existing_batches, &bucket.bucket_alias, watermark)?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    if new_schema.fields() == existing_schema.fields() {
        all_batches.extend(kept_batches.into_iter());
    }
    all_batches.extend(new_batches.into_iter());

    let now = Utc::now();
    let new_watermark_ms = compute_watermark_ms(sess, &def.source_table, bucket)
        .await
        .or(Some(watermark.timestamp_millis()));

    if all_batches.is_empty() {
        let new_def = CvDef {
            last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
            last_bucket_max_unix_ms: new_watermark_ms,
            ..def
        };
        catalog
            .set_continuous_aggregate(&sess.project, table, Some(new_def))
            .await?;
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
        return Ok(ExecResult::Empty {
            tag: "REFRESH MATERIALIZED VIEW".into(),
        });
    }

    let merged = if all_batches.len() == 1 {
        all_batches.remove(0)
    } else {
        let refs: Vec<&RecordBatch> = all_batches.iter().collect();
        arrow_select::concat::concat_batches(&new_schema, refs)
            .map_err(|e| BasinError::internal(format!("REFRESH MATERIALIZED VIEW: concat: {e}")))?
    };
    let part = PartitionKey::default_key();
    let written = storage
        .write_batch(&sess.project, table, &part, &merged)
        .await?;

    let removed_paths: Vec<String> = meta
        .snapshots
        .iter()
        .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
        .collect();
    let added = vec![DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
        bloom_filters: ::std::collections::BTreeMap::new(),
        hll_sketches: ::std::collections::BTreeMap::new(),
        tdigest_sketches: ::std::collections::BTreeMap::new(),
    }];
    catalog
        .replace_data_files(
            &sess.project,
            table,
            meta.current_snapshot,
            removed_paths.clone(),
            added,
        )
        .await?;
    for path in &removed_paths {
        if path == written.path.as_ref() {
            continue;
        }
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.project, &p).await;
    }

    let new_def = CvDef {
        last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
        last_bucket_max_unix_ms: new_watermark_ms,
        ..def
    };
    catalog
        .set_continuous_aggregate(&sess.project, table, Some(new_def))
        .await?;
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;

    Ok(ExecResult::Empty {
        tag: "REFRESH MATERIALIZED VIEW".into(),
    })
}

/// Query the source for `max(<bucket_col>)`, floor to the bucket start,
/// and return the result as Unix-epoch milliseconds. Returns `None` when
/// the source is empty / the column type isn't a recognised timestamp.
async fn compute_watermark_ms(
    sess: &ProjectSession,
    source_table: &str,
    bucket: &crate::cv_time_bucket::BucketInfo,
) -> Option<i64> {
    if source_table.is_empty() {
        return None;
    }
    let sql = format!(
        "SELECT max({col}) AS m FROM {tbl}",
        col = bucket.bucket_column,
        tbl = source_table,
    );
    let res = Box::pin(crate::executor::execute(sess, &sql)).await.ok()?;
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    use arrow_array::{
        Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch.column(0);
        if arr.is_null(0) {
            return None;
        }
        let raw: Option<chrono::DateTime<Utc>> = match arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .and_then(|a| chrono::DateTime::<Utc>::from_timestamp_micros(a.value(0))),
            DataType::Timestamp(TimeUnit::Millisecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .and_then(|a| chrono::DateTime::<Utc>::from_timestamp_millis(a.value(0))),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .and_then(|a| {
                    let v = a.value(0);
                    chrono::DateTime::<Utc>::from_timestamp(
                        v / 1_000_000_000,
                        (v % 1_000_000_000) as u32,
                    )
                }),
            DataType::Timestamp(TimeUnit::Second, _) => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .and_then(|a| chrono::DateTime::<Utc>::from_timestamp(a.value(0), 0)),
            _ => None,
        };
        if let Some(ts) = raw {
            return Some(bucket.width.floor(ts).timestamp_millis());
        }
    }
    None
}

/// Filter `batches` to rows whose `<bucket_alias>` value is strictly less
/// than `watermark`. Mirrors `basin_cv::refresher::filter_below_watermark`.
fn filter_below_watermark(
    batches: &[RecordBatch],
    bucket_alias: &str,
    watermark: chrono::DateTime<Utc>,
) -> Result<Vec<RecordBatch>> {
    use arrow_array::{
        Array, BooleanArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let col_idx = schema.index_of(bucket_alias).ok().or_else(|| {
        schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(bucket_alias))
    });
    let Some(col_idx) = col_idx else {
        return Ok(Vec::new());
    };
    let mut kept: Vec<RecordBatch> = Vec::new();
    for batch in batches {
        let arr = batch.column(col_idx);
        let mask: Option<BooleanArray> = match arr.data_type() {
            DataType::Timestamp(unit, _) => {
                let bound = match unit {
                    TimeUnit::Second => watermark.timestamp(),
                    TimeUnit::Millisecond => watermark.timestamp_millis(),
                    TimeUnit::Microsecond => watermark.timestamp_micros(),
                    TimeUnit::Nanosecond => match watermark.timestamp_nanos_opt() {
                        Some(v) => v,
                        None => return Ok(Vec::new()),
                    },
                };
                let values: Vec<Option<bool>> = (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            Some(false)
                        } else {
                            let v = match unit {
                                TimeUnit::Second => arr
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .unwrap()
                                    .value(i),
                                TimeUnit::Millisecond => arr
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .unwrap()
                                    .value(i),
                                TimeUnit::Microsecond => arr
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .unwrap()
                                    .value(i),
                                TimeUnit::Nanosecond => arr
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .unwrap()
                                    .value(i),
                            };
                            Some(v < bound)
                        }
                    })
                    .collect();
                Some(BooleanArray::from(values))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let watermark_str = watermark.format("%Y-%m-%d %H:%M:%S").to_string();
                let arr_strs: Vec<Option<String>> =
                    if matches!(arr.data_type(), DataType::LargeUtf8) {
                        let s = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        (0..s.len())
                            .map(|i| {
                                if s.is_null(i) {
                                    None
                                } else {
                                    Some(s.value(i).to_string())
                                }
                            })
                            .collect()
                    } else {
                        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..s.len())
                            .map(|i| {
                                if s.is_null(i) {
                                    None
                                } else {
                                    Some(s.value(i).to_string())
                                }
                            })
                            .collect()
                    };
                let values: Vec<Option<bool>> = arr_strs
                    .iter()
                    .map(|opt| match opt {
                        None => Some(false),
                        Some(s) => {
                            let s_norm = s.replace('T', " ");
                            Some(s_norm.as_str() < watermark_str.as_str())
                        }
                    })
                    .collect();
                Some(BooleanArray::from(values))
            }
            _ => None,
        };
        let Some(mask) = mask else {
            return Ok(Vec::new());
        };
        let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| BasinError::internal(format!("filter: {e}")))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}

/// Execute a `DROP MATERIALIZED VIEW [IF EXISTS] <name>`. Removes the
/// catalog row (which carries the `CvDef` alongside the table metadata)
/// and physically deletes the materialised Parquet files. The catalog's
/// `drop_table` is the single point of truth: from the user's
/// perspective, a SELECT against the dropped view returns the same
/// "relation does not exist" error a regular DROP TABLE produces.
pub(crate) async fn exec_drop_materialized_view(
    sess: &ProjectSession,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    // Look up the file set before dropping the catalog row, so we can
    // physically delete the bytes after the catalog commits.
    let paths_to_delete: Vec<String> = match catalog.load_table(&sess.project, &table).await {
        Ok(meta) => {
            // Confirm this is a CV — refusing to drop a regular table via
            // DROP MATERIALIZED VIEW matches PG's behaviour.
            if meta.continuous_aggregate.is_none() {
                return Err(BasinError::InvalidSchema(format!(
                    "DROP MATERIALIZED VIEW: {name:?} is not a continuous aggregate"
                )));
            }
            meta.snapshots
                .iter()
                .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
                .collect()
        }
        Err(BasinError::NotFound(_)) if if_exists => {
            return Ok(ExecResult::Empty {
                tag: "DROP MATERIALIZED VIEW".into(),
            });
        }
        Err(BasinError::NotFound(_)) => {
            return Err(BasinError::not_found(format!(
                "materialized view {name:?} does not exist"
            )));
        }
        Err(e) => return Err(e),
    };

    catalog.drop_table(&sess.project, &table).await?;
    // De-register the listing table from the DataFusion session so a
    // subsequent SELECT against the dropped view surfaces a "relation does
    // not exist" error rather than reading a stale registration.
    let _ = sess.ctx.deregister_table(table.as_str());

    for path in &paths_to_delete {
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.project, &p).await;
    }

    Ok(ExecResult::Empty {
        tag: "DROP MATERIALIZED VIEW".into(),
    })
}

// --- Lexer helpers ---------------------------------------------------

/// True if `s` (after leading whitespace stripped) begins with `kw`,
/// case-insensitive, and the byte after the keyword is not part of an
/// identifier (so `WITH` matches but `WITHOUT` does not).
fn starts_with_kw(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let kw = kw.as_bytes();
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    if bytes.len() == kw.len() {
        return true;
    }
    let next = bytes[kw.len()];
    !(next.is_ascii_alphanumeric() || next == b'_')
}

/// Skip the leading word in `s` (after leading whitespace), returning the
/// remainder. Caller is expected to have already verified the word with
/// `starts_with_kw`. Word characters are ASCII alphanumerics and `_`.
fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

/// Read a bare SQL identifier from the head of `s`. Returns `(name,
/// rest)`. Quoted identifiers and schema-qualified names are out of
/// scope for v0.1 — same constraint as the rest of the engine's DDL
/// extensions.
fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "expected an identifier, got end of statement".into(),
        ));
    }
    // Reject leading digits and quoted forms.
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

/// Skip a region the WITH-clause finder must not look inside: string
/// literals, quoted identifiers, comments. Returns the new cursor on a
/// match, or `None` when `bytes[i]` is not the start of a skippable
/// region.
fn skip_skippable(bytes: &[u8], i: usize) -> Option<usize> {
    let b = bytes[i];
    if b == b'\'' {
        let mut j = i + 1;
        while j < bytes.len() {
            if bytes[j] == b'\'' {
                if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                    j += 2;
                    continue;
                }
                return Some(j + 1);
            }
            j += 1;
        }
        return Some(j);
    }
    if b == b'"' {
        let mut j = i + 1;
        while j < bytes.len() {
            if bytes[j] == b'"' {
                if j + 1 < bytes.len() && bytes[j + 1] == b'"' {
                    j += 2;
                    continue;
                }
                return Some(j + 1);
            }
            j += 1;
        }
        return Some(j);
    }
    if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
        let mut j = i;
        while j < bytes.len() && bytes[j] != b'\n' {
            j += 1;
        }
        return Some(j);
    }
    if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
        let mut j = i + 2;
        while j + 1 < bytes.len() && !(bytes[j] == b'*' && bytes[j + 1] == b'/') {
            j += 1;
        }
        if j + 1 < bytes.len() {
            return Some(j + 2);
        }
        return Some(j);
    }
    None
}

fn bytes_match_kw(bytes: &[u8], kw: &[u8]) -> bool {
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    true
}

fn at_word_boundary(bytes: &[u8], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let prev = bytes[i - 1];
    !(prev.is_ascii_alphanumeric() || prev == b'_')
}

fn at_word_boundary_end(bytes: &[u8], i: usize) -> bool {
    if i >= bytes.len() {
        return true;
    }
    let c = bytes[i];
    !(c.is_ascii_alphanumeric() || c == b'_')
}

/// Best-effort extraction of the `FROM <table>` token in the source SQL,
/// for stamping `CvDef::source_table`. Falls back to an empty string
/// when the SELECT doesn't have a single bare-identifier source — the
/// CvDef field is informational (v0.2's incremental refresh planner reads
/// it without re-parsing) and is allowed to be empty.
fn source_table_hint(source_sql: &str) -> Option<String> {
    let lower = source_sql.to_ascii_lowercase();
    let bytes = source_sql.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        if at_word_boundary(bytes, i)
            && lower.as_bytes()[i..i + 4] == *b"from"
            && at_word_boundary_end(bytes, i + 4)
        {
            let after = source_sql[i + 4..].trim_start();
            let after_bytes = after.as_bytes();
            if after_bytes.is_empty() {
                return None;
            }
            if !(after_bytes[0].is_ascii_alphabetic() || after_bytes[0] == b'_') {
                return None;
            }
            let mut j = 1;
            while j < after_bytes.len()
                && (after_bytes[j].is_ascii_alphanumeric() || after_bytes[j] == b'_')
            {
                j += 1;
            }
            return Some(after[..j].to_string());
        }
        i += 1;
    }
    None
}

// ==========================================================================
// pg_query AST-based matchers  (alongside the textual ones above)
// ==========================================================================

/// Intent produced by [`match_create_materialized_view_ast`]. Carries the
/// materialised view name, the deparsed source SQL, and the parsed Basin
/// WITH options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateMatViewIntent {
    /// Bare table name (single-part, no schema qualifier).
    pub name: String,
    /// The `AS <select_query>` body, reconstructed by deparsing the parsed
    /// query node back to SQL text.
    pub source_sql: String,
    /// Basin-specific `WITH (...)` options extracted from `into.options`.
    pub options: CvOptions,
}

/// AST-based matcher for `REFRESH MATERIALIZED VIEW <name>`.
///
/// libpg_query parses this as [`pg_query::protobuf::RefreshMatViewStmt`].
/// Note: Basin's custom `WITH (full = true)` extension is **not** valid
/// PG syntax and therefore cannot be parsed by libpg_query — only the
/// plain `REFRESH MATERIALIZED VIEW <name>` form is matched here, so
/// `force_full` is always `false`. Callers that need to detect
/// `WITH (full = true)` should use the textual
/// [`match_refresh_materialized_view`] pre-screen instead.
///
/// Returns `(name, force_full)`.
pub(crate) fn match_refresh_materialized_view_ast(
    stmt: &pg_query::protobuf::RefreshMatViewStmt,
) -> Result<(String, bool)> {
    let rel = stmt.relation.as_ref().ok_or_else(|| {
        BasinError::InvalidSchema("REFRESH MATERIALIZED VIEW: missing relation in AST".into())
    })?;
    if rel.relname.is_empty() {
        return Err(BasinError::InvalidSchema(
            "REFRESH MATERIALIZED VIEW: empty relation name".into(),
        ));
    }
    // PG's native REFRESH MATERIALIZED VIEW has no `WITH (full=true)` option.
    // The `concurrent` flag (REFRESH MATERIALIZED VIEW CONCURRENTLY) is not
    // the same as Basin's `force_full`; we leave force_full = false here.
    Ok((rel.relname.clone(), false))
}

/// AST-based matcher for `DROP MATERIALIZED VIEW [IF EXISTS] <name>`.
///
/// libpg_query routes this as a [`pg_query::protobuf::DropStmt`] with
/// `remove_type == ObjectType::ObjectMatview`. Returns
/// `Some((name, if_exists))` when the DropStmt targets a materialized view;
/// `None` when it targets something else (the caller falls through to the
/// next handler).
pub(crate) fn match_drop_materialized_view_ast(
    stmt: &pg_query::protobuf::DropStmt,
) -> Result<Option<(String, bool)>> {
    use pg_query::protobuf::ObjectType;
    if stmt.remove_type != ObjectType::ObjectMatview as i32 {
        return Ok(None);
    }
    // For DROP MATERIALIZED VIEW the name lives at objects[0] as a List
    // whose first item is a String node.
    let name = drop_stmt_first_name(&stmt.objects).ok_or_else(|| {
        BasinError::InvalidSchema("DROP MATERIALIZED VIEW: could not extract name from AST".into())
    })?;
    Ok(Some((name, stmt.missing_ok)))
}

/// AST-based matcher for
/// `CREATE MATERIALIZED VIEW <name> [WITH (basin.continuous, refresh_interval = '<dur>')] AS <query>`.
///
/// libpg_query parses this as a
/// [`pg_query::protobuf::CreateTableAsStmt`] with
/// `objtype == ObjectType::ObjectMatview`. The Basin-specific `WITH (...)`
/// options live on `into.options` as `DefElem` nodes.
///
/// Returns `Some(CreateMatViewIntent)` when `objtype` is `ObjectMatview`;
/// `None` when the `CreateTableAsStmt` targets something else.
pub(crate) fn match_create_materialized_view_ast(
    stmt: &pg_query::protobuf::CreateTableAsStmt,
) -> Result<Option<CreateMatViewIntent>> {
    use pg_query::protobuf::ObjectType;
    if stmt.objtype != ObjectType::ObjectMatview as i32 {
        return Ok(None);
    }
    let into = stmt.into.as_deref().ok_or_else(|| {
        BasinError::InvalidSchema("CREATE MATERIALIZED VIEW: missing INTO clause in AST".into())
    })?;
    let rel = into.rel.as_ref().ok_or_else(|| {
        BasinError::InvalidSchema(
            "CREATE MATERIALIZED VIEW: missing relation in INTO clause".into(),
        )
    })?;
    if rel.relname.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE MATERIALIZED VIEW: empty view name".into(),
        ));
    }
    let name = rel.relname.clone();

    // Extract the source SQL by deparsing the query node back to text.
    let query_node = stmt.query.as_deref().ok_or_else(|| {
        BasinError::InvalidSchema("CREATE MATERIALIZED VIEW: missing query in AST".into())
    })?;
    let source_sql = deparse_node_as_stmt(query_node).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CREATE MATERIALIZED VIEW: failed to reconstruct source SQL: {e}"
        ))
    })?;

    // Extract Basin-specific options from `into.options` (DefElem list).
    let options = parse_into_cv_options(&into.options)?;

    Ok(Some(CreateMatViewIntent {
        name,
        source_sql,
        options,
    }))
}

/// Extract Basin CV options (`basin.continuous`, `refresh_interval`) from
/// the `INTO` clause's option list. The option list contains `DefElem`
/// nodes; `basin.continuous` arrives as
/// `DefElem { defnamespace: "basin", defname: "continuous", arg: None }`
/// and `refresh_interval` as
/// `DefElem { defname: "refresh_interval", arg: String("5m") }`.
fn parse_into_cv_options(options: &[pg_query::protobuf::Node]) -> Result<CvOptions> {
    use pg_query::NodeEnum;
    let mut continuous = false;
    let mut timescaledb = false;
    let mut refresh_interval_secs: Option<u64> = None;
    for node in options {
        let Some(NodeEnum::DefElem(de)) = node.node.as_ref() else {
            continue;
        };
        // `basin.continuous` → defnamespace="basin", defname="continuous", no arg.
        // `timescaledb.continuous` → defnamespace="timescaledb".
        // Also accept bare `continuous` (defnamespace empty).
        let ns = de.defnamespace.to_ascii_lowercase();
        let name = de.defname.to_ascii_lowercase();
        if name == "continuous" && ns == "timescaledb" {
            continuous = true;
            timescaledb = true;
            continue;
        }
        if name == "continuous" && (ns.is_empty() || ns == "basin") {
            continuous = true;
            continue;
        }
        if name == "refresh_interval" && (ns.is_empty() || ns == "basin") {
            let arg = de.arg.as_deref().ok_or_else(|| {
                BasinError::InvalidSchema(
                    "CREATE MATERIALIZED VIEW: refresh_interval missing value".into(),
                )
            })?;
            let val_str = str_from_node(arg).ok_or_else(|| {
                BasinError::InvalidSchema(
                    "CREATE MATERIALIZED VIEW: refresh_interval must be a string literal".into(),
                )
            })?;
            refresh_interval_secs = Some(parse_duration_secs(&val_str)?);
            continue;
        }
        // Unknown option — reject so a typo doesn't silently become a regular matview.
        return Err(BasinError::InvalidSchema(format!(
            "CREATE MATERIALIZED VIEW: unsupported option {:?}",
            de.defname
        )));
    }
    Ok(CvOptions {
        continuous,
        refresh_interval_secs,
        timescaledb,
    })
}

/// Extract the string value from a `String` node, or `None` if the node
/// is some other type.
fn str_from_node(node: &pg_query::protobuf::Node) -> Option<String> {
    use pg_query::NodeEnum;
    match node.node.as_ref()? {
        NodeEnum::String(s) => Some(s.sval.clone()),
        NodeEnum::AConst(ac) => {
            use pg_query::protobuf::a_const::Val;
            match ac.val.as_ref()? {
                Val::Sval(s) => Some(s.sval.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// For a `DROP` statement, extract the first name from the objects list.
/// The objects list for `DROP MATERIALIZED VIEW foo` is:
///   `[ List { items: [ String("foo") ] } ]`
fn drop_stmt_first_name(objects: &[pg_query::protobuf::Node]) -> Option<String> {
    use pg_query::NodeEnum;
    let first = objects.first()?;
    match first.node.as_ref()? {
        // DROP MATVIEW / DROP DOMAIN: objects[0] is a List([String(name)])
        NodeEnum::List(list) => {
            let item = list.items.first()?;
            match item.node.as_ref()? {
                NodeEnum::String(s) => Some(s.sval.clone()),
                _ => None,
            }
        }
        // Some DROP forms wrap directly in a String node.
        NodeEnum::String(s) => Some(s.sval.clone()),
        _ => None,
    }
}

/// Deparse a statement-level `Node` (e.g., a `SelectStmt`) back to its
/// SQL text by wrapping it in a synthetic `ParseResult` and calling
/// `pg_query::deparse`.
///
/// libpg_query's C deparser asserts that `ParseResult::version` matches
/// the linked `PG_VERSION_NUM`. We can't read that constant directly
/// (the `bindings` module is private), so we bootstrap the version by
/// parsing a trivial `SELECT 1` and reusing its `version` field.
pub(crate) fn deparse_node_as_stmt(
    node: &pg_query::protobuf::Node,
) -> std::result::Result<String, pg_query::Error> {
    let version = pg_query::parse("SELECT 1")?.protobuf.version;
    let raw_stmt = pg_query::protobuf::RawStmt {
        stmt: Some(Box::new(node.clone())),
        stmt_location: 0,
        stmt_len: 0,
    };
    let parse_result = pg_query::protobuf::ParseResult {
        version,
        stmts: vec![raw_stmt],
    };
    pg_query::deparse(&parse_result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_basin_cv_options_strips_with_clause() {
        let (stripped, opts) = extract_basin_cv_options(
            "CREATE MATERIALIZED VIEW mv WITH (basin.continuous, refresh_interval = '5m') AS SELECT 1",
        )
        .unwrap();
        let opts = opts.unwrap();
        assert!(opts.continuous);
        assert_eq!(opts.refresh_interval_secs, Some(300));
        // sqlparser must accept what's left.
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, &stripped)
            .unwrap();
    }

    #[test]
    fn extract_basin_cv_options_no_with_returns_empty_options() {
        let (stripped, opts) =
            extract_basin_cv_options("CREATE MATERIALIZED VIEW mv AS SELECT 1").unwrap();
        let opts = opts.unwrap();
        assert!(!opts.continuous);
        assert_eq!(opts.refresh_interval_secs, None);
        assert_eq!(stripped.trim(), "CREATE MATERIALIZED VIEW mv AS SELECT 1");
    }

    #[test]
    fn extract_basin_cv_options_non_create_passes_through() {
        let (stripped, opts) = extract_basin_cv_options("SELECT 1").unwrap();
        assert!(opts.is_none());
        assert_eq!(stripped, "SELECT 1");
    }

    #[test]
    fn parse_duration_secs_basic() {
        assert_eq!(parse_duration_secs("5m").unwrap(), 300);
        assert_eq!(parse_duration_secs("1 hour").unwrap(), 3600);
        assert_eq!(parse_duration_secs("30s").unwrap(), 30);
        assert_eq!(parse_duration_secs("2 hours 30 minutes").unwrap(), 9000);
        assert_eq!(parse_duration_secs("1 day").unwrap(), 86_400);
    }

    #[test]
    fn parse_duration_secs_rejects_garbage() {
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("abc").is_err());
        assert!(parse_duration_secs("5 lightyears").is_err());
        assert!(parse_duration_secs("5").is_err());
    }

    #[test]
    fn match_refresh_recognises_form() {
        assert_eq!(
            match_refresh_materialized_view("REFRESH MATERIALIZED VIEW mv").unwrap(),
            Some(("mv".to_string(), false))
        );
        assert_eq!(
            match_refresh_materialized_view("refresh materialized view mv;").unwrap(),
            Some(("mv".to_string(), false))
        );
        assert_eq!(match_refresh_materialized_view("SELECT 1").unwrap(), None);
        // WITH (full = true) opts out of incremental.
        assert_eq!(
            match_refresh_materialized_view("REFRESH MATERIALIZED VIEW mv WITH (full = true)")
                .unwrap(),
            Some(("mv".to_string(), true))
        );
        assert_eq!(
            match_refresh_materialized_view("REFRESH MATERIALIZED VIEW mv WITH (full=false);")
                .unwrap(),
            Some(("mv".to_string(), false))
        );
    }

    #[test]
    fn match_drop_recognises_form() {
        assert_eq!(
            match_drop_materialized_view("DROP MATERIALIZED VIEW mv").unwrap(),
            Some(("mv".to_string(), false))
        );
        assert_eq!(
            match_drop_materialized_view("DROP MATERIALIZED VIEW IF EXISTS mv").unwrap(),
            Some(("mv".to_string(), true))
        );
        // DROP TABLE / DROP VIEW must not be claimed by us — sqlparser
        // handles those.
        assert_eq!(
            match_drop_materialized_view("DROP TABLE foo").unwrap(),
            None
        );
        assert_eq!(match_drop_materialized_view("DROP VIEW foo").unwrap(), None);
    }

    #[test]
    fn unquote_string_literal_handles_doubled_quotes() {
        assert_eq!(unquote_string_literal("'hello'"), Some("hello".to_string()));
        assert_eq!(unquote_string_literal("'it''s'"), Some("it's".to_string()));
        assert_eq!(unquote_string_literal("hello"), None);
    }

    #[test]
    fn source_table_hint_finds_simple_from() {
        assert_eq!(
            source_table_hint("SELECT count(*) FROM events"),
            Some("events".to_string())
        );
        assert_eq!(
            source_table_hint("select * from orders where id = 1"),
            Some("orders".to_string())
        );
        assert_eq!(source_table_hint("SELECT 1"), None);
    }

    // --- AST-based matcher tests ------------------------------------------

    #[test]
    fn ast_match_refresh_materialized_view_simple() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("REFRESH MATERIALIZED VIEW mv").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::RefreshMatViewStmt(ref rms) = stmt.node.as_ref().unwrap() else {
            panic!("expected RefreshMatViewStmt");
        };
        let (name, force_full) = match_refresh_materialized_view_ast(rms).unwrap();
        assert_eq!(name, "mv");
        assert!(!force_full);
    }

    #[test]
    fn ast_match_refresh_materialized_view_with_full_does_not_parse() {
        // Basin's custom WITH (full=true) is not valid PG syntax — ensure
        // libpg_query rejects it so callers must use the textual pre-screen.
        let result = pg_query::parse("REFRESH MATERIALIZED VIEW mv WITH (full = true)");
        assert!(
            result.is_err(),
            "expected parse failure for Basin-only syntax"
        );
    }

    #[test]
    fn ast_match_drop_materialized_view_if_exists() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("DROP MATERIALIZED VIEW IF EXISTS mv").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_materialized_view_ast(ds).unwrap();
        assert_eq!(result, Some(("mv".to_string(), true)));
    }

    #[test]
    fn ast_match_drop_materialized_view_no_if_exists() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("DROP MATERIALIZED VIEW mv").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_materialized_view_ast(ds).unwrap();
        assert_eq!(result, Some(("mv".to_string(), false)));
    }

    #[test]
    fn ast_match_drop_table_returns_none() {
        use pg_query::NodeEnum;
        // DROP TABLE must NOT be matched by the matview matcher.
        let r = pg_query::parse("DROP TABLE foo").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_materialized_view_ast(ds).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn ast_match_create_materialized_view_no_options() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE MATERIALIZED VIEW mv AS SELECT 1").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateTableAsStmt(ref ctas) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateTableAsStmt");
        };
        let intent = match_create_materialized_view_ast(ctas).unwrap().unwrap();
        assert_eq!(intent.name, "mv");
        assert!(!intent.options.continuous);
        assert!(intent.options.refresh_interval_secs.is_none());
        // Source SQL should round-trip as a SELECT.
        assert!(intent.source_sql.to_ascii_uppercase().contains("SELECT"));
    }
}
