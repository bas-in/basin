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
use crate::{ExecResult, TenantSession};

/// Continuous-aggregate options lifted out of a `CREATE MATERIALIZED VIEW
/// ... WITH (...)` clause.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CvOptions {
    /// `basin.continuous` flag must be present and true. The flag's
    /// existence in the option list is what flags the view as a continuous
    /// aggregate (vs. a regular materialised view).
    pub continuous: bool,
    /// `refresh_interval = '<duration>'`. Required when `continuous = true`.
    /// Parsed at the point of registration; stored as seconds in the
    /// catalog (one `BIGINT`).
    pub refresh_interval_secs: Option<u64>,
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
        return Ok((sql.to_string(), Some(CvOptions { continuous: false, refresh_interval_secs: None })));
    };

    let opts = parse_cv_options(&inside)?;
    let mut stripped = String::with_capacity(sql.len());
    stripped.push_str(&sql[..start]);
    // Trim a trailing whitespace before the WITH so we don't get double
    // spaces; sqlparser tolerates either, but cleaner output helps debugging.
    let stripped = stripped.trim_end_matches(|c: char| c.is_whitespace()).to_string();
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
    let mut refresh_interval_secs: Option<u64> = None;
    for raw in split_top_level_commas(inside) {
        let token = raw.trim();
        if token.is_empty() {
            continue;
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
        total = total.checked_add(num.checked_mul(factor).ok_or_else(|| {
            BasinError::InvalidSchema("refresh_interval: overflow".into())
        })?).ok_or_else(|| {
            BasinError::InvalidSchema("refresh_interval: overflow".into())
        })?;
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

/// Recognise `REFRESH MATERIALIZED VIEW <name>` (with optional trailing
/// `;`). Returns the bare identifier on a match, or `None` when the
/// statement is something else. sqlparser 0.52 has no `REFRESH` AST node,
/// so we handle the full statement textually.
pub(crate) fn match_refresh_materialized_view(sql: &str) -> Result<Option<String>> {
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
    if !rest.trim().is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "REFRESH MATERIALIZED VIEW: unexpected trailing input {:?}",
            rest.trim()
        )));
    }
    Ok(Some(name))
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
    sess: &TenantSession,
    name: &str,
    source_sql: &str,
    refresh_interval_secs: u64,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;

    // Bounce early if a table or CV with the same name already exists.
    // Catalog::create_table itself returns a Catalog error on conflict,
    // but checking up front gives us a cleaner error message.
    let existing = sess
        .engine
        .config()
        .catalog
        .list_tables(&sess.tenant)
        .await?;
    if existing.iter().any(|t| t.as_str() == table.as_str()) {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE MATERIALIZED VIEW: relation {name:?} already exists"
        )));
    }

    // 1. Run the source query against the calling session's tenant.
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
        .create_table(&sess.tenant, &table, schema.as_ref())
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
        .write_batch(&sess.tenant, &table, &part, &merged)
        .await?;
    catalog
        .append_data_files(
            &sess.tenant,
            &table,
            SnapshotId::GENESIS,
            vec![DataFileRef {
                path: written.path.as_ref().to_string(),
                size_bytes: written.size_bytes,
                row_count: written.row_count,
                column_stats: written.column_stats.clone(),
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
    };
    catalog
        .set_continuous_aggregate(&sess.tenant, &table, Some(def))
        .await?;

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE MATERIALIZED VIEW".into(),
    })
}

/// Execute a `REFRESH MATERIALIZED VIEW <name>`.
///
/// Mirrors `basin_cv::CvRefresher::do_refresh`:
///
/// 1. Read the CV definition from the catalog. Bail if the table isn't a
///    CV.
/// 2. Re-execute the stored `query_sql` against the calling session's
///    tenant.
/// 3. Atomically swap the materialised file in via
///    `Catalog::replace_data_files`.
/// 4. Stamp a fresh `last_refreshed_at_unix_ms` on the `CvDef`.
///
/// Unlike the production refresher, this is a one-shot — the
/// `refresh_interval` is **not** consulted. A user-issued `REFRESH
/// MATERIALIZED VIEW` always re-materialises (matching PG's semantics).
pub(crate) async fn exec_refresh_materialized_view(
    sess: &TenantSession,
    name: &str,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    // Load the CV definition. A non-CV table or a missing one are both
    // user-visible errors.
    let meta = catalog.load_table(&sess.tenant, &table).await.map_err(|e| {
        match e {
            BasinError::NotFound(_) => {
                BasinError::not_found(format!("materialized view {name:?} does not exist"))
            }
            other => other,
        }
    })?;
    let def = meta.continuous_aggregate.clone().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "REFRESH MATERIALIZED VIEW: {name:?} is not a continuous aggregate"
        ))
    })?;

    // 1. Re-run the stored query. Box::pin breaks the async recursion
    //    through `crate::executor::execute`.
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
    if row_count == 0 {
        // No rows: skip the file write but still stamp the refresh
        // timestamp so the next refresh-due check measures from `now`.
        let new_def = CvDef {
            last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
            ..def
        };
        catalog
            .set_continuous_aggregate(&sess.tenant, &table, Some(new_def))
            .await?;
        refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;
        return Ok(ExecResult::Empty {
            tag: "REFRESH MATERIALIZED VIEW".into(),
        });
    }

    // 2. Write the new materialisation as a single Parquet file under the
    //    CV's prefix.
    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else {
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        arrow_select::concat::concat_batches(&schema, refs)
            .map_err(|e| BasinError::internal(format!("REFRESH MATERIALIZED VIEW: concat: {e}")))?
    };
    let part = PartitionKey::default_key();
    let written = storage
        .write_batch(&sess.tenant, &table, &part, &merged)
        .await?;

    // 3. Atomically swap the catalog manifest.
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
    }];
    catalog
        .replace_data_files(
            &sess.tenant,
            &table,
            meta.current_snapshot,
            removed_paths.clone(),
            added,
        )
        .await?;

    // 4. Best-effort: physically delete the prior files.
    for path in &removed_paths {
        if path == written.path.as_ref() {
            continue;
        }
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.tenant, &p).await;
    }

    // 5. Stamp the refresh state.
    let new_def = CvDef {
        last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
        ..def
    };
    catalog
        .set_continuous_aggregate(&sess.tenant, &table, Some(new_def))
        .await?;

    refresh_table(&sess.engine, &sess.tenant, &sess.ctx, &sess.state, &table).await?;

    Ok(ExecResult::Empty {
        tag: "REFRESH MATERIALIZED VIEW".into(),
    })
}

/// Execute a `DROP MATERIALIZED VIEW [IF EXISTS] <name>`. Removes the
/// catalog row (which carries the `CvDef` alongside the table metadata)
/// and physically deletes the materialised Parquet files. The catalog's
/// `drop_table` is the single point of truth: from the user's
/// perspective, a SELECT against the dropped view returns the same
/// "relation does not exist" error a regular DROP TABLE produces.
pub(crate) async fn exec_drop_materialized_view(
    sess: &TenantSession,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let table = TableName::new(name)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    // Look up the file set before dropping the catalog row, so we can
    // physically delete the bytes after the catalog commits.
    let paths_to_delete: Vec<String> = match catalog.load_table(&sess.tenant, &table).await {
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

    catalog.drop_table(&sess.tenant, &table).await?;
    // De-register the listing table from the DataFusion session so a
    // subsequent SELECT against the dropped view surfaces a "relation does
    // not exist" error rather than reading a stale registration.
    let _ = sess.ctx.deregister_table(table.as_str());

    for path in &paths_to_delete {
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.tenant, &p).await;
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
        sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            &stripped,
        )
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
            Some("mv".to_string())
        );
        assert_eq!(
            match_refresh_materialized_view("refresh materialized view mv;").unwrap(),
            Some("mv".to_string())
        );
        assert_eq!(
            match_refresh_materialized_view("SELECT 1").unwrap(),
            None
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
        assert_eq!(
            match_drop_materialized_view("DROP VIEW foo").unwrap(),
            None
        );
    }

    #[test]
    fn unquote_string_literal_handles_doubled_quotes() {
        assert_eq!(
            unquote_string_literal("'hello'"),
            Some("hello".to_string())
        );
        assert_eq!(
            unquote_string_literal("'it''s'"),
            Some("it's".to_string())
        );
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
}
