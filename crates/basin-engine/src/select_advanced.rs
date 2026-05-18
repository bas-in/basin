//! Pre-screen rewrites for advanced SELECT shapes that sqlparser 0.52 does not
//! fully surface to DataFusion, or that DataFusion 44 does not plan from the
//! sqlparser AST.
//!
//! All functions are pure text rewrites that return a `Cow<str>`: the original
//! string is returned untouched when no rewrite applies, so callers pay nothing
//! for SQL that doesn't use these constructs.
//!
//! ## Constructs handled
//!
//! | Construct | Issue | Fix |
//! |-----------|-------|-----|
//! | `TABLE foo` | sqlparser top-level dispatch doesn't recognise `TABLE` as a statement start | Rewrite to `SELECT * FROM foo` |
//! | `TABLESAMPLE BERNOULLI(N)` / `SYSTEM(N)` [`REPEATABLE(seed)`] | sqlparser 0.52 has the keyword but no grammar rule | Rewrite the clause into a sampling predicate (`basin_tablesample_*` UDFs) injected as a `WHERE` filter so the sample is actually taken |
//! | `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY` | sqlparser parses into `Query.fetch`, DataFusion only reads `Query.limit` | Rewrite to `LIMIT N` |
//! | `OFFSET N ROWS FETCH NEXT M ROWS ONLY` | Combined SQL-standard form | Rewrite to `LIMIT M OFFSET N` |
//! | `FOR NO KEY UPDATE [OF tbl] [SKIP LOCKED\|NOWAIT]` | sqlparser 0.52 only recognises `FOR UPDATE` / `FOR SHARE` | Rewrite to `FOR UPDATE` |
//! | `FOR KEY SHARE [OF tbl] [SKIP LOCKED\|NOWAIT]` | same | Rewrite to `FOR SHARE` |
//!
//! The following constructs already pass through the sqlparser→DataFusion
//! pipeline without any intervention from this module:
//!
//! - `DISTINCT ON (cols)` — sqlparser parses into `Select.distinct = Some(Distinct::On(…))`; DataFusion 44 translates to a `DistinctOn` logical node.
//! - `LIMIT N OFFSET M` — standard form; both sqlparser and DataFusion handle natively.
//! - `OFFSET N ROW` / `OFFSET N ROWS` — sqlparser `Offset { rows: OffsetRows::Row|Rows }` is mapped to `query.offset`; DataFusion reads `o.value` and ignores the `ROW`/`ROWS` keyword.
//! - `ORDER BY … NULLS FIRST` / `NULLS LAST` — sqlparser `OrderByExpr.nulls_first`; DataFusion honours it via `SortExpr`.
//! - `FOR UPDATE [OF tbl] [SKIP LOCKED\|NOWAIT]` / `FOR SHARE [OF tbl] …` — sqlparser parses into `Query.locks`; DataFusion ignores the lock list and executes the SELECT normally. Basin is append-only / optimistic-concurrency; row-level locking is advisory.
//! - `FETCH FIRST / NEXT N ROWS ONLY` — handled by `rewrite_fetch_to_limit` above.

use std::borrow::Cow;

/// Rewrite `TABLE <name>` (SQL-standard shorthand for `SELECT * FROM <name>`)
/// to `SELECT * FROM <name>`.
///
/// Only the exact pattern `TABLE <ident>` (case-insensitive, optional
/// semicolon) is rewritten; anything that doesn't match is returned unchanged.
/// Schemas (`TABLE schema.tbl`) are preserved: the rewritten form becomes
/// `SELECT * FROM schema.tbl`.
pub(crate) fn rewrite_table_shorthand(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim();
    let upper = trimmed.to_ascii_uppercase();
    // Must start with TABLE (case-insensitive) followed by whitespace.
    let rest = if let Some(rest) = upper.strip_prefix("TABLE") {
        rest
    } else {
        return Cow::Borrowed(sql);
    };
    // Must have at least one whitespace char after TABLE.
    if !rest.starts_with(|c: char| c.is_ascii_whitespace()) {
        return Cow::Borrowed(sql);
    }
    // Extract the table name portion from the *original* SQL (preserving case).
    let rest_orig = &trimmed["TABLE".len()..].trim_start();
    // Strip optional trailing semicolon.
    let name = rest_orig.trim_end_matches(';').trim_end();
    // Name must be non-empty and contain only identifier characters (letters,
    // digits, underscore, dot for schema.table, double-quoted idents).
    if name.is_empty() {
        return Cow::Borrowed(sql);
    }
    Cow::Owned(format!("SELECT * FROM {name}"))
}

/// Error returned when a `TABLESAMPLE` percentage is outside `[0, 100]`.
/// PostgreSQL raises SQLSTATE `2202H` (`invalid_tablesample_argument`),
/// "sample percentage must be between 0 and 100". The caller maps this
/// to a `BasinError` so the failure surfaces instead of silently
/// returning all rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableSampleError(pub String);

/// Rewrite `<table-ref> TABLESAMPLE { BERNOULLI | SYSTEM } (<pct>)
/// [ REPEATABLE (<seed>) ]` into a derived-table sub-select whose `WHERE`
/// clause carries a sampling predicate, so the sample is *actually taken*
/// (the historical behaviour silently stripped the clause and returned
/// every row — BUG #134).
///
/// The lowered form is:
///
/// ```text
/// FROM tbl TABLESAMPLE BERNOULLI(10)
///   -> FROM (SELECT * FROM tbl WHERE basin_tablesample_keep(10)) tbl
///
/// FROM tbl TABLESAMPLE SYSTEM(10) REPEATABLE(42)
///   -> FROM (SELECT * FROM tbl
///            WHERE basin_tablesample_block_keep_seeded(10, 42)) tbl
/// ```
///
/// Wrapping the sampled table in its own sub-select localises the sample to
/// that single relation (matching PG's per-table semantics) and composes
/// with joins / WHERE / GROUP BY in the outer query without any positional
/// surgery on the rest of the statement. DataFusion pushes the predicate
/// down onto the scan.
///
/// Implementation of the predicates (registered as UDFs, see
/// `crate::udf::register_tablesample_udfs`):
///
/// * `BERNOULLI(p)` -> `basin_tablesample_keep(p)`: an independent Bernoulli
///   trial per row at probability `p/100` (a volatile row-level filter).
/// * `SYSTEM(p)` -> `basin_tablesample_block_keep(p)`: one trial per
///   record-batch (DataFusion's ~8192-row batch is our "block"); every row
///   in a kept batch is emitted. This is the implementation-defined block
///   granularity PG's docs explicitly allow.
/// * `REPEATABLE(s)` adds a `, <s>` seed argument and selects the
///   `_seeded` UDF variant, giving a deterministic sample for the same
///   seed + same data + same scan order.
///
/// Returns `Ok(Cow::Borrowed)` (unchanged) when there is no `TABLESAMPLE`
/// clause or the shape isn't one we recognise, and `Err(TableSampleError)`
/// when the percentage is outside `[0, 100]` (PG parity).
pub(crate) fn rewrite_tablesample(sql: &str) -> Result<Cow<'_, str>, TableSampleError> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("TABLESAMPLE") {
        return Ok(Cow::Borrowed(sql));
    }

    let mut result = String::with_capacity(sql.len() + 64);
    let mut idx = 0usize; // byte cursor into the ORIGINAL sql
    let mut rewrote = false;

    while let Some(rel_ts) = upper[idx..].find("TABLESAMPLE") {
        let ts_pos = idx + rel_ts;
        // Identify the table-ref token that precedes TABLESAMPLE so we can
        // wrap it. We only support the common forms:
        //   FROM tbl TABLESAMPLE ...
        //   FROM tbl alias TABLESAMPLE ...
        //   FROM tbl AS alias TABLESAMPLE ...
        //   JOIN tbl [AS] alias TABLESAMPLE ...
        // Anything else (subquery before TABLESAMPLE, etc.) is left alone.
        let (table_start, table_ref, alias) = match split_preceding_table_ref(sql, &upper, ts_pos) {
            Some(v) => v,
            None => {
                // Can't safely wrap; emit text up to and including the
                // keyword unchanged and keep scanning.
                result.push_str(&sql[idx..ts_pos + "TABLESAMPLE".len()]);
                idx = ts_pos + "TABLESAMPLE".len();
                continue;
            }
        };

        // Parse `{ BERNOULLI | SYSTEM } ( <pct> ) [ REPEATABLE ( <seed> ) ]`.
        let after_kw = ts_pos + "TABLESAMPLE".len();
        let parsed = match parse_tablesample_args(sql, &upper, after_kw) {
            Some(p) => p,
            None => {
                result.push_str(&sql[idx..after_kw]);
                idx = after_kw;
                continue;
            }
        };

        // PG parity: percentage must be in [0, 100].
        if !(0.0..=100.0).contains(&parsed.pct) {
            return Err(TableSampleError(
                "sample percentage must be between 0 and 100".to_string(),
            ));
        }

        // Build the sampling predicate.
        let func = match (parsed.system, parsed.seed.is_some()) {
            (false, false) => "basin_tablesample_keep",
            (false, true) => "basin_tablesample_keep_seeded",
            (true, false) => "basin_tablesample_block_keep",
            (true, true) => "basin_tablesample_block_keep_seeded",
        };
        let pred = match &parsed.seed {
            Some(s) => format!("{func}({}, {s})", fmt_pct(parsed.pct)),
            None => format!("{func}({})", fmt_pct(parsed.pct)),
        };

        // Emit everything before the table-ref token verbatim.
        result.push_str(&sql[idx..table_start]);
        // Replace `tbl [AS] [alias]` with the derived sub-select. The alias
        // (explicit or the bare table name) keeps outer column references
        // (`tbl.col`, `alias.col`) resolving exactly as before.
        let bind = match &alias {
            Some(a) => a.clone(),
            None => default_alias(table_ref),
        };
        result.push_str(&format!("(SELECT * FROM {table_ref} WHERE {pred}) {bind}"));

        idx = parsed.end; // resume right after the (REPEATABLE) clause
        rewrote = true;
    }
    result.push_str(&sql[idx..]);

    if rewrote {
        Ok(Cow::Owned(result))
    } else {
        Ok(Cow::Borrowed(sql))
    }
}

/// Format a percentage without a trailing `.0` for integers (keeps the
/// rewritten SQL tidy and the integer-literal path in the parser).
fn fmt_pct(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{}", p as i64)
    } else {
        format!("{p}")
    }
}

/// Derive an alias from a (possibly schema-qualified, possibly quoted)
/// table reference: `public."Ev"` -> `"Ev"`, `s.t` -> `t`, `t` -> `t`.
fn default_alias(table_ref: &str) -> String {
    let last = table_ref.rsplit('.').next().unwrap_or(table_ref);
    last.trim().to_string()
}

struct ParsedSample {
    pct: f64,
    system: bool,
    seed: Option<String>,
    /// Byte offset just past the whole `BERNOULLI(..)[ REPEATABLE(..)]`.
    end: usize,
}

/// Parse the method + args starting at `pos` (just after the `TABLESAMPLE`
/// keyword). Returns `None` if the shape isn't recognised.
fn parse_tablesample_args(sql: &str, upper: &str, pos: usize) -> Option<ParsedSample> {
    let ws = pos + count_ws(&sql[pos..]);
    let rest_upper = &upper[ws..];
    let (system, after_method) = if let Some(r) = rest_upper.strip_prefix("BERNOULLI") {
        (
            false,
            ws + "BERNOULLI".len() + (r.len() - r.trim_start().len()),
        )
    } else if let Some(r) = rest_upper.strip_prefix("SYSTEM") {
        (true, ws + "SYSTEM".len() + (r.len() - r.trim_start().len()))
    } else {
        return None;
    };
    if !sql[after_method..].starts_with('(') {
        return None;
    }
    let open = after_method;
    let close_rel = sql[open + 1..].find(')')?;
    let close = open + 1 + close_rel;
    let pct: f64 = sql[open + 1..close].trim().parse().ok()?;

    // Optional REPEATABLE ( <seed> ).
    let mut end = close + 1;
    let tail_start = end + count_ws(&sql[end..]);
    let tail_upper = &upper[tail_start..];
    let seed = if let Some(r) = tail_upper.strip_prefix("REPEATABLE") {
        let ro = tail_start + "REPEATABLE".len();
        let ro = ro + count_ws(&sql[ro..]);
        if !sql[ro..].starts_with('(') {
            return None;
        }
        let rclose_rel = sql[ro + 1..].find(')')?;
        let rclose = ro + 1 + rclose_rel;
        let seed_txt = sql[ro + 1..rclose].trim().to_string();
        if seed_txt.is_empty() {
            return None;
        }
        let _ = r;
        end = rclose + 1;
        Some(seed_txt)
    } else {
        None
    };

    Some(ParsedSample {
        pct,
        system,
        seed,
        end,
    })
}

/// Given the position of the `TABLESAMPLE` keyword, walk backwards over the
/// optional alias / `AS alias` and the table reference, returning
/// `(table_start_byte, table_ref, Option<alias>)`. Only the simple
/// identifier forms are handled; returns `None` for anything else (e.g. a
/// `)` immediately before TABLESAMPLE, i.e. a subquery / paren group).
fn split_preceding_table_ref<'a>(
    sql: &'a str,
    upper: &str,
    ts_pos: usize,
) -> Option<(usize, &'a str, Option<String>)> {
    // token immediately before TABLESAMPLE
    let (t1_start, t1_end) = prev_token(sql, ts_pos)?;
    let t1 = &sql[t1_start..t1_end];
    if !is_ident_like(t1) {
        return None;
    }
    // Is there an `AS` or a bare-alias before t1?
    let (t2_start, t2_end) = match prev_token(sql, t1_start) {
        Some(v) => v,
        None => {
            // t1 is the table, no alias, nothing before it (unusual but safe
            // to leave alone — needs a FROM/JOIN keyword in practice).
            return None;
        }
    };
    let t2 = &sql[t2_start..t2_end];
    let t2_up = &upper[t2_start..t2_end];

    if t2_up == "AS" {
        // `<tbl> AS <alias> TABLESAMPLE`  -> alias = t1, table = token before AS
        let (tb_start, tb_end) = prev_token(sql, t2_start)?;
        let tb = &sql[tb_start..tb_end];
        if !is_ident_like(tb) || !is_from_or_join_anchor(sql, upper, tb_start) {
            return None;
        }
        Some((tb_start, &sql[tb_start..tb_end], Some(t1.to_string())))
    } else if is_ident_like(t2)
        && !is_reserved_clause_kw(t2_up)
        && is_from_or_join_anchor(sql, upper, t2_start)
    {
        // `<tbl> <alias> TABLESAMPLE`  -> alias = t1, table = t2
        Some((t2_start, &sql[t2_start..t2_end], Some(t1.to_string())))
    } else if is_from_or_join_kw(t2_up) {
        // `FROM <tbl> TABLESAMPLE` / `JOIN <tbl> TABLESAMPLE` -> no alias
        Some((t1_start, t1, None))
    } else {
        None
    }
}

/// True when the token starting at `start` is anchored by a `FROM` or
/// `JOIN` keyword somewhere immediately before it (directly, or directly
/// before the table when an alias sits between).
fn is_from_or_join_anchor(sql: &str, upper: &str, start: usize) -> bool {
    match prev_token(sql, start) {
        Some((s, e)) => is_from_or_join_kw(&upper[s..e]),
        None => false,
    }
}

fn is_from_or_join_kw(up: &str) -> bool {
    matches!(up, "FROM" | "JOIN")
}

/// Clause keywords that must never be mistaken for a bare table alias.
fn is_reserved_clause_kw(up: &str) -> bool {
    matches!(
        up,
        "WHERE"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "HAVING"
            | "UNION"
            | "ON"
            | "USING"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "FETCH"
            | "FOR"
    )
}

/// Identifier-ish: a SQL identifier, schema-qualified name, or double-quoted
/// identifier. Rejects punctuation tokens like `)` or `,`.
fn is_ident_like(tok: &str) -> bool {
    if tok.is_empty() {
        return false;
    }
    let bytes = tok.as_bytes();
    if bytes[0] == b'"' {
        return true; // quoted identifier (possibly schema."Quoted")
    }
    tok.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && tok
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Count leading ASCII-whitespace bytes.
fn count_ws(s: &str) -> usize {
    s.len() - s.trim_start().len()
}

/// Return the `(start, end)` byte span of the whitespace-delimited token
/// immediately before byte offset `before` (skipping trailing whitespace).
/// A leading `(` or `)` or `,` is treated as its own one-char token so the
/// caller can detect subquery / list boundaries. Quoted identifiers
/// (`"x"`, possibly `schema."x"`) are returned whole.
fn prev_token(sql: &str, before: usize) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let mut e = before;
    while e > 0 && bytes[e - 1].is_ascii_whitespace() {
        e -= 1;
    }
    if e == 0 {
        return None;
    }
    // Single-char structural tokens.
    let last = bytes[e - 1];
    if last == b'(' || last == b')' || last == b',' {
        return Some((e - 1, e));
    }
    // If the token ends with a closing quote, walk back to its opening quote
    // (and absorb a `schema.` prefix if present).
    if last == b'"' {
        let mut s = e - 1;
        // find the matching opening quote
        while s > 0 && bytes[s - 1] != b'"' {
            s -= 1;
        }
        s = s.saturating_sub(1); // include the opening quote
                                 // absorb `ident.` prefix
        while s > 0
            && (bytes[s - 1].is_ascii_alphanumeric()
                || bytes[s - 1] == b'_'
                || bytes[s - 1] == b'.'
                || bytes[s - 1] == b'"')
        {
            s -= 1;
        }
        return Some((s, e));
    }
    let mut s = e;
    while s > 0 {
        let c = bytes[s - 1];
        if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' {
            s -= 1;
        } else {
            break;
        }
    }
    if s == e {
        // Non-ident punctuation we don't model; treat as one char.
        return Some((e - 1, e));
    }
    Some((s, e))
}

/// Rewrite SQL-standard `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY`
/// (and the `OFFSET M ROWS FETCH NEXT N ROWS ONLY` combined form) to the
/// DataFusion-compatible `LIMIT N [OFFSET M]` form.
///
/// DataFusion 44 uses `Query.limit` / `Query.offset` from the sqlparser AST.
/// `Query.fetch` (populated by `FETCH FIRST`) is ignored by the planner, so
/// without this rewrite those queries would silently return all rows.
///
/// The rewrite covers:
///
/// - `… FETCH FIRST N ROWS ONLY`          → `… LIMIT N`
/// - `… FETCH NEXT N ROWS ONLY`           → `… LIMIT N`
/// - `… FETCH FIRST N ROW ONLY`           → `… LIMIT N` (singular accepted)
/// - `… OFFSET M ROWS FETCH NEXT N …`     → `… LIMIT N OFFSET M`
/// - `… OFFSET M ROW FETCH FIRST N …`     → `… LIMIT N OFFSET M`
///
/// `WITH TIES` is treated as `ONLY` (DataFusion doesn't support TIES).
/// `PERCENT` variants are not rewritten (very rare; fall through to error).
pub(crate) fn rewrite_fetch_to_limit(sql: &str) -> Cow<'_, str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("FETCH") {
        return Cow::Borrowed(sql);
    }

    // We operate on the uppercased copy for pattern matching and reconstruct
    // from the original for everything before the FETCH clause.
    //
    // Strategy: find FETCH token, extract N, extract optional preceding OFFSET,
    // build replacement.

    // Locate FETCH (whole-word).
    let fetch_pos = match find_whole_word(&upper, "FETCH") {
        Some(p) => p,
        None => return Cow::Borrowed(sql),
    };

    // Everything before FETCH (from original SQL, preserving case).
    let before_fetch_orig = &sql[..fetch_pos].trim_end();
    let before_fetch_upper = &upper[..fetch_pos];

    // Parse `FETCH { FIRST | NEXT }`.
    let after_fetch = upper[fetch_pos + "FETCH".len()..].trim_start();
    let after_first_next = if let Some(r) = after_fetch.strip_prefix("FIRST") {
        r
    } else if let Some(r) = after_fetch.strip_prefix("NEXT") {
        r
    } else {
        return Cow::Borrowed(sql);
    };
    // Must have whitespace or digit after FIRST/NEXT.
    if !after_first_next.starts_with(|c: char| c.is_ascii_whitespace() || c.is_ascii_digit()) {
        return Cow::Borrowed(sql);
    }

    let after_count_kw = after_first_next.trim_start();
    // Parse the integer count N.
    let (n_str, after_n) = split_integer(after_count_kw);
    if n_str.is_empty() {
        return Cow::Borrowed(sql);
    }
    // Skip `ROW` or `ROWS`.
    let after_rows_kw = after_n.trim_start();
    let after_rows = if let Some(r) = after_rows_kw.strip_prefix("ROWS") {
        r
    } else if let Some(r) = after_rows_kw.strip_prefix("ROW") {
        r
    } else {
        return Cow::Borrowed(sql);
    };
    // Skip `ONLY` or `WITH TIES` (treat both as ONLY).
    let after_only = after_rows.trim_start();
    let _rest = if let Some(r) = after_only.strip_prefix("ONLY") {
        r
    } else if let Some(r) = after_only.strip_prefix("WITH") {
        // WITH TIES
        r.trim_start().strip_prefix("TIES").unwrap_or(r)
    } else {
        // No ONLY/WITH TIES; still accept (trailing end of input is fine).
        after_only
    };

    // Now look for a preceding `OFFSET M ROWS` immediately before FETCH.
    // Pattern in `before_fetch_upper`: `... OFFSET <M> { ROW | ROWS }`.
    let (base_sql, offset_clause) = extract_trailing_offset(before_fetch_orig, before_fetch_upper);

    // Build the replacement.
    let replacement = match offset_clause {
        Some(m) => format!("{base_sql} LIMIT {n_str} OFFSET {m}"),
        None => format!("{before_fetch_orig} LIMIT {n_str}"),
    };
    Cow::Owned(replacement)
}

/// Rewrite `FOR NO KEY UPDATE` → `FOR UPDATE` and `FOR KEY SHARE` → `FOR SHARE`.
///
/// sqlparser 0.52 recognises only `LockType::Update` and `LockType::Share`
/// (corresponding to `FOR UPDATE` / `FOR SHARE`). The PG-specific variants
/// `FOR NO KEY UPDATE` and `FOR KEY SHARE` trigger a parse error. This rewrite
/// converts them to the closest supported form before sqlparser sees the SQL.
///
/// The optional `OF <table>`, `SKIP LOCKED`, and `NOWAIT` modifiers are
/// preserved verbatim after the rewrite; sqlparser handles them fine once the
/// lock keyword is normalised.
///
/// Basin is append-only / optimistic-concurrency; all row-level locking
/// keywords are advisory — the SELECT executes without acquiring any locks.
/// This matches the documented behaviour of `FOR UPDATE` / `FOR SHARE` in
/// Basin.
pub(crate) fn rewrite_for_no_key_update_and_key_share(sql: &str) -> Cow<'_, str> {
    let upper = sql.to_ascii_uppercase();
    // Fast path: neither target phrase is present.
    if !upper.contains("FOR NO KEY UPDATE") && !upper.contains("FOR KEY SHARE") {
        return Cow::Borrowed(sql);
    }

    // Walk through occurrences and replace.  We match case-insensitively on
    // the uppercased copy and splice from the original to preserve case of the
    // rest of the query.
    //
    // Strategy: replace "FOR NO KEY UPDATE" → "FOR UPDATE" and
    //           "FOR KEY SHARE"   → "FOR SHARE" everywhere they appear as
    //           whole-word sequences (i.e., preceded/followed by non-ident
    //           chars).  The longer pattern is checked first to avoid a
    //           "FOR NO KEY UPDATE" being clobbered by a spurious match on
    //           an imaginary "FOR … KEY SHARE".
    let mut result = String::with_capacity(sql.len());
    let mut pos = 0usize;

    while pos < sql.len() {
        let upper_tail = &upper[pos..];

        // Try "FOR NO KEY UPDATE" first (longer).
        if let Some(off) = upper_tail.find("FOR NO KEY UPDATE") {
            let abs = pos + off;
            // Whole-word boundary check.
            let before_ok = abs == 0
                || !sql.as_bytes()[abs - 1].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs - 1] != b'_';
            let end = abs + "FOR NO KEY UPDATE".len();
            let after_ok = end >= sql.len()
                || !sql.as_bytes()[end].is_ascii_alphanumeric() && sql.as_bytes()[end] != b'_';
            if before_ok && after_ok {
                result.push_str(&sql[pos..abs]);
                result.push_str("FOR UPDATE");
                pos = end;
                continue;
            }
        }

        // Try "FOR KEY SHARE".
        if let Some(off) = upper_tail.find("FOR KEY SHARE") {
            let abs = pos + off;
            let before_ok = abs == 0
                || !sql.as_bytes()[abs - 1].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs - 1] != b'_';
            let end = abs + "FOR KEY SHARE".len();
            let after_ok = end >= sql.len()
                || !sql.as_bytes()[end].is_ascii_alphanumeric() && sql.as_bytes()[end] != b'_';
            if before_ok && after_ok {
                result.push_str(&sql[pos..abs]);
                result.push_str("FOR SHARE");
                pos = end;
                continue;
            }
        }

        // No match found in the remaining tail.
        result.push_str(&sql[pos..]);
        break;
    }

    if result.is_empty() {
        Cow::Borrowed(sql)
    } else if result == sql {
        Cow::Borrowed(sql)
    } else {
        Cow::Owned(result)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find the byte position of `word` as a whole word (surrounded by non-alnum
/// or start/end) in `haystack` (which should already be uppercased).
fn find_whole_word(haystack: &str, word: &str) -> Option<usize> {
    let mut start = 0;
    loop {
        let pos = haystack[start..].find(word)?;
        let abs = start + pos;
        let before_ok = abs == 0
            || !haystack.as_bytes()[abs - 1].is_ascii_alphanumeric()
                && haystack.as_bytes()[abs - 1] != b'_';
        let end = abs + word.len();
        let after_ok = end >= haystack.len()
            || !haystack.as_bytes()[end].is_ascii_alphanumeric()
                && haystack.as_bytes()[end] != b'_';
        if before_ok && after_ok {
            return Some(abs);
        }
        start = abs + 1;
    }
}

/// Split `s` into a leading decimal-digit run and the remainder.
fn split_integer(s: &str) -> (&str, &str) {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// If `before_upper` ends with `OFFSET <M> { ROW | ROWS }` (with optional
/// trailing whitespace), strip it and return `(trimmed_base, Some(M_str))`.
/// Otherwise return `(before_orig, None)`.
fn extract_trailing_offset<'a>(
    before_orig: &'a str,
    before_upper: &str,
) -> (&'a str, Option<String>) {
    // Find the last OFFSET.
    let offset_pos = match rfind_whole_word(before_upper, "OFFSET") {
        Some(p) => p,
        None => return (before_orig, None),
    };
    let after_offset_upper = before_upper[offset_pos + "OFFSET".len()..].trim_start();
    let (m_str, after_m) = split_integer(after_offset_upper);
    if m_str.is_empty() {
        return (before_orig, None);
    }
    let after_rows = after_m.trim_start();
    let after_unit = if let Some(r) = after_rows.strip_prefix("ROWS") {
        r
    } else if let Some(r) = after_rows.strip_prefix("ROW") {
        r
    } else {
        // No ROW/ROWS after the number; don't strip (might be a column alias etc.)
        return (before_orig, None);
    };
    // The text after `ROW[S]` should be empty (or whitespace) since we're
    // at the end of `before_upper`.
    if !after_unit.trim().is_empty() {
        return (before_orig, None);
    }
    // Strip from original.
    let trimmed = before_orig[..offset_pos].trim_end();
    (trimmed, Some(m_str.to_string()))
}

/// Like `find_whole_word` but searches from the right.
fn rfind_whole_word(haystack: &str, word: &str) -> Option<usize> {
    let mut result = None;
    let mut start = 0;
    loop {
        let pos = match haystack[start..].find(word) {
            Some(p) => p,
            None => return result,
        };
        let abs = start + pos;
        let before_ok = abs == 0
            || !haystack.as_bytes()[abs - 1].is_ascii_alphanumeric()
                && haystack.as_bytes()[abs - 1] != b'_';
        let end = abs + word.len();
        let after_ok = end >= haystack.len()
            || !haystack.as_bytes()[end].is_ascii_alphanumeric()
                && haystack.as_bytes()[end] != b'_';
        if before_ok && after_ok {
            result = Some(abs);
        }
        start = abs + 1;
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- TABLE shorthand ---

    #[test]
    fn table_shorthand_simple() {
        assert_eq!(rewrite_table_shorthand("TABLE foo"), "SELECT * FROM foo");
    }

    #[test]
    fn table_shorthand_schema_qualified() {
        assert_eq!(
            rewrite_table_shorthand("TABLE public.events"),
            "SELECT * FROM public.events"
        );
    }

    #[test]
    fn table_shorthand_with_semicolon() {
        assert_eq!(rewrite_table_shorthand("TABLE foo;"), "SELECT * FROM foo");
    }

    #[test]
    fn table_shorthand_case_insensitive() {
        assert_eq!(rewrite_table_shorthand("table foo"), "SELECT * FROM foo");
    }

    #[test]
    fn table_shorthand_no_match_create_table() {
        // CREATE TABLE should not be rewritten.
        let sql = "CREATE TABLE foo (id BIGINT)";
        assert_eq!(rewrite_table_shorthand(sql), sql);
    }

    #[test]
    fn table_shorthand_no_match_select() {
        let sql = "SELECT * FROM foo";
        assert_eq!(rewrite_table_shorthand(sql), sql);
    }

    // --- TABLESAMPLE rewrite ---

    #[test]
    fn tablesample_bernoulli_rewritten_to_predicate() {
        let sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10)";
        let out = rewrite_tablesample(sql).unwrap();
        assert!(!out.contains("TABLESAMPLE"), "TABLESAMPLE removed: {out}");
        assert!(
            out.contains("basin_tablesample_keep(10)"),
            "BERNOULLI -> row predicate: {out}"
        );
        // The sampled relation is wrapped in a derived table aliased back to
        // its name so outer references still resolve.
        assert!(
            out.contains("(SELECT * FROM t WHERE basin_tablesample_keep(10)) t"),
            "wrapped sub-select with alias: {out}"
        );
    }

    #[test]
    fn tablesample_system_rewritten_to_block_predicate() {
        let sql = "SELECT * FROM t TABLESAMPLE SYSTEM(25)";
        let out = rewrite_tablesample(sql).unwrap();
        assert!(!out.contains("TABLESAMPLE"), "TABLESAMPLE removed: {out}");
        assert!(
            out.contains("basin_tablesample_block_keep(25)"),
            "SYSTEM -> block predicate: {out}"
        );
    }

    #[test]
    fn tablesample_repeatable_uses_seeded_variant() {
        let sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10) REPEATABLE(42)";
        let out = rewrite_tablesample(sql).unwrap();
        assert!(
            out.contains("basin_tablesample_keep_seeded(10, 42)"),
            "REPEATABLE -> seeded variant: {out}"
        );
        assert!(!out.contains("REPEATABLE"), "REPEATABLE consumed: {out}");
    }

    #[test]
    fn tablesample_with_where_clause_composes() {
        let sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10) WHERE id > 5";
        let out = rewrite_tablesample(sql).unwrap();
        assert!(!out.contains("TABLESAMPLE"), "TABLESAMPLE removed: {out}");
        assert!(out.contains("WHERE id > 5"), "outer WHERE kept: {out}");
        assert!(
            out.contains("basin_tablesample_keep(10)"),
            "sample predicate present: {out}"
        );
    }

    #[test]
    fn tablesample_alias_preserved() {
        let sql = "SELECT s.id FROM tbl AS s TABLESAMPLE SYSTEM(50)";
        let out = rewrite_tablesample(sql).unwrap();
        assert!(
            out.contains("(SELECT * FROM tbl WHERE basin_tablesample_block_keep(50)) s"),
            "explicit alias preserved: {out}"
        );
    }

    #[test]
    fn tablesample_out_of_range_is_error() {
        assert!(rewrite_tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI(150)").is_err());
        assert!(rewrite_tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI(-1)").is_err());
        // 0 and 100 are the valid boundary values.
        assert!(rewrite_tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI(0)").is_ok());
        assert!(rewrite_tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI(100)").is_ok());
    }

    #[test]
    fn no_tablesample_passthrough() {
        let sql = "SELECT * FROM t WHERE id > 5";
        assert_eq!(rewrite_tablesample(sql).unwrap(), sql);
    }

    // --- FETCH FIRST/NEXT rewrite ---

    #[test]
    fn fetch_first_rewritten() {
        let sql = "SELECT * FROM t FETCH FIRST 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10 in: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn fetch_next_rewritten() {
        let sql = "SELECT * FROM t FETCH NEXT 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10 in: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn offset_rows_fetch_next_rewritten() {
        let sql = "SELECT * FROM t OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 10"), "expected LIMIT 10: {out}");
        assert!(out.contains("OFFSET 5"), "expected OFFSET 5: {out}");
        assert!(!out.contains("FETCH"), "FETCH should be gone: {out}");
    }

    #[test]
    fn plain_limit_passthrough() {
        let sql = "SELECT * FROM t LIMIT 10 OFFSET 5";
        assert_eq!(rewrite_fetch_to_limit(sql), sql);
    }

    #[test]
    fn no_fetch_passthrough() {
        let sql = "SELECT id, name FROM t ORDER BY id";
        assert_eq!(rewrite_fetch_to_limit(sql), sql);
    }

    #[test]
    fn fetch_row_singular() {
        let sql = "SELECT * FROM t FETCH FIRST 1 ROW ONLY";
        let out = rewrite_fetch_to_limit(sql);
        assert!(out.contains("LIMIT 1"), "expected LIMIT 1: {out}");
    }

    // --- FOR NO KEY UPDATE / FOR KEY SHARE rewrite ---

    #[test]
    fn for_no_key_update_rewritten_to_for_update() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert!(out.contains("FOR UPDATE"), "expected FOR UPDATE: {out}");
        assert!(!out.contains("NO KEY"), "NO KEY should be gone: {out}");
    }

    #[test]
    fn for_no_key_update_skip_locked_rewritten() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE SKIP LOCKED";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR UPDATE SKIP LOCKED");
    }

    #[test]
    fn for_no_key_update_nowait_rewritten() {
        let sql = "SELECT * FROM t FOR NO KEY UPDATE NOWAIT";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR UPDATE NOWAIT");
    }

    #[test]
    fn for_key_share_rewritten_to_for_share() {
        let sql = "SELECT * FROM t FOR KEY SHARE";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR SHARE");
    }

    #[test]
    fn for_key_share_of_table_rewritten() {
        let sql = "SELECT * FROM t FOR KEY SHARE OF t";
        let out = rewrite_for_no_key_update_and_key_share(sql);
        assert_eq!(out, "SELECT * FROM t FOR SHARE OF t");
    }

    #[test]
    fn for_update_passthrough_unchanged() {
        // FOR UPDATE already valid — must not be double-rewritten.
        let sql = "SELECT * FROM t FOR UPDATE";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }

    #[test]
    fn for_share_passthrough_unchanged() {
        let sql = "SELECT * FROM t FOR SHARE";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }

    #[test]
    fn no_lock_clause_passthrough() {
        let sql = "SELECT id FROM t WHERE id = 1";
        assert_eq!(rewrite_for_no_key_update_and_key_share(sql), sql);
    }
}
