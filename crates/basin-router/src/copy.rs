//! `COPY FROM STDIN` and `COPY TO STDOUT` for the simple-query path.
//!
//! v0.3: CSV format (delimiter/null/quote/escape configurable via WITH
//! options) plus the PG binary COPY format (`WITH (FORMAT BINARY)`, STDIN /
//! STDOUT only). Column-list (`COPY t (a, b) ...`) and double-quoted
//! identifiers (`COPY "users" (id, email) FROM STDIN` — the sqlx `PgCopyIn`
//! shape) are supported. Server-side file-path
//! variants (`COPY t FROM '/abs/path'` / `COPY t TO '/abs/path'`) are gated
//! by two env vars: `BASIN_COPY_ALLOW_FILE_PATHS=1` (primary on/off gate) and
//! `BASIN_COPY_PATH_ALLOWLIST` (colon-separated directory allowlist, required
//! when file paths are enabled). Query-source COPY (`COPY (SELECT …) TO
//! STDOUT`) is supported. `COPY … FROM PROGRAM '…'` is rejected with SQLSTATE
//! 42501. BINARY format with CSV-only options (HEADER / DELIMITER / NULL /
//! QUOTE / ESCAPE) or a file path is rejected with `42601`; BINARY over a
//! column type without a binary codec is rejected with `0A000`.
//!
//! Architecture:
//!
//! - `parse_copy` runs a small hand-rolled scanner against the simple-query
//!   SQL string. On match it returns a [`CopyCommand`]; everything else
//!   returns `Ok(None)` so the caller falls through to normal query routing.
//! - For `COPY FROM STDIN` the simple-query handler sends `CopyInResponse`
//!   and stashes a [`CopyInState`] on the per-connection slot. The pgwire
//!   framework then drives `CopyHandler::on_copy_data` / `on_copy_done`,
//!   which read+drain that state.
//! - For `COPY TO STDOUT` the simple-query handler runs the underlying
//!   `SELECT * FROM <table>` synchronously and emits the full message
//!   sequence (`CopyOutResponse` → `CopyData`* → `CopyDone` →
//!   `CommandComplete` → `ReadyForQuery`) inline; no per-connection state
//!   is needed because the server drives every byte.
//!
//! ### Protocol-state-machine guarantee
//!
//! On any error mid-stream during `COPY FROM`, we *do not* short-circuit
//! the protocol: we keep accepting `CopyData` chunks (silently discarding
//! them) until `CopyDone` arrives, then emit a single `ErrorResponse` +
//! `ReadyForQuery`. Sending `ErrorResponse` mid-`CopyIn` would desync the
//! framework (it's still in `CopyInProgress`) and break the connection
//! for the rest of its life.
//!
//! ### SQL escaping
//!
//! We mirror `basin_rest::parser::quote_sql_string`: wrap text in `'…'`,
//! double internal `'`. Inputs containing `\0` are rejected (Postgres TEXT
//! columns disallow nul anyway, and embedding it in SQL is a parser
//! footgun). All other escaping is the engine's job at INSERT time.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use basin_engine::{ExecResult, ScalarParam};
use bytes::{BufMut, Bytes, BytesMut};
use pgwire::error::ErrorInfo;
use pgwire::messages::copy::{CopyData, CopyInResponse, CopyOutResponse};
use pgwire::messages::response::{CommandComplete, ReadyForQuery, TransactionStatus};
use pgwire::messages::PgWireBackendMessage;

use crate::protocol::Session;

/// Wire format of a COPY statement: CSV (the default) or the PG binary
/// COPY format (`WITH (FORMAT BINARY)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CopyFormat {
    #[default]
    Csv,
    Binary,
}

/// Format options carried with a COPY statement.
///
/// CSV defaults match Postgres CSV mode: comma delimiter, empty-string NULL,
/// double-quote character for quoting, and same char for escaping. The CSV
/// knobs are meaningless (and rejected at parse time) when `format` is
/// `Binary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyOptions {
    /// Wire format (default: CSV).
    pub(crate) format: CopyFormat,
    /// Field delimiter (default: `','`).
    pub(crate) delimiter: char,
    /// String that represents NULL in the CSV input/output (default: `""`).
    pub(crate) null_string: String,
    /// Quote character (default: `'"'`).
    pub(crate) quote: char,
    /// Escape character inside quoted fields (default: same as `quote`).
    pub(crate) escape: char,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Csv,
            delimiter: ',',
            null_string: String::new(),
            quote: '"',
            escape: '"',
        }
    }
}

/// A parsed `COPY` statement. The simple-query path branches on this before
/// running anything against the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyCommand {
    From {
        table: String,
        with_header: bool,
        /// Optional column list as written by the user (`COPY t (b, a) FROM ...`).
        /// `None` = use every column in schema order.
        columns: Option<Vec<String>>,
        /// `None` = STDIN (client streams CopyData); `Some(path)` = server-side
        /// read of an absolute filesystem path.
        path: Option<String>,
        /// CSV format options (delimiter, null, quote, escape).
        opts: CopyOptions,
    },
    To {
        table: String,
        with_header: bool,
        columns: Option<Vec<String>>,
        path: Option<String>,
        /// CSV format options.
        opts: CopyOptions,
    },
    /// `COPY (SELECT …) TO STDOUT [WITH (FORMAT CSV [, ...])]`
    QueryTo {
        /// The inner SELECT SQL (as written by the user, between the parens).
        query: String,
        with_header: bool,
        /// CSV format options.
        opts: CopyOptions,
    },
}

/// Per-connection state for an in-flight `COPY FROM STDIN`.
///
/// Holds the partial CSV buffer (since `CopyData` chunks split rows
/// arbitrarily), the resolved column metadata, the running row count, and
/// any sticky error captured mid-stream.
pub(crate) struct CopyInState {
    pub(crate) table: String,
    /// Full table schema (all columns, in declaration order).  Used by the
    /// fast-path `ingest_csv_batch` call to build the Arrow RecordBatch
    /// without going through SQL parse.
    pub(crate) full_schema: std::sync::Arc<arrow_schema::Schema>,
    /// Field shape we use for column-count validation and (fallback) INSERT
    /// VALUES rendering.  When a column list was supplied, this carries only
    /// those listed columns (in list order).  When no column list was
    /// supplied, this is identical to `full_schema`'s field list.
    pub(crate) columns: Vec<Field>,
    /// User-supplied column list, if any. Populated only when the COPY
    /// statement included `(col1, col2, ...)`.
    pub(crate) column_list: Option<Vec<String>>,
    /// Bytes received but not yet split into a complete row.
    pub(crate) buffer: Vec<u8>,
    /// Successful INSERTs since the COPY started.
    pub(crate) row_count: u64,
    /// First error observed; if present, every subsequent CopyData chunk is
    /// drained without further engine work.
    pub(crate) error: Option<String>,
    /// `WITH (HEADER true)` — skip the first row.
    pub(crate) header_pending: bool,
    /// CSV format options (delimiter, null, quote, escape).
    pub(crate) opts: CopyOptions,
    /// Pending rows accumulated for a batch `ingest_csv_batch` call.
    /// Rows are held here until `INGEST_BATCH_SIZE` is reached or the
    /// COPY stream ends (`final_chunk = true`).
    pub(crate) pending_rows: Vec<Vec<Option<String>>>,
    /// BINARY format only: the 19+-byte `PGCOPY` header has been consumed.
    pub(crate) binary_header_parsed: bool,
    /// BINARY format only: the `0xFFFF` file trailer has been consumed; any
    /// further non-empty CopyData is a protocol error.
    pub(crate) binary_done: bool,
}

/// Number of parsed CSV rows to accumulate before issuing one
/// `ingest_csv_batch` call.  Large enough to amortise the per-batch overhead
/// (Arrow builder setup, shard.write_batch WAL append) but small enough to
/// keep peak memory reasonable for wide tables.  10 000 matches the bench
/// INSERT batch size.
const INGEST_BATCH_SIZE: usize = 10_000;

impl CopyInState {
    pub(crate) fn new(
        table: String,
        full_schema: std::sync::Arc<arrow_schema::Schema>,
        columns: Vec<Field>,
        with_header: bool,
        opts: CopyOptions,
    ) -> Self {
        Self {
            table,
            full_schema,
            columns,
            column_list: None,
            buffer: Vec::new(),
            row_count: 0,
            error: None,
            header_pending: with_header,
            opts,
            pending_rows: Vec::new(),
            binary_header_parsed: false,
            binary_done: false,
        }
    }

    pub(crate) fn with_column_list(mut self, list: Vec<String>) -> Self {
        self.column_list = Some(list);
        self
    }
}

/// Result of attempting to parse one of the `COPY` shapes we support.
///
/// `Ok(Some(_))` = matched a supported shape; `Ok(None)` = SQL doesn't start
/// with `COPY`, fall through to normal query handling; `Err(_)` = SQL starts
/// with `COPY` but doesn't match our grammar — caller surfaces an
/// ErrorResponse.
///
/// Supported shapes:
/// - `COPY t [(cols)] FROM STDIN [WITH (...)]`
/// - `COPY t [(cols)] FROM '/abs/path' [WITH (...)]`
/// - `COPY t [(cols)] TO STDOUT [WITH (...)]`
/// - `COPY t [(cols)] TO '/abs/path' [WITH (...)]`
/// - `COPY (SELECT …) TO STDOUT [WITH (...)]`
///
/// Explicitly rejected:
/// - `COPY … FROM PROGRAM '…'` → SQLSTATE 42501
pub(crate) fn parse_copy(sql: &str) -> std::result::Result<Option<CopyCommand>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut sc = Scanner::new(trimmed);
    if !sc.eat_keyword("COPY") {
        return Ok(None);
    }
    sc.skip_whitespace();

    // Detect query-source: `COPY (SELECT …) TO STDOUT`
    if sc.peek_punct('(') {
        let query = parse_subquery(&mut sc)?;
        if !sc.eat_keyword("TO") {
            return Err("expected TO after COPY (SELECT …)".into());
        }
        if !sc.eat_keyword("STDOUT") {
            return Err("COPY (query) TO only supports STDOUT in this release".into());
        }
        let (with_header, opts) = parse_with_options(&mut sc)?;
        sc.skip_whitespace();
        if !sc.is_done() {
            return Err(format!(
                "unexpected trailing input after COPY: {:?}",
                sc.rest()
            ));
        }
        return Ok(Some(CopyCommand::QueryTo {
            query,
            with_header,
            opts,
        }));
    }

    let table = sc
        .eat_ident()
        .ok_or_else(|| "expected table name or '(' after COPY".to_owned())?;
    let columns = if sc.peek_punct('(') {
        Some(parse_column_list(&mut sc)?)
    } else {
        None
    };
    let direction = if sc.eat_keyword("FROM") {
        Direction::From
    } else if sc.eat_keyword("TO") {
        Direction::To
    } else {
        return Err("expected FROM or TO after COPY <table>".into());
    };
    let path = match direction {
        Direction::From => {
            if sc.eat_keyword("STDIN") {
                None
            } else if sc.eat_keyword("PROGRAM") {
                // Explicit rejection with SQLSTATE 42501 (insufficient_privilege).
                // We encode the error in a special prefix so the caller can
                // surface 42501 instead of 42601.
                return Err(
                    "\x0042501\x00COPY FROM PROGRAM is not supported (security: server-side program execution is disabled)"
                        .into(),
                );
            } else if sc.peek_punct('\'') {
                Some(parse_string_literal(&mut sc)?)
            } else {
                return Err("expected STDIN or '<absolute-path>' after COPY <table> FROM".into());
            }
        }
        Direction::To => {
            if sc.eat_keyword("STDOUT") {
                None
            } else if sc.eat_keyword("PROGRAM") {
                return Err(
                    "\x0042501\x00COPY TO PROGRAM is not supported (security: server-side program execution is disabled)"
                        .into(),
                );
            } else if sc.peek_punct('\'') {
                Some(parse_string_literal(&mut sc)?)
            } else {
                return Err("expected STDOUT or '<absolute-path>' after COPY <table> TO".into());
            }
        }
    };
    let (with_header, opts) = parse_with_options(&mut sc)?;
    sc.skip_whitespace();
    if !sc.is_done() {
        return Err(format!(
            "unexpected trailing input after COPY: {:?}",
            sc.rest()
        ));
    }
    if opts.format == CopyFormat::Binary && path.is_some() {
        return Err(
            "COPY WITH (FORMAT BINARY) is only supported for STDIN/STDOUT; file-path COPY is CSV-only"
                .into(),
        );
    }
    Ok(Some(match direction {
        Direction::From => CopyCommand::From {
            table,
            with_header,
            columns,
            path,
            opts,
        },
        Direction::To => CopyCommand::To {
            table,
            with_header,
            columns,
            path,
            opts,
        },
    }))
}

/// Parse a parenthesised subquery for `COPY (SELECT …) TO STDOUT`.
/// The caller has peeked `(` but not consumed it. We count nested parens to
/// find the matching `)` and return everything inside (trimmed).
fn parse_subquery(sc: &mut Scanner<'_>) -> std::result::Result<String, String> {
    sc.skip_whitespace();
    if !sc.eat_punct('(') {
        return Err("expected '(' to start COPY subquery".into());
    }
    let bytes = sc.s.as_bytes();
    let start = sc.pos;
    let mut depth = 1i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    while sc.pos < bytes.len() {
        let b = bytes[sc.pos];
        if in_single_quote {
            if b == b'\'' {
                // Check for doubled quote
                if sc.pos + 1 < bytes.len() && bytes[sc.pos + 1] == b'\'' {
                    sc.pos += 2;
                    continue;
                }
                in_single_quote = false;
            }
            sc.pos += 1;
            continue;
        }
        if in_double_quote {
            if b == b'"' {
                in_double_quote = false;
            }
            sc.pos += 1;
            continue;
        }
        match b {
            b'\'' => {
                in_single_quote = true;
                sc.pos += 1;
            }
            b'"' => {
                in_double_quote = true;
                sc.pos += 1;
            }
            b'(' => {
                depth += 1;
                sc.pos += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let inner = sc.s[start..sc.pos].trim().to_owned();
                    sc.pos += 1; // consume the closing ')'
                    return Ok(inner);
                }
                sc.pos += 1;
            }
            _ => {
                sc.pos += 1;
            }
        }
    }
    Err("unterminated subquery in COPY (SELECT …)".into())
}

/// Parse `(ident [, ident ...])`. Caller has peeked `(` but not consumed it.
fn parse_column_list(sc: &mut Scanner<'_>) -> std::result::Result<Vec<String>, String> {
    if !sc.eat_punct('(') {
        return Err("expected '(' to start column list".into());
    }
    let mut out = Vec::new();
    loop {
        sc.skip_whitespace();
        if sc.eat_punct(')') {
            break;
        }
        let name = sc
            .eat_ident()
            .ok_or_else(|| "expected column name in COPY column list".to_owned())?;
        out.push(name);
        sc.skip_whitespace();
        if sc.eat_punct(')') {
            break;
        }
        if !sc.eat_punct(',') {
            return Err("expected ',' or ')' in COPY column list".into());
        }
    }
    if out.is_empty() {
        return Err("COPY column list must not be empty".into());
    }
    Ok(out)
}

/// Parse a single-quoted SQL string literal (`'...'` with `''` escaping).
/// Caller has peeked `'` but not consumed it.
fn parse_string_literal(sc: &mut Scanner<'_>) -> std::result::Result<String, String> {
    sc.skip_whitespace();
    if !sc.eat_punct('\'') {
        return Err("expected '\\''".into());
    }
    let bytes = sc.s.as_bytes();
    let mut out = String::new();
    while sc.pos < bytes.len() {
        let b = bytes[sc.pos];
        if b == b'\'' {
            // Doubled quote = literal apostrophe; bare quote ends the literal.
            if sc.pos + 1 < bytes.len() && bytes[sc.pos + 1] == b'\'' {
                out.push('\'');
                sc.pos += 2;
                continue;
            }
            sc.pos += 1;
            return Ok(out);
        }
        out.push(b as char);
        sc.pos += 1;
    }
    Err("unterminated string literal".into())
}

enum Direction {
    From,
    To,
}

/// Parse the optional `WITH (FORMAT {CSV|BINARY} [, HEADER {true|false}
/// [, DELIMITER 'c'] [, NULL 's'] [, QUOTE 'c'] [, ESCAPE 'c']])`.
/// FORCE_*, ENCODING, and FORMAT TEXT are rejected. Returns
/// `(header, CopyOptions)`.
///
/// Also accepts:
/// - the modern parenthesised option list without the WITH keyword
///   (`COPY t FROM STDIN (FORMAT CSV)`) that PG ≥ 9.0 documents;
/// - the legacy `WITH CSV [HEADER]` / `WITH BINARY` shorthands (no parens)
///   that older Postgres clients still emit.
///
/// The CSV-only options (HEADER / DELIMITER / NULL / QUOTE / ESCAPE) are
/// rejected when combined with `FORMAT BINARY`, mirroring Postgres's
/// "cannot specify X in BINARY mode" errors. Option order is free-form, so
/// the conflict check runs after the whole list is parsed.
fn parse_with_options(sc: &mut Scanner<'_>) -> std::result::Result<(bool, CopyOptions), String> {
    sc.skip_whitespace();
    let has_with = sc.eat_keyword("WITH");
    sc.skip_whitespace();
    if !sc.peek_punct('(') {
        if !has_with {
            // Plain `COPY t FROM STDIN` with no options — accept, treat as CSV.
            return Ok((false, CopyOptions::default()));
        }
        // Legacy `WITH CSV [HEADER]` / `WITH BINARY` forms.
        if sc.eat_keyword("CSV") {
            let mut header = false;
            sc.skip_whitespace();
            if sc.eat_keyword("HEADER") {
                header = true;
            }
            return Ok((header, CopyOptions::default()));
        }
        if sc.eat_keyword("BINARY") {
            return Ok((
                false,
                CopyOptions {
                    format: CopyFormat::Binary,
                    ..CopyOptions::default()
                },
            ));
        }
        return Err(
            "expected '(' after WITH, or legacy 'WITH CSV [HEADER]' / 'WITH BINARY' shorthand"
                .into(),
        );
    }
    sc.eat_punct('(');
    let mut header = false;
    let mut opts = CopyOptions::default();
    // Track which CSV-only options were written out so the BINARY conflict
    // check below can name the offending option (FORMAT may legally appear
    // after the option it conflicts with).
    let mut csv_only_seen: Option<&'static str> = None;
    loop {
        sc.skip_whitespace();
        let key = sc
            .eat_ident()
            .ok_or_else(|| "expected option name inside WITH (...)".to_owned())?;
        let key_upper = key.to_ascii_uppercase();
        match key_upper.as_str() {
            "FORMAT" => {
                sc.skip_whitespace();
                let v = sc
                    .eat_ident()
                    .ok_or_else(|| "expected format value after FORMAT".to_owned())?;
                if v.eq_ignore_ascii_case("csv") {
                    opts.format = CopyFormat::Csv;
                } else if v.eq_ignore_ascii_case("binary") {
                    opts.format = CopyFormat::Binary;
                } else {
                    return Err(format!(
                        "FORMAT {v:?} is not supported (accepted: CSV, BINARY)"
                    ));
                }
            }
            "HEADER" => {
                csv_only_seen.get_or_insert("HEADER");
                sc.skip_whitespace();
                // HEADER may appear with no value (true), or with TRUE/FALSE/ON/OFF/1/0.
                if sc.peek_punct(',') || sc.peek_punct(')') {
                    header = true;
                } else {
                    let v = sc
                        .eat_ident()
                        .ok_or_else(|| "expected HEADER value".to_owned())?;
                    header = match v.to_ascii_lowercase().as_str() {
                        "true" | "on" | "1" | "t" => true,
                        "false" | "off" | "0" | "f" => false,
                        other => {
                            return Err(format!("invalid HEADER value {other:?}"));
                        }
                    };
                }
            }
            "DELIMITER" => {
                csv_only_seen.get_or_insert("DELIMITER");
                sc.skip_whitespace();
                let s = parse_string_literal(sc)?;
                let mut chars = s.chars();
                let c = chars
                    .next()
                    .ok_or_else(|| "DELIMITER must be a single character".to_owned())?;
                if chars.next().is_some() {
                    return Err(format!("DELIMITER must be a single character, got {s:?}"));
                }
                opts.delimiter = c;
            }
            "NULL" => {
                csv_only_seen.get_or_insert("NULL");
                sc.skip_whitespace();
                opts.null_string = parse_string_literal(sc)?;
            }
            "QUOTE" => {
                csv_only_seen.get_or_insert("QUOTE");
                sc.skip_whitespace();
                let s = parse_string_literal(sc)?;
                let mut chars = s.chars();
                let c = chars
                    .next()
                    .ok_or_else(|| "QUOTE must be a single character".to_owned())?;
                if chars.next().is_some() {
                    return Err(format!("QUOTE must be a single character, got {s:?}"));
                }
                opts.quote = c;
            }
            "ESCAPE" => {
                csv_only_seen.get_or_insert("ESCAPE");
                sc.skip_whitespace();
                let s = parse_string_literal(sc)?;
                let mut chars = s.chars();
                let c = chars
                    .next()
                    .ok_or_else(|| "ESCAPE must be a single character".to_owned())?;
                if chars.next().is_some() {
                    return Err(format!("ESCAPE must be a single character, got {s:?}"));
                }
                opts.escape = c;
            }
            other => {
                return Err(format!(
                    "COPY option {other:?} is not supported (accepted: FORMAT, HEADER, DELIMITER, NULL, QUOTE, ESCAPE)"
                ));
            }
        }
        sc.skip_whitespace();
        if sc.eat_punct(')') {
            break;
        }
        if !sc.eat_punct(',') {
            return Err("expected ',' or ')' in WITH (...)".into());
        }
    }
    if let (CopyFormat::Binary, Some(opt)) = (opts.format, csv_only_seen) {
        return Err(format!("cannot specify {opt} in BINARY mode"));
    }
    Ok((header, opts))
}

/// Hand-rolled scanner over the SQL string. Whitespace-aware, ASCII-only
/// keywords (case-insensitive), bare or double-quoted identifiers (quoted
/// names restricted to the bare-identifier charset — see `eat_ident`).
struct Scanner<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.s[self.pos..]
    }

    fn is_done(&self) -> bool {
        self.pos >= self.s.len()
    }

    fn skip_whitespace(&mut self) {
        let bytes = self.s.as_bytes();
        while self.pos < bytes.len() && bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        self.skip_whitespace();
        let rest = self.rest();
        if rest.len() < kw.len() {
            return false;
        }
        let head = &rest[..kw.len()];
        if !head.eq_ignore_ascii_case(kw) {
            return false;
        }
        // Must be followed by non-ident char or end-of-input.
        let next = rest.as_bytes().get(kw.len()).copied();
        let boundary_ok = match next {
            None => true,
            Some(b) => !(b.is_ascii_alphanumeric() || b == b'_'),
        };
        if !boundary_ok {
            return false;
        }
        self.pos += kw.len();
        true
    }

    /// Eat one identifier: bare (`users`) or double-quoted (`"users"`, with
    /// `""` escaping — the form sqlx's `PgCopyIn` emits). Quoted identifiers
    /// are returned *unquoted*, and only accepted when the unquoted form
    /// matches the bare-identifier charset: the name is later re-rendered
    /// into `SELECT`/`INSERT` SQL without quoting, so admitting arbitrary
    /// characters here would be an injection vector. Exotic quoted names
    /// (spaces, punctuation, doubled quotes) return `None` without consuming,
    /// which surfaces the caller's "expected table/column name" error.
    fn eat_ident(&mut self) -> Option<String> {
        self.skip_whitespace();
        let bytes = self.s.as_bytes();
        let start = self.pos;
        if start >= bytes.len() {
            return None;
        }
        if bytes[start] == b'"' {
            let mut i = start + 1;
            let mut out = String::new();
            loop {
                if i >= bytes.len() {
                    return None; // unterminated quoted identifier
                }
                let b = bytes[i];
                if b == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                out.push(b as char);
                i += 1;
            }
            let head_ok = out
                .as_bytes()
                .first()
                .is_some_and(|&b| b.is_ascii_alphabetic() || b == b'_');
            let body_ok = out.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
            if !(head_ok && body_ok) {
                return None;
            }
            self.pos = i;
            return Some(out);
        }
        let first = bytes[start];
        if !(first.is_ascii_alphabetic() || first == b'_') {
            return None;
        }
        let mut end = start + 1;
        while end < bytes.len() {
            let b = bytes[end];
            if b.is_ascii_alphanumeric() || b == b'_' {
                end += 1;
            } else {
                break;
            }
        }
        self.pos = end;
        Some(self.s[start..end].to_owned())
    }

    fn eat_punct(&mut self, p: char) -> bool {
        self.skip_whitespace();
        let bytes = self.s.as_bytes();
        if self.pos < bytes.len() && bytes[self.pos] == p as u8 {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek_punct(&self, p: char) -> bool {
        let bytes = self.s.as_bytes();
        let mut i = self.pos;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        i < bytes.len() && bytes[i] == p as u8
    }
}

/// Resolve the schema of `table` by `prepare("SELECT * FROM <table>")` and
/// keeping the returned `Field` list. Closes the prepared statement on the
/// way out so we don't leak engine handles.
pub(crate) async fn resolve_table_columns<S: Session + ?Sized>(
    session: &S,
    table: &str,
) -> std::result::Result<Vec<Field>, basin_common::BasinError> {
    let sql = format!("SELECT * FROM {} LIMIT 0", table);
    let (handle, schema) = session.prepare(&sql).await?;
    session.close_statement(&handle).await;
    Ok(schema.columns)
}

/// Given the full table schema and an optional user-supplied column list,
/// return the subset of `Field`s the COPY-IN reader should expect (in column-
/// list order). Validates that every listed column exists and is listed at
/// most once; errors return SQLSTATE 42601 at the call site.
///
/// It deliberately does NOT validate that omitted NOT-NULL columns have a
/// DEFAULT. `table_columns` here is resolved via a `SELECT * LIMIT 0` prepare
/// (see [`resolve_table_columns`]), and DataFusion strips per-field metadata
/// off projection output — so `BASIN_COLUMN_DEFAULT` (the marker behind SERIAL
/// and user DEFAULTs) is invisible at this layer. A metadata-blind check here
/// wrongly rejected valid column-list COPYs that omit a defaulted column (e.g.
/// a SERIAL primary key). Authoritative enforcement lives in the engine ingest
/// path (`copy_ingest::exec_copy_from_batch`), which reads the catalog schema
/// (metadata intact): it fills DEFAULTs for omitted columns and raises a
/// genuine NOT-NULL violation for an omitted required column with no default.
pub(crate) fn select_copy_in_columns(
    table: &str,
    table_columns: &[Field],
    column_list: Option<&[String]>,
) -> std::result::Result<Vec<Field>, String> {
    let column_list = match column_list {
        Some(l) => l,
        None => return Ok(table_columns.to_vec()),
    };
    let mut selected: Vec<Field> = Vec::with_capacity(column_list.len());
    let mut listed_names: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(column_list.len());
    for name in column_list {
        let lname = name.to_ascii_lowercase();
        if !listed_names.insert(lname.clone()) {
            return Err(format!("COPY: column {name:?} listed twice in column list"));
        }
        let field = table_columns
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("COPY: column {name:?} does not exist on table {table:?}"))?;
        selected.push(field.clone());
    }
    Ok(selected)
}

/// As `select_copy_in_columns`, but for the OUT (export) direction. Validates
/// that every listed name exists on the table; ordering follows the list.
/// No nullability check — exporting a subset is always safe.
pub(crate) fn select_copy_out_columns(
    table: &str,
    table_columns: &[Field],
    column_list: Option<&[String]>,
) -> std::result::Result<Vec<Field>, String> {
    let column_list = match column_list {
        Some(l) => l,
        None => return Ok(table_columns.to_vec()),
    };
    let mut selected: Vec<Field> = Vec::with_capacity(column_list.len());
    for name in column_list {
        let field = table_columns
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| format!("COPY: column {name:?} does not exist on table {table:?}"))?;
        selected.push(field.clone());
    }
    Ok(selected)
}

/// Overall + per-column format code for a CopyIn/OutResponse: 0 = text
/// (covers our CSV mode), 1 = binary.
fn copy_format_code(format: CopyFormat) -> i16 {
    match format {
        CopyFormat::Csv => 0,
        CopyFormat::Binary => 1,
    }
}

/// Build `CopyInResponse` for a copy with `n` columns in `format`.
pub(crate) fn copy_in_response(n: usize, format: CopyFormat) -> CopyInResponse {
    let code = copy_format_code(format);
    CopyInResponse::new(code as i8, n as i16, vec![code; n])
}

pub(crate) fn copy_out_response(n: usize, format: CopyFormat) -> CopyOutResponse {
    let code = copy_format_code(format);
    CopyOutResponse::new(code as i8, n as i16, vec![code; n])
}

/// Process every complete record currently buffered in `state`, dispatching
/// on the COPY wire format. Both arms share the same batching/flush/error
/// model: rows accumulate in `state.pending_rows` until `INGEST_BATCH_SIZE`
/// or end-of-stream, the first error latches into `state.error`, and
/// subsequent calls drain without touching the engine.
pub(crate) async fn process_buffered_rows<S: Session + ?Sized>(
    state: &mut CopyInState,
    session: &S,
    final_chunk: bool,
) {
    match state.opts.format {
        CopyFormat::Csv => process_buffered_csv_rows(state, session, final_chunk).await,
        CopyFormat::Binary => process_buffered_binary_rows(state, session, final_chunk).await,
    }
}

/// Process every complete CSV record currently buffered in `state`.
///
/// "Complete" = terminated by `\n` (or `\r\n`), with quoted fields allowed
/// to contain unescaped newlines. Trailing partial bytes stay in the
/// buffer for the next chunk.
///
/// **Fast path**: rows are accumulated in `state.pending_rows` until
/// `INGEST_BATCH_SIZE` is reached or `final_chunk` is true, then flushed
/// via `session.ingest_csv_batch`.  This bypasses SQL parse entirely for
/// common table types.  On `FeatureNotSupported` (e.g. partitioned table)
/// the function falls back to the original per-row INSERT path for the
/// current batch.
///
/// Stops processing on the first engine error, which gets latched into
/// `state.error`. Subsequent calls fall through (drain mode).
async fn process_buffered_csv_rows<S: Session + ?Sized>(
    state: &mut CopyInState,
    session: &S,
    final_chunk: bool,
) {
    if state.error.is_some() {
        // Drain mode: discard buffered bytes, never touch the engine.
        state.buffer.clear();
        return;
    }
    loop {
        let consumed = match split_record(&state.buffer, final_chunk, state.opts.quote) {
            Some((record, n)) => (record, n),
            None => {
                // No more complete records in the buffer.  If this is the
                // final chunk, flush whatever is pending.
                if final_chunk && !state.pending_rows.is_empty() {
                    flush_pending_rows(state, session).await;
                }
                return;
            }
        };
        let (record_bytes, n) = consumed;
        // Header rows: skip exactly one without inserting.
        if state.header_pending {
            state.header_pending = false;
            state.buffer.drain(..n);
            continue;
        }
        // Skip blank lines (CSV exports often end with one).
        if record_bytes.is_empty() {
            state.buffer.drain(..n);
            continue;
        }
        if state.error.is_some() {
            state.buffer.drain(..n);
            continue;
        }
        let record = match parse_csv_record(
            record_bytes,
            state.opts.delimiter,
            state.opts.quote,
            &state.opts.null_string,
        ) {
            Ok(r) => r,
            Err(e) => {
                state.error = Some(format!("CSV parse error: {e}"));
                state.buffer.clear();
                return;
            }
        };
        if record.len() != state.columns.len() {
            state.error = Some(format!(
                "COPY row has {} columns, table {} expects {}",
                record.len(),
                state.table,
                state.columns.len()
            ));
            state.buffer.clear();
            return;
        }
        // Accumulate row for the batch fast path.
        state.pending_rows.push(record);
        state.buffer.drain(..n);

        // Flush when the batch is full.
        if state.pending_rows.len() >= INGEST_BATCH_SIZE {
            flush_pending_rows(state, session).await;
            if state.error.is_some() {
                return;
            }
        }
    }
}

/// The 11-byte PG binary COPY signature: `PGCOPY\n\xff\r\n\0`. Followed on
/// the wire by a 4-byte flags word and a 4-byte extension-area length.
pub(crate) const COPY_BINARY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xFF\r\n\0";

/// Header flags word: bit 16 = OIDs-included (we reject it — OIDs were
/// removed in PG 12 and no modern client emits them); bits 17–31 are
/// "critical" per the spec, so an unknown set bit means we cannot safely
/// read the file. Bits 0–15 must be ignored.
const COPY_BINARY_CRITICAL_FLAGS: u32 = 0xFFFF_0000;

/// Try to consume the binary COPY header (signature + flags + extension
/// area) from the front of `buf`.
///
/// `Ok(Some(n))` = header valid, `n` bytes consumed; `Ok(None)` = need more
/// bytes; `Err` = malformed header.
fn parse_binary_header(buf: &[u8]) -> std::result::Result<Option<usize>, String> {
    const FIXED: usize = 11 + 4 + 4;
    if buf.len() < FIXED {
        return Ok(None);
    }
    if &buf[..11] != COPY_BINARY_SIGNATURE {
        return Err("COPY BINARY: input does not start with the PGCOPY signature".into());
    }
    let flags = u32::from_be_bytes(buf[11..15].try_into().unwrap());
    if flags & COPY_BINARY_CRITICAL_FLAGS != 0 {
        return Err(format!(
            "COPY BINARY: unsupported header flags 0x{flags:08x} (OIDs / unknown critical bits)"
        ));
    }
    let ext = u32::from_be_bytes(buf[15..19].try_into().unwrap()) as usize;
    if buf.len() < FIXED + ext {
        return Ok(None);
    }
    Ok(Some(FIXED + ext))
}

/// One parse step over the binary tuple stream.
#[derive(Debug)]
enum BinaryRecord {
    /// A complete tuple: per-field raw bytes (`None` = NULL, -1 length) and
    /// the total byte count consumed from the buffer.
    Row(Vec<Option<Vec<u8>>>, usize),
    /// The `0xFFFF` file trailer (consumes 2 bytes).
    Trailer,
    /// Not enough buffered bytes for a full tuple yet.
    NeedMore,
}

/// Try to parse one binary tuple (`i16` field count, then per-field
/// `i32` length + raw bytes) from the front of `buf`. Nothing is consumed
/// on `NeedMore` — the caller drains exactly the returned byte count.
fn parse_binary_record(
    buf: &[u8],
    expected_cols: usize,
) -> std::result::Result<BinaryRecord, String> {
    if buf.len() < 2 {
        return Ok(BinaryRecord::NeedMore);
    }
    let nfields = i16::from_be_bytes([buf[0], buf[1]]);
    if nfields == -1 {
        return Ok(BinaryRecord::Trailer);
    }
    if nfields < 0 || nfields as usize != expected_cols {
        return Err(format!(
            "COPY BINARY: tuple has {nfields} fields, expected {expected_cols}"
        ));
    }
    let mut pos = 2usize;
    let mut fields: Vec<Option<Vec<u8>>> = Vec::with_capacity(expected_cols);
    for _ in 0..expected_cols {
        if buf.len() < pos + 4 {
            return Ok(BinaryRecord::NeedMore);
        }
        let len = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if len == -1 {
            fields.push(None);
            continue;
        }
        if len < 0 {
            return Err(format!("COPY BINARY: invalid field length {len}"));
        }
        let len = len as usize;
        if buf.len() < pos + len {
            return Ok(BinaryRecord::NeedMore);
        }
        fields.push(Some(buf[pos..pos + len].to_vec()));
        pos += len;
    }
    Ok(BinaryRecord::Row(fields, pos))
}

/// Validate that every COPY-selected column has a binary codec on both the
/// decode (`protocol::decode_param_binary`) and encode
/// (`types::encode_copy_binary_field`) side. Anything outside the
/// intersection — vectors, intervals, arrays — gets a clean error naming the
/// column and its type. Callers surface this as SQLSTATE `0A000`
/// (feature_not_supported).
pub(crate) fn validate_binary_copy_columns<'a, I>(columns: I) -> std::result::Result<(), String>
where
    I: IntoIterator<Item = &'a Field>,
{
    for f in columns {
        let supported = match f.data_type() {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Decimal128(_, _) => true,
            // UUID rides FixedSizeBinary(16) + field metadata; a bare
            // FixedSizeBinary column has no binary wire mapping.
            DataType::FixedSizeBinary(16) => {
                crate::types::arrow_to_pg_type_field(f) == pgwire::api::Type::UUID
            }
            _ => false,
        };
        if !supported {
            return Err(format!(
                "COPY BINARY does not support column \"{}\" of type {} — use FORMAT CSV",
                f.name(),
                f.data_type()
            ));
        }
    }
    Ok(())
}

/// Decode one non-NULL binary field into the canonical text cell the shared
/// COPY-IN row pipeline (`ingest_csv_batch` / `build_insert_sql`) consumes.
/// Reuses the extended-protocol binary *parameter* codec
/// ([`crate::protocol::decode_param_binary`]) so COPY BINARY and binary Bind
/// values can never drift apart.
fn decode_binary_field(bytes: &[u8], field: &Field) -> std::result::Result<Option<String>, String> {
    let ty = crate::types::arrow_to_pg_type_field(field);
    let scalar = crate::protocol::decode_param_binary(bytes, &ty)
        .map_err(|e| format!("COPY BINARY: column \"{}\": {e}", field.name()))?;
    scalar_to_copy_cell(scalar, field.name())
}

/// Render a decoded [`ScalarParam`] as the text-cell form the CSV ingest
/// path already parses per column type (`\xHEX` for bytea, ISO-8601 for
/// timestamps — the param decoder already produced those as `Text`).
fn scalar_to_copy_cell(
    scalar: ScalarParam,
    col: &str,
) -> std::result::Result<Option<String>, String> {
    Ok(match scalar {
        ScalarParam::Null => None,
        ScalarParam::Int4(v) => Some(v.to_string()),
        ScalarParam::Int8(v) => Some(v.to_string()),
        ScalarParam::Float8(v) => Some(v.to_string()),
        ScalarParam::Bool(b) => Some(if b { "true" } else { "false" }.to_owned()),
        ScalarParam::Text(s) => Some(s),
        // TIMESTAMPTZ params decode to typed Unix-epoch micros; render the
        // same naive-UTC ISO-8601 cell the decoder produced before the typed
        // variant existed, so the CSV ingest path parses it unchanged.
        ScalarParam::Timestamptz(us) => {
            let dt =
                chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us).ok_or_else(|| {
                    format!("COPY BINARY: timestamptz out of range (column \"{col}\")")
                })?;
            Some(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        }
        ScalarParam::Bytea(bytes) => {
            let mut s = String::with_capacity(2 + bytes.len() * 2);
            s.push_str("\\x");
            for b in &bytes {
                use std::fmt::Write;
                let _ = write!(s, "{b:02x}");
            }
            Some(s)
        }
        ScalarParam::Array(_) => {
            return Err(format!(
                "COPY BINARY: array values are not supported (column \"{col}\")"
            ));
        }
    })
}

/// Binary-format counterpart of [`process_buffered_csv_rows`]: consume the
/// `PGCOPY` header once, then every complete tuple currently buffered.
/// Decoded rows feed the same `pending_rows` → `flush_pending_rows` batch
/// pipeline as CSV. The first error latches into `state.error` and flips the
/// stream into drain mode (bytes discarded until CopyDone) — identical
/// protocol-state guarantees to the CSV path.
async fn process_buffered_binary_rows<S: Session + ?Sized>(
    state: &mut CopyInState,
    session: &S,
    final_chunk: bool,
) {
    if state.error.is_some() {
        state.buffer.clear();
        return;
    }
    if !state.binary_header_parsed {
        match parse_binary_header(&state.buffer) {
            Ok(Some(n)) => {
                state.buffer.drain(..n);
                state.binary_header_parsed = true;
            }
            Ok(None) => {
                if final_chunk {
                    state.error =
                        Some("COPY BINARY: stream ended before a complete PGCOPY header".into());
                    state.buffer.clear();
                }
                return;
            }
            Err(e) => {
                state.error = Some(e);
                state.buffer.clear();
                return;
            }
        }
    }
    loop {
        if state.binary_done {
            if !state.buffer.is_empty() {
                state.error = Some("COPY BINARY: unexpected data after the file trailer".into());
                state.buffer.clear();
                return;
            }
            break;
        }
        match parse_binary_record(&state.buffer, state.columns.len()) {
            Ok(BinaryRecord::NeedMore) => {
                if final_chunk && !state.buffer.is_empty() {
                    state.error =
                        Some("COPY BINARY: stream ended mid-tuple (truncated input)".into());
                    state.buffer.clear();
                    return;
                }
                break;
            }
            Ok(BinaryRecord::Trailer) => {
                state.buffer.drain(..2);
                state.binary_done = true;
            }
            Ok(BinaryRecord::Row(fields, consumed)) => {
                let mut record: Vec<Option<String>> = Vec::with_capacity(fields.len());
                for (raw, field) in fields.into_iter().zip(state.columns.iter()) {
                    match raw {
                        None => record.push(None),
                        Some(bytes) => match decode_binary_field(&bytes, field) {
                            Ok(cell) => record.push(cell),
                            Err(e) => {
                                state.error = Some(e);
                                state.buffer.clear();
                                return;
                            }
                        },
                    }
                }
                state.pending_rows.push(record);
                state.buffer.drain(..consumed);
                if state.pending_rows.len() >= INGEST_BATCH_SIZE {
                    flush_pending_rows(state, session).await;
                    if state.error.is_some() {
                        return;
                    }
                }
            }
            Err(e) => {
                state.error = Some(e);
                state.buffer.clear();
                return;
            }
        }
    }
    // End-of-stream: flush whatever is pending. A missing trailer at a clean
    // tuple boundary is tolerated (be liberal in what we accept).
    if final_chunk && !state.pending_rows.is_empty() {
        flush_pending_rows(state, session).await;
    }
}

/// Flush `state.pending_rows` via `session.ingest_csv_batch` (fast path) or,
/// on `FeatureNotSupported`, fall back to the per-row INSERT path.
async fn flush_pending_rows<S: Session + ?Sized>(state: &mut CopyInState, session: &S) {
    if state.pending_rows.is_empty() || state.error.is_some() {
        return;
    }
    let rows = std::mem::take(&mut state.pending_rows);
    let n = rows.len() as u64;

    let result = session
        .ingest_csv_batch(
            &state.table,
            state.full_schema.clone(),
            state.column_list.as_deref(),
            rows.clone(),
        )
        .await;

    match result {
        Ok(ingested) => {
            state.row_count += ingested;
        }
        Err(e) if is_feature_not_supported(&e) => {
            // Partitioned table or other unsupported case: fall back to the
            // per-row INSERT path for this batch.
            fallback_insert_rows(state, session, rows).await;
        }
        Err(e) => {
            state.error = Some(format!(
                "COPY batch (rows {}–{}): {e}",
                state.row_count + 1,
                state.row_count + n
            ));
        }
    }
}

fn is_feature_not_supported(e: &basin_common::BasinError) -> bool {
    matches!(e, basin_common::BasinError::FeatureNotSupported(_))
}

/// Fallback: insert `rows` one at a time via the original SQL-per-row path.
/// Used for partitioned tables and other cases the fast path does not support.
async fn fallback_insert_rows<S: Session + ?Sized>(
    state: &mut CopyInState,
    session: &S,
    rows: Vec<Vec<Option<String>>>,
) {
    for record in rows {
        if state.error.is_some() {
            break;
        }
        let sql = match build_insert_sql(
            &state.table,
            state.column_list.as_deref(),
            &state.columns,
            &record,
        ) {
            Ok(s) => s,
            Err(e) => {
                state.error = Some(e);
                return;
            }
        };
        match session.execute(&sql).await {
            Ok(ExecResult::Empty { .. }) | Ok(ExecResult::Rows { .. }) => {
                state.row_count += 1;
            }
            Err(e) => {
                state.error = Some(format!("COPY row {}: {e}", state.row_count + 1));
                return;
            }
        }
    }
}

/// Try to extract one CSV record from the head of `buf`. Returns the record
/// bytes (without the trailing line terminator) and the number of bytes
/// consumed (including the terminator). Returns `None` if the buffer doesn't
/// contain a complete record yet.
///
/// If `final_chunk` is true and the buffer has unterminated bytes, those
/// bytes count as the final record (i.e. CSV without a trailing newline).
///
/// `quote_char` is the character used to delimit quoted fields (default `"`).
/// Only ASCII quote chars are supported here (non-ASCII fall back to `"`).
fn split_record(buf: &[u8], final_chunk: bool, quote_char: char) -> Option<(&[u8], usize)> {
    let qb = if quote_char.is_ascii() {
        quote_char as u8
    } else {
        b'"'
    };
    let mut in_quotes = false;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if in_quotes {
            if b == qb {
                // Doubled quote stays inside the field, single quote ends it.
                if i + 1 < buf.len() && buf[i + 1] == qb {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
            i += 1;
            continue;
        }
        match b {
            b if b == qb => {
                in_quotes = true;
                i += 1;
            }
            b'\n' => {
                // Trim the optional preceding \r.
                let end = if i > 0 && buf[i - 1] == b'\r' {
                    i - 1
                } else {
                    i
                };
                return Some((&buf[..end], i + 1));
            }
            _ => {
                i += 1;
            }
        }
    }
    if final_chunk && !buf.is_empty() && !in_quotes {
        // Tail without trailing newline. Strip a trailing \r if present.
        let end = if buf.last() == Some(&b'\r') {
            buf.len() - 1
        } else {
            buf.len()
        };
        Some((&buf[..end], buf.len()))
    } else {
        None
    }
}

/// Parse a single CSV record's bytes into a vec of fields. Each field is
/// either `None` (NULL per null_string matching, or unquoted empty cell) or
/// `Some(String)`.
///
/// `delimiter`, `quote`, and `null_string` come from the COPY options.
fn parse_csv_record(
    bytes: &[u8],
    delimiter: char,
    quote: char,
    null_string: &str,
) -> std::result::Result<Vec<Option<String>>, String> {
    // Reject NUL outright. Engine's literal renderer would happily embed it
    // and break the SQL parser on the engine side.
    if bytes.contains(&0u8) {
        return Err("NUL byte in CSV input".into());
    }
    let s = std::str::from_utf8(bytes).map_err(|e| format!("CSV not UTF-8: {e}"))?;
    let mut out: Vec<Option<String>> = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i <= chars.len() {
        // At the start of each iteration we are at the start of a field
        // (possibly empty, possibly past-end on the trailing delimiter case).
        if i == chars.len() {
            // Empty trailing field after a delimiter, or an entirely empty row.
            if !out.is_empty() {
                // An empty unquoted trailing field: check against null_string
                if null_string.is_empty() {
                    out.push(None); // empty unquoted = NULL (default behaviour)
                } else {
                    out.push(Some(String::new()));
                }
            }
            break;
        }
        if chars[i] == quote {
            // Quoted field.
            let mut field = String::new();
            i += 1;
            loop {
                if i >= chars.len() {
                    return Err("unterminated quoted field".into());
                }
                if chars[i] == quote {
                    if i + 1 < chars.len() && chars[i + 1] == quote {
                        field.push(quote);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                field.push(chars[i]);
                i += 1;
            }
            out.push(Some(field));
            // After closing quote: expect delimiter or end.
            if i == chars.len() {
                break;
            }
            if chars[i] != delimiter {
                return Err(format!(
                    "expected {:?} after quoted field, got {:?}",
                    delimiter, chars[i]
                ));
            }
            i += 1;
        } else {
            // Unquoted field: read until delimiter.
            let start = i;
            while i < chars.len() && chars[i] != delimiter {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect();
            // NULL detection: if raw equals null_string, treat as NULL.
            // When null_string is empty (default), empty unquoted = NULL.
            if raw == null_string || (null_string.is_empty() && raw.is_empty()) {
                out.push(None);
            } else {
                out.push(Some(raw));
            }
            if i == chars.len() {
                break;
            }
            // i points to delimiter; advance to next field.
            i += 1;
        }
    }
    Ok(out)
}

/// Build `INSERT INTO <table> [(col, ...)] VALUES (v1, v2, ...)` from one CSV
/// record. Each value is rendered per its target column type:
/// - numeric / bool: bare token (parsed by the engine's literal scanner)
/// - text / json / uuid / bytea: single-quoted with `'` doubled
/// - empty unquoted CSV cell: `NULL`
///
/// When `column_list` is `Some`, the names are emitted verbatim (already
/// validated against the schema by `select_copy_in_columns`), and the engine
/// fills the unlisted columns with their `DEFAULT` (or NULL).
fn build_insert_sql(
    table: &str,
    column_list: Option<&[String]>,
    columns: &[Field],
    record: &[Option<String>],
) -> std::result::Result<String, String> {
    let mut sql = String::with_capacity(64 + record.len() * 16);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    if let Some(list) = column_list {
        sql.push_str(" (");
        for (i, name) in list.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(name);
        }
        sql.push(')');
    }
    sql.push_str(" VALUES (");
    for (i, (cell, field)) in record.iter().zip(columns.iter()).enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        match cell {
            None => sql.push_str("NULL"),
            Some(v) => {
                if v.contains('\0') {
                    return Err("NUL byte in CSV value".into());
                }
                render_literal(v, field, &mut sql)?;
            }
        }
    }
    sql.push(')');
    Ok(sql)
}

/// Render one CSV cell as a SQL literal in `out`.
fn render_literal(v: &str, field: &Field, out: &mut String) -> std::result::Result<(), String> {
    let dt = field.data_type();
    match dt {
        DataType::Boolean => {
            let lower = v.to_ascii_lowercase();
            let b = match lower.as_str() {
                "t" | "true" | "1" | "yes" | "y" | "on" => true,
                "f" | "false" | "0" | "no" | "n" | "off" => false,
                _ => return Err(format!("invalid bool literal {v:?}")),
            };
            out.push_str(if b { "TRUE" } else { "FALSE" });
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            // Validate it parses as a signed integer; embed verbatim.
            if v.parse::<i64>().is_err() {
                return Err(format!("invalid integer literal {v:?}"));
            }
            out.push_str(v);
        }
        DataType::Float32 | DataType::Float64 => {
            if v.parse::<f64>().is_err() {
                return Err(format!("invalid float literal {v:?}"));
            }
            out.push_str(v);
        }
        // Everything else gets quoted as text. The engine's typed-INSERT
        // parser accepts string-quoted JSONB / UUID / BYTEA / vector
        // literals — same path REST and pgwire-extended already use.
        _ => {
            quote_into(v, out);
        }
    }
    Ok(())
}

/// Wrap `v` in single quotes, doubling any internal `'`. Mirror of
/// `basin_rest::parser::quote_sql_string` (kept local to keep the
/// basin-rest dep out of basin-router).
fn quote_into(v: &str, out: &mut String) {
    out.push('\'');
    for c in v.chars() {
        if c == '\'' {
            out.push('\'');
            out.push('\'');
        } else {
            out.push(c);
        }
    }
    out.push('\'');
}

/// Render the table (or column-list subset) as CSV bytes.
///
/// Returns `(header_line, body_lines, row_count)`:
/// - `header_line` is `Some(...)` when the caller asked for a header row;
///   each entry is one `<col>,<col>...\n` payload.
/// - `body_lines` is one `<csv-row>\n` byte vector per data row.
/// - `row_count` excludes the header.
///
/// Errors propagate as `BasinError`. The caller maps these to whatever the
/// transport (CopyData wire bytes vs. file bytes) needs.
pub(crate) async fn copy_to_csv_payload<S: Session + ?Sized>(
    session: &S,
    table: &str,
    column_list: Option<&[String]>,
    with_header: bool,
    opts: &CopyOptions,
) -> std::result::Result<(Option<Vec<u8>>, Vec<Vec<u8>>, u64), basin_common::BasinError> {
    // Always issue `SELECT * FROM <table>`. The engine's projection-pushdown
    // returns columns in physical table order regardless of the SELECT
    // column order (Parquet `ProjectionMask` is a set, not a list — schema
    // and batch can otherwise disagree on column order). For the column-
    // list COPY case we re-derive the output order locally, mapping each
    // requested name to its position in the full-schema result.
    let res = session.execute(&format!("SELECT * FROM {table}")).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { .. } => {
            return Err(basin_common::BasinError::Internal(format!(
                "COPY TO: SELECT * FROM {table} returned no result set"
            )));
        }
    };
    let column_indices =
        resolve_column_indices(&schema, column_list).map_err(basin_common::BasinError::Internal)?;
    let header = if with_header {
        let mut row = String::new();
        for (i, &idx) in column_indices.iter().enumerate() {
            if i > 0 {
                row.push(opts.delimiter);
            }
            csv_encode_into(
                schema.field(idx).name(),
                &mut row,
                opts.delimiter,
                opts.quote,
            );
        }
        row.push('\n');
        Some(row.into_bytes())
    } else {
        None
    };
    let mut body = Vec::with_capacity(batches.iter().map(|b| b.num_rows()).sum());
    let mut row_count: u64 = 0;
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut row = String::new();
            for (i, &idx) in column_indices.iter().enumerate() {
                if i > 0 {
                    row.push(opts.delimiter);
                }
                let col = batch.column(idx);
                let cell = render_csv_cell(col.as_ref(), r, schema.field(idx));
                csv_encode_into(&cell, &mut row, opts.delimiter, opts.quote);
            }
            row.push('\n');
            body.push(row.into_bytes());
            row_count += 1;
        }
    }
    Ok((header, body, row_count))
}

/// Encode `batches` as PG binary COPY chunks: one header chunk
/// (`PGCOPY…` signature + zero flags + empty extension), one chunk per
/// tuple (`i16` field count + length-prefixed fields), and the `0xFFFF`
/// trailer chunk. Per-field bytes come from
/// [`crate::types::encode_copy_binary_field`] — the same binary codec the
/// extended-query result path uses — so COPY BINARY output round-trips
/// through [`decode_binary_field`] by construction.
///
/// Returns `(chunks, row_count)`; the caller wraps each chunk in one
/// `CopyData` message (or concatenates them for file output).
fn binary_payload_from_batches(
    schema: &Schema,
    batches: &[RecordBatch],
    column_indices: &[usize],
) -> std::result::Result<(Vec<Vec<u8>>, u64), String> {
    validate_binary_copy_columns(column_indices.iter().map(|&i| schema.field(i)))?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(total_rows + 2);
    let mut header = Vec::with_capacity(19);
    header.extend_from_slice(COPY_BINARY_SIGNATURE);
    header.extend_from_slice(&0u32.to_be_bytes()); // flags
    header.extend_from_slice(&0u32.to_be_bytes()); // extension length
    chunks.push(header);
    let mut buf = BytesMut::new();
    let mut row_count: u64 = 0;
    for batch in batches {
        for r in 0..batch.num_rows() {
            buf.put_i16(column_indices.len() as i16);
            for &idx in column_indices {
                crate::types::encode_copy_binary_field(
                    batch.column(idx).as_ref(),
                    r,
                    schema.field(idx),
                    &mut buf,
                )
                .map_err(|e| {
                    format!(
                        "COPY BINARY: encode column {:?}: {e}",
                        schema.field(idx).name()
                    )
                })?;
            }
            chunks.push(buf.split().to_vec());
            row_count += 1;
        }
    }
    chunks.push(vec![0xFF, 0xFF]); // trailer: i16 -1
    Ok((chunks, row_count))
}

/// Map an optional COPY column list to result-schema indices (the engine
/// returns columns in physical table order; see `copy_to_csv_payload`).
fn resolve_column_indices(
    schema: &Schema,
    column_list: Option<&[String]>,
) -> std::result::Result<Vec<usize>, String> {
    match column_list {
        Some(list) => {
            let mut out = Vec::with_capacity(list.len());
            for name in list {
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(name))
                    .ok_or_else(|| format!("COPY TO: column {name:?} not in result schema"))?;
                out.push(idx);
            }
            Ok(out)
        }
        None => Ok((0..schema.fields().len()).collect()),
    }
}

/// Render the table (or column-list subset) as binary COPY chunks.
/// Binary sibling of [`copy_to_csv_payload`].
async fn copy_to_binary_payload<S: Session + ?Sized>(
    session: &S,
    table: &str,
    column_list: Option<&[String]>,
) -> std::result::Result<(Vec<Vec<u8>>, u64), basin_common::BasinError> {
    let res = session.execute(&format!("SELECT * FROM {table}")).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { .. } => {
            return Err(basin_common::BasinError::Internal(format!(
                "COPY TO: SELECT * FROM {table} returned no result set"
            )));
        }
    };
    let column_indices =
        resolve_column_indices(&schema, column_list).map_err(basin_common::BasinError::Internal)?;
    binary_payload_from_batches(&schema, &batches, &column_indices)
        .map_err(basin_common::BasinError::FeatureNotSupported)
}

/// Render a query result as CSV bytes for `COPY (SELECT …) TO STDOUT`.
///
/// Runs `query` through the engine session and encodes the result rows.
/// Returns `(header_line, body_lines, row_count)` just like
/// [`copy_to_csv_payload`].
pub(crate) async fn copy_query_to_csv_payload<S: Session + ?Sized>(
    session: &S,
    query: &str,
    with_header: bool,
    opts: &CopyOptions,
) -> std::result::Result<(Option<Vec<u8>>, Vec<Vec<u8>>, u64), basin_common::BasinError> {
    let res = session.execute(query).await?;
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { .. } => {
            return Err(basin_common::BasinError::Internal(
                "COPY (query) TO: query returned no result set".into(),
            ));
        }
    };
    let n = schema.fields().len();
    let header = if with_header {
        let mut row = String::new();
        for (i, f) in schema.fields().iter().enumerate() {
            if i > 0 {
                row.push(opts.delimiter);
            }
            csv_encode_into(f.name(), &mut row, opts.delimiter, opts.quote);
        }
        row.push('\n');
        Some(row.into_bytes())
    } else {
        None
    };
    let mut body: Vec<Vec<u8>> = Vec::new();
    let mut row_count: u64 = 0;
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut row = String::new();
            for (i, idx) in (0..n).enumerate() {
                if i > 0 {
                    row.push(opts.delimiter);
                }
                let col = batch.column(idx);
                let cell = render_csv_cell(col.as_ref(), r, schema.field(idx));
                csv_encode_into(&cell, &mut row, opts.delimiter, opts.quote);
            }
            row.push('\n');
            body.push(row.into_bytes());
            row_count += 1;
        }
    }
    Ok((header, body, row_count))
}

/// Run the full `COPY TO STDOUT` flow synchronously. Returns the backend
/// message sequence for the simple-query handler to feed onto the wire.
///
/// Wire shape:
/// 1. `CopyOutResponse(text, N cols)`
/// 2. one `CopyData(<csv-row>\n)` per row
/// 3. `CopyDone`
/// 4. `CommandComplete("COPY <count>")`
/// 5. `ReadyForQuery(Idle)`
///
/// On engine error before any rows are streamed we collapse to
/// `ErrorResponse + ReadyForQuery` (no half-open CopyOut state).
pub(crate) async fn copy_to_stdout_messages<S: Session + ?Sized>(
    session: &S,
    table: &str,
    column_list: Option<&[String]>,
    with_header: bool,
    opts: &CopyOptions,
) -> Vec<PgWireBackendMessage> {
    // Resolve full schema once so we know how many columns CopyOutResponse
    // should advertise, and so we can pre-validate the column list with a
    // user-friendly error.
    let table_columns = match resolve_table_columns(session, table).await {
        Ok(c) if !c.is_empty() => c,
        Ok(_) => {
            let info = ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("COPY TO: cannot resolve schema of {table:?}"),
            );
            return vec![
                PgWireBackendMessage::ErrorResponse(info.into()),
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ];
        }
        Err(e) => {
            return vec![
                PgWireBackendMessage::ErrorResponse(crate::error::error_response(&e)),
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ];
        }
    };
    let selected = match select_copy_out_columns(table, &table_columns, column_list) {
        Ok(s) => s,
        Err(msg) => {
            let info = ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), msg);
            return vec![
                PgWireBackendMessage::ErrorResponse(info.into()),
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ];
        }
    };
    let n_cols = selected.len();
    let mut out: Vec<PgWireBackendMessage> = Vec::new();
    // Build all CopyData chunks up-front so any engine/encode error collapses
    // to ErrorResponse + ReadyForQuery (no half-open CopyOut state). The two
    // formats only differ in chunk construction; the message shape is shared.
    let (chunks, row_count) = match opts.format {
        CopyFormat::Csv => {
            match copy_to_csv_payload(session, table, column_list, with_header, opts).await {
                Ok((header, body, row_count)) => {
                    let mut chunks = Vec::with_capacity(body.len() + 1);
                    if let Some(h) = header {
                        chunks.push(h);
                    }
                    chunks.extend(body);
                    (chunks, row_count)
                }
                Err(e) => {
                    out.push(PgWireBackendMessage::ErrorResponse(
                        crate::error::error_response(&e),
                    ));
                    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        TransactionStatus::Idle,
                    )));
                    return out;
                }
            }
        }
        CopyFormat::Binary => match copy_to_binary_payload(session, table, column_list).await {
            Ok(p) => p,
            Err(e) => {
                out.push(PgWireBackendMessage::ErrorResponse(
                    crate::error::error_response(&e),
                ));
                out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    TransactionStatus::Idle,
                )));
                return out;
            }
        },
    };
    out.push(PgWireBackendMessage::CopyOutResponse(copy_out_response(
        n_cols,
        opts.format,
    )));
    for chunk in chunks {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            chunk,
        ))));
    }
    out.push(PgWireBackendMessage::CopyDone(
        pgwire::messages::copy::CopyDone::new(),
    ));
    out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
        format!("COPY {row_count}"),
    )));
    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    out
}

/// Run `COPY (SELECT …) TO STDOUT` synchronously. Executes the query and
/// streams the result as CSV or binary COPY chunks per `opts.format`.
/// Returns the full backend-message sequence.
pub(crate) async fn copy_query_to_stdout_messages<S: Session + ?Sized>(
    session: &S,
    query: &str,
    with_header: bool,
    opts: &CopyOptions,
) -> Vec<PgWireBackendMessage> {
    let mut out: Vec<PgWireBackendMessage> = Vec::new();
    let error_messages = |e: &basin_common::BasinError| {
        vec![
            PgWireBackendMessage::ErrorResponse(crate::error::error_response(e)),
            PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
        ]
    };
    let (chunks, row_count, n_cols) = match opts.format {
        CopyFormat::Csv => {
            let payload = match copy_query_to_csv_payload(session, query, with_header, opts).await {
                Ok(p) => p,
                Err(e) => return error_messages(&e),
            };
            let (header, body, row_count) = payload;
            // For CSV query-source COPY we don't know the column count ahead
            // of running the query. Derive it from the header or body row,
            // or default to 1.
            let n_cols = if let Some(ref h) = header {
                h.iter().filter(|&&b| b == opts.delimiter as u8).count() + 1
            } else if let Some(first) = body.first() {
                first.iter().filter(|&&b| b == opts.delimiter as u8).count() + 1
            } else {
                1
            };
            let mut chunks = Vec::with_capacity(body.len() + 1);
            if let Some(h) = header {
                chunks.push(h);
            }
            chunks.extend(body);
            (chunks, row_count, n_cols)
        }
        CopyFormat::Binary => {
            let (schema, batches) = match session.execute(query).await {
                Ok(ExecResult::Rows { schema, batches }) => (schema, batches),
                Ok(ExecResult::Empty { .. }) => {
                    return error_messages(&basin_common::BasinError::Internal(
                        "COPY (query) TO: query returned no result set".into(),
                    ));
                }
                Err(e) => return error_messages(&e),
            };
            let n_cols = schema.fields().len();
            let indices: Vec<usize> = (0..n_cols).collect();
            match binary_payload_from_batches(&schema, &batches, &indices) {
                Ok((chunks, row_count)) => (chunks, row_count, n_cols),
                Err(msg) => {
                    return error_messages(&basin_common::BasinError::FeatureNotSupported(msg));
                }
            }
        }
    };
    out.push(PgWireBackendMessage::CopyOutResponse(copy_out_response(
        n_cols,
        opts.format,
    )));
    for chunk in chunks {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            chunk,
        ))));
    }
    out.push(PgWireBackendMessage::CopyDone(
        pgwire::messages::copy::CopyDone::new(),
    ));
    out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
        format!("COPY {row_count}"),
    )));
    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    out
}

/// CSV-encode a single string field into `out` using `delimiter` and `quote`.
/// Quote when the value contains the delimiter, quote char, `\n`, or `\r`;
/// double internal quote chars. Empty string is written without quotes
/// (empty-unquoted) — the COPY-IN reader will treat it as NULL by default.
fn csv_encode_into(v: &str, out: &mut String, delimiter: char, quote: char) {
    let needs_quote = v
        .chars()
        .any(|c| c == delimiter || c == quote || c == '\n' || c == '\r');
    if !needs_quote {
        out.push_str(v);
        return;
    }
    out.push(quote);
    for c in v.chars() {
        if c == quote {
            out.push(quote);
            out.push(quote);
        } else {
            out.push(c);
        }
    }
    out.push(quote);
}

/// Render one Arrow cell into the canonical CSV string representation. We
/// piggy-back on the existing text-format renderers in `crate::types` for
/// consistency with the simple-query SELECT path.
fn render_csv_cell(col: &dyn arrow_array::Array, idx: usize, field: &Field) -> String {
    if crate::types::is_null_cell(col, idx) {
        // CSV NULL = empty unquoted by default in Postgres CSV mode.
        return String::new();
    }
    crate::types::render_cell_for_copy(col, idx, field)
}

/// Build the simple-query message sequence for a `COPY FROM STDIN` *response*.
/// The connection state must be flipped to `CopyInProgress` after sending
/// `CopyInResponse` so the framework routes subsequent `CopyData` /
/// `CopyDone` to the `CopyHandler`.
pub(crate) fn copy_from_stdin_messages(
    n_cols: usize,
    format: CopyFormat,
) -> Vec<PgWireBackendMessage> {
    vec![PgWireBackendMessage::CopyInResponse(copy_in_response(
        n_cols, format,
    ))]
}

/// Primary on/off gate for server-side COPY file paths.
///
/// Set `BASIN_COPY_ALLOW_FILE_PATHS=1` to enable. Any other value (including
/// unset / empty) disables file-path COPY entirely; the connection gets
/// SQLSTATE 42501 (insufficient_privilege).
pub(crate) const COPY_ALLOW_FILE_PATHS_ENV: &str = "BASIN_COPY_ALLOW_FILE_PATHS";

/// Env-var-driven allowlist for server-side COPY file paths. Default-deny: if
/// `BASIN_COPY_PATH_ALLOWLIST` is unset or empty, every file path is rejected.
///
/// Format: colon-separated list of absolute directory prefixes. A path is
/// accepted iff (a) it is absolute and (b) it lies inside one of the listed
/// directories (after path-component normalisation; we do NOT canonicalise
/// symlinks — keep this comparison purely lexical to avoid TOCTOU surprises
/// and to allow paths to point at files that don't exist yet for COPY TO).
///
/// Example: `BASIN_COPY_PATH_ALLOWLIST=/var/lib/basin/imports:/var/lib/basin/exports`.
pub(crate) const COPY_PATH_ALLOWLIST_ENV: &str = "BASIN_COPY_PATH_ALLOWLIST";

/// Validate `path` for use as a server-side COPY file path. Returns the
/// path unchanged on success. On rejection:
/// - Returns a string prefixed with `"\x0042501\x00"` to signal SQLSTATE
///   42501 (insufficient_privilege) for the `BASIN_COPY_ALLOW_FILE_PATHS`
///   gate rejection.
/// - Returns a plain string (no prefix) for SQLSTATE 42601 errors (bad path).
pub(crate) fn validate_copy_path(path: &str) -> std::result::Result<std::path::PathBuf, String> {
    use std::path::{Component, PathBuf};

    // Primary gate: BASIN_COPY_ALLOW_FILE_PATHS=1 must be set.
    let allow_flag = std::env::var(COPY_ALLOW_FILE_PATHS_ENV).unwrap_or_default();
    if allow_flag.trim() != "1" {
        return Err(format!(
            "\x0042501\x00COPY: server-side file paths are disabled (set {COPY_ALLOW_FILE_PATHS_ENV}=1 to enable)"
        ));
    }

    let allowlist_raw = std::env::var(COPY_PATH_ALLOWLIST_ENV).unwrap_or_default();
    if allowlist_raw.is_empty() {
        return Err(format!(
            "COPY: server-side file paths are disabled (set {COPY_PATH_ALLOWLIST_ENV}=/dir1:/dir2 to enable)"
        ));
    }
    let pb = PathBuf::from(path);
    if !pb.is_absolute() {
        return Err(format!("COPY: file path must be absolute, got {path:?}"));
    }
    // Reject `..` components — keeps lexical comparison meaningful even if a
    // listed dir is nested.
    if pb.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "COPY: file path may not contain '..' components, got {path:?}"
        ));
    }
    let allowlist: Vec<PathBuf> = allowlist_raw
        .split(':')
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect();
    if allowlist.is_empty() {
        return Err(format!(
            "COPY: {COPY_PATH_ALLOWLIST_ENV} is set but contains no entries"
        ));
    }
    let allowed = allowlist.iter().any(|root| pb.starts_with(root));
    if !allowed {
        return Err(format!(
            "COPY: file path {path:?} is outside {COPY_PATH_ALLOWLIST_ENV}"
        ));
    }
    Ok(pb)
}

/// Drive `COPY <table> [(cols)] TO '<path>'` on the basin-server process.
/// Writes the rendered CSV bytes to `path` (truncating any existing file)
/// and returns the row count for the `CommandComplete` tag.
///
/// Sequential, single buffer — no per-project file handles allocated.
pub(crate) async fn copy_to_file<S: Session + ?Sized>(
    session: &S,
    table: &str,
    column_list: Option<&[String]>,
    path: &std::path::Path,
    with_header: bool,
    opts: &CopyOptions,
) -> std::result::Result<u64, basin_common::BasinError> {
    use tokio::io::AsyncWriteExt;
    let (header, body, row_count) =
        copy_to_csv_payload(session, table, column_list, with_header, opts).await?;
    let mut f = tokio::fs::File::create(path).await.map_err(|e| {
        basin_common::BasinError::Internal(format!("COPY TO {}: open: {e}", path.display()))
    })?;
    if let Some(h) = header {
        f.write_all(&h)
            .await
            .map_err(|e| basin_common::BasinError::Internal(format!("COPY TO write: {e}")))?;
    }
    for row in &body {
        f.write_all(row)
            .await
            .map_err(|e| basin_common::BasinError::Internal(format!("COPY TO write: {e}")))?;
    }
    f.flush()
        .await
        .map_err(|e| basin_common::BasinError::Internal(format!("COPY TO flush: {e}")))?;
    Ok(row_count)
}

/// Drive `COPY <table> [(cols)] FROM '<path>'` on the basin-server process.
/// Reads the file in one shot (sequential, single buffer) and feeds it
/// through the existing CSV-row → INSERT path.
pub(crate) async fn copy_from_file<S: Session + ?Sized>(
    session: &S,
    state: &mut CopyInState,
    path: &std::path::Path,
) -> std::result::Result<(), basin_common::BasinError> {
    let bytes = tokio::fs::read(path).await.map_err(|e| {
        basin_common::BasinError::Internal(format!("COPY FROM {}: read: {e}", path.display()))
    })?;
    state.buffer.extend_from_slice(&bytes);
    process_buffered_rows(state, session, true).await;
    Ok(())
}

/// Per-connection mutex around an in-flight `COPY FROM STDIN`. The
/// simple-query handler pushes one in here when it sends `CopyInResponse`;
/// `CopyHandler::on_copy_data` / `on_copy_done` pop / mutate it.
pub(crate) type CopyStateSlot = Arc<tokio::sync::Mutex<Option<CopyInState>>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn default_opts() -> CopyOptions {
        CopyOptions::default()
    }

    #[test]
    fn parse_copy_recognises_from_stdin_csv() {
        let cmd = parse_copy("COPY events FROM STDIN WITH (FORMAT CSV)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "events".into(),
                with_header: false,
                columns: None,
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_recognises_to_stdout_csv_header() {
        let cmd = parse_copy("COPY t TO STDOUT WITH (FORMAT CSV, HEADER true)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::To {
                table: "t".into(),
                with_header: true,
                columns: None,
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_no_with_clause_defaults_to_csv() {
        let cmd = parse_copy("COPY t FROM STDIN").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "t".into(),
                with_header: false,
                columns: None,
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_column_list() {
        let cmd = parse_copy("COPY t (b, a) FROM STDIN WITH (FORMAT CSV)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "t".into(),
                with_header: false,
                columns: Some(vec!["b".into(), "a".into()]),
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_to_column_list() {
        let cmd =
            parse_copy("COPY t (id, email) TO STDOUT WITH (FORMAT CSV, HEADER true)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::To {
                table: "t".into(),
                with_header: true,
                columns: Some(vec!["id".into(), "email".into()]),
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_rejects_empty_column_list() {
        let e = parse_copy("COPY t () FROM STDIN").unwrap_err();
        assert!(e.contains("column list"), "got: {e}");
    }

    #[test]
    fn parse_copy_accepts_to_file_path() {
        let cmd = parse_copy("COPY t TO '/tmp/users.csv' WITH (FORMAT CSV)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::To {
                table: "t".into(),
                with_header: false,
                columns: None,
                path: Some("/tmp/users.csv".into()),
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_from_file_path() {
        let cmd = parse_copy("COPY t FROM '/var/lib/import.csv' WITH CSV").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref path, .. }) if path.as_deref() == Some("/var/lib/import.csv"))
        );
    }

    #[test]
    fn parse_copy_accepts_delimiter_option() {
        let cmd = parse_copy("COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|')").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.delimiter == '|'),
            "expected pipe delimiter"
        );
    }

    #[test]
    fn parse_copy_accepts_null_option() {
        let cmd = parse_copy(r"COPY t FROM STDIN WITH (FORMAT CSV, NULL '\N')").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.null_string == r"\N"),
            "expected \\N null string"
        );
    }

    #[test]
    fn parse_copy_accepts_quote_and_escape() {
        let cmd =
            parse_copy("COPY t FROM STDIN WITH (FORMAT CSV, QUOTE '\"', ESCAPE '\\')").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.quote == '"' && opts.escape == '\\'),
            "expected custom quote/escape"
        );
    }

    #[test]
    fn parse_copy_accepts_query_source() {
        let cmd =
            parse_copy("COPY (SELECT id, name FROM t WHERE id > 5) TO STDOUT WITH (FORMAT CSV)")
                .unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::QueryTo { ref query, with_header, .. })
                if query.contains("SELECT") && !with_header),
            "expected QueryTo"
        );
    }

    #[test]
    fn parse_copy_accepts_query_source_with_header() {
        let cmd =
            parse_copy("COPY (SELECT * FROM t) TO STDOUT WITH (FORMAT CSV, HEADER true)").unwrap();
        assert!(
            matches!(
                cmd,
                Some(CopyCommand::QueryTo {
                    with_header: true,
                    ..
                })
            ),
            "expected QueryTo with header"
        );
    }

    #[test]
    fn parse_copy_rejects_program_from() {
        let e = parse_copy("COPY t FROM PROGRAM 'cat /etc/passwd'").unwrap_err();
        assert!(e.contains("42501") || e.contains("PROGRAM"), "got: {e}");
    }

    #[test]
    fn parse_copy_rejects_program_to() {
        let e = parse_copy("COPY t TO PROGRAM 'cat'").unwrap_err();
        assert!(e.contains("42501") || e.contains("PROGRAM"), "got: {e}");
    }

    #[test]
    fn parse_copy_rejects_unknown_formats() {
        for sql in [
            "COPY t FROM STDIN WITH (FORMAT TEXT)",
            "COPY t FROM STDIN WITH (FORMAT XML)",
        ] {
            let e = parse_copy(sql).unwrap_err();
            assert!(e.contains("FORMAT"), "got: {e}");
        }
    }

    // ── quoted identifiers (sqlx PgCopyIn shape) ───────────────────────────

    #[test]
    fn parse_copy_accepts_sqlx_quoted_identifier_shape() {
        // Exact statement from the sqlx section of the ORM corpus.
        let cmd = parse_copy(r#"COPY "users" (id, email) FROM STDIN"#).unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "users".into(),
                with_header: false,
                columns: Some(vec!["id".into(), "email".into()]),
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_quoted_column_list() {
        let cmd =
            parse_copy(r#"COPY "users" ("id", "Email_2") TO STDOUT WITH (FORMAT CSV)"#).unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::To {
                table: "users".into(),
                with_header: false,
                columns: Some(vec!["id".into(), "Email_2".into()]),
                path: None,
                opts: default_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_rejects_quoted_identifier_with_special_chars() {
        // Quoted names outside the bare-identifier charset are refused —
        // they would otherwise be re-rendered into SQL unquoted (injection).
        for sql in [
            r#"COPY "users; DROP TABLE x" FROM STDIN"#,
            r#"COPY "user name" FROM STDIN"#,
            r#"COPY "us""er" FROM STDIN"#,
            r#"COPY "" FROM STDIN"#,
        ] {
            let e = parse_copy(sql).unwrap_err();
            assert!(e.contains("table name"), "{sql}: got {e}");
        }
    }

    // ── BINARY format acceptance / rejection matrix ────────────────────────

    fn binary_opts() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Binary,
            ..CopyOptions::default()
        }
    }

    #[test]
    fn parse_copy_accepts_format_binary_from_stdin() {
        let cmd = parse_copy("COPY t FROM STDIN WITH (FORMAT BINARY)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "t".into(),
                with_header: false,
                columns: None,
                path: None,
                opts: binary_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_format_binary_to_stdout_with_columns() {
        let cmd = parse_copy("COPY t (a, b) TO STDOUT WITH (FORMAT BINARY)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::To {
                table: "t".into(),
                with_header: false,
                columns: Some(vec!["a".into(), "b".into()]),
                path: None,
                opts: binary_opts(),
            })
        );
    }

    #[test]
    fn parse_copy_accepts_legacy_with_binary_shorthand() {
        let cmd = parse_copy("COPY t FROM STDIN WITH BINARY").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.format == CopyFormat::Binary)
        );
    }

    #[test]
    fn parse_copy_accepts_option_list_without_with_keyword() {
        // Modern PG ≥ 9.0 syntax: parenthesised options, no WITH.
        let cmd = parse_copy("COPY t FROM STDIN (FORMAT BINARY)").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.format == CopyFormat::Binary)
        );
        let cmd = parse_copy("COPY t FROM STDIN (FORMAT CSV, HEADER true)").unwrap();
        assert!(matches!(
            cmd,
            Some(CopyCommand::From {
                with_header: true,
                ..
            })
        ));
    }

    #[test]
    fn parse_copy_rejects_binary_with_csv_only_options() {
        for (sql, opt) in [
            (
                "COPY t FROM STDIN WITH (FORMAT BINARY, HEADER true)",
                "HEADER",
            ),
            (
                "COPY t FROM STDIN WITH (FORMAT BINARY, DELIMITER '|')",
                "DELIMITER",
            ),
            (r"COPY t FROM STDIN WITH (FORMAT BINARY, NULL '\N')", "NULL"),
            ("COPY t TO STDOUT WITH (FORMAT BINARY, QUOTE '\"')", "QUOTE"),
            // FORMAT after the conflicting option must still be caught.
            (
                "COPY t FROM STDIN WITH (DELIMITER '|', FORMAT BINARY)",
                "DELIMITER",
            ),
        ] {
            let e = parse_copy(sql).unwrap_err();
            assert!(e.contains(opt) && e.contains("BINARY"), "{sql}: got {e}");
        }
    }

    #[test]
    fn parse_copy_rejects_binary_file_paths() {
        for sql in [
            "COPY t FROM '/tmp/x.bin' WITH (FORMAT BINARY)",
            "COPY t TO '/tmp/x.bin' WITH (FORMAT BINARY)",
        ] {
            let e = parse_copy(sql).unwrap_err();
            assert!(e.contains("STDIN/STDOUT"), "{sql}: got {e}");
        }
    }

    #[test]
    fn parse_copy_accepts_query_source_binary() {
        let cmd = parse_copy("COPY (SELECT 1) TO STDOUT WITH (FORMAT BINARY)").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::QueryTo { ref opts, .. }) if opts.format == CopyFormat::Binary)
        );
    }

    // ── binary stream parser ───────────────────────────────────────────────

    fn binary_header_bytes(ext: &[u8]) -> Vec<u8> {
        let mut h = Vec::new();
        h.extend_from_slice(COPY_BINARY_SIGNATURE);
        h.extend_from_slice(&0u32.to_be_bytes());
        h.extend_from_slice(&(ext.len() as u32).to_be_bytes());
        h.extend_from_slice(ext);
        h
    }

    #[test]
    fn parse_binary_header_consumes_header_and_extension() {
        let h = binary_header_bytes(b"abcd");
        assert_eq!(parse_binary_header(&h).unwrap(), Some(19 + 4));
        // Truncated header → need more bytes, nothing consumed.
        assert_eq!(parse_binary_header(&h[..10]).unwrap(), None);
        assert_eq!(parse_binary_header(&h[..20]).unwrap(), None);
    }

    #[test]
    fn parse_binary_header_rejects_bad_signature_and_flags() {
        let mut bad_sig = binary_header_bytes(b"");
        bad_sig[0] = b'X';
        assert!(parse_binary_header(&bad_sig)
            .unwrap_err()
            .contains("signature"));

        let mut bad_flags = binary_header_bytes(b"");
        bad_flags[11..15].copy_from_slice(&0x0001_0000u32.to_be_bytes()); // OID bit
        assert!(parse_binary_header(&bad_flags)
            .unwrap_err()
            .contains("flags"));

        // Low 16 flag bits must be ignored per the spec.
        let mut soft_flags = binary_header_bytes(b"");
        soft_flags[11..15].copy_from_slice(&0x0000_00FFu32.to_be_bytes());
        assert_eq!(parse_binary_header(&soft_flags).unwrap(), Some(19));
    }

    fn binary_tuple(fields: &[Option<&[u8]>]) -> Vec<u8> {
        let mut t = Vec::new();
        t.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for f in fields {
            match f {
                None => t.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(b) => {
                    t.extend_from_slice(&(b.len() as i32).to_be_bytes());
                    t.extend_from_slice(b);
                }
            }
        }
        t
    }

    #[test]
    fn parse_binary_record_row_null_trailer_and_splits() {
        let tuple = binary_tuple(&[Some(&7i64.to_be_bytes()), None]);
        match parse_binary_record(&tuple, 2).unwrap() {
            BinaryRecord::Row(fields, consumed) => {
                assert_eq!(consumed, tuple.len());
                assert_eq!(fields[0].as_deref(), Some(&7i64.to_be_bytes()[..]));
                assert_eq!(fields[1], None);
            }
            _ => panic!("expected Row"),
        }
        // Every strict prefix is NeedMore (chunk boundaries are arbitrary).
        for cut in 0..tuple.len() {
            assert!(
                matches!(
                    parse_binary_record(&tuple[..cut], 2).unwrap(),
                    BinaryRecord::NeedMore
                ),
                "cut at {cut}"
            );
        }
        // Trailer.
        assert!(matches!(
            parse_binary_record(&(-1i16).to_be_bytes(), 2).unwrap(),
            BinaryRecord::Trailer
        ));
    }

    #[test]
    fn parse_binary_record_rejects_field_count_mismatch() {
        let tuple = binary_tuple(&[Some(b"x")]);
        let e = parse_binary_record(&tuple, 3).unwrap_err();
        assert!(e.contains("1 fields, expected 3"), "got: {e}");
    }

    #[test]
    fn scalar_to_copy_cell_renders_bytea_hex_and_bool() {
        assert_eq!(
            scalar_to_copy_cell(ScalarParam::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]), "c").unwrap(),
            Some(r"\xdeadbeef".to_string())
        );
        assert_eq!(
            scalar_to_copy_cell(ScalarParam::Bool(true), "c").unwrap(),
            Some("true".to_string())
        );
        assert_eq!(scalar_to_copy_cell(ScalarParam::Null, "c").unwrap(), None);
    }

    #[test]
    fn validate_binary_copy_columns_accepts_scalars_rejects_vectors() {
        let ok = vec![
            Field::new("i", DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("t", DataType::Utf8, true),
            Field::new("by", DataType::Binary, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
        ];
        validate_binary_copy_columns(ok.iter()).unwrap();

        let vec_field = Field::new(
            "emb",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        );
        let e = validate_binary_copy_columns(std::iter::once(&vec_field)).unwrap_err();
        assert!(e.contains("emb") && e.contains("FORMAT CSV"), "got: {e}");
    }

    #[test]
    fn binary_payload_round_trips_through_binary_record_parser() {
        use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ok", DataType::Boolean, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("héllo🚀"), None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
            ],
        )
        .unwrap();
        let (chunks, rows) = binary_payload_from_batches(&schema, &[batch], &[0, 1, 2]).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(chunks.len(), 4); // header + 2 tuples + trailer
        assert_eq!(parse_binary_header(&chunks[0]).unwrap(), Some(19));
        let row0 = match parse_binary_record(&chunks[1], 3).unwrap() {
            BinaryRecord::Row(fields, consumed) => {
                assert_eq!(consumed, chunks[1].len());
                fields
            }
            _ => panic!("expected Row"),
        };
        let cells: Vec<Option<String>> = row0
            .iter()
            .zip(schema.fields().iter())
            .map(|(raw, f)| match raw {
                None => None,
                Some(b) => decode_binary_field(b, f).unwrap(),
            })
            .collect();
        assert_eq!(
            cells,
            vec![
                Some("1".to_string()),
                Some("héllo🚀".to_string()),
                Some("true".to_string())
            ]
        );
        let row1 = match parse_binary_record(&chunks[2], 3).unwrap() {
            BinaryRecord::Row(fields, _) => fields,
            _ => panic!("expected Row"),
        };
        assert_eq!(row1[1], None, "NULL name must be -1-length");
        assert!(matches!(
            parse_binary_record(&chunks[3], 3).unwrap(),
            BinaryRecord::Trailer
        ));
    }

    #[test]
    fn parse_copy_passes_through_non_copy_sql() {
        assert_eq!(parse_copy("SELECT 1").unwrap(), None);
        assert_eq!(parse_copy("INSERT INTO t VALUES (1)").unwrap(), None);
    }

    #[test]
    fn parse_copy_strips_trailing_semicolon() {
        let cmd = parse_copy("COPY t FROM STDIN;").unwrap();
        assert!(matches!(cmd, Some(CopyCommand::From { .. })));
    }

    #[test]
    fn split_record_handles_quoted_newline() {
        let buf = b"\"hello\nworld\",42\n";
        let (rec, n) = split_record(buf, false, '"').unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(rec, &buf[..buf.len() - 1]);
    }

    #[test]
    fn split_record_returns_none_on_partial() {
        let buf = b"abc,def";
        assert!(split_record(buf, false, '"').is_none());
        // …but final_chunk = true treats it as the final record.
        let (rec, n) = split_record(buf, true, '"').unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(rec, buf);
    }

    #[test]
    fn split_record_handles_crlf() {
        let buf = b"a,b\r\nc,d\r\n";
        let (rec, n) = split_record(buf, false, '"').unwrap();
        assert_eq!(n, 5);
        assert_eq!(rec, b"a,b");
    }

    #[test]
    fn parse_csv_record_splits_three_fields() {
        let r = parse_csv_record(b"1,foo,bar", ',', '"', "").unwrap();
        assert_eq!(
            r,
            vec![
                Some("1".to_string()),
                Some("foo".to_string()),
                Some("bar".to_string())
            ]
        );
    }

    #[test]
    fn parse_csv_record_empty_unquoted_is_null() {
        let r = parse_csv_record(b"1,,3", ',', '"', "").unwrap();
        assert_eq!(r, vec![Some("1".to_string()), None, Some("3".to_string())]);
    }

    #[test]
    fn parse_csv_record_empty_quoted_is_empty_string() {
        let r = parse_csv_record(b"1,\"\",3", ',', '"', "").unwrap();
        assert_eq!(
            r,
            vec![
                Some("1".to_string()),
                Some(String::new()),
                Some("3".to_string())
            ]
        );
    }

    #[test]
    fn parse_csv_record_handles_doubled_quote() {
        let r = parse_csv_record(b"\"he said \"\"hi\"\"\"", ',', '"', "").unwrap();
        assert_eq!(r, vec![Some("he said \"hi\"".to_string())]);
    }

    #[test]
    fn parse_csv_record_rejects_nul_byte() {
        let bytes = vec![b'a', 0u8, b'b'];
        let e = parse_csv_record(&bytes, ',', '"', "").unwrap_err();
        assert!(e.contains("NUL"), "got: {e}");
    }

    #[test]
    fn parse_csv_record_custom_delimiter() {
        let r = parse_csv_record(b"a|b|c", '|', '"', "").unwrap();
        assert_eq!(
            r,
            vec![Some("a".into()), Some("b".into()), Some("c".into())]
        );
    }

    #[test]
    fn parse_csv_record_custom_null_string() {
        let r = parse_csv_record(br"\N,foo,\N", ',', '"', r"\N").unwrap();
        assert_eq!(r, vec![None, Some("foo".into()), None]);
    }

    #[test]
    fn build_insert_sql_quotes_text_and_renders_int() {
        let cols = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ];
        let rec = vec![Some("42".to_string()), Some("it's me".to_string())];
        let sql = build_insert_sql("t", None, &cols, &rec).unwrap();
        assert_eq!(sql, "INSERT INTO t VALUES (42, 'it''s me')");
    }

    #[test]
    fn build_insert_sql_translates_null() {
        let cols = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ];
        let rec = vec![Some("1".to_string()), None];
        let sql = build_insert_sql("t", None, &cols, &rec).unwrap();
        assert_eq!(sql, "INSERT INTO t VALUES (1, NULL)");
    }

    #[test]
    fn build_insert_sql_with_column_list() {
        let cols = vec![
            Field::new("b", DataType::Utf8, false),
            Field::new("a", DataType::Int64, false),
        ];
        let list = vec!["b".to_string(), "a".to_string()];
        let rec = vec![Some("hi".to_string()), Some("7".to_string())];
        let sql = build_insert_sql("t", Some(&list), &cols, &rec).unwrap();
        assert_eq!(sql, "INSERT INTO t (b, a) VALUES ('hi', 7)");
    }

    #[test]
    fn select_copy_in_columns_subset_returns_listed_columns() {
        let table_cols = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false),
        ];
        let list = vec!["a".to_string(), "b".to_string()];
        let r = select_copy_in_columns("t", &table_cols, Some(&list)).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].name(), "a");
        assert_eq!(r[1].name(), "b");
    }

    #[test]
    fn select_copy_in_columns_omitting_required_defers_to_engine() {
        // The protocol layer no longer rejects an omitted NOT-NULL column: it
        // cannot see DEFAULT metadata (stripped by DataFusion projection), so a
        // SERIAL/defaulted column would be wrongly rejected here. Authoritative
        // NOT-NULL + DEFAULT enforcement is the engine ingest path's job. So a
        // column list that omits column "b" must pass this selection step.
        let table_cols = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false), // NOT NULL (default invisible here)
            Field::new("c", DataType::Int64, true),
        ];
        let list = vec!["a".to_string(), "c".to_string()];
        let r = select_copy_in_columns("t", &table_cols, Some(&list)).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].name(), "a");
        assert_eq!(r[1].name(), "c");
    }

    #[test]
    fn select_copy_in_columns_unknown_column_rejects() {
        let table_cols = vec![Field::new("a", DataType::Int64, true)];
        let list = vec!["nope".to_string()];
        let e = select_copy_in_columns("t", &table_cols, Some(&list)).unwrap_err();
        assert!(e.contains("does not exist"), "got: {e}");
    }

    /// Env vars are process-global; tests that mutate them must serialise
    /// through this mutex, otherwise parallel cargo test runners stomp each other.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn validate_copy_path_default_deny_no_flag() {
        let _g = env_lock();
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
        let e = validate_copy_path("/tmp/x.csv").unwrap_err();
        // Should contain the 42501 prefix (insufficient_privilege)
        assert!(e.contains("42501") || e.contains("disabled"), "got: {e}");
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
    }

    #[test]
    fn validate_copy_path_flag_set_but_no_allowlist() {
        let _g = env_lock();
        std::env::set_var(COPY_ALLOW_FILE_PATHS_ENV, "1");
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
        let e = validate_copy_path("/tmp/x.csv").unwrap_err();
        assert!(
            e.contains("disabled") || e.contains(COPY_PATH_ALLOWLIST_ENV),
            "got: {e}"
        );
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
    }

    #[test]
    fn validate_copy_path_relative_rejected() {
        let _g = env_lock();
        std::env::set_var(COPY_ALLOW_FILE_PATHS_ENV, "1");
        std::env::set_var(COPY_PATH_ALLOWLIST_ENV, "/tmp");
        let e = validate_copy_path("rel/path.csv").unwrap_err();
        assert!(e.contains("absolute"), "got: {e}");
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
    }

    #[test]
    fn validate_copy_path_inside_allowlist_ok() {
        let _g = env_lock();
        std::env::set_var(COPY_ALLOW_FILE_PATHS_ENV, "1");
        std::env::set_var(COPY_PATH_ALLOWLIST_ENV, "/tmp/basin-copy");
        let p = validate_copy_path("/tmp/basin-copy/x.csv").unwrap();
        assert_eq!(p.to_str(), Some("/tmp/basin-copy/x.csv"));
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
    }

    #[test]
    fn validate_copy_path_outside_allowlist_rejected() {
        let _g = env_lock();
        std::env::set_var(COPY_ALLOW_FILE_PATHS_ENV, "1");
        std::env::set_var(COPY_PATH_ALLOWLIST_ENV, "/tmp/basin-copy");
        let e = validate_copy_path("/etc/passwd").unwrap_err();
        assert!(e.contains("outside"), "got: {e}");
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
    }

    #[test]
    fn validate_copy_path_rejects_dotdot() {
        let _g = env_lock();
        std::env::set_var(COPY_ALLOW_FILE_PATHS_ENV, "1");
        std::env::set_var(COPY_PATH_ALLOWLIST_ENV, "/tmp");
        let e = validate_copy_path("/tmp/../etc/passwd").unwrap_err();
        assert!(e.contains(".."), "got: {e}");
        std::env::remove_var(COPY_ALLOW_FILE_PATHS_ENV);
        std::env::remove_var(COPY_PATH_ALLOWLIST_ENV);
    }

    #[test]
    fn csv_encode_quotes_when_needed() {
        let mut s = String::new();
        csv_encode_into("hello,world", &mut s, ',', '"');
        assert_eq!(s, "\"hello,world\"");

        let mut s = String::new();
        csv_encode_into("she said \"hi\"", &mut s, ',', '"');
        assert_eq!(s, "\"she said \"\"hi\"\"\"");

        let mut s = String::new();
        csv_encode_into("plain", &mut s, ',', '"');
        assert_eq!(s, "plain");
    }

    #[test]
    fn csv_encode_custom_delimiter() {
        let mut s = String::new();
        csv_encode_into("hello|world", &mut s, '|', '"');
        assert_eq!(s, "\"hello|world\"");
    }
}
