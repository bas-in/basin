//! `COPY FROM STDIN` and `COPY TO STDOUT` for the simple-query path.
//!
//! v0.2: CSV format; delimiter/null/quote/escape configurable via WITH options.
//! Column-list (`COPY t (a, b) ...`) is supported. Server-side file-path
//! variants (`COPY t FROM '/abs/path'` / `COPY t TO '/abs/path'`) are gated
//! by two env vars: `BASIN_COPY_ALLOW_FILE_PATHS=1` (primary on/off gate) and
//! `BASIN_COPY_PATH_ALLOWLIST` (colon-separated directory allowlist, required
//! when file paths are enabled). Query-source COPY (`COPY (SELECT …) TO
//! STDOUT`) is supported. `COPY … FROM PROGRAM '…'` is rejected with SQLSTATE
//! 42501. BINARY format rejected with `42601`.
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

use arrow_schema::{DataType, Field};
use basin_engine::ExecResult;
use bytes::Bytes;
use pgwire::error::ErrorInfo;
use pgwire::messages::copy::{CopyData, CopyInResponse, CopyOutResponse};
use pgwire::messages::response::{CommandComplete, ReadyForQuery, TransactionStatus};
use pgwire::messages::PgWireBackendMessage;

use crate::protocol::Session;

/// CSV format options carried with a COPY statement.
///
/// Defaults match Postgres CSV mode: comma delimiter, empty-string NULL,
/// double-quote character for quoting, and same char for escaping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyOptions {
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
    /// Field shape we use to render INSERT VALUES — same Arrow `Field`s the
    /// engine returns from a `prepare("SELECT * FROM <table>")`. When a
    /// column list was supplied, this carries only those listed columns
    /// (in list order). When no column list was supplied, this carries
    /// the full table schema.
    pub(crate) columns: Vec<Field>,
    /// User-supplied column list, if any. Populated only when the COPY
    /// statement included `(col1, col2, ...)`. Used to build
    /// `INSERT INTO t (col1, col2) VALUES (...)`.
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
}

impl CopyInState {
    pub(crate) fn new(table: String, columns: Vec<Field>, with_header: bool, opts: CopyOptions) -> Self {
        Self {
            table,
            columns,
            column_list: None,
            buffer: Vec::new(),
            row_count: 0,
            error: None,
            header_pending: with_header,
            opts,
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
                return Err(
                    "expected STDIN or '<absolute-path>' after COPY <table> FROM"
                        .into(),
                );
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
                return Err(
                    "expected STDOUT or '<absolute-path>' after COPY <table> TO"
                        .into(),
                );
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
            b'\'' => { in_single_quote = true; sc.pos += 1; }
            b'"' => { in_double_quote = true; sc.pos += 1; }
            b'(' => { depth += 1; sc.pos += 1; }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let inner = sc.s[start..sc.pos].trim().to_owned();
                    sc.pos += 1; // consume the closing ')'
                    return Ok(inner);
                }
                sc.pos += 1;
            }
            _ => { sc.pos += 1; }
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

/// Parse the optional `WITH (FORMAT CSV [, HEADER {true|false} [, DELIMITER 'c'
/// [, NULL 's'] [, QUOTE 'c'] [, ESCAPE 'c']]])`. BINARY format is rejected.
/// FORCE_*, ENCODING are rejected. Returns `(header, CopyOptions)`.
///
/// Also accepts the legacy `WITH CSV [HEADER]` shorthand (no parens) that
/// older Postgres clients still emit.
fn parse_with_options(sc: &mut Scanner<'_>) -> std::result::Result<(bool, CopyOptions), String> {
    sc.skip_whitespace();
    if !sc.eat_keyword("WITH") {
        // Plain `COPY t FROM STDIN` with no options — accept and treat as CSV.
        return Ok((false, CopyOptions::default()));
    }
    sc.skip_whitespace();
    if !sc.eat_punct('(') {
        // Legacy `WITH CSV [HEADER]` form.
        if !sc.eat_keyword("CSV") {
            return Err("expected '(' after WITH, or legacy 'WITH CSV [HEADER]' shorthand".into());
        }
        let mut header = false;
        sc.skip_whitespace();
        if sc.eat_keyword("HEADER") {
            header = true;
        }
        return Ok((header, CopyOptions::default()));
    }
    let mut header = false;
    let mut opts = CopyOptions::default();
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
                if !v.eq_ignore_ascii_case("csv") {
                    return Err(format!("only FORMAT CSV is supported; BINARY and other formats are not implemented (got {v:?})"));
                }
            }
            "HEADER" => {
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
                sc.skip_whitespace();
                opts.null_string = parse_string_literal(sc)?;
            }
            "QUOTE" => {
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
    Ok((header, opts))
}

/// Hand-rolled scanner over the SQL string. Whitespace-aware, ASCII-only
/// keywords (case-insensitive), bare identifier (no quoted ident in v0.1).
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

    fn eat_ident(&mut self) -> Option<String> {
        self.skip_whitespace();
        let bytes = self.s.as_bytes();
        let start = self.pos;
        if start >= bytes.len() {
            return None;
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

/// Metadata key carrying the source text of a column's `DEFAULT <expr>`.
/// Mirrors `basin_engine::types::BASIN_COLUMN_DEFAULT` (kept private in the
/// engine crate); the engine's INSERT path reads the metadata back during
/// row coercion. We rely on the same key here to detect "column has a
/// default" without taking on a dep on the engine's internal module.
const BASIN_COLUMN_DEFAULT_KEY: &str = "BASIN_COLUMN_DEFAULT";

fn field_has_default(f: &Field) -> bool {
    f.metadata().contains_key(BASIN_COLUMN_DEFAULT_KEY)
}

/// Given the full table schema and an optional user-supplied column list,
/// return the subset of `Field`s the COPY-IN reader should expect (in column-
/// list order). For COPY FROM with a column list, also validate that every
/// column NOT in the list is either nullable or has a DEFAULT — otherwise
/// the row would have nothing to put there. Each error returns SQLSTATE
/// 42601 at the call site.
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
    // Every NOT NULL column without a DEFAULT must be in the list.
    for f in table_columns {
        if listed_names.contains(&f.name().to_ascii_lowercase()) {
            continue;
        }
        if !f.is_nullable() && !field_has_default(f) {
            return Err(format!(
                "COPY: column \"{}\" cannot be NULL and has no default",
                f.name()
            ));
        }
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

/// Build `CopyInResponse` for a CSV/text-format copy with `n` columns.
pub(crate) fn copy_in_response(n: usize) -> CopyInResponse {
    CopyInResponse::new(0, n as i16, vec![0; n])
}

pub(crate) fn copy_out_response(n: usize) -> CopyOutResponse {
    CopyOutResponse::new(0, n as i16, vec![0; n])
}

/// Process every complete CSV record currently buffered in `state`.
///
/// "Complete" = terminated by `\n` (or `\r\n`), with quoted fields allowed
/// to contain unescaped newlines. Trailing partial bytes stay in the
/// buffer for the next chunk.
///
/// Stops processing on the first engine error, which gets latched into
/// `state.error`. Subsequent calls fall through (drain mode).
pub(crate) async fn process_buffered_rows<S: Session + ?Sized>(
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
            None => return,
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
        let record = match parse_csv_record(record_bytes, state.opts.delimiter, state.opts.quote, &state.opts.null_string) {
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
        let sql = match build_insert_sql(
            &state.table,
            state.column_list.as_deref(),
            &state.columns,
            &record,
        ) {
            Ok(s) => s,
            Err(e) => {
                state.error = Some(e);
                state.buffer.clear();
                return;
            }
        };
        // Execute synchronously. Any engine error halts further inserts but
        // not the COPY-IN drain — see module-level "protocol-state-machine
        // guarantee".
        match session.execute(&sql).await {
            Ok(ExecResult::Empty { .. }) | Ok(ExecResult::Rows { .. }) => {
                state.row_count += 1;
            }
            Err(e) => {
                state.error = Some(format!("COPY row {}: {e}", state.row_count + 1));
                state.buffer.clear();
                return;
            }
        }
        state.buffer.drain(..n);
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
    let qb = if quote_char.is_ascii() { quote_char as u8 } else { b'"' };
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
                    delimiter,
                    chars[i]
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
    let column_indices: Vec<usize> = match column_list {
        Some(list) => {
            let mut out = Vec::with_capacity(list.len());
            for name in list {
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        basin_common::BasinError::Internal(format!(
                            "COPY TO: column {name:?} not in result schema"
                        ))
                    })?;
                out.push(idx);
            }
            out
        }
        None => (0..schema.fields().len()).collect(),
    };
    let header = if with_header {
        let mut row = String::new();
        for (i, &idx) in column_indices.iter().enumerate() {
            if i > 0 {
                row.push(opts.delimiter);
            }
            csv_encode_into(schema.field(idx).name(), &mut row, opts.delimiter, opts.quote);
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
    let payload = match copy_to_csv_payload(session, table, column_list, with_header, opts).await {
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
    };
    let (header, body, row_count) = payload;
    out.push(PgWireBackendMessage::CopyOutResponse(copy_out_response(
        n_cols,
    )));
    if let Some(h) = header {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            h,
        ))));
    }
    for row in body {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            row,
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
/// streams the result as CSV. Returns the full backend-message sequence.
pub(crate) async fn copy_query_to_stdout_messages<S: Session + ?Sized>(
    session: &S,
    query: &str,
    with_header: bool,
    opts: &CopyOptions,
) -> Vec<PgWireBackendMessage> {
    let mut out: Vec<PgWireBackendMessage> = Vec::new();
    let payload = match copy_query_to_csv_payload(session, query, with_header, opts).await {
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
    };
    let (header, body, row_count) = payload;
    // For query-source COPY we don't know the column count ahead of running
    // the query. Derive it from the header or body row, or default to 1.
    let n_cols = if let Some(ref h) = header {
        h.iter().filter(|&&b| b == opts.delimiter as u8).count() + 1
    } else if let Some(first) = body.first() {
        first.iter().filter(|&&b| b == opts.delimiter as u8).count() + 1
    } else {
        1
    };
    out.push(PgWireBackendMessage::CopyOutResponse(copy_out_response(
        n_cols,
    )));
    if let Some(h) = header {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(h))));
    }
    for row in body {
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(row))));
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
    let needs_quote = v.chars().any(|c| c == delimiter || c == quote || c == '\n' || c == '\r');
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
    if col.is_null(idx) {
        // CSV NULL = empty unquoted by default in Postgres CSV mode.
        return String::new();
    }
    crate::types::render_cell_for_copy(col, idx, field)
}

/// Build the simple-query message sequence for a `COPY FROM STDIN` *response*.
/// The connection state must be flipped to `CopyInProgress` after sending
/// `CopyInResponse` so the framework routes subsequent `CopyData` /
/// `CopyDone` to the `CopyHandler`.
pub(crate) fn copy_from_stdin_messages(n_cols: usize) -> Vec<PgWireBackendMessage> {
    vec![PgWireBackendMessage::CopyInResponse(copy_in_response(
        n_cols,
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
        let cmd = parse_copy("COPY t FROM STDIN WITH (FORMAT CSV, QUOTE '\"', ESCAPE '\\')").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::From { ref opts, .. }) if opts.quote == '"' && opts.escape == '\\'),
            "expected custom quote/escape"
        );
    }

    #[test]
    fn parse_copy_accepts_query_source() {
        let cmd = parse_copy("COPY (SELECT id, name FROM t WHERE id > 5) TO STDOUT WITH (FORMAT CSV)").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::QueryTo { ref query, with_header, .. })
                if query.contains("SELECT") && !with_header),
            "expected QueryTo"
        );
    }

    #[test]
    fn parse_copy_accepts_query_source_with_header() {
        let cmd = parse_copy("COPY (SELECT * FROM t) TO STDOUT WITH (FORMAT CSV, HEADER true)").unwrap();
        assert!(
            matches!(cmd, Some(CopyCommand::QueryTo { with_header: true, .. })),
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
    fn parse_copy_rejects_non_csv_format() {
        let e = parse_copy("COPY t FROM STDIN WITH (FORMAT BINARY)").unwrap_err();
        assert!(e.contains("CSV") || e.contains("BINARY"), "got: {e}");
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
    fn select_copy_in_columns_subset_with_default_ok() {
        use std::collections::HashMap;
        let mut md = HashMap::new();
        md.insert(BASIN_COLUMN_DEFAULT_KEY.to_string(), "42".to_string());
        let table_cols = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false).with_metadata(md),
        ];
        let list = vec!["a".to_string(), "b".to_string()];
        let r = select_copy_in_columns("t", &table_cols, Some(&list)).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].name(), "a");
        assert_eq!(r[1].name(), "b");
    }

    #[test]
    fn select_copy_in_columns_missing_required_rejects() {
        let table_cols = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false), // NOT NULL, no default
            Field::new("c", DataType::Int64, true),
        ];
        let list = vec!["a".to_string(), "c".to_string()];
        let e = select_copy_in_columns("t", &table_cols, Some(&list)).unwrap_err();
        assert!(e.contains("\"b\""), "got: {e}");
        assert!(e.contains("cannot be NULL"), "got: {e}");
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
        assert!(e.contains("disabled") || e.contains(COPY_PATH_ALLOWLIST_ENV), "got: {e}");
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
