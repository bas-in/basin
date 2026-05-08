//! `COPY FROM STDIN` and `COPY TO STDOUT` for the simple-query path.
//!
//! v0.1: CSV format only, comma-delimited, RFC 4180-ish. No binary, no custom
//! delimiters, no NULL specifications, no column lists. Anything else is
//! rejected with `42601` syntax_error before we touch the engine.
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
use pgwire::messages::copy::{CopyData, CopyInResponse, CopyOutResponse};
use pgwire::messages::response::{CommandComplete, ReadyForQuery, TransactionStatus};
use pgwire::messages::PgWireBackendMessage;
use pgwire::error::ErrorInfo;

use crate::protocol::Session;

/// A parsed `COPY` statement. The simple-query path branches on this before
/// running anything against the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CopyCommand {
    From {
        table: String,
        with_header: bool,
    },
    To {
        table: String,
        with_header: bool,
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
    /// engine returns from a `prepare("SELECT * FROM <table>")`.
    pub(crate) columns: Vec<Field>,
    /// Bytes received but not yet split into a complete row.
    pub(crate) buffer: Vec<u8>,
    /// Successful INSERTs since the COPY started.
    pub(crate) row_count: u64,
    /// First error observed; if present, every subsequent CopyData chunk is
    /// drained without further engine work.
    pub(crate) error: Option<String>,
    /// `WITH (HEADER true)` — skip the first row.
    pub(crate) header_pending: bool,
}

impl CopyInState {
    pub(crate) fn new(table: String, columns: Vec<Field>, with_header: bool) -> Self {
        Self {
            table,
            columns,
            buffer: Vec::new(),
            row_count: 0,
            error: None,
            header_pending: with_header,
        }
    }
}

/// Result of attempting to parse one of the `COPY` shapes we support.
///
/// `Ok(Some(_))` = matched a supported shape; `Ok(None)` = SQL doesn't start
/// with `COPY`, fall through to normal query handling; `Err(_)` = SQL starts
/// with `COPY` but doesn't match the v0.1 grammar — caller surfaces an
/// ErrorResponse.
pub(crate) fn parse_copy(sql: &str) -> std::result::Result<Option<CopyCommand>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut sc = Scanner::new(trimmed);
    if !sc.eat_keyword("COPY") {
        return Ok(None);
    }
    let table = sc.eat_ident().ok_or_else(|| {
        "expected table name after COPY (column lists not supported in v0.1)".to_owned()
    })?;
    if sc.peek_punct('(') {
        return Err(
            "COPY <table> (col, ...) column lists are not supported in v0.1".into(),
        );
    }
    let direction = if sc.eat_keyword("FROM") {
        Direction::From
    } else if sc.eat_keyword("TO") {
        Direction::To
    } else {
        return Err("expected FROM or TO after COPY <table>".into());
    };
    match direction {
        Direction::From => {
            if !sc.eat_keyword("STDIN") {
                return Err(
                    "only COPY FROM STDIN is supported in v0.1 (file paths and PROGRAM are not)"
                        .into(),
                );
            }
        }
        Direction::To => {
            if !sc.eat_keyword("STDOUT") {
                return Err(
                    "only COPY TO STDOUT is supported in v0.1 (file paths and PROGRAM are not)"
                        .into(),
                );
            }
        }
    }
    let with_header = parse_with_options(&mut sc)?;
    sc.skip_whitespace();
    if !sc.is_done() {
        return Err(format!("unexpected trailing input after COPY: {:?}", sc.rest()));
    }
    Ok(Some(match direction {
        Direction::From => CopyCommand::From {
            table,
            with_header,
        },
        Direction::To => CopyCommand::To {
            table,
            with_header,
        },
    }))
}

enum Direction {
    From,
    To,
}

/// Parse the optional `WITH (FORMAT CSV [, HEADER {true|false}])`. Anything
/// else (DELIMITER, NULL, QUOTE, ESCAPE, FORCE_*, ENCODING) is rejected.
/// Returns the `header` value (default false).
fn parse_with_options(sc: &mut Scanner<'_>) -> std::result::Result<bool, String> {
    sc.skip_whitespace();
    if !sc.eat_keyword("WITH") {
        // Plain `COPY t FROM STDIN` with no options — accept and treat as CSV.
        return Ok(false);
    }
    sc.skip_whitespace();
    if !sc.eat_punct('(') {
        return Err("expected '(' after WITH".into());
    }
    let mut saw_format_csv = false;
    let mut header = false;
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
                    return Err(format!(
                        "only FORMAT CSV is supported in v0.1, got {v:?}"
                    ));
                }
                saw_format_csv = true;
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
            other => {
                return Err(format!(
                    "COPY option {other:?} is not supported in v0.1 (CSV only, no DELIMITER/NULL/QUOTE/ESCAPE/FORCE_*)"
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
    // We accept WITH (HEADER true) without an explicit FORMAT — implicit CSV
    // for v0.1. If the user wrote nothing, we already returned above.
    let _ = saw_format_csv;
    Ok(header)
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
        let consumed = match split_record(&state.buffer, final_chunk) {
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
        let record = match parse_csv_record(record_bytes) {
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
        let sql = match build_insert_sql(&state.table, &state.columns, &record) {
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
fn split_record(buf: &[u8], final_chunk: bool) -> Option<(&[u8], usize)> {
    let mut in_quotes = false;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if in_quotes {
            if b == b'"' {
                // Doubled quote stays inside the field, single quote ends it.
                if i + 1 < buf.len() && buf[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => {
                in_quotes = true;
                i += 1;
            }
            b'\n' => {
                // Trim the optional preceding \r.
                let end = if i > 0 && buf[i - 1] == b'\r' { i - 1 } else { i };
                return Some((&buf[..end], i + 1));
            }
            _ => {
                i += 1;
            }
        }
    }
    if final_chunk && !buf.is_empty() && !in_quotes {
        // Tail without trailing newline. Strip a trailing \r if present.
        let end = if buf.last() == Some(&b'\r') { buf.len() - 1 } else { buf.len() };
        Some((&buf[..end], buf.len()))
    } else {
        None
    }
}

/// Parse a single CSV record's bytes into a vec of fields. Each field is
/// either `None` (unquoted empty cell → SQL NULL) or `Some(String)`. RFC
/// 4180-ish: comma-delimited, double-quote = `""`, fields starting with `"`
/// are quoted.
fn parse_csv_record(bytes: &[u8]) -> std::result::Result<Vec<Option<String>>, String> {
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
        // (possibly empty, possibly past-end on the trailing comma case).
        if i == chars.len() {
            // Empty trailing field after a comma, or an entirely empty row.
            if !out.is_empty() {
                out.push(None);
            }
            break;
        }
        if chars[i] == '"' {
            // Quoted field.
            let mut field = String::new();
            i += 1;
            loop {
                if i >= chars.len() {
                    return Err("unterminated quoted field".into());
                }
                if chars[i] == '"' {
                    if i + 1 < chars.len() && chars[i + 1] == '"' {
                        field.push('"');
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
            // After closing quote: expect comma or end.
            if i == chars.len() {
                break;
            }
            if chars[i] != ',' {
                return Err(format!("expected ',' after quoted field, got {:?}", chars[i]));
            }
            i += 1;
        } else {
            // Unquoted field: read until comma.
            let start = i;
            while i < chars.len() && chars[i] != ',' {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect();
            // Postgres CSV: empty unquoted = NULL. A quoted empty would
            // have been Some("") via the quoted branch.
            if raw.is_empty() {
                out.push(None);
            } else {
                out.push(Some(raw));
            }
            if i == chars.len() {
                break;
            }
            // i points to comma; advance to next field.
            i += 1;
        }
    }
    Ok(out)
}

/// Build `INSERT INTO <table> VALUES (v1, v2, ...)` from one CSV record. Each
/// value is rendered per its target column type:
/// - numeric / bool: bare token (parsed by the engine's literal scanner)
/// - text / json / uuid / bytea: single-quoted with `'` doubled
/// - empty unquoted CSV cell: `NULL`
fn build_insert_sql(
    table: &str,
    columns: &[Field],
    record: &[Option<String>],
) -> std::result::Result<String, String> {
    let mut sql = String::with_capacity(64 + record.len() * 16);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
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
fn render_literal(
    v: &str,
    field: &Field,
    out: &mut String,
) -> std::result::Result<(), String> {
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
    with_header: bool,
) -> Vec<PgWireBackendMessage> {
    let mut out = Vec::new();
    let res = session
        .execute(&format!("SELECT * FROM {table}"))
        .await;
    let (schema, batches) = match res {
        Ok(ExecResult::Rows { schema, batches }) => (schema, batches),
        Ok(ExecResult::Empty { .. }) => {
            // Shouldn't happen for SELECT, but treat as zero-row table.
            let info = ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("COPY TO STDOUT: SELECT * FROM {table} returned no result set"),
            );
            out.push(PgWireBackendMessage::ErrorResponse(info.into()));
            out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )));
            return out;
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
    };
    let n_cols = schema.fields().len();
    out.push(PgWireBackendMessage::CopyOutResponse(copy_out_response(n_cols)));
    if with_header {
        let mut row = String::new();
        for (i, f) in schema.fields().iter().enumerate() {
            if i > 0 {
                row.push(',');
            }
            csv_encode_into(f.name(), &mut row);
        }
        row.push('\n');
        out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
            row.into_bytes(),
        ))));
    }
    let mut row_count: u64 = 0;
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut row = String::new();
            for c in 0..n_cols {
                if c > 0 {
                    row.push(',');
                }
                let col = batch.column(c);
                let cell = render_csv_cell(col.as_ref(), r, schema.field(c));
                csv_encode_into(&cell, &mut row);
            }
            row.push('\n');
            out.push(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
                row.into_bytes(),
            ))));
            row_count += 1;
        }
    }
    out.push(PgWireBackendMessage::CopyDone(
        pgwire::messages::copy::CopyDone::new(),
    ));
    out.push(PgWireBackendMessage::CommandComplete(
        CommandComplete::new(format!("COPY {row_count}")),
    ));
    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    out
}

/// CSV-encode a single string field into `out`. Quote when the value
/// contains `,`, `"`, `\n`, or `\r`; double internal `"`. Empty string is
/// written without quotes (empty-unquoted) — the COPY-IN reader will round-
/// trip it to NULL by default. For now we emit empty for SQL NULL too,
/// matching Postgres' CSV default.
fn csv_encode_into(v: &str, out: &mut String) {
    let needs_quote = v
        .as_bytes()
        .iter()
        .any(|&b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');
    if !needs_quote {
        out.push_str(v);
        return;
    }
    out.push('"');
    for c in v.chars() {
        if c == '"' {
            out.push('"');
            out.push('"');
        } else {
            out.push(c);
        }
    }
    out.push('"');
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
    vec![PgWireBackendMessage::CopyInResponse(copy_in_response(n_cols))]
}

/// Per-connection mutex around an in-flight `COPY FROM STDIN`. The
/// simple-query handler pushes one in here when it sends `CopyInResponse`;
/// `CopyHandler::on_copy_data` / `on_copy_done` pop / mutate it.
pub(crate) type CopyStateSlot = Arc<tokio::sync::Mutex<Option<CopyInState>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_copy_recognises_from_stdin_csv() {
        let cmd = parse_copy("COPY events FROM STDIN WITH (FORMAT CSV)").unwrap();
        assert_eq!(
            cmd,
            Some(CopyCommand::From {
                table: "events".into(),
                with_header: false
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
                with_header: true
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
                with_header: false
            })
        );
    }

    #[test]
    fn parse_copy_rejects_column_list() {
        let e = parse_copy("COPY t (a, b) FROM STDIN").unwrap_err();
        assert!(e.contains("column list"), "got: {e}");
    }

    #[test]
    fn parse_copy_rejects_delimiter_option() {
        let e = parse_copy("COPY t FROM STDIN WITH (FORMAT CSV, DELIMITER '|')").unwrap_err();
        assert!(e.contains("DELIMITER") || e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn parse_copy_rejects_non_csv_format() {
        let e = parse_copy("COPY t FROM STDIN WITH (FORMAT BINARY)").unwrap_err();
        assert!(e.contains("CSV"), "got: {e}");
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
        let (rec, n) = split_record(buf, false).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(rec, &buf[..buf.len() - 1]);
    }

    #[test]
    fn split_record_returns_none_on_partial() {
        let buf = b"abc,def";
        assert!(split_record(buf, false).is_none());
        // …but final_chunk = true treats it as the final record.
        let (rec, n) = split_record(buf, true).unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(rec, buf);
    }

    #[test]
    fn split_record_handles_crlf() {
        let buf = b"a,b\r\nc,d\r\n";
        let (rec, n) = split_record(buf, false).unwrap();
        assert_eq!(n, 5);
        assert_eq!(rec, b"a,b");
    }

    #[test]
    fn parse_csv_record_splits_three_fields() {
        let r = parse_csv_record(b"1,foo,bar").unwrap();
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
        let r = parse_csv_record(b"1,,3").unwrap();
        assert_eq!(
            r,
            vec![Some("1".to_string()), None, Some("3".to_string())]
        );
    }

    #[test]
    fn parse_csv_record_empty_quoted_is_empty_string() {
        let r = parse_csv_record(b"1,\"\",3").unwrap();
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
        let r = parse_csv_record(b"\"he said \"\"hi\"\"\"").unwrap();
        assert_eq!(r, vec![Some("he said \"hi\"".to_string())]);
    }

    #[test]
    fn parse_csv_record_rejects_nul_byte() {
        let bytes = vec![b'a', 0u8, b'b'];
        let e = parse_csv_record(&bytes).unwrap_err();
        assert!(e.contains("NUL"), "got: {e}");
    }

    #[test]
    fn build_insert_sql_quotes_text_and_renders_int() {
        let cols = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ];
        let rec = vec![Some("42".to_string()), Some("it's me".to_string())];
        let sql = build_insert_sql("t", &cols, &rec).unwrap();
        assert_eq!(sql, "INSERT INTO t VALUES (42, 'it''s me')");
    }

    #[test]
    fn build_insert_sql_translates_null() {
        let cols = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ];
        let rec = vec![Some("1".to_string()), None];
        let sql = build_insert_sql("t", &cols, &rec).unwrap();
        assert_eq!(sql, "INSERT INTO t VALUES (1, NULL)");
    }

    #[test]
    fn csv_encode_quotes_when_needed() {
        let mut s = String::new();
        csv_encode_into("hello,world", &mut s);
        assert_eq!(s, "\"hello,world\"");

        let mut s = String::new();
        csv_encode_into("she said \"hi\"", &mut s);
        assert_eq!(s, "\"she said \"\"hi\"\"\"");

        let mut s = String::new();
        csv_encode_into("plain", &mut s);
        assert_eq!(s, "plain");
    }
}
