//! Literal-VALUES fast scanner for bulk `INSERT ... VALUES (...), (...), ...`.
//!
//! ## Why this exists
//!
//! The general INSERT path runs the whole statement through `sqlparser`, which
//! builds ~6 `Expr` AST nodes per literal per row, then [`crate::dml::batch_from_rows`]
//! coerces each `Expr` cell back into an Arrow value. For a multi-row TEXT
//! `INSERT` (what bulk loaders and the benchmark send) the SQL front dominates:
//! a 10k-row VALUES list allocates tens of thousands of boxed AST nodes that are
//! immediately torn back down into primitive Arrow buffers. This module skips
//! the AST entirely for the *common literal shape* — it hand-tokenizes the
//! `VALUES` tail and writes Arrow arrays column-by-column.
//!
//! ## Correctness contract (read before touching this file)
//!
//! This is a **pure fast path with a guaranteed fallback**. The caller has
//! already parsed the statement header (table, column list, ON CONFLICT, …)
//! with `sqlparser`; this scanner only re-scans the raw `VALUES` tail. The
//! cardinal rule:
//!
//! > **When in doubt, return `None`.** A `None` return means "I refuse to
//! > handle this — use the slow path", and the slow path is the existing,
//! > well-tested `batch_from_rows` pipeline. The scanner must NEVER mis-parse a
//! > value: on *any* ambiguity, unexpected byte, unsupported token, malformed
//! > structure, or column type outside the supported set, it returns `None`
//! > rather than guessing. It must also never panic on adversarial input.
//!
//! Because the scanner produces the *same* full-width [`RecordBatch`] columns
//! that `batch_from_rows` would, the downstream pipeline (constraint
//! enforcement, generated columns, write) is byte-for-byte identical between
//! the two paths.
//!
//! ## Grammar accepted (everything else → `None`)
//!
//! ```text
//!   tail        := WS* '(' tuple ')' ( WS* ',' WS* '(' tuple ')' )* WS* ';'? WS* EOF
//!   tuple       := WS* value ( WS* ',' WS* value )* WS*
//!   value       := int | float | string | 'NULL' | 'TRUE' | 'FALSE'
//!   int         := '-'? DIGIT+
//!   float       := '-'? DIGIT+ ('.' DIGIT*)? ([eE] [+-]? DIGIT+)?   (must contain '.' or exponent)
//!   string      := '\'' ( [^'] | '\'\'' )* '\''                     (PG single-quote, '' escapes ')
//! ```
//!
//! Supported per-tuple arity is exactly `insert_cols.len()`; a tuple with the
//! wrong number of values → `None`.
//!
//! ## Hard `None` conditions
//!
//! * Any column in `schema` that a tuple value lands in has an Arrow type
//!   outside {Int16, Int32, Int64, Float32, Float64, Utf8 (plain TEXT/VARCHAR),
//!   Boolean, JSONB (LargeBinary + JSONB marker), Timestamp(Microsecond, tz?)}.
//!   Decorated Utf8 columns (INET, UUID-on-Utf8, BIT, TSVECTOR, …) are rejected
//!   — they carry validation the scanner deliberately does not replicate. JSONB
//!   and Timestamp cells are coerced through the *same* helpers the slow path
//!   uses (`dml::coerce_jsonb_str`, `dml::parse_timestamp_string`), so the bytes
//!   they produce are byte-identical; any coercion failure (invalid JSON,
//!   unparseable timestamp) returns `None` and the slow path surfaces the
//!   canonical error.
//! * Any token that isn't an integer / float / single-quoted string / NULL /
//!   TRUE / FALSE: casts (`::`), function calls, `$`-params, dollar-quoted
//!   strings, `ARRAY[...]`, nested parens, identifiers, E'' / N'' / x''
//!   prefixed strings, double-quoted strings.
//! * A value whose token kind is incompatible with its destination column type
//!   (e.g. a string literal for an Int64 column, a float for an Int column).
//! * NULL routed at a NOT NULL column.
//! * Structural malformation: missing/extra commas, unbalanced parens,
//!   unterminated string, trailing comma, trailing garbage after the last
//!   tuple, integer/float that overflows the destination width.
//!
//! The caller is responsible for the *other* preconditions (no ON CONFLICT, no
//! OVERRIDING clause, every omitted column is a plain nullable column with no
//! DEFAULT/IDENTITY/generated filling required) — see the executor hook.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};

/// Rows per emitted [`RecordBatch`]. Matches the typical compaction/IPC chunk
/// size; large enough to amortise per-batch overhead, small enough to keep peak
/// memory bounded on a million-row statement.
const BATCH_ROWS: usize = 8192;

/// A single scanned cell, kept type-tagged so we can dispatch the per-column
/// Arrow build once we know the destination type. Strings borrow from the
/// source SQL when no `''` un-escaping is needed (the overwhelmingly common
/// case), and own a `String` only when an escape was collapsed.
enum Cell<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(std::borrow::Cow<'a, str>),
}

/// Executor entry point: decide eligibility from the parsed INSERT header and,
/// if eligible, scan the literal `VALUES` tail into Arrow batches.
///
/// Returns `Some(batches)` only when the statement is a plain literal multi-row
/// VALUES insert that the fast scanner can reproduce *byte-identically* to the
/// slow path. Returns `None` for anything the caller should route through
/// `batch_from_rows` (the safe, well-tested path). The caller must still have
/// excluded ON CONFLICT and OVERRIDING before calling — those are statement-AST
/// facts the scanner can't see in the raw tail.
///
/// Eligibility (all must hold, else `None`):
/// * Every schema column targeted by the insert column list has a scanner-
///   supported Arrow type (see [`is_supported_col`]).
/// * Every schema column NOT targeted is nullable AND carries no DEFAULT, no
///   IDENTITY/SERIAL sequence, and is not a generated column — i.e. the only
///   correct fill for an omitted column is a plain NULL (which is exactly what
///   `expand_insert_rows` would produce). If an omitted column needs server-
///   side filling, we must use the slow path so identity/default logic runs.
/// * No targeted column is generated (writing to a generated column is an
///   error the slow path raises with the canonical message).
///
/// `insert_columns` is the parsed column list (`INSERT INTO t (a, b, ...)`);
/// when empty, the implicit target is every column in declaration order.
//
// Live entry point: called from `exec_insert` (executor.rs) for plain
// multi-row literal VALUES inserts.
pub(crate) fn try_fast_insert(
    sql: &str,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
) -> Option<Vec<RecordBatch>> {
    let n_cols = schema.fields().len();

    // Resolve the per-tuple-position → schema-column-index mapping.
    let insert_cols: Vec<usize> = if insert_columns.is_empty() {
        // Implicit full-width insert in declaration order.
        (0..n_cols).collect()
    } else {
        let mut by_name = std::collections::HashMap::with_capacity(n_cols);
        for (i, f) in schema.fields().iter().enumerate() {
            by_name.insert(f.name().to_ascii_lowercase(), i);
        }
        let mut v = Vec::with_capacity(insert_columns.len());
        for c in insert_columns {
            let idx = *by_name.get(&c.value.to_ascii_lowercase())?;
            v.push(idx);
        }
        v
    };

    // Targeted columns must be neither generated (slow path owns that error)
    // nor identity/SERIAL (the slow path enforces ALWAYS/BY-DEFAULT +
    // OVERRIDING semantics, which we deliberately do not replicate).
    let mut targeted = vec![false; n_cols];
    for &ci in &insert_cols {
        if ci >= n_cols {
            return None;
        }
        let f = schema.field(ci);
        if crate::types::field_is_generated(f).is_some()
            || crate::types::field_identity_mode(f).is_some()
        {
            return None;
        }
        targeted[ci] = true;
    }

    // Untargeted columns must be safely NULL-fillable (nullable, no
    // default/identity/generated). Otherwise the slow path must run so its
    // identity/default machinery fires.
    for (ci, is_targeted) in targeted.iter().enumerate() {
        if *is_targeted {
            continue;
        }
        let f = schema.field(ci);
        if !f.is_nullable()
            || crate::types::field_default_text(f).is_some()
            || crate::types::field_identity_mode(f).is_some()
            || crate::types::field_is_generated(f).is_some()
        {
            return None;
        }
    }

    try_parse_literal_values(sql, schema, &insert_cols)
}

/// Try to scan a literal `INSERT ... VALUES` tail directly into Arrow batches.
///
/// * `sql` — the full original statement text.
/// * `schema` — the target table's Arrow schema (full width).
/// * `insert_cols` — for each position in a VALUES tuple, the schema column
///   index it targets. Positions not covered get NULL (the caller guarantees
///   those columns are nullable and need no default/identity filling).
///
/// Returns `Some(batches)` on a clean parse of a supported literal shape, or
/// `None` to signal the caller to fall back to the slow `batch_from_rows` path.
/// Never panics; never produces a batch that differs from what the slow path
/// would build.
pub(crate) fn try_parse_literal_values(
    sql: &str,
    schema: &Schema,
    insert_cols: &[usize],
) -> Option<Vec<RecordBatch>> {
    let arity = insert_cols.len();
    if arity == 0 {
        return None;
    }
    let n_cols = schema.fields().len();
    // Validate that every targeted column has a supported type *up front*, so
    // we never start a scan we can't finish. Columns not targeted stay NULL and
    // only need to be nullable (the caller already guarantees this, but we keep
    // the dispatch table column-type-driven).
    for &ci in insert_cols {
        if ci >= n_cols {
            return None;
        }
        if !is_supported_col(schema.field(ci)) {
            return None;
        }
    }

    // Locate the `VALUES` keyword outside of any single-quoted string. We scan
    // the raw bytes for a case-insensitive `values` token bounded by
    // non-identifier characters; an occurrence inside a string literal (e.g. a
    // column value `'values'` in an earlier part of the statement — not
    // possible here since this is the header, but defensively handled) is
    // skipped.
    let bytes = sql.as_bytes();
    let values_end = find_values_keyword(bytes)?;

    let mut sc = Scanner {
        b: bytes,
        pos: values_end,
    };

    // Column-major accumulators, one per *schema* column. We push into the
    // targeted columns as we read each tuple, and fill NULL into untargeted
    // columns once per row.
    let mut cols: Vec<ColAcc> = schema
        .fields()
        .iter()
        .map(|f| ColAcc::new(f.as_ref()))
        .collect::<Option<Vec<_>>>()?;
    // Reverse map: schema col idx -> Some(tuple position) for targeted cols.
    // (Untargeted entries are None and get a NULL appended per row.)
    let mut tuple_pos_for_col: Vec<Option<usize>> = vec![None; n_cols];
    for (tpos, &ci) in insert_cols.iter().enumerate() {
        // Duplicate target column in the insert list is not something we model
        // here — bail to the slow path which has the canonical error.
        if tuple_pos_for_col[ci].is_some() {
            return None;
        }
        tuple_pos_for_col[ci] = Some(tpos);
    }

    let mut out: Vec<RecordBatch> = Vec::new();
    let mut rows_in_batch = 0usize;
    let schema_arc = Arc::new(schema.clone());

    // Scratch buffer reused for each tuple's cells.
    let mut tuple: Vec<Cell> = Vec::with_capacity(arity);

    loop {
        sc.skip_ws();
        // End of the values list: optional trailing `;` then EOF.
        if sc.at_end() {
            break;
        }
        if sc.peek() == Some(b';') {
            sc.pos += 1;
            sc.skip_ws();
            if !sc.at_end() {
                return None; // trailing garbage after `;`
            }
            break;
        }
        // Between tuples we must see a comma (handled at end of loop) — the
        // first iteration starts straight at a `(`.
        if sc.peek() != Some(b'(') {
            return None;
        }
        sc.pos += 1; // consume '('

        tuple.clear();
        // Read exactly `arity` comma-separated values.
        for i in 0..arity {
            sc.skip_ws();
            let cell = sc.read_value()?;
            tuple.push(cell);
            sc.skip_ws();
            if i + 1 < arity {
                if sc.peek() != Some(b',') {
                    return None; // too few values / malformed
                }
                sc.pos += 1;
            }
        }
        sc.skip_ws();
        if sc.peek() != Some(b')') {
            return None; // too many values / malformed
        }
        sc.pos += 1; // consume ')'

        // Place each cell into its destination column accumulator, applying the
        // type check. NULL into a NOT NULL column → None.
        for ci in 0..n_cols {
            match tuple_pos_for_col[ci] {
                Some(tpos) => {
                    let field = schema.field(ci);
                    if !cols[ci].push(&tuple[tpos], field) {
                        return None;
                    }
                }
                None => {
                    let field = schema.field(ci);
                    if !field.is_nullable() {
                        // Should never happen (caller guarantees), but stay
                        // defensive: an untargeted NOT NULL column can't be
                        // satisfied here.
                        return None;
                    }
                    cols[ci].push_null();
                }
            }
        }
        rows_in_batch += 1;

        if rows_in_batch == BATCH_ROWS {
            // `finish_batch` drains each accumulator and leaves a fresh empty
            // one in its place, so `cols` is ready for the next chunk.
            let batch = finish_batch(schema_arc.clone(), &mut cols)?;
            out.push(batch);
            rows_in_batch = 0;
        }

        // After a tuple: either a comma (more tuples) or end-of-list.
        sc.skip_ws();
        match sc.peek() {
            Some(b',') => {
                sc.pos += 1;
                // A trailing comma with nothing after is malformed.
                sc.skip_ws();
                if sc.at_end() || sc.peek() == Some(b';') {
                    return None;
                }
            }
            Some(b';') | None => {
                // handled at loop top
            }
            _ => return None, // garbage between tuples
        }
    }

    if rows_in_batch > 0 {
        let batch = finish_batch(schema_arc, &mut cols)?;
        out.push(batch);
    }

    if out.is_empty() {
        // No tuples at all — not a shape we handle; let the slow path produce
        // its canonical "INSERT requires at least one row" behaviour.
        return None;
    }
    Some(out)
}

/// Whether the scanner builds Arrow directly for this column's type. Decorated
/// Utf8 columns (those carrying type metadata for INET/UUID/BIT/etc.) are
/// rejected because they need validation we don't replicate.
fn is_supported_col(field: &arrow_schema::Field) -> bool {
    match field.data_type() {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        DataType::Float32 | DataType::Float64 => true,
        DataType::Boolean => true,
        DataType::Utf8 => {
            // Only *plain* TEXT/VARCHAR/CHAR. Reject anything carrying a logical
            // type marker that the slow path would validate. CHAR(n)/VARCHAR(n)
            // length enforcement and CHAR padding also live on the slow path, so
            // reject any charlen-marked column too.
            if crate::types::parse_charlen(field).is_some() {
                return false;
            }
            !(crate::types::field_is_inet(field)
                || crate::types::field_is_cidr(field)
                || crate::types::field_is_macaddr(field)
                || crate::types::field_is_macaddr8(field)
                || crate::types::field_is_bit(field)
                || crate::types::field_is_varbit(field)
                || crate::types::field_is_tsvector(field)
                || crate::types::field_is_tsquery(field)
                || crate::types::field_is_uuid(field))
        }
        // JSONB rides on `LargeBinary` carrying the JSONB marker. Plain
        // `LargeBinary` (BYTEA-large / non-JSONB) needs the bytea hex/escape
        // coercion the scanner does not replicate, so only the marked form is
        // admitted. The cell is coerced through `dml::coerce_jsonb_str`.
        DataType::LargeBinary => crate::types::field_is_jsonb(field),
        // TIMESTAMP / TIMESTAMPTZ stored as microsecond timestamps. Both the
        // naive (`tz = None`) and zoned (`tz = Some`) forms are admitted; the
        // tz is carried through to the produced array's DataType exactly as the
        // slow path does. Other time units are never emitted by our DDL, so we
        // only admit microseconds. The cell is coerced through
        // `dml::parse_timestamp_string` (string form) or accepted as an i64
        // epoch-micros literal (numeric form), mirroring the slow path.
        DataType::Timestamp(TimeUnit::Microsecond, _) => true,
        _ => false,
    }
}

/// Find the byte offset just past the `VALUES` keyword (case-insensitive),
/// scanning outside single-quoted strings. Returns the index of the first byte
/// after the keyword, or `None` if not found / found inside a string.
fn find_values_keyword(b: &[u8]) -> Option<usize> {
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                // Skip a single-quoted string, honouring '' escapes.
                i += 1;
                loop {
                    if i >= b.len() {
                        return None; // unterminated string in header
                    }
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2; // escaped quote
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            c if c == b'v' || c == b'V' => {
                // Possible `values`. Require a word boundary before and after.
                let prev_ok = i == 0 || !is_ident_byte(b[i - 1]);
                if prev_ok && matches_values_ci(b, i) {
                    let after = i + 6;
                    let next_ok = after >= b.len() || !is_ident_byte(b[after]);
                    if next_ok {
                        return Some(after);
                    }
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

fn matches_values_ci(b: &[u8], i: usize) -> bool {
    const KW: &[u8; 6] = b"values";
    if i + 6 > b.len() {
        return false;
    }
    for k in 0..6 {
        if b[i + k].to_ascii_lowercase() != KW[k] {
            return false;
        }
    }
    true
}

#[inline]
fn is_ident_byte(c: u8) -> bool {
    c == b'_' || c.is_ascii_alphanumeric()
}

/// Byte-level cursor over the `VALUES` tail.
struct Scanner<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.pos >= self.b.len()
    }

    #[inline]
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Read one literal value. Returns `None` on any unsupported / malformed
    /// token. On success the cursor sits immediately after the value.
    fn read_value(&mut self) -> Option<Cell<'a>> {
        let c = self.peek()?;
        match c {
            b'\'' => self.read_string(),
            b'-' | b'0'..=b'9' => self.read_number(),
            b'n' | b'N' => self.read_keyword_null(),
            b't' | b'T' => self.read_keyword(b"true", Cell::Bool(true)),
            b'f' | b'F' => self.read_keyword(b"false", Cell::Bool(false)),
            // Anything else — identifiers, casts, functions, `$`, `E'`, `x'`,
            // `ARRAY[`, double-quotes, `(` (nested) — is out of scope.
            _ => None,
        }
    }

    /// `'...'` with `''` → `'` un-escaping. Borrows when no escape is present.
    fn read_string(&mut self) -> Option<Cell<'a>> {
        debug_assert_eq!(self.peek(), Some(b'\''));
        let start = self.pos + 1;
        let mut i = start;
        let mut had_escape = false;
        loop {
            let c = *self.b.get(i)?; // None => unterminated
            if c == b'\'' {
                if self.b.get(i + 1) == Some(&b'\'') {
                    had_escape = true;
                    i += 2;
                    continue;
                }
                // End of string.
                let raw = &self.b[start..i];
                self.pos = i + 1;
                // Validate UTF-8 once; the rest of the engine assumes valid
                // UTF-8 strings. Invalid bytes → bail to slow path.
                let s = std::str::from_utf8(raw).ok()?;
                if !had_escape {
                    return Some(Cell::Str(std::borrow::Cow::Borrowed(s)));
                }
                // Collapse `''` → `'`. We replace on the validated `&str`; a
                // `'` is ASCII (never a UTF-8 continuation byte) so the doubled
                // quote can only appear at a char boundary — `replace` is safe
                // and never splits a multi-byte sequence.
                let out = s.replace("''", "'");
                return Some(Cell::Str(std::borrow::Cow::Owned(out)));
            }
            i += 1;
        }
    }

    /// Integer or float literal, optional leading `-`.
    fn read_number(&mut self) -> Option<Cell<'a>> {
        let start = self.pos;
        let mut i = self.pos;
        let mut is_float = false;
        if self.b.get(i) == Some(&b'-') {
            i += 1;
        }
        let digits_start = i;
        while let Some(&c) = self.b.get(i) {
            if c.is_ascii_digit() {
                i += 1;
            } else {
                break;
            }
        }
        // Fractional part.
        if self.b.get(i) == Some(&b'.') {
            is_float = true;
            i += 1;
            while let Some(&c) = self.b.get(i) {
                if c.is_ascii_digit() {
                    i += 1;
                } else {
                    break;
                }
            }
        }
        // Exponent.
        if matches!(self.b.get(i), Some(&b'e') | Some(&b'E')) {
            is_float = true;
            i += 1;
            if matches!(self.b.get(i), Some(&b'+') | Some(&b'-')) {
                i += 1;
            }
            let exp_digits_start = i;
            while let Some(&c) = self.b.get(i) {
                if c.is_ascii_digit() {
                    i += 1;
                } else {
                    break;
                }
            }
            if i == exp_digits_start {
                return None; // `1e` with no exponent digits
            }
        }
        // Must have consumed at least one mantissa digit.
        if i == digits_start {
            return None; // lone `-` or `-.`
        }
        let text = std::str::from_utf8(&self.b[start..i]).ok()?;
        self.pos = i;
        if is_float {
            let v: f64 = text.parse().ok()?;
            Some(Cell::Float(v))
        } else {
            // Integer. Parse as i64; overflow → bail (slow path raises the
            // canonical range error; we just decline).
            let v: i64 = text.parse().ok()?;
            Some(Cell::Int(v))
        }
    }

    /// `NULL` keyword (case-insensitive), word-bounded.
    fn read_keyword_null(&mut self) -> Option<Cell<'a>> {
        self.read_keyword(b"null", Cell::Null)
    }

    /// Match a keyword case-insensitively with a trailing word boundary.
    fn read_keyword(&mut self, kw: &[u8], val: Cell<'a>) -> Option<Cell<'a>> {
        let end = self.pos + kw.len();
        if end > self.b.len() {
            return None;
        }
        for (k, &want) in kw.iter().enumerate() {
            if self.b[self.pos + k].to_ascii_lowercase() != want {
                return None;
            }
        }
        // Word boundary: the next byte must not continue an identifier.
        if let Some(&next) = self.b.get(end) {
            if is_ident_byte(next) {
                return None;
            }
        }
        self.pos = end;
        Some(val)
    }
}

/// Per-column Arrow accumulator. One variant per supported destination type.
enum ColAcc {
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    // Utf8 owns its strings; we collapse Cow into owned on push to keep the
    // accumulator `'static`. The clone is one allocation per non-borrowed cell;
    // borrowed cells still cost a copy here, but only the bytes (no AST node).
    Utf8(Vec<Option<String>>),
    // JSONB: each non-NULL cell is a string literal coerced to canonical JSON
    // bytes via `dml::coerce_jsonb_str` (the slow path's exact pipeline). The
    // column name is retained so the coercion error message — which the slow
    // path surfaces verbatim on fallback — names the right column.
    Jsonb {
        vals: Vec<Option<Vec<u8>>>,
        col_name: String,
    },
    // Timestamp(Microsecond, tz?): each cell is an i64 epoch-micros value.
    // `data_type` carries the original (timezone-bearing) Arrow type so the
    // finished array matches the slow path's `with_data_type` byte-for-byte.
    TsMicros {
        vals: Vec<Option<i64>>,
        data_type: DataType,
    },
}

impl ColAcc {
    /// Build a fresh accumulator for `field`'s column. Takes the whole `field`
    /// (not just the `DataType`) because JSONB needs the column name for its
    /// coercion error message and the JSONB marker lives in field metadata.
    fn new(field: &arrow_schema::Field) -> Option<ColAcc> {
        let dt = field.data_type();
        Some(match dt {
            DataType::Int16 => ColAcc::Int16(Vec::with_capacity(BATCH_ROWS)),
            DataType::Int32 => ColAcc::Int32(Vec::with_capacity(BATCH_ROWS)),
            DataType::Int64 => ColAcc::Int64(Vec::with_capacity(BATCH_ROWS)),
            DataType::Float32 => ColAcc::Float32(Vec::with_capacity(BATCH_ROWS)),
            DataType::Float64 => ColAcc::Float64(Vec::with_capacity(BATCH_ROWS)),
            DataType::Boolean => ColAcc::Bool(Vec::with_capacity(BATCH_ROWS)),
            DataType::Utf8 => ColAcc::Utf8(Vec::with_capacity(BATCH_ROWS)),
            DataType::LargeBinary if crate::types::field_is_jsonb(field) => ColAcc::Jsonb {
                vals: Vec::with_capacity(BATCH_ROWS),
                col_name: field.name().clone(),
            },
            DataType::Timestamp(TimeUnit::Microsecond, _) => ColAcc::TsMicros {
                vals: Vec::with_capacity(BATCH_ROWS),
                data_type: dt.clone(),
            },
            _ => return None,
        })
    }

    /// Append `cell` to this column, type-checking it against `field`. Returns
    /// `false` (→ caller bails to slow path) on type mismatch, range overflow,
    /// or NULL-into-NOT-NULL.
    fn push(&mut self, cell: &Cell, field: &arrow_schema::Field) -> bool {
        // NULL handling is uniform: only allowed into a nullable column.
        if matches!(cell, Cell::Null) {
            if !field.is_nullable() {
                return false;
            }
            self.push_null();
            return true;
        }
        match (self, cell) {
            (ColAcc::Int16(v), Cell::Int(n)) => match i16::try_from(*n) {
                Ok(x) => {
                    v.push(Some(x));
                    true
                }
                Err(_) => false,
            },
            (ColAcc::Int32(v), Cell::Int(n)) => match i32::try_from(*n) {
                Ok(x) => {
                    v.push(Some(x));
                    true
                }
                Err(_) => false,
            },
            (ColAcc::Int64(v), Cell::Int(n)) => {
                v.push(Some(*n));
                true
            }
            // PG widens an integer literal to a float column implicitly.
            (ColAcc::Float64(v), Cell::Float(f)) => {
                v.push(Some(*f));
                true
            }
            (ColAcc::Float64(v), Cell::Int(n)) => {
                v.push(Some(*n as f64));
                true
            }
            (ColAcc::Float32(v), Cell::Float(f)) => {
                v.push(Some(*f as f32));
                true
            }
            (ColAcc::Float32(v), Cell::Int(n)) => {
                v.push(Some(*n as f32));
                true
            }
            (ColAcc::Bool(v), Cell::Bool(b)) => {
                v.push(Some(*b));
                true
            }
            (ColAcc::Utf8(v), Cell::Str(s)) => {
                v.push(Some(s.as_ref().to_string()));
                true
            }
            // JSONB: a string literal is the only INSERT form Postgres (and the
            // slow path) accepts for a JSONB column without an explicit cast.
            // We run the *same* parse+canonicalise pipeline the slow path uses
            // (`coerce_jsonb_str`). Invalid JSON → `false` so we fall back to
            // the slow path, which re-runs the identical coercion and surfaces
            // the canonical `invalid JSON literal for column …` error. A number
            // / bool into a JSONB column is a type mismatch → `false`.
            (ColAcc::Jsonb { vals, col_name }, Cell::Str(s)) => {
                match crate::dml::coerce_jsonb_str(s.as_ref(), col_name) {
                    Ok(bytes) => {
                        vals.push(Some(bytes));
                        true
                    }
                    Err(_) => false,
                }
            }
            // Timestamp(Microsecond, tz?): accept a string literal parsed via
            // the slow path's `parse_timestamp_string` (RFC3339 / PG forms /
            // naive / date-only), or a bare integer treated as epoch
            // microseconds (mirroring the slow path's numeric arm). Any parse
            // uncertainty → `false` (slow path raises the canonical error).
            (ColAcc::TsMicros { vals, .. }, Cell::Str(s)) => {
                match crate::dml::parse_timestamp_string(s.as_ref()) {
                    Ok(micros) => {
                        vals.push(Some(micros));
                        true
                    }
                    Err(_) => false,
                }
            }
            (ColAcc::TsMicros { vals, .. }, Cell::Int(n)) => {
                vals.push(Some(*n));
                true
            }
            // Any other (column, cell) pairing is a type mismatch — decline so
            // the slow path produces the canonical typed error message.
            _ => false,
        }
    }

    fn push_null(&mut self) {
        match self {
            ColAcc::Int16(v) => v.push(None),
            ColAcc::Int32(v) => v.push(None),
            ColAcc::Int64(v) => v.push(None),
            ColAcc::Float32(v) => v.push(None),
            ColAcc::Float64(v) => v.push(None),
            ColAcc::Bool(v) => v.push(None),
            ColAcc::Utf8(v) => v.push(None),
            ColAcc::Jsonb { vals, .. } => vals.push(None),
            ColAcc::TsMicros { vals, .. } => vals.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColAcc::Int16(v) => Arc::new(Int16Array::from(v)),
            ColAcc::Int32(v) => Arc::new(Int32Array::from(v)),
            ColAcc::Int64(v) => Arc::new(Int64Array::from(v)),
            ColAcc::Float32(v) => Arc::new(Float32Array::from(v)),
            ColAcc::Float64(v) => Arc::new(Float64Array::from(v)),
            ColAcc::Bool(v) => Arc::new(BooleanArray::from(v)),
            ColAcc::Utf8(v) => Arc::new(StringArray::from(v)),
            // Build from the owned `Vec<Option<Vec<u8>>>`; `LargeBinaryArray`
            // accepts an iterator of `Option<&[u8]>`.
            ColAcc::Jsonb { vals, .. } => Arc::new(LargeBinaryArray::from_iter(
                vals.iter().map(|o| o.as_deref()),
            )),
            // Re-attach the original (timezone-bearing) DataType so the array's
            // type matches the slow path's `with_data_type(...)` exactly.
            ColAcc::TsMicros { vals, data_type } => {
                let arr = TimestampMicrosecondArray::from(vals);
                Arc::new(arr.with_data_type(data_type))
            }
        }
    }
}

/// Drain the accumulators into a `RecordBatch`. Replaces each `ColAcc` with a
/// fresh empty one so the caller can keep filling the next batch.
fn finish_batch(schema: Arc<Schema>, cols: &mut [ColAcc]) -> Option<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter_mut().enumerate() {
        let field = schema.field(i);
        let taken = std::mem::replace(c, ColAcc::new(field)?);
        arrays.push(taken.finish());
    }
    RecordBatch::try_new(schema, arrays).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::Field;

    fn schema_iib() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Boolean, true),
        ])
    }

    fn scan(sql: &str, schema: &Schema, cols: &[usize]) -> Option<Vec<RecordBatch>> {
        try_parse_literal_values(sql, schema, cols)
    }

    #[test]
    fn basic_three_col() {
        let s = schema_iib();
        let b = scan(
            "INSERT INTO t (a,b,c) VALUES (1,'x',TRUE),(2,'y',FALSE)",
            &s,
            &[0, 1, 2],
        )
        .expect("should parse");
        let total: usize = b.iter().map(|r| r.num_rows()).sum();
        assert_eq!(total, 2);
        let r = &b[0];
        let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 1);
        assert_eq!(a.value(1), 2);
        let strs = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.value(0), "x");
        assert_eq!(strs.value(1), "y");
    }

    #[test]
    fn quote_escaping() {
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        let b = scan("INSERT INTO t (b) VALUES ('it''s'),('a''''b')", &s, &[0])
            .expect("should parse");
        let strs = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strs.value(0), "it's");
        assert_eq!(strs.value(1), "a''b");
    }

    #[test]
    fn unicode_and_commas_inside_string() {
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        let b = scan(
            "INSERT INTO t (b) VALUES ('héllo, (wörld)'),('comma,,,here')",
            &s,
            &[0],
        )
        .expect("should parse");
        let strs = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strs.value(0), "héllo, (wörld)");
        assert_eq!(strs.value(1), "comma,,,here");
    }

    #[test]
    fn negatives_floats_exponents() {
        let s = Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]);
        let b = scan(
            "INSERT INTO t (i,f) VALUES (-5, -1.5),(0, 2.5e3),(7, 1E-2)",
            &s,
            &[0, 1],
        )
        .expect("should parse");
        let r = &b[0];
        let i = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i.value(0), -5);
        let f = r
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f.value(0), -1.5);
        assert_eq!(f.value(1), 2500.0);
        assert_eq!(f.value(2), 0.01);
    }

    #[test]
    fn nulls_and_trailing_semicolon() {
        let s = schema_iib();
        let b = scan(
            "INSERT INTO t (a,b,c) VALUES (1,NULL,NULL),(NULL,'z',true) ;",
            &s,
            &[0, 1, 2],
        )
        .expect("should parse");
        let r = &b[0];
        let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(a.is_null(1));
        let strs = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(strs.is_null(0));
    }

    #[test]
    fn whitespace_tolerance() {
        let s = schema_iib();
        let b = scan(
            "INSERT INTO t (a,b,c)\n VALUES\t( 1 , 'x' , TRUE )\n,\n( 2 ,'y', false )",
            &s,
            &[0, 1, 2],
        )
        .expect("should parse");
        assert_eq!(b.iter().map(|r| r.num_rows()).sum::<usize>(), 2);
    }

    // ── malformed / unsupported → None ──────────────────────────────────────

    #[test]
    fn reject_cast() {
        let s = schema_iib();
        assert!(scan("INSERT INTO t (a,b,c) VALUES (1::int,'x',TRUE)", &s, &[0, 1, 2]).is_none());
    }

    #[test]
    fn reject_function_call() {
        let s = schema_iib();
        assert!(scan("INSERT INTO t (a,b,c) VALUES (now(),'x',TRUE)", &s, &[0, 1, 2]).is_none());
    }

    #[test]
    fn reject_param() {
        let s = schema_iib();
        assert!(scan("INSERT INTO t (a,b,c) VALUES ($1,'x',TRUE)", &s, &[0, 1, 2]).is_none());
    }

    #[test]
    fn reject_unterminated_string() {
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        assert!(scan("INSERT INTO t (b) VALUES ('oops)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_trailing_comma() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(scan("INSERT INTO t (a) VALUES (1),(2),", &s, &[0]).is_none());
    }

    #[test]
    fn reject_too_few_values() {
        let s = schema_iib();
        assert!(scan("INSERT INTO t (a,b,c) VALUES (1,'x')", &s, &[0, 1, 2]).is_none());
    }

    #[test]
    fn reject_too_many_values() {
        let s = schema_iib();
        assert!(scan("INSERT INTO t (a,b,c) VALUES (1,'x',TRUE,9)", &s, &[0, 1, 2]).is_none());
    }

    #[test]
    fn reject_trailing_garbage() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(scan("INSERT INTO t (a) VALUES (1) blah", &s, &[0]).is_none());
    }

    #[test]
    fn reject_type_mismatch_string_into_int() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(scan("INSERT INTO t (a) VALUES ('hi')", &s, &[0]).is_none());
    }

    #[test]
    fn reject_float_into_int() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(scan("INSERT INTO t (a) VALUES (1.5)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_null_into_not_null() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(scan("INSERT INTO t (a) VALUES (NULL)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_int_overflow_int32() {
        let s = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        assert!(scan("INSERT INTO t (a) VALUES (5000000000)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_unsupported_column_type() {
        // Date32 is deliberately out of scope for the fast path (no in-scanner
        // date parser); it must still route to the slow path.
        let s = Schema::new(vec![Field::new("d", DataType::Date32, true)]);
        assert!(scan("INSERT INTO t (d) VALUES ('2020-01-01')", &s, &[0]).is_none());
    }

    #[test]
    fn reject_plain_largebinary_without_jsonb_marker() {
        // Plain LargeBinary (non-JSONB BYTEA-large) needs bytea coercion the
        // scanner doesn't replicate → slow path.
        let s = Schema::new(vec![Field::new("b", DataType::LargeBinary, true)]);
        assert!(scan("INSERT INTO t (b) VALUES ('\\xdead')", &s, &[0]).is_none());
    }

    fn jsonb_field(name: &str) -> Field {
        let mut md = std::collections::HashMap::new();
        md.insert(
            crate::types::BASIN_TYPE_KEY.to_string(),
            crate::types::BASIN_TYPE_JSONB.to_string(),
        );
        Field::new(name, DataType::LargeBinary, true).with_metadata(md)
    }

    #[test]
    fn jsonb_canonical_bytes_match_helper() {
        let s = Schema::new(vec![jsonb_field("j")]);
        let b = scan(
            "INSERT INTO t (j) VALUES ('{\"b\":1,\"a\":2}'),(NULL)",
            &s,
            &[0],
        )
        .expect("should parse");
        let arr = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        // Canonical form sorts object keys: {"a":2,"b":1}.
        let expected = crate::dml::coerce_jsonb_str("{\"b\":1,\"a\":2}", "j").unwrap();
        assert_eq!(arr.value(0), expected.as_slice());
        assert!(arr.is_null(1));
    }

    #[test]
    fn jsonb_invalid_json_declines() {
        let s = Schema::new(vec![jsonb_field("j")]);
        // Not valid JSON (bare identifier) → scanner declines so the slow path
        // surfaces the canonical error.
        assert!(scan("INSERT INTO t (j) VALUES ('{not json}')", &s, &[0]).is_none());
    }

    #[test]
    fn jsonb_rejects_non_string_cell() {
        let s = Schema::new(vec![jsonb_field("j")]);
        assert!(scan("INSERT INTO t (j) VALUES (42)", &s, &[0]).is_none());
    }

    #[test]
    fn timestamp_string_and_epoch_forms() {
        let s = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let b = scan(
            "INSERT INTO t (ts) VALUES ('2020-01-01T00:00:00Z'),('2021-06-15 12:30:00'),(1000000),(NULL)",
            &s,
            &[0],
        )
        .expect("should parse");
        let arr = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), crate::dml::parse_timestamp_string("2020-01-01T00:00:00Z").unwrap());
        assert_eq!(arr.value(1), crate::dml::parse_timestamp_string("2021-06-15 12:30:00").unwrap());
        assert_eq!(arr.value(2), 1_000_000);
        assert!(arr.is_null(3));
    }

    #[test]
    fn timestamp_unparseable_declines() {
        let s = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        assert!(scan("INSERT INTO t (ts) VALUES ('not a date')", &s, &[0]).is_none());
    }

    #[test]
    fn timestamp_tz_carried_into_array_type() {
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let s = Schema::new(vec![Field::new("ts", dt.clone(), true)]);
        let b = scan("INSERT INTO t (ts) VALUES ('2020-01-01T00:00:00Z')", &s, &[0])
            .expect("should parse");
        assert_eq!(b[0].column(0).data_type(), &dt);
    }

    #[test]
    fn reject_lone_minus() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        assert!(scan("INSERT INTO t (a) VALUES (-)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_bad_exponent() {
        let s = Schema::new(vec![Field::new("f", DataType::Float64, true)]);
        assert!(scan("INSERT INTO t (f) VALUES (1e)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_double_quoted_string() {
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        assert!(scan("INSERT INTO t (b) VALUES (\"x\")", &s, &[0]).is_none());
    }

    #[test]
    fn reject_e_prefixed_string() {
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        assert!(scan("INSERT INTO t (b) VALUES (E'x')", &s, &[0]).is_none());
    }

    #[test]
    fn batch_chunking_over_8192() {
        let s = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let mut sql = String::from("INSERT INTO t (a) VALUES ");
        let n = 8192 + 100;
        for i in 0..n {
            if i > 0 {
                sql.push(',');
            }
            sql.push('(');
            sql.push_str(&i.to_string());
            sql.push(')');
        }
        let b = scan(&sql, &s, &[0]).expect("should parse");
        assert_eq!(b.len(), 2, "expected two batches (8192 + 100)");
        assert_eq!(b[0].num_rows(), 8192);
        assert_eq!(b[1].num_rows(), 100);
        let total: usize = b.iter().map(|r| r.num_rows()).sum();
        assert_eq!(total, n);
    }

    #[test]
    fn subset_columns_null_fill() {
        // Schema has 3 cols, insert only targets col 0 and col 2; col 1 (b,
        // nullable) is NULL-filled.
        let s = schema_iib();
        let b = scan("INSERT INTO t (a,c) VALUES (1,TRUE),(2,FALSE)", &s, &[0, 2])
            .expect("should parse");
        let r = &b[0];
        let strs = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(strs.is_null(0));
        assert!(strs.is_null(1));
        let c = r
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(c.value(0));
        assert!(!c.value(1));
    }
}
