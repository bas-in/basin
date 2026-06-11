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
//!   string      := '\'' ( [^'] | '\'\'' )* '\'' cast?               (PG single-quote, '' escapes ')
//!   cast        := WS* '::' WS* ( 'jsonb' | 'timestamp' | 'timestamptz' )  (ASCII case-insensitive)
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
//!   TRUE / FALSE: function-style `CAST(...)`, function calls, `$`-params,
//!   dollar-quoted strings, `ARRAY[...]`, nested parens, identifiers,
//!   E'' / N'' / x'' prefixed strings, double-quoted strings. The ONE cast
//!   shape admitted is a `::jsonb` / `::timestamp` / `::timestamptz` suffix on
//!   a single-quoted string (the shape the published bulk-INSERT benchmark
//!   sends: `'<json>'::jsonb`), and the tag must MATCH the destination column:
//!   `::jsonb` only into a JSONB column, `::timestamp` / `::timestamptz` only
//!   into a Timestamp(µs) column (tz-bearing or not — the slow path peels the
//!   cast wrapper and ignores its type for those two column families, so
//!   admitting the suffix is byte-identical). Any other suffix tag (`::text`,
//!   `::uuid`, `::date`, `::int`, …) and any tag/column mismatch → `None`.
//!   `::text` into a TEXT column is *deliberately* declined: the slow path's
//!   plain-Utf8 coercion (`coerce_string_ref`) does NOT peel cast wrappers —
//!   it raises "expected string literal" — and the fast path must never accept
//!   a statement the slow path rejects. A `::` cast on a non-string token
//!   (`1::int`) is still rejected outright.
//! * A value whose token kind is incompatible with its destination column type
//!   (e.g. a string literal for an Int64 column, a float for an Int column).
//! * NULL routed at a NOT NULL column.
//! * Structural malformation: missing/extra commas, unbalanced parens,
//!   unterminated string, trailing comma, trailing garbage after the last
//!   tuple, integer/float that overflows the destination width.
//!
//! The caller is responsible for the *other* preconditions (no ON CONFLICT, no
//! OVERRIDING clause, every omitted column is a plain nullable column with no
//! DEFAULT/IDENTITY/generated filling required) — see the executor hooks.
//!
//! ## Two executor hooks
//!
//! 1. **AST hook** (`exec_insert` → [`try_fast_insert`]): the statement was
//!    fully parsed (libpg_query + sqlparser) and only the VALUES tail is
//!    re-scanned. The original integration point.
//! 2. **Pre-parse hook** (`executor::try_insert_preparse` →
//!    [`classify_literal_insert`] + [`try_fast_insert_at`]): the statement is
//!    classified by an O(prefix) header scan BEFORE either full parser runs,
//!    so an engaged bulk INSERT never pays the two whole-statement parses at
//!    all. The classifier only locates the tuple list; header *semantics*
//!    (ident case folding, keyword-ness, table resolution) are delegated to a
//!    tiny sqlparser parse of just the header in the executor.

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
/// case), and own a `String` only when an escape was collapsed. A string may
/// carry a recognised `::<type>` suffix-cast tag; at append time the tag must
/// match the destination column's accumulator or the whole statement declines
/// (see [`ColAcc::push`]). Only strings ever carry a tag — the suffix parse
/// happens exclusively in [`Scanner::read_string`].
enum Cell<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str {
        s: std::borrow::Cow<'a, str>,
        cast: Option<CastTag>,
    },
}

/// The closed set of `::<type>` suffix casts the scanner admits on a string
/// literal. Each tag is admitted into exactly the column family whose slow-path
/// coercion *peels* cast wrappers (so fast and slow stay byte-identical):
/// `Jsonb` → JSONB columns (`dml::coerce_jsonb` peels), `Timestamp` /
/// `Timestamptz` → Timestamp(µs) columns (`dml::coerce_timestamp_micros`
/// peels; the string parser handles both tz-bearing and naive forms, and the
/// slow path ignores the cast's type entirely, so both tags are admitted to
/// both tz and non-tz columns). Anything else — notably `::text`, whose
/// destination's slow-path coercion does NOT peel casts and errors — is not a
/// tag: the tokenizer declines the statement on sight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CastTag {
    Jsonb,
    Timestamp,
    Timestamptz,
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
    let values_end = find_values_keyword(sql.as_bytes())?;
    try_fast_insert_at(sql, values_end, schema, insert_columns)
}

/// [`try_fast_insert`] variant for the pre-parse executor hook: the caller
/// (the [`classify_literal_insert`] classifier) has already located the byte
/// offset just past the `VALUES` keyword, so no keyword search is needed.
/// Identical eligibility gates and identical output to [`try_fast_insert`].
pub(crate) fn try_fast_insert_at(
    sql: &str,
    values_end: usize,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
) -> Option<Vec<RecordBatch>> {
    let names: Vec<&str> = insert_columns.iter().map(|c| c.value.as_str()).collect();
    let insert_cols = resolve_eligible_insert_cols(schema, &names)?;
    try_parse_literal_values_from(sql, values_end, schema, &insert_cols)
}

/// Shared column-eligibility gate for the INSERT fast paths: resolve the
/// per-tuple-position → schema-column-index mapping for `insert_columns`
/// (empty = implicit full-width insert in declaration order) and verify the
/// fast paths may build the batch at all. Returns `None` (→ slow path) when:
///
/// * any named column doesn't resolve on `schema` (case-insensitive match —
///   the same folding the original `try_fast_insert_at` mapping used);
/// * any TARGETED column is generated (the slow path owns that error) or
///   identity/SERIAL (the slow path enforces ALWAYS/BY-DEFAULT + OVERRIDING
///   semantics, which the fast paths deliberately do not replicate);
/// * any UNTARGETED column is not safely NULL-fillable — i.e. it is NOT NULL
///   or carries a DEFAULT / identity / generated definition that the slow
///   path's server-side filling machinery must run for.
pub(crate) fn resolve_eligible_insert_cols(
    schema: &Schema,
    insert_columns: &[&str],
) -> Option<Vec<usize>> {
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
            let idx = *by_name.get(&c.to_ascii_lowercase())?;
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

    Some(insert_cols)
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
    let values_end = find_values_keyword(sql.as_bytes())?;
    try_parse_literal_values_from(sql, values_end, schema, insert_cols)
}

/// [`try_parse_literal_values`] with the `VALUES`-keyword search already done:
/// `values_end` is the byte offset just past the keyword (always an ASCII
/// boundary). Both entry points funnel here so the scan body exists once.
fn try_parse_literal_values_from(
    sql: &str,
    values_end: usize,
    schema: &Schema,
    insert_cols: &[usize],
) -> Option<Vec<RecordBatch>> {
    // Defensive bounds check: a caller-supplied offset past EOF (impossible
    // from our own classifiers, but cheap to refuse) declines to the slow path.
    if values_end > sql.len() {
        return None;
    }
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

    // `values_end` points just past the `VALUES` keyword (located by
    // `find_values_keyword` on the AST-hook path, or by the pre-parse
    // classifier's structural header scan). The scan starts there.
    let bytes = sql.as_bytes();

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

// ───────────────────────────────────────────────────────────────────────────
// Pre-parse classifier (executor hook 2)
// ───────────────────────────────────────────────────────────────────────────

/// Output of [`classify_literal_insert`]: where the literal tuple list begins.
pub(crate) struct LiteralInsertPrefix {
    /// Byte offset just past the `VALUES` keyword (always an ASCII boundary).
    /// `sql[..values_end]` is the complete statement header;
    /// `sql[values_end..]` is the tuple list the scanner consumes.
    pub(crate) values_end: usize,
}

/// O(prefix) classifier for the pre-parse INSERT fast path.
///
/// Scans ONLY the statement header — never the (potentially multi-megabyte)
/// `VALUES` tail — and decides whether this *could* be a plain literal
/// multi-row INSERT:
///
/// ```text
///   header    := pad INSERT pad INTO pad table_ref pad
///                ( '(' pad ident pad ( ',' pad ident pad )* ')' pad )?
///                VALUES                      (word-bounded, case-insensitive)
///   table_ref := ident ( pad '.' pad ident )?      (≤ 1 schema qualifier)
///   ident     := [A-Za-z_][A-Za-z0-9_$]*  |  '"' ( [^"] | '""' )+ '"'
///   pad       := ( ws | '--' line-comment | nested '/* */' block-comment )*
/// ```
///
/// plus a peek that the first non-whitespace byte after `VALUES` is `(` (the
/// tuple scanner's required opener). ANY deviation — `WITH`/CTE,
/// `INSERT … SELECT`, `DEFAULT VALUES`, `OVERRIDING`, a table alias,
/// three-part names, parameters, anything unrecognised — returns `None` and
/// the caller runs the normal double-parse path unchanged.
///
/// Correctness split: this classifier is only trusted to be structurally
/// right about WHERE the tuple list starts. Header *semantics* (identifier
/// case folding, reserved-word rejection, table-name resolution) are
/// delegated to the executor, which re-parses just `sql[..values_end]` with
/// sqlparser (an O(header) parse) and declines on any disagreement. Trailing
/// clauses AFTER the tuples (`RETURNING`, `ON CONFLICT`, a second
/// `;`-separated statement) are the tuple scanner's job: it declines on any
/// non-whitespace byte after the final tuple other than one `;`.
pub(crate) fn classify_literal_insert(sql: &str) -> Option<LiteralInsertPrefix> {
    let b = sql.as_bytes();
    let mut i = skip_pad(b, 0);
    i = expect_keyword_ci(b, i, b"insert")?;
    i = skip_pad(b, i);
    i = expect_keyword_ci(b, i, b"into")?;
    i = skip_pad(b, i);

    // table_ref: ident, optionally schema-qualified by exactly one `.` part.
    i = read_header_ident(b, i)?;
    i = skip_pad(b, i);
    if b.get(i) == Some(&b'.') {
        i = skip_pad(b, i + 1);
        i = read_header_ident(b, i)?;
        i = skip_pad(b, i);
        // A third name part (`a.b.c`) is not a shape we take.
        if b.get(i) == Some(&b'.') {
            return None;
        }
    }

    // Optional `(col, col, …)` ident list. Anything that is not a plain or
    // quoted ident inside the parens (expressions, numbers, `*`) declines.
    if b.get(i) == Some(&b'(') {
        i += 1;
        loop {
            i = skip_pad(b, i);
            i = read_header_ident(b, i)?;
            i = skip_pad(b, i);
            match b.get(i)? {
                b',' => i += 1,
                b')' => {
                    i += 1;
                    break;
                }
                _ => return None,
            }
        }
        i = skip_pad(b, i);
    }

    // The VALUES keyword must come immediately next — `OVERRIDING … VALUE`,
    // `DEFAULT VALUES`, `SELECT`, a table alias, etc. all fail here.
    let values_end = expect_keyword_ci(b, i, b"values")?;

    // Peek: the tuple scanner requires `(` as the first non-whitespace byte
    // after VALUES (it does not skip comments), so decline early otherwise.
    let mut k = values_end;
    while k < b.len() && matches!(b[k], b' ' | b'\t' | b'\n' | b'\r') {
        k += 1;
    }
    if b.get(k) != Some(&b'(') {
        return None;
    }

    Some(LiteralInsertPrefix { values_end })
}

/// Skip whitespace, `--` line comments, and (nested) `/* */` block comments.
/// An unterminated block comment returns `len`, which makes every subsequent
/// token expectation fail → the classifier declines.
fn skip_pad(b: &[u8], mut i: usize) -> usize {
    loop {
        while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        if i + 1 < b.len() && b[i] == b'-' && b[i + 1] == b'-' {
            i += 2;
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
            i += 2;
            let mut depth = 1usize;
            while i < b.len() && depth > 0 {
                if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if i + 1 < b.len() && b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                return b.len(); // unterminated → decline downstream
            }
            continue;
        }
        return i;
    }
}

/// Match `kw` (ASCII case-insensitive) at `i` with a word boundary after it.
/// Returns the offset just past the keyword.
fn expect_keyword_ci(b: &[u8], i: usize, kw: &[u8]) -> Option<usize> {
    let end = i.checked_add(kw.len())?;
    if end > b.len() || !b[i..end].eq_ignore_ascii_case(kw) {
        return None;
    }
    if let Some(&next) = b.get(end) {
        if is_ident_byte(next) || next == b'$' {
            return None;
        }
    }
    Some(end)
}

/// One header identifier: bare (`[A-Za-z_][A-Za-z0-9_$]*`) or double-quoted
/// with `""` escapes (non-empty). Returns the offset just past it. The
/// classifier does not interpret the ident at all — the executor's header
/// re-parse owns case folding and reserved-word semantics.
fn read_header_ident(b: &[u8], i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => {
            let mut j = i + 1;
            loop {
                match *b.get(j)? {
                    b'"' => {
                        if b.get(j + 1) == Some(&b'"') {
                            j += 2; // "" escape
                            continue;
                        }
                        if j == i + 1 {
                            return None; // empty quoted ident
                        }
                        return Some(j + 1);
                    }
                    _ => j += 1,
                }
            }
        }
        c if c == b'_' || c.is_ascii_alphabetic() => {
            let mut j = i + 1;
            while let Some(&n) = b.get(j) {
                if is_ident_byte(n) || n == b'$' {
                    j += 1;
                } else {
                    break;
                }
            }
            Some(j)
        }
        _ => None,
    }
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

    /// `'...'` with `''` → `'` un-escaping, plus an optional `::<type>` suffix
    /// cast (see [`Scanner::read_cast_suffix`]). Borrows when no escape is
    /// present.
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
                // Optional `::<type>` suffix cast. `None` here means "a cast
                // was present but is not one we admit" → decline the whole
                // statement (the slow path owns every other cast shape).
                let cast = self.read_cast_suffix()?;
                if !had_escape {
                    return Some(Cell::Str {
                        s: std::borrow::Cow::Borrowed(s),
                        cast,
                    });
                }
                // Collapse `''` → `'`. We replace on the validated `&str`; a
                // `'` is ASCII (never a UTF-8 continuation byte) so the doubled
                // quote can only appear at a char boundary — `replace` is safe
                // and never splits a multi-byte sequence.
                let out = s.replace("''", "'");
                return Some(Cell::Str {
                    s: std::borrow::Cow::Owned(out),
                    cast,
                });
            }
            i += 1;
        }
    }

    /// Optionally consume a `::<identifier>` suffix cast right after a string
    /// literal. Three outcomes:
    ///
    /// * `Some(None)` — no `::` follows; cursor is past any whitespace that was
    ///   probed (harmless: every caller `skip_ws()`s immediately after the
    ///   value anyway, so consuming it early changes nothing).
    /// * `Some(Some(tag))` — a recognised tag (`jsonb` / `timestamp` /
    ///   `timestamptz`, ASCII case-insensitive) was consumed; cursor sits just
    ///   past the identifier.
    /// * `None` — a `:` was seen but what follows is not a recognised
    ///   `::<tag>`: lone `:`, empty identifier (`'x'::`), or any tag outside
    ///   the admitted set (`::text`, `::uuid`, `::date`, `::int`, …). The
    ///   caller declines the whole statement; the slow path owns those shapes.
    ///
    /// PG allows whitespace around `::` (`'x' :: jsonb`); we skip it on both
    /// sides. There is no lookahead hazard: once a `:` is seen, no grammar
    /// production other than a suffix cast can apply, so we never need to
    /// rewind. Multi-word spellings (`::timestamp with time zone`) parse their
    /// first identifier here and the trailing words then fail the caller's
    /// `,` / `)` structure check → statement declines, as intended.
    fn read_cast_suffix(&mut self) -> Option<Option<CastTag>> {
        self.skip_ws();
        if self.peek() != Some(b':') {
            return Some(None);
        }
        if self.b.get(self.pos + 1) != Some(&b':') {
            return None; // lone `:` — not a cast; decline
        }
        self.pos += 2;
        self.skip_ws();
        let start = self.pos;
        while let Some(c) = self.peek() {
            if is_ident_byte(c) {
                self.pos += 1;
            } else {
                break;
            }
        }
        let ident = &self.b[start..self.pos];
        if ident.eq_ignore_ascii_case(b"jsonb") {
            return Some(Some(CastTag::Jsonb));
        }
        if ident.eq_ignore_ascii_case(b"timestamp") {
            return Some(Some(CastTag::Timestamp));
        }
        if ident.eq_ignore_ascii_case(b"timestamptz") {
            return Some(Some(CastTag::Timestamptz));
        }
        None // empty or unrecognised tag → decline the statement
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
            // Plain TEXT: only an *uncast* string is admitted. A suffix-cast
            // string (even a recognised tag like `::jsonb`) must decline: the
            // slow path's `coerce_string_ref` does not peel cast wrappers — it
            // raises "expected string literal" — and the fast path must
            // preserve that behaviour exactly (decline → slow path → its
            // canonical error).
            (ColAcc::Utf8(v), Cell::Str { s, cast }) => {
                if cast.is_some() {
                    return false;
                }
                v.push(Some(s.as_ref().to_string()));
                true
            }
            // JSONB: a string literal, optionally suffix-cast `::jsonb` (the
            // benchmark's exact shape — the slow path's `coerce_jsonb` peels
            // the cast wrapper and coerces the inner string identically). A
            // `::timestamp`/`::timestamptz` tag here is a tag/column mismatch
            // → `false`. We run the *same* parse+canonicalise pipeline the
            // slow path uses (`coerce_jsonb_str`). Invalid JSON → `false` so
            // we fall back to the slow path, which re-runs the identical
            // coercion and surfaces the canonical `invalid JSON literal for
            // column …` error. A number / bool into a JSONB column is a type
            // mismatch → `false`.
            (ColAcc::Jsonb { vals, col_name }, Cell::Str { s, cast }) => {
                if !matches!(cast, None | Some(CastTag::Jsonb)) {
                    return false;
                }
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
            // microseconds (mirroring the slow path's numeric arm). A
            // `::timestamp` or `::timestamptz` suffix cast is admitted into
            // both tz-bearing and naive columns — the slow path's
            // `coerce_timestamp_micros` peels the cast wrapper without looking
            // at its type, and `parse_timestamp_string` already disambiguates
            // tz from the literal itself. A `::jsonb` tag is a tag/column
            // mismatch → `false`. Any parse uncertainty → `false` (slow path
            // raises the canonical error).
            (ColAcc::TsMicros { vals, .. }, Cell::Str { s, cast }) => {
                if !matches!(
                    cast,
                    None | Some(CastTag::Timestamp) | Some(CastTag::Timestamptz)
                ) {
                    return false;
                }
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

// ───────────────────────────────────────────────────────────────────────────
// Bind-direct batch builder (extended-protocol hook 3)
// ───────────────────────────────────────────────────────────────────────────

/// Bind-direct batch builder for the parameterized prepared-INSERT fast path
/// (`executor::try_insert_bind_direct`): build the full-width batches for
/// `INSERT INTO t [(cols)] VALUES ($1, …)[, (…)]` straight from the decoded
/// bind values — no SQL text and no AST anywhere in the per-`Execute` loop.
///
/// * `insert_columns` — the column list as written at prepare time (empty =
///   full schema width, declaration order). Names are re-resolved against the
///   CURRENT `schema` on every call because the table can change between
///   Parse and a later Execute.
/// * `rows[r][p]` — the zero-based parameter index feeding tuple position `p`
///   of VALUES row `r` (precomputed once at prepare time).
/// * `params` — the decoded bind values for this Execute.
///
/// Same cardinal rule as the literal scanner: **when in doubt, return
/// `None`** and let the caller fall back to the AST/text bind route. Cells
/// flow through the SAME [`ColAcc`] accumulators as the literal scanner, so
/// per-column type checks and JSONB / timestamp coercions are byte-identical
/// to what the text route's substituted literal would have produced:
///
/// * `Null` ↔ `NULL`, `Int4`/`Int8` ↔ integer literal, `Bool` ↔
///   `TRUE`/`FALSE`, finite `Float8` ↔ float literal (Rust float `Display`
///   round-trips, so re-parsing the rendered literal yields the same f64),
///   `Text` ↔ single-quoted string literal (JSONB / timestamp destination
///   columns coerce via the shared `dml` helpers, exactly as the slow path
///   peels and coerces a quoted literal).
/// * Non-finite `Float8` declines: the text route renders those as
///   `'NaN'::float8`-style casts, a shape outside the literal grammar whose
///   semantics the slow path owns.
/// * `Bytea` / `Array` decline: their text renderings (`'\x…'::bytea`,
///   `ARRAY[…]`) are likewise out-of-grammar shapes.
pub(crate) fn try_bind_insert_batch(
    schema: &Schema,
    insert_columns: &[String],
    rows: &[Vec<usize>],
    params: &[crate::prepared::ScalarParam],
) -> Option<Vec<RecordBatch>> {
    let names: Vec<&str> = insert_columns.iter().map(|s| s.as_str()).collect();
    let insert_cols = resolve_eligible_insert_cols(schema, &names)?;
    let arity = insert_cols.len();
    if arity == 0 || rows.is_empty() {
        return None;
    }
    let n_cols = schema.fields().len();
    // Every targeted column must have a scanner-supported type — the same
    // up-front gate `try_parse_literal_values_from` applies.
    for &ci in &insert_cols {
        if !is_supported_col(schema.field(ci)) {
            return None;
        }
    }

    let mut cols: Vec<ColAcc> = schema
        .fields()
        .iter()
        .map(|f| ColAcc::new(f.as_ref()))
        .collect::<Option<Vec<_>>>()?;
    // Reverse map: schema col idx -> Some(tuple position) for targeted cols.
    let mut tuple_pos_for_col: Vec<Option<usize>> = vec![None; n_cols];
    for (tpos, &ci) in insert_cols.iter().enumerate() {
        if tuple_pos_for_col[ci].is_some() {
            return None; // duplicate target column → slow path's canonical error
        }
        tuple_pos_for_col[ci] = Some(tpos);
    }

    let schema_arc = Arc::new(schema.clone());
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut rows_in_batch = 0usize;
    for row in rows {
        if row.len() != arity {
            return None;
        }
        for ci in 0..n_cols {
            match tuple_pos_for_col[ci] {
                Some(tpos) => {
                    let param = params.get(*row.get(tpos)?)?;
                    let cell = scalar_param_cell(param)?;
                    if !cols[ci].push(&cell, schema.field(ci)) {
                        return None;
                    }
                }
                None => {
                    if !schema.field(ci).is_nullable() {
                        return None;
                    }
                    cols[ci].push_null();
                }
            }
        }
        rows_in_batch += 1;
        if rows_in_batch == BATCH_ROWS {
            out.push(finish_batch(schema_arc.clone(), &mut cols)?);
            rows_in_batch = 0;
        }
    }
    if rows_in_batch > 0 {
        out.push(finish_batch(schema_arc, &mut cols)?);
    }
    if out.is_empty() {
        return None;
    }
    Some(out)
}

/// Map one decoded bind value to the scanner's [`Cell`] so it rides the same
/// per-column accumulators (and therefore the same type checks / coercions)
/// as a literal token. `None` = an out-of-grammar value the bind-direct path
/// must decline (see [`try_bind_insert_batch`]).
fn scalar_param_cell(p: &crate::prepared::ScalarParam) -> Option<Cell<'_>> {
    use crate::prepared::ScalarParam as SP;
    Some(match p {
        SP::Null => Cell::Null,
        SP::Int4(v) => Cell::Int(i64::from(*v)),
        SP::Int8(v) => Cell::Int(*v),
        SP::Bool(b) => Cell::Bool(*b),
        SP::Float8(f) if f.is_finite() => Cell::Float(*f),
        SP::Text(s) => Cell::Str {
            s: std::borrow::Cow::Borrowed(s.as_str()),
            cast: None,
        },
        // Non-finite floats render as '…'::float8 casts on the text route;
        // bytea / arrays render as '\x…'::bytea / ARRAY[…] — all shapes the
        // literal grammar excludes. Decline → AST/text fallback.
        SP::Float8(_) | SP::Bytea(_) | SP::Array(_) => return None,
    })
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

    // ── `::<type>` suffix casts on string literals ──────────────────────────

    #[test]
    fn jsonb_suffix_cast_admitted() {
        // The benchmark's exact shape: `'<json>'::jsonb`. Case-insensitive,
        // whitespace allowed around `::` (PG accepts `'x' :: jsonb`).
        let s = Schema::new(vec![jsonb_field("j")]);
        let b = scan(
            "INSERT INTO t (j) VALUES ('{\"b\":1,\"a\":2}'::jsonb),('[1,2]'::JSONB),\
             ('true' ::jsonb),('\"x\"':: jsonb),('null' :: JsonB)",
            &s,
            &[0],
        )
        .expect("should parse");
        let arr = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        // Bytes identical to the slow path's coercion of the uncast literal.
        let expected = crate::dml::coerce_jsonb_str("{\"b\":1,\"a\":2}", "j").unwrap();
        assert_eq!(arr.value(0), expected.as_slice());
        assert_eq!(arr.len(), 5);
    }

    #[test]
    fn jsonb_suffix_cast_with_escaped_quote() {
        // The owned-Cow (un-escaping) path must carry the cast tag too.
        let s = Schema::new(vec![jsonb_field("j")]);
        let b = scan(
            "INSERT INTO t (j) VALUES ('{\"q\":\"it''s\"}'::jsonb)",
            &s,
            &[0],
        )
        .expect("should parse");
        let arr = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        let expected = crate::dml::coerce_jsonb_str("{\"q\":\"it's\"}", "j").unwrap();
        assert_eq!(arr.value(0), expected.as_slice());
    }

    #[test]
    fn timestamp_suffix_casts_admitted_both_tags_both_tznesses() {
        // Both `::timestamp` and `::timestamptz` are admitted into both the
        // naive and tz-bearing column forms (the slow path peels the cast
        // without inspecting its type).
        for tz in [None, Some(Arc::<str>::from("UTC"))] {
            let dt = DataType::Timestamp(TimeUnit::Microsecond, tz);
            let s = Schema::new(vec![Field::new("ts", dt.clone(), true)]);
            let b = scan(
                "INSERT INTO t (ts) VALUES ('2020-01-01T00:00:00Z'::timestamptz),\
                 ('2021-06-15 12:30:00'::timestamp),('2020-01-01T00:00:00Z' :: TIMESTAMPTZ)",
                &s,
                &[0],
            )
            .expect("should parse");
            let arr = b[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(arr.value(0), 1_577_836_800_000_000);
            assert_eq!(
                arr.value(1),
                crate::dml::parse_timestamp_string("2021-06-15 12:30:00").unwrap()
            );
            assert_eq!(b[0].column(0).data_type(), &dt);
        }
    }

    #[test]
    fn reject_suffix_cast_into_text_column() {
        // `::text` is not an admitted tag at all (the slow path's plain-Utf8
        // coercion does not peel casts — it errors — so admitting it would
        // change behaviour), and a recognised tag landing in a TEXT column is
        // a tag/column mismatch. Both decline.
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        assert!(scan("INSERT INTO t (b) VALUES ('x'::text)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (b) VALUES ('{}'::jsonb)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (b) VALUES ('2020-01-01'::timestamp)", &s, &[0]).is_none());
    }

    #[test]
    fn reject_mismatched_suffix_cast() {
        // Recognised tag, wrong destination family → decline.
        let s = Schema::new(vec![jsonb_field("j")]);
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::timestamp)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::timestamptz)", &s, &[0]).is_none());
        let ts = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        assert!(scan("INSERT INTO t (ts) VALUES ('2020-01-01'::jsonb)", &ts, &[0]).is_none());
    }

    #[test]
    fn reject_unrecognised_suffix_cast_tags() {
        // Anything outside {jsonb, timestamp, timestamptz} → decline, even
        // when the destination column could otherwise take the string.
        let s = Schema::new(vec![jsonb_field("j")]);
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::json)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::uuid)", &s, &[0]).is_none());
        let d = Schema::new(vec![Field::new("d", DataType::Date32, true)]);
        assert!(scan("INSERT INTO t (d) VALUES ('2020-01-01'::date)", &d, &[0]).is_none());
    }

    #[test]
    fn reject_malformed_suffix_cast() {
        let s = Schema::new(vec![jsonb_field("j")]);
        // Lone colon, empty tag, multi-word spelling, parenthesised precision:
        // all decline (the multi-word / parenthesised forms via the structure
        // check after the first identifier).
        assert!(scan("INSERT INTO t (j) VALUES ('{}':jsonb)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::)", &s, &[0]).is_none());
        assert!(scan("INSERT INTO t (j) VALUES ('{}'::jsonb extra)", &s, &[0]).is_none());
        let ts = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        assert!(scan(
            "INSERT INTO t (ts) VALUES ('2020-01-01'::timestamp with time zone)",
            &ts,
            &[0]
        )
        .is_none());
        assert!(
            scan("INSERT INTO t (ts) VALUES ('2020-01-01'::timestamp(3))", &ts, &[0]).is_none()
        );
    }

    #[test]
    fn uncast_string_still_borrows_zero_copy() {
        // The Cow fast path for plain strings must survive the suffix-cast
        // probe: a string followed directly by `,` / `)` parses unchanged.
        let s = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        let b = scan("INSERT INTO t (b) VALUES ('plain'),('also plain')", &s, &[0])
            .expect("should parse");
        let strs = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strs.value(0), "plain");
        assert_eq!(strs.value(1), "also plain");
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

    // ── pre-parse classifier ─────────────────────────────────────────────────

    /// Classify and return the tail (everything past the reported
    /// `values_end`) so tests can assert the offset is structurally right.
    fn classify_tail(sql: &str) -> Option<&str> {
        classify_literal_insert(sql).map(|p| &sql[p.values_end..])
    }

    #[test]
    fn classify_accepts_plain_shapes() {
        assert_eq!(
            classify_tail("INSERT INTO t (a,b) VALUES (1,'x')"),
            Some(" (1,'x')")
        );
        // No column list.
        assert_eq!(classify_tail("INSERT INTO t VALUES (1)"), Some(" (1)"));
        // Schema-qualified, mixed-case keywords, newlines.
        assert_eq!(
            classify_tail("insert into public.t (a)\nvalues\n(1)"),
            Some("\n(1)")
        );
        // Quoted idents (table, schema, columns) — including a quoted ident
        // that *contains* the VALUES keyword text and parens.
        assert_eq!(
            classify_tail("INSERT INTO \"S\".\"weird values (t)\" (\"a b\", c) VALUES (1,2)"),
            Some(" (1,2)")
        );
        // Leading comments (line + nested block) and comments between tokens.
        assert_eq!(
            classify_tail("-- load\n/* outer /* inner */ */ INSERT /*x*/ INTO t VALUES (1)"),
            Some(" (1)")
        );
        // The classifier only checks the header — trailing clauses after the
        // tuples are the tuple scanner's job (it declines on them).
        assert_eq!(
            classify_tail("INSERT INTO t VALUES (1) RETURNING id"),
            Some(" (1) RETURNING id")
        );
    }

    #[test]
    fn classify_declines_non_candidates() {
        // Not an INSERT at all.
        assert!(classify_tail("SELECT 1").is_none());
        // CTE prefix.
        assert!(classify_tail(
            "WITH x AS (SELECT 1) INSERT INTO t VALUES (1)"
        )
        .is_none());
        // INSERT…SELECT (no VALUES after the column list).
        assert!(classify_tail("INSERT INTO t (a) SELECT a FROM s").is_none());
        assert!(classify_tail("INSERT INTO t SELECT * FROM s").is_none());
        // DEFAULT VALUES.
        assert!(classify_tail("INSERT INTO t DEFAULT VALUES").is_none());
        // OVERRIDING clause between column list and VALUES.
        assert!(classify_tail(
            "INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (1)"
        )
        .is_none());
        // Table alias.
        assert!(classify_tail("INSERT INTO t AS x (a) VALUES (1)").is_none());
        // Three-part name.
        assert!(classify_tail("INSERT INTO a.b.c VALUES (1)").is_none());
        // Expression / number in the column list.
        assert!(classify_tail("INSERT INTO t (a, 1) VALUES (1,2)").is_none());
        assert!(classify_tail("INSERT INTO t (a+b) VALUES (1)").is_none());
        // Empty / malformed column list.
        assert!(classify_tail("INSERT INTO t () VALUES (1)").is_none());
        assert!(classify_tail("INSERT INTO t (a,) VALUES (1)").is_none());
        // VALUES not followed by `(`.
        assert!(classify_tail("INSERT INTO t VALUES").is_none());
        assert!(classify_tail("INSERT INTO t VALUES /*c*/ (1)").is_none());
        // `VALUES` only as a prefix of a longer ident (word boundary).
        assert!(classify_tail("INSERT INTO t VALUESX (1)").is_none());
        // Unterminated quoted ident / block comment.
        assert!(classify_tail("INSERT INTO \"t VALUES (1)").is_none());
        assert!(classify_tail("/* open INSERT INTO t VALUES (1)").is_none());
        // Empty quoted ident.
        assert!(classify_tail("INSERT INTO \"\" VALUES (1)").is_none());
        // MySQL-isms.
        assert!(classify_tail("INSERT IGNORE INTO t VALUES (1)").is_none());
        assert!(classify_tail("INSERT INTO t SET a = 1").is_none());
    }

    #[test]
    fn classify_offset_feeds_scanner() {
        // End-to-end: the classifier's offset must drop the scanner exactly at
        // the tuple list, producing the same batches as the keyword-search
        // entry point.
        let s = schema_iib();
        let sql = "INSERT INTO t (a,b,c) VALUES (1,'x',TRUE),(2,'y',FALSE)";
        let p = classify_literal_insert(sql).expect("classifies");
        let via_classifier =
            try_parse_literal_values_from(sql, p.values_end, &s, &[0, 1, 2]).expect("scans");
        let via_search = try_parse_literal_values(sql, &s, &[0, 1, 2]).expect("scans");
        assert_eq!(via_classifier.len(), via_search.len());
        assert_eq!(via_classifier[0], via_search[0]);
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

    // ── bind-direct batch builder ────────────────────────────────────────────

    #[test]
    fn bind_direct_basic_single_row() {
        use crate::prepared::ScalarParam as SP;
        let s = schema_iib();
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let rows = vec![vec![0usize, 1, 2]];
        let params = vec![SP::Int8(7), SP::Text("hi".into()), SP::Bool(true)];
        let b = try_bind_insert_batch(&s, &cols, &rows, &params).expect("builds");
        assert_eq!(b.len(), 1);
        let r = &b[0];
        assert_eq!(r.num_rows(), 1);
        let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 7);
        let strs = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.value(0), "hi");
        let c = r
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(c.value(0));
    }

    #[test]
    fn bind_direct_matches_literal_scanner_batch() {
        // Cardinal invariant: building from binds yields the SAME batch the
        // literal scanner produces for the equivalent rendered statement.
        use crate::prepared::ScalarParam as SP;
        let s = schema_iib();
        let via_scanner = scan(
            "INSERT INTO t (a,b,c) VALUES (1,'x',TRUE),(2,NULL,FALSE)",
            &s,
            &[0, 1, 2],
        )
        .expect("scans");
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let rows = vec![vec![0usize, 1, 2], vec![3, 4, 5]];
        let params = vec![
            SP::Int8(1),
            SP::Text("x".into()),
            SP::Bool(true),
            SP::Int8(2),
            SP::Null,
            SP::Bool(false),
        ];
        let via_binds = try_bind_insert_batch(&s, &cols, &rows, &params).expect("builds");
        assert_eq!(via_scanner.len(), via_binds.len());
        assert_eq!(via_scanner[0], via_binds[0]);
    }

    #[test]
    fn bind_direct_subset_columns_null_fill_and_int4_widening() {
        use crate::prepared::ScalarParam as SP;
        let s = schema_iib();
        // Target only (a, c); b is NULL-filled. Int4 widens into the Int64 col.
        let cols = vec!["a".to_string(), "c".to_string()];
        let rows = vec![vec![0usize, 1]];
        let params = vec![SP::Int4(5), SP::Bool(false)];
        let b = try_bind_insert_batch(&s, &cols, &rows, &params).expect("builds");
        let r = &b[0];
        let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 5);
        let strs = r.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(strs.is_null(0));
    }

    #[test]
    fn bind_direct_declines_out_of_grammar_params() {
        use crate::prepared::ScalarParam as SP;
        let s = schema_iib();
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let rows = vec![vec![0usize, 1, 2]];
        // Bytea param → decline.
        assert!(try_bind_insert_batch(
            &s,
            &cols,
            &rows,
            &[SP::Int8(1), SP::Bytea(vec![1, 2]), SP::Bool(true)],
        )
        .is_none());
        // Array param → decline.
        assert!(try_bind_insert_batch(
            &s,
            &cols,
            &rows,
            &[SP::Int8(1), SP::Array(vec![SP::Int8(2)]), SP::Bool(true)],
        )
        .is_none());
        // Type mismatch (text into Int64 col) → decline.
        assert!(try_bind_insert_batch(
            &s,
            &cols,
            &rows,
            &[SP::Text("1".into()), SP::Text("x".into()), SP::Bool(true)],
        )
        .is_none());
        // NULL into NOT NULL column → decline.
        let strict = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(try_bind_insert_batch(
            &strict,
            &["a".to_string()],
            &[vec![0usize]],
            &[SP::Null],
        )
        .is_none());
        // Param index out of range → decline.
        assert!(try_bind_insert_batch(&s, &cols, &rows, &[SP::Int8(1)]).is_none());
        // Non-finite float → decline.
        let fschema = Schema::new(vec![Field::new("f", DataType::Float64, true)]);
        assert!(try_bind_insert_batch(
            &fschema,
            &["f".to_string()],
            &[vec![0usize]],
            &[SP::Float8(f64::NAN)],
        )
        .is_none());
    }

    #[test]
    fn bind_direct_declines_unknown_or_duplicate_columns() {
        use crate::prepared::ScalarParam as SP;
        let s = schema_iib();
        // Unknown column name → decline (schema may have drifted since Parse).
        assert!(try_bind_insert_batch(
            &s,
            &["nope".to_string()],
            &[vec![0usize]],
            &[SP::Int8(1)],
        )
        .is_none());
        // Duplicate target column → decline (slow path's canonical error).
        assert!(try_bind_insert_batch(
            &s,
            &["a".to_string(), "a".to_string()],
            &[vec![0usize, 1]],
            &[SP::Int8(1), SP::Int8(2)],
        )
        .is_none());
    }
}
