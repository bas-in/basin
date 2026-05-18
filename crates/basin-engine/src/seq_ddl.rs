// pg_query AST matchers staged for ADR 0014 Phase 2; suppress dead-code.
#![allow(dead_code)]

//! `CREATE SEQUENCE` / `DROP SEQUENCE` SQL surface (5.11.K3).
//!
//! sqlparser 0.52 has native AST nodes for both forms
//! (`Statement::CreateSequence` and `Statement::Drop` with
//! `ObjectType::Sequence`), so the executor dispatches on the parsed
//! AST rather than via a textual pre-screen. This module owns the
//! AST → catalog mapping; the underlying machinery
//! ([`basin_catalog::Catalog::create_sequence`] /
//! [`basin_catalog::Catalog::drop_sequence`]) already shipped under
//! 5.11.K3.
//!
//! Grammar accepted (PG-shaped subset):
//!
//! ```sql
//! CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name>
//!     [ START [WITH] <n> ]
//!     [ INCREMENT [BY] <n> ]
//!     [ MINVALUE <n> | NO MINVALUE ]
//!     [ MAXVALUE <n> | NO MAXVALUE ]
//!     [ CACHE <n> ]
//!     [ [NO] CYCLE ]
//! ;
//!
//! DROP SEQUENCE [IF EXISTS] <name>;
//! ```
//!
//! `TEMPORARY` is rejected (sequences are project-scoped, not session-
//! scoped, in v0.1). `OWNED BY` is parsed by sqlparser as
//! `Statement::CreateSequence::owned_by` but ignored here — the
//! sequence catalog has no concept of column-attached ownership in
//! v0.1; future `DROP COLUMN` cascade can revisit this.
//!
//! ## Identity-style column DEFAULT
//!
//! `CREATE TABLE t (id BIGINT DEFAULT nextval('s'))` parses cleanly
//! (sqlparser accepts function-call defaults) and the DEFAULT
//! expression text is stored on the column's Arrow `Field` metadata
//! (see `crate::types::BASIN_COLUMN_DEFAULT`). At INSERT time, when
//! the column is omitted, the DEFAULT text is routed through
//! [`crate::seq_udf::rewrite_sequence_calls`] before being parsed back
//! into an `Expr` and substituted into the row. This shares the same
//! string-rewrite machinery the top-level executor pre-screen uses
//! for inline `nextval(...)` calls — no separate UDF bridge.

use crate::pg_ast::ObjectNamePartExt;
use std::sync::Arc;

use basin_catalog::{Catalog, SequenceDef};
use basin_common::{BasinError, ProjectId, Result};
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{Expr, ObjectName, SequenceOptions, UnaryOperator, Value};

use crate::{ExecResult, ProjectSession};

/// Text-based intent for the textual `match_create_sequence` pre-screen.
/// Mirrors the variants of `sqlparser::ast::SequenceOptions` we care
/// about, but does not depend on the sqlparser AST so the call site can
/// route directly to the catalog without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SequenceOption {
    /// `START [WITH] <n>`.
    Start(i64),
    /// `INCREMENT [BY] <n>`.
    Increment(i64),
    /// `MINVALUE <n>`; `None` for `NO MINVALUE`.
    MinValue(Option<i64>),
    /// `MAXVALUE <n>`; `None` for `NO MAXVALUE`.
    MaxValue(Option<i64>),
    /// `CACHE <n>`.
    Cache(i64),
    /// `[NO] CYCLE`. `true` means the user wrote `NO CYCLE` (matches
    /// sqlparser's `SequenceOptions::Cycle(no)` convention).
    Cycle(bool),
}

/// Recognised `CREATE SEQUENCE` shape, lifted out of the raw SQL by
/// [`match_create_sequence`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateSequenceIntent {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub options: Vec<SequenceOption>,
}

/// Translate a sqlparser `Statement::CreateSequence` shape into a
/// [`SequenceDef`] and register it via
/// [`Catalog::create_sequence`].
pub(crate) async fn exec_create_sequence(
    sess: &ProjectSession,
    temporary: bool,
    if_not_exists: bool,
    name: ObjectName,
    sequence_options: Vec<SequenceOptions>,
) -> Result<ExecResult> {
    if temporary {
        return Err(BasinError::InvalidSchema(
            "CREATE TEMPORARY SEQUENCE is not supported; sequences are project-scoped".into(),
        ));
    }
    let seq_name = single_part_object_name(&name)?;

    let mut def = SequenceDef::with_defaults(sess.project, seq_name.clone());
    apply_options(&mut def, &sequence_options)?;

    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    match catalog.create_sequence(def).await {
        Ok(()) => {}
        Err(BasinError::Catalog(_)) if if_not_exists => {
            // Catalog returns a `Catalog`-class error on duplicate
            // registration; with `IF NOT EXISTS` the user opted into
            // idempotency.
        }
        Err(e) => return Err(e),
    }
    Ok(ExecResult::Empty {
        tag: "CREATE SEQUENCE".into(),
    })
}

/// Drop a sequence by `(project, name)`. Mirrors the rest of the DDL
/// surface in routing each name in a multi-name `DROP` through the
/// same catalog method one at a time.
pub(crate) async fn exec_drop_sequence(
    sess: &ProjectSession,
    if_exists: bool,
    names: &[ObjectName],
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP SEQUENCE: at least one sequence name is required".into(),
        ));
    }
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    for name in names {
        let seq_name = single_part_object_name(name)?;
        match catalog.drop_sequence(&sess.project, &seq_name).await {
            Ok(()) => {}
            Err(BasinError::NotFound(_)) if if_exists => {}
            Err(BasinError::NotFound(_)) => {
                return Err(BasinError::not_found(format!(
                    "sequence {seq_name:?} does not exist"
                )));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP SEQUENCE".into(),
    })
}

/// Apply each parsed `SequenceOption` onto `def`. Diverges from PG
/// only where the catalog API does: `cache_size` is a `u64` so
/// `CACHE 0` is rejected (PG accepts `CACHE 1`, treats as the same;
/// `CACHE 0` is not legal in PG either).
fn apply_options(def: &mut SequenceDef, options: &[SequenceOptions]) -> Result<()> {
    let mut start_seen = false;
    for opt in options {
        match opt {
            SequenceOptions::IncrementBy(expr, _by) => {
                let n = parse_signed_int(expr, "INCREMENT")?;
                if n == 0 {
                    return Err(BasinError::InvalidSchema(
                        "CREATE SEQUENCE: INCREMENT must be non-zero".into(),
                    ));
                }
                def.increment = n;
                // Keep PG-shaped min/max defaults aligned with the new
                // step direction unless the user explicitly overrides.
                // We only do this when `MINVALUE` / `MAXVALUE` haven't
                // been set yet — the loop visits options in source
                // order, so a later explicit MINVALUE / MAXVALUE
                // overrides this in the next iteration.
                if def.min_value == 1 && def.max_value == i64::MAX && n < 0 {
                    def.min_value = i64::MIN + 1;
                    def.max_value = -1;
                }
            }
            SequenceOptions::StartWith(expr, _with) => {
                let n = parse_signed_int(expr, "START")?;
                def.start = n;
                start_seen = true;
            }
            SequenceOptions::MinValue(Some(expr)) => {
                def.min_value = parse_signed_int(expr, "MINVALUE")?;
            }
            SequenceOptions::MinValue(None) => {
                def.min_value = if def.increment < 0 { i64::MIN + 1 } else { 1 };
            }
            SequenceOptions::MaxValue(Some(expr)) => {
                def.max_value = parse_signed_int(expr, "MAXVALUE")?;
            }
            SequenceOptions::MaxValue(None) => {
                def.max_value = if def.increment < 0 { -1 } else { i64::MAX };
            }
            SequenceOptions::Cache(expr) => {
                let n = parse_signed_int(expr, "CACHE")?;
                if n < 1 {
                    return Err(BasinError::InvalidSchema(
                        "CREATE SEQUENCE: CACHE must be >= 1".into(),
                    ));
                }
                def.cache_size = n as u64;
            }
            SequenceOptions::Cycle(no) => {
                // sqlparser uses `Cycle(no)` where `no == true` means
                // the user wrote `NO CYCLE`.
                def.cycle = !*no;
            }
        }
    }
    if !start_seen {
        // PG: when START is omitted, default is min_value (positive
        // step) or max_value (negative). If neither was specified the
        // computed default in `with_defaults` (start=1) is fine for
        // positive step.
        if def.increment < 0 && def.start == 1 {
            def.start = def.max_value;
        } else if def.min_value != 1 && def.start == 1 {
            def.start = def.min_value;
        }
    }
    if def.min_value > def.max_value {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE: MINVALUE ({}) must not exceed MAXVALUE ({})",
            def.min_value, def.max_value
        )));
    }
    if def.start < def.min_value || def.start > def.max_value {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE: START ({}) is outside [MINVALUE..MAXVALUE] ({}..{})",
            def.start, def.min_value, def.max_value
        )));
    }
    Ok(())
}

/// Parse a sqlparser numeric `Expr` into an `i64`, accepting an
/// optional unary minus. Rejects anything else (column refs,
/// function calls, etc.) so a malformed sequence option bubbles up
/// with a clear error.
fn parse_signed_int(expr: &Expr, opt_name: &str) -> Result<i64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => s.parse::<i64>().map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE SEQUENCE {opt_name}: invalid integer {s:?} ({e})"
            ))
        }),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }) => {
                let n: i64 = s.parse().map_err(|e| {
                    BasinError::InvalidSchema(format!(
                        "CREATE SEQUENCE {opt_name}: invalid integer {s:?} ({e})"
                    ))
                })?;
                Ok(-n)
            }
            other => Err(BasinError::InvalidSchema(format!(
                "CREATE SEQUENCE {opt_name}: expected integer literal, got -{other}"
            ))),
        },
        other => Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE {opt_name}: expected integer literal, got {other}"
        ))),
    }
}

/// Pull a bare-identifier sequence name out of an `ObjectName`.
/// Schema-qualified names are out of scope (matches the rest of the
/// engine's DDL surface).
fn single_part_object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified sequence names not supported in v0.1: {name}"
        )));
    }
    Ok(name.0[0].id_val().clone())
}

/// At INSERT time, evaluate a stored DEFAULT expression text.
///
/// The DEFAULT text is whatever the user wrote in
/// `CREATE TABLE t (col TYPE DEFAULT <expr>)`. We route it through
/// [`crate::seq_udf::rewrite_sequence_calls`] (so any `nextval(...)` /
/// `currval(...)` / `setval(...)` calls dispatch to the catalog) and
/// then parse the result as a single SQL expression, returning the
/// AST.
///
/// This is the load-bearing piece for `DEFAULT nextval('seq')` —
/// without it, the rewriter never sees the DEFAULT text (it only ran
/// on the original SQL string, which contained no `nextval` if the
/// user wrote `INSERT INTO t (other_col) VALUES ('a')`).
pub(crate) async fn evaluate_default_expression(
    sess: &ProjectSession,
    default_text: &str,
) -> Result<Expr> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let seq_ctx = crate::seq_udf::SequenceContext {
        catalog: &sess.engine.config().catalog,
        project: sess.project,
        session_cache: &sess.state.sequence_cache,
    };
    let rewritten = crate::seq_udf::rewrite_sequence_calls(default_text, &seq_ctx).await?;

    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&rewritten)
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "DEFAULT expression {default_text:?} does not parse: {e}"
            ))
        })?;
    parser.parse_expr().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "DEFAULT expression {default_text:?} does not parse: {e}"
        ))
    })
}

// Quiet `dead_code` while we land the SQL surface; the
// `evaluate_default_expression` path is referenced from `executor.rs`.
#[allow(dead_code)]
fn _suppress_unused_project_id(_: ProjectId) {}

/// Recognise `CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opt …]`
/// textually before sqlparser sees the SQL. sqlparser 0.52 only parses a
/// single option per `CREATE SEQUENCE` statement, so the multi-option PG
/// grammar (`START 100 INCREMENT 5 MINVALUE 1 MAXVALUE 1000 CACHE 1
/// NO CYCLE`) fails at the second option. This matcher claims any
/// statement that begins with `CREATE [TEMPORARY] SEQUENCE` and parses
/// the full PG option set; on a match the caller routes to
/// [`exec_create_sequence_pre_screen`].
///
/// Returns `Ok(None)` for any statement that isn't `CREATE SEQUENCE`
/// (e.g. `CREATE TABLE`, `DROP SEQUENCE`, `SELECT 1`). Returns
/// `Err(InvalidSchema)` when the shape starts as `CREATE SEQUENCE` but
/// the rest of the statement is malformed (missing name, unknown
/// option, non-integer literal, …).
pub(crate) fn match_create_sequence(sql: &str) -> Result<Option<CreateSequenceIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "CREATE") {
        return Ok(None);
    }
    let after_create = skip_word(trimmed).trim_start();
    let (after_temp, temporary) =
        if starts_with_kw(after_create, "TEMPORARY") || starts_with_kw(after_create, "TEMP") {
            (skip_word(after_create).trim_start(), true)
        } else {
            (after_create, false)
        };
    if !starts_with_kw(after_temp, "SEQUENCE") {
        return Ok(None);
    }
    let after_seq = skip_word(after_temp).trim_start();
    // Optional `IF NOT EXISTS`.
    let (after_ine, if_not_exists) = if starts_with_kw(after_seq, "IF") {
        let after_if = skip_word(after_seq).trim_start();
        if !starts_with_kw(after_if, "NOT") {
            return Err(BasinError::InvalidSchema(
                "CREATE SEQUENCE IF: expected NOT EXISTS".into(),
            ));
        }
        let after_not = skip_word(after_if).trim_start();
        if !starts_with_kw(after_not, "EXISTS") {
            return Err(BasinError::InvalidSchema(
                "CREATE SEQUENCE IF NOT: expected EXISTS".into(),
            ));
        }
        (skip_word(after_not).trim_start(), true)
    } else {
        (after_seq, false)
    };
    let (name, after_name) = read_simple_identifier(after_ine)?;
    // Walk the remaining tokens looking for option keywords. Order is
    // user-controlled — PG accepts any ordering — so we loop on whatever
    // the next keyword is and pick the matching arm.
    let mut rest = after_name.trim_start();
    let mut options: Vec<SequenceOption> = Vec::new();
    while !rest.is_empty() {
        if starts_with_kw(rest, "START") {
            let after = skip_word(rest).trim_start();
            // Optional `WITH`.
            let after = if starts_with_kw(after, "WITH") {
                skip_word(after).trim_start()
            } else {
                after
            };
            let (n, next) = read_signed_int(after, "START")?;
            options.push(SequenceOption::Start(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "INCREMENT") {
            let after = skip_word(rest).trim_start();
            // Optional `BY`.
            let after = if starts_with_kw(after, "BY") {
                skip_word(after).trim_start()
            } else {
                after
            };
            let (n, next) = read_signed_int(after, "INCREMENT")?;
            options.push(SequenceOption::Increment(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "MINVALUE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "MINVALUE")?;
            options.push(SequenceOption::MinValue(Some(n)));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "MAXVALUE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "MAXVALUE")?;
            options.push(SequenceOption::MaxValue(Some(n)));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "CACHE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "CACHE")?;
            options.push(SequenceOption::Cache(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "CYCLE") {
            let after = skip_word(rest).trim_start();
            options.push(SequenceOption::Cycle(false));
            rest = after.trim_start();
        } else if starts_with_kw(rest, "NO") {
            let after = skip_word(rest).trim_start();
            if starts_with_kw(after, "MINVALUE") {
                options.push(SequenceOption::MinValue(None));
                rest = skip_word(after).trim_start();
            } else if starts_with_kw(after, "MAXVALUE") {
                options.push(SequenceOption::MaxValue(None));
                rest = skip_word(after).trim_start();
            } else if starts_with_kw(after, "CYCLE") {
                options.push(SequenceOption::Cycle(true));
                rest = skip_word(after).trim_start();
            } else {
                return Err(BasinError::InvalidSchema(format!(
                    "CREATE SEQUENCE: expected MINVALUE / MAXVALUE / CYCLE after NO, got {:?}",
                    after.chars().take(16).collect::<String>()
                )));
            }
        } else {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE SEQUENCE: unrecognised token {:?}",
                rest.chars().take(16).collect::<String>()
            )));
        }
    }
    Ok(Some(CreateSequenceIntent {
        temporary,
        if_not_exists,
        name,
        options,
    }))
}

/// Pre-screen executor: take a [`CreateSequenceIntent`] (produced by
/// [`match_create_sequence`]), build a [`SequenceDef`] from it, and
/// register via [`Catalog::create_sequence`]. Mirrors
/// [`exec_create_sequence`] (the AST-driven path) but routes through
/// the text-based intent so it's reachable without sqlparser.
pub(crate) async fn exec_create_sequence_pre_screen(
    sess: &ProjectSession,
    intent: CreateSequenceIntent,
) -> Result<ExecResult> {
    if intent.temporary {
        return Err(BasinError::InvalidSchema(
            "CREATE TEMPORARY SEQUENCE is not supported; sequences are project-scoped".into(),
        ));
    }
    // Translate intent options into the same `SequenceOptions` shape
    // that `apply_options` already understands, so both pre-screen and
    // AST paths share one mapper.
    let parsed = intent
        .options
        .into_iter()
        .map(intent_option_to_sequence_options)
        .collect::<Vec<_>>();
    let mut def = SequenceDef::with_defaults(sess.project, intent.name.clone());
    apply_options(&mut def, &parsed)?;

    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    match catalog.create_sequence(def).await {
        Ok(()) => {}
        Err(BasinError::Catalog(_)) if intent.if_not_exists => {}
        Err(e) => return Err(e),
    }
    Ok(ExecResult::Empty {
        tag: "CREATE SEQUENCE".into(),
    })
}

/// Translate a text intent option to the sqlparser shape that
/// `apply_options` consumes.
fn intent_option_to_sequence_options(opt: SequenceOption) -> SequenceOptions {
    match opt {
        SequenceOption::Start(n) => SequenceOptions::StartWith(int_to_expr(n), false),
        SequenceOption::Increment(n) => SequenceOptions::IncrementBy(int_to_expr(n), false),
        SequenceOption::MinValue(Some(n)) => SequenceOptions::MinValue(Some(int_to_expr(n))),
        SequenceOption::MinValue(None) => SequenceOptions::MinValue(None),
        SequenceOption::MaxValue(Some(n)) => SequenceOptions::MaxValue(Some(int_to_expr(n))),
        SequenceOption::MaxValue(None) => SequenceOptions::MaxValue(None),
        SequenceOption::Cache(n) => SequenceOptions::Cache(int_to_expr(n)),
        SequenceOption::Cycle(no) => SequenceOptions::Cycle(no),
    }
}

fn int_to_expr(n: i64) -> Expr {
    if n < 0 {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Value(
                Value::Number((-(n as i128)).to_string(), false).into(),
            )),
        }
    } else {
        Expr::Value((Value::Number(n.to_string(), false)).into())
    }
}

// --- Lexer helpers (mirrors the textual matchers in `cv_ddl.rs`) -------

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

fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "CREATE SEQUENCE: expected an identifier, got end of statement".into(),
        ));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "CREATE SEQUENCE: expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

/// Read an `i64` literal from the head of `s`, optionally with a leading
/// unary minus. Returns `(n, rest_after_literal)`. Whitespace between
/// `-` and the digits is tolerated (`- 1` is `-1`); PG accepts the same.
fn read_signed_int<'a>(s: &'a str, opt_name: &str) -> Result<(i64, &'a str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE {opt_name}: expected an integer, got end of statement"
        )));
    }
    let (negative, rest) = if bytes[0] == b'-' {
        (true, s[1..].trim_start())
    } else if bytes[0] == b'+' {
        (false, s[1..].trim_start())
    } else {
        (false, s)
    };
    let rb = rest.as_bytes();
    let mut i = 0;
    while i < rb.len() && rb[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE {opt_name}: expected an integer at {:?}",
            rest.chars().take(8).collect::<String>()
        )));
    }
    let digits = &rest[..i];
    let mut n: i64 = digits.parse().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE {opt_name}: invalid integer {digits:?} ({e})"
        ))
    })?;
    if negative {
        n = -n;
    }
    Ok((n, &rest[i..]))
}

// ==========================================================================
// ALTER SEQUENCE — text-based pre-screen
// ==========================================================================

/// Recognised intent for `ALTER SEQUENCE [IF EXISTS] <name> [opt …]`.
/// The option set mirrors [`CreateSequenceIntent`] but all fields are
/// `Option`-wrapped because any single option may be absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlterSequenceIntent {
    pub if_exists: bool,
    pub name: String,
    pub options: Vec<SequenceOption>,
}

/// Recognise `ALTER SEQUENCE [IF EXISTS] <name> [opt …]` textually.
///
/// sqlparser 0.52 has no `Statement::AlterSequence` AST node; this
/// pre-screen handles the full PG grammar, routing to
/// [`exec_alter_sequence`]. Returns `Ok(None)` for any statement that
/// doesn't start with `ALTER SEQUENCE`.
pub(crate) fn match_alter_sequence(sql: &str) -> Result<Option<AlterSequenceIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "SEQUENCE") {
        return Ok(None);
    }
    let after_seq = skip_word(after_alter).trim_start();
    // Optional `IF EXISTS`.
    let (after_if, if_exists) = if starts_with_kw(after_seq, "IF") {
        let after_if = skip_word(after_seq).trim_start();
        if !starts_with_kw(after_if, "EXISTS") {
            return Err(BasinError::InvalidSchema(
                "ALTER SEQUENCE IF: expected EXISTS".into(),
            ));
        }
        (skip_word(after_if).trim_start(), true)
    } else {
        (after_seq, false)
    };
    let (name, after_name) = read_simple_identifier(after_if)?;
    // Parse options — same set as CREATE SEQUENCE.
    let mut rest = after_name.trim_start();
    let mut options: Vec<SequenceOption> = Vec::new();
    while !rest.is_empty() {
        if starts_with_kw(rest, "START") {
            let after = skip_word(rest).trim_start();
            let after = if starts_with_kw(after, "WITH") {
                skip_word(after).trim_start()
            } else {
                after
            };
            let (n, next) = read_signed_int(after, "START")?;
            options.push(SequenceOption::Start(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "RESTART") {
            // `RESTART [WITH] <n>` — same semantics as `setval(seq, n, false)`
            // (arms the sequence so the next nextval returns n).  Stored as
            // a `Start` option; `exec_alter_sequence` applies it via setval.
            let after = skip_word(rest).trim_start();
            let after = if starts_with_kw(after, "WITH") {
                skip_word(after).trim_start()
            } else {
                after
            };
            // Peek: if the next token is a number, read it; otherwise
            // RESTART without a value means "restart at the sequence's
            // original start value" — we encode that as Start(i64::MIN)
            // as a sentinel (exec_alter_sequence recognises this).
            let first_byte = after.bytes().next();
            if first_byte
                .map(|b| b.is_ascii_digit() || b == b'-' || b == b'+')
                .unwrap_or(false)
            {
                let (n, next) = read_signed_int(after, "RESTART")?;
                options.push(SequenceOption::Start(n));
                rest = next.trim_start();
            } else {
                // `RESTART` without value → restart at original START.
                // Encode as `i64::MIN` sentinel; exec layer handles it.
                options.push(SequenceOption::Start(i64::MIN));
                rest = after.trim_start();
            }
        } else if starts_with_kw(rest, "INCREMENT") {
            let after = skip_word(rest).trim_start();
            let after = if starts_with_kw(after, "BY") {
                skip_word(after).trim_start()
            } else {
                after
            };
            let (n, next) = read_signed_int(after, "INCREMENT")?;
            options.push(SequenceOption::Increment(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "MINVALUE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "MINVALUE")?;
            options.push(SequenceOption::MinValue(Some(n)));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "MAXVALUE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "MAXVALUE")?;
            options.push(SequenceOption::MaxValue(Some(n)));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "CACHE") {
            let after = skip_word(rest).trim_start();
            let (n, next) = read_signed_int(after, "CACHE")?;
            options.push(SequenceOption::Cache(n));
            rest = next.trim_start();
        } else if starts_with_kw(rest, "CYCLE") {
            options.push(SequenceOption::Cycle(false));
            rest = skip_word(rest).trim_start();
        } else if starts_with_kw(rest, "NO") {
            let after = skip_word(rest).trim_start();
            if starts_with_kw(after, "MINVALUE") {
                options.push(SequenceOption::MinValue(None));
                rest = skip_word(after).trim_start();
            } else if starts_with_kw(after, "MAXVALUE") {
                options.push(SequenceOption::MaxValue(None));
                rest = skip_word(after).trim_start();
            } else if starts_with_kw(after, "CYCLE") {
                options.push(SequenceOption::Cycle(true));
                rest = skip_word(after).trim_start();
            } else {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER SEQUENCE: expected MINVALUE / MAXVALUE / CYCLE after NO, got {:?}",
                    after.chars().take(16).collect::<String>()
                )));
            }
        } else if starts_with_kw(rest, "OWNED") {
            // `OWNED BY table.column` — accepted but ignored (v0.1 has no
            // column-ownership concept). Skip the next token (table.col or NONE).
            let after = skip_word(rest).trim_start();
            if starts_with_kw(after, "BY") {
                rest = skip_word(skip_word(after).trim_start()).trim_start();
            } else {
                rest = after;
            }
        } else {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER SEQUENCE: unrecognised token {:?}",
                rest.chars().take(16).collect::<String>()
            )));
        }
    }
    Ok(Some(AlterSequenceIntent {
        if_exists,
        name,
        options,
    }))
}

/// Execute `ALTER SEQUENCE [IF EXISTS] <name> [opt …]`.
///
/// In v0.1 we apply only `RESTART [WITH n]` (via `setval`) and
/// `OWNED BY` (ignored). Full `INCREMENT BY` / `MINVALUE` / `MAXVALUE`
/// / `CACHE` / `CYCLE` changes would require the catalog to replace the
/// stored `SequenceDef` — a v0.2 operation once the iceberg-backed
/// catalog supports atomic def updates. For now those options are
/// parsed and validated but produce a clear "not yet supported" error.
pub(crate) async fn exec_alter_sequence(
    sess: &ProjectSession,
    intent: AlterSequenceIntent,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();

    // Check existence before applying any option.
    let def = catalog.lookup_sequence(&sess.project, &intent.name).await;
    if def.is_none() {
        if intent.if_exists {
            return Ok(ExecResult::Empty {
                tag: "ALTER SEQUENCE".into(),
            });
        }
        return Err(BasinError::not_found(format!(
            "sequence {:?} does not exist",
            intent.name
        )));
    }
    let def = def.unwrap();

    for opt in &intent.options {
        match opt {
            SequenceOption::Start(n) => {
                // `RESTART [WITH n]` — restart the sequence at `n`.
                // Sentinel `i64::MIN` means "restart at original start".
                let restart_at = if *n == i64::MIN { def.start } else { *n };
                // setval(seq, restart_at, false) arms the sequence so the
                // next nextval returns exactly restart_at.
                catalog
                    .setval(&sess.project, &intent.name, restart_at, false)
                    .await?;
            }
            SequenceOption::Increment(_)
            | SequenceOption::MinValue(_)
            | SequenceOption::MaxValue(_)
            | SequenceOption::Cache(_)
            | SequenceOption::Cycle(_) => {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER SEQUENCE {}: changing {:?} requires a catalog def-update path \
                     not yet implemented; use DROP + CREATE SEQUENCE to change sequence \
                     parameters in v0.1",
                    intent.name,
                    match opt {
                        SequenceOption::Increment(_) => "INCREMENT",
                        SequenceOption::MinValue(_) => "MINVALUE",
                        SequenceOption::MaxValue(_) => "MAXVALUE",
                        SequenceOption::Cache(_) => "CACHE",
                        SequenceOption::Cycle(_) => "CYCLE",
                        SequenceOption::Start(_) => unreachable!(),
                    }
                )));
            }
        }
    }

    Ok(ExecResult::Empty {
        tag: "ALTER SEQUENCE".into(),
    })
}

// ==========================================================================
// pg_query AST-based matcher (alongside the textual one above)
// ==========================================================================

/// AST-based matcher for
/// `CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [option …]`.
///
/// libpg_query parses this natively as
/// [`pg_query::protobuf::CreateSeqStmt`]. Each sequence option arrives
/// as a `DefElem` node in `options` with `defname` equal to one of
/// `start`, `increment`, `minvalue`, `maxvalue`, `cache`, or `cycle`.
/// The `cycle` option carries a `Boolean` arg (`false` for `CYCLE`,
/// `true` for `NO CYCLE` — matching sqlparser's `Cycle(no)` convention).
///
/// Returns `Some(CreateSequenceIntent)` on a successful match; `None` is
/// never returned (the caller already knows this is a `CreateSeqStmt`),
/// but the signature is `Option` so agent 5's integration layer can
/// normalise the matcher shape.
pub(crate) fn match_create_sequence_ast(
    stmt: &pg_query::protobuf::CreateSeqStmt,
) -> Result<Option<CreateSequenceIntent>> {
    use pg_query::NodeEnum;

    let seq_rel = stmt.sequence.as_ref().ok_or_else(|| {
        BasinError::InvalidSchema("CREATE SEQUENCE: missing sequence relation in AST".into())
    })?;
    if seq_rel.relname.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE SEQUENCE: empty sequence name".into(),
        ));
    }
    let name = seq_rel.relname.clone();
    // TEMPORARY sequences: relpersistence == 't' in PG's internal encoding.
    let temporary = seq_rel.relpersistence == "t";
    let if_not_exists = stmt.if_not_exists;

    let mut options: Vec<SequenceOption> = Vec::new();
    for node in &stmt.options {
        let Some(NodeEnum::DefElem(de)) = node.node.as_ref() else {
            continue;
        };
        let arg_node = de.arg.as_deref();
        match de.defname.as_str() {
            "start" => {
                let n = int_from_node(arg_node, "START")?;
                options.push(SequenceOption::Start(n));
            }
            "increment" => {
                let n = int_from_node(arg_node, "INCREMENT")?;
                options.push(SequenceOption::Increment(n));
            }
            "minvalue" => {
                if let Some(arg) = arg_node {
                    let n = int_from_node(Some(arg), "MINVALUE")?;
                    options.push(SequenceOption::MinValue(Some(n)));
                } else {
                    // `NO MINVALUE` is represented as a DefElem with no arg.
                    options.push(SequenceOption::MinValue(None));
                }
            }
            "maxvalue" => {
                if let Some(arg) = arg_node {
                    let n = int_from_node(Some(arg), "MAXVALUE")?;
                    options.push(SequenceOption::MaxValue(Some(n)));
                } else {
                    options.push(SequenceOption::MaxValue(None));
                }
            }
            "cache" => {
                let n = int_from_node(arg_node, "CACHE")?;
                options.push(SequenceOption::Cache(n));
            }
            "cycle" => {
                // PG AST: `boolval` is whether the sequence *cycles*.
                //   boolval=true  → CYCLE  → sqlparser Cycle(no=false)
                //   boolval=false → NO CYCLE → sqlparser Cycle(no=true)
                // Default (no arg): treat as NO CYCLE (false).
                let cycles = bool_from_node(arg_node).unwrap_or(false);
                let no = !cycles; // sqlparser: no=true means NO CYCLE
                options.push(SequenceOption::Cycle(no));
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "CREATE SEQUENCE: unrecognised option {other:?} in AST"
                )));
            }
        }
    }

    Ok(Some(CreateSequenceIntent {
        temporary,
        if_not_exists,
        name,
        options,
    }))
}

/// Extract an `i64` from an AST `Node`. Accepts `Integer` nodes directly.
/// `arg_node` is `None` when the DefElem has no value (e.g. `NO MINVALUE`).
fn int_from_node(arg: Option<&pg_query::protobuf::Node>, opt: &str) -> Result<i64> {
    use pg_query::NodeEnum;
    let node = arg.ok_or_else(|| {
        BasinError::InvalidSchema(format!("CREATE SEQUENCE {opt}: missing value"))
    })?;
    match node.node.as_ref() {
        Some(NodeEnum::Integer(i)) => Ok(i.ival as i64),
        Some(NodeEnum::AConst(ac)) => {
            use pg_query::protobuf::a_const::Val;
            match ac.val.as_ref() {
                Some(Val::Ival(i)) => Ok(i.ival as i64),
                _ => Err(BasinError::InvalidSchema(format!(
                    "CREATE SEQUENCE {opt}: expected integer constant"
                ))),
            }
        }
        _ => Err(BasinError::InvalidSchema(format!(
            "CREATE SEQUENCE {opt}: expected integer node"
        ))),
    }
}

/// Extract a `bool` from an AST `Node`. Returns `None` when arg is absent
/// or not a Boolean node.
fn bool_from_node(arg: Option<&pg_query::protobuf::Node>) -> Option<bool> {
    use pg_query::NodeEnum;
    match arg?.node.as_ref()? {
        NodeEnum::Boolean(b) => Some(b.boolval),
        NodeEnum::AConst(ac) => {
            use pg_query::protobuf::a_const::Val;
            match ac.val.as_ref()? {
                Val::Boolval(b) => Some(b.boolval),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> sqlparser::ast::Statement {
        let dialect = PostgreSqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).expect("parse");
        assert_eq!(stmts.len(), 1);
        stmts.pop().unwrap()
    }

    /// Build a `SequenceOptions::IncrementBy(Number, _)` option.
    fn opt_increment(n: i64) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::IncrementBy(make_int_expr(n), false)
    }
    fn opt_start(n: i64) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::StartWith(make_int_expr(n), false)
    }
    fn opt_minvalue(n: Option<i64>) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::MinValue(n.map(make_int_expr))
    }
    fn opt_maxvalue(n: Option<i64>) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::MaxValue(n.map(make_int_expr))
    }
    fn opt_cache(n: i64) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::Cache(make_int_expr(n))
    }
    fn opt_cycle(no: bool) -> sqlparser::ast::SequenceOptions {
        sqlparser::ast::SequenceOptions::Cycle(no)
    }
    fn make_int_expr(n: i64) -> Expr {
        if n < 0 {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(Value::Number((-n).to_string(), false).into())),
            }
        } else {
            Expr::Value(Value::Number(n.to_string(), false).into())
        }
    }

    #[test]
    fn match_create_sequence_full_grammar() {
        let intent = match_create_sequence(
            "CREATE SEQUENCE foo START 100 INCREMENT 5 MINVALUE 1 MAXVALUE 1000 CACHE 4 NO CYCLE",
        )
        .unwrap()
        .expect("matched");
        assert_eq!(intent.name, "foo");
        assert!(!intent.temporary);
        assert!(!intent.if_not_exists);
        assert_eq!(
            intent.options,
            vec![
                SequenceOption::Start(100),
                SequenceOption::Increment(5),
                SequenceOption::MinValue(Some(1)),
                SequenceOption::MaxValue(Some(1000)),
                SequenceOption::Cache(4),
                SequenceOption::Cycle(true), // NO CYCLE => no=true
            ]
        );
    }

    #[test]
    fn match_create_sequence_canonical_pg_form() {
        let intent = match_create_sequence(
            "CREATE SEQUENCE IF NOT EXISTS foo START WITH 100 INCREMENT BY 5",
        )
        .unwrap()
        .expect("matched");
        assert!(intent.if_not_exists);
        assert_eq!(intent.name, "foo");
        assert_eq!(
            intent.options,
            vec![SequenceOption::Start(100), SequenceOption::Increment(5)]
        );
    }

    #[test]
    fn match_create_sequence_negative_increment() {
        let intent = match_create_sequence("CREATE SEQUENCE foo INCREMENT -1")
            .unwrap()
            .expect("matched");
        assert_eq!(intent.options, vec![SequenceOption::Increment(-1)]);
        // Drive the same intent through `apply_options` to confirm the
        // default min/max flip the existing sqlparser path performs is
        // preserved.
        let opts: Vec<_> = intent
            .options
            .into_iter()
            .map(intent_option_to_sequence_options)
            .collect();
        let mut def = SequenceDef::with_defaults(ProjectId::new(), "foo".to_string());
        apply_options(&mut def, &opts).unwrap();
        assert_eq!(def.increment, -1);
        assert_eq!(def.min_value, i64::MIN + 1);
        assert_eq!(def.max_value, -1);
    }

    #[test]
    fn match_create_sequence_returns_none_for_other_ddl() {
        assert!(match_create_sequence("CREATE TABLE foo (id BIGINT)")
            .unwrap()
            .is_none());
        assert!(match_create_sequence("DROP SEQUENCE foo")
            .unwrap()
            .is_none());
        assert!(match_create_sequence("SELECT 1").unwrap().is_none());
        assert!(match_create_sequence("ALTER SEQUENCE foo INCREMENT 2")
            .unwrap()
            .is_none());
    }

    #[test]
    fn match_create_sequence_temporary_accepted_by_matcher() {
        // The matcher accepts TEMPORARY (so the executor can produce a
        // stable error); the exec layer rejects it. Both `TEMPORARY` and
        // the PG-shorthand `TEMP` are recognised.
        let intent = match_create_sequence("CREATE TEMPORARY SEQUENCE foo")
            .unwrap()
            .expect("matched");
        assert!(intent.temporary);
        let intent = match_create_sequence("CREATE TEMP SEQUENCE foo")
            .unwrap()
            .expect("matched");
        assert!(intent.temporary);
    }

    #[test]
    fn match_create_sequence_no_minvalue_no_maxvalue() {
        let intent = match_create_sequence(
            "CREATE SEQUENCE foo MINVALUE 100 NO MINVALUE MAXVALUE 200 NO MAXVALUE",
        )
        .unwrap()
        .expect("matched");
        assert_eq!(
            intent.options,
            vec![
                SequenceOption::MinValue(Some(100)),
                SequenceOption::MinValue(None),
                SequenceOption::MaxValue(Some(200)),
                SequenceOption::MaxValue(None),
            ]
        );
    }

    #[test]
    fn match_create_sequence_with_trailing_semicolon() {
        let intent = match_create_sequence("CREATE SEQUENCE foo INCREMENT 5;")
            .unwrap()
            .expect("matched");
        assert_eq!(intent.name, "foo");
        assert_eq!(intent.options, vec![SequenceOption::Increment(5)]);
    }

    #[test]
    fn match_create_sequence_cycle_without_no() {
        let intent = match_create_sequence("CREATE SEQUENCE foo CYCLE")
            .unwrap()
            .expect("matched");
        assert_eq!(intent.options, vec![SequenceOption::Cycle(false)]);
    }

    #[test]
    fn match_create_sequence_missing_name_errors() {
        let err = match_create_sequence("CREATE SEQUENCE").unwrap_err();
        assert!(matches!(err, BasinError::InvalidIdent(_)));
    }

    #[test]
    fn match_create_sequence_unknown_option_errors() {
        let err = match_create_sequence("CREATE SEQUENCE foo NUDGE 5").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_sequence_bad_integer_errors() {
        let err = match_create_sequence("CREATE SEQUENCE foo INCREMENT abc").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn drop_sequence_parses_natively() {
        let stmt = parse_one("DROP SEQUENCE IF EXISTS foo");
        match stmt {
            sqlparser::ast::Statement::Drop {
                object_type,
                if_exists,
                ..
            } => {
                assert_eq!(object_type, sqlparser::ast::ObjectType::Sequence);
                assert!(if_exists);
            }
            other => panic!("expected DROP, got {other:?}"),
        }
    }

    #[test]
    fn apply_options_handles_full_grammar() {
        // Construct SequenceOptions directly because sqlparser 0.52
        // doesn't parse multi-option CREATE SEQUENCE in one statement.
        // This still tests the load-bearing `apply_options` mapping —
        // exactly what production sees once the parser surface is fed
        // a multi-option intent (via either a future textual pre-screen
        // or a sqlparser bump).
        let opts = vec![
            opt_start(100),
            opt_increment(5),
            opt_minvalue(Some(50)),
            opt_maxvalue(Some(200)),
            opt_cache(4),
            opt_cycle(false),
        ];
        let mut def = SequenceDef::with_defaults(ProjectId::new(), "foo".to_string());
        apply_options(&mut def, &opts).unwrap();
        assert_eq!(def.start, 100);
        assert_eq!(def.increment, 5);
        assert_eq!(def.min_value, 50);
        assert_eq!(def.max_value, 200);
        assert_eq!(def.cache_size, 4);
        assert!(def.cycle);
    }

    #[test]
    fn negative_increment_flips_default_min_max() {
        let stmt = parse_one("CREATE SEQUENCE foo INCREMENT -1");
        let sqlparser::ast::Statement::CreateSequence {
            sequence_options, ..
        } = stmt
        else {
            panic!()
        };
        let mut def = SequenceDef::with_defaults(ProjectId::new(), "foo".to_string());
        apply_options(&mut def, &sequence_options).unwrap();
        assert_eq!(def.increment, -1);
        assert_eq!(def.min_value, i64::MIN + 1);
        assert_eq!(def.max_value, -1);
    }

    #[test]
    fn no_minvalue_resets_to_default() {
        // Same rationale as apply_options_handles_full_grammar: bypass
        // sqlparser's single-option-per-statement limit by constructing
        // the option list directly. This exercises the apply_options
        // path that handles `MINVALUE 100` followed by `NO MINVALUE`.
        let opts = vec![opt_minvalue(Some(100)), opt_minvalue(None)];
        let mut def = SequenceDef::with_defaults(ProjectId::new(), "foo".to_string());
        apply_options(&mut def, &opts).unwrap();
        assert_eq!(def.min_value, 1);
    }

    #[test]
    fn zero_increment_rejected() {
        let stmt = parse_one("CREATE SEQUENCE foo INCREMENT 0");
        let sqlparser::ast::Statement::CreateSequence {
            sequence_options, ..
        } = stmt
        else {
            panic!()
        };
        let mut def = SequenceDef::with_defaults(ProjectId::new(), "foo".to_string());
        let err = apply_options(&mut def, &sequence_options).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    // --- AST-based matcher tests ------------------------------------------

    #[test]
    fn ast_match_create_sequence_full_options() {
        use pg_query::NodeEnum;
        let r = pg_query::parse(
            "CREATE SEQUENCE foo START 100 INCREMENT BY 5 MINVALUE 1 MAXVALUE 9999 NO CYCLE",
        )
        .unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateSeqStmt(ref css) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateSeqStmt");
        };
        let intent = match_create_sequence_ast(css).unwrap().unwrap();
        assert_eq!(intent.name, "foo");
        assert!(!intent.temporary);
        assert!(!intent.if_not_exists);
        assert_eq!(
            intent.options,
            vec![
                SequenceOption::Start(100),
                SequenceOption::Increment(5),
                SequenceOption::MinValue(Some(1)),
                SequenceOption::MaxValue(Some(9999)),
                SequenceOption::Cycle(true), // NO CYCLE → no = true
            ]
        );
    }

    #[test]
    fn ast_match_create_sequence_if_not_exists() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE SEQUENCE IF NOT EXISTS foo").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateSeqStmt(ref css) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateSeqStmt");
        };
        let intent = match_create_sequence_ast(css).unwrap().unwrap();
        assert_eq!(intent.name, "foo");
        assert!(intent.if_not_exists);
        assert!(intent.options.is_empty());
    }

    #[test]
    fn ast_match_create_sequence_negative_increment() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE SEQUENCE foo INCREMENT -1").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateSeqStmt(ref css) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateSeqStmt");
        };
        let intent = match_create_sequence_ast(css).unwrap().unwrap();
        assert_eq!(intent.options, vec![SequenceOption::Increment(-1)]);
    }
}
