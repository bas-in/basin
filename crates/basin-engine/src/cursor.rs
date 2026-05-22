//! Session-scoped cursor registry.
//!
//! Basin v0.1 cursors are *fully materialised* at `DECLARE` time: the backing
//! `SELECT` is executed immediately and all result rows are collected into an
//! in-process `Vec<RecordBatch>`.  Subsequent `FETCH` calls slice rows out of
//! that buffer and advance a position counter; `CLOSE` drops the entry from
//! the per-session map.
//!
//! ## Limitations (v0.1)
//!
//! - **In-memory only**: no streaming for large result sets.  A `DECLARE …
//!   CURSOR FOR SELECT * FROM huge_table` will OOM before it returns.  A
//!   streaming variant (backed by a DataFusion physical plan) is the v0.2 path.
//! - **WITH HOLD not supported**: cursors survive only as long as their
//!   `ProjectSession`.  The `WITH HOLD` clause (PG-specific, allows the cursor
//!   to outlive a transaction commit) is parsed and silently ignored in v0.1.
//! - **SCROLL / NO SCROLL**: all cursors support backward motion regardless of
//!   the `SCROLL` keyword — we materialise everything so backward seeks are
//!   free.
//!
//! ## MOVE
//!
//! `MOVE <direction> FROM <cursor>` advances the cursor position without
//! returning rows.  sqlparser 0.52 has no `Statement::Move` AST node, so the
//! executor pre-screens the raw SQL textually (see `cursor::match_move`) and
//! dispatches to `exec_move` before the sqlparser parse step.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_common::{BasinError, Result};
use tokio::sync::Mutex;

/// A single open cursor bound to one session.
pub(crate) struct CursorState {
    /// Materialised result set, row-by-row across Arrow batches.
    pub(crate) schema: Arc<Schema>,
    pub(crate) batches: Vec<RecordBatch>,
    /// Total logical row count (sum of batch row counts).
    total_rows: usize,
    /// Current logical position.  `0` means "before the first row";
    /// `total_rows` means "after the last row" (past-the-end).
    position: usize,
}

impl CursorState {
    pub(crate) fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            schema,
            batches,
            total_rows,
            position: 0,
        }
    }

    /// Move the cursor by `delta` rows (positive = forward, negative =
    /// backward).  Clamps to `[0, total_rows]`.
    fn move_by(&mut self, delta: i64) {
        let new_pos = (self.position as i64).saturating_add(delta);
        self.position = new_pos.clamp(0, self.total_rows as i64) as usize;
    }

    /// Fetch up to `count` rows starting at the current position and advance
    /// the cursor past them.  Returns a slice of the materialised batches
    /// covering exactly those rows.
    fn fetch_forward(&mut self, count: usize) -> Vec<RecordBatch> {
        let start = self.position;
        let end = (start + count).min(self.total_rows);
        self.position = end;
        self.slice_rows(start, end)
    }

    /// Fetch up to `count` rows *before* the current position (scroll
    /// backward) and reposition the cursor to the start of the fetched window.
    fn fetch_backward(&mut self, count: usize) -> Vec<RecordBatch> {
        let end = self.position;
        let start = end.saturating_sub(count);
        self.position = start;
        self.slice_rows(start, end)
    }

    /// Return the single row at `row_idx` (0-based absolute) and set the
    /// cursor position past it.  Returns empty if out-of-range.
    fn fetch_absolute(&mut self, row_idx: i64) -> Vec<RecordBatch> {
        if row_idx < 0 || row_idx as usize >= self.total_rows {
            // Out of range: move to boundary, return nothing.
            self.position = if row_idx < 0 { 0 } else { self.total_rows };
            return vec![];
        }
        let idx = row_idx as usize;
        self.position = idx + 1;
        self.slice_rows(idx, idx + 1)
    }

    /// Extract rows `[start, end)` from the materialised batches.
    fn slice_rows(&self, start: usize, end: usize) -> Vec<RecordBatch> {
        if start >= end {
            return vec![];
        }
        let mut out: Vec<RecordBatch> = Vec::new();
        let mut offset = 0usize;
        for batch in &self.batches {
            let batch_end = offset + batch.num_rows();
            if batch_end <= start {
                offset = batch_end;
                continue;
            }
            if offset >= end {
                break;
            }
            let lo = start.saturating_sub(offset);
            let hi = (end - offset).min(batch.num_rows());
            if lo < hi {
                out.push(batch.slice(lo, hi - lo));
            }
            offset = batch_end;
        }
        out
    }
}

/// Per-session registry of open cursors.
pub(crate) struct CursorRegistry {
    inner: Mutex<HashMap<String, CursorState>>,
}

impl CursorRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Drop every open cursor. Used by the connection-pool scrub on
    /// return-to-pool (DISCARD ALL / `CLOSE ALL` semantics) so no cursor
    /// state crosses a Session-mode pooled checkout boundary.
    pub(crate) async fn clear_all(&self) {
        self.inner.lock().await.clear();
    }

    /// Store a new cursor under `name`.  Fails if a cursor by that name is
    /// already open (matches PG's `ERROR: cursor "x" already exists`).
    pub(crate) async fn declare(
        &self,
        name: String,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let mut map = self.inner.lock().await;
        if map.contains_key(&name) {
            return Err(BasinError::internal(format!(
                "cursor \"{name}\" already exists"
            )));
        }
        map.insert(name, CursorState::new(schema, batches));
        Ok(())
    }

    /// Remove and return the cursor under `name`.  Fails with PG-style error
    /// if not found.
    pub(crate) async fn close(&self, name: &str) -> Result<()> {
        let mut map = self.inner.lock().await;
        if map.remove(name).is_none() {
            return Err(BasinError::internal(format!(
                "cursor \"{name}\" does not exist"
            )));
        }
        Ok(())
    }

    /// Apply `direction` to cursor `name`, returning sliced batches (for
    /// FETCH) or an empty vec (for MOVE).
    pub(crate) async fn apply(
        &self,
        name: &str,
        direction: CursorDirection,
        emit_rows: bool,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
        let mut map = self.inner.lock().await;
        let cursor = map
            .get_mut(name)
            .ok_or_else(|| BasinError::internal(format!("cursor \"{name}\" does not exist")))?;

        let schema = cursor.schema.clone();
        let batches = if emit_rows {
            match direction {
                CursorDirection::Next => cursor.fetch_forward(1),
                CursorDirection::Prior => cursor.fetch_backward(1),
                CursorDirection::First => {
                    cursor.position = 0;
                    cursor.fetch_forward(1)
                }
                CursorDirection::Last => {
                    cursor.position = cursor.total_rows.saturating_sub(1);
                    cursor.fetch_forward(1)
                }
                CursorDirection::Count(n) => {
                    if n >= 0 {
                        cursor.fetch_forward(n as usize)
                    } else {
                        cursor.fetch_backward((-n) as usize)
                    }
                }
                CursorDirection::Absolute(n) => cursor.fetch_absolute(n),
                CursorDirection::Relative(n) => {
                    if n >= 0 {
                        cursor.fetch_forward(n as usize)
                    } else {
                        cursor.fetch_backward((-n) as usize)
                    }
                }
                CursorDirection::All => {
                    let remaining = cursor.total_rows - cursor.position;
                    cursor.fetch_forward(remaining)
                }
                CursorDirection::Forward(n) => cursor.fetch_forward(n.unwrap_or(1) as usize),
                CursorDirection::ForwardAll => {
                    let remaining = cursor.total_rows - cursor.position;
                    cursor.fetch_forward(remaining)
                }
                CursorDirection::Backward(n) => cursor.fetch_backward(n.unwrap_or(1) as usize),
                CursorDirection::BackwardAll => {
                    let at = cursor.position;
                    cursor.fetch_backward(at)
                }
            }
        } else {
            // MOVE: advance position, no rows emitted.
            match direction {
                CursorDirection::Next => cursor.move_by(1),
                CursorDirection::Prior => cursor.move_by(-1),
                CursorDirection::First => cursor.position = 0,
                CursorDirection::Last => cursor.position = cursor.total_rows,
                CursorDirection::Count(n) => cursor.move_by(n),
                CursorDirection::Absolute(n) => {
                    cursor.position = n.clamp(0, cursor.total_rows as i64) as usize;
                }
                CursorDirection::Relative(n) => cursor.move_by(n),
                CursorDirection::All => cursor.position = cursor.total_rows,
                CursorDirection::Forward(n) => cursor.move_by(n.unwrap_or(1)),
                CursorDirection::ForwardAll => cursor.position = cursor.total_rows,
                CursorDirection::Backward(n) => cursor.move_by(-(n.unwrap_or(1))),
                CursorDirection::BackwardAll => cursor.position = 0,
            }
            vec![]
        };

        Ok((schema, batches))
    }
}

/// Parsed direction from FETCH/MOVE statements.
#[derive(Debug, Clone)]
pub(crate) enum CursorDirection {
    Next,
    Prior,
    First,
    Last,
    Count(i64),
    Absolute(i64),
    Relative(i64),
    All,
    Forward(Option<i64>),
    ForwardAll,
    Backward(Option<i64>),
    BackwardAll,
}

impl CursorDirection {
    /// Convert from sqlparser's `FetchDirection`.
    pub(crate) fn from_sqlparser(d: &sqlparser::ast::FetchDirection) -> Result<Self> {
        use sqlparser::ast::FetchDirection;
        match d {
            FetchDirection::Next => Ok(Self::Next),
            FetchDirection::Prior => Ok(Self::Prior),
            FetchDirection::First => Ok(Self::First),
            FetchDirection::Last => Ok(Self::Last),
            FetchDirection::All => Ok(Self::All),
            FetchDirection::ForwardAll => Ok(Self::ForwardAll),
            FetchDirection::BackwardAll => Ok(Self::BackwardAll),
            FetchDirection::Count { limit } => Ok(Self::Count(value_to_i64(limit)?)),
            FetchDirection::Absolute { limit } => Ok(Self::Absolute(value_to_i64(limit)?)),
            FetchDirection::Relative { limit } => Ok(Self::Relative(value_to_i64(limit)?)),
            FetchDirection::Forward { limit } => {
                Ok(Self::Forward(limit.as_ref().map(value_to_i64).transpose()?))
            }
            FetchDirection::Backward { limit } => Ok(Self::Backward(
                limit.as_ref().map(value_to_i64).transpose()?,
            )),
        }
    }
}

fn value_to_i64(v: &sqlparser::ast::Value) -> Result<i64> {
    use sqlparser::ast::Value;
    match v {
        Value::Number(n, _) => n
            .parse::<i64>()
            .map_err(|_| BasinError::internal(format!("cursor: invalid count literal: {n}"))),
        other => Err(BasinError::internal(format!(
            "cursor: expected integer literal, got: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// MOVE pre-screener (sqlparser 0.52 has no Statement::Move AST node)
// ---------------------------------------------------------------------------

/// Parsed MOVE intent extracted by `match_move`.
#[derive(Debug)]
pub(crate) struct MoveIntent {
    pub(crate) cursor_name: String,
    pub(crate) direction: CursorDirection,
}

// The canonical MOVE pre-screener used from the executor.
pub(crate) fn match_move_sql(sql: &str) -> Option<MoveIntent> {
    let trimmed = sql.trim();
    // Fast path: first word must be MOVE (any case).
    let word_end = trimmed
        .find(|c: char| c.is_ascii_whitespace())
        .unwrap_or(trimmed.len());
    if !trimmed[..word_end].eq_ignore_ascii_case("MOVE") {
        return None;
    }
    let rest = trimmed[word_end..].trim_start();
    if rest.is_empty() {
        return None;
    }
    let synthetic = format!("FETCH {rest}");
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &synthetic).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    if let sqlparser::ast::Statement::Fetch {
        name, direction, ..
    } = &stmts[0]
    {
        let dir = CursorDirection::from_sqlparser(direction).ok()?;
        Some(MoveIntent {
            cursor_name: name.value.clone(),
            direction: dir,
        })
    } else {
        None
    }
}

// ==========================================================================
// MOVE — AST-based matcher (ADR 0014 Phase 5.13.B migration 11)
// ==========================================================================

/// AST-based matcher for `MOVE <direction> [FROM|IN] <cursor>`.
///
/// libpg_query routes MOVE as [`pg_query::protobuf::FetchStmt`] with
/// `ismove = true`.  The direction and count are encoded as:
///
/// | SQL form                     | direction | how_many     |
/// |------------------------------|-----------|--------------|
/// | NEXT / FORWARD 1 / (bare)    | Forward   | 1            |
/// | PRIOR / BACKWARD 1           | Backward  | 1            |
/// | FIRST                        | Absolute  | 1            |
/// | LAST                         | Absolute  | -1           |
/// | ALL / FORWARD ALL            | Forward   | `i64::MAX`   |
/// | BACKWARD ALL                 | Backward  | `i64::MAX`   |
/// | n (plain count)              | Forward   | n            |
/// | FORWARD n                    | Forward   | n            |
/// | BACKWARD n                   | Backward  | n            |
/// | ABSOLUTE n                   | Absolute  | n            |
/// | RELATIVE n                   | Relative  | n            |
///
/// Returns a [`MoveIntent`] with the cursor name and the corresponding
/// [`CursorDirection`].
pub(crate) fn match_move_sql_ast(
    stmt: &pg_query::protobuf::FetchStmt,
) -> Result<MoveIntent> {
    use pg_query::protobuf::FetchDirection;

    let cursor_name = stmt.portalname.clone();
    if cursor_name.is_empty() {
        return Err(BasinError::internal(
            "MOVE: missing cursor name in AST".to_string(),
        ));
    }

    let direction = FetchDirection::try_from(stmt.direction).unwrap_or(FetchDirection::FetchForward);
    let n = stmt.how_many;
    let i64_max = i64::MAX;

    let dir = match direction {
        FetchDirection::FetchForward => {
            if n == i64_max {
                CursorDirection::ForwardAll
            } else if n == 1 {
                CursorDirection::Next
            } else {
                CursorDirection::Forward(Some(n))
            }
        }
        FetchDirection::FetchBackward => {
            if n == i64_max {
                CursorDirection::BackwardAll
            } else if n == 1 {
                CursorDirection::Prior
            } else {
                CursorDirection::Backward(Some(n))
            }
        }
        FetchDirection::FetchAbsolute => {
            if n == 1 {
                CursorDirection::First
            } else if n == -1 {
                CursorDirection::Last
            } else {
                CursorDirection::Absolute(n)
            }
        }
        FetchDirection::FetchRelative => CursorDirection::Relative(n),
        FetchDirection::Undefined => CursorDirection::Next,
    };

    Ok(MoveIntent {
        cursor_name,
        direction: dir,
    })
}
