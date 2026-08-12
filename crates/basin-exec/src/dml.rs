//! `Insert`, `Update` and `Delete` — DML as a physical relation.
//!
//! # DML is a relation here, not a side channel
//!
//! `basin_plan::LogicalPlan::Insert`/`Update`/`Delete` each carry a
//! `returning: Option<Vec<(Expr, String)>>` and nest inside
//! `LogicalPlan::Cte`, so `WITH x AS (INSERT ... RETURNING ...) SELECT ...
//! FROM x` has to be plannable. `basin-plan/src/lib.rs`'s module doc records
//! that this is one of exactly two shapes the previous engine could not
//! express at all (`executor.rs:3128`), and it is why these operators
//! implement [`Operator`] like everything else in this crate rather than
//! returning a bare row count: `RETURNING` rows are an ordinary
//! [`RecordBatch`] stream, pullable by whatever sits above this node in the
//! plan (a `Project` to pick out the `RETURNING` list, another CTE
//! reference, ...). See item 1/2 in this file's tests for the two shapes
//! that follow from that (`RETURNING` present vs. absent).
//!
//! # Storage is a seam, not a dependency
//!
//! Mirroring `scan.rs`'s [`crate::scan::BatchSource`], these operators do not
//! depend on `basin-storage` — they write through [`RowSink`], so every rule
//! below is testable with [`MemoryRowSink`] alone. `basin-storage` plugs in
//! a real implementation later; nothing here changes when it does.
//!
//! # Column positions, not `Expr` — and what that leaves out of scope
//!
//! Like `aggregate.rs`'s `AggregateSpec` and `scan.rs`'s `projection`, these
//! operators take pre-resolved column positions rather than
//! `basin_plan::Expr`. Concretely:
//!
//! - **`INSERT`'s defaults and `Update`'s `SET` expressions** are assumed
//!   already evaluated into the row values these operators receive — an
//!   upstream `Project` is expected to have computed `DEFAULT`s and
//!   `SET col = <expr>` against the old row, the same way `aggregate.rs`
//!   expects a `Project` to have materialised `sum(a + b)` into its own
//!   column before the aggregate ever sees it. So `Insert`/`Update`'s input
//!   schema IS the row exactly as it will be written.
//! - **`ON CONFLICT ... DO UPDATE SET ...`** is therefore reduced to
//!   "replace the conflicting row wholesale with the incoming one"
//!   ([`ConflictAction::DoUpdate`]) — the equivalent of every column's `SET`
//!   being `EXCLUDED.col`. A `SET` list narrower than the full row, or an
//!   `ON CONFLICT ... WHERE` predicate, is out of scope for this operator;
//!   see [`ConflictAction`]'s doc comment.
//! - **A row's identity** (what makes an `UPDATE`/`DELETE` target row, and
//!   what an `ON CONFLICT` target column set means) is a caller-supplied
//!   `key_cols: Vec<usize>` — positions in the row schema, not a `ColId` or
//!   an index lookup. `Update` assumes the key columns' values do not
//!   themselves change between the old and new row (an `UPDATE` that
//!   rewrites its own primary key is not modelled at this layer).
//!
//! # `UPDATE`/`DELETE` matching is upstream, not here
//!
//! Both operators assume `input` already yields exactly the rows the
//! statement's `WHERE` matched — via a `Scan` + `Filter` beneath this node,
//! same as any other physical plan. That is why "no matching rows" (item 5)
//! needs no special-case here: an empty/absent match is just an input that
//! yields nothing, which every operator below already handles.
//!
//! # `UPDATE`/`ON CONFLICT DO UPDATE` write as delete-then-insert
//!
//! [`RowSink`] has no `update` method on purpose — an update is modelled as
//! removing the old row by its key and inserting the new one, the same
//! decomposition many MVCC stores use natively (old version deleted/expired,
//! new version inserted). `Update::next_batch` and `Insert`'s conflict path
//! both do exactly this.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::{concat_batches, take};
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, SchemaRef};

use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// Reorder every column of `batch` by `indices`, keeping `batch`'s own
/// schema. Same helper `setop.rs`/`sort.rs`/`join.rs` each keep a private
/// copy of; not worth sharing across files that predate one another.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, ExecError> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(arrow_err)
}

fn empty_like(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::new_empty(Arc::clone(schema))
}

// ============================================================================
// RowSink
// ============================================================================

/// Where `Insert`/`Update`/`Delete` write. The seam that keeps this file
/// testable without `basin-storage` — see the module docs.
///
/// # Contract
///
/// - `insert` receives rows in the *full write schema* (every column of the
///   target table, in table-column order) — never a key-only projection.
///   Its return value is "how many of the given rows were actually written":
///   for a row whose key already exists (a uniqueness conflict as the sink
///   defines it), an implementation must skip that row and NOT count it,
///   rather than overwrite it or error — callers (in particular
///   `Insert`'s `ON CONFLICT` handling) rely on being able to tell a
///   conflict apart from success purely from the returned count. The
///   simplest way to make that unambiguous for a caller is to call `insert`
///   one row at a time, which is exactly what `Insert` does whenever
///   `ON CONFLICT` is present (see [`Insert::next_batch`]).
/// - `delete` receives a batch already narrowed to the *key columns only*,
///   in whatever column order the caller's `key_cols` used to build it — a
///   `RowSink` implementation must key on that same order (see
///   [`MemoryRowSink::new`]). Its return value is the number of rows that
///   actually matched and were removed; deleting a key that is not present
///   is not an error, it contributes 0.
/// - Both methods must tolerate a zero-row batch as a no-op that writes
///   nothing (item 6 in this file's tests) — not, for instance, one
///   all-NULL row.
pub trait RowSink {
    fn insert(&mut self, batch: &RecordBatch) -> Result<u64, ExecError>;
    fn delete(&mut self, keys: &RecordBatch) -> Result<u64, ExecError>;
}

/// An in-memory [`RowSink`] keyed on `key_cols`. Exists for tests, and
/// stands in for `basin-storage` until a real sink implements the trait —
/// nothing about `Insert`/`Update`/`Delete` changes when that happens.
///
/// Rows are stored as one-row `RecordBatch`es in a `HashMap` keyed by the
/// [`arrow::row`] encoding of `key_cols` — the same NULL-collapsing,
/// hashable row encoding `Distinct`/`SetOp` use in `setop.rs`, chosen here
/// for the same reason: it turns "does a row with this key already exist"
/// into a plain hash lookup without hand-rolling per-type comparison.
pub struct MemoryRowSink {
    schema: SchemaRef,
    key_cols: Vec<usize>,
    row_converter: RowConverter,
    rows: HashMap<OwnedRow, RecordBatch>,
}

impl MemoryRowSink {
    /// `schema` is the full write schema (matching every `insert` call's
    /// batch); `key_cols` are positions within it that form this sink's
    /// uniqueness key — must be non-empty, since a sink with no key columns
    /// cannot distinguish "conflict" from "new row" or resolve a `delete`.
    pub fn new(schema: SchemaRef, key_cols: Vec<usize>) -> Self {
        assert!(
            !key_cols.is_empty(),
            "MemoryRowSink requires at least one key column"
        );
        let key_fields: Vec<SortField> = key_cols
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect();
        let row_converter =
            RowConverter::new(key_fields).expect("key columns support row encoding");
        Self {
            schema,
            key_cols,
            row_converter,
            rows: HashMap::new(),
        }
    }

    fn keys_of(&self, batch: &RecordBatch) -> Result<arrow::row::Rows, ExecError> {
        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        self.row_converter
            .convert_columns(&key_arrays)
            .map_err(arrow_err)
    }

    /// Every row currently held, concatenated into one batch in whatever
    /// (unspecified) order the underlying `HashMap` iterates — tests sort
    /// before asserting. Empty rather than `None` when the sink holds
    /// nothing, matching this crate's usual "zero rows, not zero batches"
    /// convention.
    pub fn snapshot(&self) -> Result<RecordBatch, ExecError> {
        if self.rows.is_empty() {
            return Ok(empty_like(&self.schema));
        }
        concat_batches(&self.schema, self.rows.values()).map_err(arrow_err)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl RowSink for MemoryRowSink {
    fn insert(&mut self, batch: &RecordBatch) -> Result<u64, ExecError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let keys = self.keys_of(batch)?;
        let mut written = 0u64;
        for i in 0..batch.num_rows() {
            let key = keys.row(i).owned();
            if self.rows.contains_key(&key) {
                continue; // conflict: not written, not counted — see the trait doc.
            }
            self.rows.insert(key, batch.slice(i, 1));
            written += 1;
        }
        Ok(written)
    }

    fn delete(&mut self, keys: &RecordBatch) -> Result<u64, ExecError> {
        if keys.num_rows() == 0 {
            return Ok(0);
        }
        // `keys` is already narrowed to just the key columns, in `key_cols`
        // order (the caller built it via `RecordBatch::project(&key_cols)`)
        // — so every one of its own columns, in order, is the encoding this
        // sink's `row_converter` expects.
        let key_arrays: Vec<ArrayRef> = (0..keys.num_columns())
            .map(|i| Arc::clone(keys.column(i)))
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&key_arrays)
            .map_err(arrow_err)?;
        let mut removed = 0u64;
        for i in 0..keys.num_rows() {
            let key = rows.row(i).owned();
            if self.rows.remove(&key).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

// ============================================================================
// Insert
// ============================================================================

/// `ON CONFLICT` behaviour for [`Insert`] — the physical-layer analogue of
/// `basin_plan::OnConflict`, narrowed to what a column-position-only
/// operator can express; see the module docs' "Column positions, not
/// `Expr`" section for what that leaves out (arbitrary `SET` lists, `WHERE`
/// on the conflict action).
#[derive(Debug, Clone)]
pub enum ConflictAction {
    /// `ON CONFLICT (...) DO NOTHING`: a conflicting row is skipped — not
    /// written, not an error, and (item 3) not present in `RETURNING`.
    DoNothing,
    /// `ON CONFLICT (...) DO UPDATE`: a conflicting row is replaced
    /// wholesale by the incoming row (delete the old one by `key_cols`,
    /// insert the new one) and the new row appears in `RETURNING` (item 4).
    /// `key_cols` are positions in the row schema — the same columns the
    /// sink itself conflicts on, so they can be extracted from the row that
    /// just failed to insert without a second lookup.
    DoUpdate { key_cols: Vec<usize> },
}

/// `INSERT` as a physical operator: pulls already-fully-formed rows (see the
/// module docs) from `input`, writes them through `sink`, and — only when
/// `RETURNING` was requested — yields the rows as written.
pub struct Insert {
    input: Box<dyn Operator>,
    sink: Box<dyn RowSink>,
    schema: SchemaRef,
    on_conflict: Option<ConflictAction>,
    want_returning: bool,
    affected: u64,
}

impl Insert {
    /// `input`'s schema is the full write schema. `want_returning` is
    /// whether the statement has a `RETURNING` clause at all — see item 2:
    /// without it, every yielded batch has zero rows, but [`Self::affected_rows`]
    /// still accumulates.
    pub fn new(
        input: Box<dyn Operator>,
        sink: Box<dyn RowSink>,
        on_conflict: Option<ConflictAction>,
        want_returning: bool,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            sink,
            schema,
            on_conflict,
            want_returning,
            affected: 0,
        }
    }

    /// Total rows written so far (across every batch pulled to date) —
    /// the count a `INSERT 0 n` command tag needs, independent of whether
    /// `RETURNING` rows were also produced.
    pub fn affected_rows(&self) -> u64 {
        self.affected
    }

    /// No `ON CONFLICT`: the whole batch is handed to the sink in one call
    /// (the fast path — no per-row conflict bookkeeping needed). Any row the
    /// sink silently drops without a caller-visible reason is treated as an
    /// unresolved uniqueness conflict — matching Postgres, which errors on a
    /// duplicate key when there is no `ON CONFLICT` clause to catch it.
    fn insert_without_conflict_handling(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, ExecError> {
        let written = self.sink.insert(batch)?;
        if written as usize != batch.num_rows() {
            return Err(ExecError::Internal(format!(
                "insert wrote {written} of {} rows; a duplicate key with no ON CONFLICT clause \
                 is a constraint violation",
                batch.num_rows()
            )));
        }
        self.affected += written;
        Ok(if self.want_returning {
            batch.clone()
        } else {
            empty_like(&self.schema)
        })
    }

    /// `ON CONFLICT` present: rows are written one at a time so a sink's
    /// per-row `insert` return value (0 or 1, unambiguous at batch size 1)
    /// tells us definitively whether *this* row conflicted — the only way
    /// to know which rows belong in `RETURNING` (items 3 and 4).
    fn insert_with_conflict_handling(
        &mut self,
        batch: &RecordBatch,
        action: &ConflictAction,
    ) -> Result<RecordBatch, ExecError> {
        let mut written_rows: Vec<u32> = Vec::new();
        for i in 0..batch.num_rows() {
            let row = batch.slice(i, 1);
            let written = self.sink.insert(&row)?;
            if written == 1 {
                self.affected += 1;
                written_rows.push(i as u32);
                continue;
            }
            // A conflict: `written == 0`.
            match action {
                ConflictAction::DoNothing => {
                    // Skipped entirely — not written, not counted, not in
                    // RETURNING (item 3).
                }
                ConflictAction::DoUpdate { key_cols } => {
                    let key = row.project(key_cols).map_err(arrow_err)?;
                    self.sink.delete(&key)?;
                    let written = self.sink.insert(&row)?;
                    if written != 1 {
                        return Err(ExecError::Internal(
                            "ON CONFLICT DO UPDATE failed to write the row after removing the \
                             conflicting one"
                                .into(),
                        ));
                    }
                    self.affected += 1;
                    written_rows.push(i as u32);
                }
            }
        }

        if !self.want_returning {
            return Ok(empty_like(&self.schema));
        }
        if written_rows.is_empty() {
            return Ok(empty_like(&self.schema));
        }
        take_batch(batch, &UInt32Array::from(written_rows))
    }
}

impl Operator for Insert {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        if batch.num_rows() == 0 {
            // Nothing to write — an empty input batch must not become one
            // all-NULL row (item 6).
            return Ok(Some(empty_like(&self.schema)));
        }
        let out = match self.on_conflict.clone() {
            None => self.insert_without_conflict_handling(&batch)?,
            Some(action) => self.insert_with_conflict_handling(&batch, &action)?,
        };
        Ok(Some(out))
    }
}

// ============================================================================
// Update
// ============================================================================

/// `UPDATE` as a physical operator. `input` yields rows already computed as
/// the row's NEW values in full (see the module docs) — this operator's job
/// is purely mechanical: for each row, delete the old version by its key and
/// insert the new one, accumulate the affected count, and (if `RETURNING`
/// was requested) yield the new values back out.
pub struct Update {
    input: Box<dyn Operator>,
    sink: Box<dyn RowSink>,
    key_cols: Vec<usize>,
    schema: SchemaRef,
    want_returning: bool,
    affected: u64,
}

impl Update {
    /// `key_cols` are positions in `input`'s schema identifying the target
    /// row — assumed unchanged between old and new values (see the module
    /// docs' note on primary-key-rewriting updates being out of scope).
    pub fn new(
        input: Box<dyn Operator>,
        sink: Box<dyn RowSink>,
        key_cols: Vec<usize>,
        want_returning: bool,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            sink,
            key_cols,
            schema,
            want_returning,
            affected: 0,
        }
    }

    /// Total rows updated so far.
    pub fn affected_rows(&self) -> u64 {
        self.affected
    }
}

impl Operator for Update {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        if batch.num_rows() == 0 {
            // Item 6: nothing matched in this (already-filtered) batch, so
            // nothing is written — not an all-NULL row.
            return Ok(Some(empty_like(&self.schema)));
        }

        let keys = batch.project(&self.key_cols).map_err(arrow_err)?;
        self.sink.delete(&keys)?;
        let written = self.sink.insert(&batch)?;
        self.affected += written;

        Ok(Some(if self.want_returning {
            batch // the NEW values, per item 1 — never the pre-update row.
        } else {
            empty_like(&self.schema)
        }))
    }
}

// ============================================================================
// Delete
// ============================================================================

/// `DELETE` as a physical operator. `input` yields the OLD rows the
/// statement's `WHERE` matched, in full — `RETURNING` for a `DELETE` is
/// unambiguous (there is only ever one version of the row to report: the one
/// that existed right before it was removed), so this operator needs no
/// separate "as written" transform the way `Insert`/`Update` do.
pub struct Delete {
    input: Box<dyn Operator>,
    sink: Box<dyn RowSink>,
    key_cols: Vec<usize>,
    schema: SchemaRef,
    want_returning: bool,
    affected: u64,
}

impl Delete {
    pub fn new(
        input: Box<dyn Operator>,
        sink: Box<dyn RowSink>,
        key_cols: Vec<usize>,
        want_returning: bool,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            sink,
            key_cols,
            schema,
            want_returning,
            affected: 0,
        }
    }

    /// Total rows deleted so far.
    pub fn affected_rows(&self) -> u64 {
        self.affected
    }
}

impl Operator for Delete {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        if batch.num_rows() == 0 {
            return Ok(Some(empty_like(&self.schema)));
        }

        let keys = batch.project(&self.key_cols).map_err(arrow_err)?;
        let removed = self.sink.delete(&keys)?;
        self.affected += removed;

        Ok(Some(if self.want_returning {
            batch
        } else {
            empty_like(&self.schema)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::VecDeque;

    /// A fixed sequence of pre-built batches fed to the operator under test
    /// one at a time — same test double every other file in this crate
    /// defines locally (`aggregate.rs`'s `VecOperator`, `setop.rs`'s
    /// `Feed`).
    struct Feed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Feed {
        fn boxed(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
            Box::new(Feed {
                schema,
                batches: batches.into(),
            })
        }
    }

    impl Operator for Feed {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            Ok(self.batches.pop_front())
        }
    }

    /// `(id: Int64, val: Utf8)` — the row shape used across most tests
    /// below; `id` (column 0) is the key column everywhere it matters.
    fn row_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn rows(schema: &SchemaRef, ids: Vec<i64>, vals: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .unwrap()
    }

    fn ids_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn vals_of(batch: &RecordBatch) -> Vec<Option<String>> {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn drain<O: Operator + ?Sized>(op: &mut O) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.push(b);
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ── MemoryRowSink, directly ────────────────────────────────────────────

    // The wrong answer this prevents: a sink that overwrites (or errors on)
    // a duplicate key instead of silently declining it and reporting a
    // smaller written count — `Insert`'s conflict handling is built entirely
    // on trusting that count.
    #[test]
    fn sink_insert_skips_a_duplicate_key_and_reports_only_the_new_row() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        assert_eq!(
            sink.insert(&rows(&schema, vec![1], vec![Some("a")]))
                .unwrap(),
            1
        );
        assert_eq!(
            sink.insert(&rows(&schema, vec![1], vec![Some("b")]))
                .unwrap(),
            0,
            "id=1 already exists; the duplicate must not be written"
        );
        assert_eq!(sink.len(), 1);
        assert_eq!(vals_of(&sink.snapshot().unwrap()), vec![Some("a".into())]);
    }

    #[test]
    fn sink_delete_reports_only_keys_actually_present() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]))
            .unwrap();
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let removed = sink
            .delete(
                &RecordBatch::try_new(key_schema, vec![Arc::new(Int64Array::from(vec![2, 999]))])
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            removed, 1,
            "only id=2 exists; the missing id=999 contributes nothing, not an error"
        );
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn sink_insert_of_an_empty_batch_writes_nothing() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        let empty = empty_like(&schema);
        assert_eq!(sink.insert(&empty).unwrap(), 0);
        assert!(sink.is_empty(), "an empty batch must not create one row");
    }

    // ── Insert ──────────────────────────────────────────────────────────

    // Item 1: RETURNING yields the rows exactly as written, including
    // defaults — modelled here as a column already filled in by (the
    // stand-in for) an upstream Project, per the module docs. The wrong
    // answer this catches is RETURNING echoing something other than what was
    // actually handed to the sink (e.g. NULL, or the pre-default value).
    #[test]
    fn insert_returning_yields_rows_as_written_with_defaults_filled_in() {
        let schema = row_schema();
        // val is NULL in the statement but a Project upstream has already
        // resolved it to the column default "d".
        let batch = rows(&schema, vec![1, 2], vec![Some("d"), Some("d")]);
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let mut insert = Insert::new(input, sink, None, true);

        let out = drain(&mut insert);
        assert_eq!(total_rows(&out), 2);
        assert_eq!(
            vals_of(&out[0]),
            vec![Some("d".into()), Some("d".into())],
            "RETURNING must show the defaulted value that was actually written"
        );
        assert_eq!(insert.affected_rows(), 2);
    }

    // Item 2: without RETURNING, every yielded batch has zero rows, but the
    // affected count still accumulates. The wrong answer this catches is
    // conflating "no RETURNING clause" with "nothing happened".
    #[test]
    fn insert_without_returning_yields_no_rows_but_reports_affected_count() {
        let schema = row_schema();
        let batch = rows(
            &schema,
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
        );
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let mut insert = Insert::new(input, sink, None, false);

        let out = drain(&mut insert);
        assert_eq!(
            total_rows(&out),
            0,
            "no RETURNING clause means zero rows out, ever"
        );
        assert_eq!(
            insert.affected_rows(),
            3,
            "the affected count must still reflect all 3 writes"
        );
    }

    // Item 3: ON CONFLICT DO NOTHING skips conflicting rows and they must
    // not appear in RETURNING. The wrong answer this catches is RETURNING
    // echoing the whole input batch regardless of which rows actually made
    // it into the sink.
    #[test]
    fn insert_on_conflict_do_nothing_skips_conflicts_from_returning() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![2], vec![Some("old")]))
            .unwrap();
        let sink: Box<dyn RowSink> = Box::new(sink);

        let batch = rows(
            &schema,
            vec![1, 2, 3],
            vec![Some("a"), Some("new"), Some("c")],
        );
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let mut insert = Insert::new(input, sink, Some(ConflictAction::DoNothing), true);

        let out = drain(&mut insert);
        let mut returned_ids = out.iter().flat_map(ids_of).collect::<Vec<_>>();
        returned_ids.sort();
        assert_eq!(
            returned_ids,
            vec![1, 3],
            "id=2 conflicted and DO NOTHING must exclude it from RETURNING"
        );
        assert_eq!(
            insert.affected_rows(),
            2,
            "only the two non-conflicting rows were written"
        );
    }

    // Item 4: ON CONFLICT DO UPDATE yields the UPDATED row in RETURNING. The
    // wrong answer this catches is either dropping the conflicting row (like
    // DO NOTHING would) or returning the stale pre-conflict value.
    #[test]
    fn insert_on_conflict_do_update_yields_the_updated_row_in_returning() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![2], vec![Some("old")]))
            .unwrap();

        let batch = rows(&schema, vec![2], vec![Some("new")]);
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let mut insert = Insert::new(
            input,
            Box::new(sink),
            Some(ConflictAction::DoUpdate { key_cols: vec![0] }),
            true,
        );

        let out = drain(&mut insert);
        assert_eq!(total_rows(&out), 1);
        assert_eq!(
            vals_of(&out[0]),
            vec![Some("new".into())],
            "RETURNING must reflect the post-conflict UPDATE, not the stale row"
        );
        assert_eq!(insert.affected_rows(), 1);
    }

    // Item 6: a batch that arrives with zero rows must write nothing — not
    // one all-NULL row. Exercised through the operator (not just the sink
    // directly) so the operator's own short-circuit is covered too.
    #[test]
    fn insert_of_an_empty_batch_writes_nothing() {
        let schema = row_schema();
        let input = Feed::boxed(schema.clone(), vec![empty_like(&schema)]);
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let mut insert = Insert::new(input, sink, None, true);

        let out = drain(&mut insert);
        assert_eq!(total_rows(&out), 0);
        assert_eq!(insert.affected_rows(), 0);
    }

    // A duplicate key reaching Insert with no ON CONFLICT clause at all must
    // be a hard error (matching Postgres), not a silently dropped row.
    #[test]
    fn insert_without_on_conflict_errors_on_a_duplicate_key() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1], vec![Some("a")]))
            .unwrap();
        let input = Feed::boxed(
            schema.clone(),
            vec![rows(&schema, vec![1], vec![Some("b")])],
        );
        let mut insert = Insert::new(input, Box::new(sink), None, true);
        let err = insert.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::Internal(_)), "{err:?}");
    }

    #[test]
    fn insert_accumulates_affected_count_across_multiple_batches() {
        let schema = row_schema();
        let b1 = rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]);
        let b2 = rows(&schema, vec![3], vec![Some("c")]);
        let input = Feed::boxed(schema.clone(), vec![b1, b2]);
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let mut insert = Insert::new(input, sink, None, true);

        drain(&mut insert);
        assert_eq!(insert.affected_rows(), 3);
    }

    #[test]
    fn insert_memory_used_is_always_zero() {
        // This operator hands every row straight to the sink rather than
        // buffering it — memory_used honestly reports that there is nothing
        // held here (the sink's own memory is the sink's concern).
        let schema = row_schema();
        let input = Feed::boxed(
            schema.clone(),
            vec![rows(&schema, vec![1], vec![Some("a")])],
        );
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let mut insert = Insert::new(input, sink, None, true);
        assert_eq!(insert.memory_used(), 0);
        insert.next_batch().unwrap();
        assert_eq!(insert.memory_used(), 0);
    }

    // ── Update ──────────────────────────────────────────────────────────

    // Item 1 (Update half): RETURNING yields the NEW values, never the old
    // ones — the wrong answer this catches is an implementation that reads
    // RETURNING from the pre-update sink state.
    #[test]
    fn update_returning_yields_new_values_not_old() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1], vec![Some("old")]))
            .unwrap();

        // Already-matched, already-computed new row (as an upstream
        // Scan+Filter+Project would hand it over).
        let new_row = rows(&schema, vec![1], vec![Some("new")]);
        let input = Feed::boxed(schema.clone(), vec![new_row]);
        let mut update = Update::new(input, Box::new(sink), vec![0], true);

        let out = drain(&mut update);
        assert_eq!(total_rows(&out), 1);
        assert_eq!(vals_of(&out[0]), vec![Some("new".into())]);
        assert_eq!(update.affected_rows(), 1);
    }

    // Item 2 (Update half).
    #[test]
    fn update_without_returning_yields_no_rows_but_reports_affected_count() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]))
            .unwrap();
        let new_rows = rows(&schema, vec![1, 2], vec![Some("a2"), Some("b2")]);
        let input = Feed::boxed(schema.clone(), vec![new_rows]);
        let mut update = Update::new(input, Box::new(sink), vec![0], false);

        let out = drain(&mut update);
        assert_eq!(total_rows(&out), 0);
        assert_eq!(update.affected_rows(), 2);
    }

    // Item 5: UPDATE with no matching rows affects 0 and raises no error —
    // modelled as the (already WHERE-filtered) input simply yielding
    // nothing. The wrong answer this catches is panicking, or treating "no
    // input" as an error condition rather than the ordinary 0-row case.
    #[test]
    fn update_with_no_matching_rows_affects_zero_and_errors_on_nothing() {
        let schema = row_schema();
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let input = Feed::boxed(schema.clone(), vec![]);
        let mut update = Update::new(input, sink, vec![0], true);

        assert!(update.next_batch().unwrap().is_none());
        assert_eq!(update.affected_rows(), 0);
    }

    // Item 6 (Update half).
    #[test]
    fn update_of_an_empty_batch_writes_nothing() {
        let schema = row_schema();
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let input = Feed::boxed(schema.clone(), vec![empty_like(&schema)]);
        let mut update = Update::new(input, sink, vec![0], true);

        let out = drain(&mut update);
        assert_eq!(total_rows(&out), 0);
        assert_eq!(update.affected_rows(), 0);
    }

    #[test]
    fn update_spans_multiple_input_batches() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(
            &schema,
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
        ))
        .unwrap();
        let b1 = rows(&schema, vec![1], vec![Some("a2")]);
        let b2 = rows(&schema, vec![2, 3], vec![Some("b2"), Some("c2")]);
        let input = Feed::boxed(schema.clone(), vec![b1, b2]);
        let mut update = Update::new(input, Box::new(sink), vec![0], true);

        let out = drain(&mut update);
        let mut vals: Vec<String> = out.iter().flat_map(vals_of).map(|v| v.unwrap()).collect();
        vals.sort();
        assert_eq!(vals, vec!["a2", "b2", "c2"]);
        assert_eq!(update.affected_rows(), 3);
    }

    #[test]
    fn update_memory_used_is_always_zero() {
        let schema = row_schema();
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let input = Feed::boxed(
            schema.clone(),
            vec![rows(&schema, vec![1], vec![Some("a")])],
        );
        let mut update = Update::new(input, sink, vec![0], true);
        assert_eq!(update.memory_used(), 0);
        update.next_batch().unwrap();
        assert_eq!(update.memory_used(), 0);
    }

    // ── Delete ──────────────────────────────────────────────────────────

    #[test]
    fn delete_returning_yields_the_deleted_row() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]))
            .unwrap();
        let matched = rows(&schema, vec![2], vec![Some("b")]);
        let input = Feed::boxed(schema.clone(), vec![matched]);
        let mut delete = Delete::new(input, Box::new(sink), vec![0], true);

        let out = drain(&mut delete);
        assert_eq!(total_rows(&out), 1);
        assert_eq!(ids_of(&out[0]), vec![2]);
        assert_eq!(delete.affected_rows(), 1);
    }

    // Item 2 (Delete half).
    #[test]
    fn delete_without_returning_yields_no_rows_but_reports_affected_count() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]))
            .unwrap();
        let matched = rows(&schema, vec![1, 2], vec![Some("a"), Some("b")]);
        let input = Feed::boxed(schema.clone(), vec![matched]);
        let mut delete = Delete::new(input, Box::new(sink), vec![0], false);

        let out = drain(&mut delete);
        assert_eq!(total_rows(&out), 0);
        assert_eq!(delete.affected_rows(), 2);
    }

    // Item 5 (Delete half): no matching rows affects 0, no error.
    #[test]
    fn delete_with_no_matching_rows_affects_zero_and_errors_on_nothing() {
        let schema = row_schema();
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let input = Feed::boxed(schema.clone(), vec![]);
        let mut delete = Delete::new(input, sink, vec![0], true);

        assert!(delete.next_batch().unwrap().is_none());
        assert_eq!(delete.affected_rows(), 0);
    }

    // Item 6 (Delete half).
    #[test]
    fn delete_of_an_empty_batch_writes_nothing() {
        let schema = row_schema();
        let sink = Box::new(MemoryRowSink::new(schema.clone(), vec![0]));
        let input = Feed::boxed(schema.clone(), vec![empty_like(&schema)]);
        let mut delete = Delete::new(input, sink, vec![0], true);

        let out = drain(&mut delete);
        assert_eq!(total_rows(&out), 0);
        assert_eq!(delete.affected_rows(), 0);
    }

    #[test]
    fn delete_spans_multiple_input_batches() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(
            &schema,
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
        ))
        .unwrap();
        let b1 = rows(&schema, vec![1], vec![Some("a")]);
        let b2 = rows(&schema, vec![2, 3], vec![Some("b"), Some("c")]);
        let input = Feed::boxed(schema.clone(), vec![b1, b2]);
        let sink_box: Box<dyn RowSink> = Box::new(sink);
        let mut delete = Delete::new(input, sink_box, vec![0], false);

        drain(&mut delete);
        assert_eq!(delete.affected_rows(), 3);
    }

    #[test]
    fn delete_memory_used_is_always_zero() {
        let schema = row_schema();
        let mut sink = MemoryRowSink::new(schema.clone(), vec![0]);
        sink.insert(&rows(&schema, vec![1], vec![Some("a")]))
            .unwrap();
        let input = Feed::boxed(
            schema.clone(),
            vec![rows(&schema, vec![1], vec![Some("a")])],
        );
        let mut delete = Delete::new(input, Box::new(sink), vec![0], true);
        assert_eq!(delete.memory_used(), 0);
        delete.next_batch().unwrap();
        assert_eq!(delete.memory_used(), 0);
    }
}
