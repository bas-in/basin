//! `HashJoin` — the equijoin (and `Cross`) join family.
//!
//! # Shape
//!
//! Build side is always RIGHT, fully materialized into one hash table before
//! any output row is produced. Probe side is always LEFT, pulled from the
//! child operator one batch at a time — so a join over a huge left input and
//! a small right input streams instead of buffering everything, and
//! cancellation is still checked between probe batches (see
//! [`crate::operator::Operator`]'s docs on why that matters).
//!
//! This is the opposite of the intuitive "build the smaller side" heuristic a
//! cost-based optimizer would pick, but nothing here does cost-based side
//! selection — the physical planner decides which input is `left` and which
//! is `right` before this operator exists, and `HashJoin` always builds on
//! `right`. That is a deliberate simplification, not an accident: see
//! `docs/migration/df-removal/03-physical-operators.md`.
//!
//! # Single partition only
//!
//! Basin pins `target_partitions = 1`, and raising it for a bulk scan
//! simultaneously disables repartitioning of joins (`executor.rs:10984`), so
//! there is never a partitioned or symmetric hash join to build here — one
//! hash table, one probe stream, full stop.
//!
//! # NULL never equals NULL
//!
//! A join key with a NULL in *any* of its columns cannot match anything,
//! including another row whose key is also all-NULL in the same columns.
//! [`arrow::row::RowConverter`]'s byte encoding does not help here: two NULL
//! keys encode to the *same* sentinel bytes regardless of the non-null value
//! they'd otherwise carry, so inserting every row's encoded key into the
//! hash table unconditionally would make every NULL-keyed row match every
//! other NULL-keyed row — `null_keys_never_match_each_other` below is the
//! test for exactly that. The fix applied throughout this module: a row
//! whose key has a NULL component is never inserted into the build-side hash
//! table, and never looked up on the probe side. Such a row can still be
//! *itself* an "unmatched" row for `Left`/`Right`/`Full`/`LeftAnti` — it
//! simply always takes that branch.
//!
//! # Memory
//!
//! [`HashJoin::memory_used`] reports the concatenated build-side (right)
//! table's array memory honestly, and `next_batch` refuses to buffer past
//! `memory_budget` while materializing it — see
//! `sort_reports_out_of_memory_past_its_budget` in `sort.rs` for the sibling
//! operator this mirrors. As with [`crate::sort::Sort`], there is no disk
//! spill; exceeding budget fails clean with [`ExecError::OutOfMemory`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use arrow_select::take::take;
use basin_plan::JoinKind;

use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// The output schema for a join of `left` and `right` under `kind`.
///
/// Mirrors `basin_plan::schema::join_schema`: `LeftSemi`/`LeftAnti` report
/// the left schema alone (a semi/anti join filters rows, it never widens
/// them), everything else concatenates left ++ right. Columns from a side
/// that can appear as all-NULL in the output (the non-preserved side of
/// `Left`/`Right`, both sides of `Full`) are forced nullable here, since this
/// is the physical Arrow schema batches actually get built against — unlike
/// `basin_plan::Schema`, which carries no nullability at all (see that
/// module's `KNOWN LIMITATION` note).
fn join_output_schema(left: &SchemaRef, right: &SchemaRef, kind: JoinKind) -> SchemaRef {
    fn nullable_fields(schema: &SchemaRef) -> Vec<Field> {
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect()
    }

    match kind {
        JoinKind::LeftSemi | JoinKind::LeftAnti => Arc::clone(left),
        JoinKind::Inner | JoinKind::Cross => {
            let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Left => {
            let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.extend(nullable_fields(right));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Right => {
            let mut fields = nullable_fields(left);
            fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Full => {
            let mut fields = nullable_fields(left);
            fields.extend(nullable_fields(right));
            Arc::new(Schema::new(fields))
        }
    }
}

/// Per-row: does any key column hold a NULL at this row? Computed once per
/// batch rather than re-checked per lookup.
fn null_key_mask(key_cols: &[ArrayRef], num_rows: usize) -> Vec<bool> {
    let mut mask = vec![false; num_rows];
    for col in key_cols {
        for (i, m) in mask.iter_mut().enumerate() {
            if col.is_null(i) {
                *m = true;
            }
        }
    }
    mask
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect()
}

fn take_by_indices(
    source: &RecordBatch,
    indices: &UInt32Array,
) -> Result<Vec<ArrayRef>, ExecError> {
    source
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect()
}

/// A fully-NULL run of `len` rows, shaped like `schema`. Used for the
/// unmatched-right tail of a `Right`/`Full` join, where there is no live
/// left batch left to `take` NULLs out of — the left child is already
/// exhausted by the time this runs.
fn null_columns(schema: &SchemaRef, len: usize) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_array::new_null_array(f.data_type(), len))
        .collect()
}

enum Phase {
    /// Materializing the right (build) side. Loops internally over the
    /// child, same documented gap as `Sort`/`TopK`: cannot honour
    /// `statement_timeout` mid-build.
    Building,
    /// Pulling one left (probe) batch at a time.
    Probing,
    /// `Right`/`Full` only: after the left side is exhausted, one final
    /// batch of right rows that were never matched, right side real values
    /// paired with all-NULL left columns.
    UnmatchedRight,
    Done,
}

/// Hash join. See module docs for the build/probe shape, the NULL-key rule,
/// and the memory-accounting contract.
pub struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    kind: JoinKind,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    budget: usize,
    schema: SchemaRef,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    row_converter: RowConverter,

    phase: Phase,
    right_bytes: usize,
    right_batches: Vec<RecordBatch>,
    /// Set once building completes; `None` only during `Phase::Building`.
    right_table: Option<RecordBatch>,
    /// Encoded key -> row indices into `right_table`. Rows whose key has a
    /// NULL component are never inserted (see module docs).
    hash_table: HashMap<OwnedRow, Vec<u32>>,
    /// One entry per row of `right_table`; set as matches are found while
    /// probing. Only consulted for `Right`/`Full`, but harmless (and cheap)
    /// to maintain for every kind.
    right_matched: Vec<bool>,
}

impl HashJoin {
    /// `left_keys`/`right_keys` are column positions into each side's own
    /// schema, one per equijoin conjunct, in matching order — `left_keys[i]`
    /// is compared against `right_keys[i]`. Empty for `Cross`, which ignores
    /// them entirely.
    ///
    /// # Panics
    ///
    /// If the right-side key columns' Arrow types cannot form a row
    /// encoding (`arrow::row::RowConverter::new` rejects them). Equijoin
    /// planning is expected to have already cast both sides' keys to a
    /// common type; reaching this is a planner bug, the same class of
    /// invariant `ExecError::TypeMismatch`'s doc comment describes for
    /// unrelated operators — but since it can only be evaluated once the
    /// concrete Arrow types are known, it is checked at construction rather
    /// than assumed silently.
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        kind: JoinKind,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let schema = join_output_schema(&left_schema, &right_schema, kind);

        // Bounds-check before indexing. `right_schema.field(i)` panics on an
        // out-of-range index, and a panic here aborts the session rather than
        // returning an error — the bridge's fallback-on-error safety net never
        // sees an unwind. This fired on `SELECT a.id FROM t a JOIN t b ON
        // a.id = b.id`, where projection pruning had removed every column from
        // the build side while the join key still referred to one of them.
        for (side, keys, schema) in [
            ("left", &left_keys, &left_schema),
            ("right", &right_keys, &right_schema),
        ] {
            if let Some(&bad) = keys.iter().find(|&&i| i >= schema.fields().len()) {
                return Err(ExecError::Internal(format!(
                    "join key index {bad} is out of range for the {side} side's {}-column \
                     schema — a planner bug: the key survived into the plan while the column \
                     it names did not",
                    schema.fields().len()
                )));
            }
        }

        let key_fields: Vec<SortField> = right_keys
            .iter()
            .map(|&i| SortField::new(right_schema.field(i).data_type().clone()))
            .collect();
        let row_converter =
            RowConverter::new(key_fields).expect("join key columns support row encoding");

        Ok(Self {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            budget: memory_budget,
            schema,
            left_schema,
            right_schema,
            row_converter,
            phase: Phase::Building,
            right_bytes: 0,
            right_batches: Vec::new(),
            right_table: None,
            hash_table: HashMap::new(),
            right_matched: Vec::new(),
        })
    }

    /// Drain the right child fully, concatenate into one table, and build
    /// the probe hash map. Runs once, in one call to `next_batch` (the same
    /// documented cancellation gap as `Sort`'s materialization).
    fn build(&mut self) -> Result<(), ExecError> {
        while let Some(batch) = self.right.next_batch()? {
            let batch_bytes = batch.get_array_memory_size();
            let requested = self.right_bytes + batch_bytes;
            if requested > self.budget {
                return Err(ExecError::OutOfMemory {
                    requested,
                    budget: self.budget,
                });
            }
            self.right_bytes = requested;
            self.right_batches.push(batch);
        }

        let table = if self.right_batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(&self.right_schema))
        } else {
            let combined =
                concat_batches(&self.right_schema, self.right_batches.iter()).map_err(arrow_err)?;
            self.right_batches.clear();
            combined
        };
        self.right_bytes = table.get_array_memory_size();
        self.right_matched = vec![false; table.num_rows()];

        // Cross join never probes the hash table, so building it would be
        // pure waste (and its RowConverter was built from an empty key
        // list, which cannot encode anything meaningful anyway).
        if self.kind != JoinKind::Cross && !self.right_keys.is_empty() {
            let key_cols = project_columns(&table, &self.right_keys);
            let mask = null_key_mask(&key_cols, table.num_rows());
            let rows = self
                .row_converter
                .convert_columns(&key_cols)
                .map_err(arrow_err)?;
            for (i, &is_null) in mask.iter().enumerate() {
                if is_null {
                    continue; // NULL key: can never match, never inserted.
                }
                self.hash_table
                    .entry(rows.row(i).owned())
                    .or_default()
                    .push(i as u32);
            }
        }

        self.right_table = Some(table);
        self.phase = Phase::Probing;
        Ok(())
    }

    fn right_table(&self) -> &RecordBatch {
        self.right_table
            .as_ref()
            .expect("right_table is set before Phase::Probing begins")
    }

    /// Look up one probe row's matches. Empty means either a NULL key
    /// (never matches anything) or a real key with no build-side rows.
    fn probe_matches(&self, rows_null: bool, key: &OwnedRow) -> &[u32] {
        if rows_null {
            return &[];
        }
        self.hash_table.get(key).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Process one left (probe) batch under an equijoin kind (everything but
    /// `Cross`). Returns the output batch for this input batch, which may be
    /// empty (e.g. an `Inner` join batch where nothing matched).
    fn probe_batch(&mut self, left_batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let n = left_batch.num_rows();
        let key_cols = project_columns(left_batch, &self.left_keys);
        let mask = null_key_mask(&key_cols, n);
        let rows = self
            .row_converter
            .convert_columns(&key_cols)
            .map_err(arrow_err)?;

        // LeftSemi/LeftAnti: left indices only, at most one per left row,
        // right side never appears in the output at all.
        if matches!(self.kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            let mut left_idx: Vec<u32> = Vec::new();
            for (j, &key_is_null) in mask.iter().enumerate() {
                let key = rows.row(j).owned();
                let has_match = !self.probe_matches(key_is_null, &key).is_empty();
                let emit = match self.kind {
                    JoinKind::LeftSemi => has_match,
                    JoinKind::LeftAnti => !has_match,
                    _ => unreachable!(),
                };
                if emit {
                    left_idx.push(j as u32);
                }
            }
            let idx = UInt32Array::from(left_idx);
            let cols = take_by_indices(left_batch, &idx)?;
            return RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err);
        }

        // Inner/Left/Right/Full: build parallel (left_idx, right_idx) index
        // lists; `None` in either means "NULL columns for this side" and is
        // what makes an unmatched row's other side come out all-NULL.
        let mut left_idx: Vec<Option<u32>> = Vec::new();
        let mut right_idx: Vec<Option<u32>> = Vec::new();
        for (j, &key_is_null) in mask.iter().enumerate() {
            let key = rows.row(j).owned();
            // Owned copy: `probe_matches` borrows `self.hash_table`
            // immutably, and the loop below needs to mutate
            // `self.right_matched` while iterating the matches.
            let matches: Vec<u32> = self.probe_matches(key_is_null, &key).to_vec();
            if matches.is_empty() {
                match self.kind {
                    // Right preserves only the build side's unmatched rows,
                    // emitted later in Phase::UnmatchedRight — an unmatched
                    // *left* row under Right contributes nothing.
                    JoinKind::Right => {}
                    JoinKind::Inner => {}
                    JoinKind::Left | JoinKind::Full => {
                        left_idx.push(Some(j as u32));
                        right_idx.push(None);
                    }
                    JoinKind::Cross | JoinKind::LeftSemi | JoinKind::LeftAnti => unreachable!(),
                }
            } else {
                for m in matches {
                    self.right_matched[m as usize] = true;
                    left_idx.push(Some(j as u32));
                    right_idx.push(Some(m));
                }
            }
        }

        self.combine_left_right(left_batch, &left_idx, &right_idx)
    }

    /// One left batch's worth of `Cross` output: every left row paired with
    /// every right row. Empty on either side yields zero rows.
    fn cross_batch(&self, left_batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let l = left_batch.num_rows();
        let r = self.right_table().num_rows();
        if l == 0 || r == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let mut left_idx: Vec<u32> = Vec::with_capacity(l * r);
        let mut right_idx: Vec<u32> = Vec::with_capacity(l * r);
        for li in 0..l {
            for ri in 0..r {
                left_idx.push(li as u32);
                right_idx.push(ri as u32);
            }
        }
        let left_idx = UInt32Array::from(left_idx);
        let right_idx = UInt32Array::from(right_idx);
        let mut cols = take_by_indices(left_batch, &left_idx)?;
        cols.extend(take_by_indices(self.right_table(), &right_idx)?);
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }

    fn combine_left_right(
        &self,
        left_batch: &RecordBatch,
        left_idx: &[Option<u32>],
        right_idx: &[Option<u32>],
    ) -> Result<RecordBatch, ExecError> {
        let left_idx = UInt32Array::from(left_idx.to_vec());
        let right_idx = UInt32Array::from(right_idx.to_vec());
        let mut cols = take_by_indices(left_batch, &left_idx)?;
        cols.extend(take_by_indices(self.right_table(), &right_idx)?);
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }

    /// The tail batch of never-matched right rows for `Right`/`Full`: real
    /// right-side values paired with all-NULL left columns.
    fn unmatched_right_batch(&self) -> Result<RecordBatch, ExecError> {
        let unmatched: Vec<u32> = self
            .right_matched
            .iter()
            .enumerate()
            .filter(|(_, &m)| !m)
            .map(|(i, _)| i as u32)
            .collect();
        if unmatched.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let idx = UInt32Array::from(unmatched.clone());
        let right_cols = take_by_indices(self.right_table(), &idx)?;
        let left_cols = null_columns(&self.left_schema, unmatched.len());
        let mut cols = left_cols;
        cols.extend(right_cols);
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }
}

impl Operator for HashJoin {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        loop {
            match self.phase {
                Phase::Building => self.build()?,
                Phase::Probing => match self.left.next_batch()? {
                    Some(left_batch) => {
                        let out = if self.kind == JoinKind::Cross {
                            self.cross_batch(&left_batch)?
                        } else {
                            self.probe_batch(&left_batch)?
                        };
                        if out.num_rows() > 0 {
                            return Ok(Some(out));
                        }
                        // Nothing came out of this particular left batch
                        // (e.g. none of it matched an Inner join) — keep
                        // pulling rather than returning an empty batch.
                    }
                    None => {
                        self.phase = if matches!(self.kind, JoinKind::Right | JoinKind::Full) {
                            Phase::UnmatchedRight
                        } else {
                            Phase::Done
                        };
                    }
                },
                Phase::UnmatchedRight => {
                    self.phase = Phase::Done;
                    let out = self.unmatched_right_batch()?;
                    if out.num_rows() > 0 {
                        return Ok(Some(out));
                    }
                }
                Phase::Done => return Ok(None),
            }
        }
    }

    fn memory_used(&self) -> usize {
        self.right_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::collections::VecDeque;

    /// A child operator that replays a fixed list of batches, one per
    /// `next_batch` call — same helper shape as `sort.rs`'s `Feed`, so tests
    /// can control exactly where batch boundaries fall.
    struct Feed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Feed {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches: batches.into(),
            }
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

    fn int_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int32, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn int_batch(schema: &SchemaRef, cols: Vec<Vec<Option<i32>>>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = cols
            .into_iter()
            .map(|c| Arc::new(Int32Array::from(c)) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
    }

    fn feed_one(schema: SchemaRef, batch: RecordBatch) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![batch]))
    }

    fn feed_many(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, batches))
    }

    fn empty_feed(schema: SchemaRef) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![]))
    }

    /// Pull every batch until exhaustion and concatenate, so tests can
    /// assert on the whole result without caring how it was chunked across
    /// `next_batch` calls.
    fn collect_all(op: &mut dyn Operator) -> RecordBatch {
        let schema = op.schema();
        let mut batches = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            batches.push(b);
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(schema);
        }
        concat_batches(&schema, batches.iter()).unwrap()
    }

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<Option<i32>> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    /// Sort (key, payload) pairs so batch-boundary/hash-order nondeterminism
    /// doesn't make an otherwise-correct test flaky.
    fn sorted_pairs(batch: &RecordBatch, a: usize, b: usize) -> Vec<(Option<i32>, Option<i32>)> {
        let mut v: Vec<(Option<i32>, Option<i32>)> = col_i32(batch, a)
            .into_iter()
            .zip(col_i32(batch, b))
            .collect();
        v.sort();
        v
    }

    // ─── Inner ──────────────────────────────────────────────────────────

    #[test]
    fn inner_join_matches_on_equal_keys() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(10), Some(20), Some(30)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(2), Some(3), Some(4)],
                    vec![Some(200), Some(300), Some(400)],
                ],
            ),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        assert_eq!(
            sorted_pairs(&out, 1, 3),
            vec![(Some(20), Some(200)), (Some(30), Some(300))]
        );
    }

    // Trap 1: NULL never equals NULL. A hash table that groups NULL keys
    // together (rather than excluding them) would report matches here; the
    // wrong answer this prevents is a spurious NULL-to-NULL row appearing in
    // an Inner join's output.
    #[test]
    fn null_keys_never_match_each_other() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None, None, Some(1)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None, None, Some(1)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        // Only the real 1/1 pair should match; every NULL/NULL combination
        // (2 left NULLs x 2 right NULLs = 4 possible spurious rows) must be
        // absent.
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_i32(&out, 0), vec![Some(1)]);
        assert_eq!(col_i32(&out, 1), vec![Some(1)]);
    }

    #[test]
    fn inner_join_with_no_matches_is_empty() {
        let schema = int_schema(&["k"]);
        let left = feed_one(schema.clone(), int_batch(&schema, vec![vec![Some(1)]]));
        let right = feed_one(schema.clone(), int_batch(&schema, vec![vec![Some(2)]]));
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0);
    }

    // ─── Duplicate keys multiply ───────────────────────────────────────

    // Trap 6: duplicate keys on both sides must multiply, not dedupe or add.
    // 3 left rows with key=1 x 2 right rows with key=1 => 6 matched rows.
    #[test]
    fn duplicate_keys_on_both_sides_multiply() {
        let left_schema = int_schema(&["lk", "seq"]);
        let right_schema = int_schema(&["rk", "seq"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(1), Some(1)],
                    vec![Some(0), Some(1), Some(2)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![vec![Some(1), Some(1)], vec![Some(10), Some(11)]],
            ),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            6,
            "3 left x 2 right matching rows must multiply to 6"
        );
    }

    // ─── Left / Right / Full ────────────────────────────────────────────

    // Trap 4a: an unmatched left row must still be emitted, with the right
    // side entirely NULL — not dropped.
    #[test]
    fn left_join_emits_unmatched_left_rows_with_null_right() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![Some(100)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Left, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        let mut pairs = sorted_pairs(&out, 0, 2);
        pairs.sort();
        assert_eq!(pairs, vec![(Some(1), Some(100)), (Some(2), None)]);
    }

    #[test]
    fn right_join_emits_unmatched_right_rows_with_null_left() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![Some(100)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Right, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        // columns: lk, lv, rk
        let mut rows: Vec<(Option<i32>, Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| {
                (
                    col_i32(&out, 0)[i],
                    col_i32(&out, 1)[i],
                    col_i32(&out, 2)[i],
                )
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![(None, None, Some(2)), (Some(1), Some(100), Some(1))]
        );
    }

    // Trap 4b: Full join emits unmatched rows from BOTH sides, and never
    // emits a matched row twice (once from the left pass, once from a right
    // "unmatched" pass).
    #[test]
    fn full_join_emits_both_unmatched_sides_and_never_duplicates_a_match() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        // left: 1 (matches), 2 (unmatched)
        // right: 1 (matches), 3 (unmatched)
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(3)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Full, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            3,
            "1 matched + 1 unmatched-left + 1 unmatched-right"
        );
        let mut rows: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![(None, Some(3)), (Some(1), Some(1)), (Some(2), None)]
        );
    }

    #[test]
    fn full_join_with_no_overlap_emits_every_row_from_both_sides_once() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(3), Some(4)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Full, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 4);
    }

    // ─── LeftSemi / LeftAnti ────────────────────────────────────────────

    // Trap 2a: LeftSemi emits each left row AT MOST ONCE, no matter how many
    // right rows match it — existence, not multiplication (contrast with
    // duplicate_keys_on_both_sides_multiply, which is Inner).
    #[test]
    fn left_semi_emits_each_matching_left_row_once_even_with_multiple_right_matches() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        // Three right rows share key=1: a naive join would multiply the
        // left row with key=1 into 3 output rows.
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(1), Some(1)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::LeftSemi, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            1,
            "left row with key=1 must appear exactly once"
        );
        assert_eq!(col_i32(&out, 0), vec![Some(1)]);
    }

    // Trap 2b: LeftAnti emits left rows with NO match, and only those.
    #[test]
    fn left_anti_emits_only_unmatched_left_rows() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::LeftAnti, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        let mut got: Vec<Option<i32>> = col_i32(&out, 0);
        got.sort();
        assert_eq!(got, vec![Some(1), Some(3)]);
    }

    // Trap 3: LeftSemi/LeftAnti output the LEFT schema only, never left ++
    // right — matches basin_plan::schema::join_schema's contract.
    #[test]
    fn left_semi_and_anti_output_schema_is_left_only() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv", "rw"]);
        for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
            let left = empty_feed(left_schema.clone());
            let right = empty_feed(right_schema.clone());
            let join = HashJoin::new(left, right, kind, vec![0], vec![0], 1 << 30).unwrap();
            assert_eq!(
                join.schema().fields().len(),
                2,
                "{kind:?} schema must be left-only (2 fields), not left++right (5)"
            );
            assert_eq!(join.schema(), left_schema);
        }
    }

    /// A NULL-keyed left row can never match, so it must fall on the
    /// LeftAnti side (and never the LeftSemi side) — same NULL-never-equals
    /// rule as trap 1, exercised through semi/anti instead of Inner.
    #[test]
    fn null_keyed_left_row_is_never_a_semi_match_and_always_an_anti_match() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None]]),
        );

        let left_a = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None]]),
        );
        let right_a = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None]]),
        );

        let mut semi =
            HashJoin::new(left, right, JoinKind::LeftSemi, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut semi).num_rows(), 0);

        let mut anti = HashJoin::new(
            left_a,
            right_a,
            JoinKind::LeftAnti,
            vec![0],
            vec![0],
            1 << 30,
        )
        .unwrap();
        assert_eq!(collect_all(&mut anti).num_rows(), 1);
    }

    // ─── Cross ──────────────────────────────────────────────────────────

    #[test]
    fn cross_join_produces_the_full_cartesian_product() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(10), Some(20), Some(30)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 6, "2 left x 3 right = 6");
    }

    // Trap 5: Cross join with an empty side yields zero rows, on both sides.
    #[test]
    fn cross_join_with_an_empty_side_yields_zero_rows() {
        let schema = int_schema(&["k"]);

        let left = feed_one(
            schema.clone(),
            int_batch(&schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = empty_feed(schema.clone());
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0, "empty right side");

        let left = empty_feed(schema.clone());
        let right = feed_one(
            schema.clone(),
            int_batch(&schema, vec![vec![Some(1), Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0, "empty left side");
    }

    // ─── Empty inputs, other kinds ─────────────────────────────────────

    #[test]
    fn every_kind_handles_both_sides_empty() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::LeftSemi,
            JoinKind::LeftAnti,
        ] {
            let left = empty_feed(left_schema.clone());
            let right = empty_feed(right_schema.clone());
            let mut join = HashJoin::new(left, right, kind, vec![0], vec![0], 1 << 30).unwrap();
            assert_eq!(
                collect_all(&mut join).num_rows(),
                0,
                "{kind:?} on empty/empty"
            );
        }
    }

    #[test]
    fn left_join_with_empty_right_emits_every_left_row_with_null_right() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = empty_feed(right_schema.clone());
        let mut join =
            HashJoin::new(left, right, JoinKind::Left, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        assert!(col_i32(&out, 1).iter().all(Option::is_none));
    }

    // ─── Multi-column keys ──────────────────────────────────────────────

    #[test]
    fn multi_column_keys_require_every_column_to_match() {
        let left_schema = int_schema(&["a", "b", "lv"]);
        let right_schema = int_schema(&["a", "b", "rv"]);
        // (1,1) should match; (1,2) on the left has no (1,2) on the right.
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(1)],
                    vec![Some(1), Some(2)],
                    vec![Some(100), Some(200)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(1), Some(1)],
                    vec![Some(1), Some(3)],
                    vec![Some(900), Some(999)],
                ],
            ),
        );
        let mut join = HashJoin::new(
            left,
            right,
            JoinKind::Inner,
            vec![0, 1],
            vec![0, 1],
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            1,
            "only (a=1,b=1) matches on both key columns"
        );
        // columns: left a, b, lv (0,1,2), right a, b, rv (3,4,5).
        assert_eq!(col_i32(&out, 2), vec![Some(100)]);
        assert_eq!(col_i32(&out, 5), vec![Some(900)]);
    }

    /// A NULL in just one of two key columns still disqualifies the whole
    /// key from matching — the NULL rule applies per-row across the key
    /// tuple, not per-column.
    #[test]
    fn a_null_in_any_one_key_column_disqualifies_the_whole_composite_key() {
        let left_schema = int_schema(&["a", "b"]);
        let right_schema = int_schema(&["a", "b"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![None]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![None]]),
        );
        let mut join = HashJoin::new(
            left,
            right,
            JoinKind::Inner,
            vec![0, 1],
            vec![0, 1],
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut join).num_rows(),
            0,
            "a=1 matches but b=NULL must veto the whole row"
        );
    }

    // ─── Batch boundaries ───────────────────────────────────────────────

    /// The probe side is streamed batch by batch; a match that spans a left
    /// batch boundary (i.e. the second batch matches a right row) must still
    /// be found — the hash table has to survive across `next_batch` calls.
    #[test]
    fn matches_are_found_across_multiple_left_batches() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_many(
            left_schema.clone(),
            vec![
                int_batch(&left_schema, vec![vec![Some(1)]]),
                int_batch(&left_schema, vec![vec![Some(2)]]),
                int_batch(&left_schema, vec![vec![Some(3)]]),
            ],
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 3);
    }

    // ─── Memory budget ──────────────────────────────────────────────────

    #[test]
    fn memory_used_reports_the_build_side_honestly() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = empty_feed(left_schema.clone());
        let right_batch = int_batch(&right_schema, vec![(0..1000).map(Some).collect()]);
        let expected_bytes = right_batch.get_array_memory_size();
        let right = feed_one(right_schema.clone(), right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(
            join.memory_used(),
            0,
            "nothing materialized before the first pull"
        );
        join.next_batch().unwrap();
        assert_eq!(join.memory_used(), expected_bytes);
    }

    #[test]
    fn exceeding_the_memory_budget_during_build_is_out_of_memory() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = empty_feed(left_schema.clone());
        let right_batch = int_batch(&right_schema, vec![(0..1000).map(Some).collect()]);
        let size = right_batch.get_array_memory_size();
        let right = feed_one(right_schema.clone(), right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], size - 1).unwrap();
        let err = join.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }

    #[test]
    fn memory_budget_is_not_charged_for_the_probe_left_side() {
        // The left side is streamed, never buffered whole — a left input far
        // larger than the budget must not fail, so long as the (small)
        // right side fits.
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let big_left = feed_many(
            left_schema.clone(),
            (0..2000)
                .map(|i| int_batch(&left_schema, vec![vec![Some(i)]]))
                .collect(),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)]]),
        );
        let mut join =
            HashJoin::new(big_left, right, JoinKind::Inner, vec![0], vec![0], 4096).unwrap();
        // Should not error; exactly one match (key=1) exists somewhere in
        // the left stream.
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 1);
    }

    // ─── Mixed types via a string key column (sanity for RowConverter) ──

    #[test]
    fn string_key_columns_join_correctly() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(20), Some(30)])) as ArrayRef,
            ],
        )
        .unwrap();
        let left = feed_one(left_schema, left_batch);
        let right = feed_one(right_schema, right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        // "b"/"b" matches; the two NULL keys never match each other.
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_i32(&out, 1), vec![Some(2)]);
        assert_eq!(col_i32(&out, 3), vec![Some(20)]);
    }
}
