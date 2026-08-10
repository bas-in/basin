//! PK-ordered merge of a memtable iterator and a cold-tier row iterator.
//!
//! `merge_scan` implements the read-merge path for the HTAP hot tier (Phase
//! 5.14.C3).  Given two iterators over `(RowKey, RecordBatch)` pairs — one
//! from the memtable, one from the Vortex/Parquet cold tier — it produces a
//! merged, deduplicated, tombstone-suppressed stream in ascending PK order.
//!
//! ## Semantics (per ADR 0016 + TASK.md §locked-decisions)
//!
//! * **PK order preserved**: output is in ascending `RowKey` byte order.
//! * **Memtable wins on collision**: when the same PK exists in both the
//!   memtable and the cold tier, the memtable row is emitted and the cold-tier
//!   row is discarded.
//! * **Tombstone suppression**: a `MemRowValue::Tombstone` in the memtable
//!   causes the row (from either source) to be omitted from the output stream.
//!
//! ## Design
//!
//! The merge is a standard k-way merge-sort over two monotone sequences.
//! Both iterators must yield items in ascending `RowKey` order; the caller is
//! responsible for ensuring this precondition (the `MemTable` BTree always
//! satisfies it; the cold-tier iterator must be sorted, which the existing
//! Vortex/Parquet reader path guarantees by cluster-key sort at write time).
//!
//! The resulting iterator is lazy: it pulls one item from each side and
//! advances only the side whose key was smallest (or both on a tie, applying
//! the memtable-wins rule).
//!
//! ## Usage
//!
//! ```ignore
//! use basin_hottier::merge::merge_scan;
//!
//! let mem_iter  = memtable.snapshot().into_iter()
//!     .map(|(k, v)| -> (RowKey, MemRowValue) { (k, v) });
//! let cold_iter = vortex_rows.into_iter();
//! let merged = merge_scan(mem_iter, cold_iter);
//! for batch in merged { /* process */ }
//! ```

use arrow_array::RecordBatch;

use crate::memtable::MemRowValue;
use crate::row_key::RowKey;

// ── Public entrypoint ─────────────────────────────────────────────────────────

/// Merge a memtable snapshot and a cold-tier row stream into a single
/// PK-ordered, deduplicated, tombstone-suppressed iterator of `RecordBatch`es.
///
/// Each `RecordBatch` in the output contains exactly one row (matching the
/// one-row-per-key layout already used by the memtable's IPC encoding).
///
/// # Parameters
///
/// * `mem_iter`  — Iterator over `(RowKey, MemRowValue)` in ascending key order
///   (typically `MemTable::snapshot().into_iter()`).
/// * `cold_iter` — Iterator over `(RowKey, RecordBatch)` in ascending key order
///   (one-row batches from the cold tier).
///
/// # Returns
///
/// An [`Iterator<Item = RecordBatch>`] suitable for collecting or streaming
/// to the query layer.
pub fn merge_scan<M, C>(mem_iter: M, cold_iter: C) -> MergeScan<M::IntoIter, C::IntoIter>
where
    M: IntoIterator<Item = (RowKey, MemRowValue)>,
    C: IntoIterator<Item = (RowKey, RecordBatch)>,
{
    MergeScan::new(mem_iter.into_iter(), cold_iter.into_iter())
}

// ── Iterator ──────────────────────────────────────────────────────────────────

/// Lazy PK-ordered merge iterator.  Constructed by [`merge_scan`].
pub struct MergeScan<M, C>
where
    M: Iterator<Item = (RowKey, MemRowValue)>,
    C: Iterator<Item = (RowKey, RecordBatch)>,
{
    mem: Peekable2<M>,
    cold: Peekable2<C>,
}

impl<M, C> MergeScan<M, C>
where
    M: Iterator<Item = (RowKey, MemRowValue)>,
    C: Iterator<Item = (RowKey, RecordBatch)>,
{
    fn new(mem: M, cold: C) -> Self {
        Self {
            mem: Peekable2::new(mem),
            cold: Peekable2::new(cold),
        }
    }
}

impl<M, C> Iterator for MergeScan<M, C>
where
    M: Iterator<Item = (RowKey, MemRowValue)>,
    C: Iterator<Item = (RowKey, RecordBatch)>,
{
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.mem.peek(), self.cold.peek()) {
                // Both sides exhausted.
                (None, None) => return None,

                // Only cold side remains — no memtable entry to override it.
                (None, Some(_)) => {
                    let (_, batch) = self.cold.next().unwrap();
                    return Some(batch);
                }

                // Only memtable side remains.
                (Some(_), None) => {
                    let (_, value) = self.mem.next().unwrap();
                    match value {
                        MemRowValue::Tombstone => {
                            // Tombstone with no cold-side match — skip.
                            continue;
                        }
                        MemRowValue::Row { bytes, .. } | MemRowValue::Update { bytes, .. } => {
                            return decode_ipc_row(&bytes);
                        }
                    }
                }

                // Both sides have items — compare keys.
                (Some((mk, _)), Some((ck, _))) => {
                    use std::cmp::Ordering;
                    match mk.cmp(ck) {
                        Ordering::Less => {
                            // Memtable key is smaller — emit from memtable (or skip tombstone).
                            let (_, value) = self.mem.next().unwrap();
                            match value {
                                MemRowValue::Tombstone => continue,
                                MemRowValue::Row { bytes, .. }
                                | MemRowValue::Update { bytes, .. } => {
                                    return decode_ipc_row(&bytes);
                                }
                            }
                        }
                        Ordering::Greater => {
                            // Cold key is smaller — no override, emit cold row.
                            let (_, batch) = self.cold.next().unwrap();
                            return Some(batch);
                        }
                        Ordering::Equal => {
                            // Same PK: memtable wins; advance both sides.
                            let (_, mem_value) = self.mem.next().unwrap();
                            let _cold_discarded = self.cold.next().unwrap();
                            match mem_value {
                                // Tombstone: row is deleted — suppress and continue.
                                MemRowValue::Tombstone => continue,
                                MemRowValue::Row { bytes, .. }
                                | MemRowValue::Update { bytes, .. } => {
                                    return decode_ipc_row(&bytes);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ── IPC decode ────────────────────────────────────────────────────────────────

/// Decode one Arrow IPC stream `bytes` into a `RecordBatch`.
///
/// Returns `None` when the stream contains zero batches (shouldn't happen in
/// practice for well-formed hot-tier entries, but we degrade gracefully).
fn decode_ipc_row(bytes: &[u8]) -> Option<RecordBatch> {
    use arrow_ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

// ── Peek-one helper ───────────────────────────────────────────────────────────

/// A minimal one-item lookahead wrapper (we only need to peek, not the full
/// `std::iter::Peekable` API — this avoids a double-borrow fight in `next`).
struct Peekable2<I: Iterator> {
    iter: I,
    peeked: Option<I::Item>,
}

impl<I: Iterator> Peekable2<I> {
    fn new(mut iter: I) -> Self {
        let peeked = iter.next();
        Self { iter, peeked }
    }

    fn peek(&self) -> Option<&I::Item> {
        self.peeked.as_ref()
    }

    fn next(&mut self) -> Option<I::Item> {
        let val = self.peeked.take();
        self.peeked = self.iter.next();
        val
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemRowValue;
    use crate::row_key::RowKey;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn key(n: i64) -> RowKey {
        RowKey::builder().append_i64(n).finish()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// Build a one-row `RecordBatch` with `id = n`.
    fn cold_batch(n: i64) -> RecordBatch {
        let schema = schema();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n])) as _]).unwrap()
    }

    /// Encode a one-row `RecordBatch` as IPC bytes (memtable format).
    fn mem_ipc(n: i64) -> Vec<u8> {
        let batch = cold_batch(n);
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        buf
    }

    fn mem_row(n: i64) -> MemRowValue {
        MemRowValue::row(mem_ipc(n), 0)
    }

    /// Extract the `id` value from the first row of `batch`.
    fn id(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// Empty memtable + empty cold → empty output.
    #[test]
    fn both_empty_produces_no_rows() {
        let mem: Vec<(RowKey, MemRowValue)> = vec![];
        let cold: Vec<(RowKey, RecordBatch)> = vec![];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert!(out.is_empty());
    }

    /// Only memtable rows (no cold), all live → all rows emitted in key order.
    #[test]
    fn mem_only_emits_in_pk_order() {
        let mem = vec![
            (key(1), mem_row(1)),
            (key(2), mem_row(2)),
            (key(3), mem_row(3)),
        ];
        let cold: Vec<(RowKey, RecordBatch)> = vec![];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert_eq!(out.len(), 3);
        assert_eq!(id(&out[0]), 1);
        assert_eq!(id(&out[1]), 2);
        assert_eq!(id(&out[2]), 3);
    }

    /// Only cold rows → all rows emitted in order.
    #[test]
    fn cold_only_emits_in_pk_order() {
        let mem: Vec<(RowKey, MemRowValue)> = vec![];
        let cold = vec![(key(10), cold_batch(10)), (key(20), cold_batch(20))];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert_eq!(out.len(), 2);
        assert_eq!(id(&out[0]), 10);
        assert_eq!(id(&out[1]), 20);
    }

    /// Interleaved keys with no overlap → merged in ascending PK order.
    #[test]
    fn interleaved_no_overlap_preserves_pk_order() {
        // mem: 1, 3, 5  cold: 2, 4, 6
        let mem = vec![
            (key(1), mem_row(1)),
            (key(3), mem_row(3)),
            (key(5), mem_row(5)),
        ];
        let cold = vec![
            (key(2), cold_batch(2)),
            (key(4), cold_batch(4)),
            (key(6), cold_batch(6)),
        ];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert_eq!(out.len(), 6);
        let ids: Vec<i64> = out.iter().map(id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }

    /// On PK collision, memtable row wins.
    #[test]
    fn memtable_wins_on_pk_collision() {
        // mem has id=42 with value 42, cold also has pk=42 with value 99.
        // Output should yield id=42 (from memtable).
        let mem = vec![(key(42), mem_row(42))];
        let cold = vec![(key(42), cold_batch(99))]; // same key, different payload
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(id(&out[0]), 42); // memtable wins
    }

    /// Tombstone in memtable with matching cold row → row suppressed.
    #[test]
    fn tombstone_suppresses_cold_row() {
        let mem = vec![(key(7), MemRowValue::Tombstone)];
        let cold = vec![(key(7), cold_batch(7))];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert!(out.is_empty(), "tombstone must suppress the cold-tier row");
    }

    /// Tombstone in memtable with no matching cold row → nothing emitted.
    #[test]
    fn tombstone_no_cold_match_emits_nothing() {
        let mem = vec![(key(5), MemRowValue::Tombstone)];
        let cold: Vec<(RowKey, RecordBatch)> = vec![];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        assert!(out.is_empty());
    }

    /// Multiple tombstones among live rows — only live rows reach output.
    #[test]
    fn mixed_tombstones_and_live_rows() {
        let mem = vec![
            (key(1), mem_row(1)),
            (key(2), MemRowValue::Tombstone),
            (key(3), mem_row(3)),
        ];
        let cold = vec![
            (key(2), cold_batch(2)), // tombstoned
            (key(4), cold_batch(4)),
        ];
        let out: Vec<_> = merge_scan(mem, cold).collect();
        let ids: Vec<i64> = out.iter().map(id).collect();
        assert_eq!(ids, vec![1, 3, 4]);
    }

    /// 88 smoke-shape compatibility: a pure cold-only merge (no hot rows) must
    /// return identical results (simulates no-hot-tier path for the smoke suite).
    #[test]
    fn cold_only_merge_matches_direct_scan() {
        let expected_ids: Vec<i64> = (0..88).collect();
        let cold: Vec<_> = expected_ids
            .iter()
            .map(|&n| (key(n), cold_batch(n)))
            .collect();
        let out: Vec<_> = merge_scan(vec![], cold).collect();
        let got: Vec<i64> = out.iter().map(id).collect();
        assert_eq!(got, expected_ids);
    }
}
