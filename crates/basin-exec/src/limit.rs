//! `LIMIT` / `OFFSET` without an ordering.
//!
//! [`crate::sort::TopK`] covers `ORDER BY … LIMIT`, where the limit and the
//! sort fuse into one bounded-heap operator. This is the other case: a limit
//! over an unordered input, and the `OFFSET` half of both.
//!
//! It exists because the builder refused both shapes outright, which sent every
//! plain `SELECT … LIMIT 10` and every paginated query back to DataFusion. They
//! are among the most common statements an application issues, so refusing them
//! kept the owned engine's fallback rate high for no structural reason.
//!
//! # Early exit is the point
//!
//! `next_batch` stops pulling from its child as soon as it has `skip + fetch`
//! rows. That is the entire value: a `LIMIT 10` over a billion-row scan must
//! read one batch, not a billion rows and then a truncation. An implementation
//! that drains the child and slices afterwards returns identical answers and
//! throws that away — which is exactly the class of silent regression this
//! project has been careful about elsewhere.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::operator::{ExecError, Operator};

/// `LIMIT fetch OFFSET skip` over an unordered input.
pub struct Limit {
    child: Box<dyn Operator>,
    /// Rows still to discard before emitting anything.
    remaining_skip: usize,
    /// Rows still to emit. `None` is `OFFSET n` with no `LIMIT`, which skips
    /// and then emits everything left.
    remaining_fetch: Option<usize>,
    schema: SchemaRef,
    done: bool,
}

impl std::fmt::Debug for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limit")
            .field("remaining_skip", &self.remaining_skip)
            .field("remaining_fetch", &self.remaining_fetch)
            .finish_non_exhaustive()
    }
}

impl Limit {
    pub fn new(child: Box<dyn Operator>, skip: usize, fetch: Option<usize>) -> Self {
        let schema = child.schema();
        Self {
            child,
            remaining_skip: skip,
            remaining_fetch: fetch,
            schema,
            done: false,
        }
    }
}

impl Operator for Limit {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        loop {
            // `LIMIT 0` is legal SQL and yields nothing. Checked before pulling
            // so it never touches the child at all.
            if self.remaining_fetch == Some(0) {
                self.done = true;
                return Ok(None);
            }

            let Some(batch) = self.child.next_batch()? else {
                self.done = true;
                return Ok(None);
            };
            let rows = batch.num_rows();

            // Discard whole batches while they fall entirely inside the offset.
            if self.remaining_skip >= rows {
                self.remaining_skip -= rows;
                continue;
            }

            let start = self.remaining_skip;
            self.remaining_skip = 0;
            let available = rows - start;
            let take = match self.remaining_fetch {
                Some(n) => n.min(available),
                None => available,
            };
            if let Some(n) = self.remaining_fetch.as_mut() {
                *n -= take;
            }

            // A batch consumed entirely by the offset yields nothing but is not
            // end-of-stream — keep pulling rather than returning None, which
            // would truncate the query.
            if take == 0 {
                continue;
            }
            return Ok(Some(batch.slice(start, take)));
        }
    }

    fn memory_used(&self) -> usize {
        // Streaming: one child batch is in flight at a time and nothing is
        // retained across calls.
        self.child.memory_used()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// A child that counts how many batches were pulled, so early exit is
    /// observable rather than assumed.
    struct Counting {
        batches: VecDeque<RecordBatch>,
        schema: SchemaRef,
        pulls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Operator for Counting {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            self.pulls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self.batches.pop_front())
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]))
    }

    fn batch(vals: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vals.to_vec()))]).unwrap()
    }

    fn child(
        batches: Vec<RecordBatch>,
    ) -> (Box<dyn Operator>, Arc<std::sync::atomic::AtomicUsize>) {
        let pulls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Box::new(Counting {
                batches: batches.into(),
                schema: schema(),
                pulls: Arc::clone(&pulls),
            }),
            pulls,
        )
    }

    fn drain(mut op: Limit) -> Vec<i32> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            out.extend(a);
        }
        out
    }

    #[test]
    fn limit_truncates_within_a_batch() {
        let (c, _) = child(vec![batch(&[1, 2, 3, 4, 5])]);
        assert_eq!(drain(Limit::new(c, 0, Some(3))), vec![1, 2, 3]);
    }

    #[test]
    fn offset_skips_across_batch_boundaries() {
        let (c, _) = child(vec![batch(&[1, 2]), batch(&[3, 4]), batch(&[5, 6])]);
        assert_eq!(drain(Limit::new(c, 3, Some(2))), vec![4, 5]);
    }

    /// The whole reason this operator exists. A `LIMIT 2` over a long input must
    /// stop pulling once satisfied; draining and slicing afterwards gives the
    /// same rows and reads the entire table.
    #[test]
    fn limit_stops_pulling_once_satisfied() {
        let (c, pulls) = child(vec![
            batch(&[1, 2]),
            batch(&[3, 4]),
            batch(&[5, 6]),
            batch(&[7, 8]),
        ]);
        assert_eq!(drain(Limit::new(c, 0, Some(2))), vec![1, 2]);
        assert_eq!(
            pulls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "a LIMIT 2 satisfied by the first batch must not pull a second"
        );
    }

    /// `LIMIT 0` is legal SQL. It must yield nothing without touching the child
    /// at all.
    #[test]
    fn limit_zero_yields_nothing_and_reads_nothing() {
        let (c, pulls) = child(vec![batch(&[1, 2, 3])]);
        assert!(drain(Limit::new(c, 0, Some(0))).is_empty());
        assert_eq!(pulls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// `OFFSET` with no `LIMIT` skips and then emits everything remaining.
    #[test]
    fn offset_without_limit_emits_the_rest() {
        let (c, _) = child(vec![batch(&[1, 2, 3]), batch(&[4, 5])]);
        assert_eq!(drain(Limit::new(c, 2, None)), vec![3, 4, 5]);
    }

    /// An offset past the end of the input yields nothing rather than the last
    /// batch — an off-by-one here returns rows the query excluded.
    #[test]
    fn offset_beyond_the_input_yields_nothing() {
        let (c, _) = child(vec![batch(&[1, 2, 3])]);
        assert!(drain(Limit::new(c, 10, Some(5))).is_empty());
    }

    /// A batch fully consumed by the offset must not read as end-of-stream.
    /// Returning `None` there would truncate the query and drop every later
    /// batch.
    #[test]
    fn a_batch_entirely_inside_the_offset_does_not_end_the_stream() {
        let (c, _) = child(vec![batch(&[1, 2]), batch(&[3, 4])]);
        assert_eq!(drain(Limit::new(c, 2, Some(2))), vec![3, 4]);
    }

    #[test]
    fn a_limit_larger_than_the_input_returns_everything() {
        let (c, _) = child(vec![batch(&[1, 2]), batch(&[3])]);
        assert_eq!(drain(Limit::new(c, 0, Some(99))), vec![1, 2, 3]);
    }
}
