//! `HashJoin` — the equijoin family.
//!
//! Skeleton only; filled in incrementally.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use basin_plan::JoinKind;

use crate::operator::{ExecError, Operator};

pub struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    kind: JoinKind,
    schema: SchemaRef,
}

impl HashJoin {
    pub fn new(left: Box<dyn Operator>, right: Box<dyn Operator>, kind: JoinKind) -> Self {
        let schema = left.schema();
        Self {
            left,
            right,
            kind,
            schema,
        }
    }
}

impl Operator for HashJoin {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        Ok(None)
    }
}
