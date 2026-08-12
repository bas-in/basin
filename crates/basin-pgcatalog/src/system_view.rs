//! The [`SystemView`] trait: one `pg_catalog` (or `information_schema`)
//! relation, with predicate pushdown.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{catalog_source::CatalogSource, error::Error, predicate::Predicate};

/// One system catalog relation — `pg_type`, `pg_class`, `pg_namespace`, and
/// eventually the rest of the ~65-relation surface currently implemented as
/// DataFusion `TableProvider` shims.
///
/// The difference from a `TableProvider` is [`scan`](SystemView::scan)'s
/// third argument: `pushed`. Every predicate naming one of this relation's
/// own columns (per [`SystemView::schema`]) is guaranteed to have been
/// applied to the returned batch — not "may have been", the way DataFusion's
/// `_filters` were merely advisory and today's shims drop them entirely (see
/// the crate docs). A predicate naming a column the relation does not have is
/// an error, not a silent no-op.
pub trait SystemView {
    /// The relation's name, as it appears after `pg_catalog.` — `"pg_type"`,
    /// not `"pg_catalog.pg_type"`.
    fn name(&self) -> &str;

    /// The relation's Arrow schema. Column order and nullability here are
    /// exactly what a wire-protocol `RowDescription` for `SELECT * FROM
    /// <name>` would report.
    fn schema(&self) -> SchemaRef;

    /// Produce the relation's rows, applying every predicate in `pushed`
    /// whose column this relation has.
    ///
    /// `catalog` is the source of truth for anything not fully determined by
    /// this crate alone (i.e. everything except `pg_type`, which needs no
    /// catalog — see [`crate::pg_type`]).
    fn scan(&self, catalog: &dyn CatalogSource, pushed: &[Predicate])
        -> Result<RecordBatch, Error>;
}
