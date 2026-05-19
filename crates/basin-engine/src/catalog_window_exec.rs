//! `CatalogWindowExecSortElision` — elide a `SortExec` above a
//! `WindowAggExec` / `BoundedWindowAggExec` when the file's declared sort order
//! already satisfies the window's required ordering.
//!
//! # Problem
//!
//! When a table is created with `WITH (basin.sort_by='id,b')` and queried with
//! a window function such as
//!
//! ```sql
//! SELECT SUM(k) OVER (PARTITION BY b ORDER BY id) FROM t
//! ```
//!
//! DataFusion's `EnforceSorting` rule inserts a `SortExec(b, id)` before the
//! `WindowAggExec` because it only sees the `output_ordering` reported by the
//! `DataSourceExec`.  In practice the Vortex/Parquet files are already stored
//! in that order (because `basin.sort_by` flows through `with_file_sort_order`),
//! so the `SortExec` does redundant work.
//!
//! # Fix
//!
//! This `PhysicalOptimizerRule` runs **after** `EnforceSorting`.  It walks the
//! plan looking for:
//!
//! ```text
//! WindowAggExec (or BoundedWindowAggExec)
//!   └─ SortExec(sort_exprs)
//!        └─ [optional RepartitionExec]
//!             └─ DataSourceExec(output_ordering = file_order)
//! ```
//!
//! When `sort_exprs` is a prefix of (or equal to) `file_order`, the `SortExec`
//! is redundant and is removed, reconnecting the window node directly to the
//! `DataSourceExec` (or its `RepartitionExec` wrapper).
//!
//! # Correctness
//!
//! The elision is safe because:
//! * `sort_exprs` ⊆ `file_order` (prefix check) guarantees the rows are
//!   already in the required order.
//! * `WindowAggExec::with_new_children` / `BoundedWindowAggExec::with_new_children`
//!   call `try_new`, which recomputes `ordered_partition_by_indices` against
//!   the new child's `output_ordering`, so the plan properties remain consistent.
//!
//! # Non-match conditions
//!
//! * No `SortExec` between the window and the scan — nothing to elide.
//! * `sort_exprs` is not a prefix of `file_order` — sort is genuinely needed.
//! * No `DataSourceExec` reachable through at most one `RepartitionExec` — we
//!   don't know the physical file order, so we leave the plan alone.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::LexOrdering;

/// Physical optimizer rule that removes a `SortExec` sitting between a window
/// aggregate node and a `DataSourceExec` whose declared sort order already
/// covers the sort expressions.
///
/// See module documentation for the full rationale and match conditions.
#[derive(Debug, Default)]
pub struct CatalogWindowExecSortElision;

impl CatalogWindowExecSortElision {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for CatalogWindowExecSortElision {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            Ok(match try_elide_sort(Arc::clone(&node)) {
                Some(new_node) => Transformed::yes(new_node),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "catalog_window_exec_sort_elision"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Pattern matching and rewrite
// ---------------------------------------------------------------------------

/// Attempt to elide a redundant `SortExec` above a window aggregate node.
/// Returns `Some(new_plan)` on a full match, `None` to leave the node as-is.
fn try_elide_sort(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    // 1. The top node must be a window aggregate (bounded or unbounded).
    //    Extract the child without keeping a borrow of `plan`.
    let window_child_arc: Arc<dyn ExecutionPlan> = {
        let child_ref = window_child(&plan)?;
        Arc::clone(child_ref)
    };

    // 2. The window's direct child must be a SortExec.
    let sort_exec = window_child_arc
        .as_any()
        .downcast_ref::<SortExec>()?;
    let sort_exprs = sort_exec.expr();

    // 3. Walk through an optional RepartitionExec to reach the DataSourceExec.
    let sort_child: &Arc<dyn ExecutionPlan> = sort_exec.input();
    let data_source_plan = find_data_source(sort_child)?;

    // 4. The DataSourceExec must report an output_ordering that covers (is a
    //    superset prefix of) the SortExec's expressions.
    let file_ordering = data_source_plan.output_ordering()?;
    if !sort_is_prefix_of_file(sort_exprs, file_ordering) {
        return None;
    }

    // 5. Reconnect the window node to the SortExec's child (DataSourceExec or
    //    its RepartitionExec wrapper), skipping the now-redundant SortExec.
    let new_child = Arc::clone(sort_child);
    plan.with_new_children(vec![new_child]).ok()
}

/// Return the single child of `plan` if it is a `WindowAggExec` or
/// `BoundedWindowAggExec`, otherwise `None`.
fn window_child(plan: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if plan.as_any().downcast_ref::<WindowAggExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<BoundedWindowAggExec>()
            .is_some()
    {
        plan.children().into_iter().next()
    } else {
        None
    }
}

/// Walk from `node` looking for a `DataSourceExec`.  Looks through a single
/// `RepartitionExec` if present.  Returns a reference to the `DataSourceExec`
/// plan, or `None` if the pattern doesn't match.
fn find_data_source(node: &Arc<dyn ExecutionPlan>) -> Option<&Arc<dyn ExecutionPlan>> {
    if node.as_any().downcast_ref::<DataSourceExec>().is_some() {
        return Some(node);
    }
    if node.as_any().downcast_ref::<RepartitionExec>().is_some() {
        let child = node.children().into_iter().next()?;
        if child.as_any().downcast_ref::<DataSourceExec>().is_some() {
            return Some(child);
        }
    }
    None
}

/// Returns `true` when every element of `sort_exprs` appears in order at the
/// start of `file_order` (i.e., `sort_exprs` is a prefix of `file_order`).
///
/// Both slices must be non-empty and `file_order` must have at least as many
/// elements as `sort_exprs`.  Elements are compared by physical expression
/// equality and sort options.
fn sort_is_prefix_of_file(sort_exprs: &LexOrdering, file_order: &LexOrdering) -> bool {
    if sort_exprs.is_empty() || file_order.len() < sort_exprs.len() {
        return false;
    }
    sort_exprs
        .iter()
        .zip(file_order.iter())
        .all(|(s, f)| s == f)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::windows::{
        WindowAggExec, WindowUDFExpr, create_udwf_window_expr,
    };
    use datafusion::physical_plan::windows::StandardWindowExpr;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_physical_expr::LexOrdering;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion::logical_expr::WindowUDF;
    use datafusion::logical_expr::WindowFrame;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("b", DataType::Boolean, true),
            Field::new("k", DataType::Int64, true),
        ]))
    }

    fn asc_expr(schema: &SchemaRef, col_name: &str) -> PhysicalSortExpr {
        let idx = schema.index_of(col_name).unwrap();
        PhysicalSortExpr {
            expr: Arc::new(Column::new(col_name, idx)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }
    }

    /// Build a `DataSourceExec` with the given `output_ordering` and number of
    /// partitions.
    fn make_scan(
        schema: SchemaRef,
        ordering: LexOrdering,
        partition_count: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let batches: Vec<Vec<arrow::record_batch::RecordBatch>> =
            (0..partition_count).map(|_| vec![]).collect();
        let source = MemorySourceConfig::try_new(&batches, Arc::clone(&schema), None)
            .unwrap()
            .try_with_sort_information(vec![ordering])
            .unwrap();
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    /// Build a minimal `WindowAggExec` wrapping `input`.
    ///
    /// Uses `ROW_NUMBER() OVER ()` — no PARTITION BY / ORDER BY — so that plan
    /// construction does not fail.  The actual window function is irrelevant for
    /// the optimizer rule tests, which only inspect the plan tree structure.
    fn make_window_over(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Arc<dyn ExecutionPlan> {
        // Build a ROW_NUMBER() UDWF via the functions_window crate re-exported
        // from datafusion.
        let rn_impl = datafusion::functions_window::row_number::RowNumber::new();
        let rn_udf = Arc::new(WindowUDF::from(rn_impl));
        let std_expr = create_udwf_window_expr(
            &rn_udf,
            &[],
            schema.as_ref(),
            "row_number".to_string(),
            false,
        )
        .unwrap();
        let window_expr = Arc::new(StandardWindowExpr::new(
            std_expr,
            &[],
            &[],
            Arc::new(WindowFrame::new(None)),
        )) as Arc<dyn datafusion::physical_plan::WindowExpr>;

        Arc::new(
            WindowAggExec::try_new(vec![window_expr], input, true).unwrap(),
        )
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Positive: `WindowAggExec -> SortExec -> DataSourceExec` where sort
    /// expressions are a prefix of the file's declared ordering — SortExec
    /// should be removed.
    #[test]
    fn test_sort_elided_when_file_ordering_matches() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");
        let k_asc = asc_expr(&schema, "k");

        // File is sorted by [id ASC, k ASC].
        let file_ord = LexOrdering::new(vec![id_asc.clone(), k_asc.clone()]).unwrap();
        let scan = make_scan(Arc::clone(&schema), file_ord, 1);

        // SortExec requests [id ASC] — a prefix of the file ordering.
        let sort_ord = LexOrdering::new(vec![id_asc.clone()]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_ord, Arc::clone(&scan)));

        let window = make_window_over(sort, Arc::clone(&schema));

        let rule = CatalogWindowExecSortElision::new();
        let cfg = ConfigOptions::default();
        let result = rule.optimize(Arc::clone(&window), &cfg).unwrap();

        // The window node is still present.
        assert!(
            result.as_any().downcast_ref::<WindowAggExec>().is_some(),
            "window node should be preserved"
        );
        // The child of the window is now directly DataSourceExec (SortExec removed).
        let child = result.children().into_iter().next().unwrap();
        assert!(
            child.as_any().downcast_ref::<SortExec>().is_none(),
            "SortExec should have been elided; got: {}",
            child.name()
        );
        assert!(
            child.as_any().downcast_ref::<DataSourceExec>().is_some(),
            "window child should be DataSourceExec; got: {}",
            child.name()
        );
    }

    /// Negative: SortExec expressions do NOT match the file ordering prefix —
    /// the plan must be left unchanged.
    #[test]
    fn test_no_elision_on_ordering_mismatch() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");
        let k_asc = asc_expr(&schema, "k");

        // File is sorted by [id ASC].
        let file_ord = LexOrdering::new(vec![id_asc.clone()]).unwrap();
        let scan = make_scan(Arc::clone(&schema), file_ord, 1);

        // SortExec requests [k ASC] — does NOT match the file ordering.
        let sort_ord = LexOrdering::new(vec![k_asc.clone()]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_ord, Arc::clone(&scan)));

        let window = make_window_over(Arc::clone(&sort), Arc::clone(&schema));

        let rule = CatalogWindowExecSortElision::new();
        let cfg = ConfigOptions::default();
        let result = rule.optimize(Arc::clone(&window), &cfg).unwrap();

        // The SortExec must still be present.
        let child = result.children().into_iter().next().unwrap();
        assert!(
            child.as_any().downcast_ref::<SortExec>().is_some(),
            "SortExec should be preserved on mismatch; got: {}",
            child.name()
        );
    }

    /// Negative: no `DataSourceExec` beneath the `SortExec` (e.g. the scan is
    /// wrapped in a `FilterExec`) — the rule must not fire.
    #[test]
    fn test_no_elision_without_data_source_exec() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");

        // File is sorted by [id ASC].
        let file_ord = LexOrdering::new(vec![id_asc.clone()]).unwrap();
        let scan = make_scan(Arc::clone(&schema), file_ord, 1);

        // Insert a FilterExec between scan and sort: DataSourceExec is no
        // longer a direct child / grandchild of the SortExec.
        let b_idx = schema.index_of("b").unwrap();
        let b_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", b_idx));
        let filter =
            Arc::new(FilterExec::try_new(b_col, scan).unwrap()) as Arc<dyn ExecutionPlan>;

        let sort_ord = LexOrdering::new(vec![id_asc.clone()]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_ord, filter));

        let window = make_window_over(Arc::clone(&sort), Arc::clone(&schema));

        let rule = CatalogWindowExecSortElision::new();
        let cfg = ConfigOptions::default();
        let result = rule.optimize(Arc::clone(&window), &cfg).unwrap();

        // SortExec must still be present.
        let child = result.children().into_iter().next().unwrap();
        assert!(
            child.as_any().downcast_ref::<SortExec>().is_some(),
            "SortExec should be preserved when no DataSourceExec; got: {}",
            child.name()
        );
    }
}
