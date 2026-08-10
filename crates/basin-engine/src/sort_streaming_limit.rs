//! `SortStreamingLimit` — collapse to single-partition when OFFSET scan matches
//! the file's natural sort order.
//!
//! # Problem
//!
//! A query of the form
//!
//! ```sql
//! SELECT ... FROM t ORDER BY id LIMIT 100 OFFSET 1000
//! ```
//!
//! where `t` was created with `basin.sort_by='id'` produces the physical plan:
//!
//! ```text
//! GlobalLimitExec(skip=1000, fetch=Some(100))
//!   SortExec(fetch=Some(1100), expr=[id ASC])
//!     RepartitionExec(RoundRobinBatch(N))
//!       DataSourceExec(output_ordering=[id ASC])
//! ```
//!
//! DataFusion's `EnforceDistribution` rule fans the scan out to N partitions to
//! maximise I/O parallelism.  For a TopK sort that is fine — the heap terminates
//! after collecting `fetch` rows.  But `GlobalLimitExec` with a nonzero `skip`
//! forces the sort to materialise at least `skip + fetch` rows before dropping
//! the first `skip`.  When the files are already in `id ASC` order, opening all
//! N partitions simultaneously destroys the per-file ordering; the merge then
//! has to pull rows from every file before it can confirm the first output row.
//!
//! # Fix
//!
//! When the pattern above is detected, inject a `CoalescePartitionsExec`
//! between the sort's current child and the sort itself.  This forces all file
//! partitions to merge into a single stream before entering the sort, allowing
//! the TopK heap to terminate as soon as `skip + fetch` rows have been seen
//! in file-sort order — without opening every file in parallel.
//!
//! # Match conditions
//!
//! 1. `GlobalLimitExec` with `skip > 0` and `fetch = Some(_)`.
//! 2. Direct child is `SortExec` with `fetch = Some(_)`.
//! 3. `SortExec.expr` is a prefix of the `DataSourceExec`'s `output_ordering`
//!    (same expressions, same sort options, element-by-element).
//! 4. The scan currently has more than 1 output partition (coalescing a
//!    single-partition source is a no-op; we skip the rewrite).
//!
//! The rule looks through a single `RepartitionExec` between the sort and the
//! `DataSourceExec` because `EnforceDistribution` commonly inserts one there.
//!
//! # Non-goals
//!
//! The rule does not try to remove the `SortExec` itself; that belongs to
//! `PushdownSort` / `EnforceSorting`.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::LexOrdering;

/// Physical optimizer rule that forces single-partition streaming when a
/// sort-matching file scan sits below a `GlobalLimitExec` with a nonzero OFFSET.
///
/// See module documentation for the full rationale and match conditions.
#[derive(Debug, Default)]
pub struct SortStreamingLimit;

impl SortStreamingLimit {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for SortStreamingLimit {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            Ok(match try_rewrite(Arc::clone(&node)) {
                Some(new_node) => Transformed::yes(new_node),
                None => Transformed::no(node),
            })
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "sort_streaming_limit"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Pattern matching and rewrite
// ---------------------------------------------------------------------------

/// Attempt to rewrite a node.  Returns `Some(new_plan)` when the full pattern
/// matches, `None` otherwise (leaving the node untouched).
fn try_rewrite(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    // 1. Must be GlobalLimitExec with skip > 0 and a fetch.
    let global_limit = plan.as_any().downcast_ref::<GlobalLimitExec>()?;
    if global_limit.skip() == 0 {
        return None;
    }
    global_limit.fetch()?; // must have a fetch bound

    // 2. Direct child must be SortExec with a fetch bound.
    let sort_exec = global_limit
        .children()
        .first()?
        .as_any()
        .downcast_ref::<SortExec>()?;
    sort_exec.fetch()?; // must have a fetch bound
    let sort_exprs = sort_exec.expr();

    // 3. Walk through optional transparent node to locate DataSourceExec.
    let sort_child: &Arc<dyn ExecutionPlan> = sort_exec.input();
    let data_source_plan = find_data_source(sort_child)?;

    // 4. DataSourceExec must report an output_ordering whose leading elements
    //    match sort_exprs exactly (prefix match).
    let scan_ordering = data_source_plan.output_ordering()?;
    if !is_prefix_of(sort_exprs, scan_ordering) {
        return None;
    }

    // 5. Only rewrite when the scan has >1 partition.
    if data_source_plan.output_partitioning().partition_count() <= 1 {
        return None;
    }

    // 6. Inject CoalescePartitionsExec between sort's current child and sort.
    let coalesce: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(Arc::clone(sort_child)));
    let new_sort: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new(sort_exprs.clone(), coalesce)
            .with_fetch(sort_exec.fetch())
            .with_preserve_partitioning(sort_exec.preserve_partitioning()),
    );

    plan.with_new_children(vec![new_sort]).ok()
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

/// Returns `true` when `prefix` is a strict prefix (or equal) to `full_order`.
/// Both orderings must be non-empty and `full_order` must have at least as many
/// elements as `prefix`.  Elements are compared by physical expression and sort
/// options.
fn is_prefix_of(prefix: &LexOrdering, full_order: &LexOrdering) -> bool {
    if full_order.len() < prefix.len() {
        return false;
    }
    prefix.iter().zip(full_order.iter()).all(|(p, f)| p == f)
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
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::LexOrdering;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::Partitioning;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
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

    /// Build a `DataSourceExec` backed by an in-memory source that reports the
    /// given `output_ordering` and has `partition_count` partitions.
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

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Positive: skip>0, expr matches ordering, multi-partition scan ->
    /// CoalescePartitionsExec is injected.
    #[test]
    fn test_match_injects_coalesce() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");
        let ordering = LexOrdering::new(vec![id_asc.clone()]).unwrap();

        let scan = make_scan(Arc::clone(&schema), ordering.clone(), 4);
        // Simulate EnforceDistribution inserting a RepartitionExec.
        let repart = Arc::new(
            RepartitionExec::try_new(Arc::clone(&scan), Partitioning::RoundRobinBatch(4)).unwrap(),
        );
        let sort = Arc::new(
            SortExec::new(ordering.clone(), repart as Arc<dyn ExecutionPlan>)
                .with_fetch(Some(1100)),
        );
        let global = Arc::new(GlobalLimitExec::new(
            sort as Arc<dyn ExecutionPlan>,
            1000,
            Some(100),
        ));

        let rule = SortStreamingLimit::new();
        let cfg = ConfigOptions::default();
        let result = rule
            .optimize(global as Arc<dyn ExecutionPlan>, &cfg)
            .unwrap();

        // Top node is still GlobalLimitExec.
        assert!(result.as_any().downcast_ref::<GlobalLimitExec>().is_some());
        // Child is SortExec.
        let sort_out = result.children()[0];
        assert!(sort_out.as_any().downcast_ref::<SortExec>().is_some());
        // SortExec's child must be CoalescePartitionsExec.
        let coalesce_out = sort_out.children()[0];
        assert!(
            coalesce_out
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some(),
            "expected CoalescePartitionsExec, got: {}",
            coalesce_out.name()
        );
        // Single output partition.
        assert_eq!(result.output_partitioning().partition_count(), 1);
    }

    /// Negative: skip=0 -> rule must not fire.
    #[test]
    fn test_no_rewrite_when_skip_is_zero() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");
        let ordering = LexOrdering::new(vec![id_asc.clone()]).unwrap();

        let scan = make_scan(Arc::clone(&schema), ordering.clone(), 4);
        let sort = Arc::new(SortExec::new(ordering.clone(), scan).with_fetch(Some(20)));
        let global = Arc::new(GlobalLimitExec::new(
            sort as Arc<dyn ExecutionPlan>,
            0,
            Some(20),
        ));

        let rule = SortStreamingLimit::new();
        let cfg = ConfigOptions::default();
        let result = rule
            .optimize(global as Arc<dyn ExecutionPlan>, &cfg)
            .unwrap();

        let sort_out = result.children()[0];
        let sort_child = sort_out.children()[0];
        assert!(
            sort_child
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_none(),
            "should not inject CoalescePartitionsExec when skip=0"
        );
    }

    /// Negative: SortExec.expr does not match scan output_ordering -> no rewrite.
    #[test]
    fn test_no_rewrite_on_ordering_mismatch() {
        let schema = test_schema();
        let id_asc = asc_expr(&schema, "id");
        let k_asc = asc_expr(&schema, "k");

        // Scan is sorted by id ASC.
        let scan_ordering = LexOrdering::new(vec![id_asc.clone()]).unwrap();
        let scan = make_scan(Arc::clone(&schema), scan_ordering, 4);

        // Sort asks for k ASC — no match.
        let sort_ordering = LexOrdering::new(vec![k_asc.clone()]).unwrap();
        let sort = Arc::new(SortExec::new(sort_ordering, scan).with_fetch(Some(1100)));
        let global = Arc::new(GlobalLimitExec::new(
            sort as Arc<dyn ExecutionPlan>,
            1000,
            Some(100),
        ));

        let rule = SortStreamingLimit::new();
        let cfg = ConfigOptions::default();
        let result = rule
            .optimize(global as Arc<dyn ExecutionPlan>, &cfg)
            .unwrap();

        let sort_out = result.children()[0];
        let sort_child = sort_out.children()[0];
        assert!(
            sort_child
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_none(),
            "should not inject CoalescePartitionsExec on ordering mismatch"
        );
    }
}
