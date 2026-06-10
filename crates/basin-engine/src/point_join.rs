//! Point-lookup two-table INNER JOIN fast path (the "ORM hydrate").
//!
//! Recognises the single most common ORM-generated JOIN shape:
//!
//! ```sql
//! SELECT e.*, u.email
//! FROM events e JOIN users u ON e.user_id = u.id
//! WHERE e.id = $1
//! ```
//!
//! The probe side is filtered by an equality on its PRIMARY KEY, so it yields
//! at most one row. The join's equi-condition links that row's join column to
//! the lookup side's PRIMARY KEY, so the lookup also yields at most one row.
//! Both sides therefore reduce to point lookups that the existing
//! [`fast_select`](crate::fast_select) machinery already serves directly from
//! storage + the memtable overlay (UPDATE/DELETE tombstones included). We run
//! two such lookups and stitch the surviving rows into one output batch,
//! bypassing the two table scans + hash join DataFusion would otherwise build.
//!
//! Recognition is split in two so the syntactic matcher stays synchronous:
//!
//!   * [`match_point_join`] — pure-syntax recognition (no catalog access).
//!     Returns a [`PointJoinPlan`] capturing the probe side, the join columns,
//!     the lookup side, and the projection list with per-side attribution.
//!   * [`execute_point_join`] — validates the metadata-dependent invariants
//!     (PK-ness of both keys, RLS/view/soft-delete/txn gates, citext), then
//!     runs the two lookups and stitches the result. Returns `Ok(None)` for
//!     anything it cannot prove safe, so the caller falls through to DataFusion.
//!
//! Conservatism rule (same as `fast_select`): when in doubt return `None` /
//! `Ok(None)`. A missed fast path is merely slow; an over-eager match is a
//! wrong answer.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, Schema};
use basin_catalog::TableMetadata;
use basin_common::{Result, TableName};
use basin_storage::ScalarValue;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator, ObjectName, SelectItem,
    SetExpr, Statement, TableFactor,
};

use crate::fast_select::{execute_simple_select, ProjectionItem, SimpleSelectPlan};
use crate::pg_ast::ObjectNamePartExt;
use crate::{ExecResult, ProjectSession};

// ─────────────────────────────────────────────────────────────────────────────
// Plan
// ─────────────────────────────────────────────────────────────────────────────

/// Which side of the join an output column is sourced from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    /// The PK-filtered probe table (the one carrying the `WHERE pk = lit`).
    Probe,
    /// The lookup table (joined on its own PK).
    Lookup,
}

/// One output column of the join projection, with table-side attribution.
#[derive(Debug, Clone)]
pub(crate) enum JoinProjItem {
    /// `alias.*` (or a bare `*` resolved to exactly one side) expands to every
    /// column of the named side, in that side's catalog column order.
    Wildcard(Side),
    /// A single column `side.col`, output under `out_name` (the bare column
    /// name, or the `AS` alias when given).
    Column {
        side: Side,
        col: String,
        out_name: String,
    },
}

/// One recognised table reference: its base name plus optional alias.
#[derive(Debug, Clone)]
struct TableRef {
    table: TableName,
    /// Lower-cased identifier used to qualify columns: the alias when present,
    /// otherwise the bare table name. sqlparser preserves case; we compare
    /// case-insensitively to mirror PG's unquoted-identifier folding for the
    /// common ORM output (all-lowercase identifiers), which is all this path
    /// targets — a mixed-case quoted identifier mismatch simply fails the
    /// match and falls through to DataFusion.
    qualifier: String,
}

/// A recognised point-lookup INNER JOIN. Syntax-only: PK-ness of the probe and
/// lookup keys is NOT verified here (the matcher cannot be async); that is
/// checked in [`execute_point_join`] against catalog metadata.
#[derive(Debug)]
pub(crate) struct PointJoinPlan {
    /// Probe table (filtered by `probe_pk_col = probe_lit`).
    probe_table: TableName,
    /// The probe-side WHERE column (must be the probe table's single-col PK).
    probe_pk_col: String,
    /// The literal the probe PK is compared against.
    probe_lit: ScalarValue,
    /// The probe-side join column (`e.user_id`).
    probe_join_col: String,
    /// Lookup table (`users`).
    lookup_table: TableName,
    /// The lookup-side join column (`u.id`); must be the lookup table's PK.
    lookup_key_col: String,
    /// Output projection in SELECT order, with side attribution.
    projection: Vec<JoinProjItem>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Syntactic recognition
// ─────────────────────────────────────────────────────────────────────────────

/// Recognise the supported two-table INNER-JOIN point-lookup shape. Returns
/// `None` (fall through to DataFusion) for anything outside it. Pure syntax —
/// no catalog access; PK validation happens in [`execute_point_join`].
pub(crate) fn match_point_join(stmt: &Statement) -> Option<PointJoinPlan> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };

    // No CTEs, no set ops, no LIMIT/OFFSET/ORDER BY/locks/etc.
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return None;
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    // No DISTINCT / GROUP BY / HAVING / window / etc.
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !select.connect_by.is_empty()
    {
        return None;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return None,
    }

    // FROM clause: exactly one base table with exactly one INNER JOIN.
    if select.from.len() != 1 {
        return None;
    }
    let from = &select.from[0];
    if from.joins.len() != 1 {
        return None;
    }
    let left = table_ref(&from.relation)?;
    let join = &from.joins[0];
    if join.global {
        return None;
    }
    // INNER JOIN only. (LEFT OUTER deliberately excluded: the NULL-stitch is
    // not clean enough to justify the risk on this path — see module docs.)
    let constraint = match &join.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => c,
        _ => return None,
    };
    let right = table_ref(&join.relation)?;

    // Distinct tables. (Self-joins would alias the same physical table on both
    // sides; the per-side lookups are still correct, but the qualifier/column
    // attribution gets ambiguous — bail to be safe.)
    if left.table == right.table {
        return None;
    }
    // Distinct qualifiers, else column attribution is ambiguous.
    if left.qualifier == right.qualifier {
        return None;
    }

    // ON constraint: a single equi-join `a.x = b.y` across the two sides.
    let on_expr = match constraint {
        JoinConstraint::On(e) => e,
        _ => return None,
    };
    let (lq, lc, rq, rc) = match equi_pair(on_expr) {
        Some(v) => v,
        None => return None,
    };
    // Each ON operand must resolve to a DIFFERENT FROM-clause table. Here
    // `Side::Probe`/`Side::Lookup` are used as neutral "left table"/"right
    // table" tags (resolve_side maps `left`→Probe, `right`→Lookup); the true
    // probe/lookup roles are assigned later from the WHERE side. We normalise
    // the ON columns so `left_join_col` is the column on the LEFT FROM table
    // and `right_join_col` is the column on the RIGHT FROM table.
    let (left_join_col, right_join_col) =
        match (resolve_side(&lq, &left, &right), resolve_side(&rq, &left, &right)) {
            // First operand → left table, second → right table.
            (Some(Side::Probe), Some(Side::Lookup)) => (lc, rc),
            // First operand → right table, second → left table.
            (Some(Side::Lookup), Some(Side::Probe)) => (rc, lc),
            _ => return None,
        };

    // WHERE clause: single `qualifier.col = literal` (or unqualified col that
    // resolves to exactly one side). The side it lands on is the PROBE side.
    let where_expr = select.selection.as_ref()?;
    let (where_qual, where_col, where_lit) = parse_where_eq(where_expr)?;
    let where_side = match where_qual {
        Some(ref q) => resolve_side(q, &left, &right)?,
        None => {
            // Unqualified WHERE column: we cannot consult the catalog here to
            // disambiguate which side it belongs to, so require qualification.
            // A missed match is merely slower (DataFusion handles it).
            return None;
        }
    };

    // Assemble probe / lookup roles from `where_side`.
    let (probe_tref, lookup_tref, probe_join_col, lookup_key_col) = match where_side {
        Side::Probe => (
            &left,
            &right,
            left_join_col.clone(),
            right_join_col.clone(),
        ),
        Side::Lookup => {
            // WHERE landed on the right table — that table is the probe. Swap
            // roles: the right side becomes probe, left becomes lookup.
            (
                &right,
                &left,
                right_join_col.clone(),
                left_join_col.clone(),
            )
        }
    };

    // Projection: only bare `side.col`, `side.col AS name`, `side.*`, or a
    // single bare `*` (resolved to all columns of BOTH sides in FROM order),
    // and bare/unqualified columns are rejected (ambiguous without catalog).
    let projection = parse_join_projection(&select.projection, probe_tref, lookup_tref)?;

    Some(PointJoinPlan {
        probe_table: probe_tref.table.clone(),
        probe_pk_col: where_col,
        probe_lit: where_lit,
        probe_join_col,
        lookup_table: lookup_tref.table.clone(),
        lookup_key_col,
        projection,
    })
}

/// Extract `(table, qualifier)` from a `TableFactor::Table` with no extras.
fn table_ref(tf: &TableFactor) -> Option<TableRef> {
    match tf {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            ..
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
            {
                return None;
            }
            let table = single_part_table(name)?;
            let qualifier = match alias {
                Some(a) => {
                    // Aliases with column lists are not the ORM shape.
                    if !a.columns.is_empty() {
                        return None;
                    }
                    a.name.value.to_ascii_lowercase()
                }
                None => table.as_str().to_ascii_lowercase(),
            };
            Some(TableRef { table, qualifier })
        }
        _ => None,
    }
}

fn single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].id_val().clone()).ok()
}

/// Parse an equi-join `a.x = b.y` into `(a_qual, x_col, b_qual, y_col)`. Both
/// operands must be compound identifiers (`qualifier.column`).
fn equi_pair(expr: &Expr) -> Option<(String, String, String, String)> {
    if let Expr::BinaryOp { left, op, right } = expr {
        if *op != BinaryOperator::Eq {
            return None;
        }
        let (lq, lc) = compound_col(left)?;
        let (rq, rc) = compound_col(right)?;
        return Some((lq, lc, rq, rc));
    }
    None
}

/// `qualifier.column` → `(qualifier_lower, column)`. Column case is preserved
/// (used for schema lookups, which are case-sensitive in Arrow); the qualifier
/// is folded to match the FROM-clause qualifier comparison.
fn compound_col(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some((
            parts[0].value.to_ascii_lowercase(),
            parts[1].value.clone(),
        )),
        _ => None,
    }
}

/// Resolve a qualifier to a side, given the two FROM-clause refs (in FROM
/// order: `left` is the base table, `right` is the joined table). Returns the
/// ROLE-NEUTRAL side identity here — `Probe`/`Lookup` are reused as "left"/
/// "right" tags and re-mapped to true roles by the caller once the WHERE side
/// is known.
fn resolve_side(qual: &str, left: &TableRef, right: &TableRef) -> Option<Side> {
    if qual == left.qualifier {
        Some(Side::Probe)
    } else if qual == right.qualifier {
        Some(Side::Lookup)
    } else {
        None
    }
}

/// Parse a single `qualifier.col = literal` WHERE clause (the only supported
/// form). Returns `(Some(qualifier), col, literal)`. Unqualified columns yield
/// `(None, col, literal)` but are rejected upstream.
fn parse_where_eq(expr: &Expr) -> Option<(Option<String>, String, ScalarValue)> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    if *op != BinaryOperator::Eq {
        return None;
    }
    // Try `col = lit` then `lit = col`.
    if let Some((q, c)) = column_ref(left) {
        if let Some(lit) = crate::fast_select::literal_value_pub(right) {
            return Some((q, c, lit));
        }
    }
    if let Some((q, c)) = column_ref(right) {
        if let Some(lit) = crate::fast_select::literal_value_pub(left) {
            return Some((q, c, lit));
        }
    }
    None
}

/// A column reference, qualified (`t.c`) or bare (`c`).
fn column_ref(expr: &Expr) -> Option<(Option<String>, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some((
            Some(parts[0].value.to_ascii_lowercase()),
            parts[1].value.clone(),
        )),
        Expr::Identifier(id) => Some((None, id.value.clone())),
        _ => None,
    }
}

/// Parse the SELECT list into per-side projection items. Accepts only:
///   * `qualifier.*`            → [`JoinProjItem::Wildcard`]
///   * `qualifier.col`          → [`JoinProjItem::Column`] (out_name = col)
///   * `qualifier.col AS name`  → [`JoinProjItem::Column`] (out_name = name)
/// Bare `*` is NOT accepted (its expansion order across sides would have to
/// match DataFusion's, which we don't want to bet on); bare unqualified
/// columns are rejected (ambiguous without the catalog).
fn parse_join_projection(
    items: &[SelectItem],
    probe: &TableRef,
    lookup: &TableRef,
) -> Option<Vec<JoinProjItem>> {
    let mut out = Vec::with_capacity(items.len());
    let side_of = |qual: &str| -> Option<Side> {
        if qual == probe.qualifier {
            Some(Side::Probe)
        } else if qual == lookup.qualifier {
            Some(Side::Lookup)
        } else {
            None
        }
    };
    for item in items {
        match item {
            SelectItem::QualifiedWildcard(name, opts) => {
                if opts.opt_ilike.is_some()
                    || opts.opt_exclude.is_some()
                    || opts.opt_except.is_some()
                    || opts.opt_replace.is_some()
                    || opts.opt_rename.is_some()
                {
                    return None;
                }
                // `qualifier.*` — single-part object name.
                let sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(obj) = name
                else {
                    return None;
                };
                if obj.0.len() != 1 {
                    return None;
                }
                let qual = obj.0[0].id_val().to_ascii_lowercase();
                out.push(JoinProjItem::Wildcard(side_of(&qual)?));
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                let qual = parts[0].value.to_ascii_lowercase();
                let col = parts[1].value.clone();
                let side = side_of(&qual)?;
                out.push(JoinProjItem::Column {
                    side,
                    out_name: col.clone(),
                    col,
                });
            }
            SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(parts),
                alias,
            } if parts.len() == 2 => {
                let qual = parts[0].value.to_ascii_lowercase();
                let col = parts[1].value.clone();
                let side = side_of(&qual)?;
                out.push(JoinProjItem::Column {
                    side,
                    col,
                    out_name: alias.value.clone(),
                });
            }
            // Bare `*`, bare identifiers, expressions, aggregates → reject.
            _ => return None,
        }
    }
    if out.is_empty() {
        return None;
    }
    Some(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Execution
// ─────────────────────────────────────────────────────────────────────────────

/// Execute a recognised [`PointJoinPlan`]. Validates the metadata-dependent
/// invariants (PK-ness of both join keys, view/RLS/soft-delete/txn/citext
/// gates) and runs the two point lookups, stitching the surviving rows. Returns
/// `Ok(None)` whenever any gate or invariant fails so the caller falls through
/// to the DataFusion path.
pub(crate) async fn execute_point_join(
    sess: &ProjectSession,
    plan: PointJoinPlan,
    raw_sql: &str,
) -> Result<Option<ExecResult>> {
    // Transaction gate: same reasoning as fast_select — pending writes in an
    // open txn are not visible to the fast read path. Bail to DataFusion.
    if crate::session::tx_is_active(&sess.state) {
        return Ok(None);
    }

    // Load metadata for BOTH tables (epoch-validated per-session cache). A miss
    // (None) means a catalog error or unknown table → fall through.
    let probe_meta = match crate::session::load_table_meta_cached(sess, &plan.probe_table).await {
        Some((arc, view_present)) => {
            if view_present {
                return Ok(None);
            }
            (*arc).clone()
        }
        None => return Ok(None),
    };
    let lookup_meta = match crate::session::load_table_meta_cached(sess, &plan.lookup_table).await {
        Some((arc, view_present)) => {
            if view_present {
                return Ok(None);
            }
            (*arc).clone()
        }
        None => return Ok(None),
    };

    // Gate: no RLS, no soft-delete on EITHER table. (Views handled above.)
    for m in [&probe_meta, &lookup_meta] {
        if m.rls_enabled {
            return Ok(None);
        }
        if crate::types::soft_delete_column(m.schema.as_ref()).is_some() {
            return Ok(None);
        }
    }

    // PK invariants: the probe WHERE column must be the probe table's SINGLE
    // primary-key column, and the lookup join column must be the lookup
    // table's SINGLE primary-key column. Single-column PK only, so each lookup
    // yields ≤1 row.
    if probe_meta.pk_columns.as_slice() != [plan.probe_pk_col.clone()] {
        return Ok(None);
    }
    if lookup_meta.pk_columns.as_slice() != [plan.lookup_key_col.clone()] {
        return Ok(None);
    }

    // Citext gate: byte-exact comparisons in the fast path don't honour the
    // case-insensitive citext contract. Bail if either join/PK column is citext.
    if is_citext(&probe_meta, &plan.probe_pk_col)
        || is_citext(&probe_meta, &plan.probe_join_col)
        || is_citext(&lookup_meta, &plan.lookup_key_col)
    {
        return Ok(None);
    }

    // ── Probe lookup ─────────────────────────────────────────────────────────
    //
    // Read every column the output needs from the probe side, PLUS the probe
    // join column (needed to derive the lookup key). Build a SimpleSelectPlan
    // with a single Eq predicate on the probe PK and reuse execute_simple_select
    // (which serves the overlay/tombstone-merged row directly from storage).
    let probe_out_cols = side_columns(&plan, Side::Probe, &probe_meta);
    let mut probe_read: Vec<String> = probe_out_cols.clone();
    if !probe_read.contains(&plan.probe_join_col) {
        probe_read.push(plan.probe_join_col.clone());
    }
    // Always read the probe PK column too: the cold-tier tombstone / UPDATE
    // overlay in `execute_simple_select` can only suppress a deleted/overridden
    // row when the batch carries the PK column to encode and match (see the
    // lookup side below). Without it a tombstoned probe row would survive.
    if !probe_read.contains(&plan.probe_pk_col) {
        probe_read.push(plan.probe_pk_col.clone());
    }
    let probe_plan = SimpleSelectPlan {
        table: plan.probe_table.clone(),
        projection: Some(
            probe_read
                .iter()
                .map(|c| ProjectionItem::Column(c.clone()))
                .collect(),
        ),
        read_cols: Some({
            let mut rc = probe_read.clone();
            rc.sort_unstable();
            rc.dedup();
            rc
        }),
        aggregates: None,
        predicates: vec![basin_storage::Predicate::Eq(
            plan.probe_pk_col.clone(),
            plan.probe_lit.clone(),
        )],
        is_null_cols: vec![],
        in_list_preds: vec![],
        limit: None,
        offset: None,
        order_by: None,
        always_empty: false,
    };
    let probe_res =
        execute_simple_select(sess, probe_plan, Some(probe_meta.clone()), raw_sql, false).await?;
    let probe_batch = match single_row(&probe_res) {
        SingleRow::Row(b) => b,
        // INNER JOIN: no probe row → empty result with the correct schema.
        SingleRow::Empty => {
            return Ok(Some(empty_result(&plan, &probe_meta, &lookup_meta)?));
        }
        SingleRow::Unsupported => return Ok(None),
    };

    // Extract the join key value from the probe row. A NULL join key matches no
    // row under INNER-JOIN semantics → empty result.
    let join_key = match extract_scalar(&probe_batch, &plan.probe_join_col, &lookup_meta, &plan.lookup_key_col) {
        ExtractedKey::Value(v) => v,
        ExtractedKey::Null => {
            return Ok(Some(empty_result(&plan, &probe_meta, &lookup_meta)?));
        }
        ExtractedKey::Unsupported => return Ok(None),
    };

    // ── Lookup ───────────────────────────────────────────────────────────────
    let lookup_out_cols = side_columns(&plan, Side::Lookup, &lookup_meta);
    // The lookup may project zero columns (e.g. SELECT e.* only) — but the plan
    // always references at least one column overall; if the lookup side has no
    // output columns we still must verify the joined row EXISTS (INNER JOIN),
    // so read its key column.
    let mut lookup_read = lookup_out_cols.clone();
    // Always read the lookup PK column: `execute_simple_select` applies the
    // hot-tier tombstone / UPDATE-override overlay to the cold batch by encoding
    // each row's PK and matching it against the snapshot. When the projected
    // batch omits the PK column the filter has no key to compare and passes the
    // (stale) cold row through unchanged — so a DELETE'd lookup row would still
    // stitch into the join (bug: tombstone_delete_lookup_row_drops_join). The
    // PK is needed for the INNER-JOIN existence check anyway. The extra column
    // is harmless: `stitch` pulls output columns by name, ignoring the rest.
    if !lookup_read.contains(&plan.lookup_key_col) {
        lookup_read.push(plan.lookup_key_col.clone());
    }
    let lookup_plan = SimpleSelectPlan {
        table: plan.lookup_table.clone(),
        projection: Some(
            lookup_read
                .iter()
                .map(|c| ProjectionItem::Column(c.clone()))
                .collect(),
        ),
        read_cols: Some({
            let mut rc = lookup_read.clone();
            rc.sort_unstable();
            rc.dedup();
            rc
        }),
        aggregates: None,
        predicates: vec![basin_storage::Predicate::Eq(
            plan.lookup_key_col.clone(),
            join_key,
        )],
        is_null_cols: vec![],
        in_list_preds: vec![],
        limit: None,
        offset: None,
        order_by: None,
        always_empty: false,
    };
    let lookup_res =
        execute_simple_select(sess, lookup_plan, Some(lookup_meta.clone()), raw_sql, false).await?;
    let lookup_batch = match single_row(&lookup_res) {
        SingleRow::Row(b) => b,
        SingleRow::Empty => {
            return Ok(Some(empty_result(&plan, &probe_meta, &lookup_meta)?));
        }
        SingleRow::Unsupported => return Ok(None),
    };

    // ── Stitch one output row ─────────────────────────────────────────────────
    stitch(&plan, &probe_meta, &lookup_meta, &probe_batch, &lookup_batch).map(Some)
}

/// Returns the list of THIS-side output columns (in projection order, wildcards
/// expanded to the side's catalog column order) referenced by the plan.
fn side_columns(plan: &PointJoinPlan, side: Side, meta: &TableMetadata) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut push = |c: String, cols: &mut Vec<String>| {
        if !cols.contains(&c) {
            cols.push(c);
        }
    };
    for item in &plan.projection {
        match item {
            JoinProjItem::Wildcard(s) if *s == side => {
                for f in meta.schema.fields() {
                    push(f.name().clone(), &mut cols);
                }
            }
            JoinProjItem::Column { side: s, col, .. } if *s == side => {
                push(col.clone(), &mut cols);
            }
            _ => {}
        }
    }
    cols
}

/// True when `col` on `meta` is declared as citext (BASIN_TYPE=CITEXT).
fn is_citext(meta: &TableMetadata, col: &str) -> bool {
    meta.schema
        .field_with_name(col)
        .ok()
        .map(|f| {
            f.metadata()
                .get(crate::types::BASIN_TYPE_KEY)
                .map(|v| v.as_str() == crate::types::BASIN_TYPE_CITEXT)
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

enum SingleRow {
    Row(RecordBatch),
    Empty,
    /// Result was not a simple row set, or had >1 row (should be impossible for
    /// a PK lookup, but bail conservatively).
    Unsupported,
}

/// Collapse an [`ExecResult`] from a PK point lookup into at most one row.
fn single_row(res: &ExecResult) -> SingleRow {
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return SingleRow::Unsupported,
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    match total {
        0 => SingleRow::Empty,
        1 => {
            // Find the single non-empty batch.
            for b in batches {
                if b.num_rows() == 1 {
                    return SingleRow::Row(b.clone());
                }
            }
            SingleRow::Unsupported
        }
        // A PK lookup must be unique; >1 row signals a violated assumption.
        _ => SingleRow::Unsupported,
    }
}

enum ExtractedKey {
    Value(ScalarValue),
    Null,
    Unsupported,
}

/// Extract the join key from the probe row's `col`, typed to match the lookup
/// side's `lookup_key_col`. The lookup-side column type dictates the
/// `ScalarValue` variant so the lookup's `Predicate::Eq` compares correctly.
fn extract_scalar(
    batch: &RecordBatch,
    col: &str,
    lookup_meta: &TableMetadata,
    lookup_key_col: &str,
) -> ExtractedKey {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int16Type, Int32Type, Int64Type, UInt32Type, UInt64Type};
    use arrow_schema::DataType as Dt;

    let Ok(idx) = batch.schema().index_of(col) else {
        return ExtractedKey::Unsupported;
    };
    let arr = batch.column(idx);
    if arr.is_null(0) {
        return ExtractedKey::Null;
    }

    // Target variant is dictated by the lookup PK column's type so the Eq
    // predicate downstream coerces correctly (Int64 scalar widens vs narrower
    // int columns; Utf8 vs any string encoding — see batch_matches_predicates).
    let lookup_dt = match lookup_meta.schema.field_with_name(lookup_key_col) {
        Ok(f) => f.data_type().clone(),
        Err(_) => return ExtractedKey::Unsupported,
    };

    match lookup_dt {
        Dt::Int64 | Dt::Int32 | Dt::Int16 | Dt::UInt32 => {
            // Read the probe value as i64 regardless of its own width.
            let v: Option<i64> = match arr.data_type() {
                Dt::Int64 => Some(arr.as_primitive::<Int64Type>().value(0)),
                Dt::Int32 => Some(arr.as_primitive::<Int32Type>().value(0) as i64),
                Dt::Int16 => Some(arr.as_primitive::<Int16Type>().value(0) as i64),
                Dt::UInt32 => Some(arr.as_primitive::<UInt32Type>().value(0) as i64),
                _ => None,
            };
            match v {
                Some(v) => ExtractedKey::Value(ScalarValue::Int64(v)),
                None => ExtractedKey::Unsupported,
            }
        }
        Dt::UInt64 => match arr.data_type() {
            Dt::UInt64 => ExtractedKey::Value(ScalarValue::UInt64(
                arr.as_primitive::<UInt64Type>().value(0),
            )),
            _ => ExtractedKey::Unsupported,
        },
        Dt::Utf8 | Dt::LargeUtf8 | Dt::Utf8View => {
            let s: Option<String> = match arr.data_type() {
                Dt::Utf8 => Some(arr.as_string::<i32>().value(0).to_string()),
                Dt::LargeUtf8 => Some(arr.as_string::<i64>().value(0).to_string()),
                Dt::Utf8View => Some(arr.as_string_view().value(0).to_string()),
                _ => None,
            };
            match s {
                Some(s) => ExtractedKey::Value(ScalarValue::Utf8(s)),
                None => ExtractedKey::Unsupported,
            }
        }
        // Other PK types (float, bool, timestamp, …) are unusual as a join key
        // and not worth the type-coercion surface here — fall through.
        _ => ExtractedKey::Unsupported,
    }
}

/// The output Arrow schema for this plan, expanding wildcards in each side's
/// catalog column order. Returns `Ok(None)`-equivalent (an Err is never used)
/// — duplicate output field names are allowed (Arrow permits them; PG/DF allow
/// duplicate result column names too).
fn output_schema(
    plan: &PointJoinPlan,
    probe_meta: &TableMetadata,
    lookup_meta: &TableMetadata,
) -> Result<Arc<Schema>> {
    let mut fields: Vec<Arc<Field>> = Vec::new();
    for item in &plan.projection {
        match item {
            JoinProjItem::Wildcard(side) => {
                let meta = if *side == Side::Probe { probe_meta } else { lookup_meta };
                for f in meta.schema.fields() {
                    fields.push(f.clone());
                }
            }
            JoinProjItem::Column {
                side,
                col,
                out_name,
            } => {
                let meta = if *side == Side::Probe { probe_meta } else { lookup_meta };
                let src = meta
                    .schema
                    .field_with_name(col)
                    .map_err(|_| basin_common::BasinError::InvalidSchema(format!(
                        "point-join: unknown column {col}"
                    )))?;
                // Preserve the source field's type/nullability, rename to out_name.
                fields.push(Arc::new(
                    Field::new(out_name, src.data_type().clone(), src.is_nullable())
                        .with_metadata(src.metadata().clone()),
                ));
            }
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Build an empty (zero-row) result with the correct output schema. Used for
/// INNER-JOIN misses (probe miss, lookup miss, or NULL join key).
fn empty_result(
    plan: &PointJoinPlan,
    probe_meta: &TableMetadata,
    lookup_meta: &TableMetadata,
) -> Result<ExecResult> {
    let schema = output_schema(plan, probe_meta, lookup_meta)?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![],
    })
}

/// Stitch the single probe row and single lookup row into one output batch in
/// projection order. Each output column is pulled by NAME from its side's
/// batch (the per-side SimpleSelectPlan projected the needed columns); wildcard
/// columns are pulled in the side's catalog order.
fn stitch(
    plan: &PointJoinPlan,
    probe_meta: &TableMetadata,
    lookup_meta: &TableMetadata,
    probe_batch: &RecordBatch,
    lookup_batch: &RecordBatch,
) -> Result<ExecResult> {
    let schema = output_schema(plan, probe_meta, lookup_meta)?;
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(schema.fields().len());

    let pull = |b: &RecordBatch, name: &str| -> Result<arrow_array::ArrayRef> {
        let idx = b.schema().index_of(name).map_err(|_| {
            basin_common::BasinError::internal(format!("point-join stitch: batch missing {name}"))
        })?;
        Ok(b.column(idx).clone())
    };

    for item in &plan.projection {
        match item {
            JoinProjItem::Wildcard(side) => {
                let (meta, b) = if *side == Side::Probe {
                    (probe_meta, probe_batch)
                } else {
                    (lookup_meta, lookup_batch)
                };
                for f in meta.schema.fields() {
                    columns.push(pull(b, f.name())?);
                }
            }
            JoinProjItem::Column { side, col, .. } => {
                let b = if *side == Side::Probe {
                    probe_batch
                } else {
                    lookup_batch
                };
                columns.push(pull(b, col)?);
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| basin_common::BasinError::internal(format!("point-join stitch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests (syntactic recognition only; execution is covered by the
// integration test `point_join_probe.rs`).
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap()
    }

    #[test]
    fn matches_canonical_orm_hydrate() {
        let stmt =
            parse("SELECT e.*, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1");
        let plan = match_point_join(&stmt).expect("canonical hydrate must match");
        assert_eq!(plan.probe_table.as_str(), "events");
        assert_eq!(plan.probe_pk_col, "id");
        assert_eq!(plan.probe_join_col, "user_id");
        assert_eq!(plan.lookup_table.as_str(), "users");
        assert_eq!(plan.lookup_key_col, "id");
        assert_eq!(plan.projection.len(), 2);
        assert!(matches!(plan.projection[0], JoinProjItem::Wildcard(Side::Probe)));
    }

    #[test]
    fn matches_where_on_lookup_side_swaps_roles() {
        // WHERE on `u` makes users the probe and events the lookup.
        let stmt =
            parse("SELECT u.*, e.id FROM events e JOIN users u ON e.user_id = u.id WHERE u.id = 7");
        let plan = match_point_join(&stmt).expect("role-swap must match");
        assert_eq!(plan.probe_table.as_str(), "users");
        assert_eq!(plan.probe_pk_col, "id");
        // probe (users) join col is u.id; lookup (events) key col is e.user_id.
        assert_eq!(plan.probe_join_col, "id");
        assert_eq!(plan.lookup_table.as_str(), "events");
        assert_eq!(plan.lookup_key_col, "user_id");
    }

    #[test]
    fn matches_aliased_projection() {
        let stmt = parse(
            "SELECT e.id AS eid, u.email AS addr FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1",
        );
        let plan = match_point_join(&stmt).expect("aliased projection must match");
        match &plan.projection[0] {
            JoinProjItem::Column { out_name, col, side } => {
                assert_eq!(out_name, "eid");
                assert_eq!(col, "id");
                assert_eq!(*side, Side::Probe);
            }
            _ => panic!("expected aliased column"),
        }
    }

    #[test]
    fn rejects_three_table_join() {
        let stmt = parse(
            "SELECT e.* FROM events e JOIN users u ON e.user_id = u.id JOIN orgs o ON u.org_id = o.id WHERE e.id = 1",
        );
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_left_join() {
        let stmt =
            parse("SELECT e.* FROM events e LEFT JOIN users u ON e.user_id = u.id WHERE e.id = 1");
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_no_where() {
        let stmt = parse("SELECT e.*, u.email FROM events e JOIN users u ON e.user_id = u.id");
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_non_eq_where() {
        let stmt =
            parse("SELECT e.* FROM events e JOIN users u ON e.user_id = u.id WHERE e.id > 1");
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_unqualified_where() {
        let stmt =
            parse("SELECT e.* FROM events e JOIN users u ON e.user_id = u.id WHERE id = 1");
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_bare_star() {
        let stmt = parse("SELECT * FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1");
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_expression_projection() {
        let stmt = parse(
            "SELECT e.id + 1 AS x FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1",
        );
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_group_by() {
        let stmt = parse(
            "SELECT e.id FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1 GROUP BY e.id",
        );
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn rejects_limit() {
        let stmt = parse(
            "SELECT e.*, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1 LIMIT 1",
        );
        assert!(match_point_join(&stmt).is_none());
    }

    #[test]
    fn string_literal_where_matches() {
        let stmt = parse(
            "SELECT e.*, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 'abc'",
        );
        let plan = match_point_join(&stmt).expect("string-literal WHERE must match");
        assert_eq!(plan.probe_lit, ScalarValue::Utf8("abc".to_string()));
    }
}
