//! Read-time predicates.
//!
//! We define our own enum (rather than reusing `datafusion`'s `Expr` or
//! `parquet`'s `RowFilter` directly) so this crate stays small and so
//! callers in higher layers — once they exist — can map their richer
//! expression languages down to a constrained set we know how to push down.
//!
//! Supported today: column equality, less-than, greater-than against an
//! `Int64`, `UInt64`, or `Utf8` literal. Anything else is evaluated lazily
//! (i.e. not pushed into Parquet stats pruning) — see `apply_to_batch`.
//!
//! [`CompoundPredicate`] sits one level up: an `AND`/`OR`/`NOT` tree over
//! [`Predicate`] atoms plus `IS NULL` / `IS NOT NULL` / `IN`. The DML
//! mutation path uses it for richer WHERE clauses; the read path still
//! flattens to `Vec<Predicate>` because Parquet row-group pruning works
//! best on a conjunction of atoms.

use std::collections::BTreeMap;

use arrow_array::{builder::BooleanBuilder, Array, BooleanArray, RecordBatch};
use basin_common::{BasinError, Result};

use crate::data_file::ColumnStats;

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
}

#[derive(Clone, Debug)]
pub enum Predicate {
    Eq(String, ScalarValue),
    Gt(String, ScalarValue),
    Lt(String, ScalarValue),
}

impl Predicate {
    pub fn column(&self) -> &str {
        match self {
            Predicate::Eq(c, _) | Predicate::Gt(c, _) | Predicate::Lt(c, _) => c,
        }
    }
}

/// Apply a single predicate to a `RecordBatch` and return a boolean mask.
/// Used as a fallback when row-group / page pruning isn't enough; i.e. the
/// final per-row filter after Parquet has handed us back a coarse-grained
/// superset.
pub fn evaluate(
    batch: &arrow_array::RecordBatch,
    predicate: &Predicate,
) -> Result<arrow_array::BooleanArray> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
    use arrow_array::Array;
    use arrow_array::BooleanArray;

    let col_name = predicate.column();
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col_name}")))?;

    macro_rules! cmp_primitive {
        ($arrow_ty:ty, $val:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let v = $val;
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) $op v);
                }
            }
            b.finish()
        }};
    }

    let mask: BooleanArray = match (predicate, &predicate_value(predicate)) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Utf8(v))
        | (Predicate::Gt(_, _), ScalarValue::Utf8(v))
        | (Predicate::Lt(_, _), ScalarValue::Utf8(v)) => {
            let arr = col.as_string::<i32>();
            let op: fn(&str, &str) -> bool = match predicate {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
            };
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(op(arr.value(i), v.as_str()));
                }
            }
            b.finish()
        }
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) => {
            let arr = col.as_boolean();
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) == *v);
                }
            }
            b.finish()
        }
        (op, val) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate combination: {op:?} on {val:?}"
            )));
        }
    };

    Ok(mask)
}

fn predicate_value(p: &Predicate) -> ScalarValue {
    match p {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v.clone(),
    }
}

/// A boolean expression over [`Predicate`] atoms plus null-checks and `IN`
/// lists. The DML mutation path parses the WHERE clause into this shape;
/// reads still consume `Vec<Predicate>` (a conjunction of atoms) because
/// Parquet pruning is friendliest there.
#[derive(Clone, Debug)]
pub enum CompoundPredicate {
    Atom(Predicate),
    And(Vec<CompoundPredicate>),
    Or(Vec<CompoundPredicate>),
    Not(Box<CompoundPredicate>),
    /// `<col> IS NULL`.
    IsNull(String),
    /// `<col> IS NOT NULL`.
    IsNotNull(String),
    /// `<col> IN (lit, lit, ...)`. An empty list always evaluates to
    /// false, matching SQL semantics.
    In(String, Vec<ScalarValue>),
}

/// Apply a [`CompoundPredicate`] to a batch and return a boolean mask the
/// same length as the batch. NULL outcomes inside atoms are preserved (a
/// NULL on either operand of a comparison yields a NULL mask bit, just as
/// Postgres returns UNKNOWN). Logical combinators follow Kleene's
/// three-valued logic:
///   - `NULL AND false` → false
///   - `NULL AND true`  → NULL
///   - `NULL OR true`   → true
///   - `NULL OR false`  → NULL
///   - `NOT NULL`       → NULL
///
/// Three-valued logic matters for DELETE: a NULL mask bit means "we don't
/// know if this row matched", and the caller must conservatively keep
/// such rows (see `dml_mutate::invert_mask`).
pub fn evaluate_compound(
    batch: &RecordBatch,
    pred: &CompoundPredicate,
) -> Result<BooleanArray> {
    match pred {
        CompoundPredicate::Atom(p) => evaluate(batch, p),
        CompoundPredicate::And(children) => {
            if children.is_empty() {
                // Vacuously true.
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            }
            let mut acc: Option<BooleanArray> = None;
            for c in children {
                let m = evaluate_compound(batch, c)?;
                acc = Some(match acc {
                    None => m,
                    Some(prev) => and_kleene(&prev, &m),
                });
            }
            Ok(acc.expect("at least one child"))
        }
        CompoundPredicate::Or(children) => {
            if children.is_empty() {
                // Vacuously false.
                return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
            }
            let mut acc: Option<BooleanArray> = None;
            for c in children {
                let m = evaluate_compound(batch, c)?;
                acc = Some(match acc {
                    None => m,
                    Some(prev) => or_kleene(&prev, &m),
                });
            }
            Ok(acc.expect("at least one child"))
        }
        CompoundPredicate::Not(inner) => {
            let m = evaluate_compound(batch, inner)?;
            Ok(not_kleene(&m))
        }
        CompoundPredicate::IsNull(col) => is_null_mask(batch, col, true),
        CompoundPredicate::IsNotNull(col) => is_null_mask(batch, col, false),
        CompoundPredicate::In(col, values) => in_mask(batch, col, values),
    }
}

fn and_kleene(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        let av = if a.is_null(i) { None } else { Some(a.value(i)) };
        let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
        match (av, bv) {
            (Some(false), _) | (_, Some(false)) => out.append_value(false),
            (Some(true), Some(true)) => out.append_value(true),
            _ => out.append_null(),
        }
    }
    out.finish()
}

fn or_kleene(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        let av = if a.is_null(i) { None } else { Some(a.value(i)) };
        let bv = if b.is_null(i) { None } else { Some(b.value(i)) };
        match (av, bv) {
            (Some(true), _) | (_, Some(true)) => out.append_value(true),
            (Some(false), Some(false)) => out.append_value(false),
            _ => out.append_null(),
        }
    }
    out.finish()
}

fn not_kleene(a: &BooleanArray) -> BooleanArray {
    let mut out = BooleanBuilder::with_capacity(a.len());
    for i in 0..a.len() {
        if a.is_null(i) {
            out.append_null();
        } else {
            out.append_value(!a.value(i));
        }
    }
    out.finish()
}

fn is_null_mask(batch: &RecordBatch, col: &str, want_null: bool) -> Result<BooleanArray> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col}")))?;
    let mut out = BooleanBuilder::with_capacity(arr.len());
    for i in 0..arr.len() {
        out.append_value(arr.is_null(i) == want_null);
    }
    Ok(out.finish())
}

fn in_mask(batch: &RecordBatch, col: &str, values: &[ScalarValue]) -> Result<BooleanArray> {
    if values.is_empty() {
        return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
    }
    // Reuse the per-atom evaluator: build an OR over `Eq(col, v)` atoms.
    // This keeps type-coercion centralised in `evaluate`.
    let or_pred = CompoundPredicate::Or(
        values
            .iter()
            .map(|v| CompoundPredicate::Atom(Predicate::Eq(col.to_string(), v.clone())))
            .collect(),
    );
    evaluate_compound(batch, &or_pred)
}

/// Coarse outcome of pruning a predicate against a single file's stats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneOutcome {
    /// Stats prove no row in the file can match. Caller may skip the file.
    NoMatch,
    /// Stats prove every row in the file matches. Caller can shortcut for
    /// DELETE (drop the file outright); UPDATE still needs to read for SET
    /// application.
    AllMatch,
    /// Stats are inconclusive (or the predicate isn't prunable). Caller
    /// must read the file and evaluate per row.
    Mixed,
}

impl PruneOutcome {
    fn invert(self) -> Self {
        match self {
            PruneOutcome::NoMatch => PruneOutcome::AllMatch,
            PruneOutcome::AllMatch => PruneOutcome::NoMatch,
            PruneOutcome::Mixed => PruneOutcome::Mixed,
        }
    }
}

/// Decide whether a file with the supplied per-column stats can be pruned
/// against `pred`. The caller passes the table's Arrow schema so we can
/// decode the bytes Parquet stores in `min_bytes` / `max_bytes`.
///
/// CORRECTNESS RULE: when in doubt, return [`PruneOutcome::Mixed`] — that
/// forces the caller to read and re-evaluate, which is wasteful but never
/// loses data. Returning `NoMatch` for a file that contains a matching row
/// is a data-loss bug.
pub fn evaluate_compound_for_pruning(
    pred: &CompoundPredicate,
    stats: &BTreeMap<String, ColumnStats>,
    schema: &arrow_schema::Schema,
    row_count: u64,
) -> PruneOutcome {
    match pred {
        CompoundPredicate::Atom(atom) => prune_atom(atom, stats, schema),
        CompoundPredicate::And(children) => {
            // AND: any NoMatch child proves the whole thing is NoMatch.
            // Otherwise, AllMatch only if every child is AllMatch.
            let mut all_match = true;
            for c in children {
                match evaluate_compound_for_pruning(c, stats, schema, row_count) {
                    PruneOutcome::NoMatch => return PruneOutcome::NoMatch,
                    PruneOutcome::Mixed => all_match = false,
                    PruneOutcome::AllMatch => {}
                }
            }
            if children.is_empty() || all_match {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        CompoundPredicate::Or(children) => {
            // OR: any AllMatch child proves AllMatch. Otherwise, NoMatch
            // only if every child is NoMatch.
            let mut all_no_match = true;
            for c in children {
                match evaluate_compound_for_pruning(c, stats, schema, row_count) {
                    PruneOutcome::AllMatch => return PruneOutcome::AllMatch,
                    PruneOutcome::Mixed => all_no_match = false,
                    PruneOutcome::NoMatch => {}
                }
            }
            if children.is_empty() || all_no_match {
                PruneOutcome::NoMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        CompoundPredicate::Not(inner) => {
            evaluate_compound_for_pruning(inner, stats, schema, row_count).invert()
        }
        CompoundPredicate::IsNull(col) => match stats.get(col) {
            Some(s) => match (s.null_count, row_count) {
                (Some(0), _) => PruneOutcome::NoMatch,
                (Some(n), rc) if n == rc => PruneOutcome::AllMatch,
                _ => PruneOutcome::Mixed,
            },
            None => PruneOutcome::Mixed,
        },
        CompoundPredicate::IsNotNull(col) => match stats.get(col) {
            Some(s) => match (s.null_count, row_count) {
                (Some(0), _) => PruneOutcome::AllMatch,
                (Some(n), rc) if n == rc => PruneOutcome::NoMatch,
                _ => PruneOutcome::Mixed,
            },
            None => PruneOutcome::Mixed,
        },
        CompoundPredicate::In(col, values) => {
            // IN is an OR of Eq atoms; reuse the OR rule.
            let alts: Vec<CompoundPredicate> = values
                .iter()
                .map(|v| CompoundPredicate::Atom(Predicate::Eq(col.clone(), v.clone())))
                .collect();
            evaluate_compound_for_pruning(
                &CompoundPredicate::Or(alts),
                stats,
                schema,
                row_count,
            )
        }
    }
}

/// Prune a single atom against per-column stats. Returns `Mixed` if the
/// stats can't decide either way — this is the safe default and the rule
/// that keeps us from ever skipping a file that contains a matching row.
fn prune_atom(
    atom: &Predicate,
    stats: &BTreeMap<String, ColumnStats>,
    schema: &arrow_schema::Schema,
) -> PruneOutcome {
    let col = atom.column();
    let cs = match stats.get(col) {
        Some(s) => s,
        None => return PruneOutcome::Mixed,
    };
    // If ANY value in this column might be NULL, a comparison against a
    // non-null literal evaluates to UNKNOWN for those rows (treated as
    // false for matching, but it means we can never prove AllMatch).
    let has_nulls = cs.null_count.map(|n| n > 0).unwrap_or(true);

    let field = match schema.fields().iter().find(|f| f.name() == col) {
        Some(f) => f,
        None => return PruneOutcome::Mixed,
    };
    let dt = field.data_type();
    let value = match atom {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v,
    };
    let (Some(min), Some(max)) = (cs.min_bytes.as_deref(), cs.max_bytes.as_deref()) else {
        return PruneOutcome::Mixed;
    };
    use arrow_schema::DataType;
    match (dt, value) {
        (DataType::Int64, ScalarValue::Int64(v)) => {
            let (Some(min), Some(max)) = (decode_i64(min), decode_i64(max)) else {
                return PruneOutcome::Mixed;
            };
            decide_numeric(atom, *v, min, max, has_nulls)
        }
        (DataType::Float64, ScalarValue::Float64(v)) => {
            let (Some(min), Some(max)) = (decode_f64(min), decode_f64(max)) else {
                return PruneOutcome::Mixed;
            };
            decide_numeric(atom, *v, min, max, has_nulls)
        }
        (DataType::Boolean, ScalarValue::Boolean(v)) => {
            // Parquet encodes a boolean stat as a single byte 0/1.
            let min = min.first().copied().map(|b| b != 0);
            let max = max.first().copied().map(|b| b != 0);
            match (min, max, atom) {
                (Some(min), Some(max), Predicate::Eq(_, _)) => {
                    if min == max && min == *v {
                        if has_nulls {
                            PruneOutcome::Mixed
                        } else {
                            PruneOutcome::AllMatch
                        }
                    } else if min == max && min != *v {
                        PruneOutcome::NoMatch
                    } else {
                        PruneOutcome::Mixed
                    }
                }
                _ => PruneOutcome::Mixed,
            }
        }
        (DataType::Utf8, ScalarValue::Utf8(v)) => {
            let bytes = v.as_bytes();
            decide_byte_lex(atom, bytes, min, max, has_nulls)
        }
        _ => PruneOutcome::Mixed,
    }
}

fn decide_numeric<T: PartialOrd + Copy>(
    atom: &Predicate,
    v: T,
    min: T,
    max: T,
    has_nulls: bool,
) -> PruneOutcome {
    match atom {
        Predicate::Eq(_, _) => {
            if v < min || v > max {
                PruneOutcome::NoMatch
            } else if min == max && min == v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Lt(_, _) => {
            if min >= v {
                PruneOutcome::NoMatch
            } else if max < v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Gt(_, _) => {
            if max <= v {
                PruneOutcome::NoMatch
            } else if min > v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
    }
}

fn decide_byte_lex(
    atom: &Predicate,
    v: &[u8],
    min: &[u8],
    max: &[u8],
    has_nulls: bool,
) -> PruneOutcome {
    match atom {
        Predicate::Eq(_, _) => {
            if v < min || v > max {
                PruneOutcome::NoMatch
            } else if min == max && min == v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Lt(_, _) => {
            if min >= v {
                PruneOutcome::NoMatch
            } else if max < v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
        Predicate::Gt(_, _) => {
            if max <= v {
                PruneOutcome::NoMatch
            } else if min > v && !has_nulls {
                PruneOutcome::AllMatch
            } else {
                PruneOutcome::Mixed
            }
        }
    }
}

fn decode_i64(b: &[u8]) -> Option<i64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(i64::from_le_bytes(arr))
}

fn decode_f64(b: &[u8]) -> Option<f64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(f64::from_le_bytes(arr))
}

#[cfg(test)]
mod compound_tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn small_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Int64Array::from(vec![Some(1), Some(2), None, Some(4), Some(5)]);
        let names = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
            Some("e"),
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn collect_mask(arr: &BooleanArray) -> Vec<Option<bool>> {
        (0..arr.len())
            .map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) })
            .collect()
    }

    #[test]
    fn and_combines_atoms() {
        let pred = CompoundPredicate::And(vec![
            CompoundPredicate::Atom(Predicate::Gt("id".into(), ScalarValue::Int64(1))),
            CompoundPredicate::Atom(Predicate::Lt("id".into(), ScalarValue::Int64(5))),
        ]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // ids: 1,2,NULL,4,5 → only 2 and 4 match (>1 AND <5).
        assert_eq!(
            collect_mask(&mask),
            vec![
                Some(false), // 1 > 1 = false
                Some(true),
                Some(false), // null id → atom returns false; AND with false = false
                Some(true),
                Some(false),
            ]
        );
    }

    #[test]
    fn or_combines_atoms() {
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(1))),
            CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(4))),
        ]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn in_list_matches() {
        let pred = CompoundPredicate::In(
            "id".into(),
            vec![ScalarValue::Int64(2), ScalarValue::Int64(5)],
        );
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(true), Some(false), Some(false), Some(true)]
        );
    }

    #[test]
    fn empty_in_is_false() {
        let pred = CompoundPredicate::In("id".into(), vec![]);
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false); 5]
        );
    }

    #[test]
    fn is_null_finds_nulls() {
        let pred = CompoundPredicate::IsNull("id".into());
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // Only index 2 (the None id) is null.
        assert_eq!(
            collect_mask(&mask),
            vec![Some(false), Some(false), Some(true), Some(false), Some(false)]
        );
        let pred = CompoundPredicate::IsNotNull("name".into());
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(true), Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn not_negates() {
        let pred = CompoundPredicate::Not(Box::new(CompoundPredicate::Atom(
            Predicate::Eq("id".into(), ScalarValue::Int64(2)),
        )));
        let mask = evaluate_compound(&small_batch(), &pred).unwrap();
        // id=2 is true → NOT = false. Others are false → NOT = true. The NULL
        // id evaluates Eq to false (per atom rules), so NOT(false) = true.
        assert_eq!(
            collect_mask(&mask),
            vec![Some(true), Some(false), Some(true), Some(true), Some(true)]
        );
    }

    fn stats(min: i64, max: i64, nulls: u64) -> BTreeMap<String, ColumnStats> {
        let mut m = BTreeMap::new();
        m.insert(
            "id".into(),
            ColumnStats {
                null_count: Some(nulls),
                min_bytes: Some(min.to_le_bytes().to_vec()),
                max_bytes: Some(max.to_le_bytes().to_vec()),
            },
        );
        m
    }

    fn id_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, true)])
    }

    #[test]
    fn pruning_eq_outside_range_is_no_match() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(42),
        ));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_eq_inside_range_is_mixed() {
        let stats = stats(0, 1000, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(42),
        ));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn pruning_and_short_circuits_no_match() {
        let stats = stats(100, 200, 0);
        // id = 42 prunes; combined with anything via AND prunes the whole.
        let pred = CompoundPredicate::And(vec![
            CompoundPredicate::Atom(Predicate::Eq(
                "id".into(),
                ScalarValue::Int64(42),
            )),
            CompoundPredicate::Atom(Predicate::Lt(
                "id".into(),
                ScalarValue::Int64(1_000_000),
            )),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_or_requires_all_branches_no_match() {
        let stats = stats(100, 200, 0);
        // 42 is outside range (NoMatch); 150 is inside (Mixed). OR result =
        // Mixed because one branch can't be ruled out.
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq(
                "id".into(),
                ScalarValue::Int64(42),
            )),
            CompoundPredicate::Atom(Predicate::Eq(
                "id".into(),
                ScalarValue::Int64(150),
            )),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );

        // Both outside: NoMatch.
        let pred = CompoundPredicate::Or(vec![
            CompoundPredicate::Atom(Predicate::Eq(
                "id".into(),
                ScalarValue::Int64(42),
            )),
            CompoundPredicate::Atom(Predicate::Eq(
                "id".into(),
                ScalarValue::Int64(999),
            )),
        ]);
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_is_null_with_zero_nulls_proves_no_match() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::IsNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
        let pred = CompoundPredicate::IsNotNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::AllMatch
        );
    }

    #[test]
    fn pruning_is_null_with_partial_nulls_is_mixed() {
        let stats = stats(100, 200, 5);
        let pred = CompoundPredicate::IsNull("id".into());
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::Mixed
        );
    }

    #[test]
    fn pruning_in_list_uses_or_rule() {
        let stats = stats(100, 200, 0);
        let pred = CompoundPredicate::In(
            "id".into(),
            vec![ScalarValue::Int64(1), ScalarValue::Int64(2)],
        );
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::NoMatch
        );
    }

    #[test]
    fn pruning_eq_singleton_with_no_nulls_is_all_match() {
        // min == max == v and zero nulls → every row matches.
        let stats = stats(42, 42, 0);
        let pred = CompoundPredicate::Atom(Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(42),
        ));
        assert_eq!(
            evaluate_compound_for_pruning(&pred, &stats, &id_schema(), 1000),
            PruneOutcome::AllMatch
        );
    }
}
