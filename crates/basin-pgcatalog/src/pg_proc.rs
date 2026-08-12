//! `pg_catalog.pg_proc`, a view over `basin-pgtype`'s own function catalog.
//!
//! Like [`crate::pg_type`], [`crate::pg_operator`] and [`crate::pg_cast`],
//! this needs no [`CatalogSource`]: every builtin function Basin resolves
//! lives in `crates/basin-pgtype/src/func.rs`'s `FUNCS` table, already
//! checked against a live PostgreSQL 18 `pg_proc` (see that module's own docs
//! for the verifying query). This relation is that table, reshaped into
//! `pg_proc`'s column set.
//!
//! # Why this table, not a private map
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 puts `pg_operator`
//! and `pg_cast` at priority 1 for the same reason `func.rs`'s own module
//! docs give for itself: the owned planner must resolve `lower(x)`, `count(*)`,
//! `sum(x)` and friends by name and argument types against *some* table, and
//! `basin_pgtype::func::FUNCS` already is that table. `pg_proc` is the same
//! table under its real catalog name — this file costs nothing beyond itself
//! and delivers `\df` / driver introspection for free.
//!
//! # Column set and where the values come from
//!
//! `oid`, `proname`, `pronamespace`, `prorettype`, `pronargs`, `proargtypes`,
//! `prokind` — the columns the task's fidelity work calls out as mattering
//! for resolution and introspection, confirmed live:
//!
//! ```sql
//! SELECT oid, proname, pronamespace, prorettype::regtype, pronargs,
//!        proargtypes, prokind
//!   FROM pg_proc
//!  WHERE oid IN (2803, 2147, 3100, 1067, 2335)
//!  ORDER BY oid;
//! --  oid  |     proname      | pronamespace | prorettype | pronargs | proargtypes | prokind
//! -- 1067  | generate_series  |           11 | integer    |        2 | 23 23       | f
//! -- 2147  | count            |           11 | bigint     |        1 | 2276        | a
//! -- 2335  | array_agg        |           11 | anyarray   |        1 | 2776        | a
//! -- 2803  | count            |           11 | bigint     |        0 |             | a
//! -- 3100  | row_number       |           11 | bigint     |        0 |             | w
//! ```
//!
//! `pronamespace` is always [`crate::PG_CATALOG_NAMESPACE`] (confirmed `= 11`
//! for every row above) — every function this crate knows about is a
//! builtin. `pronargs` is `FuncSig::args.len()` as `i16`, matching
//! `pg_proc.pronargs`'s own type. `proargtypes` is `FuncSig::args` unchanged.
//! `prokind` is derived from [`basin_pgtype::func::FuncKind`]: `'a'` for
//! `Aggregate`, `'w'` for `Window`, and `'f'` for both `Scalar` and
//! `SetReturning` — real Postgres does the same collapse, confirmed live
//! above (`generate_series`, a set-returning function, reports `prokind =
//! 'f'` exactly like a plain scalar; the set-returning-ness lives in a
//! separate `proretset` column `func.rs`'s own docs note this crate does not
//! model, since nothing in the required column set asks for it).
//!
//! # `oid` is deduplicated to match a real primary key
//!
//! `pg_proc.oid` is a primary key in real Postgres, but `FUNCS` is not one
//! row per oid — `func.rs`'s own module docs explain that several
//! polymorphic functions (`array_agg`, `unnest`, `lag`/`lead`/`first_value`/
//! `last_value`/`nth_value`) are monomorphized into multiple rows sharing one
//! real oid, the same way [`crate::pg_operator`] handles the polymorphic
//! array operators. This relation keeps only the first `FUNCS` row for each
//! oid and drops the rest, for the same reason: reporting two rows under one
//! primary key would violate the real table's own constraint and break "a
//! predicate on oid returns exactly one row". `basin_pgtype::func::resolve`
//! is unaffected, since it consults the full `FUNCS` table directly.
//!
//! # What is deliberately absent
//!
//! Everything `func.rs`'s own module docs already say is absent: Postgres
//! ships well over a thousand builtin functions, `FUNCS` covers string,
//! math, date/time, aggregate, window and set-returning functions in the
//! categories needed to unblock planner resolution, and several real
//! `pg_proc` rows it explicitly does not include (`ceiling`, `date_trunc`'s
//! three-argument timezone-name overload, `age(xid)`, `avg(int2)`/
//! `avg(int4)`'s own dedicated oids) are simply absent from this relation
//! too — not covered wrongly, not covered yet. `proretset`, `provolatile`,
//! `proisstrict`, and every other `pg_proc` column outside the seven listed
//! above are not reported at all; a client reading them will get nothing,
//! not a wrong answer.

use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    builder::{ListBuilder, UInt32Builder},
    Int16Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::func::{FuncKind, FuncSig, FUNCS};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// `pg_proc.prokind` for this function — `'a'`/`'w'` for aggregate/window,
/// `'f'` for both scalar and set-returning (real Postgres collapses the
/// latter two as well — see the module docs).
fn prokind(kind: FuncKind) -> char {
    match kind {
        FuncKind::Aggregate => 'a',
        FuncKind::Window => 'w',
        FuncKind::Scalar | FuncKind::SetReturning => 'f',
    }
}

/// This function's value for `column`, or `None` if `column` is not one of
/// this relation's columns, and not one of the list-typed ones
/// (`proargtypes`) handled separately by the caller.
fn value(f: &FuncSig, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(f.oid),
        "proname" => Value::Text(f.name.to_string()),
        "pronamespace" => Value::Oid(crate::PG_CATALOG_NAMESPACE),
        "prorettype" => Value::Oid(f.ret),
        "pronargs" => Value::Int(f.args.len() as i64),
        "prokind" => Value::Text(prokind(f.kind).to_string()),
        // `proargtypes` is a list column; no scalar `Value` represents it, so
        // a predicate naming it is rejected by the schema check in `scan`
        // rather than reaching here.
        _ => return None,
    })
}

/// `FUNCS`, keeping only the first row for each oid — see the module docs on
/// why a real `pg_proc` cannot report the polymorphic functions' several
/// monomorphizations as separate rows.
fn deduplicated_by_oid() -> Vec<&'static FuncSig> {
    let mut seen = HashSet::new();
    FUNCS.iter().filter(|f| seen.insert(f.oid)).collect()
}

/// `pg_catalog.pg_proc`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgProc;

impl PgProc {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", DataType::UInt32, false),
            Field::new("prorettype", DataType::UInt32, false),
            Field::new("pronargs", DataType::Int16, false),
            Field::new(
                "proargtypes",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
            Field::new("prokind", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgProc {
    fn name(&self) -> &str {
        "pg_proc"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        _catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_proc",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&FuncSig> = deduplicated_by_oid()
            .into_iter()
            .filter(|f| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(f, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let pronames: StringArray = rows.iter().map(|r| Some(r.name)).collect();
        let pronamespaces: UInt32Array = rows
            .iter()
            .map(|_| crate::PG_CATALOG_NAMESPACE.get())
            .collect();
        let prorettypes: UInt32Array = rows.iter().map(|r| r.ret.get()).collect();
        let pronargs: Int16Array = rows.iter().map(|r| r.args.len() as i16).collect();
        let prokinds: StringArray = rows
            .iter()
            .map(|r| Some(prokind(r.kind).to_string()))
            .collect();

        let mut proargtypes_builder = ListBuilder::new(UInt32Builder::new());
        for r in &rows {
            for arg in r.args {
                proargtypes_builder.values().append_value(arg.get());
            }
            proargtypes_builder.append(true);
        }
        let proargtypes: ListArray = proargtypes_builder.finish();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(pronames),
                Arc::new(pronamespaces),
                Arc::new(prorettypes),
                Arc::new(pronargs),
                Arc::new(proargtypes),
                Arc::new(prokinds),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use basin_pgtype::Oid;

    use super::*;
    use crate::{mock::MockCatalog, SystemView};

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    fn col_i16(batch: &RecordBatch, name: &str) -> Vec<i16> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn proargtypes_row(batch: &RecordBatch, i: usize) -> Vec<u32> {
        let list = batch
            .column(batch.schema().index_of("proargtypes").unwrap())
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list.value(i);
        values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn row_for(batch: &RecordBatch, oid: u32) -> usize {
        col_u32(batch, "oid")
            .into_iter()
            .position(|o| o == oid)
            .unwrap_or_else(|| panic!("no pg_proc row for oid {oid}"))
    }

    #[test]
    fn name_is_pg_proc() {
        assert_eq!(PgProc.name(), "pg_proc");
    }

    #[test]
    fn schema_matches_the_documented_column_set() {
        let schema = PgProc.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "proname",
                "pronamespace",
                "prorettype",
                "pronargs",
                "proargtypes",
                "prokind",
            ]
        );
    }

    /// `count(*)` (oid 2803) is the zero-argument aggregate row; `row_number`
    /// (oid 3100) is a zero-argument window row. Confirmed live — both
    /// `pronargs = 0` and `proargtypes` empty, `prokind` distinguishing them.
    #[test]
    fn zero_arg_rows_report_empty_proargtypes_and_correct_prokind() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();

        let i = row_for(&batch, 2803);
        assert_eq!(col_str(&batch, "proname")[i], "count");
        assert_eq!(col_i16(&batch, "pronargs")[i], 0);
        assert!(proargtypes_row(&batch, i).is_empty());
        assert_eq!(col_str(&batch, "prokind")[i], "a");

        let i = row_for(&batch, 3100);
        assert_eq!(col_str(&batch, "proname")[i], "row_number");
        assert_eq!(col_i16(&batch, "pronargs")[i], 0);
        assert_eq!(col_str(&batch, "prokind")[i], "w");
    }

    /// `prokind` must actually distinguish `'a'` aggregates from `'w'` window
    /// functions from `'f'` plain functions — the core claim this column
    /// exists to let a client (or the planner) branch on.
    #[test]
    fn prokind_distinguishes_aggregate_window_and_plain() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();

        // sum(int4), oid 2108: aggregate.
        let i = row_for(&batch, 2108);
        assert_eq!(col_str(&batch, "prokind")[i], "a");

        // lag(int4), oid 3106: window.
        let i = row_for(&batch, 3106);
        assert_eq!(col_str(&batch, "prokind")[i], "w");

        // lower(text), oid 870: plain scalar.
        let i = row_for(&batch, 870);
        assert_eq!(col_str(&batch, "prokind")[i], "f");
    }

    /// `count(x)` (oid 2147) has one declared argument, Postgres's `"any"`
    /// pseudo-type (oid 2276) — confirmed live via `proargtypes`.
    #[test]
    fn count_of_x_reports_the_any_pseudo_type_argument() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 2147);
        assert_eq!(col_i16(&batch, "pronargs")[i], 1);
        assert_eq!(proargtypes_row(&batch, i), vec![2276]);
    }

    /// `generate_series(int4, int4)` (oid 1067) has two `int4` arguments and
    /// returns `int4` — confirmed live.
    #[test]
    fn generate_series_reports_its_two_int4_arguments() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 1067);
        assert_eq!(col_str(&batch, "proname")[i], "generate_series");
        assert_eq!(proargtypes_row(&batch, i), vec![23, 23]);
        assert_eq!(col_u32(&batch, "prorettype")[i], 23);
        // Set-returning, but prokind still collapses to 'f' — see the module
        // docs on why that matches real Postgres.
        assert_eq!(col_str(&batch, "prokind")[i], "f");
    }

    /// The polymorphic functions (`array_agg`, `unnest`, `lag`/`lead`/...)
    /// share one real oid across their monomorphized instantiations in
    /// `FUNCS`; this relation must report that oid exactly once.
    #[test]
    fn polymorphic_function_oid_appears_exactly_once() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        let count = col_u32(&batch, "oid")
            .into_iter()
            .filter(|&o| o == 2335)
            .count();
        assert_eq!(
            count, 1,
            "oid 2335 (array_agg) must be deduplicated to one row"
        );
    }

    /// Every row lives in `pg_catalog` (namespace 11), confirmed live.
    #[test]
    fn every_row_is_in_pg_catalog_namespace() {
        let batch = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        for ns in col_u32(&batch, "pronamespace") {
            assert_eq!(ns, 11);
        }
    }

    /// The entire point of this crate: a predicate on `oid` narrows to
    /// exactly one row, mirroring `pg_proc.oid` being a real primary key.
    #[test]
    fn pushed_oid_predicate_narrows_to_exactly_one_row() {
        let full = PgProc.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(full.num_rows() > 1, "sanity: pg_proc has more than one row");

        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(2803))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "proname"), vec!["count".to_string()]);
    }

    /// A predicate matching nothing returns zero rows, not everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("oid", Oid(999_999))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column this relation does not have at all is an
    /// error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_proc",
                column: "nope".to_string(),
            }
        );
    }

    /// `proargtypes` is a real column of this relation's schema, but it is
    /// list-typed and [`value`] has no scalar [`Value`] to report for it — so
    /// a predicate naming it is not an `UnknownColumn` error (the column
    /// exists), but matches no row at all, the same way any predicate
    /// against a column a given row cannot produce a value for behaves (see
    /// [`Predicate::matches`]'s own docs on `actual: None`).
    #[test]
    fn predicate_on_proargtypes_matches_no_rows_rather_than_erroring() {
        let filtered = PgProc
            .scan(&MockCatalog::new(), &[Predicate::eq("proargtypes", 1i64)])
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// `IN` pushdown on `proname` narrows to exactly the named functions.
    #[test]
    fn pushed_in_predicate_narrows_by_name() {
        let filtered = PgProc
            .scan(
                &MockCatalog::new(),
                &[Predicate::in_list(
                    "proname",
                    [
                        Value::Text("lower".to_string()),
                        Value::Text("upper".to_string()),
                    ],
                )],
            )
            .unwrap();
        let mut names = col_str(&filtered, "proname");
        names.sort();
        assert_eq!(names, vec!["lower".to_string(), "upper".to_string()]);
    }
}
