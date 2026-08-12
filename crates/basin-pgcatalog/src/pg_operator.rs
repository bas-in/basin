//! `pg_catalog.pg_operator`, a view over `basin-pgtype`'s own operator table.
//!
//! Like [`crate::pg_type`], this needs no [`CatalogSource`] at all: every
//! builtin operator Basin resolves lives in
//! `crates/basin-pgtype/src/operator.rs`'s `OPERATORS` table, already checked
//! against a live PostgreSQL 18 `pg_operator` (see that module's own docs for
//! the verifying query). This relation is that table, reshaped into
//! `pg_operator`'s column set.
//!
//! # Why this table, not a private map
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 ranks `pg_operator`
//! first among the missing relations, and not primarily for wire-protocol
//! compatibility: the owned planner must resolve `~`, `@>`, `=` and friends by
//! argument type against *some* table, replacing the textual rewriting in
//! `crates/basin-engine/src/pg_operators.rs`. `basin_pgtype::operator::OPERATORS`
//! already is that table. Making it queryable as `pg_catalog.pg_operator`
//! costs nothing beyond this file and delivers `\do` / driver introspection
//! for free — the same table serves both purposes at once.
//!
//! # Column set and where the values come from
//!
//! `oid`, `oprname`, `oprnamespace`, `oprleft`, `oprright`, `oprresult`,
//! `oprkind` — the columns `docs/migration/df-removal/11-pg-catalog-fidelity.md`
//! calls out as mattering for resolution and introspection. Every `(oid,
//! oprname, oprleft, oprright, oprresult)` tuple is inherited unchanged from
//! `OPERATORS`, whose own module docs record the live-database query used to
//! verify each row. `oprkind` is derived here: `'b'` when `OperatorSig::left`
//! is `Some` (a binary operator), `'l'` when it is `None` (a prefix operator,
//! Postgres's own `'l'` for "left unary"). Spot-checked live for both shapes:
//!
//! ```sql
//! SELECT oid, oprname, oprkind FROM pg_operator WHERE oid IN (96, 558);
//! --  96 | =       | b
//! -- 558 | -       | l
//! ```
//!
//! `oprnamespace` is always [`crate::PG_CATALOG_NAMESPACE`] — every operator
//! this crate knows about is a builtin.
//!
//! # `oid` is deduplicated to match a real primary key
//!
//! `pg_operator.oid` is a primary key in real Postgres (confirmed live: `"
//! pg_operator_oid_index" PRIMARY KEY, btree (oid)`), but `OPERATORS` itself
//! deliberately is **not** one row per oid — its own module docs explain that
//! the truly polymorphic array operators (`@>`, `<@`, `&&`, `||` on
//! `anyarray`/`anycompatiblearray`) are monomorphized into several rows
//! (`int4[] @> int4[]`, `text[] @> text[]`, ...) that legitimately share one
//! real oid, because that is the one oid Postgres itself resolves to
//! regardless of concrete element type. Reporting both monomorphizations as
//! separate `pg_operator` rows would violate the real table's primary key and
//! break the "a predicate on oid returns exactly one row" guarantee this
//! crate exists to provide. This relation therefore keeps only the *first*
//! `OPERATORS` row for each oid — the argument types of whichever
//! monomorphization happens to appear first in the source table — and drops
//! the rest. [`crate::operator`]-style resolution (not implemented in this
//! crate) is unaffected, since it consults the full `OPERATORS` table
//! directly, not this projection.
//!
//! # What is deliberately absent
//!
//! Everything `OPERATORS`' own module docs already say is absent: Postgres
//! ships roughly 800 builtin operators, `OPERATORS` covers comparison,
//! arithmetic, text, JSONB and a handful of array operators, and cross-type
//! rows (`int4 = int8`, real oid 416) are not tabulated — that pair resolves
//! by implicit widening onto the `int8 = int8` row (oid 410) instead, so this
//! relation's `pg_operator` will not contain a row for oid 416 at all. This is
//! a real, admitted gap versus a real server: a client that looks up
//! `pg_operator` by oid 416 will find nothing here, though `SELECT ... WHERE
//! int4col = int8col` still resolves correctly via the coercion path.

use std::{collections::HashSet, sync::Arc};

use arrow_array::{RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::operator::{OperatorSig, OPERATORS};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// This operator's value for `column`, or `None` if `column` is not one of
/// this relation's columns.
fn value(op: &OperatorSig, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(op.oid),
        "oprname" => Value::Text(op.name.to_string()),
        "oprnamespace" => Value::Oid(crate::PG_CATALOG_NAMESPACE),
        "oprleft" => Value::Oid(op.left.unwrap_or(basin_pgtype::Oid::INVALID)),
        "oprright" => Value::Oid(op.right),
        "oprresult" => Value::Oid(op.result),
        "oprkind" => Value::Text(if op.left.is_some() { "b" } else { "l" }.to_string()),
        _ => return None,
    })
}

/// `OPERATORS`, keeping only the first row for each oid — see the module docs
/// on why a real `pg_operator` cannot report the polymorphic array operators'
/// several monomorphizations as separate rows.
fn deduplicated_by_oid() -> Vec<&'static OperatorSig> {
    let mut seen = HashSet::new();
    OPERATORS.iter().filter(|op| seen.insert(op.oid)).collect()
}

/// `pg_catalog.pg_operator`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgOperator;

impl PgOperator {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("oprname", DataType::Utf8, false),
            Field::new("oprnamespace", DataType::UInt32, false),
            Field::new("oprleft", DataType::UInt32, false),
            Field::new("oprright", DataType::UInt32, false),
            Field::new("oprresult", DataType::UInt32, false),
            Field::new("oprkind", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgOperator {
    fn name(&self) -> &str {
        "pg_operator"
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
                    relation: "pg_operator",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&OperatorSig> = deduplicated_by_oid()
            .into_iter()
            .filter(|op| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(op, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let oprnames: StringArray = rows.iter().map(|r| Some(r.name)).collect();
        let oprnamespaces: UInt32Array = rows
            .iter()
            .map(|_| crate::PG_CATALOG_NAMESPACE.get())
            .collect();
        let oprlefts: UInt32Array = rows
            .iter()
            .map(|r| r.left.unwrap_or(basin_pgtype::Oid::INVALID).get())
            .collect();
        let oprrights: UInt32Array = rows.iter().map(|r| r.right.get()).collect();
        let oprresults: UInt32Array = rows.iter().map(|r| r.result.get()).collect();
        let oprkinds: StringArray = rows
            .iter()
            .map(|r| Some(if r.left.is_some() { "b" } else { "l" }))
            .collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(oprnames),
                Arc::new(oprnamespaces),
                Arc::new(oprlefts),
                Arc::new(oprrights),
                Arc::new(oprresults),
                Arc::new(oprkinds),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

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

    fn row_for(batch: &RecordBatch, oid: u32) -> usize {
        col_u32(batch, "oid")
            .into_iter()
            .position(|o| o == oid)
            .unwrap_or_else(|| panic!("no pg_operator row for oid {oid}"))
    }

    #[test]
    fn name_is_pg_operator() {
        assert_eq!(PgOperator.name(), "pg_operator");
    }

    #[test]
    fn schema_matches_the_documented_column_set() {
        let schema = PgOperator.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "oprname",
                "oprnamespace",
                "oprleft",
                "oprright",
                "oprresult",
                "oprkind",
            ]
        );
    }

    /// `int4 = int4` (oid 96) is a binary operator: `oprkind = 'b'`, real
    /// `oprleft`/`oprright` both `int4` (23), `oprresult` bool (16).
    /// Confirmed live against PostgreSQL 18.
    #[test]
    fn binary_operator_reports_oprkind_b_and_both_operand_types() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 96);
        assert_eq!(col_str(&batch, "oprname")[i], "=");
        assert_eq!(col_str(&batch, "oprkind")[i], "b");
        assert_eq!(col_u32(&batch, "oprleft")[i], 23);
        assert_eq!(col_u32(&batch, "oprright")[i], 23);
        assert_eq!(col_u32(&batch, "oprresult")[i], 16);
    }

    /// Unary minus (oid 558) is a prefix operator: `oprkind = 'l'`, and real
    /// Postgres reports `oprleft = 0` (no left operand at all), not some
    /// placeholder type. Confirmed live.
    #[test]
    fn prefix_operator_reports_oprkind_l_and_zero_oprleft() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 558);
        assert_eq!(col_str(&batch, "oprname")[i], "-");
        assert_eq!(col_str(&batch, "oprkind")[i], "l");
        assert_eq!(col_u32(&batch, "oprleft")[i], 0);
        assert_eq!(col_u32(&batch, "oprright")[i], 23);
    }

    /// The entire point of this crate: a predicate on `oid` must actually
    /// narrow the row set to exactly one row, mirroring `pg_operator.oid`
    /// being a real primary key.
    #[test]
    fn pushed_oid_predicate_narrows_to_exactly_one_row() {
        let full = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(
            full.num_rows() > 1,
            "sanity: pg_operator has more than one row"
        );

        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::eq("oid", basin_pgtype::Oid(96))],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "oprname"), vec!["=".to_string()]);
    }

    /// The polymorphic array operators (`@>` on `anyarray`) share one real
    /// oid across their monomorphized instantiations in `OPERATORS`; this
    /// relation must report that oid exactly once, not once per
    /// instantiation, since `pg_operator.oid` is a primary key in real
    /// Postgres.
    #[test]
    fn polymorphic_array_operator_oid_appears_exactly_once() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let count = col_u32(&batch, "oid")
            .into_iter()
            .filter(|&o| o == 2751)
            .count();
        assert_eq!(
            count, 1,
            "oid 2751 (@> on anyarray) must be deduplicated to one row"
        );
    }

    /// Every row lives in `pg_catalog` (namespace 11), confirmed live.
    #[test]
    fn every_row_is_in_pg_catalog_namespace() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        for ns in col_u32(&batch, "oprnamespace") {
            assert_eq!(ns, 11);
        }
    }

    /// A predicate matching nothing returns zero rows, not everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::eq("oid", basin_pgtype::Oid(999_999))],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column this relation does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgOperator
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_operator",
                column: "nope".to_string(),
            }
        );
    }

    /// `IN` pushdown on `oprname` narrows to exactly the named operators.
    #[test]
    fn pushed_in_predicate_narrows_by_name() {
        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::in_list(
                    "oprname",
                    [Value::Text("~".to_string()), Value::Text("~*".to_string())],
                )],
            )
            .unwrap();
        let mut names = col_str(&filtered, "oprname");
        names.sort();
        assert_eq!(names, vec!["~".to_string(), "~*".to_string()]);
    }
}
