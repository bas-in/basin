//! `pg_catalog.pg_enum` — the ordered label set of every `CREATE TYPE ... AS
//! ENUM` type, the table `enum_range()`/`\dT+` and every ORM's enum
//! introspection (Prisma, Drizzle, SQLAlchemy) reads to reconstruct the
//! allowed values.
//!
//! # Where the column layout comes from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//!   WHERE attrelid = 'pg_catalog.pg_enum'::regclass AND attnum > 0
//!   ORDER BY attnum;
//! --    attname    | atttypid | attnum | attnotnull
//! -- oid           | oid      |      1 | t
//! -- enumtypid     | oid      |      2 | t
//! -- enumsortorder | real     |      3 | t
//! -- enumlabel     | name     |      4 | t
//! ```
//!
//! And the label ordering, verified live against a fresh enum type:
//!
//! ```sql
//! CREATE TYPE pgcat_test_mood AS ENUM ('sad', 'ok', 'happy');
//! SELECT oid, enumtypid, enumsortorder, enumlabel FROM pg_enum
//!   WHERE enumtypid = 'pgcat_test_mood'::regtype ORDER BY enumsortorder;
//! --    oid    | enumtypid | enumsortorder | enumlabel
//! -- 27614734  | 27614732  |             1 | sad
//! -- 27614736  | 27614732  |             2 | ok
//! -- 27614738  | 27614732  |             3 | happy
//! ```
//!
//! Confirming what [`crate::catalog_source::EnumTypeInfo::labels`]'s own doc
//! comment already says: `enumsortorder` starts at **1**, not `0` — the
//! label at `labels[0]` gets `enumsortorder = 1`, `labels[1]` gets `2`, and
//! so on.
//!
//! # `enumsortorder` is `real` (`float4`), and cannot be pushed
//!
//! [`crate::predicate::Value`] has no floating-point variant — nothing in
//! this crate can construct a `Predicate` naming `enumsortorder`, so
//! [`row_value`] reports `None` for it the same way [`crate::pg_index`]
//! reports `None` for its list-typed `indkey`: the column is real and is
//! still built into every row of the returned batch, it simply cannot be a
//! pushdown target with today's `Value` enum.
//!
//! # `oid` is always `0` — a documented gap, not a bug
//!
//! Real Postgres's `pg_enum.oid` is the row's own independently-allocated
//! catalog oid (confirmed live above: `27614734`, `27614736`, ... — not
//! derived from `enumtypid` or `enumsortorder`). [`EnumTypeInfo`] carries no
//! such per-label identity — Basin addresses an enum type by `(ProjectId,
//! name)`, not by an integer id, and does not allocate one for individual
//! labels either (see [`EnumTypeInfo`]'s own module docs). Rather than
//! fabricate an oid with no ground truth, this relation reports `0` for
//! every row, the same documented-gap precedent [`crate::pg_cast::PgCast`]
//! set for `castfunc` when [`crate::catalog_source::CatalogSource`] cannot
//! resolve a real function oid either.
//!
//! # Status: [`CatalogSource::enum_types`] returns nothing yet
//!
//! [`CatalogSource::enum_types`]'s own doc comment explains why: an Arrow
//! schema records a column's physical type, not that it was declared as an
//! enum, and no `CatalogSource` implementation wires that lookup yet. This
//! relation is therefore correct and empty today — `SELECT * FROM
//! pg_catalog.pg_enum` returns zero rows against every `CatalogSource` this
//! crate currently has, exactly like real Postgres would for a project with
//! no enum types declared. It does not fabricate a row to look non-empty.

use std::sync::Arc;

use arrow_array::{Float32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, EnumTypeInfo},
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// One resolved `pg_enum` row: an enum type's oid paired with one of its
/// labels and that label's 1-based sort order.
struct EnumLabelRow {
    enumtypid: Oid,
    enumsortorder: f32,
    enumlabel: String,
}

impl EnumLabelRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns, or is `enumsortorder` (no `Value` variant
    /// represents `real` — see the module docs).
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            // Documented gap: always 0. See the module docs.
            "oid" => Value::Oid(Oid::INVALID),
            "enumtypid" => Value::Oid(self.enumtypid),
            "enumlabel" => Value::Text(self.enumlabel.clone()),
            _ => return None,
        })
    }
}

/// Flatten one [`EnumTypeInfo`] into its ordered `pg_enum` rows,
/// `enumsortorder` starting at `1` per the module docs.
fn rows_for(enum_type: &EnumTypeInfo) -> impl Iterator<Item = EnumLabelRow> + '_ {
    enum_type
        .labels
        .iter()
        .enumerate()
        .map(move |(i, label)| EnumLabelRow {
            enumtypid: enum_type.oid,
            enumsortorder: (i + 1) as f32,
            enumlabel: label.clone(),
        })
}

/// `pg_catalog.pg_enum`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgEnum;

impl PgEnum {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("enumtypid", DataType::UInt32, false),
            Field::new("enumsortorder", DataType::Float32, false),
            Field::new("enumlabel", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgEnum {
    fn name(&self) -> &str {
        "pg_enum"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_enum",
                    column: p.column().to_string(),
                });
            }
        }

        let enum_types = catalog.enum_types();
        let rows: Vec<EnumLabelRow> = enum_types
            .iter()
            .flat_map(rows_for)
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let enumtypids: UInt32Array = rows.iter().map(|r| r.enumtypid.get()).collect();
        let enumsortorders: Float32Array = rows.iter().map(|r| r.enumsortorder).collect();
        let enumlabels: StringArray = rows.iter().map(|r| Some(r.enumlabel.clone())).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(enumtypids),
                Arc::new(enumsortorders),
                Arc::new(enumlabels),
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

    fn col_f32(batch: &RecordBatch, name: &str) -> Vec<f32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Float32Array>()
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

    /// Mirrors the live `pgcat_test_mood` capture in the module docs: one
    /// enum type with three labels, plus a second enum type to prove
    /// `enumtypid` scoping.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_enum_type(EnumTypeInfo {
                oid: Oid(27614732),
                namespace: Oid(2200),
                name: "pgcat_test_mood".to_string(),
                labels: vec!["sad".to_string(), "ok".to_string(), "happy".to_string()],
            })
            .with_enum_type(EnumTypeInfo {
                oid: Oid(27614999),
                namespace: Oid(2200),
                name: "pgcat_test_size".to_string(),
                labels: vec!["small".to_string(), "large".to_string()],
            })
    }

    #[test]
    fn name_is_pg_enum() {
        assert_eq!(PgEnum.name(), "pg_enum");
    }

    /// The exact column layout live Postgres reports, in order.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgEnum.schema();
        let fields: Vec<(&str, &DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("oid", &DataType::UInt32, false),
                ("enumtypid", &DataType::UInt32, false),
                ("enumsortorder", &DataType::Float32, false),
                ("enumlabel", &DataType::Utf8, false),
            ]
        );
    }

    /// [`CatalogSource::enum_types`] returns nothing today (see module
    /// docs), so this relation must too — not a fabricated row.
    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgEnum.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Labels come back in declaration order with `enumsortorder` starting
    /// at 1, matching the live `pgcat_test_mood` capture exactly.
    #[test]
    fn labels_are_ordered_starting_at_one() {
        let batch = PgEnum
            .scan(&catalog(), &[Predicate::eq("enumtypid", Oid(27614732))])
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(col_f32(&batch, "enumsortorder"), vec![1.0, 2.0, 3.0]);
        assert_eq!(
            col_str(&batch, "enumlabel"),
            vec!["sad".to_string(), "ok".to_string(), "happy".to_string()]
        );
    }

    /// `oid` is always 0 — the documented gap, not a fabricated identity.
    #[test]
    fn oid_is_always_zero() {
        let batch = PgEnum.scan(&catalog(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 5, "sanity: 3 + 2 labels across two types");
        for oid in col_u32(&batch, "oid") {
            assert_eq!(oid, 0);
        }
    }

    /// The entire point of this crate: a pushed `enumtypid` predicate
    /// narrows to exactly one enum type's labels, applied inside `scan`
    /// rather than after it.
    #[test]
    fn pushed_enumtypid_predicate_narrows_to_one_types_labels() {
        let full = PgEnum.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 5);

        let filtered = PgEnum
            .scan(&catalog(), &[Predicate::eq("enumtypid", Oid(27614999))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        for enumtypid in col_u32(&filtered, "enumtypid") {
            assert_eq!(enumtypid, 27614999);
        }
        assert_eq!(
            col_str(&filtered, "enumlabel"),
            vec!["small".to_string(), "large".to_string()]
        );
    }

    /// A predicate naming a column `pg_enum` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgEnum
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_enum",
                column: "nope".to_string(),
            }
        );
    }
}
