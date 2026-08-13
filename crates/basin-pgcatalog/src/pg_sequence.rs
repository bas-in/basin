//! `pg_catalog.pg_sequence` — a sequence's generation parameters (`START`,
//! `INCREMENT`, `MINVALUE`/`MAXVALUE`, `CACHE`, `CYCLE`). `\d <seqname>` and
//! `pg_dump`'s `CREATE SEQUENCE` reconstruction both read this relation, not
//! the sequence's current value (that is `pg_sequences`, the view, and
//! `nextval`/`currval`, neither of which this crate touches).
//!
//! # Where the column layout comes from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//!   WHERE attrelid = 'pg_catalog.pg_sequence'::regclass AND attnum > 0
//!   ORDER BY attnum;
//! --    attname    | atttypid | attnum | attnotnull
//! -- seqrelid      | oid      |      1 | t
//! -- seqtypid      | oid      |      2 | t
//! -- seqstart      | bigint   |      3 | t
//! -- seqincrement  | bigint   |      4 | t
//! -- seqmax        | bigint   |      5 | t
//! -- seqmin        | bigint   |      6 | t
//! -- seqcache      | bigint   |      7 | t
//! -- seqcycle      | boolean  |      8 | t
//! ```
//!
//! And the values, verified live against a fresh sequence:
//!
//! ```sql
//! CREATE SEQUENCE pgcat_test_seq START 5 INCREMENT 2 MINVALUE 1 MAXVALUE 1000
//!   CACHE 3 CYCLE;
//! SELECT seqrelid, seqtypid, seqstart, seqincrement, seqmax, seqmin, seqcache,
//!        seqcycle FROM pg_sequence WHERE seqrelid = 'pgcat_test_seq'::regclass;
//! -- seqrelid | seqtypid | seqstart | seqincrement | seqmax | seqmin | seqcache | seqcycle
//! -- 27614739 |       20 |        5 |            2 |   1000 |      1 |        3 | t
//! ```
//!
//! `seqtypid = 20` is `int8`/`bigint` — the default for a plain `CREATE
//! SEQUENCE`, matching [`SequenceInfo::type_oid`]'s own doc comment on why
//! this crate cannot yet distinguish a `SERIAL` column's owned sequence
//! (which would report its owning column's type instead).
//!
//! Every column here maps onto a [`SequenceInfo`] field one-for-one — see
//! that struct's doc comments for the couple of fields ([`SequenceInfo::
//! type_oid`]) with a known gap already documented at the source.
//!
//! # Status: [`CatalogSource::sequences`] returns nothing yet
//!
//! Per that method's own doc comment: sequence parameters live in Basin's
//! sequence machinery, not in table metadata, and no `CatalogSource`
//! implementation wires that lookup yet. This relation is therefore
//! correct and empty today — `SELECT * FROM pg_catalog.pg_sequence` returns
//! zero rows against every `CatalogSource` this crate currently has. It
//! does not fabricate a row to look non-empty.

use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, SequenceInfo},
    error::Error,
    predicate::{Predicate, Value},
};

/// This sequence's value for `column`, or `None` if `column` is not one of
/// this relation's columns.
fn value(seq: &SequenceInfo, column: &str) -> Option<Value> {
    Some(match column {
        "seqrelid" => Value::Oid(seq.oid),
        "seqtypid" => Value::Oid(seq.type_oid),
        "seqstart" => Value::Int(seq.start),
        "seqincrement" => Value::Int(seq.increment),
        "seqmax" => Value::Int(seq.max_value),
        "seqmin" => Value::Int(seq.min_value),
        "seqcache" => Value::Int(seq.cache_size),
        "seqcycle" => Value::Bool(seq.cycle),
        _ => return None,
    })
}

/// `pg_catalog.pg_sequence`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgSequence;

impl PgSequence {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("seqrelid", DataType::UInt32, false),
            Field::new("seqtypid", DataType::UInt32, false),
            Field::new("seqstart", DataType::Int64, false),
            Field::new("seqincrement", DataType::Int64, false),
            Field::new("seqmax", DataType::Int64, false),
            Field::new("seqmin", DataType::Int64, false),
            Field::new("seqcache", DataType::Int64, false),
            Field::new("seqcycle", DataType::Boolean, false),
        ]))
    }
}

impl crate::SystemView for PgSequence {
    fn name(&self) -> &str {
        "pg_sequence"
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
                    relation: "pg_sequence",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<SequenceInfo> = catalog
            .sequences()
            .into_iter()
            .filter(|s| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(s, p.column()).as_ref()))
            })
            .collect();

        let seqrelids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let seqtypids: UInt32Array = rows.iter().map(|r| r.type_oid.get()).collect();
        let seqstarts: Int64Array = rows.iter().map(|r| r.start).collect();
        let seqincrements: Int64Array = rows.iter().map(|r| r.increment).collect();
        let seqmaxes: Int64Array = rows.iter().map(|r| r.max_value).collect();
        let seqmins: Int64Array = rows.iter().map(|r| r.min_value).collect();
        let seqcaches: Int64Array = rows.iter().map(|r| r.cache_size).collect();
        let seqcycles: BooleanArray = rows.iter().map(|r| r.cycle).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(seqrelids),
                Arc::new(seqtypids),
                Arc::new(seqstarts),
                Arc::new(seqincrements),
                Arc::new(seqmaxes),
                Arc::new(seqmins),
                Arc::new(seqcaches),
                Arc::new(seqcycles),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{mock::MockCatalog, Oid, SystemView};

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_bool(batch: &RecordBatch, name: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|b| b.unwrap())
            .collect()
    }

    /// Mirrors the live `pgcat_test_seq` capture in the module docs, plus a
    /// second, non-cycling sequence to prove `seqrelid` scoping and
    /// `seqcycle = false`.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_sequence(SequenceInfo {
                oid: Oid(27614739),
                type_oid: basin_pgtype::oid::INT8,
                start: 5,
                increment: 2,
                max_value: 1000,
                min_value: 1,
                cache_size: 3,
                cycle: true,
            })
            .with_sequence(SequenceInfo {
                oid: Oid(27614800),
                type_oid: basin_pgtype::oid::INT8,
                start: 1,
                increment: 1,
                max_value: i64::MAX,
                min_value: 1,
                cache_size: 1,
                cycle: false,
            })
    }

    #[test]
    fn name_is_pg_sequence() {
        assert_eq!(PgSequence.name(), "pg_sequence");
    }

    /// The exact column layout live Postgres reports, in order.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgSequence.schema();
        let fields: Vec<(&str, &DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("seqrelid", &DataType::UInt32, false),
                ("seqtypid", &DataType::UInt32, false),
                ("seqstart", &DataType::Int64, false),
                ("seqincrement", &DataType::Int64, false),
                ("seqmax", &DataType::Int64, false),
                ("seqmin", &DataType::Int64, false),
                ("seqcache", &DataType::Int64, false),
                ("seqcycle", &DataType::Boolean, false),
            ]
        );
    }

    /// [`CatalogSource::sequences`] returns nothing today (see module docs),
    /// so this relation must too — not a fabricated row.
    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgSequence.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Every parameter round-trips exactly, matching the live
    /// `pgcat_test_seq` capture in the module docs.
    #[test]
    fn parameters_match_the_source_exactly() {
        let batch = PgSequence
            .scan(&catalog(), &[Predicate::eq("seqrelid", Oid(27614739))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            col_u32(&batch, "seqtypid"),
            vec![basin_pgtype::oid::INT8.get()]
        );
        assert_eq!(col_i64(&batch, "seqstart"), vec![5]);
        assert_eq!(col_i64(&batch, "seqincrement"), vec![2]);
        assert_eq!(col_i64(&batch, "seqmax"), vec![1000]);
        assert_eq!(col_i64(&batch, "seqmin"), vec![1]);
        assert_eq!(col_i64(&batch, "seqcache"), vec![3]);
        assert_eq!(col_bool(&batch, "seqcycle"), vec![true]);
    }

    /// The entire point of this crate: a pushed `seqrelid` predicate narrows
    /// to exactly one sequence, applied inside `scan` rather than after it.
    #[test]
    fn pushed_seqrelid_predicate_narrows_to_one_sequence() {
        let full = PgSequence.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 2);

        let filtered = PgSequence
            .scan(&catalog(), &[Predicate::eq("seqrelid", Oid(27614800))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_u32(&filtered, "seqrelid"), vec![27614800]);
        assert_eq!(col_bool(&filtered, "seqcycle"), vec![false]);
    }

    /// A predicate naming a column `pg_sequence` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgSequence
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_sequence",
                column: "nope".to_string(),
            }
        );
    }
}
