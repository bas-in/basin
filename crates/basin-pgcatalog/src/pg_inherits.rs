//! `pg_catalog.pg_inherits` — for each child relation, its inheritance or
//! partition parent(s).
//!
//! `\d` on a partitioned table (`"Partition of: ..."` / `"Partitions:
//! ..."`) and on a classically-inherited table (`"Inherits: ..."` /
//! `"Number of child tables: ..."`) both read this relation, and so does
//! `pg_dump` when deciding whether to emit `INHERITS (...)`/`PARTITION OF
//! ...` for a table it is dumping. Without it, a client that has read
//! [`crate::pg_class`] sees every table but no hierarchy between any of
//! them.
//!
//! # Where these values come from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory — this
//! project has repeatedly found recall wrong and the server right (see
//! `crates/basin-pgtype/src/operator.rs`'s module docs for the precedent).
//! Column layout:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//!   WHERE attrelid = 'pg_catalog.pg_inherits'::regclass AND attnum > 0
//!   ORDER BY attnum;
//! --      attname      | atttypid | attnum | attnotnull
//! -- ------------------+----------+--------+------------
//! --  inhrelid         | oid      |      1 | t
//! --  inhparent        | oid      |      2 | t
//! --  inhseqno         | integer  |      3 | t
//! --  inhdetachpending | boolean  |      4 | t
//! ```
//!
//! Only 4 columns — no gap here for a column to land in the wrong `attnum`
//! position, unlike several sibling relations in this crate (see e.g.
//! [`crate::pg_index`]'s module docs).
//!
//! And real rows, from both a classical `INHERITS` and a partitioned table:
//!
//! ```sql
//! CREATE TABLE inh_parent (id int);
//! CREATE TABLE inh_child () INHERITS (inh_parent);
//! CREATE TABLE part_parent (id int) PARTITION BY RANGE (id);
//! CREATE TABLE part_child PARTITION OF part_parent FOR VALUES FROM (0) TO (10);
//! SELECT * FROM pg_inherits;
//! --  inhrelid | inhparent | inhseqno | inhdetachpending
//! -- ----------+-----------+----------+------------------
//! --  27614922 |  27614919 |        1 | f      -- inh_child  -> inh_parent
//! --  27614928 |  27614925 |        1 | f      -- part_child -> part_parent
//! ```
//!
//! # `pg_inherits` cannot tell `INHERITS` and partitioning apart
//!
//! Both rows above have the identical shape — same four columns, same kinds
//! of values. Nothing in `pg_inherits` itself says "this is a partition" vs
//! "this is a classical inheritance parent". The only way to tell them apart
//! is a join back to `pg_class.relkind`: a *partitioned* parent has
//! `relkind = 'p'` (confirmed live: `part_parent` reports `p`, `inh_parent`
//! reports the ordinary-table `r`, and so does `part_child` itself — a
//! partition is a perfectly ordinary `r` relation from `pg_class`'s point of
//! view, only `pg_inherits`/`pg_partitioned_table` mark it as one). This
//! relation reports exactly what real Postgres reports — both cases,
//! undistinguished — and leans on [`crate::pg_class`] for the distinguishing
//! join, the same way real `psql` does.
//!
//! # What [`crate::catalog_source::InheritanceInfo`] cannot yet supply
//!
//! `inhdetachpending` — `true` only for a partition mid-way through `ALTER
//! TABLE ... DETACH PARTITION ... CONCURRENTLY`, which `InheritanceInfo` has
//! no notion of. Live-verified `false` for both a plain `INHERITS` child and
//! a freshly-attached partition (both rows above report `f`). Reported as a
//! placeholder `false` for every row — the real value for every hierarchy
//! this crate can currently construct, not a guess, the same reasoning
//! [`crate::pg_index`] and [`crate::pg_constraint`] use for their own
//! boolean placeholders.

use std::sync::Arc;

use arrow_array::{BooleanArray, Int32Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, InheritanceInfo},
    error::Error,
    predicate::{Predicate, Value},
};

/// This row's value for `column`, or `None` if `column` is not one of this
/// relation's columns.
fn value(row: &InheritanceInfo, column: &str) -> Option<Value> {
    Some(match column {
        "inhrelid" => Value::Oid(row.child_oid),
        "inhparent" => Value::Oid(row.parent_oid),
        "inhseqno" => Value::Int(i64::from(row.seqno)),
        // `InheritanceInfo` has no notion of an in-progress `DETACH
        // PARTITION ... CONCURRENTLY` — see module docs. Live-verified
        // `false` for every hierarchy this crate can construct.
        "inhdetachpending" => Value::Bool(false),
        _ => return None,
    })
}

/// `pg_catalog.pg_inherits`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgInherits;

impl PgInherits {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("inhrelid", DataType::UInt32, false),
            Field::new("inhparent", DataType::UInt32, false),
            Field::new("inhseqno", DataType::Int32, false),
            Field::new("inhdetachpending", DataType::Boolean, false),
        ]))
    }
}

impl crate::SystemView for PgInherits {
    fn name(&self) -> &str {
        "pg_inherits"
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
                    relation: "pg_inherits",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<InheritanceInfo> = catalog
            .tables()
            .iter()
            .flat_map(|t| catalog.inheritance(t.oid))
            .filter(|row| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(row, p.column()).as_ref()))
            })
            .collect();

        let inhrelids: UInt32Array = rows.iter().map(|r| r.child_oid.get()).collect();
        let inhparents: UInt32Array = rows.iter().map(|r| r.parent_oid.get()).collect();
        let inhseqnos: Int32Array = rows.iter().map(|r| r.seqno).collect();
        // Placeholder — see module docs for why `false` is the real value
        // for every hierarchy this crate can currently construct, not a
        // guess.
        let inhdetachpendings: BooleanArray = rows.iter().map(|_| false).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(inhrelids),
                Arc::new(inhparents),
                Arc::new(inhseqnos),
                Arc::new(inhdetachpendings),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{
        catalog_source::{RelKind, TableInfo},
        mock::MockCatalog,
        Oid, SystemView,
    };

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_i32(batch: &RecordBatch, name: &str) -> Vec<i32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn table(oid: Oid, name: &str) -> TableInfo {
        TableInfo {
            oid,
            name: name.to_string(),
            namespace: Oid(16384),
            owner: Oid(10),
            kind: RelKind::OrdinaryTable,
        }
    }

    /// `inh_parent`/`inh_child` mirror the classical-`INHERITS` case;
    /// `multi_child` inherits from two parents (multiple inheritance, only
    /// possible for classical `INHERITS`, not partitioning) to exercise
    /// `inhseqno` ordering; `standalone` has no parent at all.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table(table(Oid(16385), "inh_parent"))
            .with_table(table(Oid(16390), "inh_child"))
            .with_table(table(Oid(16395), "parent_a"))
            .with_table(table(Oid(16396), "parent_b"))
            .with_table(table(Oid(16400), "multi_child"))
            .with_table(table(Oid(16405), "standalone"))
            .with_inheritance(InheritanceInfo {
                child_oid: Oid(16390),
                parent_oid: Oid(16385),
                seqno: 1,
            })
            .with_inheritance(InheritanceInfo {
                child_oid: Oid(16400),
                parent_oid: Oid(16395),
                seqno: 1,
            })
            .with_inheritance(InheritanceInfo {
                child_oid: Oid(16400),
                parent_oid: Oid(16396),
                seqno: 2,
            })
    }

    #[test]
    fn name_is_pg_inherits() {
        assert_eq!(PgInherits.name(), "pg_inherits");
    }

    /// Pins the exact column layout (name, order, nullability) against live
    /// PostgreSQL 18.2's `pg_attribute` for `pg_inherits`.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgInherits.schema();
        let got: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("inhrelid", false),
                ("inhparent", false),
                ("inhseqno", false),
                ("inhdetachpending", false),
            ]
        );
    }

    /// The entire point of this crate, applied to `pg_inherits`: a predicate
    /// on `inhrelid` must narrow to exactly one child's parent rows, applied
    /// *inside* `scan` (proven by asserting the returned batch itself, not
    /// by post-filtering it).
    #[test]
    fn pushed_inhrelid_predicate_narrows_to_one_childs_parents() {
        let full = PgInherits.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 3, "sanity: 1 + 2 inheritance rows total");

        let filtered = PgInherits
            .scan(&catalog(), &[Predicate::eq("inhrelid", Oid(16390))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_u32(&filtered, "inhrelid"), vec![16390]);
        assert_eq!(col_u32(&filtered, "inhparent"), vec![16385]);
    }

    /// A predicate on `inhparent` narrows to the children of one parent —
    /// the other direction of the same pushdown contract.
    #[test]
    fn pushed_inhparent_predicate_narrows_to_one_parents_children() {
        let filtered = PgInherits
            .scan(&catalog(), &[Predicate::eq("inhparent", Oid(16395))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_u32(&filtered, "inhrelid"), vec![16400]);
    }

    /// A child with multiple parents (classical multiple inheritance)
    /// reports one row per parent, `inhseqno` matching declaration order.
    #[test]
    fn multiple_parents_report_in_seqno_order() {
        let batch = PgInherits
            .scan(&catalog(), &[Predicate::eq("inhrelid", Oid(16400))])
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let by_parent: std::collections::HashMap<u32, i32> = col_u32(&batch, "inhparent")
            .into_iter()
            .zip(col_i32(&batch, "inhseqno"))
            .collect();
        assert_eq!(by_parent[&16395], 1);
        assert_eq!(by_parent[&16396], 2);
    }

    /// A table with no inheritance parent yields no rows — not an error.
    #[test]
    fn table_with_no_parent_yields_no_rows() {
        let batch = PgInherits
            .scan(&catalog(), &[Predicate::eq("inhrelid", Oid(16405))])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgInherits.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// A predicate naming a column `pg_inherits` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgInherits
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_inherits",
                column: "nope".to_string(),
            }
        );
    }
}
