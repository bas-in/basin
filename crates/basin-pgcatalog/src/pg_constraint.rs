//! `pg_catalog.pg_constraint` — named constraints (primary key, unique,
//! foreign key, check) attached to a table.
//!
//! Paired with [`crate::pg_index`] in
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md`: `pg_index` is how a
//! client learns a `btree` exists and is unique, `pg_constraint` is how it
//! learns *why* — a named `PRIMARY KEY`/`UNIQUE`/`FOREIGN KEY`/`CHECK`
//! constraint, the thing psql's `\d` prints in its `"Indexes:"` /
//! `"Foreign-key constraints:"` footer and the thing an ORM's migration
//! diffing keys off of by name.
//!
//! # Where these values come from
//!
//! Checked against a live PostgreSQL 18, not recalled from memory — this
//! project has repeatedly found recall wrong and the server right (see
//! `crates/basin-pgtype/src/operator.rs`'s module docs for the precedent).
//! The queries:
//!
//! ```sql
//! \d pg_constraint
//!
//! CREATE TABLE idx_parent (id serial PRIMARY KEY);
//! CREATE TABLE idx_child (
//!   id serial PRIMARY KEY,
//!   parent_id int REFERENCES idx_parent(id),
//!   CHECK (id > 0)
//! );
//!
//! SELECT oid, conname, connamespace, contype, conrelid, conkey, confrelid, confkey
//!   FROM pg_constraint
//!  WHERE conrelid = 'idx_child'::regclass OR confrelid = 'idx_child'::regclass;
//! --   oid   |         conname          | connamespace | contype | conrelid | conkey | confrelid | confkey
//! -- ...     | idx_child_id_check       |     <nsp>    |    c    |  <child> |  {1}   |     0     |
//! -- ...     | idx_child_pkey           |     <nsp>    |    p    |  <child> |  {1}   |     0     |
//! -- ...     | idx_child_parent_id_fkey |     <nsp>    |    f    |  <child> |  {2}   |  <parent> |  {1}
//! ```
//! (PostgreSQL 18 also emits a `contype = 'n'` row per `NOT NULL` column,
//! not shown above — out of scope here, see below.)
//!
//! Confirmed live:
//! - `oid`, `connamespace`, `conrelid`, `confrelid` are `oid`; `conname` is
//!   `name`; `contype` is `"char"`.
//! - `conkey`/`confkey` are nullable `smallint[]` — `{1}`/`{2}` for the
//!   constrained/referenced columns, empty (`NULL`, printed as blank above)
//!   when not applicable.
//! - `confrelid` is `0` (`InvalidOid`, not `NULL` — the column is declared
//!   `NOT NULL`) for every constraint that is not a foreign key; it only
//!   names a real relation for `contype = 'f'`.
//!
//! This relation reports the four kinds
//! [`crate::catalog_source::ConstraintKind`] models — `'p'` primary key,
//! `'u'` unique, `'f'` foreign key, `'c'` check — matching real Postgres's
//! `contype` values for those cases exactly. PostgreSQL 18's `'n'` (a
//! standalone `NOT NULL` constraint row, new in that release) is not
//! attempted, matching [`ConstraintKind`] having no variant for it.
//!
//! # What [`CatalogSource`] cannot yet supply
//!
//! [`crate::catalog_source::ConstraintInfo`] carries only `oid`, `table_oid`,
//! `name` and `kind` — no column list and no foreign-table reference. That
//! means three real `pg_constraint` columns cannot be filled in honestly from
//! what the trait exposes today:
//! - `conkey` — the constrained columns' `attnum`s. Always reported `NULL`
//!   here (a real primary key's `conkey` is never `NULL`; the gap is that
//!   `ConstraintInfo` has nothing resembling `IndexInfo::column_attnums`).
//! - `confrelid` — the referenced table, for a foreign key. Always reported
//!   as [`crate::Oid::INVALID`] (`0`) here, which is indistinguishable from a
//!   real non-FK row rather than a real FK row whose target this crate does
//!   not know.
//! - `confkey` — the referenced columns' `attnum`s. Always reported `NULL`
//!   here, same reason as `confrelid`.
//!
//! `connamespace`, by contrast, *is* derivable without changing the trait: it
//! is not on `ConstraintInfo`, but [`CatalogSource::tables`] already gives
//! every table's `namespace`, and every constraint is looked up by iterating
//! a table's own constraints — so this relation looks up the owning
//! `TableInfo` and reports its `namespace`, the same derivation
//! [`crate::pg_class::PgClass`] uses for `relnatts`/`relhasindex`.

use std::sync::Arc;

use arrow_array::{
    builder::{Int16Builder, ListBuilder},
    ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, ConstraintInfo, TableInfo},
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// A [`ConstraintInfo`] plus the one column ([`pg_constraint`](self)'s
/// `connamespace`) that must be derived from its owning table rather than
/// read off `ConstraintInfo` directly.
struct ConstraintRow {
    info: ConstraintInfo,
    connamespace: Oid,
}

/// This row's value for `column`, or `None` if `column` is not one of this
/// relation's columns, and not one of the list-typed ones (`conkey`,
/// `confkey`) handled separately by the caller.
fn row_value(row: &ConstraintRow, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(row.info.oid),
        "conname" => Value::Text(row.info.name.clone()),
        "connamespace" => Value::Oid(row.connamespace),
        "contype" => Value::Text(row.info.kind.as_char().to_string()),
        "conrelid" => Value::Oid(row.info.table_oid),
        // `ConstraintInfo` has no foreign-table reference — see the module
        // docs — so every row reports the invalid oid here, same as real
        // Postgres does for a non-FK constraint.
        "confrelid" => Value::Oid(Oid::INVALID),
        // `conkey`/`confkey` are list columns; no scalar `Value` represents
        // them, so a predicate naming either is rejected by the schema check
        // in `scan` rather than reaching here.
        _ => return None,
    })
}

/// `pg_catalog.pg_constraint`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgConstraint;

impl PgConstraint {
    fn arrow_schema() -> SchemaRef {
        let int16_list = || DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("conname", DataType::Utf8, false),
            Field::new("connamespace", DataType::UInt32, false),
            Field::new("contype", DataType::Utf8, false),
            Field::new("conrelid", DataType::UInt32, false),
            Field::new("conkey", int16_list(), true),
            Field::new("confrelid", DataType::UInt32, false),
            Field::new("confkey", int16_list(), true),
        ]))
    }
}

impl crate::SystemView for PgConstraint {
    fn name(&self) -> &str {
        "pg_constraint"
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
                    relation: "pg_constraint",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<ConstraintRow> = catalog
            .tables()
            .iter()
            .flat_map(|t: &TableInfo| {
                catalog
                    .constraints(t.oid)
                    .into_iter()
                    .map(|info| ConstraintRow {
                        info,
                        connamespace: t.namespace,
                    })
            })
            .filter(|row| {
                pushed
                    .iter()
                    .all(|p| p.matches(row_value(row, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.info.oid.get()).collect();
        let connames: StringArray = rows.iter().map(|r| Some(r.info.name.as_str())).collect();
        let connamespaces: UInt32Array = rows.iter().map(|r| r.connamespace.get()).collect();
        let contypes: StringArray = rows
            .iter()
            .map(|r| Some(r.info.kind.as_char().to_string()))
            .collect();
        let conrelids: UInt32Array = rows.iter().map(|r| r.info.table_oid.get()).collect();
        let confrelids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();

        // Always NULL — `ConstraintInfo` supplies no column list or
        // foreign-table reference to fill either with. See the module docs.
        let mut conkey_builder = ListBuilder::new(Int16Builder::new());
        let mut confkey_builder = ListBuilder::new(Int16Builder::new());
        for _ in &rows {
            conkey_builder.append(false);
            confkey_builder.append(false);
        }
        let conkeys: ListArray = conkey_builder.finish();
        let confkeys: ListArray = confkey_builder.finish();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(connames),
                Arc::new(connamespaces),
                Arc::new(contypes),
                Arc::new(conrelids),
                Arc::new(conkeys),
                Arc::new(confrelids),
                Arc::new(confkeys),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{
        catalog_source::{ConstraintKind, RelKind},
        mock::MockCatalog,
        SystemView,
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

    /// `widgets` has one constraint of each kind; `gadgets` has none.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_table(TableInfo {
                oid: Oid(16390),
                name: "gadgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(20001),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(20002),
                table_oid: Oid(16385),
                name: "widgets_name_key".to_string(),
                kind: ConstraintKind::Unique,
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(20003),
                table_oid: Oid(16385),
                name: "widgets_owner_id_fkey".to_string(),
                kind: ConstraintKind::ForeignKey,
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(20004),
                table_oid: Oid(16385),
                name: "widgets_qty_check".to_string(),
                kind: ConstraintKind::Check,
            })
    }

    #[test]
    fn name_is_pg_constraint() {
        assert_eq!(PgConstraint.name(), "pg_constraint");
    }

    /// The entire point of this crate, applied to `pg_constraint`: a
    /// predicate on `conrelid` must narrow to exactly one table's
    /// constraints.
    #[test]
    fn pushed_conrelid_predicate_narrows_to_one_tables_constraints() {
        let full = PgConstraint.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 4, "sanity: widgets has four constraints");

        let filtered = PgConstraint
            .scan(&catalog(), &[Predicate::eq("conrelid", Oid(16385))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 4);
        for relid in col_u32(&filtered, "conrelid") {
            assert_eq!(relid, 16385);
        }
    }

    /// `contype` distinguishes all four kinds this crate models.
    #[test]
    fn contype_distinguishes_all_four_kinds() {
        let batch = PgConstraint.scan(&catalog(), &[]).unwrap();
        let by_name: std::collections::HashMap<String, String> = col_str(&batch, "conname")
            .into_iter()
            .zip(col_str(&batch, "contype"))
            .collect();

        assert_eq!(by_name["widgets_pkey"], "p");
        assert_eq!(by_name["widgets_name_key"], "u");
        assert_eq!(by_name["widgets_owner_id_fkey"], "f");
        assert_eq!(by_name["widgets_qty_check"], "c");
    }

    /// `connamespace` is derived from the owning table, not stored directly
    /// on `ConstraintInfo`.
    #[test]
    fn connamespace_is_derived_from_the_owning_table() {
        let batch = PgConstraint
            .scan(&catalog(), &[Predicate::eq("oid", Oid(20001))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_u32(&batch, "connamespace"), vec![16384]);
    }

    /// A table with no constraints yields no rows — not an error.
    #[test]
    fn table_with_no_constraints_yields_no_rows() {
        let batch = PgConstraint
            .scan(&catalog(), &[Predicate::eq("conrelid", Oid(16390))])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgConstraint.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// A predicate naming a column `pg_constraint` does not have is an
    /// error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgConstraint
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_constraint",
                column: "nope".to_string(),
            }
        );
    }
}
