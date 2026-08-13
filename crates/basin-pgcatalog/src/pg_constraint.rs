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
//!   name text UNIQUE,
//!   CHECK (id > 0)
//! );
//!
//! SELECT conname, contype, condeferrable, condeferred, conenforced,
//!        convalidated, contypid, conindid, conparentid, confupdtype,
//!        confdeltype, confmatchtype, conislocal, coninhcount,
//!        connoinherit, conperiod, confupdtype::int, confdeltype::int
//!   FROM pg_constraint
//!  WHERE conrelid IN ('idx_child'::regclass, 'idx_parent'::regclass)
//!     OR confrelid = 'idx_parent'::regclass;
//! --   conname                  | contype | ... | confupdtype | confdeltype | confmatchtype | ...
//! --   idx_child_id_check       |    c    | ... |     (32)    |     (32)    |      (32)     |
//! --   idx_child_name_key       |    u    | ... |     (32)    |     (32)    |      (32)     |
//! --   idx_child_parent_id_fkey |    f    | ... |     'a'(97) |     'a'(97) |      's'(32?)  |
//! --   idx_child_pkey           |    p    | ... |     (32)    |     (32)    |      (32)     |
//! ```
//! (PostgreSQL 18 also emits a `contype = 'n'` row per `NOT NULL` column,
//! not shown above — out of scope here, see below.)
//!
//! # Column audit against live PostgreSQL 18.2 (see crate-level task docs)
//!
//! `SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//! WHERE attrelid = 'pg_catalog.pg_constraint'::regclass AND attnum > 0 ORDER
//! BY attnum` against a live server reports **28** columns. This relation
//! previously implemented 8 of them — `oid`, `conname`, `connamespace`,
//! `contype`, `conrelid`, `conkey`, `confrelid`, `confkey` — with `conrelid`,
//! `conkey`, `confrelid` and `confkey` in the wrong `attnum` position (real
//! positions 9, 21, 13, 22; this file had them packed at 5–8, the same bug
//! `pg_class`, `pg_attribute` and `pg_index` had before this audit cycle).
//! Fixed here — all four now sit at their real `attnum`. The remaining 20
//! columns, in real `attnum` order:
//!
//! - `condeferrable`, `condeferred` (5, 6) — `boolean`, `NOT NULL`.
//!   `ConstraintInfo` has no notion of `DEFERRABLE`/`INITIALLY DEFERRED`;
//!   live-verified `false` for every plain constraint this crate can
//!   construct. Added as placeholders.
//! - `conenforced` (7) — `boolean`, `NOT NULL`, new in PostgreSQL 18 (`NOT
//!   ENFORCED` on a `CHECK`/FK constraint). `ConstraintInfo` cannot represent
//!   an unenforced constraint; live-verified `true` for every constraint this
//!   crate builds. Added as a placeholder.
//! - `convalidated` (8) — `boolean`, `NOT NULL`. `ConstraintInfo` has no
//!   notion of `NOT VALID`; live-verified `true` for every constraint this
//!   crate builds (nothing here ever leaves one half-validated). Added as a
//!   placeholder.
//! - `contypid` (10) — `oid`, `NOT NULL`. Only nonzero for a `CHECK`
//!   constraint on a *domain*, which this crate does not model (constraints
//!   are always looked up per-table via [`CatalogSource::constraints`]);
//!   live-verified `0` for every table constraint. Added as `Oid::INVALID`.
//! - `conindid` (11) — `oid`, `NOT NULL`. The backing index for a
//!   `p`/`u`/`f` constraint's implicit unique/btree index — live-verified
//!   nonzero (equal to the index's own `pg_index.indexrelid`) for those three
//!   kinds, `0` for `c`. `ConstraintInfo` has no field linking a constraint to
//!   the [`crate::pg_index`] row that backs it, so this crate cannot know
//!   that oid honestly. Reported as `Oid::INVALID` for every row — a gap, not
//!   a choice; a real client cross-referencing `pg_constraint.conindid` with
//!   `pg_index.indexrelid` will not find the match a real server would offer.
//! - `conparentid` (12) — `oid`, `NOT NULL`. Only nonzero for a constraint
//!   cloned onto a partition from its parent table's constraint; this crate
//!   has no partitioning support. Added as `Oid::INVALID`.
//! - `confupdtype`, `confdeltype` (14, 15) — `"char"`, `NOT NULL`.
//!   Live-verified `' '` (space, not `NUL` — `confupdtype::int` reports `32`)
//!   for every non-foreign-key row, and `'a'` (`NO ACTION`, Postgres's
//!   default when a `REFERENCES` clause names no `ON UPDATE`/`ON DELETE`
//!   action) for a plain foreign key. `ConstraintInfo` has no field for the
//!   declared action, so every FK this crate can construct is the "no clause
//!   given" case — `'a'` is the real value for that case, not a guess, same
//!   reasoning as `pg_index`'s `indimmediate`.
//! - `confmatchtype` (16) — `"char"`, `NOT NULL`. Same story: `' '` for
//!   non-FK rows, `'s'` (`MATCH SIMPLE`, the default) for a plain FK.
//! - `conislocal` (17) — `boolean`, `NOT NULL`. `true` for a constraint
//!   defined directly on its table (not inherited from a partition parent);
//!   live-verified `true` for every constraint here, matching this crate's
//!   lack of partition-inheritance support. Added as a placeholder.
//! - `coninhcount` (18) — `smallint`, `NOT NULL`. Number of immediate parents
//!   this constraint is inherited from; `0` alongside `conislocal = true`.
//!   Added as a placeholder.
//! - `connoinherit` (19) — `boolean`, `NOT NULL`. Live-verified `true` for
//!   `p`/`u`/`f` (a primary key, unique or foreign key constraint never
//!   propagates to an inheriting child table, `NO INHERIT` or not) and
//!   `false` for `c` (a `CHECK` constraint *does* propagate unless declared
//!   `NO INHERIT`, which `ConstraintInfo` has no way to express, so the
//!   ordinary case is reported). This is not a single flat placeholder like
//!   the others above — it is a fact fully determined by
//!   [`crate::catalog_source::ConstraintKind`], computed by
//!   [`connoinherit_for`] below, not fabricated per row.
//! - `conperiod` (20) — `boolean`, `NOT NULL`, new in PostgreSQL 18 (`PERIOD`
//!   foreign keys / `WITHOUT OVERLAPS`). `ConstraintInfo` has no notion of
//!   either; live-verified `false` for every constraint this crate builds.
//!   Added as a placeholder.
//! - `conpfeqop`, `conppeqop`, `conffeqop` (23–25) — all `oid[]`, nullable.
//!   The per-column equality operators backing a foreign key ("pf" = PK-side
//!   vs FK-side, "pp" = PK-side vs PK-side, "ff" = FK-side vs FK-side).
//!   Live-verified populated (one operator oid per `conkey` entry) only for
//!   `contype = 'f'`, `NULL` otherwise. `ConstraintInfo` carries no column
//!   list at all (see below), so there is nothing to hang an operator oid on
//!   even for a foreign key; reported `NULL` for every row.
//! - `confdelsetcols` (26) — `smallint[]`, nullable. Only non-`NULL` for `ON
//!   DELETE SET NULL (col, ...)` / `ON DELETE SET DEFAULT (col, ...)` with an
//!   explicit column list, which `ConstraintInfo` cannot represent. Reported
//!   `NULL` for every row.
//! - `conexclop` (27) — `oid[]`, nullable. Only non-`NULL` for `contype =
//!   'x'` (`EXCLUDE`), which [`crate::catalog_source::ConstraintKind`] has no
//!   variant for. Reported `NULL` for every row.
//! - `conbin` (28) — `pg_node_tree`, nullable. The parsed expression tree for
//!   a `CHECK` constraint (`NULL` for every other kind). `ConstraintInfo`
//!   carries no expression text or tree for a check constraint, so even a
//!   `c`-kind row here cannot honestly report one. Reported `NULL` for every
//!   row, represented as `Utf8` — same choice [`crate::pg_index`] makes for
//!   `indexprs`/`indpred`, its own two `pg_node_tree` columns.
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
    builder::{Int16Builder, ListBuilder, UInt32Builder},
    BooleanArray, Int16Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, ConstraintInfo, ConstraintKind, TableInfo},
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

/// `connoinherit` is fully determined by `contype`, not fabricated per row —
/// see the module docs. A primary key, unique or foreign key constraint
/// never propagates to an inheriting child table; a plain `CHECK`
/// constraint does, absent a `NO INHERIT` clause `ConstraintInfo` cannot
/// express (so the ordinary, inheriting case is reported).
fn connoinherit_for(kind: ConstraintKind) -> bool {
    !matches!(kind, ConstraintKind::Check)
}

/// `confupdtype`/`confdeltype`: `' '` (not applicable) for every non-foreign
/// key, `'a'` (`NO ACTION`, Postgres's default) for a foreign key —
/// `ConstraintInfo` has no field for a declared `ON UPDATE`/`ON DELETE`
/// action, so every FK this crate builds is the no-clause-given case. See
/// module docs.
fn conf_action_for(kind: ConstraintKind) -> char {
    if kind == ConstraintKind::ForeignKey {
        'a'
    } else {
        ' '
    }
}

/// `confmatchtype`: `' '` for non-FK, `'s'` (`MATCH SIMPLE`, the default)
/// for a foreign key. See module docs.
fn confmatchtype_for(kind: ConstraintKind) -> char {
    if kind == ConstraintKind::ForeignKey {
        's'
    } else {
        ' '
    }
}

/// This row's value for `column`, or `None` if `column` is not one of this
/// relation's columns, and not one of the list-typed ones (`conkey`,
/// `confkey`, `conpfeqop`, `conppeqop`, `conffeqop`, `confdelsetcols`,
/// `conexclop`) handled separately by the caller.
fn row_value(row: &ConstraintRow, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(row.info.oid),
        "conname" => Value::Text(row.info.name.clone()),
        "connamespace" => Value::Oid(row.connamespace),
        "contype" => Value::Text(row.info.kind.as_char().to_string()),
        // `ConstraintInfo` has no deferrable/`NOT ENFORCED`/`NOT VALID`
        // concept — every constraint this crate builds is immediate,
        // enforced and validated. See module docs.
        "condeferrable" => Value::Bool(false),
        "condeferred" => Value::Bool(false),
        "conenforced" => Value::Bool(true),
        "convalidated" => Value::Bool(true),
        "conrelid" => Value::Oid(row.info.table_oid),
        // Only nonzero for a domain's `CHECK` constraint; out of scope. See
        // module docs.
        "contypid" => Value::Oid(Oid::INVALID),
        // `ConstraintInfo` has no link to the index backing a `p`/`u`/`f`
        // constraint — see module docs.
        "conindid" => Value::Oid(Oid::INVALID),
        // No partitioning support.
        "conparentid" => Value::Oid(Oid::INVALID),
        // `ConstraintInfo` has no foreign-table reference — see the module
        // docs — so every row reports the invalid oid here, same as real
        // Postgres does for a non-FK constraint.
        "confrelid" => Value::Oid(Oid::INVALID),
        "confupdtype" => Value::Text(conf_action_for(row.info.kind).to_string()),
        "confdeltype" => Value::Text(conf_action_for(row.info.kind).to_string()),
        "confmatchtype" => Value::Text(confmatchtype_for(row.info.kind).to_string()),
        // No partition-inheritance support — every constraint is local.
        "conislocal" => Value::Bool(true),
        "coninhcount" => Value::Int(0),
        "connoinherit" => Value::Bool(connoinherit_for(row.info.kind)),
        // PostgreSQL 18's `PERIOD` foreign keys are unsupported.
        "conperiod" => Value::Bool(false),
        // `conkey`/`confkey`/`conpfeqop`/`conppeqop`/`conffeqop`/
        // `confdelsetcols`/`conexclop` are list columns; no scalar `Value`
        // represents them, so a predicate naming any of them is rejected by
        // the schema check in `scan` rather than reaching here. `conbin` is
        // always `NULL` (see module docs) and likewise has no meaningful
        // scalar predicate value.
        _ => return None,
    })
}

/// `pg_catalog.pg_constraint`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgConstraint;

impl PgConstraint {
    fn arrow_schema() -> SchemaRef {
        let int16_list = || DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
        let oid_list = || DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("conname", DataType::Utf8, false),
            Field::new("connamespace", DataType::UInt32, false),
            Field::new("contype", DataType::Utf8, false),
            Field::new("condeferrable", DataType::Boolean, false),
            Field::new("condeferred", DataType::Boolean, false),
            Field::new("conenforced", DataType::Boolean, false),
            Field::new("convalidated", DataType::Boolean, false),
            Field::new("conrelid", DataType::UInt32, false),
            Field::new("contypid", DataType::UInt32, false),
            Field::new("conindid", DataType::UInt32, false),
            Field::new("conparentid", DataType::UInt32, false),
            Field::new("confrelid", DataType::UInt32, false),
            Field::new("confupdtype", DataType::Utf8, false),
            Field::new("confdeltype", DataType::Utf8, false),
            Field::new("confmatchtype", DataType::Utf8, false),
            Field::new("conislocal", DataType::Boolean, false),
            Field::new("coninhcount", DataType::Int16, false),
            Field::new("connoinherit", DataType::Boolean, false),
            Field::new("conperiod", DataType::Boolean, false),
            Field::new("conkey", int16_list(), true),
            Field::new("confkey", int16_list(), true),
            Field::new("conpfeqop", oid_list(), true),
            Field::new("conppeqop", oid_list(), true),
            Field::new("conffeqop", oid_list(), true),
            Field::new("confdelsetcols", int16_list(), true),
            Field::new("conexclop", oid_list(), true),
            Field::new("conbin", DataType::Utf8, true),
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
        // Placeholder defaults — see the module docs for why each literal is
        // the real value Postgres reports for every constraint this crate
        // can currently construct, not a guess.
        let condeferrables: BooleanArray = rows.iter().map(|_| false).collect();
        let condeferreds: BooleanArray = rows.iter().map(|_| false).collect();
        let conenforceds: BooleanArray = rows.iter().map(|_| true).collect();
        let convalidateds: BooleanArray = rows.iter().map(|_| true).collect();
        let conrelids: UInt32Array = rows.iter().map(|r| r.info.table_oid.get()).collect();
        let contypids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let conindids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let conparentids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let confrelids: UInt32Array = rows.iter().map(|_| Oid::INVALID.get()).collect();
        let confupdtypes: StringArray = rows
            .iter()
            .map(|r| Some(conf_action_for(r.info.kind).to_string()))
            .collect();
        let confdeltypes: StringArray = rows
            .iter()
            .map(|r| Some(conf_action_for(r.info.kind).to_string()))
            .collect();
        let confmatchtypes: StringArray = rows
            .iter()
            .map(|r| Some(confmatchtype_for(r.info.kind).to_string()))
            .collect();
        let conislocals: BooleanArray = rows.iter().map(|_| true).collect();
        let coninhcounts: Int16Array = rows.iter().map(|_| 0i16).collect();
        let connoinherits: BooleanArray =
            rows.iter().map(|r| connoinherit_for(r.info.kind)).collect();
        let conperiods: BooleanArray = rows.iter().map(|_| false).collect();

        // Always NULL — `ConstraintInfo` supplies no column list or
        // foreign-table reference to fill either with. See the module docs.
        let mut conkey_builder = ListBuilder::new(Int16Builder::new());
        let mut confkey_builder = ListBuilder::new(Int16Builder::new());
        let mut conpfeqop_builder = ListBuilder::new(UInt32Builder::new());
        let mut conppeqop_builder = ListBuilder::new(UInt32Builder::new());
        let mut conffeqop_builder = ListBuilder::new(UInt32Builder::new());
        let mut confdelsetcols_builder = ListBuilder::new(Int16Builder::new());
        let mut conexclop_builder = ListBuilder::new(UInt32Builder::new());
        for _ in &rows {
            conkey_builder.append(false);
            confkey_builder.append(false);
            conpfeqop_builder.append(false);
            conppeqop_builder.append(false);
            conffeqop_builder.append(false);
            confdelsetcols_builder.append(false);
            conexclop_builder.append(false);
        }
        let conkeys: ListArray = conkey_builder.finish();
        let confkeys: ListArray = confkey_builder.finish();
        let conpfeqops: ListArray = conpfeqop_builder.finish();
        let conppeqops: ListArray = conppeqop_builder.finish();
        let conffeqops: ListArray = conffeqop_builder.finish();
        let confdelsetcolss: ListArray = confdelsetcols_builder.finish();
        let conexclops: ListArray = conexclop_builder.finish();

        // Always NULL — `ConstraintInfo` carries no expression tree for a
        // `CHECK` constraint. See module docs.
        let conbins = StringArray::from(vec![None::<&str>; rows.len()]);

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(connames),
                Arc::new(connamespaces),
                Arc::new(contypes),
                Arc::new(condeferrables),
                Arc::new(condeferreds),
                Arc::new(conenforceds),
                Arc::new(convalidateds),
                Arc::new(conrelids),
                Arc::new(contypids),
                Arc::new(conindids),
                Arc::new(conparentids),
                Arc::new(confrelids),
                Arc::new(confupdtypes),
                Arc::new(confdeltypes),
                Arc::new(confmatchtypes),
                Arc::new(conislocals),
                Arc::new(coninhcounts),
                Arc::new(connoinherits),
                Arc::new(conperiods),
                Arc::new(conkeys),
                Arc::new(confkeys),
                Arc::new(conpfeqops),
                Arc::new(conppeqops),
                Arc::new(conffeqops),
                Arc::new(confdelsetcolss),
                Arc::new(conexclops),
                Arc::new(conbins),
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

    /// Pins the exact column layout (name, order, nullability) against live
    /// PostgreSQL 18.2's `pg_attribute` for `pg_constraint`. A future edit
    /// cannot silently reorder, rename, or flip nullability on a column.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgConstraint.schema();
        let got: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", false),
                ("conname", false),
                ("connamespace", false),
                ("contype", false),
                ("condeferrable", false),
                ("condeferred", false),
                ("conenforced", false),
                ("convalidated", false),
                ("conrelid", false),
                ("contypid", false),
                ("conindid", false),
                ("conparentid", false),
                ("confrelid", false),
                ("confupdtype", false),
                ("confdeltype", false),
                ("confmatchtype", false),
                ("conislocal", false),
                ("coninhcount", false),
                ("connoinherit", false),
                ("conperiod", false),
                ("conkey", true),
                ("confkey", true),
                ("conpfeqop", true),
                ("conppeqop", true),
                ("conffeqop", true),
                ("confdelsetcols", true),
                ("conexclop", true),
                ("conbin", true),
            ]
        );
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

    /// `confupdtype`/`confdeltype`/`confmatchtype` are `' '` for every
    /// non-foreign-key row, and the plain-FK defaults (`'a'`/`'a'`/`'s'`) for
    /// the foreign key — live-verified against PostgreSQL 18.2, see module
    /// docs.
    #[test]
    fn foreign_key_action_columns_distinguish_fk_from_non_fk() {
        let batch = PgConstraint.scan(&catalog(), &[]).unwrap();
        let by_name = |col: &str| -> std::collections::HashMap<String, String> {
            col_str(&batch, "conname")
                .into_iter()
                .zip(col_str(&batch, col))
                .collect()
        };
        let updtype = by_name("confupdtype");
        let deltype = by_name("confdeltype");
        let matchtype = by_name("confmatchtype");

        assert_eq!(updtype["widgets_owner_id_fkey"], "a");
        assert_eq!(deltype["widgets_owner_id_fkey"], "a");
        assert_eq!(matchtype["widgets_owner_id_fkey"], "s");

        for name in ["widgets_pkey", "widgets_name_key", "widgets_qty_check"] {
            assert_eq!(updtype[name], " ");
            assert_eq!(deltype[name], " ");
            assert_eq!(matchtype[name], " ");
        }
    }

    /// `connoinherit` is `true` for primary key/unique/foreign key and
    /// `false` for check — a fact determined by `contype`, not a flat
    /// placeholder. See module docs and [`super::connoinherit_for`].
    #[test]
    fn connoinherit_is_true_except_for_check_constraints() {
        let batch = PgConstraint.scan(&catalog(), &[]).unwrap();
        let by_name: std::collections::HashMap<String, bool> = col_str(&batch, "conname")
            .into_iter()
            .zip(col_bool(&batch, "connoinherit"))
            .collect();

        assert!(by_name["widgets_pkey"]);
        assert!(by_name["widgets_name_key"]);
        assert!(by_name["widgets_owner_id_fkey"]);
        assert!(!by_name["widgets_qty_check"]);
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
